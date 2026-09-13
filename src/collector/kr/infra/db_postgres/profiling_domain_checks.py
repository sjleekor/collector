"""Domain-specific profiling check builders, keyed by id.

These absorb the per-table "특화 분석" items from ``PLAN.md`` §4 without
spawning one analyzer module per table.  A spec lists check ids in
``domain_checks``; the query runner dispatches each through this registry.

Each builder takes ``(runner, spec, title)`` and returns a
:class:`CheckResult`.  Builders reuse the runner's private SQL helpers, so
they share the same identifier-whitelisting and timeout guarantees.  All
identifiers are still emitted via ``psycopg2.sql.Identifier``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from psycopg2 import sql

from collector.kr.domain.profiling import CheckKind, CheckResult, TableProfileSpec

if TYPE_CHECKING:  # avoid an import cycle at runtime
    from collector.kr.infra.db_postgres.profiling_query_runner import (
        PostgresProfileQueryRunner,
    )

DomainCheckBuilder = Callable[["PostgresProfileQueryRunner", TableProfileSpec, str], CheckResult]


def _ohlc_identity(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """daily_ohlcv: OHLC identity + close>high anomaly counts.

    Reports rows that violate ``low <= min(open, close)`` /
    ``high >= max(open, close)`` and the pykrx halted-day artefact where
    ``close > high`` because OHL are reported as 0.
    """
    needed = ("open", "high", "low", "close")
    if any(not runner._has_column(spec.table, c) for c in needed):
        return CheckResult(
            kind=CheckKind.NUMERIC_QUANTILES,
            title=title,
            warning="OHLC columns absent from schema",
        )
    query = sql.SQL(
        "SELECT COUNT(*) AS total_rows, "
        "COUNT(*) FILTER (WHERE {low} > LEAST({open}, {close})) AS low_gt_body, "
        "COUNT(*) FILTER (WHERE {high} < GREATEST({open}, {close})) AS high_lt_body, "
        "COUNT(*) FILTER (WHERE {close} > {high}) AS close_gt_high, "
        "ROUND(100.0 * COUNT(*) FILTER (WHERE {close} > {high}) "
        "/ NULLIF(COUNT(*), 0), 4) AS close_gt_high_pct "
        "FROM {tbl}"
    ).format(
        low=sql.Identifier("low"),
        high=sql.Identifier("high"),
        open=sql.Identifier("open"),
        close=sql.Identifier("close"),
        tbl=sql.Identifier(spec.table),
    )
    rows, rendered = runner._run_sql(query)
    return CheckResult(
        kind=CheckKind.NUMERIC_QUANTILES,
        title=title,
        rows=rows,
        sql=rendered,
        note="close>high is the pykrx halted-day artefact, not a data error",
    )


def _impossible_daily_return(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """daily_ohlcv: per-year count of one-day returns that cannot happen.

    KRX caps a session's move at +/-30%, so a stored one-day ratio outside
    [2/3, 3/2] is not a market event.  In practice it means the adjusted series
    was restated upstream (naver re-adjusts the whole history on a split) while
    the stored rows were not, leaving a discontinuity at the corporate-action
    date.  Incremental collection cannot see this: it only appends after
    ``MAX(trade_date)``.

    Measured 2026-08-15: 354 rows overall, 279 of them after the 2026-04-10
    backfill, with ratios clustering on exactly 5.0 / 10.0 / 2.0.  The defect
    had been accruing for four months with nothing looking for it, which is why
    this check exists (``poc/n1_adjusted_price_vintage.md``).

    A residual is expected and is not the same defect: reverse splits and
    capital reductions are legitimately unadjusted by the upstream source.
    Read the trend, not a zero.
    """
    needed = ("ticker", "market", "trade_date", "close")
    if any(not runner._has_column(spec.table, c) for c in needed):
        return CheckResult(
            kind=CheckKind.PIT_VALIDITY,
            title=title,
            warning="ticker/market/trade_date/close absent from schema",
        )
    query = sql.SQL(
        "WITH stepped AS ("
        "  SELECT {trade_date} AS trade_date, {close} AS close, "
        "         LAG({close}) OVER (PARTITION BY {ticker}, {market} "
        "                            ORDER BY {trade_date}) AS prev_close "
        "  FROM {tbl}"
        "), jumps AS ("
        "  SELECT trade_date, close::float8 / prev_close AS ratio "
        "  FROM stepped WHERE prev_close > 0"
        ") "
        "SELECT EXTRACT(YEAR FROM trade_date)::int AS year, "
        "COUNT(*) FILTER (WHERE ratio > 1.5 OR ratio < 0.6667) AS impossible_moves, "
        "COUNT(*) FILTER (WHERE ratio > 1.5) AS up_moves, "
        "COUNT(*) FILTER (WHERE ratio < 0.6667) AS down_moves, "
        "COUNT(*) AS observed_returns "
        "FROM jumps GROUP BY 1 "
        "HAVING COUNT(*) FILTER (WHERE ratio > 1.5 OR ratio < 0.6667) > 0 "
        "ORDER BY 1"
    ).format(
        trade_date=sql.Identifier("trade_date"),
        close=sql.Identifier("close"),
        ticker=sql.Identifier("ticker"),
        market=sql.Identifier("market"),
        tbl=sql.Identifier(spec.table),
    )
    rows, rendered = runner._run_sql(query)
    return CheckResult(
        kind=CheckKind.PIT_VALIDITY,
        title=title,
        rows=rows,
        sql=rendered,
        note=(
            "KRX daily limit is +/-30%, so these are data artefacts, not market "
            "moves. up_moves are usually reverse splits / capital reductions, "
            "which the upstream adjusted series may legitimately leave alone; "
            "down_moves are usually a stale adjustment and are repairable with "
            "`prices backfill --refetch`."
        ),
    )


def _pit_universe_coverage(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Coverage against the universe that was actually listed on each date.

    ``get_active_stocks()`` answers "who is listed now", and every collector —
    including ``validate`` — resolves its targets that way.  So a company that
    delists disappears from the target set and its earlier rows are never
    collected, while nothing in the pipeline reports a gap: the row count keeps
    rising and the active-ticker coverage stays at 100%.

    This check asks the only question that exposes it — of the tickers that
    were listed on a past date, how many have a row?  Measured 2026-08-15
    against backfilled month-end snapshots: 2,056 listed on 2016-06-30, 286
    (13.9%) with no price row, and all 286 delisted names
    (``poc/survivorship_gap.md``).

    Requires ``universe snapshot backfill`` to have run; without historical
    snapshots there is nothing to compare against and the check skips.
    """
    needed = ("ticker", "trade_date")
    if any(not runner._has_column(spec.table, c) for c in needed):
        return CheckResult(
            kind=CheckKind.FK_INTEGRITY,
            title=title,
            warning="ticker/trade_date absent from schema",
        )
    if not runner.describe_schema("stock_master_snapshot_items"):
        return CheckResult(
            kind=CheckKind.FK_INTEGRITY,
            title=title,
            warning="stock_master_snapshot_items table absent",
        )
    query = sql.SQL(
        "WITH universe AS ("
        "  SELECT s.as_of_date, i.ticker "
        "  FROM stock_master_snapshot_items i "
        "  JOIN stock_master_snapshot s ON s.snapshot_id = i.snapshot_id"
        ") "
        "SELECT u.as_of_date, "
        "COUNT(*) AS listed_tickers, "
        "COUNT(*) FILTER (WHERE t.ticker IS NOT NULL) AS covered_tickers, "
        "COUNT(*) FILTER (WHERE t.ticker IS NULL) AS missing_tickers, "
        "ROUND(100.0 * COUNT(*) FILTER (WHERE t.ticker IS NULL) "
        "/ NULLIF(COUNT(*), 0), 3) AS missing_pct "
        "FROM universe u "
        "LEFT JOIN (SELECT DISTINCT {trade_date} AS d, {ticker} AS ticker FROM {tbl}) t "
        "  ON t.d = u.as_of_date AND t.ticker = u.ticker "
        "GROUP BY 1 ORDER BY 1"
    ).format(
        trade_date=sql.Identifier("trade_date"),
        ticker=sql.Identifier("ticker"),
        tbl=sql.Identifier(spec.table),
    )
    rows, rendered = runner._run_sql(query)
    return CheckResult(
        kind=CheckKind.FK_INTEGRITY,
        title=title,
        rows=rows,
        sql=rendered,
        note=(
            "missing_pct is survivorship, not sparsity: the absent tickers were "
            "listed on that date and are absent because targets are resolved "
            "from the CURRENT universe. Cross-sectional z-scores and decile "
            "boundaries are computed without them."
        ),
    )


def _halted_zero_ratio(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """daily_ohlcv: halted-day (open=high=low=0) and volume=0 ratios."""
    needed = ("open", "high", "low", "volume")
    if any(not runner._has_column(spec.table, c) for c in needed):
        return CheckResult(
            kind=CheckKind.NUMERIC_QUANTILES,
            title=title,
            warning="OHLCV columns absent from schema",
        )
    query = sql.SQL(
        "SELECT COUNT(*) AS total_rows, "
        "ROUND(100.0 * SUM(CASE WHEN {open}=0 AND {high}=0 AND {low}=0 THEN 1 ELSE 0 END) "
        "/ NULLIF(COUNT(*), 0), 4) AS halted_zero_pct, "
        "ROUND(100.0 * SUM(CASE WHEN {volume}=0 THEN 1 ELSE 0 END) "
        "/ NULLIF(COUNT(*), 0), 4) AS zero_volume_pct "
        "FROM {tbl}"
    ).format(
        open=sql.Identifier("open"),
        high=sql.Identifier("high"),
        low=sql.Identifier("low"),
        volume=sql.Identifier("volume"),
        tbl=sql.Identifier(spec.table),
    )
    rows, rendered = runner._run_sql(query)
    return CheckResult(kind=CheckKind.NUMERIC_QUANTILES, title=title, rows=rows, sql=rendered)


def _listing_span(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """daily_ohlcv: per-ticker listing span (first/last trade_date)."""
    if not (spec.entity_key and spec.time_col):
        return CheckResult(
            kind=CheckKind.TIME_DISTRIBUTION,
            title=title,
            warning="entity_key / time_col required",
        )
    ekey = sql.Identifier(spec.entity_key)
    tcol = sql.Identifier(spec.time_col)
    query = sql.SQL(
        "SELECT MIN(span_days) AS min_span_days, MAX(span_days) AS max_span_days, "
        "ROUND(AVG(span_days), 1) AS avg_span_days, "
        "percentile_cont(0.5) WITHIN GROUP (ORDER BY span_days) AS p50_span_days "
        "FROM (SELECT {ekey} AS e, (MAX({tcol}) - MIN({tcol})) AS span_days "
        "FROM {tbl} GROUP BY {ekey}) g"
    ).format(ekey=ekey, tcol=tcol, tbl=sql.Identifier(spec.table))
    rows, rendered = runner._run_sql(query)
    return CheckResult(kind=CheckKind.TIME_DISTRIBUTION, title=title, rows=rows, sql=rendered)


# ---------------------------------------------------------------------------
# krx_security_flow_raw
# ---------------------------------------------------------------------------


def _flow_source_dedupe(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """KRX vs PYKRX natural-key overlap and value-equality on the overlap.

    Two ``source`` rows can share ``(trade_date, ticker, market, metric_code)``;
    the manual profile found the overlapping values are identical, so model
    input should pick one source.  This quantifies the overlap and any value
    conflicts so the dedupe rule (prefer KRX, fall back to PYKRX) stays valid.

    Runs without sampling on a self-join, but with hash joins disabled and
    parallelism off — the manual profile hit shared-memory errors otherwise.
    """
    needed = ("trade_date", "ticker", "market", "metric_code", "source", "value")
    if any(not runner._has_column(spec.table, c) for c in needed):
        return CheckResult(
            kind=CheckKind.DUPLICATE_GROUPS,
            title=title,
            warning="flow source/value columns absent from schema",
        )
    tbl = sql.Identifier(spec.table)
    query = sql.SQL(
        "SET LOCAL enable_hashjoin = off; "
        "SET LOCAL max_parallel_workers_per_gather = 0; "
        "SELECT COUNT(*) AS overlap_keys, "
        "COUNT(*) FILTER (WHERE k.value IS DISTINCT FROM p.value) AS value_conflicts "
        "FROM {tbl} k JOIN {tbl} p "
        "ON k.trade_date = p.trade_date AND k.ticker = p.ticker "
        "AND k.market = p.market AND k.metric_code = p.metric_code "
        "WHERE k.source = 'KRX' AND p.source = 'PYKRX'"
    ).format(tbl=tbl)
    rows, rendered = runner._run_sql(query)
    return CheckResult(
        kind=CheckKind.DUPLICATE_GROUPS,
        title=title,
        rows=rows,
        sql=rendered,
        note="overlapping KRX/PYKRX values are expected identical; dedupe prefers KRX",
    )


def _flow_pit_join_coverage(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Per-year coverage of flow keys that join to ``daily_ohlcv`` (PIT).

    Reports, per business year, how many distinct flow ticker-days have a
    matching ``daily_ohlcv`` row — the join surface used when flows are merged
    onto the price panel.
    """
    if not runner.describe_schema("daily_ohlcv"):
        return CheckResult(
            kind=CheckKind.FK_INTEGRITY, title=title, warning="daily_ohlcv table absent"
        )
    tbl = sql.Identifier(spec.table)
    query = sql.SQL(
        "SET LOCAL enable_hashjoin = off; "
        "SET LOCAL max_parallel_workers_per_gather = 0; "
        "SELECT EXTRACT(YEAR FROM f.trade_date)::int AS year, "
        "COUNT(*) AS flow_keys, "
        "COUNT(*) FILTER (WHERE o.ticker IS NOT NULL) AS joined_keys, "
        "ROUND(100.0 * COUNT(*) FILTER (WHERE o.ticker IS NOT NULL) "
        "/ NULLIF(COUNT(*), 0), 3) AS join_pct "
        "FROM (SELECT DISTINCT trade_date, ticker, market FROM {tbl}) f "
        "LEFT JOIN daily_ohlcv o "
        "ON f.trade_date = o.trade_date AND f.ticker = o.ticker AND f.market = o.market "
        "GROUP BY 1 ORDER BY 1"
    ).format(tbl=tbl)
    rows, rendered = runner._run_sql(query)
    return CheckResult(kind=CheckKind.FK_INTEGRITY, title=title, rows=rows, sql=rendered)


# ---------------------------------------------------------------------------
# dart_xbrl_fact_raw
# ---------------------------------------------------------------------------


def _xbrl_concept_top(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Top-50 concept_id + numeric-parse success and instant/duration mix.

    Sampled on the large fact table per the spec's sampling policy.
    """
    from_clause, sampled, sample_pct = runner._from_clause(spec, CheckKind.CATEGORY_TOP_N)
    query = sql.SQL(
        "SELECT concept_id, COUNT(*) AS rows, "
        "ROUND(100.0 * COUNT(value_numeric) / NULLIF(COUNT(*), 0), 2) AS numeric_pct "
        "FROM {frm} GROUP BY concept_id ORDER BY rows DESC LIMIT 50"
    ).format(frm=from_clause)
    rows, rendered = runner._run_sql(query)
    return CheckResult(
        kind=CheckKind.CATEGORY_TOP_N,
        title=title,
        rows=rows,
        sampled=sampled,
        sample_pct=sample_pct,
        sql=rendered,
        note="sampled via TABLESAMPLE" if sampled else None,
    )


def _xbrl_orphan_fact(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Facts whose ``rcept_no`` has no matching ``dart_xbrl_document`` row."""
    if not runner.describe_schema("dart_xbrl_document"):
        return CheckResult(
            kind=CheckKind.FK_INTEGRITY,
            title=title,
            warning="dart_xbrl_document table absent",
        )
    tbl = sql.Identifier(spec.table)
    query = sql.SQL(
        "SELECT COUNT(*) AS distinct_rcept, "
        "COUNT(*) FILTER (WHERE d.rcept_no IS NULL) AS orphan_rcept "
        "FROM (SELECT DISTINCT rcept_no FROM {tbl}) f "
        "LEFT JOIN dart_xbrl_document d ON f.rcept_no = d.rcept_no"
    ).format(tbl=tbl)
    rows, rendered = runner._run_sql(query)
    return CheckResult(kind=CheckKind.FK_INTEGRITY, title=title, rows=rows, sql=rendered)


# ---------------------------------------------------------------------------
# dart_financial_statement_raw
# ---------------------------------------------------------------------------


def _fin_sj_div_dist(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """fs_div / sj_div mix + non-standard ``account_id`` usage ratio.

    Non-standard account ids (DART ``-`` placeholder or empty) cannot be mapped
    to canonical metrics, so their share is a normalization-readiness signal.
    """
    tbl = sql.Identifier(spec.table)
    query = sql.SQL(
        "SELECT fs_div, sj_div, COUNT(*) AS rows, "
        "ROUND(100.0 * SUM(CASE WHEN account_id IN ('', '-') THEN 1 ELSE 0 END) "
        "/ NULLIF(COUNT(*), 0), 3) AS nonstandard_account_pct "
        "FROM {tbl} GROUP BY fs_div, sj_div ORDER BY rows DESC"
    ).format(tbl=tbl)
    rows, rendered = runner._run_sql(query)
    return CheckResult(kind=CheckKind.CATEGORY_DISTRIBUTION, title=title, rows=rows, sql=rendered)


# ---------------------------------------------------------------------------
# stock_metric_fact
# ---------------------------------------------------------------------------


def _smf_capital_impairment(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Capital impairment count: ``total_equity`` facts with value < 0."""
    tbl = sql.Identifier(spec.table)
    query = sql.SQL(
        "SELECT COUNT(*) AS total_equity_facts, "
        "COUNT(*) FILTER (WHERE value_numeric < 0) AS impaired_facts, "
        "ROUND(100.0 * COUNT(*) FILTER (WHERE value_numeric < 0) "
        "/ NULLIF(COUNT(*), 0), 3) AS impaired_pct "
        "FROM {tbl} WHERE metric_code = 'total_equity'"
    ).format(tbl=tbl)
    rows, rendered = runner._run_sql(query)
    return CheckResult(kind=CheckKind.NUMERIC_QUANTILES, title=title, rows=rows, sql=rendered)


def _smf_metric_coverage(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Per-metric ticker coverage — surfaces sparse income-statement metrics."""
    tbl = sql.Identifier(spec.table)
    query = sql.SQL(
        "SELECT metric_code, COUNT(*) AS facts, "
        "COUNT(DISTINCT ticker) AS tickers, "
        "COUNT(DISTINCT bsns_year) AS years "
        "FROM {tbl} GROUP BY metric_code ORDER BY tickers ASC"
    ).format(tbl=tbl)
    rows, rendered = runner._run_sql(query)
    return CheckResult(
        kind=CheckKind.CATEGORY_DISTRIBUTION,
        title=title,
        rows=rows,
        sql=rendered,
        note="metrics with few distinct tickers are coverage-sparse (e.g. IS items)",
    )


# ---------------------------------------------------------------------------
# common_feature_daily_fact
# ---------------------------------------------------------------------------


def _cf_pit_violation(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Per-feature look-ahead violations: ``asof_available_date > feature_date``.

    A daily feature value must only depend on information available no later
    than the feature date.  Any row where the as-of availability date is in
    the future is a look-ahead leak.  Reports the worst offenders per feature.
    """
    needed = ("asof_available_date", "feature_date", "feature_code")
    if any(not runner._has_column(spec.table, c) for c in needed):
        return CheckResult(kind=CheckKind.PIT_VALIDITY, title=title, warning="PIT columns absent")
    tbl = sql.Identifier(spec.table)
    query = sql.SQL(
        "SELECT feature_code, COUNT(*) AS rows, "
        "COUNT(*) FILTER (WHERE asof_available_date > feature_date) AS pit_violations, "
        "MAX(asof_available_date - feature_date) "
        "FILTER (WHERE asof_available_date > feature_date) AS max_lookahead_days "
        "FROM {tbl} GROUP BY feature_code "
        "HAVING COUNT(*) FILTER (WHERE asof_available_date > feature_date) > 0 "
        "ORDER BY pit_violations DESC LIMIT 50"
    ).format(tbl=tbl)
    rows, rendered = runner._run_sql(query)
    note = "no look-ahead violations" if not rows else "LOOK-AHEAD VIOLATIONS PRESENT"
    return CheckResult(kind=CheckKind.PIT_VALIDITY, title=title, rows=rows, sql=rendered, note=note)


def _cf_stale_runs(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Per-feature forward-fill / stale span: max consecutive repeated value.

    Daily features are often forward-filled from a lower-frequency source.  A
    long run of an unchanged value flags a stale/warm-up region the modeller
    should treat carefully.  Reports the longest unchanged streak per feature.
    """
    needed = ("feature_code", "feature_date", "value_numeric")
    if any(not runner._has_column(spec.table, c) for c in needed):
        return CheckResult(
            kind=CheckKind.PIT_VALIDITY, title=title, warning="stale-run columns absent"
        )
    tbl = sql.Identifier(spec.table)
    # Gaps-and-islands: a new "island" starts whenever value_numeric changes
    # within a feature ordered by date; the largest island is the longest run.
    query = sql.SQL(
        "WITH marked AS ("
        "  SELECT feature_code, feature_date, value_numeric, "
        "    CASE WHEN value_numeric IS DISTINCT FROM "
        "      LAG(value_numeric) OVER (PARTITION BY feature_code ORDER BY feature_date) "
        "    THEN 1 ELSE 0 END AS is_change "
        "  FROM {tbl}"
        "), grp AS ("
        "  SELECT feature_code, value_numeric, "
        "    SUM(is_change) OVER (PARTITION BY feature_code ORDER BY feature_date) AS island "
        "  FROM marked"
        "), runs AS ("
        "  SELECT feature_code, island, COUNT(*) AS run_len "
        "  FROM grp GROUP BY feature_code, island"
        ") "
        "SELECT feature_code, MAX(run_len) AS max_unchanged_run "
        "FROM runs GROUP BY feature_code ORDER BY max_unchanged_run DESC LIMIT 50"
    ).format(tbl=tbl)
    rows, rendered = runner._run_sql(query)
    return CheckResult(
        kind=CheckKind.PIT_VALIDITY,
        title=title,
        rows=rows,
        sql=rendered,
        note="max consecutive unchanged daily values (forward-fill / stale proxy)",
    )


def _cf_coverage_gap(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Per-feature trading-day coverage vs ``daily_ohlcv`` over the same span.

    Compares each feature's distinct dates to the count of KRX trading days in
    its own [min, max] feature-date range — a coverage gap means missing daily
    rows the panel join would need to forward-fill.
    """
    if not runner.describe_schema("daily_ohlcv"):
        return CheckResult(
            kind=CheckKind.ENTITY_TIME_COVERAGE,
            title=title,
            warning="daily_ohlcv table absent",
        )
    tbl = sql.Identifier(spec.table)
    query = sql.SQL(
        "WITH f AS ("
        "  SELECT feature_code, MIN(feature_date) AS d0, MAX(feature_date) AS d1, "
        "    COUNT(DISTINCT feature_date) AS feature_days "
        "  FROM {tbl} GROUP BY feature_code"
        "), td AS ("
        "  SELECT f.feature_code, f.feature_days, "
        "    (SELECT COUNT(DISTINCT trade_date) FROM daily_ohlcv o "
        "       WHERE o.trade_date BETWEEN f.d0 AND f.d1) AS trading_days "
        "  FROM f"
        ") "
        "SELECT feature_code, feature_days, trading_days, "
        "(trading_days - feature_days) AS coverage_gap_days "
        "FROM td ORDER BY coverage_gap_days DESC LIMIT 50"
    ).format(tbl=tbl)
    rows, rendered = runner._run_sql(query)
    return CheckResult(
        kind=CheckKind.ENTITY_TIME_COVERAGE,
        title=title,
        rows=rows,
        sql=rendered,
        note="coverage_gap_days = KRX trading days minus feature days in range",
    )


# ---------------------------------------------------------------------------
# common_feature_catalog
# ---------------------------------------------------------------------------


def _cf_catalog_orphan(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Catalog <-> series DAG integrity via ``common_feature_catalog_input``.

    Reports active features that have no input series wired (orphan features)
    and the fan-out (1:1 vs 1:N) of inputs per feature.
    """
    for ref in ("common_feature_catalog_input", "common_feature_series"):
        if not runner.describe_schema(ref):
            return CheckResult(
                kind=CheckKind.FK_INTEGRITY,
                title=title,
                warning=f"{ref} table absent",
            )
    query = sql.SQL(
        "SELECT c.active, "
        "COUNT(*) AS features, "
        "COUNT(*) FILTER (WHERE i.series_count IS NULL) AS features_without_inputs, "
        "COALESCE(MAX(i.series_count), 0) AS max_inputs_per_feature "
        "FROM common_feature_catalog c "
        "LEFT JOIN (SELECT feature_code, COUNT(*) AS series_count "
        "           FROM common_feature_catalog_input GROUP BY feature_code) i "
        "ON c.feature_code = i.feature_code "
        "GROUP BY c.active ORDER BY c.active DESC"
    )
    rows, rendered = runner._run_sql(query)
    return CheckResult(kind=CheckKind.FK_INTEGRITY, title=title, rows=rows, sql=rendered)


# ---------------------------------------------------------------------------
# dart_corp_master
# ---------------------------------------------------------------------------


def _corp_master_listing(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Listed/active ratio + ticker-null ratio + stock_master join match.

    The corp master mixes listed and unlisted corporations; most rows have a
    null ``ticker``.  This quantifies the listed share, the ticker-null ratio,
    and how many tickers join back to ``stock_master``.
    """
    tbl = sql.Identifier(spec.table)
    join_expr = sql.SQL("")
    sm_present = bool(runner.describe_schema("stock_master"))
    if sm_present:
        join_expr = sql.SQL(
            ", COUNT(*) FILTER (WHERE c.ticker IS NOT NULL AND s.ticker IS NOT NULL) "
            "AS ticker_joined_to_master"
        )
        from_clause = sql.SQL("{tbl} c LEFT JOIN stock_master s ON c.ticker = s.ticker").format(
            tbl=tbl
        )
    else:
        from_clause = sql.SQL("{tbl} c").format(tbl=tbl)
    query = sql.SQL(
        "SELECT COUNT(*) AS rows, "
        "COUNT(*) FILTER (WHERE c.is_active) AS active_rows, "
        "ROUND(100.0 * COUNT(*) FILTER (WHERE c.ticker IS NULL) "
        "/ NULLIF(COUNT(*), 0), 3) AS ticker_null_pct{join_expr} "
        "FROM {frm}"
    ).format(join_expr=join_expr, frm=from_clause)
    rows, rendered = runner._run_sql(query)
    return CheckResult(kind=CheckKind.CATEGORY_DISTRIBUTION, title=title, rows=rows, sql=rendered)


# ---------------------------------------------------------------------------
# ingestion_runs
# ---------------------------------------------------------------------------


def _ingest_run_status(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Per-run-type status mix, stale-running count, and average duration."""
    tbl = sql.Identifier(spec.table)
    query = sql.SQL(
        "SELECT run_type, status, COUNT(*) AS runs, "
        "ROUND(AVG(EXTRACT(EPOCH FROM (ended_at - started_at)))::numeric, 1) "
        "AS avg_seconds, "
        "COUNT(*) FILTER (WHERE status = 'running' "
        "AND started_at < now() - interval '1 day') AS stale_running "
        "FROM {tbl} GROUP BY run_type, status ORDER BY runs DESC"
    ).format(tbl=tbl)
    rows, rendered = runner._run_sql(query)
    return CheckResult(kind=CheckKind.CATEGORY_DISTRIBUTION, title=title, rows=rows, sql=rendered)


def _run_zero_yield(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Runs that finished ``success`` while writing nothing.

    A run that fetched no rows and reported success is indistinguishable from a
    genuinely idle run in a scheduler's exit code, so the failure mode is
    silent.  ``backfill_daily_prices`` did exactly this until 2026-08-15: a
    validation error was recorded as ``failed`` in ``ingestion_runs`` while the
    CLI printed a success line and exited 0.

    Some run types are legitimately zero-yield once caught up (an incremental
    price sync on a quiet day), so this reports a rate per run type rather than
    flagging individual runs — a rate that climbs is the signal.
    """
    tbl = sql.Identifier(spec.table)
    # counts is JSONB; the row-writing key differs per run type, so sum the
    # ones actually in use rather than guessing a single name.
    query = sql.SQL(
        "WITH yields AS ("
        "  SELECT run_type, status, "
        "         COALESCE((counts->>'rows_upserted')::bigint, 0) "
        "       + COALESCE((counts->>'bars_upserted')::bigint, 0) "
        "       + COALESCE((counts->>'items_written')::bigint, 0) "
        "       + COALESCE((counts->>'observations_upserted')::bigint, 0) AS rows_written, "
        "         COALESCE((counts->>'requests_attempted')::bigint, 0) "
        "       + COALESCE((counts->>'slices_attempted')::bigint, 0) AS attempted "
        "  FROM {tbl} WHERE status = 'success'"
        ") "
        "SELECT run_type, COUNT(*) AS success_runs, "
        "COUNT(*) FILTER (WHERE rows_written = 0 AND attempted > 0) AS zero_yield_runs, "
        "ROUND(100.0 * COUNT(*) FILTER (WHERE rows_written = 0 AND attempted > 0) "
        "/ NULLIF(COUNT(*), 0), 3) AS zero_yield_pct "
        "FROM yields GROUP BY 1 "
        "HAVING COUNT(*) FILTER (WHERE rows_written = 0 AND attempted > 0) > 0 "
        "ORDER BY 3 DESC"
    ).format(tbl=tbl)
    rows, rendered = runner._run_sql(query)
    return CheckResult(
        kind=CheckKind.CATEGORY_DISTRIBUTION,
        title=title,
        rows=rows,
        sql=rendered,
        note=(
            "Only runs that attempted requests and wrote nothing are counted, so "
            "an idle skip-everything run is excluded. A rising rate means calls "
            "are being made and discarded."
        ),
    )


def _capital_change_direction_balance(
    runner: PostgresProfileQueryRunner, spec: TableProfileSpec, title: str
) -> CheckResult:
    """Increase/decrease mix of capital-change categories.

    ``ev_net_share_issuance_yoy`` classifies events by matching
    ``isu_dcrs_stle`` strings.  Known issue I3: the decrease side never matched,
    so a feature meant to measure NET issuance was measuring GROSS.  A string
    catalogue that silently matches nothing looks exactly like a category that
    never occurs, and nothing distinguished the two.

    Reporting the per-category counts makes the failure visible: real
    corporate-action data is not one-sided, so a category set with zero
    decreases is a mapping defect, not a market fact.
    """
    if not runner._has_column(spec.table, "isu_dcrs_stle"):
        return CheckResult(
            kind=CheckKind.CATEGORY_TOP_N,
            title=title,
            warning="isu_dcrs_stle absent from schema",
        )
    query = sql.SQL(
        "SELECT {stle} AS isu_dcrs_stle, COUNT(*) AS rows, "
        "COUNT(DISTINCT corp_code) AS corps, "
        "MIN(bsns_year) AS first_year, MAX(bsns_year) AS last_year "
        "FROM {tbl} GROUP BY 1 ORDER BY 2 DESC"
    ).format(stle=sql.Identifier("isu_dcrs_stle"), tbl=sql.Identifier(spec.table))
    rows, rendered = runner._run_sql(query)
    return CheckResult(
        kind=CheckKind.CATEGORY_TOP_N,
        title=title,
        rows=rows,
        sql=rendered,
        note=(
            "Compare against the mapping catalogue in definitions/: a category "
            "present here but unmatched there is silently dropped, and a "
            "direction with no rows at all is a mapping defect (I3)."
        ),
    )


DOMAIN_CHECK_BUILDERS: dict[str, DomainCheckBuilder] = {
    # daily_ohlcv
    "ohlc_identity": _ohlc_identity,
    "halted_zero_ratio": _halted_zero_ratio,
    "listing_span": _listing_span,
    "impossible_daily_return": _impossible_daily_return,
    # any table with (ticker, trade_date) — survivorship coverage
    "pit_universe_coverage": _pit_universe_coverage,
    # dart_capital_change_raw
    "capital_change_direction_balance": _capital_change_direction_balance,
    # krx_security_flow_raw
    "flow_source_dedupe": _flow_source_dedupe,
    "flow_pit_join_coverage": _flow_pit_join_coverage,
    # dart_xbrl_fact_raw
    "xbrl_concept_top": _xbrl_concept_top,
    "xbrl_orphan_fact": _xbrl_orphan_fact,
    # dart_financial_statement_raw
    "fin_sj_div_dist": _fin_sj_div_dist,
    # stock_metric_fact
    "smf_capital_impairment": _smf_capital_impairment,
    "smf_metric_coverage": _smf_metric_coverage,
    # common_feature_daily_fact
    "cf_pit_violation": _cf_pit_violation,
    "cf_stale_runs": _cf_stale_runs,
    "cf_coverage_gap": _cf_coverage_gap,
    # common_feature_catalog
    "cf_catalog_orphan": _cf_catalog_orphan,
    # dart_corp_master
    "corp_master_listing": _corp_master_listing,
    # ingestion_runs
    "ingest_run_status": _ingest_run_status,
    "run_zero_yield": _run_zero_yield,
}
