"""PostgreSQL implementation of the :class:`ProfileQueryRunner` port.

All SQL generation lives here.  Two safety invariants are enforced:

1. **Identifier whitelisting.**  Table and column names are only ever
   emitted via :class:`psycopg2.sql.Identifier`, and every column referenced
   by a check is first intersected with the table's live schema
   (``describe_schema``).  A catalog typo or an injection attempt through a
   spec field therefore cannot reach the database as raw SQL.
2. **Values are bind parameters.**  Literal values (drilldown filters) are
   passed as query parameters, never string-formatted.

Per-check failures (including statement timeouts) are caught and returned as
a :class:`CheckResult` with ``warning`` set, so one bad query degrades a
single report section to a warning instead of failing the whole run — the
same partial-run philosophy used by ``ingestion_runs``.
"""

from __future__ import annotations

import logging

from psycopg2 import sql

from collector.kr.domain.profiling import (
    CheckKind,
    CheckResult,
    ColumnInfo,
    SamplePolicy,
    TablePreflight,
    TableProfileSpec,
)
from collector.kr.infra.db_postgres.connection import get_connection

logger = logging.getLogger(__name__)

# Checks whose SQL is expensive enough to sample on large tables.
_EXPENSIVE_KINDS: frozenset[CheckKind] = frozenset(
    {CheckKind.NUMERIC_QUANTILES, CheckKind.CATEGORY_TOP_N}
)

_TITLES: dict[CheckKind, str] = {
    CheckKind.COUNT_KEYS_RANGE: "C1 — Row / key counts & time range",
    CheckKind.TIME_DISTRIBUTION: "C2 — Distribution over time (yearly)",
    CheckKind.CATEGORY_DISTRIBUTION: "C3 — Category distribution",
    CheckKind.NULL_RATIOS: "C4 — Null / empty ratios",
    CheckKind.DUPLICATE_GROUPS: "C5 — Natural-key duplicates",
    CheckKind.PER_ENTITY_DISTRIBUTION: "C6 — Rows per entity",
    CheckKind.ENTITY_TIME_COVERAGE: "C7 — Entity x time coverage",
    CheckKind.NUMERIC_QUANTILES: "C8 — Numeric quantiles",
    CheckKind.CATEGORY_TOP_N: "C9 — Top-N codes",
    CheckKind.INGEST_TIME_TREND: "C10 — Ingestion trend",
    CheckKind.UNIT_SCALE: "C11 — Unit / scale distribution",
    CheckKind.FK_INTEGRITY: "C12 — Foreign-key integrity",
    CheckKind.PIT_VALIDITY: "C13 — Point-in-time validity",
    CheckKind.FRESHNESS: "Freshness — latest data vs ingest",
}

# Approximate quantile probabilities reported for numeric columns.
_QUANTILE_PROBS: tuple[float, ...] = (0.01, 0.25, 0.5, 0.75, 0.95, 0.99, 0.999)


class PostgresProfileQueryRunner:
    """Executes profiling checks against one PostgreSQL DSN/target."""

    def __init__(
        self,
        dsn: str,
        *,
        target: str = "local",
        sample_policy: SamplePolicy = SamplePolicy.AUTO,
        sample_pct_override: float | None = None,
        query_timeout_sec: float | None = None,
    ) -> None:
        """Create a runner bound to a single DB target.

        Args:
            dsn: PostgreSQL connection string.
            target: Target label (``local`` / ``sj2``) for provenance.
            sample_policy: ``auto`` / ``full`` / ``sample`` sampling intent.
            sample_pct_override: Force a sampling percentage (overrides spec).
            query_timeout_sec: Per-query ``statement_timeout`` in seconds.
        """
        self._dsn = dsn
        self.target = target
        self._sample_policy = sample_policy
        self._sample_pct_override = sample_pct_override
        self._query_timeout_sec = query_timeout_sec
        self._schema_cache: dict[str, list[ColumnInfo]] = {}
        self._preflight_cache: dict[str, TablePreflight] = {}

    # -- schema / preflight -------------------------------------------------

    def describe_schema(self, table: str) -> list[ColumnInfo]:
        """Return the columns of ``table`` (cached; empty if absent)."""
        if table in self._schema_cache:
            return self._schema_cache[table]
        query = (
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s "
            "ORDER BY ordinal_position"
        )
        columns: list[ColumnInfo] = []
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (table,))
                for name, data_type, is_nullable in cur.fetchall():
                    columns.append(
                        ColumnInfo(
                            name=name,
                            data_type=data_type,
                            is_nullable=(is_nullable == "YES"),
                        )
                    )
        self._schema_cache[table] = columns
        return columns

    def preflight(self, spec: TableProfileSpec) -> TablePreflight:
        """Run cheap pre-checks before profiling a (potentially large) table.

        Cached per table so the ``AUTO`` sampling decision (which consults the
        row count) does not re-query the DB for every expensive check.
        """
        cached = self._preflight_cache.get(spec.table)
        if cached is not None:
            return cached

        columns = self.describe_schema(spec.table)
        if not columns:
            result = TablePreflight(table=spec.table, exists=False)
            self._preflight_cache[spec.table] = result
            return result

        ident = sql.Identifier(spec.table)
        estimated_rows: int | None = None
        actual_rows: int | None = None
        max_time_value: str | None = None
        has_indexes = False

        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT reltuples::bigint FROM pg_class " "WHERE oid = to_regclass(%s)",
                    (f"public.{spec.table}",),
                )
                row = cur.fetchone()
                if row and row[0] is not None:
                    estimated_rows = int(row[0])

                cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(ident))
                actual_rows = int(cur.fetchone()[0])

                if spec.time_col and self._has_column(spec.table, spec.time_col):
                    cur.execute(
                        sql.SQL("SELECT MAX({}) FROM {}").format(
                            sql.Identifier(spec.time_col), ident
                        )
                    )
                    value = cur.fetchone()[0]
                    max_time_value = None if value is None else str(value)

                cur.execute(
                    "SELECT COUNT(*) FROM pg_indexes "
                    "WHERE schemaname = 'public' AND tablename = %s",
                    (spec.table,),
                )
                has_indexes = int(cur.fetchone()[0]) > 0

        result = TablePreflight(
            table=spec.table,
            exists=True,
            estimated_rows=estimated_rows,
            actual_rows=actual_rows,
            max_time_value=max_time_value,
            has_indexes=has_indexes,
            columns=tuple(columns),
        )
        self._preflight_cache[spec.table] = result
        return result

    def distinct_values(self, table: str, column: str, limit: int) -> list[str]:
        """Return up to ``limit`` distinct non-null values of ``column``."""
        if not self._has_column(table, column):
            return []
        query = sql.SQL(
            "SELECT DISTINCT {col} FROM {tbl} WHERE {col} IS NOT NULL " "ORDER BY {col} LIMIT %s"
        ).format(col=sql.Identifier(column), tbl=sql.Identifier(table))
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                return [str(r[0]) for r in cur.fetchall()]

    # -- check dispatch -----------------------------------------------------

    def run_check(
        self,
        spec: TableProfileSpec,
        kind: CheckKind,
        *,
        drill_value: str | None = None,
    ) -> CheckResult:
        """Execute one standard check, degrading failures to warnings."""
        title = _TITLES.get(kind, kind.value)
        if drill_value is not None:
            title = f"{title} — {spec.drilldown_dim}={drill_value}"
        try:
            builder = getattr(self, f"_check_{kind.value}")
        except AttributeError:
            return CheckResult(
                kind=kind,
                title=title,
                warning=f"check {kind.value!r} is not implemented yet",
            )
        try:
            return builder(spec, title, drill_value)
        except Exception as exc:  # noqa: BLE001 — failure becomes a warning
            logger.warning("Check %s on %s failed: %s", kind.value, spec.table, exc)
            return CheckResult(kind=kind, title=title, warning=f"{type(exc).__name__}: {exc}")

    def run_domain_check(self, spec: TableProfileSpec, check_id: str) -> CheckResult:
        """Execute one domain-specific check by id (degrading to warnings)."""
        from collector.kr.infra.db_postgres.profiling_domain_checks import (
            DOMAIN_CHECK_BUILDERS,
        )

        title = f"Domain — {check_id}"
        builder = DOMAIN_CHECK_BUILDERS.get(check_id)
        if builder is None:
            return CheckResult(
                kind=CheckKind.COUNT_KEYS_RANGE,
                title=title,
                warning=f"domain check {check_id!r} is not registered",
            )
        try:
            return builder(self, spec, title)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Domain check %s on %s failed: %s", check_id, spec.table, exc)
            return CheckResult(
                kind=CheckKind.COUNT_KEYS_RANGE,
                title=title,
                warning=f"{type(exc).__name__}: {exc}",
            )

    # -- helpers ------------------------------------------------------------

    def _has_column(self, table: str, column: str) -> bool:
        return any(c.name == column for c in self.describe_schema(table))

    def _present(self, table: str, columns: tuple[str, ...]) -> list[str]:
        """Intersect requested columns with the live schema (order preserved)."""
        live = {c.name for c in self.describe_schema(table)}
        return [c for c in columns if c in live]

    def _column_info(self, table: str) -> dict[str, ColumnInfo]:
        return {c.name: c for c in self.describe_schema(table)}

    def _is_year_int_column(self, table: str, column: str) -> bool:
        """True when ``column`` is an integer year axis (e.g. ``bsns_year``).

        DART / canonical-metric tables use an ``INT`` business-year column
        rather than a ``DATE``, so the time-distribution and freshness checks
        must group by the value itself instead of ``EXTRACT(YEAR FROM ...)``.
        """
        info = self._column_info(table).get(column)
        if info is None:
            return False
        return any(t in info.data_type for t in ("int", "numeric", "double", "real"))

    def _should_sample(self, spec: TableProfileSpec, kind: CheckKind) -> bool:
        if self._sample_policy == SamplePolicy.FULL:
            return False
        if kind not in _EXPENSIVE_KINDS:
            return False
        if self._effective_sample_pct(spec) is None:
            return False
        if self._sample_policy == SamplePolicy.SAMPLE:
            return True
        # AUTO: sample only expensive tables above the row threshold.
        if spec.cost_class.value != "expensive":
            return False
        rows = spec.sampling.large_row_threshold
        preflight = self.preflight(spec)
        actual = preflight.actual_rows or preflight.estimated_rows or 0
        return actual >= rows

    def _effective_sample_pct(self, spec: TableProfileSpec) -> float | None:
        if self._sample_pct_override is not None:
            return self._sample_pct_override
        return spec.sampling.sample_pct

    def _from_clause(
        self, spec: TableProfileSpec, kind: CheckKind
    ) -> tuple[sql.Composable, bool, float | None]:
        """Return ``(FROM fragment, sampled, sample_pct)`` for a check."""
        ident = sql.Identifier(spec.table)
        if self._should_sample(spec, kind):
            pct = self._effective_sample_pct(spec)
            frag = sql.SQL("{} TABLESAMPLE SYSTEM ({})").format(ident, sql.Literal(pct))
            return frag, True, pct
        return ident, False, None

    def _drill_where(
        self, spec: TableProfileSpec, drill_value: str | None
    ) -> tuple[sql.Composable, list]:
        """Return a ``WHERE`` fragment + params for an optional drilldown."""
        if drill_value is None or not spec.drilldown_dim:
            return sql.SQL(""), []
        frag = sql.SQL(" WHERE {} = %s").format(sql.Identifier(spec.drilldown_dim))
        return frag, [drill_value]

    def _run_sql(self, query: sql.Composable, params: list | tuple = ()) -> tuple[list[dict], str]:
        """Execute a query with the configured timeout; return rows + SQL text."""
        with get_connection(self._dsn) as conn:
            with conn.cursor() as cur:
                if self._query_timeout_sec:
                    cur.execute(
                        "SET LOCAL statement_timeout = %s",
                        (int(self._query_timeout_sec * 1000),),
                    )
                rendered = cur.mogrify(query, params).decode("utf-8", "replace")
                cur.execute(query, params)
                if cur.description is None:
                    return [], rendered
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]
                return rows, rendered

    # -- standard check builders (C1–C13 + freshness) -----------------------

    def _check_count_keys_range(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        select_parts: list[sql.Composable] = [sql.SQL("COUNT(*) AS total_rows")]
        if spec.entity_key and self._has_column(spec.table, spec.entity_key):
            select_parts.append(
                sql.SQL("COUNT(DISTINCT {}) AS entities").format(sql.Identifier(spec.entity_key))
            )
        if spec.time_col and self._has_column(spec.table, spec.time_col):
            tcol = sql.Identifier(spec.time_col)
            select_parts.append(sql.SQL("COUNT(DISTINCT {}) AS time_points").format(tcol))
            select_parts.append(sql.SQL("MIN({}) AS min_time").format(tcol))
            select_parts.append(sql.SQL("MAX({}) AS max_time").format(tcol))
        where, params = self._drill_where(spec, drill_value)
        query = sql.SQL("SELECT {sel} FROM {tbl}{where}").format(
            sel=sql.SQL(", ").join(select_parts),
            tbl=sql.Identifier(spec.table),
            where=where,
        )
        rows, rendered = self._run_sql(query, params)
        return CheckResult(kind=CheckKind.COUNT_KEYS_RANGE, title=title, rows=rows, sql=rendered)

    def _check_time_distribution(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        tcol = sql.Identifier(spec.time_col)
        if self._is_year_int_column(spec.table, spec.time_col):
            year_expr = sql.SQL("{}::int").format(tcol)
        else:
            year_expr = sql.SQL("EXTRACT(YEAR FROM {})::int").format(tcol)
        select_parts = [
            sql.SQL("{} AS year").format(year_expr),
            sql.SQL("COUNT(*) AS rows"),
        ]
        if spec.entity_key and self._has_column(spec.table, spec.entity_key):
            select_parts.append(
                sql.SQL("COUNT(DISTINCT {}) AS entities").format(sql.Identifier(spec.entity_key))
            )
        select_parts.append(sql.SQL("COUNT(DISTINCT {}) AS time_points").format(tcol))
        where, params = self._drill_where(spec, drill_value)
        query = sql.SQL("SELECT {sel} FROM {tbl}{where} GROUP BY 1 ORDER BY 1").format(
            sel=sql.SQL(", ").join(select_parts),
            tbl=sql.Identifier(spec.table),
            where=where,
        )
        rows, rendered = self._run_sql(query, params)
        return CheckResult(kind=CheckKind.TIME_DISTRIBUTION, title=title, rows=rows, sql=rendered)

    def _check_category_distribution(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        present = self._present(spec.table, spec.category_cols)
        if not present:
            return CheckResult(
                kind=CheckKind.CATEGORY_DISTRIBUTION,
                title=title,
                warning="no category columns present in schema",
            )
        all_rows: list[dict] = []
        rendered_parts: list[str] = []
        where, params = self._drill_where(spec, drill_value)
        for col in present:
            select_parts = [
                sql.SQL("{} AS value").format(sql.Identifier(col)),
                sql.SQL("COUNT(*) AS rows"),
            ]
            if spec.entity_key and self._has_column(spec.table, spec.entity_key):
                select_parts.append(
                    sql.SQL("COUNT(DISTINCT {}) AS entities").format(
                        sql.Identifier(spec.entity_key)
                    )
                )
            query = sql.SQL("SELECT {sel} FROM {tbl}{where} GROUP BY 1 ORDER BY rows DESC").format(
                sel=sql.SQL(", ").join(select_parts),
                tbl=sql.Identifier(spec.table),
                where=where,
            )
            rows, rendered = self._run_sql(query, params)
            for r in rows:
                r["column"] = col
            all_rows.extend(rows)
            rendered_parts.append(rendered)
        return CheckResult(
            kind=CheckKind.CATEGORY_DISTRIBUTION,
            title=title,
            rows=all_rows,
            sql=";\n".join(rendered_parts),
        )

    def _check_null_ratios(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        target_cols = spec.null_cols or tuple(c.name for c in self.describe_schema(spec.table))
        present = self._present(spec.table, target_cols)
        if not present:
            return CheckResult(
                kind=CheckKind.NULL_RATIOS, title=title, warning="no columns to inspect"
            )
        info = self._column_info(spec.table)
        select_parts: list[sql.Composable] = [sql.SQL("COUNT(*) AS total_rows")]
        for col in present:
            ident = sql.Identifier(col)
            select_parts.append(
                sql.SQL(
                    "ROUND(100.0 * SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) "
                    "/ NULLIF(COUNT(*), 0), 4) AS {alias}"
                ).format(c=ident, alias=sql.Identifier(f"null_pct__{col}"))
            )
            data_type = info[col].data_type if col in info else ""
            if "char" in data_type or data_type == "text":
                select_parts.append(
                    sql.SQL(
                        "ROUND(100.0 * SUM(CASE WHEN btrim({c}) = '' THEN 1 ELSE 0 END) "
                        "/ NULLIF(COUNT(*), 0), 4) AS {alias}"
                    ).format(c=ident, alias=sql.Identifier(f"empty_pct__{col}"))
                )
        where, params = self._drill_where(spec, drill_value)
        query = sql.SQL("SELECT {sel} FROM {tbl}{where}").format(
            sel=sql.SQL(", ").join(select_parts),
            tbl=sql.Identifier(spec.table),
            where=where,
        )
        raw, rendered = self._run_sql(query, params)
        # Reshape the single wide row into per-column tidy rows.
        wide = raw[0] if raw else {}
        total = wide.get("total_rows")
        tidy: list[dict] = []
        for col in present:
            tidy.append(
                {
                    "column": col,
                    "total_rows": total,
                    "null_pct": wide.get(f"null_pct__{col}"),
                    "empty_pct": wide.get(f"empty_pct__{col}"),
                }
            )
        return CheckResult(kind=CheckKind.NULL_RATIOS, title=title, rows=tidy, sql=rendered)

    def _check_duplicate_groups(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        present = self._present(spec.table, spec.natural_key)
        if not present:
            return CheckResult(
                kind=CheckKind.DUPLICATE_GROUPS,
                title=title,
                warning="natural key columns absent from schema",
            )
        key_idents = sql.SQL(", ").join(sql.Identifier(c) for c in present)
        where, params = self._drill_where(spec, drill_value)
        query = sql.SQL(
            "SELECT COUNT(*) AS duplicate_groups, "
            "COALESCE(SUM(grp_count - 1), 0) AS excess_rows "
            "FROM (SELECT {keys}, COUNT(*) AS grp_count FROM {tbl}{where} "
            "GROUP BY {keys} HAVING COUNT(*) > 1) dups"
        ).format(keys=key_idents, tbl=sql.Identifier(spec.table), where=where)
        rows, rendered = self._run_sql(query, params)
        if rows:
            rows[0]["natural_key"] = ", ".join(present)
        return CheckResult(kind=CheckKind.DUPLICATE_GROUPS, title=title, rows=rows, sql=rendered)

    def _check_per_entity_distribution(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        ekey = sql.Identifier(spec.entity_key)
        where, params = self._drill_where(spec, drill_value)
        query = sql.SQL(
            "SELECT MIN(c) AS min_rows, MAX(c) AS max_rows, "
            "ROUND(AVG(c), 2) AS avg_rows, "
            "percentile_cont(0.5) WITHIN GROUP (ORDER BY c) AS p50_rows, "
            "percentile_cont(0.95) WITHIN GROUP (ORDER BY c) AS p95_rows "
            "FROM (SELECT {ekey} AS e, COUNT(*) AS c FROM {tbl}{where} GROUP BY {ekey}) g"
        ).format(ekey=ekey, tbl=sql.Identifier(spec.table), where=where)
        rows, rendered = self._run_sql(query, params)
        return CheckResult(
            kind=CheckKind.PER_ENTITY_DISTRIBUTION, title=title, rows=rows, sql=rendered
        )

    def _check_entity_time_coverage(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        ekey = sql.Identifier(spec.entity_key)
        tcol = sql.Identifier(spec.time_col)
        where, params = self._drill_where(spec, drill_value)
        query = sql.SQL(
            "SELECT COUNT(*) AS total_rows, "
            "COUNT(DISTINCT {ekey}) AS entities, "
            "COUNT(DISTINCT {tcol}) AS time_points, "
            "ROUND(COUNT(*)::numeric "
            "/ NULLIF(COUNT(DISTINCT {ekey}) * COUNT(DISTINCT {tcol}), 0), 4) "
            "AS coverage_ratio "
            "FROM {tbl}{where}"
        ).format(ekey=ekey, tcol=tcol, tbl=sql.Identifier(spec.table), where=where)
        rows, rendered = self._run_sql(query, params)
        return CheckResult(
            kind=CheckKind.ENTITY_TIME_COVERAGE, title=title, rows=rows, sql=rendered
        )

    def _check_numeric_quantiles(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        present = self._present(spec.table, spec.numeric_cols)
        if not present:
            return CheckResult(
                kind=CheckKind.NUMERIC_QUANTILES,
                title=title,
                warning="no numeric columns present in schema",
            )
        from_clause, sampled, sample_pct = self._from_clause(spec, CheckKind.NUMERIC_QUANTILES)
        where, params = self._drill_where(spec, drill_value)
        all_rows: list[dict] = []
        rendered_parts: list[str] = []
        for col in present:
            ident = sql.Identifier(col)
            quantile_parts = [
                sql.SQL("percentile_cont({p}) WITHIN GROUP (ORDER BY {c}) AS {alias}").format(
                    p=sql.Literal(p), c=ident, alias=sql.Identifier(_q_alias(p))
                )
                for p in _QUANTILE_PROBS
            ]
            select_parts = [
                sql.SQL("MIN({c}) AS min_value").format(c=ident),
                sql.SQL("MAX({c}) AS max_value").format(c=ident),
                sql.SQL("ROUND(AVG({c}), 4) AS avg_value").format(c=ident),
                *quantile_parts,
                sql.SQL(
                    "ROUND(100.0 * SUM(CASE WHEN {c} = 0 THEN 1 ELSE 0 END) "
                    "/ NULLIF(COUNT(*), 0), 4) AS zero_pct"
                ).format(c=ident),
                sql.SQL(
                    "ROUND(100.0 * SUM(CASE WHEN {c} < 0 THEN 1 ELSE 0 END) "
                    "/ NULLIF(COUNT(*), 0), 4) AS negative_pct"
                ).format(c=ident),
            ]
            query = sql.SQL("SELECT {sel} FROM {frm}{where}").format(
                sel=sql.SQL(", ").join(select_parts),
                frm=from_clause,
                where=where,
            )
            rows, rendered = self._run_sql(query, params)
            for r in rows:
                r["column"] = col
            all_rows.extend(rows)
            rendered_parts.append(rendered)
        return CheckResult(
            kind=CheckKind.NUMERIC_QUANTILES,
            title=title,
            rows=all_rows,
            sampled=sampled,
            sample_pct=sample_pct,
            sql=";\n".join(rendered_parts),
            note="sampled via TABLESAMPLE" if sampled else None,
        )

    def _check_category_top_n(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        present = self._present(spec.table, spec.top_n_cols)
        if not present:
            return CheckResult(
                kind=CheckKind.CATEGORY_TOP_N,
                title=title,
                warning="no Top-N columns present in schema",
            )
        from_clause, sampled, sample_pct = self._from_clause(spec, CheckKind.CATEGORY_TOP_N)
        where, params = self._drill_where(spec, drill_value)
        all_rows: list[dict] = []
        rendered_parts: list[str] = []
        for col in present:
            ident = sql.Identifier(col)
            query = sql.SQL(
                "SELECT {c} AS value, COUNT(*) AS rows FROM {frm}{where} "
                "GROUP BY {c} ORDER BY rows DESC LIMIT 20"
            ).format(c=ident, frm=from_clause, where=where)
            rows, rendered = self._run_sql(query, params)
            for r in rows:
                r["column"] = col
            all_rows.extend(rows)
            rendered_parts.append(rendered)
        return CheckResult(
            kind=CheckKind.CATEGORY_TOP_N,
            title=title,
            rows=all_rows,
            sampled=sampled,
            sample_pct=sample_pct,
            sql=";\n".join(rendered_parts),
        )

    def _check_ingest_time_trend(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        icol = sql.Identifier(spec.ingest_col)
        where, params = self._drill_where(spec, drill_value)
        query = sql.SQL(
            "SELECT to_char({icol}, 'YYYY-MM') AS ingest_month, COUNT(*) AS rows "
            "FROM {tbl}{where} GROUP BY 1 ORDER BY 1"
        ).format(icol=icol, tbl=sql.Identifier(spec.table), where=where)
        rows, rendered = self._run_sql(query, params)
        return CheckResult(kind=CheckKind.INGEST_TIME_TREND, title=title, rows=rows, sql=rendered)

    def _check_unit_scale(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        present = self._present(spec.table, spec.unit_cols)
        if not present:
            return CheckResult(
                kind=CheckKind.UNIT_SCALE, title=title, warning="no unit columns present"
            )
        all_rows: list[dict] = []
        rendered_parts: list[str] = []
        where, params = self._drill_where(spec, drill_value)
        for col in present:
            ident = sql.Identifier(col)
            query = sql.SQL(
                "SELECT {c} AS value, COUNT(*) AS rows FROM {tbl}{where} "
                "GROUP BY {c} ORDER BY rows DESC LIMIT 30"
            ).format(c=ident, tbl=sql.Identifier(spec.table), where=where)
            rows, rendered = self._run_sql(query, params)
            for r in rows:
                r["column"] = col
            all_rows.extend(rows)
            rendered_parts.append(rendered)
        return CheckResult(
            kind=CheckKind.UNIT_SCALE, title=title, rows=all_rows, sql=";\n".join(rendered_parts)
        )

    def _check_fk_integrity(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        all_rows: list[dict] = []
        rendered_parts: list[str] = []
        for fk in spec.fk_relations:
            if not self.describe_schema(fk.ref_table):
                all_rows.append({"ref_table": fk.ref_table, "note": "referenced table absent"})
                continue
            child_cols = [c for c, _ in fk.columns]
            if len(self._present(spec.table, tuple(child_cols))) != len(child_cols):
                all_rows.append({"ref_table": fk.ref_table, "note": "child columns absent"})
                continue
            distinct_keys = sql.SQL(", ").join(sql.Identifier(c) for c in child_cols)
            on_clause = sql.SQL(" AND ").join(
                sql.SQL("c.{cc} = p.{pc}").format(cc=sql.Identifier(cc), pc=sql.Identifier(pc))
                for cc, pc in fk.columns
            )
            first_parent = sql.Identifier(fk.columns[0][1])
            query = sql.SQL(
                "SELECT COUNT(*) AS distinct_keys, "
                "COUNT(*) FILTER (WHERE p.{pcheck} IS NULL) AS orphan_keys "
                "FROM (SELECT DISTINCT {keys} FROM {child}) c "
                "LEFT JOIN {parent} p ON {on}"
            ).format(
                pcheck=first_parent,
                keys=distinct_keys,
                child=sql.Identifier(spec.table),
                parent=sql.Identifier(fk.ref_table),
                on=on_clause,
            )
            rows, rendered = self._run_sql(query)
            for r in rows:
                r["ref_table"] = fk.ref_table
            all_rows.extend(rows)
            rendered_parts.append(rendered)
        return CheckResult(
            kind=CheckKind.FK_INTEGRITY,
            title=title,
            rows=all_rows,
            sql=";\n".join(rendered_parts) if rendered_parts else None,
        )

    def _check_pit_validity(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        all_rows: list[dict] = []
        rendered_parts: list[str] = []
        where, params = self._drill_where(spec, drill_value)
        for avail_col, event_col in spec.pit_pairs:
            if not (
                self._has_column(spec.table, avail_col) and self._has_column(spec.table, event_col)
            ):
                continue
            a, e = sql.Identifier(avail_col), sql.Identifier(event_col)
            query = sql.SQL(
                "SELECT COUNT(*) AS total_rows, "
                "COUNT(*) FILTER (WHERE {a} > {e}) AS pit_violations "
                "FROM {tbl}{where}"
            ).format(a=a, e=e, tbl=sql.Identifier(spec.table), where=where)
            rows, rendered = self._run_sql(query, params)
            for r in rows:
                r["available_col"] = avail_col
                r["event_col"] = event_col
            all_rows.extend(rows)
            rendered_parts.append(rendered)
        return CheckResult(
            kind=CheckKind.PIT_VALIDITY,
            title=title,
            rows=all_rows,
            sql=";\n".join(rendered_parts) if rendered_parts else None,
        )

    def _check_freshness(
        self, spec: TableProfileSpec, title: str, drill_value: str | None
    ) -> CheckResult:
        select_parts: list[sql.Composable] = []
        if spec.time_col and self._has_column(spec.table, spec.time_col):
            tcol = sql.Identifier(spec.time_col)
            select_parts.append(sql.SQL("MAX({}) AS latest_data").format(tcol))
            # Calendar age only makes sense for a DATE/timestamp axis, not an
            # INT business-year column.
            if not self._is_year_int_column(spec.table, spec.time_col):
                select_parts.append(
                    sql.SQL("(CURRENT_DATE - MAX({}::date)) AS data_age_days").format(tcol)
                )
        if spec.ingest_col and self._has_column(spec.table, spec.ingest_col):
            icol = sql.Identifier(spec.ingest_col)
            select_parts.append(sql.SQL("MAX({}) AS latest_ingest").format(icol))
        if not select_parts:
            return CheckResult(
                kind=CheckKind.FRESHNESS, title=title, warning="no time/ingest column"
            )
        query = sql.SQL("SELECT {sel} FROM {tbl}").format(
            sel=sql.SQL(", ").join(select_parts), tbl=sql.Identifier(spec.table)
        )
        rows, rendered = self._run_sql(query)
        return CheckResult(kind=CheckKind.FRESHNESS, title=title, rows=rows, sql=rendered)


def _q_alias(prob: float) -> str:
    """Build a stable column alias for a quantile probability."""
    return "p" + format(prob, "g").replace("0.", "").replace(".", "_")
