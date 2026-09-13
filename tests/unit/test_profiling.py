"""Unit tests for the profiling subsystem (no database required).

These exercise the pure domain/service layers and the renderers against a
fake :class:`ProfileQueryRunner`, plus the SQL-builder's identifier
whitelisting against a stubbed schema cache.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from collector.kr.domain.profiling import (
    CheckKind,
    CheckResult,
    ColumnInfo,
    ProfileResult,
    ProfileTableRole,
    RunManifest,
    SamplePolicy,
    TablePreflight,
    TableProfileSpec,
)
from collector.kr.service.profiling import catalog
from collector.kr.service.profiling.runner import (
    applicable_checks,
    build_profile,
    update_manifest,
)
from collector.kr.util.time import now_kst

# ---------------------------------------------------------------------------
# Fake query runner — records calls, returns canned rows
# ---------------------------------------------------------------------------


class FakeRunner:
    """An in-memory ``ProfileQueryRunner`` for DB-free service tests."""

    def __init__(self, preflight: TablePreflight, *, distinct: list[str] | None = None) -> None:
        self._preflight = preflight
        self._distinct = distinct or []
        self.checks_run: list[tuple[str, str | None]] = []
        self.domain_run: list[str] = []

    def describe_schema(self, table: str) -> list[ColumnInfo]:
        return list(self._preflight.columns)

    def preflight(self, spec: TableProfileSpec) -> TablePreflight:
        return self._preflight

    def distinct_values(self, table: str, column: str, limit: int) -> list[str]:
        return self._distinct

    def run_check(self, spec, kind, *, drill_value=None) -> CheckResult:
        self.checks_run.append((kind.value, drill_value))
        return CheckResult(kind=kind, title=kind.value, rows=[{"n": 1}])

    def run_domain_check(self, spec, check_id) -> CheckResult:
        self.domain_run.append(check_id)
        return CheckResult(kind=CheckKind.COUNT_KEYS_RANGE, title=check_id, rows=[{"ok": 1}])


def _columns(*names: str) -> tuple[ColumnInfo, ...]:
    return tuple(ColumnInfo(name=n, data_type="text", is_nullable=False) for n in names)


def _preflight(table: str, rows: int, cols: tuple[ColumnInfo, ...]) -> TablePreflight:
    return TablePreflight(
        table=table, exists=True, estimated_rows=rows, actual_rows=rows, columns=cols
    )


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


def test_catalog_has_daily_ohlcv_reference_spec():
    spec = catalog.get_spec("daily_ohlcv")
    assert spec.table == "daily_ohlcv"
    assert spec.entity_key == "ticker"
    assert spec.time_col == "trade_date"
    assert spec.natural_key == ("trade_date", "ticker", "market")
    assert "ohlc_identity" in spec.domain_checks


def test_catalog_unknown_table_lists_known():
    with pytest.raises(KeyError, match="daily_ohlcv"):
        catalog.get_spec("does_not_exist")


def test_catalog_specs_for_weights_filters():
    full = catalog.specs_for_weights(["full"])
    assert all(s.weight.value == "full" for s in full)
    assert catalog.specs_for_weights(["nonsense"]) == []


def test_catalog_specs_for_roles_filters_raw_source_tables():
    raw = {s.table for s in catalog.specs_for_roles(["raw"])}

    assert "daily_ohlcv" in raw
    assert "krx_security_flow_raw" in raw
    assert "common_feature_observation_raw" in raw
    assert "dart_financial_statement_raw" in raw
    assert "stock_metric_fact" not in raw
    assert "common_feature_daily_fact" not in raw
    assert "ingestion_runs" not in raw


def test_catalog_roles_cover_raw_reference_operational():
    # Derived/operating tables were decommissioned (refactor §5); the profiler
    # now covers only raw + reference (config) + operational (audit) roles.
    roles = {spec.role for spec in catalog.all_specs()}

    assert roles == {
        ProfileTableRole.RAW,
        ProfileTableRole.REFERENCE,
        ProfileTableRole.OPERATIONAL,
    }


def test_catalog_weight_and_role_filter_combines_predicates():
    raw_light = {s.table for s in catalog.specs_for_weights_and_roles(["light"], ["raw"])}

    assert "stock_master" in raw_light
    assert "dart_corp_master" in raw_light
    assert "metric_catalog" not in raw_light
    assert "daily_ohlcv" not in raw_light


# ---------------------------------------------------------------------------
# applicable_checks — auto-skip when columns absent
# ---------------------------------------------------------------------------


def test_applicable_checks_full_spec_includes_all_applicable():
    spec = catalog.get_spec("daily_ohlcv")
    checks = applicable_checks(spec)
    assert CheckKind.COUNT_KEYS_RANGE in checks
    assert CheckKind.NUMERIC_QUANTILES in checks
    assert CheckKind.FK_INTEGRITY in checks
    assert CheckKind.ENTITY_TIME_COVERAGE in checks
    # daily_ohlcv has no top_n_cols / unit_cols / pit_pairs → skipped
    assert CheckKind.CATEGORY_TOP_N not in checks
    assert CheckKind.UNIT_SCALE not in checks
    assert CheckKind.PIT_VALIDITY not in checks


def test_applicable_checks_minimal_spec_skips_optional():
    spec = TableProfileSpec(table="bare")
    checks = applicable_checks(spec)
    assert checks == [CheckKind.COUNT_KEYS_RANGE, CheckKind.NULL_RATIOS]


def test_applicable_checks_requires_both_entity_and_time_for_coverage():
    entity_only = TableProfileSpec(table="t", entity_key="e")
    assert CheckKind.ENTITY_TIME_COVERAGE not in applicable_checks(entity_only)
    both = TableProfileSpec(table="t", entity_key="e", time_col="d")
    assert CheckKind.ENTITY_TIME_COVERAGE in applicable_checks(both)


# ---------------------------------------------------------------------------
# build_profile — short-circuits and assembly
# ---------------------------------------------------------------------------


def test_build_profile_skips_missing_table():
    spec = TableProfileSpec(table="ghost")
    runner = FakeRunner(TablePreflight(table="ghost", exists=False))
    result = build_profile(spec, runner, target="local")
    assert result.skipped_reason and result.skipped_reason.startswith("missing")
    assert result.checks == []


def test_build_profile_skips_empty_table():
    spec = TableProfileSpec(table="empty", time_col="d")
    runner = FakeRunner(_preflight("empty", 0, _columns("d")))
    result = build_profile(spec, runner, target="local")
    assert result.skipped_reason == "skipped: empty"
    assert result.checks == []


def test_build_profile_runs_standard_and_domain_checks():
    spec = catalog.get_spec("daily_ohlcv")
    cols = _columns(
        "trade_date",
        "ticker",
        "market",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source",
        "fetched_at",
    )
    runner = FakeRunner(_preflight("daily_ohlcv", 6_500_000, cols))
    result = build_profile(spec, runner, target="local")
    assert result.skipped_reason is None
    assert result.row_count == 6_500_000
    ran = {kind for kind, _ in runner.checks_run}
    assert "count_keys_range" in ran
    assert runner.domain_run == list(spec.domain_checks)


def test_build_profile_drilldown_threshold_gate():
    spec = TableProfileSpec(
        table="t",
        time_col="d",
        numeric_cols=("v",),
        drilldown_dim="code",
        drilldown_threshold=5,
    )
    cols = _columns("d", "v", "code")
    # Below threshold → no drilldown.
    runner = FakeRunner(_preflight("t", 100, cols), distinct=["a", "b"])
    result = build_profile(spec, runner, target="local", include_drilldown=True)
    assert result.drilldown == {}
    # At/above threshold → per-value sub-profiles.
    runner2 = FakeRunner(_preflight("t", 100, cols), distinct=["a", "b", "c", "d", "e"])
    result2 = build_profile(spec, runner2, target="local", include_drilldown=True)
    assert set(result2.drilldown) == {"a", "b", "c", "d", "e"}


# ---------------------------------------------------------------------------
# manifest folding
# ---------------------------------------------------------------------------


def test_update_manifest_counts_ok_and_failed():
    spec = TableProfileSpec(table="t", time_col="d")
    runner = FakeRunner(_preflight("t", 10, _columns("d")))
    result = build_profile(spec, runner, target="local")
    # inject one failing check
    result.checks.append(CheckResult(kind=CheckKind.FRESHNESS, title="x", warning="boom"))
    manifest = RunManifest(
        run_id="r", target="local", run_date="2026-06-19", generated_at=now_kst()
    )
    update_manifest(manifest, result)
    assert manifest.query_failed == 1
    assert manifest.query_ok >= 1
    assert manifest.tables["t"]["checks_failed"] == 1
    assert manifest.tables["t"]["warnings"] == ["boom"]


# ---------------------------------------------------------------------------
# SQL builder — identifier whitelisting (no DB, schema cache stubbed)
# ---------------------------------------------------------------------------


def _runner_with_schema(table: str, cols: tuple[ColumnInfo, ...]):
    from collector.kr.infra.db_postgres.profiling_query_runner import (
        PostgresProfileQueryRunner,
    )

    runner = PostgresProfileQueryRunner("postgresql://unused", target="local")
    runner._schema_cache[table] = list(cols)
    return runner


def test_present_intersects_with_live_schema():
    runner = _runner_with_schema("t", _columns("a", "b"))
    # 'c' is not in the live schema and a malicious name is rejected too.
    assert runner._present("t", ("a", "b", "c", "a; DROP TABLE t")) == ["a", "b"]


def test_has_column_rejects_unknown():
    runner = _runner_with_schema("t", _columns("a"))
    assert runner._has_column("t", "a") is True
    assert runner._has_column("t", "evil") is False


def test_quantile_alias_is_stable():
    from collector.kr.infra.db_postgres.profiling_query_runner import _q_alias

    assert _q_alias(0.5) == "p5"
    assert _q_alias(0.999) == "p999"
    assert _q_alias(0.01) == "p01"


# ---------------------------------------------------------------------------
# Renderers — JSON/Markdown always available, manifest round-trips
# ---------------------------------------------------------------------------


def _sample_result() -> ProfileResult:
    spec = TableProfileSpec(table="t", time_col="d", entity_key="e")
    result = ProfileResult(
        spec=spec,
        target="local",
        generated_at=datetime(2026, 6, 19, 8, 0, 0),
        preflight=_preflight("t", 42, _columns("d", "e")),
        row_count=42,
    )
    result.checks.append(
        CheckResult(
            kind=CheckKind.COUNT_KEYS_RANGE,
            title="C1",
            rows=[{"total_rows": 42, "entities": 7}],
        )
    )
    return result


def test_artifact_renderer_writes_json(tmp_path):
    from collector.kr.adapters.profiling_render.artifact_renderer import ArtifactRenderer

    written = ArtifactRenderer().render(_sample_result(), out_dir=tmp_path, formats=["json"])
    assert len(written) == 1
    data = json.loads(written[0].read_text())
    assert data["table"] == "t"
    assert data["checks"][0]["rows"][0]["total_rows"] == 42


def test_markdown_renderer_writes_table(tmp_path):
    from collector.kr.adapters.profiling_render.markdown_renderer import MarkdownRenderer

    written = MarkdownRenderer().render(_sample_result(), out_dir=tmp_path, formats=["md"])
    assert len(written) == 1
    text = written[0].read_text()
    assert "`t` profile" in text
    assert "total_rows" in text


def test_markdown_renderer_marks_skipped(tmp_path):
    from collector.kr.adapters.profiling_render.markdown_renderer import MarkdownRenderer

    result = _sample_result()
    result.skipped_reason = "skipped: empty"
    written = MarkdownRenderer().render(result, out_dir=tmp_path, formats=["md"])
    assert "skipped: empty" in written[0].read_text()


def test_index_renderer_manifest_roundtrip(tmp_path):
    from collector.kr.adapters.profiling_render.index_renderer import IndexRenderer

    result = _sample_result()
    manifest = RunManifest(
        run_id="20260619_local",
        target="local",
        run_date="2026-06-19",
        generated_at=datetime(2026, 6, 19, 8, 0, 0),
    )
    update_manifest(manifest, result)
    written = IndexRenderer().render_index(manifest, [result], out_dir=tmp_path, formats=["html"])
    names = {p.name for p in written}
    assert "_run_manifest.json" in names
    assert "run_summary.md" in names
    assert "index.html" in names
    manifest_path = tmp_path / "_run_manifest.json"
    reloaded = json.loads(manifest_path.read_text())
    assert reloaded["target"] == "local"
    assert "t" in reloaded["tables"]


def test_serialize_handles_decimal_and_dates():
    from decimal import Decimal

    from collector.kr.adapters.profiling_render.serialize import to_jsonable

    assert to_jsonable(Decimal("42")) == 42
    assert to_jsonable(Decimal("1.5")) == 1.5
    assert to_jsonable(datetime(2026, 6, 19)) == "2026-06-19T00:00:00"


# ---------------------------------------------------------------------------
# Sampling decision
# ---------------------------------------------------------------------------


def test_sample_decision_auto_only_for_large_expensive_tables():
    from collector.kr.domain.profiling import CostClass, SamplingPolicy

    runner = _runner_with_schema("big", _columns("v"))
    spec = TableProfileSpec(
        table="big",
        numeric_cols=("v",),
        cost_class=CostClass.EXPENSIVE,
        sampling=SamplingPolicy(sample_pct=1.0, large_row_threshold=1_000_000),
    )
    runner._preflight_cache["big"] = _preflight("big", 5_000_000, _columns("v"))
    assert runner._should_sample(spec, CheckKind.NUMERIC_QUANTILES) is True
    # cheap check kinds never sample
    assert runner._should_sample(spec, CheckKind.COUNT_KEYS_RANGE) is False


def test_sample_decision_full_policy_never_samples():
    from collector.kr.domain.profiling import CostClass, SamplingPolicy

    runner = _runner_with_schema("big", _columns("v"))
    runner._sample_policy = SamplePolicy.FULL
    spec = TableProfileSpec(
        table="big",
        numeric_cols=("v",),
        cost_class=CostClass.EXPENSIVE,
        sampling=SamplingPolicy(sample_pct=1.0, large_row_threshold=1),
    )
    runner._preflight_cache["big"] = _preflight("big", 5_000_000, _columns("v"))
    assert runner._should_sample(spec, CheckKind.NUMERIC_QUANTILES) is False


# ---------------------------------------------------------------------------
# M1 — large/long-format catalog specs + type-aware time axis
# ---------------------------------------------------------------------------


def test_catalog_includes_all_m1_tables():
    known = catalog.known_tables()
    for table in (
        "daily_ohlcv",
        "krx_security_flow_raw",
        "dart_financial_statement_raw",
        "dart_xbrl_fact_raw",
    ):
        assert table in known, table
    # decommissioned (recomputed by the DuckDB marts).
    assert "stock_metric_fact" not in known


def test_catalog_domain_checks_are_all_registered():
    from collector.kr.infra.db_postgres.profiling_domain_checks import (
        DOMAIN_CHECK_BUILDERS,
    )

    for spec in catalog.all_specs():
        for check_id in spec.domain_checks:
            assert check_id in DOMAIN_CHECK_BUILDERS, (spec.table, check_id)


def test_large_specs_are_expensive_and_sampled():
    for table in ("krx_security_flow_raw", "dart_xbrl_fact_raw"):
        spec = catalog.get_spec(table)
        assert spec.cost_class.value == "expensive"
        assert spec.sampling.sample_pct is not None


def test_long_format_specs_declare_drilldown():
    assert catalog.get_spec("krx_security_flow_raw").drilldown_dim == "metric_code"
    assert catalog.get_spec("dart_shareholder_return_raw").drilldown_dim == "metric_code"


def test_year_int_columns_detected_as_year_axis():
    # bsns_year is an INT axis → grouped by value, not EXTRACT(YEAR ...).
    runner = _runner_with_schema(
        "f",
        (
            ColumnInfo(name="bsns_year", data_type="integer", is_nullable=False),
            ColumnInfo(name="trade_date", data_type="date", is_nullable=False),
        ),
    )
    assert runner._is_year_int_column("f", "bsns_year") is True
    assert runner._is_year_int_column("f", "trade_date") is False
    assert runner._is_year_int_column("f", "missing") is False


def test_dart_specs_use_year_int_time_axis():
    for table in ("dart_xbrl_fact_raw", "dart_financial_statement_raw"):
        assert catalog.get_spec(table).time_col == "bsns_year"


# ---------------------------------------------------------------------------
# M2 — common_feature_* specs, PIT pairs, weight scoping
# ---------------------------------------------------------------------------


def test_catalog_includes_kept_common_feature_tables():
    # Decision 7: only the raw observations + the shared series config remain
    # Postgres tables; the daily fact + catalog(_input) moved to marts/code.
    known = catalog.known_tables()
    for table in ("common_feature_observation_raw", "common_feature_series"):
        assert table in known, table
    for dropped in (
        "common_feature_daily_fact",
        "common_feature_catalog",
        "common_feature_catalog_input",
    ):
        assert dropped not in known, dropped


def test_observation_raw_pit_pair_guards_vintage():
    spec = catalog.get_spec("common_feature_observation_raw")
    assert spec.pit_pairs == (("available_from_date", "observation_date"),)
    assert CheckKind.PIT_VALIDITY in applicable_checks(spec)


def test_pit_validity_skipped_without_pairs():
    # daily_ohlcv declares no pit_pairs → C13 is not run.
    assert CheckKind.PIT_VALIDITY not in applicable_checks(catalog.get_spec("daily_ohlcv"))


def test_common_feature_master_tables_are_light():
    # Only the kept series config remains a light reference table.
    assert catalog.get_spec("common_feature_series").weight.value == "light"


def test_light_weight_filter_selects_master_tables():
    light = {s.table for s in catalog.specs_for_weights(["light"])}
    assert "common_feature_series" in light
    assert "daily_ohlcv" not in light  # full-weight


# ---------------------------------------------------------------------------
# M3 — full catalog coverage, diff, skip-empty, per-feature split
# ---------------------------------------------------------------------------


def test_catalog_covers_all_pipeline_tables():
    # Every full-refresh pipeline table (minus out-of-scope intraday) must have
    # a profile spec so `profile all` never silently misses one.
    from collector.kr.infra.db_postgres.remote_sync import (
        PIPELINE_FULL_REFRESH_TABLE_NAMES,
    )

    known = catalog.known_tables()
    for table in PIPELINE_FULL_REFRESH_TABLE_NAMES:
        assert table in known, f"{table} missing from profile catalog"


def test_operating_tables_decommissioned():
    # operating pilot dropped (refactor §5, decision 5) — no longer profiled.
    for table in ("operating_metric_fact", "operating_source_document"):
        assert table not in catalog.known_tables()


def _drift(baseline_tables: dict, candidate_tables: dict):
    from collector.kr.service.profiling.diff import compare_manifests

    base = {"run_id": "r1", "target": "local", "tables": baseline_tables}
    cand = {"run_id": "r2", "target": "local", "tables": candidate_tables}
    return compare_manifests(base, cand, generated_at=datetime(2026, 6, 19))


def test_diff_detects_row_growth_and_time_advance():
    report = _drift(
        {"t": {"row_count": 100, "max_time_value": "2026-05-01", "checks_failed": 0}},
        {"t": {"row_count": 150, "max_time_value": "2026-06-01", "checks_failed": 0}},
    )
    d = report.tables[0]
    assert d.status == "changed"
    assert d.row_delta == 50
    assert d.row_pct == 50.0
    assert d.max_time_moved == "forward"


def test_diff_detects_added_and_removed_tables():
    report = _drift(
        {"gone": {"row_count": 10}},
        {"fresh": {"row_count": 5}},
    )
    by = {t.table: t.status for t in report.tables}
    assert by == {"fresh": "added", "gone": "removed"}


def test_diff_identical_runs_are_unchanged():
    same = {"t": {"row_count": 42, "max_time_value": "2026-06-01", "checks_failed": 0}}
    report = _drift(same, same)
    assert report.changed == []


def test_diff_flags_new_warnings_and_failed_delta():
    report = _drift(
        {"t": {"row_count": 1, "checks_failed": 0, "warnings": []}},
        {"t": {"row_count": 1, "checks_failed": 2, "warnings": ["timeout"]}},
    )
    d = report.tables[0]
    assert d.status == "changed"
    assert d.failed_delta == 2
    assert d.new_warnings == ["timeout"]


def test_markdown_renderer_splits_drilldown(tmp_path):
    from collector.kr.adapters.profiling_render.markdown_renderer import MarkdownRenderer

    spec = TableProfileSpec(table="t", time_col="d", drilldown_dim="code")
    result = ProfileResult(
        spec=spec,
        target="local",
        generated_at=datetime(2026, 6, 19, 8, 0, 0),
        preflight=_preflight("t", 9, _columns("d", "code")),
        row_count=9,
    )
    result.checks.append(CheckResult(kind=CheckKind.COUNT_KEYS_RANGE, title="C1", rows=[{"n": 9}]))
    result.drilldown["alpha"] = [
        CheckResult(kind=CheckKind.COUNT_KEYS_RANGE, title="C1", rows=[{"n": 4}])
    ]
    result.drilldown["beta/slash"] = [
        CheckResult(kind=CheckKind.COUNT_KEYS_RANGE, title="C1", rows=[{"n": 5}])
    ]
    written = MarkdownRenderer().render(result, out_dir=tmp_path, formats=["md"])
    names = {p.name for p in written}
    assert "t.md" in names
    # per-value files, with the slash sanitized
    assert "code_alpha.md" in names
    assert "code_beta_slash.md" in names
    table_md = (tmp_path / "tables" / "t.md").read_text()
    assert "Drilldown" in table_md and "code_alpha.md" in table_md


# ---------------------------------------------------------------------------
# Observability checks added 2026-08-15 (O group)
#
# Every defect found that day was cheaply detectable and nothing was looking:
# spurious split returns had been accruing for four months, and survivorship
# coverage had never been measured at all. These assertions pin the checks to
# the tables where the defects actually live, so removing one is deliberate.
# ---------------------------------------------------------------------------


def test_impossible_return_check_is_wired_to_the_price_table():
    spec = catalog.get_spec("daily_ohlcv")
    assert "impossible_daily_return" in spec.domain_checks


def test_pit_universe_coverage_covers_every_ticker_day_table():
    # Survivorship is not specific to prices: any table keyed by
    # (ticker, trade_date) inherits it from the same active-only target rule.
    for table in ("daily_ohlcv", "daily_market_cap", "krx_security_flow_raw"):
        spec = catalog.get_spec(table)
        assert "pit_universe_coverage" in spec.domain_checks, table


def test_zero_yield_check_is_wired_to_the_audit_table():
    spec = catalog.get_spec("ingestion_runs")
    assert "run_zero_yield" in spec.domain_checks


def test_capital_change_direction_balance_is_wired():
    # Known issue I3: the decrease side of the isu_dcrs_stle catalogue never
    # matched, which is indistinguishable from "decreases never happen".
    spec = catalog.get_spec("dart_capital_change_raw")
    assert "capital_change_direction_balance" in spec.domain_checks


def test_new_checks_declare_a_note_explaining_how_to_read_them():
    # A count with no interpretation gets ignored. Each of these reports a
    # number that is not self-explanatory (a residual is expected for
    # impossible returns; missing_pct is bias rather than sparsity), so the
    # builder must carry that context.
    import inspect

    from collector.kr.infra.db_postgres import profiling_domain_checks as dc

    for check_id in (
        "impossible_daily_return",
        "pit_universe_coverage",
        "run_zero_yield",
        "capital_change_direction_balance",
    ):
        builder = dc.DOMAIN_CHECK_BUILDERS[check_id]
        source = inspect.getsource(builder)
        assert "note=" in source, check_id
