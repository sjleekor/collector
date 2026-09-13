"""Profiling orchestrator: spec → applicable checks → :class:`ProfileResult`.

This module is pure orchestration.  It selects which standard checks apply
to a spec (auto-skipping checks whose required columns are absent), asks the
:class:`ProfileQueryRunner` port to execute each, and assembles the result.
It never builds SQL itself — that keeps the hexagonal boundary intact.
"""

from __future__ import annotations

import logging

from collector.kr.domain.profiling import (
    CheckKind,
    ProfileResult,
    RunManifest,
    TableProfileSpec,
)
from collector.kr.ports.profiling import ProfileQueryRunner
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)


def applicable_checks(spec: TableProfileSpec) -> list[CheckKind]:
    """Return the standard checks that apply to ``spec``, in report order.

    A check is included only when the spec provides the columns it needs,
    encoding the manual "omit if not applicable" rule (``PLAN.md`` §3) in code.
    """
    checks: list[CheckKind] = [CheckKind.COUNT_KEYS_RANGE]  # C1 always runs

    if spec.time_col:
        checks.append(CheckKind.TIME_DISTRIBUTION)  # C2
    if spec.category_cols:
        checks.append(CheckKind.CATEGORY_DISTRIBUTION)  # C3
    checks.append(CheckKind.NULL_RATIOS)  # C4 always (uses null_cols or all)
    if spec.natural_key:
        checks.append(CheckKind.DUPLICATE_GROUPS)  # C5
    if spec.entity_key:
        checks.append(CheckKind.PER_ENTITY_DISTRIBUTION)  # C6
    if spec.entity_key and spec.time_col:
        checks.append(CheckKind.ENTITY_TIME_COVERAGE)  # C7
    if spec.numeric_cols:
        checks.append(CheckKind.NUMERIC_QUANTILES)  # C8
    if spec.top_n_cols:
        checks.append(CheckKind.CATEGORY_TOP_N)  # C9
    if spec.ingest_col:
        checks.append(CheckKind.INGEST_TIME_TREND)  # C10
    if spec.unit_cols:
        checks.append(CheckKind.UNIT_SCALE)  # C11
    if spec.fk_relations:
        checks.append(CheckKind.FK_INTEGRITY)  # C12
    if spec.pit_pairs:
        checks.append(CheckKind.PIT_VALIDITY)  # C13
    if spec.ingest_col or spec.time_col:
        checks.append(CheckKind.FRESHNESS)

    return checks


def build_profile(
    spec: TableProfileSpec,
    runner: ProfileQueryRunner,
    *,
    target: str,
    include_drilldown: bool = False,
) -> ProfileResult:
    """Profile a single table by driving the query-runner port.

    Args:
        spec: The table profile spec.
        runner: The query-runner port implementation.
        target: DB target label (``local`` / ``sj2``) recorded on the result.
        include_drilldown: When true and the spec declares a ``drilldown_dim``
            with enough distinct values, run per-value check sub-profiles.

    Returns:
        A fully assembled :class:`ProfileResult`.  Individual check failures
        are recorded as warnings; only a missing/empty table short-circuits.
    """
    generated_at = now_kst()
    preflight = runner.preflight(spec)

    result = ProfileResult(
        spec=spec,
        target=target,
        generated_at=generated_at,
        preflight=preflight,
    )

    if not preflight.exists:
        result.skipped_reason = "missing: table not found in target schema"
        logger.warning("Profiling skipped for %s: table missing", spec.table)
        return result

    row_count = (
        preflight.actual_rows if preflight.actual_rows is not None else preflight.estimated_rows
    )
    result.row_count = row_count

    if row_count is not None and row_count == 0:
        result.skipped_reason = "skipped: empty"
        logger.info("Profiling skipped for %s: empty table", spec.table)
        return result

    for kind in applicable_checks(spec):
        result.checks.append(runner.run_check(spec, kind))

    for check_id in spec.domain_checks:
        result.checks.append(runner.run_domain_check(spec, check_id))

    if include_drilldown and spec.drilldown_dim:
        _build_drilldown(spec, runner, result)

    return result


def _build_drilldown(
    spec: TableProfileSpec,
    runner: ProfileQueryRunner,
    result: ProfileResult,
) -> None:
    """Populate per-value drilldown profiles when the threshold is met."""
    assert spec.drilldown_dim is not None
    values = runner.distinct_values(spec.table, spec.drilldown_dim, limit=200)
    if len(values) < spec.drilldown_threshold:
        logger.info(
            "Drilldown skipped for %s.%s: only %d distinct value(s) (< %d)",
            spec.table,
            spec.drilldown_dim,
            len(values),
            spec.drilldown_threshold,
        )
        return

    # Per-value checks reuse the numeric/quantile + time/coverage lenses.
    drill_kinds = [
        kind
        for kind in (
            CheckKind.COUNT_KEYS_RANGE,
            CheckKind.TIME_DISTRIBUTION,
            CheckKind.NUMERIC_QUANTILES,
        )
        if kind in applicable_checks(spec)
    ]
    for value in values:
        result.drilldown[value] = [
            runner.run_check(spec, kind, drill_value=value) for kind in drill_kinds
        ]


def update_manifest(manifest: RunManifest, result: ProfileResult) -> None:
    """Fold a single table's headline metrics into the run manifest."""
    ok = sum(1 for c in result.checks if c.ok)
    failed = sum(1 for c in result.checks if not c.ok)
    for results in result.drilldown.values():
        ok += sum(1 for c in results if c.ok)
        failed += sum(1 for c in results if not c.ok)

    manifest.query_ok += ok
    manifest.query_failed += failed
    manifest.tables[result.spec.table] = {
        "weight": result.spec.weight.value,
        "row_count": result.row_count,
        "skipped_reason": result.skipped_reason,
        "estimated_rows": result.preflight.estimated_rows,
        "max_time_value": result.preflight.max_time_value,
        "checks_ok": ok,
        "checks_failed": failed,
        "warnings": result.warnings,
        "drilldown_values": len(result.drilldown),
    }
