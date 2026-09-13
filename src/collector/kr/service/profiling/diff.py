"""Run-to-run drift computation for ``profile diff``.

Pure logic: takes two manifest dicts (as produced by the artifact renderer's
``manifest_to_dict``) and emits a :class:`DriftReport`.  File loading and
``latest`` symlink resolution live in the CLI composition root.

Initially this only *reports* drift (warnings).  Blocking quality gates with
per-table thresholds are deferred to a later milestone (PLAN §7).
"""

from __future__ import annotations

from datetime import datetime

from collector.kr.domain.profiling import DriftReport, TableDrift


def compare_manifests(
    baseline: dict,
    candidate: dict,
    *,
    generated_at: datetime,
) -> DriftReport:
    """Compute the drift between a baseline and candidate manifest dict.

    Args:
        baseline: The earlier run's manifest (``manifest_to_dict`` shape).
        candidate: The later run's manifest.
        generated_at: Timestamp to stamp on the report (KST).

    Returns:
        A :class:`DriftReport` with one :class:`TableDrift` per table seen in
        either run, in candidate-then-baseline-only order.
    """
    base_tables: dict[str, dict] = baseline.get("tables", {})
    cand_tables: dict[str, dict] = candidate.get("tables", {})

    ordered: list[str] = list(cand_tables)
    ordered += [t for t in base_tables if t not in cand_tables]

    report = DriftReport(
        baseline_run_id=baseline.get("run_id", "?"),
        candidate_run_id=candidate.get("run_id", "?"),
        target=candidate.get("target", baseline.get("target", "?")),
        generated_at=generated_at,
    )
    for table in ordered:
        report.tables.append(_diff_table(table, base_tables.get(table), cand_tables.get(table)))
    return report


def _diff_table(table: str, before: dict | None, after: dict | None) -> TableDrift:
    """Diff one table's headline metrics across the two runs."""
    if before is None and after is not None:
        return _populate(table, {}, after, status="added")
    if after is None and before is not None:
        return TableDrift(table=table, status="removed")
    assert before is not None and after is not None
    return _populate(table, before, after)


def _populate(table: str, before: dict, after: dict, *, status: str | None = None) -> TableDrift:
    drift = TableDrift(table=table)

    rows_b = before.get("row_count")
    rows_a = after.get("row_count")
    if isinstance(rows_b, int) and isinstance(rows_a, int):
        drift.row_delta = rows_a - rows_b
        if rows_b > 0:
            drift.row_pct = round(100.0 * (rows_a - rows_b) / rows_b, 3)
    elif isinstance(rows_a, int) and rows_b is None:
        drift.row_delta = rows_a

    drift.max_time_before = before.get("max_time_value")
    drift.max_time_after = after.get("max_time_value")
    drift.max_time_moved = _compare_time(drift.max_time_before, drift.max_time_after)

    failed_b = before.get("checks_failed", 0) or 0
    failed_a = after.get("checks_failed", 0) or 0
    drift.failed_delta = failed_a - failed_b

    drift.skipped_before = before.get("skipped_reason")
    drift.skipped_after = after.get("skipped_reason")

    warns_b = set(before.get("warnings") or [])
    drift.new_warnings = [w for w in (after.get("warnings") or []) if w not in warns_b]

    drift.status = status or _status(drift)
    return drift


def _compare_time(before: str | None, after: str | None) -> str:
    if before is None or after is None:
        return "unknown"
    if after > before:
        return "forward"
    if after < before:
        return "backward"
    return "same"


def _status(drift: TableDrift) -> str:
    """Classify a populated drift as changed/unchanged (no add/remove here)."""
    changed = (
        (drift.row_delta not in (None, 0))
        or drift.max_time_moved in ("forward", "backward")
        or drift.failed_delta != 0
        or drift.skipped_before != drift.skipped_after
        or bool(drift.new_warnings)
    )
    return "changed" if changed else "unchanged"
