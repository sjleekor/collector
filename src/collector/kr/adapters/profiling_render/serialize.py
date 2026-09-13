"""Shared serialization helpers for profiling renderers.

Profiling check rows contain Postgres-native values (``Decimal``, ``date``,
``datetime``) that are not directly JSON-serializable.  These helpers
normalize them consistently across every renderer.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from collector.kr.domain.profiling import CheckResult, ProfileResult, RunManifest


def to_jsonable(value: Any) -> Any:
    """Convert a single value into a JSON-serializable form."""
    if isinstance(value, Decimal):
        # Integers stay int; fractional Decimals become float for portability.
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def row_to_jsonable(row: dict) -> dict:
    """Convert every value in a result row to a JSON-serializable form."""
    return {k: to_jsonable(v) for k, v in row.items()}


def check_to_dict(check: CheckResult) -> dict:
    """Serialize a :class:`CheckResult` to a plain dict (SQL omitted from JSON)."""
    return {
        "kind": check.kind.value,
        "title": check.title,
        "ok": check.ok,
        "sampled": check.sampled,
        "sample_pct": check.sample_pct,
        "note": check.note,
        "warning": check.warning,
        "rows": [row_to_jsonable(r) for r in check.rows],
    }


def result_to_dict(result: ProfileResult) -> dict:
    """Serialize a full :class:`ProfileResult` to a plain dict."""
    pf = result.preflight
    return {
        "table": result.spec.table,
        "target": result.target,
        "weight": result.spec.weight.value,
        "generated_at": result.generated_at.isoformat(),
        "row_count": result.row_count,
        "skipped_reason": result.skipped_reason,
        "preflight": {
            "exists": pf.exists,
            "estimated_rows": pf.estimated_rows,
            "actual_rows": pf.actual_rows,
            "max_time_value": pf.max_time_value,
            "has_indexes": pf.has_indexes,
            "columns": [
                {"name": c.name, "data_type": c.data_type, "is_nullable": c.is_nullable}
                for c in pf.columns
            ],
        },
        "checks": [check_to_dict(c) for c in result.checks],
        "drilldown": {
            value: [check_to_dict(c) for c in checks] for value, checks in result.drilldown.items()
        },
        "warnings": result.warnings,
    }


def manifest_to_dict(manifest: RunManifest) -> dict:
    """Serialize a :class:`RunManifest` to a plain dict."""
    return {
        "run_id": manifest.run_id,
        "target": manifest.target,
        "run_date": manifest.run_date,
        "generated_at": manifest.generated_at.isoformat(),
        "git_sha": manifest.git_sha,
        "sample_policy": manifest.sample_policy,
        "query_ok": manifest.query_ok,
        "query_failed": manifest.query_failed,
        "lib_versions": manifest.lib_versions,
        "tables": manifest.tables,
    }
