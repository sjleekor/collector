"""Renderer for the ``profile diff`` drift report (Markdown + JSON)."""

from __future__ import annotations

import json
from pathlib import Path

from collector.kr.domain.profiling import DriftReport, TableDrift


class DiffRenderer:
    """Writes a drift report as ``drift_report.md`` and ``drift_report.json``."""

    def render(self, report: DriftReport, *, out_dir: Path) -> list[Path]:
        """Render the drift report; returns the written file paths."""
        out_dir.mkdir(parents=True, exist_ok=True)
        written: list[Path] = []

        json_path = out_dir / "drift_report.json"
        json_path.write_text(
            json.dumps(self._to_dict(report), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        written.append(json_path)

        md_path = out_dir / "drift_report.md"
        md_path.write_text(self._to_md(report), encoding="utf-8")
        written.append(md_path)
        return written

    def _to_dict(self, report: DriftReport) -> dict:
        return {
            "baseline_run_id": report.baseline_run_id,
            "candidate_run_id": report.candidate_run_id,
            "target": report.target,
            "generated_at": report.generated_at.isoformat(),
            "changed_count": len(report.changed),
            "tables": [
                {
                    "table": t.table,
                    "status": t.status,
                    "row_delta": t.row_delta,
                    "row_pct": t.row_pct,
                    "max_time_before": t.max_time_before,
                    "max_time_after": t.max_time_after,
                    "max_time_moved": t.max_time_moved,
                    "failed_delta": t.failed_delta,
                    "skipped_before": t.skipped_before,
                    "skipped_after": t.skipped_after,
                    "new_warnings": t.new_warnings,
                }
                for t in report.tables
            ],
        }

    def _to_md(self, report: DriftReport) -> str:
        lines = [
            "# Profile drift report",
            "",
            f"- Baseline: `{report.baseline_run_id}`",
            f"- Candidate: `{report.candidate_run_id}`",
            f"- Target: `{report.target}`",
            f"- Generated: {report.generated_at.isoformat()}",
            f"- Changed tables: {len(report.changed)} / {len(report.tables)}",
            "",
            "| Table | Status | Row Δ | Row % | Max time | Failed Δ | New warns |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        for t in report.tables:
            lines.append(
                f"| `{t.table}` | {t.status} | {_fmt_delta(t.row_delta)} | "
                f"{_fmt_pct(t.row_pct)} | {_time_cell(t)} | "
                f"{_fmt_delta(t.failed_delta)} | {len(t.new_warnings)} |"
            )
        if any(t.new_warnings for t in report.tables):
            lines += ["", "## New warnings", ""]
            for t in report.tables:
                for warning in t.new_warnings:
                    lines.append(f"- `{t.table}`: {warning}")
        return "\n".join(lines) + "\n"


def _fmt_delta(value: int | None) -> str:
    if value is None:
        return "—"
    if value > 0:
        return f"+{value:,}"
    return f"{value:,}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "—"
    sign = "+" if value > 0 else ""
    return f"{sign}{value}%"


def _time_cell(t: TableDrift) -> str:
    arrow = {"forward": "→", "backward": "←", "same": "=", "unknown": "?"}[t.max_time_moved]
    if t.max_time_after:
        return f"{arrow} {t.max_time_after}"
    return arrow
