"""Run-level index renderer — the catalog dashboard + diff baseline manifest.

Emits ``_run_manifest.json`` (headline metrics, the ``profile diff`` baseline)
plus a human-facing ``run_summary.md`` and ``index.html`` showing every
table's status, row count, and warnings at a glance.
"""

from __future__ import annotations

import json
from pathlib import Path

from collector.kr.adapters.profiling_render.serialize import manifest_to_dict
from collector.kr.domain.profiling import ProfileResult, RunManifest


class IndexRenderer:
    """Renders the per-run summary dashboard and machine-readable manifest."""

    def render_index(
        self,
        manifest: RunManifest,
        results: list[ProfileResult],
        *,
        out_dir: Path,
        formats: list[str],
    ) -> list[Path]:
        """Write the run manifest, summary Markdown, and HTML dashboard."""
        out_dir.mkdir(parents=True, exist_ok=True)
        written: list[Path] = []

        manifest_path = out_dir / "_run_manifest.json"
        manifest_path.write_text(
            json.dumps(manifest_to_dict(manifest), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        written.append(manifest_path)

        summary_path = out_dir / "run_summary.md"
        summary_path.write_text(self._summary_md(manifest), encoding="utf-8")
        written.append(summary_path)

        if "html" in formats:
            index_path = out_dir / "index.html"
            index_path.write_text(self._index_html(manifest), encoding="utf-8")
            written.append(index_path)

        return written

    def _summary_md(self, manifest: RunManifest) -> str:
        lines = [
            f"# Profile run `{manifest.run_id}`",
            "",
            f"- Target: `{manifest.target}`",
            f"- Run date: {manifest.run_date}",
            f"- Generated: {manifest.generated_at.isoformat()}",
            f"- Git SHA: `{manifest.git_sha or '—'}`",
            f"- Sample policy: `{manifest.sample_policy}`",
            f"- Checks OK / failed: {manifest.query_ok} / {manifest.query_failed}",
            "",
            "| Table | Weight | Rows | Status | Checks (ok/fail) | Warnings |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for table, info in manifest.tables.items():
            rows = info.get("row_count")
            rows_str = "—" if rows is None else f"{rows:,}"
            status = info.get("skipped_reason") or "profiled"
            warnings = info.get("warnings") or []
            lines.append(
                f"| `{table}` | {info.get('weight', '')} | {rows_str} | {status} | "
                f"{info.get('checks_ok', 0)}/{info.get('checks_failed', 0)} | {len(warnings)} |"
            )
        return "\n".join(lines) + "\n"

    def _index_html(self, manifest: RunManifest) -> str:
        rows_html: list[str] = []
        for table, info in manifest.tables.items():
            rows = info.get("row_count")
            rows_str = "—" if rows is None else f"{rows:,}"
            status = info.get("skipped_reason") or "profiled"
            failed = info.get("checks_failed", 0)
            css = "warn" if (failed or info.get("warnings")) else "ok"
            rows_html.append(
                f"<tr class='{css}'><td><code>{table}</code></td>"
                f"<td>{info.get('weight', '')}</td><td>{rows_str}</td>"
                f"<td>{status}</td>"
                f"<td>{info.get('checks_ok', 0)}/{failed}</td>"
                f"<td>{len(info.get('warnings') or [])}</td></tr>"
            )
        return (
            "<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>Profile {manifest.run_id}</title>"
            "<style>body{font-family:sans-serif;margin:2rem}"
            "table{border-collapse:collapse;width:100%}"
            "th,td{border:1px solid #ddd;padding:6px 10px;text-align:left}"
            "tr.warn{background:#fff3cd}tr.ok{background:#fff}"
            "th{background:#f1f1f1}</style></head><body>"
            f"<h1>Profile run <code>{manifest.run_id}</code></h1>"
            f"<p>Target <b>{manifest.target}</b> · {manifest.run_date} · "
            f"checks {manifest.query_ok} ok / {manifest.query_failed} failed</p>"
            "<table><thead><tr><th>Table</th><th>Weight</th><th>Rows</th>"
            "<th>Status</th><th>Checks</th><th>Warnings</th></tr></thead><tbody>"
            + "".join(rows_html)
            + "</tbody></table></body></html>"
        )
