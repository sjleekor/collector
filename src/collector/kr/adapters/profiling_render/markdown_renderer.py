"""Markdown renderer — git-diff-friendly, review-oriented table profile.

Produces a single ``<table>.md`` per profile with one section per check.
No optional dependency — always available, so the profiler yields a useful
human-readable artifact even without the ``analysis`` extra installed.
"""

from __future__ import annotations

from pathlib import Path

from collector.kr.adapters.profiling_render.serialize import to_jsonable
from collector.kr.domain.profiling import CheckResult, ProfileResult


class MarkdownRenderer:
    """Renders a profile result to a review-friendly Markdown document."""

    def render(self, result: ProfileResult, *, out_dir: Path, formats: list[str]) -> list[Path]:
        """Render ``<table>.md`` (+ per-value drilldown files) for ``md`` format."""
        if "md" not in formats:
            return []
        tables_dir = out_dir / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)
        written: list[Path] = []

        # Long-format drilldown is split into one file per dimension value under
        # tables/<table>/<dim>_<value>.md (PLAN §2.2), keeping the table-level
        # doc readable and git-diffable.
        drill_links: list[tuple[str, str]] = []
        if result.drilldown:
            drill_dir = tables_dir / result.spec.table
            drill_dir.mkdir(parents=True, exist_ok=True)
            dim = result.spec.drilldown_dim or "value"
            for value, checks in result.drilldown.items():
                filename = f"{_sanitize(dim)}_{_sanitize(value)}.md"
                drill_path = drill_dir / filename
                drill_path.write_text(
                    self._render_drilldown_text(result, value, checks), encoding="utf-8"
                )
                written.append(drill_path)
                drill_links.append((value, f"{result.spec.table}/{filename}"))

        path = tables_dir / f"{result.spec.table}.md"
        path.write_text(self._render_text(result, drill_links), encoding="utf-8")
        written.append(path)
        return written

    def _render_text(self, result: ProfileResult, drill_links: list[tuple[str, str]]) -> str:
        spec = result.spec
        lines: list[str] = [
            f"# `{spec.table}` profile",
            "",
            f"- Target: `{result.target}`",
            f"- Generated: {result.generated_at.isoformat()}",
            f"- Weight: `{spec.weight.value}`",
            f"- Row count: {_fmt(result.row_count)}",
        ]
        if result.preflight.max_time_value:
            lines.append(f"- Max `{spec.time_col}`: {result.preflight.max_time_value}")
        if result.skipped_reason:
            lines += ["", f"> **{result.skipped_reason}**", ""]
            return "\n".join(lines) + "\n"
        if result.warnings:
            lines += ["", "> ⚠ Warnings:", *[f"> - {w}" for w in result.warnings]]
        lines.append("")

        for check in result.checks:
            lines += self._render_check(check)

        if drill_links:
            lines += ["", f"## Drilldown — `{spec.drilldown_dim}` ({len(drill_links)})", ""]
            lines += [f"- [`{value}`]({link})" for value, link in drill_links]
            lines.append("")

        return "\n".join(lines) + "\n"

    def _render_drilldown_text(
        self, result: ProfileResult, value: str, checks: list[CheckResult]
    ) -> str:
        spec = result.spec
        lines = [
            f"# `{spec.table}` — {spec.drilldown_dim} = `{value}`",
            "",
            f"- Target: `{result.target}`",
            f"- Generated: {result.generated_at.isoformat()}",
            f"- [← back to table profile](../{spec.table}.md)",
            "",
        ]
        for check in checks:
            lines += self._render_check(check)
        return "\n".join(lines) + "\n"

    def _render_check(self, check: CheckResult, depth: int = 2) -> list[str]:
        heading = "#" * depth
        out = [f"{heading} {check.title}"]
        if check.warning:
            out += ["", f"> ⚠ {check.warning}", ""]
            return out
        if check.sampled:
            out += ["", f"> Sampled at {check.sample_pct}%.", ""]
        if check.note:
            out += ["", f"> {check.note}", ""]
        out.append("")
        out += _markdown_table(check.rows)
        out.append("")
        return out


def _markdown_table(rows: list[dict]) -> list[str]:
    if not rows:
        return ["_(no rows)_"]
    columns: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                columns.append(key)
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join("---" for _ in columns) + " |"
    body = [
        "| " + " | ".join(_cell(to_jsonable(row.get(c))) for c in columns) + " |" for row in rows
    ]
    return [header, divider, *body]


def _cell(value: object) -> str:
    if value is None:
        return ""
    return str(value).replace("|", "\\|")


def _fmt(value: int | None) -> str:
    return "—" if value is None else f"{value:,}"


def _sanitize(value: str) -> str:
    """Make a drilldown value safe for use as a filename component."""
    safe = "".join(c if (c.isalnum() or c in "-_") else "_" for c in str(value))
    return safe[:80] or "value"
