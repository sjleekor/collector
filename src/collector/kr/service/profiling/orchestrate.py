"""Profiling run orchestration — drives specs through runner + renderers.

This sits at the service layer but takes the runner / renderer *ports* as
arguments (the concrete Postgres runner and file renderers are constructed in
``cli/app.py``).  It owns the run-level flow: build each table's profile,
render it, and fold its metrics into the run manifest.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from collector.kr.domain.profiling import (
    ProfileResult,
    RunManifest,
    SamplePolicy,
    TableProfileSpec,
)
from collector.kr.ports.profiling import (
    ProfileIndexRenderer,
    ProfileQueryRunner,
    ProfileRenderer,
)
from collector.kr.service.profiling.runner import build_profile, update_manifest

logger = logging.getLogger(__name__)


def run_profile(
    specs: list[TableProfileSpec],
    runner: ProfileQueryRunner,
    renderer: ProfileRenderer,
    index_renderer: ProfileIndexRenderer,
    *,
    target: str,
    run_id: str,
    run_date: str,
    out_dir: Path,
    formats: list[str],
    generated_at: datetime,
    include_drilldown: bool = False,
    sample_policy: str = SamplePolicy.AUTO.value,
    git_sha: str = "",
    lib_versions: dict[str, str] | None = None,
) -> tuple[RunManifest, list[ProfileResult]]:
    """Profile every spec, render outputs, and assemble the run manifest.

    Args:
        specs: Table specs to profile (catalog order).
        runner: Query-runner port (Postgres implementation in production).
        renderer: Per-table renderer port.
        index_renderer: Run-level index/manifest renderer port.
        target: DB target label (``local`` / ``sj2``).
        run_id: Unique run identifier (``<ts>_<target>``).
        run_date: ISO run date.
        out_dir: Run output directory (``.../<target>/<run_date>``).
        formats: Output formats to render.
        generated_at: Run timestamp (KST).
        include_drilldown: Whether to split long-format drilldown dimensions.
        sample_policy: Effective sampling policy label.
        git_sha: Repo commit at run time (best-effort).
        lib_versions: Analysis-library versions for reproducibility.

    Returns:
        The assembled manifest and the per-table results.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = RunManifest(
        run_id=run_id,
        target=target,
        run_date=run_date,
        generated_at=generated_at,
        git_sha=git_sha,
        sample_policy=sample_policy,
        lib_versions=lib_versions or {},
    )

    results: list[ProfileResult] = []
    for spec in specs:
        logger.info("Profiling %s (target=%s)…", spec.table, target)
        result = build_profile(spec, runner, target=target, include_drilldown=include_drilldown)
        renderer.render(result, out_dir=out_dir, formats=formats)
        update_manifest(manifest, result)
        results.append(result)
        if result.skipped_reason:
            logger.info("  %s: %s", spec.table, result.skipped_reason)
        elif result.warnings:
            logger.warning("  %s: %d warning(s)", spec.table, len(result.warnings))

    index_renderer.render_index(manifest, results, out_dir=out_dir, formats=formats)
    return manifest, results
