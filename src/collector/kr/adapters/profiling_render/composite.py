"""Composite renderer — fans a profile result out to every sub-renderer."""

from __future__ import annotations

from pathlib import Path

from collector.kr.adapters.profiling_render.artifact_renderer import ArtifactRenderer
from collector.kr.adapters.profiling_render.markdown_renderer import MarkdownRenderer
from collector.kr.adapters.profiling_render.notebook_renderer import NotebookRenderer
from collector.kr.domain.profiling import ProfileResult


class CompositeProfileRenderer:
    """Dispatches one :class:`ProfileResult` to all configured renderers.

    Each sub-renderer reads the requested ``formats`` list and renders only
    its own formats, so the caller passes the full format list once.
    """

    def __init__(self, *, execute_notebooks: bool = True) -> None:
        """Build the composite renderer.

        Args:
            execute_notebooks: Execute notebooks with ``nbclient`` so figures
                embed.  Disable for fast unit/dry runs.
        """
        self._renderers = [
            ArtifactRenderer(),
            MarkdownRenderer(),
            NotebookRenderer(execute=execute_notebooks),
        ]

    def render(self, result: ProfileResult, *, out_dir: Path, formats: list[str]) -> list[Path]:
        """Render ``result`` into ``out_dir`` across every sub-renderer."""
        written: list[Path] = []
        for renderer in self._renderers:
            written.extend(renderer.render(result, out_dir=out_dir, formats=formats))
        return written
