"""Profiling output renderers (artifact JSON/Parquet, Markdown, Notebook, HTML).

These are adapters: they depend on heavy optional libraries (``pyarrow``,
``nbformat``, ``nbclient``, ``nbconvert``, ``matplotlib``) declared in the
``analysis`` extra and are wired only in the CLI composition root.  Each
renderer degrades gracefully — emitting a recorded warning instead of
crashing — when its optional dependency is missing.
"""

from collector.kr.adapters.profiling_render.composite import CompositeProfileRenderer
from collector.kr.adapters.profiling_render.index_renderer import IndexRenderer

__all__ = ["CompositeProfileRenderer", "IndexRenderer"]
