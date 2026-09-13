"""Jupyter Notebook renderer — reproducible deep-dive with inline graphs.

Builds a ``.ipynb`` per table with ``nbformat`` (header, schema, one section
per check with a table + matplotlib figure), executes it with ``nbclient`` so
figures embed as cell outputs, then optionally exports HTML via ``nbconvert``.

Requires the ``analysis`` extra (``nbformat``, ``nbclient``, ``nbconvert``,
``matplotlib``).  When those libraries are absent the renderer logs a warning
and returns no paths, leaving the JSON/Markdown artifacts intact.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from collector.kr.adapters.profiling_render.serialize import result_to_dict
from collector.kr.domain.profiling import ProfileResult

logger = logging.getLogger(__name__)


class NotebookRenderer:
    """Renders + executes a per-table notebook, optionally exporting HTML."""

    def __init__(self, *, execute: bool = True, kernel_timeout: int = 300) -> None:
        """Configure the notebook renderer.

        Args:
            execute: Run the notebook with ``nbclient`` so figures embed.
            kernel_timeout: Per-cell execution timeout in seconds.
        """
        self._execute = execute
        self._kernel_timeout = kernel_timeout

    def render(self, result: ProfileResult, *, out_dir: Path, formats: list[str]) -> list[Path]:
        """Render ``<table>.ipynb`` (and ``.html``) for the requested formats."""
        want_ipynb = "ipynb" in formats
        want_html = "html" in formats
        if not (want_ipynb or want_html):
            return []

        try:
            import nbformat
        except ImportError:
            logger.warning(
                "nbformat not installed — skipping notebook/HTML for %s (install "
                "the 'analysis' extra). JSON/Markdown artifacts still written.",
                result.spec.table,
            )
            return []

        notebook = self._build_notebook(nbformat, result)
        tables_dir = out_dir / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)

        written: list[Path] = []
        if self._execute:
            self._run_notebook(notebook, tables_dir)

        ipynb_path = tables_dir / f"{result.spec.table}.ipynb"
        if want_ipynb:
            with ipynb_path.open("w", encoding="utf-8") as fh:
                nbformat.write(notebook, fh)
            written.append(ipynb_path)

        if want_html:
            html_path = self._export_html(notebook, tables_dir / f"{result.spec.table}.html")
            if html_path is not None:
                written.append(html_path)

        return written

    def _build_notebook(self, nbformat, result: ProfileResult):  # noqa: ANN001
        """Assemble the notebook cells from a profile result."""
        cells = [
            nbformat.v4.new_markdown_cell(self._header_md(result)),
            nbformat.v4.new_code_cell(self._setup_code(result)),
        ]
        for idx, check in enumerate(result.checks):
            cells.append(nbformat.v4.new_markdown_cell(f"## {check.title}"))
            cells.append(nbformat.v4.new_code_cell(self._check_code(idx)))

        notebook = nbformat.v4.new_notebook()
        notebook.cells = cells
        notebook.metadata = {
            "kernelspec": {"name": "python3", "display_name": "Python 3"},
            "language_info": {"name": "python"},
            "krx_profile": {"table": result.spec.table, "target": result.target},
        }
        return notebook

    def _header_md(self, result: ProfileResult) -> str:
        spec = result.spec
        row_count = "—" if result.row_count is None else f"{result.row_count:,}"
        return (
            f"# `{spec.table}` profile\n\n"
            f"- Target: `{result.target}`\n"
            f"- Generated: {result.generated_at.isoformat()}\n"
            f"- Row count: {row_count}\n"
        )

    def _setup_code(self, result: ProfileResult) -> str:
        """Embed the serialized result so the notebook is self-contained."""
        payload = json.dumps(result_to_dict(result), ensure_ascii=False)
        return (
            "import json\n"
            "import pandas as pd\n"
            "try:\n"
            "    import matplotlib.pyplot as plt\n"
            "except Exception:\n"
            "    plt = None\n"
            f"PROFILE = json.loads(r'''{payload}''')\n"
            "CHECKS = PROFILE['checks']\n"
            "def check_df(i):\n"
            "    return pd.DataFrame(CHECKS[i]['rows'])\n"
        )

    def _check_code(self, idx: int) -> str:
        """Per-check cell: render the table and a best-effort bar chart."""
        return (
            f"_c = CHECKS[{idx}]\n"
            f"df = check_df({idx})\n"
            "if _c.get('warning'):\n"
            "    print('WARNING:', _c['warning'])\n"
            "display(df)\n"
            "if plt is not None and not df.empty:\n"
            "    num = df.select_dtypes('number')\n"
            "    if num.shape[1] >= 1 and df.shape[0] <= 60:\n"
            "        ax = num.iloc[:, :3].plot(kind='bar', figsize=(8, 3))\n"
            "        ax.set_title(_c['title'])\n"
            "        plt.tight_layout(); plt.show()\n"
        )

    def _run_notebook(self, notebook, tables_dir: Path) -> None:
        """Execute the notebook in-place; degrade to unexecuted on failure."""
        try:
            from nbclient import NotebookClient
        except ImportError:
            logger.warning("nbclient not installed — writing unexecuted notebook.")
            return
        try:
            client = NotebookClient(
                notebook,
                timeout=self._kernel_timeout,
                kernel_name="python3",
                resources={"metadata": {"path": str(tables_dir)}},
            )
            client.execute()
        except Exception as exc:  # noqa: BLE001 — keep the unexecuted notebook
            logger.warning("Notebook execution failed (%s); writing unexecuted.", exc)

    def _export_html(self, notebook, path: Path) -> Path | None:
        """Export an executed notebook to standalone HTML via nbconvert."""
        try:
            from nbconvert import HTMLExporter
        except ImportError:
            logger.warning("nbconvert not installed — skipping HTML export for %s.", path.stem)
            return None
        try:
            exporter = HTMLExporter()
            body, _ = exporter.from_notebook_node(notebook)
            path.write_text(body, encoding="utf-8")
            return path
        except Exception as exc:  # noqa: BLE001
            logger.warning("HTML export failed for %s: %s", path.stem, exc)
            return None
