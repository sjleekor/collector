"""JSON + Parquet artifact renderer — the machine-readable diff baseline."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from collector.kr.adapters.profiling_render.serialize import (
    result_to_dict,
    row_to_jsonable,
)
from collector.kr.domain.profiling import ProfileResult

logger = logging.getLogger(__name__)


class ArtifactRenderer:
    """Writes ``<table>.stats.json`` and (when available) ``<table>.dist.parquet``."""

    def render(self, result: ProfileResult, *, out_dir: Path, formats: list[str]) -> list[Path]:
        """Render machine-readable artifacts for one table profile."""
        written: list[Path] = []
        artifacts_dir = out_dir / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        table = result.spec.table

        if "json" in formats:
            json_path = artifacts_dir / f"{table}.stats.json"
            json_path.write_text(
                json.dumps(result_to_dict(result), indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            written.append(json_path)

        if "parquet" in formats:
            parquet_path = self._write_parquet(result, artifacts_dir / f"{table}.dist.parquet")
            if parquet_path is not None:
                written.append(parquet_path)

        return written

    def _write_parquet(self, result: ProfileResult, path: Path) -> Path | None:
        """Flatten every check's rows into one tidy Parquet table.

        Returns ``None`` (logged) when ``pyarrow`` is not installed so the run
        continues with the JSON artifact alone.
        """
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            logger.warning(
                "pyarrow not installed — skipping Parquet for %s (install the "
                "'analysis' extra). JSON artifact still written.",
                result.spec.table,
            )
            return None

        records: list[dict] = []
        for check in result.checks:
            for row in check.rows:
                records.append(
                    {
                        "table": result.spec.table,
                        "target": result.target,
                        "check_kind": check.kind.value,
                        "drill_value": None,
                        "sampled": check.sampled,
                        **{k: _scalarize(v) for k, v in row_to_jsonable(row).items()},
                    }
                )
        for drill_value, checks in result.drilldown.items():
            for check in checks:
                for row in check.rows:
                    records.append(
                        {
                            "table": result.spec.table,
                            "target": result.target,
                            "check_kind": check.kind.value,
                            "drill_value": drill_value,
                            "sampled": check.sampled,
                            **{k: _scalarize(v) for k, v in row_to_jsonable(row).items()},
                        }
                    )

        if not records:
            return None

        # Union of keys → consistent columns; missing keys become None.
        all_keys: list[str] = []
        seen: set[str] = set()
        for rec in records:
            for key in rec:
                if key not in seen:
                    seen.add(key)
                    all_keys.append(key)
        columns = {key: [_to_str(rec.get(key)) for rec in records] for key in all_keys}
        table = pa.table(columns)
        pq.write_table(table, path)
        return path


def _scalarize(value: object) -> object:
    """Coerce nested containers to a stable scalar for columnar storage."""
    if isinstance(value, list | dict):
        return json.dumps(value, ensure_ascii=False)
    return value


def _to_str(value: object) -> str | None:
    """Render every artifact cell as text for a schema-stable Parquet table."""
    if value is None:
        return None
    return str(value)
