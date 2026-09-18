"""parquet 쓰기·스냅샷·manifest.

쓰기는 :func:`collector.us.store.writer.write_snapshot` 하나로 모은다 —
``observed_at`` 강제를 한 자리에서 한다 (미국 계획 03 §3).
"""

from collector.us.store.writer import (
    MissingProvenanceError,
    UnknownTableError,
    read_snapshot,
    snapshot_path,
    write_snapshot,
)

__all__ = [
    "MissingProvenanceError",
    "UnknownTableError",
    "read_snapshot",
    "snapshot_path",
    "write_snapshot",
]
