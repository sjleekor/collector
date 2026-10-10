"""기준선 저장소 — 경로·원문 보관·완료 manifest 읽기.

계약은 계획 04 §3.1이다. 핵심은 한 줄이다. **파일은 추가만 하고, 읽는 쪽은
완료 manifest가 가리키는 파일만 본다.** 중간에 죽어도 manifest 없는 parquet은
없는 것이라, 이어 돌려도 관측이 빠지거나 겹치지 않는다.

쓰는 순서(R06): ① 원문(``.tmp`` → rename) ② 관측·요청 기록 parquet(새 파일,
``.tmp`` → rename) ③ 완료 manifest를 **마지막에**. 락은 셸 wrapper(flock)가
맡으므로 여기에는 락이 없다.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from collector.kr.baseline.spec import FETCHED_AT_BASES, OBS_ABSENT, TableSpec
from collector.lake import DataRoot

BASELINE_DIRNAME = "krx_baseline"
RAW_DIR = "raw_responses"
FETCH_LOG_DIR = "fetch_log"
MANIFEST_DIR = "_manifest"

_SEGMENT = re.compile(r"^[A-Za-z0-9=_.\-]+$")
_TS_FORMAT = "%Y%m%dT%H%M%S%fZ"
_RAW_NAME = re.compile(r"^(?P<ts>\d{8}T\d{12}Z)_(?P<sha>[0-9a-f]{12})\.(?P<ext>[A-Za-z0-9]+)\.gz$")

#: 테스트가 중단 지점을 만들 때 쓰는 훅 이름.
HOOK_RAW_WRITTEN = "raw_written"
HOOK_PARQUET_WRITTEN = "parquet_written"
HOOK_MANIFEST_TMP_WRITTEN = "manifest_tmp_written"


@dataclass(frozen=True)
class RawRef:
    """저장된 원문 하나. ``sha256``은 **압축 전 본문 바이트**의 해시다."""

    path: str  # 기준 디렉터리 기준 상대 경로(posix)
    sha256: str
    fetched_at: datetime
    fetched_at_basis: str


@dataclass(frozen=True)
class OrphanRaw:
    """어떤 완료 manifest의 요청 기록에도 없는 원문 파일."""

    service: str
    request_key: str
    path: str
    fetched_at: datetime
    sha12: str


def format_ts(moment: datetime) -> str:
    if moment.tzinfo is None:
        raise ValueError("fetched_at은 시간대가 있어야 합니다 (UTC aware)")
    return moment.astimezone(UTC).strftime(_TS_FORMAT)


def parse_ts(text: str) -> datetime:
    return datetime.strptime(text, _TS_FORMAT).replace(tzinfo=UTC)


_SERVICE = re.compile(r"^[a-z0-9_]+$")


def check_service(service: str) -> str:
    """서비스 이름은 ``[a-z0-9_]+``. 원문 경로의 첫 조각이라 ``/``가 들어가면 안 된다."""
    if not _SERVICE.match(service):
        raise ValueError(f"서비스 이름은 [a-z0-9_]+ 만 씁니다: {service!r}")
    return service


def request_key_dir(request_key: str) -> Path:
    """요청 키를 디렉터리로. 바꾸지 않고 검사만 한다 — 되돌릴 수 있어야 해서다."""
    segments = request_key.split("/")
    for segment in segments:
        if segment in {".", ".."} or not _SEGMENT.match(segment):
            raise ValueError(f"요청 키에 쓸 수 없는 문자가 있습니다: {request_key!r}")
    return Path(*segments)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


class BaselineStore:
    """``<DataRoot.raw>/krx_baseline`` 아래의 읽기·원문 쓰기.

    Args:
        base: 기준 디렉터리. 없으면 ``DataRoot.resolve("kr").raw / "krx_baseline"``.
            시험에서는 ``tmp_path``를 직접 준다.
        hook: 쓰는 도중 단계 이름을 받는 함수. 시험이 예외를 던져 중단을 흉내 낸다.
    """

    def __init__(self, base: Path | None = None, *, hook: Callable[[str], None] | None = None):
        self.base = Path(base) if base is not None else self.default_base()
        self.hook = hook
        #: parquet을 읽을 때마다 ``(상대 경로, 읽은 열)``을 쌓는다. 무엇을 읽는지
        #: 시험이 확인하려는 것이다(상태를 연도·열 단위로만 읽어야 한다).
        self.reads: list[tuple[str, tuple[str, ...] | None]] = []

    @staticmethod
    def default_base() -> Path:
        return DataRoot.resolve("kr").raw / BASELINE_DIRNAME

    def _fire(self, stage: str) -> None:
        if self.hook is not None:
            self.hook(stage)

    # ------------------------------------------------------------------ 원문

    def write_raw(
        self,
        service: str,
        request_key: str,
        body: bytes,
        fetched_at: datetime,
        *,
        ext: str = "json",
    ) -> RawRef:
        """받은 바이트를 gzip(mtime=0)해 둔다. 같은 파일이 있으면 다시 쓰지 않는다.

        gzip 머리에 시각이 들어가면 같은 본문이 매번 다른 파일이 된다. 그래서
        ``mtime=0``으로 재현 가능하게 만든다.
        """
        gz = gzip.compress(body, compresslevel=6, mtime=0)
        return self._place_raw(
            service, request_key, gz, hashlib.sha256(body).hexdigest(), fetched_at, "response", ext
        )

    def import_gzip_raw(
        self,
        service: str,
        request_key: str,
        gz_bytes: bytes,
        fetched_at: datetime,
        fetched_at_basis: str,
        *,
        ext: str = "json",
    ) -> RawRef:
        """이미 gzip된 원문(조사 사본)을 **바이트 그대로** 넣는다.

        ``fetched_at``과 ``fetched_at_basis``(``response``·``file_mtime``)는 호출자가
        준다. 조사 사본에는 응답 시각이 없어 파일 시각을 쓰는 경우가 있기 때문이다.
        """
        if fetched_at_basis not in FETCHED_AT_BASES:
            raise ValueError(f"fetched_at_basis는 {FETCHED_AT_BASES} 중 하나입니다")
        body = gzip.decompress(gz_bytes)
        return self._place_raw(
            service,
            request_key,
            gz_bytes,
            hashlib.sha256(body).hexdigest(),
            fetched_at,
            fetched_at_basis,
            ext,
        )

    def _place_raw(
        self,
        service: str,
        request_key: str,
        gz: bytes,
        sha256: str,
        fetched_at: datetime,
        basis: str,
        ext: str,
    ) -> RawRef:
        directory = Path(RAW_DIR) / check_service(service) / request_key_dir(request_key)
        rel = directory / f"{format_ts(fetched_at)}_{sha256[:12]}.{ext}.gz"
        dest = self.base / rel
        if not dest.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_name(dest.name + ".tmp")
            tmp.write_bytes(gz)
            os.replace(tmp, dest)
        self._fire(HOOK_RAW_WRITTEN)
        return RawRef(rel.as_posix(), sha256, fetched_at.astimezone(UTC), basis)

    def read_raw(self, rel_path: str) -> bytes:
        """저장된 원문을 풀어 받은 본문 바이트를 돌려준다."""
        return gzip.decompress((self.base / rel_path).read_bytes())

    # --------------------------------------------------------------- manifest

    def manifests(self) -> list[dict[str, Any]]:
        """완료 manifest 전부. 이름순(= 실행 ID·시도·묶음 순)."""
        directory = self.base / MANIFEST_DIR
        if not directory.is_dir():
            return []
        return [json.loads(p.read_text("utf-8")) for p in sorted(directory.glob("*.json"))]

    def committed_paths(
        self, kind: str, name: str | None = None, *, year: str | None = None
    ) -> list[Path]:
        """완료 manifest가 가리키는 파일 경로. ``kind``는 observation·fetch_log·raw.

        ``year``를 주면 경로의 ``year=YYYY`` 파티션으로 파일을 고른다.
        """
        out: list[Path] = []
        for manifest in self.manifests():
            for entry in manifest["files"]:
                if entry["kind"] != kind:
                    continue
                if name is not None and entry.get("table", entry.get("service")) != name:
                    continue
                if year is not None and f"/year={year}/" not in entry["path"]:
                    continue
                out.append(self.base / entry["path"])
        return out

    def _read_parquets(
        self,
        paths: Iterable[Path],
        schema: pa.Schema | None,
        columns: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        cols = list(columns) if columns is not None else None
        tables = []
        for path in paths:
            self.reads.append(
                (path.relative_to(self.base).as_posix(), tuple(cols) if cols else None)
            )
            # 경로의 year=를 컬럼으로 읽지 않는다
            tables.append(pq.ParquetFile(path).read(columns=cols))
        if not tables:
            empty = schema.empty_table() if schema is not None else None
            if empty is not None and cols is not None:
                empty = empty.select(cols)
            return empty.to_pandas() if empty is not None else pd.DataFrame()
        return pa.concat_tables(tables).to_pandas()

    # ------------------------------------------------------------------- 읽기

    def read_observations(
        self,
        spec: TableSpec,
        view: str = "all",
        *,
        year: str | None = None,
        columns: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """``all`` 전체 관측 · ``first`` 최초(``obs_seq=1``) · ``latest`` 키별 최신.

        ``latest``는 키별 최대 ``obs_seq``이고 그것이 ``absent``면 뺀다.
        ``year``·``columns``로 읽을 파일과 열을 줄일 수 있다(``columns``는 ``all``만,
        뷰 계산에 쓰는 ``obs_seq``·``obs_kind``와 키 칸은 알아서 넣지 않는다).
        """
        frame = self._read_parquets(
            self.committed_paths("observation", spec.name, year=year),
            spec.arrow_schema(),
            columns,
        )
        if view == "all":
            return frame
        if view == "first":
            return frame[frame["obs_seq"] == 1].reset_index(drop=True)
        if view == "latest":
            if frame.empty:
                return frame
            keys = list(spec.key_columns)
            top = frame.sort_values("obs_seq").drop_duplicates(keys, keep="last")
            return top[top["obs_kind"] != OBS_ABSENT].reset_index(drop=True)
        raise ValueError(f"view는 all·first·latest 중 하나입니다: {view!r}")

    def read_fetch_log(
        self, service: str | None = None, columns: Sequence[str] | None = None
    ) -> pd.DataFrame:
        return self._read_parquets(
            self.committed_paths("fetch_log", service), FETCH_LOG_SCHEMA, columns
        )

    # ------------------------------------------------------------ 점검·복구

    def unreferenced_files(self) -> dict[str, list[str]]:
        """완료 manifest가 가리키지 않는 parquet·원문. 지우지 않고 알려만 준다."""
        referenced = {entry["path"] for manifest in self.manifests() for entry in manifest["files"]}
        out: dict[str, list[str]] = {"parquet": [], "raw": []}
        for path in sorted(self.base.rglob("*")):
            if not path.is_file() or path.suffix == ".tmp":
                continue
            rel = path.relative_to(self.base).as_posix()
            if rel in referenced or rel.startswith(MANIFEST_DIR + "/"):
                continue
            if rel.startswith(RAW_DIR + "/"):
                out["raw"].append(rel)
            elif path.suffix == ".parquet":
                out["parquet"].append(rel)
        return out

    def verify_committed(self) -> list[str]:
        """manifest에 적힌 sha256·바이트와 다른(또는 없는) 파일 목록. 비면 정상."""
        bad: list[str] = []
        for manifest in self.manifests():
            for entry in manifest["files"]:
                path = self.base / entry["path"]
                if not path.is_file():
                    bad.append(f"{entry['path']}: 없음")
                elif path.stat().st_size != entry["bytes"] or sha256_file(path) != entry["sha256"]:
                    bad.append(f"{entry['path']}: sha256 불일치")
        return bad

    def find_orphan_raw(self) -> list[OrphanRaw]:
        """원문은 있는데 완료 요청 기록에 없는 파일. 오래된 순.

        중간에 죽으면 이런 파일이 남는다. 다시 받지 않고 원문에서 정규화한다.
        ``fetched_at``은 파일 이름의 시각이다.
        """
        log = self.read_fetch_log(columns=["raw_path"])
        done = set(log["raw_path"].dropna()) if not log.empty else set()
        root = self.base / RAW_DIR
        found: list[OrphanRaw] = []
        if not root.is_dir():
            return found
        for path in root.rglob("*.gz"):
            rel = path.relative_to(self.base).as_posix()
            match = _RAW_NAME.match(path.name)
            if rel in done or match is None:
                continue
            parts = path.relative_to(root).parts
            if len(parts) < 3:
                continue
            found.append(
                OrphanRaw(
                    service=parts[0],
                    request_key="/".join(parts[1:-1]),
                    path=rel,
                    fetched_at=parse_ts(match["ts"]),
                    sha12=match["sha"],
                )
            )
        return sorted(found, key=lambda o: (o.fetched_at, o.path))

    def next_attempt(self, run_id: str) -> int:
        """이 ``run_id``로 쓴 어떤 파일(미완료 포함)과도 겹치지 않는 시도 번호."""
        pattern = re.compile(rf"^{re.escape(run_id)}__(\d+)__\d+\.(?:parquet|json)$")
        highest = 0
        for path in self.base.rglob(f"{run_id}__*"):
            if RAW_DIR in path.parts:
                continue
            match = pattern.match(path.name)
            if match:
                highest = max(highest, int(match[1]))
        return highest + 1


FETCH_LOG_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string()),
        pa.field("attempt", pa.int64()),
        pa.field("service", pa.string()),
        pa.field("request_key", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("http_status", pa.int64()),
        pa.field("raw_path", pa.string()),
        pa.field("raw_sha256", pa.string()),
        pa.field("result", pa.string()),
        pa.field("day_kind", pa.string()),
        pa.field("row_count", pa.int64()),
        pa.field("priced_rows", pa.int64()),
        pa.field("netasst_zero_rows", pa.int64()),
        pa.field("required_missing", pa.int64()),
        pa.field("error", pa.string()),
    ]
)
