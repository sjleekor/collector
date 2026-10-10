"""관측 기록기 — 요청마다 ``obs_seq``를 매기고 묶음 단위로 확정한다.

**관측 규칙(계획 04 §3).** 키의 *최신 관측*과 ``row_hash``를 비교한다.

* 처음이면 ``obs_seq=1``, 다르면 ``+1``, 같으면 관측 행을 만들지 않고 요청
  기록에만 ``same``으로 남긴다. A → B → A의 마지막 A는 새 관측이다.
* 완료된 응답 범위(``scope``) 안에서 이전에 있던 키가 빠지면 ``absent`` 관측을
  더한다. 최신이 이미 ``absent``면 또 넣지 않는다. 불완전한 응답이나 빈 응답은
  근거가 못 된다.

**확정 단위.** 요청이 ``batch_size``(기본 200)개 쌓이면 관측·요청 기록·완료
manifest를 확정한다. 한 번 쓴 파일은 고치지 않고, 같은 이름이 이미 있으면
덮지 않고 멈춘다.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from collector.kr.baseline.spec import (
    OBS_ABSENT,
    OBS_VALUE,
    ParsedResponse,
    TableSpec,
    row_hash,
)
from collector.kr.baseline.store import (
    FETCH_LOG_DIR,
    FETCH_LOG_SCHEMA,
    HOOK_MANIFEST_TMP_WRITTEN,
    HOOK_PARQUET_WRITTEN,
    MANIFEST_DIR,
    BaselineStore,
    OrphanRaw,
    RawRef,
    sha256_file,
)

DEFAULT_BATCH_SIZE = 200

RESULT_NEW = "new_obs"
RESULT_SAME = "same"
RESULT_FAILED = "failed"


@dataclass(slots=True)
class _State:
    seq: int
    kind: str
    hash: str


class BaselineWriter:
    """한 시도(``run_id`` + ``attempt``)의 기록기.

    Args:
        store: 저장소.
        run_id: 실행 ID. ``reobserve``의 재개 판단 단위다.
        attempt: 시도 번호. ``store.next_attempt(run_id)``를 쓰면 겹치지 않는다.
        source_run: 출처 이름(``daily``·``backfill_20261022``·``import_research_pull``).
        batch_size: 이만큼 요청이 쌓이면 확정한다.
    """

    def __init__(
        self,
        store: BaselineStore,
        run_id: str,
        attempt: int,
        source_run: str,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        if "__" in run_id or "/" in run_id:
            raise ValueError("run_id에 '__'나 '/'를 쓰지 않습니다 (파일 이름 구분자)")
        self.store = store
        self.run_id = run_id
        self.attempt = attempt
        self.source_run = source_run
        self.batch_size = max(1, batch_size)
        self._batch_no = 0
        self._state: dict[str, dict[tuple[str, ...], _State]] = {}
        self._scope_index: dict[tuple[str, tuple[str, ...]], dict[tuple[str, ...], set]] = {}
        self._reset_pending()

    # ------------------------------------------------------------ 내부 상태

    def _reset_pending(self) -> None:
        self._obs: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        self._specs: dict[str, TableSpec] = {}
        self._log: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._raws: dict[str, RawRef] = {}
        self._http = 0
        self._slots: set[int] = set()
        self._requests = 0

    @property
    def pending_requests(self) -> int:
        """아직 확정되지 않은 요청 수."""
        return self._requests

    def _latest(self, spec: TableSpec) -> dict[tuple[str, ...], _State]:
        """완료 manifest가 가리키는 관측만으로 키별 최신 상태를 만든다(한 번만)."""
        if spec.name not in self._state:
            frame = self.store.read_observations(spec, "all")
            state: dict[tuple[str, ...], _State] = {}
            if not frame.empty:
                keys = list(spec.key_columns)
                top = frame.sort_values("obs_seq").drop_duplicates(keys, keep="last")
                for rec in top[[*keys, "obs_seq", "obs_kind", "row_hash"]].itertuples(index=False):
                    state[tuple(rec[: len(keys)])] = _State(
                        int(rec[len(keys)]), rec[len(keys) + 1], rec[len(keys) + 2]
                    )
            self._state[spec.name] = state
        return self._state[spec.name]

    def _index(self, spec: TableSpec, scope_cols: tuple[str, ...]) -> dict:
        slot = (spec.name, scope_cols)
        if slot not in self._scope_index:
            positions = [spec.key_columns.index(c) for c in scope_cols]
            index: dict[tuple[str, ...], set] = defaultdict(set)
            for key in self._latest(spec):
                index[tuple(key[i] for i in positions)].add(key)
            self._scope_index[slot] = index
        return self._scope_index[slot]

    def _set_state(self, spec: TableSpec, key: tuple[str, ...], state: _State) -> None:
        self._latest(spec)[key] = state
        for (name, scope_cols), index in self._scope_index.items():
            if name == spec.name:
                positions = [spec.key_columns.index(c) for c in scope_cols]
                index[tuple(key[i] for i in positions)].add(key)

    def _invalidate(self) -> None:
        """확정에 실패하면 메모리 상태를 버린다. 다음에 완료 manifest에서 다시 읽는다."""
        self._state.clear()
        self._scope_index.clear()
        self._reset_pending()

    # ------------------------------------------------------------- 요청 추가

    def add_response(
        self,
        *,
        service: str,
        request_key: str,
        raw: RawRef,
        parsed: ParsedResponse,
        http_status: int = 200,
        http_requests: int = 1,
        key_slot: int | None = None,
    ) -> str:
        """응답 하나를 관측으로 정규화해 쌓는다. ``new_obs``·``same``을 돌려준다.

        ``http_requests``는 이 요청에 쓴 실제 HTTP 횟수(재시도 포함)이고 manifest의
        실측 HTTP 수로 합쳐진다. 요청 수가 ``batch_size``에 닿으면 확정한다.
        """
        spec = parsed.spec
        self._specs[spec.name] = spec
        emitted = self._observe(spec, parsed, raw)
        result = RESULT_NEW if emitted else RESULT_SAME
        self._push_log(
            service=service,
            request_key=request_key,
            fetched_at=raw.fetched_at,
            http_status=http_status,
            raw=raw,
            result=result,
            parsed=parsed,
            error=None,
        )
        self._raws[raw.path] = raw
        self._http += http_requests
        if key_slot is not None:
            self._slots.add(key_slot)
        self._maybe_commit()
        return result

    def add_failure(
        self,
        *,
        service: str,
        request_key: str,
        fetched_at: datetime,
        error: str,
        http_status: int | None = None,
        raw: RawRef | None = None,
        http_requests: int = 1,
        key_slot: int | None = None,
    ) -> None:
        """실패한 요청을 기록한다. 관측은 만들지 않고 그 날짜를 완료로 치지 않는다.

        ``error``는 짧게 자른다. 키·토큰 같은 비밀값을 넣지 않는다.
        """
        self._push_log(
            service=service,
            request_key=request_key,
            fetched_at=fetched_at,
            http_status=http_status,
            raw=raw,
            result=RESULT_FAILED,
            parsed=None,
            error=error,
        )
        if raw is not None:
            self._raws[raw.path] = raw
        self._http += http_requests
        if key_slot is not None:
            self._slots.add(key_slot)
        self._maybe_commit()

    def _push_log(
        self,
        *,
        service: str,
        request_key: str,
        fetched_at: datetime,
        http_status: int | None,
        raw: RawRef | None,
        result: str,
        parsed: ParsedResponse | None,
        error: str | None,
    ) -> None:
        self._log[service].append(
            {
                "run_id": self.run_id,
                "attempt": self.attempt,
                "service": service,
                "request_key": request_key,
                "fetched_at": fetched_at.astimezone(UTC),
                "http_status": http_status,
                "raw_path": raw.path if raw else None,
                "raw_sha256": raw.sha256 if raw else None,
                "result": result,
                "day_kind": parsed.day_kind if parsed else None,
                "row_count": (
                    (len(parsed.rows) if parsed.row_count is None else parsed.row_count)
                    if parsed
                    else None
                ),
                "priced_rows": parsed.priced_rows if parsed else None,
                "netasst_zero_rows": parsed.netasst_zero_rows if parsed else None,
                "required_missing": parsed.required_missing if parsed else None,
                "error": (error or "")[:300] or None,
            }
        )
        self._requests += 1

    # -------------------------------------------------------------- 관측 비교

    def _observe(self, spec: TableSpec, parsed: ParsedResponse, raw: RawRef) -> int:
        """새 관측 행을 쌓고 개수를 돌려준다."""
        state = self._latest(spec)
        scope_cols = tuple(parsed.scope)
        if not set(scope_cols) <= set(spec.key_columns):
            raise ValueError(f"{spec.name}: scope 칸은 key_columns 안에 있어야 합니다")
        if parsed.complete and not scope_cols:
            raise ValueError(f"{spec.name}: 완료 응답에는 scope가 필요합니다 (absent 범위)")
        allowed = set(spec.data_columns)

        seen: set[tuple[str, ...]] = set()
        new_rows: list[tuple[tuple[str, ...], dict[str, Any], str, int, str]] = []
        for row in parsed.rows:
            unknown = set(row) - allowed
            if unknown:
                raise ValueError(f"{spec.name}: 표에 없는 칸입니다: {sorted(unknown)}")
            key = self._key_of(spec, row)
            for col in scope_cols:
                if row[col] != parsed.scope[col]:
                    raise ValueError(f"{spec.name}: 행이 scope({col})를 벗어났습니다")
            if key in seen:
                raise ValueError(f"{spec.name}: 한 응답에 같은 키가 둘 있습니다: {key}")
            seen.add(key)
            digest = row_hash(spec, row)
            prev = state.get(key)
            if prev is None:
                seq = 1
            elif prev.kind == OBS_ABSENT or prev.hash != digest:
                seq = prev.seq + 1
            else:
                continue
            new_rows.append((key, dict(row), OBS_VALUE, seq, digest))

        if parsed.complete and (parsed.rows or parsed.absent_on_empty):
            scope_key = tuple(parsed.scope[c] for c in scope_cols)
            for key in sorted(self._index(spec, scope_cols).get(scope_key, ())):
                prev = state[key]
                if key in seen or prev.kind == OBS_ABSENT:
                    continue
                blank = dict(zip(spec.key_columns, key, strict=True))
                new_rows.append((key, blank, OBS_ABSENT, prev.seq + 1, row_hash(spec, blank)))

        for key, row, kind, seq, digest in new_rows:
            out = {col: row.get(col) for col in spec.data_columns}
            out.update(
                fetched_at=raw.fetched_at,
                fetched_at_basis=raw.fetched_at_basis,
                obs_seq=seq,
                obs_kind=kind,
                source_run=self.source_run,
                run_id=self.run_id,
                attempt=self.attempt,
                raw_sha256=raw.sha256,
                raw_path=raw.path,
                row_hash=digest,
            )
            year = self._year_of(spec, key)
            self._obs[(spec.name, year)].append(out)
            self._set_state(spec, key, _State(seq, kind, digest))
        return len(new_rows)

    @staticmethod
    def _key_of(spec: TableSpec, row: Mapping[str, Any]) -> tuple[str, ...]:
        key = []
        for col in spec.key_columns:
            value = row.get(col)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{spec.name}.{col}: 키 칸은 비지 않은 문자열이어야 합니다")
            key.append(value)
        return tuple(key)

    @staticmethod
    def _year_of(spec: TableSpec, key: tuple[str, ...]) -> str:
        text = key[spec.key_columns.index(spec.date_column)][:4]
        if not (len(text) == 4 and text.isdigit()):
            raise ValueError(f"{spec.name}.{spec.date_column}: 연도를 읽을 수 없습니다")
        return text

    # ------------------------------------------------------------------ 확정

    def _maybe_commit(self) -> None:
        if self._requests >= self.batch_size:
            self.flush()

    def flush(self) -> Path | None:
        """쌓인 것을 확정한다. 없으면 아무것도 쓰지 않고 ``None``.

        순서: 관측 parquet → 요청 기록 parquet → (훅) → manifest ``.tmp`` → (훅) →
        rename. manifest가 마지막이라 중간에 죽으면 이번 묶음은 없던 것이 된다.
        실패하면 메모리 상태를 버리고 예외를 그대로 올린다.
        """
        if self._requests == 0:
            return None
        self._batch_no += 1
        stem = f"{self.run_id}__{self.attempt:03d}__{self._batch_no:05d}"
        try:
            path = self._commit(stem)
        except BaseException:
            self._invalidate()
            raise
        self._reset_pending()
        return path

    def _commit(self, stem: str) -> Path:
        base = self.store.base
        entries: list[dict[str, Any]] = []

        for (table, year), rows in sorted(self._obs.items()):
            spec = self._specs[table]
            rel = Path(table) / f"year={year}" / f"{stem}.parquet"
            data = pa.Table.from_pylist(rows, schema=spec.arrow_schema())
            entries.append(self._write_parquet(rel, data, "observation", table=table))

        for service, rows in sorted(self._log.items()):
            rel = Path(FETCH_LOG_DIR) / service / f"{stem}.parquet"
            data = pa.Table.from_pylist(rows, schema=FETCH_LOG_SCHEMA)
            entries.append(self._write_parquet(rel, data, "fetch_log", service=service))

        for raw in sorted(self._raws.values(), key=lambda r: r.path):
            path = base / raw.path
            entries.append(
                {
                    "kind": "raw",
                    "path": raw.path,
                    "sha256": sha256_file(path),
                    "bytes": path.stat().st_size,
                    "body_sha256": raw.sha256,
                    "fetched_at_basis": raw.fetched_at_basis,
                }
            )
        self.store._fire(HOOK_PARQUET_WRITTEN)

        manifest = {
            "schema_version": 1,
            "run_id": self.run_id,
            "attempt": self.attempt,
            "batch": self._batch_no,
            "source_run": self.source_run,
            "completed_at": datetime.now(UTC).isoformat(),
            "request_count": self._requests,
            "http_requests": self._http,
            "key_slots": sorted(self._slots),
            "files": entries,
        }
        dest = base / MANIFEST_DIR / f"{stem}.json"
        if dest.exists():
            raise FileExistsError(f"{dest}: 이미 있는 manifest는 고치지 않습니다")
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), "utf-8")
        self.store._fire(HOOK_MANIFEST_TMP_WRITTEN)
        os.replace(tmp, dest)
        return dest

    def _write_parquet(self, rel: Path, data: pa.Table, kind: str, **tag: str) -> dict[str, Any]:
        dest = self.store.base / rel
        if dest.exists():
            raise FileExistsError(f"{dest}: 한 번 쓴 파일은 고치지 않습니다")
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        pq.write_table(data, tmp, compression="zstd")
        os.replace(tmp, dest)
        return {
            "kind": kind,
            "path": rel.as_posix(),
            "sha256": sha256_file(dest),
            "bytes": dest.stat().st_size,
            "rows": data.num_rows,
            **tag,
        }

    # ------------------------------------------------------------------ 복구

    def recover_orphans(
        self,
        parse: Callable[[OrphanRaw, bytes], ParsedResponse],
        *,
        fetched_at_basis: str = "response",
    ) -> int:
        """원문만 남은 요청을 **다시 받지 않고** 원문에서 정규화해 쌓는다.

        ``parse(orphan, body) -> ParsedResponse``는 호출자(파서)가 준다.
        ``fetched_at``은 파일 이름의 시각이다. 새 요청을 받기 **전에** 불러야
        한다 — 오래된 응답이 최신 관측보다 뒤에 끼면 순번이 시간순이 아니게 된다.
        파일 이름에는 ``fetched_at_basis``가 없어 호출자가 준다(기본 ``response``).
        """
        count = 0
        for orphan in self.store.find_orphan_raw():
            body = self.store.read_raw(orphan.path)
            parsed = parse(orphan, body)
            raw = RawRef(
                orphan.path, hashlib.sha256(body).hexdigest(), orphan.fetched_at, fetched_at_basis
            )
            self.add_response(
                service=orphan.service,
                request_key=orphan.request_key,
                raw=raw,
                parsed=parsed,
                http_requests=0,
            )
            count += 1
        return count
