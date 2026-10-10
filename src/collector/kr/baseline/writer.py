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
from collections import OrderedDict, defaultdict
from collections.abc import Callable, Collection, Mapping, Sequence
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
    check_service,
    sha256_file,
)

DEFAULT_BATCH_SIZE = 200
#: 한 번에 메모리에 둘 (표, 연도) 상태 수.
MAX_LOADED_YEARS = 3
#: 확정 전 관측 행이 이만큼 쌓이면 요청 수와 관계없이 확정한다(메모리 상한).
DEFAULT_MAX_PENDING_ROWS = 50_000

RESULT_NEW = "new_obs"
RESULT_SAME = "same"
RESULT_FAILED = "failed"


@dataclass(frozen=True, slots=True)
class WindowResponse:
    """창을 이루는 응답 하나(건수 또는 페이지). 요청 기록 한 줄이 된다."""

    request_key: str
    raw: RawRef | None
    http_status: int | None = 200
    row_count: int | None = None
    error: str | None = None


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
        max_pending_rows: int = DEFAULT_MAX_PENDING_ROWS,
    ) -> None:
        if "__" in run_id or "/" in run_id:
            raise ValueError("run_id에 '__'나 '/'를 쓰지 않습니다 (파일 이름 구분자)")
        self.store = store
        self.run_id = run_id
        self.attempt = attempt
        self.source_run = source_run
        self.batch_size = max(1, batch_size)
        self.max_pending_rows = max(1, max_pending_rows)
        #: 행 수 기준으로 확정한 횟수(시험·로그용).
        self.row_flushes = 0
        self._batch_no = 0
        self.recovery_errors: list[str] = []
        # 상태는 (표, 연도)별로 필요할 때만 읽는다. 키·obs_seq·obs_kind·row_hash 열만.
        self._state: OrderedDict[tuple[str, str], dict[tuple[str, ...], _State]] = OrderedDict()
        self._scope_index: dict[tuple[str, str, tuple[str, ...]], dict[tuple[str, ...], set]] = {}
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

    def _year_state(self, spec: TableSpec, year: str) -> dict[tuple[str, ...], _State]:
        """한 (표, 연도)의 키별 최신 상태. 완료 manifest의 그 연도 파일에서만 읽는다.

        **왜 연도·열 단위인가.** ETF는 180만 행이라 표 전체를 모든 열로 읽으면 매일
        수백 MB~1GB를 쓴다. 날짜 요청은 한 연도 안이므로 그 연도의 파일만, 그중에서도
        비교에 필요한 키·``obs_seq``·``obs_kind``·``row_hash`` 열만 읽는다.
        """
        slot = (spec.name, year)
        if slot in self._state:
            self._state.move_to_end(slot)
            return self._state[slot]
        keys = list(spec.key_columns)
        wanted = [*keys, "obs_seq", "obs_kind", "row_hash"]
        frame = self.store.read_observations(spec, "all", year=year, columns=wanted)
        state: dict[tuple[str, ...], _State] = {}
        if not frame.empty:
            top = frame.sort_values("obs_seq").drop_duplicates(keys, keep="last")
            for rec in top[wanted].itertuples(index=False):
                state[tuple(rec[: len(keys)])] = _State(
                    int(rec[len(keys)]), rec[len(keys) + 1], rec[len(keys) + 2]
                )
        self._state[slot] = state
        self._evict(keep=slot)
        return state

    def _evict(self, keep: tuple[str, str]) -> None:
        """불러온 연도가 ``MAX_LOADED_YEARS``를 넘으면 **확정 안 된 행이 없는** 연도부터 내린다.

        import가 2010→2026 순서로 돌아도 메모리가 쌓이지 않는다. 확정 전 행이 있는
        연도는 메모리 상태가 디스크보다 앞서 있어 내리면 틀린 상태를 읽게 된다.
        """
        for slot in list(self._state):
            if len(self._state) <= MAX_LOADED_YEARS:
                break
            if slot == keep or slot in self._obs:
                continue
            del self._state[slot]
            for idx in [k for k in self._scope_index if k[:2] == slot]:
                del self._scope_index[idx]

    def _index(
        self, spec: TableSpec, year: str, scope_cols: tuple[str, ...]
    ) -> dict[tuple[str, ...], set]:
        slot = (spec.name, year, scope_cols)
        state = self._year_state(spec, year)
        if slot not in self._scope_index:
            positions = [spec.key_columns.index(c) for c in scope_cols]
            index: dict[tuple[str, ...], set] = defaultdict(set)
            for key in state:
                index[tuple(key[i] for i in positions)].add(key)
            self._scope_index[slot] = index
        return self._scope_index[slot]

    def _set_state(self, spec: TableSpec, key: tuple[str, ...], state: _State) -> None:
        year = self._year_of(spec, key)
        self._year_state(spec, year)[key] = state
        for (name, yr, scope_cols), index in self._scope_index.items():
            if name == spec.name and yr == year:
                positions = [spec.key_columns.index(c) for c in scope_cols]
                index[tuple(key[i] for i in positions)].add(key)

    def known_dates(self, spec: TableSpec, low: str, high: str) -> list[str]:
        """``low``~``high``(``date_column`` 값, 문자열 비교) 안에서 최신이 ``value``인
        키가 있는 날짜.

        SEIBro 창이 "이미 알던 기준일"을 찾을 때 쓴다. 연도별로 훑고 바로 내린다.
        """
        pos = spec.key_columns.index(spec.date_column)
        found: set[str] = set()
        for year in range(int(low[:4]), int(high[:4]) + 1):
            for key, state in self._year_state(spec, str(year)).items():
                if state.kind == OBS_VALUE and low <= key[pos] <= high:
                    found.add(key[pos])
        return sorted(found)

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
        check_service(service)
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
        check_service(service)
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
        day_kind: str | None = None,
        row_count: int | None = None,
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
                "day_kind": parsed.day_kind if parsed else day_kind,
                "row_count": (
                    (len(parsed.rows) if parsed.row_count is None else parsed.row_count)
                    if parsed
                    else row_count
                ),
                "priced_rows": parsed.priced_rows if parsed else None,
                "netasst_zero_rows": parsed.netasst_zero_rows if parsed else None,
                "required_missing": parsed.required_missing if parsed else None,
                "error": (error or "")[:300] or None,
            }
        )
        self._requests += 1

    def add_window(
        self,
        *,
        service: str,
        window_key: str,
        spec: TableSpec,
        rows: Sequence[Mapping[str, Any]],
        row_raw: Mapping[tuple[str, ...], RawRef],
        responses: Sequence[WindowResponse],
        complete: bool,
        low: str,
        high: str,
        fetched_at: datetime,
        http_requests: int,
        error: str | None = None,
    ) -> str:
        """응답 여러 개(건수·페이지)로 이뤄진 창을 **기준일별 scope**로 관측에 넣는다.

        SEIBro는 한 창이 여러 페이지 원문이고 사라짐 판단은 기준일 단위라, 응답 하나에
        관측 하나가 대응하는 ``add_response``로는 표현이 안 된다. 규칙:

        * 창이 완료면 (새 행의 기준일 ∪ ``low``~``high`` 안에서 이미 알던 기준일) 각각을
          ``complete=True, absent_on_empty=True``로 비교한다. 기준일 전체가 사라지면
          그 기준일의 키가 전부 ``absent``가 된다.
        * 미완료면 ``complete=False`` — 값만 넣고 ``absent``는 만들지 않는다.
        * 요청 기록은 응답마다 한 줄이고, 창 요약이 한 줄 더 있다
          (``request_key=window_key``, ``day_kind``는 ``complete``·``incomplete``).
          "마지막 완료 창"은 이 요약 줄에서 찾는다.
        """
        check_service(service)
        self._specs[spec.name] = spec
        date_col = spec.date_column
        by_date: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            by_date[row[date_col]].append(row)
        dates = set(by_date)
        if complete:
            dates |= set(self.known_dates(spec, low, high))
        default = next((r.raw for r in responses if r.raw is not None), None)
        emitted: list[str] = []
        for value in sorted(dates):
            parsed = ParsedResponse(
                spec, by_date.get(value, []), {date_col: value}, complete, absent_on_empty=True
            )
            if default is None and not parsed.rows:
                continue
            emitted += self._observe(spec, parsed, default or next(iter(row_raw.values())), row_raw)
        fresh = set(emitted)
        for resp in responses:
            if resp.raw is not None:
                self._raws[resp.raw.path] = resp.raw
            self._push_log(
                service=service,
                request_key=resp.request_key,
                fetched_at=resp.raw.fetched_at if resp.raw else fetched_at,
                http_status=resp.http_status,
                raw=resp.raw,
                result=(
                    RESULT_FAILED
                    if resp.error
                    else RESULT_NEW if resp.raw and resp.raw.path in fresh else RESULT_SAME
                ),
                parsed=None,
                error=resp.error,
                row_count=resp.row_count,
            )
        self._push_log(
            service=service,
            request_key=window_key,
            fetched_at=fetched_at,
            http_status=None,
            raw=None,
            result=RESULT_NEW if emitted else RESULT_SAME,
            parsed=None,
            error=error,
            day_kind="complete" if complete else "incomplete",
            row_count=len(rows),
        )
        self._http += http_requests
        self._maybe_commit()
        return RESULT_NEW if emitted else RESULT_SAME

    # -------------------------------------------------------------- 관측 비교

    def _observe(
        self,
        spec: TableSpec,
        parsed: ParsedResponse,
        raw: RawRef,
        row_raw: Mapping[tuple[str, ...], RawRef] | None = None,
    ) -> list[str]:
        """새 관측 행을 쌓고, 행마다 근거 원문 경로를 돌려준다(길이 = 새 관측 수).

        ``row_raw``가 있으면 행의 키로 그 행이 나온 원문을 고른다(SEIBro는 한 창이
        여러 페이지 원문으로 이뤄진다). 없는 키(``absent`` 포함)는 ``raw``를 쓴다.
        """
        scope_cols = tuple(parsed.scope)
        if not set(scope_cols) <= set(spec.key_columns):
            raise ValueError(f"{spec.name}: scope 칸은 key_columns 안에 있어야 합니다")
        if spec.date_column not in scope_cols:
            raise ValueError(
                f"{spec.name}: scope에 date_column({spec.date_column})이 있어야 합니다"
            )
        allowed = set(spec.data_columns)
        scope_year = parsed.scope[spec.date_column][:4]

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
            prev = self._year_state(spec, scope_year).get(key)
            if prev is None:
                seq = 1
            elif prev.kind == OBS_ABSENT or prev.hash != digest:
                seq = prev.seq + 1
            else:
                continue
            new_rows.append((key, dict(row), OBS_VALUE, seq, digest))

        if parsed.complete and (parsed.rows or parsed.absent_on_empty):
            scope_key = tuple(parsed.scope[c] for c in scope_cols)
            state = self._year_state(spec, scope_year)
            for key in sorted(self._index(spec, scope_year, scope_cols).get(scope_key, ())):
                prev = state[key]
                if key in seen or prev.kind == OBS_ABSENT:
                    continue
                blank = dict(zip(spec.key_columns, key, strict=True))
                new_rows.append((key, blank, OBS_ABSENT, prev.seq + 1, row_hash(spec, blank)))

        paths: list[str] = []
        for key, row, kind, seq, digest in new_rows:
            ref = (row_raw or {}).get(key, raw)
            out = {col: row.get(col) for col in spec.data_columns}
            out.update(
                fetched_at=ref.fetched_at,
                fetched_at_basis=ref.fetched_at_basis,
                obs_seq=seq,
                obs_kind=kind,
                source_run=self.source_run,
                run_id=self.run_id,
                attempt=self.attempt,
                raw_sha256=ref.sha256,
                raw_path=ref.path,
                row_hash=digest,
            )
            year = self._year_of(spec, key)
            self._obs[(spec.name, year)].append(out)
            self._set_state(spec, key, _State(seq, kind, digest))
            paths.append(ref.path)
        return paths

    @staticmethod
    def _key_of(spec: TableSpec, row: Mapping[str, Any]) -> tuple[str, ...]:
        key = []
        for col in spec.key_columns:
            value = row.get(col)
            if not isinstance(value, str) or (col == spec.date_column and not value):
                raise ValueError(
                    f"{spec.name}.{col}: 키 칸은 문자열이어야 하고 날짜 칸은 비면 안 됩니다"
                )
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
        """요청 수(``batch_size``) 또는 확정 전 관측 행 수(``max_pending_rows``)가 차면 확정한다.

        ETF 날짜 하나가 1,000행이라 요청 200개면 20만 행이 메모리에 쌓인다. 행 수
        기준이 따로 있어야 import·reobserve의 최대 메모리가 묶인다.
        """
        pending_rows = sum(len(rows) for rows in self._obs.values())
        if pending_rows > self.max_pending_rows:
            self.row_flushes += 1
            self.flush()
        elif self._requests >= self.batch_size:
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
        services: Collection[str] | None = None,
    ) -> int:
        """원문만 남은 요청을 **다시 받지 않고** 원문에서 정규화해 쌓는다.

        ``parse(orphan, body) -> ParsedResponse``는 호출자(파서)가 준다.
        ``fetched_at``은 파일 이름의 시각이다. 새 요청을 받기 **전에** 불러야
        한다 — 오래된 응답이 최신 관측보다 뒤에 끼면 순번이 시간순이 아니게 된다.
        파일 이름에는 ``fetched_at_basis``가 없어 호출자가 준다(기본 ``response``).
        ``services``를 주면 그 서비스의 원문만 본다(락 도메인이 다른 서비스를 건드리지 않게).

        파서가 못 읽는 원문은 건너뛰고 ``recovery_errors``에 적는다. 깨진 파일 하나가
        매일 잡을 막으면 안 되기 때문이다. 건너뛴 원문은 그대로 남아 verify가 센다.
        """
        count = 0
        self.recovery_errors: list[str] = []
        for orphan in self.store.find_orphan_raw():
            if services is not None and orphan.service not in services:
                continue
            body = self.store.read_raw(orphan.path)
            try:
                parsed = parse(orphan, body)
            except Exception as exc:  # noqa: BLE001 - 어떤 파서 오류든 건너뛴다
                self.recovery_errors.append(f"{orphan.path}: {type(exc).__name__}: {exc}"[:300])
                continue
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
