"""KRX 기준선 수집 — ETF 일봉·채권지수·파생상품지수(#10)의 sync·backfill·import.

저장 계약은 ``collector.kr.baseline``(계획 04 §3.1)이고, 여기는 **무엇을 언제 받을지**
를 정한다. 세 서비스가 같은 흐름이다.

    새 요청 전에 원문 복구 → 대상 날짜 계산 → 날짜마다
    fetch_raw → write_raw → 파서 → classify → add_response

**왜 정기 잡은 창 밖을 안 받나(R04).** 처음 판은 빈 저장소를 보면 2010년부터 받기
시작할 수 있었다. 정기 잡(``sync``)은 최근 평일 20일 창만 보고, 그보다 오래된 빈
날짜는 받지 않는다 — ``verify``가 실패로 알리고 ``backfill``이 메운다.

**``--max-calls``는 실제 HTTP 수다.** 날짜 수가 아니라 client 계수기(재시도·키 회전
포함)로 센다. 기존 ``index sync``의 ``max_calls``(날짜 작업 수)와 뜻이 다르다.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from collector.kr.adapters.krx_baseline_openapi import bond_index, derivative_index, etf_daily
from collector.kr.adapters.krx_baseline_openapi.day_status import DayStatus
from collector.kr.adapters.market_data_krx_openapi.client import (
    KrxOpenApiClient,
    KrxOpenApiEndpointNotApprovedError,
)
from collector.kr.baseline import (
    BaselineStore,
    BaselineWriter,
    OrphanRaw,
    ParsedResponse,
    TableSpec,
    completed_request_keys,
    fill_dates,
)
from collector.kr.baseline.pending import bas_dd_request_key, iter_weekdays
from collector.kr.util.pipeline import SourceAuthError, SourceQuotaExhaustedError
from collector.kr.util.time import KST, now_kst

logger = logging.getLogger(__name__)

#: 정기 잡이 보는 창(평일 수). 창 밖의 빈 날짜는 받지 않는다.
SYNC_WINDOW_WEEKDAYS = 20
DEFAULT_SYNC_MAX_CALLS = 60
DEFAULT_MAX_CONSECUTIVE_FAILURES = 5

#: 조사 사본 import의 고정 실행 ID. 조사 날짜라 상수로 둔다(2026-10-09 조사 사본).
IMPORT_RUN_ID = "import_research_pull_20261009"
IMPORT_SOURCE_RUN = "import_research_pull"


class BaselineImportError(RuntimeError):
    """조사 사본 import를 시작·계속할 수 없을 때. 아무것도 쓰기 전에 던진다."""


# ---------------------------------------------------------------- 서비스 설명


@dataclass(frozen=True)
class KrxBaselineService:
    """KRX 서비스 하나. ``service``가 원문 경로·요청 기록의 서비스 이름이다."""

    service: str
    spec: TableSpec
    group: str
    endpoint: str
    parse: Callable[[list[dict]], list[dict]]
    classify: Callable[[list[dict]], DayStatus]
    #: 백필 시작 기본값. 2010-01-04는 조사 사본의 데이터 시작일이라 상수로 둔다.
    history_start: date


def _parsed_types(
    names: tuple[str, ...], int_names: frozenset[str] = frozenset()
) -> dict[str, str]:
    out: dict[str, str] = {}
    for name in names:
        if name == "bas_date":
            out[name] = "date32"
        elif name in int_names:
            out[name] = "int64"
        else:
            out[name] = "float64"
    return out


_ETF_INT = frozenset(f"{c.lower()}_num" for c in etf_daily._INT_COLUMNS)  # noqa: SLF001

ETF_SPEC = TableSpec(
    name="etf_daily",
    key_columns=etf_daily.KEY_COLUMNS,
    source_columns=etf_daily.SOURCE_COLUMNS,
    date_column="BAS_DD",
    parsed_columns=_parsed_types(etf_daily.PARSED_COLUMNS, _ETF_INT),
)
BOND_SPEC = TableSpec(
    name="bond_index_daily",
    key_columns=bond_index.KEY_COLUMNS,
    source_columns=bond_index.SOURCE_COLUMNS,
    date_column="BAS_DD",
    parsed_columns=_parsed_types(bond_index.PARSED_COLUMNS),
)
DERIVATIVE_SPEC = TableSpec(
    name="derivative_index_daily",
    key_columns=derivative_index.KEY_COLUMNS,
    source_columns=derivative_index.SOURCE_COLUMNS,
    date_column="BAS_DD",
    parsed_columns=_parsed_types(derivative_index.PARSED_COLUMNS),
)

SERVICES: dict[str, KrxBaselineService] = {
    "etf_bydd_trd": KrxBaselineService(
        "etf_bydd_trd",
        ETF_SPEC,
        etf_daily.GROUP,
        etf_daily.ENDPOINT,
        etf_daily.parse_etf_rows,
        etf_daily.classify_etf_day,
        date(2010, 1, 4),
    ),
    "bon_dd_trd": KrxBaselineService(
        "bon_dd_trd",
        BOND_SPEC,
        bond_index.GROUP,
        bond_index.ENDPOINT,
        bond_index.parse_bond_rows,
        bond_index.classify_bond_day,
        date(2010, 1, 4),
    ),
    "drvprod_dd_trd": KrxBaselineService(
        "drvprod_dd_trd",
        DERIVATIVE_SPEC,
        derivative_index.GROUP,
        derivative_index.ENDPOINT,
        derivative_index.parse_derivative_rows,
        derivative_index.classify_derivative_day,
        date(2010, 1, 4),
    ),
}


def resolve_services(names: str | list[str] | None) -> list[KrxBaselineService]:
    """쉼표로 나눈 서비스 이름. 비면 셋 모두."""
    if names is None or names == "" or names == []:
        return list(SERVICES.values())
    items = names.split(",") if isinstance(names, str) else names
    chosen = [n.strip() for n in items if n.strip()]
    unknown = [n for n in chosen if n not in SERVICES]
    if unknown:
        raise ValueError(f"모르는 서비스: {unknown}; {sorted(SERVICES)} 중에서 고르십시오")
    return [SERVICES[n] for n in chosen]


# ------------------------------------------------------------------ 날짜 계산


def last_available_date(today: date | None = None) -> date:
    """어제(KST). 당일분은 나오지 않으므로 끝은 오늘을 넘지 않는다."""
    return (today or now_kst().date()) - timedelta(days=1)


def sync_window(end: date, weekdays: int = SYNC_WINDOW_WEEKDAYS) -> tuple[date, date]:
    """``end``로 끝나는 최근 평일 ``weekdays``일의 ``(시작, 끝)``."""
    day = end
    found = 0
    start = end
    while found < weekdays:
        if day.weekday() < 5:
            found += 1
            start = day
        day -= timedelta(days=1)
    return start, end


def sync_dates(store: BaselineStore, svc: KrxBaselineService, today: date) -> list[date]:
    """정기 ``sync`` 대상 — 최근 평일 20일 창 안의 빈·대기 날짜. **창 밖은 안 본다.**"""
    start, end = sync_window(last_available_date(today))
    return fill_dates(store, svc.service, max(start, svc.history_start), end)


def backfill_dates(
    store: BaselineStore,
    svc: KrxBaselineService,
    *,
    mode: str,
    start: date | None,
    end: date | None,
    run_id: str | None,
    today: date,
) -> list[date]:
    """``fill``은 빈·대기 날짜, ``reobserve``는 같은 ``run_id``가 안 끝낸 평일 전부."""
    first = max(start or svc.history_start, svc.history_start)
    last = min(end, last_available_date(today)) if end else last_available_date(today)
    if mode == "fill":
        return fill_dates(store, svc.service, first, last)
    if mode == "reobserve":
        if not run_id:
            raise ValueError("reobserve는 --run-id가 필요합니다 (재개를 이 실행 ID로 판단합니다)")
        done = completed_request_keys(store, svc.service, run_id)
        return [d for d in iter_weekdays(first, last) if bas_dd_request_key(d) not in done]
    raise ValueError(f"mode는 fill·reobserve 중 하나입니다: {mode!r}")


# --------------------------------------------------------------------- 정규화


def _yyyymmdd(day: date) -> str:
    return f"{day:%Y%m%d}"


def build_parsed(svc: KrxBaselineService, rows: list[dict], day: date) -> ParsedResponse:
    """원문 행 → ``ParsedResponse``. 응답 행의 ``BAS_DD``가 요청 날짜와 다르면 ``ValueError``."""
    parsed_rows = svc.parse(rows)
    expected = _yyyymmdd(day)
    wrong = sorted({r["BAS_DD"] for r in parsed_rows if r["BAS_DD"] != expected})
    if wrong:
        raise ValueError(f"응답 BAS_DD가 요청 날짜 {expected}와 다릅니다: {wrong[:3]}")
    status = svc.classify(parsed_rows)
    return ParsedResponse(
        svc.spec,
        parsed_rows,
        {"BAS_DD": expected},
        complete=True,
        absent_on_empty=False,
        day_kind=status.day_kind,
        row_count=status.row_count,
        priced_rows=status.priced_rows,
        netasst_zero_rows=status.netasst_zero_rows,
        required_missing=status.required_missing,
    )


def orphan_parser(
    services: dict[str, KrxBaselineService] | None = None,
) -> Callable[[OrphanRaw, bytes], ParsedResponse]:
    """원문만 남은 요청을 다시 정규화하는 파서. 요청 키 ``bas_dd=YYYYMMDD``에서 날짜를 읽는다."""
    table = services or SERVICES

    def parse(orphan: OrphanRaw, body: bytes) -> ParsedResponse:
        svc = table[orphan.service]
        day = datetime.strptime(orphan.request_key.split("=", 1)[1], "%Y%m%d").date()
        return build_parsed(svc, _envelope_rows(body), day)

    return parse


def _envelope_rows(body: bytes) -> list[dict]:
    data = json.loads(body)
    rows = data.get("OutBlock_1") if isinstance(data, dict) else None
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise ValueError("OutBlock_1이 리스트가 아닙니다")
    return rows


# ----------------------------------------------------------------------- 실행


@dataclass
class RunResult:
    """한 서비스 실행의 요약. CLI가 출력하고 종료 코드를 정한다."""

    service: str
    dates_planned: int = 0
    dates_attempted: int = 0
    new_obs: int = 0
    same: int = 0
    failures: int = 0
    http_requests: int = 0
    recovered: int = 0
    stopped_by: str | None = None  # max_calls · consecutive_failures · quota · fatal
    errors: dict[str, str] = field(default_factory=dict)
    manifests: int = 0

    @property
    def fatal(self) -> bool:
        return self.stopped_by in {"quota", "fatal"}


def run_dates(
    *,
    store: BaselineStore,
    client: KrxOpenApiClient,
    svc: KrxBaselineService,
    dates: list[date] | Callable[[], list[date]],
    run_id: str,
    source_run: str,
    max_calls: int | None = None,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
    batch_size: int | None = None,
) -> RunResult:
    """``dates``를 받아 저장한다. 중단되면 그때까지를 확정하고 끝낸다.

    * 새 요청 **전에** 원문만 남은 요청을 복구한다.
    * 응답을 파싱하지 못하거나 ``BAS_DD``가 요청 날짜와 다르면 그 날짜는 실패로
      남는다(관측 없음, 완료 아님). 원문은 지우지 않는다.
    * 연속 실패가 ``max_consecutive_failures``면 멈춘다. 한도 소진은 확정하고 멈춘다.
    * ``max_calls``는 ``client.counters.http_requests``의 증가분이다.
    """
    kwargs: dict[str, Any] = {} if batch_size is None else {"batch_size": batch_size}
    writer = BaselineWriter(store, run_id, store.next_attempt(run_id), source_run, **kwargs)
    result = RunResult(svc.service)
    result.recovered = writer.recover_orphans(orphan_parser(), services={svc.service})
    writer.flush()
    for message in writer.recovery_errors:
        result.errors[f"recovery:{len(result.errors)}"] = message
    # 대상 날짜는 복구가 확정된 **뒤에** 정한다. 먼저 정하면 복구한 날짜를 또 받는다.
    dates = dates() if callable(dates) else dates
    result.dates_planned = len(dates)

    http_start = client.counters.http_requests
    streak = 0
    for day in dates:
        if max_calls is not None and client.counters.http_requests - http_start >= max_calls:
            result.stopped_by = "max_calls"
            break
        if max_consecutive_failures and streak >= max_consecutive_failures:
            result.stopped_by = "consecutive_failures"
            break
        key = bas_dd_request_key(day)
        before = client.counters.http_requests
        result.dates_attempted += 1
        try:
            raw = client.fetch_raw(svc.group, svc.endpoint, {"basDd": _yyyymmdd(day)})
        except SourceQuotaExhaustedError as exc:
            result.stopped_by = "quota"
            result.errors["quota_exhausted"] = str(exc)
            break
        except (SourceAuthError, KrxOpenApiEndpointNotApprovedError) as exc:
            result.stopped_by = "fatal"
            result.errors[type(exc).__name__] = str(exc)[:300]
            break
        except Exception as exc:  # noqa: BLE001 - 요청 실패는 그 날짜만 미완료로 남긴다
            streak += 1
            result.failures += 1
            writer.add_failure(
                service=svc.service,
                request_key=key,
                fetched_at=now_fn(),
                error=f"{type(exc).__name__}: {exc}",
                http_requests=client.counters.http_requests - before,
            )
            result.errors[key] = f"{type(exc).__name__}: {exc}"[:300]
            continue

        http = client.counters.http_requests - before
        ref = store.write_raw(svc.service, key, raw.body, raw.fetched_at)
        try:
            parsed = build_parsed(svc, raw.rows, day)
            outcome = writer.add_response(
                service=svc.service,
                request_key=key,
                raw=ref,
                parsed=parsed,
                http_status=raw.status_code,
                http_requests=http,
                key_slot=raw.key_slot,
            )
        except (KeyError, ValueError, TypeError) as exc:
            # 파싱·키 중복·날짜 불일치. add_response는 상태를 바꾸기 전에 던지므로 안전하다.
            streak += 1
            result.failures += 1
            writer.add_failure(
                service=svc.service,
                request_key=key,
                fetched_at=raw.fetched_at,
                error=f"{type(exc).__name__}: {exc}",
                http_status=raw.status_code,
                raw=ref,
                http_requests=http,
                key_slot=raw.key_slot,
            )
            result.errors[key] = f"{type(exc).__name__}: {exc}"[:300]
            continue
        streak = 0
        if outcome == "new_obs":
            result.new_obs += 1
        else:
            result.same += 1

    result.http_requests = client.counters.http_requests - http_start
    if writer.flush() is not None:
        result.manifests += 1
    return result


def new_run_id(prefix: str, now: datetime | None = None) -> str:
    """시각이 든 실행 ID. 파일 이름 구분자(``__``)를 쓰지 않는다."""
    return f"{prefix}_{(now or now_kst()):%Y%m%dT%H%M%S}"


def sync(
    *,
    store: BaselineStore,
    client: KrxOpenApiClient,
    services: list[KrxBaselineService],
    today: date | None = None,
    max_calls: int | None = DEFAULT_SYNC_MAX_CALLS,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> list[RunResult]:
    """정기 잡. 서비스마다 최근 평일 20일 창의 빈·대기 날짜만 받는다.

    ``max_calls``는 **서비스 전체 합**의 HTTP 수다. 남은 예산을 다음 서비스로 넘긴다.
    """
    today = today or now_kst().date()
    run_id = new_run_id("daily")
    results: list[RunResult] = []
    http_start = client.counters.http_requests
    for svc in services:
        left = None
        if max_calls is not None:
            left = max_calls - (client.counters.http_requests - http_start)
        result = run_dates(
            store=store,
            client=client,
            svc=svc,
            dates=(
                (lambda svc=svc: sync_dates(store, svc, today)) if left is None or left > 0 else []
            ),
            run_id=run_id,
            source_run="daily",
            max_calls=left,
            max_consecutive_failures=max_consecutive_failures,
            now_fn=now_fn,
        )
        results.append(result)
        if result.fatal:
            break
    return results


def backfill(
    *,
    store: BaselineStore,
    client: KrxOpenApiClient,
    service: KrxBaselineService,
    mode: str,
    start: date | None = None,
    end: date | None = None,
    run_id: str | None = None,
    max_calls: int | None = None,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    today: date | None = None,
    now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> RunResult:
    """백필. ``fill``은 빈·대기 날짜만, ``reobserve``는 ``run_id``가 끝낸 날짜만 건너뛴다."""
    today = today or now_kst().date()
    if mode == "reobserve" and not run_id:
        raise ValueError("reobserve는 --run-id가 필요합니다 (재개를 이 실행 ID로 판단합니다)")
    return run_dates(
        store=store,
        client=client,
        svc=service,
        dates=lambda: backfill_dates(
            store, service, mode=mode, start=start, end=end, run_id=run_id, today=today
        ),
        run_id=run_id or new_run_id("backfill"),
        source_run=f"backfill_{today:%Y%m%d}",
        max_calls=max_calls,
        max_consecutive_failures=max_consecutive_failures,
        now_fn=now_fn,
    )


# --------------------------------------------------------------- 조사 사본 import

_TSV_COLUMNS = ("file", "sha256_gz", "bytes", "mtime_local", "rows", "rows_with_close", "day_kind")


@dataclass
class ImportResult:
    files_total: int = 0
    imported: int = 0
    skipped_done: int = 0
    recovered: int = 0
    day_kind_mismatch: int = 0
    manifests: int = 0


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def import_research(
    *,
    store: BaselineStore,
    path: Path,
    expect_manifest_sha256: str,
    force: bool = False,
    run_id: str = IMPORT_RUN_ID,
) -> ImportResult:
    """ETF 조사 사본을 **최초 관측본(``obs_seq=1``)**으로 들인다 (계획 §5.4, R10). HTTP 0.

    쓰기 전에 모두 확인한다. 배치는 ``<path>/manifest_files.tsv``와 ``<path>/raw/<file>``
    (없으면 ``<path>/<file>``)이다. manifest의 sha256이 기대값과 다르거나
    파일 하나라도 ``sha256_gz``와 다르면 아무것도 쓰지 않고 멈춘다. 같은 서비스에
    다른 ``run_id``의 완료 기록이 있으면 거부한다(조사 사본이 ``obs_seq=1``이어야
    하므로, ``force``로만 우회). 같은 ``run_id``로 끝낸 파일은 건너뛰어 두 번 돌려도
    아무것도 늘지 않는다.

    ``mtime_local``은 timezone이 없는 문자열이고 **KST로 읽는다**. 응답 시각이 아니라
    파일 시각이므로 ``fetched_at_basis=file_mtime``이다. 연도 순서로 처리해 상태가
    메모리에 쌓이지 않게 한다.
    """
    svc = SERVICES["etf_bydd_trd"]
    tsv = path / "manifest_files.tsv"
    if not tsv.is_file():
        raise BaselineImportError(f"{tsv}: manifest_files.tsv가 없습니다")
    actual = _sha256_bytes(tsv.read_bytes())
    if actual != expect_manifest_sha256.strip().lower():
        raise BaselineImportError(
            f"manifest_files.tsv sha256이 기대값과 다릅니다: {actual} != {expect_manifest_sha256}"
        )
    with tsv.open(encoding="utf-8", newline="") as handle:
        entries = list(csv.DictReader(handle, delimiter="\t"))
    if not entries or tuple(entries[0]) != _TSV_COLUMNS:
        raise BaselineImportError(f"manifest_files.tsv 열이 {_TSV_COLUMNS}와 다릅니다")

    # 배치는 한 가지로 정한다: 실제 조사 사본은 ``<path>/raw/<file>``이고, 없으면
    # ``<path>/<file>``. 두 곳에 나뉘어 있으면 어느 쪽이 정본인지 알 수 없어 멈춘다.
    in_raw = sum((path / "raw" / e["file"]).is_file() for e in entries)
    in_flat = sum((path / e["file"]).is_file() for e in entries)
    if in_raw and in_flat:
        raise BaselineImportError(
            f"파일이 raw/ 아래({in_raw}개)와 바로 아래({in_flat}개)에 섞여 있습니다"
        )
    base_dir = path / "raw" if in_raw else path

    plan: list[tuple[date, dict[str, str]]] = []
    bad: list[str] = []
    for entry in entries:
        file = base_dir / entry["file"]
        if not file.is_file():
            bad.append(f"{entry['file']}: 없음")
            continue
        # 바이트는 들고 있지 않는다(4,374개면 약 157MB). 들일 때 다시 읽고 다시 대조한다.
        if _sha256_bytes(file.read_bytes()) != entry["sha256_gz"]:
            bad.append(f"{entry['file']}: sha256 불일치")
            continue
        stem = entry["file"].split("_")[-1].split(".")[0]
        plan.append((datetime.strptime(stem, "%Y%m%d").date(), entry))
    if bad:
        raise BaselineImportError(f"파일 {len(bad)}개가 manifest와 다릅니다. 첫 항목: {bad[:3]}")

    if not force:
        log = store.read_fetch_log(svc.service, columns=["run_id", "result"])
        others = sorted(set(log["run_id"]) - {run_id}) if not log.empty else []
        if others:
            raise BaselineImportError(
                f"{svc.service}에 다른 run_id의 완료 기록이 있습니다({others[:3]}). "
                "조사 사본이 obs_seq=1이 되지 못합니다. 그래도 하려면 --force."
            )

    result = ImportResult(files_total=len(plan))
    writer = BaselineWriter(store, run_id, store.next_attempt(run_id), IMPORT_SOURCE_RUN)
    result.recovered = writer.recover_orphans(
        orphan_parser(), fetched_at_basis="file_mtime", services={svc.service}
    )
    if writer.flush() is not None:
        result.manifests += 1
    done = completed_request_keys(store, svc.service, run_id)

    for day, entry in sorted(plan, key=lambda item: item[0]):
        key = bas_dd_request_key(day)
        if key in done:
            result.skipped_done += 1
            continue
        data = (base_dir / entry["file"]).read_bytes()
        if _sha256_bytes(data) != entry["sha256_gz"]:
            raise BaselineImportError(
                f"{entry['file']}: 확인 뒤에 파일이 바뀌었습니다(sha256 불일치)"
            )
        fetched_at = (
            datetime.fromisoformat(entry["mtime_local"]).replace(tzinfo=KST).astimezone(UTC)
        )
        ref = store.import_gzip_raw(svc.service, key, data, fetched_at, "file_mtime")
        try:
            rows = _envelope_rows(gzip.decompress(data))
            parsed = build_parsed(svc, rows, day)
            if len(rows) != int(entry["rows"]):
                raise ValueError(f"행 수가 manifest({entry['rows']})와 다릅니다({len(rows)})")
            if parsed.day_kind != entry["day_kind"]:
                result.day_kind_mismatch += 1
            writer.add_response(
                service=svc.service,
                request_key=key,
                raw=ref,
                parsed=parsed,
                http_requests=0,
            )
        except (KeyError, ValueError, TypeError, OSError) as exc:
            raise BaselineImportError(f"{entry['file']}: 정규화 실패 — {exc}") from exc
        result.imported += 1
    if writer.flush() is not None:
        result.manifests += 1
    return result
