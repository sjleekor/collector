"""ETF 총보수(#6) 수집 — 금투협 DIS 월말 기준일의 sync·backfill·import (계획 04 §2 #6·§2.1).

저장 계약은 ``collector.kr.baseline``(계획 04 §3.1)이다. 원문 바이트(gz)·요청 기록·관측 행
(``fetched_at`` = **처음 본 시각**)·완료 manifest(마지막에 씀)를 그대로 쓴다.

    원문 복구 → 대상 달 계산 → 달마다 (후보 날짜마다) post → write_raw → 파서 → add_response

**저장 위치.** ``$STOCK_DATA_ROOT/kr/raw/krx_baseline/`` 아래다(KRX·SEIBro와 같은 레이크).

* 원문: ``raw_responses/etf_fee_dis/std_ymd=YYYYMMDD/<fetched_at>_<sha12>.xml.gz``
  최근 기준일 조회는 ``raw_responses/etf_fee_dis/latest=J_RD/…``
* 관측: ``etf_fee_snapshot/year=YYYY/<run_id>__<attempt>__<batch>.parquet``
  키 ``(STD_YMD, FUND_CODE)``. 처음 본 값이 ``obs_seq=1``, 값이 바뀌어 다시 보이면 +1.
* 요청 기록: ``fetch_log/etf_fee_dis/…``. 월말 값이 든 요청은 ``day_kind=complete``,
  값이 없는 날은 ``empty``, 최근 기준일 조회는 ``day_kind=latest=YYYYMMDD``.

**새 달은 언제 처음 보였나.** 공개 지연을 잴 방법은 이 기록뿐이다. ``sync``는 실행할 때마다
최근 기준일 조회(``latest=J_RD``) 요청 한 줄을 남기므로, 최근 기준일이 언제 바뀌었는지가
요청 기록에 시계열로 남는다. 새 달 관측의 ``fetched_at``은 그 달 값을 처음 받은 시각이다.
(10-10·11 조회 때 최근 기준일이 2026-08-31이었다 — 9월 말 값이 아직 없었다.)

**발견 방식(``discovery``).**

* ``latest``(기본): 최근 기준일 한 번 조회 → 저장된 마지막 달 뒤로 새 달이 있으면 받는다.
  새 달이 없으면 **요청 1건으로 끝난다**(그 한 줄이 공개 지연 기록이다).
* ``direct``: 최근 기준일을 안 보고 저장된 마지막 달의 다음 달 월말을 직접 묻는다. 값이
  없으면 후보 4개를 모두 묻고(요청 4건) 끝난다. ``latest``의 요청 본문이 틀렸을 때
  (미확인, ``adapters/kofia_dis_fee/request.py``)나 응답에서 날짜를 못 읽을 때 자동으로
  이쪽으로 물러난다.

**주기 권고.** 기준일이 월 1회라 **주 1회**(예: 일요일)로 충분하다. ``latest`` 한 건짜리 점검이면
매일 돌려도 가볍지만, 매일 얻는 것은 공개 지연 해상도(일 단위 대 주 단위)뿐이고 새 달 값은 사실상
같은 주에 잡힌다. 지연 해상도가 필요하면 매일, 아니면 주 1회를 고른다 — 사용자 결정.

**끊긴 응답.** 클라이언트가 같은 요청을 다시 보낸다(``</root>`` 확인). 끝내 못 받으면 그 날짜는
실패로 남기고 빈 날로 세지 않는다 — 앞날짜로 넘어가면 달이 통째로 빠진다(연구용 사본 첫 판의 실패).
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from collector.kr.adapters.kofia_dis_fee import (
    KEY_COLUMNS,
    PARSED_TYPES,
    SOURCE_COLUMNS,
    DisClient,
    DisParseError,
    DisRequestError,
    Month,
    build_fee_request,
    build_latest_std_request,
    count_rows,
    iter_months,
    month_end_candidates,
    month_key,
    month_of,
    next_month,
    parse_fee_rows,
    parse_latest_std_date,
)
from collector.kr.baseline import (
    BaselineStore,
    BaselineWriter,
    OrphanRaw,
    ParsedResponse,
    TableSpec,
    completed_request_keys,
)
from collector.kr.util.time import KST, now_kst

logger = logging.getLogger(__name__)

SERVICE = "etf_fee_dis"
LATEST_KEY = "latest=J_RD"
DEFAULT_SYNC_MAX_CALLS = 30
DEFAULT_MAX_CONSECUTIVE_FAILURES = 3

#: 연구용 사본 import의 고정 실행 ID(2026-10-11 사본).
IMPORT_RUN_ID = "import_research_dis_20261011"
IMPORT_SOURCE_RUN = "import_research_dis"

SPEC = TableSpec(
    name="etf_fee_snapshot",
    key_columns=KEY_COLUMNS,
    source_columns=SOURCE_COLUMNS,
    date_column="STD_YMD",
    parsed_columns=PARSED_TYPES,
)

KIND_COMPLETE = "complete"
KIND_EMPTY = "empty"


class DisImportError(RuntimeError):
    """연구용 사본 import를 시작·계속할 수 없을 때. 아무것도 쓰기 전에 던진다."""


def std_key(std_ymd: str) -> str:
    return f"std_ymd={std_ymd}"


# ------------------------------------------------------------------ 저장 상태


def completed_std_dates(store: BaselineStore) -> dict[str, int]:
    """값이 든 요청(``day_kind=complete``, 실패 아님)이 있는 기준일 → 마지막 행 수."""
    log = store.read_fetch_log(
        SERVICE, columns=["request_key", "result", "day_kind", "row_count", "fetched_at"]
    )
    out: dict[str, int] = {}
    if log.empty:
        return out
    ok = log[(log["result"] != "failed") & (log["day_kind"] == KIND_COMPLETE)]
    for _, row in ok.sort_values("fetched_at").iterrows():
        if row["request_key"].startswith("std_ymd="):
            out[row["request_key"].split("=", 1)[1]] = int(row["row_count"])
    return out


def stored_months(store: BaselineStore) -> dict[Month, str]:
    """저장된 달 → 그 달의 (가장 늦은) 기준일 ``YYYYMMDD``."""
    out: dict[Month, str] = {}
    for ymd in sorted(completed_std_dates(store)):
        out[(int(ymd[:4]), int(ymd[4:6]))] = ymd
    return out


def _run_complete_months(store: BaselineStore, run_id: str) -> set[Month]:
    log = store.read_fetch_log(SERVICE, columns=["run_id", "request_key", "result", "day_kind"])
    if log.empty:
        return set()
    ok = log[
        (log["run_id"] == run_id) & (log["result"] != "failed") & (log["day_kind"] == KIND_COMPLETE)
    ]
    return {
        (int(k.split("=", 1)[1][:4]), int(k.split("=", 1)[1][4:6]))
        for k in ok["request_key"]
        if k.startswith("std_ymd=")
    }


# ------------------------------------------------------------------- 정규화


def build_parsed(body: bytes, std_ymd: str) -> ParsedResponse:
    """보수 응답 → ``ParsedResponse``. 값이 없으면 ``empty``(관측·absent 근거 아님)."""
    total = count_rows(body)
    if total == 0:
        return ParsedResponse(
            SPEC, [], {"STD_YMD": std_ymd}, complete=True, absent_on_empty=False,
            day_kind=KIND_EMPTY, row_count=0,
        )  # fmt: skip
    parsed = parse_fee_rows(body, std_ymd)
    return ParsedResponse(
        SPEC, parsed.rows, {"STD_YMD": std_ymd}, complete=True, absent_on_empty=False,
        day_kind=KIND_COMPLETE, row_count=parsed.total_count,
    )  # fmt: skip


def _probe_parsed(body: bytes, today: date) -> tuple[ParsedResponse, date | None]:
    latest = parse_latest_std_date(body, today)
    label = f"latest={latest:%Y%m%d}" if latest else "latest=unparsed"
    scope = f"{latest or today:%Y%m%d}"
    return (
        ParsedResponse(SPEC, [], {"STD_YMD": scope}, complete=False, day_kind=label, row_count=0),
        latest,
    )


def orphan_parser(today: date | None = None) -> Callable[[OrphanRaw, bytes], ParsedResponse]:
    """원문만 남은 요청을 다시 정규화한다. 요청 키로 보수 응답과 최근 기준일 조회를 가른다."""

    def parse(orphan: OrphanRaw, body: bytes) -> ParsedResponse:
        if orphan.request_key == LATEST_KEY:
            return _probe_parsed(body, today or now_kst().date())[0]
        return build_parsed(body, orphan.request_key.split("=", 1)[1])

    return parse


# --------------------------------------------------------------------- 실행


@dataclass
class DisRunResult:
    """한 번 실행의 요약. CLI가 출력하고 종료 코드를 정한다."""

    discovery: str = "latest"
    latest_std: date | None = None
    months_planned: int = 0
    months_fetched: list[str] = field(default_factory=list)  # 값을 받은 기준일 YYYYMMDD
    months_missing: list[str] = field(default_factory=list)  # 후보를 다 물어도 값이 없던 달
    new_obs: int = 0
    same: int = 0
    empty_days: int = 0
    failures: int = 0
    http_requests: int = 0
    truncated: int = 0
    recovered: int = 0
    stopped_by: str | None = None  # max_calls · consecutive_failures · empty_store
    errors: dict[str, str] = field(default_factory=dict)
    manifests: int = 0

    @property
    def nothing_new(self) -> bool:
        return not self.months_fetched and not self.failures


class _Run:
    """한 번 실행의 상태(기록기·계수기·연속 실패)를 묶는다."""

    def __init__(
        self,
        store: BaselineStore,
        client: DisClient,
        run_id: str,
        source_run: str,
        max_calls: int | None,
        max_consecutive_failures: int,
        now_fn: Callable[[], datetime],
        today: date,
    ) -> None:
        self.store = store
        self.client = client
        self.writer = BaselineWriter(store, run_id, store.next_attempt(run_id), source_run)
        self.result = DisRunResult()
        self.max_calls = max_calls
        self.max_failures = max_consecutive_failures
        self.now_fn = now_fn
        self.today = today
        self.streak = 0
        self._http0 = client.counters.http_requests
        self._trunc0 = client.counters.truncated
        self.result.recovered = self.writer.recover_orphans(
            orphan_parser(today), services={SERVICE}
        )
        self.writer.flush()
        for message in self.writer.recovery_errors:
            self.result.errors[f"recovery:{len(self.result.errors)}"] = message

    def budget_left(self) -> bool:
        if self.max_calls is None:
            return True
        return self.client.counters.http_requests - self._http0 < self.max_calls

    def stop_reason(self) -> str | None:
        if not self.budget_left():
            return "max_calls"
        if self.max_failures and self.streak >= self.max_failures:
            return "consecutive_failures"
        return None

    def _fail(self, key: str, error: str, *, before: int, raw=None, status=None) -> None:
        self.streak += 1
        self.result.failures += 1
        self.result.errors[key] = error[:300]
        self.writer.add_failure(
            service=SERVICE,
            request_key=key,
            fetched_at=self.now_fn(),
            error=error,
            http_status=status,
            raw=raw,
            http_requests=self.client.counters.http_requests - before,
        )

    def probe_latest(self) -> date | None:
        """최근 기준일(``J_RD``)을 한 번 조회해 요청 기록에 남기고 날짜를 돌려준다."""
        before = self.client.counters.http_requests
        try:
            raw = self.client.post(build_latest_std_request())
        except DisRequestError as exc:
            self._fail(LATEST_KEY, f"{type(exc).__name__}: {exc}", before=before)
            return None
        ref = self.store.write_raw(SERVICE, LATEST_KEY, raw.body, raw.fetched_at, ext="xml")
        parsed, latest = _probe_parsed(raw.body, self.today)
        self.writer.add_response(
            service=SERVICE,
            request_key=LATEST_KEY,
            raw=ref,
            parsed=parsed,
            http_status=raw.status_code,
            http_requests=self.client.counters.http_requests - before,
        )
        self.streak = 0
        self.result.latest_std = latest
        return latest

    def fetch_date(self, day: date) -> str:
        """기준일 하나를 받는다. ``hit``·``empty``·``failed``를 돌려준다."""
        ymd = f"{day:%Y%m%d}"
        key = std_key(ymd)
        before = self.client.counters.http_requests
        try:
            raw = self.client.post(build_fee_request(ymd))
        except DisRequestError as exc:
            self._fail(key, f"{type(exc).__name__}: {exc}", before=before)
            return "failed"
        ref = self.store.write_raw(SERVICE, key, raw.body, raw.fetched_at, ext="xml")
        http = self.client.counters.http_requests - before
        try:
            parsed = build_parsed(raw.body, ymd)
            outcome = self.writer.add_response(
                service=SERVICE,
                request_key=key,
                raw=ref,
                parsed=parsed,
                http_status=raw.status_code,
                http_requests=http,
            )
        except (DisParseError, ValueError, TypeError) as exc:
            self._fail(
                key, f"{type(exc).__name__}: {exc}", before=before, raw=ref, status=raw.status_code
            )
            return "failed"
        self.streak = 0
        if parsed.day_kind == KIND_EMPTY:
            self.result.empty_days += 1
            return "empty"
        self.result.months_fetched.append(ymd)
        if outcome == "new_obs":
            self.result.new_obs += 1
        else:
            self.result.same += 1
        return "hit"

    def fetch_month(self, month: Month, hint: date | None = None) -> bool:
        """그 달의 월말 기준일을 찾아 받는다. ``hint``가 있으면 그 날짜 하나만 묻는다.

        후보를 다 물어도 값이 없으면 ``months_missing``에 올린다. **실패(끊김 등)가 나면 앞날짜로
        넘어가지 않고** 그 달을 멈춘다 — 다음 실행이 같은 날짜부터 다시 시도한다.
        """
        candidates = [hint] if hint else month_end_candidates(month)
        for day in candidates:
            if self.stop_reason():
                return False
            outcome = self.fetch_date(day)
            if outcome == "hit":
                return True
            if outcome == "failed":
                return False
        self.result.months_missing.append(month_key(month))
        return False

    def finish(self) -> DisRunResult:
        self.result.http_requests = self.client.counters.http_requests - self._http0
        self.result.truncated = self.client.counters.truncated - self._trunc0
        if self.writer.flush() is not None:
            self.result.manifests += 1
        if self.result.stopped_by is None:
            self.result.stopped_by = self.stop_reason()
        return self.result


def _new_run_id(prefix: str, now: datetime | None = None) -> str:
    return f"{prefix}_{(now or now_kst()):%Y%m%dT%H%M%S}"


def last_collectable_month(today: date) -> Month:
    """값이 있을 수 있는 가장 늦은 달 — 이번 달 월말은 아직 안 왔으므로 지난달이다."""
    return (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)


def sync(
    *,
    store: BaselineStore,
    client: DisClient,
    today: date | None = None,
    discovery: str = "latest",
    max_calls: int | None = DEFAULT_SYNC_MAX_CALLS,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> DisRunResult:
    """정기 잡. 저장된 마지막 달 뒤에 새 월말 기준일이 올라왔는지 보고 받는다.

    저장소가 비어 있으면 아무것도 안 받고 ``empty_store``로 멈춘다 — 2003년부터의 이력은
    ``import-research``나 ``backfill``의 일이다(KRX 정기 잡이 창 밖을 안 받는 것과 같은 이유).
    새 달이 없으면 ``latest`` 방식은 요청 1건(최근 기준일 조회)으로 끝나고 그 기록이 남는다.
    """
    if discovery not in ("latest", "direct"):
        raise ValueError(f"discovery는 latest·direct 중 하나입니다: {discovery!r}")
    today = today or now_kst().date()
    run = _Run(
        store, client, _new_run_id("daily"), "daily", max_calls, max_consecutive_failures,
        now_fn, today,
    )  # fmt: skip
    res = run.result
    res.discovery = discovery
    months = stored_months(store)
    if not months:
        res.stopped_by = "empty_store"
        res.errors["empty_store"] = (
            "저장된 월말 기준일이 없습니다. import-research나 backfill을 먼저 하십시오."
        )
        return run.finish()
    last = max(months)
    ceiling = last_collectable_month(today)

    latest: date | None = None
    if discovery == "latest":
        latest = run.probe_latest()
        if latest is None and run.streak == 0:
            res.errors["latest"] = "최근 기준일을 못 읽어 직접 탐색으로 물러납니다"
        if latest is None:
            res.discovery = "direct"
    if latest is not None:
        targets = list(iter_months(next_month(last), min(month_of(latest), ceiling)))
        res.months_planned = len(targets)
        for month in targets:
            hint = latest if month == month_of(latest) else None
            if not run.fetch_month(month, hint) and run.stop_reason():
                break
    else:
        month = next_month(last)
        while month <= ceiling and not run.stop_reason():
            res.months_planned += 1
            if not run.fetch_month(month):
                break
            month = next_month(month)
    return run.finish()


def backfill(
    *,
    store: BaselineStore,
    client: DisClient,
    start: Month,
    end: Month,
    mode: str = "fill",
    run_id: str | None = None,
    today: date | None = None,
    max_calls: int | None = None,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> DisRunResult:
    """``start``~``end``(YYYY-MM, 양끝 포함) 월말 백필.

    ``fill``은 값이 저장된 달을 건너뛰고 빈 달만 월말 탐색으로 받는다. ``reobserve``는 저장된
    달도 **그 기준일로 다시 받아** 값이 바뀌었는지 새 관측으로 남긴다(``run_id`` 필수 — 이
    ``run_id``가 끝낸 달은 건너뛰어 끊긴 데서 이어 받는다).
    """
    today = today or now_kst().date()
    if mode not in ("fill", "reobserve"):
        raise ValueError(f"mode는 fill·reobserve 중 하나입니다: {mode!r}")
    if mode == "reobserve" and not run_id:
        raise ValueError("reobserve는 --run-id가 필요합니다 (재개를 이 실행 ID로 판단합니다)")
    if start > end:
        raise ValueError("시작 달이 끝 달보다 늦습니다")
    rid = run_id or _new_run_id("backfill")
    run = _Run(
        store, client, rid, f"backfill_{today:%Y%m%d}", max_calls, max_consecutive_failures,
        now_fn, today,
    )  # fmt: skip
    run.result.discovery = mode
    ceiling = last_collectable_month(today)
    months = stored_months(store)
    skip_done = _run_complete_months(store, rid) if mode == "reobserve" else set()
    plan = [m for m in iter_months(start, min(end, ceiling))]
    run.result.months_planned = len(plan)
    for month in plan:
        if run.stop_reason():
            break
        if mode == "fill" and month in months:
            continue
        if mode == "reobserve" and month in skip_done:
            continue
        hint = None
        if mode == "reobserve" and month in months:
            ymd = months[month]
            hint = date(int(ymd[:4]), int(ymd[4:6]), int(ymd[6:8]))
        run.fetch_month(month, hint)
    return run.finish()


# --------------------------------------------------------------- 연구용 사본 import


@dataclass
class ImportResult:
    files_total: int = 0
    imported: int = 0
    skipped_done: int = 0
    recovered: int = 0
    manifests: int = 0
    rows: int = 0


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def import_research(
    *,
    store: BaselineStore,
    path: Path,
    expect_manifest_sha256: str,
    force: bool = False,
    run_id: str = IMPORT_RUN_ID,
) -> ImportResult:
    """연구용 월말 이력 사본(2026-10-11, 280개 달)을 최초 관측본(``obs_seq=1``)으로 들인다. HTTP 0.

    배치는 ``<path>/manifest.jsonl``과 ``<path>/raw/<YYYYMMDD>.xml.gz``다(``SHA256SUMS``가
    있으면 gz 해시도 대조한다). 쓰기 전에 **모두** 확인한다: ① ``manifest.jsonl`` 자체의
    sha256이 기대값과 같다 ② ``file``이 있는 줄마다 gz가 있고, 푼 본문의 sha256·행 수가
    줄의 값과 같고 ``</root>``로 끝나며 행 수가 ``dbio_total_count_``와 같다. 하나라도
    다르면 아무것도 쓰지 않는다. 같은 서비스에 다른 ``run_id``의 완료 기록이 있으면 거부한다
    (``obs_seq=1``이 사본이어야 하므로 ``force``로만 우회).

    ``fetched_at``은 사본 manifest의 ``fetched_at``(요청 시작 시각, 시간대 없는 KST 문자열)을
    UTC로 바꿔 **그대로 둔다** — 가용 시점(PIT)은 이 값이 우리가 처음 본 시각이다.
    ``fetched_at_basis=response``(요청 시작과 응답 사이는 수 초다).
    값 없는 날·실패 줄은 manifest에 남은 조사 기록이라 들이지 않는다.
    """
    manifest = path / "manifest.jsonl"
    if not manifest.is_file():
        raise DisImportError(f"{manifest}: manifest.jsonl이 없습니다")
    actual = _sha256(manifest.read_bytes())
    if actual != expect_manifest_sha256.strip().lower():
        raise DisImportError(
            f"manifest.jsonl sha256이 기대값과 다릅니다: {actual} != {expect_manifest_sha256}"
        )
    records = []
    for line in manifest.read_text("utf-8").splitlines():
        if line.strip():
            rec = json.loads(line)
            if "file" in rec:
                records.append(rec)
    if not records:
        raise DisImportError("manifest.jsonl에 file이 있는 줄이 없습니다")

    sums: dict[str, str] = {}
    sums_file = path / "SHA256SUMS"
    if sums_file.is_file():
        for line in sums_file.read_text("utf-8").splitlines():
            digest, _, name = line.strip().partition("  ")
            if name:
                sums[name] = digest

    plan: list[tuple[str, dict]] = []
    bad: list[str] = []
    seen: set[str] = set()
    for rec in records:
        rel = rec["file"]
        file = path / rel
        ymd = rec["date"]
        if ymd in seen:
            bad.append(f"{rel}: 기준일 {ymd}이 manifest에 둘 이상")
            continue
        seen.add(ymd)
        if not file.is_file():
            bad.append(f"{rel}: 없음")
            continue
        gz = file.read_bytes()
        if rel in sums and _sha256(gz) != sums[rel]:
            bad.append(f"{rel}: SHA256SUMS와 다름")
            continue
        try:
            body = gzip.decompress(gz)
        except OSError:
            bad.append(f"{rel}: gzip을 못 풂")
            continue
        if _sha256(body) != rec["sha256"]:
            bad.append(f"{rel}: 본문 sha256이 manifest와 다름")
            continue
        if not body.rstrip().endswith(b"</root>"):
            bad.append(f"{rel}: </root>로 끝나지 않음")
            continue
        try:
            n = count_rows(body)
            parsed = parse_fee_rows(body, ymd)
        except DisParseError as exc:
            bad.append(f"{rel}: {exc}")
            continue
        if n != int(rec["rows"]) or len(parsed.rows) != n:
            bad.append(f"{rel}: 행 수가 manifest({rec['rows']})·dbio_total_count_({n})와 다름")
            continue
        plan.append((ymd, rec))
    if bad:
        raise DisImportError(f"파일 {len(bad)}개가 manifest와 다릅니다. 첫 항목: {bad[:3]}")

    if not force:
        log = store.read_fetch_log(SERVICE, columns=["run_id", "result"])
        others = sorted(set(log["run_id"]) - {run_id}) if not log.empty else []
        if others:
            raise DisImportError(
                f"{SERVICE}에 다른 run_id의 완료 기록이 있습니다({others[:3]}). "
                "사본이 obs_seq=1이 되지 못합니다. 그래도 하려면 --force."
            )

    result = ImportResult(files_total=len(plan))
    writer = BaselineWriter(store, run_id, store.next_attempt(run_id), IMPORT_SOURCE_RUN)
    result.recovered = writer.recover_orphans(
        orphan_parser(), fetched_at_basis="response", services={SERVICE}
    )
    if writer.flush() is not None:
        result.manifests += 1
    done = completed_request_keys(store, SERVICE, run_id)

    for ymd, rec in sorted(plan, key=lambda item: item[0]):
        key = std_key(ymd)
        if key in done:
            result.skipped_done += 1
            continue
        gz = (path / rec["file"]).read_bytes()
        body = gzip.decompress(gz)
        if _sha256(body) != rec["sha256"]:
            raise DisImportError(f"{rec['file']}: 확인 뒤에 파일이 바뀌었습니다(sha256 불일치)")
        fetched_at = datetime.fromisoformat(rec["fetched_at"]).replace(tzinfo=KST).astimezone(UTC)
        ref = store.import_gzip_raw(SERVICE, key, gz, fetched_at, "response", ext="xml")
        parsed = build_parsed(body, ymd)
        writer.add_response(
            service=SERVICE, request_key=key, raw=ref, parsed=parsed, http_requests=0
        )
        result.imported += 1
        result.rows += len(parsed.rows)
    if writer.flush() is not None:
        result.manifests += 1
    return result
