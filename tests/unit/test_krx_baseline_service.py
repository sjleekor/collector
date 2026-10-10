"""KRX 기준선 서비스 — 가짜 client·tmp 저장소로 끝에서 끝까지 (계획 04 §10.1 시험 3)."""

from __future__ import annotations

import gzip
import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from collector.kr.adapters.krx_baseline_openapi import bond_index, derivative_index, etf_daily
from collector.kr.adapters.market_data_krx_openapi.client import (
    KrxOpenApiMalformedResponseError,
    KrxOpenApiRawResponse,
)
from collector.kr.baseline import BaselineStore
from collector.kr.baseline.pending import iter_weekdays
from collector.kr.service import krx_baseline as kb
from collector.kr.service.krx_baseline_verify import verify
from collector.kr.util.pipeline import SourceQuotaExhaustedError

ETF = kb.SERVICES["etf_bydd_trd"]
BOND = kb.SERVICES["bon_dd_trd"]
DERIV = kb.SERVICES["drvprod_dd_trd"]
TODAY = date(2026, 10, 21)  # 수요일 — 시험 입력일 뿐 코드 기본값이 아니다


def _etf_rows(day: str, codes=("A", "B"), close: str = "1000", netasst: str = "5000") -> list[dict]:
    rows = []
    for code in codes:
        row = {c: "" for c in etf_daily.SOURCE_COLUMNS}
        row.update(
            BAS_DD=day,
            ISU_CD=code,
            ISU_NM=f"etf {code}",
            TDD_CLSPRC=close,
            INVSTASST_NETASST_TOTAMT=netasst,
            NAV="1000.5",
        )
        rows.append(row)
    return rows


def _bond_rows(day: str, groups=bond_index.EXPECTED_GROUPS, value: str = "100.5") -> list[dict]:
    rows = []
    for name in sorted(groups):
        row = {c: "" for c in bond_index.SOURCE_COLUMNS}
        row.update(BAS_DD=day, BND_IDX_GRP_NM=name, TOT_EARNG_IDX=value)
        rows.append(row)
    return rows


def _deriv_rows(day: str, close: str = "300.1") -> list[dict]:
    rows = []
    for name in ("코스피 200", derivative_index.KOSPI200_TR_NAME):
        row = {c: "" for c in derivative_index.SOURCE_COLUMNS}
        row.update(BAS_DD=day, IDX_CLSS="KOSPI", IDX_NM=name, CLSPRC_IDX=close)
        rows.append(row)
    return rows


class FakeClient:
    """``fetch_raw``만 흉내 낸다. 요청한 날짜를 ``calls``에 쌓고 HTTP 수를 센다."""

    def __init__(self, responder=None, *, http_per_call: int = 1) -> None:
        self.counters = SimpleNamespace(http_requests=0)
        self.calls: list[tuple[str, str]] = []
        self.http_per_call = http_per_call
        self.now = datetime(2026, 10, 21, 11, 0, tzinfo=UTC)
        self.responder = responder or self.default

    @staticmethod
    def default(endpoint: str, day: str) -> list[dict]:
        if endpoint == etf_daily.ENDPOINT:
            return _etf_rows(day)
        if endpoint == bond_index.ENDPOINT:
            return _bond_rows(day)
        return _deriv_rows(day)

    def fetch_raw(self, group, endpoint, params):  # noqa: ANN001
        day = params["basDd"]
        self.calls.append((endpoint, day))
        self.counters.http_requests += self.http_per_call
        rows = self.responder(endpoint, day)
        self.now += timedelta(seconds=1)
        return KrxOpenApiRawResponse(
            body=json.dumps({"OutBlock_1": rows}, ensure_ascii=False).encode(),
            fetched_at=self.now,
            status_code=200,
            rows=rows,
            key_slot=1,
            group=group,
            endpoint=endpoint,
            params=dict(params),
        )

    def days(self, endpoint: str) -> list[str]:
        return [d for e, d in self.calls if e == endpoint]


@pytest.fixture
def store(tmp_path: Path) -> BaselineStore:
    return BaselineStore(tmp_path / "krx_baseline")


def _weekday_strs(start: date, end: date) -> list[str]:
    return [f"{d:%Y%m%d}" for d in iter_weekdays(start, end)]


def _sync(store, client, services=(ETF,), today=TODAY, **kw):
    return kb.sync(store=store, client=client, services=list(services), today=today, **kw)


# ------------------------------------------------------------------ R04 sync


def test_first_sync_on_an_empty_store_takes_only_the_window(store: BaselineStore) -> None:
    client = FakeClient()
    (result,) = _sync(store, client, max_calls=None)
    start, end = kb.sync_window(date(2026, 10, 20))
    assert (start, end) == (date(2026, 9, 23), date(2026, 10, 20))
    assert client.days(etf_daily.ENDPOINT) == _weekday_strs(start, end)
    assert len(client.calls) == 20 and result.new_obs == 20
    assert max(d for _, d in client.calls) == "20261020"  # 어제까지, 오늘은 안 받는다


def test_sync_never_asks_for_dates_outside_the_window(store: BaselineStore) -> None:
    client = FakeClient()
    _sync(store, client, max_calls=None)
    old = FakeClient()
    # 한 달 뒤: 창이 앞으로 밀렸다. 옛 날짜가 비었어도 받지 않는다.
    _sync(store, old, today=TODAY + timedelta(days=30), max_calls=None)
    # 앞 실행은 20261020까지 끝냈다. 21·22일은 비었지만 창(20261023~) 밖이라 받지 않는다.
    assert min(d for _, d in old.calls) == "20261023"


def test_a_friday_gap_is_filled_on_monday(store: BaselineStore) -> None:
    first = FakeClient()
    _sync(store, first, today=date(2026, 10, 16), max_calls=None)  # 금요일: 끝은 목요일
    assert max(d for _, d in first.calls) == "20261015"
    monday = FakeClient()
    _sync(store, monday, today=date(2026, 10, 19), max_calls=None)  # 끝은 일요일
    assert monday.days(etf_daily.ENDPOINT) == ["20261016"]


def test_resume_after_a_multi_day_stop_fetches_only_what_is_missing(store: BaselineStore) -> None:
    bad = {"20261005", "20261006", "20261007"}

    def responder(endpoint: str, day: str) -> list[dict]:
        if day in bad:
            raise RuntimeError("HTTP 503")
        return FakeClient.default(endpoint, day)

    broken = FakeClient(responder)
    (stopped,) = _sync(store, broken, max_calls=None, max_consecutive_failures=3)
    assert stopped.failures == 3 and stopped.stopped_by == "consecutive_failures"
    assert "20261007" in broken.days(etf_daily.ENDPOINT)
    assert "20261008" not in broken.days(etf_daily.ENDPOINT)  # 연속 실패에서 멈췄다

    healthy = FakeClient()
    (resumed,) = _sync(store, healthy, max_calls=None)
    done_before = set(broken.days(etf_daily.ENDPOINT)) - bad
    window = set(_weekday_strs(date(2026, 9, 23), date(2026, 10, 20)))
    assert set(healthy.days(etf_daily.ENDPOINT)) == window - done_before  # 끝낸 날짜는 안 받는다
    assert bad <= set(healthy.days(etf_daily.ENDPOINT))
    assert resumed.failures == 0
    log = store.read_fetch_log(ETF.service)
    assert set(log[log["result"] == "failed"]["request_key"]) == {f"bas_dd={d}" for d in bad}


def test_max_calls_counts_real_http_requests_not_dates(store: BaselineStore) -> None:
    client = FakeClient(http_per_call=2)  # 날짜마다 재시도로 2번
    (result,) = _sync(store, client, max_calls=5)
    assert result.stopped_by == "max_calls"
    assert result.http_requests == 6 and len(client.calls) == 3
    again = FakeClient()
    (_,) = _sync(store, again, max_calls=None)
    assert len(again.calls) == 17  # 끝낸 3일은 건너뛴다


def test_default_sync_budget_is_60_http_requests() -> None:
    assert kb.DEFAULT_SYNC_MAX_CALLS == 60


def test_quota_exhaustion_commits_what_is_done_and_stops(store: BaselineStore) -> None:
    def responder(endpoint: str, day: str) -> list[dict]:
        if day == "20261001":
            raise SourceQuotaExhaustedError("limit")
        return FakeClient.default(endpoint, day)

    (result,) = _sync(store, FakeClient(responder), max_calls=None)
    assert result.stopped_by == "quota" and result.fatal
    done = store.read_fetch_log(ETF.service)
    assert set(done["request_key"]) == {
        f"bas_dd={d}" for d in _weekday_strs(date(2026, 9, 23), date(2026, 9, 30))
    }
    assert store.manifests()


def test_response_bas_dd_mismatch_is_a_failure_not_an_observation(store: BaselineStore) -> None:
    def responder(endpoint: str, day: str) -> list[dict]:
        return _etf_rows("20200101" if day == "20261001" else day)

    (result,) = _sync(store, FakeClient(responder), max_calls=None, max_consecutive_failures=0)
    assert result.failures == 1
    log = store.read_fetch_log(ETF.service)
    bad = log[log["request_key"] == "bas_dd=20261001"].iloc[0]
    assert bad["result"] == "failed" and "BAS_DD" in bad["error"]
    assert bad["raw_path"]  # 원문은 남긴다
    obs = store.read_observations(ETF.spec, "all")
    assert "20200101" not in set(obs["BAS_DD"]) and "20261001" not in set(obs["BAS_DD"])


def test_malformed_response_is_recorded_as_a_failure(store: BaselineStore) -> None:
    def responder(endpoint: str, day: str) -> list[dict]:
        if day == "20261002":
            raise KrxOpenApiMalformedResponseError("OutBlock_1 missing")
        return FakeClient.default(endpoint, day)

    (result,) = _sync(store, FakeClient(responder), max_calls=None, max_consecutive_failures=0)
    assert result.failures == 1
    assert "Malformed" in result.errors["bas_dd=20261002"]


def test_all_three_services_share_one_flow(store: BaselineStore) -> None:
    client = FakeClient()
    results = _sync(store, client, services=(ETF, BOND, DERIV), max_calls=None)
    assert [r.new_obs for r in results] == [20, 20, 20]
    assert len(store.read_observations(BOND.spec, "all")) == 60
    assert store.read_observations(DERIV.spec, "all")["clsprc_idx_num"].notna().all()
    assert (store.base / "raw_responses" / "bon_dd_trd").is_dir()


# ------------------------------------------------------------------ 원문 복구


def test_a_run_recovers_orphan_raw_before_any_new_request(store: BaselineStore) -> None:
    raw = store.write_raw(
        ETF.service,
        "bas_dd=20261001",
        json.dumps({"OutBlock_1": _etf_rows("20261001")}).encode(),
        datetime(2026, 10, 2, tzinfo=UTC),
    )
    assert store.find_orphan_raw()
    client = FakeClient()
    (result,) = _sync(store, client, max_calls=None)
    assert result.recovered == 1
    assert "20261001" not in client.days(etf_daily.ENDPOINT)  # 다시 받지 않는다
    obs = store.read_observations(ETF.spec, "all")
    assert set(obs[obs["BAS_DD"] == "20261001"]["raw_path"]) == {raw.path}


# --------------------------------------------------------------- 조사 사본 import


def _research_copy(
    root: Path, days: list[str], *, zero_on: str | None = None, flat: bool = False
) -> tuple[Path, str]:
    """실제 배치: manifest는 root, gz는 root/raw/. ``flat``이면 gz도 root 바로 아래."""
    gz_dir = root if flat else root / "raw"
    gz_dir.mkdir(parents=True)
    lines = ["file\tsha256_gz\tbytes\tmtime_local\trows\trows_with_close\tday_kind"]
    for day in days:
        rows = _etf_rows(day, netasst="0" if day == zero_on else "5000")
        data = gzip.compress(json.dumps({"OutBlock_1": rows}).encode(), mtime=0)
        (gz_dir / f"etf_{day}.json.gz").write_bytes(data)
        lines.append(
            f"etf_{day}.json.gz\t{hashlib.sha256(data).hexdigest()}\t{len(data)}"
            f"\t2026-10-09T22:14:20\t{len(rows)}\t{len(rows)}\ttrading"
        )
    tsv = root / "manifest_files.tsv"
    tsv.write_text("\n".join(lines) + "\n", "utf-8")
    return root, hashlib.sha256(tsv.read_bytes()).hexdigest()


def _import_days() -> list[str]:
    return ["20100104", *_weekday_strs(date(2026, 9, 23), date(2026, 10, 8))]


def test_import_makes_obs_seq_one_with_kst_mtime_and_file_mtime_basis(
    store: BaselineStore, tmp_path: Path
) -> None:
    path, sha = _research_copy(tmp_path / "copy", _import_days(), zero_on="20261008")
    result = kb.import_research(store=store, path=path, expect_manifest_sha256=sha)
    assert result.imported == len(_import_days()) and result.manifests >= 1
    first = store.read_observations(kb.ETF_SPEC, "first")
    assert len(first) == 2 * len(_import_days())
    assert set(first["fetched_at_basis"]) == {"file_mtime"}
    assert set(first["source_run"]) == {"import_research_pull"}
    assert set(first["run_id"]) == {kb.IMPORT_RUN_ID}
    # 22:14:20 KST = 13:14:20 UTC
    assert set(first["fetched_at"].dt.strftime("%H:%M:%S")) == {"13:14:20"}
    # 원문은 gz 바이트 그대로
    ref = first.iloc[0]["raw_path"]
    assert (store.base / ref).read_bytes() == (
        path / "raw" / f"etf_{first.iloc[0]['BAS_DD']}.json.gz"
    ).read_bytes()


def test_import_twice_adds_nothing(store: BaselineStore, tmp_path: Path) -> None:
    path, sha = _research_copy(tmp_path / "copy", _import_days())
    kb.import_research(store=store, path=path, expect_manifest_sha256=sha)
    before = (len(store.read_observations(kb.ETF_SPEC)), len(store.manifests()))
    again = kb.import_research(store=store, path=path, expect_manifest_sha256=sha)
    assert again.imported == 0 and again.skipped_done == len(_import_days())
    assert (len(store.read_observations(kb.ETF_SPEC)), len(store.manifests())) == before
    assert len(store.read_fetch_log(ETF.service)) == len(_import_days())


def test_import_refuses_when_another_run_already_recorded_the_service(
    store: BaselineStore, tmp_path: Path
) -> None:
    _sync(store, FakeClient(), max_calls=None)
    path, sha = _research_copy(tmp_path / "copy", _import_days())
    with pytest.raises(kb.BaselineImportError, match="다른 run_id"):
        kb.import_research(store=store, path=path, expect_manifest_sha256=sha)
    forced = kb.import_research(store=store, path=path, expect_manifest_sha256=sha, force=True)
    assert forced.imported == len(_import_days())


def test_import_stops_on_a_hash_mismatch_before_writing_anything(
    store: BaselineStore, tmp_path: Path
) -> None:
    path, sha = _research_copy(tmp_path / "copy", _import_days())
    with pytest.raises(kb.BaselineImportError, match="기대값"):
        kb.import_research(store=store, path=path, expect_manifest_sha256="0" * 64)
    tampered = path / "raw" / "etf_20100104.json.gz"
    tampered.write_bytes(tampered.read_bytes() + b"x")
    with pytest.raises(kb.BaselineImportError, match="manifest와 다릅니다"):
        kb.import_research(store=store, path=path, expect_manifest_sha256=sha)
    assert not (store.base / "raw_responses").exists() and not store.manifests()


def test_sync_after_import_takes_only_dates_after_the_import(
    store: BaselineStore, tmp_path: Path
) -> None:
    path, sha = _research_copy(tmp_path / "copy", _import_days(), zero_on="20261008")
    kb.import_research(store=store, path=path, expect_manifest_sha256=sha)
    client = FakeClient()
    (result,) = _sync(store, client, max_calls=None)
    wanted = set(_weekday_strs(date(2026, 10, 9), date(2026, 10, 20)))
    # 10-08은 순자산 0이라 대기 → 다시 받는다. 나머지 import 날짜는 안 받는다.
    assert set(client.days(etf_daily.ENDPOINT)) == wanted | {"20261008"}
    assert result.failures == 0
    first = store.read_observations(kb.ETF_SPEC, "first")
    assert (first["obs_seq"] == 1).all()  # import가 obs_seq=1
    later = store.read_observations(kb.ETF_SPEC, "all")
    assert later[(later["BAS_DD"] == "20261008")]["obs_seq"].max() == 2  # 0 → 5000 정정 관측


def test_reobserve_covers_the_import_range_and_resumes_by_run_id(
    store: BaselineStore, tmp_path: Path
) -> None:
    days = _weekday_strs(date(2026, 10, 5), date(2026, 10, 8))
    path, sha = _research_copy(tmp_path / "copy", days)
    kb.import_research(store=store, path=path, expect_manifest_sha256=sha)

    first = FakeClient()
    r1 = kb.backfill(
        store=store,
        client=first,
        service=ETF,
        mode="reobserve",
        run_id="reobs_a",
        start=date(2026, 10, 5),
        end=date(2026, 10, 8),
        max_calls=2,
        today=TODAY,
    )
    assert r1.stopped_by == "max_calls" and first.days(etf_daily.ENDPOINT) == days[:2]
    second = FakeClient()
    r2 = kb.backfill(
        store=store,
        client=second,
        service=ETF,
        mode="reobserve",
        run_id="reobs_a",
        start=date(2026, 10, 5),
        end=date(2026, 10, 8),
        today=TODAY,
    )
    assert second.days(etf_daily.ENDPOINT) == days[2:]  # 같은 run의 나머지만
    assert r1.same + r2.same == 4  # 값이 같으면 요청 기록만 남고 관측은 안 는다
    assert len(store.read_observations(kb.ETF_SPEC)) == 2 * len(days)
    assert len(store.read_fetch_log(ETF.service)) == 8
    third = FakeClient()
    kb.backfill(
        store=store,
        client=third,
        service=ETF,
        mode="reobserve",
        run_id="reobs_b",
        start=date(2026, 10, 5),
        end=date(2026, 10, 8),
        today=TODAY,
    )
    assert third.days(etf_daily.ENDPOINT) == days  # 다른 run은 전 범위
    with pytest.raises(ValueError, match="run-id"):
        kb.backfill(store=store, client=third, service=ETF, mode="reobserve", today=TODAY)


def test_fill_backfill_starts_at_the_service_constant_and_ends_yesterday(
    store: BaselineStore,
) -> None:
    dates = kb.backfill_dates(
        store, ETF, mode="fill", start=None, end=None, run_id=None, today=date(2010, 1, 12)
    )
    assert dates[0] == ETF.history_start and dates[-1] == date(2010, 1, 11)


# ---------------------------------------------------------------------- verify


def _good_store(store: BaselineStore, services=(ETF,)) -> None:
    _sync(store, FakeClient(), services=services, max_calls=None)


def test_verify_passes_on_a_clean_store(store: BaselineStore) -> None:
    _good_store(store, (ETF, BOND, DERIV))
    report = verify(store, today=TODAY)
    assert report.ok, report.render()


def test_verify_fails_when_a_weekday_has_no_completed_record(store: BaselineStore) -> None:
    _good_store(store)
    report = verify(store, today=TODAY + timedelta(days=7), services=["etf_bydd_trd"])
    assert not report.ok
    assert any("완료 요청 기록이 없는 평일" in m for m in report.failures)


def test_verify_fails_on_a_pending_date_older_than_the_window(store: BaselineStore) -> None:
    def responder(endpoint: str, day: str) -> list[dict]:
        return _etf_rows(day, close="")  # 종가 없음 → no_price → 대기

    _sync(store, FakeClient(responder), max_calls=None)
    # 열흘 넘게 지나 창이 앞으로 밀렸고, 옛 날짜는 대기 창 안에서 마지막으로 받았다
    report = verify(store, today=TODAY + timedelta(days=60), services=["etf_bydd_trd"])
    assert any("대기 날짜" in m for m in report.failures)


def test_verify_detects_tampered_files(store: BaselineStore) -> None:
    _good_store(store)
    target = next((store.base / "etf_daily").rglob("*.parquet"))
    target.write_bytes(target.read_bytes() + b"0")
    report = verify(store, today=TODAY, services=["etf_bydd_trd"])
    assert any("manifest 대조" in m for m in report.failures)


def test_verify_warns_on_a_row_count_jump(store: BaselineStore) -> None:
    def responder(endpoint: str, day: str) -> list[dict]:
        return _etf_rows(
            day, codes=tuple("ABCDEFGHIJ" if day < "20261012" else "ABCDEFGHIJKLMNOPQRST")
        )

    _sync(store, FakeClient(responder), max_calls=None)
    report = verify(store, today=TODAY, services=["etf_bydd_trd"])
    assert report.ok and any("±20%" in m for m in report.warnings)


def test_verify_bond_needs_three_groups_and_tr_series_must_be_positive(
    store: BaselineStore,
) -> None:
    def responder(endpoint: str, day: str) -> list[dict]:
        if endpoint == bond_index.ENDPOINT:
            if day == "20261001":
                return _bond_rows(day, groups={"KTB 지수", "KRX 채권지수"})
            return _bond_rows(day, value="-5" if day == "20261002" else "100.5")
        return FakeClient.default(endpoint, day)

    _sync(store, FakeClient(responder), services=(BOND,), max_calls=None)
    report = verify(store, today=TODAY, services=["bon_dd_trd"])
    # 일부만 값이 있으면 partial이지 trading이 아니므로 3그룹 검사에는 안 걸린다
    assert any("0 이하" in m for m in report.failures)
    assert any("점프" in m for m in report.warnings)


def test_verify_reproduces_delisting_ratios_against_the_fixed_reference_day(
    store: BaselineStore, tmp_path: Path
) -> None:
    codes = [f"C{i:03d}" for i in range(100)]
    ref_codes = codes[:62]  # 2010-01-04 목록의 38%가 기준일에 없다
    days = {"20100104": codes, "20261008": ref_codes}
    root, sha = _research_copy(tmp_path / "c", [])
    # 손으로 만든 조사 사본: 위 두 날짜를 직접 쓴다
    lines = ["file\tsha256_gz\tbytes\tmtime_local\trows\trows_with_close\tday_kind"]
    for day, cs in days.items():
        data = gzip.compress(
            json.dumps({"OutBlock_1": _etf_rows(day, codes=tuple(cs))}).encode(), mtime=0
        )
        (root / f"etf_{day}.json.gz").write_bytes(data)
        lines.append(
            f"etf_{day}.json.gz\t{hashlib.sha256(data).hexdigest()}\t{len(data)}\t2026-10-09T22:14:20\t{len(cs)}\t{len(cs)}\ttrading"
        )
    (root / "manifest_files.tsv").write_text("\n".join(lines) + "\n")
    sha = hashlib.sha256((root / "manifest_files.tsv").read_bytes()).hexdigest()
    kb.import_research(store=store, path=root, expect_manifest_sha256=sha)
    report = verify(store, today=date(2026, 10, 9), services=["etf_bydd_trd"])
    assert any("상폐 재현 2010-01-04: 38.0%" in m for m in report.notes), report.render()
    assert any("2015-01-02 목록이 아직 없어 건너뜀" in m for m in report.notes)


# --------------------------------------------------------------------- 날짜 상수


def test_no_function_in_the_baseline_code_freezes_a_date_in_its_default() -> None:
    import importlib
    import inspect
    import pkgutil

    import collector.kr.baseline as baseline_pkg

    modules = [
        importlib.import_module(m.name)
        for m in pkgutil.walk_packages(baseline_pkg.__path__, "collector.kr.baseline.")
    ]
    modules += [
        importlib.import_module(f"collector.kr.service.{n}")
        for n in ("krx_baseline", "krx_baseline_verify", "seibro_dist")
    ]
    frozen = []
    for module in modules:
        for name, fn in vars(module).items():
            if not inspect.isfunction(fn) or fn.__module__ != module.__name__:
                continue
            for pname, param in inspect.signature(fn).parameters.items():
                d = param.default
                if isinstance(d, date) or (isinstance(d, str) and len(d) == 10 and d[4] == "-"):
                    frozen.append(f"{module.__name__}.{name}({pname}={d!r})")
    assert not frozen, frozen


def test_import_accepts_a_flat_layout_but_refuses_a_mixed_one(
    store: BaselineStore, tmp_path: Path
) -> None:
    flat, sha = _research_copy(tmp_path / "flat", _import_days(), flat=True)
    assert kb.import_research(store=store, path=flat, expect_manifest_sha256=sha).imported > 0
    mixed, sha2 = _research_copy(tmp_path / "mixed", _import_days())
    stray = mixed / "etf_20100104.json.gz"
    stray.write_bytes((mixed / "raw" / "etf_20100104.json.gz").read_bytes())
    other = BaselineStore(tmp_path / "other")
    with pytest.raises(kb.BaselineImportError, match="섞여"):
        kb.import_research(store=other, path=mixed, expect_manifest_sha256=sha2)


def test_import_flushes_by_pending_rows_and_rechecks_the_hash_when_reading_again(
    store: BaselineStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from collector.kr.baseline import writer as writer_mod

    flushes: list[int] = []
    original = writer_mod.BaselineWriter.flush

    def spy(self):  # noqa: ANN001
        flushes.append(sum(len(r) for r in self._obs.values()))
        return original(self)

    monkeypatch.setattr(writer_mod.BaselineWriter, "flush", spy)
    days = _import_days()
    path, sha = _research_copy(tmp_path / "copy", days)  # 날짜당 2행, 요청 수는 200 미만
    original_init = writer_mod.BaselineWriter.__init__

    def init(self, *a, **kw):  # noqa: ANN001
        kw.setdefault("max_pending_rows", 5)
        original_init(self, *a, **kw)

    monkeypatch.setattr(writer_mod.BaselineWriter, "__init__", init)
    kb.import_research(store=store, path=path, expect_manifest_sha256=sha)
    assert len([f for f in flushes if f]) >= len(days) * 2 // 6  # 행 수 기준으로 여러 번
    assert max(flushes) <= 6  # 확정 직전 쌓인 행이 상한 근처를 넘지 않는다
    assert len(store.read_observations(kb.ETF_SPEC)) == 2 * len(days)

    # 확인과 들이기 사이에 파일이 바뀌면 다시 대조해 멈춘다
    other = BaselineStore(tmp_path / "other")
    path2, sha2 = _research_copy(tmp_path / "copy2", days)
    target = path2 / "raw" / f"etf_{days[3]}.json.gz"
    real = kb._sha256_bytes
    calls = {"n": 0}

    def flaky(data: bytes) -> str:
        if data == target.read_bytes():
            calls["n"] += 1
            if calls["n"] == 2:  # 두 번째 읽기(들이기)에서만 달라진 것처럼
                return "0" * 64
        return real(data)

    monkeypatch.setattr(kb, "_sha256_bytes", flaky)
    with pytest.raises(kb.BaselineImportError, match="바뀌었습니다"):
        kb.import_research(store=other, path=path2, expect_manifest_sha256=sha2)
