"""SEIBro 분배금 서비스 — 창 고르기·완료/미완료·absent·꺼짐 (R07·R08)."""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from collector.kr.adapters.seibro_distribution import (
    EARLIEST_RGT_STD_DT,
    SOURCE_COLUMNS,
    SeibroRawResponse,
    SeibroRequestError,
)
from collector.kr.adapters.seibro_distribution.request import (
    COUNT_ACTION,
    LIST_ACTION,
    PAGE_SIZE,
)
from collector.kr.baseline import BaselineStore
from collector.kr.service import seibro_dist as sd

TODAY = date(2026, 10, 14)  # 수요일 — 분기 첫 일요일이 아니다
ENV_ON: dict[str, str] = {}


def _row(isin: str, std: str, kind: str = "이익분배", amount: str = "125") -> dict[str, str]:
    row = {c: "" for c in SOURCE_COLUMNS}
    row.update(
        ISIN=isin,
        RGT_STD_DT=std,
        RGT_RSN_DTAIL_NM=kind,
        ESTM_STDPRC=amount,
        TH1_PAY_TERM_BEGIN_DT=std,
        TAXSTD="100",
        KOR_SECN_NM="etf",
    )
    return row


def _xml_list(rows: list[dict[str, str]]) -> bytes:
    body = "".join(
        "<result>" + "".join(f'<{k} value="{v}"/>' for k, v in r.items()) + "</result>"
        for r in rows
    )
    return f"<vector><data>{body}</data></vector>".encode()


def _xml_count(n: int) -> bytes:
    return f'<result><LIST_CNT value="{n}"/></result>'.encode()


class FakeSeibro:
    """``rows``에서 요청 구간의 행을 골라 건수·페이지 응답을 만든다."""

    def __init__(self, rows: list[dict[str, str]], *, fail_page: int | None = None) -> None:
        self.rows = rows
        self.fail_page = fail_page
        self.counters = SimpleNamespace(http_requests=0)

    def post(self, xml, *, range_start=None, range_end=None, page=None):  # noqa: ANN001
        self.counters.http_requests += 1
        action = COUNT_ACTION if f'action="{COUNT_ACTION}"' in xml else LIST_ACTION
        lo = re.search(r'fromRGT_STD_DT value="(\d{8})"', xml)[1]
        hi = re.search(r'toRGT_STD_DT value="(\d{8})"', xml)[1]
        chosen = sorted(
            (r for r in self.rows if lo <= r["RGT_STD_DT"] <= hi),
            key=lambda r: (r["RGT_STD_DT"], r["ISIN"]),
        )
        if action == COUNT_ACTION:
            body = _xml_count(len(chosen))
        else:
            if self.fail_page is not None and page == self.fail_page:
                raise SeibroRequestError("HTTP 500")
            first = (page - 1) * PAGE_SIZE
            body = _xml_list(chosen[first : first + PAGE_SIZE])
        return SeibroRawResponse(
            body=body,
            fetched_at=datetime.now(UTC),
            status_code=200,
            action=action,
            range_start=range_start,
            range_end=range_end,
            page=page,
        )


@pytest.fixture
def store(tmp_path: Path) -> BaselineStore:
    return BaselineStore(tmp_path / "krx_baseline")


def _sync(store, client, today=TODAY, **kw):
    return sd.sync(store=store, client=client, env=kw.pop("env", ENV_ON), today=today, **kw)


def test_the_first_run_takes_everything_from_the_earliest_date(store: BaselineStore) -> None:
    client = FakeSeibro([_row("KR7000000001", "20240105")])
    result = _sync(store, client)
    assert (result.start, result.end) == (EARLIEST_RGT_STD_DT, TODAY)
    assert result.complete and result.full
    obs = store.read_observations(sd.SPEC, "all")
    assert len(obs) == 1 and obs.iloc[0]["obs_seq"] == 1
    assert obs.iloc[0]["rgt_std_date"] == date(2024, 1, 5)
    assert bool(obs.iloc[0]["tax_std_is_zero"]) is False
    # 원문은 페이지마다 xml 그대로, 요청 기록은 응답마다 한 줄 + 창 요약 한 줄
    log = store.read_fetch_log(sd.SERVICE)
    summary = log[log["request_key"] == sd.window_key(EARLIEST_RGT_STD_DT, TODAY)].iloc[0]
    assert summary["day_kind"] == "complete" and summary["row_count"] == 1
    assert obs.iloc[0]["raw_path"].endswith(".xml.gz")
    assert "/p=001/" in obs.iloc[0]["raw_path"]
    assert log["request_key"].str.contains("/count").any()


def test_the_next_window_starts_90_days_before_the_last_complete_end(
    store: BaselineStore,
) -> None:
    _sync(store, FakeSeibro([]))
    assert sd.last_complete_window_end(store) == TODAY
    later = TODAY + timedelta(days=7)
    result = _sync(store, FakeSeibro([]), today=later)
    assert result.start == TODAY - timedelta(days=90) and not result.full


def test_after_a_120_day_gap_the_window_reaches_back_from_the_last_complete_end(
    store: BaselineStore,
) -> None:
    _sync(store, FakeSeibro([]))
    result = _sync(store, FakeSeibro([]), today=TODAY + timedelta(days=120))
    assert result.start == TODAY - timedelta(days=90)  # 오늘−90일이 아니라 마지막 완료 끝−90일


def test_a_quarterly_first_sunday_re_checks_everything(store: BaselineStore) -> None:
    _sync(store, FakeSeibro([]))
    result = _sync(store, FakeSeibro([]), today=date(2027, 1, 3))  # 1월 첫 일요일
    assert result.full and result.start == EARLIEST_RGT_STD_DT
    assert _sync(store, FakeSeibro([]), today=date(2027, 1, 4)).full is False


def test_disabled_makes_no_request(store: BaselineStore) -> None:
    client = FakeSeibro([_row("KR7000000001", "20240105")])
    result = _sync(store, client, env={"SDC_SEIBRO_ENABLED": "0"})
    assert not result.enabled and client.counters.http_requests == 0
    assert not store.manifests()


def test_a_vanished_distribution_row_becomes_absent_in_a_complete_window(
    store: BaselineStore,
) -> None:
    keep = _row("KR7000000001", "20260930")
    gone = _row("KR7000000002", "20260930")
    _sync(store, FakeSeibro([keep, gone]))
    result = _sync(store, FakeSeibro([keep]), today=TODAY + timedelta(days=7))
    assert result.complete
    obs = store.read_observations(sd.SPEC, "all")
    dropped = obs[obs["ISIN"] == "KR7000000002"].sort_values("obs_seq")
    assert list(dropped["obs_kind"]) == ["value", "absent"]
    assert list(store.read_observations(sd.SPEC, "latest")["ISIN"]) == ["KR7000000001"]
    first = set(store.read_observations(sd.SPEC, "first")["ISIN"])
    assert first == {"KR7000000001", "KR7000000002"}


def test_a_whole_record_date_that_disappears_is_absent_too(store: BaselineStore) -> None:
    _sync(store, FakeSeibro([_row("KR7000000001", "20260930")]))
    _sync(store, FakeSeibro([]), today=TODAY + timedelta(days=7))
    obs = store.read_observations(sd.SPEC, "all")
    assert list(obs.sort_values("obs_seq")["obs_kind"]) == ["value", "absent"]


def test_an_incomplete_window_makes_no_absent_and_is_not_the_last_complete_window(
    store: BaselineStore,
) -> None:
    rows = [_row(f"KR70000000{i:02d}", "20260930") for i in range(PAGE_SIZE + 5)]
    _sync(store, FakeSeibro(rows))
    broken = FakeSeibro(rows[:3], fail_page=1)  # 목록 페이지 하나가 실패
    result = _sync(store, broken, today=TODAY + timedelta(days=7))
    assert not result.complete
    obs = store.read_observations(sd.SPEC, "all")
    assert (obs["obs_kind"] == "value").all()  # absent 없음
    assert sd.last_complete_window_end(store) == TODAY  # 미완료 창은 끝으로 안 친다
    assert "incomplete" in set(store.read_fetch_log(sd.SERVICE)["day_kind"].dropna())


def test_two_pages_link_each_row_to_the_page_it_came_from(store: BaselineStore) -> None:
    rows = [_row(f"KR70000000{i:02d}", "20260930") for i in range(PAGE_SIZE + 5)]
    _sync(store, FakeSeibro(rows))
    obs = store.read_observations(sd.SPEC, "all")
    assert len(obs) == PAGE_SIZE + 5
    assert {p.split("/")[-2] for p in obs["raw_path"]} == {"p=001", "p=002"}
    assert not obs.duplicated(["ISIN", "RGT_STD_DT", "RGT_RSN_DTAIL_NM", "obs_seq"]).any()


def test_the_same_window_again_adds_no_observation_but_logs_the_requests(
    store: BaselineStore,
) -> None:
    rows = [_row("KR7000000001", "20260930")]
    _sync(store, FakeSeibro(rows))
    before = len(store.read_fetch_log(sd.SERVICE))
    result = _sync(store, FakeSeibro(rows), today=TODAY + timedelta(days=7))
    assert not result.new_obs
    assert len(store.read_observations(sd.SPEC, "all")) == 1
    assert len(store.read_fetch_log(sd.SERVICE)) > before
