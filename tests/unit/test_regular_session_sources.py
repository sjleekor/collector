"""Same-day MDC source and the combined source (fake client, no network)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest

from collector.kr.domain.models import KrxSessionRow, StoredOhlc
from collector.kr.service.fix_regular_session import fix_regular_session
from collector.kr.service.regular_session_sources import (
    MDC_BLD,
    OPENAPI_SOURCE_NAME,
    REJECT_INCONSISTENT,
    REJECT_NONPOSITIVE,
    REJECT_TOO_EARLY,
    CombinedSource,
    KrxMdcSameDaySource,
    MdcTooEarlyError,
)
from tests.unit.test_fix_regular_session import FakeStorage

KST = timezone(timedelta(hours=9))
T = date(2026, 10, 19)  # Monday
Y = date(2026, 10, 16)  # previous Friday


def _at(day: date, hh: int, mm: int) -> datetime:
    return datetime(day.year, day.month, day.day, hh, mm, tzinfo=KST)


def _rec(code: str, o: str, h: str, low: str, c: str) -> dict[str, Any]:
    return {
        "ISU_SRT_CD": code,
        "TDD_OPNPRC": o,
        "TDD_HGPRC": h,
        "TDD_LWPRC": low,
        "TDD_CLSPRC": c,
        "ACC_TRDVOL": "1,000",
    }


class FakeClient:
    def __init__(self, data: dict[tuple[str, str], list[dict[str, Any]]]) -> None:
        self.data = data
        self.calls: list[dict[str, Any]] = []

    def post_rows(self, bld: str, params: dict[str, Any], *, output_key: str) -> list[Any]:
        assert bld == MDC_BLD and output_key == "OutBlock_1"
        assert params["share"] == "1" and params["money"] == "1" and params["locale"] == "ko_KR"
        self.calls.append(params)
        return list(self.data.get((params["trdDd"], params["mktId"]), []))


def _src(client: FakeClient, now: datetime, **kw: Any) -> KrxMdcSameDaySource:
    return KrxMdcSameDaySource(client, now_fn=lambda: now, **kw)


def test_parses_commas_maps_markets_and_tags_source() -> None:
    client = FakeClient(
        {
            ("20261019", "STK"): [_rec("005930", "220,000", "219,500", "217,000", "218,000")],
            ("20261019", "KSQ"): [_rec("035720", "50,000", "51,000", "49,000", "50,500")],
        }
    )
    # open 220,000 > high 219,500 would be inconsistent; use a valid one for this test
    client.data[("20261019", "STK")] = [_rec("005930", "218,500", "219,500", "217,000", "218,000")]
    rows = _src(client, _at(T, 21, 0)).get_session_rows(T, T)
    by_ticker = {r.ticker: r for r in rows}
    assert by_ticker["005930"] == KrxSessionRow(
        "005930", "KOSPI", T, 218500, 219500, 217000, 218000, "krx_mdc_MDCSTAT01501"
    )
    assert by_ticker["035720"].market == "KOSDAQ" and by_ticker["035720"].close == 50500
    assert {c["mktId"] for c in client.calls} == {"STK", "KSQ"}


def test_sanity_filter_flags_inconsistent_and_nonpositive_rows() -> None:
    client = FakeClient(
        {
            ("20261019", "STK"): [
                _rec("000001", "100", "110", "90", "105"),
                _rec("000002", "120", "110", "90", "105"),  # open > high
                _rec("000003", "100", "110", "90", "85"),  # close < low
                _rec("000004", "0", "0", "0", "0"),  # halted
                _rec("000005", "-", "110", "90", "100"),  # missing
            ]
        }
    )
    src = _src(client, _at(T, 21, 0))
    rows = {r.ticker: r for r in src.get_session_rows(T, T)}
    assert rows["000001"].open == 100
    for bad in ("000002", "000003", "000004", "000005"):
        assert rows[bad].open is None and rows[bad].close is None  # flagged -> krx_invalid
    reasons = {key.split("/")[0]: why for key, why in src.rejected}
    assert reasons == {
        "000002": REJECT_INCONSISTENT,
        "000003": REJECT_INCONSISTENT,
        "000004": REJECT_NONPOSITIVE,
        "000005": REJECT_NONPOSITIVE,
    }


def test_refuses_same_day_before_2030_unless_overridden() -> None:
    client = FakeClient({})
    with pytest.raises(MdcTooEarlyError):
        _src(client, _at(T, 20, 29)).get_session_rows(T, T)
    assert client.calls == []
    _src(client, _at(T, 20, 30)).get_session_rows(T, T)
    assert client.calls
    client.calls.clear()
    _src(client, _at(T, 20, 0), allow_early=True).get_session_rows(T, T)
    assert client.calls


def test_past_days_are_not_guarded_and_weekend_days_are_skipped() -> None:
    client = FakeClient({})
    _src(client, _at(T, 9, 0)).get_session_rows(Y, Y + timedelta(days=2))  # Fri, Sat, Sun
    assert {c["trdDd"] for c in client.calls} == {"20261016"}


def test_range_through_today_before_2030_raises() -> None:
    with pytest.raises(MdcTooEarlyError):
        _src(FakeClient({}), _at(T, 9, 0)).get_session_rows(Y, T + timedelta(days=5))


def _cap_row(day: date, ticker: str = "005930", c: int = 1040) -> KrxSessionRow:
    return KrxSessionRow(ticker, "KOSPI", day, 1000, 1080, 900, c)


def _stored(day: date, ticker: str = "005930") -> StoredOhlc:
    return StoredOhlc(ticker, "KOSPI", day, 1000, 1100, 900, 1050)


def test_combined_uses_mdc_only_after_latest_openapi_date_and_openapi_wins() -> None:
    storage = FakeStorage([_stored(Y), _stored(T)], [_cap_row(Y)])
    client = FakeClient({("20261019", "STK"): [_rec("005930", "1000", "1070", "900", "1030")]})
    combined = CombinedSource(storage, _src(client, _at(T, 21, 0)), now_fn=lambda: _at(T, 21, 0))
    rows = {(r.trade_date): r for r in combined.get_session_rows(Y - timedelta(days=3), T)}
    assert rows[Y].source == OPENAPI_SOURCE_NAME and rows[Y].close == 1040
    assert rows[T].source == "krx_mdc_MDCSTAT01501" and rows[T].close == 1030
    assert {c["trdDd"] for c in client.calls} == {"20261019"}  # MDC only for T


def test_combined_openapi_overrides_mdc_when_it_arrives() -> None:
    # Both days have OpenAPI rows now: MDC is not asked and the OpenAPI close wins.
    storage = FakeStorage([_stored(Y), _stored(T)], [_cap_row(Y), _cap_row(T, c=1035)])
    client = FakeClient({("20261019", "STK"): [_rec("005930", "1000", "1070", "900", "1030")]})
    combined = CombinedSource(storage, _src(client, _at(T, 21, 0)), now_fn=lambda: _at(T, 21, 0))
    rows = {r.trade_date: r for r in combined.get_session_rows(Y, T)}
    assert rows[T].close == 1035 and rows[T].source == OPENAPI_SOURCE_NAME
    assert client.calls == []


def test_combined_degrades_to_openapi_before_2030() -> None:
    storage = FakeStorage([_stored(Y), _stored(T)], [_cap_row(Y)])
    client = FakeClient({})
    now = _at(T, 20, 5)
    combined = CombinedSource(storage, _src(client, now), now_fn=lambda: now)
    rows = combined.get_session_rows(Y, T)
    assert [r.trade_date for r in rows] == [Y]
    assert combined.rejected[0][1] == REJECT_TOO_EARLY and client.calls == []


def test_end_to_end_audit_records_row_level_source_then_openapi_refix() -> None:
    storage = FakeStorage([_stored(Y), _stored(T)], [_cap_row(Y)])
    client = FakeClient({("20261019", "STK"): [_rec("005930", "1000", "1070", "900", "1030")]})
    now = _at(T, 21, 0)
    combined = CombinedSource(storage, _src(client, now), now_fn=lambda: now)
    report, run = fix_regular_session(storage, Y, T, source=combined, allow_before_cutoff=True)
    assert report.applied == 2 and run.params["krx_source"] == "combined"
    srcs = {a["plan"].trade_date: a["plan"].krx_source for a in storage.audit}
    assert srcs == {Y: OPENAPI_SOURCE_NAME, T: "krx_mdc_MDCSTAT01501"}
    assert storage.ohlc[("005930", "KOSPI", T)].close == 1030

    # OpenAPI delivers T with a different close: the next run overwrites and audits it.
    storage.krx.append(_cap_row(T, c=1035))
    combined2 = CombinedSource(storage, _src(client, now), now_fn=lambda: now)
    report2, _ = fix_regular_session(storage, Y, T, source=combined2, allow_before_cutoff=True)
    assert report2.applied == 1
    assert storage.ohlc[("005930", "KOSPI", T)].close == 1035
    last = storage.audit[-1]["plan"]
    assert last.krx_source == OPENAPI_SOURCE_NAME and (last.old_close, last.new_close) == (
        1030,
        1035,
    )


def test_flagged_mdc_rows_show_up_as_skipped_and_source_rejected() -> None:
    storage = FakeStorage([_stored(T)], [])
    client = FakeClient({("20261019", "STK"): [_rec("005930", "1000", "900", "950", "960")]})
    now = _at(T, 21, 0)
    report, run = fix_regular_session(
        storage, T, T, source=_src(client, now), allow_before_cutoff=True
    )
    assert report.skipped_by_reason() == {"krx_invalid": 1}
    assert run.counts["source_rejected"] == {REJECT_INCONSISTENT: 1}
