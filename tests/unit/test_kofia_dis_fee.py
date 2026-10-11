"""DIS 보수 어댑터 — 요청·월말 후보·끊긴 응답 재시도·파서. 네트워크 없음."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import requests

from collector.kr.adapters.kofia_dis_fee import (
    DisClient,
    DisParseError,
    DisRequestError,
    build_fee_request,
    build_latest_std_request,
    is_complete,
    is_plausible_month_end,
    month_end_candidates,
    next_month,
    parse_fee_rows,
    parse_latest_std_date,
    parse_month,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "kofia_dis_fee"


def body(name: str) -> bytes:
    return (FIX / name).read_bytes()


# ------------------------------------------------------------------ 요청 본문

#: 연구용 사본을 받을 때 쓴 스크립트(dis_fee_pull.py)의 본문 — 글자 그대로 같아야 한다.
RESEARCH_BODY = """<?xml version="1.0" encoding="utf-8"?>
<message>
  <proframeHeader>
    <pfmAppName>FS-DIS2</pfmAppName>
    <pfmSvcName>DISFundFeeCmsSO</pfmSvcName>
    <pfmFnName>select</pfmFnName>
  </proframeHeader>
  <systemHeader></systemHeader>
    <DISCondFuncDTO>
    <tmpV30>{d}</tmpV30>
    <tmpV11></tmpV11>
    <tmpV12>상장지수</tmpV12>
    <tmpV3></tmpV3>
    <tmpV5></tmpV5>
    <tmpV4></tmpV4>
</DISCondFuncDTO>
</message>
"""


def test_fee_request_is_identical_to_the_research_pull() -> None:
    assert build_fee_request("20260831") == RESEARCH_BODY.format(d="20260831")


@pytest.mark.parametrize("bad", ["2026-08-31", "202608", "", "2026083a"])
def test_fee_request_rejects_a_malformed_date(bad: str) -> None:
    with pytest.raises(ValueError):
        build_fee_request(bad)


def test_latest_request_names_the_service_and_code() -> None:
    xml = build_latest_std_request()
    assert "DISComStdYMDSO" in xml and "J_RD" in xml


# ------------------------------------------------------------------ 월말 후보


def test_month_end_candidates_step_back_over_weekdays_up_to_three() -> None:
    # 2026-08-31은 월요일 → 월·금·목·수
    assert month_end_candidates((2026, 8)) == [
        date(2026, 8, 31), date(2026, 8, 28), date(2026, 8, 27), date(2026, 8, 26),
    ]  # fmt: skip
    # 2026-05-31은 일요일 → 말일 이하 마지막 평일은 금요일 5/29
    assert month_end_candidates((2026, 5))[0] == date(2026, 5, 29)
    assert len(month_end_candidates((2026, 5))) == 4
    # 2009-05-29(금)는 연구용 사본에서 끊김이 났던 날짜다 — 5월 말일 5/31은 일요일
    assert month_end_candidates((2009, 5))[0] == date(2009, 5, 29)


def test_candidates_cross_month_and_year_edges() -> None:
    assert month_end_candidates((2026, 12))[0] == date(2026, 12, 31)
    assert next_month((2026, 12)) == (2027, 1)
    assert month_end_candidates((2027, 2))[0] == date(2027, 2, 26)  # 2/28은 일요일


def test_plausible_month_end() -> None:
    assert is_plausible_month_end(date(2026, 8, 31))
    assert is_plausible_month_end(date(2026, 8, 27))
    assert not is_plausible_month_end(date(2026, 8, 15))


def test_parse_month() -> None:
    assert parse_month("2026-08") == (2026, 8)
    for bad in ("2026-13", "202608", "26-08", "2026-8x"):
        with pytest.raises(ValueError):
            parse_month(bad)


# ------------------------------------------------------------------ 클라이언트


class _Resp:
    def __init__(self, content: bytes, status: int = 200) -> None:
        self.content = content
        self.status_code = status


class _Session:
    def __init__(self, script: list) -> None:
        self.script = list(script)
        self.sent: list[bytes] = []

    def post(self, url, data=None, headers=None, timeout=None):  # noqa: ANN001
        self.sent.append(data)
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _client(session: _Session, sleeps: list[float], **kw) -> DisClient:
    return DisClient(
        session=session, sleep_fn=sleeps.append, min_interval_seconds=3.0,
        retry_sleep_seconds=10.0, monotonic_fn=lambda: 0.0, **kw,
    )  # fmt: skip


def test_is_complete_requires_root_end() -> None:
    assert is_complete(b"<root>x</root>\n")
    assert not is_complete(b"<root><message>")


def test_truncated_body_is_retried_with_the_same_request() -> None:
    good = body("fee_20260831.xml")
    session = _Session([_Resp(good[:54]), _Resp(good)])
    sleeps: list[float] = []
    client = _client(session, sleeps)
    xml = build_fee_request("20260831")
    raw = client.post(xml)
    assert raw.body == good and raw.status_code == 200
    assert session.sent == [xml.encode("utf-8")] * 2  # 같은 날짜를 다시 보냈다
    assert client.counters.truncated == 1
    assert client.counters.http_requests == 2 and client.counters.http_retries == 1
    assert 10.0 in sleeps  # 재시도 전 대기


def test_gives_up_after_bounded_attempts_and_never_returns_a_truncated_body() -> None:
    session = _Session([_Resp(b"<root><mess")] * 3)
    client = _client(session, [])
    with pytest.raises(DisRequestError, match="truncated"):
        client.post("<x/>")
    assert client.counters.http_requests == 3


def test_connection_errors_and_5xx_are_retried_but_404_is_not() -> None:
    good = body("fee_20260731.xml")
    session = _Session([requests.ConnectionError("reset"), _Resp(b"", 503), _Resp(good)])
    assert _client(session, []).post("<x/>").body == good
    session = _Session([_Resp(b"nope", 404)])
    client = _client(session, [])
    with pytest.raises(DisRequestError, match="HTTP 404"):
        client.post("<x/>")
    assert client.counters.http_requests == 1


def test_requests_are_paced_by_the_polite_interval() -> None:
    good = body("fee_20260731.xml")
    session = _Session([_Resp(good), _Resp(good)])
    sleeps: list[float] = []
    now = [0.0]
    client = DisClient(
        session=session, sleep_fn=sleeps.append, min_interval_seconds=3.0,
        monotonic_fn=lambda: now[0],
    )  # fmt: skip
    client.post("<x/>")
    now[0] = 1.0
    client.post("<x/>")
    assert sleeps == [2.0]


# --------------------------------------------------------------------- 파서


def test_parse_fixture_rows_and_units() -> None:
    parsed = parse_fee_rows(body("fee_20260831.xml"), "20260831")
    assert parsed.total_count == 3 and len(parsed.rows) == 3
    assert parsed.unexpected_filled == 0
    first = parsed.rows[0]
    assert first["STD_YMD"] == "20260831" and first["FUND_CODE"] == "K55207EE5208"
    # 원문 칸은 문자열 그대로(앞의 0이 없다), 변환 칸은 퍼센트 float
    assert first["FEE_TOTAL"] == ".405" and first["fee_total_num"] == pytest.approx(0.405)
    assert first["TER"] == ".47" and first["ter_num"] == pytest.approx(0.47)
    assert first["std_date"] == date(2026, 8, 31)
    assert first["COMPANY_NM"] == "교보자산운용"
    assert "상장지수" in first["FUND_NM"]
    # 산식: 합계 = 운용+판매+수탁+사무관리, TER = 합계+기타비용
    for row in parsed.rows:
        parts = sum(row[f"fee_{k}_num"] for k in ("mgmt", "sell", "trust", "admin"))
        assert parts == pytest.approx(row["fee_total_num"], abs=0.011)
        assert row["fee_total_num"] + row["cost_other_num"] == pytest.approx(
            row["ter_num"], abs=0.011
        )


def test_parse_old_row_keeps_negative_other_cost_as_given() -> None:
    row = parse_fee_rows(body("fee_20030530.xml"), "20030530").rows[0]
    assert row["COST_OTHER"] == "-.67" and row["cost_other_num"] == pytest.approx(-0.67)
    assert row["TER"] == "0" and row["FUND_CODE"] == "KR5105352888"
    assert row["setup_date"] == date(2002, 10, 11)


def test_parse_rejects_count_mismatch_and_duplicate_codes() -> None:
    text = body("fee_20260831.xml").decode()
    with pytest.raises(DisParseError, match="dbio_total_count_"):
        wrong = text.replace("<dbio_total_count_>3<", "<dbio_total_count_>4<")
        parse_fee_rows(wrong.encode(), "20260831")
    dup = text.replace("KR5207A24857", "K55207EE5208")
    with pytest.raises(DisParseError, match="같은 표준코드"):
        parse_fee_rows(dup.encode(), "20260831")
    with pytest.raises(DisParseError):
        parse_fee_rows(b"<root><message>", "20260831")


def test_parse_flags_values_in_columns_we_do_not_store() -> None:
    text = body("fee_20260831.xml").decode().replace("<tmpV17 />", "<tmpV17>9</tmpV17>", 1)
    assert parse_fee_rows(text.encode(), "20260831").unexpected_filled == 1


def test_latest_std_date_ignores_the_header_transaction_date() -> None:
    xml = (
        b"<root><message><proframeHeader><pfmTrDate>20261011</pfmTrDate></proframeHeader>"
        b"<DISCondFuncListDTO><selectMeta><v>20260831</v></selectMeta>"
        b"<selectMeta><v>2026-07-31</v></selectMeta></DISCondFuncListDTO></message></root>"
    )
    assert parse_latest_std_date(xml, date(2026, 10, 11)) == date(2026, 8, 31)
    # 오늘보다 늦은 값은 버린다
    assert parse_latest_std_date(xml, date(2026, 8, 1)) == date(2026, 7, 31)
    assert parse_latest_std_date(b"<root><message/></root>") is None
    assert parse_latest_std_date(b"garbage") is None
