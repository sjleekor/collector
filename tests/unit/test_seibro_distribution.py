"""SEIBro 분배금 어댑터.

fixture(``tests/fixtures/seibro/resp_*.xml``)는 서버 조사 원본
(``r4_baseline_20261009/etf_dist/seibro/resp_k200_pre2010.xml``·``resp_brics.xml``·
``resp_k200_2y.xml``·``resp_c_2022.xml``)에서 발췌한 **실제 응답**입니다. 행만 4개로
줄였고 나머지는 원문 그대로입니다.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import requests

from collector.kr.adapters.seibro_distribution import (
    EARLIEST_RGT_STD_DT,
    KEY_COLUMNS,
    PARSED_COLUMNS,
    SOURCE_COLUMNS,
    SeibroClient,
    SeibroMalformedResponseError,
    fetch_window,
    is_quarterly_full_day,
    parse_count,
    parse_rows,
    seibro_enabled,
    weekly_window,
    year_chunks,
)
from collector.kr.adapters.seibro_distribution.request import (
    build_count_request,
    build_list_request,
)

FIX = Path(__file__).parent.parent / "fixtures" / "seibro"
LIST_BODY = (FIX / "resp_list_excerpt.xml").read_bytes()
COUNT_BODY = (FIX / "resp_count_2022.xml").read_bytes()


def _count_body(n: int) -> bytes:
    return f'<vector><data><result><LIST_CNT value="{n}"/></result></data></vector>'.encode()


def _rows_body(n: int, offset: int = 0) -> bytes:
    items = "".join(
        f'<data><result><ISIN value="KR7{offset + i:06d}007"/>'
        f'<RGT_STD_DT value="20240131"/><RGT_RSN_DTAIL_NM value="이익분배"/>'
        f'<ESTM_STDPRC value="10"/></result></data>'
        for i in range(n)
    )
    return f"<vector>{items}</vector>".encode()


class _Resp:
    def __init__(self, content: bytes, status: int = 200) -> None:
        self.content = content
        self.status_code = status


class _Session:
    """요청 XML의 액션과 페이지로 응답을 고르는 가짜 session."""

    def __init__(self, total: int, fail_page: int | None = None, count: int | None = None):
        self.total = total
        self.fail_page = fail_page
        self.count = total if count is None else count
        self.calls: list[str] = []

    def post(self, url, data, headers, timeout):
        xml = data.decode()
        self.calls.append(xml)
        if "Cnt" in xml.split(">")[0]:
            return _Resp(_count_body(self.count))
        start = int(xml.split('START_PAGE value="')[1].split('"')[0])
        page = (start - 1) // 30 + 1
        if page == self.fail_page:
            return _Resp(b"", 500)
        n = max(0, min(30, self.total - (page - 1) * 30))
        return _Resp(_rows_body(n, offset=(page - 1) * 30))


def _client(session, sleeps=None):
    sleeps = sleeps if sleeps is not None else []
    return SeibroClient(session=session, sleep_fn=sleeps.append, max_attempts=1), sleeps


def test_parse_rows_preserves_raw_and_converts() -> None:
    rows = parse_rows(LIST_BODY)
    assert len(rows) == 4
    zero_tax, normal, liquidation, frac = rows
    assert zero_tax.raw["ESTM_STDPRC"] == "50"
    assert zero_tax.raw["BUNBE"] == ".24414"  # 원문 그대로(앞자리 0 없음)
    assert zero_tax.isin == "KR7069500007" and zero_tax.stock_code == "069500"
    assert zero_tax.rgt_std_date == date(2009, 7, 31)
    assert zero_tax.pay_begin_date == date(2009, 8, 6)
    assert zero_tax.per_share_amount == 50.0
    assert zero_tax.dist_rate_pct == pytest.approx(0.24414)
    assert zero_tax.tax_std_price is None and zero_tax.tax_std_is_zero
    assert zero_tax.raw["TAXSTD"] == "0"
    assert normal.tax_std_price == pytest.approx(7826.7289627831324)
    assert not normal.tax_std_is_zero
    assert liquidation.raw["BUNBE"] == "" and liquidation.dist_rate_pct is None
    assert liquidation.per_share_amount == pytest.approx(4915.55976166667)
    assert liquidation.dist_type == "청산분배"
    assert frac.dist_rate_pct == pytest.approx(0.02059)
    assert zero_tax.key == ("KR7069500007", "20090731", "이익분배")


def test_to_record_and_column_names() -> None:
    record = parse_rows(LIST_BODY)[0].to_record()
    assert tuple(record) == SOURCE_COLUMNS + PARSED_COLUMNS
    assert record["ISIN"] == "KR7069500007" and record["TAXSTD"] == "0"
    assert isinstance(record["rgt_std_date"], date)
    assert set(KEY_COLUMNS) <= set(SOURCE_COLUMNS)


def test_column_names_unique_case_insensitive() -> None:
    names = [c.lower() for c in SOURCE_COLUMNS + PARSED_COLUMNS]
    assert len(names) == len(set(names))


def test_source_columns_match_real_response() -> None:
    first = parse_rows(LIST_BODY)[0]
    assert tuple(first.raw) == SOURCE_COLUMNS


def test_parse_count() -> None:
    assert parse_count(COUNT_BODY) == 512
    with pytest.raises(SeibroMalformedResponseError):
        parse_count(LIST_BODY)


def test_request_shape() -> None:
    xml = build_list_request(date(2026, 1, 1), date(2026, 3, 31), page=2)
    assert 'action="exerInfoDtramtPayStatPlist"' in xml
    # pull.py가 보낸 건수 요청(req_c_2022.xml)과 글자 그대로 같다.
    assert build_count_request(date(2022, 1, 1), date(2022, 12, 31)) == (
        '<reqParam action="exerInfoDtramtPayStatPlistCnt" '
        'task="ksd.safe.bip.cnts.etf.process.EtfExerInfoPTask"><etf_sort_cd value=""/>'
        '<etf_big_sort_cd value=""/><isin value=""/><mngco_custno value=""/>'
        '<RGT_RSN_DTAIL_SORT_CD value=""/><fromRGT_STD_DT value="20220101"/>'
        '<toRGT_STD_DT value="20221231"/><START_PAGE value="1"/><END_PAGE value="30"/></reqParam>'
    )
    assert 'fromRGT_STD_DT value="20260101"' in xml
    assert 'START_PAGE value="31"' in xml and 'END_PAGE value="60"' in xml
    assert 'action="exerInfoDtramtPayStatPlistCnt"' in build_count_request(
        date(2026, 1, 1), date(2026, 3, 31)
    )


def test_fetch_window_complete() -> None:
    session = _Session(total=65)
    client, _ = _client(session)
    result = fetch_window(client, date(2024, 1, 1), date(2024, 12, 31))
    assert result.complete
    assert len(result.rows) == 65
    assert (result.chunks[0].expected, result.chunks[0].actual) == (65, 65)
    assert len(result.raw_responses) == 1 + 3  # 건수 1 + 페이지 3
    assert result.raw_responses[1].page == 1 and result.raw_responses[1].status_code == 200
    assert result.raw_responses[0].fetched_at.tzinfo is not None
    assert client.counters.http_requests == 4


def test_list_cnt_mismatch_is_incomplete() -> None:
    client, _ = _client(_Session(total=65, count=70))
    result = fetch_window(client, date(2024, 1, 1), date(2024, 12, 31))
    assert not result.complete
    assert (result.chunks[0].expected, result.chunks[0].actual) == (70, 65)
    assert len(result.rows) == 65


def test_page_failure_is_incomplete_and_keeps_raw() -> None:
    client, _ = _client(_Session(total=65, fail_page=2))
    result = fetch_window(client, date(2024, 1, 1), date(2024, 12, 31))  # R07
    assert not result.complete
    assert len(result.raw_responses) == 2  # 건수 + 1페이지
    assert "SeibroRequestError" in result.chunks[0].error


def test_failure_in_one_year_stops_window() -> None:
    client, _ = _client(_Session(total=10, fail_page=1))
    result = fetch_window(client, date(2023, 1, 1), date(2024, 12, 31))
    assert not result.complete
    assert len(result.chunks) == 1


def test_duplicate_rows_collapse_by_key() -> None:
    class Dup(_Session):
        def post(self, url, data, headers, timeout):
            xml = data.decode()
            if "Cnt" in xml.split(">")[0]:
                return _Resp(_count_body(2))
            body = _rows_body(2).replace(b"</vector>", _rows_body(2)[8:])
            return _Resp(body)

    client, _ = _client(Dup(total=2))
    result = fetch_window(client, date(2024, 1, 1), date(2024, 12, 31))
    assert result.complete and len(result.rows) == 2


def test_window_covers_gap_after_120_days_off() -> None:
    today = date(2026, 10, 25)
    last_end = date(2026, 6, 27)  # 120일 전
    start, end = weekly_window(today, last_end)
    assert end == today
    assert start == date(2026, 3, 29)  # last_end - 90일
    assert start < last_end  # 사이 구간이 빠지지 않음


def test_window_normal_and_first_run() -> None:
    today = date(2026, 11, 1)
    assert weekly_window(today, date(2026, 10, 25)) == (
        date(2026, 7, 27),
        today,
    )  # last_end − 90일이 더 앞
    assert weekly_window(today, None) == (EARLIEST_RGT_STD_DT, today)
    assert EARLIEST_RGT_STD_DT == date(2003, 1, 1)


def test_quarterly_full_day() -> None:
    assert is_quarterly_full_day(date(2026, 10, 4))  # 10월 첫 일요일
    assert not is_quarterly_full_day(date(2026, 10, 11))
    assert not is_quarterly_full_day(date(2026, 10, 5))  # 월요일
    assert is_quarterly_full_day(date(2027, 1, 3))
    assert is_quarterly_full_day(date(2026, 7, 5))
    assert not is_quarterly_full_day(date(2026, 11, 1))


def test_year_chunks() -> None:
    assert year_chunks(date(2025, 11, 1), date(2026, 2, 1)) == [
        (date(2025, 11, 1), date(2025, 12, 31)),
        (date(2026, 1, 1), date(2026, 2, 1)),
    ]


def test_seibro_enabled() -> None:
    assert seibro_enabled({})
    assert seibro_enabled({"SDC_SEIBRO_ENABLED": "1"})
    assert not seibro_enabled({"SDC_SEIBRO_ENABLED": "0"})


def test_request_interval_uses_injected_sleep() -> None:
    clock = [0.0]
    sleeps: list[float] = []
    client = SeibroClient(
        session=_Session(total=0),
        sleep_fn=sleeps.append,
        monotonic_fn=lambda: clock[0],
        max_attempts=1,
    )
    xml = build_count_request(date(2024, 1, 1), date(2024, 12, 31))
    client.post(xml)
    client.post(xml)
    assert sleeps == [pytest.approx(0.6)]
    assert client.counters.throttle_waits == 1


def test_malformed_response_raises() -> None:
    class Bad:
        def post(self, *a, **k):
            return _Resp(b"<html><body>error</body></html>")

    client = SeibroClient(session=Bad(), sleep_fn=lambda s: None)
    with pytest.raises(SeibroMalformedResponseError):
        client.post(build_count_request(date(2024, 1, 1), date(2024, 12, 31)))
    with pytest.raises(SeibroMalformedResponseError):
        parse_rows(b"not xml")


def test_network_error_retries_then_fails() -> None:
    class Boom:
        n = 0

        def post(self, *a, **k):
            Boom.n += 1
            raise requests.ConnectionError("x")

    client = SeibroClient(session=Boom(), sleep_fn=lambda s: None, max_attempts=3)
    from collector.kr.adapters.seibro_distribution import SeibroRequestError

    with pytest.raises(SeibroRequestError):
        client.post(build_count_request(date(2024, 1, 1), date(2024, 12, 31)))
    assert Boom.n == 3 and client.counters.http_retries == 2
