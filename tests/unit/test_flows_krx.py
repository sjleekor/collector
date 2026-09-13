from __future__ import annotations

import json
from collections import deque
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from collector.kr.adapters.flows_krx.client import (
    KRX_LOGIN_URL,
    KrxMdcAuthenticationError,
    KrxMdcClient,
    KrxMdcResponseError,
    KrxMdcRow,
)
from collector.kr.adapters.flows_krx.codes import KrxStockCodeResolver
from collector.kr.adapters.flows_krx.parsers import (
    FOREIGN_HOLDING_BLD,
    INVESTOR_BLD,
    INVESTOR_BULK_BLD,
    SHORTING_BALANCE_BLD,
    SHORTING_BALANCE_BULK_BLD,
    SHORTING_STATUS_BLD,
    SHORTING_TRADING_BULK_BLD,
    parse_foreign_holding_rows,
    parse_investor_net_volume_bulk_rows,
    parse_investor_net_volume_rows,
    parse_shorting_balance_bulk_rows,
    parse_shorting_rows,
    parse_shorting_trading_bulk_rows,
)
from collector.kr.adapters.flows_krx.provider import KrxDirectFlowProvider
from collector.kr.domain.enums import Market, Source
from collector.kr.util.pipeline import HumanThrottle, HumanThrottlePolicy

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "flows_krx"


def _fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _rows(name: str, output_key: str, request: dict[str, Any], bld: str) -> list[KrxMdcRow]:
    del bld
    payload = _fixture(name)
    return [KrxMdcRow(row=row, request=dict(request)) for row in payload[output_key]]


def _core(record) -> tuple[str, str, Decimal | None, str, date, str, Market, Source]:
    return (
        record.metric_code,
        record.metric_name,
        record.value,
        record.unit,
        record.trade_date,
        record.ticker,
        record.market,
        record.source,
    )


def test_parse_investor_net_volume_rows_from_krx_fixture() -> None:
    request = {
        "strtDd": "20260417",
        "endDd": "20260417",
        "isuCd": "KR7005930003",
        "inqTpCd": "2",
        "trdVolVal": "1",
        "askBid": "3",
    }
    rows = _rows("investor_ticker_005930_20260417.json", "output", request, INVESTOR_BLD)

    records = parse_investor_net_volume_rows(rows, "005930", Market.KOSPI)

    assert [_core(record) for record in records] == [
        (
            "institution_net_buy_volume",
            "기관 순매수 수량",
            Decimal("-1000"),
            "shares",
            date(2026, 4, 17),
            "005930",
            Market.KOSPI,
            Source.KRX,
        ),
        (
            "individual_net_buy_volume",
            "개인 순매수 수량",
            Decimal("250"),
            "shares",
            date(2026, 4, 17),
            "005930",
            Market.KOSPI,
            Source.KRX,
        ),
        (
            "foreign_net_buy_volume",
            "외국인 순매수 수량",
            Decimal("750"),
            "shares",
            date(2026, 4, 17),
            "005930",
            Market.KOSPI,
            Source.KRX,
        ),
    ]
    assert records[0].raw_payload == {
        "source_bld": INVESTOR_BLD,
        "request": request,
        "row": rows[0].row,
    }


def test_parse_foreign_holding_rows_filters_requested_tickers() -> None:
    request = {"searchType": "1", "mktId": "STK", "trdDd": "20260417", "isuLmtRto": "0"}
    rows = _rows("foreign_holding_all_kospi_20260417.json", "output", request, FOREIGN_HOLDING_BLD)

    records = parse_foreign_holding_rows(rows, Market.KOSPI, date(2026, 4, 17), ["005930"])

    assert len(records) == 1
    assert _core(records[0]) == (
        "foreign_holding_shares",
        "외국인 보유주식수",
        Decimal("3123456789"),
        "shares",
        date(2026, 4, 17),
        "005930",
        Market.KOSPI,
        Source.KRX,
    )
    assert records[0].raw_payload["source_bld"] == FOREIGN_HOLDING_BLD


def test_parse_investor_net_volume_bulk_rows_sums_foreign_and_fills_missing_zero() -> None:
    individual_request = {
        "strtDd": "20260417",
        "endDd": "20260417",
        "mktId": "STK",
        "invstTpCd": "8000",
    }
    institution_request = {
        "strtDd": "20260417",
        "endDd": "20260417",
        "mktId": "STK",
        "invstTpCd": "7050",
    }
    foreign_request = {
        "strtDd": "20260417",
        "endDd": "20260417",
        "mktId": "STK",
        "invstTpCd": "9000",
    }
    other_foreign_request = {
        "strtDd": "20260417",
        "endDd": "20260417",
        "mktId": "STK",
        "invstTpCd": "9001",
    }

    records = parse_investor_net_volume_bulk_rows(
        individual_rows=_rows(
            "investor_bulk_individual_kospi_20260417.json",
            "output",
            individual_request,
            INVESTOR_BULK_BLD,
        ),
        institution_rows=_rows(
            "investor_bulk_institution_kospi_20260417.json",
            "output",
            institution_request,
            INVESTOR_BULK_BLD,
        ),
        foreign_rows=_rows(
            "investor_bulk_foreign_kospi_20260417.json",
            "output",
            foreign_request,
            INVESTOR_BULK_BLD,
        ),
        other_foreign_rows=_rows(
            "investor_bulk_other_foreign_kospi_20260417.json",
            "output",
            other_foreign_request,
            INVESTOR_BULK_BLD,
        ),
        market=Market.KOSPI,
        trade_date=date(2026, 4, 17),
    )
    facts = {(record.ticker, record.metric_code): record for record in records}

    assert facts[("005930", "institution_net_buy_volume")].value == Decimal("-1000")
    assert facts[("005930", "individual_net_buy_volume")].value == Decimal("250")
    assert facts[("005930", "foreign_net_buy_volume")].value == Decimal("750")
    assert facts[("000660", "institution_net_buy_volume")].value == Decimal("0")
    assert facts[("000660", "foreign_net_buy_volume")].value == Decimal("5")
    assert (
        "institution"
        in facts[("000660", "institution_net_buy_volume")].raw_payload["row"][
            "filled_missing_components_as_zero"
        ]
    )
    assert facts[("005930", "foreign_net_buy_volume")].raw_payload["source_bld"] == (
        INVESTOR_BULK_BLD
    )


def test_parse_shorting_rows_prefers_balance_endpoint_and_falls_back_to_status() -> None:
    request = {"strtDd": "20260417", "endDd": "20260417", "isuCd": "KR7005930003"}
    status_rows = _rows(
        "shorting_status_005930_20260417.json",
        "OutBlock_1",
        request,
        SHORTING_STATUS_BLD,
    )
    balance_rows = _rows(
        "shorting_balance_005930_20260417.json",
        "OutBlock_1",
        request,
        SHORTING_BALANCE_BLD,
    )

    records = parse_shorting_rows(status_rows, balance_rows, "005930", Market.KOSPI)
    facts = {record.metric_code: record for record in records}

    assert facts["short_selling_volume"].value == Decimal("1234")
    assert facts["short_selling_value"].value == Decimal("5678000")
    assert facts["short_selling_balance_quantity"].value == Decimal("1111")
    assert facts["short_selling_balance_quantity"].raw_payload["source_bld"] == SHORTING_BALANCE_BLD

    fallback_records = parse_shorting_rows(status_rows, [], "005930", Market.KOSPI)
    fallback_facts = {record.metric_code: record for record in fallback_records}
    assert fallback_facts["short_selling_balance_quantity"].value == Decimal("999")
    assert (
        fallback_facts["short_selling_balance_quantity"].raw_payload["source_bld"]
        == SHORTING_STATUS_BLD
    )


def test_parse_shorting_trading_bulk_rows_filters_requested_tickers() -> None:
    request = {"trdDd": "20260417", "mktId": "STK", "inqCond": "STMFRTSCIFDRFS"}
    rows = _rows(
        "shorting_trading_all_kospi_20260417.json",
        "OutBlock_1",
        request,
        SHORTING_TRADING_BULK_BLD,
    )

    records = parse_shorting_trading_bulk_rows(
        rows,
        Market.KOSPI,
        date(2026, 4, 17),
        ["005930"],
    )
    facts = {record.metric_code: record for record in records}

    assert set(facts) == {"short_selling_volume", "short_selling_value"}
    assert facts["short_selling_volume"].value == Decimal("1234")
    assert facts["short_selling_value"].value == Decimal("5678000")
    assert facts["short_selling_volume"].ticker == "005930"
    assert facts["short_selling_volume"].raw_payload["source_bld"] == SHORTING_TRADING_BULK_BLD


def test_parse_shorting_balance_bulk_rows_filters_requested_tickers() -> None:
    request = {"trdDd": "20260417", "mktTpCd": "1"}
    rows = _rows(
        "shorting_balance_all_kospi_20260417.json",
        "OutBlock_1",
        request,
        SHORTING_BALANCE_BULK_BLD,
    )

    records = parse_shorting_balance_bulk_rows(
        rows,
        Market.KOSPI,
        date(2026, 4, 17),
        ["005930"],
    )

    assert len(records) == 1
    assert _core(records[0]) == (
        "short_selling_balance_quantity",
        "공매도 잔고 수량",
        Decimal("1111"),
        "shares",
        date(2026, 4, 17),
        "005930",
        Market.KOSPI,
        Source.KRX,
    )
    assert records[0].raw_payload["source_bld"] == SHORTING_BALANCE_BULK_BLD


class FakeResponse:
    def __init__(
        self,
        data: dict[str, Any] | None = None,
        *,
        text: str | None = None,
        status_code: int = 200,
    ) -> None:
        self._data = data
        self.text = text if text is not None else json.dumps(data or {})
        self.status_code = status_code

    def json(self) -> dict[str, Any]:
        if self._data is None:
            raise ValueError("not json")
        return self._data


class FakeSession:
    def __init__(self, post_responses: list[FakeResponse]) -> None:
        self._post_responses = deque(post_responses)
        self.post_calls: list[dict[str, Any]] = []
        self.get_calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs) -> FakeResponse:
        self.get_calls.append({"url": url, **kwargs})
        return FakeResponse({})

    def post(self, url: str, **kwargs) -> FakeResponse:
        self.post_calls.append({"url": url, **kwargs})
        return self._post_responses.popleft()


@pytest.mark.parametrize("status_code", [200, 400])
def test_client_raises_auth_error_on_logout_response(status_code: int) -> None:
    session = FakeSession([FakeResponse(text="LOGOUT", status_code=status_code)])
    client = KrxMdcClient(session=session, warmup=False, auto_login=False)

    with pytest.raises(KrxMdcAuthenticationError, match="LOGOUT"):
        client.post_rows(
            "dbms/MDC/STAT/standard/MDCSTAT03701",
            {"trdDd": "20260417"},
            output_key="output",
        )


def test_client_logs_in_and_retries_after_logout_when_credentials_exist() -> None:
    session = FakeSession(
        [
            FakeResponse(text="LOGOUT"),
            FakeResponse({"_error_code": "CD001", "_error_message": "정상"}),
            FakeResponse({"output": [{"TRD_DD": "2026/04/17"}]}),
        ]
    )
    client = KrxMdcClient(
        session=session,
        warmup=False,
        login_id="user",
        login_pw="secret",
    )

    rows = client.post_rows(
        "dbms/MDC/STAT/standard/MDCSTAT03701",
        {"trdDd": "20260417"},
        output_key="output",
    )

    assert [row.row for row in rows] == [{"TRD_DD": "2026/04/17"}]
    assert len(session.get_calls) == 2
    assert session.post_calls[1]["url"] == KRX_LOGIN_URL
    assert session.post_calls[2]["data"]["trdDd"] == "20260417"


def test_client_handles_duplicate_login_response_with_skip_dup() -> None:
    session = FakeSession(
        [
            FakeResponse(text="LOGOUT"),
            FakeResponse({"_error_code": "CD011", "_error_message": "duplicate"}),
            FakeResponse({"_error_code": "CD001", "_error_message": "정상"}),
            FakeResponse({"output": [{"TRD_DD": "2026/04/17"}]}),
        ]
    )
    client = KrxMdcClient(
        session=session,
        warmup=False,
        login_id="user",
        login_pw="secret",
    )

    rows = client.post_rows(
        "dbms/MDC/STAT/standard/MDCSTAT03701",
        {"trdDd": "20260417"},
        output_key="output",
    )

    assert [row.row for row in rows] == [{"TRD_DD": "2026/04/17"}]
    assert session.post_calls[2]["url"] == KRX_LOGIN_URL
    assert session.post_calls[2]["data"]["skipDup"] == "Y"


def test_client_splits_730_day_range_requests() -> None:
    session = FakeSession(
        [
            FakeResponse({"output": [{"chunk": 1}]}),
            FakeResponse({"output": [{"chunk": 2}]}),
        ]
    )
    client = KrxMdcClient(session=session, warmup=False, auto_login=False)

    rows = client.post_rows(
        INVESTOR_BLD,
        {
            "strtDd": "20200101",
            "endDd": "20220102",
            "isuCd": "KR7005930003",
        },
        output_key="output",
    )

    assert [row.row["chunk"] for row in rows] == [1, 2]
    assert session.post_calls[0]["data"]["strtDd"] == "20200101"
    assert session.post_calls[0]["data"]["endDd"] == "20211231"
    assert session.post_calls[1]["data"]["strtDd"] == "20220101"
    assert session.post_calls[1]["data"]["endDd"] == "20220102"


def test_client_uses_configured_timeout_for_posts() -> None:
    session = FakeSession([FakeResponse({"output": []})])
    client = KrxMdcClient(
        session=session,
        timeout_seconds=150.0,
        warmup=False,
        auto_login=False,
    )

    client.post_rows(
        "dbms/MDC/STAT/standard/MDCSTAT03701",
        {"trdDd": "20260417"},
        output_key="output",
    )

    assert session.post_calls[0]["timeout"] == 150.0


def test_client_applies_human_throttle_spacing_and_auth_cooldown() -> None:
    session = FakeSession(
        [
            FakeResponse(text="LOGOUT"),
            FakeResponse({"_error_code": "CD001", "_error_message": "정상"}),
            FakeResponse({"output": []}),
        ]
    )
    sleeps: list[float] = []
    throttle = HumanThrottle(
        HumanThrottlePolicy(
            min_delay_seconds=1.0,
            max_delay_seconds=1.0,
            auth_cooldown_seconds=2.0,
        ),
        sleep_fn=sleeps.append,
    )
    client = KrxMdcClient(
        session=session,
        warmup=False,
        login_id="user",
        login_pw="secret",
        human_throttle=throttle,
    )

    client.post_rows(
        "dbms/MDC/STAT/standard/MDCSTAT03701",
        {"trdDd": "20260417"},
        output_key="output",
    )

    assert 2.0 in sleeps
    assert sum(abs(duration - 1.0) < 0.01 for duration in sleeps) >= 4


class FakeFinderClient:
    def __init__(self) -> None:
        self.calls = 0

    def post_rows(self, bld: str, params: dict[str, Any], *, output_key: str) -> list[KrxMdcRow]:
        self.calls += 1
        assert bld == "dbms/comm/finder/finder_stkisu"
        assert output_key == "block1"
        del params
        return [KrxMdcRow(row=row, request={}) for row in _fixture("finder_stkisu.json")["block1"]]


def test_resolver_loads_finder_once_and_filters_by_market() -> None:
    client = FakeFinderClient()
    resolver = KrxStockCodeResolver(client)  # type: ignore[arg-type]

    first = resolver.resolve("5930", Market.KOSPI)
    second = resolver.resolve_isin("005930", Market.KOSPI)

    assert first.isin == "KR7005930003"
    assert second == "KR7005930003"
    assert client.calls == 1


class FakeProviderClient:
    def post_rows(self, bld: str, params: dict[str, Any], *, output_key: str) -> list[KrxMdcRow]:
        if bld == INVESTOR_BLD:
            assert output_key == "output"
            return _rows("investor_ticker_005930_20260417.json", output_key, params, bld)
        if bld == INVESTOR_BULK_BLD:
            assert output_key == "output"
            assert params["strtDd"] == "20260417"
            assert params["endDd"] == "20260417"
            assert params["mktId"] == "STK"
            fixture_by_investor = {
                "7050": "investor_bulk_institution_kospi_20260417.json",
                "8000": "investor_bulk_individual_kospi_20260417.json",
                "9000": "investor_bulk_foreign_kospi_20260417.json",
                "9001": "investor_bulk_other_foreign_kospi_20260417.json",
            }
            return _rows(fixture_by_investor[params["invstTpCd"]], output_key, params, bld)
        if bld == SHORTING_STATUS_BLD:
            assert output_key == "OutBlock_1"
            return _rows("shorting_status_005930_20260417.json", output_key, params, bld)
        if bld == SHORTING_BALANCE_BLD:
            assert output_key == "OutBlock_1"
            return _rows("shorting_balance_005930_20260417.json", output_key, params, bld)
        if bld == SHORTING_TRADING_BULK_BLD:
            assert output_key == "OutBlock_1"
            assert params == {
                "trdDd": "20260417",
                "mktId": "STK",
                "inqCond": "STMFRTSCIFDRFS",
            }
            return _rows("shorting_trading_all_kospi_20260417.json", output_key, params, bld)
        if bld == SHORTING_BALANCE_BULK_BLD:
            assert output_key == "OutBlock_1"
            assert params == {"trdDd": "20260417", "mktTpCd": "1"}
            return _rows("shorting_balance_all_kospi_20260417.json", output_key, params, bld)
        if bld == FOREIGN_HOLDING_BLD:
            assert output_key == "output"
            return _rows("foreign_holding_all_kospi_20260417.json", output_key, params, bld)
        raise AssertionError(f"Unexpected bld: {bld}")


class FakeResolver:
    def resolve_isin(self, ticker: str, market: Market | None = None) -> str:
        assert ticker == "005930"
        assert market in {Market.KOSPI, None}
        return "KR7005930003"


def test_direct_provider_fetches_krx_records_with_krx_source() -> None:
    provider = KrxDirectFlowProvider(
        client=FakeProviderClient(),  # type: ignore[arg-type]
        resolver=FakeResolver(),  # type: ignore[arg-type]
    )

    investor = provider.fetch_investor_net_volume(
        "005930", Market.KOSPI, date(2026, 4, 17), date(2026, 4, 17)
    )
    investor_bulk = provider.fetch_investor_net_volume_bulk(
        date(2026, 4, 17), Market.KOSPI, ["005930"]
    )
    shorting = provider.fetch_shorting_metrics(
        "005930", Market.KOSPI, date(2026, 4, 17), date(2026, 4, 17)
    )
    shorting_trading = provider.fetch_shorting_trading_bulk(
        date(2026, 4, 17), Market.KOSPI, ["005930"]
    )
    shorting_balance = provider.fetch_shorting_balance_bulk(
        date(2026, 4, 17), Market.KOSPI, ["005930"]
    )
    foreign = provider.fetch_foreign_holding_shares(date(2026, 4, 17), Market.KOSPI, ["005930"])

    assert provider.source() == Source.KRX
    assert investor.error is None
    assert investor_bulk.error is None
    assert shorting.error is None
    assert shorting_trading.error is None
    assert shorting_balance.error is None
    assert foreign.error is None
    assert len(investor.records) == 3
    assert len(investor_bulk.records) == 3
    assert len(shorting.records) == 3
    assert len(shorting_trading.records) == 2
    assert len(shorting_balance.records) == 1
    assert len(foreign.records) == 1
    assert {
        record.source
        for record in (
            investor.records
            + investor_bulk.records
            + shorting.records
            + shorting_trading.records
            + shorting_balance.records
            + foreign.records
        )
    } == {Source.KRX}


def test_client_maps_authish_error_message_to_authentication_error() -> None:
    session = FakeSession(
        [
            FakeResponse({"_error_code": "CD9999", "_error_message": "로그인이 필요합니다"}),
        ]
    )
    client = KrxMdcClient(session=session, warmup=False, auto_login=False)

    with pytest.raises(KrxMdcAuthenticationError, match="auth error"):
        client.post_rows(
            "dbms/MDC/STAT/standard/MDCSTAT03701",
            {"trdDd": "20260417"},
            output_key="output",
        )


def test_direct_provider_rejects_unsupported_market_with_response_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _UnusedClient:
        def post_rows(self, *_args, **_kwargs):
            raise AssertionError("client should not be called for unsupported market")

    monkeypatch.setattr("collector.kr.adapters.flows_krx.provider.MARKET_TO_KRX_ID", {})

    provider = KrxDirectFlowProvider(
        client=_UnusedClient(),  # type: ignore[arg-type]
        resolver=FakeResolver(),  # type: ignore[arg-type]
    )

    result = provider.fetch_foreign_holding_shares(date(2026, 4, 17), Market.KOSPI, ["005930"])

    assert result.error is not None
    assert "Unsupported market" in result.error
    assert result.records == []
    assert KrxMdcResponseError.__name__ == "KrxMdcResponseError"
