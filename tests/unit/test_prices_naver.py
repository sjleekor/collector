"""Daily OHLCV straight from Naver, with no pykrx import (K-5).

The point of the adapter is what it does *not* do: `prices backfill` was
downloading Naver data through pykrx, and importing pykrx logs in to KRX. The
data is unchanged, so most of these tests pin the parsing. The ones that matter
pin what the swap could get wrong, and all three came out of live responses
rather than out of the docs: asking for enough bars to reach a start date,
surviving the blank lines the endpoint emits ahead of its XML declaration, and
telling a well-formed error page apart from a genuinely empty range.
"""

from __future__ import annotations

from datetime import date

from collector.kr.adapters.prices_naver.provider import NaverDailyPriceProvider
from collector.kr.domain.enums import Market, Source

# Verbatim from https://fchart.stock.naver.com/sise.nhn?symbol=005930&count=5,
# fetched 2026-08-18 — including the blank lines ahead of the declaration, which
# ElementTree refuses to parse unless they are stripped first.
LIVE_XML = """

<?xml version="1.0" encoding="EUC-KR" ?>
<protocol>
    <chartdata symbol="005930" name="삼성전자" count="5" timeframe="day">
        <item data="20260811|229500|243000|227500|239500|23310969" />
        <item data="20260812|243500|260500|241000|255500|27102479" />
        <item data="20260813|267500|271000|262500|268000|35530867" />
        <item data="20260814|275000|275500|266000|274500|21669476" />
        <item data="20260818|283000|288000|265000|268500|23966950" />
    </chartdata>
</protocol>
""".encode()

# Verbatim: what an unknown ticker (symbol=000000) actually returns.
EMPTY_XML = b'\n\n<?xml version="1.0" encoding="EUC-KR" ?>\n<protocol />'


class _FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200) -> None:
        self.content = content
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, response: _FakeResponse | Exception) -> None:
        self._response = response
        self.calls: list[dict[str, str]] = []

    def get(self, url, params=None, timeout=None):  # noqa: ANN001
        self.calls.append(dict(params or {}))
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


def _provider(response: _FakeResponse | Exception) -> NaverDailyPriceProvider:
    return NaverDailyPriceProvider(session=_FakeSession(response))


def _fetch(provider: NaverDailyPriceProvider, start: date, end: date):
    return provider.fetch_daily_ohlcv(ticker="005930", market=Market.KOSPI, start=start, end=end)


def test_bars_are_parsed_in_the_pipe_separated_order() -> None:
    result = _fetch(_provider(_FakeResponse(LIVE_XML)), date(2026, 8, 11), date(2026, 8, 18))

    assert result.error is None
    assert len(result.bars) == 5
    first = result.bars[0]
    assert first.trade_date == date(2026, 8, 11)
    assert (first.open, first.high, first.low, first.close) == (229500, 243000, 227500, 239500)
    assert first.volume == 23310969


def test_the_range_is_applied_after_fetching() -> None:
    # The endpoint takes a bar count, not a date range, so it always returns
    # everything up to today and the range has to be enforced here.
    result = _fetch(_provider(_FakeResponse(LIVE_XML)), date(2026, 8, 12), date(2026, 8, 13))

    assert [bar.trade_date for bar in result.bars] == [date(2026, 8, 12), date(2026, 8, 13)]


def test_the_bar_count_reaches_back_to_the_start_date() -> None:
    # `count` counts back from today. Sizing it from the requested *span*
    # instead would silently return nothing for any historical range.
    provider = _provider(_FakeResponse(LIVE_XML))

    _fetch(provider, date(2015, 1, 2), date(2015, 12, 31))

    (params,) = provider._session.calls  # type: ignore[attr-defined]
    assert int(params["count"]) > 4000
    assert params["symbol"] == "005930"
    assert params["timeframe"] == "day"


def test_the_market_comes_from_the_caller() -> None:
    # Naver does not report it, so there is nothing to disagree with.
    result = _fetch(_provider(_FakeResponse(LIVE_XML)), date(2026, 8, 11), date(2026, 8, 18))

    assert result.bars
    assert all(bar.market is Market.KOSPI for bar in result.bars)


def test_rows_are_labelled_naver_not_pykrx() -> None:
    # pykrx named the library, not the source; the rows always came from here.
    result = _fetch(_provider(_FakeResponse(LIVE_XML)), date(2026, 8, 11), date(2026, 8, 18))

    assert all(bar.source is Source.NAVER for bar in result.bars)


def test_an_unknown_ticker_is_not_an_error() -> None:
    # Measured: symbol=000000 answers `<protocol />` — a valid document with no
    # chartdata at all. The delisted-ticker backfill produces this constantly,
    # so it must stay distinguishable from a failure.
    result = _fetch(_provider(_FakeResponse(EMPTY_XML)), date(2026, 8, 11), date(2026, 8, 18))

    assert result.error is None
    assert result.bars == []


def test_a_well_formed_error_page_is_an_error_rather_than_an_empty_range() -> None:
    # Naver answers 200 with HTML when it is unhappy, and HTML parses. Only the
    # root tag separates it from a real but empty answer. Reading it as "no
    # rows" would write a gap that gap-detection later reads as absent data.
    result = _fetch(
        _provider(_FakeResponse(b"<html><body>error</body></html>")),
        date(2026, 8, 11),
        date(2026, 8, 18),
    )

    assert result.bars == []
    assert result.error is not None
    assert "unexpected response root <html>" in result.error


def test_a_truncated_response_is_an_error() -> None:
    result = _fetch(
        _provider(_FakeResponse(b'\n<?xml version="1.0" ?>\n<protocol><chartdata>')),
        date(2026, 8, 11),
        date(2026, 8, 18),
    )

    assert result.error is not None
    assert "unparseable response" in result.error


def test_a_transport_failure_is_reported_not_raised() -> None:
    # The service wraps this call in its own retry, which keys on `error`.
    provider = _provider(RuntimeError("connection reset"))

    result = _fetch(provider, date(2026, 8, 11), date(2026, 8, 18))

    assert result.error == "connection reset"
    assert result.bars == []


def test_one_malformed_bar_does_not_cost_the_other_bars() -> None:
    broken = LIVE_XML.replace(
        b'<item data="20260812|243500|260500|241000|255500|27102479" />',
        b'<item data="20260812|nope" />',
    )
    result = _fetch(_provider(_FakeResponse(broken)), date(2026, 8, 11), date(2026, 8, 18))

    assert result.error is None
    assert [bar.trade_date for bar in result.bars] == [
        date(2026, 8, 11),
        date(2026, 8, 13),
        date(2026, 8, 14),
        date(2026, 8, 18),
    ]


def test_an_inverted_range_is_refused_without_a_request() -> None:
    provider = _provider(_FakeResponse(LIVE_XML))

    result = _fetch(provider, date(2026, 8, 18), date(2026, 8, 11))

    assert result.error is not None
    assert provider._session.calls == []  # type: ignore[attr-defined]


def test_http_requests_are_counted_for_real() -> None:
    # `requests_attempted` elsewhere counts logical work, which is why the KRX
    # block could not be explained from our own audit rows (K-5).
    provider = _provider(_FakeResponse(LIVE_XML))
    assert provider.http_requests == 0

    _fetch(provider, date(2026, 8, 11), date(2026, 8, 18))

    assert provider.http_requests == 1


def test_the_adapter_module_does_not_import_pykrx() -> None:
    # The whole point. Importing pykrx runs `build_krx_session()` at module
    # scope, so a stray import here puts a KRX login back on this path.
    import collector.kr.adapters.prices_naver.provider as naver_provider

    source = open(naver_provider.__file__, encoding="utf-8").read()
    code = "\n".join(line for line in source.splitlines() if line.startswith(("import ", "from ")))
    assert "pykrx" not in code
