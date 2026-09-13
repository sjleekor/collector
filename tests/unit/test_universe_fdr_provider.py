from __future__ import annotations

from datetime import date

import pandas as pd

from collector.kr.adapters.universe_fdr.provider import FdrUniverseProvider
from collector.kr.domain.enums import Market, Source


def test_fdr_provider_maps_code_column(monkeypatch) -> None:
    provider = FdrUniverseProvider()

    def fake_stock_listing(market: str) -> pd.DataFrame:
        assert market == "KOSPI"
        return pd.DataFrame(
            [
                {"Code": "005930", "Name": "Samsung Electronics"},
                {"Code": "000660", "Name": "SK hynix"},
            ]
        )

    monkeypatch.setattr(
        "collector.kr.adapters.universe_fdr.provider.fdr.StockListing",
        fake_stock_listing,
    )

    result = provider.fetch_universe([Market.KOSPI], as_of=date(2026, 5, 21))

    assert result.error is None
    assert result.snapshot is not None
    assert result.snapshot.source == Source.FDR
    assert [stock.ticker for stock in result.snapshot.records] == ["005930", "000660"]


def test_fdr_provider_maps_symbol_column(monkeypatch) -> None:
    provider = FdrUniverseProvider()

    monkeypatch.setattr(
        "collector.kr.adapters.universe_fdr.provider.fdr.StockListing",
        lambda _market: pd.DataFrame([{"Symbol": "035420", "Name": "NAVER"}]),
    )

    result = provider.fetch_universe([Market.KOSPI], as_of=date(2026, 5, 21))

    assert result.error is None
    assert result.snapshot is not None
    assert [stock.ticker for stock in result.snapshot.records] == ["035420"]


def test_fdr_provider_parses_listing_date(monkeypatch) -> None:
    provider = FdrUniverseProvider()

    monkeypatch.setattr(
        "collector.kr.adapters.universe_fdr.provider.fdr.StockListing",
        lambda _market: pd.DataFrame(
            [{"Code": "005930", "Name": "Samsung", "ListingDate": "2020-01-02"}]
        ),
    )

    result = provider.fetch_universe([Market.KOSPI], as_of=date(2026, 5, 21))

    assert result.error is None
    assert result.snapshot is not None
    assert result.snapshot.records[0].listing_date == date(2020, 1, 2)


def test_fdr_provider_tolerates_missing_or_nat_listing_date(monkeypatch) -> None:
    provider = FdrUniverseProvider()

    monkeypatch.setattr(
        "collector.kr.adapters.universe_fdr.provider.fdr.StockListing",
        lambda _market: pd.DataFrame(
            [
                {"Code": "005930", "Name": "Samsung", "ListingDate": pd.NaT},
                {"Code": "000660", "Name": "SK hynix", "ListingDate": ""},
                {"Code": "035420", "Name": "NAVER"},  # missing column entirely
            ]
        ),
    )

    result = provider.fetch_universe([Market.KOSPI], as_of=date(2026, 5, 21))

    assert result.error is None
    assert result.snapshot is not None
    assert [s.listing_date for s in result.snapshot.records] == [None, None, None]


def test_fdr_provider_honors_alternate_listing_date_column(monkeypatch) -> None:
    provider = FdrUniverseProvider()

    monkeypatch.setattr(
        "collector.kr.adapters.universe_fdr.provider.fdr.StockListing",
        lambda _market: pd.DataFrame(
            [{"Symbol": "035420", "Name": "NAVER", "ListedDate": "2002-10-29"}]
        ),
    )

    result = provider.fetch_universe([Market.KOSPI], as_of=date(2026, 5, 21))

    assert result.error is None
    assert result.snapshot is not None
    assert result.snapshot.records[0].listing_date == date(2002, 10, 29)


def test_fdr_failure_does_not_silently_switch_to_a_login_based_source(monkeypatch) -> None:
    # The automatic pykrx fallback is gone (K-5). It fired precisely when it
    # should not have: FDR wobbles when KRX wobbles, so a failed anonymous read
    # became a KRX login nobody asked for, during the window KRX was least
    # willing to be scraped.
    provider = FdrUniverseProvider()

    monkeypatch.setattr(
        "collector.kr.adapters.universe_fdr.provider.fdr.StockListing",
        lambda _market: (_ for _ in ()).throw(ValueError("Failed to load data from KRX")),
    )

    result = provider.fetch_universe([Market.KOSPI], as_of=date(2026, 5, 21))

    assert result.snapshot is None
    assert result.error == "FDR failed: Failed to load data from KRX"


def test_the_fdr_adapter_no_longer_imports_the_pykrx_provider() -> None:
    # Importing it is enough to matter: pykrx logs in to KRX at import time,
    # so a module-level import here would put a login on a code path that
    # never uses pykrx.
    import collector.kr.adapters.universe_fdr.provider as fdr_provider

    assert not hasattr(fdr_provider, "PykrxUniverseProvider")
