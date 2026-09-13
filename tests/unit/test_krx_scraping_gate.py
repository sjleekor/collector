"""The opt-in gate on KRX scraping paths (K-5).

KRX restricted this host for automated collection, not for going too fast, so
the replacement paths only help if the old ones stop being one command away.
These tests pin that the pykrx login is closed by default, that the error says
what to run instead, and that the two exceptions — MDC direct and FDR — are
deliberate rather than forgotten.
"""

from __future__ import annotations

import pytest

from collector.kr.adapters import pykrx_auth
from collector.kr.adapters.krx_scraping import (
    ALLOW_KRX_SCRAPING_ENV,
    KrxScrapingDisabledError,
    ensure_krx_scraping_allowed,
)
from collector.kr.infra.config.settings import get_settings


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_scraping_is_closed_unless_a_host_opts_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ALLOW_KRX_SCRAPING_ENV, raising=False)

    with pytest.raises(KrxScrapingDisabledError):
        ensure_krx_scraping_allowed("something", "something else")


def test_the_gate_names_the_replacement_and_the_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An error that only says no gets worked around, usually by setting the
    # flag. Saying what to run instead is the part that changes behaviour.
    monkeypatch.delenv(ALLOW_KRX_SCRAPING_ENV, raising=False)

    with pytest.raises(KrxScrapingDisabledError) as excinfo:
        ensure_krx_scraping_allowed("importing pykrx", "`prices backfill --source naver`")

    message = str(excinfo.value)
    assert "prices backfill --source naver" in message
    assert ALLOW_KRX_SCRAPING_ENV in message
    # And why slowing down is not the fix.
    assert "not about request rate" in message


def test_opting_in_lets_the_path_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ALLOW_KRX_SCRAPING_ENV, "1")

    ensure_krx_scraping_allowed("importing pykrx", "the Open API path")


def test_the_pykrx_import_is_gated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ALLOW_KRX_SCRAPING_ENV, raising=False)
    pykrx_auth.get_pykrx_stock_module.cache_clear()
    pykrx_auth.reset_krx_login_failure_state()

    with pytest.raises(KrxScrapingDisabledError):
        pykrx_auth.get_pykrx_stock_module()


def test_the_session_refresh_is_gated_too(monkeypatch: pytest.MonkeyPatch) -> None:
    # Gating only the import would leave the empty-streak trigger free to
    # re-authenticate on a host that opted out — the same login, another door.
    monkeypatch.delenv(ALLOW_KRX_SCRAPING_ENV, raising=False)

    with pytest.raises(KrxScrapingDisabledError):
        pykrx_auth.refresh_pykrx_session()


def test_the_mdc_and_fdr_doors_are_deliberately_not_gated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Both still have prod work behind them: KIS cannot fill the short-selling
    # balance and prod has no KIS credentials, and prod has no AUTH_KEYS so
    # `universe-sync.sh` still passes `--source fdr`. Gating either would stop
    # collection instead of moving it. When that changes, this test changes.
    monkeypatch.delenv(ALLOW_KRX_SCRAPING_ENV, raising=False)

    from collector.kr.adapters.common_features_krx import provider as krx_common
    from collector.kr.adapters.flows_krx import provider as flows_krx
    from collector.kr.adapters.universe_fdr import provider as universe_fdr

    for module in (flows_krx, krx_common, universe_fdr):
        source = open(module.__file__, encoding="utf-8").read()
        assert "ensure_krx_scraping_allowed" not in source
