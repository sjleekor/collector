"""Re-login when KRX drops the session out from under pykrx.

pykrx logs in once at import and re-logs in only when its own clock says the
session expired. KRX drops it sooner than that. Measured on the N3 snapshot
backfill (2026-08-15/16): a process worked for two to four minutes and then
every response came back empty, while pykrx kept sending dead cookies because
its clock still said the session was good. Two runs died that way, and the
slower-paced run got *fewer* rows before dying — so pacing was not the thing
being punished, process lifetime was. A fresh process succeeded every time.

The trigger cannot be a single empty response: ``dataframe_empty_handler``
swallows ``JSONDecodeError`` and returns an empty frame, so a dead session and a
delisted ticker with no rows are indistinguishable one call at a time. They are
distinguishable in aggregate — a dead session empties *everything*.
"""

from __future__ import annotations

import pytest

from collector.kr.adapters import pykrx_auth
from collector.kr.infra.config.settings import get_settings


@pytest.fixture(autouse=True)
def _fresh_state(monkeypatch: pytest.MonkeyPatch):
    # Every test here exercises the login machinery, which K-5 put behind an
    # opt-in gate. Opting in is what these tests are about; the gate itself is
    # covered in `test_krx_scraping_gate.py`.
    monkeypatch.setenv("ALLOW_KRX_SCRAPING", "1")
    get_settings.cache_clear()
    pykrx_auth.reset_session_retry_state()
    yield
    pykrx_auth.reset_session_retry_state()
    get_settings.cache_clear()


def _fetcher(results: list[object]):
    """Return (callable, calls) yielding `results` in order, repeating the last."""
    calls: list[int] = []

    def fetch() -> object:
        index = min(len(calls), len(results) - 1)
        calls.append(index)
        return results[index]

    return fetch, calls


def _call(fetch, label: str = "probe"):
    return pykrx_auth.call_with_session_retry(fetch, is_empty=lambda value: not value, label=label)


def test_a_good_response_never_touches_the_session(monkeypatch) -> None:
    refreshes = []
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: refreshes.append(1) or True)

    fetch, calls = _fetcher(["data"])

    assert _call(fetch) == "data"
    assert len(calls) == 1
    assert refreshes == []


def test_a_single_empty_response_is_not_enough_to_re_login(monkeypatch) -> None:
    # One empty is ordinary: a ticker with no rows in the requested range.
    refreshes = []
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: refreshes.append(1) or True)

    assert _call(_fetcher([[]])[0]) == []
    assert _call(_fetcher([[]])[0]) == []

    assert refreshes == []


def test_a_streak_of_empties_re_logs_in_and_retries(monkeypatch) -> None:
    refreshes = []
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: refreshes.append(1) or True)

    _call(_fetcher([[]])[0])
    _call(_fetcher([[]])[0])

    # Third consecutive empty: refresh, then the retry succeeds.
    fetch, calls = _fetcher([[], "data"])
    assert _call(fetch) == "data"

    assert refreshes == [1]
    assert len(calls) == 2


def test_a_success_resets_the_streak(monkeypatch) -> None:
    refreshes = []
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: refreshes.append(1) or True)

    _call(_fetcher([[]])[0])
    _call(_fetcher([[]])[0])
    _call(_fetcher(["data"])[0])
    _call(_fetcher([[]])[0])
    _call(_fetcher([[]])[0])

    assert refreshes == []


def test_a_refresh_that_does_not_help_makes_the_next_one_cost_more(monkeypatch) -> None:
    # The delisted-price backfill is mostly legitimate empties. Paying one login
    # per three tickers there would be worse than the problem being solved, so
    # a refresh that fails to produce data raises the bar fourfold.
    refreshes = []
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: refreshes.append(1) or True)

    for _ in range(3):
        _call(_fetcher([[]])[0])
    assert refreshes == [1]

    # Trigger is now 12, so the next 11 empties must not refresh.
    for _ in range(11):
        _call(_fetcher([[]])[0])
    assert refreshes == [1]

    _call(_fetcher([[]])[0])
    assert refreshes == [1, 1]


def test_a_refresh_that_helps_restores_the_original_trigger(monkeypatch) -> None:
    refreshes = []
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: refreshes.append(1) or True)

    # Raise the bar once with an unhelpful refresh.
    for _ in range(3):
        _call(_fetcher([[]])[0])
    assert refreshes == [1]

    # Now a refresh that does produce data.
    for _ in range(11):
        _call(_fetcher([[]])[0])
    assert _call(_fetcher([[], "data"])[0]) == "data"
    assert refreshes == [1, 1]

    # Back to a trigger of 3.
    for _ in range(3):
        _call(_fetcher([[]])[0])
    assert refreshes == [1, 1, 1]


def test_a_failed_refresh_returns_the_empty_result_rather_than_raising(monkeypatch) -> None:
    # Losing the credentials mid-run must not turn into a crash; the caller's
    # circuit breaker is what decides to stop.
    monkeypatch.setattr(pykrx_auth, "refresh_pykrx_session", lambda: False)

    for _ in range(2):
        _call(_fetcher([[]])[0])

    fetch, calls = _fetcher([[], "data"])
    assert _call(fetch) == []
    assert len(calls) == 1  # no retry after a failed refresh


def test_refresh_without_credentials_reports_failure(monkeypatch) -> None:
    monkeypatch.delenv("KRX_ID", raising=False)
    monkeypatch.delenv("KRX_PW", raising=False)
    monkeypatch.setattr(pykrx_auth, "configure_krx_credentials_from_settings", lambda: None)

    assert pykrx_auth.refresh_pykrx_session() is False


def test_a_failed_krx_login_is_reported_as_such_not_as_a_json_error(monkeypatch) -> None:
    """The import-time login failure has to name itself.

    `pykrx.website.comm.webio` logs into KRX at import time, so when KRX answers
    the login endpoint with an HTML page the whole library becomes unimportable.
    Raw, that surfaced as forty lines of traceback ending in "Expecting value:
    line 13 column 1", repeated once per target date, with nothing anywhere
    naming the cause. It took reading pykrx's source to work out that the
    collectors were fine and the login was down.
    """
    import builtins
    import json

    get_pykrx_stock_module = pykrx_auth.get_pykrx_stock_module
    get_pykrx_stock_module.cache_clear()
    monkeypatch.setattr(pykrx_auth, "configure_krx_credentials_from_settings", lambda: None)

    real_import = builtins.__import__

    def _import(name, *args, **kwargs):
        if name == "pykrx":
            raise json.JSONDecodeError("Expecting value", "<html>", 25)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _import)

    with pytest.raises(pykrx_auth.KrxLoginUnavailableError) as excinfo:
        get_pykrx_stock_module()

    message = str(excinfo.value)
    assert "KRX login returned a non-JSON response" in message
    assert "not a credential problem" in message
    assert isinstance(excinfo.value.__cause__, json.JSONDecodeError)

    get_pykrx_stock_module.cache_clear()


# ---------------------------------------------------------------------------
# Login-failure cooldown
#
# KRX restricted the sj2 IP on 2026-08-16 for "자동화 수단을 통한 비정상 대량
# 조회". The volume that earned it was not the collection: it was the retry
# path. `lru_cache` does not cache exceptions and a failed `from pykrx import
# stock` leaves nothing in sys.modules, so every caller re-ran the import, and
# every import re-ran webio's import-time login -- a warmup GET, a login-page
# GET and a login POST. One 20-minute run turned 32 failed dates into 32 login
# flows, ~96 requests to the login endpoint, and collected nothing.
# ---------------------------------------------------------------------------


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def _login_state(monkeypatch):
    pykrx_auth.reset_krx_login_failure_state()
    pykrx_auth.get_pykrx_stock_module.cache_clear()
    monkeypatch.setattr(pykrx_auth, "configure_krx_credentials_from_settings", lambda: None)
    clock = _Clock()
    monkeypatch.setattr(pykrx_auth.time, "monotonic", clock)
    yield clock
    pykrx_auth.reset_krx_login_failure_state()
    pykrx_auth.get_pykrx_stock_module.cache_clear()


def _break_the_import(monkeypatch) -> list[int]:
    """Make `import pykrx` fail, counting how often the import is attempted."""
    import builtins
    import json

    attempts: list[int] = []
    real_import = builtins.__import__

    def _import(name, *args, **kwargs):
        if name == "pykrx":
            attempts.append(1)
            raise json.JSONDecodeError("Expecting value", "<html>", 25)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _import)
    return attempts


def test_a_failed_login_is_not_retried_on_every_call(_login_state, monkeypatch) -> None:
    attempts = _break_the_import(monkeypatch)

    for _ in range(30):
        with pytest.raises(pykrx_auth.KrxLoginUnavailableError):
            pykrx_auth.get_pykrx_stock_module()

    # Without the cooldown this is 30 imports and ~90 requests to KRX's login.
    assert len(attempts) == 1


def test_the_cooldown_expires_so_a_transient_failure_still_recovers(
    _login_state, monkeypatch
) -> None:
    clock = _login_state
    attempts = _break_the_import(monkeypatch)

    with pytest.raises(pykrx_auth.KrxLoginUnavailableError):
        pykrx_auth.get_pykrx_stock_module()
    assert len(attempts) == 1

    clock.advance(59)
    with pytest.raises(pykrx_auth.KrxLoginUnavailableError):
        pykrx_auth.get_pykrx_stock_module()
    assert len(attempts) == 1  # still inside the first 60s window

    clock.advance(2)
    with pytest.raises(pykrx_auth.KrxLoginUnavailableError):
        pykrx_auth.get_pykrx_stock_module()
    assert len(attempts) == 2


def test_the_cooldown_grows_so_a_real_block_costs_a_handful_of_logins(
    _login_state, monkeypatch
) -> None:
    clock = _login_state
    attempts = _break_the_import(monkeypatch)

    # Walk the cooldown ladder: 60s, then 300s, then 900s.
    for cooldown in (60.0, 300.0, 900.0):
        with pytest.raises(pykrx_auth.KrxLoginUnavailableError):
            pykrx_auth.get_pykrx_stock_module()
        clock.advance(cooldown + 1)

    # The fourth attempt enters the terminal 3600s cooldown.
    with pytest.raises(pykrx_auth.KrxLoginUnavailableError):
        pykrx_auth.get_pykrx_stock_module()
    assert len(attempts) == 4

    # Fifty minutes of a sustained block, polled every minute: no new login.
    for _ in range(50):
        clock.advance(60)
        with pytest.raises(pykrx_auth.KrxLoginUnavailableError):
            pykrx_auth.get_pykrx_stock_module()
    assert len(attempts) == 4


def test_the_cached_failure_carries_the_original_cause(_login_state, monkeypatch) -> None:
    import json

    _break_the_import(monkeypatch)

    with pytest.raises(pykrx_auth.KrxLoginUnavailableError) as first:
        pykrx_auth.get_pykrx_stock_module()
    with pytest.raises(pykrx_auth.KrxLoginUnavailableError) as second:
        pykrx_auth.get_pykrx_stock_module()

    assert isinstance(first.value.__cause__, json.JSONDecodeError)
    assert second.value is first.value


def test_a_session_refresh_is_suppressed_during_the_cooldown(_login_state, monkeypatch) -> None:
    # The refresh path is another three requests to the same login endpoint, so
    # the empty-streak trigger would reopen the amplification through it.
    _break_the_import(monkeypatch)

    with pytest.raises(pykrx_auth.KrxLoginUnavailableError):
        pykrx_auth.get_pykrx_stock_module()

    called: list[int] = []
    monkeypatch.setattr(
        pykrx_auth,
        "configure_krx_credentials_from_settings",
        lambda: called.append(1),
    )

    assert pykrx_auth.refresh_pykrx_session() is False
    assert called == []  # returned before touching credentials, let alone KRX
