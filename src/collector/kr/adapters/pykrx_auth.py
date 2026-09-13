"""Shared helpers for pykrx import-time KRX authentication.

pykrx authenticates once, at import, in ``pykrx.website.comm.webio``. That single
fact produces two distinct failure modes, and the N3 snapshot backfill hit both
on the night of 2026-08-15.

Failure 1 — the session dies mid-run
------------------------------------
pykrx re-logs in only when its own clock says the session expired (one hour,
minus a five-minute buffer). It has no notion of the server having dropped the
session first, and KRX does drop it first: a process would work for two to four
minutes and then every response came back as an error page, while pykrx kept
sending dead cookies because its clock still said the session was good. Round 1
wrote 23 snapshots in 127s at 0.5s pacing; round 2 wrote 14 in 253s at 1.5s. The
*slower* run got *fewer* rows before dying, so the pace was not what was being
punished — process lifetime was.

:func:`call_with_session_retry` handles this by keying on the response instead of
the clock. Both ``Get`` and ``Post`` resolve their session through the
module-global the auth module owns, and ``KRXSession.refresh`` mutates that
object in place, so re-logging in there fixes every pykrx call at once.

Failure 2 — the login endpoint itself goes down
-----------------------------------------------
Because the login runs at import, a login that returns HTML makes ``import
pykrx`` raise, and no amount of call-level retry can reach that: there is no
call, the library never loads. From roughly 23:54 KST the whole library was
unimportable, one probe slipped through at 00:11, and after 00:19 nothing worked
at all. The KRX MDC collectors, which authenticate separately, kept working
throughout — so this is upstream, not credentials.

:class:`KrxLoginUnavailableError` exists to say that in one line. Left raw it
surfaced as forty lines of traceback per target date, ending in "Expecting
value: line 13 column 1", naming nothing.

The operational consequence is scheduling, not code: run pykrx backfills well
clear of midnight, and let the circuit breaker stop the run when they are not.
"""

from __future__ import annotations

import contextlib
import io
import logging
import os
import time
from collections.abc import Callable
from functools import lru_cache
from types import ModuleType

from collector.kr.adapters.krx_scraping import ensure_krx_scraping_allowed
from collector.kr.infra.config.settings import configure_krx_credentials_from_settings

logger = logging.getLogger(__name__)


class KrxLoginUnavailableError(RuntimeError):
    """KRX's login endpoint did not answer with JSON, so pykrx cannot be used."""


# A failed login must not be retried per call.
#
# ``lru_cache`` does not cache exceptions, and a failed ``from pykrx import
# stock`` leaves nothing in ``sys.modules``, so every caller re-runs the whole
# import — including ``webio``'s import-time login, which is a warmup GET, a
# login-page GET and a login POST. Three requests to KRX's login endpoint, per
# call, producing nothing.
#
# Measured on one 20-minute run during the 2026-08-16 block: 32 failed dates
# became 32 login flows, ~96 requests to MDCCOMS001D1.cmd with zero useful work.
# KRX restricted the IP for "자동화 수단을 통한 비정상 대량 조회", and repeatedly
# retrying the *login* is the single worst-looking thing to do while blocked.
#
# So the failure is cached with a cooldown that grows: a network blip still
# recovers within a minute, while a real block costs four login attempts an hour
# instead of hundreds.
_LOGIN_FAILURE_COOLDOWNS = (60.0, 300.0, 900.0, 3600.0)

_login_failure: tuple[float, KrxLoginUnavailableError] | None = None
_login_failure_count = 0


def reset_krx_login_failure_state() -> None:
    """Forget a cached login failure. For tests, and for an operator retrying."""
    global _login_failure, _login_failure_count
    _login_failure = None
    _login_failure_count = 0


def _cached_login_failure(now: float) -> KrxLoginUnavailableError | None:
    if _login_failure is None:
        return None
    failed_at, error = _login_failure
    index = min(_login_failure_count, len(_LOGIN_FAILURE_COOLDOWNS)) - 1
    cooldown = _LOGIN_FAILURE_COOLDOWNS[max(index, 0)]
    remaining = cooldown - (now - failed_at)
    if remaining <= 0:
        return None
    logger.warning(
        "Skipping the KRX login: it failed %.0fs ago and the cooldown has %.0fs left. "
        "Retrying the login while blocked is what gets an IP restricted.",
        now - failed_at,
        remaining,
    )
    return error


def _record_login_failure(error: KrxLoginUnavailableError, now: float) -> None:
    global _login_failure, _login_failure_count
    _login_failure_count += 1
    _login_failure = (now, error)


@lru_cache(maxsize=1)
def get_pykrx_stock_module() -> ModuleType:
    """Import pykrx.stock after loading KRX credentials, suppressing auth chatter.

    Raises:
        KrxLoginUnavailableError: KRX's login endpoint returned something other
            than JSON. ``pykrx.website.comm.webio`` logs in at *import* time, so
            this takes the whole library down, not one call. A failure is cached
            for a growing cooldown, so callers after the first one fail without
            touching KRX at all.
    """
    ensure_krx_scraping_allowed(
        "importing pykrx",
        "`prices backfill --source naver`, `universe sync --source krx-openapi`, "
        "or `prices market-cap-backfill --source krx-openapi`",
    )

    now = time.monotonic()
    cached = _cached_login_failure(now)
    if cached is not None:
        raise cached

    configure_krx_credentials_from_settings()
    captured_output = io.StringIO()
    try:
        with (
            contextlib.redirect_stdout(captured_output),
            contextlib.redirect_stderr(captured_output),
        ):
            from pykrx import stock
    except ValueError as exc:
        # requests raises JSONDecodeError, a ValueError subclass, from
        # `resp.json()` inside login_krx. Unwrapped, this surfaces as forty
        # lines of traceback ending in "Expecting value: line 13 column 1",
        # repeated once per target, with nothing naming the actual cause —
        # which is what a KRX maintenance window looks like from in here.
        # Observed 2026-08-16 from roughly 23:54 KST onward.
        error = KrxLoginUnavailableError(
            "KRX login returned a non-JSON response, so `import pykrx` fails and "
            "every pykrx collector is blocked. This is an upstream condition "
            "(maintenance window or a block), not a credential problem — verify "
            "with a KRX MDC collector, which authenticates separately. Re-run "
            f"when KRX answers again. Underlying error: {exc}"
        )
        error.__cause__ = exc
        _record_login_failure(error, now)
        raise error

    output = captured_output.getvalue()
    if "KRX 로그인 실패" in output:
        logger.warning("pykrx KRX login failed; check KRX_ID/KRX_PW.")
    elif "KRX 로그인 완료" in output:
        logger.info("pykrx KRX login completed.")

    reset_krx_login_failure_state()
    return stock


def refresh_pykrx_session() -> bool:
    """Force pykrx to log in to KRX again, regardless of its expiry clock.

    Shares the login-failure cooldown with :func:`get_pykrx_stock_module`. A
    refresh is another three requests to the login endpoint, and during a block
    the empty-streak trigger would fire one every few calls — the same
    amplification, through a different door.

    Returns:
        ``True`` when a new session was established.
    """
    # The other door into the same login. Gating only the import would leave
    # the empty-streak trigger free to re-authenticate on a host that opted out.
    ensure_krx_scraping_allowed("refreshing the pykrx KRX session", "the Open API path")

    now = time.monotonic()
    if _cached_login_failure(now) is not None:
        return False

    configure_krx_credentials_from_settings()
    login_id = os.getenv("KRX_ID")
    login_pw = os.getenv("KRX_PW")
    if not (login_id and login_pw):
        logger.warning("Cannot refresh the KRX session: KRX_ID/KRX_PW are not set.")
        return False

    try:
        from pykrx.website.comm import auth
    except ImportError:  # pragma: no cover - pykrx is a hard dependency
        logger.exception("pykrx auth module is unavailable")
        return False

    session = auth.get_auth_session()
    if session is None:
        logger.warning("pykrx has no KRX session to refresh.")
        return False

    captured_output = io.StringIO()
    try:
        with (
            contextlib.redirect_stdout(captured_output),
            contextlib.redirect_stderr(captured_output),
        ):
            refreshed = bool(session.refresh(login_id, login_pw))
    except ValueError as exc:
        # Same non-JSON login response as the import path; record it so the
        # cooldown covers both doors.
        error = KrxLoginUnavailableError(
            f"KRX login returned a non-JSON response during a session refresh: {exc}"
        )
        error.__cause__ = exc
        _record_login_failure(error, now)
        logger.warning("KRX session refresh failed: the login endpoint is not answering.")
        return False

    if refreshed:
        logger.info("KRX session re-established after a failed response.")
        reset_krx_login_failure_state()
    else:
        logger.warning("KRX session refresh failed.")
    return refreshed


# A dropped session cannot be told apart from "no data" by exception, because
# pykrx's `dataframe_empty_handler` swallows JSONDecodeError and returns an
# empty frame. It CAN be told apart by clustering: a dead session empties every
# call, while genuinely absent data is scattered. So the trigger is consecutive
# empties, not the first one.
_INITIAL_EMPTY_STREAK_TRIGGER = 3
_MAX_EMPTY_STREAK_TRIGGER = 200

_consecutive_empty = 0
_empty_streak_trigger = _INITIAL_EMPTY_STREAK_TRIGGER


def reset_session_retry_state() -> None:
    """Forget the empty-streak state. For tests and for starting a fresh run."""
    global _consecutive_empty, _empty_streak_trigger
    _consecutive_empty = 0
    _empty_streak_trigger = _INITIAL_EMPTY_STREAK_TRIGGER


def call_with_session_retry[T](
    fetch: Callable[[], T],
    *,
    is_empty: Callable[[T], bool],
    label: str,
) -> T:
    """Run *fetch*, re-logging in and retrying when empties start clustering.

    The backoff is on the *trigger*, not on the call. If a refresh does not turn
    the empty into data, the session was fine and the data is genuinely absent,
    so the streak required to try again grows fourfold. A run of nothing but
    legitimate empties — the delisted-ticker price backfill, where most tickers
    have no rows in most ranges — therefore pays at most a handful of wasted
    logins instead of one per few tickers.

    Args:
        fetch: The pykrx call, already bound to its arguments.
        is_empty: Whether a result counts as empty.
        label: What is being fetched, for the log line.

    Returns:
        The retried result when a retry happened, otherwise the first result.
    """
    global _consecutive_empty, _empty_streak_trigger

    result = fetch()
    if not is_empty(result):
        _consecutive_empty = 0
        _empty_streak_trigger = _INITIAL_EMPTY_STREAK_TRIGGER
        return result

    _consecutive_empty += 1
    if _consecutive_empty < _empty_streak_trigger:
        return result

    logger.warning(
        "pykrx %s: %d consecutive empty responses — refreshing the KRX session and retrying.",
        label,
        _consecutive_empty,
    )
    _consecutive_empty = 0
    if not refresh_pykrx_session():
        return result

    retried = fetch()
    if is_empty(retried):
        # The session was not the problem. Make the next attempt cost more.
        _empty_streak_trigger = min(_empty_streak_trigger * 4, _MAX_EMPTY_STREAK_TRIGGER)
        logger.info(
            "pykrx %s: still empty after re-login, so the data is absent; "
            "next refresh needs %d consecutive empties.",
            label,
            _empty_streak_trigger,
        )
    else:
        _empty_streak_trigger = _INITIAL_EMPTY_STREAK_TRIGGER
    return retried
