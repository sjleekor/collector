"""Unit tests for the source-blocked circuit breaker.

Every collector treats a failed request as a per-item error: record it, move on.
That is right for one bad ticker and wrong for a source that has started
refusing, because the loop then works through its whole target list against a
server already saying no. A market-cap backfill is 6,000 slices retried four
times each, so a sustained block becomes 24,000 requests — which is how a
temporary throttle becomes a lasting one.

Nothing in this repo guarded against that: the KRX MDC adapters back off 45-180s
after an error, but the pykrx path had no error backoff and nothing anywhere
stopped a run.
"""

from __future__ import annotations

import pytest

from collector.kr.util.pipeline import ConsecutiveFailureGuard, SourceBlockedError


def _guard(threshold: int, **kwargs) -> tuple[ConsecutiveFailureGuard, list[float]]:
    slept: list[float] = []
    guard = ConsecutiveFailureGuard(
        threshold,
        label="test source",
        sleep_fn=slept.append,
        **kwargs,
    )
    return guard, slept


def test_raises_once_the_threshold_is_reached() -> None:
    guard, _ = _guard(3)

    guard.record_failure("a")
    guard.record_failure("b")
    with pytest.raises(SourceBlockedError, match="3 consecutive failures"):
        guard.record_failure("c")


def test_a_success_resets_the_counter() -> None:
    # A scatter of unrelated bad items must never trip it; only an
    # uninterrupted run of failures should.
    guard, _ = _guard(3)

    guard.record_failure("a")
    guard.record_failure("b")
    guard.record_success()
    guard.record_failure("c")
    guard.record_failure("d")

    assert guard.consecutive_failures == 2


def test_threshold_zero_disables_the_guard() -> None:
    guard, _ = _guard(0)

    for i in range(50):
        guard.record_failure(str(i))

    assert guard.consecutive_failures == 50
    assert guard.enabled is False


def test_backoff_grows_between_consecutive_failures() -> None:
    # The per-request retry backs off within one item and then resets, so
    # without this the pace between items is unchanged while everything fails.
    guard, slept = _guard(5, backoff_seconds=1.0)

    guard.record_failure("a")
    guard.record_failure("b")
    guard.record_failure("c")

    assert slept == [1.0, 2.0, 4.0]


def test_backoff_is_capped() -> None:
    guard, slept = _guard(0, backoff_seconds=1.0, max_backoff_seconds=4.0)

    for i in range(6):
        guard.record_failure(str(i))

    assert slept == [1.0, 2.0, 4.0, 4.0, 4.0, 4.0]


def test_no_backoff_sleep_on_the_failure_that_raises() -> None:
    # Sleeping and then aborting wastes the operator's time for no benefit.
    guard, slept = _guard(2, backoff_seconds=1.0)

    guard.record_failure("a")
    with pytest.raises(SourceBlockedError):
        guard.record_failure("b")

    assert slept == [1.0]


def test_message_names_the_source_and_the_last_failure() -> None:
    guard, _ = _guard(1)

    with pytest.raises(SourceBlockedError) as excinfo:
        guard.record_failure("2024-01-02/KOSPI: connection refused")

    message = str(excinfo.value)
    assert "test source" in message
    assert "connection refused" in message
    assert "re-run later to resume" in message
