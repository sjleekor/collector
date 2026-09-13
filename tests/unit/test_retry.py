"""Unit tests for collector.kr.util.retry."""

import pytest

from collector.kr.util.retry import retry


class TestRetry:
    """Tests for the retry decorator."""

    def test_success_on_first_attempt(self) -> None:
        """Function succeeds immediately — no retry needed."""
        calls = 0

        @retry(max_attempts=3, base_delay=0.01)
        def good_func() -> str:
            nonlocal calls
            calls += 1
            return "ok"

        assert good_func() == "ok"
        assert calls == 1

    def test_success_after_transient_failure(self) -> None:
        """Function fails once then succeeds — should return the successful result."""
        calls = 0

        @retry(max_attempts=3, base_delay=0.01)
        def flaky_func() -> str:
            nonlocal calls
            calls += 1
            if calls < 2:
                raise ValueError("transient")
            return "recovered"

        assert flaky_func() == "recovered"
        assert calls == 2

    def test_exhaustion_raises_last_exception(self) -> None:
        """All attempts fail — should raise the original exception."""

        @retry(max_attempts=2, base_delay=0.01)
        def always_fails() -> None:
            raise ValueError("permanent")

        with pytest.raises(ValueError, match="permanent"):
            always_fails()

    def test_specific_exception_filter(self) -> None:
        """Only the specified exception type triggers a retry."""
        calls = 0

        @retry(max_attempts=3, base_delay=0.01, exceptions=(TypeError,))
        def wrong_exception() -> None:
            nonlocal calls
            calls += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            wrong_exception()

        # Should have been called only once — ValueError is not in the retry list
        assert calls == 1

    def test_backoff_increases_delay(self) -> None:
        """Verify the function is called the expected number of times with backoff."""
        calls = 0

        @retry(max_attempts=3, base_delay=0.01, backoff_factor=2.0)
        def fail_twice() -> str:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise RuntimeError("not yet")
            return "done"

        assert fail_twice() == "done"
        assert calls == 3
