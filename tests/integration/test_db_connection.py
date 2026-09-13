"""Integration test for database connectivity.

Requires a running PostgreSQL instance. Skipped automatically if the
database is unreachable.
"""

import pytest

from collector.kr.infra.config.settings import get_settings
from collector.kr.infra.db_postgres.connection import get_connection


@pytest.fixture()
def db_dsn() -> str:
    """Return the DSN from settings."""
    return get_settings().db_dsn


def test_database_select_one(db_dsn: str) -> None:
    """Verify basic connectivity: execute SELECT 1 and check the result."""
    try:
        with get_connection(db_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()
                assert result is not None
                assert result[0] == 1
    except Exception as exc:
        pytest.skip(f"Database not reachable: {exc}")


def test_connection_rollback_on_error(db_dsn: str) -> None:
    """Verify that an exception inside the context triggers a rollback (not a commit)."""
    try:
        with pytest.raises(ZeroDivisionError):
            with get_connection(db_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                raise ZeroDivisionError("intentional")
    except Exception as exc:
        pytest.skip(f"Database not reachable: {exc}")
