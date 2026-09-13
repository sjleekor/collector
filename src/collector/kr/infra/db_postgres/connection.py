"""PostgreSQL connection management with thread-safe connection pooling.

Provides a thin wrapper around ``psycopg2`` using
``ThreadedConnectionPool`` for obtaining and releasing database connections.

Usage::

    from collector.kr.infra.db_postgres.connection import get_connection

    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Generator
from typing import Any

import psycopg2
import psycopg2.pool

logger = logging.getLogger(__name__)

# Module-level connection pools keyed by DSN.
_POOLS: dict[str, psycopg2.pool.ThreadedConnectionPool] = {}


def _mask_dsn(dsn: str) -> str:
    """Return a log-safe representation of a DSN."""
    scheme_index = dsn.find("://")
    at_index = dsn.rfind("@")
    if scheme_index == -1 or at_index == -1:
        return "***"
    return dsn[: scheme_index + 3] + "***" + dsn[at_index:]


def _get_pool(dsn: str) -> psycopg2.pool.ThreadedConnectionPool:
    """Return the DSN-specific connection pool, creating it on first call.

    Args:
        dsn: PostgreSQL connection string.

    Returns:
        The cached ``ThreadedConnectionPool`` for the DSN.
    """
    pool = _POOLS.get(dsn)
    if pool is None or pool.closed:
        logger.info("Creating new connection pool for DSN: %s", _mask_dsn(dsn))
        pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=dsn,
        )
        _POOLS[dsn] = pool
    return pool


@contextlib.contextmanager
def get_connection(dsn: str) -> Generator[Any, None, None]:
    """Yield a ``psycopg2`` connection, committing on success / rolling back on error.

    The connection is obtained from a ``ThreadedConnectionPool`` and returned
    to the pool in the ``finally`` block.

    Args:
        dsn: PostgreSQL connection string
            (e.g. ``postgresql://user:pass@host:5432/db``).

    Yields:
        A ``psycopg2.extensions.connection`` object.
    """
    pool = _get_pool(dsn)
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)
