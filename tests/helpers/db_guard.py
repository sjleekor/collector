"""Refuse to run destructive integration tests against a non-local database.

``tests/integration/test_pipeline.py`` truncates pipeline tables and
``test_universe_snapshot_backfill_db.py`` deletes rows, both on
``get_settings().db_dsn``. In a shell where that DSN points at the server (for
example under ``collector db with-remote-dsn``) they would wipe production
tables, so they only run when the DSN names this machine.
"""

from __future__ import annotations

from psycopg2.extensions import parse_dsn

_LOCAL_HOSTS = frozenset({"", "localhost", "127.0.0.1", "::1"})


def dsn_is_local(dsn: str) -> bool:
    """True when every host in ``dsn`` is this machine (TCP loopback or a unix socket)."""
    host = parse_dsn(dsn).get("host", "")
    hosts = [h.strip() for h in host.split(",")] if host else [""]
    return all(h in _LOCAL_HOSTS or h.startswith("/") for h in hosts)
