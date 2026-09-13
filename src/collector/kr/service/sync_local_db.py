"""Use-case: sync the remote sj2-server PostgreSQL data into the local DB."""

from __future__ import annotations

import contextlib
import logging
import signal
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from collector.kr.domain.enums import RunStatus, RunType
from collector.kr.domain.models import IngestionRun
from collector.kr.infra.db_postgres.remote_sync import (
    reset_local_public_tables,
    resolve_remote_dsn,
    sync_remote_tables_to_local,
    validate_remote_sync_options,
)
from collector.kr.infra.db_postgres.repositories import PostgresStorage
from collector.kr.util.time import now_kst

logger = logging.getLogger(__name__)


class SyncInterruptedError(RuntimeError):
    """Raised when the remote DB sync is interrupted by a signal."""


@dataclass(slots=True)
class RemoteDbSyncResult:
    """Summary of a remote-to-local database sync run."""

    started_at: datetime
    ended_at: datetime | None = None
    full_refresh: bool = False
    all_tables: bool = False
    tables: tuple[str, ...] | None = None
    batch_size: int = 0
    remote_host: str = ""
    ssh_host: str | None = None
    ssh_compression: bool = False
    table_counts: dict[str, int] = field(default_factory=dict)
    error: str | None = None

    @property
    def total_rows(self) -> int:
        """Return the total copied row count."""
        return sum(self.table_counts.values())


@contextlib.contextmanager
def _interrupt_guard() -> None:
    """Raise a Python exception when SIGINT or SIGTERM interrupts the sync."""
    received_signal: str | None = None

    def _handler(signum: int, frame: object | None) -> None:
        nonlocal received_signal
        del frame
        received_signal = signal.Signals(signum).name
        raise SyncInterruptedError(f"Remote DB sync interrupted by {received_signal}")

    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)


def sync_remote_db_to_local(
    *,
    local_dsn: str,
    remote_db_info_path: str | Path,
    batch_size: int,
    full_refresh: bool = False,
    all_tables: bool = False,
    tables: tuple[str, ...] | None = None,
    remote_host_override: str | None = None,
    ssh_host: str | None = None,
    ssh_local_port: int | None = None,
    ssh_compression: bool = False,
) -> RemoteDbSyncResult:
    """Sync remote PostgreSQL tables from sj2-server into the local database."""
    started_at = now_kst()
    result = RemoteDbSyncResult(
        started_at=started_at,
        full_refresh=full_refresh,
        all_tables=all_tables,
        tables=tables,
        batch_size=batch_size,
        ssh_host=ssh_host,
        ssh_compression=ssh_compression,
    )
    storage = PostgresStorage(local_dsn)

    run = IngestionRun(
        run_type=RunType.REMOTE_DB_SYNC,
        started_at=started_at,
        status=RunStatus.RUNNING,
        params={
            "remote_db_info_path": str(remote_db_info_path),
            "batch_size": batch_size,
            "full_refresh": full_refresh,
            "all_tables": all_tables,
            "tables": list(tables) if tables is not None else None,
            "remote_host_override": remote_host_override,
            "ssh_host": ssh_host,
            "ssh_local_port": ssh_local_port,
            "ssh_compression": ssh_compression,
        },
    )

    try:
        validate_remote_sync_options(
            batch_size=batch_size,
            full_refresh=full_refresh,
            all_tables=all_tables,
            tables=tables,
        )
    except Exception as exc:
        result.ended_at = now_kst()
        result.error = str(exc)
        return result

    if full_refresh and all_tables:
        dropped = reset_local_public_tables(local_dsn)
        logger.info(
            "Reset local pipeline sync tables before full-refresh sync: dropped_tables=%s",
            dropped,
        )

    storage.init_schema()
    storage.record_run(run)

    try:
        with _interrupt_guard():
            with resolve_remote_dsn(
                db_info_path=remote_db_info_path,
                host_override=remote_host_override,
                ssh_host=ssh_host,
                ssh_local_port=ssh_local_port,
                ssh_compression=ssh_compression,
            ) as (remote_info, remote_dsn):
                result.remote_host = remote_info.host
                logger.info(
                    "Starting remote DB sync: remote_host=%s full_refresh=%s "
                    "all_tables=%s tables=%s batch_size=%s ssh_host=%s "
                    "ssh_compression=%s",
                    remote_info.host,
                    full_refresh,
                    all_tables,
                    tables,
                    batch_size,
                    ssh_host,
                    ssh_compression,
                )

                table_counts = sync_remote_tables_to_local(
                    remote_dsn=remote_dsn,
                    local_dsn=local_dsn,
                    batch_size=batch_size,
                    full_refresh=full_refresh,
                    all_tables=all_tables,
                    tables=tables,
                )
                result.table_counts = table_counts
                result.ended_at = now_kst()

                run.ended_at = result.ended_at
                run.status = RunStatus.SUCCESS
                run.counts = table_counts
                storage.record_run(run)
                return result
    except Exception as exc:
        logger.exception("Remote DB sync failed")
        result.ended_at = now_kst()
        result.error = str(exc)

        run.ended_at = result.ended_at
        run.status = RunStatus.FAILED
        run.error_summary = str(exc)
        storage.record_run(run)
        return result
