"""CLI entrypoint — argparse wiring for ``collector``.

Subcommands::

    collector db init
    collector db sync-remote [--db-info-path ...] [--ssh-host ...] [--full-refresh]
                                  [--tables ...] [--all-tables]
    collector universe sync  [--source fdr|pykrx] [--markets ...]
    collector prices backfill [--market ...] [--tickers ...] [--start ...]
    collector prices market-cap-backfill [--market ...] [--start ...] [--end ...]
    collector validate       [--date ...] [--market ...]

Each subcommand parses arguments and delegates to the corresponding
service function.  Providers and storage are instantiated here (dependency
wiring) but currently raise ``NotImplementedError`` since adapters are stubs.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import threading
from collections.abc import Mapping
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

from collector.kr.adapters.opendart_common.client import OpenDartRequestExecutor
from collector.kr.domain.enums import UniverseScope
from collector.kr.infra.config.settings import get_settings
from collector.kr.infra.logging.setup import setup_logging
from collector.kr.service.backfill_stock_master import DEFAULT_SNAPSHOT_SOURCES
from collector.kr.service.freshness import (
    DEFAULT_MAX_LAG_CALENDAR_DAYS,
    DEFAULT_MAX_LAG_TRADING_DAYS,
    DEFAULT_MAX_MARKET_CAP_LAG_TRADING_DAYS,
)
from collector.kr.service.sync_kis_flows import (
    DEFAULT_LOOKBACK_DAYS as DEFAULT_KIS_LOOKBACK_DAYS,
)
from collector.kr.service.sync_kis_flows import (
    DEFAULT_MAX_CONSECUTIVE_FAILURES as DEFAULT_KIS_MAX_CONSECUTIVE_FAILURES,
)
from collector.kr.service.sync_kis_flows import (
    DEFAULT_NO_DATA_TTL_DAYS as DEFAULT_KIS_NO_DATA_TTL_DAYS,
)

logger = logging.getLogger(__name__)


def _build_opendart_request_executor() -> OpenDartRequestExecutor:
    """Construct the shared OpenDART executor for one CLI command."""
    settings = get_settings()
    return OpenDartRequestExecutor(settings.opendart_api_keys)


def _exit_if_opendart_key_exhausted(result: object, label: str) -> None:
    """Stop shell schedulers when every OpenDART key has hit its daily limit."""
    if getattr(result, "opendart_exhaustion_reason", None) != "all_rate_limited":
        return

    error = getattr(result, "errors", {}).get(
        "pipeline", "All OpenDART API keys are temporarily rate limited."
    )
    print(f"❌ {label} stopped: {error}", file=sys.stderr)
    sys.exit(75)


def _split_csv(value: str | None) -> list[str] | None:
    """Split a comma-separated CLI value into stripped non-empty tokens."""
    if value is None:
        return None
    values = [item.strip() for item in value.split(",") if item.strip()]
    return values or None


def _parse_common_sources(value: str) -> list[object]:
    """Parse a comma-separated common feature source allowlist."""
    from collector.kr.domain.enums import Source

    source_by_name = {
        "pykrx": Source.PYKRX,
        "krx": Source.KRX,
        "fdr": Source.FDR,
        "ecos": Source.ECOS,
        "fred": Source.FRED,
    }
    sources: list[Source] = []
    for raw_source in value.split(","):
        normalized = raw_source.strip().lower()
        if not normalized:
            continue
        source = source_by_name.get(normalized)
        if source is None:
            supported = ", ".join(sorted(source_by_name))
            raise argparse.ArgumentTypeError(
                f"Unsupported common feature source: {raw_source!r} (supported: {supported})"
            )
        sources.append(source)
    if not sources:
        raise argparse.ArgumentTypeError("At least one common feature source is required.")
    return sources


def _build_common_feature_providers(sources: list[object]) -> list[object]:
    """Instantiate common feature providers for a source allowlist."""
    from collector.kr.adapters.common_features_ecos import EcosCommonFeatureProvider
    from collector.kr.adapters.common_features_fdr import FdrCommonFeatureProvider
    from collector.kr.adapters.common_features_fred import FredCommonFeatureProvider
    from collector.kr.adapters.common_features_krx import KrxCommonFeatureProvider
    from collector.kr.adapters.common_features_pykrx import PykrxCommonFeatureProvider
    from collector.kr.domain.enums import Source

    provider_by_source = {
        Source.PYKRX: PykrxCommonFeatureProvider,
        Source.KRX: KrxCommonFeatureProvider,
        Source.FDR: FdrCommonFeatureProvider,
        Source.ECOS: EcosCommonFeatureProvider,
        Source.FRED: FredCommonFeatureProvider,
    }
    providers = []
    for source in sources:
        provider_factory = provider_by_source.get(source)
        if provider_factory is None:
            raise ValueError(f"Unsupported common feature source: {source}")
        providers.append(provider_factory())
    return providers


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------


def _handle_db_init(args: argparse.Namespace) -> None:
    """Handle ``collector db init``."""
    print("→ db init: initialising database schema…")
    settings = get_settings()
    from collector.kr.infra.db_postgres.repositories import PostgresStorage

    storage = PostgresStorage(settings.db_dsn)
    try:
        storage.init_schema()
        print("✅ Schema initialisation successful.")
    except Exception as exc:
        print(f"❌ Schema initialisation failed: {exc}", file=sys.stderr)
        sys.exit(1)


def _parse_remote_sync_tables(raw_value: str | None) -> tuple[str, ...] | None:
    """Parse a comma-separated remote sync table list."""
    if raw_value is None:
        return None
    table_names = tuple(part.strip() for part in raw_value.split(",") if part.strip())
    return table_names or None


def _handle_db_sync_remote(args: argparse.Namespace) -> None:
    """Handle ``collector db sync-remote``."""
    settings = get_settings()

    db_info_path = args.db_info_path or (
        str(settings.remote_db_info_path) if settings.remote_db_info_path else None
    )
    if not db_info_path:
        print(
            "❌ --db-info-path not given and REMOTE_DB_INFO_PATH is not set in .env.",
            file=sys.stderr,
        )
        sys.exit(1)
    batch_size = args.batch_size or settings.remote_db_batch_size
    remote_host_override = args.remote_host or settings.remote_db_host_override
    ssh_host = args.ssh_host or settings.remote_db_ssh_host
    ssh_local_port = args.ssh_local_port or settings.remote_db_ssh_local_port
    ssh_compression = (
        args.ssh_compression
        if args.ssh_compression is not None
        else settings.remote_db_ssh_compression
    )
    tables = _parse_remote_sync_tables(args.tables)

    print(
        f"→ db sync-remote: db_info_path={db_info_path}, "
        f"batch_size={batch_size}, full_refresh={args.full_refresh}, "
        f"all_tables={args.all_tables}, tables={tables}, "
        f"remote_host_override={remote_host_override}, ssh_host={ssh_host}, "
        f"ssh_local_port={ssh_local_port}, ssh_compression={ssh_compression}"
    )

    from collector.kr.service.sync_local_db import sync_remote_db_to_local

    result = sync_remote_db_to_local(
        local_dsn=settings.db_dsn,
        remote_db_info_path=db_info_path,
        batch_size=batch_size,
        full_refresh=args.full_refresh,
        all_tables=args.all_tables,
        tables=tables,
        remote_host_override=remote_host_override,
        ssh_host=ssh_host,
        ssh_local_port=ssh_local_port,
        ssh_compression=ssh_compression,
    )

    if result.error:
        print(f"❌ Remote DB sync failed: {result.error}", file=sys.stderr)
        sys.exit(1)

    print("✅ Remote DB sync completed.")
    print(f"   - Remote host: {result.remote_host}")
    print(f"   - Total rows synced: {result.total_rows}")
    for table_name, row_count in result.table_counts.items():
        print(f"   - {table_name}: {row_count}")


_SIGNAL_FORWARD_GRACE_SECONDS = 10.0


def _run_child_with_env_var(command: list[str], env_name: str, env_value: str) -> int:
    """Run ``command`` with one extra env var injected into the child only.

    SIGINT/SIGTERM received by this process are forwarded to the child's own
    process group (it runs in a new session so a terminal Ctrl-C targeting our
    group does not already reach it), escalating to SIGKILL after a grace
    period if the child ignores the signal. The child's exit status is
    preserved as-is, or as ``128 + signum`` if it died from a signal — the
    standard shell convention — so meaningful codes like OpenDART's exit 75
    are not swallowed.
    """
    child_env = os.environ.copy()
    child_env[env_name] = env_value

    proc = subprocess.Popen(command, env=child_env, start_new_session=True)
    grace_timer: threading.Timer | None = None

    def _kill_child(sig: int) -> None:
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            pass

    def _forward(signum: int, _frame: object) -> None:
        nonlocal grace_timer
        _kill_child(signum)
        grace_timer = threading.Timer(
            _SIGNAL_FORWARD_GRACE_SECONDS, _kill_child, args=(signal.SIGKILL,)
        )
        grace_timer.daemon = True
        grace_timer.start()

    previous_sigint = signal.signal(signal.SIGINT, _forward)
    previous_sigterm = signal.signal(signal.SIGTERM, _forward)
    try:
        proc.wait()
    finally:
        if grace_timer is not None:
            grace_timer.cancel()
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)

    if proc.returncode < 0:
        return 128 - proc.returncode
    return proc.returncode


def _handle_db_with_remote_dsn(args: argparse.Namespace) -> None:
    """Handle ``collector db with-remote-dsn -- <command...>``.

    Resolves the remote sj2-server DSN the same way ``db sync-remote`` does
    (including the SSH-tunnel case, which needs the tunnel alive for the
    whole child run — this is why the DSN is injected around a wrapped
    subprocess rather than just printed). ``SDC_REMOTE_DSN`` is set only in
    the child's environment; this process's own env and stdout/stderr never
    carry the secret.
    """
    settings = get_settings()

    db_info_path = args.db_info_path or (
        str(settings.remote_db_info_path) if settings.remote_db_info_path else None
    )
    if not db_info_path:
        print(
            "❌ --db-info-path not given and REMOTE_DB_INFO_PATH is not set in .env.",
            file=sys.stderr,
        )
        sys.exit(1)
    remote_host_override = args.remote_host or settings.remote_db_host_override
    ssh_host = args.ssh_host or settings.remote_db_ssh_host
    ssh_local_port = args.ssh_local_port or settings.remote_db_ssh_local_port
    ssh_compression = (
        args.ssh_compression
        if args.ssh_compression is not None
        else settings.remote_db_ssh_compression
    )

    command = list(args.remote_command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        print("db with-remote-dsn: missing command after `--`", file=sys.stderr)
        sys.exit(2)

    from collector.kr.infra.db_postgres.remote_sync import resolve_remote_dsn

    with resolve_remote_dsn(
        db_info_path=db_info_path,
        host_override=remote_host_override,
        ssh_host=ssh_host,
        ssh_local_port=ssh_local_port,
        ssh_compression=ssh_compression,
    ) as (_info, dsn):
        exit_code = _run_child_with_env_var(command, "SDC_REMOTE_DSN", dsn)

    sys.exit(exit_code)


# Error keys that mean the run stopped early rather than finished with some
# items failing. A run that ends with per-item errors is `partial` by design and
# exits cleanly; these two are not partial, because the remaining work was never
# attempted.
ABORTED_RUN_ERROR_KEYS = ("source_blocked", "pipeline")


def _build_krx_openapi_client(settings: object) -> object:
    """Build the KRX Open API client, or exit with a usable message.

    Exits 2 rather than raising because a missing key is an operator problem
    with a one-line fix, and the traceback would bury it.
    """
    from collector.kr.adapters.market_data_krx_openapi import KrxOpenApiClient

    keys = settings.krx_openapi_auth_keys  # type: ignore[attr-defined]
    if not keys:
        print(
            "❌ KRX Open API needs AUTH_KEYS in .env (comma-separated).\n"
            "   Issue keys at https://openapi.krx.co.kr, then apply (이용 신청) "
            "for each endpoint separately —\n"
            "   a valid key on an unapproved endpoint returns "
            "401 'Unauthorized API Call'.",
            file=sys.stderr,
        )
        sys.exit(2)

    return KrxOpenApiClient(
        keys,
        base_url=settings.krx_openapi_base_url,  # type: ignore[attr-defined]
        timeout_seconds=settings.krx_openapi_timeout_seconds,  # type: ignore[attr-defined]
        requests_per_second=settings.krx_openapi_requests_per_second,  # type: ignore[attr-defined]
        max_burst_requests=settings.krx_openapi_max_burst_requests,  # type: ignore[attr-defined]
    )


def _build_krx_throttle(
    args: argparse.Namespace,
    *,
    logger_instance: logging.Logger | None = None,
) -> object:
    """Build the KRX pacing policy for a pykrx-backed backfill.

    pykrx reaches the same ``data.krx.co.kr`` portal the MDC collectors do, so
    it gets the same policy from the same settings. They used to differ by two
    orders of magnitude — the MDC path spacing requests 1.5-4.0s apart with a
    45-180s backoff after an error, the pykrx path sleeping a flat 0.4s with no
    error backoff at all — and on 2026-08-16 KRX restricted the collector's IP
    for "자동화 수단을 통한 비정상 대량 조회".

    One host, one policy. ``--min-delay-seconds`` / ``--max-delay-seconds``
    override per run; everything else comes from ``KRX_*`` settings, which is
    the same way the MDC path is tuned.
    """
    from collector.kr.util.pipeline import HumanThrottle, HumanThrottlePolicy

    settings = get_settings()
    min_delay = (
        args.min_delay_seconds
        if args.min_delay_seconds is not None
        else settings.krx_min_delay_seconds
    )
    max_delay = (
        args.max_delay_seconds
        if args.max_delay_seconds is not None
        else settings.krx_max_delay_seconds
    )
    if min_delay > max_delay:
        print(
            f"❌ --min-delay-seconds ({min_delay}) must be <= "
            f"--max-delay-seconds ({max_delay}).",
            file=sys.stderr,
        )
        sys.exit(2)

    return HumanThrottle(
        HumanThrottlePolicy(
            min_delay_seconds=min_delay,
            max_delay_seconds=max_delay,
            long_rest_every=settings.krx_long_rest_every,
            long_rest_min_seconds=settings.krx_long_rest_min_seconds,
            long_rest_max_seconds=settings.krx_long_rest_max_seconds,
            auth_cooldown_seconds=settings.krx_auth_cooldown_seconds,
            error_backoff_min_seconds=settings.krx_error_backoff_min_seconds,
            error_backoff_max_seconds=settings.krx_error_backoff_max_seconds,
        ),
        logger_instance=logger_instance or logger,
    )


def _add_krx_source_argument(parser: argparse.ArgumentParser) -> None:
    """Attach ``--source`` to a command that can read KRX two ways.

    ``krx-openapi`` is the default because the pykrx path reaches
    ``data.krx.co.kr``, whose terms forbid automated collection, and KRX
    restricted this host for exactly that on 2026-08-16. The scraping path
    stays reachable only by naming it, until K-5 removes it.
    """
    parser.add_argument(
        "--source",
        choices=("krx-openapi", "pykrx"),
        default="krx-openapi",
        help=(
            "krx-openapi (default) uses the official API and needs AUTH_KEYS. "
            "pykrx scrapes data.krx.co.kr, which violates its terms of service "
            "and is kept only for comparison."
        ),
    )


def _add_krx_throttle_arguments(parser: argparse.ArgumentParser) -> None:
    """Attach the per-run KRX pacing overrides to a pykrx-backed command."""
    parser.add_argument(
        "--min-delay-seconds",
        type=float,
        default=None,
        help=(
            "Minimum spacing between KRX requests "
            "(default: KRX_MIN_DELAY_SECONDS, the same setting the MDC path uses)."
        ),
    )
    parser.add_argument(
        "--max-delay-seconds",
        type=float,
        default=None,
        help=(
            "Maximum spacing between KRX requests; the actual gap is random in "
            "[min, max] (default: KRX_MAX_DELAY_SECONDS)."
        ),
    )


def _exit_if_run_aborted(errors: Mapping[str, str], label: str) -> None:
    """Turn an aborted run into a non-zero exit code.

    The N3 snapshot backfill stopped at 5 consecutive failures exactly as
    designed, printed the reason, wrote 23 of 146 snapshots -- and exited 0, so
    Cronicle recorded it as a success. A stop condition the scheduler cannot see
    is not a stop condition. Every backfill command routes its aborted cases
    through here so a new one cannot quietly omit it (enforced by
    tests/unit/test_cli_handler_arguments.py).
    """
    for key in ABORTED_RUN_ERROR_KEYS:
        if key in errors:
            print(f"❌ {label} aborted ({key}): {errors[key]}", file=sys.stderr)
            sys.exit(1)


def _handle_ops_freshness_report(args: argparse.Namespace) -> None:
    """Handle ``collector ops freshness-report``."""
    settings = get_settings()

    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.freshness import build_freshness_report

    storage = PostgresStorage(settings.db_dsn)
    report = build_freshness_report(storage, running_limit=args.running_limit)

    print("✅ Freshness report generated.")
    print(f"   - daily_ohlcv latest: {report.price_latest_date or '-'}")
    print(f"   - daily_market_cap latest: {report.market_cap_latest_date or '-'}")

    print("   - flow group latest:")
    for group, latest in sorted(report.flow_group_latest_dates.items()):
        print(f"     {group}: {latest or '-'}")

    # Printed separately so a metric nobody collects is visible as a decision
    # rather than as a group that quietly stopped moving.
    if report.discontinued_flow_metrics:
        print("   - flow metrics no longer collected:")
        for metric, reason in sorted(report.discontinued_flow_metrics.items()):
            last_seen = report.flow_metric_latest_dates.get(metric)
            print(f"     {metric}: last={last_seen or '-'} ({reason})")

    print("   - common raw latest by source:")
    common_by_source: dict[str, list[str]] = {}
    for row in report.common_series:
        common_by_source.setdefault(row.source.value, []).append(
            f"{row.series_id}={row.latest_observation_date or '-'}"
        )
    for source, rows in sorted(common_by_source.items()):
        preview = ", ".join(rows[:8])
        suffix = "" if len(rows) <= 8 else f", ... (+{len(rows) - 8})"
        print(f"     {source}: {preview}{suffix}")

    print("   - DART raw year ranges:")
    for row in report.dart_year_ranges:
        year_range = f"{row.min_year}..{row.max_year}" if row.min_year is not None else "-"
        print(f"     {row.table_name}: years={year_range} rows={row.rows}")

    print("   - running ingestion runs:")
    if not report.running_runs:
        print("     -")
    for run in report.running_runs:
        started_at = run.started_at.isoformat() if run.started_at else "-"
        print(f"     {run.run_id} {run.run_type.value} started_at={started_at}")

    if not args.fail_if_stale:
        return

    from collector.kr.service.freshness import evaluate_staleness

    findings = evaluate_staleness(
        report,
        max_lag_trading_days=args.max_lag_trading_days,
        max_market_cap_lag_trading_days=args.max_market_cap_lag_trading_days,
        max_lag_calendar_days=args.max_lag_calendar_days,
    )
    if not findings:
        print("   - staleness gate: OK")
        return

    print(f"❌ {len(findings)} domain(s) behind their freshness budget:", file=sys.stderr)
    for finding in findings:
        print(f"   - {finding.describe()}", file=sys.stderr)
    sys.exit(1)


def _dart_financial_actual_attempt_estimate(
    *,
    storage: object,
    targets: list[object],
    allowed_pairs: set[tuple[int, str]],
    fs_divs: list[str],
    force: bool,
    skip_request_keys: set[str],
) -> int:
    if force:
        existing_keys: set[tuple[str, int, str, str]] = set()
        effective_skip_keys: set[str] = set()
    else:
        existing_keys = storage.get_existing_dart_financial_statement_keys(
            bsns_years=sorted({year for year, _ in allowed_pairs}),
            reprt_codes=sorted({reprt_code for _, reprt_code in allowed_pairs}),
            fs_divs=fs_divs,
            corp_codes=[corp.corp_code for corp in targets],
        )
        effective_skip_keys = skip_request_keys

    attempts = 0
    for corp in targets:
        for bsns_year, reprt_code in allowed_pairs:
            for fs_div in fs_divs:
                request_key = f"{corp.ticker}:{bsns_year}:{reprt_code}:{fs_div}"
                if (corp.corp_code, bsns_year, reprt_code, fs_div) in existing_keys:
                    continue
                if request_key in effective_skip_keys:
                    continue
                attempts += 1
    return attempts


def _dart_share_info_actual_attempt_estimate(
    *,
    storage: object,
    targets: list[object],
    allowed_pairs: set[tuple[int, str]],
    force: bool,
    skip_request_keys: set[str],
) -> int:
    if force:
        existing_share_count_keys: set[tuple[str, int, str]] = set()
        existing_return_keys: set[tuple[str, int, str, str]] = set()
        existing_capital_change_keys: set[tuple[str, int, str]] = set()
        effective_skip_keys: set[str] = set()
    else:
        bsns_years = sorted({year for year, _ in allowed_pairs})
        reprt_codes = sorted({reprt_code for _, reprt_code in allowed_pairs})
        corp_codes = [corp.corp_code for corp in targets]
        existing_share_count_keys = storage.get_existing_dart_share_count_keys(
            bsns_years=bsns_years,
            reprt_codes=reprt_codes,
            corp_codes=corp_codes,
        )
        existing_return_keys = storage.get_existing_dart_shareholder_return_keys(
            bsns_years=bsns_years,
            reprt_codes=reprt_codes,
            corp_codes=corp_codes,
        )
        existing_capital_change_keys = storage.get_existing_dart_capital_change_keys(
            bsns_years=bsns_years,
            reprt_codes=reprt_codes,
            corp_codes=corp_codes,
        )
        effective_skip_keys = skip_request_keys

    attempts = 0
    for corp in targets:
        for bsns_year, reprt_code in allowed_pairs:
            request_prefix = f"{corp.ticker}:{bsns_year}:{reprt_code}"
            if (
                (corp.corp_code, bsns_year, reprt_code) not in existing_share_count_keys
                and f"{request_prefix}:share_count" not in effective_skip_keys
            ):
                attempts += 1
            if (
                (corp.corp_code, bsns_year, reprt_code, "dividend") not in existing_return_keys
                and f"{request_prefix}:dividend" not in effective_skip_keys
            ):
                attempts += 1
            if (
                (corp.corp_code, bsns_year, reprt_code, "treasury_stock")
                not in existing_return_keys
                and f"{request_prefix}:treasury_stock" not in effective_skip_keys
            ):
                attempts += 1
            if (
                (corp.corp_code, bsns_year, reprt_code) not in existing_capital_change_keys
                and f"{request_prefix}:capital_change" not in effective_skip_keys
            ):
                attempts += 1
    return attempts


def _dart_xbrl_actual_attempt_estimate(
    *,
    storage: object,
    allowed_pairs: set[tuple[int, str]],
    tickers: list[str] | None,
    force: bool,
    skip_request_keys: set[str],
) -> int:
    bsns_years = sorted({year for year, _ in allowed_pairs})
    reprt_codes = sorted({reprt_code for _, reprt_code in allowed_pairs})
    corp_rows = storage.get_dart_corp_master(active_only=True, tickers=tickers)
    corp_by_ticker = {corp.ticker: corp for corp in corp_rows if corp.ticker}
    financial_rows = storage.get_dart_financial_statement_raw(bsns_years, reprt_codes, tickers)
    request_targets: set[tuple[str, int, str, str]] = set()
    for row in financial_rows:
        if row.ticker and row.rcept_no and (row.bsns_year, row.reprt_code) in allowed_pairs:
            request_targets.add((row.ticker, row.bsns_year, row.reprt_code, row.rcept_no))

    if force:
        existing_doc_keys: set[tuple[str, int, str, str]] = set()
        effective_skip_keys: set[str] = set()
    else:
        existing_doc_keys = storage.get_existing_dart_xbrl_document_keys(
            bsns_years=bsns_years,
            reprt_codes=reprt_codes,
            corp_codes=[corp.corp_code for corp in corp_by_ticker.values()],
        )
        effective_skip_keys = skip_request_keys

    attempts = 0
    for ticker, bsns_year, reprt_code, rcept_no in request_targets:
        corp = corp_by_ticker.get(ticker)
        if corp is None:
            continue
        request_key = f"{ticker}:{bsns_year}:{reprt_code}:{rcept_no}"
        if (corp.corp_code, bsns_year, reprt_code, rcept_no) in existing_doc_keys:
            continue
        if request_key in effective_skip_keys:
            continue
        attempts += 1
    return attempts


def _handle_dart_sync_corp(args: argparse.Namespace) -> None:
    """Handle ``collector dart sync-corp``."""
    settings = get_settings()

    print("→ dart sync-corp: downloading OpenDART corp master and validating ticker mappings")

    from collector.kr.adapters.opendart_corp.provider import OpenDartCorpCodeProvider
    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.sync_dart_corp import sync_dart_corp_master

    try:
        request_executor = _build_opendart_request_executor()
    except Exception as exc:
        print(f"❌ OpenDART corp sync failed: {exc}", file=sys.stderr)
        sys.exit(1)

    provider = OpenDartCorpCodeProvider(request_executor=request_executor)
    storage = PostgresStorage(settings.db_dsn)
    result = sync_dart_corp_master(provider=provider, storage=storage, force=args.force)

    if result.error:
        print(f"❌ OpenDART corp sync failed: {result.error}", file=sys.stderr)
        sys.exit(1)

    print("✅ OpenDART corp sync completed.")
    print(f"   - Total records fetched: {result.total_records}")
    print(f"   - Active tickers matched: {result.matched_active_tickers}")
    print(f"   - Active tickers unmatched: {len(result.unmatched_active_tickers)}")
    print(f"   - DART listed tickers missing in stock_master: {len(result.unmatched_dart_tickers)}")

    if result.unmatched_active_tickers:
        preview = ", ".join(result.unmatched_active_tickers[:10])
        print(f"   - Sample unmatched active tickers: {preview}")


def _handle_dart_sync_corp_profile(args: argparse.Namespace) -> None:
    """Handle ``collector dart sync-corp-profile``."""
    settings = get_settings()
    tickers = _split_csv(args.tickers)

    print(
        f"→ dart sync-corp-profile: tickers={tickers}, "
        f"rate_limit={args.rate_limit_seconds}, force={args.force}"
    )

    from collector.kr.adapters.opendart_corp.provider import OpenDartCorpCodeProvider
    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.sync_dart_corp_profile import sync_dart_corp_profile

    try:
        request_executor = _build_opendart_request_executor()
    except Exception as exc:
        print(f"❌ OpenDART corp profile sync failed: {exc}", file=sys.stderr)
        sys.exit(1)

    result = sync_dart_corp_profile(
        profile_provider=OpenDartCorpCodeProvider(request_executor=request_executor),
        storage=PostgresStorage(settings.db_dsn),
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
        scope=UniverseScope(args.universe_scope),
    )

    _exit_if_opendart_key_exhausted(result, "OpenDART corp profile sync")

    if result.errors:
        print(
            f"⚠ OpenDART corp profile sync completed with {len(result.errors)} errors.",
            file=sys.stderr,
        )
    else:
        print("✅ OpenDART corp profile sync completed.")

    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped:   {result.requests_skipped}")
    print(f"   - Rows upserted:      {result.rows_upserted}")
    print(f"   - No data:            {result.no_data}")
    print(f"   - History appended:   {result.history_rows_appended}")


def _handle_dart_seed_corp_profile_history(args: argparse.Namespace) -> None:
    """Handle ``collector dart seed-corp-profile-history``."""
    settings = get_settings()
    observed_month = (
        date.fromisoformat(args.observed_month).replace(day=1) if args.observed_month else None
    )

    print(f"→ dart seed-corp-profile-history: observed_month={observed_month or 'current month'}")

    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.sync_dart_corp_profile import seed_dart_corp_profile_history

    result = seed_dart_corp_profile_history(
        storage=PostgresStorage(settings.db_dsn),
        observed_month=observed_month,
    )

    if result.errors:
        print("❌ Corp profile history seed failed.", file=sys.stderr)
        for label, message in result.errors.items():
            print(f"   - {label}: {message}", file=sys.stderr)
        sys.exit(1)

    print("✅ Corp profile history seed completed.")
    print(f"   - Observed month:     {result.observed_month}")
    print(f"   - Rows inserted:      {result.rows_inserted}")
    if result.rows_inserted == 0:
        print("   (0 rows = that month was already seeded; the command is idempotent)")


def _handle_dart_sync_financials(args: argparse.Namespace) -> None:
    """Handle ``collector dart sync-financials``."""
    settings = get_settings()
    bsns_years = [int(value.strip()) for value in args.bsns_years.split(",") if value.strip()]
    reprt_codes = [value.strip() for value in args.reprt_codes.split(",") if value.strip()]
    fs_divs = [value.strip().upper() for value in args.fs_divs.split(",") if value.strip()]
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None
    if args.incremental and args.reprt_codes == "11011":
        reprt_codes = ["11011", "11012", "11013", "11014"]

    print(
        f"→ dart sync-financials: years={bsns_years}, reprt_codes={reprt_codes}, "
        f"fs_divs={fs_divs}, tickers={tickers}, rate_limit={args.rate_limit_seconds}"
    )

    from collector.kr.adapters.opendart_financials.provider import (
        OpenDartFinancialStatementProvider,
    )
    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.sync_dart_financials import sync_dart_financial_statements

    try:
        request_executor = _build_opendart_request_executor()
    except Exception as exc:
        print(f"❌ OpenDART financial sync failed: {exc}", file=sys.stderr)
        sys.exit(1)

    provider = OpenDartFinancialStatementProvider(request_executor=request_executor)
    storage = PostgresStorage(settings.db_dsn)
    allowed_year_report_pairs = None
    skip_request_keys = None
    run_params_extra = None
    if args.incremental:
        from collector.kr.domain.enums import RunStatus, RunType
        from collector.kr.service.dart_target_plan import build_dart_target_plan
        from collector.kr.util.pipeline import record_terminal_run

        active_targets = storage.get_dart_corp_master(active_only=True, tickers=tickers)
        active_count = len(active_targets)
        plan = build_dart_target_plan(
            storage,
            run_type=RunType.DART_FINANCIAL_SYNC,
            active_corp_count=active_count,
            requests_per_corp_target=len(fs_divs),
            lookback_years=args.lookback_years,
            reprt_codes=reprt_codes,
            negative_cache_ttl_days=args.negative_cache_ttl_days,
        )
        if not plan.allowed_year_report_pairs:
            record_terminal_run(
                storage,
                run_type=RunType.DART_FINANCIAL_SYNC,
                status=RunStatus.SUCCESS,
                params={**plan.audit_params(), "no_work": True},
                counts={"requests_attempted": 0, "requests_skipped": 0},
            )
            print("✅ OpenDART financial sync skipped: no available incremental targets.")
            return
        actual_attempt_estimate = _dart_financial_actual_attempt_estimate(
            storage=storage,
            targets=active_targets,
            allowed_pairs=plan.allowed_year_report_pairs,
            fs_divs=fs_divs,
            force=args.force,
            skip_request_keys=plan.negative_cache_request_keys,
        )
        if actual_attempt_estimate == 0:
            audit_params = {
                **plan.audit_params(),
                "prefilter_estimated_request_count": plan.estimated_request_count,
                "estimated_request_count": actual_attempt_estimate,
                "no_work": True,
            }
            record_terminal_run(
                storage,
                run_type=RunType.DART_FINANCIAL_SYNC,
                status=RunStatus.SUCCESS,
                params=audit_params,
                counts={"requests_attempted": 0, "requests_skipped": 0},
            )
            print("✅ OpenDART financial sync skipped: no incremental request candidates.")
            return
        if actual_attempt_estimate > args.max_attempt_targets:
            audit_params = {
                **plan.audit_params(),
                "prefilter_estimated_request_count": plan.estimated_request_count,
                "estimated_request_count": actual_attempt_estimate,
                "max_attempt_targets": args.max_attempt_targets,
            }
            record_terminal_run(
                storage,
                run_type=RunType.DART_FINANCIAL_SYNC,
                status=RunStatus.FAILED,
                params=audit_params,
                counts={"estimated_request_count": actual_attempt_estimate},
                error_summary=(
                    f"Estimated OpenDART requests exceed guard "
                    f"({actual_attempt_estimate} > {args.max_attempt_targets})."
                ),
            )
            print(
                "❌ OpenDART financial sync failed: estimated requests exceed guard "
                f"({actual_attempt_estimate} > {args.max_attempt_targets}).",
                file=sys.stderr,
            )
            sys.exit(1)
        bsns_years = plan.bsns_years
        reprt_codes = plan.reprt_codes
        allowed_year_report_pairs = plan.allowed_year_report_pairs
        skip_request_keys = set() if args.force else plan.negative_cache_request_keys
        run_params_extra = {
            **plan.audit_params(),
            "prefilter_estimated_request_count": plan.estimated_request_count,
            "estimated_request_count": actual_attempt_estimate,
            "force_bypasses_negative_cache": args.force,
        }
    result = sync_dart_financial_statements(
        provider=provider,
        storage=storage,
        bsns_years=bsns_years,
        reprt_codes=reprt_codes,
        fs_divs=fs_divs,
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
        allowed_year_report_pairs=allowed_year_report_pairs,
        skip_request_keys=skip_request_keys,
        run_params_extra=run_params_extra,
        scope=UniverseScope(args.universe_scope),
    )

    if result.errors:
        print(f"⚠ Financial sync completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ OpenDART financial sync completed.")

    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Rows upserted: {result.rows_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")
    _exit_if_opendart_key_exhausted(result, "OpenDART financial sync")


def _handle_dart_sync_share_info(args: argparse.Namespace) -> None:
    """Handle ``collector dart sync-share-info``."""
    settings = get_settings()
    bsns_years = [int(value.strip()) for value in args.bsns_years.split(",") if value.strip()]
    reprt_codes = [value.strip() for value in args.reprt_codes.split(",") if value.strip()]
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None
    if args.incremental and args.reprt_codes == "11011":
        reprt_codes = ["11011", "11012", "11013", "11014"]

    print(
        f"→ dart sync-share-info: years={bsns_years}, reprt_codes={reprt_codes}, "
        f"tickers={tickers}, rate_limit={args.rate_limit_seconds}"
    )

    from collector.kr.adapters.opendart_share_info.provider import OpenDartShareInfoProvider
    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.sync_dart_share_info import sync_dart_share_info

    try:
        request_executor = _build_opendart_request_executor()
    except Exception as exc:
        print(f"❌ OpenDART share info sync failed: {exc}", file=sys.stderr)
        sys.exit(1)

    provider = OpenDartShareInfoProvider(request_executor=request_executor)
    storage = PostgresStorage(settings.db_dsn)
    allowed_year_report_pairs = None
    skip_request_keys = None
    run_params_extra = None
    if args.incremental:
        from collector.kr.domain.enums import RunStatus, RunType
        from collector.kr.service.dart_target_plan import build_dart_target_plan
        from collector.kr.util.pipeline import record_terminal_run

        active_targets = storage.get_dart_corp_master(active_only=True, tickers=tickers)
        active_count = len(active_targets)
        plan = build_dart_target_plan(
            storage,
            run_type=RunType.DART_SHARE_INFO_SYNC,
            active_corp_count=active_count,
            requests_per_corp_target=4,
            lookback_years=args.lookback_years,
            reprt_codes=reprt_codes,
            negative_cache_ttl_days=args.negative_cache_ttl_days,
        )
        if not plan.allowed_year_report_pairs:
            record_terminal_run(
                storage,
                run_type=RunType.DART_SHARE_INFO_SYNC,
                status=RunStatus.SUCCESS,
                params={**plan.audit_params(), "no_work": True},
                counts={"requests_attempted": 0, "requests_skipped": 0},
            )
            print("✅ OpenDART share info sync skipped: no available incremental targets.")
            return
        actual_attempt_estimate = _dart_share_info_actual_attempt_estimate(
            storage=storage,
            targets=active_targets,
            allowed_pairs=plan.allowed_year_report_pairs,
            force=args.force,
            skip_request_keys=plan.negative_cache_request_keys,
        )
        if actual_attempt_estimate == 0:
            audit_params = {
                **plan.audit_params(),
                "prefilter_estimated_request_count": plan.estimated_request_count,
                "estimated_request_count": actual_attempt_estimate,
                "no_work": True,
            }
            record_terminal_run(
                storage,
                run_type=RunType.DART_SHARE_INFO_SYNC,
                status=RunStatus.SUCCESS,
                params=audit_params,
                counts={"requests_attempted": 0, "requests_skipped": 0},
            )
            print("✅ OpenDART share info sync skipped: no incremental request candidates.")
            return
        if actual_attempt_estimate > args.max_attempt_targets:
            audit_params = {
                **plan.audit_params(),
                "prefilter_estimated_request_count": plan.estimated_request_count,
                "estimated_request_count": actual_attempt_estimate,
                "max_attempt_targets": args.max_attempt_targets,
            }
            record_terminal_run(
                storage,
                run_type=RunType.DART_SHARE_INFO_SYNC,
                status=RunStatus.FAILED,
                params=audit_params,
                counts={"estimated_request_count": actual_attempt_estimate},
                error_summary=(
                    f"Estimated OpenDART requests exceed guard "
                    f"({actual_attempt_estimate} > {args.max_attempt_targets})."
                ),
            )
            print(
                "❌ OpenDART share info sync failed: estimated requests exceed guard "
                f"({actual_attempt_estimate} > {args.max_attempt_targets}).",
                file=sys.stderr,
            )
            sys.exit(1)
        bsns_years = plan.bsns_years
        reprt_codes = plan.reprt_codes
        allowed_year_report_pairs = plan.allowed_year_report_pairs
        skip_request_keys = set() if args.force else plan.negative_cache_request_keys
        run_params_extra = {
            **plan.audit_params(),
            "prefilter_estimated_request_count": plan.estimated_request_count,
            "estimated_request_count": actual_attempt_estimate,
            "force_bypasses_negative_cache": args.force,
        }
    result = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        capital_change_provider=provider,
        storage=storage,
        bsns_years=bsns_years,
        reprt_codes=reprt_codes,
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
        allowed_year_report_pairs=allowed_year_report_pairs,
        skip_request_keys=skip_request_keys,
        run_params_extra=run_params_extra,
        scope=UniverseScope(args.universe_scope),
    )

    if result.errors:
        print(f"⚠ Share info sync completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ OpenDART share info sync completed.")

    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Share count rows upserted: {result.share_count_rows_upserted}")
    print(f"   - Shareholder return rows upserted: {result.shareholder_return_rows_upserted}")
    print(f"   - Capital change rows upserted: {result.capital_change_rows_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")
    _exit_if_opendart_key_exhausted(result, "OpenDART share info sync")


def _handle_dart_sync_periodic_extras(args: argparse.Namespace) -> None:
    """Handle ``collector dart sync-periodic-extras``."""
    settings = get_settings()
    bsns_years = list(range(args.start_year, args.end_year + 1))
    reprt_codes = [value.strip() for value in args.reprt_codes.split(",") if value.strip()]
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None

    from collector.kr.domain.enums import PeriodicExtraStatement
    from collector.kr.service.sync_dart_periodic_extras import (
        DEFAULT_STATEMENTS,
        resolve_target_years,
        sync_dart_periodic_extras,
    )

    if args.statements:
        try:
            statements = [
                PeriodicExtraStatement(value.strip())
                for value in args.statements.split(",")
                if value.strip()
            ]
        except ValueError as exc:
            print(f"❌ Unknown statement type: {exc}", file=sys.stderr)
            sys.exit(1)
    else:
        statements = list(DEFAULT_STATEMENTS)

    print(
        f"→ dart sync-periodic-extras: years={args.start_year}..{args.end_year}, "
        f"statements={[s.value for s in statements]}, reprt_codes={reprt_codes}, "
        f"tickers={tickers}, scope={args.universe_scope}, force={args.force}"
    )
    # The year thinning is the difference between 162,000 calls and 83,700, so
    # print it rather than leaving the operator to infer it from the totals.
    for statement in statements:
        years = resolve_target_years(statement, bsns_years)
        print(f"   - {statement.value}: {len(years)} of {len(bsns_years)} years -> {years}")

    from collector.kr.adapters.opendart_periodic_extras.provider import (
        OpenDartPeriodicExtrasProvider,
    )
    from collector.kr.infra.db_postgres.repositories import PostgresStorage

    try:
        request_executor = _build_opendart_request_executor()
    except Exception as exc:
        print(f"❌ OpenDART periodic extras sync failed: {exc}", file=sys.stderr)
        sys.exit(1)

    provider = OpenDartPeriodicExtrasProvider(request_executor=request_executor)
    storage = PostgresStorage(settings.db_dsn)

    result = sync_dart_periodic_extras(
        provider=provider,
        storage=storage,
        bsns_years=bsns_years,
        reprt_codes=reprt_codes,
        statements=statements,
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
        scope=UniverseScope(args.universe_scope),
    )

    if result.errors:
        print(
            f"⚠ Periodic extras sync completed with {len(result.errors)} errors.",
            file=sys.stderr,
        )
    else:
        print("✅ OpenDART periodic extras sync completed.")

    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Rows upserted: {result.rows_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    print(
        "   - Slices: "
        f"pending={result.slices_pending} "
        f"complete={result.slices_skipped_complete} "
        f"no_data={result.slices_skipped_no_data} "
        f"retrying={result.slices_retrying}"
    )
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")
    _exit_if_opendart_key_exhausted(result, "OpenDART periodic extras sync")


def _handle_dart_sync_xbrl(args: argparse.Namespace) -> None:
    """Handle ``collector dart sync-xbrl``."""
    settings = get_settings()
    bsns_years = [int(value.strip()) for value in args.bsns_years.split(",") if value.strip()]
    reprt_codes = [value.strip() for value in args.reprt_codes.split(",") if value.strip()]
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None
    if args.incremental and args.reprt_codes == "11011":
        reprt_codes = ["11011", "11012", "11013", "11014"]

    print(
        f"→ dart sync-xbrl: years={bsns_years}, reprt_codes={reprt_codes}, "
        f"tickers={tickers}, rate_limit={args.rate_limit_seconds}"
    )

    from collector.kr.adapters.opendart_xbrl.provider import OpenDartXbrlProvider
    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.sync_dart_xbrl import sync_dart_xbrl

    try:
        request_executor = _build_opendart_request_executor()
    except Exception as exc:
        print(f"❌ OpenDART XBRL sync failed: {exc}", file=sys.stderr)
        sys.exit(1)

    provider = OpenDartXbrlProvider(request_executor=request_executor)
    storage = PostgresStorage(settings.db_dsn)
    allowed_year_report_pairs = None
    skip_request_keys = None
    run_params_extra = None
    if args.incremental:
        from collector.kr.domain.enums import RunStatus, RunType
        from collector.kr.service.dart_target_plan import build_dart_target_plan
        from collector.kr.util.pipeline import record_terminal_run

        active_count = len(storage.get_dart_corp_master(active_only=True, tickers=tickers))
        plan = build_dart_target_plan(
            storage,
            run_type=RunType.XBRL_PARSE,
            active_corp_count=active_count,
            requests_per_corp_target=1,
            lookback_years=args.lookback_years,
            reprt_codes=reprt_codes,
            negative_cache_ttl_days=args.negative_cache_ttl_days,
        )
        if not plan.allowed_year_report_pairs:
            record_terminal_run(
                storage,
                run_type=RunType.XBRL_PARSE,
                status=RunStatus.SUCCESS,
                params={**plan.audit_params(), "no_work": True},
                counts={"requests_attempted": 0, "requests_skipped": 0},
            )
            print("✅ OpenDART XBRL sync skipped: no available incremental targets.")
            return
        actual_attempt_estimate = _dart_xbrl_actual_attempt_estimate(
            storage=storage,
            allowed_pairs=plan.allowed_year_report_pairs,
            tickers=tickers,
            force=args.force,
            skip_request_keys=plan.negative_cache_request_keys,
        )
        if actual_attempt_estimate == 0:
            audit_params = {
                **plan.audit_params(),
                "prefilter_estimated_request_count": plan.estimated_request_count,
                "estimated_request_count": actual_attempt_estimate,
                "no_work": True,
            }
            record_terminal_run(
                storage,
                run_type=RunType.XBRL_PARSE,
                status=RunStatus.SUCCESS,
                params=audit_params,
                counts={"requests_attempted": 0, "requests_skipped": 0},
            )
            print("✅ OpenDART XBRL sync skipped: no incremental request candidates.")
            return
        if actual_attempt_estimate > args.max_attempt_targets:
            audit_params = {
                **plan.audit_params(),
                "prefilter_estimated_request_count": plan.estimated_request_count,
                "estimated_request_count": actual_attempt_estimate,
                "max_attempt_targets": args.max_attempt_targets,
            }
            record_terminal_run(
                storage,
                run_type=RunType.XBRL_PARSE,
                status=RunStatus.FAILED,
                params=audit_params,
                counts={"estimated_request_count": actual_attempt_estimate},
                error_summary=(
                    f"Estimated OpenDART requests exceed guard "
                    f"({actual_attempt_estimate} > {args.max_attempt_targets})."
                ),
            )
            print(
                "❌ OpenDART XBRL sync failed: estimated requests exceed guard "
                f"({actual_attempt_estimate} > {args.max_attempt_targets}).",
                file=sys.stderr,
            )
            sys.exit(1)
        bsns_years = plan.bsns_years
        reprt_codes = plan.reprt_codes
        allowed_year_report_pairs = plan.allowed_year_report_pairs
        skip_request_keys = set() if args.force else plan.negative_cache_request_keys
        run_params_extra = {
            **plan.audit_params(),
            "prefilter_estimated_request_count": plan.estimated_request_count,
            "estimated_request_count": actual_attempt_estimate,
            "force_bypasses_negative_cache": args.force,
        }
    result = sync_dart_xbrl(
        provider=provider,
        storage=storage,
        bsns_years=bsns_years,
        reprt_codes=reprt_codes,
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
        allowed_year_report_pairs=allowed_year_report_pairs,
        skip_request_keys=skip_request_keys,
        run_params_extra=run_params_extra,
        scope=UniverseScope(args.universe_scope),
    )

    if result.errors:
        print(f"⚠ XBRL sync completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ OpenDART XBRL sync completed.")

    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Documents upserted: {result.documents_upserted}")
    print(f"   - Facts upserted: {result.facts_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")
    _exit_if_opendart_key_exhausted(result, "OpenDART XBRL sync")


def _handle_dart_sync_filings(args: argparse.Namespace) -> None:
    """Handle ``collector dart sync-filings``."""
    settings = get_settings()
    years = [int(value.strip()) for value in args.years.split(",") if value.strip()]
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None

    print(
        f"→ dart sync-filings: years={years}, tickers={tickers}, "
        f"rate_limit={args.rate_limit_seconds}, lookback_days={args.lookback_days}"
    )

    from collector.kr.adapters.opendart_filings.provider import OpenDartFilingReceiptProvider
    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.sync_dart_filings import sync_dart_filings

    try:
        request_executor = _build_opendart_request_executor()
    except Exception as exc:
        print(f"❌ OpenDART filing receipt sync failed: {exc}", file=sys.stderr)
        sys.exit(1)

    provider = OpenDartFilingReceiptProvider(
        request_executor=request_executor,
        page_delay_seconds=args.rate_limit_seconds,
    )
    storage = PostgresStorage(settings.db_dsn)
    result = sync_dart_filings(
        filing_receipt_provider=provider,
        storage=storage,
        years=years,
        tickers=tickers,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
        scope=UniverseScope(args.universe_scope),
        lookback_days=args.lookback_days,
    )

    if result.errors:
        print(
            f"⚠ Filing receipt sync completed with {len(result.errors)} errors.",
            file=sys.stderr,
        )
    else:
        print("✅ OpenDART filing receipt sync completed.")

    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Rows upserted: {result.rows_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")
    _exit_if_opendart_key_exhausted(result, "OpenDART filing receipt sync")


def _handle_dart_backfill_xbrl_receipts(args: argparse.Namespace) -> None:
    """Handle ``collector dart backfill-xbrl-receipts``.

    Reads explicit targets from a JSON-lines file — each line an object with
    ``ticker``, ``corp_code``, ``bsns_year``, ``reprt_code``, ``rcept_no`` —
    and fetches XBRL for exactly those receipts. Deciding *which* receipts
    need backfilling (e.g. an original filing not yet captured) is a
    downstream analysis over ``dart_filing_receipt_raw`` and is not done
    here; this command only performs the fetch once targets are known.
    """
    import json

    settings = get_settings()
    targets_path = Path(args.targets_file)
    targets: list[object] = []

    from collector.kr.domain.models import XbrlBackfillTarget

    with targets_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            targets.append(
                XbrlBackfillTarget(
                    ticker=str(row["ticker"]),
                    corp_code=str(row["corp_code"]),
                    bsns_year=int(row["bsns_year"]),
                    reprt_code=str(row["reprt_code"]),
                    rcept_no=str(row["rcept_no"]),
                )
            )

    print(f"→ dart backfill-xbrl-receipts: targets={len(targets)} file={targets_path}")

    from collector.kr.adapters.opendart_xbrl.provider import OpenDartXbrlProvider
    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.sync_dart_xbrl import sync_dart_xbrl_receipt_targeted

    try:
        request_executor = _build_opendart_request_executor()
    except Exception as exc:
        print(f"❌ OpenDART XBRL receipt backfill failed: {exc}", file=sys.stderr)
        sys.exit(1)

    provider = OpenDartXbrlProvider(request_executor=request_executor)
    storage = PostgresStorage(settings.db_dsn)
    result = sync_dart_xbrl_receipt_targeted(
        provider=provider,
        storage=storage,
        targets=targets,
        rate_limit_seconds=args.rate_limit_seconds,
        force=args.force,
    )

    if result.errors:
        print(
            f"⚠ XBRL receipt backfill completed with {len(result.errors)} errors.",
            file=sys.stderr,
        )
    else:
        print("✅ OpenDART XBRL receipt backfill completed.")

    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Documents upserted: {result.documents_upserted}")
    print(f"   - Facts upserted: {result.facts_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")
    _exit_if_opendart_key_exhausted(result, "OpenDART XBRL receipt backfill")


def _handle_common_seed_catalog(args: argparse.Namespace) -> None:
    """Handle ``collector common seed-catalog``."""
    settings = get_settings()

    print(f"→ common seed-catalog: init_schema={args.init_schema}")

    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.default_common_feature_catalog import seed_common_feature_catalog

    storage = PostgresStorage(settings.db_dsn)
    if args.init_schema:
        storage.init_schema()

    result = seed_common_feature_catalog(storage)

    print("✅ Common feature catalog seed completed.")
    print(f"   - Series upserted: {result.series_upsert.updated}")
    print(f"   - Feature catalog upserted: {result.catalog_upsert.updated}")


def _handle_common_sync(args: argparse.Namespace) -> None:
    """Handle ``collector common sync``."""
    settings = get_settings()
    sources = args.sources
    series_ids = _split_csv(args.series)
    include_inactive = bool(args.include_inactive)
    if include_inactive and not series_ids:
        raise SystemExit("--include-inactive requires an explicit --series allowlist.")
    providers = _build_common_feature_providers(sources)

    print(
        f"→ common sync: sources={[source.value for source in sources]}, "
        f"series={series_ids}, start={args.start}, end={args.end}, "
        f"force={args.force}, rate_limit={args.rate_limit_seconds}, "
        f"include_inactive={include_inactive}, init_schema={args.init_schema}, "
        f"incremental={args.incremental}, lookback_days={args.lookback_days}, "
        f"max_auto_range_days={args.max_auto_range_days}, "
        f"allow_large_range={args.allow_large_range}"
    )

    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.sync_common_features import sync_common_features

    storage = PostgresStorage(settings.db_dsn)
    if args.init_schema:
        storage.init_schema()

    result = sync_common_features(
        providers=providers,
        storage=storage,
        start=args.start,
        end=args.end,
        sources=sources,
        series_ids=series_ids,
        active_only=not include_inactive,
        force=args.force,
        rate_limit_seconds=args.rate_limit_seconds,
        incremental=args.incremental,
        lookback_days=args.lookback_days,
        max_auto_range_days=args.max_auto_range_days,
        allow_large_range=args.allow_large_range,
    )

    if result.errors:
        print(f"⚠ Common feature sync completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ Common feature sync completed.")

    print(f"   - Series processed: {result.series_processed}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Rows upserted: {result.rows_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")
        if args.incremental:
            sys.exit(1)


def _handle_flows_sync(args: argparse.Namespace) -> None:
    """Handle ``collector flows sync``."""
    settings = get_settings()
    tickers = [value.strip() for value in args.tickers.split(",")] if args.tickers else None
    default_flow_date = date.today() - timedelta(days=1)
    start = args.start or default_flow_date
    end = args.end or default_flow_date

    if args.incremental and (args.start is not None or args.end is not None):
        print(
            "❌ Flow sync failed: --incremental cannot be combined with --start/--end in v1.",
            file=sys.stderr,
        )
        sys.exit(2)
    if args.incremental and args.use_price_range:
        print(
            "❌ Flow sync failed: --incremental cannot be combined with --use-price-range.",
            file=sys.stderr,
        )
        sys.exit(2)

    timeout_seconds = (
        args.timeout_seconds
        if args.timeout_seconds is not None
        else settings.krx_mdc_timeout_seconds
    )
    rate_limit_seconds = (
        args.rate_limit_seconds
        if args.rate_limit_seconds is not None
        else settings.krx_logical_rate_limit_seconds
    )
    http_min_delay_seconds = (
        args.http_min_delay_seconds
        if args.http_min_delay_seconds is not None
        else settings.krx_min_delay_seconds
    )
    http_max_delay_seconds = (
        args.http_max_delay_seconds
        if args.http_max_delay_seconds is not None
        else settings.krx_max_delay_seconds
    )
    long_rest_every = (
        args.long_rest_every if args.long_rest_every is not None else settings.krx_long_rest_every
    )
    long_rest_min_seconds = (
        args.long_rest_min_seconds
        if args.long_rest_min_seconds is not None
        else settings.krx_long_rest_min_seconds
    )
    long_rest_max_seconds = (
        args.long_rest_max_seconds
        if args.long_rest_max_seconds is not None
        else settings.krx_long_rest_max_seconds
    )
    auth_cooldown_seconds = (
        args.auth_cooldown_seconds
        if args.auth_cooldown_seconds is not None
        else settings.krx_auth_cooldown_seconds
    )
    error_backoff_min_seconds = (
        args.error_backoff_min_seconds
        if args.error_backoff_min_seconds is not None
        else settings.krx_error_backoff_min_seconds
    )
    error_backoff_max_seconds = (
        args.error_backoff_max_seconds
        if args.error_backoff_max_seconds is not None
        else settings.krx_error_backoff_max_seconds
    )

    print(
        f"→ flows sync: start={start}, end={end}, "
        f"tickers={tickers}, logical_rate_limit={rate_limit_seconds}, "
        f"http_delay={http_min_delay_seconds}..{http_max_delay_seconds}s, "
        f"long_rest_every={long_rest_every}, "
        f"long_rest={long_rest_min_seconds}..{long_rest_max_seconds}s, "
        f"auth_cooldown={auth_cooldown_seconds}s, "
        f"error_backoff={error_backoff_min_seconds}..{error_backoff_max_seconds}s, "
        f"randomize_requests={not args.ordered_requests}, "
        f"timeout={timeout_seconds}, "
        f"progress_interval={args.progress_log_interval_seconds}, "
        f"progress_every={args.progress_log_every_items}, "
        f"incremental={args.incremental}, "
        f"lookback_days={args.lookback_days}, "
        f"max_auto_range_days={args.max_auto_range_days}, "
        f"exclude_groups={args.exclude_groups}"
    )

    from collector.kr.adapters.flows_krx.provider import KrxDirectFlowProvider
    from collector.kr.domain.enums import RunStatus, RunType, Source
    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.sync_krx_flows import (
        FLOW_METRIC_GROUPS,
        resolve_incremental_flow_range,
        sync_krx_security_flows,
    )
    from collector.kr.util.pipeline import HumanThrottle, HumanThrottlePolicy, record_terminal_run

    storage = PostgresStorage(settings.db_dsn)
    run_params_extra: dict[str, object] | None = None
    exclude_groups = _split_csv(args.exclude_groups) or []
    enabled_flow_groups = [
        group for group in sorted(FLOW_METRIC_GROUPS) if group not in set(exclude_groups)
    ]

    if args.incremental:
        metric_codes = sorted(
            {metric for metrics in FLOW_METRIC_GROUPS.values() for metric in metrics}
        )
        try:
            incremental_range = resolve_incremental_flow_range(
                latest_price_date=storage.get_latest_daily_price_date(tickers=tickers),
                metric_latest_dates=storage.get_krx_security_flow_metric_max_dates(
                    metric_codes=metric_codes,
                    # KIS rows are a separate collection path. They may satisfy
                    # logical freshness, but must not advance the KRX cursor.
                    sources=(Source.KRX,),
                ),
                lookback_days=args.lookback_days,
                exclude_groups=exclude_groups,
            )
        except ValueError as exc:
            print(f"❌ Flow sync failed: {exc}", file=sys.stderr)
            sys.exit(1)

        if (
            incremental_range.auto_range_days > args.max_auto_range_days
            and not args.allow_large_range
        ):
            print(
                "❌ Flow sync failed: resolved incremental range is too large "
                f"({incremental_range.auto_range_days} days > "
                f"{args.max_auto_range_days}). Use --allow-large-range to override.",
                file=sys.stderr,
            )
            sys.exit(1)

        start = incremental_range.start
        end = incremental_range.end
        print("   - Flows incremental range resolved:")
        print(f"     latest_price_date={incremental_range.latest_price_date}")
        for group, latest in sorted(incremental_range.group_latest_dates.items()):
            lag = incremental_range.group_lag_days[group]
            print(f"     latest_{group}_date={latest} lag_days={lag}")
        print(f"     excluded_groups={incremental_range.excluded_groups}")
        print(f"     start={start}")
        print(f"     end={end}")
        print(f"     lookback_days={incremental_range.lookback_days}")
        print(f"     auto_range_days={incremental_range.auto_range_days}")
        run_params_extra = incremental_range.as_run_params()
        run_params_extra["max_auto_range_days"] = args.max_auto_range_days
        run_params_extra["allow_large_range"] = args.allow_large_range

        if incremental_range.no_work:
            record_terminal_run(
                storage,
                run_type=RunType.KRX_FLOW_SYNC,
                status=RunStatus.SUCCESS,
                params={
                    **(run_params_extra or {}),
                    "tickers": tickers,
                    "no_work": True,
                    "skip_reason": "flow metrics are current",
                },
                counts={
                    "targets_processed": 0,
                    "requests_attempted": 0,
                    "requests_skipped": 0,
                    "rows_upserted": 0,
                    "no_data_requests": 0,
                },
            )
            print("✅ KRX flow sync skipped: no incremental work.")
            return

    if args.use_price_range:
        price_range = storage.get_daily_price_date_range(tickers=tickers)
        if price_range is None:
            print(
                "❌ Flow sync failed: no daily OHLCV rows found for price range.",
                file=sys.stderr,
            )
            sys.exit(1)
        price_start, price_end = price_range
        start = max(start, price_start) if args.start else price_start
        end = min(end, price_end) if args.end else price_end
        if start > end:
            print(
                f"❌ Flow sync failed: resolved price range is empty "
                f"(start={start}, end={end}).",
                file=sys.stderr,
            )
            sys.exit(1)
        price_range_days = (end - start).days + 1
        if price_range_days > args.max_price_range_days and not args.allow_large_range:
            print(
                "❌ Flow sync failed: resolved price range is too large "
                f"({price_range_days} days > {args.max_price_range_days}). "
                "Use --allow-large-range to override.",
                file=sys.stderr,
            )
            sys.exit(1)
        print(f"   - Price range resolved: start={start}, end={end}")

    if not args.incremental:
        resolved_range_days = (end - start).days + 1
        if resolved_range_days > args.max_price_range_days and not args.allow_large_range:
            print(
                "❌ Flow sync failed: resolved range is too large "
                f"({resolved_range_days} days > {args.max_price_range_days}). "
                "Use --allow-large-range to override.",
                file=sys.stderr,
            )
            sys.exit(1)

    human_throttle = HumanThrottle(
        HumanThrottlePolicy(
            min_delay_seconds=http_min_delay_seconds,
            max_delay_seconds=http_max_delay_seconds,
            long_rest_every=long_rest_every,
            long_rest_min_seconds=long_rest_min_seconds,
            long_rest_max_seconds=long_rest_max_seconds,
            auth_cooldown_seconds=auth_cooldown_seconds,
            error_backoff_min_seconds=error_backoff_min_seconds,
            error_backoff_max_seconds=error_backoff_max_seconds,
        ),
        logger_instance=logger,
    )

    provider = KrxDirectFlowProvider(
        timeout_seconds=timeout_seconds,
        login_id=settings.krx_id,
        login_pw=settings.krx_pw,
        human_throttle=human_throttle,
    )

    result = sync_krx_security_flows(
        provider=provider,
        storage=storage,
        start=start,
        end=end,
        tickers=tickers,
        rate_limit_seconds=rate_limit_seconds,
        progress_log_interval_seconds=args.progress_log_interval_seconds,
        progress_log_every_items=args.progress_log_every_items,
        randomize_request_order=not args.ordered_requests,
        run_params_extra=run_params_extra,
        enabled_flow_groups=enabled_flow_groups,
    )

    if result.errors:
        print(f"⚠ Flow sync completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ KRX flow sync completed.")

    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Source: {provider.source().value}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Rows upserted: {result.rows_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    phase_counts = getattr(result, "phase_counts", {})
    if phase_counts:
        print("   - Phase counts:")
        for phase, counts in sorted(phase_counts.items()):
            print(
                f"     {phase}: attempted={counts.requests_attempted}, "
                f"skipped={counts.requests_skipped}, rows={counts.rows_upserted}, "
                f"no_data={counts.no_data_requests}, errors={counts.error_count}"
            )
    if result.pending_metrics:
        print(f"   - Pending metrics: {', '.join(result.pending_metrics)}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")


def _handle_flows_sync_kis(args: argparse.Namespace) -> None:
    """Handle ``collector flows sync-kis``."""
    settings = get_settings()

    from collector.kr.adapters.flows_kis import KIS_FLOW_GROUPS, KisFlowProvider
    from collector.kr.adapters.kis_common import KisClient, KisTokenCache, KisTokenProvider
    from collector.kr.infra.calendar.trading_days import get_trading_days
    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.sync_kis_flows import (
        build_kis_flow_plan,
        load_kis_flow_targets,
        sync_kis_security_flows,
    )
    from collector.kr.util.rate_limit import TokenBucket
    from collector.kr.util.time import today_kst

    tickers = _split_csv(args.tickers)
    exclude_groups = set(_split_csv(args.exclude_groups) or [])
    unknown_groups = sorted(exclude_groups - set(KIS_FLOW_GROUPS))
    if unknown_groups:
        print(
            f"❌ KIS flow sync failed: unknown group(s) {', '.join(unknown_groups)} "
            f"(supported: {', '.join(KIS_FLOW_GROUPS)}).",
            file=sys.stderr,
        )
        sys.exit(2)
    enabled_groups = [group for group in KIS_FLOW_GROUPS if group not in exclude_groups]
    if not enabled_groups:
        print("❌ KIS flow sync failed: every metric group was excluded.", file=sys.stderr)
        sys.exit(2)

    scope = UniverseScope(args.universe_scope)
    today = today_kst()
    if args.end is not None:
        end = args.end
    else:
        recent_sessions = get_trading_days(today - timedelta(days=30), today)
        if not recent_sessions:
            print(
                "❌ KIS flow sync failed: no KRX trading day found in the last 30 days.",
                file=sys.stderr,
            )
            sys.exit(1)
        end = recent_sessions[-1]
    start = args.start if args.start is not None else end - timedelta(days=args.lookback_days)
    if start > end:
        print(
            f"❌ KIS flow sync failed: resolved range is empty (start={start}, end={end}).",
            file=sys.stderr,
        )
        sys.exit(2)

    requests_per_second = (
        args.requests_per_second
        if args.requests_per_second is not None
        else settings.kis_requests_per_second
    )
    timeout_seconds = (
        args.timeout_seconds if args.timeout_seconds is not None else settings.kis_timeout_seconds
    )
    token_cache_path = (
        Path(args.token_cache_path)
        if args.token_cache_path is not None
        else settings.kis_token_cache_path
    )

    print(
        f"→ flows sync-kis: start={start}, end={end}, "
        f"tickers={tickers}, groups={','.join(enabled_groups)}, "
        f"scope={scope.value}, requests_per_second={requests_per_second}, "
        f"timeout={timeout_seconds}, "
        f"max_consecutive_failures={args.max_consecutive_failures}, "
        f"no_data_ttl_days={args.no_data_ttl_days}, "
        f"token_cache={token_cache_path}, plan_only={args.plan_only}"
    )

    storage = PostgresStorage(settings.db_dsn)

    if args.plan_only:
        targets = load_kis_flow_targets(storage, tickers, scope)
        trading_days = get_trading_days(start, end)
        if not targets or not trading_days:
            print(
                "❌ KIS flow sync plan failed: no targets or no trading days in range.",
                file=sys.stderr,
            )
            sys.exit(1)
        plan = build_kis_flow_plan(
            storage=storage,
            targets=targets,
            trading_days=trading_days,
            enabled_groups=enabled_groups,
            no_data_ttl_days=args.no_data_ttl_days,
        )
        print("✅ KIS flow sync plan (no requests sent, no token issued).")
        print(f"   - Targets: {plan.targets}")
        print(f"   - Trading days: {len(trading_days)}")
        print(f"   - Planned requests: {len(plan.items)}")
        for group, count in sorted(plan.group_counts().items()):
            print(f"     {group}: {count}")
        print(f"   - Skipped (already current): {plan.skipped_current}")
        print(f"   - Skipped (no-data tombstone): {plan.skipped_no_data}")
        for group, reason in sorted(plan.skipped_groups.items()):
            print(f"   - Skipped group {group}: {reason}")
        return

    if not settings.kis_app_key or not settings.kis_app_secret:
        print(
            "❌ KIS flow sync failed: KIS_APP_KEY / KIS_APP_SECRET are not configured.",
            file=sys.stderr,
        )
        sys.exit(2)

    token_provider = KisTokenProvider(
        app_key=settings.kis_app_key,
        app_secret=settings.kis_app_secret,
        base_url=settings.kis_base_url,
        cache=KisTokenCache(
            token_cache_path,
            refresh_margin_seconds=settings.kis_token_refresh_margin_seconds,
        ),
        timeout_seconds=timeout_seconds,
    )
    client = KisClient(
        token_provider=token_provider,
        app_key=settings.kis_app_key,
        app_secret=settings.kis_app_secret,
        base_url=settings.kis_base_url,
        bucket=TokenBucket(requests_per_second, burst=settings.kis_max_burst_requests),
        timeout_seconds=timeout_seconds,
    )
    provider = KisFlowProvider(client=client)

    result = sync_kis_security_flows(
        provider,
        storage,
        start=start,
        end=end,
        tickers=tickers,
        enabled_flow_groups=enabled_groups,
        scope=scope,
        max_consecutive_failures=args.max_consecutive_failures,
        no_data_ttl_days=args.no_data_ttl_days,
    )

    if result.aborted_reason:
        print(
            f"❌ KIS flow sync aborted ({result.aborted_reason}): "
            f"{result.errors.get('pipeline', '')}",
            file=sys.stderr,
        )
    elif result.errors:
        print(f"⚠ KIS flow sync completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ KIS flow sync completed.")

    print(f"   - Source: {provider.source().value}")
    print(f"   - Targets processed: {result.targets_processed}")
    print(f"   - Requests attempted: {result.requests_attempted}")
    print(f"   - Requests skipped: {result.requests_skipped}")
    print(f"   - Rows upserted: {result.rows_upserted}")
    print(f"   - No-data requests: {result.no_data_requests}")
    if result.rows_outside_window:
        print(
            f"   - No session in window: {result.rows_outside_window} "
            "(KIS returned rows for these tickers, all outside the requested "
            "window — no trading in it, typically a halt; not tombstoned, "
            "retried next run)"
        )
    if result.phase_counts:
        print("   - Group counts:")
        for group, counts in sorted(result.phase_counts.items()):
            print(
                f"     {group}: attempted={counts.requests_attempted}, "
                f"rows={counts.rows_upserted}, no_data={counts.no_data_requests}, "
                f"errors={counts.error_count}"
            )
    if result.http_counts:
        print("   - Real HTTP:")
        for key, value in sorted(result.http_counts.items()):
            print(f"     {key}={value}")
    for group, reason in sorted(result.skipped_groups.items()):
        print(f"   - Skipped group {group}: {reason}")
    if result.pending_metrics:
        print(f"   - Pending metrics (KRX-only): {', '.join(result.pending_metrics)}")
    if result.errors:
        for request_key, error in list(result.errors.items())[:10]:
            print(f"   - Error {request_key}: {error}")

    if result.aborted_reason:
        sys.exit(75 if result.aborted_reason == "SourceQuotaExhaustedError" else 1)


def _handle_universe_sync(args: argparse.Namespace) -> None:
    """Handle ``collector universe sync``."""
    settings = get_settings()

    # 1. Parse arguments
    source_str = args.source or settings.universe_source_default
    source_str = source_str.upper()

    from collector.kr.domain.enums import Market

    markets = []
    for m in args.markets.split(","):
        m_upper = m.strip().upper()
        if m_upper == "KOSPI":
            markets.append(Market.KOSPI)
        elif m_upper == "KOSDAQ":
            markets.append(Market.KOSDAQ)
        else:
            print(f"❌ Unknown market: {m}", file=sys.stderr)
            sys.exit(1)

    print(
        f"→ universe sync: source={source_str}, markets={[m.value for m in markets]}, "
        f"as_of={args.as_of}, full_refresh={args.full_refresh}"
    )

    # 2. Instantiate dependencies
    from collector.kr.infra.db_postgres.repositories import PostgresStorage

    storage = PostgresStorage(settings.db_dsn)

    provider = None
    if source_str == "KRX-OPENAPI":
        from collector.kr.adapters.market_data_krx_openapi.provider import (
            KrxOpenApiUniverseProvider,
        )

        provider = KrxOpenApiUniverseProvider(_build_krx_openapi_client(settings))
    elif source_str == "FDR":
        from collector.kr.adapters.universe_fdr.provider import FdrUniverseProvider

        provider = FdrUniverseProvider()
    elif source_str == "PYKRX":
        from collector.kr.adapters.universe_pykrx.provider import PykrxUniverseProvider

        provider = PykrxUniverseProvider()
    else:
        print(f"❌ Unsupported universe source: {source_str}", file=sys.stderr)
        sys.exit(1)

    # 3. Execute use case
    from collector.kr.service.sync_universe import sync_universe

    result = sync_universe(
        provider=provider,
        storage=storage,
        markets=markets,
        as_of=args.as_of,
        full_refresh=args.full_refresh,
    )

    if result.error:
        print(f"❌ Universe sync failed: {result.error}", file=sys.stderr)
        sys.exit(1)

    print("✅ Universe sync completed.")
    print(f"   - Upserted: {result.upsert.updated} records")
    if result.new_tickers:
        print(f"   - New tickers: {len(result.new_tickers)}")
    if result.delisted_tickers:
        print(f"   - Delisted tickers: {len(result.delisted_tickers)}")


def _handle_universe_backfill_snapshots(args: argparse.Namespace) -> None:
    """Handle ``collector universe backfill-snapshots``."""
    settings = get_settings()

    from collector.kr.domain.enums import Market

    markets = []
    for m in args.markets.split(","):
        m_upper = m.strip().upper()
        if m_upper in ("KOSPI", "KOSDAQ"):
            markets.append(Market(m_upper))
        else:
            print(f"❌ Unknown market: {m}", file=sys.stderr)
            sys.exit(1)

    throttle = _build_krx_throttle(args)

    print(
        f"→ universe backfill-snapshots: source={args.source}, "
        f"markets={[m.value for m in markets]}, "
        f"start={args.start}, end={args.end}, force={args.force}, "
        f"min_delay={args.min_delay_seconds}, max_delay={args.max_delay_seconds}"
    )

    from collector.kr.domain.enums import Source
    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.backfill_universe_snapshots import (
        backfill_universe_snapshots,
    )

    if args.source == "pykrx":
        from collector.kr.adapters.universe_pykrx.provider import (
            PykrxHistoricalUniverseProvider,
        )

        snapshot_provider = PykrxHistoricalUniverseProvider()
        snapshot_source = Source.PYKRX_BACKFILL
    else:
        from collector.kr.adapters.market_data_krx_openapi import (
            KrxOpenApiHistoricalUniverseProvider,
        )

        snapshot_provider = KrxOpenApiHistoricalUniverseProvider(
            _build_krx_openapi_client(settings)
        )
        snapshot_source = Source.KRX_OPENAPI_BACKFILL
        throttle = None

    result = backfill_universe_snapshots(
        provider=snapshot_provider,
        storage=PostgresStorage(settings.db_dsn),
        markets=markets,
        start=args.start,
        end=args.end,
        throttle=throttle,
        force=args.force,
        max_consecutive_failures=args.max_consecutive_failures,
        snapshot_source=snapshot_source,
    )

    if result.errors:
        print(f"⚠ Snapshot backfill completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ Snapshot backfill completed successfully.")

    print(f"   - Snapshots attempted: {result.snapshots_attempted}")
    print(f"   - Snapshots skipped:   {result.snapshots_skipped}")
    print(f"   - Snapshots written:   {result.snapshots_written}")
    print(f"   - Items written:       {result.items_written}")

    _exit_if_run_aborted(result.errors, "Snapshot backfill")


def _handle_universe_backfill_master(args: argparse.Namespace) -> None:
    """Handle ``collector universe backfill-master``."""
    settings = get_settings()

    from collector.kr.domain.enums import Source

    sources = []
    for token in args.sources.split(","):
        name = token.strip().upper()
        if not name:
            continue
        try:
            sources.append(Source(name))
        except ValueError:
            print(f"\u274c Unknown snapshot source: {token}", file=sys.stderr)
            sys.exit(1)

    print(
        "\u2192 universe backfill-master: "
        f"sources={[s.value for s in sources]}, dry_run={args.dry_run}"
    )

    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.backfill_stock_master import (
        backfill_stock_master_from_snapshots,
    )

    result = backfill_stock_master_from_snapshots(
        storage=PostgresStorage(settings.db_dsn),
        sources=sources or None,
        dry_run=args.dry_run,
    )

    print(f"   - Snapshot-only securities: {result.candidates}")
    print(f"   - Rows upserted:            {result.rows_upserted}")
    if result.dry_run:
        print("   - Dry run: nothing was written.")

    _exit_if_run_aborted(result.errors, "Stock-master backfill")


def _handle_prices_backfill(args: argparse.Namespace) -> None:
    """Handle ``collector prices backfill``."""
    settings = get_settings()

    rate_limit = args.rate_limit_seconds
    if rate_limit is None:
        rate_limit = settings.rate_limit_seconds

    long_rest_interval = args.long_rest_interval
    if long_rest_interval is None:
        long_rest_interval = settings.long_rest_interval

    long_rest_seconds = args.long_rest_seconds
    if long_rest_seconds is None:
        long_rest_seconds = settings.long_rest_seconds

    print(
        f"→ prices backfill: source={args.source}, market={args.market}, tickers={args.tickers}, "
        f"start={args.start}, end={args.end}, "
        f"rate_limit={rate_limit}, "
        f"long_rest_interval={long_rest_interval}, "
        f"long_rest_seconds={long_rest_seconds}, "
        f"incremental={args.incremental}, "
        f"lookback_days={args.lookback_days}, "
        f"max_auto_range_days={args.max_auto_range_days}, "
        f"new_ticker_start={args.new_ticker_start}, "
        f"allow_new_ticker_backfill={args.allow_new_ticker_backfill}, "
        f"allow_large_range={args.allow_large_range}, "
        f"refetch={args.refetch}, scope={args.universe_scope}"
    )

    from collector.kr.domain.enums import Market

    market_filter = None
    if args.market and args.market.upper() != "ALL":
        market_str = args.market.upper()
        if market_str == "KOSPI":
            market_filter = Market.KOSPI
        elif market_str == "KOSDAQ":
            market_filter = Market.KOSDAQ
        else:
            print(f"❌ Unknown market: {args.market}", file=sys.stderr)
            sys.exit(1)

    tickers_list = None
    if args.tickers:
        tickers_list = [t.strip() for t in args.tickers.split(",")]

    if args.source == "pykrx":
        from collector.kr.adapters.prices_pykrx.provider import PykrxDailyPriceProvider

        provider = PykrxDailyPriceProvider()
    else:
        from collector.kr.adapters.prices_naver.provider import NaverDailyPriceProvider

        provider = NaverDailyPriceProvider()

    from collector.kr.infra.db_postgres.repositories import PostgresStorage

    storage = PostgresStorage(settings.db_dsn)

    from collector.kr.service.backfill_daily import backfill_daily_prices

    result = backfill_daily_prices(
        provider=provider,
        storage=storage,
        market=market_filter,
        tickers=tickers_list,
        start=args.start,
        end=args.end,
        rate_limit_seconds=rate_limit,
        long_rest_interval=long_rest_interval,
        long_rest_seconds=long_rest_seconds,
        incremental=args.incremental,
        lookback_days=args.lookback_days,
        max_auto_range_days=args.max_auto_range_days,
        new_ticker_start=args.new_ticker_start,
        allow_new_ticker_backfill=args.allow_new_ticker_backfill,
        allow_large_range=args.allow_large_range,
        refetch=args.refetch,
        scope=UniverseScope(args.universe_scope),
        max_consecutive_failures=args.max_consecutive_failures,
    )

    if result.errors:
        print(f"⚠ Backfill completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ Backfill completed successfully.")

    print(f"   - Tickers processed: {result.tickers_processed}")
    print(f"   - Bars upserted: {result.bars_upserted}")
    if result.auto_new_ticker_start_tickers:
        print(f"   - Auto new-ticker starts: {result.auto_new_ticker_start_tickers}")
    if result.baseline_clamped_tickers:
        print(
            f"   - Clamped new-ticker starts: {result.baseline_clamped_tickers} "
            "(run full-history repair separately)"
        )
    _exit_if_run_aborted(result.errors, "Backfill")
    if args.incremental and result.errors:
        # A daily catch-up that lost tickers has a gap to fill, so it must not
        # report success even though a bulk backfill in the same state may.
        sys.exit(1)


def _handle_prices_market_cap_backfill(args: argparse.Namespace) -> None:
    """Handle ``collector prices market-cap-backfill``."""
    settings = get_settings()

    from collector.kr.domain.enums import Market

    market_arg = (args.market or "ALL").upper()
    if market_arg == "ALL":
        markets = [Market.KOSPI, Market.KOSDAQ]
    elif market_arg in ("KOSPI", "KOSDAQ"):
        markets = [Market(market_arg)]
    else:
        print(f"❌ Unknown market: {args.market}", file=sys.stderr)
        sys.exit(1)

    throttle = _build_krx_throttle(args)

    print(
        f"→ prices market-cap-backfill: source={args.source}, "
        f"markets={[m.value for m in markets]}, "
        f"start={args.start}, end={args.end}, "
        f"min_delay={args.min_delay_seconds}, max_delay={args.max_delay_seconds}, "
        # No scope here: a market-cap slice is a whole market on a date, so there
        # is no per-ticker target set for UniverseScope to select.
        f"force={args.force}, max_consecutive_failures={args.max_consecutive_failures}"
    )

    from collector.kr.infra.db_postgres.repositories import PostgresStorage
    from collector.kr.service.backfill_market_cap import backfill_market_cap

    if args.source == "pykrx":
        from collector.kr.adapters.market_cap_pykrx.provider import PykrxMarketCapProvider

        provider = PykrxMarketCapProvider()
    else:
        from collector.kr.adapters.market_data_krx_openapi import (
            KrxOpenApiMarketCapProvider,
        )

        provider = KrxOpenApiMarketCapProvider(_build_krx_openapi_client(settings))
        # The scraping throttle is randomised detection avoidance for a site
        # that forbids automation. The Open API is authorised and paces itself
        # with a token bucket inside the client.
        throttle = None

    result = backfill_market_cap(
        provider=provider,
        storage=PostgresStorage(settings.db_dsn),
        markets=markets,
        start=args.start,
        end=args.end,
        throttle=throttle,
        force=args.force,
        max_consecutive_failures=args.max_consecutive_failures,
    )

    if result.errors:
        print(f"⚠ Market-cap backfill completed with {len(result.errors)} errors.", file=sys.stderr)
    else:
        print("✅ Market-cap backfill completed successfully.")

    print(f"   - Slices attempted: {result.slices_attempted}")
    print(f"   - Slices skipped:   {result.slices_skipped}")
    print(f"   - Slices completed: {result.slices_completed}")
    print(f"   - Rows upserted:    {result.rows_upserted}")
    if result.rows_dropped:
        print(f"   - Rows dropped (zero close): {result.rows_dropped}")

    _exit_if_run_aborted(result.errors, "Market-cap backfill")


def _handle_validate(args: argparse.Namespace) -> None:
    """Handle ``collector validate``."""
    settings = get_settings()
    print(
        f"→ validate: date={args.date}, market={args.market}, "
        f"universe_drift_pct={args.universe_drift_pct}"
    )

    from collector.kr.domain.enums import Market

    market_filter = None
    if args.market and args.market.upper() != "ALL":
        market_str = args.market.upper()
        if market_str == "KOSPI":
            market_filter = Market.KOSPI
        elif market_str == "KOSDAQ":
            market_filter = Market.KOSDAQ
        else:
            print(f"❌ Unknown market: {args.market}", file=sys.stderr)
            sys.exit(1)

    from collector.kr.infra.db_postgres.repositories import PostgresStorage

    storage = PostgresStorage(settings.db_dsn)

    from collector.kr.service.validate import validate

    try:
        validate(
            storage=storage,
            market=market_filter,
            target_date=args.date,
            universe_drift_pct=args.universe_drift_pct,
        )
        print("✅ Validation completed. Check logs for details.")
    except Exception as exc:
        print(f"❌ Validation failed: {exc}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Profiling helpers + handlers
# ---------------------------------------------------------------------------


def _resolve_profile_dsn(target: str) -> str:
    """Resolve the DSN for a profiling target (``local`` / ``sj2``).

    ``local`` uses the ``.env`` ``DB_DSN``; ``sj2`` reads the same remote
    ``db_info`` secret used by ``db sync-remote`` (the ``dbq.sh`` source).
    """
    settings = get_settings()
    if target == "local":
        return settings.db_dsn
    if target == "sj2":
        from collector.kr.infra.db_postgres.remote_sync import load_remote_db_info

        if settings.remote_db_info_path is None:
            raise ValueError("REMOTE_DB_INFO_PATH is not set in .env (needed for target=sj2).")
        info = load_remote_db_info(settings.remote_db_info_path)
        host_override = settings.remote_db_host_override
        return info.to_dsn(host_override=host_override)
    raise ValueError(f"Unknown profile target: {target!r} (expected local or sj2)")


def _git_sha() -> str:
    """Return the current git commit SHA (best-effort; empty on failure)."""
    import subprocess

    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:  # noqa: BLE001 — provenance is best-effort
        return ""


def _analysis_lib_versions() -> dict[str, str]:
    """Return installed versions of the analysis libs (for the manifest)."""
    from importlib.metadata import PackageNotFoundError, version

    out: dict[str, str] = {}
    for pkg in ("matplotlib", "plotly", "nbformat", "nbclient", "nbconvert", "pyarrow"):
        try:
            out[pkg] = version(pkg)
        except PackageNotFoundError:
            continue
    return out


def _parse_sample_policy(value: str):  # noqa: ANN202
    """Parse the ``--sample-policy`` flag into a ``SamplePolicy``."""
    from collector.kr.domain.profiling import SamplePolicy

    try:
        return SamplePolicy(value)
    except ValueError as exc:
        choices = ", ".join(p.value for p in SamplePolicy)
        raise argparse.ArgumentTypeError(
            f"Invalid sample policy {value!r} (expected one of: {choices})"
        ) from exc


def _build_profile_components(args: argparse.Namespace):  # noqa: ANN202
    """Construct the query runner + renderers for a profiling run."""
    from collector.kr.adapters.profiling_render.composite import CompositeProfileRenderer
    from collector.kr.adapters.profiling_render.index_renderer import IndexRenderer
    from collector.kr.infra.db_postgres.profiling_query_runner import (
        PostgresProfileQueryRunner,
    )

    dsn = _resolve_profile_dsn(args.target)
    runner = PostgresProfileQueryRunner(
        dsn,
        target=args.target,
        sample_policy=args.sample_policy,
        sample_pct_override=args.sample_pct,
        query_timeout_sec=args.query_timeout_sec,
    )
    renderer = CompositeProfileRenderer(execute_notebooks=not args.no_execute)
    return runner, renderer, IndexRenderer()


def _run_profile_specs(args: argparse.Namespace, specs: list) -> None:
    """Shared driver for ``profile table`` / ``profile all``."""
    from collector.kr.service.profiling.orchestrate import run_profile
    from collector.kr.util.time import now_kst

    runner, renderer, index_renderer = _build_profile_components(args)
    formats = _split_csv(args.formats) or ["ipynb", "md", "html", "json", "parquet"]

    generated_at = now_kst()
    run_date = args.run_date or generated_at.date().isoformat()
    run_id = args.run_id or f"{generated_at.strftime('%Y%m%d_%H%M%S')}_{args.target}"
    if not args.out_dir:
        print("❌ --out-dir not given and SDC_REPORTS_ROOT is not set.", file=sys.stderr)
        sys.exit(1)
    out_root = Path(args.out_dir)
    out_dir = out_root / args.target / run_date

    print(
        f"→ profile: tables={[s.table for s in specs]}, target={args.target}, "
        f"formats={formats}, out={out_dir}, sample_policy={args.sample_policy.value}"
    )

    try:
        manifest, _ = run_profile(
            specs,
            runner,
            renderer,
            index_renderer,
            target=args.target,
            run_id=run_id,
            run_date=run_date,
            out_dir=out_dir,
            formats=formats,
            generated_at=generated_at,
            include_drilldown=getattr(args, "drilldown", False),
            sample_policy=args.sample_policy.value,
            git_sha=_git_sha(),
            lib_versions=_analysis_lib_versions(),
        )
    except Exception as exc:  # noqa: BLE001 — connection/setup failures
        print(f"❌ Profiling failed: {exc}", file=sys.stderr)
        sys.exit(1)

    _update_latest_symlink(out_root / args.target, run_date)
    print(
        f"✅ Profiled {len(manifest.tables)} table(s): "
        f"{manifest.query_ok} check(s) ok, {manifest.query_failed} failed. "
        f"Output: {out_dir}"
    )
    if manifest.query_failed:
        print("⚠  Some checks degraded to warnings — see run_summary.md.", file=sys.stderr)


def _update_latest_symlink(target_dir: Path, run_date: str) -> None:
    """Point ``<target>/latest`` at the newest run (best-effort)."""
    link = target_dir / "latest"
    try:
        if link.is_symlink() or link.exists():
            link.unlink()
        link.symlink_to(run_date, target_is_directory=True)
    except OSError as exc:
        logger.warning("Could not update latest symlink: %s", exc)


def _handle_profile_table(args: argparse.Namespace) -> None:
    """Handle ``collector profile table <name>``."""
    from collector.kr.service.profiling.catalog import get_spec

    try:
        spec = get_spec(args.table)
    except KeyError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        sys.exit(1)
    _run_profile_specs(args, [spec])


def _handle_profile_all(args: argparse.Namespace) -> None:
    """Handle ``collector profile all``."""
    from collector.kr.service.profiling import catalog

    weights = _split_csv(args.weight) or ["full", "light"]
    roles = _split_csv(args.role)
    if roles:
        known_roles = catalog.known_roles()
        unknown_roles = sorted({role.lower() for role in roles} - known_roles)
        if unknown_roles:
            print(
                f"❌ Unknown profile role(s): {', '.join(unknown_roles)}. "
                f"Known roles: {', '.join(sorted(known_roles))}",
                file=sys.stderr,
            )
            sys.exit(1)

    specs = catalog.specs_for_weights_and_roles(weights, roles)
    if not specs:
        role_text = roles if roles else "all"
        print(f"❌ No catalog tables match weights={weights}, roles={role_text}", file=sys.stderr)
        sys.exit(1)
    _run_profile_specs(args, specs)


def _resolve_manifest_path(out_root: Path, target: str, selector: str) -> Path:
    """Resolve a run selector to its ``_run_manifest.json`` path.

    ``selector`` is either ``latest`` (follow the ``<target>/latest`` symlink),
    a run-date directory name, or a direct path to a manifest JSON.
    """
    direct = Path(selector)
    if direct.is_file():
        return direct
    run_dir = out_root / target / selector
    return run_dir / "_run_manifest.json"


def _handle_profile_diff(args: argparse.Namespace) -> None:
    """Handle ``collector profile diff``."""
    import json

    from collector.kr.adapters.profiling_render.diff_renderer import DiffRenderer
    from collector.kr.service.profiling.diff import compare_manifests
    from collector.kr.util.time import now_kst

    if not args.out_dir:
        print("❌ --out-dir not given and SDC_REPORTS_ROOT is not set.", file=sys.stderr)
        sys.exit(1)
    out_root = Path(args.out_dir)
    candidate_path = _resolve_manifest_path(out_root, args.target, args.candidate or "latest")
    if args.baseline:
        baseline_path = Path(args.baseline)
    else:
        baseline_path = _resolve_manifest_path(out_root, args.target, args.against)

    for label, path in (("baseline", baseline_path), ("candidate", candidate_path)):
        if not path.is_file():
            print(f"❌ {label} manifest not found: {path}", file=sys.stderr)
            sys.exit(1)

    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
    report = compare_manifests(baseline, candidate, generated_at=now_kst())

    written = DiffRenderer().render(report, out_dir=candidate_path.parent)
    print(
        f"✅ Drift report: {len(report.changed)}/{len(report.tables)} table(s) changed. "
        f"{', '.join(p.name for p in written)} in {candidate_path.parent}"
    )
    for drift in report.changed:
        bits = []
        if drift.row_delta:
            bits.append(f"rows {drift.row_delta:+,}")
        if drift.max_time_moved in ("forward", "backward"):
            bits.append(f"max_time {drift.max_time_moved}")
        if drift.failed_delta:
            bits.append(f"failed {drift.failed_delta:+}")
        if drift.new_warnings:
            bits.append(f"{len(drift.new_warnings)} new warning(s)")
        print(f"   - {drift.table} [{drift.status}]: {', '.join(bits) or 'changed'}")


def _reports_root_default() -> str | None:
    """``--out-dir`` default for ``profile *`` — resolved once, at parse time."""
    return os.environ.get("SDC_REPORTS_ROOT")


def _publish_docs_dir_default() -> Path | None:
    """``--docs-dir`` default for ``profile publish`` — resolved once, at parse time."""
    value = os.environ.get("SDC_PUBLISH_DOCS_DIR")
    return Path(value) if value else None


_PUBLISH_DOCS_DIR = _publish_docs_dir_default()


def _handle_profile_publish(args: argparse.Namespace) -> None:
    """Handle ``collector profile publish`` — copy reviewed summaries to docs.

    Copies only the lightweight, reviewed artifacts (run summary, manifest,
    per-table + drilldown Markdown) into the docs tree; large HTML / ipynb /
    Parquet outputs stay under ``reports/`` (PLAN §2.2).
    """
    import shutil

    if not args.out_dir:
        print("❌ --out-dir not given and SDC_REPORTS_ROOT is not set.", file=sys.stderr)
        sys.exit(1)
    out_root = Path(args.out_dir)
    run_dir = _resolve_run_dir(out_root, args.target, args.run_id)
    if not run_dir.is_dir():
        print(f"❌ Run directory not found: {run_dir}", file=sys.stderr)
        sys.exit(1)

    dest_root = Path(args.docs_dir) if args.docs_dir else _PUBLISH_DOCS_DIR
    if dest_root is None:
        print(
            "❌ --docs-dir not given and SDC_PUBLISH_DOCS_DIR is not set.", file=sys.stderr
        )
        sys.exit(1)
    dest = dest_root / args.target / run_dir.name
    dest.mkdir(parents=True, exist_ok=True)

    copied: list[str] = []
    for name in ("run_summary.md", "_run_manifest.json", "drift_report.md"):
        src = run_dir / name
        if src.is_file():
            shutil.copy2(src, dest / name)
            copied.append(name)

    # Per-table + drilldown Markdown only (skip html/ipynb/parquet/data).
    src_tables = run_dir / "tables"
    if src_tables.is_dir():
        dest_tables = dest / "tables"
        for md_file in src_tables.rglob("*.md"):
            target_file = dest_tables / md_file.relative_to(src_tables)
            target_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(md_file, target_file)
            copied.append(str(md_file.relative_to(run_dir)))

    print(f"✅ Published {len(copied)} reviewed artifact(s) to {dest}")
    for name in copied[:8]:
        print(f"   - {name}")
    if len(copied) > 8:
        print(f"   … and {len(copied) - 8} more")


def _resolve_run_dir(out_root: Path, target: str, run_id: str | None) -> Path:
    """Resolve a run directory by run-date name (default: the latest symlink)."""
    base = out_root / target
    if run_id:
        return base / run_id
    return base / "latest"


# ---------------------------------------------------------------------------
# Parser construction
# ---------------------------------------------------------------------------


def _parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD string into a ``date``."""
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {value!r} (expected YYYY-MM-DD)")


def _parse_coverage_ratio(value: str) -> Decimal:
    """Parse a coverage ratio in the inclusive 0..1 range."""
    try:
        ratio = Decimal(value)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid coverage ratio: {value!r} (expected decimal between 0 and 1)"
        ) from exc
    if not ratio.is_finite() or ratio < Decimal("0") or ratio > Decimal("1"):
        raise argparse.ArgumentTypeError(
            f"Invalid coverage ratio: {value!r} (expected decimal between 0 and 1)"
        )
    return ratio.quantize(Decimal("0.0001"))


def _parse_positive_seconds(value: str) -> float:
    """Parse a positive second value, accepting an optional ``s`` suffix."""
    normalized = value.strip().lower()
    for suffix in ("seconds", "second", "secs", "sec", "s"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)].strip()
            break
    try:
        seconds = float(normalized)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid seconds value: {value!r} (expected positive seconds)"
        )
    if seconds <= 0:
        raise argparse.ArgumentTypeError(
            f"Invalid seconds value: {value!r} (must be greater than zero)"
        )
    return seconds


def build_parser() -> argparse.ArgumentParser:
    """Build and return the top-level argument parser."""
    parser = argparse.ArgumentParser(
        prog="collector",
        description="KRX stock data pipeline — universe sync & daily OHLCV collection.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # -- db -------------------------------------------------------------------
    db_parser = subparsers.add_parser("db", help="Database management commands.")
    db_sub = db_parser.add_subparsers(dest="db_command", required=True)

    db_init = db_sub.add_parser("init", help="Initialise database schema (run DDL).")
    db_init.set_defaults(handler=_handle_db_init)

    db_sync_remote = db_sub.add_parser(
        "sync-remote",
        help="Sync the remote sj2-server PostgreSQL data into the local PostgreSQL DB.",
    )
    db_sync_remote.add_argument(
        "--db-info-path",
        default=None,
        help="Path to the remote DB metadata file (default: from config).",
    )
    db_sync_remote.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Number of rows to fetch from the remote DB per batch (default: from config).",
    )
    db_sync_remote.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="Truncate the local synced tables and copy everything from scratch.",
    )
    db_sync_scope = db_sync_remote.add_mutually_exclusive_group()
    db_sync_scope.add_argument(
        "--all-tables",
        action="store_true",
        default=False,
        help=(
            "Sync the managed mirror tables through the schema-reset copy path. "
            "Requires --full-refresh. "
            "Drops only those local tables and re-applies sql/postgres_ddl.sql "
            "before copying so stale local schemas are replaced."
        ),
    )
    db_sync_scope.add_argument(
        "--tables",
        default=None,
        help=(
            "Comma-separated managed table names to sync. FK parent tables are "
            "included automatically. Omit to sync all managed mirror tables."
        ),
    )
    db_sync_remote.add_argument(
        "--remote-host",
        default=None,
        help="Override the remote DB hostname from db_info (default: from config/file).",
    )
    db_sync_remote.add_argument(
        "--ssh-host",
        default=None,
        help="Optional SSH host for port forwarding to the remote PostgreSQL server.",
    )
    db_sync_remote.add_argument(
        "--ssh-local-port",
        type=int,
        default=None,
        help="Optional fixed local port for the SSH tunnel (default: random free port).",
    )
    db_sync_remote.add_argument(
        "--ssh-compression",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable SSH compression for the optional DB tunnel (default: from config).",
    )
    db_sync_remote.set_defaults(handler=_handle_db_sync_remote)

    db_with_remote_dsn = db_sub.add_parser(
        "with-remote-dsn",
        help=(
            "Resolve the remote sj2-server DSN (opening an SSH tunnel if configured) and run "
            "a child command with SDC_REMOTE_DSN injected into its environment only."
        ),
    )
    db_with_remote_dsn.add_argument(
        "--db-info-path",
        default=None,
        help="Path to the remote DB metadata file (default: from config).",
    )
    db_with_remote_dsn.add_argument(
        "--remote-host",
        default=None,
        help="Override the remote DB hostname from db_info (default: from config/file).",
    )
    db_with_remote_dsn.add_argument(
        "--ssh-host",
        default=None,
        help="Optional SSH host for port forwarding to the remote PostgreSQL server.",
    )
    db_with_remote_dsn.add_argument(
        "--ssh-local-port",
        type=int,
        default=None,
        help="Optional fixed local port for the SSH tunnel (default: random free port).",
    )
    db_with_remote_dsn.add_argument(
        "--ssh-compression",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable SSH compression for the optional DB tunnel (default: from config).",
    )
    db_with_remote_dsn.add_argument(
        "remote_command",
        nargs=argparse.REMAINDER,
        help=(
            "Command to run with SDC_REMOTE_DSN set, e.g. "
            "-- bin/raw-parquet-export-all.sh --route remote"
        ),
    )
    db_with_remote_dsn.set_defaults(handler=_handle_db_with_remote_dsn)

    # -- ops ------------------------------------------------------------------
    ops_parser = subparsers.add_parser("ops", help="Read-only operational reports.")
    ops_sub = ops_parser.add_subparsers(dest="ops_command", required=True)
    ops_freshness = ops_sub.add_parser(
        "freshness-report",
        help="Report latest stored data points and running ingestion jobs.",
    )
    ops_freshness.add_argument(
        "--running-limit",
        type=int,
        default=20,
        help="Maximum running ingestion runs to show (default: 20).",
    )
    ops_freshness.add_argument(
        "--fail-if-stale",
        action="store_true",
        help=(
            "Exit 1 when a raw domain is behind its freshness budget. "
            "Without this the command only prints, so a scheduled run of it "
            "stays green through an outage. Schedule it AFTER the evening "
            "collection window."
        ),
    )
    ops_freshness.add_argument(
        "--max-lag-trading-days",
        type=int,
        default=DEFAULT_MAX_LAG_TRADING_DAYS,
        help=(
            "Trading-day budget for KRX-cadence domains "
            f"(default: {DEFAULT_MAX_LAG_TRADING_DAYS}; 1 = the latest session must be stored)."
        ),
    )
    ops_freshness.add_argument(
        "--max-lag-calendar-days",
        type=int,
        default=DEFAULT_MAX_LAG_CALENDAR_DAYS,
        help=(
            "Fallback calendar-day budget when common-series catalog metadata is unavailable; "
            "stored series use their max_stale_business_days instead "
            f"(default: {DEFAULT_MAX_LAG_CALENDAR_DAYS})."
        ),
    )
    ops_freshness.add_argument(
        "--max-market-cap-lag-trading-days",
        type=int,
        default=DEFAULT_MAX_MARKET_CAP_LAG_TRADING_DAYS,
        help=(
            "Trading-day budget for daily_market_cap, whose KRX source is T+1 "
            f"(default: {DEFAULT_MAX_MARKET_CAP_LAG_TRADING_DAYS})."
        ),
    )
    ops_freshness.set_defaults(handler=_handle_ops_freshness_report)

    # -- dart -----------------------------------------------------------------
    dart_parser = subparsers.add_parser("dart", help="OpenDART ingestion commands.")
    dart_sub = dart_parser.add_subparsers(dest="dart_command", required=True)

    dart_sync_corp = dart_sub.add_parser(
        "sync-corp",
        help="Download the OpenDART corp-code master and map it to active KRX tickers.",
    )
    dart_sync_corp.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if a previous successful corp sync is recorded.",
    )
    dart_sync_corp.set_defaults(handler=_handle_dart_sync_corp)

    dart_sync_corp_profile = dart_sub.add_parser(
        "sync-corp-profile",
        help="Fetch company.json (industry code, incorporation date, fiscal-year end).",
    )
    dart_sync_corp_profile.add_argument(
        "--tickers",
        default=None,
        help="Comma-separated ticker allowlist (default: all ticker-mapped corps).",
    )
    dart_sync_corp_profile.add_argument("--rate-limit-seconds", type=float, default=0.2)
    dart_sync_corp_profile.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Re-fetch corporations that already have a profile.",
    )
    dart_sync_corp_profile.add_argument(
        "--universe-scope",
        choices=["current", "historical"],
        default="current",
        help=(
            "Which universe to target. 'current' = currently-listed corps. "
            "'historical' = every corp that ever had a ticker; delisted names are "
            "unreachable otherwise."
        ),
    )
    dart_sync_corp_profile.set_defaults(handler=_handle_dart_sync_corp_profile)

    dart_seed_corp_profile_history = dart_sub.add_parser(
        "seed-corp-profile-history",
        help=(
            "Seed dart_corp_profile_history from dart_corp_master's current profiles "
            "(no API calls; run once before the first monthly snapshot)."
        ),
    )
    dart_seed_corp_profile_history.add_argument(
        "--observed-month",
        default=None,
        help=(
            "Month to stamp the seed rows with, YYYY-MM-DD (day is forced to the 1st). "
            "Default: the current month."
        ),
    )
    dart_seed_corp_profile_history.set_defaults(handler=_handle_dart_seed_corp_profile_history)

    dart_sync_financials = dart_sub.add_parser(
        "sync-financials",
        help="Download OpenDART single-company full financial statements into raw storage.",
    )
    dart_sync_financials.add_argument(
        "--bsns-years",
        default=str(date.today().year - 1),
        help="Comma-separated business years (default: previous year).",
    )
    dart_sync_financials.add_argument(
        "--reprt-codes",
        default="11011",
        help="Comma-separated report codes (default: 11011 for annual report).",
    )
    dart_sync_financials.add_argument(
        "--fs-divs",
        default="CFS",
        help="Comma-separated fs_div values (default: CFS).",
    )
    dart_sync_financials.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    dart_sync_financials.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.2,
        help="Seconds between OpenDART requests (default: 0.2).",
    )
    dart_sync_financials.add_argument(
        "--force",
        action="store_true",
        help="Re-download even when raw rows already exist for a request key.",
    )
    dart_sync_financials.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Resolve business years/report codes from filing availability and recent no-data.",
    )
    dart_sync_financials.add_argument(
        "--lookback-years",
        type=int,
        default=1,
        help="Number of prior business years to include in --incremental mode.",
    )
    dart_sync_financials.add_argument(
        "--max-attempt-targets",
        type=int,
        default=10000,
        help="Maximum estimated OpenDART requests allowed in --incremental mode.",
    )
    dart_sync_financials.add_argument(
        "--negative-cache-ttl-days",
        type=int,
        default=3,
        help="Days to skip request keys that recently returned no-data.",
    )
    dart_sync_financials.add_argument(
        "--universe-scope",
        choices=["current", "historical"],
        default="current",
        help=(
            "Which universe to target. 'current' = currently-listed corps "
            "(2,657). 'historical' = every corp that ever had a ticker (3,959); "
            "delisted names are unreachable otherwise."
        ),
    )
    dart_sync_financials.set_defaults(handler=_handle_dart_sync_financials)

    dart_sync_share_info = dart_sub.add_parser(
        "sync-share-info",
        help="Download OpenDART stock count, dividend, and treasury-stock disclosures.",
    )
    dart_sync_share_info.add_argument(
        "--bsns-years",
        default=str(date.today().year - 1),
        help="Comma-separated business years (default: previous year).",
    )
    dart_sync_share_info.add_argument(
        "--reprt-codes",
        default="11011",
        help="Comma-separated report codes (default: 11011 for annual report).",
    )
    dart_sync_share_info.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    dart_sync_share_info.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.2,
        help="Seconds between OpenDART request groups (default: 0.2).",
    )
    dart_sync_share_info.add_argument(
        "--force",
        action="store_true",
        help="Re-download even when raw rows already exist for a request key.",
    )
    dart_sync_share_info.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Resolve business years/report codes from filing availability and recent no-data.",
    )
    dart_sync_share_info.add_argument(
        "--lookback-years",
        type=int,
        default=1,
        help="Number of prior business years to include in --incremental mode.",
    )
    dart_sync_share_info.add_argument(
        "--max-attempt-targets",
        type=int,
        default=10000,
        help="Maximum estimated OpenDART requests allowed in --incremental mode.",
    )
    dart_sync_share_info.add_argument(
        "--negative-cache-ttl-days",
        type=int,
        default=3,
        help="Days to skip request keys that recently returned no-data.",
    )
    dart_sync_share_info.add_argument(
        "--universe-scope",
        choices=["current", "historical"],
        default="current",
        help=(
            "Which universe to target. 'current' = currently-listed corps "
            "(2,657). 'historical' = every corp that ever had a ticker (3,959); "
            "delisted names are unreachable otherwise."
        ),
    )
    dart_sync_share_info.set_defaults(handler=_handle_dart_sync_share_info)

    dart_sync_periodic_extras = dart_sub.add_parser(
        "sync-periodic-extras",
        help="Sync DS002 periodic-report extras (employees, control, audit opinion).",
    )
    dart_sync_periodic_extras.add_argument(
        "--start-year",
        type=int,
        default=2015,
        help="First business year to cover (default: 2015).",
    )
    dart_sync_periodic_extras.add_argument(
        "--end-year",
        type=int,
        required=True,
        help="Last business year to cover.",
    )
    dart_sync_periodic_extras.add_argument(
        "--reprt-codes",
        default="11011",
        help=(
            "Comma-separated report codes (default: 11011). Headcount, control "
            "and audit opinion barely move within a year, so the annual report "
            "is enough; hyslrChgSttus carries change_on for exact event dates."
        ),
    )
    dart_sync_periodic_extras.add_argument(
        "--statements",
        default=None,
        help=(
            "Comma-separated statement types "
            "(employee, executive, major_shareholder, major_change, audit_opinion). "
            "Default excludes 'executive': 32,400 calls for one thin candidate."
        ),
    )
    dart_sync_periodic_extras.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    dart_sync_periodic_extras.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.2,
        help="Seconds between OpenDART requests (default: 0.2).",
    )
    dart_sync_periodic_extras.add_argument(
        "--force",
        action="store_true",
        help="Re-collect every slice, ignoring the ledger. The ledger is still written.",
    )
    dart_sync_periodic_extras.add_argument(
        "--universe-scope",
        choices=["current", "historical"],
        default="historical",
        help=(
            "Which universe to target (default: historical). An adverse audit "
            "opinion is mostly about companies that later delisted, so "
            "'current' would bias the feature this package exists for."
        ),
    )
    dart_sync_periodic_extras.set_defaults(handler=_handle_dart_sync_periodic_extras)

    dart_sync_xbrl = dart_sub.add_parser(
        "sync-xbrl",
        help="Download and parse OpenDART XBRL ZIP filings into raw fact storage.",
    )
    dart_sync_xbrl.add_argument(
        "--bsns-years",
        default=str(date.today().year - 1),
        help="Comma-separated business years (default: previous year).",
    )
    dart_sync_xbrl.add_argument(
        "--reprt-codes",
        default="11011",
        help="Comma-separated report codes (default: 11011 for annual report).",
    )
    dart_sync_xbrl.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    dart_sync_xbrl.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.2,
        help="Seconds between OpenDART XBRL requests (default: 0.2).",
    )
    dart_sync_xbrl.add_argument(
        "--force",
        action="store_true",
        help="Re-parse even when an XBRL document is already stored for a filing.",
    )
    dart_sync_xbrl.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Resolve business years/report codes from filing availability and recent no-data.",
    )
    dart_sync_xbrl.add_argument(
        "--lookback-years",
        type=int,
        default=1,
        help="Number of prior business years to include in --incremental mode.",
    )
    dart_sync_xbrl.add_argument(
        "--max-attempt-targets",
        type=int,
        default=10000,
        help="Maximum estimated OpenDART requests allowed in --incremental mode.",
    )
    dart_sync_xbrl.add_argument(
        "--negative-cache-ttl-days",
        type=int,
        default=3,
        help="Days to skip request keys that recently returned no-data.",
    )
    dart_sync_xbrl.add_argument(
        "--universe-scope",
        choices=["current", "historical"],
        default="current",
        help=(
            "Which universe to target. 'current' = currently-listed corps "
            "(2,657). 'historical' = every corp that ever had a ticker (3,959); "
            "delisted names are unreachable otherwise."
        ),
    )
    dart_sync_xbrl.set_defaults(handler=_handle_dart_sync_xbrl)

    dart_sync_filings = dart_sub.add_parser(
        "sync-filings",
        help="Download OpenDART disclosure-receipt history (공시검색).",
    )
    dart_sync_filings.add_argument(
        "--years",
        default=str(date.today().year),
        help="Comma-separated calendar years (default: current year).",
    )
    dart_sync_filings.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    dart_sync_filings.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.2,
        help="Seconds between OpenDART requests (default: 0.2).",
    )
    dart_sync_filings.add_argument(
        "--force",
        action="store_true",
        help="Re-download even when receipts already exist for a (corp, year) window.",
    )
    dart_sync_filings.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help=(
            "For the current calendar year, request only this many recent calendar days. "
            "Past years remain full-year windows. Default: current year to date."
        ),
    )
    dart_sync_filings.add_argument(
        "--universe-scope",
        choices=["current", "historical"],
        default="current",
        help=(
            "Which universe to target. 'current' = currently-listed corps "
            "(2,657). 'historical' = every corp that ever had a ticker (3,959); "
            "delisted names are unreachable otherwise."
        ),
    )
    dart_sync_filings.set_defaults(handler=_handle_dart_sync_filings)

    dart_backfill_xbrl_receipts = dart_sub.add_parser(
        "backfill-xbrl-receipts",
        help="Fetch XBRL for an explicit list of (corp, filing, receipt) targets.",
    )
    dart_backfill_xbrl_receipts.add_argument(
        "--targets-file",
        required=True,
        help=(
            "Path to a JSON-lines file of targets, each with ticker, corp_code, "
            "bsns_year, reprt_code, rcept_no."
        ),
    )
    dart_backfill_xbrl_receipts.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.2,
        help="Seconds between OpenDART requests (default: 0.2).",
    )
    dart_backfill_xbrl_receipts.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch even when an XBRL document is already stored for a target.",
    )
    dart_backfill_xbrl_receipts.set_defaults(handler=_handle_dart_backfill_xbrl_receipts)

    # -- common ---------------------------------------------------------------
    common_parser = subparsers.add_parser(
        "common",
        help="Common market and macro feature commands.",
    )
    common_sub = common_parser.add_subparsers(dest="common_command", required=True)

    common_seed = common_sub.add_parser(
        "seed-catalog",
        help="Seed Phase 1 common feature source series and feature catalog rows.",
    )
    common_seed.add_argument(
        "--init-schema",
        action="store_true",
        default=False,
        help="Initialise/update the database schema before seeding.",
    )
    common_seed.set_defaults(handler=_handle_common_seed_catalog)

    common_sync = common_sub.add_parser(
        "sync",
        help="Sync common feature raw observations from configured providers.",
    )
    common_sync.add_argument(
        "--sources",
        type=_parse_common_sources,
        default=_parse_common_sources("pykrx,fdr"),
        help="Comma-separated source allowlist: pykrx,krx,fdr,ecos,fred (default: pykrx,fdr).",
    )
    common_sync.add_argument(
        "--series",
        default=None,
        help="Optional comma-separated common feature series_id allowlist.",
    )
    common_sync.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Start date (YYYY-MM-DD). Required unless --incremental is used.",
    )
    common_sync.add_argument(
        "--end",
        type=_parse_date,
        default=date.today(),
        help="End date (YYYY-MM-DD). Default: today.",
    )
    common_sync.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Resolve start from stored raw observation latest dates.",
    )
    common_sync.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Recent calendar-day window to rescan in --incremental mode.",
    )
    common_sync.add_argument(
        "--max-auto-range-days",
        type=int,
        default=90,
        help="Maximum inclusive day range allowed for --incremental without override.",
    )
    common_sync.add_argument(
        "--allow-large-range",
        action="store_true",
        default=False,
        help="Allow resolved common sync ranges larger than the safety guard.",
    )
    common_sync.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Re-fetch even when existing raw observations are present.",
    )
    common_sync.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.0,
        help="Seconds between provider series requests (default: 0.0).",
    )
    common_sync.add_argument(
        "--include-inactive",
        action="store_true",
        default=False,
        help="Allow explicitly selected inactive source series for smoke verification.",
    )
    common_sync.add_argument(
        "--init-schema",
        action="store_true",
        default=False,
        help="Initialise/update the database schema before syncing.",
    )
    common_sync.set_defaults(handler=_handle_common_sync)

    # -- flows ----------------------------------------------------------------
    flows_parser = subparsers.add_parser("flows", help="Security flow ingestion commands.")
    flows_sub = flows_parser.add_subparsers(dest="flows_command", required=True)

    flows_sync = flows_sub.add_parser(
        "sync",
        help="Sync daily investor/foreign/shorting raw flow metrics.",
    )
    flows_sync.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Start date (YYYY-MM-DD). Default: yesterday.",
    )
    flows_sync.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="End date (YYYY-MM-DD). Default: yesterday.",
    )
    flows_sync.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    flows_sync.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=None,
        help=(
            "Seconds between higher-level flow requests "
            "(default: env KRX_LOGICAL_RATE_LIMIT_SECONDS or 8.0)."
        ),
    )
    flows_sync.add_argument(
        "--timeout-seconds",
        type=_parse_positive_seconds,
        default=None,
        help=(
            "KRX MDC HTTP timeout in seconds "
            "(default: env KRX_MDC_TIMEOUT_SECONDS or 20; accepts 150 or 150s)."
        ),
    )
    flows_sync.add_argument(
        "--use-price-range",
        action="store_true",
        help=(
            "Use the stored daily OHLCV min/max trade_date as the flow sync range. "
            "Optional --start/--end further clamp that range."
        ),
    )
    flows_sync.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help=(
            "Resolve the flow sync range from stored daily OHLCV and KRX flow "
            "latest dates. Intended for daily catch-up runs."
        ),
    )
    flows_sync.add_argument(
        "--lookback-days",
        type=int,
        default=14,
        help=(
            "Recent calendar-day window to always rescan in --incremental mode " "(default: 14)."
        ),
    )
    flows_sync.add_argument(
        "--max-auto-range-days",
        type=int,
        default=30,
        help="Maximum inclusive day range allowed for --incremental without override.",
    )
    flows_sync.add_argument(
        "--max-price-range-days",
        type=int,
        default=90,
        help="Maximum inclusive day range allowed for --use-price-range without override.",
    )
    flows_sync.add_argument(
        "--allow-large-range",
        action="store_true",
        default=False,
        help="Allow resolved flow ranges larger than the configured safety guard.",
    )
    flows_sync.add_argument(
        "--exclude-groups",
        default=None,
        help=(
            "Comma-separated flow metric groups to exclude from range resolution and "
            "collection (foreign_holding,investor,shorting)."
        ),
    )
    flows_sync.add_argument(
        "--http-min-delay-seconds",
        type=float,
        default=None,
        help=(
            "Minimum seconds between actual KRX HTTP calls "
            "(default: env KRX_MIN_DELAY_SECONDS or 1.5)."
        ),
    )
    flows_sync.add_argument(
        "--http-max-delay-seconds",
        type=float,
        default=None,
        help=(
            "Maximum seconds between actual KRX HTTP calls "
            "(default: env KRX_MAX_DELAY_SECONDS or 4.0)."
        ),
    )
    flows_sync.add_argument(
        "--long-rest-every",
        type=int,
        default=None,
        help=(
            "Take a long random rest after this many KRX HTTP calls "
            "(default: env KRX_LONG_REST_EVERY or 15)."
        ),
    )
    flows_sync.add_argument(
        "--long-rest-min-seconds",
        type=float,
        default=None,
        help="Minimum seconds for a long KRX rest (default: env KRX_LONG_REST_MIN_SECONDS or 30).",
    )
    flows_sync.add_argument(
        "--long-rest-max-seconds",
        type=float,
        default=None,
        help="Maximum seconds for a long KRX rest (default: env KRX_LONG_REST_MAX_SECONDS or 90).",
    )
    flows_sync.add_argument(
        "--auth-cooldown-seconds",
        type=float,
        default=None,
        help=(
            "Seconds to wait after a successful KRX login "
            "(default: env KRX_AUTH_COOLDOWN_SECONDS or 10)."
        ),
    )
    flows_sync.add_argument(
        "--error-backoff-min-seconds",
        type=float,
        default=None,
        help=(
            "Minimum seconds to wait after a KRX error "
            "(default: env KRX_ERROR_BACKOFF_MIN_SECONDS or 45)."
        ),
    )
    flows_sync.add_argument(
        "--error-backoff-max-seconds",
        type=float,
        default=None,
        help=(
            "Maximum seconds to wait after a KRX error "
            "(default: env KRX_ERROR_BACKOFF_MAX_SECONDS or 180)."
        ),
    )
    flows_sync.add_argument(
        "--progress-log-interval-seconds",
        type=float,
        default=30.0,
        help="Emit flow sync progress at least this often in seconds (0 disables time-based logs).",
    )
    flows_sync.add_argument(
        "--progress-log-every-items",
        type=int,
        default=100,
        help="Emit flow sync progress every N handled items (0 disables count-based logs).",
    )
    flows_sync.add_argument(
        "--ordered-requests",
        action="store_true",
        help="Disable randomized request order and preserve deterministic traversal.",
    )
    flows_sync.add_argument(
        "--universe-scope",
        choices=["current", "historical"],
        default="current",
        help=(
            "Which universe to target. 'current' = the active universe. "
            "'historical' = every stock ever listed; required to collect flows "
            "for names that have since delisted."
        ),
    )
    flows_sync.set_defaults(handler=_handle_flows_sync)

    flows_sync_kis = flows_sub.add_parser(
        "sync-kis",
        help="Sync investor/shorting/foreign-holding raw flow metrics from KIS Developers.",
    )
    flows_sync_kis.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Start date (YYYY-MM-DD). Default: --lookback-days before --end.",
    )
    flows_sync_kis.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="End date (YYYY-MM-DD). Default: the most recent KRX trading day.",
    )
    flows_sync_kis.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_KIS_LOOKBACK_DAYS,
        help=(
            "Calendar days of overlap to re-check before --end when --start is omitted. "
            "One investor call covers 30 sessions, so overlap is nearly free and it is "
            "what lets a weekly schedule backfill days a failed run missed."
        ),
    )
    flows_sync_kis.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker allowlist.",
    )
    flows_sync_kis.add_argument(
        "--exclude-groups",
        default=None,
        help=(
            "Comma-separated flow metric groups to skip " "(foreign_holding, investor, shorting)."
        ),
    )
    flows_sync_kis.add_argument(
        "--universe-scope",
        choices=["current", "historical"],
        default="current",
        help=(
            "Which universe to target. 'current' = the active universe. "
            "'historical' = every stock ever listed."
        ),
    )
    flows_sync_kis.add_argument(
        "--requests-per-second",
        type=float,
        default=None,
        help=(
            "Token-bucket rate for KIS calls (default: env KIS_REQUESTS_PER_SECOND or 1.0). "
            "KIS documents 20/s per account, but this one was measured at 1/s on "
            "2026-08-16 — 1.2/s already draws rejections and effective throughput "
            "never exceeded ~1.1/s, so raising this buys retries, not speed."
        ),
    )
    flows_sync_kis.add_argument(
        "--timeout-seconds",
        type=_parse_positive_seconds,
        default=None,
        help="KIS HTTP timeout in seconds (default: env KIS_TIMEOUT_SECONDS or 20).",
    )
    flows_sync_kis.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=DEFAULT_KIS_MAX_CONSECUTIVE_FAILURES,
        help="Stop after N consecutive failed requests (0 disables the guard).",
    )
    flows_sync_kis.add_argument(
        "--no-data-ttl-days",
        type=int,
        default=DEFAULT_KIS_NO_DATA_TTL_DAYS,
        help=(
            "How long a ticker/group that returned no data stays tombstoned. "
            "A suspended name can resume, so this expires rather than being permanent."
        ),
    )
    flows_sync_kis.add_argument(
        "--token-cache-path",
        default=None,
        help=(
            "Where the KIS access token is cached (default: env KIS_TOKEN_CACHE_PATH). "
            "Must be a host volume — every issuance notifies the account holder."
        ),
    )
    flows_sync_kis.add_argument(
        "--plan-only",
        action="store_true",
        help=(
            "Resolve and print the work plan without calling KIS. "
            "Issues no token and sends no request."
        ),
    )
    flows_sync_kis.set_defaults(handler=_handle_flows_sync_kis)

    # -- universe -------------------------------------------------------------
    universe_parser = subparsers.add_parser("universe", help="Stock universe management.")
    universe_sub = universe_parser.add_subparsers(dest="universe_command", required=True)

    universe_sync = universe_sub.add_parser("sync", help="Sync listed stock universe.")
    universe_sync.add_argument(
        "--source",
        choices=["krx-openapi", "fdr", "pykrx"],
        default=None,
        help=(
            "Data source (default: from config UNIVERSE_SOURCE_DEFAULT). "
            "krx-openapi is the official endpoint and needs AUTH_KEYS; "
            "fdr and pykrx reach KRX outside the permitted path (K-5)."
        ),
    )
    universe_sync.add_argument(
        "--markets",
        default="kospi,kosdaq",
        help="Comma-separated market list (default: kospi,kosdaq).",
    )
    universe_sync.add_argument(
        "--as-of",
        type=_parse_date,
        default=None,
        help="Reference date (YYYY-MM-DD). Default: today (KST).",
    )
    universe_sync.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="Replace all stock_master rows instead of incremental diff.",
    )
    universe_sync.set_defaults(handler=_handle_universe_sync)

    universe_backfill = universe_sub.add_parser(
        "backfill-snapshots",
        help="Backfill month-end historical universe snapshots (survivorship audit).",
    )
    universe_backfill.add_argument(
        "--markets",
        default="kospi,kosdaq",
        help="Comma-separated market list (default: kospi,kosdaq).",
    )
    universe_backfill.add_argument("--start", type=_parse_date, default=None)
    universe_backfill.add_argument("--end", type=_parse_date, default=None)
    _add_krx_source_argument(universe_backfill)
    _add_krx_throttle_arguments(universe_backfill)
    universe_backfill.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Re-fetch dates that already have a backfilled snapshot.",
    )
    universe_backfill.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=5,
        help=(
            "Stop the run after this many targets fail in a row (0 disables). "
            "Guards against grinding through the whole target list while the "
            "source is refusing, which turns a temporary throttle into a block."
        ),
    )
    universe_backfill.set_defaults(handler=_handle_universe_backfill_snapshots)

    universe_backfill_master = universe_sub.add_parser(
        "backfill-master",
        help=(
            "Recover securities that appear in a historical snapshot but are "
            "missing from stock_master, as DELISTED."
        ),
    )
    universe_backfill_master.add_argument(
        "--sources",
        default=",".join(source.value for source in DEFAULT_SNAPSHOT_SOURCES),
        help=(
            "Comma-separated snapshot sources to read "
            f"(default: {','.join(s.value for s in DEFAULT_SNAPSHOT_SOURCES)})."
        ),
    )
    universe_backfill_master.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Report how many securities would be recovered without writing them.",
    )
    universe_backfill_master.set_defaults(handler=_handle_universe_backfill_master)

    # -- prices ---------------------------------------------------------------
    prices_parser = subparsers.add_parser("prices", help="Price data commands.")
    prices_sub = prices_parser.add_subparsers(dest="prices_command", required=True)

    prices_backfill = prices_sub.add_parser("backfill", help="Backfill daily OHLCV data.")
    prices_backfill.add_argument(
        "--source",
        choices=["naver", "pykrx"],
        default="naver",
        help=(
            "Where to fetch bars from (default: naver). Both return the same "
            "adjusted series — pykrx's adjusted path is a wrapper over the same "
            "Naver endpoint — but importing pykrx logs in to KRX (K-5)."
        ),
    )
    prices_backfill.add_argument(
        "--market",
        choices=["kospi", "kosdaq", "all"],
        default="all",
        help="Market filter (default: all).",
    )
    prices_backfill.add_argument(
        "--tickers",
        default=None,
        help="Comma-separated ticker codes (default: all active tickers).",
    )
    prices_backfill.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        help="Start date (YYYY-MM-DD). Default: 2000-01-01.",
    )
    prices_backfill.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        help="End date (YYYY-MM-DD). Default: today (KST).",
    )
    prices_backfill.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=None,
        help="Seconds between API calls (default: from config).",
    )
    prices_backfill.add_argument(
        "--long-rest-interval",
        type=int,
        default=None,
        help=("Number of API requests between long rests " "(0 disables; default: from config)."),
    )
    prices_backfill.add_argument(
        "--long-rest-seconds",
        type=float,
        default=None,
        help="Duration of each long rest in seconds (default: from config).",
    )
    prices_backfill.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help=(
            "Skip per-day gap detection and fetch only days after each "
            "ticker's MAX(trade_date). Intended for fast daily catch-up runs."
        ),
    )
    prices_backfill.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Recent calendar-day window to rescan in --incremental mode (default: 0).",
    )
    prices_backfill.add_argument(
        "--max-auto-range-days",
        type=int,
        default=10,
        help=(
            "Maximum inclusive day range allowed for --incremental without override. "
            "Also bounds auto-derived new-ticker starts (listing_date/first_seen_date)."
        ),
    )
    prices_backfill.add_argument(
        "--new-ticker-start",
        type=_parse_date,
        default=None,
        help=(
            "Explicit start date for tickers with no stored price baseline; "
            "overrides stock_master listing_date/first_seen_date and is not "
            "clamped to the auto-range window."
        ),
    )
    prices_backfill.add_argument(
        "--allow-new-ticker-backfill",
        action="store_true",
        default=False,
        help="Allow baseline-missing tickers to use --start or the default early start.",
    )
    prices_backfill.add_argument(
        "--allow-large-range",
        action="store_true",
        default=False,
        help="Allow resolved incremental price ranges larger than the safety guard.",
    )
    prices_backfill.add_argument(
        "--refetch",
        action="store_true",
        default=False,
        help=(
            "Re-fetch and overwrite the whole range instead of only filling gaps. "
            "Needed to repair rows whose adjusted prices went stale after a split. "
            "Cannot be combined with --incremental."
        ),
    )
    prices_backfill.add_argument(
        "--universe-scope",
        choices=["current", "historical"],
        default="current",
        help=(
            "Which universe to target. 'current' = the active universe. "
            "'historical' = the full stock master; delisted tickers are "
            "unreachable otherwise, even when named in --tickers."
        ),
    )
    prices_backfill.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=5,
        help=(
            "Stop the run after this many targets fail in a row (0 disables). "
            "Guards against grinding through the whole target list while the "
            "source is refusing, which turns a temporary throttle into a block."
        ),
    )
    prices_backfill.set_defaults(handler=_handle_prices_backfill)

    prices_market_cap = prices_sub.add_parser(
        "market-cap-backfill",
        help="Backfill daily KRX market cap / trading value / listed shares.",
    )
    prices_market_cap.add_argument(
        "--market",
        type=str,
        default="ALL",
        help=(
            "KOSPI | KOSDAQ | ALL (default). ALL means KOSPI and KOSDAQ as two "
            "separate requests per date — it is never passed to pykrx, which "
            "would fold in KONEX and leave the market column unfillable."
        ),
    )
    prices_market_cap.add_argument("--start", type=_parse_date, default=None)
    prices_market_cap.add_argument("--end", type=_parse_date, default=None)
    _add_krx_source_argument(prices_market_cap)
    _add_krx_throttle_arguments(prices_market_cap)
    prices_market_cap.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Re-fetch slices that are already complete.",
    )
    prices_market_cap.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=5,
        help=(
            "Stop the run after this many targets fail in a row (0 disables). "
            "Guards against grinding through the whole target list while the "
            "source is refusing, which turns a temporary throttle into a block."
        ),
    )
    prices_market_cap.set_defaults(handler=_handle_prices_market_cap_backfill)

    # -- profile --------------------------------------------------------------
    profile_parser = subparsers.add_parser(
        "profile", help="Generate reproducible table/feature statistical profiles."
    )
    profile_sub = profile_parser.add_subparsers(dest="profile_command", required=True)

    def _add_common_profile_args(sub: argparse.ArgumentParser) -> None:
        sub.add_argument(
            "--target",
            choices=["local", "sj2"],
            default="local",
            help="DB target: local mirror (.env DB_DSN) or sj2 origin (db_info).",
        )
        sub.add_argument(
            "--formats",
            default=None,
            help=(
                "Comma-separated output formats from ipynb,md,html,json,parquet "
                "(default: ipynb,md,html,json,parquet)."
            ),
        )
        sub.add_argument(
            "--out-dir",
            default=_reports_root_default(),
            help="Output root (default: $SDC_REPORTS_ROOT; required if unset).",
        )
        sub.add_argument(
            "--run-id",
            default=None,
            help="Override run id (default: <timestamp>_<target>).",
        )
        sub.add_argument(
            "--run-date",
            default=None,
            help="Override run date directory (default: today KST).",
        )
        sub.add_argument(
            "--sample-policy",
            type=_parse_sample_policy,
            default=_parse_sample_policy("auto"),
            help="Sampling policy: auto (default), full, or sample.",
        )
        sub.add_argument(
            "--sample-pct",
            type=float,
            default=None,
            help="Override TABLESAMPLE percentage for expensive checks.",
        )
        sub.add_argument(
            "--query-timeout-sec",
            type=float,
            default=180.0,
            help="Per-query statement timeout in seconds (default: 180).",
        )
        sub.add_argument(
            "--no-execute",
            action="store_true",
            default=False,
            help="Skip notebook execution (write unexecuted .ipynb only).",
        )

    profile_table = profile_sub.add_parser("table", help="Profile a single catalog table.")
    profile_table.add_argument("table", help="Table name (must exist in the profile catalog).")
    profile_table.add_argument(
        "--drilldown",
        action="store_true",
        default=False,
        help="Split long-format drilldown dimension into per-value sub-profiles.",
    )
    _add_common_profile_args(profile_table)
    profile_table.set_defaults(handler=_handle_profile_table)

    profile_all = profile_sub.add_parser("all", help="Profile the catalog by weight and role.")
    profile_all.add_argument(
        "--weight",
        default="full,light",
        help="Comma-separated weights to include (default: full,light).",
    )
    profile_all.add_argument(
        "--role",
        default=None,
        help=(
            "Comma-separated table roles to include: raw, derived, reference, operational "
            "(default: all roles)."
        ),
    )
    profile_all.add_argument(
        "--drilldown",
        action="store_true",
        default=False,
        help="Split long-format drilldown dimensions into per-value sub-profiles.",
    )
    _add_common_profile_args(profile_all)
    profile_all.set_defaults(handler=_handle_profile_all)

    profile_diff = profile_sub.add_parser(
        "diff", help="Report statistical drift between two profiling runs."
    )
    profile_diff.add_argument(
        "--target",
        choices=["local", "sj2"],
        default="local",
        help="DB target whose run directory holds the manifests (default: local).",
    )
    profile_diff.add_argument(
        "--out-dir",
        default=_reports_root_default(),
        help="Output root holding run directories (default: $SDC_REPORTS_ROOT; required if unset).",
    )
    profile_diff.add_argument(
        "--against",
        default="latest",
        help="Baseline run selector: 'latest', a run-date dir, or a manifest path.",
    )
    profile_diff.add_argument(
        "--candidate",
        default="latest",
        help="Candidate run selector (default: latest). Drift output is written here.",
    )
    profile_diff.add_argument(
        "--baseline",
        default=None,
        help="Explicit baseline manifest path (overrides --against/--target).",
    )
    profile_diff.set_defaults(handler=_handle_profile_diff)

    profile_publish = profile_sub.add_parser(
        "publish", help="Copy reviewed summary artifacts from a run into docs/."
    )
    profile_publish.add_argument(
        "--target",
        choices=["local", "sj2"],
        default="local",
        help="DB target whose run to publish (default: local).",
    )
    profile_publish.add_argument(
        "--out-dir",
        default=_reports_root_default(),
        help="Output root holding run directories (default: $SDC_REPORTS_ROOT; required if unset).",
    )
    profile_publish.add_argument(
        "--run-id",
        default=None,
        help="Run-date directory to publish (default: the latest symlink).",
    )
    profile_publish.add_argument(
        "--docs-dir",
        default=None,
        help="Override the docs destination root.",
    )
    profile_publish.set_defaults(handler=_handle_profile_publish)

    # -- validate -------------------------------------------------------------
    validate_parser = subparsers.add_parser("validate", help="Run data-quality validations.")
    validate_parser.add_argument(
        "--date",
        type=_parse_date,
        default=None,
        help="Target date (YYYY-MM-DD). Default: today (KST).",
    )
    validate_parser.add_argument(
        "--market",
        choices=["kospi", "kosdaq", "all"],
        default="all",
        help="Market filter (default: all).",
    )
    validate_parser.add_argument(
        "--universe-drift-pct",
        type=float,
        default=5.0,
        help=(
            "Alert when consecutive snapshot sizes change by more than this "
            "percentage, within a source. 0 disables the check."
        ),
    )
    validate_parser.set_defaults(handler=_handle_validate)

    return parser


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    """Parse CLI arguments, configure logging, and dispatch to handler.

    Args:
        argv: Argument list (defaults to ``sys.argv[1:]``).
    """
    settings = get_settings()
    setup_logging(
        level=settings.log_level,
        fmt=settings.log_format.value,
        log_dir=settings.log_dir,
    )

    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        args.handler(args)
    except NotImplementedError as exc:
        logger.warning("Command not yet implemented: %s", exc)
        print(f"⚠  Not implemented yet: {exc}", file=sys.stderr)
        sys.exit(1)


def dart_main(argv: list[str] | None = None) -> None:
    """Entrypoint for the ``dart`` console script."""
    main(["dart", *(sys.argv[1:] if argv is None else argv)])


if __name__ == "__main__":
    main()
