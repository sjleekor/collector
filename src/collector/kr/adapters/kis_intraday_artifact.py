"""Immutable internal KIS opening observations and fail-closed briefing mapper."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

from collector.kr.adapters.kis_intraday import (
    SEOUL,
    KisIntradayMarketProvider,
)
from collector.kr.adapters.kis_intraday_timed import KisTimedMarketProvider, SelectedSector


def _bytes(value: dict[str, Any]) -> bytes:
    body = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return (body + "\n").encode()


def _aware(value: str) -> datetime:
    instant = datetime.fromisoformat(value)
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("opening timestamps must include a timezone")
    return instant.astimezone(SEOUL)


def publish_slot_snapshot(*, snapshot: dict[str, Any], report_date: date,
                          slot_label: str, output_root: Path) -> Path:
    """Store a private, byte-addressed snapshot without replacing existing data."""
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789_-"
    if not slot_label or any(char not in allowed for char in slot_label):
        raise ValueError("slot_label must use lowercase letters, digits, underscore or hyphen")
    received_at = _aware(str(snapshot["received_at"]))
    if received_at.date() != report_date:
        raise ValueError("KIS snapshot received_at differs from report_date")
    raw = _bytes(snapshot)
    digest = hashlib.sha256(raw).hexdigest()
    directory = output_root / f"report_date={report_date.isoformat()}"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"slot-{slot_label}-{digest[:16]}.json"
    if target.exists():
        if target.read_bytes() != raw:
            raise FileExistsError("slot snapshot digest collision")
        return target
    descriptor, name = tempfile.mkstemp(prefix=".slot-", dir=directory)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(name, target)
    finally:
        Path(name).unlink(missing_ok=True)
    return target


def _live_provider(
    *, timed: bool = False, selected_sectors: tuple[SelectedSector, ...] = ()
) -> KisIntradayMarketProvider | KisTimedMarketProvider:
    from collector.kr.adapters.kis_common.client import KisClient
    from collector.kr.adapters.kis_common.token import KisTokenCache, KisTokenProvider
    from collector.kr.infra.config.settings import get_settings
    from collector.kr.util.rate_limit import TokenBucket

    settings = get_settings()
    if not settings.kis_app_key or not settings.kis_app_secret:
        raise RuntimeError("KIS credentials are not configured")
    token = KisTokenProvider(
        app_key=settings.kis_app_key, app_secret=settings.kis_app_secret,
        base_url=settings.kis_base_url,
        cache=KisTokenCache(settings.kis_token_cache_path,
                            refresh_margin_seconds=settings.kis_token_refresh_margin_seconds),
        timeout_seconds=settings.kis_timeout_seconds,
    )
    client = KisClient(
        token_provider=token, app_key=settings.kis_app_key,
        app_secret=settings.kis_app_secret, base_url=settings.kis_base_url,
        bucket=TokenBucket(settings.kis_requests_per_second,
                           burst=settings.kis_max_burst_requests),
        timeout_seconds=settings.kis_timeout_seconds,
    )
    if timed:
        return KisTimedMarketProvider(client=client, selected_sectors=selected_sectors)
    return KisIntradayMarketProvider(client=client)


def _sector_spec(value: str) -> SelectedSector:
    parts = value.split(":", 2)
    if len(parts) != 3 or not all(parts):
        raise argparse.ArgumentTypeError("sector must be MARKET:CODE:NAME")
    return SelectedSector(parts[0], parts[1], parts[2])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-date", type=date.fromisoformat, required=True)
    parser.add_argument("--slot-label", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--provider", choices=("current", "timed"), default="current")
    parser.add_argument("--sector", action="append", type=_sector_spec, default=[],
                        help="timed provider의 명시 선택 업종 MARKET:CODE:NAME (최대 4개)")
    args = parser.parse_args(argv)
    if args.provider == "timed":
        if not args.sector:
            parser.error("timed provider requires at least one --sector for opening industries")
        snapshot = _live_provider(
            timed=True, selected_sectors=tuple(args.sector)
        ).fetch_snapshot(args.report_date)
    else:
        if args.sector:
            parser.error("--sector is only valid with --provider timed")
        snapshot = _live_provider().fetch_snapshot()
    path = publish_slot_snapshot(snapshot=snapshot, report_date=args.report_date,
                                 slot_label=args.slot_label, output_root=args.output_root)
    print(json.dumps({"path": str(path), "status": snapshot["status"],
                      "observation_count": len(snapshot["observations"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
