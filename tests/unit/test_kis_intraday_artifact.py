"""KIS private opening artifacts never use HTTP receipt as exchange sample time."""

from __future__ import annotations

import datetime as dt

from collector.kr.adapters.kis_intraday_artifact import publish_slot_snapshot


def _calendar(confirmed=True):
    return {
        "market": "KR", "timezone": "Asia/Seoul",
        "coverage_start": "2026-09-30", "coverage_end": "2026-09-30",
        "default_open_at": "09:00:00", "default_close_at": "15:30:00",
        "sessions": ["2026-09-30"], "overrides": [],
        "unconfirmed_dates": [] if confirmed else ["2026-09-30"],
    }


def _snapshot(source_time=None):
    return {
        "market": "KR", "status": "observation_time_unverified",
        "received_at": "2026-09-30T09:30:05+09:00",
        "observations": [
            {"kind": "index", "market": "KOSPI", "code": "0001",
             "name": "KOSPI", "price": "3210.4", "change_percent": "0.3",
             "source_observed_at": source_time},
            {"kind": "sector", "market": "KRX", "code": "101",
             "name": "Sector", "price": "111.2", "change_percent": "0.1",
             "source_observed_at": source_time},
        ],
    }


def test_private_slot_publishes_immutable(tmp_path):
    day = dt.date(2026, 9, 30)
    snapshot = _snapshot()
    slot = publish_slot_snapshot(snapshot=snapshot, report_date=day,
                                 slot_label="0930", output_root=tmp_path)
    assert slot.name.startswith("slot-0930-")
    assert publish_slot_snapshot(snapshot=snapshot, report_date=day,
                                 slot_label="0930", output_root=tmp_path) == slot
