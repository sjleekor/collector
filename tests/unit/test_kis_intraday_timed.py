from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from collector.kr.adapters.kis_intraday import assess_session_status
from collector.kr.adapters.kis_intraday_timed import (
    MINUTE_INDEX_PATH,
    MINUTE_INDEX_TR_ID,
    KisTimedMarketProvider,
    SelectedSector,
    parse_minute_index_response,
)

KST = ZoneInfo("Asia/Seoul")
D = date(2026, 9, 30)


@dataclass
class Response:
    body: dict


class FakeClient:
    def __init__(self) -> None:
        self.calls = []

    def get(self, path, *, tr_id, params, tr_cont=""):
        self.calls.append((path, tr_id, params))
        return Response({"output2": [
            {"stck_bsop_date": "20260929", "stck_cntg_hour": "153000",
             "bstp_nmix_prpr": "9999"},
            {"stck_bsop_date": "20260930", "stck_cntg_hour": "090200",
             "bstp_nmix_prpr": "2700.12"},
            {"stck_bsop_date": "20260930", "stck_cntg_hour": "090500",
             "bstp_nmix_prpr": "2701.34"},
            {"stck_bsop_date": "20260930", "stck_cntg_hour": "091000",
             "bstp_nmix_prpr": "2702.00"},
        ]})


def test_timed_provider_uses_source_sample_and_bounded_documented_calls():
    client = FakeClient()
    provider = KisTimedMarketProvider(
        client=client,
        selected_sectors=(SelectedSector("KRX", "0002", "Large cap"),),
        now_fn=lambda: datetime(2026, 9, 30, 9, 6, tzinfo=KST),
        monotonic_fn=lambda: 1.0,
    )
    snapshot = provider.fetch_snapshot(D)
    assert len(client.calls) == snapshot["logical_request_count"] == 3
    assert all((path, tr_id) == (MINUTE_INDEX_PATH, MINUTE_INDEX_TR_ID)
               for path, tr_id, _ in client.calls)
    assert [call[2]["FID_INPUT_ISCD"] for call in client.calls] == ["0001", "1001", "0002"]
    assert client.calls[0][2]["FID_PW_DATA_INCU_YN"] == "N"
    assert snapshot["bar_semantics"] == "intraday_sample_not_completed_minute_bar"
    assert [row["source_observed_at"] for row in snapshot["observations"]] == [
        "2026-09-30T09:05:00+09:00"] * 3
    assert all(row["price"] == "2701.34" and row["change_percent"] is None
               for row in snapshot["observations"])
    assert assess_session_status(
        snapshot, calendar_open=True,
        session_open_at=datetime(2026, 9, 30, 9, tzinfo=KST),
        session_close_at=datetime(2026, 9, 30, 15, 30, tzinfo=KST),
        slot_at=datetime(2026, 9, 30, 9, 6, tzinfo=KST),
        max_age_seconds=120,
    ) == "open_session_observed"


@pytest.mark.parametrize("rows", [
    [{"stck_bsop_date": "20260929", "stck_cntg_hour": "153000", "bstp_nmix_prpr": "1"}],
    [{"stck_bsop_date": "20260930", "stck_cntg_hour": "091000", "bstp_nmix_prpr": "1"}],
    [{"stck_bsop_date": "20260930", "stck_cntg_hour": "090500", "bstp_nmix_prpr": "NaN"}],
    [{"bstp_nmix_prpr": "1"}],
])
def test_timed_parser_rejects_previous_day_future_or_missing_value(rows):
    with pytest.raises(ValueError, match="no same-day"):
        parse_minute_index_response(
            kind="index", market="KOSPI", code="0001", name="KOSPI",
            report_date=D, body={"output2": rows},
            received_at=datetime(2026, 9, 30, 9, 6, tzinfo=KST),
        )


def test_timed_provider_limits_sector_requests():
    with pytest.raises(ValueError, match="bounded request"):
        KisTimedMarketProvider(
            client=FakeClient(),
            selected_sectors=tuple(SelectedSector("KRX", f"{n:04d}", str(n))
                                   for n in range(2, 7)),
        )
