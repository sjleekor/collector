from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from collector.kr.adapters.kis_intraday import (
    INDEX_PATH,
    INDEX_TR_ID,
    SECTOR_PATH,
    SECTOR_TR_ID,
    KisIntradayMarketProvider,
    KisResponseLike,
    assess_session_status,
    public_status_only,
)

KST = ZoneInfo("Asia/Seoul")


@dataclass
class FixtureResponse:
    body: dict
    rt_cd: str = "0"
    msg_cd: str = ""
    msg1: str = ""
    tr_cont: str = ""


@dataclass
class FakeStats:
    http_requests: int = 0
    http_retries: int = 0
    pages_fetched: int = 0
    rate_limited_responses: int = 0
    token_issued: int = 0
    token_cache_hits: int = 0
    throttle_waits: int = 0
    throttle_wait_seconds: float = 0.0


class FixtureKisClient:
    def __init__(self) -> None:
        self.calls = []
        self.stats = FakeStats()

    def get(self, path, *, tr_id, params, tr_cont="") -> KisResponseLike:
        self.calls.append((path, tr_id, params, tr_cont))
        self.stats.http_requests += 1
        self.stats.pages_fetched += 1
        self.stats.token_cache_hits += 1
        if path == INDEX_PATH:
            code = params["FID_INPUT_ISCD"]
            body = {
                "rt_cd": "0",
                "output": {
                    "bstp_nmix_prpr": "2700.12",
                    "bstp_nmix_prdy_vrss": "12.34",
                    "bstp_nmix_prdy_ctrt": "0.46",
                    "hts_kor_isnm": f"index-{code}",
                },
            }
        else:
            code = params["FID_MRKT_CLS_CODE"]
            body = {
                "rt_cd": "0",
                "output1": {"summary": code},
                "output2": [
                    {
                        "bstp_cls_code": f"{code}01",
                        "hts_kor_isnm": f"sector-{code}-1",
                        "bstp_nmix_prpr": "123.45",
                        "bstp_nmix_prdy_vrss": "1.23",
                        "bstp_nmix_prdy_ctrt": "1.00",
                    },
                    {
                        "bstp_cls_code": f"{code}02",
                        "hts_kor_isnm": f"sector-{code}-2",
                        "bstp_nmix_prpr": "234.56",
                        "bstp_nmix_prdy_vrss": "-2.34",
                        "bstp_nmix_prdy_ctrt": "-0.99",
                    },
                ],
            }
        return FixtureResponse(body=body)


def test_intraday_provider_uses_four_documented_bulk_requests_and_tracks_latency() -> None:
    client = FixtureKisClient()
    ticks = iter((10.0, 13.25))
    clock = iter(
        datetime(2026, 9, 30, 9, 5, second, tzinfo=KST)
        for second in (0, 0, 0, 1, 1, 1)
    )
    provider = KisIntradayMarketProvider(
        client=client,
        now_fn=lambda: next(clock),
        monotonic_fn=lambda: next(ticks),
    )

    snapshot = provider.fetch_snapshot()

    assert len(client.calls) == 4
    assert [call[:2] for call in client.calls] == [
        (INDEX_PATH, INDEX_TR_ID),
        (INDEX_PATH, INDEX_TR_ID),
        (SECTOR_PATH, SECTOR_TR_ID),
        (SECTOR_PATH, SECTOR_TR_ID),
    ]
    assert [call[2]["FID_INPUT_ISCD"] for call in client.calls[:2]] == ["0001", "1001"]
    assert client.calls[2][2] == {
        "FID_COND_MRKT_DIV_CODE": "U",
        "FID_INPUT_ISCD": "0001",
        "FID_COND_SCR_DIV_CODE": "20214",
        "FID_MRKT_CLS_CODE": "K",
        "FID_BLNG_CLS_CODE": "0",
    }
    assert client.calls[3][2]["FID_MRKT_CLS_CODE"] == "Q"
    assert snapshot["logical_request_count"] == 4
    assert snapshot["elapsed_ms_including_retries_and_token_lookup"] == 3250
    assert snapshot["client_stat_deltas"]["http_requests"] == 4
    assert snapshot["client_stat_deltas"]["token_cache_hits"] == 4
    assert snapshot["status"] == "observation_time_unverified"
    assert len(snapshot["observations"]) == 6
    assert all(
        row["source_time_status"] == "source_timestamp_not_returned"
        for row in snapshot["observations"]
    )
    assert [row["received_at"] for row in snapshot["observations"]] == [
        datetime(2026, 9, 30, 9, 5, second, tzinfo=KST).isoformat()
        for second in (0, 0, 1, 1, 1, 1)
    ]

    public = public_status_only(snapshot)
    assert public["publication"]["status"] == "unresolved"
    assert public["index_count"] == 2
    assert public["sector_count"] == 4
    assert "observations" not in public
    assert "2700.12" not in str(public)
    assert "123.45" not in str(public)


def test_session_gate_never_confirms_closed_or_unverifiable_quotes() -> None:
    snapshot = {
        "observations": [{"source_observed_at": None}],
    }
    assert assess_session_status(snapshot, calendar_open=False) == "unavailable_closed_session"
    assert assess_session_status(snapshot, calendar_open=None) == "unavailable_calendar_unverified"
    assert assess_session_status(snapshot, calendar_open=True) == "observation_time_unverified"
    snapshot["observations"] = [{"source_observed_at": "2026-09-30T09:05:00+09:00"}]
    assert assess_session_status(
        snapshot,
        calendar_open=True,
        session_open_at=datetime(2026, 9, 30, 9, 0, tzinfo=KST),
        session_close_at=datetime(2026, 9, 30, 15, 30, tzinfo=KST),
        slot_at=datetime(2026, 9, 30, 9, 5, tzinfo=KST),
        max_age_seconds=300,
    ) == "open_session_observed"
    snapshot["observations"] = [{"source_observed_at": "2026-09-29T15:30:00+09:00"}]
    assert assess_session_status(
        snapshot,
        calendar_open=True,
        session_open_at=datetime(2026, 9, 30, 9, 0, tzinfo=KST),
        session_close_at=datetime(2026, 9, 30, 15, 30, tzinfo=KST),
        slot_at=datetime(2026, 9, 30, 9, 5, tzinfo=KST),
        max_age_seconds=300,
    ) == "observation_time_unverified"


def test_market_snapshot_requires_timezone_aware_clock() -> None:
    provider = KisIntradayMarketProvider(
        client=FixtureKisClient(), now_fn=lambda: datetime(2026, 9, 30, 9, 5)
    )
    try:
        provider.fetch_snapshot()
    except ValueError as exc:
        assert "timezone" in str(exc)
    else:
        raise AssertionError("timezone-naive market clock should be rejected")
