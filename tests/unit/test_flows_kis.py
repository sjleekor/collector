"""Unit tests for the KIS security-flow adapter.

The response fixtures are trimmed copies of live 2026-08-16 responses. The KIS
reference renders its field tables client-side, so documentation cannot be the
source of truth here — a rename upstream has to fail a test.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from collector.kr.adapters.flows_kis.parsers import (
    KisFieldError,
    parse_decimal,
    parse_foreign_holding_row,
    parse_investor_net_volume_rows,
    parse_short_selling_rows,
    parse_trade_date,
)
from collector.kr.adapters.flows_kis.provider import KIS_UNSUPPORTED_METRIC_CODES, KisFlowProvider
from collector.kr.adapters.kis_common.client import KisClient
from collector.kr.adapters.kis_common.token import (
    KisAccessToken,
    KisTokenCache,
    KisTokenProvider,
)
from collector.kr.domain.enums import Market, Source
from collector.kr.util.pipeline import SourceAuthError, SourceQuotaExhaustedError
from collector.kr.util.rate_limit import TokenBucket

KST = timezone(timedelta(hours=9))


# --- live response shapes (trimmed) -----------------------------------------

INVESTOR_ROW: dict[str, Any] = {
    "stck_bsop_date": "20260814",
    "stck_clpr": "274500",
    "acml_vol": "21669476",
    "frgn_ntby_qty": "4913433",
    "frgn_reg_ntby_qty": "4922472",
    "frgn_nreg_ntby_qty": "-9039",
    "prsn_ntby_qty": "-3049225",
    "orgn_ntby_qty": "-1830920",
    "scrt_ntby_qty": "-1390485",
    "bank_ntby_qty": "19391",
}

SHORT_SALE_ROW: dict[str, Any] = {
    "stck_bsop_date": "20260814",
    "stck_clpr": "274500",
    "acml_vol": "21669476",
    "ssts_cntg_qty": "1274200",
    "ssts_vol_rlim": "5.88",
    "acml_ssts_cntg_qty": "21302172",
    "ssts_tr_pbmn": "345781277750",
    "acml_ssts_tr_pbmn": "5223059838500",
}

INQUIRE_PRICE_OUTPUT: dict[str, Any] = {
    "stck_prpr": "274500",
    "frgn_hldn_qty": "2736287683",
    "hts_frgn_ehrt": "46.80",
    "lstn_stcn": "5846278608",
    "per": "12.34",
    "pbr": "1.23",
}


class FakeResponse:
    def __init__(
        self,
        payload: dict[str, Any],
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = json.dumps(payload)

    def json(self) -> dict[str, Any]:
        return self._payload


class FakeSession:
    """Records every call so tests can assert on real request counts."""

    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []
        self.posts: list[dict[str, Any]] = []

    def get(self, url: str, *, params: dict[str, str], headers: dict[str, str], timeout: float):
        self.calls.append({"url": url, "params": dict(params), "headers": dict(headers)})
        if not self._responses:
            raise AssertionError(f"unexpected extra KIS request: {url} {params}")
        return self._responses.pop(0)

    def post(self, url: str, *, json: dict[str, Any], headers: dict[str, str], timeout: float):
        self.posts.append({"url": url, "json": dict(json)})
        return FakeResponse({"access_token": "issued-token", "expires_in": 86400})


def _ok(body: dict[str, Any], *, tr_cont: str = "") -> FakeResponse:
    payload = {"rt_cd": "0", "msg_cd": "MCA00000", "msg1": "정상처리 되었습니다.", **body}
    return FakeResponse(payload, headers={"tr_cont": tr_cont})


def _build_provider(responses: list[FakeResponse]) -> tuple[KisFlowProvider, FakeSession]:
    session = FakeSession(responses)
    token_provider = KisTokenProvider(
        app_key="key",
        app_secret="secret",
        base_url="https://kis.test",
        cache=_StubCache(KisAccessToken("cached-token", datetime.now(KST) + timedelta(hours=12))),
        session=session,
    )
    client = KisClient(
        token_provider=token_provider,
        app_key="key",
        app_secret="secret",
        base_url="https://kis.test",
        bucket=TokenBucket(0),
        session=session,
        sleep_fn=lambda _seconds: None,
    )
    return KisFlowProvider(client=client), session


class _StubCache:
    def __init__(self, token: KisAccessToken | None) -> None:
        self.token = token
        self.stored: list[KisAccessToken] = []
        self.path = "stub"

    def load(self) -> KisAccessToken | None:
        return self.token

    def store(self, token: KisAccessToken) -> None:
        self.stored.append(token)
        self.token = token


# --- parsers -----------------------------------------------------------------


def test_investor_rows_map_the_three_canonical_metrics() -> None:
    records = parse_investor_net_volume_rows(
        [INVESTOR_ROW],
        ticker="005930",
        market=Market.KOSPI,
        start=date(2026, 8, 1),
        end=date(2026, 8, 14),
        request={"FID_INPUT_DATE_1": "20260814"},
    )

    by_metric = {record.metric_code: record for record in records}
    assert set(by_metric) == {
        "individual_net_buy_volume",
        "foreign_net_buy_volume",
        "institution_net_buy_volume",
    }
    assert by_metric["individual_net_buy_volume"].value == Decimal("-3049225")
    assert by_metric["institution_net_buy_volume"].value == Decimal("-1830920")
    # KIS reports registered + unregistered foreign combined, which is the same
    # definition as the KRX path's 외국인 + 기타외국인 sum.
    assert by_metric["foreign_net_buy_volume"].value == Decimal("4913433")
    assert by_metric["foreign_net_buy_volume"].value == Decimal(
        INVESTOR_ROW["frgn_reg_ntby_qty"]
    ) + Decimal(INVESTOR_ROW["frgn_nreg_ntby_qty"])
    assert all(record.source is Source.KIS for record in records)
    assert all(record.trade_date == date(2026, 8, 14) for record in records)


def test_the_institution_breakdown_is_not_collected() -> None:
    # Decision (a): replacement first. The 101-field breakdown is an expansion,
    # and mixing it in would make any regression impossible to attribute.
    records = parse_investor_net_volume_rows(
        [INVESTOR_ROW],
        ticker="005930",
        market=Market.KOSPI,
        start=date(2026, 8, 1),
        end=date(2026, 8, 14),
        request={},
    )
    assert not any(
        "scrt" in record.metric_code or "bank" in record.metric_code for record in records
    )
    assert len(records) == 3


def test_rows_outside_the_requested_window_are_dropped() -> None:
    older = {**INVESTOR_ROW, "stck_bsop_date": "20260701"}
    records = parse_investor_net_volume_rows(
        [INVESTOR_ROW, older],
        ticker="005930",
        market=Market.KOSPI,
        start=date(2026, 8, 1),
        end=date(2026, 8, 14),
        request={},
    )
    assert {record.trade_date for record in records} == {date(2026, 8, 14)}


def test_short_sale_rows_map_volume_and_value_only() -> None:
    records = parse_short_selling_rows(
        [SHORT_SALE_ROW],
        ticker="005930",
        market=Market.KOSPI,
        start=date(2026, 8, 1),
        end=date(2026, 8, 14),
        request={},
    )
    by_metric = {record.metric_code: record.value for record in records}
    assert by_metric == {
        "short_selling_volume": Decimal("1274200"),
        "short_selling_value": Decimal("345781277750"),
    }
    # The balance is a KRX-only series; nothing here may invent it.
    assert "short_selling_balance_quantity" not in by_metric


def test_foreign_holding_uses_the_direct_field_not_the_ratio() -> None:
    records = parse_foreign_holding_row(
        INQUIRE_PRICE_OUTPUT,
        ticker="005930",
        market=Market.KOSPI,
        trade_date=date(2026, 8, 14),
        request={},
    )
    assert len(records) == 1
    assert records[0].metric_code == "foreign_holding_shares"
    assert records[0].value == Decimal("2736287683")
    # Multiplying 상장주식수 x 소진율 would land near but not on the real figure.
    ratio_estimate = Decimal("5846278608") * Decimal("46.80") / Decimal(100)
    assert records[0].value != ratio_estimate


def test_a_renamed_response_field_fails_loudly() -> None:
    renamed = {"stck_bsop_date": "20260814", "prsn_ntby_quantity": "1"}
    with pytest.raises(KisFieldError):
        parse_investor_net_volume_rows(
            [renamed],
            ticker="005930",
            market=Market.KOSPI,
            start=date(2026, 8, 1),
            end=date(2026, 8, 14),
            request={},
        )

    with pytest.raises(KisFieldError):
        parse_foreign_holding_row(
            {"hts_frgn_ehrt": "46.80"},
            ticker="005930",
            market=Market.KOSPI,
            trade_date=date(2026, 8, 14),
            request={},
        )


def test_numeric_and_date_parsing_tolerates_blanks() -> None:
    assert parse_decimal("") is None
    assert parse_decimal("-") is None
    assert parse_decimal("1,234") == Decimal("1234")
    assert parse_trade_date("") is None
    assert parse_trade_date("not-a-date") is None
    assert parse_trade_date("20260814") == date(2026, 8, 14)


# --- provider paging ---------------------------------------------------------


def _investor_page(dates: list[str]) -> FakeResponse:
    return _ok({"output2": [{**INVESTOR_ROW, "stck_bsop_date": day} for day in dates]})


def test_investor_paging_walks_backwards_until_the_window_is_covered() -> None:
    # 30 rows per call means a 45-session window needs two calls, and the second
    # must end one day before the oldest row of the first.
    first_page = [f"202607{day:02d}" for day in range(30, 0, -1)]
    second_page = ["20260629", "20260628"]
    provider, session = _build_provider([_investor_page(first_page), _investor_page(second_page)])

    result = provider.fetch_investor_net_volume(
        ticker="005930",
        market=Market.KOSPI,
        start=date(2026, 6, 28),
        end=date(2026, 7, 30),
    )

    assert result.error is None
    assert len(session.calls) == 2
    assert session.calls[0]["params"]["FID_INPUT_DATE_1"] == "20260730"
    assert session.calls[1]["params"]["FID_INPUT_DATE_1"] == "20260630"
    assert provider.request_stats().http_requests == 2
    assert provider.request_stats().pages_fetched == 2


def test_a_short_page_stops_paging() -> None:
    provider, session = _build_provider([_investor_page(["20260814", "20260813"])])

    result = provider.fetch_investor_net_volume(
        ticker="005930",
        market=Market.KOSPI,
        start=date(2020, 1, 1),
        end=date(2026, 8, 14),
    )

    assert result.error is None
    assert len(session.calls) == 1


def test_short_sale_paging_pins_the_lower_bound_and_moves_the_upper() -> None:
    first_page = [f"202606{day:02d}" for day in range(30, 0, -1)] + [
        f"202605{day:02d}" for day in range(31, 0, -1)
    ]
    first_page = first_page[:100] if len(first_page) >= 100 else first_page
    provider, session = _build_provider(
        [_ok({"output2": [{**SHORT_SALE_ROW, "stck_bsop_date": day} for day in first_page]})]
    )

    provider.fetch_shorting_metrics(
        ticker="005930",
        market=Market.KOSPI,
        start=date(2026, 5, 1),
        end=date(2026, 6, 30),
    )

    assert session.calls[0]["params"]["FID_INPUT_DATE_1"] == "20260501"
    assert session.calls[0]["params"]["FID_INPUT_DATE_2"] == "20260630"


def test_foreign_holding_is_one_call_per_ticker() -> None:
    provider, session = _build_provider([_ok({"output": INQUIRE_PRICE_OUTPUT})])

    result = provider.fetch_foreign_holding(
        ticker="005930",
        market=Market.KOSPI,
        trade_date=date(2026, 8, 14),
    )

    assert len(result.records) == 1
    assert len(session.calls) == 1
    assert session.calls[0]["headers"]["tr_id"] == "FHKST01010100"


def test_the_balance_metric_is_declared_unsupported() -> None:
    provider, _ = _build_provider([])
    assert provider.unsupported_metric_codes() == ["short_selling_balance_quantity"]
    assert KIS_UNSUPPORTED_METRIC_CODES == ["short_selling_balance_quantity"]
    assert provider.source() is Source.KIS


# --- client: auth, quota, counters ------------------------------------------


def test_auth_and_quota_failures_raise_instead_of_returning_an_error() -> None:
    # A per-item error here would walk the whole target list against a server
    # already refusing, and each recovery costs a token issuance.
    rejected = FakeResponse(
        {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "token invalid"}, status_code=200
    )
    provider, _ = _build_provider([rejected, rejected])
    with pytest.raises(SourceAuthError):
        provider.fetch_foreign_holding(
            ticker="005930", market=Market.KOSPI, trade_date=date(2026, 8, 14)
        )


def test_sustained_rate_limiting_stops_the_run() -> None:
    limited = FakeResponse(
        {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다."}
    )
    session = FakeSession([limited] * 30)
    client = KisClient(
        token_provider=KisTokenProvider(
            app_key="key",
            app_secret="secret",
            base_url="https://kis.test",
            cache=_StubCache(
                KisAccessToken("cached-token", datetime.now(KST) + timedelta(hours=12))
            ),
            session=session,
        ),
        app_key="key",
        app_secret="secret",
        base_url="https://kis.test",
        bucket=TokenBucket(0),
        session=session,
        max_attempts=5,
        max_consecutive_rate_limits=3,
        sleep_fn=lambda _seconds: None,
    )

    with pytest.raises(SourceQuotaExhaustedError):
        client.get("/probe", tr_id="TR", params={})
    assert client.stats.rate_limited_responses >= 3


def test_transient_server_errors_are_retried_and_counted() -> None:
    session = FakeSession(
        [
            FakeResponse({"error": "boom"}, status_code=500),
            _ok({"output": INQUIRE_PRICE_OUTPUT}),
        ]
    )
    client = KisClient(
        token_provider=KisTokenProvider(
            app_key="key",
            app_secret="secret",
            base_url="https://kis.test",
            cache=_StubCache(
                KisAccessToken("cached-token", datetime.now(KST) + timedelta(hours=12))
            ),
            session=session,
        ),
        app_key="key",
        app_secret="secret",
        base_url="https://kis.test",
        bucket=TokenBucket(0),
        session=session,
        sleep_fn=lambda _seconds: None,
    )

    response = client.get("/probe", tr_id="TR", params={})

    assert response.ok
    # Two real calls for one logical request: the exact gap that made
    # `requests_attempted` unusable as a quota audit.
    assert client.stats.http_requests == 2
    assert client.stats.http_retries == 1
    assert client.stats.pages_fetched == 1
    assert client.stats.status_counts == {"500": 1, "200": 1}


def test_request_stats_flatten_into_run_counts() -> None:
    provider, _ = _build_provider([_ok({"output": INQUIRE_PRICE_OUTPUT})])
    provider.fetch_foreign_holding(
        ticker="005930", market=Market.KOSPI, trade_date=date(2026, 8, 14)
    )
    counts = provider.request_stats().as_counts()
    assert counts["http_requests"] == 1
    assert counts["http_status_200"] == 1
    assert counts["auth_token_issued"] == 0
    assert counts["auth_token_cache_hits"] == 1


# --- token cache -------------------------------------------------------------


def test_a_warm_cache_never_issues_a_token(tmp_path) -> None:
    # Every issuance sends the account holder a notification, and collectors run
    # as fresh containers. This is the whole reason the cache exists.
    cache_path = tmp_path / "kis_token.json"
    cache = KisTokenCache(cache_path, refresh_margin_seconds=600)
    cache.store(KisAccessToken("warm-token", datetime.now(KST) + timedelta(hours=12)))

    session = FakeSession([])
    provider = KisTokenProvider(
        app_key="key",
        app_secret="secret",
        base_url="https://kis.test",
        cache=KisTokenCache(cache_path, refresh_margin_seconds=600),
        session=session,
    )

    assert provider.token() == "warm-token"
    assert provider.token() == "warm-token"
    assert session.posts == []
    assert provider.issued_count == 0


def test_the_cache_file_is_owner_only(tmp_path) -> None:
    cache_path = tmp_path / "kis_token.json"
    KisTokenCache(cache_path).store(
        KisAccessToken("secret-token", datetime.now(KST) + timedelta(hours=12))
    )
    assert cache_path.stat().st_mode & 0o777 == 0o600


def test_an_expiring_token_is_reissued_and_written_back(tmp_path) -> None:
    cache_path = tmp_path / "kis_token.json"
    KisTokenCache(cache_path, refresh_margin_seconds=3600).store(
        KisAccessToken("stale-token", datetime.now(KST) + timedelta(minutes=5))
    )

    session = FakeSession([])
    provider = KisTokenProvider(
        app_key="key",
        app_secret="secret",
        base_url="https://kis.test",
        cache=KisTokenCache(cache_path, refresh_margin_seconds=3600),
        session=session,
    )

    assert provider.token() == "issued-token"
    assert provider.issued_count == 1
    assert json.loads(cache_path.read_text())["access_token"] == "issued-token"


def test_a_corrupt_cache_is_ignored_rather_than_fatal(tmp_path) -> None:
    cache_path = tmp_path / "kis_token.json"
    cache_path.write_text("{not json", encoding="utf-8")
    assert KisTokenCache(cache_path).load() is None


def test_a_float_timestamp_cache_still_loads(tmp_path) -> None:
    # The probe scripts that established the endpoints wrote epoch seconds.
    cache_path = tmp_path / "kis_token.json"
    cache_path.write_text(
        json.dumps(
            {
                "access_token": "probe-token",
                "expires_at": (datetime.now(KST) + timedelta(hours=10)).timestamp(),
            }
        ),
        encoding="utf-8",
    )
    loaded = KisTokenCache(cache_path).load()
    assert loaded is not None
    assert loaded.access_token == "probe-token"


def test_missing_credentials_fail_before_any_request() -> None:
    provider = KisTokenProvider(
        app_key="",
        app_secret="",
        base_url="https://kis.test",
        cache=_StubCache(None),
        session=FakeSession([]),
    )
    with pytest.raises(SourceAuthError):
        provider.token()


# --- token bucket ------------------------------------------------------------


def test_the_token_bucket_paces_to_the_published_quota() -> None:
    clock = {"now": 0.0}
    slept: list[float] = []

    def sleep_fn(seconds: float) -> None:
        slept.append(seconds)
        clock["now"] += seconds

    bucket = TokenBucket(
        10.0,
        burst=1,
        sleep_fn=sleep_fn,
        monotonic_fn=lambda: clock["now"],
    )

    for _ in range(4):
        bucket.acquire()

    # One burst token, then 0.1s per call at 10/s.
    assert slept == pytest.approx([0.1, 0.1, 0.1])
    assert bucket.waits == 3


def test_a_zero_rate_disables_pacing() -> None:
    bucket = TokenBucket(0, sleep_fn=lambda _seconds: pytest.fail("must not sleep"))
    bucket.acquire()
    assert bucket.enabled is False


def test_a_rate_limit_disguised_as_http_500_is_still_a_rate_limit() -> None:
    # Measured live on 2026-08-16: KIS answers "초당 거래건수를 초과하였습니다"
    # with HTTP 500, not 429. Branching on the status alone files every throttle
    # rejection as a generic server error, leaving the rate-limit counter at
    # zero and the quota breaker unable to fire.
    throttled = FakeResponse(
        {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "초당 거래건수를 초과하였습니다."},
        status_code=500,
    )
    session = FakeSession([throttled, _ok({"output": INQUIRE_PRICE_OUTPUT})])
    client = KisClient(
        token_provider=KisTokenProvider(
            app_key="key",
            app_secret="secret",
            base_url="https://kis.test",
            cache=_StubCache(
                KisAccessToken("cached-token", datetime.now(KST) + timedelta(hours=12))
            ),
            session=session,
        ),
        app_key="key",
        app_secret="secret",
        base_url="https://kis.test",
        bucket=TokenBucket(0),
        session=session,
        sleep_fn=lambda _seconds: None,
    )

    response = client.get("/probe", tr_id="TR", params={})

    assert response.ok
    assert client.stats.rate_limited_responses == 1
    assert client.stats.status_counts == {"500": 1, "200": 1}


def test_an_auth_rejection_carried_on_a_500_still_stops_the_run() -> None:
    rejected = FakeResponse(
        {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "token invalid"}, status_code=500
    )
    provider, _ = _build_provider([rejected, rejected])
    with pytest.raises(SourceAuthError):
        provider.fetch_foreign_holding(
            ticker="005930", market=Market.KOSPI, trade_date=date(2026, 8, 14)
        )


def test_the_measured_default_rate_is_what_the_account_actually_sustains() -> None:
    # KIS documents 20/s per live account. This one clears 20 consecutive calls
    # at 1.0/s and starts refusing at 1.2/s, and effective throughput never
    # exceeded ~1.1/s at any rate tried, so a higher setting buys only retries.
    from collector.kr.infra.config.settings import DEFAULT_KIS_REQUESTS_PER_SECOND, Settings

    assert DEFAULT_KIS_REQUESTS_PER_SECOND == 1.0
    assert Settings().kis_max_burst_requests == 1


def test_a_publication_lag_is_no_data_not_a_shape_change() -> None:
    # Measured on prod 2026-08-18 at 20:5x KST: KIS had that session's investor
    # breakdown for 005930 but not yet for 000300, 000880, 00088K, 001470 or
    # 001570, whose newest row was still 08-14. Asking those five for a single
    # recent day returns 30 perfectly good rows, none inside the window.
    #
    # Counting field matches only inside the window read that as "the response
    # shape changed", which tripped the consecutive-failure guard and stopped
    # the whole changeover run on its first five tickers.
    rows = [
        {
            "stck_bsop_date": "20260814",
            "prsn_ntby_qty": "410513",
            "frgn_ntby_qty": "577859",
            "orgn_ntby_qty": "-941461",
        }
    ]

    records = parse_investor_net_volume_rows(
        rows,
        ticker="000300",
        market=Market.KOSPI,
        start=date(2026, 8, 18),
        end=date(2026, 8, 18),
        request={},
    )

    assert records == []


def test_a_genuine_rename_still_raises() -> None:
    # The guard has to keep working: a silent rename would otherwise look like
    # a run that legitimately found nothing, and forward-fill collectors are
    # exactly where that goes unnoticed.
    rows = [{"stck_bsop_date": "20260818", "renamed_ntby_qty": "1"}]

    with pytest.raises(KisFieldError):
        parse_investor_net_volume_rows(
            rows,
            ticker="000300",
            market=Market.KOSPI,
            start=date(2026, 8, 18),
            end=date(2026, 8, 18),
            request={},
        )


def test_a_rename_is_caught_even_when_every_row_is_out_of_window() -> None:
    # Field presence is judged per row, not per in-window row, so the check
    # survives the case that used to mask it.
    rows = [{"stck_bsop_date": "20260814", "renamed_ntby_qty": "1"}]

    with pytest.raises(KisFieldError):
        parse_investor_net_volume_rows(
            rows,
            ticker="000300",
            market=Market.KOSPI,
            start=date(2026, 8, 18),
            end=date(2026, 8, 18),
            request={},
        )


def test_rows_outside_the_window_are_not_reported_as_no_data() -> None:
    # The no-data tombstone key is `group:ticker` with no date in it, so one
    # no-data verdict suspends that ticker for the whole TTL (7 days by
    # default). This endpoint answers with the ticker's most recent TRADED
    # sessions, so a halted ticker returns thirty good rows, all older than the
    # window. Measured on prod 2026-08-18: 000880 and four others ended at
    # 08-14, with volume zero and a frozen price since 08-10.
    #
    # A halted ticker is exactly the one whose resumption must not be missed.
    provider, _session = _build_provider(
        [
            _ok(
                {
                    "output2": [
                        {
                            "stck_bsop_date": "20260814",
                            "prsn_ntby_qty": "410513",
                            "frgn_ntby_qty": "577859",
                            "orgn_ntby_qty": "-941461",
                        }
                    ]
                }
            )
        ]
    )

    result = provider.fetch_investor_net_volume(
        ticker="000300",
        market=Market.KOSPI,
        start=date(2026, 8, 18),
        end=date(2026, 8, 18),
    )

    assert result.records == []
    assert result.no_data is False
    assert result.error is None


def test_an_empty_response_is_still_no_data() -> None:
    # The tombstone keeps its purpose: a ticker upstream genuinely has nothing
    # for must not be re-requested on every run.
    provider, _session = _build_provider([_ok({"output2": []})])

    result = provider.fetch_investor_net_volume(
        ticker="000300",
        market=Market.KOSPI,
        start=date(2026, 8, 18),
        end=date(2026, 8, 18),
    )

    assert result.records == []
    assert result.no_data is True


def test_a_halted_ticker_is_retried_rather_than_suspended() -> None:
    # The full path the fix protects: rows exist, none in window, so the run
    # writes nothing and tombstones nothing. Left as no-data the ticker would
    # be skipped for the TTL, and it would resume trading inside that gap.
    provider, _session = _build_provider(
        [
            _ok(
                {
                    "output2": [
                        {
                            "stck_bsop_date": "20260814",
                            "prsn_ntby_qty": "1",
                            "frgn_ntby_qty": "2",
                            "orgn_ntby_qty": "3",
                        },
                        {
                            "stck_bsop_date": "20260813",
                            "prsn_ntby_qty": "4",
                            "frgn_ntby_qty": "5",
                            "orgn_ntby_qty": "6",
                        },
                    ]
                }
            )
        ]
    )

    result = provider.fetch_investor_net_volume(
        ticker="000880",
        market=Market.KOSPI,
        start=date(2026, 8, 18),
        end=date(2026, 8, 18),
    )

    assert (result.records, result.no_data, result.error) == ([], False, None)
