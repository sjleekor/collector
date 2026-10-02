"""Receipt-driven retry of OpenDART no-data verdicts (periodic reports).

Pins the receipt -> slot mapping (December and non-December fiscal years,
correction prefixes, extension notices), the retry window, the cap, the
availability-gate bypass, and that the services only override an unexpired
no-data verdict, never a success.
"""

from __future__ import annotations

from datetime import date, timedelta

from collector.kr.domain.enums import SliceStatus, Source, UniverseScope
from collector.kr.domain.models import CollectionSliceState, DartPeriodicReceipt
from collector.kr.service.dart_receipt_retry import (
    ReceiptRetryPlan,
    build_receipt_retry_plan,
    parse_periodic_title,
    receipt_to_slots,
    slot_allowed,
)
from collector.kr.service.sync_dart_financials import sync_dart_financial_statements
from collector.kr.service.sync_dart_share_info import sync_dart_share_info
from collector.kr.service.sync_dart_xbrl import sync_dart_xbrl
from collector.kr.util.slice_ledger import SliceLedger
from collector.kr.util.time import now_kst
from tests.unit.test_opendart_financials import MockFinancialProvider, MockFinancialStorage
from tests.unit.test_opendart_share_info import MockShareInfoProvider, MockShareInfoStorage
from tests.unit.test_opendart_xbrl import MockXbrlProvider, MockXbrlStorage

CORP = "00126380"


# --------------------------------------------------------------------------
# title parsing and receipt -> slot mapping
# --------------------------------------------------------------------------


def test_december_fiscal_year_titles_map_to_the_report_codes() -> None:
    assert receipt_to_slots("반기보고서 (2026.06)", "12") == [(2026, "11012")]
    assert receipt_to_slots("분기보고서 (2026.03)", "12") == [(2026, "11013")]
    assert receipt_to_slots("분기보고서 (2026.09)", "12") == [(2026, "11014")]
    assert receipt_to_slots("사업보고서 (2025.12)", "12") == [(2025, "11011")]


def test_unknown_fiscal_year_month_is_treated_as_december() -> None:
    for acc_mt in ("", "  ", "x", "0", "13"):
        assert receipt_to_slots("분기보고서 (2026.09)", acc_mt) == [(2026, "11014")]


def test_non_december_fiscal_years_follow_months_since_year_end() -> None:
    # Fiscal year ends in March: Q1 = June, half-year = September,
    # Q3 = December, annual = March (bsns_year is the period-end calendar year).
    assert receipt_to_slots("분기보고서 (2026.06)", "03") == [(2026, "11013")]
    assert receipt_to_slots("반기보고서 (2026.09)", "03") == [(2026, "11012")]
    assert receipt_to_slots("분기보고서 (2026.12)", "03") == [(2026, "11014")]
    assert receipt_to_slots("사업보고서 (2026.03)", "03") == [(2026, "11011")]
    # Fiscal year ends in September: Q1 = December, Q3 = June.
    assert receipt_to_slots("분기보고서 (2025.12)", "09") == [(2025, "11013")]
    assert receipt_to_slots("분기보고서 (2026.06)", "09") == [(2026, "11014")]


def test_quarter_report_that_fits_neither_quarter_returns_both_candidates() -> None:
    # A stale fiscal-year month must cost one extra request, not a missed slot.
    assert receipt_to_slots("분기보고서 (2026.05)", "12") == [
        (2026, "11013"),
        (2026, "11014"),
    ]


def test_corrections_and_extension_notices_are_not_originals() -> None:
    for title in (
        "[기재정정]반기보고서 (2026.06)",
        "[첨부정정]사업보고서 (2025.12)",
        "[첨부추가]반기보고서 (2026.06)",
        "[기재정정]분기보고서 (2026.03)",
        "반기보고서제출기한연장신고서 (2026.06)",
        "기타경영사항(자율공시) (2026년 반기보고서(반기검토보고서) 제출 지연)",
        "기타시장안내              (상장공시위원회 개최기한 연장)",
        "반기보고서",
        "반기보고서 (2026.13)",
        "반기보고서 (2026.00)",
        "",
    ):
        assert parse_periodic_title(title) is None, title
        assert receipt_to_slots(title, "12") == [], title


def test_title_spacing_variants_still_parse() -> None:
    assert receipt_to_slots("반기보고서(2026.06)", "12") == [(2026, "11012")]
    assert receipt_to_slots("  반기보고서 (2026.06) ", "12") == [(2026, "11012")]


# --------------------------------------------------------------------------
# plan building: window, cap, filters
# --------------------------------------------------------------------------


class _ReceiptStorage:
    def __init__(self, receipts: list[DartPeriodicReceipt]) -> None:
        self.receipts = receipts
        self.calls: list[tuple[date, date]] = []

    def get_dart_periodic_receipts(
        self, rcept_from: date, rcept_to: date
    ) -> list[DartPeriodicReceipt]:
        self.calls.append((rcept_from, rcept_to))
        return [r for r in self.receipts if rcept_from <= r.rcept_dt <= rcept_to]


def _receipt(
    corp: str = CORP,
    name: str = "반기보고서 (2026.06)",
    dt: date = date(2026, 8, 14),
    acc_mt: str = "12",
    rcept_no: str | None = None,
) -> DartPeriodicReceipt:
    return DartPeriodicReceipt(
        corp_code=corp,
        report_nm=name,
        rcept_no=rcept_no or f"{dt:%Y%m%d}{corp[-6:]}",
        rcept_dt=dt,
        acc_mt=acc_mt,
    )


def test_retry_window_covers_day_one_through_n_and_then_stops() -> None:
    storage = _ReceiptStorage([_receipt(dt=date(2026, 8, 14))])
    receipt_day = date(2026, 8, 14)
    for offset in range(1, 8):
        plan = build_receipt_retry_plan(
            storage, as_of=receipt_day + timedelta(days=offset), window_days=7
        )
        assert (CORP, 2026, "11012") in plan.slots, offset
    after = build_receipt_retry_plan(storage, as_of=receipt_day + timedelta(days=8), window_days=7)
    assert after.slots == {}
    assert not after


def test_window_zero_disables_the_feature_without_reading_receipts() -> None:
    storage = _ReceiptStorage([_receipt()])
    plan = build_receipt_retry_plan(storage, as_of=date(2026, 8, 15), window_days=0)
    assert plan.slots == {} and storage.calls == []


def test_receipts_dated_after_as_of_are_ignored() -> None:
    storage = _ReceiptStorage([_receipt(dt=date(2026, 8, 20))])
    plan = build_receipt_retry_plan(storage, as_of=date(2026, 8, 15), window_days=7)
    assert plan.slots == {}


def test_plan_ignores_corrections_extensions_and_other_corps() -> None:
    storage = _ReceiptStorage(
        [
            _receipt(corp="00000001"),
            _receipt(corp="00000002", name="[기재정정]반기보고서 (2026.06)"),
            _receipt(corp="00000003", name="반기보고서제출기한연장신고서 (2026.06)"),
            _receipt(corp="00000009"),
        ]
    )
    plan = build_receipt_retry_plan(
        storage,
        as_of=date(2026, 8, 15),
        window_days=7,
        corp_codes={"00000001", "00000002", "00000003"},
    )
    assert set(plan.slots) == {("00000001", 2026, "11012")}
    assert plan.receipts_considered == 1


def test_plan_uses_each_corps_fiscal_year_month_for_quarter_reports() -> None:
    storage = _ReceiptStorage(
        [
            _receipt(corp="00000001", name="분기보고서 (2026.09)", acc_mt="12"),
            _receipt(corp="00000002", name="분기보고서 (2026.09)", acc_mt="06"),
        ]
    )
    plan = build_receipt_retry_plan(storage, as_of=date(2026, 8, 15), window_days=7)
    assert set(plan.slots) == {
        ("00000001", 2026, "11014"),
        ("00000002", 2026, "11013"),
    }


def test_cap_keeps_the_newest_receipts_and_reports_how_many_were_dropped() -> None:
    storage = _ReceiptStorage(
        [_receipt(corp=f"{i:08d}", dt=date(2026, 8, 10 + i % 4)) for i in range(1, 11)]
    )
    plan = build_receipt_retry_plan(storage, as_of=date(2026, 8, 15), window_days=7, max_slots=4)
    assert len(plan.slots) == 4
    assert plan.slots_capped == 6
    assert plan.audit_params()["receipt_retry_slots_capped"] == 6
    kept_dates = sorted(plan.slots.values(), reverse=True)
    all_dates = sorted((r.rcept_dt for r in storage.receipts), reverse=True)
    assert kept_dates == all_dates[:4]


def test_rank_keys_orders_newest_receipt_first_and_skips_unconfirmed() -> None:
    plan = ReceiptRetryPlan(
        slots={
            ("A", 2026, "11012"): date(2026, 8, 12),
            ("B", 2026, "11012"): date(2026, 8, 14),
        },
        window_days=7,
    )
    keys = plan.rank_keys(
        [
            ("a-key", ("A", 2026, "11012")),
            ("b-key", ("B", 2026, "11012")),
            ("c-key", ("C", 2026, "11012")),
        ]
    )
    assert keys == ["b-key", "a-key"]


def test_slot_allowed_lifts_the_gate_only_for_confirmed_slots() -> None:
    allowed = {(2026, "11013")}
    retry = {(CORP, 2026, "11012")}
    assert slot_allowed(allowed, retry, CORP, 2026, "11013")
    assert slot_allowed(allowed, retry, CORP, 2026, "11012")
    assert not slot_allowed(allowed, retry, "other", 2026, "11012")
    assert slot_allowed(None, {}, CORP, 2026, "11012")


# --------------------------------------------------------------------------
# ledger
# --------------------------------------------------------------------------


class _LedgerStorage:
    def __init__(self, states: list[CollectionSliceState]) -> None:
        self._states = {s.slice_key: s for s in states}

    def get_collection_slice_states(self, source, endpoint, slice_keys=None):
        return {k: v for k, v in self._states.items() if slice_keys is None or k in slice_keys}


def _state(key: str, status: SliceStatus, days_ago: int = 1) -> CollectionSliceState:
    return CollectionSliceState(
        source=Source.OPENDART,
        endpoint="ep",
        slice_key=key,
        status=status,
        expected_rows=0 if status is SliceStatus.NO_DATA else 1,
        actual_rows=0 if status is SliceStatus.NO_DATA else 1,
        updated_at=now_kst() - timedelta(days=days_ago),
    )


def test_ledger_retry_overrides_only_a_no_data_verdict() -> None:
    storage = _LedgerStorage(
        [
            _state("nd", SliceStatus.NO_DATA),
            _state("ok", SliceStatus.SUCCESS),
            _state("other-nd", SliceStatus.NO_DATA),
        ]
    )
    ledger = SliceLedger(storage, source=Source.OPENDART, endpoint="ep")
    plan = ledger.plan(["nd", "ok", "other-nd", "new"], retry_keys=["nd", "ok", "new"])

    assert plan.receipt_retry == ["nd"]
    assert set(plan.pending) == {"nd", "new"}
    assert plan.skipped_complete == ["ok"]
    assert plan.skipped_no_data == ["other-nd"]
    assert plan.as_counts()["slices_receipt_retry"] == 1


def test_ledger_without_retry_keys_is_unchanged() -> None:
    storage = _LedgerStorage([_state("nd", SliceStatus.NO_DATA)])
    plan = SliceLedger(storage, source=Source.OPENDART, endpoint="ep").plan(["nd"])
    assert plan.receipt_retry == []
    assert plan.skipped_no_data == ["nd"]


# --------------------------------------------------------------------------
# services
# --------------------------------------------------------------------------


def _seed_no_data(storage, endpoint: str, key: str, days_ago: int = 1) -> None:
    storage.slice_states[(Source.OPENDART, endpoint, key)] = CollectionSliceState(
        source=Source.OPENDART,
        endpoint=endpoint,
        slice_key=key,
        status=SliceStatus.NO_DATA,
        expected_rows=0,
        actual_rows=0,
        updated_at=now_kst() - timedelta(days=days_ago),
    )


def _retry_plan(*slots: tuple[str, int, str]) -> ReceiptRetryPlan:
    return ReceiptRetryPlan(
        slots={slot: date(2026, 8, 14) for slot in slots}, window_days=7, receipts_considered=1
    )


def _run_financials(storage, provider, **kwargs):
    return sync_dart_financial_statements(
        provider=provider,
        storage=storage,
        bsns_years=[2026],
        reprt_codes=["11012"],
        fs_divs=["CFS"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        **kwargs,
    )


def test_financials_no_data_verdict_stands_without_a_receipt() -> None:
    storage, provider = MockFinancialStorage(), MockFinancialProvider()
    _seed_no_data(storage, "fnlttSinglAcntAll", f"{CORP}:2026:11012:CFS")

    result = _run_financials(storage, provider)

    assert result.requests_attempted == 0
    assert result.requests_skipped == 1
    assert provider.calls == 0


def test_financials_receipt_overrides_the_verdict_and_logs_counts() -> None:
    storage, provider = MockFinancialStorage(), MockFinancialProvider()
    _seed_no_data(storage, "fnlttSinglAcntAll", f"{CORP}:2026:11012:CFS")

    result = _run_financials(storage, provider, receipt_retry=_retry_plan((CORP, 2026, "11012")))

    assert result.requests_attempted == 1
    assert provider.calls == 1
    assert result.rows_upserted == 1
    counts = storage.runs[-1].counts
    assert counts["receipt_retry_slots"] == 1
    assert counts["receipt_retry_requests"] == 1
    assert storage.runs[0].params["receipt_retry_slots_planned"] == 1


def test_financials_receipt_for_another_slot_changes_nothing() -> None:
    storage, provider = MockFinancialStorage(), MockFinancialProvider()
    _seed_no_data(storage, "fnlttSinglAcntAll", f"{CORP}:2026:11012:CFS")

    result = _run_financials(storage, provider, receipt_retry=_retry_plan((CORP, 2026, "11013")))

    assert result.requests_attempted == 0
    assert storage.runs[-1].counts["receipt_retry_slots"] == 0


def test_financials_receipt_bypasses_the_negative_cache_but_nothing_else_does() -> None:
    key = "005930:2026:11012:CFS"
    blocked = _run_financials(
        MockFinancialStorage(),
        MockFinancialProvider(),
        skip_request_keys={key},
    )
    assert blocked.requests_attempted == 0

    storage, provider = MockFinancialStorage(), MockFinancialProvider()
    _seed_no_data(storage, "fnlttSinglAcntAll", f"{CORP}:2026:11012:CFS")
    result = _run_financials(
        storage,
        provider,
        skip_request_keys={key},
        receipt_retry=_retry_plan((CORP, 2026, "11012")),
    )
    assert result.requests_attempted == 1


def test_financials_receipt_never_redoes_existing_raw_rows() -> None:
    storage, provider = MockFinancialStorage(), MockFinancialProvider()
    storage.existing_financial_requests.add((CORP, 2026, "11012", "CFS"))
    _seed_no_data(storage, "fnlttSinglAcntAll", f"{CORP}:2026:11012:CFS")

    result = _run_financials(storage, provider, receipt_retry=_retry_plan((CORP, 2026, "11012")))

    assert result.requests_attempted == 0
    assert provider.calls == 0


def test_financials_receipt_lifts_the_availability_gate_for_its_slot_only() -> None:
    gated = {(2026, "11013")}
    storage, provider = MockFinancialStorage(), MockFinancialProvider()
    none = _run_financials(storage, provider, allowed_year_report_pairs=gated)
    assert none.requests_attempted == 0
    assert none.requests_skipped == 1

    storage, provider = MockFinancialStorage(), MockFinancialProvider()
    lifted = _run_financials(
        storage,
        provider,
        allowed_year_report_pairs=gated,
        receipt_retry=_retry_plan((CORP, 2026, "11012")),
    )
    assert lifted.requests_attempted == 1
    assert provider.calls == 1

    storage, provider = MockFinancialStorage(), MockFinancialProvider()
    other = _run_financials(
        storage,
        provider,
        allowed_year_report_pairs=gated,
        receipt_retry=_retry_plan(("99999999", 2026, "11012")),
    )
    assert other.requests_attempted == 0


def test_financials_force_ignores_the_retry_plan() -> None:
    storage, provider = MockFinancialStorage(), MockFinancialProvider()
    result = _run_financials(
        storage, provider, force=True, receipt_retry=_retry_plan((CORP, 2026, "11012"))
    )
    assert result.requests_attempted == 1
    assert storage.runs[-1].counts["receipt_retry_slots"] == 0


def test_financials_retry_stops_after_the_window_and_the_normal_ttl_resumes() -> None:
    class StillEmpty(MockFinancialProvider):
        def fetch_financial_statement(self, corp, bsns_year, reprt_code, fs_div):
            result = super().fetch_financial_statement(corp, bsns_year, reprt_code, "OFS")
            result.fs_div = fs_div
            return result

    storage, provider = MockFinancialStorage(), StillEmpty()
    _seed_no_data(storage, "fnlttSinglAcntAll", f"{CORP}:2026:11012:CFS", days_ago=2)
    receipt = _receipt(dt=date(2026, 8, 14))
    receipts = _ReceiptStorage([receipt])

    attempts = []
    for offset in range(1, 11):
        today = receipt.rcept_dt + timedelta(days=offset)
        plan = build_receipt_retry_plan(receipts, as_of=today, window_days=7)
        attempts.append(_run_financials(storage, provider, receipt_retry=plan).requests_attempted)

    # Re-asked on day 1..7 (each answer is no-data again), skipped from day 8
    # on because the fresh verdict now stands under the normal 30-day TTL.
    assert attempts == [1, 1, 1, 1, 1, 1, 1, 0, 0, 0]


def test_share_info_receipt_re_asks_every_endpoint_of_the_slot() -> None:
    def seed(storage: MockShareInfoStorage) -> None:
        for endpoint in ("stockTotqySttus", "alotMatter", "tesstkAcqsDspsSttus", "irdsSttus"):
            _seed_no_data(storage, endpoint, f"{CORP}:2026:11012")

    def run(storage, provider, **kwargs):
        return sync_dart_share_info(
            share_count_provider=provider,
            shareholder_return_provider=provider,
            capital_change_provider=provider,
            storage=storage,
            bsns_years=[2026],
            reprt_codes=["11012"],
            tickers=["005930"],
            rate_limit_seconds=0.0,
            **kwargs,
        )

    storage, provider = MockShareInfoStorage(), MockShareInfoProvider()
    seed(storage)
    skipped = run(storage, provider)
    assert skipped.requests_attempted == 0
    assert provider.share_count_calls == 0

    storage, provider = MockShareInfoStorage(), MockShareInfoProvider()
    seed(storage)
    retried = run(
        storage,
        provider,
        skip_request_keys={"005930:2026:11012:share_count", "005930:2026:11012:dividend"},
        receipt_retry=_retry_plan((CORP, 2026, "11012")),
    )
    assert retried.requests_attempted == 4
    assert (provider.share_count_calls, provider.dividend_calls) == (1, 1)
    assert provider.treasury_stock_calls == 1
    assert provider.capital_change_calls == 1
    counts = storage.runs[-1].counts
    assert counts["receipt_retry_slots"] == 1
    assert counts["receipt_retry_requests"] == 4


def test_share_info_receipt_lifts_the_availability_gate() -> None:
    storage, provider = MockShareInfoStorage(), MockShareInfoProvider()
    result = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        storage=storage,
        bsns_years=[2026],
        reprt_codes=["11012"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        allowed_year_report_pairs={(2026, "11013")},
        receipt_retry=_retry_plan((CORP, 2026, "11012")),
    )
    assert result.requests_attempted > 0
    assert provider.share_count_calls == 1


def test_xbrl_receipt_overrides_the_verdict_and_lifts_the_gate() -> None:
    xbrl_key = f"{CORP}:2025:11011:20260310002820"

    storage, provider = MockXbrlStorage(), MockXbrlProvider()
    _seed_no_data(storage, "fnlttXbrl", xbrl_key)
    blocked = sync_dart_xbrl(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )
    assert blocked.requests_attempted == 0

    storage, provider = MockXbrlStorage(), MockXbrlProvider()
    _seed_no_data(storage, "fnlttXbrl", xbrl_key)
    retried = sync_dart_xbrl(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        allowed_year_report_pairs={(2026, "11013")},
        receipt_retry=_retry_plan((CORP, 2025, "11011")),
    )
    assert retried.requests_attempted == 1
    assert provider.calls == 1
    counts = storage.runs[-1].counts
    assert counts["receipt_retry_slots"] == 1
    assert counts["receipt_retry_requests"] == 1


def test_historical_scope_no_data_is_also_overridden_by_a_receipt() -> None:
    storage, provider = MockFinancialStorage(), MockFinancialProvider()
    _seed_no_data(storage, "fnlttSinglAcntAll", f"{CORP}:2026:11012:CFS", days_ago=400)
    result = _run_financials(
        storage,
        provider,
        scope=UniverseScope.HISTORICAL,
        receipt_retry=_retry_plan((CORP, 2026, "11012")),
    )
    assert result.requests_attempted == 1


# --------------------------------------------------------------------------
# CLI wiring
# --------------------------------------------------------------------------


def _cli_args(**overrides):
    import argparse

    values = {"receipt_retry_window_days": 7, "receipt_retry_max_slots": 3000, "force": False}
    values.update(overrides)
    return argparse.Namespace(**values)


def test_cli_helper_is_off_by_default_and_with_force() -> None:
    from collector.kr.cli.app import _build_cli_receipt_retry_plan

    storage = _ReceiptStorage([_receipt()])
    assert (
        _build_cli_receipt_retry_plan(storage, _cli_args(receipt_retry_window_days=0), []) is None
    )
    assert _build_cli_receipt_retry_plan(storage, _cli_args(force=True), []) is None
    assert storage.calls == []


def test_cli_helper_falls_back_to_normal_ttl_when_receipts_cannot_be_read() -> None:
    from collector.kr.cli.app import _build_cli_receipt_retry_plan

    class Broken:
        def get_dart_periodic_receipts(self, rcept_from, rcept_to):
            raise RuntimeError("db down")

    assert _build_cli_receipt_retry_plan(Broken(), _cli_args(), []) is None


def test_cli_parsers_default_the_retry_off_and_accept_the_flags() -> None:
    from collector.kr.cli.app import build_parser

    parser = build_parser()
    for command in ("sync-financials", "sync-share-info", "sync-xbrl"):
        default = parser.parse_args(["dart", command, "--incremental"])
        assert default.receipt_retry_window_days == 0
        on = parser.parse_args(
            [
                "dart",
                command,
                "--incremental",
                "--receipt-retry-window-days",
                "7",
                "--receipt-retry-max-slots",
                "100",
            ]
        )
        assert (on.receipt_retry_window_days, on.receipt_retry_max_slots) == (7, 100)
