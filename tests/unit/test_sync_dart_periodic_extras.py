"""DS002 periodic-report extras — adapter, year arithmetic, and resume (N6).

Two behaviours carry this package and both get the most attention here.

**Vintage.** DS002 has no way to request a particular filing, so a backfill
returns the latest corrected version of every past year. Keeping ``rcept_no``
in the unique key is what makes a later correction observable instead of
destructive — and for audit opinions and changes of control, the correction is
the signal itself.

**Resume.** The backfill is ~83,700 calls across several daily-key-limit stops,
which is more than ``ingestion_runs.params`` can carry. It resumes from the
slice ledger (L-1) instead, so the exit-75 test below is the one that matters
most in this file.
"""

from __future__ import annotations

import pytest

from collector.kr.adapters.opendart_periodic_extras.provider import (
    ENDPOINTS,
    parse_periodic_extra_response,
)
from collector.kr.domain.enums import (
    PeriodicExtraStatement,
    RunStatus,
    SliceStatus,
    Source,
    UniverseScope,
)
from collector.kr.domain.models import (
    CollectionSliceState,
    DartCorp,
    DartPeriodicExtraResult,
    UpsertResult,
)
from collector.kr.service.sync_dart_periodic_extras import (
    DEFAULT_STATEMENTS,
    LEDGER_ENDPOINT,
    resolve_target_years,
    slice_key,
    sync_dart_periodic_extras,
)
from collector.kr.util.time import now_kst

CORP = DartCorp(
    corp_code="00126380",
    corp_name="삼성전자",
    ticker="005930",
    market=None,
    stock_name="삼성전자",
    modify_date=None,
    is_active=True,
    source=Source.OPENDART,
    fetched_at=now_kst(),
)

# Shaped after the live empSttus response in the N6 PoC: several rows per
# response, split by division and gender.
EMPLOYEE_ROWS = [
    {
        "rcept_no": "20240312000736",
        "corp_code": "00126380",
        "sexdstn": "남",
        "fo_bbm": "DX부문",
        "rgllbr_co": "70000",
        "cnttk_co": "1000",
        "sm": "71000",
        "jan_salary_am": "120000000",
    },
    {
        "rcept_no": "20240312000736",
        "corp_code": "00126380",
        "sexdstn": "여",
        "fo_bbm": "DX부문",
        "rgllbr_co": "30000",
        "cnttk_co": "500",
        "sm": "30500",
        "jan_salary_am": "95000000",
    },
]


def _payload(rows: list[dict[str, str]]) -> dict[str, object]:
    return {"status": "000", "message": "정상", "list": rows}


# --------------------------------------------------------------------------
# adapter parsing
# --------------------------------------------------------------------------


def test_every_ds002_statement_has_an_endpoint() -> None:
    assert set(ENDPOINTS) == set(PeriodicExtraStatement)


def test_rows_keep_their_response_order_as_identity() -> None:
    # Division and gender labels are rewritten between years, so a natural key
    # built from them breaks joins silently. Response order was verified stable
    # across repeated requests, so it is the identity.
    result = parse_periodic_extra_response(
        _payload(EMPLOYEE_ROWS), CORP, 2023, "11011", PeriodicExtraStatement.EMPLOYEE
    )

    assert result.error is None
    assert [record.row_ordinal for record in result.records] == [0, 1]
    assert result.records[0].raw_payload["sexdstn"] == "남"


def test_the_whole_response_row_is_kept() -> None:
    # Which fields matter is still being decided, and a raw payload beats a
    # guess at the right columns.
    result = parse_periodic_extra_response(
        _payload(EMPLOYEE_ROWS), CORP, 2023, "11011", PeriodicExtraStatement.EMPLOYEE
    )

    assert result.records[0].raw_payload == EMPLOYEE_ROWS[0]


def test_a_row_without_a_receipt_number_fails_the_response() -> None:
    # Storing it with a blank rcept_no would merge every vintage of this report
    # onto one row in the unique key — the exact loss the table exists to stop.
    rows = [dict(EMPLOYEE_ROWS[0], rcept_no="")]

    result = parse_periodic_extra_response(
        _payload(rows), CORP, 2023, "11011", PeriodicExtraStatement.EMPLOYEE
    )

    assert result.records == []
    assert result.error is not None
    assert "rcept_no" in result.error


def test_response_rows_is_recorded_for_the_ledger() -> None:
    # The ledger compares what the response carried against what storage wrote.
    result = parse_periodic_extra_response(
        _payload(EMPLOYEE_ROWS), CORP, 2023, "11011", PeriodicExtraStatement.EMPLOYEE
    )

    assert result.response_rows == 2


# --------------------------------------------------------------------------
# year arithmetic — the 162,000 -> 83,700 reduction
# --------------------------------------------------------------------------

YEARS = list(range(2015, 2027))


@pytest.mark.parametrize(
    "statement",
    [
        PeriodicExtraStatement.EMPLOYEE,
        PeriodicExtraStatement.MAJOR_SHAREHOLDER,
        PeriodicExtraStatement.EXECUTIVE,
    ],
)
def test_a_one_year_response_is_requested_every_year(
    statement: PeriodicExtraStatement,
) -> None:
    assert resolve_target_years(statement, YEARS) == sorted(YEARS, reverse=True)


def _uncovered(statement: PeriodicExtraStatement, wanted: list[int]) -> list[int]:
    """Wanted years that no requested year answers for.

    A request for year R answers for ``R - step .. R``, so coverage — not the
    exact list of requested years — is what has to hold. Asserting the list
    would pin an implementation detail and still not catch a real gap.
    """
    from collector.kr.service.sync_dart_periodic_extras import YEAR_STEP

    step = YEAR_STEP[statement]
    requested = resolve_target_years(statement, wanted)
    return [
        year
        for year in wanted
        if not any(0 <= requested_year - year <= step for requested_year in requested)
    ]


def test_the_accumulated_change_history_costs_three_requests_for_twelve_years() -> None:
    # A 2023 request returned changes back to 2019, so one request answers for
    # five years. 12 years cost 3 requests instead of 12.
    years = resolve_target_years(PeriodicExtraStatement.MAJOR_CHANGE, YEARS)

    assert years == [2026, 2022, 2018]
    assert _uncovered(PeriodicExtraStatement.MAJOR_CHANGE, YEARS) == []


def test_the_audit_opinion_costs_six_requests_for_twelve_years() -> None:
    # 당기 / 전기 / 전전기 arrive together, so one request answers for three years.
    years = resolve_target_years(PeriodicExtraStatement.AUDIT_OPINION, YEARS)

    assert years == [2026, 2024, 2022, 2020, 2018, 2016]
    assert _uncovered(PeriodicExtraStatement.AUDIT_OPINION, YEARS) == []


@pytest.mark.parametrize("statement", list(PeriodicExtraStatement))
@pytest.mark.parametrize(
    "wanted",
    [
        [2025],
        [2020, 2021, 2022],
        list(range(2015, 2027)),
        list(range(2015, 2018)),
    ],
)
def test_thinning_never_leaves_a_wanted_year_uncovered(
    statement: PeriodicExtraStatement,
    wanted: list[int],
) -> None:
    # The reduction is only sound if it is lossless. A step that overshoots the
    # oldest year is the case that would quietly drop it.
    assert _uncovered(statement, wanted) == []


def test_years_are_requested_newest_first() -> None:
    # A run cut short by the daily key limit should have covered the recent
    # end, not a scatter of old years.
    years = resolve_target_years(PeriodicExtraStatement.EMPLOYEE, [2018, 2026, 2020])

    assert years == [2026, 2020, 2018]


def test_the_executive_endpoint_is_not_collected_by_default() -> None:
    # 32,400 of the 83,700 calls for one thin candidate feature that
    # MAJOR_CHANGE covers better.
    assert PeriodicExtraStatement.EXECUTIVE not in DEFAULT_STATEMENTS


# --------------------------------------------------------------------------
# service
# --------------------------------------------------------------------------


class _FakeStorage:
    def __init__(self, slice_states: list[CollectionSliceState] | None = None) -> None:
        self.runs: list[object] = []
        self.upserted: list[object] = []
        self.slice_states = {state.slice_key: state for state in (slice_states or [])}
        self.slice_writes: list[CollectionSliceState] = []

    def get_dart_corp_master(self, active_only=True, tickers=None, include_delisted=False):
        return [CORP]

    def record_run(self, run):
        self.runs.append(run)

    def upsert_dart_periodic_extras_raw(self, records):
        self.upserted.extend(records)
        return UpsertResult(updated=len(records))

    def get_collection_slice_states(self, source, endpoint, slice_keys=None):
        if slice_keys is None:
            return dict(self.slice_states)
        wanted = set(slice_keys)
        return {k: v for k, v in self.slice_states.items() if k in wanted}

    def upsert_collection_slice_states(self, states):
        self.slice_writes.extend(states)
        for state in states:
            self.slice_states[state.slice_key] = state
        return UpsertResult(updated=len(states))


class _FakeProvider:
    """Answers with employee rows, optionally failing after N calls."""

    def __init__(self, *, exhaust_after: int | None = None) -> None:
        self.calls: list[tuple[int, str, PeriodicExtraStatement]] = []
        self._exhaust_after = exhaust_after

    def fetch_periodic_extra(self, corp, bsns_year, reprt_code, statement_type):
        self.calls.append((bsns_year, reprt_code, statement_type))
        if self._exhaust_after is not None and len(self.calls) > self._exhaust_after:
            return DartPeriodicExtraResult(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type=statement_type,
                error="All OpenDART API keys are temporarily rate limited.",
                exhaustion_reason="all_rate_limited",
            )
        return parse_periodic_extra_response(
            _payload(EMPLOYEE_ROWS), corp, bsns_year, reprt_code, statement_type
        )


def _sync(storage, provider, **kwargs):
    kwargs.setdefault("bsns_years", [2024, 2025])
    kwargs.setdefault("reprt_codes", ["11011"])
    kwargs.setdefault("statements", [PeriodicExtraStatement.EMPLOYEE])
    kwargs.setdefault("rate_limit_seconds", 0)
    kwargs.setdefault("scope", UniverseScope.HISTORICAL)
    return sync_dart_periodic_extras(provider=provider, storage=storage, **kwargs)


def test_a_fresh_run_collects_every_slice() -> None:
    storage, provider = _FakeStorage(), _FakeProvider()

    result = _sync(storage, provider)

    assert result.requests_attempted == 2
    assert result.rows_upserted == 4
    assert result.errors == {}
    assert storage.runs[-1].status is RunStatus.SUCCESS


def test_the_ledger_records_the_response_and_written_row_counts() -> None:
    # "The slice has rows" is not completion; a half-written slice would
    # otherwise be skipped by every later run and repaired by none.
    storage, provider = _FakeStorage(), _FakeProvider()

    _sync(storage, provider)

    written = {state.slice_key: state for state in storage.slice_writes}
    key = slice_key(CORP.corp_code, 2025, "11011", "employee")
    assert written[key].status is SliceStatus.SUCCESS
    assert (written[key].expected_rows, written[key].actual_rows) == (2, 2)


def test_a_completed_slice_is_not_requested_again() -> None:
    done = CollectionSliceState(
        source=Source.OPENDART,
        endpoint=LEDGER_ENDPOINT,
        slice_key=slice_key(CORP.corp_code, 2025, "11011", "employee"),
        status=SliceStatus.SUCCESS,
        expected_rows=2,
        actual_rows=2,
        updated_at=now_kst(),
    )
    storage, provider = _FakeStorage([done]), _FakeProvider()

    result = _sync(storage, provider)

    assert [year for year, _, _ in provider.calls] == [2024]
    assert result.slices_skipped_complete == 1
    assert result.requests_skipped == 1


def test_exit_75_leaves_the_finished_slices_recorded_and_resumes() -> None:
    # THE test for this package. The backfill spans several daily-key-limit
    # stops, which is more than ingestion_runs.params can carry: its no-data
    # list is capped per run and only recent runs are read back.
    storage = _FakeStorage()
    first = _FakeProvider(exhaust_after=1)

    first_result = _sync(
        storage, first, bsns_years=[2023, 2024, 2025], statements=[PeriodicExtraStatement.EMPLOYEE]
    )

    assert first_result.opendart_exhaustion_reason == "all_rate_limited"
    assert storage.runs[-1].status is RunStatus.FAILED
    # The one slice that did finish was flushed before the stop.
    finished = [state for state in storage.slice_writes if state.status is SliceStatus.SUCCESS]
    assert len(finished) == 1
    assert finished[0].slice_key.endswith(":2025:11011:employee")

    # A second run picks up exactly what is left, newest-first order preserved.
    second = _FakeProvider()
    second_result = _sync(
        storage, second, bsns_years=[2023, 2024, 2025], statements=[PeriodicExtraStatement.EMPLOYEE]
    )

    assert [year for year, _, _ in second.calls] == [2024, 2023]
    assert second_result.slices_skipped_complete == 1


def test_a_failed_slice_stays_pending_for_the_next_run() -> None:
    class _FailingProvider:
        def __init__(self) -> None:
            self.calls = 0

        def fetch_periodic_extra(self, corp, bsns_year, reprt_code, statement_type):
            self.calls += 1
            return DartPeriodicExtraResult(
                corp_code=corp.corp_code,
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type=statement_type,
                error="HTTP 500",
            )

    storage = _FakeStorage()
    _sync(storage, _FailingProvider(), bsns_years=[2025])

    (state,) = storage.slice_writes
    assert state.status is SliceStatus.FAILED
    assert state.last_error == "HTTP 500"

    retried = _FakeProvider()
    result = _sync(storage, retried, bsns_years=[2025])
    assert len(retried.calls) == 1
    assert result.slices_retrying == 1


def test_no_data_is_recorded_rather_than_treated_as_a_failure() -> None:
    class _NoDataProvider:
        def fetch_periodic_extra(self, corp, bsns_year, reprt_code, statement_type):
            return DartPeriodicExtraResult(
                corp_code=corp.corp_code,
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                statement_type=statement_type,
                no_data=True,
            )

    storage = _FakeStorage()
    result = _sync(storage, _NoDataProvider(), bsns_years=[2025])

    assert result.no_data_requests == 1
    assert result.errors == {}
    (state,) = storage.slice_writes
    assert state.status is SliceStatus.NO_DATA


def test_a_correction_adds_rows_instead_of_overwriting_them() -> None:
    # A corrected periodic report gets a new receipt number. For audit opinions
    # and changes of control the correction IS the signal, so the earlier
    # vintage has to survive it.
    original = parse_periodic_extra_response(
        _payload(EMPLOYEE_ROWS), CORP, 2023, "11011", PeriodicExtraStatement.EMPLOYEE
    )
    corrected_rows = [dict(row, rcept_no="20240401000111", sm="72000") for row in EMPLOYEE_ROWS]
    corrected = parse_periodic_extra_response(
        _payload(corrected_rows), CORP, 2023, "11011", PeriodicExtraStatement.EMPLOYEE
    )

    keys = {
        (r.corp_code, r.bsns_year, r.reprt_code, r.statement_type, r.rcept_no, r.row_ordinal)
        for r in [*original.records, *corrected.records]
    }

    assert len(keys) == 4


def test_the_default_scope_is_historical() -> None:
    # An adverse audit opinion is mostly about companies that later delisted,
    # so a current-listings default would bias the feature this package is for.
    storage, provider = _FakeStorage(), _FakeProvider()

    sync_dart_periodic_extras(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        statements=[PeriodicExtraStatement.EMPLOYEE],
        rate_limit_seconds=0,
    )

    assert storage.runs[0].params["universe_scope"] == UniverseScope.HISTORICAL.value


def test_force_recollects_everything_but_still_writes_the_ledger() -> None:
    done = CollectionSliceState(
        source=Source.OPENDART,
        endpoint=LEDGER_ENDPOINT,
        slice_key=slice_key(CORP.corp_code, 2025, "11011", "employee"),
        status=SliceStatus.SUCCESS,
        expected_rows=2,
        actual_rows=2,
        updated_at=now_kst(),
    )
    storage, provider = _FakeStorage([done]), _FakeProvider()

    _sync(storage, provider, bsns_years=[2025], force=True)

    assert len(provider.calls) == 1
    assert storage.slice_writes
