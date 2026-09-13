"""Unit tests for the OpenDART company-profile sync (N2) and its history (F-1).

Fixture values are real ``company.json`` responses observed on 2026-08-15
(`docs/dev/20260731_raw_features/02_data_expansion_plan/poc/n2_company_profile.md`),
including the two shapes that broke the plan's assumptions: ``induty_code``
length varies (264 / 5821 / 21100), and LG and KB Financial Group carry the
identical code 64992.

The F-1 half is about a different property. ``dart_corp_master`` is an upsert
and forgets the previous industry code, so the monthly history is the only
thing that will ever make ``induty_code`` point-in-time — and only forwards.
What has to hold is that a month is recorded exactly once (so the monthly job
is safe to retry), that the response's own identity fields are what get
recorded, and that the seed row is distinguishable from a real observation.
"""

from __future__ import annotations

from datetime import date

from collector.kr.domain.enums import Market, RunStatus, RunType, Source, UniverseScope
from collector.kr.domain.models import (
    CompanyProfile,
    CompanyProfileResult,
    DartCorp,
    UpsertResult,
)
from collector.kr.service.sync_dart_corp_profile import (
    observation_month,
    seed_dart_corp_profile_history,
    sync_dart_corp_profile,
)
from collector.kr.util.time import now_kst

# ticker -> (corp_code, induty_code, acc_mt)
SAMPLE = {
    "005930": ("00126380", "264", "12"),  # 3 digits
    "259960": ("01253680", "5821", "12"),  # 4 digits
    "068270": ("00421045", "21100", "12"),  # 5 digits
    "003550": ("00120021", "64992", "03"),  # non-December fiscal year
}


def _corp(ticker: str) -> DartCorp:
    return DartCorp(
        corp_code=SAMPLE[ticker][0],
        corp_name=f"corp-{ticker}",
        ticker=ticker,
        market=Market.KOSPI,
        stock_name=f"name-{ticker}",
        modify_date=date(2026, 1, 1),
        is_active=True,
        source=Source.OPENDART,
        fetched_at=now_kst(),
    )


class FakeProvider:
    """Returns a profile per corp, with configurable failures."""

    def __init__(
        self,
        error_on: set[str] | None = None,
        no_data_on: set[str] | None = None,
        missing_industry_on: set[str] | None = None,
    ) -> None:
        self.error_on = error_on or set()
        self.no_data_on = no_data_on or set()
        self.missing_industry_on = missing_industry_on or set()
        self.calls: list[str] = []

    def fetch_company_profile(self, corp: DartCorp) -> CompanyProfileResult:
        self.calls.append(corp.corp_code)
        if corp.corp_code in self.error_on:
            return CompanyProfileResult(error="upstream boom")
        if corp.corp_code in self.no_data_on:
            return CompanyProfileResult(no_data=True, status_code="013")

        ticker = next(t for t, v in SAMPLE.items() if v[0] == corp.corp_code)
        _, induty, acc_mt = SAMPLE[ticker]
        if corp.corp_code in self.missing_industry_on:
            induty = None
        return CompanyProfileResult(
            profile=CompanyProfile(
                corp_code=corp.corp_code,
                induty_code=induty,
                corp_cls="Y",
                est_dt=date(1969, 1, 13),
                acc_mt=acc_mt,
                raw_payload={"corp_code": corp.corp_code, "induty_code": induty},
                fetched_at=now_kst(),
                ticker=ticker,
                corp_name=f"resp-corp-{ticker}",
                stock_name=f"resp-name-{ticker}",
            )
        )


class FakeStorage:
    def __init__(self, corps: list[DartCorp], profiled: set[str] | None = None) -> None:
        self._corps = corps
        self._profiled = profiled or set()
        self.written: list[CompanyProfile] = []
        self.recorded_runs: list = []
        # (corp_code, observed_month) -> the recorded row, so the fake enforces
        # the real table's primary key rather than merely counting calls.
        self.history: dict[tuple[str, date], dict] = {}
        self.seed_calls: list[tuple[date, str | None]] = []

    def get_dart_corp_master(
        self,
        active_only: bool = True,
        tickers: list[str] | None = None,
        include_delisted: bool = False,
    ) -> list[DartCorp]:
        if tickers:
            return [c for c in self._corps if c.ticker in tickers]
        return list(self._corps)

    def get_profiled_corp_codes(self) -> set[str]:
        return set(self._profiled)

    def upsert_company_profiles(self, profiles: list[CompanyProfile]) -> UpsertResult:
        self.written.extend(profiles)
        return UpsertResult(updated=len(profiles))

    def append_company_profile_history(
        self,
        profiles: list[CompanyProfile],
        observed_month: date,
        run_id: str | None = None,
    ) -> UpsertResult:
        inserted = 0
        for profile in profiles:
            key = (profile.corp_code, observed_month)
            if key in self.history:  # ON CONFLICT DO NOTHING
                continue
            self.history[key] = {
                "observed_at": profile.fetched_at,
                "ticker": profile.ticker,
                "corp_cls": profile.corp_cls,
                "induty_code": profile.induty_code,
                "corp_name": profile.corp_name,
                "stock_name": profile.stock_name,
                "is_seed": False,
                "run_id": run_id,
            }
            inserted += 1
        return UpsertResult(updated=inserted)

    def seed_company_profile_history(
        self, observed_month: date, run_id: str | None = None
    ) -> UpsertResult:
        self.seed_calls.append((observed_month, run_id))
        inserted = 0
        for corp in self._corps:
            if corp.corp_code not in self._profiled:
                continue
            key = (corp.corp_code, observed_month)
            if key in self.history:
                continue
            self.history[key] = {"is_seed": True, "run_id": run_id}
            inserted += 1
        return UpsertResult(updated=inserted)

    def record_run(self, run) -> None:  # noqa: ANN001 - test stub
        self.recorded_runs.append(run)


def _run(storage: FakeStorage, provider: FakeProvider, **kwargs):
    defaults = dict(rate_limit_seconds=0.0)
    defaults.update(kwargs)
    return sync_dart_corp_profile(profile_provider=provider, storage=storage, **defaults)


def test_all_ticker_mapped_corps_are_fetched_and_written() -> None:
    corps = [_corp(t) for t in SAMPLE]
    storage = FakeStorage(corps)
    provider = FakeProvider()

    result = _run(storage, provider)

    assert result.requests_attempted == 4
    assert result.rows_upserted == 4
    assert result.errors == {}
    assert len(storage.written) == 4
    assert storage.recorded_runs[-1].run_type is RunType.DART_CORP_PROFILE_SYNC
    assert storage.recorded_runs[-1].status is RunStatus.SUCCESS


def test_varying_induty_code_lengths_survive_the_round_trip() -> None:
    corps = [_corp(t) for t in SAMPLE]
    storage = FakeStorage(corps)

    _run(storage, FakeProvider())

    codes = {p.corp_code: p.induty_code for p in storage.written}
    assert codes[SAMPLE["005930"][0]] == "264"
    assert codes[SAMPLE["259960"][0]] == "5821"
    assert codes[SAMPLE["068270"][0]] == "21100"


def test_non_december_fiscal_year_is_preserved() -> None:
    # 03_w1_company_profile.md 2: the mart hardcodes period-end to December, so
    # acc_mt must survive collection intact for that to be measurable.
    corps = [_corp("003550")]
    storage = FakeStorage(corps)

    _run(storage, FakeProvider())

    assert [p.acc_mt for p in storage.written] == ["03"]


def test_corps_without_a_ticker_are_not_targeted() -> None:
    # 116k corp_codes exist; only ~3,959 have a ticker. Fetching the rest is a
    # 30x request bill for rows no feature reads.
    unlisted = DartCorp(
        corp_code="99999999",
        corp_name="unlisted",
        ticker=None,
        market=None,
        stock_name=None,
        modify_date=None,
        is_active=True,
        source=Source.OPENDART,
        fetched_at=now_kst(),
    )
    storage = FakeStorage([_corp("005930"), unlisted])
    provider = FakeProvider()

    result = _run(storage, provider)

    assert provider.calls == [SAMPLE["005930"][0]]
    assert result.requests_attempted == 1


def test_already_profiled_corps_are_skipped_and_force_overrides() -> None:
    corps = [_corp(t) for t in SAMPLE]
    profiled = {SAMPLE["005930"][0], SAMPLE["259960"][0]}
    storage = FakeStorage(corps, profiled=profiled)
    provider = FakeProvider()

    result = _run(storage, provider)

    assert result.requests_skipped == 2
    assert result.requests_attempted == 2
    assert set(provider.calls) == {SAMPLE["068270"][0], SAMPLE["003550"][0]}

    forced_storage = FakeStorage(corps, profiled=profiled)
    forced = _run(forced_storage, FakeProvider(), force=True)
    assert forced.requests_skipped == 0
    assert forced.requests_attempted == 4


def test_partial_failure_ends_the_run_partial() -> None:
    corps = [_corp(t) for t in SAMPLE]
    storage = FakeStorage(corps)
    provider = FakeProvider(error_on={SAMPLE["068270"][0]})

    result = _run(storage, provider)

    assert list(result.errors) == ["068270:company"]
    assert result.rows_upserted == 3
    assert storage.recorded_runs[-1].status is RunStatus.PARTIAL


def test_no_data_is_counted_not_an_error() -> None:
    corps = [_corp(t) for t in SAMPLE]
    storage = FakeStorage(corps)
    provider = FakeProvider(no_data_on={SAMPLE["003550"][0]})

    result = _run(storage, provider)

    assert result.no_data == 1
    assert result.errors == {}
    assert result.rows_upserted == 3


def test_missing_industry_is_stored_as_null_not_dropped() -> None:
    # Coverage has to stay measurable: a corp with no industry is a row with a
    # NULL, not an absent row.
    corps = [_corp("005930")]
    storage = FakeStorage(corps)

    _run(storage, FakeProvider(missing_industry_on={SAMPLE["005930"][0]}))

    assert len(storage.written) == 1
    assert storage.written[0].induty_code is None


def test_empty_target_set_fails_the_run() -> None:
    storage = FakeStorage([])

    result = _run(storage, FakeProvider())

    assert "pipeline" in result.errors
    assert storage.recorded_runs[-1].status is RunStatus.FAILED


def test_historical_scope_is_threaded_to_the_target_query() -> None:
    # poc/survivorship_gap.md: every DART raw table covers ~2% of the 1,330
    # delisted names because targets come from active_only=True. The flag has
    # to reach the query, not just the signature.
    class _Recording(FakeStorage):
        def __init__(self, corps):
            super().__init__(corps)
            self.calls: list[dict] = []

        def get_dart_corp_master(
            self,
            active_only: bool = True,
            tickers: list[str] | None = None,
            include_delisted: bool = False,
        ) -> list[DartCorp]:
            self.calls.append(
                {
                    "active_only": active_only,
                    "tickers": tickers,
                    "include_delisted": include_delisted,
                }
            )
            return list(self._corps)

    storage = _Recording([_corp("005930")])
    _run(storage, FakeProvider(), scope=UniverseScope.HISTORICAL)

    assert storage.calls == [{"active_only": False, "tickers": None, "include_delisted": True}]


# --------------------------------------------------------------------------
# F-1.2 — the monthly history append
# --------------------------------------------------------------------------

_MONTH = date(2026, 9, 1)


def test_every_fetch_is_also_recorded_as_a_monthly_observation() -> None:
    corps = [_corp(t) for t in SAMPLE]
    storage = FakeStorage(corps)

    result = _run(storage, FakeProvider(), observed_month=_MONTH)

    assert result.history_rows_appended == 4
    assert {key[1] for key in storage.history} == {_MONTH}
    assert storage.recorded_runs[-1].counts["history_rows_appended"] == 4


def test_a_second_run_in_the_same_month_records_nothing_new() -> None:
    # The monthly Cronicle event has to be safe to retry, and a manual run
    # mid-month must not create a second observation of the same month.
    corps = [_corp(t) for t in SAMPLE]
    storage = FakeStorage(corps)

    first = _run(storage, FakeProvider(), observed_month=_MONTH)
    # force=True so the master upsert runs again too — only the history should
    # decline to add a row.
    second = _run(storage, FakeProvider(), observed_month=_MONTH, force=True)

    assert first.history_rows_appended == 4
    assert second.rows_upserted == 4
    assert second.history_rows_appended == 0
    assert len(storage.history) == 4


def test_the_next_month_is_a_new_observation() -> None:
    corps = [_corp(t) for t in SAMPLE]
    storage = FakeStorage(corps)

    _run(storage, FakeProvider(), observed_month=_MONTH, force=True)
    _run(storage, FakeProvider(), observed_month=date(2026, 10, 1), force=True)

    assert len(storage.history) == 8
    assert {key[1] for key in storage.history} == {_MONTH, date(2026, 10, 1)}


def test_the_history_keeps_the_responses_own_identity_fields() -> None:
    # dart_corp_master.ticker comes from corpCode.xml; company.json returns its
    # own stock_code/corp_name. The two disagreeing is a signal, so the history
    # records the response, not the master.
    storage = FakeStorage([_corp("005930")])

    _run(storage, FakeProvider(), observed_month=_MONTH)

    row = storage.history[(SAMPLE["005930"][0], _MONTH)]
    assert row["ticker"] == "005930"
    assert row["corp_name"] == "resp-corp-005930"
    assert row["stock_name"] == "resp-name-005930"
    assert row["induty_code"] == "264"
    assert row["is_seed"] is False


def test_the_observation_is_linked_to_its_run() -> None:
    storage = FakeStorage([_corp("005930")])

    _run(storage, FakeProvider(), observed_month=_MONTH)

    run_id = storage.recorded_runs[-1].run_id
    assert storage.history[(SAMPLE["005930"][0], _MONTH)]["run_id"] == run_id


def test_the_month_and_the_scope_are_recorded_in_the_run_params() -> None:
    storage = FakeStorage([_corp("005930")])

    _run(storage, FakeProvider(), observed_month=_MONTH, scope=UniverseScope.HISTORICAL)

    params = storage.recorded_runs[-1].params
    assert params["observed_month"] == "2026-09-01"
    assert params["universe_scope"] == "historical"


def test_what_was_fetched_before_an_exhaustion_stop_is_still_recorded() -> None:
    # The run resumes from profile_fetched_at, so an unflushed profile is
    # re-fetched — but a flushed one whose observation was never written would
    # be skipped next time and lost for that month.
    from collector.kr.domain.models import CompanyProfileResult as _Result

    class _Exhausting(FakeProvider):
        def fetch_company_profile(self, corp: DartCorp):
            if corp.corp_code == SAMPLE["068270"][0]:
                return _Result(error="rate limited", exhaustion_reason="all_rate_limited")
            return super().fetch_company_profile(corp)

    corps = [_corp(t) for t in SAMPLE]
    storage = FakeStorage(corps)

    result = _run(storage, _Exhausting(), observed_month=_MONTH)

    assert result.opendart_exhaustion_reason == "all_rate_limited"
    assert result.rows_upserted == result.history_rows_appended
    assert len(storage.history) == result.rows_upserted


def test_the_observation_month_is_the_first_of_the_month() -> None:
    assert observation_month(date(2026, 9, 23)) == date(2026, 9, 1)


# --------------------------------------------------------------------------
# F-1.3 — the seed
# --------------------------------------------------------------------------


def test_the_seed_copies_the_masters_current_profiles() -> None:
    corps = [_corp(t) for t in SAMPLE]
    profiled = {SAMPLE[t][0] for t in SAMPLE}
    storage = FakeStorage(corps, profiled=profiled)

    result = seed_dart_corp_profile_history(storage, observed_month=_MONTH)

    assert result.rows_inserted == 4
    assert result.observed_month == _MONTH
    assert all(row["is_seed"] for row in storage.history.values())
    assert storage.recorded_runs[-1].run_type is RunType.DART_CORP_PROFILE_HISTORY_SEED
    assert storage.recorded_runs[-1].status is RunStatus.SUCCESS


def test_a_corp_with_no_profile_is_not_seeded() -> None:
    # There is nothing to record for it; a row with every field NULL would read
    # as "observed, industry unknown".
    corps = [_corp(t) for t in SAMPLE]
    storage = FakeStorage(corps, profiled={SAMPLE["005930"][0]})

    result = seed_dart_corp_profile_history(storage, observed_month=_MONTH)

    assert result.rows_inserted == 1


def test_seeding_twice_inserts_nothing_the_second_time() -> None:
    corps = [_corp(t) for t in SAMPLE]
    profiled = {SAMPLE[t][0] for t in SAMPLE}
    storage = FakeStorage(corps, profiled=profiled)

    seed_dart_corp_profile_history(storage, observed_month=_MONTH)
    again = seed_dart_corp_profile_history(storage, observed_month=_MONTH)

    assert again.rows_inserted == 0
    assert len(storage.history) == 4


def test_a_real_observation_does_not_overwrite_the_seed_row() -> None:
    # Both target (corp_code, observed_month). The seed runs first, so the
    # month it seeded keeps is_seed = true and the first *real* observation is
    # the following month's — which is why the change-rate report cannot use
    # the seed month as a baseline interval.
    corps = [_corp(t) for t in SAMPLE]
    profiled = {SAMPLE[t][0] for t in SAMPLE}
    storage = FakeStorage(corps, profiled=profiled)

    seed_dart_corp_profile_history(storage, observed_month=_MONTH)
    result = _run(storage, FakeProvider(), observed_month=_MONTH, force=True)

    assert result.history_rows_appended == 0
    assert all(row["is_seed"] for row in storage.history.values())


def test_a_seed_failure_fails_the_run() -> None:
    class _Broken(FakeStorage):
        def seed_company_profile_history(self, observed_month, run_id=None):  # noqa: ANN001
            raise RuntimeError("relation does not exist")

    storage = _Broken([_corp("005930")])

    result = seed_dart_corp_profile_history(storage, observed_month=_MONTH)

    assert "pipeline" in result.errors
    assert storage.recorded_runs[-1].status is RunStatus.FAILED
