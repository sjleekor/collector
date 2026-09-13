from datetime import date
from decimal import Decimal

from collector.kr.adapters.opendart_share_info.provider import (
    OpenDartShareInfoProvider,
    parse_capital_change_response,
    parse_dividend_response,
    parse_stock_count_response,
    parse_treasury_stock_response,
)
from collector.kr.domain.enums import Market, RunStatus, Source, UniverseScope
from collector.kr.domain.models import (
    DartCapitalChangeLine,
    DartCapitalChangeResult,
    DartCorp,
    DartShareCountLine,
    DartShareCountResult,
    DartShareholderReturnLine,
    DartShareholderReturnResult,
    IngestionRun,
    UpsertResult,
)
from collector.kr.service.sync_dart_share_info import sync_dart_share_info
from collector.kr.util.time import now_kst
from tests.helpers.fake_opendart_executor import FakeOpenDartExecutor
from tests.helpers.fake_slice_storage import FakeSliceStorageMixin


def _sample_corp() -> DartCorp:
    return DartCorp(
        corp_code="00126380",
        corp_name="삼성전자",
        ticker="005930",
        market=Market.KOSPI,
        stock_name="삼성전자",
        modify_date=date(2026, 3, 10),
        is_active=True,
        source=Source.OPENDART,
        fetched_at=now_kst(),
    )


def test_parse_stock_count_response() -> None:
    corp = _sample_corp()
    payload = {
        "status": "000",
        "message": "정상",
        "list": [
            {
                "rcept_no": "20260310002820",
                "corp_cls": "Y",
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "se": "보통주",
                "isu_stock_totqy": "20,000,000,000",
                "now_to_isu_stock_totqy": "7,780,466,850",
                "now_to_dcrs_stock_totqy": "1,860,828,928",
                "redc": "-",
                "profit_incnr": "1,860,828,928",
                "rdmstk_repy": "-",
                "etc": "-",
                "istc_totqy": "5,919,637,922",
                "tesstk_co": "91,828,987",
                "distb_stock_co": "5,827,808,935",
                "stlm_dt": "2025-12-31",
            }
        ],
    }

    result = parse_stock_count_response(payload, corp, 2025, "11011")

    assert result.error is None
    assert len(result.records) == 1
    row = result.records[0]
    assert row.se == "보통주"
    assert row.isu_stock_totqy == 20000000000
    assert row.tesstk_co == 91828987
    assert row.stlm_dt == date(2025, 12, 31)


def test_parse_dividend_response() -> None:
    corp = _sample_corp()
    payload = {
        "status": "000",
        "message": "정상",
        "list": [
            {
                "rcept_no": "20260310002820",
                "corp_cls": "Y",
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "se": "주당 현금배당금(원)",
                "stock_knd": "보통주",
                "thstrm": "1,668",
                "frmtrm": "1,446",
                "lwfr": "1,444",
                "stlm_dt": "2025-12-31",
            }
        ],
    }

    result = parse_dividend_response(payload, corp, 2025, "11011")

    assert result.error is None
    assert len(result.records) == 3
    current = next(row for row in result.records if row.metric_code == "thstrm")
    assert current.row_name == "주당 현금배당금(원)"
    assert current.stock_knd == "보통주"
    assert current.value_numeric == Decimal("1668")


def test_parse_treasury_stock_response() -> None:
    corp = _sample_corp()
    payload = {
        "status": "000",
        "message": "정상",
        "list": [
            {
                "rcept_no": "20260310002820",
                "corp_cls": "Y",
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "stock_knd": "보통주",
                "acqs_mth1": "총계",
                "acqs_mth2": "총계",
                "acqs_mth3": "총계",
                "bsis_qy": "29,700,000",
                "change_qy_acqs": "118,314,495",
                "change_qy_dsps": "6,040,880",
                "change_qy_incnr": "50,144,628",
                "trmend_qy": "91,828,987",
                "rm": "-",
                "stlm_dt": "2025-12-31",
            }
        ],
    }

    result = parse_treasury_stock_response(payload, corp, 2025, "11011")

    assert result.error is None
    assert len(result.records) == 5
    ending = next(row for row in result.records if row.metric_code == "trmend_qy")
    assert ending.stock_knd == "보통주"
    assert ending.dim1 == "총계"
    assert ending.value_numeric == Decimal("91828987")


def test_parse_capital_change_response() -> None:
    corp = _sample_corp()
    payload = {
        "status": "000",
        "message": "정상",
        "list": [
            {
                "rcept_no": "20260310002820",
                "corp_cls": "Y",
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "isu_dcrs_de": "2025-06-30",
                "isu_dcrs_stle": "유상증자(일반공모)",
                "isu_dcrs_stock_knd": "보통주",
                "isu_dcrs_qy": "1,000,000",
                "isu_dcrs_mstvdv_fval_amount": "500",
                "isu_dcrs_mstvdv_fval_amount2": "70,000",
                "stlm_dt": "2025-12-31",
            }
        ],
    }

    result = parse_capital_change_response(payload, corp, 2025, "11011")

    assert result.error is None
    assert len(result.records) == 1
    row = result.records[0]
    assert row.isu_dcrs_stle == "유상증자(일반공모)"
    assert row.isu_dcrs_qy == 1000000
    assert row.isu_dcrs_mstvdv_fval_amount == Decimal("500")
    assert row.isu_dcrs_de == date(2025, 6, 30)


def _capital_change_payload(**overrides: str) -> dict[str, object]:
    row = {
        "rcept_no": "20260310002820",
        "corp_cls": "Y",
        "corp_code": "00126380",
        "corp_name": "삼성전자",
        "isu_dcrs_de": "2010.06.15",
        "isu_dcrs_stle": "유상증자(주주배정)",
        "isu_dcrs_stock_knd": "보통주",
        "isu_dcrs_qy": "1,000,000",
        "isu_dcrs_mstvdv_fval_amount": "500",
        "isu_dcrs_mstvdv_fval_amount2": "70,000",
        "stlm_dt": "2025-12-31",
    }
    row.update(overrides)
    return {"status": "000", "message": "정상", "list": [row]}


def test_parse_capital_change_accepts_dot_separated_isu_dcrs_de() -> None:
    # irdsSttus returns this field as YYYY.MM.DD, unlike stlm_dt which is ISO.
    corp = _sample_corp()

    result = parse_capital_change_response(_capital_change_payload(), corp, 2025, "11011")

    assert result.error is None
    assert len(result.records) == 1
    assert result.records[0].isu_dcrs_de == date(2010, 6, 15)
    assert result.records[0].stlm_dt == date(2025, 12, 31)


def test_parse_capital_change_accepts_compact_isu_dcrs_de() -> None:
    corp = _sample_corp()

    result = parse_capital_change_response(
        _capital_change_payload(isu_dcrs_de="20100615"), corp, 2025, "11011"
    )

    assert result.records[0].isu_dcrs_de == date(2010, 6, 15)


def test_parse_capital_change_keeps_row_when_a_date_is_unparseable() -> None:
    # A response is parsed as a whole, so a bad date must not discard the row.
    corp = _sample_corp()

    result = parse_capital_change_response(
        _capital_change_payload(isu_dcrs_de="해당사항 없음"), corp, 2025, "11011"
    )

    assert result.error is None
    assert len(result.records) == 1
    row = result.records[0]
    assert row.isu_dcrs_de is None
    assert row.isu_dcrs_qy == 1000000
    assert row.raw_payload["isu_dcrs_de"] == "해당사항 없음"


def test_open_dart_share_info_provider_maps_no_data_result() -> None:
    corp = _sample_corp()
    provider = OpenDartShareInfoProvider(
        request_executor=FakeOpenDartExecutor(
            [
                '{"status":"013","message":"조회된 데이타가 없습니다."}'.encode(),
            ]
        )
    )

    result = provider.fetch_share_count(corp, 2025, "11011")

    assert result.no_data is True
    assert result.status_code == "013"
    assert result.error is None


class MockShareInfoProvider:
    def __init__(self) -> None:
        self.share_count_calls = 0
        self.dividend_calls = 0
        self.treasury_stock_calls = 0
        self.capital_change_calls = 0

    def fetch_share_count(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareCountResult:
        self.share_count_calls += 1
        return DartShareCountResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            records=[
                DartShareCountLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    rcept_no="20260310002820",
                    corp_cls="Y",
                    se="보통주",
                    isu_stock_totqy=20000000000,
                    now_to_isu_stock_totqy=7780466850,
                    now_to_dcrs_stock_totqy=1860828928,
                    redc="-",
                    profit_incnr="1860828928",
                    rdmstk_repy="-",
                    etc="-",
                    istc_totqy=5919637922,
                    tesstk_co=91828987,
                    distb_stock_co=5827808935,
                    stlm_dt=date(2025, 12, 31),
                    source=Source.OPENDART,
                    fetched_at=now_kst(),
                    raw_payload={"se": "보통주"},
                )
            ],
        )

    def fetch_dividend(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareholderReturnResult:
        self.dividend_calls += 1
        return DartShareholderReturnResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            statement_type="dividend",
            records=[
                DartShareholderReturnLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    statement_type="dividend",
                    row_name="주당 현금배당금(원)",
                    stock_knd="보통주",
                    dim1="",
                    dim2="",
                    dim3="",
                    metric_code="thstrm",
                    metric_name="당기",
                    value_numeric=Decimal("1668"),
                    value_text="1,668",
                    unit="",
                    rcept_no="20260310002820",
                    stlm_dt=date(2025, 12, 31),
                    source=Source.OPENDART,
                    fetched_at=now_kst(),
                    raw_payload={"se": "주당 현금배당금(원)"},
                )
            ],
        )

    def fetch_treasury_stock(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartShareholderReturnResult:
        self.treasury_stock_calls += 1
        return DartShareholderReturnResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            statement_type="treasury_stock",
            no_data=True,
        )

    def fetch_capital_change(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
    ) -> DartCapitalChangeResult:
        self.capital_change_calls += 1
        return DartCapitalChangeResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            records=[
                DartCapitalChangeLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    rcept_no="20260310002820",
                    corp_cls="Y",
                    isu_dcrs_de=date(2025, 6, 30),
                    isu_dcrs_stle="유상증자(일반공모)",
                    isu_dcrs_stock_knd="보통주",
                    isu_dcrs_qy=1000000,
                    isu_dcrs_mstvdv_fval_amount=Decimal("500"),
                    isu_dcrs_mstvdv_fval_amount2=Decimal("70000"),
                    stlm_dt=date(2025, 12, 31),
                    source=Source.OPENDART,
                    fetched_at=now_kst(),
                    raw_payload={"isu_dcrs_stle": "유상증자(일반공모)"},
                )
            ],
        )


class MockShareInfoStorage(FakeSliceStorageMixin):
    def __init__(self) -> None:
        super().__init__()
        self.runs: list[IngestionRun] = []
        self.share_count_rows: list[DartShareCountLine] = []
        self.return_rows: list[DartShareholderReturnLine] = []
        self.capital_change_rows: list[DartCapitalChangeLine] = []
        self.existing_share_count_requests: set[tuple[str, int, str]] = set()
        self.existing_return_requests: set[tuple[str, int, str, str]] = set()
        self.existing_capital_change_requests: set[tuple[str, int, str]] = set()

    def record_run(self, run: IngestionRun) -> None:
        self.runs.append(run)

    def get_dart_corp_master(
        self,
        active_only: bool = True,
        tickers: list[str] | None = None,
        include_delisted: bool = False,
    ) -> list[DartCorp]:
        records = [_sample_corp()]
        if tickers is None:
            return records
        return [record for record in records if record.ticker in tickers]

    def get_existing_dart_share_count_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str]]:
        return {
            key
            for key in self.existing_share_count_requests
            if key[1] in bsns_years
            and key[2] in reprt_codes
            and (corp_codes is None or key[0] in corp_codes)
        }

    def get_existing_dart_shareholder_return_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        return {
            key
            for key in self.existing_return_requests
            if key[1] in bsns_years
            and key[2] in reprt_codes
            and (corp_codes is None or key[0] in corp_codes)
        }

    def get_existing_dart_capital_change_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str]]:
        return {
            key
            for key in self.existing_capital_change_requests
            if key[1] in bsns_years
            and key[2] in reprt_codes
            and (corp_codes is None or key[0] in corp_codes)
        }

    def upsert_dart_share_count_raw(self, records: list[DartShareCountLine]) -> UpsertResult:
        self.share_count_rows.extend(records)
        return UpsertResult(updated=len(records))

    def upsert_dart_shareholder_return_raw(
        self,
        records: list[DartShareholderReturnLine],
    ) -> UpsertResult:
        self.return_rows.extend(records)
        return UpsertResult(updated=len(records))

    def upsert_dart_capital_change_raw(
        self,
        records: list[DartCapitalChangeLine],
    ) -> UpsertResult:
        self.capital_change_rows.extend(records)
        return UpsertResult(updated=len(records))


def test_sync_dart_share_info_counts_results() -> None:
    storage = MockShareInfoStorage()
    provider = MockShareInfoProvider()

    result = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.targets_processed == 1
    assert result.requests_attempted == 3
    assert result.requests_skipped == 0
    assert result.share_count_rows_upserted == 1
    assert result.shareholder_return_rows_upserted == 1
    assert result.no_data_requests == 1
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_sync_dart_share_info_skips_existing_raw_requests() -> None:
    storage = MockShareInfoStorage()
    storage.existing_share_count_requests.add(("00126380", 2025, "11011"))
    storage.existing_return_requests.add(("00126380", 2025, "11011", "dividend"))
    storage.existing_return_requests.add(("00126380", 2025, "11011", "treasury_stock"))
    provider = MockShareInfoProvider()

    result = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.requests_attempted == 0
    assert result.requests_skipped == 3
    assert provider.share_count_calls == 0
    assert provider.dividend_calls == 0
    assert provider.treasury_stock_calls == 0
    assert storage.share_count_rows == []
    assert storage.return_rows == []


def test_sync_dart_share_info_force_bypasses_existing_check() -> None:
    storage = MockShareInfoStorage()
    storage.existing_share_count_requests.add(("00126380", 2025, "11011"))
    storage.existing_return_requests.add(("00126380", 2025, "11011", "dividend"))
    storage.existing_return_requests.add(("00126380", 2025, "11011", "treasury_stock"))
    provider = MockShareInfoProvider()

    result = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        force=True,
    )

    assert result.requests_attempted == 3
    assert result.requests_skipped == 0
    assert provider.share_count_calls == 1
    assert provider.dividend_calls == 1
    assert provider.treasury_stock_calls == 1


def test_sync_dart_share_info_skips_sleep_when_all_sub_requests_cached(monkeypatch) -> None:
    storage = MockShareInfoStorage()
    storage.existing_share_count_requests.add(("00126380", 2025, "11011"))
    storage.existing_return_requests.add(("00126380", 2025, "11011", "dividend"))
    storage.existing_return_requests.add(("00126380", 2025, "11011", "treasury_stock"))

    sleep_calls: list[float] = []
    monkeypatch.setattr(
        "collector.kr.service.sync_dart_share_info.sleep_with_jitter",
        lambda seconds: sleep_calls.append(seconds),
    )

    sync_dart_share_info(
        share_count_provider=MockShareInfoProvider(),
        shareholder_return_provider=MockShareInfoProvider(),
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.5,
    )

    assert sleep_calls == []


def test_sync_dart_share_info_omits_capital_change_when_provider_not_given() -> None:
    storage = MockShareInfoStorage()
    provider = MockShareInfoProvider()

    result = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.requests_attempted == 3
    assert provider.capital_change_calls == 0
    assert storage.capital_change_rows == []
    assert result.capital_change_rows_upserted == 0


def test_sync_dart_share_info_with_capital_change_counts_results() -> None:
    storage = MockShareInfoStorage()
    provider = MockShareInfoProvider()

    result = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        capital_change_provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.requests_attempted == 4
    assert provider.capital_change_calls == 1
    assert result.capital_change_rows_upserted == 1
    assert len(storage.capital_change_rows) == 1


def test_sync_dart_share_info_skips_existing_capital_change_request() -> None:
    storage = MockShareInfoStorage()
    storage.existing_capital_change_requests.add(("00126380", 2025, "11011"))
    provider = MockShareInfoProvider()

    result = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        capital_change_provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.requests_attempted == 3
    assert result.requests_skipped == 1
    assert provider.capital_change_calls == 0


def test_sync_dart_share_info_persists_and_skips_each_no_data_endpoint() -> None:
    storage = MockShareInfoStorage()
    provider = MockShareInfoProvider()

    first = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        scope=UniverseScope.HISTORICAL,
    )
    treasury_calls = provider.treasury_stock_calls
    second = sync_dart_share_info(
        share_count_provider=provider,
        shareholder_return_provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        scope=UniverseScope.HISTORICAL,
    )

    assert first.no_data_requests == 1
    assert provider.treasury_stock_calls == treasury_calls
    assert second.requests_skipped >= 1
    state = storage.slice_states[
        (Source.OPENDART, "tesstkAcqsDspsSttus", "00126380:2025:11011")
    ]
    assert state.status.value == "no_data"
