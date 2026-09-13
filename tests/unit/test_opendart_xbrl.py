import io
import zipfile
from datetime import date
from decimal import Decimal

from collector.kr.adapters.opendart_xbrl.provider import (
    OpenDartXbrlProvider,
    parse_xbrl_zip_response,
)
from collector.kr.domain.enums import Market, RunStatus, RunType, Source, UniverseScope
from collector.kr.domain.models import (
    DartCorp,
    DartFinancialStatementLine,
    DartXbrlDocument,
    DartXbrlFactLine,
    DartXbrlResult,
    IngestionRun,
    UpsertResult,
    XbrlBackfillTarget,
)
from collector.kr.service.sync_dart_xbrl import sync_dart_xbrl, sync_dart_xbrl_receipt_targeted
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


def _build_sample_xbrl_zip() -> bytes:
    weighted_concept = "ifrs-full_WeightedAverageNumberOfOrdinarySharesOutstandingBasic"
    depreciation_concept = "ifrs-full_DepreciationExpense"
    weighted_label = "기본주당이익 계산에 사용된 가중평균주식수"
    instance_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl
    xmlns:xbrli="http://www.xbrl.org/2003/instance"
    xmlns:ifrs-full="http://xbrl.ifrs.org/taxonomy/2023-03-23/ifrs-full"
    xmlns:xlink="http://www.w3.org/1999/xlink">
  <xbrli:context id="D2025">
    <xbrli:entity>
      <xbrli:identifier scheme="http://example.com">00126380</xbrli:identifier>
    </xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2025-01-01</xbrli:startDate>
      <xbrli:endDate>2025-12-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>
  <xbrli:unit id="shares">
    <xbrli:measure>xbrli:shares</xbrli:measure>
  </xbrli:unit>
  <xbrli:unit id="krw">
    <xbrli:measure>iso4217:KRW</xbrli:measure>
  </xbrli:unit>
  <{weighted_concept}
      contextRef="D2025" unitRef="shares" decimals="0">6630180138</{weighted_concept}>
  <{depreciation_concept}
      contextRef="D2025" unitRef="krw" decimals="0">12345</{depreciation_concept}>
</xbrli:xbrl>
"""
    label_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<link:linkbase
    xmlns:link="http://www.xbrl.org/2003/linkbase"
    xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:labelLink xlink:type="extended" xlink:role="http://www.xbrl.org/2003/role/link">
    <link:loc xlink:type="locator"
        xlink:href="entity00126380_2025-12-31.xsd#{weighted_concept}"
        xlink:label="loc1"/>
    <link:label xlink:type="resource" xlink:label="lab1">{weighted_label}</link:label>
    <link:labelArc xlink:type="arc" xlink:from="loc1" xlink:to="lab1"/>
  </link:labelLink>
</link:linkbase>
"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("entity00126380_2025-12-31.xbrl", instance_xml)
        archive.writestr("entity00126380_2025-12-31_lab-ko.xml", label_xml)
        archive.writestr("entity00126380_2025-12-31.xsd", "<schema/>")
    return buffer.getvalue()


def test_parse_xbrl_zip_response_extracts_document_and_facts() -> None:
    corp = _sample_corp()

    result = parse_xbrl_zip_response(
        _build_sample_xbrl_zip(),
        corp=corp,
        bsns_year=2025,
        reprt_code="11011",
        rcept_no="20260310002820",
    )

    assert result.error is None
    assert result.document is not None
    assert result.document.zip_entry_count == 3
    assert result.document.instance_document_name.endswith(".xbrl")
    assert len(result.facts) == 2
    weighted = next(
        fact
        for fact in result.facts
        if fact.concept_id == "ifrs-full_WeightedAverageNumberOfOrdinarySharesOutstandingBasic"
    )
    assert weighted.value_numeric == Decimal("6630180138")
    assert weighted.label_ko == "기본주당이익 계산에 사용된 가중평균주식수"
    assert weighted.context_type == "duration"
    assert weighted.period_end == date(2025, 12, 31)


def test_open_dart_xbrl_provider_maps_file_missing_as_no_data() -> None:
    corp = _sample_corp()
    payload = (
        "<result><status>014</status>" "<message>파일이 존재하지 않습니다.</message></result>"
    ).encode()
    provider = OpenDartXbrlProvider(
        request_executor=FakeOpenDartExecutor(
            [
                payload,
            ]
        )
    )

    result = provider.fetch_xbrl(corp, 2025, "11011", "20260310002820")

    assert result.no_data is True
    assert result.status_code == "014"
    assert result.error is None


def test_open_dart_xbrl_provider_maps_no_data_status_013() -> None:
    corp = _sample_corp()
    payload = (
        "<result><status>013</status>" "<message>조회된 데이타가 없습니다.</message></result>"
    ).encode()
    provider = OpenDartXbrlProvider(
        request_executor=FakeOpenDartExecutor(
            [
                payload,
            ]
        )
    )

    result = provider.fetch_xbrl(corp, 2025, "11011", "20260310002820")

    assert result.no_data is True
    assert result.status_code == "013"
    assert result.error is None


class MockXbrlProvider:
    def __init__(self) -> None:
        self.calls = 0

    def fetch_xbrl(
        self,
        corp: DartCorp,
        bsns_year: int,
        reprt_code: str,
        rcept_no: str,
    ) -> DartXbrlResult:
        self.calls += 1
        fetched_at = now_kst()
        return DartXbrlResult(
            corp_code=corp.corp_code,
            ticker=corp.ticker or "",
            bsns_year=bsns_year,
            reprt_code=reprt_code,
            rcept_no=rcept_no,
            document=DartXbrlDocument(
                corp_code=corp.corp_code,
                ticker=corp.ticker or "",
                bsns_year=bsns_year,
                reprt_code=reprt_code,
                rcept_no=rcept_no,
                zip_entry_count=3,
                instance_document_name="entity.xbrl",
                label_ko_document_name="entity_lab-ko.xml",
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload={},
            ),
            facts=[
                DartXbrlFactLine(
                    corp_code=corp.corp_code,
                    ticker=corp.ticker or "",
                    bsns_year=bsns_year,
                    reprt_code=reprt_code,
                    rcept_no=rcept_no,
                    concept_id="ifrs-full_WeightedAverageNumberOfOrdinarySharesOutstandingBasic",
                    concept_name="WeightedAverageNumberOfOrdinarySharesOutstandingBasic",
                    namespace_uri="http://xbrl.ifrs.org/taxonomy/2023-03-23/ifrs-full",
                    context_id="ctx1",
                    context_type="duration",
                    period_start=date(2025, 1, 1),
                    period_end=date(2025, 12, 31),
                    instant_date=None,
                    dimensions=[],
                    unit_id="shares",
                    unit_measure="shares",
                    decimals="0",
                    value_numeric=Decimal("6630180138"),
                    value_text="6630180138",
                    is_nil=False,
                    label_ko="가중평균주식수",
                    source=Source.OPENDART,
                    fetched_at=fetched_at,
                    raw_payload={},
                )
            ],
        )


class MockXbrlStorage(FakeSliceStorageMixin):
    def __init__(self) -> None:
        super().__init__()
        self.runs: list[IngestionRun] = []
        self.documents: list[DartXbrlDocument] = []
        self.facts: list[DartXbrlFactLine] = []
        self.existing_xbrl_documents: set[tuple[str, int, str, str]] = set()

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

    def get_existing_dart_xbrl_document_keys(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        corp_codes: list[str] | None = None,
    ) -> set[tuple[str, int, str, str]]:
        return {
            key
            for key in self.existing_xbrl_documents
            if key[1] in bsns_years
            and key[2] in reprt_codes
            and (corp_codes is None or key[0] in corp_codes)
        }

    def get_dart_financial_statement_raw(
        self,
        bsns_years: list[int],
        reprt_codes: list[str],
        tickers: list[str] | None = None,
    ) -> list[DartFinancialStatementLine]:
        fetched_at = now_kst()
        return [
            DartFinancialStatementLine(
                corp_code="00126380",
                ticker="005930",
                bsns_year=2025,
                reprt_code="11011",
                fs_div="CFS",
                sj_div="IS",
                sj_nm="손익계산서",
                account_id="ifrs-full_Revenue",
                account_nm="매출액",
                account_detail="",
                thstrm_nm="제56기",
                thstrm_amount=Decimal("100"),
                thstrm_add_amount=None,
                frmtrm_nm="제55기",
                frmtrm_amount=Decimal("90"),
                frmtrm_q_nm="",
                frmtrm_q_amount=None,
                frmtrm_add_amount=None,
                bfefrmtrm_nm="제54기",
                bfefrmtrm_amount=Decimal("80"),
                ord=1,
                currency="KRW",
                rcept_no="20260310002820",
                source=Source.OPENDART,
                fetched_at=fetched_at,
                raw_payload={},
            )
        ]

    def upsert_dart_xbrl_documents(self, records: list[DartXbrlDocument]) -> UpsertResult:
        self.documents.extend(records)
        return UpsertResult(updated=len(records))

    def upsert_dart_xbrl_fact_raw(self, records: list[DartXbrlFactLine]) -> UpsertResult:
        self.facts.extend(records)
        return UpsertResult(updated=len(records))


def test_sync_dart_xbrl_writes_documents_and_facts() -> None:
    storage = MockXbrlStorage()
    provider = MockXbrlProvider()

    result = sync_dart_xbrl(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.targets_processed == 1
    assert result.requests_attempted == 1
    assert result.requests_skipped == 0
    assert provider.calls == 1
    assert result.documents_upserted == 1
    assert result.facts_upserted == 1
    assert len(storage.documents) == 1
    assert len(storage.facts) == 1
    assert storage.runs[0].run_type == RunType.XBRL_PARSE
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_sync_dart_xbrl_skips_existing_document() -> None:
    storage = MockXbrlStorage()
    storage.existing_xbrl_documents.add(("00126380", 2025, "11011", "20260310002820"))
    provider = MockXbrlProvider()

    result = sync_dart_xbrl(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.requests_attempted == 0
    assert result.requests_skipped == 1
    assert provider.calls == 0
    assert storage.documents == []
    assert storage.facts == []


def test_sync_dart_xbrl_force_bypasses_existing_check() -> None:
    storage = MockXbrlStorage()
    storage.existing_xbrl_documents.add(("00126380", 2025, "11011", "20260310002820"))
    provider = MockXbrlProvider()

    result = sync_dart_xbrl(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        force=True,
    )

    assert result.requests_attempted == 1
    assert result.requests_skipped == 0
    assert provider.calls == 1
    assert len(storage.documents) == 1


def test_sync_dart_xbrl_persists_and_skips_no_data() -> None:
    class NoDataProvider(MockXbrlProvider):
        def fetch_xbrl(self, corp, bsns_year, reprt_code, rcept_no):
            self.calls += 1
            return DartXbrlResult(no_data=True)

    storage = MockXbrlStorage()
    provider = NoDataProvider()

    first = sync_dart_xbrl(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        scope=UniverseScope.HISTORICAL,
    )
    second = sync_dart_xbrl(
        provider=provider,
        storage=storage,
        bsns_years=[2025],
        reprt_codes=["11011"],
        tickers=["005930"],
        rate_limit_seconds=0.0,
        scope=UniverseScope.HISTORICAL,
    )

    assert first.no_data_requests == 1
    assert second.requests_attempted == 0
    assert second.requests_skipped == 1
    assert provider.calls == 1
    state = storage.slice_states[
        (Source.OPENDART, "fnlttXbrl", "00126380:2025:11011:20260310002820")
    ]
    assert state.status.value == "no_data"


def test_sync_dart_xbrl_receipt_targeted_fetches_explicit_targets() -> None:
    storage = MockXbrlStorage()
    provider = MockXbrlProvider()
    targets = [
        XbrlBackfillTarget(
            ticker="005930",
            corp_code="00126380",
            bsns_year=2025,
            reprt_code="11011",
            rcept_no="20250101000001",
        )
    ]

    result = sync_dart_xbrl_receipt_targeted(
        provider=provider,
        storage=storage,
        targets=targets,
        rate_limit_seconds=0.0,
    )

    assert result.errors == {}
    assert result.targets_processed == 1
    assert result.requests_attempted == 1
    assert provider.calls == 1
    assert len(storage.documents) == 1
    assert storage.documents[0].rcept_no == "20250101000001"
    assert storage.runs[0].run_type == RunType.XBRL_RECEIPT_BACKFILL
    assert storage.runs[-1].status == RunStatus.SUCCESS


def test_sync_dart_xbrl_receipt_targeted_skips_existing_document() -> None:
    storage = MockXbrlStorage()
    storage.existing_xbrl_documents.add(("00126380", 2025, "11011", "20250101000001"))
    provider = MockXbrlProvider()
    targets = [
        XbrlBackfillTarget(
            ticker="005930",
            corp_code="00126380",
            bsns_year=2025,
            reprt_code="11011",
            rcept_no="20250101000001",
        )
    ]

    result = sync_dart_xbrl_receipt_targeted(
        provider=provider,
        storage=storage,
        targets=targets,
        rate_limit_seconds=0.0,
    )

    assert result.requests_attempted == 0
    assert result.requests_skipped == 1
    assert provider.calls == 0


def test_sync_dart_xbrl_receipt_targeted_errors_on_unmapped_ticker() -> None:
    storage = MockXbrlStorage()
    provider = MockXbrlProvider()
    targets = [
        XbrlBackfillTarget(
            ticker="000000",
            corp_code="00000000",
            bsns_year=2025,
            reprt_code="11011",
            rcept_no="20250101000001",
        )
    ]

    result = sync_dart_xbrl_receipt_targeted(
        provider=provider,
        storage=storage,
        targets=targets,
        rate_limit_seconds=0.0,
    )

    assert result.requests_attempted == 0
    assert len(result.errors) == 1
    assert provider.calls == 0


def test_sync_dart_xbrl_receipt_targeted_requires_targets() -> None:
    storage = MockXbrlStorage()
    provider = MockXbrlProvider()

    result = sync_dart_xbrl_receipt_targeted(
        provider=provider,
        storage=storage,
        targets=[],
        rate_limit_seconds=0.0,
    )

    assert "pipeline" in result.errors
    assert storage.runs[-1].status == RunStatus.FAILED
