"""ETF 총보수(DIS) 서비스 — sync·backfill·import-research·verify. 네트워크 없음.

가짜 클라이언트가 날짜별로 정해 둔 본문을 돌려준다. 본문은 연구용 사본에서 2~3행만 잘라 온
fixture(``tests/fixtures/kofia_dis_fee``)다.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from collector.kr.adapters.kofia_dis_fee import DisCounters, DisRawResponse, DisRequestError
from collector.kr.baseline import BaselineStore
from collector.kr.service import etf_fee_dis as ed
from collector.kr.service.etf_fee_dis_verify import verify

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "kofia_dis_fee"
TODAY = date(2026, 10, 11)
EMPTY = (
    b"<root><message><DISCondFuncListDTO><dbio_total_count_>0</dbio_total_count_>"
    b"</DISCondFuncListDTO></message></root>"
)
LATEST_0831 = (
    b"<root><message><proframeHeader><pfmTrDate>20261011</pfmTrDate></proframeHeader>"
    b"<DISCondFuncListDTO><selectMeta><v>20260831</v></selectMeta></DISCondFuncListDTO>"
    b"</message></root>"
)


def fx(name: str) -> bytes:
    return (FIX / name).read_bytes()


class FakeDis:
    """``post(xml)``가 요청의 기준일로 본문을 고른다. 정해 두지 않은 날짜는 빈 응답이다."""

    def __init__(self, fees: dict[str, bytes | Exception], latest: bytes | Exception) -> None:
        self.fees = fees
        self.latest = latest
        self.counters = DisCounters()
        self.requested: list[str] = []
        self.clock = datetime(2026, 10, 11, 9, 0, tzinfo=UTC)

    def post(self, xml: str) -> DisRawResponse:
        self.counters.http_requests += 1
        if "DISComStdYMDSO" in xml:
            self.requested.append("latest")
            item = self.latest
        else:
            ymd = re.search(r"<tmpV30>(\d{8})</tmpV30>", xml)[1]
            self.requested.append(ymd)
            item = self.fees.get(ymd, EMPTY)
        if isinstance(item, Exception):
            raise item
        self.clock = self.clock.replace(second=self.clock.second + 1)
        return DisRawResponse(item, self.clock, 200)


def make_research(tmp: Path, days: dict[str, str], *, extra_noise: bool = True) -> tuple[Path, str]:
    """연구용 사본과 같은 배치(``manifest.jsonl`` + ``raw/<날짜>.xml.gz`` + ``SHA256SUMS``)."""
    root = tmp / "research"
    (root / "raw").mkdir(parents=True)
    lines, sums = [], []
    minute = 3
    for ymd, fixture in sorted(days.items()):
        data = fx(fixture)
        gz = gzip.compress(data, mtime=0)
        (root / "raw" / f"{ymd}.xml.gz").write_bytes(gz)
        rows = int(re.search(rb"<dbio_total_count_>(\d+)<", data)[1])
        lines.append(
            {
                "month": f"{ymd[:4]}-{ymd[4:6]}", "date": ymd, "status": 200, "rows": rows,
                "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                "fetched_at": f"2026-10-11T00:{minute:02d}:12", "file": f"raw/{ymd}.xml.gz",
            }
        )  # fmt: skip
        sums.append(f"{hashlib.sha256(gz).hexdigest()}  raw/{ymd}.xml.gz")
        minute += 1
    if extra_noise:  # 사본 manifest에는 빈 날·실패 줄이 섞여 있다 — 들이지 않는다
        lines.insert(0, {"month": "2003-04", "date": "20030430", "status": 200, "rows": 0})
        lines.append({"month": "2005-10", "date": "20051031", "error": "IncompleteRead"})
    text = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in lines)
    (root / "manifest.jsonl").write_text(text, "utf-8")
    (root / "SHA256SUMS").write_text("\n".join(sums) + "\n", "utf-8")
    return root, hashlib.sha256(text.encode("utf-8")).hexdigest()


THREE_MONTHS = {
    "20260630": "fee_20260731.xml",
    "20260731": "fee_20260731.xml",
    "20260831": "fee_20260831.xml",
}


@pytest.fixture
def store(tmp_path: Path) -> BaselineStore:
    return BaselineStore(tmp_path / "lake" / "krx_baseline")


@pytest.fixture
def imported(store: BaselineStore, tmp_path: Path) -> BaselineStore:
    """2026-06·07 두 달을 사본으로 들인 저장소(08은 아직 안 받음)."""
    path, sha = make_research(tmp_path, {k: v for k, v in THREE_MONTHS.items() if k < "20260801"})
    ed.import_research(store=store, path=path, expect_manifest_sha256=sha)
    return store


# ------------------------------------------------------------ import-research


def test_import_makes_first_observations_and_keeps_the_research_fetched_at(
    store: BaselineStore, tmp_path: Path
) -> None:
    path, sha = make_research(tmp_path, THREE_MONTHS)
    result = ed.import_research(store=store, path=path, expect_manifest_sha256=sha)
    assert (result.files_total, result.imported, result.skipped_done) == (3, 3, 0)
    assert result.rows == 2 + 2 + 3
    obs = store.read_observations(ed.SPEC, "all")
    assert len(obs) == 7 and set(obs["obs_seq"]) == {1} and set(obs["obs_kind"]) == {"value"}
    # 00:03 KST(시간대 없는 문자열) → UTC 전날 15:03. 가용 시점은 이 값이다.
    first = obs[obs["STD_YMD"] == "20260630"]["fetched_at"].min()
    assert first.isoformat() == "2026-10-10T15:03:12+00:00"
    assert set(obs["fetched_at_basis"]) == {"response"}
    assert (obs["source_run"] == "import_research_dis").all()
    assert set(ed.completed_std_dates(store)) == set(THREE_MONTHS)
    assert verify(store, today=TODAY).ok


def test_import_twice_adds_nothing(store: BaselineStore, tmp_path: Path) -> None:
    path, sha = make_research(tmp_path, THREE_MONTHS)
    ed.import_research(store=store, path=path, expect_manifest_sha256=sha)
    before = sorted(p.name for p in store.base.rglob("*") if p.is_file())
    again = ed.import_research(store=store, path=path, expect_manifest_sha256=sha)
    assert (again.imported, again.skipped_done) == (0, 3)
    assert sorted(p.name for p in store.base.rglob("*") if p.is_file()) == before


def test_import_refuses_a_wrong_manifest_hash_and_writes_nothing(
    store: BaselineStore, tmp_path: Path
) -> None:
    path, _ = make_research(tmp_path, THREE_MONTHS)
    with pytest.raises(ed.DisImportError, match="manifest.jsonl sha256"):
        ed.import_research(store=store, path=path, expect_manifest_sha256="0" * 64)
    assert not store.base.exists() or not any(store.base.rglob("*.gz"))


@pytest.mark.parametrize(
    "tamper",
    ["body", "sums", "truncated", "missing"],
)
def test_import_verifies_every_file_before_writing(
    store: BaselineStore, tmp_path: Path, tamper: str
) -> None:
    path, sha = make_research(tmp_path, THREE_MONTHS)
    victim = path / "raw" / "20260731.xml.gz"
    if tamper == "body":  # 본문이 바뀜(SHA256SUMS를 같이 맞춰도 본문 sha256이 manifest와 다르다)
        victim.write_bytes(gzip.compress(fx("fee_20260831.xml"), mtime=0))
        sums = path / "SHA256SUMS"
        sums.write_text(
            "\n".join(
                f"{hashlib.sha256(p.read_bytes()).hexdigest()}  raw/{p.name}"
                for p in sorted((path / "raw").glob("*.gz"))
            )
            + "\n"
        )
    elif tamper == "sums":  # gz 바이트만 달라짐(같은 본문) → SHA256SUMS와 다름
        victim.write_bytes(gzip.compress(fx("fee_20260731.xml"), compresslevel=1, mtime=5))
    elif tamper == "truncated":
        data = fx("fee_20260731.xml")[:-30]
        gz = gzip.compress(data, mtime=0)
        victim.write_bytes(gz)
        (path / "SHA256SUMS").write_text("")  # 해시 대조는 건너뛰고 본문 검사를 본다
        manifest = path / "manifest.jsonl"
        lines = [json.loads(x) for x in manifest.read_text().splitlines()]
        for rec in lines:
            if rec.get("date") == "20260731":
                rec["sha256"] = hashlib.sha256(data).hexdigest()
        text = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in lines)
        manifest.write_text(text)
        sha = hashlib.sha256(text.encode()).hexdigest()
    else:
        victim.unlink()
    with pytest.raises(ed.DisImportError):
        ed.import_research(store=store, path=path, expect_manifest_sha256=sha)
    assert not store.base.exists() or not any(store.base.rglob("*.gz"))


def test_import_refuses_when_another_run_already_wrote(
    imported: BaselineStore, tmp_path: Path
) -> None:
    path, sha = make_research(tmp_path / "again", THREE_MONTHS)
    with pytest.raises(ed.DisImportError):
        ed.import_research(
            store=imported, path=path, expect_manifest_sha256=sha, run_id="import_other"
        )


# ----------------------------------------------------------------------- sync


def test_sync_on_an_empty_store_refuses_to_start_history(store: BaselineStore) -> None:
    fake = FakeDis({}, LATEST_0831)
    result = ed.sync(store=store, client=fake, today=TODAY)
    assert result.stopped_by == "empty_store" and fake.requested == []


def test_sync_fetches_a_new_month_once_and_a_second_run_is_a_single_request(
    imported: BaselineStore,
) -> None:
    fake = FakeDis({"20260831": fx("fee_20260831.xml")}, LATEST_0831)
    first = ed.sync(store=imported, client=fake, today=TODAY)
    assert first.months_fetched == ["20260831"] and first.new_obs == 1 and first.failures == 0
    assert fake.requested == ["latest", "20260831"]  # 최근 기준일 힌트로 날짜를 바로 물었다
    obs = imported.read_observations(ed.SPEC, "all")
    new = obs[obs["STD_YMD"] == "20260831"]
    assert len(new) == 3 and set(new["obs_seq"]) == {1}
    assert (new["source_run"] == "daily").all()
    # fetched_at은 이번에 처음 본 시각이다(사본 값보다 늦다)
    assert new["fetched_at"].min().isoformat().startswith("2026-10-11T09:00")
    stored = ed.stored_months(imported)
    assert stored[(2026, 8)] == "20260831"

    fake2 = FakeDis({"20260831": fx("fee_20260831.xml")}, LATEST_0831)
    second = ed.sync(store=imported, client=fake2, today=TODAY)
    assert second.nothing_new and second.http_requests == 1 and fake2.requested == ["latest"]
    assert len(imported.read_observations(ed.SPEC, "all")) == len(obs)  # 관측 안 늘었다
    # 최근 기준일 조회가 시도마다 기록돼 공개 지연의 시계열이 된다
    log = imported.read_fetch_log(ed.SERVICE)
    probes = log[log["request_key"] == ed.LATEST_KEY]
    assert list(probes["day_kind"]) == ["latest=20260831", "latest=20260831"]
    assert verify(imported, today=TODAY).ok


def test_sync_is_a_no_op_when_the_latest_month_is_already_stored(imported: BaselineStore) -> None:
    latest_0731 = LATEST_0831.replace(b"20260831", b"20260731")
    fake = FakeDis({}, latest_0731)
    result = ed.sync(store=imported, client=fake, today=TODAY)
    assert result.nothing_new and fake.requested == ["latest"]


def test_sync_uses_month_end_search_for_gap_months_and_the_hint_for_the_latest(
    imported: BaselineStore,
) -> None:
    # 저장 마지막 달은 7월, 최근 기준일은 9월(오늘 10/11이라 9월까지 받을 수 있다).
    fake = FakeDis(
        {
            "20260831": fx("fee_20260831.xml"),
            "20260930": fx("fee_20260831.xml"),
        },
        LATEST_0831.replace(b"20260831", b"20260930"),
    )
    result = ed.sync(store=imported, client=fake, today=TODAY)
    # 7월은 이미 저장됨. 8월은 월말 탐색(0831 첫 후보에서 적중), 9월은 최근 기준일 힌트
    assert result.months_fetched == ["20260831", "20260930"]
    assert fake.requested == ["latest", "20260831", "20260930"]


def test_sync_falls_back_to_direct_probing_when_latest_cannot_be_read(
    imported: BaselineStore,
) -> None:
    fake = FakeDis({"20260828": fx("fee_20260831.xml")}, b"<root><message/></root>")
    result = ed.sync(store=imported, client=fake, today=TODAY)
    assert result.discovery == "direct"
    # 8월 월말 후보: 0831(빈) → 0828(적중), 이어서 9월 후보 4개는 모두 빈 응답
    assert fake.requested[:3] == ["latest", "20260831", "20260828"]
    assert result.months_fetched == ["20260828"]
    assert result.months_missing == ["2026-09"] and result.failures == 0
    assert fake.requested[3:] == ["20260930", "20260929", "20260928", "20260925"]
    assert result.http_requests == 1 + 2 + 4


def test_direct_discovery_with_nothing_published_asks_four_candidates(
    imported: BaselineStore,
) -> None:
    fake = FakeDis({}, LATEST_0831)
    result = ed.sync(store=imported, client=fake, today=TODAY, discovery="direct")
    assert fake.requested == ["20260831", "20260828", "20260827", "20260826"]
    assert result.months_fetched == [] and result.failures == 0


def test_a_failed_date_is_not_treated_as_empty_and_the_next_run_retries_it(
    imported: BaselineStore,
) -> None:
    broken = DisRequestError("truncated response (54 bytes, no </root>)")
    fake = FakeDis({"20260831": broken}, LATEST_0831)
    result = ed.sync(store=imported, client=fake, today=TODAY)
    assert result.failures == 1 and result.months_fetched == []
    assert fake.requested == ["latest", "20260831"]  # 앞날짜(0828)로 넘어가지 않았다
    assert (2026, 8) not in ed.stored_months(imported)
    log = imported.read_fetch_log(ed.SERVICE)
    assert "failed" in set(log["result"])

    fake2 = FakeDis({"20260831": fx("fee_20260831.xml")}, LATEST_0831)
    retry = ed.sync(store=imported, client=fake2, today=TODAY)
    assert retry.months_fetched == ["20260831"] and retry.failures == 0


def test_max_calls_stops_cleanly(imported: BaselineStore) -> None:
    fake = FakeDis({}, LATEST_0831)
    result = ed.sync(store=imported, client=fake, today=TODAY, discovery="direct", max_calls=2)
    assert result.stopped_by == "max_calls" and len(fake.requested) == 2


def test_orphan_raw_is_recovered_without_a_new_request(imported: BaselineStore) -> None:
    """원문만 쓰고 죽은 요청은 다음 실행이 다시 받지 않고 원문에서 정규화한다."""
    imported.write_raw(
        ed.SERVICE, "std_ymd=20260831", fx("fee_20260831.xml"),
        datetime(2026, 10, 11, 8, 0, tzinfo=UTC), ext="xml",
    )  # fmt: skip
    fake = FakeDis({}, LATEST_0831)
    result = ed.sync(store=imported, client=fake, today=TODAY)
    assert result.recovered == 1
    assert ed.stored_months(imported)[(2026, 8)] == "20260831"


# ------------------------------------------------------------------- backfill


def test_backfill_fill_skips_stored_months_and_steps_back_for_missing_ones(
    imported: BaselineStore,
) -> None:
    fake = FakeDis({"20260529": fx("fee_20260731.xml")}, LATEST_0831)
    result = ed.backfill(store=imported, client=fake, start=(2026, 5), end=(2026, 7), today=TODAY)
    # 5월(말일 일요일)은 0529가 첫 후보. 6·7월은 저장돼 있어 묻지 않는다
    assert fake.requested == ["20260529"] and result.months_fetched == ["20260529"]
    assert result.months_planned == 3
    assert ed.stored_months(imported)[(2026, 5)] == "20260529"


def test_backfill_month_search_checks_up_to_four_weekdays(imported: BaselineStore) -> None:
    fake = FakeDis({"20260325": fx("fee_20260731.xml")}, LATEST_0831)  # 3/31(화)에서 4평일 앞
    result = ed.backfill(store=imported, client=fake, start=(2026, 3), end=(2026, 3), today=TODAY)
    assert fake.requested == ["20260331", "20260330", "20260327", "20260326"]
    assert result.months_missing == ["2026-03"]  # 0325는 후보 밖(3평일 앞까지만)


def test_backfill_reobserve_records_a_changed_value_as_a_new_observation_and_resumes(
    imported: BaselineStore,
) -> None:
    changed = fx("fee_20260731.xml").replace(b"<tmpV9>.405</tmpV9>", b"<tmpV9>.5</tmpV9>")
    assert changed != fx("fee_20260731.xml")
    fake = FakeDis({"20260731": changed, "20260630": fx("fee_20260731.xml")}, LATEST_0831)
    with pytest.raises(ValueError, match="run-id"):
        ed.backfill(
            store=imported, client=fake, start=(2026, 6), end=(2026, 7), mode="reobserve",
            today=TODAY,
        )  # fmt: skip
    first = ed.backfill(
        store=imported, client=fake, start=(2026, 6), end=(2026, 7), mode="reobserve",
        run_id="reobs_1", today=TODAY, max_calls=1,
    )  # fmt: skip
    assert first.stopped_by == "max_calls" and fake.requested == ["20260630"]
    second = ed.backfill(
        store=imported, client=fake, start=(2026, 6), end=(2026, 7), mode="reobserve",
        run_id="reobs_1", today=TODAY,
    )  # fmt: skip
    assert fake.requested == ["20260630", "20260731"]  # 끝낸 6월은 건너뛰고 이어 받았다
    assert second.months_fetched == ["20260731"]
    obs = imported.read_observations(ed.SPEC, "all")
    assert obs["obs_seq"].max() == 2  # 값이 바뀐 행만 새 관측
    assert verify(imported, today=TODAY).ok


# --------------------------------------------------------------------- verify


def test_verify_fails_on_a_missing_month_and_warns_when_stale(
    store: BaselineStore, tmp_path: Path
) -> None:
    path, sha = make_research(
        tmp_path, {"20260430": "fee_20260731.xml", "20260630": "fee_20260731.xml"}
    )
    ed.import_research(store=store, path=path, expect_manifest_sha256=sha)
    report = verify(store, today=date(2027, 1, 20))
    assert not report.ok
    assert any("빠진 달 1개" in m and "2026-05" in m for m in report.failures)
    assert any("마지막 기준일" in m for m in report.warnings)


def test_verify_detects_a_tampered_committed_file(imported: BaselineStore) -> None:
    parquet = next((imported.base / ed.SPEC.name).rglob("*.parquet"))
    parquet.write_bytes(parquet.read_bytes() + b"x")
    report = verify(imported, today=TODAY)
    assert not report.ok and any("manifest 대조" in m for m in report.failures)


def test_verify_detects_a_tampered_raw_body(imported: BaselineStore) -> None:
    raw = next((imported.base / "raw_responses" / ed.SERVICE).rglob("*.gz"))
    raw.write_bytes(gzip.compress(b"<root>cut</root>", mtime=0))
    report = verify(imported, today=TODAY)
    assert not report.ok


def test_verify_fails_on_an_empty_store(store: BaselineStore) -> None:
    store.base.mkdir(parents=True)
    assert not verify(store, today=TODAY).ok


def test_verify_checks_row_count_against_the_request_log(imported: BaselineStore) -> None:
    """요청 기록의 행 수(dbio_total_count_)와 관측의 행 수가 다르면 실패한다."""
    import pyarrow.parquet as pq  # noqa: PLC0415

    # 관측 parquet 한 파일의 행 하나를 빼서 다시 쓰고, manifest의 해시·크기를 맞춘다.
    parquet = next((imported.base / ed.SPEC.name / "year=2026").glob("*.parquet"))
    table = pq.read_table(parquet)
    pq.write_table(table.slice(0, table.num_rows - 1), parquet, compression="zstd")
    manifest = next((imported.base / "_manifest").glob("*.json"))
    data = json.loads(manifest.read_text())
    for entry in data["files"]:
        if entry["path"].endswith(parquet.name):
            entry["sha256"] = hashlib.sha256(parquet.read_bytes()).hexdigest()
            entry["bytes"] = parquet.stat().st_size
    manifest.write_text(json.dumps(data))
    report = verify(imported, today=TODAY)
    assert any("행 수가 요청 기록" in m for m in report.failures)
