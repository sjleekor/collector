"""기준선 저장 계약 (계획 04 §3.1) — R02·R05·R06·R08·R09의 완료 기준."""

from __future__ import annotations

import gzip
import hashlib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from collector.kr.baseline import (
    BaselineStore,
    BaselineWriter,
    ParsedResponse,
    TableSpec,
    completed_request_keys,
    fill_dates,
    is_pending,
    reobserve_keys,
    request_confirmations,
    row_hash,
    weekdays_between,
)

SPEC = TableSpec(
    name="t_daily",
    key_columns=("BAS_DD", "ISU_CD"),
    source_columns=("ISU_CD", "CLSPRC", "NETASST"),
    date_column="BAS_DD",
    parsed_columns={"clsprc_num": "float64"},
)
SVC = "svc"
T0 = datetime(2026, 10, 12, 9, 0, tzinfo=UTC)


def _row(day: str, code: str, price: str | None, netasst: str | None = "100") -> dict:
    return {
        "BAS_DD": day,
        "ISU_CD": code,
        "CLSPRC": price,
        "NETASST": netasst,
        "clsprc_num": float(price) if price else None,
    }


def _parsed(day: str, rows: list[dict], **kw) -> ParsedResponse:
    return ParsedResponse(SPEC, rows, {"BAS_DD": day}, **kw)


def _put(writer: BaselineWriter, day: str, rows: list[dict], at: datetime, **kw) -> str:
    key = f"bas_dd={day}"
    body = f"{day}|{at.isoformat()}|{rows}".encode()
    raw = writer.store.write_raw(SVC, key, body, at)
    return writer.add_response(
        service=SVC, request_key=key, raw=raw, parsed=_parsed(day, rows, **kw), key_slot=1
    )


def _writer(store: BaselineStore, run: str = "r1", source: str = "daily", **kw) -> BaselineWriter:
    return BaselineWriter(store, run, store.next_attempt(run), source, **kw)


@pytest.fixture
def store(tmp_path: Path) -> BaselineStore:
    return BaselineStore(tmp_path / "krx_baseline")


# ------------------------------------------------------------------ 원문


def test_raw_is_gzipped_deterministically_and_hashes_the_plain_body(store: BaselineStore) -> None:
    body = b'{"OutBlock_1": []}\n'
    ref = store.write_raw("etf", "bas_dd=20261009", body, T0)
    again = store.write_raw("etf", "bas_dd=20261009", body, T0)
    assert ref == again
    assert ref.sha256 == hashlib.sha256(body).hexdigest()
    assert ref.path.startswith("raw_responses/etf/bas_dd=20261009/20261012T090000000000Z_")
    assert ref.path.endswith(f"{ref.sha256[:12]}.json.gz")
    assert store.read_raw(ref.path) == body
    stored = (store.base / ref.path).read_bytes()
    assert stored == gzip.compress(body, compresslevel=6, mtime=0)


def test_imported_gzip_is_kept_byte_for_byte_with_the_callers_basis(store: BaselineStore) -> None:
    body = b'{"OutBlock_1": [{"A": "1"}]}'
    gz = gzip.compress(body, compresslevel=1, mtime=12345)
    ref = store.import_gzip_raw("etf", "bas_dd=20100104", gz, T0, "file_mtime")
    assert (store.base / ref.path).read_bytes() == gz
    assert ref.sha256 == hashlib.sha256(body).hexdigest()
    assert ref.fetched_at_basis == "file_mtime"
    with pytest.raises(ValueError):
        store.import_gzip_raw("etf", "bas_dd=20100104", gz, T0, "guess")


def test_request_keys_must_be_reversible_directory_names(store: BaselineStore) -> None:
    store.write_raw("seibro", "window=20260101_20260331/p=001", b"{}", T0)
    with pytest.raises(ValueError):
        store.write_raw("etf", "../escape", b"{}", T0)


# --------------------------------------------------- 관측 비교 · R05 · R09


def test_row_hash_uses_only_source_text_and_tells_none_from_empty() -> None:
    a = _row("20261009", "A", "10")
    b = dict(a, clsprc_num=999.0)  # 숫자 칸은 해시에 안 들어간다
    assert row_hash(SPEC, a) == row_hash(SPEC, b)
    assert row_hash(SPEC, _row("20261009", "A", "")) != row_hash(SPEC, _row("20261009", "A", None))
    with pytest.raises(TypeError):
        row_hash(SPEC, {"ISU_CD": "A", "CLSPRC": 10, "NETASST": "1"})


def test_the_first_observation_survives_repeated_identical_responses(store: BaselineStore) -> None:
    w = _writer(store)
    assert _put(w, "20261009", [_row("20261009", "A", "10")], T0) == "new_obs"
    w.flush()
    first = store.read_observations(SPEC, "all")
    manifest_files = {e["path"]: e["sha256"] for m in store.manifests() for e in m["files"]}

    w2 = _writer(store)
    for i in range(1, 4):
        assert _put(w2, "20261009", [_row("20261009", "A", "10")], T0 + timedelta(days=i)) == "same"
    w2.flush()

    after = store.read_observations(SPEC, "all")
    assert len(after) == 1
    cols = ["CLSPRC", "fetched_at", "row_hash", "obs_seq"]
    pd.testing.assert_frame_equal(first[cols], after[cols])
    assert store.verify_committed() == []
    for path, sha in manifest_files.items():  # 처음 쓴 파일은 그대로
        assert hashlib.sha256((store.base / path).read_bytes()).hexdigest() == sha
    # 확인은 요청 기록에 남고, 관측 표에는 first/last 칸이 없다
    assert "last_confirmed_at" not in after.columns and "year" not in after.columns
    conf = request_confirmations(store, SVC).iloc[0]
    assert conf["confirmations"] == 4
    assert conf["first_fetched_at"] == T0
    assert conf["last_confirmed_at"] == T0 + timedelta(days=3)


def test_a_then_b_then_a_makes_three_observations(store: BaselineStore) -> None:
    w = _writer(store)
    for i, price in enumerate(["10", "11", "11", "10"]):
        _put(w, "20261009", [_row("20261009", "A", price)], T0 + timedelta(days=i))
    w.flush()
    obs = store.read_observations(SPEC, "all").sort_values("obs_seq")
    assert list(obs["obs_seq"]) == [1, 2, 3]
    assert list(obs["CLSPRC"]) == ["10", "11", "10"]
    latest = store.read_observations(SPEC, "latest")
    assert list(latest["CLSPRC"]) == ["10"] and list(latest["obs_seq"]) == [3]
    assert list(store.read_observations(SPEC, "first")["CLSPRC"]) == ["10"]


def test_no_duplicate_key_and_seq_pairs_across_batches_and_attempts(store: BaselineStore) -> None:
    w = _writer(store, batch_size=2)
    for i in range(7):
        _put(
            w,
            "20261009",
            [_row("20261009", "A", str(i % 3)), _row("20261009", "B", "5")],
            T0 + timedelta(days=i),
        )
    w.flush()
    w2 = _writer(store)
    _put(
        w2,
        "20261009",
        [_row("20261009", "A", "9"), _row("20261009", "B", "5")],
        T0 + timedelta(days=9),
    )
    w2.flush()
    obs = store.read_observations(SPEC, "all")
    assert not obs.duplicated(["BAS_DD", "ISU_CD", "obs_seq"]).any()
    for _, grp in obs.groupby(["BAS_DD", "ISU_CD"]):
        assert sorted(grp["obs_seq"]) == list(range(1, len(grp) + 1))
    assert len(list((store.base / "_manifest").glob("*.json"))) == 5  # 4 묶음 + 1 새 시도


def test_year_is_a_path_partition_only(store: BaselineStore) -> None:
    w = _writer(store)
    _put(w, "20261230", [_row("20261230", "A", "1")], T0)
    _put(w, "20250102", [_row("20250102", "A", "1")], T0)
    w.flush()
    assert (store.base / "t_daily" / "year=2025").is_dir()
    assert (store.base / "t_daily" / "year=2026").is_dir()
    with pytest.raises(ValueError):
        TableSpec("x", ("year",), ("A",), "year")


# ------------------------------------------------------------------ R08


def test_a_vanished_key_becomes_absent_but_stays_in_the_first_view(store: BaselineStore) -> None:
    w = _writer(store)
    day = "20261009"
    _put(w, day, [_row(day, "A", "10"), _row(day, "B", "20")], T0)
    assert _put(w, day, [_row(day, "A", "10")], T0 + timedelta(days=1)) == "new_obs"
    # 이미 absent인 키는 또 넣지 않는다
    assert _put(w, day, [_row(day, "A", "10")], T0 + timedelta(days=2)) == "same"
    w.flush()
    obs = store.read_observations(SPEC, "all")
    b = obs[obs["ISU_CD"] == "B"].sort_values("obs_seq")
    assert list(b["obs_kind"]) == ["value", "absent"]
    assert b.iloc[1][["CLSPRC", "NETASST"]].isna().all()
    assert list(store.read_observations(SPEC, "latest")["ISU_CD"]) == ["A"]
    assert set(store.read_observations(SPEC, "first")["ISU_CD"]) == {"A", "B"}
    # 다시 나타나면 새 관측
    w2 = _writer(store)
    assert (
        _put(w2, day, [_row(day, "A", "10"), _row(day, "B", "20")], T0 + timedelta(days=3))
        == "new_obs"
    )
    w2.flush()
    assert set(store.read_observations(SPEC, "latest")["ISU_CD"]) == {"A", "B"}


def test_incomplete_or_empty_responses_are_not_evidence_of_absence(store: BaselineStore) -> None:
    w = _writer(store)
    day = "20261009"
    _put(w, day, [_row(day, "A", "10"), _row(day, "B", "20")], T0)
    assert _put(w, day, [_row(day, "A", "10")], T0 + timedelta(days=1), complete=False) == "same"
    assert _put(w, day, [], T0 + timedelta(days=2)) == "same"  # 빈 응답(휴장·미발표)
    # 다른 날짜의 키는 범위 밖이라 영향이 없다
    _put(w, "20261012", [_row("20261012", "A", "1")], T0 + timedelta(days=3))
    w.flush()
    assert set(store.read_observations(SPEC, "latest")["ISU_CD"]) == {"A", "B"}
    assert (store.read_observations(SPEC, "all")["obs_kind"] == "value").all()


def test_a_response_without_scope_or_with_duplicate_keys_is_refused(store: BaselineStore) -> None:
    w = _writer(store)
    raw = store.write_raw(SVC, "bas_dd=20261009", b"x", T0)
    row = _row("20261009", "A", "1")
    with pytest.raises(ValueError):
        w.add_response(
            service=SVC,
            request_key="bas_dd=20261009",
            raw=raw,
            parsed=ParsedResponse(SPEC, [row, row], {"BAS_DD": "20261009"}),
        )
    with pytest.raises(ValueError):
        w.add_response(
            service=SVC,
            request_key="bas_dd=20261009",
            raw=raw,
            parsed=ParsedResponse(SPEC, [row], {}),
        )


# ------------------------------------------------------------------ R06


class _Boom(RuntimeError):
    pass


def _crash_at(stage: str):
    def hook(name: str) -> None:
        if name == stage:
            raise _Boom(stage)

    return hook


def _parse_orphan(orphan, body: bytes) -> ParsedResponse:
    day = orphan.request_key.split("=")[1]
    return _parsed(day, [_row(day, "A", "10"), _row(day, "B", "20")])


@pytest.mark.parametrize("stage", ["raw_written", "parquet_written", "manifest_tmp_written"])
def test_a_crash_at_any_stage_loses_and_duplicates_nothing(stage: str, tmp_path: Path) -> None:
    base = tmp_path / "krx_baseline"
    day = "20261009"
    store = BaselineStore(base, hook=_crash_at(stage))
    w = _writer(store)
    rows = [_row(day, "A", "10"), _row(day, "B", "20")]
    with pytest.raises(_Boom):
        raw = store.write_raw(SVC, f"bas_dd={day}", b"body-1", T0)
        w.add_response(service=SVC, request_key=f"bas_dd={day}", raw=raw, parsed=_parsed(day, rows))
        w.flush()

    # 죽은 뒤: manifest 없는 parquet은 읽히지 않는다
    fresh = BaselineStore(base)
    assert fresh.read_observations(SPEC, "all").empty
    assert fresh.read_fetch_log().empty
    assert [o.request_key for o in fresh.find_orphan_raw()] == [f"bas_dd={day}"]
    if stage != "raw_written":
        assert fresh.unreferenced_files()["parquet"]  # 개수만 보고, 지우지 않는다

    # 이어 실행: 새 시도 번호, 원문에서 정규화(다시 받지 않음)
    w2 = _writer(fresh)
    assert w2.attempt == 2 if stage != "raw_written" else w2.attempt == 1
    assert w2.recover_orphans(_parse_orphan) == 1
    w2.flush()
    obs = fresh.read_observations(SPEC, "all")
    assert sorted(obs["ISU_CD"]) == ["A", "B"] and list(obs["obs_seq"]) == [1, 1]
    assert (obs["fetched_at"] == T0).all()  # 파일 이름의 시각
    assert fresh.find_orphan_raw() == []
    assert not obs.duplicated(["BAS_DD", "ISU_CD", "obs_seq"]).any()
    assert len(fresh.read_fetch_log()) == 1

    # 한 번 더 이어도 달라지지 않는다
    w3 = _writer(fresh)
    assert w3.recover_orphans(_parse_orphan) == 0
    w3.flush()
    assert len(fresh.read_observations(SPEC, "all")) == 2


def test_failed_commit_drops_memory_state_and_never_overwrites(store: BaselineStore) -> None:
    store.hook = _crash_at("parquet_written")
    w = _writer(store)
    with pytest.raises(_Boom):
        _put(w, "20261009", [_row("20261009", "A", "1")], T0)
        w.flush()
    store.hook = None
    assert w.pending_requests == 0
    _put(w, "20261009", [_row("20261009", "A", "1")], T0 + timedelta(days=1))  # 같은 writer로 계속
    w.flush()  # 묶음 번호가 올라가 있어 이전 parquet과 안 겹친다
    obs = store.read_observations(SPEC, "all")
    assert len(obs) == 1 and obs.iloc[0]["obs_seq"] == 1


def test_manifest_records_files_hashes_counts_and_slots_but_no_key(store: BaselineStore) -> None:
    w = _writer(store)
    _put(w, "20261009", [_row("20261009", "A", "1")], T0)
    w.store.write_raw(SVC, "bas_dd=20261010", b"x", T0)
    w.add_failure(
        service=SVC,
        request_key="bas_dd=20261010",
        fetched_at=T0,
        error="HTTP 500",
        http_status=500,
        http_requests=3,
        key_slot=2,
    )
    w.flush()
    manifest = store.manifests()[0]
    assert manifest["request_count"] == 2 and manifest["http_requests"] == 4
    assert manifest["key_slots"] == [1, 2] and manifest["source_run"] == "daily"
    kinds = sorted(e["kind"] for e in manifest["files"])
    assert kinds == ["fetch_log", "observation", "raw"]
    assert all(len(e["sha256"]) == 64 and e["bytes"] > 0 for e in manifest["files"])
    log = store.read_fetch_log(SVC)
    assert set(log["result"]) == {"new_obs", "failed"}
    assert store.verify_committed() == []


# ------------------------------------------------------- R02 · fill · 대기


def test_reobserve_covers_the_whole_range_and_resumes_by_run_id(store: BaselineStore) -> None:
    days = [f"202610{d:02d}" for d in (5, 6, 7, 8, 9)]
    keys = [f"bas_dd={d}" for d in days]
    # 조사 사본 import: 이미 관측이 있다
    imp = _writer(store, "import1", "import_research_pull")
    for d in days:
        _put(imp, d, [_row(d, "A", "10")], T0)
    imp.flush()

    # 값이 같아도 reobserve는 전 범위가 대상이다
    assert reobserve_keys(store, SVC, "reobs1", keys) == keys
    w = _writer(store, "reobs1", "backfill_x")
    for d in days[:2]:
        assert _put(w, d, [_row(d, "A", "10")], T0 + timedelta(days=1)) == "same"
    w.flush()  # N개 끝내고 중단
    assert reobserve_keys(store, SVC, "reobs1", keys) == keys[2:]
    assert completed_request_keys(store, SVC, "reobs1") == set(keys[:2])
    # 다른 run_id는 처음부터
    assert reobserve_keys(store, SVC, "reobs2", keys) == keys

    w2 = _writer(store, "reobs1", "backfill_x")
    assert w2.attempt == 2
    for d in days[2:]:
        _put(w2, d, [_row(d, "A", "11" if d == days[3] else "10")], T0 + timedelta(days=1))
    w2.flush()
    assert reobserve_keys(store, SVC, "reobs1", keys) == []
    obs = store.read_observations(SPEC, "all")
    assert len(obs) == 6  # 값이 달라진 하루만 obs_seq=2
    assert obs["obs_seq"].max() == 2
    assert len(store.read_fetch_log(SVC)) == 10  # 같은 날도 요청 기록은 남는다


def test_pending_rule_counts_weekdays_without_a_calendar() -> None:
    fri = date(2026, 10, 9)
    assert weekdays_between(fri, date(2026, 10, 12)) == 1  # 주말 건너뜀
    assert weekdays_between(fri, fri) == 0
    assert weekdays_between("20261009", "20261023") == 10
    assert is_pending("no_price", 0, 0, fri, date(2026, 10, 23))  # 10 -> 대기
    assert not is_pending("no_price", 0, 0, fri, date(2026, 10, 26))  # 11 -> 확정
    assert is_pending("trading", 3, 0, "20261009", date(2026, 10, 12))  # 순자산 0
    assert not is_pending("trading", 0, 5, fri, date(2026, 10, 12))
    assert is_pending("partial", None, None, fri, date(2026, 10, 12))
    assert not is_pending("empty", 0, 0, fri, date(2026, 12, 1))  # 백필은 확정


def test_fill_dates_returns_missing_and_pending_weekdays_only(store: BaselineStore) -> None:
    w = _writer(store)
    mon, tue, wed = "20261005", "20261006", "20261007"
    for day, kind, at in [
        (mon, "trading", datetime(2026, 10, 6, tzinfo=UTC)),  # 확정
        (tue, "no_price", datetime(2026, 10, 7, tzinfo=UTC)),  # 창 안 -> 대기
        (wed, "no_price", datetime(2026, 10, 30, tzinfo=UTC)),  # 창을 넘겨 받음 -> 확정
    ]:
        _put(w, day, [_row(day, "A", None)], at, day_kind=kind, netasst_zero_rows=0)
    w.add_failure(service=SVC, request_key="bas_dd=20261008", fetched_at=T0, error="x")
    w.flush()
    got = fill_dates(store, SVC, date(2026, 10, 5), date(2026, 10, 12))
    assert got == [date(2026, 10, 6), date(2026, 10, 8), date(2026, 10, 9), date(2026, 10, 12)]
