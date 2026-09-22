"""SEC 클라이언트 — 미국 계획 01 §1.1, 05 §6, 06 §1. 네트워크 없이 돈다."""

from __future__ import annotations

import zipfile

import pytest

from collector.us.sources import sec


class _Resp:
    def __init__(self, status=200, body=b"", headers=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}

    def iter_content(self, chunk_size=1):
        yield self._body


class _Session:
    def __init__(self, resp):
        self.resp = resp
        self.calls = []

    def get(self, url, **kw):
        self.calls.append(url)
        return self.resp


# --- UA --------------------------------------------------------------------


def test_user_agent_is_required():
    with pytest.raises(sec.SecAccessError, match="SEC_USER_AGENT"):
        sec.user_agent_from_env({})


def test_user_agent_comes_from_env():
    assert sec.user_agent_from_env({"SEC_USER_AGENT": "x/1 (a@b.c)"}) == "x/1 (a@b.c)"


# --- 분기·URL ---------------------------------------------------------------


def test_quarters_spans_the_test_window():
    qs = sec.quarters((2018, 3), (2026, 2))
    assert len(qs) == 32  # 계획이 말하는 "32분기"
    assert qs[0] == (2018, 3) and qs[-1] == (2026, 2)
    assert (2018, 4) in qs and (2019, 1) in qs


def test_url_shapes():
    assert sec.financial_statements_url(2018, 4).endswith("/2018q4.zip")
    assert sec.insider_url(2018, 4).endswith("/2018q4_form345.zip")
    # MIDAS만 밑줄 형식이다 — individual_security_2018_q4.zip
    assert sec.midas_url(2018, 4).endswith("/individual_security_2018_q4.zip")


def test_midas_2019q4_uses_the_exception_path():
    """SEC 목록이 이 한 분기만 Drupal 내부 경로로 건다. 규칙대로 만들면 404다."""
    assert (2019, 4) in sec.MIDAS_URL_EXCEPTIONS
    assert "/files/node/add/data_distribution/" in sec.midas_url(2019, 4)
    # 이웃 분기는 규칙 그대로여야 한다
    for q in (3,):
        assert "metrics-individual-security" in sec.midas_url(2019, q)
    assert "metrics-individual-security" in sec.midas_url(2020, 1)


# --- 403은 재시도하지 않는다 -------------------------------------------------


def test_403_raises_and_says_to_look_at_the_ua():
    client = sec.SecClient(user_agent="x/1 (a@b.c)", session=_Session(_Resp(403)))
    with pytest.raises(sec.SecAccessError, match="UA에 연락처"):
        client.get("https://www.sec.gov/anything")


def test_non_200_raises():
    client = sec.SecClient(user_agent="x/1 (a@b.c)", session=_Session(_Resp(404)))
    with pytest.raises(sec.SecAccessError, match="404"):
        client.get("https://www.sec.gov/anything")


def test_interval_lives_inside_the_client():
    """부르는 쪽이 잊어도 지켜져야 한다 (02 §2.1)."""
    client = sec.SecClient(user_agent="x/1 (a@b.c)", session=_Session(_Resp(200)))
    assert client.interval_seconds == sec.DEFAULT_INTERVAL_SECONDS == 5.0


# --- 200이 성공이 아니다 -----------------------------------------------------


def test_html_saved_as_zip_is_caught(tmp_path):
    """403 HTML을 .zip으로 저장한 것 — 크기만 보면 못 잡는다 (06 §1)."""
    bad = tmp_path / "2018q4.zip"
    bad.write_bytes(b"<html><body>SEC.gov | Request Rate Threshold Exceeded</body></html>")
    with pytest.raises(sec.SecAccessError, match="ZIP 매직이 아니다"):
        sec.assert_is_zip(bad)


def test_empty_zip_is_caught(tmp_path):
    p = tmp_path / "empty.zip"
    with zipfile.ZipFile(p, "w"):
        pass
    with pytest.raises(sec.SecAccessError, match="entry가 없다"):
        sec.assert_is_zip(p)


def test_truncated_zip_is_caught(tmp_path):
    p = tmp_path / "cut.zip"
    p.write_bytes(b"PK\x03\x04" + b"\x00" * 20)
    with pytest.raises(sec.SecAccessError, match="열리지 않는다"):
        sec.assert_is_zip(p)


def test_real_zip_returns_entry_names(tmp_path):
    p = tmp_path / "ok.zip"
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr("sub.txt", "adsh\tcik\tsic\n")
    assert sec.assert_is_zip(p) == ["sub.txt"]


def test_download_leaves_no_partial_file_behind(tmp_path):
    dest = tmp_path / "x" / "f.zip"
    client = sec.SecClient(user_agent="x/1 (a@b.c)", session=_Session(_Resp(200, b"PK\x03\x04")))
    client.download("https://www.sec.gov/f.zip", dest)
    assert dest.read_bytes() == b"PK\x03\x04"
    assert not list(dest.parent.glob("*.part"))


# --- 분기 ZIP 내려받기 -------------------------------------------------------


def test_quarterly_path_layout(tmp_path):
    from collector.lake import DataRoot

    p = sec.quarterly_path(DataRoot(tmp_path), "financial", 2018, 4)
    assert p.parts[-4:] == ("sec", "quarterly", "financial", "2018q4.zip")


def test_quarterly_path_rejects_unknown_kind(tmp_path):
    from collector.lake import DataRoot

    with pytest.raises(ValueError, match="모르는 갈래"):
        sec.quarterly_path(DataRoot(tmp_path), "nope", 2018, 4)


# --- 분기 추출의 끝은 raw/ 가 정한다 (2026-09-23) ----------------------------
#
# 예전엔 세 추출 함수의 끝이 ``quarters((2018, 3), (2026, 2))``로 박혀 있어
# 2026q3 zip을 받아도 레이크에 안 들어갔다.


def test_latest_available_quarter_finds_the_max_present(tmp_path):
    from collector.lake import DataRoot

    root = DataRoot(tmp_path)
    for y, q in [(2018, 3), (2019, 1), (2026, 3)]:
        p = sec.quarterly_path(root, "midas", y, q)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"PK\x03\x04")
    assert sec.latest_available_quarter(root, "midas") == (2026, 3)


def test_latest_available_quarter_ignores_names_that_dont_match(tmp_path):
    """분기 zip이 아닌 파일(``.part`` 등)은 안 센다."""
    from collector.lake import DataRoot

    root = DataRoot(tmp_path)
    p = sec.quarterly_path(root, "midas", 2018, 3)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"PK\x03\x04")
    (p.parent / "2099q9.zip").write_bytes(b"PK\x03\x04")  # 분기가 아니다(q9)
    (p.parent / "2026q3.zip.part").write_bytes(b"x")  # 아직 다 안 받았다
    assert sec.latest_available_quarter(root, "midas") == (2018, 3)


def test_latest_available_quarter_is_none_when_raw_is_empty(tmp_path):
    from collector.lake import DataRoot

    assert sec.latest_available_quarter(DataRoot(tmp_path), "midas") is None


def test_latest_available_quarter_rejects_unknown_kind(tmp_path):
    from collector.lake import DataRoot

    with pytest.raises(ValueError, match="모르는 갈래"):
        sec.latest_available_quarter(DataRoot(tmp_path), "nope")


def _zip_bytes():
    import io as _io

    buf = _io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("sub.txt", "adsh\tcik\tsic\n")
    return buf.getvalue()


def test_download_quarterly_skips_a_good_file(tmp_path):
    from collector.lake import DataRoot

    root = DataRoot(tmp_path)
    dest = sec.quarterly_path(root, "midas", 2020, 1)
    dest.parent.mkdir(parents=True)
    dest.write_bytes(_zip_bytes())
    session = _Session(_Resp(200, b"should-not-be-used"))
    r = sec.download_quarterly(
        sec.SecClient("x/1 (a@b.c)", session=session), root, "midas", 2020, 1
    )
    assert r["skipped"] is True
    assert session.calls == []  # 요청을 아예 안 보낸다


def test_download_quarterly_refetches_a_broken_file(tmp_path):
    """403 HTML이 .zip으로 남아 있으면 지우고 다시 받는다 — 이어받기의 근거다."""
    from collector.lake import DataRoot

    root = DataRoot(tmp_path)
    dest = sec.quarterly_path(root, "midas", 2020, 1)
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"<html>Request Rate Threshold Exceeded</html>")
    session = _Session(_Resp(200, _zip_bytes()))
    r = sec.download_quarterly(
        sec.SecClient("x/1 (a@b.c)", session=session), root, "midas", 2020, 1
    )
    assert r["skipped"] is False
    assert len(session.calls) == 1
    assert sec.assert_is_zip(dest) == ["sub.txt"]


# --- 추출 규칙 ---------------------------------------------------------------


def test_midas_member_must_be_exactly_one_csv(tmp_path):
    """파일명 규칙(q4_2018_all.csv)을 믿지 않고 목록에서 고른다."""
    p = tmp_path / "m.zip"
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr("README.txt", "x")
        zf.writestr("q4_2018_all.csv", "Date,Ticker\n")
    assert sec._midas_member(p, 2018, 4) == "q4_2018_all.csv"


def test_midas_member_rejects_ambiguous_zip(tmp_path):
    p = tmp_path / "m.zip"
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr("a.csv", "x")
        zf.writestr("b.csv", "y")
    with pytest.raises(sec.SecAccessError, match="CSV가 하나가 아니다"):
        sec._midas_member(p, 2018, 4)


def test_midas_dedup_rule_keeps_the_fuller_row():
    """원천에 (Date,Ticker) 중복이 있다 — 한쪽은 순위가 비어 온다 (03 §4.8)."""
    import duckdb

    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t (date DATE, ticker VARCHAR, mcap_rank INT, turn_rank INT, "
        "volatility_rank INT, price_rank INT)"
    )
    con.execute("INSERT INTO t VALUES (DATE '2018-07-16','SPB',8,10,9,9)")
    con.execute("INSERT INTO t VALUES (DATE '2018-07-16','SPB',NULL,NULL,9,NULL)")
    con.execute("INSERT INTO t VALUES (DATE '2025-11-18','OPEN',9,10,10,4)")
    con.execute("INSERT INTO t VALUES (DATE '2025-11-18','OPEN',9,10,10,4)")
    kept = con.execute("""
        SELECT date, ticker, mcap_rank FROM (
            SELECT *,
                   (mcap_rank IS NOT NULL)::INT + (turn_rank IS NOT NULL)::INT
                 + (volatility_rank IS NOT NULL)::INT
                 + (price_rank IS NOT NULL)::INT AS rank_filled
            FROM t
        )
        QUALIFY row_number() OVER (PARTITION BY date, ticker ORDER BY rank_filled DESC,
                mcap_rank NULLS LAST, turn_rank NULLS LAST,
                volatility_rank NULLS LAST, price_rank NULLS LAST) = 1
        ORDER BY date
        """).fetchall()
    assert kept == [
        (__import__("datetime").date(2018, 7, 16), "SPB", 8),
        (__import__("datetime").date(2025, 11, 18), "OPEN", 9),
    ]


# --- 안 쓰던 12개 칸 (07_midas_unused_columns.md §8) --------------------------

_MIDAS_HEADER = (
    "Date,Security,Ticker,McapRank,TurnRank,VolatilityRank,PriceRank,"
    "LitVol('000),OrderVol('000),Hidden,TradesForHidden,HiddenVol('000),"
    "TradeVolForHidden('000),Cancels,LitTrades,OddLots,TradesForOddLots,"
    "OddLotVol('000),TradeVolForOddLots('000)"
)


def _midas_zip(path, *, tag="2018q3", rows):
    """MIDAS 분기 zip 하나를 흉내 낸다. 19컬럼 헤더 그대로(실제 zip 확인 완료)."""
    year, q = int(tag[:4]), int(tag[5])
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(f"q{q}_{year}_all.csv", "\n".join([_MIDAS_HEADER, *rows]) + "\n")


def test_midas_schema_has_21_columns_and_old_four_ranks_keep_their_type():
    """스키마 계약 — 12개를 더해도 기존 순위 넷의 이름·자료형은 안 바뀐다."""
    import pyarrow as pyar

    from collector.us.store.schema import MIDAS_SECURITY_DAILY_ARROW as schema

    assert schema.names == [
        "date",
        "ticker",
        "security_type",
        "mcap_rank",
        "turn_rank",
        "volatility_rank",
        "price_rank",
        "lit_vol_k",
        "order_vol_k",
        "hidden",
        "trades_for_hidden",
        "hidden_vol_k",
        "trade_vol_for_hidden_k",
        "cancels",
        "lit_trades",
        "odd_lots",
        "trades_for_odd_lots",
        "odd_lot_vol_k",
        "trade_vol_for_odd_lots_k",
        "observed_at",
        "source_rev",
    ]
    for name in ("mcap_rank", "turn_rank", "volatility_rank", "price_rank"):
        assert schema.field(name).type == pyar.int32()
    # ('000) 칸 — 천 단위 원값 그대로 DOUBLE. ×1000 해서 int로 바꾸지 않는다
    for name in (
        "lit_vol_k",
        "order_vol_k",
        "hidden_vol_k",
        "trade_vol_for_hidden_k",
        "odd_lot_vol_k",
        "trade_vol_for_odd_lots_k",
    ):
        assert schema.field(name).type == pyar.float64()
    # 이미 건수(count)인 여섯 칸 — 단위 접미사가 없다
    for name in (
        "hidden",
        "trades_for_hidden",
        "cancels",
        "lit_trades",
        "odd_lots",
        "trades_for_odd_lots",
    ):
        assert schema.field(name).type == pyar.int64()


def test_extract_midas_reads_the_twelve_extra_columns(tmp_path):
    """07_midas_unused_columns.md §8 — 순위 넷 말고 12개도 뽑는다. 단위·음수 보존."""
    import datetime as dt

    import duckdb

    from collector.lake import DataRoot

    root = DataRoot(tmp_path)
    dest = sec.quarterly_path(root, "midas", 2018, 3)
    dest.parent.mkdir(parents=True)
    rows = [
        # 2018q3.zip 실물 첫 데이터 행 그대로 (2026-09-23 스모크 확인)
        "20180702,Stock,A,10.0,5.0,1.0,9.0,822.3989999999999,17418.978000000003,"
        "923.0,10443.0,95.113,917.512,129703.0,9485.0,3614.0,10398.0,146.449,"
        "913.5569999999999",
        # Hidden·HiddenVol 음수 — SEC 정의(SIP 체결 - 직접피드 체결)가 드물게
        # 내는 값이다(§8.1). 버리거나 0으로 바뀌면 안 된다
        "20180809,Stock,XPL,3.0,4.0,2.0,5.0,10.5,20.25,-1.0,50.0,-0.2,10.123,"
        "100,200.0,5.0,6,1.111,2.222",
    ]
    _midas_zip(dest, tag="2018q3", rows=rows)

    r = sec.extract_midas(root, snapshot_date=dt.date(2026, 1, 1))
    assert r["quarters"] == 1
    assert r["rows"] == 2

    con = duckdb.connect()
    cols = (
        "ticker, mcap_rank, turn_rank, volatility_rank, price_rank, "
        "lit_vol_k, order_vol_k, hidden, trades_for_hidden, hidden_vol_k, "
        "trade_vol_for_hidden_k, cancels, lit_trades, odd_lots, "
        "trades_for_odd_lots, odd_lot_vol_k, trade_vol_for_odd_lots_k"
    )
    rows_out = {
        row[0]: row for row in con.execute(f"SELECT {cols} FROM '{r['path']}'").fetchall()
    }

    a = rows_out["A"]
    # 기존 순위 넷 — 값이 그대로다 (회귀 확인)
    assert a[1:5] == (10, 5, 1, 9)
    # ('000) 칸은 원값(천 단위) 그대로다. ×1000 해서 주식 수로 바꾸지 않는다
    assert a[5] == pytest.approx(822.3989999999999)
    assert a[6] == pytest.approx(17418.978000000003)
    # 이미 건수인 칸 — 소수점이 있는 원문도 정수로 온다
    assert a[11] == 129703  # cancels
    assert a[12] == 9485  # lit_trades

    xpl = rows_out["XPL"]
    assert xpl[7] == -1  # hidden — 음수를 그대로 저장한다
    assert xpl[9] == pytest.approx(-0.2)  # hidden_vol_k — 마찬가지


def test_extract_midas_picks_up_a_quarter_past_the_old_frozen_end(tmp_path):
    """raw/ 에 2026q3 zip이 있으면 대상에 들어간다 — 끝이 raw/ 최신 분기다.

    예전엔 ``quarters((2018, 3), (2026, 2))``로 끝이 박혀 있어 2026q3 zip을
    받아도 레이크에 안 들어갔다 (2026-09-23). 사이 분기(2018q4\\~2026q2)는
    안 받아 뒀으니 ``missing`` 으로 그대로 잡혀야 한다 — 지금 동작을 유지한다.
    """
    import datetime as dt

    from collector.lake import DataRoot

    root = DataRoot(tmp_path)
    row_2018 = (
        "20180702,Stock,A,10.0,5.0,1.0,9.0,822.3989999999999,17418.978000000003,"
        "923.0,10443.0,95.113,917.512,129703.0,9485.0,3614.0,10398.0,146.449,"
        "913.5569999999999"
    )
    row_2026 = (
        "20260715,Stock,B,11.0,6.0,2.0,8.0,1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0," "10.0,11.0,12.0"
    )
    for y, q, tag, row in ((2018, 3, "2018q3", row_2018), (2026, 3, "2026q3", row_2026)):
        p = sec.quarterly_path(root, "midas", y, q)
        p.parent.mkdir(parents=True, exist_ok=True)
        _midas_zip(p, tag=tag, rows=[row])

    r = sec.extract_midas(root, snapshot_date=dt.date(2026, 1, 1))

    assert r["quarters"] == 2  # 2018q3 + 2026q3 만 실제로 받아 뒀다
    assert r["rows"] == 2
    assert "2026q3" not in r["missing"]  # 끝이 여기까지 늘어났다
    assert "2026q2" in r["missing"]  # 사이 분기는 여전히 missing 이다
    assert "2019q1" in r["missing"]


def test_extract_midas_dedup_keeps_the_fuller_rank_row_for_new_columns_too(tmp_path):
    """12칸을 더해도 dedup 규칙은 그대로다 — 순위 넷이 다 찬 행이 이긴다 (03 §4.8)."""
    import datetime as dt

    import duckdb

    from collector.lake import DataRoot

    root = DataRoot(tmp_path)
    dest = sec.quarterly_path(root, "midas", 2018, 3)
    dest.parent.mkdir(parents=True)
    rows = [
        # 순위 넷이 다 찼다 — 이 행이 이긴다
        "20180716,Stock,SPB,8.0,10.0,9.0,9.0,1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,"
        "9.0,10.0,11.0,12.0",
        # VolatilityRank 만 있다 — 실측 패턴 그대로, 진다
        "20180716,Stock,SPB,,,9.0,,100.0,200.0,300.0,400.0,500.0,600.0,700.0,"
        "800.0,900.0,1000.0,1100.0,1200.0",
    ]
    _midas_zip(dest, tag="2018q3", rows=rows)

    r = sec.extract_midas(root, snapshot_date=dt.date(2026, 1, 1))
    assert r["rows"] == 1  # 하나는 접혔다
    assert r["deduped_rows"] == 1

    con = duckdb.connect()
    row = con.execute(
        f"SELECT mcap_rank, lit_vol_k, trade_vol_for_odd_lots_k FROM '{r['path']}'"
    ).fetchone()
    # 이긴 행(순위 넷이 다 찬 행)의 12칸 값이지, 진 행의 값이 아니다
    assert row == (8, 1.0, 12.0)


# --- 내부자 거래 (03 §4.10) ---------------------------------------------------


def test_insider_url_uses_the_exception_table():
    """최신 분기 하나가 다른 디렉터리로 갔다 — 규칙만 믿으면 404다 (04 §2.12)."""
    assert sec.insider_url(2025, 4).endswith("/structureddata/data/"
                                             "insider-transactions-data-sets/2025q4_form345.zip")
    assert "datastandardsinnovation" in sec.insider_url(2026, 2)


def _insider_zip(path, *, tag="2018q3"):
    """분기 내부자 ZIP 하나를 흉내 낸다. 쓰는 TSV 넷만 넣는다."""
    rows = {
        "SUBMISSION.tsv": [
            "ACCESSION_NUMBER\tFILING_DATE\tPERIOD_OF_REPORT\tDOCUMENT_TYPE"
            "\tISSUERCIK\tISSUERTRADINGSYMBOL",
            "0000000000-18-000001\t31-OCT-2018\t30-OCT-2018\t4\t0000320193\tAAPL",
            # 심볼이 '-'로 오는 공시가 실제로 있다. CIK가 키다
            "0000000000-18-000002\t02-NOV-2018\t01-NOV-2018\t5\t0000773840\t-",
        ],
        "NONDERIV_TRANS.tsv": [
            "ACCESSION_NUMBER\tNONDERIV_TRANS_SK\tSECURITY_TITLE\tTRANS_DATE"
            "\tTRANS_FORM_TYPE\tTRANS_CODE\tEQUITY_SWAP_INVOLVED\tTRANS_SHARES"
            "\tTRANS_PRICEPERSHARE\tTRANS_ACQUIRED_DISP_CD\tSHRS_OWND_FOLWNG_TRANS"
            "\tDIRECT_INDIRECT_OWNERSHIP",
            "0000000000-18-000001\t11\tCommon Stock\t30-OCT-2018\t4\tS\t0"
            "\t100.0\t216.3\tD\t900.0\tD",
            "0000000000-18-000002\t12\tCommon Stock\t01-NOV-2018\t5\tP\t"
            "\t50.0\t10.0\tA\t50.0\tI",
        ],
        "DERIV_TRANS.tsv": [
            "ACCESSION_NUMBER\tDERIV_TRANS_SK\tSECURITY_TITLE\tCONV_EXERCISE_PRICE"
            "\tTRANS_DATE\tTRANS_FORM_TYPE\tTRANS_CODE\tEQUITY_SWAP_INVOLVED"
            "\tTRANS_SHARES\tTRANS_PRICEPERSHARE\tTRANS_ACQUIRED_DISP_CD"
            "\tEXPIRATION_DATE\tUNDLYNG_SEC_SHARES\tSHRS_OWND_FOLWNG_TRANS"
            "\tDIRECT_INDIRECT_OWNERSHIP",
            "0000000000-18-000001\t11\tRSU\t0.0\t30-OCT-2018\t4\tA\t0"
            "\t7.0\t0.0\tA\t31-DEC-2025\t7.0\t7.0\tD",
        ],
        "REPORTINGOWNER.tsv": [
            "ACCESSION_NUMBER\tRPTOWNERCIK\tRPTOWNERNAME\tRPTOWNER_RELATIONSHIP"
            "\tRPTOWNER_TITLE",
            "0000000000-18-000001\t0001214156\tCOOK TIMOTHY\tDirector,Officer\tCEO",
            # 쉼표 없이 붙어 오는 표기가 섞여 있다
            "0000000000-18-000002\t0000000123\tSOMEONE\tTenPercentOwnerOther\t",
            # 같은 공시에 같은 신고인이 두 번 (원천 중복)
            "0000000000-18-000002\t0000000123\tSOMEONE\tTenPercentOwnerOther\t",
        ],
    }
    with zipfile.ZipFile(path, "w") as zf:
        for name, lines in rows.items():
            zf.writestr(name, "\n".join(lines) + "\n")
        zf.writestr("FOOTNOTES.tsv", "ACCESSION_NUMBER\n")  # 안 뽑는 것도 있어야 현실이다


def test_extract_insider_end_to_end(tmp_path):
    """ZIP 하나에서 두 장이 나온다 — 방향·수량과 신고인 관계."""
    import datetime as dt

    import duckdb

    from collector.lake import DataRoot

    root = DataRoot(tmp_path)
    dest = sec.quarterly_path(root, "insider", 2018, 3)
    dest.parent.mkdir(parents=True)
    _insider_zip(dest)

    r = sec.extract_insider(root, snapshot_date=dt.date(2026, 1, 1))
    assert r["quarters"] == 1
    assert r["trans_without_submission"] == 0
    assert r["insider_trans"]["rows"] == 3  # 비파생 2 + 파생 1
    assert r["insider_owners"]["rows"] == 2  # 중복 하나를 접었다
    assert r["insider_owners"]["deduped_rows"] == 1

    con = duckdb.connect()
    t = str(r["insider_trans"]["path"])
    rows = con.execute(
        f"SELECT trans_code, acquired_disposed, trans_shares, issuer_cik, issuer_symbol,"
        f" filing_date, trans_date, is_derivative, expiration_date"
        f" FROM '{t}' ORDER BY is_derivative, trans_sk"
    ).fetchall()
    assert rows[0][:5] == ("S", "D", 100.0, 320193, "AAPL")
    assert rows[0][5] == dt.date(2018, 10, 31)  # 31-OCT-2018 을 날짜로 읽는다
    assert rows[0][7] is False and rows[0][8] is None  # 비파생은 만기가 없다
    assert rows[1][:5] == ("P", "A", 50.0, 773840, "-")
    assert rows[2][7] is True and rows[2][8] == dt.date(2025, 12, 31)

    o = str(r["insider_owners"]["path"])
    owners = con.execute(
        f"SELECT owner_cik, is_director, is_officer, is_ten_percent_owner, is_other"
        f" FROM '{o}' ORDER BY owner_cik"
    ).fetchall()
    assert owners[0] == (123, False, False, True, True)  # TenPercentOwnerOther
    assert owners[1] == (1214156, True, True, False, False)  # Director,Officer


def test_extract_insider_needs_a_zip(tmp_path):
    from collector.lake import DataRoot

    with pytest.raises(sec.SecAccessError, match="내부자 ZIP이 하나도 없다"):
        sec.extract_insider(DataRoot(tmp_path), snapshot_date="2026-01-01")
