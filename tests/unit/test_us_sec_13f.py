"""SEC Form 13F — 목록 페이지 파싱·원문 파서·정정 대체 (02_sec_13f.md).

네트워크 없이 돈다. 합성 fixture로 헤더·정정 대체·OTHERMANAGER 중복·13F-NT 제외·
한 파일 안 다중 기준일·PIT 컷오프를 본다 — 실제 13F 파일이 없어 조사 문서(02 §2·§4)의
표본 줄로 fixture를 만든다.
"""

from __future__ import annotations

import datetime as dt
import zipfile

import pytest

from collector.lake import DataRoot
from collector.us.sources import sec_13f

# --- 목록 페이지 파싱 --------------------------------------------------------

#: 실제 목록 페이지 경로(연구 §3)를 대표하되, 100자 줄 길이 제한에 걸려
#: 짧은 대역 경로(``/files/x/``)로 줄였다 — 파서는 경로 자체를 안 보므로
#: 대표성에는 영향이 없다(``sec_ftd``의 ``freq-req`` 대역명과 같은 이유).
_LISTING_HTML = """
<html><body>
<table>
<tr><td><a href="/files/x/2018q4_form13f.zip">a</a></td></tr>
<tr><td><a href='/files/x/01jun2026-31aug2026_form13f.zip'>a</a></td></tr>
<tr><td><a href="/files/x/01jan2024-29feb2024_form13f.zip">a</a></td></tr>
<tr><td><a href="/files/x/readme.htm">a</a></td></tr>
</table>
</body></html>
"""


def test_parse_listing_reads_quarterly_and_range_tags():
    listing = sec_13f.parse_listing(_LISTING_HTML)
    assert listing.files == {
        "2018q4": "https://www.sec.gov/files/x/2018q4_form13f.zip",
        "01jun2026-31aug2026": "https://www.sec.gov/files/x/01jun2026-31aug2026_form13f.zip",
        "01jan2024-29feb2024": "https://www.sec.gov/files/x/01jan2024-29feb2024_form13f.zip",
    }
    assert not listing.duplicates
    assert not listing.unparsed  # readme.htm 은 _form13f.zip 이 아니라 href 후보에도 안 든다


def test_parse_listing_rejects_a_tag_that_matches_neither_rule():
    html = '<a href="/files/data-research/x/weird_form13f.zip">a</a>'
    listing = sec_13f.parse_listing(html)
    assert listing.files == {}
    assert listing.unparsed == ["/files/data-research/x/weird_form13f.zip"]


def test_parse_listing_keeps_the_first_url_on_duplicate_period():
    html = (
        '<a href="/files/a/2018q4_form13f.zip">x</a>'
        '<a href="/files/b/2018q4_form13f.zip">y</a>'
    )
    listing = sec_13f.parse_listing(html)
    assert listing.files["2018q4"].endswith("/a/2018q4_form13f.zip")
    assert "2018q4" in listing.duplicates


def test_parse_listing_raises_when_nothing_matches():
    with pytest.raises(sec_13f.ThirteenFError, match="하나도 못 찾았다"):
        sec_13f.parse_listing("<html>no links here</html>")


def test_is_valid_period_tag():
    assert sec_13f.is_valid_period_tag("2018q4")
    assert sec_13f.is_valid_period_tag("01jun2026-31aug2026")
    assert sec_13f.is_valid_period_tag("01jan2024-29feb2024")
    assert not sec_13f.is_valid_period_tag("not-a-period")
    assert not sec_13f.is_valid_period_tag("2018q5")


# --- SecClient 로 목록을 받는다 ----------------------------------------------


class _Resp:
    def __init__(self, status=200, text=""):
        self.status_code = status
        self.text = text
        self.headers = {}

    def iter_content(self, chunk_size=1):
        yield self.text.encode()


class _Session:
    def __init__(self, resp):
        self.resp = resp
        self.calls = []

    def get(self, url, **kw):
        self.calls.append(url)
        return self.resp


def test_list_13f_files_uses_the_sec_client():
    from collector.us.sources import sec

    session = _Session(_Resp(200, _LISTING_HTML))
    client = sec.SecClient(user_agent="x/1 (a@b.c)", session=session)
    listing = sec_13f.list_13f_files(client)
    assert len(session.calls) == 1
    assert session.calls[0] == sec_13f.LIST_URL
    assert "2018q4" in listing.files


# --- raw 내려받기 --------------------------------------------------------------


def _zip_bytes(files: dict[str, str]) -> bytes:
    import io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, text in files.items():
            zf.writestr(name, text)
    return buf.getvalue()


_SUB_HEADER = "ACCESSION_NUMBER\tFILING_DATE\tSUBMISSIONTYPE\tCIK\tPERIODOFREPORT"
_INFO_HEADER = (
    "ACCESSION_NUMBER\tINFOTABLE_SK\tNAMEOFISSUER\tTITLEOFCLASS\tCUSIP\tFIGI\t"
    "VALUE\tSSHPRNAMT\tSSHPRNAMTTYPE\tPUTCALL\tINVESTMENTDISCRETION\tOTHERMANAGER\t"
    "VOTING_AUTH_SOLE\tVOTING_AUTH_SHARED\tVOTING_AUTH_NONE"
)


def _info_row(
    accession: str,
    cusip: str,
    sshprnamt: int,
    *,
    sk: int = 1,
    sshtype: str = "SH",
    putcall: str = "",
    othermanager: str = "",
) -> str:
    """INFOTABLE.tsv 한 줄. 계산에 안 쓰는 칸(NAMEOFISSUER 등)은 자리만 채운다."""
    return "\t".join(
        [
            accession,
            str(sk),
            "ISSUER",
            "COM",
            cusip,
            "",
            "100",
            str(sshprnamt),
            sshtype,
            putcall,
            "SOLE",
            othermanager,
            str(sshprnamt),
            "0",
            "0",
        ]
    )


def test_download_13f_skips_a_good_file(tmp_path):
    from collector.us.sources import sec

    root = DataRoot(tmp_path)
    dest = sec_13f.thirteenf_raw_path(root, "2018q4")
    dest.parent.mkdir(parents=True)
    dest.write_bytes(_zip_bytes({"SUBMISSION.tsv": _SUB_HEADER, "INFOTABLE.tsv": _INFO_HEADER}))
    session = _Session(_Resp(200, "should-not-be-used"))
    r = sec_13f.download_13f(
        sec.SecClient("x/1 (a@b.c)", session=session), root, "2018q4", "https://x/y.zip"
    )
    assert r["skipped"] is True
    assert session.calls == []


def test_download_13f_refetches_a_broken_file(tmp_path):
    from collector.us.sources import sec

    root = DataRoot(tmp_path)
    dest = sec_13f.thirteenf_raw_path(root, "2018q4")
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"<html>Request Rate Threshold Exceeded</html>")

    class _BinResp(_Resp):
        def iter_content(self, chunk_size=1):
            yield self._raw

    resp = _BinResp(200)
    resp._raw = _zip_bytes({"SUBMISSION.tsv": _SUB_HEADER, "INFOTABLE.tsv": _INFO_HEADER})
    session = _Session(resp)
    r = sec_13f.download_13f(
        sec.SecClient("x/1 (a@b.c)", session=session), root, "2018q4", "https://x/y.zip"
    )
    assert r["skipped"] is False
    assert len(session.calls) == 1


def test_raw_periods_reads_what_is_on_disk(tmp_path):
    root = DataRoot(tmp_path)
    assert sec_13f.raw_periods(root) == set()
    for period in ("2018q4", "01jun2026-31aug2026"):
        p = sec_13f.thirteenf_raw_path(root, period)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"PK\x03\x04")
    assert sec_13f.raw_periods(root) == {"2018q4", "01jun2026-31aug2026"}


# --- zip 멤버 -----------------------------------------------------------------


def test_extract_decoded_passes_through_valid_utf8(tmp_path):
    zpath = tmp_path / "x.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("SUBMISSION.tsv", "plain ascii text")
    dest = sec_13f._extract_decoded(zpath, "SUBMISSION.tsv", tmp_path / "out.txt")
    assert dest.read_text() == "plain ascii text"


def test_extract_decoded_falls_back_to_latin1(tmp_path):
    """``NAMEOFISSUER``에 latin-1 바이트가 섞여 오면 UTF-8 디코딩이 실패한다 —
    FTD의 ``DESCRIPTION``과 같은 걱정이다(연구 §2)."""
    zpath = tmp_path / "x.zip"
    raw = "CAF\xc9 HOLDINGS INC".encode("latin-1")
    with pytest.raises(UnicodeDecodeError):
        raw.decode("utf-8")
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("INFOTABLE.tsv", raw)
    dest = sec_13f._extract_decoded(zpath, "INFOTABLE.tsv", tmp_path / "out.txt")
    assert "CAFÉ HOLDINGS INC" in dest.read_text(encoding="utf-8")


def test_find_member_ignores_path_prefix():
    assert sec_13f._find_member(["x/SUBMISSION.tsv", "x/INFOTABLE.tsv"], "SUBMISSION.tsv") == (
        "x/SUBMISSION.tsv"
    )
    with pytest.raises(sec_13f.ThirteenFError, match="없다"):
        sec_13f._find_member(["a.txt"], "SUBMISSION.tsv")


# --- extract_13f 종단 ----------------------------------------------------------


def _lake(tmp_path) -> DataRoot:
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()
    return DataRoot(tmp_path)


def _write_13f_zip(root: DataRoot, period: str, *, submission: str, infotable: str) -> None:
    p = sec_13f.thirteenf_raw_path(root, period)
    p.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr("SUBMISSION.tsv", submission)
        zf.writestr("INFOTABLE.tsv", infotable)


def test_extract_13f_needs_a_raw_zip(tmp_path):
    with pytest.raises(sec_13f.ThirteenFError, match="받아 둔 zip이 없다"):
        sec_13f.extract_13f(_lake(tmp_path), snapshot_date="2026-09-28")


def test_extract_13f_excludes_13f_nt_and_option_rows(tmp_path):
    """13F-NT는 보유가 없어 filer 수·holders에서 빠진다. PUTCALL이 찬 옵션 행도 뺀다."""
    root = _lake(tmp_path)
    submission = "\n".join(
        [
            _SUB_HEADER,
            "0001-18-000001\t31-OCT-2018\t13F-HR\t0001111111\t30-SEP-2018",
            "0001-18-000002\t31-OCT-2018\t13F-NT\t0002222222\t30-SEP-2018",
        ]
    )
    infotable = "\n".join(
        [
            _INFO_HEADER,
            _info_row("0001-18-000001", "037833100", 1000),  # 보통주 보유 — 센다
            # 옵션(PUTCALL 채워짐) — 안 센다
            _info_row("0001-18-000001", "037833100", 500, sk=2, putcall="PUT"),
        ]
    )
    _write_13f_zip(root, "2018q4", submission=submission, infotable=infotable)

    result = sec_13f.extract_13f(root, snapshot_date="2026-09-28")
    assert result["periods_ok"] == ["2018q4"]
    assert not result["periods_failed"]

    import duckdb

    con = duckdb.connect()
    holdings = con.execute(
        f"SELECT cusip, period_of_report, n_holders, shares_total, n_filers_total_that_period"
        f" FROM '{result['inst_holdings_q']['path']}'"
    ).fetchall()
    assert holdings == [
        ("037833100", dt.date(2018, 9, 30), 1, 1000, 1),
    ]
    subs = con.execute(
        f"SELECT accession, submission_type, is_amendment, n_rows"
        f" FROM '{result['thirteenf_submissions']['path']}' ORDER BY accession"
    ).fetchall()
    assert subs == [
        ("0001-18-000001", "13F-HR", False, 2),
        ("0001-18-000002", "13F-NT", False, 0),
    ]


def test_extract_13f_full_amendment_replaces_the_original_across_files(tmp_path):
    """정정의 원본이 **다른 파일**에 있다(연구 §4.3) — 전량을 같이 읽어야 풀린다."""
    root = _lake(tmp_path)
    # 원본: 2018q3 파일에 있다고 가정 (파일 이름과 기준일이 다를 수 있다)
    submission_a = "\n".join(
        [
            _SUB_HEADER,
            "0001-18-000001\t14-NOV-2018\t13F-HR\t0001111111\t30-SEP-2018",
        ]
    )
    infotable_a = "\n".join([_INFO_HEADER, _info_row("0001-18-000001", "037833100", 1000)])
    # 정정: 2018q4 파일에 있다. 같은 (filer_cik, period_of_report) 를 대체한다 —
    # 전체 재제출(행 수 비율 1.0 근처)이고 주식 수가 바뀌었다.
    # filing_date는 원본(14-NOV-2018)보다 늦되 PIT 컷오프(period_of_report + 60일
    # = 2018-11-29) 안에 있어야 한다 — 20-NOV-2018 (51일 경과).
    submission_b = "\n".join(
        [
            _SUB_HEADER,
            "0001-18-000002\t20-NOV-2018\t13F-HR/A\t0001111111\t30-SEP-2018",
        ]
    )
    infotable_b = "\n".join([_INFO_HEADER, _info_row("0001-18-000002", "037833100", 1500)])
    _write_13f_zip(root, "2018q3", submission=submission_a, infotable=infotable_a)
    _write_13f_zip(root, "2018q4", submission=submission_b, infotable=infotable_b)

    result = sec_13f.extract_13f(root, snapshot_date="2026-09-28")
    assert set(result["periods_ok"]) == {"2018q3", "2018q4"}
    # 둘 다 INFOTABLE 행이 1개씩이라 행 수 비율은 1.0 — 부분 정정이 아니다.
    # (부분 정정 판정은 주식 수가 아니라 행 수 비율을 본다 — §4.3)
    assert result["partial_amendments"] == 0

    import duckdb

    con = duckdb.connect()
    holdings = con.execute(
        f"SELECT cusip, shares_total, n_holders"
        f" FROM '{result['inst_holdings_q']['path']}'"
    ).fetchall()
    # 정정(1500주)만 남는다 — 원본(1000주)은 대체되어 합산되지 않는다
    assert holdings == [("037833100", 1500, 1)]


def test_extract_13f_counts_a_partial_amendment(tmp_path):
    """정정본 행 수가 원본의 절반 미만이면 ``partial_amendments``에 센다.

    규칙 자체는 안 바꾼다 — 정정은 여전히 원본을 대체한다.
    """
    root = _lake(tmp_path)
    # 정정 filing_date(20-NOV-2018)는 원본(01-NOV-2018)보다 늦되 PIT 컷오프
    # (period_of_report + 60일 = 2018-11-29) 안에 있어야 한다.
    submission = "\n".join(
        [
            _SUB_HEADER,
            "0001-18-000001\t01-NOV-2018\t13F-HR\t0001111111\t30-SEP-2018",
            "0001-18-000002\t20-NOV-2018\t13F-HR/A\t0001111111\t30-SEP-2018",
        ]
    )
    infotable = "\n".join(
        [
            _INFO_HEADER,
            # 원본 4행
            _info_row("0001-18-000001", "111111100", 100, sk=1),
            _info_row("0001-18-000001", "222222200", 100, sk=2),
            _info_row("0001-18-000001", "333333300", 100, sk=3),
            _info_row("0001-18-000001", "444444400", 100, sk=4),
            # 정정 1행 — 절반 미만
            _info_row("0001-18-000002", "111111100", 100, sk=1),
        ]
    )
    _write_13f_zip(root, "2018q4", submission=submission, infotable=infotable)

    result = sec_13f.extract_13f(root, snapshot_date="2026-09-28")
    assert result["partial_amendments"] == 1

    import duckdb

    con = duckdb.connect()
    holdings = con.execute(
        f"SELECT cusip FROM '{result['inst_holdings_q']['path']}' ORDER BY cusip"
    ).fetchall()
    # 정정본(1행)만 남는다 — B·C·D CORP은 대체되어 사라진다
    assert holdings == [("111111100",)]


def test_extract_13f_dedups_othermanager_joint_holdings(tmp_path):
    """OTHERMANAGER가 찬 두 filer가 같은 (cusip, period, sshprnamt)를 보고하면
    filer 수(n_holders)에는 둘 다 세고, 주식 수 합(shares_total)은 한 번만 센다.
    """
    root = _lake(tmp_path)
    submission = "\n".join(
        [
            _SUB_HEADER,
            "0001-18-000001\t01-NOV-2018\t13F-HR\t0001111111\t30-SEP-2018",
            "0001-18-000002\t01-NOV-2018\t13F-HR\t0003333333\t30-SEP-2018",
        ]
    )
    infotable = "\n".join(
        [
            _INFO_HEADER,
            # 같은 주식 수(2000)를 공동 관리자로 각자 보고한다
            _info_row("0001-18-000001", "037833100", 2000, othermanager="0003333333"),
            _info_row("0001-18-000002", "037833100", 2000, othermanager="0001111111"),
        ]
    )
    _write_13f_zip(root, "2018q4", submission=submission, infotable=infotable)

    result = sec_13f.extract_13f(root, snapshot_date="2026-09-28")
    assert result["othermanager_filled_share"] == 1.0

    import duckdb

    con = duckdb.connect()
    holdings = con.execute(
        f"SELECT n_holders, shares_total FROM '{result['inst_holdings_q']['path']}'"
    ).fetchall()
    assert holdings == [(2, 2000)]


def test_extract_13f_groups_by_period_of_report_not_file_name(tmp_path):
    """파일 이름의 기간이 아니라 행의 ``PERIODOFREPORT``로 묶는다 — 한 파일에
    보고 기준일이 여럿 섞일 수 있다(연구 §4.2)."""
    root = _lake(tmp_path)
    # 두 번째 필러의 filing_date는 PIT 컷오프(period_of_report + 60일 =
    # 2018-08-29) 안에 있어야 한다 — 15-AUG-2018 (46일 경과).
    submission = "\n".join(
        [
            _SUB_HEADER,
            "0001-18-000001\t31-OCT-2018\t13F-HR\t0001111111\t30-SEP-2018",
            "0002-18-000001\t15-AUG-2018\t13F-HR\t0002222222\t30-JUN-2018",
        ]
    )
    infotable = "\n".join(
        [
            _INFO_HEADER,
            _info_row("0001-18-000001", "037833100", 1000),
            _info_row("0002-18-000001", "037833100", 500),
        ]
    )
    _write_13f_zip(root, "2018q4", submission=submission, infotable=infotable)

    result = sec_13f.extract_13f(root, snapshot_date="2026-09-28")

    import duckdb

    con = duckdb.connect()
    holdings = con.execute(
        f"SELECT period_of_report, shares_total FROM '{result['inst_holdings_q']['path']}'"
        f" ORDER BY period_of_report"
    ).fetchall()
    assert holdings == [
        (dt.date(2018, 6, 30), 500),
        (dt.date(2018, 9, 30), 1000),
    ]


def test_extract_13f_skips_a_broken_period_but_keeps_going(tmp_path):
    """기간 하나가 못 읽혀도 나머지는 굳는다 — ``periods_failed``에 남는다."""
    root = _lake(tmp_path)
    good_sub = "\n".join(
        [_SUB_HEADER, "0001-18-000001\t31-OCT-2018\t13F-HR\t0001111111\t30-SEP-2018"]
    )
    good_info = "\n".join([_INFO_HEADER, _info_row("0001-18-000001", "037833100", 1000)])
    _write_13f_zip(root, "2018q4", submission=good_sub, infotable=good_info)

    bad = sec_13f.thirteenf_raw_path(root, "2019q1")
    with zipfile.ZipFile(bad, "w") as zf:
        zf.writestr("SUBMISSION.tsv", good_sub)  # INFOTABLE.tsv 가 없다

    result = sec_13f.extract_13f(root, snapshot_date="2026-09-28")
    assert result["periods_ok"] == ["2018q4"]
    assert len(result["periods_failed"]) == 1
    assert "2019q1" in result["periods_failed"][0]
    assert result["inst_holdings_q"]["rows"] == 1


# --- PIT 컷오프(LAG_13F_DAYS) ---------------------------------------------------


def test_extract_13f_ignores_an_amendment_filed_after_the_cutoff(tmp_path):
    """정정본의 ``filing_date - period_of_report``가 ``LAG_13F_DAYS``를 넘으면
    무시한다 — 원본(컷오프 안)이 그대로 남는다. 원본이 몇 년 뒤 정정으로
    바뀌는 룩어헤드를 막는 규칙이다."""
    root = _lake(tmp_path)
    submission = "\n".join(
        [
            _SUB_HEADER,
            # 원본 — 컷오프(2018-09-30 + 60일 = 2018-11-29) 안
            "0001-18-000001\t01-NOV-2018\t13F-HR\t0001111111\t30-SEP-2018",
            # 정정 — 컷오프 밖 (107일 경과)
            "0001-18-000002\t15-JAN-2019\t13F-HR/A\t0001111111\t30-SEP-2018",
        ]
    )
    infotable = "\n".join(
        [
            _INFO_HEADER,
            _info_row("0001-18-000001", "037833100", 1000),
            _info_row("0001-18-000002", "037833100", 1500),
        ]
    )
    _write_13f_zip(root, "2018q4", submission=submission, infotable=infotable)

    result = sec_13f.extract_13f(root, snapshot_date="2026-09-28")
    assert result["submissions_after_cutoff"] == 1

    import duckdb

    con = duckdb.connect()
    # 원본(1000주)이 남는다 — 컷오프 밖 정정(1500주)은 대체를 못 한다
    holdings = con.execute(
        f"SELECT cusip, shares_total FROM '{result['inst_holdings_q']['path']}'"
    ).fetchall()
    assert holdings == [("037833100", 1000)]

    # thirteenf_submissions는 컷오프 없이 둘 다 남긴다
    subs = con.execute(
        f"SELECT accession FROM '{result['thirteenf_submissions']['path']}' ORDER BY accession"
    ).fetchall()
    assert subs == [("0001-18-000001",), ("0001-18-000002",)]


def test_extract_13f_drops_a_lone_submission_filed_after_the_cutoff(tmp_path):
    """단독 제출 하나뿐이어도 컷오프 밖이면 ``inst_holdings_q``에 행을 안 만든다
    — 옛 기준일에 뒤늦게 낸 제출 하나가 ``n_filers_total_that_period = 1``짜리
    이상치 행을 만드는 문제를 막는 규칙이다. ``thirteenf_submissions``에는
    그대로 남는다."""
    root = _lake(tmp_path)
    submission = "\n".join(
        [
            _SUB_HEADER,
            # 기준일 2006-09-30 인데 2018년에야 낸 제출 — 컷오프를 한참 넘는다
            "0001-18-000001\t01-NOV-2018\t13F-HR\t0001111111\t30-SEP-2006",
        ]
    )
    infotable = "\n".join([_INFO_HEADER, _info_row("0001-18-000001", "037833100", 700)])
    _write_13f_zip(root, "2018q4", submission=submission, infotable=infotable)

    result = sec_13f.extract_13f(root, snapshot_date="2026-09-28")
    assert result["submissions_after_cutoff"] == 1
    assert result["partial_amendments"] == 0
    assert result["inst_holdings_q"]["rows"] == 0

    import duckdb

    con = duckdb.connect()
    subs = con.execute(
        f"SELECT accession, n_rows FROM '{result['thirteenf_submissions']['path']}'"
    ).fetchall()
    assert subs == [("0001-18-000001", 1)]
