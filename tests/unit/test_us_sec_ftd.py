"""SEC 반월 Fails-to-Deliver — 목록 페이지 파싱·원문 파서·PIT 맵 (01_sec_ftd.md).

네트워크 없이 돈다. 합성 fixture로 헤더·트레일러·인코딩·CUSIP↔심볼 비일대일을 본다.
"""

from __future__ import annotations

import datetime as dt
import zipfile

import pytest

from collector.lake import DataRoot
from collector.us.sources import sec_ftd

# --- 목록 페이지 파싱 --------------------------------------------------------

#: 실제 네 경로(연구 §3.1)를 대표하되, 둘째는 실제 경로명이 길어 100자 제한에
#: 걸려 짧은 대역명(``freq-req``)으로 줄였다 — 파서는 경로 자체를 안 보므로
#: 대표성에는 영향이 없다.
_LISTING_HTML = """
<html><body>
<table>
<tr><td><a href="/files/data/fails-deliver-data/cnsfails202608b.zip">a</a></td></tr>
<tr><td><a href="/files/data/freq-req-foia-doc-fails-data/cnsfails202608a.zip">a</a></td></tr>
<tr><td><a href="/files/node/add/data_distribution/cnsfails201910a_0.zip">a</a></td></tr>
<tr><td><a href='/files/data/other/fails-deliver-data/cnsfails200907a.zip'>a</a></td></tr>
<tr><td><a href="/files/data/fails-deliver-data/somethingelse.zip">a</a></td></tr>
</table>
</body></html>
"""


def test_parse_listing_reads_all_four_path_shapes():
    """경로가 넷으로 섞여 있어도 파일 이름만으로 반월을 잡는다 (연구 §3.1)."""
    listing = sec_ftd.parse_listing(_LISTING_HTML)
    assert listing.files == {
        "202608b": "https://www.sec.gov/files/data/fails-deliver-data/cnsfails202608b.zip",
        "202608a": "https://www.sec.gov/files/data/freq-req-foia-doc-fails-data/cnsfails202608a.zip",
        # `_0` 접미사는 재발행 표시일 뿐이다 — 반월 태그로 정규화한다
        "201910a": "https://www.sec.gov/files/node/add/data_distribution/cnsfails201910a_0.zip",
        "200907a": "https://www.sec.gov/files/data/other/fails-deliver-data/cnsfails200907a.zip",
    }
    assert not listing.duplicates


def test_parse_listing_ignores_non_cnsfails_zip_links():
    # somethingelse.zip 은 cnsfails 를 안 담아 href 후보에도 안 든다
    listing = sec_ftd.parse_listing(_LISTING_HTML)
    assert not listing.unparsed


def test_parse_listing_keeps_the_first_url_on_duplicate_period():
    html = (
        '<a href="/files/data/fails-deliver-data/cnsfails202608b.zip">x</a>'
        '<a href="/files/data/other/fails-deliver-data/cnsfails202608b.zip">y</a>'
    )
    listing = sec_ftd.parse_listing(html)
    assert listing.files["202608b"].endswith("/fails-deliver-data/cnsfails202608b.zip")
    assert "202608b" in listing.duplicates


def test_parse_listing_raises_when_nothing_matches():
    with pytest.raises(sec_ftd.FtdError, match="하나도 못 찾았다"):
        sec_ftd.parse_listing("<html>no links here</html>")


def test_period_key_shape():
    assert sec_ftd.period_key(2026, 8, "b") == "202608b"
    assert sec_ftd.period_key(2009, 7, "a") == "200907a"
    with pytest.raises(ValueError, match="'a'·'b'"):
        sec_ftd.period_key(2026, 8, "c")


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


def test_list_ftd_files_uses_the_sec_client():
    from collector.us.sources import sec

    session = _Session(_Resp(200, _LISTING_HTML))
    client = sec.SecClient(user_agent="x/1 (a@b.c)", session=session)
    listing = sec_ftd.list_ftd_files(client)
    assert len(session.calls) == 1
    assert session.calls[0] == sec_ftd.LIST_URL
    assert "202608b" in listing.files


# --- 원문 파서 ---------------------------------------------------------------

_HEADER = "SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION|PRICE"


def test_parse_ftd_text_splits_data_from_trailer():
    """Trailer 두 줄은 정확한 문구를 몰라도 SETTLEMENT DATE 칸으로 걸러진다."""
    text = "\n".join(
        [
            _HEADER,
            "20260817|B5950S113|MDXH|1206826|MDXHEALTH SA SHS NEW(BELGIUM) |0.81",
            "20260818|037833100|AAPL|500|APPLE INC|227.5",
            "TOTAL RECORDS|2",  # Trailer 1 — 문구를 가정하지 않는다
            "TOTAL QUANTITY|1207326",  # Trailer 2
        ]
    )
    parsed = sec_ftd.parse_ftd_text(text)
    assert parsed.trailer_lines == 2
    assert len(parsed.rows) == 2
    assert parsed.rows[0].settlement_date == dt.date(2026, 8, 17)
    assert parsed.rows[0].cusip == "B5950S113"
    assert parsed.rows[0].symbol == "MDXH"
    assert parsed.rows[0].price == "0.81"
    # DESCRIPTION 의 뒤 공백은 셀을 자를 때 같이 벗겨진다
    assert parsed.rows[0].description == "MDXHEALTH SA SHS NEW(BELGIUM)"


def test_parse_ftd_text_handles_missing_price_dot():
    """PRICE가 빈 값이면 '.' 하나로 온다 (연구 §2)."""
    text = "\n".join([_HEADER, "20260817|B5950S113|MDXH|100|DESC|."])
    parsed = sec_ftd.parse_ftd_text(text)
    assert parsed.rows[0].price == "."


def test_parse_ftd_text_rejects_missing_header_column():
    bad_header = "SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION"  # PRICE 없음
    with pytest.raises(sec_ftd.FtdError, match="칸이 빠졌다"):
        sec_ftd.parse_ftd_text(bad_header + "\n20260817|X|Y|1|Z")


def test_parse_ftd_text_rejects_a_non_header_first_line():
    with pytest.raises(sec_ftd.FtdError, match="헤더가 아니다"):
        sec_ftd.parse_ftd_text("garbage,not,a,header\n1,2,3")


def test_parse_ftd_text_rejects_empty_file():
    with pytest.raises(sec_ftd.FtdError, match="비어 있다"):
        sec_ftd.parse_ftd_text("")


def test_parse_ftd_text_is_robust_to_header_column_order():
    """헤더 이름으로 칸을 잡는다 — 위치가 바뀌어도 된다."""
    header = "SYMBOL|SETTLEMENT DATE|CUSIP|PRICE|QUANTITY (FAILS)|DESCRIPTION"
    text = "\n".join([header, "MDXH|20260817|B5950S113|0.81|1206826|DESC"])
    parsed = sec_ftd.parse_ftd_text(text)
    row = parsed.rows[0]
    assert (row.symbol, row.settlement_date, row.cusip, row.price, row.quantity) == (
        "MDXH",
        dt.date(2026, 8, 17),
        "B5950S113",
        "0.81",
        "1206826",
    )


def test_decode_falls_back_to_latin1():
    """DESCRIPTION 에 latin-1 바이트가 섞여 있으면 UTF-8 디코딩이 실패한다 (연구 §2)."""
    raw = (_HEADER + "\n20260817|X|Y|1|CAF\xc9 INC|1.0").encode("latin-1")
    with pytest.raises(UnicodeDecodeError):
        raw.decode("utf-8")
    text = sec_ftd._decode(raw)
    assert "CAFÉ INC" in text


# --- zip 멤버 -----------------------------------------------------------------


def _write_ftd_zip(path, *, member_name, text, encoding="utf-8"):
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(member_name, text.encode(encoding))


def test_member_to_text_reads_the_single_member(tmp_path):
    p = tmp_path / "cnsfails202608b.zip"
    _write_ftd_zip(p, member_name="cnsfails202608b.txt", text=_HEADER + "\n20260817|X|Y|1|Z|1.0")
    text = sec_ftd._member_to_text(p)
    assert text.startswith(_HEADER)


def test_member_to_text_rejects_a_zip_with_two_members(tmp_path):
    p = tmp_path / "bad.zip"
    with zipfile.ZipFile(p, "w") as zf:
        zf.writestr("a.txt", "x")
        zf.writestr("b.txt", "y")
    with pytest.raises(sec_ftd.FtdError, match="하나가 아니다"):
        sec_ftd._member_to_text(p)


# --- raw 내려받기 --------------------------------------------------------------


def _zip_bytes(text: str) -> bytes:
    import io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("cnsfails202608b.txt", text)
    return buf.getvalue()


def test_download_ftd_skips_a_good_file(tmp_path):
    from collector.us.sources import sec

    root = DataRoot(tmp_path)
    dest = sec_ftd.ftd_raw_path(root, "202608b")
    dest.parent.mkdir(parents=True)
    dest.write_bytes(_zip_bytes(_HEADER + "\n20260817|X|Y|1|Z|1.0"))
    session = _Session(_Resp(200, "should-not-be-used"))
    r = sec_ftd.download_ftd(
        sec.SecClient("x/1 (a@b.c)", session=session), root, "202608b", "https://x/y.zip"
    )
    assert r["skipped"] is True
    assert session.calls == []


def test_download_ftd_refetches_a_broken_file(tmp_path):
    from collector.us.sources import sec

    root = DataRoot(tmp_path)
    dest = sec_ftd.ftd_raw_path(root, "202608b")
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"<html>Request Rate Threshold Exceeded</html>")

    class _BinResp(_Resp):
        def iter_content(self, chunk_size=1):
            yield self._raw

    resp = _BinResp(200)
    resp._raw = _zip_bytes(_HEADER + "\n20260817|X|Y|1|Z|1.0")
    session = _Session(resp)
    r = sec_ftd.download_ftd(
        sec.SecClient("x/1 (a@b.c)", session=session), root, "202608b", "https://x/y.zip"
    )
    assert r["skipped"] is False
    assert len(session.calls) == 1


def test_raw_periods_reads_what_is_on_disk(tmp_path):
    root = DataRoot(tmp_path)
    assert sec_ftd.raw_periods(root) == set()
    for period in ("200907a", "202608b"):
        p = sec_ftd.ftd_raw_path(root, period)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"PK\x03\x04")
    assert sec_ftd.raw_periods(root) == {"200907a", "202608b"}


# --- extract_ftd 종단 ---------------------------------------------------------


def _lake(tmp_path) -> DataRoot:
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()
    return DataRoot(tmp_path)


def test_extract_ftd_needs_a_raw_zip(tmp_path):
    with pytest.raises(sec_ftd.FtdError, match="받아 둔 zip이 없다"):
        sec_ftd.extract_ftd(_lake(tmp_path), snapshot_date="2026-09-24")


def test_extract_ftd_builds_fails_and_pit_map(tmp_path):
    """두 반월 파일에서 ``ftd_fails``·``cusip_symbol_pit``가 같이 나온다.

    시나리오: MDXH(CUSIP B5950S113)는 두 반월 모두 같은 심볼이고, 어떤 CUSIP
    (G000001AA)은 8월 전반에는 OLDSYM, 후반에는 NEWSYM 으로 나온다(티커 변경) —
    맵이 ``(cusip, symbol)`` 쌍마다 관측 구간을 따로 남기는지 본다.
    """
    root = _lake(tmp_path)

    text_a = "\n".join(
        [
            _HEADER,
            "20260803|B5950S113|MDXH|100|MDXHEALTH SA|1.0",
            "20260805|G000001AA|OLDSYM|200|RENAMED CO|2.0",
            "TOTAL RECORDS|2",
            "TOTAL QUANTITY|300",
        ]
    )
    text_b = "\n".join(
        [
            _HEADER,
            "20260817|B5950S113|MDXH|150|MDXHEALTH SA|.",  # PRICE 결측
            "20260820|G000001AA|NEWSYM|400|RENAMED CO|4.0",  # 티커가 바뀌었다
            "TOTAL RECORDS|2",
            "TOTAL QUANTITY|550",
        ]
    )
    p_a = sec_ftd.ftd_raw_path(root, "202608a")
    p_b = sec_ftd.ftd_raw_path(root, "202608b")
    p_a.parent.mkdir(parents=True, exist_ok=True)
    _write_ftd_zip(p_a, member_name="cnsfails202608a.txt", text=text_a)
    _write_ftd_zip(p_b, member_name="cnsfails202608b.txt", text=text_b)

    result = sec_ftd.extract_ftd(root, snapshot_date="2026-09-24")

    assert result["files"] == 2
    assert set(result["periods_ok"]) == {"202608a", "202608b"}
    assert not result["periods_failed"]
    assert result["trailer_lines_by_period"] == {"202608a": 2, "202608b": 2}
    assert result["price_missing_rows"] == 1
    assert result["ftd_fails"]["rows"] == 4
    assert result["cusip_symbol_pit"]["rows"] == 3  # (MDXH) 1 + (OLDSYM, NEWSYM) 2

    import duckdb

    con = duckdb.connect()
    fails = con.execute(
        f"SELECT settlement_date, cusip, symbol, quantity, price, source_rev"
        f" FROM '{result['ftd_fails']['path']}' ORDER BY settlement_date"
    ).fetchall()
    assert fails[0] == (dt.date(2026, 8, 3), "B5950S113", "MDXH", 100, 1.0, "202608a")
    assert fails[2][:5] == (dt.date(2026, 8, 17), "B5950S113", "MDXH", 150, None)  # "." -> null

    pit = {
        (r[0], r[1]): r[2:]
        for r in con.execute(
            f"SELECT cusip, symbol, first_seen, last_seen, n_settlement_dates"
            f" FROM '{result['cusip_symbol_pit']['path']}' ORDER BY cusip, symbol"
        ).fetchall()
    }
    assert pit[("B5950S113", "MDXH")] == (dt.date(2026, 8, 3), dt.date(2026, 8, 17), 2)
    assert pit[("G000001AA", "OLDSYM")] == (dt.date(2026, 8, 5), dt.date(2026, 8, 5), 1)
    assert pit[("G000001AA", "NEWSYM")] == (dt.date(2026, 8, 20), dt.date(2026, 8, 20), 1)


def test_extract_ftd_skips_a_broken_period_but_keeps_going(tmp_path):
    """반월 하나가 못 읽혀도 나머지는 굳는다 — ``periods_failed``에 남는다."""
    root = _lake(tmp_path)
    good = sec_ftd.ftd_raw_path(root, "202608b")
    good.parent.mkdir(parents=True, exist_ok=True)
    _write_ftd_zip(good, member_name="cnsfails202608b.txt", text=_HEADER + "\n20260817|X|Y|1|Z|1.0")
    bad = sec_ftd.ftd_raw_path(root, "202608a")
    with zipfile.ZipFile(bad, "w") as zf:
        zf.writestr("a.txt", "x")
        zf.writestr("b.txt", "y")  # 멤버가 둘 — FtdError

    result = sec_ftd.extract_ftd(root, snapshot_date="2026-09-24")
    assert result["periods_ok"] == ["202608b"]
    assert len(result["periods_failed"]) == 1
    assert "202608a" in result["periods_failed"][0]
    assert result["ftd_fails"]["rows"] == 1
