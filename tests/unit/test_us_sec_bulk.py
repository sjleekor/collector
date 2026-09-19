"""SEC 벌크 ZIP 적재 — 미국 계획 03 §4.3·§4.11·§4.12, 04 C6. 네트워크 없이 돈다."""

from __future__ import annotations

import datetime as dt
import json
import zipfile

import duckdb
import pytest

from collector.lake import DataRoot
from collector.us.sources import sec, sec_bulk


def _lake(tmp_path) -> DataRoot:
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()
    return DataRoot(tmp_path)


# --- 이어받기 ----------------------------------------------------------------


class _RangeSession:
    """Range를 지켜 주는 서버 흉내."""

    def __init__(self, body: bytes, *, honor_range: bool = True):
        self.body = body
        self.honor_range = honor_range
        self.sent: list[dict] = []

    def get(self, url, headers=None, **kw):
        self.sent.append(dict(headers or {}))
        start = 0
        rng = (headers or {}).get("Range")
        if rng and self.honor_range:
            start = int(rng.split("=")[1].split("-")[0])
            return _RangeResp(
                206, self.body[start:],
                {"Content-Range": f"bytes {start}-{len(self.body) - 1}/{len(self.body)}"},
            )
        return _RangeResp(200, self.body, {"Content-Length": str(len(self.body))})


class _RangeResp:
    def __init__(self, status, body, headers):
        self.status_code = status
        self._body = body
        self.headers = headers

    def iter_content(self, chunk_size=1):
        yield self._body

    def close(self):
        pass


def _zip_bytes(names=("a.json",)):
    import io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for n in names:
            zf.writestr(n, "{}")
    return buf.getvalue()


def test_download_bulk_resumes_from_the_part_file(tmp_path):
    """.part에 있는 만큼 Range로 건너뛰고 붙인다 — 1.4GB를 처음부터 다시 받지 않는다."""
    root = _lake(tmp_path)
    body = _zip_bytes()
    part = sec.bulk_path(root, "companyfacts")
    part.parent.mkdir(parents=True)
    part.with_name(part.name + ".part").write_bytes(body[:10])

    session = _RangeSession(body)
    r = sec.download_bulk(sec.SecClient("x/1 (a@b.c)", session=session), root, "companyfacts")
    assert r["done"] is True
    assert session.sent[0]["Range"] == "bytes=10-"
    assert part.read_bytes() == body


def test_download_bulk_starts_over_when_range_is_ignored(tmp_path):
    """서버가 200으로 전부 보내면 받아 둔 것을 버린다. 이어붙이면 가운데가 겹친다."""
    root = _lake(tmp_path)
    body = _zip_bytes()
    dest = sec.bulk_path(root, "submissions")
    dest.parent.mkdir(parents=True)
    dest.with_name(dest.name + ".part").write_bytes(b"XXXXXXXXXX")

    session = _RangeSession(body, honor_range=False)
    r = sec.download_bulk(sec.SecClient("x/1 (a@b.c)", session=session), root, "submissions")
    assert r["done"] is True
    assert dest.read_bytes() == body


def test_bulk_path_rejects_unknown_kind(tmp_path):
    with pytest.raises(ValueError, match="모르는 갈래"):
        sec.bulk_path(_lake(tmp_path), "frames")


# --- companyfacts -> fundamentals --------------------------------------------


def _companyfacts_zip(path):
    doc = {
        "cik": 320193,
        "entityName": "Apple Inc.",
        "facts": {
            "us-gaap": {
                "AccountsPayableCurrent": {
                    "units": {
                        "USD": [
                            # 같은 end에 값이 둘. filed가 갈라 준다 (D8)
                            {"end": "2017-09-30", "val": 49049000000, "accn": "a1",
                             "fy": 2017, "fp": "FY", "form": "10-K", "filed": "2017-11-03"},
                            {"end": "2017-09-30", "val": 44242000000, "accn": "a2",
                             "fy": 2018, "fp": "FY", "form": "10-K", "filed": "2018-11-05",
                             "frame": "CY2017Q3I"},
                        ]
                    }
                },
                "Revenues": {
                    "units": {
                        "USD": [
                            {"start": "2017-10-01", "end": "2017-12-30", "val": 88293000000,
                             "accn": "a3", "fy": 2018, "fp": "Q1", "form": "10-Q",
                             "filed": "2018-02-02"},
                            # 숫자가 아닌 val은 버린다 — 계약이 float64다
                            {"end": "2017-12-30", "val": "n/a", "accn": "a4",
                             "form": "10-Q", "filed": "2018-02-02"},
                        ]
                    }
                },
            }
        },
    }
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("CIK0000320193.json", json.dumps(doc))
        zf.writestr("CIK0000000001.json", json.dumps({"cik": 1, "facts": {}}))


def test_load_companyfacts_keeps_the_restatement_history(tmp_path):
    root = _lake(tmp_path)
    (root.raw / "sec" / "bulk").mkdir(parents=True)
    _companyfacts_zip(sec.bulk_path(root, "companyfacts"))

    r = sec_bulk.load_companyfacts(root, snapshot_date=dt.date(2026, 1, 1))
    assert r["entities"] == 2
    assert r["rows"] == 3
    assert r["skipped_nonnumeric"] == 1
    assert r["source_rev"].startswith("companyfacts.zip:")

    con = duckdb.connect()
    rows = con.execute(
        f"SELECT val, filed, frame, start FROM '{r['path']}'"
        " WHERE tag='AccountsPayableCurrent' ORDER BY filed"
    ).fetchall()
    assert [x[0] for x in rows] == [49049000000.0, 44242000000.0]
    assert rows[0][1] == dt.date(2017, 11, 3) and rows[0][2] is None
    assert rows[0][3] is None  # 시점값(instant)은 start가 없다
    assert rows[1][2] == "CY2017Q3I"


def test_load_companyfacts_can_narrow_to_a_cik_list(tmp_path):
    root = _lake(tmp_path)
    (root.raw / "sec" / "bulk").mkdir(parents=True)
    _companyfacts_zip(sec.bulk_path(root, "companyfacts"))
    r = sec_bulk.load_companyfacts(root, snapshot_date="2026-01-01", ciks=[1])
    assert r["entities"] == 1 and r["rows"] == 0


# --- submissions -> filings_index · company_meta -----------------------------


def _submissions_zip(path):
    recent = {
        "accessionNumber": ["acc-new", "acc-old"],
        "filingDate": ["2026-09-17", "2017-01-05"],
        "reportDate": ["2026-09-15", ""],
        "acceptanceDateTime": ["2026-09-17T22:30:24.000Z", "2017-01-05T12:00:00.000Z"],
        "act": ["34", ""],
        "form": ["8-K", "8-K"],
        "fileNumber": ["001-1", ""],
        "items": ["2.02,9.01", ""],
        "core_type": ["8-K", "8-K"],
        "size": [10785, 100],
        "isXBRL": [1, 0],
        "isInlineXBRL": [0, 0],
        "primaryDocument": ["a.htm", "b.htm"],
    }
    # 넘침 파일. 첫 행이 recent와 완전히 같다 — 원천에 실제로 있는 겹침이다
    over = {k: list(v) for k, v in recent.items()}
    over["accessionNumber"] = ["acc-new", "acc-mid"]
    over["filingDate"] = ["2026-09-17", "2019-05-05"]
    doc = {
        "cik": "0000000123",
        "name": "TEST CO",
        "entityType": "operating",
        "sic": "3571",
        "sicDescription": "Electronic Computers",
        "category": "Large accelerated filer",
        "fiscalYearEnd": "0926",
        "stateOfIncorporation": "CA",
        "ein": "1",
        # 목록에 null이 섞여 온다
        "tickers": ["TST", None],
        "exchanges": ["Nasdaq", None],
        "formerNames": [{"name": "OLD CO", "from": "2001-01-01T05:00:00.000Z"}],
        "filings": {"recent": recent, "files": [{"name": "CIK0000000123-submissions-001.json"}]},
    }
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("CIK0000000123.json", json.dumps(doc))
        zf.writestr("CIK0000000123-submissions-001.json", json.dumps(over))
        zf.writestr("CIK0000000999.json", json.dumps({"cik": "0000000999", "name": "SKIP"}))


def test_load_submissions_merges_overflow_and_folds_exact_repeats(tmp_path):
    root = _lake(tmp_path)
    (root.raw / "sec" / "bulk").mkdir(parents=True)
    _submissions_zip(sec.bulk_path(root, "submissions"))

    r = sec_bulk.load_submissions(
        root, snapshot_date=dt.date(2026, 1, 1), ciks=[123], since=dt.date(2018, 1, 1)
    )
    assert r["entities"] == 1
    assert r["duplicate_rows"] == 1  # recent와 넘침에 같은 행이 하나씩
    fi = r["filings_index"]
    assert fi["rows"] == 2  # acc-new · acc-mid. acc-old는 since 밖이다

    con = duckdb.connect()
    rows = con.execute(
        f"SELECT accession, form, filing_date, report_date, items, is_xbrl,"
        f" acceptance_datetime FROM '{fi['path']}' ORDER BY filing_date"
    ).fetchall()
    assert [x[0] for x in rows] == ["acc-mid", "acc-new"]
    assert rows[1][3] == dt.date(2026, 9, 15) and rows[1][4] == "2.02,9.01"
    assert rows[1][5] is True
    # Z가 붙은 값을 UTC로 읽는다 — 장 마감 뒤 접수를 가리려면 시각이 살아야 한다
    assert rows[1][6] == dt.datetime(2026, 9, 17, 22, 30, 24, tzinfo=dt.UTC)
    assert rows[0][3] is None  # reportDate 빈 문자열은 null

    meta = con.execute(
        f"SELECT cik, name, tickers, exchanges, former_names, sic"
        f" FROM '{r['company_meta']['path']}'"
    ).fetchall()
    assert meta[0][:4] == (123, "TEST CO", "TST", "Nasdaq")  # 목록의 null을 걸렀다
    assert json.loads(meta[0][4])[0]["name"] == "OLD CO"
    assert meta[0][5] == "3571"


def test_load_submissions_since_none_keeps_everything(tmp_path):
    root = _lake(tmp_path)
    (root.raw / "sec" / "bulk").mkdir(parents=True)
    _submissions_zip(sec.bulk_path(root, "submissions"))
    r = sec_bulk.load_submissions(
        root, snapshot_date="2026-01-01", ciks=[123], since=None
    )
    assert r["filings_index"]["rows"] == 3
