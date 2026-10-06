"""nasdaqtrader 심볼 디렉터리 원문 수집. 네트워크 없이 FTP를 가짜로 바꾼다."""

from __future__ import annotations

import datetime as dt
import ftplib
import gzip

import pytest

from collector.lake import DataRoot
from collector.us.ops import daily
from collector.us.sources import nasdaqtrader_symdir as sd
from collector.us.sources.wayback import ListingFile, parse_listing_file


def _lake(tmp_path) -> DataRoot:
    for layer in ("raw", "derived", "datasets", "output"):
        (tmp_path / layer).mkdir()
    return DataRoot(tmp_path)


def _body(kind: str, *, rows: int = 1_200, created: str = "1005202618:01") -> bytes:
    header = sd.KINDS[kind][1]
    n = header.count("|")
    lines = [header]
    for i in range(rows):
        sym = f"S{i:05d}"
        if kind == "nasdaqlisted":
            lines.append(f"{sym}|Name {i} - Common Stock|Q|N|N|100|N|N")
        else:
            lines.append(f"{sym}|Name {i}|N|{sym}|N|100|N|{sym}")
    lines.append(f"File Creation Time: {created}" + "|" * n)
    return ("\r\n".join(lines) + "\r\n").encode("ascii")


class FakeClient:
    """``SymdirClient.get``를 대신한다. 요청 수를 센다."""

    def __init__(self, bodies: dict[str, bytes]):
        self.bodies = bodies
        self.calls: list[str] = []

    def get(self, url: str) -> bytes:
        self.calls.append(url)
        for kind, (u, _) in sd.KINDS.items():
            if u == url:
                return self.bodies[kind]
        raise AssertionError(url)


def _client(**over) -> FakeClient:
    bodies = {k: _body(k) for k in sd.KINDS}
    bodies.update(over)
    return FakeClient(bodies)


NOW = dt.datetime(2026, 10, 6, 0, 30, 15, tzinfo=dt.UTC)  # KST 09:30


def test_writes_raw_bytes_and_listing_file_reads_them(tmp_path):
    root = _lake(tmp_path)
    client = _client()
    res = sd.fetch_kind(client, root, "nasdaqlisted", now=NOW)
    assert res["status"] == "written"
    path = sd.symdir_dir(root) / "nasdaqlisted_20261006003015.txt"
    assert path.read_bytes() == client.bodies["nasdaqlisted"]  # 원문 그대로
    lf = ListingFile(path)
    assert (lf.kind, lf.snapshot) == ("nasdaqlisted", "20261006003015")
    as_of, rows = parse_listing_file(path)
    assert as_of == dt.datetime(2026, 10, 5, 18, 1) and len(rows) == 1_200
    # 임시 파일이 안 남는다
    assert [p.name for p in sd.symdir_dir(root).glob("*.part")] == []
    assert [p.name for p in sd.symdir_dir(root).glob(".*")] == []


def test_both_kinds_have_parseable_names(tmp_path):
    root = _lake(tmp_path)
    client = _client()
    for kind in sd.KINDS:
        sd.fetch_kind(client, root, kind, now=NOW)
    kinds = {ListingFile(p).kind for p in sd.symdir_dir(root).glob("*.txt")}
    assert kinds == {"nasdaqlisted", "otherlisted"}


def test_same_creation_time_is_not_written_again(tmp_path):
    root = _lake(tmp_path)
    client = _client()
    sd.fetch_kind(client, root, "otherlisted", now=NOW)
    later = NOW + dt.timedelta(days=1)
    res = sd.fetch_kind(client, root, "otherlisted", now=later)
    assert res["status"] == "unchanged"
    assert len(sd.saved_files(root, "otherlisted")) == 1


def test_new_creation_time_is_written(tmp_path):
    root = _lake(tmp_path)
    sd.fetch_kind(_client(), root, "otherlisted", now=NOW)
    newer = _client(otherlisted=_body("otherlisted", created="1006202618:01"))
    res = sd.fetch_kind(newer, root, "otherlisted", now=NOW + dt.timedelta(days=1))
    assert res["status"] == "written"
    assert len(sd.saved_files(root, "otherlisted")) == 2


def test_second_run_same_kst_day_makes_no_request(tmp_path):
    root = _lake(tmp_path)
    client = _client()
    sd.fetch_kind(client, root, "nasdaqlisted", now=NOW)
    assert len(client.calls) == 1
    res = sd.fetch_kind(client, root, "nasdaqlisted", now=NOW + dt.timedelta(hours=3))
    assert res["status"] == "already_today" and len(client.calls) == 1
    # 같은 파일이라 안 쓴 날도 "오늘 받음"으로 센다
    sd.fetch_kind(client, root, "nasdaqlisted", now=NOW + dt.timedelta(days=1))
    again = NOW + dt.timedelta(days=1, hours=2)
    assert sd.fetch_kind(client, root, "nasdaqlisted", now=again)["status"] == "already_today"
    assert len(client.calls) == 2


def test_kst_day_boundary(tmp_path):
    """UTC로는 같은 날이어도 KST 날짜가 바뀌면 다시 받는다."""
    root = _lake(tmp_path)
    client = _client()
    t1 = dt.datetime(2026, 10, 6, 14, 0, tzinfo=dt.UTC)  # KST 23:00
    t2 = dt.datetime(2026, 10, 6, 16, 0, tzinfo=dt.UTC)  # KST 다음날 01:00
    sd.fetch_kind(client, root, "nasdaqlisted", now=t1)
    sd.fetch_kind(client, root, "nasdaqlisted", now=t2)
    assert len(client.calls) == 2


def test_force_ignores_daily_limit(tmp_path):
    root = _lake(tmp_path)
    client = _client()
    sd.fetch_kind(client, root, "nasdaqlisted", now=NOW)
    res = sd.fetch_kind(client, root, "nasdaqlisted", now=NOW, force=True)
    assert len(client.calls) == 2 and res["status"] == "unchanged"


@pytest.mark.parametrize(
    "bad",
    [
        gzip.compress(_body("nasdaqlisted")),
        b"<html><body>Access denied</body></html>\r\n" * 3,
        _body("nasdaqlisted").replace(b"File Creation Time:", b"Created:"),
        _body("nasdaqlisted", rows=10),
        b"",
    ],
    ids=["gzip", "not_header", "no_creation_time", "few_rows", "empty"],
)
def test_validation_failure_leaves_no_file(tmp_path, bad):
    root = _lake(tmp_path)
    client = _client(nasdaqlisted=bad)
    with pytest.raises(sd.SymdirError):
        sd.fetch_kind(client, root, "nasdaqlisted", now=NOW)
    out = sd.symdir_dir(root)
    leftover = [p.name for p in out.iterdir()] if out.exists() else []
    assert leftover == []
    # 실패는 "오늘 받음"으로 안 센다 — 다음 실행이 다시 시도한다
    assert sd.fetched_today(root, "nasdaqlisted", now=NOW) is False


def test_notice_created_and_not_overwritten(tmp_path):
    root = _lake(tmp_path)
    sd.fetch_kind(_client(), root, "nasdaqlisted", now=NOW)
    notice = sd.symdir_dir(root) / "NOTICE"
    text = notice.read_text(encoding="utf-8")
    assert "Copyright © 2021, The Nasdaq, Inc. All rights reserved." in text
    assert "2026-10-06" in text and "06_nasdaq_trader.md" in text
    assert "nasdaqlisted.txt" in text
    assert "ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt" in text
    assert "Incapsula" in text
    notice.write_text("고친 내용", encoding="utf-8")
    assert sd.ensure_notice(root) is False
    assert notice.read_text(encoding="utf-8") == "고친 내용"
    # 빌더 glob(*.txt)에 안 걸린다
    assert notice not in set(sd.symdir_dir(root).glob("*.txt"))


# --- FTP 클라이언트 ---------------------------------------------------------


class FakeFTP:
    """``ftplib.FTP``를 대신한다. 연결 수와 호출 순서를 남긴다."""

    instances: list[FakeFTP] = []
    script: list[object] = []  # 연결마다 하나씩 꺼낸다: bytes면 성공, 예외면 retrbinary에서 던진다

    def __init__(self, host, timeout=None):
        self.host, self.timeout = host, timeout
        self.log: list[str] = []
        self.closed = False
        FakeFTP.instances.append(self)

    def login(self):
        self.log.append("login")

    def cwd(self, d):
        self.log.append(f"cwd {d}")

    def retrbinary(self, cmd, cb):
        self.log.append(cmd)
        item = FakeFTP.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        cb(item[:7])
        cb(item[7:])

    def quit(self):
        self.closed = True

    def close(self):
        self.closed = True


@pytest.fixture
def fake_ftp():
    FakeFTP.instances, FakeFTP.script = [], []
    return FakeFTP


def _ftp_client(**over) -> sd.SymdirClient:
    kw = dict(interval_seconds=0, retry_wait_seconds=0, ftp_factory=FakeFTP)
    kw.update(over)
    return sd.SymdirClient(**kw)


def test_ftp_returns_bytes_as_received(fake_ftp):
    body = _body("otherlisted")
    fake_ftp.script = [body]
    c = _ftp_client()
    assert c.get(sd.KINDS["otherlisted"][0]) == body
    (f,) = fake_ftp.instances
    assert f.host == "ftp.nasdaqtrader.com" and f.timeout == 60
    assert f.log == ["login", "cwd symboldirectory", "RETR otherlisted.txt"]
    assert f.closed and c.requests_made == 1


def test_ftp_new_connection_per_kind(fake_ftp):
    fake_ftp.script = [_body("nasdaqlisted"), _body("otherlisted")]
    c = _ftp_client()
    for kind in sd.KINDS:
        c.get(sd.KINDS[kind][0])
    assert len(fake_ftp.instances) == 2 and all(f.closed for f in fake_ftp.instances)
    assert [f.log[-1] for f in fake_ftp.instances] == [
        "RETR nasdaqlisted.txt",
        "RETR otherlisted.txt",
    ]


@pytest.mark.parametrize("exc", [ftplib.error_temp("421 busy"), TimeoutError("t"), EOFError()])
def test_ftp_transient_error_retries_then_succeeds(fake_ftp, exc):
    body = _body("nasdaqlisted")
    fake_ftp.script = [exc, body]
    c = _ftp_client()
    assert c.get(sd.KINDS["nasdaqlisted"][0]) == body
    assert len(fake_ftp.instances) == 2 and c.requests_made == 2
    assert all(f.closed for f in fake_ftp.instances)


def test_ftp_transient_error_gives_up_after_retries(fake_ftp):
    fake_ftp.script = [ftplib.error_temp("421")] * 3
    c = _ftp_client()
    with pytest.raises(sd.SymdirError):
        c.get(sd.KINDS["nasdaqlisted"][0])
    assert len(fake_ftp.instances) == 3


def test_ftp_permanent_error_fails_immediately(fake_ftp):
    fake_ftp.script = [ftplib.error_perm("550 no such file")]
    c = _ftp_client()
    with pytest.raises(sd.SymdirError, match="550"):
        c.get(sd.KINDS["nasdaqlisted"][0])
    assert len(fake_ftp.instances) == 1 and fake_ftp.instances[0].closed


# --- us-daily 등록 -----------------------------------------------------------


def _patch_client(monkeypatch, client):
    monkeypatch.setattr(sd, "SymdirClient", lambda: client)


def test_registered_in_daily_sources():
    assert "nasdaqtrader_symdir" in daily.SOURCES


def test_run_source_reports_like_other_sources(tmp_path, monkeypatch):
    root = _lake(tmp_path)
    client = _client()
    _patch_client(monkeypatch, client)
    run = daily.run_nasdaqtrader_symdir(root)
    assert run.name == "nasdaqtrader_symdir" and run.ok and run.fetched == 2
    assert run.missing == [] and run.pending == 0
    # 같은 날 다시 돌리면 요청이 없다
    again = daily.run_nasdaqtrader_symdir(root)
    assert again.fetched == 0 and again.skipped == 2 and len(client.calls) == 2
    forced = daily.run_nasdaqtrader_symdir(root, force=True)
    assert len(client.calls) == 4 and forced.fetched == 0 and forced.ok


def test_run_source_dry_run_does_not_request_or_write(tmp_path, monkeypatch):
    root = _lake(tmp_path)
    client = _client()
    _patch_client(monkeypatch, client)
    run = daily.run_nasdaqtrader_symdir(root, dry_run=True)
    assert run.pending == 2 and run.fetched == 0 and client.calls == []
    assert not sd.symdir_dir(root).exists()


def test_run_source_failure_is_reported_and_other_kind_continues(tmp_path, monkeypatch):
    root = _lake(tmp_path)
    client = _client(nasdaqlisted=gzip.compress(b"x" * 100))
    _patch_client(monkeypatch, client)
    run = daily.run_nasdaqtrader_symdir(root)
    assert run.ok is False and run.fetched == 1
    assert len(run.missing) == 1 and run.missing[0].startswith("nasdaqlisted")


def test_run_daily_failure_does_not_block_other_sources(tmp_path, monkeypatch):
    from collector.us import calendars

    root = _lake(tmp_path)
    calendars.load_trading_calendar(
        root, snapshot_date="2026-09-01", start="2026-01-01", end="2026-12-31"
    )
    _patch_client(monkeypatch, _client(nasdaqlisted=b"", otherlisted=b""))
    result = daily.run_daily(
        root,
        snapshot_date="2026-10-06",
        today=dt.date(2026, 10, 6),
        sources=("nasdaqtrader_symdir", "finra_regsho"),
        dry_run=False,
        budget_seconds=0.0,
    )
    by = {s["name"]: s for s in result["sources"]}
    assert set(by) == {"nasdaqtrader_symdir", "finra_regsho"}
    assert by["nasdaqtrader_symdir"]["ok"] is False and result["ok"] is False


def test_run_daily_dry_run_selects_source(tmp_path):
    from collector.us import calendars

    root = _lake(tmp_path)
    calendars.load_trading_calendar(
        root, snapshot_date="2026-09-01", start="2026-01-01", end="2026-12-31"
    )
    result = daily.run_daily(
        root,
        snapshot_date="2026-10-06",
        today=dt.date(2026, 10, 6),
        sources=("nasdaqtrader_symdir",),
        dry_run=True,
    )
    (s,) = result["sources"]
    assert s["name"] == "nasdaqtrader_symdir" and s["pending"] == 2 and s["ok"]
