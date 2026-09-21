"""과거 티커 → CIK 맵 (미국 2차 후속 `03_cik_pit_probe.md`).

**이 코드가 생긴 이유.** `universe_daily.cik`이 SEC의 **오늘자**
`company_tickers.json` 한 벌에서 왔다. 상폐·피인수·개명한 회사가 구조적으로
빠져서 **`cik`이 붙었나가 곧 "2026년에도 살아 있나"**였다 — 끝까지 남은 종목
98.9% 대 사라진 종목 26.5% (2026-09-21 실측).

시험이 지키는 것 셋:

1. **gzip을 푼다.** `id_` 모드가 아카이브된 원본 바이트를 그대로 주므로
   `Content-Encoding: gzip`이던 응답은 압축된 채로 온다. probe 19개 중
   13개가 그랬고 첫 실행이 `UnicodeDecodeError`로 죽었다
2. **`as_of`를 파일 이름에서 읽는다** — 합집합이 아니라 PIT로 쓰라고
3. **스냅샷이 없으면 옛 동작으로 되돌아간다** — 배포 순서가 어긋나도 안 죽는다
"""

from __future__ import annotations

import gzip
import json
from datetime import date

import pytest

from collector.lake import DataRoot
from collector.us.sources import wayback

SAMPLE = {
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 789019, "ticker": "msft", "title": "MICROSOFT CORP"},
}


def _write(root: DataRoot, ts: str, payload: dict, *, gz: bool = False) -> None:
    path = wayback.company_tickers_path(root, ts)
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload).encode()
    path.write_bytes(gzip.compress(body) if gz else body)


def test_parses_plain_json(tmp_path) -> None:
    root = DataRoot(tmp_path)
    _write(root, "20200710094727", SAMPLE)
    as_of, pairs = wayback.parse_company_tickers(
        wayback.company_tickers_path(root, "20200710094727")
    )
    assert as_of == date(2020, 7, 10)
    assert sorted(pairs) == [("AAPL", 320193), ("MSFT", 789019)]


def test_parses_gzip_body(tmp_path) -> None:
    """**받은 바이트를 그대로 굳힌다.** 푸는 것은 읽을 때다 (X1)."""
    root = DataRoot(tmp_path)
    _write(root, "20180122203859", SAMPLE, gz=True)
    path = wayback.company_tickers_path(root, "20180122203859")
    assert path.read_bytes()[:2] == b"\x1f\x8b"  # 정말 gzip 인지부터
    as_of, pairs = wayback.parse_company_tickers(path)
    assert as_of == date(2018, 1, 22)
    assert ("AAPL", 320193) in pairs


def test_ticker_is_upper_cased(tmp_path) -> None:
    root = DataRoot(tmp_path)
    _write(root, "20200710094727", SAMPLE)
    _, pairs = wayback.parse_company_tickers(
        wayback.company_tickers_path(root, "20200710094727")
    )
    assert ("MSFT", 789019) in pairs


def test_map_keeps_one_row_per_snapshot_and_symbol(tmp_path) -> None:
    """**합집합으로 뭉개지 않는다.** 같은 티커가 스냅샷마다 다른 CIK 일 수 있다."""
    root = DataRoot(tmp_path)
    _write(root, "20190119054359", {"0": {"cik_str": 111, "ticker": "XYZ"}})
    _write(root, "20230103190354", {"0": {"cik_str": 222, "ticker": "XYZ"}})
    rows = wayback.ticker_cik_map(root)
    assert sorted(rows) == [
        ("XYZ", 111, date(2019, 1, 19)),
        ("XYZ", 222, date(2023, 1, 3)),
    ]


def test_duplicate_ticker_inside_one_snapshot_takes_the_first(tmp_path) -> None:
    root = DataRoot(tmp_path)
    _write(
        root,
        "20220103153400",
        {"0": {"cik_str": 1, "ticker": "DUP"}, "1": {"cik_str": 2, "ticker": "DUP"}},
    )
    rows = wayback.ticker_cik_map(root)
    assert rows == [("DUP", 1, date(2022, 1, 3))]


def test_falls_back_to_todays_map_when_no_snapshots(tmp_path) -> None:
    """**배포 순서가 어긋나도 안 죽는다.** 대신 생존편향이 있는 옛 동작이다."""
    root = DataRoot(tmp_path)
    legacy = root.raw / "sec" / "company_tickers" / "company_tickers.json"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(json.dumps(SAMPLE))
    rows = wayback.ticker_cik_map(root)
    assert {(s, c) for s, c, _ in rows} == {("AAPL", 320193), ("MSFT", 789019)}
    assert len({d for *_, d in rows}) == 1  # as_of 가 한 종류 = 되돌아갔다는 표시


def test_raises_when_nothing_at_all(tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match="us-tickers sync"):
        wayback.ticker_cik_map(DataRoot(tmp_path))


def test_rejects_a_file_without_a_timestamp(tmp_path) -> None:
    root = DataRoot(tmp_path)
    path = wayback.company_tickers_dir(root) / "company_tickers_nope.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(SAMPLE))
    with pytest.raises(wayback.WaybackParseError, match="timestamp"):
        wayback.parse_company_tickers(path)


def test_rejects_an_empty_map(tmp_path) -> None:
    """403 HTML 이나 빈 응답을 조용히 통과시키면 안 된다."""
    root = DataRoot(tmp_path)
    _write(root, "20200710094727", {})
    with pytest.raises(wayback.WaybackParseError, match="티커가 하나도 없다"):
        wayback.parse_company_tickers(wayback.company_tickers_path(root, "20200710094727"))


def test_download_skips_what_is_already_there(tmp_path) -> None:
    root = DataRoot(tmp_path)
    _write(root, "20200710094727", SAMPLE)
    # 파일이 1KB 를 넘어야 "있음"으로 센다 — 잘린 파일을 다시 받게 하려고
    wayback.company_tickers_path(root, "20200710094727").write_bytes(
        json.dumps({str(i): {"cik_str": i, "ticker": f"T{i}"} for i in range(200)}).encode()
    )

    class _Boom(wayback.WaybackClient):
        def get(self, url, *, params=None):  # noqa: ANN001
            raise AssertionError("이미 있는 것을 다시 받으면 안 된다")

    client = _Boom(user_agent="t")
    out = wayback.download_company_tickers(client, root, timestamps=["20200710094727"])
    assert out == {
        "available": 1,
        "fetched": 0,
        "skipped": 1,
        "failed": [],
        "dir": wayback.company_tickers_dir(root),
    }
