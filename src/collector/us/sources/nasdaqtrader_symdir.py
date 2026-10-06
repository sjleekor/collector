"""nasdaqtrader 심볼 디렉터리 — 현재 파일을 매일 받아 원문 그대로 쌓는다.

``listing_snapshots``는 Wayback이 저장한 옛 파일(``raw/wayback/symdir/``)로 만든다.
다시 받는 코드가 없어 목록이 멈췄다 (2026-10-06 확인). 이 모듈은 **1단계 —
원문만 쌓는다.** ``listing_snapshots``와 유니버스는 건드리지 않는다.

조건 (2026-10-06 결정, 근거 ``my/milestones/us/research/data/open_apis/06_nasdaq_trader.md``):

* **하루에 한 번만** 받는다 (KST 날짜 기준). ``force``로만 무시한다.
* **원문을 바꾸지 않는다.** 받은 바이트를 그대로 쓰고, 저작권 고지(``NOTICE``)를 같이 둔다.
* **원문·목록을 밖으로 다시 내보내지 않는다.** 개인·비상업 연구용 보관이다.

저장 이름은 ``<kind>_<UTC 받은 시각 YYYYMMDDHHMMSS>.txt``라 Wayback 원문과 같은
``ListingFile`` 규칙으로 읽힌다. 날짜 기록 파일(``LAST_FETCH.json``)과 ``NOTICE``는
``*.txt``가 아니라 빌더의 glob에 안 걸린다.

**공식 FTP(``ftp://ftp.nasdaqtrader.com/symboldirectory/``)로 받는다 (2026-10-06).**
웹 경로(HTTPS)는 같은 세션의 두 번째 요청이 Incapsula 봇 방어 HTML로 막혀 쓰지 않는다.
세션을 새로 만들어 피하는 방식은 봇 방어를 우회하는 것이라 택하지 않았다. 사이트의
심볼 디렉터리 안내 페이지가 가리키는 배포 경로가 FTP고, 크기·헤더·끝줄이 HTTPS 원문과
같았다. 익명 로그인, 패시브 모드, kind마다 새 연결이다.
"""

from __future__ import annotations

import datetime as dt
import ftplib
import hashlib
import io
import json
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from collector.lake import DataRoot
from collector.us.sources.wayback import WaybackParseError, parse_listing_file

FTP_HOST = "ftp.nasdaqtrader.com"
FTP_DIR = "symboldirectory"
BASE_URL = f"ftp://{FTP_HOST}/{FTP_DIR}"

#: kind -> (FTP 경로, 기대하는 첫 줄). 헤더가 다르면 형식이 바뀐 것이라 저장하지 않는다.
KINDS: dict[str, tuple[str, str]] = {
    "nasdaqlisted": (
        f"{BASE_URL}/nasdaqlisted.txt",
        "Symbol|Security Name|Market Category|Test Issue|Financial Status"
        "|Round Lot Size|ETF|NextShares",
    ),
    "otherlisted": (
        f"{BASE_URL}/otherlisted.txt",
        "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol",
    ),
}

#: 2026-10-06 실측이 5,636행·7,653행이다. 이 밑이면 잘린 파일이다.
MIN_ROWS = 1_000

#: 같은 호스트에 두 요청뿐이다.
DEFAULT_INTERVAL_SECONDS = 2.0
RETRIES = 3
RETRY_WAIT_SECONDS = 5.0

KST = ZoneInfo("Asia/Seoul")

NOTICE_NAME = "NOTICE"
LAST_FETCH_NAME = "LAST_FETCH.json"

NOTICE_TEXT = """\
nasdaqtrader 심볼 디렉터리 원문 보관본

원천: ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt
      ftp://ftp.nasdaqtrader.com/symboldirectory/otherlisted.txt
2026-10-06부터 FTP로 받는다 (웹 경로는 Incapsula 봇 방어로 같은 세션 재요청이 막혀 쓰지 않는다).

Copyright © 2021, The Nasdaq, Inc. All rights reserved.

이 디렉터리의 파일은 개인·비상업 연구용으로 원문을 바꾸지 않고 보관한다.
원문과 그 목록은 밖으로 다시 내보내지(재배포하지) 않는다.

결정일: 2026-10-06
근거: my/milestones/us/research/data/open_apis/06_nasdaq_trader.md
      (nasdaqtrader 사이트 약관 Copyright 조항)
"""


class SymdirError(RuntimeError):
    """받지 못했거나 받은 것이 기대한 형식이 아니다."""


@dataclass
class SymdirClient:
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS
    retries: int = RETRIES
    retry_wait_seconds: float = RETRY_WAIT_SECONDS
    timeout: float = 60.0
    ftp_factory: Callable[..., ftplib.FTP] = ftplib.FTP
    _last: float = field(default=0.0, init=False, repr=False)
    requests_made: int = field(default=0, init=False)

    def _wait(self, seconds: float) -> None:
        gap = time.monotonic() - self._last
        if self._last and gap < seconds:
            time.sleep(seconds - gap)

    def _retrieve(self, url: str) -> bytes:
        """연결을 새로 열어 한 파일을 받고 닫는다. 익명 로그인, 패시브(기본)."""
        parts = urlsplit(url)
        directory, _, name = parts.path.strip("/").rpartition("/")
        buf = io.BytesIO()
        ftp = self.ftp_factory(parts.hostname or FTP_HOST, timeout=self.timeout)
        try:
            ftp.login()
            if directory:
                ftp.cwd(directory)
            ftp.retrbinary(f"RETR {name}", buf.write)
        finally:
            try:
                ftp.quit()
            except (OSError, EOFError, ftplib.Error):
                ftp.close()
        return buf.getvalue()

    def get(self, url: str) -> bytes:
        """**바이트를 그대로 돌려준다.** 4xx 응답·네트워크 오류만 다시 시도한다."""
        last_exc: Exception | None = None
        for attempt in range(max(1, self.retries)):
            self._wait(self.interval_seconds if attempt == 0 else self.retry_wait_seconds)
            try:
                return self._retrieve(url)
            except ftplib.error_perm as exc:  # 5xx는 다시 해도 같다
                raise SymdirError(f"{url}: {exc}") from exc
            except (ftplib.error_temp, OSError, EOFError) as exc:  # timeout은 OSError
                last_exc = exc
            finally:
                self._last = time.monotonic()
                self.requests_made += 1
        raise SymdirError(f"{url}: {last_exc}") from last_exc


def symdir_dir(root: DataRoot) -> Path:
    """``raw/nasdaqtrader/symdir/``"""
    return root.raw / "nasdaqtrader" / "symdir"


def saved_files(root: DataRoot, kind: str) -> list[Path]:
    """그 kind로 저장한 원문. 이른 것부터."""
    return sorted(symdir_dir(root).glob(f"{kind}_*.txt"))


def _atomic_write(path: Path, data: bytes) -> None:
    tmp = path.with_name(f".{path.name}.part")
    try:
        tmp.write_bytes(data)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def ensure_notice(root: DataRoot) -> bool:
    """``NOTICE``가 없으면 만든다. 있으면 **덮지 않는다.** 만들었으면 ``True``."""
    path = symdir_dir(root) / NOTICE_NAME
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(path, NOTICE_TEXT.encode("utf-8"))
    return True


def _read_last_fetch(root: DataRoot) -> dict[str, str]:
    path = symdir_dir(root) / LAST_FETCH_NAME
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _write_last_fetch(root: DataRoot, kind: str, at: dt.datetime) -> None:
    state = _read_last_fetch(root)
    state[kind] = at.isoformat()
    _atomic_write(
        symdir_dir(root) / LAST_FETCH_NAME,
        (json.dumps(state, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )


def fetched_today(root: DataRoot, kind: str, *, now: dt.datetime) -> bool:
    """그 kind를 **오늘(KST)** 이미 받았나. 같은 파일이라 안 쓴 날도 센다."""
    raw = _read_last_fetch(root).get(kind)
    if not raw:
        return False
    try:
        at = dt.datetime.fromisoformat(raw)
    except ValueError:
        return False
    return at.astimezone(KST).date() == now.astimezone(KST).date()


def validate(kind: str, data: bytes, work: Path) -> dt.datetime:
    """받은 바이트를 검증하고 ``File Creation Time``(as_of)을 돌려준다.

    ``parse_listing_file``의 검증(gzip·헤더·생성 시각 줄)을 그대로 쓰려고
    ``work``에 임시로 쓴다. 이름이 ``*.txt``가 아니라 다른 것과 안 섞인다.
    """
    expected_header = KINDS[kind][1]
    first = data.split(b"\n", 1)[0].decode("latin-1").strip()
    if data[:2] in (b"\x1f\x8b", b"\x1f\xef"):
        raise SymdirError(f"{kind}: gzip 바이트다")
    if first != expected_header:
        raise SymdirError(f"{kind}: 헤더가 다르다 — {first[:80]!r}")
    work.write_bytes(data)
    try:
        as_of, rows = parse_listing_file(work)
    except WaybackParseError as exc:
        raise SymdirError(f"{kind}: {exc}") from exc
    finally:
        work.unlink(missing_ok=True)
    if len(rows) < MIN_ROWS:
        raise SymdirError(f"{kind}: {len(rows)}행뿐이다 (최소 {MIN_ROWS})")
    return as_of


def fetch_kind(
    client: SymdirClient,
    root: DataRoot,
    kind: str,
    *,
    now: dt.datetime | None = None,
    force: bool = False,
) -> dict[str, object]:
    """한 kind를 받아 검증하고 새 원문이면 저장한다.

    돌려주는 ``status``: ``written`` · ``unchanged``(생성 시각이나 sha가 같다) ·
    ``already_today``(오늘 이미 받아 요청도 안 했다). 실패는 ``SymdirError``다.
    """
    now = now or dt.datetime.now(dt.UTC)
    out = symdir_dir(root)
    if not force and fetched_today(root, kind, now=now):
        return {"kind": kind, "status": "already_today"}

    data = client.get(KINDS[kind][0])
    out.mkdir(parents=True, exist_ok=True)
    as_of = validate(kind, data, out / f".{kind}_check.part")

    prior = saved_files(root, kind)
    if prior:
        latest = prior[-1]
        same_sha = hashlib.sha256(latest.read_bytes()).digest() == hashlib.sha256(data).digest()
        try:
            same_time = parse_listing_file(latest)[0] == as_of
        except WaybackParseError:
            same_time = False
        if same_sha or same_time:
            ensure_notice(root)
            _write_last_fetch(root, kind, now)
            return {"kind": kind, "status": "unchanged", "as_of": as_of.isoformat()}

    path = out / f"{kind}_{now.astimezone(dt.UTC):%Y%m%d%H%M%S}.txt"
    ensure_notice(root)
    _atomic_write(path, data)
    _write_last_fetch(root, kind, now)
    return {
        "kind": kind,
        "status": "written",
        "path": str(path),
        "as_of": as_of.isoformat(),
        "bytes": len(data),
    }
