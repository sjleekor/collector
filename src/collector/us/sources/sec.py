"""SEC EDGAR — 연락처 UA가 없으면 403이다.

미국 계획 01 §1.1·§1.2, 05 §6, 06 §1.

* **UA에 연락처를 넣는다.** 없으면 403이고, 본문이
  ``Request Rate Threshold Exceeded``라 rate 초과로 읽히지만 **속도를 늦춰도
  안 풀린다.** 5분 간격 60회를 5시간 돌려도 403이었다.
* **403은 재시도하지 않는다.** 재시도가 푸는 종류의 실패가 아니다 —
  요청을 고쳐야 푼다. UA·헤더·경로를 먼저 본다.
* **rate limit이 호스트별이 아니다.** SEC가 밝힌 한도가 기계 수와 무관한
  사용자 단위 총합 10 req/s라 ``www``와 ``data``가 같은 버킷을 쓴다.
  limiter 하나로 묶고 운영값은 **5초(0.2 req/s)** — 한도의 1/50이다.
* **200이 성공이 아니다.** 403 HTML을 ``.zip``으로 저장한 것은 크기만
  봐서는 못 잡는다. ZIP은 매직 바이트와 entry 목록까지 연다.
"""

from __future__ import annotations

import os
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

import requests

BASE = "https://www.sec.gov"
DATA_BASE = "https://data.sec.gov"

#: 운영 간격. 한도(10 req/s)의 1/50이다 — 상한은 목표가 아니다.
DEFAULT_INTERVAL_SECONDS = 5.0


class SecAccessError(RuntimeError):
    """SEC가 거부했거나 받은 것이 기대한 형식이 아니다."""


def user_agent_from_env(env: dict[str, str] | None = None) -> str:
    """``SEC_USER_AGENT``를 읽는다. 기본값을 두지 않는다.

    이 저장소는 public이라 연락처가 코드에 박히면 공개된다. 값은 ``.env``에만
    둔다. (``Settings``에도 같은 이름 필드가 있는데 그쪽은 dotenv 소스가
    미선언 키에서 죽지 않게 하는 선언이다 — 미국 계획 X21.)
    """
    env = os.environ if env is None else env
    ua = env.get("SEC_USER_AGENT")
    if not ua:
        raise SecAccessError(
            "SEC_USER_AGENT가 없습니다. .env를 확인하십시오. "
            '형식: "<name>/<ver> (<contact email>)" — 연락처가 없으면 403입니다.'
        )
    return ua


def quarters(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    """``(연, 분기)`` 목록. 양끝을 포함한다."""
    out = []
    y, q = start
    while (y, q) <= end:
        out.append((y, q))
        y, q = (y + 1, 1) if q == 4 else (y, q + 1)
    return out


def financial_statements_url(year: int, quarter: int) -> str:
    """분기 재무 데이터셋. ``sub.txt``의 SIC가 **filing 시점 값이라 PIT다**."""
    return f"{BASE}/files/dera/data/financial-statement-data-sets/{year}q{quarter}.zip"


def insider_url(year: int, quarter: int) -> str:
    """내부자 거래 Form 3·4·5."""
    return (
        f"{BASE}/files/structureddata/data/insider-transactions-data-sets/"
        f"{year}q{quarter}_form345.zip"
    )


def midas_url(year: int, quarter: int) -> str:
    """MIDAS 일별 종목 지표. ``McapRank``가 일별 횡단면 decile이다.

    2012 Q1만 파일명이 ``q10``으로 규칙에서 벗어나는데 검정 구간 밖이다.
    """
    return (
        f"{BASE}/files/opa/data/market-structure/metrics-individual-security/"
        f"individual_security_{year}_q{quarter}.zip"
    )


COMPANY_TICKERS_URL = f"{BASE}/files/company_tickers.json"
COMPANYFACTS_BULK_URL = f"{BASE}/Archives/edgar/daily-index/xbrl/companyfacts.zip"
SUBMISSIONS_BULK_URL = f"{BASE}/Archives/edgar/daily-index/bulkdata/submissions.zip"


@dataclass
class SecClient:
    """SEC 요청 하나를 책임진다. 간격이 클라이언트 안에 있다 — 부르는 쪽이 잊어도 지켜진다."""

    user_agent: str
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS
    session: requests.Session = field(default_factory=requests.Session)
    _last_request_at: float = field(default=0.0, init=False, repr=False)

    def _wait(self) -> None:
        gap = time.monotonic() - self._last_request_at
        if self._last_request_at and gap < self.interval_seconds:
            time.sleep(self.interval_seconds - gap)

    def get(self, url: str, *, stream: bool = True) -> requests.Response:
        """한 번 요청한다. **403은 재시도하지 않는다.**"""
        self._wait()
        try:
            resp = self.session.get(
                url,
                headers={"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"},
                stream=stream,
                timeout=120,
            )
        finally:
            self._last_request_at = time.monotonic()
        if resp.status_code == 403:
            raise SecAccessError(
                f"403 {url}\n"
                "본문이 rate 초과라고 해도 속도 문제가 아닐 수 있다. "
                "UA에 연락처가 들어 있는지 먼저 본다 (01 §1.1). 재시도하지 않는다."
            )
        if resp.status_code != 200:
            raise SecAccessError(f"{resp.status_code} {url}")
        return resp

    def download(self, url: str, dest: Path) -> Path:
        """원문을 그대로 ``dest``에 굳힌다.

        **파싱 실패해도 원문이 남아야 재분석이 된다** (X1). 부분 파일을
        남기지 않으려고 ``.part``에 받아 다 받은 뒤 옮긴다.
        """
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_suffix(dest.suffix + ".part")
        resp = self.get(url)
        with part.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
        part.replace(dest)
        return dest


def assert_is_zip(path: Path) -> list[str]:
    """ZIP인지 **열어서** 본다. 403 HTML을 .zip으로 저장한 것을 여기서 잡는다."""
    if path.stat().st_size < 4:
        raise SecAccessError(f"{path}: 너무 작다 ({path.stat().st_size} B)")
    with path.open("rb") as fh:
        if fh.read(2) != b"PK":
            head = path.read_bytes()[:120]
            raise SecAccessError(f"{path}: ZIP 매직이 아니다. 앞부분: {head!r}")
    try:
        with zipfile.ZipFile(path) as zf:
            names = zf.namelist()
    except zipfile.BadZipFile as exc:
        raise SecAccessError(f"{path}: ZIP이 열리지 않는다 — {exc}") from exc
    if not names:
        raise SecAccessError(f"{path}: entry가 없다")
    return names
