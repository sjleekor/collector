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

from collector.lake import DataRoot

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


#: 목록 페이지가 규칙과 다른 경로로 거는 분기. **디렉터리가 바뀌는 중이다** —
#: 82개 링크 중 최신 분기 하나만 ``datastandardsinnovation``을 가리키고 나머지는
#: 전부 ``structureddata``다 (2026-09-19 실측). 규칙대로 만든 URL은 404다.
#: 새 분기가 또 404면 목록 페이지의 ``href``를 먼저 본다 — MIDAS와 같은 일이다.
INSIDER_URL_EXCEPTIONS: dict[tuple[int, int], str] = {
    (2026, 2): (
        f"{BASE}/files/datastandardsinnovation/data/insider-transactions-data-sets/"
        "2026q2_form345.zip"
    ),
}


def insider_url(year: int, quarter: int) -> str:
    """내부자 거래 Form 3·4·5. **규칙으로 만들되 예외표를 먼저 본다.**"""
    if (year, quarter) in INSIDER_URL_EXCEPTIONS:
        return INSIDER_URL_EXCEPTIONS[(year, quarter)]
    return (
        f"{BASE}/files/structureddata/data/insider-transactions-data-sets/"
        f"{year}q{quarter}_form345.zip"
    )


#: 목록 페이지가 규칙과 다른 경로로 거는 분기. SEC 쪽 발행 실수로 보인다 —
#: 58개 링크 중 2019 Q4 하나만 Drupal 내부 경로(``/files/node/add/...``)를
#: 가리킨다. 규칙대로 만든 URL은 404고 이쪽은 200이다 (2026-09-19 실측).
#: 2012 Q1의 ``q10`` 예외는 검정 구간 밖이라 여기 없다.
MIDAS_URL_EXCEPTIONS: dict[tuple[int, int], str] = {
    (2019, 4): f"{BASE}/files/node/add/data_distribution/individual_security_2019_q4.zip",
}


def midas_url(year: int, quarter: int) -> str:
    """MIDAS 일별 종목 지표. ``McapRank``가 일별 횡단면 decile이다.

    **규칙으로 만들되 예외표를 먼저 본다.** 404가 나면 목록 페이지
    (``/data-research/sec-markets-data/marketstructuredata-security``)의
    ``href``를 확인한다 — 파일이 없는 게 아니라 경로가 다를 수 있다.
    """
    if (year, quarter) in MIDAS_URL_EXCEPTIONS:
        return MIDAS_URL_EXCEPTIONS[(year, quarter)]
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

    def get(
        self,
        url: str,
        *,
        stream: bool = True,
        headers: dict[str, str] | None = None,
        ok: tuple[int, ...] = (200,),
    ) -> requests.Response:
        """한 번 요청한다. **403은 재시도하지 않는다.**

        ``headers``로 ``Range``를 넣으면 ``ok=(200, 206)``으로 받는다 — 서버가
        Range를 무시하고 200으로 전부 보낼 수 있어 부르는 쪽이 둘 다 다뤄야 한다.
        """
        self._wait()
        sent = {"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"}
        if headers:
            sent.update(headers)
        try:
            resp = self.session.get(url, headers=sent, stream=stream, timeout=120)
        finally:
            self._last_request_at = time.monotonic()
        if resp.status_code == 403:
            raise SecAccessError(
                f"403 {url}\n"
                "본문이 rate 초과라고 해도 속도 문제가 아닐 수 있다. "
                "UA에 연락처가 들어 있는지 먼저 본다 (01 §1.1). 재시도하지 않는다."
            )
        if resp.status_code not in ok:
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


#: 분기 ZIP 세 갈래. 값은 (연, 분기) -> URL 함수다.
QUARTERLY_KINDS = {
    "financial": financial_statements_url,
    "insider": insider_url,
    "midas": midas_url,
}


def quarterly_path(root: DataRoot, kind: str, year: int, quarter: int) -> Path:
    """``raw/sec/quarterly/<kind>/<YYYY>q<Q>.zip`` — 원문을 그대로 둔다 (02 §1)."""
    if kind not in QUARTERLY_KINDS:
        raise ValueError(f"모르는 갈래: {kind!r} (있는 것: {sorted(QUARTERLY_KINDS)})")
    return root.raw / "sec" / "quarterly" / kind / f"{year}q{quarter}.zip"


def download_quarterly(
    client: SecClient,
    root: DataRoot,
    kind: str,
    year: int,
    quarter: int,
    *,
    skip_existing: bool = True,
) -> dict[str, object]:
    """분기 ZIP 하나를 ``raw/``에 굳힌다. 이미 멀쩡하면 건너뛴다.

    **받자마자 열어 본다** — 403 HTML을 ``.zip``으로 저장한 것은 크기만
    봐서는 못 잡는다 (06 §1). 이어받기가 되는 이유도 이것이다: 열리는
    파일만 "받았다"로 친다.
    """
    dest = quarterly_path(root, kind, year, quarter)
    if skip_existing and dest.is_file():
        try:
            entries = assert_is_zip(dest)
            return {
                "path": dest,
                "skipped": True,
                "entries": len(entries),
                "bytes": dest.stat().st_size,
            }
        except SecAccessError:
            dest.unlink()  # 깨진 것은 다시 받는다

    client.download(QUARTERLY_KINDS[kind](year, quarter), dest)
    entries = assert_is_zip(dest)
    return {"path": dest, "skipped": False, "entries": len(entries), "bytes": dest.stat().st_size}


def _member_to_temp(zip_path: Path, member: str, dest: Path) -> Path:
    """ZIP 안의 멤버 하나를 임시 파일로 푼다."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf, zf.open(member) as src, dest.open("wb") as out:
        while chunk := src.read(1 << 20):
            out.write(chunk)
    return dest


def _midas_member(zip_path: Path, year: int, quarter: int) -> str:
    """MIDAS CSV 이름. ``q4_2018_all.csv`` 꼴인데 규칙을 믿지 않고 목록에서 고른다."""
    names = [n for n in assert_is_zip(zip_path) if n.lower().endswith(".csv")]
    if len(names) != 1:
        raise SecAccessError(f"{zip_path}: CSV가 하나가 아니다 — {names}")
    return names[0]


def extract_filings_sub(
    root: DataRoot,
    *,
    snapshot_date,
    observed_at=None,
    work_dir: Path | None = None,
) -> dict[str, object]:
    """분기 ZIP들의 ``sub.txt``를 ``filings_sub`` 한 장으로 (03 §4.7).

    **업종 PIT의 원천이다.** ``sic``이 filing 시점 값이고 ``filed``가 축이다.
    """
    import datetime as _dt

    import duckdb

    from collector.us.store.writer import snapshot_path, verify_snapshot

    observed_at = observed_at or _dt.datetime.now(_dt.UTC)
    work_dir = work_dir or (root.output / "_tmp" / "filings_sub")
    work_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    parts, missing = [], []

    for y, q in quarters((2018, 3), (2026, 2)):
        zp = quarterly_path(root, "financial", y, q)
        if not zp.is_file():
            missing.append(f"{y}q{q}")
            continue
        tmp = _member_to_temp(zp, "sub.txt", work_dir / f"{y}q{q}_sub.txt")
        part = work_dir / f"{y}q{q}.parquet"
        con.execute(
            f"""
            COPY (
                SELECT adsh,
                       CAST(cik AS BIGINT)                       AS cik,
                       name,
                       sic,                              -- 앞자리 0이 있다. 문자열로 둔다
                       form,
                       try_strptime(period,  '%Y%m%d')::DATE     AS period,
                       TRY_CAST(fy AS INTEGER)                   AS fy,
                       fp,
                       try_strptime(filed,   '%Y%m%d')::DATE     AS filed,
                       fye,
                       COALESCE(TRY_CAST(prevrpt AS INTEGER), 0) <> 0 AS prevrpt,
                       countryba,
                       former,
                       try_strptime(changed, '%Y%m%d')::DATE     AS changed,
                       CAST(? AS TIMESTAMP WITH TIME ZONE)       AS observed_at,
                       CAST(? AS VARCHAR)                        AS source_rev
                FROM read_csv('{tmp}', delim='\t', header=true, quote='',
                              escape='', all_varchar=true)
            ) TO '{part}' (FORMAT PARQUET)
            """,
            [observed_at, f"{y}q{q}"],
        )
        tmp.unlink()
        parts.append(part)

    if not parts:
        raise SecAccessError("분기 재무 ZIP이 하나도 없다. 먼저 받는다.")

    dest = snapshot_path(root, "filings_sub", snapshot_date)
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{work_dir}/*.parquet'))"
        f" TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    for part in parts:
        part.unlink()
    stats = verify_snapshot(dest, "filings_sub", unique_on=("adsh",))
    return {"path": dest, "quarters": len(parts), "missing": missing, **stats}


def extract_midas(
    root: DataRoot,
    *,
    snapshot_date,
    observed_at=None,
    work_dir: Path | None = None,
) -> dict[str, object]:
    """분기 MIDAS CSV들을 ``midas_security_daily`` 한 장으로 (03 §4.8).

    순위 넷만 뽑는다. rank 값이 연도마다 ``"1"``/``"1.0"``으로 와서 숫자로 파싱한다.
    """
    import datetime as _dt

    import duckdb

    from collector.us.store.writer import snapshot_path, verify_snapshot

    observed_at = observed_at or _dt.datetime.now(_dt.UTC)
    work_dir = work_dir or (root.output / "_tmp" / "midas")
    work_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    parts, missing = [], []

    for y, q in quarters((2018, 3), (2026, 2)):
        zp = quarterly_path(root, "midas", y, q)
        if not zp.is_file():
            missing.append(f"{y}q{q}")
            continue
        tmp = _member_to_temp(zp, _midas_member(zp, y, q), work_dir / f"{y}q{q}.csv")
        part = work_dir / f"{y}q{q}.parquet"
        con.execute(
            f"""
            COPY (
                SELECT try_strptime("Date", '%Y%m%d')::DATE            AS date,
                       "Ticker"                                        AS ticker,
                       "Security"                                      AS security_type,
                       -- "1" 과 "1.0" 이 섞여 온다. DOUBLE 로 받고 내린다.
                       CAST(TRY_CAST("McapRank"       AS DOUBLE) AS INTEGER) AS mcap_rank,
                       CAST(TRY_CAST("TurnRank"       AS DOUBLE) AS INTEGER) AS turn_rank,
                       CAST(TRY_CAST("VolatilityRank" AS DOUBLE) AS INTEGER) AS volatility_rank,
                       CAST(TRY_CAST("PriceRank"      AS DOUBLE) AS INTEGER) AS price_rank,
                       CAST(? AS TIMESTAMP WITH TIME ZONE)             AS observed_at,
                       CAST(? AS VARCHAR)                              AS source_rev
                FROM read_csv('{tmp}', header=true, all_varchar=true)
            ) TO '{part}' (FORMAT PARQUET)
            """,
            [observed_at, f"{y}q{q}"],
        )
        tmp.unlink()
        parts.append(part)

    if not parts:
        raise SecAccessError("MIDAS ZIP이 하나도 없다. 먼저 받는다.")

    dest = snapshot_path(root, "midas_security_daily", snapshot_date)
    dest.parent.mkdir(parents=True, exist_ok=True)

    # 원천에 (Date, Ticker) 중복이 있다. 두 모양이다 (2026-09-19 실측, 756조합):
    #   1. 완전히 같은 행이 2~3번 (457행)
    #   2. 한 행은 순위 넷이 다 차 있고 다른 행은 VolatilityRank 만 있고 나머지가 빈다
    # 채워진 것을 남긴다 — 비어 있는 쪽은 정보가 없다. 완전 동일이면 아무거나 같다.
    # 몇 행을 버렸는지 돌려주므로 조용히 사라지지 않는다.
    before = con.execute(
        f"SELECT count(*) FROM read_parquet('{work_dir}/*.parquet')"
    ).fetchone()[0]
    con.execute(
        f"""
        COPY (
            SELECT * EXCLUDE (rank_filled)
            FROM (
                SELECT *,
                       (mcap_rank IS NOT NULL)::INT + (turn_rank IS NOT NULL)::INT
                     + (volatility_rank IS NOT NULL)::INT + (price_rank IS NOT NULL)::INT
                       AS rank_filled
                FROM read_parquet('{work_dir}/*.parquet')
            )
            QUALIFY row_number() OVER (
                PARTITION BY date, ticker
                ORDER BY rank_filled DESC,
                         mcap_rank NULLS LAST, turn_rank NULLS LAST,
                         volatility_rank NULLS LAST, price_rank NULLS LAST
            ) = 1
        ) TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    for part in parts:
        part.unlink()
    stats = verify_snapshot(dest, "midas_security_daily", unique_on=("date", "ticker"))
    return {
        "path": dest,
        "quarters": len(parts),
        "missing": missing,
        "deduped_rows": before - int(stats["rows"]),
        **stats,
    }


#: 내부자 ZIP에서 쓰는 TSV. 나머지 여섯(FOOTNOTES·*_HOLDING·OWNER_SIGNATURE·
#: 메타·readme)은 원문에만 두고 뽑지 않는다.
INSIDER_MEMBERS = ("SUBMISSION.tsv", "NONDERIV_TRANS.tsv", "DERIV_TRANS.tsv", "REPORTINGOWNER.tsv")

#: 날짜가 ``31-OCT-2018`` 꼴이다. 분기 재무 데이터셋(``20181031``)과 다르다.
_INSIDER_DATE = "%d-%b-%Y"

_TSV = "delim='\t', header=true, quote='', escape='', all_varchar=true"


def _insider_trans_select(tmp: Path, *, deriv: bool) -> str:
    """거래 TSV 한 장을 공통 컬럼으로 맞춘다. 파생 전용 셋은 비파생에서 null이다."""
    sk = "DERIV_TRANS_SK" if deriv else "NONDERIV_TRANS_SK"
    if deriv:
        extra = f"""
                   TRY_CAST("CONV_EXERCISE_PRICE" AS DOUBLE)      AS conv_exercise_price,
                   TRY_CAST("UNDLYNG_SEC_SHARES"  AS DOUBLE)      AS underlying_shares,
                   try_strptime("EXPIRATION_DATE", '{_INSIDER_DATE}')::DATE AS expiration_date,"""
    else:
        extra = """
                   CAST(NULL AS DOUBLE)                           AS conv_exercise_price,
                   CAST(NULL AS DOUBLE)                           AS underlying_shares,
                   CAST(NULL AS DATE)                             AS expiration_date,"""
    return f"""
            SELECT "ACCESSION_NUMBER"                             AS accession,
                   {str(deriv).upper()}                           AS is_derivative,
                   TRY_CAST("{sk}" AS BIGINT)                     AS trans_sk,
                   "SECURITY_TITLE"                               AS security_title,
                   try_strptime("TRANS_DATE", '{_INSIDER_DATE}')::DATE AS trans_date,
                   "TRANS_CODE"                                   AS trans_code,
                   "TRANS_FORM_TYPE"                              AS trans_form_type,
                   TRY_CAST("EQUITY_SWAP_INVOLVED" AS INTEGER) <> 0 AS equity_swap_involved,
                   TRY_CAST("TRANS_SHARES"         AS DOUBLE)     AS trans_shares,
                   TRY_CAST("TRANS_PRICEPERSHARE"  AS DOUBLE)     AS trans_pricepershare,
                   "TRANS_ACQUIRED_DISP_CD"                       AS acquired_disposed,
                   TRY_CAST("SHRS_OWND_FOLWNG_TRANS" AS DOUBLE)   AS shares_owned_following,
                   "DIRECT_INDIRECT_OWNERSHIP"                    AS direct_indirect,{extra}
            FROM read_csv('{tmp}', {_TSV})
    """


def extract_insider(
    root: DataRoot,
    *,
    snapshot_date,
    observed_at=None,
    work_dir: Path | None = None,
) -> dict[str, object]:
    """분기 내부자 ZIP들을 ``insider_trans``·``insider_owners`` 두 장으로 (03 §4.10).

    **`filing_date`가 PIT의 축이다.** 거래일이 아니라 공시일 기준으로 붙인다 —
    Form 4는 거래 2영업일 안이지만 Form 5는 회계연도 뒤 45일까지 늦다.
    """
    import datetime as _dt

    import duckdb

    from collector.us.store.writer import snapshot_path, verify_snapshot

    observed_at = observed_at or _dt.datetime.now(_dt.UTC)
    work_dir = work_dir or (root.output / "_tmp" / "insider")
    work_dir.mkdir(parents=True, exist_ok=True)
    # 앞 실행이 중간에 끊겼으면 조각이 남아 있다. 글롭으로 읽으므로 먼저 치운다
    for stale in work_dir.glob("*.parquet"):
        stale.unlink()
    con = duckdb.connect()
    trans_parts, owner_parts, missing = [], [], []
    unmatched = 0

    for y, q in quarters((2018, 3), (2026, 2)):
        zp = quarterly_path(root, "insider", y, q)
        if not zp.is_file():
            missing.append(f"{y}q{q}")
            continue
        tag = f"{y}q{q}"
        tmp = {
            m: _member_to_temp(zp, m, work_dir / f"{tag}_{m}") for m in INSIDER_MEMBERS
        }

        trans_part = work_dir / f"{tag}_trans.parquet"
        con.execute(
            f"""
            COPY (
                SELECT t.accession,
                       t.is_derivative,
                       t.trans_sk,
                       TRY_CAST(s."ISSUERCIK" AS BIGINT)                  AS issuer_cik,
                       s."ISSUERTRADINGSYMBOL"                            AS issuer_symbol,
                       s."DOCUMENT_TYPE"                                  AS doc_type,
                       try_strptime(s."FILING_DATE", '{_INSIDER_DATE}')::DATE AS filing_date,
                       try_strptime(s."PERIOD_OF_REPORT", '{_INSIDER_DATE}')::DATE
                           AS period_of_report,
                       t.security_title, t.trans_date, t.trans_code, t.trans_form_type,
                       t.equity_swap_involved, t.trans_shares, t.trans_pricepershare,
                       t.acquired_disposed, t.shares_owned_following, t.direct_indirect,
                       t.conv_exercise_price, t.underlying_shares, t.expiration_date,
                       CAST(? AS TIMESTAMP WITH TIME ZONE)                AS observed_at,
                       CAST(? AS VARCHAR)                                 AS source_rev
                FROM (
                    {_insider_trans_select(tmp['NONDERIV_TRANS.tsv'], deriv=False)}
                    UNION ALL BY NAME
                    {_insider_trans_select(tmp['DERIV_TRANS.tsv'], deriv=True)}
                ) t
                -- LEFT다. 공시 메타가 없는 거래를 조용히 버리지 않는다 — 몇 건인지 센다
                LEFT JOIN read_csv('{tmp["SUBMISSION.tsv"]}', {_TSV}) s
                       ON s."ACCESSION_NUMBER" = t.accession
            ) TO '{trans_part}' (FORMAT PARQUET)
            """,
            [observed_at, tag],
        )
        unmatched += con.execute(
            f"SELECT count(*) FROM read_parquet('{trans_part}') WHERE issuer_cik IS NULL"
        ).fetchone()[0]

        owner_part = work_dir / f"{tag}_owners.parquet"
        rel = 'upper("RPTOWNER_RELATIONSHIP")'
        con.execute(
            f"""
            COPY (
                SELECT "ACCESSION_NUMBER"                  AS accession,
                       TRY_CAST("RPTOWNERCIK" AS BIGINT)   AS owner_cik,
                       "RPTOWNERNAME"                      AS owner_name,
                       "RPTOWNER_RELATIONSHIP"             AS relationship,
                       -- 쉼표로 끊긴 것과 붙어 온 것이 섞여 있다("DirectorOther").
                       -- 부분 문자열로 본다 — 넷 중 어느 것도 서로의 부분이 아니다.
                       {rel} LIKE '%DIRECTOR%'             AS is_director,
                       {rel} LIKE '%OFFICER%'              AS is_officer,
                       {rel} LIKE '%TENPERCENTOWNER%'      AS is_ten_percent_owner,
                       {rel} LIKE '%OTHER%'                AS is_other,
                       "RPTOWNER_TITLE"                    AS officer_title,
                       CAST(? AS TIMESTAMP WITH TIME ZONE) AS observed_at,
                       CAST(? AS VARCHAR)                  AS source_rev
                FROM read_csv('{tmp["REPORTINGOWNER.tsv"]}', {_TSV})
            ) TO '{owner_part}' (FORMAT PARQUET)
            """,
            [observed_at, tag],
        )

        for path in tmp.values():
            path.unlink()
        trans_parts.append(trans_part)
        owner_parts.append(owner_part)

    if not trans_parts:
        raise SecAccessError("내부자 ZIP이 하나도 없다. 먼저 받는다.")

    out: dict[str, object] = {
        "quarters": len(trans_parts),
        "missing": missing,
        "trans_without_submission": unmatched,
    }
    for table, parts, unique_on, dedup in (
        ("insider_trans", trans_parts, ("accession", "is_derivative", "trans_sk"), False),
        # 같은 공시에 같은 신고인이 두 번 오는 일이 있다(원천 중복). 하나만 남긴다
        ("insider_owners", owner_parts, ("accession", "owner_cik"), True),
    ):
        dest = snapshot_path(root, table, snapshot_date)
        dest.parent.mkdir(parents=True, exist_ok=True)
        src = "read_parquet([" + ", ".join(f"'{p}'" for p in parts) + "])"
        body = f"SELECT * FROM {src}"
        if dedup:
            body += (
                " QUALIFY row_number() OVER"
                " (PARTITION BY accession, owner_cik ORDER BY source_rev) = 1"
            )
        before = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
        con.execute(f"COPY ({body}) TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        stats = verify_snapshot(dest, table, unique_on=unique_on)
        out[table] = {"path": dest, "deduped_rows": before - int(stats["rows"]), **stats}

    for part in trans_parts + owner_parts:
        part.unlink()
    return out


#: 벌크 ZIP 둘. CIK별 1만 2천 요청을 2요청으로 바꾼다 (D5).
BULK_URLS = {
    "companyfacts": COMPANYFACTS_BULK_URL,
    "submissions": SUBMISSIONS_BULK_URL,
}


def bulk_path(root: DataRoot, kind: str) -> Path:
    """``raw/sec/bulk/<kind>.zip`` (02 §1)."""
    if kind not in BULK_URLS:
        raise ValueError(f"모르는 갈래: {kind!r} (있는 것: {sorted(BULK_URLS)})")
    return root.raw / "sec" / "bulk" / f"{kind}.zip"


def download_bulk(
    client: SecClient,
    root: DataRoot,
    kind: str,
    *,
    budget_seconds: float | None = None,
    skip_existing: bool = True,
) -> dict[str, object]:
    """벌크 ZIP 하나를 ``raw/``에 굳힌다. **끊기면 이어받는다.**

    1.4GB·1.6GB라 한 번에 다 받다가 끊기면 처음부터다. SEC가
    ``Accept-Ranges: bytes``를 주므로 ``.part``의 크기만큼 건너뛰고 붙인다
    (2026-09-19 실측). ``budget_seconds``를 주면 그 시간이 지날 때 멈추고
    ``done=False``로 돌아온다 — 다음 호출이 이어서 받는다.

    **서버가 Range를 무시하면 처음부터 다시 받는다.** 206이 아니라 200이 오면
    받은 것을 버리고 새로 쓴다. 안 그러면 파일 가운데가 겹쳐 깨진다.
    """
    dest = bulk_path(root, kind)
    if skip_existing and dest.is_file():
        try:
            entries = assert_is_zip(dest)
            return {
                "path": dest,
                "done": True,
                "skipped": True,
                "entries": len(entries),
                "bytes": dest.stat().st_size,
            }
        except SecAccessError:
            dest.unlink()

    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    have = part.stat().st_size if part.is_file() else 0
    resp = client.get(
        BULK_URLS[kind],
        headers={"Range": f"bytes={have}-"} if have else None,
        ok=(200, 206),
    )
    if have and resp.status_code != 206:
        have = 0  # Range를 무시했다. 이어붙이면 깨진다
    total = resp.headers.get("Content-Range", "").rsplit("/", 1)[-1]
    total = int(total) if total.isdigit() else int(resp.headers.get("Content-Length", 0)) + have

    started = time.monotonic()
    out_of_time = False
    with part.open("ab" if have else "wb") as fh:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            fh.write(chunk)
            have += len(chunk)
            if budget_seconds is not None and time.monotonic() - started > budget_seconds:
                out_of_time = True
                break
    resp.close()
    if out_of_time and have < total:
        return {"path": part, "done": False, "skipped": False, "bytes": have, "total": total}

    part.replace(dest)
    entries = assert_is_zip(dest)
    return {
        "path": dest,
        "done": True,
        "skipped": False,
        "entries": len(entries),
        "bytes": dest.stat().st_size,
    }
