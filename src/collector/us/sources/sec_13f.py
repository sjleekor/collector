"""SEC Form 13F 데이터셋 — 기관 보유(수급 계층 F19의 원천).

연구 [`02_sec_13f.md`](
../../../../../my/milestones/us/research/data/source_expansion/02_sec_13f.md).

* **목록 페이지를 파싱해야 한다.** ``sec_ftd.py``와 같은 이유다 — 파일 54개의
  URL을 규칙 하나로 계산할 수 있다는 보장이 없다(연구가 확인한 것은 파일
  이름 규칙뿐이지 URL 경로가 전부 같은 디렉터리인지는 아니다). 그래서 매번
  목록 페이지(``LIST_URL``)의 ``href``를 읽는다.
* **이름 규칙이 둘로 갈린다.** ``2013q2``\\~``2023q4``는 분기(``2018q4``),
  ``2024-01``부터는 **접수일 3개월 구간**(``01jun2026-31aug2026``)이다(연구 §3).
  둘 다 ``_form13f.zip``로 끝나므로 그 앞부분을 그대로 기간 태그로 쓴다 —
  분기인지 구간인지를 안 가른다.
* **파일 이름의 기간으로 행을 자르면 안 된다.** 파일 하나 안에 보고
  기준일(``PERIODOFREPORT``)이 25\\~100개 섞여 있다(연구 §4.2) — 행마다
  ``PERIODOFREPORT``로 묶어야 한다. ``FILING_DATE``가 PIT의 축이다.
* **정정(``13F-HR/A``)의 원본이 다른 파일에 있을 수 있다** — 2026년 파일의
  정정 354건 중 207건이 그렇다(연구 §4.3). 그래서 이 모듈의 :func:`extract_13f`는
  ``raw/``에 받아 둔 zip **전부**를 한 번에 읽어 ``(filer_cik, period_of_report)``
  로 정정을 푼다 — 반월마다 따로 맵을 만들면 파일 경계에서 못 잇는
  ``cusip_symbol_pit``와 같은 이유다(``sec_ftd.py``).
* **인코딩을 UTF-8로 단정하지 않는다.** FTD의 ``DESCRIPTION``에 latin-1
  바이트가 섞여 있던 것과 같은 걱정이 13F의 ``NAMEOFISSUER``(외국 발행사)에도
  있을 수 있다 — 실제 파일로 확인 못 했다. UTF-8을 먼저 시도하고 실패하면
  latin-1로 되돌아간다(``_extract_decoded``).
* **``VALUE``는 쓰지 않는다.** 단위(천 달러 -> 달러)가 도중에 바뀌는데
  경계를 못 쟀다(연구 §4.1). 주식 수(``SSHPRNAMT``)와 filer 수만 쓴다.
"""

from __future__ import annotations

import datetime as dt
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

from collector.lake import DataRoot
from collector.us.sources.sec import BASE, SecAccessError, SecClient, assert_is_zip

#: 목록 페이지. 54개(2026-09-22 실측) 반기/구간 zip의 href가 여기 있다(연구 §3).
LIST_URL = f"{BASE}/data-research/sec-markets-data/form-13f-data-sets"

#: ``href="...2018q4_form13f.zip"`` 류. 이름 앞부분만 뽑는다 — 분기든 구간이든
#: 신경 쓰지 않는다.
_HREF_RE = re.compile(r'href\s*=\s*(["\'])([^"\']*_form13f\.zip)\1', re.IGNORECASE)

#: href에서 ``_form13f.zip`` 앞부분을 기간 태그로 뽑는다.
_FILENAME_RE = re.compile(r"([0-9a-z]+(?:-[0-9a-z]+)?)_form13f\.zip$", re.IGNORECASE)

#: ``2013q2``\ ~\ ``2023q4`` 꼴.
_QUARTER_TAG_RE = re.compile(r"^\d{4}q[1-4]$", re.IGNORECASE)

#: ``01jun2026-31aug2026`` 꼴 (접수일 3개월 구간, 첫 구간만 2개월).
_RANGE_TAG_RE = re.compile(r"^\d{2}[a-z]{3}\d{4}-\d{2}[a-z]{3}\d{4}$", re.IGNORECASE)


class ThirteenFError(RuntimeError):
    """13F 목록·원문이 기대한 모양이 아니다."""


def is_valid_period_tag(tag: str) -> bool:
    """분기(``2018q4``) 또는 접수일 구간(``01jun2026-31aug2026``) 꼴인가."""
    return bool(_QUARTER_TAG_RE.match(tag) or _RANGE_TAG_RE.match(tag))


def _absolute_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"{BASE}{href}"
    raise ThirteenFError(f"상대 경로를 못 푼다: {href!r}")


@dataclass(frozen=True)
class ThirteenFListing:
    """목록 페이지 파싱 결과. ``duplicates``는 같은 기간에 링크가 둘 이상일 때다."""

    files: dict[str, str]  # period -> url
    duplicates: dict[str, list[str]]
    unparsed: list[str]  # _form13f.zip 을 담은 href인데 태그를 못 뽑거나 규칙과 다른 것


def parse_listing(html: str) -> ThirteenFListing:
    """목록 페이지 HTML에서 ``{period: url}``을 뽑는다.

    **파일 이름만 본다. 경로는 안 본다** — ``sec_ftd.parse_listing``과 같은
    태도다. 태그가 분기·구간 어느 규칙에도 안 맞으면 ``unparsed``로 보낸다.
    """
    hrefs = [m.group(2) for m in _HREF_RE.finditer(html)]
    if not hrefs:
        raise ThirteenFError(
            f"{LIST_URL}: _form13f.zip 링크를 하나도 못 찾았다 — 페이지 구조가 바뀌었나 본다."
        )

    files: dict[str, str] = {}
    duplicates: dict[str, list[str]] = {}
    unparsed: list[str] = []
    for href in hrefs:
        m = _FILENAME_RE.search(href)
        tag = m.group(1) if m else None
        if not tag or not is_valid_period_tag(tag):
            unparsed.append(href)
            continue
        url = _absolute_url(href)
        if tag in files and files[tag] != url:
            duplicates.setdefault(tag, [files[tag]]).append(url)
            continue  # 먼저 본 것을 그대로 쓴다 — 결정적이어야 한다
        files[tag] = url
    return ThirteenFListing(files=files, duplicates=duplicates, unparsed=unparsed)


def list_13f_files(client: SecClient) -> ThirteenFListing:
    """목록 페이지를 받아 파싱한다. 새 기간이 올라왔는지는 매번 이걸로 안다."""
    resp = client.get(LIST_URL, stream=False)
    return parse_listing(resp.text)


# --- raw ----------------------------------------------------------------


def thirteenf_raw_dir(root: DataRoot) -> Path:
    """``raw/sec/13f/`` — 원문 zip을 그대로 둔다(02 §1의 raw 원칙)."""
    return root.raw / "sec" / "13f"


def thirteenf_raw_path(root: DataRoot, period: str) -> Path:
    """``raw/sec/13f/<period>_form13f.zip``. 원문 파일 이름을 그대로 쓴다."""
    return thirteenf_raw_dir(root) / f"{period}_form13f.zip"


def download_13f(
    client: SecClient,
    root: DataRoot,
    period: str,
    url: str,
    *,
    skip_existing: bool = True,
) -> dict[str, object]:
    """기간 zip 하나를 ``raw/``에 굳힌다. **받자마자 열어 본다**(``sec.py``와 같은 이유)."""
    dest = thirteenf_raw_path(root, period)
    if skip_existing and dest.is_file():
        try:
            entries = assert_is_zip(dest)
            return {
                "period": period,
                "path": dest,
                "skipped": True,
                "entries": len(entries),
                "bytes": dest.stat().st_size,
            }
        except SecAccessError:
            dest.unlink()  # 깨진 것은 다시 받는다

    client.download(url, dest)
    entries = assert_is_zip(dest)
    return {
        "period": period,
        "path": dest,
        "skipped": False,
        "entries": len(entries),
        "bytes": dest.stat().st_size,
    }


def raw_periods(root: DataRoot) -> set[str]:
    """``raw/``에 이미 받아 둔 기간 태그. 이어받기 판단이 여기서 나온다."""
    directory = thirteenf_raw_dir(root)
    if not directory.is_dir():
        return set()
    out = set()
    for p in directory.glob("*_form13f.zip"):
        m = _FILENAME_RE.search(p.name)
        if m and is_valid_period_tag(m.group(1)):
            out.add(m.group(1))
    return out


# --- zip 멤버 -------------------------------------------------------------


def _find_member(names: list[str], wanted: str) -> str:
    """``names`` 중 ``wanted``와 같은 파일(경로 접두어는 무시)을 찾는다."""
    wanted_lower = wanted.lower()
    for n in names:
        base = n.rsplit("/", 1)[-1]
        if base.lower() == wanted_lower:
            return n
    raise ThirteenFError(f"zip 안에 {wanted}가 없다 — 있는 것: {names}")


def _extract_decoded(zip_path: Path, member: str, dest: Path) -> Path:
    """zip 멤버를 임시 파일로 푼다. **UTF-8로 안전하게** — FTD의 ``DESCRIPTION``과
    같은 걱정이 ``NAMEOFISSUER``(외국 발행사)에도 있을 수 있어서다(연구 §2 참고,
    실제 13F 파일로는 아직 확인 못 했다).

    UTF-8로 그대로 읽히면 바이트를 그대로 쓴다(대용량 파일에서 불필요한
    재인코딩을 피한다). 실패하면 latin-1로 디코딩한 뒤 UTF-8로 다시 인코딩해
    duckdb의 ``read_csv``(기본 UTF-8)가 읽을 수 있게 한다.
    """
    with zipfile.ZipFile(zip_path) as zf, zf.open(member) as fh:
        raw = fh.read()
    try:
        raw.decode("utf-8")
        dest.write_bytes(raw)
    except UnicodeDecodeError:
        dest.write_text(raw.decode("latin-1"), encoding="utf-8")
    return dest


# --- derived: thirteenf_submissions · inst_holdings_q -------------------------

#: SUBMISSION.tsv·INFOTABLE.tsv 의 날짜 칸이 이 꼴이다 (``31-OCT-2018``).
#: 내부자 거래 데이터셋(``sec.py``의 ``_INSIDER_DATE``)과 같다.
_DATE_FMT = "%d-%b-%Y"

#: 13F 제출의 PIT 컷오프 — ``filing_date - period_of_report``(달력일)가 이보다
#: 크면 ``inst_holdings_q``에서 뺀다. ``inst_holdings_q``는 ``(filer_cik,
#: period_of_report)``마다 filing_date가 가장 늦은 제출을 남기는데 제출 시점
#: 제한이 없어서, ① 몇 년 뒤에 낸 정정본이 과거 분기 값을 바꾸는 룩어헤드와
#: ② 옛 기준일에 뒤늦게 낸 제출 하나가 ``n_filers_total_that_period = 1``짜리
#: 이상치 행을 만드는 문제가 둘 다 실물 2018q4 zip으로 확인됐다. 그 파일의
#: 13F-HR ``filing_date - period_of_report``는 중앙값 44일 · p99 50일 · 최대
#: 74일(2026-09-28 실측) — 60일이면 p99를 덮고도 여유가 있다.
#: **사전등록이 닫히기 전 임시값이다** — 다른 기간으로 다시 재면 바뀔 수 있다.
LAG_13F_DAYS = 60

_TSV = "delim='\t', header=true, quote='', escape='', all_varchar=true"


def extract_13f(
    root: DataRoot,
    *,
    snapshot_date: dt.date | str,
    observed_at: dt.datetime | None = None,
    work_dir: Path | None = None,
) -> dict[str, object]:
    """``raw/sec/13f/``의 기간 zip들을 ``thirteenf_submissions``·``inst_holdings_q`` 로.

    한 로더가 표 둘을 낸다 — ``sec_ftd.extract_ftd``와 같은 모양이다
    (``ops/derive.py``의 ``Recipe.tables``).

    **규칙 (계획 20260927_us4_flow_features §5.3):**

    * ``13F-NT``\\/``13F-NT/A``는 뺀다 — 보유 표가 없다
    * **PIT 컷오프**: ``filing_date <= period_of_report + LAG_13F_DAYS``인
      제출만 쓴다 — 원본·정정 모두. 컷오프 밖 제출 수는 ``submissions_after_cutoff``에
      센다. ``thirteenf_submissions``는 이 컷오프 없이 전량 그대로 남긴다 —
      ``LAG_13F_DAYS`` 실측을 그 표에서 하므로 순환을 막는다
    * ``13F-HR/A``는 같은 ``(filer_cik, period_of_report)``의 앞 제출을
      **대체**한다 — 그 조합에서(컷오프 안 제출 중) ``filing_date``가 가장
      늦은 accession 하나만 남긴다. **전량을 한 번에 읽어야** 한다 — 정정의
      원본이 다른 파일에 있을 수 있다(연구 §4.3)
    * 남는 것 중 ``SSHPRNAMTTYPE = 'SH'``\\이고 ``PUTCALL``\\이 빈 행만 쓴다
      (옵션 보유를 주식 보유로 안 센다)
    * ``OTHERMANAGER``\\가 찬 행은 filer 수(``n_holders``)에는 세고, 주식 수
      합(``shares_total``)에서는 같은 ``(cusip, period_of_report, sshprnamt)``
      중복을 한 번만 센다 — 공동 보유 이중계상을 거칠게 걷어내는 것이다.
      정확한 위치식별자가 없어 생기는 한계다(연구 §4.2)
    * ``VALUE``는 안 쓴다 — 단위가 도중에 바뀐다(연구 §4.1)

    **부분 정정**(정정본 행 수가 원본의 절반 미만 — 연구 §4.3의 6\\~7%)은 규칙을
    안 바꾸고 그 수만 반환값의 ``partial_amendments``에 적는다.
    """
    import duckdb

    from collector.us.store.writer import snapshot_path, verify_snapshot

    observed_at = observed_at or dt.datetime.now(dt.UTC)
    work_dir = work_dir or (root.output / "_tmp" / "sec_13f")
    work_dir.mkdir(parents=True, exist_ok=True)
    # 앞 실행이 중간에 끊겼으면 조각이 남아 있다. 글롭으로 읽으므로 먼저 치운다.
    for stale in work_dir.glob("*.parquet"):
        stale.unlink()

    files = sorted(thirteenf_raw_dir(root).glob("*_form13f.zip"))
    if not files:
        raise ThirteenFError(
            f"{thirteenf_raw_dir(root)}에 받아 둔 zip이 없다. 먼저 받는다 (us-daily run)."
        )

    con = duckdb.connect()
    sub_parts: list[Path] = []
    info_parts: list[Path] = []
    periods_ok: list[str] = []
    periods_failed: list[str] = []

    for path in files:
        m = _FILENAME_RE.search(path.name)
        period = m.group(1) if m else path.stem
        try:
            names = assert_is_zip(path)
            sub_member = _find_member(names, "SUBMISSION.tsv")
            info_member = _find_member(names, "INFOTABLE.tsv")
            sub_tmp = _extract_decoded(path, sub_member, work_dir / f"{period}_submission.txt")
            info_tmp = _extract_decoded(path, info_member, work_dir / f"{period}_infotable.txt")
        except (SecAccessError, ThirteenFError) as exc:
            periods_failed.append(f"{period}: {exc}")
            continue

        sub_part = work_dir / f"{period}_submission.parquet"
        con.execute(
            f"""
            COPY (
                SELECT "ACCESSION_NUMBER"                                  AS accession,
                       try_strptime("FILING_DATE", '{_DATE_FMT}')::DATE    AS filing_date,
                       upper(trim("SUBMISSIONTYPE"))                      AS submission_type,
                       TRY_CAST("CIK" AS BIGINT)                          AS filer_cik,
                       try_strptime("PERIODOFREPORT", '{_DATE_FMT}')::DATE AS period_of_report,
                       CAST(? AS TIMESTAMP WITH TIME ZONE)                AS observed_at,
                       CAST(? AS VARCHAR)                                 AS source_rev
                FROM read_csv('{sub_tmp}', {_TSV})
            ) TO '{sub_part}' (FORMAT PARQUET)
            """,
            [observed_at, period],
        )

        info_part = work_dir / f"{period}_infotable.parquet"
        con.execute(
            f"""
            COPY (
                SELECT "ACCESSION_NUMBER"                       AS accession,
                       "CUSIP"                                  AS cusip,
                       TRY_CAST("SSHPRNAMT" AS DOUBLE)           AS sshprnamt,
                       upper(trim("SSHPRNAMTTYPE"))              AS sshprnamttype,
                       nullif(trim("PUTCALL"), '')               AS putcall,
                       nullif(trim("OTHERMANAGER"), '')          AS othermanager
                FROM read_csv('{info_tmp}', {_TSV})
            ) TO '{info_part}' (FORMAT PARQUET)
            """,
        )

        sub_tmp.unlink()
        info_tmp.unlink()
        sub_parts.append(sub_part)
        info_parts.append(info_part)
        periods_ok.append(period)

    if not sub_parts:
        raise ThirteenFError(
            "행을 하나도 못 썼다 — 받아 둔 zip을 전부 못 읽었다: " + "; ".join(periods_failed)
        )

    sub_glob = "read_parquet([" + ", ".join(f"'{p}'" for p in sub_parts) + "])"
    info_glob = "read_parquet([" + ", ".join(f"'{p}'" for p in info_parts) + "])"

    # accession 별 INFOTABLE 원본 행 수 (필터 전). thirteenf_submissions.n_rows 와
    # 부분 정정 판정(행 수 비율)이 둘 다 이걸 쓴다.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE n_rows_by_accession AS
        SELECT accession, count(*) AS n_rows FROM {info_glob} GROUP BY accession
        """
    )

    # 같은 accession이 파일 경계 문제로 두 번 들어오는 방어적 dedup.
    before_subs = con.execute(f"SELECT count(*) FROM {sub_glob}").fetchone()[0]
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE submissions_dedup AS
        SELECT * FROM {sub_glob}
        QUALIFY row_number() OVER (PARTITION BY accession ORDER BY source_rev) = 1
        """
    )
    dup_accessions = before_subs - con.execute(
        "SELECT count(*) FROM submissions_dedup"
    ).fetchone()[0]

    # --- thirteenf_submissions ---
    dest_sub = snapshot_path(root, "thirteenf_submissions", snapshot_date)
    dest_sub.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT s.accession, s.filing_date, s.submission_type, s.filer_cik,
                   s.period_of_report,
                   s.submission_type LIKE '%/A'  AS is_amendment,
                   COALESCE(n.n_rows, 0)         AS n_rows,
                   s.observed_at, s.source_rev
            FROM submissions_dedup s
            LEFT JOIN n_rows_by_accession n USING (accession)
        ) TO '{dest_sub}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    sub_stats = verify_snapshot(dest_sub, "thirteenf_submissions", unique_on=("accession",))

    # --- PIT 컷오프: filing_date - period_of_report > LAG_13F_DAYS 인 제출은 뺀다 ---
    # 13F-NT/13F-NT/A 는 여기서 이미 빠진다 — 보유 표가 없어 filer 수에도 못 낀다.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE hr_all AS
        SELECT *,
               filing_date > period_of_report + INTERVAL '{LAG_13F_DAYS} days' AS after_cutoff
        FROM submissions_dedup
        WHERE submission_type LIKE '13F-HR%'
        """
    )
    submissions_after_cutoff = con.execute(
        "SELECT count(*) FROM hr_all WHERE after_cutoff"
    ).fetchone()[0]

    # --- 정정 대체: 컷오프 안에서, 같은 (filer_cik, period_of_report)의
    # filing_date 최신만 ---
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE hr_ranked AS
        SELECT *,
               row_number() OVER (
                   PARTITION BY filer_cik, period_of_report
                   ORDER BY filing_date DESC, accession DESC
               ) AS rn_latest,
               row_number() OVER (
                   PARTITION BY filer_cik, period_of_report
                   ORDER BY filing_date ASC, accession ASC
               ) AS rn_first,
               count(*) OVER (PARTITION BY filer_cik, period_of_report) AS n_versions
        FROM hr_all
        WHERE NOT after_cutoff
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE chosen AS
        SELECT h.*, n.n_rows AS n_rows
        FROM hr_ranked h
        LEFT JOIN n_rows_by_accession n USING (accession)
        WHERE rn_latest = 1
        """
    )

    # 부분 정정 — 정정본이 있고(n_versions >= 2) 남긴 행 수가 원본의 절반 미만.
    # `hr_ranked`엔 n_rows가 없다(그 계산은 `chosen`에서만 join했다) — 원본 쪽도
    # 같은 방식으로 `n_rows_by_accession`을 다시 join한다.
    partial_amendments = con.execute(
        """
        SELECT count(*) FROM (
            SELECT c.n_versions,
                   c.n_rows                      AS kept_rows,
                   nf.n_rows                     AS original_rows
            FROM chosen c
            JOIN hr_ranked f
              ON f.filer_cik = c.filer_cik
             AND f.period_of_report = c.period_of_report
             AND f.rn_first = 1
            LEFT JOIN n_rows_by_accession nf ON nf.accession = f.accession
        )
        WHERE n_versions >= 2
          AND original_rows > 0
          AND kept_rows / CAST(original_rows AS DOUBLE) < 0.5
        """
    ).fetchone()[0]

    # 남긴 accession의 INFOTABLE 행 중 주식 보유만(SH·PUTCALL 빈 값·CUSIP 있음).
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE filtered AS
        SELECT i.cusip, c.period_of_report, c.filer_cik, i.sshprnamt, i.othermanager
        FROM {info_glob} i
        JOIN chosen c USING (accession)
        WHERE i.sshprnamttype = 'SH' AND i.putcall IS NULL AND i.cusip IS NOT NULL
        """
    )
    filtered_rows, othermanager_filled = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE othermanager IS NOT NULL) FROM filtered"
    ).fetchone()
    othermanager_share = (othermanager_filled / filtered_rows) if filtered_rows else 0.0

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE n_holders_tbl AS
        SELECT cusip, period_of_report, CAST(count(DISTINCT filer_cik) AS INTEGER) AS n_holders
        FROM filtered GROUP BY cusip, period_of_report
        """
    )
    # OTHERMANAGER 공동 보유 이중계상 방지 — (cusip, period, sshprnamt) 중복을 한 번만.
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE shares_dedup AS
        SELECT DISTINCT cusip, period_of_report, sshprnamt FROM filtered
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE shares_total_tbl AS
        SELECT cusip, period_of_report,
               CAST(round(sum(sshprnamt)) AS BIGINT) AS shares_total
        FROM shares_dedup GROUP BY cusip, period_of_report
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE n_filers_tbl AS
        SELECT period_of_report,
               CAST(count(DISTINCT filer_cik) AS INTEGER) AS n_filers_total_that_period
        FROM chosen GROUP BY period_of_report
        """
    )

    dest_holdings = snapshot_path(root, "inst_holdings_q", snapshot_date)
    dest_holdings.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT h.cusip, h.period_of_report, h.n_holders,
                   COALESCE(s.shares_total, 0)   AS shares_total,
                   f.n_filers_total_that_period,
                   CAST(? AS TIMESTAMP WITH TIME ZONE) AS observed_at,
                   CAST(? AS VARCHAR)                  AS source_rev
            FROM n_holders_tbl h
            LEFT JOIN shares_total_tbl s USING (cusip, period_of_report)
            LEFT JOIN n_filers_tbl f USING (period_of_report)
        ) TO '{dest_holdings}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """,
        [observed_at, f"13f:{len(periods_ok)}periods"],
    )
    holdings_stats = verify_snapshot(
        dest_holdings, "inst_holdings_q", unique_on=("cusip", "period_of_report")
    )

    return {
        "files": len(files),
        "periods_ok": periods_ok,
        "periods_failed": periods_failed,
        "duplicate_accessions_dropped": dup_accessions,
        "submissions_after_cutoff": submissions_after_cutoff,
        "partial_amendments": partial_amendments,
        "othermanager_filled_share": othermanager_share,
        "thirteenf_submissions": {"path": dest_sub, **sub_stats},
        "inst_holdings_q": {"path": dest_holdings, **holdings_stats},
    }
