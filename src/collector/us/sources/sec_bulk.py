"""SEC 벌크 ZIP 둘을 parquet으로 — CIK별 1만 2천 요청을 2요청으로 (D5).

미국 계획 03 §4.3·§4.11·§4.12, 04 C6.

* **``companyfacts.zip``이 정정 이력을 통째로 준다.** 같은 ``end``에 값이 여럿
  들어 있고 ``filed``가 갈라 준다 — 최신 1벌만 있으면 어느 시점 기준이든
  복원된다 (D8). 그래서 주 단위로 쌓지 않는다.
* **``submissions.zip``의 ``filings.recent``는 1,000건에서 끊긴다.** 나머지는
  ``CIK##########-submissions-001.json`` 같은 넘침 파일에 있고 같은 ZIP 안에
  들어 있다. 활발한 발행사는 Form 4만으로 1,000건을 몇 해에 채우므로
  **넘침을 안 읽으면 8년 구간이 통째로 빈다.**
* **표준 라이브러리 ``json``을 쓴다.** ``orjson``이 환경에 있지만 선언된
  의존이 아니다 — 미선언 의존에 기대면 다른 데서 실행이 안 된다.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import zipfile
from collections.abc import Iterable, Iterator
from pathlib import Path

import pyarrow as pyar
import pyarrow.parquet as pq

from collector.lake import DataRoot
from collector.us.sources.sec import bulk_path
from collector.us.store.schema import ARROW_SCHEMAS
from collector.us.store.writer import snapshot_path, staged_snapshot, verify_snapshot

#: 벌크 안의 CIK별 파일 이름. 넘침 파일은 이 꼴이 아니다.
_MAIN = re.compile(r"CIK(\d{10})\.json$")
_OVERFLOW = re.compile(r"CIK(\d{10})-submissions-\d+\.json$")


def source_rev(path: Path) -> str:
    """``<파일>:<바이트>``. **크기가 원천의 변경 신호다** (06 §7)."""
    return f"{path.name}:{path.stat().st_size}"


def _to_date(values: list[str | None]) -> pyar.Array:
    """빈 문자열을 null로 두고 date32로 캐스팅한다 — 파이썬 루프를 안 돈다."""
    return pyar.array([v or None for v in values], type=pyar.string()).cast(pyar.date32())


class _Sink:
    """행 묶음을 parquet에 곧장 흘린다 — 조각 파일을 거치지 않는다.

    ``dest``는 **임시 경로**다. 호출하는 쪽이 :func:`staged_snapshot`으로 감싸
    검증이 끝난 뒤에만 최종 경로로 옮긴다.

    1억 2천만 행을 조각으로 썼다가 다시 이어 붙이면 같은 양을 두 번 읽고 두 번
    쓴다. 한 번에 다 메모리에 올릴 수도 없으므로 ``ParquetWriter``를 열어 두고
    묶음마다 밀어 넣는다.
    """

    def __init__(self, dest: Path, table: str) -> None:
        self.schema = ARROW_SCHEMAS[table]
        self.table = table
        self.dest = dest
        dest.parent.mkdir(parents=True, exist_ok=True)
        self._writer = pq.ParquetWriter(dest, self.schema, compression="zstd")
        self.rows = 0

    def write(self, columns: dict[str, pyar.Array]) -> None:
        batch = pyar.table(columns).select(self.schema.names).cast(self.schema)
        self.rows += batch.num_rows
        self._writer.write_table(batch)

    def close(self, *, unique_on: tuple[str, ...] | None = None) -> dict[str, object]:
        self._writer.close()
        return verify_snapshot(self.dest, self.table, unique_on=unique_on)


# --- companyfacts -> fundamentals -------------------------------------------


def load_companyfacts(
    root: DataRoot,
    *,
    snapshot_date: dt.date | str,
    observed_at: dt.datetime | None = None,
    ciks: Iterable[int] | None = None,
    rows_per_part: int = 2_000_000,
) -> dict[str, object]:
    """``companyfacts.zip`` → ``fundamentals`` (03 §4.3).

    **CIK를 안 거른다** (``ciks=None`). 지금 있는 CIK 목록은 전부
    ``company_tickers.json``(현재 매핑)에서 나와 상폐·개명 발행사가 빠져 있다
    (X12). 거기에 맞춰 거르면 그 편향이 재무 데이터로 옮겨 온다. 2만 378개
    발행사 전부라도 parquet 몇 GB다.
    """
    dest = snapshot_path(root, "fundamentals", snapshot_date)
    with staged_snapshot(dest) as out:
        result = _build_fundamentals(
            root,
            out=out,
            observed_at=observed_at,
            ciks=ciks,
            rows_per_part=rows_per_part,
        )
    result["path"] = dest
    return result


def _build_fundamentals(
    root: DataRoot,
    *,
    out: Path,
    observed_at: dt.datetime | None,
    ciks: Iterable[int] | None,
    rows_per_part: int,
) -> dict[str, object]:
    """``load_companyfacts``의 본체. ``out``(임시 경로)에 쓰고 검증까지 한다."""
    observed_at = observed_at or dt.datetime.now(dt.UTC)
    zp = bulk_path(root, "companyfacts")
    rev = source_rev(zp)
    keep = set(ciks) if ciks is not None else None
    cols: dict[str, list] = {
        k: []
        for k in ("cik", "taxonomy", "tag", "unit", "start", "end",
                  "val", "fy", "fp", "form", "filed", "accn", "frame")
    }
    entities = 0
    skipped_nonnumeric = 0
    sink = _Sink(out, "fundamentals")

    def flush() -> None:
        if not cols["cik"]:
            return
        sink.write(
            {
                "cik": pyar.array(cols["cik"], type=pyar.int64()),
                "taxonomy": pyar.array(cols["taxonomy"], type=pyar.string()),
                "tag": pyar.array(cols["tag"], type=pyar.string()),
                "unit": pyar.array(cols["unit"], type=pyar.string()),
                "start": _to_date(cols["start"]),
                "end": _to_date(cols["end"]),
                "val": pyar.array(cols["val"], type=pyar.float64()),
                "fy": pyar.array(cols["fy"], type=pyar.int32()),
                "fp": pyar.array(cols["fp"], type=pyar.string()),
                "form": pyar.array(cols["form"], type=pyar.string()),
                "filed": _to_date(cols["filed"]),
                "accn": pyar.array(cols["accn"], type=pyar.string()),
                "frame": pyar.array(cols["frame"], type=pyar.string()),
                "observed_at": pyar.array(
                    [observed_at] * len(cols["cik"]), type=pyar.timestamp("us", tz="UTC")
                ),
            }
        )
        for v in cols.values():
            v.clear()

    with zipfile.ZipFile(zp) as zf:
        for name in zf.namelist():
            m = _MAIN.match(name)
            if not m:
                continue
            cik = int(m.group(1))
            if keep is not None and cik not in keep:
                continue
            doc = json.loads(zf.read(name))
            entities += 1
            for taxonomy, tags in (doc.get("facts") or {}).items():
                for tag, body in tags.items():
                    for unit, facts in (body.get("units") or {}).items():
                        for f in facts:
                            val = f.get("val")
                            if not isinstance(val, int | float) or isinstance(val, bool):
                                skipped_nonnumeric += 1
                                continue
                            cols["cik"].append(cik)
                            cols["taxonomy"].append(taxonomy)
                            cols["tag"].append(tag)
                            cols["unit"].append(unit)
                            cols["start"].append(f.get("start"))
                            cols["end"].append(f.get("end"))
                            cols["val"].append(float(val))
                            cols["fy"].append(f.get("fy"))
                            cols["fp"].append(f.get("fp"))
                            cols["form"].append(f.get("form"))
                            cols["filed"].append(f.get("filed"))
                            cols["accn"].append(f.get("accn"))
                            cols["frame"].append(f.get("frame"))
            if len(cols["cik"]) >= rows_per_part:
                flush()
    flush()

    stats = sink.close()
    return {
        "path": out,
        "entities": entities,
        "skipped_nonnumeric": skipped_nonnumeric,
        "source_rev": rev,
        **stats,
    }


# --- submissions -> filings_index · company_meta -----------------------------


def _filing_rows(block: dict[str, list]) -> Iterator[tuple]:
    """열 방향으로 온 것을 행으로 돌린다. 길이가 다르면 짧은 쪽에서 끊긴다."""
    n = len(block.get("accessionNumber") or [])
    get = lambda k, i: (block.get(k) or [None] * n)[i]  # noqa: E731
    for i in range(n):
        yield (
            get("accessionNumber", i), get("form", i), get("filingDate", i),
            get("reportDate", i), get("acceptanceDateTime", i), get("act", i),
            get("fileNumber", i), get("items", i), get("core_type", i),
            get("primaryDocument", i), get("isXBRL", i), get("isInlineXBRL", i),
            get("size", i),
        )


def load_submissions(
    root: DataRoot,
    *,
    snapshot_date: dt.date | str,
    observed_at: dt.datetime | None = None,
    ciks: Iterable[int] | None = None,
    since: dt.date | None = dt.date(2018, 1, 1),
    rows_per_part: int = 2_000_000,
) -> dict[str, object]:
    """``submissions.zip`` → ``filings_index``·``company_meta`` (03 §4.11·§4.12).

    **여기서는 CIK를 거른다.** 벌크에 98만 5천 CIK가 들어 있는데 대부분이
    Form 4를 내는 개인이다. ``ciks``에 발행사만 준다. ``since``보다 오래된
    공시는 버린다 — 검정 구간이 2018-07부터다.
    """
    dest = snapshot_path(root, "filings_index", snapshot_date)
    meta_dest = snapshot_path(root, "company_meta", snapshot_date)
    # 표 둘을 임시에 쓰고 검증한 뒤 옮긴다. 먼저 쓴 ``with`` 항목이 나중에 옮겨지므로
    # ``filings_index``(``needs_rebuild``가 보는 앞 표)가 **마지막에** 자리를 잡는다.
    # 검증이 실패하면 둘 다 안 생긴다 — 이전 스냅샷이 그대로 최신이고 다음 실행이 다시 한다
    with staged_snapshot(dest) as out, staged_snapshot(meta_dest) as meta_out:
        result = _build_submissions(
            root,
            out=out,
            meta_out=meta_out,
            observed_at=observed_at,
            ciks=ciks,
            since=since,
            rows_per_part=rows_per_part,
        )
    result["filings_index"]["path"] = dest  # type: ignore[index]
    result["company_meta"]["path"] = meta_dest  # type: ignore[index]
    return result


def _build_submissions(
    root: DataRoot,
    *,
    out: Path,
    meta_out: Path,
    observed_at: dt.datetime | None,
    ciks: Iterable[int] | None,
    since: dt.date | None,
    rows_per_part: int,
) -> dict[str, object]:
    """``load_submissions``의 본체. ``out``·``meta_out``(임시 경로)에 쓰고 검증까지 한다."""
    observed_at = observed_at or dt.datetime.now(dt.UTC)
    zp = bulk_path(root, "submissions")
    rev = source_rev(zp)
    keep = set(ciks) if ciks is not None else None
    since_s = since.isoformat() if since else None
    fil: dict[str, list] = {
        k: []
        for k in ("cik", "accession", "form", "filing_date", "report_date",
                  "acceptance_datetime", "act", "file_number", "items", "core_type",
                  "primary_document", "is_xbrl", "is_inline_xbrl", "size")
    }
    meta_rows: list[tuple] = []
    entities = 0
    duplicate_rows = 0
    conflicting_rows = 0
    sink = _Sink(out, "filings_index")

    def flush() -> None:
        if not fil["cik"]:
            return
        sink.write(
            {
                "cik": pyar.array(fil["cik"], type=pyar.int64()),
                "accession": pyar.array(fil["accession"], type=pyar.string()),
                "form": pyar.array(fil["form"], type=pyar.string()),
                "filing_date": _to_date(fil["filing_date"]),
                "report_date": _to_date(fil["report_date"]),
                # ``2026-09-17T22:30:24.000Z`` — 접수 시각이다. 장 마감 뒤 접수면
                # 그날 종가에 못 쓴다. 날짜만 남기면 그 판단을 못 한다
                "acceptance_datetime": pyar.array(
                    [v or None for v in fil["acceptance_datetime"]], type=pyar.string()
                ).cast(pyar.timestamp("us", tz="UTC")),
                "act": pyar.array(fil["act"], type=pyar.string()),
                "file_number": pyar.array(fil["file_number"], type=pyar.string()),
                "items": pyar.array(fil["items"], type=pyar.string()),
                "core_type": pyar.array(fil["core_type"], type=pyar.string()),
                "primary_document": pyar.array(fil["primary_document"], type=pyar.string()),
                "is_xbrl": pyar.array(
                    [None if v is None else bool(v) for v in fil["is_xbrl"]], type=pyar.bool_()
                ),
                "is_inline_xbrl": pyar.array(
                    [None if v is None else bool(v) for v in fil["is_inline_xbrl"]],
                    type=pyar.bool_(),
                ),
                "size": pyar.array(fil["size"], type=pyar.int64()),
                "observed_at": pyar.array(
                    [observed_at] * len(fil["cik"]), type=pyar.timestamp("us", tz="UTC")
                ),
                "source_rev": pyar.array([rev] * len(fil["cik"]), type=pyar.string()),
            }
        )
        for v in fil.values():
            v.clear()

    with zipfile.ZipFile(zp) as zf:
        names = zf.namelist()
        overflow: dict[int, list[str]] = {}
        for name in names:
            m = _OVERFLOW.search(name)
            if m:
                overflow.setdefault(int(m.group(1)), []).append(name)

        for name in names:
            m = _MAIN.match(name)
            if not m:
                continue
            cik = int(m.group(1))
            if keep is not None and cik not in keep:
                continue
            doc = json.loads(zf.read(name))
            entities += 1
            former = doc.get("formerNames") or []
            meta_rows.append((
                cik,
                doc.get("name"),
                doc.get("entityType"),
                doc.get("sic") or None,
                doc.get("sicDescription") or None,
                doc.get("category") or None,
                doc.get("fiscalYearEnd") or None,
                doc.get("stateOfIncorporation") or None,
                doc.get("ein") or None,
                # 목록에 null이 섞여 온다 — 티커는 있는데 거래소가 비는 발행사다
                ",".join(v for v in (doc.get("tickers") or []) if v) or None,
                ",".join(v for v in (doc.get("exchanges") or []) if v) or None,
                json.dumps(former, ensure_ascii=False) if former else None,
            ))

            blocks = [((doc.get("filings") or {}).get("recent") or {})]
            for extra in overflow.get(cik, []):
                blocks.append(json.loads(zf.read(extra)))
            # 같은 공시가 `recent`와 넘침 파일에 겹쳐 오는 일이 있다 (2026-09-19
            # 실측 9쌍). 한 CIK 안에서 **완전히 같은 행**은 접는다 — 같은
            # accession이 form만 다르게 두 번 색인된 것은 원천에 실제로 있다.
            #
            # **키(accession, form)가 같은데 내용이 다른 행**도 겹쳐 온다. 2026-10-03
            # 실측 1쌍(CIK 1132597 `0001564590-19-045194` 6-K): 두 블록에서 접수
            # 시각만 5시간 다르다(13:15:09Z / 18:15:09Z). EDGAR 색인 페이지의 접수
            # 시각은 08:15:09 ET = 13:15:09Z라서 **`recent` 쪽이 맞고 넘침 파일 쪽이
            # 틀렸다.** 블록을 `recent` → 넘침 순으로 읽으므로 **먼저 읽은 행을
            # 남긴다** (결정적이다). 몇 행을 버렸는지 ``conflicting_rows``에 센다.
            seen: set[tuple] = set()
            seen_keys: set[tuple] = set()
            for block in blocks:
                for row in _filing_rows(block):
                    if since_s and (row[2] or "") < since_s:
                        continue
                    if row in seen:
                        duplicate_rows += 1
                        continue
                    seen.add(row)
                    ident = (row[0], row[1])  # (accession, form)
                    if ident in seen_keys:
                        conflicting_rows += 1
                        continue
                    seen_keys.add(ident)
                    fil["cik"].append(cik)
                    for key, value in zip(
                        ("accession", "form", "filing_date", "report_date",
                         "acceptance_datetime", "act", "file_number", "items",
                         "core_type", "primary_document", "is_xbrl", "is_inline_xbrl",
                         "size"),
                        row,
                        strict=True,
                    ):
                        fil[key].append(value)
            if len(fil["cik"]) >= rows_per_part:
                flush()
    flush()

    summary: dict[str, object] = {
        "entities": entities,
        "source_rev": rev,
        "duplicate_rows": duplicate_rows,
        "conflicting_rows": conflicting_rows,
    }
    # 키가 (cik, accession)이 아니다 — 한 accession이 form을 달리해 두 번
    # 색인된 것이 있다 (SC 13D/A와 SC TO-T/A, 접수 시각도 4시간 다르다)
    out_stats: dict[str, object] = {}
    out_stats["filings_index"] = {
        "path": out, **sink.close(unique_on=("cik", "accession", "form"))
    }

    fields = ("cik", "name", "entity_type", "sic", "sic_description", "category",
              "fiscal_year_end", "state_of_incorporation", "ein", "tickers",
              "exchanges", "former_names")
    meta_cols = {
        f: pyar.array([r[i] for r in meta_rows],
                      type=pyar.int64() if f == "cik" else pyar.string())
        for i, f in enumerate(fields)
    }
    meta_cols["observed_at"] = pyar.array(
        [observed_at] * len(meta_rows), type=pyar.timestamp("us", tz="UTC")
    )
    meta_cols["source_rev"] = pyar.array([rev] * len(meta_rows), type=pyar.string())
    pq.write_table(
        pyar.table(meta_cols).select(ARROW_SCHEMAS["company_meta"].names),
        meta_out,
        compression="zstd",
    )
    out_stats["company_meta"] = {
        "path": meta_out,
        **verify_snapshot(meta_out, "company_meta", unique_on=("cik",)),
    }
    return {**summary, **out_stats}
