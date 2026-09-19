"""Wikipedia 리비전 — 지수 구성종목을 그 시점 모습으로 (04 C5).

미국 계획 01 §1, 03 §3, 06 §1.

* **리비전은 불변이다.** 그래서 캐시를 걸어도 된다 (05 §6.1). 받은 wikitext를
  ``raw/``에 그대로 굳히고 다시 안 받는다.
* **표 모양이 해마다 바뀐다.** 머리글이 ``Ticker symbol``이었다가
  ``Ticker Symbol``이 되고, 칸 구분이 ``||``와 줄바꿈 ``|``로 섞인다.
  **위치가 아니라 머리글 이름으로 칸을 잡는다** — Wayback 상장 기록과 같은 규칙이다
  (03 §4.9).
* **내용은 한 요청에 50리비전까지 온다.** ``revids=``에 묶어 보내면 주 1회 표본
  420개가 **9요청**이다. 계획의 420요청·35분이 여기서 줄어든다.
* 연락처 UA를 쓴다. ``.env`` 키를 늘리지 않으려고 ``SEC_USER_AGENT``를 그대로
  쓴다 — **미선언 키 하나로 CLI가 통째로 죽은 적이 있다** (07 X21).
"""

from __future__ import annotations

import datetime as dt
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests

from collector.lake import DataRoot

API = "https://en.wikipedia.org/w/api.php"

#: 지수별 문서. 값은 ``(index_id, 문서 제목)``이다.
INDEX_PAGES: dict[str, str] = {
    "SP500": "List of S&P 500 companies",
}

#: ``revids=``에 묶을 수 있는 최대. 익명 사용자 한도다.
REVIDS_PER_REQUEST = 50

#: 리비전 목록 한 번에 오는 최대.
REVLIST_LIMIT = "max"

#: 읽기 요청이라 한도가 넉넉하다. 직렬 1초면 예의를 지킨다.
DEFAULT_INTERVAL_SECONDS = 1.0


class WikipediaError(RuntimeError):
    """Wikipedia가 거부했거나 받은 것이 기대한 형식이 아니다."""


@dataclass
class WikipediaClient:
    user_agent: str
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS
    session: requests.Session = field(default_factory=requests.Session)
    _last_request_at: float = field(default=0.0, init=False, repr=False)
    requests_made: int = field(default=0, init=False)

    def _wait(self) -> None:
        gap = time.monotonic() - self._last_request_at
        if self._last_request_at and gap < self.interval_seconds:
            time.sleep(self.interval_seconds - gap)

    def get(self, params: dict[str, str]) -> dict:
        self._wait()
        try:
            resp = self.session.get(
                API,
                params={"format": "json", **params},
                headers={"User-Agent": self.user_agent},
                timeout=60,
            )
        finally:
            self._last_request_at = time.monotonic()
            self.requests_made += 1
        if resp.status_code != 200:
            raise WikipediaError(f"{resp.status_code} {params}")
        body = resp.json()
        if "error" in body:
            raise WikipediaError(str(body["error"]))
        return body

    def revisions(self, title: str, *, since: dt.date) -> list[dict]:
        """``since`` 이후 리비전 전부. ``(revid, timestamp)``만 받는다."""
        out: list[dict] = []
        params = {
            "action": "query",
            "prop": "revisions",
            "titles": title,
            "rvlimit": REVLIST_LIMIT,
            "rvprop": "ids|timestamp",
            "rvdir": "newer",
            "rvstart": f"{since.isoformat()}T00:00:00Z",
        }
        while True:
            body = self.get(params)
            pages = (body.get("query") or {}).get("pages") or {}
            if "-1" in pages:
                raise WikipediaError(f"문서가 없다: {title!r}")
            for page in pages.values():
                out.extend(page.get("revisions") or [])
            cont = body.get("continue")
            if not cont:
                return out
            params = {**params, **cont}

    def contents(self, revids: list[int]) -> dict[int, dict]:
        """리비전 여러 개의 wikitext. 한 요청에 50개까지."""
        out: dict[int, dict] = {}
        for i in range(0, len(revids), REVIDS_PER_REQUEST):
            chunk = revids[i : i + REVIDS_PER_REQUEST]
            body = self.get(
                {
                    "action": "query",
                    "prop": "revisions",
                    "revids": "|".join(str(r) for r in chunk),
                    "rvprop": "ids|timestamp|content",
                    "rvslots": "main",
                }
            )
            for page in ((body.get("query") or {}).get("pages") or {}).values():
                for rev in page.get("revisions") or []:
                    out[rev["revid"]] = {
                        "timestamp": rev["timestamp"],
                        "text": rev["slots"]["main"]["*"],
                    }
        return out


# --- 표본 고르기 -------------------------------------------------------------


def weekly_sample(revisions: list[dict], *, weekday: int = 0) -> list[dict]:
    """주 1회 표본. **그 주의 마지막 리비전**을 고른다.

    주중에 편입·제외가 여러 번 반영되면 마지막 것이 그 주말 기준이다.
    """
    by_week: dict[tuple[int, int], dict] = {}
    for rev in revisions:
        ts = dt.datetime.fromisoformat(rev["timestamp"].replace("Z", "+00:00"))
        key = ts.isocalendar()[:2]
        cur = by_week.get(key)
        if cur is None or rev["timestamp"] > cur["timestamp"]:
            by_week[key] = rev
    return [by_week[k] for k in sorted(by_week)]


# --- wikitext 파싱 -----------------------------------------------------------

_LINK = re.compile(r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]")
_TEMPLATE = re.compile(r"\{\{[^}]*?\|([^}|]*)\}\}")
_EXTLINK = re.compile(r"\[(?:https?|//)\S*\s+([^\]]*)\]")
_BARE_EXTLINK = re.compile(r"\[(?:https?|//)\S*\]")
_TAG = re.compile(r"<[^>]+>")
_REF = re.compile(r"<ref[^>]*?(?:/>|>.*?</ref>)", re.S)


def clean_cell(text: str) -> str:
    """위키 문법을 걷어낸다. ``{{NyseSymbol|MMM}}`` → ``MMM``."""
    text = _REF.sub("", text)
    text = _TEMPLATE.sub(r"\1", text)
    text = _LINK.sub(r"\1", text)
    text = _EXTLINK.sub(r"\1", text)
    text = _BARE_EXTLINK.sub("", text)
    text = _TAG.sub("", text)
    return text.replace("&nbsp;", " ").replace("'''", "").strip(" \t|")


def _split_cells(line: str, sep: str) -> list[str]:
    return [c for c in line.split(sep)]


def _table_rows(block: str) -> tuple[list[str], list[list[str]]]:
    """wikitable 하나를 ``(머리글, 행들)``로. 칸 구분이 두 꼴로 섞여 온다."""
    header: list[str] = []
    rows: list[list[str]] = []
    current: list[str] | None = None
    for raw in block.splitlines():
        line = raw.strip()
        if line.startswith("|-"):
            if current:
                rows.append(current)
            current = []
            continue
        if line.startswith("!") and not header:
            header = [clean_cell(c) for c in _split_cells(line.lstrip("!"), "!!")]
            continue
        if line.startswith("!") and current is not None and not rows:
            header += [clean_cell(c) for c in _split_cells(line.lstrip("!"), "!!")]
            continue
        if line.startswith("|") and not line.startswith("|}") and current is not None:
            current += [clean_cell(c) for c in _split_cells(line[1:], "||")]
    if current:
        rows.append(current)
    return header, [r for r in rows if r]


#: 머리글 이름 → 우리 컬럼. 소문자로 맞춘 뒤 **부분 문자열**로 본다.
_HEADER_MAP: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("symbol", ("ticker symbol", "symbol", "ticker")),
    ("security", ("security", "company")),
    ("gics_sector", ("gics sector", "sector")),
    ("gics_sub_industry", ("gics sub industry", "gics sub-industry", "sub industry")),
    ("headquarters", ("headquarters", "address of headquarters")),
    ("date_added", ("date first added", "date added")),
    ("cik", ("cik", "central index key")),
    ("founded", ("founded",)),
)


def _header_index(header: list[str]) -> dict[str, int]:
    lowered = [h.lower() for h in header]
    out: dict[str, int] = {}
    for field_name, needles in _HEADER_MAP:
        for needle in needles:
            hit = next(
                (i for i, h in enumerate(lowered) if needle in h and i not in out.values()),
                None,
            )
            if hit is not None:
                out[field_name] = hit
                break
    return out


def parse_constituents(text: str) -> list[dict[str, str | None]]:
    """구성종목 표를 행 목록으로. **머리글에 심볼 칸이 있는 첫 표**를 쓴다."""
    for block in re.findall(r"\{\|.*?\n\|\}", text, flags=re.S):
        header, rows = _table_rows(block)
        idx = _header_index(header)
        if "symbol" not in idx or "security" not in idx:
            continue
        out = []
        for row in rows:
            symbol = row[idx["symbol"]].strip() if idx["symbol"] < len(row) else ""
            if not symbol:
                continue
            item: dict[str, str | None] = {"symbol": symbol}
            for field_name in ("security", "gics_sector", "gics_sub_industry", "cik"):
                pos = idx.get(field_name)
                item[field_name] = (
                    row[pos].strip() or None if pos is not None and pos < len(row) else None
                )
            out.append(item)
        if out:
            return out
    return []


# --- 받아서 굳히기 -----------------------------------------------------------


def revision_path(root: DataRoot, index_id: str, revid: int) -> Path:
    return root.raw / "wikipedia" / "index" / index_id / f"revid={revid}.wikitext"


def load_index_constituents(
    root: DataRoot,
    client: WikipediaClient,
    *,
    snapshot_date: dt.date | str,
    since: dt.date = dt.date(2018, 1, 1),
    pages: dict[str, str] | None = None,
    observed_at: dt.datetime | None = None,
) -> dict[str, object]:
    """``index_constituents`` 한 장 (03 §4.14)."""
    import pyarrow as pyar

    from collector.us.store.writer import snapshot_path, verify_snapshot, write_snapshot_arrow

    pages = pages or INDEX_PAGES
    observed_at = observed_at or dt.datetime.now(dt.UTC)
    cols: dict[str, list] = {
        k: []
        for k in ("index_id", "revid", "as_of", "symbol", "security",
                  "gics_sector", "gics_sub_industry", "cik")
    }
    per_index: dict[str, dict[str, int]] = {}

    for index_id, title in pages.items():
        revisions = client.revisions(title, since=since)
        sample = weekly_sample(revisions)
        want = [int(r["revid"]) for r in sample]

        cached = {
            revid: revision_path(root, index_id, revid)
            for revid in want
            if revision_path(root, index_id, revid).is_file()
        }
        missing = [revid for revid in want if revid not in cached]
        fetched = client.contents(missing) if missing else {}
        for revid, body in fetched.items():
            dest = revision_path(root, index_id, revid)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(body["text"])

        stamp = {int(r["revid"]): r["timestamp"] for r in sample}
        empty = 0
        for revid in want:
            text = (
                fetched[revid]["text"]
                if revid in fetched
                else cached[revid].read_text()
            )
            rows = parse_constituents(text)
            if not rows:
                empty += 1
                continue
            as_of = dt.datetime.fromisoformat(stamp[revid].replace("Z", "+00:00"))
            for row in rows:
                cols["index_id"].append(index_id)
                cols["revid"].append(revid)
                cols["as_of"].append(as_of)
                cols["symbol"].append(row["symbol"])
                cols["security"].append(row["security"])
                cols["gics_sector"].append(row["gics_sector"])
                cols["gics_sub_industry"].append(row["gics_sub_industry"])
                cols["cik"].append(row["cik"])
        per_index[index_id] = {
            "revisions": len(revisions),
            "sampled": len(want),
            "reused": len(cached),
            "unparsed": empty,
        }

    n = len(cols["index_id"])
    table = pyar.table(
        {
            "index_id": pyar.array(cols["index_id"], type=pyar.string()),
            "as_of": pyar.array(cols["as_of"], type=pyar.timestamp("us", tz="UTC")),
            "revid": pyar.array(cols["revid"], type=pyar.int64()),
            "symbol": pyar.array(cols["symbol"], type=pyar.string()),
            "security": pyar.array(cols["security"], type=pyar.string()),
            "gics_sector": pyar.array(cols["gics_sector"], type=pyar.string()),
            "gics_sub_industry": pyar.array(cols["gics_sub_industry"], type=pyar.string()),
            "cik": pyar.array(cols["cik"], type=pyar.string()),
            "observed_at": pyar.array(
                [observed_at] * n, type=pyar.timestamp("us", tz="UTC")
            ),
        }
    )
    dest = snapshot_path(root, "index_constituents", snapshot_date)
    write_snapshot_arrow(
        table, "index_constituents", dest, unique_on=("index_id", "revid", "symbol")
    )
    stats = verify_snapshot(
        dest, "index_constituents", unique_on=("index_id", "revid", "symbol")
    )
    return {
        "path": dest,
        "requests": client.requests_made,
        "per_index": per_index,
        **stats,
    }
