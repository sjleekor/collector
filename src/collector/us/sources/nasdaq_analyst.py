"""``api.nasdaq.com`` 애널리스트 추정치 — 오늘부터 쌓는 것 말고는 방법이 없다.

source_expansion 04(``my/milestones/us/research/data/source_expansion/04_nasdaq_analyst.md``)·
99(같은 디렉터리의 ``99_gaps_and_next.md``) §3.4·§4 순번 4.

* **과거를 복원 못 한다.** ``earnings-forecast`` 응답에 기준 시각(``asOf``)이
  없고, 같은 원천의 ``targetprice.historicalConsensus``도 13개월뿐이다 —
  **전진 축적 전용** 판정이다(04 §3·§5). 그래서 이 모듈은 "오늘 무엇을
  받았나"가 아니라 "이번 주 유니버스 중 아직 못 받은 심볼이 누구인가"로
  할 일을 잡는다.

* **``earnings-forecast``만 derived로 뽑는다.** 같은 원천의 ``targetprice``·
  ``ratings``도 실측으로 필드를 확인했지만(04 §2 표, 2026-09-24 재확인),
  ``historicalConsensus``가 종목마다 같은 13개월인지·``upgradesDowngrades``가
  언제 차는지가 아직 미확인이다(99 §3.4 1·2번). 분기 5·연간 4개 컨센서스
  EPS와 **4주 상향·하향 건수**만 지금 값어치가 확정됐다 — 04 §2.1이 "제일
  값어치가 있다"고 적은 바로 그 표다. 나머지 둘도 raw는 원한다면 이 모듈의
  ``EARNINGS_FORECAST_URL`` 옆에 URL만 더해 넣으면 되지만, 지금은 손대지
  않는다.

* **HTTP 200이 성공이 아니다** (06 §1과 같은 원칙). 2026-09-24 sj2-server
  실측: 존재하지 않는 심볼(``ZZZZZ``)도 200을 주고 본문 ``status.rCode``가
  400("Symbol not exists.")이다. 반대로 분석 커버리지가 아예 없는 심볼
  (``ATER``·``BRK.B``)도 200에 ``quarterlyForecast: null``이다. **둘 다
  실패가 아니라 원문을 그대로 남기고 0행으로 판단한다** — 재시도해도 안
  바뀌는 응답을 실패로 세면 영원히 재시도만 하게 된다. 진짜 실패(타임아웃·
  5xx·잘못된 JSON)만 ``NasdaqError``/``requests`` 예외로 올라온다.

* **요청은 브라우저 UA로 보낸다** — :mod:`collector.us.sources.nasdaq`과 같은
  호스트·같은 클라이언트다. 간격은 04 §4의 비용 어림(초당 1건 = 종목당
  5초 = 한 바퀴 5.8시간)을 그대로 쓴다. 대상은 04의 "전체 약 4,200종목"이
  아니라 ``universe_daily``의 유동성 필터를 통과한 유니버스라 실제로는 더
  적고 더 여유가 있다.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

from collector.lake import DataRoot
from collector.us.sources.nasdaq import BROWSER_USER_AGENT, NasdaqClient, NasdaqError, parse_money

__all__ = [
    "BROWSER_USER_AGENT",
    "NasdaqClient",
    "NasdaqError",
    "EARNINGS_FORECAST_URL",
    "DEFAULT_INTERVAL_SECONDS",
    "week_start",
    "earnings_forecast_path",
    "target_symbols",
    "summarize_earnings_forecast",
    "parse_fiscal_end",
    "fetch_one",
    "load_nasdaq_analyst_estimates",
]

EARNINGS_FORECAST_URL = "https://api.nasdaq.com/api/analyst/{symbol}/earnings-forecast"

#: 04 §4 비용 표가 이 값으로 어림했다(간격 5초 = 한 바퀴 5.8시간). 그대로 쓴다 —
#: 이 원천에서 실측된 차단·429 관찰은 아직 없으니 보수적인 값을 유지한다.
DEFAULT_INTERVAL_SECONDS = 5.0

_MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}
#: 실측 포맷 "Sep 2026" (공백 구분). ``nasdaq.parse_fiscal_quarter``의
#: "Dec/2023"(슬래시 구분)과 다르다 — 같은 회사·같은 소스여도 엔드포인트마다
#: 다른 표기를 쓴다.
_FISCAL_END_RE = re.compile(r"^([A-Za-z]{3})[A-Za-z]*\s+(\d{4})$")


def week_start(day: dt.date) -> dt.date:
    """ISO 주의 월요일. raw 파티션 키다.

    **같은 주에 여러 번 돌아도 같은 자리에 쌓인다** — Cronicle이 토요일에
    걸어 두었어도 재시도가 다른 요일로 밀리면 날짜가 다시 계산에 들어가는데,
    ``day``가 속한 ISO 주만 보므로 파티션이 갈라지지 않는다.
    """
    return day - dt.timedelta(days=day.weekday())


def earnings_forecast_path(root: DataRoot, snapshot_date: dt.date | str, symbol: str) -> Path:
    """``raw/nasdaq/analyst_earnings_forecast/snapshot_date=<주 월요일>/<심볼>.json``.

    **절대 덮어쓰거나 지우지 않는다** — 이 원천은 다시 못 받는다(04 §3).
    """
    d = snapshot_date.isoformat() if isinstance(snapshot_date, dt.date) else str(snapshot_date)
    return (
        root.raw
        / "nasdaq"
        / "analyst_earnings_forecast"
        / f"snapshot_date={d}"
        / f"{symbol}.json"
    )


def target_symbols(root: DataRoot) -> list[str]:
    """이번 주 대상 — ``universe_daily`` 최신 날짜의 ``in_universe`` 멤버.

    최신 스냅샷이 없으면 먼저 만들라고 말한다. 유니버스 자체가 진입·유지
    문턱으로 이미 좁혀 놓은 것이라(03 §5.3) 04 §4의 "전체 약 4,200종목"보다
    작다 — 비용은 그 어림보다 더 여유가 있다.
    """
    import duckdb

    from collector.us.store.writer import latest_snapshot

    path = latest_snapshot(root, "universe_daily")
    if path is None:
        raise FileNotFoundError(
            "universe_daily 스냅샷이 없다. 먼저 만든다 — collector us-universe rebuild"
        )
    con = duckdb.connect()
    rows = con.execute(
        f"""
        SELECT DISTINCT symbol FROM read_parquet('{path}')
        WHERE date = (SELECT max(date) FROM read_parquet('{path}'))
          AND in_universe
        ORDER BY symbol
        """
    ).fetchall()
    return [r[0] for r in rows]


def _iter_sections(doc: dict):
    """``data`` 아래 ``rows`` 리스트를 담은 구간을 전부 찾는다.

    실측(2026-09-24, sj2-server, AAPL)은 ``quarterlyForecast``·
    ``yearlyForecast``다. 이름에 기대지 않고 **``rows`` 리스트를 담은 하위
    구조를 전부** 찾는다 — 이 원천이 나중에 구간을 더하거나 이름을 바꿔도
    조용히 깨지지 않는다. 커버리지가 없는 심볼(``ATER``)은 두 구간이 모두
    ``null``이라 여기서 자연히 빠진다 — 실패가 아니라 0행이다.
    """
    data = doc.get("data")
    if not isinstance(data, dict):
        return
    for key, value in data.items():
        if isinstance(value, dict) and isinstance(value.get("rows"), list):
            yield key, value["rows"]
        elif isinstance(value, list):
            yield key, value


def summarize_earnings_forecast(doc: dict) -> dict[str, object]:
    """섹션·행 수·응답 상태. **200이 성공이 아니다** — ``status.rCode``를 같이 낸다."""
    sections = dict(_iter_sections(doc))
    status = doc.get("status") or {}
    return {
        "sections": sorted(sections),
        "rows": sum(len(v) for v in sections.values()),
        "status_rcode": status.get("rCode"),
        "message": doc.get("message"),
    }


def _num(value: object) -> float | None:
    """실측 응답은 숫자 그대로 온다(``1.98``). 문자열이 섞여도 받는다."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return parse_money(str(value))


def _count(value: object) -> int | None:
    n = _num(value)
    return None if n is None else int(n)


def parse_fiscal_end(text: str | None) -> dt.date | None:
    """``Sep 2026`` → 2026-09-30. 그 달의 마지막 날로 둔다.

    실측 포맷이다(2026-09-24, AAPL). 못 알아보는 형식이면 ``None`` — 호출부가
    ``fiscal_end_raw``는 그대로 남기므로 정보가 사라지지 않는다.
    """
    raw = (text or "").strip()
    m = _FISCAL_END_RE.match(raw)
    if not m:
        return None
    month = _MONTHS.get(m.group(1)[:3].title())
    if month is None:
        return None
    year = int(m.group(2))
    return dt.date(year + (month == 12), (month % 12) + 1, 1) - dt.timedelta(days=1)


def fetch_one(
    client: NasdaqClient,
    root: DataRoot,
    snapshot_date: dt.date | str,
    symbol: str,
    *,
    skip_existing: bool = True,
) -> dict[str, object]:
    """한 심볼의 이번 주 스냅샷을 받아 원문 그대로 굳힌다.

    ``skip_existing``의 기본값이 **True**다 — 같은 주 안에서 몇 번을 다시
    돌려도 이미 받은 심볼은 건드리지 않는다(이어받기). ``nasdaq.py``의
    ``fetch_earnings``가 기본 False인 것과 반대인 이유는 원천이 다르다:
    실적 캘린더는 같은 날짜가 나중에 정정되는데(``epsForecast`` 정정, 05
    §6.1), 이 원천은 애초에 기준 시각이 없어(``asOf: null``) "정정"이라는
    개념이 없다 — 같은 주 안에서 다시 받아도 새로 알 것이 없다.

    ``fetched_at``을 원문과 같이 봉투에 담는다
    (``{"fetched_at": ..., "response": ...}``). **파일 mtime에 기대지
    않는다** — 미국 레이크는 서버가 정본이고 맥은 미러가 덮어써서 mtime이
    실제 수집 시각과 달라질 수 있다(D9).
    """
    dest = earnings_forecast_path(root, snapshot_date, symbol)
    if skip_existing and dest.is_file():
        return {"symbol": symbol, "status": "skipped", "path": dest}

    doc = client.get_json(EARNINGS_FORECAST_URL.format(symbol=symbol), {})
    fetched_at = dt.datetime.now(dt.UTC)
    envelope = {"fetched_at": fetched_at.isoformat(), "response": doc}
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(envelope, ensure_ascii=False))
    return {
        "symbol": symbol,
        "status": "fetched",
        "path": dest,
        **summarize_earnings_forecast(doc),
    }


def load_nasdaq_analyst_estimates(
    root: DataRoot,
    *,
    snapshot_date: dt.date | str,
) -> dict[str, object]:
    """``raw/nasdaq/analyst_earnings_forecast/``의 JSON 전부를 누적 정규화한다.

    **매주 쌓인 파티션을 전부 다시 읽는다** — ``nasdaq.load_earnings_calendar``와
    같은 모양이다(cumulative). ``observed_at``은 이 derive를 돌린 지금이
    아니라 **그 파일을 실제로 받은 시각**(봉투의 ``fetched_at``)이다 — 설계
    요구가 "PIT 관점에서 as_of는 수집 시각"이라고 못박았다. 봉투가 없는
    파일(있어서는 안 되지만)은 파일 mtime으로 떨어진다.
    """
    import pyarrow as pyar

    from collector.us.store.writer import snapshot_path, verify_snapshot, write_snapshot_arrow

    src_dir = root.raw / "nasdaq" / "analyst_earnings_forecast"
    files = sorted(src_dir.glob("snapshot_date=*/*.json"))
    if not files:
        raise NasdaqError(
            f"{src_dir}에 받아 둔 JSON이 없다. 먼저 받는다 — collector us-nasdaq-analyst run"
        )

    cols: dict[str, list] = {
        k: []
        for k in (
            "collected_week",
            "symbol",
            "period_type",
            "fiscal_end_raw",
            "fiscal_end",
            "consensus_eps_forecast",
            "high_eps_forecast",
            "low_eps_forecast",
            "n_estimates",
            "up",
            "down",
            "observed_at",
        )
    }
    empty_files = fiscal_end_unparsed = missing_envelope = 0

    for path in files:
        week = dt.date.fromisoformat(path.parent.name.split("=", 1)[1])
        symbol = path.stem
        envelope = json.loads(path.read_text())
        if isinstance(envelope, dict) and "response" in envelope and "fetched_at" in envelope:
            doc = envelope["response"] or {}
            fetched_at = dt.datetime.fromisoformat(envelope["fetched_at"])
        else:
            # 봉투가 없는 파일 — 있어서는 안 되지만 조용히 죽는 대신 mtime으로 받는다
            doc = envelope
            fetched_at = dt.datetime.fromtimestamp(path.stat().st_mtime, dt.UTC)
            missing_envelope += 1
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=dt.UTC)

        rows_found = False
        for period_type, rows in _iter_sections(doc):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                rows_found = True
                fiscal_end_raw = (row.get("fiscalEnd") or "").strip() or None
                fiscal_end = parse_fiscal_end(fiscal_end_raw)
                if fiscal_end_raw and fiscal_end is None:
                    fiscal_end_unparsed += 1
                cols["collected_week"].append(week)
                cols["symbol"].append(symbol)
                cols["period_type"].append(period_type)
                cols["fiscal_end_raw"].append(fiscal_end_raw)
                cols["fiscal_end"].append(fiscal_end)
                cols["consensus_eps_forecast"].append(_num(row.get("consensusEPSForecast")))
                cols["high_eps_forecast"].append(_num(row.get("highEPSForecast")))
                cols["low_eps_forecast"].append(_num(row.get("lowEPSForecast")))
                cols["n_estimates"].append(_count(row.get("noOfEstimates")))
                cols["up"].append(_count(row.get("up")))
                cols["down"].append(_count(row.get("down")))
                cols["observed_at"].append(fetched_at)
        if not rows_found:
            empty_files += 1

    n = len(cols["symbol"])
    table = pyar.table(
        {
            "collected_week": pyar.array(cols["collected_week"], type=pyar.date32()),
            "symbol": pyar.array(cols["symbol"], type=pyar.string()),
            "period_type": pyar.array(cols["period_type"], type=pyar.string()),
            "fiscal_end_raw": pyar.array(cols["fiscal_end_raw"], type=pyar.string()),
            "fiscal_end": pyar.array(cols["fiscal_end"], type=pyar.date32()),
            "consensus_eps_forecast": pyar.array(
                cols["consensus_eps_forecast"], type=pyar.float64()
            ),
            "high_eps_forecast": pyar.array(cols["high_eps_forecast"], type=pyar.float64()),
            "low_eps_forecast": pyar.array(cols["low_eps_forecast"], type=pyar.float64()),
            "n_estimates": pyar.array(cols["n_estimates"], type=pyar.int32()),
            "up": pyar.array(cols["up"], type=pyar.int32()),
            "down": pyar.array(cols["down"], type=pyar.int32()),
            "observed_at": pyar.array(cols["observed_at"], type=pyar.timestamp("us", tz="UTC")),
        }
    )
    # 받은 파일이 전부 커버리지 0행이면 n == 0 이다 — write_snapshot_arrow·
    # verify_snapshot 은 빈 표도 그대로 받는다 (구조 검사만 하고 값은 없다).
    dest = snapshot_path(root, "nasdaq_analyst_estimates", snapshot_date)
    unique_on = ("collected_week", "symbol", "period_type", "fiscal_end_raw")
    write_snapshot_arrow(table, "nasdaq_analyst_estimates", dest, unique_on=unique_on)
    stats = verify_snapshot(dest, "nasdaq_analyst_estimates", unique_on=unique_on)
    return {
        "path": dest,
        "files": len(files),
        "rows": n,
        "empty_files": empty_files,
        "fiscal_end_unparsed": fiscal_end_unparsed,
        "missing_envelope": missing_envelope,
        **stats,
    }
