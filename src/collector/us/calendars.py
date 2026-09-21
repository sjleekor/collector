"""거래일 캘린더 — ``exchange_calendars``를 스냅샷으로 굳힌다 (04 C5).

**라이브러리를 그때그때 부르지 않고 한 벌 굳히는 이유.** 라이브러리 판이 바뀌면
과거 휴장일 판정이 조용히 달라진다. 유니버스 조립([`universe/build.py`])과
모델링이 같은 날짜 집합을 봐야 하므로 **버전을 행에 박아 남긴다.**

조기 종료일(반장)을 따로 표시한다 — 12월 24일·추수감사절 다음 날처럼 13:00에
닫는 날이다. 거래량·거래대금 피쳐가 그날 절반이 되는 것이 이상값이 아니다.
"""

from __future__ import annotations

import datetime as dt

import pyarrow as pyar

from collector.lake import DataRoot
from collector.us.store.writer import snapshot_path, verify_snapshot, write_snapshot_arrow

DEFAULT_EXCHANGE = "XNYS"

#: 정규장 마감 시각(거래소 현지). 이보다 일찍 닫으면 조기 종료일이다.
REGULAR_CLOSE = dt.time(16, 0)

#: 캘린더를 **오늘로부터 이만큼 앞까지** 굳힌다.
#:
#: **끝 날짜를 코드에 박지 않는다.** 원래 기본값이 ``"2026-12-31"`` 이었다 —
#: C5 를 돌린 해의 연말이다. 그대로 두면 **2027-01-01 에 새 세션이 0건이 되고
#: 하루 실행이 조용히 멈춘다** (2026-09-21 확인). 날짜를 굳히지 말고 지평을
#: 굳힌다.
HORIZON_YEARS = 3


def load_trading_calendar(
    root: DataRoot,
    *,
    snapshot_date: dt.date | str,
    start: dt.date | str = "2018-01-01",
    end: dt.date | str | None = None,
    exchange: str = DEFAULT_EXCHANGE,
    observed_at: dt.datetime | None = None,
    today: dt.date | None = None,
) -> dict[str, object]:
    """``trading_calendar`` 한 장. 요청이 0이다 — 라이브러리가 준다.

    ``end`` 를 안 주면 **오늘로부터 ``HORIZON_YEARS`` 년 뒤**까지 굳힌다.
    고정 날짜를 기본값에 두면 그 날짜가 지나는 순간 수집이 멈춘다.
    """
    import exchange_calendars as xcals

    observed_at = observed_at or dt.datetime.now(dt.UTC)
    if end is None:
        base = today or dt.date.today()
        end = base.replace(year=base.year + HORIZON_YEARS, month=12, day=31)
    cal = xcals.get_calendar(exchange)
    sessions = cal.sessions_in_range(str(start), str(end))

    closes = cal.closes.loc[sessions]
    tz = cal.tz
    dates, early, close_local = [], [], []
    for session, close_utc in zip(sessions, closes, strict=True):
        local = close_utc.tz_localize("UTC").tz_convert(tz) if close_utc.tzinfo is None else (
            close_utc.tz_convert(tz)
        )
        dates.append(session.date())
        close_local.append(local.time())
        early.append(local.time() < REGULAR_CLOSE)

    table = pyar.table(
        {
            "date": pyar.array(dates, type=pyar.date32()),
            "exchange": pyar.array([exchange] * len(dates), type=pyar.string()),
            "close_local": pyar.array(close_local, type=pyar.time64("us")),
            "is_early_close": pyar.array(early, type=pyar.bool_()),
            "observed_at": pyar.array(
                [observed_at] * len(dates), type=pyar.timestamp("us", tz="UTC")
            ),
            "source_rev": pyar.array(
                [f"exchange_calendars {xcals.__version__}"] * len(dates), type=pyar.string()
            ),
        }
    )
    dest = snapshot_path(root, "trading_calendar", snapshot_date)
    write_snapshot_arrow(table, "trading_calendar", dest, unique_on=("date", "exchange"))
    stats = verify_snapshot(dest, "trading_calendar", unique_on=("date", "exchange"))
    return {
        "path": dest,
        "exchange": exchange,
        "early_closes": sum(early),
        "first": dates[0],
        "last": dates[-1],
        **stats,
    }
