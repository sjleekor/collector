"""거래일 달력 — 다음 거래일 계산을 한곳에 둔다.

**``n1(d)`` 는 d 보다 **뒤인** 첫 거래일이다**(d 가 거래일이어도 d 는 아니다).
"공개 시점의 다음 거래일부터 쓴다"가 전부 이 함수다 (설계 2.1·2.2).
"""

from __future__ import annotations

import bisect
import datetime as dt


class Calendar:
    def __init__(self, sessions: list[dt.date]):
        if not sessions:
            raise ValueError("거래일이 하나도 없다")
        self.sessions = sorted(sessions)
        self._set = set(self.sessions)

    @classmethod
    def xnys(cls, start: dt.date, end: dt.date) -> Calendar:
        """``exchange_calendars`` XNYS. 라이브러리가 아는 마지막 날까지만 준다."""
        import exchange_calendars as xcals

        cal = xcals.get_calendar("XNYS")
        last = cal.last_session.date()
        stop = min(end, last)
        return cls([d.date() for d in cal.sessions_in_range(start, stop)])

    def is_session(self, d: dt.date) -> bool:
        return d in self._set

    def n1(self, d: dt.date) -> dt.date | None:
        """d 뒤의 첫 거래일. 달력이 끝났으면 ``None``."""
        i = bisect.bisect_right(self.sessions, d)
        return self.sessions[i] if i < len(self.sessions) else None

    def n2(self, d: dt.date) -> dt.date | None:
        """d 뒤의 둘째 거래일."""
        i = bisect.bisect_right(self.sessions, d)
        return self.sessions[i + 1] if i + 1 < len(self.sessions) else None

    def first_in_month(self, year: int, month: int, floor: dt.date | None = None) -> dt.date | None:
        """그 달 첫 거래일. ``floor`` 가 있으면 그 날 이후의 첫 거래일."""
        lo = dt.date(year, month, 1)
        if floor is not None and floor > lo:
            lo = floor
        i = bisect.bisect_left(self.sessions, lo)
        if i < len(self.sessions) and (
            self.sessions[i].year == year and self.sessions[i].month == month
        ):
            return self.sessions[i]
        return None

    def register(self, con, *, from_date: dt.date) -> None:
        """DuckDB 에 ``xcal(date, idx)`` 와 ``nxt(d, n1, n2)`` 를 만든다.

        ``idx`` 는 거래일 순번이라 두 날짜 사이 빠진 거래일 수를 뺄셈으로 센다.
        """
        con.execute("CREATE OR REPLACE TABLE xcal (date DATE, idx INTEGER)")
        con.executemany(
            "INSERT INTO xcal VALUES (?, ?)", [(d, i) for i, d in enumerate(self.sessions)]
        )
        days = []
        d = from_date
        last = self.sessions[-1]
        while d <= last:
            days.append((d, self.n1(d), self.n2(d)))
            d += dt.timedelta(days=1)
        con.execute("CREATE OR REPLACE TABLE nxt (d DATE, n1 DATE, n2 DATE)")
        con.executemany("INSERT INTO nxt VALUES (?, ?, ?)", days)


def ftd_publication(settlement: dt.date, lag_days: int) -> dt.date:
    """FTD 결제일이 든 반월 구간 끝 + ``lag_days`` 달력일 (``02_lag_constants.md`` ``LAG_FTD``)."""
    if settlement.day <= 15:
        end = dt.date(settlement.year, settlement.month, 15)
    else:
        nxt = dt.date(settlement.year + (settlement.month == 12), (settlement.month % 12) + 1, 1)
        end = nxt - dt.timedelta(days=1)
    return end + dt.timedelta(days=lag_days)
