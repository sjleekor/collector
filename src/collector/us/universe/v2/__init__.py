"""미국 유니버스 v2 — 종목 식별 → 증권 마스터 → ``universe_daily_v2`` (설계 02).

v1(``collector.us.universe.build``)은 **건드리지 않는다.** v2 는 자기 전용 표만 쓴다.

* ``listing_snapshots_v2`` — Wayback 과 매일 받는 nasdaqtrader 원문을 합친 상장 목록
* ``security_segments`` — 심볼 재사용을 가른 종목 구간 (``SYMBOL#n``)
* ``security_master`` — 구간별 증권 종류와 포함 여부
* ``universe_daily_v2`` — ``(date, security_id)`` 단위 멤버십
"""

#: 규칙 버전. **규칙을 바꾸면 올린다** — 행마다 박혀 어느 규칙으로 만든 것인지 남는다.
#: 마지막 자리는 시험이 못 가르는 가정(FTD 공개 지연, 집단 재개일 문턱)이 바뀔 때 올린다.
SEGMENT_RULE_VERSION = "seg-r3c.1"
MASTER_RULE_VERSION = "master-r3c.1"
UNIVERSE_RULE_VERSION = "ud2-1"
RULE_VERSION = f"{SEGMENT_RULE_VERSION}+{MASTER_RULE_VERSION}+{UNIVERSE_RULE_VERSION}"
