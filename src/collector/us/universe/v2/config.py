"""v2 규칙의 숫자와 스위치. 값마다 근거 절을 적는다 (``01_measurements.md``)."""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass


@dataclass(frozen=True)
class V2Rules:
    #: 가격 공백으로 치는 최소 거래일 수 (§5.7 1.1)
    gap_min: int = 21
    #: 이 이상이면 보강 신호 없이도 끊는다 — 1년 이상 거래가 없던 심볼 (B4, §5.8 3.3)
    gap_dorm: int = 252
    #: 같은 날 이만큼 이상 심볼이 재개하면 가격 결손으로 보고 B4에서 뺀다 (**임의 값**, §5.8 3.3)
    mass_resume_symbols: int = 20
    #: 같은 날 cik 전환이 이만큼 이상이면 지도 일괄 갱신으로 보고 신호 K 에서 뺀다 (§5.7 1.2)
    mass_cik_day: int = 50
    #: 보강 신호를 찾는 재개일 앞뒤 일수 (§5.7 1.1)
    corroboration_days: int = 180
    #: CUSIP 앞 6자리 변경을 신호 C 로 치는 옛 구간과의 최소 간격(일)
    cusip_sep_days: int = 30
    #: 새 CUSIP 앞 6자리가 옛 것과 이만큼 떨어지면 S_dorm 후보(일)
    cusip_dorm_days: int = 252
    #: S_dorm: 새 first_seen 이 첫 가격일 앞뒤 이 안이어야 한다(일)
    s_dorm_first_days: int = 30
    #: 같은 심볼의 분리 사건이 이 안이면 하나로 합친다(일)
    merge_days: int = 60
    #: 새 구간은 이 행 수가 찬 뒤부터 판정한다 (예열, §5.7 1.3)
    warmup_rows: int = 20
    #: FTD 공개 지연: 반월 구간 끝 + 이 일수 (``02_lag_constants.md`` ``LAG_FTD``).
    #: 2020년 이전에도 같다고 **가정**한다.
    lag_ftd_days: int = 20
    #: FTD 설명을 버리는 나이(일) (§5.8 1.1)
    ftd_max_age_days: int = 400
    #: 정기보고서가 이 개월 안에 있어야 운영회사로 본다 (§5.7 1.4)
    periodic_months: int = 18
    #: 마지막 공시가 이보다 오래된 cik 는 분리 뒤 구간에서 무효 (일, §5.7 1.1)
    cik_dormant_days: int = 365
    #: 약한 이름 펀드 규칙에서 상장 이름과 발행사 이름의 자카드 문턱 (§5.7 1.4)
    weak_fund_jaccard: float = 0.6
    #: 확인 안 됨 비율이 이 위면 completion 에 경고를 남긴다. **제안값**이다 (설계 7장)
    unknown_warn_ratio: float = 0.03
    #: ``G_dorm``(공백 252거래일 이상 + 보강 신호)의 인지 시점을 **재개일로** 둔다.
    #: 집단 재개일(가격 결손 의심)은 빼고, 그날은 보강 신호가 알려진 때 끊는다.
    #: 설계 문서는 "보강 신호 중 늦은 쪽"이다. 그러면 ``G_gap252``(보강 신호 없음, 재개일에 인지)와
    #: 시점이 어긋나 입력을 t에서 잘라 다시 만든 분리 집합이 전체 실행과 안 맞는다(T7).
    #: 설계 그대로(``False``)로 돌려 효과를 잴 수 있게 스위치로 둔다.
    dorm_known_at_resume: bool = True
    #: 설계 7장 "5.06 없는 합병 보조 규칙". 별도 측정 뒤 켠다 — **기본은 끔**.
    #: 켜면 상장 목록 이름이 SPAC 꼴에서 운영회사 꼴로 이미 바뀐 심볼의 ``spac_sic`` 를 푼다.
    spac_release_name_change: bool = False


#: v1 과 같은 문턱 (03 §5.3)
ENTRY_ADV_USD = 1_000_000
MAINTAIN_ADV_USD = 700_000
MIN_TRADED_DAYS_20 = 10
USABLE_FROM = _dt.date(2018, 9, 7)
#: 20거래일 롤링을 채우는 시작일. v1 전체 재빌드의 ``start - 90일`` 과 같다.
ROLL_FROM = _dt.date(2018, 6, 9)
JUDGE_LAG_MONTHS = 1

DEFAULT_RULES = V2Rules()
