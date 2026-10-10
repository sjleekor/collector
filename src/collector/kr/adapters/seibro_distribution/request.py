"""SEIBro 분배금지급현황 요청 XML.

2026-10-09 조사 때 ``pull.py``가 보낸 모양 그대로입니다. 목록과 건수는 액션
이름만 다르고(``…Cnt``) 매개변수는 같습니다.
"""

from __future__ import annotations

from datetime import date
from xml.sax.saxutils import quoteattr

ENDPOINT_URL = "https://seibro.or.kr/websquare/engine/proworks/callServletService.jsp"
REFERER_URL = (
    "https://seibro.or.kr/websquare/control.jsp?w2xPath=/IPORTAL/user/etf/BIP_CNTS06030V.xml"
)
TASK = "ksd.safe.bip.cnts.etf.process.EtfExerInfoPTask"
LIST_ACTION = "exerInfoDtramtPayStatPlist"
COUNT_ACTION = "exerInfoDtramtPayStatPlistCnt"

#: 서버가 한 번에 주는 최대 행 수. ``END_PAGE``를 키워도 30행입니다.
PAGE_SIZE = 30


def fmt_date(d: date) -> str:
    """``date`` → ``YYYYMMDD``."""
    return d.strftime("%Y%m%d")


def build_request(
    action: str,
    start: date,
    end: date,
    *,
    start_page: int = 1,
    end_page: int = PAGE_SIZE,
) -> str:
    """요청 XML을 만듭니다. 건수 요청에도 같은 페이지 칸을 넣습니다."""
    if action not in (LIST_ACTION, COUNT_ACTION):
        raise ValueError(f"unknown SEIBro action: {action}")
    return (
        f"<reqParam action={quoteattr(action)} task={quoteattr(TASK)}>"
        '<etf_sort_cd value=""/><etf_big_sort_cd value=""/><isin value=""/>'
        '<mngco_custno value=""/><RGT_RSN_DTAIL_SORT_CD value=""/>'
        f'<fromRGT_STD_DT value="{fmt_date(start)}"/>'
        f'<toRGT_STD_DT value="{fmt_date(end)}"/>'
        f'<START_PAGE value="{start_page}"/><END_PAGE value="{end_page}"/>'
        "</reqParam>"
    )


def build_list_request(start: date, end: date, page: int) -> str:
    """``page``(1부터)번째 30행 목록 요청."""
    first = (page - 1) * PAGE_SIZE + 1
    return build_request(LIST_ACTION, start, end, start_page=first, end_page=first + PAGE_SIZE - 1)


def build_count_request(start: date, end: date) -> str:
    """``LIST_CNT``를 받는 건수 요청."""
    return build_request(COUNT_ACTION, start, end)
