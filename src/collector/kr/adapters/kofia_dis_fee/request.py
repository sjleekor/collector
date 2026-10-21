"""금투협 DIS "펀드별 보수비용비교" 요청 — 호출 규격은 계획 04 §2.1 끝(2026-10-10~11 조사).

``POST https://dis.kofia.or.kr/proframeWeb/XMLSERVICES/``에 XML을 보낸다. 인증은 없다.

* 데이터: ``FS-DIS2`` / ``DISFundFeeCmsSO`` / ``select``. 입력 ``DISCondFuncDTO``의
  ``tmpV30``이 기준일(YYYYMMDD)이고 **정확히 맞아야** 값이 나온다(월 중간은 0건).
  ``tmpV12``(펀드명)에 "상장지수"를 주면 ETF만 온다. 연구용 사본(280개 달)을 받을 때
  쓴 본문과 글자 그대로 같다(시험이 고정한다).
* 최근 기준일: ``FS-DIS2`` / ``DISComStdYMDSO`` / ``select``, ``codeDesc=J_RD``.

최근 기준일 요청 본문은 2026-10-10 23:56 서버에서 성공한 요청
(``research_refs/.../dis_20261010/req_std.xml``)과 글자 그대로 같다.
입력 DTO는 ``DISComStdYMDDTO``다(화면 스크립트 ``srchCond_inquiryPeriod.js``의
``setStdDtOp``). 응답은 ``DISComStdYMDListDTO/list/standardDt`` 하나에 날짜가 온다
(fixture ``latest_j_rd.xml``, 그때 값 20260831). 틀린 DTO(``DISCondFuncDTO``)로 처음 썼던 것을
메인 검증에서 고쳤다(10-11).
"""

from __future__ import annotations

ENDPOINT_URL = "https://dis.kofia.or.kr/proframeWeb/XMLSERVICES/"
REFERER_URL = (
    "https://dis.kofia.or.kr/websquare/index.jsp?w2xPath=/wq/fundann/DISFundFeeCMS.xml"
    "&divisionId=MDIS01005001000000&serviceId=SDIS01005001000"
)
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0 Safari/537.36"
)

#: 펀드명 조건. 이 글자가 든 펀드만 온다 — ETF("…증권상장지수투자신탁")를 가리는 방법이다.
ETF_NAME_FILTER = "상장지수"

FEE_BODY = """<?xml version="1.0" encoding="utf-8"?>
<message>
  <proframeHeader>
    <pfmAppName>FS-DIS2</pfmAppName>
    <pfmSvcName>DISFundFeeCmsSO</pfmSvcName>
    <pfmFnName>select</pfmFnName>
  </proframeHeader>
  <systemHeader></systemHeader>
    <DISCondFuncDTO>
    <tmpV30>{d}</tmpV30>
    <tmpV11></tmpV11>
    <tmpV12>{name}</tmpV12>
    <tmpV3></tmpV3>
    <tmpV5></tmpV5>
    <tmpV4></tmpV4>
</DISCondFuncDTO>
</message>
"""

LATEST_STD_BODY = """<?xml version="1.0" encoding="utf-8"?>
<message>
  <proframeHeader>
    <pfmAppName>FS-DIS2</pfmAppName>
    <pfmSvcName>DISComStdYMDSO</pfmSvcName>
    <pfmFnName>select</pfmFnName>
  </proframeHeader>
  <systemHeader></systemHeader>
    <DISComStdYMDDTO>
    <codeDesc>J_RD</codeDesc>
</DISComStdYMDDTO>
</message>
"""


def build_fee_request(std_ymd: str, name_filter: str = ETF_NAME_FILTER) -> str:
    """기준일(``YYYYMMDD``)의 ETF 보수 조회 본문."""
    if len(std_ymd) != 8 or not std_ymd.isdigit():
        raise ValueError(f"기준일은 YYYYMMDD여야 합니다: {std_ymd!r}")
    return FEE_BODY.format(d=std_ymd, name=name_filter)


def build_latest_std_request() -> str:
    """최근 기준일(``J_RD``) 조회 본문. 위 경고를 본다."""
    return LATEST_STD_BODY
