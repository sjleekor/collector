"""이름·심볼·FTD 설명으로 증권 종류를 가르는 규칙 (설계 02 §2.2).

**순수 함수만 둔다.** DuckDB·파일을 안 만진다. 시험이 규칙 하나씩을 직접 부른다.

``classify_name`` 은 modeler 의 ``reporting.markdown.classify_us`` 와 같은 규칙이다.
collector 가 modeler 를 import 하면 순환이라(``collector`` 가 ``modeler`` 의 의존성)
정규식을 옮겨 왔다. **고치면 두 곳을 같이 고친다.**
"""

from __future__ import annotations

import re

# --- 이름 규칙 (modeler classify_us 와 같다) -----------------------------------

KIND_COMMON = "common"
KIND_BOND = "bond"
KIND_PREFERRED = "preferred"
KIND_SPAC = "spac"
KIND_FUND = "fund"
KIND_UNKNOWN = "unknown"

BOND_RE = re.compile(
    r"\bnotes?\b|\bdebentures?\b|\bsubordinated\b|\bcapital obligation\b|\betns?\b", re.I
)
PREF_RE = re.compile(r"\b(?:pfd|prd|preferred|preference)\b", re.I)
COMMON_RE = re.compile(r"\bcommon stock\b|\bordinary shares?\b", re.I)
SPAC_RE = re.compile(r"\bacquisition\b|\bspac\b|\bblank check\b", re.I)
SPAC_SERIES_RE = re.compile(
    r"\b[ivx]{1,4}\b,?(?: (?:inc|ltd|corp|co)\.?)?\s*-?\s*class a ordinary shares?\b", re.I
)
FUND_RE = re.compile(r"\bfunds?\b|\bclosed[- ]end\b|\bbusiness development compan|\bbdc\b", re.I)
NOT_FUND_RE = re.compile(
    r"\breit\b|\brealty\b|\bpropert(?:y|ies)\b|\breal estate\b|\bre finance\b|\bmortgage\b"
    r"|\blodging\b|\bhotels?\b|\bhospitality\b|\bresidential\b|\bindustrial\b"
    r"|\bself storage\b|\bnet lease\b|\bhomes\b|\boffice\b"
    r"|\bban(?:k|corp|cshares)\b|\btrust (?:company|corporation)\b|\broyalty\b",
    re.I,
)
MUNI_RE = re.compile(r"\bmunicipals?\b|\bportfolio\b", re.I)
TRUSTISH_RE = re.compile(r"\btrust\b|\bbeneficial interests?\b|\bsbi\b", re.I)
CEF_WORD_RE = re.compile(
    r"\bincome\b|\bdividend\b|\bequity\b|\bterm\b|\bopportunit|\bresources\b|\bsciences\b"
    r"|\btechnology\b|\butility\b|\binfrastructure\b|\bmicro-cap\b|\bsmall-cap\b|\bgold\b"
    r"|\bcredit\b|\byield\b|\bfloating\b|\bduration\b|\ballocation\b|\bstrateg|\bpremium\b"
    r"|\benhanced\b|\binvestors\b|\btotal return\b|\bmulti-sector\b|\bconvertible\b",
    re.I,
)
CEF_SPONSOR_RE = re.compile(
    r"^(?:blackrock|nuveen|eaton vance|pimco|gabelli|gamco|calamos|abrdn|aberdeen|royce"
    r"|western asset|cohen & steers|doubleline|virtus|john hancock|templeton|neuberger|dws"
    r"|allspring|invesco|liberty all-star|kayne anderson|guggenheim|nyli|pgim|saba|rivernorth"
    r"|thornburg|xai|reaves|duff & phelps|cornerstone|highland|bny mellon|lmp"
    r"|columbia seligman|eagle point)\b",
    re.I,
)


def classify_name(symbol: str, security_name: str | None) -> str:
    """상장 목록 이름으로 본 종류. 이름이 없으면 ``unknown``, 단 ``$`` 심볼은 우선주."""
    name = (security_name or "").strip()
    if not name:
        return KIND_PREFERRED if "$" in symbol else KIND_UNKNOWN
    if BOND_RE.search(name):
        return KIND_BOND
    if "$" in symbol:
        return KIND_PREFERRED
    if PREF_RE.search(name) and not COMMON_RE.search(name):
        return KIND_PREFERRED
    if SPAC_RE.search(name) or SPAC_SERIES_RE.search(name):
        return KIND_SPAC
    if FUND_RE.search(name):
        return KIND_FUND
    if NOT_FUND_RE.search(name):
        return KIND_COMMON
    if MUNI_RE.search(name):
        return KIND_FUND
    if CEF_WORD_RE.search(name) and (TRUSTISH_RE.search(name) or CEF_SPONSOR_RE.search(name)):
        return KIND_FUND
    return KIND_COMMON


# --- 워런트·권리·유닛 (규칙 6) ----------------------------------------------------

_WARRANT_RE = re.compile(r"\bwarrants?\b", re.I)
_RIGHT_RE = re.compile(r"\brights?\b", re.I)
_RIGHT_OK_RE = re.compile(r"\brights? (?:agreement|plan)\b", re.I)
_UNIT_RE = re.compile(r"\bunits?\b", re.I)
#: 보통주를 뜻하는 Units. ``Common Units``·``Limited Partner(ship) Units`` 같은 MLP·운용사.
_UNIT_OK_RE = re.compile(
    r"\bcommon units?\b|\blimited partner(?:ship)? (?:interests? )?units?\b"
    r"|\blimited partnership units?\b|\blp units?\b",
    re.I,
)


def token_other(security_name: str | None) -> str | None:
    """이름에 단어로 warrant·right·unit 이 있으면 그 종류, 없으면 ``None``.

    **단어 경계로 찾는다.** ``United``·``Unity`` 가 ``unit`` 에 안 걸린다.
    ``right to receive`` 같은 ADR 설명은 ``rights agreement|plan`` 만 예외로 둔다.
    """
    if not security_name:
        return None
    low = security_name.lower()
    if _WARRANT_RE.search(low):
        return "warrant"
    if _RIGHT_RE.search(low) and not _RIGHT_OK_RE.search(low):
        return "right"
    if _UNIT_RE.search(low) and not _UNIT_OK_RE.search(low):
        return "unit"
    return None


# --- 발행사 이름 (규칙 7·11) -------------------------------------------------------

COMMODITY_RE = re.compile(r"\b(trust|etf|fund|etn|shares|index)\b|bullion|currencyshares", re.I)


def issuer_name_kind(symbol: str, name: str | None) -> str | None:
    """발행사 이름만 보고 낸 종류. 상장 목록 이름이 없을 때 쓴다 (규칙 11).

    펀드는 **강한 규칙**(``fund``·``closed-end``·BDC 단어)만 본다.
    """
    if not isinstance(name, str):
        return None
    if BOND_RE.search(name):
        return KIND_BOND
    if "$" in symbol or (PREF_RE.search(name) and not COMMON_RE.search(name)):
        return KIND_PREFERRED
    if SPAC_RE.search(name) or SPAC_SERIES_RE.search(name):
        return KIND_SPAC
    if FUND_RE.search(name):
        return KIND_FUND
    return KIND_COMMON


# --- 이름 비교 ---------------------------------------------------------------------

_STOP = frozenset(
    """INC CORP CORPORATION CO COMPANY LTD LIMITED PLC LLC LP L P SA NV AG SE THE COMMON STOCK
    STOCKS SHARES SHARE ORDINARY CLASS CL A B C D ADS ADR AMERICAN DEPOSITARY NEW UNITS UNIT
    WARRANT WARRANTS RIGHT RIGHTS BENEFICIAL INTEREST INT SER SERIES PREFERRED HLDGS HLD HOLDINGS
    HOLDING GROUP GRP AND OF EACH REPRESENTING SUBORDINATED VOTING CAPITAL""".split()
)
_TOKEN_RE = re.compile(r"[A-Z0-9]+")


def norm(name: str | None) -> tuple[str, ...]:
    """이름을 비교용 토큰으로. `` - `` 뒤(증권 설명)는 버린다."""
    if name is None:
        return ()
    text = name.upper().split(" - ")[0].replace("&", " ")
    return tuple(t for t in _TOKEN_RE.findall(text) if t not in _STOP)


def norm_full(name: str | None) -> tuple[str, ...]:
    """``norm`` 인데 `` - `` 양쪽을 다 둔다. 발행사 접두 변경을 가려내려는 것이다."""
    if name is None:
        return ()
    return norm(name.replace(" - ", " "))


def big_name_change(a: tuple[str, ...], b: tuple[str, ...]) -> bool:
    """상장 목록 이름이 **다른 회사 이름으로** 바뀌었나 (신호 N, 01 §5.7 1.1).

    보정 둘이 있다. ① 한쪽 토큰 집합이 다른 쪽을 포함하면(발행사 접두 변경,
    ``Credit Suisse AG - VelocityShares ...``) 아니다. ② 첫 토큰의 접두 일치는
    5자 이상일 때만 같은 이름으로 본다(``SPAC``·``SPACE`` 가 같아지는 것을 막는다).
    """
    if not a or not b:
        return False
    if a[0] == b[0]:
        return False
    f, g = a[0], b[0]
    if len(f) >= 5 and len(g) >= 5 and (f.startswith(g) or g.startswith(f)):
        return False
    sa, sb = set(a), set(b)
    if (sa <= sb and len(sa) >= 2) or (sb <= sa and len(sb) >= 2):
        return False
    return len(sa & sb) / len(sa | sb) < 0.5


def jaccard(a: str | None, b: str | None) -> float | None:
    """상장 목록 이름과 발행사 이름의 토큰 자카드. 한쪽이 비면 ``None``."""
    if not isinstance(a, str) or not isinstance(b, str):
        return None
    ta, tb = set(norm(a)), set(norm(b))
    if not ta or not tb:
        return None
    return len(ta & tb) / len(ta | tb)


# --- FTD 설명 (B1) ----------------------------------------------------------------

_LP_UNIT_RE = re.compile(
    r"\bUNIT (?:LTD|BEN|REPSTG|REP)|\b(?:INT|LP|LTD|PARTNERS?|PARTN|L\.?P\.?) UNITS?\b"
    r"|\bUNITS? (?:LTD|BEN|REPSTG|LP|L\.P|REPR|REPRESENTING)|\bCOM UNIT"
)
_FTD_SPAC = re.compile(r"\bACQUISITION\b|\bACQ\b|\bACQ\.CORP\b")
_FTD_WT = re.compile(r"\bWT|WARRANT|RIGHT")
_FTD_WT2 = re.compile(r"\bWTS?\b|\bWARRANTS?\b|\bWRNT\b|\bRTS?\b|\bRIGHTS?\b")
_FTD_RIGHT_OK = re.compile(r"\bRIGHTS? (?:AGREEMENT|PLAN)\b")
_FTD_UNIT = re.compile(r"\bUNITS?\b")
_FTD_UNIT_OK = re.compile(
    r"\bBEN\b|\bLTD PART|\bLIMITED PART|\bINT\b|ROYALTY|\bRTY\b|\bPARTNERS|\bLP\b|\bL\.?P\.?\b"
)
_FTD_PREF = re.compile(r"\bPFD\b|\bPERP|\bREPSTG\b|\bDEP SHS\b|\bDEP SH\b|\bDEPOSITARY SH")
_FTD_ADR = re.compile(r"\bADS\b|\bADR\b")
_FTD_PREF2 = re.compile(r"(?<!^)\bPREFERRED\b|\bPREF\b")
_FTD_BOND_STRONG = re.compile(
    r"\bJR SUB|\bSUB (?:N|D|NT|DEB)|\bF(?:I?XD?|IXED) RATE|\bGTD\b|\bSR$|\bMICROSECTORS"
    r"|\bBANK MONTREAL|\bTANGIBLE\b|\bSHS REPS?T\b"
)
_FTD_COM = re.compile(r"\bCOM\b|\bCL [A-C]\b")
_FTD_BOND = re.compile(
    r"\bNTS?\b|\bNOTES?\b|\bDEBS?\b|\bDEBENTURES?\b|\bETNS?\b|\bETRACS\b|\bIPATH\b|\bDUE\b"
)
_FTD_COUPON = re.compile(r"\d\.\d+%")
_FTD_ETF = re.compile(r"\bETF\b")
_FTD_FUNDISH = re.compile(
    r"\bETF\b|\bFD\b|\bFDS\b|\bFUNDS?\b|\bISHARES\b|\bSPDR\b|\bPROSHARES\b|\bVANGUARD\b"
    r"|\bINVESCO\b|\bTRUST\b|\bTR\b|\bETFS\b"
)

#: ``issuer_only`` 에서 뺄 FTD 설명 종류 (B1). 펀드·ETF 꼴은 안 뺀다 — 63멤버월 중 62개가 보통주다.
FTD_EXCLUDE_KINDS = frozenset({"bond", "preferred", "warrant_right_unit", "spac"})


def ftd_kind(description: str | None) -> str | None:
    """FTD 설명(30자로 잘림)에서 찾은 종류. 설명이 없으면 ``None``.

    ``bond``·``preferred``·``warrant_right_unit``·``spac`` 이 거를 대상이다.
    ``fund_like`` 는 보기만 하고 안 거른다. 나머지는 ``common_candidate`` 다.
    패턴 일부는 ``issuer_only`` 누출 예를 보고 더했다(표본 안 값, 설계 7장).
    """
    if not isinstance(description, str) or not description.strip():
        return None
    u = description.upper().strip()
    if _FTD_SPAC.search(u) and not _FTD_WT.search(u):
        return "spac"
    if _FTD_WT2.search(u) and not _FTD_RIGHT_OK.search(u):
        return "warrant_right_unit"
    if _FTD_UNIT.search(u) and not _LP_UNIT_RE.search(u) and not _FTD_UNIT_OK.search(u):
        return "warrant_right_unit"
    if _FTD_PREF.search(u) and not _FTD_ADR.search(u):
        return "preferred"
    if _FTD_PREF2.search(u):
        return "preferred"
    if _FTD_BOND_STRONG.search(u) and not _FTD_COM.search(u):
        return "bond"
    if _FTD_BOND.search(u):
        return "bond"
    if _FTD_COUPON.search(u) and not _FTD_ETF.search(u):
        return "bond"
    if _FTD_FUNDISH.search(u):
        return "fund_like"
    return "common_candidate"


# --- 심볼 접미 보조 신호 (B1) -----------------------------------------------------

#: Nasdaq 다섯째 글자. G·H·I(전환사채), M·N(우선주), T·W·Z(권리·워런트 계열), U(유닛), R(권리).
#: A·B·K·Y 는 보통주 비율이 96~100% 라 안 거르고, L·O·P 는 보통주가 섞여 안 쓴다.
SUFFIX5_NONCOMMON = frozenset("GHIMNRTUWZ")
_FIVE_LETTERS = re.compile(r"[A-Z]{5}")
_DOT_NONCOMMON = re.compile(r"\.(?:U|W|WS|RT)$")


def suffix_noncommon(symbol: str) -> bool:
    """심볼 꼴이 비보통주를 가리키나. ``$`` 포함, 5글자 끝 글자, ``.U``·``.W``."""
    if "$" in symbol:
        return True
    if _FIVE_LETTERS.fullmatch(symbol) and symbol[-1] in SUFFIX5_NONCOMMON:
        return True
    return bool(_DOT_NONCOMMON.search(symbol))


# --- 보통주 세부 꼴 (security_type 표기용) ------------------------------------------

_ADR_RE = re.compile(r"\bamerican depositary\b|\badrs?\b|\bdepositary shares?\b", re.I)
_REIT_RE = re.compile(r"\breit\b|\breal estate\b|\brealty\b|\bpropert(?:y|ies)\b", re.I)
_COMMON_EQUITY_RE = re.compile(
    r"\bcommon units?\b|\blimited partner(?:ship)? (?:interests? )?units?\b"
    r"|\blimited partnership units?\b|\blp units?\b|\bbeneficial interests?\b"
    r"|\bmembership interests?\b|\bcommon shares of beneficial interest\b",
    re.I,
)


def common_subtype(security_name: str | None) -> str:
    """보통주로 판정된 증권의 세부 꼴. 판정(include)에는 안 쓰고 표기만 한다."""
    if not security_name:
        return "common"
    if _ADR_RE.search(security_name):
        return "adr"
    if _COMMON_EQUITY_RE.search(security_name):
        return "common_equity"
    if _REIT_RE.search(security_name):
        return "reit"
    return "common"
