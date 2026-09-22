"""**그날의 날짜를 기본값에 굳히지 마라** — 이 코드베이스의 되풀이 실수다.

세 번 났다.

1. C8 `weekly_macro` 가 스냅샷을 **오늘 날짜로** 찾았다. 캘린더·거시는 매일
   다시 굳히지 않으므로 늘 "없음"이 되어 주 1회가 매일 1회가 됐다 (04 §2.17)
2. `build_universe_daily` 가 **입력** 스냅샷을 **출력** 파티션 키로 찾았다.
   표마다 굳는 주기가 달라 재판정이 아예 안 돌았다 (2026-09-21)
3. 그리고 이 파일이 막는 것 — **끝 날짜가 기본값에 박혀 있었다**

   ``universe/build.py``   ``end = "2026-09-09"``   C4 를 돌린 날의 가격 최대일
   ``calendars.py``        ``end = "2026-12-31"``   C5 를 돌린 해의 연말
   ``validate/coverage.py````end = "2026-09-09"``

   유니버스는 **이미** 그 날짜에서 안 자라고 있었고, 캘린더는 **2027-01-01 에**
   새 세션이 0건이 되어 하루 실행을 조용히 멈출 참이었다.

**끝 날짜는 데이터가 정한다** (``prices_daily`` 의 마지막 날) **아니면 지평이
정한다** (오늘 + N년). 코드에 박힌 날짜는 반드시 언젠가 지나간다.
"""

from __future__ import annotations

import ast
import datetime as dt
import importlib
import inspect
import pkgutil
import re

import pytest

import collector.us

#: ``YYYY-MM-DD`` 로 보이는 문자열.
_DATE_TEXT = re.compile(r"^\d{4}-\d{2}-\d{2}$")

#: **시작점은 박아도 된다.** 원천 이력이 언제부터인가는 과거 사실이라 안 바뀐다.
#: 끝은 다르다 — 반드시 지나간다.
_ALLOWED: frozenset[tuple[str, str]] = frozenset(
    {
        ("load_trading_calendar", "start"),  # 원천 이력 시작
        ("build_universe_daily", "start"),  # USABLE_FROM (03 §5.3)
        ("scan", "start"),
        # `since` 는 하한이다. 검정 구간이 2018 부터라 그 앞은 받을 이유가 없다
        ("load_submissions", "since"),
        ("load_index_constituents", "since"),
    }
)


def _public_functions():
    for mod in pkgutil.walk_packages(collector.us.__path__, "collector.us."):
        module = importlib.import_module(mod.name)
        for name, obj in vars(module).items():
            if name.startswith("_") or not inspect.isfunction(obj):
                continue
            if obj.__module__ != module.__name__:
                continue
            yield mod.name, name, obj


def _frozen_date_defaults():
    for mod_name, fn_name, fn in _public_functions():
        for param_name, param in inspect.signature(fn).parameters.items():
            default = param.default
            looks_like_date = isinstance(default, dt.date) or (
                isinstance(default, str) and _DATE_TEXT.match(default)
            )
            if looks_like_date and (fn_name, param_name) not in _ALLOWED:
                yield f"{mod_name}.{fn_name}({param_name}={default!r})"


def test_no_function_freezes_a_date_in_its_default():
    """끝 날짜·기준일을 기본값에 박은 함수가 없다."""
    frozen = sorted(_frozen_date_defaults())
    assert not frozen, (
        "날짜가 기본값에 박혀 있다. 끝은 데이터(prices 최대일)나 지평(오늘+N년)이 "
        "정하게 하라. 시작점처럼 과거 사실이면 _ALLOWED 에 이유와 함께 적어라:\n  "
        + "\n  ".join(frozen)
    )


def test_allowlist_has_no_stale_entries():
    """``_ALLOWED`` 에 이제는 없는 함수·인자가 남아 있지 않다."""
    live = {
        (fn_name, p)
        for _, fn_name, fn in _public_functions()
        for p in inspect.signature(fn).parameters
    }
    assert not _ALLOWED - live


# --- 네 번째: 기본값이 아니라 호출부에 박힌 끝 -----------------------------
#
# 위 검사는 함수 **기본값**만 본다. SEC 분기 추출 함수 셋
# (``extract_filings_sub``·``extract_midas``·``extract_insider``)은 끝 분기가
# 함수 **본문**에 ``quarters((2018, 3), (2026, 2))``처럼 튜플 리터럴로 박혀
# 있었다 — 매개변수 기본값이 아니라 호출부라 위 검사로는 못 잡았다. 2026q3
# zip을 받아도 레이크에 안 들어가던 원인이었다 (2026-09-23).


def _quarters_call_sites():
    """``quarters(시작, 끝)`` 호출부를 소스에서 찾아 ``끝`` 인자 노드를 낸다."""
    for mod in pkgutil.walk_packages(collector.us.__path__, "collector.us."):
        module = importlib.import_module(mod.name)
        source_file = module.__file__
        if not source_file:
            continue
        tree = ast.parse(open(source_file, encoding="utf-8").read(), filename=source_file)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or len(node.args) < 2:
                continue
            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if name == "quarters":
                yield mod.name, node.lineno, node.args[1]


def _literal_int_pair(node: ast.AST) -> tuple[int, int] | None:
    """``(2026, 2)``처럼 정수 둘짜리 튜플 리터럴이면 값을, 아니면 ``None``."""
    if not (isinstance(node, ast.Tuple) and len(node.elts) == 2):
        return None
    values: list[int] = []
    for elt in node.elts:
        if isinstance(elt, ast.Constant) and isinstance(elt.value, int):
            values.append(elt.value)
        else:
            return None
    return (values[0], values[1])


#: ``quarters(시작, 끝)`` 호출부에서 끝이 튜플 리터럴이어도 되는 자리.
#: 지금은 없다 — 끝은 항상 ``latest_available_quarter``(raw/) 나 오늘짜로
#: 계산한 변수(예: ``_last_closed_quarter``)에서 나와야 한다.
_ALLOWED_QUARTER_CALL_ENDS: frozenset[tuple[str, int]] = frozenset()


def test_no_quarters_call_freezes_an_end_literal():
    """``quarters(시작, (연도, 분기))``처럼 끝이 튜플 리터럴로 박힌 호출이 없다.

    시작이 튜플 리터럴인 것은 상관없다 — 시작점은 과거 사실이라 박아도 된다
    (``_ALLOWED``와 같은 원칙). 문제는 **끝**이 리터럴일 때다.
    """
    frozen = []
    for mod_name, lineno, end_node in _quarters_call_sites():
        end = _literal_int_pair(end_node)
        if end is None or (mod_name, lineno) in _ALLOWED_QUARTER_CALL_ENDS:
            continue
        frozen.append(f"{mod_name}:{lineno} quarters(..., end={end!r})")
    frozen.sort()
    assert not frozen, (
        "quarters() 호출의 끝이 튜플 리터럴로 박혀 있다. raw/ 에 실제로 있는 "
        "분기(latest_available_quarter)나 오늘짜로 계산한 변수로 정하라:\n  " + "\n  ".join(frozen)
    )


# --- 고친 자리가 실제로 어떻게 도나 ---------------------------------------


def test_calendar_default_end_follows_today():
    """캘린더 기본 끝이 **오늘 기준**으로 앞을 본다."""
    from collector.us import calendars

    assert calendars.HORIZON_YEARS >= 2
    sig = inspect.signature(calendars.load_trading_calendar)
    assert sig.parameters["end"].default is None


def test_universe_and_scan_default_end_is_data_driven():
    from collector.us.universe import build
    from collector.us.validate import coverage

    assert inspect.signature(build.build_universe_daily).parameters["end"].default is None
    assert inspect.signature(coverage.scan).parameters["end"].default is None


def test_sessions_through_refuses_to_run_past_the_calendar(tmp_path):
    """**조용히 적게 돌려주지 않는다.** 그러면 수집이 exit 0 으로 멈춘다."""
    import pyarrow as pyar
    import pyarrow.parquet as pq

    from collector.lake import DataRoot
    from collector.us.ops import daily

    root = DataRoot(tmp_path)
    d = root.derived / "snapshots" / "trading_calendar" / "snapshot_date=2026-09-19"
    d.mkdir(parents=True)
    pq.write_table(
        pyar.table(
            {"date": pyar.array([dt.date(2026, 12, 30), dt.date(2026, 12, 31)], pyar.date32())}
        ),
        d / "part.parquet",
    )

    assert daily.calendar_end(root) == dt.date(2026, 12, 31)
    # 캘린더 안이면 돈다
    assert daily.sessions_through(root, until=dt.date(2026, 12, 31)) == [
        dt.date(2026, 12, 30),
        dt.date(2026, 12, 31),
    ]
    # 하루만 넘어가도 멈추고 무엇을 하라고 말한다
    with pytest.raises(daily.CalendarExhaustedError, match="us-calendar build"):
        daily.sessions_through(root, until=dt.date(2027, 1, 2))


def test_calendar_end_is_none_when_nothing_is_stored(tmp_path):
    from collector.lake import DataRoot
    from collector.us.ops import daily

    assert daily.calendar_end(DataRoot(tmp_path)) is None
