"""integration 시험을 pytest-xdist 워커 하나에 묶는다.

이 디렉터리의 시험은 같은 PostgreSQL 표를 쓴다 — ``test_pipeline``은 시험마다
``stock_master`` 등을 ``TRUNCATE``하고, ``test_universe_snapshot_backfill_db``는 같은 표에
쓴다. 워커가 나뉘면 서로의 데이터를 지운다. ``--dist loadgroup``(pyproject addopts)에서
같은 ``xdist_group``은 한 워커에서 차례로 돈다.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_HERE = Path(__file__).parent

# Modules that TRUNCATE/DELETE on get_settings().db_dsn — see tests/helpers/db_guard.py.
_WRITES_SETTINGS_DB = frozenset({"test_pipeline.py", "test_universe_snapshot_backfill_db.py"})


@pytest.fixture(autouse=True)
def _refuse_non_local_db(request: pytest.FixtureRequest) -> None:
    # conftest autouse fixtures run before the module's own (clean_db, storage),
    # so the skip happens before any statement reaches the database.
    if request.node.path.name not in _WRITES_SETTINGS_DB:
        return
    from collector.kr.infra.config.settings import get_settings
    from tests.helpers.db_guard import dsn_is_local

    if not dsn_is_local(get_settings().db_dsn):
        pytest.skip("db_dsn이 로컬이 아니다 — 표를 비우는 시험이라 돌리지 않는다")


# tryfirst: xdist가 같은 훅에서 xdist_group을 읽어 nodeid에 "@db"를 붙인다. 이 conftest는
# 그보다 먼저 등록돼 나중에 불리므로, 앞당기지 않으면 표시가 늦어 묶음이 안 된다.
@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if _HERE in item.path.parents:
            item.add_marker(pytest.mark.xdist_group("db"))
