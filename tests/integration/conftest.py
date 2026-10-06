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


# tryfirst: xdist가 같은 훅에서 xdist_group을 읽어 nodeid에 "@db"를 붙인다. 이 conftest는
# 그보다 먼저 등록돼 나중에 불리므로, 앞당기지 않으면 표시가 늦어 묶음이 안 된다.
@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if _HERE in item.path.parents:
            item.add_marker(pytest.mark.xdist_group("db"))
