from __future__ import annotations

import pytest

from tests.helpers.db_guard import dsn_is_local


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://u:p@localhost:5432/mydb",
        "postgresql://u:p@127.0.0.1/mydb",
        "postgresql://u@[::1]/mydb",
        "dbname=mydb user=u",
        "host=/tmp dbname=mydb",
        "host=localhost,127.0.0.1 dbname=mydb",
    ],
)
def test_local_dsns_are_allowed(dsn: str) -> None:
    assert dsn_is_local(dsn)


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://u:p@sj2-server:5432/krx_data",
        "host=10.0.0.5 dbname=krx_data",
        "host=localhost,sj2-server dbname=mydb",
    ],
)
def test_remote_dsns_are_refused(dsn: str) -> None:
    assert not dsn_is_local(dsn)
