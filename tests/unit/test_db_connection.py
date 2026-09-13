from collector.kr.infra.db_postgres.connection import _mask_dsn


def test_mask_dsn_hides_credentials() -> None:
    masked = _mask_dsn("postgresql://collector:secret@localhost:5432/krx_db")

    assert masked == "postgresql://***@localhost:5432/krx_db"
    assert "collector" not in masked
    assert "secret" not in masked


def test_mask_dsn_hides_percent_encoded_credentials() -> None:
    masked = _mask_dsn("postgresql://collector:p%40ss%21@localhost:5432/krx_db")

    assert masked == "postgresql://***@localhost:5432/krx_db"
    assert "collector" not in masked
    assert "p%40ss%21" not in masked


def test_mask_dsn_without_authority_is_fully_masked() -> None:
    assert _mask_dsn("dbname=krx_db user=collector password=secret") == "***"
