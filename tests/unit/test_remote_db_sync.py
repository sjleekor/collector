from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from collector.kr.infra.db_postgres import remote_sync
from collector.kr.infra.db_postgres.remote_sync import (
    PIPELINE_FULL_REFRESH_TABLES,
    SYNC_TABLE_SPECS,
    DatabaseTable,
    _adapt_insert_row,
    _build_conflict_action,
    _build_copy_select_sql,
    _build_insert_select_from_stage_statement,
    _copy_status_row_count,
    _daily_ohlcv_checkpoint_payload,
    _open_ssh_tunnel,
    _prune_missing_rows,
    _row_conflict_key,
    _select_required_public_tables,
    _select_resume_cursor,
    _select_sync_specs,
    _sort_tables_by_fk_dependencies,
    _sync_selected_public_tables_to_local,
    _upsert_rows,
    _validate_full_database_table_sets,
    _validate_no_external_fk_children,
    _validate_prune_external_fk_children,
    load_remote_db_info,
    reset_local_public_tables,
    sync_remote_tables_to_local,
    validate_remote_sync_options,
)
from collector.kr.service import sync_local_db


def test_load_remote_db_info_parses_expected_fields(tmp_path: Path) -> None:
    info_file = tmp_path / "db_info"
    info_file.write_text(
        "\n".join(
            [
                "# sj2-server DB Information",
                "Server Host: sj2-server",
                "Host Port: 5432",
                "Container: sdc-postgres",
                "",
                "# Credentials",
                "POSTGRES_USER: krx_user",
                "POSTGRES_PASSWORD: secret!",
                "POSTGRES_DB: krx_data",
            ]
        ),
        encoding="utf-8",
    )

    info = load_remote_db_info(info_file)

    assert info.host == "sj2-server"
    assert info.port == 5432
    assert info.container == "sdc-postgres"
    assert info.db_name == "krx_data"
    assert info.to_dsn() == "postgresql://krx_user:secret%21@sj2-server:5432/krx_data"


def test_load_remote_db_info_requires_expected_fields(tmp_path: Path) -> None:
    info_file = tmp_path / "db_info"
    info_file.write_text("Server Host: sj2-server\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Missing required remote DB fields"):
        load_remote_db_info(info_file)


def test_sync_remote_tables_requires_positive_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        sync_remote_tables_to_local(
            remote_dsn="postgresql://remote",
            local_dsn="postgresql://local",
            batch_size=0,
            full_refresh=False,
        )


def test_all_tables_sync_requires_full_refresh() -> None:
    with pytest.raises(ValueError, match="all_tables sync requires full_refresh=True"):
        sync_remote_tables_to_local(
            remote_dsn="postgresql://remote",
            local_dsn="postgresql://local",
            batch_size=1000,
            full_refresh=False,
            all_tables=True,
        )


def test_sync_options_reject_all_tables_with_explicit_tables() -> None:
    with pytest.raises(ValueError, match="cannot be combined"):
        validate_remote_sync_options(
            batch_size=1000,
            full_refresh=True,
            all_tables=True,
            tables=("daily_ohlcv",),
        )


def test_service_validates_before_full_refresh_reset(monkeypatch, tmp_path: Path) -> None:
    def fail_if_reset(_local_dsn: str) -> int:
        raise AssertionError("reset should not run before validation")

    monkeypatch.setattr(sync_local_db, "reset_local_public_tables", fail_if_reset)

    result = sync_local_db.sync_remote_db_to_local(
        local_dsn="postgresql://local",
        remote_db_info_path=tmp_path / "missing-db-info",
        batch_size=1000,
        full_refresh=True,
        all_tables=True,
        tables=("daily_ohlcv",),
    )

    assert result.error == "all_tables sync cannot be combined with explicit tables"


def test_managed_mirror_tables_exclude_local_audit_tables() -> None:
    pipeline_names = [table.name for table in PIPELINE_FULL_REFRESH_TABLES]
    spec_names = {spec.name for spec in SYNC_TABLE_SPECS}

    # The mirror only carries raw + the shared common_feature_series config now;
    # the derived/catalog tables are recomputed by the DuckDB marts (refactor §5.2).
    # +daily_market_cap (N1), +dart_employee_raw / dart_governance_raw (N6),
    # +dart_corp_profile_history (F-1)
    assert len(pipeline_names) == 19
    assert set(pipeline_names).issubset(spec_names)  # every mirrored table has a spec
    assert "ingestion_runs" not in pipeline_names
    assert "sync_checkpoints" not in pipeline_names
    assert "krx_security_flow_raw" in pipeline_names
    assert "common_feature_series" in pipeline_names
    # decommissioned -> no longer mirrored.
    assert "stock_metric_fact" not in pipeline_names
    assert "common_feature_daily_fact" not in pipeline_names
    assert "metric_catalog" not in pipeline_names


def test_daily_market_cap_spec_matches_ddl_and_cursor() -> None:
    # N1.  The mirror's select/insert column lists are explicit, so a column
    # added to the DDL but not here is dropped silently — no test catches that
    # for free the way the profile catalog does.  Pin the shape.
    spec = next(spec for spec in SYNC_TABLE_SPECS if spec.name == "daily_market_cap")

    select_columns = tuple(col.strip() for col in spec.select_list.split(","))
    # insert_columns must be a positional prefix of select_list
    assert select_columns[: len(spec.insert_columns)] == spec.insert_columns

    assert spec.insert_columns == (
        "trade_date",
        "ticker",
        "market",
        "source_close",
        "market_cap",
        "trading_value",
        "listed_shares",
        "volume",
        "source",
        "fetched_at",
        # K-4: the unadjusted OHLC that arrives with the Open API response.
        # Appended AFTER fetched_at so cursor_indexes below keeps pointing at
        # the same columns — inserting them mid-list would silently repoint
        # the resume cursor at source_open.
        "source_open",
        "source_high",
        "source_low",
    )
    assert spec.conflict_columns == ("trade_date", "ticker", "market")
    # PK columns are never in update_columns; every value column is.
    assert set(spec.update_columns) == set(spec.insert_columns) - set(spec.conflict_columns)
    # cursor_indexes point at (fetched_at, trade_date, ticker, market)
    assert tuple(select_columns[i] for i in spec.cursor_indexes) == (
        "fetched_at",
        "trade_date",
        "ticker",
        "market",
    )


def test_daily_market_cap_registered_in_every_required_place() -> None:
    # `01_implementation_checklist.md` §1: a new table must land in six places.
    # Places 2 (this module) and 3 (profile catalog) are importable from here.
    # Place 6 (research config, `research.etl.config.RAW_TABLES`) lives in
    # modeler/ since the project split (S5) — that check moved to modeler/'s
    # own test suite (S6).
    from collector.kr.infra.db_postgres.remote_sync import (
        PIPELINE_FULL_REFRESH_TABLE_NAMES,
    )
    from collector.kr.service.profiling import catalog

    assert "daily_market_cap" in PIPELINE_FULL_REFRESH_TABLE_NAMES
    assert "daily_market_cap" in catalog.known_tables()


def test_stock_master_spec_includes_new_date_columns() -> None:
    spec = next(spec for spec in SYNC_TABLE_SPECS if spec.name == "stock_master")

    select_columns = tuple(col.strip() for col in spec.select_list.split(","))
    assert select_columns[: len(spec.insert_columns)] == spec.insert_columns
    assert "listing_date" in spec.insert_columns
    assert "first_seen_date" in spec.insert_columns
    assert "listing_date" in spec.update_columns
    assert "first_seen_date" in spec.update_columns
    # updated_at cursor is unchanged at index 6.
    assert spec.cursor_indexes == (6, 0, 1)
    assert select_columns[6] == "updated_at"


def test_stock_master_snapshot_items_spec_includes_listing_date() -> None:
    spec = next(spec for spec in SYNC_TABLE_SPECS if spec.name == "stock_master_snapshot_items")

    select_columns = tuple(col.strip() for col in spec.select_list.split(","))
    # insert_columns strips the table alias prefix present in select_list.
    stripped = tuple(col.split(".")[-1] for col in select_columns)
    assert stripped[: len(spec.insert_columns)] == spec.insert_columns
    assert "listing_date" in spec.insert_columns
    assert "listing_date" in spec.update_columns
    # fetched_at is the joined parent watermark and moves from index 5 -> 6.
    assert spec.cursor_indexes == (6, 0, 1, 2)
    assert select_columns[6] == "s.fetched_at"


def test_security_flow_raw_uses_update_aware_incremental_cursor() -> None:
    pipeline_names = [table.name for table in PIPELINE_FULL_REFRESH_TABLES]
    assert "krx_security_flow_raw" in pipeline_names

    spec = next(spec for spec in SYNC_TABLE_SPECS if spec.name == "krx_security_flow_raw")
    assert spec.order_columns == ("fetched_at", "raw_id")
    assert spec.cursor_indexes == (9, 0)
    assert spec.conflict_columns == ("trade_date", "ticker", "market", "metric_code", "source")
    assert spec.json_columns == ("raw_payload",)


def test_partial_sync_includes_fk_dependency_closure() -> None:
    # common_feature_observation_raw FK-depends on common_feature_series, so a
    # partial sync of the observations pulls the series config first.
    selected_names = [spec.name for spec in _select_sync_specs(("common_feature_observation_raw",))]

    assert selected_names == [
        "common_feature_series",
        "common_feature_observation_raw",
    ]


def test_partial_sync_rejects_unknown_table() -> None:
    with pytest.raises(ValueError, match="Unsupported sync table"):
        _select_sync_specs(("missing_table",))


def test_special_common_feature_specs_cover_conflict_edge_cases() -> None:
    observation_spec = next(
        spec for spec in SYNC_TABLE_SPECS if spec.name == "common_feature_observation_raw"
    )
    input_spec = next(
        spec for spec in SYNC_TABLE_SPECS if spec.name == "common_feature_catalog_input"
    )

    assert observation_spec.conflict_constraint == "uq_common_feature_observation_raw"
    assert input_spec.update_columns == ()
    assert input_spec.do_nothing_when_no_update_columns is True
    assert input_spec.always_full_scan is True
    assert input_spec.prune_missing_after_full_scan is True


def test_copy_merge_specs_are_limited_to_update_aware_tables() -> None:
    copy_merge_specs = {spec.name for spec in SYNC_TABLE_SPECS if spec.copy_merge_enabled}

    assert copy_merge_specs == {
        "daily_ohlcv",
        "daily_market_cap",
        "krx_security_flow_raw",
        "dart_financial_statement_raw",
        "dart_share_count_raw",
        "dart_shareholder_return_raw",
        "dart_capital_change_raw",
        "dart_filing_receipt_raw",
        "dart_employee_raw",
        "dart_governance_raw",
        "dart_xbrl_document",
        "dart_xbrl_fact_raw",
        "dart_corp_profile_history",
        "stock_metric_fact",
        "common_feature_observation_raw",
        "common_feature_daily_fact",
    }


def test_small_catalog_specs_are_full_scan_pruned() -> None:
    pruned_specs = {spec.name for spec in SYNC_TABLE_SPECS if spec.prune_missing_after_full_scan}

    assert {
        "stock_master",
        "stock_master_snapshot",
        "stock_master_snapshot_items",
        "metric_catalog",
        "metric_mapping_rule",
        "common_feature_series",
        "common_feature_catalog",
        "common_feature_catalog_input",
    }.issubset(pruned_specs)


def test_raw_upsert_preserves_remote_surrogate_id(monkeypatch) -> None:
    spec = next(spec for spec in SYNC_TABLE_SPECS if spec.name == "krx_security_flow_raw")
    cursor = _FakeCursor([])
    connection = _FakeConnection(cursor)
    captured: dict[str, object] = {}

    def fake_execute_values(cur, statement, values, page_size) -> None:
        captured["cursor"] = cur
        captured["statement"] = statement
        captured["values"] = values
        captured["page_size"] = page_size

    monkeypatch.setattr(remote_sync.psycopg2.extras, "execute_values", fake_execute_values)

    _upsert_rows(
        local_conn=connection,
        spec=spec,
        rows=[
            (
                10,
                date(2026, 4, 17),
                "005930",
                "KOSPI",
                "foreign_net_buy_volume",
                "외국인 순매수 수량",
                123,
                "shares",
                "KRX",
                datetime(2026, 4, 18, 0, 0, tzinfo=UTC),
                {"raw": "payload"},
            )
        ],
    )

    assert spec.preserve_remote_surrogate_columns == ("raw_id",)
    assert "raw_id = EXCLUDED.raw_id" in captured["statement"]
    assert connection.commits == 1


def test_daily_ohlcv_conflict_action_preserves_stale_update_guard() -> None:
    spec = next(spec for spec in SYNC_TABLE_SPECS if spec.name == "daily_ohlcv")

    conflict_action = _build_conflict_action(spec)
    stage_statement = _build_insert_select_from_stage_statement(
        spec=spec,
        stage_table="remote_sync_stage_daily_ohlcv",
    )

    assert "ON CONFLICT (trade_date, ticker, market) DO UPDATE" in conflict_action
    assert "WHERE daily_ohlcv.fetched_at <= EXCLUDED.fetched_at" in conflict_action
    assert "WHERE daily_ohlcv.fetched_at <= EXCLUDED.fetched_at" in stage_statement


def test_build_copy_select_sql_quotes_cursor_and_limit_with_mogrify() -> None:
    spec = next(spec for spec in SYNC_TABLE_SPECS if spec.name == "krx_security_flow_raw")
    cursor = _FakeMogrifyCursor()

    statement = _build_copy_select_sql(
        remote_cur=cursor,
        spec=spec,
        cursor_values=(datetime(2026, 4, 18, 0, 0, tzinfo=UTC), 10),
        batch_size=50000,
    )

    assert statement.startswith("COPY (SELECT raw_id, trade_date")
    assert "WHERE (fetched_at, raw_id) > ('quoted-cursor')" in statement
    assert "LIMIT 50000" in statement
    assert statement.endswith(") TO STDOUT WITH (FORMAT CSV, NULL '\\N')")
    assert cursor.calls == [
        (
            "WHERE (fetched_at, raw_id) > (%s, %s)",
            (datetime(2026, 4, 18, 0, 0, tzinfo=UTC), 10),
        ),
        ("%s", (50000,)),
    ]


def test_prune_missing_rows_uses_temp_key_table_for_large_key_sets(monkeypatch) -> None:
    spec = next(spec for spec in SYNC_TABLE_SPECS if spec.name == "common_feature_catalog_input")
    keys = {(f"feature_{index:04d}", f"series_{index:04d}", "primary") for index in range(1001)}
    cursor = _FakeCursor([])
    connection = _FakeConnection(cursor)
    captured: dict[str, object] = {"calls": []}

    def fake_execute_values(cur, statement, values, page_size) -> None:
        captured["calls"].append(
            {
                "cursor": cur,
                "statement": statement,
                "values": values,
                "page_size": page_size,
            }
        )

    monkeypatch.setattr(remote_sync.psycopg2.extras, "execute_values", fake_execute_values)

    row = ("feature_a", "series_a", "primary")
    assert _row_conflict_key(spec=spec, row=row) == row

    _prune_missing_rows(local_conn=connection, spec=spec, keys=keys)

    execute_values_calls = captured["calls"]
    assert len(execute_values_calls) == 1
    assert execute_values_calls[0]["statement"] == (
        "INSERT INTO remote_sync_prune_keys (feature_code, series_id, role) VALUES %s"
    )
    assert set(execute_values_calls[0]["values"]) == keys
    assert execute_values_calls[0]["page_size"] == 1000
    statements = [statement for statement, _params in cursor.executed]
    assert any(
        "CREATE TEMP TABLE remote_sync_prune_keys ON COMMIT DROP" in statement
        for statement in statements
    )
    assert any(
        "DELETE FROM common_feature_catalog_input AS target" in statement
        for statement in statements
    )
    assert not any("FROM (VALUES" in statement for statement in statements)
    assert connection.commits == 1


def test_security_flow_raw_json_payload_is_adapted_for_execute_values() -> None:
    spec = next(spec for spec in SYNC_TABLE_SPECS if spec.name == "krx_security_flow_raw")
    row = (
        1,
        date(2026, 4, 17),
        "005930",
        "KOSPI",
        "foreign_net_buy_volume",
        "외국인 순매수 수량",
        123,
        "shares",
        "KRX",
        datetime(2026, 4, 18, 0, 0, tzinfo=UTC),
        {"raw": "payload"},
    )

    adapted = _adapt_insert_row(spec=spec, row=row)

    assert adapted[:-1] == row[:-1]
    assert adapted[-1].adapted == {"raw": "payload"}


def test_validate_full_database_table_sets_reports_mismatches() -> None:
    remote_tables = (
        DatabaseTable(schema="public", name="stock_master"),
        DatabaseTable(schema="public", name="daily_ohlcv"),
    )
    local_tables = (
        DatabaseTable(schema="public", name="stock_master"),
        DatabaseTable(schema="public", name="ingestion_runs"),
    )

    with pytest.raises(ValueError, match="missing locally: daily_ohlcv"):
        _validate_full_database_table_sets(remote_tables=remote_tables, local_tables=local_tables)


def test_select_required_public_tables_allows_extra_public_tables() -> None:
    required = (
        DatabaseTable(schema="public", name="stock_master"),
        DatabaseTable(schema="public", name="daily_ohlcv"),
    )
    remote_tables = required + (DatabaseTable(schema="public", name="custom_remote"),)
    local_tables = required + (DatabaseTable(schema="public", name="custom_local"),)

    selected = _select_required_public_tables(
        remote_tables=remote_tables,
        local_tables=local_tables,
        required_tables=required,
    )

    assert selected == required


def test_select_required_public_tables_reports_missing_targets() -> None:
    required = (
        DatabaseTable(schema="public", name="stock_master"),
        DatabaseTable(schema="public", name="daily_ohlcv"),
    )

    with pytest.raises(ValueError, match="missing locally: daily_ohlcv"):
        _select_required_public_tables(
            remote_tables=required,
            local_tables=(DatabaseTable(schema="public", name="stock_master"),),
            required_tables=required,
        )


def test_sort_tables_by_fk_dependencies_copies_parents_first() -> None:
    parent = DatabaseTable(schema="public", name="stock_master_snapshot")
    child = DatabaseTable(schema="public", name="stock_master_snapshot_items")
    unrelated = DatabaseTable(schema="public", name="daily_ohlcv")

    ordered = _sort_tables_by_fk_dependencies(
        tables=(child, unrelated, parent),
        dependencies=((child, parent),),
    )

    assert ordered.index(parent) < ordered.index(child)
    assert set(ordered) == {parent, child, unrelated}


def test_full_refresh_subset_rejects_external_fk_children() -> None:
    parent = DatabaseTable(schema="public", name="metric_catalog")
    child = DatabaseTable(schema="public", name="metric_mapping_rule")

    with pytest.raises(ValueError, match="Unsafe full-refresh table subset"):
        _validate_no_external_fk_children(tables=(parent,), dependencies=((child, parent),))

    _validate_no_external_fk_children(tables=(parent, child), dependencies=((child, parent),))


def test_pruning_subset_rejects_external_fk_children() -> None:
    parent = DatabaseTable(schema="public", name="metric_catalog")
    child = DatabaseTable(schema="public", name="metric_mapping_rule")
    parent_spec = next(spec for spec in SYNC_TABLE_SPECS if spec.name == "metric_catalog")
    child_spec = next(spec for spec in SYNC_TABLE_SPECS if spec.name == "metric_mapping_rule")

    with pytest.raises(ValueError, match="Unsafe pruning table subset"):
        _validate_prune_external_fk_children(
            specs=(parent_spec,),
            dependencies=((child, parent),),
        )

    _validate_prune_external_fk_children(
        specs=(parent_spec, child_spec),
        dependencies=((child, parent),),
    )


def test_copy_status_row_count_parses_copy_status() -> None:
    assert _copy_status_row_count("COPY 123") == 123
    assert _copy_status_row_count("") is None


def test_daily_ohlcv_checkpoint_payload_serializes_cursor() -> None:
    cursor = (
        datetime(2026, 4, 19, 0, 0, tzinfo=UTC),
        date(2026, 4, 18),
        "005930",
        "KOSPI",
    )

    payload = _daily_ohlcv_checkpoint_payload(cursor)

    assert payload == {
        "fetched_at": "2026-04-19T00:00:00+00:00",
        "trade_date": "2026-04-18",
        "ticker": "005930",
        "market": "KOSPI",
    }


def test_select_resume_cursor_prefers_furthest_cursor() -> None:
    checkpoint_cursor = (
        datetime(2026, 4, 19, 0, 0, tzinfo=UTC),
        date(2026, 4, 18),
        "005930",
        "KOSPI",
    )
    local_cursor = (
        datetime(2026, 4, 18, 23, 0, tzinfo=UTC),
        date(2026, 4, 18),
        "000660",
        "KOSPI",
    )

    assert _select_resume_cursor(checkpoint_cursor, local_cursor) == checkpoint_cursor
    assert _select_resume_cursor(None, local_cursor) == local_cursor
    assert _select_resume_cursor(checkpoint_cursor, None) == checkpoint_cursor


class _FakeCursor:
    def __init__(self, table_names: list[str]) -> None:
        self._table_names = table_names
        self.executed: list[tuple[object, object]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def execute(self, statement, params=None) -> None:
        self.executed.append((statement, params))

    def fetchall(self) -> list[tuple[str]]:
        return [(name,) for name in self._table_names]


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.autocommit = False
        self.closed = False
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def _identifier_pairs(statement) -> list[tuple[str, ...]]:
    """Pull the wrapped strings out of every Identifier in a Composable."""
    from psycopg2 import sql as psycopg2_sql

    pairs: list[tuple[str, ...]] = []
    if isinstance(statement, psycopg2_sql.Identifier):
        pairs.append(tuple(statement.strings))
    elif isinstance(statement, psycopg2_sql.Composed):
        for part in statement.seq:
            pairs.extend(_identifier_pairs(part))
    return pairs


def test_reset_local_public_tables_drops_only_pipeline_tables(monkeypatch) -> None:
    cursor = _FakeCursor(["stock_master", "daily_ohlcv", "custom_table"])
    connection = _FakeConnection(cursor)

    monkeypatch.setattr(remote_sync.psycopg2, "connect", lambda dsn: connection)

    dropped = reset_local_public_tables("postgresql://local")

    assert dropped == 2
    assert connection.autocommit is True
    assert connection.closed is True

    select_stmt, select_params = cursor.executed[0]
    assert "FROM pg_tables" in select_stmt
    assert select_params == (
        "public",
        [table.name for table in PIPELINE_FULL_REFRESH_TABLES],
    )

    drop_targets = [_identifier_pairs(stmt) for stmt, _ in cursor.executed[1:]]
    assert drop_targets == [
        [("public", "stock_master"), ("public", "daily_ohlcv")],
    ]


def test_reset_local_public_tables_handles_empty_db(monkeypatch) -> None:
    cursor = _FakeCursor([])
    connection = _FakeConnection(cursor)

    monkeypatch.setattr(remote_sync.psycopg2, "connect", lambda dsn: connection)

    assert reset_local_public_tables("postgresql://local") == 0
    assert len(cursor.executed) == 1


def test_selected_full_refresh_uses_no_commit_truncate(monkeypatch) -> None:
    local_conn = _FakeConnection(_FakeCursor([]))
    remote_conn = _FakeConnection(_FakeCursor([]))
    copied_tables: list[str] = []
    truncate_calls: list[dict[str, object]] = []
    sequence_tables: list[str] = []
    sequence_commit_counts: list[int] = []
    checkpoint_resets = 0

    def fake_truncate(*, local_conn, tables, commit=True) -> None:
        truncate_calls.append(
            {
                "local_conn": local_conn,
                "tables": tables,
                "commit": commit,
            }
        )

    def fake_copy_database_table(*, remote_conn, local_conn, table, columns) -> int:
        del remote_conn, local_conn, columns
        copied_tables.append(table.name)
        return 7

    def fake_sync_owned_sequences(*, remote_conn, local_conn, tables) -> int:
        del remote_conn
        sequence_commit_counts.append(local_conn.commits)
        sequence_tables.extend(table.name for table in tables)
        return 0

    def fake_reset_checkpoint(local_conn) -> None:
        nonlocal checkpoint_resets
        del local_conn
        checkpoint_resets += 1

    monkeypatch.setattr(remote_sync, "_prepare_local_full_refresh_session", lambda _conn: None)
    monkeypatch.setattr(remote_sync, "_list_foreign_key_dependencies", lambda _conn: ())
    monkeypatch.setattr(remote_sync, "_validate_full_database_columns", lambda **_kwargs: None)
    monkeypatch.setattr(remote_sync, "_truncate_database_tables", fake_truncate)
    monkeypatch.setattr(remote_sync, "_list_table_columns", lambda _conn, _table: ("id",))
    monkeypatch.setattr(remote_sync, "_copy_database_table", fake_copy_database_table)
    monkeypatch.setattr(remote_sync, "_sync_owned_sequences", fake_sync_owned_sequences)
    monkeypatch.setattr(
        remote_sync,
        "_reset_daily_ohlcv_checkpoint_from_local",
        fake_reset_checkpoint,
    )

    specs = (next(spec for spec in SYNC_TABLE_SPECS if spec.name == "daily_ohlcv"),)

    results = _sync_selected_public_tables_to_local(
        remote_conn=remote_conn,
        local_conn=local_conn,
        specs=specs,
    )

    assert results == {"daily_ohlcv": 7}
    assert copied_tables == ["daily_ohlcv"]
    assert sequence_tables == ["daily_ohlcv"]
    assert sequence_commit_counts == [1]
    assert checkpoint_resets == 1
    assert truncate_calls[0]["commit"] is False
    assert local_conn.commits == 2
    assert local_conn.rollbacks == 0


def test_open_ssh_tunnel_can_enable_compression(monkeypatch) -> None:
    commands: list[list[str]] = []

    class FakeProcess:
        stdout = None
        stderr = None

        def terminate(self) -> None:
            return None

        def wait(self, timeout=None) -> int:
            del timeout
            return 0

        def kill(self) -> None:
            return None

    def fake_popen(cmd, **kwargs):
        del kwargs
        commands.append(cmd)
        return FakeProcess()

    monkeypatch.setattr(remote_sync, "_find_free_port", lambda: 15432)
    monkeypatch.setattr(remote_sync, "_wait_for_local_port", lambda **_kwargs: None)
    monkeypatch.setattr(remote_sync.subprocess, "Popen", fake_popen)

    with _open_ssh_tunnel(
        ssh_host="whi@sj2-server",
        remote_port=5432,
        local_port=None,
        compression=True,
    ) as forwarded_port:
        assert forwarded_port == 15432

    assert commands[0][:4] == ["ssh", "-C", "-o", "ExitOnForwardFailure=yes"]

    with _open_ssh_tunnel(
        ssh_host="whi@sj2-server",
        remote_port=5432,
        local_port=15433,
        compression=False,
    ):
        pass

    assert commands[1][:3] == ["ssh", "-o", "ExitOnForwardFailure=yes"]
    assert "-C" not in commands[1]


class _FakeMogrifyCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def mogrify(self, query: str, params: tuple[object, ...]) -> bytes:
        self.calls.append((query, params))
        if query.startswith("WHERE"):
            return b"WHERE (fetched_at, raw_id) > ('quoted-cursor')"
        return str(params[0]).encode()
