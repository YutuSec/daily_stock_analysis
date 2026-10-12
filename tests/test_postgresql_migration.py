from __future__ import annotations

from datetime import date, datetime
import sqlite3

import pytest
from sqlalchemy import Boolean, Column, Date, DateTime, Integer, MetaData, String, Table

from scripts.migrate_sqlite_to_postgres import (
    _convert_value,
    _row_digest,
    _sqlite_connection,
    _validate_target_url,
)
from scripts.backup_sqlite import backup_sqlite
from src.config import Config
from src.storage import _postgres_identifier


def test_database_url_takes_precedence_over_sqlite_path(tmp_path) -> None:
    config = Config(
        database_url="postgresql+psycopg://dsa@postgres:5432/daily_stock_analysis",
        database_path=str(tmp_path / "fallback.sqlite3"),
    )

    assert config.get_db_url() == config.database_url
    assert not (tmp_path / "fallback.sqlite3").exists()


@pytest.mark.parametrize(
    "value",
    [
        "mysql+pymysql://dsa@db/example",
        "postgresql+psycopg:///missing-host",
    ],
)
def test_database_url_rejects_unsupported_or_hostless_values(value) -> None:
    config = Config(database_url=value)
    with pytest.raises(ValueError):
        config.get_db_url()


def test_database_url_keeps_sqlite_compatibility(tmp_path) -> None:
    path = tmp_path / "nested" / "local.sqlite3"
    config = Config(database_url=None, database_path=str(path))

    assert config.get_db_url() == f"sqlite:///{path.absolute()}"
    assert path.parent.is_dir()


def test_target_url_requires_postgresql() -> None:
    with pytest.raises(ValueError, match="postgresql"):
        _validate_target_url("sqlite:///example.sqlite3")


def test_postgresql_identifier_shortening_is_stable_and_collision_resistant() -> None:
    first = "ix_decision_signal_report_type_market_stock_profile_action_horizon_phase"
    second = "ix_decision_signal_trace_type_market_stock_profile_action_horizon_phase"

    assert len(_postgres_identifier(first).encode("utf-8")) <= 63
    assert len(_postgres_identifier(second).encode("utf-8")) <= 63
    assert _postgres_identifier(first) == _postgres_identifier(first)
    assert _postgres_identifier(first) != _postgres_identifier(second)


def test_sqlite_source_integrity_is_fail_closed(tmp_path) -> None:
    invalid = tmp_path / "invalid.sqlite3"
    invalid.write_bytes(b"not a sqlite database")

    with pytest.raises((sqlite3.DatabaseError, RuntimeError)):
        _sqlite_connection(invalid)


def test_sqlite_backup_is_verified_and_never_overwrites(tmp_path) -> None:
    source = tmp_path / "source.sqlite3"
    with sqlite3.connect(source) as connection:
        connection.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT)")
        connection.execute("INSERT INTO sample (value) VALUES ('preserved')")

    destination = tmp_path / "backup"
    manifest = backup_sqlite(source, destination)

    assert manifest["quick_check"] == "ok"
    assert manifest["foreign_key_check"] == "ok"
    assert manifest["table_count"] == 1
    assert manifest["row_count"] == 1
    assert len(manifest["sha256"]) == 64
    with pytest.raises(FileExistsError):
        backup_sqlite(source, destination)


def test_migration_normalizes_sqlite_values_for_postgresql() -> None:
    metadata = MetaData()
    table = Table(
        "sample",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("enabled", Boolean),
        Column("day", Date),
        Column("created_at", DateTime),
        Column("name", String),
    )

    converted = {
        "id": _convert_value(table.c.id, "4"),
        "enabled": _convert_value(table.c.enabled, 1),
        "day": _convert_value(table.c.day, "2026-09-05"),
        "created_at": _convert_value(
            table.c.created_at,
            "2026-09-05 01:02:03.123456",
        ),
        "name": _convert_value(table.c.name, "牧原股份"),
    }

    assert converted == {
        "id": 4,
        "enabled": True,
        "day": date(2026, 9, 5),
        "created_at": datetime(2026, 9, 5, 1, 2, 3, 123456),
        "name": "牧原股份",
    }
    assert _row_digest([converted], list(converted)) == _row_digest(
        [dict(converted)],
        list(converted),
    )
