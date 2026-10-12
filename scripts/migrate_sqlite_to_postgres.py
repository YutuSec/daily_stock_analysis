#!/usr/bin/env python3
"""Fail-closed SQLite to PostgreSQL migration for DSA application tables.

The source is opened read-only and must pass SQLite integrity checks. The
target must be PostgreSQL and empty. All copies and sequence resets run in one
transaction; any mismatch rolls the target transaction back.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import sqlite3
import sys
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import Boolean, Date, DateTime, Integer, create_engine, func, select, text
from sqlalchemy.engine import Connection, URL, make_url
from sqlalchemy.sql.schema import Column, Table

from src.storage import (
    Base,
    POSTGRES_SCHEMA_ADVISORY_LOCK_ID,
    create_application_schema,
)


def _safe_url(value: str) -> str:
    return make_url(value).render_as_string(hide_password=True)


def _validate_target_url(value: str) -> URL:
    url = make_url(value)
    if url.get_backend_name() != "postgresql":
        raise ValueError("target URL must use the postgresql backend")
    if not url.host:
        raise ValueError("target PostgreSQL URL must include a hostname")
    return url


def _sqlite_connection(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(f"SQLite source does not exist: {path}")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    quick_check = [row[0] for row in connection.execute("PRAGMA quick_check")]
    if quick_check != ["ok"]:
        connection.close()
        raise RuntimeError(f"SQLite quick_check failed: {quick_check[:10]}")
    foreign_key_errors = list(connection.execute("PRAGMA foreign_key_check"))
    if foreign_key_errors:
        connection.close()
        raise RuntimeError(
            "SQLite foreign_key_check failed: "
            f"{[tuple(row) for row in foreign_key_errors[:10]]}"
        )
    return connection


def _source_tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }


def _quoted_sqlite_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _convert_value(column: Column[Any], value: Any) -> Any:
    if value is None:
        return None
    if isinstance(column.type, DateTime):
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if isinstance(column.type, Date):
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return date.fromisoformat(str(value)[:10])
    if isinstance(column.type, Boolean):
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)
    if isinstance(column.type, Integer):
        return int(value)
    return value


def _source_rows(
    connection: sqlite3.Connection,
    table: Table,
) -> list[dict[str, Any]]:
    column_names = [column.name for column in table.columns]
    columns_sql = ", ".join(_quoted_sqlite_identifier(name) for name in column_names)
    pk_names = [column.name for column in table.primary_key.columns]
    order_sql = ""
    if pk_names:
        order_sql = " ORDER BY " + ", ".join(
            _quoted_sqlite_identifier(name) for name in pk_names
        )
    query = (
        f"SELECT {columns_sql} FROM {_quoted_sqlite_identifier(table.name)}"
        f"{order_sql}"
    )
    return [
        {
            column.name: _convert_value(column, row[column.name])
            for column in table.columns
        }
        for row in connection.execute(query)
    ]


def _canonical_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return format(value, ".17g")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return str(value)


def _row_digest(rows: Iterable[dict[str, Any]], column_names: list[str]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        payload = [_canonical_value(row.get(name)) for name in column_names]
        digest.update(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _target_rows(connection: Connection, table: Table) -> list[dict[str, Any]]:
    statement = select(table)
    pk_columns = list(table.primary_key.columns)
    if pk_columns:
        statement = statement.order_by(*pk_columns)
    return [dict(row) for row in connection.execute(statement).mappings().all()]


def _pk_bounds(rows: list[dict[str, Any]], table: Table) -> dict[str, Any]:
    names = [column.name for column in table.primary_key.columns]
    if not names or not rows:
        return {"min": None, "max": None}
    keys = [tuple(_canonical_value(row[name]) for name in names) for row in rows]
    return {
        "min": list(min(keys)),
        "max": list(max(keys)),
    }


def _reset_sequences(connection: Connection, table: Table) -> list[str]:
    reset: list[str] = []
    for column in table.primary_key.columns:
        if not isinstance(column.type, Integer):
            continue
        sequence = connection.execute(
            text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
            {"table_name": table.name, "column_name": column.name},
        ).scalar_one_or_none()
        if not sequence:
            continue
        maximum = connection.execute(select(func.max(column))).scalar_one_or_none()
        if maximum is None:
            connection.execute(
                text("SELECT setval(CAST(:sequence AS regclass), 1, false)"),
                {"sequence": sequence},
            )
        else:
            connection.execute(
                text("SELECT setval(CAST(:sequence AS regclass), :value, true)"),
                {"sequence": sequence, "value": int(maximum)},
            )
        reset.append(str(sequence))
    return reset


def migrate(
    source_path: Path,
    target_url: str,
    *,
    batch_size: int = 500,
) -> dict[str, Any]:
    _validate_target_url(target_url)
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    source = _sqlite_connection(source_path.resolve())
    engine = create_engine(target_url, pool_pre_ping=True)
    report: dict[str, Any] = {
        "status": "failed",
        "source": str(source_path.resolve()),
        "target": _safe_url(target_url),
        "table_count": 0,
        "row_count": 0,
        "tables": {},
    }
    try:
        expected_tables = set(Base.metadata.tables)
        actual_tables = _source_tables(source)
        if actual_tables != expected_tables:
            raise RuntimeError(
                "SQLite table set does not match application metadata: "
                f"missing={sorted(expected_tables - actual_tables)} "
                f"unexpected={sorted(actual_tables - expected_tables)}"
            )

        with engine.begin() as target:
            target.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": POSTGRES_SCHEMA_ADVISORY_LOCK_ID},
            )
            create_application_schema(target)

            non_empty = {}
            for table in Base.metadata.sorted_tables:
                count = int(target.execute(select(func.count()).select_from(table)).scalar_one())
                if count:
                    non_empty[table.name] = count
            if non_empty:
                raise RuntimeError(
                    "target database is not empty; refusing destructive migration: "
                    f"{non_empty}"
                )

            for table in Base.metadata.sorted_tables:
                source_rows = _source_rows(source, table)
                for offset in range(0, len(source_rows), batch_size):
                    target.execute(table.insert(), source_rows[offset : offset + batch_size])

            for table in Base.metadata.sorted_tables:
                source_rows = _source_rows(source, table)
                target_rows = _target_rows(target, table)
                column_names = [column.name for column in table.columns]
                source_digest = _row_digest(source_rows, column_names)
                target_digest = _row_digest(target_rows, column_names)
                source_bounds = _pk_bounds(source_rows, table)
                target_bounds = _pk_bounds(target_rows, table)
                matches = (
                    len(source_rows) == len(target_rows)
                    and source_digest == target_digest
                    and source_bounds == target_bounds
                )
                report["tables"][table.name] = {
                    "source_count": len(source_rows),
                    "target_count": len(target_rows),
                    "source_digest": source_digest,
                    "target_digest": target_digest,
                    "source_pk": source_bounds,
                    "target_pk": target_bounds,
                    "matches": matches,
                    "sequences_reset": _reset_sequences(target, table),
                }
                if not matches:
                    raise RuntimeError(f"verification failed for table {table.name}")

        report["status"] = "passed"
        report["table_count"] = len(report["tables"])
        report["row_count"] = sum(
            item["source_count"] for item in report["tables"].values()
        )
        return report
    finally:
        source.close()
        engine.dispose()


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument(
        "--target-url",
        default=os.getenv("DATABASE_URL", ""),
        help="PostgreSQL SQLAlchemy URL; defaults to DATABASE_URL",
    )
    parser.add_argument("--report", type=Path)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()

    if not args.target_url:
        parser.error("--target-url or DATABASE_URL is required")

    try:
        report = migrate(
            args.source,
            args.target_url,
            batch_size=args.batch_size,
        )
    except Exception as exc:
        print(f"Migration failed: {exc}", file=sys.stderr)
        return 1

    if args.report:
        _write_report(args.report, report)
    print(
        "Migration verified: "
        f"{report['table_count']} tables, {report['row_count']} rows, "
        f"target={report['target']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
