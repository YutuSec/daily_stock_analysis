#!/usr/bin/env python3
"""Create and verify a transactionally consistent SQLite backup."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def backup_sqlite(source: Path, destination_dir: Path) -> dict:
    source = source.resolve()
    destination_dir = destination_dir.resolve()
    if not source.is_file():
        raise FileNotFoundError(f"SQLite source does not exist: {source}")
    destination_dir.mkdir(parents=True, exist_ok=False)

    destination = destination_dir / "stock_analysis.sqlite3"
    temporary = destination_dir / ".stock_analysis.sqlite3.tmp"
    source_connection = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    target_connection = sqlite3.connect(temporary)
    try:
        source_connection.backup(target_connection)
    finally:
        target_connection.close()
        source_connection.close()

    validation = sqlite3.connect(f"file:{temporary}?mode=ro", uri=True)
    try:
        quick_check = [row[0] for row in validation.execute("PRAGMA quick_check")]
        if quick_check != ["ok"]:
            raise RuntimeError(f"backup quick_check failed: {quick_check[:10]}")
        foreign_key_errors = [
            tuple(row) for row in validation.execute("PRAGMA foreign_key_check")
        ]
        if foreign_key_errors:
            raise RuntimeError(
                f"backup foreign_key_check failed: {foreign_key_errors[:10]}"
            )
        tables = [
            str(row[0])
            for row in validation.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        counts = {
            table: int(
                validation.execute(
                    'SELECT count(*) FROM "' + table.replace('"', '""') + '"'
                ).fetchone()[0]
            )
            for table in tables
        }
    finally:
        validation.close()

    temporary.replace(destination)
    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": str(source),
        "backup": str(destination),
        "bytes": destination.stat().st_size,
        "sha256": _sha256(destination),
        "quick_check": "ok",
        "foreign_key_check": "ok",
        "table_count": len(counts),
        "row_count": sum(counts.values()),
        "tables": counts,
    }
    manifest_path = destination_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination-dir", type=Path, required=True)
    args = parser.parse_args()
    manifest = backup_sqlite(args.source, args.destination_dir)
    print(
        "SQLite backup verified: "
        f"{manifest['table_count']} tables, {manifest['row_count']} rows, "
        f"sha256={manifest['sha256']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
