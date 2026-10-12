#!/usr/bin/env python3
"""Create local Docker secret files for the PostgreSQL deployment.

The generated directory lives under ``data/`` (gitignored). Existing secret
files are never overwritten, so rerunning this command is safe.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets


def _write_new_secret(path: Path, content: str) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--directory",
        type=Path,
        default=Path("data/postgres-secrets"),
        help="Local secret directory (default: data/postgres-secrets)",
    )
    args = parser.parse_args()

    secret_dir = args.directory.resolve()
    secret_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(secret_dir, 0o700)

    password_path = secret_dir / "password"
    pgpass_path = secret_dir / "pgpass"
    if password_path.exists() or pgpass_path.exists():
        if not password_path.is_file() or not pgpass_path.is_file():
            raise SystemExit("PostgreSQL secret paths are incomplete or not regular files")
        os.chmod(password_path, 0o600)
        os.chmod(pgpass_path, 0o600)
        print(f"PostgreSQL secret files already exist in {secret_dir}; kept unchanged.")
        return 0

    password = secrets.token_urlsafe(48)
    try:
        _write_new_secret(password_path, f"{password}\n")
        _write_new_secret(
            pgpass_path,
            f"postgres:5432:daily_stock_analysis:dsa:{password}\n",
        )
    except Exception:
        password_path.unlink(missing_ok=True)
        pgpass_path.unlink(missing_ok=True)
        raise

    print(f"Created PostgreSQL secret files in {secret_dir} (contents not displayed).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
