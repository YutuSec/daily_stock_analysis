# Local PostgreSQL Migration and Rollback

PostgreSQL is recommended when the Docker `server` and `analyzer` containers
share one database. SQLite remains supported for compatibility and emergency
rollback, but a macOS Docker bind mount should not be used for sustained
multi-process SQLite/WAL writes.

## Deployment model

- PostgreSQL data is stored in the Docker named volume `postgres_data`.
- The password is stored under the gitignored `data/postgres-secrets/`; it is
  not embedded in `.env`, a database URL, or logs.
- Both application containers wait for PostgreSQL health before starting.
- An explicit `DATABASE_URL` takes precedence. Otherwise Compose supplies the
  project PostgreSQL default.
- SQLite remains available by setting an explicit SQLite URL.

## Initial setup

```bash
python3 scripts/init_postgres_secrets.py
docker compose -f docker/docker-compose.yml up -d postgres
```

The secret initializer never prints the password or overwrites existing secret
files. Back up `data/postgres-secrets/` separately.

## Migrate SQLite

Create a consistent backup first. The destination directory must not exist.
The command verifies SQLite integrity, foreign keys, table and row counts, and
records a SHA-256 digest:

```bash
python3 scripts/backup_sqlite.py \
  --source data/stock_analysis.db \
  --destination-dir data/backups/postgresql-cutover-YYYYMMDD-HHMM
```

Stop both writers and make one final backup before cutover. The PostgreSQL
target must be empty; the migration command refuses to overwrite a non-empty
database:

```bash
docker compose -f docker/docker-compose.yml stop server analyzer
docker compose -f docker/docker-compose.yml run --rm --no-deps server \
  python scripts/migrate_sqlite_to_postgres.py \
  --source /app/data/backups/postgresql-cutover-YYYYMMDD-HHMM/stock_analysis.sqlite3 \
  --report /app/data/backups/postgresql-cutover-YYYYMMDD-HHMM/migration-report.json
docker compose -f docker/docker-compose.yml up -d server analyzer
```

The copy and verification run in one PostgreSQL transaction. Every table is
checked by row count, primary-key bounds, and a full-row SHA-256 digest. Any
mismatch rolls the transaction back. Integer sequences are reset after import.

## Verify

```bash
docker compose -f docker/docker-compose.yml ps
docker logs --tail 200 stock-server
docker logs --tail 200 stock-analyzer
curl -fsS http://localhost:8000/api/health
```

Database logs should identify PostgreSQL while masking credentials. Analysis
history, paper plans and positions, capital accounts, and runtime settings must
match the migration report.

## Roll back to the retained SQLite database

If post-cutover checks fail, add this to the root `.env`:

```dotenv
DATABASE_URL=sqlite:////app/data/stock_analysis.db
SQLITE_WAL_ENABLED=false
```

Then recreate the application containers:

```bash
docker compose -f docker/docker-compose.yml up -d --force-recreate server analyzer
```

Migration never overwrites `data/stock_analysis.db`, so it remains the
immediate pre-cutover rollback source. Writes made only to PostgreSQL after
cutover are not reverse-synchronized. Stop the application and retain the
PostgreSQL volume before rollback; never run `docker compose down -v` without a
verified backup.

## Safety boundaries

- Never commit `data/postgres-secrets/`, `.env`, or database exports.
- Never force-truncate an existing target. Migrate again into a fresh database.
- `docker compose down` retains named volumes; `docker compose down -v` deletes
  them and must not be used without a verified backup.

