# PostgreSQL 本地迁移与回滚

Docker 双容器（`server` 与 `analyzer`）共享数据库时，推荐使用 PostgreSQL。原有
SQLite 仍作为兼容与紧急回滚路径，但不再建议通过 macOS Docker bind mount 让两个
进程长期并发写同一个 SQLite/WAL 文件。

## 部署结构

- PostgreSQL 数据保存在 Docker named volume `postgres_data`。
- 数据库密码保存在被 Git 忽略的 `data/postgres-secrets/`，不会写入 `.env`、URL 或日志。
- `server` 与 `analyzer` 等待 PostgreSQL 健康后才启动。
- `DATABASE_URL` 显式配置时优先；未配置时，Compose 使用项目 PostgreSQL 默认地址。
- 删除 `DATABASE_URL` 或改为 SQLite URL 可以走兼容路径。

## 首次准备

```bash
python3 scripts/init_postgres_secrets.py
docker compose -f docker/docker-compose.yml up -d postgres
```

密钥脚本不会显示密码，也不会覆盖已存在的密钥。请单独备份
`data/postgres-secrets/`；丢失密码后，现有 PostgreSQL volume 无法由应用重新认证。

## SQLite 迁移

先生成一致性备份。目录必须不存在，脚本会检查 `quick_check`、外键、表数、行数与
SHA-256：

```bash
python3 scripts/backup_sqlite.py \
  --source data/stock_analysis.db \
  --destination-dir data/backups/postgresql-cutover-YYYYMMDD-HHMM
```

最终切换前停止两个写入进程，再制作一次最终备份。迁移目标必须为空；工具绝不会
覆盖非空 PostgreSQL：

```bash
docker compose -f docker/docker-compose.yml stop server analyzer
docker compose -f docker/docker-compose.yml run --rm --no-deps server \
  python scripts/migrate_sqlite_to_postgres.py \
  --source /app/data/backups/postgresql-cutover-YYYYMMDD-HHMM/stock_analysis.sqlite3 \
  --report /app/data/backups/postgresql-cutover-YYYYMMDD-HHMM/migration-report.json
docker compose -f docker/docker-compose.yml up -d server analyzer
```

迁移在一个 PostgreSQL 事务内完成，并逐表核对行数、主键边界和全行 SHA-256；任一
表不一致都会回滚。整数主键序列会重置到已迁移最大值，避免新写入撞 ID。

## 验证

```bash
docker compose -f docker/docker-compose.yml ps
docker logs --tail 200 stock-server
docker logs --tail 200 stock-analyzer
curl -fsS http://localhost:8000/api/health
```

日志中的数据库 URL 应显示 PostgreSQL 且密码被隐藏。历史分析、模拟计划、模拟持仓、
资金账户及运行时设置应与迁移报告中的对应表行数一致。

## 回滚到保留的 SQLite

若 PostgreSQL 切换后的运行验证失败，在根目录 `.env` 添加：

```dotenv
DATABASE_URL=sqlite:////app/data/stock_analysis.db
SQLITE_WAL_ENABLED=false
```

然后重新创建应用容器：

```bash
docker compose -f docker/docker-compose.yml up -d --force-recreate server analyzer
```

迁移过程不会覆盖原 `data/stock_analysis.db`，因此可立即回到切换前状态。注意：切换后
只写入 PostgreSQL 的新数据不会自动反向同步到 SQLite；回滚前应先停止应用并保留
PostgreSQL volume，不要执行 `docker compose down -v`。

## 安全边界

- 不要把 `data/postgres-secrets/`、`.env` 或数据库导出提交到 Git。
- 不要对已有数据的目标库强行清表；请使用新的空数据库重新迁移。
- `docker compose down` 不删除 named volume；`docker compose down -v` 会删除数据库
  volume，除非已有验证备份，否则禁止使用。

