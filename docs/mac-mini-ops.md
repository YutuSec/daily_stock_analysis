# Mac Mini 部署与运维手册

## 目录

- [快速部署](#快速部署)
- [服务架构](#服务架构)
- [常用运维命令](#常用运维命令)
- [配置说明](#配置说明)
- [日志与排障](#日志与排障)
- [数据备份与恢复](#数据备份与恢复)
- [更新升级](#更新升级)
- [安全建议](#安全建议)

---

## 快速部署

```bash
# 1. 克隆仓库
git clone https://github.com/YutuSec/daily_stock_analysis.git
cd daily_stock_analysis

# 2. 一键部署（自动创建目录、密码、构建镜像、启动服务）
bash scripts/deploy-mac-mini.sh
```

部署脚本会自动完成：
- 检查 Docker 环境
- 创建 data/logs/reports 等数据目录
- 生成 PostgreSQL 随机密码
- 从 .env.example 创建 .env（首次需手动填入 LLM API Key）
- 构建 Docker 镜像
- 启动所有服务并验证健康状态

---

## 服务架构

| 容器 | 名称 | 职责 | 默认端口 |
|------|------|------|----------|
| postgres | stock-postgres | PostgreSQL 数据库 | 5432（容器内部） |
| analyzer | stock-analyzer | 定时分析任务（每日自动运行） | - |
| server | stock-server | FastAPI Web 服务 + API | 8000 |

---

## 常用运维命令

以下命令均在项目根目录执行。可设置别名简化操作：

```bash
alias dsa='docker compose -f docker/docker-compose.yml'
```

### 服务控制

```bash
# 启动所有服务
dsa up -d

# 仅启动 Web API（不启动定时分析）
dsa up -d server

# 仅启动定时分析（不启动 Web）
dsa up -d analyzer

# 停止所有服务
dsa down

# 重启某个服务
dsa restart server
dsa restart analyzer
dsa restart postgres

# 查看服务状态
dsa ps
```

### 日志查看

```bash
# 查看所有服务日志（实时跟踪）
dsa logs -f --tail=100

# 查看某个服务日志
dsa logs -f --tail=100 server
dsa logs -f --tail=100 analyzer
dsa logs -f --tail=100 postgres

# 查看最近的错误
dsa logs server 2>&1 | grep -i error | tail -20
```

### 健康检查

```bash
# API 健康检查
curl http://localhost:8000/api/health

# 数据库连接检查
docker exec stock-postgres pg_isready -U dsa -d daily_stock_analysis

# 容器资源使用
docker stats stock-server stock-analyzer stock-postgres --no-stream
```

### 手动触发分析

```bash
# 进入 analyzer 容器执行一次分析
docker exec -u dsa stock-analyzer python main.py --stocks 600519

# 大盘复盘
docker exec -u dsa stock-analyzer python main.py --market-review

# Dry run（不发送通知）
docker exec -u dsa stock-analyzer python main.py --dry-run --stocks 600519
```

---

## 配置说明

### .env 关键配置项

| 配置项 | 说明 | 必填 |
|--------|------|------|
| `STOCK_LIST` | 自选股列表，逗号分隔 | 否（有默认值） |
| `LLM_PRIMARY_API_KEY` | 主 LLM API Key | 是 |
| `LLM_PRIMARY_PROTOCOL` | LLM 协议（deepseek/openai/anthropic） | 是 |
| `LLM_PRIMARY_BASE_URL` | LLM API 地址 | 是 |
| `LLM_PRIMARY_MODELS` | 模型名称 | 是 |
| `API_PORT` | Web 服务端口，默认 8000 | 否 |
| `FEISHU_WEBHOOK_URL` | 飞书通知 Webhook | 否 |
| `FEISHU_WEBHOOK_SECRET` | 飞书 Webhook 密钥 | 否 |
| `TUSHARE_TOKEN` | Tushare Pro 数据源 Token | 否 |

修改 `.env` 后需重启服务生效：

```bash
dsa up -d
```

### 端口与防火墙

如需从局域网其他设备访问 Web 界面：

```bash
# 查看 Mac Mini 的 IP
ipconfig getifaddr en0

# 访问地址: http://<Mac-Mini-IP>:8000
```

macOS 默认不阻止入站连接。如果开启了防火墙，在"系统设置 → 网络 → 防火墙 → 选项"中允许 Docker/OrbStack 接受传入连接。

### 代理设置

如果网络环境需要代理访问外部 API，编辑 `docker/docker-compose.yml`，取消 `x-common` 中 proxy 注释：

```yaml
environment:
  - http_proxy=http://host.docker.internal:10809
  - https_proxy=http://host.docker.internal:10809
```

---

## 日志与排障

### 日志位置

| 日志 | 路径 | 说明 |
|------|------|------|
| 应用日志 | `./logs/` | 挂载到容器内 /app/logs |
| Docker 日志 | `dsa logs <service>` | JSON 格式，自动轮转 10MB×3 |
| 分析报告 | `./reports/` | 生成的分析报告 |

### 常见问题

**Q: API 健康检查失败**

```bash
# 1. 检查容器是否在运行
dsa ps

# 2. 查看 server 日志
dsa logs server --tail=50

# 3. 确认端口没被占用
lsof -i :8000
```

**Q: 数据库连接失败**

```bash
# 1. 检查 postgres 容器状态
dsa ps postgres

# 2. 检查密码文件
cat data/postgres-secrets/password

# 3. 手动连接测试
docker exec -it stock-postgres psql -U dsa -d daily_stock_analysis -c "SELECT 1;"
```

**Q: 分析任务不执行**

```bash
# 1. 确认 analyzer 在运行
dsa ps analyzer

# 2. 查看调度日志
dsa logs analyzer --tail=100 | grep -i schedule

# 3. 确认交易日历（非交易日不执行分析）
docker exec -u dsa stock-analyzer python -c "from src.core.trading_calendar import *; print('today is trading day:', is_trading_day())"
```

**Q: LLM 调用失败**

```bash
# 查看 LLM 相关日志
dsa logs server 2>&1 | grep -iE 'llm|api_key|rate.limit|timeout' | tail -20

# 确认 .env 中 API Key 正确
grep LLM_PRIMARY .env
```

**Q: 内存不足（OOM）**

`docker-compose.yml` 默认限制每个应用容器 1GB 内存。如果 Mac Mini 内存充裕，可调大：

```yaml
deploy:
  resources:
    limits:
      memory: 2G
```

---

## 从现有环境迁移数据

如果你从已有的生产环境迁移（例如原来的 Mac/Linux 服务器），需要把 PostgreSQL 数据导入新部署的 Mac Mini。

### 在源机器上导出

```bash
cd daily_stock_analysis

# 导出数据库（约 61MB）
docker exec stock-postgres pg_dump -U dsa --no-owner --no-acl daily_stock_analysis > data/backups/pg_dump_$(date +%Y%m%d).sql

# 传输到 Mac Mini
scp data/backups/pg_dump_*.sql USER@MAC_MINI_IP:~/
```

### 在 Mac Mini 上导入

```bash
cd daily_stock_analysis

# 确保部署已完成、postgres 已就绪
docker exec stock-postgres pg_isready -U dsa -d daily_stock_analysis

# 导入数据
docker exec -i stock-postgres psql -U dsa -d daily_stock_analysis < ~/pg_dump_20261012.sql

# 验证关键表的数据量
docker exec stock-postgres psql -U dsa -d daily_stock_analysis -c "
SELECT 'analysis_history' as tbl, count(*) FROM analysis_history
UNION ALL SELECT 'stock_daily', count(*) FROM stock_daily
UNION ALL SELECT 'decision_signals', count(*) FROM decision_signals
UNION ALL SELECT 'llm_usage', count(*) FROM llm_usage
ORDER BY count DESC;
"

# 重启服务使应用加载最新数据
docker compose -f docker/docker-compose.yml restart server analyzer
```

### 从 SQLite 迁移（如果之前用的 SQLite）

```bash
# 将 SQLite 数据库文件传到 Mac Mini
scp data/stock_analysis.db USER@MAC_MINI_IP:~/daily_stock_analysis/data/

# 在 Mac Mini 上运行迁移脚本
docker exec -u dsa stock-server python scripts/migrate_sqlite_to_postgres.py
```

---

## 数据备份与恢复

### PostgreSQL 备份

```bash
# 备份数据库
docker exec stock-postgres pg_dump -U dsa daily_stock_analysis > backup_$(date +%Y%m%d).sql

# 定时备份（添加到 crontab）
# crontab -e
# 0 2 * * * cd /path/to/daily_stock_analysis && docker exec stock-postgres pg_dump -U dsa daily_stock_analysis > backups/backup_$(date +\%Y\%m\%d).sql
```

### PostgreSQL 恢复

```bash
# 恢复数据库
docker exec -i stock-postgres psql -U dsa -d daily_stock_analysis < backup_20261012.sql
```

### 数据卷备份

```bash
# 备份所有持久化数据
tar czf dsa-data-backup-$(date +%Y%m%d).tar.gz data/ logs/ reports/
```

### 完整迁移

如果要迁移到另一台机器：

```bash
# 源机器：导出
docker exec stock-postgres pg_dump -U dsa daily_stock_analysis > db_dump.sql
tar czf dsa-migrate.tar.gz db_dump.sql data/ logs/ reports/ .env data/postgres-secrets/

# 目标机器：导入
tar xzf dsa-migrate.tar.gz
bash scripts/deploy-mac-mini.sh
docker exec -i stock-postgres psql -U dsa -d daily_stock_analysis < db_dump.sql
```

---

## 更新升级

```bash
# 1. 拉取最新代码
git pull

# 2. 重新构建并启动（--build 只重建有变化的层）
dsa up -d --build

# 如果依赖有大变化，清理后重建
dsa down
docker builder prune -f
dsa up -d --build
```

回滚到上一版本：

```bash
git log --oneline -5          # 查看最近提交
git checkout <commit-hash>    # 切到目标版本
dsa up -d --build             # 重建
```

---

## 安全建议

1. **密码管理**：PostgreSQL 密码存放在 `data/postgres-secrets/`，权限应为 600，不要提交到 git
2. **API Key**：`.env` 文件不要提交到 git（已在 .gitignore 中排除）
3. **网络隔离**：PostgreSQL 端口未暴露到宿主机，仅容器间可访问
4. **及时更新**：定期 `git pull` 并重新构建以获取安全修复
5. **日志轮转**：Docker 日志已配置自动轮转（10MB × 3 份），应用日志按需清理
6. **资源限制**：容器已配置内存限制，防止单个容器耗尽系统资源
