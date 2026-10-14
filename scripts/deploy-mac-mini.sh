#!/bin/bash
# ===================================
# Daily Stock Analysis - Mac Mini 一键部署脚本
# 用法: bash scripts/deploy-mac-mini.sh
# ===================================
set -euo pipefail

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ---- 0. 前置检查 ----
info "检查运行环境..."

if ! command -v docker &>/dev/null; then
    error "未检测到 Docker。请先安装：\n  brew install orbstack    # 推荐，轻量\n  brew install --cask docker  # 或 Docker Desktop"
fi

if ! docker info &>/dev/null; then
    error "Docker 未启动，请先启动 Docker Desktop 或 OrbStack"
fi

if ! docker compose version &>/dev/null; then
    error "未检测到 docker compose 插件"
fi

info "Docker 环境正常 ✓"

# ---- 1. 确定项目目录 ----
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"
info "项目目录: $PROJECT_DIR"

# ---- 2. 创建必要目录 ----
info "创建数据目录..."
mkdir -p data/postgres-secrets logs reports strategies longbridge_tokens

# ---- 3. 初始化 PostgreSQL 密码 ----
if [ ! -f data/postgres-secrets/password ]; then
    info "初始化 PostgreSQL 密码..."
    # 生成随机密码
    DB_PASSWORD="$(openssl rand -base64 24 | tr -d '/+=' | head -c 24)"
    echo -n "$DB_PASSWORD" > data/postgres-secrets/password
    echo "localhost:5432:daily_stock_analysis:dsa:$DB_PASSWORD" > data/postgres-secrets/pgpass
    chmod 600 data/postgres-secrets/password data/postgres-secrets/pgpass
    info "PostgreSQL 密码已生成并保存到 data/postgres-secrets/"
    info "密码: $DB_PASSWORD  （请妥善记录）"
else
    info "PostgreSQL 密码已存在，跳过 ✓"
fi

# ---- 4. 初始化 .env ----
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        warn ".env 已从模板创建，请编辑 .env 填入以下关键配置："
        echo ""
        echo "  必填项："
        echo "    LLM_PRIMARY_API_KEY=你的API密钥"
        echo ""
        echo "  可选项："
        echo "    STOCK_LIST=600519,300750,002594"
        echo "    API_PORT=8000"
        echo "    FEISHU_WEBHOOK_URL=飞书通知地址"
        echo ""
        echo "  编辑命令: nano .env"
        echo ""
        read -p "是否现在编辑 .env？[y/N] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            ${EDITOR:-nano} .env
        fi
    else
        error "未找到 .env.example，请确认仓库完整性"
    fi
else
    info ".env 已存在 ✓"
fi

# ---- 5. 构建镜像 ----
info "开始构建 Docker 镜像（首次约 5-10 分钟）..."
docker compose -f docker/docker-compose.yml build

# ---- 6. 启动服务 ----
info "启动所有服务..."
docker compose -f docker/docker-compose.yml up -d

# ---- 7. 等待健康检查 ----
info "等待服务就绪..."
MAX_WAIT=60
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    if docker compose -f docker/docker-compose.yml ps --format json 2>/dev/null | grep -q '"healthy"'; then
        break
    fi
    # fallback: 直接检查 API
    if curl -sf http://localhost:${API_PORT:-8000}/api/health &>/dev/null; then
        break
    fi
    sleep 3
    WAITED=$((WAITED + 3))
    echo -n "."
done
echo

# ---- 8. 验证 ----
info "验证服务状态..."
echo ""
docker compose -f docker/docker-compose.yml ps
echo ""

API_PORT="${API_PORT:-8000}"
if curl -sf "http://localhost:$API_PORT/api/health" &>/dev/null; then
    info "API 服务正常 ✓  地址: http://localhost:$API_PORT"
    info "Web 界面: http://localhost:$API_PORT"
else
    warn "API 健康检查未通过，可能仍在启动中。请稍后手动检查："
    echo "  curl http://localhost:$API_PORT/api/health"
    echo "  docker compose -f docker/docker-compose.yml logs -f --tail=50"
fi

echo ""
info "========================================"
info "  部署完成！"
info "  Web 界面:  http://localhost:$API_PORT"
info "  局域网访问: http://$(hostname -I 2>/dev/null | awk '{print $1}' || ipconfig getifaddr en0 2>/dev/null || echo 'Mac-Mini-IP'):$API_PORT"
info "  运维文档:  docs/mac-mini-ops.md"
info "========================================"
