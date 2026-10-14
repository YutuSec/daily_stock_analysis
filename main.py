# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 主调度程序
===================================

职责：
1. 协调各模块完成股票分析流程
2. 实现低并发的线程池调度
3. 全局异常处理，确保单股失败不影响整体
4. 提供命令行入口

使用方式：
    python main.py              # 正常运行
    python main.py --debug      # 调试模式
    python main.py --dry-run    # 仅获取数据不分析

交易理念（已融入分析）：
- 严进策略：不追高，乖离率 > 5% 不买入
- 趋势交易：只做 MA5>MA10>MA20 多头排列
- 效率优先：关注筹码集中度好的股票
- 买点偏好：缩量回踩 MA5/MA10 支撑
"""
from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from dotenv import dotenv_values
from src.config import setup_env

_INITIAL_PROCESS_ENV = dict(os.environ)
setup_env()

# 代理配置 - 通过 USE_PROXY 环境变量控制，默认关闭
# GitHub Actions 环境自动跳过代理配置
if os.getenv("GITHUB_ACTIONS") != "true" and os.getenv("USE_PROXY", "false").lower() == "true":
    # 本地开发环境，启用代理（可在 .env 中配置 PROXY_HOST 和 PROXY_PORT）
    proxy_host = os.getenv("PROXY_HOST", "127.0.0.1")
    proxy_port = os.getenv("PROXY_PORT", "10809")
    proxy_url = f"http://{proxy_host}:{proxy_port}"
    os.environ["http_proxy"] = proxy_url
    os.environ["https_proxy"] = proxy_url

_packaged_import_probe = os.getenv("DSA_PACKAGED_IMPORT_PROBE")
if _packaged_import_probe:
    import importlib
    import sys

    try:
        importlib.import_module(_packaged_import_probe)
    except Exception as exc:
        print(
            f"ERROR: packaged import failed for {_packaged_import_probe}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"OK: packaged import succeeded for {_packaged_import_probe}")
    sys.exit(0)

import argparse
import logging
import sys
import time
import uuid
from datetime import date, datetime, time as datetime_time, timezone, timedelta
from zoneinfo import ZoneInfo

from src.webui_frontend import prepare_webui_frontend_assets
from src.config import get_config, Config
from src.logging_config import setup_logging
from src.services.stock_list_parser import split_stock_list
from src.services.stock_code_utils import resolve_index_stock_code_for_analysis


logger = logging.getLogger(__name__)
_RUNTIME_ENV_FILE_KEYS = set()
_PUBLIC_BIND_HOSTS = frozenset({"0.0.0.0", "::", "[::]", "*"})


def _get_active_env_path() -> Path:
    env_file = os.getenv("ENV_FILE")
    if env_file:
        return Path(env_file)
    return Path(__file__).resolve().parent / ".env"


def _is_public_bind_host(host: str) -> bool:
    return (host or "").strip().lower() in _PUBLIC_BIND_HOSTS


def _warn_if_public_webui_without_auth(host: str) -> None:
    if not _is_public_bind_host(host):
        return

    from src.auth import is_auth_enabled

    if is_auth_enabled():
        return
    logger.warning(
        "WEBUI_HOST=%s binds the Web UI to a public interface while "
        "ADMIN_AUTH_ENABLED=false. Keep this service behind a trusted network "
        "boundary or enable admin authentication before exposing it.",
        host,
    )


def _resolve_web_service_bind(args: argparse.Namespace, config: Config) -> Tuple[str, int]:
    """Resolve the effective Web/API bind address from CLI first, then config."""
    host = args.host if args.host is not None else (config.webui_host or "127.0.0.1")
    port = args.port if args.port is not None else config.webui_port
    return host, port


def _read_active_env_values() -> Optional[Dict[str, str]]:
    env_path = _get_active_env_path()
    if not env_path.exists():
        return {}

    try:
        values = dotenv_values(env_path)
    except Exception as exc:  # pragma: no cover - defensive branch
        logger.warning("读取配置文件 %s 失败，继续沿用当前环境变量: %s", env_path, exc)
        return None

    return {
        str(key): "" if value is None else str(value)
        for key, value in values.items()
        if key is not None
    }


_ACTIVE_ENV_FILE_VALUES = _read_active_env_values() or {}
_RUNTIME_ENV_FILE_KEYS = {
    key for key in _ACTIVE_ENV_FILE_VALUES
    if key not in _INITIAL_PROCESS_ENV
}

# setup_env() already ran at import time above.
_env_bootstrapped = True


def _bootstrap_environment() -> None:
    """Load .env and apply optional local proxy settings.

    Guarded to be idempotent so it can safely be called from lazy-import
    paths used by API / bot consumers.
    """
    global _env_bootstrapped
    if _env_bootstrapped:
        return

    from src.config import setup_env

    setup_env()

    if os.getenv("GITHUB_ACTIONS") != "true" and os.getenv("USE_PROXY", "false").lower() == "true":
        proxy_host = os.getenv("PROXY_HOST", "127.0.0.1")
        proxy_port = os.getenv("PROXY_PORT", "10809")
        proxy_url = f"http://{proxy_host}:{proxy_port}"
        os.environ["http_proxy"] = proxy_url
        os.environ["https_proxy"] = proxy_url

    _env_bootstrapped = True


def _setup_bootstrap_logging(debug: bool = False) -> None:
    """Initialize stderr-only logging before config is loaded.

    File handlers are deferred until ``config.log_dir`` is known (via the
    subsequent ``setup_logging()`` call) so that healthy runs never create
    log files in a hard-coded directory.
    """
    level = logging.DEBUG if debug else logging.INFO
    root = logging.getLogger()
    root.setLevel(level)
    if not any(
        isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stderr
        for h in root.handlers
    ):
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        root.addHandler(handler)


def _setup_runtime_logging(log_dir: str, debug: bool = False) -> bool:
    """Switch to configured logging, falling back to console on file I/O errors."""
    try:
        setup_logging(log_prefix="stock_analysis", debug=debug, log_dir=log_dir)
        return True
    except OSError as exc:
        logger.warning(
            "文件日志初始化失败，已降级为控制台日志输出；日志目录 %r 当前不可写或不可创建: %s。"
            "官方 Docker 镜像启动入口会自动修复默认挂载目录权限；若仍失败，"
            "请检查是否使用了 --user、只读挂载、rootless Docker 或 NFS 等限制写入的环境。",
            log_dir,
            exc,
        )
        return False


def _get_stock_analysis_pipeline():
    """Lazily import StockAnalysisPipeline for external consumers.

    Also ensures env/proxy bootstrap has run so that API / bot consumers
    that never call ``main()`` still get ``USE_PROXY`` applied.
    """
    _bootstrap_environment()
    from src.core.pipeline import StockAnalysisPipeline as _Pipeline

    return _Pipeline


class _LazyPipelineDescriptor:
    """Descriptor that resolves StockAnalysisPipeline on first attribute access."""

    _resolved = None

    def __set_name__(self, owner, name):
        self._name = name

    def __get__(self, obj, objtype=None):
        if self._resolved is None:
            self._resolved = _get_stock_analysis_pipeline()
        return self._resolved


class _ModuleExports:
    StockAnalysisPipeline = _LazyPipelineDescriptor()


_exports = _ModuleExports()


def __getattr__(name: str):
    if name == "StockAnalysisPipeline":
        return _exports.StockAnalysisPipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _reload_env_file_values_preserving_overrides() -> None:
    """Refresh `.env`-managed env vars without clobbering process env overrides."""
    global _RUNTIME_ENV_FILE_KEYS

    latest_values = _read_active_env_values()
    if latest_values is None:
        return

    managed_keys = {
        key for key in latest_values
        if key not in _INITIAL_PROCESS_ENV
    }

    for key in _RUNTIME_ENV_FILE_KEYS - managed_keys:
        os.environ.pop(key, None)

    for key in managed_keys:
        os.environ[key] = latest_values[key]

    _RUNTIME_ENV_FILE_KEYS = managed_keys


def parse_arguments() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='A股自选股智能分析系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  python main.py                    # 正常运行
  python main.py --debug            # 调试模式
  python main.py --dry-run          # 仅获取数据，不进行 AI 分析
  python main.py --stocks 600519,000001  # 指定分析特定股票
  python main.py --no-notify        # 不发送推送通知
  python main.py --check-notify     # 检查通知配置，不发送通知
  python main.py --single-notify    # 启用单股推送模式（每分析完一只立即推送）
  python main.py --schedule         # 启用定时任务模式
  python main.py --market-review    # 仅运行大盘复盘
        '''
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='启用调试模式，输出详细日志'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='仅获取数据，不进行 AI 分析'
    )

    parser.add_argument(
        '--stocks',
        type=str,
        help='指定要分析的股票代码，逗号分隔（覆盖配置文件）'
    )

    parser.add_argument(
        '--no-notify',
        action='store_true',
        help='不发送推送通知'
    )

    parser.add_argument(
        '--check-notify',
        action='store_true',
        help='只读检查通知渠道配置，不发送通知'
    )

    parser.add_argument(
        '--single-notify',
        action='store_true',
        help='启用单股推送模式：每分析完一只股票立即推送，而不是汇总推送'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='并发线程数（默认使用配置值）'
    )

    parser.add_argument(
        '--schedule',
        action='store_true',
        help='启用定时任务模式，每日定时执行'
    )

    parser.add_argument(
        '--no-run-immediately',
        action='store_true',
        help='定时任务启动时不立即执行一次'
    )

    parser.add_argument(
        '--market-review',
        action='store_true',
        help='仅运行大盘复盘分析'
    )

    parser.add_argument(
        '--no-market-review',
        action='store_true',
        help='跳过大盘复盘分析'
    )

    parser.add_argument(
        '--force-run',
        action='store_true',
        help='跳过交易日检查，强制执行全量分析（Issue #373）'
    )

    parser.add_argument(
        '--webui',
        action='store_true',
        help='启动 Web 管理界面'
    )

    parser.add_argument(
        '--webui-only',
        action='store_true',
        help='仅启动 Web 服务，不执行自动分析'
    )

    parser.add_argument(
        '--serve',
        action='store_true',
        help='启动 FastAPI 后端服务（同时执行分析任务）'
    )

    parser.add_argument(
        '--serve-only',
        action='store_true',
        help='仅启动 FastAPI 后端服务，不自动执行分析'
    )

    parser.add_argument(
        '--port',
        type=int,
        default=None,
        help='FastAPI 服务端口（默认使用 WEBUI_PORT，未配置时为 8000）'
    )

    parser.add_argument(
        '--host',
        type=str,
        default=None,
        help='FastAPI 服务监听地址（默认使用 WEBUI_HOST，未配置时为 127.0.0.1）'
    )

    parser.add_argument(
        '--no-context-snapshot',
        action='store_true',
        help='不保存分析上下文快照'
    )

    # === Backtest ===
    parser.add_argument(
        '--backtest',
        action='store_true',
        help='运行回测（对历史分析结果进行评估）'
    )

    parser.add_argument(
        '--backtest-code',
        type=str,
        default=None,
        help='仅回测指定股票代码'
    )

    parser.add_argument(
        '--backtest-days',
        type=int,
        default=None,
        help='回测评估窗口（交易日数，默认使用配置）'
    )

    parser.add_argument(
        '--backtest-force',
        action='store_true',
        help='强制回测（即使已有回测结果也重新计算）'
    )

    return parser.parse_args()


def _compute_trading_day_filter(
    config: Config,
    args: argparse.Namespace,
    stock_codes: List[str],
) -> Tuple[List[str], Optional[str], bool]:
    """
    Compute filtered stock list and effective market review region (Issue #373).

    Returns:
        (filtered_codes, effective_region, should_skip_all)
        - effective_region None = use config default (check disabled)
        - effective_region '' = all relevant markets closed, skip market review
        - should_skip_all: skip entire run when no stocks and no market review to run
    """
    force_run = getattr(args, 'force_run', False)
    if force_run or not getattr(config, 'trading_day_check_enabled', True):
        return (stock_codes, None, False)

    from src.core.trading_calendar import (
        get_market_for_stock,
        get_open_markets_today,
        compute_effective_region,
        MARKET_TIMEZONE,
    )

    stock_markets = {get_market_for_stock(code) for code in stock_codes}
    if None in stock_markets:
        raise ValueError("无法确定股票所属市场，暂停自动分析；请使用带交易所标识的代码或显式 --force-run")
    required_markets = set(stock_markets)
    if config.market_review_enabled and not getattr(args, 'no_market_review', False):
        review_markets = compute_effective_region(
            getattr(config, 'market_review_region', 'cn') or 'cn', set(MARKET_TIMEZONE)
        )
        required_markets.update((review_markets or '').split(','))
        required_markets.discard('')
    open_markets = get_open_markets_today(markets=required_markets)
    filtered_codes = []
    for code in stock_codes:
        mkt = get_market_for_stock(code)
        if mkt in open_markets:
            filtered_codes.append(code)

    if config.market_review_enabled and not getattr(args, 'no_market_review', False):
        effective_region = compute_effective_region(
            getattr(config, 'market_review_region', 'cn') or 'cn', open_markets
        )
    else:
        effective_region = None

    should_skip_all = (not filtered_codes) and (effective_region or '') == ''
    return (filtered_codes, effective_region, should_skip_all)


def _run_market_review_with_shared_lock(
    config: Config,
    run_market_review_func: Callable[..., Any],
    **kwargs: Any,
) -> Any:
    from src.core.market_review_lock import (
        release_market_review_lock,
        try_acquire_market_review_lock,
    )

    lock_token = try_acquire_market_review_lock(config)
    if lock_token is None:
        logger.warning("大盘复盘正在执行中，跳过本次大盘复盘")
        return None

    try:
        params = dict(kwargs)
        params.setdefault("config", config)
        return run_market_review_func(**params)
    finally:
        release_market_review_lock(lock_token)


def _is_multi_market_region(region: str) -> bool:
    normalized = str(region or "").strip().lower()
    if not normalized:
        return False
    if normalized == "both":
        return True
    parts = {item.strip() for item in normalized.split(",") if item.strip()}
    return len(parts) > 1


def _refresh_stock_index_cache_for_analysis(config: Config) -> None:
    """Best-effort stock-index refresh for CLI/scheduled analysis paths."""
    try:
        from src.services.stock_index_remote_service import (
            refresh_remote_stock_index_cache,
            settings_from_config,
        )

        result = refresh_remote_stock_index_cache(settings_from_config(config))
        if result.refreshed:
            logger.info("[stock-index] 分析前已刷新股票索引缓存: %s", result.cache_path)
        elif result.error:
            logger.debug("[stock-index] 分析前刷新未完成，继续使用本地索引: %s", result.error)
    except Exception as exc:  # noqa: BLE001 - stock index freshness must not block analysis.
        logger.warning("[stock-index] 分析前刷新股票索引失败，继续执行分析: %s", exc)


def _prime_daily_market_context(
    config: Config,
    pipeline: Any,
    *,
    region: str,
    no_market_review: bool,
    allow_generate: bool,
    force_refresh: bool = False,
    target_date: Optional[date] = None,
    return_full_report: bool = False,
    require_current_query_match: bool = False,
) -> Union[str, Tuple[str, str]]:
    """Load/reuse the run's market context, avoiding unbounded background generation."""
    if no_market_review or not region:
        return ("", "") if return_full_report else ""

    from src.services.daily_market_context import DailyMarketContextService

    if not _is_multi_market_region(region):
        service = getattr(pipeline, "_daily_market_context_service", None)
        if service is None:
            service = DailyMarketContextService(db_manager=pipeline.db)
            pipeline._daily_market_context_service = service
    else:
        service = DailyMarketContextService(db_manager=pipeline.db)

    get_context_kwargs = {
        "region": region,
        "config": config,
        "notifier": pipeline.notifier,
        "analyzer": pipeline.analyzer,
        "search_service": pipeline.search_service,
        "force_refresh": force_refresh,
        "allow_generate": allow_generate,
        "persist_market_review_history": False,
        "target_date": target_date,
        "require_query_id_match": require_current_query_match,
    }
    current_query_id = getattr(pipeline, "query_id", None)
    if isinstance(current_query_id, str) and current_query_id.strip():
        get_context_kwargs["current_query_id"] = current_query_id

    context = service.get_context(**get_context_kwargs)
    if context is None:
        return ("", "") if return_full_report else ""

    # Runtime context generation is preload-only and must not replace the full
    # market review run, except the query-scoped fallback after that run fails.
    if context.source != "analysis_history" and not (
        require_current_query_match and context.source == "market_review_runtime"
    ):
        return ("", "") if return_full_report else ""

    summary = str(getattr(context, "summary", ""))
    full_report = str(getattr(context, "full_report", "") or "")
    if return_full_report:
        return summary, full_report
    return summary


def _can_reuse_market_context_for_review(summary: str, region: str) -> bool:
    if not summary:
        return False
    normalized = str(region or "").strip().lower()
    if normalized == "both":
        return False
    parts = {item.strip() for item in normalized.split(",") if item.strip()}
    return len(parts) <= 1


def _resolve_daily_market_context_market(market: str, normalized_region: str) -> str:
    if "," not in normalized_region:
        return market
    parts = [item.strip() for item in normalized_region.split(",") if item.strip()]
    if parts and all(item in {"jp", "kr"} for item in parts):
        return parts[0]
    return market


def _resolve_daily_market_context_target_date(
    region: str,
    current_time: datetime,
) -> date:
    normalized_region = str(region or "cn").strip().lower()
    market = normalized_region if normalized_region in {"cn", "hk", "us", "jp", "kr"} else "cn"

    from src.core.trading_calendar import get_effective_trading_date

    return get_effective_trading_date(
        _resolve_daily_market_context_market(market, normalized_region),
        current_time=current_time,
    )


def _market_review_report_text(review_result: Any) -> str:
    if review_result is None:
        return ""
    report = getattr(review_result, "report", None)
    if isinstance(report, str):
        return report
    return review_result if isinstance(review_result, str) else ""


def _save_reused_market_review_report(
    notifier: Any,
    market_report: str,
    *,
    config: Config,
    trigger_source: str,
    region: str,
) -> None:
    body = str(market_report or "").strip()
    if not body:
        return
    title = (
        "# 🎯 Market Review"
        if str(getattr(config, "report_language", "zh")).strip().lower() == "en"
        else "# 🎯 大盘复盘"
    )
    if not any(body.startswith(item) for item in ("# 🎯 大盘复盘", "# 🎯 Market Review")):
        body = f"{title}\n\n{body}"
    try:
        date_str = datetime.now().strftime('%Y%m%d')
        report_filename = f"market_review_{date_str}.md"
        filepath = notifier.save_report_to_file(body, report_filename)
        logger.info(
            "[MarketReview] component=market_review action=save_reused_report "
            "trigger_source=%s region=%s path=%s",
            trigger_source,
            region,
            filepath,
        )
    except Exception as exc:
        logger.warning("复用大盘上下文保存大盘复盘报告失败: %s", exc)


def _apply_watchlist_tiering(
    config: Config,
    args: argparse.Namespace,
    stock_codes: List[str],
) -> List[str]:
    """定时批次的自选分级；关闭、--force-run 或任何异常时保持原列表（fail-open）。"""
    if (
        not stock_codes
        or not getattr(config, "watchlist_tiering_enabled", False)
        or getattr(args, "force_run", False)
    ):
        return stock_codes
    try:
        from data_provider.base import DataFetcherManager
        from src.repositories.portfolio_repo import PortfolioRepository
        from src.services.watchlist_tiering_service import select_scheduled_codes
        from src.storage import get_db

        db = get_db()
        result = select_scheduled_codes(
            list(stock_codes),
            config=config,
            db=db,
            fetcher_manager=DataFetcherManager(),
            portfolio_repo=PortfolioRepository(db),
        )
        for code in result.skipped:
            logger.info("自选分级跳过: %s（未到刷新间隔且无异动）", code)
        return result.to_analyze
    except Exception as exc:
        logger.warning("自选分级异常(fail-open，保持全量分析): %s", exc)
        return stock_codes


def run_full_analysis(
    config: Config,
    args: argparse.Namespace,
    stock_codes: Optional[List[str]] = None,
    *,
    raise_errors: bool = False,
    apply_tiering: bool = False,
) -> bool:
    """
    执行完整的分析流程（个股 + 大盘复盘）

    这是定时任务调用的主函数
    """
    # Import pipeline modules outside the broad try/except so that import-time
    # failures propagate to the caller instead of being silently swallowed.
    from src.core.market_review import run_market_review
    from src.core.pipeline import StockAnalysisPipeline

    try:
        _refresh_stock_index_cache_for_analysis(config)

        # Issue #529: Hot-reload STOCK_LIST from .env on each scheduled run
        if stock_codes is None:
            config.refresh_stock_list()

        # Issue #373: Trading day filter (per-stock, per-market)
        effective_codes = stock_codes if stock_codes is not None else config.stock_list
        filtered_codes, effective_region, should_skip = _compute_trading_day_filter(
            config, args, effective_codes
        )
        if should_skip:
            logger.info(
                "今日所有相关市场均为非交易日，跳过执行。可使用 --force-run 强制执行。"
            )
            return True
        if set(filtered_codes) != set(effective_codes):
            skipped = set(effective_codes) - set(filtered_codes)
            logger.info("今日休市股票已跳过: %s", skipped)
        stock_codes = filtered_codes

        # 自选股分级：仅定时批次生效，按持仓/计划、刷新间隔和异动降频
        if apply_tiering:
            stock_codes = _apply_watchlist_tiering(config, args, stock_codes)

        # 命令行参数 --single-notify 覆盖配置（#55）
        if getattr(args, 'single_notify', False):
            config.single_stock_notify = True

        # Issue #190: 个股与大盘复盘合并推送
        merge_notification = (
            getattr(config, 'merge_email_notification', False)
            and config.market_review_enabled
            and not getattr(args, 'no_market_review', False)
            and not config.single_stock_notify
        )

        # 创建调度器
        save_context_snapshot = None
        if getattr(args, 'no_context_snapshot', False):
            save_context_snapshot = False
        query_id = uuid.uuid4().hex
        market_review_region = (
            effective_region
            if effective_region is not None
            else (getattr(config, 'market_review_region', 'cn') or 'cn')
        )
        should_run_market_review = (
            config.market_review_enabled
            and not args.no_market_review
            and (market_review_region or '') != ''
        )
        should_use_daily_market_context = (
            should_run_market_review
            and getattr(config, 'daily_market_context_enabled', True)
        )
        analysis_reference_time = datetime.now(timezone.utc)
        daily_market_context_target_date = None
        if should_use_daily_market_context:
            daily_market_context_target_date = _resolve_daily_market_context_target_date(
                market_review_region,
                analysis_reference_time,
            )
        market_report = ""
        market_context_summary = ""
        market_context_full_report = ""
        market_context_generated_during_stock = False
        pipeline = StockAnalysisPipeline(
            config=config,
            max_workers=args.workers,
            query_id=query_id,
            query_source="cli",
            save_context_snapshot=save_context_snapshot,
            daily_market_context_enabled=should_use_daily_market_context,
            daily_market_context_allow_generate=should_use_daily_market_context,
        )
        if should_use_daily_market_context:
            # Prompt-side context can reuse historical summaries, while full-merge
            # content must avoid silently reusing unrelated historical reports.
            _prime_daily_market_context(
                config,
                pipeline=pipeline,
                region=market_review_region,
                no_market_review=args.no_market_review,
                allow_generate=False,
                target_date=daily_market_context_target_date,
                return_full_report=False,
            )
            (
                market_context_summary,
                market_context_full_report,
            ) = _prime_daily_market_context(
                config,
                pipeline=pipeline,
                region=market_review_region,
                no_market_review=args.no_market_review,
                allow_generate=False,
                target_date=daily_market_context_target_date,
                return_full_report=True,
                require_current_query_match=True,
            )

        # 1. 运行个股分析
        results = pipeline.run(
            stock_codes=stock_codes,
            dry_run=args.dry_run,
            send_notification=not args.no_notify,
            merge_notification=merge_notification,
            current_time=analysis_reference_time,
        )

        if should_use_daily_market_context and not market_context_summary:
            (
                market_context_summary,
                market_context_full_report,
            ) = _prime_daily_market_context(
                config,
                pipeline=pipeline,
                region=market_review_region,
                no_market_review=args.no_market_review,
                allow_generate=False,
                target_date=daily_market_context_target_date,
                return_full_report=True,
                require_current_query_match=True,
            )
            market_context_generated_during_stock = bool(market_context_summary)

        # Issue #128: 分析间隔 - 在个股分析和大盘分析之间添加延迟
        analysis_delay = getattr(config, 'analysis_delay', 0)

        # 2. 运行大盘复盘（如果启用且不是仅个股模式）
        if should_run_market_review:
            schedule_mode = bool(
                getattr(args, 'schedule', False)
                or getattr(config, 'schedule_enabled', False)
            )
            review_trigger_source = "schedule" if schedule_mode else "cli"
            can_reuse_market_context = (
                _can_reuse_market_context_for_review(
                    market_context_summary,
                    market_review_region,
                )
                if should_use_daily_market_context
                else False
            )

            can_skip_market_review = (
                (merge_notification or market_context_generated_during_stock)
                and can_reuse_market_context
                and bool(market_context_full_report or market_context_summary)
            )
            if can_skip_market_review:
                market_report = market_context_full_report or market_context_summary
                logger.info(
                    "复盘上下文可复用，跳过重复大盘复盘并复用上下文内容。"
                )
                _save_reused_market_review_report(
                    pipeline.notifier,
                    market_report,
                    config=config,
                    trigger_source=review_trigger_source,
                    region=market_review_region,
                )
                if (
                    market_context_generated_during_stock
                    and not merge_notification
                    and not args.no_notify
                    and pipeline.notifier.is_available()
                ):
                    if pipeline.notifier.send(
                        f"# 📈 大盘复盘\n\n{market_report}",
                        email_send_to_all=True,
                        route_type="report",
                    ):
                        logger.info("复用本轮大盘上下文推送大盘复盘成功")
                    else:
                        logger.warning("复用本轮大盘上下文推送大盘复盘失败")

            review_result = None
            if not can_skip_market_review:
                if analysis_delay > 0:
                    logger.info(f"等待 {analysis_delay} 秒后执行大盘复盘（避免API限流）...")
                    time.sleep(analysis_delay)

                review_result = _run_market_review_with_shared_lock(
                    config,
                    run_market_review,
                    notifier=pipeline.notifier,
                    analyzer=pipeline.analyzer,
                    search_service=pipeline.search_service,
                    send_notification=not args.no_notify,
                    merge_notification=merge_notification,
                    override_region=market_review_region,
                    query_id=query_id,
                    trigger_source=review_trigger_source,
                )
                # 如果复盘仍未执行成功，再做一次复用历史/缓存读取（防止与并发运行竞态）。
                if not review_result and should_use_daily_market_context:
                    (
                        market_context_summary,
                        market_context_full_report,
                    ) = _prime_daily_market_context(
                        config,
                        pipeline=pipeline,
                        region=market_review_region,
                        no_market_review=args.no_market_review,
                        allow_generate=False,
                        target_date=daily_market_context_target_date,
                        return_full_report=True,
                        require_current_query_match=True,
                    )
                    can_reuse_market_context = _can_reuse_market_context_for_review(
                        market_context_summary,
                        market_review_region,
                    )
                elif not review_result:
                    can_reuse_market_context = False

            # 如果有结果，赋值给 market_report 用于后续飞书文档生成
            if review_result:
                market_report = _market_review_report_text(review_result)
            elif can_reuse_market_context:
                market_report = market_context_full_report or market_context_summary

        # Issue #190: 合并推送（个股+大盘复盘）
        if merge_notification and (results or market_report) and not args.no_notify:
            parts = []
            if market_report:
                parts.append(f"# 📈 大盘复盘\n\n{market_report}")
            if results:
                dashboard_content = pipeline.notifier.generate_aggregate_report(
                    results,
                    getattr(config, 'report_type', 'simple'),
                )
                parts.append(f"# 🚀 个股决策仪表盘\n\n{dashboard_content}")
            if parts:
                combined_content = "\n\n---\n\n".join(parts)
                if pipeline.notifier.is_available():
                    if pipeline.notifier.send(combined_content, email_send_to_all=True, route_type="report"):
                        logger.info("已合并推送（个股+大盘复盘）")
                    else:
                        logger.warning("合并推送失败")

        # 输出摘要
        if results:
            logger.info("\n===== 分析结果摘要 =====")
            for r in sorted(results, key=lambda x: x.sentiment_score, reverse=True):
                emoji = r.get_emoji()
                logger.info(
                    f"{emoji} {r.name}({r.code}): {r.operation_advice} | "
                    f"评分 {r.sentiment_score} | {r.trend_prediction}"
                )

        logger.info("\n任务执行完成")

        # === 新增：生成飞书云文档 ===
        try:
            from src.feishu_doc import FeishuDocManager

            feishu_doc = FeishuDocManager()
            if feishu_doc.is_configured() and (results or market_report):
                logger.info("正在创建飞书云文档...")

                # 1. 准备标题 "01-01 13:01大盘复盘"
                tz_cn = timezone(timedelta(hours=8))
                now = datetime.now(tz_cn)
                doc_title = f"{now.strftime('%Y-%m-%d %H:%M')} 大盘复盘"

                # 2. 准备内容 (拼接个股分析和大盘复盘)
                full_content = ""

                # 添加大盘复盘内容（如果有）
                if market_report:
                    full_content += f"# 📈 大盘复盘\n\n{market_report}\n\n---\n\n"

                # 添加个股决策仪表盘（使用 NotificationService 生成，按 report_type 分支）
                if results:
                    dashboard_content = pipeline.notifier.generate_aggregate_report(
                        results,
                        getattr(config, 'report_type', 'simple'),
                    )
                    full_content += f"# 🚀 个股决策仪表盘\n\n{dashboard_content}"

                # 3. 创建文档
                doc_url = feishu_doc.create_daily_doc(doc_title, full_content)
                if doc_url:
                    logger.info(f"飞书云文档创建成功: {doc_url}")
                    # 可选：将文档链接也推送到群里
                    if not args.no_notify:
                        pipeline.notifier.send(
                            f"[{now.strftime('%Y-%m-%d %H:%M')}] 复盘文档创建成功: {doc_url}",
                            route_type="report",
                        )

        except Exception as e:
            logger.error(f"飞书文档生成失败: {e}")

        # === Auto backtest ===
        try:
            if getattr(config, 'backtest_enabled', False):
                from src.services.backtest_service import BacktestService

                logger.info("开始自动回测...")
                service = BacktestService()
                stats = service.run_backtest(
                    force=False,
                    eval_window_days=getattr(config, 'backtest_eval_window_days', 10),
                    min_age_days=getattr(config, 'backtest_min_age_days', 14),
                    limit=200,
                )
                logger.info(
                    f"自动回测完成: processed={stats.get('processed')} saved={stats.get('saved')} "
                    f"completed={stats.get('completed')} insufficient={stats.get('insufficient')} errors={stats.get('errors')}"
                )
        except Exception as e:
            logger.warning(f"自动回测失败（已忽略）: {e}")

        # Pipeline.run keeps successful reports when individual stocks fail.
        # Preserve those reports/notifications, but do not call a partial batch
        # successful merely because the pipeline returned without raising.
        if raise_errors and not args.dry_run and len(results) < len(stock_codes):
            raise RuntimeError(
                f"analysis_partial_failure: succeeded={len(results)} expected={len(stock_codes)}"
            )
        return True

    except Exception as e:
        logger.exception(f"分析流程执行失败: {e}")
        if raise_errors:
            raise
        return False


def _run_paper_trading_monitor_if_enabled(config: Config) -> None:
    """Run the auxiliary paper ledger once after the complete model batch."""

    if not getattr(config, "paper_trading_enabled", False):
        return
    try:
        stats = _build_paper_trading_service(config).run_monitor()
        logger.info(
            "盘后模拟交易监控完成: date=%s plans=%s opened=%s closed=%s failed_closed=%s",
            stats.get("evaluation_date"),
            stats.get("plans_evaluated"),
            stats.get("positions_opened"),
            stats.get("positions_closed"),
            stats.get("failed_closed"),
        )
    except Exception as exc:
        # The batch isolates auxiliary failures; propagate so scheduler status
        # records failure without invalidating already-persisted reports.
        logger.warning("盘后模拟交易监控失败（已保留分析报告）: %s", exc, exc_info=True)
        raise


def _run_market_opportunity_pool_if_enabled(config: Config) -> None:
    """Run the market-review driven opportunity pool after a completed batch."""

    if not _market_opportunity_pool_enabled(config):
        return
    try:
        from src.services.market_opportunity_service import MarketOpportunityService

        region = _resolve_market_opportunity_region(config)
        if region is None:
            logger.info("大盘风口机会池跳过: market_review_region 不包含 A 股 CN")
            return
        stats = MarketOpportunityService(config=config).run_after_market_review(
            market=region,
            current_time=datetime.now(timezone.utc),
            owner="scheduled-analysis",
        )
        discovery = stats.get("discovery", {}) if isinstance(stats, dict) else {}
        jobs = stats.get("jobs", {}) if isinstance(stats, dict) else {}
        research = discovery.get("research") if isinstance(discovery.get("research"), dict) else {}
        research_outcomes = stats.get("research_outcomes") if isinstance(stats, dict) else None
        logger.info(
            "大盘风口机会池完成: status=%s run_id=%s candidates=%s jobs=%s processed=%s failed=%s reason=%s "
            "research_block=%s research_outcomes=%s",
            stats.get("status") if isinstance(stats, dict) else None,
            discovery.get("run_id"),
            discovery.get("selected_candidate_count"),
            discovery.get("job_count"),
            jobs.get("processed"),
            jobs.get("failed"),
            discovery.get("reason"),
            research.get("block_reason"),
            research_outcomes if isinstance(research_outcomes, dict) else {},
        )
    except Exception as exc:
        logger.warning("大盘风口机会池失败（已保留分析报告）: %s", exc, exc_info=True)
        raise


def _market_opportunity_pool_enabled(config: Config) -> bool:
    try:
        from src.services.runtime_settings_service import RuntimeSettingsService

        mode = RuntimeSettingsService(config=config).get_market_opportunity_mode().value
        return str(mode or "off").strip().lower() != "off"
    except Exception as exc:
        logger.warning("大盘风口机会池模式解析失败，已 fail-closed 跳过: %s", exc, exc_info=True)
        return False


def _resolve_market_opportunity_region(config: Config) -> Optional[str]:
    region = str(getattr(config, "market_review_region", "cn") or "cn").strip().lower()
    parts = {part.strip() for part in region.split(",") if part.strip()}
    if region in {"", "both"} or "cn" in parts or "a" in parts:
        return "cn"
    return None


_market_opportunity_evening_last_session: Optional[str] = None

# The server's runtime scheduler and the analyzer's CLI scheduler both register
# the evening task.  Their shared lease is per 5-minute bucket, so a later
# bucket could start a second run; this per-session DB lease admits one.
_MARKET_OPPORTUNITY_EVENING_SESSION_TASK = "market_opportunity_evening_session"
# A crashed or recreated owner frees the session only after this expires.
_MARKET_OPPORTUNITY_EVENING_SESSION_LEASE_SECONDS = 3 * 3600


def _claim_market_opportunity_evening_session(session_key: str) -> Dict[str, Any]:
    from src.services.scheduler_heartbeat import claim_scheduler_task_bucket

    return claim_scheduler_task_bucket(
        task_name=_MARKET_OPPORTUNITY_EVENING_SESSION_TASK,
        bucket_started_at=datetime.combine(date.fromisoformat(session_key), datetime_time()),
        lease_seconds=_MARKET_OPPORTUNITY_EVENING_SESSION_LEASE_SECONDS,
        owner_kind="evening_pool",
        details={"session": session_key},
    )


def _finish_market_opportunity_evening_session(lease_key: str, error_message: Optional[str] = None) -> None:
    from src.services.scheduler_heartbeat import finish_scheduler_task_lease

    # A failure is terminal too ("finished" + last_error): no other instance
    # retries the same session, matching the in-process single attempt.
    try:
        finish_scheduler_task_lease(
            lease_key=lease_key,
            status="finished" if error_message else "succeeded",
            error_message=error_message,
        )
    except Exception as exc:
        logger.warning("大盘风口机会池晚间运行租约收尾失败: lease=%s error=%s", lease_key, exc)


def _run_market_opportunity_pool_evening_if_due(config: Config):
    """Run the opportunity pool once per CN session after MARKET_OPPORTUNITY_RUN_TIME.

    End-of-day bars are often still incomplete right after the close, so the
    pool can be deferred to the evening.  It still waits for the session's
    market review and for any running analysis batch.  One attempt per session
    across all scheduler instances (per-session DB lease); a run after a
    restart reuses the session's discovery run.
    """

    run_time = str(getattr(config, "market_opportunity_run_time", "") or "").strip()
    if not run_time:
        return {"status": "skipped", "reason": "evening_run_disabled"}
    china_now = datetime.now(ZoneInfo("Asia/Shanghai"))
    if china_now.strftime("%H:%M") < run_time:
        return {"status": "skipped", "reason": "before_run_time"}
    session_key = china_now.date().isoformat()
    global _market_opportunity_evening_last_session
    if _market_opportunity_evening_last_session == session_key:
        return {"status": "skipped", "reason": "already_ran_today"}
    try:
        from src.core.trading_calendar import is_market_open

        if not is_market_open("cn", china_now.date(), strict=True):
            return {"status": "skipped", "reason": "market_closed"}
    except Exception:
        # Unverifiable calendar is not a holiday: keep the session and check
        # again later; the pool's own new-work gate fails closed here too.
        return {"status": "skipped", "reason": "trading_calendar_unavailable"}
    if not _market_opportunity_pool_enabled(config):
        return {"status": "skipped", "reason": "pool_disabled"}
    region = _resolve_market_opportunity_region(config)
    if region is None:
        return {"status": "skipped", "reason": "market_review_region_not_cn"}
    from src.services.runtime_scheduler import global_analysis_running

    if global_analysis_running():
        return {"status": "skipped", "reason": "analysis_busy"}
    from src.services.market_opportunity_service import MarketOpportunityService

    readiness = MarketOpportunityService(config=config).market_review_readiness(
        market=region,
        current_time=datetime.now(timezone.utc),
    )
    if not readiness.get("ready"):
        return {"status": "skipped", "reason": str(readiness.get("reason") or "market_review_not_ready")}
    try:
        claim = _claim_market_opportunity_evening_session(session_key)
    except Exception as exc:
        # Unverifiable claim: do not run (could duplicate) and do not consume
        # the session; the next tick tries again.
        logger.warning("大盘风口机会池晚间运行租约领取失败，稍后重试: %s", exc)
        return {"status": "skipped", "reason": "session_claim_unavailable"}
    if not claim.get("claimed"):
        # Not consumed locally either, so an expired lease of a crashed owner
        # can still be taken over by this instance later.
        return {"status": "skipped", "reason": "session_claimed_by_other_instance"}
    _market_opportunity_evening_last_session = session_key
    logger.info(
        "大盘风口机会池晚间运行开始: run_time=%s session=%s market_review_id=%s",
        run_time,
        readiness.get("session"),
        readiness.get("report_id"),
    )
    lease_key = str(claim.get("lease_key") or "")
    try:
        _run_market_opportunity_pool_if_enabled(config)
    except Exception as exc:
        _finish_market_opportunity_evening_session(lease_key, error_message=str(exc) or type(exc).__name__)
        raise
    _finish_market_opportunity_evening_session(lease_key)
    return None


def _build_paper_trading_service(config: Config):
    from src.services.paper_trading_service import PaperTradingService

    return PaperTradingService.from_config(config)


_paper_trading_intraday_last_attempt_monotonic: Optional[float] = None
_paper_trading_opening_confirmation_last_attempt_monotonic: Optional[float] = None


def _run_paper_trading_intraday_monitor_if_enabled(config: Config):
    """Run one realtime quote paper-monitor sweep inside schedule mode."""

    if not (
        getattr(config, "paper_trading_enabled", False)
        and getattr(config, "paper_trading_intraday_enabled", False)
    ):
        return {"status": "skipped", "reason": "monitor_disabled"}
    interval_minutes = max(1, getattr(config, "paper_trading_intraday_interval_minutes", 1))
    interval_seconds = interval_minutes * 60
    global _paper_trading_intraday_last_attempt_monotonic
    now_monotonic = time.monotonic()
    if (
        _paper_trading_intraday_last_attempt_monotonic is not None
        and now_monotonic - _paper_trading_intraday_last_attempt_monotonic < interval_seconds
    ):
        return {"status": "skipped", "reason": "monitor_throttled"}
    _paper_trading_intraday_last_attempt_monotonic = now_monotonic
    started_perf = time.perf_counter()
    try:
        stats = _build_paper_trading_service(config).run_intraday_monitor(
            interval_seconds=interval_seconds,
        )
        logger.info(
            "盘中模拟交易监控完成: bucket=%s observations=%s opened=%s closed=%s "
            "failed_closed=%s skipped_closed_market=%s duration_seconds=%.2f",
            stats.get("bucket_started_at"),
            stats.get("observations"),
            stats.get("positions_opened"),
            stats.get("positions_closed"),
            stats.get("failed_closed"),
            stats.get("skipped_closed_market"),
            time.perf_counter() - started_perf,
        )
        return stats
    except Exception as exc:
        logger.warning("盘中模拟交易监控失败: %s", exc, exc_info=True)
        raise


def _run_paper_trading_opening_confirmation_if_enabled(config: Config):
    """Run the independent CN opening-confirmation quote sweep."""

    if not (
        getattr(config, "paper_trading_enabled", False)
        and getattr(config, "paper_trading_intraday_enabled", False)
    ):
        return {"status": "skipped", "reason": "monitor_disabled"}
    china_now = datetime.now(ZoneInfo("Asia/Shanghai"))
    china_clock = china_now.time().replace(tzinfo=None)
    if not (datetime_time(9, 30) <= china_clock <= datetime_time(9, 35)):
        return {"status": "skipped", "reason": "outside_opening_window"}
    try:
        from src.core.trading_calendar import is_market_open

        if not is_market_open("cn", china_now.date()):
            return {"status": "skipped", "reason": "market_closed"}
    except Exception:
        # The trading calendar is fail-open elsewhere in the project.  Keep
        # that behavior here so a missing optional calendar package does not
        # silently disable a configured local monitor.
        pass
    global _paper_trading_opening_confirmation_last_attempt_monotonic
    now_monotonic = time.monotonic()
    if (
        _paper_trading_opening_confirmation_last_attempt_monotonic is not None
        and now_monotonic - _paper_trading_opening_confirmation_last_attempt_monotonic < 30
    ):
        return {"status": "skipped", "reason": "monitor_throttled"}
    _paper_trading_opening_confirmation_last_attempt_monotonic = now_monotonic
    started_perf = time.perf_counter()
    try:
        stats = _build_paper_trading_service(config).run_opening_confirmation_check()
        logger.info(
            "开盘确认模拟交易校验完成: bucket=%s observations=%s confirmed=%s "
            "reanalysis_requested=%s failed_closed=%s "
            "duration_seconds=%.2f",
            stats.get("bucket_started_at"),
            stats.get("observations"),
            stats.get("confirmed"),
            stats.get("reanalysis_requested"),
            stats.get("failed_closed"),
            time.perf_counter() - started_perf,
        )
    except Exception as exc:
        logger.warning("开盘确认模拟交易校验失败: %s", exc, exc_info=True)
        raise


def _run_paper_trading_opening_auction_if_enabled(config: Config) -> None:
    """Backward-compatible alias for callers upgraded before the task rename."""

    _run_paper_trading_opening_confirmation_if_enabled(config)


def run_scheduled_analysis(
    config: Config,
    args: argparse.Namespace,
    stock_codes: Optional[List[str]] = None,
) -> bool:
    """Run one scheduled batch, optionally once per configured LLM route.

    Multi-model runs are intentionally sequential: the stock universe is frozen
    once for the whole batch, while each model receives an isolated config with
    cross-model fallback disabled.  This avoids concurrent provider/cache writes
    and ensures every stored history row identifies the model that produced it.
    """
    from src.services.analysis_model_service import (
        build_model_scoped_config,
        list_analysis_models,
    )

    failures: List[str] = []
    models = [None]
    scheduled_stock_codes = stock_codes
    try:
        if getattr(config, "schedule_multi_model_enabled", False):
            if stock_codes is None:
                config.refresh_stock_list()
                scheduled_stock_codes = list(config.stock_list)
            else:
                scheduled_stock_codes = list(stock_codes)
            models = list_analysis_models(config)
            if not models:
                raise RuntimeError("定时多模型已启用，但未找到可用的 LLM 模型配置")
            logger.info("定时多模型分析已启用: models=%s stocks=%s", models, scheduled_stock_codes)
    except Exception as exc:
        models = []
        failures.append(f"analysis_setup: {exc}")
        logger.exception("定时分析准备失败")

    # 多模型批次冻结股票后只做一次自选分级，保证每个模型拿到同一批股票；
    # 单模型批次在 run_full_analysis 内（热读取 STOCK_LIST 与交易日过滤之后）分级。
    multi_model = bool(getattr(config, "schedule_multi_model_enabled", False))
    if multi_model and scheduled_stock_codes:
        scheduled_stock_codes = _apply_watchlist_tiering(config, args, scheduled_stock_codes)

    for model in models:
        try:
            scoped_config = build_model_scoped_config(config, model) if model else config
            if model:
                # Isolate multi-model Markdown artifacts as well as histories.
                scoped_config.report_filename_suffix = model
                logger.info("定时多模型分析开始: model=%s fallback=disabled", model)
            succeeded = run_full_analysis(
                scoped_config,
                args,
                scheduled_stock_codes,
                raise_errors=True,
                apply_tiering=not multi_model,
            )
            if not succeeded:
                failures.append(f"{model or 'default_model'}: analysis_unsuccessful (reported failure)")
        except Exception as exc:
            failures.append(f"{model or 'default_model'}: {exc}")
            logger.exception("定时分析失败: model=%s", model or "default_model")

    # Auxiliary ledgers must still advance when one model route fails.  The
    # opportunity pool will independently require a current complete market
    # review, while the paper monitor must continue protecting existing lots.
    # Run both before surfacing the partial batch failure to the scheduler.
    # MARKET_OPPORTUNITY_RUN_TIME moves the pool to its evening background task.
    auxiliary_tasks = [("paper_monitor", _run_paper_trading_monitor_if_enabled)]
    evening_run_time = str(getattr(config, "market_opportunity_run_time", "") or "").strip()
    if evening_run_time:
        logger.info("大盘风口机会池改为晚间运行（MARKET_OPPORTUNITY_RUN_TIME=%s），本批次不内联执行", evening_run_time)
    else:
        auxiliary_tasks.insert(0, ("market_opportunity", _run_market_opportunity_pool_if_enabled))
    for name, task in auxiliary_tasks:
        try:
            task(config)
        except Exception as exc:
            failures.append(f"{name}: {exc}")
            logger.exception("定时辅助任务失败: %s", name)
    if failures:
        raise RuntimeError("定时分析/辅助任务存在失败: " + "; ".join(failures))
    return True


def _run_analysis_with_runtime_scheduler_lock(
    config: Config,
    args: argparse.Namespace,
    stock_codes: Optional[List[str]] = None,
) -> None:
    from src.services.runtime_scheduler import run_with_global_analysis_lock

    # Keep startup/triggered analysis in sync with API runtime scheduler and
    # run-now entrypoint. Blocking is expected here because startup paths should
    # wait for an in-flight job before returning a response.
    run_with_global_analysis_lock(
        task_runner=run_full_analysis,
        config=config,
        args=args,
        stock_codes=stock_codes,
        blocking=True,
    )


def start_api_server(host: str, port: int, config: Config) -> None:
    """
    在后台线程启动 FastAPI 服务

    Args:
        host: 监听地址
        port: 监听端口
        config: 配置对象
    """
    import socket
    import threading
    import uvicorn

    probe = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind((host, port))
    except OSError as exc:
        raise RuntimeError(f"FastAPI port is not available: {host}:{port}") from exc
    finally:
        probe.close()

    level_name = (config.log_level or "INFO").lower()
    use_config_signal_handlers = True
    uvicorn_kwargs = {
        "host": host,
        "port": port,
        "log_level": level_name,
        "log_config": None,
    }
    # Import the ASGI app object in the calling thread instead of handing uvicorn
    # the "api.app:app" import string. With the string, uvicorn imports the app
    # lazily inside the server thread, and that import (litellm + the full app
    # tree, ~10s+ on constrained hosts) runs inside the startup probe window
    # below, tripping the 3.0s timeout and causing a restart loop on slower
    # machines. Importing first keeps the heavy work out of the probe window;
    # genuine import failures still surface immediately to the caller.
    from api.app import app as fastapi_app

    try:
        uvicorn_config = uvicorn.Config(
            fastapi_app,
            install_signal_handlers=False,
            **uvicorn_kwargs,
        )
    except TypeError:
        # Older uvicorn versions do not accept install_signal_handlers in
        # Config; fall back and only disable signal handling via Server attribute
        # when it's a boolean flag.
        use_config_signal_handlers = False
        uvicorn_config = uvicorn.Config(
            fastapi_app,
            **uvicorn_kwargs,
        )
    uvicorn_server = uvicorn.Server(config=uvicorn_config)
    if not use_config_signal_handlers:
        install_signal_handlers = getattr(uvicorn_server, "install_signal_handlers", None)
        if isinstance(install_signal_handlers, bool):
            uvicorn_server.install_signal_handlers = False

    startup_error: list[BaseException] = []

    def run_server():
        try:
            uvicorn_server.run()
        except Exception as exc:  # noqa: BLE001 - surface startup issues to caller promptly
            startup_error.append(exc)

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()

    timeout_seconds = 3.0
    wait_deadline = time.time() + timeout_seconds
    while time.time() < wait_deadline:
        if startup_error:
            raise RuntimeError(
                f"FastAPI server failed to start: {host}:{port}; {startup_error[0]}"
            )
        if uvicorn_server.started:
            logger.info(f"FastAPI 服务已启动: http://{host}:{port}")
            return
        if not thread.is_alive():
            break
        time.sleep(0.05)

    if startup_error:
        raise RuntimeError(f"FastAPI server failed to start: {host}:{port}; {startup_error[0]}")
    if uvicorn_server.started:
        logger.info(f"FastAPI 服务已启动: http://{host}:{port}")
        return
    if not thread.is_alive():
        raise RuntimeError(f"FastAPI 服务器启动后立即退出: {host}:{port}")

    raise RuntimeError(f"FastAPI 服务在 {timeout_seconds:.1f}s 内未完成启动: {host}:{port}")


def _is_truthy_env(var_name: str, default: str = "true") -> bool:
    """Parse common truthy / falsy environment values."""
    value = os.getenv(var_name, default).strip().lower()
    return value not in {"0", "false", "no", "off"}


def start_bot_stream_clients(config: Config) -> None:
    """Start bot stream clients when enabled in config."""
    # 启动钉钉 Stream 客户端
    if config.dingtalk_stream_enabled:
        try:
            from bot.platforms import start_dingtalk_stream_background, DINGTALK_STREAM_AVAILABLE
            if DINGTALK_STREAM_AVAILABLE:
                if start_dingtalk_stream_background():
                    logger.info("[Main] Dingtalk Stream client started in background.")
                else:
                    logger.warning("[Main] Dingtalk Stream client failed to start.")
            else:
                logger.warning("[Main] Dingtalk Stream enabled but SDK is missing.")
                logger.warning("[Main] Run: pip install dingtalk-stream")
        except Exception as exc:
            logger.error(f"[Main] Failed to start Dingtalk Stream client: {exc}")

    # 启动飞书 Stream 客户端
    if getattr(config, 'feishu_stream_enabled', False):
        try:
            from bot.platforms import start_feishu_stream_background, FEISHU_SDK_AVAILABLE
            if FEISHU_SDK_AVAILABLE:
                if start_feishu_stream_background():
                    logger.info("[Main] Feishu Stream client started in background.")
                else:
                    logger.warning("[Main] Feishu Stream client failed to start.")
            else:
                logger.warning("[Main] Feishu Stream enabled but SDK is missing.")
                logger.warning("[Main] Run: pip install lark-oapi")
        except Exception as exc:
            logger.error(f"[Main] Failed to start Feishu Stream client: {exc}")


def _resolve_scheduled_stock_codes(stock_codes: Optional[List[str]]) -> Optional[List[str]]:
    """Scheduled runs should always read the latest persisted watchlist."""
    if stock_codes is not None:
        logger.warning(
            "定时模式下检测到 --stocks 参数；计划执行将忽略启动时股票快照，并在每次运行前重新读取最新的 STOCK_LIST。"
        )
    return None


def _reload_runtime_config() -> Config:
    """Reload config from the latest persisted `.env` values for scheduled runs."""
    _reload_env_file_values_preserving_overrides()
    Config.reset_instance()
    return get_config()


def _build_schedule_time_provider(default_schedule_time: str):
    """Read the latest schedule time directly from the active config file.

    Fallback order:
    1. Process-level env override (set before launch) → honour it.
    2. Persisted config file value (written by WebUI) → use it.
    3. Documented system default ``"18:00"`` → always fall back here so
       that clearing SCHEDULE_TIME in WebUI correctly resets the schedule.
    """
    from src.core.config_manager import ConfigManager

    _SYSTEM_DEFAULT_SCHEDULE_TIME = "18:00"
    manager = ConfigManager()

    def _provider() -> str:
        if "SCHEDULE_TIME" in _INITIAL_PROCESS_ENV:
            return os.getenv("SCHEDULE_TIME", default_schedule_time)

        config_map = manager.read_config_map()
        schedule_time = (config_map.get("SCHEDULE_TIME", "") or "").strip()
        if schedule_time:
            return schedule_time
        return _SYSTEM_DEFAULT_SCHEDULE_TIME

    return _provider


def _build_schedule_times_provider(default_schedule_time: str):
    """Read the latest SCHEDULE_TIMES with SCHEDULE_TIME fallback."""
    from src.core.config_manager import ConfigManager
    from src.scheduler import normalize_schedule_times

    _SYSTEM_DEFAULT_SCHEDULE_TIME = "18:00"
    manager = ConfigManager()

    def _provider():
        if "SCHEDULE_TIMES" in _INITIAL_PROCESS_ENV:
            return normalize_schedule_times(
                os.getenv("SCHEDULE_TIMES", ""),
                fallback_time=os.getenv("SCHEDULE_TIME", default_schedule_time),
            )
        if "SCHEDULE_TIME" in _INITIAL_PROCESS_ENV:
            return normalize_schedule_times(
                os.getenv("SCHEDULE_TIMES", ""),
                fallback_time=os.getenv("SCHEDULE_TIME", default_schedule_time),
            )

        config_map = manager.read_config_map()
        schedule_time = (config_map.get("SCHEDULE_TIME", "") or "").strip() or _SYSTEM_DEFAULT_SCHEDULE_TIME
        return normalize_schedule_times(
            config_map.get("SCHEDULE_TIMES", ""),
            fallback_time=schedule_time,
        )

    return _provider


def main() -> int:
    """
    主入口函数

    Returns:
        退出码（0 表示成功）
    """
    # 解析命令行参数
    args = parse_arguments()

    # 在配置加载前先初始化 bootstrap 日志，确保早期失败也能落盘
    try:
        _setup_bootstrap_logging(debug=args.debug)
    except Exception as exc:
        logging.basicConfig(
            level=logging.DEBUG if getattr(args, "debug", False) else logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            stream=sys.stderr,
        )
        logger.warning("Bootstrap 日志初始化失败，已回退到 stderr: %s", exc)

    # 加载配置（在 bootstrap logging 之后执行，确保异常有日志）
    try:
        config = get_config()
    except Exception as exc:
        logger.exception("加载配置失败: %s", exc)
        return 1

    # 配置日志（输出到控制台和文件）
    try:
        _setup_runtime_logging(config.log_dir, debug=args.debug)
    except Exception as exc:
        logger.exception("切换到配置日志目录失败: %s", exc)
        return 1

    logger.info("=" * 60)
    logger.info("A股自选股智能分析系统 启动")
    logger.info(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # 验证配置
    warnings = config.validate()
    for warning in warnings:
        logger.warning(warning)

    if getattr(args, "check_notify", False):
        from src.services.notification_diagnostics import (
            format_notification_diagnostics,
            run_notification_diagnostics,
        )

        result = run_notification_diagnostics(config)
        print(format_notification_diagnostics(result))
        return 0 if result.ok else 1

    # 解析股票列表（统一为大写 Issue #355）
    stock_codes = None
    if args.stocks:
        stock_codes = [
            resolve_index_stock_code_for_analysis(c)
            for c in split_stock_list(args.stocks)
            if (c or "").strip()
        ]
        logger.info(f"使用命令行指定的股票列表: {stock_codes}")

    # === 处理 --webui / --webui-only 参数，映射到 --serve / --serve-only ===
    if args.webui:
        args.serve = True
    if args.webui_only:
        args.serve_only = True

    # 兼容旧版 WEBUI_ENABLED 环境变量
    if config.webui_enabled and not (args.serve or args.serve_only):
        args.serve = True

    # === 启动 Web 服务 (如果启用) ===
    start_serve = (args.serve or args.serve_only) and os.getenv("GITHUB_ACTIONS") != "true"

    if start_serve:
        args.host, args.port = _resolve_web_service_bind(args, config)
        _warn_if_public_webui_without_auth(args.host)

    bot_clients_started = False
    if start_serve:
        from src.services.runtime_scheduler import (
            CLI_SCHEDULER_OWNER_ENV,
            RUNTIME_SCHEDULER_ARGS_ENV,
            RUNTIME_SCHEDULER_FORCE_ENABLED_ENV,
            RUNTIME_SCHEDULER_RUN_IMMEDIATELY_ENV,
            RUNTIME_SCHEDULER_SUPPRESS_START_ENV,
        )

        # The API runtime scheduler owns schedules once the Web/API service starts.
        # This keeps Web settings, status, and run-now actions attached to the real
        # scheduler instead of a separate CLI loop.
        os.environ.pop(CLI_SCHEDULER_OWNER_ENV, None)
        if args.serve_only:
            os.environ[RUNTIME_SCHEDULER_SUPPRESS_START_ENV] = "true"
        else:
            os.environ.pop(RUNTIME_SCHEDULER_SUPPRESS_START_ENV, None)
        runtime_schedule_requested = not args.serve_only and (
            args.schedule or config.schedule_enabled
        )
        if not args.serve_only and args.schedule:
            os.environ[RUNTIME_SCHEDULER_FORCE_ENABLED_ENV] = "true"
        else:
            os.environ.pop(RUNTIME_SCHEDULER_FORCE_ENABLED_ENV, None)
        if runtime_schedule_requested:
            runtime_run_immediately = config.schedule_run_immediately
            if getattr(args, 'no_run_immediately', False):
                runtime_run_immediately = False
            os.environ[RUNTIME_SCHEDULER_RUN_IMMEDIATELY_ENV] = (
                "true" if runtime_run_immediately else "false"
            )
        else:
            os.environ.pop(RUNTIME_SCHEDULER_RUN_IMMEDIATELY_ENV, None)
        os.environ[RUNTIME_SCHEDULER_ARGS_ENV] = json.dumps({
            "no_notify": bool(getattr(args, "no_notify", False)),
            "no_market_review": bool(getattr(args, "no_market_review", False)),
            "dry_run": bool(getattr(args, "dry_run", False)),
            "force_run": bool(getattr(args, "force_run", False)),
            "single_notify": bool(getattr(args, "single_notify", False)),
            "no_context_snapshot": bool(getattr(args, "no_context_snapshot", False)),
            "workers": getattr(args, "workers", None),
        })
        if not prepare_webui_frontend_assets():
            logger.warning("前端静态资源未就绪，继续启动 FastAPI 服务（Web 页面可能不可用）")
        try:
            start_api_server(host=args.host, port=args.port, config=config)
            bot_clients_started = True
        except Exception as e:
            logger.error(f"启动 FastAPI 服务失败: {e}")
            if args.serve_only:
                return 1
            start_serve = False

    if bot_clients_started:
        start_bot_stream_clients(config)

    # === 仅 Web 服务模式：不自动执行分析 ===
    if args.serve_only:
        logger.info("模式: 仅 Web 服务")
        logger.info(f"Web 服务运行中: http://{args.host}:{args.port}")
        logger.info("通过 /api/v1/analysis/analyze 接口触发分析")
        logger.info(f"API 文档: http://{args.host}:{args.port}/docs")
        logger.info("按 Ctrl+C 退出...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\n用户中断，程序退出")
        return 0

    try:
        # 模式0: 回测
        if getattr(args, 'backtest', False):
            logger.info("模式: 回测")
            from src.services.backtest_service import BacktestService

            service = BacktestService()
            stats = service.run_backtest(
                code=getattr(args, 'backtest_code', None),
                force=getattr(args, 'backtest_force', False),
                eval_window_days=getattr(args, 'backtest_days', None),
            )
            logger.info(
                f"回测完成: processed={stats.get('processed')} saved={stats.get('saved')} "
                f"completed={stats.get('completed')} insufficient={stats.get('insufficient')} errors={stats.get('errors')}"
            )
            return 0

        # 模式1: 仅大盘复盘
        if args.market_review:
            from src.core.market_review import run_market_review
            from src.core.market_review_runtime import build_market_review_runtime

            # Issue #373: Trading day check for market-review-only mode.
            # Do NOT use _compute_trading_day_filter here: that helper checks
            # config.market_review_enabled, which would wrongly block an
            # explicit --market-review invocation when the flag is disabled.
            effective_region = None
            if not getattr(args, 'force_run', False) and getattr(config, 'trading_day_check_enabled', True):
                from src.core.trading_calendar import (
                    get_open_markets_today, compute_effective_region as _compute_region, MARKET_TIMEZONE,
                )
                requested_region = _compute_region(
                    getattr(config, 'market_review_region', 'cn') or 'cn', set(MARKET_TIMEZONE)
                )
                open_markets = get_open_markets_today(markets=set((requested_region or '').split(',')) - {''})
                effective_region = _compute_region(
                    getattr(config, 'market_review_region', 'cn') or 'cn', open_markets
                )
                if effective_region == '':
                    logger.info("今日大盘复盘相关市场均为非交易日，跳过执行。可使用 --force-run 强制执行。")
                    return 0

            logger.info("模式: 仅大盘复盘")
            notifier, analyzer, search_service = build_market_review_runtime(config)

            _run_market_review_with_shared_lock(
                config,
                run_market_review,
                notifier=notifier,
                analyzer=analyzer,
                search_service=search_service,
                send_notification=not args.no_notify,
                override_region=effective_region,
                trigger_source="cli",
            )
            return 0

        # 模式2: 定时任务模式
        if args.schedule or config.schedule_enabled:
            if start_serve:
                logger.info("模式: Web/API runtime scheduler")
                logger.info(f"Web 服务运行中: http://{args.host}:{args.port}")
                logger.info("Web/API runtime scheduler 已接管定时任务，保存设置会作用于当前进程")
                logger.info("按 Ctrl+C 退出...")
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    logger.info("\n用户中断，程序退出")
                return 0

            logger.info("模式: 定时任务")
            logger.info(f"每日执行时间: {config.schedule_time}")

            # Determine whether to run immediately:
            # Command line arg --no-run-immediately overrides config if present.
            # Otherwise use config (defaults to True).
            should_run_immediately = config.schedule_run_immediately
            if getattr(args, 'no_run_immediately', False):
                should_run_immediately = False

            logger.info(f"启动时立即执行: {should_run_immediately}")

            from src.scheduler import run_with_schedule
            from src.services.runtime_scheduler import (
                build_market_opportunity_evening_background_tasks,
                run_with_global_analysis_lock,
            )
            scheduled_stock_codes = _resolve_scheduled_stock_codes(stock_codes)
            schedule_time_provider = _build_schedule_time_provider(config.schedule_time)
            schedule_times_provider = _build_schedule_times_provider(config.schedule_time)

            def _run_full_analysis_raising(task_config, task_args, task_stock_codes):
                return run_full_analysis(task_config, task_args, task_stock_codes, raise_errors=True, apply_tiering=True)

            def scheduled_task():
                runtime_config = _reload_runtime_config()
                if (
                    getattr(runtime_config, "schedule_multi_model_enabled", False)
                    or getattr(runtime_config, "paper_trading_enabled", False)
                    or _market_opportunity_pool_enabled(runtime_config)
                ):
                    runner = run_scheduled_analysis
                else:
                    runner = _run_full_analysis_raising
                # Hold the shared analysis lock so the evening opportunity pool
                # never starts while this batch is still running.
                return run_with_global_analysis_lock(runner, runtime_config, args, scheduled_stock_codes)

            background_tasks = []
            if getattr(config, 'agent_event_monitor_enabled', False):
                from src.services.alert_worker import AlertWorker

                interval_minutes = max(1, getattr(config, 'agent_event_monitor_interval_minutes', 5))
                alert_worker = AlertWorker(config_provider=_reload_runtime_config)

                def event_monitor_task():
                    stats = alert_worker.run_once()
                    triggered_count = stats.get("triggered", 0)
                    if triggered_count:
                        logger.info("[EventMonitor] 本轮触发 %d 条提醒", triggered_count)

                background_tasks.append({
                    "task": event_monitor_task,
                    "interval_seconds": interval_minutes * 60,
                    "run_immediately": True,
                    "name": "agent_event_monitor",
                })

            def paper_trading_intraday_task():
                return _run_paper_trading_intraday_monitor_if_enabled(_reload_runtime_config())

            def paper_trading_opening_confirmation_task():
                return _run_paper_trading_opening_confirmation_if_enabled(_reload_runtime_config())

            background_tasks.append({
                "task": paper_trading_opening_confirmation_task,
                "interval_seconds": 30,
                "run_immediately": False,
                "name": "paper_trading_opening_confirmation_0930",
                "lease_task_name": "paper_trading_opening_confirmation_0930",
                "active_time_start": "09:30:00",
                "active_time_end": "09:35:00",
                "active_timezone": "Asia/Shanghai",
            })

            paper_interval_minutes = max(
                1,
                int(getattr(config, "paper_trading_intraday_interval_minutes", 5) or 5),
            )
            background_tasks.append({
                "task": paper_trading_intraday_task,
                "interval_seconds": paper_interval_minutes * 60,
                "run_immediately": False,
                "name": "paper_trading_intraday_monitor",
                "lease_task_name": "paper_trading_intraday_monitor",
            })
            background_tasks.extend(
                build_market_opportunity_evening_background_tasks(
                    config,
                    config_provider=_reload_runtime_config,
                    runner=_run_market_opportunity_pool_evening_if_due,
                )
            )

            schedule_kwargs = {
                "task": scheduled_task,
                "schedule_time": config.schedule_time,
                "run_immediately": should_run_immediately,
                "background_tasks": background_tasks,
                "schedule_time_provider": schedule_time_provider,
                "shared_lease_enabled": True,
            }
            if hasattr(config, "schedule_times"):
                schedule_kwargs["schedule_times"] = config.schedule_times
                schedule_kwargs["schedule_times_provider"] = schedule_times_provider
            run_with_schedule(**schedule_kwargs)
            return 0

        # 模式3: 正常单次运行
        if config.run_immediately:
            _run_analysis_with_runtime_scheduler_lock(config, args, stock_codes)
        else:
            logger.info("配置为不立即运行分析 (RUN_IMMEDIATELY=false)")

        logger.info("\n程序执行完成")

        # 如果启用了服务且是非定时任务模式，保持程序运行
        keep_running = start_serve and not (args.schedule or config.schedule_enabled)
        if keep_running:
            logger.info("API 服务运行中 (按 Ctrl+C 退出)...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass

        return 0

    except KeyboardInterrupt:
        logger.info("\n用户中断，程序退出")
        return 130

    except Exception as e:
        logger.exception(f"程序执行失败: {e}")
        return 1


if __name__ == "__main__":
    multiprocessing.freeze_support()
    sys.exit(main())
