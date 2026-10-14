# -*- coding: utf-8 -*-
"""
自选股分级分析服务

根据股票的持仓状态、上次分析时间、实时异动情况，决定定时分析中
哪些股票需要分析、哪些可以跳过。

分级规则（按优先级）：
1. 必分析：有真实持仓、模拟持仓或 active 模拟计划
2. 到期刷新：距上次分析超过 N 个交易日，或从未分析过
3. 异动触发：涨跌幅绝对值 ≥ 阈值，或量比 ≥ 阈值
4. 跳过：以上均不满足

失败策略：任何查询异常均 fail-open（照常分析），不降低覆盖率。
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class TieringResult:
    """分级筛选结果"""
    to_analyze: List[str]       # 需要分析的股票列表（保留原始 code 格式）
    skipped: List[str]          # 被跳过的股票列表
    reasons: Dict[str, str]     # code -> 跳过/分析原因

    @property
    def summary(self) -> str:
        reason_counts: Dict[str, int] = {}
        for r in self.reasons.values():
            reason_counts[r] = reason_counts.get(r, 0) + 1
        parts = [f"{k}: {v}" for k, v in sorted(reason_counts.items())]
        return (
            f"分级筛选: 分析 {len(self.to_analyze)} 只, "
            f"跳过 {len(self.skipped)} 只 ({', '.join(parts)})"
        )


# ---------------------------------------------------------------------------
# Code normalization helpers
# ---------------------------------------------------------------------------

def _to_plain_code(code: str) -> str:
    """将各种格式的股票代码归一化为纯数字（A 股）或标准格式（HK/US 等）。

    复用 data_provider.base.normalize_stock_code，延迟导入避免循环依赖。
    """
    from data_provider.base import normalize_stock_code
    return normalize_stock_code(str(code or ""))


def _build_held_code_set(
    *,
    db: Any,
    portfolio_repo: Any,
) -> Set[str]:
    """收集所有持仓和 active 计划涉及的股票代码，归一化后返回。

    三个来源：
    - 真实持仓：PortfolioRepository.list_cached_position_identities()
      返回 (market, symbol)，symbol 是带前缀的如 "SH513980"
    - 模拟持仓：PaperTradingRepository.list_open_positions_all()
      stock_code 是纯数字如 "002714"
    - 模拟计划：PaperTradingRepository.list_active_plans()
      stock_code 是纯数字如 "002714"
    """
    held: Set[str] = set()

    # 1) 真实持仓
    try:
        if portfolio_repo is not None:
            identities = portfolio_repo.list_cached_position_identities()
            for _market, symbol in identities:
                held.add(_to_plain_code(symbol))
    except Exception as e:
        logger.warning("自选分级: 读取真实持仓异常(fail-open): %s", e)

    # 2) 模拟持仓 + 模拟计划
    try:
        from src.repositories.paper_trading_repo import PaperTradingRepository
        paper_repo = PaperTradingRepository(db)

        for pos in paper_repo.list_open_positions_all():
            held.add(_to_plain_code(pos.stock_code))

        for plan in paper_repo.list_active_plans():
            held.add(_to_plain_code(plan.stock_code))
    except Exception as e:
        logger.warning("自选分级: 读取模拟持仓/计划异常(fail-open): %s", e)

    return held


# ---------------------------------------------------------------------------
# Trading-day distance
# ---------------------------------------------------------------------------

def _trading_days_since(
    last_date: date,
    today: date,
    market: str,
) -> Optional[int]:
    """计算 last_date 和 today 之间有多少个交易日（不含 last_date，含 today）。

    返回 None 表示无法计算（xcals 不可用），调用方应 fail-open。
    """
    try:
        import exchange_calendars as xcals
        from src.core.trading_calendar import MARKET_EXCHANGE
    except ImportError:
        return None

    ex = MARKET_EXCHANGE.get(market)
    if not ex:
        return None

    try:
        cal = xcals.get_calendar(ex)
        # 获取 (last_date, today] 范围内的交易日
        # sessions_in_range 是闭区间 [start, end]，所以我们要排除 last_date
        import pandas as pd
        start = pd.Timestamp(last_date)
        end = pd.Timestamp(today)
        if start >= end:
            return 0
        sessions = cal.sessions_in_range(start, end)
        # 排除 last_date 本身
        count = len(sessions)
        if len(sessions) > 0 and sessions[0].date() == last_date:
            count -= 1
        return count
    except Exception as e:
        logger.warning("自选分级: 交易日计算异常(fail-open): %s", e)
        return None


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------

def select_scheduled_codes(
    codes: List[str],
    *,
    config: Any,
    db: Any,
    fetcher_manager: Any = None,
    portfolio_repo: Any = None,
    today: Optional[date] = None,
) -> TieringResult:
    """对定时分析的自选股列表进行分级筛选。

    Args:
        codes: 原始自选股列表（STOCK_LIST 格式）
        config: 系统配置对象
        db: 数据库实例（Storage）
        fetcher_manager: DataFetcherManager 实例，用于获取实时行情
        portfolio_repo: PortfolioRepository 实例
        today: 当前日期，默认取 datetime.now().date()

    Returns:
        TieringResult(to_analyze, skipped, reasons)
    """
    if not codes:
        return TieringResult(to_analyze=[], skipped=[], reasons={})

    # 读取配置，未配置时使用默认值（等同于全量分析）
    enabled = getattr(config, "watchlist_tiering_enabled", False)
    if not enabled:
        reasons = {c: "tiering_disabled" for c in codes}
        return TieringResult(to_analyze=list(codes), skipped=[], reasons=reasons)

    refresh_days = getattr(config, "watchlist_refresh_trading_days", 3)
    anomaly_pct = getattr(config, "watchlist_anomaly_pct_chg", 4.0)
    anomaly_vol = getattr(config, "watchlist_anomaly_volume_ratio", 2.0)

    if today is None:
        today = datetime.now().date()

    # Step 1: 收集持仓代码集合
    held_codes = _build_held_code_set(db=db, portfolio_repo=portfolio_repo)

    # Step 2: 先按持仓/计划和刷新间隔定级，未命中的才需要实时行情
    reasons: Dict[str, str] = {}
    pending: List[str] = []
    for code in codes:
        reason = _classify_without_quote(
            code=code,
            plain_code=_to_plain_code(code),
            held_codes=held_codes,
            db=db,
            today=today,
            refresh_days=refresh_days,
        )
        if reason is None:
            pending.append(code)
        else:
            reasons[code] = reason

    # Step 3: 只为待定股票取实时行情并检查异动
    if pending:
        quotes = _fetch_quotes(fetcher_manager, pending)
        for code in pending:
            reasons[code] = _classify_by_quote(
                quotes.get(_to_plain_code(code)),
                anomaly_pct=anomaly_pct,
                anomaly_vol=anomaly_vol,
            )

    # 保持 STOCK_LIST 原顺序
    to_analyze = [c for c in codes if reasons[c] != "skipped"]
    skipped = [c for c in codes if reasons[c] == "skipped"]

    result = TieringResult(to_analyze=to_analyze, skipped=skipped, reasons=reasons)
    logger.info(result.summary)
    return result


def _classify_without_quote(
    *,
    code: str,
    plain_code: str,
    held_codes: Set[str],
    db: Any,
    today: date,
    refresh_days: int,
) -> Optional[str]:
    """规则 1/2：持仓计划与刷新间隔。命中返回分析原因，未命中返回 None（待查异动）。"""

    if plain_code in held_codes:
        return "held_or_planned"

    try:
        last_analysis_date = _get_last_analysis_date(db, code, plain_code)
    except Exception as e:
        logger.warning("自选分级: 检查分析历史异常(fail-open): code=%s err=%s", code, e)
        return "history_check_error"
    if last_analysis_date is None:
        return "never_analyzed"

    from src.market_context import detect_market
    trading_days = _trading_days_since(last_analysis_date, today, detect_market(code))
    if trading_days is None:
        # 交易日历不可用时按自然日判断，偏向分析
        if (today - last_analysis_date).days >= refresh_days:
            return "refresh_due"
    elif trading_days >= refresh_days:
        return "refresh_due"
    return None


def _fetch_quotes(fetcher_manager: Any, codes: List[str]) -> Dict[str, Any]:
    """获取实时行情，按归一化代码索引；失败的股票不出现在结果中。"""

    quotes: Dict[str, Any] = {}
    if fetcher_manager is None:
        return quotes
    try:
        fetcher_manager.prefetch_realtime_quotes(codes)
    except Exception as e:
        logger.warning("自选分级: 预取实时行情异常(继续逐只获取): %s", e)
    for code in codes:
        try:
            quote = fetcher_manager.get_realtime_quote(code, log_final_failure=False)
        except Exception as e:
            logger.warning("自选分级: 获取实时行情异常(fail-open): code=%s err=%s", code, e)
            continue
        if quote is not None:
            quotes[_to_plain_code(code)] = quote
    return quotes


def _classify_by_quote(quote: Any, *, anomaly_pct: float, anomaly_vol: float) -> str:
    """规则 3/4：实时异动。行情缺失或两项指标都缺失时保守地照常分析。"""

    if quote is None:
        return "quote_unavailable"
    pct = _as_float(getattr(quote, "change_pct", None))
    vol_ratio = _as_float(getattr(quote, "volume_ratio", None))
    if pct is None and vol_ratio is None:
        return "quote_unavailable"
    if pct is not None and abs(pct) >= anomaly_pct:
        return "anomaly_pct_chg"
    if vol_ratio is not None and vol_ratio >= anomaly_vol:
        return "anomaly_volume_ratio"
    return "skipped"


def _as_float(value: Any) -> Optional[float]:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _get_last_analysis_date(db: Any, code: str, plain_code: str) -> Optional[date]:
    """获取某只股票最近一次分析的日期。

    analysis_history.code 保存的是分析入参原样，可能带前缀/后缀，
    原始格式与归一化格式都查询，取最新一条。查询异常向上抛出，由调用方 fail-open。
    """
    latest: Optional[datetime] = None
    for candidate in {code, plain_code}:
        rows = db.get_analysis_history(code=candidate, days=30, limit=1)
        created = rows[0].created_at if rows else None
        if created is not None and (latest is None or created > latest):
            latest = created
    return latest.date() if latest else None
