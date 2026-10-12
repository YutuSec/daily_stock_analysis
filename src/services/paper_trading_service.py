# -*- coding: utf-8 -*-
"""End-of-day, report-attributed paper trading with no order surface."""

from __future__ import annotations

import json
import hashlib
import math
import re
from bisect import bisect_right
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from sqlalchemy import and_, desc, func, or_, select, text, update
from sqlalchemy.exc import IntegrityError

from data_provider.base import DataFetcherManager
from data_provider.base import canonical_stock_code, normalize_stock_code
from src.core.trading_calendar import (
    build_market_phase_context,
    get_effective_trading_date,
    get_market_for_stock,
    get_market_now,
    is_market_open,
)
from src.repositories.paper_trading_repo import PaperTradingRepository
from src.schemas.decision_action import (
    display_action_fields,
    normalize_decision_action,
    normalize_holding_state,
)
from src.schemas.decision_scale import action_for_score
from src.services.decision_signal_service import VALID_MARKETS
from src.services.analysis_model_service import validate_analysis_models
from src.services.daily_market_context import (
    _market_context_calendar,
    market_context_completed_session_lag,
    market_context_freshness_expires_at,
    resolve_market_context_freshness_expires_at,
)
from src.services.opportunity_market_risk_policy import (
    BENCHMARK_INDEX_CODE as OPPORTUNITY_BENCHMARK_INDEX_CODE,
    OPPORTUNITY_MARKET_POLICY_VERSION,
    REQUIRED_WINDOW_SESSIONS as OPPORTUNITY_INDEX_WINDOW_SESSIONS,
    evaluate_opportunity_market_policy,
)
from src.services.paper_symbol_identity import canonical_paper_identity, stock_daily_variants
from src.services.paper_strategy_policy import (
    POLICY_VERSION as PAPER_STRATEGY_POLICY_VERSION,
    entry_geometry_guard,
    plan_holding_sessions_for_horizon,
    setup_contract,
    setup_factor_policy,
    validate_risk_revision_geometry,
)
from src.utils.sniper_points import parse_sniper_value
from src.storage import (
    AnalysisHistory,
    AnalysisReportExecutionIntent,
    DailyMarketRegime,
    DecisionSignalRecord,
    PaperAccountRiskEvent,
    PaperAccountRiskSnapshot,
    PaperCapitalAdjustment,
    PaperCapitalAccount,
    PaperExecutionPlan,
    PaperNotificationDispatch,
    PaperPlanObservation,
    PaperPlanEvaluation,
    PaperPosition,
    PaperReanalysisRequest,
    PaperResolution,
    PaperRiskRevision,
    PaperStrategyThesis,
    PaperStrategyTransition,
    PaperSignalConsumption,
    PaperSyncLink,
    PaperTradeEvent,
    StockDaily,
    PortfolioPosition,
    REPORT_EXECUTION_SCOPE_PRIMARY,
    authorized_report_execution_clause,
    evaluate_report_execution_intent,
    normalize_report_execution_scope,
    utc_naive_now,
)


PLAN_VERSION = "paper-hybrid-v3"
TRADING_STYLE_POLICY_VERSION = "trading-style-v1"
STRATEGY_THESIS_POLICY_VERSION = "strategy-thesis-v1"
MARKET_REGIME_POLICY_VERSION = "market-regime-v1"
REPORT_PLAN_RECONCILE_MAX_ATTEMPTS = 3
ENTRY_LEG_SCHEMA_VERSION = "entry-legs-v1"
EXIT_LEG_SCHEMA_VERSION = "exit-legs-v1"
LEGACY_EOD_PLAN_VERSION = "paper-eod-v1"
UPGRADABLE_ACTIVE_PLAN_VERSIONS = frozenset({LEGACY_EOD_PLAN_VERSION, "paper-hybrid-v2"})
NORMALIZED_NOTIONAL = 100000.0
VOLUME_POLICY_RATIO = 1.5
VOLUME_POLICY_SESSIONS = 5
INTRADAY_MIN_FACTOR_DOMAINS = 2
INTRADAY_MIN_COMPOSITE_SCORE = 0.6
INTRADAY_MIN_RISK_REWARD = 1.2
INTRADAY_MAX_QUOTE_STALE_SECONDS = 360
QUOTE_CLOCK_SKEW_SECONDS = 120
MANUAL_ANALYSIS_SOURCES = frozenset({"api", "web", "bot", "cli", "desktop", "manual"})
REANALYSIS_RECONCILE_GRACE_SECONDS = 900
AUCTION_PRICE_DEVIATION_PCT = 2.0
AUCTION_MIN_MATCHED_VOLUME_RATIO = 0.20
ACCOUNT_DAILY_LOSS_LIMIT_PCT = 3.0
ACCOUNT_DRAWDOWN_LIMIT_PCT = 10.0
ACCOUNT_DRAWDOWN_RECOVERY_PCT = 8.0
ACCOUNT_LOSING_STREAK_LIMIT = 3
ACCOUNT_DAILY_TRADE_LIMIT = 10
ACCOUNT_DAILY_OPEN_ADD_LIMIT = 5
ACCOUNT_SECTOR_CAP_PCT = 60.0
ACCOUNT_CORRELATION_CAP_PCT = 60.0
ACCOUNT_UNKNOWN_CAP_PCT = 50.0
ASSET_EXPOSURE_POLICY_VERSION = "asset-exposure-v1"
DEFAULT_ASSET_EXPOSURE_CAPS = {
    "individual_stock": 20.0,
    "theme_etf": 30.0,
    "broad_etf": 40.0,
    "unknown": 20.0,
}
CORRELATION_THRESHOLD = 0.85
CORRELATION_LOOKBACK_SESSIONS = 60
CORRELATION_MIN_OVERLAP_SESSIONS = 30
STRATEGY_ENTRY_CONFIRMATION_SESSIONS = 2
STRATEGY_FLIP_CONFIRMATION_SESSIONS = 2
DETERMINISTIC_HARD_RISK_IMMEDIATE = True
PRICE_RULE_TYPES = frozenset({"close_between", "close_gte", "close_lte"})
FACTOR_WEIGHTS = {
    "trend": 0.25,
    "volume": 0.30,
    "momentum": 0.30,
    "breakout": 0.15,
}
SUPPORTED_RULE_TYPES = frozenset({
    "close_between",
    "close_gte",
    "close_lte",
    "close_above_prior_sma",
    "close_above_prior_high",
    "volume_ratio_gte_prior_avg",
})
HORIZON_SESSIONS = {
    "intraday": 1,
    "1d": 1,
    "3d": 3,
    "5d": 5,
    "10d": 10,
    "swing": 20,
    "long": 60,
}
TRADING_STYLE_SETTINGS = {
    "short_swing": {
        "label": "短线波段",
        "expected_holding_sessions_range": [3, 10],
        "default_holding_sessions": 10,
        "max_holding_sessions": 15,
        "risk_multiplier": 0.75,
        "min_risk_reward": 1.3,
        "recommended_intraday_interval_minutes": 3,
        "plan_refresh_cadence_sessions": 1,
        "entry_leg_limit": 2,
        "add_limit": 1,
        "llm_event_cooldown_minutes": 90,
        "llm_daily_cap": 2,
        "analysis_window_sessions": 80,
        "core_ma_windows": [5, 10, 20],
        "volatility_windows": [14, 20],
        "breakout_window_sessions": 20,
        "reanalysis_cooldown_minutes": 90,
        "compatible_horizons": ["intraday", "1d", "3d", "5d", "10d"],
        "default_signal_horizon": "10d",
        "geometry_guard": {
            "min_stop_atr_multiple": 0.75,
            "min_stop_pct": 0.012,
            "max_entry_above_support_pct": 0.08,
        },
    },
    "position": {
        "label": "中线持仓",
        "expected_holding_sessions_range": [10, 40],
        "default_holding_sessions": 40,
        "max_holding_sessions": 60,
        "risk_multiplier": 1.0,
        "min_risk_reward": 1.2,
        "recommended_intraday_interval_minutes": 5,
        "plan_refresh_cadence_sessions": 2,
        "entry_leg_limit": 2,
        "add_limit": 1,
        "llm_event_cooldown_minutes": 240,
        "llm_daily_cap": 1,
        "analysis_window_sessions": 180,
        "core_ma_windows": [10, 20, 60],
        "volatility_windows": [20, 60],
        "breakout_window_sessions": 60,
        "reanalysis_cooldown_minutes": 240,
        "compatible_horizons": ["10d", "swing"],
        "default_signal_horizon": "swing",
        "geometry_guard": {
            "min_stop_atr_multiple": 1.0,
            "min_stop_pct": 0.018,
            "max_entry_above_support_pct": 0.12,
        },
    },
    "trend": {
        "label": "趋势持有",
        "expected_holding_sessions_range": [40, 120],
        "default_holding_sessions": 120,
        "max_holding_sessions": 180,
        "risk_multiplier": 0.80,
        "min_risk_reward": 1.8,
        "recommended_intraday_interval_minutes": 15,
        "plan_refresh_cadence_sessions": 5,
        "entry_leg_limit": 3,
        "add_limit": 2,
        "llm_event_cooldown_minutes": 1440,
        "llm_daily_cap": 1,
        "analysis_window_sessions": 500,
        "core_ma_windows": [20, 60, 120, 250],
        "volatility_windows": [20, 60, 120],
        "breakout_window_sessions": 250,
        "reanalysis_cooldown_minutes": 1440,
        "compatible_horizons": ["long"],
        "default_signal_horizon": "long",
        "geometry_guard": {
            "min_stop_atr_multiple": 1.25,
            "min_stop_pct": 0.025,
            "max_entry_above_support_pct": 0.18,
        },
    },
}
MARKET_REGIME_EXECUTION_POLICY = {
    "normal": {"entry_allowed": True, "allocation_multiplier": 1.0, "reason": "market_context_normal"},
    "cautious": {"entry_allowed": True, "allocation_multiplier": 0.5, "reason": "market_context_cautious"},
    "risk_off": {"entry_allowed": False, "allocation_multiplier": 0.0, "reason": "market_context_risk_off"},
    "unavailable": {"entry_allowed": False, "allocation_multiplier": 0.0, "reason": "market_context_unavailable"},
}
MARKET_REGULAR_CLOSE = {
    "cn": time(15, 0),
    "hk": time(16, 0),
    "us": time(16, 0),
    "jp": time(15, 30),
    "kr": time(15, 30),
    "tw": time(13, 30),
}
# 计划 / 持仓批次的 source_kind：按来源报告保存时记录的 intent_source 归类，仅供展示。
# 机会池候选分析、红灯研究与实验导入都来自机会池；legacy_baseline 是迁移前既有报告
# 的统一基线，不区分来源，单独标记；其余来源（analysis_pipeline 等）归为普通分析。
# 没有 intent 记录时 source_kind 为 None。
REPORT_SOURCE_KIND_BY_INTENT_SOURCE = {
    "market_opportunity": "market_opportunity",
    "market_opportunity_research": "market_opportunity",
    "opportunity_experiment_import": "market_opportunity",
    "legacy_baseline": "legacy",
}
REPORT_SOURCE_KIND_DEFAULT = "analysis"


class PaperTradingNotFoundError(ValueError):
    """Raised when a report, signal, or plan does not exist."""


class PaperTradingConflictError(ValueError):
    """Raised when an idempotency key is reused with a different payload."""


class PaperTradingExecutionNotAuthorizedError(ValueError):
    """Raised when a report's frozen execution purpose does not cover this scope."""

    def __init__(self, reason: str, *, report_id: int):
        self.reason = str(reason)
        self.report_id = int(report_id)
        super().__init__(
            f"analysis report {self.report_id} is not authorized for paper execution: {self.reason}"
        )


class PaperTradingService:
    """Produce and evaluate simulated plans; never place or represent orders."""

    def __init__(
        self,
        repo: Optional[PaperTradingRepository] = None,
        *,
        normalized_notional: float = NORMALIZED_NOTIONAL,
        intraday_min_factor_domains: int = INTRADAY_MIN_FACTOR_DOMAINS,
        intraday_min_composite_score: float = INTRADAY_MIN_COMPOSITE_SCORE,
        intraday_min_risk_reward: float = INTRADAY_MIN_RISK_REWARD,
        intraday_max_quote_stale_seconds: int = INTRADAY_MAX_QUOTE_STALE_SECONDS,
        initial_capital: float = 100000.0,
        risk_per_trade_pct: float = 1.0,
        max_position_pct: float = 20.0,
        max_symbol_exposure_pct: float = 50.0,
        asset_exposure_caps: Optional[Mapping[str, Any]] = None,
        asset_class_overrides: Optional[Mapping[str, Any]] = None,
        max_total_exposure_pct: float = 80.0,
        max_open_positions: int = 5,
        default_reduce_fraction: float = 0.5,
        slippage_pct: float = 0.0,
        commission_rate: float = 0.0,
        min_commission: float = 0.0,
        sell_tax_rate: float = 0.0,
        feishu_notify: bool = False,
        t0_symbols: Optional[Sequence[str]] = None,
        auction_price_deviation_pct: float = AUCTION_PRICE_DEVIATION_PCT,
        auction_min_matched_volume_ratio: float = AUCTION_MIN_MATCHED_VOLUME_RATIO,
        account_daily_loss_limit_pct: float = ACCOUNT_DAILY_LOSS_LIMIT_PCT,
        account_drawdown_limit_pct: float = ACCOUNT_DRAWDOWN_LIMIT_PCT,
        account_drawdown_recovery_pct: float = ACCOUNT_DRAWDOWN_RECOVERY_PCT,
        account_losing_streak_limit: int = ACCOUNT_LOSING_STREAK_LIMIT,
        account_daily_trade_limit: int = ACCOUNT_DAILY_TRADE_LIMIT,
        account_daily_open_add_limit: int = ACCOUNT_DAILY_OPEN_ADD_LIMIT,
        account_sector_cap_pct: float = ACCOUNT_SECTOR_CAP_PCT,
        account_correlation_cap_pct: float = ACCOUNT_CORRELATION_CAP_PCT,
        account_unknown_cap_pct: float = ACCOUNT_UNKNOWN_CAP_PCT,
        correlation_threshold: float = CORRELATION_THRESHOLD,
        correlation_lookback_sessions: int = CORRELATION_LOOKBACK_SESSIONS,
        correlation_min_overlap_sessions: int = CORRELATION_MIN_OVERLAP_SESSIONS,
        trading_style: str = "position",
        trading_style_overrides: Optional[Mapping[str, Any]] = None,
        strategy_thesis_enabled: bool = True,
        strategy_entry_confirmation_sessions: int = STRATEGY_ENTRY_CONFIRMATION_SESSIONS,
        strategy_flip_confirmation_sessions: int = STRATEGY_FLIP_CONFIRMATION_SESSIONS,
        plan_compensation_enabled: bool = True,
        plan_compensation_lookback_days: int = 30,
        market_context_risk_control_enabled: bool = False,
        market_context_lookback_days: int = 3,
        execution_scope: str = REPORT_EXECUTION_SCOPE_PRIMARY,
    ):
        self.repo = repo or PaperTradingRepository()
        self.db = self.repo.db
        # Scope comes from the trusted service owner, never from API payloads.
        self.execution_scope = normalize_report_execution_scope(execution_scope)
        if not self._positive_finite(normalized_notional):
            raise ValueError("normalized_notional must be finite and positive")
        self.normalized_notional = float(normalized_notional)
        self.intraday_min_factor_domains = max(1, int(intraday_min_factor_domains))
        self.intraday_min_composite_score = min(1.0, max(0.0, float(intraday_min_composite_score)))
        self.intraday_max_quote_stale_seconds = max(30, int(intraday_max_quote_stale_seconds))
        self.initial_capital = max(1.0, float(initial_capital))
        self.risk_per_trade_pct = max(0.0, float(risk_per_trade_pct))
        self.max_position_pct = max(0.0, float(max_position_pct))
        self.max_symbol_exposure_pct = max(0.0, float(max_symbol_exposure_pct))
        self.asset_exposure_caps = self._normalize_asset_exposure_caps(asset_exposure_caps)
        self.asset_class_overrides = {
            str(key).strip().lower(): self._normalize_asset_type(value)
            for key, value in dict(asset_class_overrides or {}).items()
            if str(key).strip()
        }
        self.max_total_exposure_pct = max(0.0, float(max_total_exposure_pct))
        self.max_open_positions = max(1, int(max_open_positions))
        self.default_reduce_fraction = min(1.0, float(default_reduce_fraction))
        if not (0.0 < self.default_reduce_fraction <= 1.0):
            raise ValueError("default_reduce_fraction must be in (0, 1]")
        self.slippage_pct = max(0.0, float(slippage_pct))
        self.commission_rate = max(0.0, float(commission_rate))
        self.min_commission = max(0.0, float(min_commission))
        self.sell_tax_rate = max(0.0, float(sell_tax_rate))
        self.feishu_notify = bool(feishu_notify)
        self.t0_identities = self._normalize_t0_identities(t0_symbols or [])
        self.auction_price_deviation_pct = max(0.0, float(auction_price_deviation_pct))
        self.auction_min_matched_volume_ratio = max(0.0, float(auction_min_matched_volume_ratio))
        self.account_daily_loss_limit_pct = max(0.0, float(account_daily_loss_limit_pct))
        self.account_drawdown_limit_pct = max(0.0, float(account_drawdown_limit_pct))
        self.account_drawdown_recovery_pct = max(0.0, float(account_drawdown_recovery_pct))
        self.account_losing_streak_limit = max(1, int(account_losing_streak_limit))
        self.account_daily_trade_limit = max(1, int(account_daily_trade_limit))
        self.account_daily_open_add_limit = max(1, int(account_daily_open_add_limit))
        self.account_sector_cap_pct = max(0.0, float(account_sector_cap_pct))
        self.account_correlation_cap_pct = max(0.0, float(account_correlation_cap_pct))
        self.account_unknown_cap_pct = max(0.0, float(account_unknown_cap_pct))
        self.correlation_threshold = min(1.0, max(0.0, float(correlation_threshold)))
        self.correlation_lookback_sessions = max(1, int(correlation_lookback_sessions))
        self.correlation_min_overlap_sessions = max(1, int(correlation_min_overlap_sessions))
        self.trading_style = self._normalize_trading_style(trading_style)
        self.trading_style_overrides = {
            str(key).strip().lower(): self._normalize_trading_style(value)
            for key, value in dict(trading_style_overrides or {}).items()
            if str(key).strip()
        }
        self.trading_style_settings = dict(TRADING_STYLE_SETTINGS[self.trading_style])
        self.strategy_thesis_enabled = bool(strategy_thesis_enabled)
        self.strategy_entry_confirmation_sessions = max(1, int(strategy_entry_confirmation_sessions))
        self.strategy_flip_confirmation_sessions = max(1, int(strategy_flip_confirmation_sessions))
        self.intraday_min_risk_reward = max(0.0, float(intraday_min_risk_reward))
        self.default_holding_sessions = int(self.trading_style_settings["default_holding_sessions"])
        self.max_holding_sessions = int(self.trading_style_settings.get("max_holding_sessions", self.default_holding_sessions))
        self.analysis_window_sessions = max(20, int(self.trading_style_settings["analysis_window_sessions"]))
        self.style_risk_multiplier = max(0.0, float(self.trading_style_settings["risk_multiplier"]))
        self.reanalysis_cooldown_minutes = max(
            1,
            int(self.trading_style_settings["reanalysis_cooldown_minutes"]),
        )
        self.plan_compensation_enabled = bool(plan_compensation_enabled)
        self.plan_compensation_lookback_days = max(1, int(plan_compensation_lookback_days))
        self.market_context_risk_control_enabled = bool(market_context_risk_control_enabled)
        self.market_context_lookback_days = max(1, int(market_context_lookback_days))

    @classmethod
    def from_config(
        cls,
        config: Optional[Any] = None,
        repo: Optional[PaperTradingRepository] = None,
        *,
        execution_scope: str = REPORT_EXECUTION_SCOPE_PRIMARY,
    ) -> "PaperTradingService":
        if config is None:
            from src.config import get_config

            config = get_config()
        return cls(
            repo=repo,
            execution_scope=execution_scope,
            intraday_min_factor_domains=getattr(config, "paper_trading_intraday_min_factor_domains", INTRADAY_MIN_FACTOR_DOMAINS),
            intraday_min_composite_score=getattr(config, "paper_trading_intraday_min_composite_score", INTRADAY_MIN_COMPOSITE_SCORE),
            intraday_min_risk_reward=getattr(config, "paper_trading_intraday_min_risk_reward", INTRADAY_MIN_RISK_REWARD),
            intraday_max_quote_stale_seconds=getattr(config, "paper_trading_intraday_max_quote_stale_seconds", INTRADAY_MAX_QUOTE_STALE_SECONDS),
            initial_capital=getattr(config, "paper_trading_initial_capital", 100000.0),
            risk_per_trade_pct=getattr(config, "paper_trading_risk_per_trade_pct", 1.0),
            max_position_pct=getattr(config, "paper_trading_max_position_pct", 20.0),
            max_symbol_exposure_pct=getattr(config, "paper_trading_max_symbol_exposure_pct", 50.0),
            asset_exposure_caps=getattr(config, "paper_trading_asset_exposure_caps", DEFAULT_ASSET_EXPOSURE_CAPS),
            asset_class_overrides=getattr(config, "paper_trading_asset_class_overrides", {}),
            max_total_exposure_pct=getattr(config, "paper_trading_max_total_exposure_pct", 80.0),
            max_open_positions=getattr(config, "paper_trading_max_open_positions", 5),
            default_reduce_fraction=getattr(config, "paper_trading_default_reduce_fraction", 0.5),
            slippage_pct=getattr(config, "paper_trading_slippage_pct", 0.03),
            commission_rate=getattr(config, "paper_trading_commission_rate", 0.0003),
            min_commission=getattr(config, "paper_trading_min_commission", 5.0),
            sell_tax_rate=getattr(config, "paper_trading_sell_tax_rate", 0.0005),
            feishu_notify=getattr(config, "paper_trading_feishu_notify", False),
            t0_symbols=getattr(config, "paper_trading_t0_symbols", []),
            auction_price_deviation_pct=getattr(config, "paper_trading_auction_price_deviation_pct", AUCTION_PRICE_DEVIATION_PCT),
            auction_min_matched_volume_ratio=getattr(config, "paper_trading_auction_min_matched_volume_ratio", AUCTION_MIN_MATCHED_VOLUME_RATIO),
            account_daily_loss_limit_pct=getattr(config, "paper_trading_account_daily_loss_limit_pct", ACCOUNT_DAILY_LOSS_LIMIT_PCT),
            account_drawdown_limit_pct=getattr(config, "paper_trading_account_drawdown_limit_pct", ACCOUNT_DRAWDOWN_LIMIT_PCT),
            account_drawdown_recovery_pct=getattr(config, "paper_trading_account_drawdown_recovery_pct", ACCOUNT_DRAWDOWN_RECOVERY_PCT),
            account_losing_streak_limit=getattr(config, "paper_trading_account_losing_streak_limit", ACCOUNT_LOSING_STREAK_LIMIT),
            account_daily_trade_limit=getattr(config, "paper_trading_account_daily_trade_limit", ACCOUNT_DAILY_TRADE_LIMIT),
            account_daily_open_add_limit=getattr(config, "paper_trading_account_daily_open_add_limit", ACCOUNT_DAILY_OPEN_ADD_LIMIT),
            account_sector_cap_pct=getattr(config, "paper_trading_account_sector_cap_pct", ACCOUNT_SECTOR_CAP_PCT),
            account_correlation_cap_pct=getattr(config, "paper_trading_account_correlation_cap_pct", ACCOUNT_CORRELATION_CAP_PCT),
            account_unknown_cap_pct=getattr(config, "paper_trading_account_unknown_cap_pct", ACCOUNT_UNKNOWN_CAP_PCT),
            correlation_threshold=getattr(config, "paper_trading_correlation_threshold", CORRELATION_THRESHOLD),
            correlation_lookback_sessions=getattr(config, "paper_trading_correlation_lookback_sessions", CORRELATION_LOOKBACK_SESSIONS),
            correlation_min_overlap_sessions=getattr(config, "paper_trading_correlation_min_overlap_sessions", CORRELATION_MIN_OVERLAP_SESSIONS),
            trading_style=getattr(config, "paper_trading_style", "position"),
            trading_style_overrides=getattr(config, "paper_trading_style_overrides", {}),
            strategy_thesis_enabled=getattr(config, "paper_trading_strategy_thesis_enabled", True),
            strategy_entry_confirmation_sessions=getattr(
                config,
                "paper_trading_strategy_entry_confirmation_sessions",
                STRATEGY_ENTRY_CONFIRMATION_SESSIONS,
            ),
            strategy_flip_confirmation_sessions=getattr(
                config,
                "paper_trading_strategy_flip_confirmation_sessions",
                STRATEGY_FLIP_CONFIRMATION_SESSIONS,
            ),
            plan_compensation_enabled=getattr(config, "paper_trading_plan_compensation_enabled", True),
            plan_compensation_lookback_days=getattr(config, "paper_trading_plan_compensation_lookback_days", 30),
            market_context_risk_control_enabled=getattr(config, "paper_trading_market_context_risk_control_enabled", True),
            market_context_lookback_days=getattr(config, "paper_trading_market_context_lookback_days", 3),
        )

    @staticmethod
    def _normalize_trading_style(value: Any) -> str:
        key = str(value or "position").strip().lower().replace("-", "_")
        aliases = {
            "short": "short_swing",
            "swing": "short_swing",
            "short_swing": "short_swing",
            "medium": "position",
            "position": "position",
            "balanced": "position",
            "long": "trend",
            "trend": "trend",
            "long_trend": "trend",
        }
        return aliases.get(key, "position")

    def _trading_style_evidence(
        self,
        *,
        signal: Optional[DecisionSignalRecord] = None,
        raw_report: Optional[Mapping[str, Any]] = None,
        market: Optional[str] = None,
        stock_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {}
        if signal is not None:
            metadata, _ = self._json_object(getattr(signal, "metadata_json", None))
        frozen = metadata.get("paper_trading_style_evidence")
        if not isinstance(frozen, Mapping):
            frozen = {}
        frozen_context = frozen.get("style_context") if isinstance(frozen.get("style_context"), Mapping) else None
        if frozen_context is None and isinstance(metadata.get("paper_trading_style_context"), Mapping):
            frozen_context = metadata.get("paper_trading_style_context")
        indicator_snapshot = frozen.get("indicator_snapshot") if isinstance(frozen.get("indicator_snapshot"), Mapping) else None
        if indicator_snapshot is None and isinstance(metadata.get("paper_trading_style_indicators"), Mapping):
            indicator_snapshot = metadata.get("paper_trading_style_indicators")

        if isinstance(frozen_context, Mapping):
            style = self._normalize_trading_style(frozen_context.get("trading_style"))
            source = str(frozen_context.get("source") or frozen.get("source") or "analysis_context_snapshot")
            matched_key = frozen_context.get("matched_override_key")
            settings = dict(
                frozen_context.get("settings")
                if isinstance(frozen_context.get("settings"), Mapping)
                else TRADING_STYLE_SETTINGS[style]
            )
            try:
                required = max(1, int(settings.get("analysis_window_sessions") or 1))
            except (TypeError, ValueError):
                required = 1
            raw_bars = frozen.get("bars_observed")
            if raw_bars is None and isinstance(indicator_snapshot, Mapping):
                raw_bars = indicator_snapshot.get("bars_observed")
            try:
                bars_observed = int(raw_bars) if raw_bars is not None else None
            except (TypeError, ValueError):
                bars_observed = None
            indicator_sufficient = bool(
                indicator_snapshot.get("history_sufficient")
                if isinstance(indicator_snapshot, Mapping)
                else frozen.get("history_sufficient")
            )
            history_sufficient = bool(
                indicator_sufficient
                and bars_observed is not None
                and bars_observed >= required
            )
            freeze_error = None if history_sufficient else "insufficient_frozen_style_evidence"
        else:
            style = "position"
            source = "legacy_unknown"
            matched_key = None
            settings = dict(TRADING_STYLE_SETTINGS[style])
            required = max(1, int(settings.get("analysis_window_sessions") or 1))
            bars_observed = None
            history_sufficient = False
            freeze_error = "missing_frozen_style_evidence"
        compatible_horizons = list(settings.get("compatible_horizons") or [])
        horizon = str(getattr(signal, "horizon", "") or "").lower() or None
        horizon_source = str(
            metadata.get("paper_trading_horizon_source")
            or metadata.get("horizon_source")
            or ""
        ).strip().lower() or None
        return {
            "trading_style": style,
            "policy_version": TRADING_STYLE_POLICY_VERSION,
            "source": source,
            "matched_override_key": matched_key,
            "settings": settings,
            "style_context": dict(frozen_context) if isinstance(frozen_context, Mapping) else None,
            "indicator_snapshot": dict(indicator_snapshot) if isinstance(indicator_snapshot, Mapping) else None,
            "bars_observed": bars_observed,
            "analysis_window_sessions": required,
            "history_sufficient": history_sufficient,
            "frozen_evidence_valid": bool(history_sufficient),
            "freeze_error": freeze_error,
            "signal_horizon": horizon,
            "signal_horizon_source": horizon_source,
            "horizon_compatible": (
                horizon in set(compatible_horizons)
                if horizon and compatible_horizons and horizon_source != "paper_trading_style_default" else True
            ),
            "compatible_horizons": compatible_horizons,
            "report_style_hint": (
                raw_report.get("trading_style")
                if isinstance(raw_report, Mapping)
                else None
            ),
            "deterministic_monitor_cadence_independent": True,
        }

    def _ensure_signal_strategy_metadata(
        self,
        signal_id: int,
        style_evidence: Mapping[str, Any],
    ) -> None:
        """Freeze the resolved paper style onto the signal metadata once."""

        with self.db.session_scope() as session:
            row = session.get(DecisionSignalRecord, int(signal_id))
            if row is None:
                return
            metadata, _ = self._json_object(getattr(row, "metadata_json", None))
            if not isinstance(metadata, dict):
                metadata = {}
            existing = metadata.get("paper_trading_style")
            style = str(style_evidence.get("trading_style") or self.trading_style)
            settings = style_evidence.get("settings") if isinstance(style_evidence.get("settings"), Mapping) else TRADING_STYLE_SETTINGS[style]
            if existing == style and metadata.get("paper_trading_style_policy_version"):
                return
            metadata["paper_trading_style"] = style
            metadata["paper_trading_style_policy_version"] = TRADING_STYLE_POLICY_VERSION
            metadata["paper_trading_style_settings"] = dict(settings)
            metadata["paper_trading_style_evidence"] = dict(style_evidence)
            row.metadata_json = self._json_dumps(metadata)
            row.updated_at = utc_naive_now()

    def _resolve_effective_trading_style(
        self,
        *,
        market: Optional[str],
        stock_code: Optional[str],
    ) -> Tuple[str, str, Optional[str]]:
        market_norm = str(market or "").strip().lower()
        code_text = str(stock_code or "").strip().lower()
        candidates: List[str] = []
        identity = canonical_paper_identity(market_norm, stock_code or "")
        if identity is not None:
            canonical_market, canonical_code = identity
            candidates.extend([
                f"{canonical_market}:{canonical_code}".lower(),
                f"{canonical_market}:{str(stock_code or '').strip().lower()}",
            ])
        if market_norm and code_text:
            candidates.append(f"{market_norm}:{code_text}")
        for key in dict.fromkeys(candidates):
            if key in self.trading_style_overrides:
                return self.trading_style_overrides[key], "paper_trading_style_override_config", key
        return self.trading_style, "paper_trading_style_config", None

    def _effective_style_settings_for_identity(
        self,
        *,
        market: Optional[str],
        stock_code: Optional[str],
    ) -> Dict[str, Any]:
        style, _source, _matched_key = self._resolve_effective_trading_style(
            market=market,
            stock_code=stock_code,
        )
        return dict(TRADING_STYLE_SETTINGS[style])

    def _style_evidence_for_identity(
        self,
        *,
        market: Optional[str],
        stock_code: Optional[str],
        source: str,
    ) -> Dict[str, Any]:
        style, style_source, matched_key = self._resolve_effective_trading_style(
            market=market,
            stock_code=stock_code,
        )
        settings = dict(TRADING_STYLE_SETTINGS[style])
        return {
            "trading_style": style,
            "policy_version": TRADING_STYLE_POLICY_VERSION,
            "source": source,
            "style_source": style_source,
            "matched_override_key": matched_key,
            "settings": settings,
            "history_sufficient": None,
            "frozen_evidence_valid": True,
            "deterministic_monitor_cadence_independent": True,
        }

    def _repair_portfolio_import_strategy_fields_in_session(
        self,
        session,
        *,
        plan: Optional[PaperExecutionPlan],
        position: Optional[PaperPosition],
        market: str,
        stock_code: str,
        now: datetime,
    ) -> bool:
        def existing_complete_style(row) -> Optional[Dict[str, Any]]:
            if row is None:
                return None
            style = self._normalize_trading_style(getattr(row, "trading_style", None))
            if getattr(row, "trading_style_policy_version", None) != TRADING_STYLE_POLICY_VERSION:
                return None
            payload, _ = self._json_object(getattr(row, "trading_style_settings_json", None) or "{}")
            settings = payload.get("settings") if isinstance(payload, Mapping) else None
            if not isinstance(settings, Mapping):
                return None
            expected_max = int(settings.get("max_holding_sessions") or TRADING_STYLE_SETTINGS[style]["max_holding_sessions"])
            if getattr(row, "max_holding_sessions", None) != expected_max:
                return None
            return {
                "trading_style": style,
                "policy_version": TRADING_STYLE_POLICY_VERSION,
                "source": payload.get("source") or "existing_frozen_import_style",
                "style_source": payload.get("style_source"),
                "matched_override_key": payload.get("matched_override_key"),
                "settings": dict(settings),
                "history_sufficient": payload.get("history_sufficient"),
                "frozen_evidence_valid": True,
                "deterministic_monitor_cadence_independent": True,
            }

        complete = [item for item in (existing_complete_style(plan), existing_complete_style(position)) if item]
        complete_styles = {item["trading_style"] for item in complete}
        rows = [row for row in (plan, position) if row is not None]
        if len(complete) == len(rows) and len(complete_styles) == 1:
            return False
        if len(complete_styles) == 1:
            style_evidence = complete[0]
        else:
            style_evidence = self._style_evidence_for_identity(
                market=market,
                stock_code=stock_code,
                source="portfolio_import_identity_resolution",
            )
        style = str(style_evidence["trading_style"])
        settings = dict(style_evidence["settings"])
        max_holding = int(settings.get("max_holding_sessions") or self.max_holding_sessions)
        changed = False
        for row in (plan, position):
            if row is None:
                continue
            row_changed = False
            if getattr(row, "trading_style", None) != style:
                row.trading_style = style
                row_changed = True
            if getattr(row, "trading_style_policy_version", None) != TRADING_STYLE_POLICY_VERSION:
                row.trading_style_policy_version = TRADING_STYLE_POLICY_VERSION
                row_changed = True
            desired_settings_json = self._json_dumps(style_evidence)
            if getattr(row, "trading_style_settings_json", None) != desired_settings_json:
                row.trading_style_settings_json = desired_settings_json
                row_changed = True
            if getattr(row, "max_holding_sessions", None) != max_holding:
                row.max_holding_sessions = max_holding
                row_changed = True
            if row_changed and hasattr(row, "updated_at"):
                row.updated_at = now
            changed = changed or row_changed
        return changed

    def repair_portfolio_import_strategy_fields(
        self,
        *,
        now: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Repair frozen style fields on existing portfolio-import paper rows.

        This is intentionally narrow: it only updates the strategy style
        metadata/max-duration columns on ``portfolio_import`` plan/position
        rows and never rewrites dates, ledger events, cost basis, quantity, or
        sync payloads.
        """

        repaired_at = now or utc_naive_now()
        max_rows = int(limit) if limit is not None else None
        if max_rows is not None and max_rows <= 0:
            return {
                "scanned": 0,
                "repaired": 0,
                "skipped": 0,
                "limit": max_rows,
                "items": [],
            }

        scanned = 0
        repaired = 0
        skipped = 0
        items: List[Dict[str, Any]] = []
        seen_plans: set[int] = set()

        def add_item(*, plan, position, changed: bool, reason: str) -> None:
            nonlocal scanned, repaired, skipped
            scanned += 1
            if changed:
                repaired += 1
            else:
                skipped += 1
            items.append({
                "plan_id": getattr(plan, "id", None) if plan is not None else None,
                "position_id": getattr(position, "id", None) if position is not None else None,
                "market": getattr(position, "market", None) or getattr(plan, "market", None),
                "stock_code": getattr(position, "stock_code", None) or getattr(plan, "stock_code", None),
                "repaired": bool(changed),
                "reason": reason,
            })

        def remaining() -> Optional[int]:
            if max_rows is None:
                return None
            return max(0, max_rows - scanned)

        with self._ledger_write_session() as session:
            position_query = (
                select(PaperPosition)
                .where(PaperPosition.sync_origin == "portfolio_import")
                .order_by(PaperPosition.id.asc())
            )
            if max_rows is not None:
                position_query = position_query.limit(max_rows)
            positions = list(session.execute(position_query).scalars().all())
            for position in positions:
                if max_rows is not None and scanned >= max_rows:
                    break
                plan = (
                    session.get(PaperExecutionPlan, int(position.plan_id))
                    if getattr(position, "plan_id", None) is not None
                    else None
                )
                if plan is not None and getattr(plan, "id", None) is not None:
                    seen_plans.add(int(plan.id))
                market = str(getattr(position, "market", None) or getattr(plan, "market", None) or "").strip().lower()
                stock_code = str(getattr(position, "stock_code", None) or getattr(plan, "stock_code", None) or "").strip()
                if not market or not stock_code:
                    add_item(plan=plan, position=position, changed=False, reason="missing_identity")
                    continue
                changed = self._repair_portfolio_import_strategy_fields_in_session(
                    session,
                    plan=plan,
                    position=position,
                    market=market,
                    stock_code=stock_code,
                    now=repaired_at,
                )
                add_item(
                    plan=plan,
                    position=position,
                    changed=changed,
                    reason="style_fields_repaired" if changed else "style_fields_already_complete",
                )

            plan_limit = remaining()
            if plan_limit is None or plan_limit > 0:
                plan_query = (
                    select(PaperExecutionPlan)
                    .where(PaperExecutionPlan.action == "portfolio_import")
                    .order_by(PaperExecutionPlan.id.asc())
                )
                if seen_plans:
                    plan_query = plan_query.where(~PaperExecutionPlan.id.in_(seen_plans))
                if plan_limit is not None:
                    plan_query = plan_query.limit(plan_limit)
                plans = list(session.execute(plan_query).scalars().all())
                for plan in plans:
                    if max_rows is not None and scanned >= max_rows:
                        break
                    market = str(getattr(plan, "market", None) or "").strip().lower()
                    stock_code = str(getattr(plan, "stock_code", None) or "").strip()
                    if not market or not stock_code:
                        add_item(plan=plan, position=None, changed=False, reason="missing_identity")
                        continue
                    changed = self._repair_portfolio_import_strategy_fields_in_session(
                        session,
                        plan=plan,
                        position=None,
                        market=market,
                        stock_code=stock_code,
                        now=repaired_at,
                    )
                    add_item(
                        plan=plan,
                        position=None,
                        changed=changed,
                        reason="style_fields_repaired" if changed else "style_fields_already_complete",
                    )

        return {
            "scanned": scanned,
            "repaired": repaired,
            "skipped": skipped,
            "limit": max_rows,
            "items": items,
        }

    def _analysis_window_sessions_for_identity(self, market: Optional[str], stock_code: Optional[str]) -> int:
        settings = self._effective_style_settings_for_identity(market=market, stock_code=stock_code)
        return max(20, int(settings.get("analysis_window_sessions") or self.analysis_window_sessions))

    def _style_settings_for_plan(self, plan: Any) -> Dict[str, Any]:
        payload, _error = self._json_object(getattr(plan, "trading_style_settings_json", None))
        settings = payload.get("settings") if isinstance(payload, Mapping) else None
        if isinstance(settings, Mapping):
            return dict(settings)
        style = self._normalize_trading_style(getattr(plan, "trading_style", None))
        return dict(TRADING_STYLE_SETTINGS[style])

    def _style_risk_multiplier_for_plan(self, plan: Any) -> float:
        settings = self._style_settings_for_plan(plan)
        try:
            return max(0.0, float(settings.get("risk_multiplier")))
        except (TypeError, ValueError):
            return max(0.0, self.style_risk_multiplier)

    def _market_regime_snapshot_for_plan(
        self,
        *,
        market: str,
        session_date: Optional[date],
    ) -> Dict[str, Any]:
        if not isinstance(session_date, date):
            return {"regime": "unavailable", "policy_version": MARKET_REGIME_POLICY_VERSION}
        try:
            with self.db.get_session() as session:
                row = self.repo.latest_market_regime(
                    region=str(market or "").lower(),
                    session_date=session_date,
                )
        except Exception:
            row = None
        if row is None:
            return {"regime": "unavailable", "policy_version": MARKET_REGIME_POLICY_VERSION}
        return {
            "id": int(row.id),
            "regime": str(row.regime or "unavailable"),
            "policy_version": str(row.policy_version or MARKET_REGIME_POLICY_VERSION),
            "source_report_id": row.source_report_id,
        }

    def _strategy_thesis_key(
        self,
        *,
        source_model: str,
        market: str,
        stock_code: str,
        trading_style: str,
    ) -> str:
        seed = {
            "policy": STRATEGY_THESIS_POLICY_VERSION,
            "source_model": str(source_model or ""),
            "market": str(market or "").lower(),
            "stock_code": str(stock_code or ""),
            "trading_style": self._normalize_trading_style(trading_style),
        }
        return "thesis-" + hashlib.sha256(
            self._json_dumps(seed).encode("utf-8")
        ).hexdigest()[:32]

    def _strategy_transition_key(
        self,
        *,
        thesis_key: str,
        source_report_id: int,
        decision_signal_id: Optional[int],
        validation_fingerprint: Optional[str] = None,
    ) -> str:
        seed = (
            f"{thesis_key}|report:{int(source_report_id)}|"
            f"signal:{decision_signal_id or 0}|validation:{validation_fingerprint or 'default'}"
        )
        return "thesis-transition-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:32]

    def _strategy_validation_fingerprint(
        self,
        *,
        initial_executable: bool,
        invalid_reasons: Sequence[str],
    ) -> str:
        payload = {
            "initial_executable": bool(initial_executable),
            "invalid_reasons": sorted({str(item) for item in invalid_reasons if item}),
        }
        return hashlib.sha256(self._json_dumps(payload).encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _strategy_action_bucket(action: Any) -> str:
        normalized = normalize_decision_action(action)
        if normalized in {"buy", "add"}:
            return "bullish"
        if normalized in {"sell", "reduce", "avoid", "alert"}:
            return "defensive"
        if normalized == "hold":
            return "holding"
        return "neutral"

    def _report_strategy_text(self, raw_report: Mapping[str, Any], report: AnalysisHistory) -> Optional[str]:
        candidates: List[Any] = [
            raw_report.get("analysis_summary"),
            raw_report.get("summary"),
            raw_report.get("core_insight"),
        ]
        dashboard = raw_report.get("dashboard")
        if isinstance(dashboard, Mapping):
            candidates.extend([
                dashboard.get("core_insight"),
                dashboard.get("insight"),
                dashboard.get("summary"),
            ])
        battle_plan = raw_report.get("battle_plan")
        if isinstance(battle_plan, Mapping):
            candidates.extend([
                battle_plan.get("strategy"),
                battle_plan.get("plan"),
            ])
        for value in candidates:
            text = str(value or "").strip()
            if text:
                return text[:1000]
        text = str(getattr(report, "analysis_summary", "") or "").strip()
        return text[:1000] if text else None

    @staticmethod
    def _strategy_confidence(signal: Optional[DecisionSignalRecord]) -> Optional[float]:
        if signal is None:
            return None
        try:
            value = float(getattr(signal, "confidence", None))
        except (TypeError, ValueError):
            return None
        return value if math.isfinite(value) else None

    @staticmethod
    def _strategy_session_dates_from_evidence(
        evidence: Mapping[str, Any],
        key: str,
    ) -> List[str]:
        raw = evidence.get(key) if isinstance(evidence, Mapping) else None
        if not isinstance(raw, list):
            return []
        dates: List[str] = []
        for item in raw:
            try:
                value = date.fromisoformat(str(item)).isoformat()
            except (TypeError, ValueError):
                continue
            if value not in dates:
                dates.append(value)
        return dates

    def _strategy_report_session_date(
        self,
        *,
        signal: Optional[DecisionSignalRecord],
        market: str,
        report_created_at: datetime,
    ) -> date:
        # Report creation is server-owned. Metadata may describe an effective
        # bar or a next-session plan and must not manufacture another session.
        report_time = report_created_at
        if report_time.tzinfo is not None:
            return report_time.astimezone(self._market_timezone(market)).date()
        return report_time.date()

    def _strategy_confirmation_session_valid(self, market: str, session_date: date) -> bool:
        """Never count weekends, holidays or an unavailable calendar as evidence."""
        from src.core.trading_calendar import MarketPhase, infer_market_phase

        local_noon = datetime.combine(session_date, time(12), tzinfo=self._market_timezone(market))
        return infer_market_phase(market, local_noon) not in {
            MarketPhase.UNKNOWN, MarketPhase.NON_TRADING,
        }

    @staticmethod
    def _lock_strategy_state(session) -> None:
        """Serialize first-thesis inserts across report hooks and bootstrap."""
        if session.get_bind().dialect.name == "postgresql":
            session.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": 731904207})

    def _report_is_older_than_strategy_observation(
        self,
        session,
        *,
        thesis: Optional[PaperStrategyThesis],
        report: AnalysisHistory,
        report_time: datetime,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        if thesis is None or getattr(thesis, "last_report_id", None) is None:
            return False, None
        current_id = int(getattr(report, "id", 0) or 0)
        last_id = int(getattr(thesis, "last_report_id", 0) or 0)
        if current_id == last_id:
            return False, None
        last_report = session.get(AnalysisHistory, last_id)
        if last_report is None:
            return False, None
        last_time = getattr(last_report, "created_at", None)
        if not isinstance(last_time, datetime):
            return False, None
        current_tuple = (self._utc_naive(report_time), current_id)
        last_tuple = (self._utc_naive(last_time), last_id)
        if current_tuple >= last_tuple:
            return False, None
        return True, {
            "current_report_id": current_id,
            "current_report_created_at": current_tuple[0].isoformat(),
            "last_report_id": last_id,
            "last_report_created_at": last_tuple[0].isoformat(),
        }

    def _open_position_count_for_identity_in_session(
        self,
        session,
        *,
        source_model: str,
        market: str,
        stock_code: str,
    ) -> int:
        return int(session.execute(
            select(func.count(PaperPosition.id)).where(
                PaperPosition.source_model == source_model,
                PaperPosition.market == market,
                PaperPosition.stock_code == stock_code,
                PaperPosition.status == "open",
            )
        ).scalar() or 0)

    def _freeze_unfilled_entry_plans_in_session(
        self,
        session,
        *,
        source_model: str,
        market: str,
        stock_code: str,
        source_report_id: int,
        reason: str,
        evidence: Mapping[str, Any],
    ) -> int:
        plans = list(session.execute(
            select(PaperExecutionPlan)
            .where(
                PaperExecutionPlan.source_model == source_model,
                PaperExecutionPlan.market == market,
                PaperExecutionPlan.stock_code == stock_code,
                PaperExecutionPlan.plan_version == PLAN_VERSION,
                PaperExecutionPlan.status.in_(["active", "triggered"]),
                PaperExecutionPlan.executable.is_(True),
            )
            .order_by(desc(PaperExecutionPlan.report_created_at), desc(PaperExecutionPlan.id))
        ).scalars().all())
        frozen = 0
        now = utc_naive_now()
        for plan in plans:
            if int(plan.source_report_id or 0) == int(source_report_id):
                continue
            descriptions, _ = self._json_list(plan.descriptive_conditions_json)
            descriptions.append({
                "source": "paper_trading.strategy_thesis",
                "policy_version": STRATEGY_THESIS_POLICY_VERSION,
                "reason": reason,
                "text": "latest accepted non-bullish thesis freezes remaining entry authority; open lots keep deterministic monitoring",
                "frozen_by_source_report_id": int(source_report_id),
                "evidence": dict(evidence or {}),
            })
            plan.status = "superseded"
            plan.updated_at = now
            plan.descriptive_conditions_json = self._json_dumps(descriptions)
            frozen += 1
        return frozen

    @staticmethod
    def _serialize_strategy_transition(row: PaperStrategyTransition) -> Dict[str, Any]:
        evidence, _ = PaperTradingService._json_object(getattr(row, "evidence_json", None) or "{}")
        return {
            "id": int(row.id),
            "thesis_id": row.thesis_id,
            "thesis_key": row.thesis_key,
            "policy_version": row.policy_version,
            "source_model": row.source_model,
            "market": row.market,
            "stock_code": row.stock_code,
            "trading_style": row.trading_style,
            "source_report_id": row.source_report_id,
            "decision_signal_id": row.decision_signal_id,
            "raw_action": row.raw_action,
            "previous_state": row.previous_state,
            "new_state": row.new_state,
            "result": row.result,
            "reason": row.reason,
            "plan_authorized": bool(row.plan_authorized),
            "risk_revision_authorized": bool(row.risk_revision_authorized),
            "freeze_entry_authorized": bool(row.freeze_entry_authorized),
            "confirmation_count": row.confirmation_count,
            "contradiction_count": row.contradiction_count,
            "entry_low": row.entry_low,
            "entry_high": row.entry_high,
            "stop_loss": row.stop_loss,
            "target_price": row.target_price,
            "evidence": evidence,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _serialize_strategy_thesis_row(row: Optional[PaperStrategyThesis]) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        evidence, _ = PaperTradingService._json_object(getattr(row, "evidence_json", None) or "{}")
        return {
            "id": int(row.id),
            "thesis_key": row.thesis_key,
            "policy_version": row.policy_version,
            "source_model": row.source_model,
            "market": row.market,
            "stock_code": row.stock_code,
            "stock_name": row.stock_name,
            "trading_style": row.trading_style,
            "state": row.state,
            "effective_action": row.effective_action,
            "raw_action": row.raw_action,
            "action_bucket": row.action_bucket,
            "confirmation_count": row.confirmation_count,
            "contradiction_count": row.contradiction_count,
            "accepted_report_id": row.accepted_report_id,
            "accepted_signal_id": row.accepted_signal_id,
            "last_report_id": row.last_report_id,
            "last_signal_id": row.last_signal_id,
            "entry_low": row.entry_low,
            "entry_high": row.entry_high,
            "stop_loss": row.stop_loss,
            "target_price": row.target_price,
            "thesis_text": row.thesis_text,
            "invalidation_text": row.invalidation_text,
            "evidence": evidence,
            "accepted_at": row.accepted_at.isoformat() if row.accepted_at else None,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    def _evaluate_strategy_thesis_transition(
        self,
        *,
        report: AnalysisHistory,
        signal: Optional[DecisionSignalRecord],
        source_model: str,
        market: str,
        stock_code: str,
        stock_name: Optional[str],
        trading_style: str,
        raw_action: Optional[str],
        raw_report: Mapping[str, Any],
        entry_low: Optional[float],
        entry_high: Optional[float],
        stop_loss: Optional[float],
        target_price: Optional[float],
        initial_executable: bool,
        invalid_reasons: Sequence[str],
        report_created_at: datetime,
    ) -> Dict[str, Any]:
        """Persist whether a report is accepted into the medium-term thesis.

        LLM reports are treated as evidence.  This state machine decides whether
        the evidence may change executable entry plans or held-position risk.
        Hard risk remains immediate; ordinary contradictory reports need
        confirmation so a medium-term style does not churn on single-run noise.
        """

        if not self.strategy_thesis_enabled:
            return {
                "policy_version": STRATEGY_THESIS_POLICY_VERSION,
                "result": "disabled",
                "reason": "strategy_thesis_disabled",
                "plan_authorized": bool(initial_executable),
                "risk_revision_authorized": True,
                "freeze_entry_authorized": False,
                "state": None,
                "confirmation_count": 0,
                "contradiction_count": 0,
            }
        if signal is None or not source_model or not market or not stock_code:
            return {
                "policy_version": STRATEGY_THESIS_POLICY_VERSION,
                "result": "rejected",
                "reason": "missing_strategy_identity",
                "plan_authorized": False,
                "risk_revision_authorized": False,
                "freeze_entry_authorized": False,
                "state": "unavailable",
                "confirmation_count": 0,
                "contradiction_count": 0,
            }

        style = self._normalize_trading_style(trading_style)
        thesis_key = self._strategy_thesis_key(
            source_model=source_model,
            market=market,
            stock_code=stock_code,
            trading_style=style,
        )
        validation_fingerprint = self._strategy_validation_fingerprint(
            initial_executable=initial_executable,
            invalid_reasons=invalid_reasons,
        )
        transition_key = self._strategy_transition_key(
            thesis_key=thesis_key,
            source_report_id=int(report.id),
            decision_signal_id=getattr(signal, "id", None),
            validation_fingerprint=validation_fingerprint,
        )
        action = normalize_decision_action(raw_action) or "unknown"
        bucket = self._strategy_action_bucket(action)
        confidence = self._strategy_confidence(signal)
        report_time = self._utc_naive(report_created_at)
        report_session_date = self._strategy_report_session_date(
            signal=signal,
            market=market,
            report_created_at=report_created_at,
        )
        with self._ledger_write_session() as session:
            self._lock_strategy_state(session)
            existing_transition = session.execute(
                select(PaperStrategyTransition)
                .where(PaperStrategyTransition.transition_key == transition_key)
                .limit(1)
            ).scalar_one_or_none()
            if existing_transition is not None:
                return self._serialize_strategy_transition(existing_transition)

            thesis = session.execute(
                select(PaperStrategyThesis)
                .where(PaperStrategyThesis.thesis_key == thesis_key)
                .limit(1)
            ).scalar_one_or_none()
            previous_state = thesis.state if thesis is not None else None
            previous_bucket = thesis.action_bucket if thesis is not None else None
            previous_action = thesis.effective_action if thesis is not None else None
            previous_evidence, _ = (
                self._json_object(getattr(thesis, "evidence_json", None))
                if thesis is not None else ({}, None)
            )
            stale_report, stale_evidence = self._report_is_older_than_strategy_observation(
                session,
                thesis=thesis,
                report=report,
                report_time=report_time,
            )
            has_open_position = self._open_position_count_for_identity_in_session(
                session,
                source_model=source_model,
                market=market,
                stock_code=stock_code,
            ) > 0
            plan_authorized = False
            risk_revision_authorized = False
            freeze_entry_authorized = False
            result = "observed"
            reason = "evidence_recorded"
            new_state = previous_state or "watching"
            confirmation_count = 0
            contradiction_count = int(getattr(thesis, "contradiction_count", 0) or 0) if thesis is not None else 0
            confirmation_dates = self._strategy_session_dates_from_evidence(
                previous_evidence,
                "confirmation_session_dates",
            )
            contradiction_dates = self._strategy_session_dates_from_evidence(
                previous_evidence,
                "contradiction_session_dates",
            )
            session_date_key = report_session_date.isoformat()
            confirmation_session_valid = self._strategy_confirmation_session_valid(market, report_session_date)
            declared_session = self._signal_session_date(signal)
            confirmation_session_valid = confirmation_session_valid and (
                declared_session is None or declared_session == report_session_date
            )
            confirmation_now = utc_naive_now().replace(tzinfo=timezone.utc)
            confirmation_expires_at = market_context_freshness_expires_at(market, report_session_date)
            confirmation_fresh = bool(
                confirmation_expires_at is not None
                and self._utc_naive(confirmation_now) < confirmation_expires_at
                and report_session_date <= confirmation_now.astimezone(self._market_timezone(market)).date()
            )
            # Revalidate legacy dates too: old state may have counted weekends.
            confirmation_dates = [value for value in confirmation_dates
                                  if value <= session_date_key and self._strategy_confirmation_session_valid(
                                      market, date.fromisoformat(value))]
            contradiction_dates = [value for value in contradiction_dates
                                   if value <= session_date_key and self._strategy_confirmation_session_valid(
                                       market, date.fromisoformat(value))]
            # A months-old candidate is not the first half of today's
            # confirmation. Carry evidence only across adjacent sessions.
            for dates in (confirmation_dates, contradiction_dates):
                if dates:
                    last_expiry = market_context_freshness_expires_at(market, date.fromisoformat(dates[-1]))
                    next_day = (last_expiry.replace(tzinfo=timezone.utc).astimezone(
                        self._market_timezone(market)).date() if last_expiry else None)
                    if next_day is None or report_session_date > next_day:
                        dates.clear()
            required: Optional[int] = None

            if stale_report:
                evidence = {
                    "raw_action": action,
                    "action_bucket": bucket,
                    "previous_state": previous_state,
                    "previous_action": previous_action,
                    "previous_bucket": previous_bucket,
                    "has_open_position": has_open_position,
                    "initial_executable": bool(initial_executable),
                    "invalid_reasons": list(dict.fromkeys(str(item) for item in invalid_reasons if item)),
                    "entry_confirmation_required": self.strategy_entry_confirmation_sessions,
                    "flip_confirmation_required": self.strategy_flip_confirmation_sessions,
                    "effective_confirmation_required": None,
                    "hard_risk_immediate": DETERMINISTIC_HARD_RISK_IMMEDIATE,
                    "confidence": confidence,
                    "thesis_text": self._report_strategy_text(raw_report, report),
                    "report_session_date": session_date_key,
                    "validation_fingerprint": validation_fingerprint,
                    "stale_report": stale_evidence or {},
                    "confirmation_session_dates": confirmation_dates,
                    "contradiction_session_dates": contradiction_dates,
                }
                transition = PaperStrategyTransition(
                    transition_key=transition_key,
                    thesis_id=thesis.id if thesis is not None else None,
                    thesis_key=thesis_key,
                    policy_version=STRATEGY_THESIS_POLICY_VERSION,
                    source_model=source_model,
                    market=market,
                    stock_code=stock_code,
                    trading_style=style,
                    source_report_id=int(report.id),
                    decision_signal_id=getattr(signal, "id", None),
                    raw_action=action,
                    previous_state=previous_state,
                    new_state=previous_state or "watching",
                    result="rejected",
                    reason="stale_report_older_than_strategy_observation",
                    plan_authorized=False,
                    risk_revision_authorized=False,
                    freeze_entry_authorized=False,
                    confirmation_count=int(getattr(thesis, "confirmation_count", 0) or 0) if thesis is not None else 0,
                    contradiction_count=contradiction_count,
                    entry_low=entry_low,
                    entry_high=entry_high,
                    stop_loss=stop_loss,
                    target_price=target_price,
                    evidence_json=self._json_dumps(evidence),
                )
                session.add(transition)
                session.flush()
                return self._serialize_strategy_transition(transition)

            bullish_executable = bucket == "bullish" and bool(initial_executable)
            verified_intraday_reanalysis = self._is_verified_same_session_reanalysis(
                signal=signal,
                raw_report=raw_report,
                market=market,
            )

            if confirmation_session_valid and not confirmation_fresh and bucket in {"bullish", "defensive"}:
                confirmation_count = len(confirmation_dates)
                contradiction_count = len(contradiction_dates)
                new_state = previous_state or ("position_monitoring" if has_open_position else "watching")
                reason = "strategy_report_outside_confirmation_window"
            elif not confirmation_session_valid and bucket in {"bullish", "defensive"}:
                # A weekend report is useful information, not another trading
                # session confirming a thesis. Preserve prior valid evidence.
                confirmation_count = len(confirmation_dates)
                contradiction_count = len(contradiction_dates)
                freeze_entry_authorized = bucket == "defensive"
                new_state = previous_state or ("position_monitoring" if has_open_position else "candidate")
                reason = "confirmation_session_unavailable_or_non_trading"
            elif bullish_executable:
                pending_bullish_confirmation = bool(
                    previous_state == "candidate"
                    and previous_evidence.get("action_bucket") == "bullish"
                )
                continuation = (
                    pending_bullish_confirmation
                    or previous_bucket in {None, "bullish", "holding"}
                    or previous_action in {None, "buy", "add", "hold"}
                )
                if not continuation:
                    confirmation_dates = []
                if session_date_key not in confirmation_dates:
                    confirmation_dates.append(session_date_key)
                confirmation_count = len(confirmation_dates)
                contradiction_dates = []
                contradiction_count = 0
                required = self.strategy_entry_confirmation_sessions
                if (
                    not has_open_position
                    and thesis is not None
                    and previous_bucket in {"defensive"}
                ):
                    required = max(required, self.strategy_flip_confirmation_sessions)
                if has_open_position:
                    required = 1
                    new_state = "position_monitoring"
                else:
                    new_state = "entry_active" if confirmation_count >= required else "candidate"
                if confirmation_count >= required:
                    plan_authorized = True
                    risk_revision_authorized = has_open_position
                    result = "accepted"
                    reason = "accepted_entry_thesis"
                    contradiction_count = 0
                else:
                    result = "observed"
                    reason = "awaiting_entry_confirmation"
            elif bucket == "bullish":
                freeze_entry_authorized = True
                new_state = "position_monitoring" if has_open_position else "watching"
                reason = "entry_evidence_invalid"
                confirmation_dates = []
                confirmation_count = 0
            elif has_open_position and action in {"hold", "watch"}:
                new_state = "position_monitoring"
                risk_revision_authorized = stop_loss is not None or target_price is not None
                freeze_entry_authorized = True
                result = "accepted"
                reason = "accepted_holding_risk_refresh"
                confirmation_count = 0
                contradiction_count = 0
                confirmation_dates = [session_date_key]
                contradiction_dates = []
            elif has_open_position and bucket == "defensive" and verified_intraday_reanalysis:
                new_state = "risk_off"
                risk_revision_authorized = True
                result = "accepted"
                reason = "accepted_verified_intraday_reanalysis"
                freeze_entry_authorized = True
                confirmation_count = 0
                contradiction_count = 0
                confirmation_dates = []
                contradiction_dates = [session_date_key]
            elif has_open_position and bucket == "defensive":
                if session_date_key not in contradiction_dates:
                    contradiction_dates.append(session_date_key)
                contradiction_count = len(contradiction_dates)
                confirmation_dates = []
                new_state = "risk_review"
                freeze_entry_authorized = True
                if contradiction_count >= self.strategy_flip_confirmation_sessions:
                    new_state = "risk_off"
                    risk_revision_authorized = True
                    result = "accepted"
                    reason = "accepted_confirmed_defensive_flip"
                    confirmation_count = 0
                else:
                    result = "observed"
                    reason = "awaiting_defensive_confirmation"
            else:
                freeze_entry_authorized = True
                new_state = "watching" if bucket in {"neutral", "holding"} else "risk_off"
                result = "accepted"
                reason = "accepted_non_bullish_freezes_unfilled_entry"
                confirmation_count = 0
                contradiction_count = 0
                confirmation_dates = []
                contradiction_dates = [session_date_key] if bucket == "defensive" else []

            evidence = {
                "raw_action": action,
                "action_bucket": bucket,
                "previous_state": previous_state,
                "previous_action": previous_action,
                "previous_bucket": previous_bucket,
                "has_open_position": has_open_position,
                "initial_executable": bool(initial_executable),
                "invalid_reasons": list(dict.fromkeys(str(item) for item in invalid_reasons if item)),
                "entry_confirmation_required": self.strategy_entry_confirmation_sessions,
                "flip_confirmation_required": self.strategy_flip_confirmation_sessions,
                "effective_confirmation_required": required if bullish_executable else None,
                "hard_risk_immediate": DETERMINISTIC_HARD_RISK_IMMEDIATE,
                "confidence": confidence,
                "thesis_text": self._report_strategy_text(raw_report, report),
                "report_session_date": session_date_key,
                "validation_fingerprint": validation_fingerprint,
                "confirmation_session_dates": confirmation_dates,
                "contradiction_session_dates": contradiction_dates,
                "distinct_session_confirmation": True,
                "confirmation_session_valid": confirmation_session_valid,
                "confirmation_fresh": confirmation_fresh,
                "confirmation_expires_at": confirmation_expires_at.isoformat() if confirmation_expires_at else None,
                "declared_session_date": declared_session.isoformat() if declared_session else None,
                "verified_intraday_reanalysis": verified_intraday_reanalysis,
            }

            if thesis is None:
                thesis = PaperStrategyThesis(
                    thesis_key=thesis_key,
                    policy_version=STRATEGY_THESIS_POLICY_VERSION,
                    source_model=source_model,
                    market=market,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    trading_style=style,
                    state=new_state,
                    effective_action=action if result == "accepted" else (previous_action or "watch"),
                    raw_action=action,
                    action_bucket=bucket,
                    confirmation_count=confirmation_count,
                    contradiction_count=contradiction_count,
                    last_report_id=int(report.id),
                    last_signal_id=getattr(signal, "id", None),
                    evidence_json=self._json_dumps(evidence),
                )
                session.add(thesis)
                session.flush()
            else:
                thesis.last_report_id = int(report.id)
                thesis.last_signal_id = getattr(signal, "id", None)
                thesis.raw_action = action
                thesis.confirmation_count = confirmation_count
                thesis.contradiction_count = contradiction_count
                thesis.evidence_json = self._json_dumps(evidence)
                thesis.updated_at = utc_naive_now()

            if result == "accepted":
                thesis.state = new_state
                thesis.effective_action = action
                thesis.action_bucket = bucket
                thesis.accepted_report_id = int(report.id)
                thesis.accepted_signal_id = getattr(signal, "id", None)
                thesis.accepted_at = report_time
                thesis.entry_low = entry_low if entry_low is not None else thesis.entry_low
                thesis.entry_high = entry_high if entry_high is not None else thesis.entry_high
                # Held-position prices become effective only after the revision
                # consumer validates a fresh/as-of quote and applies them. The
                # transition itself records proposals, not effective risk lines.
                if not has_open_position:
                    thesis.stop_loss = stop_loss if stop_loss is not None else thesis.stop_loss
                    thesis.target_price = target_price if target_price is not None else thesis.target_price
                thesis.thesis_text = evidence.get("thesis_text") or thesis.thesis_text
                thesis.invalidation_text = None
            else:
                thesis.state = new_state

            frozen_count = 0
            if freeze_entry_authorized:
                frozen_count = self._freeze_unfilled_entry_plans_in_session(
                    session,
                    source_model=source_model,
                    market=market,
                    stock_code=stock_code,
                    source_report_id=int(report.id),
                    reason=reason,
                    evidence=evidence,
                )
                evidence["frozen_entry_plan_count"] = frozen_count
                thesis.evidence_json = self._json_dumps(evidence)

            transition = PaperStrategyTransition(
                transition_key=transition_key,
                thesis_id=thesis.id,
                thesis_key=thesis_key,
                policy_version=STRATEGY_THESIS_POLICY_VERSION,
                source_model=source_model,
                market=market,
                stock_code=stock_code,
                trading_style=style,
                source_report_id=int(report.id),
                decision_signal_id=getattr(signal, "id", None),
                raw_action=action,
                previous_state=previous_state,
                new_state=new_state,
                result=result,
                reason=reason,
                plan_authorized=plan_authorized,
                risk_revision_authorized=risk_revision_authorized,
                freeze_entry_authorized=freeze_entry_authorized,
                confirmation_count=confirmation_count,
                contradiction_count=contradiction_count,
                entry_low=entry_low,
                entry_high=entry_high,
                stop_loss=stop_loss,
                target_price=target_price,
                evidence_json=self._json_dumps(evidence),
            )
            session.add(transition)
            session.flush()
            summary = self._serialize_strategy_transition(transition)
            summary["frozen_entry_plan_count"] = frozen_count
            return summary

    def _strategy_transition_allows_risk_revision(
        self,
        *,
        report: AnalysisHistory,
        signal: Optional[DecisionSignalRecord],
        source_model: str,
        market: str,
        stock_code: str,
    ) -> bool:
        if not self.strategy_thesis_enabled:
            return True
        if signal is None:
            return False
        style_evidence = self._trading_style_evidence(
            signal=signal,
            raw_report=None,
            market=market,
            stock_code=stock_code,
        )
        style = self._normalize_trading_style(style_evidence.get("trading_style") or self.trading_style)
        thesis_key = self._strategy_thesis_key(
            source_model=source_model,
            market=market,
            stock_code=stock_code,
            trading_style=style,
        )
        with self.db.get_session() as session:
            row = session.execute(
                select(PaperStrategyTransition)
                .where(
                    PaperStrategyTransition.thesis_key == thesis_key,
                    PaperStrategyTransition.source_report_id == int(report.id),
                    PaperStrategyTransition.decision_signal_id == getattr(signal, "id", None),
                )
                .order_by(desc(PaperStrategyTransition.created_at), desc(PaperStrategyTransition.id))
                .limit(1)
            ).scalar_one_or_none()
            return bool(row and row.risk_revision_authorized)

    def _ensure_strategy_transition_for_signal(
        self,
        *,
        report: Optional[AnalysisHistory],
        signal: Optional[DecisionSignalRecord],
        source_model: str,
        market: str,
        stock_code: str,
        initial_executable: bool = False,
        invalid_reasons: Sequence[str] = (),
    ) -> Optional[Dict[str, Any]]:
        if not self.strategy_thesis_enabled:
            return {
                "policy_version": STRATEGY_THESIS_POLICY_VERSION,
                "result": "disabled",
                "reason": "strategy_thesis_disabled",
                "plan_authorized": bool(initial_executable),
                "risk_revision_authorized": True,
                "freeze_entry_authorized": False,
            }
        if report is None or signal is None:
            return None
        identity = canonical_paper_identity(market, stock_code)
        canonical_market, canonical_code = (
            identity if identity is not None else (str(market or "").lower(), str(stock_code or ""))
        )
        raw_report, _raw_error = self._json_object(getattr(report, "raw_result", None))
        style_evidence = self._trading_style_evidence(
            signal=signal,
            raw_report=raw_report,
            market=canonical_market,
            stock_code=canonical_code,
        )
        style = self._normalize_trading_style(style_evidence.get("trading_style") or self.trading_style)
        # A monitor discovers persisted evidence; it must not adjudicate that
        # same report again with a different caller-specific invalid_reason.
        # Doing so could replace an accepted defensive transition with a later
        # expired observation and silently cancel its execution authority.
        thesis_key = self._strategy_thesis_key(
            source_model=str(source_model or getattr(report, "model_used", None) or getattr(signal, "source_agent", "") or "").strip(),
            market=canonical_market,
            stock_code=canonical_code,
            trading_style=style,
        )
        with self.db.get_session() as session:
            existing = session.scalar(select(PaperStrategyTransition).where(
                PaperStrategyTransition.thesis_key == thesis_key,
                PaperStrategyTransition.source_report_id == int(report.id),
                PaperStrategyTransition.decision_signal_id == signal.id,
            ).order_by(desc(PaperStrategyTransition.created_at), desc(PaperStrategyTransition.id)).limit(1))
            if existing is not None:
                return self._serialize_strategy_transition(existing)
        return self._evaluate_strategy_thesis_transition(
            report=report,
            signal=signal,
            source_model=str(source_model or getattr(report, "model_used", None) or getattr(signal, "source_agent", "") or "").strip(),
            market=canonical_market,
            stock_code=canonical_code,
            stock_name=getattr(signal, "stock_name", None) or getattr(report, "name", None),
            trading_style=style,
            raw_action=normalize_decision_action(getattr(signal, "action", None)),
            raw_report=raw_report,
            entry_low=self._safe_price(getattr(signal, "entry_low", None) or getattr(report, "secondary_buy", None)),
            entry_high=self._safe_price(getattr(signal, "entry_high", None) or getattr(report, "ideal_buy", None)),
            stop_loss=self._safe_price(getattr(signal, "stop_loss", None) or getattr(report, "stop_loss", None)),
            target_price=self._safe_price(getattr(signal, "target_price", None) or getattr(report, "take_profit", None)),
            initial_executable=bool(initial_executable),
            invalid_reasons=list(invalid_reasons or ()),
            report_created_at=(
                getattr(report, "created_at", None)
                or getattr(signal, "created_at", None)
                or utc_naive_now()
            ),
        )

    def _ensure_defensive_signal_strategy_transition(
        self,
        signal: Optional[DecisionSignalRecord],
        *,
        report: Optional[AnalysisHistory],
        evaluated_at: datetime,
        invalid_reason: str,
    ) -> Optional[Dict[str, Any]]:
        """Create the thesis audit row required before model-only exits.

        Defensive report signals may be discovered later by the EOD or intraday
        monitor rather than immediately after the report is stored.  The monitor
        itself must remain read-only while resolving eligibility inside position
        write transactions, so callers invoke this preflight outside those
        transactions and then let ``_defensive_signal_eligibility`` verify the
        persisted decision.
        """

        if not self.strategy_thesis_enabled or signal is None or report is None:
            return None
        eligibility = self._holding_signal_eligibility(
            signal,
            report=report,
            evaluated_at=self._utc_naive(evaluated_at),
        )
        if not eligibility.get("eligible"):
            return None
        if eligibility.get("execution_action") not in {"sell", "reduce", "avoid"}:
            return None
        source_model = str(
            getattr(report, "model_used", None)
            or getattr(signal, "source_agent", None)
            or ""
        ).strip()
        return self._ensure_strategy_transition_for_signal(
            report=report,
            signal=signal,
            source_model=source_model,
            market=str(getattr(signal, "market", "") or ""),
            stock_code=str(getattr(signal, "stock_code", "") or ""),
            initial_executable=False,
            invalid_reasons=(invalid_reason,),
        )

    def _preflight_defensive_transition_for_position(
        self,
        position: PaperPosition,
        *,
        eval_date: date,
    ) -> Optional[Dict[str, Any]]:
        """Ensure the latest EOD defensive signal has an audit row before writes."""

        signal = self.repo.latest_defensive_signal(
            market=position.market,
            stock_code=position.stock_code,
            source_model=position.source_model,
            after_date=position.opened_on,
            through_date=eval_date,
            after_time=getattr(position, "opened_at", None),
            execution_scope=self.execution_scope,
        )
        if signal is None:
            return None
        report = self.repo.get_report(int(signal.source_report_id)) if getattr(signal, "source_report_id", None) else None
        return self._ensure_defensive_signal_strategy_transition(
            signal,
            report=report,
            evaluated_at=self._market_local_naive_to_utc_naive(
                position.market,
                eval_date,
                MARKET_REGULAR_CLOSE.get(position.market, time(15, 0)),
            ),
            invalid_reason="eod_model_exit_without_entry_authority",
        )

    def _latest_strategy_thesis_for_identity(
        self,
        *,
        source_model: str,
        market: str,
        stock_code: str,
        trading_style: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        style = self._normalize_trading_style(trading_style or self.trading_style)
        thesis_key = self._strategy_thesis_key(
            source_model=source_model,
            market=market,
            stock_code=stock_code,
            trading_style=style,
        )
        with self.db.get_session() as session:
            return self._latest_strategy_thesis_for_identity_in_session(
                session,
                source_model=source_model,
                market=market,
                stock_code=stock_code,
                trading_style=style,
            )

    def _latest_strategy_thesis_for_identity_in_session(
        self,
        session,
        *,
        source_model: str,
        market: str,
        stock_code: str,
        trading_style: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        style = self._normalize_trading_style(trading_style or self.trading_style)
        thesis_key = self._strategy_thesis_key(
            source_model=source_model,
            market=market,
            stock_code=stock_code,
            trading_style=style,
        )
        row = session.execute(
            select(PaperStrategyThesis)
            .where(PaperStrategyThesis.thesis_key == thesis_key)
            .limit(1)
        ).scalar_one_or_none()
        return self._serialize_strategy_thesis_row(row)

    def _pending_strategy_transition_for_identity_in_session(
        self,
        session,
        *,
        source_model: str,
        market: str,
        stock_code: str,
        trading_style: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return only the latest unresolved cross-session thesis transition.

        This is prompt context rather than execution authority.  Looking at the
        latest transition first prevents an older awaiting row from appearing
        pending after a newer accepted/rejected observation resolved it.
        """

        style = self._normalize_trading_style(trading_style or self.trading_style)
        thesis_key = self._strategy_thesis_key(
            source_model=source_model,
            market=market,
            stock_code=stock_code,
            trading_style=style,
        )
        row = session.execute(
            select(PaperStrategyTransition)
            .where(PaperStrategyTransition.thesis_key == thesis_key)
            .order_by(desc(PaperStrategyTransition.created_at), desc(PaperStrategyTransition.id))
            .limit(1)
        ).scalar_one_or_none()
        if row is None or row.result != "observed" or row.reason not in {
            "awaiting_entry_confirmation",
            "awaiting_defensive_confirmation",
        }:
            return None
        return self._serialize_strategy_transition(row)

    def report_execution_authorization(self, source_report_id: int) -> Dict[str, Any]:
        """Evaluate a report's frozen purpose for this service's execution scope."""

        return evaluate_report_execution_intent(
            self.repo.get_report_execution_intent(int(source_report_id)),
            execution_scope=self.execution_scope,
        )

    def create_plan_for_report(
        self,
        source_report_id: int,
        *,
        decision_signal_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Idempotently persist the current plan-version snapshot for one report."""

        report = self.repo.get_report(int(source_report_id))
        if report is None:
            raise PaperTradingNotFoundError(f"analysis report not found: {source_report_id}")
        if report.report_type == "market_review" or report.code == "MARKET":
            raise ValueError("market review reports are not eligible for paper execution plans")
        authorization = self.report_execution_authorization(int(report.id))
        if not authorization["authorized"]:
            raise PaperTradingExecutionNotAuthorizedError(
                authorization["reason"],
                report_id=int(report.id),
            )

        signal = (
            self.repo.get_signal(decision_signal_id)
            if decision_signal_id is not None
            else self.repo.latest_report_signal(report.id)
        )
        if signal is not None and (
            signal.source_type != "analysis" or signal.source_report_id != report.id
        ):
            raise ValueError("decision signal is not linked to the source analysis report")
        if decision_signal_id is not None and signal is None:
            raise PaperTradingNotFoundError(f"decision signal not found: {decision_signal_id}")

        signal_key = signal.id if signal is not None else 0
        plan_key = f"analysis:{report.id}:signal:{signal_key}:{PLAN_VERSION}"
        existing = self.repo.get_plan_by_key(plan_key)

        raw_report, raw_error = self._json_object(report.raw_result)
        if signal is not None:
            action = signal.action
        else:
            fallback_action = display_action_fields(
                operation_advice=raw_report.get("operation_advice") or report.operation_advice,
                explicit_action=raw_report.get("action"),
                action_label=raw_report.get("action_label"),
                report_type=report.report_type,
                report_language=raw_report.get("report_language"),
                sentiment_score=raw_report.get("sentiment_score") or report.sentiment_score,
                holding_state=raw_report.get("holding_state"),
                position_actions=raw_report.get("position_actions"),
            )
            action = fallback_action.get("action")
        market = signal.market if signal is not None else get_market_for_stock(normalize_stock_code(report.code))
        raw_stock_code = (
            signal.stock_code
            if signal is not None
            else canonical_stock_code(normalize_stock_code(report.code))
        )
        identity = canonical_paper_identity(market or "", raw_stock_code)
        stock_code = identity[1] if identity is not None else canonical_stock_code(raw_stock_code)
        model = str(
            report.model_used
            or raw_report.get("model_used")
            or (signal.source_agent if signal is not None else "")
            or ""
        ).strip()
        report_created_at = report.created_at or utc_naive_now()

        rules, descriptive, invalid_reasons = self._build_rules(
            report=report,
            signal=signal,
            raw_report=raw_report,
        )
        horizon = signal.horizon if signal is not None else None
        style_evidence = self._trading_style_evidence(
            signal=signal,
            raw_report=raw_report,
            market=market or "unknown",
            stock_code=stock_code,
        )
        effective_style = str(style_evidence.get("trading_style") or self.trading_style)
        effective_style_settings = dict(style_evidence.get("settings") or TRADING_STYLE_SETTINGS[effective_style])
        entry_low = self._safe_price(signal.entry_low if signal is not None else report.secondary_buy)
        entry_high = self._safe_price(signal.entry_high if signal is not None else report.ideal_buy)
        stop = self._safe_price(signal.stop_loss if signal is not None else report.stop_loss)
        target = self._safe_price(signal.target_price if signal is not None else report.take_profit)
        if raw_error:
            descriptive.append({"source": "report.raw_result", "text": raw_error, "reason": "invalid_json"})
            invalid_reasons.append("invalid_report_payload")
        bullish_entry_intent = action in {"buy", "add"}
        contract_correction_allowed = bullish_entry_intent and not self._is_same_session_reanalysis(raw_report)
        if not bullish_entry_intent:
            invalid_reasons.append("unsupported_or_non_bullish_action")
        if market not in VALID_MARKETS:
            invalid_reasons.append("unsupported_market")
        if identity is None:
            invalid_reasons.append("ambiguous_or_invalid_stock_identity")
        if not model:
            invalid_reasons.append("missing_source_model")
        if not rules:
            invalid_reasons.append("no_supported_entry_rule")
        if not any(rule.get("type") in PRICE_RULE_TYPES for rule in rules if isinstance(rule, dict)):
            invalid_reasons.append("no_supported_price_entry_rule")
        if stop is None:
            invalid_reasons.append("missing_valid_stop_loss")
        if target is None:
            invalid_reasons.append("missing_valid_target_price")
        entry_legs, entry_leg_errors = self._extract_entry_legs(
            raw_report=raw_report,
            base_rules=rules,
            entry_low=entry_low,
            entry_high=entry_high,
            style_settings=effective_style_settings,
        )
        exit_legs, exit_leg_errors, weighted_target, weighted_risk_reward, exit_targets = self._extract_exit_legs(
            raw_report=raw_report,
            entry_low=entry_low,
            entry_high=entry_high,
            stop=stop,
        )
        invalid_reasons.extend(entry_leg_errors)
        invalid_reasons.extend(exit_leg_errors)
        explicit_entry_contract = self._raw_entry_legs(raw_report) is not None
        explicit_exit_contract = self._raw_exit_legs(raw_report) is not None
        risk_entry_low = entry_low
        risk_entry_high = entry_high
        if explicit_entry_contract:
            explicit_low, explicit_high, explicit_bounded = self._entry_leg_price_bounds(entry_legs)
            if not explicit_bounded:
                invalid_reasons.append("unbounded_explicit_entry_geometry")
            else:
                risk_entry_low = explicit_low
                risk_entry_high = explicit_high
                if stop is not None and explicit_low is not None and stop >= explicit_low:
                    invalid_reasons.append("stop_not_below_entry")
                if target is not None and explicit_high is not None and target <= explicit_high:
                    invalid_reasons.append("target_not_above_entry")
        if explicit_exit_contract and exit_legs:
            reference_entry = self._entry_reference_price(risk_entry_low, risk_entry_high)
            if reference_entry is None:
                invalid_reasons.append("invalid_exit_leg_entry_reference")
            elif any(
                self._safe_price(leg.get("target_price")) is None
                or float(self._safe_price(leg.get("target_price"))) <= reference_entry
                for leg in exit_legs
            ):
                invalid_reasons.append("exit_leg_target_not_above_entry")
        if explicit_exit_contract and exit_legs:
            weighted_risk_reward = self._weighted_exit_risk_reward(
                exit_legs=exit_legs,
                stop=stop,
                entry_low=risk_entry_low,
                entry_high=risk_entry_high,
            )
            risk_target = weighted_target
            risk_reward = weighted_risk_reward
            final_leg_target = self._safe_price(exit_legs[-1].get("target_price")) if exit_legs else None
            canonical_final_target = exit_targets.get("final_target") or final_leg_target
            if (
                target is not None
                and canonical_final_target is not None
                and not math.isclose(target, canonical_final_target, rel_tol=0.0, abs_tol=1e-6)
            ):
                invalid_reasons.append("take_profit_final_target_mismatch")
        else:
            risk_target = target
            risk_reward = self._risk_reward_ratio(stop, target, risk_entry_low, risk_entry_high)
        if risk_reward is None:
            invalid_reasons.append("invalid_entry_risk_reward")
        elif risk_reward < self.intraday_min_risk_reward:
            invalid_reasons.append("risk_reward_below_policy")
        setup_evidence = setup_contract(raw_report, rules)
        setup_type = setup_evidence.get("setup_type")
        setup_policy = setup_factor_policy(setup_type if isinstance(setup_type, str) else None)
        if not setup_type:
            invalid_reasons.append(str(setup_evidence.get("error") or "unsupported_strategy_setup_type"))
        indicator_snapshot = (
            style_evidence.get("indicator_snapshot")
            if isinstance(style_evidence.get("indicator_snapshot"), Mapping)
            else None
        )
        geometry_guard = entry_geometry_guard(
            style=effective_style,
            style_settings=effective_style_settings,
            indicator_snapshot=indicator_snapshot,
            entry_reference=self._entry_reference_price(risk_entry_low, risk_entry_high),
            stop_loss=stop,
        )
        geometry_reasons = [
            str(reason)
            for reason in geometry_guard.get("reasons", [])
            if str(reason)
        ]
        if not bool(geometry_guard.get("passed")):
            invalid_reasons.extend(geometry_reasons or ["entry_geometry_guard_failed"])
            if contract_correction_allowed:
                invalid_reasons.append("requires_same_model_reanalysis")
        horizon_holding_policy = plan_holding_sessions_for_horizon(
            horizon,
            effective_style_settings,
            horizon_source=style_evidence.get("signal_horizon_source"),
        )
        compatible_horizons = set(effective_style_settings.get("compatible_horizons") or [])
        explicit_horizon = str(style_evidence.get("signal_horizon_source") or "") != "paper_trading_style_default"
        if explicit_horizon and horizon and compatible_horizons and str(horizon).lower() not in compatible_horizons:
            invalid_reasons.append("incompatible_report_horizon_for_trading_style")
        if not bool(style_evidence.get("frozen_evidence_valid")):
            invalid_reasons.append(str(style_evidence.get("freeze_error") or "missing_frozen_style_evidence"))
        descriptive.append({
            "source": "paper_trading.entry_legs",
            "schema_version": ENTRY_LEG_SCHEMA_VERSION,
            "text": "versioned entry legs execution contract",
            "entry_legs": entry_legs,
            "reason": "execution_contract",
        })
        descriptive.append({
            "source": "paper_trading.trading_style",
            "schema_version": TRADING_STYLE_POLICY_VERSION,
            "text": f"style={effective_style}",
            "trading_style": effective_style,
            "settings": effective_style_settings,
            "evidence": style_evidence,
            "holding_policy": horizon_holding_policy,
            "reason": "execution_policy",
        })
        descriptive.append({
            "source": "paper_trading.strategy_setup",
            "schema_version": PAPER_STRATEGY_POLICY_VERSION,
            "text": f"setup={setup_type or 'unsupported'}",
            "setup_type": setup_type,
            "setup": setup_evidence,
            "factor_policy": setup_policy,
            "geometry_guard": geometry_guard,
            "reason": (
                "execution_policy"
                if setup_type and bool(geometry_guard.get("passed"))
                else "fail_closed"
            ),
        })
        if explicit_exit_contract:
            descriptive.append({
                "source": "paper_trading.exit_legs",
                "schema_version": EXIT_LEG_SCHEMA_VERSION,
                "text": "versioned staged exit execution contract",
                "exit_legs": exit_legs,
                "weighted_target_price": weighted_target,
                "weighted_risk_reward": risk_reward,
                "source_target_price": target,
                "first_take_profit": exit_targets.get("first_take_profit"),
                "final_target": exit_targets.get("final_target"),
                "risk_target_price": risk_target,
                "reason": "execution_contract",
            })
        same_session_reanalysis = self._is_verified_same_session_reanalysis(
            signal=signal,
            raw_report=raw_report,
            market=market or "unknown",
        )
        report_timezone = self._market_timezone(market or "unknown")
        report_market_datetime = (
            report_created_at.astimezone(report_timezone)
            if report_created_at.tzinfo
            else report_created_at.replace(tzinfo=report_timezone)
        )
        report_session_date = self._signal_session_date(signal) or report_market_datetime.date()
        plan_valid_for_date = (
            report_session_date
            if same_session_reanalysis
            else self._next_trading_session(market or "unknown", report_session_date)
        )
        next_check_obligation, next_check_reason = self._build_next_check_obligation(
            raw_report=raw_report,
            market=market or "unknown",
            valid_for_date=plan_valid_for_date,
            report_created_at=report_created_at,
            style_settings=effective_style_settings,
        )
        if next_check_obligation is not None:
            descriptive.append({
                "source": "paper_trading.next_check_obligation",
                "schema_version": "next-check-v1",
                "text": f"next_check_time={next_check_obligation.get('raw')}",
                "obligation": next_check_obligation,
                "reason": "once_only_reanalysis",
            })
        elif next_check_reason:
            descriptive.append({
                "source": "paper_trading.next_check_obligation",
                "schema_version": "next-check-v1",
                "text": next_check_reason,
                "reason": "descriptive_only",
            })

        market_regime_snapshot = self._market_regime_snapshot_for_plan(
            market=market or "unknown",
            session_date=plan_valid_for_date,
        )
        initial_executable = not invalid_reasons
        strategy_transition = self._evaluate_strategy_thesis_transition(
            report=report,
            signal=signal,
            source_model=model,
            market=market or "unknown",
            stock_code=stock_code,
            stock_name=(signal.stock_name if signal is not None else report.name),
            trading_style=effective_style,
            raw_action=action,
            raw_report=raw_report,
            entry_low=risk_entry_low,
            entry_high=risk_entry_high,
            stop_loss=stop,
            target_price=target,
            initial_executable=initial_executable,
            invalid_reasons=invalid_reasons,
            report_created_at=report_created_at,
        )
        descriptive.append({
            "source": "paper_trading.strategy_thesis",
            "schema_version": STRATEGY_THESIS_POLICY_VERSION,
            "text": (
                f"{strategy_transition.get('result')}:{strategy_transition.get('reason')}"
            ),
            "reason": strategy_transition.get("reason"),
            "transition": strategy_transition,
        })
        if initial_executable and not bool(strategy_transition.get("plan_authorized")):
            invalid_reasons.append("strategy_thesis_not_accepted")
        executable = not invalid_reasons and bool(strategy_transition.get("plan_authorized", initial_executable))
        if invalid_reasons:
            descriptive.append({
                "source": "plan_validator",
                "text": ";".join(sorted(set(invalid_reasons))),
                "reason": "fail_closed",
            })
        non_executable_status = (
            "superseded"
            if strategy_transition.get("reason") == "stale_report_older_than_strategy_observation"
            else "non_executable"
        )
        fields = dict(
            plan_key=plan_key,
            plan_version=PLAN_VERSION,
            source_report_id=report.id,
            decision_signal_id=signal.id if signal is not None else None,
            source_model=model or "unknown",
            market=market or "unknown",
            stock_code=stock_code,
            stock_name=(signal.stock_name if signal is not None else report.name),
            report_date=report_session_date,
            report_created_at=report_created_at,
            valid_for_date=plan_valid_for_date,
            action=action or "unknown",
            executable=executable,
            status="active" if executable else non_executable_status,
            rules_json=self._json_dumps(rules),
            descriptive_conditions_json=self._json_dumps(descriptive),
            stop_loss=stop,
            target_price=target,
            max_holding_sessions=int(horizon_holding_policy["applied_sessions"]),
            trading_style=effective_style,
            trading_style_policy_version=TRADING_STYLE_POLICY_VERSION,
            trading_style_settings_json=self._json_dumps(style_evidence),
            market_regime_context_id=market_regime_snapshot.get("id"),
            market_regime=market_regime_snapshot.get("regime"),
            market_regime_policy_version=market_regime_snapshot.get("policy_version"),
        )
        # A report can be produced by a manual request, the post-close schedule,
        # opening confirmation, or a later intraday reanalysis.  All of those
        # origins share the same report/signal contract, so update already-open
        # lots as soon as the holding-aware report is persisted.  Waiting for a
        # future quote poll made post-close/manual revisions invisible and left
        # the UI showing the source-plan values until the next trading session.
        risk_refresh_result: Optional[Dict[str, Any]] = None
        if bool(strategy_transition.get("risk_revision_authorized")):
            risk_refresh_result = self._refresh_open_position_risk_from_report(
                report=report,
                signal=signal,
                source_model=model,
                market=market or "unknown",
                stock_code=stock_code,
                strategy_transition=strategy_transition,
            )
            descriptive.append({
                "source": "paper_trading.strategy_risk_refresh",
                "schema_version": STRATEGY_THESIS_POLICY_VERSION,
                "text": str(risk_refresh_result.get("reason") or risk_refresh_result.get("status")),
                "reason": risk_refresh_result.get("reason") or risk_refresh_result.get("status"),
                "result": risk_refresh_result,
            })
            fields["descriptive_conditions_json"] = self._json_dumps(descriptive)
        if existing is not None:
            if contract_correction_allowed and "stop_not_below_entry" in set(invalid_reasons):
                self._ensure_invalid_plan_contract_reanalysis(
                    existing,
                    trigger_type="plan_contract_invalid_stop",
                    session_date=plan_valid_for_date,
                    reasons=invalid_reasons,
                )
            if contract_correction_allowed and set(invalid_reasons).intersection({
                "stop_within_atr_noise",
                "stop_within_minimum_price_noise",
                "entry_structure_too_far_from_support",
            }):
                self._ensure_invalid_plan_contract_reanalysis(
                    existing,
                    trigger_type="plan_contract_invalid_stop",
                    session_date=plan_valid_for_date,
                    reasons=invalid_reasons,
                )
            if contract_correction_allowed and "risk_reward_below_policy" in set(invalid_reasons):
                self._ensure_invalid_plan_contract_reanalysis(
                    existing,
                    trigger_type="plan_contract_low_payoff",
                    session_date=plan_valid_for_date,
                    reasons=invalid_reasons,
                )
            desired_status = "active" if executable else "non_executable"
            needs_revalidation = (
                existing.status in {"active", "non_executable"}
                and (
                    bool(existing.executable) != bool(executable)
                    or existing.status != desired_status
                )
            )
            if needs_revalidation:
                with self.db.session_scope() as session:
                    row = session.get(PaperExecutionPlan, existing.id)
                    has_fill = session.execute(
                        select(PaperPosition.id)
                        .where(PaperPosition.plan_id == existing.id)
                        .limit(1)
                    ).scalar_one_or_none() is not None
                    has_trade_event = session.execute(
                        select(PaperTradeEvent.id)
                        .where(PaperTradeEvent.plan_id == existing.id)
                        .limit(1)
                    ).scalar_one_or_none() is not None
                    if row is not None and not has_fill and not has_trade_event:
                        # Revalidation repairs the contract for the same source
                        # report; it must not reinterpret when that report was
                        # allowed to become authoritative.  In particular, a
                        # same-session manual plan created during continuous
                        # trading must not be rolled to tomorrow merely because
                        # its contract is revalidated during lunch or post-close.
                        fields["report_date"] = row.report_date
                        fields["valid_for_date"] = row.valid_for_date
                        fields["market_regime_context_id"] = row.market_regime_context_id
                        fields["market_regime"] = row.market_regime
                        fields["market_regime_policy_version"] = row.market_regime_policy_version
                        audit_conditions, _ = self._json_list(fields["descriptive_conditions_json"])
                        audit_conditions.append({
                            "source": "plan_contract_revalidation",
                            "reason": "contract_revalidation",
                            "text": (
                                f"status:{row.status}->{desired_status};"
                                f"executable:{bool(row.executable)}->{bool(executable)}"
                            ),
                            "previous_status": row.status,
                            "new_status": desired_status,
                            "previous_executable": bool(row.executable),
                            "new_executable": bool(executable),
                            "revalidated_at": utc_naive_now().isoformat(),
                        })
                        fields["descriptive_conditions_json"] = self._json_dumps(audit_conditions)
                        for key, value in fields.items():
                            if key != "plan_key":
                                setattr(row, key, value)
                        row.updated_at = utc_naive_now()
                        self._repair_plan_lifecycle_in_session(
                            session,
                            source_model=row.source_model,
                            market=row.market,
                            stock_code=row.stock_code,
                            current_session_date=None,
                        )
                        item = self._serialize_plan_runtime(row, positions=[], requests=[])
                        return {"item": item, "created": False, "revalidated": True}
            if contract_correction_allowed and set(invalid_reasons).intersection({
                "stop_not_below_entry",
                "risk_reward_below_policy",
                "stop_within_atr_noise",
                "stop_within_minimum_price_noise",
                "entry_structure_too_far_from_support",
            }):
                refreshed = self.repo.get_plan(existing.id)
                if refreshed is not None:
                    return {
                        "item": self._serialize_plans_with_runtime([refreshed])[0],
                        "created": False,
                        "revalidated": False,
                    }
            return {
                "item": self._serialize_plans_with_runtime([existing])[0],
                "created": False,
                "revalidated": False,
            }
        created_plan_id: Optional[int] = None
        try:
            with self.db.session_scope() as session:
                row = PaperExecutionPlan(**fields)
                session.add(row)
                session.flush()
                created_plan_id = int(row.id)
                self._complete_matching_reanalysis_request_in_session(
                    session,
                    replacement_plan=row,
                    raw_report=raw_report,
                )
                self._repair_plan_lifecycle_in_session(
                    session,
                    source_model=row.source_model,
                    market=row.market,
                    stock_code=row.stock_code,
                    current_session_date=None,
                )
                item = self._serialize_plan_runtime(row, positions=[], requests=[])
        except IntegrityError:
            existing = self.repo.get_plan_by_key(plan_key)
            if existing is None:
                raise
            return {"item": self._serialize_plans_with_runtime([existing])[0], "created": False}
        if created_plan_id is not None and contract_correction_allowed and set(invalid_reasons).intersection({
            "stop_not_below_entry",
            "risk_reward_below_policy",
            "stop_within_atr_noise",
            "stop_within_minimum_price_noise",
            "entry_structure_too_far_from_support",
        }):
            created_plan = self.repo.get_plan(created_plan_id)
            if created_plan is not None:
                trigger_type = (
                    "plan_contract_invalid_stop"
                    if "stop_not_below_entry" in set(invalid_reasons)
                    or "stop_within_atr_noise" in set(invalid_reasons)
                    or "stop_within_minimum_price_noise" in set(invalid_reasons)
                    else "plan_contract_low_payoff"
                )
                self._ensure_invalid_plan_contract_reanalysis(
                    created_plan,
                    trigger_type=trigger_type,
                    session_date=plan_valid_for_date,
                    reasons=invalid_reasons,
                )
                refreshed = self.repo.get_plan(created_plan_id)
                if refreshed is not None:
                    item = self._serialize_plans_with_runtime([refreshed])[0]
        return {"item": item, "created": True}

    def _ensure_invalid_plan_contract_reanalysis(
        self,
        plan,
        *,
        trigger_type: str,
        session_date: date,
        reasons: Sequence[str],
    ) -> Optional[Dict[str, Any]]:
        """Create one auditable same-model correction request for unsafe plans."""

        reason_list = sorted({str(reason) for reason in reasons if reason})
        evidence = {
            "mode": "plan_contract_validation",
            "policy": {
                "fail_closed": True,
                "no_silent_price_adjustment": True,
                "same_model_reanalysis": True,
            },
            "reasons": reason_list,
        }
        return self._ensure_reanalysis_request(
            plan=plan,
            trigger_type=trigger_type,
            session_date=session_date,
            due_at=utc_naive_now(),
            observed_at=utc_naive_now(),
            evidence=evidence,
            initial_status="pending",
            submit=True,
        )

    def _refresh_open_position_risk_from_report(
        self,
        *,
        report: AnalysisHistory,
        signal: Optional[DecisionSignalRecord],
        source_model: str,
        market: str,
        stock_code: str,
        strategy_transition: Optional[Mapping[str, Any]] = None,
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Apply one newer holding-aware report to every matching open lot.

        Entry-plan replacement and held-position risk revision are deliberately
        separate lifecycles.  A non-bullish report may be non-executable as a new
        entry plan while still carrying the authoritative holding stop/target.
        """

        if signal is None:
            return {"status": "skipped", "reason": "missing_decision_signal", "items": []}
        if self.strategy_thesis_enabled:
            transition_authorized = (
                isinstance(strategy_transition, Mapping)
                and bool(strategy_transition.get("risk_revision_authorized"))
            )
            if not transition_authorized:
                transition_authorized = self._strategy_transition_allows_risk_revision(
                    report=report,
                    signal=signal,
                    source_model=source_model,
                    market=market,
                    stock_code=stock_code,
                )
            if not transition_authorized:
                strategy_transition = self._ensure_strategy_transition_for_signal(
                    report=report,
                    signal=signal,
                    source_model=source_model,
                    market=market,
                    stock_code=stock_code,
                    initial_executable=False,
                    invalid_reasons=("risk_refresh_without_entry_authority",),
                )
                transition_authorized = (
                    isinstance(strategy_transition, Mapping)
                    and bool(strategy_transition.get("risk_revision_authorized"))
                )
            if not transition_authorized:
                return {
                    "status": "skipped",
                    "reason": "strategy_thesis_not_accepted_for_risk_revision",
                    "strategy_thesis_transition": dict(strategy_transition) if isinstance(strategy_transition, Mapping) else None,
                    "items": [],
                }
        # Retrying deferred protection uses the current evaluation clock, not
        # the report's old creation timestamp. Otherwise a newly fetched quote
        # would always look like future data and could never release a retry.
        evaluated_at = current_time or utc_naive_now()
        eligibility = self._holding_signal_eligibility(
            signal,
            report=report,
            evaluated_at=self._utc_naive(evaluated_at),
        )
        stop, target = self._risk_revision_values_from_eligibility(eligibility)
        if not eligibility.get("eligible"):
            return {
                "status": "skipped",
                "reason": "holding_signal_ineligible",
                "reasons": list(eligibility.get("reasons") or []),
                "items": [],
            }
        if stop is None and target is None:
            return {"status": "skipped", "reason": "missing_holding_risk_values", "items": []}

        identity = canonical_paper_identity(market, stock_code)
        if identity is None:
            return {"status": "skipped", "reason": "invalid_stock_identity", "items": []}
        canonical_market, canonical_code = identity
        with self._ledger_write_session() as session:
            positions = list(session.execute(
                select(PaperPosition)
                .where(
                    PaperPosition.source_model == source_model,
                    PaperPosition.market == canonical_market,
                    PaperPosition.stock_code == canonical_code,
                    PaperPosition.status == "open",
                )
                .order_by(PaperPosition.opened_at, PaperPosition.opened_on, PaperPosition.id)
            ).scalars().all())
            eligible_positions = [
                position for position in positions
                if self._position_opened_before_signal(position, getattr(signal, "created_at", None))
            ]
            revisions = self._apply_signal_risk_revisions(
                session,
                positions=eligible_positions,
                signal=signal,
                eligibility=eligibility,
                evaluated_at=self._utc_naive(evaluated_at),
                current_price=None,
            )
        return {
            "status": "applied" if any(item.get("result") == "applied" for item in revisions) else "audited",
            "reason": "same_model_newer_holding_report",
            "position_count": len(eligible_positions),
            "items": revisions,
        }

    def reconcile_open_position_risk_from_latest_reports(
        self,
        *,
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Recover eligible report revisions that predate the current process.

        This is intentionally idempotent: the per-position revision key prevents
        a scheduled monitor from applying the same report twice.  Reports that
        did not freeze a holding state remain ineligible instead of being
        retroactively guessed from today's account state.
        """

        now = current_time or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        open_positions = self.repo.list_open_positions_all()
        groups: Dict[Tuple[str, str, str], List[PaperPosition]] = {}
        for position in open_positions:
            groups.setdefault((
                str(position.source_model or ""),
                str(position.market or ""),
                str(position.stock_code or ""),
            ), []).append(position)
        summary: Dict[str, Any] = {
            "groups": len(groups),
            "signals_found": 0,
            "positions_considered": len(open_positions),
            "revisions_applied": 0,
            "revisions_rejected": 0,
            "revisions_noop": 0,
            "skipped": 0,
        }
        for (source_model, market, stock_code), positions in sorted(groups.items()):
            earliest = min(
                positions,
                key=lambda item: (item.opened_at or datetime.max, item.opened_on or date.max, item.id),
            )
            through_date = now.astimezone(self._market_timezone(market)).date()
            signal = self.repo.latest_defensive_signal(
                market=market,
                stock_code=stock_code,
                source_model=source_model,
                after_date=earliest.opened_on,
                through_date=through_date,
                after_time=getattr(earliest, "opened_at", None),
                execution_scope=self.execution_scope,
            )
            if signal is None or getattr(signal, "source_report_id", None) is None:
                summary["skipped"] += 1
                continue
            report = self.repo.get_report(int(signal.source_report_id))
            if report is None:
                summary["skipped"] += 1
                continue
            summary["signals_found"] += 1
            outcome = self._refresh_open_position_risk_from_report(
                report=report,
                signal=signal,
                source_model=source_model,
                market=market,
                stock_code=stock_code,
                current_time=now,
            )
            items = outcome.get("items") if isinstance(outcome, Mapping) else []
            if not isinstance(items, list):
                items = []
            summary["revisions_applied"] += sum(
                1 for item in items
                if isinstance(item, Mapping)
                and not item.get("existing")
                and item.get("result") == "applied"
            )
            summary["revisions_rejected"] += sum(
                1 for item in items
                if isinstance(item, Mapping)
                and not item.get("existing")
                and item.get("result") == "rejected"
            )
            summary["revisions_noop"] += sum(
                1 for item in items
                if isinstance(item, Mapping)
                and not item.get("existing")
                and item.get("result") == "noop"
            )
            if not items:
                summary["skipped"] += 1
        return summary

    def bootstrap_strategy_observations(self) -> Dict[str, Any]:
        """Initialize missing continuity state from latest reports, never trades.

        Existing terminal plan reconciliations are deliberately not reopened.
        A historical report creates an observation with zero confirmations and
        no accepted prices/entry authority; only a new adjudication may trade.
        """
        summary = {"enabled": self.strategy_thesis_enabled, "created": 0, "existing": 0}
        if not self.strategy_thesis_enabled:
            return summary
        with self._ledger_write_session() as session:
            self._lock_strategy_state(session)
            rows = session.execute(
                select(AnalysisHistory, DecisionSignalRecord)
                .outerjoin(DecisionSignalRecord, and_(
                    DecisionSignalRecord.source_report_id == AnalysisHistory.id,
                    DecisionSignalRecord.source_type == "analysis",
                ))
                # Filter before "latest": a newer research report must not
                # mask the latest report that may seed this identity.
                .where(authorized_report_execution_clause(
                    AnalysisHistory.id, execution_scope=self.execution_scope,
                ))
                .order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id), desc(DecisionSignalRecord.id))
            ).all()
            seen = set()
            for report, signal in rows:
                model = str(getattr(report, "model_used", None) or "").strip()
                identity = canonical_paper_identity(
                    get_market_for_stock(normalize_stock_code(report.code)), report.code,
                )
                if not identity or not model:
                    continue
                observation_identity = (model, *identity)
                if observation_identity in seen:
                    continue
                seen.add(observation_identity)
                if signal is not None and canonical_paper_identity(signal.market, signal.stock_code) != identity:
                    # A bad joined signal must neither seed another symbol nor
                    # permit fallback to this report identity's older report.
                    continue
                # Standard pipeline signals may omit source_agent; the joined
                # report is authoritative for model attribution in that case.
                if signal is None or (
                    str(signal.source_agent or "").strip()
                    and str(signal.source_agent).strip() != model
                ):
                    continue
                raw, raw_error = self._json_object(report.raw_result)
                if raw_error or raw.get("success") is False or raw.get("error"):
                    continue
                market, stock_code = identity
                style = self._trading_style_evidence(
                    signal=signal, raw_report=raw, market=market, stock_code=stock_code,
                )["trading_style"]
                key = self._strategy_thesis_key(
                    source_model=model, market=market, stock_code=stock_code, trading_style=style,
                )
                if session.execute(select(PaperStrategyThesis.id).where(
                    PaperStrategyThesis.thesis_key == key,
                )).scalar_one_or_none() is not None:
                    summary["existing"] += 1
                    continue
                holding = self._open_position_count_for_identity_in_session(
                    session, source_model=model, market=market, stock_code=stock_code,
                ) > 0
                action = normalize_decision_action(signal.action) or "unknown"
                state, effective = ("position_monitoring", "hold") if holding else ("watching", "watch")
                evidence = {
                    "bootstrap_only": True, "source_report_id": report.id,
                    "raw_action": action, "confirmation_session_dates": [],
                    "contradiction_session_dates": [], "no_trade_authority": True,
                    "reason": "latest_report_observation_without_historical_replay",
                }
                thesis = PaperStrategyThesis(
                    thesis_key=key, policy_version=STRATEGY_THESIS_POLICY_VERSION,
                    source_model=model, market=market, stock_code=stock_code,
                    stock_name=signal.stock_name, trading_style=style, state=state,
                    effective_action=effective, raw_action=action, action_bucket="neutral",
                    confirmation_count=0, contradiction_count=0,
                    last_report_id=report.id, last_signal_id=signal.id,
                    thesis_text=self._report_strategy_text(raw, report), evidence_json=self._json_dumps(evidence),
                )
                session.add(thesis)
                session.flush()
                session.add(PaperStrategyTransition(
                    transition_key="bootstrap:" + key, thesis_id=thesis.id, thesis_key=key,
                    policy_version=STRATEGY_THESIS_POLICY_VERSION, source_model=model,
                    market=market, stock_code=stock_code, trading_style=style,
                    source_report_id=report.id, decision_signal_id=signal.id, raw_action=action,
                    new_state=state, result="observed", reason="bootstrap_observation_only",
                    plan_authorized=False, risk_revision_authorized=False, freeze_entry_authorized=False,
                    confirmation_count=0, contradiction_count=0, evidence_json=self._json_dumps(evidence),
                ))
                summary["created"] += 1
        return summary

    def reconcile_report_execution_plans(self, *, limit: int = 200) -> Dict[str, Any]:
        """Recover missing current-version plans from persisted reports.

        The initial report-save hook is intentionally best-effort so analysis
        completion is not lost when the paper ledger is temporarily unavailable.
        This recovery pass consumes the durable analysis history and signal
        records later, using the same eligibility checks as normal plan creation.
        """

        summary: Dict[str, Any] = {
            "enabled": self.plan_compensation_enabled,
            "lookback_days": self.plan_compensation_lookback_days,
            "reports_considered": 0,
            "created": 0,
            "existing": 0,
            "non_executable": 0,
            "errors": 0,
            "items": [],
        }
        if not self.plan_compensation_enabled:
            return summary
        summary["strategy_bootstrap"] = self.bootstrap_strategy_observations()
        since = utc_naive_now() - timedelta(days=self.plan_compensation_lookback_days)
        reports = self.repo.list_due_report_plan_reconciliations(
            plan_version=PLAN_VERSION,
            since=since,
            limit=limit,
        )
        for report in reports:
            summary["reports_considered"] += 1
            result = self.reconcile_report_execution_plan(
                int(report.id),
                trigger_source="scheduled_reconciler",
            )
            item = result.get("item") if isinstance(result, Mapping) else {}
            if result.get("created"):
                summary["created"] += 1
            elif result.get("status") == "completed":
                summary["existing"] += 1
            if isinstance(item, Mapping) and not item.get("executable"):
                summary["non_executable"] += 1
            if result.get("status") in {"retry_wait", "failed"}:
                summary["errors"] += 1
            summary["items"].append(result)
        return summary

    def reconcile_report_execution_plan(
        self,
        source_report_id: int,
        *,
        decision_signal_id: Optional[int] = None,
        trigger_source: str = "immediate_post_save",
    ) -> Dict[str, Any]:
        """Durably reconcile one report through DecisionSignal and paper plan."""

        report = self.repo.get_report(int(source_report_id))
        if report is None:
            raise PaperTradingNotFoundError(f"analysis report not found: {source_report_id}")
        claim = self.repo.claim_report_plan_reconciliation(
            report=report,
            plan_version=PLAN_VERSION,
            max_attempts=REPORT_PLAN_RECONCILE_MAX_ATTEMPTS,
        )
        existing_state = self.repo.get_report_plan_reconciliation(
            source_report_id=int(source_report_id),
            plan_version=PLAN_VERSION,
        )
        if claim is None:
            return {
                "report_id": int(source_report_id),
                "status": getattr(existing_state, "status", "skipped"),
                "classification": getattr(existing_state, "classification", None),
                "signal_id": getattr(existing_state, "decision_signal_id", None),
                "plan_id": getattr(existing_state, "paper_plan_id", None),
                "attempt_count": getattr(existing_state, "attempt_count", None),
            }

        def finish(
            *,
            status: str,
            classification: str,
            signal_id: Optional[int] = None,
            plan_id: Optional[int] = None,
            error: Optional[str] = None,
            details: Optional[Mapping[str, Any]] = None,
        ) -> Dict[str, Any]:
            attempts = int(getattr(claim, "attempt_count", 0) or 0)
            terminal_status = status
            next_retry_at = None
            if status == "failed" and attempts < REPORT_PLAN_RECONCILE_MAX_ATTEMPTS:
                terminal_status = "retry_wait"
                next_retry_at = utc_naive_now() + timedelta(minutes=attempts)
            self.repo.finish_report_plan_reconciliation(
                reconciliation_id=int(claim.id),
                status=terminal_status,
                classification=classification,
                decision_signal_id=signal_id,
                paper_plan_id=plan_id,
                next_retry_at=next_retry_at,
                last_error=error,
                details_json=self._json_dumps({
                    "trigger_source": trigger_source,
                    "attempt_count": attempts,
                    **dict(details or {}),
                }),
            )
            return {
                "report_id": int(source_report_id),
                "status": terminal_status,
                "classification": classification,
                "signal_id": signal_id,
                "plan_id": plan_id,
                "attempt_count": attempts,
                "next_retry_at": next_retry_at.isoformat() if next_retry_at else None,
                "error": error,
            }

        raw_report, raw_error = self._json_object(report.raw_result)
        if report.report_type == "market_review" or report.code == "MARKET":
            return finish(
                status="terminal",
                classification="market_review_not_stock_plan",
                details={"report_type": report.report_type, "code": report.code},
            )
        authorization = self.report_execution_authorization(int(report.id))
        if not authorization["authorized"]:
            # Terminal before signal backfill: research reports never reach
            # DecisionSignal, strategy confirmation, or plan state.
            blocked = finish(
                status="terminal",
                classification="execution_not_authorized",
                error=authorization["reason"],
                details={
                    "execution_block_reason": authorization["reason"],
                    "execution_scope": authorization["execution_scope"],
                    "execution_intent": authorization["execution_intent"],
                    "report_execution_scope": authorization["report_execution_scope"],
                },
            )
            blocked["reason"] = authorization["reason"]
            return blocked
        if raw_error or raw_report.get("success") is False or raw_report.get("error_message"):
            return finish(
                status="terminal",
                classification="failed_analysis_not_stock_plan",
                error=raw_error or raw_report.get("error_message"),
            )

        signal = (
            self.repo.get_signal(decision_signal_id)
            if decision_signal_id is not None
            else self.repo.latest_report_signal(int(report.id))
        )
        if signal is None:
            try:
                from src.services.decision_signal_service import DecisionSignalService

                DecisionSignalService(db_manager=self.db)._backfill_analysis_signal_from_history(int(report.id))
                signal = self.repo.latest_report_signal(int(report.id))
            except Exception as exc:
                return finish(
                    status="failed",
                    classification="decision_signal_backfill_error",
                    error=str(exc),
                )
        if signal is None:
            return finish(
                status="failed",
                classification="missing_decision_signal",
                error="report has no persisted DecisionSignal",
            )
        if str(getattr(signal, "source_agent", "") or "").strip() and str(getattr(report, "model_used", "") or "").strip():
            if str(signal.source_agent).strip() != str(report.model_used).strip():
                return finish(
                    status="terminal",
                    classification="signal_model_mismatch",
                    signal_id=int(signal.id),
                    error="decision signal source_agent does not match report model_used",
                )
        with self.db.get_session() as session:
            represented_position = session.execute(
                select(PaperPosition)
                .where(
                    PaperPosition.source_report_id == int(report.id),
                    PaperPosition.status == "open",
                )
                .order_by(desc(PaperPosition.id))
                .limit(1)
            ).scalar_one_or_none()
            if represented_position is not None:
                represented = finish(
                    status="completed",
                    classification="existing_open_position_already_represents_report",
                    signal_id=int(signal.id),
                    plan_id=represented_position.plan_id,
                    details={"position_id": represented_position.id},
                )
                represented["created"] = False
                represented["item"] = {
                    "id": represented_position.plan_id,
                    "position_id": represented_position.id,
                    "status": "represented_by_open_position",
                }
                return represented
        try:
            result = self.create_plan_for_report(int(report.id), decision_signal_id=int(signal.id))
        except Exception as exc:
            return finish(
                status="failed",
                classification="paper_plan_create_error",
                signal_id=int(signal.id),
                error=str(exc),
            )
        item = result.get("item") if isinstance(result, Mapping) else {}
        plan_id = item.get("id") if isinstance(item, Mapping) else None
        completed = finish(
            status="completed",
            classification="stock_report_plan_reconciled",
            signal_id=int(signal.id),
            plan_id=int(plan_id) if isinstance(plan_id, int) else None,
            details={
                "created": bool(result.get("created")),
                "executable": item.get("executable") if isinstance(item, Mapping) else None,
                "plan_status": item.get("status") if isinstance(item, Mapping) else None,
            },
        )
        completed["created"] = bool(result.get("created"))
        completed["item"] = item
        return completed

    def _complete_matching_reanalysis_request_in_session(self, session, *, replacement_plan, raw_report: Mapping[str, Any]) -> None:
        if not self._is_same_session_reanalysis(raw_report):
            return
        replacement_signal = (
            session.get(DecisionSignalRecord, int(replacement_plan.decision_signal_id))
            if replacement_plan.decision_signal_id is not None
            else None
        )
        if not self._is_verified_same_session_reanalysis(
            signal=replacement_signal,
            raw_report=raw_report,
            market=replacement_plan.market,
        ):
            return
        source_plan_id = None
        for container_key in ("paper_trading", "auction_reanalysis"):
            container = raw_report.get(container_key) if isinstance(raw_report, Mapping) else None
            if isinstance(container, Mapping) and container.get("source_plan_id") is not None:
                source_plan_id = container.get("source_plan_id")
                break
        if source_plan_id is None:
            return
        request = session.execute(
            select(PaperReanalysisRequest)
            .where(
                PaperReanalysisRequest.source_plan_id == int(source_plan_id),
                PaperReanalysisRequest.source_model == replacement_plan.source_model,
                PaperReanalysisRequest.market == replacement_plan.market,
                PaperReanalysisRequest.stock_code == replacement_plan.stock_code,
                PaperReanalysisRequest.status.in_(["pending", "claimed", "submitted"]),
            )
            .order_by(desc(PaperReanalysisRequest.created_at), desc(PaperReanalysisRequest.id))
            .limit(1)
        ).scalar_one_or_none()
        if request is None:
            return
        request.status = "completed"
        request.completed_at = utc_naive_now()
        request.replacement_report_id = replacement_plan.source_report_id
        request.replacement_plan_id = replacement_plan.id
        request.updated_at = utc_naive_now()
        source_plan = session.get(PaperExecutionPlan, int(source_plan_id))
        if source_plan is not None and source_plan.status == "active":
            source_plan.status = "superseded"
            source_plan.updated_at = utc_naive_now()

    def run_monitor(
        self,
        evaluation_date: Optional[date] = None,
        *,
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Use the latest date proven closed for every active market.

        An explicit date remains fail-closed per market: rows dated after that
        market's latest proven close are marked incomplete before ``run_eod``.
        """

        now = current_time or datetime.now(timezone.utc)
        self._upgrade_legacy_active_plans()
        plan_reconciled = self.reconcile_report_execution_plans()
        risk_reconciled = self.reconcile_open_position_risk_from_latest_reports(current_time=now)
        identities = {
            (plan.market, plan.stock_code)
            for plan in self.repo.list_active_plans()
        }
        positions = self.repo.list_open_positions_all()
        identities.update((position.market, position.stock_code) for position in positions)
        proven_by_market = {
            market: self._latest_proven_complete_date(market, now)
            for market, _ in identities
        }
        groups: Dict[date, set[Tuple[str, str]]] = {}
        for identity in identities:
            eval_date = (
                self._as_date(evaluation_date)
                if evaluation_date is not None
                else proven_by_market[identity[0]]
            )
            groups.setdefault(eval_date, set()).add(identity)

        aggregate: Dict[str, Any] = {
            "evaluation_date": (
                self._as_date(evaluation_date).isoformat()
                if evaluation_date is not None
                else None
            ),
            "evaluation_dates": {
                market: proven.isoformat() for market, proven in proven_by_market.items()
            },
            "plans_evaluated": 0,
            "triggered": 0,
            "positions_opened": 0,
            "positions_closed": 0,
            "positions_reduced": 0,
            "failed_closed": 0,
            "evaluations": [],
            "events": [],
            "report_plans_reconciled": plan_reconciled,
            "risk_revisions_reconciled": risk_reconciled,
        }
        for eval_date, group_identities in sorted(groups.items()):
            bars_by_stock: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
            for market, stock_code in group_identities:
                rows = self.repo.daily_bars(
                    market,
                    stock_code,
                    eval_date,
                    limit=self._analysis_window_sessions_for_identity(market, stock_code),
                )
                proven_date = proven_by_market[market]
                bars_by_stock[(market, stock_code)] = [
                    {
                        "date": row.date,
                        "open": row.open,
                        "high": row.high,
                        "low": row.low,
                        "close": row.close,
                        "volume": (
                            row.to_dict().get("volume")
                            if callable(getattr(row, "to_dict", None))
                            else row.volume
                        ),
                        "is_complete": (
                            row.date <= proven_date
                            and not getattr(row, "_paper_variant_conflict", False)
                        ),
                    }
                    for row in rows
                ]
            result = self.run_eod(
                eval_date,
                bars_by_stock,
                _only_identities=group_identities,
            )
            for key in (
                "plans_evaluated", "triggered", "positions_opened",
                "positions_closed", "positions_reduced", "failed_closed",
            ):
                aggregate[key] += result[key]
            aggregate["evaluations"].extend(result["evaluations"])
            aggregate["events"].extend(result["events"])
        return aggregate

    def run_eod(
        self,
        evaluation_date: date,
        bars_by_stock: Mapping[Any, Sequence[Mapping[str, Any]]],
        *,
        _only_identities: Optional[set[Tuple[str, str]]] = None,
    ) -> Dict[str, Any]:
        """Deterministically evaluate complete EOD bars and mutate only the paper ledger."""

        eval_date = self._as_date(evaluation_date)
        normalized_inputs = self._normalize_bar_inputs(bars_by_stock)
        summary: Dict[str, Any] = {
            "evaluation_date": eval_date.isoformat(),
            "plans_evaluated": 0,
            "triggered": 0,
            "positions_opened": 0,
            "positions_closed": 0,
            "positions_reduced": 0,
            "failed_closed": 0,
            "evaluations": [],
            "events": [],
        }

        summary["open_position_counts_repaired"] = self._reconcile_open_position_counts()
        open_positions = self.repo.list_open_positions_all()
        for position in open_positions:
            if _only_identities is not None and (position.market, position.stock_code) not in _only_identities:
                continue
            self._preflight_defensive_transition_for_position(position, eval_date=eval_date)
            bars = normalized_inputs.get((position.market, position.stock_code), [])
            outcome = self._evaluate_and_close_position(position, eval_date, bars)
            if outcome is None:
                continue
            summary["evaluations"].append(outcome["evaluation"])
            if outcome.get("event"):
                summary["events"].append(outcome["event"])
                if outcome["event"].get("event_type") == "reduce":
                    summary["positions_reduced"] += 1
                else:
                    summary["positions_closed"] += 1
            if outcome["evaluation"]["result"] == "failed_closed":
                summary["failed_closed"] += 1

        self._repair_eod_plan_lifecycle(eval_date, only_identities=_only_identities)
        for plan in self.repo.list_active_plans():
            if _only_identities is not None and (plan.market, plan.stock_code) not in _only_identities:
                continue
            bars = normalized_inputs.get((plan.market, plan.stock_code), [])
            outcome = self._evaluate_and_open_plan(plan, eval_date, bars)
            if outcome is None:
                continue
            summary["plans_evaluated"] += 1
            summary["evaluations"].append(outcome["evaluation"])
            if outcome["evaluation"]["result"] == "triggered":
                summary["triggered"] += 1
            if outcome.get("event"):
                summary["events"].append(outcome["event"])
                summary["positions_opened"] += 1
            if outcome["evaluation"]["result"] == "failed_closed":
                summary["failed_closed"] += 1
        summary["dispatches"] = self.dispatch_pending_notifications()
        return summary

    def _repair_eod_plan_lifecycle(
        self,
        eval_date: date,
        *,
        only_identities: Optional[set[Tuple[str, str]]] = None,
    ) -> int:
        repaired = 0
        with self._ledger_write_session() as session:
            query = (
                select(
                    PaperExecutionPlan.source_model,
                    PaperExecutionPlan.market,
                    PaperExecutionPlan.stock_code,
                )
                .where(
                    PaperExecutionPlan.status == "active",
                    PaperExecutionPlan.executable.is_(True),
                )
                .distinct()
            )
            identities = list(session.execute(query).all())
            for source_model, market, stock_code in identities:
                if only_identities is not None and (market, stock_code) not in only_identities:
                    continue
                repaired += self._repair_plan_lifecycle_in_session(
                    session,
                    source_model=source_model,
                    market=market,
                    stock_code=stock_code,
                    current_session_date=eval_date,
                )
        return repaired

    def list_plans(
        self, *, model: Optional[str] = None, status: Optional[str] = None,
        offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        rows, total = self.repo.list_plans(model=model, status=status, offset=offset, limit=limit)
        return {"items": self._serialize_plans_with_runtime(rows), "total": total}

    def list_operations(
        self, *, model: Optional[str] = None, offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(int(limit), 500))
        with self.db.get_session() as session:
            plan_query = (
                select(PaperExecutionPlan)
                .outerjoin(PaperPosition, PaperPosition.plan_id == PaperExecutionPlan.id)
                .outerjoin(
                    PaperReanalysisRequest,
                    PaperReanalysisRequest.source_plan_id == PaperExecutionPlan.id,
                )
                .where(
                    or_(
                        and_(
                            PaperExecutionPlan.status == "active",
                            PaperExecutionPlan.executable.is_(True),
                        ),
                        PaperPosition.status == "open",
                        PaperReanalysisRequest.status.in_(["pending", "claimed", "submitted"]),
                    )
                )
            )
            if model:
                plan_query = plan_query.where(PaperExecutionPlan.source_model == model)
            rows = list(session.execute(
                plan_query.distinct().order_by(desc(PaperExecutionPlan.created_at), desc(PaperExecutionPlan.id))
            ).scalars().all())
            serialized = self._serialize_plans_with_runtime(rows, session=session)
            operations = [item for item in serialized if item.get("is_operational")]
            total = len(operations)
            return {"items": operations[safe_offset:safe_offset + safe_limit], "total": total}

    def list_positions(
        self, *, model: Optional[str] = None, status: Optional[str] = None,
        offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        rows, total = self.repo.list_positions(model=model, status=status, offset=offset, limit=limit)
        if not rows:
            return {"items": [], "total": total}
        position_ids = [int(row.id) for row in rows]
        plan_ids = list({int(row.plan_id) for row in rows})
        with self.db.get_session() as session:
            plans = {
                int(plan.id): plan
                for plan in session.execute(
                    select(PaperExecutionPlan).where(PaperExecutionPlan.id.in_(plan_ids))
                ).scalars().all()
            }
            revision_rows = list(session.execute(
                select(PaperRiskRevision)
                .where(PaperRiskRevision.position_id.in_(position_ids))
                .order_by(
                    PaperRiskRevision.position_id,
                    desc(PaperRiskRevision.revision_version),
                    desc(PaperRiskRevision.id),
                )
            ).scalars().all())
            regime_ids = {
                int(position.market_regime_context_id)
                for position in rows
                if getattr(position, "market_regime_context_id", None) is not None
            }
            market_regimes = {
                int(row.id): row
                for row in session.execute(
                    select(DailyMarketRegime).where(DailyMarketRegime.id.in_(regime_ids))
                ).scalars().all()
            } if regime_ids else {}
            source_kinds = self._report_source_kinds_in_session(
                session, [getattr(row, "source_report_id", None) for row in rows],
            )
        latest_revision_by_position: Dict[int, PaperRiskRevision] = {}
        for revision in revision_rows:
            latest_revision_by_position.setdefault(int(revision.position_id), revision)
        held_sessions = self._held_sessions_by_position(rows)
        items = []
        for row in rows:
            item = self._serialize_position(
                row,
                source_plan=plans.get(int(row.plan_id)),
                latest_risk_revision=latest_revision_by_position.get(int(row.id)),
                market_regime_context=market_regimes.get(int(row.market_regime_context_id))
                if getattr(row, "market_regime_context_id", None) is not None
                else None,
            )
            item["source_kind"] = self._source_kind_for_report(source_kinds, row.source_report_id)
            item["held_sessions"] = held_sessions.get(int(row.id))
            items.append(item)
        return {"items": items, "total": total}

    def list_holdings(
        self, *, model: Optional[str] = None, offset: int = 0, limit: int = 100,
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Aggregate open lots server-side by model/market/symbol/currency."""

        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(int(limit), 500))
        now_aware = current_time or datetime.now(timezone.utc)
        now = self._utc_naive(now_aware)
        availability_dates: Dict[str, date] = {}
        with self.db.get_session() as session:
            query = select(PaperPosition).where(PaperPosition.status == "open")
            if model:
                query = query.where(PaperPosition.source_model == model)
            positions = list(session.execute(
                query.order_by(
                    PaperPosition.source_model,
                    PaperPosition.market,
                    PaperPosition.stock_code,
                    PaperPosition.currency,
                    PaperPosition.opened_on,
                    PaperPosition.id,
                )
            ).scalars().all())
            groups: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
            for position in positions:
                currency = position.currency or self._currency_for_market(position.market)
                key = (position.source_model, position.market, position.stock_code, currency)
                item = groups.setdefault(key, {
                    "source_model": position.source_model,
                    "market": position.market,
                    "stock_code": position.stock_code,
                    "stock_name": position.stock_name,
                    "currency": currency,
                    "total_quantity": 0.0,
                    "available_quantity": 0.0,
                    "frozen_quantity": 0.0,
                    "total_cost": 0.0,
                    "lot_count": 0,
                    "lot_ids": [],
                    "latest_price": None,
                    "observed_at": None,
                    "market_value": None,
                    "mark_source": None,
                    "mark_stale": None,
                    "valuation_complete": False,
                    "symbol_concentration_pct": None,
                    "unrealized_pnl": None,
                    "unrealized_return_pct": None,
                })
                quantity = float(position.quantity or 0.0)
                total_cost = self._position_allocated_capital(position)
                available = getattr(position, "available_quantity", None)
                frozen = getattr(position, "frozen_quantity", None)
                if available is None:
                    available = 0.0 if getattr(position, "pending_exit_reason", None) else quantity
                if frozen is None:
                    frozen = quantity if getattr(position, "pending_exit_reason", None) else 0.0
                market_key = str(position.market or "").lower()
                availability_date = availability_dates.get(market_key)
                if availability_date is None:
                    availability_date = self._availability_date_for_market(position.market, now_aware)
                    availability_dates[market_key] = availability_date
                if not getattr(position, "pending_exit_reason", None):
                    if self._can_close_position_on_date(position, availability_date):
                        available = quantity
                        frozen = 0.0
                    else:
                        if getattr(position, "opened_on", None) == self._market_local_date_for_time(position.market, now_aware):
                            available = 0.0
                            frozen = quantity
                        else:
                            persisted_available = float(available or 0.0)
                            available = min(quantity, max(0.0, persisted_available))
                            frozen = max(0.0, quantity - available)
                item["stock_name"] = item["stock_name"] or position.stock_name
                item["total_quantity"] += quantity
                item["available_quantity"] += float(available or 0.0)
                item["frozen_quantity"] += float(frozen or 0.0)
                item["total_cost"] += total_cost
                item["lot_count"] += 1
                item["lot_ids"].append(position.id)
            for key, item in groups.items():
                source_model, market, stock_code, _currency = key
                total_quantity = item["total_quantity"]
                item["weighted_average_price"] = (
                    item["total_cost"] / total_quantity if total_quantity > 0 else None
                )
                account = session.execute(
                    select(PaperCapitalAccount)
                    .where(
                        PaperCapitalAccount.source_model == source_model,
                        PaperCapitalAccount.market == market,
                        PaperCapitalAccount.currency == _currency,
                    )
                    .limit(1)
                ).scalar_one_or_none()
                book_equity = self._book_equity(account) if account is not None else None
                item["symbol_concentration_pct"] = (
                    item["total_cost"] / book_equity * 100.0
                    if book_equity and book_equity > 0 else None
                )
                mark = self._valuation_mark_for_identity(
                    session,
                    source_model=source_model,
                    market=market,
                    stock_code=stock_code,
                    current_time=now_aware,
                )
                if mark is not None:
                    latest_price = float(mark["price"])
                    market_value = total_quantity * latest_price
                    item["latest_price"] = latest_price
                    item["observed_at"] = mark["observed_at"].isoformat() if mark.get("observed_at") else None
                    item["market_value"] = market_value
                    item["mark_source"] = mark.get("source")
                    item["mark_stale"] = bool(mark.get("stale"))
                    item["valuation_complete"] = True
                    item["unrealized_pnl"] = market_value - item["total_cost"]
                    item["unrealized_return_pct"] = (
                        (market_value / item["total_cost"] - 1.0) * 100.0
                        if item["total_cost"] > 0 else None
                    )
            sorted_items = list(groups.values())
            sorted_items.sort(key=lambda row: (
                row["source_model"],
                row["market"],
                row["stock_code"],
                row["currency"],
            ))
            total = len(sorted_items)
            return {"total": total, "items": sorted_items[safe_offset:safe_offset + safe_limit]}

    def list_evaluations(
        self, *, model: Optional[str] = None, offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        rows, total = self.repo.list_evaluations(model=model, offset=offset, limit=limit)
        return {
            "items": [self._serialize_evaluation(evaluation, plan=plan) for evaluation, plan in rows],
            "total": total,
        }

    def list_observations(
        self, *, model: Optional[str] = None, offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        rows, total = self.repo.list_observations(model=model, offset=offset, limit=limit)
        return {"items": [self._serialize_observation(row) for row in rows], "total": total}

    def list_auction_observations(
        self, *, model: Optional[str] = None, offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        """List opening-confirmation observations plus historical auction audit rows.

        The endpoint name remains stable for API compatibility.  New rows use
        ``opening_confirmation`` evidence; legacy ``preopen_auction`` rows stay
        visible as historical audit evidence and are never relabelled.
        """

        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(int(limit), 500))
        conditions = [
            or_(
                PaperPlanObservation.evidence_json.like('%"mode": "preopen_auction"%'),
                PaperPlanObservation.evidence_json.like('%"mode":"preopen_auction"%'),
                PaperPlanObservation.evidence_json.like('%"mode": "opening_confirmation"%'),
                PaperPlanObservation.evidence_json.like('%"mode":"opening_confirmation"%'),
            )
        ]
        if model:
            conditions.append(PaperPlanObservation.source_model == model)
        clause = and_(*conditions)
        with self.db.get_session() as session:
            total = session.execute(select(func.count(PaperPlanObservation.id)).where(clause)).scalar() or 0
            rows = list(session.execute(
                select(PaperPlanObservation)
                .where(clause)
                .order_by(desc(PaperPlanObservation.observed_at), desc(PaperPlanObservation.id))
                .offset(safe_offset)
                .limit(safe_limit)
            ).scalars().all())
        return {"items": [self._serialize_observation(row) for row in rows], "total": int(total)}

    def list_current_account_risk_locks(
        self, *, model: Optional[str] = None, account_id: Optional[int] = None,
        offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        """Return the latest risk-lock/permission row for each account."""

        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(int(limit), 500))
        conditions = []
        if model:
            conditions.append(PaperAccountRiskSnapshot.source_model == model)
        if account_id is not None:
            conditions.append(PaperAccountRiskSnapshot.capital_account_id == int(account_id))
        clause = and_(*conditions) if conditions else True
        latest_by_account: Dict[int, PaperAccountRiskSnapshot] = {}
        with self.db.get_session() as session:
            rows = list(session.execute(
                select(PaperAccountRiskSnapshot)
                .where(clause)
                .order_by(
                    PaperAccountRiskSnapshot.capital_account_id,
                    desc(PaperAccountRiskSnapshot.observed_at),
                    desc(PaperAccountRiskSnapshot.id),
                )
            ).scalars().all())
            for row in rows:
                account_key = int(row.capital_account_id)
                if account_key not in latest_by_account:
                    latest_by_account[account_key] = row
            current_rows = sorted(
                latest_by_account.values(),
                key=lambda item: (item.observed_at or datetime.min, item.id or 0),
                reverse=True,
            )
            total = len(current_rows)
            items = [self._serialize_current_account_risk_lock(row) for row in current_rows[safe_offset:safe_offset + safe_limit]]
        return {"items": items, "total": int(total)}

    def list_events(
        self, *, model: Optional[str] = None, offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        rows, total = self.repo.list_events(model=model, offset=offset, limit=limit)
        dispatches = self.repo.list_dispatches_for_event_ids([row.id for row in rows])
        dispatch_by_event_id: Dict[int, PaperNotificationDispatch] = {}
        for dispatch in dispatches:
            if dispatch.event_id not in dispatch_by_event_id:
                dispatch_by_event_id[dispatch.event_id] = dispatch
        return {
            "items": [
                self._serialize_event(row, notification_dispatch=dispatch_by_event_id.get(row.id))
                for row in rows
            ],
            "total": total,
        }

    def list_resolutions(
        self, *, model: Optional[str] = None, offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(int(limit), 500))
        with self.db.get_session() as session:
            clause = PaperResolution.source_model == model if model else True
            total = session.execute(select(func.count(PaperResolution.id)).where(clause)).scalar() or 0
            rows = list(session.execute(
                select(PaperResolution)
                .where(clause)
                .order_by(desc(PaperResolution.created_at), desc(PaperResolution.id))
                .offset(safe_offset)
                .limit(safe_limit)
            ).scalars().all())
            return {"items": [self._serialize_resolution(row) for row in rows], "total": int(total)}

    def list_risk_revisions(
        self, *, model: Optional[str] = None, offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(int(limit), 500))
        with self.db.get_session() as session:
            clause = PaperRiskRevision.source_model == model if model else True
            total = session.execute(select(func.count(PaperRiskRevision.id)).where(clause)).scalar() or 0
            rows = list(session.execute(
                select(PaperRiskRevision)
                .where(clause)
                .order_by(desc(PaperRiskRevision.created_at), desc(PaperRiskRevision.id))
                .offset(safe_offset)
                .limit(safe_limit)
            ).scalars().all())
            return {"items": [self._serialize_risk_revision(row) for row in rows], "total": int(total)}

    def list_reanalysis_requests(
        self, *, model: Optional[str] = None, status: Optional[str] = None,
        offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(int(limit), 500))
        with self.db.get_session() as session:
            conditions = []
            if model:
                conditions.append(PaperReanalysisRequest.source_model == model)
            if status:
                conditions.append(PaperReanalysisRequest.status == status)
            clause = and_(*conditions) if conditions else True
            total = session.execute(select(func.count(PaperReanalysisRequest.id)).where(clause)).scalar() or 0
            rows = list(session.execute(
                select(PaperReanalysisRequest)
                .where(clause)
                .order_by(desc(PaperReanalysisRequest.created_at), desc(PaperReanalysisRequest.id))
                .offset(safe_offset)
                .limit(safe_limit)
            ).scalars().all())
            return {"items": [self._serialize_reanalysis_request(row) for row in rows], "total": int(total)}

    def list_report_plan_reconciliations(
        self,
        *,
        model: Optional[str] = None,
        status: Optional[str] = None,
        plan_version: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Dict[str, Any]:
        rows, total = self.repo.list_report_plan_reconciliations(
            plan_version=plan_version or PLAN_VERSION,
            model=model,
            status=status,
            offset=offset,
            limit=limit,
        )
        report_ids = [int(row.source_report_id) for row in rows if row.source_report_id is not None]
        reports_by_id: Dict[int, AnalysisHistory] = {}
        if report_ids:
            with self.db.get_session() as session:
                reports_by_id = {
                    int(report.id): report
                    for report in session.execute(
                        select(AnalysisHistory).where(AnalysisHistory.id.in_(report_ids))
                    ).scalars().all()
                }
        return {
            "items": [
                self._serialize_report_plan_reconciliation(
                    row,
                    report=reports_by_id.get(int(row.source_report_id)),
                )
                for row in rows
            ],
            "total": total,
        }

    def list_account_risk_snapshots(
        self, *, model: Optional[str] = None, account_id: Optional[int] = None,
        offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(int(limit), 500))
        with self.db.get_session() as session:
            conditions = []
            if model:
                conditions.append(PaperAccountRiskSnapshot.source_model == model)
            if account_id is not None:
                conditions.append(PaperAccountRiskSnapshot.capital_account_id == int(account_id))
            clause = and_(*conditions) if conditions else True
            total = session.execute(select(func.count(PaperAccountRiskSnapshot.id)).where(clause)).scalar() or 0
            rows = list(session.execute(
                select(PaperAccountRiskSnapshot)
                .where(clause)
                .order_by(desc(PaperAccountRiskSnapshot.observed_at), desc(PaperAccountRiskSnapshot.id))
                .offset(safe_offset)
                .limit(safe_limit)
            ).scalars().all())
            return {"items": [self._serialize_account_risk_snapshot(row) for row in rows], "total": int(total)}

    def list_account_risk_events(
        self, *, model: Optional[str] = None, account_id: Optional[int] = None,
        offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(int(limit), 500))
        with self.db.get_session() as session:
            conditions = []
            if model:
                conditions.append(PaperAccountRiskEvent.source_model == model)
            if account_id is not None:
                conditions.append(PaperAccountRiskEvent.capital_account_id == int(account_id))
            clause = and_(*conditions) if conditions else True
            total = session.execute(select(func.count(PaperAccountRiskEvent.id)).where(clause)).scalar() or 0
            rows = list(session.execute(
                select(PaperAccountRiskEvent)
                .where(clause)
                .order_by(desc(PaperAccountRiskEvent.created_at), desc(PaperAccountRiskEvent.id))
                .offset(safe_offset)
                .limit(safe_limit)
            ).scalars().all())
            return {"items": [self._serialize_account_risk_event(row) for row in rows], "total": int(total)}

    def set_account_manual_hold(
        self, *, account_id: int, reason: str, idempotency_key: str,
    ) -> Dict[str, Any]:
        if not str(reason or "").strip():
            raise ValueError("reason is required")
        with self._ledger_write_session() as session:
            account = self._get_account_for_update_by_id(session, int(account_id))
            snapshot = self._compute_account_risk_snapshot_in_session(
                session,
                account=account,
                current_time=datetime.now(timezone.utc),
                bucket_started_at=datetime.now(timezone.utc),
                persist=True,
            )
            event, created = self._record_account_risk_event_in_session(
                session,
                account=account,
                snapshot=snapshot,
                event_type="manual_hold",
                old_state=snapshot.active_state,
                new_state="manual_hold",
                active_reasons=["manual_hold"],
                metric="manual_hold",
                threshold=None,
                source="api",
                recovery_condition={"manual_resume_required": True},
                evidence={"reason": reason},
                idempotency_key=idempotency_key,
            )
            return {"item": self._serialize_account_risk_event(event), "created": created}

    def resume_account_risk(
        self, *, account_id: int, reason: str, idempotency_key: str,
    ) -> Dict[str, Any]:
        if not str(reason or "").strip():
            raise ValueError("reason is required")
        with self._ledger_write_session() as session:
            account = self._get_account_for_update_by_id(session, int(account_id))
            snapshot = self._compute_account_risk_snapshot_in_session(
                session,
                account=account,
                current_time=datetime.now(timezone.utc),
                bucket_started_at=datetime.now(timezone.utc),
                persist=True,
            )
            hard_reasons = set(self._hard_risk_reasons(snapshot))
            if hard_reasons:
                raise PaperTradingConflictError(
                    "manual resume blocked by unrecovered hard risk: " + ",".join(sorted(hard_reasons))
                )
            event, created = self._record_account_risk_event_in_session(
                session,
                account=account,
                snapshot=snapshot,
                event_type="manual_resume",
                old_state=snapshot.active_state,
                new_state="normal",
                active_reasons=[],
                metric="manual_resume",
                threshold=None,
                source="api",
                recovery_condition={"hard_metrics_recovered": True},
                evidence={"reason": reason},
                idempotency_key=idempotency_key,
            )
            return {"item": self._serialize_account_risk_event(event), "created": created}

    def list_accounts(
        self, *, model: Optional[str] = None, market: Optional[str] = None,
        offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        rows, total = self.repo.list_accounts(model=model, market=market, offset=offset, limit=limit)
        current_time = datetime.now(timezone.utc)
        with self.db.get_session() as session:
            return {
                "items": [
                    self._serialize_account(
                        row,
                        valuation=self._account_valuation_snapshot(
                            session,
                            row,
                            current_time=current_time,
                        ),
                    )
                    for row in rows
                ],
                "total": total,
            }

    def list_adjustments(
        self, *, model: Optional[str] = None, account_id: Optional[int] = None,
        adjustment_type: Optional[str] = None, offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        rows, total = self.repo.list_adjustments(
            model=model,
            account_id=account_id,
            adjustment_type=adjustment_type,
            offset=offset,
            limit=limit,
        )
        return {"items": [self._serialize_adjustment(row) for row in rows], "total": total}

    def analysis_context_for_stock(
        self,
        *,
        source_model: str,
        stock_code: str,
        current_time: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return model-scoped paper holding context for prompt consumption."""

        model = str(source_model or "").strip()
        if not model:
            return None
        effective_current_time = current_time or datetime.now(timezone.utc)
        normalized_code = normalize_stock_code(stock_code)
        market = get_market_for_stock(normalized_code)
        identity = canonical_paper_identity(market, normalized_code)
        if identity is None:
            return None
        market, code = identity
        with self.db.get_session() as session:
            strategy_thesis = self._latest_strategy_thesis_for_identity_in_session(
                session,
                source_model=model,
                market=market,
                stock_code=code,
            )
            strategy_pending_transition = self._pending_strategy_transition_for_identity_in_session(
                session,
                source_model=model,
                market=market,
                stock_code=code,
            )
            positions = list(session.execute(
                select(PaperPosition)
                .where(
                    PaperPosition.source_model == model,
                    PaperPosition.market == market,
                    PaperPosition.stock_code == code,
                    PaperPosition.status == "open",
                )
                .order_by(desc(PaperPosition.opened_on), desc(PaperPosition.id))
            ).scalars().all())
            account = self._find_account_for_update(
                session,
                source_model=model,
                market=market,
            )
            if not positions:
                exposure_snapshot = (
                    self._symbol_exposure_snapshot(
                        session,
                        account,
                        source_model=model,
                        market=market,
                        stock_code=code,
                    )
                    if account is not None else None
                )
                risk_capacity_context = self._portfolio_risk_capacity_context(account, exposure_snapshot)
                return {
                    "source": "paper_trading",
                    "source_model": model,
                    "market": market,
                    "stock_code": code,
                    "quantity": 0.0,
                    "holding_state": "empty",
                    "sync_origin": "none",
                    "sync_status": "none",
                    "capital_account_id": account.id if account is not None else None,
                    "cash": float(account.cash or 0.0) if account is not None else None,
                    "book_equity": self._book_equity(account) if account is not None else None,
                    "exposure": float(account.exposure or 0.0) if account is not None else None,
                    "total_exposure_pct": (
                        (float(account.exposure or 0.0) / self._book_equity(account) * 100.0)
                        if account is not None and self._book_equity(account) > 0 else None
                    ),
                    "symbol_concentration_pct": 0.0 if account is not None and self._book_equity(account) > 0 else None,
                    "asset_type": exposure_snapshot.get("asset_type") if exposure_snapshot else None,
                    "asset_type_source": exposure_snapshot.get("asset_type_source") if exposure_snapshot else None,
                    "asset_exposure_cap_pct": exposure_snapshot.get("asset_exposure_cap_pct") if exposure_snapshot else None,
                    "asset_exposure_effective_cap_pct": exposure_snapshot.get("asset_exposure_effective_cap_pct") if exposure_snapshot else None,
                    "asset_exposure_cap_source": exposure_snapshot.get("asset_exposure_cap_source") if exposure_snapshot else None,
                    "asset_exposure_scope": exposure_snapshot.get("asset_exposure_scope") if exposure_snapshot else None,
                    **risk_capacity_context,
                    "currency": account.currency if account is not None else self._currency_for_market(market),
                    "trading_style": self.trading_style,
                    "trading_style_settings": self.trading_style_settings,
                    "strategy_thesis": strategy_thesis,
                    "strategy_pending_transition": strategy_pending_transition,
                    "context_frozen_at": utc_naive_now().isoformat(),
                }
            account = account or session.get(PaperCapitalAccount, int(positions[0].capital_account_id)) if positions[0].capital_account_id else account
            holding_context = self._aggregate_open_holding_context(
                session,
                positions,
                account=account,
                current_time=effective_current_time,
            )
            exposure_snapshot = (
                self._symbol_exposure_snapshot(
                    session,
                    account,
                    source_model=model,
                    market=market,
                    stock_code=code,
                )
                if account is not None else None
            )
            risk_capacity_context = self._portfolio_risk_capacity_context(account, exposure_snapshot)
            return {
                "source": "paper_trading",
                "source_model": model,
                "market": market,
                "stock_code": code,
                **holding_context,
                "holding_state": "holding",
                "capital_account_id": account.id if account is not None else positions[0].capital_account_id,
                "cash": float(account.cash or 0.0) if account is not None else None,
                "book_equity": self._book_equity(account) if account is not None else None,
                "exposure": float(account.exposure or 0.0) if account is not None else None,
                "total_exposure_pct": (
                    (float(account.exposure or 0.0) / self._book_equity(account) * 100.0)
                    if account is not None and self._book_equity(account) > 0 else None
                ),
                "asset_type": exposure_snapshot.get("asset_type") if exposure_snapshot else None,
                "asset_type_source": exposure_snapshot.get("asset_type_source") if exposure_snapshot else None,
                "asset_exposure_cap_pct": exposure_snapshot.get("asset_exposure_cap_pct") if exposure_snapshot else None,
                "asset_exposure_effective_cap_pct": exposure_snapshot.get("asset_exposure_effective_cap_pct") if exposure_snapshot else None,
                "asset_exposure_cap_source": exposure_snapshot.get("asset_exposure_cap_source") if exposure_snapshot else None,
                "asset_exposure_scope": exposure_snapshot.get("asset_exposure_scope") if exposure_snapshot else None,
                **risk_capacity_context,
                "trading_style": self.trading_style,
                "trading_style_settings": self.trading_style_settings,
                "strategy_thesis": strategy_thesis,
                "strategy_pending_transition": strategy_pending_transition,
                "context_frozen_at": utc_naive_now().isoformat(),
            }

    def sync_portfolio_position_to_paper(
        self,
        *,
        portfolio_position_id: int,
        source_models: Sequence[str],
        trade_date: date,
        idempotency_key: str,
    ) -> Dict[str, Any]:
        key_root = str(idempotency_key or "").strip()
        if not key_root:
            raise ValueError("idempotency_key is required")
        models = self._validate_sync_models(source_models)
        if not isinstance(trade_date, date):
            raise ValueError("trade_date is required")

        with self._ledger_write_session() as session:
            portfolio_position = session.get(PortfolioPosition, int(portfolio_position_id))
            if portfolio_position is None:
                raise PaperTradingNotFoundError(f"portfolio position not found: {portfolio_position_id}")
            quantity = self._positive_amount(portfolio_position.quantity, "portfolio quantity")
            price = self._positive_amount(portfolio_position.avg_cost, "portfolio avg_cost")
            identity = canonical_paper_identity(portfolio_position.market, portfolio_position.symbol)
            if identity is None:
                raise ValueError("portfolio position identity is ambiguous or unsupported")
            market, stock_code = identity
            currency = (portfolio_position.currency or self._currency_for_market(market)).upper()
            payload_base = {
                "portfolio_position_id": int(portfolio_position_id),
                "market": market,
                "stock_code": stock_code,
                "quantity": round(quantity, 8),
                "price": round(price, 8),
                "trade_date": trade_date.isoformat(),
                "currency": currency,
                "direction": "portfolio_to_paper",
            }
            prepared: List[Dict[str, Any]] = []
            for model in models:
                payload = {**payload_base, "source_model": model}
                model_key = f"{key_root}:{hashlib.sha256(model.encode('utf-8')).hexdigest()[:12]}"
                payload_hash = self._sync_payload_hash(payload)
                existing = self._sync_link_for_update(session, model_key)
                if existing is not None:
                    if existing.payload_hash != payload_hash:
                        raise PaperTradingConflictError("idempotency key reused with a different sync payload")
                    existing_plan = (
                        session.get(PaperExecutionPlan, int(existing.paper_plan_id))
                        if getattr(existing, "paper_plan_id", None) is not None
                        else None
                    )
                    existing_position = (
                        session.get(PaperPosition, int(existing.paper_position_id))
                        if getattr(existing, "paper_position_id", None) is not None
                        else None
                    )
                    repaired = self._repair_portfolio_import_strategy_fields_in_session(
                        session,
                        plan=existing_plan,
                        position=existing_position,
                        market=market,
                        stock_code=stock_code,
                        now=utc_naive_now(),
                    )
                    if repaired:
                        details, details_error = self._json_object(existing.details_json or "{}")
                        if details_error:
                            details = {}
                        details["strategy_style_repaired"] = True
                        details["strategy_style_repaired_at"] = utc_naive_now().isoformat()
                        details["preserved_trade_date"] = existing.trade_date.isoformat() if existing.trade_date else None
                        details["preserved_paper_event_id"] = existing.paper_event_id
                        existing.details_json = self._json_dumps(details)
                        existing.updated_at = utc_naive_now()
                    prepared.append({"existing": existing})
                    continue
                report = self._latest_report_for_identity_in_session(
                    session,
                    source_model=model,
                    market=market,
                    stock_code=stock_code,
                )
                if report is None:
                    raise PaperTradingNotFoundError(f"missing model report for {model} {market}/{stock_code}")
                open_position = self._open_position_for_identity_in_session(
                    session,
                    source_model=model,
                    market=market,
                    stock_code=stock_code,
                )
                if open_position is not None:
                    raise PaperTradingConflictError(f"paper position already open for {model} {market}/{stock_code}")
                account = self._get_or_create_account(session, source_model=model, market=market)
                allocated = quantity * price
                if float(account.cash or 0.0) + 1e-8 < allocated:
                    raise ValueError(
                        f"insufficient paper cash for {model} {market}/{stock_code}: "
                        f"required={round(allocated, 6)}, cash={round(float(account.cash or 0.0), 6)}"
                    )
                prepared.append({
                    "existing": None,
                    "model": model,
                    "model_key": model_key,
                    "payload_hash": payload_hash,
                    "report": report,
                    "account": account,
                    "allocated": allocated,
                    "payload": payload,
                })

            created = False
            items: List[Dict[str, Any]] = []
            for entry in prepared:
                existing = entry.get("existing")
                if existing is not None:
                    items.append(self._serialize_sync_link(existing))
                    continue
                model = entry["model"]
                report = entry["report"]
                account = entry["account"]
                allocated = float(entry["allocated"])
                now = utc_naive_now()
                style_evidence = self._style_evidence_for_identity(
                    market=market,
                    stock_code=stock_code,
                    source="portfolio_import_identity_resolution",
                )
                import_style = str(style_evidence["trading_style"])
                import_style_settings = dict(style_evidence["settings"])
                import_max_holding_sessions = int(
                    import_style_settings.get("max_holding_sessions")
                    or self.max_holding_sessions
                )
                plan = PaperExecutionPlan(
                    plan_key=(
                        f"portfolio-import:{model}:{market}:{stock_code}:"
                        f"{portfolio_position_id}:{trade_date.isoformat()}"
                    )[:160],
                    plan_version=PLAN_VERSION,
                    source_report_id=report.id,
                    decision_signal_id=None,
                    source_model=model,
                    market=market,
                    stock_code=stock_code,
                    stock_name=getattr(report, "name", None),
                    report_date=(report.created_at or now).date(),
                    report_created_at=report.created_at or now,
                    action="portfolio_import",
                    executable=False,
                    status="triggered",
                    rules_json="[]",
                    descriptive_conditions_json=self._json_dumps([
                        {"source": "portfolio_sync", "reason": "seed_existing_holding"}
                    ]),
                    stop_loss=getattr(report, "stop_loss", None),
                    target_price=getattr(report, "take_profit", None),
                    max_holding_sessions=import_max_holding_sessions,
                    trading_style=import_style,
                    trading_style_policy_version=TRADING_STYLE_POLICY_VERSION,
                    trading_style_settings_json=self._json_dumps(style_evidence),
                )
                session.add(plan)
                session.flush()
                account.cash = float(account.cash or 0.0) - allocated
                account.invested_capital = float(account.invested_capital or 0.0) + allocated
                account.exposure = float(account.exposure or 0.0) + allocated
                account.open_position_count = int(account.open_position_count or 0) + 1
                account.updated_at = now
                available_quantity, frozen_quantity = self._initial_sellability_quantities(
                    market=market,
                    stock_code=stock_code,
                    quantity=quantity,
                )
                position = PaperPosition(
                    plan_id=plan.id,
                    source_report_id=report.id,
                    decision_signal_id=None,
                    source_model=model,
                    market=market,
                    stock_code=stock_code,
                    stock_name=getattr(report, "name", None),
                    status="open",
                    opened_on=trade_date,
                    opened_at=self._market_local_naive_to_utc_naive(market, trade_date, time(15, 0)),
                    execution_index=1,
                    opened_price=price,
                    normalized_notional=allocated,
                    quantity=quantity,
                    initial_quantity=quantity,
                    available_quantity=available_quantity,
                    frozen_quantity=frozen_quantity,
                    capital_account_id=account.id,
                    currency=currency,
                    allocated_capital=allocated,
                    initial_allocated_capital=allocated,
                    remaining_cost=allocated,
                    realized_quantity=0.0,
                    realized_proceeds=0.0,
                    realized_fees=0.0,
                    realized_pnl=0.0,
                    lot_size=self._lot_size_for_market(market),
                    sizing_reason="portfolio_import_exact_quantity",
                    legacy_fixed_notional=False,
                    sync_origin="portfolio_import",
                    sync_status="synced_from_portfolio",
                    source_portfolio_position_id=int(portfolio_position_id),
                    last_synced_at=now,
                    sellability_rule=self._sellability_rule_for(market, stock_code),
                    stop_loss=getattr(report, "stop_loss", None),
                    target_price=getattr(report, "take_profit", None),
                    max_holding_sessions=import_max_holding_sessions,
                    trading_style=plan.trading_style,
                    trading_style_policy_version=plan.trading_style_policy_version,
                    trading_style_settings_json=plan.trading_style_settings_json,
                )
                session.add(position)
                session.flush()
                self._sync_open_position_count_in_session(session, account)
                event = PaperTradeEvent(
                    event_key=f"portfolio-import:{plan.id}:open:{trade_date.isoformat()}",
                    plan_id=plan.id,
                    position_id=position.id,
                    source_report_id=report.id,
                    decision_signal_id=None,
                    source_model=model,
                    market=market,
                    stock_code=stock_code,
                    stock_name=getattr(report, "name", None),
                    event_type="open",
                    event_date=trade_date,
                    price=price,
                    quantity=quantity,
                    gross_amount=allocated,
                    fees=0.0,
                    net_amount=allocated,
                    fill_price=price,
                    fill_evidence_grade="portfolio_import_cost_basis",
                    reason="portfolio_import",
                    capital_account_id=account.id,
                    currency=currency,
                    allocated_capital=allocated,
                    cash_after=account.cash,
                    exposure_after=account.exposure,
                    realized_pnl_after=account.realized_pnl,
                    details_json=self._json_dumps({
                        "simulated": True,
                        "origin": "portfolio_import",
                        "quantity": quantity,
                        "portfolio_position_id": int(portfolio_position_id),
                        "sync_status": "synced_from_portfolio",
                    }),
                )
                session.add(event)
                session.flush()
                link = PaperSyncLink(
                    idempotency_key=entry["model_key"],
                    payload_hash=entry["payload_hash"],
                    direction="portfolio_to_paper",
                    status="synced",
                    source_model=model,
                    market=market,
                    stock_code=stock_code,
                    portfolio_account_id=portfolio_position.account_id,
                    portfolio_position_id=int(portfolio_position_id),
                    paper_plan_id=plan.id,
                    paper_position_id=position.id,
                    paper_event_id=event.id,
                    quantity=quantity,
                    price=price,
                    fee=0.0,
                    trade_date=trade_date,
                    origin="portfolio_import",
                    details_json=self._json_dumps(entry["payload"]),
                )
                session.add(link)
                session.flush()
                items.append(self._serialize_sync_link(link))
                created = True
            return {"created": created, "items": items}

    def sync_paper_position_to_portfolio(
        self,
        *,
        paper_position_id: int,
        portfolio_account_id: int,
        side: str,
        quantity: float,
        price: float,
        trade_date: date,
        fee: float = 0.0,
        idempotency_key: str,
        note: Optional[str] = None,
    ) -> Dict[str, Any]:
        key = str(idempotency_key or "").strip()
        if not key:
            raise ValueError("idempotency_key is required")
        side_norm = str(side or "").strip().lower()
        if side_norm not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        quantity_value = self._positive_amount(quantity, "quantity")
        price_value = self._positive_amount(price, "price")
        fee_value = self._non_negative_amount(fee, "fee")
        if not isinstance(trade_date, date):
            raise ValueError("trade_date is required")

        with self._ledger_write_session() as session:
            position = session.get(PaperPosition, int(paper_position_id))
            if position is None:
                raise PaperTradingNotFoundError(f"paper position not found: {paper_position_id}")
            from src.services.portfolio_service import (
                PortfolioConflictError as PortfolioLedgerConflictError,
                PortfolioOversellError,
                PortfolioService,
            )

            portfolio_service = PortfolioService()
            account = portfolio_service._require_active_account_in_session(
                session=session,
                account_id=int(portfolio_account_id),
            )
            identity = canonical_paper_identity(position.market, position.stock_code)
            if identity is None:
                raise ValueError("paper position identity is ambiguous or unsupported")
            market, stock_code = identity
            if str(account.market or "").lower() != market:
                raise ValueError("portfolio account market does not match paper position")
            symbol = self._portfolio_symbol_for_identity(market, stock_code)
            symbol = portfolio_service._normalize_symbol_for_storage(symbol)
            currency = portfolio_service._normalize_currency(self._currency_for_market(market))
            trade_uid = f"paper-sync:{key}"[:128]
            payload = {
                "paper_position_id": int(paper_position_id),
                "portfolio_account_id": int(portfolio_account_id),
                "side": side_norm,
                "market": market,
                "stock_code": stock_code,
                "symbol": symbol,
                "source_model": position.source_model,
                "quantity": round(quantity_value, 8),
                "price": round(price_value, 8),
                "fee": round(fee_value, 8),
                "trade_date": trade_date.isoformat(),
                "direction": "paper_to_portfolio",
            }
            payload_hash = self._sync_payload_hash(payload)
            existing = self._sync_link_for_update(session, key)
            if existing is not None:
                if existing.payload_hash != payload_hash:
                    raise PaperTradingConflictError("idempotency key reused with a different sync payload")
                return {"created": False, "item": self._serialize_sync_link(existing)}
            duplicate_links = session.execute(
                select(PaperSyncLink)
                .where(
                    PaperSyncLink.direction == "paper_to_portfolio",
                    PaperSyncLink.paper_position_id == int(paper_position_id),
                    PaperSyncLink.portfolio_account_id == int(portfolio_account_id),
                )
            ).scalars().all()
            for link in duplicate_links:
                details, _ = self._json_object(link.details_json)
                if details.get("side") == side_norm:
                    raise PaperTradingConflictError("paper position side already synced to this portfolio account")
            try:
                portfolio_service._validate_trade_identity(
                    account_id=int(portfolio_account_id),
                    trade_uid=trade_uid,
                    dedup_hash=payload_hash,
                    session=session,
                )
                if side_norm == "sell":
                    portfolio_service._validate_sell_quantity(
                        account_id=int(portfolio_account_id),
                        symbol=symbol,
                        market=market,
                        currency=currency,
                        trade_date=trade_date,
                        quantity=quantity_value,
                        session=session,
                    )
                trade = portfolio_service.repo.add_trade_in_session(
                    session=session,
                    account_id=int(portfolio_account_id),
                    trade_uid=trade_uid,
                    symbol=symbol,
                    market=market,
                    currency=currency,
                    trade_date=trade_date,
                    side=side_norm,
                    quantity=quantity_value,
                    price=price_value,
                    fee=fee_value,
                    tax=0.0,
                    note=(note or f"paper sync {position.source_model} #{paper_position_id}")[:255],
                    dedup_hash=payload_hash,
                )
            except (PortfolioLedgerConflictError, PortfolioOversellError) as exc:
                raise PaperTradingConflictError(str(exc)) from exc
            now = utc_naive_now()
            position.sync_status = "synced_to_portfolio"
            position.last_synced_at = now
            position.updated_at = now
            link = PaperSyncLink(
                idempotency_key=key,
                payload_hash=payload_hash,
                direction="paper_to_portfolio",
                status="synced",
                source_model=position.source_model,
                market=market,
                stock_code=stock_code,
                portfolio_account_id=int(portfolio_account_id),
                portfolio_trade_id=trade.id,
                paper_plan_id=position.plan_id,
                paper_position_id=position.id,
                quantity=quantity_value,
                price=price_value,
                fee=fee_value,
                trade_date=trade_date,
                origin="paper_confirmation",
                details_json=self._json_dumps(payload),
            )
            session.add(link)
            session.flush()
            return {"created": True, "item": self._serialize_sync_link(link)}

    def initialize_account(
        self,
        *,
        source_model: str,
        market: str,
        initial_capital: Optional[float] = None,
    ) -> Dict[str, Any]:
        model = self._validate_configured_model(source_model)
        normalized_market = self._normalize_supported_market(market)
        capital = self.initial_capital if initial_capital is None else float(initial_capital)
        if not self._positive_finite(capital):
            raise ValueError("initial_capital must be finite and positive")
        with self._ledger_write_session() as session:
            existing = self._find_account_for_update(
                session,
                source_model=model,
                market=normalized_market,
            )
            if existing is not None:
                return {"created": False, "item": self._serialize_account(existing)}
            account = self._create_account(
                session,
                source_model=model,
                market=normalized_market,
                initial_capital=capital,
            )
            return {"created": True, "item": self._serialize_account(account)}

    def adjust_account(
        self,
        account_id: int,
        *,
        adjustment_type: str,
        amount: float,
        idempotency_key: str,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        kind = str(adjustment_type or "").strip().lower()
        if kind not in {"deposit", "withdrawal"}:
            raise ValueError("adjustment_type must be deposit or withdrawal")
        if not self._positive_finite(amount):
            raise ValueError("amount must be finite and positive")
        key = str(idempotency_key or "").strip()
        if not key:
            raise ValueError("idempotency_key is required")
        if len(key) > 128:
            raise ValueError("idempotency_key is too long")
        clean_reason = str(reason or "").strip()
        if not clean_reason:
            raise ValueError("reason is required")
        clean_reason = clean_reason[:255]
        payload_hash = self._adjustment_payload_hash(
            account_id=int(account_id),
            adjustment_type=kind,
            amount=float(amount),
            reason=clean_reason,
        )

        with self._ledger_write_session() as session:
            existing = self._get_adjustment_for_update(session, key)
            if existing is not None:
                if existing.payload_hash != payload_hash:
                    raise PaperTradingConflictError("idempotency_key was already used for a different adjustment")
                account = session.get(PaperCapitalAccount, int(existing.account_id))
                return {
                    "created": False,
                    "item": self._serialize_adjustment(existing),
                    "account": self._serialize_account(account) if account is not None else None,
                }

            account = self._get_account_for_update_by_id(session, int(account_id))
            before_cash = float(account.cash or 0.0)
            before_book = self._book_equity(account)
            signed_amount = float(amount) if kind == "deposit" else -float(amount)
            after_cash = before_cash + signed_amount
            after_book = before_book + signed_amount
            if kind == "withdrawal":
                if after_cash < -1e-9:
                    raise ValueError("withdrawal amount exceeds available cash")
                max_total_exposure = after_book * (float(account.max_total_exposure_pct or 0.0) / 100.0)
                if after_book <= 0 or float(account.exposure or 0.0) > max_total_exposure + 1e-9:
                    raise ValueError("withdrawal would violate max total exposure policy")
            account.cash = after_cash
            account.external_cash_flow = float(getattr(account, "external_cash_flow", 0.0) or 0.0) + signed_amount
            account.updated_at = utc_naive_now()
            adjustment = PaperCapitalAdjustment(
                idempotency_key=key,
                payload_hash=payload_hash,
                account_id=account.id,
                source_model=account.source_model,
                market=account.market,
                currency=account.currency,
                adjustment_type=kind,
                amount=float(amount),
                reason=clean_reason,
                before_cash=before_cash,
                after_cash=after_cash,
                before_book_equity=before_book,
                after_book_equity=after_book,
            )
            session.add(adjustment)
            session.flush()
            return {
                "created": True,
                "item": self._serialize_adjustment(adjustment),
                "account": self._serialize_account(account),
            }

    def list_dispatches(
        self, *, model: Optional[str] = None, status: Optional[str] = None,
        offset: int = 0, limit: int = 100,
    ) -> Dict[str, Any]:
        rows, total = self.repo.list_dispatches(model=model, status=status, offset=offset, limit=limit)
        return {"items": [self._serialize_dispatch(row) for row in rows], "total": total}

    def performance(self, model: Optional[str] = None) -> Dict[str, Any]:
        stats = self.repo.performance(model=model)
        valuation = self._performance_valuation(model=model)
        realized_pnl = stats.get("net_realized_pnl", stats.get("total_pnl", 0.0))
        total_pnl = (
            float(realized_pnl or 0.0) + float(valuation["unrealized_pnl"])
            if valuation["valuation_complete"] else None
        )
        result = {
            "model": model,
            "total_plans": stats["total_plans"],
            "executable_plans": stats["executable_plans"],
            "open_positions": stats["open_positions"],
            "closed_positions": stats["closed_positions"],
            "wins": stats["wins"],
            "losses": stats["losses"],
            "win_rate_pct": (
                round(stats["wins"] / stats["return_count"] * 100, 6)
                if stats["return_count"] else None
            ),
            "realized_return_pct": (
                round(float(realized_pnl or 0.0) / stats["total_notional"] * 100, 6)
                if stats["total_notional"] else None
            ),
            "gross_realized_pnl": round(stats.get("gross_realized_pnl", 0.0), 6),
            "net_realized_pnl": round(float(realized_pnl or 0.0), 6),
            "realized_pnl": round(float(realized_pnl or 0.0), 6),
            "unrealized_pnl": (
                round(float(valuation["unrealized_pnl"]), 6)
                if valuation["valuation_complete"] else None
            ),
            "total_pnl": round(float(total_pnl), 6) if total_pnl is not None else None,
            "market_value": (
                round(float(valuation["market_value"]), 6)
                if valuation["valuation_complete"] else None
            ),
            "valuation_complete": valuation["valuation_complete"],
            "unpriced_position_count": valuation["unpriced_position_count"],
            "unpriced_holding_count": valuation["unpriced_holding_count"],
            "avg_return_pct": (
                round(stats["avg_return_pct"], 6)
                if stats["avg_return_pct"] is not None else None
            ),
            "realized_positions": stats.get("realized_positions", 0),
            "partially_realized_positions": stats.get("partially_realized_positions", 0),
            "total_fees": round(stats.get("total_fees", 0.0), 6),
            "total_commission": round(stats.get("event_commission", 0.0), 6),
            "total_sell_tax": round(stats.get("event_sell_tax", 0.0), 6),
            "total_slippage_amount": round(stats.get("event_slippage_amount", 0.0), 6),
            "explicit_cost_total": round(
                float(stats.get("event_commission", 0.0) or 0.0)
                + float(stats.get("event_sell_tax", 0.0) or 0.0)
                + float(stats.get("event_slippage_amount", 0.0) or 0.0),
                6,
            ),
            "normalized_notional_per_position": self.normalized_notional,
        }
        if model is None:
            result["models"] = [self.performance(item) for item in self.repo.performance_models()]
        return result

    def _performance_valuation(self, *, model: Optional[str]) -> Dict[str, Any]:
        current_time = datetime.now(timezone.utc)
        conditions = [PaperCapitalAccount.source_model == model] if model else []
        clause = and_(*conditions) if conditions else True
        totals = {
            "market_value": 0.0,
            "unrealized_pnl": 0.0,
            "valuation_complete": True,
            "unpriced_position_count": 0,
            "unpriced_holding_count": 0,
        }
        with self.db.get_session() as session:
            accounts = list(session.execute(
                select(PaperCapitalAccount).where(clause)
            ).scalars().all())
            for account in accounts:
                snapshot = self._account_valuation_snapshot(
                    session,
                    account,
                    current_time=current_time,
                )
                totals["valuation_complete"] = (
                    totals["valuation_complete"]
                    and bool(snapshot.get("valuation_complete"))
                )
                totals["unpriced_position_count"] += int(snapshot.get("unpriced_position_count") or 0)
                totals["unpriced_holding_count"] += int(snapshot.get("unpriced_holding_count") or 0)
                if snapshot.get("valuation_complete"):
                    totals["market_value"] += float(snapshot.get("market_value") or 0.0)
                    totals["unrealized_pnl"] += float(snapshot.get("unrealized_pnl") or 0.0)
        return totals

    def run_intraday_monitor(
        self,
        *,
        current_time: Optional[datetime] = None,
        fetcher_manager: Optional[Any] = None,
        interval_seconds: int = 60,
    ) -> Dict[str, Any]:
        """Evaluate active hybrid plans and open positions on fresh realtime quotes.

        This mutates only the local paper ledger. It never calls broker APIs and
        never changes manually recorded portfolio holdings.
        """

        now = current_time or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        upgraded = self._upgrade_old_active_plans()
        repaired = self._repair_plan_lifecycle(now)
        plan_reconciled = self.reconcile_report_execution_plans()
        reanalysis_reconciled = self.reconcile_reanalysis_requests(current_time=now)
        risk_reconciled = self.reconcile_open_position_risk_from_latest_reports(current_time=now)
        bucket = self._bucket_start(now, max(30, int(interval_seconds)))
        manager = fetcher_manager or DataFetcherManager()
        active_plans = self.repo.list_intraday_candidate_plans()
        open_positions = self.repo.list_open_positions_all()
        summary: Dict[str, Any] = {
            "mode": "intraday",
            "observed_at": now.isoformat(),
            "bucket_started_at": bucket.isoformat(),
            "plans_evaluated": 0,
            "positions_evaluated": 0,
            "target_identity_count": 0,
            "quote_fetch_attempts": 0,
            "quote_fetch_successes": 0,
            "quote_fetch_failures": 0,
            "quote_stale_count": 0,
            "observations": 0,
            "positions_opened": 0,
            "positions_closed": 0,
            "positions_reduced": 0,
            "valuation_marks_recorded": 0,
            "skipped_closed_market": 0,
            "failed_closed": 0,
            "legacy_plans_upgraded": upgraded,
            "plans_lifecycle_repaired": repaired,
            "report_plans_reconciled": plan_reconciled,
            "reanalysis_requests_reconciled": reanalysis_reconciled,
            "risk_revisions_reconciled": risk_reconciled,
            "events": [],
        }
        reduced_order_keys = set()
        by_identity: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for plan in active_plans:
            by_identity.setdefault((plan.market, plan.stock_code), {"plans": [], "positions": []})["plans"].append(plan)
        for position in open_positions:
            by_identity.setdefault((position.market, position.stock_code), {"plans": [], "positions": []})["positions"].append(position)
        summary["target_identity_count"] = len(by_identity)

        # Refresh all open-holding marks before evaluating any new entry.  A
        # sequential fetch-and-trade loop can otherwise let the first symbol
        # see stale account equity simply because another holding has not been
        # visited yet in the current bucket.  The mark is display/risk input
        # only; every fill still validates the live quote independently.
        runtime_by_identity: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for identity, group in sorted(by_identity.items()):
            market, stock_code = identity
            phase = build_market_phase_context(
                market=market,
                current_time=now,
                trigger_source="paper_trading_intraday",
            )
            if not self._phase_allows_intraday_monitor(phase):
                summary["skipped_closed_market"] += len(group["plans"]) + len(group["positions"])
                continue
            summary["quote_fetch_attempts"] += 1
            quote = self._fetch_quote(manager, stock_code)
            if quote is None:
                summary["quote_fetch_failures"] += 1
            else:
                summary["quote_fetch_successes"] += 1
                if getattr(quote, "is_stale", None) is True:
                    summary["quote_stale_count"] += 1
            prior_date = self._latest_proven_complete_date(market, now)
            prior_bars = [
                {
                    "date": row.date,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": (
                        row.to_dict().get("volume")
                        if callable(getattr(row, "to_dict", None))
                        else row.volume
                    ),
                    "ma5": row.ma5,
                    "ma10": row.ma10,
                    "ma20": row.ma20,
                    "is_complete": not getattr(row, "_paper_variant_conflict", False),
                }
                for row in self.repo.daily_bars(
                    market,
                    stock_code,
                    prior_date,
                    limit=self._analysis_window_sessions_for_identity(market, stock_code),
                )
            ]
            summary["valuation_marks_recorded"] += self._persist_intraday_valuation_marks(
                positions=group["positions"],
                quote=quote,
                now=now,
                bucket=bucket,
                phase=phase,
            )
            runtime_by_identity[identity] = {
                "group": group,
                "phase": phase,
                "quote": quote,
                "prior_bars": prior_bars,
            }

        for identity, runtime in sorted(runtime_by_identity.items()):
            group = runtime["group"]
            phase = runtime["phase"]
            quote = runtime["quote"]
            prior_bars = runtime["prior_bars"]
            position_outcomes = self._evaluate_intraday_position_groups(
                group["positions"], quote, prior_bars, now, bucket, phase
            )
            summary["positions_evaluated"] += len(group["positions"])
            for outcome in position_outcomes:
                if outcome is None:
                    continue
                summary["observations"] += 1
                events = outcome.get("events") if isinstance(outcome.get("events"), list) else []
                if outcome.get("event"):
                    events = [outcome["event"], *events]
                for event in events:
                    summary["events"].append(event)
                    if event.get("event_type") == "close":
                        summary["positions_closed"] += 1
                    elif event.get("event_type") == "reduce":
                        details = event.get("details") if isinstance(event.get("details"), dict) else {}
                        order_key = details.get("logical_order_key") or f"event:{event.get('id')}"
                        if order_key not in reduced_order_keys:
                            reduced_order_keys.add(order_key)
                            summary["positions_reduced"] += 1
                if outcome.get("result") == "failed_closed":
                    summary["failed_closed"] += 1

            for plan in group["plans"]:
                summary["plans_evaluated"] += 1
                self._consume_due_next_check_obligation(plan=plan, now=now, phase=phase)
                outcome = self._evaluate_intraday_plan(
                    plan, quote, prior_bars, now, bucket, phase
                )
                if outcome is None:
                    continue
                summary["observations"] += 1
                if outcome.get("event"):
                    summary["events"].append(outcome["event"])
                    summary["positions_opened"] += 1
                if outcome.get("result") == "failed_closed":
                    summary["failed_closed"] += 1
        summary["dispatches"] = self.dispatch_pending_notifications()
        return summary

    def _persist_intraday_valuation_marks(
        self,
        *,
        positions: Sequence[PaperPosition],
        quote: Any,
        now: datetime,
        bucket: datetime,
        phase: Any,
    ) -> int:
        """Persist one fresh non-executable mark per model/symbol/bucket.

        Position evaluation intentionally suppresses ordinary ``hold`` rows
        when aggregate signal handling is enabled.  Without this independent
        mark, a successful monitor fetch can therefore leave holdings and
        account-risk snapshots pointing at an older event observation.
        """

        open_positions = [
            position for position in positions
            if str(getattr(position, "status", "") or "").lower() == "open"
        ]
        if not open_positions:
            return 0
        price, quote_error = self._fresh_quote_price(quote, observed_at=now)
        if quote_error or price is None:
            return 0

        grouped: Dict[Tuple[str, str, str], List[PaperPosition]] = {}
        for position in open_positions:
            if self._realtime_quote_identity_error(plan=position, quote=quote):
                continue
            identity = canonical_paper_identity(position.market, position.stock_code)
            if identity is None:
                continue
            grouped.setdefault(
                (str(position.source_model or ""), identity[0], identity[1]),
                [],
            ).append(position)
        if not grouped:
            return 0

        observed_naive = self._utc_naive(now)
        bucket_naive = self._utc_naive(bucket)
        recorded = 0
        with self._ledger_write_session() as session:
            for (source_model, market, stock_code), scoped_positions in sorted(grouped.items()):
                scoped_positions.sort(key=lambda item: int(item.id or 0))
                representative = scoped_positions[0]
                key_seed = (
                    f"{source_model}|{market}|{stock_code}|"
                    f"{bucket_naive.isoformat()}|valuation_mark"
                )
                observation_key = "valuation-" + hashlib.sha256(
                    key_seed.encode("utf-8")
                ).hexdigest()[:32]
                existing = session.execute(
                    select(PaperPlanObservation.id)
                    .where(PaperPlanObservation.observation_key == observation_key)
                    .limit(1)
                ).scalar_one_or_none()
                if existing is not None:
                    continue
                db_position = session.get(PaperPosition, int(representative.id))
                if db_position is None or db_position.status != "open":
                    continue
                db_plan = session.get(PaperExecutionPlan, int(db_position.plan_id))
                if db_plan is None:
                    continue
                scoped_ids = sorted(int(item.id) for item in scoped_positions if item.id is not None)
                evidence = {
                    "mode": "valuation_mark",
                    "phase": self._phase_evidence(phase),
                    "quote": self._quote_evidence(quote),
                    "position_ids": scoped_ids,
                    "policy": {
                        "display_and_account_risk_only": True,
                        "not_an_execution_fill": True,
                        "freshness_revalidated_for_every_fill": True,
                        "one_mark_per_model_symbol_bucket": True,
                    },
                    "reasons": ["fresh_monitor_quote_persisted_for_valuation"],
                }
                session.add(PaperPlanObservation(
                    observation_key=observation_key,
                    plan_id=db_plan.id,
                    position_id=None,
                    source_report_id=db_plan.source_report_id,
                    decision_signal_id=db_plan.decision_signal_id,
                    source_model=source_model,
                    market=market,
                    stock_code=stock_code,
                    stock_name=db_plan.stock_name,
                    mode="valuation",
                    observed_at=observed_naive,
                    bucket_started_at=bucket_naive,
                    quote_price=price,
                    quote_source=self._quote_source(quote),
                    quote_provider_timestamp=str(getattr(quote, "provider_timestamp", "") or "")[:64] or None,
                    quote_fetched_at=str(getattr(quote, "fetched_at", "") or "")[:64] or None,
                    result="valuation_mark",
                    action="none",
                    required_passed=True,
                    factors_passed=0,
                    factors_available=0,
                    evidence_json=self._json_dumps(evidence),
                ))
                recorded += 1
            session.flush()
        return recorded

    def run_opening_confirmation_check(
        self,
        *,
        current_time: Optional[datetime] = None,
        fetcher_manager: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Confirm prior-session CN entry plans on fresh 09:30-09:35 quotes.

        This sweep never opens a position.  It records a durable confirmation
        or submits one same-model reanalysis request when the opening evidence
        materially invalidates the prior plan.  The normal intraday monitor
        remains the only entry/exit executor.
        """

        now = current_time or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        china_now = now.astimezone(ZoneInfo("Asia/Shanghai"))
        local_clock = china_now.time().replace(tzinfo=None)
        if not (time(9, 30) <= local_clock <= time(9, 35)):
            return {
                "mode": "opening_confirmation",
                "observed_at": now.isoformat(),
                "skipped": True,
                "reason": "outside_cn_opening_confirmation_window",
                "window": "09:30:00-09:35:00 Asia/Shanghai",
            }
        upgraded = self._upgrade_old_active_plans()
        repaired = self._repair_plan_lifecycle(now)
        plan_reconciled = self.reconcile_report_execution_plans()
        reconciled = self.reconcile_reanalysis_requests(current_time=now)
        bucket = self._bucket_start(now, 30)
        manager = fetcher_manager or DataFetcherManager()
        plans = self.repo.list_intraday_candidate_plans()
        summary: Dict[str, Any] = {
            "mode": "opening_confirmation",
            "observed_at": now.isoformat(),
            "bucket_started_at": bucket.isoformat(),
            "plans_evaluated": 0,
            "observations": 0,
            "confirmed": 0,
            "reanalysis_requested": 0,
            "failed_closed": 0,
            "skipped_non_cn": 0,
            "skipped_same_session": 0,
            "skipped_already_terminal": 0,
            "legacy_plans_upgraded": upgraded,
            "plans_lifecycle_repaired": repaired,
            "report_plans_reconciled": plan_reconciled,
            "reanalysis_requests_reconciled": reconciled,
        }
        by_identity: Dict[Tuple[str, str], List[Any]] = {}
        for plan in plans:
            if str(plan.market or "").lower() != "cn":
                summary["skipped_non_cn"] += 1
                continue
            if plan.report_date >= china_now.date():
                summary["skipped_same_session"] += 1
                continue
            if getattr(plan, "valid_for_date", None) != china_now.date():
                continue
            by_identity.setdefault((plan.market, plan.stock_code), []).append(plan)

        for (market, stock_code), identity_plans in sorted(by_identity.items()):
            phase = build_market_phase_context(
                market=market,
                current_time=now,
                trigger_source="paper_trading_opening_confirmation",
            )
            if not self._phase_allows_intraday_monitor(phase):
                summary["failed_closed"] += len(identity_plans)
                continue
            pending_plans = [
                plan for plan in identity_plans
                if not self._opening_confirmation_terminal(plan=plan, session_date=china_now.date())
            ]
            summary["skipped_already_terminal"] += len(identity_plans) - len(pending_plans)
            if not pending_plans:
                continue
            quote = self._fetch_quote(manager, stock_code)
            for plan in pending_plans:
                summary["plans_evaluated"] += 1
                outcome = self._process_opening_confirmation_plan(
                    plan=plan,
                    quote=quote,
                    now=now,
                    bucket=bucket,
                    phase=phase,
                )
                if outcome is None:
                    continue
                summary["observations"] += 1
                if outcome.get("result") == "opening_confirmed":
                    summary["confirmed"] += 1
                elif outcome.get("result") == "reanalysis_blocked":
                    summary["reanalysis_requested"] += 1
                elif outcome.get("result") == "failed_closed":
                    summary["failed_closed"] += 1
        return summary

    def run_opening_auction_check(
        self,
        *,
        current_time: Optional[datetime] = None,
        fetcher_manager: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Compatibility alias; the paid auction strategy is no longer scheduled."""

        return self.run_opening_confirmation_check(
            current_time=current_time,
            fetcher_manager=fetcher_manager,
        )

    def _upgrade_old_active_plans(self) -> int:
        """Create current-version replacements for old active plans exactly once."""

        upgraded = 0
        old_active = [
            plan for plan in self.repo.list_active_plans()
            if plan.plan_version in UPGRADABLE_ACTIVE_PLAN_VERSIONS and plan.status == "active"
        ]
        for plan in old_active:
            try:
                result = self.create_plan_for_report(
                    plan.source_report_id,
                    decision_signal_id=plan.decision_signal_id,
                )
            except Exception:
                continue
            with self.db.session_scope() as session:
                has_open_position = session.execute(select(PaperPosition.id).where(
                    PaperPosition.plan_id == plan.id,
                    PaperPosition.status == "open",
                ).limit(1)).scalar_one_or_none() is not None
                item = result.get("item") if isinstance(result, dict) else None
                new_id = item.get("id") if isinstance(item, dict) else None
                replacement_ready = False
                if new_id is not None:
                    replacement = session.get(PaperExecutionPlan, int(new_id))
                    replacement_ready = bool(
                        replacement is not None
                        and replacement.plan_version == PLAN_VERSION
                        and replacement.executable
                        and replacement.status == "active"
                    )
                if not (replacement_ready or has_open_position):
                    continue
                db_plan = session.get(PaperExecutionPlan, plan.id)
                if db_plan is not None and db_plan.status == "active":
                    db_plan.status = "superseded"
                    db_plan.updated_at = utc_naive_now()
                    upgraded += 1
                elif db_plan is not None and db_plan.status == "superseded":
                    upgraded += 1
        return upgraded

    _upgrade_legacy_active_plans = _upgrade_old_active_plans

    def _repair_plan_lifecycle(self, current_time: Optional[datetime] = None) -> int:
        now = current_time or datetime.now(timezone.utc)
        repaired = 0
        with self._ledger_write_session() as session:
            identities = session.execute(
                select(
                    PaperExecutionPlan.source_model,
                    PaperExecutionPlan.market,
                    PaperExecutionPlan.stock_code,
                )
                .where(
                    PaperExecutionPlan.status == "active",
                    PaperExecutionPlan.executable.is_(True),
                )
                .distinct()
            ).all()
            for source_model, market, stock_code in identities:
                phase = build_market_phase_context(
                    market=market,
                    current_time=now,
                    trigger_source="paper_trading_lifecycle_repair",
                )
                repaired += self._repair_plan_lifecycle_in_session(
                    session,
                    source_model=source_model,
                    market=market,
                    stock_code=stock_code,
                    current_session_date=self._phase_session_date(phase),
                )
        return repaired

    def _repair_plan_lifecycle_in_session(
        self,
        session,
        *,
        source_model: str,
        market: str,
        stock_code: str,
        current_session_date: Optional[date],
    ) -> int:
        plans = list(session.execute(
            select(PaperExecutionPlan)
            .where(
                PaperExecutionPlan.source_model == source_model,
                PaperExecutionPlan.market == market,
                PaperExecutionPlan.stock_code == stock_code,
                PaperExecutionPlan.plan_version == PLAN_VERSION,
            )
            .order_by(
                desc(PaperExecutionPlan.report_created_at),
                desc(PaperExecutionPlan.id),
            )
        ).scalars().all())
        if not plans:
            return 0
        repaired = 0
        now = utc_naive_now()
        plan_ids = [int(plan.id) for plan in plans if getattr(plan, "id", None) is not None]
        open_plan_ids = set()
        if plan_ids:
            open_plan_ids = set(session.execute(
                select(PaperPosition.plan_id)
                .where(
                    PaperPosition.plan_id.in_(plan_ids),
                    PaperPosition.status == "open",
                )
            ).scalars().all())
        authority = next(
            (
                plan for plan in plans
                if int(plan.id) in open_plan_ids
                or (plan.status == "active" and bool(plan.executable))
            ),
            None,
        )
        for plan in plans:
            valid_for = getattr(plan, "valid_for_date", None)
            if valid_for is None:
                valid_for = self._next_trading_session(plan.market, plan.report_date)
                plan.valid_for_date = valid_for
                plan.updated_at = now
                repaired += 1
            if (
                authority is not None
                and plan.id == authority.id
                and plan.status == "active"
                and current_session_date is not None
                and valid_for is not None
                and valid_for < current_session_date
            ):
                plan.status = "expired"
                plan.updated_at = now
                repaired += 1
                continue
            if (
                authority is not None
                and plan.id != authority.id
                and plan.status in {"active", "triggered"}
                and int(plan.id) not in open_plan_ids
            ):
                plan.status = "superseded"
                plan.updated_at = now
                repaired += 1
        return repaired

    def _fetch_quote(self, manager: Any, stock_code: str) -> Optional[Any]:
        try:
            return manager.get_realtime_quote(stock_code, log_final_failure=False)
        except TypeError:
            return manager.get_realtime_quote(stock_code)

    @staticmethod
    def _normalize_t0_identities(symbols: Sequence[str]) -> set[Tuple[str, str]]:
        result: set[Tuple[str, str]] = set()
        for raw in symbols or []:
            text = str(raw or "").strip()
            if not text:
                continue
            market = get_market_for_stock(normalize_stock_code(text)) or ""
            identity = canonical_paper_identity(market, text)
            if identity is not None:
                result.add(identity)
        return result

    def _sellability_rule_for(self, market: str, stock_code: str) -> str:
        identity = canonical_paper_identity(market, stock_code)
        if identity is not None and identity in self.t0_identities:
            return "explicit_t0"
        if str(market or "").lower() == "cn":
            return "cn_t1"
        return "non_cn_t0"

    def _can_close_position_on_date(self, position: PaperPosition, event_date: date) -> bool:
        opened_on = position.opened_on
        if opened_on is None or event_date > opened_on:
            return True
        return self._sellability_rule_for(position.market, position.stock_code) in {"explicit_t0", "non_cn_t0"}

    def _sellability_evidence(self, position: PaperPosition, event_date: date) -> Dict[str, Any]:
        rule = self._sellability_rule_for(position.market, position.stock_code)
        identity = canonical_paper_identity(position.market, position.stock_code)
        return {
            "market": position.market,
            "stock_code": position.stock_code,
            "opened_on": position.opened_on.isoformat() if position.opened_on else None,
            "event_date": event_date.isoformat(),
            "rule": rule,
            "same_day_close_allowed": rule == "explicit_t0",
            "explicit_t0": identity in self.t0_identities if identity is not None else False,
        }

    def _initial_sellability_quantities(
        self,
        *,
        market: str,
        stock_code: str,
        quantity: float,
    ) -> Tuple[float, float]:
        safe_quantity = float(quantity or 0.0)
        if self._sellability_rule_for(market, stock_code) == "cn_t1":
            return 0.0, safe_quantity
        return safe_quantity, 0.0

    def _availability_date_for_market(self, market: str, current_time: datetime) -> date:
        try:
            phase = build_market_phase_context(
                market=market,
                current_time=current_time,
                trigger_source="paper_trading_holdings_availability",
            )
            session_date = getattr(phase, "session_date", None)
            if isinstance(session_date, date):
                return session_date
            market_time = getattr(phase, "market_local_time", None)
            if isinstance(market_time, datetime):
                return market_time.date()
        except Exception:
            pass
        return self._utc_naive(current_time).date()

    def _market_local_date_for_time(self, market: str, current_time: datetime) -> date:
        try:
            tz = self._market_timezone(market)
            aware = current_time if current_time.tzinfo else current_time.replace(tzinfo=timezone.utc)
            return aware.astimezone(tz).date()
        except Exception:
            return self._utc_naive(current_time).date()

    @staticmethod
    def _next_trading_session(market: str, after_date: date) -> date:
        candidate = after_date + timedelta(days=1)
        for _ in range(20):
            if is_market_open(market, candidate):
                return candidate
            candidate += timedelta(days=1)
        return after_date + timedelta(days=1)

    def _opening_confirmation_terminal(self, *, plan, session_date: date) -> bool:
        china_tz = ZoneInfo("Asia/Shanghai")
        start = datetime.combine(session_date, time(9, 30), tzinfo=china_tz).astimezone(timezone.utc).replace(tzinfo=None)
        end = datetime.combine(session_date, time(15, 0), tzinfo=china_tz).astimezone(timezone.utc).replace(tzinfo=None)
        with self.db.get_session() as session:
            row = session.execute(
                select(PaperPlanObservation.id)
                .where(
                    PaperPlanObservation.plan_id == int(plan.id),
                    PaperPlanObservation.observed_at >= start,
                    PaperPlanObservation.observed_at <= end,
                    or_(
                        PaperPlanObservation.result.in_(("opening_confirmed", "reanalysis_blocked")),
                        PaperPlanObservation.evidence_json.like('%"opening_confirmation_status": "confirmed"%'),
                        PaperPlanObservation.evidence_json.like('%"opening_confirmation_status":"confirmed"%'),
                    ),
                )
                .limit(1)
            ).scalar_one_or_none()
        return row is not None

    @staticmethod
    def _opening_confirmation_required(*, plan, phase) -> bool:
        market = str(getattr(plan, "market", "") or "").lower()
        local_time = getattr(phase, "market_local_time", None)
        if market != "cn" or not isinstance(local_time, datetime):
            return False
        session_date = local_time.date()
        valid_for_date = getattr(plan, "valid_for_date", None)
        report_date = getattr(plan, "report_date", None)
        return bool(
            local_time.time() >= time(9, 30)
            and isinstance(report_date, date)
            and report_date < session_date
            and valid_for_date == session_date
        )

    def _process_opening_confirmation_plan(
        self,
        *,
        plan,
        quote,
        now: datetime,
        bucket: datetime,
        phase,
        persist_confirmed: bool = True,
    ) -> Optional[Dict[str, Any]]:
        evidence: Dict[str, Any] = {
            "mode": "opening_confirmation",
            "phase": self._phase_evidence(phase),
            "quote": self._quote_evidence(quote),
            "policy": {
                "window": "09:30:00-09:35:00 Asia/Shanghai",
                "ordinary_realtime_quote": True,
                "true_auction_order_book_claimed": False,
                "no_fill_in_confirmation_sweep": True,
                "same_model_reanalysis_on_material_deviation": True,
                "price_deviation_threshold_pct": self.auction_price_deviation_pct,
                "min_opening_volume_ratio": self.auction_min_matched_volume_ratio,
            },
        }
        identity_error = self._realtime_quote_identity_error(plan=plan, quote=quote)
        price, quote_error = self._fresh_quote_price(quote, observed_at=now)
        failure = identity_error or quote_error
        if failure:
            evidence["fail_closed_reason"] = failure
            return self._persist_intraday_outcome(
                plan=plan,
                position=None,
                now=now,
                bucket=bucket,
                quote=quote,
                result="failed_closed",
                action="none",
                reasons=[failure],
                evidence=evidence,
                required_passed=False,
            )
        assert price is not None
        material, material_error = self._opening_confirmation_material_deviation(
            plan=plan,
            quote=quote,
            price=price,
            session_date=self._phase_session_date(phase),
        )
        if material_error:
            evidence["fail_closed_reason"] = material_error
            return self._persist_intraday_outcome(
                plan=plan,
                position=None,
                now=now,
                bucket=bucket,
                quote=quote,
                result="failed_closed",
                action="none",
                reasons=[material_error],
                evidence=evidence,
                required_passed=False,
            )
        evidence["opening_confirmation"] = material
        material_reasons = list((material or {}).get("material_reasons") or [])
        if material_reasons:
            request = self._ensure_reanalysis_request(
                plan=plan,
                trigger_type="opening_confirmation_deviation",
                session_date=self._phase_session_date(phase),
                due_at=self._utc_naive(now),
                observed_at=self._utc_naive(now),
                evidence=evidence,
                initial_status="pending",
                submit=True,
            )
            evidence["reanalysis_request"] = request
            request_status = str((request or {}).get("status") or "failed_closed")
            result = "reanalysis_blocked" if request_status in {"pending", "claimed", "submitted"} else "failed_closed"
            reason = (
                "opening_confirmation_material_deviation_reanalysis_requested"
                if result == "reanalysis_blocked"
                else "opening_confirmation_reanalysis_submission_failed"
            )
            if result == "failed_closed":
                evidence["fail_closed_reason"] = reason
            return self._persist_intraday_outcome(
                plan=plan,
                position=None,
                now=now,
                bucket=bucket,
                quote=quote,
                result=result,
                action="none",
                reasons=[reason, *material_reasons],
                evidence=evidence,
                required_passed=False,
            )
        evidence["opening_confirmation_status"] = "confirmed"
        if not persist_confirmed:
            return {
                "result": "opening_confirmed",
                "action": "none",
                "reasons": ["fresh_opening_quote_no_material_plan_deviation"],
                "evidence": evidence,
            }
        return self._persist_intraday_outcome(
            plan=plan,
            position=None,
            now=now,
            bucket=bucket,
            quote=quote,
            result="opening_confirmed",
            action="none",
            reasons=["fresh_opening_quote_no_material_plan_deviation"],
            evidence=evidence,
            required_passed=True,
        )

    def _opening_confirmation_material_deviation(
        self,
        *,
        plan,
        quote,
        price: float,
        session_date: date,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        rules, rules_error = self._json_list(getattr(plan, "rules_json", "[]"))
        if rules_error:
            return None, "invalid_opening_confirmation_rules"
        price_rules = [
            rule for rule in rules
            if isinstance(rule, Mapping) and rule.get("type") in PRICE_RULE_TYPES
        ]
        bounds: List[float] = []
        for rule in price_rules:
            if rule.get("type") == "close_between":
                values = (rule.get("low"), rule.get("high"))
            else:
                values = (rule.get("value"),)
            bounds.extend(value for value in (self._safe_price(item) for item in values) if value is not None)
        if not bounds:
            return None, "missing_opening_confirmation_price_rule"
        entry_low, entry_high = min(bounds), max(bounds)
        deviation_pct = 0.0
        if price < entry_low:
            deviation_pct = (entry_low / price - 1.0) * 100.0
        elif price > entry_high:
            deviation_pct = (price / entry_high - 1.0) * 100.0

        prior_close = self._safe_price(getattr(quote, "pre_close", None))
        if prior_close is None:
            try:
                with self.db.get_session() as session:
                    row = session.execute(
                        select(StockDaily)
                        .where(
                            StockDaily.code.in_(stock_daily_variants(plan.market, plan.stock_code)),
                            StockDaily.date < session_date,
                        )
                        .order_by(desc(StockDaily.date))
                        .limit(1)
                    ).scalar_one_or_none()
                prior_close = self._safe_price(row.close) if row is not None else None
            except Exception:
                prior_close = None
        gap_pct = (price / prior_close - 1.0) * 100.0 if prior_close else None
        volume_ratio = self._safe_float(getattr(quote, "volume_ratio", None))
        # Confirmation validates the pending plan, not a hypothetical market
        # order at the opening quote. Keep spot economics as audit evidence;
        # the execution monitor still checks the actual fill conditions.
        exit_contract = self._plan_exit_contract(plan) or {}
        target = exit_contract.get("weighted_target_price") or plan.target_price
        risk_reward = self._risk_reward_ratio(plan.stop_loss, target, entry_low, entry_high)
        spot_risk_reward = self._risk_reward_ratio(plan.stop_loss, target, price, price)
        stop = self._safe_price(plan.stop_loss)
        target_boundaries = [self._safe_price(plan.target_price)]
        target_boundaries.extend(
            self._safe_price(leg.get("target_price"))
            for leg in exit_contract.get("exit_legs", [])
        )
        valid_targets = [value for value in target_boundaries if value is not None]
        first_target = min(valid_targets) if valid_targets else None
        threshold = self.auction_price_deviation_pct
        factors: Dict[str, Any] = {
            "entry_band": {
                "observed_price": price,
                "entry_low": entry_low,
                "entry_high": entry_high,
                "deviation_pct": deviation_pct,
                "threshold_pct": threshold,
                "available": True,
                "position": "above" if price > entry_high else "below" if price < entry_low else "inside",
                "waiting_for_entry": price > entry_high,
                # Staying above the intended pullback buy zone is not by
                # itself evidence that the zone is invalid. Downward breaks
                # retain their existing conservative review threshold.
                "material": price < entry_low and deviation_pct >= threshold,
            },
            "prior_close_gap": {
                "prior_close": prior_close,
                "gap_pct": gap_pct,
                "threshold_pct": threshold,
                "available": gap_pct is not None,
                "material": gap_pct is not None and abs(gap_pct) >= threshold,
            },
            "opening_volume_ratio": {
                "ratio": volume_ratio,
                "threshold": self.auction_min_matched_volume_ratio,
                "available": volume_ratio is not None,
                "material": volume_ratio is not None and volume_ratio < self.auction_min_matched_volume_ratio,
            },
            "risk_reward": {
                "risk_reward": risk_reward,
                "basis": "planned_entry_range",
                "entry_reference_price": self._entry_reference_price(entry_low, entry_high),
                "target_reference_price": target,
                "spot_risk_reward": spot_risk_reward,
                "threshold": self.intraday_min_risk_reward,
                "available": risk_reward is not None,
                "material": risk_reward is None or risk_reward < self.intraday_min_risk_reward,
            },
            "risk_boundary": {
                "stop_loss": stop,
                "first_target": first_target,
                "stop_breached": stop is not None and price <= stop,
                "target_reached": first_target is not None and price >= first_target,
                "available": stop is not None and first_target is not None,
                "material": (stop is not None and price <= stop)
                or (first_target is not None and price >= first_target),
            },
        }
        material_reasons = [
            key for key, value in factors.items()
            if isinstance(value, Mapping) and value.get("material")
        ]
        return {
            "observed_price": price,
            "prior_close": prior_close,
            "entry_low": entry_low,
            "entry_high": entry_high,
            "gap_pct": gap_pct,
            "deviation_pct": deviation_pct,
            "volume_ratio": volume_ratio,
            "risk_reward": risk_reward,
            "risk_reward_basis": "planned_entry_range",
            "spot_risk_reward": spot_risk_reward,
            "material": bool(material_reasons),
            "material_reasons": material_reasons,
            "factors": factors,
        }, None

    @staticmethod
    def _realtime_quote_identity_error(*, plan, quote) -> Optional[str]:
        if quote is None:
            return None
        expected = canonical_paper_identity(plan.market, plan.stock_code)
        quote_code = str(getattr(quote, "code", "") or "").strip()
        quote_market = str(getattr(quote, "market", "") or plan.market).strip().lower()
        source = getattr(quote, "source", None)
        source_value = getattr(source, "value", source)
        if expected is None:
            return "invalid_plan_symbol_identity"
        if not quote_code:
            return "missing_realtime_quote_identity"
        actual = canonical_paper_identity(quote_market, quote_code)
        if actual != expected:
            return "realtime_quote_identity_mismatch"
        if not str(source_value or "").strip():
            return "missing_realtime_quote_source"
        return None

    @staticmethod
    def _preopen_fail_closed_reason(phase) -> str:
        phase_value = getattr(getattr(phase, "phase", None), "value", str(getattr(phase, "phase", "")))
        if str(phase.market or "").lower() == "cn" and phase_value in {"premarket", "closing_auction"}:
            return f"cn_continuous_session_required:{phase_value}"
        return f"market_phase_not_tradable:{phase_value}"

    def _process_preopen_plan(self, *, plan, manager, now: datetime, bucket: datetime, phase) -> Optional[Dict[str, Any]]:
        # Legacy 09:26 auction callbacks are no longer an active runtime path.
        # Historical preopen_auction rows remain readable via list APIs, but
        # execution uses only the CN 09:30-09:35 opening_confirmation sweep.
        return None
        market = str(getattr(phase, "market", "") or "").lower()
        phase_value = getattr(getattr(phase, "phase", None), "value", str(getattr(phase, "phase", "")))
        local_time = getattr(phase, "market_local_time", None)
        if market != "cn" or phase_value not in {"opening_auction", "premarket"} or not isinstance(local_time, datetime):
            return None
        if not (time(9, 15) <= local_time.time() < time(9, 30)):
            return None
        if not (time(9, 26) <= local_time.time() <= time(9, 29, 30)):
            reason = f"cn_continuous_session_required:{phase_value}_before_auction_publish_window"
            return self._persist_intraday_outcome(
                plan=plan,
                position=None,
                now=now,
                bucket=bucket,
                quote=None,
                result="failed_closed",
                action="none",
                reasons=[reason],
                evidence={
                    "mode": "preopen_auction",
                    "phase": self._phase_evidence(phase),
                    "policy": {
                        "dedicated_auction_snapshot_required": True,
                        "official_publish_window": "09:26:00-09:29:30 Asia/Shanghai",
                        "no_preopen_fill": True,
                        "ordinary_realtime_quote_forbidden": True,
                    },
                    "reasons": [reason],
                },
                required_passed=False,
            )
        snapshot = self._fetch_auction_snapshot(manager, plan.stock_code, plan.market)
        diagnostics = self._auction_provider_diagnostics(manager, plan.stock_code, plan.market)
        evidence = {
            "mode": "preopen_auction",
            "phase": self._phase_evidence(phase),
            "auction": self._auction_evidence(snapshot),
            "auction_capability_diagnostics": diagnostics,
            "policy": {
                "dedicated_auction_snapshot_required": True,
                "ordinary_realtime_quote_forbidden": True,
                "no_preopen_fill": True,
            },
        }
        validation_error = self._validate_auction_snapshot_for_plan(
            plan=plan,
            snapshot=snapshot,
            phase=phase,
            now=now,
        )
        if validation_error:
            request = self._ensure_reanalysis_request(
                plan=plan,
                trigger_type="auction_deviation",
                session_date=local_time.date(),
                due_at=self._utc_naive(now),
                observed_at=self._utc_naive(now),
                evidence={**evidence, "validation_error": validation_error},
                initial_status="unavailable",
                failure_reason=validation_error,
                submit=False,
            )
            return self._persist_intraday_outcome(
                plan=plan,
                position=None,
                now=now,
                bucket=bucket,
                quote=None,
                result="failed_closed",
                action="none",
                reasons=[validation_error],
                evidence={**evidence, "validation_error": validation_error, "reanalysis_request": request},
                required_passed=False,
            )
        deviation = self._auction_material_deviation(plan, snapshot)
        if not deviation:
            return self._persist_intraday_outcome(
                plan=plan,
                position=None,
                now=now,
                bucket=bucket,
                quote=None,
                result="auction_observed",
                action="none",
                reasons=["auction_snapshot_available_no_material_deviation"],
                evidence=evidence,
                required_passed=False,
            )
        data_unavailable_reasons = list(deviation.get("data_unavailable_reasons") or [])
        market_material_reasons = list(deviation.get("market_material_reasons") or [])
        trigger_type = "auction_data_unavailable" if data_unavailable_reasons and not market_material_reasons else "auction_deviation"
        request = self._ensure_reanalysis_request(
            plan=plan,
            trigger_type=trigger_type,
            session_date=local_time.date(),
            due_at=self._utc_naive(now),
            observed_at=self._utc_naive(now),
            evidence={**evidence, "material_deviation": deviation},
            initial_status="unavailable" if data_unavailable_reasons and not market_material_reasons else "pending",
            submit=not (data_unavailable_reasons and not market_material_reasons),
        )
        reason = (
            "auction_data_unavailable_reanalysis_blocked"
            if data_unavailable_reasons and not market_material_reasons
            else "auction_material_deviation_reanalysis_requested"
        )
        return self._persist_intraday_outcome(
            plan=plan,
            position=None,
            now=now,
            bucket=bucket,
            quote=None,
            result="reanalysis_blocked",
            action="none",
            reasons=[reason],
            evidence={**evidence, "material_deviation": deviation, "reanalysis_request": request},
            required_passed=False,
        )

    def _fetch_auction_snapshot(self, manager: Any, stock_code: str, market: str) -> Optional[Any]:
        if not hasattr(manager, "get_auction_snapshot"):
            return None
        try:
            return manager.get_auction_snapshot(stock_code, market=market)
        except TypeError:
            return manager.get_auction_snapshot(stock_code)
        except Exception:
            return None

    def _auction_provider_diagnostics(self, manager: Any, stock_code: str, market: str) -> List[Dict[str, Any]]:
        if not hasattr(manager, "get_auction_capability_diagnostics"):
            return [{"available": False, "reason": "manager_has_no_dedicated_auction_diagnostics"}]
        try:
            result = manager.get_auction_capability_diagnostics(stock_code, market=market)
        except TypeError:
            result = manager.get_auction_capability_diagnostics(stock_code)
        except Exception as exc:
            return [{"available": False, "reason": str(exc)[:160]}]
        return list(result or []) if isinstance(result, list) else [dict(result)] if isinstance(result, Mapping) else []

    @staticmethod
    def _auction_evidence(snapshot: Any) -> Dict[str, Any]:
        if snapshot is None:
            return {"available": False, "capability": "missing"}
        def safe_value(value):
            if isinstance(value, float) and not math.isfinite(value):
                return str(value)
            return value
        return {
            "available": True,
            "code": getattr(snapshot, "code", None),
            "market": getattr(snapshot, "market", None),
            "phase": getattr(snapshot, "phase", None),
            "source": getattr(snapshot, "source", None),
            "virtual_price": safe_value(getattr(snapshot, "virtual_price", None)),
            "matched_volume": safe_value(getattr(snapshot, "matched_volume", None)),
            "unmatched_volume": safe_value(getattr(snapshot, "unmatched_volume", None)),
            "unmatched_side": getattr(snapshot, "unmatched_side", None),
            "provider_timestamp": getattr(snapshot, "provider_timestamp", None),
            "fetched_at": getattr(snapshot, "fetched_at", None),
            "is_stale": getattr(snapshot, "is_stale", None),
            "stale_seconds": getattr(snapshot, "stale_seconds", None),
            "provenance": getattr(snapshot, "provenance", None),
        }

    def _validate_auction_snapshot_for_plan(self, *, plan, snapshot: Any, phase, now: datetime) -> Optional[str]:
        if snapshot is None:
            return "auction_snapshot_unavailable"
        if not getattr(snapshot, "has_attributable_data", lambda: False)():
            return "auction_snapshot_unattributable"
        snapshot_identity = canonical_paper_identity(
            str(getattr(snapshot, "market", "") or ""),
            str(getattr(snapshot, "code", "") or ""),
        )
        plan_identity = canonical_paper_identity(str(plan.market or ""), str(plan.stock_code or ""))
        if snapshot_identity is None or snapshot_identity != plan_identity:
            return "auction_snapshot_identity_mismatch"
        if str(getattr(snapshot, "phase", "") or "").lower() != "opening_auction":
            return "auction_snapshot_wrong_phase"
        if getattr(snapshot, "is_stale", None) is True:
            return "auction_snapshot_stale"
        price = self._safe_price(getattr(snapshot, "virtual_price", None))
        if price is None:
            return "auction_snapshot_invalid_virtual_price"
        matched_volume = self._safe_float(getattr(snapshot, "matched_volume", None))
        if matched_volume is None or matched_volume < 0:
            return "auction_snapshot_invalid_matched_volume"
        provenance = getattr(snapshot, "provenance", None)
        if not isinstance(provenance, Mapping) or not provenance:
            return "auction_snapshot_missing_provenance"
        provenance_fields = provenance.get("fields") if isinstance(provenance.get("fields"), Mapping) else provenance
        tushare_stk_auction = provenance.get("api_name") == "stk_auction"
        required_fields = ("virtual_price", "matched_volume", "trade_date") if tushare_stk_auction else ("virtual_price", "matched_volume", "provider_timestamp")
        for field in required_fields:
            if not provenance_fields.get(field):
                return "auction_snapshot_missing_provenance_fields"
        market = str(getattr(phase, "market", "") or plan.market or "").lower()
        tz = self._market_timezone(market)
        local_session_date = self._phase_session_date(phase)
        now_local = now.astimezone(tz) if now.tzinfo else now.replace(tzinfo=timezone.utc).astimezone(tz)
        now_utc = now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
        max_future_skew = timedelta(seconds=QUOTE_CLOCK_SKEW_SECONDS)
        parsed_times: Dict[str, datetime] = {}
        tushare_stk_auction = isinstance(provenance, Mapping) and provenance.get("api_name") == "stk_auction"
        labels = ("fetched_at",) if tushare_stk_auction else ("provider_timestamp", "fetched_at")
        for label in labels:
            raw_value = getattr(snapshot, label, None)
            parsed = self._parse_quote_datetime(raw_value)
            if parsed is None:
                return f"auction_snapshot_invalid_{label}"
            parsed_aware = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            parsed_utc = parsed_aware.astimezone(timezone.utc)
            parsed_times[label] = parsed_utc
            parsed_local = parsed_aware.astimezone(tz)
            if parsed_local.date() != local_session_date:
                return f"auction_snapshot_{label}_wrong_session"
            local_clock = parsed_local.time().replace(tzinfo=None)
            if tushare_stk_auction:
                if not (time(9, 26) <= local_clock <= time(9, 29, 30)):
                    return f"auction_snapshot_{label}_outside_official_publish_window"
            elif not (time(9, 26) <= local_clock <= time(9, 29, 30)):
                return f"auction_snapshot_{label}_outside_opening_auction"
            if parsed_local > now_local + max_future_skew:
                return f"auction_snapshot_{label}_future"
        if tushare_stk_auction:
            trade_date = str(provenance.get("trade_date") or "")
            if trade_date != local_session_date.strftime("%Y%m%d"):
                return "auction_snapshot_trade_date_wrong_session"
            if not provenance.get("official_publish_window"):
                return "auction_snapshot_missing_official_publish_window"
        else:
            provider_age = (now_utc - parsed_times["provider_timestamp"]).total_seconds()
            if provider_age < -QUOTE_CLOCK_SKEW_SECONDS:
                return "auction_snapshot_provider_timestamp_future"
            if provider_age > self.intraday_max_quote_stale_seconds:
                return "auction_snapshot_provider_timestamp_stale"
            fetch_lag = (parsed_times["fetched_at"] - parsed_times["provider_timestamp"]).total_seconds()
            if fetch_lag < -QUOTE_CLOCK_SKEW_SECONDS:
                return "auction_snapshot_fetched_before_provider"
            if fetch_lag > self.intraday_max_quote_stale_seconds:
                return "auction_snapshot_fetch_lag_stale"
        return None

    def _auction_material_deviation(self, plan, snapshot: Any) -> Optional[Dict[str, Any]]:
        price = self._safe_price(getattr(snapshot, "virtual_price", None))
        if price is None:
            return None
        factors: Dict[str, Any] = {}
        prior_close = None
        provenance = getattr(snapshot, "provenance", None)
        provenance_map = provenance if isinstance(provenance, Mapping) else {}
        api_name = str(provenance_map.get("api_name") or "").lower()
        try:
            with self.db.get_session() as session:
                daily_rows = list(session.execute(
                    select(StockDaily)
                    .where(StockDaily.code.in_(stock_daily_variants(plan.market, plan.stock_code)))
                    .order_by(desc(StockDaily.date))
                    .limit(max(2, VOLUME_POLICY_SESSIONS + 1))
                ).scalars().all())
            if daily_rows:
                prior_close = self._safe_price(daily_rows[0].close)
        except Exception:
            prior_close = None
        rules, _ = self._json_list(getattr(plan, "rules_json", "[]"))
        price_rules = [rule for rule in rules if isinstance(rule, Mapping) and rule.get("type") in PRICE_RULE_TYPES]
        if not price_rules:
            return None
        lows = [self._safe_price(rule.get("low") if rule.get("type") == "close_between" else rule.get("value")) for rule in price_rules]
        highs = [self._safe_price(rule.get("high") if rule.get("type") == "close_between" else rule.get("value")) for rule in price_rules]
        bounds = [value for value in [*lows, *highs] if value is not None]
        if not bounds:
            return None
        low, high = min(bounds), max(bounds)
        deviation_pct = 0.0
        if price < low:
            deviation_pct = (low / price - 1.0) * 100.0
        elif price > high:
            deviation_pct = (price / high - 1.0) * 100.0
        factors["entry_band"] = {
            "virtual_price": price,
            "entry_low": low,
            "entry_high": high,
            "deviation_pct": deviation_pct,
            "threshold_pct": self.auction_price_deviation_pct,
            "material": deviation_pct >= self.auction_price_deviation_pct,
        }
        matched_volume = self._safe_float(getattr(snapshot, "matched_volume", None))
        official_volume_ratio = self._safe_float(provenance_map.get("volume_ratio"))
        volume_ratio = official_volume_ratio
        factors["matched_volume"] = {
            "matched_volume": matched_volume,
            "volume_ratio_source": "tushare_stk_auction.volume_ratio" if official_volume_ratio is not None else None,
            "ratio": volume_ratio,
            "threshold": self.auction_min_matched_volume_ratio,
            "material": volume_ratio is not None and volume_ratio < self.auction_min_matched_volume_ratio,
            "available": volume_ratio is not None,
            "unavailable_reason": None if volume_ratio is not None else "official_auction_volume_ratio_unavailable",
        }
        unmatched_volume = self._safe_float(getattr(snapshot, "unmatched_volume", None))
        unmatched_side = str(getattr(snapshot, "unmatched_side", "") or "").lower()
        imbalance_ratio = (
            unmatched_volume / matched_volume
            if unmatched_volume is not None and matched_volume and matched_volume > 0 else None
        )
        adverse_unmatched = unmatched_side in {"sell", "ask", "offer", "卖", "卖出"} and imbalance_ratio is not None and imbalance_ratio >= 1.0
        factors["unmatched_imbalance"] = {
            "unmatched_volume": unmatched_volume,
            "unmatched_side": unmatched_side or None,
            "imbalance_ratio": imbalance_ratio,
            "material": adverse_unmatched,
            "available": imbalance_ratio is not None,
            "unavailable_reason": (
                "stk_auction_does_not_provide_unmatched_imbalance"
                if api_name == "stk_auction" and imbalance_ratio is None else None
            ),
        }
        gap_pct = None
        limit_proximity = None
        limit_evidence = {"available": False, "reason": "limit_price_provider_unavailable"}
        if prior_close and prior_close > 0:
            gap_pct = (price / prior_close - 1.0) * 100.0
            trade_date = None
            if isinstance(provenance_map, Mapping):
                trade_date = str(provenance_map.get("trade_date") or "")
            if not trade_date:
                trade_date = self._market_local_date_for_time(plan.market, datetime.now(timezone.utc)).strftime("%Y%m%d")
            limit_prices = self._fetch_limit_prices_for_plan(plan, trade_date=trade_date)
            if limit_prices:
                limit_up = self._safe_price(limit_prices.get("up_limit"))
                limit_down = self._safe_price(limit_prices.get("down_limit"))
                if limit_up is not None and limit_down is not None and limit_up > 0 and limit_down > 0:
                    limit_proximity = min(abs(price / limit_up - 1.0), abs(price / limit_down - 1.0)) * 100.0
                    limit_evidence = {
                        "available": True,
                        "trade_date": limit_prices.get("trade_date"),
                        "up_limit": limit_up,
                        "down_limit": limit_down,
                        "provider": limit_prices.get("provider"),
                        "api_name": limit_prices.get("api_name"),
                        "fields": limit_prices.get("fields"),
                    }
        factors["prior_close_gap"] = {
            "prior_close": prior_close,
            "gap_pct": gap_pct,
            "available": gap_pct is not None,
            "material": gap_pct is not None and abs(gap_pct) >= self.auction_price_deviation_pct,
        }
        factors["limit_price_proximity"] = {
            **limit_evidence,
            "limit_proximity_pct": limit_proximity,
            "material": limit_proximity is not None and limit_proximity <= 0.5,
            "data_unavailable": not limit_evidence.get("available"),
        }
        rr = self._risk_reward_ratio(plan.stop_loss, plan.target_price, price, price)
        factors["risk_reward"] = {
            "risk_reward": rr,
            "threshold": self.intraday_min_risk_reward,
            "material": rr is None or rr < self.intraday_min_risk_reward,
        }
        market_material_reasons = [
            key for key, value in factors.items()
            if isinstance(value, Mapping) and value.get("material")
        ]
        data_unavailable_reasons = [
            key for key, value in factors.items()
            if isinstance(value, Mapping) and (
                value.get("data_unavailable")
                or (value.get("available") is False and key in {"matched_volume", "limit_price_proximity"})
            )
        ]
        if market_material_reasons or data_unavailable_reasons:
            return {
                "virtual_price": price,
                "entry_low": low,
                "entry_high": high,
                "deviation_pct": deviation_pct,
                "material_reasons": market_material_reasons,
                "market_material_reasons": market_material_reasons,
                "data_unavailable_reasons": data_unavailable_reasons,
                "factors": factors,
            }
        return None

    def _fetch_limit_prices_for_plan(self, plan, *, trade_date: str) -> Optional[Dict[str, Any]]:
        try:
            manager = DataFetcherManager()
            if not hasattr(manager, "get_limit_prices"):
                return None
            return manager.get_limit_prices(plan.stock_code, trade_date=trade_date, market=plan.market)
        except Exception:
            return None

    def _reanalysis_request_key(self, *, plan, trigger_type: str, session_date: date) -> str:
        raw = (
            f"{plan.source_model}|{plan.market}|{plan.stock_code}|"
            f"{session_date.isoformat()}|plan:{plan.id}|{trigger_type}"
        )
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
        return f"paper-reanalysis:{digest}"

    def _ensure_reanalysis_request(
        self,
        *,
        plan,
        trigger_type: str,
        session_date: date,
        due_at: Optional[datetime],
        observed_at: Optional[datetime],
        evidence: Mapping[str, Any],
        initial_status: str = "pending",
        failure_reason: Optional[str] = None,
        submit: bool = False,
    ) -> Optional[Dict[str, Any]]:
        key = self._reanalysis_request_key(
            plan=plan,
            trigger_type=trigger_type,
            session_date=session_date,
        )
        claimed_request_id: Optional[int] = None
        with self._ledger_write_session() as session:
            existing = session.execute(
                select(PaperReanalysisRequest)
                .where(PaperReanalysisRequest.request_key == key)
                .limit(1)
            ).scalar_one_or_none()
            if existing is not None:
                if submit and existing.status == "pending":
                    claimed_at = utc_naive_now()
                    result = session.execute(
                        update(PaperReanalysisRequest)
                        .where(
                            PaperReanalysisRequest.id == existing.id,
                            PaperReanalysisRequest.status == "pending",
                        )
                        .values(
                            status="claimed",
                            claimed_at=claimed_at,
                            updated_at=claimed_at,
                        )
                    )
                    session.flush()
                    if int(result.rowcount or 0) == 1:
                        claimed_request_id = int(existing.id)
                    else:
                        session.expire(existing)
                        return self._serialize_reanalysis_request(existing)
                else:
                    return self._serialize_reanalysis_request(existing)
                session.refresh(existing)
                serialized = self._serialize_reanalysis_request(existing)
            else:
                db_plan = session.get(PaperExecutionPlan, int(plan.id))
                if db_plan is None:
                    return None
                rate_limit = self._reanalysis_rate_limit_decision(
                    session,
                    plan=db_plan,
                    trigger_type=trigger_type,
                    session_date=session_date,
                    observed_at=observed_at or due_at,
                    exclude_request_key=key,
                )
                if rate_limit.get("blocked"):
                    blocked_at = utc_naive_now()
                    request = PaperReanalysisRequest(
                        request_key=key,
                        trigger_type=trigger_type,
                        status="blocked",
                        source_plan_id=db_plan.id,
                        source_report_id=db_plan.source_report_id,
                        decision_signal_id=db_plan.decision_signal_id,
                        source_model=db_plan.source_model,
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        stock_name=db_plan.stock_name,
                        session_date=session_date,
                        due_at=due_at,
                        observed_at=observed_at,
                        claimed_at=None,
                        failure_reason=str(rate_limit.get("reason") or "reanalysis_rate_limited")[:128],
                        evidence_json=self._json_dumps({
                            **dict(evidence),
                            "reanalysis_rate_limit": rate_limit,
                        }),
                        updated_at=blocked_at,
                    )
                    session.add(request)
                    session.flush()
                    return self._serialize_reanalysis_request(request)
                request_status = "claimed" if submit and initial_status == "pending" else initial_status
                request = PaperReanalysisRequest(
                    request_key=key,
                    trigger_type=trigger_type,
                    status=request_status,
                    source_plan_id=db_plan.id,
                    source_report_id=db_plan.source_report_id,
                    decision_signal_id=db_plan.decision_signal_id,
                    source_model=db_plan.source_model,
                    market=db_plan.market,
                    stock_code=db_plan.stock_code,
                    stock_name=db_plan.stock_name,
                    session_date=session_date,
                    due_at=due_at,
                    observed_at=observed_at,
                    claimed_at=utc_naive_now() if request_status == "claimed" else None,
                    failure_reason=failure_reason,
                    evidence_json=self._json_dumps(dict(evidence)),
                )
                session.add(request)
                session.flush()
                if request_status == "claimed":
                    claimed_request_id = int(request.id)
                serialized = self._serialize_reanalysis_request(request)
        if claimed_request_id is None:
            return serialized
        return self._submit_claimed_reanalysis_request(claimed_request_id) or serialized

    def _reanalysis_rate_limit_decision(
        self,
        session,
        *,
        plan,
        trigger_type: str,
        session_date: date,
        observed_at: Optional[datetime],
        exclude_request_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        settings = self._style_settings_for_plan(plan)
        try:
            cooldown_minutes = max(0, int(
                settings.get("llm_event_cooldown_minutes")
                if settings.get("llm_event_cooldown_minutes") is not None
                else settings.get("reanalysis_cooldown_minutes", self.reanalysis_cooldown_minutes)
            ))
        except (TypeError, ValueError):
            cooldown_minutes = max(0, int(self.reanalysis_cooldown_minutes))
        try:
            daily_cap = max(0, int(settings.get("llm_daily_cap", 1)))
        except (TypeError, ValueError):
            daily_cap = 1
        now = self._utc_naive(observed_at or datetime.now(timezone.utc))
        statuses = ["pending", "claimed", "submitted", "completed"]
        filters = [
            PaperReanalysisRequest.source_model == plan.source_model,
            PaperReanalysisRequest.market == plan.market,
            PaperReanalysisRequest.stock_code == plan.stock_code,
            PaperReanalysisRequest.session_date == session_date,
            PaperReanalysisRequest.status.in_(statuses),
        ]
        if exclude_request_key:
            filters.append(PaperReanalysisRequest.request_key != exclude_request_key)
        all_rows = list(session.execute(
            select(PaperReanalysisRequest)
            .where(and_(*filters))
            .order_by(desc(PaperReanalysisRequest.created_at), desc(PaperReanalysisRequest.id))
        ).scalars().all())
        event_rows = [row for row in all_rows if row.trigger_type == trigger_type]
        style = getattr(plan, "trading_style", None) or "position"
        base = {
            "policy_version": TRADING_STYLE_POLICY_VERSION,
            "trading_style": style,
            "trigger_type": trigger_type,
            "cooldown_minutes": cooldown_minutes,
            "daily_cap": daily_cap,
            "counted_statuses": statuses,
            "existing_count": len(all_rows),
            "existing_event_count": len(event_rows),
        }
        if daily_cap > 0 and len(all_rows) >= daily_cap:
            return {**base, "blocked": True, "reason": "reanalysis_daily_cap_exceeded"}
        if cooldown_minutes > 0 and event_rows:
            latest = event_rows[0]
            latest_at = (
                latest.submitted_at
                or latest.claimed_at
                or latest.observed_at
                or latest.created_at
            )
            latest_at = self._utc_naive(latest_at)
            if latest_at is not None and (now - latest_at).total_seconds() < cooldown_minutes * 60:
                return {
                    **base,
                    "blocked": True,
                    "reason": "reanalysis_cooldown_active",
                    "latest_request_id": latest.id,
                    "latest_status": latest.status,
                    "latest_at": latest_at.isoformat(),
                }
        return {**base, "blocked": False, "reason": None}

    def _submit_claimed_reanalysis_request(self, request_id: int) -> Optional[Dict[str, Any]]:
        with self._ledger_write_session() as session:
            request = session.get(PaperReanalysisRequest, int(request_id))
            if request is None:
                return None
            if request.status != "claimed" or request.task_id:
                return self._serialize_reanalysis_request(request)
            db_plan = session.get(PaperExecutionPlan, int(request.source_plan_id))
            if db_plan is None:
                request.status = "failed_closed"
                request.failure_reason = "source_plan_missing_before_submit"
                request.updated_at = utc_naive_now()
                session.flush()
                return self._serialize_reanalysis_request(request)
            source_model = str(db_plan.source_model or "").strip()
            stock_code = str(db_plan.stock_code or "").strip()
            stock_name = db_plan.stock_name
            source_plan_id = int(db_plan.id)
            # 同会话重分析沿用源计划冻结的风格，避免机会池短线计划被按账户风格重写。
            source_trading_style = str(db_plan.trading_style or "").strip()
            session_date = request.session_date
            trigger_type = request.trigger_type
        holding_context = self.analysis_context_for_stock(
            source_model=source_model,
            stock_code=stock_code,
        )
        if not isinstance(holding_context, dict):
            with self._ledger_write_session() as session:
                request = session.get(PaperReanalysisRequest, int(request_id))
                if request is None:
                    return None
                if request.status == "claimed":
                    request.status = "failed_closed"
                    request.failure_reason = "paper_holding_context_unavailable_before_submit"
                    request.updated_at = utc_naive_now()
                    session.flush()
                return self._serialize_reanalysis_request(request)
        payload = {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "original_query": (
                    f"paper_trading_reanalysis:{trigger_type}:"
                    f"source_plan={source_plan_id}:session={session_date.isoformat()}"
                ),
                "selection_source": None,
                "query_source": "paper_trading_reanalysis",
                "report_type": "detailed",
                "analysis_phase": "auto",
                "force_refresh": True,
                "model": source_model,
                "portfolio_context": {
                    **holding_context,
                    "disable_cross_model_fallback": True,
                    "paper_trading": {
                        "same_session_reanalysis": True,
                        "source_plan_id": source_plan_id,
                        "trigger_type": trigger_type,
                        "session_date": session_date.isoformat(),
                        "fallback_disabled": True,
                        **({"trading_style": source_trading_style} if source_trading_style else {}),
                    },
                },
            }
        try:
            from src.services.task_queue import get_task_queue

            task = get_task_queue().submit_task(**payload)
            task_id = getattr(task, "task_id", None)
            with self._ledger_write_session() as session:
                request = session.get(PaperReanalysisRequest, int(request_id))
                if request is None:
                    return None
                if request.status != "claimed":
                    return self._serialize_reanalysis_request(request)
                request.status = "submitted"
                request.task_id = task_id
                request.query_id = task_id
                request.submitted_at = utc_naive_now()
                request.updated_at = utc_naive_now()
                session.flush()
                return self._serialize_reanalysis_request(request)
        except Exception as exc:
            existing_task_id = getattr(exc, "existing_task_id", None)
            with self._ledger_write_session() as session:
                request = session.get(PaperReanalysisRequest, int(request_id))
                if request is None:
                    return None
                if existing_task_id:
                    request.status = "submitted"
                    request.task_id = existing_task_id
                    request.query_id = existing_task_id
                    request.submitted_at = utc_naive_now()
                    request.updated_at = utc_naive_now()
                else:
                    request.status = "failed_closed"
                    request.failure_reason = str(exc)[:128]
                    request.updated_at = utc_naive_now()
                session.flush()
                return self._serialize_reanalysis_request(request)

    def reconcile_reanalysis_requests(self, *, current_time: Optional[datetime] = None) -> Dict[str, int]:
        """Reconcile durable reanalysis requests without blind resubmission."""

        now = self._utc_naive(current_time or datetime.now(timezone.utc))
        cutoff = now - timedelta(seconds=REANALYSIS_RECONCILE_GRACE_SECONDS)
        summary = {"completed": 0, "failed_closed": 0, "unchanged": 0}
        try:
            from src.services.task_queue import TaskStatus, get_task_queue
        except Exception:
            TaskStatus = None  # type: ignore[assignment]
            get_task_queue = None  # type: ignore[assignment]
        queue = get_task_queue() if get_task_queue is not None else None
        with self._ledger_write_session() as session:
            requests = list(session.execute(
                select(PaperReanalysisRequest)
                .where(PaperReanalysisRequest.status.in_(["claimed", "submitted"]))
                .order_by(PaperReanalysisRequest.created_at, PaperReanalysisRequest.id)
            ).scalars().all())
            for request in requests:
                reference_time = request.submitted_at or request.claimed_at or request.created_at
                expired = bool(reference_time and reference_time < cutoff)
                report = self._find_reanalysis_report_for_request(session, request)
                replacement_plan = None
                if report is not None:
                    replacement_plan = self._find_reanalysis_replacement_plan_for_request(session, request, report)
                    if replacement_plan is not None:
                        request.status = "completed"
                        request.replacement_report_id = report.id
                        request.replacement_plan_id = replacement_plan.id
                        request.completed_at = now
                        request.updated_at = now
                        summary["completed"] += 1
                        continue
                    if not expired:
                        summary["unchanged"] += 1
                        continue
                    request.status = "failed_closed"
                    request.failure_reason = "matching_report_without_replacement_plan"
                    request.updated_at = now
                    summary["failed_closed"] += 1
                    continue
                task = None
                if request.task_id and queue is not None and hasattr(queue, "get_task"):
                    try:
                        task = queue.get_task(request.task_id)
                    except Exception:
                        task = None
                task_status = getattr(task, "status", None)
                status_value = getattr(task_status, "value", task_status)
                if status_value == (getattr(TaskStatus, "COMPLETED", None).value if TaskStatus is not None else "completed"):
                    if not expired:
                        summary["unchanged"] += 1
                        continue
                    request.status = "failed_closed"
                    request.failure_reason = "completed_task_without_matching_report"
                    request.updated_at = now
                    summary["failed_closed"] += 1
                    continue
                if status_value in {"failed", "cancelled"}:
                    request.status = "failed_closed"
                    request.failure_reason = f"task_{status_value}"
                    request.updated_at = now
                    summary["failed_closed"] += 1
                    continue
                if expired:
                    request.status = "failed_closed"
                    request.failure_reason = "reanalysis_request_unrecoverable_after_restart"
                    request.updated_at = now
                    summary["failed_closed"] += 1
                    continue
                summary["unchanged"] += 1
            session.flush()
        return summary

    def _find_reanalysis_report_for_request(self, session, request: PaperReanalysisRequest) -> Optional[AnalysisHistory]:
        query_ids = [value for value in [request.task_id, request.query_id] if value]
        if not query_ids:
            return None
        query = (
            select(AnalysisHistory)
            .where(
                AnalysisHistory.model_used == request.source_model,
                AnalysisHistory.code.in_(stock_daily_variants(request.market, request.stock_code) + [request.stock_code]),
            )
            .order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id))
        )
        query = query.where(AnalysisHistory.query_id.in_(query_ids))
        return session.execute(query.limit(1)).scalar_one_or_none()

    def _find_reanalysis_replacement_plan_for_request(
        self,
        session,
        request: PaperReanalysisRequest,
        report: AnalysisHistory,
    ) -> Optional[PaperExecutionPlan]:
        return session.execute(
            select(PaperExecutionPlan)
            .where(
                PaperExecutionPlan.source_report_id == report.id,
                PaperExecutionPlan.source_model == request.source_model,
                PaperExecutionPlan.market == request.market,
                PaperExecutionPlan.stock_code == request.stock_code,
            )
            .order_by(desc(PaperExecutionPlan.created_at), desc(PaperExecutionPlan.id))
            .limit(1)
        ).scalar_one_or_none()

    def _unresolved_reanalysis_request(self, plan) -> Optional[PaperReanalysisRequest]:
        with self.db.get_session() as session:
            return session.execute(
                select(PaperReanalysisRequest)
                .where(
                    PaperReanalysisRequest.source_plan_id == int(plan.id),
                    PaperReanalysisRequest.status.in_(["pending", "claimed", "submitted"]),
                )
                .order_by(desc(PaperReanalysisRequest.created_at), desc(PaperReanalysisRequest.id))
                .limit(1)
            ).scalar_one_or_none()

    def _consume_due_next_check_obligation(self, *, plan, now: datetime, phase) -> Optional[PaperReanalysisRequest]:
        obligation = self._next_check_obligation_from_plan(plan)
        if not obligation:
            return None
        try:
            due_at = datetime.fromisoformat(str(obligation.get("due_at")))
        except (TypeError, ValueError):
            return None
        now_naive = self._utc_naive(now)
        session_date = self._phase_session_date(phase) or plan.valid_for_date or now_naive.date()
        if due_at > now_naive:
            return None
        return self._ensure_reanalysis_request(
            plan=plan,
            trigger_type="next_check",
            session_date=session_date,
            due_at=due_at,
            observed_at=now_naive,
            evidence={
                "mode": "next_check",
                "obligation": obligation,
                "phase": self._phase_evidence(phase),
            },
            initial_status="pending",
            submit=True,
        )

    def _evaluate_intraday_position_groups(self, positions, quote, prior_bars, now, bucket, phase):
        outcomes: List[Dict[str, Any]] = []
        groups: Dict[Tuple[str, str, str, int, str], List[Any]] = {}
        for position in positions:
            groups.setdefault((
                str(position.source_model or ""),
                str(position.market or ""),
                str(position.stock_code or ""),
                int(position.capital_account_id or 0),
                str(position.currency or self._currency_for_market(position.market)),
            ), []).append(position)

        for group_positions in groups.values():
            group_positions.sort(key=lambda item: (
                item.opened_at or datetime.min,
                item.opened_on or date.min,
                item.id,
            ))
            for position in group_positions:
                outcome = self._evaluate_intraday_position(
                    position, quote, prior_bars, now, bucket, phase, allow_model_signal=False
                )
                if outcome is not None:
                    outcomes.append(outcome)
            aggregate = self._evaluate_aggregate_defensive_signal(
                group_positions, quote, now, bucket, phase
            )
            if aggregate is not None:
                outcomes.append(aggregate)
        return outcomes

    def _evaluate_aggregate_defensive_signal(self, positions, quote, now, bucket, phase):
        open_positions = [position for position in positions if getattr(position, "status", None) == "open"]
        if not open_positions:
            return None
        first = open_positions[0]
        local_date = phase.market_local_time.date()
        earliest = min(
            open_positions,
            key=lambda item: (item.opened_at or datetime.max, item.opened_on or date.max, item.id),
        )
        signal = self.repo.latest_defensive_signal(
            market=first.market,
            stock_code=first.stock_code,
            source_model=first.source_model,
            after_date=earliest.opened_on,
            through_date=local_date,
            after_time=getattr(earliest, "opened_at", None),
            execution_scope=self.execution_scope,
        )
        if signal is None:
            return None
        report = self.repo.get_report(int(signal.source_report_id)) if getattr(signal, "source_report_id", None) else None
        self._ensure_defensive_signal_strategy_transition(
            signal,
            report=report,
            evaluated_at=self._utc_naive(now),
            invalid_reason="intraday_model_exit_without_entry_authority",
        )
        eligibility = self._holding_signal_eligibility(
            signal,
            report=report,
            evaluated_at=self._utc_naive(now),
        )
        if not eligibility["eligible"]:
            return None
        action_value = eligibility["execution_action"]
        if action_value in {"sell", "reduce", "avoid"}:
            eligibility = self._defensive_signal_eligibility(
                signal,
                report=report,
                evaluated_at=self._utc_naive(now),
            )
            if not eligibility["eligible"]:
                return None
            action_value = eligibility["execution_action"]
        risk_stop, risk_target = self._risk_revision_values_from_eligibility(eligibility)
        has_risk_revision = risk_stop is not None or risk_target is not None
        if action_value not in {"sell", "reduce"} and not has_risk_revision:
            return None
        price, quote_error = self._fresh_quote_price(quote, observed_at=now)
        if quote_error:
            plan = self.repo.get_plan(first.plan_id)
            if plan is None:
                return None
            return self._persist_intraday_outcome(
                plan=plan,
                position=first,
                now=now,
                bucket=bucket,
                quote=quote,
                result="failed_closed",
                action="none",
                reasons=[quote_error],
                evidence={
                    "mode": "intraday",
                    "phase": self._phase_evidence(phase),
                    "quote": self._quote_evidence(quote),
                    "aggregate_defensive_signal": {
                        "signal_id": signal.id,
                        "action": action_value,
                        "position_ids": [position.id for position in open_positions],
                    },
                },
                required_passed=False,
            )
        assert price is not None
        close_reason = (
            "model_sell" if action_value == "sell"
            else "model_reduce" if action_value == "reduce"
            else "risk_revision"
        )
        if close_reason == "risk_revision":
            return self._persist_aggregate_defensive_exit(
                positions=open_positions,
                signal=signal,
                close_reason=close_reason,
                quote_price=price,
                quote=quote,
                now=now,
                bucket=bucket,
                phase=phase,
                signal_evidence=eligibility,
            )
        desired_quantity = self._aggregate_close_quantity_for_reason(open_positions, close_reason)
        resolution = self.resolve_paper_intents(
            candidates=[{
                "kind": close_reason,
                "action": "close",
                "reason": close_reason,
                "price": price,
                "signal_id": signal.id,
                "requested_quantity": desired_quantity,
                "pending_semantics": "sticky_full_exit" if close_reason == "model_sell" else "sticky_partial_exit",
            }],
            gates={"fail_closed": False},
            state={
                "entry": "intraday_aggregate_defensive",
                "position_ids": [position.id for position in open_positions],
                "bucket": self._utc_naive(bucket).isoformat(),
            },
        )
        chosen = resolution.get("chosen")
        if not chosen:
            return None
        close_reason = str(chosen.get("reason") or close_reason)
        return self._persist_aggregate_defensive_exit(
            positions=open_positions,
            signal=signal,
            close_reason=close_reason,
            quote_price=price,
            quote=quote,
            now=now,
            bucket=bucket,
            phase=phase,
            signal_evidence={**eligibility, "intent_resolution": resolution},
        )

    def _holding_signal_eligibility(
        self,
        signal,
        *,
        report: Optional[AnalysisHistory],
        evaluated_at: datetime,
    ) -> Dict[str, Any]:
        top_level_action = normalize_decision_action(getattr(signal, "action", None))
        metadata, metadata_error = self._json_object(getattr(signal, "metadata_json", None))
        holding_state = normalize_holding_state(metadata.get("holding_state"))
        effective_action_source = str(metadata.get("effective_action_source") or "").strip()
        base_action = normalize_decision_action(metadata.get("base_action"))
        holding_snapshot = self._holding_action_snapshot(metadata)
        holding_action = normalize_decision_action(holding_snapshot.get("action"))
        # The persisted signal action is the account-effective report-time
        # action.  The alternate holding branch remains audit evidence and
        # must never become operational for an empty/unknown report.
        execution_action = top_level_action
        reasons: List[str] = []
        if getattr(signal, "source_type", None) != "analysis":
            reasons.append("signal_not_analysis_source")
        if getattr(signal, "source_report_id", None) is None or report is None:
            reasons.append("missing_source_report")
        if getattr(signal, "status", None) != "active":
            reasons.append("signal_not_active")
        expires_at = getattr(signal, "expires_at", None)
        if expires_at is not None and expires_at <= evaluated_at:
            reasons.append("signal_expired")
        if holding_state != "holding":
            reasons.append("report_time_holding_state_not_holding")
        if (
            effective_action_source == "position_actions.holding"
            and holding_action != top_level_action
        ):
            reasons.append("effective_holding_action_mismatch")
        if holding_state == "unknown" and holding_action not in {
            "sell", "reduce", "avoid", "hold", "watch", "alert", "buy", "add"
        }:
            reasons.append("missing_or_invalid_holding_action")
        plan_quality = str(getattr(signal, "plan_quality", "") or "").lower()
        if plan_quality not in {"complete", "partial"}:
            reasons.append("insufficient_plan_quality")
        score_action = action_for_score(getattr(signal, "score", None))
        if score_action is None:
            reasons.append("missing_or_invalid_top_level_score")
        elif score_action is not None and top_level_action != score_action:
            resolved_holding_branch = (
                holding_state == "holding"
                and effective_action_source == "position_actions.holding"
                and holding_action == top_level_action
                and base_action == score_action
            )
            if not resolved_holding_branch:
                reasons.append("score_action_mismatch")
        if metadata_error:
            reasons.append("invalid_signal_metadata")
        confidence = getattr(signal, "confidence", None)
        try:
            confidence_value = float(confidence)
        except (TypeError, ValueError):
            confidence_value = None
            reasons.append("invalid_confidence")
        if confidence_value is not None and not math.isfinite(confidence_value):
            reasons.append("invalid_confidence")
        if confidence_value is not None and confidence_value < 0.4:
            reasons.append("confidence_below_trade_floor")
        if execution_action == "sell" and confidence_value is not None and 0.4 <= confidence_value < 0.6:
            execution_action = "reduce"
        return {
            "eligible": not reasons,
            "reasons": reasons,
            "signal_id": getattr(signal, "id", None),
            "action": top_level_action,
            "base_action": base_action,
            "holding_state": holding_state,
            "effective_action_source": effective_action_source or None,
            "holding_action": holding_action,
            "execution_action": execution_action,
            "holding_snapshot": holding_snapshot,
            "score": getattr(signal, "score", None),
            "score_action": score_action,
            "plan_quality": plan_quality,
            "confidence": confidence_value,
            "confidence_evidence": (
                "low" if confidence_value is not None and confidence_value < 0.6 else "normal"
            ),
        }

    def _defensive_signal_eligibility(
        self,
        signal,
        *,
        report: Optional[AnalysisHistory],
        evaluated_at: datetime,
    ) -> Dict[str, Any]:
        eligibility = self._holding_signal_eligibility(
            signal,
            report=report,
            evaluated_at=evaluated_at,
        )
        if eligibility.get("execution_action") not in {"sell", "reduce", "avoid"}:
            reasons = list(eligibility.get("reasons") or [])
            reasons.append("effective_action_not_defensive")
            eligibility["reasons"] = reasons
            eligibility["eligible"] = False
        if eligibility.get("eligible") and self.strategy_thesis_enabled:
            source_model = str(
                getattr(report, "model_used", None)
                or getattr(signal, "source_agent", None)
                or ""
            ).strip()
            transition_authorized = (
                report is not None
                and self._strategy_transition_allows_risk_revision(
                    report=report,
                    signal=signal,
                    source_model=source_model,
                    market=str(getattr(signal, "market", "") or ""),
                    stock_code=str(getattr(signal, "stock_code", "") or ""),
                )
            )
            eligibility["strategy_thesis_transition"] = {
                "policy_version": STRATEGY_THESIS_POLICY_VERSION,
                "risk_revision_authorized": bool(transition_authorized),
                "source_report_id": getattr(signal, "source_report_id", None),
                "decision_signal_id": getattr(signal, "id", None),
                "source_model": source_model,
                "market": str(getattr(signal, "market", "") or ""),
                "stock_code": str(getattr(signal, "stock_code", "") or ""),
                "reason": "existing_strategy_transition_required_for_model_exit",
            }
            if not transition_authorized:
                reasons = list(eligibility.get("reasons") or [])
                reasons.append("strategy_thesis_not_confirmed_for_model_exit")
                eligibility["reasons"] = reasons
                eligibility["eligible"] = False
        return eligibility

    def _resolution_bucket(
        self,
        *,
        mode: str,
        source_model: str,
        market: str,
        stock_code: str,
        currency: Optional[str],
        capital_account_id: Optional[int],
        bucket_started_at: Optional[datetime] = None,
        evaluation_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        if bucket_started_at is None and evaluation_date is not None:
            bucket_started_at = datetime.combine(evaluation_date, time.min)
        bucket_value = self._utc_naive(bucket_started_at).isoformat() if bucket_started_at else None
        identity = canonical_paper_identity(market, stock_code)
        canonical_code = identity[1] if identity is not None else str(stock_code or "")
        return {
            "policy_version": PLAN_VERSION,
            "mode": str(mode or "intraday"),
            "capital_account_id": int(capital_account_id) if capital_account_id else None,
            "source_model": str(source_model or ""),
            "market": str(market or "").lower(),
            "canonical_stock_code": canonical_code,
            "currency": currency or self._currency_for_market(market),
            "bucket_started_at": bucket_value,
            "evaluation_date": evaluation_date.isoformat() if evaluation_date else None,
        }

    def _resolution_key(self, bucket: Mapping[str, Any]) -> str:
        payload = {
            "policy_version": str(bucket.get("policy_version") or PLAN_VERSION),
            "capital_account_id": bucket.get("capital_account_id"),
            "source_model": str(bucket.get("source_model") or ""),
            "market": str(bucket.get("market") or "").lower(),
            "canonical_stock_code": str(bucket.get("canonical_stock_code") or bucket.get("stock_code") or ""),
            "currency": bucket.get("currency"),
            "evaluation_mode": str(bucket.get("mode") or bucket.get("evaluation_mode") or "intraday"),
            "bucket_started_at": str(bucket.get("bucket_started_at") or ""),
        }
        return "res-" + hashlib.sha256(
            self._json_dumps(payload).encode("utf-8")
        ).hexdigest()[:32]

    def _risk_revision_key(
        self,
        position: PaperPosition,
        *,
        source_report_id: Optional[int],
        decision_signal_id: Optional[int],
        new_stop_loss: Optional[float],
        new_target_price: Optional[float],
        evaluated_at: datetime,
        source: str,
    ) -> str:
        key_seed = (
            f"{position.id}|{source_report_id}|{decision_signal_id}|"
            f"{new_stop_loss}|{new_target_price}|{source}"
        )
        return "risk-" + hashlib.sha256(key_seed.encode("utf-8")).hexdigest()[:32]

    def _persist_resolution(
        self,
        session,
        *,
        resolution_key: str,
        bucket: Mapping[str, Any],
        status: str,
        selected_intent: Optional[str],
        candidate_intents: Sequence[Mapping[str, Any]],
        suppressed_intents: Optional[Sequence[Mapping[str, Any]]] = None,
        selected_intent_payload: Optional[Mapping[str, Any]] = None,
        source_report_id: Optional[int] = None,
        decision_signal_id: Optional[int] = None,
        source_identities: Optional[Sequence[Mapping[str, Any]]] = None,
        requested_quantity: Optional[float] = None,
        executed_quantity: Optional[float] = None,
        unfilled_quantity: Optional[float] = None,
        evidence: Optional[Mapping[str, Any]] = None,
    ) -> PaperResolution:
        row = session.execute(
            select(PaperResolution)
            .where(PaperResolution.resolution_key == resolution_key)
            .limit(1)
        ).scalar_one_or_none()
        now = utc_naive_now()
        if row is None:
            row = PaperResolution(
                resolution_key=resolution_key,
                policy_version=str(bucket.get("policy_version") or PLAN_VERSION),
                mode=str(bucket.get("mode") or "intraday"),
                capital_account_id=bucket.get("capital_account_id"),
                source_model=str(bucket.get("source_model") or ""),
                market=str(bucket.get("market") or ""),
                stock_code=str(bucket.get("canonical_stock_code") or bucket.get("stock_code") or ""),
                currency=bucket.get("currency"),
                bucket_started_at=(
                    datetime.fromisoformat(str(bucket["bucket_started_at"]))
                    if bucket.get("bucket_started_at") else None
                ),
                evaluation_date=(
                    date.fromisoformat(str(bucket["evaluation_date"]))
                    if bucket.get("evaluation_date") else None
                ),
                created_at=now,
            )
            session.add(row)
        row.status = str(status or "candidate")[:32]
        row.selected_intent = str(selected_intent)[:32] if selected_intent else None
        row.source_report_id = source_report_id
        row.decision_signal_id = decision_signal_id
        row.requested_quantity = requested_quantity
        row.executed_quantity = executed_quantity
        row.unfilled_quantity = unfilled_quantity
        row.candidate_intents_json = self._json_dumps(list(candidate_intents or []))
        row.suppressed_intents_json = self._json_dumps(list(suppressed_intents or []))
        row.selected_intent_json = self._json_dumps(dict(selected_intent_payload or {}))
        row.source_identities_json = self._json_dumps(list(source_identities or []))
        row.evidence_json = self._json_dumps(dict(evidence or {}))
        row.updated_at = now
        session.flush()
        return row

    def _resolution_fields_for_event(
        self,
        *,
        resolution_key: Optional[str],
        logical_order_key: Optional[str],
        fill: Optional[Mapping[str, Any]] = None,
        quote: Optional[Any] = None,
    ) -> Dict[str, Any]:
        fill = fill or {}
        return {
            "requested_quantity": fill.get("requested_quantity", fill.get("quantity")),
            "executed_quantity": fill.get("executed_quantity", fill.get("quantity")),
            "unfilled_quantity": fill.get("unfilled_quantity", 0.0),
            "resolution_key": resolution_key,
            "logical_order_key": logical_order_key,
            "instrument_assumption": fill.get("instrument_assumption"),
            "quote_provider_timestamp": str(getattr(quote, "provider_timestamp", "") or fill.get("quote_provider_timestamp") or "")[:64] or None,
            "quote_fetched_at": str(getattr(quote, "fetched_at", "") or fill.get("quote_fetched_at") or "")[:64] or None,
        }

    def _record_risk_revision(
        self,
        session,
        position: PaperPosition,
        *,
        source_report_id: Optional[int],
        decision_signal_id: Optional[int],
        new_stop_loss: Optional[float],
        new_target_price: Optional[float],
        evaluated_at: datetime,
        current_price: Optional[float] = None,
        style_geometry_evidence: Optional[Mapping[str, Any]] = None,
        source: str = "model_signal",
    ) -> Optional[PaperRiskRevision]:
        old_stop = self._safe_price(getattr(position, "stop_loss", None))
        old_target = self._safe_price(getattr(position, "target_price", None))
        proposed_stop = self._safe_price(new_stop_loss)
        proposed_target = self._safe_price(new_target_price)
        evaluated_naive = self._utc_naive(evaluated_at)
        evidence = {
            "source": source,
            "old_stop_loss": old_stop,
            "new_stop_loss": proposed_stop,
            "old_target_price": old_target,
            "new_target_price": proposed_target,
            "current_price": current_price,
            "opened_at": position.opened_at.isoformat() if position.opened_at else None,
            "opened_on": position.opened_on.isoformat() if position.opened_on else None,
        }
        revision_key = self._risk_revision_key(
            position,
            source_report_id=source_report_id,
            decision_signal_id=decision_signal_id,
            new_stop_loss=proposed_stop,
            new_target_price=proposed_target,
            evaluated_at=evaluated_at,
            source=source,
        )
        existing = session.execute(
            select(PaperRiskRevision)
            .where(PaperRiskRevision.revision_key == revision_key)
            .limit(1)
        ).scalar_one_or_none()
        if existing is not None:
            return existing
        reason = "noop_equal"
        result = "noop"
        apply_stop = old_stop
        apply_target = old_target
        effective_stop = proposed_stop if proposed_stop is not None else old_stop
        effective_target = proposed_target if proposed_target is not None else old_target
        if proposed_stop is None and proposed_target is None:
            reason = "invalid_missing_risk_values"
            result = "rejected"
        elif getattr(position, "opened_at", None) and evaluated_naive <= position.opened_at:
            reason = "pre_entry_revision_rejected"
            result = "rejected"
        elif (
            effective_stop is not None
            and effective_target is not None
            and effective_stop >= effective_target
        ):
            reason = "contradictory_exit_prices_rejected"
            result = "rejected"
        elif proposed_stop is not None and old_stop is not None and proposed_stop < old_stop:
            reason = "lower_stop_loss_rejected"
            result = "rejected"
        else:
            validation = validate_risk_revision_geometry(
                current_price=current_price,
                old_stop_loss=old_stop,
                proposed_stop_loss=proposed_stop,
                proposed_target_price=proposed_target,
            )
            evidence["geometry_validation"] = validation.evidence
            if not validation.passed:
                reason = validation.reason
                result = "rejected"
            elif proposed_stop == old_stop and proposed_target == old_target:
                reason = "noop_equal"
                result = "noop"
            elif proposed_stop is not None and current_price is not None:
                style_geometry = style_geometry_evidence if isinstance(style_geometry_evidence, Mapping) else {}
                risk_guard = entry_geometry_guard(
                    style=str(style_geometry.get("trading_style") or getattr(position, "trading_style", None) or self.trading_style),
                    style_settings=(
                        style_geometry.get("settings")
                        if isinstance(style_geometry.get("settings"), Mapping)
                        else self._style_settings_for_plan(position)
                    ),
                    indicator_snapshot=(
                        style_geometry.get("indicator_snapshot")
                        if isinstance(style_geometry.get("indicator_snapshot"), Mapping)
                        else None
                    ),
                    entry_reference=current_price,
                    stop_loss=proposed_stop,
                )
                evidence["risk_revision_geometry_guard"] = risk_guard
                if not bool(risk_guard.get("passed")):
                    reason = "risk_revision_geometry_guard_rejected:" + ",".join(
                        str(item) for item in risk_guard.get("reasons", []) if item
                    )
                    result = "rejected"
                else:
                    reason = "accepted"
                    result = "applied"
                    if proposed_stop is not None:
                        apply_stop = proposed_stop
                        position.stop_loss = proposed_stop
                    if proposed_target is not None:
                        apply_target = proposed_target
                        position.target_price = proposed_target
                    position.updated_at = utc_naive_now()
            else:
                reason = "accepted"
                result = "applied"
                if proposed_stop is not None:
                    apply_stop = proposed_stop
                    position.stop_loss = proposed_stop
                if proposed_target is not None:
                    apply_target = proposed_target
                    position.target_price = proposed_target
                position.updated_at = utc_naive_now()

        existing_versions = session.execute(
            select(PaperRiskRevision.id)
            .where(PaperRiskRevision.position_id == position.id)
            .order_by(desc(PaperRiskRevision.id))
        ).scalars().all()
        version = len(existing_versions) + 1
        row = PaperRiskRevision(
            revision_key=revision_key,
            policy_version=PLAN_VERSION,
            position_id=position.id,
            plan_id=position.plan_id,
            source_report_id=source_report_id,
            decision_signal_id=decision_signal_id,
            source_model=position.source_model,
            market=position.market,
            stock_code=position.stock_code,
            revision_version=version,
            result=result,
            reason=reason,
            old_stop_loss=old_stop,
            new_stop_loss=proposed_stop,
            applied_stop_loss=apply_stop,
            old_target_price=old_target,
            new_target_price=proposed_target,
            applied_target_price=apply_target,
            evaluated_at=evaluated_naive,
            evidence_json=self._json_dumps(evidence),
        )
        session.add(row)
        session.flush()
        return row

    def _risk_revision_values_from_eligibility(self, eligibility: Mapping[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        snapshot = eligibility.get("holding_snapshot") if isinstance(eligibility, Mapping) else None
        if not isinstance(snapshot, Mapping):
            return None, None
        stop = (
            snapshot.get("stop_loss")
            or snapshot.get("stop")
            or snapshot.get("defensive_stop")
            or snapshot.get("risk_stop")
        )
        target = snapshot.get("take_profit") or snapshot.get("target_price") or snapshot.get("target")
        return self._safe_price(stop), self._safe_price(target)

    def _risk_revision_style_geometry_evidence(self, signal, position: PaperPosition) -> Dict[str, Any]:
        metadata, _ = self._json_object(getattr(signal, "metadata_json", None) or "{}")
        frozen = metadata.get("paper_trading_style_evidence") if isinstance(metadata, Mapping) else None
        if isinstance(frozen, Mapping):
            return {
                "source": frozen.get("source") or "signal_frozen_style_evidence",
                "trading_style": frozen.get("trading_style") or getattr(position, "trading_style", None) or self.trading_style,
                "settings": dict(frozen.get("settings")) if isinstance(frozen.get("settings"), Mapping) else self._style_settings_for_plan(position),
                "indicator_snapshot": (
                    dict(frozen.get("indicator_snapshot"))
                    if isinstance(frozen.get("indicator_snapshot"), Mapping)
                    else None
                ),
            }
        indicators = metadata.get("paper_trading_style_indicators") if isinstance(metadata, Mapping) else None
        return {
            "source": "position_style_fallback",
            "trading_style": getattr(position, "trading_style", None) or self.trading_style,
            "settings": self._style_settings_for_plan(position),
            "indicator_snapshot": dict(indicators) if isinstance(indicators, Mapping) else None,
        }

    def _risk_revision_reference_price_in_session(
        self,
        session,
        position: PaperPosition,
        *,
        signal,
        evaluated_at: datetime,
    ) -> Dict[str, Any]:
        """Resolve the auditable price used to reject implicit immediate exits."""

        market = str(getattr(position, "market", "") or "")
        stock_code = str(getattr(position, "stock_code", "") or "")
        mark = self._valuation_mark_for_identity(
            session,
            source_model=str(getattr(position, "source_model", "") or ""),
            market=market,
            stock_code=stock_code,
            current_time=evaluated_at,
        )
        evaluated_naive = self._utc_naive(evaluated_at)
        mark_observed_at = mark.get("observed_at") if isinstance(mark, Mapping) else None
        if (
            mark is not None
            and mark.get("price") is not None
            and not mark.get("stale")
            and (
                not isinstance(mark_observed_at, datetime)
                or self._utc_naive(mark_observed_at) <= evaluated_naive
            )
        ):
            return {
                "price": float(mark["price"]),
                "source": mark.get("source") or "fresh_valuation_mark",
                "observed_at": mark.get("observed_at").isoformat() if mark.get("observed_at") else None,
                "fresh": True,
            }
        signal_phase = str(getattr(signal, "market_phase", "") or "").strip().lower()
        if signal_phase == "intraday":
            return {
                "price": None,
                "source": "unavailable",
                "fresh": False,
                "validated_price": False,
                "reason": "intraday_risk_revision_requires_fresh_quote",
                "stale_mark": dict(mark) if isinstance(mark, Mapping) else None,
            }
        signal_session_date = self._signal_session_date(signal)
        report_date = signal_session_date or (
            self._market_local_date_for_time(market, evaluated_at)
            if isinstance(evaluated_at, datetime)
            else None
        )
        evaluated_aware = evaluated_at if evaluated_at.tzinfo else evaluated_at.replace(tzinfo=timezone.utc)
        expected_complete_date = self._latest_proven_complete_date(market, evaluated_aware)
        if expected_complete_date is None:
            return {
                "price": None,
                "source": "unavailable",
                "fresh": False,
                "validated_price": False,
                "reason": "latest_complete_session_unavailable",
                "stale_mark": dict(mark) if isinstance(mark, Mapping) else None,
            }
        if isinstance(report_date, date):
            daily = self._latest_complete_daily_mark(
                session,
                market=market,
                stock_code=stock_code,
                through=report_date,
            )
            if daily is not None and self._safe_float(getattr(daily, "close", None)) is not None:
                observed_at = self._daily_close_observed_at(market, daily.date)
                if daily.date != expected_complete_date:
                    return {
                        "price": None,
                        "source": "unavailable",
                        "fresh": False,
                        "validated_price": False,
                        "reason": "report_asof_daily_close_not_latest_complete_session",
                        "as_of_date": daily.date.isoformat(),
                        "expected_complete_date": expected_complete_date.isoformat(),
                    }
                if observed_at > evaluated_naive:
                    return {
                        "price": None,
                        "source": "unavailable",
                        "fresh": False,
                        "validated_price": False,
                        "reason": "report_asof_daily_close_not_observed_at_evaluation_time",
                        "as_of_date": daily.date.isoformat(),
                        "observed_at": observed_at.isoformat(),
                    }
                return {
                    "price": float(daily.close),
                    "source": "report_asof_stock_daily_close",
                    "observed_at": observed_at.isoformat(),
                    "fresh": False,
                    "as_of_date": daily.date.isoformat(),
                    "expected_complete_date": expected_complete_date.isoformat(),
                    "validated_price": True,
                }
        return {
            "price": None,
            "source": "unavailable",
            "fresh": False,
            "validated_price": False,
            "stale_mark": dict(mark) if isinstance(mark, Mapping) else None,
        }

    def _apply_signal_risk_revisions(
        self,
        session,
        *,
        positions: Sequence[PaperPosition],
        signal,
        eligibility: Mapping[str, Any],
        evaluated_at: datetime,
        current_price: Optional[float],
    ) -> List[Dict[str, Any]]:
        stop, target = self._risk_revision_values_from_eligibility(eligibility)
        revisions: List[Dict[str, Any]] = []
        if stop is None and target is None:
            return revisions
        for position in positions:
            reference = self._risk_revision_reference_price_in_session(
                session,
                position,
                signal=signal,
                evaluated_at=evaluated_at,
            )
            resolved_current_price = (
                current_price
                if current_price is not None
                else self._safe_price(reference.get("price"))
            )
            if resolved_current_price is None:
                revisions.append({
                    "id": None,
                    "position_id": position.id,
                    "existing": False,
                    "result": "deferred",
                    "reason": str(reference.get("reason") or "missing_validated_reference_price_deferred"),
                    "old_stop_loss": getattr(position, "stop_loss", None),
                    "new_stop_loss": stop,
                    "applied_stop_loss": getattr(position, "stop_loss", None),
                    "old_target_price": getattr(position, "target_price", None),
                    "new_target_price": target,
                    "applied_target_price": getattr(position, "target_price", None),
                    "reference_price": reference,
                    "retryable": True,
                })
                continue
            revision_key = self._risk_revision_key(
                position,
                source_report_id=getattr(signal, "source_report_id", None),
                decision_signal_id=getattr(signal, "id", None),
                new_stop_loss=stop,
                new_target_price=target,
                evaluated_at=evaluated_at,
                source="eligible_post_entry_signal",
            )
            existing = session.execute(
                select(PaperRiskRevision)
                .where(PaperRiskRevision.revision_key == revision_key)
                .limit(1)
            ).scalar_one_or_none()
            if existing is not None:
                revisions.append({
                    "id": existing.id,
                    "position_id": position.id,
                    "existing": True,
                    "result": existing.result,
                    "reason": existing.reason,
                    "old_stop_loss": existing.old_stop_loss,
                    "new_stop_loss": existing.new_stop_loss,
                    "applied_stop_loss": existing.applied_stop_loss,
                    "old_target_price": existing.old_target_price,
                    "new_target_price": existing.new_target_price,
                    "applied_target_price": existing.applied_target_price,
                })
                continue
            before_stop = getattr(position, "stop_loss", None)
            row = self._record_risk_revision(
                session,
                position,
                source_report_id=getattr(signal, "source_report_id", None),
                decision_signal_id=getattr(signal, "id", None),
                new_stop_loss=stop,
                new_target_price=target,
                evaluated_at=evaluated_at,
                current_price=resolved_current_price,
                style_geometry_evidence=self._risk_revision_style_geometry_evidence(signal, position),
                source="eligible_post_entry_signal",
            )
            revisions.append({
                "id": row.id if row else None,
                "position_id": position.id,
                "existing": False,
                "result": row.result if row else None,
                "reason": row.reason if row else None,
                "old_stop_loss": before_stop,
                "new_stop_loss": row.new_stop_loss if row else None,
                "applied_stop_loss": row.applied_stop_loss if row else None,
                "old_target_price": row.old_target_price if row else None,
                "new_target_price": row.new_target_price if row else None,
                "applied_target_price": row.applied_target_price if row else None,
                "reference_price": reference,
            })
        if any(item.get("result") == "applied" for item in revisions):
            sync = self._sync_thesis_effective_risk_lines_in_session(
                session,
                positions=positions,
                evaluated_at=evaluated_at,
            )
            if sync:
                for item in revisions:
                    if item.get("result") == "applied":
                        item["thesis_risk_sync"] = sync
        return revisions

    def _sync_thesis_effective_risk_lines_in_session(
        self,
        session,
        *,
        positions: Sequence[PaperPosition],
        evaluated_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        open_positions = [
            position
            for position in positions
            if getattr(position, "status", None) == "open"
        ]
        if not open_positions:
            return None
        first = open_positions[0]
        scope = {
            "source_model": str(getattr(first, "source_model", "") or ""),
            "market": str(getattr(first, "market", "") or ""),
            "stock_code": str(getattr(first, "stock_code", "") or ""),
            "trading_style": self._normalize_trading_style(getattr(first, "trading_style", None) or self.trading_style),
        }
        if not all(
            str(getattr(position, "source_model", "") or "") == scope["source_model"]
            and str(getattr(position, "market", "") or "") == scope["market"]
            and str(getattr(position, "stock_code", "") or "") == scope["stock_code"]
            and self._normalize_trading_style(getattr(position, "trading_style", None) or self.trading_style) == scope["trading_style"]
            for position in open_positions
        ):
            return {
                "result": "skipped",
                "reason": "mixed_position_scope",
                "position_ids": [position.id for position in open_positions],
            }
        open_positions = list(session.execute(
            select(PaperPosition)
            .where(
                PaperPosition.source_model == scope["source_model"],
                PaperPosition.market == scope["market"],
                PaperPosition.stock_code == scope["stock_code"],
                PaperPosition.trading_style == scope["trading_style"],
                PaperPosition.status == "open",
            )
            .order_by(PaperPosition.id)
        ).scalars().all())
        if not open_positions:
            return None
        thesis_key = self._strategy_thesis_key(**scope)
        thesis = session.execute(
            select(PaperStrategyThesis)
            .where(PaperStrategyThesis.thesis_key == thesis_key)
            .limit(1)
        ).scalar_one_or_none()
        if thesis is None:
            return {
                "result": "skipped",
                "reason": "strategy_thesis_not_found",
                "thesis_key": thesis_key,
            }

        def unique_prices(attr: str) -> List[float]:
            values: List[float] = []
            for position in open_positions:
                price = self._safe_price(getattr(position, attr, None))
                if price is None:
                    continue
                if not any(math.isclose(price, existing, rel_tol=0.0, abs_tol=1e-9) for existing in values):
                    values.append(price)
            return values

        stops = unique_prices("stop_loss")
        targets = unique_prices("target_price")
        per_lot = [
            {
                "position_id": position.id,
                "stop_loss": self._safe_price(getattr(position, "stop_loss", None)),
                "target_price": self._safe_price(getattr(position, "target_price", None)),
            }
            for position in open_positions
        ]
        evidence, _ = self._json_object(getattr(thesis, "evidence_json", None) or "{}")
        sync_event = {
            "result": "per_lot_only",
            "reason": "risk_lines_differ_across_lots",
            "position_ids": [position.id for position in open_positions],
            "per_lot_risk_lines": per_lot,
            "evaluated_at": self._utc_naive(evaluated_at).isoformat(),
        }
        if len(stops) <= 1 and len(targets) <= 1:
            if stops:
                thesis.stop_loss = stops[0]
            if targets:
                thesis.target_price = targets[0]
            thesis.updated_at = utc_naive_now()
            sync_event = {
                **sync_event,
                "result": "synced",
                "reason": "all_open_lots_share_effective_risk_lines",
                "synced_stop_loss": stops[0] if stops else None,
                "synced_target_price": targets[0] if targets else None,
            }
        evidence["risk_revision_sync"] = sync_event
        thesis.evidence_json = self._json_dumps(evidence)
        session.flush()
        return sync_event

    def _persist_aggregate_defensive_exit(
        self,
        *,
        positions,
        signal,
        close_reason: str,
        quote_price: float,
        quote,
        now: datetime,
        bucket: datetime,
        phase,
        signal_evidence: Mapping[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if not positions:
            return None
        observed_naive = self._utc_naive(now)
        bucket_naive = self._utc_naive(bucket)
        event_date = phase.market_local_time.date()
        first = positions[0]
        account_id = int(getattr(first, "capital_account_id", 0) or 0)
        currency = str(getattr(first, "currency", None) or self._currency_for_market(first.market))
        resolution_bucket = self._resolution_bucket(
            mode="intraday_position",
            source_model=first.source_model,
            market=first.market,
            stock_code=first.stock_code,
            currency=currency,
            capital_account_id=account_id or None,
            bucket_started_at=bucket_naive,
        )
        resolution_key = self._resolution_key(resolution_bucket)
        logical_seed = f"{resolution_key}|signal:{signal.id}|intent:{close_reason}"
        logical_order_key = "agg-" + hashlib.sha256(logical_seed.encode("utf-8")).hexdigest()[:24]
        observation_key = f"{logical_order_key}:observation"
        consumption_key = (
            f"{account_id or 'none'}|{first.source_model}|{first.market}|{first.stock_code}|"
            f"{currency}|{signal.id}|{close_reason}"
        )

        with self._ledger_write_session() as session:
            existing = session.execute(
                select(PaperPlanObservation)
                .where(PaperPlanObservation.observation_key == observation_key)
                .limit(1)
            ).scalar_one_or_none()
            if existing is not None:
                return None
            consumed = session.execute(
                select(PaperSignalConsumption)
                .where(PaperSignalConsumption.consumption_key == consumption_key)
                .limit(1)
            ).scalar_one_or_none()
            if consumed is not None:
                return None
            db_positions = list(session.execute(
                select(PaperPosition)
                .where(
                    PaperPosition.source_model == first.source_model,
                    PaperPosition.market == first.market,
                    PaperPosition.stock_code == first.stock_code,
                    PaperPosition.capital_account_id == account_id if account_id else PaperPosition.capital_account_id.is_(None),
                    PaperPosition.status == "open",
                )
                .order_by(PaperPosition.opened_at, PaperPosition.opened_on, PaperPosition.id)
            ).scalars().all())
            if not db_positions:
                return None
            signal_created_at = getattr(signal, "created_at", None)
            eligible_positions = [
                position for position in db_positions
                if self._position_opened_before_signal(position, signal_created_at)
                and not getattr(position, "pending_exit_reason", None)
            ]
            if not eligible_positions:
                return None
            risk_revisions = self._apply_signal_risk_revisions(
                session,
                positions=eligible_positions,
                signal=signal,
                eligibility=signal_evidence,
                evaluated_at=observed_naive,
                current_price=quote_price,
            )
            breached_revisions = [
                item for item in risk_revisions
                if item.get("result") in {"applied", "applied_breached"}
                and item.get("applied_stop_loss") is not None
                and quote_price <= float(item["applied_stop_loss"])
            ]
            reached_target_revisions = [
                item for item in risk_revisions
                if item.get("result") in {"applied", "applied_breached"}
                and item.get("applied_target_price") is not None
                and quote_price >= float(item["applied_target_price"])
            ]
            if breached_revisions:
                close_reason = "model_risk_stop_breached"
            elif reached_target_revisions:
                close_reason = "model_risk_target_reached"
            elif close_reason == "risk_revision":
                plan = session.get(PaperExecutionPlan, int(eligible_positions[0].plan_id))
                if plan is None:
                    return None
                applied = any(item.get("result") == "applied" for item in risk_revisions)
                rejected = any(item.get("result") == "rejected" for item in risk_revisions)
                result = "risk_revised" if applied else "risk_revision_rejected" if rejected else "risk_revision_noop"
                evidence = {
                    "mode": "intraday",
                    "phase": self._phase_evidence(phase),
                    "quote": self._quote_evidence(quote),
                    "aggregate_holding_signal": {
                        **dict(signal_evidence),
                        "risk_revisions": risk_revisions,
                        "policy": "same_model_post_entry_versioned_risk_revision",
                        "eligible_position_ids": [position.id for position in eligible_positions],
                    },
                }
                self._persist_resolution(
                    session,
                    resolution_key=resolution_key,
                    bucket=resolution_bucket,
                    status=result,
                    selected_intent=None,
                    candidate_intents=[{
                        "intent": "risk_revision",
                        "source": "aggregate_holding_signal",
                        "signal_id": getattr(signal, "id", None),
                    }],
                    source_report_id=getattr(signal, "source_report_id", None),
                    decision_signal_id=getattr(signal, "id", None),
                    source_identities=[
                        {"kind": "position", "id": position.id}
                        for position in eligible_positions
                    ],
                    requested_quantity=0.0,
                    executed_quantity=0.0,
                    unfilled_quantity=0.0,
                    evidence=evidence,
                )
                observation = PaperPlanObservation(
                    observation_key=observation_key,
                    plan_id=plan.id,
                    position_id=None,
                    source_report_id=getattr(signal, "source_report_id", None),
                    decision_signal_id=getattr(signal, "id", None),
                    source_model=plan.source_model,
                    market=plan.market,
                    stock_code=plan.stock_code,
                    stock_name=plan.stock_name,
                    mode="intraday",
                    observed_at=observed_naive,
                    bucket_started_at=bucket_naive,
                    quote_price=quote_price,
                    quote_source=self._quote_source(quote),
                    quote_provider_timestamp=str(getattr(quote, "provider_timestamp", "") or "")[:64] or None,
                    quote_fetched_at=str(getattr(quote, "fetched_at", "") or "")[:64] or None,
                    resolution_key=resolution_key,
                    resolution_bucket_json=self._json_dumps(resolution_bucket),
                    logical_order_key=logical_order_key,
                    result=result,
                    action="hold",
                    required_passed=applied,
                    evidence_json=self._json_dumps({**evidence, "reasons": [result]}),
                )
                session.add(observation)
                session.flush()
                session.add(PaperSignalConsumption(
                    consumption_key=consumption_key,
                    signal_id=int(signal.id),
                    capital_account_id=account_id or None,
                    source_model=first.source_model,
                    market=first.market,
                    stock_code=first.stock_code,
                    currency=currency,
                    holding_intent_kind="risk_revision",
                    resolution_key=resolution_key,
                    logical_order_key=logical_order_key,
                ))
                session.flush()
                return {"result": result, "observation": self._serialize_observation(observation), "events": []}
            desired_quantity = self._aggregate_close_quantity_for_reason(eligible_positions, close_reason)
            candidate_intents = [{
                "intent": close_reason,
                "source": "aggregate_defensive_signal",
                "signal_id": getattr(signal, "id", None),
                "desired_quantity": desired_quantity,
                "eligible_position_ids": [position.id for position in eligible_positions],
            }]
            if desired_quantity <= 0:
                plan = session.get(PaperExecutionPlan, int(eligible_positions[0].plan_id))
                if plan is None:
                    return None
                self._persist_resolution(
                    session,
                    resolution_key=resolution_key,
                    bucket=resolution_bucket,
                    status="suppressed",
                    selected_intent=None,
                    candidate_intents=candidate_intents,
                    suppressed_intents=[{
                        "intent": close_reason,
                        "reason": "reduce_quantity_rounded_to_zero",
                        "desired_quantity": desired_quantity,
                    }],
                    source_report_id=plan.source_report_id,
                    decision_signal_id=plan.decision_signal_id,
                    requested_quantity=desired_quantity,
                    executed_quantity=0.0,
                    unfilled_quantity=desired_quantity,
                    evidence={"signal": dict(signal_evidence), "mode": "intraday"},
                )
                observation = PaperPlanObservation(
                    observation_key=observation_key,
                    plan_id=plan.id,
                    position_id=eligible_positions[0].id,
                    source_report_id=plan.source_report_id,
                    decision_signal_id=plan.decision_signal_id,
                    source_model=plan.source_model,
                    market=plan.market,
                    stock_code=plan.stock_code,
                    stock_name=plan.stock_name,
                    mode="intraday",
                    observed_at=observed_naive,
                    bucket_started_at=bucket_naive,
                    quote_price=quote_price,
                    quote_source=self._quote_source(quote),
                    quote_provider_timestamp=str(getattr(quote, "provider_timestamp", "") or "")[:64] or None,
                    quote_fetched_at=str(getattr(quote, "fetched_at", "") or "")[:64] or None,
                    resolution_key=resolution_key,
                    resolution_bucket_json=self._json_dumps(resolution_bucket),
                    logical_order_key=logical_order_key,
                    result="suppressed",
                    action="none",
                    required_passed=False,
	                    evidence_json=self._json_dumps({
                        "mode": "intraday",
                        "phase": self._phase_evidence(phase),
                        "quote": self._quote_evidence(quote),
                        "resolution_key": resolution_key,
                        "resolution_bucket": resolution_bucket,
	                        "aggregate_defensive_signal": {
	                            **dict(signal_evidence),
	                            "risk_revisions": risk_revisions,
	                            "policy": "partial_reduce_zero_quantity_suppressed",
                            "logical_order_key": logical_order_key,
                            "eligible_position_ids": [position.id for position in eligible_positions],
                        },
                        "reasons": ["reduce_quantity_rounded_to_zero"],
                    }),
                )
                session.add(observation)
                session.flush()
                return {"result": "suppressed", "observation": self._serialize_observation(observation), "events": []}
            sell_allocations, pending_allocations = self._aggregate_exit_allocations(
                eligible_positions,
                desired_quantity=desired_quantity,
                event_date=event_date,
            )
            plan = session.get(PaperExecutionPlan, int(eligible_positions[0].plan_id))
            if plan is None:
                return None
            evidence = {
                "mode": "intraday",
                "phase": self._phase_evidence(phase),
                "quote": self._quote_evidence(quote),
                    "aggregate_defensive_signal": {
                        **dict(signal_evidence),
                        "risk_revisions": risk_revisions,
                        "policy": "model_signal_applies_to_aggregate_symbol_holding",
                    "logical_order_key": logical_order_key,
                    "desired_quantity": desired_quantity,
                    "sellable_quantity": sum(quantity for _position, quantity in sell_allocations),
                    "pending_quantity": sum(quantity for _position, quantity in pending_allocations),
                    "eligible_position_ids": [position.id for position in eligible_positions],
                    "all_position_ids": [position.id for position in db_positions],
                },
            }
            result = "closed" if sell_allocations else "pending_exit"
            if sell_allocations and pending_allocations:
                result = "partially_closed_pending_exit"
            self._persist_resolution(
                session,
                resolution_key=resolution_key,
                bucket=resolution_bucket,
                status="selected",
                selected_intent=close_reason,
                candidate_intents=candidate_intents,
                selected_intent_payload={
                    "intent": close_reason,
                    "logical_order_key": logical_order_key,
                    "desired_quantity": desired_quantity,
                    "sellable_quantity": sum(quantity for _position, quantity in sell_allocations),
                    "pending_quantity": sum(quantity for _position, quantity in pending_allocations),
                },
                source_report_id=plan.source_report_id,
                decision_signal_id=plan.decision_signal_id,
                source_identities=[
                    {"kind": "position", "id": position.id, "opened_at": position.opened_at.isoformat() if position.opened_at else None}
                    for position in eligible_positions
                ],
                requested_quantity=desired_quantity,
                executed_quantity=0.0,
                unfilled_quantity=desired_quantity,
                evidence=evidence,
            )
            observation = PaperPlanObservation(
                observation_key=observation_key,
                plan_id=plan.id,
                position_id=None,
                source_report_id=plan.source_report_id,
                decision_signal_id=plan.decision_signal_id,
                source_model=plan.source_model,
                market=plan.market,
                stock_code=plan.stock_code,
                stock_name=plan.stock_name,
                mode="intraday",
                observed_at=observed_naive,
                bucket_started_at=bucket_naive,
                quote_price=quote_price,
                quote_source=self._quote_source(quote),
                quote_provider_timestamp=str(getattr(quote, "provider_timestamp", "") or "")[:64] or None,
                quote_fetched_at=str(getattr(quote, "fetched_at", "") or "")[:64] or None,
                resolution_key=resolution_key,
                resolution_bucket_json=self._json_dumps(resolution_bucket),
                logical_order_key=logical_order_key,
                result=result,
                action="reduce" if "reduce" in close_reason else "close",
                required_passed=True,
                evidence_json=self._json_dumps({**evidence, "reasons": [close_reason, "aggregate_model_defensive_signal"]}),
            )
            session.add(observation)
            session.flush()
            events: List[Dict[str, Any]] = []
            sold_quantity = sum(quantity for _position, quantity in sell_allocations)
            aggregate_fill = None
            if sold_quantity > 0:
                aggregate_fill, fill_error = self._fill_assumption(
                    side="sell",
                    quote_price=quote_price,
                    quantity=sold_quantity,
                    market=positions[0].market if positions else None,
                    stock_code=positions[0].stock_code if positions else None,
                    quote=quote,
                    observed_at=now,
                )
                if fill_error:
                    self._persist_resolution(
                        session,
                        resolution_key=resolution_key,
                        bucket=resolution_bucket,
                        status="failed_closed",
                        selected_intent=close_reason,
                        candidate_intents=candidate_intents,
                        selected_intent_payload={
                            "intent": close_reason,
                            "logical_order_key": logical_order_key,
                            "desired_quantity": desired_quantity,
                            "sellable_quantity": sum(quantity for _position, quantity in sell_allocations),
                            "pending_quantity": sum(quantity for _position, quantity in pending_allocations),
                        },
                        source_report_id=plan.source_report_id,
                        decision_signal_id=plan.decision_signal_id,
                        source_identities=[
                            {"kind": "position", "id": position.id, "opened_at": position.opened_at.isoformat() if position.opened_at else None}
                            for position in eligible_positions
                        ],
                        requested_quantity=desired_quantity,
                        executed_quantity=0.0,
                        unfilled_quantity=desired_quantity,
                        evidence={**evidence, "reasons": [fill_error]},
                    )
                    observation.result = "failed_closed"
                    observation.action = "none"
                    observation.evidence_json = self._json_dumps({**evidence, "reasons": [fill_error]})
                    session.flush()
                    return {"result": "failed_closed", "observation": self._serialize_observation(observation), "events": []}
                assert aggregate_fill is not None
                savepoint = session.begin_nested()
                mutation_error = None
                try:
                    for allocation_index, (position, quantity) in enumerate(sell_allocations, start=1):
                        account = session.get(PaperCapitalAccount, position.capital_account_id) if position.capital_account_id else None
                        fill_slice = self._fill_slice(
                            aggregate_fill,
                            quantity=quantity,
                            total_quantity=sold_quantity,
                            allocation_index=allocation_index,
                            allocation_count=len(sell_allocations),
                            prior_slices=[
                                json.loads(event_row.details_json)["allocated_fill"]
                                for event_row in session.execute(
                                    select(PaperTradeEvent)
                                    .where(PaperTradeEvent.logical_order_key == logical_order_key)
                                    .order_by(PaperTradeEvent.id)
                                ).scalars().all()
                            ],
                        )
                        fill_result, fill_error = self._apply_position_exit(
                            session,
                            account,
                            position,
                            quote_price=quote_price,
                            reason=close_reason,
                            event_date=event_date,
                            quote=quote,
                            requested_quantity=quantity,
                            requested_fraction=(
                                quantity / float(position.quantity or 0.0)
                                if float(position.quantity or 0.0) > 0 else None
                            ),
                            fill_override=fill_slice,
                            observed_at=now,
                        )
                        if fill_error:
                            mutation_error = fill_error
                            events = []
                            savepoint.rollback()
                            break
                        assert fill_result is not None
                        position.close_signal_id = signal.id
                        plan_row = session.get(PaperExecutionPlan, position.plan_id)
                        if plan_row is not None:
                            if not self._plan_has_live_lots_or_pending_legs(
                                session,
                                plan_row,
                                exclude_position_id=position.id if position.status == "closed" else None,
                            ):
                                plan_row.status = "closed"
                            else:
                                plan_row.status = "triggered"
                            plan_row.updated_at = utc_naive_now()
                        event_type = "reduce" if "reduce" in close_reason or position.status != "closed" else "close"
                        event_row = PaperTradeEvent(
                            event_key=f"{logical_order_key}:pos:{position.id}:fill",
                            plan_id=position.plan_id,
                            position_id=position.id,
                            source_report_id=position.source_report_id,
                            decision_signal_id=position.decision_signal_id,
                            source_model=position.source_model,
                            market=position.market,
                            stock_code=position.stock_code,
                            stock_name=position.stock_name,
                            event_type=event_type,
                            event_date=event_date,
                            price=fill_result["fill_price"],
                            quantity=fill_result["quantity"],
                            gross_amount=fill_result["gross_amount"],
                            fees=fill_result["fees"],
                            commission=fill_result.get("commission"),
                            tax=fill_result.get("tax", fill_result.get("sell_tax")),
                            slippage_amount=fill_result.get("slippage_amount"),
                            gross_realized_pnl=fill_result.get("realized_gross_pnl"),
                            net_realized_pnl=fill_result.get("realized_pnl"),
                            net_amount=fill_result["net_amount"],
                            fill_price=fill_result["fill_price"],
                            fill_evidence_grade=fill_result["evidence_grade"],
                            reason=close_reason,
                            **self._resolution_fields_for_event(
                                resolution_key=resolution_key,
                                logical_order_key=logical_order_key,
                                fill=fill_result,
                                quote=quote,
                            ),
                            triggering_signal_id=signal.id,
                            capital_account_id=account.id if account is not None else None,
                            currency=account.currency if account is not None else position.currency,
                            allocated_capital=fill_result["cost_reduced"],
                            cash_after=account.cash if account is not None else None,
                            exposure_after=account.exposure if account is not None else None,
                            realized_pnl_after=account.realized_pnl if account is not None else None,
                            details_json=self._json_dumps({
                                "simulated": True,
                                "logical_order_key": logical_order_key,
                                "logical_order_policy": "aggregate_once_fifo_allocated_to_lots",
                                "observation_id": observation.id,
                                "aggregate_fill": aggregate_fill,
                                "allocated_fill": fill_slice,
                                "allocation_index": allocation_index,
                                "allocation_count": len(sell_allocations),
                                "quantity": fill_result["quantity"],
                                "remaining_quantity": fill_result["remaining_quantity"],
                                "remaining_cost": fill_result["remaining_cost"],
                                "realized_pnl": fill_result["realized_pnl"],
                                "realized_gross_pnl": fill_result["realized_gross_pnl"],
                                "return_pct": position.simulated_return_pct,
                            }),
                        )
                        session.add(event_row)
                        session.flush()
                        self._ensure_lifecycle_dispatch(session, event_row, account=account, lifecycle="close")
                        events.append(self._serialize_event(event_row))
                    else:
                        savepoint.commit()
                except Exception:
                    if savepoint.is_active:
                        savepoint.rollback()
                    raise
                if mutation_error:
                    self._persist_resolution(
                        session,
                        resolution_key=resolution_key,
                        bucket=resolution_bucket,
                        status="failed_closed",
                        selected_intent=close_reason,
                        candidate_intents=candidate_intents,
                        selected_intent_payload={
                            "intent": close_reason,
                            "logical_order_key": logical_order_key,
                            "desired_quantity": desired_quantity,
                            "sellable_quantity": sum(quantity for _position, quantity in sell_allocations),
                            "pending_quantity": sum(quantity for _position, quantity in pending_allocations),
                        },
                        source_report_id=plan.source_report_id,
                        decision_signal_id=plan.decision_signal_id,
                        source_identities=[
                            {"kind": "position", "id": position.id, "opened_at": position.opened_at.isoformat() if position.opened_at else None}
                            for position in eligible_positions
                        ],
                        requested_quantity=desired_quantity,
                        executed_quantity=0.0,
                        unfilled_quantity=desired_quantity,
                        evidence={**evidence, "reasons": [mutation_error]},
                    )
                    observation.result = "failed_closed"
                    observation.action = "none"
                    observation.evidence_json = self._json_dumps({**evidence, "reasons": [mutation_error]})
                    session.flush()
                    return {"result": "failed_closed", "observation": self._serialize_observation(observation), "events": []}
            for position, quantity in pending_allocations:
                if quantity <= 0 or getattr(position, "pending_exit_reason", None):
                    continue
                account = session.get(PaperCapitalAccount, position.capital_account_id) if position.capital_account_id else None
                position.pending_exit_reason = close_reason
                position.pending_exit_on = event_date
                position.pending_exit_price = quote_price
                position.available_quantity = max(0.0, float(position.quantity or 0.0) - quantity)
                position.frozen_quantity = quantity
                position.updated_at = utc_naive_now()
                event_row = PaperTradeEvent(
                    event_key=f"{logical_order_key}:pos:{position.id}:pending",
                    plan_id=position.plan_id,
                    position_id=position.id,
                    source_report_id=position.source_report_id,
                    decision_signal_id=position.decision_signal_id,
                    source_model=position.source_model,
                    market=position.market,
                    stock_code=position.stock_code,
                    stock_name=position.stock_name,
                    event_type="pending_exit",
                    event_date=event_date,
                    price=quote_price,
                    quantity=quantity,
                    reason=close_reason,
                    requested_quantity=quantity,
                    executed_quantity=0.0,
                    unfilled_quantity=quantity,
                    resolution_key=resolution_key,
                    logical_order_key=logical_order_key,
                    instrument_assumption="paper_pending_t1_exit_no_fill",
                    quote_provider_timestamp=str(getattr(quote, "provider_timestamp", "") or "")[:64] or None,
                    quote_fetched_at=str(getattr(quote, "fetched_at", "") or "")[:64] or None,
                    triggering_signal_id=signal.id,
                    capital_account_id=account.id if account is not None else None,
                    currency=account.currency if account is not None else position.currency,
                    allocated_capital=self._position_allocated_capital(position),
                    cash_after=account.cash if account is not None else None,
                    exposure_after=account.exposure if account is not None else None,
                    realized_pnl_after=account.realized_pnl if account is not None else None,
                    details_json=self._json_dumps({
                        "simulated": True,
                        "logical_order_key": logical_order_key,
                        "logical_order_policy": "aggregate_once_fifo_allocated_to_lots",
                        "observation_id": observation.id,
                        "quantity": quantity,
                        "pending_fraction": quantity / float(position.quantity or 0.0) if float(position.quantity or 0.0) > 0 else None,
                        "fill_policy": "next_actual_session_fresh_quote",
                        "sellability": self._sellability_evidence(position, event_date),
                    }),
                )
                session.add(event_row)
                session.flush()
                position.pending_exit_event_id = event_row.id
                self._ensure_lifecycle_dispatch(session, event_row, account=account, lifecycle="pending_exit")
                events.append(self._serialize_event(event_row))
            if events:
                self._persist_resolution(
                    session,
                    resolution_key=resolution_key,
                    bucket=resolution_bucket,
                    status=result,
                    selected_intent=close_reason,
                    candidate_intents=candidate_intents,
                    selected_intent_payload={
                        "intent": close_reason,
                        "logical_order_key": logical_order_key,
                        "desired_quantity": desired_quantity,
                        "sellable_quantity": sum(quantity for _position, quantity in sell_allocations),
                        "pending_quantity": sum(quantity for _position, quantity in pending_allocations),
                    },
                    source_report_id=plan.source_report_id,
                    decision_signal_id=plan.decision_signal_id,
                    source_identities=[
                        {"kind": "position", "id": position.id, "opened_at": position.opened_at.isoformat() if position.opened_at else None}
                        for position in eligible_positions
                    ],
                    requested_quantity=desired_quantity,
                    executed_quantity=sum(quantity for _position, quantity in sell_allocations),
                    unfilled_quantity=sum(quantity for _position, quantity in pending_allocations),
                    evidence=evidence,
                )
                session.add(PaperSignalConsumption(
                    consumption_key=consumption_key,
                    signal_id=int(signal.id),
                    capital_account_id=account_id or None,
                    source_model=first.source_model,
                    market=first.market,
                    stock_code=first.stock_code,
                    currency=currency,
                    holding_intent_kind=close_reason,
                    resolution_key=resolution_key,
                    logical_order_key=logical_order_key,
                ))
                session.flush()
            return {
                "result": result,
                "observation": self._serialize_observation(observation),
                "events": events,
            }

    @staticmethod
    def _position_opened_before_signal(position: PaperPosition, signal_created_at: Optional[datetime]) -> bool:
        if signal_created_at is None:
            return False
        opened_at = getattr(position, "opened_at", None)
        if opened_at is None and getattr(position, "opened_on", None):
            opened_at = datetime.combine(position.opened_on, datetime.min.time())
        return opened_at is not None and signal_created_at > opened_at

    def _aggregate_close_quantity_for_reason(self, positions, reason: str) -> float:
        total = sum(max(0.0, float(position.quantity or 0.0)) for position in positions)
        if total <= 0:
            return 0.0
        if "reduce" not in str(reason or "").lower():
            return total
        lot_size = max(1, max(int(getattr(position, "lot_size", None) or self._lot_size_for_market(position.market)) for position in positions))
        raw = total * self.default_reduce_fraction
        quantity = math.floor(raw / lot_size) * lot_size
        return float(quantity)

    def _aggregate_exit_allocations(self, positions, *, desired_quantity: float, event_date: date):
        remaining = max(0.0, float(desired_quantity or 0.0))
        sell_allocations = []
        pending_allocations = []
        for position in positions:
            if remaining <= 1e-9:
                break
            quantity = float(position.quantity or 0.0)
            if quantity <= 0:
                continue
            sellable = self._sellable_quantity_for_aggregate(position, event_date)
            allocated = min(remaining, sellable)
            if allocated > 1e-9:
                sell_allocations.append((position, allocated))
                remaining -= allocated
        for position in positions:
            if remaining <= 1e-9:
                break
            quantity = float(position.quantity or 0.0)
            if quantity <= 0:
                continue
            already_sold = sum(qty for item, qty in sell_allocations if item.id == position.id)
            unavailable = max(0.0, quantity - already_sold - self._sellable_quantity_for_aggregate(position, event_date))
            allocated = min(remaining, unavailable)
            if allocated > 1e-9:
                pending_allocations.append((position, allocated))
                remaining -= allocated
        return sell_allocations, pending_allocations

    def _sellable_quantity_for_aggregate(self, position: PaperPosition, event_date: date) -> float:
        if self._can_close_position_on_date(position, event_date):
            return max(0.0, float(position.quantity or 0.0))
        # ``available_quantity`` is a cached display/ledger field and can be
        # stale after imports or older versions.  It must never override the
        # authoritative instrument sellability rule (notably CN A-share T+1).
        return 0.0

    @staticmethod
    def _instrument_type_for_cost(market: Optional[str], stock_code: Optional[str]) -> str:
        market_norm = str(market or "").strip().lower()
        code = str(stock_code or "").strip().lower()
        if "." in code:
            code = code.split(".", 1)[0]
        code = re.sub(r"^(sh|sz|bj)", "", code)
        if market_norm != "cn":
            return "non_cn"
        if code.startswith(("15", "16", "18", "50", "51", "52", "56", "58")):
            return "cn_etf_fund"
        if code.startswith(("0", "3", "4", "6", "8", "9")):
            return "cn_stock"
        return "cn_unknown"

    def _sell_tax_rate_for_instrument(self, side: str, market: Optional[str], stock_code: Optional[str]) -> float:
        if str(side or "").lower() != "sell":
            return 0.0
        instrument_type = self._instrument_type_for_cost(market, stock_code)
        if instrument_type == "cn_stock":
            return self.sell_tax_rate
        return 0.0

    @staticmethod
    def _fill_slice(
        fill: Mapping[str, Any],
        *,
        quantity: float,
        total_quantity: float,
        allocation_index: int = 1,
        allocation_count: int = 1,
        prior_slices: Optional[Sequence[Mapping[str, Any]]] = None,
    ) -> Dict[str, Any]:
        ratio = float(quantity) / float(total_quantity) if total_quantity else 0.0
        result = dict(fill)
        result["quantity"] = float(quantity)
        result["requested_quantity"] = float(quantity)
        result["executed_quantity"] = float(quantity)
        result["unfilled_quantity"] = 0.0
        prior_slices = list(prior_slices or [])
        for key in ("gross_amount", "commission", "sell_tax", "tax", "slippage_amount", "fees", "net_amount"):
            total_value = round(float(fill.get(key, 0.0) or 0.0), 2)
            if allocation_index >= allocation_count:
                allocated_before = round(sum(float(item.get(key, 0.0) or 0.0) for item in prior_slices), 2)
                result[key] = round(total_value - allocated_before, 2)
            else:
                result[key] = round(total_value * ratio, 2)
        result["allocation_index"] = allocation_index
        result["allocation_count"] = allocation_count
        return result

    def resolve_paper_intents(
        self,
        *,
        candidates: Sequence[Mapping[str, Any]],
        gates: Optional[Mapping[str, Any]] = None,
        state: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Resolve paper-trading intents through one versioned policy.

        Paths may collect market facts, quote facts, report facts, and candidate
        intents, but this function owns precedence and the final executable
        intent selection.
        """

        gates = dict(gates or {})
        state = dict(state or {})
        normalized: List[Dict[str, Any]] = []
        suppressed: List[Dict[str, Any]] = []
        priority = {
            "hard_stop": 10,
            "thesis_invalidation": 20,
            "pending_sticky_exit": 25,
            "model_sell": 30,
            "model_reduce": 40,
            "take_profit": 50,
            "trailing_profit": 55,
            "time_exit": 60,
            "add": 80,
            "entry": 90,
            "hold": 100,
            "alert": 110,
            "none": 120,
        }
        for index, raw in enumerate(candidates or []):
            item = dict(raw)
            kind = str(item.get("kind") or item.get("intent") or "none")
            item["kind"] = kind
            item["policy_priority"] = int(item.get("priority") or priority.get(kind, 1000))
            item["candidate_index"] = index
            if item.get("eligible", True):
                normalized.append(item)
            else:
                suppressed.append({**item, "suppressed_reason": item.get("reason") or "candidate_not_eligible"})
        if gates.get("fail_closed"):
            return {
                "policy_version": PLAN_VERSION,
                "chosen": None,
                "suppressed": [*suppressed, *normalized],
                "requested_quantity": 0.0,
                "pending_semantics": None,
                "reasons": list(gates.get("reasons") or ["gate_fail_closed"]),
                "state": state,
            }
        if not normalized:
            return {
                "policy_version": PLAN_VERSION,
                "chosen": None,
                "suppressed": suppressed,
                "requested_quantity": 0.0,
                "pending_semantics": None,
                "reasons": ["no_eligible_intent"],
                "state": state,
            }
        normalized.sort(key=lambda item: (item["policy_priority"], item["candidate_index"]))
        chosen = normalized[0]
        suppressed.extend(normalized[1:])
        return {
            "policy_version": PLAN_VERSION,
            "chosen": chosen,
            "suppressed": suppressed,
            "requested_quantity": float(chosen.get("requested_quantity") or chosen.get("quantity") or 0.0),
            "pending_semantics": chosen.get("pending_semantics"),
            "reasons": list(chosen.get("reasons") or [chosen.get("reason") or chosen["kind"]]),
            "state": state,
        }

    def _evaluate_intraday_position(self, position, quote, prior_bars, now, bucket, phase, *, allow_model_signal: bool = True):
        plan = self.repo.get_plan(position.plan_id)
        if plan is None:
            return None
        price, quote_error = self._fresh_quote_price(quote, observed_at=now)
        quote_error = self._realtime_quote_identity_error(plan=position, quote=quote) or quote_error
        evidence = {
            "mode": "intraday",
            "phase": self._phase_evidence(phase),
            "quote": self._quote_evidence(quote),
            "exit_checks": {},
        }
        result = "position_held"
        action = "hold"
        reasons: List[str] = []
        close_reason = None
        close_price = None
        chosen_exit_leg = None
        chosen_fraction = None
        chosen_quantity = None
        triggering_signal = None
        if quote_error:
            result = "failed_closed"
            reasons.append(quote_error)
        else:
            assert price is not None
            local_date = phase.market_local_time.date()
            if getattr(position, "pending_exit_reason", None):
                if not self._can_close_position_on_date(position, local_date):
                    result = "pending_exit"
                    action = "none"
                    reasons.append("pending_exit_waiting_next_actual_session")
                    evidence["pending_exit"] = {
                        "pending_exit_on": position.pending_exit_on.isoformat() if position.pending_exit_on else None,
                        "pending_exit_price": position.pending_exit_price,
                        "sellability": self._sellability_evidence(position, local_date),
                    }
                    return self._persist_intraday_outcome(
                        plan=plan,
                        position=position,
                        now=now,
                        bucket=bucket,
                        quote=quote,
                        result=result,
                        action=action,
                        reasons=reasons,
                        evidence=evidence,
                        required_passed=True,
                    )
                pending_reason = str(position.pending_exit_reason)
                conditional_profit = any(token in pending_reason for token in ("target", "take_profit", "trailing_profit"))
                if conditional_profit and position.pending_exit_price is not None and price < float(position.pending_exit_price):
                    evidence["pending_exit"] = {
                        "pending_exit_on": position.pending_exit_on.isoformat() if position.pending_exit_on else None,
                        "pending_exit_price": position.pending_exit_price,
                        "revalidated": False,
                        "policy": "conditional_profit_pending_cancelled_when_threshold_no_longer_holds",
                    }
                    with self._ledger_write_session() as session:
                        db_position = session.get(PaperPosition, position.id)
                        if db_position is not None:
                            db_position.pending_exit_reason = None
                            db_position.pending_exit_on = None
                            db_position.pending_exit_price = None
                            db_position.pending_exit_event_id = None
                            db_position.available_quantity = float(db_position.quantity or 0.0)
                            db_position.frozen_quantity = 0.0
                            db_position.updated_at = utc_naive_now()
                    result = "position_held"
                    action = "hold"
                    reasons.append("conditional_profit_pending_cancelled")
                    return self._persist_intraday_outcome(
                        plan=plan,
                        position=position,
                        now=now,
                        bucket=bucket,
                        quote=quote,
                        result=result,
                        action=action,
                        reasons=reasons,
                        evidence=evidence,
                        required_passed=True,
                    )
                close_reason, close_price = str(position.pending_exit_reason), price
                evidence["pending_exit"] = {
                    "pending_exit_on": position.pending_exit_on.isoformat() if position.pending_exit_on else None,
                    "pending_exit_price": position.pending_exit_price,
                    "revalidated": True if conditional_profit else None,
                    "fill_policy": "fresh_quote_next_actual_session",
                }
                result = "closed"
                action = "close"
                reasons.append("pending_exit_filled_with_fresh_quote")
                return self._persist_intraday_outcome(
                    plan=plan,
                    position=position,
                    now=now,
                    bucket=bucket,
                    quote=quote,
                    result=result,
                    action=action,
                    reasons=reasons,
                    evidence=evidence,
                    required_passed=True,
                    close_reason=close_reason,
                    close_price=close_price,
                    triggering_signal=None,
                )
            hit_stop = position.stop_loss is not None and price <= float(position.stop_loss)
            model_risk_provenance = self._effective_model_risk_provenance(position)
            stop_revision = model_risk_provenance.get("stop")
            target_revision = model_risk_provenance.get("target")
            with self.db.get_session() as session:
                staged_exit_leg = (
                    None
                    if target_revision is not None
                    else self._next_exit_leg_for_position(
                        session,
                        plan=plan,
                        position=position,
                        observed_high=price,
                    )
                )
            has_exit_contract = self._plan_exit_contract(plan) is not None
            hit_target = (
                staged_exit_leg is not None
                or (
                    position.target_price is not None
                    and price >= float(position.target_price)
                    and (target_revision is not None or not has_exit_contract)
                )
            )
            evidence["exit_checks"].update({
                "stop_loss": position.stop_loss,
                "target_price": position.target_price,
                "hit_stop": hit_stop,
                "hit_target": hit_target,
                "staged_exit_leg": staged_exit_leg,
                "effective_risk_provenance": {
                    "stop_revision_id": getattr(stop_revision, "id", None),
                    "stop_source_report_id": getattr(stop_revision, "source_report_id", None),
                    "target_revision_id": getattr(target_revision, "id", None),
                    "target_source_report_id": getattr(target_revision, "source_report_id", None),
                },
            })
            candidates: List[Dict[str, Any]] = []
            if hit_stop:
                stop_reason = "model_risk_stop_breached" if stop_revision is not None else "intraday_stop_loss"
                candidates.append({
                    "kind": "hard_stop",
                    "action": "close",
                    "reason": stop_reason,
                    "price": price,
                    "requested_quantity": float(position.quantity or 0.0),
                    "pending_semantics": "sticky_full_exit",
                })
                if stop_revision is not None and stop_revision.decision_signal_id is not None:
                    triggering_signal = self.repo.get_signal(int(stop_revision.decision_signal_id))
            if hit_target:
                target_reason = (
                    f"take_profit:{staged_exit_leg.get('leg_id')}"
                    if staged_exit_leg is not None
                    else ("model_risk_target_reached" if target_revision is not None else "intraday_take_profit")
                )
                candidates.append({
                    "kind": "take_profit",
                    "action": "close",
                    "reason": target_reason,
                    "price": price,
                    "requested_quantity": (
                        self._exit_leg_quantity(position, staged_exit_leg)
                        if staged_exit_leg is not None
                        else float(position.quantity or 0.0)
                    ),
                    "requested_fraction": (
                        float(staged_exit_leg.get("allocation_fraction") or 0.0)
                        if staged_exit_leg is not None
                        else None
                    ),
                    "exit_leg": staged_exit_leg,
                    "pending_semantics": "conditional_profit",
                })
                if target_revision is not None and target_revision.decision_signal_id is not None:
                    triggering_signal = self.repo.get_signal(int(target_revision.decision_signal_id))
            if allow_model_signal:
                evidence["defensive_signal"] = {
                    "policy": "model_defensive_signals_are_resolved_once_per_aggregate_holding",
                    "reason": "single_lot_position_path_handles_only_lot_local_stop_target_and_pending_exit",
                }
            candidates.append({"kind": "hold", "action": "hold", "reason": "no_intraday_exit_condition_met"})
            resolution = self.resolve_paper_intents(
                candidates=candidates,
                gates={"fail_closed": False},
                state={
                    "entry": "intraday_position",
                    "position_id": getattr(position, "id", None),
                    "market_date": local_date.isoformat(),
                    "sellable": self._can_close_position_on_date(position, local_date),
                },
            )
            evidence["intent_resolution"] = resolution
            chosen = resolution.get("chosen")
            if chosen and chosen.get("action") == "close":
                close_reason, close_price = str(chosen.get("reason")), float(chosen.get("price") or price)
                chosen_exit_leg = chosen.get("exit_leg") if isinstance(chosen.get("exit_leg"), Mapping) else None
                chosen_fraction = self._safe_float(chosen.get("requested_fraction"))
                chosen_quantity = self._safe_float(chosen.get("requested_quantity"))
                if not self._can_close_position_on_date(position, local_date):
                    result = "pending_exit"
                    action = "pending_exit"
                    reasons.append(f"t1_pending:{close_reason}")
                    evidence["sellability"] = self._sellability_evidence(position, local_date)
                else:
                    result = "closed"
                    action = "close"
                    reasons.extend(resolution.get("reasons") or [close_reason])
            else:
                reasons.append("no_intraday_exit_condition_met")
        if not allow_model_signal and result == "position_held" and action == "hold":
            return None
        return self._persist_intraday_outcome(
            plan=plan,
            position=position,
            now=now,
            bucket=bucket,
            quote=quote,
            result=result,
            action=action,
            reasons=reasons,
            evidence=evidence,
            required_passed=True,
            close_reason=close_reason,
            close_price=close_price,
            triggering_signal=triggering_signal,
            exit_leg=chosen_exit_leg,
            close_requested_fraction=chosen_fraction,
            close_requested_quantity=chosen_quantity,
        )

    def _effective_model_risk_provenance(self, position: PaperPosition) -> Dict[str, Optional[PaperRiskRevision]]:
        """Find the applied revision that produced each currently effective line."""

        current_stop = self._safe_price(getattr(position, "stop_loss", None))
        current_target = self._safe_price(getattr(position, "target_price", None))
        with self.db.get_session() as session:
            rows = list(session.execute(
                select(PaperRiskRevision)
                .where(
                    PaperRiskRevision.position_id == int(position.id),
                    PaperRiskRevision.result.in_(["applied", "applied_breached"]),
                )
                .order_by(desc(PaperRiskRevision.revision_version), desc(PaperRiskRevision.id))
            ).scalars().all())
        stop_revision = next((
            row for row in rows
            if current_stop is not None
            and row.applied_stop_loss is not None
            and math.isclose(float(row.applied_stop_loss), current_stop, rel_tol=0.0, abs_tol=1e-9)
        ), None)
        target_revision = next((
            row for row in rows
            if current_target is not None
            and row.applied_target_price is not None
            and math.isclose(float(row.applied_target_price), current_target, rel_tol=0.0, abs_tol=1e-9)
        ), None)
        return {"stop": stop_revision, "target": target_revision}

    def _evaluate_intraday_plan(self, plan, quote, prior_bars, now, bucket, phase):
        if not plan.executable or plan.status != "active":
            if plan.status != "triggered":
                return None
            if not self._plan_has_pending_entry_leg(plan):
                return None
        unresolved = self._unresolved_reanalysis_request(plan)
        if unresolved is not None:
            return self._persist_intraday_outcome(
                plan=plan, position=None, now=now, bucket=bucket, quote=quote,
                result="reanalysis_blocked", action="none",
                reasons=[f"reanalysis_request_unresolved:{unresolved.status}"],
                evidence={
                    "mode": "intraday",
                    "phase": self._phase_evidence(phase),
                    "quote": self._quote_evidence(quote),
                    "reanalysis_request": self._serialize_reanalysis_request(unresolved),
                },
                required_passed=False,
            )
        phase_session_date = self._phase_session_date(phase)
        if plan.status == "active" and getattr(plan, "valid_for_date", None) is not None and phase_session_date != plan.valid_for_date:
            reason = (
                "plan_not_yet_valid_for_session"
                if phase_session_date < plan.valid_for_date
                else "plan_expired_for_session"
            )
            return self._persist_intraday_outcome(
                plan=plan, position=None, now=now, bucket=bucket, quote=quote,
                result="failed_closed" if reason == "plan_not_yet_valid_for_session" else "expired",
                action="none",
                reasons=[reason],
                evidence={
                    "mode": "intraday",
                    "phase": self._phase_evidence(phase),
                    "quote": self._quote_evidence(quote),
                    "valid_for_date": plan.valid_for_date.isoformat(),
                },
                required_passed=False,
            )
        same_session_authority = (
            getattr(plan, "valid_for_date", None) is not None
            and plan.valid_for_date == plan.report_date
        )
        if (
            phase.market_local_time.date() < plan.report_date
            or (
                phase.market_local_time.date() == plan.report_date
                and not same_session_authority
            )
        ):
            return self._persist_intraday_outcome(
                plan=plan, position=None, now=now, bucket=bucket, quote=quote,
                result="skipped", action="none",
                reasons=["intraday_monitor_requires_session_after_report_date"],
                evidence={"mode": "intraday", "phase": self._phase_evidence(phase), "quote": self._quote_evidence(quote)},
                required_passed=False,
            )
        identity_error = self._realtime_quote_identity_error(plan=plan, quote=quote)
        price, quote_error = self._fresh_quote_price(quote, observed_at=now)
        rules, rules_error = self._json_list(plan.rules_json)
        evidence = {
            "mode": "intraday",
            "policy": self._intraday_policy_evidence(),
            "phase": self._phase_evidence(phase),
            "quote": self._quote_evidence(quote),
            "required_rules": [],
            "factor_domains": [],
        }
        reasons: List[str] = []
        result = "not_triggered"
        if identity_error or quote_error:
            return self._persist_intraday_outcome(
                plan=plan, position=None, now=now, bucket=bucket, quote=quote,
                result="failed_closed", action="none", reasons=[identity_error or quote_error], evidence=evidence,
                required_passed=False,
            )
        if rules_error or not rules:
            return self._persist_intraday_outcome(
                plan=plan, position=None, now=now, bucket=bucket, quote=quote,
                result="failed_closed", action="none", reasons=[rules_error or "missing_rules"], evidence=evidence,
                required_passed=False,
            )
        assert price is not None
        session_date = self._phase_session_date(phase)
        if (
            self._opening_confirmation_required(plan=plan, phase=phase)
            and not self._opening_confirmation_terminal(plan=plan, session_date=session_date)
        ):
            confirmation = self._process_opening_confirmation_plan(
                plan=plan,
                quote=quote,
                now=now,
                bucket=bucket,
                phase=phase,
                persist_confirmed=False,
            )
            if confirmation is None:
                return None
            if confirmation.get("result") != "opening_confirmed":
                return confirmation
            confirmation_evidence = confirmation.get("evidence") if isinstance(confirmation.get("evidence"), Mapping) else {}
            evidence["opening_confirmation"] = confirmation_evidence.get("opening_confirmation")
            evidence["opening_confirmation_quote"] = confirmation_evidence.get("quote")
            evidence["opening_confirmation_policy"] = confirmation_evidence.get("policy")
            evidence["opening_confirmation_status"] = "confirmed"
        gap_veto = self._opening_gap_veto(rules, price, phase)
        if gap_veto:
            evidence["gap_veto"] = gap_veto
            return self._persist_intraday_outcome(
                plan=plan,
                position=None,
                now=now,
                bucket=bucket,
                quote=quote,
                result="failed_closed",
                action="none",
                reasons=["opening_gap_veto"],
                evidence=evidence,
                required_passed=False,
            )
        required_passed, rule_reasons = self._evaluate_intraday_required_rules(
            rules, price, quote, prior_bars, evidence
        )
        factors = self._intraday_factor_domains(plan, price, quote, prior_bars)
        evidence["factor_domains"] = factors
        setup_check = self._setup_confirmation_policy(plan, factors, mode="intraday")
        composite = float(setup_check.get("score") or 0.0)
        risk_reward = self._risk_reward_ratio(plan.stop_loss, plan.target_price, price, price)
        evidence["setup_confirmation_policy"] = setup_check
        evidence["risk_reward"] = risk_reward
        evidence["composite_score"] = round(composite, 6)
        evidence["factors_available"] = setup_check.get("factors_available", 0)
        evidence["factors_passed"] = setup_check.get("factors_passed", 0)
        if not required_passed:
            reasons.extend(rule_reasons)
        if not setup_check.get("passed"):
            reasons.extend(str(reason) for reason in setup_check.get("reasons") or [])
        if risk_reward is None or risk_reward < self.intraday_min_risk_reward:
            reasons.append("risk_reward_below_policy")
        selected_leg = None
        if not reasons:
            selected_leg, leg_reason = self._select_entry_leg_for_fill(
                plan=plan,
                price=price,
                prior_bars=prior_bars,
            )
            evidence["selected_entry_leg"] = selected_leg
            if selected_leg is None:
                result = "not_triggered"
                reasons.append(leg_reason or "no_eligible_entry_leg")
            else:
                result = "triggered"
                reasons.append("required_rules_and_composite_factors_passed")
                reasons.append(f"entry_leg_selected:{selected_leg.get('leg_id')}")
        elif any(reason.startswith(("invalid_rule", "required_rule_unavailable")) for reason in reasons) or setup_check.get("fail_closed"):
            result = "failed_closed"
        candidates = []
        if result == "triggered":
            candidates.append({
                "kind": "entry",
                "action": "open",
                "reason": "required_rules_and_composite_factors_passed",
                "price": price,
                "entry_leg": selected_leg,
            })
        else:
            candidates.append({
                "kind": "none",
                "action": "none",
                "reason": reasons[0] if reasons else "entry_not_triggered",
                "eligible": result != "failed_closed",
            })
        resolution = self.resolve_paper_intents(
            candidates=candidates,
            gates={
                "fail_closed": result == "failed_closed",
                "reasons": reasons,
            },
            state={
                "entry": "intraday_plan",
                "plan_id": getattr(plan, "id", None),
                "bucket": self._utc_naive(bucket).isoformat(),
            },
        )
        evidence["intent_resolution"] = resolution
        chosen = resolution.get("chosen")
        should_open = bool(chosen and chosen.get("action") == "open")
        return self._persist_intraday_outcome(
            plan=plan,
            position=None,
            now=now,
            bucket=bucket,
            quote=quote,
            result="triggered" if should_open else result,
            action="open" if should_open else "none",
            reasons=resolution.get("reasons") or reasons,
            evidence=evidence,
            required_passed=required_passed,
            open_price=price if should_open else None,
            entry_leg=dict(chosen.get("entry_leg") or {}) if should_open else None,
        )

    def _select_entry_leg_for_fill(self, *, plan, price: float, prior_bars: Sequence[Mapping[str, Any]]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        legs = self._plan_entry_legs(plan)
        if not legs:
            return None, "missing_entry_legs"
        with self.db.get_session() as session:
            positions = list(session.execute(
                select(PaperPosition)
                .where(PaperPosition.plan_id == plan.id)
            ).scalars().all())
            open_positions = [position for position in positions if position.status == "open"]
            executed_counts: Dict[str, int] = {}
            for position in positions:
                leg_id = str(getattr(position, "entry_leg_id", None) or "legacy_entry")
                executed_counts[leg_id] = executed_counts.get(leg_id, 0) + 1
            for leg in sorted(legs, key=lambda item: (int(item.get("sequence") or 0), str(item.get("leg_id") or ""))):
                leg_id = str(leg.get("leg_id") or "legacy_entry")
                max_executions = max(1, int(leg.get("max_executions") or 1))
                if executed_counts.get(leg_id, 0) >= max_executions:
                    continue
                role = str(leg.get("role") or "initial").lower()
                if role != "initial" and not open_positions:
                    continue
                if role == "initial" and open_positions and leg_id == "legacy_entry":
                    continue
                rule_reasons: List[str] = []
                all_price_rules_passed = True
                current = {"close": price, "high": price, "low": price, "open": price, "volume": None}
                for rule in leg.get("price_rules") or []:
                    if not isinstance(rule, Mapping):
                        all_price_rules_passed = False
                        rule_reasons.append("invalid_leg_price_rule")
                        continue
                    passed, _rule_evidence, reason = self._evaluate_rule(rule, current, list(prior_bars or []))
                    if not passed:
                        all_price_rules_passed = False
                        if reason:
                            rule_reasons.append(reason)
                if all_price_rules_passed:
                    return dict(leg), None
            return None, "no_entry_leg_price_or_prerequisite_passed"

    def _plan_has_pending_entry_leg(self, plan) -> bool:
        legs = self._plan_entry_legs(plan)
        if not legs:
            return False
        with self.db.get_session() as session:
            positions = list(session.execute(
                select(PaperPosition).where(PaperPosition.plan_id == plan.id)
            ).scalars().all())
        counts: Dict[str, int] = {}
        has_open = False
        for position in positions:
            leg_id = str(getattr(position, "entry_leg_id", None) or "legacy_entry")
            counts[leg_id] = counts.get(leg_id, 0) + 1
            if position.status == "open":
                has_open = True
        for leg in legs:
            role = str(leg.get("role") or "initial").lower()
            leg_id = str(leg.get("leg_id") or "legacy_entry")
            if role == "initial":
                if counts.get(leg_id, 0) < max(1, int(leg.get("max_executions") or 1)):
                    return True
                continue
            if not has_open:
                continue
            if counts.get(leg_id, 0) < max(1, int(leg.get("max_executions") or 1)):
                return True
        return False

    def _plan_has_live_lots_or_pending_legs(
        self,
        session,
        plan,
        *,
        exclude_position_id: Optional[int] = None,
    ) -> bool:
        position_query = (
            select(PaperPosition)
            .where(
                PaperPosition.plan_id == plan.id,
                PaperPosition.status == "open",
            )
        )
        if exclude_position_id is not None:
            position_query = position_query.where(PaperPosition.id != int(exclude_position_id))
        live_positions = list(session.execute(position_query).scalars().all())
        if live_positions:
            return True
        legs = self._plan_entry_legs(plan)
        if not legs:
            return False
        counts: Dict[str, int] = {}
        all_positions = list(session.execute(
            select(PaperPosition).where(PaperPosition.plan_id == plan.id)
        ).scalars().all())
        for position in all_positions:
            leg_id = str(getattr(position, "entry_leg_id", None) or "legacy_entry")
            counts[leg_id] = counts.get(leg_id, 0) + 1
        for leg in legs:
            role = str(leg.get("role") or "initial").lower()
            if role == "initial":
                continue
            leg_id = str(leg.get("leg_id") or "legacy_entry")
            if counts.get(leg_id, 0) < max(1, int(leg.get("max_executions") or 1)):
                return True
        return False

    def _persist_intraday_outcome(
        self, *, plan, position, now, bucket, quote, result, action, reasons,
        evidence, required_passed: bool, open_price: Optional[float] = None, close_reason: Optional[str] = None,
        close_price: Optional[float] = None, triggering_signal=None,
        entry_leg: Optional[Mapping[str, Any]] = None,
        exit_leg: Optional[Mapping[str, Any]] = None,
        close_requested_fraction: Optional[float] = None,
        close_requested_quantity: Optional[float] = None,
    ):
        observed_naive = self._utc_naive(now)
        bucket_naive = self._utc_naive(bucket)
        leg_id_for_key = str((entry_leg or {}).get("leg_id") or getattr(position, "entry_leg_id", None) or "none")
        observation_key = (
            f"plan:{plan.id}:pos:{position.id if position is not None else 0}:"
            f"leg:{leg_id_for_key}:intraday:{bucket_naive.isoformat()}"
        )
        with self._ledger_write_session() as session:
            existing = session.execute(select(PaperPlanObservation).where(
                PaperPlanObservation.observation_key == observation_key
            )).scalar_one_or_none()
            if existing is not None:
                return None
            db_plan = session.get(PaperExecutionPlan, plan.id)
            db_position = session.get(PaperPosition, position.id) if position is not None else None
            currency = (
                getattr(db_position, "currency", None)
                if db_position is not None
                else self._currency_for_market(db_plan.market)
            )
            account_id = getattr(db_position, "capital_account_id", None) if db_position is not None else None
            resolution_bucket = self._resolution_bucket(
                mode="intraday_position" if db_position is not None else "intraday_plan",
                source_model=db_plan.source_model,
                market=db_plan.market,
                stock_code=db_plan.stock_code,
                currency=currency,
                capital_account_id=account_id,
                bucket_started_at=bucket_naive,
            )
            resolution_key = self._resolution_key(resolution_bucket)
            logical_order_key = None
            if action in {"open", "close", "pending_exit"}:
                logical_order_key = "ord-" + hashlib.sha256(
                    f"{resolution_key}|{action}|{close_reason or 'entry'}".encode("utf-8")
                ).hexdigest()[:24]
            self._persist_resolution(
                session,
                resolution_key=resolution_key,
                bucket=resolution_bucket,
                status=result,
                selected_intent=action if action != "none" else None,
                candidate_intents=[{
                    "intent": action,
                    "result": result,
                    "reasons": list(reasons or []),
                    "close_reason": close_reason,
                    "entry_leg": dict(entry_leg or {}),
                }],
                selected_intent_payload={
                    "intent": action,
                    "close_reason": close_reason,
                    "logical_order_key": logical_order_key,
                } if action not in {"none", "hold"} else {},
                source_report_id=db_plan.source_report_id,
                decision_signal_id=db_plan.decision_signal_id,
                source_identities=(
                    [{"kind": "position", "id": db_position.id}]
                    if db_position is not None else [{"kind": "plan", "id": db_plan.id}]
                ),
                evidence={**evidence, "reasons": reasons},
            )
            factors = evidence.get("factor_domains") if isinstance(evidence, dict) else None
            counted_factors = [
                item for item in factors or []
                if item.get("counts_for_entry", True)
            ]
            factors_available = len([item for item in counted_factors if item.get("available")])
            factors_passed = len([
                item for item in counted_factors
                if item.get("available") and item.get("passed")
            ])
            observation = PaperPlanObservation(
                observation_key=observation_key,
                plan_id=db_plan.id,
                position_id=db_position.id if db_position is not None else None,
                source_report_id=db_plan.source_report_id,
                decision_signal_id=db_plan.decision_signal_id,
                source_model=db_plan.source_model,
                market=db_plan.market,
                stock_code=db_plan.stock_code,
                stock_name=db_plan.stock_name,
                mode="intraday",
                observed_at=observed_naive,
                bucket_started_at=bucket_naive,
                quote_price=self._safe_price(getattr(quote, "price", None)),
                quote_source=self._quote_source(quote),
                quote_provider_timestamp=str(getattr(quote, "provider_timestamp", "") or "")[:64] or None,
                quote_fetched_at=str(getattr(quote, "fetched_at", "") or "")[:64] or None,
                resolution_key=resolution_key,
                resolution_bucket_json=self._json_dumps(resolution_bucket),
                logical_order_key=logical_order_key,
                result=result,
                action=action,
                composite_score=evidence.get("composite_score") if isinstance(evidence, dict) else None,
                required_passed=bool(required_passed),
                factors_available=factors_available,
                factors_passed=factors_passed,
                evidence_json=self._json_dumps({**evidence, "reasons": reasons}),
            )
            session.add(observation)
            session.flush()
            event = None
            event_date = self._event_date_from_evidence(evidence, now)
            if result == "expired" and db_plan.status == "active":
                db_plan.status = "expired"
                db_plan.updated_at = utc_naive_now()
            if action == "open" and open_price is not None:
                selected_leg = dict(entry_leg or {})
                selected_leg_id = str(selected_leg.get("leg_id") or "legacy_entry")[:64]
                bucket_open_exists = session.execute(
                    select(PaperPlanObservation.id)
                    .where(
                        PaperPlanObservation.plan_id == db_plan.id,
                        PaperPlanObservation.bucket_started_at == bucket_naive,
                        PaperPlanObservation.action == "open",
                        PaperPlanObservation.result == "triggered",
                        PaperPlanObservation.id != observation.id,
                    )
                    .limit(1)
                ).scalar_one_or_none() is not None
                if bucket_open_exists:
                    observation.result = "not_triggered"
                    observation.action = "none"
                    observation.evidence_json = self._json_dumps({**evidence, "reasons": ["plan_bucket_already_filled"]})
                    session.flush()
                    return {"result": "not_triggered", "observation": self._serialize_observation(observation), "event": None}
                max_executions = max(1, int(selected_leg.get("max_executions") or 1))
                existing_count = session.execute(select(PaperPosition).where(
                    PaperPosition.plan_id == db_plan.id,
                    PaperPosition.entry_leg_id == selected_leg_id,
                )).scalars().all()
                open_same_plan = session.execute(select(PaperPosition.id).where(
                    PaperPosition.plan_id == db_plan.id,
                    PaperPosition.status == "open",
                ).limit(1)).scalar_one_or_none() is not None
                if str(selected_leg.get("role") or "initial").lower() != "initial" and not open_same_plan:
                    observation.result = "failed_closed"
                    observation.action = "none"
                    observation.evidence_json = self._json_dumps({**evidence, "reasons": ["entry_leg_requires_open_holding"]})
                    session.flush()
                    return {"result": "failed_closed", "observation": self._serialize_observation(observation), "event": None}
                if len(existing_count) < max_executions:
                    account = self._get_or_create_account(
                        session,
                        source_model=db_plan.source_model,
                        market=db_plan.market,
                    )
                    existing_identity_holding = session.execute(select(PaperPosition.id).where(
                        PaperPosition.capital_account_id == account.id,
                        PaperPosition.source_model == db_plan.source_model,
                        PaperPosition.market == db_plan.market,
                        PaperPosition.stock_code == db_plan.stock_code,
                        PaperPosition.status == "open",
                    ).limit(1)).scalar_one_or_none() is not None
                    budget_snapshot = self._ensure_plan_budget_snapshot(db_plan, account)
                    leg_allocation_fraction = float(selected_leg.get("allocation_fraction") or 1.0)
                    execution_allocation_fraction = leg_allocation_fraction / float(max_executions)
                    market_policy_decision = self._market_policy_decision_in_session(
                        session,
                        account=account,
                        market=db_plan.market,
                        current_time=now,
                    )
                    symbol_cap_factor = float(market_policy_decision.get("symbol_cap_factor", 1.0) or 0.0)
                    total_cap_factor = float(market_policy_decision.get("total_cap_factor", 1.0) or 0.0)
                    sizing, sizing_error = self._size_position(
                        account,
                        price=open_price,
                        stop_loss=db_plan.stop_loss,
                        market=db_plan.market,
                        allocation_fraction=execution_allocation_fraction,
                        existing_holding=existing_identity_holding,
                        allocation_budget=db_plan.plan_allocation_budget,
                        risk_budget=db_plan.plan_risk_budget,
                        risk_multiplier=self._style_risk_multiplier_for_plan(db_plan),
                        decision=market_policy_decision,
                    )
                    if sizing_error:
                        result = "failed_closed"
                        action = "none"
                        reasons = [sizing_error]
                        observation.result = result
                        observation.action = action
                        observation.evidence_json = self._json_dumps({**evidence, "reasons": reasons})
                        session.flush()
                        return {
                            "result": result,
                            "observation": self._serialize_observation(observation),
                            "event": None,
                        }
                    symbol_snapshot = self._symbol_exposure_snapshot(
                        session,
                        account,
                        source_model=db_plan.source_model,
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        projected_additional_cost=sizing["allocated_capital"],
                        symbol_cap_factor=symbol_cap_factor,
                    )
                    if not symbol_snapshot["within_limit"]:
                        result = "failed_closed"
                        action = "none"
                        reasons = ["symbol_exposure_cap_exceeded"]
                        observation.result = result
                        observation.action = action
                        observation.evidence_json = self._json_dumps({
                            **evidence,
                            "reasons": reasons,
                            "symbol_exposure": symbol_snapshot,
                        })
                        session.flush()
                        return {
                            "result": result,
                            "observation": self._serialize_observation(observation),
                            "event": None,
                        }
                    account_risk_gate = self._account_risk_gate_for_open(
                        session,
                        account=account,
                        plan=db_plan,
                        projected_additional_cost=sizing["allocated_capital"],
                        current_time=now,
                        bucket_started_at=bucket_naive,
                        resolution_key=resolution_key,
                        decision=market_policy_decision,
                    )
                    if not account_risk_gate["allowed"]:
                        result = "failed_closed"
                        action = "none"
                        reasons = [f"account_risk_lock:{reason}" for reason in account_risk_gate["reasons"]]
                        observation.result = result
                        observation.action = action
                        observation.evidence_json = self._json_dumps({
                            **evidence,
                            "reasons": reasons,
                            "symbol_exposure": symbol_snapshot,
                            "account_risk_gate": account_risk_gate,
                        })
                        self._persist_resolution(
                            session,
                            resolution_key=resolution_key,
                            bucket=resolution_bucket,
                            status=result,
                            selected_intent=None,
                            candidate_intents=[{"intent": "open", "entry_leg": selected_leg}],
                            suppressed_intents=[{"intent": "open", "reasons": reasons}],
                            source_report_id=db_plan.source_report_id,
                            decision_signal_id=db_plan.decision_signal_id,
                            requested_quantity=sizing.get("quantity"),
                            executed_quantity=0.0,
                            unfilled_quantity=sizing.get("quantity"),
                            evidence={
                                **evidence,
                                "symbol_exposure": symbol_snapshot,
                                "account_risk_gate": account_risk_gate,
                                "reasons": reasons,
                            },
                        )
                        session.flush()
                        return {
                            "result": result,
                            "observation": self._serialize_observation(observation),
                            "event": None,
                        }
                    fill, fill_error = self._fill_assumption(
                        side="buy",
                        quote_price=open_price,
                        quantity=sizing["quantity"],
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        quote=quote,
                        observed_at=now,
                    )
                    if fill_error:
                        result = "failed_closed"
                        action = "none"
                        reasons = [fill_error]
                        observation.result = result
                        observation.action = action
                        observation.evidence_json = self._json_dumps({**evidence, "reasons": reasons})
                        session.flush()
                        return {
                            "result": result,
                            "observation": self._serialize_observation(observation),
                            "event": None,
                        }
                    assert fill is not None
                    symbol_snapshot = self._symbol_exposure_snapshot(
                        session,
                        account,
                        source_model=db_plan.source_model,
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        projected_additional_cost=fill["net_amount"],
                        symbol_cap_factor=symbol_cap_factor,
                    )
                    invariant_error = self._open_fill_invariant_error(
                        account=account,
                        fill=fill,
                        symbol_snapshot=symbol_snapshot,
                        total_cap_factor=total_cap_factor,
                    )
                    if invariant_error:
                        result = "failed_closed"
                        action = "none"
                        reasons = [invariant_error]
                        observation.result = result
                        observation.action = action
                        observation.evidence_json = self._json_dumps({
                            **evidence,
                            "reasons": reasons,
                            "symbol_exposure": symbol_snapshot,
                            "fill": fill,
                        })
                        session.flush()
                        return {
                            "result": result,
                            "observation": self._serialize_observation(observation),
                            "event": None,
                        }
                    account_risk_gate = self._account_risk_gate_for_open(
                        session,
                        account=account,
                        plan=db_plan,
                        projected_additional_cost=fill["net_amount"],
                        current_time=now,
                        bucket_started_at=bucket_naive,
                        resolution_key=resolution_key,
                        decision=market_policy_decision,
                    )
                    if not account_risk_gate["allowed"]:
                        result = "failed_closed"
                        action = "none"
                        reasons = [f"account_risk_lock:{reason}" for reason in account_risk_gate["reasons"]]
                        observation.result = result
                        observation.action = action
                        observation.evidence_json = self._json_dumps({
                            **evidence,
                            "reasons": reasons,
                            "symbol_exposure": symbol_snapshot,
                            "account_risk_gate": account_risk_gate,
                            "fill": fill,
                        })
                        session.flush()
                        return {
                            "result": result,
                            "observation": self._serialize_observation(observation),
                            "event": None,
                        }
                    sizing["allocated_capital"] = fill["net_amount"]
                    self._persist_resolution(
                        session,
                        resolution_key=resolution_key,
                        bucket=resolution_bucket,
                        status="triggered",
                        selected_intent="open",
                        candidate_intents=[{"intent": "open", "entry_leg": selected_leg}],
                        selected_intent_payload={
                            "intent": "open",
                            "logical_order_key": logical_order_key,
                            "entry_leg_id": selected_leg_id,
                        },
                        source_report_id=db_plan.source_report_id,
                        decision_signal_id=db_plan.decision_signal_id,
                        requested_quantity=fill["requested_quantity"],
                        executed_quantity=fill["executed_quantity"],
                        unfilled_quantity=fill["unfilled_quantity"],
                        evidence={**evidence, "symbol_exposure": symbol_snapshot},
                    )
                    self._debit_account_for_open(
                        account,
                        sizing,
                        count_new_holding=not existing_identity_holding,
                    )
                    available_quantity, frozen_quantity = self._initial_sellability_quantities(
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        quantity=sizing["quantity"],
                    )
                    position_row = PaperPosition(
                        plan_id=db_plan.id,
                        entry_leg_id=selected_leg_id,
                        execution_index=len(existing_count) + 1,
                        source_report_id=db_plan.source_report_id,
                        decision_signal_id=db_plan.decision_signal_id,
                        source_model=db_plan.source_model,
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        stock_name=db_plan.stock_name,
                        opened_on=event_date,
                        opened_at=observed_naive,
                        opened_price=fill["fill_price"],
                        normalized_notional=sizing["allocated_capital"],
                        quantity=sizing["quantity"],
                        initial_quantity=sizing["quantity"],
                        available_quantity=available_quantity,
                        frozen_quantity=frozen_quantity,
                        capital_account_id=account.id,
                        currency=sizing["currency"],
                        allocated_capital=sizing["allocated_capital"],
                        initial_allocated_capital=sizing["allocated_capital"],
                        remaining_cost=sizing["allocated_capital"],
                        realized_quantity=0.0,
                        realized_proceeds=0.0,
                        realized_fees=0.0,
                        realized_pnl=0.0,
                        lot_size=sizing["lot_size"],
                        risk_amount=sizing["risk_amount"],
                        risk_per_share=sizing["risk_per_share"],
                        sizing_reason=sizing["sizing_reason"],
                        legacy_fixed_notional=False,
                        sellability_rule=self._sellability_rule_for(db_plan.market, db_plan.stock_code),
                        stop_loss=db_plan.stop_loss,
                        target_price=db_plan.target_price,
                        max_holding_sessions=db_plan.max_holding_sessions,
                        trading_style=db_plan.trading_style,
                        trading_style_policy_version=db_plan.trading_style_policy_version,
                        trading_style_settings_json=db_plan.trading_style_settings_json,
                        market_regime_context_id=db_plan.market_regime_context_id,
                        market_regime=db_plan.market_regime,
                        market_regime_policy_version=db_plan.market_regime_policy_version,
                    )
                    session.add(position_row)
                    session.flush()
                    self._sync_open_position_count_in_session(session, account)
                    db_plan.status = "triggered"
                    db_plan.updated_at = utc_naive_now()
                    event_row = PaperTradeEvent(
                        event_key=(
                            f"plan:{db_plan.id}:leg:{selected_leg_id}:"
                            f"exec:{len(existing_count) + 1}:intraday_open:{bucket_naive.isoformat()}"
                        ),
                        plan_id=db_plan.id,
                        position_id=position_row.id,
                        source_report_id=db_plan.source_report_id,
                        decision_signal_id=db_plan.decision_signal_id,
                        source_model=db_plan.source_model,
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        stock_name=db_plan.stock_name,
                        event_type="open",
                        event_date=event_date,
                        price=fill["fill_price"],
                        quantity=sizing["quantity"],
                        gross_amount=fill["gross_amount"],
                        fees=fill["fees"],
                        commission=fill.get("commission"),
                        tax=fill.get("tax", fill.get("sell_tax")),
                        slippage_amount=fill.get("slippage_amount"),
                        net_amount=fill["net_amount"],
                        fill_price=fill["fill_price"],
                        fill_evidence_grade=fill["evidence_grade"],
                        reason="intraday_composite_entry",
                        **self._resolution_fields_for_event(
                            resolution_key=resolution_key,
                            logical_order_key=logical_order_key,
                            fill=fill,
                            quote=quote,
                        ),
                        capital_account_id=account.id,
                        currency=account.currency,
                        allocated_capital=sizing["allocated_capital"],
                        cash_after=account.cash,
                        exposure_after=account.exposure,
                        realized_pnl_after=account.realized_pnl,
                        details_json=self._json_dumps({
                            "simulated": True,
                            "observation_id": observation.id,
                            "entry_leg": selected_leg,
                            "entry_leg_id": selected_leg_id,
                            "execution_index": len(existing_count) + 1,
                            "execution_allocation_fraction": execution_allocation_fraction,
                            "quantity": sizing["quantity"],
                            "fill": fill,
                            "symbol_exposure": symbol_snapshot,
                            "stop_loss": db_plan.stop_loss,
                            "target_price": db_plan.target_price,
                            "risk_reward": self._risk_reward_ratio(db_plan.stop_loss, db_plan.target_price, open_price, open_price),
                            "risk_amount": sizing["risk_amount"],
                            "sizing_reason": sizing["sizing_reason"],
                            "plan_budget_snapshot": budget_snapshot,
                            "evidence_summary": {
                                "reasons": reasons,
                                "required_rules": evidence.get("required_rules", []),
                                "factor_domains": evidence.get("factor_domains", []),
                            },
                        }),
                    )
                    session.add(event_row)
                    session.flush()
                    self._ensure_open_dispatch(session, event_row, observation=observation, account=account)
                    event = self._serialize_event(event_row)
                else:
                    observation.result = "not_triggered"
                    observation.action = "none"
                    observation.evidence_json = self._json_dumps({**evidence, "reasons": ["entry_leg_max_executions_reached"]})
            elif action == "pending_exit" and db_position is not None and close_price is not None and close_reason:
                if not getattr(db_position, "pending_exit_reason", None):
                    pending_quantity = (
                        min(float(db_position.quantity or 0.0), float(close_requested_quantity))
                        if close_requested_quantity is not None
                        else self._close_quantity_for_reason(
                            db_position,
                            close_reason,
                            requested_fraction=close_requested_fraction,
                        )
                    )
                    if pending_quantity <= 0:
                        observation.result = "failed_closed"
                        observation.action = "none"
                        observation.evidence_json = self._json_dumps({**evidence, "reasons": ["no_sellable_quantity_for_pending_exit"]})
                        session.flush()
                        return {"result": "failed_closed", "observation": self._serialize_observation(observation), "event": None}
                    db_position.pending_exit_reason = close_reason
                    db_position.pending_exit_on = event_date
                    db_position.pending_exit_price = close_price
                    db_position.available_quantity = max(0.0, float(db_position.quantity or 0.0) - pending_quantity)
                    db_position.frozen_quantity = pending_quantity
                    db_position.updated_at = utc_naive_now()
                    account = session.get(PaperCapitalAccount, db_position.capital_account_id) if db_position.capital_account_id else None
                    event_row = PaperTradeEvent(
                        event_key=(
                            f"plan:{db_plan.id}:pos:{db_position.id}:"
                            f"leg:{getattr(db_position, 'entry_leg_id', None) or 'legacy_entry'}:"
                            f"pending_exit:{event_date.isoformat()}"
                        ),
                        plan_id=db_plan.id,
                        position_id=db_position.id,
                        source_report_id=db_position.source_report_id,
                        decision_signal_id=db_position.decision_signal_id,
                        source_model=db_position.source_model,
                        market=db_position.market,
                        stock_code=db_position.stock_code,
                        stock_name=db_position.stock_name,
                        event_type="pending_exit",
                        event_date=event_date,
                        price=close_price,
                        quantity=pending_quantity,
                        reason=close_reason,
                        requested_quantity=pending_quantity,
                        executed_quantity=0.0,
                        unfilled_quantity=pending_quantity,
                        resolution_key=resolution_key,
                        logical_order_key=logical_order_key,
                        instrument_assumption="paper_pending_t1_exit_no_fill",
                        quote_provider_timestamp=str(getattr(quote, "provider_timestamp", "") or "")[:64] or None,
                        quote_fetched_at=str(getattr(quote, "fetched_at", "") or "")[:64] or None,
                        triggering_signal_id=triggering_signal.id if triggering_signal is not None else None,
                        capital_account_id=account.id if account is not None else None,
                        currency=account.currency if account is not None else getattr(db_position, "currency", None),
                        allocated_capital=self._position_allocated_capital(db_position),
                        cash_after=account.cash if account is not None else None,
                        exposure_after=account.exposure if account is not None else None,
                        realized_pnl_after=account.realized_pnl if account is not None else None,
                        details_json=self._json_dumps({
                        "simulated": True,
                        "observation_id": observation.id,
                        "quantity": pending_quantity,
                        "exit_leg": dict(exit_leg or {}),
                        "exit_leg_id": (exit_leg or {}).get("leg_id"),
                        "pending_fraction": (
                            pending_quantity / float(db_position.quantity or 0.0)
                            if float(db_position.quantity or 0.0) > 0 else None
                        ),
                        "opened_price": db_position.opened_price,
                        "pending_exit_on": event_date.isoformat(),
                        "pending_exit_price": close_price,
                        "stop_loss": db_position.stop_loss,
                        "target_price": db_position.target_price,
                        "sellability": self._sellability_evidence(db_position, event_date),
                        "fill_policy": "next_actual_session_fresh_quote",
                    }),
                    )
                    session.add(event_row)
                    session.flush()
                    db_position.pending_exit_event_id = event_row.id
                    self._ensure_lifecycle_dispatch(session, event_row, account=account, lifecycle="pending_exit")
                    event = self._serialize_event(event_row)
            elif action == "close" and db_position is not None and close_price is not None and close_reason:
                account = session.get(PaperCapitalAccount, db_position.capital_account_id) if db_position.capital_account_id else None
                fill_result, fill_error = self._apply_position_exit(
                    session,
                    account,
                    db_position,
                    quote_price=close_price,
                    reason=close_reason,
                    event_date=event_date,
                    quote=quote,
                    requested_fraction=close_requested_fraction,
                    requested_quantity=close_requested_quantity,
                    observed_at=now,
                )
                if fill_error:
                    observation.result = "failed_closed"
                    observation.action = "none"
                    observation.evidence_json = self._json_dumps({**evidence, "reasons": [fill_error]})
                    session.flush()
                    return {
                        "result": "failed_closed",
                        "observation": self._serialize_observation(observation),
                        "event": None,
                    }
                assert fill_result is not None
                self._persist_resolution(
                    session,
                    resolution_key=resolution_key,
                    bucket=resolution_bucket,
                    status="closed",
                    selected_intent="close",
                    candidate_intents=[{"intent": "close", "close_reason": close_reason}],
                    selected_intent_payload={
                        "intent": "close",
                        "close_reason": close_reason,
                        "logical_order_key": logical_order_key,
                    },
                    source_report_id=db_plan.source_report_id,
                    decision_signal_id=db_plan.decision_signal_id,
                    requested_quantity=fill_result["requested_quantity"],
                    executed_quantity=fill_result["executed_quantity"],
                    unfilled_quantity=fill_result["unfilled_quantity"],
                    evidence={**evidence, "fill": fill_result},
                )
                db_position.close_signal_id = triggering_signal.id if triggering_signal is not None else None
                if not self._plan_has_live_lots_or_pending_legs(
                    session,
                    db_plan,
                    exclude_position_id=db_position.id if db_position.status == "closed" else None,
                ):
                    db_plan.status = "closed"
                else:
                    db_plan.status = "triggered"
                db_plan.updated_at = utc_naive_now()
                event_type = "close" if fill_result.get("final_close") else "reduce"
                event_row = PaperTradeEvent(
                    event_key=(
                        f"plan:{db_plan.id}:pos:{db_position.id}:"
                        f"leg:{getattr(db_position, 'entry_leg_id', None) or 'legacy_entry'}:"
                        f"intraday_close:{bucket_naive.isoformat()}"
                    ),
                    plan_id=db_plan.id,
                    position_id=db_position.id,
                    source_report_id=db_position.source_report_id,
                    decision_signal_id=db_position.decision_signal_id,
                    source_model=db_position.source_model,
                    market=db_position.market,
                    stock_code=db_position.stock_code,
                    stock_name=db_position.stock_name,
                        event_type=event_type,
                        event_date=event_date,
                        price=fill_result["fill_price"],
                        quantity=fill_result["quantity"],
                        gross_amount=fill_result["gross_amount"],
                        fees=fill_result["fees"],
                        commission=fill_result.get("commission"),
                        tax=fill_result.get("tax", fill_result.get("sell_tax")),
                        slippage_amount=fill_result.get("slippage_amount"),
                        gross_realized_pnl=fill_result.get("realized_gross_pnl"),
                        net_realized_pnl=fill_result.get("realized_pnl"),
                        net_amount=fill_result["net_amount"],
                        fill_price=fill_result["fill_price"],
                        fill_evidence_grade=fill_result["evidence_grade"],
                        reason=close_reason,
                    **self._resolution_fields_for_event(
                        resolution_key=resolution_key,
                        logical_order_key=logical_order_key,
                        fill=fill_result,
                        quote=quote,
                    ),
                    triggering_signal_id=triggering_signal.id if triggering_signal is not None else None,
                    capital_account_id=account.id if account is not None else None,
                    currency=account.currency if account is not None else getattr(db_position, "currency", None),
                        allocated_capital=fill_result["cost_reduced"],
                        cash_after=account.cash if account is not None else None,
                        exposure_after=account.exposure if account is not None else None,
                        realized_pnl_after=account.realized_pnl if account is not None else None,
                        details_json=self._json_dumps({
                            "simulated": True,
                            "observation_id": observation.id,
                            "quantity": fill_result["quantity"],
                            "remaining_quantity": fill_result["remaining_quantity"],
                            "remaining_cost": fill_result["remaining_cost"],
                            "exit_leg": dict(exit_leg or {}),
                            "exit_leg_id": (exit_leg or {}).get("leg_id"),
                            "opened_price": db_position.opened_price,
                            "closed_price": fill_result["fill_price"],
                            "fill": fill_result,
                            "stop_loss": db_position.stop_loss,
                            "target_price": db_position.target_price,
                            "pending_exit_on": db_position.pending_exit_on.isoformat() if db_position.pending_exit_on else None,
                            "pending_exit_price": db_position.pending_exit_price,
                            "realized_pnl": fill_result["realized_pnl"],
                            "realized_gross_pnl": fill_result["realized_gross_pnl"],
                            "return_pct": db_position.simulated_return_pct,
                        }),
                )
                session.add(event_row)
                session.flush()
                self._ensure_lifecycle_dispatch(session, event_row, account=account, lifecycle="close")
                event = self._serialize_event(event_row)
            return {
                "result": result,
                "observation": self._serialize_observation(observation),
                "event": event,
            }

    def _build_rules(self, *, report, signal, raw_report: Dict[str, Any]):
        rules: List[Dict[str, Any]] = []
        descriptive: List[Dict[str, Any]] = []
        invalid: List[str] = []
        low_raw = signal.entry_low if signal is not None else report.secondary_buy
        high_raw = signal.entry_high if signal is not None else report.ideal_buy
        low = self._safe_price(low_raw)
        high = self._safe_price(high_raw)
        if low_raw is not None and low is None:
            invalid.append("invalid_entry_low")
        if high_raw is not None and high is None:
            invalid.append("invalid_entry_high")
        if low is not None and high is not None:
            if low > high:
                invalid.append("contradictory_entry_range")
            else:
                rules.append({"type": "close_between", "low": low, "high": high, "source": "decision_signal" if signal else "analysis_report"})
        elif low is not None:
            rules.append({"type": "close_gte", "value": low, "source": "decision_signal" if signal else "analysis_report"})
        elif high is not None:
            rules.append({"type": "close_lte", "value": high, "source": "decision_signal" if signal else "analysis_report"})

        stop = self._safe_price(signal.stop_loss if signal is not None else report.stop_loss)
        target = self._safe_price(signal.target_price if signal is not None else report.take_profit)
        if (signal.stop_loss if signal is not None else report.stop_loss) is not None and stop is None:
            invalid.append("invalid_stop_loss")
        if (signal.target_price if signal is not None else report.take_profit) is not None and target is None:
            invalid.append("invalid_target_price")
        if stop is not None and target is not None and stop >= target:
            invalid.append("contradictory_exit_prices")
        if self._raw_entry_legs(raw_report) is None:
            reference_low = low if low is not None else high
            reference_high = high if high is not None else low
            if stop is not None and reference_low is not None and stop >= reference_low:
                invalid.append("stop_not_below_entry")
            if target is not None and reference_high is not None and target <= reference_high:
                invalid.append("target_not_above_entry")

        for text in self._action_checklist_texts(raw_report):
            descriptive.append({
                "source": "report.dashboard.battle_plan.action_checklist",
                "text": text,
                "reason": "audit_context_only",
                "available": None,
            })

        for source, text in self._condition_texts(signal=signal, raw_report=raw_report):
            parsed_rules, fully_parsed = self._parse_condition(text, source=source)
            rules.extend(parsed_rules)
            if not fully_parsed:
                descriptive.append({
                    "source": source,
                    "text": text,
                    "reason": "unavailable_qualitative_condition",
                    "available": False,
                })

        deduped = []
        seen = set()
        for rule in rules:
            key = self._json_dumps(rule)
            if key not in seen:
                seen.add(key)
                deduped.append(rule)
        return deduped, descriptive, invalid

    def _extract_entry_legs(
        self,
        *,
        raw_report: Dict[str, Any],
        base_rules: Sequence[Mapping[str, Any]],
        entry_low: Optional[float],
        entry_high: Optional[float],
        style_settings: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        explicit = self._raw_entry_legs(raw_report)
        if explicit is None:
            price_rules = [
                dict(rule)
                for rule in base_rules
                if isinstance(rule, Mapping) and rule.get("type") in PRICE_RULE_TYPES
            ]
            if not price_rules:
                if entry_low is not None and entry_high is not None:
                    price_rules = [{"type": "close_between", "low": entry_low, "high": entry_high, "source": "legacy_ideal_secondary"}]
                elif entry_low is not None:
                    price_rules = [{"type": "close_gte", "value": entry_low, "source": "legacy_ideal_secondary"}]
                elif entry_high is not None:
                    price_rules = [{"type": "close_lte", "value": entry_high, "source": "legacy_ideal_secondary"}]
            return ([{
                "leg_id": "legacy_entry",
                "role": "initial",
                "sequence": 1,
                "allocation_fraction": 1.0,
                "max_executions": 1,
                "price_rules": price_rules,
                "prerequisites": [],
                "source": "legacy_ideal_secondary",
            }], [])
        if isinstance(explicit, Mapping):
            if explicit.get("schema_version") not in {None, ENTRY_LEG_SCHEMA_VERSION}:
                return [], ["unsupported_entry_leg_schema_version"]
            explicit = explicit.get("legs")
        if not isinstance(explicit, list) or not explicit:
            return [], ["invalid_entry_legs_payload"]
        result: List[Dict[str, Any]] = []
        errors: List[str] = []
        seen_ids = set()
        total_fraction = 0.0
        valid_roles = {"initial", "add", "pullback_add", "breakout_add"}
        for index, raw_leg in enumerate(explicit, start=1):
            if not isinstance(raw_leg, Mapping):
                errors.append("invalid_entry_leg")
                continue
            leg_id = re.sub(r"[^A-Za-z0-9_.:-]+", "_", str(raw_leg.get("leg_id") or f"leg_{index}").strip())[:64]
            if not leg_id or leg_id in seen_ids:
                errors.append("duplicate_or_empty_entry_leg_id")
                continue
            seen_ids.add(leg_id)
            role = str(raw_leg.get("role") or ("initial" if index == 1 else "add")).strip().lower()
            if role not in valid_roles:
                errors.append("invalid_entry_leg_role")
            try:
                allocation_fraction = float(raw_leg.get("allocation_fraction"))
            except (TypeError, ValueError):
                allocation_fraction = math.nan
            if not self._positive_finite(allocation_fraction) or allocation_fraction > 1.0:
                errors.append("invalid_entry_leg_fraction")
                allocation_fraction = 0.0
            total_fraction += allocation_fraction
            try:
                max_executions = max(1, int(raw_leg.get("max_executions", 1)))
            except (TypeError, ValueError):
                max_executions = 1
            raw_rules = raw_leg.get("price_rules") or raw_leg.get("rules") or []
            price_rules = []
            if isinstance(raw_rules, Mapping):
                raw_rules = [raw_rules]
            if isinstance(raw_rules, list):
                for rule in raw_rules:
                    if not isinstance(rule, Mapping):
                        continue
                    cleaned = {k: v for k, v in dict(rule).items() if k in {"type", "low", "high", "value", "source"}}
                    if cleaned.get("type") not in PRICE_RULE_TYPES:
                        continue
                    if cleaned.get("type") == "close_between":
                        low = self._safe_price(cleaned.get("low"))
                        high = self._safe_price(cleaned.get("high"))
                        if low is None or high is None or low > high:
                            errors.append("invalid_entry_leg_price_range")
                            continue
                        cleaned["low"], cleaned["high"] = low, high
                    elif cleaned.get("type") in {"close_gte", "close_lte"}:
                        value = self._safe_price(cleaned.get("value"))
                        if value is None:
                            errors.append("invalid_entry_leg_price_value")
                            continue
                        cleaned["value"] = value
                    cleaned.setdefault("source", "entry_leg")
                    price_rules.append(cleaned)
            if not price_rules:
                errors.append("entry_leg_missing_price_rule")
            prerequisites = raw_leg.get("prerequisites") if isinstance(raw_leg.get("prerequisites"), list) else []
            result.append({
                "leg_id": leg_id,
                "role": role,
                "sequence": int(raw_leg.get("sequence") or index),
                "allocation_fraction": allocation_fraction,
                "max_executions": max_executions,
                "price_rules": price_rules,
                "prerequisites": [str(item)[:80] for item in prerequisites],
                "source": "report_explicit",
            })
        if total_fraction > 1.000001:
            errors.append("entry_leg_fractions_exceed_100pct")
        settings = style_settings if isinstance(style_settings, Mapping) else {}
        try:
            entry_leg_limit = max(1, int(settings.get("entry_leg_limit") or len(result) or 1))
        except (TypeError, ValueError):
            entry_leg_limit = max(1, len(result) or 1)
        try:
            add_limit = max(0, int(settings.get("add_limit") or 0))
        except (TypeError, ValueError):
            add_limit = 0
        if len(result) > entry_leg_limit:
            errors.append("entry_leg_limit_exceeded_for_trading_style")
        add_slots = 0
        for leg in result:
            role = str(leg.get("role") or "").strip().lower()
            executions = max(1, int(leg.get("max_executions") or 1))
            if role == "initial":
                add_slots += max(0, executions - 1)
            else:
                add_slots += executions
        if add_slots > add_limit:
            errors.append("add_limit_exceeded_for_trading_style")
        result.sort(key=lambda item: (int(item.get("sequence") or 0), str(item.get("leg_id") or "")))
        return result, sorted(set(errors))

    def _entry_leg_price_bounds(
        self,
        entry_legs: Sequence[Mapping[str, Any]],
    ) -> Tuple[Optional[float], Optional[float], bool]:
        """Return finite bounds only when every explicit entry rule is bounded."""

        lows: List[float] = []
        highs: List[float] = []
        if not entry_legs:
            return None, None, False
        for leg in entry_legs:
            rules = leg.get("price_rules") if isinstance(leg, Mapping) else None
            if not isinstance(rules, list) or not rules:
                return None, None, False
            for rule in rules:
                if not isinstance(rule, Mapping) or rule.get("type") != "close_between":
                    return None, None, False
                low = self._safe_price(rule.get("low"))
                high = self._safe_price(rule.get("high"))
                if low is None or high is None or low > high:
                    return None, None, False
                lows.append(low)
                highs.append(high)
        if not lows or not highs:
            return None, None, False
        return min(lows), max(highs), True

    def _extract_exit_legs(
        self,
        *,
        raw_report: Dict[str, Any],
        entry_low: Optional[float],
        entry_high: Optional[float],
        stop: Optional[float],
    ) -> Tuple[List[Dict[str, Any]], List[str], Optional[float], Optional[float], Dict[str, Optional[float]]]:
        explicit = self._raw_exit_legs(raw_report)
        explicit_targets = self._raw_exit_targets(raw_report)
        if explicit is None:
            return [], [], None, None, explicit_targets
        if isinstance(explicit, Mapping):
            if explicit.get("schema_version") not in {None, EXIT_LEG_SCHEMA_VERSION}:
                return [], ["unsupported_exit_leg_schema_version"], None, None, explicit_targets
            explicit = explicit.get("legs")
        if not isinstance(explicit, list) or not explicit:
            return [], ["invalid_exit_legs_payload"], None, None, explicit_targets

        entry = self._entry_reference_price(entry_low, entry_high)
        stop_price = self._safe_price(stop)
        result: List[Dict[str, Any]] = []
        errors: List[str] = []
        seen_ids = set()
        total_fraction = 0.0
        prior_target: Optional[float] = None
        weighted_target = 0.0
        valid_roles = {"partial_take_profit", "final_take_profit", "take_profit"}
        for index, raw_leg in enumerate(explicit, start=1):
            if not isinstance(raw_leg, Mapping):
                errors.append("invalid_exit_leg")
                continue
            leg_id = re.sub(r"[^A-Za-z0-9_.:-]+", "_", str(raw_leg.get("leg_id") or f"tp{index}").strip())[:64]
            if not leg_id or leg_id in seen_ids:
                errors.append("duplicate_or_empty_exit_leg_id")
                continue
            seen_ids.add(leg_id)
            role = str(raw_leg.get("role") or ("final_take_profit" if index == len(explicit) else "partial_take_profit")).strip().lower()
            if role not in valid_roles:
                errors.append("invalid_exit_leg_role")
            try:
                allocation_fraction = float(raw_leg.get("allocation_fraction"))
            except (TypeError, ValueError):
                allocation_fraction = math.nan
            if not self._positive_finite(allocation_fraction) or allocation_fraction > 1.0:
                errors.append("invalid_exit_leg_fraction")
                allocation_fraction = 0.0
            target_price = self._safe_price(raw_leg.get("target_price"))
            normalized_from_price_rule = False
            if target_price is None:
                # Compatibility for reports emitted by the old prompt, whose
                # exit-leg example was underspecified and could mirror the
                # entry-leg ``close_between`` shape.  Only accept a single,
                # bounded range when its declared canonical first/final target
                # lies inside it; otherwise remain fail-closed.
                expected_target = None
                if index == 1:
                    expected_target = explicit_targets.get("first_take_profit")
                if index == len(explicit):
                    expected_target = explicit_targets.get("final_target") or expected_target
                rules = raw_leg.get("price_rules")
                if isinstance(rules, list) and len(rules) == 1 and isinstance(rules[0], Mapping):
                    rule = rules[0]
                    low = self._safe_price(rule.get("low"))
                    high = self._safe_price(rule.get("high"))
                    canonical = self._safe_price(expected_target)
                    if (
                        rule.get("type") == "close_between"
                        and low is not None
                        and high is not None
                        and low <= high
                        and canonical is not None
                        and low <= canonical <= high
                    ):
                        target_price = canonical
                        normalized_from_price_rule = True
            if target_price is None:
                errors.append("invalid_exit_leg_target_price")
                target_price = 0.0
            if entry is not None and target_price <= entry:
                errors.append("exit_leg_target_not_above_entry")
            if prior_target is not None and target_price <= prior_target:
                errors.append("exit_leg_targets_not_strictly_increasing")
            prior_target = target_price
            total_fraction += allocation_fraction
            weighted_target += allocation_fraction * target_price
            result.append({
                "leg_id": leg_id,
                "sequence": int(raw_leg.get("sequence") or index),
                "role": role,
                "allocation_fraction": allocation_fraction,
                "target_price": target_price,
                "source": (
                    "report_explicit_price_rule_normalized"
                    if normalized_from_price_rule
                    else "report_explicit"
                ),
            })
        if abs(total_fraction - 1.0) > 1e-6:
            errors.append("exit_leg_fractions_must_equal_100pct")
        sorted_result = sorted(result, key=lambda item: (int(item.get("sequence") or 0), str(item.get("leg_id") or "")))
        if sorted_result:
            first_take_profit = explicit_targets.get("first_take_profit")
            final_target = explicit_targets.get("final_target")
            first_target = self._safe_price(sorted_result[0].get("target_price"))
            final_leg_target = self._safe_price(sorted_result[-1].get("target_price"))
            if (
                first_take_profit is not None
                and first_target is not None
                and not math.isclose(first_take_profit, first_target, rel_tol=0.0, abs_tol=1e-6)
            ):
                errors.append("exit_leg_first_take_profit_mismatch")
            if (
                final_target is not None
                and final_leg_target is not None
                and not math.isclose(final_target, final_leg_target, rel_tol=0.0, abs_tol=1e-6)
            ):
                errors.append("exit_leg_final_target_mismatch")
        weighted_risk_reward = None
        if entry is not None and stop_price is not None and entry > stop_price and weighted_target > entry:
            weighted_risk_reward = (weighted_target - entry) / (entry - stop_price)
        elif not errors:
            errors.append("invalid_exit_leg_weighted_risk_reward")
        return sorted_result, sorted(set(errors)), (weighted_target if result else None), weighted_risk_reward, explicit_targets

    @staticmethod
    def _raw_exit_targets(raw_report: Dict[str, Any]) -> Dict[str, Optional[float]]:
        dashboard = raw_report.get("dashboard")
        sniper = None
        if isinstance(dashboard, Mapping):
            battle = dashboard.get("battle_plan")
            if isinstance(battle, Mapping):
                candidate = battle.get("sniper_points")
                if isinstance(candidate, Mapping):
                    sniper = candidate
        if sniper is None:
            sniper = raw_report.get("sniper_points") if isinstance(raw_report.get("sniper_points"), Mapping) else {}
        return {
            "first_take_profit": parse_sniper_value(sniper.get("first_take_profit")) if isinstance(sniper, Mapping) else None,
            "final_target": parse_sniper_value(sniper.get("final_target")) if isinstance(sniper, Mapping) else None,
        }

    def _weighted_exit_risk_reward(
        self,
        *,
        exit_legs: Sequence[Mapping[str, Any]],
        stop: Optional[float],
        entry_low: Optional[float],
        entry_high: Optional[float],
    ) -> Optional[float]:
        entry = self._entry_reference_price(entry_low, entry_high)
        stop_price = self._safe_price(stop)
        if entry is None or stop_price is None or entry <= stop_price:
            return None
        weighted_target = 0.0
        for leg in exit_legs:
            if not isinstance(leg, Mapping):
                return None
            target_price = self._safe_price(leg.get("target_price"))
            fraction = self._safe_float(leg.get("allocation_fraction"))
            if target_price is None or fraction is None:
                return None
            weighted_target += float(fraction) * target_price
        reward = weighted_target - entry
        risk = entry - stop_price
        if reward <= 0 or risk <= 0:
            return None
        return reward / risk

    @staticmethod
    def _raw_entry_legs(raw_report: Dict[str, Any]) -> Optional[Any]:
        dashboard = raw_report.get("dashboard")
        if isinstance(dashboard, Mapping):
            battle = dashboard.get("battle_plan")
            if isinstance(battle, Mapping):
                sniper = battle.get("sniper_points")
                if isinstance(sniper, Mapping) and "entry_legs" in sniper:
                    return {
                        "schema_version": sniper.get("entry_leg_schema_version"),
                        "legs": sniper.get("entry_legs"),
                    }
                if "entry_legs" in battle:
                    return battle.get("entry_legs")
        paper = raw_report.get("paper_trading")
        if isinstance(paper, Mapping) and "entry_legs" in paper:
            return paper.get("entry_legs")
        return None

    @staticmethod
    def _raw_exit_legs(raw_report: Dict[str, Any]) -> Optional[Any]:
        dashboard = raw_report.get("dashboard")
        if isinstance(dashboard, Mapping):
            battle = dashboard.get("battle_plan")
            if isinstance(battle, Mapping):
                sniper = battle.get("sniper_points")
                if isinstance(sniper, Mapping) and "exit_legs" in sniper:
                    return {
                        "schema_version": sniper.get("exit_leg_schema_version"),
                        "legs": sniper.get("exit_legs"),
                    }
                if "exit_legs" in battle:
                    return battle.get("exit_legs")
        paper = raw_report.get("paper_trading")
        if isinstance(paper, Mapping) and "exit_legs" in paper:
            return paper.get("exit_legs")
        return None

    def _plan_entry_legs(self, plan) -> List[Dict[str, Any]]:
        descriptions, _ = self._json_list(getattr(plan, "descriptive_conditions_json", "[]"))
        for item in descriptions:
            if isinstance(item, Mapping) and item.get("source") == "paper_trading.entry_legs":
                legs = item.get("entry_legs")
                if isinstance(legs, list):
                    return [dict(leg) for leg in legs if isinstance(leg, Mapping)]
        rules, _ = self._json_list(getattr(plan, "rules_json", "[]"))
        return [{
            "leg_id": "legacy_entry",
            "role": "initial",
            "sequence": 1,
            "allocation_fraction": 1.0,
            "max_executions": 1,
            "price_rules": [
                dict(rule) for rule in rules
                if isinstance(rule, Mapping) and rule.get("type") in PRICE_RULE_TYPES
            ],
            "prerequisites": [],
            "source": "legacy_ideal_secondary",
        }]

    def _plan_exit_contract(self, plan) -> Optional[Dict[str, Any]]:
        descriptions, _ = self._json_list(getattr(plan, "descriptive_conditions_json", "[]"))
        for item in descriptions:
            if isinstance(item, Mapping) and item.get("source") == "paper_trading.exit_legs":
                legs = item.get("exit_legs")
                if isinstance(legs, list):
                    return {
                        "schema_version": item.get("schema_version") or EXIT_LEG_SCHEMA_VERSION,
                        "exit_legs": [dict(leg) for leg in legs if isinstance(leg, Mapping)],
                        "weighted_target_price": item.get("weighted_target_price"),
                        "weighted_risk_reward": item.get("weighted_risk_reward"),
                        "source_target_price": item.get("source_target_price"),
                    }
        return None

    def _plan_strategy_setup(self, plan) -> Dict[str, Any]:
        descriptions, _ = self._json_list(getattr(plan, "descriptive_conditions_json", "[]"))
        for item in descriptions:
            if isinstance(item, Mapping) and item.get("source") == "paper_trading.strategy_setup":
                return {
                    "setup_type": item.get("setup_type"),
                    "factor_policy": (
                        dict(item.get("factor_policy"))
                        if isinstance(item.get("factor_policy"), Mapping)
                        else setup_factor_policy(item.get("setup_type"))
                    ),
                    "geometry_guard": (
                        dict(item.get("geometry_guard"))
                        if isinstance(item.get("geometry_guard"), Mapping)
                        else None
                    ),
                }
        rules, _ = self._json_list(getattr(plan, "rules_json", "[]"))
        setup = setup_contract({}, rules)
        return {
            "setup_type": setup.get("setup_type"),
            "factor_policy": setup_factor_policy(setup.get("setup_type")),
            "geometry_guard": None,
        }

    def _executed_exit_leg_ids_for_position(self, session, position_id: int) -> set:
        rows = session.execute(
            select(PaperTradeEvent.details_json, PaperTradeEvent.reason)
            .where(
                PaperTradeEvent.position_id == int(position_id),
                PaperTradeEvent.event_type.in_(["close", "reduce"]),
            )
        ).all()
        executed = set()
        for details_json, reason in rows:
            details, _ = self._json_object(details_json or "{}")
            leg_id = details.get("exit_leg_id") or details.get("exitLegId")
            if leg_id:
                executed.add(str(leg_id))
                continue
            reason_text = str(reason or "")
            if reason_text.startswith("take_profit:"):
                executed.add(reason_text.split(":", 1)[1])
        return executed

    def _next_exit_leg_for_position(
        self,
        session,
        *,
        plan,
        position,
        observed_high: float,
    ) -> Optional[Dict[str, Any]]:
        contract = self._plan_exit_contract(plan)
        if not contract:
            return None
        executed = self._executed_exit_leg_ids_for_position(session, int(position.id))
        for leg in sorted(contract.get("exit_legs") or [], key=lambda item: (int(item.get("sequence") or 0), str(item.get("leg_id") or ""))):
            leg_id = str(leg.get("leg_id") or "")
            if not leg_id or leg_id in executed:
                continue
            target_price = self._safe_price(leg.get("target_price"))
            if target_price is not None and observed_high >= target_price:
                return dict(leg)
        return None

    @staticmethod
    def _is_same_session_reanalysis(raw_report: Mapping[str, Any]) -> bool:
        paper = raw_report.get("paper_trading") if isinstance(raw_report, Mapping) else None
        auction = raw_report.get("auction_reanalysis") if isinstance(raw_report, Mapping) else None
        return bool(
            isinstance(paper, Mapping) and paper.get("same_session_reanalysis")
            or isinstance(auction, Mapping) and auction.get("same_session")
        )

    def _signal_metadata(self, signal) -> Dict[str, Any]:
        if signal is None:
            return {}
        metadata, error = self._json_object(getattr(signal, "metadata_json", None))
        return {} if error else metadata

    def _signal_session_date(self, signal) -> Optional[date]:
        summary = self._signal_metadata(signal).get("market_phase_summary")
        if not isinstance(summary, Mapping):
            return None
        raw = summary.get("session_date")
        try:
            return date.fromisoformat(str(raw)) if raw else None
        except (TypeError, ValueError):
            return None

    def _is_verified_same_session_reanalysis(
        self,
        *,
        signal,
        raw_report: Mapping[str, Any],
        market: str,
    ) -> bool:
        if signal is None:
            return False
        trigger_source = str(getattr(signal, "trigger_source", "") or "").strip().lower()
        source_phase = str(getattr(signal, "market_phase", "") or "").strip().lower()
        explicitly_requested = self._is_same_session_reanalysis(raw_report)
        allowed_sources = set(MANUAL_ANALYSIS_SOURCES)
        if explicitly_requested:
            allowed_sources.add("paper_trading_reanalysis")
        if trigger_source not in allowed_sources or source_phase != "intraday":
            return False
        runtime_phase = build_market_phase_context(
            market=market,
            current_time=datetime.now(timezone.utc),
            trigger_source="paper_trading_plan_activation",
        )
        phase_value = getattr(runtime_phase, "phase", None)
        runtime_phase_value = str(getattr(phase_value, "value", phase_value) or "").strip().lower()
        if runtime_phase_value != "intraday" or getattr(runtime_phase, "is_market_open_now", None) is not True:
            return False
        source_session_date = self._signal_session_date(signal)
        return (
            source_session_date is not None
            and source_session_date == self._phase_session_date(runtime_phase)
        )

    def _build_next_check_obligation(
        self,
        *,
        raw_report: Dict[str, Any],
        market: str,
        valid_for_date: date,
        report_created_at: datetime,
        style_settings: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        text_value = self._raw_next_check_time(raw_report)
        text = str(text_value or "").strip()
        if not text:
            cadence = 0
            if isinstance(style_settings, Mapping):
                try:
                    cadence = max(0, int(style_settings.get("plan_refresh_cadence_sessions") or 0))
                except (TypeError, ValueError):
                    cadence = 0
            if cadence <= 0:
                return None, None
            tz = self._market_timezone(market)
            due_date = valid_for_date
            for _ in range(cadence):
                due_date = self._next_trading_session(market, due_date)
            due_local = datetime.combine(due_date, time(14, 50), tzinfo=tz)
            return {
                "raw": f"style_plan_refresh_cadence:{cadence}",
                "due_at": due_local.astimezone(timezone.utc).replace(tzinfo=None).isoformat(),
                "market_timezone": getattr(tz, "key", str(tz)),
                "market_local_due_at": due_local.isoformat(),
                "session_date": due_local.date().isoformat(),
                "status": "pending",
                "trigger_type": "next_check",
                "source": "trading_style_plan_refresh_cadence",
                "plan_refresh_cadence_sessions": cadence,
            }, None
        lower = text.lower()
        if lower in {"n/a", "na", "none", "无", "待确认", "after confirmation"}:
            return None, "next_check_placeholder_or_descriptive"
        tz = self._market_timezone(market)
        report_dt = (
            report_created_at.astimezone(tz)
            if report_created_at.tzinfo
            else report_created_at.replace(tzinfo=tz)
        )
        due_local: Optional[datetime] = None
        try:
            if re.match(r"^\d{4}-\d{2}-\d{2}t\d{2}:\d{2}", lower):
                parsed = datetime.fromisoformat(text)
                if parsed.tzinfo is None:
                    return None, "next_check_iso_requires_offset"
                due_local = parsed.astimezone(tz)
            else:
                match = re.match(r"^(?:(next session|下个交易日)\s*)?(\d{1,2}):(\d{2})$", lower)
                if not match:
                    return None, "next_check_descriptive_only"
                explicit_next_session = bool(match.group(1))
                hour, minute = int(match.group(2)), int(match.group(3))
                if hour > 23 or minute > 59:
                    return None, "next_check_invalid_wall_time"
                due_date = (
                    self._next_trading_session(market, report_dt.date())
                    if explicit_next_session
                    else valid_for_date
                )
                due_local = datetime.combine(due_date, time(hour, minute), tzinfo=tz)
        except (TypeError, ValueError):
            return None, "next_check_invalid_time"
        if due_local is None:
            return None, "next_check_descriptive_only"
        if not self._next_check_within_supported_session(market, due_local):
            return None, "next_check_closed_session_target"
        if due_local <= report_dt:
            return None, "next_check_stale_past_time"
        return {
            "raw": text,
            "due_at": due_local.astimezone(timezone.utc).replace(tzinfo=None).isoformat(),
            "market_timezone": getattr(tz, "key", str(tz)),
            "market_local_due_at": due_local.isoformat(),
            "session_date": due_local.date().isoformat(),
            "status": "pending",
            "trigger_type": "next_check",
        }, None

    @staticmethod
    def _raw_next_check_time(raw_report: Dict[str, Any]) -> Optional[Any]:
        dashboard = raw_report.get("dashboard") if isinstance(raw_report, Mapping) else None
        if isinstance(dashboard, Mapping):
            phase = dashboard.get("phase_decision")
            if isinstance(phase, Mapping):
                return phase.get("next_check_time")
        return None

    @staticmethod
    def _market_timezone(market: str) -> ZoneInfo:
        market_norm = str(market or "").lower()
        if market_norm == "us":
            return ZoneInfo("America/New_York")
        if market_norm in {"jp"}:
            return ZoneInfo("Asia/Tokyo")
        if market_norm in {"kr"}:
            return ZoneInfo("Asia/Seoul")
        return ZoneInfo("Asia/Shanghai")

    @staticmethod
    def _next_check_within_supported_session(market: str, due_local: datetime) -> bool:
        market_norm = str(market or "").lower()
        local_time = due_local.time()
        if market_norm == "cn":
            return time(9, 15) <= local_time <= time(11, 30) or time(13, 0) <= local_time <= time(15, 0)
        if market_norm == "hk":
            return time(9, 30) <= local_time <= time(12, 0) or time(13, 0) <= local_time <= time(16, 0)
        if market_norm == "us":
            return time(9, 30) <= local_time <= time(16, 0)
        return time(9, 0) <= local_time <= time(16, 0)

    def _market_local_naive_to_utc_naive(
        self,
        market: str,
        local_date: date,
        local_time: time,
    ) -> datetime:
        tz = self._market_timezone(market)
        local_dt = datetime.combine(local_date, local_time, tzinfo=tz)
        return local_dt.astimezone(timezone.utc).replace(tzinfo=None)

    def _action_checklist_texts(self, raw_report: Dict[str, Any]) -> List[str]:
        dashboard = raw_report.get("dashboard")
        if not isinstance(dashboard, dict):
            return []
        battle = dashboard.get("battle_plan")
        if not isinstance(battle, dict) or not isinstance(battle.get("action_checklist"), list):
            return []
        result = []
        seen = set()
        for item in battle["action_checklist"]:
            text = str(item).strip()
            key = re.sub(r"\s+", " ", text)
            if text and key not in seen:
                seen.add(key)
                result.append(text)
        return result

    def _condition_texts(self, *, signal, raw_report: Dict[str, Any]) -> List[Tuple[str, str]]:
        values: List[Tuple[str, str]] = []
        if signal is not None and signal.watch_conditions:
            parsed, _ = self._json_value(signal.watch_conditions)
            items = parsed if isinstance(parsed, list) else [signal.watch_conditions]
            values.extend(("decision_signal.watch_conditions", str(item).strip()) for item in items)
        dashboard = raw_report.get("dashboard")
        if isinstance(dashboard, dict):
            phase = dashboard.get("phase_decision")
            if isinstance(phase, dict) and isinstance(phase.get("watch_conditions"), list):
                values.extend(("report.dashboard.phase_decision.watch_conditions", str(item).strip()) for item in phase["watch_conditions"])
        result = []
        seen = set()
        for source, text in values:
            key = re.sub(r"\s+", " ", text)
            if text and key not in seen:
                seen.add(key)
                result.append((source, text))
        return result

    def _parse_condition(self, text: str, *, source: str) -> Tuple[List[Dict[str, Any]], bool]:
        """Parse every recognized clause and prove the entire text was consumed."""

        compact = re.sub(r"\s+", " ", text.strip())
        lower = compact.lower()
        rules: List[Dict[str, Any]] = []
        spans: List[Tuple[int, int]] = []

        def overlaps(start: int, end: int) -> bool:
            return any(start < used_end and end > used_start for used_start, used_end in spans)

        def add(match: re.Match[str], rule: Dict[str, Any]) -> None:
            if overlaps(match.start(), match.end()):
                return
            spans.append((match.start(), match.end()))
            rule.update({"source_text": text, "source": source})
            rules.append(rule)

        for match in re.finditer(
            r"(?:站上|收于|above)\s*(?:ma|sma)\s*(\d{1,3})",
            lower,
            re.IGNORECASE,
        ):
            period = int(match.group(1))
            if 2 <= period <= 250:
                add(match, {"type": "close_above_prior_sma", "period": period})

        for match in re.finditer(
            r"(?:突破|break(?:s|out)?(?: above)?)\s*(\d{1,3})\s*(?:日|day)[^,，;；且&]*(?:高|high)",
            lower,
            re.IGNORECASE,
        ):
            period = int(match.group(1))
            if 2 <= period <= 250:
                add(match, {"type": "close_above_prior_high", "period": period})

        for match in re.finditer(
            r"(?:站稳|突破|above)\s*(?:前高|prior\s+high)\s*(?:为|at)?\s*(\d+(?:\.\d+)?)",
            lower,
            re.IGNORECASE,
        ):
            value = float(match.group(1))
            if self._positive_finite(value):
                add(match, {"type": "close_gte", "value": value})

        for match in re.finditer(
            r"(?:成交量|volume)\s*(?:达到|至少为|>=|≥|at\s+least|is)?\s*(?:过去|前|prior)?\s*(\d{1,3})\s*(?:日|day|session)[^,，;；且&]*(\d+(?:\.\d+)?)\s*(?:x|倍)",
            lower,
            re.IGNORECASE,
        ):
            period, ratio = int(match.group(1)), float(match.group(2))
            if 2 <= period <= 250 and 1.0 <= ratio <= 20.0:
                add(match, {
                    "type": "volume_ratio_gte_prior_avg",
                    "ratio": ratio,
                    "period": period,
                    "threshold_source": "source_text",
                })

        for match in re.finditer(
            r"(?:量比|volume\s+ratio)[^0-9]{0,20}(?:>=|≥|至少|放大至|达到|above|over|at\s+least)?\s*(\d+(?:\.\d+)?)\s*(?:以上|x|倍)?",
            lower,
            re.IGNORECASE,
        ):
            ratio = float(match.group(1))
            if 1.0 <= ratio <= 20.0:
                add(match, {
                    "type": "volume_ratio_gte_prior_avg",
                    "ratio": ratio,
                    "period": VOLUME_POLICY_SESSIONS,
                    "threshold_source": "source_text",
                    "period_source": "policy_default",
                    "policy_version": PLAN_VERSION,
                })

        has_explicit_volume = any(
            rule.get("type") == "volume_ratio_gte_prior_avg"
            and rule.get("threshold_source") == "source_text"
            for rule in rules
        )
        for match in re.finditer(r"放量|volume\s+(?:breakout|expansion)", lower, re.IGNORECASE):
            if overlaps(match.start(), match.end()):
                continue
            if has_explicit_volume:
                # Consume the qualitative synonym without stacking the v1
                # default over a threshold explicitly supplied in this text.
                spans.append((match.start(), match.end()))
            else:
                add(match, {
                    "type": "volume_ratio_gte_prior_avg",
                    "ratio": VOLUME_POLICY_RATIO,
                    "period": VOLUME_POLICY_SESSIONS,
                    "threshold_source": "policy_default",
                    "policy_version": PLAN_VERSION,
                })

        remaining = list(lower)
        for start, end in spans:
            remaining[start:end] = " " * (end - start)
        residual = "".join(remaining)
        residual = re.sub(
            r"(?:并且|同时|而且|且|and|then|[&,+，;；()（）])",
            "",
            residual,
            flags=re.IGNORECASE,
        )
        residual = re.sub(
            r"(?:收盘|收于|收在|close)",
            "",
            residual,
            flags=re.IGNORECASE,
        )
        fully_parsed = bool(rules) and not residual.strip()
        return rules, fully_parsed

    def _evaluate_and_open_plan(self, plan, eval_date: date, bars: List[Dict[str, Any]]):
        with self._ledger_write_session() as session:
            db_plan = session.get(PaperExecutionPlan, plan.id)
            validity_outcome = self._eod_validity_outcome(db_plan, eval_date)
            existing = session.execute(select(PaperPlanEvaluation).where(
                PaperPlanEvaluation.evaluation_key == f"plan:{plan.id}:leg:plan_entry:pos:0:eod:{eval_date.isoformat()}",
            )).scalar_one_or_none()
            if existing is not None and existing.result != "failed_closed":
                return None
            if validity_outcome is not None:
                result, reasons, evidence = validity_outcome
                evaluation = existing or PaperPlanEvaluation(
                    evaluation_key=f"plan:{db_plan.id}:leg:plan_entry:pos:0:eod:{eval_date.isoformat()}",
                    plan_id=db_plan.id,
                    position_id=None,
                    entry_leg_id="plan_entry",
                    evaluation_date=eval_date,
                    result=result,
                )
                evaluation.result = result
                evaluation.reasons_json = self._json_dumps(reasons)
                evaluation.evidence_json = self._json_dumps(evidence)
                if existing is None:
                    session.add(evaluation)
                if result == "expired" and db_plan.status == "active":
                    db_plan.status = "expired"
                    db_plan.updated_at = utc_naive_now()
                session.flush()
                return {"evaluation": self._serialize_evaluation(evaluation), "event": None}
            result, reasons, evidence, current = self._evaluate_entry(db_plan, eval_date, bars)
            candidates = []
            if result == "triggered" and current is not None:
                candidates.append({
                    "kind": "entry",
                    "action": "open",
                    "reason": "eod_entry_rules_triggered",
                    "price": current["close"],
                })
            else:
                candidates.append({
                    "kind": "none",
                    "action": "none",
                    "reason": reasons[0] if reasons else result,
                    "eligible": result != "failed_closed",
                })
            resolution = self.resolve_paper_intents(
                candidates=candidates,
                gates={"fail_closed": result == "failed_closed", "reasons": reasons},
                state={"entry": "eod_open", "plan_id": db_plan.id, "evaluation_date": eval_date.isoformat()},
            )
            evidence = {**evidence, "intent_resolution": resolution}
            chosen = resolution.get("chosen")
            if result not in {"failed_closed", "skipped", "expired"} and not (chosen and chosen.get("action") == "open"):
                result = "not_triggered"
                reasons = resolution.get("reasons") or reasons
            evaluation = existing or PaperPlanEvaluation(
                evaluation_key=f"plan:{db_plan.id}:leg:plan_entry:pos:0:eod:{eval_date.isoformat()}",
                plan_id=db_plan.id,
                position_id=None,
                entry_leg_id="plan_entry",
                evaluation_date=eval_date,
                result=result,
            )
            evaluation.result = result
            evaluation.reasons_json = self._json_dumps(reasons)
            evaluation.evidence_json = self._json_dumps(evidence)
            if existing is None:
                session.add(evaluation)
            event = None
            if result == "triggered" and current is not None:
                legacy_leg = self._plan_entry_legs(db_plan)[0] if self._plan_entry_legs(db_plan) else {"leg_id": "legacy_entry", "allocation_fraction": 1.0}
                legacy_leg_id = str(legacy_leg.get("leg_id") or "legacy_entry")[:64]
                existing_position = session.execute(select(PaperPosition).where(
                    PaperPosition.plan_id == db_plan.id,
                    PaperPosition.entry_leg_id == legacy_leg_id,
                ).limit(1)).scalar_one_or_none()
                if existing_position is None:
                    price = current["close"]
                    account = self._get_or_create_account(
                        session,
                        source_model=db_plan.source_model,
                        market=db_plan.market,
                    )
                    resolution_bucket = self._resolution_bucket(
                        mode="eod_open",
                        source_model=db_plan.source_model,
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        currency=account.currency,
                        capital_account_id=account.id,
                        evaluation_date=eval_date,
                    )
                    resolution_key = self._resolution_key(resolution_bucket)
                    logical_order_key = "ord-" + hashlib.sha256(
                        f"{resolution_key}|open".encode("utf-8")
                    ).hexdigest()[:24]
                    budget_snapshot = self._ensure_plan_budget_snapshot(db_plan, account)
                    eod_current_time = self._market_local_naive_to_utc_naive(
                        db_plan.market,
                        eval_date,
                        MARKET_REGULAR_CLOSE.get(db_plan.market, time(15, 0)),
                    )
                    market_policy_decision = self._market_policy_decision_in_session(
                        session,
                        account=account,
                        market=db_plan.market,
                        current_time=eod_current_time,
                    )
                    symbol_cap_factor = float(market_policy_decision.get("symbol_cap_factor", 1.0) or 0.0)
                    total_cap_factor = float(market_policy_decision.get("total_cap_factor", 1.0) or 0.0)
                    sizing, sizing_error = self._size_position(
                        account,
                        price=price,
                        stop_loss=db_plan.stop_loss,
                        market=db_plan.market,
                        allocation_fraction=float(legacy_leg.get("allocation_fraction") or 1.0),
                        existing_holding=False,
                        allocation_budget=db_plan.plan_allocation_budget,
                        risk_budget=db_plan.plan_risk_budget,
                        risk_multiplier=self._style_risk_multiplier_for_plan(db_plan),
                        decision=market_policy_decision,
                    )
                    if sizing_error:
                        evaluation.result = "failed_closed"
                        sizing_reasons = [sizing_error]
                        evaluation.reasons_json = self._json_dumps(sizing_reasons)
                        evaluation.evidence_json = self._json_dumps({**evidence, "sizing_error": sizing_error})
                        session.flush()
                        return {"evaluation": self._serialize_evaluation(evaluation), "event": None}
                    symbol_snapshot = self._symbol_exposure_snapshot(
                        session,
                        account,
                        source_model=db_plan.source_model,
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        projected_additional_cost=sizing["allocated_capital"],
                        symbol_cap_factor=symbol_cap_factor,
                    )
                    if not symbol_snapshot["within_limit"]:
                        evaluation.result = "failed_closed"
                        evaluation.reasons_json = self._json_dumps(["symbol_exposure_cap_exceeded"])
                        evaluation.evidence_json = self._json_dumps({**evidence, "symbol_exposure": symbol_snapshot})
                        session.flush()
                        return {"evaluation": self._serialize_evaluation(evaluation), "event": None}
                    account_risk_gate = self._account_risk_gate_for_open(
                        session,
                        account=account,
                        plan=db_plan,
                        projected_additional_cost=sizing["allocated_capital"],
                        current_time=eod_current_time,
                        bucket_started_at=datetime.combine(eval_date, time.min),
                        resolution_key=resolution_key,
                        decision=market_policy_decision,
                    )
                    if not account_risk_gate["allowed"]:
                        risk_reasons = [f"account_risk_lock:{reason}" for reason in account_risk_gate["reasons"]]
                        evaluation.result = "failed_closed"
                        evaluation.reasons_json = self._json_dumps(risk_reasons)
                        evaluation.evidence_json = self._json_dumps({
                            **evidence,
                            "symbol_exposure": symbol_snapshot,
                            "account_risk_gate": account_risk_gate,
                        })
                        self._persist_resolution(
                            session,
                            resolution_key=resolution_key,
                            bucket=resolution_bucket,
                            status="failed_closed",
                            selected_intent=None,
                            candidate_intents=[{"intent": "open", "source": "eod_entry_rules"}],
                            suppressed_intents=[{"intent": "open", "reasons": risk_reasons}],
                            source_report_id=db_plan.source_report_id,
                            decision_signal_id=db_plan.decision_signal_id,
                            requested_quantity=sizing.get("quantity"),
                            executed_quantity=0.0,
                            unfilled_quantity=sizing.get("quantity"),
                            evidence={
                                **evidence,
                                "symbol_exposure": symbol_snapshot,
                                "account_risk_gate": account_risk_gate,
                                "reasons": risk_reasons,
                            },
                        )
                        session.flush()
                        return {"evaluation": self._serialize_evaluation(evaluation), "event": None}
                    fill, fill_error = self._fill_assumption(
                        side="buy",
                        quote_price=price,
                        quantity=sizing["quantity"],
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                    )
                    if fill_error:
                        evaluation.result = "failed_closed"
                        evaluation.reasons_json = self._json_dumps([fill_error])
                        evaluation.evidence_json = self._json_dumps({**evidence, "fill_error": fill_error})
                        session.flush()
                        return {"evaluation": self._serialize_evaluation(evaluation), "event": None}
                    assert fill is not None
                    symbol_snapshot = self._symbol_exposure_snapshot(
                        session,
                        account,
                        source_model=db_plan.source_model,
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        projected_additional_cost=fill["net_amount"],
                        symbol_cap_factor=symbol_cap_factor,
                    )
                    invariant_error = self._open_fill_invariant_error(
                        account=account,
                        fill=fill,
                        symbol_snapshot=symbol_snapshot,
                        total_cap_factor=total_cap_factor,
                    )
                    if invariant_error:
                        evaluation.result = "failed_closed"
                        evaluation.reasons_json = self._json_dumps([invariant_error])
                        evaluation.evidence_json = self._json_dumps({
                            **evidence,
                            "fill_error": invariant_error,
                            "symbol_exposure": symbol_snapshot,
                            "fill": fill,
                        })
                        session.flush()
                        return {"evaluation": self._serialize_evaluation(evaluation), "event": None}
                    account_risk_gate = self._account_risk_gate_for_open(
                        session,
                        account=account,
                        plan=db_plan,
                        projected_additional_cost=fill["net_amount"],
                        current_time=eod_current_time,
                        bucket_started_at=datetime.combine(eval_date, time.min),
                        resolution_key=resolution_key,
                        decision=market_policy_decision,
                    )
                    if not account_risk_gate["allowed"]:
                        risk_reasons = [f"account_risk_lock:{reason}" for reason in account_risk_gate["reasons"]]
                        evaluation.result = "failed_closed"
                        evaluation.reasons_json = self._json_dumps(risk_reasons)
                        evaluation.evidence_json = self._json_dumps({
                            **evidence,
                            "symbol_exposure": symbol_snapshot,
                            "account_risk_gate": account_risk_gate,
                            "fill": fill,
                        })
                        session.flush()
                        return {"evaluation": self._serialize_evaluation(evaluation), "event": None}
                    sizing["allocated_capital"] = fill["net_amount"]
                    self._persist_resolution(
                        session,
                        resolution_key=resolution_key,
                        bucket=resolution_bucket,
                        status="triggered",
                        selected_intent="open",
                        candidate_intents=[{"intent": "open", "source": "eod_entry_rules"}],
                        selected_intent_payload={"intent": "open", "logical_order_key": logical_order_key},
                        source_report_id=db_plan.source_report_id,
                        decision_signal_id=db_plan.decision_signal_id,
                        requested_quantity=fill["requested_quantity"],
                        executed_quantity=fill["executed_quantity"],
                        unfilled_quantity=fill["unfilled_quantity"],
                        evidence={**evidence, "symbol_exposure": symbol_snapshot},
                    )
                    self._debit_account_for_open(account, sizing, count_new_holding=True)
                    available_quantity, frozen_quantity = self._initial_sellability_quantities(
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        quantity=sizing["quantity"],
                    )
                    position = PaperPosition(
                        plan_id=db_plan.id,
                        entry_leg_id=legacy_leg_id,
                        execution_index=1,
                        source_report_id=db_plan.source_report_id,
                        decision_signal_id=db_plan.decision_signal_id,
                        source_model=db_plan.source_model,
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        stock_name=db_plan.stock_name,
                        opened_on=eval_date,
                        opened_at=self._market_local_naive_to_utc_naive(
                            db_plan.market,
                            eval_date,
                            MARKET_REGULAR_CLOSE.get(db_plan.market, time(15, 0)),
                        ),
                        opened_price=fill["fill_price"],
                        normalized_notional=sizing["allocated_capital"],
                        quantity=sizing["quantity"],
                        initial_quantity=sizing["quantity"],
                        available_quantity=available_quantity,
                        frozen_quantity=frozen_quantity,
                        capital_account_id=account.id,
                        currency=sizing["currency"],
                        allocated_capital=sizing["allocated_capital"],
                        initial_allocated_capital=sizing["allocated_capital"],
                        remaining_cost=sizing["allocated_capital"],
                        realized_quantity=0.0,
                        realized_proceeds=0.0,
                        realized_fees=0.0,
                        realized_pnl=0.0,
                        lot_size=sizing["lot_size"],
                        risk_amount=sizing["risk_amount"],
                        risk_per_share=sizing["risk_per_share"],
                        sizing_reason=sizing["sizing_reason"],
                        legacy_fixed_notional=False,
                        sellability_rule=self._sellability_rule_for(db_plan.market, db_plan.stock_code),
                        stop_loss=db_plan.stop_loss,
                        target_price=db_plan.target_price,
                        max_holding_sessions=db_plan.max_holding_sessions,
                        trading_style=db_plan.trading_style,
                        trading_style_policy_version=db_plan.trading_style_policy_version,
                        trading_style_settings_json=db_plan.trading_style_settings_json,
                        market_regime_context_id=db_plan.market_regime_context_id,
                        market_regime=db_plan.market_regime,
                        market_regime_policy_version=db_plan.market_regime_policy_version,
                    )
                    session.add(position)
                    session.flush()
                    self._sync_open_position_count_in_session(session, account)
                    event_row = PaperTradeEvent(
                        event_key=f"plan:{db_plan.id}:leg:{legacy_leg_id}:exec:1:open:{eval_date.isoformat()}",
                        plan_id=db_plan.id,
                        position_id=position.id,
                        source_report_id=db_plan.source_report_id,
                        decision_signal_id=db_plan.decision_signal_id,
                        source_model=db_plan.source_model,
                        market=db_plan.market,
                        stock_code=db_plan.stock_code,
                        stock_name=db_plan.stock_name,
                        event_type="open",
                        event_date=eval_date,
                        price=fill["fill_price"],
                        quantity=sizing["quantity"],
                        gross_amount=fill["gross_amount"],
                        fees=fill["fees"],
                        commission=fill.get("commission"),
                        tax=fill.get("tax", fill.get("sell_tax")),
                        slippage_amount=fill.get("slippage_amount"),
                        net_amount=fill["net_amount"],
                        fill_price=fill["fill_price"],
                        fill_evidence_grade=fill["evidence_grade"],
                        reason="all_required_rules_passed",
                        **self._resolution_fields_for_event(
                            resolution_key=resolution_key,
                            logical_order_key=logical_order_key,
                            fill=fill,
                        ),
                        capital_account_id=account.id,
                        currency=account.currency,
                        allocated_capital=sizing["allocated_capital"],
                        cash_after=account.cash,
                        exposure_after=account.exposure,
                        realized_pnl_after=account.realized_pnl,
                        details_json=self._json_dumps({
                            "evaluation_id": None,
                            "simulated": True,
                            "entry_leg_id": legacy_leg_id,
                            "entry_leg": legacy_leg,
                            "execution_index": 1,
                            "quantity": sizing["quantity"],
                            "fill": fill,
                            "symbol_exposure": symbol_snapshot,
                            "stop_loss": db_plan.stop_loss,
                            "target_price": db_plan.target_price,
                            "risk_reward": self._risk_reward_ratio(db_plan.stop_loss, db_plan.target_price, price, price),
                            "risk_amount": sizing["risk_amount"],
                            "sizing_reason": sizing["sizing_reason"],
                            "plan_budget_snapshot": budget_snapshot,
                            "evidence_summary": evidence,
                        }),
                    )
                    session.add(event_row)
                    session.flush()
                    self._ensure_open_dispatch(session, event_row, evaluation=evaluation, account=account)
                    db_plan.status = "triggered"
                    db_plan.updated_at = utc_naive_now()
                    event = self._serialize_event(event_row)
            session.flush()
            return {"evaluation": self._serialize_evaluation(evaluation), "event": event}

    def _evaluate_entry(self, plan, eval_date: date, bars: List[Dict[str, Any]]):
        if not plan.executable or plan.status != "active":
            return "failed_closed", ["plan_not_executable_or_active"], {}, None
        if eval_date <= plan.report_date:
            return "skipped", ["evaluation_must_be_after_report_date"], {"report_date": plan.report_date.isoformat()}, None
        current, prior, error = self._split_complete_bars(bars, eval_date)
        if error:
            return "failed_closed", [error], {}, None
        rules, rules_error = self._json_list(plan.rules_json)
        if rules_error or not rules:
            return "failed_closed", [rules_error or "missing_rules"], {}, current
        evidence: Dict[str, Any] = {"bar_date": eval_date.isoformat(), "close": current["close"], "rules": []}
        reasons: List[str] = []
        all_passed = True
        for rule in rules:
            passed, rule_evidence, reason = self._evaluate_rule(rule, current, prior)
            evidence["rules"].append(rule_evidence)
            if not passed:
                all_passed = False
                reasons.append(reason)
        if all_passed:
            policy = self._entry_policy(plan, current, prior, mode="eod")
            evidence["policy"] = policy
            if policy["passed"]:
                return "triggered", ["required_rules_and_composite_factors_passed"], evidence, current
            if policy["fail_closed"]:
                return "failed_closed", policy["reasons"], evidence, current
            return "not_triggered", policy["reasons"], evidence, current
        if any(reason.startswith("insufficient_prior_") or reason.startswith("invalid_rule") for reason in reasons):
            return "failed_closed", reasons, evidence, current
        return "not_triggered", reasons, evidence, current

    def _evaluate_rule(self, rule, current, prior):
        if not isinstance(rule, dict) or rule.get("type") not in SUPPORTED_RULE_TYPES:
            return False, {"rule": rule, "passed": False}, "invalid_rule_type"
        rule_type = rule["type"]
        close = current["close"]
        evidence = {"rule": rule, "passed": False}
        try:
            if rule_type == "close_between":
                passed = float(rule["low"]) <= close <= float(rule["high"])
            elif rule_type == "close_gte":
                passed = close >= float(rule["value"])
            elif rule_type == "close_lte":
                passed = close <= float(rule["value"])
            else:
                period = int(rule["period"])
                if len(prior) < period:
                    return False, evidence, f"insufficient_prior_{period}_sessions"
                window = prior[-period:]
                if rule_type == "close_above_prior_sma":
                    threshold = sum(bar["close"] for bar in window) / period
                    passed = close > threshold
                elif rule_type == "close_above_prior_high":
                    threshold = max(bar["high"] for bar in window)
                    passed = close > threshold
                else:
                    threshold = sum(bar["volume"] for bar in window) / period * float(rule["ratio"])
                    passed = current["volume"] >= threshold
                evidence.update({
                    "prior_window_dates": [bar["date"].isoformat() for bar in window],
                    "threshold": threshold,
                })
            evidence["passed"] = bool(passed)
            evidence["observed_close"] = close
            if rule_type == "volume_ratio_gte_prior_avg":
                evidence["observed_volume"] = current["volume"]
            return bool(passed), evidence, "rule_passed" if passed else f"rule_not_met:{rule_type}"
        except (KeyError, TypeError, ValueError, OverflowError):
            return False, evidence, f"invalid_rule:{rule_type}"

    def _evaluate_intraday_required_rules(self, rules, price, quote, prior_bars, evidence):
        all_passed = True
        reasons: List[str] = []
        for rule in rules:
            if self._intraday_rule_mode(rule) == "close_only":
                evidence["required_rules"].append({"rule": rule, "mode": "close_only", "available": False, "passed": False})
                all_passed = False
                reasons.append("required_rule_unavailable:close_only")
                continue
            passed, rule_evidence, reason = self._evaluate_intraday_rule(rule, price, quote, prior_bars)
            evidence["required_rules"].append(rule_evidence)
            if not passed:
                all_passed = False
                reasons.append(reason)
        return all_passed, reasons

    def _opening_gap_veto(self, rules, price: float, phase) -> Optional[Dict[str, Any]]:
        local_time = getattr(phase, "market_local_time", None)
        if not isinstance(local_time, datetime):
            return None
        if str(getattr(phase, "market", "") or "").lower() != "cn":
            return None
        if not (time(9, 30) <= local_time.time() < time(9, 45)):
            return None
        upper_bound = self._entry_upper_bound(rules)
        if upper_bound is None or price <= upper_bound:
            return None
        return {
            "type": "opening_gap_veto",
            "policy": "do_not_chase_above_entry_upper_bound",
            "observed_price": price,
            "entry_upper_bound": upper_bound,
            "market_local_time": local_time.isoformat(),
        }

    @staticmethod
    def _entry_upper_bound(rules) -> Optional[float]:
        candidates: List[float] = []
        for rule in rules or []:
            if not isinstance(rule, dict):
                continue
            try:
                if rule.get("type") == "close_between":
                    candidates.append(float(rule["high"]))
                elif rule.get("type") == "close_lte":
                    candidates.append(float(rule["value"]))
            except (TypeError, ValueError, KeyError):
                continue
        if not candidates:
            return None
        return min(candidates)

    def _evaluate_intraday_rule(self, rule, price, quote, prior_bars):
        if not isinstance(rule, dict) or rule.get("type") not in SUPPORTED_RULE_TYPES:
            return False, {"rule": rule, "available": False, "passed": False}, "invalid_rule_type"
        rule_type = rule["type"]
        item = {"rule": rule, "mode": "intraday", "available": True, "passed": False, "observed_price": price}
        try:
            if rule_type == "close_between":
                passed = float(rule["low"]) <= price <= float(rule["high"])
            elif rule_type == "close_gte":
                passed = price >= float(rule["value"])
            elif rule_type == "close_lte":
                passed = price <= float(rule["value"])
            elif rule_type == "volume_ratio_gte_prior_avg":
                ratio = self._quote_volume_ratio(quote, prior_bars)
                if not self._positive_finite(ratio):
                    return False, {**item, "available": False}, "required_rule_unavailable:volume_ratio"
                threshold = float(rule["ratio"])
                passed = float(ratio) >= threshold
                item.update({"observed_volume_ratio": float(ratio), "threshold": threshold})
            else:
                period = int(rule["period"])
                complete = [bar for bar in prior_bars if self._bar_complete(bar)]
                if len(complete) < period:
                    return False, item, f"insufficient_prior_{period}_sessions"
                window = complete[-period:]
                if rule_type == "close_above_prior_sma":
                    threshold = sum(float(bar["close"]) for bar in window) / period
                    passed = price > threshold
                else:
                    threshold = max(float(bar["high"]) for bar in window)
                    passed = price > threshold
                item.update({
                    "threshold": threshold,
                    "prior_window_dates": [bar["date"].isoformat() for bar in window],
                })
            item["passed"] = bool(passed)
            return bool(passed), item, "rule_passed" if passed else f"rule_not_met:{rule_type}"
        except (KeyError, TypeError, ValueError, OverflowError):
            return False, item, f"invalid_rule:{rule_type}"

    def _intraday_factor_domains(self, plan, price, quote, prior_bars):
        complete = [bar for bar in prior_bars if self._bar_complete(bar)]
        current = self._current_from_quote(price, quote)
        style_settings = self._style_settings_for_plan(plan)
        domains = []
        domains.append(self._factor_conviction(plan))
        domains.append(self._factor_momentum(price, quote, complete))
        domains.append(self._factor_volume(quote, current, complete, mode="intraday"))
        domains.append(self._factor_trend(price, complete, style_settings=style_settings))
        domains.append(self._factor_breakout(price, quote, complete, current, style_settings=style_settings))
        domains.extend(self._setup_factor_domains(plan, price, current, complete, quote=quote, mode="intraday"))
        return domains

    def _factor_conviction(self, plan):
        signal = self.repo.get_signal(plan.decision_signal_id) if plan.decision_signal_id else None
        score = signal.score if signal is not None else None
        available = score is not None
        passed = available and int(score) >= 60 and plan.action in {"buy", "add"}
        return {
            "domain": "report_conviction", "available": available,
            "passed": bool(passed), "score": score, "weight": 0.10,
            "counts_for_entry": False,
        }

    def _factor_momentum(self, price, quote, complete):
        change_pct = getattr(quote, "change_pct", None)
        pre_close = getattr(quote, "pre_close", None)
        if self._safe_float(pre_close) is None and complete:
            pre_close = complete[-1].get("close")
        parsed_change = self._safe_float(change_pct)
        available = parsed_change is not None or self._positive_finite(pre_close)
        passed = False
        if parsed_change is not None:
            passed = 0 <= parsed_change <= 8
        elif self._positive_finite(pre_close):
            change_pct = (price / float(pre_close) - 1.0) * 100.0
            passed = 0 <= change_pct <= 8
        return {
            "domain": "momentum", "available": available, "passed": bool(passed),
            "change_pct": change_pct, "pre_close": pre_close, "weight": FACTOR_WEIGHTS["momentum"],
        }

    def _factor_volume(self, quote, current, complete, *, mode: str):
        ratio = getattr(quote, "volume_ratio", None) if quote is not None else None
        if (
            mode == "eod"
            and not self._positive_finite(ratio)
            and current is not None
            and len(complete) >= VOLUME_POLICY_SESSIONS
        ):
            avg = sum(float(bar["volume"]) for bar in complete[-VOLUME_POLICY_SESSIONS:]) / VOLUME_POLICY_SESSIONS
            ratio = float(current["volume"]) / avg
        available = self._positive_finite(ratio)
        return {
            "domain": "volume",
            "available": available,
            "passed": bool(available and float(ratio) >= VOLUME_POLICY_RATIO),
            "volume_ratio": ratio,
            "threshold": VOLUME_POLICY_RATIO,
            "weight": FACTOR_WEIGHTS["volume"],
        }

    def _factor_trend(self, price, complete, *, style_settings: Optional[Mapping[str, Any]] = None):
        windows = (
            style_settings.get("core_ma_windows")
            if isinstance(style_settings, Mapping)
            else None
        )
        if not isinstance(windows, Sequence) or isinstance(windows, (str, bytes)) or len(windows) < 2:
            windows = [5, 10]
        try:
            fast_window = max(1, int(windows[0]))
            slow_window = max(fast_window, int(windows[1]))
        except (TypeError, ValueError):
            fast_window, slow_window = 5, 10
        if len(complete) < slow_window:
            return {"domain": "trend", "available": False, "passed": False, "reason": "insufficient_prior_sessions"}
        fast_ma = sum(float(bar["close"]) for bar in complete[-fast_window:]) / fast_window
        slow_ma = sum(float(bar["close"]) for bar in complete[-slow_window:]) / slow_window
        return {
            "domain": "trend", "available": True,
            "passed": price > fast_ma and fast_ma >= slow_ma,
            "fast_ma": fast_ma,
            "slow_ma": slow_ma,
            "fast_window": fast_window,
            "slow_window": slow_window,
            "weight": FACTOR_WEIGHTS["trend"],
        }

    def _factor_breakout(self, price, quote, complete, current=None, *, style_settings: Optional[Mapping[str, Any]] = None):
        try:
            window = max(
                1,
                int(
                    style_settings.get("breakout_window_sessions")
                    if isinstance(style_settings, Mapping)
                    else 20
                ),
            )
        except (TypeError, ValueError):
            window = 20
        if len(complete) < window:
            return {"domain": "breakout", "available": False, "passed": False, "reason": "insufficient_prior_sessions"}
        prior_high = max(float(bar["high"]) for bar in complete[-window:])
        return {
            "domain": "breakout", "available": True,
            "passed": price > prior_high,
            "prior_high": prior_high,
            "breakout_window": window,
            "observed_price": price,
            "weight": FACTOR_WEIGHTS["breakout"],
        }

    def _setup_factor_domains(self, plan, price, current, complete, *, quote=None, mode: str):
        setup = self._plan_strategy_setup(plan)
        setup_type = setup.get("setup_type")
        if setup_type != "pullback":
            return []
        return [
            self._factor_support_confirmation(price, setup.get("geometry_guard")),
            self._factor_shrinking_volume(current, complete, quote=quote, mode=mode),
        ]

    def _factor_support_confirmation(self, price, geometry_guard):
        support = None
        if isinstance(geometry_guard, Mapping):
            support = self._safe_price(geometry_guard.get("support"))
        available = support is not None
        tolerance = float(price) * 0.02
        passed = bool(available and price >= float(support) and price <= float(support) + tolerance)
        return {
            "domain": "support_confirmation",
            "available": available,
            "passed": passed,
            "support": support,
            "observed_price": price,
            "tolerance_pct": 2.0,
            "weight": 0.30,
        }

    def _factor_shrinking_volume(self, current, complete, *, quote=None, mode: str):
        if mode == "intraday":
            ratio = self._safe_float(getattr(quote, "volume_ratio", None))
            if ratio is None:
                return {
                    "domain": "shrinking_volume",
                    "available": False,
                    "passed": False,
                    "reason": "missing_intraday_volume_ratio",
                    "policy": "intraday_requires_source_volume_ratio_or_same_time_baseline",
                    "weight": 0.30,
                }
            return {
                "domain": "shrinking_volume",
                "available": True,
                "passed": ratio <= 0.85,
                "volume_ratio": ratio,
                "threshold_lte": 0.85,
                "policy": "source_volume_ratio",
                "weight": 0.30,
            }
        if current is None or len(complete) < VOLUME_POLICY_SESSIONS:
            return {
                "domain": "shrinking_volume",
                "available": False,
                "passed": False,
                "reason": "insufficient_prior_sessions",
                "weight": 0.30,
            }
        avg = sum(float(bar["volume"]) for bar in complete[-VOLUME_POLICY_SESSIONS:]) / VOLUME_POLICY_SESSIONS
        current_volume = self._safe_float(current.get("volume"))
        if current_volume is None or avg <= 0:
            return {
                "domain": "shrinking_volume",
                "available": False,
                "passed": False,
                "reason": "missing_current_volume",
                "weight": 0.30,
            }
        ratio = current_volume / avg
        return {
            "domain": "shrinking_volume",
            "available": True,
            "passed": ratio <= 0.85,
            "volume_ratio_to_avg": ratio,
            "threshold_lte": 0.85,
            "weight": 0.30,
        }

    def _setup_confirmation_policy(self, plan, factors: Sequence[Mapping[str, Any]], *, mode: str) -> Dict[str, Any]:
        setup = self._plan_strategy_setup(plan)
        setup_type = setup.get("setup_type")
        setup_policy = setup.get("factor_policy") if isinstance(setup.get("factor_policy"), Mapping) else {}
        if not setup_policy:
            setup_policy = setup_factor_policy(setup_type)
        if setup_policy.get("error") or not setup_type:
            reason = str(setup_policy.get("error") or "missing_strategy_setup_type")
            return {
                "version": PLAN_VERSION,
                "mode": mode,
                "setup_type": setup_type,
                "passed": False,
                "fail_closed": True,
                "reasons": [reason],
                "score": 0.0,
                "min_score": self.intraday_min_composite_score,
                "factors_available": 0,
                "factors_passed": 0,
                "selected_factor_domains": [],
                "setup_factor_policy": dict(setup_policy),
            }

        by_domain = {
            str(item.get("domain")): dict(item)
            for item in factors
            if isinstance(item, Mapping) and item.get("domain") is not None
        }
        required = [str(domain) for domain in setup_policy.get("required_domains") or []]
        alternatives = [
            [str(domain) for domain in group]
            for group in setup_policy.get("alternative_confirmations") or []
            if isinstance(group, Sequence) and not isinstance(group, (str, bytes))
        ]
        optional = [str(domain) for domain in setup_policy.get("optional_domains") or []]
        selected_names: List[str] = []
        for domain in required:
            if domain not in selected_names:
                selected_names.append(domain)
        for group in alternatives:
            for domain in group:
                if domain not in selected_names:
                    selected_names.append(domain)
        for domain in optional:
            if domain not in selected_names:
                selected_names.append(domain)

        def domain_item(domain: str) -> Dict[str, Any]:
            return by_domain.get(domain, {"domain": domain, "available": False, "passed": False})

        def domain_available(domain: str) -> bool:
            return bool(domain_item(domain).get("available"))

        def domain_passed(domain: str) -> bool:
            item = domain_item(domain)
            return bool(item.get("available") and item.get("passed"))

        reasons: List[str] = []
        for domain in required:
            if not domain_available(domain):
                reasons.append(f"setup_required_domain_unavailable:{domain}")
            elif not domain_passed(domain):
                reasons.append(f"setup_required_domain_not_passed:{domain}")

        alternative_passed = True
        if alternatives:
            alternative_passed = any(all(domain_passed(domain) for domain in group) for group in alternatives)
            if not alternative_passed:
                reasons.append(
                    "setup_alternative_confirmation_not_passed:"
                    + "|".join("+".join(group) for group in alternatives)
                )

        selected = [domain_item(domain) for domain in selected_names]
        available = [item for item in selected if item.get("available")]
        passed = [item for item in available if item.get("passed")]
        total_weight = sum(max(0.0, float(item.get("weight") or 0.0)) for item in selected)
        if total_weight <= 0:
            total_weight = float(len(selected) or 1)
            score = sum(1.0 for item in passed) / total_weight
        else:
            score = sum(max(0.0, float(item.get("weight") or 0.0)) for item in passed) / total_weight
        if score < self.intraday_min_composite_score:
            reasons.append("setup_score_below_threshold")
        if len(available) < self.intraday_min_factor_domains:
            reasons.append("insufficient_available_factor_domains")
        if len(passed) < self.intraday_min_factor_domains:
            reasons.append("insufficient_passing_factor_domains")

        return {
            "version": PLAN_VERSION,
            "mode": mode,
            "setup_type": setup_type,
            "passed": not reasons,
            "fail_closed": any(
                reason.startswith("setup_required_domain_unavailable")
                or reason.startswith("setup_alternative_confirmation_not_passed")
                or reason in {"unsupported_strategy_setup_type", "missing_strategy_setup_type"}
                or reason == "insufficient_available_factor_domains"
                for reason in reasons
            ),
            "reasons": reasons or ["setup_policy_passed"],
            "score": round(score, 6),
            "min_score": self.intraday_min_composite_score,
            "factors_available": len(available),
            "factors_passed": len(passed),
            "selected_factor_domains": selected_names,
            "setup_factor_policy": dict(setup_policy),
            "factor_domains": selected,
            "alternative_passed": alternative_passed,
        }

    def _entry_policy(self, plan, current, prior_bars, *, mode: str) -> Dict[str, Any]:
        factors = self._intraday_factor_domains(plan, float(current["close"]), None, prior_bars)
        if mode == "eod":
            complete = [bar for bar in prior_bars if self._bar_complete(bar)]
            style_settings = self._style_settings_for_plan(plan)
            factors = [
                self._factor_conviction(plan),
                self._factor_momentum(float(current["close"]), None, complete),
                self._factor_volume(None, current, complete, mode="eod"),
                self._factor_trend(float(current["close"]), complete, style_settings=style_settings),
                self._factor_breakout(float(current["close"]), None, complete, current, style_settings=style_settings),
            ]
            factors.extend(self._setup_factor_domains(plan, float(current["close"]), current, complete, quote=None, mode="eod"))
        setup_check = self._setup_confirmation_policy(plan, factors, mode=mode)
        risk_reward = self._plan_risk_reward_ratio(plan, current["close"], current["close"])
        reasons: List[str] = []
        if not setup_check.get("passed"):
            reasons.extend(str(reason) for reason in setup_check.get("reasons") or [])
        if risk_reward is None or risk_reward < self.intraday_min_risk_reward:
            reasons.append("risk_reward_below_policy")
        return {
            "version": PLAN_VERSION,
            "mode": mode,
            "required_passed": True,
            "passed": not reasons,
            "fail_closed": bool(setup_check.get("fail_closed")) or "risk_reward_below_policy" in reasons,
            "reasons": reasons or ["policy_passed"],
            "score": setup_check.get("score", 0.0),
            "min_score": self.intraday_min_composite_score,
            "factors_available": setup_check.get("factors_available", 0),
            "factors_passed": setup_check.get("factors_passed", 0),
            "min_factor_domains": self.intraday_min_factor_domains,
            "risk_reward": risk_reward,
            "min_risk_reward": self.intraday_min_risk_reward,
            "factor_domains": factors,
            "setup_type": setup_check.get("setup_type"),
            "setup_confirmation_policy": setup_check,
            "setup_factor_policy": dict(setup_check.get("setup_factor_policy") or {}),
        }

    def _current_from_quote(self, price, quote) -> Dict[str, Any]:
        high = self._safe_price(getattr(quote, "high", None)) or price
        low = self._safe_price(getattr(quote, "low", None)) or price
        open_price = self._safe_price(getattr(quote, "open_price", None)) or price
        volume = self._safe_float(getattr(quote, "volume", None))
        return {
            "open": open_price,
            "high": max(high, price, open_price),
            "low": min(low, price, open_price),
            "close": price,
            "volume": volume or 0.0,
            "session_progress": None,
        }

    def _quote_volume_ratio(self, quote, prior_bars) -> Optional[float]:
        ratio = getattr(quote, "volume_ratio", None)
        if self._positive_finite(ratio):
            return float(ratio)
        return None

    def _fresh_quote_price(
        self,
        quote,
        *,
        observed_at: Optional[datetime] = None,
    ) -> Tuple[Optional[float], Optional[str]]:
        if quote is None:
            return None, "missing_realtime_quote"
        price = self._safe_price(getattr(quote, "price", None))
        if price is None:
            return None, "missing_realtime_price"
        reference_time = observed_at or datetime.now(timezone.utc)
        if reference_time.tzinfo is None:
            reference_time = reference_time.replace(tzinfo=timezone.utc)
        reference_time = reference_time.astimezone(timezone.utc)

        def validate_quote_time(value: Any, *, label: str) -> Tuple[Optional[datetime], Optional[str]]:
            parsed = self._parse_quote_datetime(value)
            if parsed is None:
                return None, f"invalid_quote_{label}"
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            parsed = parsed.astimezone(timezone.utc)
            age = (reference_time - parsed).total_seconds()
            if age < -QUOTE_CLOCK_SKEW_SECONDS:
                return None, f"future_quote_{label}"
            if age > self.intraday_max_quote_stale_seconds:
                return None, "stale_realtime_quote"
            return parsed, None

        if getattr(quote, "is_stale", None) is True:
            return None, "stale_realtime_quote"
        stale_seconds = getattr(quote, "stale_seconds", None)
        if stale_seconds is not None:
            try:
                if int(stale_seconds) > self.intraday_max_quote_stale_seconds:
                    return None, "stale_realtime_quote"
            except (TypeError, ValueError):
                return None, "invalid_quote_staleness"
        provider_timestamp = getattr(quote, "provider_timestamp", None)
        fetched_at = getattr(quote, "fetched_at", None)
        if provider_timestamp:
            provider_dt, provider_error = validate_quote_time(
                provider_timestamp,
                label="provider_timestamp",
            )
            if provider_error:
                return None, provider_error
            if fetched_at:
                fetched_dt = self._parse_quote_datetime(fetched_at)
                if fetched_dt is None:
                    return None, "invalid_quote_fetched_at"
                if fetched_dt.tzinfo is None:
                    fetched_dt = fetched_dt.replace(tzinfo=timezone.utc)
                fetched_dt = fetched_dt.astimezone(timezone.utc)
                provider_age = (fetched_dt - provider_dt).total_seconds()
                if provider_age < -QUOTE_CLOCK_SKEW_SECONDS:
                    return None, "future_quote_provider_timestamp"
                if provider_age > self.intraday_max_quote_stale_seconds:
                    return None, "stale_realtime_quote"
        elif stale_seconds is None:
            if not fetched_at:
                return None, "missing_quote_fetched_at"
            _fetched_dt, fetched_error = validate_quote_time(
                fetched_at,
                label="fetched_at",
            )
            if fetched_error:
                return None, fetched_error
        return price, None

    @staticmethod
    def _parse_quote_datetime(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str) and value.strip():
            try:
                return datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    @staticmethod
    def _intraday_rule_mode(rule) -> str:
        text = str(rule.get("source_text") or "")
        return "close_only" if re.search(r"收盘|收于|收在|close", text, re.IGNORECASE) else "intraday"

    @staticmethod
    def _bucket_start(value: datetime, interval_seconds: int) -> datetime:
        timestamp = int(value.timestamp())
        return datetime.fromtimestamp(timestamp - (timestamp % interval_seconds), timezone.utc)

    @staticmethod
    def _utc_naive(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    def _risk_reward_ratio(self, stop, target, low, high) -> Optional[float]:
        stop_price = self._safe_price(stop)
        target_price = self._safe_price(target)
        entry = self._entry_reference_price(low, high)
        if stop_price is None or target_price is None or entry is None:
            return None
        risk = entry - stop_price
        reward = target_price - entry
        if risk <= 0 or reward <= 0:
            return None
        return reward / risk

    def _plan_risk_reward_ratio(self, plan, low, high) -> Optional[float]:
        contract = self._plan_exit_contract(plan)
        if contract and contract.get("weighted_risk_reward") is not None:
            try:
                value = float(contract.get("weighted_risk_reward"))
            except (TypeError, ValueError):
                value = math.nan
            if math.isfinite(value) and value > 0:
                return value
        target = (
            contract.get("weighted_target_price")
            if contract and contract.get("weighted_target_price") is not None
            else plan.target_price
        )
        return self._risk_reward_ratio(plan.stop_loss, target, low, high)

    def _entry_reference_price(self, low, high) -> Optional[float]:
        low_price = self._safe_price(low)
        high_price = self._safe_price(high)
        if low_price is not None and high_price is not None:
            return (low_price + high_price) / 2.0
        return low_price if low_price is not None else high_price

    def _intraday_policy_evidence(self) -> Dict[str, Any]:
        return {
            "version": PLAN_VERSION,
            "min_factor_domains": self.intraday_min_factor_domains,
            "min_composite_score": self.intraday_min_composite_score,
            "min_risk_reward": self.intraday_min_risk_reward,
            "max_quote_stale_seconds": self.intraday_max_quote_stale_seconds,
        }

    @staticmethod
    def _phase_evidence(phase) -> Dict[str, Any]:
        return {
            "market": phase.market,
            "phase": getattr(phase.phase, "value", str(phase.phase)),
            "market_local_time": phase.market_local_time.isoformat(),
            "effective_daily_bar_date": phase.effective_daily_bar_date.isoformat(),
            "is_market_open_now": phase.is_market_open_now,
        }

    @staticmethod
    def _phase_session_date(phase) -> date:
        session_date = getattr(phase, "session_date", None)
        if isinstance(session_date, date):
            return session_date
        market_local_time = getattr(phase, "market_local_time", None)
        if isinstance(market_local_time, datetime):
            return market_local_time.date()
        return datetime.now(timezone.utc).date()

    @staticmethod
    def _event_date_from_evidence(evidence: Dict[str, Any], fallback: datetime) -> date:
        phase = evidence.get("phase") if isinstance(evidence, dict) else None
        value = phase.get("market_local_time") if isinstance(phase, dict) else None
        if value:
            try:
                return datetime.fromisoformat(str(value)).date()
            except ValueError:
                pass
        return fallback.date()

    @staticmethod
    def _phase_allows_intraday_monitor(phase) -> bool:
        phase_value = getattr(getattr(phase, "phase", None), "value", str(getattr(phase, "phase", "")))
        return phase.is_market_open_now is True and phase_value == "intraday"

    @staticmethod
    def _phase_allows_preopen_observation(phase) -> bool:
        return False

    def _quote_evidence(self, quote) -> Dict[str, Any]:
        if quote is None:
            return {"available": False}
        return {
            "available": True,
            "code": getattr(quote, "code", None),
            "market": getattr(quote, "market", None),
            "source": self._quote_source(quote),
            "price": getattr(quote, "price", None),
            "high": getattr(quote, "high", None),
            "low": getattr(quote, "low", None),
            "volume": getattr(quote, "volume", None),
            "volume_ratio": getattr(quote, "volume_ratio", None),
            "change_pct": getattr(quote, "change_pct", None),
            "provider_timestamp": getattr(quote, "provider_timestamp", None),
            "fetched_at": getattr(quote, "fetched_at", None),
            "is_stale": getattr(quote, "is_stale", None),
            "stale_seconds": getattr(quote, "stale_seconds", None),
            "fallback_from": getattr(quote, "fallback_from", None),
        }

    @staticmethod
    def _quote_source(quote) -> Optional[str]:
        if quote is None:
            return None
        source = getattr(quote, "source", None)
        return getattr(source, "value", None) or (str(source) if source is not None else None)

    def _evaluate_and_close_position(self, position, eval_date: date, bars: List[Dict[str, Any]]):
        if eval_date <= position.opened_on:
            return None
        with self._ledger_write_session() as session:
            db_position = session.get(PaperPosition, position.id)
            if db_position is None or db_position.status != "open":
                return None
            existing = session.execute(select(PaperPlanEvaluation).where(
                PaperPlanEvaluation.evaluation_key == (
                    f"plan:{db_position.plan_id}:"
                    f"leg:{getattr(db_position, 'entry_leg_id', None) or 'legacy_entry'}:"
                    f"pos:{db_position.id}:eod:{eval_date.isoformat()}"
                ),
            )).scalar_one_or_none()
            if existing is not None and existing.result != "failed_closed":
                return None
            current, prior, error = self._split_complete_bars(bars, eval_date)
            close_reason = None
            close_price = None
            chosen_exit_leg = None
            chosen_fraction = None
            chosen_quantity = None
            triggering_signal = None
            reasons: List[str] = []
            evidence: Dict[str, Any] = {}
            result = "position_held"
            if error:
                result, reasons = "failed_closed", [error]
            else:
                hit_stop = db_position.stop_loss is not None and current["low"] <= db_position.stop_loss
                model_risk_provenance = self._effective_model_risk_provenance(db_position)
                target_revision = model_risk_provenance.get("target")
                plan_row = session.get(PaperExecutionPlan, db_position.plan_id)
                staged_exit_leg = (
                    None
                    if target_revision is not None
                    else self._next_exit_leg_for_position(
                        session,
                        plan=plan_row,
                        position=db_position,
                        observed_high=float(current["high"]),
                    )
                )
                has_exit_contract = self._plan_exit_contract(plan_row) is not None if plan_row is not None else False
                hit_target = (
                    staged_exit_leg is not None
                    or (
                        db_position.target_price is not None
                        and current["high"] >= db_position.target_price
                        and (target_revision is not None or not has_exit_contract)
                    )
                )
                evidence.update({"bar_date": eval_date.isoformat(), "low": current["low"], "high": current["high"], "close": current["close"]})
                candidates: List[Dict[str, Any]] = []
                if hit_stop and hit_target:
                    result, reasons = "failed_closed", ["ambiguous_stop_and_target_same_bar"]
                if hit_stop:
                    candidates.append({"kind": "hard_stop", "action": "close", "reason": "stop_loss", "price": float(db_position.stop_loss), "requested_quantity": float(db_position.quantity or 0.0)})
                if hit_target:
                    target_reason = f"take_profit:{staged_exit_leg.get('leg_id')}" if staged_exit_leg is not None else "take_profit"
                    target_price = (
                        float(staged_exit_leg.get("target_price"))
                        if staged_exit_leg is not None
                        else float(db_position.target_price)
                    )
                    target_fraction = (
                        float(staged_exit_leg.get("allocation_fraction") or 0.0)
                        if staged_exit_leg is not None
                        else None
                    )
                    candidates.append({
                        "kind": "take_profit",
                        "action": "close",
                        "reason": target_reason,
                        "price": target_price,
                        "requested_quantity": (
                            self._exit_leg_quantity(db_position, staged_exit_leg)
                            if target_fraction is not None
                            else float(db_position.quantity or 0.0)
                        ),
                        "requested_fraction": target_fraction,
                        "exit_leg": staged_exit_leg,
                        "pending_semantics": "conditional_profit",
                    })
                triggering_signal = self.repo.latest_defensive_signal(
                    market=db_position.market,
                    stock_code=db_position.stock_code,
                    source_model=db_position.source_model,
                    after_date=db_position.opened_on,
                    through_date=eval_date,
                    after_time=getattr(db_position, "opened_at", None),
                    execution_scope=self.execution_scope,
                )
                if triggering_signal is not None:
                    report = self.repo.get_report(int(triggering_signal.source_report_id)) if getattr(triggering_signal, "source_report_id", None) else None
                    eligibility = self._defensive_signal_eligibility(
                        triggering_signal,
                        report=report,
                        evaluated_at=self._market_local_naive_to_utc_naive(
                            db_position.market,
                            eval_date,
                            MARKET_REGULAR_CLOSE.get(db_position.market, time(15, 0)),
                        ),
                    )
                    action_value = eligibility["execution_action"] if eligibility["eligible"] else None
                    evidence["defensive_signal"] = {
                        "id": triggering_signal.id,
                        "action": normalize_decision_action(getattr(triggering_signal, "action", None)),
                        "holding_action": eligibility.get("holding_action"),
                        "execution_action": action_value,
                        "eligibility": eligibility,
                        "created_at": triggering_signal.created_at.isoformat() if triggering_signal.created_at else None,
                    }
                    if action_value == "sell":
                        candidates.append({"kind": "model_sell", "action": "close", "reason": "model_sell", "price": current["close"], "signal_id": triggering_signal.id, "requested_quantity": float(db_position.quantity or 0.0)})
                    elif action_value == "reduce":
                        candidates.append({"kind": "model_reduce", "action": "close", "reason": "model_reduce", "price": current["close"], "signal_id": triggering_signal.id, "requested_quantity": self._close_quantity_for_reason(db_position, "model_reduce")})
                    elif action_value == "avoid":
                        reasons.append("avoid_signal_blocks_new_entries_only")
                sessions_held = sum(1 for bar in prior if bar["date"] > db_position.opened_on)
                evidence["completed_sessions_held_before_today"] = sessions_held
                if sessions_held + 1 >= db_position.max_holding_sessions:
                    candidates.append({"kind": "time_exit", "action": "close", "reason": "expiry", "price": current["close"], "requested_quantity": float(db_position.quantity or 0.0)})
                if result != "failed_closed":
                    candidates.append({"kind": "hold", "action": "hold", "reason": "no_exit_condition_met"})
                    resolution = self.resolve_paper_intents(
                        candidates=candidates,
                        gates={"fail_closed": False},
                        state={"entry": "eod_close", "position_id": db_position.id, "evaluation_date": eval_date.isoformat()},
                    )
                    evidence["intent_resolution"] = resolution
                    chosen = resolution.get("chosen")
                    if chosen and chosen.get("action") == "close":
                        close_reason = str(chosen.get("reason"))
                        close_price = float(chosen.get("price") or current["close"])
                        chosen_exit_leg = chosen.get("exit_leg") if isinstance(chosen.get("exit_leg"), Mapping) else None
                        chosen_fraction = self._safe_float(chosen.get("requested_fraction"))
                        chosen_quantity = self._safe_float(chosen.get("requested_quantity"))
                if close_reason:
                    result, reasons = "closed", [close_reason]
                elif not reasons:
                    reasons = ["no_exit_condition_met"]
            evaluation = existing or PaperPlanEvaluation(
                evaluation_key=(
                    f"plan:{db_position.plan_id}:"
                    f"leg:{getattr(db_position, 'entry_leg_id', None) or 'legacy_entry'}:"
                    f"pos:{db_position.id}:eod:{eval_date.isoformat()}"
                ),
                plan_id=db_position.plan_id,
                position_id=db_position.id,
                entry_leg_id=getattr(db_position, "entry_leg_id", None) or "legacy_entry",
                evaluation_date=eval_date,
                result=result,
            )
            evaluation.result = result
            evaluation.reasons_json = self._json_dumps(reasons)
            evaluation.evidence_json = self._json_dumps(evidence)
            if existing is None:
                session.add(evaluation)
            event = None
            if close_reason and close_price is not None:
                account = session.get(PaperCapitalAccount, db_position.capital_account_id) if db_position.capital_account_id else None
                resolution_bucket = self._resolution_bucket(
                    mode="eod_close",
                    source_model=db_position.source_model,
                    market=db_position.market,
                    stock_code=db_position.stock_code,
                    currency=(account.currency if account is not None else getattr(db_position, "currency", None)),
                    capital_account_id=(account.id if account is not None else getattr(db_position, "capital_account_id", None)),
                    evaluation_date=eval_date,
                )
                resolution_key = self._resolution_key(resolution_bucket)
                logical_order_key = "ord-" + hashlib.sha256(
                    f"{resolution_key}|{close_reason}".encode("utf-8")
                ).hexdigest()[:24]
                fill_result, fill_error = self._apply_position_exit(
                    session,
                    account,
                    db_position,
                    quote_price=close_price,
                    reason=close_reason,
                    event_date=eval_date,
                    requested_fraction=chosen_fraction,
                    requested_quantity=chosen_quantity,
                )
                if fill_error:
                    evaluation.result = "failed_closed"
                    evaluation.reasons_json = self._json_dumps([fill_error])
                    evaluation.evidence_json = self._json_dumps({**evidence, "fill_error": fill_error})
                    session.flush()
                    return {"evaluation": self._serialize_evaluation(evaluation), "event": None}
                assert fill_result is not None
                self._persist_resolution(
                    session,
                    resolution_key=resolution_key,
                    bucket=resolution_bucket,
                    status="closed",
                    selected_intent="close",
                    candidate_intents=[{"intent": "close", "close_reason": close_reason}],
                    selected_intent_payload={"intent": "close", "close_reason": close_reason, "logical_order_key": logical_order_key},
                    source_report_id=db_position.source_report_id,
                    decision_signal_id=db_position.decision_signal_id,
                    source_identities=[{"kind": "position", "id": db_position.id}],
                    requested_quantity=fill_result["requested_quantity"],
                    executed_quantity=fill_result["executed_quantity"],
                    unfilled_quantity=fill_result["unfilled_quantity"],
                    evidence={**evidence, "fill": fill_result},
                )
                db_position.close_signal_id = triggering_signal.id if triggering_signal is not None else None
                plan = session.get(PaperExecutionPlan, db_position.plan_id)
                if plan is not None:
                    plan.status = (
                        "closed"
                        if not self._plan_has_live_lots_or_pending_legs(
                            session,
                            plan,
                            exclude_position_id=db_position.id if db_position.status == "closed" else None,
                        )
                        else "triggered"
                    )
                    plan.updated_at = utc_naive_now()
                event_type = "close" if fill_result.get("final_close") else "reduce"
                event_row = PaperTradeEvent(
                    event_key=(
                        f"plan:{db_position.plan_id}:pos:{db_position.id}:"
                        f"leg:{getattr(db_position, 'entry_leg_id', None) or 'legacy_entry'}:"
                        f"close:{eval_date.isoformat()}"
                    ),
                    plan_id=db_position.plan_id,
                    position_id=db_position.id,
                    source_report_id=db_position.source_report_id,
                    decision_signal_id=db_position.decision_signal_id,
                    source_model=db_position.source_model,
                    market=db_position.market,
                    stock_code=db_position.stock_code,
                    stock_name=db_position.stock_name,
                    event_type=event_type,
                    event_date=eval_date,
                    price=fill_result["fill_price"],
                    quantity=fill_result["quantity"],
                    gross_amount=fill_result["gross_amount"],
                    fees=fill_result["fees"],
                    commission=fill_result.get("commission"),
                    tax=fill_result.get("tax", fill_result.get("sell_tax")),
                    slippage_amount=fill_result.get("slippage_amount"),
                    gross_realized_pnl=fill_result.get("realized_gross_pnl"),
                    net_realized_pnl=fill_result.get("realized_pnl"),
                    net_amount=fill_result["net_amount"],
                    fill_price=fill_result["fill_price"],
                    fill_evidence_grade=fill_result["evidence_grade"],
                    reason=close_reason,
                    **self._resolution_fields_for_event(
                        resolution_key=resolution_key,
                        logical_order_key=logical_order_key,
                        fill=fill_result,
                    ),
                    triggering_signal_id=triggering_signal.id if triggering_signal is not None else None,
                    capital_account_id=account.id if account is not None else None,
                    currency=account.currency if account is not None else getattr(db_position, "currency", None),
                    allocated_capital=fill_result["cost_reduced"],
                    cash_after=account.cash if account is not None else None,
                    exposure_after=account.exposure if account is not None else None,
                    realized_pnl_after=account.realized_pnl if account is not None else None,
                    details_json=self._json_dumps({
                        "simulated": True,
                        "entry_leg_id": getattr(db_position, "entry_leg_id", None) or "legacy_entry",
                        "quantity": fill_result["quantity"],
                        "remaining_quantity": fill_result["remaining_quantity"],
                        "remaining_cost": fill_result["remaining_cost"],
                        "exit_leg": dict(chosen_exit_leg or {}),
                        "exit_leg_id": (chosen_exit_leg or {}).get("leg_id"),
                        "opened_price": db_position.opened_price,
                        "closed_price": fill_result["fill_price"],
                        "fill": fill_result,
                        "stop_loss": db_position.stop_loss,
                        "target_price": db_position.target_price,
                        "realized_pnl": fill_result["realized_pnl"],
                        "realized_gross_pnl": fill_result["realized_gross_pnl"],
                        "return_pct": db_position.simulated_return_pct,
                    }),
                )
                session.add(event_row)
                session.flush()
                self._ensure_lifecycle_dispatch(session, event_row, account=account, lifecycle="close")
                event = self._serialize_event(event_row)
            session.flush()
            return {"evaluation": self._serialize_evaluation(evaluation), "event": event}

    def _eod_validity_outcome(
        self,
        plan: PaperExecutionPlan,
        eval_date: date,
    ) -> Optional[Tuple[str, List[str], Dict[str, Any]]]:
        if getattr(plan, "report_date", None) is not None and eval_date <= plan.report_date:
            return None
        valid_for = getattr(plan, "valid_for_date", None)
        if valid_for is None:
            return None
        if eval_date == valid_for:
            return None
        reason = (
            "plan_not_yet_valid_for_session"
            if eval_date < valid_for
            else "plan_expired_for_session"
        )
        result = "failed_closed" if eval_date < valid_for else "expired"
        return (
            result,
            [reason],
            {
                "mode": "eod",
                "policy": "valid_for_date_required",
                "evaluation_date": eval_date.isoformat(),
                "valid_for_date": valid_for.isoformat(),
            },
        )

    def _split_complete_bars(self, bars, eval_date):
        exact = [bar for bar in bars if bar.get("date") == eval_date]
        if len(exact) != 1:
            return None, [], "missing_or_ambiguous_evaluation_bar"
        current = exact[0]
        if not self._bar_complete(current):
            return None, [], "incomplete_or_non_finite_evaluation_bar"
        prior = [bar for bar in bars if bar.get("date") < eval_date]
        if any(not self._bar_complete(bar) for bar in prior):
            return None, [], "incomplete_or_non_finite_prior_bar"
        return current, prior, None

    @classmethod
    def _normalize_bar_inputs(cls, inputs):
        normalized: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for raw_key, raw_bars in inputs.items():
            if isinstance(raw_key, tuple) and len(raw_key) == 2:
                market, raw_code = str(raw_key[0]).lower(), str(raw_key[1])
            else:
                raw_code = str(raw_key)
                market = get_market_for_stock(normalize_stock_code(raw_code)) or "unknown"
            identity = canonical_paper_identity(market, raw_code)
            if identity is None:
                continue
            market, code = identity
            bars = []
            for raw in raw_bars or []:
                if not isinstance(raw, Mapping):
                    bars.append({"date": None, "is_complete": False})
                    continue
                bar = dict(raw)
                try:
                    bar["date"] = cls._as_date(bar.get("date"))
                except (TypeError, ValueError):
                    bar["date"] = None
                bars.append(bar)
            bars.sort(key=lambda bar: bar.get("date") or date.min)
            normalized.setdefault((market, code), []).extend(bars)
            normalized[(market, code)].sort(key=lambda bar: bar.get("date") or date.min)
        return normalized

    @staticmethod
    def _latest_proven_complete_date(market: str, current_time: datetime) -> date:
        """Return a conservative completed-session ceiling for one market."""

        market_now = get_market_now(market, current_time=current_time)
        close_time = MARKET_REGULAR_CLOSE.get(market)
        conservative = market_now.date()
        if close_time is None or market_now.time().replace(tzinfo=None) < close_time:
            conservative -= timedelta(days=1)
        calendar_effective = get_effective_trading_date(market, current_time=current_time)
        return min(conservative, calendar_effective)

    @classmethod
    def _bar_complete(cls, bar):
        if bar.get("is_complete") is not True:
            return False
        values = [bar.get(key) for key in ("open", "high", "low", "close", "volume")]
        if not all(cls._positive_finite(value) for value in values):
            return False
        low, high = float(bar["low"]), float(bar["high"])
        open_price, close = float(bar["open"]), float(bar["close"])
        return low <= min(open_price, close) <= max(open_price, close) <= high

    @staticmethod
    def _positive_finite(value):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return False
        return math.isfinite(number) and number > 0

    @classmethod
    def _safe_price(cls, value):
        return float(value) if cls._positive_finite(value) else None

    @staticmethod
    def _safe_float(value):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    @staticmethod
    def _currency_for_market(market: str) -> str:
        return {
            "cn": "CNY",
            "hk": "HKD",
            "us": "USD",
            "jp": "JPY",
            "kr": "KRW",
            "tw": "TWD",
        }.get((market or "").lower(), "CNY")

    @staticmethod
    def _lot_size_for_market(market: str) -> int:
        return 100 if (market or "").lower() == "cn" else 1

    def _account_key(self, source_model: str, market: str, currency: str) -> str:
        return f"{source_model}:{market}:{currency}"

    def _validate_configured_model(self, source_model: str) -> str:
        model = str(source_model or "").strip()
        if not model:
            raise ValueError("source_model is required")
        from src.config import get_config

        return validate_analysis_models(get_config(), [model])[0]

    @staticmethod
    def _normalize_supported_market(market: str) -> str:
        normalized = str(market or "").strip().lower()
        if normalized not in VALID_MARKETS:
            raise ValueError(f"unsupported market: {market}")
        return normalized

    @staticmethod
    def _book_equity(account: PaperCapitalAccount) -> float:
        return float(account.cash or 0.0) + float(account.invested_capital or 0.0)

    def _valuation_mark_for_identity(
        self,
        session,
        *,
        source_model: str,
        market: str,
        stock_code: str,
        current_time: datetime,
    ) -> Optional[Dict[str, Any]]:
        """Return the latest display valuation mark without changing execution freshness rules."""

        now = self._utc_naive(current_time)
        candidates: List[Dict[str, Any]] = []
        observation = session.execute(
            select(PaperPlanObservation)
            .where(
                PaperPlanObservation.source_model == source_model,
                PaperPlanObservation.market == market,
                PaperPlanObservation.stock_code == stock_code,
                PaperPlanObservation.quote_price.is_not(None),
            )
            .order_by(desc(PaperPlanObservation.observed_at), desc(PaperPlanObservation.id))
            .limit(1)
        ).scalar_one_or_none()
        if observation is not None and self._safe_float(observation.quote_price) is not None:
            observed_at = observation.observed_at
            age = (now - observed_at).total_seconds() if observed_at is not None else None
            candidates.append({
                "price": float(observation.quote_price),
                "observed_at": observed_at,
                "source": observation.quote_source or "paper_plan_observation",
                "stale": age is None or age > self.intraday_max_quote_stale_seconds,
            })

        daily = self._latest_complete_daily_mark(session, market=market, stock_code=stock_code, through=now.date())
        if daily is not None:
            price = self._safe_float(daily.close)
            if price is not None:
                candidates.append({
                    "price": price,
                    "observed_at": self._daily_close_observed_at(market, daily.date),
                    "source": "stock_daily_close",
                    "stale": True,
                })
        if not candidates:
            return None
        candidates.sort(key=lambda item: item.get("observed_at") or datetime.min, reverse=True)
        return candidates[0]

    @staticmethod
    def _daily_close_observed_at(market: str, trading_date: date) -> datetime:
        normalized = str(market or "").lower()
        if normalized in {"cn", "hk"}:
            close_hour = 15 if normalized == "cn" else 16
            local_zone = ZoneInfo("Asia/Shanghai")
        elif normalized == "us":
            close_hour = 16
            local_zone = ZoneInfo("America/New_York")
        else:
            close_hour = 15
            local_zone = timezone.utc
        local_dt = datetime.combine(trading_date, time(hour=close_hour, minute=0), tzinfo=local_zone)
        return local_dt.astimezone(timezone.utc).replace(tzinfo=None)

    def _latest_complete_daily_mark(self, session, *, market: str, stock_code: str, through: date) -> Optional[StockDaily]:
        variants = stock_daily_variants(market, stock_code)
        if not variants:
            return None
        rows = session.execute(
            select(StockDaily)
            .where(StockDaily.code.in_(variants), StockDaily.date <= through)
            .order_by(desc(StockDaily.date), desc(StockDaily.id))
            .limit(max(1, len(variants)) * 8)
        ).scalars().all()
        by_date: Dict[date, List[StockDaily]] = {}
        for row in rows:
            by_date.setdefault(row.date, []).append(row)
        for bar_date in sorted(by_date, reverse=True):
            candidates = by_date[bar_date]
            candidates.sort(key=lambda row: variants.index(row.code))
            snapshots = {
                tuple(getattr(row, field) for field in ("open", "high", "low", "close", "volume"))
                for row in candidates
            }
            if len(snapshots) > 1:
                continue
            chosen = candidates[0]
            if self._safe_float(chosen.close) is not None:
                return chosen
        return None

    def _account_valuation_snapshot(
        self,
        session,
        account: PaperCapitalAccount,
        *,
        current_time: datetime,
    ) -> Dict[str, Any]:
        positions = list(session.execute(
            select(PaperPosition)
            .where(
                PaperPosition.capital_account_id == account.id,
                PaperPosition.status == "open",
            )
            .order_by(PaperPosition.market, PaperPosition.stock_code, PaperPosition.id)
        ).scalars().all())
        priced_market_value = 0.0
        unrealized = 0.0
        unpriced = 0
        unpriced_identities = set()
        observed: List[datetime] = []
        stale = False
        for position in positions:
            mark = self._valuation_mark_for_identity(
                session,
                source_model=position.source_model,
                market=position.market,
                stock_code=position.stock_code,
                current_time=current_time,
            )
            if mark is None:
                unpriced += 1
                unpriced_identities.add((position.source_model, position.market, position.stock_code, position.currency))
                continue
            market_value = float(position.quantity or 0.0) * float(mark["price"])
            priced_market_value += market_value
            unrealized += market_value - self._position_allocated_capital(position)
            if mark.get("observed_at") is not None:
                observed.append(mark["observed_at"])
            stale = stale or bool(mark.get("stale"))
        valuation_complete = unpriced == 0
        observed_at = max(observed).isoformat() if observed else None
        if not valuation_complete:
            return {
                "market_value": None,
                "priced_market_value": priced_market_value if priced_market_value > 0 else None,
                "unrealized_pnl": None,
                "total_pnl": None,
                "market_equity": None,
                "valuation_complete": False,
                "unpriced_position_count": unpriced,
                "unpriced_holding_count": len(unpriced_identities),
                "valuation_observed_at": observed_at,
                "valuation_stale": stale if observed else None,
            }
        return {
            "market_value": priced_market_value,
            "priced_market_value": priced_market_value,
            "unrealized_pnl": unrealized,
            "total_pnl": float(account.realized_pnl or 0.0) + unrealized,
            "market_equity": float(account.cash or 0.0) + priced_market_value,
            "valuation_complete": True,
            "unpriced_position_count": 0,
            "unpriced_holding_count": 0,
            "valuation_observed_at": observed_at,
            "valuation_stale": stale,
        }

    def _aggregate_open_holding_context(
        self,
        session,
        positions: Sequence[PaperPosition],
        *,
        account: Optional[PaperCapitalAccount],
        current_time: datetime,
    ) -> Dict[str, Any]:
        availability_dates: Dict[str, date] = {}
        quantity = 0.0
        available_quantity = 0.0
        frozen_quantity = 0.0
        total_cost = 0.0
        stop_lots: List[Dict[str, Any]] = []
        target_lots: List[Dict[str, Any]] = []
        origins = set()
        statuses = set()
        latest_position = positions[0]
        earliest_opened = None
        latest_opened = None
        for position in positions:
            qty = float(position.quantity or 0.0)
            cost = self._position_allocated_capital(position)
            quantity += qty
            total_cost += cost
            market_key = str(position.market or "").lower()
            availability_date = availability_dates.get(market_key)
            if availability_date is None:
                availability_date = self._availability_date_for_market(position.market, current_time)
                availability_dates[market_key] = availability_date
            if getattr(position, "pending_exit_reason", None):
                available = float(getattr(position, "available_quantity", 0.0) or 0.0)
                frozen = float(getattr(position, "frozen_quantity", qty) or 0.0)
            elif self._can_close_position_on_date(position, availability_date):
                available = qty
                frozen = 0.0
            else:
                if getattr(position, "opened_on", None) == self._market_local_date_for_time(position.market, current_time):
                    available = 0.0
                    frozen = qty
                else:
                    persisted_available = float(getattr(position, "available_quantity", 0.0) or 0.0)
                    available = min(qty, max(0.0, persisted_available))
                    frozen = max(0.0, qty - available)
            available_quantity += available
            frozen_quantity += frozen
            if position.stop_loss is not None:
                stop_lots.append({"position_id": position.id, "quantity": qty, "stop_loss": position.stop_loss})
            if position.target_price is not None:
                target_lots.append({"position_id": position.id, "quantity": qty, "target_price": position.target_price})
            origins.add(getattr(position, "sync_origin", None) or "paper_generated")
            statuses.add(getattr(position, "sync_status", None) or "paper_only")
            if position.opened_on is not None:
                earliest_opened = position.opened_on if earliest_opened is None else min(earliest_opened, position.opened_on)
                latest_opened = position.opened_on if latest_opened is None else max(latest_opened, position.opened_on)
        avg_cost = total_cost / quantity if quantity > 0 else None
        mark = self._valuation_mark_for_identity(
            session,
            source_model=latest_position.source_model,
            market=latest_position.market,
            stock_code=latest_position.stock_code,
            current_time=current_time,
        )
        market_value = None
        unrealized_pnl = None
        unrealized_return_pct = None
        mark_source = None
        mark_stale = None
        valuation_complete = False
        latest_price = None
        observed_at = None
        if mark is not None:
            latest_price = float(mark["price"])
            market_value = quantity * latest_price
            unrealized_pnl = market_value - total_cost
            unrealized_return_pct = (market_value / total_cost - 1.0) * 100.0 if total_cost > 0 else None
            mark_source = mark.get("source")
            mark_stale = bool(mark.get("stale"))
            valuation_complete = True
            observed_at = mark["observed_at"].isoformat() if mark.get("observed_at") else None
        book_equity = self._book_equity(account) if account is not None else None
        return {
            "stock_name": latest_position.stock_name,
            "quantity": quantity,
            "total_quantity": quantity,
            "available_quantity": available_quantity,
            "frozen_quantity": frozen_quantity,
            "avg_cost": avg_cost,
            "weighted_average_price": avg_cost,
            "total_cost": total_cost,
            "lot_count": len(positions),
            "lot_ids": [position.id for position in positions],
            "opened_on": latest_opened.isoformat() if latest_opened else None,
            "earliest_opened_on": earliest_opened.isoformat() if earliest_opened else None,
            "latest_opened_on": latest_opened.isoformat() if latest_opened else None,
            "stop_loss": latest_position.stop_loss,
            "target_price": latest_position.target_price,
            "stop_loss_lots": stop_lots,
            "target_price_lots": target_lots,
            "sync_origin": "mixed" if len(origins) > 1 else next(iter(origins)),
            "sync_status": "mixed" if len(statuses) > 1 else next(iter(statuses)),
            "currency": latest_position.currency or (account.currency if account is not None else None),
            "latest_price": latest_price,
            "observed_at": observed_at,
            "market_value": market_value,
            "mark_source": mark_source,
            "mark_stale": mark_stale,
            "valuation_complete": valuation_complete,
            "unrealized_pnl": unrealized_pnl,
            "unrealized_return_pct": unrealized_return_pct,
            "symbol_concentration_pct": (total_cost / book_equity * 100.0) if book_equity and book_equity > 0 else None,
        }

    @staticmethod
    def _adjustment_payload_hash(
        *,
        account_id: int,
        adjustment_type: str,
        amount: float,
        reason: Optional[str],
    ) -> str:
        payload = {
            "account_id": int(account_id),
            "adjustment_type": adjustment_type,
            "amount": round(float(amount), 8),
            "reason": reason or "",
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _validate_sync_models(self, requested: Sequence[str]) -> List[str]:
        from src.config import get_config

        models = validate_analysis_models(get_config(), requested)
        if not models:
            raise ValueError("at least one configured source_model is required")
        return models

    @staticmethod
    def _sync_payload_hash(payload: Mapping[str, Any]) -> str:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _positive_amount(value: Any, label: str) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{label} must be finite and positive") from exc
        if not math.isfinite(number) or number <= 0:
            raise ValueError(f"{label} must be finite and positive")
        return number

    @staticmethod
    def _non_negative_amount(value: Any, label: str) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{label} must be finite and non-negative") from exc
        if not math.isfinite(number) or number < 0:
            raise ValueError(f"{label} must be finite and non-negative")
        return number

    @staticmethod
    def _portfolio_symbol_for_identity(market: str, stock_code: str) -> str:
        market_norm = str(market or "").lower()
        code = str(stock_code or "").upper()
        if market_norm == "cn":
            if code.startswith(("4", "8", "92")):
                return f"BJ{code}"
            if code.startswith(("5", "6", "9")):
                return f"SH{code}"
            return f"SZ{code}"
        return code

    def _latest_report_for_identity_in_session(
        self,
        session,
        *,
        source_model: str,
        market: str,
        stock_code: str,
    ):
        variants = stock_daily_variants(market, stock_code)
        candidates = list(dict.fromkeys([stock_code, *variants]))
        return session.execute(
            select(AnalysisHistory)
            .where(
                AnalysisHistory.model_used == source_model,
                AnalysisHistory.code.in_(candidates),
                or_(
                    AnalysisHistory.report_type.is_(None),
                    AnalysisHistory.report_type != "market_review",
                ),
                authorized_report_execution_clause(
                    AnalysisHistory.id,
                    execution_scope=self.execution_scope,
                ),
            )
            .order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id))
            .limit(1)
        ).scalar_one_or_none()

    def _open_position_for_identity_in_session(
        self,
        session,
        *,
        source_model: str,
        market: str,
        stock_code: str,
    ) -> Optional[PaperPosition]:
        return session.execute(
            select(PaperPosition)
            .where(
                PaperPosition.source_model == source_model,
                PaperPosition.market == market,
                PaperPosition.stock_code == stock_code,
                PaperPosition.status == "open",
            )
            .order_by(desc(PaperPosition.created_at), desc(PaperPosition.id))
            .limit(1)
        ).scalar_one_or_none()

    def _sync_link_for_update(self, session, idempotency_key: str) -> Optional[PaperSyncLink]:
        query = select(PaperSyncLink).where(PaperSyncLink.idempotency_key == idempotency_key)
        if not getattr(self.db, "_is_sqlite_engine", False):
            query = query.with_for_update()
        return session.execute(query.limit(1)).scalar_one_or_none()

    def _find_account_for_update(self, session, *, source_model: str, market: str) -> Optional[PaperCapitalAccount]:
        currency = self._currency_for_market(market)
        account_key = self._account_key(source_model, market, currency)
        query = select(PaperCapitalAccount).where(PaperCapitalAccount.account_key == account_key)
        if not getattr(self.db, "_is_sqlite_engine", False):
            query = query.with_for_update()
        return session.execute(query).scalar_one_or_none()

    def _get_account_for_update_by_id(self, session, account_id: int) -> PaperCapitalAccount:
        query = select(PaperCapitalAccount).where(PaperCapitalAccount.id == int(account_id))
        if not getattr(self.db, "_is_sqlite_engine", False):
            query = query.with_for_update()
        account = session.execute(query).scalar_one_or_none()
        if account is None:
            raise PaperTradingNotFoundError(f"paper capital account not found: {account_id}")
        return account

    def _get_adjustment_for_update(self, session, idempotency_key: str) -> Optional[PaperCapitalAdjustment]:
        query = select(PaperCapitalAdjustment).where(PaperCapitalAdjustment.idempotency_key == idempotency_key)
        if not getattr(self.db, "_is_sqlite_engine", False):
            query = query.with_for_update()
        return session.execute(query).scalar_one_or_none()

    def _create_account(
        self,
        session,
        *,
        source_model: str,
        market: str,
        initial_capital: Optional[float] = None,
    ) -> PaperCapitalAccount:
        currency = self._currency_for_market(market)
        account_key = self._account_key(source_model, market, currency)
        capital = self.initial_capital if initial_capital is None else float(initial_capital)
        account = PaperCapitalAccount(
            account_key=account_key,
            source_model=source_model,
            market=market,
            currency=currency,
            initial_capital=capital,
            cash=capital,
            external_cash_flow=0.0,
            invested_capital=0.0,
            realized_pnl=0.0,
            exposure=0.0,
            open_position_count=0,
            risk_per_trade_pct=self.risk_per_trade_pct,
            max_position_pct=self.max_position_pct,
            max_total_exposure_pct=self.max_total_exposure_pct,
            max_open_positions=self.max_open_positions,
        )
        session.add(account)
        session.flush()
        return account

    def _get_or_create_account(self, session, *, source_model: str, market: str) -> PaperCapitalAccount:
        normalized_market = self._normalize_supported_market(market)
        account = self._find_account_for_update(
            session,
            source_model=source_model,
            market=normalized_market,
        )
        if account is not None:
            return account
        return self._create_account(
            session,
            source_model=source_model,
            market=normalized_market,
            initial_capital=self.initial_capital,
        )

    def _ensure_plan_budget_snapshot(
        self,
        plan: PaperExecutionPlan,
        account: PaperCapitalAccount,
    ) -> Dict[str, Any]:
        """Freeze one plan-level sizing budget at first fill."""

        if (
            not self._positive_finite(getattr(plan, "plan_allocation_budget", None))
            or not self._positive_finite(getattr(plan, "plan_risk_budget", None))
        ):
            snapshot_equity = self._book_equity(account)
            plan.budget_snapshot_equity = snapshot_equity
            plan.plan_allocation_budget = snapshot_equity * (float(account.max_position_pct or 0.0) / 100.0)
            plan.plan_risk_budget = snapshot_equity * (float(account.risk_per_trade_pct or 0.0) / 100.0)
            plan.budget_snapshot_at = utc_naive_now()
        return {
            "plan_allocation_budget": float(plan.plan_allocation_budget or 0.0),
            "plan_risk_budget": float(plan.plan_risk_budget or 0.0),
            "budget_snapshot_equity": float(plan.budget_snapshot_equity or 0.0),
            "budget_snapshot_at": plan.budget_snapshot_at.isoformat() if getattr(plan, "budget_snapshot_at", None) else None,
        }

    def _risk_snapshot_key(self, *, account_id: int, session_date: date, bucket_started_at: datetime) -> str:
        raw = f"{PLAN_VERSION}|account:{int(account_id)}|{session_date.isoformat()}|{self._utc_naive(bucket_started_at).isoformat()}"
        return "acct-risk-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    def _compute_account_risk_snapshot_in_session(
        self,
        session,
        *,
        account: PaperCapitalAccount,
        current_time: datetime,
        bucket_started_at: Optional[datetime] = None,
        persist: bool = True,
    ) -> PaperAccountRiskSnapshot:
        now = current_time if current_time.tzinfo else current_time.replace(tzinfo=timezone.utc)
        bucket = self._utc_naive(bucket_started_at or now)
        session_date = self._market_local_date_for_time(account.market, now)
        positions = list(session.execute(
            select(PaperPosition)
            .where(
                PaperPosition.capital_account_id == account.id,
                PaperPosition.status == "open",
            )
            .order_by(PaperPosition.market, PaperPosition.stock_code, PaperPosition.id)
        ).scalars().all())
        market_value = 0.0
        unrealized = 0.0
        valuation_complete = True
        valuation_stale = False
        marks: List[Dict[str, Any]] = []
        by_symbol_cost: Dict[str, float] = {}
        for position in positions:
            cost = self._position_allocated_capital(position)
            by_symbol_cost[position.stock_code] = by_symbol_cost.get(position.stock_code, 0.0) + cost
            mark = self._valuation_mark_for_identity(
                session,
                source_model=position.source_model,
                market=position.market,
                stock_code=position.stock_code,
                current_time=now,
            )
            if mark is None:
                valuation_complete = False
                marks.append({"position_id": position.id, "stock_code": position.stock_code, "available": False})
                continue
            stale = bool(mark.get("stale"))
            observed_at = mark.get("observed_at")
            if mark.get("source") == "stock_daily_close" and isinstance(observed_at, datetime):
                observed_session = self._market_local_date_for_time(
                    position.market,
                    observed_at if observed_at.tzinfo else observed_at.replace(tzinfo=timezone.utc),
                )
                if observed_session == session_date:
                    stale = False
            valuation_stale = valuation_stale or stale
            price = float(mark["price"])
            value = float(position.quantity or 0.0) * price
            market_value += value
            unrealized += value - cost
            marks.append({
                "position_id": position.id,
                "stock_code": position.stock_code,
                "available": True,
                "price": price,
                "source": mark.get("source"),
                "stale": stale,
                "observed_at": mark.get("observed_at").isoformat() if mark.get("observed_at") else None,
            })
        realized = float(account.realized_pnl or 0.0)
        current_equity = float(account.cash or 0.0) + market_value
        if not positions:
            current_equity = self._book_equity(account)
        session_start_equity = self._session_start_equity(
            session,
            account=account,
            session_date=session_date,
            current_time=now,
            current_equity=current_equity,
            valuation_complete=valuation_complete,
            valuation_stale=valuation_stale,
        )
        previous_peak = self._cashflow_neutral_previous_peak(
            session,
            account=account,
            current_time=now,
            fallback=session_start_equity,
        )
        peak_equity = max(float(previous_peak or session_start_equity or current_equity), current_equity)
        daily_pnl = current_equity - session_start_equity
        daily_loss_pct = max(0.0, (-daily_pnl / session_start_equity * 100.0)) if session_start_equity > 0 else None
        drawdown_pct = max(0.0, ((peak_equity - current_equity) / peak_equity * 100.0)) if peak_equity > 0 else None
        start_dt = datetime.combine(session_date, time.min)
        end_dt = datetime.combine(session_date, time.max)
        events = list(session.execute(
            select(PaperTradeEvent)
            .where(
                PaperTradeEvent.capital_account_id == account.id,
                PaperTradeEvent.event_date == session_date,
            )
        ).scalars().all())
        logical_trade_keys = {
            str(event.logical_order_key or f"event:{event.id}") for event in events
        }
        open_add_keys = {
            str(event.logical_order_key or f"event:{event.id}")
            for event in events
            if event.event_type == "open"
        }
        reduce_close_keys = {
            str(event.logical_order_key or f"event:{event.id}")
            for event in events
            if event.event_type in {"reduce", "close", "pending_exit"}
        }
        losing_streak = self._losing_logical_exit_streak(session, account)
        cooldown_status = self._cooldown_status_for_session(
            session,
            account=account,
            session_date=session_date,
            losing_streak=losing_streak,
        )
        concentration = self._account_concentration_evidence(
            session,
            account=account,
            positions=positions,
            marked_equity=current_equity if valuation_complete else None,
            candidate=None,
        )
        active_reasons = self._risk_reasons_from_metrics(
            valuation_complete=valuation_complete,
            valuation_stale=valuation_stale,
            current_equity=current_equity,
            daily_loss_pct=daily_loss_pct if valuation_complete and not valuation_stale else None,
            drawdown_pct=drawdown_pct if valuation_complete and not valuation_stale else None,
            losing_streak=losing_streak if cooldown_status["active"] else 0,
            daily_trade_count=len(logical_trade_keys),
            daily_open_add_count=len(open_add_keys),
            concentration=concentration,
        )
        active_state = self._risk_state_from_reasons(active_reasons)
        manual_hold = self._manual_hold_active_in_session(session, account.id)
        if manual_hold:
            active_state = "manual_hold"
            if "manual_hold" not in active_reasons:
                active_reasons.append("manual_hold")
        permissions = self._risk_permissions(active_state, active_reasons)
        key = self._risk_snapshot_key(
            account_id=account.id,
            session_date=session_date,
            bucket_started_at=bucket,
        )
        previous_snapshot = session.execute(
            select(PaperAccountRiskSnapshot)
            .where(
                PaperAccountRiskSnapshot.capital_account_id == account.id,
                PaperAccountRiskSnapshot.snapshot_key != key,
            )
            .order_by(desc(PaperAccountRiskSnapshot.observed_at), desc(PaperAccountRiskSnapshot.id))
            .limit(1)
        ).scalar_one_or_none()
        row = session.execute(
            select(PaperAccountRiskSnapshot).where(PaperAccountRiskSnapshot.snapshot_key == key).limit(1)
        ).scalar_one_or_none()
        if row is None:
            row = PaperAccountRiskSnapshot(
                snapshot_key=key,
                policy_version=PLAN_VERSION,
                capital_account_id=account.id,
                source_model=account.source_model,
                market=account.market,
                currency=account.currency,
                session_date=session_date,
                bucket_started_at=bucket,
                created_at=utc_naive_now(),
            )
            if persist:
                session.add(row)
        row.observed_at = self._utc_naive(now)
        row.session_start_equity = session_start_equity
        row.current_equity = current_equity
        row.peak_equity = peak_equity
        row.cash = float(account.cash or 0.0)
        row.exposure = float(account.exposure or 0.0)
        row.realized_pnl = realized
        row.unrealized_pnl = unrealized if valuation_complete else None
        row.total_pnl = realized + unrealized if valuation_complete else None
        row.daily_pnl = daily_pnl if valuation_complete else None
        row.daily_loss_pct = daily_loss_pct if valuation_complete else None
        row.drawdown_pct = drawdown_pct if valuation_complete else None
        row.losing_logical_exit_streak = losing_streak
        row.daily_trade_count = len(logical_trade_keys)
        row.daily_open_add_count = len(open_add_keys)
        row.daily_reduce_close_count = len(reduce_close_keys)
        row.valuation_complete = bool(valuation_complete)
        row.valuation_stale = bool(valuation_stale)
        row.total_utilization_pct = (float(account.exposure or 0.0) / current_equity * 100.0) if current_equity > 0 else None
        row.symbol_utilization_pct = max(
            [(cost / current_equity * 100.0) for cost in by_symbol_cost.values()],
            default=0.0,
        ) if current_equity > 0 else None
        row.sector_utilization_pct = concentration.get("sector", {}).get("utilization_pct")
        row.correlation_utilization_pct = concentration.get("correlation", {}).get("utilization_pct")
        row.unknown_utilization_pct = concentration.get("unknown", {}).get("utilization_pct")
        row.active_state = active_state
        row.active_reasons_json = self._json_dumps(active_reasons)
        row.permissions_json = self._json_dumps(permissions)
        row.concentration_json = self._json_dumps(concentration)
        row.evidence_json = self._json_dumps({
            "marks": marks,
            "position_count": len(positions),
            "session_trade_keys": sorted(logical_trade_keys),
            "session_window": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
            "session_start_equity_policy": "latest_reliable_snapshot_plus_external_cash_flow_since_snapshot_or_first_valid_snapshot_anchor",
            "peak_equity_policy": "previous_peak_adjusted_by_external_cash_flow_since_peak_snapshot",
            "cooldown": cooldown_status,
            "policy": self._account_risk_policy_evidence(),
        })
        row.updated_at = utc_naive_now()
        if persist:
            session.flush()
            self._record_risk_state_transition_if_changed(
                session,
                account=account,
                snapshot=row,
                previous_snapshot=previous_snapshot,
                cooldown_status=cooldown_status,
            )
        return row

    def _session_start_equity(
        self,
        session,
        *,
        account: PaperCapitalAccount,
        session_date: date,
        current_time: datetime,
        current_equity: float,
        valuation_complete: bool,
        valuation_stale: bool,
    ) -> float:
        current_naive = self._utc_naive(current_time)
        same_session = session.execute(
            select(PaperAccountRiskSnapshot)
            .where(
                PaperAccountRiskSnapshot.capital_account_id == account.id,
                PaperAccountRiskSnapshot.session_date == session_date,
                PaperAccountRiskSnapshot.session_start_equity.is_not(None),
                PaperAccountRiskSnapshot.observed_at <= current_naive,
            )
            .order_by(PaperAccountRiskSnapshot.observed_at, PaperAccountRiskSnapshot.id)
            .limit(1)
        ).scalar_one_or_none()
        if same_session is not None and same_session.session_start_equity is not None:
            cash_delta = self._external_cash_flow_delta(
                session,
                account_id=account.id,
                start_exclusive=same_session.observed_at,
                end_inclusive=current_naive,
            )
            return float(same_session.session_start_equity) + cash_delta
        previous = session.execute(
            select(PaperAccountRiskSnapshot)
            .where(
                PaperAccountRiskSnapshot.capital_account_id == account.id,
                PaperAccountRiskSnapshot.current_equity.is_not(None),
                PaperAccountRiskSnapshot.valuation_complete.is_(True),
                PaperAccountRiskSnapshot.valuation_stale.is_(False),
                PaperAccountRiskSnapshot.observed_at <= current_naive,
            )
            .order_by(desc(PaperAccountRiskSnapshot.observed_at), desc(PaperAccountRiskSnapshot.id))
            .limit(1)
        ).scalar_one_or_none()
        if previous is None:
            return float(current_equity if valuation_complete and not valuation_stale else self._book_equity(account))
        cash_delta = self._external_cash_flow_delta(
            session,
            account_id=account.id,
            start_exclusive=previous.observed_at,
            end_inclusive=current_naive,
        )
        return float(previous.current_equity or self._book_equity(account)) + cash_delta

    def _external_cash_flow_delta(
        self,
        session,
        *,
        account_id: int,
        start_exclusive: Optional[datetime],
        end_inclusive: datetime,
    ) -> float:
        query = select(PaperCapitalAdjustment).where(
            PaperCapitalAdjustment.account_id == account_id,
            PaperCapitalAdjustment.created_at <= end_inclusive,
        )
        if start_exclusive is not None:
            query = query.where(PaperCapitalAdjustment.created_at > self._utc_naive(start_exclusive))
        adjustments = list(session.execute(query).scalars().all())
        cash_delta = 0.0
        for item in adjustments:
            amount = float(item.amount or 0.0)
            cash_delta += amount if item.adjustment_type == "deposit" else -amount
        return cash_delta

    def _cashflow_neutral_previous_peak(
        self,
        session,
        *,
        account: PaperCapitalAccount,
        current_time: datetime,
        fallback: float,
    ) -> float:
        current_naive = self._utc_naive(current_time)
        latest_peak_snapshot = session.execute(
            select(PaperAccountRiskSnapshot)
            .where(
                PaperAccountRiskSnapshot.capital_account_id == account.id,
                PaperAccountRiskSnapshot.peak_equity.is_not(None),
                PaperAccountRiskSnapshot.observed_at <= current_naive,
            )
            .order_by(desc(PaperAccountRiskSnapshot.peak_equity), desc(PaperAccountRiskSnapshot.observed_at), desc(PaperAccountRiskSnapshot.id))
            .limit(1)
        ).scalar_one_or_none()
        if latest_peak_snapshot is None or latest_peak_snapshot.peak_equity is None:
            return float(fallback or self._book_equity(account))
        cash_delta = self._external_cash_flow_delta(
            session,
            account_id=account.id,
            start_exclusive=latest_peak_snapshot.observed_at,
            end_inclusive=current_naive,
        )
        return float(latest_peak_snapshot.peak_equity) + cash_delta

    def _cooldown_status_for_session(
        self,
        session,
        *,
        account: PaperCapitalAccount,
        session_date: date,
        losing_streak: int,
    ) -> Dict[str, Any]:
        latest = session.execute(
            select(PaperAccountRiskEvent)
            .where(
                PaperAccountRiskEvent.capital_account_id == account.id,
                PaperAccountRiskEvent.event_type.in_(["cooldown_start", "cooldown_recovered"]),
            )
            .order_by(desc(PaperAccountRiskEvent.created_at), desc(PaperAccountRiskEvent.id))
            .limit(1)
        ).scalar_one_or_none()
        if latest is not None and latest.event_type == "cooldown_start":
            recovery, _ = self._json_object(latest.recovery_condition_json or "{}")
            expiry_raw = recovery.get("expiry_session")
            try:
                expiry_session = date.fromisoformat(str(expiry_raw))
            except Exception:
                expiry_session = None
            if expiry_session is None or session_date <= expiry_session:
                return {"active": True, "started_event_id": latest.id, "expiry_session": expiry_raw}
            return {"active": False, "expired": True, "started_event_id": latest.id, "expiry_session": expiry_raw}
        if losing_streak >= self.account_losing_streak_limit:
            return {
                "active": True,
                "new": True,
                "expiry_session": self._next_trading_session(account.market, session_date).isoformat(),
            }
        return {"active": False}

    def _record_risk_state_transition_if_changed(
        self,
        session,
        *,
        account: PaperCapitalAccount,
        snapshot: PaperAccountRiskSnapshot,
        previous_snapshot: Optional[PaperAccountRiskSnapshot],
        cooldown_status: Mapping[str, Any],
    ) -> None:
        reasons, _ = self._json_list(snapshot.active_reasons_json or "[]")
        previous_reasons, _ = self._json_list(previous_snapshot.active_reasons_json or "[]") if previous_snapshot is not None else ([], None)
        previous_state = previous_snapshot.active_state if previous_snapshot is not None else None
        if previous_snapshot is None and snapshot.active_state == "normal" and not reasons:
            return
        if previous_state == snapshot.active_state and sorted(previous_reasons) == sorted(reasons):
            return
        event_type = "risk_state_changed"
        if snapshot.active_state == "cooldown":
            event_type = "cooldown_start"
        elif cooldown_status.get("expired") and snapshot.active_state == "normal":
            event_type = "cooldown_recovered"
        self._record_account_risk_event_in_session(
            session,
            account=account,
            snapshot=snapshot,
            event_type=event_type,
            old_state=previous_state,
            new_state=snapshot.active_state,
            active_reasons=reasons,
            metric="account_risk_state",
            threshold=None,
            source="snapshot",
            recovery_condition={
                "expiry_session": cooldown_status.get("expiry_session"),
                "drawdown_recovery_pct": self.account_drawdown_recovery_pct,
                "reduce_close_allowed": True,
            },
            evidence={"previous_snapshot_id": getattr(previous_snapshot, "id", None), "cooldown": dict(cooldown_status or {})},
        )

    def _account_risk_policy_evidence(self) -> Dict[str, Any]:
        return {
            "daily_loss_limit_pct": self.account_daily_loss_limit_pct,
            "drawdown_limit_pct": self.account_drawdown_limit_pct,
            "drawdown_recovery_pct": self.account_drawdown_recovery_pct,
            "losing_streak_limit": self.account_losing_streak_limit,
            "daily_trade_limit": self.account_daily_trade_limit,
            "daily_open_add_limit": self.account_daily_open_add_limit,
            "sector_cap_pct": self.account_sector_cap_pct,
            "correlation_cap_pct": self.account_correlation_cap_pct,
            "unknown_cap_pct": self.account_unknown_cap_pct,
            "correlation_threshold": self.correlation_threshold,
            "correlation_lookback_sessions": self.correlation_lookback_sessions,
            "correlation_min_overlap_sessions": self.correlation_min_overlap_sessions,
        }

    def _risk_reasons_from_metrics(
        self,
        *,
        valuation_complete: bool,
        valuation_stale: bool,
        current_equity: Optional[float],
        daily_loss_pct: Optional[float],
        drawdown_pct: Optional[float],
        losing_streak: int,
        daily_trade_count: int,
        daily_open_add_count: int,
        concentration: Mapping[str, Any],
    ) -> List[str]:
        reasons: List[str] = []
        if current_equity is None or current_equity <= 0:
            reasons.append("nonpositive_equity")
        if not valuation_complete:
            reasons.append("incomplete_valuation")
        if valuation_stale:
            reasons.append("stale_valuation")
        if daily_loss_pct is not None and daily_loss_pct >= self.account_daily_loss_limit_pct:
            reasons.append("daily_loss_limit_breached")
        if drawdown_pct is not None and drawdown_pct >= self.account_drawdown_limit_pct:
            reasons.append("drawdown_limit_breached")
        if losing_streak >= self.account_losing_streak_limit:
            reasons.append("losing_streak_cooldown")
        if daily_trade_count >= self.account_daily_trade_limit:
            reasons.append("daily_trade_limit_reached")
        if daily_open_add_count >= self.account_daily_open_add_limit:
            reasons.append("daily_open_add_limit_reached")
        for key in ("sector", "correlation", "unknown"):
            item = concentration.get(key) if isinstance(concentration, Mapping) else None
            if isinstance(item, Mapping) and item.get("over_cap"):
                reasons.append(f"{key}_concentration_cap_exceeded")
        return reasons

    @staticmethod
    def _risk_state_from_reasons(reasons: Sequence[str]) -> str:
        hard = {
            "nonpositive_equity",
            "incomplete_valuation",
            "stale_valuation",
            "daily_loss_limit_breached",
            "drawdown_limit_breached",
            "daily_trade_limit_reached",
            "daily_open_add_limit_reached",
            "sector_concentration_cap_exceeded",
            "correlation_concentration_cap_exceeded",
            "unknown_concentration_cap_exceeded",
            "market_context_risk_off",
            "market_context_capacity_cap",
        }
        if any(reason in hard for reason in reasons):
            return "reduce_only"
        if "losing_streak_cooldown" in reasons:
            return "cooldown"
        return "normal"

    @staticmethod
    def _risk_permissions(state: str, reasons: Sequence[str]) -> Dict[str, Any]:
        allow_new_risk = state == "normal"
        return {
            "open": allow_new_risk,
            "add": allow_new_risk,
            "reduce": True,
            "close": True,
            "hard_stop": True,
            "t1_pending_exit": True,
            "state": state,
            "reasons": list(reasons or []),
        }

    def _hard_risk_reasons(self, snapshot: PaperAccountRiskSnapshot) -> List[str]:
        reasons, _ = self._json_list(snapshot.active_reasons_json or "[]")
        hard = {
            "nonpositive_equity",
            "incomplete_valuation",
            "stale_valuation",
            "daily_loss_limit_breached",
            "drawdown_limit_breached",
            "daily_trade_limit_reached",
            "daily_open_add_limit_reached",
            "sector_concentration_cap_exceeded",
            "correlation_concentration_cap_exceeded",
            "unknown_concentration_cap_exceeded",
            "market_context_risk_off",
            "market_context_capacity_cap",
        }
        return [str(reason) for reason in reasons if str(reason) in hard]

    def _manual_hold_active_in_session(self, session, account_id: int) -> bool:
        latest = session.execute(
            select(PaperAccountRiskEvent)
            .where(
                PaperAccountRiskEvent.capital_account_id == int(account_id),
                PaperAccountRiskEvent.event_type.in_(["manual_hold", "manual_resume"]),
            )
            .order_by(desc(PaperAccountRiskEvent.created_at), desc(PaperAccountRiskEvent.id))
            .limit(1)
        ).scalar_one_or_none()
        return latest is not None and latest.event_type == "manual_hold"

    def _losing_logical_exit_streak(self, session, account: PaperCapitalAccount) -> int:
        rows = list(session.execute(
            select(PaperTradeEvent)
            .where(
                PaperTradeEvent.capital_account_id == account.id,
                PaperTradeEvent.event_type.in_(["reduce", "close"]),
            )
            .order_by(desc(PaperTradeEvent.event_date), desc(PaperTradeEvent.id))
            .limit(50)
        ).scalars().all())
        seen = set()
        streak = 0
        for event in rows:
            key = str(event.logical_order_key or f"event:{event.id}")
            if key in seen:
                continue
            seen.add(key)
            details, _ = self._json_object(event.details_json or "{}")
            pnl = self._safe_float(details.get("realized_pnl"))
            if pnl is None:
                pnl = self._safe_float(details.get("net_realized_pnl"))
            if pnl is None:
                position = session.get(PaperPosition, int(event.position_id)) if event.position_id else None
                pnl = self._safe_float(getattr(position, "realized_pnl", None)) if position is not None else None
            if pnl is not None and pnl < 0:
                streak += 1
                continue
            break
        return streak

    def _account_concentration_evidence(
        self,
        session,
        *,
        account: PaperCapitalAccount,
        positions: Sequence[PaperPosition],
        marked_equity: Optional[float],
        candidate: Optional[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        equity = marked_equity if marked_equity and marked_equity > 0 else None
        entries: List[Dict[str, Any]] = []
        for position in positions:
            entries.append({
                "market": position.market,
                "stock_code": position.stock_code,
                "exposure": self._position_allocated_capital(position),
                "sector": self._sector_or_unknown(session, position.market, position.stock_code),
                "bars": self._complete_return_series(session, position.market, position.stock_code),
            })
        if candidate:
            entries.append({
                "market": str(candidate.get("market") or account.market),
                "stock_code": str(candidate.get("stock_code") or ""),
                "exposure": float(candidate.get("projected_additional_cost") or 0.0),
                "sector": self._sector_or_unknown(
                    session,
                    str(candidate.get("market") or account.market),
                    str(candidate.get("stock_code") or ""),
                ),
                "bars": self._complete_return_series(session, str(candidate.get("market") or account.market), str(candidate.get("stock_code") or "")),
                "candidate": True,
            })
        sector_exposure: Dict[str, float] = {}
        unknown_exposure = 0.0
        for entry in entries:
            sector = str(entry.get("sector") or "unknown")
            exposure = float(entry.get("exposure") or 0.0)
            sector_exposure[sector] = sector_exposure.get(sector, 0.0) + exposure
            if sector == "unknown" or len(entry.get("bars") or []) < self.correlation_min_overlap_sessions:
                unknown_exposure += exposure
        max_sector = max(sector_exposure.values(), default=0.0)
        cluster_exposure, cluster_members, max_corr = self._correlated_cluster_exposure(entries)
        def bucket(exposure: float, cap_pct: float, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            utilization = (exposure / equity * 100.0) if equity else None
            return {
                "exposure": exposure,
                "cap_pct": cap_pct,
                "cap_amount": (equity * cap_pct / 100.0) if equity else None,
                "utilization_pct": utilization,
                "over_cap": bool(equity and utilization is not None and utilization > cap_pct + 1e-9),
                **(extra or {}),
            }
        return {
            "sector": bucket(max_sector, self.account_sector_cap_pct, {"groups": sector_exposure}),
            "correlation": bucket(cluster_exposure, self.account_correlation_cap_pct, {
                "members": cluster_members,
                "max_correlation": max_corr,
                "threshold": self.correlation_threshold,
                "lookback_sessions": self.correlation_lookback_sessions,
                "min_overlap_sessions": self.correlation_min_overlap_sessions,
            }),
            "unknown": bucket(unknown_exposure, self.account_unknown_cap_pct, {
                "reason": "missing_sector_or_insufficient_correlation_bars",
            }),
        }

    def _sector_or_unknown(self, session, market: str, stock_code: str) -> str:
        """Return persisted attributable sector/asset metadata or explicit unknown."""

        identity = canonical_paper_identity(market, stock_code)
        if identity is None:
            return "unknown"
        canonical_market, canonical_code = identity
        rows = list(session.execute(
            select(AnalysisHistory)
            .where(
                AnalysisHistory.code.in_(stock_daily_variants(canonical_market, canonical_code) + [canonical_code]),
                # Research-only report payloads must not reclassify account risk.
                authorized_report_execution_clause(AnalysisHistory.id, execution_scope=self.execution_scope),
            )
            .order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id))
            .limit(20)
        ).scalars().all())
        for row in rows:
            for payload_text in (getattr(row, "context_snapshot", None), getattr(row, "raw_result", None)):
                payload, error = self._json_object(payload_text or "{}")
                if error or not payload:
                    continue
                sector = self._extract_attributable_sector(payload)
                if sector:
                    return sector
        return "unknown"

    def _extract_attributable_sector(self, payload: Mapping[str, Any]) -> Optional[str]:
        candidates: List[Any] = []
        for key in ("belong_boards", "boards", "related_boards"):
            value = payload.get(key)
            if isinstance(value, Mapping):
                candidates.append(value.get("data"))
                candidates.append(value.get("items"))
                candidates.append(value.get("belong_boards"))
            else:
                candidates.append(value)
        fundamental = payload.get("fundamental_context")
        if isinstance(fundamental, Mapping):
            boards = fundamental.get("boards")
            if isinstance(boards, Mapping):
                candidates.extend([boards.get("data"), boards.get("items"), boards.get("belong_boards")])
            else:
                candidates.append(boards)
        for value in candidates:
            if isinstance(value, Mapping):
                value = [value]
            if not isinstance(value, list):
                continue
            for item in value:
                if not isinstance(item, Mapping):
                    continue
                name = str(
                    item.get("name")
                    or item.get("板块名称")
                    or item.get("industry")
                    or item.get("sector")
                    or ""
                ).strip()
                type_text = str(
                    item.get("type")
                    or item.get("board_type")
                    or item.get("板块类型")
                    or item.get("source")
                    or ""
                ).lower()
                if not name:
                    continue
                if any(token in type_text for token in ("industry", "sector", "行业", "asset_group")):
                    return name[:64]
        return None

    def _complete_return_series(self, session, market: str, stock_code: str) -> List[Dict[str, Any]]:
        rows = list(session.execute(
            select(StockDaily)
            .where(StockDaily.code.in_(stock_daily_variants(market, stock_code)))
            .order_by(desc(StockDaily.date))
            .limit(self.correlation_lookback_sessions + 1)
        ).scalars().all())
        by_date: Dict[date, Any] = {}
        for row in rows:
            existing = by_date.get(row.date)
            if existing is not None and self._safe_float(existing.close) != self._safe_float(row.close):
                return []
            by_date[row.date] = row
        ordered = [by_date[key] for key in sorted(by_date.keys())]
        returns: List[Dict[str, Any]] = []
        prev = None
        for row in ordered:
            close = self._safe_float(row.close)
            if close is None or close <= 0:
                prev = None
                continue
            if prev is not None and prev > 0:
                returns.append({"date": row.date.isoformat(), "return": close / prev - 1.0})
            prev = close
        return returns[-self.correlation_lookback_sessions:]

    def _correlated_cluster_exposure(self, entries: Sequence[Mapping[str, Any]]) -> Tuple[float, List[str], Optional[float]]:
        candidate_entries = [entry for entry in entries if len(entry.get("bars") or []) >= self.correlation_min_overlap_sessions]
        if not candidate_entries:
            return 0.0, [], None
        exposures: Dict[str, float] = {}
        adjacency: Dict[str, set] = {}
        max_corr = None
        for entry in candidate_entries:
            key = f"{entry.get('market')}:{entry.get('stock_code')}"
            exposures[key] = exposures.get(key, 0.0) + float(entry.get("exposure") or 0.0)
            adjacency.setdefault(key, set()).add(key)
        for idx, left in enumerate(candidate_entries):
            for right in candidate_entries[idx + 1:]:
                corr = self._return_correlation(left.get("bars") or [], right.get("bars") or [])
                if corr is None:
                    continue
                max_corr = corr if max_corr is None else max(max_corr, corr)
                if corr >= self.correlation_threshold:
                    lkey = f"{left.get('market')}:{left.get('stock_code')}"
                    rkey = f"{right.get('market')}:{right.get('stock_code')}"
                    adjacency.setdefault(lkey, set()).add(rkey)
                    adjacency.setdefault(rkey, set()).add(lkey)
        visited = set()
        best_members: List[str] = []
        best_exposure = 0.0
        for node in adjacency:
            if node in visited:
                continue
            stack = [node]
            members = []
            exposure = 0.0
            while stack:
                current = stack.pop()
                if current in visited:
                    continue
                visited.add(current)
                members.append(current)
                exposure += exposures.get(current, 0.0)
                stack.extend(adjacency.get(current, set()) - visited)
            if exposure > best_exposure:
                best_exposure = exposure
                best_members = sorted(members)
        return best_exposure, best_members, max_corr

    def _return_correlation(self, left: Sequence[Any], right: Sequence[Any]) -> Optional[float]:
        def dated_map(values: Sequence[Any]) -> Dict[str, float]:
            result: Dict[str, float] = {}
            for item in values:
                if not isinstance(item, Mapping):
                    return {}
                value = self._safe_float(item.get("return"))
                date_key = str(item.get("date") or "")
                if not date_key or value is None:
                    return {}
                result[date_key] = value
            return result
        left_map = dated_map(left)
        right_map = dated_map(right)
        overlap_dates = sorted(set(left_map) & set(right_map))
        overlap = len(overlap_dates)
        if overlap < self.correlation_min_overlap_sessions:
            return None
        xs = [left_map[key] for key in overlap_dates[-self.correlation_lookback_sessions:]]
        ys = [right_map[key] for key in overlap_dates[-self.correlation_lookback_sessions:]]
        overlap = len(xs)
        if overlap < self.correlation_min_overlap_sessions:
            return None
        mean_x = sum(xs) / overlap
        mean_y = sum(ys) / overlap
        var_x = sum((x - mean_x) ** 2 for x in xs)
        var_y = sum((y - mean_y) ** 2 for y in ys)
        if var_x <= 0 or var_y <= 0:
            return None
        cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        return cov / math.sqrt(var_x * var_y)

    def _experiment_arm(self) -> Optional[str]:
        """Return the registered A/B/C arm for an experiment scope, else ``None``.

        Only trusted experiment DBs carry an ``experiment:<id>:<abc>`` scope; the
        primary book never enables the opportunity arms.
        """

        scope = self.execution_scope
        if scope == REPORT_EXECUTION_SCOPE_PRIMARY:
            return None
        parts = scope.split(":")
        if len(parts) == 3 and parts[0] == "experiment" and parts[2] in {"a", "b", "c"}:
            return parts[2].upper()
        return None

    def _market_policy_decision_in_session(
        self,
        session,
        *,
        account: PaperCapitalAccount,
        market: str,
        current_time: datetime,
    ) -> Dict[str, Any]:
        """Single market-risk decision every open-path gate consumes.

        Primary keeps the existing regime semantics untouched.  Experiment arms
        evaluate the pure ``evaluate_opportunity_market_policy`` on a snapshot
        built from the same authoritative regime facts plus the arm DB's CSI300
        window and the trusted calendar, so sizing, symbol/total caps and the
        pre-fill re-check all read one decision with one ``current_time``.
        """

        context = self._market_context_execution_risk_in_session(
            session,
            market=market,
            current_time=current_time,
        )
        arm = self._experiment_arm()
        if arm is None:
            return {
                "policy": "primary",
                "arm": None,
                "block_new_risk": bool(context.get("block_new_risk")),
                "allocation_multiplier": float(context.get("allocation_multiplier", 1.0) or 0.0),
                "symbol_cap_factor": 1.0,
                "total_cap_factor": 1.0,
                "reasons": list(context.get("reasons") or []),
                "market_context": context,
                "exception": None,
                "policy_version": None,
                "input_hash": None,
            }

        base_symbol = float(self.max_symbol_exposure_pct or 0.0)
        base_total = float(account.max_total_exposure_pct or 0.0)
        snapshot = self._build_opportunity_market_snapshot(
            session,
            market=market,
            current_time=current_time,
            context=context,
            base_symbol_pct=base_symbol,
            base_total_pct=base_total,
        )
        decision = evaluate_opportunity_market_policy(
            snapshot,
            arm=arm,
            policy_version=OPPORTUNITY_MARKET_POLICY_VERSION,
        )
        symbol_cap_factor = (
            float(decision["max_symbol_exposure_pct"]) / base_symbol if base_symbol > 0 else 1.0
        )
        total_cap_factor = (
            float(decision["max_total_exposure_pct"]) / base_total if base_total > 0 else 1.0
        )
        return {
            "policy": "opportunity_experiment",
            "arm": arm,
            "block_new_risk": not bool(decision["entry_allowed"]),
            "allocation_multiplier": float(decision["allocation_multiplier"]),
            "symbol_cap_factor": symbol_cap_factor,
            "total_cap_factor": total_cap_factor,
            "reasons": list(decision["reasons"]),
            "market_context": context,
            "exception": decision.get("exception"),
            "policy_version": decision.get("policy_version"),
            "input_hash": decision.get("input_hash"),
            "opportunity_trend": decision.get("trend"),
        }

    def _build_opportunity_market_snapshot(
        self,
        session,
        *,
        market: str,
        current_time: datetime,
        context: Mapping[str, Any],
        base_symbol_pct: float,
        base_total_pct: float,
    ) -> Dict[str, Any]:
        """Assemble the account-independent snapshot the pure B1 policy consumes.

        The regime block reflects the engine's authoritative freshness verdict
        (never rewriting a persisted ``risk_off`` row); direction/shock come from
        the arm DB's CSI300 window and the trusted exchange calendar.
        """

        wanted_market = str(market or "").strip().lower()
        as_of, bars = self._opportunity_index_bars(session, wanted_market, current_time)
        sessions = self._opportunity_calendar_sessions(wanted_market, as_of) if as_of else []
        regime_name = str(context.get("regime") or "unavailable").strip().lower()
        # ``available`` is the engine's own freshness verdict (fresh & known),
        # independent of the entry block — a current ``risk_off`` is available,
        # so the pure B1 exception can still evaluate it instead of failing
        # closed on a spurious staleness. A stale/unknown context forces the
        # policy's own stale/unavailable branch; the persisted regime is never
        # rewritten to ``normal``.
        fresh = bool(context.get("available"))
        if fresh and as_of and regime_name in {"normal", "cautious", "risk_off"}:
            regime_block = {
                "regime": regime_name,
                "valid": True,
                "session_date": as_of.isoformat(),
            }
        elif regime_name in {"normal", "cautious", "risk_off"}:
            # Known but stale/uncurrent: keep the name, drop currency so the pure
            # policy reports ``market_context_stale`` rather than authorizing.
            regime_block = {"regime": regime_name, "valid": True, "session_date": None}
        else:
            regime_block = {"regime": "unavailable", "valid": False, "session_date": None}
        return {
            "as_of": as_of.isoformat() if as_of else None,
            "regime": regime_block,
            "base_limits": {
                "max_symbol_exposure_pct": base_symbol_pct,
                "max_total_exposure_pct": base_total_pct,
            },
            "index": {
                "code": OPPORTUNITY_BENCHMARK_INDEX_CODE,
                "sessions": [day.isoformat() for day in sessions],
                "bars": bars,
            },
        }

    def _opportunity_index_bars(
        self,
        session,
        market: str,
        current_time: datetime,
    ) -> Tuple[Optional[date], List[Dict[str, Any]]]:
        """Return (as_of, bars) for CSI300 up to the current session, fail-closed."""

        try:
            through = self._market_local_date_for_time(market, current_time)
        except Exception:
            return None, []
        variants = [OPPORTUNITY_BENCHMARK_INDEX_CODE, "sh000300", "000300"]
        rows = list(session.execute(
            select(StockDaily)
            .where(StockDaily.code.in_(variants), StockDaily.date <= through)
            .order_by(desc(StockDaily.date), desc(StockDaily.id))
            .limit(OPPORTUNITY_INDEX_WINDOW_SESSIONS + 5)
        ).scalars().all())
        if not rows:
            return None, []
        as_of = rows[0].date
        bars: List[Dict[str, Any]] = []
        seen: set = set()
        for row in rows:
            if row.date in seen:
                continue
            seen.add(row.date)
            bars.append({
                "date": row.date.isoformat(),
                "close": float(row.close) if row.close is not None else None,
            })
        return as_of, bars

    def _opportunity_calendar_sessions(self, market: str, as_of: date) -> List[date]:
        """Return the trusted CSI300 calendar window ending at ``as_of``, fail-closed."""

        parts = _market_context_calendar(market)
        if parts is None:
            return []
        cal, _ex, _tz = parts
        try:
            start = as_of - timedelta(days=OPPORTUNITY_INDEX_WINDOW_SESSIONS * 3 + 30)
            sessions = [ts.date() for ts in cal.sessions_in_range(start, as_of)]
        except Exception:
            return []
        return sessions[-OPPORTUNITY_INDEX_WINDOW_SESSIONS:] if sessions else []

    def _market_context_execution_risk_in_session(
        self,
        session,
        *,
        market: str,
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Translate persisted structured market regime into entry/add gates.

        Absence of risk keywords is intentionally not treated as ``normal``.
        When the structured regime is missing, stale, or unavailable, new risk
        fails closed while exits remain outside this entry/add-only gate.
        """

        if not self.market_context_risk_control_enabled:
            return {"enabled": False, "block_new_risk": False, "allocation_multiplier": 1.0}
        now = current_time or datetime.now(timezone.utc)
        now_naive = self._utc_naive(now)
        wanted_market = str(market or "").lower()
        session_date = self._market_local_date_for_time(wanted_market, now)
        row = session.execute(
            select(DailyMarketRegime)
            .where(
                DailyMarketRegime.region == wanted_market,
                DailyMarketRegime.session_date <= session_date,
                DailyMarketRegime.policy_version == MARKET_REGIME_POLICY_VERSION,
            )
            .order_by(desc(DailyMarketRegime.session_date), desc(DailyMarketRegime.created_at), desc(DailyMarketRegime.id))
            .limit(1)
        ).scalar_one_or_none()
        if row is not None:
            effective_expires_at = resolve_market_context_freshness_expires_at(
                wanted_market,
                row.session_date,
                row.freshness_expires_at,
            )
            completed_session_lag = market_context_completed_session_lag(
                wanted_market,
                row.session_date,
                current_time=now,
            )
            calendar_unavailable = effective_expires_at is None or completed_session_lag is None
            expired = bool(effective_expires_at is None or effective_expires_at <= now_naive)
            stale_by_sessions = (
                completed_session_lag is None
                or completed_session_lag >= self.market_context_lookback_days
            )
            stale = bool(
                row.session_date > session_date
                or expired
                or stale_by_sessions
            )
            regime = str(row.regime or "unavailable").strip().lower()
            policy = MARKET_REGIME_EXECUTION_POLICY.get(regime, MARKET_REGIME_EXECUTION_POLICY["unavailable"])
            provenance, _ = self._json_object(getattr(row, "provenance_json", None) or "{}")
            reasons: List[str] = []
            if stale:
                reasons.append("market_context_stale")
                if calendar_unavailable:
                    reasons.append("market_context_calendar_unavailable")
            elif not policy.get("entry_allowed"):
                reasons.append(str(policy.get("reason") or "market_context_unavailable"))
            elif regime == "cautious":
                reasons.append("market_context_cautious")
            return {
                "enabled": True,
                "available": not stale and regime in MARKET_REGIME_EXECUTION_POLICY,
                "source_report_id": row.source_report_id,
                "market_context_id": row.id,
                "observed_at": row.created_at.isoformat() if row.created_at else None,
                "freshness_expires_at": effective_expires_at.isoformat() if effective_expires_at else None,
                "completed_session_lag": completed_session_lag,
                "market": wanted_market,
                "session_date": row.session_date.isoformat() if row.session_date else None,
                "regime": regime if regime in MARKET_REGIME_EXECUTION_POLICY else "unavailable",
                "policy_version": row.policy_version,
                "source": provenance.get("source"),
                "provenance": provenance,
                "block_new_risk": stale or not bool(policy.get("entry_allowed")),
                "allocation_multiplier": 0.0 if stale else float(policy.get("allocation_multiplier", 0.0) or 0.0),
                "reasons": reasons,
            }
        return {
            "enabled": True,
            "available": False,
            "market": wanted_market,
            "regime": "unavailable",
            "policy_version": MARKET_REGIME_POLICY_VERSION,
            "session_date": session_date.isoformat(),
            "block_new_risk": True,
            "allocation_multiplier": 0.0,
            "reasons": ["market_context_unavailable"],
        }

    def market_context_risk_status(
        self,
        *,
        market: str = "cn",
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Return the authoritative persisted market-regime execution gate snapshot."""

        with self.db.get_session() as session:
            snapshot = self._market_context_execution_risk_in_session(
                session,
                market=market,
                current_time=current_time,
            )
        return {
            **snapshot,
            "scope": {
                "market": str(market or "cn").strip().lower() or "cn",
                "source": "daily_market_regimes",
                "model": None,
            },
            "fresh": bool(snapshot.get("available")) and not bool(snapshot.get("block_new_risk") and "market_context_stale" in (snapshot.get("reasons") or [])),
            "entry_allowed": not bool(snapshot.get("block_new_risk")),
        }

    @staticmethod
    def _market_review_matches(raw: Mapping[str, Any], market: str) -> bool:
        if not market:
            return True
        region = str(
            raw.get("market_review_region")
            or raw.get("region")
            or raw.get("market")
            or ""
        ).lower()
        if not region or region in {"both", "all", "global"}:
            return True
        regions = {item.strip() for item in re.split(r"[,/|;\s]+", region) if item.strip()}
        return market in regions

    @staticmethod
    def _extract_market_risk_tags(text: str, raw: Mapping[str, Any]) -> List[str]:
        tags: List[str] = []
        raw_tags = raw.get("risk_tags") or raw.get("market_risk_tags")
        if isinstance(raw_tags, Sequence) and not isinstance(raw_tags, (str, bytes)):
            tags.extend(str(item).strip().lower() for item in raw_tags if str(item).strip())
        patterns = {
            "high_risk": ["高风险", "风险较高", "high risk", "extreme risk"],
            "risk_off": ["risk off", "避险", "防御优先", "降低仓位"],
            "market_cooling": ["退潮", "降温", "赚钱效应下降", "情绪走弱"],
            "conservative": ["保守", "谨慎", "控制仓位", "轻仓"],
            "weak": ["弱势", "偏弱", "承压"],
        }
        for tag, needles in patterns.items():
            if any(needle.lower() in text for needle in needles) and tag not in tags:
                tags.append(tag)
        return tags

    @classmethod
    def _extract_market_position_cap_pct(cls, text: str, raw: Mapping[str, Any]) -> Optional[float]:
        for key in ("position_cap_pct", "max_position_pct", "suggested_position_pct"):
            value = cls._safe_float(raw.get(key))
            if value is not None and value >= 0:
                return min(100.0, value)
        match = re.search(r"(仓位|总仓位|建议仓位)[^0-9%]{0,12}([0-9]+(?:\.[0-9]+)?)\s*%", text)
        if match:
            value = cls._safe_float(match.group(2))
            if value is not None:
                return min(100.0, max(0.0, value))
        return None

    def _account_risk_gate_for_open(
        self,
        session,
        *,
        account: PaperCapitalAccount,
        plan: PaperExecutionPlan,
        projected_additional_cost: float,
        current_time: datetime,
        bucket_started_at: Optional[datetime],
        resolution_key: Optional[str],
        decision: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        if decision is None:
            decision = self._market_policy_decision_in_session(
                session,
                account=account,
                market=plan.market,
                current_time=current_time,
            )
        market_context = decision.get("market_context") or {}
        snapshot = self._compute_account_risk_snapshot_in_session(
            session,
            account=account,
            current_time=current_time,
            bucket_started_at=bucket_started_at or current_time,
            persist=True,
        )
        concentration = self._account_concentration_evidence(
            session,
            account=account,
            positions=list(session.execute(
                select(PaperPosition)
                .where(PaperPosition.capital_account_id == account.id, PaperPosition.status == "open")
            ).scalars().all()),
            marked_equity=snapshot.current_equity if snapshot.valuation_complete else None,
            candidate={
                "market": plan.market,
                "stock_code": plan.stock_code,
                "projected_additional_cost": projected_additional_cost,
            },
        )
        reasons, _ = self._json_list(snapshot.active_reasons_json or "[]")
        reasons = list(reasons)
        for reason in self._risk_reasons_from_metrics(
            valuation_complete=bool(snapshot.valuation_complete),
            valuation_stale=bool(snapshot.valuation_stale),
            current_equity=snapshot.current_equity,
            daily_loss_pct=snapshot.daily_loss_pct,
            drawdown_pct=snapshot.drawdown_pct,
            losing_streak=int(snapshot.losing_logical_exit_streak or 0),
            daily_trade_count=int(snapshot.daily_trade_count or 0),
            daily_open_add_count=int(snapshot.daily_open_add_count or 0),
            concentration=concentration,
        ):
            if reason not in reasons:
                reasons.append(reason)
        # One decision drives both the block verdict and the capacity cap, so a
        # granted experiment red-light trial is never re-blocked by the legacy
        # regime rule, and the position cap is scaled once by the same multiplier.
        market_multiplier = max(0.0, min(1.0, float(decision.get("allocation_multiplier") or 0.0)))
        market_capital_cap = self._book_equity(account) * (float(account.max_position_pct or 0.0) / 100.0) * market_multiplier
        if decision.get("block_new_risk"):
            market_reasons = list(decision.get("reasons") or []) or ["market_context_unavailable"]
            for market_reason in market_reasons:
                if market_reason not in reasons:
                    reasons.append(str(market_reason))
        elif market_context.get("available") and projected_additional_cost > market_capital_cap + 1e-9:
            if "market_context_capacity_cap" not in reasons:
                reasons.append("market_context_capacity_cap")
            market_context = {
                **market_context,
                "blocked_by_capacity": True,
                "projected_additional_cost": float(projected_additional_cost or 0.0),
                "adjusted_position_capital": market_capital_cap,
            }
        state = self._risk_state_from_reasons(reasons)
        if self._manual_hold_active_in_session(session, account.id):
            state = "manual_hold"
            if "manual_hold" not in reasons:
                reasons.append("manual_hold")
        permissions = self._risk_permissions(state, reasons)
        if not permissions["open"]:
            self._record_account_risk_event_in_session(
                session,
                account=account,
                snapshot=snapshot,
                event_type="entry_blocked",
                old_state=snapshot.active_state,
                new_state=state,
                active_reasons=reasons,
                metric="open_add_gate",
                threshold=None,
                source="resolver",
                recovery_condition={"reduce_close_allowed": True},
                evidence={
                    "plan_id": plan.id,
                    "resolution_key": resolution_key,
                    "concentration": concentration,
                    "market_context": market_context,
                    "permissions": permissions,
                },
                resolution_key=resolution_key,
            )
        return {
            "allowed": bool(permissions["open"]),
            "state": state,
            "reasons": reasons,
            "snapshot": self._serialize_account_risk_snapshot(snapshot),
            "concentration": concentration,
            "market_context": market_context,
            "market_policy_decision": self._market_policy_audit(decision),
            "permissions": permissions,
        }

    def _estimated_buy_net_amount(self, *, price: float, quantity: float) -> float:
        if not self._positive_finite(price) or not self._positive_finite(quantity):
            return 0.0
        fill_price = float(price) * (1.0 + self.slippage_pct / 100.0)
        gross = fill_price * float(quantity)
        commission = max(self.min_commission, gross * self.commission_rate) if gross > 0 else 0.0
        return float(gross + commission)

    def _shrink_buy_quantity_to_cap(
        self,
        *,
        quantity: float,
        lot_size: int,
        price: float,
        cap: float,
    ) -> float:
        safe_lot = max(1, int(lot_size or 1))
        candidate = math.floor(max(0.0, float(quantity or 0.0)) / safe_lot) * safe_lot
        limit = max(0.0, float(cap or 0.0))
        while candidate > 0 and self._estimated_buy_net_amount(price=price, quantity=candidate) > limit + 1e-9:
            candidate -= safe_lot
        return float(max(0, candidate))

    def _open_fill_invariant_error(
        self,
        *,
        account: PaperCapitalAccount,
        fill: Mapping[str, Any],
        symbol_snapshot: Optional[Mapping[str, Any]] = None,
        total_cap_factor: float = 1.0,
    ) -> Optional[str]:
        net_amount = self._safe_float(fill.get("net_amount"))
        if net_amount is None or net_amount <= 0:
            return "invalid_open_fill_net_amount"
        if net_amount > float(account.cash or 0.0) + 1e-9:
            return "open_fill_exceeds_available_cash"
        book_equity = self._book_equity(account)
        position_cap = book_equity * (float(account.max_position_pct or 0.0) / 100.0)
        if net_amount > position_cap + 1e-9:
            return "open_fill_exceeds_position_cap"
        # The experiment red-light trial scales the frozen total-exposure cap
        # once; the primary book always passes factor 1.0 (unchanged room).
        factor = max(0.0, float(total_cap_factor if total_cap_factor is not None else 1.0))
        total_room = max(
            0.0,
            book_equity * (float(account.max_total_exposure_pct or 0.0) / 100.0) * factor
            - float(account.exposure or 0.0),
        )
        if net_amount > total_room + 1e-9:
            return "open_fill_exceeds_total_exposure_cap"
        if isinstance(symbol_snapshot, Mapping) and not bool(symbol_snapshot.get("within_limit", True)):
            return "open_fill_exceeds_symbol_exposure_cap"
        return None

    def _record_account_risk_event_in_session(
        self,
        session,
        *,
        account: PaperCapitalAccount,
        snapshot: PaperAccountRiskSnapshot,
        event_type: str,
        old_state: Optional[str],
        new_state: str,
        active_reasons: Sequence[str],
        metric: Optional[str],
        threshold: Optional[float],
        source: str,
        recovery_condition: Mapping[str, Any],
        evidence: Mapping[str, Any],
        idempotency_key: Optional[str] = None,
        resolution_key: Optional[str] = None,
        logical_order_key: Optional[str] = None,
    ) -> Tuple[PaperAccountRiskEvent, bool]:
        payload_hash = hashlib.sha256(
            self._json_dumps({
                "account_id": account.id,
                "event_type": event_type,
                "new_state": new_state,
                "active_reasons": list(active_reasons or []),
                "metric": metric,
                "threshold": threshold,
                "source": source,
                "recovery_condition": dict(recovery_condition or {}),
                "evidence": dict(evidence or {}),
            }).encode("utf-8")
        ).hexdigest()
        event_key = (
            f"risk-event:{idempotency_key}"
            if idempotency_key
            else "risk-event-" + hashlib.sha256(
                f"{account.id}|{snapshot.snapshot_key}|{event_type}|{new_state}|{','.join(active_reasons)}|{resolution_key or ''}".encode("utf-8")
            ).hexdigest()[:32]
        )
        existing = session.execute(
            select(PaperAccountRiskEvent)
            .where(PaperAccountRiskEvent.event_key == event_key)
            .limit(1)
        ).scalar_one_or_none()
        if existing is not None:
            if idempotency_key and existing.payload_hash != payload_hash:
                raise PaperTradingConflictError("idempotency key reused with a different risk-event payload")
            return existing, False
        row = PaperAccountRiskEvent(
            event_key=event_key,
            idempotency_key=idempotency_key,
            payload_hash=payload_hash,
            event_type=event_type,
            capital_account_id=account.id,
            snapshot_id=snapshot.id,
            resolution_key=resolution_key,
            logical_order_key=logical_order_key,
            source_model=account.source_model,
            market=account.market,
            currency=account.currency,
            old_state=old_state,
            new_state=new_state,
            active_reasons_json=self._json_dumps(list(active_reasons or [])),
            metric=metric,
            threshold=threshold,
            source=source,
            recovery_condition_json=self._json_dumps(dict(recovery_condition or {})),
            evidence_json=self._json_dumps(dict(evidence or {})),
            notification_status="not_configured" if not self.feishu_notify else "pending",
        )
        session.add(row)
        session.flush()
        return row, True

    def _size_position(
        self,
        account: PaperCapitalAccount,
        *,
        price: float,
        stop_loss: Optional[float],
        market: str,
        allocation_fraction: float = 1.0,
        existing_holding: bool = False,
        allocation_budget: Optional[float] = None,
        risk_budget: Optional[float] = None,
        risk_multiplier: Optional[float] = None,
        decision: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        stop = self._safe_price(stop_loss)
        if stop is None or stop >= price:
            return None, "invalid_or_missing_stop_loss_for_sizing"
        if decision is None:
            with self.db.get_session() as session:
                decision = self._market_policy_decision_in_session(
                    session,
                    account=account,
                    market=market,
                    current_time=datetime.now(timezone.utc),
                )
        market_context = decision.get("market_context") or {}
        if decision.get("block_new_risk"):
            reason = (decision.get("reasons") or ["market_context_risk_off"])[0]
            return None, str(reason)
        # Budget multiplier and cap scaling both come from the one decision, so
        # the multiplier is applied exactly once across the whole budget chain.
        allocation_multiplier = max(0.0, float(decision.get("allocation_multiplier", 1.0) or 0.0))
        total_cap_factor = max(0.0, float(decision.get("total_cap_factor", 1.0) or 0.0))
        risk_per_share = price - stop
        lot_size = self._lot_size_for_market(market)
        book_equity = self._book_equity(account)
        fraction = min(1.0, max(0.0, float(allocation_fraction or 0.0)))
        if fraction <= 0:
            return None, "invalid_entry_leg_fraction_for_sizing"
        policy_multiplier = min(
            1.0,
            max(0.0, float(risk_multiplier if risk_multiplier is not None else self.style_risk_multiplier))
            * allocation_multiplier,
        )
        if policy_multiplier <= 0:
            return None, "execution_policy_allocation_zero"
        max_risk_capital = (
            float(risk_budget)
            if self._positive_finite(risk_budget)
            else book_equity * (account.risk_per_trade_pct / 100.0)
        ) * fraction * policy_multiplier
        max_position_capital = (
            float(allocation_budget)
            if self._positive_finite(allocation_budget)
            else book_equity * (account.max_position_pct / 100.0)
        ) * fraction * policy_multiplier
        max_total_exposure = book_equity * (account.max_total_exposure_pct / 100.0) * total_cap_factor
        exposure_room = max(0.0, max_total_exposure - float(account.exposure or 0.0))
        cash_room = max(0.0, float(account.cash or 0.0))
        if not existing_holding and int(account.open_position_count or 0) >= int(account.max_open_positions or self.max_open_positions):
            return None, "max_open_positions_reached"
        risk_limited_qty = max_risk_capital / risk_per_share
        capital_limit = min(max_position_capital, exposure_room, cash_room)
        capital_limited_qty = capital_limit / price
        raw_qty = min(risk_limited_qty, capital_limited_qty)
        quantity = math.floor(raw_qty / lot_size) * lot_size
        quantity = self._shrink_buy_quantity_to_cap(
            quantity=quantity,
            lot_size=lot_size,
            price=price,
            cap=capital_limit,
        )
        if quantity <= 0:
            return None, "insufficient_capital_for_lot"
        allocated = self._estimated_buy_net_amount(price=price, quantity=quantity)
        return {
            "quantity": float(quantity),
            "allocated_capital": float(allocated),
            "lot_size": lot_size,
            "risk_amount": float(quantity * risk_per_share),
            "risk_per_share": float(risk_per_share),
            "currency": account.currency,
            "sizing_reason": "min_of_risk_position_exposure_cash_lot",
            "trading_style": self.trading_style,
            "execution_policy_multiplier": policy_multiplier,
            "market_context_risk": market_context,
            "market_policy_decision": self._market_policy_audit(decision),
        }, None

    @staticmethod
    def _market_policy_audit(decision: Mapping[str, Any]) -> Dict[str, Any]:
        """Compact, credential-free audit trail of the market-policy decision."""

        return {
            "policy": decision.get("policy"),
            "arm": decision.get("arm"),
            "policy_version": decision.get("policy_version"),
            "input_hash": decision.get("input_hash"),
            "exception": decision.get("exception"),
            "allocation_multiplier": decision.get("allocation_multiplier"),
            "symbol_cap_factor": decision.get("symbol_cap_factor"),
            "total_cap_factor": decision.get("total_cap_factor"),
            "reasons": list(decision.get("reasons") or []),
        }

    @staticmethod
    def _normalize_asset_type(value: Any) -> str:
        key = str(value or "unknown").strip().lower().replace("-", "_").replace(" ", "_")
        aliases = {
            "individual_stock": "individual_stock",
            "individualstock": "individual_stock",
            "stock": "individual_stock",
            "equity": "individual_stock",
            "common_stock": "individual_stock",
            "theme_etf": "theme_etf",
            "themeetf": "theme_etf",
            "theme_fund": "theme_etf",
            "sector_etf": "theme_etf",
            "industry_etf": "theme_etf",
            "concept_etf": "theme_etf",
            "broad_etf": "broad_etf",
            "broadetf": "broad_etf",
            "broad_index_etf": "broad_etf",
            "index_etf": "broad_etf",
            "index_fund": "broad_etf",
            "unknown": "unknown",
        }
        return aliases.get(key, "unknown")

    @classmethod
    def _normalize_asset_exposure_caps(cls, caps: Optional[Mapping[str, Any]]) -> Dict[str, float]:
        normalized = dict(DEFAULT_ASSET_EXPOSURE_CAPS)
        for key, value in dict(caps or {}).items():
            asset_type = cls._normalize_asset_type(key)
            try:
                cap = float(value)
            except (TypeError, ValueError):
                continue
            normalized[asset_type] = max(0.0, cap)
        return normalized

    def _asset_override_for_identity(self, market: str, stock_code: str) -> Optional[str]:
        key = f"{str(market or '').strip().lower()}:{str(stock_code or '').strip().lower()}"
        return self.asset_class_overrides.get(key)

    @staticmethod
    def _is_cn_etf_identity(market: str, stock_code: str) -> bool:
        code = str(stock_code or "").strip().lower()
        if "." in code:
            code = code.split(".", 1)[0]
        code = re.sub(r"^(sh|sz|bj)", "", code)
        return str(market or "").strip().lower() == "cn" and code.startswith(("15", "16", "18", "50", "51", "52", "56", "58"))

    @staticmethod
    def _is_cn_stock_identity(market: str, stock_code: str) -> bool:
        code = str(stock_code or "").strip().lower()
        if "." in code:
            code = code.split(".", 1)[0]
        code = re.sub(r"^(sh|sz|bj)", "", code)
        return str(market or "").strip().lower() == "cn" and code.startswith(("0", "3", "4", "6", "8", "9"))

    def _asset_type_for_identity(self, session, market: str, stock_code: str) -> Tuple[str, str]:
        """Classify asset exposure without using report prose or LLM free text."""

        identity = canonical_paper_identity(market, stock_code)
        canonical_market, canonical_code = identity if identity is not None else (str(market or "").lower(), str(stock_code or ""))
        override = self._asset_override_for_identity(canonical_market, canonical_code)
        if override:
            return override, "explicit_override"
        metadata_type = self._asset_type_from_structured_metadata(session, canonical_market, canonical_code)
        if metadata_type:
            return metadata_type, "structured_metadata"
        if self._is_cn_etf_identity(canonical_market, canonical_code):
            return "unknown", "cn_etf_without_verified_class"
        if self._is_cn_stock_identity(canonical_market, canonical_code):
            return "individual_stock", "cn_stock_code_rule"
        return "individual_stock", "default_non_cn_equity"

    def _asset_type_from_structured_metadata(self, session, market: str, stock_code: str) -> Optional[str]:
        rows = list(session.execute(
            select(AnalysisHistory)
            .where(
                AnalysisHistory.code.in_(stock_daily_variants(market, stock_code) + [stock_code]),
                authorized_report_execution_clause(AnalysisHistory.id, execution_scope=self.execution_scope),
            )
            .order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id))
            .limit(20)
        ).scalars().all())
        for row in rows:
            payload, error = self._json_object(getattr(row, "context_snapshot", None) or "{}")
            if error or not payload:
                continue
            asset_type = self._extract_structured_asset_type(payload)
            if asset_type:
                return asset_type
        return None

    def _extract_structured_asset_type(self, payload: Mapping[str, Any]) -> Optional[str]:
        candidates: List[Any] = []
        for parent_key in ("provider_context", "identity", "symbol_identity", "fundamental_context", "valuation"):
            parent = payload.get(parent_key)
            if not isinstance(parent, Mapping):
                continue
            for key in (
                "asset_type",
                "asset_class",
                "instrument_type",
                "security_type",
                "fund_type",
                "index_type",
                "tracking_type",
            ):
                if key in parent:
                    candidates.append(parent.get(key))
        for value in candidates:
            asset_type = self._normalize_asset_type(value)
            if asset_type != "unknown":
                return asset_type
        return None

    def _symbol_exposure_snapshot(
        self,
        session,
        account: PaperCapitalAccount,
        *,
        source_model: str,
        market: str,
        stock_code: str,
        projected_additional_cost: float = 0.0,
        symbol_cap_factor: float = 1.0,
    ) -> Dict[str, Any]:
        asset_type, asset_source = self._asset_type_for_identity(session, market, stock_code)
        cap_pct = float(self.asset_exposure_caps.get(asset_type, self.asset_exposure_caps.get("unknown", 20.0)))
        base_effective_cap_pct = min(self.max_symbol_exposure_pct, cap_pct)
        # The experiment red-light trial scales the frozen symbol cap once; the
        # primary book always passes factor 1.0 so its ceiling is unchanged.
        factor = max(0.0, float(symbol_cap_factor if symbol_cap_factor is not None else 1.0))
        effective_cap_pct = base_effective_cap_pct * factor
        rows = list(session.execute(
            select(PaperPosition)
            .where(
                PaperPosition.capital_account_id == account.id,
                PaperPosition.source_model == source_model,
                PaperPosition.market == market,
                PaperPosition.stock_code == stock_code,
                PaperPosition.status == "open",
            )
            .order_by(PaperPosition.id)
        ).scalars().all())
        current = sum(self._position_remaining_cost(row) for row in rows)
        projected = current + max(0.0, float(projected_additional_cost or 0.0))
        book_equity = self._book_equity(account)
        ceiling = book_equity * (effective_cap_pct / 100.0)
        return {
            "policy_version": ASSET_EXPOSURE_POLICY_VERSION,
            "asset_type": asset_type,
            "asset_type_source": asset_source,
            "asset_exposure_scope": "identity",
            "asset_exposure_caps": dict(self.asset_exposure_caps),
            "asset_exposure_cap_pct": cap_pct,
            "asset_exposure_effective_cap_pct": effective_cap_pct,
            "asset_exposure_cap_source": "min_global_and_asset_class",
            "symbol_cap_factor": factor,
            "asset_class_current_exposure": current,
            "asset_class_projected_exposure": projected,
            "current_symbol_exposure": current,
            "projected_symbol_exposure": projected,
            "symbol_exposure_ceiling": ceiling,
            "max_symbol_exposure_pct": self.max_symbol_exposure_pct,
            "book_equity": book_equity,
            "contributing_position_ids": [row.id for row in rows],
            "within_limit": projected <= ceiling + 1e-9,
        }

    def _portfolio_risk_capacity_context(
        self,
        account: Optional[PaperCapitalAccount],
        exposure_snapshot: Optional[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        if account is None or not isinstance(exposure_snapshot, Mapping):
            return {}
        book_equity = self._book_equity(account)
        current_symbol_exposure = float(exposure_snapshot.get("current_symbol_exposure") or 0.0)
        symbol_ceiling = self._safe_float(exposure_snapshot.get("symbol_exposure_ceiling"))
        asset_room = (
            max(0.0, symbol_ceiling - current_symbol_exposure)
            if symbol_ceiling is not None else None
        )
        total_ceiling = book_equity * (float(account.max_total_exposure_pct or 0.0) / 100.0)
        current_total_exposure = float(account.exposure or 0.0)
        position_ceiling = book_equity * (float(account.max_position_pct or 0.0) / 100.0)
        risk_budget = book_equity * (float(account.risk_per_trade_pct or 0.0) / 100.0)
        sizing_context = {
            "cash": float(account.cash or 0.0),
            "book_equity": book_equity,
            "account_exposure": current_total_exposure,
            "risk_per_trade_pct": float(account.risk_per_trade_pct or 0.0),
            "risk_budget": risk_budget,
            "max_position_pct": float(account.max_position_pct or 0.0),
            "plan_position_ceiling": position_ceiling,
            "max_total_exposure_pct": float(account.max_total_exposure_pct or 0.0),
            "total_exposure_ceiling": total_ceiling,
            "total_exposure_remaining_capacity": max(0.0, total_ceiling - current_total_exposure),
            "asset_type": exposure_snapshot.get("asset_type"),
            "asset_type_source": exposure_snapshot.get("asset_type_source"),
            "asset_exposure_cap_pct": exposure_snapshot.get("asset_exposure_cap_pct"),
            "asset_exposure_effective_cap_pct": exposure_snapshot.get("asset_exposure_effective_cap_pct"),
            "asset_exposure_ceiling": symbol_ceiling,
            "asset_exposure_current": current_symbol_exposure,
            "asset_exposure_remaining_capacity": asset_room,
            "sizing_note": "read_only_capacity_context_no_trade_authorization",
        }
        return {
            "asset_exposure_ceiling": symbol_ceiling,
            "asset_exposure_current": current_symbol_exposure,
            "asset_exposure_remaining_capacity": asset_room,
            "sizing_context": sizing_context,
        }

    def _fill_assumption(
        self,
        *,
        side: str,
        quote_price: float,
        quantity: float,
        market: Optional[str] = None,
        stock_code: Optional[str] = None,
        quote: Optional[Any] = None,
        observed_at: Optional[datetime] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if not self._positive_finite(quote_price):
            return None, "invalid_fill_quote_price"
        if not self._positive_finite(quantity):
            return None, "invalid_fill_quantity"
        side_norm = str(side or "").lower()
        if side_norm not in {"buy", "sell"}:
            return None, "invalid_fill_side"
        if getattr(quote, "is_halted", False) or getattr(quote, "halted", False):
            return None, "quote_indicates_halt"
        if getattr(quote, "executable", True) is False:
            return None, "quote_indicates_non_executable"
        liquidity = getattr(quote, "liquidity", None)
        if liquidity is not None and str(liquidity).lower() in {"none", "zero", "no_liquidity"}:
            return None, "quote_indicates_no_liquidity"
        if getattr(quote, "no_liquidity", False):
            return None, "quote_indicates_no_liquidity"
        limit_state = str(getattr(quote, "limit_state", "") or "").lower()
        if side_norm == "buy" and limit_state in {"limit_up", "up_limit", "buy_locked"}:
            return None, "quote_indicates_buy_limit_blocked"
        if side_norm == "sell" and limit_state in {"limit_down", "down_limit", "sell_locked"}:
            return None, "quote_indicates_sell_limit_blocked"
        observed_naive = self._utc_naive(observed_at) if observed_at is not None else utc_naive_now()
        fetched_at = getattr(quote, "fetched_at", None)
        provider_timestamp = getattr(quote, "provider_timestamp", None)
        quote_stale_seconds = None
        timestamp_value = fetched_at or provider_timestamp
        if timestamp_value:
            try:
                timestamp_dt = datetime.fromisoformat(str(timestamp_value))
                if timestamp_dt.tzinfo is not None and timestamp_dt.utcoffset() is not None:
                    timestamp_dt = timestamp_dt.astimezone(timezone.utc).replace(tzinfo=None)
                quote_stale_seconds = max(0.0, (observed_naive - timestamp_dt.replace(tzinfo=None)).total_seconds())
            except (TypeError, ValueError):
                quote_stale_seconds = None
        if quote_stale_seconds is not None and quote_stale_seconds > self.intraday_max_quote_stale_seconds:
            return None, "quote_stale_for_fill"
        slippage = self.slippage_pct / 100.0
        price = float(quote_price) * (1.0 + slippage if side_norm == "buy" else 1.0 - slippage)
        gross = price * float(quantity)
        commission = max(self.min_commission, gross * self.commission_rate) if gross > 0 else 0.0
        instrument_type = self._instrument_type_for_cost(market, stock_code)
        sell_tax_rate = self._sell_tax_rate_for_instrument(side_norm, market, stock_code)
        sell_tax = gross * sell_tax_rate if side_norm == "sell" else 0.0
        slippage_amount = abs(price - float(quote_price)) * float(quantity)
        fees = commission + sell_tax
        net = gross + fees if side_norm == "buy" else gross - fees
        evidence_grade = "quote_based_unverified"
        if getattr(quote, "fill_evidence_grade", None):
            evidence_grade = str(getattr(quote, "fill_evidence_grade"))[:32]
        return {
            "side": side_norm,
            "quote_price": float(quote_price),
            "fill_price": float(price),
            "quantity": float(quantity),
            "requested_quantity": float(quantity),
            "executed_quantity": float(quantity),
            "unfilled_quantity": 0.0,
            "gross_amount": float(gross),
            "commission": float(commission),
            "sell_tax": float(sell_tax),
            "tax": float(sell_tax),
            "slippage_amount": float(slippage_amount),
            "fees": float(fees),
            "net_amount": float(net),
            "slippage_pct": self.slippage_pct,
            "commission_rate": self.commission_rate,
            "min_commission": self.min_commission,
            "sell_tax_rate": sell_tax_rate,
            "configured_cn_stock_sell_tax_rate": self.sell_tax_rate,
            "instrument_type": instrument_type,
            "cost_policy": {
                "version": "paper-cost-v2",
                "market": str(market or "").lower() or None,
                "stock_code": stock_code,
                "instrument_type": instrument_type,
                "commission_rate": self.commission_rate,
                "min_commission": self.min_commission,
                "slippage_pct": self.slippage_pct,
                "sell_tax_rate": sell_tax_rate,
                "cn_stock_sell_stamp_tax_rate": self.sell_tax_rate,
                "cn_etf_fund_sell_stamp_tax_rate": 0.0,
            },
            "evidence_grade": evidence_grade,
            "instrument_assumption": "paper_market_order_with_configured_slippage",
            "quote_source": self._quote_source(quote),
            "quote_provider_timestamp": str(provider_timestamp or "")[:64] or None,
            "quote_fetched_at": str(fetched_at or "")[:64] or None,
            "quote_stale_seconds": quote_stale_seconds,
            "quote_is_stale": bool(
                quote_stale_seconds is not None
                and quote_stale_seconds > self.intraday_max_quote_stale_seconds
            ),
            "order_book_fields_available": bool(getattr(quote, "bid_price", None) or getattr(quote, "ask_price", None)),
        }, None

    def _position_initial_quantity(self, position: PaperPosition) -> float:
        value = self._safe_float(getattr(position, "initial_quantity", None))
        return value if value is not None and value > 0 else float(position.quantity or 0.0)

    def _position_initial_cost(self, position: PaperPosition) -> float:
        value = self._safe_float(getattr(position, "initial_allocated_capital", None))
        if value is not None and value > 0:
            return value
        return self._position_allocated_capital(position)

    def _position_remaining_cost(self, position: PaperPosition) -> float:
        value = self._safe_float(getattr(position, "remaining_cost", None))
        if value is not None and value >= 0:
            return value
        initial_qty = self._position_initial_quantity(position)
        if initial_qty <= 0:
            return 0.0
        return self._position_initial_cost(position) * max(0.0, float(position.quantity or 0.0)) / initial_qty

    def _close_quantity_for_reason(
        self,
        position: PaperPosition,
        reason: str,
        *,
        requested_fraction: Optional[float] = None,
    ) -> float:
        remaining = float(position.quantity or 0.0)
        if remaining <= 0:
            return 0.0
        reason_norm = str(reason or "").lower()
        if "reduce" in reason_norm or requested_fraction is not None:
            fraction = self.default_reduce_fraction if requested_fraction is None else requested_fraction
            fraction = min(1.0, max(0.0, float(fraction or 0.0)))
            raw = self._position_initial_quantity(position) * fraction
            lot_size = max(1, int(getattr(position, "lot_size", None) or self._lot_size_for_market(position.market)))
            quantity = math.floor(raw / lot_size) * lot_size
            return float(min(remaining, quantity))
        return remaining

    def _exit_leg_quantity(self, position: PaperPosition, leg: Optional[Mapping[str, Any]]) -> float:
        remaining = float(getattr(position, "quantity", 0.0) or 0.0)
        if remaining <= 0 or not isinstance(leg, Mapping):
            return 0.0
        role = str(leg.get("role") or "").lower()
        if role in {"final", "final_take_profit", "take_profit"}:
            return remaining
        fraction = self._safe_float(leg.get("allocation_fraction"))
        if fraction is None:
            return 0.0
        lot_size = max(1, int(getattr(position, "lot_size", None) or self._lot_size_for_market(position.market)))
        quantity = math.floor((self._position_initial_quantity(position) * min(1.0, max(0.0, fraction))) / lot_size) * lot_size
        return float(min(remaining, quantity))

    def _apply_position_exit(
        self,
        session,
        account: Optional[PaperCapitalAccount],
        position: PaperPosition,
        *,
        quote_price: float,
        reason: str,
        event_date: date,
        quote: Optional[Any] = None,
        requested_fraction: Optional[float] = None,
        requested_quantity: Optional[float] = None,
        fill_override: Optional[Dict[str, Any]] = None,
        observed_at: Optional[datetime] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if getattr(position, "pending_exit_reason", None) and float(getattr(position, "frozen_quantity", 0.0) or 0.0) > 0:
            quantity = min(float(position.quantity or 0.0), float(position.frozen_quantity or 0.0))
        elif requested_quantity is not None:
            quantity = min(float(position.quantity or 0.0), max(0.0, float(requested_quantity or 0.0)))
        else:
            quantity = self._close_quantity_for_reason(position, reason, requested_fraction=requested_fraction)
        if quantity <= 0:
            return None, "no_sellable_quantity_for_exit"
        raw_available = getattr(position, "available_quantity", None)
        sellable_quantity = float(quantity if raw_available is None else (raw_available or 0.0))
        if self._can_close_position_on_date(position, event_date):
            sellable_quantity = max(sellable_quantity, float(position.quantity or 0.0))
        if getattr(position, "pending_exit_reason", None):
            sellable_quantity += float(getattr(position, "frozen_quantity", 0.0) or 0.0)
        if sellable_quantity < quantity - 1e-9:
            return None, "insufficient_available_quantity_for_exit"
        if fill_override is not None:
            fill = dict(fill_override)
            fill_error = None
        else:
            fill, fill_error = self._fill_assumption(
                side="sell",
                quote_price=quote_price,
                quantity=quantity,
                market=position.market,
                stock_code=position.stock_code,
                quote=quote,
                observed_at=observed_at,
            )
            if fill_error:
                return None, fill_error
            assert fill is not None
        if abs(float(fill.get("quantity") or 0.0) - quantity) > 1e-9:
            return None, "fill_quantity_mismatch"
        remaining_before = self._position_remaining_cost(position)
        qty_before = float(position.quantity or 0.0)
        cost_reduced = remaining_before if quantity >= qty_before - 1e-9 else remaining_before * quantity / qty_before
        realized_gross = fill["gross_amount"] - cost_reduced
        realized_net = fill["net_amount"] - cost_reduced
        remaining_qty = max(0.0, qty_before - quantity)
        remaining_cost = max(0.0, remaining_before - cost_reduced)
        position.quantity = remaining_qty
        position.remaining_cost = remaining_cost
        position.allocated_capital = remaining_cost
        position.realized_quantity = float(getattr(position, "realized_quantity", 0.0) or 0.0) + quantity
        position.realized_proceeds = float(getattr(position, "realized_proceeds", 0.0) or 0.0) + fill["gross_amount"]
        position.realized_fees = float(getattr(position, "realized_fees", 0.0) or 0.0) + fill["fees"]
        position.realized_pnl = float(getattr(position, "realized_pnl", 0.0) or 0.0) + realized_net
        position.simulated_pnl = float(position.realized_pnl or 0.0)
        position.simulated_return_pct = (
            (float(position.realized_pnl or 0.0) / self._position_initial_cost(position)) * 100.0
            if self._position_initial_cost(position) > 0 else None
        )
        if getattr(position, "pending_exit_reason", None):
            frozen_before = float(getattr(position, "frozen_quantity", 0.0) or 0.0)
            consumed_frozen = min(frozen_before, quantity)
            position.frozen_quantity = max(0.0, frozen_before - consumed_frozen)
            position.available_quantity = max(0.0, remaining_qty - float(position.frozen_quantity or 0.0))
        else:
            position.available_quantity = max(0.0, float(getattr(position, "available_quantity", qty_before) or 0.0) - quantity)
            position.frozen_quantity = max(0.0, float(getattr(position, "frozen_quantity", 0.0) or 0.0))
        if remaining_qty <= 1e-9:
            position.status = "closed"
            position.closed_on = event_date
            position.closed_price = fill["fill_price"]
            position.close_reason = reason
            position.available_quantity = 0.0
            position.frozen_quantity = 0.0
            position.pending_exit_reason = None
        elif getattr(position, "pending_exit_reason", None) and float(position.frozen_quantity or 0.0) <= 1e-9:
            position.pending_exit_reason = None
            position.pending_exit_on = None
            position.pending_exit_price = None
            position.pending_exit_event_id = None
        position.updated_at = utc_naive_now()
        if account is not None:
            account.cash = float(account.cash or 0.0) + fill["net_amount"]
            account.invested_capital = max(0.0, float(account.invested_capital or 0.0) - cost_reduced)
            account.exposure = max(0.0, float(account.exposure or 0.0) - cost_reduced)
            account.realized_pnl = float(account.realized_pnl or 0.0) + realized_net
            if position.status == "closed":
                # Recount instead of decrementing: sibling lots closed earlier in
                # this session are not flushed yet (autoflush is off), so a
                # "another open lot exists?" query would see them as open.
                self._sync_open_position_count_in_session(session, account)
            account.updated_at = utc_naive_now()
        return {
            **fill,
            "cost_reduced": cost_reduced,
            "realized_gross_pnl": realized_gross,
            "realized_pnl": realized_net,
            "remaining_quantity": remaining_qty,
            "remaining_cost": remaining_cost,
            "exit_fraction": quantity / qty_before if qty_before > 0 else None,
            "final_close": position.status == "closed",
        }, None

    def _count_open_holdings_in_session(self, session, account_id: int) -> int:
        """Distinct open holding identities (model, market, code) of one account."""

        session.flush()
        rows = session.execute(
            select(PaperPosition.source_model, PaperPosition.market, PaperPosition.stock_code)
            .where(
                PaperPosition.capital_account_id == int(account_id),
                PaperPosition.status == "open",
            )
            .distinct()
        ).all()
        return len(rows)

    def _sync_open_position_count_in_session(self, session, account: Optional[PaperCapitalAccount]) -> None:
        """Set open_position_count from the open lots; the counter is derived, never authoritative."""

        if account is None or getattr(account, "id", None) is None:
            return
        account.open_position_count = self._count_open_holdings_in_session(session, account.id)

    def _reconcile_open_position_counts(self) -> int:
        """Repair drifted open_position_count values before the EOD cycle gates new entries."""

        repaired = 0
        with self._ledger_write_session() as session:
            accounts = list(session.execute(select(PaperCapitalAccount)).scalars().all())
            for account in accounts:
                actual = self._count_open_holdings_in_session(session, account.id)
                stored = int(account.open_position_count or 0)
                if stored != actual:
                    account.open_position_count = actual
                    account.updated_at = utc_naive_now()
                    repaired += 1
        return repaired

    def _debit_account_for_open(
        self,
        account: PaperCapitalAccount,
        sizing: Dict[str, Any],
        *,
        count_new_holding: bool = True,
    ) -> None:
        allocated = float(sizing["allocated_capital"])
        account.cash = float(account.cash or 0.0) - allocated
        account.invested_capital = float(account.invested_capital or 0.0) + allocated
        account.exposure = float(account.exposure or 0.0) + allocated
        if count_new_holding:
            account.open_position_count = int(account.open_position_count or 0) + 1
        account.updated_at = utc_naive_now()

    def _credit_account_for_close(self, session, account: Optional[PaperCapitalAccount], position: PaperPosition, close_price: float) -> None:
        if account is None:
            return
        allocated = self._position_allocated_capital(position)
        proceeds = float(position.quantity or 0.0) * close_price
        pnl = proceeds - allocated
        account.cash = float(account.cash or 0.0) + proceeds
        account.invested_capital = max(0.0, float(account.invested_capital or 0.0) - allocated)
        account.exposure = max(0.0, float(account.exposure or 0.0) - allocated)
        other_same_holding = session.execute(
            select(PaperPosition.id)
            .where(
                PaperPosition.capital_account_id == account.id,
                PaperPosition.source_model == position.source_model,
                PaperPosition.market == position.market,
                PaperPosition.stock_code == position.stock_code,
                PaperPosition.status == "open",
                PaperPosition.id != position.id,
            )
            .limit(1)
        ).scalar_one_or_none() is not None
        if not other_same_holding:
            account.open_position_count = max(0, int(account.open_position_count or 0) - 1)
        account.realized_pnl = float(account.realized_pnl or 0.0) + pnl
        account.updated_at = utc_naive_now()

    def _position_allocated_capital(self, position: PaperPosition) -> float:
        remaining = self._safe_float(getattr(position, "remaining_cost", None))
        if remaining is not None and remaining >= 0:
            return remaining
        allocated = self._safe_float(getattr(position, "allocated_capital", None))
        if allocated is not None and allocated > 0:
            return allocated
        return float(position.opened_price or 0.0) * float(position.quantity or 0.0)

    @contextmanager
    def _ledger_write_session(self):
        """Open a ledger mutation transaction with an early SQLite writer lock."""

        session = self.db.get_session()
        try:
            if getattr(self.db, "_is_sqlite_engine", False):
                session.connection().exec_driver_sql("BEGIN IMMEDIATE")
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _ensure_open_dispatch(self, session, event_row: PaperTradeEvent, *, account: PaperCapitalAccount, **_context) -> Optional[PaperNotificationDispatch]:
        return self._ensure_lifecycle_dispatch(session, event_row, account=account, lifecycle="open")

    def _ensure_lifecycle_dispatch(
        self,
        session,
        event_row: PaperTradeEvent,
        *,
        account: Optional[PaperCapitalAccount],
        lifecycle: Optional[str] = None,
    ) -> Optional[PaperNotificationDispatch]:
        lifecycle_key = str(lifecycle or event_row.event_type or "event").strip().lower()
        dispatch_key = f"paper-{lifecycle_key}:event:{event_row.id}:feishu"
        existing = session.execute(select(PaperNotificationDispatch).where(
            PaperNotificationDispatch.dispatch_key == dispatch_key
        )).scalar_one_or_none()
        if existing is not None:
            return existing
        payload = self._build_feishu_lifecycle_payload(event_row, account=account, lifecycle=lifecycle_key)
        status = "pending"
        if not self.feishu_notify:
            status = "disabled"
        elif not self._feishu_route_configured():
            status = "unconfigured"
        dispatch = PaperNotificationDispatch(
            dispatch_key=dispatch_key,
            event_id=event_row.id,
            channel="feishu",
            status=status,
            attempt_count=0,
            payload_json=self._json_dumps(payload),
        )
        session.add(dispatch)
        session.flush()
        return dispatch

    def _build_feishu_lifecycle_payload(
        self,
        event_row: PaperTradeEvent,
        *,
        account: Optional[PaperCapitalAccount],
        lifecycle: str,
    ) -> Dict[str, Any]:
        details = json.loads(event_row.details_json or "{}")
        quantity = details.get("quantity", "") or ""
        allocated = float(event_row.allocated_capital or 0.0)
        cash_after = float(event_row.cash_after or 0.0)
        exposure_after = float(event_row.exposure_after or 0.0)
        evidence_summary = details.get("evidence_summary", details.get("fill_policy", "见 paper ledger"))
        if not isinstance(evidence_summary, str):
            evidence_summary = self._json_dumps(evidence_summary)
        followup_route = f"/paper-trading?model={event_row.source_model}"
        lifecycle_label = {
            "open": "模拟开仓",
            "pending_exit": "T+1待平仓",
            "close": "模拟平仓",
        }.get(lifecycle, lifecycle)
        content = (
            "【模拟交易提醒】仅纸面模拟，不连接券商、不代表真实下单\n"
            f"生命周期: {lifecycle_label}\n"
            f"模型: {event_row.source_model}\n"
            f"市场/币种: {event_row.market}/{getattr(account, 'currency', None) or event_row.currency}\n"
            f"股票: {event_row.stock_name or ''} {event_row.stock_code}\n"
            f"来源报告: {event_row.source_report_id}\n"
            f"时间(中国): {self._format_china_time(event_row.created_at) or event_row.event_date.isoformat()}\n"
            f"价格: {event_row.price:.4f}\n"
            f"数量: {quantity}\n"
            f"原因: {event_row.reason}\n"
            f"分配资金: {allocated:.2f}\n"
            f"剩余现金: {cash_after:.2f}\n"
            f"账户敞口: {exposure_after:.2f}\n"
            f"止损/止盈: {details.get('stop_loss')} / {details.get('target_price')}\n"
            f"后续确认: {followup_route}\n"
            f"证据: {evidence_summary}"
        )
        return {
            "content": content,
            "event_id": event_row.id,
            "event_key": event_row.event_key,
            "account_id": getattr(account, "id", None),
            "lifecycle": lifecycle,
            "followup_route": followup_route,
            "details": details,
        }

    def dispatch_pending_notifications(self, *, sender: Optional[Any] = None) -> Dict[str, int]:
        """Send retryable Feishu dispatches after ledger transactions commit."""

        stats = {"sent": 0, "failed": 0, "skipped": 0}
        if not self.feishu_notify:
            return stats
        if sender is None:
            from src.config import get_config
            from src.notification_sender.feishu_sender import FeishuSender

            sender = FeishuSender(get_config())
        rows = self.repo.list_retryable_dispatches(limit=500, max_attempts=3)
        for row in rows:
            if row.channel != "feishu":
                stats["skipped"] += 1
                continue
            claimed = self.repo.claim_dispatch_for_send(row.id, max_attempts=3)
            if claimed is None:
                stats["skipped"] += 1
                continue
            payload = json.loads(claimed.payload_json or "{}")
            content = str(payload.get("content") or "")
            ok = False
            error = None
            try:
                ok = bool(sender.send_to_feishu(content))
            except Exception as exc:  # pragma: no cover - defensive network guard
                error = f"{type(exc).__name__}: {exc}"
            with self.db.session_scope() as session:
                current = session.get(PaperNotificationDispatch, row.id)
                if current is None or current.status != "sending":
                    continue
                current.updated_at = utc_naive_now()
                if ok:
                    current.status = "sent"
                    current.sent_at = utc_naive_now()
                    current.last_error = None
                    stats["sent"] += 1
                else:
                    current.status = "failed"
                    current.last_error = error or "feishu_send_failed"
                    stats["failed"] += 1
        risk_stats = self.dispatch_pending_risk_notifications(sender=sender)
        return {
            "sent": stats["sent"] + risk_stats["sent"],
            "failed": stats["failed"] + risk_stats["failed"],
            "skipped": stats["skipped"] + risk_stats["skipped"],
        }

    def dispatch_pending_risk_notifications(self, *, sender: Optional[Any] = None) -> Dict[str, int]:
        """Send retryable Feishu notifications for account-risk transitions."""

        stats = {"sent": 0, "failed": 0, "skipped": 0}
        if not self.feishu_notify:
            return stats
        if sender is None:
            from src.config import get_config
            from src.notification_sender.feishu_sender import FeishuSender

            sender = FeishuSender(get_config())
        with self.db.session_scope() as session:
            rows = list(session.execute(
                select(PaperAccountRiskEvent)
                .where(PaperAccountRiskEvent.notification_status.in_(["pending", "failed"]))
                .order_by(PaperAccountRiskEvent.created_at, PaperAccountRiskEvent.id)
                .limit(500)
            ).scalars().all())
            for row in rows:
                row.notification_status = "sending"
            session.flush()
            payloads = [self._build_feishu_risk_event_payload(row) for row in rows]
        for payload in payloads:
            row_id = int(payload["risk_event_id"])
            ok = False
            error = None
            try:
                ok = bool(sender.send_to_feishu(str(payload.get("content") or "")))
            except Exception as exc:  # pragma: no cover - defensive network guard
                error = f"{type(exc).__name__}: {exc}"
            with self.db.session_scope() as session:
                current = session.get(PaperAccountRiskEvent, row_id)
                if current is None or current.notification_status != "sending":
                    stats["skipped"] += 1
                    continue
                if ok:
                    current.notification_status = "sent"
                    current.notification_error = None
                    stats["sent"] += 1
                else:
                    current.notification_status = "failed"
                    current.notification_error = error or "feishu_send_failed"
                    stats["failed"] += 1
        return stats

    def _build_feishu_risk_event_payload(self, event_row: PaperAccountRiskEvent) -> Dict[str, Any]:
        reasons, _ = self._json_list(event_row.active_reasons_json or "[]")
        recovery, _ = self._json_object(event_row.recovery_condition_json or "{}")
        evidence, _ = self._json_object(event_row.evidence_json or "{}")
        content = (
            "【模拟交易风控提醒】仅纸面模拟，不连接券商、不代表真实下单\n"
            f"事件: {event_row.event_type}\n"
            f"模型: {event_row.source_model}\n"
            f"市场/币种: {event_row.market}/{event_row.currency}\n"
            f"账户: {event_row.capital_account_id}\n"
            f"状态: {event_row.old_state or '-'} -> {event_row.new_state}\n"
            f"原因: {', '.join(str(item) for item in reasons) or '-'}\n"
            f"指标/阈值: {event_row.metric or '-'} / {event_row.threshold if event_row.threshold is not None else '-'}\n"
            f"恢复条件: {self._json_dumps(recovery) if recovery else '-'}\n"
            f"关联决议: {event_row.resolution_key or '-'}\n"
            f"时间(中国): {self._format_china_time(event_row.created_at) or '-'}\n"
            f"后续确认: /paper-trading?model={event_row.source_model}\n"
            f"证据: {self._json_dumps(evidence) if evidence else '-'}"
        )
        return {
            "content": content,
            "risk_event_id": event_row.id,
            "event_key": event_row.event_key,
            "account_id": event_row.capital_account_id,
            "followup_route": f"/paper-trading?model={event_row.source_model}",
        }

    def _feishu_route_configured(self) -> bool:
        try:
            from src.config import get_config

            config = get_config()
        except Exception:
            return False
        return bool(
            getattr(config, "feishu_webhook_url", None)
            or (
                getattr(config, "feishu_app_id", None)
                and getattr(config, "feishu_app_secret", None)
                and getattr(config, "feishu_chat_id", None)
            )
        )

    def _build_feishu_open_payload(self, event_row: PaperTradeEvent, *, account: PaperCapitalAccount) -> Dict[str, Any]:
        details = json.loads(event_row.details_json or "{}")
        quantity = details.get("quantity", "") or ""
        allocated = float(event_row.allocated_capital or 0.0)
        cash_after = float(event_row.cash_after or 0.0)
        exposure_after = float(event_row.exposure_after or 0.0)
        evidence_summary = details.get("evidence_summary", "见 paper ledger")
        if not isinstance(evidence_summary, str):
            evidence_summary = self._json_dumps(evidence_summary)
        sync_status = details.get("sync_status") or "paper_only"
        followup_route = f"/paper-trading?model={event_row.source_model}"
        content = (
            "【模拟交易提醒】仅纸面模拟，不连接券商、不代表真实下单\n"
            f"模型: {event_row.source_model}\n"
            f"市场/币种: {event_row.market}/{account.currency}\n"
            f"股票: {event_row.stock_name or ''} {event_row.stock_code}\n"
            f"来源报告: {event_row.source_report_id}\n"
            f"成交时间(中国): {self._format_china_time(event_row.created_at) or event_row.event_date.isoformat()}\n"
            f"模拟买入价: {event_row.price:.4f}\n"
            f"数量: {quantity}\n"
            f"分配资金: {allocated:.2f}\n"
            f"剩余现金: {cash_after:.2f}\n"
            f"账户敞口: {exposure_after:.2f}\n"
            f"止损/止盈: {details.get('stop_loss')} / {details.get('target_price')}\n"
            f"风险收益比: {details.get('risk_reward')}\n"
            f"同步状态: {sync_status}\n"
            f"后续确认: {followup_route}\n"
            f"证据: {evidence_summary}"
        )
        return {
            "content": content,
            "event_id": event_row.id,
            "event_key": event_row.event_key,
            "account_id": account.id,
            "sync_status": sync_status,
            "followup_route": followup_route,
            "details": details,
        }

    @staticmethod
    def _format_china_time(value: Any) -> Optional[str]:
        if not isinstance(value, datetime):
            return None
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        china_tz = timezone(timedelta(hours=8))
        return dt.astimezone(china_tz).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _as_date(value):
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value)
        raise TypeError("date value is required")

    @staticmethod
    def _json_dumps(value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)

    @staticmethod
    def _json_value(value):
        try:
            return json.loads(value), None
        except (TypeError, ValueError) as exc:
            return None, str(exc)

    @classmethod
    def _json_object(cls, value):
        parsed, error = cls._json_value(value or "{}")
        if error or not isinstance(parsed, dict):
            return {}, error or "report payload is not an object"
        return parsed, None

    @classmethod
    def _json_list(cls, value):
        parsed, error = cls._json_value(value)
        if error or not isinstance(parsed, list):
            return [], error or "rules payload is not a list"
        return parsed, None

    @staticmethod
    def _holding_action_snapshot(metadata: Mapping[str, Any]) -> Dict[str, Any]:
        position_actions = metadata.get("position_actions") if isinstance(metadata, Mapping) else None
        if isinstance(position_actions, Mapping):
            holding = position_actions.get("holding")
            if isinstance(holding, Mapping):
                return dict(holding)
        holding = metadata.get("holding_action") if isinstance(metadata, Mapping) else None
        if isinstance(holding, Mapping):
            return dict(holding)
        if isinstance(holding, str):
            return {"action": holding}
        return {}

    def _serialize_plans_with_runtime(self, rows, *, session=None) -> List[Dict[str, Any]]:
        plans = list(rows or [])
        if not plans:
            return []
        own_session = None
        if session is None:
            own_session = self.db.get_session()
            session = own_session.__enter__()
        try:
            plan_ids = [int(row.id) for row in plans]
            positions = list(session.execute(
                select(PaperPosition)
                .where(PaperPosition.plan_id.in_(plan_ids))
                .order_by(PaperPosition.plan_id, PaperPosition.id)
            ).scalars().all())
            requests = list(session.execute(
                select(PaperReanalysisRequest)
                .where(PaperReanalysisRequest.source_plan_id.in_(plan_ids))
                .order_by(PaperReanalysisRequest.source_plan_id, desc(PaperReanalysisRequest.created_at), desc(PaperReanalysisRequest.id))
            ).scalars().all())
            position_ids = [int(position.id) for position in positions]
            revision_rows = list(session.execute(
                select(PaperRiskRevision)
                .where(PaperRiskRevision.position_id.in_(position_ids))
                .order_by(
                    PaperRiskRevision.position_id,
                    desc(PaperRiskRevision.revision_version),
                    desc(PaperRiskRevision.id),
                )
            ).scalars().all()) if position_ids else []
            positions_by_plan: Dict[int, List[PaperPosition]] = {}
            for position in positions:
                positions_by_plan.setdefault(int(position.plan_id), []).append(position)
            requests_by_plan: Dict[int, List[PaperReanalysisRequest]] = {}
            for request in requests:
                requests_by_plan.setdefault(int(request.source_plan_id), []).append(request)
            latest_revision_by_position: Dict[int, PaperRiskRevision] = {}
            for revision in revision_rows:
                latest_revision_by_position.setdefault(int(revision.position_id), revision)
            regime_ids = {
                int(value)
                for value in [
                    *(getattr(plan, "market_regime_context_id", None) for plan in plans),
                    *(getattr(position, "market_regime_context_id", None) for position in positions),
                ]
                if value is not None
            }
            market_regimes: Dict[int, DailyMarketRegime] = {}
            if regime_ids:
                market_regimes = {
                    int(row.id): row
                    for row in session.execute(
                        select(DailyMarketRegime).where(DailyMarketRegime.id.in_(regime_ids))
                    ).scalars().all()
                }
            source_kinds = self._report_source_kinds_in_session(session, [
                *(getattr(plan, "source_report_id", None) for plan in plans),
                *(getattr(position, "source_report_id", None) for position in positions),
            ])
            held_sessions = self._held_sessions_by_position(
                [position for position in positions if position.status == "open"]
            )
            items = [
                self._serialize_plan_runtime(
                    row,
                    positions=positions_by_plan.get(int(row.id), []),
                    requests=requests_by_plan.get(int(row.id), []),
                    latest_revision_by_position=latest_revision_by_position,
                    market_regimes=market_regimes,
                )
                for row in plans
            ]
            for item in items:
                item["source_kind"] = self._source_kind_for_report(source_kinds, item.get("source_report_id"))
                for lot in item.get("open_lots") or []:
                    lot["source_kind"] = self._source_kind_for_report(source_kinds, lot.get("source_report_id"))
                    lot["held_sessions"] = held_sessions.get(int(lot["id"]))
            return items
        finally:
            if own_session is not None:
                own_session.__exit__(None, None, None)

    def _report_source_kinds_in_session(self, session, report_ids) -> Dict[int, str]:
        """Batch-map report id -> source kind; reports without an intent row are absent."""

        ids = sorted({int(value) for value in report_ids if value is not None})
        if not ids:
            return {}
        rows = session.execute(
            select(
                AnalysisReportExecutionIntent.analysis_history_id,
                AnalysisReportExecutionIntent.intent_source,
            ).where(AnalysisReportExecutionIntent.analysis_history_id.in_(ids))
        ).all()
        return {
            int(report_id): REPORT_SOURCE_KIND_BY_INTENT_SOURCE.get(
                str(intent_source or "").strip(), REPORT_SOURCE_KIND_DEFAULT,
            )
            for report_id, intent_source in rows
        }

    @staticmethod
    def _source_kind_for_report(source_kinds: Mapping[int, str], report_id) -> Optional[str]:
        if report_id is None:
            return None
        return source_kinds.get(int(report_id))

    def _held_sessions_by_position(self, positions) -> Dict[int, Optional[int]]:
        """Exchange sessions held per lot, on the same basis as the EOD time exit.

        Counts sessions after ``opened_on`` up to the market-local today for open
        lots (``closed_on`` for closed lots).  Reaching ``max_holding_sessions``
        means the time exit fires at that session's close.  Lots whose market
        calendar is unavailable or out of range stay ``None``; natural days are
        never substituted.
        """

        held: Dict[int, Optional[int]] = {}
        lots_by_market: Dict[str, List[Tuple[int, date, date]]] = {}
        today_by_market: Dict[str, date] = {}
        now = utc_naive_now()
        for position in positions:
            position_id = int(position.id)
            held[position_id] = None
            opened_on = getattr(position, "opened_on", None)
            market = str(getattr(position, "market", None) or "").lower()
            if not isinstance(opened_on, date) or not market:
                continue
            if position.status == "open":
                if market not in today_by_market:
                    today_by_market[market] = self._market_local_date_for_time(market, now)
                end = today_by_market[market]
            else:
                end = getattr(position, "closed_on", None)
            if not isinstance(end, date):
                continue
            lots_by_market.setdefault(market, []).append((position_id, opened_on, end))
        for market, lots in lots_by_market.items():
            calendar_parts = _market_context_calendar(market)
            if calendar_parts is None:
                continue
            calendar = calendar_parts[0]
            try:
                first_session = calendar.first_session.date()
                last_session = calendar.last_session.date()
                range_start = max(min(opened_on for _, opened_on, _ in lots) + timedelta(days=1), first_session)
                range_end = min(max(end for _, _, end in lots), last_session)
                sessions = (
                    [ts.date() for ts in calendar.sessions_in_range(range_start, range_end)]
                    if range_start <= range_end else []
                )
            except Exception:
                continue
            for position_id, opened_on, end in lots:
                if opened_on < first_session or end > last_session:
                    continue
                held[position_id] = max(0, bisect_right(sessions, end) - bisect_right(sessions, opened_on))
        return held

    def _serialize_plan_runtime(
        self,
        row,
        *,
        positions: Sequence[PaperPosition],
        requests: Sequence[PaperReanalysisRequest],
        latest_revision_by_position: Optional[Mapping[int, PaperRiskRevision]] = None,
        market_regimes: Optional[Mapping[int, DailyMarketRegime]] = None,
    ) -> Dict[str, Any]:
        market_regimes = market_regimes or {}
        item = self._serialize_plan(
            row,
            market_regime_context=market_regimes.get(int(row.market_regime_context_id))
            if getattr(row, "market_regime_context_id", None) is not None
            else None,
        )
        open_positions = [position for position in positions if position.status == "open"]
        pending_positions = [
            position for position in open_positions
            if getattr(position, "pending_exit_reason", None)
        ]
        entry_legs = self._plan_entry_legs(row)
        exit_contract = self._plan_exit_contract(row)
        exit_legs = list((exit_contract or {}).get("exit_legs") or [])
        executed_leg_counts: Dict[str, int] = {}
        for position in positions:
            if position.status not in {"open", "closed"}:
                continue
            leg_id = str(getattr(position, "entry_leg_id", None) or "legacy_entry")
            executed_leg_counts[leg_id] = executed_leg_counts.get(leg_id, 0) + 1
        executed_exit_leg_ids = set()
        if exit_legs and positions:
            with self.db.get_session() as session:
                for position in positions:
                    executed_exit_leg_ids.update(self._executed_exit_leg_ids_for_position(session, int(position.id)))
        executed_leg_ids = sorted(executed_leg_counts)
        all_leg_ids = [str(leg.get("leg_id") or "") for leg in entry_legs if leg.get("leg_id")]
        pending_leg_ids = [
            str(leg.get("leg_id"))
            for leg in entry_legs
            if leg.get("leg_id")
            and executed_leg_counts.get(str(leg.get("leg_id")), 0)
            < max(1, int(leg.get("max_executions") or 1))
        ]
        all_exit_leg_ids = [str(leg.get("leg_id") or "") for leg in exit_legs if leg.get("leg_id")]
        pending_exit_leg_ids = [
            leg_id for leg_id in all_exit_leg_ids if leg_id not in executed_exit_leg_ids
        ]
        unresolved_requests = [
            req for req in requests
            if req.status in {"pending", "claimed", "submitted"}
        ]
        revisions = latest_revision_by_position or {}
        effective_risk_lots = []
        for position in open_positions:
            revision = revisions.get(int(position.id))
            effective_risk_lots.append({
                "position_id": int(position.id),
                "quantity": float(position.quantity or 0.0),
                "source_plan_stop_loss": row.stop_loss,
                "source_plan_target_price": row.target_price,
                "effective_stop_loss": position.stop_loss,
                "effective_target_price": position.target_price,
                "revision_id": revision.id if revision is not None else None,
                "revision_version": revision.revision_version if revision is not None else None,
                "revision_source_report_id": revision.source_report_id if revision is not None else None,
                "revision_decision_signal_id": revision.decision_signal_id if revision is not None else None,
                "revision_result": revision.result if revision is not None else None,
                "revision_reason": revision.reason if revision is not None else None,
                "revision_evaluated_at": (
                    revision.evaluated_at.isoformat()
                    if revision is not None and revision.evaluated_at is not None else None
                ),
            })
        if pending_positions:
            stage = "pending_exit"
        elif unresolved_requests:
            stage = "reanalysis_blocked"
        elif open_positions and pending_leg_ids:
            stage = "entry_and_position_monitoring"
        elif open_positions:
            stage = "position_monitoring"
        elif row.status == "active" and row.executable:
            stage = "waiting_entry"
        else:
            stage = "history"
        is_operational = stage != "history"
        item.update({
            "entry_leg_schema_version": ENTRY_LEG_SCHEMA_VERSION,
            "entry_legs": entry_legs,
            "exit_leg_schema_version": (exit_contract or {}).get("schema_version"),
            "exit_legs": exit_legs,
            "weighted_target_price": (exit_contract or {}).get("weighted_target_price"),
            "weighted_risk_reward": (exit_contract or {}).get("weighted_risk_reward"),
            "executed_exit_leg_ids": sorted(executed_exit_leg_ids),
            "pending_exit_leg_ids": pending_exit_leg_ids,
            "next_check_obligation": self._next_check_obligation_from_plan(row),
            "reanalysis_requests": [self._serialize_reanalysis_request(req) for req in requests],
            "operational_state": stage,
            "stage": stage,
            "is_operational": is_operational,
            "lot_count": len(open_positions),
            "open_lot_count": len(open_positions),
            "pending_exit_count": len(pending_positions),
            "position_ids": [position.id for position in open_positions],
            "entry_leg_ids": all_leg_ids,
            "executed_entry_leg_ids": executed_leg_ids,
            "executed_entry_leg_counts": executed_leg_counts,
            "pending_entry_leg_ids": pending_leg_ids,
            "reanalysis_request_ids": [req.id for req in unresolved_requests],
            "effective_risk_lots": effective_risk_lots,
            "effective_stop_loss_values": sorted({
                float(position.stop_loss)
                for position in open_positions
                if position.stop_loss is not None
            }),
            "effective_target_price_values": sorted({
                float(position.target_price)
                for position in open_positions
                if position.target_price is not None
            }),
            "latest_risk_revision_ids": sorted({
                int(revisions[int(position.id)].id)
                for position in open_positions
                if int(position.id) in revisions
            }),
            "open_lots": [
                self._serialize_position(
                    position,
                    source_plan=row,
                    latest_risk_revision=revisions.get(int(position.id)),
                    market_regime_context=market_regimes.get(int(position.market_regime_context_id))
                    if getattr(position, "market_regime_context_id", None) is not None
                    else None,
                )
                for position in open_positions
            ],
        })
        return item

    def _next_check_obligation_from_plan(self, row) -> Optional[Dict[str, Any]]:
        descriptions, _ = self._json_list(getattr(row, "descriptive_conditions_json", "[]"))
        for item in descriptions:
            if isinstance(item, Mapping) and item.get("source") == "paper_trading.next_check_obligation":
                obligation = item.get("obligation")
                return dict(obligation) if isinstance(obligation, Mapping) else None
        return None

    @classmethod
    def _serialize_report_plan_reconciliation(cls, row, *, report=None) -> Dict[str, Any]:
        details, _ = cls._json_object(getattr(row, "details_json", None) or "{}")
        report_snapshot = None
        if report is not None:
            report_snapshot = {
                "id": report.id,
                "code": report.code,
                "name": report.name,
                "report_type": report.report_type,
                "model_used": report.model_used,
                "created_at": report.created_at.isoformat() if report.created_at else None,
            }
        return {
            "id": row.id,
            "reconciliation_key": row.reconciliation_key,
            "source_report_id": row.source_report_id,
            "report": report_snapshot,
            "plan_version": row.plan_version,
            "source_model": row.source_model,
            "status": row.status,
            "classification": row.classification,
            "attempt_count": row.attempt_count,
            "max_attempts": row.max_attempts,
            "next_retry_at": row.next_retry_at.isoformat() if row.next_retry_at else None,
            "last_attempt_at": row.last_attempt_at.isoformat() if row.last_attempt_at else None,
            "last_error": row.last_error,
            "decision_signal_id": row.decision_signal_id,
            "paper_plan_id": row.paper_plan_id,
            "details": details,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @classmethod
    def _serialize_reanalysis_request(cls, row) -> Dict[str, Any]:
        evidence, _ = cls._json_object(row.evidence_json)
        return {
            "id": row.id,
            "request_key": row.request_key,
            "trigger_type": row.trigger_type,
            "status": row.status,
            "source_plan_id": row.source_plan_id,
            "source_report_id": row.source_report_id,
            "decision_signal_id": row.decision_signal_id,
            "source_model": row.source_model,
            "market": row.market,
            "stock_code": row.stock_code,
            "stock_name": row.stock_name,
            "session_date": row.session_date.isoformat() if row.session_date else None,
            "due_at": row.due_at.isoformat() if row.due_at else None,
            "observed_at": row.observed_at.isoformat() if row.observed_at else None,
            "claimed_at": row.claimed_at.isoformat() if row.claimed_at else None,
            "submitted_at": row.submitted_at.isoformat() if row.submitted_at else None,
            "completed_at": row.completed_at.isoformat() if row.completed_at else None,
            "task_id": row.task_id,
            "query_id": row.query_id,
            "replacement_report_id": row.replacement_report_id,
            "replacement_plan_id": row.replacement_plan_id,
            "failure_reason": row.failure_reason,
            "evidence": evidence,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @classmethod
    def _serialize_plan(cls, row, *, market_regime_context=None):
        rules, _ = cls._json_list(row.rules_json)
        descriptions, _ = cls._json_list(row.descriptive_conditions_json)
        style_evidence, _ = cls._json_object(getattr(row, "trading_style_settings_json", None) or "{}")
        exit_contract = None
        strategy_thesis_transition = None
        trading_style = None
        trading_style_settings = None
        for item in descriptions:
            if isinstance(item, Mapping) and item.get("source") == "paper_trading.exit_legs":
                exit_contract = item
            if isinstance(item, Mapping) and item.get("source") == "paper_trading.strategy_thesis":
                transition = item.get("transition")
                if isinstance(transition, Mapping):
                    strategy_thesis_transition = dict(transition)
            if isinstance(item, Mapping) and item.get("source") == "paper_trading.trading_style":
                trading_style = item.get("trading_style")
                settings = item.get("settings")
                trading_style_settings = dict(settings) if isinstance(settings, Mapping) else None
        if not trading_style:
            trading_style = getattr(row, "trading_style", None)
        if trading_style_settings is None and isinstance(style_evidence.get("settings"), Mapping):
            trading_style_settings = dict(style_evidence.get("settings"))
        indicator_snapshot = (
            style_evidence.get("indicator_snapshot")
            if isinstance(style_evidence.get("indicator_snapshot"), Mapping)
            else None
        )
        style_history_evidence = None
        if indicator_snapshot is not None or "bars_observed" in style_evidence:
            style_history_evidence = {
                "analysis_window_sessions": style_evidence.get("analysis_window_sessions"),
                "bars_observed": style_evidence.get("bars_observed"),
                "history_sufficient": style_evidence.get("history_sufficient"),
                "indicator_snapshot": dict(indicator_snapshot) if indicator_snapshot is not None else None,
            }
        market_regime_snapshot = cls._serialize_market_regime_context(market_regime_context)
        strategy_evidence = (
            strategy_thesis_transition.get("evidence")
            if isinstance(strategy_thesis_transition, Mapping)
            and isinstance(strategy_thesis_transition.get("evidence"), Mapping)
            else {}
        )
        strategy_result = (
            strategy_thesis_transition.get("result")
            if isinstance(strategy_thesis_transition, Mapping) else None
        )
        strategy_raw_action = (
            strategy_thesis_transition.get("raw_action")
            if isinstance(strategy_thesis_transition, Mapping) else None
        )
        strategy_effective_action = (
            strategy_raw_action
            if strategy_result == "accepted"
            else strategy_evidence.get("previous_action")
        )
        strategy_confirmation_required = strategy_evidence.get("effective_confirmation_required")
        if strategy_confirmation_required is None and isinstance(strategy_thesis_transition, Mapping):
            if strategy_thesis_transition.get("reason") == "awaiting_defensive_confirmation":
                strategy_confirmation_required = strategy_evidence.get("flip_confirmation_required")
        return {
            "id": row.id, "plan_version": row.plan_version, "status": row.status,
            "executable": bool(row.executable), "source_report_id": row.source_report_id,
            "decision_signal_id": row.decision_signal_id, "source_model": row.source_model,
            "market": row.market, "stock_code": row.stock_code, "stock_name": row.stock_name,
            "report_date": row.report_date.isoformat(), "action": row.action, "rules": rules,
            "valid_for_date": row.valid_for_date.isoformat() if getattr(row, "valid_for_date", None) else None,
            "descriptive_conditions": descriptions, "stop_loss": row.stop_loss,
            "exit_leg_schema_version": exit_contract.get("schema_version") if exit_contract else None,
            "exit_legs": exit_contract.get("exit_legs") if exit_contract else [],
            "weighted_target_price": exit_contract.get("weighted_target_price") if exit_contract else None,
            "weighted_risk_reward": exit_contract.get("weighted_risk_reward") if exit_contract else None,
            "trading_style": trading_style,
            "trading_style_policy_version": getattr(row, "trading_style_policy_version", None),
            "trading_style_settings": trading_style_settings,
            "trading_style_evidence": style_evidence or None,
            "style_history_evidence": style_history_evidence,
            "strategy_thesis_transition": strategy_thesis_transition,
            "strategy_thesis_state": (
                strategy_thesis_transition.get("new_state")
                if isinstance(strategy_thesis_transition, Mapping) else None
            ),
            "strategy_thesis_result": (
                strategy_thesis_transition.get("result")
                if isinstance(strategy_thesis_transition, Mapping) else None
            ),
            "strategy_thesis_reason": (
                strategy_thesis_transition.get("reason")
                if isinstance(strategy_thesis_transition, Mapping) else None
            ),
            "strategy_thesis_plan_authorized": (
                bool(strategy_thesis_transition.get("plan_authorized"))
                if isinstance(strategy_thesis_transition, Mapping) else None
            ),
            "strategy_thesis_risk_authorized": (
                bool(strategy_thesis_transition.get("risk_revision_authorized"))
                if isinstance(strategy_thesis_transition, Mapping) else None
            ),
            # These fields are derived server-side from the persisted transition
            # audit.  The client must not guess which LLM opinion currently owns
            # execution authority.
            "strategy_thesis_raw_action": strategy_raw_action,
            "strategy_thesis_effective_action": strategy_effective_action,
            "strategy_thesis_entry_permitted": (
                bool(strategy_thesis_transition.get("plan_authorized"))
                if isinstance(strategy_thesis_transition, Mapping) else None
            ),
            "strategy_thesis_confirmation_count": (
                strategy_thesis_transition.get("confirmation_count")
                if isinstance(strategy_thesis_transition, Mapping) else None
            ),
            "strategy_thesis_confirmation_required": strategy_confirmation_required,
            "strategy_thesis_confirmation_session_dates": strategy_evidence.get("confirmation_session_dates"),
            "strategy_thesis_contradiction_session_dates": strategy_evidence.get("contradiction_session_dates"),
            "market_regime_context_id": getattr(row, "market_regime_context_id", None),
            "market_regime": getattr(row, "market_regime", None),
            "market_regime_policy_version": getattr(row, "market_regime_policy_version", None),
            "market_regime_context": market_regime_snapshot,
            "target_price": row.target_price, "max_holding_sessions": row.max_holding_sessions,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @classmethod
    def _serialize_market_regime_context(cls, row) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        provenance, _ = cls._json_object(getattr(row, "provenance_json", None) or "{}")
        return {
            "id": row.id,
            "region": row.region,
            "session_date": row.session_date.isoformat() if row.session_date else None,
            "regime": row.regime,
            "allocation_multiplier": row.allocation_multiplier,
            "entry_allowed": row.entry_allowed,
            "policy_version": row.policy_version,
            "source_report_id": row.source_report_id,
            "source_query_id": row.source_query_id,
            "freshness_expires_at": row.freshness_expires_at.isoformat() if row.freshness_expires_at else None,
            "provenance": provenance,
        }

    @staticmethod
    def _serialize_observation(row):
        evidence = json.loads(row.evidence_json)
        quote = evidence.get("quote") if isinstance(evidence.get("quote"), Mapping) else {}
        confirmation = (
            evidence.get("opening_confirmation")
            if isinstance(evidence.get("opening_confirmation"), Mapping)
            else {}
        )
        phase = evidence.get("phase") if isinstance(evidence.get("phase"), Mapping) else {}
        auction = evidence.get("auction") if isinstance(evidence.get("auction"), Mapping) else {}
        reanalysis = (
            evidence.get("reanalysis_request")
            if isinstance(evidence.get("reanalysis_request"), Mapping)
            else {}
        )
        fail_reason = evidence.get("fail_closed_reason") or evidence.get("validation_error")
        identity_failures = {
            "invalid_plan_symbol_identity",
            "missing_realtime_quote_identity",
            "realtime_quote_identity_mismatch",
            "missing_realtime_quote_source",
            "auction_snapshot_identity_mismatch",
        }
        has_attributable_market_evidence = bool(
            quote.get("available") is True or auction.get("available") is True
        )
        return {
            "id": row.id,
            "plan_id": row.plan_id,
            "position_id": row.position_id,
            "source_report_id": row.source_report_id,
            "decision_signal_id": row.decision_signal_id,
            "source_model": row.source_model,
            "market": row.market,
            "stock_code": row.stock_code,
            "stock_name": row.stock_name,
            "mode": evidence.get("mode") or row.mode,
            "phase": phase.get("phase"),
            "observed_at": row.observed_at.isoformat(),
            "bucket_started_at": row.bucket_started_at.isoformat(),
            "quote_price": row.quote_price,
            "quote_source": row.quote_source,
            "quote_provider_timestamp": row.quote_provider_timestamp,
            "quote_fetched_at": row.quote_fetched_at,
            "provider_timestamp": row.quote_provider_timestamp or quote.get("provider_timestamp"),
            "fetched_at": row.quote_fetched_at or quote.get("fetched_at"),
            "source": row.quote_source or quote.get("source"),
            "identity_verified": (
                False if fail_reason in identity_failures
                else True if has_attributable_market_evidence
                else None
            ),
            "stale": (
                True if "stale" in str(fail_reason or "") or quote.get("is_stale") is True
                else False if has_attributable_market_evidence
                else None
            ),
            "indicative_price": auction.get("indicative_price") or auction.get("virtual_price"),
            "virtual_price": confirmation.get("observed_price") or auction.get("virtual_price"),
            "prior_close": confirmation.get("prior_close") or auction.get("prior_close"),
            "entry_low": confirmation.get("entry_low"),
            "entry_high": confirmation.get("entry_high"),
            "gap_pct": confirmation.get("gap_pct") or auction.get("gap_pct"),
            "matched_volume": auction.get("matched_volume"),
            "matched_volume_ratio": auction.get("matched_volume_ratio"),
            "unmatched_volume": auction.get("unmatched_volume"),
            "unmatched_side": auction.get("unmatched_side"),
            "imbalance_pct": auction.get("imbalance_pct"),
            "limit_proximity_pct": auction.get("limit_proximity_pct"),
            "opening_volume_ratio": confirmation.get("volume_ratio"),
            "material_change": confirmation.get("material"),
            "fail_closed_reason": fail_reason,
            "reanalysis_request_id": reanalysis.get("id"),
            "resolution_key": getattr(row, "resolution_key", None),
            "resolution_bucket": json.loads(row.resolution_bucket_json) if getattr(row, "resolution_bucket_json", None) else None,
            "logical_order_key": getattr(row, "logical_order_key", None),
            "result": row.result,
            "action": row.action,
            "composite_score": row.composite_score,
            "required_passed": bool(row.required_passed),
            "factors_passed": row.factors_passed,
            "factors_available": row.factors_available,
            "evidence": evidence,
        }

    @staticmethod
    def _serialize_resolution(row):
        return {
            "id": row.id,
            "resolution_key": row.resolution_key,
            "policy_version": row.policy_version,
            "mode": row.mode,
            "status": row.status,
            "capital_account_id": row.capital_account_id,
            "source_model": row.source_model,
            "market": row.market,
            "stock_code": row.stock_code,
            "currency": row.currency,
            "bucket_started_at": row.bucket_started_at.isoformat() if row.bucket_started_at else None,
            "evaluation_date": row.evaluation_date.isoformat() if row.evaluation_date else None,
            "source_report_id": row.source_report_id,
            "decision_signal_id": row.decision_signal_id,
            "selected_intent": row.selected_intent,
            "requested_quantity": row.requested_quantity,
            "executed_quantity": row.executed_quantity,
            "unfilled_quantity": row.unfilled_quantity,
            "candidate_intents": json.loads(row.candidate_intents_json or "[]"),
            "suppressed_intents": json.loads(row.suppressed_intents_json or "[]"),
            "chosen_intent": json.loads(row.selected_intent_json or "{}"),
            "source_identities": json.loads(row.source_identities_json or "[]"),
            "evidence": json.loads(row.evidence_json or "{}"),
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @staticmethod
    def _serialize_risk_revision(row):
        return {
            "id": row.id,
            "revision_key": row.revision_key,
            "policy_version": row.policy_version,
            "position_id": row.position_id,
            "plan_id": row.plan_id,
            "source_report_id": row.source_report_id,
            "decision_signal_id": row.decision_signal_id,
            "source_model": row.source_model,
            "market": row.market,
            "stock_code": row.stock_code,
            "revision_version": row.revision_version,
            "result": row.result,
            "reason": row.reason,
            "old_stop_loss": row.old_stop_loss,
            "new_stop_loss": row.new_stop_loss,
            "applied_stop_loss": row.applied_stop_loss,
            "old_target_price": row.old_target_price,
            "new_target_price": row.new_target_price,
            "applied_target_price": row.applied_target_price,
            "evaluated_at": row.evaluated_at.isoformat() if row.evaluated_at else None,
            "evidence": json.loads(row.evidence_json or "{}"),
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _serialize_account_risk_snapshot(row):
        return {
            "id": row.id,
            "snapshot_key": row.snapshot_key,
            "policy_version": row.policy_version,
            "capital_account_id": row.capital_account_id,
            "source_model": row.source_model,
            "market": row.market,
            "currency": row.currency,
            "session_date": row.session_date.isoformat() if row.session_date else None,
            "bucket_started_at": row.bucket_started_at.isoformat() if row.bucket_started_at else None,
            "observed_at": row.observed_at.isoformat() if row.observed_at else None,
            "session_start_equity": row.session_start_equity,
            "current_equity": row.current_equity,
            "peak_equity": row.peak_equity,
            "cash": row.cash,
            "exposure": row.exposure,
            "realized_pnl": row.realized_pnl,
            "unrealized_pnl": row.unrealized_pnl,
            "total_pnl": row.total_pnl,
            "daily_pnl": row.daily_pnl,
            "daily_loss_pct": row.daily_loss_pct,
            "drawdown_pct": row.drawdown_pct,
            "losing_logical_exit_streak": row.losing_logical_exit_streak,
            "daily_trade_count": row.daily_trade_count,
            "daily_open_add_count": row.daily_open_add_count,
            "daily_reduce_close_count": row.daily_reduce_close_count,
            "valuation_complete": bool(row.valuation_complete),
            "valuation_stale": bool(row.valuation_stale),
            "total_utilization_pct": row.total_utilization_pct,
            "symbol_utilization_pct": row.symbol_utilization_pct,
            "sector_utilization_pct": row.sector_utilization_pct,
            "correlation_utilization_pct": row.correlation_utilization_pct,
            "unknown_utilization_pct": row.unknown_utilization_pct,
            "active_state": row.active_state,
            "active_reasons": json.loads(row.active_reasons_json or "[]"),
            "permissions": json.loads(row.permissions_json or "{}"),
            "concentration": json.loads(row.concentration_json or "{}"),
            "evidence": json.loads(row.evidence_json or "{}"),
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @staticmethod
    def _serialize_account_risk_event(row):
        return {
            "id": row.id,
            "event_key": row.event_key,
            "idempotency_key": row.idempotency_key,
            "event_type": row.event_type,
            "capital_account_id": row.capital_account_id,
            "snapshot_id": row.snapshot_id,
            "resolution_key": row.resolution_key,
            "logical_order_key": row.logical_order_key,
            "source_model": row.source_model,
            "market": row.market,
            "currency": row.currency,
            "old_state": row.old_state,
            "new_state": row.new_state,
            "active_reasons": json.loads(row.active_reasons_json or "[]"),
            "metric": row.metric,
            "threshold": row.threshold,
            "source": row.source,
            "recovery_condition": json.loads(row.recovery_condition_json or "{}"),
            "evidence": json.loads(row.evidence_json or "{}"),
            "notification_status": row.notification_status,
            "notification_error": row.notification_error,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _serialize_current_account_risk_lock(row):
        permissions = json.loads(row.permissions_json or "{}")
        reasons = json.loads(row.active_reasons_json or "[]")
        return {
            "capital_account_id": row.capital_account_id,
            "snapshot_id": row.id,
            "snapshot_key": row.snapshot_key,
            "policy_version": row.policy_version,
            "source_model": row.source_model,
            "market": row.market,
            "currency": row.currency,
            "session_date": row.session_date.isoformat() if row.session_date else None,
            "bucket_started_at": row.bucket_started_at.isoformat() if row.bucket_started_at else None,
            "observed_at": row.observed_at.isoformat() if row.observed_at else None,
            "active_state": row.active_state,
            "active_reasons": reasons,
            "permissions": permissions,
            "open_enabled": bool(permissions.get("open")),
            "add_enabled": bool(permissions.get("add")),
            "exits_enabled": bool(
                permissions.get("reduce", True)
                and permissions.get("close", True)
                and permissions.get("hard_stop", True)
                and permissions.get("t1_pending_exit", True)
            ),
            "valuation_complete": bool(row.valuation_complete),
            "valuation_stale": bool(row.valuation_stale),
            "daily_loss_pct": row.daily_loss_pct,
            "drawdown_pct": row.drawdown_pct,
            "losing_logical_exit_streak": row.losing_logical_exit_streak,
            "daily_trade_count": row.daily_trade_count,
            "daily_open_add_count": row.daily_open_add_count,
            "concentration": json.loads(row.concentration_json or "{}"),
            "evidence": json.loads(row.evidence_json or "{}"),
        }

    @staticmethod
    def _serialize_evaluation(row, *, plan=None):
        item = {
            "id": row.id, "evaluation_key": getattr(row, "evaluation_key", None),
            "plan_id": row.plan_id, "position_id": getattr(row, "position_id", None),
            "entry_leg_id": getattr(row, "entry_leg_id", None) or "legacy_entry",
            "evaluation_date": row.evaluation_date.isoformat(),
            "result": row.result, "reasons": json.loads(row.reasons_json),
            "evidence": json.loads(row.evidence_json),
        }
        if plan is not None:
            item.update({
                "source_model": plan.source_model,
                "stock_code": plan.stock_code,
                "stock_name": plan.stock_name,
            })
        return item

    @classmethod
    def _serialize_position(cls, row, *, source_plan=None, latest_risk_revision=None, market_regime_context=None):
        style_evidence, _ = cls._json_object(getattr(row, "trading_style_settings_json", None) or "{}")
        indicator_snapshot = (
            style_evidence.get("indicator_snapshot")
            if isinstance(style_evidence.get("indicator_snapshot"), Mapping)
            else None
        )
        style_history_evidence = None
        if indicator_snapshot is not None or "bars_observed" in style_evidence:
            style_history_evidence = {
                "analysis_window_sessions": style_evidence.get("analysis_window_sessions"),
                "bars_observed": style_evidence.get("bars_observed"),
                "history_sufficient": style_evidence.get("history_sufficient"),
                "indicator_snapshot": dict(indicator_snapshot) if indicator_snapshot is not None else None,
            }
        item = {
            "id": row.id, "plan_id": row.plan_id, "source_report_id": row.source_report_id,
            "entry_leg_id": getattr(row, "entry_leg_id", None) or "legacy_entry",
            "execution_index": getattr(row, "execution_index", None) or 1,
            "decision_signal_id": row.decision_signal_id, "source_model": row.source_model,
            "market": row.market, "stock_code": row.stock_code, "stock_name": row.stock_name,
            "status": row.status, "opened_on": row.opened_on.isoformat(),
            "opened_at": row.opened_at.isoformat() if getattr(row, "opened_at", None) else None,
            "opened_price": row.opened_price,
            "normalized_notional": row.normalized_notional, "quantity": row.quantity,
            "initial_quantity": getattr(row, "initial_quantity", None),
            "initial_allocated_capital": getattr(row, "initial_allocated_capital", None),
            "remaining_cost": getattr(row, "remaining_cost", None),
            "realized_quantity": getattr(row, "realized_quantity", None),
            "realized_proceeds": getattr(row, "realized_proceeds", None),
            "realized_fees": getattr(row, "realized_fees", None),
            "realized_pnl": getattr(row, "realized_pnl", None),
            "available_quantity": getattr(row, "available_quantity", None),
            "frozen_quantity": getattr(row, "frozen_quantity", None),
            "capital_account_id": getattr(row, "capital_account_id", None),
            "currency": getattr(row, "currency", None),
            "allocated_capital": getattr(row, "allocated_capital", None) or row.normalized_notional,
            "lot_size": getattr(row, "lot_size", None),
            "risk_amount": getattr(row, "risk_amount", None),
            "risk_per_share": getattr(row, "risk_per_share", None),
            "sizing_reason": getattr(row, "sizing_reason", None),
            "legacy_fixed_notional": bool(getattr(row, "legacy_fixed_notional", False) or not getattr(row, "capital_account_id", None)),
            "sync_origin": getattr(row, "sync_origin", None) or "paper_generated",
            "sync_status": getattr(row, "sync_status", None) or "paper_only",
            "source_portfolio_position_id": getattr(row, "source_portfolio_position_id", None),
            "last_synced_at": row.last_synced_at.isoformat() if getattr(row, "last_synced_at", None) else None,
            "pending_exit_reason": getattr(row, "pending_exit_reason", None),
            "pending_exit_on": row.pending_exit_on.isoformat() if getattr(row, "pending_exit_on", None) else None,
            "pending_exit_price": getattr(row, "pending_exit_price", None),
            "pending_exit_event_id": getattr(row, "pending_exit_event_id", None),
            "sellability_rule": getattr(row, "sellability_rule", None),
            "trading_style": getattr(row, "trading_style", None),
            "trading_style_policy_version": getattr(row, "trading_style_policy_version", None),
            "trading_style_settings": (
                dict(style_evidence.get("settings"))
                if isinstance(style_evidence.get("settings"), Mapping)
                else None
            ),
            "trading_style_evidence": style_evidence or None,
            "style_history_evidence": style_history_evidence,
            "market_regime_context_id": getattr(row, "market_regime_context_id", None),
            "market_regime": getattr(row, "market_regime", None),
            "market_regime_policy_version": getattr(row, "market_regime_policy_version", None),
            "market_regime_context": cls._serialize_market_regime_context(market_regime_context),
            "stop_loss": row.stop_loss, "target_price": row.target_price,
            "max_holding_sessions": row.max_holding_sessions,
            "closed_on": row.closed_on.isoformat() if row.closed_on else None,
            "closed_price": row.closed_price, "close_reason": row.close_reason,
            "close_signal_id": row.close_signal_id, "simulated_return_pct": row.simulated_return_pct,
            "simulated_pnl": row.simulated_pnl,
        }
        item.update({
            "source_plan_stop_loss": getattr(source_plan, "stop_loss", None),
            "source_plan_target_price": getattr(source_plan, "target_price", None),
            "effective_stop_loss": row.stop_loss,
            "effective_target_price": row.target_price,
            "latest_risk_revision": (
                cls._serialize_risk_revision(latest_risk_revision)
                if latest_risk_revision is not None else None
            ),
        })
        return item

    @staticmethod
    def _serialize_event(row, *, notification_dispatch=None):
        return {
            "id": row.id, "plan_id": row.plan_id, "position_id": row.position_id,
            "source_report_id": row.source_report_id, "decision_signal_id": row.decision_signal_id,
            "source_model": row.source_model, "market": row.market, "stock_code": row.stock_code,
            "stock_name": row.stock_name, "event_type": row.event_type,
            "event_date": row.event_date.isoformat(), "price": row.price, "reason": row.reason,
            "quantity": getattr(row, "quantity", None),
            "requested_quantity": getattr(row, "requested_quantity", None),
            "executed_quantity": getattr(row, "executed_quantity", None),
            "unfilled_quantity": getattr(row, "unfilled_quantity", None),
            "gross_amount": getattr(row, "gross_amount", None),
            "fees": getattr(row, "fees", None),
            "commission": getattr(row, "commission", None),
            "tax": getattr(row, "tax", None),
            "slippage_amount": getattr(row, "slippage_amount", None),
            "gross_realized_pnl": getattr(row, "gross_realized_pnl", None),
            "net_realized_pnl": getattr(row, "net_realized_pnl", None),
            "net_amount": getattr(row, "net_amount", None),
            "fill_price": getattr(row, "fill_price", None),
            "fill_evidence_grade": getattr(row, "fill_evidence_grade", None),
            "resolution_key": getattr(row, "resolution_key", None),
            "logical_order_key": getattr(row, "logical_order_key", None),
            "instrument_assumption": getattr(row, "instrument_assumption", None),
            "quote_provider_timestamp": getattr(row, "quote_provider_timestamp", None),
            "quote_fetched_at": getattr(row, "quote_fetched_at", None),
            "triggering_signal_id": row.triggering_signal_id, "details": json.loads(row.details_json),
            "capital_account_id": getattr(row, "capital_account_id", None),
            "currency": getattr(row, "currency", None),
            "allocated_capital": getattr(row, "allocated_capital", None),
            "cash_after": getattr(row, "cash_after", None),
            "exposure_after": getattr(row, "exposure_after", None),
            "realized_pnl_after": getattr(row, "realized_pnl_after", None),
            "notification_dispatch": (
                PaperTradingService._serialize_dispatch(notification_dispatch)
                if notification_dispatch is not None else None
            ),
        }

    @staticmethod
    def _serialize_account(row, *, valuation: Optional[Dict[str, Any]] = None):
        book_equity = PaperTradingService._book_equity(row)
        max_total_exposure = book_equity * (row.max_total_exposure_pct / 100.0)
        max_position_capital = book_equity * (row.max_position_pct / 100.0)
        valuation = valuation or {
            "market_value": 0.0 if int(row.open_position_count or 0) == 0 else None,
            "priced_market_value": 0.0 if int(row.open_position_count or 0) == 0 else None,
            "unrealized_pnl": 0.0 if int(row.open_position_count or 0) == 0 else None,
            "total_pnl": float(row.realized_pnl or 0.0) if int(row.open_position_count or 0) == 0 else None,
            "market_equity": float(row.cash or 0.0) if int(row.open_position_count or 0) == 0 else None,
            "valuation_complete": int(row.open_position_count or 0) == 0,
            "unpriced_position_count": 0,
            "unpriced_holding_count": 0,
            "valuation_observed_at": None,
            "valuation_stale": False if int(row.open_position_count or 0) == 0 else None,
        }
        return {
            "id": row.id,
            "account_key": row.account_key,
            "source_model": row.source_model,
            "market": row.market,
            "currency": row.currency,
            "initial_capital": row.initial_capital,
            "external_cash_flow": getattr(row, "external_cash_flow", 0.0) or 0.0,
            "book_equity": book_equity,
            "cash": row.cash,
            "invested_capital": row.invested_capital,
            "realized_pnl": row.realized_pnl,
            "market_value": valuation.get("market_value"),
            "priced_market_value": valuation.get("priced_market_value"),
            "unrealized_pnl": valuation.get("unrealized_pnl"),
            "total_pnl": valuation.get("total_pnl"),
            "market_equity": valuation.get("market_equity"),
            "valuation_complete": valuation.get("valuation_complete"),
            "unpriced_position_count": valuation.get("unpriced_position_count"),
            "unpriced_holding_count": valuation.get("unpriced_holding_count"),
            "valuation_observed_at": valuation.get("valuation_observed_at"),
            "valuation_stale": valuation.get("valuation_stale"),
            "exposure": row.exposure,
            "open_position_count": row.open_position_count,
            "max_total_exposure": max_total_exposure,
            "max_position_capital": max_position_capital,
            "risk_per_trade_pct": row.risk_per_trade_pct,
            "max_position_pct": row.max_position_pct,
            "max_total_exposure_pct": row.max_total_exposure_pct,
            "max_open_positions": row.max_open_positions,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @staticmethod
    def _serialize_sync_link(row):
        details = json.loads(row.details_json or "{}")
        return {
            "id": row.id,
            "idempotency_key": row.idempotency_key,
            "direction": row.direction,
            "status": row.status,
            "source_model": row.source_model,
            "market": row.market,
            "stock_code": row.stock_code,
            "portfolio_account_id": row.portfolio_account_id,
            "portfolio_position_id": row.portfolio_position_id,
            "portfolio_trade_id": row.portfolio_trade_id,
            "paper_plan_id": row.paper_plan_id,
            "paper_position_id": row.paper_position_id,
            "paper_event_id": row.paper_event_id,
            "quantity": row.quantity,
            "price": row.price,
            "fee": row.fee,
            "trade_date": row.trade_date.isoformat() if row.trade_date else None,
            "origin": row.origin,
            "details": details,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @staticmethod
    def _serialize_adjustment(row):
        return {
            "id": row.id,
            "idempotency_key": row.idempotency_key,
            "account_id": row.account_id,
            "source_model": row.source_model,
            "market": row.market,
            "currency": row.currency,
            "adjustment_type": row.adjustment_type,
            "amount": row.amount,
            "reason": row.reason,
            "before_cash": row.before_cash,
            "after_cash": row.after_cash,
            "before_book_equity": row.before_book_equity,
            "after_book_equity": row.after_book_equity,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _serialize_dispatch(row):
        return {
            "id": row.id,
            "dispatch_key": row.dispatch_key,
            "event_id": row.event_id,
            "channel": row.channel,
            "status": row.status,
            "attempt_count": row.attempt_count,
            "last_error": row.last_error,
            "payload": json.loads(row.payload_json or "{}"),
            "sent_at": row.sent_at.isoformat() if row.sent_at else None,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }


def resolve_paper_trading_style_context(
    config: Any,
    *,
    market: Optional[str],
    stock_code: Optional[str],
    base_style_override: Optional[str] = None,
) -> Dict[str, Any]:
    """Resolve the effective configured paper-trading style without opening trading state.

    ``base_style_override`` lets a caller (e.g. the opportunity pool) pick its own
    base style instead of ``PAPER_TRADING_STYLE``; per-symbol
    ``PAPER_TRADING_STYLE_OVERRIDES`` still take precedence over it. An empty or
    unrecognized override is ignored, keeping the paper-trading base style.
    """

    override_text = str(base_style_override or "").strip().lower().replace("-", "_")
    use_override = override_text in TRADING_STYLE_SETTINGS
    base_style = PaperTradingService._normalize_trading_style(
        override_text if use_override else getattr(config, "paper_trading_style", "position")
    )
    raw_overrides = getattr(config, "paper_trading_style_overrides", None)
    overrides = {
        str(key).strip().lower(): PaperTradingService._normalize_trading_style(value)
        for key, value in dict(raw_overrides or {}).items()
        if str(key).strip()
    }
    market_norm = str(market or "").strip().lower()
    code_text = str(stock_code or "").strip().lower()
    candidates: List[str] = []
    identity = canonical_paper_identity(market_norm, stock_code or "")
    if identity is not None:
        canonical_market, canonical_code = identity
        candidates.extend([
            f"{canonical_market}:{canonical_code}".lower(),
            f"{canonical_market}:{str(stock_code or '').strip().lower()}",
        ])
    if market_norm and code_text:
        candidates.append(f"{market_norm}:{code_text}")
    matched_key: Optional[str] = None
    style = base_style
    for key in dict.fromkeys(candidates):
        if key in overrides:
            matched_key = key
            style = overrides[key]
            break
    if matched_key:
        source = "paper_trading_style_override_config"
    elif use_override:
        source = "base_style_override"
    else:
        source = "paper_trading_style_config"
    settings = dict(TRADING_STYLE_SETTINGS[style])
    return {
        "trading_style": style,
        "policy_version": TRADING_STYLE_POLICY_VERSION,
        "source": source,
        "matched_override_key": matched_key,
        "market": market_norm or None,
        "stock_code": stock_code,
        "settings": settings,
    }
