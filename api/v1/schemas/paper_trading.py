# -*- coding: utf-8 -*-
"""API schemas for the end-of-day paper-trading monitor."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class PaperTradingRunRequest(BaseModel):
    mode: Literal["eod", "intraday"] = Field(
        "eod",
        description="Monitor mode. eod evaluates complete daily bars; intraday evaluates fresh realtime quotes.",
    )
    evaluation_date: Optional[date] = Field(
        None,
        description="Trading date to evaluate; defaults to the latest complete local daily bar.",
    )


class PaperTradingPlanCreateRequest(BaseModel):
    source_report_id: int = Field(..., gt=0)
    decision_signal_id: Optional[int] = Field(None, gt=0)


class PaperCapitalAccountCreateRequest(BaseModel):
    source_model: str = Field(..., min_length=1, max_length=255)
    market: str = Field(..., min_length=1, max_length=8)
    initial_capital: Optional[float] = Field(None, gt=0)


class PaperCapitalAdjustmentRequest(BaseModel):
    adjustment_type: Literal["deposit", "withdrawal"]
    amount: float = Field(..., gt=0)
    idempotency_key: str = Field(..., min_length=1, max_length=128)
    reason: str = Field(..., min_length=1, max_length=255)


class PaperCapitalAdjustmentResponse(BaseModel):
    created: bool
    item: Dict[str, Any]
    account: Optional[Dict[str, Any]] = None


class PaperPortfolioImportRequest(BaseModel):
    portfolio_position_id: int = Field(..., gt=0)
    source_models: List[str] = Field(..., min_length=1)
    trade_date: date
    idempotency_key: str = Field(..., min_length=1, max_length=128)


class PaperPortfolioSyncRequest(BaseModel):
    paper_position_id: int = Field(..., gt=0)
    portfolio_account_id: int = Field(..., gt=0)
    side: Literal["buy", "sell"]
    quantity: float = Field(..., gt=0)
    price: float = Field(..., gt=0)
    trade_date: date
    fee: float = Field(0.0, ge=0)
    idempotency_key: str = Field(..., min_length=1, max_length=128)
    note: Optional[str] = Field(None, max_length=255)


class PaperRiskManualControlRequest(BaseModel):
    account_id: int = Field(..., gt=0)
    reason: str = Field(..., min_length=1, max_length=255)
    idempotency_key: str = Field(..., min_length=1, max_length=128)


class PaperSyncListResponse(BaseModel):
    created: bool
    items: List[Dict[str, Any]]


class PaperSyncMutationResponse(BaseModel):
    created: bool
    item: Dict[str, Any]


class PaperTradingMutationResponse(BaseModel):
    created: bool
    item: Dict[str, Any]


class PaperTradingListResponse(BaseModel):
    total: int
    items: List[Dict[str, Any]]


class PaperTradingScopeItem(BaseModel):
    scope_id: str
    label: str
    kind: str  # "primary" | "experiment"
    experiment_id: Optional[str] = None
    arm: Optional[str] = None


class PaperTradingScopeListResponse(BaseModel):
    items: List[PaperTradingScopeItem]


class PaperReportPlanReconciliationItem(BaseModel):
    id: int
    reconciliation_key: str
    source_report_id: int
    report: Optional[Dict[str, Any]] = None
    plan_version: str
    source_model: Optional[str] = None
    status: str
    classification: Optional[str] = None
    attempt_count: int
    max_attempts: int
    next_retry_at: Optional[str] = None
    last_attempt_at: Optional[str] = None
    last_error: Optional[str] = None
    decision_signal_id: Optional[int] = None
    paper_plan_id: Optional[int] = None
    details: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class PaperReportPlanReconciliationListResponse(BaseModel):
    total: int
    items: List[PaperReportPlanReconciliationItem]


class PaperTradingRunResponse(BaseModel):
    stats: Dict[str, Any]


class PaperTradingPerformanceResponse(BaseModel):
    metrics: Dict[str, Any]


class PaperTradingStatusResponse(BaseModel):
    enabled: bool
    intraday_enabled: bool
    interval_minutes: int
    interval_seconds: int
    min_factor_domains: int
    min_composite_score: float
    min_risk_reward: float
    max_quote_stale_seconds: int
    trading_style: str = "position"
    trading_style_settings: Dict[str, Any] = Field(default_factory=dict)
    strategy_thesis_enabled: bool = True
    strategy_entry_confirmation_sessions: int = 2
    strategy_flip_confirmation_sessions: int = 2
    strategy_hard_risk_immediate: bool = True
    plan_compensation_enabled: bool = True
    plan_compensation_lookback_days: int = 30
    market_context_risk_control_enabled: bool = True
    market_context_lookback_days: int = 3
    market_context_risk: Dict[str, Any] = Field(default_factory=dict)
    initial_capital: float
    risk_per_trade_pct: float
    max_position_pct: float
    max_symbol_exposure_pct: float
    asset_exposure_caps: Dict[str, float] = Field(default_factory=dict)
    asset_class_overrides: Dict[str, str] = Field(default_factory=dict)
    max_total_exposure_pct: float
    max_open_positions: int
    default_reduce_fraction: float
    slippage_pct: float
    commission_rate: float
    min_commission: float
    sell_tax_rate: float
    auction_price_deviation_pct: float
    auction_min_matched_volume_ratio: float
    account_daily_loss_limit_pct: float
    account_drawdown_limit_pct: float
    account_drawdown_recovery_pct: float
    account_losing_streak_limit: int
    account_daily_trade_limit: int
    account_daily_open_add_limit: int
    account_sector_cap_pct: float
    account_correlation_cap_pct: float
    account_unknown_cap_pct: float
    correlation_threshold: float
    correlation_lookback_sessions: int
    correlation_min_overlap_sessions: int
    feishu_notify_enabled: bool
    feishu_route_configured: bool
    local_scheduler_enabled: bool
    local_scheduler_running: bool
    local_scheduler_owner: str = "api"
    scheduler_enabled: bool
    scheduler_running: bool
    scheduler_heartbeat: Optional[Dict[str, Any]] = None
    scheduler_heartbeat_fresh: bool = False
    scheduler_task_states: Dict[str, Any] = Field(default_factory=dict)
    scheduler_last_run_summaries: Dict[str, Any] = Field(default_factory=dict)
    intraday_monitor_last_run_summary: Optional[Dict[str, Any]] = None
    scheduler_owner_kind: Optional[str] = None
    scheduler_owner_id: Optional[str] = None
    scheduler_last_seen_at: Optional[str] = None
    schedule_times: List[str]
    next_run_at: Optional[str] = None
    latest_observation_bucket: Optional[str] = None
