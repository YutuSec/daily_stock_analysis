# -*- coding: utf-8 -*-
"""Schemas for the market opportunity pool API."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class MarketOpportunityModelState(BaseModel):
    model: str
    status: str = "missing"
    job_id: Optional[int] = None
    task_id: Optional[str] = None
    query_id: Optional[str] = None
    analysis_history_id: Optional[int] = None
    decision_signal_id: Optional[int] = None
    paper_plan_id: Optional[int] = None
    failure_reason: Optional[str] = None
    no_plan_reason: Optional[str] = None
    paper_reconciliation: Dict[str, Any] = Field(default_factory=dict)
    plan_executable: Optional[bool] = None
    plan_status: Optional[str] = None
    confirmation_progress: Optional[Dict[str, Any]] = None
    followup: Optional[Dict[str, Any]] = None
    execution_intent: Optional[str] = None
    research: Optional[Dict[str, Any]] = None
    action: Optional[str] = None
    action_label: Optional[str] = None
    updated_at: Optional[str] = None


class MarketOpportunityRunItem(BaseModel):
    id: int
    run_key: str
    market: str
    trade_date: Optional[str] = None
    source_report_id: Optional[int] = None
    market_regime_id: Optional[int] = None
    market_regime: Optional[str] = None
    mode: str
    status: str
    policy_version: str
    config_snapshot: Dict[str, Any] = Field(default_factory=dict)
    provenance: Dict[str, Any] = Field(default_factory=dict)
    theme_count: int = 0
    selected_theme_count: int = 0
    candidate_count: int = 0
    selected_candidate_count: int = 0
    llm_budget: int = 0
    error_message: Optional[str] = None
    health_status: Optional[str] = None
    health_reason: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class MarketOpportunityThemeItem(BaseModel):
    id: int
    run_id: int
    theme_key: str
    theme_type: str
    theme_name: str
    theme_code: Optional[str] = None
    rank: Optional[int] = None
    change_pct: Optional[float] = None
    status: str
    score: Optional[float] = None
    score_components: Dict[str, Any] = Field(default_factory=dict)
    rejection_reasons: List[Any] = Field(default_factory=list)
    data_quality: Dict[str, Any] = Field(default_factory=dict)
    constituent_count: int = 0
    eligible_constituent_count: int = 0
    created_at: Optional[str] = None


class MarketOpportunityCandidateItem(BaseModel):
    id: int
    run_id: int
    candidate_key: str
    market: str
    exchange: str
    stock_code: str
    symbol: str
    stock_name: Optional[str] = None
    primary_theme_key: Optional[str] = None
    related_themes: List[Any] = Field(default_factory=list)
    status: str
    rank: Optional[int] = None
    score: Optional[float] = None
    score_components: Dict[str, Any] = Field(default_factory=dict)
    rejection_reasons: List[Any] = Field(default_factory=list)
    data_quality: Dict[str, Any] = Field(default_factory=dict)
    quote_snapshot: Dict[str, Any] = Field(default_factory=dict)
    as_of: Optional[str] = None
    valid_until: Optional[str] = None
    snapshot_hash: Optional[str] = None
    created_at: Optional[str] = None
    analysis_jobs: List[Dict[str, Any]] = Field(default_factory=list)
    model_states: List[MarketOpportunityModelState] = Field(default_factory=list)
    no_plan_reason: Optional[str] = None


class MarketOpportunityAnalysisJobItem(BaseModel):
    id: int
    run_id: int
    candidate_id: int
    job_key: str
    model: str
    status: str
    attempt_count: int = 0
    max_attempts: int = 0
    task_id: Optional[str] = None
    query_id: Optional[str] = None
    analysis_history_id: Optional[int] = None
    decision_signal_id: Optional[int] = None
    paper_plan_id: Optional[int] = None
    failure_reason: Optional[str] = None
    evidence: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class MarketOpportunityStatusResponse(BaseModel):
    mode: str
    enabled: bool
    mode_source: Optional[str] = None
    mode_version: Optional[int] = None
    mode_updated_at: Optional[str] = None
    mode_updated_by: Optional[str] = None
    mode_degraded: bool = False
    mode_provenance: Dict[str, Any] = Field(default_factory=dict)
    configured_models: List[str] = Field(default_factory=list)
    limits: Dict[str, Any] = Field(default_factory=dict)
    observe_gate: Dict[str, Any] = Field(default_factory=dict)
    message: str


class MarketOpportunityModeUpdateRequest(BaseModel):
    mode: str = Field(..., pattern="^(off|observe|paper)$")
    expected_version: int = Field(..., ge=0)
    reason: Optional[str] = Field(default=None, max_length=255)


class MarketOpportunityModeClearRequest(BaseModel):
    expected_version: int = Field(..., ge=0)
    reason: Optional[str] = Field(default=None, max_length=255)


class MarketOpportunityDiscoverRequest(BaseModel):
    market: str = Field(default="cn")
    report_id: Optional[int] = None


class MarketOpportunityJobRunRequest(BaseModel):
    limit: int = Field(default=20, ge=1, le=200)
    owner: Optional[str] = None


class MarketOpportunityActionResponse(BaseModel):
    status: str
    result: Dict[str, Any] = Field(default_factory=dict)


class MarketOpportunityRunListResponse(BaseModel):
    total: int
    page: int
    limit: int
    items: List[MarketOpportunityRunItem] = Field(default_factory=list)
    configured_models: List[str] = Field(default_factory=list)


class MarketOpportunityRunDetailResponse(MarketOpportunityRunItem):
    themes: List[MarketOpportunityThemeItem] = Field(default_factory=list)
    candidates: List[MarketOpportunityCandidateItem] = Field(default_factory=list)
    analysis_jobs: List[MarketOpportunityAnalysisJobItem] = Field(default_factory=list)
    configured_models: List[str] = Field(default_factory=list)
    llm_submitted_job_count: Optional[int] = None


class MarketOpportunityThemeListResponse(BaseModel):
    total: int
    page: int
    limit: int
    items: List[MarketOpportunityThemeItem] = Field(default_factory=list)


class MarketOpportunityCandidateListResponse(BaseModel):
    total: int
    page: int
    limit: int
    items: List[MarketOpportunityCandidateItem] = Field(default_factory=list)
    configured_models: List[str] = Field(default_factory=list)


class MarketOpportunityAnalysisJobListResponse(BaseModel):
    total: int
    page: int
    limit: int
    items: List[MarketOpportunityAnalysisJobItem] = Field(default_factory=list)
    configured_models: List[str] = Field(default_factory=list)


class MarketOpportunityEvaluationResponse(BaseModel):
    windows: List[int] = Field(default_factory=list)
    candidate_count: int = 0
    candidate_metrics: List[Dict[str, Any]] = Field(default_factory=list)
    summary: Dict[str, Any] = Field(default_factory=dict)
    theme_summary: Dict[str, Any] = Field(default_factory=dict)
    model_execution_summary: Dict[str, Any] = Field(default_factory=dict)
    strategy_comparison_summary: Dict[str, Any] = Field(default_factory=dict)
