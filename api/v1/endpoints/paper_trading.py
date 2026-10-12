# -*- coding: utf-8 -*-
"""Authenticated API for report-derived, end-of-day paper trading."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.security import APIKeyCookie

from api.deps import get_runtime_scheduler_service
from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.paper_trading import (
    PaperCapitalAccountCreateRequest,
    PaperCapitalAdjustmentRequest,
    PaperCapitalAdjustmentResponse,
    PaperReportPlanReconciliationListResponse,
    PaperPortfolioImportRequest,
    PaperPortfolioSyncRequest,
    PaperRiskManualControlRequest,
    PaperSyncListResponse,
    PaperSyncMutationResponse,
    PaperTradingListResponse,
    PaperTradingMutationResponse,
    PaperTradingPerformanceResponse,
    PaperTradingPlanCreateRequest,
    PaperTradingRunRequest,
    PaperTradingRunResponse,
    PaperTradingScopeListResponse,
    PaperTradingStatusResponse,
)
from src.auth import COOKIE_NAME
from src.config import get_config
from src.utils.sanitize import sanitize_decision_signal_text
from src.services.paper_trading_service import (
    PaperTradingConflictError,
    PaperTradingExecutionNotAuthorizedError,
    PaperTradingNotFoundError,
    PaperTradingService,
)
from src.services.runtime_scheduler import RuntimeSchedulerService
from src.services.scheduler_heartbeat import latest_scheduler_heartbeat


logger = logging.getLogger(__name__)
admin_session_cookie = APIKeyCookie(
    name=COOKIE_NAME,
    scheme_name="AdminSessionCookie",
    auto_error=False,
)
router = APIRouter(dependencies=[Security(admin_session_cookie)])

AUTH_RESPONSE = {
    401: {
        "model": ErrorResponse,
        "description": "未登录或管理员会话无效（ADMIN_AUTH_ENABLED=true 时）",
    },
}


@contextmanager
def _scoped_paper_service(scope_id: Optional[str]):
    """Yield a read-only PaperTradingService bound to the requested scope.

    ``scope_id`` omitted or ``primary`` returns the normal main-DB service. An
    ``experiment:<id>:<arm>`` scope is resolved through the *trusted* registry
    file (never a client-supplied DB address) to a read-only connection on that
    arm's isolated database, disposed when the request finishes. Unknown or
    malformed scopes, or a process with no registry configured, fail closed
    with HTTP 400/404 rather than silently reading the main account.
    """

    from src.storage import DatabaseManager, REPORT_EXECUTION_SCOPE_PRIMARY
    from src.services.opportunity_experiment_service import (
        OpportunityExperimentError,
        load_experiment_registries,
        resolve_scope_registry,
    )

    requested = str(scope_id or "").strip() or REPORT_EXECUTION_SCOPE_PRIMARY
    if requested == REPORT_EXECUTION_SCOPE_PRIMARY:
        yield PaperTradingService.from_config()
        return

    config = get_config()
    try:
        registries = load_experiment_registries(
            getattr(config, "opportunity_experiment_registry_path", "")
        )
        registry, arm = resolve_scope_registry(requested, registries)
    except OpportunityExperimentError as exc:
        raise HTTPException(
            status_code=404,
            detail={"error": "unknown_execution_scope", "message": str(exc)},
        ) from exc

    if not registries:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "experiments_not_configured",
                "message": "no opportunity experiment registry is configured",
            },
        )

    scope = registry.scope_for(arm)
    try:
        arm_url = registry.raw_arm_db_url(arm, main_db_url=config.get_db_url())
    except OpportunityExperimentError as exc:
        # 隔离校验失败（指向主库/组间同库/未配置）一律 fail-closed，绝不回落主库。
        raise HTTPException(
            status_code=400,
            detail={"error": "experiment_scope_unavailable", "message": str(exc)},
        ) from exc

    scoped_db = DatabaseManager.create_scoped_readonly(arm_url)
    try:
        from src.repositories.paper_trading_repo import PaperTradingRepository

        repo = PaperTradingRepository(db_manager=scoped_db)
        yield PaperTradingService.from_config(repo=repo, execution_scope=scope)
    finally:
        scoped_db.dispose()


def _scheduler_task_heartbeat_alive(heartbeat: Dict[str, Any]) -> bool:
    if not isinstance(heartbeat, dict):
        return False
    if not bool(heartbeat.get("fresh")):
        return False
    status = str(heartbeat.get("status") or "").strip().lower()
    if status not in {"idle", "alive", "running"}:
        return False
    details = heartbeat.get("details") if isinstance(heartbeat.get("details"), dict) else {}
    if details.get("enabled") is False:
        return False
    return True


def _parse_observed_at(value: Any) -> datetime:
    try:
        return datetime.fromisoformat(str(value or ""))
    except Exception:
        return datetime.min


def _shared_scheduler_status(shared_scheduler: Dict[str, Any], scheduler_task_states: Dict[str, Any]) -> Dict[str, Any]:
    task_heartbeats = []
    active_leases = []
    active_lease = False
    for task in scheduler_task_states.values():
        if not isinstance(task, dict):
            continue
        lease = task.get("lease") if isinstance(task.get("lease"), dict) else {}
        if bool(lease.get("active")):
            active_lease = True
            active_leases.append(lease)
        heartbeat = task.get("heartbeat") if isinstance(task.get("heartbeat"), dict) else {}
        if _scheduler_task_heartbeat_alive(heartbeat):
            task_heartbeats.append(heartbeat)

    task_running = bool(task_heartbeats or active_lease)
    root_running = bool(shared_scheduler.get("fresh")) and str(shared_scheduler.get("status") or "").strip().lower() in {
        "alive",
        "running",
    }
    owner_heartbeat = None
    if task_heartbeats:
        owner_heartbeat = max(task_heartbeats, key=lambda item: _parse_observed_at(item.get("observed_at")))
    owner_lease = None
    if active_leases:
        owner_lease = max(
            active_leases,
            key=lambda item: _parse_observed_at(
                item.get("heartbeat_at")
                or item.get("started_at")
                or item.get("bucket_started_at")
                or item.get("lease_until")
            ),
        )
    owner_source = owner_heartbeat or owner_lease or {}
    owner_seen_at = (
        shared_scheduler.get("observed_at")
        or (owner_heartbeat or {}).get("observed_at")
        or (owner_lease or {}).get("heartbeat_at")
        or (owner_lease or {}).get("started_at")
        or (owner_lease or {}).get("bucket_started_at")
        or (owner_lease or {}).get("lease_until")
    )
    return {
        "running": task_running or root_running,
        "fresh": bool(shared_scheduler.get("fresh")) or bool(task_heartbeats) or active_lease,
        "owner_kind": shared_scheduler.get("owner_kind") or owner_source.get("owner_kind"),
        "owner_id": shared_scheduler.get("owner_id") or owner_source.get("owner_id"),
        "observed_at": owner_seen_at,
    }


def _scheduler_last_run_summaries(scheduler_task_states: Dict[str, Any]) -> Dict[str, Any]:
    summaries: Dict[str, Any] = {}
    for name, state in scheduler_task_states.items():
        if not isinstance(state, dict):
            continue
        heartbeat = state.get("heartbeat") if isinstance(state.get("heartbeat"), dict) else {}
        lease = state.get("lease") if isinstance(state.get("lease"), dict) else {}
        heartbeat_details = heartbeat.get("details") if isinstance(heartbeat.get("details"), dict) else {}
        lease_details = lease.get("details") if isinstance(lease.get("details"), dict) else {}
        summary = heartbeat_details.get("last_run_summary")
        if not isinstance(summary, dict):
            summary = lease_details.get("last_run_summary")
        if isinstance(summary, dict) and summary:
            safe_summary = dict(summary)
            if safe_summary.get("error_message"):
                safe_summary["error_message"] = sanitize_decision_signal_text(safe_summary["error_message"])[:2000]
            summaries[str(name)] = safe_summary
    return summaries


def _internal_error(message: str, exc: Exception) -> HTTPException:
    logger.error("%s: %s", message, exc, exc_info=True)
    return HTTPException(
        status_code=500,
        detail={"error": "internal_error", "message": message},
    )


def _validation_error(exc: Exception) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={"error": "validation_error", "message": str(exc)},
    )


@router.get("/status", response_model=PaperTradingStatusResponse, responses=AUTH_RESPONSE)
def monitor_status(
    scheduler: RuntimeSchedulerService = Depends(get_runtime_scheduler_service),
) -> PaperTradingStatusResponse:
    try:
        config = get_config()
        service = PaperTradingService.from_config(config)
        interval_minutes = max(1, int(getattr(config, "paper_trading_intraday_interval_minutes", 1)))
        scheduler_status = scheduler.status()
        local_scheduler_enabled = bool(scheduler_status.get("enabled"))
        local_scheduler_running = bool(scheduler_status.get("running"))
        shared_scheduler = scheduler_status.get("shared_scheduler") or latest_scheduler_heartbeat()
        scheduler_task_states = dict(shared_scheduler.get("tasks") or shared_scheduler.get("task_states") or {})
        latest_bucket = service.repo.latest_observation_bucket()
        market_context_risk = service.market_context_risk_status(market="cn")
        shared_status = _shared_scheduler_status(shared_scheduler, scheduler_task_states)
        scheduler_last_run_summaries = _scheduler_last_run_summaries(scheduler_task_states)
        intraday_monitor_summary = scheduler_last_run_summaries.get("paper_trading_intraday_monitor")
        return PaperTradingStatusResponse(
            enabled=bool(getattr(config, "paper_trading_enabled", False)),
            intraday_enabled=bool(getattr(config, "paper_trading_intraday_enabled", False)),
            interval_minutes=interval_minutes,
            interval_seconds=interval_minutes * 60,
            min_factor_domains=service.intraday_min_factor_domains,
            min_composite_score=service.intraday_min_composite_score,
            min_risk_reward=service.intraday_min_risk_reward,
            max_quote_stale_seconds=service.intraday_max_quote_stale_seconds,
            trading_style=service.trading_style,
            trading_style_settings=service.trading_style_settings,
            strategy_thesis_enabled=service.strategy_thesis_enabled,
            strategy_entry_confirmation_sessions=service.strategy_entry_confirmation_sessions,
            strategy_flip_confirmation_sessions=service.strategy_flip_confirmation_sessions,
            strategy_hard_risk_immediate=True,
            plan_compensation_enabled=service.plan_compensation_enabled,
            plan_compensation_lookback_days=service.plan_compensation_lookback_days,
            market_context_risk_control_enabled=service.market_context_risk_control_enabled,
            market_context_lookback_days=service.market_context_lookback_days,
            market_context_risk=market_context_risk,
            initial_capital=service.initial_capital,
            risk_per_trade_pct=service.risk_per_trade_pct,
            max_position_pct=service.max_position_pct,
            max_symbol_exposure_pct=service.max_symbol_exposure_pct,
            asset_exposure_caps=service.asset_exposure_caps,
            asset_class_overrides=service.asset_class_overrides,
            max_total_exposure_pct=service.max_total_exposure_pct,
            max_open_positions=service.max_open_positions,
            default_reduce_fraction=service.default_reduce_fraction,
            slippage_pct=service.slippage_pct,
            commission_rate=service.commission_rate,
            min_commission=service.min_commission,
            sell_tax_rate=service.sell_tax_rate,
            auction_price_deviation_pct=service.auction_price_deviation_pct,
            auction_min_matched_volume_ratio=service.auction_min_matched_volume_ratio,
            account_daily_loss_limit_pct=service.account_daily_loss_limit_pct,
            account_drawdown_limit_pct=service.account_drawdown_limit_pct,
            account_drawdown_recovery_pct=service.account_drawdown_recovery_pct,
            account_losing_streak_limit=service.account_losing_streak_limit,
            account_daily_trade_limit=service.account_daily_trade_limit,
            account_daily_open_add_limit=service.account_daily_open_add_limit,
            account_sector_cap_pct=service.account_sector_cap_pct,
            account_correlation_cap_pct=service.account_correlation_cap_pct,
            account_unknown_cap_pct=service.account_unknown_cap_pct,
            correlation_threshold=service.correlation_threshold,
            correlation_lookback_sessions=service.correlation_lookback_sessions,
            correlation_min_overlap_sessions=service.correlation_min_overlap_sessions,
            feishu_notify_enabled=service.feishu_notify,
            feishu_route_configured=service._feishu_route_configured(),
            local_scheduler_enabled=local_scheduler_enabled,
            local_scheduler_running=local_scheduler_running,
            local_scheduler_owner="api_process",
            scheduler_enabled=local_scheduler_enabled or bool(shared_status.get("fresh")),
            scheduler_running=local_scheduler_running or bool(shared_status.get("running")),
            scheduler_heartbeat=shared_scheduler,
            scheduler_heartbeat_fresh=bool(shared_scheduler.get("fresh")),
            scheduler_task_states=scheduler_task_states,
            scheduler_last_run_summaries=scheduler_last_run_summaries,
            intraday_monitor_last_run_summary=intraday_monitor_summary,
            scheduler_owner_kind=shared_status.get("owner_kind"),
            scheduler_owner_id=shared_status.get("owner_id"),
            scheduler_last_seen_at=shared_status.get("observed_at"),
            schedule_times=list(scheduler_status.get("schedule_times") or shared_scheduler.get("schedule_times") or []),
            next_run_at=scheduler_status.get("next_run_at") or shared_scheduler.get("next_run_at"),
            latest_observation_bucket=latest_bucket.isoformat() if latest_bucket else None,
        )
    except Exception as exc:
        raise _internal_error("Read paper-trading monitor status failed", exc) from exc


@router.post(
    "/plans",
    response_model=PaperTradingMutationResponse,
    responses={
        **AUTH_RESPONSE,
        400: {"model": ErrorResponse},
        409: {"model": ErrorResponse, "description": "报告执行用途不授权当前模拟账户（如研究专用报告）"},
        500: {"model": ErrorResponse},
    },
    summary="从历史分析报告创建模拟执行计划",
)
def create_plan(request: PaperTradingPlanCreateRequest) -> PaperTradingMutationResponse:
    try:
        result = PaperTradingService.from_config().create_plan_for_report(
            request.source_report_id,
            decision_signal_id=request.decision_signal_id,
        )
        return PaperTradingMutationResponse(**result)
    except PaperTradingExecutionNotAuthorizedError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "execution_not_authorized",
                "message": str(exc),
                "detail": {"reason": exc.reason, "report_id": exc.report_id},
            },
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "validation_error", "message": str(exc)},
        ) from exc
    except Exception as exc:
        raise _internal_error("Create paper-trading plan failed", exc) from exc


@router.get("/plans", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_plans(
    model: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    scope_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        with _scoped_paper_service(scope_id) as service:
            result = service.list_plans(
                model=model,
                status=status,
                offset=(page - 1) * limit,
                limit=limit,
            )
        return PaperTradingListResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise _internal_error("List paper-trading plans failed", exc) from exc


@router.get("/positions", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_positions(
    model: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    scope_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        with _scoped_paper_service(scope_id) as service:
            result = service.list_positions(
                model=model,
                status=status,
                offset=(page - 1) * limit,
                limit=limit,
            )
        return PaperTradingListResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise _internal_error("List paper positions failed", exc) from exc


@router.get("/operations", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_operations(
    model: Optional[str] = Query(None),
    scope_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        with _scoped_paper_service(scope_id) as service:
            result = service.list_operations(
                model=model,
                offset=(page - 1) * limit,
                limit=limit,
            )
        return PaperTradingListResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise _internal_error("List paper current operations failed", exc) from exc


@router.get("/holdings", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_holdings(
    model: Optional[str] = Query(None),
    scope_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        with _scoped_paper_service(scope_id) as service:
            result = service.list_holdings(
                model=model,
                offset=(page - 1) * limit,
                limit=limit,
            )
        return PaperTradingListResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise _internal_error("List paper aggregate holdings failed", exc) from exc


@router.get("/events", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_events(
    model: Optional[str] = Query(None),
    scope_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        with _scoped_paper_service(scope_id) as service:
            result = service.list_events(
                model=model,
                offset=(page - 1) * limit,
                limit=limit,
            )
        return PaperTradingListResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise _internal_error("List paper-trading events failed", exc) from exc


@router.get("/resolutions", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_resolutions(
    model: Optional[str] = Query(None),
    scope_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        with _scoped_paper_service(scope_id) as service:
            result = service.list_resolutions(
                model=model,
                offset=(page - 1) * limit,
                limit=limit,
            )
        return PaperTradingListResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise _internal_error("List paper-trading resolutions failed", exc) from exc


@router.get("/scopes", response_model=PaperTradingScopeListResponse, responses=AUTH_RESPONSE)
def list_scopes() -> PaperTradingScopeListResponse:
    """List the read-only execution scopes the UI may select.

    Always includes ``primary``. Experiment scopes appear only when a trusted
    registry file is configured for this process; scope metadata never exposes a
    database address or credential — only the scope id, arm, and a label.
    """

    from src.storage import REPORT_EXECUTION_SCOPE_PRIMARY
    from src.services.opportunity_experiment_service import (
        OpportunityExperimentError,
        load_experiment_registries,
    )

    items = [{"scope_id": REPORT_EXECUTION_SCOPE_PRIMARY, "label": "主账户", "kind": "primary"}]
    config = get_config()
    try:
        registries = load_experiment_registries(
            getattr(config, "opportunity_experiment_registry_path", "")
        )
    except OpportunityExperimentError as exc:
        # 注册表配置损坏时只暴露主账户，不因实验配置错误拖垮页面。
        logger.warning("opportunity experiment registry unavailable: %s", exc)
        registries = []
    for registry in registries:
        for arm in ("a", "b", "c"):
            items.append({
                "scope_id": registry.scope_for(arm),
                "label": f"{registry.experiment_id} · {arm.upper()}组",
                "kind": "experiment",
                "experiment_id": registry.experiment_id,
                "arm": arm,
            })
    return PaperTradingScopeListResponse(items=items)


@router.get("/risk-revisions", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_risk_revisions(
    model: Optional[str] = Query(None),
    scope_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        with _scoped_paper_service(scope_id) as service:
            result = service.list_risk_revisions(
                model=model,
                offset=(page - 1) * limit,
                limit=limit,
            )
        return PaperTradingListResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise _internal_error("List paper-trading risk revisions failed", exc) from exc


@router.get("/reanalysis-requests", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_reanalysis_requests(
    model: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        result = PaperTradingService.from_config().list_reanalysis_requests(
            model=model,
            status=status,
            offset=(page - 1) * limit,
            limit=limit,
        )
        return PaperTradingListResponse(**result)
    except Exception as exc:
        raise _internal_error("List paper-trading reanalysis requests failed", exc) from exc


@router.get("/report-plan-reconciliations", response_model=PaperReportPlanReconciliationListResponse, responses=AUTH_RESPONSE)
def list_report_plan_reconciliations(
    model: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    plan_version: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperReportPlanReconciliationListResponse:
    try:
        result = PaperTradingService.from_config().list_report_plan_reconciliations(
            model=model,
            status=status,
            plan_version=plan_version,
            offset=(page - 1) * limit,
            limit=limit,
        )
        return PaperReportPlanReconciliationListResponse(**result)
    except Exception as exc:
        raise _internal_error("List report-plan reconciliations failed", exc) from exc


@router.get("/risk-snapshots", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_risk_snapshots(
    model: Optional[str] = Query(None),
    account_id: Optional[int] = Query(None, ge=1),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        result = PaperTradingService.from_config().list_account_risk_snapshots(
            model=model,
            account_id=account_id,
            offset=(page - 1) * limit,
            limit=limit,
        )
        return PaperTradingListResponse(**result)
    except Exception as exc:
        raise _internal_error("List paper-trading account risk snapshots failed", exc) from exc


@router.get("/risk-events", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_risk_events(
    model: Optional[str] = Query(None),
    account_id: Optional[int] = Query(None, ge=1),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        result = PaperTradingService.from_config().list_account_risk_events(
            model=model,
            account_id=account_id,
            offset=(page - 1) * limit,
            limit=limit,
        )
        return PaperTradingListResponse(**result)
    except Exception as exc:
        raise _internal_error("List paper-trading account risk events failed", exc) from exc


@router.post("/risk/manual-hold", response_model=PaperTradingMutationResponse, responses=AUTH_RESPONSE)
def manual_hold(request: PaperRiskManualControlRequest) -> PaperTradingMutationResponse:
    try:
        result = PaperTradingService.from_config().set_account_manual_hold(
            account_id=request.account_id,
            reason=request.reason,
            idempotency_key=request.idempotency_key,
        )
        return PaperTradingMutationResponse(**result)
    except PaperTradingNotFoundError as exc:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": str(exc)}) from exc
    except PaperTradingConflictError as exc:
        raise HTTPException(status_code=409, detail={"error": "risk_conflict", "message": str(exc)}) from exc
    except ValueError as exc:
        raise _validation_error(exc) from exc
    except Exception as exc:
        raise _internal_error("Set paper-trading manual hold failed", exc) from exc


@router.post("/risk/manual-resume", response_model=PaperTradingMutationResponse, responses=AUTH_RESPONSE)
def manual_resume(request: PaperRiskManualControlRequest) -> PaperTradingMutationResponse:
    try:
        result = PaperTradingService.from_config().resume_account_risk(
            account_id=request.account_id,
            reason=request.reason,
            idempotency_key=request.idempotency_key,
        )
        return PaperTradingMutationResponse(**result)
    except PaperTradingNotFoundError as exc:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": str(exc)}) from exc
    except PaperTradingConflictError as exc:
        raise HTTPException(status_code=409, detail={"error": "risk_conflict", "message": str(exc)}) from exc
    except ValueError as exc:
        raise _validation_error(exc) from exc
    except Exception as exc:
        raise _internal_error("Resume paper-trading account risk failed", exc) from exc


@router.get("/accounts", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_accounts(
    model: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    scope_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        with _scoped_paper_service(scope_id) as service:
            result = service.list_accounts(
                model=model,
                market=market,
                offset=(page - 1) * limit,
                limit=limit,
            )
        return PaperTradingListResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise _internal_error("List paper-trading capital accounts failed", exc) from exc


@router.post(
    "/accounts",
    response_model=PaperTradingMutationResponse,
    responses={**AUTH_RESPONSE, 400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
    summary="初始化模拟资金账户",
)
def initialize_account(request: PaperCapitalAccountCreateRequest) -> PaperTradingMutationResponse:
    try:
        result = PaperTradingService.from_config().initialize_account(
            source_model=request.source_model,
            market=request.market,
            initial_capital=request.initial_capital,
        )
        return PaperTradingMutationResponse(**result)
    except ValueError as exc:
        raise _validation_error(exc) from exc
    except Exception as exc:
        raise _internal_error("Initialize paper capital account failed", exc) from exc


@router.post(
    "/accounts/{account_id}/adjustments",
    response_model=PaperCapitalAdjustmentResponse,
    responses={
        **AUTH_RESPONSE,
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="登记模拟资金入金或出金",
)
def adjust_account(account_id: int, request: PaperCapitalAdjustmentRequest) -> PaperCapitalAdjustmentResponse:
    try:
        result = PaperTradingService.from_config().adjust_account(
            account_id,
            adjustment_type=request.adjustment_type,
            amount=request.amount,
            idempotency_key=request.idempotency_key,
            reason=request.reason,
        )
        return PaperCapitalAdjustmentResponse(**result)
    except PaperTradingNotFoundError as exc:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": str(exc)}) from exc
    except PaperTradingConflictError as exc:
        raise HTTPException(status_code=409, detail={"error": "idempotency_conflict", "message": str(exc)}) from exc
    except ValueError as exc:
        raise _validation_error(exc) from exc
    except Exception as exc:
        raise _internal_error("Adjust paper capital account failed", exc) from exc


@router.get("/adjustments", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_adjustments(
    model: Optional[str] = Query(None),
    account_id: Optional[int] = Query(None, ge=1),
    adjustment_type: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        result = PaperTradingService.from_config().list_adjustments(
            model=model,
            account_id=account_id,
            adjustment_type=adjustment_type,
            offset=(page - 1) * limit,
            limit=limit,
        )
        return PaperTradingListResponse(**result)
    except Exception as exc:
        raise _internal_error("List paper capital adjustments failed", exc) from exc


@router.post(
    "/sync/from-portfolio",
    response_model=PaperSyncListResponse,
    responses={
        **AUTH_RESPONSE,
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="将真实持仓显式导入选定模型的模拟账本",
)
def sync_from_portfolio(request: PaperPortfolioImportRequest) -> PaperSyncListResponse:
    try:
        result = PaperTradingService.from_config().sync_portfolio_position_to_paper(
            portfolio_position_id=request.portfolio_position_id,
            source_models=request.source_models,
            trade_date=request.trade_date,
            idempotency_key=request.idempotency_key,
        )
        return PaperSyncListResponse(**result)
    except PaperTradingNotFoundError as exc:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": str(exc)}) from exc
    except PaperTradingConflictError as exc:
        raise HTTPException(status_code=409, detail={"error": "sync_conflict", "message": str(exc)}) from exc
    except ValueError as exc:
        raise _validation_error(exc) from exc
    except Exception as exc:
        raise _internal_error("Sync portfolio position to paper ledger failed", exc) from exc


@router.post(
    "/sync/to-portfolio",
    response_model=PaperSyncMutationResponse,
    responses={
        **AUTH_RESPONSE,
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    summary="将确认后的模拟仓位交易显式登记到真实持仓",
)
def sync_to_portfolio(request: PaperPortfolioSyncRequest) -> PaperSyncMutationResponse:
    try:
        result = PaperTradingService.from_config().sync_paper_position_to_portfolio(
            paper_position_id=request.paper_position_id,
            portfolio_account_id=request.portfolio_account_id,
            side=request.side,
            quantity=request.quantity,
            price=request.price,
            trade_date=request.trade_date,
            fee=request.fee,
            idempotency_key=request.idempotency_key,
            note=request.note,
        )
        return PaperSyncMutationResponse(**result)
    except PaperTradingNotFoundError as exc:
        raise HTTPException(status_code=404, detail={"error": "not_found", "message": str(exc)}) from exc
    except PaperTradingConflictError as exc:
        raise HTTPException(status_code=409, detail={"error": "sync_conflict", "message": str(exc)}) from exc
    except ValueError as exc:
        raise _validation_error(exc) from exc
    except Exception as exc:
        raise _internal_error("Sync paper position to portfolio failed", exc) from exc


@router.get("/dispatches", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_dispatches(
    model: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        result = PaperTradingService.from_config().list_dispatches(
            model=model,
            status=status,
            offset=(page - 1) * limit,
            limit=limit,
        )
        return PaperTradingListResponse(**result)
    except Exception as exc:
        raise _internal_error("List paper-trading notification dispatches failed", exc) from exc


@router.get("/evaluations", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_evaluations(
    model: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        result = PaperTradingService.from_config().list_evaluations(
            model=model,
            offset=(page - 1) * limit,
            limit=limit,
        )
        return PaperTradingListResponse(**result)
    except Exception as exc:
        raise _internal_error("List paper-trading evaluations failed", exc) from exc


@router.get("/observations", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_observations(
    model: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        result = PaperTradingService.from_config().list_observations(
            model=model,
            offset=(page - 1) * limit,
            limit=limit,
        )
        return PaperTradingListResponse(**result)
    except Exception as exc:
        raise _internal_error("List paper-trading observations failed", exc) from exc


@router.get("/auction-observations", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_auction_observations(
    model: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        result = PaperTradingService.from_config().list_auction_observations(
            model=model,
            offset=(page - 1) * limit,
            limit=limit,
        )
        return PaperTradingListResponse(**result)
    except Exception as exc:
        raise _internal_error("List paper-trading auction observations failed", exc) from exc


@router.get("/account-risk/locks", response_model=PaperTradingListResponse, responses=AUTH_RESPONSE)
def list_current_account_risk_locks(
    model: Optional[str] = Query(None),
    account_id: Optional[int] = Query(None, ge=1),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
) -> PaperTradingListResponse:
    try:
        result = PaperTradingService.from_config().list_current_account_risk_locks(
            model=model,
            account_id=account_id,
            offset=(page - 1) * limit,
            limit=limit,
        )
        return PaperTradingListResponse(**result)
    except Exception as exc:
        raise _internal_error("List paper-trading current account risk locks failed", exc) from exc


@router.get("/performance", response_model=PaperTradingPerformanceResponse, responses=AUTH_RESPONSE)
def performance(model: Optional[str] = Query(None)) -> PaperTradingPerformanceResponse:
    try:
        return PaperTradingPerformanceResponse(metrics=PaperTradingService.from_config().performance(model=model))
    except Exception as exc:
        raise _internal_error("Read paper-trading performance failed", exc) from exc


@router.post("/run", response_model=PaperTradingRunResponse, responses=AUTH_RESPONSE)
def run_monitor(request: PaperTradingRunRequest) -> PaperTradingRunResponse:
    try:
        service = PaperTradingService.from_config()
        if request.mode == "intraday":
            stats = service.run_intraday_monitor()
        else:
            stats = service.run_monitor(evaluation_date=request.evaluation_date)
        return PaperTradingRunResponse(stats=stats)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "validation_error", "message": str(exc)},
        ) from exc
    except Exception as exc:
        raise _internal_error("Run paper-trading monitor failed", exc) from exc
