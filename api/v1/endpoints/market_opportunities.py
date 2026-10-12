# -*- coding: utf-8 -*-
"""Authenticated API for market opportunity discovery ledgers."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.security import APIKeyCookie
from sqlalchemy import desc
from sqlalchemy.exc import SQLAlchemyError

from api.deps import get_config_dep, get_database_manager
from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.market_opportunities import (
    MarketOpportunityActionResponse,
    MarketOpportunityAnalysisJobItem,
    MarketOpportunityAnalysisJobListResponse,
    MarketOpportunityCandidateItem,
    MarketOpportunityCandidateListResponse,
    MarketOpportunityDiscoverRequest,
    MarketOpportunityEvaluationResponse,
    MarketOpportunityJobRunRequest,
    MarketOpportunityModeClearRequest,
    MarketOpportunityModeUpdateRequest,
    MarketOpportunityModelState,
    MarketOpportunityRunItem,
    MarketOpportunityRunDetailResponse,
    MarketOpportunityRunListResponse,
    MarketOpportunityStatusResponse,
    MarketOpportunityThemeItem,
    MarketOpportunityThemeListResponse,
)
from src.auth import COOKIE_NAME
from src.config import Config
from src.repositories.market_opportunity_repository import MarketOpportunityRepository
from src.services.analysis_model_service import list_analysis_models
from src.services.market_opportunity_service import MarketOpportunityService
from src.services.market_opportunity_evaluation_service import MarketOpportunityEvaluationService
from src.services.runtime_settings_service import (
    RuntimeSettingConflictError,
    RuntimeSettingValidationError,
    RuntimeSettingsService,
)
from src.storage import (
    DatabaseManager,
    MarketOpportunityAnalysisJob,
    MarketOpportunityCandidate,
    MarketOpportunityRun,
    MarketOpportunityTheme,
)


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


def _page_offset(page: int, limit: int) -> int:
    return (max(1, int(page)) - 1) * max(1, int(limit))


def _repo(db_manager: DatabaseManager) -> MarketOpportunityRepository:
    return MarketOpportunityRepository(db_manager)


def _internal_error(message: str, exc: Exception) -> HTTPException:
    logger.error("%s: %s", message, exc, exc_info=True)
    return HTTPException(
        status_code=500,
        detail={"error": "internal_error", "message": message},
    )


def _paper_reconciliation(job: Dict[str, Any]) -> Dict[str, Any]:
    evidence = job.get("evidence") if isinstance(job.get("evidence"), dict) else {}
    reconciliation = evidence.get("paper_reconciliation") if isinstance(evidence.get("paper_reconciliation"), dict) else {}
    return dict(reconciliation)


def _enrich_model_state_with_reconciliation(state: Dict[str, Any], job: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    item = dict(state)
    reconciliation = _paper_reconciliation(job or {})
    item["paper_reconciliation"] = reconciliation
    plan_item = reconciliation.get("item") if isinstance(reconciliation.get("item"), dict) else None
    plan_state = (
        MarketOpportunityService._paper_plan_state(plan_item)
        if isinstance(plan_item, dict)
        else {}
    )
    item["plan_executable"] = item.get("plan_executable", plan_state.get("executable"))
    item["plan_status"] = item.get("plan_status") or plan_state.get("plan_status")
    item["confirmation_progress"] = item.get("confirmation_progress") or plan_state.get("confirmation_progress")
    evidence = (job or {}).get("evidence") if isinstance((job or {}).get("evidence"), dict) else {}
    item["followup"] = item.get("followup") or (
        evidence.get("followup") if isinstance(evidence.get("followup"), dict) else None
    )
    if item.get("paper_plan_id") is None and reconciliation.get("plan_id") is not None:
        item["paper_plan_id"] = reconciliation.get("plan_id")
    if item.get("decision_signal_id") is None and reconciliation.get("signal_id") is not None:
        item["decision_signal_id"] = reconciliation.get("signal_id")
    if item.get("plan_executable") is not True and item.get("no_plan_reason") is None:
        item["no_plan_reason"] = (
            plan_state.get("no_plan_reason")
            or reconciliation.get("no_plan_reason")
            or reconciliation.get("classification")
            or item.get("failure_reason")
        )
    return item


def _model_states(configured_models: Sequence[str], jobs: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    job_by_model = {str(job.get("model") or ""): job for job in jobs if job.get("model")}
    states: List[Dict[str, Any]] = [
        _enrich_model_state_with_reconciliation(state, job_by_model.get(str(state.get("model") or "")))
        for state in MarketOpportunityService._model_states(list(configured_models), list(jobs))
    ]
    for model, job in job_by_model.items():
        if model in configured_models:
            continue
        extra = MarketOpportunityService._model_states([model], [job])[0]
        states.append(_enrich_model_state_with_reconciliation(extra, job))
    return states


def _candidate_no_plan_reason(
    candidate: Dict[str, Any],
    jobs: Sequence[Dict[str, Any]],
    configured_models: Sequence[str],
) -> Optional[str]:
    status = str(candidate.get("status") or "").strip().lower()
    if status != "selected":
        reasons = candidate.get("rejection_reasons") or []
        if reasons:
            prefix = "candidate_observing" if status == "observing" else "candidate_rejected"
            return prefix + ": " + ", ".join(str(item) for item in reasons[:3])
        return f"candidate_{status or 'not_selected'}"
    return MarketOpportunityService._candidate_no_plan_reason(list(configured_models), list(jobs))


def _theme_source_unavailable(theme: Dict[str, Any]) -> bool:
    quality = theme.get("data_quality") if isinstance(theme.get("data_quality"), dict) else {}
    reasons = [str(item) for item in (theme.get("rejection_reasons") or [])]
    errors = [str(item) for item in (quality.get("errors") or [])]
    markers = reasons + errors + [
        str(quality.get("snapshot_status") or ""),
        str(quality.get("status") or ""),
    ]
    return any(
        marker in {"failed", "constituent_snapshot_failed", "constituents_unavailable", "provider_error"}
        or marker.startswith("provider_error")
        for marker in markers
    )


def _run_health(run: Dict[str, Any], themes: Sequence[Dict[str, Any]] = ()) -> Dict[str, Optional[str]]:
    status = str(run.get("status") or "").strip().lower()
    if status in {"failed", "blocked"}:
        return {"health_status": status, "health_reason": run.get("error_message") or status}
    provenance = run.get("provenance") or {}
    health = provenance.get("observation_health") or {}
    if provenance.get("frozen_membership_replay") is True:
        return {"health_status": "historical_replay", "health_reason": "frozen_membership_no_observation_credit"}
    if health and health.get("healthy") is not True:
        return {"health_status": "source_unavailable", "health_reason": health.get("reason") or "observation_data_incomplete"}
    if provenance.get("analysis_block_reason") in {"market_regime_risk_off", "market_regime_unavailable"}:
        return {"health_status": "risk_blocked", "health_reason": provenance["analysis_block_reason"]}
    selected = int(run.get("selected_candidate_count") or 0)
    observed = int(provenance.get("observation_candidate_count") or 0)
    warming = any((theme.get("score_components") or {}).get("evidence_stage") == "cold_start" for theme in themes)
    if warming:
        return {"health_status": "warming_up", "health_reason": "theme_history_warming_up"}
    if selected > 0:
        return {"health_status": "has_candidates", "health_reason": None}
    if observed > 0:
        return {"health_status": "observing", "health_reason": "analysis_conditions_pending"}
    if themes and any(_theme_source_unavailable(theme) for theme in themes):
        return {"health_status": "source_unavailable", "health_reason": "theme_constituents_unavailable"}
    if int(run.get("theme_count") or 0) == 0:
        return {"health_status": "no_themes", "health_reason": "no_theme_payload"}
    if health.get("healthy") is True:
        return {"health_status": "healthy_empty", "health_reason": "no_selected_candidates"}
    return {"health_status": "zero_candidates", "health_reason": "no_selected_candidates"}


def _enrich_run_health(run: Dict[str, Any], themes: Sequence[Dict[str, Any]] = ()) -> Dict[str, Any]:
    item = dict(run)
    item.update(_run_health(item, themes))
    return item


def _attach_candidate_jobs(
    repo: MarketOpportunityRepository,
    candidates: List[Dict[str, Any]],
    jobs_by_candidate: Dict[int, List[Dict[str, Any]]],
    configured_models: Sequence[str],
) -> List[Dict[str, Any]]:
    del repo
    enriched: List[Dict[str, Any]] = []
    for candidate in candidates:
        candidate_id = int(candidate.get("id") or 0)
        jobs = jobs_by_candidate.get(candidate_id, [])
        item = dict(candidate)
        item["analysis_jobs"] = jobs
        item["model_states"] = _model_states(configured_models, jobs)
        item["no_plan_reason"] = _candidate_no_plan_reason(item, jobs, configured_models)
        enriched.append(item)
    return enriched


def _jobs_for_candidates(
    repo: MarketOpportunityRepository,
    candidate_ids: Iterable[int],
    *,
    model: Optional[str] = None,
) -> Dict[int, List[Dict[str, Any]]]:
    ids = [int(item) for item in candidate_ids if item]
    if not ids:
        return {}
    requested_model = str(model or "").strip()
    with repo.db.session_scope() as session:
        rows = (
            session.query(MarketOpportunityAnalysisJob)
            .filter(MarketOpportunityAnalysisJob.candidate_id.in_(ids))
        )
        if requested_model:
            rows = rows.filter(MarketOpportunityAnalysisJob.model == requested_model)
        rows = rows.order_by(
            MarketOpportunityAnalysisJob.model.asc(),
            MarketOpportunityAnalysisJob.created_at.asc(),
        ).all()
        grouped: Dict[int, List[Dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(int(row.candidate_id), []).append(repo._job_to_dict(row))
        return grouped


def _limits(config: Config) -> Dict[str, Any]:
    return {
        "discovery_max_themes": int(getattr(config, "market_opportunity_discovery_max_themes", 15)),
        "max_observation_candidates": int(getattr(config, "market_opportunity_max_observation_candidates", 30)),
        "max_themes": int(getattr(config, "market_opportunity_max_themes", 3)),
        "max_candidates_per_theme": int(getattr(config, "market_opportunity_max_candidates_per_theme", 3)),
        "max_total_candidates": int(getattr(config, "market_opportunity_max_total_candidates", 8)),
        "max_llm_calls_per_run": int(getattr(config, "market_opportunity_max_llm_calls_per_run", 16)),
        "min_20d_avg_amount_cny": float(getattr(config, "market_opportunity_min_20d_avg_amount_cny", 100000000.0)),
        "blacklist_count": len(getattr(config, "market_opportunity_blacklist", []) or []),
    }


def _opportunity_mode(config: Config) -> str:
    return str(getattr(config, "market_opportunity_mode", "off") or "off").strip().lower()


def _opportunity_budget(config: Config) -> int:
    return int(getattr(config, "market_opportunity_max_llm_calls_per_run", 16) or 0)


def _status_response(
    *,
    payload: Dict[str, Any],
    config: Config,
    fallback_setting: Optional[Any] = None,
) -> MarketOpportunityStatusResponse:
    mode_setting = dict(payload.get("mode_setting") or {})
    if fallback_setting is not None and not mode_setting:
        mode_setting = fallback_setting.to_dict()
    observe_gate = dict(payload.get("observe_gate") or {})
    observe_gate.setdefault("required_trading_days", 3)
    observe_gate.setdefault("completed", False)
    observe_gate.setdefault(
        "message",
        "Observe rollout must complete three trading days before paper enablement is accepted.",
    )
    return MarketOpportunityStatusResponse(
        mode=str(payload.get("mode") or mode_setting.get("value") or _opportunity_mode(config)),
        enabled=bool(payload.get("enabled")),
        mode_source=mode_setting.get("source"),
        mode_version=mode_setting.get("version"),
        mode_updated_at=mode_setting.get("updated_at"),
        mode_updated_by=mode_setting.get("updated_by"),
        mode_degraded=bool(mode_setting.get("degraded")),
        mode_provenance=dict(mode_setting.get("provenance") or {}),
        configured_models=list(payload.get("configured_models") or list_analysis_models(config)),
        limits=dict(payload.get("limits") or _limits(config)),
        observe_gate=observe_gate,
        message=str(payload.get("message") or "disabled"),
    )


def _parse_windows(value: Optional[str]) -> List[int]:
    if not value:
        return [1, 3, 5, 10, 20, 40]
    windows: List[int] = []
    for item in str(value).split(","):
        item = item.strip()
        if not item:
            continue
        try:
            parsed = int(item)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": "invalid_windows", "message": "windows must be comma-separated integers"},
            ) from exc
        if parsed <= 0 or parsed > 60:
            raise HTTPException(
                status_code=400,
                detail={"error": "invalid_windows", "message": "window must be between 1 and 60 trading bars"},
            )
        windows.append(parsed)
    return sorted(set(windows)) or [1, 3, 5, 10, 20, 40]


@router.get("/status", response_model=MarketOpportunityStatusResponse, responses=AUTH_RESPONSE)
def get_market_opportunity_status(
    config: Config = Depends(get_config_dep),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> MarketOpportunityStatusResponse:
    payload = MarketOpportunityService(config=config, db_manager=db_manager).status()
    return _status_response(payload=payload, config=config)


@router.get("/mode", response_model=MarketOpportunityStatusResponse, responses=AUTH_RESPONSE)
def get_market_opportunity_mode(
    config: Config = Depends(get_config_dep),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> MarketOpportunityStatusResponse:
    payload = MarketOpportunityService(config=config, db_manager=db_manager).status()
    return _status_response(payload=payload, config=config)


@router.put(
    "/mode",
    response_model=MarketOpportunityStatusResponse,
    responses={
        **AUTH_RESPONSE,
        400: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
)
def update_market_opportunity_mode(
    request: MarketOpportunityModeUpdateRequest,
    db_manager: DatabaseManager = Depends(get_database_manager),
    config: Config = Depends(get_config_dep),
) -> MarketOpportunityStatusResponse:
    try:
        service = MarketOpportunityService(config=config, db_manager=db_manager)
        runtime_settings = RuntimeSettingsService(config=config, db_manager=db_manager)
        current_setting = runtime_settings.get_market_opportunity_mode()
        if current_setting.version != request.expected_version:
            raise RuntimeSettingConflictError(current_version=current_setting.version)
        if request.mode == "paper":
            gate = service.paper_enablement_gate(market="cn")
            if not gate.get("completed"):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "observe_gate_incomplete",
                        "message": "paper mode requires completed observe rollout",
                        "observe_gate": gate,
                    },
                )
        setting = runtime_settings.set_market_opportunity_mode(
            request.mode,
            expected_version=request.expected_version,
            actor="web-admin",
            reason=request.reason,
        )
        payload = service.status()
        return _status_response(payload=payload, config=config, fallback_setting=setting)
    except RuntimeSettingValidationError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_mode", "message": str(exc)}) from exc
    except RuntimeSettingConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"error": "runtime_setting_version_conflict", "current_version": exc.current_version},
        ) from exc
    except SQLAlchemyError as exc:
        raise _internal_error("Update market opportunity mode failed", exc) from exc


@router.delete(
    "/mode",
    response_model=MarketOpportunityStatusResponse,
    responses={**AUTH_RESPONSE, 409: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def clear_market_opportunity_mode(
    request: MarketOpportunityModeClearRequest,
    db_manager: DatabaseManager = Depends(get_database_manager),
    config: Config = Depends(get_config_dep),
) -> MarketOpportunityStatusResponse:
    try:
        setting = RuntimeSettingsService(config=config, db_manager=db_manager).clear_market_opportunity_mode(
            expected_version=request.expected_version,
            actor="web-admin",
            reason=request.reason,
        )
        payload = MarketOpportunityService(config=config, db_manager=db_manager).status()
        return _status_response(payload=payload, config=config, fallback_setting=setting)
    except RuntimeSettingConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"error": "runtime_setting_version_conflict", "current_version": exc.current_version},
        ) from exc
    except SQLAlchemyError as exc:
        raise _internal_error("Clear market opportunity mode failed", exc) from exc


@router.get("/runs", response_model=MarketOpportunityRunListResponse, responses={**AUTH_RESPONSE, 500: {"model": ErrorResponse}})
def list_runs(
    market: Optional[str] = Query(None, max_length=16),
    mode: Optional[str] = Query(None, max_length=16),
    status: Optional[str] = Query(None, max_length=32),
    trade_date_from: Optional[date] = Query(None),
    trade_date_to: Optional[date] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=200),
    db_manager: DatabaseManager = Depends(get_database_manager),
    config: Config = Depends(get_config_dep),
) -> MarketOpportunityRunListResponse:
    try:
        repo = _repo(db_manager)
        with repo.db.session_scope() as session:
            query = session.query(MarketOpportunityRun)
            if market:
                query = query.filter(MarketOpportunityRun.market == market)
            if mode:
                query = query.filter(MarketOpportunityRun.mode == mode)
            if status:
                query = query.filter(MarketOpportunityRun.status == status)
            if trade_date_from:
                query = query.filter(MarketOpportunityRun.trade_date >= trade_date_from)
            if trade_date_to:
                query = query.filter(MarketOpportunityRun.trade_date <= trade_date_to)
            total = query.count()
            rows = (
                query.order_by(desc(MarketOpportunityRun.trade_date), desc(MarketOpportunityRun.created_at))
                .offset(_page_offset(page, limit))
                .limit(limit)
                .all()
            )
            run_ids = [int(row.id) for row in rows]
            themes_by_run: Dict[int, List[Dict[str, Any]]] = {run_id: [] for run_id in run_ids}
            if run_ids:
                theme_rows = (
                    session.query(MarketOpportunityTheme)
                    .filter(MarketOpportunityTheme.run_id.in_(run_ids))
                    .all()
                )
                for theme in theme_rows:
                    themes_by_run.setdefault(int(theme.run_id), []).append(repo._theme_to_dict(theme))
            service = MarketOpportunityService(config=config, db_manager=db_manager, repository=repo)
            observation_health = {
                run_market: service._board_observations().observation_health_by_date(run_market)
                for run_market in {row.market for row in rows}
            }
            items = [
                MarketOpportunityRunItem(**_enrich_run_health(
                    service.project_observation_health(repo._run_to_dict(row), health_by_date=observation_health[row.market]),
                    themes_by_run.get(int(row.id), []),
                ))
                for row in rows
            ]
        return MarketOpportunityRunListResponse(
            total=total,
            page=page,
            limit=limit,
            items=items,
            configured_models=list_analysis_models(config),
        )
    except SQLAlchemyError as exc:
        raise _internal_error("List market opportunity runs failed", exc) from exc


@router.get(
    "/runs/{run_id}",
    response_model=MarketOpportunityRunDetailResponse,
    responses={**AUTH_RESPONSE, 404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def get_run_detail(
    run_id: int,
    db_manager: DatabaseManager = Depends(get_database_manager),
    config: Config = Depends(get_config_dep),
) -> MarketOpportunityRunDetailResponse:
    try:
        repo = _repo(db_manager)
        payload = repo.get_run_detail(run_id)
        if payload is None:
            raise HTTPException(status_code=404, detail={"error": "not_found", "message": "机会池运行不存在"})
        configured_models = list_analysis_models(config)
        jobs_by_candidate: Dict[int, List[Dict[str, Any]]] = {}
        for job in payload.get("analysis_jobs", []):
            jobs_by_candidate.setdefault(int(job.get("candidate_id") or 0), []).append(job)
        payload["candidates"] = _attach_candidate_jobs(
            repo,
            list(payload.get("candidates") or []),
            jobs_by_candidate,
            configured_models,
        )
        payload = MarketOpportunityService(config=config, db_manager=db_manager, repository=repo).project_observation_health(
            payload, themes=payload.get("themes", []), candidates=payload["candidates"],
        )
        payload.update(_run_health(payload, list(payload.get("themes") or [])))
        payload["configured_models"] = configured_models
        payload["llm_submitted_job_count"] = sum(
            1 for job in payload.get("analysis_jobs") or [] if MarketOpportunityService._job_reached_llm(job)
        )
        return MarketOpportunityRunDetailResponse(**payload)
    except HTTPException:
        raise
    except SQLAlchemyError as exc:
        raise _internal_error("Get market opportunity run failed", exc) from exc


@router.get("/themes", response_model=MarketOpportunityThemeListResponse, responses={**AUTH_RESPONSE, 500: {"model": ErrorResponse}})
def list_themes(
    run_id: Optional[int] = Query(None, ge=1),
    theme_type: Optional[str] = Query(None, max_length=16),
    status: Optional[str] = Query(None, max_length=32),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=200),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> MarketOpportunityThemeListResponse:
    try:
        repo = _repo(db_manager)
        with repo.db.session_scope() as session:
            query = session.query(MarketOpportunityTheme)
            if run_id:
                query = query.filter(MarketOpportunityTheme.run_id == run_id)
            if theme_type:
                query = query.filter(MarketOpportunityTheme.theme_type == theme_type)
            if status:
                query = query.filter(MarketOpportunityTheme.status == status)
            total = query.count()
            rows = (
                query.order_by(desc(MarketOpportunityTheme.created_at), MarketOpportunityTheme.rank.asc())
                .offset(_page_offset(page, limit))
                .limit(limit)
                .all()
            )
            items = [MarketOpportunityThemeItem(**repo._theme_to_dict(row)) for row in rows]
        return MarketOpportunityThemeListResponse(total=total, page=page, limit=limit, items=items)
    except SQLAlchemyError as exc:
        raise _internal_error("List market opportunity themes failed", exc) from exc


@router.get("/candidates", response_model=MarketOpportunityCandidateListResponse, responses={**AUTH_RESPONSE, 500: {"model": ErrorResponse}})
def list_candidates(
    run_id: Optional[int] = Query(None, ge=1),
    market: Optional[str] = Query(None, max_length=16),
    status: Optional[str] = Query(None, max_length=32),
    model: Optional[str] = Query(None, max_length=255),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=200),
    db_manager: DatabaseManager = Depends(get_database_manager),
    config: Config = Depends(get_config_dep),
) -> MarketOpportunityCandidateListResponse:
    try:
        repo = _repo(db_manager)
        candidate_page = repo.list_candidates_page(
            run_id=run_id, market=market, status=status, page=page, limit=limit,
        )
        total = candidate_page["total"]
        candidates = candidate_page["items"]
        configured_models = [model] if model else list_analysis_models(config)
        jobs_by_candidate = _jobs_for_candidates(repo, [item.get("id") for item in candidates], model=model)
        items = [
            MarketOpportunityCandidateItem(**item)
            for item in _attach_candidate_jobs(repo, candidates, jobs_by_candidate, configured_models)
        ]
        return MarketOpportunityCandidateListResponse(
            total=total,
            page=page,
            limit=limit,
            items=items,
            configured_models=configured_models,
        )
    except SQLAlchemyError as exc:
        raise _internal_error("List market opportunity candidates failed", exc) from exc


@router.get("/analysis-jobs", response_model=MarketOpportunityAnalysisJobListResponse, responses={**AUTH_RESPONSE, 500: {"model": ErrorResponse}})
def list_analysis_jobs(
    run_id: Optional[int] = Query(None, ge=1),
    candidate_id: Optional[int] = Query(None, ge=1),
    model: Optional[str] = Query(None, max_length=255),
    status: Optional[str] = Query(None, max_length=32),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=200),
    db_manager: DatabaseManager = Depends(get_database_manager),
    config: Config = Depends(get_config_dep),
) -> MarketOpportunityAnalysisJobListResponse:
    try:
        repo = _repo(db_manager)
        with repo.db.session_scope() as session:
            query = session.query(MarketOpportunityAnalysisJob)
            if run_id:
                query = query.filter(MarketOpportunityAnalysisJob.run_id == run_id)
            if candidate_id:
                query = query.filter(MarketOpportunityAnalysisJob.candidate_id == candidate_id)
            if model:
                query = query.filter(MarketOpportunityAnalysisJob.model == model)
            if status:
                query = query.filter(MarketOpportunityAnalysisJob.status == status)
            total = query.count()
            rows = (
                query.order_by(desc(MarketOpportunityAnalysisJob.created_at), MarketOpportunityAnalysisJob.id.asc())
                .offset(_page_offset(page, limit))
                .limit(limit)
                .all()
            )
            items = [MarketOpportunityAnalysisJobItem(**repo._job_to_dict(row)) for row in rows]
        return MarketOpportunityAnalysisJobListResponse(
            total=total,
            page=page,
            limit=limit,
            items=items,
            configured_models=list_analysis_models(config),
        )
    except SQLAlchemyError as exc:
        raise _internal_error("List market opportunity analysis jobs failed", exc) from exc


@router.get(
    "/evaluation",
    response_model=MarketOpportunityEvaluationResponse,
    responses={**AUTH_RESPONSE, 400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def get_market_opportunity_evaluation(
    run_id: Optional[int] = Query(None, ge=1),
    market: Optional[str] = Query(None, max_length=16),
    windows: Optional[str] = Query("1,3,5,10,20,40", max_length=64),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> MarketOpportunityEvaluationResponse:
    try:
        payload = MarketOpportunityEvaluationService(db_manager=db_manager).evaluate(
            run_id=run_id,
            market=market,
            windows=_parse_windows(windows),
        )
        return MarketOpportunityEvaluationResponse(**payload)
    except HTTPException:
        raise
    except SQLAlchemyError as exc:
        raise _internal_error("Evaluate market opportunities failed", exc) from exc


@router.post(
    "/discover",
    response_model=MarketOpportunityActionResponse,
    responses={**AUTH_RESPONSE, 400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def discover_market_opportunities(
    request: MarketOpportunityDiscoverRequest,
    config: Config = Depends(get_config_dep),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> MarketOpportunityActionResponse:
    try:
        mode = RuntimeSettingsService(config=config, db_manager=db_manager).get_market_opportunity_mode().value
        if mode == "off":
            raise HTTPException(
                status_code=400,
                detail={"error": "market_opportunity_disabled", "message": "market opportunity mode=off"},
            )
        service = MarketOpportunityService(config=config, db_manager=db_manager)
        if request.report_id:
            result = service.discover_from_report(report_id=request.report_id, market=request.market)
        else:
            result = service.discover_from_latest_market_review(market=request.market)
        return MarketOpportunityActionResponse(status=str(result.get("status", "unknown")), result=result)
    except SQLAlchemyError as exc:
        raise _internal_error("Discover market opportunities failed", exc) from exc


@router.post(
    "/runs/{run_id}/analyze",
    response_model=MarketOpportunityActionResponse,
    responses={**AUTH_RESPONSE, 400: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}},
)
def analyze_market_opportunity_run(
    run_id: int,
    request: MarketOpportunityJobRunRequest,
    config: Config = Depends(get_config_dep),
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> MarketOpportunityActionResponse:
    try:
        mode = RuntimeSettingsService(config=config, db_manager=db_manager).get_market_opportunity_mode().value
        if mode != "paper":
            raise HTTPException(
                status_code=400,
                detail={"error": "market_opportunity_not_paper", "message": "候选分析仅在 market opportunity mode=paper 时启用"},
            )
        configured_models = list_analysis_models(config)
        if not configured_models:
            raise HTTPException(
                status_code=400,
                detail={"error": "configured_models_missing", "message": "未配置可用于机会池分析的模型"},
            )
        budget = _opportunity_budget(config)
        if budget <= 0:
            raise HTTPException(
                status_code=400,
                detail={"error": "llm_budget_disabled", "message": "MARKET_OPPORTUNITY_MAX_LLM_CALLS_PER_RUN must be > 0"},
            )
        result = MarketOpportunityService(config=config, db_manager=db_manager).analyze_run(
            run_id,
            owner=request.owner,
            limit=min(int(request.limit), budget),
        )
        if result.get("status") == "not_found":
            raise HTTPException(status_code=404, detail={"error": "not_found", "message": "机会池运行不存在"})
        return MarketOpportunityActionResponse(status=str(result.get("status", "unknown")), result=result)
    except HTTPException:
        raise
    except SQLAlchemyError as exc:
        raise _internal_error("Run market opportunity analyses failed", exc) from exc
