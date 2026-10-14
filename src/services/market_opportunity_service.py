# -*- coding: utf-8 -*-
"""Market opportunity discovery, deterministic scoring, and analysis jobs."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from functools import lru_cache
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from sqlalchemy import desc

from data_provider import DataFetcherManager
from data_provider.akshare_board_provider import (
    BoardConstituent,
    BoardConstituentSnapshot,
    BoardHistorySnapshot,
)
from data_provider.board_provider import CompositeBoardConstituentProvider
from data_provider.base import normalize_stock_code
from src.config import Config, get_config, normalize_market_opportunity_style
from src.repositories.market_opportunity_repository import MarketOpportunityRepository
from src.services.analysis_model_service import list_analysis_models
from src.services.board_observation_service import (
    BoardObservationService, BOARD_DISCOVERY_POLICY_VERSION, OBSERVATION_HEALTH_POLICY_VERSION,
)
from src.services.market_opportunity_followup_service import (
    RESEARCH_REVIEW_POLICY_VERSION,
    SELECTION_CONTEXT_VERSION,
    build_followup_obligation,
    build_research_review_obligation,
    build_selection_context,
    followup_state_for_session,
)
from src.services.runtime_settings_service import RuntimeSettingsService
from src.storage import (
    AnalysisHistory,
    DailyMarketRegime,
    DatabaseManager,
    MARKET_OPPORTUNITY_FOLLOWUP_RUN_KEY_PREFIX,
    MARKET_OPPORTUNITY_RESEARCH_JOB_KEY_PREFIX,
    MARKET_OPPORTUNITY_RESEARCH_REVIEW_RUN_KEY_PREFIX,
    MarketOpportunityAnalysisJob,
    MarketOpportunityRun,
    REPORT_EXECUTION_INTENT_PAPER_CANDIDATE,
    REPORT_EXECUTION_INTENT_RESEARCH_ONLY,
    REPORT_EXECUTION_POLICY_VERSION,
    REPORT_EXECUTION_SCOPE_PRIMARY,
    is_market_opportunity_research_job_key,
    report_execution_intent_clause,
    utc_naive_now,
)

logger = logging.getLogger(__name__)

OPPORTUNITY_POLICY_VERSION = "opportunity-policy-v4"
THEME_POLICY_VERSION = "theme-policy-v4"
CANDIDATE_POLICY_VERSION = "candidate-policy-v4"
RESEARCH_BUNDLE_VERSION = "opportunity-research-bundle-v1"
RESEARCH_JOB_SOURCE = "market_opportunity_research"
RESEARCH_REVIEW_SOURCE = "opportunity_research_review"
VALID_MODES = {"off", "observe", "paper"}
# 研究任务不经过主账户对账；该结构与模拟交易服务拒绝 research_only 报告时的分类一致。
RESEARCH_ONLY_RECONCILIATION = {
    "status": "skipped",
    "classification": "execution_not_authorized",
    "reason": "research_only",
    "no_plan_reason": "research_only",
    "executable": False,
}
# 研究任务结果分别计数：看多、观望、分析失败、预算用尽、所需数据不可用、错过会话、未结束。
RESEARCH_OUTCOMES = (
    "bullish", "non_bullish", "analysis_failed", "budget_exhausted", "data_unavailable", "expired", "in_progress",
)


@dataclass(frozen=True)
class OpportunityPolicy:
    mode: str
    mode_source: str
    mode_version: Optional[int]
    max_themes: int
    max_candidates_per_theme: int
    max_total_candidates: int
    max_llm_calls_per_run: int
    min_20d_avg_amount_cny: float
    blacklist: Tuple[str, ...]
    observe_gate_days: int
    discovery_max_themes: int = 15
    max_observation_candidates: int = 30
    research_daily_llm_budget: int = 16
    risk_off_max_candidates: int = 2
    risk_off_min_score: int = 80


@lru_cache(maxsize=1)
def _static_prompt_digest() -> Tuple[str, str]:
    """Hash the static analysis prompts once; dynamic sections are declared, not hashed."""

    from src.analyzer import ANALYSIS_REPORT_TOOL_SCHEMA_VERSION, GeminiAnalyzer

    digest = hashlib.sha256()
    for prompt in (GeminiAnalyzer.SYSTEM_PROMPT, GeminiAnalyzer.LEGACY_DEFAULT_SYSTEM_PROMPT):
        digest.update(str(prompt).encode("utf-8"))
        digest.update(b"\0")
    return str(ANALYSIS_REPORT_TOOL_SCHEMA_VERSION), digest.hexdigest()


def _static_prompt_fingerprint() -> Dict[str, Any]:
    tool_schema_version, prompt_sha256 = _static_prompt_digest()
    return {
        "tool_schema_version": tool_schema_version,
        "static_system_prompt_sha256": prompt_sha256,
        "limitations": ["dynamic_skill_and_style_sections_not_hashed"],
    }


class MarketOpportunityService:
    """Build a bounded opportunity pool from a complete post-market review."""

    def __init__(
        self,
        *,
        config: Optional[Config] = None,
        db_manager: Optional[DatabaseManager] = None,
        fetcher_manager: Optional[DataFetcherManager] = None,
        board_provider: Optional[Any] = None,
        repository: Optional[MarketOpportunityRepository] = None,
        analysis_service_factory: Optional[Any] = None,
        paper_trading_service_factory: Optional[Any] = None,
        runtime_settings_service: Optional[RuntimeSettingsService] = None,
        board_observation_service: Optional[Any] = None,
    ) -> None:
        self._frozen_constituents: Dict[Tuple[Any, ...], BoardConstituentSnapshot] = {}
        self.config = config or get_config()
        self.db = db_manager or DatabaseManager.get_instance()
        self.fetcher_manager = fetcher_manager or DataFetcherManager()
        self.board_provider = board_provider or CompositeBoardConstituentProvider()
        self.repo = repository or MarketOpportunityRepository(self.db)
        self.analysis_service_factory = analysis_service_factory
        self.paper_trading_service_factory = paper_trading_service_factory
        self.board_observation_service = board_observation_service
        self._daily_bar_cache: Dict[Tuple[str, date, int], Optional[pd.DataFrame]] = {}
        self.runtime_settings_service = runtime_settings_service or RuntimeSettingsService(
            config=self.config,
            db_manager=self.db,
        )

    def _board_observations(self) -> Any:
        if self.board_observation_service is None:
            self.board_observation_service = BoardObservationService(
                config=self.config, db_manager=self.db, fetcher_manager=self.fetcher_manager,
            )
        return self.board_observation_service

    def run_after_market_review(
        self,
        *,
        market: str = "cn",
        current_time: Optional[datetime] = None,
        owner: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Discover opportunities from the latest complete market review and consume jobs.

        This is the scheduler-facing entry point.  It intentionally creates normal
        stock analysis reports first, then lets the existing paper-trading
        reconciliation decide whether any report becomes a simulated plan.
        """

        scheduler_session = self._current_effective_session(
            market=market,
            current_time=current_time,
        )
        submitted_recovery = (
            self._recover_submitted_jobs(
                run_id=None,
                limit=200,
                effective_session=scheduler_session,
            )
            if scheduler_session is not None
            else {"processed": 0, "failed": 0, "uncertain": 0, "skipped_reason": "current_session_unavailable"}
        )
        new_work_gate = self._automatic_new_work_gate(market=market, current_time=current_time)
        if not new_work_gate.get("allowed"):
            status = (
                "ok"
                if (
                    submitted_recovery.get("processed")
                    or submitted_recovery.get("failed")
                    or submitted_recovery.get("uncertain")
                )
                else "skipped"
            )
            skipped = {
                "status": "skipped",
                "reason": new_work_gate.get("reason"),
                "market_phase": new_work_gate,
            }
            return {
                "status": status,
                "discovery": skipped,
                "submitted_recovery": submitted_recovery,
                "followups": {**skipped, "runs": [], "expired": 0, "jobs": {"processed": 0, "failed": 0, "uncertain_submitted": 0}},
                "research_reviews": {**skipped, "runs": [], "expired": 0, "jobs": {"processed": 0, "failed": 0, "uncertain_submitted": 0}},
                "research_outcomes": self._research_outcome_counts([]),
                "jobs": {"processed": 0, "failed": 0, "uncertain_submitted": 0},
                "new_work_gate": new_work_gate,
            }
        observation = None
        if scheduler_session is not None and self._policy().mode != "off":
            observation = self._board_observations().collect(market=market, trade_date=scheduler_session)
        discovery = self.discover_from_latest_market_review(
            market=market,
            current_time=current_time,
        )
        evaluation_history = {"status": "skipped", "reason": "mode_off_or_session_unavailable"}
        if scheduler_session is not None and self._policy().mode != "off":
            from src.services.market_opportunity_history_service import MarketOpportunityHistoryService
            try:
                evaluation_history = MarketOpportunityHistoryService(
                    db_manager=self.db, fetcher_manager=self.fetcher_manager,
                ).refresh(market=market, trade_date=scheduler_session)
            except Exception as exc:
                logger.warning("Opportunity evaluation history refresh failed: %s", exc)
                evaluation_history = {"status": "failed", "reason": type(exc).__name__}
        followups = self._materialize_due_followup_runs(
            market=market,
            current_time=current_time,
        )
        followup_jobs = {"processed": 0, "failed": 0, "uncertain_submitted": 0}
        for followup in followups.get("runs", []):
            outcome = self.process_pending_jobs(
                owner=owner,
                limit=int(followup.get("job_count") or 0),
                run_id=int(followup["run_id"]),
                effective_session=self._parse_date(followup.get("trade_date")),
            )
            for key in ("processed", "failed", "uncertain_submitted"):
                followup_jobs[key] += int(outcome.get(key) or 0)

        # 研究复核在新候选之前消费；其预算名额已在前一会话预留。
        research_reviews = self._materialize_due_research_reviews(
            market=market,
            current_time=current_time,
        )
        review_jobs = {"processed": 0, "failed": 0, "uncertain_submitted": 0}
        for review in research_reviews.get("runs", []):
            outcome = self.process_pending_jobs(
                owner=owner,
                limit=int(review.get("job_count") or 0),
                run_id=int(review["run_id"]),
                effective_session=self._parse_date(review.get("trade_date")),
            )
            for key in ("processed", "failed", "uncertain_submitted"):
                review_jobs[key] += int(outcome.get(key) or 0)

        jobs = {"processed": 0, "failed": 0, "uncertain_submitted": 0}
        if discovery.get("status") in {"analyzing", "ready"}:
            limit = int(discovery.get("job_count") or 0)
            if limit > 0:
                jobs = self.process_pending_jobs(
                    owner=owner,
                    limit=limit,
                    run_id=int(discovery["run_id"]) if discovery.get("run_id") is not None else None,
                    effective_session=self._parse_date(discovery.get("trade_date")),
                )
        overall_status = (
            "ok"
            if (
                followup_jobs.get("processed")
                or review_jobs.get("processed")
                or jobs.get("processed")
                or submitted_recovery.get("processed")
                or submitted_recovery.get("failed")
            )
            else discovery.get("status")
        )
        # 本轮触及的研究任务（当日发现 + 到期复核）按结果分别计数，网络失败与观望不混同。
        research_outcomes = self._research_outcome_counts(
            [discovery.get("run_id"), *(item.get("run_id") for item in research_reviews.get("runs", []))]
        )
        return {
            "status": overall_status,
            "board_observation": observation,
            "evaluation_history": evaluation_history,
            "discovery": discovery,
            "submitted_recovery": submitted_recovery,
            "followups": {**followups, "jobs": followup_jobs},
            "research_reviews": {**research_reviews, "jobs": review_jobs},
            "research_outcomes": research_outcomes,
            "jobs": jobs,
        }

    def _materialize_due_followup_runs(
        self,
        *,
        market: str,
        current_time: Optional[datetime],
    ) -> Dict[str, Any]:
        """Turn first-session confirmation obligations into one-day jobs.

        The obligation lives on the completed origin job.  A deterministic run
        key makes scheduler retries idempotent, and the copied candidate expires
        at the end of the due session so a missed day can never be replayed late.
        """

        return self._materialize_due_obligation_runs(
            market=market,
            current_time=current_time,
            evidence_key="followup",
            close_expired=self._close_expired_followup,
            describe=self._describe_followup_run,
        )

    def _materialize_due_research_reviews(
        self,
        *,
        market: str,
        current_time: Optional[datetime],
    ) -> Dict[str, Any]:
        """Turn bullish research outcomes into one research-only review on the next session.

        The review re-analyses fresh data under the same one-day expiry as a
        follow-up; it never becomes a primary-account confirmation.
        """

        return self._materialize_due_obligation_runs(
            market=market,
            current_time=current_time,
            evidence_key="research_review",
            close_expired=self._close_expired_research_review,
            describe=self._describe_research_review_run,
        )

    def _describe_followup_run(
        self,
        *,
        origin_job: Mapping[str, Any],
        origin_candidate: Any,
        model: str,
        effective_session: date,
        obligation: Mapping[str, Any],
    ) -> Optional[Dict[str, Any]]:
        run_key = self._stable_hash({
            "type": "opportunity_confirmation_followup",
            "origin_job_id": int(origin_job["id"]),
            "model": model,
            "trade_date": effective_session.isoformat(),
            "policy": OPPORTUNITY_POLICY_VERSION,
        })
        return {
            "run_key": f"{MARKET_OPPORTUNITY_FOLLOWUP_RUN_KEY_PREFIX}{run_key}",
            "provenance": {
                "source": "opportunity_confirmation_followup",
                "origin_job_id": int(origin_job["id"]),
                "origin_candidate_id": int(origin_candidate.id),
                "origin_report_id": origin_job.get("analysis_history_id"),
                "due_date": effective_session.isoformat(),
            },
            "data_quality": {
                "source": "opportunity_confirmation_followup",
                "followup_reanalysis": True,
                "origin_job_id": int(origin_job["id"]),
                "origin_candidate_id": int(origin_candidate.id),
                "origin_snapshot_hash": origin_candidate.snapshot_hash,
                "as_of": effective_session.isoformat(),
            },
            "snapshot_hash": self._stable_hash({
                "origin": origin_candidate.snapshot_hash,
                "date": effective_session.isoformat(),
                "model": model,
            }),
            "job_key": f"market-opportunity-followup:{int(origin_job['id'])}:{model}:{effective_session.isoformat()}",
            "job_evidence": {
                "source": "opportunity_confirmation_followup",
                "origin_job_id": int(origin_job["id"]),
                "origin_report_id": origin_job.get("analysis_history_id"),
            },
        }

    def _describe_research_review_run(
        self,
        *,
        origin_job: Mapping[str, Any],
        origin_candidate: Any,
        model: str,
        effective_session: date,
        obligation: Mapping[str, Any],
    ) -> Optional[Dict[str, Any]]:
        # 同一候选/模型/原会话共用一个复核身份，重复发现只会产生一次复核调用。
        identity = str(obligation.get("identity") or "")
        if not identity:
            return None
        origin_ids = {
            "origin_job_id": int(origin_job["id"]),
            "origin_report_id": origin_job.get("analysis_history_id"),
            "origin_candidate_id": int(origin_candidate.id),
        }
        return {
            "run_key": f"{MARKET_OPPORTUNITY_RESEARCH_REVIEW_RUN_KEY_PREFIX}{identity}",
            "provenance": {
                "source": RESEARCH_REVIEW_SOURCE,
                "execution_intent": REPORT_EXECUTION_INTENT_RESEARCH_ONLY,
                **origin_ids,
                "due_date": effective_session.isoformat(),
                "policy_version": RESEARCH_REVIEW_POLICY_VERSION,
            },
            "data_quality": {
                "source": RESEARCH_REVIEW_SOURCE,
                "research_review": True,
                "origin_job_id": int(origin_job["id"]),
                "origin_candidate_id": int(origin_candidate.id),
                "origin_snapshot_hash": origin_candidate.snapshot_hash,
                "as_of": effective_session.isoformat(),
            },
            "snapshot_hash": self._stable_hash({
                "origin": origin_candidate.snapshot_hash,
                "date": effective_session.isoformat(),
                "model": model,
                "source": RESEARCH_REVIEW_SOURCE,
            }),
            "job_key": f"{MARKET_OPPORTUNITY_RESEARCH_JOB_KEY_PREFIX}review:{identity}",
            "job_evidence": {
                "source": RESEARCH_JOB_SOURCE,
                "execution_intent": REPORT_EXECUTION_INTENT_RESEARCH_ONLY,
                "research_kind": "review",
                "research_review_of": origin_ids,
            },
        }

    def _materialize_due_obligation_runs(
        self,
        *,
        market: str,
        current_time: Optional[datetime],
        evidence_key: str,
        close_expired: Callable[..., None],
        describe: Callable[..., Optional[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Materialize next-session obligations stored under ``evidence[evidence_key]``."""

        policy = self._policy()
        if policy.mode != "paper":
            return {"status": "disabled", "runs": [], "expired": 0}
        report = self._latest_market_review(market=market)
        if report is None:
            return {"status": "blocked", "reason": "market_review_not_found", "runs": [], "expired": 0}
        payload = self._extract_market_payload(report)
        effective_session = self._payload_trade_date(payload) or self._report_trade_date(report)
        gate = self._postmarket_report_gate(
            report=report,
            payload=payload,
            market=market,
            current_time=current_time,
        )
        if not gate.get("ok"):
            return {
                "status": "blocked",
                "reason": str(gate.get("reason") or "postmarket_report_gate_failed"),
                "runs": [],
                "expired": 0,
            }

        due_jobs: List[Dict[str, Any]] = []
        expired = 0
        page = 1
        while True:
            batch = self.repo.list_jobs_page(status="completed", page=page, limit=200)
            for row in batch.get("items", []):
                evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
                obligation = evidence.get(evidence_key) if isinstance(evidence.get(evidence_key), dict) else None
                if not obligation or str(obligation.get("status") or "scheduled") not in {"scheduled", "spawned"}:
                    continue
                state = followup_state_for_session(obligation, effective_session=effective_session)
                if state == "expired":
                    expired += 1
                    close_expired(
                        origin_job=row,
                        obligation=obligation,
                        effective_session=effective_session,
                    )
                    continue
                if state == "due":
                    due_jobs.append(row)
            if page * int(batch.get("limit") or 200) >= int(batch.get("total") or 0):
                break
            page += 1

        regime = self._latest_regime(market=market, trade_date=effective_session)
        created_runs: List[Dict[str, Any]] = []
        for origin_job in due_jobs:
            origin_candidate = self.repo.get_candidate(int(origin_job["candidate_id"]))
            if origin_candidate is None:
                continue
            model = str(origin_job.get("model") or "")
            if not model:
                continue
            obligation = dict(origin_job.get("evidence", {}).get(evidence_key) or {})
            spec = describe(
                origin_job=origin_job,
                origin_candidate=origin_candidate,
                model=model,
                effective_session=effective_session,
                obligation=obligation,
            )
            if spec is None:
                continue
            run = self.repo.create_or_get_run({
                "run_key": spec["run_key"],
                "market": market,
                "trade_date": effective_session,
                "source_report_id": int(report.id),
                "market_regime_id": getattr(regime, "id", None),
                "market_regime": str(getattr(regime, "regime", None) or "unavailable"),
                "mode": "paper",
                "status": "analyzing",
                "policy_version": OPPORTUNITY_POLICY_VERSION,
                "config_snapshot": self._config_snapshot(policy),
                "provenance": spec["provenance"],
                "llm_budget": 1,
                "started_at": utc_naive_now(),
            })
            existing = self._existing_run_child_summary(int(run.id))
            if not existing.get("has_children"):
                def parsed_list(value: Any) -> List[Any]:
                    if isinstance(value, list):
                        return list(value)
                    try:
                        parsed = json.loads(value or "[]")
                        return list(parsed) if isinstance(parsed, list) else []
                    except (TypeError, ValueError):
                        return []

                candidate_key = str(origin_candidate.candidate_key)
                candidate = {
                    "candidate_key": candidate_key,
                    "market": str(origin_candidate.market or market),
                    "exchange": str(origin_candidate.exchange),
                    "stock_code": str(origin_candidate.stock_code),
                    "symbol": str(origin_candidate.symbol),
                    "stock_name": origin_candidate.stock_name,
                    "primary_theme_key": origin_candidate.primary_theme_key,
                    "related_themes": parsed_list(origin_candidate.related_themes_json),
                    "status": "selected",
                    "rank": 1,
                    "score": None,
                    "score_components": {
                        "origin_score": origin_candidate.score,
                        "origin_components": self._json_dict(origin_candidate.score_components_json),
                        "current_session_rescoring": "delegated_to_full_llm_reanalysis",
                    },
                    "rejection_reasons": [],
                    "data_quality": spec["data_quality"],
                    "quote_snapshot": {},
                    "as_of": effective_session,
                    "valid_until": effective_session,
                    "snapshot_hash": spec["snapshot_hash"],
                }
                self.repo.replace_run_children(
                    run_id=int(run.id),
                    themes=[],
                    candidates=[candidate],
                    jobs=[{
                        "candidate_key": candidate_key,
                        "job_key": spec["job_key"],
                        "model": model,
                        "status": "pending",
                        "max_attempts": 2,
                        "evidence": spec["job_evidence"],
                    }],
                )
                self.repo.update_run(
                    int(run.id),
                    status="analyzing",
                    theme_count=0,
                    selected_theme_count=0,
                    candidate_count=1,
                    selected_candidate_count=1,
                    completed_at=None,
                )
            self.repo.patch_job_evidence(
                int(origin_job["id"]),
                {evidence_key: {
                    **obligation,
                    "status": "spawned",
                    "spawned_run_id": int(run.id),
                    "spawned_on": effective_session.isoformat(),
                }},
            )
            summary = self._existing_run_child_summary(int(run.id))
            created_runs.append({
                "run_id": int(run.id),
                "trade_date": effective_session.isoformat(),
                "job_count": int(summary.get("claimable_job_count") or 0),
                "origin_job_id": int(origin_job["id"]),
            })
        return {
            "status": "ready" if created_runs else "none_due",
            "trade_date": effective_session.isoformat(),
            "runs": created_runs,
            "expired": expired,
        }

    def _close_expired_followup(
        self,
        *,
        origin_job: Mapping[str, Any],
        obligation: Mapping[str, Any],
        effective_session: date,
    ) -> None:
        """Close a missed follow-up and never leave a spawned obligation stuck.

        A completed child may exist if the process crashed after the child job
        committed but before the origin evidence was patched. In that case the
        child reconciliation is authoritative. Otherwise all unresolved child
        jobs are failed and the entry remains frozen; they cannot be recovered
        on a later session.
        """

        def completed_result(evidence: Mapping[str, Any]) -> Dict[str, Any]:
            reconciliation = (
                evidence.get("paper_reconciliation")
                if isinstance(evidence.get("paper_reconciliation"), Mapping)
                else {}
            )
            item = reconciliation.get("item") if isinstance(reconciliation.get("item"), Mapping) else None
            plan_state = self._paper_plan_state(item) if isinstance(item, Mapping) else {
                "executable": reconciliation.get("executable"),
                "no_plan_reason": reconciliation.get("no_plan_reason") or reconciliation.get("classification"),
            }
            return {
                "result": "confirmed" if plan_state.get("executable") is True else "frozen",
                "reason": plan_state.get("no_plan_reason") or reconciliation.get("classification"),
            }

        self._close_expired_obligation(
            origin_job=origin_job,
            obligation=obligation,
            effective_session=effective_session,
            evidence_key="followup",
            expiry_reason="followup_not_completed_by_expiry",
            expired_result="frozen",
            completed_result=completed_result,
        )

    def _close_expired_research_review(
        self,
        *,
        origin_job: Mapping[str, Any],
        obligation: Mapping[str, Any],
        effective_session: date,
    ) -> None:
        """Close a missed research review; a late session never runs it."""

        def completed_result(evidence: Mapping[str, Any]) -> Dict[str, Any]:
            bundle = evidence.get("research_bundle") if isinstance(evidence.get("research_bundle"), Mapping) else {}
            outcome = bundle.get("outcome") if isinstance(bundle.get("outcome"), Mapping) else {}
            if evidence.get("recovered_after_expiry"):
                return {
                    "status": "expired",
                    "result": "expired",
                    "reason": "submitted_job_session_expired_after_report",
                }
            return {
                "result": str(outcome.get("classification") or "failed"),
                "bundle_id": bundle.get("bundle_id"),
            }

        self._close_expired_obligation(
            origin_job=origin_job,
            obligation=obligation,
            effective_session=effective_session,
            evidence_key="research_review",
            expiry_reason="research_review_not_completed_by_expiry",
            expired_result="expired",
            completed_result=completed_result,
        )

    def _close_expired_obligation(
        self,
        *,
        origin_job: Mapping[str, Any],
        obligation: Mapping[str, Any],
        effective_session: date,
        evidence_key: str,
        expiry_reason: str,
        expired_result: str,
        completed_result: Callable[[Mapping[str, Any]], Dict[str, Any]],
    ) -> None:
        origin_job_id = int(origin_job["id"])
        spawned_run_id = obligation.get("spawned_run_id")
        child_jobs: List[Dict[str, Any]] = []
        if spawned_run_id is not None:
            detail = self.repo.get_run_detail(int(spawned_run_id)) or {}
            child_jobs = list(detail.get("analysis_jobs") or [])
        completed = next(
            (row for row in reversed(child_jobs) if str(row.get("status") or "") == "completed"),
            None,
        )
        if completed is not None:
            evidence = completed.get("evidence") if isinstance(completed.get("evidence"), Mapping) else {}
            patch = {
                **dict(obligation),
                "status": "completed",
                "closed_on": effective_session.isoformat(),
                "completed_by_job_id": int(completed["id"]),
                "completed_report_id": completed.get("analysis_history_id"),
                **completed_result(evidence),
                "recovered_from_completed_child": True,
            }
        else:
            reason = expiry_reason
            for child in child_jobs:
                if str(child.get("status") or "") in {"completed", "failed"}:
                    continue
                child_evidence = child.get("evidence") if isinstance(child.get("evidence"), Mapping) else {}
                self.repo.complete_job(
                    int(child["id"]),
                    status="failed",
                    query_id=child.get("query_id"),
                    failure_reason=reason,
                    evidence={
                        **dict(child_evidence),
                        f"{evidence_key}_expired": {
                            "closed_on": effective_session.isoformat(),
                            "reason": reason,
                        },
                    },
                )
            if spawned_run_id is not None:
                self.repo.update_run(
                    int(spawned_run_id),
                    status="blocked",
                    error_message=reason,
                    completed_at=utc_naive_now(),
                )
            patch = {
                **dict(obligation),
                "status": "expired",
                "closed_on": effective_session.isoformat(),
                "result": expired_result,
                "reason": reason,
            }
        self.repo.patch_job_evidence(origin_job_id, {evidence_key: patch})

    def market_review_readiness(
        self,
        *,
        market: str = "cn",
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Read-only check that the current session's market review exists.

        A deferred (evening) pool run must wait for it: discovering from an older
        review fails the post-market gate, and ``_persist_blocked_run`` would then
        mark that earlier session's run (same ``run_key``) as blocked.
        """

        session = self._current_effective_session(market=market, current_time=current_time)
        if session is None:
            return {"ready": False, "reason": "current_session_unavailable"}
        report = self._latest_market_review(market=market)
        if report is None:
            return {"ready": False, "reason": "market_review_not_found", "session": session.isoformat()}
        report_date = self._payload_trade_date(self._extract_market_payload(report)) or self._report_trade_date(report)
        ready = report_date == session
        return {
            "ready": ready,
            "reason": None if ready else "market_review_not_ready",
            "session": session.isoformat(),
            "report_id": int(report.id),
            "report_date": report_date.isoformat(),
        }

    def discover_from_latest_market_review(
        self,
        *,
        market: str = "cn",
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        report = self._latest_market_review(market=market)
        if report is None:
            return {"status": "blocked", "reason": "market_review_not_found"}
        return self.discover_from_report(
            report_id=int(report.id),
            market=market,
            current_time=current_time,
        )

    def discover_from_report(
        self,
        *,
        report_id: int,
        market: str = "cn",
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        policy = self._policy()
        if policy.mode == "off":
            return {"status": "disabled", "reason": "MARKET_OPPORTUNITY_MODE=off"}

        report = self._get_market_review(report_id)
        if report is None:
            return {"status": "blocked", "reason": "market_review_not_found"}

        payload = self._extract_market_payload(report)
        trade_date = self._payload_trade_date(payload) or self._report_trade_date(report)
        if not payload:
            trade_date = self._report_trade_date(report)
            return self._persist_blocked_run(
                policy=policy,
                market=market,
                report=report,
                trade_date=trade_date,
                reason="structured_market_payload_missing",
            )
        gate = self._postmarket_report_gate(report=report, payload=payload, market=market, current_time=current_time)
        if not gate.get("ok"):
            return self._persist_blocked_run(
                policy=policy,
                market=market,
                report=report,
                trade_date=trade_date,
                reason=str(gate.get("reason") or "postmarket_report_gate_failed"),
            )
        observe_gate = self._observe_gate_status(market=market, as_of=trade_date, policy=policy)
        market_regime = self._latest_regime(market=market, trade_date=trade_date)
        regime_name = str(getattr(market_regime, "regime", "") or "unavailable")
        run = self.repo.create_or_get_run(
            {
                "run_key": self._run_key(report_id=int(report.id), policy=policy),
                "market": market,
                "trade_date": trade_date,
                "source_report_id": int(report.id),
                "market_regime_id": getattr(market_regime, "id", None),
                "market_regime": regime_name,
                "mode": policy.mode,
                "status": "discovering",
                "policy_version": OPPORTUNITY_POLICY_VERSION,
                "config_snapshot": self._config_snapshot(policy),
                "provenance": {"source": "market_review", "report_id": int(report.id)},
                "llm_budget": policy.max_llm_calls_per_run,
                "started_at": utc_naive_now(),
            }
        )
        existing = self._existing_run_child_summary(int(run.id))
        if existing.get("has_children"):
            reused = {
                "status": existing["status"],
                "run_id": int(run.id),
                "trade_date": trade_date.isoformat(),
                "mode": policy.mode,
                "theme_count": existing["theme_count"],
                "selected_theme_count": existing["selected_theme_count"],
                "candidate_count": existing["candidate_count"],
                "selected_candidate_count": existing["selected_candidate_count"],
                "job_count": existing["claimable_job_count"],
                "reused_run": True,
            }
            # 复用运行沿用首次发现时落库的研究准入状态，与新建运行的返回结构一致。
            if isinstance(existing.get("research"), Mapping):
                reused["research"] = dict(existing["research"])
            return reused

        legacy_themes = self._extract_themes(payload)
        observations = self._board_observations()
        observation = observations.collect(market=market, trade_date=trade_date)
        themes = observations.discovery_themes(
            market=market, as_of=trade_date, limit=policy.discovery_max_themes,
        )
        if not themes:
            themes = legacy_themes
        self._frozen_constituents.clear()
        replay_source_runs = []
        if observation.get("status") not in {"ok", "healthy"}:
            reader = getattr(self.repo, "frozen_constituents_for_report", None)
            frozen_rows = reader(report_id=int(report.id), market=market, trade_date=trade_date) if callable(reader) else []
            if isinstance(frozen_rows, list):
                by_key = {item["theme_key"]: item for item in themes}
                for frozen in frozen_rows:
                    theme = by_key.get(frozen.get("theme_key"))
                    raw = frozen.get("snapshot", {})
                    if (not theme or raw.get("provider") != theme.get("theme_source")
                            or raw.get("board_code") != theme.get("theme_code") or raw.get("market") != market
                            or raw.get("board_type") != theme.get("theme_type") or raw.get("board_name") != theme.get("theme_name")
                            or raw.get("as_of") != trade_date.isoformat()):
                        continue
                    try:
                        snapshot = BoardConstituentSnapshot(**{
                            **raw, "as_of": date.fromisoformat(raw["as_of"]),
                            "fetched_at": datetime.fromisoformat(raw["fetched_at"]),
                            "constituents": [BoardConstituent(**item) for item in raw["constituents"]],
                        })
                    except (TypeError, ValueError, KeyError):
                        continue
                    key = (market, trade_date, theme["theme_type"], theme.get("theme_code"), theme["theme_name"])
                    self._frozen_constituents[key] = snapshot
                    replay_source_runs.append(int(frozen["run_id"]))
        legacy_keys = {item["theme_key"] for item in legacy_themes}
        for theme in themes:
            theme["legacy_report_theme"] = theme["theme_key"] in legacy_keys
        self._daily_bar_cache.clear()
        selected_themes, theme_rows, candidate_rows = self._score_themes_and_candidates(
            themes=themes,
            policy=policy,
            market=market,
            trade_date=trade_date,
            regime=regime_name,
        )
        observation_health = self._observation_health(observation, theme_rows, candidate_rows)
        candidate_rows = self._rank_and_cap_candidates(candidate_rows, policy=policy, regime=regime_name)
        selected_candidate_count = sum(1 for item in candidate_rows if item.get("status") == "selected")
        analysis_allowed = (
            policy.mode == "paper" and observe_gate.get("completed")
            and regime_name not in {"risk_off", "unavailable"}
        )
        # 红灯本身不阻止研究：同样要求 paper 与工程门槛；数据不可用（unavailable）仍然阻止。
        research_allowed = bool(
            policy.mode == "paper" and observe_gate.get("completed") and regime_name == "risk_off"
        )
        if regime_name == "risk_off":
            # 红灯日收紧研究入选数量与分数门槛；落选者保留为观察。
            candidate_rows = self._risk_off_research_shortlist(candidate_rows, policy=policy)
            selected_candidate_count = sum(1 for item in candidate_rows if item.get("status") == "selected")
        models = list_analysis_models(self.config) if (analysis_allowed or research_allowed) else []
        if analysis_allowed and not models:
            return self._persist_blocked_run(
                policy=policy,
                market=market,
                report=report,
                trade_date=trade_date,
                reason="configured_models_missing",
                market_regime=market_regime,
            )
        budget_block_reason = self._paper_llm_budget_block_reason(
            policy=policy,
            models=models,
            selected_candidate_count=selected_candidate_count,
        ) if analysis_allowed else None
        research: Optional[Dict[str, Any]] = None
        if research_allowed:
            research = self._research_budget_state(
                policy=policy,
                market=market,
                session_date=trade_date,
                models=models,
                selected_candidate_count=selected_candidate_count,
            )
            jobs = [] if research.get("block_reason") else self._build_research_jobs(
                run_id=int(run.id),
                candidates=candidate_rows,
                models=models,
                max_candidates=int(research.get("max_candidates") or 0),
            )
            research["job_count"] = len(jobs)
        else:
            jobs = self._build_jobs(run_id=int(run.id), candidates=candidate_rows, models=models, policy=policy)

        self.repo.replace_run_children(
            run_id=int(run.id),
            themes=theme_rows,
            candidates=candidate_rows,
            jobs=jobs,
        )
        error_message = None
        if not themes:
            status = "blocked"
            error_message = "market_themes_missing"
        elif (
            theme_rows
            and not selected_candidate_count
            and all(str(item.get("status") or "") == "insufficient_data" for item in theme_rows)
        ):
            status = "blocked"
            error_message = "theme_constituents_unavailable"
        elif budget_block_reason:
            status = "blocked"
            error_message = budget_block_reason
        else:
            status = "ready" if policy.mode == "observe" else ("analyzing" if jobs else "completed")
        provenance = {
            "source": "market_review", "report_id": int(report.id),
            "observation_health": observation_health,
            "frozen_membership_replay": bool(replay_source_runs),
            "replay_source_run_ids": sorted(set(replay_source_runs)),
            "observation_candidate_count": sum(item.get("status") in {"selected", "observing"} for item in candidate_rows),
            "analysis_block_reason": (
                f"market_regime_{regime_name}" if regime_name in {"risk_off", "unavailable"}
                else "observe_gate_incomplete" if policy.mode == "paper" and not observe_gate.get("completed")
                else None
            ),
        }
        if research is not None:
            provenance["research"] = research
        self.repo.update_run(
            int(run.id),
            status=status,
            theme_count=len(theme_rows),
            selected_theme_count=len(selected_themes),
            candidate_count=len(candidate_rows),
            selected_candidate_count=selected_candidate_count,
            error_message=error_message,
            provenance=provenance,
            completed_at=utc_naive_now() if status in {"ready", "completed", "blocked"} else None,
        )
        result = {
            "status": status,
            "run_id": int(run.id),
            "trade_date": trade_date.isoformat(),
            "mode": policy.mode,
            "theme_count": len(theme_rows),
            "selected_theme_count": len(selected_themes),
            "candidate_count": len(candidate_rows),
            "selected_candidate_count": selected_candidate_count,
            "job_count": len(jobs),
            "reason": error_message,
        }
        if research is not None:
            result["research"] = research
        return result

    def analyze_run(
        self,
        run_id: int,
        *,
        owner: Optional[str] = None,
        limit: int = 20,
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Create missing jobs for one run and process only that run."""

        policy = self._policy()
        if policy.mode != "paper":
            return {"status": "disabled", "reason": "mode_not_paper", "processed": 0}
        detail = self.repo.get_run_detail(int(run_id))
        if detail is None:
            return {"status": "not_found", "reason": "run_not_found", "processed": 0}
        trade_date = self._parse_date(detail.get("trade_date"))
        market = str(detail.get("market") or "cn")
        current_session = self._current_effective_session(market=market, current_time=current_time)
        if current_session is None:
            reason = "current_session_unavailable"
            self.repo.update_run(
                int(run_id),
                status="blocked",
                error_message=reason,
                completed_at=utc_naive_now(),
            )
            return {
                "status": "blocked",
                "reason": reason,
                "trade_date": trade_date.isoformat() if trade_date else None,
                "current_session": None,
                "processed": 0,
                "created_jobs": 0,
            }
        if trade_date is None or trade_date != current_session:
            recovery = self._recover_submitted_jobs(
                run_id=int(run_id),
                limit=limit,
                effective_session=current_session,
            )
            reason = "stale_run_not_current_session"
            self.repo.update_run(
                int(run_id),
                status="blocked",
                error_message=reason,
                completed_at=utc_naive_now(),
            )
            return {
                "status": "blocked",
                "reason": reason,
                "trade_date": trade_date.isoformat() if trade_date else None,
                "current_session": current_session.isoformat(),
                "submitted_recovery": recovery,
                "processed": int(recovery.get("processed", 0) or 0),
                "created_jobs": 0,
            }
        run_key = str(detail.get("run_key") or "")
        is_research_review = run_key.startswith(MARKET_OPPORTUNITY_RESEARCH_REVIEW_RUN_KEY_PREFIX)
        # 红灯只把新发现的工作改为研究；数据不可用仍阻止，主账户确认在红灯下仍冻结。
        if detail.get("market_regime") == "unavailable" or (
            detail.get("market_regime") == "risk_off"
            and run_key.startswith(MARKET_OPPORTUNITY_FOLLOWUP_RUN_KEY_PREFIX)
        ):
            reason = f"market_regime_{detail['market_regime']}"
            self.repo.update_run(
                int(run_id),
                status="blocked",
                error_message=reason,
                completed_at=utc_naive_now(),
            )
            return {
                "status": "blocked", "reason": reason,
                "processed": 0, "created_jobs": 0,
            }
        research_only = is_research_review or detail.get("market_regime") == "risk_off"
        observe_gate = self._observe_gate_status(
            market=market,
            as_of=trade_date,
            policy=policy,
        )
        if not observe_gate.get("completed"):
            self.repo.update_run(int(run_id), status="blocked", error_message="observe_gate_incomplete", completed_at=utc_naive_now())
            return {
                "status": "blocked",
                "reason": "observe_gate_incomplete",
                "observe_gate": observe_gate,
                "processed": 0,
            }
        models = list_analysis_models(self.config)
        if not models:
            self.repo.update_run(int(run_id), status="blocked", error_message="configured_models_missing", completed_at=utc_naive_now())
            return {"status": "blocked", "reason": "configured_models_missing", "processed": 0}
        selected_candidate_count = sum(
            1 for item in detail.get("candidates") or []
            if item.get("status") == "selected"
        )
        if is_research_review:
            # 复核任务在到期日物化时已固定，手动触发只继续处理，不新增调用。
            created = 0
        elif research_only:
            analyzed_candidate_ids = {
                int(job.get("candidate_id"))
                for job in detail.get("analysis_jobs") or []
                if job.get("candidate_id") is not None
            }
            # 红灯限量按整轮候选计算，已分析过的也占名额，手动重复触发不会突破上限。
            shortlisted = self._risk_off_research_shortlist(
                list(detail.get("candidates") or []),
                policy=policy,
            )
            pending_candidates = [
                item for item in shortlisted
                if item.get("status") == "selected" and int(item.get("id") or 0) not in analyzed_candidate_ids
            ]
            research = self._research_budget_state(
                policy=policy,
                market=market,
                session_date=trade_date,
                models=models,
                selected_candidate_count=len(pending_candidates),
            )
            if research.get("block_reason"):
                return {
                    "status": "blocked",
                    "reason": research["block_reason"],
                    "research": research,
                    "processed": 0,
                    "created_jobs": 0,
                }
            created = self.repo.create_jobs_for_run(
                run_id=int(run_id),
                jobs=self._build_research_jobs(
                    run_id=int(run_id),
                    candidates=pending_candidates,
                    models=models,
                    max_candidates=int(research.get("max_candidates") or 0),
                ),
                effective_session=trade_date,
            )
        else:
            budget_block_reason = self._paper_llm_budget_block_reason(
                policy=policy,
                models=models,
                selected_candidate_count=selected_candidate_count,
            )
            if budget_block_reason:
                self.repo.update_run(int(run_id), status="blocked", error_message=budget_block_reason, completed_at=utc_naive_now())
                return {
                    "status": "blocked",
                    "reason": budget_block_reason,
                    "processed": 0,
                    "created_jobs": 0,
                }
            jobs = self._build_jobs(
                run_id=int(run_id),
                candidates=list(detail.get("candidates") or []),
                models=models,
                policy=policy,
            )
            created = self.repo.create_jobs_for_run(
                run_id=int(run_id),
                jobs=jobs,
                effective_session=trade_date,
            )
        if created:
            self.repo.update_run(int(run_id), status="analyzing", completed_at=None)
        result = self.process_pending_jobs(
            owner=owner,
            limit=limit,
            run_id=int(run_id),
            effective_session=trade_date,
        )
        result["created_jobs"] = created
        return result

    @staticmethod
    def _current_effective_session(*, market: str, current_time: Optional[datetime] = None) -> Optional[date]:
        try:
            from src.core.trading_calendar import get_effective_trading_date
            from src.services.daily_market_context import market_context_freshness_expires_at

            effective = get_effective_trading_date(market, current_time=current_time)
            # ``get_effective_trading_date`` is intentionally fail-open for
            # legacy checkpoint callers. Opportunity execution must be stricter:
            # if the exchange calendar cannot prove the next-session expiry, do
            # not treat a natural date fallback as tradable.
            if market_context_freshness_expires_at(market, effective) is None:
                return None
            return effective
        except Exception as exc:
            logger.warning("机会池无法解析可信当前交易日，已 fail-closed: %s", exc, exc_info=True)
            return None

    def process_pending_jobs(
        self,
        *,
        owner: Optional[str] = None,
        limit: int = 1,
        run_id: Optional[int] = None,
        effective_session: Optional[date] = None,
    ) -> Dict[str, Any]:
        policy = self._policy()
        if policy.mode != "paper":
            return {"status": "disabled", "reason": "mode_not_paper", "processed": 0}
        if effective_session is None:
            effective_session = self._current_effective_session(market="cn")
        if effective_session is None:
            return {
                "status": "blocked",
                "reason": "current_session_unavailable",
                "processed": 0,
                "failed": 0,
                "uncertain_submitted": 0,
            }
        owner_id = owner or f"market-opportunity-{uuid.uuid4().hex[:8]}"
        recovered = self._recover_submitted_jobs(
            run_id=run_id,
            limit=limit,
            effective_session=effective_session,
        )
        observe_gate = self._observe_gate_status(market="cn", as_of=effective_session, policy=policy)
        if not observe_gate.get("completed"):
            return {"status": "blocked", "reason": "observe_gate_incomplete", "processed": int(recovered.get("processed", 0)),
                    "failed": int(recovered.get("failed", 0)), "uncertain_submitted": int(recovered.get("uncertain", 0))}
        jobs = self.repo.claim_pending_jobs(
            owner=owner_id,
            limit=limit,
            run_id=run_id,
            effective_session=effective_session,
        )
        processed = 0
        failed = 0
        uncertain = int(recovered.get("uncertain", 0) or 0)
        run_ids = set()
        for job in jobs:
            run_ids.add(int(job.run_id))
            candidate = self.repo.get_candidate(int(job.candidate_id))
            if candidate is None:
                self.repo.complete_job(int(job.id), status="failed", failure_reason="candidate_missing")
                failed += 1
                continue
            run_detail_reader = getattr(self.repo, "get_run_detail", None)
            job_run = run_detail_reader(int(job.run_id)) if callable(run_detail_reader) else {}
            if is_market_opportunity_research_job_key(getattr(job, "job_key", None)):
                # 研究任务按不可变 job_key 分流，永不进入主账户对账。
                outcome = self._process_research_job(
                    job=job,
                    candidate=candidate,
                    job_run=job_run if isinstance(job_run, dict) else {},
                    owner_id=owner_id,
                    policy=policy,
                )
                if outcome == "processed":
                    processed += 1
                else:
                    failed += 1
                continue
            if isinstance(job_run, dict) and job_run.get("market_regime") in {"risk_off", "unavailable"}:
                job_evidence = self._json_dict(getattr(job, "evidence_json", None))
                if (
                    job_run.get("market_regime") == "unavailable"
                    and job_evidence.get("source") == "opportunity_confirmation_followup"
                ):
                    pass
                else:
                    self.repo.complete_job(int(job.id), status="failed", failure_reason=f"market_regime_{job_run['market_regime']}")
                    failed += 1
                    continue
            query_id = job.query_id or f"market_opp_{int(job.id)}"
            selection_context = build_selection_context(candidate=candidate, job=job)
            try:
                existing_report_id = self._same_day_stock_report_id(
                    candidate=candidate,
                    model=str(job.model),
                    trade_date=getattr(candidate, "as_of", None),
                )
                if existing_report_id:
                    self._attach_opportunity_provenance(
                        report_id=int(existing_report_id),
                        candidate=candidate,
                        job=job,
                    )
                    reconcile = self._reconcile_report_to_paper_plan(
                        int(existing_report_id),
                        trigger_source="market_opportunity_existing_report",
                    )
                    self._complete_job_with_artifacts(
                        job_id=int(job.id),
                        status="completed",
                        query_id=query_id,
                        analysis_history_id=int(existing_report_id),
                        reconcile=reconcile,
                        evidence=self._completion_evidence(
                            candidate=candidate,
                            job=job,
                            report_id=int(existing_report_id),
                            reconcile=reconcile,
                            base={
                            "source": "market_opportunity",
                            "candidate_id": int(candidate.id),
                            "reused_same_day_report": True,
                            },
                        ),
                    )
                    processed += 1
                    continue

                mark_submitted = getattr(self.repo, "mark_job_submitted", None)
                if callable(mark_submitted) and not mark_submitted(
                    int(job.id),
                    owner=owner_id,
                    query_id=query_id,
                ):
                    self.repo.complete_job(
                        int(job.id),
                        status="failed_retryable",
                        query_id=query_id,
                        failure_reason="job_submission_claim_lost",
                        evidence={"source": "market_opportunity", "stage": "mark_submitted"},
                    )
                    failed += 1
                    continue
                result = self._build_analysis_service().analyze_stock(
                    candidate.symbol or candidate.stock_code,
                    report_type="detailed",
                    force_refresh=True,
                    query_id=query_id,
                    send_notification=False,
                    analysis_phase="postmarket",
                    query_source="market_opportunity",
                    selection_context=selection_context,
                    model=job.model,
                    execution_intent={
                        "execution_intent": REPORT_EXECUTION_INTENT_PAPER_CANDIDATE,
                        "execution_scope": REPORT_EXECUTION_SCOPE_PRIMARY,
                        "decision_as_of": getattr(candidate, "as_of", None),
                        "intent_source": "market_opportunity",
                    },
                    trading_style_override=self._candidate_analysis_style(candidate),
                )
                report_id = self._analysis_history_id_for_query(query_id)
                if not result or not report_id:
                    self.repo.complete_job(
                        int(job.id),
                        status="failed_retryable",
                        query_id=query_id,
                        failure_reason="analysis_report_missing_after_sync_call",
                        evidence={
                            "result_present": bool(result),
                            "report_id_present": bool(report_id),
                            "source": "market_opportunity",
                            "stage": "sync_analysis_failed",
                        },
                    )
                    failed += 1
                    continue
                self._attach_opportunity_provenance(
                    report_id=report_id,
                    candidate=candidate,
                    job=job,
                )
                reconcile = self._reconcile_report_to_paper_plan(
                    report_id,
                    trigger_source="market_opportunity_report_completed",
                )
                self._complete_job_with_artifacts(
                    job_id=int(job.id),
                    status="completed",
                    query_id=query_id,
                    analysis_history_id=report_id,
                    reconcile=reconcile,
                    evidence=self._completion_evidence(
                        candidate=candidate,
                        job=job,
                        report_id=report_id,
                        reconcile=reconcile,
                        base={
                        "source": "market_opportunity",
                        "candidate_id": int(candidate.id),
                        "reused_same_day_report": False,
                        },
                    ),
                )
                processed += 1
            except Exception as exc:
                logger.warning("Market opportunity job failed: %s", exc, exc_info=True)
                recovered_report_id = self._analysis_history_id_for_query(query_id)
                if recovered_report_id:
                    reconcile = self._reconcile_report_to_paper_plan(
                        recovered_report_id,
                        trigger_source="market_opportunity_exception_recovered",
                    )
                    self._complete_job_with_artifacts(
                        job_id=int(job.id),
                        status="completed",
                        query_id=query_id,
                        analysis_history_id=recovered_report_id,
                        reconcile=reconcile,
                        evidence=self._completion_evidence(
                            candidate=candidate,
                            job=job,
                            report_id=recovered_report_id,
                            reconcile=reconcile,
                            base={
                            "source": "market_opportunity",
                            "candidate_id": int(candidate.id),
                            "recovered_after_exception": True,
                            },
                        ),
                    )
                    processed += 1
                    continue
                self.repo.complete_job(
                    int(job.id),
                    status="failed_retryable",
                    query_id=query_id,
                    failure_reason=f"sync_analysis_exception:{type(exc).__name__}:{str(exc)[:100]}",
                    evidence={
                        "source": "market_opportunity",
                        "stage": "sync_analysis_failed",
                    },
                )
                failed += 1
        for run_id in run_ids:
            self._refresh_run_status(run_id)
        return {
            "status": "ok",
            "processed": processed + int(recovered.get("processed", 0) or 0),
            "failed": failed + int(recovered.get("failed", 0) or 0),
            "uncertain_submitted": uncertain,
        }

    def _process_research_job(
        self,
        *,
        job: Any,
        candidate: Any,
        job_run: Mapping[str, Any],
        owner_id: str,
        policy: OpportunityPolicy,
    ) -> str:
        """Run one research-only job and return ``processed`` or ``failed``.

        The model call is charged to the shared session ledger before the job is
        marked submitted, once per attempt.  The report is saved research-only
        and is never reconciled into the primary paper account.
        """

        base = self._research_evidence_base(job=job)
        query_id = job.query_id or f"market_opp_{int(job.id)}"
        if job_run.get("market_regime") == "unavailable":
            return self._fail_research_job(
                job=job, query_id=query_id, base=base, reason="market_regime_unavailable",
            )
        try:
            existing_report_id = self._same_day_stock_report_id(
                candidate=candidate,
                model=str(job.model),
                trade_date=getattr(candidate, "as_of", None),
                execution_intent=REPORT_EXECUTION_INTENT_RESEARCH_ONLY,
            )
            if existing_report_id:
                self._attach_opportunity_provenance(report_id=int(existing_report_id), candidate=candidate, job=job)
                self._complete_research_job(
                    job=job,
                    candidate=candidate,
                    query_id=query_id,
                    report_id=int(existing_report_id),
                    base={**base, "reused_same_day_report": True},
                )
                return "processed"

            reservation_key = None
            if base["research_kind"] == "review":
                reservation_key = self._research_review_origin_obligation(base).get("reservation_key")
            slot = self.repo.acquire_research_budget_slot(
                market=str(getattr(candidate, "market", None) or job_run.get("market") or "cn"),
                session_date=getattr(candidate, "as_of", None),
                limit=self._research_budget_limit(policy),
                job_id=int(job.id),
                attempt_no=max(1, int(getattr(job, "attempt_count", 0) or 0)),
                job_kind=str(base["research_kind"]),
                query_id=query_id,
                reservation_key=reservation_key,
            )
            if slot is None:
                return self._fail_research_job(
                    job=job, query_id=query_id, base=base, reason="research_budget_exhausted",
                )
            mark_submitted = getattr(self.repo, "mark_job_submitted", None)
            if callable(mark_submitted) and not mark_submitted(int(job.id), owner=owner_id, query_id=query_id):
                self.repo.complete_job(
                    int(job.id),
                    status="failed_retryable",
                    query_id=query_id,
                    failure_reason="job_submission_claim_lost",
                    evidence={**base, "stage": "mark_submitted", "budget_slot": slot.get("slot_no")},
                )
                return "failed"
            result = self._build_analysis_service().analyze_stock(
                candidate.symbol or candidate.stock_code,
                report_type="detailed",
                force_refresh=True,
                query_id=query_id,
                send_notification=False,
                analysis_phase="postmarket",
                query_source="market_opportunity",
                selection_context=build_selection_context(candidate=candidate, job=job),
                model=job.model,
                execution_intent={
                    "execution_intent": REPORT_EXECUTION_INTENT_RESEARCH_ONLY,
                    "execution_scope": REPORT_EXECUTION_SCOPE_PRIMARY,
                    "decision_as_of": getattr(candidate, "as_of", None),
                    "intent_source": RESEARCH_JOB_SOURCE,
                },
                trading_style_override=self._candidate_analysis_style(candidate),
            )
            report_id = self._analysis_history_id_for_query(query_id)
            if not result or not report_id:
                self.repo.complete_job(
                    int(job.id),
                    status="failed_retryable",
                    query_id=query_id,
                    failure_reason="analysis_report_missing_after_sync_call",
                    evidence={
                        **base,
                        "result_present": bool(result),
                        "report_id_present": bool(report_id),
                        "stage": "sync_analysis_failed",
                    },
                )
                return "failed"
            self._attach_opportunity_provenance(report_id=report_id, candidate=candidate, job=job)
            self._complete_research_job(
                job=job,
                candidate=candidate,
                query_id=query_id,
                report_id=report_id,
                base={**base, "reused_same_day_report": False},
            )
            return "processed"
        except Exception as exc:
            logger.warning("Market opportunity research job failed: %s", exc, exc_info=True)
            recovered_report_id = self._analysis_history_id_for_query(query_id)
            if recovered_report_id:
                self._complete_research_job(
                    job=job,
                    candidate=candidate,
                    query_id=query_id,
                    report_id=recovered_report_id,
                    base={**base, "recovered_after_exception": True},
                )
                return "processed"
            self.repo.complete_job(
                int(job.id),
                status="failed_retryable",
                query_id=query_id,
                failure_reason=f"sync_analysis_exception:{type(exc).__name__}:{str(exc)[:100]}",
                evidence={**base, "stage": "sync_analysis_failed"},
            )
            return "failed"

    def _fail_research_job(
        self,
        *,
        job: Any,
        query_id: Optional[str],
        base: Mapping[str, Any],
        reason: str,
        stage: str = "research_admission",
    ) -> str:
        """Terminally fail a research job and close its review link."""

        self.repo.complete_job(
            int(job.id),
            status="failed",
            query_id=query_id,
            failure_reason=reason,
            evidence={**dict(base), "stage": stage},
        )
        if base.get("research_kind") == "review":
            self._patch_research_review_origin(base, {"status": "failed", "result": "not_reviewed", "reason": reason})
        return "failed"

    def _complete_research_job(
        self,
        *,
        job: Any,
        candidate: Any,
        query_id: Optional[str],
        report_id: int,
        base: Mapping[str, Any],
        expired_on: Optional[date] = None,
        failure_reason: Optional[str] = None,
    ) -> None:
        """Persist the frozen research bundle; research never reaches paper reconciliation.

        ``expired_on`` marks a report recovered after its session ended: the
        bundle is kept for audit, but it neither schedules nor completes a review.
        """

        bundle = self._research_bundle(job=job, candidate=candidate, report_id=report_id)
        evidence: Dict[str, Any] = {
            **dict(base),
            "analysis_history_id": int(report_id),
            "research_bundle": bundle,
            "paper_reconciliation": dict(RESEARCH_ONLY_RECONCILIATION),
        }
        if base.get("research_kind") == "initial" and expired_on is None and candidate is not None:
            review = self._schedule_research_review(
                job=job,
                candidate=candidate,
                report_id=int(report_id),
                outcome=str(bundle["outcome"]["classification"]),
            )
            if review is not None:
                evidence["research_review"] = review
        self.repo.complete_job(
            int(job.id),
            status="completed",
            query_id=query_id,
            analysis_history_id=int(report_id),
            failure_reason=failure_reason,
            evidence=evidence,
        )
        if base.get("research_kind") != "review":
            return
        if expired_on is not None:
            # 错过复核日的报告只留档，不得日后补成复核结果。
            self._patch_research_review_origin(base, {
                "status": "expired",
                "closed_on": expired_on.isoformat(),
                "result": "expired",
                "reason": failure_reason or "submitted_job_session_expired_after_report",
            })
            return
        self._patch_research_review_origin(base, {
            "status": "completed",
            "completed_by_job_id": int(job.id),
            "completed_report_id": int(report_id),
            "result": bundle["outcome"]["classification"],
            "bundle_id": bundle["bundle_id"],
            "closed_on": bundle.get("as_of"),
        })

    def _schedule_research_review(
        self,
        *,
        job: Any,
        candidate: Any,
        report_id: int,
        outcome: str,
    ) -> Optional[Dict[str, Any]]:
        """Reserve one next-session review slot for a bullish initial research result.

        The identity ignores run/job ids so a rediscovered candidate shares one
        review.  Reviews never schedule further reviews.
        """

        as_of = self._parse_date(getattr(candidate, "as_of", None))
        if as_of is None:
            return None
        market = str(getattr(candidate, "market", None) or "cn")
        model = str(getattr(job, "model", "") or "")
        identity = self._stable_hash({
            "type": "opportunity_research_review",
            "candidate_key": getattr(candidate, "candidate_key", None),
            "model": model,
            "origin_session": as_of.isoformat(),
            "policy": RESEARCH_REVIEW_POLICY_VERSION,
        })
        due_date = self._next_trading_day(market, as_of)
        reservation_key = f"research-review:{identity}"
        obligation = build_research_review_obligation(
            outcome=outcome,
            candidate=candidate,
            model=model,
            due_date=due_date,
            origin_job_id=int(job.id),
            origin_report_id=int(report_id),
            reservation_key=reservation_key,
        )
        if obligation is None:
            return None
        obligation["identity"] = identity
        slot = self.repo.reserve_research_budget_slot(
            market=market,
            session_date=due_date,
            limit=self._research_budget_limit(self._policy()),
            reservation_key=reservation_key,
        )
        if slot is None:
            return {
                **obligation,
                "status": "skipped",
                "result": "not_scheduled",
                "reason": "research_review_budget_unavailable",
            }
        return {**obligation, "reserved_slot_no": slot.get("slot_no")}

    def _research_review_origin_obligation(self, base: Mapping[str, Any]) -> Dict[str, Any]:
        review_of = base.get("research_review_of")
        if not isinstance(review_of, Mapping) or review_of.get("origin_job_id") is None:
            return {}
        origin = self.repo.get_job(int(review_of["origin_job_id"])) or {}
        evidence = origin.get("evidence") if isinstance(origin.get("evidence"), Mapping) else {}
        obligation = evidence.get("research_review")
        return dict(obligation) if isinstance(obligation, Mapping) else {}

    def _patch_research_review_origin(self, base: Mapping[str, Any], patch: Mapping[str, Any]) -> None:
        review_of = base.get("research_review_of")
        if not isinstance(review_of, Mapping) or review_of.get("origin_job_id") is None:
            return
        obligation = self._research_review_origin_obligation(base)
        self.repo.patch_job_evidence(
            int(review_of["origin_job_id"]),
            {"research_review": {**obligation, **dict(patch)}},
        )

    def _research_bundle(self, *, job: Any, candidate: Any, report_id: int) -> Dict[str, Any]:
        """Freeze the account-agnostic facts and conditions of one research result."""

        from src.schemas.decision_action import localize_action_label, normalize_decision_action
        from src.services.decision_signal_service import BULLISH_ACTIONS

        report: Dict[str, Any] = {}
        with self.db.session_scope() as session:
            row = session.get(AnalysisHistory, int(report_id))
            if row is not None:
                raw = self._json_dict(row.raw_result)
                # 以报告自身动作为准，不做评分对齐：观望/持有不能被情绪分抬成买入。
                action = normalize_decision_action(raw.get("action")) or normalize_decision_action(
                    raw.get("operation_advice") or getattr(row, "operation_advice", None)
                )
                report = {
                    "raw": raw,
                    "context": self._json_dict(row.context_snapshot),
                    "model_used": row.model_used,
                    "report_type": row.report_type,
                    "created_at": row.created_at,
                    "action": action,
                    "action_label": localize_action_label(action, raw.get("report_language")) if action else None,
                    "sniper_points": {
                        "ideal_buy": row.ideal_buy,
                        "secondary_buy": row.secondary_buy,
                        "stop_loss": row.stop_loss,
                        "take_profit": row.take_profit,
                    },
                }
        raw = report.get("raw") or {}
        context = report.get("context") or {}
        if not report or raw.get("success") is False:
            classification = "failed"
        elif normalize_decision_action(report.get("action")) in BULLISH_ACTIONS:
            classification = "bullish"
        else:
            classification = "non_bullish"
        dashboard = raw.get("dashboard") if isinstance(raw.get("dashboard"), Mapping) else {}
        phase_decision = dashboard.get("phase_decision") if isinstance(dashboard.get("phase_decision"), Mapping) else {}
        # 报告回写的机会池来源属于事后标注，不计入输入哈希。
        input_hash = self._stable_hash({
            key: value for key, value in context.items()
            if key not in {"query_source", "selection_sources", "market_opportunity"}
        }) if report else None
        output_hash = self._stable_hash(raw) if report else None
        as_of = self._parse_date(getattr(candidate, "as_of", None)) if candidate is not None else None
        market = str(getattr(candidate, "market", None) or "cn")
        kind = self._research_job_kind(getattr(job, "job_key", None))
        model = str(getattr(job, "model", "") or "")
        candidate_key = getattr(candidate, "candidate_key", None)
        created_at = report.get("created_at")
        return {
            "bundle_version": RESEARCH_BUNDLE_VERSION,
            "bundle_id": self._stable_hash({
                "report_id": int(report_id),
                "candidate_key": candidate_key,
                "model": model,
                "as_of": as_of.isoformat() if as_of else None,
                "research_kind": kind,
                "input_hash": input_hash,
                "output_hash": output_hash,
                "bundle_version": RESEARCH_BUNDLE_VERSION,
            }),
            "experiment_id": None,
            "report_id": int(report_id),
            "research_kind": kind,
            "as_of": as_of.isoformat() if as_of else None,
            # 报告保存时间（服务器本地时钟）；最早可执行时段沿用下一交易日契约。
            "available_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
            "earliest_executable_session": self._next_trading_day(market, as_of).isoformat() if as_of else None,
            "candidate": {
                "candidate_id": int(getattr(candidate, "id", 0) or 0),
                "candidate_key": candidate_key,
                "stock_code": getattr(candidate, "stock_code", None),
                "symbol": getattr(candidate, "symbol", None),
                "rank": getattr(candidate, "rank", None),
                "score": getattr(candidate, "score", None),
                "primary_theme_key": getattr(candidate, "primary_theme_key", None),
            },
            "model": model,
            "model_used": report.get("model_used"),
            "report_schema_version": raw.get("report_schema_version"),
            "policy_versions": {
                "opportunity": OPPORTUNITY_POLICY_VERSION,
                "theme": THEME_POLICY_VERSION,
                "candidate": CANDIDATE_POLICY_VERSION,
                "selection_context": SELECTION_CONTEXT_VERSION,
                "report_execution": REPORT_EXECUTION_POLICY_VERSION,
                "research_review": RESEARCH_REVIEW_POLICY_VERSION,
            },
            "prompt_fingerprint": _static_prompt_fingerprint(),
            "input_hash": input_hash,
            "output_hash": output_hash,
            # 账户无关的完整研究正文，供受信任编排器在隔离组内按当前交易日重建可执行报告。
            # 与 output_hash 对应做完整性校验；to_dict() 不含账户/数量/成本，符合“实验无法读取主账户可变状态”。
            "report_payload": raw if report else None,
            "report_type": str(report.get("report_type") or "simple"),
            "outcome": {
                "classification": classification,
                "action": report.get("action"),
                "action_label": report.get("action_label"),
            },
            "conditions": {
                "sniper_points": report.get("sniper_points") or {},
                "phase_decision": {
                    key: phase_decision.get(key)
                    for key in (
                        "entry_mode", "action_window", "immediate_action", "watch_conditions",
                        "next_check_time", "confidence_reason", "data_limitations", "phase_context",
                    )
                    if key in phase_decision
                },
                "position_actions": raw.get("position_actions"),
            },
        }

    def list_runs(self, *, limit: int = 20) -> List[Dict[str, Any]]:
        rows = self.repo.list_runs(limit=limit)
        return [self.project_observation_health(row) for row in rows]

    def get_run_detail(self, run_id: int) -> Optional[Dict[str, Any]]:
        detail = self.repo.get_run_detail(run_id)
        if detail is None:
            return None
        configured = list_analysis_models(self.config)
        jobs_by_candidate: Dict[int, List[Dict[str, Any]]] = {}
        for job in detail.get("analysis_jobs", []):
            jobs_by_candidate.setdefault(int(job.get("candidate_id") or 0), []).append(job)
        enriched_candidates = []
        for candidate in detail.get("candidates", []):
            item = dict(candidate)
            jobs = jobs_by_candidate.get(int(item["id"]), [])
            item["analysis_jobs"] = jobs
            item["model_states"] = self._model_states(configured, jobs)
            item["no_plan_reason"] = self._candidate_no_plan_reason(configured, jobs)
            enriched_candidates.append(item)
        detail["candidates"] = enriched_candidates
        detail["configured_models"] = configured
        return self.project_observation_health(detail, themes=detail.get("themes", []), candidates=detail["candidates"])

    def list_candidates(self, *, run_id: Optional[int] = None, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        return self.repo.list_candidates(run_id=run_id, status=status, limit=limit)

    def _existing_run_child_summary(self, run_id: int) -> Dict[str, Any]:
        detail = self.repo.get_run_detail(int(run_id))
        if detail is None:
            return {"has_children": False}
        themes = list(detail.get("themes") or [])
        candidates = list(detail.get("candidates") or [])
        jobs = list(detail.get("analysis_jobs") or [])
        if not (themes or candidates or jobs):
            return {"has_children": False}
        if str(detail.get("status") or "") == "blocked" and not any(
            str(job.get("status") or "") in {"claimed", "submitted", "completed"}
            for job in jobs
        ):
            return {"has_children": False}
        claimable = [
            job
            for job in jobs
            if str(job.get("status") or "") in {"pending", "claimed", "failed_retryable", "submitted"}
        ]
        status = str(detail.get("status") or "completed")
        if claimable:
            status = "analyzing"
        provenance = detail.get("provenance") if isinstance(detail.get("provenance"), Mapping) else {}
        return {
            "has_children": True,
            "status": status,
            "theme_count": len(themes),
            "selected_theme_count": sum(1 for item in themes if item.get("status") == "selected"),
            "candidate_count": len(candidates),
            "selected_candidate_count": sum(1 for item in candidates if item.get("status") == "selected"),
            "claimable_job_count": len(claimable),
            "research": provenance.get("research"),
        }

    def status(self) -> Dict[str, Any]:
        effective_mode = self.runtime_settings_service.get_market_opportunity_mode()
        policy = self._policy_from_mode(effective_mode)
        observe_gate = self._observe_gate_status(market="cn", as_of=None, policy=policy)
        return {
            "mode": policy.mode,
            "mode_setting": effective_mode.to_dict(),
            "enabled": policy.mode != "off",
            "configured_models": list_analysis_models(self.config),
            "limits": self._config_snapshot(policy),
            "observe_gate": observe_gate,
            "message": "disabled" if policy.mode == "off" else f"market opportunity mode={policy.mode}",
        }

    def paper_enablement_gate(self, *, market: str = "cn", as_of: Optional[date] = None) -> Dict[str, Any]:
        """Return the observe rollout gate used before accepting paper mode."""

        effective_mode = self.runtime_settings_service.get_market_opportunity_mode()
        policy = self._policy_from_mode(effective_mode)
        paper_policy = OpportunityPolicy(
            mode="paper",
            mode_source=policy.mode_source,
            mode_version=policy.mode_version,
            discovery_max_themes=policy.discovery_max_themes,
            max_themes=policy.max_themes,
            max_candidates_per_theme=policy.max_candidates_per_theme,
            max_total_candidates=policy.max_total_candidates,
            max_observation_candidates=policy.max_observation_candidates,
            max_llm_calls_per_run=policy.max_llm_calls_per_run,
            min_20d_avg_amount_cny=policy.min_20d_avg_amount_cny,
            blacklist=policy.blacklist,
            observe_gate_days=policy.observe_gate_days,
            research_daily_llm_budget=policy.research_daily_llm_budget,
            risk_off_max_candidates=policy.risk_off_max_candidates,
            risk_off_min_score=policy.risk_off_min_score,
        )
        return self._observe_gate_status(market=market, as_of=as_of, policy=paper_policy)

    def _observe_gate_status(
        self,
        *,
        market: str,
        as_of: Optional[date],
        policy: OpportunityPolicy,
    ) -> Dict[str, Any]:
        required = max(0, int(policy.observe_gate_days))
        if required == 0:
            return {
                "required_trading_days": 0,
                "completed_trading_days": 0,
                "completed": True,
                "status": "disabled",
                "dates": [],
                "message": "paper observe gate disabled",
            }
        with self.db.session_scope() as session:
            query = session.query(MarketOpportunityRun).filter(
                MarketOpportunityRun.market == str(market or "cn").strip().lower(),
                MarketOpportunityRun.mode.in_(["observe", "paper"]),
            )
            if as_of is not None:
                query = query.filter(MarketOpportunityRun.trade_date <= as_of)
            rows = query.order_by(desc(MarketOpportunityRun.trade_date), desc(MarketOpportunityRun.id)).all()
            runs = [MarketOpportunityRepository._run_to_dict(row) for row in rows]
        # Full board collection is independent of LLM/report/stock data success.
        # Never derive observation credit from a cached run verdict or candidate count.
        snapshot_health = self._board_observations().observation_health_by_date(market, as_of)
        health_by_date = {day: health["healthy"] for day, health in snapshot_health.items()}
        excluded = {day: str(health["reason"]) for day, health in snapshot_health.items() if not health["healthy"]}
        for run in runs:
            day = str(run.get("trade_date") or "")
            if not day:
                continue
            health_by_date.setdefault(day, False)
            if not health_by_date[day]:
                recorded = (run.get("provenance") or {}).get("observation_health") or {}
                excluded.setdefault(day, str(recorded.get("reason") or run.get("error_message") or "observation_health_unverified"))
        unique_dates = sorted((day for day, healthy in health_by_date.items() if healthy), reverse=True)
        completed = len(unique_dates) >= required
        return {
            "required_trading_days": required,
            "completed_trading_days": len(unique_dates),
            "completed": completed,
            "status": "completed" if completed else "observe_required",
            "dates": unique_dates[:required],
            "total_run_days": len({str(run.get("trade_date")) for run in runs}),
            "total_observation_days": len(snapshot_health),
            "healthy_run_days": len(unique_dates),
            "excluded_dates": [{"date": day, "reason": reason} for day, reason in excluded.items() if not health_by_date[day]],
            "counting_basis": "healthy_board_observation_days",
            "policy_version": OBSERVATION_HEALTH_POLICY_VERSION,
            "message": "paper mode allowed" if completed else f"need {required} healthy observation trading days before paper mode",
        }

    @staticmethod
    def _discovery_quality(themes: List[Dict[str, Any]], candidates: List[Dict[str, Any]], *, scope: str) -> Dict[str, Any]:
        data_errors = {
            "insufficient_daily_bars", "relative_strength_benchmark_unavailable",
            "insufficient_style_history", "non_finite_metrics", "future_bar_rejected",
            "daily_bar_not_as_of_trade_date", "benchmark_bar_not_as_of_trade_date",
            "trading_style_unavailable", "limit_prices_unavailable", "daily_data_unavailable",
        }
        incomplete = 0
        reason_counts: Dict[str, int] = {}
        for item in candidates:
            failures = data_errors.intersection(item.get("rejection_reasons") or [])
            if (item.get("score_components") or {}).get("history_status") == "short_history":
                failures.discard("insufficient_daily_bars")
            if failures:
                incomplete += 1
                for reason in failures:
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
        theme_errors = sum(item.get("status") == "insufficient_data" for item in themes)
        return {
            "status": "partial" if incomplete or theme_errors or not themes else "complete",
            "candidate_count": len(candidates), "candidate_data_incomplete_count": incomplete,
            "theme_data_incomplete_count": theme_errors, "reason_counts": reason_counts, "scope": scope,
        }

    @classmethod
    def _observation_health(cls, observation: Dict[str, Any], themes: List[Dict[str, Any]], candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        reason = None
        if observation.get("status") not in {"ok", "healthy"} or not observation.get("snapshot_id"):
            reason = str(observation.get("reason") or "board_observation_unavailable")
        return {
            "healthy": reason is None, "reason": reason,
            "source_snapshot_id": observation.get("snapshot_id"), "source": observation.get("source"),
            "counting_basis": "healthy_board_observation_days",
            "policy_version": OBSERVATION_HEALTH_POLICY_VERSION,
            "discovery_quality": cls._discovery_quality(themes, candidates, scope="discovery_candidates"),
        }

    def project_observation_health(self, run: Dict[str, Any], *,
                                   health_by_date: Optional[Dict[str, Any]] = None,
                                   themes: Optional[List[Dict[str, Any]]] = None,
                                   candidates: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Read-only current verdict, with the originally frozen verdict retained."""
        item = dict(run)
        provenance = dict(item.get("provenance") or {})
        recorded = provenance.get("observation_health") or {}
        if provenance.get("frozen_membership_replay") or not recorded.get("source_snapshot_id"):
            return item
        if health_by_date is None:
            health_by_date = self._board_observations().observation_health_by_date(item.get("market") or "cn")
        evidence = health_by_date.get(str(item.get("trade_date"))) or {}
        if (evidence.get("source_snapshot_id") != recorded.get("source_snapshot_id")
                or evidence.get("source") != recorded.get("source")
                or evidence.get("market") != (item.get("market") or "cn")):
            evidence = {**recorded, "healthy": False, "reason": "snapshot_reference_mismatch",
                        "policy_version": OBSERVATION_HEALTH_POLICY_VERSION}
        effective = dict(evidence)
        if recorded.get("discovery_quality"):
            effective["discovery_quality"] = recorded["discovery_quality"]
        elif candidates is not None:
            effective["discovery_quality"] = self._discovery_quality(themes or [], candidates, scope="persisted_candidates")
        provenance["recorded_observation_health"] = recorded
        provenance["observation_health"] = effective
        item["provenance"] = provenance
        return item

    def list_runs_page(
        self,
        *,
        page: int = 1,
        limit: int = 20,
        market: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = self.repo.list_runs_page(page=page, limit=limit, market=market, status=status)
        payload["configured_models"] = list_analysis_models(self.config)
        payload["items"] = [self.project_observation_health(row) for row in payload["items"]]
        return payload

    def list_themes_page(
        self,
        *,
        run_id: Optional[int] = None,
        status: Optional[str] = None,
        page: int = 1,
        limit: int = 50,
    ) -> Dict[str, Any]:
        return self.repo.list_themes_page(run_id=run_id, status=status, page=page, limit=limit)

    def list_candidates_page(
        self,
        *,
        run_id: Optional[int] = None,
        status: Optional[str] = None,
        page: int = 1,
        limit: int = 50,
    ) -> Dict[str, Any]:
        payload = self.repo.list_candidates_page(run_id=run_id, status=status, page=page, limit=limit)
        configured = list_analysis_models(self.config)
        jobs_by_candidate: Dict[int, List[Dict[str, Any]]] = {}
        for row in payload.get("items", []):
            detail = self.repo.get_run_detail(int(row["run_id"]))
            for job in (detail or {}).get("analysis_jobs", []):
                jobs_by_candidate.setdefault(int(job.get("candidate_id") or 0), []).append(job)
        enriched = []
        for row in payload.get("items", []):
            item = dict(row)
            jobs = jobs_by_candidate.get(int(item["id"]), [])
            item["analysis_jobs"] = jobs
            item["model_states"] = self._model_states(configured, jobs)
            item["no_plan_reason"] = self._candidate_no_plan_reason(configured, jobs)
            enriched.append(item)
        payload["items"] = enriched
        payload["configured_models"] = configured
        return payload

    def list_jobs_page(
        self,
        *,
        run_id: Optional[int] = None,
        candidate_id: Optional[int] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
        limit: int = 50,
    ) -> Dict[str, Any]:
        payload = self.repo.list_jobs_page(
            run_id=run_id,
            candidate_id=candidate_id,
            model=model,
            status=status,
            page=page,
            limit=limit,
        )
        payload["configured_models"] = list_analysis_models(self.config)
        payload["items"] = [self.project_observation_health(row) for row in payload["items"]]
        return payload

    @staticmethod
    def _automatic_new_work_gate(
        *,
        market: str,
        current_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Fail-closed gate for scheduler-created opportunity discovery/followups."""

        try:
            from src.core.trading_calendar import MarketPhase, build_market_phase_context, is_market_open

            context = build_market_phase_context(
                market=market,
                current_time=current_time,
                trigger_source="scheduler",
                analysis_intent="auto",
                analysis_phase="auto",
            )
            warnings = list(getattr(context, "warnings", []) or [])
            payload = context.to_dict()
            if warnings:
                return {
                    "allowed": False,
                    "reason": "calendar_untrusted",
                    "warnings": warnings,
                    **payload,
                }
            if context.phase == MarketPhase.NON_TRADING or context.is_trading_day is not True:
                return {
                    "allowed": False,
                    "reason": "market_not_open_for_new_opportunity_work",
                    **payload,
                }
            if not is_market_open(market, context.session_date, strict=True):
                return {
                    "allowed": False,
                    "reason": "market_not_open_for_new_opportunity_work",
                    **payload,
                }
            return {"allowed": True, "reason": None, **payload}
        except Exception as exc:
            logger.warning("机会池自动新任务日历闸门无法验证，已 fail-closed: %s", exc, exc_info=True)
            return {
                "allowed": False,
                "reason": "calendar_untrusted",
                "warnings": ["calendar_error"],
            }

    def _policy(self) -> OpportunityPolicy:
        effective_mode = self.runtime_settings_service.get_market_opportunity_mode()
        return self._policy_from_mode(effective_mode)

    def _policy_from_mode(self, effective_mode: Any) -> OpportunityPolicy:
        mode = str(effective_mode.value or "off").strip().lower()
        if mode not in VALID_MODES:
            mode = "off"
        raw_llm_budget = getattr(self.config, "market_opportunity_max_llm_calls_per_run", 16)
        llm_budget = 16 if raw_llm_budget is None else int(raw_llm_budget)
        raw_research_budget = getattr(self.config, "market_opportunity_research_daily_llm_budget", 16)
        research_budget = 16 if raw_research_budget is None else int(raw_research_budget)
        raw_risk_off_max = getattr(self.config, "market_opportunity_risk_off_max_candidates", 2)
        risk_off_max = 2 if raw_risk_off_max is None else int(raw_risk_off_max)
        raw_risk_off_score = getattr(self.config, "market_opportunity_risk_off_min_score", 80)
        risk_off_score = 80 if raw_risk_off_score is None else int(raw_risk_off_score)
        return OpportunityPolicy(
            mode=mode,
            mode_source=effective_mode.source,
            mode_version=effective_mode.version,
            discovery_max_themes=max(1, min(30, int(getattr(self.config, "market_opportunity_discovery_max_themes", 15) or 15))),
            max_themes=max(1, int(getattr(self.config, "market_opportunity_max_themes", 3) or 3)),
            max_candidates_per_theme=max(1, int(getattr(self.config, "market_opportunity_max_candidates_per_theme", 3) or 3)),
            max_total_candidates=max(1, int(getattr(self.config, "market_opportunity_max_total_candidates", 8) or 8)),
            max_observation_candidates=max(
                1,
                int(
                    getattr(
                        self.config,
                        "market_opportunity_max_observation_candidates",
                        max(30, int(getattr(self.config, "market_opportunity_max_total_candidates", 8) or 8)),
                    )
                    or 30
                ),
            ),
            max_llm_calls_per_run=max(0, llm_budget),
            min_20d_avg_amount_cny=max(0.0, float(getattr(self.config, "market_opportunity_min_20d_avg_amount_cny", 100000000.0) or 0.0)),
            blacklist=tuple(
                str(item).strip().lower()
                for item in getattr(self.config, "market_opportunity_blacklist", []) or []
                if str(item).strip()
            ),
            observe_gate_days=max(0, int(getattr(self.config, "market_opportunity_observe_gate_days", 3) or 0)),
            research_daily_llm_budget=max(0, research_budget),
            risk_off_max_candidates=max(0, risk_off_max),
            risk_off_min_score=max(0, min(100, risk_off_score)),
        )

    @staticmethod
    def _paper_llm_budget_block_reason(
        *,
        policy: OpportunityPolicy,
        models: Iterable[str],
        selected_candidate_count: int,
    ) -> Optional[str]:
        if policy.mode != "paper" or selected_candidate_count <= 0:
            return None
        model_count = len([str(model).strip() for model in models if str(model).strip()])
        if model_count <= 0:
            return "configured_models_missing"
        if policy.max_llm_calls_per_run <= 0:
            return "llm_budget_zero"
        if policy.max_llm_calls_per_run < model_count:
            return "llm_budget_insufficient_for_configured_models"
        return None

    def _score_themes_and_candidates(
        self,
        *,
        themes: List[Dict[str, Any]],
        policy: OpportunityPolicy,
        market: str,
        trade_date: date,
        regime: str,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        selected_theme_limit = policy.discovery_max_themes
        theme_threshold = 60.0
        candidate_threshold = 70.0
        legacy_theme_threshold = 75.0 if regime == "cautious" else 65.0
        theme_rows: List[Dict[str, Any]] = []
        candidate_rows_by_key: Dict[str, Dict[str, Any]] = {}
        scored_themes: List[Tuple[Dict[str, Any], BoardConstituentSnapshot]] = []

        for theme in themes:
            identity_error = self._industry_theme_identity_error(theme)
            if identity_error:
                theme_rows.append(
                    self._theme_identity_rejection_row(
                        theme=theme,
                        trade_date=trade_date,
                        reason=identity_error,
                    )
                )
                continue
            snapshot = self._fetch_board_snapshot(
                market=market,
                board_type=theme["theme_type"],
                board_name=theme["theme_name"],
                board_code=theme.get("theme_code"),
                as_of=trade_date,
            )
            snapshot_as_of = self._parse_date(getattr(snapshot, "as_of", None))
            snapshot_date_matches = snapshot_as_of == trade_date
            if snapshot_date_matches:
                theme_score, components, reasons = self._score_theme(theme, snapshot)
            else:
                theme_score = 0.0
                components = {
                    "theme_policy_version": THEME_POLICY_VERSION,
                    "snapshot_as_of": snapshot_as_of.isoformat() if snapshot_as_of else None,
                    "expected_as_of": trade_date.isoformat(),
                }
                reasons = ["constituent_snapshot_as_of_mismatch"]
            if self._theme_is_blacklisted(theme, policy):
                reasons.append("blacklisted_theme")
            status = "selected" if theme_score >= theme_threshold and not reasons else "rejected"
            if not reasons and 45.0 <= theme_score < theme_threshold:
                status = "observing"
            components["legacy_report_theme"] = bool(theme.get("legacy_report_theme", True))
            components["legacy_eligible"] = bool(
                components["legacy_report_theme"] and regime not in {"risk_off", "unavailable"}
                and float(components.get("legacy_score") or 0) >= legacy_theme_threshold
                and not components.get("legacy_reasons") and not reasons
            )
            components["legacy_candidate_threshold"] = 78.0 if regime == "cautious" else 70.0
            if (
                not snapshot_date_matches
                or snapshot.status not in {"ok", "partial"}
                or not snapshot.constituents
            ):
                status = "insufficient_data"
                if snapshot.status not in {"ok", "partial"} or not snapshot.constituents:
                    reasons.append("constituents_unavailable")
            theme_key = theme["theme_key"]
            row = {
                "theme_key": theme_key,
                "theme_type": theme["theme_type"],
                "theme_name": theme["theme_name"],
                "theme_code": theme.get("theme_code"),
                "theme_source": theme.get("theme_source"),
                "theme_taxonomy": theme.get("theme_taxonomy"),
                "rank": theme.get("rank"),
                "change_pct": theme.get("change_pct"),
                "status": status,
                "score": round(theme_score, 4),
                "score_components": components,
                "rejection_reasons": reasons,
                "data_quality": {
                    "snapshot_status": snapshot.status,
                    "snapshot_provider": snapshot.provider,
                    "snapshot_schema_version": snapshot.schema_version,
                    "snapshot_as_of": snapshot_as_of.isoformat() if snapshot_as_of else None,
                    "expected_as_of": trade_date.isoformat(),
                    "snapshot_date_matches": snapshot_date_matches,
                    "errors": snapshot.errors,
                    "ranking_source": theme.get("theme_source"),
                    "ranking_taxonomy": theme.get("theme_taxonomy"),
                },
                "constituent_snapshot": snapshot.to_dict(),
                "constituent_count": len(snapshot.constituents),
                "eligible_constituent_count": 0,
            }
            theme_rows.append(row)
            scored_themes.append((row, snapshot))

        eligible_themes = [
            (row, snapshot)
            for row, snapshot in scored_themes
            if row.get("status") in {"selected", "observing"}
        ]
        eligible_themes.sort(
            key=lambda item: (
                -float(item[0].get("score") or 0.0),
                int(item[0].get("rank") or 1_000_000),
                str(item[0].get("theme_key") or ""),
            )
        )
        selected_entries = eligible_themes[:selected_theme_limit]
        for row, _snapshot in eligible_themes[selected_theme_limit:]:
            row["status"] = "rejected"
            reasons = list(row.get("rejection_reasons") or [])
            reasons.append("over_total_theme_limit")
            row["rejection_reasons"] = reasons

        legacy_entries = sorted(
            [row for row, _ in scored_themes if row["score_components"].get("legacy_eligible")],
            key=lambda row: (-float(row["score_components"].get("legacy_score") or 0), int(row.get("rank") or 9999)),
        )
        legacy_allowed = {row["theme_key"] for row in legacy_entries[:min(policy.max_themes, 2 if regime == "cautious" else policy.max_themes)]}
        for row, _ in scored_themes:
            row["score_components"]["legacy_eligible"] = row["theme_key"] in legacy_allowed
        selected_themes = [row for row, _snapshot in selected_entries]
        for row, snapshot in selected_entries:
            theme_key = str(row.get("theme_key") or "")
            # Bound daily-data work while retaining liquid pullbacks as well as
            # advancing members.  This is an explicit sampled universe shared
            # by both policy comparisons, never an exhaustive-market claim.
            sample = self._candidate_constituent_sample(snapshot, policy)
            row["data_quality"]["candidate_sample_size"] = len(sample)
            row["data_quality"]["candidate_sample_total"] = len(snapshot.constituents)
            for constituent in sample:
                candidate = self._score_candidate(
                    constituent=constituent.to_dict(),
                    theme=row,
                    trade_date=trade_date,
                    policy=policy,
                    threshold=candidate_threshold,
                )
                existing = candidate_rows_by_key.get(candidate["candidate_key"])
                if existing is None or (candidate.get("score") or 0) > (existing.get("score") or 0):
                    candidate_rows_by_key[candidate["candidate_key"]] = candidate
                else:
                    related = existing.setdefault("related_themes", [])
                    related.append({"theme_key": theme_key, "theme_name": row.get("theme_name")})
            row["eligible_constituent_count"] = sum(
                1 for candidate in candidate_rows_by_key.values()
                if candidate.get("primary_theme_key") == theme_key and candidate.get("status") == "selected"
            )

        return selected_themes, theme_rows, self._candidate_presort_pool(
            candidate_rows_by_key.values(),
            policy=policy,
            regime=regime,
        )

    @staticmethod
    def _industry_theme_identity_error(theme: Mapping[str, Any]) -> Optional[str]:
        if str(theme.get("theme_type") or "").strip().lower() != "industry":
            return None
        source = str(theme.get("theme_source") or "").strip().lower()
        code = str(theme.get("theme_code") or "").strip()
        if not source or not code:
            return "source_identity_unavailable"
        if source == "eastmoney_industry":
            suffix = code.upper()[2:] if code.upper().startswith("BK") else ""
            return None if suffix.isdigit() else "source_identity_unavailable"
        if source == "csrc_tencent":
            suffix = code[len("csrc_"):] if code.startswith("csrc_") else ""
            return None if len(suffix) == 3 and suffix[0].isupper() and suffix[1:].isdigit() else "source_identity_unavailable"
        if source in {"sina_industry", "akshare_sina"}:
            suffix = code[len("hangye_"):] if code.startswith("hangye_") else ""
            return None if suffix.isalnum() else "source_identity_unavailable"
        return "source_identity_unavailable"

    @staticmethod
    def _theme_identity_rejection_row(
        *,
        theme: Mapping[str, Any],
        trade_date: date,
        reason: str,
    ) -> Dict[str, Any]:
        return {
            "theme_key": theme["theme_key"],
            "theme_type": theme["theme_type"],
            "theme_name": theme["theme_name"],
            "theme_code": theme.get("theme_code"),
            "theme_source": theme.get("theme_source"),
            "theme_taxonomy": theme.get("theme_taxonomy"),
            "rank": theme.get("rank"),
            "change_pct": theme.get("change_pct"),
            "status": "insufficient_data",
            "score": 0.0,
            "score_components": {
                "theme_policy_version": THEME_POLICY_VERSION,
                "expected_as_of": trade_date.isoformat(),
                "ranking_source": theme.get("theme_source"),
                "ranking_taxonomy": theme.get("theme_taxonomy"),
            },
            "rejection_reasons": [reason],
            "data_quality": {
                "snapshot_status": "not_requested",
                "snapshot_provider": None,
                "snapshot_schema_version": None,
                "snapshot_as_of": None,
                "expected_as_of": trade_date.isoformat(),
                "snapshot_date_matches": False,
                "errors": [reason],
                "ranking_source": theme.get("theme_source"),
                "ranking_taxonomy": theme.get("theme_taxonomy"),
            },
            "constituent_snapshot": {
                "status": "not_requested",
                "errors": [reason],
            },
            "constituent_count": 0,
            "eligible_constituent_count": 0,
        }

    def _fetch_board_snapshot(
        self,
        *,
        market: str,
        board_type: str,
        board_name: str,
        board_code: Optional[str],
        as_of: date,
    ) -> BoardConstituentSnapshot:
        frozen = self._frozen_constituents.get((market, as_of, board_type, board_code, board_name))
        if frozen is not None:
            return frozen
        return self.board_provider.get_board_constituents(
            market=market,
            board_type=board_type,
            board_name=board_name,
            board_code=board_code,
            as_of=as_of,
        )

    def _fetch_board_history(
        self,
        *,
        market: str,
        board_type: str,
        board_name: str,
        as_of: date,
    ) -> Optional[BoardHistorySnapshot]:
        get_history = getattr(self.board_provider, "get_board_history", None)
        if not callable(get_history):
            return None
        return get_history(
            market=market,
            board_type=board_type,
            board_name=board_name,
            as_of=as_of,
            lookback_days=10,
        )

    def _score_theme(self, theme: Dict[str, Any], snapshot: BoardConstituentSnapshot) -> Tuple[float, Dict[str, Any], List[str]]:
        rank = int(theme.get("rank") or 1)
        change_pct = self._finite_float(theme.get("change_pct")) or 0.0
        history = None if snapshot.provider in {"akshare_sina", "csrc_tencent"} else self._fetch_board_history(
            market=snapshot.market, board_type=snapshot.board_type,
            board_name=snapshot.board_name, as_of=snapshot.as_of,
        )
        breadth_metrics = self._theme_breadth_metrics(snapshot)
        legacy_persistence = self._theme_persistence_days(theme, snapshot, history)
        persistence_payload = (
            self._board_observations().persistence_for_theme(
                market=snapshot.market, as_of=snapshot.as_of,
                theme_key=theme["theme_key"], provider=snapshot.provider,
            ) if snapshot.provider == "csrc_tencent" else legacy_persistence
        )
        persistence_days = persistence_payload.get("days")
        required_window = int(persistence_payload.get("required_window_size") or 0)
        available_window = int(persistence_payload.get("available_window_size") or 0)
        history_complete = (
            required_window > 0 and available_window >= required_window
            if snapshot.provider == "csrc_tencent" else persistence_days is not None
        )
        legacy_days = legacy_persistence.get("days")
        legacy_ranking = max(0.0, min(25.0, 25.0 - (rank - 1) * 2.0 + max(change_pct, 0) * 0.8))
        daily_ranking_strength = max(0.0, min(20.0, 20.0 - (rank - 1) * 0.6 + max(change_pct, 0) * 0.5))
        discovery_components = BoardObservationService.discovery_score_components(theme, persistence_payload)
        medium_term_adjustment = (discovery_components["trend_20d"] or 0.0) * 0.5
        ranking_strength = max(0.0, min(20.0, daily_ranking_strength + medium_term_adjustment))
        breadth = min(25.0, float(breadth_metrics.get("advancing_ratio") or 0.0) * 25.0)
        amount_coverage = float(breadth_metrics.get("amount_coverage_ratio") or 0.0)
        volume_expansion = float(breadth_metrics.get("volume_expansion_ratio") or 0.0)
        liquidity = min(20.0, amount_coverage * 15.0 + volume_expansion * 5.0)
        completeness = 25.0 if snapshot.status == "ok" else (15.0 if snapshot.status == "partial" else 0.0)
        persistence = min(10.0, persistence_days * 2.0) if persistence_days is not None else 0.0
        score = ranking_strength + breadth + liquidity + completeness + persistence
        reasons: List[str] = []
        if snapshot.status not in {"ok", "partial"}:
            reasons.append("constituent_snapshot_failed")
        if len(snapshot.constituents) < 5:
            reasons.append("too_few_constituents")
        if not breadth_metrics.get("ok"):
            reasons.append(str(breadth_metrics.get("reason") or "theme_breadth_unavailable"))
        legacy_reasons = list(reasons)
        if legacy_days is None or legacy_days < 3:
            legacy_reasons.append("theme_persistence_unavailable")
        legacy_score = (
            legacy_ranking + breadth + min(15.0, amount_coverage * 10.0 + volume_expansion * 5.0)
            + (10.0 if snapshot.status == "ok" else 5.0 if snapshot.status == "partial" else 0.0)
            + (min(25.0, legacy_days * 5.0) if legacy_days is not None else 0.0)
        )
        return score, {
            "ranking_strength": ranking_strength, "breadth": breadth,
            "daily_ranking_strength": daily_ranking_strength,
            "medium_term_adjustment": medium_term_adjustment,
            "breadth_source": "constituent_quotes_or_daily_bars",
            "valid_constituent_count": breadth_metrics.get("valid_count"),
            "advancing_constituent_count": breadth_metrics.get("advancing_count"),
            "advancing_ratio": breadth_metrics.get("advancing_ratio"),
            "persistence": persistence, "persistence_days": persistence_days,
            "persistence_source": persistence_payload.get("source"),
            "persistence_window_dates": persistence_payload.get("window_dates"),
            "persistence_positive_dates": persistence_payload.get("positive_dates"),
            "required_window_size": persistence_payload.get("required_window_size"),
            "available_window_size": persistence_payload.get("available_window_size"),
            "missing_dates": persistence_payload.get("missing_dates", []),
            "return_5d": persistence_payload.get("return_5d"),
            "return_20d": persistence_payload.get("return_20d"),
            "return_20d_evidence": persistence_payload.get("return_20d_evidence"),
            "discovery_score": BoardObservationService._discovery_score(theme, persistence_payload),
            "discovery_score_components": discovery_components,
            "evidence_stage": "established" if history_complete else "cold_start",
            "board_history_status": getattr(history, "status", None),
            "liquidity_participation": liquidity, "amount_coverage_ratio": amount_coverage,
            "volume_expansion_ratio": volume_expansion, "data_completeness": completeness,
            "theme_policy_version": THEME_POLICY_VERSION,
            "legacy_score": legacy_score, "legacy_reasons": legacy_reasons,
        }, reasons

    @staticmethod
    def _finite_evidence(value: Any) -> Any:
        """Keep missing metrics explicit and valid in API/JSON snapshots."""
        if isinstance(value, dict):
            return {key: MarketOpportunityService._finite_evidence(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [MarketOpportunityService._finite_evidence(item) for item in value]
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value

    def _candidate_constituent_sample(self, snapshot: BoardConstituentSnapshot, policy: OpportunityPolicy) -> List[Any]:
        limit = max(policy.max_candidates_per_theme, policy.max_candidates_per_theme * 4)
        def amount(item: Any) -> float:
            raw = getattr(item, "raw", {}) or {}
            return self._finite_float(raw.get("amount", raw.get("成交额"))) or 0.0
        return sorted(snapshot.constituents, key=lambda item: (-amount(item), item.symbol))[:limit]

    def _theme_breadth_metrics(self, snapshot: BoardConstituentSnapshot) -> Dict[str, Any]:
        constituents = list(snapshot.constituents or [])
        if not constituents:
            return {"ok": False, "reason": "no_constituents"}
        valid_count = 0
        advancing_count = 0
        amount_count = 0
        volume_expansion_count = 0
        sample_limit = min(len(constituents), 80)
        fallback_fetch_limit = min(sample_limit, 30)
        fallback_fetches = 0
        for constituent in constituents[:sample_limit]:
            raw = dict(getattr(constituent, "raw", {}) or {})
            raw_change = self._finite_float(
                raw.get("涨跌幅", raw.get("change_pct", raw.get("pct_chg")))
            )
            raw_amount = self._finite_float(raw.get("成交额", raw.get("amount")))
            raw_volume_ratio = self._finite_float(raw.get("量比", raw.get("volume_ratio")))
            if raw_change is not None:
                valid_count += 1
                if raw_change > 0:
                    advancing_count += 1
                if raw_amount is not None and raw_amount > 0:
                    amount_count += 1
                if raw_volume_ratio is not None and raw_volume_ratio >= 1.2:
                    volume_expansion_count += 1
                continue
            if fallback_fetches >= fallback_fetch_limit:
                continue
            fallback_fetches += 1
            symbol = str(getattr(constituent, "symbol", "") or getattr(constituent, "code", "") or "")
            bars = self._daily_bars(symbol, snapshot.as_of)
            if bars is None or len(bars) < 2:
                continue
            frame = bars.copy()
            if "date" in frame.columns:
                frame = frame.sort_values("date")
            frame = frame[frame["date"].apply(lambda value: pd.to_datetime(value).date() <= snapshot.as_of)] if "date" in frame.columns else frame
            if len(frame) < 2:
                continue
            last = frame.iloc[-1]
            prev = frame.iloc[-2]
            close = self._finite_float(last.get("close"))
            prev_close = self._finite_float(prev.get("close"))
            if close is None or prev_close is None or close <= 0 or prev_close <= 0:
                continue
            valid_count += 1
            pct_chg = self._finite_float(last.get("pct_chg"))
            if (pct_chg is not None and pct_chg > 0) or (pct_chg is None and close > prev_close):
                advancing_count += 1
            amount = self._finite_float(last.get("amount"))
            if amount is not None and amount > 0:
                amount_count += 1
            volume = self._finite_float(last.get("volume"))
            if volume is not None and volume > 0 and "volume" in frame.columns and len(frame) >= 6:
                prior_avg = pd.to_numeric(frame["volume"].iloc[-6:-1], errors="coerce").dropna().mean()
                if prior_avg and math.isfinite(float(prior_avg)) and volume >= float(prior_avg) * 1.2:
                    volume_expansion_count += 1
        min_valid = min(10, max(5, sample_limit // 5))
        if valid_count < min_valid:
            return {
                "ok": False,
                "reason": "insufficient_constituent_daily_bars",
                "sample_count": sample_limit,
                "valid_count": valid_count,
                "fallback_daily_fetches": fallback_fetches,
            }
        return {
            "ok": True,
            "sample_count": sample_limit,
            "valid_count": valid_count,
            "fallback_daily_fetches": fallback_fetches,
            "advancing_count": advancing_count,
            "advancing_ratio": round(advancing_count / valid_count, 4),
            "amount_coverage_ratio": round(amount_count / valid_count, 4),
            "volume_expansion_ratio": round(volume_expansion_count / valid_count, 4),
        }

    def _theme_persistence_days(
        self,
        theme: Dict[str, Any],
        snapshot: BoardConstituentSnapshot,
        history: Optional[BoardHistorySnapshot],
    ) -> Dict[str, Any]:
        # LLM-produced market-review payloads may mention "active days", but
        # executable selection must be based on independently observed market
        # data.  Payload persistence is therefore ignored here and can only be
        # used by the UI as descriptive text outside this gate.
        if history is not None and str(getattr(history, "status", "") or "") == "ok":
            bars = list(getattr(history, "bars", []) or [])
            if len(bars) >= 6:
                recent = bars[-5:]
                valid_changes = [
                    bar.change_pct
                    for bar in recent
                    if bar.change_pct is not None and math.isfinite(float(bar.change_pct))
                ]
                if len(valid_changes) >= 5:
                    return {
                        "days": sum(1 for value in valid_changes if float(value) > 0),
                        "source": "board_history_snapshot",
                        "window_dates": [bar.date.isoformat() for bar in recent],
                        "positive_dates": [
                            bar.date.isoformat()
                            for bar in recent
                            if bar.change_pct is not None and math.isfinite(float(bar.change_pct)) and float(bar.change_pct) > 0
                        ],
                    }
        return self._theme_persistence_from_prior_runs(theme=theme, snapshot=snapshot)

    def _theme_persistence_from_prior_runs(
        self,
        *,
        theme: Dict[str, Any],
        snapshot: BoardConstituentSnapshot,
        window_size: int = 5,
    ) -> Dict[str, Any]:
        provider = str(getattr(snapshot, "provider", "") or "").strip()
        current_change = self._finite_float(theme.get("change_pct"))
        prior = self.repo.theme_persistence_window(
            market=snapshot.market,
            as_of=snapshot.as_of,
            theme_key=str(theme.get("theme_key") or ""),
            provider=provider,
            window_size=window_size,
        )
        observations = list(prior)
        observations.append(
            {
                "trade_date": snapshot.as_of,
                "present": True,
                "provider_matches": True,
                "status": "current",
                "change_pct": current_change,
            }
        )
        if len(observations) < window_size:
            return {
                "days": None,
                "source": "persisted_theme_observations",
                "window_dates": [
                    item["trade_date"].isoformat() if hasattr(item.get("trade_date"), "isoformat") else str(item.get("trade_date"))
                    for item in observations
                ],
                "positive_dates": [],
                "required_window_size": window_size,
                "available_window_size": len(observations),
                "missing_dates": [],
                "history_state": "cold_start",
            }
        positive_dates: List[str] = []
        for item in observations[-window_size:]:
            value = self._finite_float(item.get("change_pct"))
            qualifies = bool(item.get("present")) and bool(item.get("provider_matches")) and value is not None and value > 0
            if qualifies:
                trade_date = item.get("trade_date")
                positive_dates.append(trade_date.isoformat() if hasattr(trade_date, "isoformat") else str(trade_date))
        return {
            "days": len(positive_dates),
            "source": "persisted_theme_observations",
            "window_dates": [
                item["trade_date"].isoformat() if hasattr(item.get("trade_date"), "isoformat") else str(item.get("trade_date"))
                for item in observations[-window_size:]
            ],
            "positive_dates": positive_dates,
            "required_window_size": window_size,
            "available_window_size": len(observations[-window_size:]),
            "missing_dates": [],
            "history_state": "confirmed" if len(positive_dates) >= 3 else "established_negative",
        }

    def _score_candidate(
        self,
        *,
        constituent: Dict[str, Any],
        theme: Dict[str, Any],
        trade_date: date,
        policy: OpportunityPolicy,
        threshold: float,
    ) -> Dict[str, Any]:
        code = normalize_stock_code(str(constituent.get("code") or ""))
        exchange = str(constituent.get("exchange") or "").lower()
        symbol = str(constituent.get("symbol") or f"{code}.{exchange.upper()}")
        reasons: List[str] = []
        components: Dict[str, Any] = {"candidate_policy_version": CANDIDATE_POLICY_VERSION}
        quote_snapshot: Dict[str, Any] = {}
        score = 0.0
        if self._candidate_is_blacklisted(market="cn", exchange=exchange, code=code, symbol=symbol, policy=policy):
            reasons.append("blacklisted")
        if exchange not in {"sh", "sz"} or not code.isdigit() or len(code) != 6:
            reasons.append("ambiguous_stock_identity")
        if code.startswith(("51", "52", "56", "58", "15", "16", "18")):
            reasons.append("etf_not_supported")
        name = str(constituent.get("name") or "").strip()
        if "ST" in name.upper() or "退" in name:
            reasons.append("st_or_delisting")

        if not reasons:
            style_context = self._candidate_style_context(
                market="cn",
                stock_code=symbol or code,
            )
            components["trading_style"] = style_context.get("trading_style")
            components["trading_style_source"] = style_context.get("source")
            components["trading_style_policy_version"] = style_context.get("policy_version")
            components["trading_style_core_ma_windows"] = (
                style_context.get("settings", {}).get("core_ma_windows")
                if isinstance(style_context.get("settings"), Mapping)
                else None
            )
            settings = style_context.get("settings") or {}
            core_windows = settings.get("core_ma_windows") or []
            if not core_windows:
                reasons.append("trading_style_unavailable")
            history_days = int(settings.get("analysis_window_sessions") or 180)
            volatility_windows = settings.get("volatility_windows") or []
            required_bars = max(
                [history_days, 60, *core_windows, *volatility_windows]
            )
            bars = self._daily_bars(symbol, trade_date, days=history_days)
            benchmark_bars = self._benchmark_bars(trade_date, days=history_days)
            if bars is None or len(bars) < required_bars:
                reasons.append("insufficient_daily_bars")
                available = 0 if bars is None else len(bars)
                # A current, finite short history is a legitimate eligibility
                # exclusion (e.g. a newly listed stock), not a failed daily run.
                short_history = bool(available and self._last_bar_date(bars) == trade_date
                                     and self._finite_unique_bar_dates(bars) == available)
                components["available_daily_bars"] = available
                components["required_daily_bars"] = required_bars
                components["history_status"] = "short_history" if short_history else "data_unavailable"
                if not short_history:
                    reasons.append("daily_data_unavailable")
            elif benchmark_bars is None or len(benchmark_bars) < required_bars:
                reasons.append("relative_strength_benchmark_unavailable")
            else:
                metrics = self._candidate_metrics(
                    bars,
                    benchmark_bars=benchmark_bars,
                    style_context=style_context,
                )
                quote_snapshot = metrics.get("quote_snapshot", {})
                components.update(metrics.get("components", {}))
                if not components.get("style_history_sufficient"):
                    reasons.append("insufficient_style_history")
                if not self._metrics_are_finite(metrics):
                    reasons.append("non_finite_metrics")
                if metrics["avg_amount_20"] < policy.min_20d_avg_amount_cny:
                    reasons.append("low_20d_amount")
                if metrics["last_volume"] <= 0 or metrics["last_amount"] <= 0:
                    reasons.append("suspended_or_untradable")
                if metrics.get("one_price_bar"):
                    try:
                        limits = self.fetcher_manager.get_limit_prices(code, trade_date=trade_date.isoformat(), market="cn")
                    except Exception:
                        limits = None
                    quote_snapshot["limit_prices"] = limits if isinstance(limits, dict) else None
                    if not isinstance(limits, dict) or self._parse_date(limits.get("trade_date")) != trade_date:
                        reasons.append("limit_prices_unavailable")
                    else:
                        bounds = [self._finite_float(limits.get(key)) for key in ("up_limit", "down_limit")]
                        if any(bound is None or bound <= 0 for bound in bounds):
                            reasons.append("limit_prices_unavailable")
                        elif any(abs(float(quote_snapshot["close"]) - bound) < 0.005 for bound in bounds):
                            reasons.append("one_price_limit_untradable")
                if metrics["last_date"] > trade_date:
                    reasons.append("future_bar_rejected")
                if metrics["last_date"] != trade_date:
                    reasons.append("daily_bar_not_as_of_trade_date")
                if metrics["benchmark_last_date"] != trade_date:
                    reasons.append("benchmark_bar_not_as_of_trade_date")
                score = (
                    min(20.0, float(theme.get("score") or 0) / 5.0)
                    + metrics["relative_strength_score"]
                    + metrics["trend_score"]
                    + metrics["liquidity_score"]
                    + metrics["price_structure_score"]
                    + metrics["risk_score"]
                )

        theme_components = theme.get("score_components") or {}
        components["evidence_stage"] = theme_components.get("evidence_stage", "cold_start")
        components["theme_persistence_source"] = theme_components.get("persistence_source")
        data_complete = bool(quote_snapshot) and not reasons
        analysis_reasons = []
        if score < threshold:
            analysis_reasons.append("below_analysis_threshold")
        if theme.get("status", "selected") != "selected":
            analysis_reasons.append("theme_below_analysis_threshold")
        if components.get("setup_type") not in {"pullback", "trend_continuation"}:
            analysis_reasons.append("entry_setup_unconfirmed")
        status = "selected" if data_complete and not analysis_reasons else "observing" if data_complete else "rejected"
        legacy_score = score - min(20.0, float(theme.get("score") or 0) / 5.0) + min(20.0, float(theme_components.get("legacy_score", theme.get("score") or 0)) / 5.0)
        legacy_reasons = list(reasons) + list(theme_components.get("legacy_reasons") or [])
        if not theme_components.get("legacy_eligible", True):
            legacy_reasons.append("legacy_theme_not_selected")
        if legacy_score < float(theme_components.get("legacy_candidate_threshold") or threshold):
            legacy_reasons.append("below_legacy_analysis_threshold")
        components["strategy_comparison"] = {
            "legacy_selected": data_complete and not legacy_reasons,
            "legacy_score": round(legacy_score, 4) if math.isfinite(legacy_score) else None,
            "legacy_reasons": legacy_reasons,
            "current_selected": status == "selected",
            "policy_version": CANDIDATE_POLICY_VERSION,
            "comparison_scope": "same_observed_constituent_sample",
        }
        if data_complete:
            reasons = analysis_reasons
        components = self._finite_evidence(components)
        quote_snapshot = self._finite_evidence(quote_snapshot)
        return {
            "candidate_key": f"cn:{exchange}:{code}",
            "market": "cn",
            "exchange": exchange,
            "stock_code": code,
            "symbol": symbol,
            "stock_name": name,
            "primary_theme_key": theme.get("theme_key"),
            "related_themes": [{"theme_key": theme.get("theme_key"), "theme_name": theme.get("theme_name")}],
            "status": status,
            "rank": None,
            "score": round(score, 4) if math.isfinite(score) else None,
            "score_components": components,
            "rejection_reasons": reasons,
            "data_quality": {
                "as_of": trade_date.isoformat(),
                "source": "daily_bars",
                "daily_bar_source": quote_snapshot.get("source"),
                "benchmark_source": quote_snapshot.get("benchmark_source"),
                "trading_style": components.get("trading_style"),
                "trading_style_source": components.get("trading_style_source"),
                "missing_evidence": ["theme_history_warming_up"] if components.get("evidence_stage") == "cold_start" else [],
            },
            "quote_snapshot": quote_snapshot,
            "as_of": trade_date,
            "valid_until": self._next_trading_day("cn", trade_date),
            "snapshot_hash": self._stable_hash({
                "policy": CANDIDATE_POLICY_VERSION, "symbol": symbol, "as_of": trade_date.isoformat(),
                "theme": theme, "score": score, "components": components,
                "quote": quote_snapshot, "reasons": reasons,
            }),
        }

    @staticmethod
    def _theme_is_blacklisted(theme: Dict[str, Any], policy: OpportunityPolicy) -> bool:
        blacklist = set(policy.blacklist)
        if not blacklist:
            return False
        tokens = {
            str(theme.get("theme_key") or "").strip().lower(),
            str(theme.get("theme_name") or "").strip().lower(),
            str(theme.get("theme_code") or "").strip().lower(),
        }
        return any(token and token in blacklist for token in tokens)

    @staticmethod
    def _candidate_is_blacklisted(
        *,
        market: str,
        exchange: str,
        code: str,
        symbol: str,
        policy: OpportunityPolicy,
    ) -> bool:
        blacklist = set(policy.blacklist)
        if not blacklist:
            return False
        normalized_symbol = str(symbol or "").strip().lower()
        normalized_exchange = str(exchange or "").strip().lower()
        normalized_code = normalize_stock_code(str(code or "").strip())
        tokens = {
            f"{str(market or 'cn').strip().lower()}:{normalized_exchange}:{normalized_code}",
            normalized_symbol,
            f"{normalized_code}.{normalized_exchange.upper()}".lower(),
            f"{normalized_exchange}{normalized_code}",
            f"{normalized_exchange}.{normalized_code}",
        }
        return any(token and token in blacklist for token in tokens)

    def _candidate_metrics(
        self,
        bars: pd.DataFrame,
        *,
        benchmark_bars: pd.DataFrame,
        style_context: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        frame = bars.copy()
        for column in ("open", "high", "low", "close", "volume", "amount", "pct_chg"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        if "date" in frame.columns:
            frame = frame.sort_values("date")
        frame = frame.dropna(subset=["close"])
        benchmark = benchmark_bars.copy()
        benchmark["close"] = pd.to_numeric(benchmark["close"], errors="coerce")
        if "date" in benchmark.columns:
            benchmark = benchmark.sort_values("date")
        benchmark = benchmark.dropna(subset=["close"])
        context = style_context or self._candidate_style_context(market="cn", stock_code="")
        settings = context.get("settings") or {}
        core_windows = list(settings.get("core_ma_windows") or [])
        return_window = max(core_windows) if core_windows else 60
        volatility_window = max(settings.get("volatility_windows") or [20])
        analysis_window_sessions = max(1, int(settings.get("analysis_window_sessions") or 180))
        required_bars = max(60, return_window + 1, volatility_window + 1, analysis_window_sessions)
        stock_unique_dates = self._finite_unique_bar_dates(frame)
        benchmark_unique_dates = self._finite_unique_bar_dates(benchmark)
        history_sufficient = (
            stock_unique_dates >= required_bars
            and benchmark_unique_dates >= required_bars
        )
        style_mas = {w: float(frame["close"].tail(w).mean()) for w in core_windows if len(frame) >= w}
        last = frame.iloc[-1]
        benchmark_last = benchmark.iloc[-1]
        close = float(last["close"])
        open_price = float(last.get("open", close))
        high = float(last.get("high", close))
        low = float(last.get("low", close))
        last_volume = float(last.get("volume", 0.0) or 0.0)
        last_amount = float(last.get("amount", 0.0) or 0.0)
        pct_chg = float(last.get("pct_chg", 0.0) or 0.0)
        ma5 = float(frame["close"].tail(5).mean())
        ma10 = float(frame["close"].tail(10).mean())
        ma20 = float(frame["close"].tail(20).mean())
        ma60 = float(frame["close"].tail(60).mean())
        ret5 = close / float(frame["close"].iloc[-6]) - 1 if len(frame) >= 6 else 0.0
        ret20 = close / float(frame["close"].iloc[-21]) - 1 if len(frame) >= 21 else 0.0
        benchmark_ret20 = (
            float(benchmark["close"].iloc[-1]) / float(benchmark["close"].iloc[-21]) - 1
            if len(benchmark) >= 21
            else 0.0
        )
        excess_ret20 = ret20 - benchmark_ret20
        style_return = close / float(frame["close"].iloc[-return_window - 1]) - 1 if history_sufficient else float("nan")
        benchmark_return = (
            float(benchmark["close"].iloc[-1]) / float(benchmark["close"].iloc[-return_window - 1]) - 1
            if history_sufficient else float("nan")
        )
        # Normalize excess return to the existing 20-session score scale;
        # different holding styles must not get a larger score just for time.
        normalized_excess = ((1 + style_return) ** (20 / return_window)
                             - (1 + benchmark_return) ** (20 / return_window))
        avg_amount_20 = float(frame["amount"].tail(20).mean()) if "amount" in frame else 0.0
        style_payload = self._candidate_style_score_payload(
            close=close,
            ma5=ma5,
            ma10=ma10,
            ma20=ma20,
            ma60=ma60,
            style_context=context,
            moving_averages=style_mas,
        )
        trend_score = style_payload["trend_score"]
        relative_strength_score = max(0.0, min(20.0, 10.0 + normalized_excess * 100.0)) if history_sufficient else float("nan")
        liquidity_score = max(0.0, min(15.0, avg_amount_20 / 100000000.0 * 7.5))
        structure_window = return_window
        structure_ma = style_mas.get(structure_window)
        structure_return = (
            close / float(frame["close"].iloc[-structure_window - 1]) - 1
            if len(frame) >= structure_window + 1
            else float("nan")
        )
        price_structure_score = (
            15.0
            if (
                structure_ma is not None
                and math.isfinite(float(structure_ma))
                and math.isfinite(structure_return)
                and close >= float(structure_ma)
                and structure_return >= -0.03
            )
            else 7.0
        )
        volatility = float(frame["close"].pct_change().tail(volatility_window).std() or 0.0)
        risk_score = max(0.0, min(10.0, 10.0 - volatility * 100.0))
        last_date = pd.to_datetime(last["date"]).date() if "date" in frame else date.today()
        benchmark_last_date = pd.to_datetime(benchmark_last["date"]).date() if "date" in benchmark else date.today()
        source_name = str(frame.attrs.get("source_name") or "").strip() or None
        benchmark_source_name = str(benchmark.attrs.get("source_name") or "").strip() or None
        one_price_bar = math.isclose(open_price, high) and math.isclose(high, low) and math.isclose(low, close)
        # Limit status is established from dated exchange limits by the caller,
        # never from a universal 9.5% approximation across 10%/20% boards.
        one_price_limit = False
        medium_ma = style_mas.get(core_windows[-1]) if core_windows else None
        prior_medium = float(frame["close"].iloc[-core_windows[-1]-5:-5].mean()) if core_windows and len(frame) >= core_windows[-1] + 5 else None
        trend_intact = bool(medium_ma and prior_medium and close >= medium_ma and medium_ma >= prior_medium)
        timing_ma = style_mas.get(20) or structure_ma
        prior_volume = float(frame["volume"].iloc[-21:-1].mean()) if "volume" in frame and len(frame) >= 21 else 0.0
        pullback = bool(trend_intact and timing_ma and abs(close / timing_ma - 1) <= 0.04
                        and last_volume > 0 and prior_volume > 0 and last_volume < prior_volume
                        and close >= open_price and close >= float(frame["close"].iloc[-2]))
        continuation = bool(
            trend_intact
            and timing_ma
            and close >= timing_ma
            and ret20 >= 0
            # A-share continuation should not require strict benchmark outperformance
            # before the opportunity pool is allowed to observe/analyze it.  Keep
            # the absolute trend and 20-session return gates, but allow a small
            # neutral/slightly-lagging band so equal-index moves are not rejected
            # as entry_setup_unconfirmed.
            and (not math.isfinite(normalized_excess) or normalized_excess >= -0.01)
        )
        setup_type = "pullback" if pullback else "trend_continuation" if continuation else "unconfirmed"
        return {
            "avg_amount_20": avg_amount_20,
            "last_amount": last_amount,
            "last_volume": last_volume,
            "last_date": last_date,
            "benchmark_last_date": benchmark_last_date,
            "one_price_limit": one_price_limit,
            "one_price_bar": one_price_bar,
            "relative_strength_score": relative_strength_score,
            "trend_score": trend_score,
            "liquidity_score": liquidity_score,
            "price_structure_score": price_structure_score,
            "risk_score": risk_score,
            "components": {
                "setup_type": setup_type,
                "setup_evidence": {"trend_intact": trend_intact, "shrinking_volume": bool(prior_volume > 0 and last_volume < prior_volume), "support_ma": timing_ma},
                "theme_confirmation": "from_primary_theme_score",
                "ret5_pct": round(ret5 * 100, 4),
                "ret20_pct": round(ret20 * 100, 4),
                "benchmark_ret20_pct": round(benchmark_ret20 * 100, 4),
                "excess_ret20_pct": round(excess_ret20 * 100, 4),
                "ma5": ma5,
                "ma10": ma10,
                "ma20": ma20,
                "ma60": ma60,
                "ma60_relation_pct": round((close / ma60 - 1.0) * 100.0, 4) if ma60 > 0 else None,
                "style_medium_term_alignment": style_payload["medium_term_alignment"],
                "style_trend_adjustment": style_payload["style_trend_adjustment"],
                "style_trend_basis": style_payload["style_trend_basis"],
                "style_core_moving_averages": {f"ma{w}": value for w, value in style_mas.items()},
                "style_history_sufficient": history_sufficient and bool(core_windows),
                "style_required_history_sessions": required_bars,
                "style_bars_observed": stock_unique_dates,
                "style_benchmark_bars_observed": benchmark_unique_dates,
                "style_analysis_window_sessions": analysis_window_sessions,
                "style_return_window": return_window,
                "style_return_pct": style_return * 100 if history_sufficient else None,
                "style_benchmark_return_pct": benchmark_return * 100 if history_sufficient else None,
                "style_price_structure_window": structure_window,
                "style_price_structure_ma": structure_ma,
                "style_price_structure_return_pct": (
                    round(structure_return * 100, 4)
                    if math.isfinite(structure_return)
                    else None
                ),
                "style_volatility_window": volatility_window,
                "style_volatility": volatility,
                "short_timing_basis": "configured style return window and matching style MA; auxiliary price-structure score only",
                "avg_amount_20": avg_amount_20,
                "volatility_20": float(frame["close"].pct_change().tail(20).std() or 0.0),
                "relative_strength": relative_strength_score,
                "trend_quality": trend_score,
                "liquidity": liquidity_score,
                "price_structure": price_structure_score,
                "risk_adjustment": risk_score,
                "one_price_limit": one_price_limit,
            },
            "quote_snapshot": {
                "last_date": last_date.isoformat(),
                "benchmark_last_date": benchmark_last_date.isoformat(),
                "close": close,
                "avg_amount_20": avg_amount_20,
                "source": source_name,
                "benchmark_source": benchmark_source_name,
            },
        }

    def _daily_bars(self, code: str, trade_date: date, *, days: Optional[int] = None) -> Optional[pd.DataFrame]:
        if days is None:
            settings = self._candidate_style_context(market="cn", stock_code=code).get("settings") or {}
            days = int(settings.get("analysis_window_sessions") or 180)
        cache_key = (str(code), trade_date, int(days))
        if cache_key in self._daily_bar_cache:
            return self._daily_bar_cache[cache_key]
        self._daily_bar_cache[cache_key] = None
        start_date = (trade_date - timedelta(days=days * 2)).isoformat()
        end_date = trade_date.isoformat()
        try:
            data_requirements = {"required_fields": ["amount"], "required_tail_rows": 20} if isinstance(self.fetcher_manager, DataFetcherManager) else {}
            result = self.fetcher_manager.get_daily_data(code, start_date=start_date, end_date=end_date, days=days, **data_requirements)
        except Exception as exc:
            logger.info("daily bars unavailable for %s: %s", code, exc)
            return None
        source_name = None
        bars = result
        if isinstance(result, tuple):
            bars = result[0] if len(result) >= 1 else None
            source_name = str(result[1] or "").strip() if len(result) >= 2 else None
        if bars is None or bars.empty:
            return None
        bars = bars.copy()
        if "date" in bars.columns:
            bars = bars.sort_values("date")
        if source_name:
            bars.attrs["source_name"] = source_name
        self._daily_bar_cache[cache_key] = bars
        return bars

    def _benchmark_bars(self, trade_date: date, *, days: Optional[int] = None) -> Optional[pd.DataFrame]:
        # CSI 300 is used only as a broad A-share baseline for excess-return
        # evidence.  If the provider cannot return it, opportunity preselection
        # fails closed instead of relabeling absolute return as relative strength.
        index_fetch = getattr(self.fetcher_manager, "get_index_daily_data", None)
        if callable(index_fetch):
            history_days = int(days or 180)
            cache_key = ("index:sh000300", trade_date, history_days)
            if cache_key not in self._daily_bar_cache:
                self._daily_bar_cache[cache_key] = None
                try:
                    result = index_fetch(
                        "sh000300", start_date=(trade_date - timedelta(days=history_days * 2)).isoformat(),
                        end_date=trade_date.isoformat(), days=history_days,
                    )
                    frame, source = result if isinstance(result, tuple) else (result, None)
                    if frame is not None and not frame.empty:
                        frame = frame.copy().sort_values("date")
                        frame = frame[pd.to_datetime(frame["date"]).dt.date <= trade_date]
                        frame.attrs["source_name"] = source
                        self._daily_bar_cache[cache_key] = frame
                except Exception as exc:
                    logger.info("CSI 300 daily history unavailable: %s", exc)
            return self._daily_bar_cache[cache_key]
        fallback: Optional[pd.DataFrame] = None
        for code in ("000300.SH", "sh000300", "000300"):
            bars = self._daily_bars(code, trade_date, days=days)
            if bars is None or len(bars) < 60:
                continue
            if self._last_bar_date(bars) == trade_date:
                return bars
            if fallback is None:
                fallback = bars
        return fallback

    @staticmethod
    def _finite_unique_bar_dates(frame: pd.DataFrame) -> int:
        if frame is None or frame.empty or "close" not in frame.columns:
            return 0
        valid = frame[pd.to_numeric(frame["close"], errors="coerce").map(math.isfinite)]
        if valid.empty:
            return 0
        if "date" not in valid.columns:
            return len(valid)
        dates = pd.to_datetime(valid["date"], errors="coerce").dropna()
        return int(dates.dt.date.nunique())

    @staticmethod
    def _metrics_are_finite(metrics: Dict[str, Any]) -> bool:
        required = (
            "avg_amount_20",
            "last_amount",
            "last_volume",
            "relative_strength_score",
            "trend_score",
            "liquidity_score",
            "price_structure_score",
            "risk_score",
        )
        for key in required:
            value = metrics.get(key)
            if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                return False
        return True

    def _next_trading_day(self, market: str, after_date: date) -> date:
        try:
            from src.core.trading_calendar import is_market_open

            cursor = after_date + timedelta(days=1)
            for _ in range(14):
                if is_market_open(market, cursor):
                    return cursor
                cursor += timedelta(days=1)
        except Exception:
            pass
        cursor = after_date + timedelta(days=1)
        while cursor.weekday() >= 5:
            cursor += timedelta(days=1)
        return cursor

    def _rank_and_cap_candidates(self, candidates: List[Dict[str, Any]], *, policy: OpportunityPolicy, regime: str) -> List[Dict[str, Any]]:
        # Freeze both decisions on the same dated sample before any future bars
        # are visible.  Old thresholds and caps remain independent of new ones.
        legacy_allowed = set()
        legacy_counts: Dict[str, int] = {}
        legacy_limit = min(policy.max_total_candidates, 4 if regime == "cautious" else policy.max_total_candidates)
        for item in sorted(candidates, key=lambda row: -float((row.get("score_components") or {}).get("strategy_comparison", {}).get("legacy_score") or 0)):
            comparison = (item.get("score_components") or {}).get("strategy_comparison") or {}
            theme = str(item.get("primary_theme_key") or "")
            if comparison.get("legacy_selected") and len(legacy_allowed) < legacy_limit and legacy_counts.get(theme, 0) < policy.max_candidates_per_theme:
                legacy_allowed.add(item["candidate_key"])
                legacy_counts[theme] = legacy_counts.get(theme, 0) + 1
        allowed = set()
        counts: Dict[str, int] = {}
        ranked = sorted(candidates, key=lambda row: (-float(row.get("score") or 0), row["candidate_key"]))
        for item in ranked:
            if item.get("status") != "selected":
                continue
            theme = str(item.get("primary_theme_key") or "")
            if len(allowed) >= min(policy.max_total_candidates, policy.max_observation_candidates) or counts.get(theme, 0) >= policy.max_candidates_per_theme:
                continue
            if theme not in counts and len(counts) >= policy.max_themes:
                continue
            allowed.add(item["candidate_key"])
            counts[theme] = counts.get(theme, 0) + 1
        selected, observing, rejected = [], [], []
        for item in ranked:
            row = dict(item)
            row["score_components"] = dict(item.get("score_components") or {})
            comparison = dict(row["score_components"].get("strategy_comparison") or {})
            if comparison:
                if comparison.get("legacy_selected") and row["candidate_key"] not in legacy_allowed:
                    comparison["legacy_reasons"] = list(comparison.get("legacy_reasons") or []) + ["legacy_candidate_cap"]
                comparison["legacy_selected"] = row["candidate_key"] in legacy_allowed
                comparison["current_selected"] = row["candidate_key"] in allowed
                row["score_components"]["strategy_comparison"] = comparison
            if row["candidate_key"] in allowed:
                row["rank"] = len(selected) + 1
                selected.append(row)
            elif row.get("status") in {"selected", "observing"}:
                if row.get("status") == "selected":
                    row["rejection_reasons"] = list(row.get("rejection_reasons") or []) + ["analysis_candidate_budget"]
                row["status"] = "observing"
                observing.append(row)
            else:
                rejected.append(row)
        observing = observing[:max(0, policy.max_observation_candidates - len(selected))]
        for index, row in enumerate(observing, start=len(selected) + 1):
            row["rank"] = index
        # Retain rejected old-rule picks for the paired comparison.
        legacy_rejected = [row for row in rejected if row["candidate_key"] in legacy_allowed]
        other_rejected = [row for row in rejected if row["candidate_key"] not in legacy_allowed]
        return selected + observing + legacy_rejected + other_rejected[:policy.max_observation_candidates]

    def _candidate_presort_pool(self, candidates: Iterable[Dict[str, Any]], *, policy: OpportunityPolicy, regime: str) -> List[Dict[str, Any]]:
        # Already bounded by the explicit per-theme constituent sample.  Apply
        # all selection caps together so weaker first themes cannot evict later ones.
        return list(candidates)

    def _candidate_style_context(
        self,
        *,
        market: str,
        stock_code: str,
        base_style: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            from src.services.paper_trading_service import resolve_paper_trading_style_context

            return resolve_paper_trading_style_context(
                self.config,
                market=market,
                stock_code=stock_code,
                base_style_override=base_style or getattr(self.config, "market_opportunity_style", "") or None,
            )
        except Exception as exc:
            logger.debug("paper trading style context unavailable for opportunity candidate: %s", exc)
            return {
                "trading_style": "unavailable",
                "policy_version": "trading-style-unavailable",
                "source": "style_resolution_failed",
                "matched_override_key": None,
                "market": market,
                "stock_code": stock_code,
                "settings": {},
            }

    def _candidate_analysis_style(self, candidate: Any) -> Optional[str]:
        """Base style for the candidate's LLM analysis, frozen when it was screened.

        Follow-up/review candidates carry the origin selection under
        ``origin_components``. A missing or unresolved style falls back to the
        configured pool style; ``None`` keeps ``PAPER_TRADING_STYLE``.
        """

        components = self._json_dict(
            getattr(candidate, "score_components", None)
            or getattr(candidate, "score_components_json", None)
        )
        origin = components.get("origin_components")
        for value in (
            components.get("trading_style"),
            origin.get("trading_style") if isinstance(origin, Mapping) else None,
            getattr(self.config, "market_opportunity_style", ""),
        ):
            style = normalize_market_opportunity_style(str(value or ""))
            if style:
                return style
        return None

    @staticmethod
    def _candidate_style_score_payload(
        *,
        close: float,
        ma5: float,
        ma10: float,
        ma20: float,
        ma60: float,
        style_context: Optional[Mapping[str, Any]],
        moving_averages: Optional[Mapping[int, float]] = None,
    ) -> Dict[str, Any]:
        base_score = 20.0 if close >= ma5 >= ma10 >= ma20 else (12.0 if close >= ma20 else 4.0)
        context = style_context if isinstance(style_context, Mapping) else {}
        style = str(context.get("trading_style") or "position").strip().lower()
        settings = context.get("settings") if isinstance(context.get("settings"), Mapping) else {}
        core_windows = list(settings.get("core_ma_windows") or [])
        averages = {5: ma5, 10: ma10, 20: ma20, 60: ma60}
        averages.update(moving_averages or {})
        core = [averages.get(window, float("nan")) for window in core_windows]
        valid = bool(core) and all(math.isfinite(value) and value > 0 for value in [close, *core])
        medium_term_alignment = bool(valid and close >= core[-1] and core[-2 if len(core) > 1 else -1] >= core[-1])
        # Grade the configured horizon, not a fixed short-MA stack.  In
        # particular a trend-style MA250 must exist before scoring it.
        if not valid:
            trend_score = 0.0
        elif all(left >= right for left, right in zip([close, *core], core)):
            trend_score = 20.0
        elif medium_term_alignment:
            trend_score = 12.0
        else:
            trend_score = 4.0
        basis = "style_core_ma_" + style + "_" + "_".join(str(window) for window in core_windows)
        return {
            "trend_score": trend_score,
            "base_trend_score": base_score,
            "medium_term_alignment": medium_term_alignment,
            "style_trend_adjustment": trend_score - base_score,
            "style_trend_basis": basis,
        }

    @staticmethod
    def _last_bar_date(bars: pd.DataFrame) -> Optional[date]:
        if bars is None or bars.empty or "date" not in bars.columns:
            return None
        try:
            frame = bars.sort_values("date")
            return pd.to_datetime(frame.iloc[-1]["date"]).date()
        except Exception:
            return None

    def _build_jobs(
        self,
        *,
        run_id: int,
        candidates: List[Dict[str, Any]],
        models: Iterable[str],
        policy: OpportunityPolicy,
    ) -> List[Dict[str, Any]]:
        model_list = [str(model).strip() for model in models if str(model).strip()]
        if not model_list or policy.max_llm_calls_per_run <= 0:
            return []
        selected = [item for item in candidates if item.get("status") == "selected"]
        max_candidates = policy.max_llm_calls_per_run // max(1, len(model_list))
        jobs: List[Dict[str, Any]] = []
        for candidate in selected[:max_candidates]:
            for model in model_list:
                jobs.append(
                    {
                        "candidate_key": candidate["candidate_key"],
                        "job_key": self._stable_hash({
                            "run_id": run_id,
                            "candidate_key": candidate["candidate_key"],
                            "model": model,
                            "policy": OPPORTUNITY_POLICY_VERSION,
                        }),
                        "model": model,
                        "status": "pending",
                        "evidence": {"source": "market_opportunity", "mode": policy.mode},
                    }
                )
        return jobs

    @staticmethod
    def _research_budget_limit(policy: OpportunityPolicy) -> int:
        # 实验研究每日共享预算与既有单次运行上限取更严格者；0 停止全部研究调用。
        return max(0, min(int(policy.research_daily_llm_budget), int(policy.max_llm_calls_per_run)))

    def _research_budget_state(
        self,
        *,
        policy: OpportunityPolicy,
        market: str,
        session_date: date,
        models: Iterable[str],
        selected_candidate_count: int,
    ) -> Dict[str, Any]:
        """Report how many research candidates the shared session budget admits.

        Reserved review slots are already counted as used, so next-session
        reviews keep priority over new candidates.  The ledger remains the hard
        cap at call time; this is only the admission estimate for new jobs.
        """

        model_count = len([str(model).strip() for model in models if str(model).strip()])
        limit = self._research_budget_limit(policy)
        usage = self.repo.research_budget_usage(market=market, session_date=session_date, limit=limit)
        remaining = int(usage.get("remaining") or 0)
        block_reason = None
        if selected_candidate_count > 0:
            if model_count <= 0:
                block_reason = "configured_models_missing"
            elif limit <= 0:
                block_reason = "research_budget_zero"
            elif limit < model_count:
                block_reason = "research_budget_insufficient_for_configured_models"
            elif remaining < model_count:
                block_reason = "research_budget_exhausted"
        return {
            **usage,
            "session_date": session_date.isoformat(),
            "execution_intent": REPORT_EXECUTION_INTENT_RESEARCH_ONLY,
            "model_count": model_count,
            "max_candidates": remaining // model_count if block_reason is None and model_count > 0 else 0,
            "block_reason": block_reason,
        }

    @staticmethod
    def _risk_off_research_shortlist(
        candidates: List[Dict[str, Any]],
        *,
        policy: OpportunityPolicy,
    ) -> List[Dict[str, Any]]:
        """红灯日收紧研究入选：只保留分数达标的前 N 只 selected，其余降为 observing。

        研究预算账本仍是调用时的硬上限；这里只减少红灯日送 LLM 的候选。
        """

        max_count = max(0, int(policy.risk_off_max_candidates))
        min_score = float(policy.risk_off_min_score)
        ranked_selected = sorted(
            (row for row in candidates if row.get("status") == "selected"),
            key=lambda row: (-float(row.get("score") or 0), str(row.get("candidate_key") or "")),
        )
        eligible = [row for row in ranked_selected if float(row.get("score") or 0) >= min_score]
        kept_keys = {row["candidate_key"] for row in eligible[:max_count]}
        result: List[Dict[str, Any]] = []
        for item in candidates:
            row = item
            if item.get("status") == "selected" and item["candidate_key"] not in kept_keys:
                row = dict(item)
                row["status"] = "observing"
                row["rejection_reasons"] = list(item.get("rejection_reasons") or []) + ["risk_off_research_cap"]
            result.append(row)
        return result

    def _build_research_jobs(
        self,
        *,
        run_id: int,
        candidates: List[Dict[str, Any]],
        models: Iterable[str],
        max_candidates: int,
    ) -> List[Dict[str, Any]]:
        """Build initial research-only jobs; the job key prefix is the trusted marker."""

        model_list = [str(model).strip() for model in models if str(model).strip()]
        if not model_list or max_candidates <= 0:
            return []
        selected = [item for item in candidates if item.get("status") == "selected"]
        jobs: List[Dict[str, Any]] = []
        for candidate in selected[:max_candidates]:
            for model in model_list:
                identity = self._stable_hash({
                    "run_id": run_id,
                    "candidate_key": candidate["candidate_key"],
                    "model": model,
                    "kind": "initial",
                    "policy": OPPORTUNITY_POLICY_VERSION,
                })
                jobs.append(
                    {
                        "candidate_key": candidate["candidate_key"],
                        "job_key": f"{MARKET_OPPORTUNITY_RESEARCH_JOB_KEY_PREFIX}initial:{identity}",
                        "model": model,
                        "status": "pending",
                        "evidence": {
                            "source": RESEARCH_JOB_SOURCE,
                            "execution_intent": REPORT_EXECUTION_INTENT_RESEARCH_ONLY,
                            "research_kind": "initial",
                        },
                    }
                )
        return jobs

    @staticmethod
    def _research_job_kind(job_key: Any) -> str:
        review_prefix = f"{MARKET_OPPORTUNITY_RESEARCH_JOB_KEY_PREFIX}review:"
        return "review" if str(job_key or "").startswith(review_prefix) else "initial"

    @staticmethod
    def _research_outcome(job: Mapping[str, Any]) -> str:
        """Classify one research job; watch results are kept, never rewritten as buys."""

        status = str(job.get("status") or "")
        reason = str(job.get("failure_reason") or "")
        if status in {"pending", "claimed", "submitted"}:
            return "in_progress"
        if status == "completed":
            if reason == "submitted_job_session_expired_after_report":
                return "expired"
            evidence = job.get("evidence") if isinstance(job.get("evidence"), Mapping) else {}
            bundle = evidence.get("research_bundle") if isinstance(evidence.get("research_bundle"), Mapping) else {}
            outcome = bundle.get("outcome") if isinstance(bundle.get("outcome"), Mapping) else {}
            classification = str(outcome.get("classification") or "")
            return classification if classification in {"bullish", "non_bullish"} else "analysis_failed"
        return {
            "research_budget_exhausted": "budget_exhausted",
            "market_regime_unavailable": "data_unavailable",
            "submitted_job_session_expired": "expired",
        }.get(reason, "analysis_failed")

    @staticmethod
    def _job_reached_llm(job: Mapping[str, Any]) -> bool:
        """Whether a job's latest attempt actually submitted an analysis to the LLM.

        The run's research budget snapshot is taken before any job executes, so
        the page counts jobs instead. Same-day report reuse never calls a model;
        earlier attempts of a retried job are not observable here.
        """

        status = str(job.get("status") or "")
        if status == "submitted":
            return True
        if status == "completed":
            evidence = job.get("evidence") if isinstance(job.get("evidence"), Mapping) else {}
            return evidence.get("reused_same_day_report") is not True
        reason = str(job.get("failure_reason") or "")
        return (
            reason == "analysis_report_missing_after_sync_call"
            or reason.startswith("sync_analysis_exception")
            or reason.startswith("submitted_job_")
        )

    def _research_outcome_counts(self, run_ids: Iterable[Any]) -> Dict[str, int]:
        """Count research jobs of the given runs by explicit outcome."""

        counts = {key: 0 for key in RESEARCH_OUTCOMES}
        for run_id in dict.fromkeys(int(item) for item in run_ids if item is not None):
            detail = self.repo.get_run_detail(run_id) or {}
            for job in detail.get("analysis_jobs") or []:
                if is_market_opportunity_research_job_key(job.get("job_key")):
                    counts[self._research_outcome(job)] += 1
        return counts

    def _research_evidence_base(self, *, job: Any) -> Dict[str, Any]:
        """Rebuild research evidence from trusted job identity, keeping the review link."""

        base: Dict[str, Any] = {
            "source": RESEARCH_JOB_SOURCE,
            "execution_intent": REPORT_EXECUTION_INTENT_RESEARCH_ONLY,
            "research_kind": self._research_job_kind(getattr(job, "job_key", None)),
            "candidate_id": int(getattr(job, "candidate_id", 0) or 0),
        }
        existing = self._json_dict(getattr(job, "evidence_json", None))
        review_of = existing.get("research_review_of")
        if isinstance(review_of, Mapping):
            base["research_review_of"] = dict(review_of)
        return base

    def _extract_themes(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        containers = [
            payload,
            payload.get("market_theme_context") if isinstance(payload.get("market_theme_context"), dict) else {},
            payload.get("market_structure_context") if isinstance(payload.get("market_structure_context"), dict) else {},
        ]
        themes: List[Dict[str, Any]] = []
        for container in containers:
            self._append_ranked_theme_rows(
                themes,
                rows=self._nested_list(container, "sectors", "top"),
                theme_type="industry",
            )
            self._append_ranked_theme_rows(
                themes,
                rows=self._nested_list(container, "concepts", "top"),
                theme_type="concept",
            )
            for theme_type, key in (("industry", "leading_industries"), ("concept", "leading_concepts"), ("concept", "active_themes")):
                rows = container.get(key)
                if not isinstance(rows, list):
                    continue
                self._append_ranked_theme_rows(themes, rows=rows, theme_type=theme_type)
        return themes

    @staticmethod
    def _nested_list(payload: Dict[str, Any], *keys: str) -> List[Dict[str, Any]]:
        value: Any = payload
        for key in keys:
            if not isinstance(value, dict):
                return []
            value = value.get(key)
        return value if isinstance(value, list) else []

    def _append_ranked_theme_rows(
        self,
        themes: List[Dict[str, Any]],
        *,
        rows: List[Dict[str, Any]],
        theme_type: str,
    ) -> None:
        for idx, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or row.get("theme_name") or row.get("board_name") or "").strip()
            if not name:
                continue
            theme_key = f"{theme_type}:{name}"
            if any(item["theme_key"] == theme_key for item in themes):
                continue
            themes.append(
                {
                    "theme_key": theme_key,
                    "theme_type": theme_type,
                    "theme_name": name,
                    "theme_code": row.get("code") or row.get("theme_code"),
                    "theme_source": row.get("source") or row.get("theme_source"),
                    "theme_taxonomy": row.get("taxonomy") or row.get("theme_taxonomy"),
                    "rank": row.get("rank") or idx,
                    "change_pct": row.get("change_pct") or row.get("pct_chg") or row.get("涨跌幅"),
                    "persistence_days": row.get("persistence_days")
                    or row.get("active_days")
                    or row.get("days_active")
                    or row.get("top_rank_days"),
                    "persistence": row.get("persistence"),
                }
            )

    def _extract_market_payload(self, report: AnalysisHistory) -> Dict[str, Any]:
        for payload in (self._json_dict(report.context_snapshot), self._json_dict(report.raw_result)):
            found = self._find_market_payload(payload)
            if found:
                return found
        return {}

    def _find_market_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        for key in ("market_review_payload", "market_theme_context", "market_structure_context"):
            value = payload.get(key)
            if isinstance(value, dict):
                return value
        details = payload.get("details")
        if isinstance(details, dict):
            found = self._find_market_payload(details)
            if found:
                return found
        context = payload.get("context_snapshot")
        if isinstance(context, dict):
            found = self._find_market_payload(context)
            if found:
                return found
        return {}

    def _persist_blocked_run(
        self,
        *,
        policy: OpportunityPolicy,
        market: str,
        report: AnalysisHistory,
        trade_date: date,
        reason: str,
        market_regime: Optional[DailyMarketRegime] = None,
    ) -> Dict[str, Any]:
        run = self.repo.create_or_get_run(
            {
                "run_key": self._run_key(report_id=int(report.id), policy=policy),
                "market": market,
                "trade_date": trade_date,
                "source_report_id": int(report.id),
                "market_regime_id": getattr(market_regime, "id", None),
                "market_regime": getattr(market_regime, "regime", "unavailable"),
                "mode": policy.mode,
                "status": "blocked",
                "policy_version": OPPORTUNITY_POLICY_VERSION,
                "config_snapshot": self._config_snapshot(policy),
                "provenance": {"source": "market_review", "report_id": int(report.id), "blocked_reason": reason},
                "llm_budget": policy.max_llm_calls_per_run,
                "started_at": utc_naive_now(),
            }
        )
        self.repo.update_run(int(run.id), status="blocked", error_message=reason, completed_at=utc_naive_now())
        return {
            "status": "blocked",
            "run_id": int(run.id),
            "trade_date": trade_date.isoformat(),
            "reason": reason,
        }

    def _get_market_review(self, report_id: int) -> Optional[AnalysisHistory]:
        with self.db.session_scope() as session:
            row = session.get(AnalysisHistory, int(report_id))
            if row is None or row.report_type != "market_review":
                return None
            session.expunge(row)
            return row

    def _latest_market_review(self, *, market: str) -> Optional[AnalysisHistory]:
        with self.db.session_scope() as session:
            rows = (
                session.query(AnalysisHistory)
                .filter(AnalysisHistory.report_type == "market_review")
                .order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id))
                .limit(20)
                .all()
            )
            for row in rows:
                payload = self._json_dict(row.context_snapshot)
                region = str(payload.get("market_review_region") or "").lower()
                region_parts = {part.strip() for part in region.split(",") if part.strip()}
                if region in {"", market, "both"} or market in region_parts:
                    session.expunge(row)
                    return row
        return None

    def _latest_regime(self, *, market: str, trade_date: date) -> Optional[DailyMarketRegime]:
        with self.db.session_scope() as session:
            row = (
                session.query(DailyMarketRegime)
                .filter(DailyMarketRegime.region == market, DailyMarketRegime.session_date == trade_date)
                .order_by(desc(DailyMarketRegime.session_date), desc(DailyMarketRegime.created_at), desc(DailyMarketRegime.id))
                .first()
            )
            if row is not None:
                session.expunge(row)
            return row

    def _analysis_history_id_for_query(self, query_id: str) -> Optional[int]:
        with self.db.session_scope() as session:
            row = session.query(AnalysisHistory).filter_by(query_id=query_id).order_by(desc(AnalysisHistory.id)).first()
            return int(row.id) if row is not None else None

    def _same_day_stock_report_id(
        self,
        *,
        candidate: Any,
        model: str,
        trade_date: Optional[date],
        execution_intent: str = REPORT_EXECUTION_INTENT_PAPER_CANDIDATE,
        execution_scope: str = REPORT_EXECUTION_SCOPE_PRIMARY,
    ) -> Optional[int]:
        """Reuse only a same-day report whose frozen purpose matches the job."""

        if trade_date is None:
            return None
        day_start = datetime.combine(trade_date, datetime_time.min)
        day_end = datetime.combine(trade_date + timedelta(days=1), datetime_time.min)
        with self.db.session_scope() as session:
            rows = (
                session.query(AnalysisHistory)
                .filter(
                    AnalysisHistory.report_type != "market_review",
                    AnalysisHistory.model_used == model,
                    AnalysisHistory.created_at >= day_start,
                    AnalysisHistory.created_at < day_end,
                    report_execution_intent_clause(
                        AnalysisHistory.id,
                        execution_intent=execution_intent,
                        execution_scope=execution_scope,
                    ),
                )
                .order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id))
                .limit(50)
                .all()
            )
            for row in rows:
                if self._history_row_matches_candidate(row, candidate):
                    if self._same_day_report_is_reusable(
                        row,
                        candidate=candidate,
                        trade_date=trade_date,
                        # 研究与账户无关：同一股票、模型、会话只研究一次，首次研究与复核互相复用。
                        match_selection_evidence=execution_intent != REPORT_EXECUTION_INTENT_RESEARCH_ONLY,
                    ):
                        return int(row.id)
        return None

    def _same_day_report_is_reusable(
        self,
        row: AnalysisHistory,
        *,
        candidate: Any,
        trade_date: date,
        match_selection_evidence: bool = True,
    ) -> bool:
        context = self._json_dict(getattr(row, "context_snapshot", None))
        raw = self._json_dict(getattr(row, "raw_result", None))
        if raw.get("success") is False or context.get("success") is False:
            return False
        phase = self._extract_report_phase(context, raw)
        if phase != "postmarket":
            return False
        as_of = self._extract_report_as_of(context, raw)
        if as_of != trade_date:
            return False
        selection = context.get("selection_context")
        if not isinstance(selection, Mapping):
            enhanced = context.get("enhanced_context")
            selection = enhanced.get("selection_context") if isinstance(enhanced, Mapping) else None
        if not isinstance(selection, Mapping):
            return False
        if str(selection.get("source") or "") != "market_opportunity":
            return False
        if str(selection.get("candidate_key") or "") != str(getattr(candidate, "candidate_key", "") or ""):
            return False
        expected = build_selection_context(candidate=candidate, job=object())
        if match_selection_evidence and not expected.get("snapshot_hash"):
            return False
        # IDs can differ when the same frozen candidate is rediscovered.  The
        # actual input evidence and its contract version must still match;
        # legacy hashes alone did not cover quote/components/theme contents.
        evidence_keys = set(expected) - {"candidate_id", "run_id", "job_id", "model", "instruction"}
        if match_selection_evidence and any(selection.get(key) != expected[key] for key in evidence_keys):
            return False
        style_context = self._candidate_style_context(
            market="cn",
            stock_code=str(getattr(candidate, "symbol", None) or getattr(candidate, "stock_code", "") or ""),
            base_style=self._candidate_analysis_style(candidate),
        )
        report_style = self._extract_report_trading_style(context, raw)
        expected_style = str(style_context.get("trading_style") or "").strip().lower()
        return bool(report_style and expected_style and report_style == expected_style)

    @staticmethod
    def _extract_report_phase(context: Mapping[str, Any], raw: Mapping[str, Any]) -> Optional[str]:
        candidates = [
            context.get("analysis_phase"),
            raw.get("analysis_phase"),
            context.get("market_phase"),
            raw.get("market_phase"),
        ]
        for container in (
            context.get("market_phase_summary"),
            raw.get("market_phase_summary"),
            context.get("market_phase_context"),
            raw.get("market_phase_context"),
        ):
            if isinstance(container, Mapping):
                candidates.extend([
                    container.get("phase"),
                    container.get("analysis_intent"),
                ])
        for value in candidates:
            text = str(value or "").strip().lower()
            if text:
                return text
        return None

    def _completion_evidence(
        self,
        *,
        candidate: Any,
        job: Any,
        report_id: int,
        reconcile: Mapping[str, Any],
        base: Mapping[str, Any],
    ) -> Dict[str, Any]:
        result = dict(base)
        data_quality = self._json_dict(getattr(candidate, "data_quality_json", None))
        if data_quality.get("followup_reanalysis"):
            result["followup_of"] = {
                "origin_job_id": data_quality.get("origin_job_id"),
                "origin_candidate_id": data_quality.get("origin_candidate_id"),
            }
        else:
            candidate_date = self._parse_date(getattr(candidate, "as_of", None))
            if candidate_date is not None:
                obligation = build_followup_obligation(
                    reconciliation=reconcile,
                    candidate=candidate,
                    model=str(getattr(job, "model", "") or ""),
                    due_date=self._next_trading_day(
                        str(getattr(candidate, "market", "cn") or "cn"),
                        candidate_date,
                    ),
                    origin_job_id=int(getattr(job, "id", 0) or 0),
                    origin_report_id=int(report_id),
                )
                if obligation is not None:
                    result["followup"] = obligation
        return result

    def _extract_report_as_of(self, context: Mapping[str, Any], raw: Mapping[str, Any]) -> Optional[date]:
        candidates = [
            context.get("as_of"),
            context.get("trade_date"),
            raw.get("as_of"),
            raw.get("trade_date"),
        ]
        for container in (
            context.get("market_phase_summary"),
            raw.get("market_phase_summary"),
            context.get("market_phase_context"),
            raw.get("market_phase_context"),
        ):
            if isinstance(container, Mapping):
                candidates.extend([
                    container.get("effective_daily_bar_date"),
                    container.get("session_date"),
                ])
        quote = context.get("realtime_quote_raw")
        if isinstance(quote, Mapping):
            candidates.extend([quote.get("date"), quote.get("trade_date")])
        for value in candidates:
            parsed = self._parse_date(value)
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _extract_report_trading_style(context: Mapping[str, Any], raw: Mapping[str, Any]) -> Optional[str]:
        candidates = [
            context.get("paper_trading_style"),
            raw.get("paper_trading_style"),
            context.get("trading_style"),
            raw.get("trading_style"),
        ]
        for container in (
            context.get("paper_trading_style_context"),
            raw.get("paper_trading_style_context"),
            context.get("paper_trading_style_evidence"),
            raw.get("paper_trading_style_evidence"),
        ):
            if isinstance(container, Mapping):
                candidates.append(container.get("trading_style"))
                nested = container.get("style_context")
                if isinstance(nested, Mapping):
                    candidates.append(nested.get("trading_style"))
        enhanced = context.get("enhanced_context")
        if isinstance(enhanced, Mapping):
            nested = enhanced.get("paper_trading_style_context")
            if isinstance(nested, Mapping):
                candidates.append(nested.get("trading_style"))
        for value in candidates:
            text = str(value or "").strip().lower()
            if text:
                return text
        return None

    def _recover_submitted_jobs(
        self,
        *,
        run_id: Optional[int],
        limit: int,
        effective_session: Optional[date] = None,
    ) -> Dict[str, Any]:
        list_submitted = getattr(self.repo, "list_submitted_jobs", None)
        if not callable(list_submitted):
            return {"processed": 0, "failed": 0, "uncertain": 0}
        processed = 0
        failed = 0
        uncertain = 0
        run_ids = set()
        for job in list_submitted(run_id=run_id, limit=limit):
            run_ids.add(int(job.run_id))
            candidate = self.repo.get_candidate(int(job.candidate_id))
            candidate_session = self._parse_date(getattr(candidate, "as_of", None)) if candidate is not None else None
            query_id = str(getattr(job, "query_id", "") or "").strip()
            is_research = is_market_opportunity_research_job_key(getattr(job, "job_key", None))
            research_base = self._research_evidence_base(job=job) if is_research else {}
            if not query_id:
                failed += 1
                if is_research:
                    self._fail_research_job(
                        job=job,
                        query_id=None,
                        base=research_base,
                        reason="submitted_job_missing_query_id",
                        stage="submitted_recovery",
                    )
                    continue
                self.repo.complete_job(
                    int(job.id),
                    status="failed",
                    failure_reason="submitted_job_missing_query_id",
                    evidence={"source": "market_opportunity", "stage": "submitted_recovery"},
                )
                continue
            report_id = self._analysis_history_id_for_query(query_id)
            if effective_session is not None and candidate_session != effective_session:
                existing_evidence = self._json_dict(getattr(job, "evidence_json", None))
                followup_of = (
                    existing_evidence.get("followup_of")
                    if isinstance(existing_evidence.get("followup_of"), Mapping)
                    else None
                )
                expiry_evidence = {
                    **existing_evidence,
                    "submitted_expiry": {
                        "candidate_session": candidate_session.isoformat() if candidate_session else None,
                        "effective_session": effective_session.isoformat(),
                    },
                }
                if is_research:
                    if report_id:
                        if candidate is not None:
                            self._attach_opportunity_provenance(report_id=report_id, candidate=candidate, job=job)
                        self._complete_research_job(
                            job=job,
                            candidate=candidate,
                            query_id=query_id,
                            report_id=report_id,
                            base={
                                **research_base,
                                "submitted_expiry": expiry_evidence["submitted_expiry"],
                                "recovered_submitted": True,
                                "recovered_after_expiry": True,
                            },
                            expired_on=effective_session,
                            failure_reason="submitted_job_session_expired_after_report",
                        )
                        processed += 1
                        continue
                    failed += 1
                    self.repo.complete_job(
                        int(job.id),
                        status="failed",
                        query_id=query_id,
                        failure_reason="submitted_job_session_expired",
                        evidence={**research_base, "submitted_expiry": expiry_evidence["submitted_expiry"]},
                    )
                    if research_base.get("research_kind") == "review":
                        self._patch_research_review_origin(research_base, {
                            "status": "expired",
                            "closed_on": effective_session.isoformat(),
                            "result": "expired",
                            "reason": "submitted_job_session_expired",
                        })
                    continue
                if report_id:
                    if candidate is not None:
                        self._attach_opportunity_provenance(report_id=report_id, candidate=candidate, job=job)
                    self.repo.complete_job(
                        int(job.id),
                        status="completed",
                        query_id=query_id,
                        analysis_history_id=report_id,
                        failure_reason="submitted_job_session_expired_after_report",
                        evidence={
                            **expiry_evidence,
                            "source": "market_opportunity",
                            "recovered_submitted": True,
                            "recovered_after_expiry": True,
                            "paper_reconciliation_skipped": "expired_session",
                            "candidate_id": int(getattr(job, "candidate_id", 0) or 0),
                            "analysis_history_id": int(report_id),
                        },
                    )
                    processed += 1
                    continue
                failed += 1
                self.repo.complete_job(
                    int(job.id),
                    status="failed",
                    query_id=query_id,
                    failure_reason="submitted_job_session_expired",
                    evidence=expiry_evidence,
                )
                if isinstance(followup_of, Mapping) and followup_of.get("origin_job_id") is not None:
                    origin = self.repo.get_job(int(followup_of["origin_job_id"])) or {}
                    origin_evidence = origin.get("evidence") if isinstance(origin.get("evidence"), Mapping) else {}
                    obligation = origin_evidence.get("followup") if isinstance(origin_evidence.get("followup"), Mapping) else {}
                    self.repo.patch_job_evidence(
                        int(followup_of["origin_job_id"]),
                        {"followup": {
                            **dict(obligation),
                            "status": "expired",
                            "closed_on": effective_session.isoformat(),
                            "result": "frozen",
                            "reason": "submitted_job_session_expired",
                        }},
                    )
                continue
            if not report_id:
                submitted_at = getattr(job, "submitted_at", None) or getattr(job, "updated_at", None)
                # An unknown external call must never be made retryable: a
                # slow worker may still be generating the same report.  The
                # existing attempt lease bounds the observation wait; a late
                # return from that worker can still complete its original job.
                if isinstance(submitted_at, datetime) and utc_naive_now() - submitted_at > timedelta(seconds=900):
                    failed += 1
                    if is_research:
                        self._fail_research_job(
                            job=job,
                            query_id=query_id,
                            base={**research_base, "recovery_deadline_seconds": 900},
                            reason="submitted_job_recovery_deadline_expired",
                            stage="submitted_recovery",
                        )
                        continue
                    self.repo.complete_job(
                        int(job.id),
                        status="failed",
                        query_id=query_id,
                        failure_reason="submitted_job_recovery_deadline_expired",
                        evidence={
                            **self._json_dict(getattr(job, "evidence_json", None)),
                            "source": "market_opportunity",
                            "stage": "submitted_recovery",
                            "recovery_deadline_seconds": 900,
                        },
                    )
                else:
                    uncertain += 1
                continue
            if candidate is not None:
                self._attach_opportunity_provenance(report_id=report_id, candidate=candidate, job=job)
            if is_research:
                self._complete_research_job(
                    job=job,
                    candidate=candidate,
                    query_id=query_id,
                    report_id=report_id,
                    base={**research_base, "recovered_submitted": True},
                )
                processed += 1
                continue
            reconcile = self._reconcile_report_to_paper_plan(
                report_id,
                trigger_source="market_opportunity_submitted_recovery",
            )
            self._complete_job_with_artifacts(
                job_id=int(job.id),
                status="completed",
                query_id=query_id,
                analysis_history_id=report_id,
                reconcile=reconcile,
                evidence=(
                    self._completion_evidence(
                        candidate=candidate,
                        job=job,
                        report_id=report_id,
                        reconcile=reconcile,
                        base={
                            "source": "market_opportunity",
                            "recovered_submitted": True,
                            "candidate_id": int(getattr(job, "candidate_id", 0) or 0),
                        },
                    )
                    if candidate is not None
                    else {
                        "source": "market_opportunity",
                        "recovered_submitted": True,
                        "candidate_id": int(getattr(job, "candidate_id", 0) or 0),
                    }
                ),
            )
            processed += 1
        for recovered_run_id in run_ids:
            self._refresh_run_status(recovered_run_id)
        return {"processed": processed, "failed": failed, "uncertain": uncertain}

    @staticmethod
    def _history_row_matches_candidate(row: AnalysisHistory, candidate: Any) -> bool:
        candidate_code = normalize_stock_code(str(getattr(candidate, "stock_code", "") or ""))
        candidate_exchange = str(getattr(candidate, "exchange", "") or "").strip().lower()
        candidate_symbol = str(getattr(candidate, "symbol", "") or "").strip().upper()
        row_code = str(getattr(row, "code", "") or "").strip().upper()
        if not candidate_code or not candidate_exchange or not row_code:
            return False
        if candidate_symbol and row_code == candidate_symbol:
            return True
        row_exchange = ""
        if row_code.endswith(".SH"):
            row_exchange = "sh"
        elif row_code.endswith(".SZ"):
            row_exchange = "sz"
        elif row_code.startswith("SH") and len(row_code) >= 8:
            row_exchange = "sh"
        elif row_code.startswith("SZ") and len(row_code) >= 8:
            row_exchange = "sz"
        if not row_exchange:
            return False
        return row_exchange == candidate_exchange and normalize_stock_code(row_code) == candidate_code

    def _attach_opportunity_provenance(self, *, report_id: int, candidate: Any, job: Any) -> None:
        with self.db.session_scope() as session:
            report = session.get(AnalysisHistory, int(report_id))
            if report is None:
                return
            context = self._json_dict(report.context_snapshot)
            context["query_source"] = "market_opportunity"
            source = {
                "candidate_id": int(getattr(candidate, "id", 0) or 0),
                "run_id": int(getattr(candidate, "run_id", 0) or 0),
                "candidate_key": getattr(candidate, "candidate_key", None),
                "primary_theme_key": getattr(candidate, "primary_theme_key", None),
                "score": getattr(candidate, "score", None),
                "job_id": int(getattr(job, "id", 0) or 0),
                "job_key": getattr(job, "job_key", None),
                "model": getattr(job, "model", None),
                "policy_version": OPPORTUNITY_POLICY_VERSION,
            }
            selection_sources = list(context.get("selection_sources") or [])
            if not any(
                item.get("source") == "market_opportunity"
                and item.get("candidate_id") == source["candidate_id"]
                for item in selection_sources
                if isinstance(item, dict)
            ):
                selection_sources.append({"source": "market_opportunity", **source})
            context["selection_sources"] = selection_sources
            context["market_opportunity"] = source
            report.context_snapshot = json.dumps(context, ensure_ascii=False, sort_keys=True, default=str)

    def _reconcile_report_to_paper_plan(self, report_id: int, *, trigger_source: str) -> Dict[str, Any]:
        if not getattr(self.config, "paper_trading_enabled", False):
            return {"status": "skipped", "classification": "paper_trading_disabled"}
        try:
            service = self._build_paper_trading_service()
            result = service.reconcile_report_execution_plan(int(report_id), trigger_source=trigger_source)
            item = result.get("item") if isinstance(result, Mapping) else None
            plan_id = result.get("plan_id") if isinstance(result, Mapping) else None
            # A previously completed reconciliation returns only its persisted
            # ids.  Rehydrate the authoritative plan so recovery and API paths
            # expose the same executable/confirmation semantics as first-run.
            if not isinstance(item, Mapping) and plan_id is not None:
                plan = service.repo.get_plan(int(plan_id))
                if plan is not None:
                    item = service._serialize_plan(plan)
                    result["item"] = item
            if isinstance(item, Mapping):
                result.update(self._paper_plan_state(item))
            return result
        except Exception as exc:
            logger.warning("Market opportunity paper-plan reconciliation failed: %s", exc, exc_info=True)
            return {
                "status": "failed",
                "classification": "paper_plan_reconciliation_error",
                "error": f"{type(exc).__name__}:{str(exc)[:120]}",
            }

    @staticmethod
    def _paper_plan_state(item: Mapping[str, Any]) -> Dict[str, Any]:
        transition = item.get("strategy_thesis_transition")
        transition = dict(transition) if isinstance(transition, Mapping) else {}
        transition_evidence = transition.get("evidence")
        transition_evidence = (
            dict(transition_evidence)
            if isinstance(transition_evidence, Mapping)
            else {}
        )
        current = int(transition.get("confirmation_count") or 0)
        required = int(
            transition_evidence.get("effective_confirmation_required")
            or transition_evidence.get("entry_confirmation_required")
            or 0
        )
        executable = bool(item.get("executable"))
        reason = None
        if not executable:
            reason = transition.get("reason")
            if not reason:
                descriptions = item.get("descriptive_conditions")
                if isinstance(descriptions, list):
                    validator = next(
                        (
                            row for row in descriptions
                            if isinstance(row, Mapping) and row.get("source") == "plan_validator"
                        ),
                        None,
                    )
                    if isinstance(validator, Mapping):
                        reason = validator.get("text") or validator.get("reason")
            reason = reason or str(item.get("status") or "plan_not_executable")
        return {
            "executable": executable,
            "plan_status": item.get("status"),
            "confirmation_progress": {
                "current": current,
                "required": required,
                "completed": bool(required > 0 and current >= required),
                "reason": transition.get("reason"),
            },
            "no_plan_reason": reason,
        }

    def _complete_job_with_artifacts(
        self,
        *,
        job_id: int,
        status: str,
        query_id: str,
        analysis_history_id: int,
        reconcile: Dict[str, Any],
        evidence: Dict[str, Any],
    ) -> None:
        merged_evidence = {
            **dict(evidence),
            "paper_reconciliation": reconcile,
        }
        # complete_job persists report/signal/plan ids and evidence atomically.
        # A second link transaction could leave a half-written job on restart.
        self.repo.complete_job(
            int(job_id),
            status=status,
            query_id=query_id,
            analysis_history_id=int(analysis_history_id),
            evidence=merged_evidence,
        )
        followup_of = evidence.get("followup_of") if isinstance(evidence.get("followup_of"), Mapping) else None
        origin_job_id = followup_of.get("origin_job_id") if isinstance(followup_of, Mapping) else None
        if origin_job_id is not None:
            origin = self.repo.get_job(int(origin_job_id))
            origin_evidence = origin.get("evidence") if isinstance(origin, Mapping) else {}
            obligation = origin_evidence.get("followup") if isinstance(origin_evidence, Mapping) else {}
            self.repo.patch_job_evidence(
                int(origin_job_id),
                {"followup": {
                    **(dict(obligation) if isinstance(obligation, Mapping) else {}),
                    "status": "completed",
                    "completed_by_job_id": int(job_id),
                    "completed_report_id": int(analysis_history_id),
                    "result": (
                        "confirmed"
                        if reconcile.get("executable") is True
                        else "frozen"
                    ),
                    "reason": reconcile.get("no_plan_reason") or reconcile.get("classification"),
                }},
            )

    def _build_analysis_service(self) -> Any:
        if self.analysis_service_factory is not None:
            return self.analysis_service_factory()
        from src.services.analysis_service import AnalysisService

        return AnalysisService()

    def _build_paper_trading_service(self) -> Any:
        if self.paper_trading_service_factory is not None:
            return self.paper_trading_service_factory()
        from src.services.paper_trading_service import PaperTradingService

        return PaperTradingService.from_config(self.config)

    @staticmethod
    def _model_action(
        *,
        is_research: bool,
        evidence: Mapping[str, Any],
        plan_item: Optional[Mapping[str, Any]],
    ) -> Dict[str, Optional[str]]:
        """Expose the model's own advice; paper jobs without a persisted plan stay empty."""

        from src.schemas.decision_action import localize_action_label, normalize_decision_action

        action = None
        label = None
        if is_research:
            bundle = evidence.get("research_bundle") if isinstance(evidence.get("research_bundle"), Mapping) else {}
            outcome = bundle.get("outcome") if isinstance(bundle.get("outcome"), Mapping) else {}
            action = normalize_decision_action(outcome.get("action"))
            label = outcome.get("action_label")
        elif isinstance(plan_item, Mapping):
            transition = plan_item.get("strategy_thesis_transition")
            transition = transition if isinstance(transition, Mapping) else {}
            # 计划的 action 可能已被策略确认改写，优先取模型原始建议。
            action = (
                normalize_decision_action(plan_item.get("strategy_thesis_raw_action"))
                or normalize_decision_action(transition.get("raw_action"))
                or normalize_decision_action(plan_item.get("action"))
            )
        if action and not label:
            label = localize_action_label(action)
        return {"action": action, "action_label": label if action else None}

    @staticmethod
    def _model_states(configured_models: List[str], jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        by_model = {str(job.get("model") or ""): job for job in jobs}
        states: List[Dict[str, Any]] = []
        for model in configured_models:
            job = by_model.get(model)
            if not job:
                states.append({"model": model, "status": "missing", "no_plan_reason": "job_not_created"})
                continue
            evidence = job.get("evidence") if isinstance(job.get("evidence"), dict) else {}
            reconciliation = evidence.get("paper_reconciliation") if isinstance(evidence.get("paper_reconciliation"), dict) else {}
            plan_item = reconciliation.get("item") if isinstance(reconciliation.get("item"), Mapping) else None
            plan_state = (
                MarketOpportunityService._paper_plan_state(plan_item)
                if isinstance(plan_item, Mapping)
                else {
                    "executable": reconciliation.get("executable"),
                    "plan_status": (
                        reconciliation.get("plan_status")
                        or (
                            reconciliation.get("status")
                            if reconciliation.get("status") in {
                                "active", "non_executable", "superseded", "triggered", "closed"
                            }
                            else None
                        )
                    ),
                    "confirmation_progress": reconciliation.get("confirmation_progress"),
                    "no_plan_reason": reconciliation.get("no_plan_reason"),
                }
            )
            plan_id = job.get("paper_plan_id") or reconciliation.get("plan_id")
            executable = plan_state.get("executable")
            no_plan_reason = None
            if executable is not True:
                no_plan_reason = (
                    plan_state.get("no_plan_reason")
                    or reconciliation.get("classification")
                    or job.get("failure_reason")
                    or ("analysis_pending" if str(job.get("status") or "") in {"pending", "claimed", "submitted"} else None)
                )
            is_research = is_market_opportunity_research_job_key(job.get("job_key"))
            research = None
            if is_research:
                bundle = evidence.get("research_bundle") if isinstance(evidence.get("research_bundle"), dict) else {}
                research = {
                    "kind": MarketOpportunityService._research_job_kind(job.get("job_key")),
                    "outcome": MarketOpportunityService._research_outcome(job),
                    "bundle_id": bundle.get("bundle_id"),
                    "review": evidence.get("research_review") if isinstance(evidence.get("research_review"), dict) else None,
                    "review_of": (
                        evidence.get("research_review_of")
                        if isinstance(evidence.get("research_review_of"), dict)
                        else None
                    ),
                }
            states.append(
                {
                    "model": model,
                    "status": job.get("status") or "unknown",
                    "job_id": job.get("id"),
                    "task_id": job.get("task_id"),
                    "query_id": job.get("query_id"),
                    "analysis_history_id": job.get("analysis_history_id"),
                    "decision_signal_id": job.get("decision_signal_id") or reconciliation.get("signal_id"),
                    "paper_plan_id": plan_id,
                    "plan_executable": executable,
                    "plan_status": plan_state.get("plan_status"),
                    "confirmation_progress": plan_state.get("confirmation_progress"),
                    "followup": evidence.get("followup") if isinstance(evidence.get("followup"), dict) else None,
                    "execution_intent": (
                        REPORT_EXECUTION_INTENT_RESEARCH_ONLY
                        if is_research
                        else REPORT_EXECUTION_INTENT_PAPER_CANDIDATE
                    ),
                    "research": research,
                    **MarketOpportunityService._model_action(
                        is_research=is_research,
                        evidence=evidence,
                        plan_item=plan_item,
                    ),
                    "failure_reason": job.get("failure_reason"),
                    "no_plan_reason": no_plan_reason,
                    "updated_at": job.get("updated_at") or job.get("created_at"),
                }
            )
        return states

    @staticmethod
    def _candidate_no_plan_reason(configured_models: List[str], jobs: List[Dict[str, Any]]) -> Optional[str]:
        if not configured_models:
            return "configured_models_missing"
        if not jobs:
            return "analysis_jobs_missing"
        states = MarketOpportunityService._model_states(configured_models, jobs)
        if any(state.get("plan_executable") is True for state in states):
            return None
        reasons = [str(state.get("no_plan_reason") or state.get("failure_reason") or state.get("status") or "") for state in states]
        compact = [reason for reason in reasons if reason]
        return ",".join(sorted(set(compact))) if compact else "no_executable_plan"

    def _refresh_run_status(self, run_id: int) -> None:
        with self.db.session_scope() as session:
            run = session.get(MarketOpportunityRun, int(run_id))
            if run is None:
                return
            rows = (
                session.query(MarketOpportunityAnalysisJob.status)
                .filter(MarketOpportunityAnalysisJob.run_id == int(run_id))
                .all()
            )
            statuses = [str(row[0] or "") for row in rows]
            if not statuses:
                return
            if any(status in {"pending", "claimed", "failed_retryable", "submitted"} for status in statuses):
                run.status = "analyzing"
            elif any(status == "completed" for status in statuses):
                run.status = "completed"
                run.completed_at = utc_naive_now()
            else:
                run.status = "failed"
                run.completed_at = utc_naive_now()
            run.updated_at = utc_naive_now()

    @staticmethod
    def _report_trade_date(report: AnalysisHistory) -> date:
        return report.created_at.date() if report.created_at else date.today()

    def _payload_trade_date(self, payload: Dict[str, Any]) -> Optional[date]:
        for key in ("date", "trade_date", "session_date", "as_of"):
            value = payload.get(key) if isinstance(payload, dict) else None
            if not value:
                continue
            try:
                return pd.to_datetime(value).date()
            except Exception:
                continue
        return None

    @staticmethod
    def _parse_date(value: Any) -> Optional[date]:
        if isinstance(value, date):
            return value
        if not value:
            return None
        try:
            return pd.to_datetime(value).date()
        except Exception:
            return None

    def _postmarket_report_gate(
        self,
        *,
        report: AnalysisHistory,
        payload: Dict[str, Any],
        market: str,
        current_time: Optional[datetime],
    ) -> Dict[str, Any]:
        report_date = self._payload_trade_date(payload) or self._report_trade_date(report)
        try:
            from src.core.trading_calendar import get_effective_trading_date, get_market_now, is_market_open

            market_now = get_market_now(market, current_time)
            effective_date = get_effective_trading_date(market, current_time)
            if report_date != effective_date:
                return {
                    "ok": False,
                    "reason": "market_report_not_latest_effective_trading_date",
                    "report_date": report_date.isoformat(),
                    "effective_date": effective_date.isoformat(),
                }
            if not is_market_open(market, report_date, strict=True):
                return {"ok": False, "reason": "market_report_date_not_trading_day"}
            if market_now.date() == report_date and market_now.time().replace(tzinfo=None) < datetime_time(15, 0):
                return {"ok": False, "reason": "market_report_not_postmarket_complete"}
        except Exception as exc:
            logger.warning("Post-market opportunity gate calendar check failed: %s", exc, exc_info=True)
            return {"ok": False, "reason": "calendar_untrusted"}
        return {"ok": True, "report_date": report_date.isoformat()}

    @staticmethod
    def _config_snapshot(policy: OpportunityPolicy) -> Dict[str, Any]:
        return {
            "mode": policy.mode,
            "mode_source": policy.mode_source,
            "mode_version": policy.mode_version,
            "discovery_max_themes": policy.discovery_max_themes,
            "max_themes": policy.max_themes,
            "max_candidates_per_theme": policy.max_candidates_per_theme,
            "max_total_candidates": policy.max_total_candidates,
            "max_observation_candidates": policy.max_observation_candidates,
            "max_llm_calls_per_run": policy.max_llm_calls_per_run,
            "research_daily_llm_budget": policy.research_daily_llm_budget,
            "risk_off_max_candidates": policy.risk_off_max_candidates,
            "risk_off_min_score": policy.risk_off_min_score,
            "min_20d_avg_amount_cny": policy.min_20d_avg_amount_cny,
            "observe_gate_days": policy.observe_gate_days,
            "board_discovery_policy_version": BOARD_DISCOVERY_POLICY_VERSION,
            "theme_policy_version": THEME_POLICY_VERSION,
            "candidate_policy_version": CANDIDATE_POLICY_VERSION,
        }

    @staticmethod
    def _run_key(*, report_id: int, policy: OpportunityPolicy) -> str:
        return f"market-opportunity:{report_id}:{policy.mode}:{OPPORTUNITY_POLICY_VERSION}"

    @staticmethod
    def _json_dict(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        if not value:
            return {}
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _finite_float(value: Any) -> Optional[float]:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if math.isfinite(parsed) else None

    @staticmethod
    def _stable_hash(value: Any) -> str:
        return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()
