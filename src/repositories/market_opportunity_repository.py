# -*- coding: utf-8 -*-
"""Repository for durable market opportunity discovery ledgers."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import desc, or_, select, update
from sqlalchemy.exc import IntegrityError

from src.storage import (
    MARKET_OPPORTUNITY_FOLLOWUP_RUN_KEY_PREFIX,
    MARKET_OPPORTUNITY_RESEARCH_REVIEW_RUN_KEY_PREFIX,
    REPORT_EXECUTION_INTENT_PAPER_CANDIDATE,
    REPORT_EXECUTION_INTENT_RESEARCH_ONLY,
    AnalysisHistory,
    DatabaseManager,
    MarketOpportunityAnalysisJob,
    MarketOpportunityCandidate,
    MarketOpportunityResearchBudgetSlot,
    MarketOpportunityRun,
    MarketOpportunityTheme,
    is_market_opportunity_research_job_key,
    utc_naive_now,
)

# Follow-up and research-review runs copy one candidate without observing
# themes; they must not stand in for a discovery run's theme observation.
_NON_DISCOVERY_RUN_KEY_PREFIXES = (
    MARKET_OPPORTUNITY_FOLLOWUP_RUN_KEY_PREFIX,
    MARKET_OPPORTUNITY_RESEARCH_REVIEW_RUN_KEY_PREFIX,
)


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True, default=str)


def _json_loads(value: Any, default: Any) -> Any:
    if not value:
        return default
    try:
        parsed = json.loads(value)
        return parsed if parsed is not None else default
    except Exception:
        return default


def _coerce_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        return None


class MarketOpportunityRepository:
    """Persistence facade for opportunity runs, themes, candidates and jobs."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        self.db = db_manager or DatabaseManager.get_instance()

    def create_or_get_run(self, payload: Dict[str, Any]) -> MarketOpportunityRun:
        with self.db.session_scope() as session:
            existing = session.query(MarketOpportunityRun).filter_by(run_key=payload["run_key"]).one_or_none()
            if existing is not None:
                return self._detach(session, existing)
            run = MarketOpportunityRun(
                run_key=payload["run_key"],
                market=payload.get("market", "cn"),
                trade_date=payload["trade_date"],
                source_report_id=payload["source_report_id"],
                market_regime_id=payload.get("market_regime_id"),
                market_regime=payload.get("market_regime", "unavailable"),
                mode=payload.get("mode", "off"),
                status=payload.get("status", "pending"),
                policy_version=payload.get("policy_version", "opportunity-policy-v1"),
                config_snapshot_json=_json_dumps(payload.get("config_snapshot", {})),
                provenance_json=_json_dumps(payload.get("provenance", {})),
                llm_budget=int(payload.get("llm_budget", 0) or 0),
                started_at=payload.get("started_at"),
            )
            session.add(run)
            try:
                session.flush()
            except IntegrityError:
                session.rollback()
                with self.db.session_scope() as retry_session:
                    existing = retry_session.query(MarketOpportunityRun).filter_by(run_key=payload["run_key"]).one()
                    return self._detach(retry_session, existing)
            return self._detach(session, run)

    def update_run(self, run_id: int, **fields: Any) -> None:
        json_fields = {"config_snapshot", "provenance"}
        mapped = {}
        for key, value in fields.items():
            if key in json_fields:
                mapped[f"{key}_json"] = _json_dumps(value)
            else:
                mapped[key] = value
        mapped["updated_at"] = utc_naive_now()
        with self.db.session_scope() as session:
            session.query(MarketOpportunityRun).filter_by(id=run_id).update(mapped)

    def replace_run_children(
        self,
        *,
        run_id: int,
        themes: Iterable[Dict[str, Any]],
        candidates: Iterable[Dict[str, Any]],
        jobs: Iterable[Dict[str, Any]],
    ) -> None:
        with self.db.session_scope() as session:
            protected = (
                session.query(MarketOpportunityAnalysisJob.id)
                .filter(
                    MarketOpportunityAnalysisJob.run_id == int(run_id),
                    MarketOpportunityAnalysisJob.status.in_(["claimed", "submitted", "completed"]),
                )
                .first()
            )
            if protected is not None:
                raise RuntimeError("market_opportunity_run_has_nonreplaceable_jobs")
            session.query(MarketOpportunityAnalysisJob).filter_by(run_id=run_id).delete()
            session.query(MarketOpportunityCandidate).filter_by(run_id=run_id).delete()
            session.query(MarketOpportunityTheme).filter_by(run_id=run_id).delete()

            theme_id_by_key: Dict[str, int] = {}
            for payload in themes:
                theme = MarketOpportunityTheme(
                    run_id=run_id,
                    theme_key=payload["theme_key"],
                    theme_type=payload["theme_type"],
                    theme_name=payload["theme_name"],
                    theme_code=payload.get("theme_code"),
                    rank=payload.get("rank"),
                    change_pct=payload.get("change_pct"),
                    status=payload.get("status", "rejected"),
                    score=payload.get("score"),
                    score_components_json=_json_dumps(payload.get("score_components", {})),
                    rejection_reasons_json=_json_dumps(payload.get("rejection_reasons", [])),
                    data_quality_json=_json_dumps(payload.get("data_quality", {})),
                    constituent_snapshot_json=_json_dumps(payload.get("constituent_snapshot", {})),
                    constituent_count=int(payload.get("constituent_count", 0) or 0),
                    eligible_constituent_count=int(payload.get("eligible_constituent_count", 0) or 0),
                )
                session.add(theme)
                session.flush()
                theme_id_by_key[theme.theme_key] = int(theme.id)

            candidate_id_by_key: Dict[str, int] = {}
            for payload in candidates:
                candidate = MarketOpportunityCandidate(
                    run_id=run_id,
                    theme_id=theme_id_by_key.get(payload.get("primary_theme_key")),
                    candidate_key=payload["candidate_key"],
                    market=payload.get("market", "cn"),
                    exchange=payload["exchange"],
                    stock_code=payload["stock_code"],
                    symbol=payload["symbol"],
                    stock_name=payload.get("stock_name"),
                    primary_theme_key=payload.get("primary_theme_key"),
                    related_themes_json=_json_dumps(payload.get("related_themes", [])),
                    status=payload.get("status", "rejected"),
                    rank=payload.get("rank"),
                    score=payload.get("score"),
                    score_components_json=_json_dumps(payload.get("score_components", {})),
                    rejection_reasons_json=_json_dumps(payload.get("rejection_reasons", [])),
                    data_quality_json=_json_dumps(payload.get("data_quality", {})),
                    quote_snapshot_json=_json_dumps(payload.get("quote_snapshot", {})),
                    as_of=payload["as_of"],
                    valid_until=payload.get("valid_until"),
                    snapshot_hash=payload.get("snapshot_hash"),
                )
                session.add(candidate)
                session.flush()
                candidate_id_by_key[candidate.candidate_key] = int(candidate.id)

            for payload in jobs:
                candidate_id = candidate_id_by_key.get(payload["candidate_key"])
                if candidate_id is None:
                    continue
                session.add(
                    MarketOpportunityAnalysisJob(
                        job_key=payload["job_key"],
                        run_id=run_id,
                        candidate_id=candidate_id,
                        model=payload["model"],
                        status=payload.get("status", "pending"),
                        max_attempts=int(payload.get("max_attempts", 2) or 2),
                        evidence_json=_json_dumps(payload.get("evidence", {})),
                    )
                )

    def claim_pending_jobs(
        self,
        *,
        owner: str,
        limit: int,
        run_id: Optional[int] = None,
        lease_seconds: int = 900,
        effective_session: Optional[date] = None,
    ) -> List[MarketOpportunityAnalysisJob]:
        now = utc_naive_now()
        session_date = _coerce_date(effective_session) or now.date()
        lease_until = now + timedelta(seconds=max(60, int(lease_seconds)))
        with self.db.session_scope() as session:
            eligible_candidate_ids = (
                select(MarketOpportunityCandidate.id)
                .join(MarketOpportunityRun, MarketOpportunityRun.id == MarketOpportunityCandidate.run_id)
                .where(
                    MarketOpportunityCandidate.status == "selected",
                    MarketOpportunityCandidate.as_of == session_date,
                    MarketOpportunityCandidate.valid_until.is_not(None),
                    MarketOpportunityCandidate.valid_until >= session_date,
                    MarketOpportunityRun.trade_date == session_date,
                    MarketOpportunityRun.status.in_(["ready", "analyzing", "completed"]),
                )
            )
            query = (
                session.query(MarketOpportunityAnalysisJob)
                .with_entities(MarketOpportunityAnalysisJob.id)
                .filter(
                    MarketOpportunityAnalysisJob.status.in_(["pending", "failed_retryable", "claimed"]),
                    MarketOpportunityAnalysisJob.candidate_id.in_(eligible_candidate_ids),
                    or_(
                        MarketOpportunityAnalysisJob.next_retry_at.is_(None),
                        MarketOpportunityAnalysisJob.next_retry_at <= now,
                    ),
                    or_(
                        MarketOpportunityAnalysisJob.lease_expires_at.is_(None),
                        MarketOpportunityAnalysisJob.lease_expires_at <= now,
                    ),
                    MarketOpportunityAnalysisJob.attempt_count < MarketOpportunityAnalysisJob.max_attempts,
                )
            )
            if run_id is not None:
                query = query.filter(MarketOpportunityAnalysisJob.run_id == int(run_id))
            candidate_ids = [
                int(row[0])
                for row in (
                    query
                    .order_by(MarketOpportunityAnalysisJob.created_at.asc())
                    .limit(max(1, int(limit)))
                    .all()
                )
            ]
            claimed: List[MarketOpportunityAnalysisJob] = []
            for job_id in candidate_ids:
                result = session.execute(
                    update(MarketOpportunityAnalysisJob)
                    .where(
                        MarketOpportunityAnalysisJob.id == job_id,
                        MarketOpportunityAnalysisJob.status.in_(["pending", "failed_retryable", "claimed"]),
                        MarketOpportunityAnalysisJob.candidate_id.in_(eligible_candidate_ids),
                        or_(
                            MarketOpportunityAnalysisJob.next_retry_at.is_(None),
                            MarketOpportunityAnalysisJob.next_retry_at <= now,
                        ),
                        or_(
                            MarketOpportunityAnalysisJob.lease_expires_at.is_(None),
                            MarketOpportunityAnalysisJob.lease_expires_at <= now,
                        ),
                        MarketOpportunityAnalysisJob.attempt_count < MarketOpportunityAnalysisJob.max_attempts,
                    )
                    .values(
                        status="claimed",
                        lease_owner=owner,
                        lease_expires_at=lease_until,
                        attempt_count=MarketOpportunityAnalysisJob.attempt_count + 1,
                        updated_at=now,
                    )
                )
                if int(result.rowcount or 0) != 1:
                    continue
                job = session.query(MarketOpportunityAnalysisJob).filter_by(id=job_id).one()
                claimed.append(self._detach(session, job))
            return claimed

    def complete_job(
        self,
        job_id: int,
        *,
        status: str,
        query_id: Optional[str] = None,
        task_id: Optional[str] = None,
        analysis_history_id: Optional[int] = None,
        failure_reason: Optional[str] = None,
        evidence: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self.db.session_scope() as session:
            job = session.query(MarketOpportunityAnalysisJob).filter_by(id=job_id).one_or_none()
            if job is None:
                return
            job.status = status
            job.query_id = query_id or job.query_id
            job.task_id = task_id or job.task_id
            job.analysis_history_id = analysis_history_id or job.analysis_history_id
            job.failure_reason = failure_reason
            safe_evidence = dict(evidence or {})
            reconciliation = safe_evidence.get("paper_reconciliation")
            if isinstance(reconciliation, dict):
                signal_id = reconciliation.get("signal_id")
                plan_id = reconciliation.get("plan_id")
                if signal_id is not None:
                    job.decision_signal_id = int(signal_id)
                if plan_id is not None:
                    job.paper_plan_id = int(plan_id)
            job.evidence_json = _json_dumps(safe_evidence)
            job.lease_owner = None
            job.lease_expires_at = None
            job.updated_at = utc_naive_now()

    def mark_job_submitted(self, job_id: int, *, owner: str, query_id: str) -> bool:
        """Persist the external-call identity before invoking an LLM.

        A submitted job is deliberately excluded from lease reclaim.  A restart
        must reconcile it by ``query_id`` before any retry decision, avoiding an
        uncertain duplicate model charge.
        """
        now = utc_naive_now()
        with self.db.session_scope() as session:
            result = session.execute(
                update(MarketOpportunityAnalysisJob)
                .where(
                    MarketOpportunityAnalysisJob.id == int(job_id),
                    MarketOpportunityAnalysisJob.status == "claimed",
                    MarketOpportunityAnalysisJob.lease_owner == str(owner),
                )
                .values(
                    status="submitted",
                    query_id=str(query_id),
                    updated_at=now,
                )
            )
            return int(result.rowcount or 0) == 1

    def list_submitted_jobs(self, *, run_id: Optional[int] = None, limit: int = 200) -> List[MarketOpportunityAnalysisJob]:
        with self.db.session_scope() as session:
            query = session.query(MarketOpportunityAnalysisJob).filter(
                MarketOpportunityAnalysisJob.status == "submitted"
            )
            if run_id is not None:
                query = query.filter(MarketOpportunityAnalysisJob.run_id == int(run_id))
            rows = (
                query.order_by(MarketOpportunityAnalysisJob.updated_at.asc())
                .limit(max(1, min(int(limit or 200), 1000)))
                .all()
            )
            return [self._detach(session, row) for row in rows]

    def link_job_artifacts(
        self,
        job_id: int,
        *,
        analysis_history_id: Optional[int] = None,
        decision_signal_id: Optional[int] = None,
        paper_plan_id: Optional[int] = None,
        status: Optional[str] = None,
        failure_reason: Optional[str] = None,
        evidence: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Attach production report/signal/plan identities to an opportunity job."""
        values: Dict[str, Any] = {
            "updated_at": utc_naive_now(),
            "lease_owner": None,
            "lease_expires_at": None,
        }
        if analysis_history_id is not None:
            values["analysis_history_id"] = int(analysis_history_id)
        if decision_signal_id is not None:
            values["decision_signal_id"] = int(decision_signal_id)
        if paper_plan_id is not None:
            values["paper_plan_id"] = int(paper_plan_id)
        if status is not None:
            values["status"] = str(status)
        if failure_reason is not None:
            values["failure_reason"] = str(failure_reason)
        if evidence is not None:
            values["evidence_json"] = _json_dumps(evidence)
        with self.db.session_scope() as session:
            result = session.query(MarketOpportunityAnalysisJob).filter_by(id=int(job_id)).update(values)
            return int(result or 0) == 1

    def get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        with self.db.session_scope() as session:
            row = session.query(MarketOpportunityAnalysisJob).filter_by(id=int(job_id)).one_or_none()
            return self._job_to_dict(row) if row is not None else None

    def create_jobs_for_run(
        self,
        *,
        run_id: int,
        jobs: Iterable[Dict[str, Any]],
        effective_session: Optional[date] = None,
    ) -> int:
        """Create missing per-model jobs for an existing opportunity run."""
        created = 0
        session_date = _coerce_date(effective_session) or utc_naive_now().date()
        with self.db.session_scope() as session:
            candidates = {
                row.candidate_key: int(row.id)
                for row in session.query(MarketOpportunityCandidate)
                .join(MarketOpportunityRun, MarketOpportunityRun.id == MarketOpportunityCandidate.run_id)
                .filter(
                    MarketOpportunityCandidate.run_id == int(run_id),
                    MarketOpportunityCandidate.status == "selected",
                    MarketOpportunityCandidate.as_of == session_date,
                    MarketOpportunityCandidate.valid_until.is_not(None),
                    MarketOpportunityCandidate.valid_until >= session_date,
                    MarketOpportunityRun.trade_date == session_date,
                    MarketOpportunityRun.status.in_(["ready", "analyzing", "completed"]),
                )
                .all()
            }
            for payload in jobs:
                candidate_id = candidates.get(payload["candidate_key"])
                if candidate_id is None:
                    continue
                exists = (
                    session.query(MarketOpportunityAnalysisJob.id)
                    .filter_by(candidate_id=candidate_id, model=payload["model"])
                    .one_or_none()
                )
                if exists is not None:
                    continue
                session.add(
                    MarketOpportunityAnalysisJob(
                        job_key=payload["job_key"],
                        run_id=int(run_id),
                        candidate_id=candidate_id,
                        model=payload["model"],
                        status=payload.get("status", "pending"),
                        max_attempts=int(payload.get("max_attempts", 2) or 2),
                        evidence_json=_json_dumps(payload.get("evidence", {})),
                    )
                )
                created += 1
        return created

    def patch_job_evidence(self, job_id: int, patch: Dict[str, Any]) -> bool:
        """Merge top-level evidence fields without discarding audit history."""

        with self.db.session_scope() as session:
            job = (
                session.query(MarketOpportunityAnalysisJob)
                .filter_by(id=int(job_id))
                .with_for_update()
                .one_or_none()
            )
            if job is None:
                return False
            evidence = _json_loads(job.evidence_json, {})
            if not isinstance(evidence, dict):
                evidence = {}
            evidence.update(dict(patch))
            job.evidence_json = _json_dumps(evidence)
            job.updated_at = utc_naive_now()
            return True

    def acquire_research_budget_slot(
        self,
        *,
        market: str,
        session_date: date,
        limit: int,
        job_id: int,
        attempt_no: int,
        job_kind: str = "initial",
        query_id: Optional[str] = None,
        reservation_key: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Charge one research model attempt against the shared session budget.

        Charging is idempotent per ``(job_id, attempt_no)`` so a restart never
        pays twice for the same attempt, while every new attempt (a retry)
        occupies its own slot.  A matching reservation is consumed first.
        Only slots numbered ``<= limit`` count, so lowering the limit applies
        immediately and ``0`` stops all research calls.  Returns ``None`` when
        the budget is exhausted.
        """

        target_market = str(market or "cn").strip().lower()
        safe_limit = max(0, int(limit or 0))
        for _ in range(safe_limit + 2):
            try:
                with self.db.session_scope() as session:
                    existing = (
                        session.query(MarketOpportunityResearchBudgetSlot)
                        .filter_by(job_id=int(job_id), attempt_no=int(attempt_no))
                        .one_or_none()
                    )
                    if existing is not None:
                        return self._budget_slot_to_dict(existing)
                    now = utc_naive_now()
                    if reservation_key:
                        result = session.execute(
                            update(MarketOpportunityResearchBudgetSlot)
                            .where(
                                MarketOpportunityResearchBudgetSlot.reservation_key == str(reservation_key),
                                MarketOpportunityResearchBudgetSlot.market == target_market,
                                MarketOpportunityResearchBudgetSlot.session_date == session_date,
                                MarketOpportunityResearchBudgetSlot.state == "reserved",
                                MarketOpportunityResearchBudgetSlot.slot_no <= safe_limit,
                            )
                            .values(
                                state="charged",
                                job_id=int(job_id),
                                attempt_no=int(attempt_no),
                                job_kind=str(job_kind),
                                query_id=query_id,
                                updated_at=now,
                            )
                        )
                        if int(result.rowcount or 0) == 1:
                            row = (
                                session.query(MarketOpportunityResearchBudgetSlot)
                                .filter_by(reservation_key=str(reservation_key))
                                .one()
                            )
                            return self._budget_slot_to_dict(row)
                    slot_no = self._free_research_budget_slot(
                        session, market=target_market, session_date=session_date, limit=safe_limit
                    )
                    if slot_no is None:
                        return None
                    row = MarketOpportunityResearchBudgetSlot(
                        market=target_market,
                        session_date=session_date,
                        slot_no=slot_no,
                        job_id=int(job_id),
                        attempt_no=int(attempt_no),
                        job_kind=str(job_kind),
                        state="charged",
                        query_id=query_id,
                    )
                    session.add(row)
                    session.flush()
                    return self._budget_slot_to_dict(row)
            except IntegrityError:
                # A concurrent writer took the same slot or attempt; re-read.
                continue
        return None

    def reserve_research_budget_slot(
        self,
        *,
        market: str,
        session_date: date,
        limit: int,
        reservation_key: str,
        job_kind: str = "review",
    ) -> Optional[Dict[str, Any]]:
        """Hold one future-session slot for a research review, idempotently."""

        target_market = str(market or "cn").strip().lower()
        safe_limit = max(0, int(limit or 0))
        for _ in range(safe_limit + 2):
            try:
                with self.db.session_scope() as session:
                    existing = (
                        session.query(MarketOpportunityResearchBudgetSlot)
                        .filter_by(reservation_key=str(reservation_key))
                        .one_or_none()
                    )
                    if existing is not None:
                        return self._budget_slot_to_dict(existing)
                    slot_no = self._free_research_budget_slot(
                        session, market=target_market, session_date=session_date, limit=safe_limit
                    )
                    if slot_no is None:
                        return None
                    row = MarketOpportunityResearchBudgetSlot(
                        market=target_market,
                        session_date=session_date,
                        slot_no=slot_no,
                        job_kind=str(job_kind),
                        state="reserved",
                        reservation_key=str(reservation_key),
                    )
                    session.add(row)
                    session.flush()
                    return self._budget_slot_to_dict(row)
            except IntegrityError:
                continue
        return None

    def research_budget_usage(self, *, market: str, session_date: date, limit: int) -> Dict[str, int]:
        """Summarize occupied research slots within the current limit."""

        target_market = str(market or "cn").strip().lower()
        safe_limit = max(0, int(limit or 0))
        with self.db.session_scope() as session:
            states = [
                str(row[0])
                for row in session.query(MarketOpportunityResearchBudgetSlot.state)
                .filter(
                    MarketOpportunityResearchBudgetSlot.market == target_market,
                    MarketOpportunityResearchBudgetSlot.session_date == session_date,
                    MarketOpportunityResearchBudgetSlot.slot_no <= safe_limit,
                )
                .all()
            ]
        charged = sum(1 for state in states if state == "charged")
        reserved = sum(1 for state in states if state == "reserved")
        used = charged + reserved
        return {
            "limit": safe_limit,
            "used": used,
            "charged": charged,
            "reserved": reserved,
            "remaining": max(0, safe_limit - used),
        }

    @staticmethod
    def _free_research_budget_slot(session: Any, *, market: str, session_date: date, limit: int) -> Optional[int]:
        if limit <= 0:
            return None
        taken = {
            int(row[0])
            for row in session.query(MarketOpportunityResearchBudgetSlot.slot_no)
            .filter(
                MarketOpportunityResearchBudgetSlot.market == market,
                MarketOpportunityResearchBudgetSlot.session_date == session_date,
                MarketOpportunityResearchBudgetSlot.slot_no <= limit,
            )
            .all()
        }
        for slot_no in range(1, limit + 1):
            if slot_no not in taken:
                return slot_no
        return None

    def annotate_report_context(self, report_id: int, opportunity: Dict[str, Any]) -> None:
        """Attach opportunity provenance to a persisted analysis report snapshot."""
        with self.db.session_scope() as session:
            row = session.get(AnalysisHistory, int(report_id))
            if row is None:
                return
            snapshot = _json_loads(row.context_snapshot, {})
            if not isinstance(snapshot, dict):
                snapshot = {}
            snapshot["market_opportunity"] = dict(opportunity)
            row.context_snapshot = _json_dumps(snapshot)

    def list_runs(self, *, limit: int = 20) -> List[Dict[str, Any]]:
        with self.db.session_scope() as session:
            rows = (
                session.query(MarketOpportunityRun)
                .order_by(desc(MarketOpportunityRun.trade_date), desc(MarketOpportunityRun.created_at))
                .limit(max(1, min(int(limit or 20), 200)))
                .all()
            )
            return [self._run_to_dict(row) for row in rows]

    def list_runs_page(
        self,
        *,
        page: int = 1,
        limit: int = 20,
        market: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self.db.session_scope() as session:
            query = session.query(MarketOpportunityRun)
            if market:
                query = query.filter(MarketOpportunityRun.market == str(market).strip().lower())
            if status:
                query = query.filter(MarketOpportunityRun.status == str(status).strip().lower())
            return self._page(
                query,
                page=page,
                limit=limit,
                order_by=(desc(MarketOpportunityRun.trade_date), desc(MarketOpportunityRun.created_at)),
                serializer=self._run_to_dict,
            )

    def get_run_detail(self, run_id: int) -> Optional[Dict[str, Any]]:
        with self.db.session_scope() as session:
            run = session.query(MarketOpportunityRun).filter_by(id=run_id).one_or_none()
            if run is None:
                return None
            themes = session.query(MarketOpportunityTheme).filter_by(run_id=run_id).order_by(MarketOpportunityTheme.score.desc()).all()
            candidates = session.query(MarketOpportunityCandidate).filter_by(run_id=run_id).order_by(MarketOpportunityCandidate.status.desc(), MarketOpportunityCandidate.rank.asc()).all()
            jobs = session.query(MarketOpportunityAnalysisJob).filter_by(run_id=run_id).order_by(MarketOpportunityAnalysisJob.created_at.asc()).all()
            payload = self._run_to_dict(run)
            payload["themes"] = [self._theme_to_dict(item) for item in themes]
            payload["candidates"] = [self._candidate_to_dict(item) for item in candidates]
            payload["analysis_jobs"] = [self._job_to_dict(item) for item in jobs]
            return payload

    def list_candidates(self, *, run_id: Optional[int] = None, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        with self.db.session_scope() as session:
            query = session.query(MarketOpportunityCandidate)
            if run_id:
                query = query.filter_by(run_id=run_id)
            if status:
                query = query.filter_by(status=status)
            rows = query.order_by(desc(MarketOpportunityCandidate.created_at), MarketOpportunityCandidate.rank.asc()).limit(max(1, min(int(limit or 100), 500))).all()
            return [self._candidate_to_dict(row) for row in rows]

    def list_themes_page(
        self,
        *,
        run_id: Optional[int] = None,
        status: Optional[str] = None,
        page: int = 1,
        limit: int = 50,
    ) -> Dict[str, Any]:
        with self.db.session_scope() as session:
            query = session.query(MarketOpportunityTheme)
            if run_id is not None:
                query = query.filter(MarketOpportunityTheme.run_id == int(run_id))
            if status:
                query = query.filter(MarketOpportunityTheme.status == str(status).strip().lower())
            return self._page(
                query,
                page=page,
                limit=limit,
                order_by=(desc(MarketOpportunityTheme.created_at), MarketOpportunityTheme.rank.asc()),
                serializer=self._theme_to_dict,
            )

    def list_candidates_page(
        self,
        *,
        run_id: Optional[int] = None,
        market: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
        limit: int = 50,
    ) -> Dict[str, Any]:
        with self.db.session_scope() as session:
            query = session.query(MarketOpportunityCandidate)
            if run_id is not None:
                query = query.filter(MarketOpportunityCandidate.run_id == int(run_id))
            if market:
                query = query.filter(MarketOpportunityCandidate.market == market)
            if status:
                query = query.filter(MarketOpportunityCandidate.status == str(status).strip().lower())
            return self._page(
                query,
                page=page,
                limit=limit,
                order_by=(desc(MarketOpportunityCandidate.run_id), MarketOpportunityCandidate.rank.asc().nullslast(),
                          desc(MarketOpportunityCandidate.score), MarketOpportunityCandidate.id.asc()),
                serializer=self._candidate_to_dict,
            )

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
        with self.db.session_scope() as session:
            query = session.query(MarketOpportunityAnalysisJob)
            if run_id is not None:
                query = query.filter(MarketOpportunityAnalysisJob.run_id == int(run_id))
            if candidate_id is not None:
                query = query.filter(MarketOpportunityAnalysisJob.candidate_id == int(candidate_id))
            if model:
                query = query.filter(MarketOpportunityAnalysisJob.model == str(model))
            if status:
                query = query.filter(MarketOpportunityAnalysisJob.status == str(status).strip().lower())
            return self._page(
                query,
                page=page,
                limit=limit,
                order_by=(desc(MarketOpportunityAnalysisJob.created_at), MarketOpportunityAnalysisJob.id.asc()),
                serializer=self._job_to_dict,
            )

    def frozen_constituents_for_report(self, *, report_id: int, market: str, trade_date: date) -> List[Dict[str, Any]]:
        """Read original dated membership evidence for an explicit report replay."""
        with self.db.session_scope() as session:
            rows = session.execute(
                select(MarketOpportunityTheme, MarketOpportunityRun)
                .join(MarketOpportunityRun, MarketOpportunityRun.id == MarketOpportunityTheme.run_id)
                .where(MarketOpportunityRun.source_report_id == int(report_id),
                       MarketOpportunityRun.market == market,
                       MarketOpportunityRun.trade_date == trade_date)
                .order_by(desc(MarketOpportunityRun.id))
            ).all()
            seen = set()
            output = []
            for theme, run in rows:
                snapshot = _json_loads(theme.constituent_snapshot_json, {})
                key = theme.theme_key
                if key in seen or snapshot.get("status") not in {"ok", "partial"} or not snapshot.get("constituents"):
                    continue
                if snapshot.get("as_of") != trade_date.isoformat():
                    continue
                seen.add(key)
                output.append({"run_id": run.id, "theme_key": key, "snapshot": snapshot})
            return output

    def theme_persistence_window(
        self,
        *,
        market: str,
        as_of: date,
        theme_key: str,
        provider: str,
        window_size: int = 5,
    ) -> List[Dict[str, Any]]:
        """Return fixed prior opportunity-run dates with exact-theme observations.

        The returned rows intentionally include dates where the theme is absent;
        callers add the current run observation separately.  This prevents
        cherry-picking only the dates where a theme appeared.
        """

        safe_window = max(1, min(int(window_size or 5), 20))
        target_market = str(market or "cn").strip().lower()
        target_key = str(theme_key or "").strip()
        target_provider = str(provider or "").strip()
        if not target_market or not target_key or not target_provider:
            return []
        discovery_only = [
            ~MarketOpportunityRun.run_key.startswith(prefix)
            for prefix in _NON_DISCOVERY_RUN_KEY_PREFIXES
        ]
        with self.db.session_scope() as session:
            date_rows = (
                session.query(MarketOpportunityRun.trade_date)
                .filter(
                    MarketOpportunityRun.market == target_market,
                    MarketOpportunityRun.trade_date < as_of,
                    MarketOpportunityRun.status.in_(["ready", "completed", "analyzing"]),
                    *discovery_only,
                )
                .distinct()
                .order_by(desc(MarketOpportunityRun.trade_date))
                .limit(max(0, safe_window - 1))
                .all()
            )
            trade_dates = [row[0] for row in date_rows if row[0] is not None]
            output: List[Dict[str, Any]] = []
            for trade_date in sorted(trade_dates):
                run = (
                    session.query(MarketOpportunityRun)
                    .filter(
                        MarketOpportunityRun.market == target_market,
                        MarketOpportunityRun.trade_date == trade_date,
                        MarketOpportunityRun.status.in_(["ready", "completed", "analyzing"]),
                        *discovery_only,
                    )
                    .order_by(desc(MarketOpportunityRun.created_at), desc(MarketOpportunityRun.id))
                    .first()
                )
                if run is None:
                    continue
                theme = (
                    session.query(MarketOpportunityTheme)
                    .filter(
                        MarketOpportunityTheme.run_id == int(run.id),
                        MarketOpportunityTheme.theme_key == target_key,
                    )
                    .one_or_none()
                )
                # V3 observes more boards than the old report Top5.  Those
                # extra boards must not improve the frozen legacy baseline.
                if theme is not None:
                    components = _json_loads(theme.score_components_json, {})
                    if components.get("legacy_report_theme") is False:
                        theme = None
                provider_matches = False
                if theme is not None:
                    snapshot = _json_loads(theme.constituent_snapshot_json, {})
                    provider_matches = str(snapshot.get("provider") or "").strip() == target_provider
                output.append(
                    {
                        "trade_date": trade_date,
                        "present": bool(theme is not None),
                        "provider_matches": provider_matches,
                        "status": theme.status if theme is not None else "missing",
                        "change_pct": theme.change_pct if theme is not None and provider_matches else None,
                        "run_id": int(run.id),
                        "theme_id": int(theme.id) if theme is not None else None,
                    }
                )
            return output

    def get_candidate(self, candidate_id: int) -> Optional[MarketOpportunityCandidate]:
        with self.db.session_scope() as session:
            row = session.query(MarketOpportunityCandidate).filter_by(id=candidate_id).one_or_none()
            return self._detach(session, row) if row is not None else None

    @staticmethod
    def _page(
        query: Any,
        *,
        page: int,
        limit: int,
        order_by: Any,
        serializer: Any,
    ) -> Dict[str, Any]:
        safe_page = max(1, int(page or 1))
        safe_limit = max(1, min(int(limit or 50), 200))
        total = int(query.order_by(None).count())
        rows = (
            query.order_by(*order_by)
            .offset((safe_page - 1) * safe_limit)
            .limit(safe_limit)
            .all()
        )
        return {
            "items": [serializer(row) for row in rows],
            "total": total,
            "page": safe_page,
            "limit": safe_limit,
        }

    @staticmethod
    def _detach(session: Any, row: Any) -> Any:
        session.flush()
        session.expunge(row)
        return row

    @staticmethod
    def _run_to_dict(row: MarketOpportunityRun) -> Dict[str, Any]:
        return {
            "id": row.id,
            "run_key": row.run_key,
            "market": row.market,
            "trade_date": row.trade_date.isoformat() if row.trade_date else None,
            "source_report_id": row.source_report_id,
            "market_regime_id": row.market_regime_id,
            "market_regime": row.market_regime,
            "mode": row.mode,
            "status": row.status,
            "policy_version": row.policy_version,
            "config_snapshot": _json_loads(row.config_snapshot_json, {}),
            "provenance": _json_loads(row.provenance_json, {}),
            "theme_count": row.theme_count,
            "selected_theme_count": row.selected_theme_count,
            "candidate_count": row.candidate_count,
            "selected_candidate_count": row.selected_candidate_count,
            "llm_budget": row.llm_budget,
            "error_message": row.error_message,
            "started_at": row.started_at.isoformat() if row.started_at else None,
            "completed_at": row.completed_at.isoformat() if row.completed_at else None,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @staticmethod
    def _theme_to_dict(row: MarketOpportunityTheme) -> Dict[str, Any]:
        return {
            "id": row.id,
            "run_id": row.run_id,
            "theme_key": row.theme_key,
            "theme_type": row.theme_type,
            "theme_name": row.theme_name,
            "theme_code": row.theme_code,
            "rank": row.rank,
            "change_pct": row.change_pct,
            "status": row.status,
            "score": row.score,
            "score_components": _json_loads(row.score_components_json, {}),
            "rejection_reasons": _json_loads(row.rejection_reasons_json, []),
            "data_quality": _json_loads(row.data_quality_json, {}),
            "constituent_snapshot": _json_loads(row.constituent_snapshot_json, {}),
            "constituent_count": row.constituent_count,
            "eligible_constituent_count": row.eligible_constituent_count,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _candidate_to_dict(row: MarketOpportunityCandidate) -> Dict[str, Any]:
        return {
            "id": row.id,
            "run_id": row.run_id,
            "candidate_key": row.candidate_key,
            "market": row.market,
            "exchange": row.exchange,
            "stock_code": row.stock_code,
            "symbol": row.symbol,
            "stock_name": row.stock_name,
            "primary_theme_key": row.primary_theme_key,
            "related_themes": _json_loads(row.related_themes_json, []),
            "status": row.status,
            "rank": row.rank,
            "score": row.score,
            "score_components": _json_loads(row.score_components_json, {}),
            "rejection_reasons": _json_loads(row.rejection_reasons_json, []),
            "data_quality": _json_loads(row.data_quality_json, {}),
            "quote_snapshot": _json_loads(row.quote_snapshot_json, {}),
            "as_of": row.as_of.isoformat() if row.as_of else None,
            "valid_until": row.valid_until.isoformat() if row.valid_until else None,
            "snapshot_hash": row.snapshot_hash,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _job_to_dict(row: MarketOpportunityAnalysisJob) -> Dict[str, Any]:
        return {
            "id": row.id,
            "run_id": row.run_id,
            "candidate_id": row.candidate_id,
            "job_key": row.job_key,
            "execution_intent": (
                REPORT_EXECUTION_INTENT_RESEARCH_ONLY
                if is_market_opportunity_research_job_key(row.job_key)
                else REPORT_EXECUTION_INTENT_PAPER_CANDIDATE
            ),
            "model": row.model,
            "status": row.status,
            "attempt_count": row.attempt_count,
            "max_attempts": row.max_attempts,
            "task_id": row.task_id,
            "query_id": row.query_id,
            "analysis_history_id": row.analysis_history_id,
            "decision_signal_id": row.decision_signal_id,
            "paper_plan_id": row.paper_plan_id,
            "failure_reason": row.failure_reason,
            "evidence": _json_loads(row.evidence_json, {}),
            "lease_owner": row.lease_owner,
            "lease_expires_at": row.lease_expires_at.isoformat() if row.lease_expires_at else None,
            "next_retry_at": row.next_retry_at.isoformat() if row.next_retry_at else None,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    @staticmethod
    def _budget_slot_to_dict(row: MarketOpportunityResearchBudgetSlot) -> Dict[str, Any]:
        return {
            "id": row.id,
            "market": row.market,
            "session_date": row.session_date.isoformat() if row.session_date else None,
            "slot_no": row.slot_no,
            "job_id": row.job_id,
            "attempt_no": row.attempt_no,
            "job_kind": row.job_kind,
            "state": row.state,
            "reservation_key": row.reservation_key,
            "query_id": row.query_id,
        }
