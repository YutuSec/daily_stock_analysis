# -*- coding: utf-8 -*-
"""Persistence helpers for report-derived paper-trading plans and ledgers."""

from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional, Tuple

from sqlalchemy import Integer, and_, case, desc, exists, func, or_, select, update

from src.services.paper_symbol_identity import stock_daily_variants

from src.storage import (
    AnalysisHistory,
    AnalysisReportExecutionIntent,
    DailyMarketRegime,
    DatabaseManager,
    DecisionSignalRecord,
    PaperExecutionPlan,
    PaperCapitalAdjustment,
    PaperCapitalAccount,
    PaperNotificationDispatch,
    PaperPlanObservation,
    PaperPlanEvaluation,
    PaperPosition,
    PaperReanalysisRequest,
    PaperSyncLink,
    PaperTradeEvent,
    PortfolioPosition,
    REPORT_EXECUTION_SCOPE_PRIMARY,
    ReportPlanReconciliation,
    StockDaily,
    authorized_report_execution_clause,
    utc_naive_now,
)


class PaperTradingRepository:
    """Small DB access layer; business decisions remain in the service."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def get_report(self, report_id: int) -> Optional[AnalysisHistory]:
        with self.db.get_session() as session:
            return session.get(AnalysisHistory, int(report_id))

    def get_report_execution_intent(self, report_id: int) -> Optional[AnalysisReportExecutionIntent]:
        return self.db.get_report_execution_intent(int(report_id))

    def get_signal(self, signal_id: int) -> Optional[DecisionSignalRecord]:
        with self.db.get_session() as session:
            return session.get(DecisionSignalRecord, int(signal_id))

    def latest_report_signal(self, report_id: int) -> Optional[DecisionSignalRecord]:
        with self.db.get_session() as session:
            return session.execute(
                select(DecisionSignalRecord)
                .where(
                    DecisionSignalRecord.source_type == "analysis",
                    DecisionSignalRecord.source_report_id == int(report_id),
                )
                .order_by(desc(DecisionSignalRecord.created_at), desc(DecisionSignalRecord.id))
                .limit(1)
            ).scalar_one_or_none()

    def list_reports_missing_current_plans(
        self,
        *,
        plan_version: str,
        since: Optional[datetime] = None,
        limit: int = 200,
    ) -> List[AnalysisHistory]:
        """Return normal analysis reports without a current paper plan."""

        safe_limit = max(1, min(int(limit), 1000))
        filters = [
            AnalysisHistory.code != "MARKET",
            or_(
                AnalysisHistory.report_type.is_(None),
                AnalysisHistory.report_type != "market_review",
            ),
            ~exists().where(
                and_(
                    PaperExecutionPlan.source_report_id == AnalysisHistory.id,
                    PaperExecutionPlan.plan_version == plan_version,
                )
            ),
            ~exists().where(
                and_(
                    ReportPlanReconciliation.source_report_id == AnalysisHistory.id,
                    ReportPlanReconciliation.plan_version == plan_version,
                )
            ),
        ]
        if since is not None:
            filters.append(AnalysisHistory.created_at >= since)
        with self.db.get_session() as session:
            rows = session.execute(
                select(AnalysisHistory)
                .where(and_(*filters))
                .order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id))
                .limit(safe_limit)
            ).scalars().all()
            return list(rows)

    def get_report_plan_reconciliation(
        self,
        *,
        source_report_id: int,
        plan_version: str,
    ) -> Optional[ReportPlanReconciliation]:
        key = f"analysis:{int(source_report_id)}:{plan_version}"
        with self.db.get_session() as session:
            return session.execute(
                select(ReportPlanReconciliation)
                .where(ReportPlanReconciliation.reconciliation_key == key)
                .limit(1)
            ).scalar_one_or_none()

    def claim_report_plan_reconciliation(
        self,
        *,
        report: AnalysisHistory,
        plan_version: str,
        max_attempts: int = 3,
    ) -> Optional[ReportPlanReconciliation]:
        """Claim a report-plan reconciliation row for one bounded attempt."""

        key = f"analysis:{int(report.id)}:{plan_version}"
        now = utc_naive_now()

        def _snapshot(row: ReportPlanReconciliation) -> ReportPlanReconciliation:
            return ReportPlanReconciliation(
                id=row.id,
                reconciliation_key=row.reconciliation_key,
                source_report_id=row.source_report_id,
                plan_version=row.plan_version,
                source_model=row.source_model,
                decision_signal_id=row.decision_signal_id,
                paper_plan_id=row.paper_plan_id,
                status=row.status,
                classification=row.classification,
                attempt_count=row.attempt_count,
                max_attempts=row.max_attempts,
                next_retry_at=row.next_retry_at,
                last_attempt_at=row.last_attempt_at,
                last_error=row.last_error,
                details_json=row.details_json,
                created_at=row.created_at,
                updated_at=row.updated_at,
            )

        with self.db.session_scope() as session:
            row = session.execute(
                select(ReportPlanReconciliation)
                .where(ReportPlanReconciliation.reconciliation_key == key)
                .limit(1)
            ).scalar_one_or_none()
            if row is None:
                row = ReportPlanReconciliation(
                    reconciliation_key=key,
                    source_report_id=int(report.id),
                    plan_version=plan_version,
                    source_model=(str(getattr(report, "model_used", "") or "").strip() or None),
                    status="processing",
                    attempt_count=1,
                    max_attempts=max(1, int(max_attempts)),
                    last_attempt_at=now,
                    details_json="{}",
                )
                session.add(row)
                try:
                    session.flush()
                except Exception:
                    session.rollback()
                    return self.claim_report_plan_reconciliation(
                        report=report,
                        plan_version=plan_version,
                        max_attempts=max_attempts,
                    )
                return _snapshot(row)
            claim_result = session.execute(
                update(ReportPlanReconciliation)
                .where(
                    ReportPlanReconciliation.reconciliation_key == key,
                    ReportPlanReconciliation.status.notin_(["completed", "terminal", "processing"]),
                    ReportPlanReconciliation.attempt_count < ReportPlanReconciliation.max_attempts,
                    or_(
                        ReportPlanReconciliation.next_retry_at.is_(None),
                        ReportPlanReconciliation.next_retry_at <= now,
                    ),
                )
                .values(
                    status="processing",
                    attempt_count=ReportPlanReconciliation.attempt_count + 1,
                    last_attempt_at=now,
                    last_error=None,
                    updated_at=now,
                )
            )
            if int(getattr(claim_result, "rowcount", 0) or 0) != 1:
                return None
            claimed = session.execute(
                select(ReportPlanReconciliation)
                .where(ReportPlanReconciliation.reconciliation_key == key)
                .limit(1)
            ).scalar_one()
            return _snapshot(claimed)

    def finish_report_plan_reconciliation(
        self,
        *,
        reconciliation_id: int,
        status: str,
        classification: str,
        decision_signal_id: Optional[int] = None,
        paper_plan_id: Optional[int] = None,
        next_retry_at: Optional[datetime] = None,
        last_error: Optional[str] = None,
        details_json: str = "{}",
    ) -> None:
        now = utc_naive_now()
        with self.db.session_scope() as session:
            row = session.get(ReportPlanReconciliation, int(reconciliation_id))
            if row is None:
                return
            row.status = str(status or "failed")[:32]
            row.classification = str(classification or "unknown")[:64]
            row.decision_signal_id = decision_signal_id
            row.paper_plan_id = paper_plan_id
            row.next_retry_at = next_retry_at
            row.last_error = str(last_error)[:2000] if last_error else None
            row.details_json = details_json
            row.updated_at = now

    def list_due_report_plan_reconciliations(
        self,
        *,
        plan_version: str,
        since: Optional[datetime] = None,
        limit: int = 200,
    ) -> List[AnalysisHistory]:
        now = utc_naive_now()
        safe_limit = max(1, int(limit))
        with self.db.get_session() as session:
            rows = list(session.execute(
                select(AnalysisHistory)
                .join(
                    ReportPlanReconciliation,
                    ReportPlanReconciliation.source_report_id == AnalysisHistory.id,
                )
                .where(
                    ReportPlanReconciliation.plan_version == plan_version,
                    ReportPlanReconciliation.status.in_(["pending", "retry_wait", "failed"]),
                    ReportPlanReconciliation.attempt_count < ReportPlanReconciliation.max_attempts,
                    or_(
                        ReportPlanReconciliation.next_retry_at.is_(None),
                        ReportPlanReconciliation.next_retry_at <= now,
                    ),
                )
                .order_by(ReportPlanReconciliation.next_retry_at, AnalysisHistory.id)
                .limit(safe_limit)
            ).scalars().all())
        remaining = max(0, safe_limit - len(rows))
        missing = self.list_reports_missing_current_plans(
            plan_version=plan_version,
            since=since,
            limit=remaining,
        ) if remaining else []
        by_id = {int(row.id): row for row in rows}
        for row in missing:
            by_id.setdefault(int(row.id), row)
        return list(by_id.values())

    def list_report_plan_reconciliations(
        self,
        *,
        plan_version: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[List[ReportPlanReconciliation], int]:
        conditions = []
        if plan_version:
            conditions.append(ReportPlanReconciliation.plan_version == plan_version)
        if model:
            conditions.append(ReportPlanReconciliation.source_model == model)
        if status:
            conditions.append(ReportPlanReconciliation.status == status)
        clause = and_(*conditions) if conditions else True
        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(int(limit), 500))
        with self.db.get_session() as session:
            total = session.execute(select(func.count(ReportPlanReconciliation.id)).where(clause)).scalar() or 0
            rows = list(session.execute(
                select(ReportPlanReconciliation)
                .where(clause)
                .order_by(desc(ReportPlanReconciliation.updated_at), desc(ReportPlanReconciliation.id))
                .offset(safe_offset)
                .limit(safe_limit)
            ).scalars().all())
            return rows, int(total)

    def latest_market_regime(
        self,
        *,
        region: str,
        session_date: date,
    ) -> Optional[DailyMarketRegime]:
        with self.db.get_session() as session:
            return session.execute(
                select(DailyMarketRegime)
                .where(
                    DailyMarketRegime.region == region,
                    DailyMarketRegime.session_date <= session_date,
                )
                .order_by(desc(DailyMarketRegime.session_date), desc(DailyMarketRegime.id))
                .limit(1)
            ).scalar_one_or_none()

    def get_plan_by_key(self, plan_key: str) -> Optional[PaperExecutionPlan]:
        with self.db.get_session() as session:
            return session.execute(
                select(PaperExecutionPlan).where(PaperExecutionPlan.plan_key == plan_key).limit(1)
            ).scalar_one_or_none()

    def get_plan(self, plan_id: int) -> Optional[PaperExecutionPlan]:
        with self.db.get_session() as session:
            return session.get(PaperExecutionPlan, int(plan_id))

    def list_active_plans(self) -> List[PaperExecutionPlan]:
        with self.db.get_session() as session:
            return list(session.execute(
                select(PaperExecutionPlan)
                .where(PaperExecutionPlan.status == "active")
                .order_by(PaperExecutionPlan.id)
            ).scalars().all())

    def list_intraday_candidate_plans(self) -> List[PaperExecutionPlan]:
        """Return current-version plans whose entries may still need monitoring."""

        with self.db.get_session() as session:
            return list(session.execute(
                select(PaperExecutionPlan)
                .outerjoin(PaperPosition, PaperPosition.plan_id == PaperExecutionPlan.id)
                .where(
                    PaperExecutionPlan.plan_version.like("paper-hybrid%"),
                    or_(
                        PaperExecutionPlan.status == "active",
                        and_(
                            PaperExecutionPlan.status.in_(["triggered", "superseded", "closed"]),
                            PaperPosition.status == "open",
                        ),
                    ),
                )
                .distinct()
                .order_by(PaperExecutionPlan.id)
            ).scalars().all())

    def list_plans(
        self,
        *,
        model: Optional[str] = None,
        status: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[List[PaperExecutionPlan], int]:
        conditions = []
        if model:
            conditions.append(PaperExecutionPlan.source_model == model)
        if status:
            conditions.append(PaperExecutionPlan.status == status)
        clause = and_(*conditions) if conditions else True
        with self.db.get_session() as session:
            total = session.execute(
                select(func.count(PaperExecutionPlan.id)).where(clause)
            ).scalar() or 0
            rows = session.execute(
                select(PaperExecutionPlan)
                .where(clause)
                .order_by(desc(PaperExecutionPlan.created_at), desc(PaperExecutionPlan.id))
                .offset(max(0, offset))
                .limit(max(1, min(limit, 500)))
            ).scalars().all()
            return list(rows), int(total)

    def list_positions(
        self,
        *,
        model: Optional[str] = None,
        status: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[List[PaperPosition], int]:
        conditions = []
        if model:
            conditions.append(PaperPosition.source_model == model)
        if status:
            conditions.append(PaperPosition.status == status)
        clause = and_(*conditions) if conditions else True
        with self.db.get_session() as session:
            total = session.execute(select(func.count(PaperPosition.id)).where(clause)).scalar() or 0
            rows = session.execute(
                select(PaperPosition)
                .where(clause)
                .order_by(desc(PaperPosition.created_at), desc(PaperPosition.id))
                .offset(max(0, offset))
                .limit(max(1, min(limit, 500)))
            ).scalars().all()
            return list(rows), int(total)

    def get_position(self, position_id: int) -> Optional[PaperPosition]:
        with self.db.get_session() as session:
            return session.get(PaperPosition, int(position_id))

    def get_portfolio_position(self, position_id: int) -> Optional[PortfolioPosition]:
        with self.db.get_session() as session:
            return session.get(PortfolioPosition, int(position_id))

    def latest_report_for_identity(
        self,
        *,
        source_model: str,
        market: str,
        stock_code: str,
        execution_scope: str = REPORT_EXECUTION_SCOPE_PRIMARY,
    ) -> Optional[AnalysisHistory]:
        variants = stock_daily_variants(market, stock_code)
        candidates = [stock_code, *variants]
        with self.db.get_session() as session:
            return session.execute(
                select(AnalysisHistory)
                .where(
                    AnalysisHistory.model_used == source_model,
                    AnalysisHistory.code.in_(list(dict.fromkeys(candidates))),
                    or_(
                        AnalysisHistory.report_type.is_(None),
                        AnalysisHistory.report_type != "market_review",
                    ),
                    authorized_report_execution_clause(
                        AnalysisHistory.id,
                        execution_scope=execution_scope,
                    ),
                )
                .order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id))
                .limit(1)
            ).scalar_one_or_none()

    def get_open_position_by_identity(
        self,
        *,
        source_model: str,
        market: str,
        stock_code: str,
    ) -> Optional[PaperPosition]:
        with self.db.get_session() as session:
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

    def get_sync_link_by_key(self, idempotency_key: str) -> Optional[PaperSyncLink]:
        with self.db.get_session() as session:
            return session.execute(
                select(PaperSyncLink)
                .where(PaperSyncLink.idempotency_key == idempotency_key)
                .limit(1)
            ).scalar_one_or_none()

    def list_open_positions_all(self) -> List[PaperPosition]:
        """Return the complete active paper ledger for monitor processing."""

        with self.db.get_session() as session:
            return list(session.execute(
                select(PaperPosition)
                .where(PaperPosition.status == "open")
                .order_by(PaperPosition.id)
            ).scalars().all())

    def list_accounts(
        self,
        *,
        model: Optional[str] = None,
        market: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[List[PaperCapitalAccount], int]:
        conditions = []
        if model:
            conditions.append(PaperCapitalAccount.source_model == model)
        if market:
            conditions.append(PaperCapitalAccount.market == market)
        clause = and_(*conditions) if conditions else True
        with self.db.get_session() as session:
            total = session.execute(select(func.count(PaperCapitalAccount.id)).where(clause)).scalar() or 0
            rows = session.execute(
                select(PaperCapitalAccount)
                .where(clause)
                .order_by(PaperCapitalAccount.source_model, PaperCapitalAccount.market, PaperCapitalAccount.currency)
                .offset(max(0, offset))
                .limit(max(1, min(limit, 500)))
            ).scalars().all()
            return list(rows), int(total)

    def list_adjustments(
        self,
        *,
        model: Optional[str] = None,
        account_id: Optional[int] = None,
        adjustment_type: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[List[PaperCapitalAdjustment], int]:
        conditions = []
        if model:
            conditions.append(PaperCapitalAdjustment.source_model == model)
        if account_id is not None:
            conditions.append(PaperCapitalAdjustment.account_id == int(account_id))
        if adjustment_type:
            conditions.append(PaperCapitalAdjustment.adjustment_type == adjustment_type)
        clause = and_(*conditions) if conditions else True
        with self.db.get_session() as session:
            total = session.execute(select(func.count(PaperCapitalAdjustment.id)).where(clause)).scalar() or 0
            rows = session.execute(
                select(PaperCapitalAdjustment)
                .where(clause)
                .order_by(desc(PaperCapitalAdjustment.created_at), desc(PaperCapitalAdjustment.id))
                .offset(max(0, offset))
                .limit(max(1, min(limit, 500)))
            ).scalars().all()
            return list(rows), int(total)

    def list_dispatches(
        self,
        *,
        model: Optional[str] = None,
        status: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[List[PaperNotificationDispatch], int]:
        conditions = []
        if status:
            conditions.append(PaperNotificationDispatch.status == status)
        if model:
            conditions.append(PaperTradeEvent.source_model == model)
        condition = and_(*conditions) if conditions else True
        with self.db.get_session() as session:
            total_query = select(func.count(PaperNotificationDispatch.id))
            rows_query = select(PaperNotificationDispatch)
            if model:
                total_query = total_query.join(PaperTradeEvent, PaperTradeEvent.id == PaperNotificationDispatch.event_id)
                rows_query = rows_query.join(PaperTradeEvent, PaperTradeEvent.id == PaperNotificationDispatch.event_id)
            total = session.execute(total_query.where(condition)).scalar() or 0
            rows = session.execute(
                rows_query
                .where(condition)
                .order_by(desc(PaperNotificationDispatch.created_at), desc(PaperNotificationDispatch.id))
                .offset(max(0, offset))
                .limit(max(1, min(limit, 500)))
            ).scalars().all()
            return list(rows), int(total)

    def list_retryable_dispatches(self, *, limit: int = 500, max_attempts: int = 3) -> List[PaperNotificationDispatch]:
        safe_limit = max(1, min(limit, 5000))
        safe_attempts = max(1, int(max_attempts))
        with self.db.get_session() as session:
            pending = session.execute(
                select(PaperNotificationDispatch)
                .where(PaperNotificationDispatch.status == "pending")
                .order_by(PaperNotificationDispatch.created_at, PaperNotificationDispatch.id)
                .limit(safe_limit)
            ).scalars().all()
            if len(pending) >= safe_limit:
                return list(pending)
            failed = session.execute(
                select(PaperNotificationDispatch)
                .where(
                    and_(
                        PaperNotificationDispatch.status == "failed",
                        PaperNotificationDispatch.attempt_count < safe_attempts,
                    )
                )
                .order_by(PaperNotificationDispatch.created_at, PaperNotificationDispatch.id)
                .limit(safe_limit - len(pending))
            ).scalars().all()
            return list(pending) + list(failed)

    def claim_dispatch_for_send(self, dispatch_id: int, *, max_attempts: int = 3) -> Optional[PaperNotificationDispatch]:
        safe_attempts = max(1, int(max_attempts))
        with self.db.session_scope() as session:
            statement = (
                update(PaperNotificationDispatch)
                .where(
                    and_(
                        PaperNotificationDispatch.id == int(dispatch_id),
                        PaperNotificationDispatch.channel == "feishu",
                        or_(
                            PaperNotificationDispatch.status == "pending",
                            and_(
                                PaperNotificationDispatch.status == "failed",
                                PaperNotificationDispatch.attempt_count < safe_attempts,
                            ),
                        ),
                    )
                )
                .values(
                    status="sending",
                    attempt_count=PaperNotificationDispatch.attempt_count + 1,
                    updated_at=datetime.utcnow(),
                    last_error=None,
                )
            )
            result = session.execute(statement)
            if result.rowcount != 1:
                return None
            session.flush()
            row = session.get(PaperNotificationDispatch, int(dispatch_id))
            payload = PaperNotificationDispatch(
                id=row.id,
                dispatch_key=row.dispatch_key,
                event_id=row.event_id,
                channel=row.channel,
                status=row.status,
                attempt_count=row.attempt_count,
                last_error=row.last_error,
                payload_json=row.payload_json,
                sent_at=row.sent_at,
                created_at=row.created_at,
                updated_at=row.updated_at,
            )
            return payload

    def list_evaluations(
        self,
        *,
        model: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[List[Tuple[PaperPlanEvaluation, PaperExecutionPlan]], int]:
        condition = PaperExecutionPlan.source_model == model if model else True
        with self.db.get_session() as session:
            total = session.execute(
                select(func.count(PaperPlanEvaluation.id))
                .join(PaperExecutionPlan, PaperExecutionPlan.id == PaperPlanEvaluation.plan_id)
                .where(condition)
            ).scalar() or 0
            rows = session.execute(
                select(PaperPlanEvaluation, PaperExecutionPlan)
                .join(PaperExecutionPlan, PaperExecutionPlan.id == PaperPlanEvaluation.plan_id)
                .where(condition)
                .order_by(
                    desc(PaperPlanEvaluation.evaluation_date),
                    desc(PaperPlanEvaluation.id),
                )
                .offset(max(0, offset))
                .limit(max(1, min(limit, 500)))
            ).all()
            return list(rows), int(total)

    def list_observations(
        self,
        *,
        model: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[List[PaperPlanObservation], int]:
        condition = PaperPlanObservation.source_model == model if model else True
        with self.db.get_session() as session:
            total = session.execute(
                select(func.count(PaperPlanObservation.id)).where(condition)
            ).scalar() or 0
            rows = session.execute(
                select(PaperPlanObservation)
                .where(condition)
                .order_by(desc(PaperPlanObservation.observed_at), desc(PaperPlanObservation.id))
                .offset(max(0, offset))
                .limit(max(1, min(limit, 500)))
            ).scalars().all()
            return list(rows), int(total)

    def latest_observation_bucket(self) -> Optional[datetime]:
        with self.db.get_session() as session:
            return session.execute(
                select(func.max(PaperPlanObservation.bucket_started_at))
            ).scalar_one_or_none()

    def list_events(
        self,
        *,
        model: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Tuple[List[PaperTradeEvent], int]:
        clause = PaperTradeEvent.source_model == model if model else True
        with self.db.get_session() as session:
            total = session.execute(select(func.count(PaperTradeEvent.id)).where(clause)).scalar() or 0
            rows = session.execute(
                select(PaperTradeEvent)
                .where(clause)
                .order_by(desc(PaperTradeEvent.event_date), desc(PaperTradeEvent.id))
                .offset(max(0, offset))
                .limit(max(1, min(limit, 500)))
            ).scalars().all()
            return list(rows), int(total)

    def list_reanalysis_requests_for_plan_ids(self, plan_ids: List[int]) -> List[PaperReanalysisRequest]:
        ids = [int(plan_id) for plan_id in plan_ids if plan_id is not None]
        if not ids:
            return []
        with self.db.get_session() as session:
            return list(session.execute(
                select(PaperReanalysisRequest)
                .where(PaperReanalysisRequest.source_plan_id.in_(ids))
                .order_by(PaperReanalysisRequest.source_plan_id, desc(PaperReanalysisRequest.created_at), desc(PaperReanalysisRequest.id))
            ).scalars().all())

    def list_dispatches_for_event_ids(self, event_ids: List[int]) -> List[PaperNotificationDispatch]:
        ids = [int(event_id) for event_id in event_ids if event_id is not None]
        if not ids:
            return []
        with self.db.get_session() as session:
            rows = session.execute(
                select(PaperNotificationDispatch)
                .where(PaperNotificationDispatch.event_id.in_(ids))
                .order_by(PaperNotificationDispatch.event_id, desc(PaperNotificationDispatch.created_at), desc(PaperNotificationDispatch.id))
            ).scalars().all()
            return list(rows)

    def daily_bars(
        self,
        market: str,
        stock_code: str,
        through: date,
        *,
        limit: int = 80,
    ) -> List[StockDaily]:
        variants = stock_daily_variants(market, stock_code)
        if not variants:
            return []
        with self.db.get_session() as session:
            rows = session.execute(
                select(StockDaily)
                .where(StockDaily.code.in_(variants), StockDaily.date <= through)
                .order_by(desc(StockDaily.date), desc(StockDaily.id))
                .limit(max(1, limit) * len(variants))
            ).scalars().all()
            by_date = {}
            for row in rows:
                by_date.setdefault(row.date, []).append(row)
            reconciled = []
            for bar_date in sorted(by_date, reverse=True)[:max(1, limit)]:
                candidates = by_date[bar_date]
                candidates.sort(key=lambda row: variants.index(row.code))
                chosen = candidates[0]
                snapshots = {
                    tuple(getattr(row, field) for field in ("open", "high", "low", "close", "volume"))
                    for row in candidates
                }
                chosen._paper_variant_conflict = len(snapshots) > 1
                reconciled.append(chosen)
            return list(reversed(reconciled))

    def performance(self, *, model: Optional[str] = None) -> dict:
        """Aggregate the complete ledger without pagination limits."""

        plan_condition = PaperExecutionPlan.source_model == model if model else True
        position_condition = PaperPosition.source_model == model if model else True
        with self.db.get_session() as session:
            realized_qty = func.coalesce(PaperPosition.realized_quantity, 0.0)
            realized_pnl_value = case(
                (realized_qty > 0, func.coalesce(PaperPosition.realized_pnl, 0.0)),
                (PaperPosition.status == "closed", func.coalesce(PaperPosition.simulated_pnl, 0.0)),
                else_=func.coalesce(PaperPosition.realized_pnl, 0.0),
            )
            realized_return_value = case(
                (realized_qty > 0, PaperPosition.simulated_return_pct),
                (PaperPosition.status == "closed", PaperPosition.simulated_return_pct),
                else_=None,
            )
            total_plans, executable_plans = session.execute(
                select(
                    func.count(PaperExecutionPlan.id),
                    func.coalesce(func.sum(func.cast(PaperExecutionPlan.executable, Integer)), 0),
                ).where(plan_condition)
            ).one()
            stats = session.execute(
                select(
                    func.coalesce(func.sum(func.cast(PaperPosition.status == "open", Integer)), 0),
                    func.coalesce(func.sum(func.cast(PaperPosition.status == "closed", Integer)), 0),
                    func.coalesce(func.sum(func.cast(realized_pnl_value > 0, Integer)), 0),
                    func.coalesce(func.sum(func.cast(realized_pnl_value < 0, Integer)), 0),
                    func.sum(realized_pnl_value),
                    func.sum(case((or_(realized_qty > 0, PaperPosition.status == "closed"), func.coalesce(PaperPosition.initial_allocated_capital, PaperPosition.normalized_notional)), else_=0.0)),
                    func.avg(realized_return_value),
                    func.count(realized_return_value),
                    func.coalesce(func.sum(func.cast(or_(realized_qty > 0, PaperPosition.status == "closed"), Integer)), 0),
                    func.coalesce(func.sum(func.cast(and_(realized_qty > 0, PaperPosition.status == "open"), Integer)), 0),
                    func.sum(func.coalesce(PaperPosition.realized_fees, 0.0)),
                ).where(position_condition)
            ).one()
            event_condition = PaperTradeEvent.source_model == model if model else True
            event_stats = session.execute(
                select(
                    func.coalesce(func.sum(PaperTradeEvent.gross_amount), 0.0),
                    func.coalesce(func.sum(PaperTradeEvent.net_amount), 0.0),
                    func.coalesce(func.sum(PaperTradeEvent.fees), 0.0),
                    func.coalesce(func.sum(PaperTradeEvent.commission), 0.0),
                    func.coalesce(func.sum(PaperTradeEvent.tax), 0.0),
                    func.coalesce(func.sum(PaperTradeEvent.slippage_amount), 0.0),
                    func.coalesce(func.sum(PaperTradeEvent.gross_realized_pnl), 0.0),
                    func.coalesce(func.sum(PaperTradeEvent.net_realized_pnl), 0.0),
                    func.coalesce(func.sum(func.cast(PaperTradeEvent.event_type == "reduce", Integer)), 0),
                    func.coalesce(func.sum(func.cast(PaperTradeEvent.event_type == "close", Integer)), 0),
                    func.coalesce(func.sum(func.cast(PaperTradeEvent.event_type == "pending_exit", Integer)), 0),
                    func.coalesce(func.sum(func.cast(PaperTradeEvent.logical_order_key.is_not(None), Integer)), 0),
                    func.coalesce(func.sum(func.cast(PaperTradeEvent.fill_evidence_grade == "quote_based_unverified", Integer)), 0),
                ).where(event_condition)
            ).one()
            open_cost = session.execute(
                select(func.coalesce(func.sum(PaperPosition.remaining_cost), 0.0))
                .where(and_(position_condition, PaperPosition.status == "open"))
            ).scalar_one()
            realized_pnl = float(stats[4] or 0.0)
            unrealized_pnl = 0.0
            return {
                "total_plans": int(total_plans or 0),
                "executable_plans": int(executable_plans or 0),
                "open_positions": int(stats[0] or 0),
                "closed_positions": int(stats[1] or 0),
                "wins": int(stats[2] or 0),
                "losses": int(stats[3] or 0),
                "gross_realized_pnl": realized_pnl + float(stats[10] or 0.0),
                "net_realized_pnl": realized_pnl,
                "unrealized_pnl": unrealized_pnl,
                "total_pnl": realized_pnl + unrealized_pnl,
                "total_notional": float(stats[5] or 0.0),
                "avg_return_pct": float(stats[6]) if stats[6] is not None else None,
                "return_count": int(stats[7] or 0),
                "realized_positions": int(stats[8] or 0),
                "partially_realized_positions": int(stats[9] or 0),
                "total_fees": float(stats[10] or 0.0),
                "event_gross_amount": float(event_stats[0] or 0.0),
                "event_net_amount": float(event_stats[1] or 0.0),
                "event_fees": float(event_stats[2] or 0.0),
                "event_commission": float(event_stats[3] or 0.0),
                "event_sell_tax": float(event_stats[4] or 0.0),
                "event_slippage_amount": float(event_stats[5] or 0.0),
                "event_gross_realized_pnl": float(event_stats[6] or 0.0),
                "event_net_realized_pnl": float(event_stats[7] or 0.0),
                "partial_exit_orders": int(event_stats[8] or 0),
                "full_exit_orders": int(event_stats[9] or 0),
                "pending_exit_orders": int(event_stats[10] or 0),
                "logical_order_allocation_events": int(event_stats[11] or 0),
                "quote_based_unverified_events": int(event_stats[12] or 0),
                "open_remaining_cost": float(open_cost or 0.0),
            }

    def performance_models(self) -> List[str]:
        with self.db.get_session() as session:
            plan_models = session.execute(select(PaperExecutionPlan.source_model).distinct()).scalars()
            position_models = session.execute(select(PaperPosition.source_model).distinct()).scalars()
            return sorted({str(value).strip() for value in [*plan_models, *position_models] if str(value or "").strip()})

    def latest_defensive_signal(
        self,
        *,
        market: str,
        stock_code: str,
        source_model: str,
        after_date: date,
        through_date: date,
        after_time: Optional[datetime] = None,
        execution_scope: str = REPORT_EXECUTION_SCOPE_PRIMARY,
    ) -> Optional[DecisionSignalRecord]:
        time_boundary = after_time or datetime.combine(after_date, datetime.min.time())
        with self.db.get_session() as session:
            return session.execute(
                select(DecisionSignalRecord)
                .outerjoin(
                    AnalysisHistory,
                    and_(
                        DecisionSignalRecord.source_type == "analysis",
                        DecisionSignalRecord.source_report_id == AnalysisHistory.id,
                    ),
                )
                .where(
                    DecisionSignalRecord.market == market,
                    DecisionSignalRecord.stock_code == stock_code,
                    DecisionSignalRecord.status == "active",
                    DecisionSignalRecord.plan_quality.notin_(("minimal", "unknown")),
                    or_(
                        and_(
                            DecisionSignalRecord.source_type == "analysis",
                            DecisionSignalRecord.source_report_id.is_not(None),
                            AnalysisHistory.model_used == source_model,
                            authorized_report_execution_clause(
                                AnalysisHistory.id,
                                execution_scope=execution_scope,
                            ),
                        ),
                        and_(
                            or_(
                                DecisionSignalRecord.source_type != "analysis",
                                DecisionSignalRecord.source_report_id.is_(None),
                            ),
                            DecisionSignalRecord.source_agent == source_model,
                        ),
                    ),
                    DecisionSignalRecord.created_at > time_boundary,
                    # Keep the bound value typed DATE on PostgreSQL; an ISO
                    # string becomes VARCHAR and cannot be compared with date().
                    func.date(DecisionSignalRecord.created_at) <= through_date,
                )
                .order_by(desc(DecisionSignalRecord.created_at), desc(DecisionSignalRecord.id))
                .limit(1)
            ).scalar_one_or_none()
