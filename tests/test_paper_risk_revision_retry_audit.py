"""R5 risk-revision retry and per-lot thesis sync regressions."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select

from src.repositories.paper_trading_repo import PaperTradingRepository
from src.services.paper_trading_service import PaperTradingService
from src.storage import (
    AnalysisHistory,
    DatabaseManager,
    DecisionSignalRecord,
    PaperExecutionPlan,
    PaperPlanObservation,
    PaperPosition,
    PaperRiskRevision,
    PaperStrategyThesis,
)
from tests.report_intent_helpers import add_report_execution_intent


@pytest.fixture()
def ledger(tmp_path):
    DatabaseManager.reset_instance()
    db = DatabaseManager(db_url=f"sqlite:///{tmp_path / 'risk-revision.db'}")
    try:
        yield db
    finally:
        DatabaseManager.reset_instance()


def _service(db: DatabaseManager) -> PaperTradingService:
    return PaperTradingService(repo=PaperTradingRepository(db))


def _add_report_signal(
    session,
    *,
    code: str = "600519",
    model: str = "model-a",
    action: str = "hold",
    created_at: datetime,
    target_price: float | None = 140.0,
) -> tuple[AnalysisHistory, DecisionSignalRecord]:
    report = AnalysisHistory(
        query_id=f"risk-revision-{created_at.isoformat()}",
        code=code,
        name="贵州茅台",
        report_type="full",
        model_used=model,
        raw_result=json.dumps({"success": True, "analysis_summary": "holding risk update"}, ensure_ascii=False),
        created_at=created_at,
    )
    session.add(report)
    session.flush()
    add_report_execution_intent(session, report.id)
    holding_action = {"action": action}
    if target_price is not None:
        holding_action["target_price"] = target_price
    base_action = "watch" if action == "hold" else action
    score = 50 if action == "hold" else 70 if action == "buy" else 30
    signal = DecisionSignalRecord(
        stock_code=code,
        stock_name="贵州茅台",
        market="cn",
        source_type="analysis",
        source_report_id=report.id,
        source_agent=model,
        trigger_source="system",
        action=action,
        horizon="10d",
        confidence=0.8,
        score=score,
        plan_quality="partial",
        status="active",
        metadata_json=json.dumps({
            "holding_state": "holding",
            "base_action": base_action,
            "effective_action_source": "position_actions.holding",
            "position_actions": {"holding": holding_action},
        }, ensure_ascii=False),
        created_at=created_at,
    )
    session.add(signal)
    session.flush()
    return report, signal


def _add_plan_position(
    session,
    *,
    report: AnalysisHistory,
    signal: DecisionSignalRecord,
    plan_key: str,
    opened_at: datetime,
    stop_loss: float = 90.0,
    target_price: float = 130.0,
    model: str = "model-a",
    code: str = "600519",
    trading_style: str = "position",
) -> tuple[PaperExecutionPlan, PaperPosition]:
    plan = PaperExecutionPlan(
        plan_key=plan_key,
        plan_version="paper-hybrid-v3",
        source_report_id=report.id,
        decision_signal_id=signal.id,
        source_model=model,
        market="cn",
        stock_code=code,
        stock_name="贵州茅台",
        report_date=opened_at.date(),
        report_created_at=opened_at,
        valid_for_date=opened_at.date(),
        action="buy",
        executable=True,
        status="triggered",
        rules_json="[]",
        descriptive_conditions_json="[]",
        stop_loss=stop_loss,
        target_price=target_price,
        max_holding_sessions=20,
        trading_style=trading_style,
    )
    session.add(plan)
    session.flush()
    position = PaperPosition(
        plan_id=plan.id,
        source_report_id=report.id,
        decision_signal_id=signal.id,
        source_model=model,
        market="cn",
        stock_code=code,
        stock_name="贵州茅台",
        status="open",
        opened_on=opened_at.date(),
        opened_at=opened_at,
        opened_price=100.0,
        normalized_notional=10000.0,
        quantity=100.0,
        initial_quantity=100.0,
        available_quantity=100.0,
        frozen_quantity=0.0,
        stop_loss=stop_loss,
        target_price=target_price,
        max_holding_sessions=20,
        trading_style=trading_style,
    )
    session.add(position)
    session.flush()
    return plan, position


def _authorize_holding_risk_revision(
    service: PaperTradingService,
    db: DatabaseManager,
    report_id: int,
    signal_id: int,
) -> None:
    with db.get_session() as session:
        report = session.get(AnalysisHistory, report_id)
        signal = session.get(DecisionSignalRecord, signal_id)
        transition = service._evaluate_strategy_thesis_transition(
            report=report,
            signal=signal,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            trading_style="position",
            raw_action="hold",
            raw_report=json.loads(report.raw_result),
            entry_low=None,
            entry_high=None,
            stop_loss=None,
            target_price=140.0,
            initial_executable=False,
            invalid_reasons=["risk_refresh_without_entry_authority"],
            report_created_at=report.created_at,
        )
    assert transition["risk_revision_authorized"] is True


def test_reconcile_risk_revision_retries_after_fresh_valuation_mark_and_is_idempotent(ledger):
    service = _service(ledger)
    opened_at = datetime(2026, 8, 3, 10, 0)
    report_at = datetime(2026, 8, 4, 10, 0)
    first_eval = datetime(2026, 8, 4, 10, 1, tzinfo=timezone.utc)
    retry_eval = datetime(2026, 8, 4, 10, 5, tzinfo=timezone.utc)

    with ledger.session_scope() as session:
        entry_report, entry_signal = _add_report_signal(
            session,
            action="buy",
            created_at=opened_at - timedelta(days=1),
            target_price=130.0,
        )
        plan, position = _add_plan_position(
            session,
            report=entry_report,
            signal=entry_signal,
            plan_key="risk-retry-plan",
            opened_at=opened_at,
        )
        risk_report, risk_signal = _add_report_signal(
            session,
            action="hold",
            created_at=report_at,
            target_price=140.0,
        )
        risk_report_id = risk_report.id
        risk_signal_id = risk_signal.id
        plan_id = plan.id
        position_id = position.id

    _authorize_holding_risk_revision(service, ledger, risk_report_id, risk_signal_id)

    with ledger.get_session() as session:
        risk_report = session.get(AnalysisHistory, risk_report_id)
        risk_signal = session.get(DecisionSignalRecord, risk_signal_id)
        first = service._refresh_open_position_risk_from_report(
            report=risk_report,
            signal=risk_signal,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            current_time=first_eval,
        )
    assert first["status"] == "audited"
    assert first["items"][0]["result"] == "deferred"
    assert first["items"][0]["retryable"] is True
    with ledger.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.target_price == 130.0
        assert session.scalar(select(func.count()).select_from(PaperRiskRevision)) == 0

    with ledger.session_scope() as session:
        session.add(PaperPlanObservation(
            observation_key="risk-retry-fresh-valuation",
            plan_id=plan_id,
            position_id=position_id,
            source_report_id=risk_report_id,
            decision_signal_id=risk_signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            mode="intraday",
            observed_at=retry_eval.replace(tzinfo=None) - timedelta(minutes=1),
            bucket_started_at=retry_eval.replace(tzinfo=None).replace(minute=0, second=0, microsecond=0),
            quote_price=100.0,
            quote_source="fresh_valuation_mark",
            result="valuation_mark",
            action="hold",
            required_passed=True,
            evidence_json="{}",
        ))

    second = service.reconcile_open_position_risk_from_latest_reports(current_time=retry_eval)
    assert second["signals_found"] == 1
    assert second["revisions_applied"] == 1
    with ledger.get_session() as session:
        position = session.get(PaperPosition, position_id)
        revision = session.scalar(select(PaperRiskRevision))
        assert position.target_price == 140.0
        assert revision.result == "applied"
        assert revision.evidence_json

    third = service.reconcile_open_position_risk_from_latest_reports(current_time=retry_eval)
    assert third["signals_found"] == 1
    assert third["revisions_applied"] == 0
    with ledger.get_session() as session:
        assert session.scalar(select(func.count()).select_from(PaperRiskRevision)) == 1
        assert session.get(PaperPosition, position_id).target_price == 140.0


def test_thesis_risk_sync_requeries_all_open_lots_and_preserves_per_lot_prices(ledger):
    service = _service(ledger)
    opened_at = datetime(2026, 8, 3, 10, 0)
    with ledger.session_scope() as session:
        report, signal = _add_report_signal(
            session,
            action="buy",
            created_at=opened_at - timedelta(days=1),
            target_price=130.0,
        )
        _plan1, first = _add_plan_position(
            session,
            report=report,
            signal=signal,
            plan_key="risk-sync-plan-1",
            opened_at=opened_at,
            stop_loss=91.0,
            target_price=131.0,
        )
        _plan2, second = _add_plan_position(
            session,
            report=report,
            signal=signal,
            plan_key="risk-sync-plan-2",
            opened_at=opened_at + timedelta(minutes=1),
            stop_loss=92.0,
            target_price=132.0,
        )
        thesis_key = service._strategy_thesis_key(
            source_model="model-a",
            market="cn",
            stock_code="600519",
            trading_style="position",
        )
        thesis = PaperStrategyThesis(
            thesis_key=thesis_key,
            policy_version="strategy-thesis-v1",
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            trading_style="position",
            state="position_monitoring",
            effective_action="hold",
            raw_action="hold",
            action_bucket="holding",
            confirmation_count=0,
            contradiction_count=0,
            last_report_id=report.id,
            last_signal_id=signal.id,
            stop_loss=90.0,
            target_price=130.0,
            evidence_json="{}",
        )
        session.add(thesis)
        session.flush()

        sync = service._sync_thesis_effective_risk_lines_in_session(
            session,
            positions=[first],
            evaluated_at=datetime(2026, 8, 4, 10, 5),
        )

        assert sync["result"] == "per_lot_only"
        assert sync["reason"] == "risk_lines_differ_across_lots"
        assert sync["position_ids"] == [first.id, second.id]
        assert {item["position_id"] for item in sync["per_lot_risk_lines"]} == {first.id, second.id}
        assert thesis.stop_loss == 90.0
        assert thesis.target_price == 130.0
        evidence = json.loads(thesis.evidence_json)
        assert evidence["risk_revision_sync"]["result"] == "per_lot_only"
        assert len(evidence["risk_revision_sync"]["per_lot_risk_lines"]) == 2
