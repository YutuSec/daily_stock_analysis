"""R7/R9: real ledger continuity and strict-session negative cases."""
import json
from datetime import date, datetime
from unittest.mock import patch

import pytest
from sqlalchemy import func, select

from src.storage import (
    AnalysisHistory, DatabaseManager, DecisionSignalRecord, PaperExecutionPlan,
    PaperPosition, PaperStrategyThesis, PaperStrategyTransition, PaperTradeEvent,
)
from src.services.paper_trading_service import PaperTradingService
from src.repositories.paper_trading_repo import PaperTradingRepository
from tests.report_intent_helpers import add_report_execution_intent


@pytest.fixture
def ledger(tmp_path):
    DatabaseManager.reset_instance()
    db = DatabaseManager(db_url=f"sqlite:///{tmp_path / 'continuity.db'}")
    yield db
    DatabaseManager.reset_instance()


def report_signal(db, day, action="buy", model="audit-model", declared_day=None):
    with db.session_scope() as session:
        report = AnalysisHistory(
            code="002714.SZ", name="测试", report_type="full", model_used=model,
            created_at=datetime.combine(day, datetime.min.time()),
            raw_result=json.dumps({"success": True, "analysis_summary": "thesis evidence"}),
        )
        session.add(report)
        session.flush()
        add_report_execution_intent(session, report.id)
        signal = DecisionSignalRecord(
            source_report_id=report.id, source_agent=model, source_type="analysis",
            market="cn", stock_code="002714", action=action, trigger_source="manual",
            horizon="swing", created_at=report.created_at, status="active", plan_quality="complete",
            metadata_json=json.dumps({"market_phase_summary": {"session_date": (declared_day or day).isoformat()}}),
        )
        session.add(signal)
        session.flush()
        return report.id, signal.id


def adjudicate(db, service, day, *, executable=True, action="buy", declared_day=None, evaluated_on=None):
    rid, sid = report_signal(db, day, action, declared_day=declared_day)
    with db.get_session() as session, patch(
        "src.services.paper_trading_service.utc_naive_now",
        return_value=datetime.combine(evaluated_on or day, datetime.min.time()).replace(hour=8),
    ):
        report, signal = session.get(AnalysisHistory, rid), session.get(DecisionSignalRecord, sid)
        return service._evaluate_strategy_thesis_transition(
            report=report, signal=signal, source_model="audit-model", market="cn",
            stock_code="002714", stock_name="测试", trading_style="position", raw_action=action,
            raw_report={}, entry_low=100, entry_high=101, stop_loss=95, target_price=120,
            initial_executable=executable, invalid_reasons=[] if executable else ["missing_confirmation_data"],
            report_created_at=report.created_at,
        )


def test_default_confirmation_distinct_sessions_not_weekends(ledger):
    service = PaperTradingService(repo=PaperTradingRepository(ledger))
    assert service.strategy_entry_confirmation_sessions == 2
    first = adjudicate(ledger, service, date(2026, 9, 4))
    repeated = adjudicate(ledger, service, date(2026, 9, 4))
    weekend = adjudicate(ledger, service, date(2026, 9, 5))
    second = adjudicate(ledger, service, date(2026, 9, 7))
    assert first["confirmation_count"] == repeated["confirmation_count"] == weekend["confirmation_count"] == 1
    assert not any(row["plan_authorized"] for row in (first, repeated, weekend))
    assert weekend["reason"] == "confirmation_session_unavailable_or_non_trading"
    assert not weekend["freeze_entry_authorized"]
    assert second["plan_authorized"] and second["confirmation_count"] == 2


def test_style_overrides_require_market_in_both_prompt_and_execution(ledger):
    from types import SimpleNamespace
    from src.config import parse_paper_trading_style_overrides
    from src.services.paper_trading_service import resolve_paper_trading_style_context
    raw = {"600519": "trend", "cn:600519": "short_swing"}
    parsed = parse_paper_trading_style_overrides(json.dumps(raw))
    assert parsed == {"cn:600519": "short_swing"}
    service = PaperTradingService(repo=PaperTradingRepository(ledger), trading_style_overrides=raw)
    config = SimpleNamespace(paper_trading_style="position", paper_trading_style_overrides=raw)
    assert service._resolve_effective_trading_style(market="cn", stock_code="600519.SH")[0] == "short_swing"
    assert resolve_paper_trading_style_context(config, market="cn", stock_code="600519.SH")["trading_style"] == "short_swing"
    assert service._resolve_effective_trading_style(market="us", stock_code="600519")[0] == "position"
    assert resolve_paper_trading_style_context(config, market="us", stock_code="600519")["trading_style"] == "position"


def test_status_schema_confirmation_default_matches_runtime():
    from api.v1.schemas.paper_trading import PaperTradingStatusResponse
    assert PaperTradingStatusResponse.model_fields["strategy_entry_confirmation_sessions"].default == 2


def test_bootstrap_consumes_standard_signal_without_optional_source_agent(ledger):
    rid, sid = report_signal(ledger, date(2026, 9, 4))
    with ledger.session_scope() as session:
        session.get(DecisionSignalRecord, sid).source_agent = None
    service = PaperTradingService(repo=PaperTradingRepository(ledger))
    assert service.bootstrap_strategy_observations()["created"] == 1
    with ledger.get_session() as session:
        thesis = session.scalar(select(PaperStrategyThesis))
        assert thesis.source_model == "audit-model"
        assert thesis.last_report_id == rid
        assert thesis.accepted_report_id is None and thesis.confirmation_count == 0


def test_bootstrap_mismatched_signal_cannot_seed_or_shadow_another_symbol(ledger):
    report_signal(ledger, date(2026, 9, 3))
    latest, sid = report_signal(ledger, date(2026, 9, 4))
    valid_other, other_sid = report_signal(ledger, date(2026, 9, 3))
    with ledger.session_scope() as session:
        session.get(DecisionSignalRecord, sid).stock_code = "600519"
        session.get(AnalysisHistory, valid_other).code = "600519.SH"
        session.get(DecisionSignalRecord, other_sid).stock_code = "600519"
    service = PaperTradingService(repo=PaperTradingRepository(ledger))
    assert service.bootstrap_strategy_observations()["created"] == 1
    with ledger.get_session() as session:
        thesis = session.scalar(select(PaperStrategyThesis))
        assert thesis.stock_code == "600519" and thesis.last_report_id == valid_other
        assert thesis.last_report_id != latest
        assert thesis.accepted_report_id is None


def test_monitor_preflight_preserves_confirmed_defensive_transition(ledger):
    from tests.test_paper_trading_service import _seed_open_paper_lot, _confirmed_defensive_signal, _single_confirmation_service
    _seed_open_paper_lot(ledger, opened_on=date(2026, 8, 7))
    service = _single_confirmation_service()
    rid, sid = _confirmed_defensive_signal(ledger, service)
    with ledger.get_session() as session:
        report = session.get(AnalysisHistory, rid)
        signal = session.get(DecisionSignalRecord, sid)
        before = session.scalar(select(func.count()).select_from(PaperStrategyTransition))
        transition = service._ensure_strategy_transition_for_signal(
            report=report, signal=signal, source_model="model-a", market="cn",
            stock_code="600519", invalid_reasons=("monitor_call_without_entry_authority",),
        )
        assert transition["risk_revision_authorized"]
        assert transition["reason"] == "accepted_confirmed_defensive_flip"
        assert session.scalar(select(func.count()).select_from(PaperStrategyTransition)) == before


def test_unknown_calendar_cannot_confirm(ledger):
    service = PaperTradingService(repo=PaperTradingRepository(ledger))
    with patch("src.core.trading_calendar._XCALS_AVAILABLE", False):
        result = adjudicate(ledger, service, date(2026, 9, 7))
    assert not result["plan_authorized"]
    assert result["confirmation_count"] == 0


def test_missing_evidence_does_not_confirm_or_preserve_old_entry(ledger):
    service = PaperTradingService(repo=PaperTradingRepository(ledger))
    adjudicate(ledger, service, date(2026, 9, 3))
    bad = adjudicate(ledger, service, date(2026, 9, 4), executable=False)
    assert not bad["plan_authorized"] and bad["freeze_entry_authorized"]
    assert bad["confirmation_count"] == 0
    following = adjudicate(ledger, service, date(2026, 9, 7))
    assert following["confirmation_count"] == 1 and not following["plan_authorized"]


def test_bootstrap_latest_only_is_idempotent_and_never_replays(ledger):
    service = PaperTradingService(repo=PaperTradingRepository(ledger))
    report_signal(ledger, date(2026, 8, 13))
    latest, _ = report_signal(ledger, date(2026, 9, 4))
    summary = service.bootstrap_strategy_observations()
    assert summary["created"] == 1
    assert service.bootstrap_strategy_observations()["created"] == 0
    with ledger.get_session() as session:
        thesis = session.execute(select(PaperStrategyThesis)).scalar_one()
        transition = session.execute(select(PaperStrategyTransition)).scalar_one()
        assert thesis.last_report_id == latest
        assert thesis.accepted_report_id is None and thesis.stop_loss is None
        assert thesis.effective_action == "watch" and thesis.confirmation_count == 0
        assert transition.result == "observed"
        assert not transition.plan_authorized and not transition.risk_revision_authorized
        for model in (PaperExecutionPlan, PaperPosition, PaperTradeEvent):
            assert session.execute(select(func.count()).select_from(model)).scalar_one() == 0
    following = adjudicate(ledger, service, date(2026, 9, 7))
    assert not following["plan_authorized"] and following["confirmation_count"] == 1


def test_bootstrap_does_not_overwrite_existing_thesis(ledger):
    service = PaperTradingService(repo=PaperTradingRepository(ledger))
    adjudicate(ledger, service, date(2026, 9, 3))
    adjudicate(ledger, service, date(2026, 9, 4))
    assert service.bootstrap_strategy_observations()["created"] == 0
    with ledger.get_session() as session:
        thesis = session.execute(select(PaperStrategyThesis)).scalar_one()
        assert thesis.state == "entry_active" and thesis.confirmation_count == 2


def test_future_metadata_date_does_not_manufacture_confirmation(ledger):
    service = PaperTradingService(repo=PaperTradingRepository(ledger))
    adjudicate(ledger, service, date(2026, 9, 4))
    result = adjudicate(ledger, service, date(2026, 9, 4), declared_day=date(2026, 9, 7))
    assert result["confirmation_count"] == 1 and not result["plan_authorized"]
    assert not result["evidence"]["confirmation_session_valid"]


@pytest.mark.parametrize("has_signal", [True, False])
def test_bootstrap_latest_failure_never_falls_back(ledger, has_signal):
    service = PaperTradingService(repo=PaperTradingRepository(ledger))
    report_signal(ledger, date(2026, 8, 13))
    latest, sid = report_signal(ledger, date(2026, 9, 4))
    with ledger.session_scope() as session:
        session.get(AnalysisHistory, latest).raw_result = '{"success": false}'
        if not has_signal:
            session.delete(session.get(DecisionSignalRecord, sid))
    assert service.bootstrap_strategy_observations()["created"] == 0
    with ledger.get_session() as session:
        assert session.execute(select(func.count()).select_from(PaperStrategyThesis)).scalar_one() == 0


def test_expired_reports_cannot_seed_new_thesis_authority(ledger):
    service = PaperTradingService(repo=PaperTradingRepository(ledger))
    outcomes = [adjudicate(ledger, service, day, evaluated_on=date(2026, 9, 7))
                for day in (date(2026, 8, 13), date(2026, 8, 14))]
    assert all(not item["plan_authorized"] and item["confirmation_count"] == 0 for item in outcomes)
    assert all(item["reason"] == "strategy_report_outside_confirmation_window" for item in outcomes)


def test_old_candidate_does_not_count_toward_fresh_confirmation(ledger):
    service = PaperTradingService(repo=PaperTradingRepository(ledger))
    first = adjudicate(ledger, service, date(2026, 8, 13))
    latest = adjudicate(ledger, service, date(2026, 9, 7))
    assert first["confirmation_count"] == latest["confirmation_count"] == 1
    assert not latest["plan_authorized"]
