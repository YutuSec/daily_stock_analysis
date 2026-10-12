"""Actual producer shapes survive report validation and reach policy consumers."""
import os
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from src.analyzer import GeminiAnalyzer
from src.core.pipeline import StockAnalysisPipeline
from src.schemas.report_schema import BattlePlan
from src.services.paper_strategy_policy import entry_geometry_guard, setup_contract
from src.services.paper_trading_service import TRADING_STYLE_SETTINGS
from src.storage import normalize_report_execution_intent


def test_setup_tool_schema_validation_and_consumer_agree():
    tool = GeminiAnalyzer._analysis_report_tool()
    props = tool["function"]["parameters"]["properties"]
    battle_schema = props["dashboard"]["properties"]["battle_plan"]
    assert set(battle_schema["properties"]["setup"]["properties"]["setup_type"]["enum"]) == {
        "breakout", "pullback", "trend_continuation",
    }
    battle = BattlePlan.model_validate({"setup": {"setup_type": "pullback"}}).model_dump()
    contract = setup_contract({"dashboard": {"battle_plan": battle}}, [])
    assert contract["setup_type"] == "pullback"
    assert contract["source"] == "explicit_structured_report"


def test_real_style_snapshot_atr_and_support_reach_geometry_guard():
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    settings = TRADING_STYLE_SETTINGS["position"]
    # Complete chronological price observations, no mocked snapshot fields.
    dates = pd.bdate_range("2025-01-01", periods=180)
    frame = pd.DataFrame({
        "date": dates, "open": 100.0, "close": 100.0,
        "high": 105.0, "low": 95.0, "volume": 1000.0,
    })
    snapshot = pipeline._style_indicator_snapshot(frame, {
        "trading_style": "position", "settings": settings,
    })
    assert snapshot["support"] == 100
    assert snapshot["volatility"]["atr20"] == 10
    rejected = entry_geometry_guard(
        style="position", style_settings=settings, indicator_snapshot=snapshot,
        entry_reference=100, stop_loss=96,
    )
    assert rejected["atr"] == 10 and not rejected["passed"]
    assert "stop_within_atr_noise" in rejected["reasons"]
    accepted = entry_geometry_guard(
        style="position", style_settings=settings, indicator_snapshot=snapshot,
        entry_reference=100, stop_loss=88,
    )
    assert accepted["passed"] and accepted["support"] == 100


def _hook_pipeline(execution_intent=None, *, db=None):
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    pipeline.config = SimpleNamespace(paper_trading_enabled=True, litellm_model="model-a")
    pipeline.query_source = "system"
    pipeline.trace_id = None
    pipeline.portfolio_context = None
    pipeline.db = db
    if execution_intent is not None:
        pipeline.execution_intent = normalize_report_execution_intent(execution_intent)
    return pipeline


def test_research_only_pipeline_rejects_account_context_before_opening_ledger():
    with patch("src.core.pipeline.get_db") as get_db:
        with pytest.raises(ValueError, match="account-agnostic"):
            StockAnalysisPipeline(
                config=SimpleNamespace(max_workers=1, save_context_snapshot=False),
                portfolio_context={"source": "portfolio", "quantity": 100},
                execution_intent={"execution_intent": "research_only", "intent_source": "unit_test"},
            )
    get_db.assert_not_called()


def test_research_only_post_save_hook_skips_signal_and_paper_reconcile():
    research = _hook_pipeline({"execution_intent": "research_only", "intent_source": "unit_test"})
    default = _hook_pipeline()
    hook_args = dict(query_id="q", report_type="detailed", context_snapshot={})
    with patch(
        "src.core.pipeline.extract_and_persist_from_analysis_result",
        return_value={"item": {"id": 5}},
    ) as extract, patch("src.services.paper_trading_service.PaperTradingService.from_config") as paper:
        assert research._effective_portfolio_context_for_stock("600519") is None
        research._extract_decision_signal_after_history_save(
            result=SimpleNamespace(code="600519"), source_report_id=7, **hook_args,
        )
        extract.assert_not_called()
        paper.assert_not_called()

        # The default producer still feeds the signal layer and paper reconcile.
        default._extract_decision_signal_after_history_save(
            result=SimpleNamespace(code="600519"), source_report_id=8, **hook_args,
        )
    assert extract.call_args.kwargs["source_report_id"] == 8
    paper.return_value.reconcile_report_execution_plan.assert_called_once_with(8, decision_signal_id=5)


def test_research_pipeline_report_persists_purpose_and_scheduled_compensation_stays_blocked(tmp_path):
    from src.analyzer import AnalysisResult
    from src.config import Config
    from src.services.paper_trading_service import PaperTradingService
    from src.storage import DatabaseManager, DecisionSignalRecord, PaperExecutionPlan, ReportPlanReconciliation

    old_database_path = os.environ.get("DATABASE_PATH")
    os.environ["DATABASE_PATH"] = str(tmp_path / "research-producer.db")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    try:
        db = DatabaseManager.get_instance()
        pipeline = _hook_pipeline(
            {"execution_intent": "research_only", "decision_as_of": "2026-09-25", "intent_source": "market_opportunity_research"},
            db=db,
        )
        result = AnalysisResult(
            code="600519", name="贵州茅台", sentiment_score=75, trend_prediction="看多",
            operation_advice="买入", decision_type="buy", analysis_summary="research evidence",
        )
        report_id = db.save_analysis_history(
            result=result, query_id="research-producer", report_type="detailed",
            news_content=None, execution_intent=pipeline._report_execution_intent(),
        )
        pipeline._extract_decision_signal_after_history_save(
            result=result, query_id="research-producer", source_report_id=report_id,
            report_type="detailed", context_snapshot={},
        )
        compensation = PaperTradingService().reconcile_report_execution_plans()

        intent = db.get_report_execution_intent(report_id)
        assert (intent.execution_intent, intent.execution_scope, intent.decision_as_of.isoformat()) == (
            "research_only", "primary", "2026-09-25",
        )
        assert [(item["classification"], item["reason"]) for item in compensation["items"]] == [
            ("execution_not_authorized", "research_only"),
        ]
        with db.get_session() as session:
            assert session.query(DecisionSignalRecord).count() == 0
            assert session.query(PaperExecutionPlan).count() == 0
            assert session.query(ReportPlanReconciliation).one().status == "terminal"
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        if old_database_path is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = old_database_path


def test_market_opportunity_research_job_reaches_pipeline_without_account_context(tmp_path, monkeypatch):
    from datetime import date

    from src.config import Config
    from src.repositories.market_opportunity_repository import MarketOpportunityRepository
    from src.services.analysis_service import AnalysisService
    from src.services.market_opportunity_service import MarketOpportunityService
    from src.storage import AnalysisHistory, DatabaseManager

    class RecordingPipeline:
        calls = []

        def __init__(self, **kwargs):
            RecordingPipeline.calls.append(kwargs)

        def process_single_stock(self, **kwargs):
            return None

    day = date(2026, 9, 7)
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "research-job.db"))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'research-job.db'}")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    try:
        db = DatabaseManager.get_instance()
        repo = MarketOpportunityRepository(db)
        with db.session_scope() as session:
            review = AnalysisHistory(code="MARKET", name="A股大盘", report_type="market_review", query_id="market-day")
            session.add(review)
            session.flush()
            review_id = int(review.id)
        run = repo.create_or_get_run({
            "run_key": f"market-opportunity:{review_id}:paper:opportunity-policy-v4",
            "market": "cn", "trade_date": day, "source_report_id": review_id,
            "market_regime": "risk_off", "mode": "paper", "status": "analyzing", "llm_budget": 4,
        })
        repo.replace_run_children(run_id=run.id, themes=[], candidates=[{
            "candidate_key": "cn:sh:600519", "market": "cn", "exchange": "sh", "stock_code": "600519",
            "symbol": "600519.SH", "stock_name": "贵州茅台", "status": "selected", "rank": 1, "score": 90.0,
            "as_of": day, "valid_until": day, "snapshot_hash": "producer",
        }], jobs=[{
            "candidate_key": "cn:sh:600519", "job_key": "market-opportunity-research:initial:producer",
            "model": "model-a", "evidence": {"source": "market_opportunity_research"},
        }])
        service = MarketOpportunityService(
            config=SimpleNamespace(
                market_opportunity_mode="paper", market_opportunity_max_llm_calls_per_run=4,
                market_opportunity_observe_gate_days=0, paper_trading_enabled=True,
            ),
            db_manager=db,
            repository=repo,
            analysis_service_factory=AnalysisService,
        )
        service._observe_gate_status = lambda **_: {"completed": True}

        with patch("src.config.get_config", return_value=SimpleNamespace()), patch(
            "src.services.analysis_model_service.build_model_scoped_config", side_effect=lambda config, model: config,
        ), patch("src.core.pipeline.StockAnalysisPipeline", RecordingPipeline):
            outcome = service.process_pending_jobs(owner="producer", limit=1, run_id=run.id, effective_session=day)

        # 真实分析服务把研究用途交给流水线，且不带任何账户仓位上下文。
        assert len(RecordingPipeline.calls) == 1
        kwargs = RecordingPipeline.calls[0]
        assert kwargs["portfolio_context"] is None
        assert kwargs["query_source"] == "market_opportunity"
        assert kwargs["execution_intent"]["execution_intent"] == "research_only"
        assert kwargs["execution_intent"]["intent_source"] == "market_opportunity_research"
        assert kwargs["selection_context"]["source"] == "market_opportunity"
        job = repo.get_run_detail(run.id)["analysis_jobs"][0]
        assert outcome["failed"] == 1
        assert (job["status"], job["failure_reason"]) == ("failed_retryable", "analysis_report_missing_after_sync_call")
        assert repo.research_budget_usage(market="cn", session_date=day, limit=4)["charged"] == 1
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
