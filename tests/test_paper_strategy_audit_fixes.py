# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import select

from src.config import Config
from src.services.paper_trading_service import (
    PaperTradingService,
    TRADING_STYLE_POLICY_VERSION,
    TRADING_STYLE_SETTINGS,
)
from src.storage import (
    AnalysisHistory,
    DatabaseManager,
    DecisionSignalRecord,
    PaperExecutionPlan,
    PaperPosition,
    PaperRiskRevision,
    PaperStrategyThesis,
    PaperSyncLink,
    PaperTradeEvent,
    PortfolioAccount,
    PortfolioPosition,
    StockDaily,
)
from tests.report_intent_helpers import add_report_execution_intent


def _isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    os.environ["DATABASE_PATH"] = str(tmp_path / "strategy-audit.db")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager.get_instance()
    return db, old_database_path


def _restore_db(old_database_path):
    DatabaseManager.reset_instance()
    Config.reset_instance()
    if old_database_path is None:
        os.environ.pop("DATABASE_PATH", None)
    else:
        os.environ["DATABASE_PATH"] = old_database_path


def _style_metadata(style: str, *, atr20=None, support=None):
    settings = dict(TRADING_STYLE_SETTINGS[style])
    indicators = {
        "trading_style": style,
        "analysis_window_sessions": settings["analysis_window_sessions"],
        "bars_observed": settings["analysis_window_sessions"],
        "history_sufficient": True,
    }
    indicators["moving_averages"] = {"ma20": support} if support is not None else {}
    indicators["volatility"] = {"atr20": atr20} if atr20 is not None else {}
    if support is not None:
        indicators["support"] = support
        indicators["support_basis"] = "nearest_frozen_style_ma_at_or_below_close"
    style_context = {
        "trading_style": style,
        "source": "unit_frozen_report",
        "settings": settings,
    }
    return {
        "holding_state": "empty",
        "base_action": "buy",
        "effective_action_source": "position_actions.empty",
        "paper_trading_style_context": style_context,
        "paper_trading_style_indicators": indicators,
        "paper_trading_style_evidence": {
            "source": "analysis_context_snapshot",
            "style_context": style_context,
            "indicator_snapshot": indicators,
            "bars_observed": settings["analysis_window_sessions"],
            "analysis_window_sessions": settings["analysis_window_sessions"],
            "history_sufficient": True,
        },
    }


def _report_and_signal(db, *, style="position", horizon="10d", raw_report=None, atr20=None, support=None):
    with db.session_scope() as session:
        report = AnalysisHistory(
            query_id=f"strategy-{style}-{horizon}",
            code="600519",
            name="贵州茅台",
            report_type="full",
            model_used="model-a",
            operation_advice="买入",
            raw_result=json.dumps(raw_report or {"paper_trading": {"setup_type": "pullback"}}, ensure_ascii=False),
            ideal_buy=100.0,
            secondary_buy=96.0,
            stop_loss=90.0,
            take_profit=125.0,
            created_at=datetime(2026, 8, 3, 15, 30),
        )
        session.add(report)
        session.flush()
        add_report_execution_intent(session, report.id)
        signal = DecisionSignalRecord(
            stock_code="600519",
            stock_name="贵州茅台",
            market="cn",
            source_type="analysis",
            source_report_id=report.id,
            source_agent="model-a",
            trace_id=f"trace-{report.id}",
            trigger_source="system",
            action="buy",
            horizon=horizon,
            entry_low=96.0,
            entry_high=100.0,
            stop_loss=90.0,
            target_price=125.0,
            confidence=0.8,
            score=65,
            plan_quality="partial",
            status="active",
            metadata_json=json.dumps(_style_metadata(style, atr20=atr20, support=support), ensure_ascii=False),
            created_at=datetime(2026, 8, 3, 15, 30),
        )
        session.add(signal)
        session.flush()
        return report.id, signal.id


def _policy_plan(setup_type: str, *, support=99.0):
    settings = dict(TRADING_STYLE_SETTINGS["position"])
    return SimpleNamespace(
        id=1,
        decision_signal_id=None,
        action="buy",
        stop_loss=90.0,
        target_price=125.0,
        trading_style="position",
        trading_style_settings_json=json.dumps({"settings": settings}),
        rules_json=json.dumps([{"type": "close_between", "low": 96.0, "high": 100.0}]),
        descriptive_conditions_json=json.dumps([
            {
                "source": "paper_trading.strategy_setup",
                "setup_type": setup_type,
                "factor_policy": {
                    **(
                        {"required_domains": ["momentum", "volume", "breakout"], "optional_domains": ["trend"]}
                        if setup_type == "breakout"
                        else {}
                    ),
                    **(
                        {
                            "required_domains": ["trend"],
                            "alternative_confirmations": [["support_confirmation", "shrinking_volume"]],
                            "optional_domains": ["momentum"],
                        }
                        if setup_type == "pullback"
                        else {}
                    ),
                    **(
                        {"required_domains": ["trend", "momentum"], "optional_domains": ["volume", "breakout"]}
                        if setup_type == "trend_continuation"
                        else {}
                    ),
                },
                "geometry_guard": {"support": support},
            }
        ]),
    )


def _trend_bars(*, high=99.0, last_close=99.0):
    bars = []
    for idx in range(60):
        close = last_close - (59 - idx) * 0.05
        bars.append({
            "date": date(2026, 5, 1) + timedelta(days=idx),
            "open": close - 0.1,
            "high": high,
            "low": close - 0.5,
            "close": close,
            "volume": 1000.0,
            "is_complete": True,
        })
    return bars


def _trend_with_negative_momentum_bars():
    bars = []
    for idx in range(50):
        close = 95.0 + idx * 0.05
        bars.append({
            "date": date(2026, 5, 1) + timedelta(days=idx),
            "open": close - 0.1,
            "high": 102.0,
            "low": close - 0.5,
            "close": close,
            "volume": 1000.0,
            "is_complete": True,
        })
    for offset, close in enumerate([98.5, 98.8, 99.0, 99.2, 99.4, 99.6, 99.8, 100.0, 100.2, 101.0]):
        bars.append({
            "date": date(2026, 6, 20) + timedelta(days=offset),
            "open": close - 0.1,
            "high": 102.0,
            "low": close - 0.5,
            "close": close,
            "volume": 1000.0,
            "is_complete": True,
        })
    return bars


def test_setup_policy_blocks_breakout_without_structured_breakout_in_eod_and_intraday():
    service = PaperTradingService(repo=None)
    plan = _policy_plan("breakout")
    prior = _trend_bars(high=101.0, last_close=99.0)
    current = {
        "date": date(2026, 8, 4),
        "open": 99.5,
        "high": 100.5,
        "low": 99.0,
        "close": 100.0,
        "volume": 2000.0,
        "is_complete": True,
    }
    eod = service._entry_policy(plan, current, prior, mode="eod")
    quote = SimpleNamespace(
        change_pct=1.0,
        pre_close=99.0,
        volume_ratio=2.0,
        high=100.5,
        low=99.0,
        open_price=99.5,
        volume=2000.0,
    )
    intraday_factors = service._intraday_factor_domains(plan, 100.0, quote, prior)
    intraday = service._setup_confirmation_policy(plan, intraday_factors, mode="intraday")

    assert eod["passed"] is False
    assert "setup_required_domain_not_passed:breakout" in eod["reasons"]
    assert intraday["passed"] is False
    assert "setup_required_domain_not_passed:breakout" in intraday["reasons"]


def test_setup_policy_blocks_pullback_without_support_confirmation_in_eod_and_intraday():
    service = PaperTradingService(repo=None)
    plan = _policy_plan("pullback", support=95.0)
    prior = _trend_bars(high=101.0, last_close=99.0)
    current = {
        "date": date(2026, 8, 4),
        "open": 99.5,
        "high": 100.5,
        "low": 99.0,
        "close": 100.0,
        "volume": 800.0,
        "is_complete": True,
    }
    eod = service._entry_policy(plan, current, prior, mode="eod")
    quote = SimpleNamespace(
        change_pct=1.0,
        pre_close=99.0,
        volume_ratio=0.8,
        high=100.5,
        low=99.0,
        open_price=99.5,
        volume=800.0,
    )
    intraday_factors = service._intraday_factor_domains(plan, 100.0, quote, prior)
    intraday = service._setup_confirmation_policy(plan, intraday_factors, mode="intraday")

    reason = "setup_alternative_confirmation_not_passed:support_confirmation+shrinking_volume"
    assert eod["passed"] is False
    assert reason in eod["reasons"]
    assert intraday["passed"] is False
    assert reason in intraday["reasons"]


def test_setup_policy_blocks_trend_continuation_without_momentum_in_eod_and_intraday():
    service = PaperTradingService(repo=None)
    plan = _policy_plan("trend_continuation")
    prior = _trend_with_negative_momentum_bars()
    current = {
        "date": date(2026, 8, 4),
        "open": 100.3,
        "high": 100.8,
        "low": 100.0,
        "close": 100.5,
        "volume": 2000.0,
        "is_complete": True,
    }
    eod = service._entry_policy(plan, current, prior, mode="eod")
    quote = SimpleNamespace(
        change_pct=-0.5,
        pre_close=101.0,
        volume_ratio=2.0,
        high=100.8,
        low=100.0,
        open_price=100.3,
        volume=2000.0,
    )
    intraday_factors = service._intraday_factor_domains(plan, 100.5, quote, prior)
    intraday = service._setup_confirmation_policy(plan, intraday_factors, mode="intraday")

    assert eod["passed"] is False
    assert "setup_required_domain_not_passed:momentum" in eod["reasons"]
    assert intraday["passed"] is False
    assert "setup_required_domain_not_passed:momentum" in intraday["reasons"]


def test_setup_policy_respects_configured_minimum_factor_domains():
    service = PaperTradingService(repo=None, intraday_min_factor_domains=3)
    plan = _policy_plan("trend_continuation")
    factors = [
        {"domain": "trend", "available": True, "passed": True, "weight": 0.25},
        {"domain": "momentum", "available": True, "passed": True, "weight": 0.30},
    ]
    result = service._setup_confirmation_policy(plan, factors, mode="intraday")

    assert result["passed"] is False
    assert "insufficient_available_factor_domains" in result["reasons"]
    assert "insufficient_passing_factor_domains" in result["reasons"]


def test_eod_entry_policy_uses_frozen_trend_windows(tmp_path):
    db, old = _isolated_db(tmp_path)
    try:
        service = PaperTradingService(repo=None)
        service.db = db
        service.repo.db = db
        report_id, signal_id = _report_and_signal(db, style="trend", horizon="long")
        result = service.create_plan_for_report(report_id, decision_signal_id=signal_id)
        plan_id = result["item"]["id"]
        with db.get_session() as session:
            plan = session.get(PaperExecutionPlan, plan_id)
            current = {"date": date(2026, 8, 4), "open": 100, "high": 106, "low": 99, "close": 105, "volume": 2000, "is_complete": True}
            prior = [
                {"date": date(2026, 5, 1) + timedelta(days=i), "open": 90, "high": 100 + i * 0.1, "low": 89, "close": 95 + i * 0.1, "volume": 1000, "is_complete": True}
                for i in range(59)
            ]
            policy = service._entry_policy(plan, current, prior, mode="eod")
        assert any(
            item["domain"] == "trend" and item["available"] is False
            for item in policy["factor_domains"]
        )
    finally:
        _restore_db(old)


def test_plan_creation_rejects_stop_inside_frozen_atr_noise(tmp_path):
    db, old = _isolated_db(tmp_path)
    try:
        service = PaperTradingService(repo=None)
        service.db = db
        service.repo.db = db
        report_id, signal_id = _report_and_signal(db, style="position", horizon="10d", atr20=5.0, support=97.0)
        with db.session_scope() as session:
            signal = session.get(DecisionSignalRecord, signal_id)
            signal.stop_loss = 97.0
        result = service.create_plan_for_report(report_id, decision_signal_id=signal_id)
        item = result["item"]
        assert item["executable"] is False
        reasons = {
            condition.get("text")
            for condition in item["descriptive_conditions"]
            if condition.get("source") == "plan_validator"
        }
        assert any("stop_within_atr_noise" in str(reason) for reason in reasons)
    finally:
        _restore_db(old)


def test_portfolio_import_repairs_existing_strategy_style_without_reopening(tmp_path):
    db, old = _isolated_db(tmp_path)
    try:
        service = PaperTradingService(
            repo=None,
            trading_style="position",
            trading_style_overrides={"cn:600519": "trend"},
        )
        service.db = db
        service.repo.db = db
        with db.session_scope() as session:
            account = PortfolioAccount(name="paper", market="cn", base_currency="CNY")
            session.add(account)
            session.flush()
            position = PortfolioPosition(
                account_id=account.id,
                symbol="600519",
                market="cn",
                currency="CNY",
                quantity=100.0,
                avg_cost=100.0,
                total_cost=10000.0,
                last_price=100.0,
                market_value_base=10000.0,
                valuation_currency="CNY",
            )
            session.add(position)
            import_report = AnalysisHistory(
                query_id="import-report",
                code="600519",
                name="贵州茅台",
                report_type="full",
                model_used="model-a",
                raw_result="{}",
                stop_loss=90.0,
                take_profit=120.0,
                created_at=datetime(2026, 8, 3, 15, 0),
            )
            session.add(import_report)
            session.flush()
            add_report_execution_intent(session, import_report.id)
            portfolio_position_id = position.id
        with patch("src.services.paper_trading_service.validate_analysis_models", return_value=["model-a"]):
            first = service.sync_portfolio_position_to_paper(
                portfolio_position_id=portfolio_position_id,
                source_models=["model-a"],
                trade_date=date(2026, 8, 1),
                idempotency_key="import-style-repair",
            )
        link = first["items"][0]
        with db.session_scope() as session:
            plan = session.get(PaperExecutionPlan, link["paper_plan_id"])
            paper_position = session.get(PaperPosition, link["paper_position_id"])
            plan.trading_style = "position"
            paper_position.trading_style = "position"
            plan.max_holding_sessions = 10
            paper_position.max_holding_sessions = 10
            original_opened_at = paper_position.opened_at
        with patch("src.services.paper_trading_service.validate_analysis_models", return_value=["model-a"]):
            second = service.sync_portfolio_position_to_paper(
                portfolio_position_id=portfolio_position_id,
                source_models=["model-a"],
                trade_date=date(2026, 8, 1),
                idempotency_key="import-style-repair",
            )
        assert second["created"] is False
        with db.get_session() as session:
            positions = session.execute(select(PaperPosition)).scalars().all()
            plans = session.execute(select(PaperExecutionPlan)).scalars().all()
            paper_position = session.get(PaperPosition, link["paper_position_id"])
            plan = session.get(PaperExecutionPlan, link["paper_plan_id"])
            assert len(positions) == 1
            assert len(plans) == 1
            assert paper_position.opened_on == date(2026, 8, 1)
            assert paper_position.opened_at == original_opened_at
            assert paper_position.trading_style == "trend"
            assert plan.trading_style == "trend"
            assert paper_position.max_holding_sessions == TRADING_STYLE_SETTINGS["trend"]["max_holding_sessions"]
    finally:
        _restore_db(old)


def test_portfolio_import_repair_does_not_bump_unchanged_position_metadata(tmp_path):
    db, old = _isolated_db(tmp_path)
    try:
        service = PaperTradingService(
            repo=None,
            trading_style="position",
            trading_style_overrides={"cn:600519": "trend"},
        )
        service.db = db
        service.repo.db = db
        now = datetime(2026, 8, 5, 10, 0)
        old_position_updated_at = datetime(2026, 8, 1, 10, 0)
        with db.session_scope() as session:
            plan = PaperExecutionPlan(
                plan_key="repair-plan",
                plan_version="paper-hybrid-v3",
                source_report_id=1,
                source_model="model-a",
                market="cn",
                stock_code="600519",
                report_date=date(2026, 8, 1),
                report_created_at=datetime(2026, 8, 1, 9, 0),
                action="buy",
                executable=True,
                status="triggered",
                rules_json="[]",
                max_holding_sessions=10,
                trading_style="position",
                updated_at=datetime(2026, 8, 1, 9, 0),
            )
            session.add(plan)
            session.flush()
            position = PaperPosition(
                plan_id=plan.id,
                source_report_id=1,
                source_model="model-a",
                market="cn",
                stock_code="600519",
                status="open",
                opened_on=date(2026, 8, 1),
                opened_price=100.0,
                normalized_notional=10000.0,
                quantity=100.0,
                max_holding_sessions=TRADING_STYLE_SETTINGS["trend"]["max_holding_sessions"],
                trading_style="trend",
                trading_style_policy_version=TRADING_STYLE_POLICY_VERSION,
                trading_style_settings_json=json.dumps(
                    service._style_evidence_for_identity(
                        market="cn",
                        stock_code="600519",
                        source="portfolio_import_identity_resolution",
                    ),
                    ensure_ascii=False,
                    sort_keys=True,
                    allow_nan=False,
                ),
                updated_at=old_position_updated_at,
            )
            session.add(position)
            session.flush()
            changed = service._repair_portfolio_import_strategy_fields_in_session(
                session,
                plan=plan,
                position=position,
                market="cn",
                stock_code="600519",
                now=now,
            )
        assert changed is True
        with db.get_session() as session:
            persisted_plan = session.execute(select(PaperExecutionPlan)).scalar_one()
            persisted_position = session.execute(select(PaperPosition)).scalar_one()
            assert persisted_plan.trading_style == "trend"
            assert persisted_plan.updated_at == now
            assert persisted_position.trading_style == "trend"
            assert persisted_position.updated_at == old_position_updated_at
    finally:
        _restore_db(old)


def test_portfolio_import_idempotent_retry_preserves_existing_frozen_style_after_config_change(tmp_path):
    db, old = _isolated_db(tmp_path)
    try:
        first_service = PaperTradingService(
            repo=None,
            trading_style="position",
            trading_style_overrides={"cn:600519": "trend"},
        )
        first_service.db = db
        first_service.repo.db = db
        with db.session_scope() as session:
            account = PortfolioAccount(name="paper", market="cn", base_currency="CNY")
            session.add(account)
            session.flush()
            position = PortfolioPosition(
                account_id=account.id,
                symbol="600519",
                market="cn",
                currency="CNY",
                quantity=100.0,
                avg_cost=100.0,
                total_cost=10000.0,
                last_price=100.0,
                market_value_base=10000.0,
                valuation_currency="CNY",
            )
            session.add(position)
            import_report = AnalysisHistory(
                query_id="import-preserve-report",
                code="600519",
                name="贵州茅台",
                report_type="full",
                model_used="model-a",
                raw_result="{}",
                stop_loss=90.0,
                take_profit=120.0,
                created_at=datetime(2026, 8, 3, 15, 0),
            )
            session.add(import_report)
            session.flush()
            add_report_execution_intent(session, import_report.id)
            portfolio_position_id = position.id
        with patch("src.services.paper_trading_service.validate_analysis_models", return_value=["model-a"]):
            first = first_service.sync_portfolio_position_to_paper(
                portfolio_position_id=portfolio_position_id,
                source_models=["model-a"],
                trade_date=date(2026, 8, 1),
                idempotency_key="import-preserve-style",
            )
        second_service = PaperTradingService(
            repo=None,
            trading_style="position",
            trading_style_overrides={"cn:600519": "short_swing"},
        )
        second_service.db = db
        second_service.repo.db = db
        with patch("src.services.paper_trading_service.validate_analysis_models", return_value=["model-a"]):
            second = second_service.sync_portfolio_position_to_paper(
                portfolio_position_id=portfolio_position_id,
                source_models=["model-a"],
                trade_date=date(2026, 8, 1),
                idempotency_key="import-preserve-style",
            )
        assert first["created"] is True
        assert second["created"] is False
        with db.get_session() as session:
            plans = session.execute(select(PaperExecutionPlan)).scalars().all()
            positions = session.execute(select(PaperPosition)).scalars().all()
            assert len(plans) == 1
            assert len(positions) == 1
            assert plans[0].trading_style == "trend"
            assert positions[0].trading_style == "trend"
            assert plans[0].max_holding_sessions == TRADING_STYLE_SETTINGS["trend"]["max_holding_sessions"]
            assert positions[0].max_holding_sessions == TRADING_STYLE_SETTINGS["trend"]["max_holding_sessions"]
    finally:
        _restore_db(old)


def test_portfolio_import_batch_repair_preserves_frozen_style_and_ledger(tmp_path):
    db, old = _isolated_db(tmp_path)
    try:
        first_service = PaperTradingService(
            repo=None,
            trading_style="position",
            trading_style_overrides={"cn:600519": "trend"},
        )
        first_service.db = db
        first_service.repo.db = db
        with db.session_scope() as session:
            account = PortfolioAccount(name="paper", market="cn", base_currency="CNY")
            session.add(account)
            session.flush()
            portfolio_position = PortfolioPosition(
                account_id=account.id,
                symbol="600519",
                market="cn",
                currency="CNY",
                quantity=100.0,
                avg_cost=100.0,
                total_cost=10000.0,
                last_price=100.0,
                market_value_base=10000.0,
                valuation_currency="CNY",
            )
            session.add(portfolio_position)
            import_report = AnalysisHistory(
                query_id="import-batch-repair-report",
                code="600519",
                name="贵州茅台",
                report_type="full",
                model_used="model-a",
                raw_result="{}",
                stop_loss=90.0,
                take_profit=120.0,
                created_at=datetime(2026, 8, 3, 15, 0),
            )
            session.add(import_report)
            session.flush()
            add_report_execution_intent(session, import_report.id)
            portfolio_position_id = portfolio_position.id
        with patch("src.services.paper_trading_service.validate_analysis_models", return_value=["model-a"]):
            imported = first_service.sync_portfolio_position_to_paper(
                portfolio_position_id=portfolio_position_id,
                source_models=["model-a"],
                trade_date=date(2026, 8, 1),
                idempotency_key="import-batch-preserve-style",
            )
        link = imported["items"][0]
        with db.session_scope() as session:
            plan = session.get(PaperExecutionPlan, link["paper_plan_id"])
            paper_position = session.get(PaperPosition, link["paper_position_id"])
            event_count = session.execute(select(PaperTradeEvent)).scalars().all()
            link_count = session.execute(select(PaperSyncLink)).scalars().all()
            assert len(event_count) == 1
            assert len(link_count) == 1
            original_opened_on = paper_position.opened_on
            original_opened_at = paper_position.opened_at
            original_quantity = paper_position.quantity
            original_opened_price = paper_position.opened_price
            plan.trading_style = None
            plan.trading_style_policy_version = None
            plan.trading_style_settings_json = None
            plan.max_holding_sessions = 5
        second_service = PaperTradingService(
            repo=None,
            trading_style="position",
            trading_style_overrides={"cn:600519": "short_swing"},
        )
        second_service.db = db
        second_service.repo.db = db

        result = second_service.repair_portfolio_import_strategy_fields(
            now=datetime(2026, 8, 6, 10, 0),
        )

        assert result["scanned"] == 1
        assert result["repaired"] == 1
        with db.get_session() as session:
            plans = session.execute(select(PaperExecutionPlan)).scalars().all()
            positions = session.execute(select(PaperPosition)).scalars().all()
            events = session.execute(select(PaperTradeEvent)).scalars().all()
            links = session.execute(select(PaperSyncLink)).scalars().all()
            assert len(plans) == 1
            assert len(positions) == 1
            assert len(events) == 1
            assert len(links) == 1
            plan = plans[0]
            paper_position = positions[0]
            assert plan.trading_style == "trend"
            assert paper_position.trading_style == "trend"
            assert plan.max_holding_sessions == TRADING_STYLE_SETTINGS["trend"]["max_holding_sessions"]
            assert paper_position.max_holding_sessions == TRADING_STYLE_SETTINGS["trend"]["max_holding_sessions"]
            assert paper_position.opened_on == original_opened_on
            assert paper_position.opened_at == original_opened_at
            assert paper_position.quantity == original_quantity
            assert paper_position.opened_price == original_opened_price
    finally:
        _restore_db(old)


def test_risk_revision_rejects_target_below_validated_asof_price(tmp_path):
    db, old = _isolated_db(tmp_path)
    try:
        service = PaperTradingService(repo=None)
        service.db = db
        service.repo.db = db
        with db.session_scope() as session:
            report = AnalysisHistory(
                query_id="risk-report",
                code="600519",
                name="贵州茅台",
                report_type="full",
                model_used="model-a",
                raw_result="{}",
                created_at=datetime(2026, 8, 4, 15, 30),
            )
            session.add(report)
            session.add(StockDaily(code="600519.SH", date=date(2026, 8, 4), open=99, high=101, low=98, close=100, volume=1000))
            session.flush()
            add_report_execution_intent(session, report.id)
            signal = DecisionSignalRecord(
                stock_code="600519",
                stock_name="贵州茅台",
                market="cn",
                source_type="analysis",
                source_report_id=report.id,
                source_agent="model-a",
                trigger_source="system",
                action="hold",
                horizon="10d",
                confidence=0.8,
                score=50,
                plan_quality="partial",
                status="active",
                metadata_json=json.dumps({
                    "holding_state": "holding",
                    "base_action": "hold",
                    "effective_action_source": "position_actions.holding",
                    "position_actions": {
                        "holding": {"action": "hold", "stop_loss": 95.0, "target_price": 99.0}
                    },
                }, ensure_ascii=False),
                created_at=datetime(2026, 8, 4, 15, 30),
            )
            session.add(signal)
            session.flush()
            plan = PaperExecutionPlan(
                plan_key="risk-plan",
                plan_version="paper-hybrid-v3",
                source_report_id=report.id,
                decision_signal_id=signal.id,
                source_model="model-a",
                market="cn",
                stock_code="600519",
                report_date=date(2026, 8, 3),
                report_created_at=datetime(2026, 8, 3, 15, 0),
                valid_for_date=date(2026, 8, 4),
                action="buy",
                executable=True,
                status="triggered",
                rules_json="[]",
                max_holding_sessions=20,
                trading_style="position",
            )
            session.add(plan)
            session.flush()
            position = PaperPosition(
                plan_id=plan.id,
                source_report_id=report.id,
                decision_signal_id=signal.id,
                source_model="model-a",
                market="cn",
                stock_code="600519",
                status="open",
                opened_on=date(2026, 8, 3),
                opened_at=datetime(2026, 8, 3, 15, 0),
                opened_price=100.0,
                normalized_notional=10000.0,
                quantity=100.0,
                stop_loss=90.0,
                target_price=120.0,
                max_holding_sessions=20,
            )
            session.add(position)
            session.flush()
            eligibility = service._holding_signal_eligibility(
                signal,
                report=report,
                evaluated_at=datetime(2026, 8, 4, 15, 30),
            )
            revisions = service._apply_signal_risk_revisions(
                session,
                positions=[position],
                signal=signal,
                eligibility=eligibility,
                evaluated_at=datetime(2026, 8, 4, 15, 30),
                current_price=None,
            )
        assert revisions[0]["result"] == "rejected"
        assert revisions[0]["reason"] == "target_below_or_at_current_price_rejected"
        with db.get_session() as session:
            revision = session.execute(select(PaperRiskRevision)).scalar_one()
            persisted_position = session.execute(select(PaperPosition)).scalar_one()
            assert revision.reason == "target_below_or_at_current_price_rejected"
            assert persisted_position.target_price == 120.0
    finally:
        _restore_db(old)


def test_risk_revision_consumes_frozen_pipeline_atr_before_tightening_stop(tmp_path):
    db, old = _isolated_db(tmp_path)
    try:
        service = PaperTradingService(repo=None)
        service.db = db
        service.repo.db = db
        with db.session_scope() as session:
            report = AnalysisHistory(
                query_id="risk-atr-report",
                code="600519",
                name="贵州茅台",
                report_type="full",
                model_used="model-a",
                raw_result="{}",
                created_at=datetime(2026, 8, 4, 15, 30),
            )
            session.add(report)
            session.add(StockDaily(code="600519.SH", date=date(2026, 8, 4), open=99, high=101, low=98, close=100, volume=1000))
            session.flush()
            add_report_execution_intent(session, report.id)
            signal = DecisionSignalRecord(
                stock_code="600519",
                stock_name="贵州茅台",
                market="cn",
                source_type="analysis",
                source_report_id=report.id,
                source_agent="model-a",
                trigger_source="system",
                action="hold",
                horizon="10d",
                confidence=0.8,
                score=50,
                plan_quality="partial",
                status="active",
                metadata_json=json.dumps({
                    **_style_metadata("position", atr20=5.0, support=99.0),
                    "holding_state": "holding",
                    "base_action": "hold",
                    "effective_action_source": "position_actions.holding",
                    "position_actions": {
                        "holding": {"action": "hold", "stop_loss": 98.0, "target_price": 130.0}
                    },
                }, ensure_ascii=False),
                created_at=datetime(2026, 8, 4, 15, 30),
            )
            session.add(signal)
            session.flush()
            plan = PaperExecutionPlan(
                plan_key="risk-atr-plan",
                plan_version="paper-hybrid-v3",
                source_report_id=report.id,
                decision_signal_id=signal.id,
                source_model="model-a",
                market="cn",
                stock_code="600519",
                report_date=date(2026, 8, 3),
                report_created_at=datetime(2026, 8, 3, 15, 0),
                valid_for_date=date(2026, 8, 4),
                action="buy",
                executable=True,
                status="triggered",
                rules_json="[]",
                max_holding_sessions=20,
                trading_style="position",
            )
            session.add(plan)
            session.flush()
            position = PaperPosition(
                plan_id=plan.id,
                source_report_id=report.id,
                decision_signal_id=signal.id,
                source_model="model-a",
                market="cn",
                stock_code="600519",
                status="open",
                opened_on=date(2026, 8, 3),
                opened_at=datetime(2026, 8, 3, 15, 0),
                opened_price=100.0,
                normalized_notional=10000.0,
                quantity=100.0,
                stop_loss=90.0,
                target_price=130.0,
                max_holding_sessions=20,
                trading_style="position",
            )
            session.add(position)
            session.flush()
            eligibility = service._holding_signal_eligibility(
                signal,
                report=report,
                evaluated_at=datetime(2026, 8, 4, 15, 30),
            )
            revisions = service._apply_signal_risk_revisions(
                session,
                positions=[position],
                signal=signal,
                eligibility=eligibility,
                evaluated_at=datetime(2026, 8, 4, 15, 30),
                current_price=None,
            )
        assert revisions[0]["result"] == "rejected"
        assert revisions[0]["reason"].startswith("risk_revision_geometry_guard_rejected")
        assert "stop_within_atr_noise" in revisions[0]["reason"]
        with db.get_session() as session:
            persisted_position = session.execute(select(PaperPosition)).scalar_one()
            revision = session.execute(select(PaperRiskRevision)).scalar_one()
            evidence = json.loads(revision.evidence_json)
            assert evidence["risk_revision_geometry_guard"]["atr"] == 5.0
            assert persisted_position.stop_loss == 90.0
    finally:
        _restore_db(old)


def test_service_risk_revision_without_validated_price_is_retryable_and_non_mutating(tmp_path):
    db, old = _isolated_db(tmp_path)
    try:
        service = PaperTradingService(repo=None)
        service.db = db
        service.repo.db = db
        with db.session_scope() as session:
            report = AnalysisHistory(
                query_id="risk-no-price-report",
                code="600519",
                name="贵州茅台",
                report_type="full",
                model_used="model-a",
                raw_result="{}",
                created_at=datetime(2026, 8, 4, 10, 0),
            )
            session.add(report)
            session.flush()
            add_report_execution_intent(session, report.id)
            signal = DecisionSignalRecord(
                stock_code="600519",
                stock_name="贵州茅台",
                market="cn",
                source_type="analysis",
                source_report_id=report.id,
                source_agent="model-a",
                trigger_source="system",
                action="hold",
                horizon="10d",
                confidence=0.8,
                score=50,
                plan_quality="partial",
                status="active",
                metadata_json=json.dumps({
                    "holding_state": "holding",
                    "base_action": "hold",
                    "effective_action_source": "position_actions.holding",
                    "position_actions": {
                        "holding": {"action": "hold", "stop_loss": 95.0, "target_price": 120.0}
                    },
                }, ensure_ascii=False),
                created_at=datetime(2026, 8, 4, 10, 0),
            )
            session.add(signal)
            session.flush()
            plan = PaperExecutionPlan(
                plan_key="risk-no-price-plan",
                plan_version="paper-hybrid-v3",
                source_report_id=report.id,
                decision_signal_id=signal.id,
                source_model="model-a",
                market="cn",
                stock_code="600519",
                report_date=date(2026, 8, 3),
                report_created_at=datetime(2026, 8, 3, 15, 0),
                valid_for_date=date(2026, 8, 4),
                action="buy",
                executable=True,
                status="triggered",
                rules_json="[]",
                max_holding_sessions=20,
                trading_style="position",
            )
            session.add(plan)
            session.flush()
            position = PaperPosition(
                plan_id=plan.id,
                source_report_id=report.id,
                decision_signal_id=signal.id,
                source_model="model-a",
                market="cn",
                stock_code="600519",
                status="open",
                opened_on=date(2026, 8, 3),
                opened_at=datetime(2026, 8, 3, 15, 0),
                opened_price=100.0,
                normalized_notional=10000.0,
                quantity=100.0,
                stop_loss=90.0,
                target_price=130.0,
                max_holding_sessions=20,
            )
            session.add(position)
            session.flush()
            eligibility = service._holding_signal_eligibility(
                signal,
                report=report,
                evaluated_at=datetime(2026, 8, 4, 10, 0),
            )
            revisions = service._apply_signal_risk_revisions(
                session,
                positions=[position],
                signal=signal,
                eligibility=eligibility,
                evaluated_at=datetime(2026, 8, 4, 10, 0),
                current_price=None,
            )
        assert len(revisions) == 1
        revision = revisions[0]
        assert revision["id"] is None
        assert revision["result"] == "deferred"
        assert revision["retryable"] is True
        assert revision["old_stop_loss"] == 90.0
        assert revision["applied_stop_loss"] == 90.0
        assert revision["old_target_price"] == 130.0
        assert revision["applied_target_price"] == 130.0
        assert revision["reference_price"]["price"] is None
        assert revision["reference_price"]["validated_price"] is False
        with db.get_session() as session:
            assert session.execute(select(PaperRiskRevision)).scalars().all() == []
            persisted_position = session.execute(select(PaperPosition)).scalar_one()
            assert persisted_position.stop_loss == 90.0
            assert persisted_position.target_price == 130.0
    finally:
        _restore_db(old)
