# -*- coding: utf-8 -*-
"""Focused asset exposure policy checks for paper trading."""

from __future__ import annotations

import json
import os
from datetime import date, datetime

import pytest
from sqlalchemy import select

from src.config import (
    Config,
    parse_paper_trading_asset_class_overrides,
    parse_paper_trading_asset_exposure_caps,
)
from src.repositories.paper_trading_repo import PaperTradingRepository
from src.services.paper_trading_service import PaperTradingService
from src.storage import (
    AnalysisHistory,
    DatabaseManager,
    DecisionSignalRecord,
    PaperCapitalAccount,
    PaperExecutionPlan,
    PaperPosition,
    PaperTradeEvent,
)
from tests.report_intent_helpers import add_report_execution_intent


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    os.environ["DATABASE_PATH"] = str(tmp_path / "paper-asset-exposure.db")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager.get_instance()
    try:
        yield db
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        if old_database_path is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = old_database_path


def _source_rows(session, *, stock_code: str, model: str = "model-a", raw_result: dict | None = None) -> tuple[int, int]:
    report = AnalysisHistory(
        query_id=f"asset-exposure-{stock_code}",
        code=stock_code,
        name="测试标的",
        report_type="full",
        model_used=model,
        operation_advice="买入",
        raw_result=json.dumps(raw_result or {"dashboard": {}}, ensure_ascii=False),
        ideal_buy=100.0,
        secondary_buy=96.0,
        stop_loss=95.0,
        take_profit=130.0,
        created_at=datetime(2026, 8, 10),
    )
    session.add(report)
    session.flush()
    add_report_execution_intent(session, report.id)
    signal = DecisionSignalRecord(
        stock_code=stock_code,
        stock_name="测试标的",
        market="cn",
        source_type="analysis",
        source_report_id=report.id,
        source_agent=model,
        trace_id=f"asset-exposure-{report.id}",
        trigger_source="system",
        action="buy",
        horizon="5d",
        entry_low=96.0,
        entry_high=100.0,
        stop_loss=95.0,
        target_price=130.0,
        watch_conditions="[]",
        confidence=0.8,
        score=65,
        plan_quality="partial",
        status="active",
        metadata_json="{}",
        created_at=datetime(2026, 8, 10),
    )
    session.add(signal)
    session.flush()
    return report.id, signal.id


def _seed_position(
    db,
    *,
    account_id: int,
    stock_code: str,
    cost: float,
    opened_on: date = date(2026, 8, 10),
    model: str = "model-a",
) -> int:
    with db.session_scope() as session:
        db_account = session.get(PaperCapitalAccount, account_id)
        report_id, signal_id = _source_rows(session, stock_code=stock_code, model=model)
        plan = PaperExecutionPlan(
            plan_key=f"asset-plan-{model}-{stock_code}-{opened_on.isoformat()}",
            plan_version="paper-hybrid-v3",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model=model,
            market="cn",
            stock_code=stock_code,
            stock_name="测试标的",
            report_date=opened_on,
            report_created_at=datetime.combine(opened_on, datetime.min.time()),
            valid_for_date=opened_on,
            action="buy",
            executable=True,
            status="triggered",
            rules_json="[]",
            descriptive_conditions_json="[]",
            stop_loss=95,
            target_price=130,
            max_holding_sessions=10,
        )
        session.add(plan)
        session.flush()
        position = PaperPosition(
            plan_id=plan.id,
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model=model,
            market="cn",
            stock_code=stock_code,
            stock_name="测试标的",
            status="open",
            opened_on=opened_on,
            opened_price=100,
            normalized_notional=cost,
            quantity=cost / 100.0,
            initial_quantity=cost / 100.0,
            capital_account_id=db_account.id,
            currency="CNY",
            allocated_capital=cost,
            initial_allocated_capital=cost,
            remaining_cost=cost,
            realized_quantity=0,
            realized_proceeds=0,
            realized_fees=0,
            realized_pnl=0,
            legacy_fixed_notional=False,
            sellability_rule="cn_t1",
            stop_loss=95,
            target_price=130,
            max_holding_sessions=10,
        )
        session.add(position)
        session.flush()
        db_account.cash = float(db_account.cash or 0.0) - cost
        db_account.invested_capital = float(db_account.invested_capital or 0.0) + cost
        db_account.exposure = float(db_account.exposure or 0.0) + cost
        db_account.open_position_count = int(db_account.open_position_count or 0) + 1
        return int(position.id)


def _account_id(db) -> int:
    with db.session_scope() as session:
        account = PaperCapitalAccount(
            account_key="model-a:cn:CNY",
            source_model="model-a",
            market="cn",
            currency="CNY",
            initial_capital=100000,
            cash=100000,
            invested_capital=0,
            exposure=0,
            open_position_count=0,
        )
        session.add(account)
        session.flush()
        return int(account.id)


def test_individual_stock_uses_stricter_asset_cap_than_legacy_global(isolated_db):
    account_id = _account_id(isolated_db)
    service = PaperTradingService(repo=PaperTradingRepository(db_manager=isolated_db), max_symbol_exposure_pct=50.0)
    with isolated_db.get_session() as session:
        snapshot = service._symbol_exposure_snapshot(
            session,
            session.get(PaperCapitalAccount, account_id),
            source_model="model-a",
            market="cn",
            stock_code="600519",
            projected_additional_cost=21000,
        )
    assert snapshot["asset_type"] == "individual_stock"
    assert snapshot["asset_exposure_cap_pct"] == pytest.approx(20.0)
    assert snapshot["asset_exposure_effective_cap_pct"] == pytest.approx(20.0)
    assert snapshot["symbol_exposure_ceiling"] == pytest.approx(20000.0)
    assert snapshot["within_limit"] is False


def test_asset_exposure_config_json_parses_caps_and_overrides():
    assert parse_paper_trading_asset_exposure_caps(
        '{"individual_stock":18,"themeETF":28,"broadETF":38,"unknown":12}'
    ) == {
        "individual_stock": 18.0,
        "theme_etf": 28.0,
        "broad_etf": 38.0,
        "unknown": 12.0,
    }
    assert parse_paper_trading_asset_class_overrides(
        '{"cn:510300":"broadETF","cn:512480":"theme_etf"}'
    ) == {
        "cn:510300": "broad_etf",
        "cn:512480": "theme_etf",
    }


def test_asset_override_requires_exact_market_identity(isolated_db):
    assert parse_paper_trading_asset_class_overrides('{"512480":"broad_etf"}') == {}
    service = PaperTradingService(repo=PaperTradingRepository(isolated_db), asset_class_overrides={"512480": "broad_etf", "cn:512480": "theme_etf"})
    assert service._asset_override_for_identity("cn", "512480") == "theme_etf"
    assert service._asset_override_for_identity("hk", "512480") is None


def test_theme_etf_explicit_override_selects_cap_without_cross_etf_pooling(isolated_db):
    account_id = _account_id(isolated_db)
    first_id = _seed_position(isolated_db, account_id=account_id, stock_code="512480", cost=18000)
    _seed_position(isolated_db, account_id=account_id, stock_code="512690", cost=15000)
    service = PaperTradingService(
        repo=PaperTradingRepository(db_manager=isolated_db),
        max_symbol_exposure_pct=50.0,
        asset_class_overrides={"cn:512480": "theme_etf", "cn:512690": "themeETF"},
    )
    with isolated_db.get_session() as session:
        snapshot = service._symbol_exposure_snapshot(
            session,
            session.get(PaperCapitalAccount, account_id),
            source_model="model-a",
            market="cn",
            stock_code="512480",
            projected_additional_cost=13000,
        )
    assert snapshot["asset_type"] == "theme_etf"
    assert snapshot["asset_exposure_scope"] == "identity"
    assert snapshot["current_symbol_exposure"] == pytest.approx(18000.0)
    assert snapshot["asset_class_current_exposure"] == pytest.approx(18000.0)
    assert snapshot["projected_symbol_exposure"] == pytest.approx(31000.0)
    assert snapshot["symbol_exposure_ceiling"] == pytest.approx(30000.0)
    assert set(snapshot["contributing_position_ids"]) == {first_id}
    assert snapshot["within_limit"] is False


def test_llm_writable_raw_result_asset_type_does_not_upgrade_etf_cap(isolated_db):
    account_id = _account_id(isolated_db)
    with isolated_db.session_scope() as session:
        _source_rows(
            session,
            stock_code="510300",
            raw_result={"asset_type": "broad_etf", "dashboard": {"summary": "LLM-written field"}},
        )
    service = PaperTradingService(repo=PaperTradingRepository(db_manager=isolated_db), max_symbol_exposure_pct=50.0)
    with isolated_db.get_session() as session:
        snapshot = service._symbol_exposure_snapshot(
            session,
            session.get(PaperCapitalAccount, account_id),
            source_model="model-a",
            market="cn",
            stock_code="510300",
            projected_additional_cost=21000,
        )
    assert snapshot["asset_type"] == "unknown"
    assert snapshot["asset_type_source"] == "cn_etf_without_verified_class"
    assert snapshot["asset_exposure_cap_pct"] == pytest.approx(20.0)
    assert snapshot["symbol_exposure_ceiling"] == pytest.approx(20000.0)


def test_unidentified_cn_etf_stays_unknown_conservative(isolated_db):
    account_id = _account_id(isolated_db)
    service = PaperTradingService(repo=PaperTradingRepository(db_manager=isolated_db), max_symbol_exposure_pct=50.0)
    with isolated_db.get_session() as session:
        snapshot = service._symbol_exposure_snapshot(
            session,
            session.get(PaperCapitalAccount, account_id),
            source_model="model-a",
            market="cn",
            stock_code="510300",
            projected_additional_cost=21000,
        )
    assert snapshot["asset_type"] == "unknown"
    assert snapshot["asset_type_source"] == "cn_etf_without_verified_class"
    assert snapshot["asset_exposure_cap_pct"] == pytest.approx(20.0)
    assert snapshot["symbol_exposure_ceiling"] == pytest.approx(20000.0)
    assert snapshot["within_limit"] is False


def test_analysis_context_exposes_asset_and_sizing_capacity_without_trade(isolated_db):
    account_id = _account_id(isolated_db)
    _seed_position(isolated_db, account_id=account_id, stock_code="510300", cost=25000)
    service = PaperTradingService(
        repo=PaperTradingRepository(db_manager=isolated_db),
        max_symbol_exposure_pct=50.0,
        asset_class_overrides={"cn:510300": "broad_etf"},
    )

    context = service.analysis_context_for_stock(source_model="model-a", stock_code="510300")

    assert context["asset_type"] == "broad_etf"
    assert context["asset_exposure_effective_cap_pct"] == pytest.approx(40.0)
    assert context["asset_exposure_ceiling"] == pytest.approx(40000.0)
    assert context["asset_exposure_current"] == pytest.approx(25000.0)
    assert context["asset_exposure_remaining_capacity"] == pytest.approx(15000.0)
    assert context["sizing_context"]["cash"] == pytest.approx(75000.0)
    assert context["sizing_context"]["risk_budget"] == pytest.approx(1000.0)
    assert context["sizing_context"]["total_exposure_remaining_capacity"] == pytest.approx(55000.0)
    assert context["sizing_context"]["sizing_note"] == "read_only_capacity_context_no_trade_authorization"
    with isolated_db.get_session() as session:
        assert session.query(PaperTradeEvent).count() == 0
