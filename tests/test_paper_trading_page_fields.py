# -*- coding: utf-8 -*-
"""Display-only fields the paper-trading page reads from plans and lots.

``source_kind`` buckets a plan / lot by the trusted ``intent_source`` saved with
its source report, and ``held_sessions`` counts exchange sessions on the same
basis as the EOD time exit.  Both are additive: no ledger state changes.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional
from unittest.mock import patch

# 别名避免 pytest 把替换后的 TestClient 当测试类收集。
from fastapi.testclient import TestClient as _TestClient

from api.app import create_app
from src.config import Config
from src.services.paper_trading_service import PaperTradingService
from src.storage import PaperCapitalAccount, PaperExecutionPlan, PaperPosition
from tests.report_intent_helpers import add_report_execution_intent
from tests.test_paper_trading_service import _source, isolated_db  # noqa: F401

# 2026-10-09 16:00 CST：国庆休市（10-01..10-07）之后的第二个交易日收盘后。
NOW_UTC_NAIVE = datetime(2026, 10, 9, 8, 0)


def _seed_lot(
    db,
    *,
    stock_code: str,
    intent_source: Optional[str],
    opened_on: date,
    closed_on: Optional[date] = None,
    model: str = "model-a",
):
    report_id, signal_id = _source(
        db,
        created_on=opened_on - timedelta(days=1),
        report_code=stock_code,
        signal_code=stock_code,
        model=model,
        execution_intent=None,
    )
    with db.session_scope() as session:
        if intent_source is not None:
            add_report_execution_intent(session, report_id, intent_source=intent_source)
        account = session.query(PaperCapitalAccount).filter_by(account_key=f"{model}:cn:CNY").one_or_none()
        if account is None:
            account = PaperCapitalAccount(
                account_key=f"{model}:cn:CNY",
                source_model=model,
                market="cn",
                currency="CNY",
                initial_capital=100000,
                cash=100000,
            )
            session.add(account)
            session.flush()
        plan = PaperExecutionPlan(
            plan_key=f"page-fields-{model}-{stock_code}",
            plan_version="paper-hybrid-v3",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model=model,
            market="cn",
            stock_code=stock_code,
            stock_name="测试股",
            report_date=opened_on - timedelta(days=1),
            report_created_at=datetime.combine(opened_on - timedelta(days=1), datetime.min.time()),
            valid_for_date=opened_on,
            action="buy",
            executable=True,
            status="closed" if closed_on else "triggered",
            rules_json="[]",
            descriptive_conditions_json="[]",
            stop_loss=95,
            target_price=130,
            max_holding_sessions=10,
        )
        session.add(plan)
        session.flush()
        session.add(PaperPosition(
            plan_id=plan.id,
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model=model,
            market="cn",
            stock_code=stock_code,
            stock_name="测试股",
            status="closed" if closed_on else "open",
            opened_on=opened_on,
            opened_price=100,
            normalized_notional=20000,
            quantity=200,
            capital_account_id=account.id,
            currency="CNY",
            allocated_capital=20000,
            remaining_cost=20000,
            lot_size=100,
            sellability_rule="cn_t1",
            stop_loss=95,
            target_price=130,
            max_holding_sessions=10,
            closed_on=closed_on,
            closed_price=110 if closed_on else None,
            close_reason="take_profit" if closed_on else None,
        ))
    return report_id


def test_plans_and_lots_expose_report_source_kind(isolated_db):
    _seed_lot(isolated_db, stock_code="600001", intent_source="market_opportunity", opened_on=date(2026, 9, 29))
    _seed_lot(isolated_db, stock_code="600002", intent_source="market_opportunity_research", opened_on=date(2026, 9, 29))
    _seed_lot(isolated_db, stock_code="600003", intent_source="analysis_pipeline", opened_on=date(2026, 9, 29))
    _seed_lot(isolated_db, stock_code="600004", intent_source="legacy_baseline", opened_on=date(2026, 9, 29))
    _seed_lot(isolated_db, stock_code="600005", intent_source=None, opened_on=date(2026, 9, 29))
    expected = {
        "600001": "market_opportunity",
        "600002": "market_opportunity",
        "600003": "analysis",
        "600004": "legacy",
        "600005": None,
    }
    service = PaperTradingService()

    with patch("src.services.paper_trading_service.utc_naive_now", return_value=NOW_UTC_NAIVE):
        plans = service.list_plans(model="model-a")["items"]
        lots = service.list_positions(model="model-a", status="open")["items"]
        operations = service.list_operations(model="model-a")["items"]

    assert {item["stock_code"]: item["source_kind"] for item in plans} == expected
    assert {item["stock_code"]: item["source_kind"] for item in lots} == expected
    assert {item["stock_code"]: item["open_lots"][0]["source_kind"] for item in operations} == expected
    # 旧字段保持原样。
    assert all(item["sync_status"] == "paper_only" for item in lots)


def test_held_sessions_count_exchange_sessions_not_natural_days(isolated_db):
    # 09-29 开仓：之后的交易日只有 09-30、10-08、10-09（国庆休市不计），自然日则是 10 天。
    _seed_lot(isolated_db, stock_code="600011", intent_source="analysis_pipeline", opened_on=date(2026, 9, 29))
    # 当日开仓的批次持有 0 个交易日。
    _seed_lot(isolated_db, stock_code="600012", intent_source="analysis_pipeline", opened_on=date(2026, 10, 9))
    # 已平仓批次数到平仓日：09-29、09-30、10-08。
    _seed_lot(
        isolated_db,
        stock_code="600013",
        intent_source="analysis_pipeline",
        opened_on=date(2026, 9, 28),
        closed_on=date(2026, 10, 8),
    )
    service = PaperTradingService()

    with patch("src.services.paper_trading_service.utc_naive_now", return_value=NOW_UTC_NAIVE):
        lots = service.list_positions(model="model-a")["items"]
        operations = service.list_operations(model="model-a")["items"]

    held = {item["stock_code"]: item["held_sessions"] for item in lots}
    assert held == {"600011": 3, "600012": 0, "600013": 3}
    assert {item["stock_code"]: item["max_holding_sessions"] for item in lots}["600011"] == 10
    assert {
        item["stock_code"]: item["open_lots"][0]["held_sessions"] for item in operations
    } == {"600011": 3, "600012": 0}


def test_held_sessions_is_null_when_calendar_unavailable(isolated_db):
    _seed_lot(isolated_db, stock_code="600021", intent_source="market_opportunity", opened_on=date(2026, 9, 29))
    service = PaperTradingService()

    with patch("src.services.paper_trading_service.utc_naive_now", return_value=NOW_UTC_NAIVE), patch(
        "src.services.paper_trading_service._market_context_calendar", return_value=None,
    ):
        lot = service.list_positions(model="model-a", status="open")["items"][0]
        operation = service.list_operations(model="model-a")["items"][0]

    assert lot["held_sessions"] is None
    assert operation["open_lots"][0]["held_sessions"] is None
    # 来源与日历无关，照常返回。
    assert lot["source_kind"] == "market_opportunity"


def test_positions_api_passes_new_fields_through(isolated_db, tmp_path, monkeypatch):
    _seed_lot(isolated_db, stock_code="600031", intent_source="market_opportunity", opened_on=date(2026, 9, 29))
    monkeypatch.setenv("ADMIN_AUTH_ENABLED", "false")
    monkeypatch.setenv("PAPER_TRADING_ENABLED", "false")
    Config.reset_instance()
    client = _TestClient(create_app(static_dir=tmp_path / "missing-static"))

    with patch("src.services.paper_trading_service.utc_naive_now", return_value=NOW_UTC_NAIVE):
        positions = client.get("/api/v1/paper-trading/positions?status=open")
        plans = client.get("/api/v1/paper-trading/plans")

    assert positions.status_code == 200
    item = positions.json()["items"][0]
    assert item["source_kind"] == "market_opportunity"
    assert item["held_sessions"] == 3
    assert plans.status_code == 200
    assert plans.json()["items"][0]["source_kind"] == "market_opportunity"
