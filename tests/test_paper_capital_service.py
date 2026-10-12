# -*- coding: utf-8 -*-
"""Focused SQLite tests for paper capital accounts and Feishu dispatch state."""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from datetime import date
from types import SimpleNamespace

from tests.test_paper_trading_service import (
    _bars,
    _create_plan_with_report_clock,
    _persist_daily_rows,
    _source,
    isolated_db,
)

from src.config import Config
from src.services.paper_trading_service import PaperTradingService
from src.storage import DatabaseManager, PaperCapitalAccount, PaperCapitalAdjustment, PaperNotificationDispatch, PaperPosition, PaperTradeEvent


def test_shared_capital_isolated_by_model_market_currency(isolated_db):
    cn_a, cn_a_signal = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["放量"],
        entry_low=96.0,
        entry_high=100.0,
        stop_loss=95.0,
        target_price=130.0,
        model="model-a",
    )
    cn_b, cn_b_signal = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["放量"],
        entry_low=96.0,
        entry_high=100.0,
        stop_loss=95.0,
        target_price=130.0,
        model="model-b",
    )
    hk, hk_signal = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["放量"],
        entry_low=96.0,
        entry_high=100.0,
        stop_loss=95.0,
        target_price=130.0,
        market="hk",
        report_code="00700.HK",
        signal_code="HK00700",
        model="model-a",
    )
    service = PaperTradingService(strategy_entry_confirmation_sessions=1)
    _create_plan_with_report_clock(service, cn_a, decision_signal_id=cn_a_signal)
    _create_plan_with_report_clock(service, cn_b, decision_signal_id=cn_b_signal)
    _create_plan_with_report_clock(service, hk, decision_signal_id=hk_signal)

    result = service.run_eod(
        date(2026, 8, 10),
        {
            ("cn", "600519"): _bars(start=date(2026, 8, 5), days=6, current_volume=151.0),
            ("hk", "00700"): _bars(start=date(2026, 8, 5), days=6, current_volume=151.0),
        },
    )

    assert result["positions_opened"] == 3
    accounts = service.list_accounts()["items"]
    assert {item["account_key"] for item in accounts} == {
        "model-a:cn:CNY",
        "model-b:cn:CNY",
        "model-a:hk:HKD",
    }
    assert all(item["cash"] < item["initial_capital"] for item in accounts)


def test_capital_account_sizing_open_close_and_disabled_dispatch(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["放量"],
        entry_low=96.0,
        entry_high=100.0,
        stop_loss=95.0,
        target_price=130.0,
    )
    service = PaperTradingService(
        initial_capital=100000.0,
        risk_per_trade_pct=1.0,
        max_position_pct=20.0,
        max_total_exposure_pct=80.0,
        max_open_positions=5,
        feishu_notify=False,
        strategy_entry_confirmation_sessions=1,
    )
    _create_plan_with_report_clock(service, report_id, decision_signal_id=signal_id)
    opened = service.run_eod(
        date(2026, 8, 10),
        {("cn", "600519"): _bars(start=date(2026, 8, 5), days=6, current_volume=151.0)},
    )

    assert opened["positions_opened"] == 1
    with isolated_db.get_session() as session:
        account = session.query(PaperCapitalAccount).one()
        position = session.query(PaperPosition).one()
        dispatch = session.query(PaperNotificationDispatch).one()
        assert account.account_key == "model-a:cn:CNY"
        assert position.lot_size == 100
        assert position.quantity == 200
        assert position.allocated_capital == 20000
        assert account.cash == 80000
        assert account.exposure == 20000
        assert account.open_position_count == 1
        assert dispatch.status == "disabled"
        assert "仅纸面模拟" in dispatch.payload_json
        assert "模型: model-a" in dispatch.payload_json
        assert "时间(中国):" in dispatch.payload_json
        assert "分配资金: 20000.00" in dispatch.payload_json
        assert "factor_domains" in dispatch.payload_json
        assert '"rules"' in dispatch.payload_json

    close_bars = _bars(start=date(2026, 8, 5), days=7, current_volume=151.0)
    close_bars[-1]["high"] = 131.0
    close_bars[-1]["low"] = 96.0
    close_bars[-1]["close"] = 130.0
    closed = service.run_eod(
        date(2026, 8, 11),
        {("cn", "600519"): close_bars},
    )

    assert closed["positions_closed"] == 1
    with isolated_db.get_session() as session:
        account = session.query(PaperCapitalAccount).one()
        assert account.open_position_count == 0
        assert account.exposure == 0
        assert account.cash > 80000
    events = service.list_events(model="model-a")["items"]
    open_event = next(item for item in events if item["event_type"] == "open")
    close_event = next(item for item in events if item["event_type"] == "close")
    assert open_event["notification_dispatch"]["status"] == "disabled"
    assert close_event["notification_dispatch"]["status"] == "disabled"


def test_sizing_fails_closed_when_one_lot_exceeds_risk_budget(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["放量"],
        entry_low=96.0,
        entry_high=100.0,
        stop_loss=80.0,
        target_price=130.0,
    )
    service = PaperTradingService(
        initial_capital=100000.0,
        risk_per_trade_pct=1.0,
        max_position_pct=20.0,
        max_total_exposure_pct=80.0,
        max_open_positions=5,
        strategy_entry_confirmation_sessions=1,
    )
    _create_plan_with_report_clock(service, report_id, decision_signal_id=signal_id)
    result = service.run_eod(
        date(2026, 8, 10),
        {("cn", "600519"): _bars(start=date(2026, 8, 5), days=6, current_volume=151.0)},
    )

    assert result["positions_opened"] == 0
    assert result["failed_closed"] == 1
    assert result["evaluations"][0]["reasons"] == ["insufficient_capital_for_lot"]


def test_failed_dispatch_retries_without_duplicate_success(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["放量"],
        entry_low=96.0,
        entry_high=100.0,
        stop_loss=95.0,
        target_price=130.0,
    )
    service = PaperTradingService(
        initial_capital=100000.0,
        risk_per_trade_pct=1.0,
        max_position_pct=20.0,
        max_total_exposure_pct=80.0,
        max_open_positions=5,
        feishu_notify=True,
        strategy_entry_confirmation_sessions=1,
    )
    service._feishu_route_configured = lambda: True
    _create_plan_with_report_clock(service, report_id, decision_signal_id=signal_id)

    class Sender:
        def __init__(self):
            self.calls = 0

        def send_to_feishu(self, _content):
            self.calls += 1
            return self.calls >= 2

    sender = Sender()
    original_dispatch = service.dispatch_pending_notifications
    service.dispatch_pending_notifications = lambda *args, **kwargs: original_dispatch(sender=sender)
    service.run_eod(
        date(2026, 8, 10),
        {("cn", "600519"): _bars(start=date(2026, 8, 5), days=6, current_volume=151.0)},
    )
    no_exit_bars = _bars(start=date(2026, 8, 5), days=7, current_volume=151.0)
    no_exit_bars[-1]["low"] = 96.0
    no_exit_bars[-1]["high"] = 105.0
    no_exit_bars[-1]["close"] = 100.0
    retry = service.run_eod(date(2026, 8, 11), {("cn", "600519"): no_exit_bars})
    no_exit_bars_2 = _bars(start=date(2026, 8, 5), days=8, current_volume=151.0)
    no_exit_bars_2[-1]["low"] = 96.0
    no_exit_bars_2[-1]["high"] = 105.0
    no_exit_bars_2[-1]["close"] = 100.0
    duplicate = service.run_eod(date(2026, 8, 12), {("cn", "600519"): no_exit_bars_2})
    service.dispatch_pending_notifications = original_dispatch

    assert retry["dispatches"]["sent"] == 1
    assert duplicate["dispatches"]["sent"] == 0
    assert sender.calls == 2
    with isolated_db.get_session() as session:
        dispatch = session.query(PaperNotificationDispatch).one()
        assert dispatch.status == "sent"
        assert dispatch.attempt_count == 2


def test_retry_query_prioritizes_new_pending_over_permanent_failures(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["放量"],
        entry_low=96.0,
        entry_high=100.0,
        stop_loss=95.0,
        target_price=130.0,
    )
    service = PaperTradingService(
        initial_capital=100000.0,
        risk_per_trade_pct=1.0,
        max_position_pct=20.0,
        max_total_exposure_pct=80.0,
        max_open_positions=5,
        feishu_notify=True,
        strategy_entry_confirmation_sessions=1,
    )
    service._feishu_route_configured = lambda: True
    _create_plan_with_report_clock(service, report_id, decision_signal_id=signal_id)
    service.dispatch_pending_notifications = lambda *args, **kwargs: {"sent": 0, "failed": 0, "skipped": 0}
    service.run_eod(
        date(2026, 8, 10),
        {("cn", "600519"): _bars(start=date(2026, 8, 5), days=6, current_volume=151.0)},
    )

    with isolated_db.session_scope() as session:
        event = session.query(PaperTradeEvent).one()
        session.query(PaperNotificationDispatch).delete()
        for index in range(501):
            session.add(PaperNotificationDispatch(
                dispatch_key=f"paper-open:event:{event.id}:failed:{index}",
                event_id=event.id,
                channel="feishu",
                status="failed",
                attempt_count=3,
                payload_json='{"content":"old failed"}',
            ))
        session.add(PaperNotificationDispatch(
            dispatch_key=f"paper-open:event:{event.id}:pending:new",
            event_id=event.id,
            channel="feishu",
            status="pending",
            attempt_count=0,
            payload_json='{"content":"new pending"}',
        ))

    class Sender:
        def __init__(self):
            self.contents = []

        def send_to_feishu(self, content):
            self.contents.append(content)
            return True

    sender = Sender()
    stats = PaperTradingService(feishu_notify=True).dispatch_pending_notifications(sender=sender)

    assert stats["sent"] == 1
    assert sender.contents == ["new pending"]


def test_dispatch_claim_allows_only_one_concurrent_sender(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["放量"],
        entry_low=96.0,
        entry_high=100.0,
        stop_loss=95.0,
        target_price=130.0,
    )
    setup_service = PaperTradingService(feishu_notify=True, strategy_entry_confirmation_sessions=1)
    setup_service._feishu_route_configured = lambda: True
    _create_plan_with_report_clock(setup_service, report_id, decision_signal_id=signal_id)
    setup_service.dispatch_pending_notifications = lambda *args, **kwargs: {"sent": 0, "failed": 0, "skipped": 0}
    setup_service.run_eod(
        date(2026, 8, 10),
        {("cn", "600519"): _bars(start=date(2026, 8, 5), days=6, current_volume=151.0)},
    )

    class Sender:
        def __init__(self):
            self.calls = 0
            self.lock = threading.Lock()

        def send_to_feishu(self, _content):
            with self.lock:
                self.calls += 1
            time.sleep(0.1)
            return True

    sender = Sender()
    services = [PaperTradingService(feishu_notify=True), PaperTradingService(feishu_notify=True)]
    results = []

    def worker(service):
        results.append(service.dispatch_pending_notifications(sender=sender))

    threads = [threading.Thread(target=worker, args=(service,)) for service in services]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert sender.calls == 1
    assert sum(item["sent"] for item in results) == 1
    with isolated_db.get_session() as session:
        dispatch = session.query(PaperNotificationDispatch).one()
        assert dispatch.status == "sent"
        assert dispatch.attempt_count == 1


def test_sqlite_ledger_write_lock_serializes_concurrent_capital_debits(isolated_db):
    service = PaperTradingService(
        initial_capital=100000.0,
        risk_per_trade_pct=1.0,
        max_position_pct=20.0,
        max_total_exposure_pct=80.0,
        max_open_positions=5,
    )
    errors = []

    def worker(hold_lock=False):
        try:
            with service._ledger_write_session() as session:
                account = service._get_or_create_account(
                    session,
                    source_model="model-a",
                    market="cn",
                )
                sizing, error = service._size_position(account, price=100.0, stop_loss=95.0, market="cn")
                assert error is None
                if hold_lock:
                    time.sleep(0.2)
                service._debit_account_for_open(account, sizing)
        except Exception as exc:  # pragma: no cover - failure surfaced below
            errors.append(exc)

    first = threading.Thread(target=worker, kwargs={"hold_lock": True})
    second = threading.Thread(target=worker)
    first.start()
    time.sleep(0.05)
    second.start()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    with isolated_db.get_session() as session:
        account = session.query(PaperCapitalAccount).one()
        assert account.cash == 60000
        assert account.invested_capital == 40000
        assert account.exposure == 40000
        assert account.open_position_count == 2


def test_explicit_account_initialization_and_adjustment_idempotency(isolated_db, monkeypatch):
    service = PaperTradingService(initial_capital=100000.0)
    monkeypatch.setattr(service, "_validate_configured_model", lambda model: model)

    created = service.initialize_account(
        source_model="model-a",
        market="cn",
        initial_capital=120000.0,
    )
    repeated = service.initialize_account(
        source_model="model-a",
        market="cn",
        initial_capital=1.0,
    )
    deposit = service.adjust_account(
        created["item"]["id"],
        adjustment_type="deposit",
        amount=5000.0,
        idempotency_key="fund-1",
        reason="top up",
    )
    retry = service.adjust_account(
        created["item"]["id"],
        adjustment_type="deposit",
        amount=5000.0,
        idempotency_key="fund-1",
        reason="top up",
    )
    withdrawal = service.adjust_account(
        created["item"]["id"],
        adjustment_type="withdrawal",
        amount=2000.0,
        idempotency_key="fund-2",
        reason="reduce cash",
    )

    assert created["created"] is True
    assert repeated["created"] is False
    assert repeated["item"]["initial_capital"] == 120000.0
    assert deposit["created"] is True
    assert retry["created"] is False
    assert retry["item"]["id"] == deposit["item"]["id"]
    assert withdrawal["account"]["cash"] == 123000.0
    assert withdrawal["account"]["external_cash_flow"] == 3000.0
    assert withdrawal["account"]["book_equity"] == 123000.0
    with isolated_db.get_session() as session:
        account = session.query(PaperCapitalAccount).one()
        rows = session.query(PaperCapitalAdjustment).order_by(PaperCapitalAdjustment.id).all()
        assert account.initial_capital == 120000.0
        assert account.cash == 123000.0
        assert [row.adjustment_type for row in rows] == ["deposit", "withdrawal"]
        assert rows[0].before_cash == 120000.0
        assert rows[0].after_cash == 125000.0


def test_adjustment_rejects_conflicting_key_and_unsafe_withdrawal(isolated_db, monkeypatch):
    service = PaperTradingService(initial_capital=100000.0, max_total_exposure_pct=80.0)
    monkeypatch.setattr(service, "_validate_configured_model", lambda model: model)
    account_id = service.initialize_account(source_model="model-a", market="cn")["item"]["id"]
    service.adjust_account(
        account_id,
        adjustment_type="deposit",
        amount=1000.0,
        idempotency_key="same-key",
        reason="first",
    )
    try:
        service.adjust_account(
            account_id,
            adjustment_type="deposit",
            amount=2000.0,
            idempotency_key="same-key",
            reason="first",
        )
    except Exception as exc:
        assert exc.__class__.__name__ == "PaperTradingConflictError"
    else:  # pragma: no cover - assertion above should always raise
        raise AssertionError("conflicting idempotency key must fail")

    with isolated_db.session_scope() as session:
        account = session.query(PaperCapitalAccount).one()
        account.cash = 20000.0
        account.invested_capital = 80000.0
        account.exposure = 80000.0

    try:
        service.adjust_account(
            account_id,
            adjustment_type="withdrawal",
            amount=1000.0,
            idempotency_key="unsafe-withdrawal",
            reason="would breach exposure",
        )
    except ValueError as exc:
        assert "max total exposure" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("unsafe withdrawal must fail")


def test_account_initialization_validates_model_and_market(isolated_db, monkeypatch):
    service = PaperTradingService()

    def reject(_model):
        raise ValueError("not configured")

    monkeypatch.setattr(service, "_validate_configured_model", reject)
    try:
        service.initialize_account(source_model="missing-model", market="cn")
    except ValueError as exc:
        assert "not configured" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("unknown model must fail")

    monkeypatch.setattr(service, "_validate_configured_model", lambda model: model)
    try:
        service.initialize_account(source_model="model-a", market="moon")
    except ValueError as exc:
        assert "unsupported market" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("unsupported market must fail")


def test_adjusted_book_equity_changes_future_sizing_limits(isolated_db, monkeypatch):
    service = PaperTradingService(
        initial_capital=100000.0,
        risk_per_trade_pct=10.0,
        max_position_pct=20.0,
        max_total_exposure_pct=80.0,
    )
    monkeypatch.setattr(service, "_validate_configured_model", lambda model: model)
    account_id = service.initialize_account(source_model="model-a", market="cn")["item"]["id"]
    service.adjust_account(
        account_id,
        adjustment_type="deposit",
        amount=50000.0,
        idempotency_key="sizing-top-up",
        reason="increase paper sizing budget",
    )
    with isolated_db.get_session() as session:
        account = session.get(PaperCapitalAccount, account_id)
        sizing, error = service._size_position(account, price=100.0, stop_loss=50.0, market="cn")

    assert error is None
    assert sizing["allocated_capital"] == 30000.0


def test_adjustments_persist_across_sqlite_restart(tmp_path, monkeypatch):
    db_path = tmp_path / "paper-funds.db"
    old_database_path = os.environ.get("DATABASE_PATH")
    os.environ["DATABASE_PATH"] = str(db_path)
    Config.reset_instance()
    DatabaseManager.reset_instance()
    try:
        db = DatabaseManager.get_instance()
        service = PaperTradingService(db_manager=None)
    except TypeError:
        service = PaperTradingService()
    monkeypatch.setattr(service, "_validate_configured_model", lambda model: model)
    try:
        account_id = service.initialize_account(source_model="model-a", market="cn", initial_capital=90000.0)["item"]["id"]
        service.adjust_account(
            account_id,
            adjustment_type="deposit",
            amount=10000.0,
            idempotency_key="restart-proof",
            reason="restart read",
        )
        DatabaseManager.reset_instance()
        reloaded = DatabaseManager.get_instance()
        with reloaded.get_session() as session:
            account = session.query(PaperCapitalAccount).one()
            adjustment = session.query(PaperCapitalAdjustment).one()
            assert account.cash == 100000.0
            assert adjustment.after_book_equity == 100000.0
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        if old_database_path is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = old_database_path


def test_legacy_sqlite_schema_migrates_capital_columns_without_dropping_rows(tmp_path):
    db_path = tmp_path / "legacy-paper.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE paper_positions (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO paper_positions (id) VALUES (1)")
        conn.execute("CREATE TABLE paper_trade_events (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO paper_trade_events (id) VALUES (1)")
        conn.commit()

    old_database_path = os.environ.get("DATABASE_PATH")
    os.environ["DATABASE_PATH"] = str(db_path)
    Config.reset_instance()
    DatabaseManager.reset_instance()
    try:
        db = DatabaseManager.get_instance()
        with db._engine.connect() as conn:
            position_columns = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(paper_positions)").fetchall()}
            event_columns = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(paper_trade_events)").fetchall()}
            account_columns = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(paper_capital_accounts)").fetchall()}
            adjustment_columns = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(paper_capital_adjustments)").fetchall()}
            position_count = conn.exec_driver_sql("SELECT COUNT(*) FROM paper_positions").scalar()
            event_count = conn.exec_driver_sql("SELECT COUNT(*) FROM paper_trade_events").scalar()
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        if old_database_path is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = old_database_path

    assert position_count == 1
    assert event_count == 1
    assert {
        "capital_account_id",
        "currency",
        "allocated_capital",
        "lot_size",
        "risk_amount",
        "risk_per_share",
        "sizing_reason",
        "legacy_fixed_notional",
    }.issubset(position_columns)
    assert {
        "capital_account_id",
        "currency",
        "allocated_capital",
        "cash_after",
        "exposure_after",
        "realized_pnl_after",
    }.issubset(event_columns)
    assert "external_cash_flow" in account_columns
    assert {"idempotency_key", "account_id", "before_cash", "after_cash"}.issubset(adjustment_columns)


def _seed_open_lots(isolated_db, *, count: int, stored_count: int):
    from datetime import datetime as _dt

    from src.storage import PaperExecutionPlan

    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), model="model-a")
    with isolated_db.session_scope() as session:
        account = PaperCapitalAccount(
            account_key="model-a:cn:CNY",
            source_model="model-a",
            market="cn",
            currency="CNY",
            initial_capital=100000.0,
            cash=100000.0 - 10000.0 * count,
            invested_capital=10000.0 * count,
            realized_pnl=0.0,
            exposure=10000.0 * count,
            open_position_count=stored_count,
            risk_per_trade_pct=1.0,
            max_position_pct=20.0,
            max_total_exposure_pct=80.0,
            max_open_positions=5,
        )
        session.add(account)
        session.flush()
        position_ids = []
        for index in range(count):
            plan = PaperExecutionPlan(
                plan_key=f"count-plan-{index}",
                plan_version="paper-hybrid-v3",
                source_report_id=report_id,
                decision_signal_id=signal_id,
                source_model="model-a",
                market="cn",
                stock_code="600519",
                stock_name="贵州茅台",
                report_date=date(2026, 8, 7),
                report_created_at=_dt(2026, 8, 7),
                valid_for_date=date(2026, 8, 10),
                action="buy",
                executable=True,
                status="triggered",
                rules_json="[]",
                descriptive_conditions_json="[]",
                stop_loss=90,
                target_price=130,
                max_holding_sessions=10,
            )
            session.add(plan)
            session.flush()
            position = PaperPosition(
                plan_id=plan.id,
                source_report_id=report_id,
                decision_signal_id=signal_id,
                source_model="model-a",
                market="cn",
                stock_code="600519",
                stock_name="贵州茅台",
                status="open",
                opened_on=date(2026, 8, 10),
                opened_price=100.0,
                normalized_notional=10000.0,
                quantity=100,
                available_quantity=100,
                frozen_quantity=0,
                capital_account_id=account.id,
                currency="CNY",
                allocated_capital=10000.0,
                lot_size=100,
                legacy_fixed_notional=False,
                sellability_rule="cn_t1",
                stop_loss=90,
                target_price=130,
                max_holding_sessions=10,
            )
            session.add(position)
            session.flush()
            position_ids.append(position.id)
    return position_ids


def test_closing_sibling_lots_in_one_session_releases_the_holding_count(isolated_db):
    # Production drift: several lots of one stock closed in the same ledger
    # session each saw the unflushed sibling as "still open" and never
    # decremented, leaving open_position_count > 0 with no open lots.
    position_ids = _seed_open_lots(isolated_db, count=2, stored_count=1)
    service = PaperTradingService(commission_rate=0.0, min_commission=0.0, slippage_pct=0.0)

    with isolated_db.session_scope() as session:
        account = session.query(PaperCapitalAccount).one()
        for position_id in position_ids:
            position = session.get(PaperPosition, position_id)
            fill, error = service._apply_position_exit(
                session,
                account,
                position,
                quote_price=120.0,
                reason="intraday_take_profit",
                event_date=date(2026, 8, 12),
                requested_quantity=100,
            )
            assert error is None, error
            assert fill["final_close"] is True

    with isolated_db.get_session() as session:
        account = session.query(PaperCapitalAccount).one()
        assert account.open_position_count == 0
        assert session.query(PaperPosition).filter(PaperPosition.status == "open").count() == 0


def test_run_eod_repairs_drifted_holding_count_before_gating_entries(isolated_db):
    _seed_open_lots(isolated_db, count=0, stored_count=2)
    service = PaperTradingService()

    summary = service.run_eod(date(2026, 8, 12), {})

    assert summary["open_position_counts_repaired"] == 1
    with isolated_db.get_session() as session:
        assert session.query(PaperCapitalAccount).one().open_position_count == 0
    assert service.run_eod(date(2026, 8, 13), {})["open_position_counts_repaired"] == 0


def test_holding_count_counts_identities_not_lots(isolated_db):
    _seed_open_lots(isolated_db, count=3, stored_count=3)
    service = PaperTradingService()

    assert service._reconcile_open_position_counts() == 1
    with isolated_db.get_session() as session:
        assert session.query(PaperCapitalAccount).one().open_position_count == 1
