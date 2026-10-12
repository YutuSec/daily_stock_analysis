# -*- coding: utf-8 -*-
"""V3 acceptance-style checks for paper-trading cross-day contracts.

These tests intentionally reuse the real service/database fixtures from
``test_paper_trading_service`` and only stub market time, quote feed, and the
external Feishu sender. They do not fabricate LLM success or bypass the paper
trading risk/ledger logic.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from sqlalchemy import select

from src.repositories.paper_trading_repo import PaperTradingRepository
from src.services.paper_trading_service import PaperTradingService
from src.storage import (
    PaperCapitalAccount,
    PaperExecutionPlan,
    PaperNotificationDispatch,
    PaperPosition,
    PaperTradeEvent,
)
from tests.test_paper_trading_service import (
    _QuoteManager,
    _bars,
    _market_phase,
    _persist_daily_rows,
    _quote,
    _seed_open_paper_lot,
    _single_confirmation_service,
    _source,
    isolated_db,
)


def _service(db, **kwargs) -> PaperTradingService:
    return _single_confirmation_service(repo=PaperTradingRepository(db), **kwargs)


def test_v3_existing_cn_position_stages_same_day_stop_then_next_day_closes_with_fees_and_idempotent_notification(
    isolated_db,
):
    """Existing holding: same-day stop is T+1 staged, next day closes once, notification failure is isolated."""

    _plan_id, position_id = _seed_open_paper_lot(
        isolated_db,
        opened_on=date(2026, 8, 10),
        stock_code="002714",
    )
    service = _service(
        isolated_db,
        commission_rate=0.001,
        min_commission=5.0,
        sell_tax_rate=0.001,
        feishu_notify=False,
    )

    same_day_phase = _market_phase(date(2026, 8, 10))
    same_day_now = datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=same_day_phase):
        pending = service.run_intraday_monitor(
            current_time=same_day_now,
            fetcher_manager=_QuoteManager(
                _quote(
                    price=90.0,
                    code="002714",
                    provider_timestamp="2026-08-10T10:00:00+08:00",
                    fetched_at="2026-08-10T10:00:03+08:00",
                )
            ),
            interval_seconds=300,
        )
        duplicate_pending = service.run_intraday_monitor(
            current_time=same_day_now,
            fetcher_manager=_QuoteManager(
                _quote(
                    price=89.0,
                    code="002714",
                    provider_timestamp="2026-08-10T10:00:10+08:00",
                    fetched_at="2026-08-10T10:00:13+08:00",
                )
            ),
            interval_seconds=300,
        )

    assert pending["positions_closed"] == 0
    assert duplicate_pending["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.status == "open"
        assert position.pending_exit_reason == "intraday_stop_loss"
        assert position.available_quantity == pytest.approx(0.0)
        assert position.frozen_quantity == pytest.approx(200.0)
        assert session.query(PaperTradeEvent).filter_by(event_type="pending_exit").count() == 1

    next_day_phase = _market_phase(date(2026, 8, 11))
    next_day_now = datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=next_day_phase):
        closed = service.run_intraday_monitor(
            current_time=next_day_now,
            fetcher_manager=_QuoteManager(
                _quote(
                    price=88.0,
                    code="002714",
                    provider_timestamp="2026-08-11T10:00:00+08:00",
                    fetched_at="2026-08-11T10:00:03+08:00",
                )
            ),
            interval_seconds=300,
        )
        duplicate_close = service.run_intraday_monitor(
            current_time=next_day_now,
            fetcher_manager=_QuoteManager(
                _quote(
                    price=87.0,
                    code="002714",
                    provider_timestamp="2026-08-11T10:00:10+08:00",
                    fetched_at="2026-08-11T10:00:13+08:00",
                )
            ),
            interval_seconds=300,
        )

    assert closed["positions_closed"] == 1
    assert duplicate_close["positions_closed"] == 0
    with isolated_db.session_scope() as session:
        position = session.get(PaperPosition, position_id)
        assert position.status == "closed"
        assert position.closed_price == pytest.approx(88.0)
        assert position.close_reason == "intraday_stop_loss"

        close_event = session.execute(
            select(PaperTradeEvent).where(PaperTradeEvent.event_type == "close")
        ).scalar_one()
        assert close_event.quantity == pytest.approx(200.0)
        assert close_event.gross_amount == pytest.approx(17600.0)
        assert close_event.commission == pytest.approx(17.6)
        assert close_event.tax == pytest.approx(17.6)
        assert close_event.fees == pytest.approx(35.2)
        assert close_event.net_realized_pnl == pytest.approx(-2435.2)

        account = session.get(PaperCapitalAccount, close_event.capital_account_id)
        assert account.cash == pytest.approx(97564.8)
        assert account.exposure == pytest.approx(0.0)
        assert account.realized_pnl == pytest.approx(-2435.2)

        # The trading service created dispatch rows with notifications disabled
        # during ledger mutation. Flip the existing close dispatch to pending to
        # test send failure/retry without creating a duplicate dispatch or
        # rerunning the trade.
        close_dispatch = session.execute(
            select(PaperNotificationDispatch).where(PaperNotificationDispatch.event_id == close_event.id)
        ).scalar_one()
        close_dispatch.status = "pending"
        close_dispatch.attempt_count = 0
        close_dispatch.last_error = None
        service.feishu_notify = True

    class FailingSender:
        calls = 0

        def send_to_feishu(self, content):
            self.calls += 1
            assert "模拟平仓" in content
            raise RuntimeError("feishu unavailable")

    sender = FailingSender()
    first_dispatch = service.dispatch_pending_notifications(sender=sender)
    second_dispatch = service.dispatch_pending_notifications(sender=sender)

    assert first_dispatch["failed"] == 1
    assert second_dispatch["failed"] == 1
    assert sender.calls == 2
    with isolated_db.get_session() as session:
        close_event = session.execute(
            select(PaperTradeEvent).where(PaperTradeEvent.event_type == "close")
        ).scalar_one()
        dispatch = session.execute(
            select(PaperNotificationDispatch).where(PaperNotificationDispatch.event_id == close_event.id)
        ).scalar_one()
        position = session.get(PaperPosition, position_id)
        assert dispatch.status == "failed"
        assert dispatch.attempt_count == 2
        assert "feishu unavailable" in (dispatch.last_error or "")
        assert position.status == "closed"
        assert session.query(PaperNotificationDispatch).count() == 2
        assert session.query(PaperTradeEvent).filter_by(event_type="close").count() == 1


def test_v3_report_plan_partial_take_profit_freezes_on_open_day_and_executes_each_leg_once_next_session(
    isolated_db,
):
    """Report-derived plan: same-day profit trigger freezes under CN T+1, then exit legs execute once."""

    raw_result = {
        "model_used": "model-a",
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "exit_leg_schema_version": "exit-legs-v1",
                    "exit_legs": [
                        {
                            "leg_id": "tp1",
                            "sequence": 1,
                            "role": "partial_take_profit",
                            "allocation_fraction": 0.5,
                            "target_price": 120,
                        },
                        {
                            "leg_id": "tp2",
                            "sequence": 2,
                            "role": "final_take_profit",
                            "allocation_fraction": 0.5,
                            "target_price": 140,
                        },
                    ],
                }
            }
        },
    }
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        raw_result=raw_result,
        target_price=140.0,
    )
    service = _service(isolated_db)
    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    assert plan["executable"] is True
    assert plan["weighted_target_price"] == pytest.approx(130.0)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    open_day = datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 10))):
        opened = service.run_intraday_monitor(
            current_time=open_day,
            fetcher_manager=_QuoteManager(_quote(provider_timestamp="2026-08-10T10:00:00+08:00")),
        )
        same_day_profit = service.run_intraday_monitor(
            current_time=open_day + timedelta(minutes=1),
            fetcher_manager=_QuoteManager(
                _quote(
                    price=121.0,
                    high=121.0,
                    provider_timestamp="2026-08-10T10:01:00+08:00",
                    fetched_at="2026-08-10T10:01:03+08:00",
                )
            ),
        )

    assert opened["positions_opened"] == 1
    assert same_day_profit["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        assert position.pending_exit_reason == "take_profit:tp1"
        assert position.available_quantity == pytest.approx(100.0)
        assert position.frozen_quantity == pytest.approx(100.0)

    next_phase = _market_phase(date(2026, 8, 11))
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=next_phase):
        first_leg = service.run_intraday_monitor(
            current_time=open_day + timedelta(days=1),
            fetcher_manager=_QuoteManager(
                _quote(
                    price=121.0,
                    high=121.0,
                    provider_timestamp="2026-08-11T10:00:00+08:00",
                    fetched_at="2026-08-11T10:00:03+08:00",
                )
            ),
        )
        duplicate_first_leg = service.run_intraday_monitor(
            current_time=open_day + timedelta(days=1, minutes=1),
            fetcher_manager=_QuoteManager(
                _quote(
                    price=121.0,
                    high=121.0,
                    provider_timestamp="2026-08-11T10:01:00+08:00",
                    fetched_at="2026-08-11T10:01:03+08:00",
                )
            ),
        )
        second_leg = service.run_intraday_monitor(
            current_time=open_day + timedelta(days=1, minutes=2),
            fetcher_manager=_QuoteManager(
                _quote(
                    price=141.0,
                    high=141.0,
                    provider_timestamp="2026-08-11T10:02:00+08:00",
                    fetched_at="2026-08-11T10:02:03+08:00",
                )
            ),
        )

    assert first_leg["positions_reduced"] == 1
    assert duplicate_first_leg["positions_reduced"] == 0
    assert duplicate_first_leg["positions_closed"] == 0
    assert second_leg["positions_closed"] == 1

    closed_plan = service.list_plans(model="model-a", status="closed")["items"][0]
    assert closed_plan["executed_exit_leg_ids"] == ["tp1", "tp2"]
    assert closed_plan["pending_exit_leg_ids"] == []
    with isolated_db.get_session() as session:
        events = session.execute(select(PaperTradeEvent).order_by(PaperTradeEvent.id)).scalars().all()
        assert [event.event_type for event in events] == ["open", "pending_exit", "reduce", "close"]
        assert [event.reason for event in events[2:]] == ["take_profit:tp1", "take_profit:tp2"]
        assert [event.quantity for event in events[2:]] == pytest.approx([100.0, 100.0])
        assert session.execute(select(PaperPosition)).scalar_one().status == "closed"
