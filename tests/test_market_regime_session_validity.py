# -*- coding: utf-8 -*-
"""Focused R1 checks for market-regime session validity."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import select

from src.config import Config
from src.core.market_review import MarketReviewRunResult
from src.services.daily_market_context import DailyMarketContextService
from src.services.paper_trading_service import PaperTradingService
from src.storage import AnalysisHistory, DailyMarketRegime, DatabaseManager


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    os.environ["DATABASE_PATH"] = str(tmp_path / "market-regime-session-validity.db")
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


class _FakeCalendar:
    def __init__(self, sessions):
        self.sessions = sorted(sessions)

    def is_session(self, value):
        return self._as_date(value) in self.sessions

    def date_to_session(self, value, direction="previous"):
        value_date = self._as_date(value)
        if value_date in self.sessions:
            return datetime.combine(value_date, time.min)
        if direction == "previous":
            candidates = [item for item in self.sessions if item <= value_date]
            if not candidates:
                raise ValueError("no previous session")
            return datetime.combine(candidates[-1], time.min)
        candidates = [item for item in self.sessions if item >= value_date]
        if not candidates:
            raise ValueError("no next session")
        return datetime.combine(candidates[0], time.min)

    def next_session(self, session):
        value_date = self._as_date(session)
        candidates = [item for item in self.sessions if item > value_date]
        if not candidates:
            raise ValueError("no next session")
        return datetime.combine(candidates[0], time.min)

    def session_close(self, session):
        local = datetime.combine(self._as_date(session), time(15, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
        return local.astimezone(timezone.utc)

    @staticmethod
    def _as_date(value):
        if isinstance(value, datetime):
            return value.date()
        return value


def _calendar_patch():
    sessions = {
        date(2026, 9, 4),  # Friday
        date(2026, 9, 7),  # Monday
        date(2026, 9, 8),
    }
    return patch(
        "src.services.daily_market_context._market_context_calendar",
        return_value=(_FakeCalendar(sessions), "XFAKE", "Asia/Shanghai"),
    )


def _add_regime(
    db,
    *,
    session_date: date,
    regime: str = "normal",
    freshness_expires_at: datetime | None = None,
    key_suffix: str | None = None,
):
    with db.session_scope() as session:
        row = DailyMarketRegime(
            regime_key=f"cn:{session_date.isoformat()}:{key_suffix or regime}:market-regime-v1",
            region="cn",
            session_date=session_date,
            regime=regime,
            allocation_multiplier=1.0 if regime == "normal" else 0.5,
            entry_allowed=regime != "risk_off",
            freshness_expires_at=freshness_expires_at,
            policy_version="market-regime-v1",
            provenance_json=json.dumps({"source": "unit"}, ensure_ascii=False),
        )
        session.add(row)
        session.flush()
        return row.id


def test_friday_postclose_context_is_valid_until_next_actual_session_close(isolated_db):
    friday = date(2026, 9, 4)
    service = PaperTradingService(market_context_risk_control_enabled=True, market_context_lookback_days=1)

    with _calendar_patch():
        _add_regime(isolated_db, session_date=friday)
        with isolated_db.get_session() as session:
            monday_before_close = service._market_context_execution_risk_in_session(
                session,
                market="cn",
                current_time=datetime(2026, 9, 7, 6, 59, tzinfo=timezone.utc),
            )
            monday_at_close = service._market_context_execution_risk_in_session(
                session,
                market="cn",
                current_time=datetime(2026, 9, 7, 7, 0, tzinfo=timezone.utc),
            )

    assert monday_before_close["block_new_risk"] is False
    assert monday_before_close["completed_session_lag"] == 0
    assert monday_before_close["freshness_expires_at"] == "2026-09-07T07:00:00"
    assert monday_at_close["block_new_risk"] is True
    assert "market_context_stale" in monday_at_close["reasons"]


def test_legacy_same_day_midnight_expiry_is_recalculated_without_mutating_row(isolated_db):
    friday = date(2026, 9, 4)
    legacy_expiry = datetime(2026, 9, 4, 23, 59, 59)
    row_id = _add_regime(
        isolated_db,
        session_date=friday,
        freshness_expires_at=legacy_expiry,
    )
    service = PaperTradingService(market_context_risk_control_enabled=True, market_context_lookback_days=1)

    with _calendar_patch(), isolated_db.get_session() as session:
        snapshot = service._market_context_execution_risk_in_session(
            session,
            market="cn",
            current_time=datetime(2026, 9, 7, 6, 59, tzinfo=timezone.utc),
        )
    with isolated_db.get_session() as session:
        stored = session.get(DailyMarketRegime, row_id)

    assert snapshot["block_new_risk"] is False
    assert snapshot["freshness_expires_at"] == "2026-09-07T07:00:00"
    assert stored.freshness_expires_at == legacy_expiry


def test_newer_regime_takes_precedence_over_prior_session(isolated_db):
    service = PaperTradingService(market_context_risk_control_enabled=True, market_context_lookback_days=1)

    with _calendar_patch():
        _add_regime(isolated_db, session_date=date(2026, 9, 4), regime="normal", key_suffix="old-normal")
        _add_regime(isolated_db, session_date=date(2026, 9, 7), regime="cautious", key_suffix="new-cautious")
        with isolated_db.get_session() as session:
            snapshot = service._market_context_execution_risk_in_session(
                session,
                market="cn",
                current_time=datetime(2026, 9, 7, 6, 59, tzinfo=timezone.utc),
            )

    assert snapshot["session_date"] == "2026-09-07"
    assert snapshot["regime"] == "cautious"
    assert snapshot["block_new_risk"] is False
    assert snapshot["allocation_multiplier"] == pytest.approx(0.5)


def test_stale_beyond_completed_trading_sessions_blocks_entry(isolated_db):
    service = PaperTradingService(market_context_risk_control_enabled=True, market_context_lookback_days=2)

    with _calendar_patch():
        _add_regime(isolated_db, session_date=date(2026, 9, 4))
        with isolated_db.get_session() as session:
            snapshot = service._market_context_execution_risk_in_session(
                session,
                market="cn",
                current_time=datetime(2026, 9, 8, 7, 1, tzinfo=timezone.utc),
            )

    assert snapshot["completed_session_lag"] == 2
    assert snapshot["block_new_risk"] is True
    assert "market_context_stale" in snapshot["reasons"]


def test_missing_calendar_fails_closed_without_natural_day_fallback(isolated_db):
    _add_regime(isolated_db, session_date=date(2026, 9, 4))
    service = PaperTradingService(market_context_risk_control_enabled=True, market_context_lookback_days=3)

    with patch("src.services.daily_market_context._market_context_calendar", return_value=None), isolated_db.get_session() as session:
        snapshot = service._market_context_execution_risk_in_session(
            session,
            market="cn",
            current_time=datetime(2026, 9, 7, 6, 59, tzinfo=timezone.utc),
        )

    assert snapshot["block_new_risk"] is True
    assert snapshot["allocation_multiplier"] == pytest.approx(0.0)
    assert "market_context_stale" in snapshot["reasons"]
    assert "market_context_calendar_unavailable" in snapshot["reasons"]
    assert snapshot["freshness_expires_at"] is None


def test_generated_market_context_persists_regime_with_real_history_id(isolated_db):
    query_id = "market_context_entry-q_cn"
    payload = {
        "kind": "market_review",
        "region": "cn",
        "market_regime": "normal",
        "summary": "结构健康，允许正常试仓。",
    }
    with isolated_db.session_scope() as session:
        history = AnalysisHistory(
            query_id=query_id,
            code="MARKET",
            report_type="market_review",
            analysis_summary="结构健康，允许正常试仓。",
            news_content="结构健康，允许正常试仓。",
            raw_result=json.dumps({"raw_response": "结构健康，允许正常试仓。"}, ensure_ascii=False),
            context_snapshot=json.dumps(
                {
                    "report_kind": "market_review",
                    "market_review_region": "cn",
                    "market_review_payload": payload,
                    "report_language": "zh",
                },
                ensure_ascii=False,
            ),
            created_at=datetime(2026, 9, 4, 16, 0),
        )
        session.add(history)
        session.flush()
        history_id = history.id

    service = DailyMarketContextService(
        db_manager=isolated_db,
        today_fn=lambda: date(2026, 9, 4),
    )
    result = MarketReviewRunResult(
        report="结构健康，允许正常试仓。",
        market_review_payload=payload,
    )

    with _calendar_patch(), patch(
        "src.services.daily_market_context.run_market_review",
        return_value=result,
    ):
        context = service.get_context(
            region="cn",
            config=SimpleNamespace(report_language="zh"),
            notifier=MagicMock(),
            analyzer=MagicMock(),
            search_service=MagicMock(),
            force_refresh=True,
            current_query_id="entry-q",
        )

    with isolated_db.get_session() as session:
        regime = session.execute(select(DailyMarketRegime)).scalar_one()

    assert context is not None
    assert regime.source_report_id == history_id
    assert regime.source_query_id == "entry-q"
    assert regime.freshness_expires_at == datetime(2026, 9, 7, 7, 0)
