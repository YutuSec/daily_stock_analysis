# -*- coding: utf-8 -*-
"""Focused REQ-001/REQ-002 checks using a real temporary SQLite database."""

from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Optional
from unittest.mock import patch

import pytest
from sqlalchemy import select, text

from src.config import Config
from src.services.paper_trading_service import (
    PaperTradingConflictError,
    PaperTradingExecutionNotAuthorizedError,
    PaperTradingService,
    TRADING_STYLE_POLICY_VERSION,
    TRADING_STYLE_SETTINGS,
)
from src.services.portfolio_service import PortfolioService
from data_provider.realtime_types import UnifiedAuctionSnapshot
from src.storage import (
    AnalysisHistory,
    DailyMarketRegime,
    DatabaseManager,
    DecisionSignalRecord,
    PaperCapitalAccount,
    PaperCapitalAdjustment,
    PaperExecutionPlan,
    PaperNotificationDispatch,
    PaperAccountRiskEvent,
    PaperAccountRiskSnapshot,
    PaperPlanObservation,
    PaperPlanEvaluation,
    PaperPosition,
    PaperReanalysisRequest,
    ReportPlanReconciliation,
    PaperResolution,
    PaperRiskRevision,
    PaperSignalConsumption,
    PaperStrategyThesis,
    PaperStrategyTransition,
    PaperSyncLink,
    PaperTradeEvent,
    PortfolioDailySnapshot,
    PortfolioPosition,
    PortfolioTrade,
    StockDaily,
    utc_naive_now,
)
from src.repositories.paper_trading_repo import PaperTradingRepository
from tests.report_intent_helpers import add_report_execution_intent


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    os.environ["DATABASE_PATH"] = str(tmp_path / "paper-trading.db")
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


def _frozen_style_metadata(style="position", *, bars_observed=None, history_sufficient=True, support=98.0):
    settings = dict(TRADING_STYLE_SETTINGS[style])
    bars = int(bars_observed if bars_observed is not None else settings["analysis_window_sessions"])
    style_context = {
        "trading_style": style,
        "policy_version": TRADING_STYLE_POLICY_VERSION,
        "source": "unit_frozen_report",
        "matched_override_key": None,
        "market": "cn",
        "stock_code": "600519",
        "settings": settings,
    }
    indicators = {
        "trading_style": style,
        "policy_version": TRADING_STYLE_POLICY_VERSION,
        "analysis_window_sessions": settings["analysis_window_sessions"],
        "bars_observed": bars,
        "history_sufficient": bool(history_sufficient),
        "core_ma_windows": list(settings.get("core_ma_windows") or []),
        "volatility_windows": list(settings.get("volatility_windows") or []),
        "moving_averages": {"ma20": support} if support is not None else {},
        "volatility": {},
        "support": support,
        "support_basis": "nearest_frozen_style_ma_at_or_below_close" if support is not None else None,
    }
    return {
        "paper_trading_style_context": style_context,
        "paper_trading_style_indicators": indicators,
        "paper_trading_style": style,
        "paper_trading_style_policy_version": TRADING_STYLE_POLICY_VERSION,
        "paper_trading_style_settings": settings,
        "paper_trading_style_evidence": {
            "source": "analysis_context_snapshot",
            "style_context": style_context,
            "indicator_snapshot": indicators,
            "trading_style": style,
            "policy_version": TRADING_STYLE_POLICY_VERSION,
            "settings": settings,
            "bars_observed": bars,
            "analysis_window_sessions": settings["analysis_window_sessions"],
            "history_sufficient": bool(history_sufficient),
            "frozen_at_source_report_save": True,
        },
    }


def _create_plan_with_report_clock(service, report_id, *args, **kwargs):
    signal_id = kwargs.get("decision_signal_id")
    with service.db.get_session() as session:
        report = session.get(AnalysisHistory, int(report_id))
        frozen_now = (
            report.created_at
            if report is not None and isinstance(report.created_at, datetime)
            else datetime(2026, 8, 3, 15, 30)
        )
        if signal_id is not None:
            signal = session.get(DecisionSignalRecord, int(signal_id))
            if signal is not None and isinstance(signal.created_at, datetime):
                frozen_now = max(frozen_now, signal.created_at)
    with patch("src.services.paper_trading_service.utc_naive_now", return_value=frozen_now):
        return service.create_plan_for_report(report_id, *args, **kwargs)


def _single_confirmation_service(**kwargs):
    """Legacy execution tests opt into one-session thesis confirmation explicitly."""

    kwargs.setdefault("strategy_entry_confirmation_sessions", 1)
    service = PaperTradingService(**kwargs)
    original_create_plan = service.create_plan_for_report

    def create_plan_for_report_with_fixture_clock(report_id, *args, **inner_kwargs):
        return _create_plan_with_report_clock(
            SimpleNamespace(db=service.db, create_plan_for_report=original_create_plan),
            report_id,
            *args,
            **inner_kwargs,
        )

    service.create_plan_for_report = create_plan_for_report_with_fixture_clock
    return service


def _strategy_setup_conditions(setup_type="trend_continuation"):
    return [{
        "source": "paper_trading.strategy_setup",
        "schema_version": "paper-strategy-policy-v1",
        "reason": "execution_policy",
        "setup_type": setup_type,
        "setup": {
            "setup_type": setup_type,
            "source": "explicit_structured_report",
            "policy_version": "paper-strategy-policy-v1",
        },
        "text": f"setup={setup_type}",
    }]


def _source(
    db, *, created_on=date(2026, 8, 3), watch=None, market="cn",
    report_code="600519", signal_code="600519", model="model-a",
    raw_result=None,
    execution_intent: Optional[str] = "paper_candidate",
    execution_scope: str = "primary",
    **signal_overrides,
):
    """Seed one report and signal; ``execution_intent=None`` leaves the purpose undeclared."""

    with db.session_scope() as session:
        effective_raw = raw_result or {"model_used": model, "dashboard": {}}
        if isinstance(effective_raw, dict):
            paper_contract = dict(effective_raw.get("paper_trading") or {})
            paper_contract.setdefault("setup_type", "trend_continuation")
            effective_raw = {**effective_raw, "paper_trading": paper_contract}
        report = AnalysisHistory(
            query_id=f"paper-{created_on.isoformat()}",
            code=report_code,
            name="贵州茅台",
            report_type="full",
            model_used=model,
            operation_advice="买入",
            raw_result=json.dumps(effective_raw, ensure_ascii=False),
            ideal_buy=100.0,
            secondary_buy=96.0,
            stop_loss=95.0,
            take_profit=130.0,
            created_at=datetime.combine(created_on, datetime.min.time()),
        )
        session.add(report)
        session.flush()
        if execution_intent is not None:
            add_report_execution_intent(
                session,
                report.id,
                execution_intent=execution_intent,
                execution_scope=execution_scope,
            )
        action_value = str(signal_overrides.get("action", "buy"))
        default_score = {
            "buy": 65,
            "add": 65,
            "watch": 50,
            "hold": 50,
            "reduce": 35,
            "sell": 10,
            "avoid": 35,
        }.get(action_value, 65)
        holding_state = "holding" if action_value in {"hold", "reduce", "sell"} else "empty"
        fixture_support = signal_overrides.get("entry_low")
        try:
            fixture_support = float(fixture_support) if fixture_support is not None else 98.0
        except (TypeError, ValueError):
            fixture_support = 98.0
        if not math.isfinite(fixture_support) or fixture_support <= 0:
            fixture_support = 98.0
        default_metadata = {
            "holding_state": holding_state,
            "base_action": action_value,
            "effective_action_source": f"position_actions.{holding_state}",
            "position_actions": {
                "empty": {"action": action_value, "reason": "unit-test empty action snapshot"},
                "holding": {"action": action_value, "reason": "unit-test holding action snapshot"},
            },
            **_frozen_style_metadata("position", support=fixture_support),
        }
        fields = dict(
            stock_code=signal_code,
            stock_name="贵州茅台",
            market=market,
            source_type="analysis",
            source_report_id=report.id,
            source_agent=model,
            trace_id=f"trace-{report.id}",
            trigger_source="system",
            action="buy",
            horizon="10d",
            entry_low=96.0,
            entry_high=100.0,
            stop_loss=95.0,
            target_price=130.0,
            watch_conditions=json.dumps(watch or ["放量"], ensure_ascii=False),
            confidence=0.8,
            score=default_score,
            plan_quality="partial",
            status="active",
            metadata_json=json.dumps(default_metadata, ensure_ascii=False),
            created_at=datetime.combine(created_on, datetime.min.time()),
        )
        fields.update(signal_overrides)
        signal = DecisionSignalRecord(**fields)
        session.add(signal)
        session.flush()
        return report.id, signal.id


def test_report_plan_reconciliation_retry_claim_is_atomic_for_two_workers(isolated_db):
    report_id, _ = _source(isolated_db, model="model-a")
    plan_version = "paper-v2"
    with isolated_db.session_scope() as session:
        session.add(ReportPlanReconciliation(
            reconciliation_key=f"analysis:{report_id}:{plan_version}",
            source_report_id=report_id,
            plan_version=plan_version,
            source_model="model-a",
            status="retry_wait",
            attempt_count=1,
            max_attempts=3,
            next_retry_at=utc_naive_now() - timedelta(minutes=1),
            details_json="{}",
        ))

    repo = PaperTradingRepository(db_manager=isolated_db)
    report_stub = SimpleNamespace(id=report_id, model_used="model-a")

    def claim_once():
        return repo.claim_report_plan_reconciliation(
            report=report_stub,
            plan_version=plan_version,
            max_attempts=3,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: claim_once(), range(2)))

    claimed = [item for item in results if item is not None]
    assert len(claimed) == 1
    assert claimed[0].attempt_count == 2
    with isolated_db.get_session() as session:
        row = session.execute(select(ReportPlanReconciliation)).scalar_one()
        assert row.status == "processing"
        assert row.attempt_count == 2


def test_due_reconciliation_retry_is_not_starved_by_terminal_no_plan_rows(isolated_db):
    plan_version = "paper-v2"
    terminal_ids = []
    for idx in range(5):
        report_id, _ = _source(isolated_db, created_on=date(2026, 8, 10 + idx), model="model-a")
        terminal_ids.append(report_id)
        with isolated_db.session_scope() as session:
            session.add(ReportPlanReconciliation(
                reconciliation_key=f"analysis:{report_id}:{plan_version}",
                source_report_id=report_id,
                plan_version=plan_version,
                source_model="model-a",
                status="terminal",
                classification="failed_analysis",
                attempt_count=1,
                max_attempts=3,
                details_json="{}",
            ))
    retry_report_id, _ = _source(isolated_db, created_on=date(2026, 8, 3), model="model-a")
    with isolated_db.session_scope() as session:
        session.add(ReportPlanReconciliation(
            reconciliation_key=f"analysis:{retry_report_id}:{plan_version}",
            source_report_id=retry_report_id,
            plan_version=plan_version,
            source_model="model-a",
            status="retry_wait",
            classification="transient_error",
            attempt_count=1,
            max_attempts=3,
            next_retry_at=utc_naive_now() - timedelta(minutes=5),
            last_error="temporary",
            details_json="{}",
        ))

    rows = PaperTradingRepository(db_manager=isolated_db).list_due_report_plan_reconciliations(
        plan_version=plan_version,
        limit=3,
    )
    ids = [int(row.id) for row in rows]

    assert retry_report_id in ids
    assert not (set(terminal_ids) & set(ids))


def _bars(start=date(2026, 8, 3), *, days=8, current_volume=151.0):
    rows = []
    total_days = max(days, TRADING_STYLE_SETTINGS["position"]["analysis_window_sessions"])
    first_date = start - timedelta(days=total_days - days)
    for index in range(total_days):
        bar_date = first_date + timedelta(days=index)
        is_current = index == total_days - 1
        close = 100.0 if is_current else 99.0
        rows.append({
            "date": bar_date,
            "open": close,
            "high": close + 5.0,
            "low": close - 5.0,
            "close": close,
            "volume": current_volume if is_current else 100.0,
            "is_complete": True,
        })
    return rows


def _persist_daily_rows(db, bars, code="600519.SH"):
    rows = list(bars)
    if rows:
        existing_dates = {item["date"] for item in rows}
        last = dict(rows[-1])
        cursor = max(existing_dates) + timedelta(days=1)
        while cursor <= date(2026, 8, 7):
            if cursor.weekday() < 5 and cursor not in existing_dates:
                last = {**last, "date": cursor}
                rows.append(dict(last))
                existing_dates.add(cursor)
            cursor += timedelta(days=1)
    with db.session_scope() as session:
        for item in rows:
            session.add(StockDaily(
                code=code,
                date=item["date"],
                open=item["open"],
                high=item["high"],
                low=item["low"],
                close=item["close"],
                volume=item["volume"],
                data_source="paper-intraday-test|vol=shares",
            ))


def _portfolio_position_id_for(
    db,
    *,
    account_id: int,
    symbol: str = "SH600519",
    market: str = "cn",
) -> int:
    with db.get_session() as session:
        row = session.execute(
            select(PortfolioPosition)
            .where(
                PortfolioPosition.account_id == int(account_id),
                PortfolioPosition.symbol == symbol,
                PortfolioPosition.market == market,
            )
            .limit(1)
        ).scalar_one_or_none()
        assert row is not None
        return int(row.id)


def _quote(
    price=100.0,
    *,
    code="600519",
    market="cn",
    volume_ratio=1.8,
    change_pct=1.0,
    high=106.0,
    stale=False,
    provider_timestamp="2026-08-10T10:00:00+08:00",
    fetched_at="2026-08-10T10:00:03+08:00",
    stale_seconds="default",
):
    return SimpleNamespace(
        code=code,
        market=market,
        price=price,
        high=high,
        low=price - 2.0,
        volume=2000,
        volume_ratio=volume_ratio,
        change_pct=change_pct,
        pre_close=price - 1.0,
        source="unit_quote",
        provider_timestamp=provider_timestamp,
        fetched_at=fetched_at,
        is_stale=stale,
        stale_seconds=(3 if not stale else 3600) if stale_seconds == "default" else stale_seconds,
    )


def _phase():
    return SimpleNamespace(
        market="cn",
        phase="intraday",
        market_local_time=datetime(2026, 8, 10, 10, 0),
        effective_daily_bar_date=date(2026, 8, 7),
        is_market_open_now=True,
    )


def _closing_phase():
    item = _phase()
    item.phase = "closing_auction"
    return item


def _phase_closing_auction():
    item = _phase()
    item.phase = "closing_auction"
    item.is_market_open_now = True
    return item


class _QuoteManager:
    def __init__(self, quote):
        self.quote = quote

    def get_realtime_quote(self, *_args, **_kwargs):
        return self.quote


class _AuctionManager(_QuoteManager):
    def __init__(self, quote=None, auction=None):
        super().__init__(quote)
        self.auction = auction

    def get_auction_snapshot(self, *_args, **_kwargs):
        return self.auction


class _CountingTaskQueue:
    def __init__(self, *, task=None):
        self._lock = threading.Lock()
        self.submissions = []
        self.task = task

    def submit_task(self, *args, **kwargs):
        with self._lock:
            self.submissions.append({"args": args, "kwargs": kwargs})
            return SimpleNamespace(task_id=f"task-{len(self.submissions)}")

    def get_task(self, task_id):
        return self.task


def _auction_snapshot(**overrides):
    fields = dict(
        code="600519",
        market="cn",
        phase="opening_auction",
        virtual_price=105.0,
        matched_volume=1000.0,
        unmatched_volume=0.0,
        unmatched_side=None,
        source="unit_auction",
        provider_timestamp="2026-08-10T09:27:00+08:00",
        fetched_at="2026-08-10T09:27:03+08:00",
        stale_seconds=3,
        is_stale=False,
        provenance={
            "fields": {
                "virtual_price": "raw.virtual_price",
                "matched_volume": "raw.matched_volume",
                "provider_timestamp": "raw.provider_timestamp",
            }
        },
    )
    fields.update(overrides)
    return UnifiedAuctionSnapshot(**fields)


def test_plan_is_versioned_whitelist_only_attributed_and_idempotent(isolated_db):
    report_id, signal_id = _source(isolated_db)
    service = _single_confirmation_service()

    first = service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    second = service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    assert first["created"] is True
    assert second["created"] is False
    item = first["item"]
    assert item["plan_version"] == "paper-hybrid-v3"
    assert item["source_report_id"] == report_id
    assert item["decision_signal_id"] == signal_id
    assert item["source_model"] == "model-a"
    assert item["market"] == "cn" and item["stock_code"] == "600519"
    assert item["executable"] is True
    assert {rule["type"] for rule in item["rules"]} == {
        "close_between", "volume_ratio_gte_prior_avg"
    }
    volume_rule = next(rule for rule in item["rules"] if rule["type"].startswith("volume"))
    assert volume_rule["ratio"] == 1.5
    assert volume_rule["period"] == 5
    assert volume_rule["threshold_source"] == "policy_default"
    with isolated_db.get_session() as session:
        assert session.query(PaperExecutionPlan).count() == 1


def test_conditional_entry_signal_reaches_paper_plan_but_waits_for_two_report_confirmation(isolated_db):
    first_metadata = {
        "holding_state": "empty",
        "entry_mode": "conditional",
        **_frozen_style_metadata("position"),
    }
    first_report, first_signal = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        action="buy",
        metadata_json=json.dumps(first_metadata, ensure_ascii=False),
    )
    service = PaperTradingService()

    first = _create_plan_with_report_clock(
        service,
        first_report,
        decision_signal_id=first_signal,
    )["item"]

    assert first["action"] == "buy"
    assert first["executable"] is False
    assert first["status"] == "non_executable"
    assert any(
        "strategy_thesis_not_accepted" in condition.get("text", "")
        for condition in first["descriptive_conditions"]
        if condition.get("source") == "plan_validator"
    )

    second_metadata = {
        "holding_state": "empty",
        "entry_mode": "conditional",
        **_frozen_style_metadata("position"),
    }
    second_report, second_signal = _source(
        isolated_db,
        created_on=date(2026, 8, 12),
        action="buy",
        metadata_json=json.dumps(second_metadata, ensure_ascii=False),
    )
    second = _create_plan_with_report_clock(
        service,
        second_report,
        decision_signal_id=second_signal,
    )["item"]

    assert second["action"] == "buy"
    assert second["executable"] is True
    assert second["status"] == "active"
    with isolated_db.get_session() as session:
        signal = session.get(DecisionSignalRecord, first_signal)
        assert json.loads(signal.metadata_json)["entry_mode"] == "conditional"
        transitions = list(session.execute(
            select(PaperStrategyTransition).order_by(PaperStrategyTransition.id)
        ).scalars().all())
        assert transitions[-2].reason == "awaiting_entry_confirmation"
        assert transitions[-2].confirmation_count == 1
        assert transitions[-1].reason == "accepted_entry_thesis"
        assert transitions[-1].confirmation_count == 2


def test_legacy_watch_signal_remains_non_executable_for_paper_plan_consumer(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        action="watch",
        metadata_json=json.dumps({
            "holding_state": "empty",
            **_frozen_style_metadata("position"),
        }, ensure_ascii=False),
    )
    service = _single_confirmation_service()

    item = _create_plan_with_report_clock(
        service,
        report_id,
        decision_signal_id=signal_id,
    )["item"]

    assert item["action"] == "watch"
    assert item["executable"] is False
    assert item["status"] == "non_executable"
    assert any(
        "unsupported_or_non_bullish_action" in condition.get("text", "")
        for condition in item["descriptive_conditions"]
        if condition.get("source") == "plan_validator"
    )
    opened = service.run_eod(date(2026, 8, 4), {("cn", "600519"): _bars(start=date(2026, 7, 30), days=6)})
    assert opened["positions_opened"] == 0


@pytest.mark.parametrize(
    "overrides,metadata_patch,expected_reason",
    [
        ({"target_price": 101.0}, {}, "risk_reward_below_policy"),
        (
            {"entry_low": 99.0, "entry_high": 100.0, "stop_loss": 97.0},
            {"volatility": {"atr20": 5.0}, "support": 97.0},
            "stop_within_atr_noise",
        ),
    ],
)
def test_conditional_entry_signal_does_not_bypass_existing_plan_risk_gates(
    isolated_db,
    overrides,
    metadata_patch,
    expected_reason,
):
    metadata = {
        "holding_state": "empty",
        "entry_mode": "conditional",
        **_frozen_style_metadata("position", support=metadata_patch.get("support", 98.0)),
    }
    if metadata_patch.get("volatility"):
        metadata["paper_trading_style_indicators"]["volatility"] = dict(metadata_patch["volatility"])
        metadata["paper_trading_style_evidence"]["indicator_snapshot"]["volatility"] = dict(
            metadata_patch["volatility"]
        )
    report_id, signal_id = _source(
        isolated_db,
        action="buy",
        metadata_json=json.dumps(metadata, ensure_ascii=False),
        **overrides,
    )
    service = _single_confirmation_service()

    item = _create_plan_with_report_clock(
        service,
        report_id,
        decision_signal_id=signal_id,
    )["item"]

    assert item["action"] == "buy"
    assert item["executable"] is False
    assert item["status"] == "non_executable"
    assert any(
        expected_reason in condition.get("text", "")
        for condition in item["descriptive_conditions"]
        if condition.get("source") == "plan_validator"
    )


def test_plan_uses_frozen_signal_style_after_runtime_config_changes(isolated_db):
    metadata = {"holding_state": "empty", **_frozen_style_metadata("trend")}
    report_id, signal_id = _source(
        isolated_db,
        horizon="long",
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    service = _single_confirmation_service(
        trading_style="short_swing",
        trading_style_overrides={"cn:600519": "short_swing"},
    )

    item = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]

    assert item["executable"] is True
    assert item["trading_style"] == "trend"
    assert item["max_holding_sessions"] == 120
    style_entry = next(entry for entry in item["descriptive_conditions"] if entry.get("source") == "paper_trading.trading_style")
    assert style_entry["evidence"]["source"] == "unit_frozen_report"
    assert style_entry["evidence"]["frozen_evidence_valid"] is True
    assert style_entry["settings"]["analysis_window_sessions"] == 500
    assert style_entry["settings"]["core_ma_windows"] == [20, 60, 120, 250]
    assert style_entry["settings"]["plan_refresh_cadence_sessions"] == 5
    cadence_entry = next(entry for entry in item["descriptive_conditions"] if entry.get("source") == "paper_trading.next_check_obligation")
    assert cadence_entry["obligation"]["source"] == "trading_style_plan_refresh_cadence"
    assert cadence_entry["obligation"]["plan_refresh_cadence_sessions"] == 5
    with isolated_db.get_session() as session:
        signal = session.get(DecisionSignalRecord, signal_id)
        metadata = json.loads(signal.metadata_json)
        assert metadata["paper_trading_style"] == "trend"
        assert metadata["paper_trading_style_settings"]["max_holding_sessions"] == 180
        assert metadata["paper_trading_style_settings"]["analysis_window_sessions"] == 500


@pytest.mark.parametrize("style,horizon,expected_sessions", [
    ("short_swing", "10d", 10),
    ("position", "swing", 40),
    ("trend", "long", 120),
])
def test_defaulted_style_horizon_uses_style_default_holding_duration(
    isolated_db,
    style,
    horizon,
    expected_sessions,
):
    metadata = {
        "holding_state": "empty",
        "horizon_source": "paper_trading_style_default",
        "paper_trading_horizon_source": "paper_trading_style_default",
        **_frozen_style_metadata(style),
    }
    report_id, signal_id = _source(
        isolated_db,
        horizon=horizon,
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )

    item = _single_confirmation_service().create_plan_for_report(
        report_id,
        decision_signal_id=signal_id,
    )["item"]

    assert item["executable"] is True
    assert item["trading_style"] == style
    assert item["max_holding_sessions"] == expected_sessions
    style_entry = next(
        entry
        for entry in item["descriptive_conditions"]
        if entry.get("source") == "paper_trading.trading_style"
    )
    assert style_entry["holding_policy"]["duration_source"] == "style_default_holding_sessions"
    assert style_entry["holding_policy"]["applied_sessions"] == expected_sessions


@pytest.mark.parametrize("horizon", ["10d", "swing"])
def test_explicit_trend_horizon_below_trend_window_fails_closed(isolated_db, horizon):
    metadata = {"holding_state": "empty", **_frozen_style_metadata("trend")}
    report_id, signal_id = _source(
        isolated_db,
        horizon=horizon,
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )

    item = _single_confirmation_service().create_plan_for_report(
        report_id,
        decision_signal_id=signal_id,
    )["item"]

    assert item["executable"] is False
    assert item["status"] == "non_executable"
    assert item["trading_style"] == "trend"
    assert any(
        "incompatible_report_horizon_for_trading_style" in entry.get("text", "")
        for entry in item["descriptive_conditions"]
        if entry.get("source") == "plan_validator"
    )


def test_incompatible_trading_style_horizon_fails_closed(isolated_db):
    metadata = {"holding_state": "empty", **_frozen_style_metadata("short_swing")}
    report_id, signal_id = _source(
        isolated_db,
        horizon="long",
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    service = _single_confirmation_service(trading_style="position")

    item = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]

    assert item["executable"] is False
    assert item["status"] == "non_executable"
    assert any(
        "incompatible_report_horizon_for_trading_style" in entry.get("text", "")
        for entry in item["descriptive_conditions"]
        if entry.get("source") == "plan_validator"
    )


def test_missing_frozen_style_evidence_fails_closed_for_legacy_report(isolated_db):
    legacy_metadata = {
        "holding_state": "empty",
        "base_action": "buy",
        "effective_action_source": "position_actions.empty",
    }
    report_id, signal_id = _source(
        isolated_db,
        metadata_json=json.dumps(legacy_metadata, ensure_ascii=False),
    )

    item = _single_confirmation_service().create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]

    assert item["executable"] is False
    assert item["status"] == "non_executable"
    assert item["trading_style"] == "position"
    assert any(
        "missing_frozen_style_evidence" in entry.get("text", "")
        for entry in item["descriptive_conditions"]
        if entry.get("source") == "plan_validator"
    )


@pytest.mark.parametrize("style,required", [
    ("short_swing", 80),
    ("position", 180),
    ("trend", 500),
])
def test_insufficient_frozen_style_evidence_fails_closed(isolated_db, style, required):
    metadata = {
        "holding_state": "empty",
        **_frozen_style_metadata(style, bars_observed=required - 1, history_sufficient=False),
    }
    report_id, signal_id = _source(
        isolated_db,
        horizon="5d" if style != "trend" else "long",
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )

    item = _single_confirmation_service().create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]

    assert item["executable"] is False
    assert item["status"] == "non_executable"
    assert item["trading_style"] == style
    assert any(
        "insufficient_frozen_style_evidence" in entry.get("text", "")
        for entry in item["descriptive_conditions"]
        if entry.get("source") == "plan_validator"
    )


def test_opened_lot_inherits_frozen_trading_style_from_plan(isolated_db):
    metadata = {
        "holding_state": "empty",
        "horizon_source": "paper_trading_style_default",
        "paper_trading_horizon_source": "paper_trading_style_default",
        **_frozen_style_metadata("trend"),
    }
    report_id, signal_id = _source(
        isolated_db,
        horizon="long",
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    service = _single_confirmation_service(
        trading_style="position",
        trading_style_overrides={"cn:600519": "trend"},
        commission_rate=0.0,
        min_commission=0.0,
        slippage_pct=0.0,
    )
    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    bars = _bars(start=date(2026, 7, 30), days=6, current_volume=151.0)

    opened = service.run_eod(date(2026, 8, 4), {("cn", "600519"): bars})

    assert opened["positions_opened"] == 1
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition).where(PaperPosition.plan_id == plan["id"])).scalar_one()
        assert position.trading_style == "trend"
        assert position.trading_style_policy_version == "trading-style-v1"
        settings = json.loads(position.trading_style_settings_json)
        assert settings["trading_style"] == "trend"
        assert settings["settings"]["max_holding_sessions"] == 180
        assert settings["settings"]["add_limit"] == 2
        assert position.max_holding_sessions == 120


def test_eod_entry_sizing_includes_fee_slippage_before_debit(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 3))
    service = _single_confirmation_service(
        slippage_pct=0.03,
        commission_rate=0.0003,
        min_commission=5.0,
    )
    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]

    opened = service.run_eod(date(2026, 8, 4), {("cn", "600519"): _bars(start=date(2026, 7, 30), days=6, current_volume=151.0)})

    assert opened["positions_opened"] == 1, opened
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition).where(PaperPosition.plan_id == plan["id"])).scalar_one()
        account = session.execute(select(PaperCapitalAccount)).scalar_one()
        event = session.execute(select(PaperTradeEvent).where(PaperTradeEvent.event_type == "open")).scalar_one()
        position_cap = 100000.0 * 0.20
        assert position.quantity == pytest.approx(100)
        assert position.allocated_capital == pytest.approx(event.net_amount)
        assert position.allocated_capital <= position_cap
        assert account.cash >= 0
        assert account.exposure <= position_cap
        assert event.net_amount == pytest.approx(10008.0009)


def test_explicit_long_trend_new_plan_uses_style_default_holding_duration(isolated_db):
    metadata = {"holding_state": "empty", **_frozen_style_metadata("trend")}
    report_id, signal_id = _source(
        isolated_db,
        horizon="long",
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    service = _single_confirmation_service(
        trading_style="position",
        commission_rate=0.0,
        min_commission=0.0,
        slippage_pct=0.0,
    )
    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]

    assert plan["executable"] is True
    assert plan["trading_style"] == "trend"
    assert plan["max_holding_sessions"] == 120

    opened = service.run_eod(date(2026, 8, 4), {("cn", "600519"): _bars(start=date(2026, 7, 30), days=6, current_volume=151.0)})

    assert opened["positions_opened"] == 1
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition).where(PaperPosition.plan_id == plan["id"])).scalar_one()
        assert position.trading_style == "trend"
        assert position.max_holding_sessions == 120


def test_existing_frozen_trend_records_keep_persisted_literal_horizon(isolated_db):
    service = _single_confirmation_service(trading_style="trend")
    with isolated_db.session_scope() as session:
        report = AnalysisHistory(
            query_id="old-frozen-trend",
            code="600519",
            name="贵州茅台",
            report_type="full",
            model_used="model-a",
            operation_advice="买入",
            raw_result=json.dumps({"model_used": "model-a"}, ensure_ascii=False),
            ideal_buy=100.0,
            secondary_buy=96.0,
            stop_loss=95.0,
            take_profit=130.0,
            created_at=datetime(2026, 8, 3, 15, 30),
        )
        session.add(report)
        session.flush()
        add_report_execution_intent(session, report.id)
        style_evidence = _frozen_style_metadata("trend")["paper_trading_style_evidence"]
        plan = PaperExecutionPlan(
            plan_key="old-frozen-trend-plan",
            plan_version="paper-hybrid-v3",
            source_report_id=report.id,
            decision_signal_id=None,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            report_date=date(2026, 8, 3),
            report_created_at=datetime(2026, 8, 3, 15, 30),
            valid_for_date=date(2026, 8, 4),
            action="buy",
            executable=True,
            status="triggered",
            rules_json="[]",
            descriptive_conditions_json="[]",
            stop_loss=95.0,
            target_price=130.0,
            max_holding_sessions=60,
            trading_style="trend",
            trading_style_policy_version=TRADING_STYLE_POLICY_VERSION,
            trading_style_settings_json=json.dumps(style_evidence, ensure_ascii=False),
        )
        session.add(plan)
        session.flush()
        position = PaperPosition(
            plan_id=plan.id,
            source_report_id=report.id,
            decision_signal_id=None,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            status="open",
            opened_on=date(2026, 8, 4),
            opened_at=datetime(2026, 8, 4, 7, 0),
            execution_index=1,
            opened_price=100.0,
            normalized_notional=10000.0,
            quantity=100.0,
            initial_quantity=100.0,
            max_holding_sessions=60,
            trading_style="trend",
            trading_style_policy_version=TRADING_STYLE_POLICY_VERSION,
            trading_style_settings_json=json.dumps(style_evidence, ensure_ascii=False),
        )
        session.add(position)
        session.flush()
        plan_id = plan.id

    listed_plan = service.list_plans(model="model-a")["items"][0]
    listed_position = service.list_positions(model="model-a", status="open")["items"][0]

    assert listed_plan["id"] == plan_id
    assert listed_plan["trading_style"] == "trend"
    assert listed_plan["max_holding_sessions"] == 60
    assert listed_position["trading_style"] == "trend"
    assert listed_position["max_holding_sessions"] == 60
    with isolated_db.get_session() as session:
        persisted_plan = session.get(PaperExecutionPlan, plan_id)
        assert persisted_plan.max_holding_sessions == 60


def test_explicit_position_swing_new_plan_uses_style_default_holding_duration(isolated_db):
    metadata = {"holding_state": "empty", **_frozen_style_metadata("position")}
    report_id, signal_id = _source(
        isolated_db,
        horizon="swing",
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    plan = _single_confirmation_service().create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]

    assert plan["executable"] is True
    assert plan["trading_style"] == "position"
    assert plan["max_holding_sessions"] == 40


def test_explicit_position_numeric_10d_keeps_numeric_duration(isolated_db):
    metadata = {"holding_state": "empty", **_frozen_style_metadata("position")}
    report_id, signal_id = _source(
        isolated_db,
        horizon="10d",
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    plan = _single_confirmation_service().create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]

    assert plan["executable"] is True
    assert plan["trading_style"] == "position"
    assert plan["max_holding_sessions"] == 10


def test_plan_position_and_operation_serializers_expose_frozen_style_and_market_regime(isolated_db):
    metadata = {"holding_state": "empty", **_frozen_style_metadata("short_swing")}
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 3),
        horizon="intraday",
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    with isolated_db.session_scope() as session:
        session.add(DailyMarketRegime(
            regime_key="cn:2026-08-04:serializer:market-regime-v1",
            region="cn",
            session_date=date(2026, 8, 4),
            regime="cautious",
            allocation_multiplier=0.5,
            entry_allowed=True,
            freshness_expires_at=datetime(2026, 8, 4, 23, 59),
            policy_version="market-regime-v1",
            source_report_id=report_id,
            source_query_id="daily-market-serializer",
            provenance_json=json.dumps({"source": "unit-regime", "parser": "structured"}, ensure_ascii=False),
        ))
    service = _single_confirmation_service(
        trading_style="trend",
        market_context_risk_control_enabled=False,
        commission_rate=0.0,
        min_commission=0.0,
        slippage_pct=0.0,
    )

    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    opened = service.run_eod(date(2026, 8, 4), {("cn", "600519"): _bars(start=date(2026, 7, 28), days=8)})

    assert plan["trading_style"] == "short_swing"
    assert plan["trading_style_policy_version"] == TRADING_STYLE_POLICY_VERSION
    assert plan["trading_style_settings"]["analysis_window_sessions"] == 80
    assert plan["trading_style_evidence"]["source"] == "unit_frozen_report"
    assert plan["trading_style_evidence"]["frozen_evidence_valid"] is True
    assert opened["positions_opened"] == 1

    listed_plan = service.list_plans(model="model-a")["items"][0]
    assert listed_plan["trading_style"] == "short_swing"
    assert listed_plan["style_history_evidence"]["analysis_window_sessions"] == 80
    assert listed_plan["style_history_evidence"]["bars_observed"] == 80
    assert listed_plan["style_history_evidence"]["history_sufficient"] is True
    assert listed_plan["market_regime"] == "cautious"
    assert listed_plan["market_regime_policy_version"] == "market-regime-v1"
    assert listed_plan["market_regime_context"]["entry_allowed"] is True
    assert listed_plan["market_regime_context"]["allocation_multiplier"] == pytest.approx(0.5)
    assert listed_plan["market_regime_context"]["provenance"]["source"] == "unit-regime"

    position = service.list_positions(status="open")["items"][0]
    assert position["trading_style"] == "short_swing"
    assert position["trading_style_policy_version"] == TRADING_STYLE_POLICY_VERSION
    assert position["trading_style_settings"]["analysis_window_sessions"] == 80
    assert position["style_history_evidence"]["bars_observed"] == 80
    assert position["market_regime"] == "cautious"
    assert position["market_regime_context"]["source_query_id"] == "daily-market-serializer"

    operation = service.list_operations(model="model-a")["items"][0]
    assert operation["trading_style"] == "short_swing"
    assert operation["market_regime"] == "cautious"
    assert operation["strategy_thesis_raw_action"] == "buy"
    assert operation["strategy_thesis_effective_action"] == "buy"
    assert operation["strategy_thesis_entry_permitted"] is True
    assert operation["strategy_thesis_confirmation_count"] == 1
    assert operation["strategy_thesis_confirmation_required"] == 1
    assert operation["open_lots"][0]["trading_style"] == "short_swing"
    assert operation["open_lots"][0]["market_regime_context"]["provenance"]["parser"] == "structured"


def test_trading_style_override_changes_monitoring_history_lookback(isolated_db):
    metadata = {"holding_state": "empty", **_frozen_style_metadata("trend")}
    report_id, signal_id = _source(
        isolated_db,
        horizon="long",
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    service = _single_confirmation_service(
        trading_style="short_swing",
        trading_style_overrides={"cn:600519": "trend"},
    )
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    seen_limits = []

    def fake_daily_bars(market, stock_code, eval_date, *, limit):
        seen_limits.append(limit)
        return []

    service.repo.daily_bars = fake_daily_bars
    service._latest_proven_complete_date = lambda market, now: date(2026, 8, 4)

    service.run_monitor(current_time=datetime(2026, 8, 4, 8, 0, tzinfo=timezone.utc))

    assert seen_limits
    assert seen_limits[0] == 500


def test_empty_position_effective_watch_cannot_create_phantom_reduce_trade(isolated_db):
    metadata = {
        "holding_state": "empty",
        "base_action": "reduce",
        "effective_action_source": "position_actions.empty",
        "position_actions": {
            "empty": {"action": "watch"},
            "holding": {"action": "reduce"},
        },
    }
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 27),
        report_code="002173.SZ",
        signal_code="002173",
        raw_result={
            "model_used": "model-a",
            "holding_state": "empty",
            "action": "reduce",
            "position_actions": metadata["position_actions"],
            "dashboard": {},
        },
        action="watch",
        score=37,
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )

    item = _single_confirmation_service().create_plan_for_report(
        report_id,
        decision_signal_id=signal_id,
    )["item"]

    assert item["action"] == "watch"
    assert item["executable"] is False
    assert item["status"] == "non_executable"
    assert _single_confirmation_service().list_positions()["total"] == 0
    assert _single_confirmation_service().list_events()["total"] == 0


def test_no_signal_plan_fallback_resolves_frozen_empty_branch(isolated_db):
    position_actions = {
        "empty": {"action": "watch", "advice": "空仓观望"},
        "holding": {"action": "reduce", "advice": "持仓减仓"},
    }
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 27),
        report_code="002173.SZ",
        signal_code="002173",
        raw_result={
            "model_used": "model-a",
            "sentiment_score": 37,
            "operation_advice": "减仓",
            "action": "reduce",
            "action_label": "减仓",
            "holding_state": "empty",
            "position_actions": position_actions,
            "dashboard": {},
        },
        action="watch",
        score=37,
    )
    with isolated_db.session_scope() as session:
        report = session.get(AnalysisHistory, report_id)
        report.operation_advice = "减仓"
        session.delete(session.get(DecisionSignalRecord, signal_id))

    item = _single_confirmation_service().create_plan_for_report(report_id)["item"]

    assert item["decision_signal_id"] is None
    assert item["action"] == "watch"
    assert item["executable"] is False
    assert item["status"] == "non_executable"
    assert _single_confirmation_service().list_positions()["total"] == 0
    assert _single_confirmation_service().list_events()["total"] == 0


def test_legacy_report_creates_single_entry_leg_and_operations_waiting_entry(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7))
    service = _single_confirmation_service()

    result = service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    item = result["item"]
    assert item["entry_leg_schema_version"] == "entry-legs-v1"
    assert [leg["leg_id"] for leg in item["entry_legs"]] == ["legacy_entry"]
    assert item["entry_legs"][0]["source"] == "legacy_ideal_secondary"
    operations = service.list_operations()
    assert operations["total"] == 1
    assert operations["items"][0]["stage"] == "waiting_entry"


def test_explicit_entry_legs_round_trip_and_invalid_fraction_fails_closed(isolated_db):
    raw_result = {
        "model_used": "model-a",
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "entry_leg_schema_version": "entry-legs-v1",
                    "entry_legs": [
                        {"leg_id": "initial", "role": "initial", "sequence": 1, "allocation_fraction": 0.6, "max_executions": 1, "price_rules": [{"type": "close_between", "low": 96, "high": 100}], "prerequisites": []},
                        {"leg_id": "add_breakout", "role": "breakout_add", "sequence": 2, "allocation_fraction": 0.4, "max_executions": 1, "price_rules": [{"type": "close_between", "low": 96, "high": 101}], "prerequisites": ["initial"]},
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
    service = _single_confirmation_service()

    item = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]

    assert [leg["leg_id"] for leg in item["entry_legs"]] == ["initial", "add_breakout"]
    assert sum(leg["allocation_fraction"] for leg in item["entry_legs"]) == pytest.approx(1.0)

    bad_raw = {
        "model_used": "model-a",
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "entry_leg_schema_version": "entry-legs-v1",
                    "entry_legs": [
                        {"leg_id": "a", "role": "initial", "allocation_fraction": 0.8, "price_rules": [{"type": "close_between", "low": 96, "high": 100}]},
                        {"leg_id": "b", "role": "add", "allocation_fraction": 0.8, "price_rules": [{"type": "close_between", "low": 96, "high": 100}]},
                    ],
                }
            }
        },
    }
    bad_report, bad_signal = _source(isolated_db, created_on=date(2026, 8, 8), raw_result=bad_raw)
    bad = service.create_plan_for_report(bad_report, decision_signal_id=bad_signal)["item"]
    assert bad["executable"] is False
    reasons = json.dumps(bad["descriptive_conditions"], ensure_ascii=False)
    assert "entry_leg_fractions_exceed_100pct" in reasons


def test_explicit_entry_leg_geometry_overrides_broad_signal_range_and_revalidates(isolated_db):
    raw_result = {
        "model_used": "model-a",
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "entry_leg_schema_version": "entry-legs-v1",
                    "entry_legs": [{
                        "leg_id": "initial",
                        "role": "initial",
                        "allocation_fraction": 1.0,
                        "price_rules": [{"type": "close_between", "low": 9.4, "high": 9.5}],
                    }],
                }
            }
        },
    }
    report_id, signal_id = _source(
        isolated_db,
        raw_result=raw_result,
        entry_low=9.23,
        entry_high=9.45,
        stop_loss=9.23,
        target_price=9.6,
    )
    service = _single_confirmation_service(intraday_min_risk_reward=0.5)
    with isolated_db.session_scope() as session:
        report = session.get(AnalysisHistory, report_id)
        signal = session.get(DecisionSignalRecord, signal_id)
        plan = PaperExecutionPlan(
            plan_key=f"analysis:{report_id}:signal:{signal_id}:paper-hybrid-v3",
            plan_version="paper-hybrid-v3",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            report_date=report.created_at.date(),
            report_created_at=report.created_at,
            valid_for_date=date(2026, 8, 4),
            action="buy",
            executable=False,
            status="non_executable",
            rules_json="[]",
            descriptive_conditions_json=json.dumps([{
                "source": "plan_validator",
                "reason": "fail_closed",
                "text": "stop_not_below_entry",
            }]),
            stop_loss=signal.stop_loss,
            target_price=signal.target_price,
            max_holding_sessions=5,
        )
        session.add(plan)

    first = service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    second = service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    assert first["created"] is False and first["revalidated"] is True
    assert first["item"]["executable"] is True
    assert first["item"]["status"] == "active"
    assert any(
        entry.get("source") == "plan_contract_revalidation"
        and entry.get("previous_status") == "non_executable"
        and entry.get("new_status") == "active"
        for entry in first["item"]["descriptive_conditions"]
    )
    assert second["created"] is False and second["revalidated"] is False
    with isolated_db.get_session() as session:
        assert session.query(PaperExecutionPlan).count() == 1
        assert session.query(PaperPosition).count() == 0
        assert session.query(PaperTradeEvent).count() == 0


def test_explicit_entry_leg_geometry_remains_fail_closed_when_unbounded(isolated_db):
    raw_result = {
        "model_used": "model-a",
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "entry_leg_schema_version": "entry-legs-v1",
                    "entry_legs": [{
                        "leg_id": "initial",
                        "role": "initial",
                        "allocation_fraction": 1.0,
                        "price_rules": [{"type": "close_gte", "value": 96.0}],
                    }],
                }
            }
        },
    }
    report_id, signal_id = _source(isolated_db, raw_result=raw_result)

    item = _single_confirmation_service().create_plan_for_report(
        report_id,
        decision_signal_id=signal_id,
    )["item"]

    assert item["executable"] is False
    assert any(
        "unbounded_explicit_entry_geometry" in entry["text"]
        for entry in item["descriptive_conditions"]
        if entry.get("source") == "plan_validator"
    )


def test_invalid_entry_stop_contract_requests_one_corrective_reanalysis(isolated_db):
    raw_result = {
        "model_used": "model-a",
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "entry_leg_schema_version": "entry-legs-v1",
                    "entry_legs": [
                        {
                            "leg_id": "initial",
                            "role": "initial",
                            "allocation_fraction": 1.0,
                            "price_rules": [{"type": "close_between", "low": 1.315, "high": 1.331}],
                        }
                    ],
                }
            }
        },
    }
    report_id, signal_id = _source(
        isolated_db,
        raw_result=raw_result,
        entry_low=1.315,
        entry_high=1.331,
        stop_loss=1.315,
        target_price=1.40,
    )
    service = _single_confirmation_service(intraday_min_risk_reward=0.1)
    queue = _CountingTaskQueue()

    with patch("src.services.task_queue.get_task_queue", return_value=queue):
        first = service.create_plan_for_report(report_id, decision_signal_id=signal_id)
        second = service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    assert first["item"]["executable"] is False
    assert first["item"]["status"] == "non_executable"
    assert first["item"]["stop_loss"] == pytest.approx(1.315)
    assert any(
        "stop_not_below_entry" in entry["text"]
        for entry in first["item"]["descriptive_conditions"]
        if entry.get("source") == "plan_validator"
    )
    assert second["created"] is False
    assert len(queue.submissions) == 1
    operations = service.list_operations(model="model-a")["items"]
    assert len(operations) == 1
    assert operations[0]["stage"] == "reanalysis_blocked"
    assert operations[0]["stop_loss"] == pytest.approx(1.315)
    assert any(
        "stop_not_below_entry" in entry["text"]
        for entry in operations[0]["descriptive_conditions"]
        if entry.get("source") == "plan_validator"
    )
    assert len(operations[0]["reanalysis_requests"]) == 1
    assert operations[0]["reanalysis_requests"][0]["status"] == "submitted"
    with isolated_db.session_scope() as session:
        requests = session.execute(select(PaperReanalysisRequest)).scalars().all()
        assert len(requests) == 1
        assert requests[0].trigger_type == "plan_contract_invalid_stop"
        assert requests[0].status == "submitted"
        assert requests[0].task_id == "task-1"
        evidence = json.loads(requests[0].evidence_json)
        assert evidence["policy"]["fail_closed"] is True
        assert evidence["policy"]["no_silent_price_adjustment"] is True
        assert "stop_not_below_entry" in evidence["reasons"]
        assert "stop_within_minimum_price_noise" in evidence["reasons"]
        requests[0].status = "failed"

    assert service.list_operations(model="model-a")["items"] == []


def test_low_risk_reward_contract_requests_one_corrective_reanalysis(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        entry_low=1.322,
        entry_high=1.338,
        stop_loss=1.283,
        target_price=1.35,
    )
    service = _single_confirmation_service(intraday_min_risk_reward=1.2)
    queue = _CountingTaskQueue()

    with patch("src.services.task_queue.get_task_queue", return_value=queue):
        first = service.create_plan_for_report(report_id, decision_signal_id=signal_id)
        second = service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    assert first["item"]["executable"] is False
    assert first["item"]["status"] == "non_executable"
    assert second["created"] is False
    assert len(queue.submissions) == 1
    operations = service.list_operations(model="model-a")["items"]
    assert operations[0]["stage"] == "reanalysis_blocked"
    assert operations[0]["reanalysis_requests"][0]["trigger_type"] == "plan_contract_low_payoff"


def test_low_risk_reward_watch_plan_does_not_request_corrective_reanalysis(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        entry_low=1.322,
        entry_high=1.338,
        stop_loss=1.283,
        target_price=1.35,
        action="watch",
        score=50,
    )
    service = _single_confirmation_service(intraday_min_risk_reward=1.2)
    queue = _CountingTaskQueue()

    with patch("src.services.task_queue.get_task_queue", return_value=queue):
        item = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]

    assert item["executable"] is False
    assert len(queue.submissions) == 0
    assert service.list_operations(model="model-a")["items"] == []


def test_invalid_contract_reanalysis_replacement_does_not_chain_requests(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        entry_low=1.322,
        entry_high=1.338,
        stop_loss=1.283,
        target_price=1.35,
    )
    service = _single_confirmation_service(intraday_min_risk_reward=1.2)
    queue = _CountingTaskQueue()
    with patch("src.services.task_queue.get_task_queue", return_value=queue):
        first = service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    source_plan_id = first["item"]["id"]

    replacement_raw = {
        "model_used": "model-a",
        "paper_trading": {"same_session_reanalysis": True, "source_plan_id": source_plan_id},
        "dashboard": {},
    }
    replacement_report_id, replacement_signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 10),
        raw_result=replacement_raw,
        entry_low=1.322,
        entry_high=1.338,
        stop_loss=1.283,
        target_price=1.35,
        market_phase="intraday",
        trigger_source="paper_trading_reanalysis",
        metadata_json=json.dumps({
            "holding_state": "empty",
            "base_action": "buy",
            "effective_action_source": "position_actions.empty",
            "market_phase_summary": {"session_date": "2026-08-10"},
            "position_actions": {
                "empty": {"action": "buy"},
                "holding": {"action": "hold"},
            },
        }, ensure_ascii=False),
    )

    with patch("src.services.task_queue.get_task_queue", return_value=queue), patch(
        "src.services.paper_trading_service.build_market_phase_context",
        return_value=_market_phase(date(2026, 8, 10)),
    ):
        replacement = service.create_plan_for_report(replacement_report_id, decision_signal_id=replacement_signal_id)

    assert replacement["item"]["executable"] is False
    assert len(queue.submissions) == 1
    with isolated_db.get_session() as session:
        requests = session.execute(select(PaperReanalysisRequest).order_by(PaperReanalysisRequest.id)).scalars().all()
        assert len(requests) == 1
        assert requests[0].replacement_plan_id == replacement["item"]["id"]
        assert requests[0].status == "completed"


@pytest.mark.parametrize(
    "sniper_points,expected_reason",
    [
        (
            {
                "exit_leg_schema_version": "exit-legs-v1",
                "exit_legs": [
                    {"leg_id": "tp1", "sequence": 1, "role": "partial_take_profit", "allocation_fraction": 0.4, "target_price": 120},
                    {"leg_id": "tp2", "sequence": 2, "role": "final_take_profit", "allocation_fraction": 0.4, "target_price": 140},
                ],
            },
            "exit_leg_fractions_must_equal_100pct",
        ),
        (
            {
                "exit_leg_schema_version": "exit-legs-v1",
                "exit_legs": [
                    {"leg_id": "tp1", "sequence": 1, "role": "partial_take_profit", "allocation_fraction": 0.5, "target_price": 140},
                    {"leg_id": "tp2", "sequence": 2, "role": "final_take_profit", "allocation_fraction": 0.5, "target_price": 120},
                ],
            },
            "exit_leg_targets_not_strictly_increasing",
        ),
        (
            {
                "exit_leg_schema_version": "exit-legs-v1",
                "final_target": 138,
                "exit_legs": [
                    {"leg_id": "tp1", "sequence": 1, "role": "partial_take_profit", "allocation_fraction": 0.5, "target_price": 120},
                    {"leg_id": "tp2", "sequence": 2, "role": "final_take_profit", "allocation_fraction": 0.5, "target_price": 140},
                ],
            },
            "exit_leg_final_target_mismatch",
        ),
        (
            {
                "exit_leg_schema_version": "exit-legs-v1",
                "exit_legs": [
                    {"leg_id": "tp1", "sequence": 1, "role": "partial_take_profit", "allocation_fraction": 0.5, "target_price": 120},
                    {"leg_id": "tp2", "sequence": 2, "role": "final_take_profit", "allocation_fraction": 0.5, "target_price": 140},
                ],
            },
            "take_profit_final_target_mismatch",
        ),
    ],
)
def test_invalid_exit_leg_contracts_fail_closed(isolated_db, sniper_points, expected_reason):
    raw_result = {
        "model_used": "model-a",
        "dashboard": {"battle_plan": {"sniper_points": sniper_points}},
    }
    target_price = 130.0 if expected_reason == "take_profit_final_target_mismatch" else 140.0
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        raw_result=raw_result,
        target_price=target_price,
    )

    item = _single_confirmation_service(intraday_min_risk_reward=0.1).create_plan_for_report(
        report_id,
        decision_signal_id=signal_id,
    )["item"]

    assert item["executable"] is False
    assert any(
        expected_reason in entry["text"]
        for entry in item["descriptive_conditions"]
        if entry.get("source") == "plan_validator"
    )


def test_legacy_exit_price_ranges_normalize_only_to_declared_targets(isolated_db):
    raw_result = {
        "model_used": "model-a",
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "first_take_profit": 120.0,
                    "final_target": 140.0,
                    "exit_leg_schema_version": "exit-legs-v1",
                    "exit_legs": [
                        {
                            "leg_id": "tp1",
                            "sequence": 1,
                            "role": "partial_take_profit",
                            "allocation_fraction": 0.5,
                            "price_rules": [{"type": "close_between", "low": 119.5, "high": 120.5}],
                        },
                        {
                            "leg_id": "tp2",
                            "sequence": 2,
                            "role": "final_take_profit",
                            "allocation_fraction": 0.5,
                            "price_rules": [{"type": "close_between", "low": 139.5, "high": 140.5}],
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

    item = _single_confirmation_service().create_plan_for_report(
        report_id,
        decision_signal_id=signal_id,
    )["item"]

    assert item["executable"] is True, item["descriptive_conditions"]
    assert item["weighted_target_price"] == pytest.approx(130.0)
    assert [leg["target_price"] for leg in item["exit_legs"]] == [120.0, 140.0]
    assert all(
        leg["source"] == "report_explicit_price_rule_normalized"
        for leg in item["exit_legs"]
    )


def test_legacy_exit_price_range_without_declared_target_stays_fail_closed(isolated_db):
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
                            "role": "take_profit",
                            "allocation_fraction": 1.0,
                            "price_rules": [{"type": "close_between", "low": 119.5, "high": 120.5}],
                        }
                    ],
                }
            }
        },
    }
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        raw_result=raw_result,
        target_price=120.0,
    )

    item = _single_confirmation_service().create_plan_for_report(
        report_id,
        decision_signal_id=signal_id,
    )["item"]

    assert item["executable"] is False
    assert any(
        "invalid_exit_leg_target_price" in entry["text"]
        for entry in item["descriptive_conditions"]
        if entry.get("source") == "plan_validator"
    )


def test_explicit_exit_legs_use_weighted_risk_reward_and_partial_take_profit_once(isolated_db):
    raw_result = {
        "model_used": "model-a",
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "exit_leg_schema_version": "exit-legs-v1",
                    "exit_legs": [
                        {"leg_id": "tp1", "sequence": 1, "role": "partial_take_profit", "allocation_fraction": 0.5, "target_price": 120},
                        {"leg_id": "tp2", "sequence": 2, "role": "final_take_profit", "allocation_fraction": 0.5, "target_price": 140},
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
    service = _single_confirmation_service()
    created = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    assert created["executable"] is True, created["descriptive_conditions"]
    assert created["weighted_target_price"] == pytest.approx(130.0)
    assert created["weighted_risk_reward"] > 1.2
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    now = datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        opened = service.run_intraday_monitor(current_time=now, fetcher_manager=_QuoteManager(_quote()))
    assert opened["positions_opened"] == 1

    next_phase = _phase()
    next_phase.market_local_time = datetime(2026, 8, 11, 10, 0)
    next_phase.effective_daily_bar_date = date(2026, 8, 8)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=next_phase):
        first_exit = service.run_intraday_monitor(
            current_time=now + timedelta(days=1),
            fetcher_manager=_QuoteManager(_quote(price=121.0, high=121.0, provider_timestamp="2026-08-11T10:00:00+08:00", fetched_at="2026-08-11T10:00:03+08:00")),
        )
        duplicate = service.run_intraday_monitor(
            current_time=now + timedelta(days=1, minutes=1),
            fetcher_manager=_QuoteManager(_quote(price=121.0, high=121.0, provider_timestamp="2026-08-11T10:01:00+08:00", fetched_at="2026-08-11T10:01:03+08:00")),
        )

    assert first_exit["positions_closed"] + first_exit["positions_reduced"] >= 1
    assert duplicate["positions_closed"] + duplicate["positions_reduced"] == 0
    operations = service.list_operations(model="model-a")["items"]
    assert operations[0]["executed_exit_leg_ids"] == ["tp1"]
    assert operations[0]["pending_exit_leg_ids"] == ["tp2"]

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=next_phase):
        final_exit = service.run_intraday_monitor(
            current_time=now + timedelta(days=1, minutes=2),
            fetcher_manager=_QuoteManager(_quote(price=141.0, high=141.0, provider_timestamp="2026-08-11T10:02:00+08:00", fetched_at="2026-08-11T10:02:03+08:00")),
        )

    assert final_exit["positions_closed"] == 1
    completed_plan = service.list_plans(model="model-a", status="closed")["items"][0]
    assert completed_plan["executed_exit_leg_ids"] == ["tp1", "tp2"]
    assert completed_plan["pending_exit_leg_ids"] == []
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        assert position.status == "closed"
        assert position.quantity == pytest.approx(0.0)
        event = session.execute(
            select(PaperTradeEvent).where(PaperTradeEvent.reason == "take_profit:tp1")
        ).scalar_one()
        assert event.event_type == "reduce"
        assert event.quantity == pytest.approx(100.0)
        details = json.loads(event.details_json)
        assert details["exit_leg_id"] == "tp1"
        final_event = session.execute(
            select(PaperTradeEvent).where(PaperTradeEvent.reason == "take_profit:tp2")
        ).scalar_one()
        assert final_event.event_type == "close"
        assert final_event.quantity == pytest.approx(100.0)
        final_details = json.loads(final_event.details_json)
        assert final_details["exit_leg_id"] == "tp2"


def test_cancelled_t1_staged_profit_does_not_consume_exit_leg(isolated_db):
    plan_id, position_id = _seed_open_paper_lot(
        isolated_db, opened_on=date(2026, 8, 11), stock_code="600519", model="model-a"
    )
    service = _single_confirmation_service()
    with isolated_db.session_scope() as session:
        plan = session.get(PaperExecutionPlan, plan_id)
        plan.target_price = 140.0
        plan.descriptive_conditions_json = json.dumps([{
            "source": "paper_trading.exit_legs",
            "schema_version": "exit-legs-v1",
            "exit_legs": [
                {"leg_id": "tp1", "sequence": 1, "role": "partial_take_profit", "allocation_fraction": 0.5, "target_price": 120},
                {"leg_id": "tp2", "sequence": 2, "role": "final_take_profit", "allocation_fraction": 0.5, "target_price": 140},
            ],
            "weighted_target_price": 130,
            "weighted_risk_reward": 6,
        }], ensure_ascii=False)
        position = session.get(PaperPosition, position_id)
        position.target_price = 140.0

    same_day = _market_phase(date(2026, 8, 11))
    next_day = _market_phase(date(2026, 8, 12))
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=same_day):
        pending = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(price=121, high=121, provider_timestamp="2026-08-11T10:00:00+08:00", fetched_at="2026-08-11T10:00:03+08:00")),
            interval_seconds=300,
        )
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=next_day):
        cancelled = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 12, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(price=119, high=119, provider_timestamp="2026-08-12T10:00:00+08:00", fetched_at="2026-08-12T10:00:03+08:00")),
            interval_seconds=300,
        )
        retriggered = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 12, 2, 5, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(price=121, high=121, provider_timestamp="2026-08-12T10:05:00+08:00", fetched_at="2026-08-12T10:05:03+08:00")),
            interval_seconds=300,
        )

    assert pending["positions_closed"] == 0
    assert pending["positions_reduced"] == 0
    with isolated_db.get_session() as session:
        pending_events = session.execute(
            select(PaperTradeEvent).where(PaperTradeEvent.event_type == "pending_exit")
        ).scalars().all()
        assert len(pending_events) == 1
        assert pending_events[0].reason == "take_profit:tp1"
    assert cancelled["positions_closed"] == 0
    assert retriggered["positions_reduced"] == 1
    item = service.list_operations(model="model-a")["items"][0]
    assert item["executed_exit_leg_ids"] == ["tp1"]
    assert item["pending_exit_leg_ids"] == ["tp2"]
    with isolated_db.get_session() as session:
        events = session.execute(select(PaperTradeEvent).order_by(PaperTradeEvent.id)).scalars().all()
        assert [event.event_type for event in events] == ["pending_exit", "reduce"]
        assert events[1].reason == "take_profit:tp1"
        position = session.get(PaperPosition, position_id)
        assert position.quantity == pytest.approx(100.0)
        assert position.pending_exit_reason is None


def test_newer_target_revision_supersedes_old_staged_exit_contract(isolated_db):
    plan_id, position_id = _seed_open_paper_lot(
        isolated_db, opened_on=date(2026, 8, 10), stock_code="600519", model="model-a"
    )
    service = _single_confirmation_service()
    with isolated_db.session_scope() as session:
        plan = session.get(PaperExecutionPlan, plan_id)
        plan.target_price = 150.0
        plan.descriptive_conditions_json = json.dumps([{
            "source": "paper_trading.exit_legs",
            "schema_version": "exit-legs-v1",
            "exit_legs": [
                {"leg_id": "tp1", "sequence": 1, "role": "partial_take_profit", "allocation_fraction": 0.5, "target_price": 130},
                {"leg_id": "tp2", "sequence": 2, "role": "final_take_profit", "allocation_fraction": 0.5, "target_price": 150},
            ],
            "weighted_target_price": 140,
            "weighted_risk_reward": 9,
        }], ensure_ascii=False)
        position = session.get(PaperPosition, position_id)
        position.target_price = 130.0
        service._record_risk_revision(
            session,
            position,
            source_report_id=plan.source_report_id,
            decision_signal_id=plan.decision_signal_id,
            new_stop_loss=96.0,
            new_target_price=125.0,
            evaluated_at=datetime(2026, 8, 11, 10, 0),
            current_price=124.0,
            source="eligible_post_entry_signal",
        )

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 5, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(price=126, high=126, provider_timestamp="2026-08-11T10:05:00+08:00", fetched_at="2026-08-11T10:05:03+08:00")),
            interval_seconds=300,
        )

    assert result["positions_closed"] == 1
    with isolated_db.get_session() as session:
        event = session.execute(select(PaperTradeEvent).where(PaperTradeEvent.event_type == "close")).scalar_one()
        assert event.reason == "model_risk_target_reached"
        details = json.loads(event.details_json)
        assert details["exit_leg_id"] is None


def test_explicit_entry_leg_geometry_is_authoritative_for_risk_reward(isolated_db):
    raw_result = {
        "model_used": "model-a",
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "entry_leg_schema_version": "entry-legs-v1",
                    "entry_legs": [{
                        "leg_id": "initial",
                        "role": "initial",
                        "allocation_fraction": 1.0,
                        "price_rules": [{"type": "close_between", "low": 10.0, "high": 10.0}],
                    }],
                }
            }
        },
    }
    report_id, signal_id = _source(
        isolated_db,
        raw_result=raw_result,
        entry_low=96.0,
        entry_high=100.0,
        stop_loss=9.7,
        target_price=10.5,
        metadata_json=json.dumps({
            "holding_state": "empty",
            **_frozen_style_metadata("position", support=10.0),
        }, ensure_ascii=False),
    )

    item = _single_confirmation_service(intraday_min_risk_reward=1.2).create_plan_for_report(
        report_id,
        decision_signal_id=signal_id,
    )["item"]

    assert item["executable"] is True
    assert item["status"] == "active"


def test_intraday_executes_one_leg_per_bucket_and_add_leg_next_bucket(isolated_db):
    raw_result = {
        "model_used": "model-a",
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "entry_leg_schema_version": "entry-legs-v1",
                    "entry_legs": [
                        {"leg_id": "initial", "role": "initial", "sequence": 1, "allocation_fraction": 0.5, "max_executions": 1, "price_rules": [{"type": "close_between", "low": 96, "high": 100}]},
                        {"leg_id": "add", "role": "add", "sequence": 2, "allocation_fraction": 0.5, "max_executions": 1, "price_rules": [{"type": "close_between", "low": 96, "high": 100}]},
                    ],
                }
            }
        },
    }
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), raw_result=raw_result)
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    now = datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        first = service.run_intraday_monitor(current_time=now, fetcher_manager=_QuoteManager(_quote()))
        duplicate_bucket = service.run_intraday_monitor(current_time=now, fetcher_manager=_QuoteManager(_quote()))
    with isolated_db.session_scope() as session:
        account = session.execute(select(PaperCapitalAccount)).scalar_one()
        plan_row = session.execute(select(PaperExecutionPlan)).scalar_one()
        assert plan_row.plan_allocation_budget == pytest.approx(20000.0)
        assert plan_row.plan_risk_budget == pytest.approx(1000.0)
        account.cash += 900000.0
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        second = service.run_intraday_monitor(
            current_time=now + timedelta(minutes=6),
            fetcher_manager=_QuoteManager(_quote(provider_timestamp="2026-08-10T10:06:00+08:00", fetched_at="2026-08-10T10:06:03+08:00")),
        )

    assert first["positions_opened"] == 1, first
    assert duplicate_bucket["positions_opened"] == 0, duplicate_bucket
    assert second["positions_opened"] == 1, second
    with isolated_db.get_session() as session:
        positions = session.execute(select(PaperPosition).order_by(PaperPosition.id)).scalars().all()
        assert [p.entry_leg_id for p in positions] == ["initial", "add"]
        assert [p.allocated_capital for p in positions] == pytest.approx([10000.0, 10000.0])
        assert sum(p.allocated_capital for p in positions) == pytest.approx(20000.0)
        account = session.execute(select(PaperCapitalAccount)).scalar_one()
        assert account.open_position_count == 1
    operations = service.list_operations()
    assert operations["total"] == 1
    assert operations["items"][0]["stage"] == "position_monitoring"
    assert operations["items"][0]["open_lot_count"] == 2


def test_intraday_entry_sizing_includes_fee_slippage_before_debit(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7))
    service = _single_confirmation_service(
        slippage_pct=0.03,
        commission_rate=0.0003,
        min_commission=5.0,
    )
    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 10))):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                provider_timestamp="2026-08-10T10:00:00+08:00",
                fetched_at="2026-08-10T10:00:03+08:00",
            )),
        )

    assert result["positions_opened"] == 1, result
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition).where(PaperPosition.plan_id == plan["id"])).scalar_one()
        account = session.execute(select(PaperCapitalAccount)).scalar_one()
        event = session.execute(select(PaperTradeEvent).where(PaperTradeEvent.event_type == "open")).scalar_one()
        position_cap = 100000.0 * 0.20
        assert position.quantity == pytest.approx(100)
        assert position.allocated_capital == pytest.approx(event.net_amount)
        assert position.allocated_capital <= position_cap
        assert account.cash >= 0
        assert account.exposure <= position_cap
        assert event.net_amount == pytest.approx(10008.0009)


def test_repeated_entry_leg_execution_splits_snapshot_budget(isolated_db):
    raw_result = {
        "model_used": "model-a",
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "entry_leg_schema_version": "entry-legs-v1",
                    "entry_legs": [
                        {
                            "leg_id": "initial",
                            "role": "initial",
                            "sequence": 1,
                            "allocation_fraction": 1.0,
                            "max_executions": 2,
                            "price_rules": [{"type": "close_between", "low": 96, "high": 100}],
                        },
                    ],
                }
            }
        },
    }
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), raw_result=raw_result)
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    now = datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        first = service.run_intraday_monitor(current_time=now, fetcher_manager=_QuoteManager(_quote()))
    first_operation = service.list_operations()["items"][0]
    assert first_operation["stage"] == "entry_and_position_monitoring"
    assert first_operation["executed_entry_leg_counts"] == {"initial": 1}
    assert first_operation["pending_entry_leg_ids"] == ["initial"]
    with isolated_db.session_scope() as session:
        account = session.execute(select(PaperCapitalAccount)).scalar_one()
        account.cash += 900000.0
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        second = service.run_intraday_monitor(
            current_time=now + timedelta(minutes=6),
            fetcher_manager=_QuoteManager(_quote(provider_timestamp="2026-08-10T10:06:00+08:00", fetched_at="2026-08-10T10:06:03+08:00")),
        )

    assert first["positions_opened"] == 1
    assert second["positions_opened"] == 1
    with isolated_db.get_session() as session:
        plan = session.execute(select(PaperExecutionPlan)).scalar_one()
        positions = session.execute(select(PaperPosition).order_by(PaperPosition.id)).scalars().all()
        assert plan.plan_allocation_budget == pytest.approx(20000.0)
        assert [p.execution_index for p in positions] == [1, 2]
        assert [p.allocated_capital for p in positions] == pytest.approx([10000.0, 10000.0])
        assert sum(p.allocated_capital for p in positions) <= plan.plan_allocation_budget


def test_preopen_monitor_skips_without_quote_or_persistence(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7))
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    phase = _market_phase(date(2026, 8, 10), phase="premarket", hour=9, minute=27)

    class NoQuoteManager:
        def get_realtime_quote(self, *_args, **_kwargs):
            raise AssertionError("preopen monitor must not fetch ordinary realtime quotes")

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=phase):
        stats = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 1, 27, tzinfo=timezone.utc),
            fetcher_manager=NoQuoteManager(),
        )

    assert stats["positions_opened"] == 0
    assert stats["observations"] == 0
    assert stats["failed_closed"] == 0
    with isolated_db.get_session() as session:
        assert session.execute(select(PaperPlanObservation)).scalar_one_or_none() is None
        assert session.execute(select(PaperReanalysisRequest)).scalar_one_or_none() is None


def test_opening_confirmation_fresh_quote_confirms_without_trading(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7))
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    phase = _market_phase(date(2026, 8, 10), hour=9, minute=31)
    quote = _quote(
        provider_timestamp="2026-08-10T09:31:00+08:00",
        fetched_at="2026-08-10T09:31:03+08:00",
    )

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=phase):
        stats = service.run_opening_confirmation_check(
            current_time=datetime(2026, 8, 10, 1, 31, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(quote),
        )

    assert stats["confirmed"] == 1
    assert stats["reanalysis_requested"] == 0
    with isolated_db.get_session() as session:
        observation = session.execute(select(PaperPlanObservation)).scalar_one()
        evidence = json.loads(observation.evidence_json)
        assert observation.result == "opening_confirmed"
        assert evidence["policy"]["ordinary_realtime_quote"] is True
        assert evidence["policy"]["true_auction_order_book_claimed"] is False
        assert evidence["policy"]["no_fill_in_confirmation_sweep"] is True
        assert session.execute(select(PaperPosition)).scalar_one_or_none() is None
    listed = service.list_auction_observations()
    assert listed["total"] == 1
    assert listed["items"][0]["mode"] == "opening_confirmation"
    assert listed["items"][0]["virtual_price"] == pytest.approx(100.0)
    assert listed["items"][0]["source"] == "unit_quote"
    assert listed["items"][0]["identity_verified"] is True


@pytest.mark.parametrize(
    "quote,expected_reason",
    [
        (None, "missing_realtime_quote"),
        (_quote(code="000001", provider_timestamp="2026-08-10T09:31:00+08:00", fetched_at="2026-08-10T09:31:03+08:00"), "realtime_quote_identity_mismatch"),
        (_quote(market="hk", provider_timestamp="2026-08-10T09:31:00+08:00", fetched_at="2026-08-10T09:31:03+08:00"), "realtime_quote_identity_mismatch"),
        (_quote(code="", provider_timestamp="2026-08-10T09:31:00+08:00", fetched_at="2026-08-10T09:31:03+08:00"), "missing_realtime_quote_identity"),
        (_quote(stale=True, provider_timestamp="2026-08-10T09:20:00+08:00", fetched_at="2026-08-10T09:20:03+08:00"), "stale_realtime_quote"),
    ],
)
def test_opening_confirmation_rejects_missing_stale_or_wrong_identity_quote(
    isolated_db, quote, expected_reason,
):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7))
    service = _single_confirmation_service(intraday_max_quote_stale_seconds=30)
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    phase = _market_phase(date(2026, 8, 10), hour=9, minute=31)

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=phase):
        stats = service.run_opening_confirmation_check(
            current_time=datetime(2026, 8, 10, 1, 31, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(quote),
        )

    assert stats["failed_closed"] == 1
    assert stats["confirmed"] == 0
    assert stats["reanalysis_requested"] == 0
    with isolated_db.get_session() as session:
        observation = session.execute(select(PaperPlanObservation)).scalar_one()
        assert observation.result == "failed_closed"
        assert expected_reason in observation.evidence_json
        assert session.execute(select(PaperPosition)).scalar_one_or_none() is None


def test_opening_confirmation_material_deviation_submits_once_and_never_trades(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), model="model-a")
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    phase = _market_phase(date(2026, 8, 10), hour=9, minute=31)
    queue = _CountingTaskQueue()
    quote = _quote(
        price=105,
        provider_timestamp="2026-08-10T09:31:00+08:00",
        fetched_at="2026-08-10T09:31:03+08:00",
    )
    quote.pre_close = 100.0  # A genuine overnight gap, not merely waiting above entry.

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=phase), patch(
        "src.services.task_queue.get_task_queue", return_value=queue,
    ):
        first = service.run_opening_confirmation_check(
            current_time=datetime(2026, 8, 10, 1, 31, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(quote),
        )
        second = service.run_opening_confirmation_check(
            current_time=datetime(2026, 8, 10, 1, 31, 30, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(quote),
        )

    assert first["reanalysis_requested"] == 1
    assert second["reanalysis_requested"] == 0
    assert second["skipped_already_terminal"] == 1
    assert len(queue.submissions) == 1
    with isolated_db.get_session() as session:
        request = session.execute(select(PaperReanalysisRequest)).scalar_one()
        assert request.trigger_type == "opening_confirmation_deviation"
        assert request.status == "submitted"
        assert session.execute(select(PaperPosition)).scalar_one_or_none() is None


@pytest.mark.parametrize("catch_up", [False, True])
def test_opening_wait_above_entry_does_not_reanalyze_or_chase(isolated_db, catch_up):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7))
    service = _single_confirmation_service()
    plan_id = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]["id"]
    phase = _market_phase(date(2026, 8, 10), hour=9, minute=31)
    quote = _quote(price=118, provider_timestamp="2026-08-10T09:31:00+08:00",
                   fetched_at="2026-08-10T09:31:03+08:00")
    queue = _CountingTaskQueue()
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=phase), patch(
        "src.services.task_queue.get_task_queue", return_value=queue,
    ):
        run = service.run_intraday_monitor if catch_up else service.run_opening_confirmation_check
        stats = run(current_time=datetime(2026, 8, 10, 1, 31, tzinfo=timezone.utc),
                    fetcher_manager=_QuoteManager(quote))
    assert not queue.submissions
    with isolated_db.get_session() as session:
        observation = session.execute(select(PaperPlanObservation)).scalar_one()
        evidence = json.loads(observation.evidence_json)["opening_confirmation"]
        assert evidence["risk_reward_basis"] == "planned_entry_range"
        assert evidence["risk_reward"] > service.intraday_min_risk_reward
        assert evidence["spot_risk_reward"] < service.intraday_min_risk_reward
        assert evidence["factors"]["entry_band"]["waiting_for_entry"] is True
        assert not evidence["material"]
        assert session.get(PaperExecutionPlan, plan_id).status == "active"
        assert session.execute(select(PaperPosition)).scalar_one_or_none() is None
        if catch_up:
            assert observation.result == "failed_closed"  # Existing anti-chase execution veto remains.
            assert json.loads(observation.evidence_json)["gap_veto"]["type"] == "opening_gap_veto"
        else:
            assert stats["confirmed"] == 1


@pytest.mark.parametrize("price,pre_close,volume_ratio,reason", [
    (94.0, 94.5, 1.8, "risk_boundary"),
    (130.0, 129.5, 1.8, "risk_boundary"),
    (105.0, 100.0, 1.8, "prior_close_gap"),
    (100.0, 99.5, 0.01, "opening_volume_ratio"),
])
def test_opening_real_plan_concerns_still_block_and_reanalyze(isolated_db, price, pre_close, volume_ratio, reason):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7))
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    quote = _quote(price=price, volume_ratio=volume_ratio,
                   provider_timestamp="2026-08-10T09:31:00+08:00",
                   fetched_at="2026-08-10T09:31:03+08:00")
    quote.pre_close = pre_close
    queue = _CountingTaskQueue()
    with patch("src.services.paper_trading_service.build_market_phase_context",
               return_value=_market_phase(date(2026, 8, 10), hour=9, minute=31)), patch(
        "src.services.task_queue.get_task_queue", return_value=queue,
    ):
        stats = service.run_opening_confirmation_check(
            current_time=datetime(2026, 8, 10, 1, 31, tzinfo=timezone.utc), fetcher_manager=_QuoteManager(quote))
    assert stats["reanalysis_requested"] == 1
    assert len(queue.submissions) == 1
    with isolated_db.get_session() as session:
        observation = session.execute(select(PaperPlanObservation)).scalar_one()
        assert reason in json.loads(observation.evidence_json)["opening_confirmation"]["material_reasons"]
        assert session.execute(select(PaperPosition)).scalar_one_or_none() is None


def test_opening_plan_economics_not_masked_by_frozen_staged_rr(isolated_db):
    service = _single_confirmation_service()
    plan = SimpleNamespace(rules_json=json.dumps([{"type":"close_between", "low":96, "high":100}]),
        stop_loss=95, target_price=100, market="cn", stock_code="600519",
        descriptive_conditions_json=json.dumps([{"source":"paper_trading.exit_legs",
            "exit_legs":[{"target_price":100, "allocation_fraction":1}],
            "weighted_target_price":100, "weighted_risk_reward":99}]))
    evidence, error = service._opening_confirmation_material_deviation(
        plan=plan, quote=_quote(price=99), price=99, session_date=date(2026, 8, 10))
    assert error is None
    assert evidence["risk_reward"] == pytest.approx(2 / 3)
    assert "risk_reward" in evidence["material_reasons"]


def test_opening_confirmation_does_not_fetch_outside_window(isolated_db):
    service = _single_confirmation_service()

    class NoQuoteManager:
        def get_realtime_quote(self, *_args, **_kwargs):
            raise AssertionError("outside-window confirmation must not fetch quotes")

    result = service.run_opening_confirmation_check(
        current_time=datetime(2026, 8, 10, 1, 29, 59, tzinfo=timezone.utc),
        fetcher_manager=NoQuoteManager(),
    )

    assert result["skipped"] is True
    assert result["reason"] == "outside_cn_opening_confirmation_window"


def test_next_check_obligation_consumed_once_and_blocks_entry(isolated_db):
    raw_result = {
        "model_used": "model-a",
        "dashboard": {"phase_decision": {"next_check_time": "10:00"}},
    }
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), raw_result=raw_result)
    service = _single_confirmation_service()
    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    assert plan["next_check_obligation"]["session_date"] == "2026-08-10"
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    fake_task = SimpleNamespace(task_id="task-next-check")
    fake_queue = SimpleNamespace(submit_task=lambda *args, **kwargs: fake_task)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()), patch(
        "src.services.task_queue.get_task_queue",
        return_value=fake_queue,
    ):
        stats1 = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote()),
        )
        stats2 = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 6, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(provider_timestamp="2026-08-10T10:06:00+08:00", fetched_at="2026-08-10T10:06:03+08:00")),
        )

    assert stats1["positions_opened"] == 0
    assert stats2["positions_opened"] == 0
    with isolated_db.get_session() as session:
        requests = session.execute(select(PaperReanalysisRequest)).scalars().all()
        assert len(requests) == 1
        assert requests[0].status == "submitted"
        assert requests[0].task_id == "task-next-check"


@pytest.mark.parametrize(
    "next_check_text,expected_session_date",
    [
        ("10:00", "2026-08-10"),
        ("下个交易日 10:00", "2026-08-11"),
    ],
)
def test_same_session_reanalysis_binds_next_check_to_requested_session(
    isolated_db,
    next_check_text,
    expected_session_date,
):
    raw_result = {
        "model_used": "model-a",
        "paper_trading": {"same_session_reanalysis": True},
        "dashboard": {"phase_decision": {"next_check_time": next_check_text}},
    }
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 10),
        raw_result=raw_result,
        trigger_source="paper_trading_reanalysis",
        market_phase="intraday",
        metadata_json=json.dumps({
            "market_phase_summary": {"phase": "intraday", "session_date": "2026-08-10"},
            "position_actions": {"holding": {"action": "buy"}},
        }),
    )

    with patch(
        "src.services.paper_trading_service.build_market_phase_context",
        return_value=_market_phase(date(2026, 8, 10)),
    ):
        item = _single_confirmation_service().create_plan_for_report(
            report_id,
            decision_signal_id=signal_id,
        )["item"]

    assert item["valid_for_date"] == "2026-08-10"
    assert item["next_check_obligation"]["session_date"] == expected_session_date


def test_untrusted_raw_same_session_flag_cannot_activate_system_postmarket_report(isolated_db):
    raw_result = {
        "model_used": "model-a",
        "paper_trading": {"same_session_reanalysis": True},
    }
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 10),
        raw_result=raw_result,
        trigger_source="system",
        market_phase="postmarket",
        metadata_json=json.dumps({
            "market_phase_summary": {"phase": "postmarket", "session_date": "2026-08-10"},
            "position_actions": {"holding": {"action": "buy"}},
        }),
    )
    with patch(
        "src.services.paper_trading_service.build_market_phase_context",
        return_value=_market_phase(date(2026, 8, 10)),
    ):
        item = _single_confirmation_service().create_plan_for_report(
            report_id,
            decision_signal_id=signal_id,
        )["item"]

    assert item["valid_for_date"] == "2026-08-11"


def test_manual_intraday_without_frozen_session_date_cannot_activate_same_session(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 10),
        trigger_source="api",
        market_phase="intraday",
        metadata_json=json.dumps({
            "market_phase_summary": {"phase": "intraday"},
            "position_actions": {"holding": {"action": "buy"}},
        }),
    )
    with patch(
        "src.services.paper_trading_service.build_market_phase_context",
        return_value=_market_phase(date(2026, 8, 10)),
    ):
        item = _single_confirmation_service().create_plan_for_report(
            report_id,
            decision_signal_id=signal_id,
        )["item"]

    assert item["valid_for_date"] == "2026-08-11"


def test_reanalysis_request_atomic_claim_submits_once_with_two_workers(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7))
    service = _single_confirmation_service()
    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    plan_ref = SimpleNamespace(
        id=plan["id"],
        source_model=plan["source_model"],
        market=plan["market"],
        stock_code=plan["stock_code"],
    )
    fake_queue = _CountingTaskQueue()

    def ensure_once():
        with patch("src.services.task_queue.get_task_queue", return_value=fake_queue):
            return service._ensure_reanalysis_request(
                plan=plan_ref,
                trigger_type="auction_deviation",
                session_date=date(2026, 8, 10),
                due_at=datetime(2026, 8, 10, 1, 20),
                observed_at=datetime(2026, 8, 10, 1, 20),
                evidence={"source": "unit"},
                submit=True,
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _idx: ensure_once(), range(2)))

    assert len(fake_queue.submissions) == 1
    assert {item["status"] for item in results} <= {"claimed", "submitted"}
    with isolated_db.get_session() as session:
        requests = session.execute(select(PaperReanalysisRequest)).scalars().all()
        assert len(requests) == 1
        assert requests[0].status == "submitted"
        assert requests[0].task_id == "task-1"


def _create_plan_with_frozen_style(isolated_db, style: str, *, created_on: date, horizon: Optional[str] = None):
    if horizon is None:
        horizon = str(TRADING_STYLE_SETTINGS[style].get("default_signal_horizon") or "10d")
    metadata = {"holding_state": "empty", **_frozen_style_metadata(style)}
    report_id, signal_id = _source(
        isolated_db,
        created_on=created_on,
        horizon=horizon,
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    item = _single_confirmation_service().create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    return SimpleNamespace(
        id=item["id"],
        source_model=item["source_model"],
        market=item["market"],
        stock_code=item["stock_code"],
    )


def test_short_swing_reanalysis_rate_limit_uses_90m_cooldown_and_daily_cap_2(isolated_db):
    service = _single_confirmation_service()
    first_plan = _create_plan_with_frozen_style(isolated_db, "short_swing", created_on=date(2026, 8, 7))
    second_plan = _create_plan_with_frozen_style(isolated_db, "short_swing", created_on=date(2026, 8, 8))
    third_plan = _create_plan_with_frozen_style(isolated_db, "short_swing", created_on=date(2026, 8, 9))
    session_date = date(2026, 8, 10)

    first = service._ensure_reanalysis_request(
        plan=first_plan,
        trigger_type="auction_deviation",
        session_date=session_date,
        due_at=datetime(2026, 8, 10, 1, 30),
        observed_at=datetime(2026, 8, 10, 1, 30),
        evidence={"source": "unit"},
    )
    cooldown = service._ensure_reanalysis_request(
        plan=second_plan,
        trigger_type="auction_deviation",
        session_date=session_date,
        due_at=datetime(2026, 8, 10, 2, 0),
        observed_at=datetime(2026, 8, 10, 2, 0),
        evidence={"source": "unit"},
    )
    allowed_after_cooldown = service._ensure_reanalysis_request(
        plan=second_plan,
        trigger_type="auction_deviation_retry",
        session_date=session_date,
        due_at=datetime(2026, 8, 10, 3, 1),
        observed_at=datetime(2026, 8, 10, 3, 1),
        evidence={"source": "unit"},
    )
    daily_cap = service._ensure_reanalysis_request(
        plan=third_plan,
        trigger_type="auction_deviation_retry",
        session_date=session_date,
        due_at=datetime(2026, 8, 10, 4, 40),
        observed_at=datetime(2026, 8, 10, 4, 40),
        evidence={"source": "unit"},
    )

    assert first["status"] == "pending"
    assert cooldown["status"] == "blocked"
    assert cooldown["failure_reason"] == "reanalysis_cooldown_active"
    assert allowed_after_cooldown["status"] == "pending"
    assert daily_cap["status"] == "blocked"
    assert daily_cap["failure_reason"] == "reanalysis_daily_cap_exceeded"


@pytest.mark.parametrize("style,horizon,cooldown_minutes", [
    ("position", "5d", 240),
    ("trend", "long", 1440),
])
def test_position_and_trend_reanalysis_daily_cap_one(isolated_db, style, horizon, cooldown_minutes):
    service = _single_confirmation_service()
    first_plan = _create_plan_with_frozen_style(isolated_db, style, created_on=date(2026, 8, 7), horizon=horizon)
    second_plan = _create_plan_with_frozen_style(isolated_db, style, created_on=date(2026, 8, 8), horizon=horizon)
    session_date = date(2026, 8, 10)

    first = service._ensure_reanalysis_request(
        plan=first_plan,
        trigger_type="opening_confirmation_deviation",
        session_date=session_date,
        due_at=datetime(2026, 8, 10, 1, 30),
        observed_at=datetime(2026, 8, 10, 1, 30),
        evidence={"source": "unit"},
    )
    blocked = service._ensure_reanalysis_request(
        plan=second_plan,
        trigger_type="opening_confirmation_deviation",
        session_date=session_date,
        due_at=datetime(2026, 8, 10, 6, 0),
        observed_at=datetime(2026, 8, 10, 6, 0),
        evidence={"source": "unit"},
    )

    assert first["status"] == "pending"
    assert blocked["status"] == "blocked"
    assert blocked["failure_reason"] == "reanalysis_daily_cap_exceeded"
    assert blocked["evidence"]["reanalysis_rate_limit"]["cooldown_minutes"] == cooldown_minutes
    assert blocked["evidence"]["reanalysis_rate_limit"]["daily_cap"] == 1


def test_reanalysis_reconcile_never_blind_resubmits_and_completes_matching_report(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7))
    service = _single_confirmation_service()
    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    with isolated_db.session_scope() as session:
        request = PaperReanalysisRequest(
            request_key="unit-submitted",
            trigger_type="next_check",
            status="submitted",
            source_plan_id=plan["id"],
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            session_date=date(2026, 8, 10),
            submitted_at=datetime(2026, 8, 10, 1, 20),
            task_id="task-existing",
            query_id="task-existing",
            evidence_json="{}",
        )
        session.add(request)
        replacement_report = AnalysisHistory(
            query_id="task-existing",
            code="600519",
            name="贵州茅台",
            report_type="full",
            model_used="model-a",
            operation_advice="观望",
            raw_result=json.dumps({"paper_trading": {"same_session_reanalysis": True}}, ensure_ascii=False),
            created_at=datetime(2026, 8, 10, 1, 25),
        )
        session.add(replacement_report)
        session.flush()
        add_report_execution_intent(session, replacement_report.id)
        session.add(PaperExecutionPlan(
            plan_key="unit-replacement-plan",
            plan_version="paper-hybrid-v3",
            source_report_id=replacement_report.id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            report_date=date(2026, 8, 10),
            report_created_at=datetime(2026, 8, 10, 1, 25),
            valid_for_date=date(2026, 8, 10),
            action="buy",
            status="active",
            executable=True,
            rules_json="[]",
            descriptive_conditions_json="[]",
        ))
    fake_queue = _CountingTaskQueue(task=None)
    with patch("src.services.task_queue.get_task_queue", return_value=fake_queue):
        summary = service.reconcile_reanalysis_requests(current_time=datetime(2026, 8, 10, 1, 26, tzinfo=timezone.utc))

    assert fake_queue.submissions == []
    assert summary["completed"] == 1
    with isolated_db.get_session() as session:
        request = session.execute(select(PaperReanalysisRequest)).scalar_one()
        assert request.status == "completed"
        assert request.replacement_report_id is not None
        assert request.replacement_plan_id is not None


def test_reanalysis_reconcile_waits_for_completed_task_report_within_grace(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7))
    service = _single_confirmation_service()
    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    with isolated_db.session_scope() as session:
        session.add(PaperReanalysisRequest(
            request_key="unit-completed-wait",
            trigger_type="next_check",
            status="submitted",
            source_plan_id=plan["id"],
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            session_date=date(2026, 8, 10),
            submitted_at=datetime(2026, 8, 10, 1, 20),
            task_id="task-wait",
            query_id="task-wait",
            evidence_json="{}",
        ))
    fake_queue = _CountingTaskQueue(task=SimpleNamespace(status="completed", result={"query_id": "task-wait"}))
    with patch("src.services.task_queue.get_task_queue", return_value=fake_queue):
        summary = service.reconcile_reanalysis_requests(current_time=datetime(2026, 8, 10, 1, 25, tzinfo=timezone.utc))

    assert fake_queue.submissions == []
    assert summary["unchanged"] == 1
    with isolated_db.get_session() as session:
        request = session.execute(select(PaperReanalysisRequest)).scalar_one()
        assert request.status == "submitted"


def test_reanalysis_reconcile_fails_closed_stale_claim_without_resubmit(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7))
    service = _single_confirmation_service()
    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    with isolated_db.session_scope() as session:
        session.add(PaperReanalysisRequest(
            request_key="unit-stale-claimed",
            trigger_type="auction_deviation",
            status="claimed",
            source_plan_id=plan["id"],
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            session_date=date(2026, 8, 10),
            claimed_at=datetime(2026, 8, 10, 1, 0),
            evidence_json="{}",
        ))
    fake_queue = _CountingTaskQueue(task=None)
    with patch("src.services.task_queue.get_task_queue", return_value=fake_queue):
        summary = service.reconcile_reanalysis_requests(current_time=datetime(2026, 8, 10, 1, 30, tzinfo=timezone.utc))

    assert fake_queue.submissions == []
    assert summary["failed_closed"] == 1
    with isolated_db.get_session() as session:
        request = session.execute(select(PaperReanalysisRequest)).scalar_one()
        assert request.status == "failed_closed"
        assert request.failure_reason == "reanalysis_request_unrecoverable_after_restart"


def test_legacy_defensive_signal_without_holding_action_is_unusable(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 24), model="model-a")
    service = _single_confirmation_service()
    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    with isolated_db.session_scope() as session:
        account = PaperCapitalAccount(
            account_key="paper:model-a:cn:CNY",
            source_model="model-a",
            market="cn",
            currency="CNY",
            initial_capital=100000,
            cash=90000,
            invested_capital=10000,
            realized_pnl=0,
            exposure=10000,
            open_position_count=1,
        )
        session.add(account)
        session.flush()
        position = PaperPosition(
            plan_id=plan["id"],
            entry_leg_id="legacy_entry",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            status="open",
            opened_on=date(2026, 8, 25),
            opened_at=datetime(2026, 8, 25, 2, 0),
            opened_price=100.0,
            normalized_notional=10000,
            quantity=100,
            capital_account_id=account.id,
            currency="CNY",
            allocated_capital=10000,
            stop_loss=95.0,
            target_price=130.0,
            max_holding_sessions=10,
        )
        session.add(position)
        old_signal = DecisionSignalRecord(
            stock_code="600519",
            stock_name="贵州茅台",
            market="cn",
            source_type="analysis",
            source_report_id=report_id,
            source_agent="model-a",
            trace_id="old-sell",
            trigger_source="system",
            action="sell",
            horizon="1d",
            plan_quality="partial",
            status="active",
            created_at=datetime(2026, 8, 25, 1, 0),
        )
        new_signal = DecisionSignalRecord(
            stock_code="600519",
            stock_name="贵州茅台",
            market="cn",
            source_type="analysis",
            source_report_id=report_id,
            source_agent="model-a",
            trace_id="new-sell",
            trigger_source="system",
            action="sell",
            horizon="1d",
            plan_quality="partial",
            status="active",
            created_at=datetime(2026, 8, 25, 7, 10),
        )
        session.add(old_signal)
        session.add(new_signal)

    bars = _bars(start=date(2026, 8, 20), days=7)
    bars[-1]["date"] = date(2026, 8, 26)
    bars[-1]["low"] = 98.0
    bars[-1]["high"] = 102.0
    bars[-1]["close"] = 101.0
    result = service.run_eod(date(2026, 8, 26), {("cn", "600519"): bars})

    assert result["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        assert position.status == "open"
        observation = session.execute(
            select(PaperPlanEvaluation)
            .where(PaperPlanEvaluation.position_id == position.id)
            .order_by(PaperPlanEvaluation.id.desc())
        ).scalar_one()
        evidence = json.loads(observation.evidence_json)
        eligibility = evidence["defensive_signal"]["eligibility"]
        assert "missing_or_invalid_holding_action" in eligibility["reasons"]
        assert "invalid_confidence" in eligibility["reasons"]


def test_legacy_null_opened_at_backfills_but_missing_holding_action_remains_unusable(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 24), model="model-a")
    service = _single_confirmation_service()
    plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    with isolated_db.session_scope() as session:
        account = PaperCapitalAccount(
            account_key="paper:model-a:cn:CNY",
            source_model="model-a",
            market="cn",
            currency="CNY",
            initial_capital=100000,
            cash=90000,
            invested_capital=10000,
            realized_pnl=0,
            exposure=10000,
            open_position_count=1,
        )
        session.add(account)
        session.flush()
        session.add(PaperPosition(
            plan_id=plan["id"],
            entry_leg_id="legacy_entry",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            status="open",
            opened_on=date(2026, 8, 25),
            opened_at=None,
            opened_price=100.0,
            normalized_notional=10000,
            quantity=100,
            capital_account_id=account.id,
            currency="CNY",
            allocated_capital=10000,
            stop_loss=95.0,
            target_price=130.0,
            max_holding_sessions=10,
        ))
        for trace_id, created_at in [
            ("old-before-close", datetime(2026, 8, 25, 6, 59)),
            ("new-after-close", datetime(2026, 8, 25, 7, 10)),
        ]:
            session.add(DecisionSignalRecord(
                stock_code="600519",
                stock_name="贵州茅台",
                market="cn",
                source_type="analysis",
                source_report_id=report_id,
                source_agent="model-a",
                trace_id=trace_id,
                trigger_source="system",
                action="sell",
                horizon="1d",
                plan_quality="partial",
                status="active",
                created_at=created_at,
            ))

    DatabaseManager.reset_instance()
    Config.reset_instance()
    migrated_db = DatabaseManager.get_instance()
    migrated_service = _single_confirmation_service()
    with migrated_db.get_session() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        assert position.opened_at == datetime(2026, 8, 25, 7, 0)

    bars = _bars(start=date(2026, 8, 20), days=7)
    bars[-1]["date"] = date(2026, 8, 26)
    bars[-1]["low"] = 98.0
    bars[-1]["high"] = 102.0
    bars[-1]["close"] = 101.0
    result = migrated_service.run_eod(date(2026, 8, 26), {("cn", "600519"): bars})

    assert result["positions_closed"] == 0
    with migrated_db.get_session() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        assert position.status == "open"
        assert position.close_reason is None
        evaluation = session.execute(
            select(PaperPlanEvaluation)
            .where(PaperPlanEvaluation.position_id == position.id)
            .order_by(PaperPlanEvaluation.id.desc())
        ).scalar_one()
        evidence = json.loads(evaluation.evidence_json)
        assert "missing_or_invalid_holding_action" in evidence["defensive_signal"]["eligibility"]["reasons"]


def test_service_factory_clamps_composite_threshold_to_registry_bounds(isolated_db):
    service = PaperTradingService.from_config(SimpleNamespace(
        paper_trading_intraday_min_factor_domains=2,
        paper_trading_intraday_min_composite_score=2.0,
        paper_trading_intraday_min_risk_reward=1.2,
        paper_trading_intraday_max_quote_stale_seconds=360,
    ))

    assert service.intraday_min_composite_score == 1.0


@pytest.mark.parametrize(
    "overrides,watch,expected_reason",
    [
        ({"entry_low": float("inf")}, ["放量"], "invalid_entry_low"),
        ({"entry_low": 120.0, "entry_high": 110.0}, ["放量"], "contradictory_entry_range"),
        ({"stop_loss": 140.0, "target_price": 130.0}, ["放量"], "contradictory_exit_prices"),
    ],
)
def test_invalid_price_or_risk_geometry_remains_non_executable(isolated_db, overrides, watch, expected_reason):
    report_id, signal_id = _source(isolated_db, watch=watch, **overrides)
    item = _single_confirmation_service().create_plan_for_report(
        report_id, decision_signal_id=signal_id
    )["item"]
    assert item["executable"] is False
    assert item["status"] == "non_executable"
    assert any(expected_reason in entry["text"] for entry in item["descriptive_conditions"])


def test_qualitative_watch_is_unavailable_evidence_not_execution_blocker(isolated_db):
    report_id, signal_id = _source(isolated_db, watch=["关注量能改善"])
    item = _single_confirmation_service().create_plan_for_report(
        report_id,
        decision_signal_id=signal_id,
    )["item"]

    assert item["executable"] is True
    unavailable = [
        entry for entry in item["descriptive_conditions"]
        if entry["reason"] == "unavailable_qualitative_condition"
    ]
    assert unavailable
    assert unavailable[0]["available"] is False


def test_report_day_never_triggers_and_missing_or_partial_bar_fails_closed(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 3))
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    same_day = service.run_eod(date(2026, 8, 3), {("cn", "600519"): _bars(start=date(2026, 7, 29), days=6)})
    assert same_day["triggered"] == 0
    assert same_day["evaluations"][0]["result"] == "skipped"

    missing = service.run_eod(date(2026, 8, 4), {("cn", "600519"): []})
    assert missing["failed_closed"] == 1
    assert missing["evaluations"][0]["reasons"] == ["missing_or_ambiguous_evaluation_bar"]

    report2, signal2 = _source(isolated_db, created_on=date(2026, 8, 4))
    service.create_plan_for_report(report2, decision_signal_id=signal2)
    partial_bars = _bars(start=date(2026, 7, 31), days=6)
    partial_bars[-1]["is_complete"] = False
    partial = service.run_eod(date(2026, 8, 5), {("cn", "600519"): partial_bars})
    assert partial["failed_closed"] >= 1
    assert all(event["event_type"] != "open" for event in partial["events"])


def test_prior_only_volume_window_opens_once_and_stop_closes_once(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 3))
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    entry_date = date(2026, 8, 4)
    bars = _bars(start=date(2026, 7, 30), days=6, current_volume=151.0)

    opened = service.run_eod(entry_date, {("cn", "600519"): bars})
    duplicate = service.run_eod(entry_date, {("cn", "600519"): bars})

    assert opened["positions_opened"] == 1
    assert opened["evaluations"][0]["result"] == "triggered"
    volume_evidence = next(
        item for item in opened["evaluations"][0]["evidence"]["rules"]
        if item["rule"]["type"] == "volume_ratio_gte_prior_avg"
    )
    assert entry_date.isoformat() not in volume_evidence["prior_window_dates"]
    assert volume_evidence["threshold"] == 150.0
    assert duplicate["positions_opened"] == 0

    exit_bars = bars + [{
        "date": date(2026, 8, 5), "open": 100.0, "high": 101.0,
        "low": 79.0, "close": 85.0, "volume": 100.0, "is_complete": True,
    }]
    closed = service.run_eod(date(2026, 8, 5), {("cn", "600519"): exit_bars})
    duplicate_close = service.run_eod(date(2026, 8, 5), {("cn", "600519"): exit_bars})
    assert closed["positions_closed"] == 1
    assert closed["events"][0]["reason"] == "stop_loss"
    assert duplicate_close["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.query(PaperPosition).one()
        assert position.status == "closed"
        assert position.normalized_notional == pytest.approx(20000.0)
        assert position.initial_allocated_capital == pytest.approx(20000.0)
        assert position.remaining_cost == pytest.approx(0.0)
        assert position.quantity == pytest.approx(0.0)
        assert position.realized_pnl == pytest.approx(-1000.0)
        assert position.simulated_return_pct == pytest.approx(-5.0)
        account = session.query(PaperCapitalAccount).one()
        assert account.open_position_count == 0
        assert account.exposure == pytest.approx(0.0)
        assert account.realized_pnl == pytest.approx(-1000.0)
        assert session.query(PaperPlanEvaluation).count() == 2
        assert session.query(PaperTradeEvent).count() == 2


def test_all_monitor_entries_call_central_resolver(isolated_db, monkeypatch):
    calls = []
    original = PaperTradingService.resolve_paper_intents

    def spy(self, *, candidates, gates=None, state=None):
        calls.append((state or {}).get("entry"))
        return original(self, candidates=candidates, gates=gates, state=state)

    monkeypatch.setattr(PaperTradingService, "resolve_paper_intents", spy)

    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 3), model="model-eod")
    eod_service = _single_confirmation_service()
    eod_service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    bars = _bars(start=date(2026, 7, 30), days=6, current_volume=151.0)
    opened = eod_service.run_eod(date(2026, 8, 4), {("cn", "600519"): bars})
    assert opened["positions_opened"] == 1
    exit_bars = bars + [{
        "date": date(2026, 8, 5), "open": 100.0, "high": 101.0,
        "low": 79.0, "close": 85.0, "volume": 100.0, "is_complete": True,
    }]
    eod_service.run_eod(date(2026, 8, 5), {("cn", "600519"): exit_bars})

    _seed_open_paper_lot(
        isolated_db,
        opened_on=date(2026, 8, 10),
        stock_code="002714",
        model="model-intraday-position",
    )
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 10))):
        _single_confirmation_service().run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                code="002714",
                price=90,
                provider_timestamp="2026-08-10T10:00:00+08:00",
                fetched_at="2026-08-10T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    plan_report_id, plan_signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        report_code="002715",
        signal_code="002715",
        model="model-intraday-plan",
    )
    plan_service = _single_confirmation_service()
    plan_service.create_plan_for_report(plan_report_id, decision_signal_id=plan_signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        plan_service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(code="002715")),
            interval_seconds=300,
        )

    assert {"eod_open", "eod_close", "intraday_position", "intraday_plan"}.issubset(set(calls))


def test_concurrent_intraday_workers_do_not_duplicate_resolution_or_open(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), model="model-concurrent")
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))
    now = datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc)

    def run_once():
        with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
            return _single_confirmation_service().run_intraday_monitor(
                current_time=now,
                fetcher_manager=_QuoteManager(_quote()),
                interval_seconds=300,
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _index: run_once(), range(2)))

    assert sum(item["positions_opened"] for item in results) == 1
    with isolated_db.get_session() as session:
        assert session.query(PaperPosition).count() == 1
        assert session.query(PaperPlanObservation).filter_by(action="open", result="triggered").count() == 1
        assert session.query(PaperResolution).filter_by(selected_intent="open").count() == 1


def test_eod_expires_stale_active_plan_and_only_opens_current_valid_plan(isolated_db):
    old_report, old_signal = _source(isolated_db, created_on=date(2026, 8, 1), model="model-a")
    new_report, new_signal = _source(isolated_db, created_on=date(2026, 8, 7), model="model-a")
    _other_plan_id, other_position_id = _seed_open_paper_lot(
        isolated_db,
        opened_on=date(2026, 8, 8),
        stock_code="002714",
    )
    _persist_daily_rows(
        isolated_db,
        _bars(start=date(2026, 8, 5), days=6),
        code="002714.SZ",
    )
    eval_date = date(2026, 8, 10)
    with isolated_db.session_scope() as session:
        old_plan = PaperExecutionPlan(
            plan_key="stale-eod-active",
            plan_version="paper-hybrid-v3",
            source_report_id=old_report,
            decision_signal_id=old_signal,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            report_date=date(2026, 8, 1),
            report_created_at=datetime(2026, 8, 1),
            valid_for_date=date(2026, 8, 8),
            action="buy",
            executable=True,
            status="active",
            rules_json=json.dumps([
                {"type": "close_between", "low": 96.0, "high": 100.0},
                {"type": "volume_ratio_gte_prior_avg", "ratio": 1.5, "period": 5},
            ]),
            descriptive_conditions_json=json.dumps(_strategy_setup_conditions(), ensure_ascii=False),
            stop_loss=95.0,
            target_price=130.0,
            max_holding_sessions=5,
        )
        new_plan = PaperExecutionPlan(
            plan_key="current-eod-active",
            plan_version="paper-hybrid-v3",
            source_report_id=new_report,
            decision_signal_id=new_signal,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            report_date=date(2026, 8, 7),
            report_created_at=datetime(2026, 8, 7),
            valid_for_date=eval_date,
            action="buy",
            executable=True,
            status="active",
            rules_json=json.dumps([
                {"type": "close_between", "low": 96.0, "high": 100.0},
                {"type": "volume_ratio_gte_prior_avg", "ratio": 1.5, "period": 5},
            ]),
            descriptive_conditions_json=json.dumps(_strategy_setup_conditions(), ensure_ascii=False),
            stop_loss=95.0,
            target_price=130.0,
            max_holding_sessions=5,
        )
        session.add_all([old_plan, new_plan])
        session.flush()
        old_plan_id = old_plan.id
        new_plan_id = new_plan.id

    bars = _bars(start=date(2026, 8, 5), days=6, current_volume=151.0)
    result = _single_confirmation_service().run_eod(eval_date, {("cn", "600519"): bars})

    assert result["positions_opened"] == 1
    assert result["triggered"] == 1
    with isolated_db.get_session() as session:
        old_plan = session.get(PaperExecutionPlan, old_plan_id)
        new_plan = session.get(PaperExecutionPlan, new_plan_id)
        assert old_plan.status == "superseded"
        assert new_plan.status == "triggered"
        assert session.query(PaperPosition).filter_by(plan_id=old_plan_id).count() == 0
        assert session.query(PaperPosition).filter_by(plan_id=new_plan_id, status="open").count() == 1
        assert session.get(PaperPosition, other_position_id).status == "open"


def test_ambiguous_exit_bar_fails_closed_without_closing(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 3))
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    bars = _bars(start=date(2026, 7, 30), days=6)
    service.run_eod(date(2026, 8, 4), {("cn", "600519"): bars})
    ambiguous = bars + [{
        "date": date(2026, 8, 5), "open": 100.0, "high": 131.0,
        "low": 79.0, "close": 100.0, "volume": 100.0, "is_complete": True,
    }]
    result = service.run_eod(date(2026, 8, 5), {("cn", "600519"): ambiguous})
    assert result["failed_closed"] == 1
    assert result["positions_closed"] == 0
    assert result["evaluations"][0]["reasons"] == ["ambiguous_stop_and_target_same_bar"]
    assert service.list_positions(status="open")["total"] == 1


def test_run_monitor_uses_real_persisted_daily_rows_and_performance(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 3))
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    bars = _bars(start=date(2026, 7, 30), days=6)
    with isolated_db.session_scope() as session:
        for item in bars:
            session.add(StockDaily(
                code="600519.SH", date=item["date"], open=item["open"], high=item["high"],
                low=item["low"], close=item["close"], volume=item["volume"],
                data_source="paper-runtime-proof|vol=shares",
            ))

    result = service.run_monitor(date(2026, 8, 4))
    stats = service.performance("model-a")
    assert result["positions_opened"] == 1
    assert stats == {
        "model": "model-a",
        "total_plans": 1,
        "executable_plans": 1,
        "open_positions": 1,
        "closed_positions": 0,
        "wins": 0,
        "losses": 0,
        "win_rate_pct": None,
        "gross_realized_pnl": 0.0,
        "net_realized_pnl": 0.0,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "total_pnl": 0.0,
        "market_value": 20000.0,
        "valuation_complete": True,
        "unpriced_position_count": 0,
        "unpriced_holding_count": 0,
        "realized_return_pct": None,
        "avg_return_pct": None,
        "realized_positions": 0,
        "partially_realized_positions": 0,
        "total_fees": 0.0,
        "total_commission": 0.0,
        "total_sell_tax": 0.0,
        "total_slippage_amount": 0.0,
        "explicit_cost_total": 0.0,
        "normalized_notional_per_position": 100000.0,
    }


def test_schema_create_all_is_idempotent_and_contains_no_order_surface(isolated_db):
    from src.storage import Base

    Base.metadata.create_all(isolated_db._engine)
    Base.metadata.create_all(isolated_db._engine)
    forbidden_substrings = ("broker", "live_order", "credential")
    forbidden_exact = {"order_id", "broker_account_id", "live_account_id"}
    for table in (PaperExecutionPlan, PaperPlanEvaluation, PaperPosition, PaperTradeEvent, PaperPlanObservation):
        column_names = {column.name for column in table.__table__.columns}
        assert not any(token in name for name in column_names for token in forbidden_substrings)
        assert not any(name in forbidden_exact for name in column_names)


def test_deleting_source_history_cleans_paper_ledger_children(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 1))
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    service.run_eod(
        date(2026, 8, 8),
        {("cn", "600519"): _bars(start=date(2026, 8, 3), days=6)},
    )

    assert isolated_db.delete_analysis_history_records([report_id]) == 1
    with isolated_db.get_session() as session:
        assert session.query(PaperTradeEvent).count() == 0
        assert session.query(PaperPosition).count() == 0
        assert session.query(PaperPlanEvaluation).count() == 0
        assert session.query(PaperExecutionPlan).count() == 0
        assert session.query(DecisionSignalRecord).count() == 0


def test_compound_conditions_preserve_all_rules_and_explicit_volume_wins(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        watch=["放量且量比>=1.2且站稳前高0.632", "站上MA10"],
    )
    item = _single_confirmation_service().create_plan_for_report(
        report_id, decision_signal_id=signal_id
    )["item"]
    assert item["executable"] is True
    volume_rules = [rule for rule in item["rules"] if rule["type"] == "volume_ratio_gte_prior_avg"]
    assert len(volume_rules) == 1
    assert volume_rules[0]["ratio"] == 1.2
    assert volume_rules[0]["threshold_source"] == "source_text"
    assert {rule["type"] for rule in item["rules"]} >= {
        "close_between", "close_gte", "close_above_prior_sma",
    }


def test_production_like_watch_text_builds_v3_executable_plan_with_unavailable_context(isolated_db):
    production_raw = {
        "model_used": "model-a",
        "dashboard": {
            "battle_plan": {
                "action_checklist": [
                    "若低开回踩MA5不破可轻仓试错",
                    "若板块走弱则降低仓位",
                ],
            },
            "phase_decision": {
                "watch_conditions": [
                    "量比能否放大至1.2以上配合上攻",
                    "大盘创新药板块是否企稳",
                ],
            },
        },
    }
    report_id, signal_id = _source(
        isolated_db,
        watch=[
            "明日开盘是否回踩MA5（1.273）不破",
            "量比能否放大至1.2以上配合上攻",
        ],
        raw_result=production_raw,
    )

    item = _single_confirmation_service().create_plan_for_report(
        report_id,
        decision_signal_id=signal_id,
    )["item"]

    assert item["plan_version"] == "paper-hybrid-v3"
    assert item["executable"] is True
    volume_rules = [rule for rule in item["rules"] if rule["type"] == "volume_ratio_gte_prior_avg"]
    assert len(volume_rules) == 1
    assert volume_rules[0]["ratio"] == 1.2
    assert volume_rules[0]["threshold_source"] == "source_text"
    assert "action_checklist" not in {rule.get("source") for rule in item["rules"]}
    unavailable_texts = {
        entry["text"] for entry in item["descriptive_conditions"]
        if entry.get("reason") == "unavailable_qualitative_condition"
    }
    assert "明日开盘是否回踩MA5（1.273）不破" in unavailable_texts
    assert "大盘创新药板块是否企稳" in unavailable_texts
    audit_texts = {
        entry["text"] for entry in item["descriptive_conditions"]
        if entry.get("reason") == "audit_context_only"
    }
    assert "若低开回踩MA5不破可轻仓试错" in audit_texts


def test_partially_understood_compound_text_retains_rules_and_unavailable_context(isolated_db):
    report_id, signal_id = _source(isolated_db, watch=["放量站上MA10并关注量能"])
    item = _single_confirmation_service().create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    assert item["executable"] is True
    assert {rule["type"] for rule in item["rules"]} >= {
        "volume_ratio_gte_prior_avg", "close_above_prior_sma",
    }
    assert item["descriptive_conditions"][0]["text"] == "放量站上MA10并关注量能"
    assert item["descriptive_conditions"][0]["reason"] == "unavailable_qualitative_condition"


def test_bare_stockdaily_code_never_matches_suffix_plan_and_failed_day_retries(isolated_db):
    report_id, signal_id = _source(
        isolated_db, created_on=date(2026, 8, 3),
        report_code="512480.SH", signal_code="512480.SH",
    )
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    bars = _bars(start=date(2026, 7, 30), days=6)
    with isolated_db.session_scope() as session:
        for item in bars:
            session.add(StockDaily(
                code="512480",
                data_source="paper-bare-code-proof|vol=shares",
                **{key: value for key, value in item.items() if key != "is_complete"},
            ))
    first = service.run_monitor(date(2026, 8, 4), current_time=datetime(2026, 8, 4, 18, 0))
    assert first["failed_closed"] == 1 and first["positions_opened"] == 0
    with isolated_db.session_scope() as session:
        for item in bars:
            session.add(StockDaily(
                code="512480.SH",
                data_source="paper-suffix-code-proof|vol=shares",
                **{key: value for key, value in item.items() if key != "is_complete"},
            ))
    retried = service.run_monitor(date(2026, 8, 4), current_time=datetime(2026, 8, 4, 18, 0))
    assert retried["positions_opened"] == 1
    with isolated_db.get_session() as session:
        assert session.query(PaperPlanEvaluation).count() == 1


@pytest.mark.parametrize(
    "market,report_code,signal_code,daily_code",
    [("hk", "00700.HK", "HK00700", "00700.HK"), ("us", "AAPL.US", "AAPL", "AAPL.US")],
)
def test_current_session_before_close_is_not_complete(
    isolated_db, market, report_code, signal_code, daily_code,
):
    report_id, signal_id = _source(
        isolated_db, created_on=date(2026, 8, 7), market=market,
        report_code=report_code, signal_code=signal_code,
    )
    service = _single_confirmation_service()
    with isolated_db.session_scope() as session:
        session.add(PaperExecutionPlan(
            plan_key=f"current-session-incomplete:{market}:{signal_code}",
            plan_version="paper-hybrid-v3",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market=market,
            stock_code=signal_code,
            stock_name="贵州茅台",
            report_date=date(2026, 8, 7),
            report_created_at=datetime(2026, 8, 7),
            valid_for_date=date(2026, 8, 10),
            action="buy",
            executable=True,
            status="active",
            rules_json=json.dumps([
                {"type": "close_between", "low": 96.0, "high": 100.0},
                {"type": "volume_ratio_gte_prior_avg", "ratio": 1.5, "period": 5},
            ]),
            descriptive_conditions_json=json.dumps(_strategy_setup_conditions(), ensure_ascii=False),
            stop_loss=95.0,
            target_price=130.0,
            max_holding_sessions=5,
        ))
    bars = _bars(start=date(2026, 8, 5), days=6)
    with isolated_db.session_scope() as session:
        for item in bars:
            session.add(StockDaily(code=daily_code, **{key: value for key, value in item.items() if key != "is_complete"}))
    result = service.run_monitor(date(2026, 8, 10), current_time=datetime(2026, 8, 10, 15, 10))
    assert result["failed_closed"] == 1, result
    assert result["positions_opened"] == 0
    assert result["evaluations"][0]["reasons"] == ["incomplete_or_non_finite_evaluation_bar"]


def test_defensive_signal_is_isolated_by_source_model(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 3), model="model-a")
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    bars = _bars(start=date(2026, 7, 30), days=6)
    service.run_eod(date(2026, 8, 4), {("cn", "600519"): bars})
    _source(isolated_db, created_on=date(2026, 8, 5), model="model-b", action="sell")
    held_bars = bars + [{"date": date(2026, 8, 5), "open": 100, "high": 105, "low": 96, "close": 100, "volume": 100, "is_complete": True}]
    held = service.run_eod(date(2026, 8, 5), {("cn", "600519"): held_bars})
    assert held["positions_closed"] == 0
    _confirmed_defensive_signal(
        isolated_db,
        service,
        action="sell",
        model="model-a",
        first_day=date(2026, 8, 6),
        second_day=date(2026, 8, 7),
    )
    close_bars = held_bars + [{"date": date(2026, 8, 6), "open": 100, "high": 105, "low": 96, "close": 100, "volume": 100, "is_complete": True}]
    pending = service.run_eod(date(2026, 8, 6), {("cn", "600519"): close_bars})
    assert pending["positions_closed"] == 0
    confirmed_bars = close_bars + [{"date": date(2026, 8, 7), "open": 100, "high": 105, "low": 96, "close": 100, "volume": 100, "is_complete": True}]
    closed = service.run_eod(date(2026, 8, 7), {("cn", "600519"): confirmed_bars})
    assert closed["positions_closed"] == 1
    assert closed["events"][0]["reason"] == "model_sell"


def test_conflicting_legal_stockdaily_variants_fail_closed_per_date(isolated_db):
    report_id, signal_id = _source(
        isolated_db, created_on=date(2026, 8, 3),
        report_code="159915.SZ", signal_code="159915.SZ",
    )
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    bars = _bars(start=date(2026, 7, 30), days=6)
    with isolated_db.session_scope() as session:
        for item in bars:
            payload = {key: value for key, value in item.items() if key != "is_complete"}
            session.add(StockDaily(code="159915.SZ", **payload))
            alternate = dict(payload)
            if item["date"] == date(2026, 8, 4):
                alternate.update(open=102.0, high=106.0, low=96.0, close=102.0)
            session.add(StockDaily(code="SZ159915", **alternate))
    rows = service.repo.daily_bars("cn", "159915", date(2026, 8, 4))
    assert len({row.date for row in rows}) == len(rows)
    assert rows[-1].code == "159915.SZ"
    assert rows[-1]._paper_variant_conflict is True
    result = service.run_monitor(date(2026, 8, 4), current_time=datetime(2026, 8, 4, 18, 0))
    assert result["positions_opened"] == 0
    assert result["evaluations"][0]["reasons"] == ["incomplete_or_non_finite_evaluation_bar"]


def test_performance_aggregates_more_than_500_rows_without_pagination(isolated_db):
    report_id, _ = _source(isolated_db, created_on=date(2026, 8, 1))
    with isolated_db.session_scope() as session:
        plans = []
        for index in range(501):
            plan = PaperExecutionPlan(
                plan_key=f"bulk:{index}", plan_version="paper-eod-v1",
                source_report_id=report_id, source_model="bulk-model", market="cn",
                stock_code="600519", report_date=date(2026, 8, 1),
                report_created_at=datetime(2026, 8, 1), action="buy", executable=True,
                status="closed", rules_json="[]", descriptive_conditions_json="[]",
                max_holding_sessions=5,
            )
            session.add(plan)
            plans.append(plan)
        session.flush()
        for plan in plans:
            session.add(PaperPosition(
                plan_id=plan.id, source_report_id=report_id, source_model="bulk-model",
                market="cn", stock_code="600519", status="closed",
                opened_on=date(2026, 8, 2), opened_price=100.0,
                normalized_notional=100000.0, quantity=1000.0,
                max_holding_sessions=5, closed_on=date(2026, 8, 3),
                closed_price=110.0, close_reason="target_price",
                simulated_return_pct=10.0, simulated_pnl=10000.0,
            ))
    result = _single_confirmation_service().performance("bulk-model")
    assert result["total_plans"] == 501
    assert result["closed_positions"] == 501
    assert result["wins"] == 501
    assert result["win_rate_pct"] == 100.0
    assert result["realized_return_pct"] == 10.0
    assert result["avg_return_pct"] == 10.0


def test_run_monitor_processes_all_501_open_positions(isolated_db):
    report_id, _ = _source(isolated_db, created_on=date(2026, 8, 1))
    with isolated_db.session_scope() as session:
        plans = []
        for index in range(501):
            plan = PaperExecutionPlan(
                plan_key=f"open-bulk:{index}", plan_version="paper-eod-v1",
                source_report_id=report_id, source_model="bulk-open-model", market="cn",
                stock_code="600519", report_date=date(2026, 8, 1),
                report_created_at=datetime(2026, 8, 1), action="buy", executable=True,
                status="triggered", rules_json="[]", descriptive_conditions_json="[]",
                stop_loss=90.0, target_price=130.0, max_holding_sessions=5,
            )
            session.add(plan)
            plans.append(plan)
        session.flush()
        for plan in plans:
            session.add(PaperPosition(
                plan_id=plan.id, source_report_id=report_id, source_model="bulk-open-model",
                market="cn", stock_code="600519", status="open",
                opened_on=date(2026, 8, 2), opened_price=100.0,
                normalized_notional=100000.0, quantity=1000.0,
                stop_loss=90.0, target_price=130.0, max_holding_sessions=5,
            ))
        session.add(StockDaily(
            code="600519.SH", date=date(2026, 8, 3), open=100.0,
            high=105.0, low=89.0, close=95.0, volume=100.0,
            data_source="paper-501-open-proof|vol=shares",
        ))
    result = _single_confirmation_service().run_monitor(
        date(2026, 8, 3), current_time=datetime(2026, 8, 4, 18, 0)
    )
    assert result["positions_closed"] == 501
    with isolated_db.get_session() as session:
        positions = session.query(PaperPosition).order_by(PaperPosition.id).all()
        assert len(positions) == 501
        assert positions[500].status == "closed"
        assert positions[500].close_reason == "stop_loss"
        assert session.query(PaperTradeEvent).filter_by(event_type="close").count() == 501


def test_intraday_monitor_requires_composite_factors_before_opening(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        score=65,
    )
    service = _single_confirmation_service()
    item = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    assert item["plan_version"] == "paper-hybrid-v3"
    _persist_daily_rows(
        isolated_db,
        _bars(start=date(2026, 7, 10), days=25, current_volume=100.0),
    )

    weak_quote = _quote(volume_ratio=0.8, change_pct=-0.2)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        weak = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(weak_quote),
            interval_seconds=60,
        )
    assert weak["positions_opened"] == 0
    assert weak["observations"] == 1
    weak_observation = service.list_observations()["items"][0]
    assert weak_observation["result"] == "not_triggered"
    assert weak_observation["factors_available"] == weak_observation["evidence"]["factors_available"]
    assert weak_observation["factors_passed"] == weak_observation["evidence"]["factors_passed"]

    strong_quote = _quote(volume_ratio=1.8, change_pct=1.1)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        strong = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 1),
            fetcher_manager=_QuoteManager(strong_quote),
            interval_seconds=60,
        )
        duplicate = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 1, 20),
            fetcher_manager=_QuoteManager(strong_quote),
            interval_seconds=60,
        )

    assert strong["positions_opened"] == 1
    assert duplicate["positions_opened"] == 0
    observations = [
        item for item in service.list_observations()["items"]
        if item["result"] != "valuation_mark"
    ]
    assert len(observations) == 2
    open_observation = next(item for item in observations if item["action"] == "open")
    assert open_observation["factors_passed"] >= 2
    assert open_observation["factors_available"] == open_observation["evidence"]["factors_available"]
    assert open_observation["factors_passed"] == open_observation["evidence"]["factors_passed"]
    assert open_observation["quote_source"] == "unit_quote"
    with isolated_db.get_session() as session:
        assert session.query(PaperPlanObservation).filter(
            PaperPlanObservation.result != "valuation_mark"
        ).count() == 2
        assert session.query(PaperPosition).count() == 1
        assert session.query(PaperTradeEvent).filter_by(event_type="open").count() == 1


def test_intraday_monitor_fails_closed_on_stale_quote(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), score=65)
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(_quote(stale=True)),
            interval_seconds=60,
        )
    assert result["failed_closed"] == 1
    observation = service.list_observations()["items"][0]
    assert observation["result"] == "failed_closed"
    assert observation["evidence"]["reasons"] == ["stale_realtime_quote"]
    assert service.list_positions()["total"] == 0


def test_intraday_monitor_fails_closed_on_stale_fetched_at_without_provider_timestamp(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), score=65)
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    fetched_only_old_quote = _quote(
        provider_timestamp=None,
        fetched_at="2026-08-10T09:40:00+08:00",
        stale_seconds=None,
    )
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(fetched_only_old_quote),
            interval_seconds=60,
        )

    assert result["failed_closed"] == 1
    observation = service.list_observations()["items"][0]
    assert observation["result"] == "failed_closed"
    assert observation["evidence"]["reasons"] == ["stale_realtime_quote"]
    assert service.list_positions()["total"] == 0


def test_intraday_volume_rule_requires_provider_volume_ratio(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["量比>=1.5"],
        score=65,
    )
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(_quote(volume_ratio=None, change_pct=1.0)),
            interval_seconds=60,
        )

    assert result["positions_opened"] == 0
    assert result["failed_closed"] == 1
    observation = service.list_observations()["items"][0]
    assert observation["required_passed"] is False
    assert "required_rule_unavailable:volume_ratio" in observation["evidence"]["reasons"]


def test_intraday_close_only_required_rule_records_required_not_passed(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["收盘站上MA10"],
        score=65,
    )
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(_quote(volume_ratio=2.0, change_pct=1.0)),
            interval_seconds=60,
        )

    assert result["positions_opened"] == 0
    assert result["failed_closed"] == 1
    observation = service.list_observations()["items"][0]
    assert observation["required_passed"] is False
    assert "required_rule_unavailable:close_only" in observation["evidence"]["reasons"]
    close_only_rules = [
        item for item in observation["evidence"]["required_rules"]
        if item.get("mode") == "close_only"
    ]
    assert close_only_rules
    assert close_only_rules[0]["available"] is False
    assert close_only_rules[0]["passed"] is False


def test_intraday_negative_momentum_is_available_but_not_passing(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), score=65)
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(_quote(volume_ratio=0.8, change_pct=-0.2)),
            interval_seconds=60,
        )
    observation = service.list_observations()["items"][0]
    momentum = next(
        item for item in observation["evidence"]["factor_domains"]
        if item["domain"] == "momentum"
    )
    assert momentum["available"] is True
    assert momentum["passed"] is False
    assert observation["result"] == "not_triggered"


def test_intraday_composite_excludes_report_conviction_score(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["站上MA5"],
        score=95,
        entry_high=101.0,
    )
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(_quote(price=100.5, high=100.5, volume_ratio=None, change_pct=1.0)),
            interval_seconds=60,
        )

    assert result["positions_opened"] == 0
    observation = service.list_observations()["items"][0]
    assert observation["result"] == "not_triggered"
    assert observation["composite_score"] == pytest.approx(0.55)
    assert "setup_score_below_threshold" in observation["evidence"]["reasons"]
    conviction = next(
        item for item in observation["evidence"]["factor_domains"]
        if item["domain"] == "report_conviction"
    )
    assert conviction["passed"] is True
    assert conviction["counts_for_entry"] is False


def test_intraday_monitor_skips_closing_auction_phase(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), score=65)
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_closing_phase()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(_quote()),
            interval_seconds=60,
        )
    assert result["skipped_closed_market"] == 1
    assert service.list_observations()["total"] == 0


def test_intraday_breakout_uses_current_price_not_session_high(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["突破20日高"],
        score=65,
    )
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(_quote(price=100.0, high=120.0, volume_ratio=2.0)),
            interval_seconds=60,
        )
    assert result["positions_opened"] == 0
    observation = service.list_observations()["items"][0]
    assert any(
        item["rule"]["type"] == "close_above_prior_high" and item["passed"] is False
        for item in observation["evidence"]["required_rules"]
    )


def test_intraday_stop_loss_closes_open_position_without_waiting_for_factors(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), score=65)
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(_quote()),
            interval_seconds=60,
        )
        closed = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 1),
            fetcher_manager=_QuoteManager(_quote(price=79.0, volume_ratio=0.1, change_pct=-5.0)),
            interval_seconds=60,
        )
    assert closed["positions_closed"] == 0
    position = service.list_positions()["items"][0]
    assert position["status"] == "open"
    assert position["pending_exit_reason"] == "intraday_stop_loss"

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        closed = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0),
            fetcher_manager=_QuoteManager(_quote(
                price=78.0,
                volume_ratio=0.1,
                change_pct=-5.0,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=60,
        )
    assert closed["positions_closed"] == 1
    position = service.list_positions()["items"][0]
    assert position["status"] == "closed"
    assert position["close_reason"] == "intraday_stop_loss"
    assert position["closed_price"] == 78


def test_intraday_monitor_upgrades_active_legacy_plan_once(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), score=65)
    with isolated_db.session_scope() as session:
        session.add(PaperExecutionPlan(
            plan_key="legacy-active",
            plan_version="paper-eod-v1",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            report_date=date(2026, 8, 7),
            report_created_at=datetime(2026, 8, 7),
            action="buy",
            executable=True,
            status="active",
            rules_json=json.dumps([{"type": "close_between", "low": 96.0, "high": 100.0}]),
            descriptive_conditions_json="[]",
            stop_loss=95.0,
            target_price=130.0,
            max_holding_sessions=5,
        ))
    service = _single_confirmation_service()
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(_quote()),
            interval_seconds=60,
        )
    assert result["legacy_plans_upgraded"] == 1
    with isolated_db.get_session() as session:
        versions = {row.plan_version: row.status for row in session.query(PaperExecutionPlan).all()}
        assert versions["paper-eod-v1"] == "superseded"
        assert "paper-hybrid-v3" in versions


def test_intraday_monitor_upgrades_active_v2_plan_without_position(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), score=65)
    with isolated_db.session_scope() as session:
        session.add(PaperExecutionPlan(
            plan_key="v2-active",
            plan_version="paper-hybrid-v2",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            report_date=date(2026, 8, 7),
            report_created_at=datetime(2026, 8, 7),
            action="buy",
            executable=True,
            status="active",
            rules_json=json.dumps([{"type": "close_between", "low": 96.0, "high": 100.0}]),
            descriptive_conditions_json="[]",
            stop_loss=95.0,
            target_price=130.0,
            max_holding_sessions=5,
        ))
    service = _single_confirmation_service()
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(_quote()),
            interval_seconds=60,
        )

    assert result["legacy_plans_upgraded"] == 1
    with isolated_db.get_session() as session:
        old_v2 = session.query(PaperExecutionPlan).filter_by(plan_version="paper-hybrid-v2").one()
        assert old_v2.status == "superseded"
        new_v3 = session.query(PaperExecutionPlan).filter_by(plan_version="paper-hybrid-v3").one()
        assert new_v3.status == "triggered"
        assert new_v3.executable is True
        assert session.query(PaperPosition).filter_by(plan_id=new_v3.id, status="open").count() == 1


def test_intraday_monitor_upgrades_legacy_even_when_old_non_executable_v2_exists(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), score=65)
    with isolated_db.session_scope() as session:
        session.add(PaperExecutionPlan(
            plan_key="legacy-active-with-bad-v2",
            plan_version="paper-eod-v1",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            report_date=date(2026, 8, 7),
            report_created_at=datetime(2026, 8, 7),
            action="buy",
            executable=True,
            status="active",
            rules_json=json.dumps([{"type": "close_between", "low": 96.0, "high": 100.0}]),
            descriptive_conditions_json="[]",
            stop_loss=95.0,
            target_price=130.0,
            max_holding_sessions=5,
        ))
        session.add(PaperExecutionPlan(
            plan_key=f"analysis:{report_id}:signal:{signal_id}:paper-hybrid-v2",
            plan_version="paper-hybrid-v2",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            report_date=date(2026, 8, 7),
            report_created_at=datetime(2026, 8, 7),
            action="buy",
            executable=False,
            status="non_executable",
            rules_json="[]",
            descriptive_conditions_json=json.dumps([{"reason": "preexisting_bad_v2"}]),
            stop_loss=80.0,
            target_price=130.0,
            max_holding_sessions=5,
        ))
    service = _single_confirmation_service()
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(_quote()),
            interval_seconds=60,
        )

    assert result["legacy_plans_upgraded"] == 1
    with isolated_db.get_session() as session:
        legacy = session.query(PaperExecutionPlan).filter_by(plan_version="paper-eod-v1").one()
        assert legacy.status == "superseded"
        old_v2 = session.query(PaperExecutionPlan).filter_by(plan_version="paper-hybrid-v2").one()
        assert old_v2.status == "non_executable"
        new_v3 = session.query(PaperExecutionPlan).filter_by(plan_version="paper-hybrid-v3").one()
        assert new_v3.status == "triggered"
        assert new_v3.executable is True
        assert session.query(PaperPosition).filter_by(plan_id=new_v3.id, status="open").count() == 1


def test_intraday_monitor_closes_open_legacy_position_without_duplicate_plan(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), score=65)
    with isolated_db.session_scope() as session:
        plan = PaperExecutionPlan(
            plan_key="legacy-triggered",
            plan_version="paper-eod-v1",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            report_date=date(2026, 8, 7),
            report_created_at=datetime(2026, 8, 7),
            action="buy",
            executable=True,
            status="triggered",
            rules_json="[]",
            descriptive_conditions_json="[]",
            stop_loss=80.0,
            target_price=130.0,
            max_holding_sessions=5,
        )
        session.add(plan)
        session.flush()
        session.add(PaperPosition(
            plan_id=plan.id,
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            status="open",
            opened_on=date(2026, 8, 8),
            opened_price=100.0,
            normalized_notional=100000.0,
            quantity=1000.0,
            stop_loss=80.0,
            target_price=130.0,
            max_holding_sessions=5,
        ))
    service = _single_confirmation_service()
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(_quote(price=79.0, volume_ratio=0.1, change_pct=-5.0)),
            interval_seconds=60,
        )
    assert result["positions_closed"] == 1
    assert result["legacy_plans_upgraded"] == 0
    with isolated_db.get_session() as session:
        assert session.query(PaperExecutionPlan).count() == 1
        assert session.query(PaperPosition).count() == 1


def test_intraday_monitor_skips_closing_auction_even_if_calendar_marks_open(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), score=65)
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase_closing_auction()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 6, 57),
            fetcher_manager=_QuoteManager(_quote()),
            interval_seconds=60,
        )
    assert result["skipped_closed_market"] == 1
    assert result["observations"] == 0
    assert service.list_positions()["total"] == 0


def test_intraday_breakout_requires_current_price_not_only_day_high(isolated_db):
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        watch=["突破20日高"],
        score=65,
    )
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    bars = _bars(start=date(2026, 7, 10), days=25)
    for item in bars:
        item["high"] = 105.0
    _persist_daily_rows(isolated_db, bars)

    pulled_back_quote = _quote(price=100.0, high=110.0, volume_ratio=2.0, change_pct=1.0)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0),
            fetcher_manager=_QuoteManager(pulled_back_quote),
            interval_seconds=60,
        )
    assert result["positions_opened"] == 0
    observation = service.list_observations()["items"][0]
    assert "rule_not_met:close_above_prior_high" in observation["evidence"]["reasons"]


def test_sync_portfolio_position_to_selected_paper_model_is_idempotent(isolated_db):
    _source(isolated_db, report_code="600519.SH", signal_code="600519", model="model-a")
    portfolio = PortfolioService()
    account = portfolio.create_account(name="cn", broker="manual", market="cn", base_currency="CNY")
    portfolio.record_cash_ledger(account_id=account["id"], event_date=date(2026, 8, 1), direction="in", amount=100000)
    portfolio.record_trade(
        account_id=account["id"],
        symbol="SH600519",
        trade_date=date(2026, 8, 2),
        side="buy",
        quantity=200,
        price=100,
        market="cn",
        currency="CNY",
    )
    portfolio.get_portfolio_snapshot(account_id=account["id"], as_of=date(2026, 8, 3), include_realtime=False)
    position_id = _portfolio_position_id_for(isolated_db, account_id=account["id"])
    service = _single_confirmation_service(initial_capital=50000)

    with patch("src.services.paper_trading_service.validate_analysis_models", return_value=["model-a"]):
        result = service.sync_portfolio_position_to_paper(
            portfolio_position_id=position_id,
            source_models=["model-a"],
            trade_date=date(2026, 8, 3),
            idempotency_key="sync-a",
        )
        retry = service.sync_portfolio_position_to_paper(
            portfolio_position_id=position_id,
            source_models=["model-a"],
            trade_date=date(2026, 8, 3),
            idempotency_key="sync-a",
        )

    assert result["created"] is True
    assert retry["created"] is False
    refreshed = portfolio.get_portfolio_snapshot(
        account_id=account["id"],
        as_of=date(2026, 8, 4),
        include_realtime=False,
    )
    assert refreshed["accounts"][0]["positions"]
    context = service.analysis_context_for_stock(source_model="model-a", stock_code="600519.SH")
    assert context["holding_state"] == "holding"
    assert context["source_model"] == "model-a"
    assert context["quantity"] == 200
    with isolated_db.get_session() as session:
        paper_position = session.execute(select(PaperPosition)).scalar_one()
        assert paper_position.source_model == "model-a"
        assert paper_position.sync_origin == "portfolio_import"
        assert paper_position.sync_status == "synced_from_portfolio"
        assert paper_position.source_portfolio_position_id == position_id
        assert paper_position.quantity == 200
        link = session.execute(select(PaperSyncLink)).scalar_one()
        assert link.portfolio_position_id == position_id
        assert link.paper_position_id == paper_position.id
        assert session.query(PaperTradeEvent).filter_by(reason="portfolio_import").count() == 1
        paper_account = session.execute(select(PaperCapitalAccount)).scalar_one()
        assert paper_account.cash == 30000


def test_analysis_context_includes_empty_account_capital_without_holding(isolated_db):
    service = _single_confirmation_service(initial_capital=50000)
    with patch("src.services.paper_trading_service.validate_analysis_models", return_value=["model-a"]):
        service.initialize_account(source_model="model-a", market="cn", initial_capital=80000)

    context = service.analysis_context_for_stock(source_model="model-a", stock_code="600519.SH")

    assert context["source"] == "paper_trading"
    assert context["source_model"] == "model-a"
    assert context["holding_state"] == "empty"
    assert context["quantity"] == 0
    assert context["cash"] == 80000
    assert context["book_equity"] == 80000
    assert context["exposure"] == 0
    assert context["capital_account_id"] is not None


def test_analysis_context_isolated_by_model_account_and_position(isolated_db):
    report_a, signal_a = _source(
        isolated_db,
        report_code="600519.SH",
        signal_code="600519",
        model="model-a",
    )
    report_b, signal_b = _source(
        isolated_db,
        report_code="600519.SH",
        signal_code="600519",
        model="model-b",
    )
    with isolated_db.session_scope() as session:
        account_a = PaperCapitalAccount(
            account_key="model-a:cn:CNY",
            source_model="model-a",
            market="cn",
            currency="CNY",
            initial_capital=100000,
            cash=90000,
            exposure=10000,
        )
        account_b = PaperCapitalAccount(
            account_key="model-b:cn:CNY",
            source_model="model-b",
            market="cn",
            currency="CNY",
            initial_capital=60000,
            cash=30000,
            exposure=30000,
        )
        session.add_all([account_a, account_b])
        session.flush()
        plan_a = PaperExecutionPlan(
            plan_key="ctx-plan-a",
            plan_version="paper-hybrid-v3",
            source_report_id=report_a,
            decision_signal_id=signal_a,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            report_date=date(2026, 8, 3),
            report_created_at=datetime(2026, 8, 3),
            action="buy",
            executable=True,
            status="triggered",
            rules_json="[]",
            descriptive_conditions_json="[]",
            max_holding_sessions=10,
        )
        plan_b = PaperExecutionPlan(
            plan_key="ctx-plan-b",
            plan_version="paper-hybrid-v3",
            source_report_id=report_b,
            decision_signal_id=signal_b,
            source_model="model-b",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            report_date=date(2026, 8, 3),
            report_created_at=datetime(2026, 8, 3),
            action="buy",
            executable=True,
            status="triggered",
            rules_json="[]",
            descriptive_conditions_json="[]",
            max_holding_sessions=10,
        )
        session.add_all([plan_a, plan_b])
        session.flush()
        session.add_all([
            PaperPosition(
                plan_id=plan_a.id,
                source_report_id=report_a,
                decision_signal_id=signal_a,
                source_model="model-a",
                market="cn",
                stock_code="600519",
                stock_name="贵州茅台",
                status="open",
                opened_on=date(2026, 8, 4),
                opened_price=100,
                normalized_notional=10000,
                quantity=100,
                currency="CNY",
                allocated_capital=10000,
                capital_account_id=account_a.id,
                stop_loss=95,
                target_price=130,
                max_holding_sessions=10,
                sync_origin="portfolio_import",
                sync_status="synced_from_portfolio",
            ),
            PaperPosition(
                plan_id=plan_b.id,
                source_report_id=report_b,
                decision_signal_id=signal_b,
                source_model="model-b",
                market="cn",
                stock_code="600519",
                stock_name="贵州茅台",
                status="open",
                opened_on=date(2026, 8, 4),
                opened_price=120,
                normalized_notional=30000,
                quantity=250,
                currency="CNY",
                allocated_capital=30000,
                capital_account_id=account_b.id,
                stop_loss=110,
                target_price=150,
                max_holding_sessions=10,
            ),
        ])

    service = _single_confirmation_service()
    context_a = service.analysis_context_for_stock(source_model="model-a", stock_code="600519.SH")
    context_b = service.analysis_context_for_stock(source_model="model-b", stock_code="600519.SH")

    assert context_a["quantity"] == 100
    assert context_a["cash"] == 90000
    assert context_a["sync_status"] == "synced_from_portfolio"
    assert context_b["quantity"] == 250
    assert context_b["cash"] == 30000
    assert context_b["sync_status"] == "paper_only"


def test_feishu_open_payload_contains_paper_trade_risk_and_sync_context(isolated_db):
    service = _single_confirmation_service()
    event = SimpleNamespace(
        id=7,
        event_key="open:model-a:600519",
        source_model="model-a",
        market="cn",
        stock_code="600519",
        stock_name="贵州茅台",
        source_report_id=42,
        event_date=date(2026, 8, 24),
        created_at=datetime(2026, 8, 24, 2, 31),
        price=101.25,
        allocated_capital=20250,
        cash_after=79750,
        exposure_after=20250,
        details_json=json.dumps(
            {
                "quantity": 200,
                "stop_loss": 95,
                "target_price": 130,
                "risk_reward": 2.4,
                "sync_status": "synced_from_portfolio",
                "evidence_summary": "放量且站上MA5",
            },
            ensure_ascii=False,
        ),
    )
    account = SimpleNamespace(id=3, currency="CNY")

    payload = service._build_feishu_open_payload(event, account=account)

    assert payload["sync_status"] == "synced_from_portfolio"
    assert payload["followup_route"] == "/paper-trading?model=model-a"
    content = payload["content"]
    assert "仅纸面模拟，不连接券商、不代表真实下单" in content
    assert "模型: model-a" in content
    assert "数量: 200" in content
    assert "模拟买入价: 101.2500" in content
    assert "分配资金: 20250.00" in content
    assert "剩余现金: 79750.00" in content
    assert "账户敞口: 20250.00" in content
    assert "止损/止盈: 95 / 130" in content
    assert "同步状态: synced_from_portfolio" in content
    assert "后续确认: /paper-trading?model=model-a" in content
    assert "放量且站上MA5" in content


def test_feishu_close_payload_contains_exit_trade_details(isolated_db):
    service = _single_confirmation_service()
    event = SimpleNamespace(
        id=8,
        event_key="close:model-a:600519",
        source_model="model-a",
        market="cn",
        stock_code="600519",
        stock_name="贵州茅台",
        source_report_id=42,
        event_date=date(2026, 8, 25),
        created_at=datetime(2026, 8, 25, 2, 31),
        price=94.50,
        reason="intraday_stop_loss",
        allocated_capital=20000,
        cash_after=98900,
        exposure_after=0,
        currency="CNY",
        details_json=json.dumps(
            {
                "quantity": 200,
                "opened_price": 100,
                "closed_price": 94.5,
                "stop_loss": 95,
                "target_price": 130,
                "realized_pnl": -1100,
                "return_pct": -5.5,
                "fill_policy": "fresh_quote_next_actual_session",
            },
            ensure_ascii=False,
        ),
    )
    account = SimpleNamespace(id=3, currency="CNY")

    payload = service._build_feishu_lifecycle_payload(event, account=account, lifecycle="close")

    content = payload["content"]
    assert payload["lifecycle"] == "close"
    assert "生命周期: 模拟平仓" in content
    assert "模型: model-a" in content
    assert "价格: 94.5000" in content
    assert "数量: 200" in content
    assert "止损/止盈: 95 / 130" in content
    assert "证据: fresh_quote_next_actual_session" in content


def test_paper_trading_market_rule_schema_migrations_are_idempotent(isolated_db):
    def columns(db, table_name: str) -> set[str]:
        with db.get_session() as session:
            rows = session.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        return {str(row[1]) for row in rows}

    assert {
        "available_quantity",
        "frozen_quantity",
        "pending_exit_reason",
        "pending_exit_on",
        "pending_exit_price",
        "pending_exit_event_id",
        "sellability_rule",
    }.issubset(columns(isolated_db, "paper_positions"))
    assert "valid_for_date" in columns(isolated_db, "paper_execution_plans")

    DatabaseManager.reset_instance()
    Config.reset_instance()
    db = DatabaseManager.get_instance()
    assert "valid_for_date" in columns(db, "paper_execution_plans")
    assert db is not None


def test_paper_trading_runtime_migration_handles_fresh_legacy_partial_and_rerun(tmp_path):
    db_path = tmp_path / "paper-runtime-migration.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE paper_positions ("
            "id INTEGER PRIMARY KEY, status TEXT, source_model TEXT, market TEXT, stock_code TEXT, "
            "opened_price FLOAT, quantity FLOAT, normalized_notional FLOAT, simulated_pnl FLOAT)"
        )
        conn.execute(
            "INSERT INTO paper_positions "
            "(id, status, source_model, market, stock_code, opened_price, quantity, normalized_notional, simulated_pnl) "
            "VALUES (1, 'closed', 'legacy-model', 'cn', '600519', 100, 200, 20000, -75)"
        )
        conn.execute(
            "CREATE TABLE paper_trade_events ("
            "id INTEGER PRIMARY KEY, event_type TEXT, price FLOAT, allocated_capital FLOAT)"
        )
        conn.execute(
            "INSERT INTO paper_trade_events (id, event_type, price, allocated_capital) "
            "VALUES (1, 'close', 99.5, 19900)"
        )
        conn.execute(
            "CREATE TABLE paper_capital_accounts ("
            "id INTEGER PRIMARY KEY, account_key TEXT, source_model TEXT, market TEXT, currency TEXT, "
            "initial_capital FLOAT, cash FLOAT, exposure FLOAT, invested_capital FLOAT, realized_pnl FLOAT, "
            "open_position_count INTEGER, max_position_pct FLOAT, max_total_exposure_pct FLOAT, "
            "risk_per_trade_pct FLOAT, max_open_positions INTEGER, created_at DATETIME, updated_at DATETIME)"
        )
        conn.execute(
            "INSERT INTO paper_capital_accounts "
            "(id, account_key, source_model, market, currency, initial_capital, cash, exposure, invested_capital, "
            "realized_pnl, open_position_count, max_position_pct, max_total_exposure_pct, risk_per_trade_pct, "
            "max_open_positions) "
            "VALUES (1, 'legacy-model:cn:CNY', 'legacy-model', 'cn', 'CNY', 100000, 80100, 0, 0, -75, 0, 20, 80, 1, 5)"
        )
        conn.commit()

    old_database_path = os.environ.get("DATABASE_PATH")
    os.environ["DATABASE_PATH"] = str(db_path)
    Config.reset_instance()
    DatabaseManager.reset_instance()
    try:
        first = DatabaseManager.get_instance()
        DatabaseManager.reset_instance()
        second = DatabaseManager.get_instance()
        with second._engine.connect() as conn:
            position_columns = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(paper_positions)").fetchall()}
            event_columns = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(paper_trade_events)").fetchall()}
            account_columns = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(paper_capital_accounts)").fetchall()}
            position_count = conn.exec_driver_sql("SELECT COUNT(*) FROM paper_positions").scalar()
            event_count = conn.exec_driver_sql("SELECT COUNT(*) FROM paper_trade_events").scalar()
            account_count = conn.exec_driver_sql("SELECT COUNT(*) FROM paper_capital_accounts").scalar()
            quantity, remaining_cost, realized = conn.exec_driver_sql(
                "SELECT initial_quantity, remaining_cost, realized_pnl FROM paper_positions WHERE id = 1"
            ).fetchone()
            fill_price, gross, fees, net_amount = conn.exec_driver_sql(
                "SELECT fill_price, gross_amount, fees, net_amount FROM paper_trade_events WHERE id = 1"
            ).fetchone()
            fk_errors = conn.exec_driver_sql("PRAGMA foreign_key_check").fetchall()
            integrity = conn.exec_driver_sql("PRAGMA integrity_check").scalar()
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        if old_database_path is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = old_database_path

    assert first is not None
    assert position_count == 1
    assert event_count == 1
    assert account_count == 1
    assert quantity == pytest.approx(200)
    assert remaining_cost == pytest.approx(20000)
    assert realized == pytest.approx(-75)
    assert fill_price == pytest.approx(99.5)
    assert gross == pytest.approx(19900)
    assert fees == pytest.approx(0)
    assert net_amount == pytest.approx(19900)
    assert {
        "initial_quantity",
        "initial_allocated_capital",
        "remaining_cost",
        "realized_quantity",
        "available_quantity",
        "frozen_quantity",
    }.issubset(position_columns)
    assert {
        "requested_quantity",
        "executed_quantity",
        "unfilled_quantity",
        "logical_order_key",
        "instrument_assumption",
    }.issubset(event_columns)
    assert "external_cash_flow" in account_columns
    assert fk_errors == []
    assert integrity == "ok"


def _market_phase(day: date, phase: str = "intraday", hour: int = 10, minute: int = 0):
    return SimpleNamespace(
        market="cn",
        phase=phase,
        market_local_time=datetime.combine(day, datetime.min.time()).replace(hour=hour, minute=minute),
        effective_daily_bar_date=day - timedelta(days=1),
        session_date=day,
        is_market_open_now=phase in {"intraday", "closing_auction"},
    )


def _seed_open_paper_lot(db, *, opened_on: date, stock_code: str = "600519", model: str = "model-a"):
    report_id, signal_id = _source(
        db,
        created_on=opened_on - timedelta(days=3),
        report_code=f"{stock_code}.SH" if stock_code.startswith("6") else stock_code,
        signal_code=stock_code,
        model=model,
    )
    with db.session_scope() as session:
        account = session.execute(
            select(PaperCapitalAccount).where(PaperCapitalAccount.account_key == f"{model}:cn:CNY")
        ).scalar_one_or_none()
        if account is None:
            account = PaperCapitalAccount(
                account_key=f"{model}:cn:CNY",
                source_model=model,
                market="cn",
                currency="CNY",
                initial_capital=100000,
                cash=80000,
                invested_capital=20000,
                exposure=20000,
                open_position_count=1,
            )
            session.add(account)
            session.flush()
        else:
            account.cash = float(account.cash or 0.0) - 20000
            account.invested_capital = float(account.invested_capital or 0.0) + 20000
            account.exposure = float(account.exposure or 0.0) + 20000
            account.open_position_count = int(account.open_position_count or 0) + 1
        plan = PaperExecutionPlan(
            plan_key=f"lot-plan-{model}-{stock_code}-{opened_on.isoformat()}",
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
            stock_name="测试股",
            status="open",
            opened_on=opened_on,
            opened_price=100,
            normalized_notional=20000,
            quantity=200,
            initial_quantity=200,
            available_quantity=200,
            frozen_quantity=0,
            capital_account_id=account.id,
            currency="CNY",
            allocated_capital=20000,
            initial_allocated_capital=20000,
            remaining_cost=20000,
            realized_quantity=0,
            realized_proceeds=0,
            realized_fees=0,
            realized_pnl=0,
            lot_size=100,
            legacy_fixed_notional=False,
            sellability_rule="cn_t1",
            stop_loss=95,
            target_price=130,
            max_holding_sessions=10,
        )
        session.add(position)
        session.flush()
        return plan.id, position.id


def _confirmed_defensive_signal(
    db,
    service: PaperTradingService,
    *,
    action: str = "reduce",
    model: str = "model-a",
    stock_code: str = "600519",
    first_day: date = date(2026, 8, 10),
    second_day: date = date(2026, 8, 11),
    metadata: dict | None = None,
    score: int | None = None,
) -> tuple[int, int]:
    latest: tuple[int, int] | None = None
    for day in (first_day, second_day):
        kwargs = {"action": action}
        if metadata is not None:
            kwargs["metadata_json"] = json.dumps(metadata, ensure_ascii=False)
        if score is not None:
            kwargs["score"] = score
        report_id, signal_id = _source(
            db,
            created_on=day,
            model=model,
            report_code=f"{stock_code}.SH" if stock_code.startswith("6") else stock_code,
            signal_code=stock_code,
            **kwargs,
        )
        with db.session_scope() as session:
            signal = session.get(DecisionSignalRecord, signal_id)
            signal.created_at = datetime.combine(day, datetime.min.time()).replace(hour=10, minute=5)
        service.create_plan_for_report(report_id, decision_signal_id=signal_id)
        latest = (report_id, signal_id)
    assert latest is not None
    return latest


def test_strategy_defensive_to_buy_requires_distinct_sessions(isolated_db):
    service = _single_confirmation_service(
        strategy_entry_confirmation_sessions=1,
        strategy_flip_confirmation_sessions=2,
    )
    defensive_report, defensive_signal = _source(
        isolated_db,
        created_on=date(2026, 8, 10),
        action="avoid",
        entry_low=None,
        entry_high=None,
        stop_loss=None,
        target_price=None,
    )
    service.create_plan_for_report(defensive_report, decision_signal_id=defensive_signal)

    first_report, first_signal = _source(isolated_db, created_on=date(2026, 8, 11), action="buy")
    first = service.create_plan_for_report(first_report, decision_signal_id=first_signal)["item"]
    repeated_report, repeated_signal = _source(isolated_db, created_on=date(2026, 8, 11), action="buy")
    repeated = service.create_plan_for_report(repeated_report, decision_signal_id=repeated_signal)["item"]
    second_report, second_signal = _source(isolated_db, created_on=date(2026, 8, 12), action="buy")
    second = service.create_plan_for_report(second_report, decision_signal_id=second_signal)["item"]

    assert first["status"] == "non_executable"
    assert repeated["status"] == "non_executable"
    assert second["status"] == "active"
    with isolated_db.get_session() as session:
        transitions = list(session.execute(
            select(PaperStrategyTransition).order_by(PaperStrategyTransition.id)
        ).scalars().all())
        first_transition, repeated_transition, accepted_transition = transitions[-3:]
        assert first_transition.reason == "awaiting_entry_confirmation"
        assert first_transition.confirmation_count == 1
        assert repeated_transition.reason == "awaiting_entry_confirmation"
        assert repeated_transition.confirmation_count == 1
        assert accepted_transition.reason == "accepted_entry_thesis"
        assert accepted_transition.confirmation_count == 2
        thesis = session.execute(select(PaperStrategyThesis)).scalar_one()
        assert thesis.state == "entry_active"
        assert thesis.effective_action == "buy"


def test_strategy_context_exposes_only_latest_pending_transition(isolated_db):
    service = PaperTradingService(strategy_flip_confirmation_sessions=2)
    defensive_report, defensive_signal = _source(
        isolated_db,
        created_on=date(2026, 8, 10),
        action="avoid",
        entry_low=None,
        entry_high=None,
        stop_loss=None,
        target_price=None,
    )
    _create_plan_with_report_clock(service, defensive_report, decision_signal_id=defensive_signal)
    buy_report, buy_signal = _source(isolated_db, created_on=date(2026, 8, 11), action="buy")
    _create_plan_with_report_clock(service, buy_report, decision_signal_id=buy_signal)

    pending = service.analysis_context_for_stock(
        source_model="model-a",
        stock_code="600519.SH",
    )["strategy_pending_transition"]
    assert pending["reason"] == "awaiting_entry_confirmation"
    assert pending["confirmation_count"] == 1
    assert pending["evidence"]["confirmation_session_dates"] == ["2026-08-11"]

    accepted_report, accepted_signal = _source(isolated_db, created_on=date(2026, 8, 12), action="buy")
    _create_plan_with_report_clock(service, accepted_report, decision_signal_id=accepted_signal)
    resolved = service.analysis_context_for_stock(
        source_model="model-a",
        stock_code="600519.SH",
    )
    assert resolved["strategy_pending_transition"] is None


def test_strategy_stale_report_cannot_mutate_thesis_or_plan_authority(isolated_db):
    service = _single_confirmation_service()
    newer_report, newer_signal = _source(isolated_db, created_on=date(2026, 8, 12), action="buy")
    newer = service.create_plan_for_report(newer_report, decision_signal_id=newer_signal)["item"]
    older_report, older_signal = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        action="avoid",
        entry_low=None,
        entry_high=None,
        stop_loss=None,
        target_price=None,
    )
    older = service.create_plan_for_report(older_report, decision_signal_id=older_signal)["item"]

    assert newer["status"] == "active"
    assert older["status"] == "superseded"
    with isolated_db.get_session() as session:
        thesis = session.execute(select(PaperStrategyThesis)).scalar_one()
        stale = session.execute(
            select(PaperStrategyTransition)
            .where(PaperStrategyTransition.source_report_id == older_report)
        ).scalar_one()
        assert thesis.last_report_id == newer_report
        assert thesis.effective_action == "buy"
        assert stale.reason == "stale_report_older_than_strategy_observation"
        assert stale.plan_authorized is False
        assert stale.risk_revision_authorized is False


def test_unconfirmed_defensive_signal_cannot_model_exit_but_hard_stop_executes(isolated_db):
    _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="600519")
    service = _single_confirmation_service(strategy_flip_confirmation_sessions=2)
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        action="reduce",
    )
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    with isolated_db.get_session() as session:
        transition = session.execute(
            select(PaperStrategyTransition)
            .where(PaperStrategyTransition.source_report_id == report_id)
        ).scalar_one()
        assert transition.reason == "awaiting_defensive_confirmation"
        assert transition.risk_revision_authorized is False

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=94,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert result["positions_closed"] == 1
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        assert position.status == "closed"
        assert position.close_reason == "intraday_stop_loss"


def test_verified_intraday_reanalysis_can_immediately_authorize_risk_off(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(
        isolated_db,
        opened_on=date(2026, 8, 10),
        stock_code="600519",
    )
    service = _single_confirmation_service(strategy_flip_confirmation_sessions=2)
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        action="sell",
        trigger_source="paper_trading_reanalysis",
        raw_result={
            "model_used": "model-a",
            "dashboard": {},
            "paper_trading": {"same_session_reanalysis": True},
        },
    )
    with isolated_db.session_scope() as session:
        signal = session.get(DecisionSignalRecord, signal_id)
        metadata = json.loads(signal.metadata_json)
        metadata["market_phase_summary"] = {"session_date": "2026-08-11"}
        signal.metadata_json = json.dumps(metadata, ensure_ascii=False)
        signal.market_phase = "intraday"
    with patch(
        "src.services.paper_trading_service.build_market_phase_context",
        return_value=_market_phase(date(2026, 8, 11)),
    ):
        service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    with isolated_db.get_session() as session:
        transition = session.execute(
            select(PaperStrategyTransition)
            .where(PaperStrategyTransition.source_report_id == report_id)
        ).scalar_one()
        assert transition.result == "accepted"
        assert transition.reason == "accepted_verified_intraday_reanalysis"
        assert transition.risk_revision_authorized is True

    # The accepted defensive authority must reach the real execution path.  The
    # quote is deliberately between the deterministic stop and target, so the
    # only reason this lot can close is the verified model sell signal.
    with patch(
        "src.services.paper_trading_service.build_market_phase_context",
        return_value=_market_phase(date(2026, 8, 11)),
    ):
        monitored = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert monitored["positions_closed"] == 1
    assert any(event.get("reason") == "model_sell" for event in monitored["events"])
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.status == "closed"
        assert position.close_reason == "model_sell"


@pytest.mark.parametrize(
    "signal_phase,source_session_date,activation_phase,monitor_day",
    [
        (None, "2026-08-11", _market_phase(date(2026, 8, 11)), date(2026, 8, 11)),
        ("intraday", None, _market_phase(date(2026, 8, 11)), date(2026, 8, 11)),
        ("intraday", "2026-08-10", _market_phase(date(2026, 8, 11)), date(2026, 8, 11)),
        (
            "intraday",
            "2026-08-11",
            _market_phase(date(2026, 8, 11), phase="postmarket", hour=15, minute=10),
            date(2026, 8, 12),
        ),
    ],
    ids=["missing-intraday-phase", "missing-session-date", "wrong-session-date", "market-not-open"],
)
def test_self_marked_reanalysis_cannot_bypass_strict_verification(
    isolated_db,
    signal_phase,
    source_session_date,
    activation_phase,
    monitor_day,
):
    _plan_id, position_id = _seed_open_paper_lot(
        isolated_db,
        opened_on=date(2026, 8, 10),
        stock_code="600519",
    )
    service = _single_confirmation_service(strategy_flip_confirmation_sessions=2)
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        action="sell",
        trigger_source="paper_trading_reanalysis",
        raw_result={
            "model_used": "model-a",
            "dashboard": {},
            "paper_trading": {"same_session_reanalysis": True},
        },
    )
    with isolated_db.session_scope() as session:
        signal = session.get(DecisionSignalRecord, signal_id)
        metadata = json.loads(signal.metadata_json)
        if source_session_date is not None:
            metadata["market_phase_summary"] = {"session_date": source_session_date}
        signal.metadata_json = json.dumps(metadata, ensure_ascii=False)
        signal.market_phase = signal_phase

    with patch(
        "src.services.paper_trading_service.build_market_phase_context",
        return_value=activation_phase,
    ):
        service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    with isolated_db.get_session() as session:
        transition = session.execute(
            select(PaperStrategyTransition)
            .where(PaperStrategyTransition.source_report_id == report_id)
        ).scalar_one()
        assert transition.result == "observed"
        expected_reason = (
            "confirmation_session_unavailable_or_non_trading"
            if source_session_date == "2026-08-10"
            else "awaiting_defensive_confirmation"
        )
        assert transition.reason == expected_reason
        assert transition.risk_revision_authorized is False

    with patch(
        "src.services.paper_trading_service.build_market_phase_context",
        return_value=_market_phase(monitor_day),
    ):
        monitored = service.run_intraday_monitor(
            current_time=datetime(
                monitor_day.year,
                monitor_day.month,
                monitor_day.day,
                2,
                0,
                tzinfo=timezone.utc,
            ),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp=f"{monitor_day.isoformat()}T10:00:00+08:00",
                fetched_at=f"{monitor_day.isoformat()}T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert monitored["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.status == "open"
        assert position.close_reason is None


def test_legacy_version_open_position_remains_in_current_operations(isolated_db):
    plan_id, position_id = _seed_open_paper_lot(
        isolated_db,
        opened_on=date(2026, 8, 10),
    )
    with isolated_db.session_scope() as session:
        plan = session.get(PaperExecutionPlan, plan_id)
        plan.plan_version = "paper-v2"

    operations = _single_confirmation_service().list_operations()

    assert operations["total"] == 1
    assert operations["items"][0]["position_ids"] == [position_id]
    assert operations["items"][0]["stage"] == "position_monitoring"


def test_cn_a_share_same_day_exit_becomes_pending_then_closes_next_session_at_fresh_quote(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="002714")
    service = _single_confirmation_service(feishu_notify=True)

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 10))):
        pending = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=90,
                code="002714",
                provider_timestamp="2026-08-10T10:00:00+08:00",
                fetched_at="2026-08-10T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert pending["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.status == "open"
        assert position.pending_exit_reason == "intraday_stop_loss"
        assert position.available_quantity == 0
        assert position.frozen_quantity == 200
        pending_event = session.execute(select(PaperTradeEvent).where(PaperTradeEvent.event_type == "pending_exit")).scalar_one()
        assert pending_event.price == 90

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        closed = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=88,
                code="002714",
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert closed["positions_closed"] == 1
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.status == "closed"
        assert position.closed_price == 88
        assert position.close_reason == "intraday_stop_loss"
        events = session.execute(select(PaperTradeEvent).order_by(PaperTradeEvent.id)).scalars().all()
        assert [event.event_type for event in events] == ["pending_exit", "close"]
        dispatches = session.query(PaperNotificationDispatch).count()
        assert dispatches == 2


def test_explicit_t0_symbol_can_close_on_opening_day(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="600519")
    service = _single_confirmation_service(t0_symbols=["600519.SH"])

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 10))):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=131,
                provider_timestamp="2026-08-10T10:00:00+08:00",
                fetched_at="2026-08-10T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert result["positions_closed"] == 1
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.status == "closed"
        assert position.closed_price == 131
        assert position.pending_exit_reason is None


def test_model_reduce_partially_exits_and_keeps_remaining_lot_open(isolated_db):
    _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="600519")
    service = _single_confirmation_service(default_reduce_fraction=0.5)
    _confirmed_defensive_signal(isolated_db, service, action="reduce")

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert result["positions_reduced"] == 1, result
    assert result["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        assert position.status == "open"
        assert position.quantity == pytest.approx(100.0)
        assert position.remaining_cost == pytest.approx(10000.0)
        assert position.realized_quantity == pytest.approx(100.0)
        assert position.realized_pnl == pytest.approx(1000.0)
        event = session.execute(select(PaperTradeEvent).where(PaperTradeEvent.event_type == "reduce")).scalar_one()
        assert event.quantity == pytest.approx(100.0)
        assert event.fill_evidence_grade == "quote_based_unverified"


def test_defensive_signal_date_filter_uses_typed_date_bind(isolated_db):
    from sqlalchemy import Date, event
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.orm import Session

    observed = []

    def inspect_query(state):
        if state.is_select:
            observed.append(state.statement.compile(dialect=postgresql.dialect()))

    event.listen(Session, "do_orm_execute", inspect_query)
    try:
        PaperTradingRepository(isolated_db).latest_defensive_signal(
            market="cn", stock_code="600519", source_model="model-a",
            after_date=date(2026, 8, 10), through_date=date(2026, 8, 11),
        )
    finally:
        event.remove(Session, "do_orm_execute", inspect_query)
    assert any(
        isinstance(bind.type, Date) and bind.value == date(2026, 8, 11)
        for statement in observed for bind in statement.binds.values()
    ), "PostgreSQL date() comparison requires a DATE bind, not VARCHAR"


def test_repeated_aggregate_reduce_uses_remaining_quantity_and_consumes_after_success(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="600519")
    with isolated_db.session_scope() as session:
        position = session.get(PaperPosition, position_id)
        position.quantity = 600
        position.initial_quantity = 1000
        position.available_quantity = 600
        position.frozen_quantity = 0
        position.allocated_capital = 60000
        position.initial_allocated_capital = 100000
        position.remaining_cost = 60000
        position.realized_quantity = 400
        position.realized_proceeds = 48000
        position.realized_pnl = 8000
        account = session.get(PaperCapitalAccount, position.capital_account_id)
        account.cash = 40000
        account.invested_capital = 60000
        account.exposure = 60000
        account.realized_pnl = 8000

    service = _single_confirmation_service(default_reduce_fraction=0.5)
    _confirmed_defensive_signal(isolated_db, service, action="reduce")

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        first = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )
        retry = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 5, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=111,
                provider_timestamp="2026-08-11T10:05:00+08:00",
                fetched_at="2026-08-11T10:05:03+08:00",
            )),
            interval_seconds=300,
        )

    assert first["positions_reduced"] == 1, first
    assert retry["positions_reduced"] == 0, retry
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.status == "open"
        assert position.quantity == pytest.approx(300)
        assert position.realized_quantity == pytest.approx(700)
        event = session.execute(select(PaperTradeEvent).where(PaperTradeEvent.event_type == "reduce")).scalar_one()
        assert event.quantity == pytest.approx(300)
        assert event.net_realized_pnl == pytest.approx(3000)
        assert session.query(PaperSignalConsumption).count() == 1


def test_failed_aggregate_reduce_does_not_consume_signal_and_retries(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="600519")
    service = _single_confirmation_service(default_reduce_fraction=0.5)
    _confirmed_defensive_signal(isolated_db, service, action="reduce")
    blocked_quote = _quote(
        price=110,
        provider_timestamp="2026-08-11T10:00:00+08:00",
        fetched_at="2026-08-11T10:00:03+08:00",
    )
    blocked_quote.limit_state = "sell_locked"

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        failed = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(blocked_quote),
            interval_seconds=300,
        )
        retried = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 5, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=111,
                provider_timestamp="2026-08-11T10:05:00+08:00",
                fetched_at="2026-08-11T10:05:03+08:00",
            )),
            interval_seconds=300,
        )

    assert failed["failed_closed"] == 1, failed
    assert failed["positions_reduced"] == 0
    assert retried["positions_reduced"] == 1, retried
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.quantity == pytest.approx(100)
        assert session.query(PaperSignalConsumption).count() == 1
        failed_observation = session.execute(
            select(PaperPlanObservation).where(PaperPlanObservation.result == "failed_closed")
        ).scalar_one()
        assert "quote_indicates_sell_limit_blocked" in failed_observation.evidence_json


def test_failed_aggregate_reduce_rolls_back_partial_lot_mutation_and_retries(isolated_db):
    _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 9), stock_code="600519")
    _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="600519")
    service = _single_confirmation_service(default_reduce_fraction=0.75)
    _confirmed_defensive_signal(isolated_db, service, action="reduce")
    original_apply_exit = service._apply_position_exit
    call_count = 0

    def fail_second_allocation(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            return None, "forced_second_lot_failure"
        return original_apply_exit(*args, **kwargs)

    with (
        patch.object(service, "_apply_position_exit", side_effect=fail_second_allocation),
        patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))),
    ):
        failed = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert failed["failed_closed"] == 1, failed
    assert failed["positions_reduced"] == 0
    with isolated_db.get_session() as session:
        positions = session.execute(select(PaperPosition).order_by(PaperPosition.opened_on)).scalars().all()
        assert [position.quantity for position in positions] == pytest.approx([200, 200])
        account = session.execute(select(PaperCapitalAccount)).scalar_one()
        assert account.cash == pytest.approx(60000)
        assert account.invested_capital == pytest.approx(40000)
        assert account.exposure == pytest.approx(40000)
        assert session.query(PaperTradeEvent).count() == 0
        assert session.query(PaperSignalConsumption).count() == 0
        resolution = session.execute(select(PaperResolution)).scalar_one()
        assert resolution.status == "failed_closed"
        assert resolution.requested_quantity == pytest.approx(300)
        assert resolution.executed_quantity == pytest.approx(0)
        assert resolution.unfilled_quantity == pytest.approx(300)

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        retried = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 5, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=111,
                provider_timestamp="2026-08-11T10:05:00+08:00",
                fetched_at="2026-08-11T10:05:03+08:00",
            )),
            interval_seconds=300,
        )

    assert retried["positions_reduced"] == 1, retried
    with isolated_db.get_session() as session:
        positions = session.execute(select(PaperPosition).order_by(PaperPosition.opened_on)).scalars().all()
        assert [position.quantity for position in positions] == pytest.approx([0, 100])
        events = session.execute(select(PaperTradeEvent).order_by(PaperTradeEvent.id)).scalars().all()
        assert [event.quantity for event in events] == pytest.approx([200, 100])
        assert session.query(PaperSignalConsumption).count() == 1
        retry_resolution = session.execute(
            select(PaperResolution).where(PaperResolution.status == "closed")
        ).scalar_one()
        assert retry_resolution.requested_quantity == pytest.approx(300)
        assert retry_resolution.executed_quantity == pytest.approx(300)
        assert retry_resolution.unfilled_quantity == pytest.approx(0)


def test_model_defensive_signal_uses_report_time_effective_holding_action(isolated_db):
    _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="600519")
    metadata = {
        "holding_state": "holding",
        "base_action": "buy",
        "effective_action_source": "position_actions.holding",
        "position_actions": {
            "holding": {"action": "reduce", "reason": "top-level buy, holding reduce"},
        }
    }
    service = _single_confirmation_service(default_reduce_fraction=0.5)
    _confirmed_defensive_signal(
        isolated_db,
        service,
        action="reduce",
        metadata=metadata,
        score=65,
    )
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert result["positions_reduced"] == 1
    with isolated_db.get_session() as session:
        event = session.execute(select(PaperTradeEvent).where(PaperTradeEvent.event_type == "reduce")).scalar_one()
        details = json.loads(event.details_json)
        assert details["aggregate_fill"]["evidence_grade"] == "quote_based_unverified"
        observation = session.execute(select(PaperPlanObservation).where(PaperPlanObservation.result == "closed")).scalar_one()
        evidence = json.loads(observation.evidence_json)
        assert evidence["aggregate_defensive_signal"]["action"] == "reduce"
        assert evidence["aggregate_defensive_signal"]["holding_action"] == "reduce"


def test_empty_report_holding_branch_remains_audit_only_for_open_position(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(
        isolated_db,
        opened_on=date(2026, 8, 10),
        stock_code="600519",
    )
    metadata = {
        "holding_state": "empty",
        "base_action": "reduce",
        "effective_action_source": "position_actions.empty",
        "position_actions": {
            "empty": {"action": "watch", "reason": "report-time empty"},
            "holding": {"action": "reduce", "reason": "audit-only alternate branch"},
        },
    }
    _report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        model="model-a",
        action="watch",
        score=37,
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    with isolated_db.session_scope() as session:
        signal = session.get(DecisionSignalRecord, signal_id)
        signal.created_at = datetime(2026, 8, 11, 10, 5)

    service = _single_confirmation_service(default_reduce_fraction=0.5)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert result["positions_reduced"] == 0
    assert result["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.status == "open"
        assert position.quantity == pytest.approx(200.0)
        trade_events = session.execute(select(PaperTradeEvent)).scalars().all()
        assert trade_events == []


def test_model_reduce_aggregates_multi_lot_once_and_allocates_single_min_commission(isolated_db):
    for day in (date(2026, 8, 7), date(2026, 8, 8), date(2026, 8, 9)):
        _seed_open_paper_lot(isolated_db, opened_on=day, stock_code="600519")
    service = _single_confirmation_service(default_reduce_fraction=0.5, min_commission=30.0)
    _confirmed_defensive_signal(isolated_db, service, action="reduce")
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert result["positions_reduced"] == 1, result
    with isolated_db.get_session() as session:
        events = session.execute(
            select(PaperTradeEvent)
            .where(PaperTradeEvent.event_type == "reduce")
            .order_by(PaperTradeEvent.id)
        ).scalars().all()
        assert len(events) == 2
        details = [json.loads(event.details_json) for event in events]
        logical_keys = {item["logical_order_key"] for item in details}
        assert len(logical_keys) == 1
        assert sum(event.quantity for event in events) == pytest.approx(300.0)
        assert sum(event.fees for event in events) == pytest.approx(30.0)
        assert [event.quantity for event in events] == pytest.approx([200.0, 100.0])
        positions = session.execute(select(PaperPosition).order_by(PaperPosition.opened_on)).scalars().all()
        assert [position.quantity for position in positions] == pytest.approx([0.0, 100.0, 200.0])
        assert [position.status for position in positions] == ["closed", "open", "open"]
        account = session.execute(select(PaperCapitalAccount)).scalar_one()
        assert account.realized_pnl == pytest.approx(2970.0)


def test_model_reduce_rounding_to_zero_is_suppressed_not_full_close(isolated_db):
    _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 9), stock_code="600519")
    with isolated_db.session_scope() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        position.quantity = 50
        position.initial_quantity = 50
        position.available_quantity = 50
        position.remaining_cost = 5000
        position.initial_allocated_capital = 5000
        position.allocated_capital = 5000
    service = _single_confirmation_service(default_reduce_fraction=0.5)
    _confirmed_defensive_signal(isolated_db, service, action="reduce")
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert result["positions_reduced"] == 0
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        assert position.status == "open"
        assert position.quantity == pytest.approx(50)
        assert session.query(PaperTradeEvent).count() == 0
        observation = session.execute(select(PaperPlanObservation).where(PaperPlanObservation.result == "suppressed")).scalar_one()
        evidence = json.loads(observation.evidence_json)
        assert "reduce_quantity_rounded_to_zero" in evidence["reasons"]


def test_aggregate_model_signal_is_consumed_once_across_later_buckets(isolated_db):
    for day in (date(2026, 8, 7), date(2026, 8, 8)):
        _seed_open_paper_lot(isolated_db, opened_on=day, stock_code="600519")
    service = _single_confirmation_service(default_reduce_fraction=0.5)
    _confirmed_defensive_signal(isolated_db, service, action="reduce")
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        first = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )
        second = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 5, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=111,
                provider_timestamp="2026-08-11T10:05:00+08:00",
                fetched_at="2026-08-11T10:05:03+08:00",
            )),
            interval_seconds=300,
        )

    assert first["positions_reduced"] == 1
    assert second["positions_reduced"] == 0
    with isolated_db.get_session() as session:
        assert session.query(PaperSignalConsumption).count() == 1
        events = session.execute(select(PaperTradeEvent).where(PaperTradeEvent.event_type == "reduce")).scalars().all()
        assert sum(event.quantity for event in events) == pytest.approx(200.0)
        observations = session.execute(select(PaperPlanObservation).where(PaperPlanObservation.logical_order_key.is_not(None))).scalars().all()
        assert {item.resolution_key for item in observations}


def test_resolution_ledger_persists_selected_intent_and_event_evidence(isolated_db):
    _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 9), stock_code="600519")
    service = _single_confirmation_service(default_reduce_fraction=0.5)
    _confirmed_defensive_signal(isolated_db, service, action="reduce")
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert result["positions_reduced"] == 1
    with isolated_db.get_session() as session:
        resolution = session.execute(select(PaperResolution)).scalar_one()
        assert resolution.selected_intent == "model_reduce"
        assert resolution.requested_quantity == pytest.approx(100.0)
        assert resolution.executed_quantity == pytest.approx(100.0)
        assert resolution.unfilled_quantity == pytest.approx(0.0)
        selected = json.loads(resolution.selected_intent_json)
        assert selected["logical_order_key"]
        event = session.execute(select(PaperTradeEvent).where(PaperTradeEvent.event_type == "reduce")).scalar_one()
        assert event.resolution_key == resolution.resolution_key
        assert event.logical_order_key == selected["logical_order_key"]
        assert event.requested_quantity == pytest.approx(100.0)
        assert event.executed_quantity == pytest.approx(100.0)
        assert event.unfilled_quantity == pytest.approx(0.0)
        assert event.instrument_assumption == "paper_market_order_with_configured_slippage"
        assert event.quote_provider_timestamp == "2026-08-11T10:00:00+08:00"


def test_resolution_key_uses_only_policy_account_identity_mode_and_bucket(isolated_db):
    service = _single_confirmation_service()
    bucket = service._resolution_bucket(
        mode="intraday_position",
        source_model="model-a",
        market="cn",
        stock_code="600519.SH",
        currency="CNY",
        capital_account_id=7,
        bucket_started_at=datetime(2026, 8, 11, 2, 0),
    )
    salted_like_bucket = {
        **bucket,
        "plan_id": 99,
        "position_id": 88,
        "observation_key": "path-specific-observation",
        "signal_id": 77,
        "order_id": "ord-path-salt",
    }
    different_mode = {**bucket, "mode": "intraday_plan"}

    assert service._resolution_key(bucket) == service._resolution_key(salted_like_bucket)
    assert service._resolution_key(bucket) != service._resolution_key(different_mode)


def test_fill_allocation_uses_single_order_min_commission_with_cent_residual(isolated_db):
    for day in (date(2026, 8, 7), date(2026, 8, 8), date(2026, 8, 9)):
        _seed_open_paper_lot(isolated_db, opened_on=day, stock_code="600519")
    service = _single_confirmation_service(default_reduce_fraction=0.5, min_commission=30.01)
    _confirmed_defensive_signal(isolated_db, service, action="reduce")
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    with isolated_db.get_session() as session:
        events = session.execute(
            select(PaperTradeEvent)
            .where(PaperTradeEvent.event_type == "reduce")
            .order_by(PaperTradeEvent.id)
        ).scalars().all()
        assert len(events) == 2
        assert [event.fees for event in events] == pytest.approx([20.01, 10.0])
        assert sum(event.fees for event in events) == pytest.approx(30.01)
        details = [json.loads(event.details_json) for event in events]
        assert [item["allocated_fill"]["allocation_index"] for item in details] == [1, 2]
        assert details[-1]["allocated_fill"]["fees"] == pytest.approx(10.0)


def test_risk_revision_ledger_covers_applied_rejected_and_noop_cases(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 9), stock_code="600519")
    service = _single_confirmation_service()
    with isolated_db.session_scope() as session:
        position = session.get(PaperPosition, position_id)
        applied = service._record_risk_revision(
            session,
            position,
            source_report_id=position.source_report_id,
            decision_signal_id=position.decision_signal_id,
            new_stop_loss=96.0,
            new_target_price=132.0,
            evaluated_at=datetime(2026, 8, 10, 7, 0),
            current_price=100.0,
        )
        lower = service._record_risk_revision(
            session,
            position,
            source_report_id=position.source_report_id,
            decision_signal_id=position.decision_signal_id,
            new_stop_loss=94.0,
            new_target_price=132.0,
            evaluated_at=datetime(2026, 8, 10, 8, 0),
            current_price=100.0,
        )
        noop = service._record_risk_revision(
            session,
            position,
            source_report_id=position.source_report_id,
            decision_signal_id=None,
            new_stop_loss=96.0,
            new_target_price=132.0,
            evaluated_at=datetime(2026, 8, 10, 9, 0),
            current_price=100.0,
        )
        assert applied.result == "applied"
        assert lower.result == "rejected"
        assert lower.reason == "lower_stop_loss_rejected"
        assert noop.result == "noop"
        assert position.stop_loss == pytest.approx(96.0)
        assert session.query(PaperRiskRevision).count() == 3


def test_risk_revision_replay_is_idempotent_without_duplicate_rows(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 9), stock_code="600519")
    service = _single_confirmation_service()
    with isolated_db.session_scope() as session:
        position = session.get(PaperPosition, position_id)
        first = service._record_risk_revision(
            session,
            position,
            source_report_id=position.source_report_id,
            decision_signal_id=position.decision_signal_id,
            new_stop_loss=96.0,
            new_target_price=132.0,
            evaluated_at=datetime(2026, 8, 10, 7, 0),
            current_price=100.0,
            source="eligible_post_entry_signal",
        )
        replay = service._record_risk_revision(
            session,
            position,
            source_report_id=position.source_report_id,
            decision_signal_id=position.decision_signal_id,
            new_stop_loss=96.0,
            new_target_price=132.0,
            evaluated_at=datetime(2026, 8, 10, 9, 0),
            current_price=100.0,
            source="eligible_post_entry_signal",
        )
        assert replay.id == first.id
        assert replay.revision_key == first.revision_key
        assert session.query(PaperRiskRevision).count() == 1


def test_fill_fail_closed_for_halt_no_liquidity_and_limit_block(isolated_db):
    service = _single_confirmation_service()
    halted, halted_error = service._fill_assumption(
        side="sell",
        quote_price=100,
        quantity=100,
        quote=SimpleNamespace(is_halted=True),
        observed_at=datetime(2026, 8, 11, 2, 0),
    )
    no_liq, no_liq_error = service._fill_assumption(
        side="sell",
        quote_price=100,
        quantity=100,
        quote=SimpleNamespace(no_liquidity=True),
        observed_at=datetime(2026, 8, 11, 2, 0),
    )
    limit, limit_error = service._fill_assumption(
        side="sell",
        quote_price=100,
        quantity=100,
        quote=SimpleNamespace(limit_state="limit_down"),
        observed_at=datetime(2026, 8, 11, 2, 0),
    )

    assert halted is None and halted_error == "quote_indicates_halt"
    assert no_liq is None and no_liq_error == "quote_indicates_no_liquidity"
    assert limit is None and limit_error == "quote_indicates_sell_limit_blocked"


def test_fill_cost_policy_cn_stock_etf_and_zero_cost(isolated_db):
    service = _single_confirmation_service(
        commission_rate=0.0003,
        min_commission=5.0,
        sell_tax_rate=0.0005,
        slippage_pct=0.0,
    )
    stock_fill, stock_error = service._fill_assumption(
        side="sell",
        quote_price=100.0,
        quantity=100.0,
        market="cn",
        stock_code="600519",
        observed_at=datetime(2026, 8, 11, 2, 0),
    )
    etf_fill, etf_error = service._fill_assumption(
        side="sell",
        quote_price=100.0,
        quantity=100.0,
        market="cn",
        stock_code="510300",
        observed_at=datetime(2026, 8, 11, 2, 0),
    )
    zero_cost_service = _single_confirmation_service(
        commission_rate=0.0,
        min_commission=0.0,
        sell_tax_rate=0.0005,
        slippage_pct=0.0,
    )
    zero_fill, zero_error = zero_cost_service._fill_assumption(
        side="buy",
        quote_price=100.0,
        quantity=100.0,
        market="cn",
        stock_code="600519",
        observed_at=datetime(2026, 8, 11, 2, 0),
    )

    assert stock_error is None
    assert stock_fill["instrument_type"] == "cn_stock"
    assert stock_fill["tax"] == pytest.approx(5.0)
    assert stock_fill["fees"] == pytest.approx(10.0)
    assert etf_error is None
    assert etf_fill["instrument_type"] == "cn_etf_fund"
    assert etf_fill["tax"] == pytest.approx(0.0)
    assert etf_fill["fees"] == pytest.approx(5.0)
    assert zero_error is None
    assert zero_fill["fees"] == pytest.approx(0.0)
    assert zero_fill["net_amount"] == pytest.approx(10000.0)


def test_performance_aggregates_explicit_commission_tax_slippage_repo_wide(isolated_db):
    report_id, signal_id = _source(isolated_db)
    plan = _single_confirmation_service().create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    with isolated_db.session_scope() as session:
        position = PaperPosition(
            plan_id=plan["id"],
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            status="closed",
            opened_on=date(2026, 8, 10),
            opened_at=datetime(2026, 8, 10, 7, 0),
            opened_price=100.0,
            normalized_notional=10000.0,
            quantity=0.0,
            initial_quantity=100.0,
            available_quantity=0.0,
            frozen_quantity=0.0,
            currency="CNY",
            allocated_capital=0.0,
            initial_allocated_capital=10000.0,
            remaining_cost=0.0,
            realized_quantity=100.0,
            realized_proceeds=10131.0,
            realized_fees=19.0,
            realized_pnl=131.0,
            simulated_return_pct=1.31,
            lot_size=100,
            legacy_fixed_notional=False,
            sellability_rule="cn_stock_t1",
            max_holding_sessions=10,
        )
        session.add(position)
        session.flush()
        session.add_all([
            PaperTradeEvent(
                event_key="explicit-stock-cost",
                plan_id=plan["id"],
                position_id=position.id,
                source_report_id=report_id,
                decision_signal_id=signal_id,
                source_model="model-a",
                market="cn",
                stock_code="600519",
                stock_name="贵州茅台",
                event_type="close",
                event_date=date(2026, 8, 11),
                price=101.0,
                quantity=100.0,
                gross_amount=10100.0,
                commission=5.0,
                tax=5.0,
                slippage_amount=2.0,
                fees=12.0,
                net_amount=10088.0,
                gross_realized_pnl=100.0,
                net_realized_pnl=88.0,
                reason="unit",
                details_json="{}",
            ),
            PaperTradeEvent(
                event_key="explicit-etf-cost",
                plan_id=plan["id"],
                position_id=position.id,
                source_report_id=report_id,
                decision_signal_id=signal_id,
                source_model="model-a",
                market="cn",
                stock_code="510300",
                stock_name="沪深300ETF",
                event_type="reduce",
                event_date=date(2026, 8, 11),
                price=100.5,
                quantity=100.0,
                gross_amount=10050.0,
                commission=5.0,
                tax=0.0,
                slippage_amount=2.0,
                fees=7.0,
                net_amount=10043.0,
                gross_realized_pnl=50.0,
                net_realized_pnl=43.0,
                reason="unit",
                details_json="{}",
            ),
        ])

    result = _single_confirmation_service().performance("model-a")

    assert result["total_commission"] == pytest.approx(10.0)
    assert result["total_sell_tax"] == pytest.approx(5.0)
    assert result["total_slippage_amount"] == pytest.approx(4.0)
    assert result["explicit_cost_total"] == pytest.approx(19.0)
    assert result["total_fees"] == pytest.approx(19.0)
    assert result["gross_realized_pnl"] == pytest.approx(150.0)
    assert result["net_realized_pnl"] == pytest.approx(131.0)
    assert result["gross_realized_pnl"] - result["net_realized_pnl"] == pytest.approx(result["total_fees"])


def test_legacy_trade_event_totals_are_preserved_after_cost_columns_exist(isolated_db):
    report_id, signal_id = _source(isolated_db)
    plan = _single_confirmation_service().create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    with isolated_db.session_scope() as session:
        position = PaperPosition(
            plan_id=plan["id"],
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            status="closed",
            opened_on=date(2026, 8, 10),
            opened_at=datetime(2026, 8, 10, 7, 0),
            opened_price=100.0,
            normalized_notional=10000.0,
            quantity=0.0,
            initial_quantity=100.0,
            available_quantity=0.0,
            frozen_quantity=0.0,
            currency="CNY",
            allocated_capital=0.0,
            initial_allocated_capital=10000.0,
            remaining_cost=0.0,
            realized_quantity=100.0,
            realized_proceeds=10000.0,
            realized_fees=12.34,
            realized_pnl=-12.34,
            lot_size=100,
            legacy_fixed_notional=False,
            sellability_rule="cn_stock_t1",
            max_holding_sessions=10,
        )
        session.add(position)
        session.flush()
        event = PaperTradeEvent(
            event_key="legacy-cost-row",
            plan_id=plan["id"],
            position_id=position.id,
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            event_type="close",
            event_date=date(2026, 8, 11),
            price=100.0,
            quantity=100.0,
            gross_amount=10000.0,
            fees=12.34,
            net_amount=9987.66,
            reason="legacy_import",
            details_json="{}",
        )
        session.add(event)
        session.flush()
        event_id = event.id
    with isolated_db.get_session() as session:
        row = session.get(PaperTradeEvent, event_id)
        assert row.fees == pytest.approx(12.34)
        assert row.net_amount == pytest.approx(9987.66)
        assert row.commission is None
        assert row.tax is None
        assert row.slippage_amount is None
    performance = _single_confirmation_service().performance("model-a")
    assert performance["total_fees"] == pytest.approx(12.34)
    assert performance["total_commission"] == pytest.approx(0.0)
    assert performance["total_sell_tax"] == pytest.approx(0.0)
    assert performance["total_slippage_amount"] == pytest.approx(0.0)
    assert performance["gross_realized_pnl"] - performance["net_realized_pnl"] == pytest.approx(12.34)


def test_market_regime_gate_blocks_missing_stale_and_invalid_context(isolated_db):
    service = _single_confirmation_service(market_context_risk_control_enabled=True, market_context_lookback_days=1)
    now = datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc)

    with isolated_db.get_session() as session:
        missing = service._market_context_execution_risk_in_session(session, market="cn", current_time=now)
    assert missing["block_new_risk"] is True
    assert missing["allocation_multiplier"] == pytest.approx(0.0)
    assert "market_context_unavailable" in missing["reasons"]

    with isolated_db.session_scope() as session:
        session.add(DailyMarketRegime(
            regime_key="cn:2026-08-09:market-regime-v1",
            region="cn",
            session_date=date(2026, 8, 9),
            regime="normal",
            allocation_multiplier=1.0,
            entry_allowed=True,
            freshness_expires_at=datetime(2026, 8, 9, 15, 0),
            policy_version="market-regime-v1",
            provenance_json=json.dumps({"source": "unit"}, ensure_ascii=False),
        ))
    with isolated_db.get_session() as session:
        stale = service._market_context_execution_risk_in_session(session, market="cn", current_time=now)
    assert stale["block_new_risk"] is True
    assert "market_context_stale" in stale["reasons"]

    with isolated_db.session_scope() as session:
        session.add(DailyMarketRegime(
            regime_key="cn:2026-08-10:market-regime-v1",
            region="cn",
            session_date=date(2026, 8, 10),
            regime="garbled",
            allocation_multiplier=1.0,
            entry_allowed=True,
            freshness_expires_at=datetime(2026, 8, 10, 23, 59),
            policy_version="market-regime-v1",
            provenance_json=json.dumps({"source": "unit"}, ensure_ascii=False),
        ))
    with isolated_db.get_session() as session:
        invalid = service._market_context_execution_risk_in_session(session, market="cn", current_time=now)
    assert invalid["block_new_risk"] is True
    assert invalid["regime"] == "unavailable"


def test_market_regime_risk_off_cautious_and_normal_gate_open_add_sizing(isolated_db):
    service = _single_confirmation_service(market_context_risk_control_enabled=True)
    with isolated_db.session_scope() as session:
        session.add(DailyMarketRegime(
            regime_key="cn:2026-08-10:risk-off:market-regime-v1",
            region="cn",
            session_date=date(2026, 8, 10),
            regime="risk_off",
            allocation_multiplier=0.0,
            entry_allowed=False,
            freshness_expires_at=datetime(2026, 8, 10, 23, 59),
            policy_version="market-regime-v1",
            provenance_json=json.dumps({"source": "unit"}, ensure_ascii=False),
        ))
    with isolated_db.get_session() as session:
        risk_off = service._market_context_execution_risk_in_session(
            session,
            market="cn",
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
        )
    assert risk_off["block_new_risk"] is True
    assert risk_off["allocation_multiplier"] == pytest.approx(0.0)
    assert "market_context_risk_off" in risk_off["reasons"]

    with isolated_db.session_scope() as session:
        session.add(DailyMarketRegime(
            regime_key="cn:2026-08-10:cautious:market-regime-v1",
            region="cn",
            session_date=date(2026, 8, 10),
            regime="cautious",
            allocation_multiplier=0.5,
            entry_allowed=True,
            freshness_expires_at=datetime(2026, 8, 10, 23, 59),
            policy_version="market-regime-v1",
            provenance_json=json.dumps({"source": "unit"}, ensure_ascii=False),
        ))
    with isolated_db.get_session() as session:
        cautious = service._market_context_execution_risk_in_session(
            session,
            market="cn",
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
        )
    assert cautious["block_new_risk"] is False
    assert cautious["allocation_multiplier"] == pytest.approx(0.5)
    assert "market_context_cautious" in cautious["reasons"]

    status = service.market_context_risk_status(
        market="cn",
        current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
    )
    assert status["scope"] == {"market": "cn", "source": "daily_market_regimes", "model": None}
    assert status["fresh"] is True
    assert status["entry_allowed"] is True
    assert status["regime"] == "cautious"

    with isolated_db.session_scope() as session:
        session.add(DailyMarketRegime(
            regime_key="cn:2026-08-10:normal:market-regime-v1",
            region="cn",
            session_date=date(2026, 8, 10),
            regime="normal",
            allocation_multiplier=1.0,
            entry_allowed=True,
            freshness_expires_at=datetime(2026, 8, 10, 23, 59),
            policy_version="market-regime-v1",
            provenance_json=json.dumps({"source": "unit"}, ensure_ascii=False),
        ))
    with isolated_db.get_session() as session:
        normal = service._market_context_execution_risk_in_session(
            session,
            market="cn",
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
        )
    assert normal["block_new_risk"] is False
    assert normal["allocation_multiplier"] == pytest.approx(1.0)
    assert normal["reasons"] == []


def test_market_regime_missing_does_not_block_exit_path(isolated_db):
    report_id, signal_id = _source(isolated_db)
    plan = _single_confirmation_service().create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    service = _single_confirmation_service(
        market_context_risk_control_enabled=True,
        commission_rate=0.0,
        min_commission=0.0,
        slippage_pct=0.0,
    )
    with isolated_db.session_scope() as session:
        account = PaperCapitalAccount(
            account_key="exit-regime:model-a:cn:CNY",
            source_model="model-a",
            market="cn",
            currency="CNY",
            initial_capital=100000.0,
            cash=90000.0,
            invested_capital=10000.0,
            realized_pnl=0.0,
            exposure=10000.0,
            open_position_count=1,
            risk_per_trade_pct=1.0,
            max_position_pct=20.0,
            max_total_exposure_pct=80.0,
            max_open_positions=5,
        )
        session.add(account)
        session.flush()
        position = PaperPosition(
            plan_id=plan["id"],
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            status="open",
            opened_on=date(2026, 8, 9),
            opened_at=datetime(2026, 8, 9, 7, 0),
            opened_price=100.0,
            normalized_notional=10000.0,
            quantity=100.0,
            initial_quantity=100.0,
            available_quantity=100.0,
            frozen_quantity=0.0,
            capital_account_id=account.id,
            currency="CNY",
            allocated_capital=10000.0,
            initial_allocated_capital=10000.0,
            remaining_cost=10000.0,
            realized_quantity=0.0,
            realized_proceeds=0.0,
            realized_fees=0.0,
            realized_pnl=0.0,
            lot_size=100,
            legacy_fixed_notional=False,
            sellability_rule="cn_stock_t1",
            max_holding_sessions=10,
        )
        session.add(position)
        session.flush()
        fill_result, error = service._apply_position_exit(
            session,
            account,
            position,
            quote_price=101.0,
            reason="protective_stop",
            event_date=date(2026, 8, 10),
            observed_at=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
        )
    assert error is None
    assert fill_result is not None
    assert fill_result["final_close"] is True


def test_conditional_take_profit_pending_revalidates_next_session_before_selling(isolated_db):
    _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="600519")
    service = _single_confirmation_service()

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 10))):
        pending = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=131,
                provider_timestamp="2026-08-10T10:00:00+08:00",
                fetched_at="2026-08-10T10:00:03+08:00",
            )),
            interval_seconds=300,
        )
    assert pending["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        assert position.pending_exit_reason == "intraday_take_profit"
        assert position.frozen_quantity == pytest.approx(200.0)

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        held = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=120,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )
    assert held["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        assert position.status == "open"
        assert position.pending_exit_reason is None
        assert position.available_quantity == pytest.approx(200.0)
        assert position.frozen_quantity == pytest.approx(0.0)


def test_same_symbol_exposure_cap_blocks_cross_plan_add_without_banning_adds(isolated_db):
    _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="600519")
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 10), model="model-a")
    service = _single_confirmation_service(max_symbol_exposure_pct=30.0)
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=100,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )

    assert result["positions_opened"] == 0
    observations = service.list_observations()["items"]
    blocked = [item for item in observations if "symbol_exposure_cap_exceeded" in item["evidence"].get("reasons", [])]
    assert blocked
    assert blocked[0]["evidence"]["symbol_exposure"]["projected_symbol_exposure"] > blocked[0]["evidence"]["symbol_exposure"]["symbol_exposure_ceiling"]


def test_account_risk_gate_blocks_open_on_unknown_bucket_without_inventing_classification(isolated_db):
    report_id, signal_id = _source(isolated_db)
    service = _single_confirmation_service(account_unknown_cap_pct=10.0)
    plan_item = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    with service._ledger_write_session() as session:
        plan = session.get(PaperExecutionPlan, plan_item["id"])
        account = service._get_or_create_account(session, source_model=plan.source_model, market=plan.market)
        gate = service._account_risk_gate_for_open(
            session,
            account=account,
            plan=plan,
            projected_additional_cost=20000.0,
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            bucket_started_at=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            resolution_key="res-test-unknown",
        )
        assert gate["allowed"] is False
        assert "unknown_concentration_cap_exceeded" in gate["reasons"]
        assert gate["concentration"]["unknown"]["over_cap"] is True
        assert gate["concentration"]["unknown"]["reason"] == "missing_sector_or_insufficient_correlation_bars"
        assert session.query(PaperAccountRiskSnapshot).count() == 1
        event = session.query(PaperAccountRiskEvent).one()
        assert event.event_type == "entry_blocked"
        assert event.resolution_key == "res-test-unknown"


def test_auction_observation_endpoint_source_filters_preopen_only(isolated_db):
    report_id, signal_id = _source(isolated_db)
    service = _single_confirmation_service()
    plan_item = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    with service._ledger_write_session() as session:
        plan = session.get(PaperExecutionPlan, plan_item["id"])
        assert plan is not None
        session.add(PaperPlanObservation(
            observation_key="obs-preopen-auction",
            plan_id=plan.id,
            source_report_id=plan.source_report_id,
            decision_signal_id=plan.decision_signal_id,
            source_model=plan.source_model,
            market=plan.market,
            stock_code=plan.stock_code,
            stock_name=plan.stock_name,
            mode="intraday",
            observed_at=datetime(2026, 8, 10, 1, 24),
            bucket_started_at=datetime(2026, 8, 10, 1, 24),
            quote_price=None,
            quote_source=None,
            result="reanalysis_blocked",
            action="none",
            required_passed=False,
            evidence_json=service._json_dumps({
                "mode": "preopen_auction",
                "auction": {"available": True, "phase": "opening_auction"},
            }),
        ))
        session.add(PaperPlanObservation(
            observation_key="obs-ordinary-intraday",
            plan_id=plan.id,
            source_report_id=plan.source_report_id,
            decision_signal_id=plan.decision_signal_id,
            source_model=plan.source_model,
            market=plan.market,
            stock_code=plan.stock_code,
            stock_name=plan.stock_name,
            mode="intraday",
            observed_at=datetime(2026, 8, 10, 2, 0),
            bucket_started_at=datetime(2026, 8, 10, 2, 0),
            quote_price=100.0,
            quote_source="unit_quote",
            result="not_triggered",
            action="none",
            required_passed=False,
            evidence_json=service._json_dumps({"mode": "intraday"}),
        ))

    result = service.list_auction_observations(model="model-a")

    assert result["total"] == 1
    assert result["items"][0]["evidence"]["mode"] == "preopen_auction"
    assert result["items"][0]["result"] == "reanalysis_blocked"


def test_current_account_risk_locks_return_latest_snapshot_with_exits_enabled(isolated_db):
    service = _single_confirmation_service()
    with service._ledger_write_session() as session:
        account = service._create_account(
            session,
            source_model="model-a",
            market="cn",
            initial_capital=100000,
        )
        session.add(PaperAccountRiskSnapshot(
            snapshot_key="risk-snapshot-old",
            policy_version="paper-hybrid-v3",
            capital_account_id=account.id,
            source_model=account.source_model,
            market=account.market,
            currency=account.currency,
            session_date=date(2026, 8, 10),
            bucket_started_at=datetime(2026, 8, 10, 1, 20),
            observed_at=datetime(2026, 8, 10, 1, 20),
            current_equity=100000.0,
            active_state="normal",
            active_reasons_json="[]",
            permissions_json=service._json_dumps(service._risk_permissions("normal", [])),
            concentration_json="{}",
            evidence_json="{}",
        ))
        session.add(PaperAccountRiskSnapshot(
            snapshot_key="risk-snapshot-new",
            policy_version="paper-hybrid-v3",
            capital_account_id=account.id,
            source_model=account.source_model,
            market=account.market,
            currency=account.currency,
            session_date=date(2026, 8, 10),
            bucket_started_at=datetime(2026, 8, 10, 2, 0),
            observed_at=datetime(2026, 8, 10, 2, 0),
            current_equity=100000.0,
            active_state="reduce_only",
            active_reasons_json=service._json_dumps(["unknown_concentration_cap_exceeded"]),
            permissions_json=service._json_dumps(service._risk_permissions("reduce_only", ["unknown_concentration_cap_exceeded"])),
            concentration_json=service._json_dumps({"unknown": {"over_cap": True}}),
            evidence_json=service._json_dumps({"source": "unit"}),
        ))

    result = service.list_current_account_risk_locks(model="model-a")

    assert result["total"] == 1
    item = result["items"][0]
    assert item["snapshot_key"] == "risk-snapshot-new"
    assert item["active_state"] == "reduce_only"
    assert item["open_enabled"] is False
    assert item["add_enabled"] is False
    assert item["exits_enabled"] is True
    assert item["concentration"]["unknown"]["over_cap"] is True


def test_account_risk_session_start_equity_uses_prior_session_plus_today_cashflow(isolated_db):
    service = _single_confirmation_service()
    with service._ledger_write_session() as session:
        account = service._create_account(
            session,
            source_model="model-a",
            market="cn",
            initial_capital=100000,
        )
        account.cash = 95000
        account.external_cash_flow = 5000
        session.add(PaperAccountRiskSnapshot(
            snapshot_key="prior-session",
            policy_version="paper-hybrid-v3",
            capital_account_id=account.id,
            source_model=account.source_model,
            market=account.market,
            currency=account.currency,
            session_date=date(2026, 8, 7),
            bucket_started_at=datetime(2026, 8, 7, 7, 0),
            observed_at=datetime(2026, 8, 7, 7, 0),
            session_start_equity=100000,
            current_equity=90000,
            peak_equity=100000,
            valuation_complete=True,
            valuation_stale=False,
            active_state="normal",
            active_reasons_json="[]",
            permissions_json=service._json_dumps(service._risk_permissions("normal", [])),
            concentration_json="{}",
            evidence_json="{}",
        ))
        session.add(PaperCapitalAdjustment(
            idempotency_key="deposit-session",
            payload_hash="hash",
            account_id=account.id,
            source_model=account.source_model,
            market=account.market,
            currency=account.currency,
            adjustment_type="deposit",
            amount=5000,
            reason="cashflow",
            before_cash=90000,
            after_cash=95000,
            before_book_equity=90000,
            after_book_equity=95000,
            created_at=datetime(2026, 8, 10, 1, 0),
        ))
        snapshot = service._compute_account_risk_snapshot_in_session(
            session,
            account=account,
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            bucket_started_at=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
        )
        values = (snapshot.session_start_equity, snapshot.daily_pnl, snapshot.daily_loss_pct)

    assert values[0] == pytest.approx(95000)
    assert values[1] == pytest.approx(0)
    assert values[2] == pytest.approx(0)


def test_account_risk_intraday_cashflow_neutralizes_existing_session_baseline_and_peak(isolated_db):
    service = _single_confirmation_service()
    with service._ledger_write_session() as session:
        account = service._create_account(
            session,
            source_model="model-a",
            market="cn",
            initial_capital=100000,
        )
        account.cash = 110000
        account.external_cash_flow = 10000
        session.add(PaperAccountRiskSnapshot(
            snapshot_key="morning-session",
            policy_version="paper-hybrid-v3",
            capital_account_id=account.id,
            source_model=account.source_model,
            market=account.market,
            currency=account.currency,
            session_date=date(2026, 8, 10),
            bucket_started_at=datetime(2026, 8, 10, 1, 30),
            observed_at=datetime(2026, 8, 10, 1, 30),
            session_start_equity=100000,
            current_equity=100000,
            peak_equity=100000,
            valuation_complete=True,
            valuation_stale=False,
            active_state="normal",
            active_reasons_json="[]",
            permissions_json=service._json_dumps(service._risk_permissions("normal", [])),
            concentration_json="{}",
            evidence_json="{}",
        ))
        session.add(PaperCapitalAdjustment(
            idempotency_key="intraday-deposit",
            payload_hash="hash",
            account_id=account.id,
            source_model=account.source_model,
            market=account.market,
            currency=account.currency,
            adjustment_type="deposit",
            amount=10000,
            reason="intraday cashflow",
            before_cash=100000,
            after_cash=110000,
            before_book_equity=100000,
            after_book_equity=110000,
            created_at=datetime(2026, 8, 10, 2, 0),
        ))
        snapshot = service._compute_account_risk_snapshot_in_session(
            session,
            account=account,
            current_time=datetime(2026, 8, 10, 3, 0, tzinfo=timezone.utc),
            bucket_started_at=datetime(2026, 8, 10, 3, 0, tzinfo=timezone.utc),
        )
        values = (snapshot.session_start_equity, snapshot.peak_equity, snapshot.daily_pnl, snapshot.drawdown_pct)

    assert values[0] == pytest.approx(110000)
    assert values[1] == pytest.approx(110000)
    assert values[2] == pytest.approx(0)
    assert values[3] == pytest.approx(0)


def test_account_risk_withdrawal_neutralizes_hwm_and_daily_loss(isolated_db):
    service = _single_confirmation_service()
    with service._ledger_write_session() as session:
        account = service._create_account(
            session,
            source_model="model-a",
            market="cn",
            initial_capital=100000,
        )
        account.cash = 90000
        account.external_cash_flow = -10000
        session.add(PaperAccountRiskSnapshot(
            snapshot_key="prior-hwm",
            policy_version="paper-hybrid-v3",
            capital_account_id=account.id,
            source_model=account.source_model,
            market=account.market,
            currency=account.currency,
            session_date=date(2026, 8, 7),
            bucket_started_at=datetime(2026, 8, 7, 7, 0),
            observed_at=datetime(2026, 8, 7, 7, 0),
            session_start_equity=100000,
            current_equity=100000,
            peak_equity=100000,
            valuation_complete=True,
            valuation_stale=False,
            active_state="normal",
            active_reasons_json="[]",
            permissions_json=service._json_dumps(service._risk_permissions("normal", [])),
            concentration_json="{}",
            evidence_json="{}",
        ))
        session.add(PaperCapitalAdjustment(
            idempotency_key="overnight-withdrawal",
            payload_hash="hash",
            account_id=account.id,
            source_model=account.source_model,
            market=account.market,
            currency=account.currency,
            adjustment_type="withdraw",
            amount=10000,
            reason="cashflow",
            before_cash=100000,
            after_cash=90000,
            before_book_equity=100000,
            after_book_equity=90000,
            created_at=datetime(2026, 8, 9, 12, 0),
        ))
        snapshot = service._compute_account_risk_snapshot_in_session(
            session,
            account=account,
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            bucket_started_at=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
        )
        values = (snapshot.session_start_equity, snapshot.peak_equity, snapshot.daily_loss_pct, snapshot.drawdown_pct)

    assert values[0] == pytest.approx(90000)
    assert values[1] == pytest.approx(90000)
    assert values[2] == pytest.approx(0)
    assert values[3] == pytest.approx(0)


def test_losing_streak_cooldown_expires_after_one_trading_session_and_records_recovery(isolated_db):
    service = _single_confirmation_service(account_losing_streak_limit=2)
    report_id, signal_id = _source(isolated_db)
    plan_id = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]["id"]
    with service._ledger_write_session() as session:
        account = service._create_account(
            session,
            source_model="model-a",
            market="cn",
            initial_capital=100000,
        )
        position = PaperPosition(
            plan_id=plan_id,
            entry_leg_id="loss-fixture",
            execution_index=1,
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model=account.source_model,
            market=account.market,
            stock_code="600519",
            stock_name="贵州茅台",
            status="closed",
            opened_on=date(2026, 8, 6),
            opened_at=datetime(2026, 8, 6, 2, 0),
            opened_price=100,
            normalized_notional=20000,
            quantity=0,
            initial_quantity=200,
            capital_account_id=account.id,
            currency=account.currency,
            allocated_capital=0,
            initial_allocated_capital=20000,
            remaining_cost=0,
            realized_quantity=200,
            realized_pnl=-15,
            sync_origin="paper_generated",
            sync_status="paper_only",
            sellability_rule="cn_t1",
            max_holding_sessions=10,
            closed_on=date(2026, 8, 7),
            closed_price=100,
            close_reason="unit_loss_fixture",
        )
        session.add(position)
        session.flush()
        for idx, pnl in enumerate([-10.0, -5.0], start=1):
            session.add(PaperTradeEvent(
                event_key=f"loss-{idx}",
                plan_id=plan_id,
                source_report_id=report_id,
                decision_signal_id=signal_id,
                position_id=position.id,
                capital_account_id=account.id,
                source_model=account.source_model,
                market=account.market,
                stock_code="600519",
                stock_name="贵州茅台",
                event_type="close",
                event_date=date(2026, 8, 7),
                price=100,
                quantity=100,
                gross_amount=10000,
                fees=5,
                net_amount=9995,
                logical_order_key=f"order-loss-{idx}",
                reason="unit_loss_fixture",
                details_json=service._json_dumps({"realized_pnl": pnl}),
            ))
        session.flush()
        first = service._compute_account_risk_snapshot_in_session(
            session,
            account=account,
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            bucket_started_at=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
        )
        assert first.active_state == "cooldown"
        second = service._compute_account_risk_snapshot_in_session(
            session,
            account=account,
            current_time=datetime(2026, 8, 12, 2, 0, tzinfo=timezone.utc),
            bucket_started_at=datetime(2026, 8, 12, 2, 0, tzinfo=timezone.utc),
        )
        second_state = second.active_state
        events = session.execute(select(PaperAccountRiskEvent).order_by(PaperAccountRiskEvent.id)).scalars().all()
        event_types = [event.event_type for event in events]

    assert second_state == "normal"
    assert event_types == ["cooldown_start", "cooldown_recovered"]


def test_correlation_uses_overlapping_return_dates_not_tail_position():
    service = _single_confirmation_service(correlation_min_overlap_sessions=2)
    left = [
        {"date": "2026-08-01", "return": 0.01},
        {"date": "2026-08-02", "return": 0.02},
        {"date": "2026-08-03", "return": 0.03},
    ]
    right = [
        {"date": "2026-08-01", "return": 0.01},
        {"date": "2026-08-03", "return": 0.03},
        {"date": "2026-08-04", "return": -0.50},
    ]

    assert service._return_correlation(left, right) == pytest.approx(1.0)


def test_manual_hold_is_idempotent_and_conflicting_reuse_returns_conflict(isolated_db):
    service = _single_confirmation_service()
    with service._ledger_write_session() as session:
        account_row = service._create_account(
            session,
            source_model="model-a",
            market="cn",
            initial_capital=100000,
        )
        account_id = account_row.id
    first = service.set_account_manual_hold(
        account_id=account_id,
        reason="operator pause",
        idempotency_key="hold-key-1",
    )
    replay = service.set_account_manual_hold(
        account_id=account_id,
        reason="operator pause",
        idempotency_key="hold-key-1",
    )
    assert first["created"] is True
    assert replay["created"] is False
    assert replay["item"]["event_key"] == first["item"]["event_key"]
    with pytest.raises(PaperTradingConflictError):
        service.set_account_manual_hold(
            account_id=account_id,
            reason="different reason",
            idempotency_key="hold-key-1",
        )


def test_risk_event_feishu_dispatch_failure_does_not_change_risk_state(isolated_db):
    service = _single_confirmation_service(feishu_notify=True)
    with service._ledger_write_session() as session:
        account_row = service._create_account(
            session,
            source_model="model-a",
            market="cn",
            initial_capital=100000,
        )
        snapshot = service._compute_account_risk_snapshot_in_session(
            session,
            account=account_row,
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            bucket_started_at=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
        )
        event, created = service._record_account_risk_event_in_session(
            session,
            account=account_row,
            snapshot=snapshot,
            event_type="manual_hold",
            old_state=snapshot.active_state,
            new_state="manual_hold",
            active_reasons=["manual_hold"],
            metric="manual_hold",
            threshold=None,
            source="api",
            recovery_condition={"manual_resume_required": True},
            evidence={"reason": "operator pause"},
            idempotency_key="risk-notify-1",
        )
        event_id = event.id
        assert created is True

    class FailingSender:
        def send_to_feishu(self, content):
            assert "模拟交易风控提醒" in content
            return False

    stats = service.dispatch_pending_risk_notifications(sender=FailingSender())
    assert stats == {"sent": 0, "failed": 1, "skipped": 0}
    with isolated_db.get_session() as session:
        row = session.get(PaperAccountRiskEvent, event_id)
        assert row.new_state == "manual_hold"
        assert row.notification_status == "failed"
        assert row.notification_error == "feishu_send_failed"


def test_concurrent_intraday_open_respects_symbol_cap_and_unique_resolution(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), model="model-a")
    _single_confirmation_service(max_symbol_exposure_pct=20.0).create_plan_for_report(report_id, decision_signal_id=signal_id)
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))

    def worker():
        service = _single_confirmation_service(max_symbol_exposure_pct=20.0)
        return service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote()),
        )

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_phase()):
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _idx: worker(), range(2)))

    assert sum(result["positions_opened"] for result in results) == 1
    with isolated_db.get_session() as session:
        account = session.execute(select(PaperCapitalAccount)).scalar_one()
        ceiling = (float(account.cash or 0.0) + float(account.invested_capital or 0.0)) * 0.20
        assert float(account.exposure or 0.0) <= ceiling + 1e-9
        assert session.query(PaperPosition).count() == 1
        assert session.query(PaperResolution).count() == 1


def test_holdings_aggregate_multiple_lots_and_fresh_mark(isolated_db):
    _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="600519")
    _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 11), stock_code="600519")
    with isolated_db.session_scope() as session:
        first = session.execute(select(PaperPosition).order_by(PaperPosition.id).limit(1)).scalar_one()
        first.pending_exit_reason = "intraday_stop_loss"
        first.available_quantity = 0
        first.frozen_quantity = first.quantity
        plan = session.get(PaperExecutionPlan, first.plan_id)
        session.add(PaperPlanObservation(
            observation_key="holding-mark",
            plan_id=plan.id,
            position_id=first.id,
            source_report_id=plan.source_report_id,
            decision_signal_id=plan.decision_signal_id,
            source_model=plan.source_model,
            market=plan.market,
            stock_code=plan.stock_code,
            stock_name=plan.stock_name,
            mode="intraday",
            observed_at=utc_naive_now(),
            bucket_started_at=utc_naive_now(),
            quote_price=110,
            result="position_held",
            action="hold",
            required_passed=True,
            factors_passed=0,
            factors_available=0,
            evidence_json="{}",
        ))

    result = _single_confirmation_service(intraday_max_quote_stale_seconds=600).list_holdings(
        model="model-a",
        current_time=datetime(2026, 8, 25, 2, 0, tzinfo=timezone.utc),
    )

    assert result["total"] == 1
    item = result["items"][0]
    assert item["total_quantity"] == 400
    assert item["available_quantity"] == 200
    assert item["frozen_quantity"] == 200
    assert item["lot_count"] == 2
    assert item["total_cost"] == 40000
    assert item["weighted_average_price"] == 100
    assert item["latest_price"] == 110
    assert item["market_value"] == 44000
    assert item["mark_source"] == "paper_plan_observation"
    assert item["mark_stale"] is False
    assert item["valuation_complete"] is True
    assert item["symbol_concentration_pct"] == pytest.approx(40000 / 100000 * 100.0)
    assert item["unrealized_pnl"] == 4000


def test_no_event_intraday_quote_persists_fresh_valuation_mark_once_per_bucket(isolated_db):
    _seed_open_paper_lot(
        isolated_db,
        opened_on=date(2026, 8, 10),
        stock_code="002714",
    )
    service = _single_confirmation_service(intraday_max_quote_stale_seconds=360)
    first_quote = _quote(
        price=110.0,
        code="002714",
        provider_timestamp="2026-08-12T10:00:00+08:00",
        fetched_at="2026-08-12T10:00:03+08:00",
    )
    with patch(
        "src.services.paper_trading_service.build_market_phase_context",
        return_value=_market_phase(date(2026, 8, 12)),
    ):
        first = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 12, 2, 0, 5, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(first_quote),
            interval_seconds=300,
        )
        replay = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 12, 2, 0, 30, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(first_quote),
            interval_seconds=300,
        )
        second = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 12, 2, 5, 5, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=111.0,
                code="002714",
                provider_timestamp="2026-08-12T10:05:00+08:00",
                fetched_at="2026-08-12T10:05:03+08:00",
            )),
            interval_seconds=300,
        )

    assert first["positions_closed"] == 0
    assert first["positions_opened"] == 0
    assert first["valuation_marks_recorded"] == 1
    assert replay["valuation_marks_recorded"] == 0
    assert second["valuation_marks_recorded"] == 1
    holding = service.list_holdings(
        model="model-a",
        current_time=datetime(2026, 8, 12, 2, 5, 10, tzinfo=timezone.utc),
    )["items"][0]
    assert holding["latest_price"] == 111.0
    assert holding["mark_source"] == "unit_quote"
    assert holding["mark_stale"] is False
    assert holding["market_value"] == 22200.0
    with isolated_db.get_session() as session:
        marks = list(session.execute(
            select(PaperPlanObservation)
            .where(PaperPlanObservation.result == "valuation_mark")
            .order_by(PaperPlanObservation.bucket_started_at)
        ).scalars().all())
    assert len(marks) == 2
    assert marks[-1].action == "none"
    assert marks[-1].position_id is None
    evidence = json.loads(marks[-1].evidence_json)
    assert evidence["policy"]["not_an_execution_fill"] is True
    assert evidence["position_ids"]


def test_stale_or_wrong_identity_quote_never_creates_valuation_mark_or_exit(isolated_db):
    _seed_open_paper_lot(
        isolated_db,
        opened_on=date(2026, 8, 10),
        stock_code="002714",
    )
    service = _single_confirmation_service(intraday_max_quote_stale_seconds=360)
    with patch(
        "src.services.paper_trading_service.build_market_phase_context",
        return_value=_market_phase(date(2026, 8, 12)),
    ):
        stale = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 12, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=90.0,
                code="002714",
                stale=True,
                provider_timestamp="2026-08-12T09:00:00+08:00",
                fetched_at="2026-08-12T09:00:03+08:00",
            )),
            interval_seconds=300,
        )
        wrong_identity = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 12, 2, 5, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=90.0,
                code="600519",
                provider_timestamp="2026-08-12T10:05:00+08:00",
                fetched_at="2026-08-12T10:05:03+08:00",
            )),
            interval_seconds=300,
        )

    assert stale["valuation_marks_recorded"] == 0
    assert wrong_identity["valuation_marks_recorded"] == 0
    assert stale["positions_closed"] == 0
    assert wrong_identity["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        mark_count = len(list(session.execute(
            select(PaperPlanObservation.id).where(
                PaperPlanObservation.result == "valuation_mark"
            )
        ).scalars().all()))
    assert position.status == "open"
    assert mark_count == 0


def test_account_mark_to_market_keeps_realized_unrealized_and_book_equity_separate(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), model="model-a")
    with isolated_db.session_scope() as session:
        account = PaperCapitalAccount(
            account_key="model-a:cn:CNY",
            source_model="model-a",
            market="cn",
            currency="CNY",
            initial_capital=100000,
            cash=40930,
            invested_capital=58995,
            realized_pnl=-75,
            exposure=58995,
            open_position_count=3,
        )
        session.add(account)
        session.flush()
        plans = []
        for index in range(3):
            plan = PaperExecutionPlan(
                plan_key=f"valuation-plan-{index}",
                plan_version="paper-hybrid-v3",
                source_report_id=report_id,
                decision_signal_id=signal_id,
                source_model="model-a",
                market="cn",
                stock_code="002714",
                stock_name="牧原股份",
                report_date=date(2026, 8, 7),
                report_created_at=datetime(2026, 8, 7),
                valid_for_date=date(2026, 8, 10),
                action="buy",
                executable=True,
                status="triggered",
                rules_json="[]",
                descriptive_conditions_json="[]",
                stop_loss=35,
                target_price=42,
                max_holding_sessions=10,
            )
            session.add(plan)
            session.flush()
            plans.append(plan)
            session.add(PaperPosition(
                plan_id=plan.id,
                source_report_id=report_id,
                decision_signal_id=signal_id,
                source_model="model-a",
                market="cn",
                stock_code="002714",
                stock_name="牧原股份",
                status="open",
                opened_on=date(2026, 8, 10 + index),
                opened_price=39.33,
                normalized_notional=19665,
                quantity=500,
                available_quantity=500,
                frozen_quantity=0,
                capital_account_id=account.id,
                currency="CNY",
                allocated_capital=19665,
                lot_size=100,
                legacy_fixed_notional=False,
                sellability_rule="cn_t1",
                stop_loss=35,
                target_price=42,
                max_holding_sessions=10,
            ))
        closed_plan = PaperExecutionPlan(
            plan_key="valuation-closed-plan",
            plan_version="paper-hybrid-v3",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="002714",
            stock_name="牧原股份",
            report_date=date(2026, 8, 6),
            report_created_at=datetime(2026, 8, 6),
            valid_for_date=date(2026, 8, 7),
            action="buy",
            executable=True,
            status="closed",
            rules_json="[]",
            descriptive_conditions_json="[]",
            stop_loss=35,
            target_price=42,
            max_holding_sessions=10,
        )
        session.add(closed_plan)
        session.flush()
        session.add(PaperPosition(
            plan_id=closed_plan.id,
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="002714",
            stock_name="牧原股份",
            status="closed",
            opened_on=date(2026, 8, 6),
            opened_price=39.43,
            closed_on=date(2026, 8, 7),
            closed_price=39.28,
            close_reason="stop_loss",
            normalized_notional=19715,
            quantity=500,
            capital_account_id=account.id,
            currency="CNY",
            allocated_capital=19715,
            max_holding_sessions=10,
            simulated_pnl=-75,
            simulated_return_pct=-0.3804,
        ))
        observed_at = utc_naive_now()
        session.add(PaperPlanObservation(
            observation_key="valuation-muyuan-mark",
            plan_id=plans[0].id,
            position_id=None,
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="002714",
            stock_name="牧原股份",
            mode="intraday",
            observed_at=observed_at,
            bucket_started_at=observed_at,
            quote_price=39.28,
            quote_source="unit_quote",
            result="position_held",
            action="hold",
            required_passed=True,
            factors_passed=0,
            factors_available=0,
            evidence_json="{}",
        ))

    service = _single_confirmation_service(intraday_max_quote_stale_seconds=360)
    account_item = service.list_accounts(model="model-a")["items"][0]
    assert account_item["realized_pnl"] == -75
    assert account_item["market_value"] == pytest.approx(58920)
    assert account_item["unrealized_pnl"] == pytest.approx(-75)
    assert account_item["total_pnl"] == pytest.approx(-150)
    assert account_item["book_equity"] == pytest.approx(99925)
    assert account_item["market_equity"] == pytest.approx(99850)
    assert account_item["valuation_complete"] is True
    assert account_item["unpriced_position_count"] == 0

    holding = service.list_holdings(
        model="model-a",
        current_time=datetime(2026, 8, 25, 2, 0, tzinfo=timezone.utc),
    )["items"][0]
    assert holding["total_quantity"] == 1500
    assert holding["market_value"] == pytest.approx(58920)
    assert holding["unrealized_pnl"] == pytest.approx(-75)
    assert holding["mark_source"] == "unit_quote"
    assert holding["valuation_complete"] is True
    assert holding["symbol_concentration_pct"] == pytest.approx(58995 / 99925 * 100.0)

    context = service.analysis_context_for_stock(
        source_model="model-a",
        stock_code="002714.SZ",
        current_time=datetime(2026, 8, 25, 2, 0, tzinfo=timezone.utc),
    )
    assert context["quantity"] == 1500
    assert context["total_quantity"] == 1500
    assert context["lot_count"] == 3
    assert context["avg_cost"] == pytest.approx(39.33)
    assert context["total_cost"] == pytest.approx(58995)
    assert context["available_quantity"] == 1500
    assert context["frozen_quantity"] == 0
    assert context["unrealized_pnl"] == pytest.approx(-75)
    assert context["book_equity"] == pytest.approx(99925)
    assert context["symbol_concentration_pct"] == pytest.approx(58995 / 99925 * 100.0)
    assert context["total_exposure_pct"] == pytest.approx(58995 / 99925 * 100.0)
    assert len(context["stop_loss_lots"]) == 3


def test_unpriced_open_holding_keeps_account_market_totals_null(isolated_db):
    _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="600519")
    service = _single_confirmation_service()

    account = service.list_accounts(model="model-a")["items"][0]
    assert account["market_value"] is None
    assert account["unrealized_pnl"] is None
    assert account["total_pnl"] is None
    assert account["market_equity"] is None
    assert account["valuation_complete"] is False
    assert account["unpriced_position_count"] == 1
    assert account["priced_market_value"] is None

    holding = service.list_holdings(model="model-a")["items"][0]
    assert holding["market_value"] is None
    assert holding["unrealized_pnl"] is None
    assert holding["valuation_complete"] is False
    assert holding["mark_source"] is None


def test_stale_daily_mark_values_display_but_does_not_satisfy_intraday_execution_quote(isolated_db):
    _plan_id, _position_id = _seed_open_paper_lot(isolated_db, opened_on=date(2026, 8, 10), stock_code="002714")
    with isolated_db.session_scope() as session:
        session.add(StockDaily(
            code="002714.SZ",
            date=date(2026, 8, 11),
            open=39.28,
            high=39.28,
            low=39.28,
            close=39.28,
            volume=1000,
            data_source="stale-display-close",
        ))

    service = _single_confirmation_service(intraday_max_quote_stale_seconds=360)
    holding = service.list_holdings(
        model="model-a",
        current_time=datetime(2026, 8, 12, 2, 0, tzinfo=timezone.utc),
    )["items"][0]
    assert holding["latest_price"] == 39.28
    assert holding["mark_source"] == "stock_daily_close"
    assert holding["mark_stale"] is True
    assert holding["valuation_complete"] is True

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 12))):
        intraday = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 12, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=34.0,
                stale=True,
                provider_timestamp="2026-08-12T09:00:00+08:00",
                fetched_at="2026-08-12T09:00:03+08:00",
            )),
            interval_seconds=300,
        )
    assert intraday["positions_closed"] == 0
    assert intraday["failed_closed"] == 1


def test_valuation_chooses_newer_complete_daily_close_over_older_observation(isolated_db):
    plan_id, position_id = _seed_open_paper_lot(
        isolated_db,
        opened_on=date(2026, 8, 10),
        stock_code="002714",
    )
    with isolated_db.session_scope() as session:
        plan = session.get(PaperExecutionPlan, plan_id)
        session.add(PaperPlanObservation(
            observation_key="old-observation-before-daily-close",
            plan_id=plan.id,
            position_id=position_id,
            source_report_id=plan.source_report_id,
            decision_signal_id=plan.decision_signal_id,
            source_model=plan.source_model,
            market=plan.market,
            stock_code=plan.stock_code,
            stock_name=plan.stock_name,
            mode="intraday",
            observed_at=datetime(2026, 8, 11, 1, 0),
            bucket_started_at=datetime(2026, 8, 11, 1, 0),
            quote_price=41.0,
            quote_source="old_intraday_quote",
            result="position_held",
            action="hold",
            required_passed=True,
            factors_passed=0,
            factors_available=0,
            evidence_json="{}",
        ))
        session.add(StockDaily(
            code="002714.SZ",
            date=date(2026, 8, 11),
            open=39.28,
            high=39.28,
            low=39.28,
            close=39.28,
            volume=1000,
            data_source="newer-complete-close",
        ))

    item = _single_confirmation_service().list_holdings(
        model="model-a",
        current_time=datetime(2026, 8, 12, 2, 0, tzinfo=timezone.utc),
    )["items"][0]

    assert item["latest_price"] == 39.28
    assert item["mark_source"] == "stock_daily_close"
    assert item["mark_stale"] is True


def test_cn_t1_holding_aggregate_freezes_opening_day_then_releases_next_session(isolated_db):
    _plan_id, _position_id = _seed_open_paper_lot(
        isolated_db,
        opened_on=date(2026, 8, 10),
        stock_code="002714",
    )
    service = _single_confirmation_service()

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 10))):
        opening_day = service.list_holdings(
            model="model-a",
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
        )
    item = opening_day["items"][0]
    assert item["total_quantity"] == 200
    assert item["available_quantity"] == 0
    assert item["frozen_quantity"] == 200

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        next_session = service.list_holdings(
            model="model-a",
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
        )
    item = next_session["items"][0]
    assert item["available_quantity"] == 200
    assert item["frozen_quantity"] == 0


def test_non_cn_t0_rule_allows_same_day_close_without_weakening_cn_t1(isolated_db):
    service = _single_confirmation_service()
    us_position = SimpleNamespace(market="us", stock_code="AAPL", opened_on=date(2026, 8, 10))
    cn_position = SimpleNamespace(market="cn", stock_code="600519", opened_on=date(2026, 8, 10))
    explicit_t0_service = _single_confirmation_service(t0_symbols=["600519"])

    assert service._sellability_rule_for("us", "AAPL") == "non_cn_t0"
    assert service._can_close_position_on_date(us_position, date(2026, 8, 10)) is True
    assert service._sellability_rule_for("cn", "600519") == "cn_t1"
    assert service._can_close_position_on_date(cn_position, date(2026, 8, 10)) is False
    assert explicit_t0_service._sellability_rule_for("cn", "600519") == "explicit_t0"
    assert explicit_t0_service._can_close_position_on_date(cn_position, date(2026, 8, 10)) is True


def test_latest_next_session_plan_supersedes_older_without_touching_open_lot(isolated_db):
    first_report, first_signal = _source(isolated_db, created_on=date(2026, 9, 4), model="model-a")
    second_report, second_signal = _source(isolated_db, created_on=date(2026, 9, 7), model="model-a")
    service = _single_confirmation_service()
    first = service.create_plan_for_report(first_report, decision_signal_id=first_signal)["item"]
    second = service.create_plan_for_report(second_report, decision_signal_id=second_signal)["item"]

    with isolated_db.get_session() as session:
        first_plan = session.get(PaperExecutionPlan, first["id"])
        second_plan = session.get(PaperExecutionPlan, second["id"])
        assert first_plan.status == "superseded"
        assert second_plan.status == "active"
        assert second_plan.valid_for_date is not None


def test_preopen_phase_skips_without_quote_fetch_or_observation(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), model="model-a")
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    class NoQuoteManager:
        def get_realtime_quote(self, *_args, **_kwargs):
            raise AssertionError("preopen should not fetch realtime quote")

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 10), phase="premarket", hour=9, minute=15)):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 1, 0, tzinfo=timezone.utc),
            fetcher_manager=NoQuoteManager(),
            interval_seconds=300,
        )

    assert result["positions_opened"] == 0
    assert result["failed_closed"] == 0
    assert result["observations"] == 0
    assert result["target_identity_count"] == 1
    assert result["quote_fetch_attempts"] == 0
    assert result["quote_fetch_successes"] == 0
    assert result["skipped_closed_market"] == 1
    with isolated_db.get_session() as session:
        assert session.execute(select(PaperPlanObservation)).scalar_one_or_none() is None


def test_intraday_monitor_empty_targets_reports_zero_without_observation(isolated_db):
    service = _single_confirmation_service()

    class NoQuoteManager:
        def get_realtime_quote(self, *_args, **_kwargs):
            raise AssertionError("empty monitor target set should not fetch quote")

    result = service.run_intraday_monitor(
        current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
        fetcher_manager=NoQuoteManager(),
        interval_seconds=300,
    )

    assert result["target_identity_count"] == 0
    assert result["plans_evaluated"] == 0
    assert result["positions_evaluated"] == 0
    assert result["quote_fetch_attempts"] == 0
    assert result["quote_fetch_successes"] == 0
    assert result["observations"] == 0
    with isolated_db.get_session() as session:
        assert session.execute(select(PaperPlanObservation)).scalar_one_or_none() is None


def test_preopen_and_closed_boundaries_never_fetch_or_persist(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), model="model-a")
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    class NoQuoteManager:
        def get_realtime_quote(self, *_args, **_kwargs):
            raise AssertionError("closed/preopen boundary should not fetch quote")

    cases = [
        (_market_phase(date(2026, 8, 10), phase="premarket", hour=8, minute=59), 0),
        (_market_phase(date(2026, 8, 10), phase="premarket", hour=9, minute=0), 0),
        (_market_phase(date(2026, 8, 10), phase="opening_auction", hour=9, minute=15), 0),
        (_market_phase(date(2026, 8, 10), phase="opening_auction", hour=9, minute=20), 0),
        (_market_phase(date(2026, 8, 10), phase="opening_auction", hour=9, minute=25), 0),
        (_market_phase(date(2026, 8, 10), phase="premarket", hour=9, minute=29), 0),
        (_market_phase(date(2026, 8, 10), phase="premarket", hour=9, minute=30), 0),
        (_market_phase(date(2026, 8, 10), phase="lunch_break", hour=12, minute=0), 0),
        (_market_phase(date(2026, 8, 10), phase="closing_auction", hour=14, minute=57), 0),
        (_market_phase(date(2026, 8, 10), phase="postmarket", hour=15, minute=10), 0),
        (_market_phase(date(2026, 8, 9), phase="closed", hour=9, minute=15), 0),
    ]
    for index, (phase, expected_observations) in enumerate(cases, start=1):
        with patch("src.services.paper_trading_service.build_market_phase_context", return_value=phase):
            result = service.run_intraday_monitor(
                current_time=datetime(2026, 8, 10, 1, index, tzinfo=timezone.utc),
                fetcher_manager=NoQuoteManager(),
                interval_seconds=60,
            )
        assert result["observations"] == expected_observations
        assert result["quote_fetch_attempts"] == 0
        assert result["quote_fetch_successes"] == 0
        assert result["positions_opened"] == 0


def test_opening_material_deviation_blocks_entry_and_requests_reanalysis(isolated_db):
    report_id, signal_id = _source(isolated_db, created_on=date(2026, 8, 7), model="model-a")
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 31), days=6))
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    queue = _CountingTaskQueue()
    opening_quote = _quote(
        price=105,
        provider_timestamp="2026-08-10T09:31:00+08:00",
        fetched_at="2026-08-10T09:31:03+08:00",
    )
    opening_quote.pre_close = 100.0
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 10), hour=9, minute=31)), patch(
        "src.services.task_queue.get_task_queue", return_value=queue,
    ):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 1, 31, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(opening_quote),
            interval_seconds=60,
        )

    assert result["positions_opened"] == 0
    assert result["failed_closed"] == 0
    assert len(queue.submissions) == 1
    submitted = queue.submissions[0]["kwargs"]
    assert submitted["model"] == "model-a"
    assert submitted["portfolio_context"]["source"] == "paper_trading"
    assert submitted["portfolio_context"]["source_model"] == "model-a"
    assert submitted["portfolio_context"]["holding_state"] == "empty"
    assert submitted["portfolio_context"]["quantity"] == pytest.approx(0)
    assert submitted["portfolio_context"]["disable_cross_model_fallback"] is True
    assert submitted["portfolio_context"]["paper_trading"]["source_plan_id"]
    with isolated_db.get_session() as session:
        observation = session.execute(select(PaperPlanObservation)).scalar_one()
        request = session.execute(select(PaperReanalysisRequest)).scalar_one()
        assert observation.result == "reanalysis_blocked"
        assert "opening_confirmation_material_deviation" in observation.evidence_json
        assert request.status == "submitted"
        assert request.trigger_type == "opening_confirmation_deviation"
        assert session.execute(select(PaperPosition)).scalar_one_or_none() is None


def test_same_session_reanalysis_carries_source_plan_trading_style(isolated_db):
    # 机会池短线计划触发的同会话重分析沿用计划冻结的风格，不被账户的中线风格改写。
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 7),
        model="model-a",
        metadata_json=json.dumps({
            "holding_state": "empty",
            "base_action": "buy",
            "effective_action_source": "position_actions.empty",
            "position_actions": {
                "empty": {"action": "buy", "reason": "unit-test empty action snapshot"},
                "holding": {"action": "buy", "reason": "unit-test holding action snapshot"},
            },
            **_frozen_style_metadata("short_swing"),
        }, ensure_ascii=False),
    )
    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 31), days=6))
    service = _single_confirmation_service()
    service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    queue = _CountingTaskQueue()
    opening_quote = _quote(
        price=105,
        provider_timestamp="2026-08-10T09:31:00+08:00",
        fetched_at="2026-08-10T09:31:03+08:00",
    )
    opening_quote.pre_close = 100.0
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 10), hour=9, minute=31)), patch(
        "src.services.task_queue.get_task_queue", return_value=queue,
    ):
        service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 1, 31, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(opening_quote),
            interval_seconds=60,
        )

    with isolated_db.get_session() as session:
        assert session.execute(select(PaperExecutionPlan)).scalar_one().trading_style == "short_swing"
    assert len(queue.submissions) == 1
    paper_control = queue.submissions[0]["kwargs"]["portfolio_context"]["paper_trading"]
    assert paper_control["same_session_reanalysis"] is True
    assert paper_control["trading_style"] == "short_swing"


def test_sync_portfolio_position_rejects_missing_model_report_without_partial_mutation(isolated_db):
    portfolio = PortfolioService()
    account = portfolio.create_account(name="cn", broker="manual", market="cn", base_currency="CNY")
    portfolio.record_trade(
        account_id=account["id"],
        symbol="SH600519",
        trade_date=date(2026, 8, 2),
        side="buy",
        quantity=200,
        price=100,
        market="cn",
        currency="CNY",
    )
    portfolio.get_portfolio_snapshot(account_id=account["id"], as_of=date(2026, 8, 3), include_realtime=False)
    position_id = _portfolio_position_id_for(isolated_db, account_id=account["id"])
    service = _single_confirmation_service(initial_capital=50000)

    with patch("src.services.paper_trading_service.validate_analysis_models", return_value=["model-a"]):
        with pytest.raises(Exception, match="missing model report"):
            service.sync_portfolio_position_to_paper(
                portfolio_position_id=position_id,
                source_models=["model-a"],
                trade_date=date(2026, 8, 3),
                idempotency_key="missing-report",
            )

    with isolated_db.get_session() as session:
        assert session.query(PaperPosition).count() == 0
        assert session.query(PaperSyncLink).count() == 0
        assert session.query(PaperTradeEvent).count() == 0


def test_sync_paper_position_to_portfolio_records_confirmed_trade_once(isolated_db):
    report_id, signal_id = _source(isolated_db, report_code="600519.SH", signal_code="600519", model="model-a")
    portfolio = PortfolioService()
    account = portfolio.create_account(name="cn", broker="manual", market="cn", base_currency="CNY")
    with isolated_db.session_scope() as session:
        plan = PaperExecutionPlan(
            plan_key="paper-sync-plan",
            plan_version="paper-hybrid-v3",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            report_date=date(2026, 8, 3),
            report_created_at=datetime(2026, 8, 3),
            action="buy",
            executable=True,
            status="triggered",
            rules_json="[]",
            descriptive_conditions_json="[]",
            max_holding_sessions=10,
        )
        session.add(plan)
        session.flush()
        paper_position = PaperPosition(
            plan_id=plan.id,
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            status="open",
            opened_on=date(2026, 8, 4),
            opened_price=101,
            normalized_notional=10100,
            quantity=100,
            currency="CNY",
            allocated_capital=10100,
            stop_loss=95,
            target_price=130,
            max_holding_sessions=10,
        )
        session.add(paper_position)
        session.flush()
        paper_position_id = paper_position.id

    service = _single_confirmation_service()
    result = service.sync_paper_position_to_portfolio(
        paper_position_id=paper_position_id,
        portfolio_account_id=account["id"],
        side="buy",
        quantity=100,
        price=101,
        trade_date=date(2026, 8, 4),
        fee=1.5,
        idempotency_key="paper-confirm-1",
    )
    retry = service.sync_paper_position_to_portfolio(
        paper_position_id=paper_position_id,
        portfolio_account_id=account["id"],
        side="buy",
        quantity=100,
        price=101,
        trade_date=date(2026, 8, 4),
        fee=1.5,
        idempotency_key="paper-confirm-1",
    )

    assert result["created"] is True
    assert retry["created"] is False
    with isolated_db.get_session() as session:
        trade = session.execute(select(PortfolioTrade)).scalar_one()
        assert trade.symbol == "SH600519"
        assert trade.side == "buy"
        assert trade.trade_uid == "paper-sync:paper-confirm-1"
        position = session.get(PaperPosition, paper_position_id)
        assert position.sync_status == "synced_to_portfolio"
        assert session.query(PaperSyncLink).count() == 1


def test_sync_paper_position_to_portfolio_invalidates_portfolio_snapshot_cache(isolated_db):
    report_id, signal_id = _source(isolated_db, report_code="600519.SH", signal_code="600519", model="model-a")
    portfolio = PortfolioService()
    account = portfolio.create_account(name="cn", broker="manual", market="cn", base_currency="CNY")
    portfolio.get_portfolio_snapshot(
        account_id=account["id"],
        as_of=date(2026, 8, 5),
        include_realtime=False,
    )
    with isolated_db.get_session() as session:
        assert session.execute(
            select(PortfolioDailySnapshot).where(
                PortfolioDailySnapshot.account_id == account["id"],
                PortfolioDailySnapshot.snapshot_date == date(2026, 8, 5),
            )
        ).scalar_one_or_none() is not None

    with isolated_db.session_scope() as session:
        plan = PaperExecutionPlan(
            plan_key="paper-sync-cache-plan",
            plan_version="paper-hybrid-v3",
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            report_date=date(2026, 8, 3),
            report_created_at=datetime(2026, 8, 3),
            action="buy",
            executable=True,
            status="triggered",
            rules_json="[]",
            descriptive_conditions_json="[]",
            max_holding_sessions=10,
        )
        session.add(plan)
        session.flush()
        paper_position = PaperPosition(
            plan_id=plan.id,
            source_report_id=report_id,
            decision_signal_id=signal_id,
            source_model="model-a",
            market="cn",
            stock_code="600519",
            stock_name="贵州茅台",
            status="open",
            opened_on=date(2026, 8, 4),
            opened_price=101,
            normalized_notional=10100,
            quantity=100,
            currency="CNY",
            allocated_capital=10100,
            stop_loss=95,
            target_price=130,
            max_holding_sessions=10,
        )
        session.add(paper_position)
        session.flush()
        paper_position_id = paper_position.id

    service = _single_confirmation_service()
    service.sync_paper_position_to_portfolio(
        paper_position_id=paper_position_id,
        portfolio_account_id=account["id"],
        side="buy",
        quantity=100,
        price=101,
        trade_date=date(2026, 8, 4),
        fee=1.5,
        idempotency_key="paper-confirm-cache",
    )

    with isolated_db.get_session() as session:
        assert session.execute(
            select(PortfolioDailySnapshot).where(
                PortfolioDailySnapshot.account_id == account["id"],
                PortfolioDailySnapshot.snapshot_date == date(2026, 8, 5),
            )
        ).scalar_one_or_none() is None


def test_manual_intraday_analysis_is_valid_and_executable_in_same_session(isolated_db):
    metadata = {
        "market_phase_summary": {"phase": "intraday", "session_date": "2026-08-10"},
        "position_actions": {"holding": {"action": "buy", "reason": "same-session unit"}},
        **_frozen_style_metadata("position"),
    }
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 10),
        trigger_source="api",
        market_phase="intraday",
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    service = _single_confirmation_service()
    phase = _market_phase(date(2026, 8, 10))
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=phase):
        plan = service.create_plan_for_report(report_id, decision_signal_id=signal_id)["item"]
    assert plan["valid_for_date"] == "2026-08-10"

    _persist_daily_rows(isolated_db, _bars(start=date(2026, 7, 10), days=25))
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=phase):
        result = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 10, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote()),
            interval_seconds=300,
        )

    assert result["positions_opened"] == 1, result
    with isolated_db.get_session() as session:
        position = session.execute(select(PaperPosition)).scalar_one()
        assert position.opened_on == date(2026, 8, 10)
        assert position.source_report_id == report_id


def test_contract_revalidation_preserves_original_same_session_validity(isolated_db):
    metadata = {
        "market_phase_summary": {"phase": "intraday", "session_date": "2026-08-10"},
        "position_actions": {"holding": {"action": "buy"}},
        **_frozen_style_metadata("position"),
    }
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 10),
        trigger_source="api",
        market_phase="intraday",
        horizon="intraday",
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    service = _single_confirmation_service()
    with patch(
        "src.services.paper_trading_service.build_market_phase_context",
        return_value=_market_phase(date(2026, 8, 10)),
    ):
        initial = service.create_plan_for_report(
            report_id,
            decision_signal_id=signal_id,
        )["item"]
    assert initial["valid_for_date"] == "2026-08-10"
    assert initial["executable"] is False

    with isolated_db.session_scope() as session:
        session.get(DecisionSignalRecord, signal_id).horizon = "swing"

    lunch = _market_phase(date(2026, 8, 10), phase="lunch_break", hour=12)
    with patch(
        "src.services.paper_trading_service.build_market_phase_context",
        return_value=lunch,
    ):
        repaired = service.create_plan_for_report(
            report_id,
            decision_signal_id=signal_id,
        )["item"]

    assert repaired["executable"] is True
    assert repaired["status"] == "active"
    assert repaired["valid_for_date"] == "2026-08-10"


@pytest.mark.parametrize(
    "trigger_source,source_phase,runtime_phase",
    [
        ("system", "intraday", "intraday"),
        ("api", "postmarket", "postmarket"),
        ("api", "intraday", "lunch_break"),
    ],
)
def test_nonmanual_or_noncontinuous_analysis_remains_next_session_only(
    isolated_db, trigger_source, source_phase, runtime_phase
):
    metadata = {
        "market_phase_summary": {"phase": source_phase, "session_date": "2026-08-10"},
        "position_actions": {"holding": {"action": "buy"}},
    }
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 10),
        trigger_source=trigger_source,
        market_phase=source_phase,
        metadata_json=json.dumps(metadata, ensure_ascii=False),
    )
    phase = _market_phase(date(2026, 8, 10), phase=runtime_phase, hour=12 if runtime_phase == "lunch_break" else 15)
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=phase):
        plan = _single_confirmation_service().create_plan_for_report(
            report_id, decision_signal_id=signal_id
        )["item"]

    assert plan["valid_for_date"] == "2026-08-11"


def test_newest_nonexecutable_report_freezes_older_entry_authority_per_model(isolated_db):
    old_report, old_signal = _source(isolated_db, created_on=date(2026, 8, 7), model="model-a")
    other_report, other_signal = _source(isolated_db, created_on=date(2026, 8, 7), model="model-b")
    service = _single_confirmation_service()
    old_plan = service.create_plan_for_report(old_report, decision_signal_id=old_signal)["item"]
    other_plan = service.create_plan_for_report(other_report, decision_signal_id=other_signal)["item"]
    new_report, new_signal = _source(
        isolated_db,
        created_on=date(2026, 8, 8),
        model="model-a",
        action="watch",
        entry_low=None,
        entry_high=None,
        stop_loss=None,
        target_price=None,
    )
    newest = service.create_plan_for_report(new_report, decision_signal_id=new_signal)["item"]

    assert newest["status"] == "non_executable"
    with isolated_db.get_session() as session:
        assert session.get(PaperExecutionPlan, old_plan["id"]).status == "superseded"
        assert session.get(PaperExecutionPlan, other_plan["id"]).status == "active"


def test_out_of_order_older_report_cannot_displace_newer_nonexecutable_authority(isolated_db):
    newer_report, newer_signal = _source(
        isolated_db,
        created_on=date(2026, 8, 9),
        action="watch",
        entry_low=None,
        entry_high=None,
        stop_loss=None,
        target_price=None,
    )
    older_report, older_signal = _source(isolated_db, created_on=date(2026, 8, 8))
    service = _single_confirmation_service()
    newer = service.create_plan_for_report(newer_report, decision_signal_id=newer_signal)["item"]
    older = service.create_plan_for_report(older_report, decision_signal_id=older_signal)["item"]

    with isolated_db.get_session() as session:
        assert session.get(PaperExecutionPlan, newer["id"]).status == "non_executable"
        assert session.get(PaperExecutionPlan, older["id"]).status == "superseded"


def test_new_report_freezes_old_pending_legs_but_keeps_open_lot_monitored(isolated_db):
    old_plan_id, position_id = _seed_open_paper_lot(
        isolated_db, opened_on=date(2026, 8, 10), stock_code="600519", model="model-a"
    )
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        model="model-a",
        action="watch",
        entry_low=None,
        entry_high=None,
        stop_loss=None,
        target_price=None,
    )
    _single_confirmation_service().create_plan_for_report(report_id, decision_signal_id=signal_id)

    with isolated_db.get_session() as session:
        # A filled plan must remain attached to its open lot so deterministic
        # stop/target/T+1 monitoring can continue even after newer evidence
        # freezes any remaining entry authority.
        assert session.get(PaperExecutionPlan, old_plan_id).status == "superseded"
        assert session.get(PaperPosition, position_id).status == "open"
        assert session.query(PaperPosition).count() == 1
    operations = _single_confirmation_service().list_operations(model="model-a")["items"]
    assert any(item["id"] == old_plan_id and item["stage"] == "position_monitoring" for item in operations)


def test_holding_report_tightens_risk_once_across_repeated_monitor_polls(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(
        isolated_db, opened_on=date(2026, 8, 10), stock_code="600519", model="model-a"
    )
    metadata = {
        "holding_state": "holding",
        "base_action": "watch",
        "effective_action_source": "position_actions.holding",
        "position_actions": {
            "holding": {
                "action": "watch",
                "reason": "raise protection",
                "stop_loss": 98.0,
                "target_price": 140.0,
            }
        }
    }
    _report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        model="model-a",
        action="watch",
        metadata_json=json.dumps(metadata),
    )
    with isolated_db.session_scope() as session:
        session.get(DecisionSignalRecord, signal_id).created_at = datetime(2026, 8, 11, 9, 45)
    service = _single_confirmation_service()
    phase = _market_phase(date(2026, 8, 11))
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=phase):
        first = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )
        second = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 6, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=111,
                provider_timestamp="2026-08-11T10:06:00+08:00",
                fetched_at="2026-08-11T10:06:03+08:00",
            )),
            interval_seconds=300,
        )

    assert first["positions_closed"] == second["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.stop_loss == pytest.approx(98.0)
        assert position.target_price == pytest.approx(140.0)
        assert session.query(PaperRiskRevision).count() == 1
        assert session.query(PaperTradeEvent).count() == 0


def test_manual_and_postmarket_reports_immediately_version_every_matching_open_lot(isolated_db):
    first_plan_id, first_position_id = _seed_open_paper_lot(
        isolated_db, opened_on=date(2026, 8, 10), stock_code="600519", model="model-a"
    )
    second_plan_id, second_position_id = _seed_open_paper_lot(
        isolated_db, opened_on=date(2026, 8, 9), stock_code="600519", model="model-a"
    )
    _other_plan_id, other_position_id = _seed_open_paper_lot(
        isolated_db, opened_on=date(2026, 8, 10), stock_code="600519", model="model-b"
    )
    service = _single_confirmation_service()

    def holding_metadata(stop: float, target: float) -> str:
        return json.dumps({
            "holding_state": "holding",
            "base_action": "watch",
            "effective_action_source": "position_actions.holding",
            "position_actions": {
                "holding": {
                    "action": "watch",
                    "reason": "newer holding-aware report",
                    "stop_loss": stop,
                    "target_price": target,
                }
            },
        })

    manual_report_id, manual_signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        model="model-a",
        action="watch",
        trigger_source="api",
        metadata_json=holding_metadata(98.0, 140.0),
    )
    with isolated_db.session_scope() as session:
        session.get(DecisionSignalRecord, manual_signal_id).created_at = datetime(2026, 8, 11, 10, 0)
        session.add(StockDaily(code="600519.SH", date=date(2026, 8, 11), open=109, high=112, low=108, close=110, volume=1000))
    service.create_plan_for_report(manual_report_id, decision_signal_id=manual_signal_id)

    with isolated_db.get_session() as session:
        assert session.get(PaperPosition, first_position_id).stop_loss == pytest.approx(98.0)
        assert session.get(PaperPosition, second_position_id).target_price == pytest.approx(140.0)
        assert session.get(PaperPosition, other_position_id).stop_loss == pytest.approx(95.0)
        assert session.query(PaperRiskRevision).count() == 2

    postmarket_report_id, postmarket_signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 12),
        model="model-a",
        action="watch",
        trigger_source="system",
        metadata_json=holding_metadata(99.0, 145.0),
    )
    with isolated_db.session_scope() as session:
        session.get(DecisionSignalRecord, postmarket_signal_id).created_at = datetime(2026, 8, 12, 15, 10)
        session.add(StockDaily(code="600519.SH", date=date(2026, 8, 12), open=110, high=113, low=109, close=111, volume=1000))
    service.create_plan_for_report(postmarket_report_id, decision_signal_id=postmarket_signal_id)

    with isolated_db.get_session() as session:
        assert session.get(PaperPosition, first_position_id).stop_loss == pytest.approx(99.0)
        assert session.get(PaperPosition, first_position_id).target_price == pytest.approx(145.0)
        assert session.get(PaperPosition, second_position_id).stop_loss == pytest.approx(99.0)
        assert session.get(PaperPosition, second_position_id).target_price == pytest.approx(145.0)
        assert session.get(PaperPosition, other_position_id).stop_loss == pytest.approx(95.0)
        assert session.query(PaperRiskRevision).count() == 4

    positions = service.list_positions(model="model-a", status="open", limit=10)["items"]
    by_id = {item["id"]: item for item in positions}
    for position_id in (first_position_id, second_position_id):
        item = by_id[position_id]
        assert item["source_plan_stop_loss"] == pytest.approx(95.0)
        assert item["source_plan_target_price"] == pytest.approx(130.0)
        assert item["effective_stop_loss"] == pytest.approx(99.0)
        assert item["effective_target_price"] == pytest.approx(145.0)
        assert item["latest_risk_revision"]["source_report_id"] == postmarket_report_id
        assert item["latest_risk_revision"]["revision_version"] == 2

    operations = service.list_operations(model="model-a", limit=20)["items"]
    operation_by_plan = {item["id"]: item for item in operations}
    for plan_id, position_id in (
        (first_plan_id, first_position_id),
        (second_plan_id, second_position_id),
    ):
        lot = operation_by_plan[plan_id]["effective_risk_lots"][0]
        assert lot["position_id"] == position_id
        assert lot["source_plan_stop_loss"] == pytest.approx(95.0)
        assert lot["effective_stop_loss"] == pytest.approx(99.0)
        assert lot["effective_target_price"] == pytest.approx(145.0)
        assert lot["revision_source_report_id"] == postmarket_report_id


def test_monitor_reconciles_eligible_preexisting_report_revision_once(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(
        isolated_db, opened_on=date(2026, 8, 10), stock_code="600519", model="model-a"
    )
    metadata = json.dumps({
        "holding_state": "holding",
        "base_action": "watch",
        "effective_action_source": "position_actions.holding",
        "position_actions": {
            "holding": {
                "action": "watch",
                "reason": "stored before runtime upgrade",
                "stop_loss": 98.0,
                "target_price": 140.0,
            }
        },
    })
    report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        model="model-a",
        action="watch",
        trigger_source="system",
        metadata_json=metadata,
    )
    with isolated_db.session_scope() as session:
        session.get(DecisionSignalRecord, signal_id).created_at = datetime(2026, 8, 11, 15, 10)
        session.add(StockDaily(code="600519.SH", date=date(2026, 8, 11), open=109, high=112, low=108, close=110, volume=1000))

    service = _single_confirmation_service()
    first = service.reconcile_open_position_risk_from_latest_reports(
        current_time=datetime(2026, 8, 12, 1, 0, tzinfo=timezone.utc)
    )
    second = service.reconcile_open_position_risk_from_latest_reports(
        current_time=datetime(2026, 8, 12, 1, 5, tzinfo=timezone.utc)
    )

    assert first["revisions_applied"] == 1
    assert second["revisions_applied"] == 0
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        revision = session.execute(select(PaperRiskRevision)).scalar_one()
        assert position.stop_loss == pytest.approx(98.0)
        assert position.target_price == pytest.approx(140.0)
        assert revision.source_report_id == report_id
        assert revision.decision_signal_id == signal_id


def test_risk_revision_rejects_lower_stop_and_contradictory_prices(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(
        isolated_db, opened_on=date(2026, 8, 9), stock_code="600519"
    )
    service = _single_confirmation_service()
    with isolated_db.session_scope() as session:
        position = session.get(PaperPosition, position_id)
        lower = service._record_risk_revision(
            session,
            position,
            source_report_id=901,
            decision_signal_id=901,
            new_stop_loss=94,
            new_target_price=140,
            evaluated_at=datetime(2026, 8, 10, 10, 0),
            current_price=100,
        )
        contradictory = service._record_risk_revision(
            session,
            position,
            source_report_id=902,
            decision_signal_id=902,
            new_stop_loss=120,
            new_target_price=110,
            evaluated_at=datetime(2026, 8, 10, 10, 5),
            current_price=100,
        )
        lower_but_breached = service._record_risk_revision(
            session,
            position,
            source_report_id=903,
            decision_signal_id=903,
            new_stop_loss=94,
            new_target_price=140,
            evaluated_at=datetime(2026, 8, 10, 10, 10),
            current_price=90,
        )
        assert lower.result == "rejected" and lower.reason == "lower_stop_loss_rejected"
        assert contradictory.result == "rejected"
        assert contradictory.reason == "contradictory_exit_prices_rejected"
        assert lower_but_breached.result == "rejected"
        assert lower_but_breached.reason == "lower_stop_loss_rejected"
        assert position.stop_loss == pytest.approx(95)
        assert position.target_price == pytest.approx(130)


@pytest.mark.parametrize(
    "holding_risk,expected_reason",
    [
        ({"stop_loss": 101.0, "target_price": 140.0}, "model_risk_stop_breached"),
        ({"stop_loss": 98.0, "target_price": 99.0}, "model_risk_target_reached"),
    ],
)
def test_newly_breached_model_risk_obeys_cn_t1_then_closes_next_session(
    isolated_db, holding_risk, expected_reason
):
    _plan_id, position_id = _seed_open_paper_lot(
        isolated_db, opened_on=date(2026, 8, 11), stock_code="600519", model="model-a"
    )
    metadata = {
        "holding_state": "holding",
        "base_action": "watch",
        "effective_action_source": "position_actions.holding",
        "position_actions": {
            "holding": {"action": "watch", "reason": "fresh defense", **holding_risk}
        }
    }
    _report_id, signal_id = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        model="model-a",
        action="watch",
        metadata_json=json.dumps(metadata),
    )
    with isolated_db.session_scope() as session:
        session.get(DecisionSignalRecord, signal_id).created_at = datetime(2026, 8, 11, 9, 45)
    service = _single_confirmation_service()
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        first = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=100,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )
    assert first["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.status == "open"
        assert position.pending_exit_reason is None
        assert position.stop_loss == pytest.approx(95.0)
        assert position.target_price == pytest.approx(130.0)
        revision = session.execute(select(PaperRiskRevision)).scalar_one()
        assert revision.result == "rejected"
        assert revision.reason in {
            "stop_above_or_at_current_price_rejected",
            "target_below_or_at_current_price_rejected",
        }
        assert session.query(PaperTradeEvent).filter_by(event_type="pending_exit").count() == 0

    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 12))):
        second = service.run_intraday_monitor(
            current_time=datetime(2026, 8, 12, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=102,
                provider_timestamp="2026-08-12T10:00:00+08:00",
                fetched_at="2026-08-12T10:00:03+08:00",
            )),
            interval_seconds=300,
        )
    assert second["positions_closed"] == 0
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.status == "open"
        assert position.close_reason is None


def test_other_model_report_cannot_revise_open_position_risk(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(
        isolated_db, opened_on=date(2026, 8, 10), stock_code="600519", model="model-a"
    )
    metadata = {
        "position_actions": {
            "holding": {"action": "watch", "stop_loss": 99.0, "target_price": 140.0}
        }
    }
    _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        model="model-b",
        action="watch",
        metadata_json=json.dumps(metadata),
    )
    with patch("src.services.paper_trading_service.build_market_phase_context", return_value=_market_phase(date(2026, 8, 11))):
        _single_confirmation_service().run_intraday_monitor(
            current_time=datetime(2026, 8, 11, 2, 0, tzinfo=timezone.utc),
            fetcher_manager=_QuoteManager(_quote(
                price=110,
                provider_timestamp="2026-08-11T10:00:00+08:00",
                fetched_at="2026-08-11T10:00:03+08:00",
            )),
            interval_seconds=300,
        )
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        assert position.stop_loss == pytest.approx(95)
        assert position.target_price == pytest.approx(130)
        assert session.query(PaperRiskRevision).count() == 0


def _research_history_result(code: str = "600519"):
    from src.analyzer import AnalysisResult

    return AnalysisResult(
        code=code,
        name="贵州茅台",
        sentiment_score=72,
        trend_prediction="看多",
        operation_advice="买入",
        decision_type="buy",
        confidence_level="高",
        analysis_summary="research-only evidence",
    )


def _ledger_counts(db) -> dict:
    with db.get_session() as session:
        return {
            model.__tablename__: session.query(model).count()
            for model in (
                DecisionSignalRecord,
                PaperExecutionPlan,
                PaperStrategyThesis,
                PaperStrategyTransition,
                PaperPosition,
                PaperTradeEvent,
                PaperCapitalAccount,
                PaperRiskRevision,
            )
        }


@pytest.mark.parametrize(
    ("execution_intent", "execution_scope", "reason"),
    [
        ("research_only", "primary", "research_only"),
        ("paper_candidate", "experiment:exp1:b", "execution_scope_mismatch"),
        (None, "primary", "execution_intent_missing"),
    ],
)
def test_unauthorized_report_purpose_cannot_create_plan_or_touch_ledger(
    isolated_db, execution_intent, execution_scope, reason,
):
    report_id, signal_id = _source(
        isolated_db,
        execution_intent=execution_intent,
        execution_scope=execution_scope,
    )
    before = _ledger_counts(isolated_db)
    service = _single_confirmation_service()

    with pytest.raises(PaperTradingExecutionNotAuthorizedError) as excinfo:
        service.create_plan_for_report(report_id, decision_signal_id=signal_id)

    assert excinfo.value.reason == reason
    assert excinfo.value.report_id == report_id
    assert _ledger_counts(isolated_db) == before
    if execution_intent == "paper_candidate":
        # The same frozen purpose authorizes only its own trusted scope.
        scoped = PaperTradingService(execution_scope=execution_scope)
        assert scoped.report_execution_authorization(report_id)["authorized"] is True


def test_research_report_reconcile_is_terminal_visible_and_never_retried(isolated_db):
    report_id = isolated_db.save_analysis_history(
        result=_research_history_result(),
        query_id="research-reconcile",
        report_type="detailed",
        news_content=None,
        execution_intent={
            "execution_intent": "research_only",
            "intent_source": "unit_test",
        },
    )
    assert report_id > 0
    service = _single_confirmation_service()

    first_pass = service.reconcile_report_execution_plans()
    immediate = service.reconcile_report_execution_plan(report_id)
    second_pass = service.reconcile_report_execution_plans()

    assert first_pass["reports_considered"] == 1
    blocked = first_pass["items"][0]
    assert blocked["status"] == "terminal"
    assert blocked["classification"] == "execution_not_authorized"
    assert blocked["reason"] == "research_only"
    assert blocked["signal_id"] is None and blocked["plan_id"] is None
    assert (immediate["status"], immediate["classification"]) == ("terminal", "execution_not_authorized")
    assert second_pass["reports_considered"] == 0
    with isolated_db.get_session() as session:
        row = session.execute(select(ReportPlanReconciliation)).scalar_one()
        assert (row.status, row.attempt_count, row.last_error) == ("terminal", 1, "research_only")
        details = json.loads(row.details_json)
        assert details["execution_block_reason"] == "research_only"
        assert details["execution_scope"] == "primary"
    # Terminal before signal backfill: no DecisionSignal, thesis or plan exists.
    assert set(_ledger_counts(isolated_db).values()) == {0}


def test_newer_unauthorized_reports_do_not_mask_bootstrap_or_import_identity(isolated_db):
    legit_report, _ = _source(isolated_db, created_on=date(2026, 8, 3))
    _source(isolated_db, created_on=date(2026, 8, 4), execution_intent="research_only")
    _source(isolated_db, created_on=date(2026, 8, 5), execution_scope="experiment:exp1:c")
    _source(isolated_db, created_on=date(2026, 8, 6), execution_intent=None)
    service = PaperTradingService()

    assert service.bootstrap_strategy_observations()["created"] == 1
    with isolated_db.get_session() as session:
        thesis = session.execute(select(PaperStrategyThesis)).scalar_one()
        assert thesis.last_report_id == legit_report
        latest = service._latest_report_for_identity_in_session(
            session, source_model="model-a", market="cn", stock_code="600519",
        )
        assert latest.id == legit_report


def test_research_report_cannot_revise_open_risk_while_legit_report_still_protects(isolated_db):
    _plan_id, position_id = _seed_open_paper_lot(
        isolated_db, opened_on=date(2026, 8, 10), stock_code="600519", model="model-a"
    )

    def holding_metadata(stop_loss: float, target_price: float) -> str:
        return json.dumps({
            "holding_state": "holding",
            "base_action": "watch",
            "effective_action_source": "position_actions.holding",
            "position_actions": {
                "holding": {"action": "watch", "stop_loss": stop_loss, "target_price": target_price},
            },
        })

    legit_report, legit_signal = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        action="watch",
        metadata_json=holding_metadata(98.0, 140.0),
    )
    research_report, research_signal = _source(
        isolated_db,
        created_on=date(2026, 8, 11),
        action="watch",
        metadata_json=holding_metadata(105.0, 160.0),
        execution_intent="research_only",
    )
    with isolated_db.session_scope() as session:
        session.get(DecisionSignalRecord, legit_signal).created_at = datetime(2026, 8, 11, 15, 10)
        # Even a directly inserted research signal is newer and must stay invisible.
        session.get(DecisionSignalRecord, research_signal).created_at = datetime(2026, 8, 11, 15, 20)
        session.add(StockDaily(code="600519.SH", date=date(2026, 8, 11), open=109, high=112, low=108, close=110, volume=1000))

    summary = _single_confirmation_service().reconcile_open_position_risk_from_latest_reports(
        current_time=datetime(2026, 8, 12, 1, 0, tzinfo=timezone.utc)
    )

    assert summary["revisions_applied"] == 1
    with isolated_db.get_session() as session:
        position = session.get(PaperPosition, position_id)
        revision = session.execute(select(PaperRiskRevision)).scalar_one()
        assert revision.source_report_id == legit_report != research_report
        assert position.stop_loss == pytest.approx(98.0)
        assert position.target_price == pytest.approx(140.0)


def test_research_report_metadata_cannot_reclassify_account_asset_or_sector(isolated_db):
    context = json.dumps({
        "provider_context": {"asset_type": "broad_etf"},
        "belong_boards": [{"name": "半导体", "type": "行业"}],
    }, ensure_ascii=False)
    research_report, _ = _source(
        isolated_db, report_code="510300", signal_code="510300", execution_intent="research_only",
    )
    with isolated_db.session_scope() as session:
        session.get(AnalysisHistory, research_report).context_snapshot = context
    service = PaperTradingService()

    with isolated_db.get_session() as session:
        assert service._asset_type_from_structured_metadata(session, "cn", "510300") is None
        assert service._sector_or_unknown(session, "cn", "510300") == "unknown"

    legit_report, _ = _source(isolated_db, report_code="510300", signal_code="510300")
    with isolated_db.session_scope() as session:
        session.get(AnalysisHistory, legit_report).context_snapshot = context
    with isolated_db.get_session() as session:
        assert service._asset_type_from_structured_metadata(session, "cn", "510300") == "broad_etf"
        assert service._sector_or_unknown(session, "cn", "510300") == "半导体"
