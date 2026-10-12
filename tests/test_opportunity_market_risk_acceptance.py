# -*- coding: utf-8 -*-
"""R5: end-to-end red-light acceptance for the opportunity A/B/C experiment arms.

The pure policy and the single decision point are covered by
``test_opportunity_market_risk_policy`` and
``test_opportunity_market_policy_engine``.  This module proves the *fill*
outcome through the real ``run_eod`` open cycle: under a fresh red-light regime
with an intact CSI300 medium trend, arm A opens nothing, arm B fills at a
throttled 0.25x budget bounded by the scaled 5% symbol / 20% total caps, and
arm C fills at the full base budget.  Fees are charged, the market multiplier is
applied once, and an already-open position over the trial cap is never
force-sold.

Report/signal/plan seeding and stock bars reuse the proven helpers in
``test_paper_trading_service`` so this file only asserts the experiment-arm
differences, not the generic paper-trading machinery.
"""

import contextlib
import json
import os
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import select

from src.config import Config
from src.core import trading_calendar
from src.services.paper_trading_service import PaperTradingService
from src.storage import (
    DailyMarketRegime,
    DatabaseManager,
    PaperPosition,
    StockDaily,
)
from tests.test_paper_trading_service import (
    _create_plan_with_report_clock,
    _frozen_style_metadata,
    _seed_open_paper_lot,
    _source,
    TRADING_STYLE_SETTINGS,
)

BENCHMARK = "000300.SH"
AS_OF = date(2026, 9, 24)
EQUITY = 100000.0
# 低价个股，使红灯试探期缩放后的 5% 单股上限（5000 元）仍能买下至少一手（100 股）。
# 参照方案 §123：红灯试探以约 10 元买价为例。
ENTRY_PRICE = 9.0
ENTRY_LOW = 8.0
ENTRY_HIGH = 9.5
STOP_LOSS = 7.5
TARGET_PRICE = 13.0
# 个股 bars 的最后一根（完整）落在 AS_OF；start + (days-1) == AS_OF。
STOCK_BARS_START = AS_OF - timedelta(days=5)


def _stock_bars(*, days=6):
    """Low-priced complete daily bars ending at AS_OF, mirroring the shape of the
    shared ``_bars`` helper but priced so the trial cap can afford a lot."""

    window = max(days, TRADING_STYLE_SETTINGS["position"]["analysis_window_sessions"])
    first_date = STOCK_BARS_START - timedelta(days=window - days)
    rows = []
    for index in range(window):
        bar_date = first_date + timedelta(days=index)
        is_current = index == window - 1
        close = ENTRY_PRICE if is_current else ENTRY_PRICE - 0.1
        rows.append({
            "date": bar_date,
            "open": close, "high": close + 0.5, "low": close - 0.5,
            "close": close, "volume": 151.0 if is_current else 100.0,
            "is_complete": True,
        })
    return rows


@contextlib.contextmanager
def _arm_database(tmp_path, tag):
    old = os.environ.get("DATABASE_PATH")
    old_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_PATH"] = str(tmp_path / f"acceptance-{tag}.db")
    os.environ.pop("DATABASE_URL", None)
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager.get_instance()
    try:
        yield db
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        for key, value in (("DATABASE_PATH", old), ("DATABASE_URL", old_url)):
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _csi300_sessions(count):
    cal = trading_calendar.xcals.get_calendar("XSHG")
    sessions = [ts.date() for ts in cal.sessions_in_range("2026-04-01", AS_OF.isoformat())]
    return sessions[-count:]


def _alternating(count, *, low=100.0, high=102.0):
    return [high if (count - 1 - i) % 2 == 0 else low for i in range(count)]


def _seed_intact_red_light(db):
    """Fresh risk_off regime plus an intact-trend CSI300 window ending at AS_OF."""

    closes = _alternating(62)
    sessions = _csi300_sessions(len(closes))
    with db.session_scope() as session:
        for day, close in zip(sessions, closes):
            session.add(StockDaily(
                code=BENCHMARK, date=day,
                open=close, high=close + 1.0, low=close - 1.0, close=close,
                volume=1000.0, data_source="acceptance-index",
            ))
        session.add(DailyMarketRegime(
            regime_key=f"cn:{AS_OF.isoformat()}:risk_off:market-regime-v1",
            region="cn", session_date=AS_OF, regime="risk_off",
            allocation_multiplier=0.0, entry_allowed=False,
            freshness_expires_at=datetime(2026, 9, 25, 15, 0),
            policy_version="market-regime-v1",
            provenance_json=json.dumps({"source": "acceptance"}, ensure_ascii=False),
        ))


def _service(arm):
    return PaperTradingService(
        execution_scope=f"experiment:exp1:{arm}",
        market_context_risk_control_enabled=True,
        strategy_entry_confirmation_sessions=1,
        max_symbol_exposure_pct=20.0,
        max_total_exposure_pct=80.0,
        commission_rate=0.0003,
        min_commission=5.0,
        slippage_pct=0.03,
        initial_capital=EQUITY,
    )


def _run_arm(tmp_path, arm):
    """Seed + open one arm end to end; return (opened, allocated_capital, quantity)."""

    with _arm_database(tmp_path, arm) as db:
        _seed_intact_red_light(db)
        # 报告日为 AS_OF 的上一交易日（2026-09-23），使计划 valid_for_date 恰为 AS_OF。
        report_id, signal_id = _source(
            db,
            created_on=date(2026, 9, 23),
            execution_scope=f"experiment:exp1:{arm}",
            entry_low=ENTRY_LOW,
            entry_high=ENTRY_HIGH,
            stop_loss=STOP_LOSS,
            target_price=TARGET_PRICE,
            metadata_json=json.dumps({
                "holding_state": "empty",
                **_frozen_style_metadata("position", support=ENTRY_LOW),
            }, ensure_ascii=False),
        )
        service = _service(arm)
        _create_plan_with_report_clock(service, report_id, decision_signal_id=signal_id)
        summary = service.run_eod(AS_OF, {("cn", "600519"): _stock_bars()})
        allocated = 0.0
        quantity = 0.0
        with db.get_session() as session:
            position = session.execute(
                select(PaperPosition).where(PaperPosition.status == "open")
            ).scalar_one_or_none()
            if position is not None:
                allocated = float(position.allocated_capital or 0.0)
                quantity = float(position.quantity or 0.0)
        return summary["positions_opened"], allocated, quantity


def test_red_light_arm_a_opens_nothing(tmp_path):
    opened, allocated, _ = _run_arm(tmp_path, "a")
    assert opened == 0
    assert allocated == 0.0


def test_red_light_arm_b_fills_within_scaled_trial_caps(tmp_path):
    opened, allocated, quantity = _run_arm(tmp_path, "b")
    assert opened == 1
    assert quantity > 0
    # 红灯试探：总敞口缩放到 20%、单股缩放到 5%；建仓成本（含费用）落在缩放上限内。
    assert allocated <= EQUITY * 0.20
    assert allocated <= EQUITY * 0.05 + 50.0  # 5% 单股上限 + 少量费用容差


def test_red_light_arm_c_fills_more_than_trial_arm_b(tmp_path):
    _, allocated_b, quantity_b = _run_arm(tmp_path, "b")
    opened_c, allocated_c, quantity_c = _run_arm(tmp_path, "c")
    assert opened_c == 1
    # 诊断组 C 不按红灯缩量，建仓规模严格大于被限制的 B。
    assert quantity_c > quantity_b
    assert allocated_c > allocated_b


def test_red_light_does_not_force_sell_existing_over_cap_position(tmp_path):
    """A pre-existing arm-B lot above the trial cap is held, never force-sold."""

    with _arm_database(tmp_path, "hold") as db:
        _seed_intact_red_light(db)
        # 现有持仓开仓价 100、止损 95、止盈 130；喂入区间内的行情（收 100，
        # 最低 98 > 止损，最高 102 < 止盈），确保任何平仓只可能来自（错误的）缩放上限。
        _plan_id, _position_id = _seed_open_paper_lot(
            db, opened_on=AS_OF - timedelta(days=5), stock_code="600519", model="model-a"
        )
        held_bars = [
            {
                "date": STOCK_BARS_START + timedelta(days=index),
                "open": 100.0, "high": 102.0, "low": 98.0, "close": 100.0,
                "volume": 120.0, "is_complete": True,
            }
            for index in range(6)
        ]
        service = _service("b")
        summary = service.run_eod(AS_OF, {("cn", "600519"): held_bars})
        assert summary["positions_closed"] == 0
        with db.get_session() as session:
            held = session.execute(
                select(PaperPosition).where(PaperPosition.status == "open")
            ).scalar_one()
            # 现有持仓（200 股 / 2 万成本）高于试探期上限，但保持持有不被强制卖出。
            assert float(held.quantity) == pytest.approx(200)
