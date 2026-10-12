# -*- coding: utf-8 -*-
"""R3: the paper engine's single market-policy consumption point per arm.

These tests assert that experiment arms (``experiment:<id>:<abc>``) consume the
pure ``evaluate_opportunity_market_policy`` through one decision that sizing, the
symbol cap, the total-exposure invariant and the account gate all read, while the
primary book and green/yellow days keep the legacy regime behaviour untouched.
"""

import json
import os
from datetime import date, datetime, time, timezone

import pytest

from src.config import Config
from src.core import trading_calendar
from src.services.paper_trading_service import PaperTradingService
from src.storage import DailyMarketRegime, DatabaseManager, StockDaily

BENCHMARK = "000300.SH"
AS_OF = date(2026, 9, 24)
NOON_UTC = datetime(2026, 9, 24, 2, 0, tzinfo=timezone.utc)


@pytest.fixture()
def isolated_db(tmp_path):
    old = os.environ.get("DATABASE_PATH")
    os.environ["DATABASE_PATH"] = str(tmp_path / "opp-engine.db")
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager.get_instance()
    try:
        yield db
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        if old is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = old


def _csi300_sessions(count):
    cal = trading_calendar.xcals.get_calendar("XSHG")
    sessions = [ts.date() for ts in cal.sessions_in_range("2026-04-01", AS_OF.isoformat())]
    return sessions[-count:]


def _seed_index(db, closes, *, code=BENCHMARK):
    sessions = _csi300_sessions(len(closes))
    with db.session_scope() as session:
        for day, close in zip(sessions, closes):
            session.add(StockDaily(
                code=code,
                date=day,
                open=close,
                high=close + 1.0,
                low=close - 1.0,
                close=close,
                volume=1000.0,
                data_source="opp-engine-test",
            ))
    return sessions


def _seed_regime(db, regime, *, session_date=AS_OF):
    with db.session_scope() as session:
        session.add(DailyMarketRegime(
            regime_key=f"cn:{session_date.isoformat()}:{regime}:market-regime-v1",
            region="cn",
            session_date=session_date,
            regime=regime,
            allocation_multiplier={"normal": 1.0, "cautious": 0.5}.get(regime, 0.0),
            entry_allowed=regime in {"normal", "cautious"},
            freshness_expires_at=datetime(2026, 9, 25, 15, 0),
            policy_version="market-regime-v1",
            provenance_json=json.dumps({"source": "unit"}, ensure_ascii=False),
        ))


def _alternating(count, *, low=100.0, high=102.0):
    # Ends high; MA20 == MA20 five sessions ago; close >= MA60; variance > 0.
    return [high if (count - 1 - i) % 2 == 0 else low for i in range(count)]


def _service(arm):
    return PaperTradingService(
        market_context_risk_control_enabled=True,
        execution_scope=f"experiment:exp1:{arm}",
        max_symbol_exposure_pct=20.0,
        max_total_exposure_pct=80.0,
    )


def _decide(db, service, current_time=NOON_UTC):
    with db.get_session() as session:
        account = service._get_or_create_account(session, source_model="model-a", market="cn")
        return service._market_policy_decision_in_session(
            session, account=account, market="cn", current_time=current_time,
        )


def test_red_light_intact_trend_blocks_arm_a_but_grants_arm_b_quarter(isolated_db):
    _seed_index(isolated_db, _alternating(62))
    _seed_regime(isolated_db, "risk_off")

    arm_a = _decide(isolated_db, _service("a"))
    arm_b = _decide(isolated_db, _service("b"))

    assert arm_a["block_new_risk"] is True
    assert arm_a["reasons"] == ["market_context_risk_off"]
    assert arm_a["exception"] is None

    assert arm_b["block_new_risk"] is False
    assert arm_b["allocation_multiplier"] == pytest.approx(0.25)
    assert arm_b["symbol_cap_factor"] == pytest.approx(0.25)
    assert arm_b["total_cap_factor"] == pytest.approx(0.25)
    assert arm_b["exception"] == "red_light_trial"
    assert arm_b["policy_version"] == "opportunity-market-risk-b1-v1"
    assert len(arm_b["input_hash"]) == 64


def test_red_light_broken_trend_keeps_arm_b_closed(isolated_db):
    # Strictly rising then a hard break below MA60 fails the direction gate.
    closes = _alternating(62)
    closes[-1] = 90.0
    _seed_index(isolated_db, closes)
    _seed_regime(isolated_db, "risk_off")

    arm_b = _decide(isolated_db, _service("b"))

    assert arm_b["block_new_risk"] is True
    assert arm_b["exception"] is None
    assert "index_below_ma60" in arm_b["reasons"]


@pytest.mark.parametrize("regime,multiplier", [("normal", 1.0), ("cautious", 0.5)])
def test_green_and_yellow_are_identical_for_arm_a_and_b(isolated_db, regime, multiplier):
    _seed_index(isolated_db, _alternating(62))
    _seed_regime(isolated_db, regime)

    arm_a = _decide(isolated_db, _service("a"))
    arm_b = _decide(isolated_db, _service("b"))

    for key in ("block_new_risk", "allocation_multiplier", "symbol_cap_factor", "total_cap_factor"):
        assert arm_a[key] == arm_b[key]
    assert arm_b["block_new_risk"] is False
    assert arm_b["allocation_multiplier"] == pytest.approx(multiplier)
    assert arm_b["symbol_cap_factor"] == pytest.approx(1.0)
    assert arm_b["exception"] is None


def test_arm_c_ignores_direction_but_needs_valid_regime(isolated_db):
    _seed_index(isolated_db, _alternating(62))
    _seed_regime(isolated_db, "risk_off")

    arm_c = _decide(isolated_db, _service("c"))

    assert arm_c["block_new_risk"] is False
    assert arm_c["allocation_multiplier"] == pytest.approx(1.0)
    assert arm_c["symbol_cap_factor"] == pytest.approx(1.0)
    assert arm_c["total_cap_factor"] == pytest.approx(1.0)


def test_stale_regime_blocks_every_arm(isolated_db):
    _seed_index(isolated_db, _alternating(62))
    _seed_regime(isolated_db, "risk_off", session_date=date(2026, 9, 18))

    for arm in ("a", "b", "c"):
        decision = _decide(isolated_db, _service(arm))
        assert decision["block_new_risk"] is True


def test_primary_scope_keeps_legacy_regime_semantics(isolated_db):
    _seed_index(isolated_db, _alternating(62))
    _seed_regime(isolated_db, "risk_off")
    service = PaperTradingService(market_context_risk_control_enabled=True)  # primary

    decision = _decide(isolated_db, service)

    assert decision["policy"] == "primary"
    assert decision["arm"] is None
    assert decision["block_new_risk"] is True
    assert decision["symbol_cap_factor"] == pytest.approx(1.0)
    assert decision["total_cap_factor"] == pytest.approx(1.0)


def test_red_light_scaled_caps_flow_into_symbol_and_total_invariants(isolated_db):
    _seed_index(isolated_db, _alternating(62))
    _seed_regime(isolated_db, "risk_off")
    service = _service("b")
    decision = _decide(isolated_db, service)
    symbol_factor = decision["symbol_cap_factor"]
    total_factor = decision["total_cap_factor"]

    with service.db.get_session() as session:
        account = service._get_or_create_account(session, source_model="model-a", market="cn")
        book_equity = service._book_equity(account)
        scaled = service._symbol_exposure_snapshot(
            session, account,
            source_model="model-a", market="cn", stock_code="600519",
            projected_additional_cost=book_equity * 0.10,
            symbol_cap_factor=symbol_factor,
        )
        unscaled = service._symbol_exposure_snapshot(
            session, account,
            source_model="model-a", market="cn", stock_code="600519",
            projected_additional_cost=book_equity * 0.10,
        )

    # Base symbol cap is min(20, asset-class 20) = 20% -> scaled to 5%.
    assert unscaled["symbol_exposure_ceiling"] == pytest.approx(book_equity * 0.20)
    assert scaled["symbol_exposure_ceiling"] == pytest.approx(book_equity * 0.05)
    # 10% projected exposure clears the 20% base cap but breaches the 5% trial cap.
    assert unscaled["within_limit"] is True
    assert scaled["within_limit"] is False

    # Total-exposure invariant scales the frozen 80% cap to 20%.
    fake_account = _AccountView(book_equity=book_equity, max_total_exposure_pct=80.0, cash=book_equity)
    over_trial = {"net_amount": book_equity * 0.30}
    within_trial = {"net_amount": book_equity * 0.10}
    assert service._open_fill_invariant_error(
        account=fake_account, fill=over_trial, total_cap_factor=total_factor,
    ) == "open_fill_exceeds_total_exposure_cap"
    assert service._open_fill_invariant_error(
        account=fake_account, fill=within_trial, total_cap_factor=total_factor,
    ) is None


class _AccountView:
    """Minimal stand-in for the invariant check's account attribute reads."""

    def __init__(self, *, book_equity, max_total_exposure_pct, cash):
        # _book_equity reads cash + invested_capital.
        self.cash = cash
        self.invested_capital = max(0.0, book_equity - cash)
        self.max_total_exposure_pct = max_total_exposure_pct
        self.max_position_pct = 100.0
        self.exposure = 0.0
