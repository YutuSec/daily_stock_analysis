# -*- coding: utf-8 -*-

from src.services.paper_strategy_policy import (
    entry_geometry_guard,
    plan_holding_sessions_for_horizon,
    setup_contract,
    validate_risk_revision_geometry,
)


TRADING_STYLE_SETTINGS = {
    "position": {
        "compatible_horizons": ["10d", "swing"],
        "default_signal_horizon": "swing",
        "default_holding_sessions": 40,
        "max_holding_sessions": 60,
    }
}


def test_position_style_horizons_are_medium_term_only():
    settings = TRADING_STYLE_SETTINGS["position"]
    assert settings["compatible_horizons"] == ["10d", "swing"]
    assert plan_holding_sessions_for_horizon("10d", settings)["applied_sessions"] == 10
    assert plan_holding_sessions_for_horizon("swing", settings)["applied_sessions"] == 40
    assert plan_holding_sessions_for_horizon("long", settings)["applied_sessions"] <= settings["max_holding_sessions"]


def test_style_matching_symbolic_horizon_uses_default_holding_sessions_without_provenance():
    settings = TRADING_STYLE_SETTINGS["position"]

    numeric = plan_holding_sessions_for_horizon("10d", settings)
    defaulted = plan_holding_sessions_for_horizon(
        "swing",
        settings,
        horizon_source="paper_trading_style_default",
    )

    assert numeric["applied_sessions"] == 10
    assert numeric["duration_source"] == "explicit_horizon"
    assert plan_holding_sessions_for_horizon("swing", settings)["applied_sessions"] == 40
    assert defaulted["applied_sessions"] == 40
    assert defaulted["duration_source"] == "style_default_holding_sessions"


def test_setup_contract_requires_explicit_or_structured_geometry_not_prose():
    prose_only = {"dashboard": {"phase_decision": {"watch_conditions": ["回踩缩量企稳"]}}}
    assert setup_contract(prose_only, [])["error"] == "unsupported_strategy_setup_type"

    explicit = {"paper_trading": {"setup_type": "pullback"}}
    assert setup_contract(explicit, [])["setup_type"] == "pullback"

    inferred = setup_contract({}, [{"type": "close_above_prior_high", "period": 20}])
    assert inferred["setup_type"] == "breakout"
    assert inferred["source"] == "legacy_structured_rules"


def test_geometry_guard_uses_atr_when_available_but_missing_atr_is_not_blocking():
    without_atr = entry_geometry_guard(
        style="position",
        style_settings=TRADING_STYLE_SETTINGS["position"],
        indicator_snapshot={},
        entry_reference=100.0,
        stop_loss=97.0,
    )
    assert without_atr["passed"] is True
    assert without_atr["atr"] is None

    pipeline_shape = entry_geometry_guard(
        style="position",
        style_settings=TRADING_STYLE_SETTINGS["position"],
        indicator_snapshot={
            "source": "pipeline.style_indicator_snapshot",
            "moving_averages": {"ma20": 97.0, "ma60": 92.0},
            "volatility": {"atr20": 5.0, "atr60": 7.0},
            "support": 97.0,
            "support_basis": "nearest_frozen_style_ma_at_or_below_close",
        },
        entry_reference=100.0,
        stop_loss=97.0,
    )
    assert pipeline_shape["passed"] is False
    assert pipeline_shape["atr"] == 5.0
    assert pipeline_shape["support"] == 97.0
    assert "stop_within_atr_noise" in pipeline_shape["reasons"]


def test_risk_revision_rejects_implicit_immediate_exit_geometry():
    missing_price = validate_risk_revision_geometry(
        current_price=None,
        old_stop_loss=90.0,
        proposed_stop_loss=95.0,
        proposed_target_price=120.0,
    )
    assert missing_price.passed is False
    assert missing_price.reason == "missing_validated_reference_price_rejected"

    stop_validation = validate_risk_revision_geometry(
        current_price=100.0,
        old_stop_loss=90.0,
        proposed_stop_loss=101.0,
        proposed_target_price=120.0,
    )
    assert stop_validation.passed is False
    assert stop_validation.reason == "stop_above_or_at_current_price_rejected"

    target_validation = validate_risk_revision_geometry(
        current_price=100.0,
        old_stop_loss=90.0,
        proposed_stop_loss=95.0,
        proposed_target_price=99.0,
    )
    assert target_validation.passed is False
    assert target_validation.reason == "target_below_or_at_current_price_rejected"
