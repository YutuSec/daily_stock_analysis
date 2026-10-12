# -*- coding: utf-8 -*-
"""Pure B1 market-risk policy for the opportunity A/B/C experiment."""

from datetime import date, timedelta

import pytest

from src.services.opportunity_market_risk_policy import (
    OPPORTUNITY_MARKET_POLICY_VERSION,
    evaluate_opportunity_market_policy,
)

VERSION = OPPORTUNITY_MARKET_POLICY_VERSION
BASE_LIMITS = {"max_symbol_exposure_pct": 20.0, "max_total_exposure_pct": 80.0}


def _sessions(count, *, end=date(2026, 9, 25)):
    days = []
    cursor = end
    while len(days) < count:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor -= timedelta(days=1)
    return list(reversed(days))


def _snapshot(closes, *, regime="risk_off", regime_session=None, code="000300.SH", base=None, sessions=None):
    sessions = sessions or _sessions(len(closes))
    as_of = sessions[-1]
    return {
        "as_of": as_of.isoformat(),
        "regime": {
            "regime": regime,
            "session_date": (regime_session or as_of).isoformat(),
            "valid": True,
        },
        "base_limits": dict(base or BASE_LIMITS),
        "index": {
            "code": code,
            "sessions": [day.isoformat() for day in sessions],
            "bars": [{"date": day.isoformat(), "close": close} for day, close in zip(sessions, closes)],
        },
    }


def _alternating(count, *, low=100.0, high=102.0, end_high=True):
    closes = [high if (count - 1 - i) % 2 == 0 else low for i in range(count)]
    return closes if end_high else [low if value == high else high for value in closes]


def _evaluate(snapshot, arm="B"):
    return evaluate_opportunity_market_policy(snapshot, arm=arm, policy_version=VERSION)


def test_red_light_with_intact_medium_trend_grants_quarter_budget_and_quarter_caps():
    decision = _evaluate(_snapshot(_alternating(62)))

    assert decision["entry_allowed"] is True
    assert decision["exception"] == "red_light_trial"
    assert decision["allocation_multiplier"] == 0.25
    assert decision["max_symbol_exposure_pct"] == 5.0
    assert decision["max_total_exposure_pct"] == 20.0
    assert decision["policy_version"] == VERSION
    assert decision["as_of"] == "2026-09-25"
    assert len(decision["input_hash"]) == 64


def test_red_light_caps_scale_the_frozen_base_limits_not_fixed_percentages():
    decision = _evaluate(_snapshot(
        _alternating(62),
        base={"max_symbol_exposure_pct": 10.0, "max_total_exposure_pct": 60.0},
    ))

    assert decision["max_symbol_exposure_pct"] == 2.5
    assert decision["max_total_exposure_pct"] == 15.0


def test_arm_a_keeps_existing_red_light_block_even_when_trend_is_intact():
    decision = _evaluate(_snapshot(_alternating(62)), arm="A")

    assert decision["entry_allowed"] is False
    assert decision["allocation_multiplier"] == 0.0
    assert decision["reasons"] == ["market_context_risk_off"]
    assert decision["exception"] is None


def test_boundary_equalities_still_grant_the_exception():
    # 纯交替序列：MA20 与 5 日前 MA20 完全相等。
    equal_ma20 = _evaluate(_snapshot(_alternating(62)))
    metrics = equal_ma20["trend"]["metrics"]
    assert metrics["ma20"] == metrics["ma20_5_sessions_ago"]
    assert equal_ma20["entry_allowed"] is True

    # 收盘价恰好等于 MA60：窗口内其余 59 个收盘均值为 101，当日收 101。
    closes = [100.0 if i % 2 == 0 else 102.0 for i in range(60)] + [101.0, 101.0]
    at_ma60 = _evaluate(_snapshot(closes))
    metrics = at_ma60["trend"]["metrics"]
    assert metrics["close"] == metrics["ma60"] == 101.0
    assert at_ma60["trend"]["reasons"] == []
    assert at_ma60["entry_allowed"] is True


def test_shock_boundary_is_inclusive_at_exactly_minus_two_sigma():
    # 此前 60 个收益率在 +1/64 与 -1/65 间交替，总体标准差恰为 129/8320；
    # 4160 -> 4031 的当日收益率恰为 -2σ。
    prior = [4160.0 if i % 2 == 0 else 4225.0 for i in range(61)]
    exact = _evaluate(_snapshot(prior + [4031.0]))
    assert "index_extreme_shock" not in exact["trend"]["reasons"]

    beyond = _evaluate(_snapshot(prior + [4030.0]))
    assert "index_extreme_shock" in beyond["trend"]["reasons"]
    assert beyond["entry_allowed"] is False


@pytest.mark.parametrize(
    "closes, reason",
    [
        (_alternating(62, end_high=False), "index_below_ma60"),
        ([100.0 + i for i in range(61)] + [158.0], "index_extreme_shock"),
    ],
)
def test_red_light_with_broken_trend_or_shock_stays_closed_but_keeps_reason(closes, reason):
    decision = _evaluate(_snapshot(closes))

    assert decision["entry_allowed"] is False
    assert decision["allocation_multiplier"] == 0.0
    assert decision["trend"]["reasons"] == [reason]
    assert decision["reasons"] == ["market_context_risk_off", reason]


def test_declining_ma20_blocks_the_exception():
    closes = _alternating(62)
    closes[-3] = 101.0

    decision = _evaluate(_snapshot(closes))

    assert decision["entry_allowed"] is False
    assert decision["trend"]["reasons"] == ["index_ma20_declining"]


def test_zero_volatility_window_never_grants_the_exception():
    decision = _evaluate(_snapshot([100.0] * 62))

    assert decision["entry_allowed"] is False
    assert decision["trend"]["reasons"] == ["index_volatility_zero"]


@pytest.mark.parametrize(
    "mutate, reason",
    [
        (lambda snap: snap["index"].update(code="000001.SH"), "benchmark_code_mismatch"),
        (lambda snap: snap["index"]["bars"].pop(30), "index_window_gap"),
        (
            lambda snap: snap["index"]["bars"].append({"date": "2026-09-28", "close": 101.0}),
            "index_future_bar",
        ),
        (lambda snap: snap["index"]["bars"].pop(), "index_bar_not_as_of"),
        (lambda snap: snap["index"]["bars"][10].update(close=float("nan")), "index_close_invalid"),
        (lambda snap: snap["index"]["bars"].append(dict(snap["index"]["bars"][5])), "index_duplicate_bar"),
        (lambda snap: snap["index"]["bars"][40].update(close=300.0), "index_return_out_of_range"),
        (
            lambda snap: snap["index"]["bars"].append({"date": "2026-09-13", "close": 101.0}),
            "index_bar_off_calendar",
        ),
        (lambda snap: snap["index"].update(sessions=snap["index"]["sessions"][:-1]), "index_calendar_not_as_of"),
        (lambda snap: snap["index"].update(sessions=snap["index"]["sessions"][2:]), "index_window_insufficient"),
    ],
)
def test_untrusted_index_windows_fail_closed(mutate, reason):
    snapshot = _snapshot(_alternating(62))
    mutate(snapshot)

    decision = _evaluate(snapshot)

    assert decision["entry_allowed"] is False
    assert decision["allocation_multiplier"] == 0.0
    assert decision["trend"]["reasons"] == [reason]


@pytest.mark.parametrize("arm", ["A", "B", "C"])
def test_stale_or_invalid_or_unavailable_market_context_blocks_every_arm(arm):
    stale = _snapshot(_alternating(62), regime_session=date(2026, 9, 24))
    invalid = _snapshot(_alternating(62))
    invalid["regime"]["valid"] = False
    unavailable = _snapshot(_alternating(62), regime="unavailable")
    unknown = _snapshot(_alternating(62), regime="bullish")

    reasons = [_evaluate(item, arm)["reasons"] for item in (stale, invalid, unavailable, unknown)]

    assert reasons == [
        ["market_context_stale"],
        ["market_context_invalid"],
        ["market_context_unavailable"],
        ["market_context_unavailable"],
    ]
    for item in (stale, invalid, unavailable, unknown):
        assert _evaluate(item, arm)["entry_allowed"] is False


@pytest.mark.parametrize("regime", ["normal", "cautious"])
def test_green_and_yellow_are_identical_for_a_and_b(regime):
    snapshot = _snapshot(_alternating(62, end_high=False), regime=regime)

    decision_a = _evaluate(snapshot, "A")
    decision_b = _evaluate(snapshot, "B")

    comparable = ("entry_allowed", "allocation_multiplier", "max_symbol_exposure_pct", "max_total_exposure_pct", "reasons", "exception")
    assert {key: decision_a[key] for key in comparable} == {key: decision_b[key] for key in comparable}
    assert decision_b["allocation_multiplier"] == (1.0 if regime == "normal" else 0.5)
    assert decision_b["max_symbol_exposure_pct"] == 20.0
    assert decision_b["trend"] is None


def test_diagnostic_arm_c_ignores_direction_but_not_data_validity():
    decision = _evaluate(_snapshot(_alternating(62, end_high=False)), "C")

    assert decision["entry_allowed"] is True
    assert decision["allocation_multiplier"] == 1.0
    assert decision["max_symbol_exposure_pct"] == 20.0
    assert decision["max_total_exposure_pct"] == 80.0


def test_missing_or_invalid_base_limits_fail_closed():
    snapshot = _snapshot(_alternating(62), regime="normal")
    snapshot["base_limits"] = {"max_symbol_exposure_pct": 0, "max_total_exposure_pct": 80}

    decision = _evaluate(snapshot, "C")

    assert decision["entry_allowed"] is False
    assert decision["reasons"] == ["base_limits_invalid"]


@pytest.mark.parametrize("arm", ["D", "", "primary", None])
def test_unregistered_arm_is_rejected(arm):
    with pytest.raises(ValueError):
        evaluate_opportunity_market_policy(_snapshot(_alternating(62)), arm=arm, policy_version=VERSION)


def test_unknown_policy_version_is_rejected():
    with pytest.raises(ValueError):
        evaluate_opportunity_market_policy(_snapshot(_alternating(62)), arm="B", policy_version="b1-v0")


def test_same_snapshot_gives_same_hash_and_diagnostics_never_change_authorization():
    snapshot = _snapshot(_alternating(62))
    with_diagnostics = _snapshot(_alternating(62))
    with_diagnostics["diagnostics"] = {"breadth_5d": 0.1, "volatility_ratio_20_60": 9.0, "ignored": 1}

    first = _evaluate(snapshot)
    second = _evaluate(snapshot)
    diagnosed = _evaluate(with_diagnostics)

    assert first == second
    assert diagnosed["entry_allowed"] is True
    assert diagnosed["diagnostics"] == {"breadth_5d": 0.1, "volatility_ratio_20_60": 9.0}
    assert diagnosed["input_hash"] != first["input_hash"]
