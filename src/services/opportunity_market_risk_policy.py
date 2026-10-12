# -*- coding: utf-8 -*-
"""Pure market-risk policy for the opportunity A/B/C experiment arms.

``evaluate_opportunity_market_policy`` never touches a database, the system
clock, the network or a model.  Every input (structured regime, frozen base
limits, CSI 300 closes and the trusted session calendar) arrives in the
snapshot, so the same snapshot always yields the same decision and hash.

Arm A is the existing regime rule (``_execution_policy_for_regime``).  Arm B
adds exactly one red-light exception (B1); arm C ignores market direction but
still requires a valid, current regime.  Green/yellow behaviour of B equals A.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from fractions import Fraction
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from src.services.daily_market_context import _execution_policy_for_regime

OPPORTUNITY_MARKET_POLICY_VERSION = "opportunity-market-risk-b1-v1"
SUPPORTED_POLICY_VERSIONS = frozenset({OPPORTUNITY_MARKET_POLICY_VERSION})
REGISTERED_ARMS = ("A", "B", "C")
BENCHMARK_INDEX_CODE = "000300.SH"
KNOWN_REGIMES = frozenset({"normal", "cautious", "risk_off", "unavailable"})

LONG_MA_SESSIONS = 60
SHORT_MA_SESSIONS = 20
SHORT_MA_LOOKBACK_SESSIONS = 5
SHOCK_WINDOW_SESSIONS = 60
SHOCK_SIGMA_MULTIPLE = 2
RED_LIGHT_FACTOR = Fraction(1, 4)
# 沪深 300 单日涨跌超过该幅度视为数据异常而非行情，不给红灯例外。
INDEX_RETURN_SANITY_LIMIT = Fraction(15, 100)
# 冲击判断需要 60 个此前收益率（61 个收盘）加当日收盘；MA60 与 MA20 回看都落在窗口内。
REQUIRED_WINDOW_SESSIONS = SHOCK_WINDOW_SESSIONS + 2


def evaluate_opportunity_market_policy(
    snapshot: Mapping[str, Any],
    *,
    arm: str,
    policy_version: str,
) -> Dict[str, Any]:
    """Return the entry decision one experiment arm consumes for one session.

    Unknown arms or policy versions are caller bugs and raise; data problems
    fail closed with explicit reasons instead of raising.
    """

    normalized_arm = str(arm or "").strip().upper()
    if normalized_arm not in REGISTERED_ARMS:
        raise ValueError(f"unregistered opportunity experiment arm: {arm!r}")
    if policy_version not in SUPPORTED_POLICY_VERSIONS:
        raise ValueError(f"unsupported opportunity market policy version: {policy_version!r}")

    snapshot = dict(snapshot or {})
    as_of = _coerce_date(snapshot.get("as_of"))
    base = _base_limits(snapshot.get("base_limits"))
    regime, regime_reasons = _current_regime(snapshot.get("regime"), as_of)
    decision: Dict[str, Any] = {
        "policy_version": policy_version,
        "arm": normalized_arm,
        "as_of": as_of.isoformat() if as_of else None,
        "regime": regime,
        "entry_allowed": False,
        "allocation_multiplier": 0.0,
        "max_symbol_exposure_pct": 0.0,
        "max_total_exposure_pct": 0.0,
        "reasons": [],
        "exception": None,
        "trend": None,
        "diagnostics": _passthrough_diagnostics(snapshot.get("diagnostics")),
        "input_hash": _input_hash(snapshot, arm=normalized_arm, policy_version=policy_version),
    }
    if as_of is None:
        decision["reasons"] = ["policy_as_of_invalid"]
        return decision
    if base is None:
        decision["reasons"] = ["base_limits_invalid"]
        return decision
    if regime_reasons:
        decision["reasons"] = regime_reasons
        return decision

    base_policy = _execution_policy_for_regime(regime)
    if normalized_arm == "C":
        # 诊断对照：不按大盘方向/波动缩量，但仍要求当期有效的大盘状态。
        return _granted(decision, base, Fraction(1), reasons=["diagnostic_arm_ignores_market_direction"])
    if base_policy["entry_allowed"]:
        return _granted(
            decision,
            base,
            Fraction(str(base_policy["allocation_multiplier"])),
            reasons=list(base_policy["reasons"]),
        )
    decision["reasons"] = list(base_policy["reasons"])
    if normalized_arm == "A" or regime != "risk_off":
        return decision

    trend = _evaluate_red_light_trend(snapshot.get("index"), as_of)
    decision["trend"] = trend
    if not trend["eligible"]:
        decision["reasons"] = [*decision["reasons"], *trend["reasons"]]
        return decision
    decision["exception"] = "red_light_trial"
    return _granted(
        decision,
        base,
        RED_LIGHT_FACTOR,
        reasons=["market_context_risk_off", "red_light_trial_medium_trend_intact"],
        cap_factor=RED_LIGHT_FACTOR,
    )


def _granted(
    decision: Dict[str, Any],
    base: Tuple[Fraction, Fraction],
    multiplier: Fraction,
    *,
    reasons: List[str],
    cap_factor: Fraction = Fraction(1),
) -> Dict[str, Any]:
    symbol_cap, total_cap = base
    decision.update(
        entry_allowed=multiplier > 0,
        allocation_multiplier=float(multiplier),
        max_symbol_exposure_pct=float(symbol_cap * cap_factor),
        max_total_exposure_pct=float(total_cap * cap_factor),
        reasons=reasons,
    )
    return decision


def _evaluate_red_light_trend(index: Any, as_of: date) -> Dict[str, Any]:
    """Check the three B1 conditions on a gap-free CSI 300 window ending at ``as_of``."""

    result: Dict[str, Any] = {"eligible": False, "reasons": [], "metrics": {}}
    index = index if isinstance(index, Mapping) else {}
    if str(index.get("code") or "") != BENCHMARK_INDEX_CODE:
        result["reasons"] = ["benchmark_code_mismatch"]
        return result
    closes, reason = _aligned_closes(index.get("bars"), index.get("sessions"), as_of)
    if reason:
        result["reasons"] = [reason]
        return result

    today = closes[-1]
    long_ma = sum(closes[-LONG_MA_SESSIONS:]) / LONG_MA_SESSIONS
    short_ma = sum(closes[-SHORT_MA_SESSIONS:]) / SHORT_MA_SESSIONS
    lagged = closes[:-SHORT_MA_LOOKBACK_SESSIONS]
    short_ma_lagged = sum(lagged[-SHORT_MA_SESSIONS:]) / SHORT_MA_SESSIONS
    returns = [closes[i] / closes[i - 1] - 1 for i in range(1, len(closes))]
    if any(abs(item) > INDEX_RETURN_SANITY_LIMIT for item in returns):
        result["reasons"] = ["index_return_out_of_range"]
        return result
    today_return = returns[-1]
    prior = returns[-SHOCK_WINDOW_SESSIONS - 1:-1]
    mean = sum(prior) / len(prior)
    # 总体方差（除以 N），不含当日。
    variance = sum((item - mean) ** 2 for item in prior) / len(prior)
    result["metrics"] = {
        "close": float(today),
        "ma60": float(long_ma),
        "ma20": float(short_ma),
        "ma20_5_sessions_ago": float(short_ma_lagged),
        "return": float(today_return),
        "prior_60_return_population_std": math.sqrt(float(variance)),
        "shock_threshold": -SHOCK_SIGMA_MULTIPLE * math.sqrt(float(variance)),
        "window_sessions": len(closes),
    }
    if variance == 0:
        result["reasons"] = ["index_volatility_zero"]
        return result
    reasons: List[str] = []
    if today < long_ma:
        reasons.append("index_below_ma60")
    if short_ma < short_ma_lagged:
        reasons.append("index_ma20_declining")
    # r >= -2σ  <=>  r >= 0 或 r² <= 4σ²；用精确分数比较，边界等号不受浮点误差影响。
    if today_return < 0 and today_return ** 2 > (SHOCK_SIGMA_MULTIPLE ** 2) * variance:
        reasons.append("index_extreme_shock")
    result["reasons"] = reasons
    result["eligible"] = not reasons
    return result


def _aligned_closes(bars: Any, sessions: Any, as_of: date) -> Tuple[List[Fraction], Optional[str]]:
    """Return the last closes only if they match the trusted calendar exactly."""

    if not isinstance(bars, Sequence) or isinstance(bars, (str, bytes)):
        return [], "index_window_missing"
    if not isinstance(sessions, Sequence) or isinstance(sessions, (str, bytes)):
        return [], "index_calendar_missing"
    session_dates = [_coerce_date(item) for item in sessions]
    if any(item is None for item in session_dates) or session_dates != sorted(set(session_dates)):
        return [], "index_calendar_invalid"
    session_dates = [item for item in session_dates if item <= as_of]
    if not session_dates or session_dates[-1] != as_of:
        return [], "index_calendar_not_as_of"
    if len(session_dates) < REQUIRED_WINDOW_SESSIONS:
        return [], "index_window_insufficient"
    by_date: Dict[date, Fraction] = {}
    for bar in bars:
        if not isinstance(bar, Mapping):
            return [], "index_bar_invalid"
        bar_date = _coerce_date(bar.get("date"))
        if bar_date is None:
            return [], "index_bar_invalid"
        if bar_date > as_of:
            return [], "index_future_bar"
        if bar_date in by_date:
            return [], "index_duplicate_bar"
        close = _positive_fraction(bar.get("close"))
        if close is None:
            return [], "index_close_invalid"
        by_date[bar_date] = close
    window = session_dates[-REQUIRED_WINDOW_SESSIONS:]
    if as_of not in by_date:
        return [], "index_bar_not_as_of"
    if any(day not in by_date for day in window):
        return [], "index_window_gap"
    window_set = set(window)
    if any(window[0] <= day <= as_of and day not in window_set for day in by_date):
        return [], "index_bar_off_calendar"
    return [by_date[day] for day in window], None


def _current_regime(value: Any, as_of: Optional[date]) -> Tuple[str, List[str]]:
    regime = value if isinstance(value, Mapping) else {}
    name = str(regime.get("regime") or "unavailable").strip().lower()
    if name not in KNOWN_REGIMES:
        return "unavailable", ["market_context_unavailable"]
    if name == "unavailable":
        return name, ["market_context_unavailable"]
    if regime.get("valid") is not True:
        return name, ["market_context_invalid"]
    session = _coerce_date(regime.get("session_date"))
    if as_of is None or session != as_of:
        return name, ["market_context_stale"]
    return name, []


def _base_limits(value: Any) -> Optional[Tuple[Fraction, Fraction]]:
    limits = value if isinstance(value, Mapping) else {}
    symbol_cap = _positive_fraction(limits.get("max_symbol_exposure_pct"))
    total_cap = _positive_fraction(limits.get("max_total_exposure_pct"))
    if symbol_cap is None or total_cap is None or symbol_cap > 100 or total_cap > 100:
        return None
    return symbol_cap, total_cap


def _positive_fraction(value: Any) -> Optional[Fraction]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return Fraction(number)


def _coerce_date(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError:
        return None


def _passthrough_diagnostics(value: Any) -> Dict[str, Any]:
    # 5 日广度与 20/60 日波动比仅作诊断，缺失保持缺失，不参与授权。
    if not isinstance(value, Mapping):
        return {}
    return {key: value[key] for key in ("breadth_5d", "volatility_ratio_20_60") if key in value}


def _input_hash(snapshot: Mapping[str, Any], *, arm: str, policy_version: str) -> str:
    payload = {"snapshot": snapshot, "arm": arm, "policy_version": policy_version}
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
