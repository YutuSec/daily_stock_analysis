# -*- coding: utf-8 -*-
"""Pure policy helpers for paper-trading strategy contracts."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence


SUPPORTED_SETUP_TYPES = frozenset({"breakout", "pullback", "trend_continuation"})
POLICY_VERSION = "paper-strategy-policy-v1"


STYLE_GEOMETRY_DEFAULTS: Dict[str, Dict[str, float]] = {
    "short_swing": {
        "min_stop_atr_multiple": 0.75,
        "min_stop_pct": 0.012,
        "max_entry_above_support_pct": 0.08,
    },
    "position": {
        "min_stop_atr_multiple": 1.0,
        "min_stop_pct": 0.018,
        "max_entry_above_support_pct": 0.12,
    },
    "trend": {
        "min_stop_atr_multiple": 1.25,
        "min_stop_pct": 0.025,
        "max_entry_above_support_pct": 0.18,
    },
}


def safe_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def normalize_setup_type(value: Any) -> Optional[str]:
    key = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "break": "breakout",
        "breakthrough": "breakout",
        "突破": "breakout",
        "pull_back": "pullback",
        "回踩": "pullback",
        "回调": "pullback",
        "trend": "trend_continuation",
        "continuation": "trend_continuation",
        "趋势延续": "trend_continuation",
    }
    normalized = aliases.get(key, key)
    return normalized if normalized in SUPPORTED_SETUP_TYPES else None


def extract_explicit_setup(raw_report: Mapping[str, Any]) -> Dict[str, Any]:
    """Return only an explicitly structured setup declaration."""

    candidates = []
    paper = raw_report.get("paper_trading") if isinstance(raw_report, Mapping) else None
    if isinstance(paper, Mapping):
        candidates.extend([paper.get("setup"), paper.get("strategy_setup"), paper])
    dashboard = raw_report.get("dashboard") if isinstance(raw_report, Mapping) else None
    if isinstance(dashboard, Mapping):
        battle = dashboard.get("battle_plan")
        if isinstance(battle, Mapping):
            candidates.extend([battle.get("setup"), battle.get("strategy_setup")])
            sniper = battle.get("sniper_points")
            if isinstance(sniper, Mapping):
                candidates.extend([sniper.get("setup"), sniper.get("strategy_setup")])
    for candidate in candidates:
        if isinstance(candidate, Mapping):
            raw_type = (
                candidate.get("setup_type")
                or candidate.get("type")
                or candidate.get("entry_setup")
                or candidate.get("strategy_setup_type")
            )
            setup_type = normalize_setup_type(raw_type)
            if setup_type:
                return {
                    "setup_type": setup_type,
                    "source": "explicit_structured_report",
                    "raw": dict(candidate),
                    "policy_version": POLICY_VERSION,
                }
            if raw_type:
                return {
                    "setup_type": None,
                    "source": "explicit_structured_report",
                    "raw": dict(candidate),
                    "policy_version": POLICY_VERSION,
                    "error": "unsupported_strategy_setup_type",
                }
    return {
        "setup_type": None,
        "source": "missing",
        "policy_version": POLICY_VERSION,
        "error": "missing_strategy_setup_type",
    }


def infer_legacy_setup_from_rules(rules: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Conservative legacy inference from structured rule geometry only."""

    typed = [rule for rule in rules if isinstance(rule, Mapping)]
    has_prior_high = any(rule.get("type") == "close_above_prior_high" for rule in typed)
    has_sma = any(rule.get("type") == "close_above_prior_sma" for rule in typed)
    has_bounded_price = any(rule.get("type") == "close_between" for rule in typed)
    if has_prior_high:
        setup_type = "breakout"
    elif has_sma and has_bounded_price:
        setup_type = "trend_continuation"
    elif has_bounded_price:
        setup_type = "pullback"
    else:
        return {
            "setup_type": None,
            "source": "legacy_structured_rules",
            "policy_version": POLICY_VERSION,
            "error": "unsupported_strategy_setup_type",
        }
    return {
        "setup_type": setup_type,
        "source": "legacy_structured_rules",
        "policy_version": POLICY_VERSION,
        "inference_basis": "structured_price_rule_geometry",
    }


def setup_contract(raw_report: Mapping[str, Any], rules: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    explicit = extract_explicit_setup(raw_report)
    if explicit.get("setup_type") or explicit.get("source") == "explicit_structured_report":
        return explicit
    return infer_legacy_setup_from_rules(rules)


def setup_factor_policy(setup_type: Optional[str]) -> Dict[str, Any]:
    if setup_type == "breakout":
        return {
            "required_domains": ["momentum", "volume", "breakout"],
            "optional_domains": ["trend"],
            "description": "breakout requires price through structured resistance plus confirmation factors",
        }
    if setup_type == "pullback":
        return {
            "required_domains": ["trend"],
            "alternative_confirmations": [["support_confirmation", "shrinking_volume"]],
            "optional_domains": ["momentum"],
            "description": "pullback may be confirmed by support hold with contracted/shrinking volume",
        }
    if setup_type == "trend_continuation":
        return {
            "required_domains": ["trend", "momentum"],
            "optional_domains": ["volume", "breakout"],
            "description": "trend continuation requires trend alignment and non-exhausted momentum",
        }
    return {
        "required_domains": [],
        "optional_domains": [],
        "error": "unsupported_strategy_setup_type",
    }


def horizon_duration_sessions(horizon: Any, *, default_sessions: int) -> int:
    key = str(horizon or "").strip().lower()
    mapping = {
        "intraday": 1,
        "1d": 1,
        "3d": 3,
        "5d": 5,
        "10d": 10,
        "swing": 20,
        "long": 60,
    }
    return int(mapping.get(key, default_sessions))


def plan_holding_sessions_for_horizon(
    horizon: Any,
    style_settings: Mapping[str, Any],
    *,
    horizon_source: Any = None,
) -> Dict[str, Any]:
    default_sessions = int(style_settings.get("default_holding_sessions") or 10)
    max_sessions = int(style_settings.get("max_holding_sessions") or default_sessions)
    normalized_horizon = str(horizon or "").strip().lower()
    normalized_source = str(horizon_source or "").strip().lower()
    default_signal_horizon = str(style_settings.get("default_signal_horizon") or "").strip().lower()
    uses_style_default = normalized_source in {
        "paper_trading_style_default",
        "style_default",
        "trading_style_default",
    } or bool(default_signal_horizon and normalized_horizon == default_signal_horizon)
    requested = (
        default_sessions
        if uses_style_default
        else horizon_duration_sessions(horizon, default_sessions=default_sessions)
    )
    return {
        "horizon": normalized_horizon or None,
        "horizon_source": normalized_source or None,
        "requested_sessions": requested,
        "max_sessions": max_sessions,
        "applied_sessions": min(requested, max_sessions),
        "duration_source": "style_default_holding_sessions" if uses_style_default else "explicit_horizon",
        "policy_version": POLICY_VERSION,
    }


def _indicator_value(snapshot: Mapping[str, Any], keys: Sequence[str]) -> Optional[float]:
    for key in keys:
        value = safe_float(snapshot.get(key))
        if value is not None:
            return value
    for container_key in ("latest", "volatility", "moving_averages", "structure"):
        nested = snapshot.get(container_key)
        if not isinstance(nested, Mapping):
            continue
        for key in keys:
            value = safe_float(nested.get(key))
            if value is not None:
                return value
    nested = snapshot.get("latest") if isinstance(snapshot.get("latest"), Mapping) else None
    if nested:
        for key in keys:
            value = safe_float(nested.get(key))
            if value is not None:
                return value
    return None


def entry_geometry_guard(
    *,
    style: str,
    style_settings: Mapping[str, Any],
    indicator_snapshot: Optional[Mapping[str, Any]],
    entry_reference: Optional[float],
    stop_loss: Optional[float],
) -> Dict[str, Any]:
    thresholds = {
        **STYLE_GEOMETRY_DEFAULTS.get(style, STYLE_GEOMETRY_DEFAULTS["position"]),
        **(
            style_settings.get("geometry_guard")
            if isinstance(style_settings.get("geometry_guard"), Mapping)
            else {}
        ),
    }
    evidence: Dict[str, Any] = {
        "policy_version": POLICY_VERSION,
        "style": style,
        "thresholds": thresholds,
        "passed": True,
        "reasons": [],
    }
    entry = safe_float(entry_reference)
    stop = safe_float(stop_loss)
    if entry is None or stop is None:
        evidence["passed"] = False
        evidence["reasons"].append("missing_entry_or_stop_for_geometry_guard")
        return evidence
    risk_distance = float(entry) - float(stop)
    evidence["risk_distance"] = risk_distance
    if risk_distance <= 0:
        evidence["passed"] = False
        evidence["reasons"].append("stop_not_below_entry")
        return evidence
    min_pct_distance = float(entry) * float(thresholds["min_stop_pct"])
    evidence["min_pct_distance"] = min_pct_distance
    if risk_distance < min_pct_distance:
        evidence["passed"] = False
        evidence["reasons"].append("stop_within_minimum_price_noise")
    snapshot = indicator_snapshot if isinstance(indicator_snapshot, Mapping) else {}
    atr = _indicator_value(snapshot, ("atr", "atr14", "atr_14", "atr20", "atr_20"))
    evidence["atr"] = atr
    if atr is not None:
        min_atr_distance = float(atr) * float(thresholds["min_stop_atr_multiple"])
        evidence["min_atr_distance"] = min_atr_distance
        if risk_distance < min_atr_distance:
            evidence["passed"] = False
            evidence["reasons"].append("stop_within_atr_noise")
    support = _indicator_value(snapshot, ("support", "support_price", "nearest_support"))
    evidence["support"] = support
    if support is not None:
        max_above_support = float(entry) * float(thresholds["max_entry_above_support_pct"])
        evidence["max_entry_above_support"] = max_above_support
        if abs(float(entry) - float(support)) > max_above_support:
            evidence["passed"] = False
            evidence["reasons"].append("entry_structure_too_far_from_support")
    return evidence


@dataclass(frozen=True)
class RiskRevisionValidation:
    passed: bool
    reason: str
    evidence: Dict[str, Any]


def validate_risk_revision_geometry(
    *,
    current_price: Optional[float],
    old_stop_loss: Optional[float],
    proposed_stop_loss: Optional[float],
    proposed_target_price: Optional[float],
) -> RiskRevisionValidation:
    evidence = {
        "policy_version": POLICY_VERSION,
        "current_price": current_price,
        "old_stop_loss": old_stop_loss,
        "proposed_stop_loss": proposed_stop_loss,
        "proposed_target_price": proposed_target_price,
        "no_implicit_immediate_exit": True,
        "preserve_hard_existing_stop": True,
    }
    if proposed_stop_loss is None and proposed_target_price is None:
        return RiskRevisionValidation(False, "invalid_missing_risk_values", evidence)
    if old_stop_loss is not None and proposed_stop_loss is not None and proposed_stop_loss < old_stop_loss:
        return RiskRevisionValidation(False, "lower_stop_loss_rejected", evidence)
    if current_price is None:
        return RiskRevisionValidation(False, "missing_validated_reference_price_rejected", evidence)
    if current_price is not None:
        if proposed_stop_loss is not None and proposed_stop_loss >= current_price:
            return RiskRevisionValidation(False, "stop_above_or_at_current_price_rejected", evidence)
        if proposed_target_price is not None and proposed_target_price <= current_price:
            return RiskRevisionValidation(False, "target_below_or_at_current_price_rejected", evidence)
    return RiskRevisionValidation(True, "accepted", evidence)
