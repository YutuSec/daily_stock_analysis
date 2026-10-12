# -*- coding: utf-8 -*-
"""Decision action taxonomy helpers for Issue #1390 P0.

This module is deliberately separate from ``src.agent.protocols``:
``DecisionAction`` is the new eight-state display taxonomy, while
``decision_type`` remains the existing buy/hold/sell statistics contract.
"""

from __future__ import annotations

import math
import re
from typing import Any, Dict, Literal, Mapping, Optional, TypedDict, get_args

from src.report_language import localize_operation_advice, normalize_report_language
from src.schemas.decision_scale import (
    action_for_score,
    extract_decision_guardrail_reason,
    normalize_score,
    score_action_conflicts_without_guardrail,
)

DecisionAction = Literal["buy", "add", "hold", "reduce", "sell", "watch", "avoid", "alert"]


class DecisionActionFields(TypedDict):
    action: Optional[DecisionAction]
    action_label: Optional[str]


class PositionAwareActionFields(DecisionActionFields):
    """One account-effective action plus auditable resolution metadata."""

    holding_state: str
    resolution_source: str
    base_action: Optional[DecisionAction]


_ACTION_VALUES = set(get_args(DecisionAction))
_NON_STOCK_REPORT_TYPES = {"market_review"}

_ACTION_LABELS: Dict[str, Dict[str, str]] = {
    "buy": {"zh": "买入", "en": "Buy", "ko": "매수"},
    "add": {"zh": "加仓", "en": "Add", "ko": "추가 매수"},
    "hold": {"zh": "持有", "en": "Hold", "ko": "보유"},
    "reduce": {"zh": "减仓", "en": "Reduce", "ko": "비중축소"},
    "sell": {"zh": "卖出", "en": "Sell", "ko": "매도"},
    "watch": {"zh": "观望", "en": "Watch", "ko": "관망"},
    "avoid": {"zh": "回避", "en": "Avoid", "ko": "회피"},
    "alert": {"zh": "预警", "en": "Alert", "ko": "경고"},
}

_EXPLICIT_ALIASES: Dict[str, DecisionAction] = {
    "strong buy": "buy",
    "accumulate": "add",
    "trim": "reduce",
    "strong sell": "sell",
    "wait": "watch",
}

_ACTION_PHRASES: Dict[DecisionAction, tuple[str, ...]] = {
    "avoid": (
        "不建议买入",
        "避免买入",
        "do not buy",
        "don't buy",
        "dont buy",
        "回避",
        "规避",
        "avoid",
    ),
    "alert": (
        "风险预警",
        "触发告警",
        "risk alert",
        "警惕",
        "alert",
    ),
    "buy": (
        "强烈买入",
        "strong_buy",
        "strong buy",
        "买入",
        "布局",
        "建仓",
        "buy",
    ),
    "add": (
        "加仓",
        "增持",
        "accumulate",
        "add",
    ),
    "hold": (
        "持有观察",
        "洗盘观察",
        "持有",
        "hold",
    ),
    "watch": (
        "观望",
        "等待",
        "wait",
        "watch",
    ),
    "reduce": (
        "减仓",
        "trim",
        "reduce",
    ),
    "sell": (
        "强烈卖出",
        "strong_sell",
        "strong sell",
        "卖出",
        "清仓",
        "sell",
    ),
}

_NEGATED_ACTION_PHRASES: Dict[DecisionAction, tuple[str, ...]] = {
    "avoid": (
        "暂不买入",
        "不要买入",
        "不宜买入",
        "先不买入",
        "不建议建仓",
        "暂不建仓",
        "不要建仓",
        "不宜建仓",
        "先不建仓",
        "无需建仓",
        "无须建仓",
        "不建议布局",
        "暂不布局",
        "不要布局",
        "不宜布局",
        "先不布局",
        "无需布局",
        "无须布局",
        "无需买入",
        "无须买入",
        "not buy",
        "do not buy",
        "don't buy",
        "dont buy",
        "no buy",
        "no need to buy",
        "need not buy",
        "cannot buy",
        "can't buy",
        "cant buy",
    ),
    "hold": (
        "不建议加仓",
        "无需加仓",
        "不要加仓",
        "不宜加仓",
        "暂不加仓",
        "无须加仓",
        "不建议增持",
        "无需增持",
        "不要增持",
        "不宜增持",
        "暂不增持",
        "无须增持",
        "不建议卖出",
        "无需卖出",
        "不要卖出",
        "不宜卖出",
        "暂不卖出",
        "无须卖出",
        "不建议减仓",
        "无需减仓",
        "不要减仓",
        "不宜减仓",
        "暂不减仓",
        "无须减仓",
        "不建议清仓",
        "无需清仓",
        "不要清仓",
        "不宜清仓",
        "暂不清仓",
        "无须清仓",
        "not add",
        "do not add",
        "don't add",
        "dont add",
        "no add",
        "no need to add",
        "need not add",
        "cannot add",
        "can't add",
        "cant add",
        "not accumulate",
        "do not accumulate",
        "don't accumulate",
        "dont accumulate",
        "no accumulate",
        "no need to accumulate",
        "need not accumulate",
        "cannot accumulate",
        "can't accumulate",
        "cant accumulate",
        "not sell",
        "do not sell",
        "don't sell",
        "dont sell",
        "no sell",
        "no need to sell",
        "need not sell",
        "cannot sell",
        "can't sell",
        "cant sell",
        "not reduce",
        "do not reduce",
        "don't reduce",
        "dont reduce",
        "no reduce",
        "no need to reduce",
        "need not reduce",
        "cannot reduce",
        "can't reduce",
        "cant reduce",
        "not trim",
        "do not trim",
        "don't trim",
        "dont trim",
        "no trim",
        "no need to trim",
        "need not trim",
        "cannot trim",
        "can't trim",
        "cant trim",
    ),
}

_GUARD_ACTIONS: tuple[DecisionAction, ...] = ("avoid", "alert")
_ENGLISH_NEGATED_ACTION_TERMS: Dict[DecisionAction, tuple[str, ...]] = {
    "avoid": ("buy",),
    "hold": ("add", "accumulate", "sell", "reduce", "trim"),
}
_ENGLISH_AVOIDED_HOLD_ACTION_TERMS = ("adding", "accumulating", "selling", "reducing", "trimming")
_ENGLISH_DEFERRED_ACTION_TERMS = ("buy", "add", "accumulate", "sell", "reduce", "trim")
_FINANCIAL_COMPOUND_SENTINEL = "financialcompound"

_SCORE_COMPATIBLE_ACTIONS: Dict[str, set[str]] = {
    "buy": {"buy", "add"},
    "watch": {"hold", "watch"},
    "reduce": {"reduce"},
    "sell": {"sell", "avoid", "alert"},
}


def decision_type_for_action(action: Any) -> str:
    """Map the canonical action to the legacy three-state statistics bucket."""

    normalized = normalize_decision_action(action)
    if normalized in {"buy", "add"}:
        return "buy"
    if normalized in {"reduce", "sell", "avoid", "alert"}:
        return "sell"
    return "hold"


def _as_mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _structured_guardrail(result: Any, raw_score: int) -> Dict[str, Any]:
    """Return a validated, auditable guardrail or an explicit non-applied record.

    Narrative text is deliberately insufficient.  A score/action override is
    accepted only when the producer supplied every audit field and the score
    delta is arithmetically consistent.
    """

    dashboard = _as_mapping(getattr(result, "dashboard", None))
    source = _as_mapping(getattr(result, "guardrail", None))
    if not source:
        source = _as_mapping(dashboard.get("decision_guardrail"))
    if not source:
        calibration = _as_mapping(dashboard.get("decision_score_calibration"))
        stability = _as_mapping(dashboard.get("decision_stability"))
        source = {
            "applied": stability.get("applied"),
            "reason_code": stability.get("reason_code") or calibration.get("reason_code"),
            "reason": stability.get("reason") or calibration.get("guardrail_reason"),
            "raw_score": calibration.get("raw_score"),
            "adjusted_score": calibration.get("adjusted_score"),
            "score_delta": calibration.get("score_delta") or stability.get("score_delta"),
        }

    if source.get("applied") is not True:
        return {"applied": False}

    reason_code = str(source.get("reason_code") or "").strip()
    reason = str(source.get("reason") or "").strip()
    declared_raw = normalize_score(source.get("raw_score"))
    adjusted = normalize_score(source.get("adjusted_score"))
    try:
        delta = int(source.get("score_delta"))
    except (TypeError, ValueError):
        delta = None
    valid = (
        bool(reason_code)
        and bool(reason)
        and declared_raw == raw_score
        and adjusted is not None
        and delta is not None
        and delta == adjusted - raw_score
    )
    if not valid:
        return {"applied": False, "invalid": True}
    return {
        "applied": True,
        "reason_code": reason_code,
        "reason": reason,
        "raw_score": raw_score,
        "adjusted_score": adjusted,
        "score_delta": delta,
    }


def _score_compatible_action(action: Any, score: int) -> Optional[DecisionAction]:
    normalized = normalize_decision_action(action)
    canonical = action_for_score(score)
    if normalized and normalized in _SCORE_COMPATIBLE_ACTIONS.get(str(canonical), set()):
        return normalized
    if canonical in _ACTION_VALUES:
        return canonical  # type: ignore[return-value]
    return None


def _positive_finite_price(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0 else None


def _normalize_position_actions(result: Any) -> Dict[str, Dict[str, Any]]:
    dashboard = _as_mapping(getattr(result, "dashboard", None))
    core = _as_mapping(dashboard.get("core_conclusion"))
    legacy = _as_mapping(core.get("position_advice"))
    supplied = _as_mapping(getattr(result, "position_actions", None))
    output: Dict[str, Dict[str, Any]] = {}
    aliases = {
        "empty": ("empty", "no_position", "no position", "none", "cash", "空仓", "无持仓", "未持仓"),
        "holding": ("holding", "has_position", "has position", "existing_position", "existing position", "持仓", "已有持仓"),
    }
    for state, legacy_key in (("empty", "no_position"), ("holding", "has_position")):
        block: Dict[str, Any] = {}
        for alias in aliases[state]:
            candidate = supplied.get(alias)
            if isinstance(candidate, Mapping):
                block = _as_mapping(candidate)
                break
        legacy_advice = legacy.get(legacy_key)
        if not legacy_advice:
            for alias in aliases[state]:
                legacy_advice = legacy.get(alias)
                if legacy_advice:
                    break
        advice = str(block.get("advice") or legacy_advice or "").strip()
        action = normalize_decision_action(block.get("action"))
        advice_action = normalize_decision_action(advice)
        if action and advice_action and advice_action != action:
            advice = localize_action_label(
                action, getattr(result, "report_language", "zh")
            ) or str(action)
        elif action and not advice:
            advice = localize_action_label(
                action, getattr(result, "report_language", "zh")
            ) or str(action)
        normalized: Dict[str, Any] = {}
        if action:
            normalized["action"] = action
        if advice:
            normalized["advice"] = advice
        if state == "holding":
            stop_loss = _positive_finite_price(block.get("stop_loss"))
            target_price = _positive_finite_price(block.get("target_price"))
            if stop_loss is not None:
                normalized["stop_loss"] = stop_loss
            if target_price is not None:
                normalized["target_price"] = target_price
        output[state] = normalized
    return output


def normalize_result_decision_contract(
    result: Any,
    *,
    explicit_action: Any = None,
    align_with_score: bool = True,
) -> Any:
    """Make score, action, labels and legacy fields one coherent contract.

    ``raw_score`` is the model score. ``adjusted_score`` may differ only under
    a complete structured guardrail.  Human-readable advice is always derived
    from the final action, so it cannot contradict persisted statistics.
    """

    dashboard = _as_mapping(getattr(result, "dashboard", None))
    calibration = _as_mapping(dashboard.get("decision_score_calibration"))
    raw_score = normalize_score(getattr(result, "raw_score", None))
    if raw_score is None:
        raw_score = normalize_score(calibration.get("raw_score"))
    if raw_score is None:
        raw_score = normalize_score(getattr(result, "sentiment_score", None))
    if raw_score is None:
        return result

    guardrail = _structured_guardrail(result, raw_score)
    adjusted_score = raw_score
    if guardrail.get("applied") is True:
        adjusted_score = int(guardrail["adjusted_score"])

    candidate = explicit_action
    if normalize_decision_action(candidate) is None:
        candidate = getattr(result, "action", None)
    # operation_advice is a legacy input only when no structured action exists.
    if normalize_decision_action(candidate) is None:
        candidate = getattr(result, "operation_advice", None)
    language = getattr(result, "report_language", "zh")
    normalized_position_actions = _normalize_position_actions(result)
    if align_with_score:
        final_action = _score_compatible_action(candidate, adjusted_score)
    else:
        final_action = normalize_decision_action(candidate)
        if final_action is None:
            score_action = action_for_score(adjusted_score)
            final_action = score_action if score_action in _ACTION_VALUES else None
    holding_state = normalize_holding_state(getattr(result, "holding_state", None))
    if holding_state in {"empty", "holding"}:
        resolved = resolve_position_aware_action_fields(
            holding_state=holding_state,
            position_actions=normalized_position_actions,
            fallback_action=final_action,
            report_language=language,
        )
        final_action = resolved.get("action")
    label = localize_action_label(final_action, language) if final_action else None

    result.raw_score = raw_score
    result.adjusted_score = adjusted_score
    result.sentiment_score = adjusted_score
    result.action = final_action
    result.action_label = label
    result.operation_advice = label or ""
    result.decision_type = decision_type_for_action(final_action)
    result.guardrail = guardrail
    result.position_actions = normalized_position_actions

    dashboard["sentiment_score"] = adjusted_score
    dashboard["action"] = final_action
    dashboard["action_label"] = label
    dashboard["operation_advice"] = result.operation_advice
    dashboard["decision_type"] = result.decision_type
    dashboard["decision_guardrail"] = guardrail
    dashboard["decision_score_calibration"] = {
        "raw_score": raw_score,
        "adjusted_score": adjusted_score,
        "final_action": final_action,
        "score_delta": adjusted_score - raw_score,
        **(
            {
                "guardrail_reason": guardrail["reason"],
                "reason_code": guardrail["reason_code"],
            }
            if guardrail.get("applied") is True
            else {}
        ),
    }
    result.dashboard = dashboard
    return result


def apply_position_aware_result_contract(
    result: Any,
    *,
    report_type: Any = None,
) -> Any:
    """Persist the account-effective action once report-time holdings are known."""

    if result is None:
        return result
    state = normalize_holding_state(getattr(result, "holding_state", None))
    if state == "unknown":
        return result
    if getattr(result, "success", True) is False:
        return result

    resolved = resolve_position_aware_action_fields(
        holding_state=state,
        position_actions=getattr(result, "position_actions", None),
        fallback_action=getattr(result, "action", None)
        or getattr(result, "action_label", None)
        or getattr(result, "operation_advice", None),
        report_language=getattr(result, "report_language", "zh"),
    )
    action = resolved.get("action")
    label = resolved.get("action_label")
    result.action = action
    result.action_label = label
    if label:
        result.operation_advice = label
    result.decision_type = decision_type_for_action(action)

    dashboard = _as_mapping(getattr(result, "dashboard", None))
    if dashboard:
        dashboard["action"] = action
        dashboard["action_label"] = label
        dashboard["operation_advice"] = result.operation_advice
        dashboard["decision_type"] = result.decision_type
        dashboard["position_action_resolution"] = {
            "holding_state": resolved.get("holding_state"),
            "source": resolved.get("resolution_source"),
            "base_action": resolved.get("base_action"),
        }
        result.dashboard = dashboard
    return result


def _normalize_key(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", " ").replace("-", " ")


def _mask_english_financial_compounds(text: str) -> str:
    text = re.sub(
        r"(?<![a-z0-9_])buy\s*back(?![a-z0-9_])",
        _FINANCIAL_COMPOUND_SENTINEL,
        text,
    )
    return re.sub(
        r"(?<![a-z0-9_])sell\s*off(?![a-z0-9_])",
        _FINANCIAL_COMPOUND_SENTINEL,
        text,
    )


def _word_or_substring_match(text: str, phrase: str) -> bool:
    if not text or not phrase:
        return False
    normalized_phrase = _normalize_key(phrase)
    if re.search(r"[a-z]", normalized_phrase):
        return bool(re.search(rf"(?<![a-z0-9_]){re.escape(normalized_phrase)}(?![a-z0-9_])", text))
    return normalized_phrase in text


def _english_negated_action_matches(text: str) -> set[DecisionAction]:
    matches: set[DecisionAction] = set()
    negation_prefix = (
        r"(?:not\s+(?:a\s+|an\s+|to\s+)?|"
        r"no\s+(?:need\s+to\s+)?|"
        r"need\s+not\s+|"
        r"cannot\s+|can't\s+|cant\s+|"
        r"do\s+not\s+|don't\s+|dont\s+)"
    )
    for action, terms in _ENGLISH_NEGATED_ACTION_TERMS.items():
        for term in terms:
            if re.search(rf"(?<![a-z0-9_]){negation_prefix}{re.escape(term)}(?![a-z0-9_])", text):
                matches.add(action)
    return matches


def _has_english_avoided_hold_action(text: str) -> bool:
    terms = "|".join(re.escape(term) for term in _ENGLISH_AVOIDED_HOLD_ACTION_TERMS)
    return bool(re.search(rf"(?<![a-z0-9_])avoid\s+(?:{terms})(?![a-z0-9_])", text))


def _has_english_deferred_action(text: str) -> bool:
    terms = "|".join(re.escape(term) for term in _ENGLISH_DEFERRED_ACTION_TERMS)
    if re.search(rf"(?<![a-z0-9_])wait(?:ing)?\s+to\s+(?:{terms})(?![a-z0-9_])", text):
        return True
    return bool(
        re.search(
            rf"(?<![a-z0-9_])waiting\s+(?:for|until)\b.*?(?<![a-z0-9_])(?:{terms})(?![a-z0-9_])",
            text,
        )
    )


def _explicit_action(value: Any) -> Optional[DecisionAction]:
    normalized = _normalize_key(value)
    if not normalized:
        return None
    if normalized in _ACTION_VALUES:
        return normalized  # type: ignore[return-value]
    return _EXPLICIT_ALIASES.get(normalized)


def normalize_decision_action(value: Any) -> Optional[DecisionAction]:
    """Return a unique eight-state action for explicit values or clear text.

    Unknown or ambiguous human-readable advice returns ``None`` rather than
    defaulting to a neutral action.
    """

    explicit = _explicit_action(value)
    if explicit:
        return explicit

    text = _mask_english_financial_compounds(_normalize_key(value))
    if not text:
        return None

    if _has_english_deferred_action(text):
        return None

    negated_matches: set[DecisionAction] = set()
    if _has_english_avoided_hold_action(text):
        negated_matches.add("hold")
    negated_matches.update(_english_negated_action_matches(text))
    for action, phrases in _NEGATED_ACTION_PHRASES.items():
        if any(_word_or_substring_match(text, phrase) for phrase in phrases):
            negated_matches.add(action)
    if len(negated_matches) == 1:
        return next(iter(negated_matches))
    if len(negated_matches) > 1:
        return None

    guard_matches: set[DecisionAction] = set()
    for action in _GUARD_ACTIONS:
        if any(_word_or_substring_match(text, phrase) for phrase in _ACTION_PHRASES[action]):
            guard_matches.add(action)
    if len(guard_matches) == 1:
        return next(iter(guard_matches))
    if len(guard_matches) > 1:
        return None

    matches: set[DecisionAction] = set()
    for action, phrases in _ACTION_PHRASES.items():
        if action in _GUARD_ACTIONS:
            continue
        if any(_word_or_substring_match(text, phrase) for phrase in phrases):
            matches.add(action)

    if len(matches) == 1:
        return next(iter(matches))
    if matches and matches <= {"hold", "watch"}:
        return "watch" if "watch" in matches else "hold"
    return None


def localize_action_label(action: Any, language: Optional[str] = "zh") -> Optional[str]:
    """Return a localized display label for a decision action."""

    normalized = _explicit_action(action)
    if not normalized:
        return None
    return _ACTION_LABELS[normalized][normalize_report_language(language)]


def build_action_fields(
    *,
    operation_advice: Any = None,
    explicit_action: Any = None,
    report_type: Any = None,
    report_language: Optional[str] = "zh",
    sentiment_score: Any = None,
    guardrail_reason: Any = None,
    align_with_score: bool = False,
) -> DecisionActionFields:
    """Build optional public action fields without mutating legacy contracts."""

    if str(report_type or "").strip().lower() in _NON_STOCK_REPORT_TYPES:
        return {"action": None, "action_label": None}

    action = normalize_decision_action(explicit_action)
    if action is None:
        advice_text = str(operation_advice or "").strip()
        if advice_text:
            action = normalize_decision_action(advice_text)

    if align_with_score and score_action_conflicts_without_guardrail(
        score=sentiment_score,
        action=action,
        guardrail_reason=guardrail_reason,
    ):
        score_action = action_for_score(sentiment_score)
        if score_action in _ACTION_VALUES:
            action = score_action  # type: ignore[assignment]

    return {
        "action": action,
        "action_label": localize_action_label(action, report_language) if action else None,
    }


def normalize_holding_state(value: Any) -> str:
    """Normalize only an explicitly frozen account state.

    Unknown or malformed values must never be interpreted as ownership.
    """

    normalized = str(value or "").strip().lower()
    return normalized if normalized in {"empty", "holding"} else "unknown"


def resolve_position_aware_action_fields(
    *,
    holding_state: Any,
    position_actions: Any,
    fallback_action: Any,
    report_language: Optional[str] = "zh",
) -> PositionAwareActionFields:
    """Resolve the effective action for the report-time account state.

    The model may provide separate ``empty`` and ``holding`` branches.  A
    branch action is operationally authoritative only for its matching frozen
    state.  Empty accounts fail closed: a missing or malformed empty branch
    can never inherit ``hold``, ``reduce`` or ``sell`` from a holder-oriented
    top-level recommendation.
    """

    state = normalize_holding_state(holding_state)
    base_action = normalize_decision_action(fallback_action)
    branches = _as_mapping(position_actions)
    branch = _as_mapping(branches.get(state)) if state in {"empty", "holding"} else {}
    branch_action = normalize_decision_action(branch.get("action"))
    if branch_action is None:
        branch_action = normalize_decision_action(branch.get("advice"))

    action = branch_action or base_action
    source = f"position_actions.{state}" if branch_action else "canonical_fallback"

    if state == "empty" and action in {"hold", "reduce", "sell"}:
        action = "watch" if action == "hold" else "avoid"
        source = "empty_state_fail_closed"

    return {
        "action": action,
        "action_label": localize_action_label(action, report_language) if action else None,
        "holding_state": state,
        "resolution_source": source,
        "base_action": base_action,
    }


def _result_guardrail_reason(result: Any) -> Optional[str]:
    return extract_decision_guardrail_reason(
        {
            "guardrail_reason": getattr(result, "guardrail_reason", None),
            "downgrade_reason": getattr(result, "downgrade_reason", None),
            "dashboard": getattr(result, "dashboard", None),
            "metadata": getattr(result, "metadata", None),
        }
    )


def display_action_fields(
    *,
    operation_advice: Any = None,
    explicit_action: Any = None,
    action_label: Any = None,
    report_type: Any = None,
    report_language: Optional[str] = "zh",
    sentiment_score: Any = None,
    guardrail_reason: Any = None,
    holding_state: Any = None,
    position_actions: Any = None,
) -> DecisionActionFields:
    """Resolve one canonical action for every public display surface."""

    action_source = explicit_action
    if normalize_decision_action(action_source) is None and str(action_label or "").strip():
        action_source = action_label
    fields = build_action_fields(
        operation_advice=operation_advice,
        explicit_action=action_source,
        report_type=report_type,
        report_language=report_language,
        sentiment_score=sentiment_score,
        guardrail_reason=guardrail_reason,
        align_with_score=True,
    )
    resolved = resolve_position_aware_action_fields(
        holding_state=holding_state,
        position_actions=position_actions,
        fallback_action=fields["action"],
        report_language=report_language,
    )
    return {"action": resolved["action"], "action_label": resolved["action_label"]}


def _display_result_kwargs(
    result: Any,
    *,
    report_language: Optional[str] = None,
    report_type: Any = None,
) -> dict[str, Any]:
    return {
        "operation_advice": getattr(result, "operation_advice", None),
        "explicit_action": getattr(result, "action", None),
        "action_label": getattr(result, "action_label", None),
        "report_type": report_type or getattr(result, "report_type", None),
        "report_language": report_language or getattr(result, "report_language", "zh"),
        "sentiment_score": getattr(result, "sentiment_score", None),
        "guardrail_reason": _result_guardrail_reason(result),
        "holding_state": getattr(result, "holding_state", None),
        "position_actions": getattr(result, "position_actions", None),
    }


def display_action_fields_for_result(
    result: Any,
    *,
    report_language: Optional[str] = None,
    report_type: Any = None,
) -> DecisionActionFields:
    return display_action_fields(
        **_display_result_kwargs(result, report_language=report_language, report_type=report_type)
    )


def display_operation_advice_for_result(
    result: Any,
    *,
    report_language: Optional[str] = None,
    report_type: Any = None,
) -> str:
    """Return the same localized action label used by Web/API display fields."""

    fields = display_action_fields_for_result(
        result,
        report_language=report_language,
        report_type=report_type,
    )
    if fields["action_label"]:
        return fields["action_label"]
    language = report_language or getattr(result, "report_language", "zh")
    return localize_operation_advice(getattr(result, "operation_advice", None), language)


def display_decision_type_for_result(
    result: Any,
    *,
    report_language: Optional[str] = None,
    report_type: Any = None,
) -> str:
    """Map the displayed eight-state action to the legacy three summary buckets."""

    action = display_action_fields_for_result(
        result,
        report_language=report_language,
        report_type=report_type,
    )["action"]
    if action in {"buy", "add"}:
        return "buy"
    if action in {"reduce", "sell"}:
        return "sell"
    if action is not None:
        return "hold"
    legacy = str(getattr(result, "decision_type", "") or "").strip().lower()
    if legacy in {"buy", "hold", "sell"}:
        return legacy
    return "hold"
