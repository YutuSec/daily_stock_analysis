# -*- coding: utf-8 -*-
"""Tests for Issue #1390 P0 decision action taxonomy helpers."""

import pytest

from src.schemas.decision_action import (
    build_action_fields,
    display_action_fields_for_result,
    display_decision_type_for_result,
    display_operation_advice_for_result,
    localize_action_label,
    normalize_decision_action,
)
from src.schemas.decision_scale import action_for_score, decision_type_for_score, signal_key_for_score


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("strong_buy", "buy"),
        ("强烈买入", "buy"),
        ("买入", "buy"),
        ("布局", "buy"),
        ("建仓", "buy"),
        ("add", "add"),
        ("加仓", "add"),
        ("增持", "add"),
        ("accumulate", "add"),
        ("hold", "hold"),
        ("持有", "hold"),
        ("持有观察", "hold"),
        ("洗盘观察", "hold"),
        ("watch", "watch"),
        ("观望", "watch"),
        ("等待", "watch"),
        ("wait", "watch"),
        ("reduce", "reduce"),
        ("减仓", "reduce"),
        ("trim", "reduce"),
        ("sell", "sell"),
        ("卖出", "sell"),
        ("清仓", "sell"),
        ("strong_sell", "sell"),
        ("强烈卖出", "sell"),
        ("avoid", "avoid"),
        ("回避", "avoid"),
        ("规避", "avoid"),
        ("不建议买入", "avoid"),
        ("避免买入", "avoid"),
        ("do not buy", "avoid"),
        ("alert", "alert"),
        ("风险预警", "alert"),
        ("警惕", "alert"),
        ("触发告警", "alert"),
        ("risk alert", "alert"),
    ],
)
def test_normalize_decision_action_matrix(value: str, expected: str) -> None:
    assert normalize_decision_action(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        None,
        "观察",
        "等待突破后买入",
        "waiting to buy",
        "买入或卖出",
        "buy or sell",
        "买盘增强，继续观察",
        "卖压缓解，继续观察",
        "卖方评级分歧",
        "no buyback announced",
        "cannot buyback shares now",
        "share buy-back announced",
        "share buy back announced",
        "no selloff risk",
        "not selloff yet",
        "sell-off risk remains low",
        "sell off risk remains low",
        "no sell-off pressure",
        "risk alert, avoid buying",
        "风险预警，避免买入",
        "普通复盘说明",
    ],
)
def test_normalize_decision_action_unknown_or_ambiguous_returns_none(value: str | None) -> None:
    assert normalize_decision_action(value) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("暂不买入", "avoid"),
        ("不要买入", "avoid"),
        ("不宜买入", "avoid"),
        ("先不买入", "avoid"),
        ("无需买入", "avoid"),
        ("无须买入", "avoid"),
        ("不建议建仓", "avoid"),
        ("暂不建仓", "avoid"),
        ("无需建仓", "avoid"),
        ("无须建仓", "avoid"),
        ("不建议布局", "avoid"),
        ("先不布局", "avoid"),
        ("无需布局", "avoid"),
        ("无须布局", "avoid"),
        ("no buy", "avoid"),
        ("no need to buy", "avoid"),
        ("need not buy", "avoid"),
        ("cannot buy", "avoid"),
        ("can't buy", "avoid"),
        ("not a buy yet", "avoid"),
        ("not to buy", "avoid"),
        ("avoid buying", "avoid"),
        ("avoid buying into weakness", "avoid"),
        ("不建议加仓", "hold"),
        ("无须加仓", "hold"),
        ("no add", "hold"),
        ("no need to add", "hold"),
        ("need not add", "hold"),
        ("cannot add", "hold"),
        ("not to add", "hold"),
        ("no accumulate", "hold"),
        ("can't accumulate", "hold"),
        ("not to accumulate", "hold"),
        ("不建议卖出", "hold"),
        ("无需卖出", "hold"),
        ("无须卖出", "hold"),
        ("不要卖出", "hold"),
        ("暂不卖出", "hold"),
        ("no sell", "hold"),
        ("no need to sell", "hold"),
        ("cannot sell", "hold"),
        ("can't sell", "hold"),
        ("not a sell yet", "hold"),
        ("not to sell", "hold"),
        ("无需减仓", "hold"),
        ("无须减仓", "hold"),
        ("no reduce", "hold"),
        ("no need to reduce", "hold"),
        ("cannot reduce", "hold"),
        ("not to reduce", "hold"),
        ("no trim", "hold"),
        ("can't trim", "hold"),
        ("not a trim yet", "hold"),
        ("not to trim", "hold"),
        ("avoid selling into weakness", "hold"),
        ("avoid trimming before earnings", "hold"),
        ("avoid reducing exposure before earnings", "hold"),
        ("不建议清仓", "hold"),
    ],
)
def test_normalize_decision_action_handles_negated_trade_actions(value: str, expected: str) -> None:
    assert normalize_decision_action(value) == expected


@pytest.mark.parametrize(
    "advice",
    [
        "无需买入，等待确认",
        "无须建仓，继续观察",
        "无需布局，等待突破",
        "no buy until breakout",
        "no need to buy before confirmation",
        "cannot buy before confirmation",
        "can't buy before confirmation",
        "not a buy yet",
        "not to buy",
    ],
)
def test_build_action_fields_prioritizes_negated_buy_advice_over_embedded_buy_phrase(advice: str) -> None:
    assert build_action_fields(operation_advice=advice) == {
        "action": "avoid",
        "action_label": "回避",
    }


@pytest.mark.parametrize(
    "advice",
    [
        "无须加仓，维持仓位",
        "无需卖出，继续持有",
        "无须减仓，等待确认",
        "no add before confirmation",
        "cannot add before confirmation",
        "no need to accumulate here",
        "can't accumulate here",
        "no sell before earnings",
        "cannot sell before earnings",
        "no need to reduce exposure",
        "can't reduce exposure",
        "no trim while trend holds",
        "cannot trim while trend holds",
        "not a sell yet",
        "not a trim yet",
        "not to sell",
        "not to trim",
        "avoid selling into weakness",
        "avoid trimming before earnings",
        "avoid reducing exposure before earnings",
    ],
)
def test_build_action_fields_prioritizes_negated_hold_advice_over_embedded_trade_phrase(advice: str) -> None:
    assert build_action_fields(operation_advice=advice) == {
        "action": "hold",
        "action_label": "持有",
    }


@pytest.mark.parametrize(
    "advice",
    [
        "risk alert, avoid buying",
        "风险预警，避免买入",
    ],
)
def test_build_action_fields_keeps_multi_guard_advice_empty(advice: str) -> None:
    assert build_action_fields(operation_advice=advice) == {
        "action": None,
        "action_label": None,
    }


@pytest.mark.parametrize(
    "advice",
    [
        "买盘增强，继续观察",
        "卖压缓解，继续观察",
        "卖方评级分歧",
    ],
)
def test_build_action_fields_keeps_chinese_financial_context_empty(advice: str) -> None:
    assert build_action_fields(operation_advice=advice) == {
        "action": None,
        "action_label": None,
    }


@pytest.mark.parametrize(
    "advice",
    [
        "no buyback announced",
        "cannot buyback shares now",
        "no selloff risk",
        "not selloff yet",
    ],
)
def test_build_action_fields_keeps_financial_compound_terms_empty(advice: str) -> None:
    assert build_action_fields(operation_advice=advice) == {
        "action": None,
        "action_label": None,
    }


@pytest.mark.parametrize(
    "advice",
    [
        "share buy-back announced",
        "share buy back announced",
        "sell-off risk remains low",
        "sell off risk remains low",
        "no sell-off pressure",
    ],
)
def test_build_action_fields_keeps_hyphenated_financial_compound_terms_empty(advice: str) -> None:
    assert build_action_fields(operation_advice=advice) == {
        "action": None,
        "action_label": None,
    }


@pytest.mark.parametrize(
    ("advice", "expected_action", "expected_label"),
    [
        ("buy after sell-off", "buy", "买入"),
        ("sell after buy-back rumor", "sell", "卖出"),
    ],
)
def test_financial_compound_mask_preserves_separate_action_terms(
    advice: str,
    expected_action: str,
    expected_label: str,
) -> None:
    assert normalize_decision_action(advice) == expected_action
    assert build_action_fields(operation_advice=advice) == {
        "action": expected_action,
        "action_label": expected_label,
    }


def test_localize_action_label_uses_report_language() -> None:
    assert localize_action_label("avoid", "zh") == "回避"
    assert localize_action_label("avoid", "en") == "Avoid"


@pytest.mark.parametrize(
    ("action", "expected_label"),
    [
        ("buy", "매수"),
        ("add", "추가 매수"),
        ("hold", "보유"),
        ("reduce", "비중축소"),
        ("sell", "매도"),
        ("watch", "관망"),
        ("avoid", "회피"),
        ("alert", "경고"),
    ],
)
def test_localize_action_label_supports_korean(action: str, expected_label: str) -> None:
    assert localize_action_label(action, "ko") == expected_label


def test_build_action_fields_respects_market_review_exclusion() -> None:
    fields = build_action_fields(
        operation_advice="买入",
        explicit_action="buy",
        report_type="market_review",
    )

    assert fields == {"action": None, "action_label": None}


def test_build_action_fields_prefers_explicit_action_over_advice() -> None:
    fields = build_action_fields(
        operation_advice="买入",
        explicit_action="watch",
        report_language="zh",
    )

    assert fields == {"action": "watch", "action_label": "观望"}


def test_build_action_fields_keeps_empty_action_without_advice_or_explicit_action() -> None:
    fields = build_action_fields(
        operation_advice=None,
        report_language="zh",
    )

    assert fields == {"action": None, "action_label": None}


@pytest.mark.parametrize(
    ("score", "expected_signal", "expected_action", "expected_decision_type"),
    [
        (28, "reduce", "reduce", "sell"),
        (38, "reduce", "reduce", "sell"),
        (42, "watch", "watch", "hold"),
        (55, "watch", "watch", "hold"),
        (60, "buy", "buy", "buy"),
        (66, "buy", "buy", "buy"),
        (72, "buy", "buy", "buy"),
    ],
)
def test_canonical_score_scale_boundaries(
    score: int,
    expected_signal: str,
    expected_action: str,
    expected_decision_type: str,
) -> None:
    assert signal_key_for_score(score) == expected_signal
    assert action_for_score(score) == expected_action
    assert decision_type_for_score(score) == expected_decision_type


def test_build_action_fields_can_align_neutral_action_with_directional_score() -> None:
    assert build_action_fields(
        operation_advice="持有",
        sentiment_score=72,
        align_with_score=True,
    ) == {"action": "buy", "action_label": "买入"}

    assert build_action_fields(
        operation_advice="观望",
        sentiment_score=28,
        align_with_score=True,
    ) == {"action": "reduce", "action_label": "减仓"}


def test_build_action_fields_keeps_neutral_score_conflict_when_guardrail_is_explicit() -> None:
    assert build_action_fields(
        operation_advice="持有/观望待回踩",
        sentiment_score=72,
        guardrail_reason="等待回踩确认",
        align_with_score=True,
    ) == {"action": "watch", "action_label": "观望"}


def test_unified_contract_can_preserve_native_tool_action_without_score_promotion() -> None:
    from src.schemas.decision_action import normalize_result_decision_contract

    result = type(
        "Result",
        (),
        {
            "sentiment_score": 72,
            "raw_score": 72,
            "adjusted_score": None,
            "action": "watch",
            "operation_advice": "观望",
            "decision_type": "hold",
            "action_label": None,
            "report_language": "zh",
            "dashboard": {},
            "guardrail": None,
            "position_actions": None,
            "holding_state": "unknown",
        },
    )()

    normalize_result_decision_contract(result, explicit_action="watch", align_with_score=False)

    assert result.raw_score == 72
    assert result.adjusted_score == 72
    assert result.sentiment_score == 72
    assert result.action == "watch"
    assert result.action_label == "观望"
    assert result.operation_advice == "观望"
    assert result.decision_type == "hold"


def test_display_helpers_share_score_aligned_action_with_report_rows_and_counts() -> None:
    result = type(
        "Result",
        (),
        {
            "operation_advice": "Hold",
            "action": None,
            "action_label": None,
            "sentiment_score": 72,
            "decision_type": "hold",
            "report_language": "en",
            "dashboard": {},
        },
    )()

    assert display_action_fields_for_result(result) == {"action": "buy", "action_label": "Buy"}
    assert display_operation_advice_for_result(result) == "Buy"
    assert display_decision_type_for_result(result) == "buy"


def test_display_helpers_preserve_neutral_action_when_guardrail_was_applied() -> None:
    result = type(
        "Result",
        (),
        {
            "operation_advice": "Hold",
            "action": "hold",
            "action_label": "Hold",
            "sentiment_score": 72,
            "decision_type": "hold",
            "report_language": "en",
            "dashboard": {
                "decision_stability": {
                    "applied": True,
                    "reason": "Wait for confirmation",
                }
            },
        },
    )()

    assert display_action_fields_for_result(result) == {"action": "hold", "action_label": "Hold"}
    assert display_operation_advice_for_result(result) == "Hold"
    assert display_decision_type_for_result(result) == "hold"


def test_unified_contract_derives_legacy_fields_from_structured_action() -> None:
    from src.schemas.decision_action import normalize_result_decision_contract

    result = type(
        "Result",
        (),
        {
            "sentiment_score": 65,
            "raw_score": None,
            "action": "buy",
            "operation_advice": "观望",
            "decision_type": "hold",
            "action_label": None,
            "report_language": "zh",
            "dashboard": {},
            "guardrail": None,
            "position_actions": None,
        },
    )()

    normalize_result_decision_contract(result)

    assert result.raw_score == 65
    assert result.adjusted_score == 65
    assert result.sentiment_score == 65
    assert result.action == "buy"
    assert result.action_label == "买入"
    assert result.operation_advice == "买入"
    assert result.decision_type == "buy"


def test_unified_contract_requires_complete_guardrail_and_explicit_scenario_actions() -> None:
    from src.schemas.decision_action import normalize_result_decision_contract

    result = type(
        "Result",
        (),
        {
            "sentiment_score": 72,
            "raw_score": None,
            "action": "watch",
            "operation_advice": "观望",
            "decision_type": "hold",
            "action_label": None,
            "report_language": "zh",
            "dashboard": {
                "core_conclusion": {
                    "position_advice": {
                        "no_position": "空仓观望",
                        "has_position": "继续持有",
                    }
                }
            },
            "guardrail": {"applied": True, "reason": "等待确认"},
            "position_actions": None,
        },
    )()

    normalize_result_decision_contract(result)

    assert result.action == "buy"
    assert result.operation_advice == "买入"
    assert result.guardrail == {"applied": False, "invalid": True}
    assert result.position_actions["empty"] == {"advice": "空仓观望"}
    assert result.position_actions["holding"] == {"advice": "继续持有"}


def test_unified_contract_accepts_auditable_guardrail_and_rewrites_conflicting_position_advice() -> None:
    from src.schemas.decision_action import normalize_result_decision_contract

    result = type(
        "Result",
        (),
        {
            "sentiment_score": 72,
            "raw_score": None,
            "action": "watch",
            "operation_advice": "观望",
            "decision_type": "hold",
            "action_label": None,
            "report_language": "zh",
            "dashboard": {},
            "guardrail": {
                "applied": True,
                "reason_code": "near_resistance",
                "reason": "临近压力位且量价背离",
                "raw_score": 72,
                "adjusted_score": 55,
                "score_delta": -17,
            },
            "position_actions": {
                "empty": {"action": "watch", "advice": "观望"},
                "holding": {"action": "hold", "advice": "观望"},
            },
        },
    )()

    normalize_result_decision_contract(result)

    assert result.raw_score == 72
    assert result.adjusted_score == result.sentiment_score == 55
    assert result.action == "watch"
    assert result.operation_advice == "观望"
    assert result.guardrail["score_delta"] == -17
    assert result.position_actions["holding"] == {"action": "hold", "advice": "持有"}


def test_unified_contract_resolves_empty_hold_alias_to_watch() -> None:
    from src.schemas.decision_action import normalize_result_decision_contract

    result = type(
        "Result",
        (),
        {
            "sentiment_score": 46,
            "raw_score": None,
            "action": "hold",
            "operation_advice": "持有",
            "decision_type": "hold",
            "action_label": None,
            "report_language": "zh",
            "holding_state": "empty",
            "dashboard": {
                "core_conclusion": {
                    "position_advice": {
                        "无持仓": {"action": "hold", "advice": "持有观察"},
                        "existing_position": {"action": "reduce", "advice": "持仓减仓"},
                    },
                },
            },
            "guardrail": None,
            "position_actions": None,
        },
    )()

    normalize_result_decision_contract(result)

    assert result.holding_state == "empty"
    assert result.action == "watch"
    assert result.action_label == "观望"
    assert result.operation_advice == "观望"
    assert result.decision_type == "hold"
    assert result.position_actions["empty"].get("action") is None
    assert result.position_actions["holding"].get("action") is None


def test_unified_contract_preserves_only_explicit_valid_holding_risk() -> None:
    from src.schemas.decision_action import normalize_result_decision_contract

    result = type(
        "Result",
        (),
        {
            "sentiment_score": 55,
            "raw_score": None,
            "action": "watch",
            "operation_advice": "观望",
            "decision_type": "hold",
            "action_label": None,
            "report_language": "zh",
            "dashboard": {"battle_plan": {"sniper_points": {"stop_loss": 88, "take_profit": 150}}},
            "guardrail": None,
            "position_actions": {
                "empty": {"action": "watch", "advice": "等待"},
                "holding": {
                    "action": "hold",
                    "advice": "持有",
                    "stop_loss": 101.5,
                    "target_price": 128,
                },
            },
        },
    )()

    normalize_result_decision_contract(result)

    assert result.position_actions["holding"]["stop_loss"] == 101.5
    assert result.position_actions["holding"]["target_price"] == 128.0

    result.position_actions = {
        "holding": {"action": "hold", "advice": "持有", "stop_loss": -1, "target_price": "nan"}
    }
    normalize_result_decision_contract(result)
    assert result.position_actions["holding"] == {"action": "hold", "advice": "持有"}
    assert "stop_loss" not in result.position_actions["holding"]
    assert "target_price" not in result.position_actions["holding"]


def test_position_aware_action_uses_matching_frozen_branch() -> None:
    from src.schemas.decision_action import resolve_position_aware_action_fields

    branches = {
        "empty": {"action": "watch", "advice": "空仓观望"},
        "holding": {"action": "reduce", "advice": "持仓减仓"},
    }

    empty = resolve_position_aware_action_fields(
        holding_state="empty",
        position_actions=branches,
        fallback_action="reduce",
        report_language="zh",
    )
    holding = resolve_position_aware_action_fields(
        holding_state="holding",
        position_actions=branches,
        fallback_action="watch",
        report_language="zh",
    )

    assert empty == {
        "action": "watch",
        "action_label": "观望",
        "holding_state": "empty",
        "resolution_source": "position_actions.empty",
        "base_action": "reduce",
    }
    assert holding["action"] == "reduce"
    assert holding["resolution_source"] == "position_actions.holding"


def test_position_aware_action_fails_closed_for_empty_holder_only_fallbacks() -> None:
    from src.schemas.decision_action import resolve_position_aware_action_fields

    assert resolve_position_aware_action_fields(
        holding_state="empty",
        position_actions={},
        fallback_action="hold",
    )["action"] == "watch"
    assert resolve_position_aware_action_fields(
        holding_state="empty",
        position_actions={"empty": {"action": "sell"}},
        fallback_action="sell",
    )["action"] == "avoid"
    assert resolve_position_aware_action_fields(
        holding_state="empty",
        position_actions={},
        fallback_action="reduce",
    )["action"] == "avoid"


def test_position_aware_action_does_not_infer_holding_when_state_is_unknown() -> None:
    from src.schemas.decision_action import resolve_position_aware_action_fields

    resolved = resolve_position_aware_action_fields(
        holding_state="unexpected",
        position_actions={
            "empty": {"action": "watch"},
            "holding": {"action": "reduce"},
        },
        fallback_action="alert",
    )

    assert resolved["holding_state"] == "unknown"
    assert resolved["action"] == "alert"
    assert resolved["resolution_source"] == "canonical_fallback"
