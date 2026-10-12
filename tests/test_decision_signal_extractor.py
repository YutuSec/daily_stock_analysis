# -*- coding: utf-8 -*-
"""Tests for extracting DecisionSignal assets from completed reports."""

from __future__ import annotations

import os
import json

import pytest

from src.analyzer import AnalysisResult, GeminiAnalyzer
from src.config import Config
from src.services.decision_signal_extractor import (
    build_decision_signal_payload_from_report,
    extract_and_persist_from_analysis_result,
)
from src.services.decision_signal_service import DecisionSignalService
from src.storage import DatabaseManager


BUILD_PROFILE_SOURCE = "legacy_unknown"


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    db_path = tmp_path / "decision_signal_extractor.db"
    os.environ["DATABASE_PATH"] = str(db_path)
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


def _result(**overrides) -> AnalysisResult:
    result = AnalysisResult(
        code="600519",
        name="贵州茅台",
        sentiment_score=82,
        trend_prediction="看多",
        operation_advice="买入",
        decision_type="buy",
        confidence_level="高",
        analysis_summary="趋势确认，量价配合。",
        risk_warning="跌破支撑需止损",
        report_language="zh",
    )
    result.dashboard = {
        "battle_plan": {
            "sniper_points": {
                "ideal_buy": "理想买入点：1700元",
                "secondary_buy": "1680-1690（回踩MA5附近）",
                "stop_loss": "止损位：1600元",
                "take_profit": "目标位：1850元",
            },
            "action_checklist": ["放量突破前高", "回踩不破MA10"],
        },
        "phase_decision": {
            "watch_conditions": ["盘中量能继续放大"],
        },
        "intelligence": {
            "risk_alerts": ["估值偏高"],
            "positive_catalysts": ["业绩超预期"],
        },
    }
    for key, value in overrides.items():
        setattr(result, key, value)
    return result


def test_build_payload_rejects_invalid_profile_source() -> None:
    with pytest.raises(ValueError, match="profile_source"):
        build_decision_signal_payload_from_report(
            _result(),
            trace_id="trace-invalid-profile-source",
            query_source="api",
            report_type="simple",
            profile_source="typo",
        )


def test_native_tool_buy_without_executable_contract_is_not_persistable() -> None:
    result = _result(report_schema_version="analysis-report-tool-v1")
    result.action = "buy"
    result.dashboard["phase_decision"]["entry_mode"] = "conditional"

    payload = build_decision_signal_payload_from_report(
        result,
        trace_id="trace-native-tool-incomplete-buy",
        query_source="api",
        report_type="full",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is None


def test_native_tool_buy_with_execution_contract_ignores_missing_display_fields() -> None:
    result = _result(report_schema_version="analysis-report-tool-v1")
    result.action = "buy"
    result.position_actions = {
        "empty": {"action": "buy"},
        "holding": {"action": "hold"},
    }
    result.analysis_summary = ""
    result.key_points = ""
    result.news_summary = ""
    result.fundamental_analysis = ""
    result.dashboard = {
        "battle_plan": {
            "sniper_points": {
                "ideal_buy": 1690,
                "secondary_buy": 1700,
                "stop_loss": 1600,
                "take_profit": 1850,
                "final_target": 1850,
                "entry_leg_schema_version": "entry-legs-v1",
                "entry_legs": [
                    {
                        "leg_id": "initial",
                        "role": "initial",
                        "sequence": 1,
                        "allocation_fraction": 1.0,
                        "max_executions": 1,
                        "price_rules": [{"type": "close_between", "low": 1690, "high": 1700}],
                        "prerequisites": [],
                    }
                ],
                "exit_leg_schema_version": "exit-legs-v1",
                "exit_legs": [
                    {
                        "leg_id": "tp1",
                        "role": "final_take_profit",
                        "sequence": 1,
                        "allocation_fraction": 1.0,
                        "target_price": 1850,
                    }
                ],
            }
        },
        "phase_decision": {
            "entry_mode": "conditional",
            "action_window": "下个交易日盘中跟踪",
            "immediate_action": "等待确认",
            "watch_conditions": ["放量站稳"],
            "next_check_time": "下个交易日 09:35",
        },
        "intelligence": {"risk_alerts": "估值偏高"},
    }

    payload = build_decision_signal_payload_from_report(
        result,
        trace_id="trace-native-tool-executable-buy",
        query_source="api",
        report_type="full",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert payload["action"] == "buy"
    assert payload["entry_low"] == 1690
    assert payload["entry_high"] == 1700
    assert payload["stop_loss"] == 1600
    assert payload["target_price"] == 1850
    assert payload["metadata"]["entry_mode"] == "conditional"


def test_native_tool_watch_with_ideal_prices_does_not_upgrade_to_buy_signal() -> None:
    result = _result(
        report_schema_version="analysis-report-tool-v1",
        action="watch",
        decision_type="hold",
        operation_advice="观望，等待触发条件",
    )
    result.position_actions = {
        "empty": {"action": "watch"},
        "holding": {"action": "hold"},
    }
    result.dashboard = {
        "battle_plan": {
            "sniper_points": {
                "ideal_buy": 1690,
                "secondary_buy": 1700,
                "stop_loss": 1600,
                "take_profit": 1850,
                "final_target": 1850,
                "entry_leg_schema_version": "entry-legs-v1",
                "entry_legs": [
                    {
                        "leg_id": "initial",
                        "role": "initial",
                        "sequence": 1,
                        "allocation_fraction": 1.0,
                        "max_executions": 1,
                        "price_rules": [{"type": "close_between", "low": 1690, "high": 1700}],
                        "prerequisites": [],
                    }
                ],
                "exit_leg_schema_version": "exit-legs-v1",
                "exit_legs": [
                    {
                        "leg_id": "tp1",
                        "role": "final_take_profit",
                        "sequence": 1,
                        "allocation_fraction": 1.0,
                        "target_price": 1850,
                    }
                ],
            }
        },
        "phase_decision": {
            "entry_mode": "none",
            "action_window": "下个交易日盘中跟踪",
            "immediate_action": "等待确认",
            "watch_conditions": ["放量站稳"],
            "next_check_time": "下个交易日 09:35",
        },
    }

    payload = build_decision_signal_payload_from_report(
        result,
        trace_id="trace-native-tool-watch-price-plan",
        query_source="api",
        report_type="full",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert payload["action"] == "watch"
    assert payload["metadata"]["entry_mode"] == "none"


def test_parse_response_native_high_score_watch_reaches_signal_as_watch_not_buy() -> None:
    payload = {
        "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
        "stock_name": "贵州茅台",
        "raw_score": 72,
        "adjusted_score": 72,
        "sentiment_score": 72,
        "trend_prediction": "看多",
        "operation_advice": "观望，等待触发条件",
        "decision_type": "hold",
        "action": "watch",
        "position_actions": {
            "empty": {"action": "watch"},
            "holding": {"action": "hold"},
        },
        "confidence_level": "中",
        "analysis_summary": "分数偏强但还未触发入场条件。",
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "ideal_buy": 1690,
                    "secondary_buy": 1700,
                    "stop_loss": 1600,
                    "take_profit": 1850,
                    "final_target": 1850,
                    "entry_leg_schema_version": "entry-legs-v1",
                    "entry_legs": [
                        {
                            "leg_id": "initial",
                            "role": "initial",
                            "sequence": 1,
                            "allocation_fraction": 1.0,
                            "max_executions": 1,
                            "price_rules": [{"type": "close_between", "low": 1690, "high": 1700}],
                            "prerequisites": [],
                        }
                    ],
                    "exit_leg_schema_version": "exit-legs-v1",
                    "exit_legs": [
                        {
                            "leg_id": "tp1",
                            "role": "final_take_profit",
                            "sequence": 1,
                            "allocation_fraction": 1.0,
                            "target_price": 1850,
                        }
                    ],
                }
            },
            "phase_decision": {
                "entry_mode": "none",
                "action_window": "下个交易日盘中跟踪",
                "immediate_action": "等待确认",
                "watch_conditions": ["放量站稳"],
                "next_check_time": "下个交易日 09:35",
            },
        },
    }
    result = GeminiAnalyzer()._parse_response(
        json.dumps(payload, ensure_ascii=False),
        "600519",
        "贵州茅台",
    )

    signal_payload = build_decision_signal_payload_from_report(
        result,
        trace_id="trace-parse-native-watch-high-score",
        query_source="api",
        report_type="full",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert result.action == "watch"
    assert signal_payload is not None
    assert signal_payload["action"] == "watch"
    assert signal_payload["metadata"]["raw_action"] == "watch"
    assert signal_payload["metadata"]["final_action"] == "watch"
    assert signal_payload["metadata"]["entry_mode"] == "none"


def test_native_tool_conditional_watch_is_rejected_even_with_complete_price_plan() -> None:
    result = _result(
        report_schema_version="analysis-report-tool-v1",
        action="watch",
        decision_type="hold",
        operation_advice="观望但给出条件价",
    )
    result.position_actions = {
        "empty": {"action": "watch"},
        "holding": {"action": "hold"},
    }
    result.dashboard = {
        "battle_plan": {
            "sniper_points": {
                "ideal_buy": 1690,
                "secondary_buy": 1700,
                "stop_loss": 1600,
                "take_profit": 1850,
                "final_target": 1850,
                "entry_leg_schema_version": "entry-legs-v1",
                "entry_legs": [
                    {
                        "leg_id": "initial",
                        "role": "initial",
                        "sequence": 1,
                        "allocation_fraction": 1.0,
                        "max_executions": 1,
                        "price_rules": [{"type": "close_between", "low": 1690, "high": 1700}],
                        "prerequisites": [],
                    }
                ],
                "exit_leg_schema_version": "exit-legs-v1",
                "exit_legs": [
                    {
                        "leg_id": "tp1",
                        "role": "final_take_profit",
                        "sequence": 1,
                        "allocation_fraction": 1.0,
                        "target_price": 1850,
                    }
                ],
            }
        },
        "phase_decision": {
            "entry_mode": "conditional",
            "action_window": "下个交易日盘中跟踪",
            "immediate_action": "等待确认",
            "watch_conditions": ["放量站稳"],
            "next_check_time": "下个交易日 09:35",
        },
    }

    payload = build_decision_signal_payload_from_report(
        result,
        trace_id="trace-native-tool-conditional-watch",
        query_source="api",
        report_type="full",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is None


def test_native_tool_buy_with_empty_watch_conditions_is_not_persistable() -> None:
    result = _result(report_schema_version="analysis-report-tool-v1")
    result.action = "buy"
    result.position_actions = {
        "empty": {"action": "buy"},
        "holding": {"action": "hold"},
    }
    result.dashboard = {
        "battle_plan": {
            "sniper_points": {
                "ideal_buy": 1690,
                "secondary_buy": 1700,
                "stop_loss": 1600,
                "take_profit": 1850,
                "final_target": 1850,
                "entry_leg_schema_version": "entry-legs-v1",
                "entry_legs": [
                    {
                        "leg_id": "initial",
                        "role": "initial",
                        "sequence": 1,
                        "allocation_fraction": 1.0,
                        "max_executions": 1,
                        "price_rules": [{"type": "close_between", "low": 1690, "high": 1700}],
                        "prerequisites": [],
                    }
                ],
                "exit_leg_schema_version": "exit-legs-v1",
                "exit_legs": [
                    {
                        "leg_id": "tp1",
                        "role": "final_take_profit",
                        "sequence": 1,
                        "allocation_fraction": 1.0,
                        "target_price": 1850,
                    }
                ],
            }
        },
        "phase_decision": {
            "entry_mode": "conditional",
            "action_window": "下个交易日盘中跟踪",
            "immediate_action": "等待确认",
            "watch_conditions": [],
            "next_check_time": "下个交易日 09:35",
        },
    }

    payload = build_decision_signal_payload_from_report(
        result,
        trace_id="trace-native-tool-empty-watch-conditions",
        query_source="api",
        report_type="full",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is None


def test_build_payload_includes_tw_market() -> None:
    """A Taiwan (`tw`) stock is now first-class on the DecisionSignal write path
    (service VALID_MARKETS accepts tw, matching jp/kr).

    Regression guard for the data-layer MVP follow-up: the analysis pipeline
    auto-extracts a DecisionSignal after history save, so tw must PRODUCE a
    payload (market == "tw") rather than be silently dropped by _normalize_market.
    A plain action ("buy") is set so the path reaches the market mapping.
    """
    result = _result(code="2330.TW", name="台积电")

    payload = build_decision_signal_payload_from_report(
        result,
        context_snapshot=None,
        portfolio_context=None,
        source_report_id=None,
        trace_id="trace-tw",
        query_source="api",
        report_type="full",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert payload["market"] == "tw"
    assert payload["action"] == "buy"


def test_intraday_analysis_uses_frozen_position_style_horizon() -> None:
    payload = build_decision_signal_payload_from_report(
        _result(),
        context_snapshot={
            "market_phase_summary": {"phase": "intraday"},
            "enhanced_context": {
                "paper_trading_style_context": {
                    "trading_style": "position",
                    "settings": {
                        "compatible_horizons": ["3d", "5d", "10d", "swing"],
                        "default_signal_horizon": "swing",
                    },
                },
                "paper_trading_style_indicators": {
                    "history_sufficient": True,
                    "bars_observed": 220,
                },
            },
        },
        source_report_id=902,
        trace_id="trace-position-intraday",
        query_source="api",
        report_type="full",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert payload["market_phase"] == "intraday"
    assert payload["horizon"] == "swing"


def test_build_payload_maps_report_context_and_price_plan() -> None:
    result = _result()
    result.position_actions = {
        "empty": {"action": "buy", "reason": "空仓可试仓"},
        "holding": {
            "action": "reduce",
            "reason": "已有持仓先降风险",
            "stop_loss": 1660.0,
            "target_price": 1820.0,
        },
    }
    result.market_phase_summary = {"phase": "postmarket"}
    result.analysis_context_pack_overview = {"data_quality": {"overall_score": 55, "level": "fair"}}
    context_snapshot = {
        "market_phase_summary": {
            "phase": "intraday",
            "session_date": "2026-06-15",
            "minutes_to_open": None,
            "minutes_to_close": 120,
        },
        "analysis_context_pack_overview": {
            "data_quality": {"overall_score": 91, "level": "good"},
        },
    }

    payload = build_decision_signal_payload_from_report(
        result,
        context_snapshot=context_snapshot,
        portfolio_context={"quantity": "200"},
        source_report_id=88,
        trace_id="trace-88",
        query_source="api",
        report_type="full",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert payload["stock_code"] == "600519"
    assert payload["stock_name"] == "贵州茅台"
    assert payload["market"] == "cn"
    assert payload["source_type"] == "analysis"
    assert payload["source_report_id"] == 88
    assert payload["trace_id"] == "trace-88"
    assert payload["decision_profile"] == "balanced"
    assert payload["trigger_source"] == "api"
    assert payload["action"] == "reduce"
    assert payload["confidence"] == 0.8
    assert payload["score"] == 82
    assert payload["market_phase"] == "intraday"
    assert payload["entry_low"] == 1690.0
    assert payload["entry_high"] == 1700.0
    assert payload["stop_loss"] == 1600.0
    assert payload["target_price"] == 1850.0
    assert payload["data_quality_summary"]["overall_score"] == 91
    assert payload["watch_conditions"] == ["盘中量能继续放大"]
    assert payload["risk_summary"] == ["跌破支撑需止损", "估值偏高"]
    assert payload["catalyst_summary"] == ["业绩超预期"]
    assert payload["metadata"]["report_confidence_level"] == "高"
    assert payload["metadata"]["decision_profile"] == "balanced"
    assert payload["metadata"]["profile_source"] == BUILD_PROFILE_SOURCE
    assert payload["metadata"]["profile_policy_version"] == "decision-profile-v1"
    assert payload["metadata"]["signal_generation_version"] == "legacy-report-extractor-v1"
    assert payload["metadata"]["decision_signal_metadata_version"] == "decision-signal-metadata-v1"
    assert "scoring_version" not in payload["metadata"]
    assert "scoring_breakdown" not in payload["metadata"]
    assert payload["metadata"]["market_phase_summary"] == {
        "phase": "intraday",
        "session_date": "2026-06-15",
        "minutes_to_close": 120,
    }
    assert payload["metadata"]["holding_state"] == "holding"
    assert payload["metadata"]["base_action"] == "buy"
    assert payload["metadata"]["effective_action_source"] == "position_actions.holding"
    assert payload["metadata"]["action_adjustment_reason"] == "holding_state_resolution"
    assert payload["metadata"]["position_actions"]["holding"]["action"] == "reduce"
    assert payload["metadata"]["position_actions"]["holding"]["reason"] == "已有持仓先降风险"
    assert payload["metadata"]["position_actions"]["holding"]["stop_loss"] == 1660.0
    assert payload["metadata"]["position_actions"]["holding"]["target_price"] == 1820.0


def test_build_payload_uses_empty_branch_instead_of_holder_reduce_action() -> None:
    result = _result(
        code="002173.SZ",
        name="创新医疗",
        sentiment_score=37,
        operation_advice="减仓",
        action="reduce",
        decision_type="sell",
    )
    result.holding_state = "empty"
    result.position_actions = {
        "empty": {"action": "watch", "advice": "空仓者观望"},
        "holding": {"action": "reduce", "advice": "持仓者减仓"},
    }

    payload = build_decision_signal_payload_from_report(
        result,
        portfolio_context={"quantity": 0},
        source_report_id=126,
        trace_id="trace-empty-innovation-medical",
        query_source="schedule",
        report_type="full",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert payload["action"] == "watch"
    assert payload["action_label"] == "观望"
    assert payload["metadata"]["holding_state"] == "empty"
    assert payload["metadata"]["base_action"] == "reduce"
    assert payload["metadata"]["effective_action_source"] == "position_actions.empty"
    assert payload["metadata"]["action_adjustment_reason"] == "holding_state_resolution"


def test_build_payload_adds_market_structure_metadata() -> None:
    context_snapshot = {
        "market_structure_context": {
            "schema_version": "market-structure-v1",
            "status": "partial",
            "market": "cn",
            "market_theme_context": {
                "schema_version": "market-theme-v1",
                "status": "partial",
                "market": "cn",
            },
            "stock_market_position": {
                "schema_version": "stock-market-position-v1",
                "status": "partial",
                "stock_code": "300024",
                "market": "cn",
                "primary_theme": {"name": "机器人概念"},
                "theme_phase": "accelerating",
                "stock_role": "follower",
                "risk_tags": [{"code": "theme_data_partial", "message": "partial"}],
            },
        }
    }

    payload = build_decision_signal_payload_from_report(
        _result(code="300024", name="机器人"),
        context_snapshot=context_snapshot,
        source_report_id=91,
        trace_id="trace-market-structure",
        query_source="api",
        report_type="full",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    metadata = payload["metadata"]
    assert metadata["market_structure_version"] == "market-structure-v1"
    assert metadata["market_theme_version"] == "market-theme-v1"
    assert metadata["stock_market_position_version"] == "stock-market-position-v1"
    assert metadata["market_structure_status"] == "partial"
    assert metadata["primary_theme"] == "机器人概念"
    assert metadata["theme_phase"] == "accelerating"
    assert metadata["stock_role"] == "follower"
    assert metadata["market_structure_risk_tags"] == ["theme_data_partial"]


def test_build_payload_freezes_paper_trading_style_context_from_snapshot() -> None:
    style_context = {
        "trading_style": "trend",
        "policy_version": "trading-style-v1",
        "source": "paper_trading_style_override_config",
        "matched_override_key": "cn:600519",
        "settings": {
            "analysis_window_sessions": 500,
            "max_holding_sessions": 180,
            "core_ma_windows": [20, 60, 120, 250],
        },
    }
    indicators = {
        "analysis_window_sessions": 500,
        "bars_observed": 500,
        "history_sufficient": True,
    }
    payload = build_decision_signal_payload_from_report(
        _result(),
        context_snapshot={
            "enhanced_context": {
                "paper_trading_style_context": style_context,
                "paper_trading_style_indicators": indicators,
            }
        },
        source_report_id=99,
        trace_id="trace-style-freeze",
        query_source="system",
        report_type="full",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    metadata = payload["metadata"]
    assert metadata["paper_trading_style"] == "trend"
    assert metadata["paper_trading_style_context"] == style_context
    assert metadata["paper_trading_style_indicators"]["bars_observed"] == 500
    evidence = metadata["paper_trading_style_evidence"]
    assert evidence["source"] == "analysis_context_snapshot"
    assert evidence["analysis_window_sessions"] == 500
    assert evidence["history_sufficient"] is True
    assert payload["horizon"] == "long"
    assert metadata["horizon_source"] == "paper_trading_style_default"
    assert metadata["paper_trading_horizon_source"] == "paper_trading_style_default"


def test_build_payload_uses_result_fallbacks_and_optional_catalysts() -> None:
    result = _result(confidence_level="低")
    result.dashboard = {
        "battle_plan": {
            "sniper_points": {"ideal_buy": "1700"},
            "action_checklist": ["等待回踩确认"],
        },
        "intelligence": {},
    }
    result.market_phase_summary = {"phase": "postmarket"}
    result.analysis_context_pack_overview = {"data_quality": {"level": "limited"}}

    payload = build_decision_signal_payload_from_report(
        result,
        context_snapshot=None,
        source_report_id=None,
        trace_id="trace-fallback",
        query_source="",
        report_type="simple",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert payload["market_phase"] == "postmarket"
    assert payload["data_quality_summary"] == {"level": "limited"}
    assert payload["entry_low"] == 1700.0
    assert "entry_high" not in payload
    assert payload["watch_conditions"] == ["等待回踩确认"]
    assert "catalyst_summary" not in payload
    assert payload["trigger_source"] == "system"
    assert payload["confidence"] == 0.4
    assert payload["metadata"]["holding_state"] == "unknown"


def test_build_payload_aligns_high_neutral_action_without_guardrail_to_buy() -> None:
    result = _result(
        sentiment_score=72,
        operation_advice="持有",
        decision_type="hold",
        action=None,
    )

    payload = build_decision_signal_payload_from_report(
        result,
        trace_id="trace-high-neutral",
        query_source="api",
        report_type="simple",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert payload["action"] == "buy"
    assert payload["action_label"] == "买入"
    assert payload["metadata"]["raw_action"] == "hold"
    assert payload["metadata"]["final_action"] == "buy"
    assert payload["metadata"]["action_adjustment_reason"] == "canonical_score_alignment"
    assert payload["metadata"]["score_scale"]["score_band"] == "60-79"


def test_build_payload_ignores_narrative_only_guardrail_reason() -> None:
    result = _result(
        sentiment_score=72,
        operation_advice="持有/观望待回踩",
        decision_type="hold",
        action=None,
    )

    payload = build_decision_signal_payload_from_report(
        result,
        trace_id="trace-high-neutral-guarded",
        query_source="api",
        report_type="simple",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert payload["action"] == "buy"
    assert payload["action_label"] == "买入"
    assert payload["metadata"]["raw_action"] == "watch"
    assert payload["metadata"]["final_action"] == "buy"
    assert "guardrail_reason" not in payload["metadata"]


def test_build_payload_uses_stability_calibration_raw_and_adjusted_scores() -> None:
    result = _result(
        sentiment_score=59,
        operation_advice="观望",
        decision_type="hold",
        action=None,
    )
    dashboard = result.dashboard or {}
    dashboard["decision_score_calibration"] = {
        "raw_score": 72,
        "adjusted_score": 59,
        "final_action": "watch",
        "guardrail_reason": "资金流较弱，按风险规则降级",
    }
    result.dashboard = dashboard

    payload = build_decision_signal_payload_from_report(
        result,
        trace_id="trace-stability-calibration",
        query_source="api",
        report_type="simple",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert payload["score"] == 59
    assert payload["metadata"]["raw_score"] == 72
    assert payload["metadata"]["adjusted_score"] == 59
    assert payload["metadata"]["score_scale"]["score_band"] == "40-59"
    assert payload["metadata"]["raw_action"] == "watch"
    assert payload["metadata"]["final_action"] == "watch"
    assert "guardrail_reason" not in payload["metadata"]


def test_build_payload_aligns_low_neutral_action_to_reduce() -> None:
    result = _result(
        sentiment_score=28,
        operation_advice="观望",
        decision_type="hold",
        action=None,
    )

    payload = build_decision_signal_payload_from_report(
        result,
        trace_id="trace-low-neutral",
        query_source="api",
        report_type="simple",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert payload["action"] == "reduce"
    assert payload["action_label"] == "减仓"
    assert payload["metadata"]["raw_action"] == "watch"
    assert payload["metadata"]["final_action"] == "reduce"
    assert payload["metadata"]["score_scale"]["score_band"] == "20-39"


def test_build_payload_records_empty_holding_state_from_explicit_portfolio_context() -> None:
    payload = build_decision_signal_payload_from_report(
        _result(),
        portfolio_context={"quantity": 0},
        trace_id="trace-empty-holding",
        query_source="api",
        report_type="simple",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert payload["metadata"]["holding_state"] == "empty"


def test_runtime_decision_signal_summary_is_not_serialized_by_analysis_result_to_dict() -> None:
    result = _result()
    setattr(result, "decision_signal_summary", {"action": "sell", "reason": "risk"})

    assert "decision_signal_summary" not in result.to_dict()


def test_build_payload_maps_secondary_only_entry_to_entry_high() -> None:
    result = _result()
    result.dashboard = {
        "battle_plan": {
            "sniper_points": {"secondary_buy": "次优买入点：1680元"},
        },
    }

    payload = build_decision_signal_payload_from_report(
        result,
        trace_id="trace-secondary-only",
        query_source="api",
        report_type="simple",
        profile_source=BUILD_PROFILE_SOURCE,
    )

    assert payload is not None
    assert "entry_low" not in payload
    assert payload["entry_high"] == 1680.0


def test_build_payload_reuses_shared_sniper_fallback_paths(isolated_db) -> None:
    result = _result()
    result.dashboard = {}
    result.raw_response = {
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "ideal_buy": "1690",
                    "secondary_buy": "1705",
                    "stop_loss": "1620",
                    "take_profit": "1880",
                }
            }
        }
    }

    payload = build_decision_signal_payload_from_report(
        result,
        trace_id="trace-raw-sniper",
        query_source="api",
        report_type="simple",
        profile_source=BUILD_PROFILE_SOURCE,
    )
    stored_points = isolated_db._extract_sniper_points(result)

    assert payload is not None
    assert stored_points == {
        "ideal_buy": 1690.0,
        "secondary_buy": 1705.0,
        "stop_loss": 1620.0,
        "take_profit": 1880.0,
    }
    assert payload["entry_low"] == 1690.0
    assert payload["entry_high"] == 1705.0
    assert payload["stop_loss"] == 1620.0
    assert payload["target_price"] == 1880.0


def test_build_payload_skips_ambiguous_action_non_stock_and_unknown_market() -> None:
    ambiguous = _result(operation_advice="买盘增强，继续观察", action=None)
    assert build_decision_signal_payload_from_report(
        ambiguous,
        trace_id="trace-1",
        query_source="api",
        report_type="simple",
        profile_source=BUILD_PROFILE_SOURCE,
    ) is None

    market_review = _result(operation_advice="买入", action="buy")
    assert build_decision_signal_payload_from_report(
        market_review,
        trace_id="trace-2",
        query_source="api",
        report_type="market_review",
        profile_source=BUILD_PROFILE_SOURCE,
    ) is None

    unknown_market = _result(code="UNKNOWN", operation_advice="买入", action="buy")
    assert build_decision_signal_payload_from_report(
        unknown_market,
        trace_id="trace-3",
        query_source="api",
        report_type="simple",
        profile_source=BUILD_PROFILE_SOURCE,
    ) is None


def test_extract_and_persist_reuses_service_dedup_and_sanitization(isolated_db) -> None:
    service = DecisionSignalService(db_manager=isolated_db)
    result = _result(
        analysis_summary="趋势确认 token=super-secret",
    )

    first = extract_and_persist_from_analysis_result(
        result,
        context_snapshot={"market_phase_summary": {"phase": "intraday"}},
        portfolio_context={"quantity": 10},
        source_report_id=901,
        trace_id="trace-901",
        query_source="api",
        report_type="full",
        profile_source="auto_default",
        service=service,
    )
    second = extract_and_persist_from_analysis_result(
        result,
        context_snapshot={"market_phase_summary": {"phase": "intraday"}},
        portfolio_context={"quantity": 10},
        source_report_id=901,
        trace_id="trace-901",
        query_source="api",
        report_type="full",
        profile_source="auto_default",
        service=service,
    )

    assert first is not None
    assert second is not None
    assert first["created"] is True
    assert second["created"] is False
    assert first["item"]["reason"] == "趋势确认 token=[REDACTED]"
    assert first["item"]["plan_quality"] == "complete"
    assert first["item"]["horizon"] == "intraday"
    assert first["item"]["expires_at"] is not None

    listed = service.list_signals(source_report_id=901)
    assert listed["total"] == 1
    persisted = listed["items"][0]
    assert persisted["source_report_id"] == 901
    assert persisted["metadata"]["holding_state"] == "holding"
    assert persisted["metadata"]["decision_profile"] == "balanced"
    assert persisted["metadata"]["profile_source"] == "auto_default"
    assert persisted["metadata"]["profile_policy_version"] == "decision-profile-v1"
    assert persisted["metadata"]["signal_generation_version"] == "legacy-report-extractor-v1"
    assert persisted["metadata"]["decision_signal_metadata_version"] == "decision-signal-metadata-v1"
    assert "scoring_version" not in persisted["metadata"]
    assert "scoring_breakdown" not in persisted["metadata"]
    assert persisted["reason"] == "趋势确认 token=[REDACTED]"
    assert persisted["entry_low"] == 1690.0
    assert persisted["entry_high"] == 1700.0


def test_extract_and_persist_reuses_stability_score_metadata(isolated_db) -> None:
    service = DecisionSignalService(db_manager=isolated_db)
    result = _result(
        sentiment_score=59,
        operation_advice="观望",
        decision_type="hold",
        action=None,
        confidence_level="中",
    )
    dashboard = result.dashboard or {}
    dashboard["decision_score_calibration"] = {
        "raw_score": 72,
        "adjusted_score": 59,
        "final_action": "watch",
        "guardrail_reason": "资金流较弱，按风险规则降级",
    }
    result.dashboard = dashboard

    created = extract_and_persist_from_analysis_result(
        result,
        context_snapshot={"market_phase_summary": {"phase": "intraday"}},
        portfolio_context={"quantity": 10},
        source_report_id=903,
        trace_id="trace-stability-persist",
        query_source="api",
        report_type="full",
        profile_source="auto_default",
        service=service,
    )

    assert created is not None
    persisted = service.list_signals(source_report_id=903)["items"][0]
    assert persisted["metadata"]["raw_score"] == 72
    assert persisted["metadata"]["adjusted_score"] == 59
    assert persisted["metadata"]["raw_action"] == "watch"
    assert persisted["metadata"]["final_action"] == "watch"
    assert "guardrail_reason" not in persisted["metadata"]


def test_extract_and_persist_writes_tw_signal(isolated_db) -> None:
    """End-to-end write-leg guard: a tw analysis must PERSIST a DecisionSignal
    through create_signal -> _normalize_market -> DB, not merely build the payload.

    Closes the silent-failure leg where _normalize_market("tw") raised ValueError
    inside extract_and_persist and was swallowed by its broad except -> return None,
    so every tw analysis produced no signal while jp/kr did.
    """
    service = DecisionSignalService(db_manager=isolated_db)
    result = _result(code="2330.TW", name="台积电")

    created = extract_and_persist_from_analysis_result(
        result,
        context_snapshot={"market_phase_summary": {"phase": "intraday"}},
        portfolio_context={"quantity": 10},
        source_report_id=2330,
        trace_id="trace-tw-persist",
        query_source="api",
        report_type="full",
        profile_source="auto_default",
        service=service,
    )

    assert created is not None
    assert created["created"] is True
    assert created["item"]["market"] == "tw"

    listed = service.list_signals(source_report_id=2330)
    assert listed["total"] == 1
    assert listed["items"][0]["market"] == "tw"


def test_extract_and_persist_missing_price_plan_does_not_fabricate_fields(isolated_db) -> None:
    service = DecisionSignalService(db_manager=isolated_db)
    result = _result()
    result.dashboard = {"battle_plan": {"sniper_points": {}}, "intelligence": {}}

    created = extract_and_persist_from_analysis_result(
        result,
        context_snapshot={"market_phase_summary": {"phase": "postmarket"}},
        source_report_id=902,
        trace_id="trace-902",
        query_source="schedule",
        report_type="simple",
        profile_source="auto_default",
        service=service,
    )

    assert created is not None
    item = created["item"]
    assert item["plan_quality"] == "minimal"
    assert item["horizon"] == "3d"
    assert item["expires_at"] is not None
    assert item["entry_low"] is None
    assert item["entry_high"] is None
    assert item["stop_loss"] is None
    assert item["target_price"] is None
