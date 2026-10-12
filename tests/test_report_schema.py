# -*- coding: utf-8 -*-
"""
===================================
Report Engine - Schema parsing and fallback tests
===================================

Tests for AnalysisReportSchema validation and analyzer fallback behavior.
"""

import copy
import json
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

# Mock litellm before importing analyzer (optional runtime dep)
try:
    import litellm  # noqa: F401
except ModuleNotFoundError:
    sys.modules["litellm"] = MagicMock()

from src.schemas.report_schema import AnalysisReportSchema
from src.analyzer import (
    GeminiAnalyzer,
    AnalysisResult,
    _AllModelsFailedError,
    _looks_like_structured_submission_error,
)
from src.services.decision_signal_extractor import build_decision_signal_payload_from_report
from src.config import Config
from src.llm.deepseek_structured import (
    DEEPSEEK_REPORT_MAX_OUTPUT_TOKENS,
    DeepSeekStructuredRoute,
    OfficialDeepSeekStructuredClient,
    extract_responses_text,
    resolve_official_deepseek_structured_route,
)


class TestAnalysisReportSchema(unittest.TestCase):
    """Schema parsing tests."""

    def test_integrity_retry_prompt_is_transport_neutral(self) -> None:
        """A retry prompt must work for Responses JSON Schema and Tool Call routes."""
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)

        prompt = analyzer._build_integrity_retry_prompt(
            "base prompt",
            '{"action":"buy"}',
            ["dashboard.battle_plan.sniper_points.stop_loss"],
            report_language="zh",
        )

        self.assertIn("结构化格式", prompt)
        self.assertNotIn("必需工具", prompt)
        self.assertNotIn("submit_analysis_report", prompt)

    def test_valid_dashboard_parses(self) -> None:
        """Valid LLM-like JSON parses successfully."""
        data = {
            "stock_name": "贵州茅台",
            "sentiment_score": 75,
            "trend_prediction": "看多",
            "operation_advice": "持有",
            "decision_type": "hold",
            "confidence_level": "中",
            "dashboard": {
                "core_conclusion": {"one_sentence": "持有观望"},
                "intelligence": {"risk_alerts": []},
                "battle_plan": {"sniper_points": {"stop_loss": "110元"}},
            },
            "analysis_summary": "基本面稳健",
        }
        schema = AnalysisReportSchema.model_validate(data)
        self.assertEqual(schema.stock_name, "贵州茅台")
        self.assertEqual(schema.sentiment_score, 75)
        self.assertIsNotNone(schema.dashboard)

    def test_schema_allows_optional_fields_missing(self) -> None:
        """Schema accepts minimal valid structure."""
        data = {
            "stock_name": "测试",
            "sentiment_score": 50,
            "trend_prediction": "震荡",
            "operation_advice": "观望",
        }
        schema = AnalysisReportSchema.model_validate(data)
        self.assertIsNone(schema.dashboard)
        self.assertIsNone(schema.analysis_summary)

    def test_schema_preserves_explicit_holding_risk_prices(self) -> None:
        schema = AnalysisReportSchema.model_validate({
            "position_actions": {
                "empty": {"action": "watch", "advice": "等待"},
                "holding": {
                    "action": "hold",
                    "advice": "继续持有并上移保护",
                    "stop_loss": 101.5,
                    "target_price": 128.0,
                },
            }
        })

        self.assertEqual(schema.position_actions.holding.stop_loss, 101.5)
        self.assertEqual(schema.position_actions.holding.target_price, 128.0)

    def test_schema_preserves_optional_exit_legs(self) -> None:
        schema = AnalysisReportSchema.model_validate({
            "dashboard": {
                "battle_plan": {
                    "sniper_points": {
                        "take_profit": "140",
                        "exit_leg_schema_version": "exit-legs-v1",
                        "exit_legs": [
                            {"leg_id": "tp1", "sequence": 1, "allocation_fraction": 0.5, "target_price": 130},
                            {"leg_id": "tp2", "sequence": 2, "allocation_fraction": 0.5, "target_price": 140},
                        ],
                    }
                }
            }
        })

        sniper = schema.dashboard.battle_plan.sniper_points
        self.assertEqual(sniper.exit_leg_schema_version, "exit-legs-v1")
        self.assertEqual(sniper.exit_legs[0]["leg_id"], "tp1")

    def test_tool_schema_requires_submission_version(self) -> None:
        tool = GeminiAnalyzer._analysis_report_tool()

        self.assertEqual(tool["function"]["name"], "submit_analysis_report")
        params = tool["function"]["parameters"]
        self.assertIn("report_schema_version", params["required"])
        self.assertIn("action", params["required"])
        self.assertIn("position_actions", params["required"])
        self.assertIn("confidence_level", params["required"])
        self.assertNotIn("stock_name", params["required"])
        self.assertNotIn("trend_prediction", params["required"])
        self.assertNotIn("operation_advice", params["required"])
        self.assertNotIn("decision_type", params["required"])
        self.assertNotIn("analysis_summary", params["required"])
        self.assertNotIn("news_summary", params["required"])
        self.assertNotIn("fundamental_analysis", params["required"])
        self.assertNotIn("dashboard", params["required"])
        phase_properties = (
            params["properties"]["dashboard"]["properties"]["phase_decision"]["properties"]
        )
        self.assertEqual(phase_properties["watch_conditions"]["minItems"], 1)
        self.assertEqual(
            phase_properties["entry_mode"]["enum"],
            ["none", "conditional", "immediate"],
        )
        self.assertEqual(
            params["properties"]["report_schema_version"]["const"],
            GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
        )

    def test_tool_schema_restricts_confidence_level_to_receive_side_labels(self) -> None:
        tool = GeminiAnalyzer._analysis_report_tool()
        confidence = tool["function"]["parameters"]["properties"]["confidence_level"]

        self.assertEqual(confidence["type"], "string")
        self.assertEqual(
            confidence["enum"],
            ["高", "中", "低", "high", "medium", "mid", "low"],
        )
        self.assertNotIn("中等偏低", confidence["enum"])

    def test_tool_schema_does_not_force_entry_prices_for_non_buy_actions(self) -> None:
        tool = GeminiAnalyzer._analysis_report_tool()
        sniper = (
            tool["function"]["parameters"]["properties"]["dashboard"]
            ["properties"]["battle_plan"]["properties"]["sniper_points"]
        )

        self.assertNotIn("required", sniper)

    def test_schema_rejects_nonpositive_holding_risk_prices(self) -> None:
        with self.assertRaises(Exception):
            AnalysisReportSchema.model_validate({
                "position_actions": {
                    "holding": {"action": "hold", "stop_loss": 0},
                }
            })

    def test_schema_accepts_phase_decision_and_defaults_lists(self) -> None:
        """Dashboard accepts the optional phase_decision contract."""
        data = {
            "stock_name": "贵州茅台",
            "sentiment_score": 70,
            "trend_prediction": "震荡",
            "operation_advice": "持有",
            "dashboard": {
                "core_conclusion": {"one_sentence": "等待确认"},
                "phase_decision": {
                    "phase_context": {"phase": "intraday", "market": "cn"},
                    "entry_mode": "conditional",
                    "action_window": "盘中跟踪",
                    "immediate_action": "等待确认",
                    "next_check_time": "14:30",
                    "confidence_reason": "数据质量可用",
                },
            },
        }

        schema = AnalysisReportSchema.model_validate(data)

        self.assertIsNotNone(schema.dashboard)
        phase_decision = schema.dashboard and schema.dashboard.phase_decision
        self.assertIsNotNone(phase_decision)
        if phase_decision:
            self.assertEqual(phase_decision.watch_conditions, [])
            self.assertEqual(phase_decision.data_limitations, [])
            self.assertEqual(phase_decision.phase_context["phase"], "intraday")
            self.assertEqual(phase_decision.entry_mode, "conditional")

    def test_schema_rejects_invalid_phase_entry_mode(self) -> None:
        with self.assertRaises(Exception):
            AnalysisReportSchema.model_validate({
                "dashboard": {"phase_decision": {"entry_mode": "wait_for_price"}},
            })

    def test_schema_coerces_non_trading_display_lists(self) -> None:
        schema = AnalysisReportSchema.model_validate({
            "dashboard": {
                "intelligence": {
                    "risk_alerts": "单条风险提示",
                    "positive_catalysts": {"unexpected": "shape"},
                },
                "battle_plan": {"action_checklist": "观察量能"},
                "phase_decision": {
                    "watch_conditions": "放量站稳",
                    "data_limitations": None,
                },
            },
            "limitations": "新闻缺失",
        })

        dashboard = schema.dashboard
        self.assertEqual(dashboard.intelligence.risk_alerts, ["单条风险提示"])
        self.assertEqual(dashboard.intelligence.positive_catalysts, [])
        self.assertEqual(dashboard.battle_plan.action_checklist, ["观察量能"])
        self.assertIsNone(dashboard.phase_decision.watch_conditions)
        self.assertEqual(dashboard.phase_decision.data_limitations, [])
        self.assertEqual(schema.limitations, ["新闻缺失"])

    def test_schema_allows_numeric_strings(self) -> None:
        """Schema accepts string values for numeric fields (LLM may return N/A)."""
        data = {
            "stock_name": "测试",
            "sentiment_score": 60,
            "trend_prediction": "看多",
            "operation_advice": "买入",
            "dashboard": {
                "data_perspective": {
                    "price_position": {
                        "current_price": "N/A",
                        "bias_ma5": "2.5",
                    }
                }
            },
        }
        schema = AnalysisReportSchema.model_validate(data)
        self.assertIsNotNone(schema.dashboard)
        pp = schema.dashboard and schema.dashboard.data_perspective and schema.dashboard.data_perspective.price_position
        self.assertIsNotNone(pp)
        if pp:
            self.assertEqual(pp.current_price, "N/A")
            self.assertEqual(pp.bias_ma5, "2.5")

    def test_schema_fails_on_invalid_sentiment_score(self) -> None:
        """Schema validation fails when sentiment_score out of range."""
        data = {
            "stock_name": "测试",
            "sentiment_score": 150,  # out of 0-100
            "trend_prediction": "看多",
            "operation_advice": "买入",
        }
        with self.assertRaises(Exception):
            AnalysisReportSchema.model_validate(data)


class TestAnalyzerSchemaFallback(unittest.TestCase):
    """Analyzer fallback when schema validation fails."""

    def test_parse_response_marks_schema_failures_non_actionable(self) -> None:
        """Schema-invalid provider output must not become a valid trading report."""
        analyzer = GeminiAnalyzer()
        response = json.dumps({
            "stock_name": "贵州茅台",
            "sentiment_score": 150,  # invalid for schema
            "trend_prediction": "看多",
            "operation_advice": "持有",
            "analysis_summary": "测试摘要",
            "dashboard": {"core_conclusion": {"one_sentence": "测试摘要"}},
        })
        result = analyzer._parse_response(response, "600519", "贵州茅台")
        self.assertIsInstance(result, AnalysisResult)
        self.assertEqual(result.code, "600519")
        self.assertFalse(result.success)
        self.assertIsNone(result.action)
        self.assertIsNone(result.action_label)
        self.assertEqual(result.decision_type, "failed")
        self.assertEqual(result.operation_advice, "分析失败")

    def test_parse_response_valid_json_succeeds(self) -> None:
        """Valid JSON produces correct AnalysisResult."""
        analyzer = GeminiAnalyzer()
        response = json.dumps({
            "stock_name": "贵州茅台",
            "sentiment_score": 72,
            "trend_prediction": "看多",
            "operation_advice": "持有",
            "decision_type": "hold",
            "confidence_level": "高",
            "analysis_summary": "技术面向好",
            "dashboard": {"core_conclusion": {"one_sentence": "技术面向好"}},
        })
        result = analyzer._parse_response(response, "600519", "股票600519")
        self.assertIsInstance(result, AnalysisResult)
        self.assertEqual(result.name, "贵州茅台")
        self.assertEqual(result.sentiment_score, 72)
        self.assertEqual(result.analysis_summary, "技术面向好")
        self.assertEqual(result.action, "buy")
        self.assertEqual(result.action_label, "买入")
        self.assertEqual(result.operation_advice, "买入")
        self.assertEqual(result.decision_type, "buy")

    def test_parse_response_preserves_explicit_action_in_raw_result(self) -> None:
        analyzer = GeminiAnalyzer()
        response = json.dumps({
            "stock_name": "贵州茅台",
            "sentiment_score": 58,
            "trend_prediction": "震荡",
            "operation_advice": "持有观察",
            "decision_type": "hold",
            "action": "watch",
            "analysis_summary": "等待确认",
            "dashboard": {"core_conclusion": {"one_sentence": "等待确认"}},
        })

        result = analyzer._parse_response(response, "600519", "股票600519")
        raw_result = result.to_dict()

        self.assertEqual(result.action, "watch")
        self.assertEqual(result.action_label, "观望")
        self.assertEqual(result.decision_type, "hold")
        self.assertEqual(raw_result["action"], "watch")
        self.assertEqual(raw_result["action_label"], "观望")

    def test_native_tool_schema_version_survives_result_serialization(self) -> None:
        result = AnalysisResult(
            code="600519",
            name="贵州茅台",
            sentiment_score=50,
            trend_prediction="震荡",
            operation_advice="观望",
            report_schema_version=GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
        )

        self.assertEqual(
            result.to_dict()["report_schema_version"],
            GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
        )

    def test_parse_response_keeps_unknown_dashboard_fields(self) -> None:
        analyzer = GeminiAnalyzer()
        response = json.dumps({
            "stock_name": "贵州茅台",
            "sentiment_score": 72,
            "trend_prediction": "看多",
            "operation_advice": "持有",
            "decision_type": "hold",
            "analysis_summary": "技术面向好",
            "dashboard": {
                "core_conclusion": {
                    "one_sentence": "先观察",
                    "signal_type": "🟡持有观望",
                },
                "decision_stability": {
                    "applied": True,
                    "reason": "回测验证",
                },
            },
        })
        result = analyzer._parse_response(response, "600519", "股票600519")
        self.assertEqual(result.dashboard["decision_stability"]["applied"], True)
        self.assertEqual(result.dashboard["decision_stability"]["reason"], "回测验证")

    def test_parse_response_repairs_single_json_candidate(self) -> None:
        analyzer = GeminiAnalyzer()
        response = """```json
{
  "stock_name": "贵州茅台",
  "sentiment_score": 68,
  "trend_prediction": "看多",
  "operation_advice": "持有",
  "analysis_summary": "继续观察",
  "dashboard": {"core_conclusion": {"one_sentence": "继续观察"}},
}
```"""

        result = analyzer._parse_response(response, "600519", "股票600519")

        self.assertTrue(result.success)
        self.assertEqual(result.name, "贵州茅台")
        self.assertEqual(result.sentiment_score, 68)

    def test_parse_response_accepts_single_generic_json_fence(self) -> None:
        analyzer = GeminiAnalyzer()
        response = """```
{
  "stock_name": "贵州茅台",
  "sentiment_score": 67,
  "trend_prediction": "看多",
  "operation_advice": "持有",
  "analysis_summary": "技术面向好"
  ,"dashboard": {"core_conclusion": {"one_sentence": "技术面向好"}}
}
```"""

        result = analyzer._parse_response(response, "600519", "股票600519")

        self.assertTrue(result.success)
        self.assertEqual(result.name, "贵州茅台")
        self.assertEqual(result.sentiment_score, 67)

    def test_parse_response_repairs_nested_single_json_candidate(self) -> None:
        analyzer = GeminiAnalyzer()
        response = """```json
{
  "stock_name": "贵州茅台",
  "sentiment_score": 69,
  "trend_prediction": "看多",
  "operation_advice": "持有",
  "analysis_summary": "继续观察",
  "dashboard": {"core_conclusion": {"one_sentence": "继续观察",},},
}
```"""

        result = analyzer._parse_response(response, "600519", "股票600519")

        self.assertTrue(result.success)
        self.assertEqual(result.sentiment_score, 69)
        self.assertEqual(result.dashboard["core_conclusion"]["one_sentence"], "继续观察")

    def test_validate_json_response_accepts_single_generic_json_fence(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        analyzer._validate_json_response("""```
{
  "stock_name": "贵州茅台",
  "sentiment_score": 66,
  "trend_prediction": "看多",
  "operation_advice": "持有",
  "analysis_summary": "技术面向好",
  "dashboard": {"core_conclusion": {"one_sentence": "技术面向好"}}
}
```""")

    def test_validate_json_response_accepts_single_json_fence(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        analyzer._validate_json_response("""```json
{
  "stock_name": "贵州茅台",
  "sentiment_score": 65,
  "trend_prediction": "看多",
  "operation_advice": "持有",
  "analysis_summary": "技术面向好",
  "dashboard": {"core_conclusion": {"one_sentence": "技术面向好"}}
}
```""")

    def test_validate_json_response_rejects_ambiguous_json_before_repair(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        with self.assertRaises(Exception) as context:
            analyzer._validate_json_response('{"sentiment_score": 70} {"sentiment_score": 80}')

        self.assertEqual(getattr(context.exception, "details", {}).get("reason"), "ambiguous_json")

    def test_validate_json_response_rejects_generic_fence_with_outside_text(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        with self.assertRaises(Exception) as context:
            analyzer._validate_json_response("""Here is the JSON:
```
{"sentiment_score": 70, "trend_prediction": "看多"}
```""")

        self.assertEqual(getattr(context.exception, "details", {}).get("reason"), "ambiguous_json")

    def test_validate_json_response_rejects_multiple_json_fences(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        with self.assertRaises(Exception) as context:
            analyzer._validate_json_response("""```json
{"sentiment_score": 70}
```
```json
{"sentiment_score": 80}
```""")

        self.assertEqual(getattr(context.exception, "details", {}).get("reason"), "ambiguous_json")

    def test_validate_json_response_rejects_non_json_language_fence(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        with self.assertRaises(Exception) as context:
            analyzer._validate_json_response("""```text
{"sentiment_score": 70, "trend_prediction": "看多"}
```""")

        self.assertEqual(getattr(context.exception, "details", {}).get("reason"), "ambiguous_json")

    def test_validate_json_response_rejects_missing_minimal_contract(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        with self.assertRaises(Exception) as context:
            analyzer._validate_json_response('{"stock_name": "贵州茅台"}')

        self.assertEqual(getattr(context.exception, "details", {}).get("reason"), "minimal_contract_failed")

    def test_validate_json_response_rejects_parser_unconstructable_sentiment(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        with self.assertRaises(Exception) as context:
            analyzer._validate_json_response(json.dumps({
                "stock_name": "贵州茅台",
                "sentiment_score": "not-a-number",
                "trend_prediction": "看多",
                "operation_advice": "持有",
                "analysis_summary": "测试摘要",
                "dashboard": {"core_conclusion": {"one_sentence": "测试"}},
            }))

        self.assertEqual(getattr(context.exception, "details", {}).get("reason"), "parser_contract_failed")

    def test_validate_json_response_allows_display_schema_drift_before_defaulting(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        base = {
            "stock_name": "牧原股份",
            "sentiment_score": 55,
            "trend_prediction": "震荡",
            "operation_advice": "持有",
            "analysis_summary": "格式漂移样例",
        }
        drift_cases = [
            {"stock_name": ["牧原股份"]},
            {"news_summary": {"headline": "展示字段对象漂移"}},
            {"fundamental_analysis": ["展示字段数组漂移"]},
            {"hot_topics": 123},
            {"dashboard": "展示块整体漂移"},
            {"dashboard": {"core_conclusion": "结论块漂移"}},
            {"dashboard": {"core_conclusion": {"one_sentence": ["结论字段漂移"]}}},
            {"dashboard": {"intelligence": "新闻形态漂移"}},
            {"dashboard": {"data_perspective": {"trend_status": {"trend_score": {"score": 55}}}}},
            {"dashboard": {"data_perspective": {"price_position": {"ma5": {"value": 39.5}}}}},
            {"dashboard": {"data_perspective": {"volume_analysis": {"turnover_rate": ["2.3%"]}}}},
            {"dashboard": {"data_perspective": {"chip_structure": {"concentration": {"level": "中"}}}}},
            {"dashboard": {"signal_attribution": {"technical_indicators": {"weight": 40}}}},
            {"dashboard": {"phase_decision": {"phase_context": "CN · 盘后"}}},
        ]

        for case in drift_cases:
            with self.subTest(case=case):
                payload = dict(base)
                payload.update(case)
                analyzer._validate_json_response(json.dumps(payload, ensure_ascii=False))

    def test_validate_json_response_rejects_native_trading_type_drift(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        base = {
            "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            "raw_score": 55,
            "adjusted_score": 55,
            "sentiment_score": 55,
            "action": "watch",
            "position_actions": {
                "empty": {"action": "watch"},
                "holding": {"action": "hold"},
            },
            "confidence_level": "中",
        }
        drift_cases = [
            {"raw_score": {"score": 55}},
            {"adjusted_score": ["55"]},
            {"sentiment_score": "not-a-number"},
            {"action": {"decision": "watch"}},
            {"position_actions": "watch"},
            {"position_actions": {"empty": {"action": "watch"}, "holding": {"action": {"bad": "hold"}}}},
            {"confidence_level": {"level": "中"}},
        ]

        for case in drift_cases:
            with self.subTest(case=case):
                payload = dict(base)
                payload.update(case)
                with self.assertRaises(Exception) as context:
                    analyzer._validate_json_response(json.dumps(payload, ensure_ascii=False))
                self.assertEqual(
                    getattr(context.exception, "details", {}).get("reason"),
                    "native_tool_core_contract_failed",
                )

    def test_explicit_confidence_label_preserves_explanation_without_changing_decision(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")
        base = {
            "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            "raw_score": 50, "adjusted_score": 50, "sentiment_score": 50, "action": "watch",
            "position_actions": {"empty": {"action": "watch"}, "holding": {"action": "hold"}},
            "confidence_level": "中等（技术面数据完整，但新闻面本次未检索）",
        }
        normalized = analyzer._validate_analysis_minimal_contract(base)
        self.assertEqual(normalized['confidence_level'], '中')
        self.assertEqual(normalized['action'], 'watch')
        self.assertEqual(normalized['adjusted_score'], 50)
        self.assertIn(base['confidence_level'], normalized['limitations'][-1])
        self.assertEqual(base['confidence_level'], '中等（技术面数据完整，但新闻面本次未检索）')
        for invalid in (None, '中高', '非常确定', '大约中等', {'level':'中'}):
            with self.subTest(invalid=invalid), self.assertRaises(Exception):
                analyzer._validate_analysis_minimal_contract({**base, 'confidence_level':invalid})

    def test_validate_json_response_allows_legacy_nested_display_type_drift(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        analyzer._validate_json_response(json.dumps({
            "stock_name": "牧原股份",
            "sentiment_score": 55,
            "trend_prediction": "震荡",
            "operation_advice": "持有",
            "analysis_summary": "格式漂移样例",
            "dashboard": {
                "phase_decision": {
                    "phase_context": "CN · 盘后",
                },
                "intelligence": "新闻形态漂移",
            },
        }, ensure_ascii=False))

    def test_validate_json_response_allows_native_non_trading_type_drift(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        response = json.dumps({
            "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            "raw_score": 72,
            "adjusted_score": 72,
            "sentiment_score": 72,
            "stock_name": {"name": "牧原股份"},
            "trend_prediction": ["看多"],
            "operation_advice": ["买入"],
            "action": "buy",
            "position_actions": {
                "empty": {"action": "buy", "advice": {"text": "分批买入"}},
                "holding": {"action": "hold", "advice": ["继续持有"]},
            },
            "confidence_level": "中",
            "analysis_summary": {"summary": "展示字段对象漂移"},
            "news_summary": {"headline": "展示字段对象漂移"},
            "fundamental_analysis": ["展示字段数组漂移"],
            "hot_topics": 123,
            "search_performed": {"provider": "news"},
            "dashboard": {
                "core_conclusion": {"one_sentence": ["展示字段数组漂移"]},
                "data_perspective": {
                    "trend_status": {"trend_score": {"score": 72}},
                    "price_position": {"ma5": {"value": 39.5}},
                    "volume_analysis": {"turnover_rate": ["2.3%"]},
                    "chip_structure": {"concentration": {"level": "中"}},
                },
                "intelligence": {
                    "latest_news": {"headline": "展示字段对象漂移"},
                    "risk_alerts": {"unexpected": "shape"},
                    "positive_catalysts": ["催化"],
                },
                "battle_plan": {
                    "action_checklist": {"step": "展示字段对象漂移"},
                    "sniper_points": {
                        "ideal_buy": 39.5,
                        "secondary_buy": 39.7,
                        "stop_loss": 38.8,
                        "take_profit": 41.3,
                        "final_target": 41.3,
                        "entry_leg_schema_version": "entry-legs-v1",
                        "entry_legs": [
                            {
                                "leg_id": "initial",
                                "role": "initial",
                                "sequence": 1,
                                "allocation_fraction": 1.0,
                                "max_executions": 1,
                                "price_rules": [{"type": "close_between", "low": 39.5, "high": 39.7}],
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
                                "target_price": 41.3,
                            }
                        ],
                    },
                },
                "phase_decision": {
                    "action_window": "下个交易日盘中跟踪",
                    "immediate_action": "等待确认",
                    "watch_conditions": ["放量不破入场区间"],
                    "next_check_time": "下个交易日 09:35",
                    "confidence_reason": {"why": "展示字段对象漂移"},
                    "data_limitations": None,
                },
                "signal_attribution": {"technical_indicators": {"weight": 40}},
            },
        }, ensure_ascii=False)

        analyzer._validate_json_response(response)
        result = GeminiAnalyzer()._parse_response(response, "002714", "牧原股份")

        self.assertTrue(result.success)
        self.assertEqual(result.name, "牧原股份")
        self.assertEqual(result.action, "buy")
        self.assertIsInstance(result.trend_prediction, str)
        self.assertIsInstance(result.operation_advice, str)
        self.assertIsInstance(result.analysis_summary, str)
        self.assertIsInstance(result.news_summary, str)
        self.assertIsInstance(result.fundamental_analysis, str)
        self.assertIsInstance(result.hot_topics, str)

    def test_native_watch_all_nested_display_drift_does_not_block_report(self) -> None:
        analyzer = GeminiAnalyzer()
        base_payload = {
            "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            "raw_score": 55,
            "adjusted_score": 55,
            "sentiment_score": 55,
            "trend_prediction": "震荡",
            "operation_advice": "观望",
            "decision_type": "hold",
            "action": "watch",
            "position_actions": {
                "empty": {"action": "watch"},
                "holding": {"action": "hold"},
            },
            "confidence_level": "中",
            "dashboard": {
                "core_conclusion": {},
                "data_perspective": {},
                "intelligence": {},
                "battle_plan": {},
                "phase_decision": {},
                "signal_attribution": {},
            },
        }
        drift_cases = [
            ("dashboard.core_conclusion.position_advice", "非对象"),
            ("dashboard.data_perspective.trend_status", "非对象"),
            ("dashboard.data_perspective.price_position", "非对象"),
            ("dashboard.data_perspective.volume_analysis", "非对象"),
            ("dashboard.data_perspective.chip_structure", "非对象"),
            ("dashboard.data_perspective.trend_status", {"is_bullish": {"bad": True}}),
            ("dashboard.battle_plan.sniper_points", "非对象"),
            ("dashboard.battle_plan.position_strategy", "非对象"),
            ("dashboard.battle_plan.action_checklist", {"step": "观察"}),
            ("dashboard.intelligence", "非对象"),
            ("dashboard.signal_attribution", "非对象"),
            ("dashboard.signal_attribution", {"technical_indicators": {"weight": 50}}),
        ]

        for path, value in drift_cases:
            with self.subTest(path=path, value=value):
                payload = copy.deepcopy(base_payload)
                target = payload
                parts = path.split(".")
                for part in parts[:-1]:
                    target = target.setdefault(part, {})
                target[parts[-1]] = value

                analyzer._validate_json_response(json.dumps(payload, ensure_ascii=False))
                result = analyzer._parse_response(
                    json.dumps(payload, ensure_ascii=False),
                    "002714",
                    "牧原股份",
                )

                self.assertTrue(result.success)
                self.assertEqual(result.action, "watch")

    def test_native_display_drift_validate_parse_and_signal_path_is_safe(self) -> None:
        analyzer = GeminiAnalyzer()
        payload = {
            "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            "stock_name": {"bad": "牧原股份"},
            "raw_score": 72,
            "adjusted_score": 72,
            "sentiment_score": 72,
            "trend_prediction": ["看多"],
            "operation_advice": {"advice": "买入"},
            "decision_type": ["buy"],
            "action": "buy",
            "position_actions": {
                "empty": {"action": "buy", "advice": {"text": "分批买入"}},
                "holding": {"action": "hold", "advice": ["继续持有"]},
            },
            "confidence_level": "中",
            "search_performed": {"unexpected": True},
            "analysis_summary": {"summary": "展示字段对象漂移"},
            "key_points": ["展示字段数组漂移"],
            "risk_warning": {"risk": "展示字段对象漂移"},
            "buy_reason": ["展示字段数组漂移"],
            "trend_analysis": {"text": "展示字段对象漂移"},
            "short_term_outlook": ["展示字段数组漂移"],
            "medium_term_outlook": {"text": "展示字段对象漂移"},
            "technical_analysis": ["展示字段数组漂移"],
            "ma_analysis": {"text": "展示字段对象漂移"},
            "volume_analysis": ["展示字段数组漂移"],
            "pattern_analysis": {"text": "展示字段对象漂移"},
            "fundamental_analysis": ["展示字段数组漂移"],
            "sector_position": {"text": "展示字段对象漂移"},
            "company_highlights": ["展示字段数组漂移"],
            "news_summary": {"headline": "展示字段对象漂移"},
            "market_sentiment": ["展示字段数组漂移"],
            "hot_topics": 123,
            "data_sources": {"source": "展示字段对象漂移"},
            "dashboard": {
                "core_conclusion": {"one_sentence": ["展示字段数组漂移"]},
                "data_perspective": {
                    "trend_status": {"ma_alignment": {"text": "多头"}, "trend_score": {"score": 72}},
                    "price_position": {"ma5": {"value": 39.5}, "bias_status": ["安全"]},
                    "volume_analysis": {"turnover_rate": ["2.3%"], "volume_meaning": {"text": "温和放量"}},
                    "chip_structure": {"concentration": {"level": "中"}, "chip_health": ["一般"]},
                },
                "intelligence": {
                    "latest_news": {"headline": "展示字段对象漂移"},
                    "risk_alerts": {"unexpected": "shape"},
                    "positive_catalysts": "单条催化",
                    "sentiment_summary": ["展示字段数组漂移"],
                },
                "battle_plan": {
                    "position_strategy": {
                        "suggested_position": {"text": "轻仓"},
                        "entry_plan": ["分批"],
                        "risk_control": {"text": "止损"},
                    },
                    "action_checklist": {"step": "展示字段对象漂移"},
                    "sniper_points": {
                        "ideal_buy": 39.5,
                        "secondary_buy": 39.7,
                        "stop_loss": 38.8,
                        "take_profit": 41.3,
                        "final_target": 41.3,
                        "entry_leg_schema_version": "entry-legs-v1",
                        "entry_legs": [
                            {
                                "leg_id": "initial",
                                "role": "initial",
                                "sequence": 1,
                                "allocation_fraction": 1.0,
                                "max_executions": 1,
                                "price_rules": [{"type": "close_between", "low": 39.5, "high": 39.7}],
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
                                "target_price": 41.3,
                            }
                        ],
                    },
                },
                "phase_decision": {
                    "phase_context": "CN · 盘后",
                    "entry_mode": "conditional",
                    "action_window": "下个交易日盘中跟踪",
                    "immediate_action": "等待确认",
                    "watch_conditions": ["放量不破入场区间"],
                    "next_check_time": "下个交易日 09:35",
                    "confidence_reason": {"why": "展示字段对象漂移"},
                    "data_limitations": {"unexpected": "shape"},
                },
                "signal_attribution": {
                    "technical_indicators": {"weight": 40},
                    "strongest_bullish_signal": ["放量"],
                },
            },
        }

        normalized = analyzer._validate_analysis_minimal_contract(dict(payload))
        result = analyzer._parse_response(json.dumps(payload, ensure_ascii=False), "002714", "股票002714")

        self.assertIsInstance(normalized["stock_name"], str)
        self.assertTrue(result.success)
        self.assertIsInstance(result.name, str)
        self.assertIsInstance(result.trend_prediction, str)
        self.assertIsInstance(result.operation_advice, str)
        self.assertIsInstance(result.decision_type, str)
        self.assertIsInstance(result.news_summary, str)
        self.assertIsInstance(result.fundamental_analysis, str)
        self.assertFalse(result.search_performed)
        self.assertEqual(result.action, "buy")
        self.assertEqual(result.raw_score, 72)
        self.assertEqual(result.adjusted_score, 72)
        self.assertEqual(result.position_actions["empty"]["action"], "buy")

        payload_signal = build_decision_signal_payload_from_report(
            result,
            trace_id="trace-native-display-drift",
            query_source="api",
            report_type="full",
            profile_source="legacy_unknown",
        )

        self.assertIsNotNone(payload_signal)
        self.assertEqual(payload_signal["action"], "buy")
        self.assertEqual(payload_signal["entry_low"], 39.5)
        self.assertEqual(payload_signal["entry_high"], 39.7)
        self.assertEqual(payload_signal["stop_loss"], 38.8)
        self.assertEqual(payload_signal["target_price"], 41.3)
        self.assertEqual(payload_signal["metadata"]["entry_mode"], "conditional")

    def test_native_buy_execution_field_type_drift_blocks_signal_only(self) -> None:
        analyzer = GeminiAnalyzer()
        payload = {
            "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            "raw_score": 72,
            "adjusted_score": 72,
            "sentiment_score": 72,
            "trend_prediction": "看多",
            "operation_advice": "买入",
            "decision_type": "buy",
            "action": "buy",
            "position_actions": {
                "empty": {"action": "buy"},
                "holding": {"action": "hold"},
            },
            "confidence_level": "中",
            "dashboard": {
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": 39.5,
                        "secondary_buy": 39.7,
                        "stop_loss": {"value": 38.8},
                        "take_profit": 41.3,
                        "final_target": 41.3,
                        "entry_leg_schema_version": "entry-legs-v1",
                        "entry_legs": [
                            {
                                "leg_id": "initial",
                                "role": "initial",
                                "sequence": 1,
                                "allocation_fraction": 1.0,
                                "max_executions": 1,
                                "price_rules": [{"type": "close_between", "low": 39.5, "high": 39.7}],
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
                                "target_price": 41.3,
                            }
                        ],
                    }
                },
                "phase_decision": {
                    "action_window": {"text": "下个交易日盘中跟踪"},
                    "immediate_action": "等待确认",
                    "watch_conditions": ["放量不破入场区间"],
                    "next_check_time": "下个交易日 09:35",
                },
            },
        }

        analyzer._validate_json_response(json.dumps(payload, ensure_ascii=False))
        result = analyzer._parse_response(json.dumps(payload, ensure_ascii=False), "002714", "牧原股份")
        ok, missing = analyzer._check_content_integrity(result)
        payload_signal = build_decision_signal_payload_from_report(
            result,
            trace_id="trace-native-exec-drift",
            query_source="api",
            report_type="full",
            profile_source="legacy_unknown",
        )

        self.assertTrue(result.success)
        self.assertFalse(ok)
        self.assertIn("dashboard.battle_plan.sniper_points.stop_loss", missing)
        self.assertIn("dashboard.phase_decision.action_window", missing)
        self.assertIsNone(payload_signal)

    def test_native_buy_watch_conditions_must_be_nonempty_string_list_for_signal(self) -> None:
        analyzer = GeminiAnalyzer()
        base_payload = {
            "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            "raw_score": 72,
            "adjusted_score": 72,
            "sentiment_score": 72,
            "trend_prediction": "看多",
            "operation_advice": "买入",
            "decision_type": "buy",
            "action": "buy",
            "position_actions": {
                "empty": {"action": "buy"},
                "holding": {"action": "hold"},
            },
            "confidence_level": "中",
            "dashboard": {
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": 39.5,
                        "secondary_buy": 39.7,
                        "stop_loss": 38.8,
                        "take_profit": 41.3,
                        "final_target": 41.3,
                        "entry_leg_schema_version": "entry-legs-v1",
                        "entry_legs": [
                            {
                                "leg_id": "initial",
                                "role": "initial",
                                "sequence": 1,
                                "allocation_fraction": 1.0,
                                "max_executions": 1,
                                "price_rules": [{"type": "close_between", "low": 39.5, "high": 39.7}],
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
                                "target_price": 41.3,
                            }
                        ],
                    }
                },
                "phase_decision": {
                    "entry_mode": "conditional",
                    "action_window": "下个交易日盘中跟踪",
                    "immediate_action": "等待确认",
                    "watch_conditions": ["放量不破入场区间"],
                    "next_check_time": "下个交易日 09:35",
                },
            },
        }

        valid_result = analyzer._parse_response(
            json.dumps(base_payload, ensure_ascii=False),
            "002714",
            "牧原股份",
        )
        valid_signal = build_decision_signal_payload_from_report(
            valid_result,
            trace_id="trace-native-watch-conditions-valid",
            query_source="api",
            report_type="full",
            profile_source="legacy_unknown",
        )
        self.assertIsNotNone(valid_signal)
        self.assertEqual(valid_signal["metadata"]["entry_mode"], "conditional")

        invalid_cases = [
            {"unexpected": "shape"},
            "放量不破入场区间",
            None,
            [],
            ["   "],
            ["放量不破入场区间", 123],
        ]
        for invalid_watch_conditions in invalid_cases:
            with self.subTest(watch_conditions=invalid_watch_conditions):
                payload = copy.deepcopy(base_payload)
                payload["dashboard"]["phase_decision"]["watch_conditions"] = invalid_watch_conditions

                analyzer._validate_json_response(json.dumps(payload, ensure_ascii=False))
                result = analyzer._parse_response(
                    json.dumps(payload, ensure_ascii=False),
                    "002714",
                    "牧原股份",
                )
                ok, missing = analyzer._check_content_integrity(result)
                signal = build_decision_signal_payload_from_report(
                    result,
                    trace_id="trace-native-watch-conditions-invalid",
                    query_source="api",
                    report_type="full",
                    profile_source="legacy_unknown",
                )

                self.assertTrue(result.success)
                self.assertFalse(ok)
                self.assertIn("dashboard.phase_decision.watch_conditions", missing)
                self.assertIsNone(signal)

    def test_validate_json_response_accepts_native_core_without_display_fields(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        analyzer._validate_json_response(json.dumps({
            "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            "raw_score": 55,
            "adjusted_score": 55,
            "sentiment_score": 55,
            "action": "watch",
            "position_actions": {
                "empty": {"action": "watch"},
                "holding": {"action": "hold"},
            },
            "confidence_level": "中",
        }, ensure_ascii=False))

    def test_validate_json_response_rejects_native_missing_trading_core_fields(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._config_override = SimpleNamespace(generation_backend="litellm")

        with self.assertRaises(Exception) as context:
            analyzer._validate_json_response(json.dumps({
                "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
                "raw_score": 55,
                "adjusted_score": 55,
                "sentiment_score": 55,
                "action": "buy",
                "position_actions": {
                    "empty": {"action": "buy"},
                    "holding": {"action": "hold"},
                },
                "confidence_level": "非常确定",
            }, ensure_ascii=False))

        self.assertEqual(
            getattr(context.exception, "details", {}).get("reason"),
            "native_tool_core_contract_failed",
        )

    def test_extract_required_tool_arguments_accepts_exact_single_tool_call(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        response = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        tool_calls=[
                            SimpleNamespace(
                                function=SimpleNamespace(
                                    name="submit_analysis_report",
                                    arguments=json.dumps(
                                        {
                                            "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
                                            "stock_name": "牧原股份",
                                        },
                                        ensure_ascii=False,
                                    ),
                                )
                            )
                        ]
                    )
                )
            ]
        )

        text = analyzer._extract_required_tool_arguments(response, "submit_analysis_report")

        self.assertEqual(json.loads(text)["stock_name"], "牧原股份")

    def test_extract_required_tool_arguments_rejects_plain_text_or_wrong_version(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)

        with self.assertRaises(ValueError):
            analyzer._extract_required_tool_arguments(SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="{}"))]), "submit_analysis_report")

        response = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        tool_calls=[
                            SimpleNamespace(
                                function=SimpleNamespace(
                                    name="submit_analysis_report",
                                    arguments=json.dumps({"report_schema_version": "legacy-json"}),
                                )
                            )
                        ]
                    )
                )
            ]
        )
        with self.assertRaises(ValueError) as context:
            analyzer._extract_required_tool_arguments(response, "submit_analysis_report")
        self.assertEqual(str(context.exception), "tool_call_schema_version_mismatch")

    def test_deepseek_thinking_route_omits_named_tool_choice(self) -> None:
        self.assertIsNone(
            GeminiAnalyzer._litellm_tool_choice_for_model("deepseek/deepseek-v4-pro", "submit_analysis_report")
        )
        self.assertEqual(
            GeminiAnalyzer._litellm_tool_choice_for_model(
                "deepseek/deepseek-v4-pro",
                "submit_analysis_report",
                force_deepseek_non_thinking=True,
            ),
            {"type": "function", "function": {"name": "submit_analysis_report"}},
        )
        self.assertEqual(
            GeminiAnalyzer._litellm_tool_choice_for_model("anthropic/claude-sonnet-5", "submit_analysis_report"),
            {"type": "function", "function": {"name": "submit_analysis_report"}},
        )

    @staticmethod
    def _native_report_payload(**overrides):
        payload = {
            "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            "stock_name": "测试标的",
            "raw_score": 72,
            "adjusted_score": 72,
            "sentiment_score": 72,
            "trend_prediction": "看多",
            "operation_advice": "买入",
            "decision_type": "buy",
            "action": "buy",
            "confidence_level": "中",
            "position_actions": {
                "empty": {"action": "buy", "advice": "轻仓试多"},
                "holding": {"action": "hold", "advice": "持有观察", "stop_loss": 9.5, "target_price": 12.0},
            },
            "dashboard": {
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": 10.0,
                        "secondary_buy": 9.8,
                        "stop_loss": 9.5,
                        "take_profit": 12.0,
                    }
                },
                "phase_decision": {
                    "action_window": "next_session",
                    "immediate_action": "等待确认",
                    "watch_conditions": ["放量突破 10.10"],
                    "next_check_time": "09:35",
                },
            },
            "analysis_summary": "结构化报告",
        }
        payload.update(overrides)
        return payload

    def test_official_deepseek_report_uses_responses_schema_not_tool_call(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="deepseek/deepseek-v4-pro",
                litellm_fallback_models=["anthropic/claude-sonnet-5"],
                llm_model_list=[],
                deepseek_api_keys=["test-deepseek-key"],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=False)
        analyzer._dispatch_litellm_completion = MagicMock()
        response = SimpleNamespace(
            output_text=json.dumps(self._native_report_payload(), ensure_ascii=False),
            usage=SimpleNamespace(input_tokens=10, output_tokens=20, total_tokens=30),
        )

        with patch(
            "src.analyzer.OfficialDeepSeekStructuredClient.create_structured_report",
            return_value=response,
        ) as create_responses:
            text, model, usage = analyzer._call_litellm_impl(
                "prompt",
                {"max_output_tokens": 128, "temperature": 0.7},
                response_validator=analyzer._validate_json_response,
                structured_tool=analyzer._analysis_report_tool(),
            )

        kwargs = create_responses.call_args.kwargs
        self.assertEqual(kwargs["messages"][0]["role"], "system")
        self.assertEqual(
            kwargs["schema"]["properties"]["report_schema_version"]["const"],
            GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
        )
        self.assertEqual(
            kwargs["schema"]["properties"]["confidence_level"]["enum"],
            ["高", "中", "低", "high", "medium", "mid", "low"],
        )
        self.assertEqual(kwargs["max_output_tokens"], DEEPSEEK_REPORT_MAX_OUTPUT_TOKENS)
        self.assertEqual(kwargs["temperature"], 0.7)
        self.assertEqual(model, "deepseek/deepseek-v4-pro")
        self.assertEqual(json.loads(text)["action"], "buy")
        self.assertEqual(usage["provider"], "deepseek")
        analyzer._dispatch_litellm_completion.assert_not_called()

    def test_official_deepseek_empty_responses_uses_one_same_model_json_fallback(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="deepseek/deepseek-v4-pro",
                litellm_fallback_models=["anthropic/claude-sonnet-5"],
                llm_model_list=[],
                deepseek_api_keys=["test-deepseek-key"],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=False)
        empty_response = SimpleNamespace(output_text="", usage={"input_tokens": 1, "output_tokens": 0})
        chat_response = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content=json.dumps(self._native_report_payload(), ensure_ascii=False)
                    )
                )
            ],
            usage={"prompt_tokens": 11, "completion_tokens": 22, "total_tokens": 33},
        )

        with patch(
            "src.analyzer.OfficialDeepSeekStructuredClient.create_structured_report",
            return_value=empty_response,
        ) as create_responses, patch(
            "src.analyzer.OfficialDeepSeekStructuredClient.create_json_fallback",
            return_value=chat_response,
        ) as create_fallback:
            text, model, usage = analyzer._call_litellm_impl(
                "prompt without magic word",
                {"max_output_tokens": 128, "temperature": 0.7},
                response_validator=analyzer._validate_json_response,
                structured_tool=analyzer._analysis_report_tool(),
            )

        create_responses.assert_called_once()
        create_fallback.assert_called_once()
        fallback_kwargs = create_fallback.call_args.kwargs
        self.assertEqual(fallback_kwargs["messages"][1]["content"].count("JSON"), 1)
        self.assertEqual(model, "deepseek/deepseek-v4-pro")
        self.assertEqual(json.loads(text)["report_schema_version"], GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION)
        self.assertEqual(usage["provider"], "deepseek")

    def test_official_deepseek_nonempty_invalid_json_fails_closed_without_chat_retry(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="deepseek/deepseek-v4-pro",
                litellm_fallback_models=["anthropic/claude-sonnet-5"],
                llm_model_list=[],
                deepseek_api_keys=["test-deepseek-key"],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=False)
        analyzer._dispatch_litellm_completion = MagicMock()
        response = SimpleNamespace(
            status="completed",
            output_text="{not valid json",
            usage=SimpleNamespace(input_tokens=10, output_tokens=20, total_tokens=30),
        )

        with patch(
            "src.analyzer.OfficialDeepSeekStructuredClient.create_structured_report",
            return_value=response,
        ) as create_responses, patch(
            "src.analyzer.OfficialDeepSeekStructuredClient.create_json_fallback",
        ) as create_fallback:
            with self.assertRaises(_AllModelsFailedError) as context:
                analyzer._call_litellm_impl(
                    "prompt",
                    {"max_output_tokens": 128, "temperature": 0.7},
                    response_validator=analyzer._validate_json_response,
                    structured_tool=analyzer._analysis_report_tool(),
                )

        create_responses.assert_called_once()
        create_fallback.assert_not_called()
        analyzer._dispatch_litellm_completion.assert_not_called()
        self.assertEqual(context.exception.last_error_kind, "deepseek_structured_output")

    def test_official_deepseek_token_limited_response_reuses_official_max_for_same_model_fallback(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="deepseek/deepseek-v4-pro",
                litellm_fallback_models=["anthropic/claude-sonnet-5"],
                llm_model_list=[],
                deepseek_api_keys=["test-deepseek-key"],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=False)
        analyzer._dispatch_litellm_completion = MagicMock()
        incomplete_response = SimpleNamespace(
            status="incomplete",
            incomplete_details=SimpleNamespace(reason="max_output_tokens"),
            output_text='{"report_schema_version":"analysis-report-tool-v1"',
            usage=SimpleNamespace(
                input_tokens=9000,
                output_tokens=DEEPSEEK_REPORT_MAX_OUTPUT_TOKENS,
                output_tokens_details=SimpleNamespace(reasoning_tokens=14000),
            ),
        )
        chat_response = SimpleNamespace(
            choices=[SimpleNamespace(
                finish_reason="stop",
                message=SimpleNamespace(content=json.dumps(self._native_report_payload(), ensure_ascii=False)),
            )],
            usage={"prompt_tokens": 9000, "completion_tokens": 4000, "total_tokens": 13000},
        )

        with patch(
            "src.analyzer.OfficialDeepSeekStructuredClient.create_structured_report",
            return_value=incomplete_response,
        ) as create_responses, patch(
            "src.analyzer.OfficialDeepSeekStructuredClient.create_json_fallback",
            return_value=chat_response,
        ) as create_fallback:
            text, model, _usage = analyzer._call_litellm_impl(
                "prompt",
                {"max_output_tokens": 8192, "temperature": 0.7},
                response_validator=analyzer._validate_json_response,
                structured_tool=analyzer._analysis_report_tool(),
            )

        self.assertEqual(
            create_responses.call_args.kwargs["max_output_tokens"],
            DEEPSEEK_REPORT_MAX_OUTPUT_TOKENS,
        )
        self.assertEqual(
            create_fallback.call_args.kwargs["max_tokens"],
            DEEPSEEK_REPORT_MAX_OUTPUT_TOKENS,
        )
        self.assertEqual(model, "deepseek/deepseek-v4-pro")
        self.assertEqual(json.loads(text)["action"], "buy")
        analyzer._dispatch_litellm_completion.assert_not_called()

    def test_official_deepseek_content_filter_fails_closed_without_fallback(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="deepseek/deepseek-v4-pro",
                llm_model_list=[],
                deepseek_api_keys=["test-deepseek-key"],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=False)
        response = SimpleNamespace(
            status="incomplete",
            incomplete_details=SimpleNamespace(reason="content_filter"),
            output_text="",
            usage=SimpleNamespace(input_tokens=10, output_tokens=0),
        )

        with patch(
            "src.analyzer.OfficialDeepSeekStructuredClient.create_structured_report",
            return_value=response,
        ), patch(
            "src.analyzer.OfficialDeepSeekStructuredClient.create_json_fallback",
        ) as create_fallback:
            with self.assertRaises(_AllModelsFailedError) as context:
                analyzer._call_litellm_impl(
                    "prompt",
                    {"max_output_tokens": 8192, "temperature": 0.7},
                    response_validator=analyzer._validate_json_response,
                    structured_tool=analyzer._analysis_report_tool(),
                )

        create_fallback.assert_not_called()
        self.assertEqual(context.exception.last_error_kind, "content_filter")

    def test_official_deepseek_unknown_incomplete_reason_fails_closed_without_fallback(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="deepseek/deepseek-v4-pro",
                llm_model_list=[],
                deepseek_api_keys=["test-deepseek-key"],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=False)
        response = SimpleNamespace(
            status="incomplete",
            incomplete_details=SimpleNamespace(reason="unknown"),
            output_text='{"report_schema_version":"analysis-report-tool-v1"',
            usage=SimpleNamespace(input_tokens=10, output_tokens=100),
        )

        with patch(
            "src.analyzer.OfficialDeepSeekStructuredClient.create_structured_report",
            return_value=response,
        ), patch(
            "src.analyzer.OfficialDeepSeekStructuredClient.create_json_fallback",
        ) as create_fallback:
            with self.assertRaises(_AllModelsFailedError) as context:
                analyzer._call_litellm_impl(
                    "prompt",
                    {"max_output_tokens": 8192, "temperature": 0.7},
                    response_validator=analyzer._validate_json_response,
                    structured_tool=analyzer._analysis_report_tool(),
                )

        create_fallback.assert_not_called()
        self.assertEqual(context.exception.last_error_kind, "transport_provider")
        self.assertIn("incomplete for an unsupported reason: unknown", str(context.exception))

    def test_extract_responses_text_never_includes_reasoning_items(self) -> None:
        response = SimpleNamespace(output=[
            SimpleNamespace(
                type="reasoning",
                content=[SimpleNamespace(type="reasoning_text", text="private reasoning")],
            ),
            SimpleNamespace(
                type="message",
                content=[SimpleNamespace(type="output_text", text='{"action":"watch"}')],
            ),
        ])

        self.assertEqual(extract_responses_text(response), '{"action":"watch"}')

    def test_legacy_deepseek_chat_is_not_sent_to_official_responses(self) -> None:
        cfg = Config(
            litellm_model="deepseek/deepseek-chat",
            litellm_fallback_models=[],
            llm_model_list=[],
            deepseek_api_keys=["test-deepseek-key"],
        )
        self.assertIsNone(resolve_official_deepseek_structured_route("deepseek/deepseek-chat", cfg))

        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(return_value=cfg)
        analyzer._has_channel_config = MagicMock(return_value=False)
        analyzer._dispatch_litellm_completion = MagicMock(
            return_value=SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            tool_calls=[
                                SimpleNamespace(
                                    function=SimpleNamespace(
                                        name="submit_analysis_report",
                                        arguments=json.dumps(self._native_report_payload(), ensure_ascii=False),
                                    )
                                )
                            ]
                        )
                    )
                ],
                usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            )
        )

        with patch("src.analyzer.OfficialDeepSeekStructuredClient.create_structured_report") as create_responses:
            text, _model, _usage = analyzer._call_litellm_impl(
                "prompt",
                {"max_output_tokens": 128, "temperature": 0.7},
                response_validator=analyzer._validate_json_response,
                structured_tool=analyzer._analysis_report_tool(),
            )

        create_responses.assert_not_called()
        analyzer._dispatch_litellm_completion.assert_called_once()
        self.assertEqual(json.loads(text)["stock_name"], "测试标的")

    def test_non_official_deepseek_gateway_keeps_litellm_tool_path(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="deepseek/deepseek-v4-pro",
                litellm_fallback_models=[],
                llm_model_list=[
                    {
                        "model_name": "deepseek/deepseek-v4-pro",
                        "litellm_params": {
                            "model": "deepseek/deepseek-v4-pro",
                            "api_key": "test-deepseek-key",
                            "api_base": "https://proxy.example/v1",
                        },
                    }
                ],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=True)
        analyzer._dispatch_litellm_completion = MagicMock(
            return_value=SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            tool_calls=[
                                SimpleNamespace(
                                    function=SimpleNamespace(
                                        name="submit_analysis_report",
                                        arguments=json.dumps(self._native_report_payload(), ensure_ascii=False),
                                    )
                                )
                            ]
                        )
                    )
                ],
                usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            )
        )

        with patch("src.analyzer.OfficialDeepSeekStructuredClient.create_structured_report") as create_responses:
            text, _model, _usage = analyzer._call_litellm_impl(
                "prompt",
                {"max_output_tokens": 128, "temperature": 0.7},
                response_validator=analyzer._validate_json_response,
                structured_tool=analyzer._analysis_report_tool(),
            )

        create_responses.assert_not_called()
        call_kwargs = analyzer._dispatch_litellm_completion.call_args.args[1]
        self.assertIn("tools", call_kwargs)
        self.assertEqual(json.loads(text)["stock_name"], "测试标的")

    def test_official_deepseek_client_responses_sdk_kwargs(self) -> None:
        fake_client = SimpleNamespace(
            responses=SimpleNamespace(create=MagicMock(return_value=SimpleNamespace(output_text="{}"))),
            chat=SimpleNamespace(completions=SimpleNamespace(create=MagicMock())),
        )
        route = DeepSeekStructuredRoute(
            route_model="deepseek/deepseek-v4-pro",
            wire_model="deepseek/deepseek-v4-pro",
            api_model="deepseek-v4-pro",
            api_key="test-key",
            base_url="https://api.deepseek.com",
            source="unit",
        )
        client = OfficialDeepSeekStructuredClient(route, client=fake_client)
        schema = {"type": "object", "properties": {"answer": {"type": "string"}}}
        messages = [{"role": "system", "content": "system"}, {"role": "user", "content": "prompt"}]

        client.create_structured_report(
            messages=messages,
            schema=schema,
            max_output_tokens=256,
            temperature=0.7,
            timeout=12.0,
        )

        fake_client.responses.create.assert_called_once()
        kwargs = fake_client.responses.create.call_args.kwargs
        self.assertEqual(kwargs["model"], "deepseek-v4-pro")
        self.assertEqual(kwargs["input"], messages)
        self.assertEqual(kwargs["text"]["format"]["type"], "json_schema")
        self.assertEqual(kwargs["text"]["format"]["name"], "analysis_report")
        self.assertEqual(kwargs["text"]["format"]["schema"], schema)
        self.assertEqual(kwargs["reasoning"], {"effort": "high"})
        self.assertEqual(kwargs["max_output_tokens"], 256)
        self.assertEqual(kwargs["temperature"], 0.7)
        self.assertEqual(kwargs["timeout"], 12.0)
        self.assertNotIn("tools", kwargs)
        self.assertNotIn("tool_choice", kwargs)

    def test_official_deepseek_client_chat_json_fallback_sdk_kwargs(self) -> None:
        fake_client = SimpleNamespace(
            responses=SimpleNamespace(create=MagicMock()),
            chat=SimpleNamespace(
                completions=SimpleNamespace(create=MagicMock(return_value=SimpleNamespace(choices=[])))
            ),
        )
        route = DeepSeekStructuredRoute(
            route_model="deepseek/deepseek-v4-pro",
            wire_model="deepseek/deepseek-v4-pro",
            api_model="deepseek-v4-pro",
            api_key="test-key",
            base_url="https://api.deepseek.com",
            source="unit",
        )
        client = OfficialDeepSeekStructuredClient(route, client=fake_client)
        messages = [{"role": "system", "content": "system"}, {"role": "user", "content": "return json"}]

        client.create_json_fallback(
            messages=messages,
            max_tokens=256,
            temperature=0.7,
            timeout=12.0,
        )

        fake_client.chat.completions.create.assert_called_once()
        kwargs = fake_client.chat.completions.create.call_args.kwargs
        self.assertEqual(kwargs["model"], "deepseek-v4-pro")
        self.assertEqual(kwargs["messages"], messages)
        self.assertEqual(kwargs["response_format"], {"type": "json_object"})
        self.assertEqual(kwargs["reasoning_effort"], "high")
        self.assertEqual(kwargs["extra_body"], {"thinking": {"type": "enabled"}})
        self.assertEqual(kwargs["max_tokens"], 256)
        self.assertEqual(kwargs["temperature"], 0.7)
        self.assertEqual(kwargs["timeout"], 12.0)
        fake_client.responses.create.assert_not_called()

    def test_deepseek_structured_resubmission_disables_thinking_and_forces_tool(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="deepseek/deepseek-v4-pro",
                litellm_fallback_models=[],
                llm_model_list=[
                    {
                        "model_name": "deepseek/deepseek-v4-pro",
                        "litellm_params": {
                            "model": "deepseek/deepseek-v4-pro",
                            "api_key": "test-key",
                            "api_base": "https://proxy.example/v1",
                        },
                    }
                ],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=False)
        analyzer._dispatch_litellm_completion = MagicMock(
            return_value=SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            tool_calls=[
                                SimpleNamespace(
                                    function=SimpleNamespace(
                                        name="submit_analysis_report",
                                        arguments=json.dumps(
                                            {
                                                "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
                                                "stock_name": "测试标的",
                                            },
                                            ensure_ascii=False,
                                        ),
                                    )
                                )
                            ]
                        )
                    )
                ],
                usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            )
        )

        analyzer._call_litellm_impl(
            "完整原始证据与上一轮分析草稿",
            {
                "max_output_tokens": 128,
                "temperature": 0.7,
                "_structured_resubmission": True,
            },
            response_validator=lambda text: None,
            structured_tool=analyzer._analysis_report_tool(),
        )

        call_kwargs = analyzer._dispatch_litellm_completion.call_args.args[1]
        self.assertEqual(call_kwargs["extra_body"], {"thinking": {"type": "disabled"}})
        self.assertEqual(
            call_kwargs["tool_choice"],
            {"type": "function", "function": {"name": "submit_analysis_report"}},
        )
        self.assertEqual(call_kwargs.get("temperature"), 0.7)

    def test_structured_resubmission_pins_same_model_and_uses_message_handoff(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="deepseek/deepseek-v4-pro",
                litellm_fallback_models=["anthropic/claude-sonnet-5"],
                llm_model_list=[
                    {
                        "model_name": "deepseek/deepseek-v4-pro",
                        "litellm_params": {
                            "model": "deepseek/deepseek-v4-pro",
                            "api_key": "test-key",
                            "api_base": "https://proxy.example/v1",
                        },
                    }
                ],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=False)
        long_draft = "首轮可见分析：" + ("保持分析能力" * 900) + "尾部决策：买入，止损 10.00，目标 12.00"
        analyzer._dispatch_litellm_completion = MagicMock(
            return_value=SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            tool_calls=[
                                SimpleNamespace(
                                    function=SimpleNamespace(
                                        name="submit_analysis_report",
                                        arguments=json.dumps(
                                            {
                                                "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
                                                "stock_name": "测试标的",
                                            },
                                            ensure_ascii=False,
                                        ),
                                    )
                                )
                            ]
                        )
                    )
                ],
                usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            )
        )

        analyzer._call_litellm_impl(
            "只做结构化提交",
            {
                "max_output_tokens": 128,
                "temperature": 0.7,
                "_structured_resubmission": True,
                "_structured_resubmission_model": "deepseek/deepseek-v4-pro",
                "_structured_retry_original_prompt": "完整原始证据",
                "_structured_retry_previous_response": long_draft,
            },
            response_validator=lambda text: None,
            structured_tool=analyzer._analysis_report_tool(),
        )

        dispatch_model, call_kwargs = analyzer._dispatch_litellm_completion.call_args.args[:2]
        self.assertEqual(dispatch_model, "deepseek/deepseek-v4-pro")
        self.assertEqual(call_kwargs["model"], "deepseek/deepseek-v4-pro")
        self.assertEqual([message["role"] for message in call_kwargs["messages"]], ["system", "user", "assistant", "user"])
        self.assertIn("完整原始证据", call_kwargs["messages"][1]["content"])
        self.assertIn("尾部决策：买入", call_kwargs["messages"][2]["content"])
        self.assertNotIn("anthropic/claude-sonnet-5", dispatch_model)
        self.assertGreaterEqual(GeminiAnalyzer.STRUCTURED_RETRY_DRAFT_MAX_CHARS, 65536)

    def test_structured_resubmission_keeps_original_evidence_when_draft_is_empty(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="deepseek/deepseek-v4-pro",
                litellm_fallback_models=[],
                llm_model_list=[
                    {
                        "model_name": "deepseek/deepseek-v4-pro",
                        "litellm_params": {
                            "model": "deepseek/deepseek-v4-pro",
                            "api_key": "test-key",
                            "api_base": "https://proxy.example/v1",
                        },
                    }
                ],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=False)
        analyzer._dispatch_litellm_completion = MagicMock(
            return_value=SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            tool_calls=[
                                SimpleNamespace(
                                    function=SimpleNamespace(
                                        name="submit_analysis_report",
                                        arguments=json.dumps(
                                            {
                                                "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
                                                "stock_name": "测试标的",
                                            },
                                            ensure_ascii=False,
                                        ),
                                    )
                                )
                            ]
                        )
                    )
                ],
                usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            )
        )

        analyzer._call_litellm_impl(
            "只做结构化提交",
            {
                "max_output_tokens": 128,
                "temperature": 0.7,
                "_structured_resubmission": True,
                "_structured_resubmission_model": "deepseek/deepseek-v4-pro",
                "_structured_retry_original_prompt": "完整原始证据，即使首轮没有可见草稿也必须保留",
                "_structured_retry_previous_response": "",
            },
            response_validator=lambda text: None,
            structured_tool=analyzer._analysis_report_tool(),
        )

        _, call_kwargs = analyzer._dispatch_litellm_completion.call_args.args[:2]
        self.assertEqual([message["role"] for message in call_kwargs["messages"]], ["system", "user", "user"])
        self.assertIn("完整原始证据", call_kwargs["messages"][1]["content"])
        self.assertIn("只做结构化提交", call_kwargs["messages"][2]["content"])

    def test_structured_retry_handoff_never_carries_reasoning_content(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        draft = json.dumps(
            {
                "reasoning_content": "隐藏思考不应传递",
                "operation_advice": "买入",
                "nested": {"reasoning_content": "内部隐藏思考"},
            },
            ensure_ascii=False,
        )

        sanitized = analyzer._sanitize_structured_retry_draft(draft)

        self.assertIn("operation_advice", sanitized)
        self.assertNotIn("reasoning_content", sanitized)
        self.assertNotIn("隐藏思考", sanitized)

    def test_missing_tool_call_text_is_retained_only_for_same_model_resubmission(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="deepseek/deepseek-v4-pro",
                litellm_fallback_models=[],
                llm_model_list=[],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=False)
        analyzer._dispatch_litellm_completion = MagicMock(
            return_value=SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content="这是没有 tool_call 的分析草稿"))],
                usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            )
        )

        with self.assertRaises(_AllModelsFailedError) as context:
            analyzer._call_litellm_impl(
                "prompt",
                {"max_output_tokens": 128, "temperature": 0.7},
                response_validator=lambda text: None,
                structured_tool=analyzer._analysis_report_tool(),
            )

        self.assertIn("tool_call_missing", str(context.exception))
        self.assertIn("assistant_visible_draft", context.exception.last_response_text or "")
        self.assertIn("这是没有 tool_call 的分析草稿", context.exception.last_response_text or "")
        self.assertEqual(context.exception.last_model, "deepseek/deepseek-v4-pro")
        self.assertEqual(context.exception.last_error_kind, "structured_submission")

    def test_structured_retry_context_binds_terminal_failure_model_not_previous_draft_model(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="provider/primary-model",
                litellm_fallback_models=["provider/fallback-model"],
                llm_model_list=[],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=False)

        def fake_dispatch(model, call_kwargs, **kwargs):
            if model == "provider/primary-model":
                return SimpleNamespace(
                    choices=[SimpleNamespace(message=SimpleNamespace(content="primary visible draft"))],
                    usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
                )
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content=""))],
                usage={"prompt_tokens": 4, "completion_tokens": 5, "total_tokens": 9},
            )

        analyzer._dispatch_litellm_completion = MagicMock(side_effect=fake_dispatch)

        with self.assertRaises(_AllModelsFailedError) as context:
            analyzer._call_litellm_impl(
                "prompt",
                {"max_output_tokens": 128, "temperature": 0.7},
                response_validator=lambda text: None,
                structured_tool=analyzer._analysis_report_tool(),
            )

        self.assertEqual(context.exception.last_model, "provider/fallback-model")
        self.assertIsNone(context.exception.last_response_text)
        self.assertEqual(context.exception.last_error_kind, "structured_submission")

    def test_invalid_tool_arguments_and_visible_content_are_both_retained_as_untrusted_retry_draft(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._router = None
        analyzer._legacy_router_model_list = []
        invalid_args = (
            '{"report_schema_version":"legacy-json","stock_name":"测试","operation_advice":"买入"'
        )
        analyzer._get_runtime_config = MagicMock(
            return_value=Config(
                litellm_model="deepseek/deepseek-v4-pro",
                litellm_fallback_models=[],
                llm_model_list=[],
            )
        )
        analyzer._has_channel_config = MagicMock(return_value=False)
        analyzer._dispatch_litellm_completion = MagicMock(
            return_value=SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            tool_calls=[
                                SimpleNamespace(
                                    function=SimpleNamespace(
                                        name="submit_analysis_report",
                                        arguments=invalid_args,
                                    )
                                )
                            ],
                            content="完整可见分析正文也必须保留",
                        )
                    )
                ],
                usage={"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
            )
        )

        with self.assertRaises(_AllModelsFailedError) as context:
            analyzer._call_litellm_impl(
                "prompt",
                {"max_output_tokens": 128, "temperature": 0.7},
                response_validator=lambda text: None,
                structured_tool=analyzer._analysis_report_tool(),
            )

        self.assertIn("operation_advice", context.exception.last_response_text or "")
        self.assertIn("完整可见分析正文也必须保留", context.exception.last_response_text or "")

    def test_structured_resubmission_prompt_marks_previous_text_as_draft(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)

        prompt = analyzer._build_structured_tool_retry_prompt(
            "原始证据",
            report_language="zh",
            previous_response="上一轮普通文本分析",
        )

        self.assertIn("原始证据", prompt)
        self.assertIn("上一轮普通文本分析", prompt)
        self.assertIn("只视为分析草稿", prompt)
        self.assertIn("submit_analysis_report", prompt)

    def test_structured_retry_decision_lock_rejects_score_action_and_price_drift(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        draft = json.dumps(
            {
                "sentiment_score": 78,
                "operation_advice": "买入",
                "action": "buy",
                "dashboard": {
                    "battle_plan": {
                        "sniper_points": {
                            "ideal_buy": "10.00元",
                            "stop_loss": "9.50元",
                            "take_profit": "12.00元",
                        }
                    }
                },
            },
            ensure_ascii=False,
        )
        lock = analyzer._build_structured_retry_decision_lock(draft)
        drifted = json.dumps(
            {
                "sentiment_score": 60,
                "operation_advice": "观望",
                "action": "watch",
                "dashboard": {
                    "battle_plan": {
                        "sniper_points": {
                            "ideal_buy": "10.00元",
                            "stop_loss": "9.20元",
                            "take_profit": "12.00元",
                        }
                    }
                },
            },
            ensure_ascii=False,
        )

        with self.assertRaises(ValueError) as context:
            analyzer._validate_structured_retry_decision_lock(drifted, lock)

        self.assertIn("structured_retry_core_drift", str(context.exception))
        self.assertIn("sentiment_score", str(context.exception))
        self.assertIn("operation_advice", str(context.exception))

    def test_structured_retry_decision_lock_compares_text_fields_as_text_not_numbers(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        draft = json.dumps(
            {
                "operation_advice": "买入，明日 10:00 后观察",
                "dashboard": {
                    "phase_decision": {
                        "next_check_time": "10:05",
                        "watch_conditions": ["放量突破 10.20 元", "回踩 MA5 不破"],
                    }
                },
            },
            ensure_ascii=False,
        )
        lock = analyzer._build_structured_retry_decision_lock(draft)
        drifted = json.dumps(
            {
                "operation_advice": "买入，明日 10:30 后观察",
                "dashboard": {
                    "phase_decision": {
                        "next_check_time": "10:35",
                        "watch_conditions": ["放量突破 10.20 元", "回踩 MA5 不破"],
                    }
                },
            },
            ensure_ascii=False,
        )

        with self.assertRaises(ValueError) as context:
            analyzer._validate_structured_retry_decision_lock(drifted, lock)

        self.assertIn("operation_advice", str(context.exception))
        self.assertIn("next_check_time", str(context.exception))

    def test_structured_retry_decision_lock_allows_schema_field_completion(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        draft = json.dumps(
            {
                "sentiment_score": 78,
                "operation_advice": "买入",
                "dashboard": {"battle_plan": {"sniper_points": {"stop_loss": "9.50元"}}},
            },
            ensure_ascii=False,
        )
        lock = analyzer._build_structured_retry_decision_lock(draft)
        completed = json.dumps(
            {
                "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
                "stock_name": "测试标的",
                "sentiment_score": 78,
                "operation_advice": "买入",
                "analysis_summary": "补齐字段",
                "dashboard": {
                    "battle_plan": {
                        "sniper_points": {
                            "ideal_buy": "10.00元",
                            "stop_loss": 9.5,
                            "take_profit": "12.00元",
                        }
                    }
                },
            },
            ensure_ascii=False,
        )

        analyzer._validate_structured_retry_decision_lock(completed, lock)

    def test_structured_retry_decision_lock_is_empty_for_unparseable_free_text(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)

        lock = analyzer._build_structured_retry_decision_lock("这是一段无法机器恢复字段的自然语言草稿")

        self.assertEqual(lock, {})

    def test_structured_resubmission_classifier_separates_transport_errors(self) -> None:
        self.assertTrue(_looks_like_structured_submission_error("ValueError: tool_call_missing"))
        self.assertTrue(_looks_like_structured_submission_error("schema_validation_failed at validation"))
        self.assertFalse(
            _looks_like_structured_submission_error(
                "InternalServerError: AnthropicException - [SSL: UNEXPECTED_EOF_WHILE_READING]"
            )
        )

        transport_after_draft = _AllModelsFailedError(
            "last attempt disconnected",
            last_response_text="an earlier model returned an untrusted draft",
            last_error_kind="transport_provider",
        )
        self.assertFalse(_looks_like_structured_submission_error(transport_after_draft))

        structured_without_draft = _AllModelsFailedError(
            "provider wrapper hid the validation detail",
            last_error_kind="structured_submission",
        )
        self.assertTrue(_looks_like_structured_submission_error(structured_without_draft))

    def test_structured_prompt_replaces_literal_json_template_but_keeps_logic_anchors(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._skill_instructions_override = ""
        analyzer._default_skill_policy_override = ""
        analyzer._use_legacy_default_prompt_override = False
        analyzer._resolved_prompt_state = {
            "skill_instructions": "",
            "default_skill_policy": "",
            "use_legacy_default_prompt": False,
        }

        prompt = analyzer._get_analysis_system_prompt("zh", stock_code="600519", structured_tool_output=True)

        self.assertIn("submit_analysis_report", prompt)
        self.assertNotIn("```json", prompt)
        for anchor in ("市场阶段", "大盘环境", "市场结构", "技术指标", "量价", "筹码", "基本面", "新闻", "持仓状态", "风险收益"):
            self.assertIn(anchor, prompt)

    def test_legacy_prompt_keeps_json_envelope_without_tool_requirement(self) -> None:
        analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
        analyzer._skill_instructions_override = ""
        analyzer._default_skill_policy_override = ""
        analyzer._use_legacy_default_prompt_override = False
        analyzer._resolved_prompt_state = {
            "skill_instructions": "",
            "default_skill_policy": "",
            "use_legacy_default_prompt": False,
        }

        prompt = analyzer._get_analysis_system_prompt("zh", stock_code="600519", structured_tool_output=False)

        self.assertIn("```json", prompt)
        self.assertIn("整个回复必须且只能是一个 JSON 对象", prompt)

    def test_parse_response_keeps_invalid_provider_output_non_actionable(self) -> None:
        analyzer = GeminiAnalyzer()
        response = json.dumps({
            "stock_name": "贵州茅台",
            "sentiment_score": "not-a-number",
            "trend_prediction": "看多",
            "operation_advice": "持有",
            "analysis_summary": "测试摘要",
            "dashboard": {"core_conclusion": {"one_sentence": "测试"}},
        })

        result = analyzer._parse_response(response, "600519", "股票600519")

        self.assertFalse(result.success)
        self.assertIsNone(result.action)
        self.assertIsNone(result.action_label)
        self.assertEqual(result.decision_type, "failed")
        self.assertEqual(result.operation_advice, "分析失败")
        self.assertEqual(result.sentiment_score, 50)
        self.assertIn("JSON", result.error_message)

    def test_buy_action_integrity_requires_executable_entry_exit_and_phase_contract(self) -> None:
        result = AnalysisResult(
            code="002714",
            name="牧原股份",
            sentiment_score=72,
            trend_prediction="看多",
            operation_advice="买入",
            decision_type="buy",
            action="buy",
            report_schema_version=GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            position_actions={
                "empty": {"action": "buy", "advice": "按计划买入"},
                "holding": {"action": "hold", "advice": "已有持仓继续观察"},
            },
            analysis_summary="回踩确认后买入",
            dashboard={
                "core_conclusion": {"one_sentence": "回踩确认后买入"},
                "intelligence": {"risk_alerts": []},
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": 39.5,
                        "secondary_buy": 39.7,
                        "stop_loss": 38.8,
                        "take_profit": 41.3,
                        "final_target": 41.3,
                        "entry_leg_schema_version": "entry-legs-v1",
                        "entry_legs": [
                            {
                                "leg_id": "initial",
                                "role": "initial",
                                "sequence": 1,
                                "allocation_fraction": 1.0,
                                "max_executions": 1,
                                "price_rules": [{"type": "close_between", "low": 39.5, "high": 39.7}],
                                "prerequisites": ["放量确认"],
                            }
                        ],
                        "exit_leg_schema_version": "exit-legs-v1",
                        "exit_legs": [
                            {
                                "leg_id": "tp1",
                                "role": "final_take_profit",
                                "sequence": 1,
                                "allocation_fraction": 1.0,
                                "target_price": 41.3,
                            }
                        ],
                    }
                },
                "phase_decision": {
                    "phase_context": {"phase": "postmarket"},
                    "action_window": "下个交易日盘中跟踪",
                    "immediate_action": "等待确认",
                    "watch_conditions": ["放量不破入场区间"],
                    "next_check_time": "下个交易日 09:35",
                    "confidence_reason": "量价条件待确认",
                    "data_limitations": [],
                },
            },
        )

        ok, missing = GeminiAnalyzer.__new__(GeminiAnalyzer)._check_content_integrity(result)

        self.assertTrue(ok, missing)

    def test_native_buy_integrity_ignores_missing_display_fields_when_execution_contract_is_valid(self) -> None:
        result = AnalysisResult(
            code="002714",
            name="牧原股份",
            sentiment_score=72,
            trend_prediction="看多",
            operation_advice="买入",
            decision_type="buy",
            action="buy",
            confidence_level="中",
            report_schema_version=GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            position_actions={
                "empty": {"action": "buy"},
                "holding": {"action": "hold"},
            },
            dashboard={
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": 39.5,
                        "secondary_buy": 39.7,
                        "stop_loss": 38.8,
                        "take_profit": 41.3,
                        "final_target": 41.3,
                        "entry_leg_schema_version": "entry-legs-v1",
                        "entry_legs": [
                            {
                                "leg_id": "initial",
                                "role": "initial",
                                "sequence": 1,
                                "allocation_fraction": 1.0,
                                "max_executions": 1,
                                "price_rules": [{"type": "close_between", "low": 39.5, "high": 39.7}],
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
                                "target_price": 41.3,
                            }
                        ],
                    }
                },
                "phase_decision": {
                    "action_window": "下个交易日盘中跟踪",
                    "immediate_action": "等待确认",
                    "watch_conditions": ["放量不破入场区间"],
                    "next_check_time": "下个交易日 09:35",
                },
            },
        )

        ok, missing = GeminiAnalyzer.__new__(GeminiAnalyzer)._check_content_integrity(result)

        self.assertTrue(ok, missing)

    def test_native_buy_integrity_rejects_empty_or_drifted_watch_conditions(self) -> None:
        result = AnalysisResult(
            code="002714",
            name="牧原股份",
            sentiment_score=72,
            trend_prediction="看多",
            operation_advice="买入",
            decision_type="buy",
            action="buy",
            confidence_level="中",
            report_schema_version=GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            position_actions={
                "empty": {"action": "buy"},
                "holding": {"action": "hold"},
            },
            dashboard={
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": 39.5,
                        "secondary_buy": 39.7,
                        "stop_loss": 38.8,
                        "take_profit": 41.3,
                        "final_target": 41.3,
                        "entry_leg_schema_version": "entry-legs-v1",
                        "entry_legs": [
                            {
                                "leg_id": "initial",
                                "role": "initial",
                                "sequence": 1,
                                "allocation_fraction": 1.0,
                                "max_executions": 1,
                                "price_rules": [{"type": "close_between", "low": 39.5, "high": 39.7}],
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
                                "target_price": 41.3,
                            }
                        ],
                    }
                },
                "phase_decision": {
                    "action_window": "下个交易日盘中跟踪",
                    "immediate_action": "等待确认",
                    "watch_conditions": [],
                    "next_check_time": "下个交易日 09:35",
                },
            },
        )

        ok, missing = GeminiAnalyzer.__new__(GeminiAnalyzer)._check_content_integrity(result)

        self.assertFalse(ok)
        self.assertEqual(missing, ["dashboard.phase_decision.watch_conditions"])

        result.dashboard["phase_decision"]["watch_conditions"] = {"condition": "放量确认"}
        ok, missing = GeminiAnalyzer.__new__(GeminiAnalyzer)._check_content_integrity(result)

        self.assertFalse(ok)
        self.assertEqual(missing, ["dashboard.phase_decision.watch_conditions"])

    def test_native_buy_integrity_rejects_invalid_confidence_as_execution_only(self) -> None:
        result = AnalysisResult(
            code="002714",
            name="牧原股份",
            sentiment_score=72,
            trend_prediction="看多",
            operation_advice="买入",
            decision_type="buy",
            action="buy",
            confidence_level="非常确定",
            report_schema_version=GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            position_actions={
                "empty": {"action": "buy"},
                "holding": {"action": "hold"},
            },
            dashboard={
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": 39.5,
                        "secondary_buy": 39.7,
                        "stop_loss": 38.8,
                        "take_profit": 41.3,
                        "final_target": 41.3,
                        "entry_leg_schema_version": "entry-legs-v1",
                        "entry_legs": [
                            {
                                "leg_id": "initial",
                                "role": "initial",
                                "sequence": 1,
                                "allocation_fraction": 1.0,
                                "max_executions": 1,
                                "price_rules": [{"type": "close_between", "low": 39.5, "high": 39.7}],
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
                                "target_price": 41.3,
                            }
                        ],
                    }
                },
                "phase_decision": {
                    "action_window": "下个交易日盘中跟踪",
                    "immediate_action": "等待确认",
                    "watch_conditions": ["放量不破入场区间"],
                    "next_check_time": "下个交易日 09:35",
                },
            },
        )

        ok, missing = GeminiAnalyzer.__new__(GeminiAnalyzer)._check_content_integrity(result)

        self.assertFalse(ok)
        self.assertEqual(missing, ["confidence_level"])

    def test_buy_action_integrity_rejects_missing_exit_legs(self) -> None:
        result = AnalysisResult(
            code="002714",
            name="牧原股份",
            sentiment_score=72,
            trend_prediction="看多",
            operation_advice="买入",
            decision_type="buy",
            action="buy",
            report_schema_version=GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            position_actions={
                "empty": {"action": "buy", "advice": "按计划买入"},
                "holding": {"action": "hold", "advice": "已有持仓继续观察"},
            },
            analysis_summary="回踩确认后买入",
            dashboard={
                "core_conclusion": {"one_sentence": "回踩确认后买入"},
                "intelligence": {"risk_alerts": []},
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": 39.5,
                        "secondary_buy": 39.7,
                        "stop_loss": 38.8,
                        "take_profit": 41.3,
                        "entry_leg_schema_version": "entry-legs-v1",
                        "entry_legs": [
                            {
                                "leg_id": "initial",
                                "sequence": 1,
                                "allocation_fraction": 1.0,
                                "max_executions": 1,
                                "price_rules": [{"type": "close_between", "low": 39.5, "high": 39.7}],
                            }
                        ],
                    }
                },
                "phase_decision": {
                    "phase_context": {"phase": "postmarket"},
                    "action_window": "下个交易日盘中跟踪",
                    "immediate_action": "等待确认",
                    "watch_conditions": ["放量不破入场区间"],
                    "next_check_time": "下个交易日 09:35",
                    "confidence_reason": "量价条件待确认",
                    "data_limitations": [],
                },
            },
        )

        ok, missing = GeminiAnalyzer.__new__(GeminiAnalyzer)._check_content_integrity(result)

        self.assertFalse(ok)
        self.assertIn("dashboard.battle_plan.sniper_points.exit_legs", missing)
        self.assertTrue(all(
            field.startswith("dashboard.battle_plan.sniper_points.")
            or field.startswith("dashboard.phase_decision.")
            for field in missing
        ))

    def test_native_tool_explicit_watch_is_not_score_aligned_to_buy(self) -> None:
        analyzer = GeminiAnalyzer()
        response = json.dumps({
            "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            "stock_name": "测试标的",
            "sentiment_score": 72,
            "raw_score": 72,
            "adjusted_score": 72,
            "trend_prediction": "看多",
            "operation_advice": "观望",
            "decision_type": "hold",
            "action": "watch",
            "position_actions": {
                "empty": {"action": "watch"},
                "holding": {"action": "hold"},
            },
            "confidence_level": "中",
            "analysis_summary": "证据不足",
            "dashboard": {
                "core_conclusion": {"one_sentence": "等待确认"},
                "intelligence": {"risk_alerts": []},
            },
        }, ensure_ascii=False)

        result = analyzer._parse_response(response, "600519", "测试标的")
        ok, missing = analyzer._check_content_integrity(result)

        self.assertEqual(result.action, "watch")
        self.assertTrue(ok, missing)

    def test_native_tool_low_score_buy_still_uses_score_gate(self) -> None:
        analyzer = GeminiAnalyzer()
        response = json.dumps({
            "report_schema_version": GeminiAnalyzer.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
            "stock_name": "测试标的",
            "sentiment_score": 28,
            "raw_score": 28,
            "adjusted_score": 28,
            "trend_prediction": "看空",
            "operation_advice": "买入",
            "decision_type": "buy",
            "action": "buy",
            "position_actions": {
                "empty": {"action": "buy"},
                "holding": {"action": "hold"},
            },
            "confidence_level": "低",
            "analysis_summary": "分数偏弱，不应直接执行买入。",
            "dashboard": {
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": 39.5,
                        "secondary_buy": 39.7,
                        "stop_loss": 38.8,
                        "take_profit": 43.0,
                        "final_target": 43.0,
                        "entry_leg_schema_version": "entry-legs-v1",
                        "entry_legs": [
                            {
                                "leg_id": "initial",
                                "role": "initial",
                                "sequence": 1,
                                "allocation_fraction": 1.0,
                                "max_executions": 1,
                                "price_rules": [{"type": "close_between", "low": 39.5, "high": 39.7}],
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
                                "target_price": 43.0,
                            }
                        ],
                    }
                },
                "phase_decision": {
                    "entry_mode": "conditional",
                    "action_window": "下个交易日盘中跟踪",
                    "immediate_action": "等待确认",
                    "watch_conditions": ["放量不破入场区间"],
                    "next_check_time": "下个交易日 09:35",
                },
            },
        }, ensure_ascii=False)

        result = analyzer._parse_response(response, "600519", "测试标的")

        self.assertEqual(result.action, "reduce")
        self.assertEqual(result.decision_type, "sell")
        self.assertEqual(result.operation_advice, "减仓")
        self.assertEqual(result.dashboard["decision_score_calibration"]["final_action"], "reduce")

    def test_parse_response_keeps_partial_claude_output_non_actionable(self) -> None:
        """Incomplete Claude-style JSON must be a failed attempt, not hold/50."""
        analyzer = GeminiAnalyzer()
        response = json.dumps({
            "stock_name": "创新医疗",
            "analysis_summary": "格式不完整，仅返回了摘要。",
            "dashboard": {"core_conclusion": {"one_sentence": "继续持有"}},
        }, ensure_ascii=False)

        result = analyzer._parse_response(response, "002173", "创新医疗")

        self.assertFalse(result.success)
        self.assertIsNone(result.action)
        self.assertIsNone(result.action_label)
        self.assertEqual(result.decision_type, "failed")
        self.assertEqual(result.operation_advice, "分析失败")

    def test_parse_text_response_honors_injected_runtime_report_language(self) -> None:
        """Fallback text parsing should use the analyzer's injected config, not the global singleton."""
        with patch.object(GeminiAnalyzer, "_init_litellm", return_value=None):
            analyzer = GeminiAnalyzer(config=SimpleNamespace(report_language="en"))

        result = analyzer._parse_text_response("bullish buy setup", "AAPL", "Apple")

        self.assertEqual(result.report_language, "en")
        self.assertFalse(result.success)
        self.assertEqual(result.trend_prediction, "Sideways")
        self.assertEqual(result.operation_advice, "Analysis failed")
        self.assertIsNone(result.action)
        self.assertEqual(result.confidence_level, "Low")
