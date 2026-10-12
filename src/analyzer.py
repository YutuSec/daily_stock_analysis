# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - AI分析层
===================================

职责：
1. 封装 LLM 调用逻辑（通过 LiteLLM 统一调用 Gemini/Anthropic/OpenAI 等）
2. 结合技术面和消息面生成分析报告
3. 解析 LLM 响应为结构化 AnalysisResult
"""

import json
import logging
import math
import re
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple, Callable

import litellm
from json_repair import repair_json
from litellm import Router

from src.agent.llm_adapter import (
    get_thinking_extra_body,
    resolve_fallback_litellm_wire_models,
    register_fallback_model_pricing,
)
from src.agent.provider_trace import resolved_model_provider_identity
from src.config import (
    Config,
    extra_litellm_params,
    get_api_keys_for_model,
    get_config,
    get_configured_llm_models,
    resolve_news_window_days,
)
from src.llm.hermes import (
    HERMES_CHANNEL_NAME,
    build_hermes_redaction_values,
    canonicalize_hermes_model_ref,
    filter_non_hermes_deployments,
    hermes_blocked_route_candidates,
    is_masked_secret_placeholder,
    open_hermes_no_proxy_client,
    route_deployment_origins,
    route_has_hermes,
    sanitize_hermes_error_text,
)
from src.llm.generation_params import apply_litellm_generation_params
from src.llm.errors import call_litellm_with_param_recovery
from src.llm.backend_registry import (
    LOCAL_CLI_GENERATION_BACKEND_IDS,
    LITELLM_BACKEND_ID,
    resolve_generation_backend_id,
    resolve_generation_fallback_backend_id,
)
from src.llm.backend_factory import create_generation_backend
from src.llm.generation_backend import (
    GenerationBackend,
    GenerationError,
    GenerationErrorCode,
)
from src.llm.deepseek_structured import (
    DEEPSEEK_REPORT_MAX_OUTPUT_TOKENS,
    DeepSeekStructuredGenerationError,
    OfficialDeepSeekStructuredClient,
    chat_completion_finish_reason,
    ensure_json_instruction,
    extract_chat_completion_text,
    extract_responses_text,
    normalize_deepseek_usage,
    responses_diagnostics,
    resolve_official_deepseek_structured_route,
)
from src.llm.usage import (
    attach_legacy_message_stability_audit,
    attach_message_hmacs,
    extract_usage_payload,
    normalize_litellm_usage,
    should_persist_usage_telemetry,
)
from src.llm.local_cli_backend import redact_diagnostic_text
from src.llm.provider_cache import (
    apply_prompt_cache_hints,
    build_provider_cache_route_context,
    filter_prompt_cache_telemetry,
)
from src.storage import persist_llm_usage
from src.data.stock_mapping import STOCK_NAME_MAP
from src.report_language import (
    get_signal_level,
    get_no_data_text,
    get_placeholder_text,
    get_unknown_text,
    get_chip_unavailable_text,
    infer_decision_type_from_advice,
    is_chip_placeholder_value,
    localize_chip_health,
    localize_confidence_level,
    localize_operation_advice,
    localize_trend_prediction,
    normalize_report_language,
)
from src.schemas.decision_action import (
    build_action_fields,
    normalize_decision_action,
    normalize_result_decision_contract,
)
from src.schemas.decision_scale import (
    CANONICAL_DECISION_SCALE_PROMPT_ZH,
    normalize_score,
    score_band_metadata,
)
from src.schemas.report_schema import AnalysisReportSchema
from src.market_context import detect_market, get_market_role, get_market_guidelines
from src.services.daily_market_context import format_daily_market_context_prompt_section
from src.market_phase_prompt import format_market_phase_prompt_section
from src.market_structure_prompt import format_market_structure_prompt_section

logger = logging.getLogger(__name__)

ANALYSIS_REPORT_TOOL_SCHEMA_VERSION = "analysis-report-tool-v1"

LEGACY_DEFAULT_TRADING_POLICY_ZH = """## 默认技能基线（无显式风格时适用）

当前激活的 skills 可以补充细化分析视角；没有显式风格或技能时，以下基线作为默认短周期/波段风控参考，而不是覆盖纸面交易风格的硬门槛。

### 1. 严进策略（不追高）
- MA5 乖离用于衡量入场位置：<2% 通常更接近理想买点，2-5% 需要小仓与确认，>5% 一般不追高
- 若处于强趋势、趋势延续或中线持仓风格，必须结合更长均线、ATR/波动、支撑压力、成交参与度和风险收益后再判断，不能仅因 MA5 乖离一项直接定为观望

### 2. 趋势交易（顺势而为）
- MA5/MA10/MA20 多头排列是短周期趋势参考，不是所有交易风格的买入必要条件
- 空头排列或均线显著走弱时，新开仓需更严格；已有持仓应结合止损、目标位、T+1 和最新防御信号处理
- 均线发散上行优于均线粘合，但中线/趋势风格还需参考 MA20/MA60/MA120 或配置给出的核心窗口

### 3. 效率优先（筹码结构）
- 关注筹码集中度：90%集中度 < 15% 表示筹码集中
- 获利比例分析：70-90% 获利盘时需警惕获利回吐
- 平均成本与现价关系：现价高于平均成本 5-15% 通常较健康，但 ETF 或无筹码数据时不得编造或强行扣分

### 4. 买点偏好（回踩支撑）
- 优先寻找与当前 setup 匹配的买点：突破看放量确认，回踩看缩量与支撑，趋势延续看参与度和风险收益
- MA5/MA10 回踩可作为短周期买点参考；中线风格应同时检查 MA20/MA60、关键支撑、波动和资金
- 跌破关键支撑或风格失效线时优先防御，而不是机械套用单一均线

### 5. 风险排查重点
- 减持公告、业绩预亏、监管处罚、行业政策利空、大额解禁

### 6. 估值关注（PE/PB）
- PE/PB 明显偏高时需在风险点中说明；ETF、指数或缺少估值数据时不得把不适用当成负面证据
"""


def _localized_text(language: Any, *, en: str, zh: str, ko: str) -> str:
    """Pick a deterministic fallback string for the report language (zh/en/ko)."""
    normalized = normalize_report_language(language)
    if normalized == "en":
        return en
    if normalized == "ko":
        return ko
    return zh


def _report_display_text(value: Any, default: str = "") -> str:
    """Return a safe text value for non-trading report/display fields.

    Provider-native structured output still carries the original dashboard
    object into the trading contract checks.  This helper is only used for
    narrative fields that are stored or rendered as text, so type drift such as
    a list of news bullets should not fail the report or leak raw Python
    objects into downstream UI/notifications.
    """

    if value is None:
        return default
    if isinstance(value, str):
        text = value.strip()
        return text if text else default
    if isinstance(value, (list, tuple, set)):
        parts = [str(item).strip() for item in value if str(item or "").strip()]
        return "；".join(parts) if parts else default
    if isinstance(value, dict):
        try:
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError):
            return str(value)
    text = str(value).strip()
    return text if text else default


def _analysis_failed_label(language: Any) -> str:
    return _localized_text(
        language,
        en="Analysis failed",
        zh="分析失败",
        ko="분석 실패",
    )


def _mark_failed_result_non_actionable(result: "AnalysisResult") -> "AnalysisResult":
    """Failed model output is an attempt record, not an investment decision."""

    result.success = False
    result.sentiment_score = 50
    result.trend_prediction = localize_trend_prediction('震荡', result.report_language)
    result.action = None
    result.action_label = None
    result.operation_advice = _analysis_failed_label(result.report_language)
    result.decision_type = "failed"
    result.confidence_level = localize_confidence_level('低', result.report_language)
    return result


def _normalize_risk_warning_values(value: Any) -> List[str]:
    """Normalize arbitrary risk_warning values into a flat list of text alerts."""
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        normalized: List[str] = []
        for item in value:
            normalized.extend(_normalize_risk_warning_values(item))
        return normalized
    if isinstance(value, dict):
        if not value:
            return []
        try:
            dumped = json.dumps(value, ensure_ascii=False)
            text = dumped.strip()
        except (TypeError, ValueError):
            text = str(value).strip()
        return [text] if text else []
    text = str(value).strip()
    return [text] if text else []


def _today_has_realtime_overlay(today: Any) -> bool:
    if not isinstance(today, dict):
        return False
    data_source = today.get("data_source") or today.get("dataSource")
    if isinstance(data_source, str) and data_source.startswith("realtime:"):
        return True
    if today.get("is_partial_bar") is True or today.get("isPartialBar") is True:
        return True
    if today.get("is_estimated") is True or today.get("isEstimated") is True:
        return True
    return bool(today.get("estimated_fields") or today.get("estimatedFields"))


def _today_looks_complete_daily_bar(
    context: Dict[str, Any],
    phase_context: Dict[str, Any],
) -> bool:
    today = context.get("today")
    if (
        not isinstance(today, dict)
        or today.get("close") in (None, "")
        or _today_has_realtime_overlay(today)
    ):
        return False

    effective_date = phase_context.get("effective_daily_bar_date")
    today_date = today.get("date") or today.get("trade_date") or context.get("date")
    if effective_date and today_date and str(today_date) != str(effective_date):
        return False
    return True


def _phase_aware_quote_labels(context: Dict[str, Any]) -> Tuple[str, str]:
    """Choose Chinese quote-table labels that do not conflict with phase context."""
    phase_context = context.get("market_phase_context")
    if not isinstance(phase_context, dict):
        return "今日行情", "收盘价"

    phase = str(phase_context.get("phase") or "").strip()
    if phase in {"premarket", "non_trading"}:
        today = context.get("today")
        if _today_looks_complete_daily_bar(context, phase_context):
            return "上一完整交易日行情", "上一完整交易日收盘价"
        if _today_has_realtime_overlay(today):
            return "最新行情", "实时估算价"
        if isinstance(today, dict) and today.get("close") not in (None, ""):
            return "最新行情", "最新价"
        return "今日行情", "收盘价"

    if (
        phase in {"intraday", "lunch_break", "closing_auction"}
        and phase_context.get("is_partial_bar") is True
    ):
        return "最新行情", "盘中估算价"

    return "今日行情", "收盘价"


def _should_hide_regular_session_ohlc(context: Dict[str, Any]) -> bool:
    phase_context = context.get("market_phase_context")
    if not isinstance(phase_context, dict):
        return False

    phase = str(phase_context.get("phase") or "").strip()
    return phase in {"premarket", "non_trading"} and not _today_looks_complete_daily_bar(
        context,
        phase_context,
    )


def _legacy_market_group(stock_code: Any) -> str:
    code = str(stock_code or "").strip()
    if not code or code.lower() == "unknown":
        return "unknown"
    market = detect_market(code)
    return market if market in {"cn", "hk", "us"} else "unknown"


def _legacy_audit_marker_specs(
    context: Dict[str, Any],
    *,
    code: str,
    stock_name: str,
    report_language: str,
    news_context: Optional[str],
    analysis_context_pack_summary: Optional[str],
) -> List[Dict[str, Any]]:
    markers: List[Dict[str, Any]] = []

    def add(marker_name: str, value: Any) -> None:
        if value is None:
            return
        text = str(value).strip()
        if not text:
            return
        markers.append(
            {
                "marker_name": marker_name,
                "message_role": "user",
                "text": text,
            }
        )

    add("stock_code", code)
    add("stock_name", stock_name)
    add("analysis_date", context.get("date"))
    add("market_phase", "## Market Phase Context" if report_language in ("en", "ko") else "## 市场阶段上下文")
    add("daily_market_context", "## Daily Market Context" if report_language in ("en", "ko") else "## 大盘环境摘要")
    add("market_structure_context", "## Market Structure Context" if report_language in ("en", "ko") else "## 市场结构上下文")
    add("analysis_context_pack", analysis_context_pack_summary)
    add("quote", "## 📈 技术面数据")
    add("news_context", "## 📰 舆情情报" if news_context else None)
    return markers


class _LiteLLMStreamError(RuntimeError):
    """Internal error wrapper that records whether any text was streamed."""

    def __init__(self, message: str, *, partial_received: bool = False):
        super().__init__(message)
        self.partial_received = partial_received


class _AllModelsFailedError(Exception):
    """Raised when every model in the fallback chain fails.

    This includes both LLM call errors and JSON parse errors (when a
    ``response_validator`` is provided to :meth:`GeminiAnalyzer._call_litellm`).

    The ``last_response_text`` attribute holds the raw text from the last model
    that *did* return a response (but whose JSON could not be validated), so
    callers can still attempt a best-effort text fallback.

    ``last_model`` and ``last_usage`` record the model name and token usage
    from the last attempt so callers can persist usage even on fallback.
    """

    def __init__(
        self,
        message: str,
        *,
        last_response_text: Optional[str] = None,
        last_model: Optional[str] = None,
        last_usage: Optional[Dict[str, Any]] = None,
        last_error_kind: Optional[str] = None,
    ):
        super().__init__(message)
        self.last_response_text = last_response_text
        self.last_model = last_model
        self.last_usage = last_usage or {}
        self.last_error_kind = last_error_kind


class _StructuredToolExtractionError(ValueError):
    """Tool-call extraction failed, optionally with an untrusted visible draft."""

    def __init__(self, message: str, *, draft_text: Optional[str] = None):
        super().__init__(message)
        self.draft_text = draft_text


def _looks_like_structured_submission_error(error: Any) -> bool:
    """Return whether an LLM failure is recoverable by re-submitting the tool payload."""
    explicit_kind = getattr(error, "last_error_kind", None)
    if explicit_kind is not None:
        return explicit_kind == "structured_submission"

    text = str(error or "").lower()
    recoverable_markers = (
        "tool_call_",
        "schema_validation_failed",
        "minimal_contract_failed",
        "parser_contract_failed",
        "json_root_not_object",
        "ambiguous_json",
        "empty_response",
    )
    if any(marker in text for marker in recoverable_markers):
        return True

    # Compatibility for callers that constructed the aggregate exception
    # before ``last_error_kind`` existed. Runtime exceptions always carry the
    # explicit terminal kind, so an earlier draft cannot turn a later network
    # failure into a structured retry.
    return bool(getattr(error, "last_response_text", None))


def _classify_litellm_failure(error: Any) -> str:
    """Classify the terminal provider attempt without consulting prior responses."""
    return (
        "structured_submission"
        if _looks_like_structured_submission_error(error)
        else "transport_provider"
    )


from src.utils.data_processing import normalize_report_signal_attribution


def _positive_price_from_any(value: Any) -> Optional[float]:
    if value is None or isinstance(value, (list, tuple, dict)):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed > 0 else None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"(?<!\d)(\d+(?:\.\d+)?)(?!\d)", text.replace(",", ""))
    if not match:
        return None
    try:
        parsed = float(match.group(1))
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) and parsed > 0 else None


def _valid_entry_leg(leg: Any) -> bool:
    if not isinstance(leg, dict):
        return False
    if not str(leg.get("leg_id") or "").strip():
        return False
    if not str(leg.get("role") or "").strip():
        return False
    prerequisites = leg.get("prerequisites")
    if not isinstance(prerequisites, list) or any(
        not isinstance(item, str) or not item.strip()
        for item in prerequisites
    ):
        return False
    try:
        allocation = float(leg.get("allocation_fraction"))
        sequence = int(leg.get("sequence"))
        max_executions = int(leg.get("max_executions", 1))
    except (TypeError, ValueError):
        return False
    if not (math.isfinite(allocation) and 0 < allocation <= 1 and sequence > 0 and max_executions > 0):
        return False
    price_rules = leg.get("price_rules")
    if not isinstance(price_rules, list) or not price_rules:
        return False
    has_valid_rule = False
    for rule in price_rules:
        if not isinstance(rule, dict):
            return False
        rule_type = str(rule.get("type") or "").strip()
        if rule_type != "close_between":
            return False
        low = _positive_price_from_any(rule.get("low"))
        high = _positive_price_from_any(rule.get("high"))
        if low is not None and high is not None and low <= high:
            has_valid_rule = True
        else:
            return False
    return has_valid_rule


def _entry_leg_bounds(entry_legs: Any) -> Optional[Tuple[float, float]]:
    if not isinstance(entry_legs, list) or not entry_legs:
        return None
    lows: List[float] = []
    highs: List[float] = []
    allocations: List[float] = []
    seen_ids: set[str] = set()
    seen_sequences: set[int] = set()
    for leg in entry_legs:
        if not _valid_entry_leg(leg):
            return None
        leg_id = str(leg.get("leg_id") or "").strip()
        sequence = int(leg.get("sequence"))
        if leg_id in seen_ids or sequence in seen_sequences:
            return None
        seen_ids.add(leg_id)
        seen_sequences.add(sequence)
        allocations.append(float(leg.get("allocation_fraction")))
        for rule in leg.get("price_rules") or []:
            if isinstance(rule, dict) and str(rule.get("type") or "").strip() == "close_between":
                low = _positive_price_from_any(rule.get("low"))
                high = _positive_price_from_any(rule.get("high"))
                if low is not None and high is not None:
                    lows.append(low)
                    highs.append(high)
                    break
    if not lows or not highs:
        return None
    total_allocation = sum(allocations)
    if total_allocation <= 0 or total_allocation > 1.02:
        return None
    return min(lows), max(highs)


def _valid_exit_legs(exit_legs: Any) -> bool:
    if not isinstance(exit_legs, list) or not exit_legs:
        return False
    allocations: List[float] = []
    targets: List[float] = []
    seen_ids: set[str] = set()
    seen_sequences: set[int] = set()
    sequences: List[int] = []
    for leg in exit_legs:
        if not isinstance(leg, dict):
            return False
        leg_id = str(leg.get("leg_id") or "").strip()
        role = str(leg.get("role") or "").strip()
        if not leg_id or not role or leg_id in seen_ids:
            return False
        seen_ids.add(leg_id)
        try:
            sequence = int(leg.get("sequence"))
            allocation = float(leg.get("allocation_fraction"))
        except (TypeError, ValueError):
            return False
        target = _positive_price_from_any(leg.get("target_price"))
        if (
            sequence <= 0
            or sequence in seen_sequences
            or not math.isfinite(allocation)
            or not 0 < allocation <= 1
            or target is None
        ):
            return False
        seen_sequences.add(sequence)
        sequences.append(sequence)
        allocations.append(allocation)
        targets.append(target)
    ordered_sequences = all(left < right for left, right in zip(sequences, sequences[1:]))
    strictly_increasing = all(left < right for left, right in zip(targets, targets[1:]))
    return ordered_sequences and abs(sum(allocations) - 1.0) <= 0.02 and strictly_increasing


def _buy_contract_missing_fields(result: "AnalysisResult", sniper_points: Dict[str, Any]) -> List[str]:
    action = normalize_decision_action(getattr(result, "action", None))
    if action not in {"buy", "add"}:
        return []

    missing: List[str] = []
    entry_legs = sniper_points.get("entry_legs")
    entry_bounds = _entry_leg_bounds(entry_legs)
    if sniper_points.get("entry_leg_schema_version") != "entry-legs-v1":
        missing.append("dashboard.battle_plan.sniper_points.entry_leg_schema_version")
    if entry_bounds is None:
        missing.append("dashboard.battle_plan.sniper_points.entry_legs")

    stop_loss = _positive_price_from_any(sniper_points.get("stop_loss"))
    take_profit = _positive_price_from_any(sniper_points.get("take_profit"))
    final_target = _positive_price_from_any(sniper_points.get("final_target")) or take_profit
    if stop_loss is None:
        missing.append("dashboard.battle_plan.sniper_points.stop_loss")
    if take_profit is None:
        missing.append("dashboard.battle_plan.sniper_points.take_profit")
    if final_target is None:
        missing.append("dashboard.battle_plan.sniper_points.final_target")

    if sniper_points.get("exit_leg_schema_version") != "exit-legs-v1":
        missing.append("dashboard.battle_plan.sniper_points.exit_leg_schema_version")
    if not _valid_exit_legs(sniper_points.get("exit_legs")):
        missing.append("dashboard.battle_plan.sniper_points.exit_legs")
    else:
        exit_targets = [
            _positive_price_from_any(leg.get("target_price"))
            for leg in sniper_points.get("exit_legs", [])
            if isinstance(leg, dict)
        ]
        if final_target is not None and exit_targets and abs(exit_targets[-1] - final_target) > 0.01:
            missing.append("dashboard.battle_plan.sniper_points.final_target_exit_mismatch")

    if (
        stop_loss is not None
        and final_target is not None
        and entry_bounds is not None
        and not (stop_loss < entry_bounds[0] <= entry_bounds[1] < final_target)
    ):
        missing.append("dashboard.battle_plan.sniper_points.entry_stop_target_order")
    elif stop_loss is not None and final_target is not None and stop_loss >= final_target:
        missing.append("dashboard.battle_plan.sniper_points.stop_target_order")
    return missing


def buy_contract_missing_fields(
    result: "AnalysisResult",
    sniper_points: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Return executable buy/add contract violations for downstream gates.

    Keeping this check reusable prevents a later score-alignment or
    position-aware resolution step from turning a non-executable report into
    a simulated buy.
    """
    if sniper_points is None:
        dashboard = result.dashboard if isinstance(result.dashboard, dict) else {}
        battle_plan = dashboard.get("battle_plan")
        battle_plan = battle_plan if isinstance(battle_plan, dict) else {}
        candidate = battle_plan.get("sniper_points")
        sniper_points = candidate if isinstance(candidate, dict) else {}
    missing = _buy_contract_missing_fields(result, sniper_points)
    if normalize_decision_action(getattr(result, "action", None)) in {"buy", "add"}:
        dashboard = result.dashboard if isinstance(result.dashboard, dict) else {}
        missing.extend(_phase_decision_missing_fields(dashboard))
    return list(dict.fromkeys(missing))


def _is_execution_contract_field(field: str) -> bool:
    """Whether a missing field should block execution, not report visibility."""

    return (
        field.startswith("dashboard.battle_plan.sniper_points.")
        or field.startswith("dashboard.phase_decision.")
        or field == "confidence_level"
    )


def _is_valid_trading_confidence_level(value: Any) -> bool:
    key = str(value or "").strip().lower()
    return key in {"高", "high", "中", "medium", "mid", "低", "low"}


def _native_tool_core_contract_missing_fields(data: Dict[str, Any]) -> List[str]:
    """Receive-side guard for fields that can alter trading behavior."""

    if data.get("report_schema_version") != ANALYSIS_REPORT_TOOL_SCHEMA_VERSION:
        return []
    missing: List[str] = []
    for field in ("raw_score", "adjusted_score", "sentiment_score"):
        if normalize_score(data.get(field)) is None:
            missing.append(field)
    if not isinstance(data.get("action"), str) or not normalize_decision_action(data.get("action")):
        missing.append("action")
    position_actions = data.get("position_actions")
    if not isinstance(position_actions, dict):
        missing.append("position_actions")
    else:
        for branch in ("empty", "holding"):
            branch_value = position_actions.get(branch)
            if not isinstance(branch_value, dict):
                missing.append(f"position_actions.{branch}")
                continue
            if (
                not isinstance(branch_value.get("action"), str)
                or not normalize_decision_action(branch_value.get("action"))
            ):
                missing.append(f"position_actions.{branch}.action")
    if not _is_valid_trading_confidence_level(data.get("confidence_level")):
        missing.append("confidence_level")
    return missing


def _phase_decision_missing_fields(dashboard: Dict[str, Any]) -> List[str]:
    phase_decision = dashboard.get("phase_decision")
    phase_decision = phase_decision if isinstance(phase_decision, dict) else {}
    missing: List[str] = []
    for field in ("action_window", "immediate_action", "next_check_time"):
        value = phase_decision.get(field)
        if not isinstance(value, str) or not value.strip():
            missing.append(f"dashboard.phase_decision.{field}")
    watch_conditions = phase_decision.get("watch_conditions")
    if (
        not isinstance(watch_conditions, list)
        or not watch_conditions
        or any(not isinstance(item, str) or not item.strip() for item in watch_conditions)
    ):
        missing.append("dashboard.phase_decision.watch_conditions")
    return missing


def _phase_entry_mode(dashboard: Dict[str, Any]) -> Optional[str]:
    phase_decision = dashboard.get("phase_decision")
    if not isinstance(phase_decision, dict):
        return None
    mode = str(phase_decision.get("entry_mode") or "").strip().lower()
    return mode if mode in {"none", "conditional", "immediate"} else None


def check_content_integrity(
    result: "AnalysisResult",
    *,
    require_phase_decision: bool = False,
) -> Tuple[bool, List[str]]:
    """
    Check mandatory fields for report content integrity.
    Returns (pass, missing_fields). Module-level for use by pipeline (agent weak mode).

    Note:
    - Required fields: missing → pass=False, added to missing_fields
    - Optional fields (e.g., signal_attribution): missing → pass=True and are not added to missing_fields
    """
    missing: List[str] = []

    def _is_blank_text(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return not value.strip()
        return True

    def _is_invalid_risk_alerts(value: Any) -> bool:
        return not isinstance(value, list)

    def _is_invalid_stop_loss(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, (list, tuple, dict)):
            return True
        if isinstance(value, str):
            return not value.strip()
        return False

    if result.sentiment_score is None:
        missing.append("sentiment_score")
    trade_action = normalize_decision_action(getattr(result, "action", None))
    advice = result.operation_advice
    if not trade_action and (not advice or not isinstance(advice, str) or _is_blank_text(advice)):
        missing.append("action")
    if not _is_valid_trading_confidence_level(getattr(result, "confidence_level", None)):
        missing.append("confidence_level")
    dash = result.dashboard if isinstance(result.dashboard, dict) else {}
    battle = dash.get("battle_plan")
    battle = battle if isinstance(battle, dict) else {}
    sp = battle.get("sniper_points")
    sp = sp if isinstance(sp, dict) else {}
    native_tool_contract = (
        getattr(result, "report_schema_version", None)
        == ANALYSIS_REPORT_TOOL_SCHEMA_VERSION
    )
    if native_tool_contract:
        position_actions = getattr(result, "position_actions", None)
        if not isinstance(position_actions, dict):
            missing.append("position_actions")
        else:
            for branch in ("empty", "holding"):
                branch_value = position_actions.get(branch)
                if not isinstance(branch_value, dict):
                    missing.append(f"position_actions.{branch}")
                    continue
                if not normalize_decision_action(branch_value.get("action")):
                    missing.append(f"position_actions.{branch}.action")
        missing.extend(_buy_contract_missing_fields(result, sp))
        entry_mode = _phase_entry_mode(dash)
        raw_action = normalize_decision_action(getattr(result, "action", None))
        if entry_mode in {"conditional", "immediate"} and raw_action not in {"buy", "add"}:
            missing.append("dashboard.phase_decision.entry_mode_action_mismatch")
        elif entry_mode == "none" and raw_action in {"buy", "add"}:
            missing.append("dashboard.phase_decision.entry_mode_action_mismatch")
    require_trade_phase_contract = trade_action in {"buy", "add", "reduce", "sell"}
    if require_trade_phase_contract:
        missing.extend(_phase_decision_missing_fields(dash))
    return len(missing) == 0, missing


def apply_placeholder_fill(result: "AnalysisResult", missing_fields: List[str]) -> None:
    """Fill missing mandatory fields with placeholders (in-place). Module-level for pipeline."""

    def _is_blank_text(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return not value.strip()
        return True

    def _is_invalid_risk_alerts(value: Any) -> bool:
        return not isinstance(value, list)

    def _is_invalid_stop_loss(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, (list, tuple, dict)):
            return True
        if isinstance(value, str):
            return not value.strip()
        return False

    report_language = normalize_report_language(getattr(result, "report_language", "zh"))
    placeholder = get_placeholder_text(report_language)
    phase_decision_placeholders = {
        "dashboard.phase_decision.action_window": _localized_text(
            report_language,
            en="Model did not provide a phase action window",
            zh="模型未提供阶段化行动窗口",
            ko="모델이 단계별 행동 구간을 제공하지 않았습니다",
        ),
        "dashboard.phase_decision.immediate_action": _localized_text(
            report_language,
            en="Model did not provide a phase-aware immediate action",
            zh="模型未提供阶段化即时动作",
            ko="모델이 단계 인식 즉시 동작을 제공하지 않았습니다",
        ),
        "dashboard.phase_decision.next_check_time": _localized_text(
            report_language,
            en="Model did not provide a next check point",
            zh="模型未提供下一次检查点",
            ko="모델이 다음 점검 시점을 제공하지 않았습니다",
        ),
        "dashboard.phase_decision.confidence_reason": _localized_text(
            report_language,
            en="Model did not provide a phase confidence rationale",
            zh="模型未提供阶段化置信度理由",
            ko="모델이 단계별 신뢰도 근거를 제공하지 않았습니다",
        ),
    }
    for field in missing_fields:
        if field == "sentiment_score":
            result.sentiment_score = 50
        elif field == "operation_advice":
            if _is_blank_text(result.operation_advice):
                result.operation_advice = placeholder
        elif field == "analysis_summary":
            if _is_blank_text(result.analysis_summary):
                result.analysis_summary = placeholder
        elif field == "dashboard.core_conclusion.one_sentence":
            if not result.dashboard:
                result.dashboard = {}
            core = result.dashboard.get("core_conclusion")
            if not isinstance(core, dict):
                core = {}
                result.dashboard["core_conclusion"] = core
            fallback_sentence = (
                result.analysis_summary
                or result.operation_advice
                or placeholder
            )
            if _is_blank_text(core.get("one_sentence")):
                result.dashboard["core_conclusion"]["one_sentence"] = fallback_sentence
        elif field == "dashboard.intelligence.risk_alerts":
            if not result.dashboard:
                result.dashboard = {}
            intelligence = result.dashboard.get("intelligence")
            if not isinstance(intelligence, dict):
                intelligence = {}
                result.dashboard["intelligence"] = intelligence
            if _is_invalid_risk_alerts(intelligence.get("risk_alerts")):
                risk_warning_values = _normalize_risk_warning_values(result.risk_warning)
                intelligence["risk_alerts"] = risk_warning_values
        elif field == "dashboard.battle_plan.sniper_points.stop_loss":
            if not result.dashboard:
                result.dashboard = {}
            battle_plan = result.dashboard.get("battle_plan")
            if not isinstance(battle_plan, dict):
                battle_plan = {}
                result.dashboard["battle_plan"] = battle_plan
            sniper_points = battle_plan.get("sniper_points")
            if not isinstance(sniper_points, dict):
                sniper_points = {}
                battle_plan["sniper_points"] = sniper_points
            if _is_invalid_stop_loss(sniper_points.get("stop_loss")):
                sniper_points["stop_loss"] = placeholder
        elif field.startswith("dashboard.phase_decision."):
            if not result.dashboard:
                result.dashboard = {}
            phase_decision = result.dashboard.get("phase_decision")
            if not isinstance(phase_decision, dict):
                phase_decision = {}
                result.dashboard["phase_decision"] = phase_decision
            if field == "dashboard.phase_decision.phase_context":
                if not isinstance(phase_decision.get("phase_context"), dict):
                    phase_decision["phase_context"] = {}
            elif field == "dashboard.phase_decision.watch_conditions":
                if not isinstance(phase_decision.get("watch_conditions"), list):
                    phase_decision["watch_conditions"] = []
            elif field == "dashboard.phase_decision.data_limitations":
                if not isinstance(phase_decision.get("data_limitations"), list):
                    phase_decision["data_limitations"] = []
            elif field in phase_decision_placeholders:
                if _is_blank_text(phase_decision.get(field.rsplit(".", 1)[-1])):
                    phase_decision[field.rsplit(".", 1)[-1]] = phase_decision_placeholders[field]


# ---------- chip_structure fallback (Issue #589) ----------

_CHIP_KEYS: tuple = ("profit_ratio", "avg_cost", "concentration", "chip_health")


def _is_value_placeholder(v: Any) -> bool:
    """True if value is empty or placeholder (N/A, 数据缺失, etc.)."""
    return is_chip_placeholder_value(v)


_RISK_WARNING_PLACEHOLDER_TEXTS = {
    "",
    "n/a",
    "na",
    "none",
    "null",
    "unknown",
    "tbd",
    "暂无",
    "待补充",
    "数据缺失",
    "未知",
    "无",
}

_STRUCTURAL_RISK_PHRASE_HINTS = (
    "重大利空",
    "重大风险",
    "关键风险",
    "减持",
    "高位减持",
    "退市",
    "退市风险",
    "停牌",
    "重大问询",
    "处罚",
    "限售",
    "违规",
    "违规风险",
    "诉讼",
    "问询",
    "监管",
    "财务",
    "审计",
    "爆雷",
    "暴雷",
    "违约",
    "违约风险",
    "流动性危机",
    "债务",
    "清算",
    "破产",
    "重大变脸",
    "major risk",
    "material adverse",
    "suspension",
    "delisting",
    "regulatory",
    "downgrade",
    "liquidity",
    "default",
)

_CAPITAL_FLOW_UNAVAILABLE_STATUS = {
    "not_supported",
    "not supported",
    "unsupported",
    "unavailable",
    "not_available",
    "not available",
    "none",
    "na",
    "n/a",
    "null",
    "missing",
}

_CAPITAL_FLOW_FAILED_STATUS = {
    "failed",
    "error",
    "fetch failed",
    "fetch_failed",
    "timeout",
    "timed out",
    "transport error",
    "provider error",
}


def _is_meaningful_text(value: Any) -> bool:
    text = str(value).strip() if value is not None else ""
    if not text:
        return False
    lowered = text.strip().lower()
    return lowered not in _RISK_WARNING_PLACEHOLDER_TEXTS


def _safe_float(v: Any, default: float = 0.0) -> float:
    """Safely convert to float; return default on failure. Private helper for chip fill."""
    if v is None:
        return default
    if isinstance(v, (int, float)):
        try:
            return default if math.isnan(float(v)) else float(v)
        except (ValueError, TypeError):
            return default
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return default


def _coerce_chip_metric(v: Any) -> Optional[float]:
    """Convert chip metrics while preserving the distinction between missing and zero."""
    if v is None:
        return None
    try:
        numeric = float(v)
    except (TypeError, ValueError):
        try:
            numeric = float(str(v).strip())
        except (TypeError, ValueError):
            return None
    return None if math.isnan(numeric) else numeric


_BULLISH_TREND_HINTS: Tuple[str, ...] = (
    "多头排列",
    "持续上涨",
    "趋势向上",
    "上升趋势",
    "向上发散",
    "bullish",
    "uptrend",
)
_WEAK_BULLISH_TREND_HINTS: Tuple[str, ...] = ("弱势多头",)
_BEARISH_TREND_HINTS: Tuple[str, ...] = (
    "空头排列",
    "持续下跌",
    "趋势向下",
    "下降趋势",
    "向下发散",
    "bearish",
    "downtrend",
)
_WEAK_BEARISH_TREND_HINTS: Tuple[str, ...] = ("弱势空头",)
_NEGATION_TOKENS: Tuple[str, ...] = (
    "不是",
    "并非",
    "并未",
    "没有",
    "尚不",
    "尚未",
    "未",
    "无",
    "不属",
    "非",
    "not ",
    "no ",
)
_NEGATION_BREAK_CHARS: Tuple[str, ...] = (",", ".", ";", ":", "!", "?", "，", "。", "；", "：", "！", "？", "\n")
_NEGATION_LOOKBACK_CHARS = 16
_NEGATION_MAX_GAP_CHARS = 8
_NEGATION_SCOPE_BREAK_TOKENS: Tuple[str, ...] = (
    "而是",
    "但是",
    "但",
    "反而",
    "反倒",
    "转为",
    "转成",
    "改为",
    "改成",
    " but ",
    " instead ",
    " rather ",
)
_SINGLE_CHAR_NEGATION_GAP_PREFIXES: Tuple[str, ...] = (
    "形成",
    "出现",
    "进入",
    "转为",
    "转成",
    "构成",
    "呈现",
    "显示",
    "属于",
    "是",
    "有",
    "能",
    "见",
    "站",
    "守",
    "破",
)


def _normalize_prompt_reason_items(items: Any) -> List[str]:
    """Normalize prompt reason/risk items into a clean string list."""
    if not isinstance(items, list):
        return []
    normalized: List[str] = []
    for item in items:
        text = str(item).strip()
        if text:
            normalized.append(text)
    return normalized


def _contains_trend_hint(text: str, hints: Tuple[str, ...]) -> bool:
    """Return True when text contains a non-negated strong trend hint."""
    lowered = text.strip().lower()

    def _has_negation_scope_break(gap: str) -> bool:
        normalized_gap = gap.lower()
        for token in _NEGATION_SCOPE_BREAK_TOKENS:
            token_index = normalized_gap.find(token)
            if token_index > 0:
                return True
        return False

    def _is_valid_negation_gap(token: str, gap: str) -> bool:
        if not gap:
            return True
        if token not in {"未", "无", "非"}:
            return True
        return any(gap.startswith(prefix) for prefix in _SINGLE_CHAR_NEGATION_GAP_PREFIXES)

    def _is_negated_match(index: int) -> bool:
        prefix = lowered[max(0, index - _NEGATION_LOOKBACK_CHARS):index]
        for token in _NEGATION_TOKENS:
            token_index = prefix.rfind(token)
            if token_index < 0:
                continue
            gap = prefix[token_index + len(token):]
            if any(char in gap for char in _NEGATION_BREAK_CHARS):
                continue
            stripped_gap = gap.strip()
            if len(stripped_gap) > _NEGATION_MAX_GAP_CHARS:
                continue
            if _has_negation_scope_break(stripped_gap):
                continue
            if not _is_valid_negation_gap(token, stripped_gap):
                continue
            return True
        return False

    for hint in hints:
        keyword = hint.lower()
        start = 0
        while True:
            index = lowered.find(keyword, start)
            if index < 0:
                break
            if not _is_negated_match(index):
                return True
            start = index + len(keyword)
    return False


def _infer_trend_direction(trend: Dict[str, Any]) -> str:
    """Infer the final trend direction from trend_status and ma_alignment."""
    combined = " ".join(
        str(trend.get(key, "")).strip()
        for key in ("trend_status", "ma_alignment")
        if str(trend.get(key, "")).strip()
    )
    if not combined:
        return "neutral"
    lowered = combined.lower()
    normalized = lowered.replace(" ", "")
    has_bullish = (
        _contains_trend_hint(combined, _BULLISH_TREND_HINTS + _WEAK_BULLISH_TREND_HINTS)
        or "ma5>ma10>ma20" in normalized
        or (
            "ma5>ma10" in normalized
            and any(pattern in normalized for pattern in ("ma10≤ma20", "ma10<=ma20"))
        )
    )
    has_bearish = (
        _contains_trend_hint(combined, _BEARISH_TREND_HINTS + _WEAK_BEARISH_TREND_HINTS)
        or "ma5<ma10<ma20" in normalized
        or (
            "ma5<ma10" in normalized
            and any(pattern in normalized for pattern in ("ma10≥ma20", "ma10>=ma20"))
        )
    )
    if has_bullish and not has_bearish:
        return "bullish"
    if has_bearish and not has_bullish:
        return "bearish"
    return "neutral"


def _filter_conflicting_trend_items(items: List[str], conflict_hints: Tuple[str, ...]) -> List[str]:
    """Drop reasons that directly conflict with the final trend direction."""
    return [item for item in items if not _contains_trend_hint(item, conflict_hints)]


def _sanitize_trend_analysis_for_prompt(
    trend: Any,
    *,
    volume_change_ratio: Any = None,
) -> Dict[str, Any]:
    """Clean prompt-only trend hints on a derived copy without touching runtime/provider config."""
    trend_dict = dict(trend) if isinstance(trend, dict) else {}
    signal_reasons = _normalize_prompt_reason_items(trend_dict.get("signal_reasons"))
    risk_factors = _normalize_prompt_reason_items(trend_dict.get("risk_factors"))
    prompt_notes: List[str] = []
    trend_direction = _infer_trend_direction(trend_dict)

    if trend_direction == "bearish":
        filtered_signal_reasons = _filter_conflicting_trend_items(
            signal_reasons,
            _BULLISH_TREND_HINTS + _WEAK_BULLISH_TREND_HINTS,
        )
        if len(filtered_signal_reasons) != len(signal_reasons):
            prompt_notes.append("当前技术结构偏空，已剔除与空头主判断直接冲突的看多结构理由。")
        signal_reasons = filtered_signal_reasons
        prompt_notes.append(
            "若新闻、业绩或政策催化偏多，只能表述为“事件先行、技术待确认”或“基本面偏多，但技术面尚未确认”，严禁写成确定性买点。"
        )
    elif trend_direction == "bullish":
        filtered_signal_reasons = _filter_conflicting_trend_items(
            signal_reasons,
            _BEARISH_TREND_HINTS + _WEAK_BEARISH_TREND_HINTS,
        )
        if len(filtered_signal_reasons) != len(signal_reasons):
            prompt_notes.append("当前技术结构偏多，已剔除与多头主判断直接冲突的空头结构理由。")
        signal_reasons = filtered_signal_reasons
        filtered_risk_factors = _filter_conflicting_trend_items(
            risk_factors,
            _BEARISH_TREND_HINTS + _WEAK_BEARISH_TREND_HINTS,
        )
        if len(filtered_risk_factors) != len(risk_factors):
            prompt_notes.append("当前技术结构偏多，已剔除与多头主判断直接冲突的空头结构风险表述。")
        risk_factors = filtered_risk_factors

    parsed_volume_change = _safe_float(volume_change_ratio, default=math.nan)
    if math.isfinite(parsed_volume_change) and parsed_volume_change > 10:
        prompt_notes.append(
            f"成交量较昨日变化约 {parsed_volume_change:.2f} 倍，可能存在异常数据或一次性冲量；量能信号必须降权解读，不能机械视为强确认。"
        )

    trend_dict["signal_reasons"] = signal_reasons
    trend_dict["risk_factors"] = risk_factors
    trend_dict["prompt_consistency_notes"] = prompt_notes
    trend_dict["prompt_trend_direction"] = trend_direction
    return trend_dict


def _derive_chip_health(profit_ratio: float, concentration_90: float, language: str = "zh") -> str:
    """Derive chip_health from profit_ratio and concentration_90."""
    if profit_ratio >= 0.9:
        return localize_chip_health("警惕", language)  # 获利盘极高
    if concentration_90 >= 0.25:
        return localize_chip_health("警惕", language)  # 筹码分散
    if concentration_90 < 0.15 and 0.3 <= profit_ratio < 0.9:
        return localize_chip_health("健康", language)  # 集中且获利比例适中
    return localize_chip_health("一般", language)


def _build_chip_structure_from_data(chip_data: Any, language: str = "zh") -> Dict[str, Any]:
    """Build chip_structure dict from ChipDistribution or dict."""
    if hasattr(chip_data, "profit_ratio"):
        pr = _safe_float(chip_data.profit_ratio)
        ac = chip_data.avg_cost
        c90 = _safe_float(chip_data.concentration_90)
    else:
        d = chip_data if isinstance(chip_data, dict) else {}
        pr = _safe_float(d.get("profit_ratio"))
        ac = d.get("avg_cost")
        c90 = _safe_float(d.get("concentration_90"))
    chip_health = _derive_chip_health(pr, c90, language=language)
    return {
        "profit_ratio": f"{pr:.1%}",
        "avg_cost": ac if (ac is not None and _safe_float(ac) != 0.0) else "N/A",
        "concentration": f"{c90:.2%}",
        "chip_health": chip_health,
    }


def _has_meaningful_chip_data(chip_data: Any) -> bool:
    """Return True when chip data has the core metrics required for reporting."""
    if not chip_data:
        return False
    if hasattr(chip_data, "avg_cost"):
        avg_cost = _coerce_chip_metric(getattr(chip_data, "avg_cost", None))
        concentration_90 = _coerce_chip_metric(getattr(chip_data, "concentration_90", None))
        concentration_70 = _coerce_chip_metric(getattr(chip_data, "concentration_70", None))
    else:
        d = chip_data if isinstance(chip_data, dict) else {}
        avg_cost = _coerce_chip_metric(d.get("avg_cost"))
        concentration_90_value = d.get("concentration_90")
        if concentration_90_value is None:
            concentration_90_value = d.get("concentration")
        concentration_90 = _coerce_chip_metric(concentration_90_value)
        concentration_70 = _coerce_chip_metric(d.get("concentration_70"))
    return (
        avg_cost is not None
        and avg_cost > 0
        and (
            (concentration_90 is not None and concentration_90 >= 0)
            or (concentration_70 is not None and concentration_70 >= 0)
        )
    )


def _chip_not_applicable_text(language: str) -> str:
    return _localized_text(
        language,
        zh="指数/ETF不适用个股筹码分布指标，本项不参与数据质量或风险判断。",
        en=(
            "Stock-specific chip distribution is not applicable to an index/ETF; "
            "this is not treated as missing or negative evidence."
        ),
        ko=(
            "지수/ETF에는 개별 종목 칩 분포 지표가 적용되지 않으며 "
            "결측 또는 부정적 근거로 처리하지 않습니다."
        ),
    )


def _mark_chip_structure_unavailable(
    result: "AnalysisResult",
    language: str,
    *,
    not_applicable: bool = False,
) -> None:
    if not result:
        return
    if not isinstance(getattr(result, "dashboard", None), dict):
        result.dashboard = {}
    data_perspective = result.dashboard.get("data_perspective")
    if not isinstance(data_perspective, dict):
        data_perspective = {}
        result.dashboard["data_perspective"] = data_perspective
    data_perspective["chip_structure"] = {}
    data_perspective["chip_applicability"] = "not_applicable" if not_applicable else "missing"
    data_perspective["chip_unavailable_reason"] = (
        _chip_not_applicable_text(language)
        if not_applicable
        else get_chip_unavailable_text(language)
    )


def normalize_chip_structure_availability(
    result: "AnalysisResult",
    chip_data: Any,
    *,
    is_index_etf: bool = False,
) -> None:
    """Normalize chip evidence while distinguishing missing from not-applicable."""
    if not result:
        return
    language = getattr(result, "report_language", "zh")
    if is_index_etf:
        _mark_chip_structure_unavailable(result, language, not_applicable=True)
        return
    if _has_meaningful_chip_data(chip_data):
        fill_chip_structure_if_needed(result, chip_data)
        return
    _mark_chip_structure_unavailable(result, language, not_applicable=False)


def normalize_index_etf_checklist(
    result: "AnalysisResult",
    *,
    is_index_etf: bool = False,
) -> None:
    """Replace stock-only ETF checklist items without relying on item positions."""
    if not result or not is_index_etf or not isinstance(result.dashboard, dict):
        return
    battle_plan = result.dashboard.get("battle_plan")
    if not isinstance(battle_plan, dict):
        return
    checklist = battle_plan.get("action_checklist")
    if not isinstance(checklist, list):
        return

    language = normalize_report_language(getattr(result, "report_language", "zh"))
    normalized_items: List[str] = []
    for item in checklist:
        text = str(item or "").strip()
        lowered = text.lower()
        is_chip_item = "筹码" in text or "chip" in lowered
        is_stock_valuation_item = bool(
            "市盈率" in text
            or "市净率" in text
            or re.search(r"(?<![a-z])p/?[eb](?![a-z])", lowered)
        )
        if is_chip_item:
            normalized_items.append(
                _localized_text(
                    language,
                    zh="➖ ETF不适用个股筹码分布；本项改以成交额、换手率及流动性进行判断",
                    en=(
                        "➖ Stock-specific chip distribution is not applicable to ETFs; "
                        "assess turnover and liquidity instead"
                    ),
                    ko=(
                        "➖ ETF에는 개별 종목 칩 분포가 적용되지 않으며 "
                        "거래대금·회전율·유동성으로 평가합니다"
                    ),
                )
            )
            continue
        if is_stock_valuation_item:
            normalized_items.append(
                _localized_text(
                    language,
                    zh="➖ 个股PE/PB不适用于ETF；若无跟踪指数估值分位，本项不作负面判断",
                    en=(
                        "➖ Stock-level P/E and P/B are not applicable to ETFs; "
                        "absence of index valuation data is not negative evidence"
                    ),
                    ko=(
                        "➖ 개별 종목 PER/PBR은 ETF에 적용되지 않으며 "
                        "지수 밸류에이션 부재를 부정적 근거로 보지 않습니다"
                    ),
                )
            )
            continue
        normalized_items.append(text)
    battle_plan["action_checklist"] = normalized_items


def fill_chip_structure_if_needed(result: "AnalysisResult", chip_data: Any) -> None:
    """When chip_data exists, fill chip_structure placeholder fields from chip_data (in-place)."""
    if not result or not _has_meaningful_chip_data(chip_data):
        return
    try:
        if not result.dashboard:
            result.dashboard = {}
        dash = result.dashboard
        # Use `or {}` rather than setdefault so that an explicit `null` from LLM is also replaced
        dp = dash.get("data_perspective") or {}
        dash["data_perspective"] = dp
        cs = dp.get("chip_structure") or {}
        filled = _build_chip_structure_from_data(
            chip_data,
            language=getattr(result, "report_language", "zh"),
        )
        # Start from a copy of cs to preserve any extra keys the LLM may have added
        merged = dict(cs)
        for k in _CHIP_KEYS:
            if _is_value_placeholder(merged.get(k)):
                merged[k] = filled[k]
        if merged != cs:
            dp["chip_structure"] = merged
            logger.info("[chip_structure] Filled placeholder chip fields from data source (Issue #589)")
    except Exception as e:
        logger.warning("[chip_structure] Fill failed, skipping: %s", e)


_PRICE_POS_KEYS = ("ma5", "ma10", "ma20", "bias_ma5", "bias_status", "current_price", "support_level", "resistance_level")


def fill_price_position_if_needed(
    result: "AnalysisResult",
    trend_result: Any = None,
    realtime_quote: Any = None,
) -> None:
    """Fill missing price_position fields from trend_result / realtime data (in-place)."""
    if not result:
        return
    try:
        if not result.dashboard:
            result.dashboard = {}
        dash = result.dashboard
        dp = dash.get("data_perspective") or {}
        dash["data_perspective"] = dp
        pp = dp.get("price_position") or {}

        computed: Dict[str, Any] = {}
        if trend_result:
            tr = trend_result if isinstance(trend_result, dict) else (
                trend_result.__dict__ if hasattr(trend_result, "__dict__") else {}
            )
            computed["ma5"] = tr.get("ma5")
            computed["ma10"] = tr.get("ma10")
            computed["ma20"] = tr.get("ma20")
            computed["bias_ma5"] = tr.get("bias_ma5")
            computed["current_price"] = tr.get("current_price")
            support_levels = tr.get("support_levels") or []
            resistance_levels = tr.get("resistance_levels") or []
            if support_levels:
                computed["support_level"] = support_levels[0]
            if resistance_levels:
                computed["resistance_level"] = resistance_levels[0]
        if realtime_quote:
            rq = realtime_quote if isinstance(realtime_quote, dict) else (
                realtime_quote.to_dict() if hasattr(realtime_quote, "to_dict") else {}
            )
            if _is_value_placeholder(computed.get("current_price")):
                computed["current_price"] = rq.get("price")

        filled = False
        for k in _PRICE_POS_KEYS:
            if _is_value_placeholder(pp.get(k)) and not _is_value_placeholder(computed.get(k)):
                pp[k] = computed[k]
                filled = True
        if filled:
            dp["price_position"] = pp
            logger.info("[price_position] Filled placeholder fields from computed data")
    except Exception as e:
        logger.warning("[price_position] Fill failed, skipping: %s", e)


def stabilize_decision_with_structure(
    result: "AnalysisResult",
    trend_result: Any = None,
    fundamental_context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Calibrate aggressive buy/sell advice with price levels and capital flow.

    The LLM can overreact to one-day price movement.  This guard keeps the
    public `decision_type` enum stable while allowing richer neutral wording
    such as 震荡/洗盘观察 when support, resistance, and fund flow do not confirm
    an immediate buy/sell action.
    """
    if not result:
        return

    try:
        language = normalize_report_language(getattr(result, "report_language", "zh"))
        dashboard = result.dashboard if isinstance(result.dashboard, dict) else {}
        data_perspective = dashboard.get("data_perspective") if isinstance(dashboard, dict) else {}
        if not isinstance(data_perspective, dict):
            data_perspective = {}
        price_position = data_perspective.get("price_position")
        if not isinstance(price_position, dict):
            price_position = {}

        trend_dict = _as_dict_for_decision_guard(trend_result)
        current_price = _first_numeric_value(
            getattr(result, "current_price", None),
            price_position.get("current_price"),
            trend_dict.get("current_price"),
        )
        support = _first_numeric_value(
            price_position.get("support_level"),
            _first_list_value(trend_dict.get("support_levels")),
        )
        resistance = _first_numeric_value(
            price_position.get("resistance_level"),
            _first_list_value(trend_dict.get("resistance_levels")),
        )
        decision_type = infer_decision_type_from_advice(
            getattr(result, "decision_type", ""),
            default=getattr(result, "decision_type", "hold") or "hold",
        )
        decision_type = decision_type if decision_type in {"buy", "hold", "sell"} else "hold"
        flow_bias, flow_reason = _capital_flow_bias_with_status(fundamental_context)
        if flow_bias == "unavailable":
            if isinstance(fundamental_context, dict) and "capital_flow" in fundamental_context:
                _set_decision_stability_unavailable(
                    result,
                    language,
                    current_price=current_price,
                    support=support,
                    resistance=resistance,
                    flow_status=flow_reason,
                )
            return

        if current_price is None:
            return

        broke_support = support is not None and current_price < support * 0.985
        near_support = support is not None and not broke_support and current_price <= support * 1.03
        breakout = resistance is not None and current_price > resistance * 1.01
        near_resistance = (
            resistance is not None
            and not breakout
            and current_price >= resistance * 0.97
        )
        mid_range = (
            support is not None
            and resistance is not None
            and support * 1.03 < current_price < resistance * 0.97
        )

        has_significant_risk = _has_structural_risk_alert(result)

        if decision_type == "buy":
            if near_resistance and flow_bias != "inflow":
                _downgrade_to_structural_hold(
                    result,
                    language,
                    advice_key="range",
                    reason_key="buy_near_resistance",
                    current_price=current_price,
                    support=support,
                    resistance=resistance,
                    flow_bias=flow_bias,
                )
            elif flow_bias == "outflow" and not breakout:
                _downgrade_to_structural_hold(
                    result,
                    language,
                    advice_key="range",
                    reason_key="buy_with_outflow",
                    current_price=current_price,
                    support=support,
                    resistance=resistance,
                    flow_bias=flow_bias,
                )
            elif mid_range and flow_bias == "neutral":
                _downgrade_to_structural_hold(
                    result,
                    language,
                    advice_key="range",
                    reason_key="hold_mid_range",
                    current_price=current_price,
                    support=support,
                    resistance=resistance,
                    flow_bias=flow_bias,
                )
        elif decision_type == "sell":
            if near_support and (flow_bias != "outflow") and not has_significant_risk:
                _downgrade_to_structural_hold(
                    result,
                    language,
                    advice_key="shakeout",
                    reason_key="sell_near_support",
                    current_price=current_price,
                    support=support,
                    resistance=resistance,
                    flow_bias=flow_bias,
                )
            elif flow_bias == "inflow" and not broke_support and not has_significant_risk:
                _downgrade_to_structural_hold(
                    result,
                    language,
                    advice_key="hold",
                    reason_key="sell_with_inflow",
                    current_price=current_price,
                    support=support,
                    resistance=resistance,
                    flow_bias=flow_bias,
                )
        elif decision_type == "hold":
            change_pct = _first_numeric_value(getattr(result, "change_pct", None))
            if change_pct is not None and change_pct < 0 and near_support and flow_bias != "outflow":
                _set_structural_hold_wording(
                    result,
                    language,
                    advice_key="shakeout",
                    reason_key="hold_shakeout",
                    current_price=current_price,
                    support=support,
                    resistance=resistance,
                    flow_bias=flow_bias,
                )
            elif mid_range and flow_bias == "neutral":
                _set_structural_hold_wording(
                    result,
                    language,
                    advice_key="range",
                    reason_key="hold_mid_range",
                    current_price=current_price,
                    support=support,
                    resistance=resistance,
                    flow_bias=flow_bias,
                )
        _sync_stability_dashboard_fields(result)
    except Exception as exc:
        logger.warning("[decision_stability] skipped: %s", exc)


def _has_structural_risk_alert(result: "AnalysisResult") -> bool:
    dashboard = result.dashboard if isinstance(result.dashboard, dict) else {}

    risk_text = getattr(result, "risk_warning", "")
    if _is_significant_structural_risk(risk_text):
        return True

    intelligence = dashboard.get("intelligence") if isinstance(dashboard, dict) else None
    if isinstance(intelligence, dict):
        risk_alerts = intelligence.get("risk_alerts")
        if isinstance(risk_alerts, str):
            if _is_significant_structural_risk(risk_alerts):
                return True
        elif isinstance(risk_alerts, (list, tuple, set)):
            if any(_is_significant_structural_risk(item) for item in risk_alerts):
                return True

    core_conclusion = dashboard.get("core_conclusion") if isinstance(dashboard, dict) else None
    if isinstance(core_conclusion, dict):
        signal_type = str(core_conclusion.get("signal_type", "")).strip()
        if _is_significant_structural_risk(signal_type):
            return True
    return False


def _is_significant_structural_risk(value: Any) -> bool:
    text = str(value or "").strip()
    if not _is_meaningful_text(text):
        return False

    normalized = text.lower()
    if any(keyword in normalized for keyword in _STRUCTURAL_RISK_PHRASE_HINTS):
        return True

    return "重大" in text and "风险" in normalized


def _sync_stability_dashboard_fields(result: "AnalysisResult") -> None:
    dashboard = result.dashboard if isinstance(result.dashboard, dict) else {}
    result.dashboard = dashboard
    dashboard["sentiment_score"] = getattr(result, "sentiment_score", None)
    dashboard["operation_advice"] = getattr(result, "operation_advice", None)
    dashboard["decision_type"] = getattr(result, "decision_type", None)


def _as_dict_for_decision_guard(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "to_dict"):
        try:
            converted = value.to_dict()
            return converted if isinstance(converted, dict) else {}
        except Exception:
            return {}
    if hasattr(value, "__dict__"):
        return dict(value.__dict__)
    return {}


def _first_list_value(value: Any) -> Any:
    if isinstance(value, (list, tuple)) and value:
        return value[0]
    return value


def _coerce_numeric_value(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        if math.isfinite(float(value)):
            return float(value)
        return None
    text = str(value).replace(",", "").replace("，", "").strip()
    if not text or text.upper() in {"N/A", "NA", "NONE", "NULL"}:
        return None
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _first_numeric_value(*values: Any) -> Optional[float]:
    for value in values:
        if isinstance(value, (list, tuple)):
            nested = _first_numeric_value(*value)
            if nested is not None:
                return nested
            continue
        numeric = _coerce_numeric_value(value)
        if numeric is not None:
            return numeric
    return None


def _capital_flow_bias(fundamental_context: Optional[Dict[str, Any]]) -> str:
    return _capital_flow_bias_with_status(fundamental_context)[0]


def _capital_flow_bias_with_status(
    fundamental_context: Optional[Dict[str, Any]],
) -> tuple[str, str]:
    if not isinstance(fundamental_context, dict):
        return "unavailable", "invalid_context"
    block = fundamental_context.get("capital_flow")
    if not isinstance(block, dict):
        return "unavailable", "capital_flow_block_missing"
    status = str(block.get("status") or "").strip().lower()
    normalized_status = status.replace("-", " ").replace("_", " ").strip()
    if normalized_status in _CAPITAL_FLOW_FAILED_STATUS:
        return "unavailable", status or "failed"
    if normalized_status in _CAPITAL_FLOW_UNAVAILABLE_STATUS or "not supported" in normalized_status:
        return "unavailable", status or "not_supported"
    data = block.get("data") if isinstance(block.get("data"), dict) else block
    stock_flow = data.get("stock_flow") if isinstance(data, dict) else None
    if not isinstance(stock_flow, dict) or not stock_flow:
        return "unavailable", "empty_stock_flow"

    def _flow_direction(value: Optional[float]) -> Optional[str]:
        if value is None or value == 0:
            return None
        return "inflow" if value > 0 else "outflow"

    numeric_values = [
        _coerce_numeric_value(stock_flow.get("main_net_inflow")),
        _coerce_numeric_value(stock_flow.get("inflow_5d")),
        _coerce_numeric_value(stock_flow.get("inflow_10d")),
    ]
    if all(value is None for value in numeric_values):
        return "unavailable", "missing_or_na_flow_fields"

    ordered_signals = [
        _flow_direction(value) for value in numeric_values
    ]
    directions = {signal for signal in ordered_signals if signal is not None}
    if not directions or len(directions) > 1:
        return "neutral", "conflict_or_missing"
    for signal in ordered_signals:
        if signal is not None:
            return signal, "ok"
    return "neutral", "neutral"


def _capital_flow_unavailable_kind(reason: str) -> str:
    normalized = str(reason or "").strip().lower().replace("-", " ").replace("_", " ")
    if "not supported" in normalized or "unsupported" in normalized or "not applicable" in normalized:
        return "not_supported"
    if normalized in _CAPITAL_FLOW_FAILED_STATUS or any(
        token in normalized for token in ("failed", "error", "timeout", "timed out")
    ):
        return "failed"
    return "missing"


def _capital_flow_status_for_stability(reason: str, language: str) -> str:
    kind = _capital_flow_unavailable_kind(reason)
    if kind == "not_supported":
        return "市场资金流服务不适用或暂不支持" if language == "zh" else "Capital flow source not applicable or unsupported"
    if kind == "failed":
        return "资金流获取失败" if language == "zh" else "capital flow fetch failed"
    normalized = str(reason or "").strip().lower()
    if "empty_stock_flow" in normalized or "missing" in normalized:
        return "资金流数据缺失" if language == "zh" else "capital flow data unavailable"
    return "资金流数据不可用" if language == "zh" else "capital flow unavailable"


def _set_decision_stability_unavailable(
    result: "AnalysisResult",
    language: str,
    *,
    current_price: Optional[float],
    support: Optional[float],
    resistance: Optional[float],
    flow_status: str,
) -> None:
    dashboard = result.dashboard if isinstance(result.dashboard, dict) else {}
    result.dashboard = dashboard
    flow_kind = _capital_flow_unavailable_kind(flow_status)
    if language == "zh":
        reason_text = (
            "资金流不适用，未使用资金流校准"
            if flow_kind == "not_supported"
            else "资金流获取失败，未使用资金流校准"
            if flow_kind == "failed"
            else "资金流不可用，未使用资金流校准"
        )
    else:
        reason_text = (
            "Capital flow not applicable; stability calibration not applied"
            if flow_kind == "not_supported"
            else "Capital flow fetch failed; stability calibration not applied"
            if flow_kind == "failed"
            else "Capital flow unavailable; stability calibration not applied"
        )
    dashboard["decision_stability"] = {
        "applied": False,
        "reason": reason_text,
        "capital_flow_status": _capital_flow_status_for_stability(flow_status, language),
        "capital_flow_unavailable_kind": flow_kind,
        "current_price": current_price,
        "support": support,
        "resistance": resistance,
        "capital_flow_bias": "unavailable",
    }
    limitations = list(getattr(result, "limitations", None) or [])
    limitation_code = (
        "capital_flow_not_supported"
        if flow_kind == "not_supported"
        else "capital_flow_failed"
        if flow_kind == "failed"
        else "capital_flow_unavailable"
    )
    already_recorded = limitation_code in limitations
    if not already_recorded:
        limitations.append(limitation_code)
    result.limitations = limitations
    confidence_key = str(getattr(result, "confidence_level", "") or "").strip().lower()
    if flow_kind == "failed" and not already_recorded:
        if confidence_key in {"高", "high"}:
            result.confidence_level = localize_confidence_level("中", language)
        elif confidence_key in {"中", "medium", "mid"}:
            result.confidence_level = localize_confidence_level("低", language)
    _sync_stability_dashboard_fields(result)


def _record_decision_score_calibration(
    result: "AnalysisResult",
    *,
    raw_score: int,
    adjusted_score: int,
    final_action: str,
    guardrail_reason: Optional[str],
) -> None:
    dashboard = result.dashboard if isinstance(result.dashboard, dict) else {}
    result.dashboard = dashboard
    calibration = score_band_metadata(raw_score)
    calibration.update(
        {
            "raw_score": raw_score,
            "adjusted_score": adjusted_score,
            "final_action": final_action,
        }
    )
    if guardrail_reason:
        calibration["guardrail_reason"] = guardrail_reason
    dashboard["decision_score_calibration"] = calibration


def _bound_hold_watch_sentiment_score(
    result: "AnalysisResult",
    *,
    reason: Optional[str] = None,
    final_action: str = "watch",
) -> None:
    """Record a neutral guardrail candidate without inventing a score.

    A prior implementation forced every neutralized result into 45-59.  That
    destroyed the model score and made optional/partial evidence look like a
    real adjustment.  Final contract normalization now requires an explicit,
    structured adjusted score before an action can leave its score band.
    """
    try:
        score = int(getattr(result, "sentiment_score", 50))
    except (TypeError, ValueError):
        score = 50
    _record_decision_score_calibration(
        result,
        raw_score=score,
        adjusted_score=score,
        final_action=final_action,
        guardrail_reason=None,
    )


def _apply_hold_watch_dashboard(
    result: "AnalysisResult",
    language: str,
    *,
    advice: str,
    reason: str,
    current_price: Optional[float],
    support: Optional[float],
    resistance: Optional[float],
    flow_bias: str,
    no_position: str,
    has_position: str,
    capital_flow_status: Optional[str] = None,
) -> None:
    result.operation_advice = advice

    dashboard = result.dashboard if isinstance(result.dashboard, dict) else {}
    result.dashboard = dashboard
    core = dashboard.get("core_conclusion")
    if not isinstance(core, dict):
        core = {}
        dashboard["core_conclusion"] = core
    core["signal_type"] = "🟡持有观望" if language == "zh" else "🟡 Hold / Watch"
    core["one_sentence"] = f"{advice}：{reason}" if language == "zh" else f"{advice}: {reason}"

    position_advice = core.get("position_advice")
    if not isinstance(position_advice, dict):
        position_advice = {}
        core["position_advice"] = position_advice
    position_advice["no_position"] = no_position
    position_advice["has_position"] = has_position

    stability = {
        "applied": False,
        "reason": "结构信号仅形成候选调整；缺少显式 adjusted_score，未应用风控调整。"
        if language == "zh"
        else "Structural evidence proposed an adjustment, but no explicit adjusted_score was supplied.",
        "candidate_reason": reason,
        "reason_code": "explicit_adjusted_score_required",
        "current_price": current_price,
        "support": support,
        "resistance": resistance,
        "capital_flow_bias": flow_bias,
    }
    if capital_flow_status is not None:
        stability["capital_flow_status"] = capital_flow_status
    score_calibration = dashboard.get("decision_score_calibration")
    if isinstance(score_calibration, dict):
        stability["raw_score"] = score_calibration.get("raw_score")
        stability["adjusted_score"] = score_calibration.get("adjusted_score")
        stability["final_action"] = score_calibration.get("final_action")
    dashboard["decision_stability"] = stability

    if reason and reason not in str(result.risk_warning or ""):
        sep = "；" if language == "zh" else "; "
        result.risk_warning = f"{result.risk_warning}{sep}{reason}" if result.risk_warning else reason
    result.buy_reason = reason or result.buy_reason


def _downgrade_buy_without_capital_flow(
    result: "AnalysisResult",
    language: str,
    *,
    current_price: Optional[float],
    support: Optional[float],
    resistance: Optional[float],
    flow_status: str,
) -> None:
    status_text = _capital_flow_status_for_stability(flow_status, language)
    if language == "zh":
        advice = "持有观察"
        reason = f"{status_text}，买入结论缺少资金面确认，先按观察处理。"
        no_position = "空仓先不追买，等待资金流恢复、支撑确认或有效突破后再行动。"
        has_position = "持仓以关键支撑为风控线，资金流恢复前控制仓位。"
        confidence = "低"
    else:
        advice = "Hold and watch"
        reason = f"{status_text}; the buy call lacks capital-flow confirmation, so treat it as watch-only."
        no_position = "Do not chase; wait for capital-flow recovery, support confirmation, or a valid breakout."
        has_position = "Use key support as the risk line and keep position size controlled until capital flow recovers."
        confidence = "Low"

    result.decision_type = "hold"
    result.confidence_level = confidence
    _bound_hold_watch_sentiment_score(result, reason=reason, final_action="hold")
    _apply_hold_watch_dashboard(
        result,
        language,
        advice=advice,
        reason=reason,
        current_price=current_price,
        support=support,
        resistance=resistance,
        flow_bias="unavailable",
        no_position=no_position,
        has_position=has_position,
        capital_flow_status=status_text,
    )
    _sync_stability_dashboard_fields(result)
    logger.info("[decision_stability] Downgraded buy because capital flow is unavailable: %s", flow_status)


def _downgrade_to_structural_hold(
    result: "AnalysisResult",
    language: str,
    *,
    advice_key: str,
    reason_key: str,
    current_price: float,
    support: Optional[float],
    resistance: Optional[float],
    flow_bias: str,
) -> None:
    _set_structural_hold_wording(
        result,
        language,
        advice_key=advice_key,
        reason_key=reason_key,
        current_price=current_price,
        support=support,
        resistance=resistance,
        flow_bias=flow_bias,
        calibrate_score=True,
    )


def _set_structural_hold_wording(
    result: "AnalysisResult",
    language: str,
    *,
    advice_key: str,
    reason_key: str,
    current_price: float,
    support: Optional[float],
    resistance: Optional[float],
    flow_bias: str,
    calibrate_score: bool = False,
) -> None:
    advice_map = {
        "zh": {
            "range": "震荡观望",
            "shakeout": "洗盘观察",
            "hold": "持有观察",
        },
        "en": {
            "range": "Range-bound watch",
            "shakeout": "Shakeout watch",
            "hold": "Hold and watch",
        },
        "ko": {
            "range": "박스권 관망",
            "shakeout": "흔들기 관찰",
            "hold": "보유 관찰",
        },
    }
    advice_default = {"zh": "持有观察", "en": "Hold and watch", "ko": "보유 관찰"}.get(language, "Hold and watch")
    advice = advice_map.get(language, advice_map["en"]).get(advice_key, advice_default)
    reason_templates = {
        "zh": {
            "buy_near_resistance": "价格接近压力位且主力资金未确认流入，不宜仅因短线反弹追买。",
            "buy_with_outflow": "主力资金流出与买入结论冲突，买点需等待支撑确认或资金回流。",
            "sell_near_support": "价格贴近支撑且未见资金持续流出，不宜仅因单日下跌直接卖出。",
            "sell_with_inflow": "主力资金流入与卖出结论冲突，先按持有观察处理并跟踪支撑失效。",
            "hold_shakeout": "价格回落至支撑附近但资金未确认流出，更适合按洗盘观察处理。",
            "hold_mid_range": "价格处于支撑与压力之间且资金流不明确，维持震荡观望更可操作。",
        },
        "en": {
            "buy_near_resistance": "Price is near resistance without confirmed main-force inflow, so chasing the rebound is not actionable.",
            "buy_with_outflow": "Main-force outflow conflicts with a buy call; wait for support confirmation or capital inflow.",
            "sell_near_support": "Price is near support without sustained outflow, so a one-day drop is not enough to sell.",
            "sell_with_inflow": "Main-force inflow conflicts with a sell call; hold and watch for support failure.",
            "hold_shakeout": "Price pulled back near support without confirmed outflow, which is better treated as a shakeout watch.",
            "hold_mid_range": "Price is between support and resistance with neutral fund flow, so range-bound watch is more actionable.",
        },
        "ko": {
            "buy_near_resistance": "가격이 저항선에 근접했고 주력 자금 유입이 확인되지 않아 단기 반등만 보고 추격 매수하기 어렵습니다.",
            "buy_with_outflow": "주력 자금 유출이 매수 결론과 상충하므로 지지 확인이나 자금 재유입을 기다려야 합니다.",
            "sell_near_support": "가격이 지지선에 근접했고 지속적 유출이 없어 하루 하락만으로 매도하기 어렵습니다.",
            "sell_with_inflow": "주력 자금 유입이 매도 결론과 상충하므로 우선 보유 관찰하며 지지 이탈을 추적합니다.",
            "hold_shakeout": "가격이 지지선 부근까지 눌렸지만 유출이 확인되지 않아 흔들기 관찰로 처리하는 것이 적절합니다.",
            "hold_mid_range": "가격이 지지선과 저항선 사이이고 자금 흐름이 불명확해 박스권 관망이 더 실행 가능합니다.",
        },
    }
    reason = reason_templates.get(language, reason_templates["en"]).get(reason_key, "")
    if calibrate_score:
        final_action = "watch" if advice_key in {"range", "shakeout"} else "hold"
        _bound_hold_watch_sentiment_score(result, reason=reason, final_action=final_action)
        dashboard = result.dashboard if isinstance(result.dashboard, dict) else {}
        result.dashboard = dashboard
        dashboard["decision_stability"] = {
            "applied": False,
            "reason_code": "explicit_adjusted_score_required",
            "reason": "结构信号仅形成候选调整；缺少显式 adjusted_score，未应用风控调整。"
            if language == "zh"
            else "Structural evidence proposed an adjustment, but no explicit adjusted_score was supplied.",
            "candidate_reason": reason,
            "current_price": current_price,
            "support": support,
            "resistance": resistance,
            "capital_flow_bias": flow_bias,
        }
        if reason and reason not in str(result.risk_warning or ""):
            sep = "；" if language == "zh" else "; "
            result.risk_warning = f"{result.risk_warning}{sep}{reason}" if result.risk_warning else reason
        _sync_stability_dashboard_fields(result)
        return
    result.operation_advice = advice
    if advice_key == "range":
        if language == "zh" and "震荡" not in str(result.trend_prediction):
            result.trend_prediction = "震荡"
        elif language == "en":
            result.trend_prediction = "Sideways"
        elif language == "ko":
            result.trend_prediction = "횡보"

    if language == "zh":
        no_position = "空仓先不追涨杀跌，等待支撑确认、放量突破或资金回流后再行动。"
        has_position = "持仓以关键支撑为风控线，未跌破前以观察和分批控仓为主。"
    elif language == "ko":
        no_position = "현금 보유 시 추격·투매를 삼가고 지지 확인·대량 돌파·자금 재유입 후 행동하세요."
        has_position = "보유 시 핵심 지지선을 리스크 관리선으로 삼고, 이탈 전까지 관찰과 분할 관리 위주로 대응하세요."
    else:
        no_position = "Do not chase or panic; wait for support confirmation, breakout, or renewed inflow."
        has_position = "Use key support as the risk line and manage position size unless support fails."
    _apply_hold_watch_dashboard(
        result,
        language,
        advice=advice,
        reason=reason,
        current_price=current_price,
        support=support,
        resistance=resistance,
        flow_bias=flow_bias,
        no_position=no_position,
        has_position=has_position,
    )
    logger.info("[decision_stability] Applied structural hold calibration: %s", reason_key)


def get_stock_name_multi_source(
    stock_code: str,
    context: Optional[Dict] = None,
    data_manager = None
) -> str:
    """
    多来源获取股票中文名称

    获取策略（按优先级）：
    1. 从传入的 context 中获取（realtime 数据）
    2. 从静态映射表 STOCK_NAME_MAP 获取
    3. 从 DataFetcherManager 获取（各数据源）
    4. 返回默认名称（股票+代码）

    Args:
        stock_code: 股票代码
        context: 分析上下文（可选）
        data_manager: DataFetcherManager 实例（可选）

    Returns:
        股票中文名称
    """
    # 1. 从上下文获取（实时行情数据）
    if context:
        # 优先从 stock_name 字段获取
        if context.get('stock_name'):
            name = context['stock_name']
            if name and not name.startswith('股票'):
                return name

        # 其次从 realtime 数据获取
        if 'realtime' in context and context['realtime'].get('name'):
            return context['realtime']['name']

    # 2. 从静态映射表获取
    if stock_code in STOCK_NAME_MAP:
        return STOCK_NAME_MAP[stock_code]

    # 3. 从数据源获取
    if data_manager is None:
        try:
            from data_provider.base import DataFetcherManager
            data_manager = DataFetcherManager()
        except Exception as e:
            logger.debug(f"无法初始化 DataFetcherManager: {e}")

    if data_manager:
        try:
            name = data_manager.get_stock_name(stock_code)
            if name:
                # 更新缓存
                STOCK_NAME_MAP[stock_code] = name
                return name
        except Exception as e:
            logger.debug(f"从数据源获取股票名称失败: {e}")

    # 4. 返回默认名称
    return f'股票{stock_code}'


@dataclass
class AnalysisResult:
    """
    AI 分析结果数据类 - 决策仪表盘版

    封装 Gemini 返回的分析结果，包含决策仪表盘和详细分析
    """
    code: str
    name: str

    # ========== 核心指标 ==========
    sentiment_score: int  # 综合评分 0-100 (>70强烈看多, >60看多, 40-60震荡, <40看空)
    trend_prediction: str  # 趋势预测：强烈看多/看多/震荡/看空/强烈看空
    operation_advice: str  # 操作建议：买入/加仓/持有/减仓/卖出/观望
    decision_type: str = "hold"  # 决策类型：buy/hold/sell（用于统计）
    confidence_level: str = "中"  # 置信度：高/中/低
    report_language: str = "zh"  # 报告输出语言：zh/en
    report_schema_version: Optional[str] = None  # Native tool contract version, when present
    action: Optional[str] = None  # 建议动作 taxonomy：buy/add/hold/reduce/sell/watch/avoid/alert
    action_label: Optional[str] = None  # 本地化建议动作标签
    raw_score: Optional[int] = None  # 模型原始评分
    adjusted_score: Optional[int] = None  # 结构化风控后的最终评分
    guardrail: Optional[Dict[str, Any]] = None  # 可审计风控调整
    limitations: Optional[List[str]] = None  # 数据限制；不得隐式改变评分/动作
    position_actions: Optional[Dict[str, Dict[str, Any]]] = None
    holding_state: str = "unknown"  # 仅冻结持仓状态，不持久化账户/数量/成本

    # ========== 决策仪表盘 (新增) ==========
    dashboard: Optional[Dict[str, Any]] = None  # 完整的决策仪表盘数据

    # ========== 走势分析 ==========
    trend_analysis: str = ""  # 走势形态分析（支撑位、压力位、趋势线等）
    short_term_outlook: str = ""  # 短期展望（1-3日）
    medium_term_outlook: str = ""  # 中期展望（1-2周）

    # ========== 技术面分析 ==========
    technical_analysis: str = ""  # 技术指标综合分析
    ma_analysis: str = ""  # 均线分析（多头/空头排列，金叉/死叉等）
    volume_analysis: str = ""  # 量能分析（放量/缩量，主力动向等）
    pattern_analysis: str = ""  # K线形态分析

    # ========== 基本面分析 ==========
    fundamental_analysis: str = ""  # 基本面综合分析
    sector_position: str = ""  # 板块地位和行业趋势
    company_highlights: str = ""  # 公司亮点/风险点

    # ========== 情绪面/消息面分析 ==========
    news_summary: str = ""  # 近期重要新闻/公告摘要
    market_sentiment: str = ""  # 市场情绪分析
    hot_topics: str = ""  # 相关热点话题

    # ========== 综合分析 ==========
    analysis_summary: str = ""  # 综合分析摘要
    key_points: str = ""  # 核心看点（3-5个要点）
    risk_warning: str = ""  # 风险提示
    buy_reason: str = ""  # 买入/卖出理由

    # ========== 元数据 ==========
    market_snapshot: Optional[Dict[str, Any]] = None  # 当日行情快照（展示用）
    raw_response: Optional[str] = None  # 原始响应（调试用）
    search_performed: bool = False  # 是否执行了联网搜索
    data_sources: str = ""  # 数据来源说明
    success: bool = True
    error_message: Optional[str] = None

    # ========== 价格数据（分析时快照）==========
    current_price: Optional[float] = None  # 分析时的股价
    change_pct: Optional[float] = None     # 分析时的涨跌幅(%)

    # ========== 模型标记（Issue #528）==========
    model_used: Optional[str] = None  # 分析使用的 LLM 模型（完整名，如 gemini/gemini-2.0-flash）

    # ========== 历史对比（Report Engine P0）==========
    query_id: Optional[str] = None  # 本次分析 query_id，用于历史对比时排除本次记录

    # ========== 基本面上下文（仅运行时，用于通知拼装；不持久化到 to_dict）==========
    fundamental_context: Optional[Dict[str, Any]] = None
    market_structure_context: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'code': self.code,
            'name': self.name,
            'sentiment_score': self.sentiment_score,
            'trend_prediction': self.trend_prediction,
            'operation_advice': self.operation_advice,
            'decision_type': self.decision_type,
            'confidence_level': self.confidence_level,
            'report_language': self.report_language,
            'report_schema_version': self.report_schema_version,
            'action': self.action,
            'action_label': self.action_label,
            'raw_score': self.raw_score,
            'adjusted_score': self.adjusted_score,
            'guardrail': self.guardrail,
            'limitations': list(self.limitations or []),
            'position_actions': self.position_actions,
            'holding_state': self.holding_state,
            'dashboard': self.dashboard,  # 决策仪表盘数据
            'trend_analysis': self.trend_analysis,
            'short_term_outlook': self.short_term_outlook,
            'medium_term_outlook': self.medium_term_outlook,
            'technical_analysis': self.technical_analysis,
            'ma_analysis': self.ma_analysis,
            'volume_analysis': self.volume_analysis,
            'pattern_analysis': self.pattern_analysis,
            'fundamental_analysis': self.fundamental_analysis,
            'sector_position': self.sector_position,
            'company_highlights': self.company_highlights,
            'news_summary': self.news_summary,
            'market_sentiment': self.market_sentiment,
            'hot_topics': self.hot_topics,
            'analysis_summary': self.analysis_summary,
            'key_points': self.key_points,
            'risk_warning': self.risk_warning,
            'buy_reason': self.buy_reason,
            'market_snapshot': self.market_snapshot,
            'search_performed': self.search_performed,
            'success': self.success,
            'error_message': self.error_message,
            'current_price': self.current_price,
            'change_pct': self.change_pct,
            'model_used': self.model_used,
            'market_structure_context': self.market_structure_context,
        }

    def get_core_conclusion(self) -> str:
        """获取核心结论（一句话）"""
        if self.dashboard and 'core_conclusion' in self.dashboard:
            return self.dashboard['core_conclusion'].get('one_sentence', self.analysis_summary)
        return self.analysis_summary

    def get_position_advice(self, has_position: bool = False) -> str:
        """获取持仓建议"""
        if self.dashboard and 'core_conclusion' in self.dashboard:
            pos_advice = self.dashboard['core_conclusion'].get('position_advice', {})
            if has_position:
                return pos_advice.get('has_position', self.operation_advice)
            return pos_advice.get('no_position', self.operation_advice)
        return self.operation_advice

    def get_sniper_points(self) -> Dict[str, str]:
        """获取狙击点位"""
        if self.dashboard and 'battle_plan' in self.dashboard:
            return self.dashboard['battle_plan'].get('sniper_points', {})
        return {}

    def get_checklist(self) -> List[str]:
        """获取检查清单"""
        if self.dashboard and 'battle_plan' in self.dashboard:
            return self.dashboard['battle_plan'].get('action_checklist', [])
        return []

    def get_risk_alerts(self) -> List[str]:
        """获取风险警报"""
        if self.dashboard and 'intelligence' in self.dashboard:
            return self.dashboard['intelligence'].get('risk_alerts', [])
        return []

    def get_emoji(self) -> str:
        """根据操作建议返回对应 emoji"""
        _, emoji, _ = get_signal_level(
            self.operation_advice,
            self.sentiment_score,
            self.report_language,
        )
        return emoji

    def get_confidence_stars(self) -> str:
        """返回置信度星级"""
        star_map = {
            "高": "⭐⭐⭐",
            "high": "⭐⭐⭐",
            "中": "⭐⭐",
            "medium": "⭐⭐",
            "低": "⭐",
            "low": "⭐",
        }
        return star_map.get(str(self.confidence_level or "").strip().lower(), "⭐⭐")


def populate_decision_action_fields(
    result: AnalysisResult,
    *,
    explicit_action: Any = None,
    report_type: Any = None,
    use_existing_action: bool = True,
    align_with_score: bool = True,
) -> AnalysisResult:
    """Apply the unified score/action contract to every persisted result."""

    action_source = explicit_action
    if action_source is None and use_existing_action:
        action_source = getattr(result, "action", None)

    return normalize_result_decision_contract(
        result,
        explicit_action=action_source,
        align_with_score=align_with_score,
    )


class GeminiAnalyzer:
    """
    Gemini AI 分析器

    职责：
    1. 调用 Google Gemini API 进行股票分析
    2. 结合预先搜索的新闻和技术面数据生成分析报告
    3. 解析 AI 返回的 JSON 格式结果

    使用方式：
        analyzer = GeminiAnalyzer()
        result = analyzer.analyze(context, news_context)
    """

    # ========================================
    # 系统提示词 - 决策仪表盘 v2.0
    # ========================================
    # 输出格式升级：从简单信号升级为决策仪表盘
    # 核心模块：核心结论 + 数据透视 + 舆情情报 + 作战计划
    # ========================================

    LEGACY_DEFAULT_SYSTEM_PROMPT = """你是一位专注于趋势交易的{market_placeholder}投资分析师，负责生成专业的【决策仪表盘】分析报告。

{guidelines_placeholder}

    """ + LEGACY_DEFAULT_TRADING_POLICY_ZH + """

""" + CANONICAL_DECISION_SCALE_PROMPT_ZH + """

## 输出格式：决策仪表盘 JSON

请严格按照以下 JSON 格式输出，这是一个完整的【决策仪表盘】：

```json
{
    "stock_name": "股票中文名称",
    "raw_score": 0-100整数,
    "adjusted_score": 0-100整数（无风控调整时等于 raw_score）,
    "sentiment_score": 0-100整数（必须等于 adjusted_score）,
    "trend_prediction": "强烈看多/看多/震荡/看空/强烈看空",
    "operation_advice": "买入/加仓/持有/减仓/卖出/观望",
    "decision_type": "buy/hold/sell",
    "action": "buy/add/hold/reduce/sell/watch/avoid/alert",
    "guardrail": {
        "applied": true/false,
        "reason_code": "仅 applied=true 时填写稳定原因码",
        "reason": "仅 applied=true 时填写明确证据",
        "raw_score": 0-100整数,
        "adjusted_score": 0-100整数,
        "score_delta": 整数
    },
    "limitations": ["可选数据缺失项；只降低置信度，不改变评分或动作"],
    "position_actions": {
        "empty": {"action": "buy/watch/avoid/alert", "advice": "空仓者建议；空仓可以买入，但不得使用持有/减仓/卖出"},
        "holding": {
            "action": "add/hold/reduce/sell/watch/avoid/alert",
            "advice": "持仓者建议",
            "stop_loss": 持仓情况下建议继续生效的止损价（正数；无法可靠给出时省略）,
            "target_price": 持仓情况下建议继续生效的止盈/目标价（正数；无法可靠给出时省略）
        }
    },
    "confidence_level": "高/中/低",

    "dashboard": {
        "core_conclusion": {
            "one_sentence": "一句话核心结论（30字以内，直接告诉用户做什么）",
            "signal_type": "🟢买入信号/🟡持有观望/🔴卖出信号/⚠️风险警告",
            "time_sensitivity": "立即行动/今日内/本周内/不急",
            "position_advice": {
                "no_position": "空仓者建议：具体操作指引",
                "has_position": "持仓者建议：具体操作指引"
            }
        },

        "data_perspective": {
            "trend_status": {
                "ma_alignment": "均线排列状态描述",
                "is_bullish": true/false,
                "trend_score": 0-100
            },
            "price_position": {
                "current_price": 当前价格数值,
                "ma5": MA5数值,
                "ma10": MA10数值,
                "ma20": MA20数值,
                "bias_ma5": 乖离率百分比数值,
                "bias_status": "安全/警戒/危险",
                "support_level": 支撑位价格,
                "resistance_level": 压力位价格
            },
            "volume_analysis": {
                "volume_ratio": 量比数值,
                "volume_status": "放量/缩量/平量",
                "turnover_rate": 换手率百分比,
                "volume_meaning": "量能含义解读（如：缩量回调表示抛压减轻）"
            },
            "chip_structure": {
                "profit_ratio": 获利比例,
                "avg_cost": 平均成本,
                "concentration": 筹码集中度,
                "chip_health": "健康/一般/警惕"
            }
        },

        "intelligence": {
            "latest_news": "【最新消息】近期重要新闻摘要",
            "risk_alerts": ["风险点1：具体描述", "风险点2：具体描述"],
            "positive_catalysts": ["利好1：具体描述", "利好2：具体描述"],
            "earnings_outlook": "业绩预期分析（基于年报预告、业绩快报等）",
            "sentiment_summary": "舆情情绪一句话总结"
        },

        "battle_plan": {
            "sniper_points": {
                "ideal_buy": "理想买入点：XX元（符合所选交易风格的支撑/突破/回踩条件）",
                "secondary_buy": "次优买入点：XX元（更保守或二次确认后执行）",
                "stop_loss": "止损位：XX元（风格失效条件、关键支撑或X%风险）",
                "take_profit": "最终目标位：XX元（必须等于 final_target 或最后一条 exit_legs.target_price）",
                "first_take_profit": "第一止盈位：XX元（可选；分批止盈时填写第一目标）",
                "final_target": "最终目标位：XX元（可选；分批止盈时必须填写）",
                "entry_leg_schema_version": "entry-legs-v1（可选；仅当你明确给出分批执行时填写）",
                "entry_legs": [
                    {"leg_id": "initial", "role": "initial", "sequence": 1, "allocation_fraction": 1.0, "max_executions": 1, "price_rules": [{"type": "close_between", "low": 0, "high": 0}], "prerequisites": []}
                ],
                "exit_leg_schema_version": "exit-legs-v1（可选；仅当你明确给出分段止盈时填写）",
                "exit_legs": [
                    {"leg_id": "tp1", "role": "partial_take_profit", "sequence": 1, "allocation_fraction": 0.5, "target_price": 0},
                    {"leg_id": "tp2", "role": "final_take_profit", "sequence": 2, "allocation_fraction": 0.5, "target_price": 0}
                ]
            },
            "position_strategy": {
                "suggested_position": "建议仓位：X成",
                "entry_plan": "分批建仓策略描述",
                "risk_control": "风控策略描述"
            },
            "action_checklist": [
                "✅/⚠️/❌ 检查项1：多头排列",
                "✅/⚠️/❌ 检查项2：乖离率合理（强势趋势可放宽）",
                "✅/⚠️/❌ 检查项3：量能配合",
                "✅/⚠️/❌ 检查项4：无重大利空",
                "✅/⚠️/❌ 检查项5：筹码健康",
                "✅/⚠️/❌ 检查项6：PE估值合理"
            ]
        },

        "phase_decision": {
            "phase_context": {"phase": "premarket/intraday/lunch_break/closing_auction/postmarket/non_trading/unknown"},
            "entry_mode": "none/conditional/immediate",
            "action_window": "盘前计划/盘中跟踪/午间确认/收盘前风控/盘后复盘/非交易日观察",
            "immediate_action": "立即行动/等待确认/观察/止损止盈预警/禁止追高/无盘中动作",
            "watch_conditions": ["观察条件1", "观察条件2"],
            "next_check_time": "下一次检查点或市场本地时间",
            "confidence_reason": "置信度理由，说明阶段和数据质量限制",
            "data_limitations": ["阶段或数据质量限制1", "阶段或数据质量限制2"]
        },

        "signal_attribution": {
            "technical_indicators": 技术指标贡献度(0-100),
            "news_sentiment": 新闻舆情贡献度(0-100),
            "fundamentals": 基本面贡献度(0-100),
            "market_conditions": 市场环境贡献度(0-100),
            "strongest_bullish_signal": "最强看多信号名称",
            "strongest_bearish_signal": "最强看空信号名称"
        }
    },

    "analysis_summary": "100字综合分析摘要",
    "key_points": "3-5个核心看点，逗号分隔",
    "risk_warning": "风险提示",
    "buy_reason": "操作理由，引用交易理念",

    "trend_analysis": "走势形态分析",
    "short_term_outlook": "短期1-3日展望",
    "medium_term_outlook": "中期1-2周展望",
    "technical_analysis": "技术面综合分析",
    "ma_analysis": "均线系统分析",
    "volume_analysis": "量能分析",
    "pattern_analysis": "K线形态分析",
    "fundamental_analysis": "基本面分析",
    "sector_position": "板块行业分析",
    "company_highlights": "公司亮点/风险",
    "news_summary": "新闻摘要",
    "market_sentiment": "市场情绪",
    "hot_topics": "相关热点",

    "search_performed": true/false,
    "data_sources": "数据来源说明"
}
```

## 评分标准

### 强烈买入（80-100分）：
- ✅ 趋势结构符合所选交易风格：短线可重点看 MA5/MA10/MA20，中线/持仓需结合 MA10/MA20/MA60，趋势风格需结合 MA20/MA60/MA120 或更长周期
- ✅ 价格位置与风格匹配：短线低乖离，中线回踩核心支撑，趋势风格处于主升/回撤可控区
- ✅ 缩量回调、放量突破或趋势延续信号与当前 setup 匹配
- ✅ 筹码集中健康
- ✅ 消息面有利好催化

### 买入（60-79分）：
- ✅ 主趋势或阶段结构偏积极，但允许短期均线缠绕
- ✅ 入场位置、止损距离和风险收益比仍可控
- ✅ 量能/资金/筹码至少部分确认，且没有重大否决项
- ⚪ 允许一项次要条件不满足

### 观望（40-59分）：
- ⚠️ 乖离率 >5%（追高风险）
- ⚠️ 均线缠绕趋势不明
- ⚠️ 有风险事件

### 减仓（20-39分）：
- ⚠️ 趋势走弱或跌破关键均线
- ⚠️ 资金/量能转弱，风险明显高于收益
- ⚠️ 以降低仓位和保护收益为主

### 卖出（0-19分）：
- ❌ 空头排列或趋势显著恶化
- ❌ 跌破关键支撑/止损位
- ❌ 放量下跌或重大利空

## 决策仪表盘核心原则

1. **核心结论先行**：一句话说清该买该卖
2. **分持仓建议**：空仓者和持仓者给不同建议
3. **精确狙击点**：必须给出具体价格，不说模糊的话
4. **检查清单可视化**：用 ✅⚠️❌ 明确显示每项检查结果
5. **风险优先级**：舆情中的风险点要醒目标出

## 可操作性与稳定性约束

- 不得仅因为单日涨跌或评分跨线就在“买入/卖出”之间剧烈切换。
- 持仓动作必须先分支判断：空仓允许 `buy/watch/avoid/alert`，不得输出 `hold/reduce/sell`，也不得写“空仓不能买入”；已有持仓允许 `add/hold/reduce/sell/watch/avoid/alert`，止损/止盈建议只对已有仓位或新入场计划生效。
- `dashboard.phase_decision.entry_mode` 可选，取值只能为 `none`、`conditional`、`immediate`。`conditional` 表示已有明确入场授权但等待触发条件，顶层 `action` 仍必须是 `buy/add` 且买入契约完整；`none` 表示无入场授权。不得仅因理想买入价、评分或 watch 文案把 `watch` 升级为买入。
- `confidence_level` 必须只填写单独标签：`高`、`中` 或 `低`；置信理由写入 `limitations`、`risk_warning` 或 `dashboard.phase_decision.confidence_reason`，不得写“中等偏低”或带括号长说明。
- 趋势判断与入场择时必须分开：MA5 乖离和短均线更多用于买点时机，不能把 “MA5>MA10>MA20” 当作所有交易风格的买入硬门槛；中线和趋势风格应把核心 thesis 放在更长均线、波动、支撑压力、成交/资金和风险收益上。
- 未搜索到新闻、新闻接口不可用或时间不合规，只能说明“当前窗口没有可引用的新闻证据”，不得推断为“没有事件/没有利空”；基本面数据陈旧只能作为历史背景，不得当作当前最新基本面。
- 资金流、筹码或基本面字段缺失/不适用时，只能列为不确定性或适用性限制，不得当作真实利空，也不得仅因缺失自动扣分或改变动作；已有负面财报、趋势破位、风险事件仍应照常影响结论。
- 操作建议必须同时参考价格位置（支撑/压力位）、量能/筹码、主力资金流向和风险事件；其中不适用于该标的的数据源应跳过，不作为否定条件。
- 股价位于支撑与压力之间、资金流不明确时，优先输出“持有/震荡/观望/洗盘观察”等可执行的中性建议；`decision_type` 仍保持 `hold`。
- 只有在接近支撑确认或有效突破压力，且资金流/量价配合时，才能给出买入；未触价但条件策略有效时可用 `action=buy/add` + `entry_mode=conditional` 表达候选条件策略授权，预设入场区间仍须符合净盈亏比、ATR 和交易风格证据；两交易日报告确认、市场/账户风险和触价门禁由后端独立执行，不得声称这些后端门禁已经通过；接近压力且资金流出时不得追买。
- 只有在跌破关键支撑、主力资金持续流出或风险显著放大时，才能给出卖出/减仓。
- 必须输出 `dashboard.phase_decision` 七字段；盘中/午休/临近收盘要给出当前动作、观察条件和下一次检查点。
- 建议输出可选展示字段 `dashboard.signal_attribution` 六字段；解释推荐理由的构成，包括技术指标、新闻舆情、基本面、市场环境的贡献度，以及最强看多/看空信号。
- 盘前、非交易日或未知阶段不得伪造今日盘中走势；quote/daily_bars/technical 存在 stale、fallback、missing、fetch_failed、partial 或 estimated 时，`confidence_level` 不得为高。"""

    SYSTEM_PROMPT = """你是一位{market_placeholder}投资分析师，负责生成专业的【决策仪表盘】分析报告。

{guidelines_placeholder}

{default_skill_policy_section}
{skills_section}

""" + CANONICAL_DECISION_SCALE_PROMPT_ZH + """

## 输出格式：决策仪表盘 JSON

请严格按照以下 JSON 格式输出，这是一个完整的【决策仪表盘】：

```json
{
    "stock_name": "股票中文名称",
    "raw_score": 0-100整数,
    "adjusted_score": 0-100整数（无风控调整时等于 raw_score）,
    "sentiment_score": 0-100整数（必须等于 adjusted_score）,
    "trend_prediction": "强烈看多/看多/震荡/看空/强烈看空",
    "operation_advice": "买入/加仓/持有/减仓/卖出/观望",
    "decision_type": "buy/hold/sell",
    "action": "buy/add/hold/reduce/sell/watch/avoid/alert",
    "guardrail": {
        "applied": true/false,
        "reason_code": "仅 applied=true 时填写稳定原因码",
        "reason": "仅 applied=true 时填写明确证据",
        "raw_score": 0-100整数,
        "adjusted_score": 0-100整数,
        "score_delta": 整数
    },
    "limitations": ["可选数据缺失项；只降低置信度，不改变评分或动作"],
    "position_actions": {
        "empty": {"action": "buy/watch/avoid/alert", "advice": "空仓者建议；空仓可以买入，但不得使用持有/减仓/卖出"},
        "holding": {
            "action": "add/hold/reduce/sell/watch/avoid/alert",
            "advice": "持仓者建议",
            "stop_loss": 持仓情况下建议继续生效的止损价（正数；无法可靠给出时省略）,
            "target_price": 持仓情况下建议继续生效的止盈/目标价（正数；无法可靠给出时省略）
        }
    },
    "confidence_level": "高/中/低",

    "dashboard": {
        "core_conclusion": {
            "one_sentence": "一句话核心结论（30字以内，直接告诉用户做什么）",
            "signal_type": "🟢买入信号/🟡持有观望/🔴卖出信号/⚠️风险警告",
            "time_sensitivity": "立即行动/今日内/本周内/不急",
            "position_advice": {
                "no_position": "空仓者建议：具体操作指引",
                "has_position": "持仓者建议：具体操作指引"
            }
        },

        "data_perspective": {
            "trend_status": {
                "ma_alignment": "均线排列状态描述",
                "is_bullish": true/false,
                "trend_score": 0-100
            },
            "price_position": {
                "current_price": 当前价格数值,
                "ma5": MA5数值,
                "ma10": MA10数值,
                "ma20": MA20数值,
                "bias_ma5": 乖离率百分比数值,
                "bias_status": "安全/警戒/危险",
                "support_level": 支撑位价格,
                "resistance_level": 压力位价格
            },
            "volume_analysis": {
                "volume_ratio": 量比数值,
                "volume_status": "放量/缩量/平量",
                "turnover_rate": 换手率百分比,
                "volume_meaning": "量能含义解读（如：缩量回调表示抛压减轻）"
            },
            "chip_structure": {
                "profit_ratio": 获利比例,
                "avg_cost": 平均成本,
                "concentration": 筹码集中度,
                "chip_health": "健康/一般/警惕"
            }
        },

        "intelligence": {
            "latest_news": "【最新消息】近期重要新闻摘要",
            "risk_alerts": ["风险点1：具体描述", "风险点2：具体描述"],
            "positive_catalysts": ["利好1：具体描述", "利好2：具体描述"],
            "earnings_outlook": "业绩预期分析（基于年报预告、业绩快报等）",
            "sentiment_summary": "舆情情绪一句话总结"
        },

        "battle_plan": {
            "sniper_points": {
                "ideal_buy": "理想入场位：XX元（满足主要技能触发条件）",
                "secondary_buy": "次优入场位：XX元（更保守或确认后执行）",
                "stop_loss": "止损位：XX元（失效条件或X%风险）",
                "take_profit": "最终目标位：XX元（必须等于 final_target 或最后一条 exit_legs.target_price）",
                "first_take_profit": "第一止盈位：XX元（可选；分批止盈时填写第一目标）",
                "final_target": "最终目标位：XX元（可选；分批止盈时必须填写）",
                "entry_leg_schema_version": "entry-legs-v1（可选；仅明确分批时填写）",
                "entry_legs": [
                    {"leg_id": "initial", "role": "initial", "sequence": 1, "allocation_fraction": 1.0, "max_executions": 1, "price_rules": [{"type": "close_between", "low": 0, "high": 0}], "prerequisites": []}
                ],
                "exit_leg_schema_version": "exit-legs-v1（可选；仅明确分段止盈时填写）",
                "exit_legs": [
                    {"leg_id": "tp1", "role": "partial_take_profit", "sequence": 1, "allocation_fraction": 0.5, "target_price": 0},
                    {"leg_id": "tp2", "role": "final_take_profit", "sequence": 2, "allocation_fraction": 0.5, "target_price": 0}
                ]
            },
            "position_strategy": {
                "suggested_position": "建议仓位：X成",
                "entry_plan": "分批建仓策略描述",
                "risk_control": "风控策略描述"
            },
            "action_checklist": [
                "✅/⚠️/❌ 检查项1：当前结构是否满足激活技能条件",
                "✅/⚠️/❌ 检查项2：入场位置与风险回报是否合理",
                "✅/⚠️/❌ 检查项3：量价/波动/筹码是否支持判断",
                "✅/⚠️/❌ 检查项4：无重大利空",
                "✅/⚠️/❌ 检查项5：仓位与止损计划明确",
                "✅/⚠️/❌ 检查项6：估值/业绩/催化与结论匹配"
            ]
        },

        "phase_decision": {
            "phase_context": {"phase": "premarket/intraday/lunch_break/closing_auction/postmarket/non_trading/unknown"},
            "entry_mode": "none/conditional/immediate",
            "action_window": "盘前计划/盘中跟踪/午间确认/收盘前风控/盘后复盘/非交易日观察",
            "immediate_action": "立即行动/等待确认/观察/止损止盈预警/禁止追高/无盘中动作",
            "watch_conditions": ["观察条件1", "观察条件2"],
            "next_check_time": "下一次检查点或市场本地时间",
            "confidence_reason": "置信度理由，说明阶段和数据质量限制",
            "data_limitations": ["阶段或数据质量限制1", "阶段或数据质量限制2"]
        },

        "signal_attribution": {
            "technical_indicators": 技术指标贡献度(0-100),
            "news_sentiment": 新闻舆情贡献度(0-100),
            "fundamentals": 基本面贡献度(0-100),
            "market_conditions": 市场环境贡献度(0-100),
            "strongest_bullish_signal": "最强看多信号名称",
            "strongest_bearish_signal": "最强看空信号名称"
        }
    },

    "analysis_summary": "100字综合分析摘要",
    "key_points": "3-5个核心看点，逗号分隔",
    "risk_warning": "风险提示",
    "buy_reason": "操作理由，引用激活技能或风险框架",

    "trend_analysis": "走势形态分析",
    "short_term_outlook": "短期1-3日展望",
    "medium_term_outlook": "中期1-2周展望",
    "technical_analysis": "技术面综合分析",
    "ma_analysis": "均线系统分析",
    "volume_analysis": "量能分析",
    "pattern_analysis": "K线形态分析",
    "fundamental_analysis": "基本面分析",
    "sector_position": "板块行业分析",
    "company_highlights": "公司亮点/风险",
    "news_summary": "新闻摘要",
    "market_sentiment": "市场情绪",
    "hot_topics": "相关热点",

    "search_performed": true/false,
    "data_sources": "数据来源说明"
}
```

## 评分标准

### 强烈买入（80-100分）：
- ✅ 多个激活技能同时支持积极结论
- ✅ 上行空间、触发条件与风险回报清晰
- ✅ 关键风险已排查，仓位与止损计划明确
- ✅ 重要数据和情报结论彼此一致

### 买入（60-79分）：
- ✅ 主信号偏积极，但仍有少量待确认项
- ✅ 允许存在可控风险或次优入场点
- ✅ 需要在报告中明确补充观察条件

### 观望（40-59分）：
- ⚠️ 信号分歧较大，或缺乏足够确认
- ⚠️ 风险与机会大致均衡
- ⚠️ 更适合等待触发条件或回避不确定性

### 减仓（20-39分）：
- ⚠️ 主要结论转弱，风险明显高于收益
- ⚠️ 触发了部分失效条件，现有仓位需要降低暴露
- ⚠️ 更适合保护收益而不是进攻

### 卖出（0-19分）：
- ❌ 触发了止损/失效条件或重大利空
- ❌ 趋势或风险显著恶化
- ❌ 现有仓位应优先退出

## 决策仪表盘核心原则

1. **核心结论先行**：一句话说清该买该卖
2. **分持仓建议**：空仓者和持仓者给不同建议
3. **精确狙击点**：必须给出具体价格，不说模糊的话
4. **检查清单可视化**：用 ✅⚠️❌ 明确显示每项检查结果
5. **风险优先级**：舆情中的风险点要醒目标出

## 可操作性与稳定性约束

- 不得仅因为单日涨跌或评分跨线就在“买入/卖出”之间剧烈切换。
- 持仓动作必须先分支判断：空仓允许 `buy/watch/avoid/alert`，不得输出 `hold/reduce/sell`，也不得写“空仓不能买入”；已有持仓允许 `add/hold/reduce/sell/watch/avoid/alert`，止损/止盈建议只对已有仓位或新入场计划生效。
- `dashboard.phase_decision.entry_mode` 可选，取值只能为 `none`、`conditional`、`immediate`。`conditional` 表示已有明确入场授权但等待触发条件，顶层 `action` 仍必须是 `buy/add` 且买入契约完整；`none` 表示无入场授权。不得仅因理想买入价、评分或 watch 文案把 `watch` 升级为买入。
- `confidence_level` 必须只填写单独标签：`高`、`中` 或 `低`；置信理由写入 `limitations`、`risk_warning` 或 `dashboard.phase_decision.confidence_reason`，不得写“中等偏低”或带括号长说明。
- 趋势判断与入场择时必须分开：MA5 乖离和短均线更多用于买点时机，不能把 “MA5>MA10>MA20” 当作所有交易风格的买入硬门槛；中线和趋势风格应把核心 thesis 放在更长均线、波动、支撑压力、成交/资金和风险收益上。
- 未搜索到新闻、新闻接口不可用或时间不合规，只能说明“当前窗口没有可引用的新闻证据”，不得推断为“没有事件/没有利空”；基本面数据陈旧只能作为历史背景，不得当作当前最新基本面。
- 资金流、筹码或基本面字段缺失/不适用时，只能列为不确定性或适用性限制，不得当作真实利空，也不得仅因缺失自动扣分或改变动作；已有负面财报、趋势破位、风险事件仍应照常影响结论。
- 操作建议必须同时参考价格位置（支撑/压力位）、量能/筹码、主力资金流向和风险事件；其中不适用于该标的的数据源应跳过，不作为否定条件。
- 股价位于支撑与压力之间、资金流不明确时，优先输出“持有/震荡/观望/洗盘观察”等可执行的中性建议；`decision_type` 仍保持 `hold`。
- 只有在接近支撑确认或有效突破压力，且资金流/量价配合时，才能给出买入；未触价但条件策略有效时可用 `action=buy/add` + `entry_mode=conditional` 表达候选条件策略授权，预设入场区间仍须符合净盈亏比、ATR 和交易风格证据；两交易日报告确认、市场/账户风险和触价门禁由后端独立执行，不得声称这些后端门禁已经通过；接近压力且资金流出时不得追买。
- 只有在跌破关键支撑、主力资金持续流出或风险显著放大时，才能给出卖出/减仓。
- 必须输出 `dashboard.phase_decision` 七字段；盘中/午休/临近收盘要给出当前动作、观察条件和下一次检查点。
- 建议输出可选展示字段 `dashboard.signal_attribution` 六字段；解释推荐理由的构成，包括技术指标、新闻舆情、基本面、市场环境的贡献度，以及最强看多/看空信号。
- 盘前、非交易日或未知阶段不得伪造今日盘中走势；quote/daily_bars/technical 存在 stale、fallback、missing、fetch_failed、partial 或 estimated 时，`confidence_level` 不得为高。"""

    TEXT_SYSTEM_PROMPT = """你是一位专业的股票分析助手。

- 回答必须基于用户提供的数据与上下文
- 若信息不足，要明确指出不确定性
- 不要编造价格、财报或新闻事实
"""

    ANALYSIS_REPORT_TOOL_NAME = "submit_analysis_report"
    ANALYSIS_REPORT_TOOL_SCHEMA_VERSION = ANALYSIS_REPORT_TOOL_SCHEMA_VERSION

    @classmethod
    def _analysis_report_tool(cls) -> Dict[str, Any]:
        """Return the native tool contract used for standard stock reports."""
        text = {"type": "string", "minLength": 1}
        number_or_text = {
            "anyOf": [
                {"type": "number", "exclusiveMinimum": 0},
                {"type": "string", "minLength": 1},
            ]
        }
        metric_number_or_text = {
            "anyOf": [
                {"type": "number"},
                {"type": "string", "minLength": 1},
            ]
        }
        action_enum = {
            "type": "string",
            "enum": ["buy", "add", "hold", "reduce", "sell", "watch", "avoid", "alert"],
        }
        confidence_enum = {
            "type": "string",
            "enum": ["高", "中", "低", "high", "medium", "mid", "low"],
        }
        return {
            "type": "function",
            "function": {
                "name": cls.ANALYSIS_REPORT_TOOL_NAME,
                "description": (
                    "Submit one complete stock-analysis decision dashboard. "
                    "Use this tool exactly once; do not answer with plain text."
                ),
                "parameters": {
                    "type": "object",
                    "additionalProperties": True,
                    "required": [
                        "report_schema_version",
                        "raw_score",
                        "adjusted_score",
                        "sentiment_score",
                        "action",
                        "position_actions",
                        "confidence_level",
                    ],
                    "properties": {
                        "report_schema_version": {
                            "type": "string",
                            "const": cls.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION,
                        },
                        "stock_name": text,
                        "raw_score": {"type": "integer", "minimum": 0, "maximum": 100},
                        "adjusted_score": {"type": "integer", "minimum": 0, "maximum": 100},
                        "sentiment_score": {"type": "integer", "minimum": 0, "maximum": 100},
                        "trend_prediction": text,
                        "operation_advice": text,
                        "decision_type": {"type": "string", "enum": ["buy", "hold", "sell"]},
                        "action": action_enum,
                        "confidence_level": confidence_enum,
                        "limitations": {"type": "array", "items": text},
                        "position_actions": {
                            "type": "object",
                            "additionalProperties": True,
                            "required": ["empty", "holding"],
                            "properties": {
                                "empty": {
                                    "type": "object",
                                    "additionalProperties": True,
                                    "required": ["action"],
                                    "properties": {"action": action_enum, "advice": text},
                                },
                                "holding": {
                                    "type": "object",
                                    "additionalProperties": True,
                                    "required": ["action"],
                                    "properties": {
                                        "action": action_enum,
                                        "advice": text,
                                        "stop_loss": {"type": "number", "exclusiveMinimum": 0},
                                        "target_price": {"type": "number", "exclusiveMinimum": 0},
                                    },
                                },
                            },
                        },
                        "dashboard": {
                            "type": "object",
                            "additionalProperties": True,
                            "properties": {
                                "core_conclusion": {
                                    "type": "object",
                                    "additionalProperties": True,
                                    "properties": {
                                        "one_sentence": text,
                                        "signal_type": text,
                                        "time_sensitivity": text,
                                        "position_advice": {
                                            "type": "object",
                                            "additionalProperties": True,
                                            "properties": {
                                                "no_position": text,
                                                "has_position": text,
                                            },
                                        },
                                    },
                                },
                                "data_perspective": {
                                    "type": "object",
                                    "additionalProperties": True,
                                    "properties": {
                                        "trend_status": {
                                            "type": "object",
                                            "additionalProperties": True,
                                            "properties": {
                                                "ma_alignment": text,
                                                "is_bullish": {"type": "boolean"},
                                                "trend_score": metric_number_or_text,
                                            },
                                        },
                                        "price_position": {
                                            "type": "object",
                                            "additionalProperties": True,
                                            "properties": {
                                                "current_price": number_or_text,
                                                "ma5": number_or_text,
                                                "ma10": number_or_text,
                                                "ma20": number_or_text,
                                                "bias_ma5": metric_number_or_text,
                                                "bias_status": text,
                                                "support_level": number_or_text,
                                                "resistance_level": number_or_text,
                                            },
                                        },
                                        "volume_analysis": {
                                            "type": "object",
                                            "additionalProperties": True,
                                            "properties": {
                                                "volume_ratio": metric_number_or_text,
                                                "volume_status": text,
                                                "turnover_rate": metric_number_or_text,
                                                "volume_meaning": text,
                                            },
                                        },
                                        "chip_structure": {
                                            "type": "object",
                                            "additionalProperties": True,
                                            "properties": {
                                                "profit_ratio": metric_number_or_text,
                                                "avg_cost": number_or_text,
                                                "concentration": metric_number_or_text,
                                                "chip_health": text,
                                            },
                                        },
                                    },
                                },
                                "intelligence": {
                                    "type": "object",
                                    "additionalProperties": True,
                                    "properties": {
                                        "latest_news": text,
                                        "risk_alerts": {"type": "array", "items": text},
                                        "positive_catalysts": {"type": "array", "items": text},
                                        "earnings_outlook": text,
                                        "sentiment_summary": text,
                                    },
                                },
                                "battle_plan": {
                                    "type": "object",
                                    "additionalProperties": True,
                                    "properties": {
                                        "setup": {
                                            "type": "object",
                                            "properties": {
                                                "setup_type": {"type": "string", "enum": ["breakout", "pullback", "trend_continuation"]},
                                            },
                                            "required": ["setup_type"],
                                            "additionalProperties": False,
                                        },
                                        "sniper_points": {
                                            "type": "object",
                                            "additionalProperties": True,
                                            # Entry/exit prices are conditionally mandatory only
                                            # for buy/add.  Requiring them here for watch/sell
                                            # would pressure the model to invent prices.
                                            "properties": {
                                                "ideal_buy": number_or_text,
                                                "secondary_buy": number_or_text,
                                                "stop_loss": number_or_text,
                                                "take_profit": number_or_text,
                                                "first_take_profit": number_or_text,
                                                "final_target": number_or_text,
                                                "entry_leg_schema_version": {"type": "string", "const": "entry-legs-v1"},
                                                "entry_legs": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "additionalProperties": True,
                                                        "required": ["leg_id", "role", "sequence", "allocation_fraction", "max_executions", "price_rules", "prerequisites"],
                                                        "properties": {
                                                            "leg_id": text,
                                                            "role": text,
                                                            "sequence": {"type": "integer", "minimum": 1},
                                                            "allocation_fraction": {"type": "number", "exclusiveMinimum": 0, "maximum": 1},
                                                            "max_executions": {"type": "integer", "minimum": 1},
                                                            "price_rules": {
                                                                "type": "array",
                                                                "minItems": 1,
                                                                "items": {
                                                                    "type": "object",
                                                                    "additionalProperties": True,
                                                                    "required": ["type", "low", "high"],
                                                                    "properties": {
                                                                        "type": {"type": "string", "const": "close_between"},
                                                                        "low": {"type": "number", "exclusiveMinimum": 0},
                                                                        "high": {"type": "number", "exclusiveMinimum": 0},
                                                                    },
                                                                },
                                                            },
                                                            "prerequisites": {"type": "array", "items": text},
                                                        },
                                                    },
                                                },
                                                "exit_leg_schema_version": {"type": "string", "const": "exit-legs-v1"},
                                                "exit_legs": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "additionalProperties": True,
                                                        "required": ["leg_id", "role", "sequence", "allocation_fraction", "target_price"],
                                                        "properties": {
                                                            "leg_id": text,
                                                            "role": text,
                                                            "sequence": {"type": "integer", "minimum": 1},
                                                            "allocation_fraction": {"type": "number", "exclusiveMinimum": 0, "maximum": 1},
                                                            "target_price": {"type": "number", "exclusiveMinimum": 0},
                                                        },
                                                    },
                                                },
                                            },
                                        },
                                        "position_strategy": {
                                            "type": "object",
                                            "additionalProperties": True,
                                            "properties": {
                                                "suggested_position": text,
                                                "entry_plan": text,
                                                "risk_control": text,
                                            },
                                        },
                                        "action_checklist": {"type": "array", "items": text},
                                    },
                                },
                                "phase_decision": {
                                    "type": "object",
                                    "additionalProperties": True,
                                    "properties": {
                                        "phase_context": {"type": "object", "additionalProperties": True},
                                        "entry_mode": {
                                            "type": "string",
                                            "enum": ["none", "conditional", "immediate"],
                                        },
                                        "action_window": text,
                                        "immediate_action": text,
                                        "watch_conditions": {"type": "array", "minItems": 1, "items": text},
                                        "next_check_time": text,
                                        "confidence_reason": text,
                                        "data_limitations": {"type": "array", "items": text},
                                    },
                                },
                                "signal_attribution": {
                                    "type": "object",
                                    "additionalProperties": True,
                                    "properties": {
                                        "technical_indicators": metric_number_or_text,
                                        "news_sentiment": metric_number_or_text,
                                        "fundamentals": metric_number_or_text,
                                        "market_conditions": metric_number_or_text,
                                        "strongest_bullish_signal": text,
                                        "strongest_bearish_signal": text,
                                    },
                                },
                            },
                        },
                        "analysis_summary": text,
                        "key_points": text,
                        "risk_warning": text,
                        "buy_reason": text,
                        "trend_analysis": text,
                        "short_term_outlook": text,
                        "medium_term_outlook": text,
                        "technical_analysis": text,
                        "ma_analysis": text,
                        "volume_analysis": text,
                        "pattern_analysis": text,
                        "fundamental_analysis": text,
                        "sector_position": text,
                        "company_highlights": text,
                        "news_summary": text,
                        "market_sentiment": text,
                        "hot_topics": text,
                        "data_sources": text,
                    },
                },
            },
        }

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        config: Optional[Config] = None,
        skills: Optional[List[str]] = None,
        skill_instructions: Optional[str] = None,
        default_skill_policy: Optional[str] = None,
        use_legacy_default_prompt: Optional[bool] = None,
    ):
        """Initialize LLM Analyzer via LiteLLM.

        Args:
            api_key: Ignored (kept for backward compatibility). Keys are loaded from config.
        """
        self._config_override = config
        self._requested_skills = list(skills) if skills is not None else None
        self._skill_instructions_override = skill_instructions
        self._default_skill_policy_override = default_skill_policy
        self._use_legacy_default_prompt_override = use_legacy_default_prompt
        self._resolved_prompt_state: Optional[Dict[str, Any]] = None
        self._router = None
        self._legacy_router_model_list: List[Dict[str, Any]] = []
        self._litellm_available = False
        self._init_litellm()
        if not self._litellm_available:
            try:
                backend_id, _fallback_backend_id = self._resolve_generation_backend_config()
            except GenerationError:
                backend_id = ""
            if backend_id in LOCAL_CLI_GENERATION_BACKEND_IDS:
                logger.info(
                    "Analyzer generation backend: %s configured; LiteLLM API keys are not "
                    "required for stock analysis generation",
                    backend_id,
                )
            else:
                logger.warning("No LLM configured (LITELLM_MODEL / API keys), AI analysis will be unavailable")

    def _get_runtime_config(self) -> Config:
        """Return the runtime config, honoring injected overrides for tests/pipeline."""
        return getattr(self, "_config_override", None) or get_config()

    def _get_skill_prompt_sections(self) -> tuple[str, str, bool]:
        """Resolve skill instructions + default baseline + prompt mode."""
        skill_instructions = getattr(self, "_skill_instructions_override", None)
        default_skill_policy = getattr(self, "_default_skill_policy_override", None)
        use_legacy_default_prompt = getattr(self, "_use_legacy_default_prompt_override", None)

        if skill_instructions is not None and default_skill_policy is not None:
            return (
                skill_instructions,
                default_skill_policy,
                bool(use_legacy_default_prompt) if use_legacy_default_prompt is not None else False,
            )

        resolved_state = getattr(self, "_resolved_prompt_state", None)
        if resolved_state is None:
            from src.agent.factory import resolve_skill_prompt_state

            prompt_state = resolve_skill_prompt_state(
                self._get_runtime_config(),
                skills=getattr(self, "_requested_skills", None),
            )
            resolved_state = {
                "skill_instructions": prompt_state.skill_instructions,
                "default_skill_policy": prompt_state.default_skill_policy,
                "use_legacy_default_prompt": bool(getattr(prompt_state, "use_legacy_default_prompt", False)),
            }
            self._resolved_prompt_state = resolved_state

        return (
            skill_instructions if skill_instructions is not None else resolved_state.get("skill_instructions", ""),
            default_skill_policy if default_skill_policy is not None else resolved_state.get("default_skill_policy", ""),
            (
                use_legacy_default_prompt
                if use_legacy_default_prompt is not None
                else bool(resolved_state.get("use_legacy_default_prompt", False))
            ),
        )

    def _get_analysis_system_prompt(
        self,
        report_language: str,
        stock_code: str = "",
        *,
        structured_tool_output: bool = False,
        paper_trading_style_context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Build the analyzer system prompt with output-language guidance."""
        lang = normalize_report_language(report_language)
        market_role = get_market_role(stock_code, lang)
        market_guidelines = get_market_guidelines(stock_code, lang)
        skill_instructions, default_skill_policy, use_legacy_default_prompt = self._get_skill_prompt_sections()
        has_style_contract = isinstance(paper_trading_style_context, dict) and bool(
            paper_trading_style_context.get("trading_style")
            or paper_trading_style_context.get("settings")
        )
        if use_legacy_default_prompt and not has_style_contract:
            base_prompt = self.LEGACY_DEFAULT_SYSTEM_PROMPT.replace(
                "{market_placeholder}", market_role
            ).replace(
                "{guidelines_placeholder}", market_guidelines
            )
        else:
            skills_section = ""
            if skill_instructions:
                skills_section = f"## 激活的交易技能\n\n{skill_instructions}\n"
            default_skill_policy_section = ""
            if has_style_contract:
                style = str(paper_trading_style_context.get("trading_style") or "unknown").strip() or "unknown"
                settings = paper_trading_style_context.get("settings")
                settings = settings if isinstance(settings, dict) else {}
                default_skill_policy_section = f"""## 纸面交易风格覆盖默认短周期基线（必须严格遵守）

当前报告带有冻结纸面交易风格：`{style}`。该风格的输入窗口、核心均线、ATR/波动、持有期和刷新节奏以用户上下文中的“纸面交易风格契约”为准。

- 默认 bull_trend / 多头排列基线只作为短周期参考，不是本次风格的硬门槛。
- 不得把 `MA5 > MA10 > MA20`、MA5 乖离小于固定阈值或“只做多头排列”作为所有风格的买入必要条件。
- 中线/持仓风格必须综合 MA10/MA20/MA60、支撑压力、ATR/波动、成交参与度、资金和风险收益；趋势风格还应参考 MA20/MA60/MA120 或更长周期。
- 若配置给出的核心窗口为 `{settings.get('core_ma_windows', 'N/A')}`、分析窗口为 `{settings.get('analysis_window_sessions', 'N/A')}` 个交易日，后续价格、止损、止盈和观察条件必须与这些窗口一致。
"""
            elif default_skill_policy:
                default_skill_policy_section = f"{default_skill_policy}\n"
            base_prompt = (
                self.SYSTEM_PROMPT.replace("{market_placeholder}", market_role)
                .replace("{guidelines_placeholder}", market_guidelines)
                .replace("{default_skill_policy_section}", default_skill_policy_section)
                .replace("{skills_section}", skills_section)
            )
        if structured_tool_output:
            base_prompt = self._replace_output_template_with_tool_contract(base_prompt, lang)
        if lang == "en":
            envelope = (
                """

## Output Envelope (highest priority)

- Submit the report by calling `submit_analysis_report` exactly once.
- Do not add plain text, Markdown fences, headings, a company-name preface, explanations, or a conclusion summary outside the tool arguments.
- Put every requested answer and explanation inside the tool arguments. Never append a second narrative section.
"""
                if structured_tool_output
                else """

## Output Envelope (highest priority)

- The entire response must be exactly one JSON object and nothing else.
- Do not add Markdown fences, headings, a company-name preface, explanations, or a conclusion summary before or after the JSON object.
- Put every requested answer and explanation inside the defined JSON fields. Never append a second narrative section.
"""
            )
            return base_prompt + """

## Output Language (highest priority)

- Keep all JSON keys unchanged.
- `decision_type` must remain `buy|hold|sell`.
- All human-readable JSON values must be written in English.
- Use the common English company name when you are confident; otherwise keep the original listed company name instead of inventing one.
- This includes `stock_name`, `trend_prediction`, `operation_advice`, `confidence_level`, nested dashboard text, checklist items, and all narrative summaries.
""" + envelope
        if lang == "ko":
            envelope = (
                """

## 출력 외피 (최우선)

- `submit_analysis_report` 도구를 정확히 한 번 호출하여 보고서를 제출하세요.
- 도구 인자 밖에 일반 텍스트, Markdown 코드 펜스, 제목, 종목명 확인 문구, 설명 또는 결론 요약을 추가하지 마세요.
- 요청된 모든 답변과 설명은 도구 인자 안에 작성하고 별도의 서술 섹션을 덧붙이지 마세요.
"""
                if structured_tool_output
                else """

## 출력 외피 (최우선)

- 전체 응답은 정확히 하나의 JSON 객체여야 하며 그 밖의 내용은 출력하지 마세요.
- JSON 앞뒤에 Markdown 코드 펜스, 제목, 종목명 확인 문구, 설명 또는 결론 요약을 추가하지 마세요.
- 요청된 모든 답변과 설명은 정의된 JSON 필드 안에 작성하고 별도의 서술 섹션을 덧붙이지 마세요.
"""
            )
            return base_prompt + """

## Output Language (highest priority)

- Keep all JSON keys unchanged.
- `decision_type` must remain `buy|hold|sell`.
- All human-readable JSON values must be written in Korean (한국어).
- Use the common Korean or original listed company name when confident; do not invent one.
- This includes `stock_name`, `trend_prediction`, `operation_advice`, `confidence_level`, nested dashboard text, checklist items, and all narrative summaries.
""" + envelope
        envelope = (
            """

## 输出封装（最高优先级）

- 必须且只能调用 `submit_analysis_report` 工具一次来提交报告。
- 工具参数外不得添加普通文本、Markdown 代码围栏、标题、股票名称核对、解释或结论摘要。
- 所有重点问题的回答与说明都必须写入工具参数字段，禁止追加第二段说明。
"""
            if structured_tool_output
            else """

## 输出封装（最高优先级）

- 整个回复必须且只能是一个 JSON 对象，不得包含任何其他内容。
- JSON 对象前后不得添加 Markdown 代码围栏、标题、股票名称核对、解释或结论摘要。
- 所有重点问题的回答与说明都必须写入已定义的 JSON 字段，禁止在 JSON 后追加第二段说明。
"""
        )
        return base_prompt + """

## 输出语言（最高优先级）

- 所有 JSON 键名保持不变。
- `decision_type` 必须保持为 `buy|hold|sell`。
- 所有面向用户的人类可读文本值必须使用中文。
""" + envelope

    @classmethod
    def _replace_output_template_with_tool_contract(cls, base_prompt: str, lang: str) -> str:
        """Replace literal JSON examples with a semantic tool-submission contract."""
        if lang in ("en", "ko"):
            replacement = f"""
## Structured report submission

- Submit the complete decision dashboard through the `{cls.ANALYSIS_REPORT_TOOL_NAME}` tool with `report_schema_version="{cls.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION}"`.
- The tool schema replaces only the literal JSON example. It does not replace the reasoning or evidence contract below.
- Preserve this complete chain: market phase → daily market context → market structure → technical indicators → price/volume and turnover → chip structure → fundamentals → news → portfolio state → risk/reward → action timing.

### Evidence and decision sequence

1. Establish the market phase and data freshness first. Never describe a premarket, postmarket, stale, partial, fallback, estimated, or missing quote as a completed live session.
2. Evaluate daily-market regime and market structure before the stock: risk appetite, index/sector direction, theme phase, stock role, and any market-level veto.
3. Evaluate trend and location: MA alignment, support/resistance, deviation from MA5, pattern and volatility. Location must be considered together with direction.
4. Evaluate confirmation: volume ratio, turnover, expansion/contraction, capital flow and chip structure. Missing evidence is a limitation, not a positive signal.
5. Evaluate fundamentals, announcements, news catalysts and risks with dates and source scope; do not invent facts.
6. Resolve portfolio state before choosing an action. Empty accounts may buy when evidence and risk/reward support it; they must not be described as unable to buy, and must not use hold/reduce/sell. Existing holdings may add/hold/reduce/sell/watch/avoid/alert with explicit risk controls.
7. Compare upside, downside, stop distance, target distance and risk/reward; then choose action timing and confidence.

### Field semantics and cross-field consistency

- `raw_score` is the evidence score. `adjusted_score` is the final risk-controlled score. Any adjustment must be explicit in the guardrail/calibration evidence; missing data alone must not silently change the score.
- `sentiment_score`, `trend_prediction`, `operation_advice`, `decision_type`, `action`, localized action text, `position_actions`, core conclusion and phase decision must describe one consistent final decision.
- `confidence_level` must be exactly one standalone label: `high`, `medium`, or `low`. Put confidence explanations in `limitations`, `risk_warning`, or `dashboard.phase_decision.confidence_reason`; do not write compound labels such as medium-low or a parenthesized sentence inside `confidence_level`.
- `dashboard.phase_decision.entry_mode` is optional and must be `none`, `conditional`, or `immediate` when present. `conditional` means a real buy/add authorization is waiting for stated trigger conditions, so top-level `action` must still be `buy/add` and the executable buy/add contract must be complete. `none` means no entry authorization. Never upgrade `watch` into buy/add merely because an ideal price or score exists.
- `dashboard.core_conclusion` states the one-sentence thesis, signal class, time sensitivity and separate empty/holding guidance.
- `dashboard.data_perspective` records trend, price location, volume/turnover and chip evidence. Use an unavailable/limited description when input data is absent instead of creating values.
- `dashboard.intelligence` separates latest news, dated risks, positive catalysts, earnings outlook and sentiment summary.
- `dashboard.battle_plan` contains price/risk levels, position sizing rationale, entry plan, risk control and an evidence checklist.
- `dashboard.phase_decision` must state the observed phase, action window, immediate action, concrete watch conditions, next check point, confidence rationale and data limitations.
- `dashboard.signal_attribution` explains the four contribution dimensions and strongest bullish/bearish evidence; it must not override the final action by itself.
- Detailed narrative fields must explain trend, short/medium outlook, technicals, MAs, volume, pattern, fundamentals, sector position, company risks/highlights, news, market sentiment and hot topics without contradicting the dashboard.
- Separate position thesis from short-term timing. MA5 and short MA alignment are timing/location evidence; do not make MA5>MA10>MA20 a universal buy gate for medium-term or trend styles. Medium-term holdings should emphasize MA10/MA20/MA60 plus support, volatility, participation and risk/reward; trend styles should include MA20/MA60/MA120 or longer context.
- If news is unavailable, missing, outside the allowed window, or has unknown dates, say there is no citable current-window news evidence; do not infer that no events or no risks exist. Stale fundamentals may be historical background only, not current fundamental evidence.

### Executable buy/add contract

- A buy/add decision is valid only when evidence supports a bounded `close_between` entry range, explicit allocation per unique ordered entry leg, positive stop loss, ordered exit legs whose allocations total approximately 1.0, and `stop < entry range < final target`.
- A conditional entry may wait for price/volume confirmation in `phase_decision.immediate_action`. Treat it as authorization for a candidate conditional strategy: the preset entry range must be supported by net risk/reward, ATR, and trading-style evidence, while two-report confirmation, market/account-risk, and trigger-price gates are enforced independently downstream. Do not claim those downstream gates have already passed, and do not raise the score just to obtain buy/add.
- Entry allocations are portfolio-budget requests, not permission to exceed account, single-stock, concentration, risk, or maximum-position limits enforced downstream.
- For buy/add, declare `dashboard.battle_plan.setup.setup_type` as breakout, pullback, or trend_continuation. Match confirmation to that setup: expansion for a breakout, controlled contraction plus support for a pullback, maintained trend plus participation for continuation. Do not demand breakout volume from a pullback or treat missing evidence as confirmation.
- Use the frozen trading-style horizon, MA and ATR values. Do not switch a medium-term position to an intraday thesis. On hold/watch, a new stop above current price or target below current price is not a disguised sell instruction; explain a defensive action explicitly instead. Entry confirmation is counted by the backend, never by repeated model calls on the same day.
- Every buy/add must include action window, watch conditions, next check time, confidence reason and limitations. If any required price or confirmation cannot be justified, return watch/avoid instead of fabricating an executable plan.
- Hold/watch/reduce/sell/avoid must not be padded with invented entry prices merely to fill a shape.
"""
        else:
            replacement = f"""
## 结构化报告提交

- 通过 `{cls.ANALYSIS_REPORT_TOOL_NAME}` 工具提交完整决策仪表盘，并填写 `report_schema_version="{cls.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION}"`。
- Tool Schema 只替代原来的大段 JSON 示例，不替代下列推理、证据和一致性契约。
- 必须保留完整链路：市场阶段 → 大盘环境 → 市场结构 → 技术指标 → 量价/换手 → 筹码 → 基本面 → 新闻 → 持仓状态 → 风险收益 → 动作时机。

### 证据与决策顺序

1. 先确认市场阶段和数据新鲜度。盘前、盘后、陈旧、部分、fallback、估算或缺失数据，不得描述成已经完成的实时盘中事实。
2. 先看大盘和市场结构，再看个股：风险偏好、指数/行业方向、题材阶段、个股角色以及大盘级否决条件都要进入判断。
3. 判断趋势和位置：均线排列、支撑压力、MA5 乖离、形态与波动必须综合使用，不能只看涨跌方向而忽略所处位置。
4. 判断确认信号：量比、换手、放量/缩量、资金流和筹码结构要互相印证；缺失数据只能列入限制，不能当成利好。
5. 基本面、公告、新闻催化和风险必须注明日期与来源范围，不得编造事实。
6. 决定动作前先识别持仓状态。空仓在证据和风险收益支持时可以买入，不得写成“空仓不能买入”，也不得对空仓使用持有/减仓/卖出；已有持仓才可使用加仓/持有/减仓/卖出/观望/回避/预警，并给出明确风控。
7. 比较上行空间、下行风险、止损距离、目标距离和风险收益比，最后确定动作时机与置信度。

### 字段语义与前后一致性

- `raw_score` 是证据原始分，`adjusted_score` 是经过明确风险规则后的最终分；任何调整都必须在 guardrail/校准证据中可审计，缺数据不能暗中改分。
- `sentiment_score`、趋势、操作建议、`decision_type`、`action`、本地化动作、分持仓动作、核心结论和阶段决策必须表达同一个最终决策。
- `confidence_level` 必须只填写单独标签：`高`、`中` 或 `低`。置信度解释写入 `limitations`、`risk_warning` 或 `dashboard.phase_decision.confidence_reason`，不得写“中等偏低”或带括号长说明。
- `dashboard.phase_decision.entry_mode` 是可选字段；填写时只能是 `none`、`conditional` 或 `immediate`。`conditional` 表示已有真实买入/加仓授权但等待触发条件，顶层 `action` 仍必须是 `buy/add` 且可执行买入/加仓契约完整；`none` 表示没有入场授权。不得仅因理想价或评分存在，把 `watch` 升级成买入/加仓。
- `dashboard.core_conclusion` 写一句话结论、信号类别、时效性和空仓/持仓两套建议。
- `dashboard.data_perspective` 写趋势、价格位置、量价/换手和筹码证据；输入缺失时明确“不可用/受限”，不得补造数值。
- `dashboard.intelligence` 分开写最新消息、带日期的风险、正面催化、业绩展望和情绪摘要。
- `dashboard.battle_plan` 写价格与风险点位、仓位理由、入场计划、风险控制和证据检查清单。
- `dashboard.phase_decision` 必须写观察到的市场阶段、行动窗口、立即动作、具体观察条件、下次检查点、置信理由和数据限制。
- `dashboard.signal_attribution` 解释技术、新闻、基本面和大盘四类贡献以及最强多空证据，但不得单独推翻最终动作。
- 走势、短中期展望、技术面、均线、量能、形态、基本面、行业地位、公司亮点/风险、新闻、市场情绪和热点等详细字段都必须与仪表盘一致。
- 持仓 thesis 与短线入场择时要分开。MA5 和短均线排列主要是时机/位置证据，不得把 MA5>MA10>MA20 当作中线或趋势风格的通用买入硬门槛；中线持仓应重点结合 MA10/MA20/MA60、支撑压力、波动、成交/资金和风险收益，趋势风格还应参考 MA20/MA60/MA120 或更长周期。
- 新闻缺失、不可用、超出窗口或时间未知，只表示“当前窗口没有可引用的新闻证据”，不得推断为“没有事件/没有利空”；基本面数据陈旧只能作历史背景，不得当作当前最新基本面。

### 可执行买入/加仓契约

- 买入/加仓只有在证据支持时才成立：必须提供有上下界的 `close_between` 入场区间、唯一且顺序明确的入场批次及分配比例、正数止损、顺序递增且比例合计约为 1.0 的退出批次，并满足 `止损 < 入场区间 < 最终目标`。
- 条件入场可以在 `phase_decision.immediate_action` 中写等待价格/量能确认，并表示候选条件策略授权；预设入场区间本身仍必须有净盈亏比、ATR 和交易风格证据支持。两交易日报告确认、大盘/市场、账户风险和触价门禁由下游独立执行；不得声称这些下游门禁已经通过，也不得为了获得买入/加仓而抬高评分。
- 入场比例只是账户预算请求，不能绕过后端的总资金、单股集中度、单笔风险和最大持仓数限制。
- 买入/加仓须在 `dashboard.battle_plan.setup.setup_type` 声明 breakout（突破）、pullback（回踩）或 trend_continuation（趋势延续）。突破看放量确认，回踩看缩量与支撑，延续看趋势与参与度；不得用突破放量条件否定合理缩量回踩，也不得把缺失数据当作确认。
- 使用冻结风格对应的期限、均线和 ATR 数值，不得把中线仓变成日内策略。持有/观望时，不应给出现价以上止损或现价以下目标来隐含立即卖出；需要防御退出时明确表达动作和证据。确认次数由后端按不同交易日计算，同日重复调用不能累计。
- 每个买入/加仓还必须给出行动窗口、观察条件、下次检查时间、置信理由和数据限制；任一价格或确认条件无法从证据推出时，应输出观望/回避，不得编造可执行计划。
- 持有、观望、减仓、卖出或回避不应为了填格式而编造买入价。
"""
        return re.sub(
            r"\n## 输出格式：决策仪表盘 JSON\n.*?(?=\n## 评分标准)",
            "\n" + replacement,
            base_prompt,
            flags=re.DOTALL,
        )

    def _has_channel_config(self, config: Config) -> bool:
        """Check if multi-channel config (channels / YAML / legacy model_list) is active."""
        return bool(config.llm_model_list) and not all(
            e.get('model_name', '').startswith('__legacy_') for e in config.llm_model_list
        )

    @staticmethod
    def _legacy_router_provider_alias(model: str) -> str:
        provider = model.split("/", 1)[0] if "/" in model else "openai"
        return f"__legacy_{provider}__"

    @staticmethod
    def _build_legacy_router_model_list_from_config(
        model: str,
        model_list: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Build legacy-router candidates from configured legacy llm_model_list entries."""
        if not model:
            return []
        target_model = model
        target_legacy_alias = GeminiAnalyzer._legacy_router_provider_alias(model)
        legacy_entries: List[Dict[str, Any]] = []
        for entry in model_list or []:
            if not isinstance(entry, dict):
                continue
            model_name = str(entry.get("model_name") or "").strip()
            if model_name != target_legacy_alias:
                continue

            params = entry.get("litellm_params")
            if not isinstance(params, dict):
                continue

            api_key = str(params.get("api_key") or "").strip()
            if not api_key or len(api_key) < 8:
                continue

            deployed_params = dict(params)
            deployed_params["model"] = target_model
            deployed_params["api_key"] = api_key
            legacy_entries.append({
                "model_name": target_model,
                "litellm_params": deployed_params,
            })

        return legacy_entries

    def _init_litellm(self) -> None:
        """Initialize litellm Router from channels / YAML / legacy keys."""
        config = self._get_runtime_config()
        if self._get_hermes_config_error(config) is not None:
            logger.error("Analyzer LLM: Hermes channel configuration blocks legacy fallback")
            return
        litellm_model = config.litellm_model
        if not litellm_model:
            backend_id = ""
            try:
                backend_id = resolve_generation_backend_id(config)
            except GenerationError:
                pass
            if backend_id in LOCAL_CLI_GENERATION_BACKEND_IDS:
                logger.info(
                    "Analyzer LiteLLM: LITELLM_MODEL not configured; using %s generation backend",
                    backend_id,
                )
            else:
                logger.warning("Analyzer LLM: LITELLM_MODEL not configured")
            return

        self._litellm_available = True

        # --- Channel / YAML path: build Router from pre-built model_list ---
        if self._has_channel_config(config):
            model_list = config.llm_model_list
            if self._get_mixed_hermes_route_error(config, litellm_model) is not None:
                self._litellm_available = False
                logger.error("Analyzer LLM: mixed Hermes/non-Hermes route requires deployment-level no-proxy support")
                return
            router_model_list = model_list
            if route_has_hermes(model_list, litellm_model):
                # Hermes-only routes are dispatched directly with a request-scoped
                # no-proxy OpenAI client. Keeping them out of Router prevents the
                # default proxy-aware transport from seeing the Hermes bearer key.
                router_model_list = filter_non_hermes_deployments(model_list)
                if not router_model_list:
                    self._litellm_available = True
                    logger.info("Analyzer LLM: Hermes-only route will use direct no-proxy completion")
                    return
            try:
                self._router = Router(
                    model_list=router_model_list,
                    routing_strategy="simple-shuffle",
                    num_retries=2,
                )
            except TypeError:
                logger.debug("Analyzer LLM: Router constructor signature not compatible; fallback to direct mode")
                self._router = None
            else:
                unique_models = list(dict.fromkeys(
                    e['litellm_params']['model'] for e in model_list
                ))
                logger.info(
                    f"Analyzer LLM: Router initialized from channels/YAML — "
                    f"{len(router_model_list)} deployment(s), models: {unique_models}"
                )
                return

        # --- Legacy path: build Router for multi-key, or use single key ---
        keys = get_api_keys_for_model(litellm_model, config)
        legacy_model_list = self._build_legacy_router_model_list_from_config(
            litellm_model,
            config.llm_model_list,
        )
        if len(legacy_model_list) <= 1 and keys:
            extra_params = extra_litellm_params(litellm_model, config)
            configured_model_list = [
                {
                    "model_name": litellm_model,
                    "litellm_params": {
                        "model": litellm_model,
                        "api_key": k,
                        **extra_params,
                    },
                }
                for k in keys
            ]
            if not legacy_model_list:
                legacy_model_list = configured_model_list
            elif len(legacy_model_list) < len(configured_model_list):
                legacy_model_list = configured_model_list

        if len(legacy_model_list) > 1:
            self._legacy_router_model_list = legacy_model_list
            try:
                self._router = Router(
                    model_list=legacy_model_list,
                    routing_strategy="simple-shuffle",
                    num_retries=2,
                )
            except TypeError:
                logger.debug("Analyzer LLM: Legacy Router constructor signature not compatible; using legacy model_list fallback")
                self._router = None
            else:
                logger.info(
                    f"Analyzer LLM: Legacy Router initialized with {len(legacy_model_list)} keys "
                    f"for {litellm_model}"
                )
                return

        if keys:
            logger.info(f"Analyzer LLM: litellm initialized (model={litellm_model})")
        else:
            logger.info(
                f"Analyzer LLM: litellm initialized (model={litellm_model}, "
                f"API key from environment)"
            )

    def is_available(self) -> bool:
        """Check whether the configured generation backend is available."""
        backend_error = self.get_generation_backend_config_error()
        if backend_error is not None:
            return self._can_use_generation_fallback(backend_error)
        backend_id, _fallback_backend_id = self._resolve_generation_backend_config()
        if backend_id in LOCAL_CLI_GENERATION_BACKEND_IDS:
            return True
        return self._litellm_runtime_available()

    def _litellm_runtime_available(self) -> bool:
        return self._router is not None or self._litellm_available

    def _can_use_generation_fallback(self, backend_error: GenerationError) -> bool:
        if not backend_error.fallbackable:
            return False
        try:
            _backend_id, fallback_backend_id = self._resolve_generation_backend_config()
        except GenerationError:
            return False
        return (
            fallback_backend_id == LITELLM_BACKEND_ID
            and self._litellm_runtime_available()
        )

    def _resolve_generation_backend_config(self) -> Tuple[str, Optional[str]]:
        """Resolve and validate generation backend ids."""
        config = self._get_runtime_config()
        backend_id = resolve_generation_backend_id(config)
        fallback_backend_id = resolve_generation_fallback_backend_id(config)
        return backend_id, fallback_backend_id

    def get_generation_backend_config_error(self) -> Optional[GenerationError]:
        """Return a structured backend config error, if the backend cannot run."""
        try:
            backend_id, _fallback_backend_id = self._resolve_generation_backend_config()
            config = self._get_runtime_config()
            hermes_error = self._get_hermes_config_error(config)
            if hermes_error is not None:
                return hermes_error
            for model in [getattr(config, "litellm_model", "")] + list(getattr(config, "litellm_fallback_models", []) or []):
                mixed_error = self._get_mixed_hermes_route_error(config, model)
                if mixed_error is not None:
                    return mixed_error
            if backend_id in LOCAL_CLI_GENERATION_BACKEND_IDS:
                backend = self._get_generation_backend(backend_id)
                get_config_error = getattr(backend, "get_config_error", None)
                if callable(get_config_error):
                    return get_config_error()
        except GenerationError as exc:
            return exc
        return None

    def _get_hermes_config_error(self, config: Config) -> Optional[GenerationError]:
        issues = list(getattr(config, "llm_channel_config_issues", []) or [])
        if not getattr(config, "llm_blocks_legacy_fallback", False) or not issues:
            return None
        blocked_routes = set(getattr(config, "llm_blocked_hermes_routes", []) or [])
        selected_models = [
            ("LITELLM_MODEL", getattr(config, "litellm_model", "") or ""),
            *[
                ("LITELLM_FALLBACK_MODELS", fallback_model)
                for fallback_model in list(getattr(config, "litellm_fallback_models", []) or [])
            ],
        ]
        selected_blocked_route = ""
        selected_field = ""
        for field_name, model in selected_models:
            raw_model = str(model or "").strip()
            if not raw_model:
                continue
            candidates = hermes_blocked_route_candidates(raw_model)
            candidates.add(raw_model)
            try:
                candidates.add(canonicalize_hermes_model_ref(raw_model).route_model)
            except (TypeError, ValueError) as exc:
                logger.debug("Failed to canonicalize selected Hermes route candidate %r: %s", raw_model, exc)
            matched = candidates & blocked_routes
            if matched:
                selected_blocked_route = sorted(matched)[0]
                selected_field = field_name
                break
        if blocked_routes and not selected_blocked_route and getattr(config, "llm_model_list", None):
            return None
        first = issues[0]
        code = (
            "explicit_hermes_route_invalid"
            if selected_blocked_route
            else first.get("code", "invalid_hermes_channel")
        )
        return GenerationError(
            error_code=GenerationErrorCode.UNSAFE_CONFIG,
            stage="configuration",
            retryable=False,
            fallbackable=False,
            backend=LITELLM_BACKEND_ID,
            provider=HERMES_CHANNEL_NAME,
            details={
                "field": selected_field or first.get("field", "LLM_HERMES_API_KEY"),
                "code": code,
                "reason": code,
                "message": first.get("message", "Hermes channel configuration is invalid"),
                "issues": issues,
                "route_name": selected_blocked_route or None,
            },
        )

    def _get_mixed_hermes_route_error(self, config: Config, model: str) -> Optional[GenerationError]:
        if not model:
            return None
        origins = route_deployment_origins(getattr(config, "llm_model_list", []) or [], model)
        if not origins.is_mixed:
            return None
        return GenerationError(
            error_code=GenerationErrorCode.UNSAFE_CONFIG,
            stage="configuration",
            retryable=False,
            fallbackable=False,
            backend=LITELLM_BACKEND_ID,
            provider=HERMES_CHANNEL_NAME,
            details={
                "field": "LLM_CHANNELS",
                "code": "mixed_hermes_route_unsupported",
                "reason": "router_deployment_no_proxy_unavailable",
                "route_name": model,
            },
        )

    def _hermes_redaction_values_for_model(self, config: Config, model: str = "") -> set[str]:
        redactions: set[str] = set()
        deployments = list(getattr(config, "llm_model_list", []) or [])
        selected_deployments = deployments
        if model:
            origins = route_deployment_origins(deployments, model)
            selected_deployments = list(origins.hermes_deployments or [])
            if not selected_deployments and not origins.has_hermes:
                return redactions
        for deployment in selected_deployments:
            if not isinstance(deployment, dict):
                continue
            if not route_has_hermes([deployment], str(deployment.get("model_name") or "")):
                continue
            params = deployment.get("litellm_params") or {}
            if isinstance(params, dict):
                redactions.update(build_hermes_redaction_values(params.get("api_key")))
        return redactions

    def _sanitize_hermes_exception_text(
        self,
        exc: Any,
        *,
        config: Optional[Config] = None,
        model: str = "",
    ) -> str:
        runtime_config = config or self._get_runtime_config()
        redactions = self._hermes_redaction_values_for_model(runtime_config, model)
        if not redactions:
            return str(exc)
        return sanitize_hermes_error_text(exc, redaction_values=redactions)

    def _litellm_redaction_values_for_model(self, config: Config, model: str = "") -> set[str]:
        redactions = self._hermes_redaction_values_for_model(config, model)
        try:
            redactions.update(build_hermes_redaction_values(*get_api_keys_for_model(model, config)))
        except Exception:
            pass
        origins = route_deployment_origins(getattr(config, "llm_model_list", []) or [], model)
        for deployment in (*origins.hermes_deployments, *origins.non_hermes_deployments):
            params = deployment.get("litellm_params") if isinstance(deployment, dict) else None
            if isinstance(params, dict):
                redactions.update(build_hermes_redaction_values(params.get("api_key")))
        return redactions

    def _sanitize_litellm_exception_text(
        self,
        exc: Any,
        *,
        config: Optional[Config] = None,
        model: str = "",
    ) -> str:
        runtime_config = config or self._get_runtime_config()
        redactions = self._litellm_redaction_values_for_model(runtime_config, model)
        sanitized = sanitize_hermes_error_text(exc, redaction_values=redactions)
        return redact_diagnostic_text(sanitized, limit=500)

    def _dispatch_litellm_completion(
        self,
        model: str,
        call_kwargs: Dict[str, Any],
        *,
        config: Config,
        use_channel_router: bool,
        router_model_names: set[str],
    ) -> Any:
        """Dispatch a LiteLLM completion through router or direct fallback."""
        origins = route_deployment_origins(config.llm_model_list, model)
        if origins.is_mixed:
            raise RuntimeError("Hermes/non-Hermes mixed generation route is not supported without deployment-level no-proxy client support")
        if origins.is_hermes_only:
            deployment = origins.hermes_deployments[0]
            params = dict(deployment.get("litellm_params") or {})
            api_key = str(params.get("api_key") or "").strip()
            base_url = str(params.get("api_base") or "").strip()
            if is_masked_secret_placeholder(api_key):
                raise RuntimeError("Hermes API key is a masked placeholder and cannot be used for generation")
            timeout = float(call_kwargs.get("timeout") or 30.0)
            hermes_kwargs = dict(call_kwargs)
            hermes_kwargs["model"] = str(params.get("model") or model)
            hermes_kwargs["stream"] = False
            hermes_kwargs.pop("api_key", None)
            hermes_kwargs.pop("api_base", None)
            with open_hermes_no_proxy_client(api_key=api_key, base_url=base_url, timeout=timeout) as client:
                hermes_kwargs["client"] = client
                return litellm.completion(**hermes_kwargs)

        wire_models = resolve_fallback_litellm_wire_models(model, config.llm_model_list)
        register_fallback_model_pricing(wire_models)
        effective_kwargs = dict(call_kwargs)
        if use_channel_router and self._router and model in router_model_names:
            return self._router.completion(**effective_kwargs)
        if self._router and model == config.litellm_model and not use_channel_router:
            return self._router.completion(**effective_kwargs)

        keys = get_api_keys_for_model(model, config)
        if keys:
            effective_kwargs["api_key"] = keys[0]
        effective_kwargs.update(extra_litellm_params(model, config))
        return litellm.completion(**effective_kwargs)

    def _normalize_usage(
        self,
        usage_obj: Any,
        *,
        model: str = "",
        provider: Optional[str] = None,
        messages: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Normalize usage objects from LiteLLM responses/chunks."""
        if not usage_obj:
            usage = attach_message_hmacs({}, messages) if messages is not None else {}
            return filter_prompt_cache_telemetry(usage, self._get_runtime_config())
        usage = normalize_litellm_usage(usage_obj, model=model, provider=provider)
        if messages is not None:
            usage = attach_message_hmacs(usage, messages)
        return filter_prompt_cache_telemetry(usage, self._get_runtime_config())

    @staticmethod
    def _get_response_field(obj: Any, key: str) -> Any:
        """Read a field from dict-like or object-like LiteLLM payloads."""
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    def _extract_text_blocks(self, blocks: Any) -> str:
        """Extract text from OpenAI-compatible content block lists."""
        if not blocks:
            return ""

        parts: List[str] = []
        for block in blocks:
            if isinstance(block, str):
                parts.append(block)
                continue

            text = None
            if isinstance(block, dict):
                text = block.get("text")
                if text is None:
                    text = block.get("content")
            else:
                text = getattr(block, "text", None)
                if text is None:
                    text = getattr(block, "content", None)

            if isinstance(text, str) and text:
                parts.append(text)

        return "".join(parts).strip()

    def _extract_completion_text(self, response: Any) -> str:
        """Extract text from non-stream LiteLLM completion responses."""
        choices = self._get_response_field(response, "choices")
        if not choices:
            return ""

        choice = choices[0]
        message = self._get_response_field(choice, "message")

        content_blocks = self._get_response_field(choice, "content_blocks")
        if content_blocks is None and message is not None:
            content_blocks = self._get_response_field(message, "content_blocks")
        block_text = self._extract_text_blocks(content_blocks)
        if block_text:
            return block_text

        content = None
        if message is not None:
            content = self._get_response_field(message, "content")
        if content is None:
            content = self._get_response_field(choice, "content")

        if isinstance(content, list):
            return self._extract_text_blocks(content)
        if isinstance(content, str):
            return content.strip()
        return str(content).strip() if content is not None else ""

    def _extract_required_tool_arguments(self, response: Any, expected_name: str) -> str:
        """Extract exactly one forced tool-call argument payload.

        Standard stock analysis uses this path to avoid accepting prose or a
        loosely formatted JSON blob when a provider/gateway supports native
        tool calling.  The returned value is normalized JSON text so existing
        report parsing and integrity checks keep working unchanged.
        """
        choices = self._get_response_field(response, "choices")
        if not choices:
            raise _StructuredToolExtractionError("tool_call_missing")

        tool_calls: List[Any] = []
        for choice in choices:
            message = self._get_response_field(choice, "message")
            choice_calls = None
            if message is not None:
                choice_calls = self._get_response_field(message, "tool_calls")
            if choice_calls is None:
                choice_calls = self._get_response_field(choice, "tool_calls")
            if choice_calls:
                tool_calls.extend(list(choice_calls))

        if len(tool_calls) != 1:
            raise _StructuredToolExtractionError(
                "tool_call_missing" if not tool_calls else "tool_call_ambiguous"
            )

        tool_call = tool_calls[0]
        function = self._get_response_field(tool_call, "function")
        name = self._get_response_field(function, "name") if function is not None else None
        if name != expected_name:
            raise _StructuredToolExtractionError("tool_call_name_mismatch")

        arguments = self._get_response_field(function, "arguments") if function is not None else None
        if isinstance(arguments, dict):
            payload = arguments
            raw_arguments_text = json.dumps(arguments, ensure_ascii=False, separators=(",", ":"))
        elif isinstance(arguments, str):
            if not arguments.strip():
                raise _StructuredToolExtractionError("tool_call_empty_arguments")
            raw_arguments_text = arguments
            try:
                payload = json.loads(arguments)
            except json.JSONDecodeError as exc:
                raise _StructuredToolExtractionError(
                    "tool_call_invalid_json",
                    draft_text=raw_arguments_text,
                ) from exc
        else:
            raise _StructuredToolExtractionError("tool_call_empty_arguments")

        if not isinstance(payload, dict) or not payload:
            raise _StructuredToolExtractionError(
                "tool_call_empty_arguments",
                draft_text=raw_arguments_text,
            )
        if payload.get("report_schema_version") != self.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION:
            raise _StructuredToolExtractionError(
                "tool_call_schema_version_mismatch",
                draft_text=raw_arguments_text,
            )
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    STRUCTURED_RETRY_DRAFT_MAX_CHARS = 65536

    @classmethod
    def _combine_structured_retry_drafts(
        cls,
        *,
        raw_tool_arguments: Optional[str] = None,
        assistant_text: Optional[str] = None,
    ) -> str:
        """Combine all visible first-stage artifacts without carrying hidden reasoning."""
        parts: List[str] = []
        raw = cls._sanitize_structured_retry_draft(raw_tool_arguments)
        visible = cls._sanitize_structured_retry_draft(assistant_text)
        if raw:
            parts.append("### rejected_tool_arguments\n" + raw)
        if visible and visible != raw:
            parts.append("### assistant_visible_draft\n" + visible)
        return cls._sanitize_structured_retry_draft("\n\n".join(parts))

    @classmethod
    def _sanitize_structured_retry_draft(cls, text: Optional[str]) -> str:
        """Keep only visible/auditable draft text for second-stage serialization."""
        if not text:
            return ""

        def _drop_reasoning_content(value: Any) -> Any:
            if isinstance(value, dict):
                return {
                    key: _drop_reasoning_content(item)
                    for key, item in value.items()
                    if str(key).lower() != "reasoning_content"
                }
            if isinstance(value, list):
                return [_drop_reasoning_content(item) for item in value]
            return value

        stripped = str(text).strip()
        try:
            parsed = json.loads(stripped)
            stripped = json.dumps(
                _drop_reasoning_content(parsed),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        except Exception:
            stripped = "\n".join(
                line for line in stripped.splitlines()
                if "reasoning_content" not in line.lower()
            )
        return redact_diagnostic_text(stripped, limit=cls.STRUCTURED_RETRY_DRAFT_MAX_CHARS).strip()

    @classmethod
    def _load_json_object_from_untrusted_draft(cls, text: Optional[str]) -> Optional[Dict[str, Any]]:
        draft = cls._sanitize_structured_retry_draft(text)
        if not draft:
            return None
        candidates = []
        for marker in ("### rejected_tool_arguments", "### assistant_visible_draft"):
            if marker in draft:
                section = draft.split(marker, 1)[1]
                next_marker = section.find("\n### ")
                if next_marker >= 0:
                    section = section[:next_marker]
                candidates.append(section.strip())
        candidates.append(draft)
        if "{" in draft and "}" in draft:
            candidates.append(draft[draft.find("{"): draft.rfind("}") + 1])
        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
            except Exception:
                try:
                    parsed = json.loads(repair_json(candidate))
                except Exception:
                    continue
            if isinstance(parsed, dict):
                return parsed
        return None

    @staticmethod
    def _extract_path_value(data: Dict[str, Any], path: str) -> Any:
        current: Any = data
        for part in path.split("."):
            if not isinstance(current, dict) or part not in current:
                return None
            current = current[part]
        return current

    @classmethod
    def _build_structured_retry_decision_lock(cls, draft_text: Optional[str]) -> Dict[str, Any]:
        """Extract machine-comparable core decisions from a JSON-like first-stage draft."""
        data = cls._load_json_object_from_untrusted_draft(draft_text)
        if not data:
            return {}
        paths = (
            "raw_score",
            "adjusted_score",
            "sentiment_score",
            "trend_prediction",
            "operation_advice",
            "decision_type",
            "action",
            "position_actions.empty.action",
            "position_actions.empty.advice",
            "position_actions.empty.stop_loss",
            "position_actions.empty.target_price",
            "position_actions.holding.action",
            "position_actions.holding.advice",
            "position_actions.holding.stop_loss",
            "position_actions.holding.target_price",
            "dashboard.battle_plan.sniper_points.ideal_buy",
            "dashboard.battle_plan.sniper_points.secondary_buy",
            "dashboard.battle_plan.sniper_points.stop_loss",
            "dashboard.battle_plan.sniper_points.take_profit",
            "dashboard.battle_plan.sniper_points.first_take_profit",
            "dashboard.battle_plan.sniper_points.final_target",
            "dashboard.battle_plan.sniper_points.entry_legs",
            "dashboard.battle_plan.sniper_points.exit_legs",
            "dashboard.phase_decision.action_window",
            "dashboard.phase_decision.immediate_action",
            "dashboard.phase_decision.watch_conditions",
            "dashboard.phase_decision.next_check_time",
        )
        lock: Dict[str, Any] = {}
        for path in paths:
            value = cls._extract_path_value(data, path)
            if value not in (None, "", [], {}):
                lock[path] = value
        return lock

    @staticmethod
    def _numeric_value(value: Any) -> Optional[float]:
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            return float(value)
        if isinstance(value, str):
            match = re.search(r"-?\d+(?:\.\d+)?", value.replace(",", ""))
            if match:
                try:
                    return float(match.group(0))
                except ValueError:
                    return None
        return None

    @staticmethod
    def _path_uses_numeric_lock(path: str) -> bool:
        return (
            path in {"raw_score", "adjusted_score", "sentiment_score"}
            or path.endswith(".stop_loss")
            or path.endswith(".target_price")
            or path.startswith("dashboard.battle_plan.sniper_points.")
            and not path.endswith(".entry_legs")
            and not path.endswith(".exit_legs")
        )

    @classmethod
    def _normalize_decision_lock_value(cls, value: Any, *, path: str = "") -> Any:
        if cls._path_uses_numeric_lock(path):
            numeric = cls._numeric_value(value)
            if numeric is not None:
                return ("number", round(numeric, 6))
        if isinstance(value, str):
            return ("text", re.sub(r"\s+", " ", value).strip())
        if isinstance(value, list):
            return (
                "list",
                [cls._normalize_decision_lock_value(item, path=f"{path}[]") for item in value],
            )
        if isinstance(value, dict):
            return (
                "dict",
                {
                    str(key): cls._normalize_decision_lock_value(value[key], path=f"{path}.{key}" if path else str(key))
                    for key in sorted(value)
                },
            )
        return value

    @classmethod
    def _validate_structured_retry_decision_lock(
        cls,
        response_text: str,
        decision_lock: Optional[Dict[str, Any]],
    ) -> None:
        """Fail closed if second-stage tool args silently change locked decisions."""
        if not decision_lock:
            return
        data = cls._load_json_object_from_untrusted_draft(response_text)
        if not data:
            raise ValueError("structured_retry_core_drift: final response is not JSON")
        violations: List[str] = []
        for path, expected in decision_lock.items():
            actual = cls._extract_path_value(data, path)
            if actual in (None, "", [], {}):
                violations.append(f"{path}: missing")
                continue
            if cls._normalize_decision_lock_value(actual, path=path) != cls._normalize_decision_lock_value(expected, path=path):
                violations.append(f"{path}: {expected!r} -> {actual!r}")
        if violations:
            preview = "; ".join(violations[:5])
            raise ValueError(f"structured_retry_core_drift: {preview}")

    @staticmethod
    def _litellm_tool_choice_for_model(
        model: str,
        tool_name: str,
        *,
        force_deepseek_non_thinking: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Return provider-aware tool choice without weakening receive-side validation."""
        provider = (model.split("/", 1)[0] if "/" in model else model).strip().lower()
        if provider == "deepseek" and not force_deepseek_non_thinking:
            # DeepSeek thinking routes reject named tool_choice.  We still send
            # exactly one tool and fail closed unless the response contains one
            # matching tool call.
            return None
        return {
            "type": "function",
            "function": {"name": tool_name},
        }

    def _extract_stream_text(self, chunk: Any) -> str:
        """Extract provider-agnostic text delta from a LiteLLM streaming chunk."""
        choices = chunk.get("choices") if isinstance(chunk, dict) else getattr(chunk, "choices", None)
        if not choices:
            return ""

        choice = choices[0]
        delta = choice.get("delta") if isinstance(choice, dict) else getattr(choice, "delta", None)
        message = choice.get("message") if isinstance(choice, dict) else getattr(choice, "message", None)

        content: Any = None
        if isinstance(delta, dict):
            content = delta.get("content")
        elif isinstance(delta, str):
            content = delta
        elif delta is not None:
            content = getattr(delta, "content", None)

        if content is None:
            if isinstance(message, dict):
                content = message.get("content")
            elif message is not None:
                content = getattr(message, "content", None)

        if isinstance(content, list):
            parts: List[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    text = item.get("text")
                    if isinstance(text, str):
                        parts.append(text)
            return "".join(parts)

        return content if isinstance(content, str) else ""

    def _consume_litellm_stream(
        self,
        stream_response: Any,
        *,
        model: str,
        usage_model: Optional[str] = None,
        provider: Optional[str] = None,
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """Consume a LiteLLM stream into a single text payload."""
        chunks: List[str] = []
        usage: Dict[str, Any] = {}
        chars_received = 0
        next_emit_at = 1

        try:
            for chunk in stream_response:
                chunk_usage = extract_usage_payload(chunk)
                normalized_usage = self._normalize_usage(
                    chunk_usage,
                    model=usage_model or model,
                    provider=provider,
                )
                if normalized_usage:
                    usage = normalized_usage

                delta_text = self._extract_stream_text(chunk)
                if not delta_text:
                    continue

                chunks.append(delta_text)
                chars_received += len(delta_text)
                if progress_callback and chars_received >= next_emit_at:
                    progress_callback(chars_received)
                    next_emit_at = chars_received + 160
        except Exception as exc:
            raise _LiteLLMStreamError(
                f"{model} stream interrupted: {exc}",
                partial_received=chars_received > 0,
            ) from exc

        response_text = "".join(chunks).strip()
        if not response_text:
            raise _LiteLLMStreamError(
                f"{model} stream returned empty response",
                partial_received=False,
            )

        if progress_callback and chars_received > 0:
            progress_callback(chars_received)

        return response_text, usage

    def _get_generation_backend(self, backend_id: Optional[str] = None) -> GenerationBackend:
        """Return the configured generation backend."""
        config = self._get_runtime_config()
        resolved_backend_id = backend_id or self._resolve_generation_backend_config()[0]
        return create_generation_backend(
            resolved_backend_id,
            config=config,
            litellm_completion_callable=self._call_litellm_impl,
        )

    def _call_litellm(
        self,
        prompt: str,
        generation_config: dict,
        *,
        system_prompt: Optional[str] = None,
        stream: bool = False,
        stream_progress_callback: Optional[Callable[[int], None]] = None,
        response_validator: Optional[Callable[[str], None]] = None,
        audit_context: Optional[Dict[str, Any]] = None,
        structured_tool: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, str, Dict[str, Any]]:
        """Compatibility wrapper around the configured generation backend."""
        preflight_error = self.get_generation_backend_config_error()
        if preflight_error is not None and not self._can_use_generation_fallback(preflight_error):
            raise preflight_error
        backend_id, fallback_backend_id = self._resolve_generation_backend_config()
        try:
            result = self._get_generation_backend(backend_id).generate(
                prompt,
                generation_config,
                system_prompt=system_prompt,
                stream=stream,
                stream_progress_callback=stream_progress_callback,
                response_validator=response_validator,
                audit_context=audit_context,
                structured_tool=structured_tool,
            )
        except GenerationError as exc:
            if not exc.fallbackable or not fallback_backend_id:
                raise
            try:
                fallback_backend = self._get_generation_backend(fallback_backend_id)
            except GenerationError as fallback_exc:
                raise GenerationError(
                    error_code=fallback_exc.error_code,
                    stage="fallback",
                    retryable=False,
                    fallbackable=False,
                    backend=fallback_backend_id,
                    provider=fallback_exc.provider,
                    details={
                        "primary_error": {
                            "error_code": exc.error_code.value,
                            "backend": exc.backend,
                            "provider": exc.provider,
                            "stage": exc.stage,
                            "details": exc.details,
                        },
                        "fallback_error": fallback_exc.details,
                    },
                ) from fallback_exc
            try:
                result = fallback_backend.generate(
                    prompt,
                    generation_config,
                    system_prompt=system_prompt,
                    stream=stream,
                    stream_progress_callback=stream_progress_callback,
                    response_validator=response_validator,
                    audit_context=audit_context,
                    structured_tool=structured_tool,
                )
            except _AllModelsFailedError:
                raise
            except GenerationError as fallback_exc:
                raise GenerationError(
                    error_code=fallback_exc.error_code,
                    stage="fallback",
                    retryable=False,
                    fallbackable=False,
                    backend=fallback_backend_id,
                    provider=fallback_exc.provider,
                    details={
                        "reason": "fallback_backend_failed",
                        "primary_error": {
                            "error_code": exc.error_code.value,
                            "backend": exc.backend,
                            "provider": exc.provider,
                            "stage": exc.stage,
                            "details": exc.details,
                        },
                        "fallback_error": {
                            "error_code": fallback_exc.error_code.value,
                            "backend": fallback_exc.backend,
                            "provider": fallback_exc.provider,
                            "stage": fallback_exc.stage,
                            "details": fallback_exc.details,
                        },
                    },
                ) from fallback_exc
            except Exception as fallback_exc:
                raise GenerationError(
                    error_code=GenerationErrorCode.UNKNOWN_BACKEND_ERROR,
                    stage="fallback",
                    retryable=False,
                    fallbackable=False,
                    backend=fallback_backend_id,
                    provider=fallback_backend_id,
                    details={
                        "reason": "fallback_backend_failed",
                        "primary_error": {
                            "error_code": exc.error_code.value,
                            "backend": exc.backend,
                            "provider": exc.provider,
                            "stage": exc.stage,
                            "details": exc.details,
                        },
                        "fallback_error": str(fallback_exc),
                    },
                ) from fallback_exc
        return result.text, result.model, result.usage

    def _call_official_deepseek_structured_report(
        self,
        prompt: str,
        generation_config: dict,
        *,
        route: Any,
        system_prompt: Optional[str],
        response_validator: Optional[Callable[[str], None]],
        audit_context: Optional[Dict[str, Any]],
        structured_tool: Dict[str, Any],
    ) -> Tuple[str, str, Dict[str, Any]]:
        """Generate one DeepSeek report through official Responses JSON Schema."""

        # The official DeepSeek ceiling covers reasoning plus visible output.
        # Use it from the first request instead of staging smaller budgets that
        # can truncate an otherwise valid structured report.
        max_tokens = DEEPSEEK_REPORT_MAX_OUTPUT_TOKENS
        requested_temperature = generation_config.get("temperature", 0.7)
        requested_timeout = generation_config.get("timeout")
        timeout = float(requested_timeout) if requested_timeout not in (None, "") else None
        effective_system_prompt = system_prompt or self.TEXT_SYSTEM_PROMPT
        messages = [
            {"role": "system", "content": effective_system_prompt},
            {"role": "user", "content": prompt},
        ]
        schema = dict(structured_tool.get("function", {}).get("parameters") or {})
        if not schema:
            raise ValueError("structured tool is missing function.parameters")

        def _attach_usage(
            usage: Dict[str, Any],
            *,
            transport: str,
            used_messages: List[Dict[str, Any]],
        ) -> Dict[str, Any]:
            if audit_context is None:
                normalized = filter_prompt_cache_telemetry(
                    attach_message_hmacs(usage, used_messages),
                    self._get_runtime_config(),
                )
            else:
                effective_audit_context = dict(audit_context)
                effective_audit_context["provider"] = "deepseek"
                effective_audit_context["transport"] = transport
                normalized = filter_prompt_cache_telemetry(
                    attach_legacy_message_stability_audit(
                        usage,
                        used_messages,
                        effective_audit_context,
                    ),
                    self._get_runtime_config(),
                )
            normalized["provider"] = "deepseek"
            normalized["transport"] = transport
            return normalized

        def _validate_or_fail(
            text: str,
            usage: Dict[str, Any],
            *,
            stage: str,
        ) -> Tuple[str, str, Dict[str, Any]]:
            try:
                if response_validator is not None:
                    response_validator(text)
            except Exception as exc:
                raise _AllModelsFailedError(
                    (
                        "Official DeepSeek structured output failed validation "
                        f"at {stage}: {self._sanitize_litellm_exception_text(exc, model=route.route_model)}"
                    ),
                    last_response_text=text,
                    last_model=route.route_model,
                    last_usage=usage,
                    last_error_kind="deepseek_structured_output",
                ) from exc
            return text, route.route_model, usage

        client = OfficialDeepSeekStructuredClient(route)
        try:
            response = client.create_structured_report(
                messages=messages,
                schema=schema,
                max_output_tokens=int(max_tokens),
                temperature=float(requested_temperature),
                timeout=timeout,
            )
            content = extract_responses_text(response)
            diagnostics = responses_diagnostics(response, output_chars=len(content))
            logger.info(
                "[DeepSeek Responses] completion status=%s reason=%s input_tokens=%s "
                "output_tokens=%s reasoning_tokens=%s output_chars=%s budget=%s",
                diagnostics["status"],
                diagnostics["incomplete_reason"],
                diagnostics["input_tokens"],
                diagnostics["output_tokens"],
                diagnostics["reasoning_tokens"],
                diagnostics["output_chars"],
                max_tokens,
            )
            usage = _attach_usage(
                normalize_deepseek_usage(response, model=route.wire_model, messages=messages),
                transport="deepseek_responses",
                used_messages=messages,
            )
            response_status = diagnostics["status"]
            incomplete_reason = diagnostics["incomplete_reason"]
            if response_status == "incomplete":
                if incomplete_reason == "content_filter":
                    raise _AllModelsFailedError(
                        "Official DeepSeek structured output was blocked by content_filter",
                        last_model=route.route_model,
                        last_usage=usage,
                        last_error_kind="content_filter",
                    )
                if incomplete_reason == "max_output_tokens":
                    raise DeepSeekStructuredGenerationError("incomplete:max_output_tokens")
                raise _AllModelsFailedError(
                    (
                        "Official DeepSeek structured output was incomplete for an unsupported reason: "
                        f"{incomplete_reason or 'unknown'}"
                    ),
                    last_model=route.route_model,
                    last_usage=usage,
                    last_error_kind="transport_provider",
                )
            if response_status == "failed":
                raise DeepSeekStructuredGenerationError(
                    f"failed:{diagnostics['error_code'] or 'unknown'}"
                )
            if response_status not in (None, "completed"):
                raise DeepSeekStructuredGenerationError(f"unexpected_status:{response_status}")
            if not content:
                raise DeepSeekStructuredGenerationError("empty_response")
            return _validate_or_fail(content, usage, stage="responses")
        except _AllModelsFailedError:
            raise
        except Exception as primary_exc:
            logger.warning(
                "[DeepSeek Responses] %s unavailable or empty; retrying same model once with Chat JSON fallback: %s",
                route.route_model,
                self._sanitize_litellm_exception_text(primary_exc, model=route.route_model),
            )

        fallback_messages = [
            messages[0],
            {"role": "user", "content": ensure_json_instruction(prompt)},
        ]
        fallback_max_tokens = DEEPSEEK_REPORT_MAX_OUTPUT_TOKENS
        try:
            fallback_client = OfficialDeepSeekStructuredClient(route)
            response = fallback_client.create_json_fallback(
                messages=fallback_messages,
                max_tokens=int(fallback_max_tokens),
                temperature=float(requested_temperature),
                timeout=timeout,
            )
            content = extract_chat_completion_text(response)
            finish_reason = chat_completion_finish_reason(response)
            logger.info(
                "[DeepSeek Chat JSON fallback] finish_reason=%s output_chars=%s budget=%s",
                finish_reason,
                len(content),
                fallback_max_tokens,
            )
            usage = _attach_usage(
                normalize_deepseek_usage(response, model=route.wire_model, messages=fallback_messages),
                transport="deepseek_chat_json_fallback",
                used_messages=fallback_messages,
            )
            if finish_reason in {"length", "content_filter", "insufficient_system_resource"}:
                raise _AllModelsFailedError(
                    f"Official DeepSeek Chat JSON fallback did not complete: {finish_reason}",
                    last_model=route.route_model,
                    last_usage=usage,
                    last_error_kind=("content_filter" if finish_reason == "content_filter" else "transport_provider"),
                )
            if not content:
                raise DeepSeekStructuredGenerationError("empty_response")
            return _validate_or_fail(content, usage, stage="chat_json_fallback")
        except _AllModelsFailedError:
            raise
        except Exception as fallback_exc:
            raise _AllModelsFailedError(
                (
                    "Official DeepSeek structured output failed; same-model Chat JSON fallback also failed. "
                    f"Last error: {self._sanitize_litellm_exception_text(fallback_exc, model=route.route_model)}"
                ),
                last_model=route.route_model,
                last_error_kind="transport_provider",
            ) from fallback_exc

    def _call_litellm_impl(
        self,
        prompt: str,
        generation_config: dict,
        *,
        system_prompt: Optional[str] = None,
        stream: bool = False,
        stream_progress_callback: Optional[Callable[[int], None]] = None,
        response_validator: Optional[Callable[[str], None]] = None,
        audit_context: Optional[Dict[str, Any]] = None,
        structured_tool: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, str, Dict[str, Any]]:
        """Call LLM via litellm with fallback across configured models.

        When channels/YAML are configured, every model goes through the Router
        (which handles per-model key selection, load balancing, and retries).
        In legacy mode, the primary model may use the Router while fallback
        models fall back to direct litellm.completion().

        Args:
            prompt: User prompt text.
            generation_config: Dict with optional keys: temperature, max_output_tokens, max_tokens.
            response_validator: Optional callable that accepts the raw response text and raises
                an exception if the response is unacceptable (e.g. not valid JSON).  When it
                raises, the current model is treated as failed and the next fallback model is
                tried.  If all models fail validation, :class:`_AllModelsFailedError` is raised
                with ``last_response_text`` set to the last raw response received.

        Returns:
            Tuple of (response text, model_used, usage). On success model_used is the full model
            name and usage is a dict with prompt_tokens, completion_tokens, total_tokens.
        """
        config = self._get_runtime_config()
        max_tokens = (
            generation_config.get('max_output_tokens')
            or generation_config.get('max_tokens')
            or 8192
        )
        requested_temperature = generation_config.get('temperature', 0.7)
        requested_timeout = generation_config.get("timeout")
        structured_resubmission = bool(generation_config.get("_structured_resubmission"))
        structured_resubmission_model = str(generation_config.get("_structured_resubmission_model") or "").strip()
        structured_retry_original_prompt = str(generation_config.get("_structured_retry_original_prompt") or "")
        structured_retry_draft = self._sanitize_structured_retry_draft(
            generation_config.get("_structured_retry_previous_response")
        )
        structured_retry_decision_lock = generation_config.get("_structured_retry_decision_lock") or {}

        if structured_resubmission and structured_resubmission_model:
            models_to_try = [structured_resubmission_model]
        else:
            models_to_try = [config.litellm_model] + (config.litellm_fallback_models or [])
        models_to_try = [m for m in models_to_try if m]

        use_channel_router = self._has_channel_config(config)

        if structured_tool is not None and models_to_try:
            deepseek_route = resolve_official_deepseek_structured_route(models_to_try[0], config)
            if deepseek_route is not None:
                if len(models_to_try) > 1:
                    logger.info(
                        "[DeepSeek Responses] using provider-native structured output for %s; "
                        "configured fallback models are not used for this selected-model report request",
                        deepseek_route.route_model,
                    )
                return self._call_official_deepseek_structured_report(
                    prompt,
                    generation_config,
                    route=deepseek_route,
                    system_prompt=system_prompt,
                    response_validator=response_validator,
                    audit_context=audit_context,
                    structured_tool=structured_tool,
                )

        last_error = None
        last_response_text: Optional[str] = None
        last_model: Optional[str] = None
        last_usage: Dict[str, Any] = {}
        last_error_kind: Optional[str] = None
        effective_system_prompt = system_prompt or self.TEXT_SYSTEM_PROMPT
        router_model_names = set(get_configured_llm_models(config.llm_model_list))

        def _validate_response_text(response_text: str) -> str:
            if response_validator is None:
                return response_text
            try:
                response_validator(response_text)
                return response_text
            except Exception:
                normalized = self._normalize_single_fenced_json_response(response_text)
                if normalized is None:
                    raise
                response_validator(normalized)
                logger.warning(
                    "[LiteLLM] normalized one validated JSON fence and discarded non-JSON envelope text"
                )
                return normalized

        for model in models_to_try:
            origins = route_deployment_origins(config.llm_model_list, model)
            model_stream = bool(stream and structured_tool is None and not origins.has_hermes)
            recovery_model_list = config.llm_model_list
            legacy_router_model_list = getattr(self, "_legacy_router_model_list", None) or []
            if legacy_router_model_list and model == config.litellm_model and not use_channel_router:
                recovery_model_list = legacy_router_model_list
            usage_model, usage_provider = resolved_model_provider_identity(model, recovery_model_list)

            try:
                def _attach_usage_audit(
                    usage: Dict[str, Any],
                    messages: List[Dict[str, Any]],
                ) -> Dict[str, Any]:
                    if audit_context is None:
                        return filter_prompt_cache_telemetry(
                            attach_message_hmacs(usage, messages),
                            config,
                        )
                    effective_audit_context = dict(audit_context)
                    effective_audit_context["provider"] = usage_provider
                    effective_audit_context["transport"] = (
                        effective_audit_context.get("transport") or "litellm"
                    )
                    return filter_prompt_cache_telemetry(
                        attach_legacy_message_stability_audit(
                            usage,
                            messages,
                            effective_audit_context,
                        ),
                        config,
                    )

                model_short = model.split("/")[-1] if "/" in model else model
                provider_name = (model.split("/", 1)[0] if "/" in model else "").strip().lower()
                is_deepseek_route = provider_name == "deepseek" or usage_provider == "deepseek"
                if structured_resubmission and is_deepseek_route:
                    # DeepSeek V4 thinking mode cannot reliably accept a forced
                    # named tool.  Keep the first analytical pass untouched,
                    # then use the same model in non-thinking mode only as a
                    # deterministic Schema submission pass.
                    extra = {"thinking": {"type": "disabled"}}
                else:
                    extra = get_thinking_extra_body(model_short)
                if structured_resubmission and structured_retry_original_prompt:
                    messages = [
                        {"role": "system", "content": effective_system_prompt},
                        {"role": "user", "content": structured_retry_original_prompt},
                    ]
                    if structured_retry_draft:
                        messages.append(
                            {
                                "role": "assistant",
                                "content": (
                                    "上一阶段可见分析草稿（不含隐藏 reasoning_content，尚未验收；"
                                    "第二阶段必须忠实结构化，不得静默重判核心结论）：\n"
                                    f"{structured_retry_draft}"
                                ),
                            }
                        )
                    messages.append({"role": "user", "content": prompt})
                else:
                    messages = [
                        {"role": "system", "content": effective_system_prompt},
                        {"role": "user", "content": prompt},
                    ]
                call_kwargs: Dict[str, Any] = {
                    "model": model,
                    "messages": messages,
                    "max_tokens": max_tokens,
                }
                if structured_tool is not None:
                    tool_name = str(structured_tool.get("function", {}).get("name") or "").strip()
                    if not tool_name:
                        raise ValueError("structured tool is missing function.name")
                    call_kwargs["tools"] = [structured_tool]
                    tool_choice = self._litellm_tool_choice_for_model(
                        model,
                        tool_name,
                        force_deepseek_non_thinking=(
                            structured_resubmission and is_deepseek_route
                        ),
                    )
                    if tool_choice is not None:
                        call_kwargs["tool_choice"] = tool_choice
                    call_kwargs["parallel_tool_calls"] = False
                elif response_validator is not None:
                    # Ask compatible providers/gateways to enforce a JSON-only
                    # response on the wire.  Unsupported endpoints are handled
                    # by the request-scoped parameter recovery below.
                    call_kwargs["response_format"] = {"type": "json_object"}
                if requested_timeout not in (None, ""):
                    call_kwargs["timeout"] = requested_timeout
                if extra:
                    call_kwargs["extra_body"] = extra
                uses_router = (
                    (use_channel_router and self._router and model in router_model_names)
                    or (self._router and model == config.litellm_model and not use_channel_router)
                )
                if not uses_router:
                    try:
                        keys = get_api_keys_for_model(model, config)
                    except AttributeError:
                        keys = []
                    if keys:
                        call_kwargs["api_key"] = keys[0]
                    try:
                        call_kwargs.update(extra_litellm_params(model, config))
                    except AttributeError:
                        pass
                call_kwargs = apply_litellm_generation_params(
                    call_kwargs,
                    model,
                    requested_temperature,
                    model_list=recovery_model_list,
                )
                route_context = build_provider_cache_route_context(
                    model=model,
                    provider=usage_provider,
                    call_kwargs=call_kwargs,
                    model_list=recovery_model_list,
                    call_type="analysis",
                )
                hint_result = apply_prompt_cache_hints(call_kwargs, route_context, config)
                call_kwargs = hint_result.call_kwargs
                if requested_timeout not in (None, ""):
                    call_kwargs["timeout"] = requested_timeout
                if hint_result.diagnostics:
                    logger.debug("[PromptCache] %s", hint_result.diagnostics)

                _stream_text: Optional[str] = None
                _stream_usage: Dict[str, Any] = {}

                if model_stream:
                    try:
                        stream_response = call_litellm_with_param_recovery(
                            lambda kwargs: self._dispatch_litellm_completion(
                                model,
                                kwargs,
                                config=config,
                                use_channel_router=use_channel_router,
                                router_model_names=router_model_names,
                            ),
                            model=model,
                            call_kwargs={**call_kwargs, "stream": True},
                            model_list=recovery_model_list,
                            cache_recovery=False,
                            logger=logger,
                        )
                        _stream_text, _stream_usage = self._consume_litellm_stream(
                            stream_response,
                            model=model,
                            usage_model=usage_model,
                            provider=usage_provider,
                            progress_callback=stream_progress_callback,
                        )
                    except _LiteLLMStreamError as exc:
                        safe_error = self._sanitize_litellm_exception_text(exc, config=config, model=model)
                        if exc.partial_received:
                            logger.warning(
                                "[LiteLLM] %s stream failed after partial output, retrying non-stream for same model: %s",
                                model,
                                safe_error,
                            )
                        else:
                            logger.warning(
                                "[LiteLLM] %s stream unavailable before first chunk, falling back to non-stream: %s",
                                model,
                                safe_error,
                            )
                        last_error = RuntimeError(f"{type(exc).__name__}: {safe_error}")
                    except Exception as exc:
                        safe_error = self._sanitize_litellm_exception_text(exc, config=config, model=model)
                        logger.warning(
                            "[LiteLLM] %s stream request failed before first chunk, falling back to non-stream: %s",
                            model,
                            safe_error,
                        )

                if _stream_text is not None:
                    last_response_text = _stream_text
                    last_model = model
                    _stream_usage = _attach_usage_audit(_stream_usage, call_kwargs["messages"])
                    last_usage = _stream_usage
                    validated_stream_text = _validate_response_text(_stream_text)
                    return validated_stream_text, model, _stream_usage

                response = call_litellm_with_param_recovery(
                    lambda kwargs: self._dispatch_litellm_completion(
                        model,
                        kwargs,
                        config=config,
                        use_channel_router=use_channel_router,
                        router_model_names=router_model_names,
                    ),
                    model=model,
                    call_kwargs=call_kwargs,
                    model_list=recovery_model_list,
                    logger=logger,
                )

                if structured_tool is not None:
                    tool_name = str(structured_tool.get("function", {}).get("name") or "").strip()
                    try:
                        content = self._extract_required_tool_arguments(response, tool_name)
                    except Exception as tool_exc:
                        assistant_text = self._extract_completion_text(response)
                        draft_text = self._combine_structured_retry_drafts(
                            raw_tool_arguments=getattr(tool_exc, "draft_text", None),
                            assistant_text=assistant_text,
                        )
                        if draft_text:
                            setattr(tool_exc, "handoff_draft_text", draft_text)
                            usage_messages = None if audit_context is not None else call_kwargs["messages"]
                            usage = self._normalize_usage(
                                extract_usage_payload(response),
                                model=usage_model or model,
                                provider=usage_provider,
                                messages=usage_messages,
                            )
                            if audit_context is not None:
                                usage = _attach_usage_audit(usage, call_kwargs["messages"])
                            last_response_text = draft_text
                            last_model = model
                            last_usage = usage
                            logger.warning(
                                "[LLM Tool Schema] %s returned an unaccepted visible/tool draft without a valid %s call; "
                                "keeping it only as same-model resubmission context",
                                model,
                                tool_name,
                            )
                        raise
                else:
                    content = self._extract_completion_text(response)
                if content:
                    usage_messages = None if audit_context is not None else call_kwargs["messages"]
                    usage = self._normalize_usage(
                        extract_usage_payload(response),
                        model=usage_model or model,
                        provider=usage_provider,
                        messages=usage_messages,
                    )
                    if audit_context is not None:
                        usage = _attach_usage_audit(usage, call_kwargs["messages"])
                    last_response_text = content
                    last_model = model
                    last_usage = usage
                    if structured_resubmission and structured_retry_decision_lock:
                        self._validate_structured_retry_decision_lock(
                            content,
                            structured_retry_decision_lock,
                        )
                    validated_content = _validate_response_text(content)
                    return (validated_content, model, usage)
                raise ValueError("LLM returned empty response")

            except Exception as e:
                safe_error = self._sanitize_litellm_exception_text(e, config=config, model=model)
                logger.warning("[LiteLLM] %s failed: %s", model, safe_error)
                last_error = RuntimeError(f"{type(e).__name__}: {safe_error}")
                # Classify this terminal attempt directly. ``last_response_text``
                # may belong to an earlier model and must not affect retry type.
                last_error_kind = _classify_litellm_failure(e)
                if last_error_kind == "structured_submission" and structured_tool is not None:
                    # Bind a structured retry to the terminal model that failed
                    # the native-tool contract.  Do not accidentally carry a
                    # previous model's draft across fallback boundaries.
                    last_model = model
                    if not getattr(e, "handoff_draft_text", None) and not getattr(e, "draft_text", None):
                        last_response_text = None
                        last_usage = {}
                continue

        raise _AllModelsFailedError(
            f"All LLM models failed (tried {len(models_to_try)} model(s)). Last error: {last_error}",
            last_response_text=last_response_text,
            last_model=last_model,
            last_usage=last_usage,
            last_error_kind=last_error_kind,
        )

    def generate_text(
        self,
        prompt: str,
        max_tokens: int = 2048,
        temperature: float = 0.7,
    ) -> Optional[str]:
        """Public entry point for free-form text generation.

        External callers (e.g. MarketAnalyzer) must use this method instead of
        calling _call_litellm() directly or accessing private attributes such as
        _litellm_available, _router, _model, _use_openai, or _use_anthropic.

        Args:
            prompt:      Text prompt to send to the LLM.
            max_tokens:  Maximum tokens in the response (default 2048).
            temperature: Sampling temperature (default 0.7).

        Returns:
            Response text, or None if the LLM call fails (error is logged).
        """
        try:
            result = self._call_litellm(
                prompt,
                generation_config={"max_tokens": max_tokens, "temperature": temperature},
            )
            if isinstance(result, tuple):
                text, model_used, usage = result
                if should_persist_usage_telemetry(usage):
                    persist_llm_usage(usage, model_used, call_type="market_review")
                return text
            return result
        except GenerationError:
            raise
        except Exception as exc:
            logger.error("[generate_text] LLM call failed: %s", exc)
            return None

    def analyze(
        self, 
        context: Dict[str, Any],
        news_context: Optional[str] = None,
        progress_callback: Optional[Callable[[int, str], None]] = None,
        stream_progress_callback: Optional[Callable[[int], None]] = None,
        analysis_context_pack_summary: Optional[str] = None,
    ) -> AnalysisResult:
        """
        分析单只股票
        
        流程：
        1. 格式化输入数据（技术面 + 新闻）
        2. 调用 Gemini API（带重试和模型切换）
        3. 解析 JSON 响应
        4. 返回结构化结果
        
        Args:
            context: 从 storage.get_analysis_context() 获取的上下文数据
            news_context: 预先搜索的新闻内容（可选）

        Returns:
            AnalysisResult 对象
        """
        def _emit_progress(progress: int, message: str) -> None:
            if progress_callback is None:
                return
            try:
                progress_callback(progress, message)
            except Exception as exc:
                logger.debug("[analyzer] progress callback skipped: %s", exc)

        code = context.get('code', 'Unknown')
        config = self._get_runtime_config()
        report_language = normalize_report_language(getattr(config, "report_language", "zh"))
        requested_backend = str(
            getattr(config, "generation_backend", LITELLM_BACKEND_ID)
            or LITELLM_BACKEND_ID
        ).strip().lower()
        attempted_model = getattr(config, "litellm_model", None) or "unknown"
        use_deepseek_responses_report = bool(
            requested_backend == LITELLM_BACKEND_ID
            and resolve_official_deepseek_structured_route(attempted_model, config) is not None
        )
        use_structured_report_tool = (
            requested_backend == LITELLM_BACKEND_ID
            and not use_deepseek_responses_report
        )
        system_prompt = self._get_analysis_system_prompt(
            report_language,
            stock_code=code,
            structured_tool_output=use_structured_report_tool,
            paper_trading_style_context=(
                context.get("paper_trading_style_context")
                if isinstance(context.get("paper_trading_style_context"), dict)
                else None
            ),
        )
        skill_instructions, default_skill_policy, use_legacy_default_prompt = self._get_skill_prompt_sections()
        
        # 请求前增加延时（防止连续请求触发限流）
        request_delay = config.gemini_request_delay
        if request_delay > 0:
            logger.debug(f"[LLM] 请求前等待 {request_delay:.1f} 秒...")
            _emit_progress(65, f"{code}：LLM 请求前等待 {request_delay:.1f} 秒")
            time.sleep(request_delay)
        
        # 优先从上下文获取股票名称（由 main.py 传入）
        name = context.get('stock_name')
        if not name or name.startswith('股票'):
            # 备选：从 realtime 中获取
            if 'realtime' in context and context['realtime'].get('name'):
                name = context['realtime']['name']
            else:
                # 最后从映射表获取
                name = STOCK_NAME_MAP.get(code, f'股票{code}')

        backend_error = self.get_generation_backend_config_error()
        if backend_error is not None and not self._can_use_generation_fallback(backend_error):
            details = backend_error.details or {}
            field = str(details.get("field") or "GENERATION_BACKEND")
            requested_backend = str(details.get("requested_backend") or backend_error.backend)
            reason = str(details.get("reason") or backend_error.error_code.value)
            if report_language == "en":
                summary = (
                    "AI analysis is unavailable because the generation backend "
                    f"cannot start: {backend_error.error_code.value}."
                )
                risk_warning = (
                    f"Check {field}={requested_backend} ({reason}) or set a valid "
                    "backend/fallback before retrying."
                )
            elif report_language == "ko":
                summary = (
                    "생성 백엔드를 시작할 수 없어 AI 분석을 사용할 수 없습니다: "
                    f"{backend_error.error_code.value}."
                )
                risk_warning = (
                    f"{field}={requested_backend} ({reason})를 확인하거나 유효한 "
                    "백엔드/폴백을 설정한 뒤 다시 시도하세요."
                )
            else:
                summary = (
                    "AI 分析功能不可用：生成后端无法启动，"
                    f"{backend_error.error_code.value}。"
                )
                risk_warning = (
                    f"请检查 {field}={requested_backend}（{reason}），"
                    "或配置有效后端/回退后重试。"
                )
            return AnalysisResult(
                code=code,
                name=name,
                sentiment_score=50,
                trend_prediction=localize_trend_prediction('震荡', report_language),
                operation_advice=_analysis_failed_label(report_language),
                decision_type="failed",
                action=None,
                action_label=None,
                confidence_level=localize_confidence_level('低', report_language),
                analysis_summary=summary,
                risk_warning=risk_warning,
                success=False,
                error_message=(
                    f"{backend_error.error_code.value}: {field}={requested_backend}"
                ),
                model_used=attempted_model,
                report_language=report_language,
            )

        # 如果模型不可用，返回默认结果
        if not self.is_available():
            return AnalysisResult(
                code=code,
                name=name,
                sentiment_score=50,
                trend_prediction=localize_trend_prediction('震荡', report_language),
                operation_advice=_analysis_failed_label(report_language),
                decision_type="failed",
                action=None,
                action_label=None,
                confidence_level=localize_confidence_level('低', report_language),
                analysis_summary=_localized_text(
                    report_language,
                    en='AI analysis is unavailable because no API key is configured.',
                    zh='AI 分析功能未启用（未配置 API Key）',
                    ko='API 키가 설정되지 않아 AI 분석을 사용할 수 없습니다.',
                ),
                risk_warning=_localized_text(
                    report_language,
                    en='Configure an LLM API key (GEMINI_API_KEY/ANTHROPIC_API_KEY/OPENAI_API_KEY) and retry.',
                    zh='请配置 LLM API Key（GEMINI_API_KEY/ANTHROPIC_API_KEY/OPENAI_API_KEY）后重试',
                    ko='LLM API 키(GEMINI_API_KEY/ANTHROPIC_API_KEY/OPENAI_API_KEY)를 설정한 뒤 다시 시도하세요.',
                ),
                success=False,
                error_message=_localized_text(
                    report_language,
                    en='LLM API key is not configured',
                    zh='LLM API Key 未配置',
                    ko='LLM API 키가 설정되지 않았습니다',
                ),
                model_used=attempted_model,
                report_language=report_language,
            )
        
        try:
            # 格式化输入（包含技术面数据和新闻）
            prompt = self._format_prompt(
                context,
                name,
                news_context,
                report_language=report_language,
                analysis_context_pack_summary=analysis_context_pack_summary,
            )
            legacy_audit_context = {
                "language": report_language,
                "market_group": _legacy_market_group(code),
                "analysis_mode": "stock_analysis",
                "legacy_prompt_mode": "legacy_default" if use_legacy_default_prompt else "skill_aware",
                "skill_config": {
                    "skill_instructions": skill_instructions,
                    "default_skill_policy": default_skill_policy,
                    "use_legacy_default_prompt": use_legacy_default_prompt,
                },
                "transport": "litellm",
                "dynamic_markers": _legacy_audit_marker_specs(
                    context,
                    code=code,
                    stock_name=name,
                    report_language=report_language,
                    news_context=news_context,
                    analysis_context_pack_summary=analysis_context_pack_summary,
                ),
            }
            
            backend_id, _fallback_backend_id = self._resolve_generation_backend_config()
            model_name = attempted_model
            if backend_id in LOCAL_CLI_GENERATION_BACKEND_IDS:
                model_name = backend_id
                legacy_audit_context["transport"] = backend_id
            logger.info(f"========== AI 分析 {name}({code}) ==========")
            logger.info(f"[LLM配置] 模型: {model_name}")
            logger.info(f"[LLM配置] Prompt 长度: {len(prompt)} 字符")
            logger.info(f"[LLM配置] 是否包含新闻: {'是' if news_context else '否'}")

            # 本地 CLI backend 是进程执行能力，不记录完整 prompt。
            if backend_id in LOCAL_CLI_GENERATION_BACKEND_IDS:
                prompt_preview = redact_diagnostic_text(prompt, limit=500)
            else:
                prompt_preview = prompt[:500] + "..." if len(prompt) > 500 else prompt
            logger.info(f"[LLM Prompt 预览]\n{prompt_preview}")
            if backend_id not in LOCAL_CLI_GENERATION_BACKEND_IDS:
                logger.debug(f"=== 完整 Prompt ({len(prompt)}字符) ===\n{prompt}\n=== End Prompt ===")

            # 设置生成配置
            generation_config = {
                "temperature": config.llm_temperature,
                "max_output_tokens": 8192,
            }

            logger.info(f"[LLM调用] 开始调用 {model_name}...")
            _emit_progress(68, f"{name}：LLM 已接收请求，等待响应")

            # 使用 litellm 调用（支持完整性校验重试）
            current_prompt = prompt
            structured_retry_state: Dict[str, Any] = {}
            retry_count = 0
            max_retries = config.report_integrity_retry if config.report_integrity_enabled else 0

            while True:
                start_time = time.time()
                try:
                    request_generation_config = dict(generation_config)
                    request_generation_config["_structured_resubmission"] = retry_count > 0
                    if retry_count > 0 and structured_retry_state:
                        request_generation_config.update(structured_retry_state)
                    response_text, model_used, llm_usage = self._call_litellm(
                        current_prompt,
                        request_generation_config,
                        system_prompt=system_prompt,
                        stream=True,
                        stream_progress_callback=stream_progress_callback,
                        response_validator=self._validate_json_response,
                        audit_context=legacy_audit_context,
                        structured_tool=(
                            self._analysis_report_tool()
                            if backend_id == LITELLM_BACKEND_ID
                            else None
                        ),
                    )
                except _AllModelsFailedError as exc:
                    if backend_id == LITELLM_BACKEND_ID:
                        if (
                            _looks_like_structured_submission_error(exc)
                            and retry_count < max_retries
                        ):
                            previous_response = exc.last_response_text or ""
                            decision_lock = self._build_structured_retry_decision_lock(
                                previous_response
                            )
                            retry_model = exc.last_model or model_name
                            current_prompt = self._build_structured_tool_retry_prompt(
                                prompt,
                                report_language=report_language,
                                previous_response=previous_response,
                                decision_lock=decision_lock,
                                include_base_prompt=False,
                            )
                            structured_retry_state = {
                                "_structured_resubmission_model": retry_model,
                                "_structured_retry_original_prompt": prompt,
                                "_structured_retry_previous_response": previous_response,
                                "_structured_retry_decision_lock": decision_lock,
                            }
                            retry_count += 1
                            logger.info(
                                "[LLM Tool Schema] structured submission rejected; retrying tool-only with same model %s (%d/%d, locked=%d)",
                                retry_model,
                                retry_count,
                                max_retries,
                                len(decision_lock),
                            )
                            _emit_progress(
                                min(99, 92 + retry_count * 2),
                                f"{name}：结构化报告未通过校验，正在重新提交（{retry_count}/{max_retries}）",
                            )
                            continue
                        if not _looks_like_structured_submission_error(exc):
                            logger.warning(
                                "[LLM Tool Schema] structured resubmission skipped because the last failure "
                                "looks like a transport/provider error rather than a schema submission error: %s",
                                self._sanitize_litellm_exception_text(exc, config=config, model=model_name),
                            )
                        raise
                    if exc.last_response_text is not None:
                        logger.warning(
                            "[LLM JSON] %s(%s): all models returned invalid JSON/schema, using parser fallback before integrity handling",
                            name,
                            code,
                        )
                        response_text = exc.last_response_text
                        model_used = exc.last_model or model_name
                        llm_usage = exc.last_usage or {}
                    else:
                        raise
                elapsed = time.time() - start_time

                # 记录响应信息
                logger.info(
                    f"[LLM返回] {model_name} 响应成功, 耗时 {elapsed:.2f}s, 响应长度 {len(response_text)} 字符"
                )
                if backend_id in LOCAL_CLI_GENERATION_BACKEND_IDS:
                    response_preview = redact_diagnostic_text(response_text, limit=300)
                else:
                    response_preview = response_text[:300] + "..." if len(response_text) > 300 else response_text
                logger.info(f"[LLM返回 预览]\n{response_preview}")
                if backend_id not in LOCAL_CLI_GENERATION_BACKEND_IDS:
                    logger.debug(
                        f"=== {model_name} 完整响应 ({len(response_text)}字符) ===\n{response_text}\n=== End Response ==="
                    )
                # Keep parser/retry progress monotonic so task progress/message never "goes backward".
                parse_progress = min(99, 93 + retry_count * 2)
                _emit_progress(parse_progress, f"{name}：LLM 返回完成，正在解析 JSON")

                # 解析响应
                result = self._parse_response(response_text, code, name)
                result.raw_response = response_text
                result.search_performed = bool(news_context)
                result.market_snapshot = self._build_market_snapshot(context)
                result.model_used = model_used
                result.report_language = report_language
                is_index_etf = bool(context.get("is_index_etf", False))
                normalize_chip_structure_availability(
                    result,
                    context.get("chip"),
                    is_index_etf=is_index_etf,
                )
                normalize_index_etf_checklist(result, is_index_etf=is_index_etf)

                # 内容完整性校验（可选）
                if not config.report_integrity_enabled:
                    break
                require_phase_decision = isinstance(context.get("market_phase_context"), dict)
                pass_integrity, missing_fields = self._check_content_integrity(
                    result,
                    require_phase_decision=require_phase_decision,
                )
                if pass_integrity:
                    break
                if retry_count < max_retries:
                    current_prompt = self._build_integrity_retry_prompt(
                        prompt,
                        response_text,
                        missing_fields,
                        report_language=report_language,
                    )
                    retry_count += 1
                    logger.info(
                        "[LLM完整性] 必填字段缺失 %s，第 %d 次补全重试",
                        missing_fields,
                        retry_count,
                    )
                    retry_progress = min(99, 92 + retry_count * 2)
                    _emit_progress(
                        retry_progress,
                        f"{name}：报告字段不完整，正在补全重试（{retry_count}/{max_retries}）",
                    )
                else:
                    execution_only_missing = bool(missing_fields) and all(
                        _is_execution_contract_field(field) for field in missing_fields
                    )
                    if execution_only_missing:
                        result.error_message = (
                            "analysis report generated, but trading plan skipped because "
                            "execution fields are incomplete after retry: "
                            + ",".join(missing_fields)
                        )
                        limitations = list(result.limitations or [])
                        limitations.append(
                            "交易执行字段不完整；报告仅用于阅读，不生成模拟交易计划"
                        )
                        result.limitations = list(dict.fromkeys(limitations))
                        logger.warning(
                            "[LLM完整性] 交易执行字段缺失 %s，保留报告但跳过交易计划",
                            missing_fields,
                        )
                    else:
                        self._apply_placeholder_fill(result, missing_fields)
                        result.success = False
                        result.error_message = (
                            "analysis report missing required fields after retry: "
                            + ",".join(missing_fields)
                        )
                        _mark_failed_result_non_actionable(result)
                        logger.warning(
                            "[LLM完整性] 核心必填字段缺失 %s，标记为失败尝试",
                            missing_fields,
                        )
                    break

            if should_persist_usage_telemetry(llm_usage):
                persist_llm_usage(llm_usage, model_used, call_type="analysis", stock_code=code)

            logger.info(f"[LLM解析] {name}({code}) 分析完成: {result.trend_prediction}, 评分 {result.sentiment_score}")

            return result
            
        except Exception as e:
            safe_error = self._sanitize_hermes_exception_text(e)
            logger.error("AI 分析 %s(%s) 失败: %s", name, code, safe_error)
            return AnalysisResult(
                code=code,
                name=name,
                sentiment_score=50,
                trend_prediction=localize_trend_prediction('震荡', report_language),
                operation_advice=_analysis_failed_label(report_language),
                decision_type="failed",
                action=None,
                action_label=None,
                confidence_level=localize_confidence_level('低', report_language),
                analysis_summary=_localized_text(
                    report_language,
                    en=f'Analysis failed: {safe_error[:100]}',
                    zh=f'分析过程出错: {safe_error[:100]}',
                    ko=f'분석 중 오류가 발생했습니다: {safe_error[:100]}',
                ),
                risk_warning=_localized_text(
                    report_language,
                    en='Analysis failed. Please retry later or review manually.',
                    zh='分析失败，请稍后重试或手动分析',
                    ko='분석에 실패했습니다. 잠시 후 다시 시도하거나 수동으로 검토하세요.',
                ),
                search_performed=bool(news_context),
                success=False,
                error_message=safe_error,
                model_used=attempted_model,
                report_language=report_language,
            )
    
    def _format_prompt(
        self, 
        context: Dict[str, Any], 
        name: str,
        news_context: Optional[str] = None,
        report_language: str = "zh",
        analysis_context_pack_summary: Optional[str] = None,
    ) -> str:
        """
        格式化分析提示词（决策仪表盘 v2.0）
        
        包含：技术指标、实时行情（量比/换手率）、筹码分布、趋势分析、新闻
        
        Args:
            context: 技术面数据上下文（包含增强数据）
            name: 股票名称（默认值，可能被上下文覆盖）
            news_context: 预先搜索的新闻内容
        """
        code = context.get('code', 'Unknown')
        report_language = normalize_report_language(report_language)
        _, _, use_legacy_default_prompt = self._get_skill_prompt_sections()
        paper_style_context = context.get("paper_trading_style_context")
        has_style_contract = isinstance(paper_style_context, dict) and bool(
            paper_style_context.get("trading_style")
            or paper_style_context.get("settings")
        )
        effective_legacy_default_prompt = use_legacy_default_prompt and not has_style_contract
        
        # 优先使用上下文中的股票名称（从 realtime_quote 获取）
        stock_name = context.get('stock_name', name)
        if not stock_name or stock_name == f'股票{code}':
            stock_name = STOCK_NAME_MAP.get(code, f'股票{code}')
            
        today = context.get('today', {})
        unknown_text = get_unknown_text(report_language)
        no_data_text = get_no_data_text(report_language)
        quote_section_title, close_price_label = _phase_aware_quote_labels(context)
        hide_regular_session_ohlc = _should_hide_regular_session_ohlc(context)
        realtime_overlay_quote = hide_regular_session_ohlc and _today_has_realtime_overlay(today)
        pct_chg_label = "实时涨跌幅" if realtime_overlay_quote else "涨跌幅"
        volume_label = "实时成交量" if realtime_overlay_quote else "成交量"
        amount_label = "实时成交额" if realtime_overlay_quote else "成交额"
        quote_rows = [
            f"| {close_price_label} | {today.get('close', 'N/A')} 元 |",
        ]
        if not hide_regular_session_ohlc:
            quote_rows.extend(
                [
                    f"| 开盘价 | {today.get('open', 'N/A')} 元 |",
                    f"| 最高价 | {today.get('high', 'N/A')} 元 |",
                    f"| 最低价 | {today.get('low', 'N/A')} 元 |",
                ]
            )
        quote_rows.extend(
            [
                f"| {pct_chg_label} | {today.get('pct_chg', 'N/A')}% |",
                f"| {volume_label} | {self._format_volume(today.get('volume'))} |",
                f"| {amount_label} | {self._format_amount(today.get('amount'))} |",
            ]
        )
        quote_rows_text = "\n".join(quote_rows)
        
        # ========== 构建决策仪表盘格式的输入 ==========
        prompt = f"""# 决策仪表盘分析请求

## 📊 股票基础信息
| 项目 | 数据 |
|------|------|
| 股票代码 | **{code}** |
| 股票名称 | **{stock_name}** |
| 分析日期 | {context.get('date', unknown_text)} |

---
"""
        portfolio_context = context.get("portfolio_context")
        if isinstance(portfolio_context, dict):
            holding_state = str(portfolio_context.get("holding_state") or "unknown").strip() or "unknown"
            quantity = portfolio_context.get("quantity")
            avg_cost = portfolio_context.get("avg_cost") or portfolio_context.get("average_cost")
            cash = portfolio_context.get("cash")
            exposure_pct = portfolio_context.get("total_exposure_pct")
            prompt += f"""
## 💼 当前持仓上下文（必须用于动作语义）
| 项目 | 数据 |
|------|------|
| 数据来源 | {portfolio_context.get('source', 'unknown')} |
| 当前标的持仓状态 | {holding_state} |
| 当前标的持仓数量 | {quantity if quantity is not None else 'N/A'} |
| 当前标的平均成本 | {avg_cost if avg_cost is not None else 'N/A'} |
| 可用现金 | {cash if cash is not None else 'N/A'} |
| 账户总暴露比例 | {exposure_pct if exposure_pct is not None else 'N/A'}% |
| 资产类别/识别来源 | {portfolio_context.get('asset_type', 'unknown')} / {portfolio_context.get('asset_type_source', 'unknown')} |
| 当前标的有效集中度上限 | {portfolio_context.get('asset_exposure_effective_cap_pct', 'N/A')}% |
| 当前标的剩余资金容量 | {portfolio_context.get('asset_exposure_remaining_capacity', 'N/A')} |
| 资金与风险预算（只读） | {portfolio_context.get('sizing_context', 'N/A')} |

> 动作契约：如果当前标的持仓状态为 `empty`，证据充分时可以 `buy`，但顶层 `action` 和 `position_actions.empty.action` 不得使用 `hold/reduce/sell`，也不得写“空仓不能买入”；中性结论请用 `watch`，风险结论请用 `avoid/alert`。如果当前标的持仓状态为 `holding`，必须在 `position_actions.holding` 中给出持仓风控建议，能可靠判断时给出 `stop_loss` 与 `target_price`。
"""
            strategy_thesis = portfolio_context.get("strategy_thesis")
            if isinstance(strategy_thesis, dict):
                prompt += f"""
### 上一版生效策略 thesis（只读上下文）
| 项目 | 数据 |
|------|------|
| 策略状态 | {strategy_thesis.get('state', 'N/A')} |
| 生效动作 | {strategy_thesis.get('effective_action', 'N/A')} |
| 最近原始动作 | {strategy_thesis.get('raw_action', 'N/A')} |
| 生效报告 ID | {strategy_thesis.get('accepted_report_id', 'N/A')} |
| 生效时间 | {strategy_thesis.get('accepted_at', 'N/A')} |
| 生效买入区间 | {strategy_thesis.get('entry_low', 'N/A')} - {strategy_thesis.get('entry_high', 'N/A')} |
| 生效止损/目标 | {strategy_thesis.get('stop_loss', 'N/A')} / {strategy_thesis.get('target_price', 'N/A')} |
| 连续确认/反向确认 | {strategy_thesis.get('confirmation_count', 'N/A')} / {strategy_thesis.get('contradiction_count', 'N/A')} |

> 你可以更新观点，但必须说明相对上一版 thesis 是“延续、增强、减弱、失效还是反转”。若要从观望/防御切换为买入，必须给出新的价格、止损、目标和证据；若只是证据不足，不要用买入。
"""
            pending_transition = portfolio_context.get("strategy_pending_transition")
            if isinstance(pending_transition, dict):
                pending_evidence = pending_transition.get("evidence")
                if not isinstance(pending_evidence, dict):
                    pending_evidence = {}
                prompt += f"""
### 尚待确认的策略转换（只读上下文）
| 项目 | 数据 |
|------|------|
| 原始动作 | {pending_transition.get('raw_action', 'N/A')} |
| 转换方向 | {pending_transition.get('previous_state', 'N/A')} -> {pending_transition.get('new_state', 'N/A')} |
| 当前结果/原因 | {pending_transition.get('result', 'N/A')} / {pending_transition.get('reason', 'N/A')} |
| 已确认交易日数 | {pending_transition.get('confirmation_count', 0)} |
| 已反向确认交易日数 | {pending_transition.get('contradiction_count', 0)} |
| 已记录确认日期 | {pending_evidence.get('confirmation_session_dates', [])} |
| 已记录反向日期 | {pending_evidence.get('contradiction_session_dates', [])} |
| 入场/反转所需交易日 | {pending_evidence.get('entry_confirmation_required', 'N/A')} / {pending_evidence.get('flip_confirmation_required', 'N/A')} |

> 该转换尚未取得执行权。请基于当前证据独立判断，不要为了凑足确认次数而重复上一结论；后端只按不同交易日计数并决定是否授权。
"""
        selection_context = context.get("selection_context")
        if isinstance(selection_context, dict):
            prompt += f"""
## 🧭 机会池入选证据（只读，不代表买入指令）
```json
{json.dumps(selection_context, ensure_ascii=False, sort_keys=True, default=str)}
```

> 请独立验证上述候选理由。主题、排名与评分只说明为何进入分析队列，不构成交易结论；若当前个股证据、市场环境或风险收益不支持，必须输出 `watch/avoid`，不得为了迎合机会池而输出 `buy/add`。
"""
        prompt += format_market_phase_prompt_section(
            context.get("market_phase_context"),
            report_language=report_language,
        )
        daily_market_context_section = format_daily_market_context_prompt_section(
            context.get("daily_market_context"),
            report_language=report_language,
        )
        if daily_market_context_section:
            prompt += daily_market_context_section
        market_structure_section = format_market_structure_prompt_section(
            context.get("market_structure_context"),
            report_language=report_language,
        )
        if market_structure_section:
            prompt += market_structure_section
        if isinstance(analysis_context_pack_summary, str) and analysis_context_pack_summary:
            prompt += analysis_context_pack_summary
        if isinstance(paper_style_context, dict):
            style_settings = paper_style_context.get("settings") if isinstance(paper_style_context.get("settings"), dict) else {}
            style_indicators = context.get("paper_trading_style_indicators") if isinstance(context.get("paper_trading_style_indicators"), dict) else {}
            prompt += f"""
## 🧭 纸面交易风格契约（必须保持时域一致）
| 项目 | 数值 |
|------|------|
| 风格 | {paper_style_context.get('trading_style', 'unknown')} |
| 策略版本 | {paper_style_context.get('policy_version', 'unknown')} |
| 日线输入长度 | {style_settings.get('analysis_window_sessions', 'N/A')} 个完整交易日 |
| 核心均线窗口 | {style_settings.get('core_ma_windows', 'N/A')} |
| ATR/波动窗口 | {style_settings.get('volatility_windows', 'N/A')} |
| 最大持有期 | {style_settings.get('max_holding_sessions', 'N/A')} 个交易日 |
| 计划刷新间隔 | {style_settings.get('plan_refresh_cadence_sessions', 'N/A')} 个交易日 |
| 入场腿/加仓上限 | {style_settings.get('entry_leg_limit', 'N/A')} / {style_settings.get('add_limit', 'N/A')} |
| 风险倍率 | {style_settings.get('risk_multiplier', 'N/A')} |
| 已观测日K数量 | {style_indicators.get('bars_observed', 'N/A')} |
| 风格均线实际值 | {style_indicators.get('moving_averages', 'N/A')} |
| 风格 ATR 实际值 | {style_indicators.get('volatility', 'N/A')} |
| 冻结均线支撑/来源 | {style_indicators.get('support', 'N/A')} / {style_indicators.get('support_basis', 'N/A')} |
| 允许报告期限 | {style_settings.get('compatible_horizons', 'N/A')} |
| 默认持有期 | {style_settings.get('default_holding_sessions', 'N/A')} 个交易日 |
| 价格/ATR 噪声护栏 | {style_settings.get('geometry_guard', 'N/A')} |

> 价格目标、止损、分批入场/退出和证据窗口必须与上述风格一致；不得把短线目标和长线止损混用。
> 请区分“持仓 thesis”和“入场择时”：MA5 乖离、MA5/MA10 短均线更多用于是否等待更优买点；中线风格的核心判断应结合 MA10/MA20/MA60、支撑压力、ATR/波动、量价资金和风险收益，不能把 MA5>MA10>MA20 当作所有风格的买入硬门槛。
"""
        prompt += f"""

## 📈 技术面数据

### {quote_section_title}
| 指标 | 数值 |
|------|------|
{quote_rows_text}

### 均线系统（关键判断指标）
| 均线 | 数值 | 说明 |
|------|------|------|
| MA5 | {today.get('ma5', 'N/A')} | 短期趋势线 |
| MA10 | {today.get('ma10', 'N/A')} | 中短期趋势线 |
| MA20 | {today.get('ma20', 'N/A')} | 中期趋势线 |
| 均线形态 | {context.get('ma_status', unknown_text)} | 多头/空头/缠绕 |
"""
        
        # 添加实时行情数据（量比、换手率等）
        if 'realtime' in context:
            rt = context['realtime']
            prompt += f"""
### 实时行情增强数据
| 指标 | 数值 | 解读 |
|------|------|------|
| 当前价格 | {rt.get('price', 'N/A')} 元 | |
| **量比** | **{rt.get('volume_ratio', 'N/A')}** | {rt.get('volume_ratio_desc', '')} |
| **换手率** | **{rt.get('turnover_rate', 'N/A')}%** | |
| 市盈率(动态) | {rt.get('pe_ratio', 'N/A')} | |
| 市净率 | {rt.get('pb_ratio', 'N/A')} | |
| 总市值 | {self._format_amount(rt.get('total_mv'))} | |
| 流通市值 | {self._format_amount(rt.get('circ_mv'))} | |
| 60日涨跌幅 | {rt.get('change_60d', 'N/A')}% | 中期表现 |
"""

        # 添加财报与分红（价值投资口径）
        fundamental_context = context.get("fundamental_context") if isinstance(context, dict) else None
        earnings_block = (
            fundamental_context.get("earnings", {})
            if isinstance(fundamental_context, dict)
            else {}
        )
        earnings_data = (
            earnings_block.get("data", {})
            if isinstance(earnings_block, dict)
            else {}
        )
        financial_report = (
            earnings_data.get("financial_report", {})
            if isinstance(earnings_data, dict)
            else {}
        )
        dividend_metrics = (
            earnings_data.get("dividend", {})
            if isinstance(earnings_data, dict)
            else {}
        )
        if isinstance(financial_report, dict) or isinstance(dividend_metrics, dict):
            financial_report = financial_report if isinstance(financial_report, dict) else {}
            dividend_metrics = dividend_metrics if isinstance(dividend_metrics, dict) else {}
            ttm_yield = dividend_metrics.get("ttm_dividend_yield_pct", "N/A")
            ttm_cash = dividend_metrics.get("ttm_cash_dividend_per_share", "N/A")
            ttm_count = dividend_metrics.get("ttm_event_count", "N/A")
            report_date = financial_report.get("report_date", "N/A")
            prompt += f"""
### 财报与分红（价值投资口径）
| 指标 | 数值 | 说明 |
|------|------|------|
| 最近报告期 | {report_date} | 来自结构化财报字段 |
| 营业收入 | {financial_report.get('revenue', 'N/A')} | |
| 归母净利润 | {financial_report.get('net_profit_parent', 'N/A')} | |
| 经营现金流 | {financial_report.get('operating_cash_flow', 'N/A')} | |
| ROE | {financial_report.get('roe', 'N/A')} | |
| 近12个月每股现金分红 | {ttm_cash} | 仅现金分红、税前口径 |
| TTM 股息率 | {ttm_yield} | 公式：近12个月每股现金分红 / 当前价格 × 100% |
| TTM 分红事件数 | {ttm_count} | |

> 若上述字段为 N/A 或缺失，请明确写“数据缺失，无法判断”，禁止编造。
"""

        capital_flow_block = (
            fundamental_context.get("capital_flow", {})
            if isinstance(fundamental_context, dict)
            else {}
        )
        capital_flow_data = (
            capital_flow_block.get("data", {})
            if isinstance(capital_flow_block, dict)
            else {}
        )
        stock_flow = (
            capital_flow_data.get("stock_flow", {})
            if isinstance(capital_flow_data, dict)
            else {}
        )
        sector_flow = (
            capital_flow_data.get("sector_rankings", {})
            if isinstance(capital_flow_data, dict)
            else {}
        )
        has_capital_flow = (
            isinstance(stock_flow, dict)
            and any(v is not None for v in stock_flow.values())
        ) or (
            isinstance(sector_flow, dict)
            and (sector_flow.get("top") or sector_flow.get("bottom"))
        )
        if has_capital_flow:
            top_sectors = sector_flow.get("top", []) if isinstance(sector_flow, dict) else []
            bottom_sectors = sector_flow.get("bottom", []) if isinstance(sector_flow, dict) else []
            top_sector_text = "、".join(
                str(item.get("name", "")).strip()
                for item in top_sectors[:3]
                if isinstance(item, dict) and str(item.get("name", "")).strip()
            ) or "N/A"
            bottom_sector_text = "、".join(
                str(item.get("name", "")).strip()
                for item in bottom_sectors[:3]
                if isinstance(item, dict) and str(item.get("name", "")).strip()
            ) or "N/A"
            prompt += f"""
### 主力资金流向（操作建议过滤器）
| 指标 | 数值 | 决策含义 |
|------|------|----------|
| 主力净流入 | {stock_flow.get('main_net_inflow', 'N/A')} | 正值偏支持，负值偏压制 |
| 5日净流入 | {stock_flow.get('inflow_5d', 'N/A')} | 用于判断资金持续性 |
| 10日净流入 | {stock_flow.get('inflow_10d', 'N/A')} | 用于判断资金持续性 |
| 资金流入靠前板块 | {top_sector_text} | 板块资金共振参考 |
| 资金流出靠前板块 | {bottom_sector_text} | 板块风险参考 |

> 资金流向只能作为价格位置的过滤器：接近压力且主力流出时不得追买；接近支撑且未放量跌破时，优先判断为持有观察、震荡或洗盘观察。资金流缺失、获取失败或对 ETF/指数不适用时，只能列入数据限制，不得当作真实流出或利空。
"""

        # 添加三大法人动向（台股筹码过滤器）— tw-only；仅当 institution 区块 status='ok'
        # 且有净额时注入，其他市场 status='not_supported' 会跳过，严格 additive。
        institution_block = (
            fundamental_context.get("institution", {})
            if isinstance(fundamental_context, dict)
            else {}
        )
        institution_data = (
            institution_block.get("data", {})
            if isinstance(institution_block, dict)
            else {}
        )
        if (
            isinstance(institution_block, dict)
            and institution_block.get("status") == "ok"
            and isinstance(institution_data, dict)
            and all(
                institution_data.get(key) is not None
                for key in ("foreign_net", "trust_net", "dealer_net", "total_net")
            )
        ):
            prompt += f"""
### 三大法人动向（台股筹码过滤器，净买卖超，单位:股）
| 法人 | 净买卖超 | 决策含义 |
|------|------|----------|
| 外资 | {institution_data.get('foreign_net', 'N/A')} | 正值=净买超偏支持，负值=净卖超偏压制 |
| 投信 | {institution_data.get('trust_net', 'N/A')} | 投信持续买超常伴随中线做多 |
| 自营商 | {institution_data.get('dealer_net', 'N/A')} | 短线避险/自营方向参考 |
| 三大法人合计 | {institution_data.get('total_net', 'N/A')} | 台股最受关注的筹码信号 |
| 资料日期 | {institution_data.get('date', 'N/A')} | 来源 {institution_data.get('source', 'N/A')} |

> 三大法人是台股的筹码过滤器（相当于 A 股主力资金/龙虎榜的角色，但口径不同、不可混用）：外资与投信同向净买支持价格、同向净卖压制价格。请据此判断台股筹码结构，不要在有本数据时写“筹码结构：数据缺失”。
"""

        # 添加筹码分布数据
        if 'chip' in context:
            chip = context['chip']
            profit_ratio = chip.get('profit_ratio', 0)
            prompt += f"""
### 筹码分布数据（效率指标）
| 指标 | 数值 | 健康标准 |
|------|------|----------|
| **获利比例** | **{profit_ratio:.1%}** | 70-90%时警惕 |
| 平均成本 | {chip.get('avg_cost', 'N/A')} 元 | 现价应高于5-15% |
| 90%筹码集中度 | {chip.get('concentration_90', 0):.2%} | <15%为集中 |
| 70%筹码集中度 | {chip.get('concentration_70', 0):.2%} | |
| 筹码状态 | {chip.get('chip_status', unknown_text)} | |
"""
        else:
            chip_unavailable_text = get_chip_unavailable_text(report_language)
            chip_instruction = (
                "Do not fabricate profit ratio, average cost, or concentration. Mention chip data "
                "unavailability only once in the report; do not repeat per-field no-data text in `chip_structure`."
                if report_language in ("en", "ko")
                else "请勿编造获利比例、平均成本或集中度；报告中只说明一次筹码数据不可用，不要把“数据缺失，无法判断”逐字段重复写入 `chip_structure`。"
            )
            prompt += f"""
### 筹码分布数据（效率指标）
> {chip_unavailable_text}
> {chip_instruction}
"""
        
        # 添加趋势分析结果（仅隐式内建 bull_trend 默认回退保留旧口径）
        if 'trend_analysis' in context:
            trend = _sanitize_trend_analysis_for_prompt(
                context['trend_analysis'],
                volume_change_ratio=context.get('volume_change_ratio'),
            )
            consistency_notes = trend.get('prompt_consistency_notes', [])
            if effective_legacy_default_prompt:
                bias_warning = "🚨 超过5%，严禁追高！" if trend.get('bias_ma5', 0) > 5 else "✅ 安全范围"
                prompt += f"""
### 趋势分析预判（基于交易理念）
| 指标 | 数值 | 判定 |
|------|------|------|
| 趋势状态 | {trend.get('trend_status', unknown_text)} | |
| 均线排列 | {trend.get('ma_alignment', unknown_text)} | MA5>MA10>MA20为多头 |
| 趋势强度 | {trend.get('trend_strength', 0)}/100 | |
| **乖离率(MA5)** | **{trend.get('bias_ma5', 0):+.2f}%** | {bias_warning} |
| 乖离率(MA10) | {trend.get('bias_ma10', 0):+.2f}% | |
| 量能状态 | {trend.get('volume_status', unknown_text)} | {trend.get('volume_trend', '')} |
| 系统信号 | {trend.get('buy_signal', unknown_text)} | |
| 系统评分 | {trend.get('signal_score', 0)}/100 | |

#### 系统分析理由
**买入理由**：
{chr(10).join('- ' + r for r in trend.get('signal_reasons', ['无'])) if trend.get('signal_reasons') else '- 无'}

**风险因素**：
{chr(10).join('- ' + r for r in trend.get('risk_factors', ['无'])) if trend.get('risk_factors') else '- 无'}
"""
                if consistency_notes:
                    prompt += f"""

**一致性约束**：
{chr(10).join('- ' + note for note in consistency_notes)}
"""
            else:
                bias_warning = (
                    "🚨 偏离较大，需谨慎评估追高风险"
                    if trend.get('bias_ma5', 0) > 5
                    else "✅ 位置相对可控"
                )
                prompt += f"""
### 技术与结构分析（供激活技能判断参考）
| 指标 | 数值 | 说明 |
|------|------|------|
| 趋势状态 | {trend.get('trend_status', unknown_text)} | |
| 均线排列 | {trend.get('ma_alignment', unknown_text)} | 结合激活技能判断结构强弱 |
| 趋势强度 | {trend.get('trend_strength', 0)}/100 | |
| **价格位置(MA5)** | **{trend.get('bias_ma5', 0):+.2f}%** | {bias_warning} |
| 价格位置(MA10) | {trend.get('bias_ma10', 0):+.2f}% | |
| 量能状态 | {trend.get('volume_status', unknown_text)} | {trend.get('volume_trend', '')} |
| 系统信号 | {trend.get('buy_signal', unknown_text)} | |
| 系统评分 | {trend.get('signal_score', 0)}/100 | |

#### 系统分析理由
**支持因素**：
{chr(10).join('- ' + r for r in trend.get('signal_reasons', ['无'])) if trend.get('signal_reasons') else '- 无'}

**风险因素**：
{chr(10).join('- ' + r for r in trend.get('risk_factors', ['无'])) if trend.get('risk_factors') else '- 无'}
"""
                if consistency_notes:
                    prompt += f"""

**一致性约束**：
{chr(10).join('- ' + note for note in consistency_notes)}
"""
        
        # 添加昨日对比数据
        if 'yesterday' in context:
            volume_change = context.get('volume_change_ratio', 'N/A')
            prompt += f"""
### 量价变化
- 成交量较昨日变化：{volume_change}倍
- 价格较昨日变化：{context.get('price_change_ratio', 'N/A')}%
"""
            parsed_volume_change = _safe_float(volume_change, default=math.nan)
            if math.isfinite(parsed_volume_change) and parsed_volume_change > 10:
                prompt += """
- ⚠️ 量能异常提示：成交量较昨日放大超过10倍，可能受异常数据或一次性冲量影响，必须降权解读，不能机械视为强确认信号
"""
        
        # 添加新闻搜索结果（重点区域）
        news_window_days: Optional[int] = None
        context_window = context.get("news_window_days")
        try:
            if context_window is not None:
                parsed_window = int(context_window)
                if parsed_window > 0:
                    news_window_days = parsed_window
        except (TypeError, ValueError):
            news_window_days = None

        if news_window_days is None:
            prompt_config = self._get_runtime_config()
            news_window_days = resolve_news_window_days(
                news_max_age_days=getattr(prompt_config, "news_max_age_days", 3),
                news_strategy_profile=getattr(prompt_config, "news_strategy_profile", "short"),
            )
        prompt += """
---

## 📰 舆情情报
"""
        if news_context:
            prompt += f"""
以下是 **{stock_name}({code})** 近{news_window_days}日的新闻搜索结果，请重点提取：
1. 🚨 **风险警报**：减持、处罚、利空
2. 🎯 **利好催化**：业绩、合同、政策
3. 📊 **业绩预期**：年报预告、业绩快报
4. 🕒 **时间规则（强制）**：
   - 输出到 `risk_alerts` / `positive_catalysts` / `latest_news` 的每一条都必须带具体日期（YYYY-MM-DD）
   - 超出近{news_window_days}日窗口的新闻一律忽略
   - 时间未知、无法确定发布日期的新闻一律忽略

```
{news_context}
```
"""
        else:
            prompt += """
未搜索到该股票近期的可引用新闻。请主要依据技术面数据进行分析；这只表示当前新闻窗口无可用证据，不代表没有事件、没有利空或基本面已经更新。
"""

        # 注入缺失数据警告
        if context.get('data_missing'):
            prompt += """
⚠️ **数据缺失警告**
由于接口限制，当前无法获取完整的实时行情和技术指标数据。
请 **忽略上述表格中的 N/A 数据**，重点依据 **【📰 舆情情报】** 中的新闻进行基本面和情绪面分析。
在回答技术面问题（如均线、乖离率）时，请直接说明“数据缺失，无法判断”，**严禁编造数据**。
"""

        # 明确的输出要求
        prompt += f"""
---

## ✅ 分析任务

请为 **{stock_name}({code})** 生成【决策仪表盘】，严格按照 JSON 格式输出。
"""
        if context.get('is_index_etf'):
            prompt += """
> ⚠️ **指数/ETF 分析约束**：该标的为指数跟踪型 ETF 或市场指数。
> - 风险分析仅关注：**指数走势、跟踪误差、市场流动性**
> - 严禁将基金公司的诉讼、声誉、高管变动纳入风险警报
> - 业绩预期基于**指数成分股整体表现**，而非基金公司财报
> - `risk_alerts` 中不得出现基金管理人相关的公司经营风险
> - **个股筹码分布不适用于指数/ETF**。筹码为空时必须标记为“不适用/N/A”，不得描述为数据缺失风险，也不得因此降低评分或置信度
> - **个股 PE/PB 不适用于 ETF**。优先分析跟踪指数整体估值或估值分位；未提供指数估值时不得将其视为负面证据
> - ETF 的行动检查项不得把“筹码健康”作为必要条件，应以成交额、换手率、流动性、折溢价或跟踪表现等适用指标替代
> - ETF 缺少个股 PE/PB、个股筹码或个股主力资金流时，不得仅因指标不适用而自动降低买入判断

"""
        prompt += f"""
### ⚠️ 重要：输出正确的股票名称格式
正确的股票名称格式为“股票名称（股票代码）”，例如“贵州茅台（600519）”。
如果上方显示的股票名称为"股票{code}"或不正确，请在分析开头**明确输出该股票的正确中文全称**。
"""
        if effective_legacy_default_prompt:
            prompt += f"""

### 重点关注（必须明确回答）：
1. ❓ 当前趋势结构是否符合所选交易风格？短线可重点看 MA5/MA10/MA20，中线/持仓需结合 MA10/MA20/MA60，趋势风格需结合更长周期
2. ❓ 当前乖离率和价格位置是否支持入场？—— 偏离过大必须标注"严禁追高/等待回踩"
3. ❓ 量能是否与 setup 匹配（突破看放量，回踩看缩量与支撑，趋势延续看参与度）？
4. ❓ 筹码结构是否健康？
5. ❓ 消息面有无重大利空或当前窗口无可引用证据？（减持、处罚、业绩变脸等）
"""
        else:
            prompt += f"""

### 重点关注（必须明确回答）：
1. ❓ 当前结构是否满足激活技能的关键触发条件？
2. ❓ 当前入场位置与风险回报是否合理？若偏离过大，请明确说明等待条件
3. ❓ 量能、波动与筹码结构是否支持当前结论？
4. ❓ 消息面有无重大利空或与技能结论冲突的信息？
5. ❓ 若结论成立，具体触发条件、止损位、观察点分别是什么？
"""
        prompt += f"""

### 决策仪表盘要求：
- **股票名称**：必须输出正确的中文全称（如"贵州茅台"而非"股票600519"）
- **核心结论**：一句话说清该买/该卖/该等
- **持仓分类建议**：空仓者怎么做 vs 持仓者怎么做
- **具体狙击点位**：买入价、止损价、目标价（精确到分）
- **止损/止盈契约**：入场价、止损价、`take_profit` 和可选 `exit_legs` 必须属于同一交易周期；`take_profit` 必须表示最终目标，不得把第一止盈位写成完整目标；若第一目标只是短线触达位而最终目标更远，必须填写 `first_take_profit`、`final_target` 和 `exit_legs`，并让 `exit_legs.allocation_fraction` 合计等于 1.0、目标价严格递增
- **风险收益约束**：若按入场中位价计算，完整止盈或分批止盈加权后的收益相对止损风险无法达到系统最低盈亏比，必须改为 `watch/avoid` 或明确“等待更优入场/更远目标确认”，不得为了通过校验编造价格
- **检查清单**：每项用 ✅/⚠️/❌ 标记
- **消息面时间合规**：`latest_news`、`risk_alerts`、`positive_catalysts` 不得包含超出近{news_window_days}日或时间未知的信息
- **技术面一致性**：严禁把“空头排列”和“多头排列”等互斥结论同时当作有效依据；若基本面/事件面与技术面冲突，必须明确写“事件先行、技术待确认”或“基本面偏多，但技术面尚未确认”
 
请输出完整的 JSON 格式决策仪表盘。"""

        if report_language == "en":
            prompt += """

### Output language requirements (highest priority)
- Keep every JSON key exactly as defined above; do not translate keys.
- `decision_type` must remain `buy`, `hold`, or `sell`.
- All human-readable JSON values must be in English.
- This includes `stock_name`, `trend_prediction`, `operation_advice`, `confidence_level`, all nested dashboard text, checklist items, and every summary field.
- Use the common English company name when you are confident. If not, keep the listed company name rather than inventing one.
- When data is missing, explain it in English instead of Chinese.
"""
        elif report_language == "ko":
            prompt += """

### Output language requirements (highest priority)
- Keep every JSON key exactly as defined above; do not translate keys.
- `decision_type` must remain `buy`, `hold`, or `sell`.
- All human-readable JSON values must be in Korean (한국어).
- This includes `stock_name`, `trend_prediction`, `operation_advice`, `confidence_level`, all nested dashboard text, checklist items, and every summary field.
- Use the common Korean or original listed company name when you are confident. If not, keep the listed company name rather than inventing one.
- When data is missing, explain it in Korean instead of Chinese.
"""
        else:
            prompt += f"""

### 输出语言要求（最高优先级）
- 所有 JSON 键名必须保持不变，不要翻译键名。
- `decision_type` 必须保持为 `buy`、`hold`、`sell`。
- 所有面向用户的人类可读文本值必须使用中文。
- 当数据缺失时，请使用中文直接说明“{no_data_text}，无法判断”。
"""
        
        return prompt
    
    def _format_volume(self, volume: Optional[float]) -> str:
        """格式化成交量显示"""
        if volume is None:
            return 'N/A'
        if volume >= 1e8:
            return f"{volume / 1e8:.2f} 亿股"
        elif volume >= 1e4:
            return f"{volume / 1e4:.2f} 万股"
        else:
            return f"{volume:.0f} 股"
    
    def _format_amount(self, amount: Optional[float]) -> str:
        """格式化成交额显示"""
        if amount is None:
            return 'N/A'
        if amount >= 1e8:
            return f"{amount / 1e8:.2f} 亿元"
        elif amount >= 1e4:
            return f"{amount / 1e4:.2f} 万元"
        else:
            return f"{amount:.0f} 元"

    def _format_percent(self, value: Optional[float]) -> str:
        """格式化百分比显示"""
        if value is None:
            return 'N/A'
        try:
            return f"{float(value):.2f}%"
        except (TypeError, ValueError):
            return 'N/A'

    def _format_price(self, value: Optional[float]) -> str:
        """格式化价格显示"""
        if value is None:
            return 'N/A'
        try:
            return f"{float(value):.2f}"
        except (TypeError, ValueError):
            return 'N/A'

    def _build_market_snapshot(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """构建当日行情快照（展示用）"""
        today = context.get('today', {}) or {}
        realtime = context.get('realtime', {}) or {}
        yesterday = context.get('yesterday', {}) or {}

        prev_close = yesterday.get('close')
        close = today.get('close')
        high = today.get('high')
        low = today.get('low')

        amplitude = None
        change_amount = None
        if prev_close not in (None, 0) and high is not None and low is not None:
            try:
                amplitude = (float(high) - float(low)) / float(prev_close) * 100
            except (TypeError, ValueError, ZeroDivisionError):
                amplitude = None
        if prev_close is not None and close is not None:
            try:
                change_amount = float(close) - float(prev_close)
            except (TypeError, ValueError):
                change_amount = None

        snapshot = {
            "date": context.get('date', '未知'),
            "close": self._format_price(close),
            "open": self._format_price(today.get('open')),
            "high": self._format_price(high),
            "low": self._format_price(low),
            "prev_close": self._format_price(prev_close),
            "pct_chg": self._format_percent(today.get('pct_chg')),
            "change_amount": self._format_price(change_amount),
            "amplitude": self._format_percent(amplitude),
            "volume": self._format_volume(today.get('volume')),
            "amount": self._format_amount(today.get('amount')),
        }

        if realtime:
            snapshot.update({
                "price": self._format_price(realtime.get('price')),
                "volume_ratio": realtime.get('volume_ratio', 'N/A'),
                "turnover_rate": self._format_percent(realtime.get('turnover_rate')),
                "source": getattr(realtime.get('source'), 'value', realtime.get('source', 'N/A')),
            })

        return snapshot

    def _check_content_integrity(
        self,
        result: AnalysisResult,
        *,
        require_phase_decision: bool = False,
    ) -> Tuple[bool, List[str]]:
        """Delegate to module-level check_content_integrity."""
        return check_content_integrity(result, require_phase_decision=require_phase_decision)

    def _build_integrity_complement_prompt(self, missing_fields: List[str], report_language: str = "zh") -> str:
        """Build complement instruction for missing mandatory fields."""
        report_language = normalize_report_language(report_language)
        if report_language in ("en", "ko"):
            lines = ["### Completion requirements: fill the missing mandatory fields below and output the full JSON again:"]
            for f in missing_fields:
                if f == "sentiment_score":
                    lines.append("- sentiment_score: integer score from 0 to 100")
                elif f == "operation_advice":
                    lines.append("- operation_advice: localized action advice")
                elif f == "analysis_summary":
                    lines.append("- analysis_summary: concise analysis summary")
                elif f == "dashboard.core_conclusion.one_sentence":
                    lines.append("- dashboard.core_conclusion.one_sentence: one-line decision")
                elif f == "dashboard.intelligence.risk_alerts":
                    lines.append("- dashboard.intelligence.risk_alerts: risk alert list (can be empty)")
                elif f == "dashboard.battle_plan.sniper_points.stop_loss":
                    lines.append("- dashboard.battle_plan.sniper_points.stop_loss: stop-loss level")
                elif f == "dashboard.battle_plan.sniper_points.take_profit":
                    lines.append("- dashboard.battle_plan.sniper_points.take_profit: final take-profit target")
                elif f == "dashboard.battle_plan.sniper_points.final_target":
                    lines.append("- dashboard.battle_plan.sniper_points.final_target: final target matching the last exit leg")
                elif f == "dashboard.battle_plan.sniper_points.entry_leg_schema_version":
                    lines.append("- dashboard.battle_plan.sniper_points.entry_leg_schema_version: entry-legs-v1")
                elif f == "dashboard.battle_plan.sniper_points.entry_legs":
                    lines.append("- dashboard.battle_plan.sniper_points.entry_legs: at least one executable entry leg with price_rules and allocation")
                elif f == "dashboard.battle_plan.sniper_points.exit_leg_schema_version":
                    lines.append("- dashboard.battle_plan.sniper_points.exit_leg_schema_version: exit-legs-v1")
                elif f == "dashboard.battle_plan.sniper_points.exit_legs":
                    lines.append("- dashboard.battle_plan.sniper_points.exit_legs: target legs whose allocation sums to 1.0")
                elif f == "dashboard.battle_plan.sniper_points.stop_target_order":
                    lines.append("- buy/add price order: stop_loss must be below the final take-profit target")
                elif f == "dashboard.battle_plan.sniper_points.entry_stop_target_order":
                    lines.append("- buy/add price order: stop_loss < entry range < final target")
                elif f == "dashboard.battle_plan.sniper_points.final_target_exit_mismatch":
                    lines.append("- final_target/take_profit must equal the last exit leg target")
                elif f == "dashboard.phase_decision.phase_context":
                    lines.append("- dashboard.phase_decision.phase_context: public market phase summary subset")
                elif f == "dashboard.phase_decision.action_window":
                    lines.append("- dashboard.phase_decision.action_window: phase-aware action window")
                elif f == "dashboard.phase_decision.immediate_action":
                    lines.append("- dashboard.phase_decision.immediate_action: act now / wait / watch / no intraday action")
                elif f == "dashboard.phase_decision.watch_conditions":
                    lines.append("- dashboard.phase_decision.watch_conditions: list of watch conditions")
                elif f == "dashboard.phase_decision.next_check_time":
                    lines.append("- dashboard.phase_decision.next_check_time: next check point or market-local time")
                elif f == "dashboard.phase_decision.confidence_reason":
                    lines.append("- dashboard.phase_decision.confidence_reason: confidence rationale and data limits")
                elif f == "dashboard.phase_decision.data_limitations":
                    lines.append("- dashboard.phase_decision.data_limitations: list of phase/data quality limitations")
            return "\n".join(lines)

        lines = ["### 补全要求：请在上方分析基础上补充以下必填内容，并输出完整 JSON："]
        for f in missing_fields:
            if f == "sentiment_score":
                lines.append("- sentiment_score: 0-100 综合评分")
            elif f == "operation_advice":
                lines.append("- operation_advice: 买入/加仓/持有/减仓/卖出/观望")
            elif f == "analysis_summary":
                lines.append("- analysis_summary: 综合分析摘要")
            elif f == "dashboard.core_conclusion.one_sentence":
                lines.append("- dashboard.core_conclusion.one_sentence: 一句话决策")
            elif f == "dashboard.intelligence.risk_alerts":
                lines.append("- dashboard.intelligence.risk_alerts: 风险警报列表（可为空数组）")
            elif f == "dashboard.battle_plan.sniper_points.stop_loss":
                lines.append("- dashboard.battle_plan.sniper_points.stop_loss: 止损价")
            elif f == "dashboard.battle_plan.sniper_points.take_profit":
                lines.append("- dashboard.battle_plan.sniper_points.take_profit: 最终止盈目标")
            elif f == "dashboard.battle_plan.sniper_points.final_target":
                lines.append("- dashboard.battle_plan.sniper_points.final_target: 与最后一个退出批次一致的最终目标")
            elif f == "dashboard.battle_plan.sniper_points.entry_leg_schema_version":
                lines.append("- dashboard.battle_plan.sniper_points.entry_leg_schema_version: entry-legs-v1")
            elif f == "dashboard.battle_plan.sniper_points.entry_legs":
                lines.append("- dashboard.battle_plan.sniper_points.entry_legs: 至少一个带 price_rules 和 allocation 的可执行入场批次")
            elif f == "dashboard.battle_plan.sniper_points.exit_leg_schema_version":
                lines.append("- dashboard.battle_plan.sniper_points.exit_leg_schema_version: exit-legs-v1")
            elif f == "dashboard.battle_plan.sniper_points.exit_legs":
                lines.append("- dashboard.battle_plan.sniper_points.exit_legs: allocation 合计为 1.0 的止盈/退出批次")
            elif f == "dashboard.battle_plan.sniper_points.stop_target_order":
                lines.append("- 买入/加仓价格关系：stop_loss 必须低于最终止盈目标")
            elif f == "dashboard.battle_plan.sniper_points.entry_stop_target_order":
                lines.append("- 买入/加仓价格关系：stop_loss < 入场区间 < 最终目标")
            elif f == "dashboard.battle_plan.sniper_points.final_target_exit_mismatch":
                lines.append("- final_target/take_profit 必须等于最后一个退出批次目标")
            elif f == "dashboard.phase_decision.phase_context":
                lines.append("- dashboard.phase_decision.phase_context: 公开低敏市场阶段摘要子集")
            elif f == "dashboard.phase_decision.action_window":
                lines.append("- dashboard.phase_decision.action_window: 阶段化行动窗口")
            elif f == "dashboard.phase_decision.immediate_action":
                lines.append("- dashboard.phase_decision.immediate_action: 立即行动/等待确认/观察/无盘中动作")
            elif f == "dashboard.phase_decision.watch_conditions":
                lines.append("- dashboard.phase_decision.watch_conditions: 观察条件数组")
            elif f == "dashboard.phase_decision.next_check_time":
                lines.append("- dashboard.phase_decision.next_check_time: 下一次检查点或市场本地时间")
            elif f == "dashboard.phase_decision.confidence_reason":
                lines.append("- dashboard.phase_decision.confidence_reason: 置信度理由与数据限制")
            elif f == "dashboard.phase_decision.data_limitations":
                lines.append("- dashboard.phase_decision.data_limitations: 阶段/数据质量限制数组")
        return "\n".join(lines)

    def _build_integrity_retry_prompt(
        self,
        base_prompt: str,
        previous_response: str,
        missing_fields: List[str],
        report_language: str = "zh",
    ) -> str:
        """Build compact retry prompt without replaying the full prior response."""
        complement = self._build_integrity_complement_prompt(missing_fields, report_language=report_language)
        preview = redact_diagnostic_text(previous_response, limit=1200).strip()
        if normalize_report_language(report_language) in ("en", "ko"):
            prefix = (
                "### Retry instruction\n"
                "The previous submission failed validation. Keep the original analysis context and reasoning chain, "
                "fix the fields listed below, and submit the full dashboard again in the required structured format. "
                "Do not invent prices or risks merely to pass validation.\n"
                "Previous response preview for continuity:"
            )
        else:
            prefix = (
                "### 重试指令\n"
                "上一次提交未通过校验。请保留原始分析上下文和完整推理链，只修复下列字段，"
                "并按当前要求的结构化格式重新提交完整决策仪表盘。不得为了通过校验编造价格或风险。\n"
                "上一次响应摘要如下，仅用于保持上下文："
            )
        return "\n\n".join([
            base_prompt,
            prefix,
            preview,
            complement,
        ])

    def _build_structured_tool_retry_prompt(
        self,
        base_prompt: str,
        *,
        report_language: str,
        previous_response: Optional[str] = None,
        decision_lock: Optional[Dict[str, Any]] = None,
        include_base_prompt: bool = True,
    ) -> str:
        """Retry a rejected native-tool submission without accepting free text."""
        previous_response = self._sanitize_structured_retry_draft(previous_response)
        lock_keys = sorted((decision_lock or {}).keys())
        lock_summary = ""
        if lock_keys:
            lock_summary = "\nLocked first-stage decision fields: " + ", ".join(lock_keys)
        if normalize_report_language(report_language) == "en":
            instruction = (
                "### Structured resubmission\n"
                "The previous tool submission was rejected by the receive-side schema. "
                "Do not re-run or replace the market analysis. Treat the previous visible assistant "
                "draft as the authoritative analytical conclusion and use the original evidence only "
                "to fill missing schema fields or correct impossible type/schema violations. "
                "Call submit_analysis_report exactly once again. "
                "Include every required field with the declared type, preserve the decision logic, "
                "and do not invent prices or facts to satisfy validation. If an already stated core "
                "decision, score, action, entry, stop-loss, or target cannot be represented faithfully, "
                "fail instead of silently changing it."
                f"{lock_summary}"
            )
            if previous_response:
                instruction += (
                    "\n\nThe previous assistant message below is only an analytical draft because it did "
                    "not contain the required tool call. It is not an accepted report, but its visible "
                    "decision content must be preserved during serialization.\n"
                    f"{previous_response}"
                )
        else:
            instruction = (
                "### 结构化重新提交\n"
                "上一次工具参数未通过接收端 Schema。不要重新做一遍市场判断，也不要替换原判断。"
                "请把上一轮可见助手草稿视为权威分析结论，只允许参考原始证据补齐缺失 Schema 字段"
                "或修正不可能通过的类型/结构错误；并且只调用一次 submit_analysis_report。"
                "所有必填字段必须使用声明的类型，保持原有决策逻辑，不得为了通过校验编造价格或事实。"
                "如果已经出现的核心决策、评分、动作、买点、止损或目标无法被忠实表达，应失败，"
                "不得静默改写。"
                f"{lock_summary}"
            )
            if previous_response:
                instruction += (
                    "\n\n下面的上一轮助手消息只视为分析草稿，因为它没有包含必需的工具调用；"
                    "它不是已接受报告，但其中可见的决策内容在结构化提交时必须保持一致。\n"
                    f"{previous_response}"
                )
        return "\n\n".join([base_prompt, instruction]) if include_base_prompt else instruction

    def _apply_placeholder_fill(self, result: AnalysisResult, missing_fields: List[str]) -> None:
        """Delegate to module-level apply_placeholder_fill."""
        apply_placeholder_fill(result, missing_fields)

    def _extract_analysis_json_object(self, response_text: str) -> Tuple[str, Dict[str, Any]]:
        """Extract the single allowed JSON object from an LLM response."""

        text = response_text or ""
        stripped = text.strip()
        if not stripped:
            raise ValueError("empty_response")

        fence_pattern = re.compile(
            r"```[ \t]*(?P<lang>[A-Za-z0-9_-]*)[ \t]*\n?(?P<body>.*?)```",
            flags=re.DOTALL,
        )
        fenced_matches = list(fence_pattern.finditer(text))
        if len(fenced_matches) > 1:
            raise ValueError("ambiguous_json")
        if len(fenced_matches) == 1:
            match = fenced_matches[0]
            outside = (text[:match.start()] + text[match.end():]).strip()
            if outside:
                raise ValueError("ambiguous_json")
            fence_lang = (match.group("lang") or "").strip().lower()
            if fence_lang not in {"", "json"}:
                raise ValueError("ambiguous_json")
            json_str = match.group("body").strip()
            data = self._load_analysis_json_candidate(json_str)
            return json_str, data
        if "```" in text:
            raise ValueError("ambiguous_json")

        try:
            data = self._load_analysis_json_candidate(stripped)
        except json.JSONDecodeError as exc:
            if self._contains_embedded_json_object(text):
                raise ValueError("ambiguous_json") from exc
            raise
        return stripped, data

    def _normalize_single_fenced_json_response(self, response_text: str) -> Optional[str]:
        """Recover one JSON fence from provider-added, non-JSON envelope text.

        This is intentionally narrower than the public parser contract.  It is
        used only immediately after the original response validator rejects a
        provider response, and the extracted body is validated again before it
        can be returned or persisted.
        """

        text = response_text or ""
        fence_pattern = re.compile(
            r"```[ \t]*(?P<lang>[A-Za-z0-9_-]*)[ \t]*\n?(?P<body>.*?)```",
            flags=re.DOTALL,
        )
        matches = list(fence_pattern.finditer(text))
        if len(matches) != 1:
            return None

        match = matches[0]
        fence_lang = (match.group("lang") or "").strip().lower()
        if fence_lang not in {"", "json"}:
            return None

        outside = (text[:match.start()] + text[match.end():]).strip()
        if "```" in outside or "{" in outside or "}" in outside:
            return None

        json_text = match.group("body").strip()
        try:
            self._load_analysis_json_candidate(json_text)
        except Exception:
            return None
        return json_text

    def _load_analysis_json_candidate(self, json_str: str) -> Dict[str, Any]:
        """Parse one already-selected JSON candidate, repairing common LLM JSON drift."""
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError:
            stripped = (json_str or "").strip()
            try:
                _obj, end = json.JSONDecoder().raw_decode(stripped)
            except json.JSONDecodeError:
                pass
            else:
                if stripped[end:].strip():
                    raise
            if not (stripped.startswith("{") and stripped.endswith("}")):
                raise
            repaired = self._fix_json_string(stripped)
            data = json.loads(repaired)
        if not isinstance(data, dict):
            raise TypeError("json_root_not_object")
        return data

    @staticmethod
    def _contains_embedded_json_object(text: str) -> bool:
        decoder = json.JSONDecoder()
        count = 0
        for index, char in enumerate(text):
            if char != "{":
                continue
            try:
                _obj, end = decoder.raw_decode(text[index:])
            except json.JSONDecodeError:
                continue
            count += 1
            before = text[:index].strip()
            after = text[index + end:].strip()
            if count > 1 or before or after:
                return True
        return False

    def _validate_analysis_minimal_contract(self, data: Dict[str, Any]) -> Dict[str, Any]:
        # Normalize an explicit confidence label with a parenthesized explanation.
        # Do not infer missing/ambiguous confidence: it affects downstream gates.
        raw_confidence = data.get("confidence_level")
        if isinstance(raw_confidence, str) and not _is_valid_trading_confidence_level(raw_confidence):
            match = re.fullmatch(
                r"(高|中等|中|低|high|medium|mid|low)(?:\s*[（(][^\r\n]*[）)])?",
                raw_confidence.strip(), re.IGNORECASE,
            )
            if match:
                data = dict(data)
                label = match.group(1).lower()
                data['confidence_level'] = {'中等': '中', 'mid': 'medium'}.get(label, label)
                limitations = data.get('limitations')
                limitations = list(limitations) if isinstance(limitations, list) else ([limitations] if limitations else [])
                data['limitations'] = limitations + [f"confidence_detail: {raw_confidence}"]
        decision_keys = {
            "sentiment_score",
            "trend_prediction",
            "operation_advice",
            "decision_type",
        }
        if not any(key in data for key in decision_keys):
            raise self._generation_validation_error(
                GenerationErrorCode.SCHEMA_VALIDATION_FAILED,
                reason="minimal_contract_failed",
                message="analysis JSON does not contain a decision field",
            )
        if "dashboard" in data and not isinstance(data.get("dashboard"), dict):
            data["dashboard"] = None
        native_missing = _native_tool_core_contract_missing_fields(data)
        if native_missing:
            raise self._generation_validation_error(
                GenerationErrorCode.SCHEMA_VALIDATION_FAILED,
                reason="native_tool_core_contract_failed",
                message="missing or invalid trading core fields: " + ",".join(native_missing),
            )
        if "sentiment_score" in data:
            try:
                int(data.get("sentiment_score", 50))
            except (TypeError, ValueError) as exc:
                raise self._generation_validation_error(
                    GenerationErrorCode.SCHEMA_VALIDATION_FAILED,
                    reason="parser_contract_failed",
                    message="sentiment_score must be integer-compatible",
                ) from exc
        try:
            schema = AnalysisReportSchema.model_validate(data)
        except Exception as exc:
            raise self._generation_validation_error(
                GenerationErrorCode.SCHEMA_VALIDATION_FAILED,
                reason="schema_validation_failed",
                message=str(exc)[:200],
            ) from exc
        return schema.model_dump(exclude_none=True)

    def _generation_validation_error(
        self,
        error_code: GenerationErrorCode,
        *,
        reason: str,
        message: str,
    ) -> GenerationError:
        try:
            backend_id, _fallback_backend_id = self._resolve_generation_backend_config()
        except GenerationError:
            backend_id = "generation_backend"
        return GenerationError(
            error_code=error_code,
            stage="validation",
            retryable=True,
            fallbackable=True,
            backend=backend_id,
            provider=backend_id,
            details={
                "reason": reason,
                "message": message,
            },
        )

    def _parse_response(
        self, 
        response_text: str, 
        code: str, 
        name: str
    ) -> AnalysisResult:
        """
        解析 Gemini 响应（决策仪表盘版）
        
        尝试从响应中提取 JSON 格式的分析结果，包含 dashboard 字段
        如果解析失败，尝试智能提取或返回默认结果
        """
        try:
            report_language = normalize_report_language(
                getattr(self._get_runtime_config(), "report_language", "zh")
            )
            try:
                _json_str, data = self._extract_analysis_json_object(response_text)
                data = self._validate_analysis_minimal_contract(data)
            except Exception as exc:
                logger.warning("无法从响应中提取唯一有效 JSON，标记为解析失败: %s", exc)
                return self._parse_text_response(response_text, code, name)

            # 提取 dashboard 数据
            dashboard = data.get('dashboard', None)
            guardrail_reason = data.get("guardrail_reason") or data.get("downgrade_reason")
            if guardrail_reason and isinstance(dashboard, dict):
                score_calibration = dashboard.get("decision_score_calibration")
                if not isinstance(score_calibration, dict):
                    score_calibration = {}
                    dashboard["decision_score_calibration"] = score_calibration
                score_calibration.setdefault("guardrail_reason", str(guardrail_reason).strip())
            # 归一化 signal_attribution（LLM 可能返回字符串/负数/总和≠100）
            normalize_report_signal_attribution(dashboard)

            # 优先使用 AI 返回的股票名称（如果原名称无效或包含代码）
            ai_stock_name = _report_display_text(data.get('stock_name'))
            if ai_stock_name and (name.startswith('股票') or name == code or 'Unknown' in name):
                name = ai_stock_name

            # 解析所有字段，使用默认值防止缺失
            # 解析 decision_type，如果没有则根据 operation_advice 推断
            decision_type = _report_display_text(data.get('decision_type'))
            if not decision_type:
                op = _report_display_text(
                    data.get('operation_advice'),
                    localize_operation_advice('持有', report_language),
                )
                decision_type = infer_decision_type_from_advice(op, default='hold')

            explicit_action = data.get("action")
            if explicit_action is None and isinstance(dashboard, dict):
                explicit_action = dashboard.get("action")

            result = AnalysisResult(
                code=code,
                name=name,
                # 核心指标
                sentiment_score=int(data.get('sentiment_score', 50)),
                trend_prediction=_report_display_text(
                    data.get('trend_prediction'),
                    localize_trend_prediction('震荡', report_language),
                ),
                operation_advice=_report_display_text(
                    data.get('operation_advice'),
                    localize_operation_advice('持有', report_language),
                ),
                decision_type=decision_type,
                confidence_level=localize_confidence_level(
                    data.get('confidence_level', localize_confidence_level('中', report_language)),
                    report_language,
                ),
                report_language=report_language,
                report_schema_version=data.get("report_schema_version"),
                raw_score=data.get("raw_score", data.get("sentiment_score")),
                adjusted_score=data.get("adjusted_score"),
                guardrail=data.get("guardrail") if isinstance(data.get("guardrail"), dict) else None,
                limitations=(
                    [str(item) for item in data.get("limitations", []) if str(item or "").strip()]
                    if isinstance(data.get("limitations"), list)
                    else []
                ),
                position_actions=(
                    data.get("position_actions")
                    if isinstance(data.get("position_actions"), dict)
                    else None
                ),
                # 决策仪表盘
                dashboard=dashboard,
                # 走势分析
                trend_analysis=_report_display_text(data.get('trend_analysis')),
                short_term_outlook=_report_display_text(data.get('short_term_outlook')),
                medium_term_outlook=_report_display_text(data.get('medium_term_outlook')),
                # 技术面
                technical_analysis=_report_display_text(data.get('technical_analysis')),
                ma_analysis=_report_display_text(data.get('ma_analysis')),
                volume_analysis=_report_display_text(data.get('volume_analysis')),
                pattern_analysis=_report_display_text(data.get('pattern_analysis')),
                # 基本面
                fundamental_analysis=_report_display_text(data.get('fundamental_analysis')),
                sector_position=_report_display_text(data.get('sector_position')),
                company_highlights=_report_display_text(data.get('company_highlights')),
                # 情绪面/消息面
                news_summary=_report_display_text(data.get('news_summary')),
                market_sentiment=_report_display_text(data.get('market_sentiment')),
                hot_topics=_report_display_text(data.get('hot_topics')),
                # 综合
                analysis_summary=_report_display_text(data.get('analysis_summary'), _localized_text(
                    report_language, en='Analysis completed', zh='分析完成', ko='분석 완료')),
                key_points=_report_display_text(data.get('key_points')),
                risk_warning=_report_display_text(data.get('risk_warning')),
                buy_reason=_report_display_text(data.get('buy_reason')),
                # 元数据
                search_performed=data.get('search_performed') if isinstance(data.get('search_performed'), bool) else False,
                data_sources=_report_display_text(data.get('data_sources'), _localized_text(
                    report_language, en='Technical data', zh='技术面数据', ko='기술적 데이터')),
                success=True,
            )
            normalized_explicit_action = normalize_decision_action(explicit_action)
            parsed_result = populate_decision_action_fields(
                result,
                explicit_action=explicit_action,
                align_with_score=True,
            )
            if (
                data.get("report_schema_version") == self.ANALYSIS_REPORT_TOOL_SCHEMA_VERSION
                and normalized_explicit_action not in {"buy", "add"}
                and normalize_decision_action(getattr(parsed_result, "action", None)) in {"buy", "add"}
            ):
                parsed_result = populate_decision_action_fields(
                    parsed_result,
                    explicit_action=normalized_explicit_action,
                    align_with_score=False,
                )
            return parsed_result
                
        except json.JSONDecodeError as e:
            logger.warning(f"JSON 解析失败: {e}，标记为解析失败")
            return self._parse_text_response(response_text, code, name)
    
    def _fix_json_string(self, json_str: str) -> str:
        """修复常见的 JSON 格式问题"""
        import re
        
        # 移除注释
        json_str = re.sub(r'//.*?\n', '\n', json_str)
        json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
        
        # 修复尾随逗号
        json_str = re.sub(r',\s*}', '}', json_str)
        json_str = re.sub(r',\s*]', ']', json_str)
        
        # 确保布尔值是小写
        json_str = json_str.replace('True', 'true').replace('False', 'false')
        
        # fix by json-repair
        json_str = repair_json(json_str)
        
        return json_str

    def _validate_json_response(self, text: str) -> None:
        """Validate that *text* contains one parser-compatible JSON object.

        Used as the ``response_validator`` argument to :meth:`_call_litellm` so
        that a JSON-less or unparseable reply from the primary model is treated
        as a model failure and triggers fallback to the next configured model.

        Raises:
            GenerationError: if the response has no unique parser-compatible
                JSON object, the selected JSON candidate cannot be parsed, or
                the parsed object cannot satisfy the minimal parser contract.
        """
        try:
            _json_str, data = self._extract_analysis_json_object(text)
        except ValueError as exc:
            reason = str(exc) or "invalid_json"
            if reason == "ambiguous_json":
                message = "JSON source is ambiguous"
            else:
                message = "No unique JSON object found in LLM response"
            raise self._generation_validation_error(
                GenerationErrorCode.INVALID_JSON,
                reason=reason,
                message=message,
            ) from exc
        except json.JSONDecodeError as exc:
            raise self._generation_validation_error(
                GenerationErrorCode.INVALID_JSON,
                reason="invalid_json",
                message=str(exc)[:200],
            ) from exc
        except Exception as exc:
            raise self._generation_validation_error(
                GenerationErrorCode.INVALID_JSON,
                reason="invalid_json",
                message=str(exc)[:200],
            ) from exc

        self._validate_analysis_minimal_contract(data)
    
    def _parse_text_response(
        self, 
        response_text: str, 
        code: str, 
        name: str
    ) -> AnalysisResult:
        """从纯文本响应中尽可能提取分析信息"""
        report_language = normalize_report_language(
            getattr(self._get_runtime_config(), "report_language", "zh")
        )
        # 尝试识别关键词来判断情绪
        sentiment_score = 50
        trend = localize_trend_prediction('震荡', report_language)
        advice = _analysis_failed_label(report_language)
        
        text_lower = response_text.lower()
        
        # 简单的情绪识别
        positive_keywords = ['看多', '买入', '上涨', '突破', '强势', '利好', '加仓', 'bullish', 'buy']
        negative_keywords = ['看空', '卖出', '下跌', '跌破', '弱势', '利空', '减仓', 'bearish', 'sell']
        
        positive_count = sum(1 for kw in positive_keywords if kw in text_lower)
        negative_count = sum(1 for kw in negative_keywords if kw in text_lower)
        
        if positive_count > negative_count + 1:
            sentiment_score = 65
            trend = localize_trend_prediction('看多', report_language)
            advice = localize_operation_advice('买入', report_language)
            decision_type = 'buy'
        elif negative_count > positive_count + 1:
            sentiment_score = 35
            trend = localize_trend_prediction('看空', report_language)
            advice = localize_operation_advice('卖出', report_language)
            decision_type = 'sell'
        else:
            decision_type = 'hold'
        
        # 截取前500字符作为摘要
        summary = response_text[:500] if response_text else _localized_text(
            report_language, en='No analysis result', zh='无分析结果', ko='분석 결과 없음')
        
        result = AnalysisResult(
            code=code,
            name=name,
            sentiment_score=sentiment_score,
            trend_prediction=trend,
            operation_advice=advice,
            decision_type=decision_type,
            confidence_level=localize_confidence_level('低', report_language),
            analysis_summary=summary,
            key_points=_localized_text(
                report_language,
                en='JSON parsing failed; treat this as best-effort output.',
                zh='JSON解析失败，仅供参考',
                ko='JSON 파싱에 실패했습니다. 참고용으로만 사용하세요.',
            ),
            risk_warning=_localized_text(
                report_language,
                en='The result may be inaccurate. Cross-check with other information.',
                zh='分析结果可能不准确，建议结合其他信息判断',
                ko='결과가 부정확할 수 있습니다. 다른 정보와 교차 확인하세요.',
            ),
            raw_response=response_text,
            success=False,
            error_message='LLM response is not valid JSON/schema; failed attempt will be persisted without a trading action',
            report_language=report_language,
        )
        return _mark_failed_result_non_actionable(result)
    
    def batch_analyze(
        self, 
        contexts: List[Dict[str, Any]],
        delay_between: float = 2.0
    ) -> List[AnalysisResult]:
        """
        批量分析多只股票
        
        注意：为避免 API 速率限制，每次分析之间会有延迟
        
        Args:
            contexts: 上下文数据列表
            delay_between: 每次分析之间的延迟（秒）
            
        Returns:
            AnalysisResult 列表
        """
        results = []
        
        for i, context in enumerate(contexts):
            if i > 0:
                logger.debug(f"等待 {delay_between} 秒后继续...")
                time.sleep(delay_between)
            
            result = self.analyze(context)
            results.append(result)
        
        return results


# 便捷函数
def get_analyzer() -> GeminiAnalyzer:
    """获取 LLM 分析器实例"""
    return GeminiAnalyzer()


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)
    
    # 模拟上下文数据
    test_context = {
        'code': '600519',
        'date': '2026-01-09',
        'today': {
            'open': 1800.0,
            'high': 1850.0,
            'low': 1780.0,
            'close': 1820.0,
            'volume': 10000000,
            'amount': 18200000000,
            'pct_chg': 1.5,
            'ma5': 1810.0,
            'ma10': 1800.0,
            'ma20': 1790.0,
            'volume_ratio': 1.2,
        },
        'ma_status': '多头排列 📈',
        'volume_change_ratio': 1.3,
        'price_change_ratio': 1.5,
    }
    
    analyzer = GeminiAnalyzer()
    
    if analyzer.is_available():
        print("=== AI 分析测试 ===")
        result = analyzer.analyze(test_context)
        print(f"分析结果: {result.to_dict()}")
    else:
        print("Gemini API 未配置，跳过测试")
