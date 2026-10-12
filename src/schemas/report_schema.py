# -*- coding: utf-8 -*-
"""
===================================
Report Engine - Pydantic Schema
===================================

Defines AnalysisReportSchema for validating LLM JSON output.
Aligns with SYSTEM_PROMPT in src/analyzer.py.
Uses Optional for lenient parsing; business-layer integrity checks are separate.
"""

import json
import math
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _lenient_text_list(value: Any) -> List[str]:
    """Normalize narrative/display list fields without failing the report.

    Trading-critical fields are still validated in the business layer.  These
    arrays are used for presentation, explanations, or passive observation, so
    a provider returning a single string should not make an otherwise usable
    decision report fail schema validation.
    """

    if value in (None, "", [], {}):
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item or "").strip()]
    return []


def _lenient_optional_text(value: Any) -> Optional[str]:
    """Normalize narrative/display text fields without failing a report.

    The analyzer keeps machine-action fields strict in the receive-side
    trading contract.  By contrast, provider drift in explanatory text should
    not reject an otherwise valid decision: a list can become a joined
    sentence and an object can become compact JSON for display/debugging.
    """

    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, (list, tuple, set)):
        parts = [str(item).strip() for item in value if str(item or "").strip()]
        return "；".join(parts) if parts else None
    if isinstance(value, dict):
        try:
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError):
            return str(value)
    return str(value).strip() or None


def _lenient_display_metric(value: Any) -> Optional[Union[int, float, str]]:
    """Normalize non-trading dashboard metrics before Union validation.

    These fields are displayed as evidence or diagnostics.  If a provider
    accidentally returns a list/object here, the value is preserved as compact
    display text instead of failing the whole report.  Trading execution uses
    separate receive-side contract checks and does not rely on these fields.
    """

    if value is None:
        return None
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (int, float)):
        return value
    return _lenient_optional_text(value)


def _lenient_display_bool(value: Any) -> Optional[bool]:
    """Normalize display-only boolean evidence without failing a report."""

    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        key = value.strip().lower()
        if key in {"true", "1", "yes", "y", "是", "看多", "多头"}:
            return True
        if key in {"false", "0", "no", "n", "否", "看空", "空头"}:
            return False
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    return None


def _object_or_none(value: Any) -> Optional[Dict[str, Any]]:
    return value if isinstance(value, dict) else None


def _execution_metric_or_none(value: Any) -> Optional[Union[int, float, str]]:
    """Keep execution price fields fail-closed for object/array drift."""

    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, str)):
        return value
    return None


def _execution_text_or_none(value: Any) -> Optional[str]:
    """Keep execution phase fields fail-closed for object/array drift."""

    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _execution_text_list_or_none(value: Any) -> Optional[List[str]]:
    """Keep execution condition lists fail-closed for shape drift."""

    if not isinstance(value, list):
        return None
    conditions: List[str] = []
    for item in value:
        if not isinstance(item, str):
            return None
        text = item.strip()
        if not text:
            return None
        conditions.append(text)
    return conditions


class PositionAdvice(BaseModel):
    """Position advice for no-position vs has-position."""

    no_position: Optional[str] = None
    has_position: Optional[str] = None

    @field_validator("no_position", "has_position", mode="before")
    @classmethod
    def normalize_display_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)


class PositionAction(BaseModel):
    """Machine-readable action plus scenario-specific narrative advice."""

    action: Optional[str] = None
    advice: Optional[str] = None
    stop_loss: Optional[float] = Field(None, gt=0)
    target_price: Optional[float] = Field(None, gt=0)

    @field_validator("advice", mode="before")
    @classmethod
    def normalize_advice(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)


class PositionActions(BaseModel):
    empty: Optional[PositionAction] = None
    holding: Optional[PositionAction] = None


class DecisionGuardrail(BaseModel):
    """Auditable score adjustment; narrative-only overrides are invalid."""

    applied: bool = False
    reason_code: Optional[str] = None
    reason: Optional[str] = None
    raw_score: Optional[int] = Field(None, ge=0, le=100)
    adjusted_score: Optional[int] = Field(None, ge=0, le=100)
    score_delta: Optional[int] = None

    @field_validator("reason_code", "reason", mode="before")
    @classmethod
    def normalize_display_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)


class CoreConclusion(BaseModel):
    """Core conclusion block."""

    one_sentence: Optional[str] = None
    signal_type: Optional[str] = None
    time_sensitivity: Optional[str] = None
    position_advice: Optional[PositionAdvice] = None

    @field_validator("one_sentence", "signal_type", "time_sensitivity", mode="before")
    @classmethod
    def normalize_display_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)

    @field_validator("position_advice", mode="before")
    @classmethod
    def normalize_display_object(cls, value: Any) -> Optional[Dict[str, Any]]:
        return _object_or_none(value)


class TrendStatus(BaseModel):
    """Trend status."""

    ma_alignment: Optional[str] = None
    is_bullish: Optional[bool] = None
    trend_score: Optional[Union[int, float, str]] = None

    @field_validator("ma_alignment", mode="before")
    @classmethod
    def normalize_display_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)

    @field_validator("trend_score", mode="before")
    @classmethod
    def normalize_display_metric(cls, value: Any) -> Optional[Union[int, float, str]]:
        return _lenient_display_metric(value)

    @field_validator("is_bullish", mode="before")
    @classmethod
    def normalize_display_bool(cls, value: Any) -> Optional[bool]:
        return _lenient_display_bool(value)


class PricePosition(BaseModel):
    """Price position (may contain N/A strings)."""

    current_price: Optional[Union[int, float, str]] = None
    ma5: Optional[Union[int, float, str]] = None
    ma10: Optional[Union[int, float, str]] = None
    ma20: Optional[Union[int, float, str]] = None
    bias_ma5: Optional[Union[int, float, str]] = None
    bias_status: Optional[str] = None
    support_level: Optional[Union[int, float, str]] = None
    resistance_level: Optional[Union[int, float, str]] = None

    @field_validator("bias_status", mode="before")
    @classmethod
    def normalize_display_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)

    @field_validator(
        "current_price",
        "ma5",
        "ma10",
        "ma20",
        "bias_ma5",
        "support_level",
        "resistance_level",
        mode="before",
    )
    @classmethod
    def normalize_display_metric(cls, value: Any) -> Optional[Union[int, float, str]]:
        return _lenient_display_metric(value)


class VolumeAnalysis(BaseModel):
    """Volume analysis."""

    volume_ratio: Optional[Union[int, float, str]] = None
    volume_status: Optional[str] = None
    turnover_rate: Optional[Union[int, float, str]] = None
    volume_meaning: Optional[str] = None

    @field_validator("volume_status", "volume_meaning", mode="before")
    @classmethod
    def normalize_display_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)

    @field_validator("volume_ratio", "turnover_rate", mode="before")
    @classmethod
    def normalize_display_metric(cls, value: Any) -> Optional[Union[int, float, str]]:
        return _lenient_display_metric(value)


class ChipStructure(BaseModel):
    """Chip structure."""

    profit_ratio: Optional[Union[int, float, str]] = None
    avg_cost: Optional[Union[int, float, str]] = None
    concentration: Optional[Union[int, float, str]] = None
    chip_health: Optional[str] = None

    @field_validator("chip_health", mode="before")
    @classmethod
    def normalize_display_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)

    @field_validator("profit_ratio", "avg_cost", "concentration", mode="before")
    @classmethod
    def normalize_display_metric(cls, value: Any) -> Optional[Union[int, float, str]]:
        return _lenient_display_metric(value)


class DataPerspective(BaseModel):
    """Data perspective block."""

    trend_status: Optional[TrendStatus] = None
    price_position: Optional[PricePosition] = None
    volume_analysis: Optional[VolumeAnalysis] = None
    chip_structure: Optional[ChipStructure] = None

    @field_validator(
        "trend_status",
        "price_position",
        "volume_analysis",
        "chip_structure",
        mode="before",
    )
    @classmethod
    def normalize_display_blocks(cls, value: Any) -> Optional[Dict[str, Any]]:
        return _object_or_none(value)


class Intelligence(BaseModel):
    """Intelligence block."""

    latest_news: Optional[str] = None
    risk_alerts: Optional[List[str]] = None
    positive_catalysts: Optional[List[str]] = None
    earnings_outlook: Optional[str] = None
    sentiment_summary: Optional[str] = None

    @field_validator("risk_alerts", "positive_catalysts", mode="before")
    @classmethod
    def normalize_display_lists(cls, value: Any) -> List[str]:
        return _lenient_text_list(value)

    @field_validator("latest_news", "earnings_outlook", "sentiment_summary", mode="before")
    @classmethod
    def normalize_display_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)


class SniperPoints(BaseModel):
    """Sniper points (ideal_buy, stop_loss, etc.)."""

    ideal_buy: Optional[Union[str, int, float]] = None
    secondary_buy: Optional[Union[str, int, float]] = None
    stop_loss: Optional[Union[str, int, float]] = None
    take_profit: Optional[Union[str, int, float]] = None
    first_take_profit: Optional[Union[str, int, float]] = None
    final_target: Optional[Union[str, int, float]] = None
    entry_leg_schema_version: Optional[str] = None
    entry_legs: Optional[List[Dict[str, Any]]] = None
    exit_leg_schema_version: Optional[str] = None
    exit_legs: Optional[List[Dict[str, Any]]] = None

    @field_validator("entry_leg_schema_version", "exit_leg_schema_version", mode="before")
    @classmethod
    def normalize_schema_version_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)

    @field_validator(
        "ideal_buy",
        "secondary_buy",
        "stop_loss",
        "take_profit",
        "first_take_profit",
        "final_target",
        mode="before",
    )
    @classmethod
    def normalize_display_or_execution_metric(
        cls,
        value: Any,
    ) -> Optional[Union[int, float, str]]:
        return _execution_metric_or_none(value)


class PositionStrategy(BaseModel):
    """Position strategy."""

    suggested_position: Optional[str] = None
    entry_plan: Optional[str] = None
    risk_control: Optional[str] = None

    @field_validator("suggested_position", "entry_plan", "risk_control", mode="before")
    @classmethod
    def normalize_display_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)


class BattlePlan(BaseModel):
    """Battle plan block."""

    sniper_points: Optional[SniperPoints] = None
    position_strategy: Optional[PositionStrategy] = None
    action_checklist: Optional[List[str]] = None
    # Keep the explicit setup in validated/persisted reports; the execution
    # consumer validates its value without failing unrelated display fields.
    setup: Optional[Dict[str, Any]] = None

    @field_validator("action_checklist", mode="before")
    @classmethod
    def normalize_action_checklist(cls, value: Any) -> List[str]:
        return _lenient_text_list(value)

    @field_validator("sniper_points", "position_strategy", "setup", mode="before")
    @classmethod
    def normalize_plan_blocks(cls, value: Any) -> Optional[Dict[str, Any]]:
        return _object_or_none(value)


class PhaseDecision(BaseModel):
    """Market-phase-aware intraday decision guardrail output."""

    phase_context: Optional[Dict[str, Any]] = None
    entry_mode: Optional[Literal["none", "conditional", "immediate"]] = None
    action_window: Optional[str] = None
    immediate_action: Optional[str] = None
    watch_conditions: Optional[List[str]] = Field(default_factory=list)
    next_check_time: Optional[str] = None
    confidence_reason: Optional[str] = None
    data_limitations: List[str] = Field(default_factory=list)

    @field_validator("action_window", "immediate_action", "next_check_time", mode="before")
    @classmethod
    def normalize_phase_execution_text(cls, value: Any) -> Optional[str]:
        return _execution_text_or_none(value)

    @field_validator("entry_mode", mode="before")
    @classmethod
    def normalize_entry_mode(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        if not isinstance(value, str):
            return value
        text = value.strip().lower()
        return text or None

    @field_validator("confidence_reason", mode="before")
    @classmethod
    def normalize_phase_display_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)

    @field_validator("phase_context", mode="before")
    @classmethod
    def normalize_phase_context(cls, value: Any) -> Optional[Dict[str, Any]]:
        return value if isinstance(value, dict) else None

    @field_validator("watch_conditions", mode="before")
    @classmethod
    def normalize_phase_watch_conditions(cls, value: Any) -> Optional[List[str]]:
        return _execution_text_list_or_none(value)

    @field_validator("data_limitations", mode="before")
    @classmethod
    def normalize_phase_display_lists(cls, value: Any) -> List[str]:
        return _lenient_text_list(value)


class SignalAttribution(BaseModel):
    """Signal attribution analysis - explains what factors contributed most to the recommendation."""

    technical_indicators: Optional[Union[int, float, str]] = None
    news_sentiment: Optional[Union[int, float, str]] = None
    fundamentals: Optional[Union[int, float, str]] = None
    market_conditions: Optional[Union[int, float, str]] = None
    strongest_bullish_signal: Optional[str] = None
    strongest_bearish_signal: Optional[str] = None

    @field_validator("strongest_bullish_signal", "strongest_bearish_signal", mode="before")
    @classmethod
    def normalize_display_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)

    @field_validator(
        "technical_indicators",
        "news_sentiment",
        "fundamentals",
        "market_conditions",
        mode="before",
    )
    @classmethod
    def normalize_display_metric(cls, value: Any) -> Optional[Union[int, float, str]]:
        return _lenient_display_metric(value)

    @model_validator(mode='after')
    def validate_and_normalize_contributions(self) -> 'SignalAttribution':
        """Validate and normalize contribution weights.

        - Try to convert string values to numbers
        - Clamp values to 0-100
        - Normalize non-zero sum to 100 if all four values are valid numbers
        - Preserve all-zero as "no effective signal"
        - Set invalid values to None
        """
        contrib_fields = ['technical_indicators', 'news_sentiment', 'fundamentals', 'market_conditions']
        values = {}

        for field in contrib_fields:
            val = getattr(self, field)
            if val is None:
                values[field] = None
                continue

            # Try to convert string to number
            if isinstance(val, str):
                # Handle "N/A", "null", etc.
                if val.strip().upper() in ('N/A', 'NULL', 'NONE', ''):
                    values[field] = None
                    continue
                # Handle "70%" or "70"
                try:
                    # Remove % sign and convert
                    cleaned = val.replace('%', '').strip()
                    val = float(cleaned)
                except (ValueError, AttributeError):
                    values[field] = None
                    continue

            # Ensure it's a number
            try:
                val = float(val)
            except (TypeError, ValueError):
                values[field] = None
                continue

            if not math.isfinite(val):
                values[field] = None
                continue

            # Clamp to 0-100
            if val < 0:
                val = 0
            if val > 100:
                val = 100

            values[field] = val

        # Normalize to sum = 100 if all values are valid and non-zero
        valid_values = {k: v for k, v in values.items() if v is not None}
        if len(valid_values) == 4:
            total = sum(valid_values.values())
            if total > 0:
                # Normalize non-zero sum to 100
                for field in contrib_fields:
                    if values[field] is not None:
                        values[field] = round(values[field] * 100 / total)

                # Adjust rounding errors to keep non-zero sums at 100
                final_sum = sum(values[f] for f in contrib_fields)
                if final_sum != 100:
                    # Add/subtract the difference to/from the first non-zero value
                    diff = 100 - final_sum
                    for field in contrib_fields:
                        if values[field] > 0:
                            values[field] += diff
                            break

        # Update the model fields
        for field in contrib_fields:
            setattr(self, field, values[field])

        return self


class Dashboard(BaseModel):
    """Dashboard block."""

    model_config = ConfigDict(extra="allow")

    core_conclusion: Optional[CoreConclusion] = None
    data_perspective: Optional[DataPerspective] = None
    intelligence: Optional[Intelligence] = None
    battle_plan: Optional[BattlePlan] = None
    phase_decision: Optional[PhaseDecision] = None
    signal_attribution: Optional[SignalAttribution] = None

    @field_validator(
        "core_conclusion",
        "data_perspective",
        "intelligence",
        "battle_plan",
        "phase_decision",
        "signal_attribution",
        mode="before",
    )
    @classmethod
    def normalize_display_blocks(cls, value: Any) -> Optional[Dict[str, Any]]:
        return value if isinstance(value, dict) else None


class AnalysisReportSchema(BaseModel):
    """
    Top-level schema for LLM report JSON.
    Aligns with SYSTEM_PROMPT output format.
    """

    model_config = ConfigDict(extra="allow")  # Allow extra fields from LLM

    report_schema_version: Optional[str] = None
    stock_name: Optional[str] = None
    sentiment_score: Optional[int] = Field(None, ge=0, le=100)
    raw_score: Optional[int] = Field(None, ge=0, le=100)
    adjusted_score: Optional[int] = Field(None, ge=0, le=100)
    trend_prediction: Optional[str] = None
    operation_advice: Optional[str] = None
    action: Optional[str] = None
    action_label: Optional[str] = None
    decision_type: Optional[str] = None
    confidence_level: Optional[str] = None
    guardrail: Optional[DecisionGuardrail] = None
    limitations: List[str] = Field(default_factory=list)
    position_actions: Optional[PositionActions] = None

    dashboard: Optional[Dashboard] = None

    analysis_summary: Optional[str] = None
    key_points: Optional[str] = None
    risk_warning: Optional[str] = None
    buy_reason: Optional[str] = None

    trend_analysis: Optional[str] = None
    short_term_outlook: Optional[str] = None
    medium_term_outlook: Optional[str] = None
    technical_analysis: Optional[str] = None
    ma_analysis: Optional[str] = None
    volume_analysis: Optional[str] = None
    pattern_analysis: Optional[str] = None
    fundamental_analysis: Optional[str] = None
    sector_position: Optional[str] = None
    company_highlights: Optional[str] = None
    news_summary: Optional[str] = None
    market_sentiment: Optional[str] = None
    hot_topics: Optional[str] = None

    search_performed: Optional[bool] = None
    data_sources: Optional[str] = None

    @field_validator("limitations", mode="before")
    @classmethod
    def normalize_limitations(cls, value: Any) -> List[str]:
        return _lenient_text_list(value)

    @field_validator("search_performed", mode="before")
    @classmethod
    def normalize_search_performed(cls, value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            key = value.strip().lower()
            if key in {"true", "yes", "1", "是", "已检索"}:
                return True
            if key in {"false", "no", "0", "否", "未检索"}:
                return False
        return None

    @field_validator(
        "stock_name",
        "trend_prediction",
        "operation_advice",
        "action_label",
        "decision_type",
        "analysis_summary",
        "key_points",
        "risk_warning",
        "buy_reason",
        "trend_analysis",
        "short_term_outlook",
        "medium_term_outlook",
        "technical_analysis",
        "ma_analysis",
        "volume_analysis",
        "pattern_analysis",
        "fundamental_analysis",
        "sector_position",
        "company_highlights",
        "news_summary",
        "market_sentiment",
        "hot_topics",
        "data_sources",
        mode="before",
    )
    @classmethod
    def normalize_non_trading_text(cls, value: Any) -> Optional[str]:
        return _lenient_optional_text(value)
