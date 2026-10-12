# -*- coding: utf-8 -*-
"""Canonical score-to-decision scale shared by reports and DecisionSignal."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional


CANONICAL_DECISION_SCALE_VERSION = "decision-scale-v1"


@dataclass(frozen=True)
class DecisionScaleBand:
    min_score: int
    max_score: int
    signal_key: str
    action: str
    decision_type: str
    label_zh: str
    description_zh: str


CANONICAL_DECISION_SCALE: tuple[DecisionScaleBand, ...] = (
    DecisionScaleBand(80, 100, "strong_buy", "buy", "buy", "强烈买入", "高胜率机会，可执行买入/加仓计划"),
    DecisionScaleBand(60, 79, "buy", "buy", "buy", "买入", "偏积极机会，允许少量待确认项"),
    DecisionScaleBand(40, 59, "watch", "watch", "hold", "观望", "信号分歧或确认不足，等待触发条件"),
    DecisionScaleBand(20, 39, "reduce", "reduce", "sell", "减仓", "风险明显抬升，优先降低暴露"),
    DecisionScaleBand(0, 19, "sell", "sell", "sell", "卖出", "趋势或风险显著恶化，优先退出"),
)


CANONICAL_DECISION_SCALE_PROMPT_ZH = """## Canonical 评分与动作口径

- `raw_score` 是模型原始评分；`adjusted_score` 是最终评分；兼容字段 `sentiment_score` 必须等于 `adjusted_score`。
- 最终 `operation_advice`、`action_label`、三态 `decision_type` 必须全部由最终 `action` 派生，不得分别生成互相矛盾的值。
- 80-100：强烈买入，`action=buy`，`decision_type=buy`。
- 60-79：买入，`action=buy`，`decision_type=buy`。
- 40-59：观望，`action=watch`，`decision_type=hold`。
- 20-39：减仓，`action=reduce`，`decision_type=sell`。
- 0-19：卖出，`action=sell`，`decision_type=sell`。
- `decision_type` 只保留 `buy|hold|sell` 兼容统计；更细建议必须写入 `action`。
- 可选数据缺失只能写入 `limitations` 并降低 `confidence_level`，禁止因此修改评分或动作。
- 只有 `guardrail.applied=true` 且同时提供明确 `reason_code`、`reason`、`raw_score`、`adjusted_score`、`score_delta` 时，才允许调整评分；`score_delta` 必须等于 adjusted_score-raw_score，最终 action 必须与 adjusted_score 区间一致。
- 同时输出 `position_actions.empty` 与 `position_actions.holding`，每项包含 `action` 和 `advice`；`holding` 可在证据充分时额外输出正数 `stop_loss` 与 `target_price`，无法可靠给出时必须省略，不得用顶层入场止损/止盈补齐；通用顶层 `action` 仍表达方向判断。"""


def normalize_score(value: Any) -> Optional[int]:
    """Return a bounded integer score when possible."""

    try:
        score = int(float(value))
    except (TypeError, ValueError):
        return None
    if 0 <= score <= 100:
        return score
    return None


def decision_band_for_score(value: Any) -> Optional[DecisionScaleBand]:
    """Return the canonical decision band for a 0-100 score."""

    score = normalize_score(value)
    if score is None:
        return None
    for band in CANONICAL_DECISION_SCALE:
        if band.min_score <= score <= band.max_score:
            return band
    return None


def signal_key_for_score(value: Any) -> Optional[str]:
    band = decision_band_for_score(value)
    return band.signal_key if band else None


def action_for_score(value: Any) -> Optional[str]:
    band = decision_band_for_score(value)
    return band.action if band else None


def decision_type_for_score(value: Any) -> Optional[str]:
    band = decision_band_for_score(value)
    return band.decision_type if band else None


def score_band_metadata(value: Any) -> dict[str, Any]:
    """Return stable metadata for persistence and diagnostics."""

    score = normalize_score(value)
    band = decision_band_for_score(score)
    if score is None or band is None:
        return {}
    return {
        "scale_version": CANONICAL_DECISION_SCALE_VERSION,
        "score": score,
        "score_band": f"{band.min_score}-{band.max_score}",
        "signal_key": band.signal_key,
        "canonical_action": band.action,
        "canonical_decision_type": band.decision_type,
    }


def extract_decision_guardrail_reason(payload: Any) -> Optional[str]:
    """Extract an applied score/action guardrail reason from a result payload."""

    data = payload if isinstance(payload, Mapping) else {}
    dashboard = data.get("dashboard") if isinstance(data.get("dashboard"), Mapping) else {}
    calibration = (
        dashboard.get("decision_score_calibration")
        if isinstance(dashboard.get("decision_score_calibration"), Mapping)
        else {}
    )
    stability = (
        dashboard.get("decision_stability")
        if isinstance(dashboard.get("decision_stability"), Mapping)
        else {}
    )
    metadata = data.get("metadata") if isinstance(data.get("metadata"), Mapping) else {}

    stability_applied = stability.get("applied")
    include_stability_reason = stability_applied not in (False, 0, "0", "false", "False")
    candidates = [
        data.get("guardrail_reason"),
        data.get("downgrade_reason"),
        data.get("decision_score_guardrail_reason"),
        metadata.get("guardrail_reason"),
        metadata.get("downgrade_reason"),
        calibration.get("guardrail_reason"),
        calibration.get("downgrade_reason"),
    ]
    if include_stability_reason:
        candidates.extend(
            [
                stability.get("guardrail_reason"),
                stability.get("downgrade_reason"),
                stability.get("reason"),
            ]
        )

    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            return text
    return None


def score_action_conflicts_without_guardrail(
    *,
    score: Any,
    action: Any,
    guardrail_reason: Any = None,
) -> bool:
    """Return True when a neutral action conflicts with a directional score."""

    if str(guardrail_reason or "").strip():
        return False
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"hold", "watch"}:
        return False
    score_action = action_for_score(score)
    return score_action in {"buy", "reduce", "sell"}
