# -*- coding: utf-8 -*-
"""Request-scoped model selection helpers for stock analysis."""

from __future__ import annotations

import copy
from typing import Iterable, List

from src.config import Config, get_configured_llm_models
from src.llm.backend_registry import LITELLM_BACKEND_ID


def list_analysis_models(config: Config) -> List[str]:
    """Return configured analysis route names without exposing credentials."""
    candidates: List[str] = list(
        get_configured_llm_models(getattr(config, "llm_model_list", []) or [])
    )
    candidates.extend(
        [
            str(getattr(config, "litellm_model", "") or "").strip(),
            *[
                str(model or "").strip()
                for model in (getattr(config, "litellm_fallback_models", []) or [])
            ],
        ]
    )

    models: List[str] = []
    seen = set()
    for model in candidates:
        if not model or model.startswith("__legacy_") or model in seen:
            continue
        seen.add(model)
        models.append(model)
    return models


def validate_analysis_models(config: Config, requested: Iterable[str]) -> List[str]:
    """Validate and de-duplicate request model routes while preserving order."""
    available = list_analysis_models(config)
    available_set = set(available)
    models: List[str] = []
    seen = set()
    for raw_model in requested:
        model = str(raw_model or "").strip()
        if not model or model in seen:
            continue
        if model not in available_set:
            raise ValueError(
                f"模型 {model} 未配置；可用模型: {', '.join(available) or '无'}"
            )
        seen.add(model)
        models.append(model)
    return models


def build_model_scoped_config(config: Config, model: str) -> Config:
    """Copy config and lock all analysis generation paths to one model route."""
    selected = validate_analysis_models(config, [model])
    if not selected:
        raise ValueError("请求模型不能为空")

    scoped = copy.copy(config)
    scoped.generation_backend = LITELLM_BACKEND_ID
    scoped.generation_fallback_backend = ""
    scoped.litellm_model = selected[0]
    scoped.litellm_fallback_models = []
    scoped.agent_litellm_model = selected[0]
    return scoped
