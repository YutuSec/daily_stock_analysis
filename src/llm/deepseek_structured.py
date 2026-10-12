# -*- coding: utf-8 -*-
"""Official DeepSeek structured report generation helpers.

This module intentionally bypasses LiteLLM tool-call translation for stock
report submission.  A stock report is final structured output, not an external
tool action.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from src.agent.provider_trace import resolved_model_provider_identity
from src.config import Config, get_api_keys_for_model
from src.llm.generation_params import resolve_litellm_wire_model
from src.llm.usage import normalize_litellm_usage


DEEPSEEK_OFFICIAL_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_RESPONSES_SUPPORTED_MODEL_PREFIX = "deepseek-v4-"
# Official model limit: 384 Ki tokens. DeepSeek counts reasoning and visible
# output together, so the report route uses the provider ceiling directly.
DEEPSEEK_REPORT_MAX_OUTPUT_TOKENS = 384 * 1024


class DeepSeekStructuredGenerationError(RuntimeError):
    """DeepSeek official structured-output route failed."""


@dataclass(frozen=True)
class DeepSeekStructuredRoute:
    """Resolved official DeepSeek route details."""

    route_model: str
    wire_model: str
    api_model: str
    api_key: str
    base_url: str
    source: str


def _field(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _base_url_is_official_deepseek(base_url: Optional[str]) -> bool:
    raw = (base_url or DEEPSEEK_OFFICIAL_BASE_URL).strip() or DEEPSEEK_OFFICIAL_BASE_URL
    parsed = urlparse(raw)
    return parsed.scheme in {"http", "https"} and parsed.netloc.lower() == "api.deepseek.com"


def _canonical_base_url(base_url: Optional[str]) -> str:
    raw = (base_url or DEEPSEEK_OFFICIAL_BASE_URL).strip() or DEEPSEEK_OFFICIAL_BASE_URL
    return raw.rstrip("/")


def _api_model_name(wire_model: str) -> str:
    model = (wire_model or "").strip()
    if model.startswith("deepseek/"):
        return model.split("/", 1)[1]
    return model


def _supports_official_responses_schema(api_model: str) -> bool:
    """Return whether the documented DeepSeek Responses API supports this model."""

    return (api_model or "").strip().startswith(DEEPSEEK_RESPONSES_SUPPORTED_MODEL_PREFIX)


def _model_list(config: Config) -> List[Dict[str, Any]]:
    return list(getattr(config, "llm_model_list", []) or [])


def _deployment_candidates(config: Config, route_model: str) -> Iterable[Dict[str, Any]]:
    candidates = {route_model}
    model_list = _model_list(config)
    wire = resolve_litellm_wire_model(route_model, model_list)
    if wire:
        candidates.add(wire)
    for deployment in model_list:
        if not isinstance(deployment, dict):
            continue
        name = str(deployment.get("model_name") or "").strip()
        params = deployment.get("litellm_params") or {}
        param_model = str(params.get("model") or "").strip() if isinstance(params, dict) else ""
        if name in candidates or param_model in candidates:
            yield deployment


def resolve_official_deepseek_structured_route(
    model: str,
    config: Config,
) -> Optional[DeepSeekStructuredRoute]:
    """Return route details only for official DeepSeek API models.

    Non-official DeepSeek-compatible gateways are deliberately excluded because
    the Responses API shape and thinking controls are provider-specific.
    """

    route_model = (model or "").strip()
    if not route_model:
        return None

    model_list = _model_list(config)
    wire_model, provider = resolved_model_provider_identity(route_model, model_list)
    if provider != "deepseek":
        return None
    api_model = _api_model_name(wire_model)
    if not _supports_official_responses_schema(api_model):
        return None

    for deployment in _deployment_candidates(config, route_model):
        params = deployment.get("litellm_params") or {}
        if not isinstance(params, dict):
            continue
        base_url = params.get("api_base") or params.get("base_url") or DEEPSEEK_OFFICIAL_BASE_URL
        if not _base_url_is_official_deepseek(str(base_url)):
            return None
        api_key = str(params.get("api_key") or "").strip()
        if api_key:
            return DeepSeekStructuredRoute(
                route_model=route_model,
                wire_model=wire_model,
                api_model=api_model,
                api_key=api_key,
                base_url=_canonical_base_url(str(base_url)),
                source="model_list",
            )

    keys = get_api_keys_for_model(route_model, config)
    if not keys:
        return None
    return DeepSeekStructuredRoute(
        route_model=route_model,
        wire_model=wire_model,
        api_model=api_model,
        api_key=keys[0],
        base_url=DEEPSEEK_OFFICIAL_BASE_URL,
        source="legacy_env",
    )


def build_responses_json_schema_format(
    *,
    name: str,
    schema: Dict[str, Any],
) -> Dict[str, Any]:
    """Build DeepSeek/OpenAI-compatible Responses structured-output format."""

    return {
        "format": {
            "type": "json_schema",
            "name": name,
            "schema": schema,
        }
    }


def extract_responses_text(response: Any) -> str:
    """Extract only the final assistant message from a Responses object.

    DeepSeek thinking responses place ``reasoning`` items before the final
    ``message`` item.  Never concatenate ``reasoning_text`` into report JSON.
    """

    output_text = _field(response, "output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    parts: List[str] = []
    for item in _field(response, "output") or []:
        if _field(item, "type") != "message":
            continue
        for content in _field(item, "content") or []:
            if _field(content, "type") != "output_text":
                continue
            text = _field(content, "text")
            if isinstance(text, str):
                parts.append(text)
    return "".join(parts).strip()


def responses_diagnostics(response: Any, *, output_chars: int) -> Dict[str, Any]:
    """Return non-content completion metadata safe for operational logs."""

    incomplete = _field(response, "incomplete_details")
    error = _field(response, "error")
    usage = _field(response, "usage")
    output_details = _field(usage, "output_tokens_details")
    return {
        "status": str(_field(response, "status") or "").strip().lower() or None,
        "incomplete_reason": str(_field(incomplete, "reason") or "").strip().lower() or None,
        "error_code": str(_field(error, "code") or "").strip() or None,
        "input_tokens": _field(usage, "input_tokens"),
        "output_tokens": _field(usage, "output_tokens"),
        "reasoning_tokens": _field(output_details, "reasoning_tokens"),
        "output_chars": max(0, int(output_chars)),
    }


def chat_completion_finish_reason(response: Any) -> Optional[str]:
    """Return the first Chat Completion choice finish reason, if present."""

    choices = _field(response, "choices") or []
    if not choices:
        return None
    reason = str(_field(choices[0], "finish_reason") or "").strip().lower()
    return reason or None


def extract_chat_completion_text(response: Any) -> str:
    """Extract text from an OpenAI-compatible chat completion fallback."""

    choices = _field(response, "choices") or []
    if not choices:
        return ""
    choice = choices[0]
    message = _field(choice, "message")
    content = _field(message, "content") if message is not None else _field(choice, "content")
    if isinstance(content, str):
        return content.strip()
    return ""


def normalize_deepseek_usage(response: Any, *, model: str, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Normalize Responses or Chat usage into the repository usage shape."""

    raw_usage = _field(response, "usage")
    if raw_usage is None:
        usage = normalize_litellm_usage(None, model=model, provider="deepseek")
        usage.setdefault("provider", "deepseek")
        usage.setdefault("model", model)
        return usage
    if not isinstance(raw_usage, dict):
        raw_usage = {
            key: _field(raw_usage, key)
            for key in (
                "prompt_tokens",
                "completion_tokens",
                "total_tokens",
                "input_tokens",
                "output_tokens",
            )
            if _field(raw_usage, key) is not None
        }
    mapped = dict(raw_usage)
    if "prompt_tokens" not in mapped and mapped.get("input_tokens") is not None:
        mapped["prompt_tokens"] = mapped.get("input_tokens")
    if "completion_tokens" not in mapped and mapped.get("output_tokens") is not None:
        mapped["completion_tokens"] = mapped.get("output_tokens")
    if "total_tokens" not in mapped:
        prompt = mapped.get("prompt_tokens")
        completion = mapped.get("completion_tokens")
        if isinstance(prompt, int) and isinstance(completion, int):
            mapped["total_tokens"] = prompt + completion
    usage = normalize_litellm_usage(mapped, model=model, provider="deepseek")
    usage.setdefault("provider", "deepseek")
    usage.setdefault("model", model)
    return usage


class OfficialDeepSeekStructuredClient:
    """Direct official DeepSeek client for stock report structured output."""

    def __init__(self, route: DeepSeekStructuredRoute, *, client: Any = None):
        self.route = route
        self._client = client

    def _openai_client(self) -> Any:
        if self._client is not None:
            return self._client
        import openai

        return openai.OpenAI(api_key=self.route.api_key, base_url=self.route.base_url)

    def create_structured_report(
        self,
        *,
        messages: List[Dict[str, Any]],
        schema: Dict[str, Any],
        max_output_tokens: int,
        temperature: float,
        timeout: Optional[float] = None,
    ) -> Any:
        kwargs: Dict[str, Any] = {
            "model": self.route.api_model,
            "input": messages,
            "text": build_responses_json_schema_format(
                name="analysis_report",
                schema=schema,
            ),
            "reasoning": {"effort": "high"},
            "max_output_tokens": max_output_tokens,
            "temperature": temperature,
        }
        if timeout is not None:
            kwargs["timeout"] = timeout
        return self._openai_client().responses.create(**kwargs)

    def create_json_fallback(
        self,
        *,
        messages: List[Dict[str, Any]],
        max_tokens: int,
        temperature: float,
        timeout: Optional[float] = None,
    ) -> Any:
        kwargs: Dict[str, Any] = {
            "model": self.route.api_model,
            "messages": messages,
            "response_format": {"type": "json_object"},
            "reasoning_effort": "high",
            "extra_body": {"thinking": {"type": "enabled"}},
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if timeout is not None:
            kwargs["timeout"] = timeout
        return self._openai_client().chat.completions.create(**kwargs)


def ensure_json_instruction(prompt: str) -> str:
    """Ensure Chat JSON fallback satisfies DeepSeek's explicit JSON instruction."""

    text = prompt or ""
    if "json" in text.lower():
        return text
    return text + "\n\n请只返回一个 JSON object，不要输出 Markdown 或解释文字。"
