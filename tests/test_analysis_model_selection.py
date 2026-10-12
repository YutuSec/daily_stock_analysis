from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from api.v1.endpoints.analysis import trigger_analysis
from api.v1.schemas.analysis import AnalyzeRequest
from src.config import Config
from src.services.analysis_model_service import (
    build_model_scoped_config,
    list_analysis_models,
    validate_analysis_models,
)
from src.services.task_queue import AnalysisTaskQueue


def _config() -> Config:
    return Config(
        litellm_model="deepseek",
        litellm_fallback_models=["claude"],
        llm_model_list=[
            {"model_name": "deepseek", "litellm_params": {"model": "deepseek/deepseek-chat"}},
            {"model_name": "claude", "litellm_params": {"model": "anthropic/claude-sonnet"}},
        ],
    )


def test_model_scoped_config_disables_cross_model_fallback_without_mutating_global_config():
    config = _config()

    assert list_analysis_models(config) == ["deepseek", "claude"]
    assert validate_analysis_models(config, ["claude", "claude"]) == ["claude"]

    scoped = build_model_scoped_config(config, "claude")

    assert scoped is not config
    assert scoped.litellm_model == "claude"
    assert scoped.agent_litellm_model == "claude"
    assert scoped.litellm_fallback_models == []
    assert scoped.generation_fallback_backend == ""
    assert config.litellm_model == "deepseek"
    assert config.litellm_fallback_models == ["claude"]


def test_unknown_request_model_is_rejected():
    with pytest.raises(ValueError, match="未配置"):
        validate_analysis_models(_config(), ["unknown"])


def test_multi_model_request_accepts_more_than_two_distinct_models():
    request = AnalyzeRequest(
        stock_code="600519",
        async_mode=True,
        models=["deepseek", "claude", "gemini"],
    )

    assert request.models == ["deepseek", "claude", "gemini"]


def test_multi_model_request_rejects_duplicate_models():
    with pytest.raises(ValueError, match="不同的非空模型"):
        AnalyzeRequest(
            stock_code="600519",
            async_mode=True,
            models=["deepseek", "deepseek"],
        )


def test_task_queue_submits_one_task_per_model_and_deduplicates_by_model():
    original_instance = AnalysisTaskQueue._instance
    AnalysisTaskQueue._instance = None
    try:
        queue = AnalysisTaskQueue(max_workers=1)
        executor = MagicMock()
        executor.submit.return_value = Future()
        queue._executor = executor

        accepted, duplicates = queue.submit_tasks_batch(
            ["600519"],
            models=["deepseek", "claude"],
        )

        assert duplicates == []
        assert [task.model for task in accepted] == ["deepseek", "claude"]
        assert [call.args[-1] for call in executor.submit.call_args_list] == ["deepseek", "claude"]

        accepted_again, duplicates_again = queue.submit_tasks_batch(
            ["600519.SH"],
            models=["deepseek", "claude"],
        )
        assert accepted_again == []
        assert [item.model for item in duplicates_again] == ["deepseek", "claude"]
    finally:
        queue = AnalysisTaskQueue._instance
        if queue is not None and queue is not original_instance:
            executor_instance = getattr(queue, "_executor", None)
            if executor_instance is not None and hasattr(executor_instance, "shutdown"):
                executor_instance.shutdown(wait=False)
        AnalysisTaskQueue._instance = original_instance


def test_async_api_returns_batch_response_for_one_stock_and_two_models():
    queue = MagicMock()
    queue.submit_tasks_batch.return_value = (
        [
            SimpleNamespace(task_id="deepseek-task", trace_id="deepseek-task", stock_code="600519", analysis_phase="auto", model="deepseek"),
            SimpleNamespace(task_id="claude-task", trace_id="claude-task", stock_code="600519", analysis_phase="auto", model="claude"),
        ],
        [],
    )
    request = AnalyzeRequest(
        stock_code="600519",
        async_mode=True,
        models=["deepseek", "claude"],
    )

    with patch("api.v1.endpoints.analysis.get_task_queue", return_value=queue):
        response = trigger_analysis(request=request, config=_config())

    assert response.status_code == 202
    queue.submit_tasks_batch.assert_called_once()
    assert queue.submit_tasks_batch.call_args.kwargs["models"] == ["deepseek", "claude"]
    assert b'"accepted"' in response.body
    assert b'"model":"deepseek"' in response.body
    assert b'"model":"claude"' in response.body
