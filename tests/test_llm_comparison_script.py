from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.config import Config, LITELLM_BACKEND_ID
import pytest

from scripts.run_llm_comparison import _comparison_models, _run_model, _safe_name


class _FakePipeline:
    instances = []

    def __init__(self, *, config, **kwargs):
        self.config = config
        self.kwargs = kwargs
        self.run_kwargs = None
        self.__class__.instances.append(self)

    def run(self, **kwargs):
        self.run_kwargs = kwargs
        return [
            SimpleNamespace(
                code="002714",
                name="牧原股份",
                model_used=self.config.litellm_model,
                operation_advice="买入",
                sentiment_score=70,
                success=True,
                query_id="analysis-query-id",
            )
        ]

    def _generate_aggregate_report(self, results, report_type):
        return f"# {results[0].model_used}\n"


def test_safe_name_removes_model_path_separators() -> None:
    assert _safe_name("openai/gpt-5:test") == "openai_gpt-5_test"


def test_comparison_models_accepts_every_configured_route() -> None:
    assert _comparison_models("provider/a,provider/b,provider/c") == [
        "provider/a",
        "provider/b",
        "provider/c",
    ]


@pytest.mark.parametrize("raw", ["provider/a", "provider/a,provider/a"])
def test_comparison_models_requires_distinct_routes(raw: str) -> None:
    with pytest.raises(ValueError, match="至少两个不同"):
        _comparison_models(raw)


def test_run_model_disables_fallback_and_writes_unique_report(tmp_path: Path) -> None:
    _FakePipeline.instances = []
    config = Config(
        stock_list=["002714"],
        litellm_model="provider/original",
        litellm_fallback_models=["provider/fallback"],
        agent_mode=True,
    )

    with patch("scripts.run_llm_comparison.StockAnalysisPipeline", _FakePipeline):
        summary = _run_model(
            base_config=config,
            model="provider/model-a",
            variant_index=0,
            stock_codes=["002714"],
            reference_time=SimpleNamespace(),
            run_id="20260812T000000Z",
            output_dir=tmp_path,
            workers=1,
        )

    pipeline = _FakePipeline.instances[0]
    assert pipeline.config.litellm_model == "provider/model-a"
    assert pipeline.config.litellm_fallback_models == []
    assert pipeline.config.generation_backend == LITELLM_BACKEND_ID
    assert pipeline.config.agent_mode is False
    assert pipeline.run_kwargs["send_notification"] is False
    assert pipeline.run_kwargs["save_local_report"] is False
    assert summary["analysis_query_ids"] == ["analysis-query-id"]
    assert Path(summary["report_path"]).read_text(encoding="utf-8") == "# provider/model-a\n"
