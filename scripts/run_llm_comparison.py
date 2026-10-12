#!/usr/bin/env python3
"""Run the same local stock-analysis batch with multiple isolated LLM routes."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import LITELLM_BACKEND_ID, Config, get_config  # noqa: E402
from src.core.pipeline import StockAnalysisPipeline  # noqa: E402
from src.enums import ReportType  # noqa: E402

logger = logging.getLogger(__name__)


def _csv_values(raw: str) -> List[str]:
    return [part.strip() for part in str(raw or "").split(",") if part.strip()]


def _safe_name(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return normalized.strip("._-") or "model"


def _comparison_models(raw: str) -> List[str]:
    models = _csv_values(raw)
    if len(models) < 2 or len(set(models)) != len(models):
        raise ValueError("--models 必须提供至少两个不同的模型/路由名")
    return models


def _report_type(config: Config) -> ReportType:
    value = str(getattr(config, "report_type", "simple") or "simple").lower()
    if value == "full":
        return ReportType.FULL
    if value == "brief":
        return ReportType.BRIEF
    return ReportType.SIMPLE


def _run_model(
    *,
    base_config: Config,
    model: str,
    variant_index: int,
    stock_codes: List[str],
    reference_time: datetime,
    run_id: str,
    output_dir: Path,
    workers: int,
) -> Dict[str, Any]:
    # Comparison runs must not silently cross over to the other model.
    config = replace(
        base_config,
        generation_backend=LITELLM_BACKEND_ID,
        generation_fallback_backend=LITELLM_BACKEND_ID,
        litellm_model=model,
        litellm_fallback_models=[],
        agent_mode=False,
        agent_skills=["all"],
        single_stock_notify=False,
    )
    query_id = f"cmp_{run_id}_{variant_index}_{uuid.uuid4().hex[:8]}"
    pipeline = StockAnalysisPipeline(
        config=config,
        max_workers=max(1, workers),
        query_id=query_id,
        query_source="cli_llm_comparison",
        daily_market_context_allow_generate=False,
    )
    results = pipeline.run(
        stock_codes=stock_codes,
        dry_run=False,
        send_notification=False,
        current_time=reference_time,
        save_local_report=False,
    )

    report_path = output_dir / f"{variant_index + 1:02d}_{_safe_name(model)}.md"
    if results:
        report = pipeline._generate_aggregate_report(results, _report_type(config))
        report_path.write_text(report, encoding="utf-8")

    return {
        "configured_model": model,
        "actual_models": sorted({result.model_used or model for result in results}),
        "run_query_id": query_id,
        "analysis_query_ids": sorted({result.query_id for result in results if result.query_id}),
        "report_path": str(report_path) if results else None,
        "result_count": len(results),
        "stocks": [
            {
                "code": result.code,
                "name": result.name,
                "model_used": result.model_used,
                "operation_advice": result.operation_advice,
                "sentiment_score": result.sentiment_score,
                "success": result.success,
            }
            for result in results
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="并行调用多个 LLM 路由生成独立报告，并写入本地 analysis_history。",
    )
    parser.add_argument("--models", required=True, help="至少两个模型/路由名，逗号分隔")
    parser.add_argument("--stocks", help="股票代码，逗号分隔；默认读取 STOCK_LIST")
    parser.add_argument("--workers", type=int, default=1, help="每个模型内部的股票并发数（默认 1）")
    parser.add_argument(
        "--output-dir",
        help="输出目录；默认 reports/llm_comparison/<运行批次>",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        models = _comparison_models(args.models)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if args.workers < 1:
        raise SystemExit("--workers 必须大于等于 1")

    config = get_config()
    stock_codes = _csv_values(args.stocks) if args.stocks else list(config.stock_list)
    if not stock_codes:
        raise SystemExit("未提供 --stocks，且 STOCK_LIST 为空")

    reference_time = datetime.now(timezone.utc)
    run_id = reference_time.strftime("%Y%m%dT%H%M%SZ")
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else PROJECT_ROOT / "reports" / "llm_comparison" / run_id
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    summaries: List[Dict[str, Any]] = []
    failures: List[Dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=len(models), thread_name_prefix="llm-compare") as executor:
        futures = {
            executor.submit(
                _run_model,
                base_config=config,
                model=model,
                variant_index=index,
                stock_codes=stock_codes,
                reference_time=reference_time,
                run_id=run_id,
                output_dir=output_dir,
                workers=args.workers,
            ): (index, model)
            for index, model in enumerate(models)
        }
        for future in as_completed(futures):
            index, model = futures[future]
            try:
                summaries.append(future.result())
            except Exception as exc:
                logger.exception("模型比较运行失败: %s", model)
                failures.append({"configured_model": model, "error": str(exc)})

    summaries.sort(key=lambda item: models.index(item["configured_model"]))
    manifest = {
        "run_id": run_id,
        "reference_time": reference_time.isoformat(),
        "stock_codes": stock_codes,
        "models": summaries,
        "failures": failures,
        "notes": [
            "各模型流水线使用同一冻结时间、同一股票列表，并禁用跨模型 fallback。",
            "行情/新闻由各并行流水线分别读取；第三方实时响应差异仍可能影响对比输入。",
            "回测筛选应使用 actual_models 中记录的 model_used。",
        ],
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    print(f"比较批次: {run_id}")
    print(f"输出目录: {output_dir}")
    for summary in summaries:
        print(
            f"- {summary['configured_model']}: {summary['result_count']} 份分析, "
            f"报告={summary['report_path'] or '未生成'}"
        )
    for failure in failures:
        print(f"- {failure['configured_model']}: 失败 ({failure['error']})", file=sys.stderr)

    return 1 if failures or any(item["result_count"] == 0 for item in summaries) else 0


if __name__ == "__main__":
    raise SystemExit(main())
