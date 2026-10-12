# -*- coding: utf-8 -*-
"""API contract checks for the read-only market opportunity pool surface."""

from __future__ import annotations

import os
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from api.deps import get_config_dep
from api.app import create_app
from src.config import Config
from src.storage import (
    AnalysisHistory,
    DatabaseManager,
    MarketOpportunityAnalysisJob,
    MarketOpportunityCandidate,
    MarketOpportunityRun,
    MarketOpportunityTheme,
)


class MarketOpportunityApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {
            key: os.environ.get(key)
            for key in (
                "ENV_FILE",
                "DATABASE_PATH",
                "MARKET_OPPORTUNITY_MODE",
                "ADMIN_AUTH_ENABLED",
                "STOCK_LIST",
                "GEMINI_API_KEY",
            )
        }
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        env_path = root / ".env"
        env_path.write_text(
            "\n".join((
                "STOCK_LIST=600519",
                "GEMINI_API_KEY=test",
                "ADMIN_AUTH_ENABLED=false",
                "MARKET_OPPORTUNITY_MODE=observe",
                f"DATABASE_PATH={root / 'opportunity-api.db'}",
            )) + "\n",
            encoding="utf-8",
        )
        os.environ["ENV_FILE"] = str(env_path)
        os.environ["DATABASE_PATH"] = str(root / "opportunity-api.db")
        os.environ["MARKET_OPPORTUNITY_MODE"] = "observe"
        os.environ["ADMIN_AUTH_ENABLED"] = "false"
        os.environ["STOCK_LIST"] = "600519"
        os.environ["GEMINI_API_KEY"] = "test"
        Config.reset_instance()
        DatabaseManager.reset_instance()
        self.client = TestClient(create_app(static_dir=root / "missing-static"))

    def _override_config(self, **values) -> Config:
        config = replace(Config.get_instance(), **values)
        self.client.app.dependency_overrides[get_config_dep] = lambda: config
        return config

    def tearDown(self) -> None:
        self.client.app.dependency_overrides.clear()
        DatabaseManager.reset_instance()
        Config.reset_instance()
        for key, value in self._old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.temp_dir.cleanup()

    def _seed_run(self) -> None:
        now = datetime(2026, 8, 31, 15, 20, 0)
        with DatabaseManager.get_instance().session_scope() as session:
            report = AnalysisHistory(
                query_id="market-review-1",
                code="MARKET",
                name="A股大盘",
                report_type="market_review",
                model_used="deepseek/deepseek-v4-pro",
                sentiment_score=60,
                operation_advice="观望",
                trend_prediction="震荡",
                analysis_summary="market review",
                created_at=now,
            )
            session.add(report)
            session.flush()
            run = MarketOpportunityRun(
                run_key="cn:2026-08-31:market-review-1:policy",
                market="cn",
                trade_date=date(2026, 8, 31),
                source_report_id=report.id,
                market_regime="normal",
                mode="observe",
                status="completed",
                policy_version="opportunity-policy-v1",
                config_snapshot_json='{"max_total_candidates": 8}',
                provenance_json='{"source": "unit-test"}',
                theme_count=1,
                selected_theme_count=1,
                candidate_count=1,
                selected_candidate_count=1,
                llm_budget=16,
                started_at=now,
                completed_at=now,
                created_at=now,
                updated_at=now,
            )
            session.add(run)
            session.flush()
            theme = MarketOpportunityTheme(
                run_id=run.id,
                theme_key="concept:创新药",
                theme_type="concept",
                theme_name="创新药",
                status="selected",
                score=82.5,
                score_components_json='{"breadth": 20}',
                rejection_reasons_json="[]",
                data_quality_json='{"status": "ok"}',
                constituent_snapshot_json='{"provider": "akshare"}',
                constituent_count=80,
                eligible_constituent_count=12,
                created_at=now,
                updated_at=now,
            )
            session.add(theme)
            session.flush()
            candidate = MarketOpportunityCandidate(
                run_id=run.id,
                theme_id=theme.id,
                candidate_key="cn:sz:002173",
                market="cn",
                exchange="SZ",
                stock_code="002173",
                symbol="002173.SZ",
                stock_name="创新医疗",
                primary_theme_key=theme.theme_key,
                related_themes_json='["concept:创新药"]',
                status="selected",
                rank=1,
                score=76.0,
                score_components_json='{"relative_strength": 18}',
                rejection_reasons_json="[]",
                data_quality_json='{"status": "ok"}',
                quote_snapshot_json='{"source": "daily"}',
                as_of=date(2026, 8, 31),
                valid_until=date(2026, 9, 1),
                snapshot_hash="hash",
                created_at=now,
                updated_at=now,
            )
            session.add(candidate)
            session.flush()
            session.add(MarketOpportunityAnalysisJob(
                job_key="job:deepseek",
                run_id=run.id,
                candidate_id=candidate.id,
                model="deepseek/deepseek-v4-pro",
                status="completed",
                analysis_history_id=123,
                decision_signal_id=456,
                paper_plan_id=None,
                evidence_json=(
                    '{"origin": "market_opportunity", '
                    '"paper_reconciliation": {'
                    '"classification": "awaiting_entry_confirmation", '
                    '"signal_id": 456, '
                    '"plan_id": null, '
                    '"status": "non_executable", '
                    '"executable": false, '
                    '"confirmation_progress": {"current": 1, "required": 2}'
                    "}}"
                ),
                created_at=now,
                updated_at=now,
            ))

    def test_status_exposes_mode_models_limits_and_observe_gate(self) -> None:
        with patch(
            "src.services.market_opportunity_service.list_analysis_models",
            return_value=["deepseek/deepseek-v4-pro", "anthropic/claude-sonnet-5"],
        ):
            response = self.client.get("/api/v1/market-opportunities/status")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["mode"], "observe")
        self.assertEqual(payload["mode_source"], "environment")
        self.assertFalse(payload["mode_degraded"])
        self.assertEqual(payload["configured_models"], ["deepseek/deepseek-v4-pro", "anthropic/claude-sonnet-5"])
        self.assertFalse(payload["observe_gate"]["completed"])
        self.assertEqual(payload["limits"]["max_total_candidates"], 8)

    def test_mode_get_returns_authoritative_runtime_setting(self) -> None:
        self._override_config(market_opportunity_mode="observe")

        response = self.client.get("/api/v1/market-opportunities/mode")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["mode"], "observe")
        self.assertEqual(payload["mode_source"], "environment")
        self.assertEqual(payload["mode_version"], 0)

    def test_lists_runs_and_candidates_with_total_and_dynamic_model_states(self) -> None:
        self._seed_run()
        models = ["deepseek/deepseek-v4-pro", "anthropic/claude-sonnet-5"]
        with patch("api.v1.endpoints.market_opportunities.list_analysis_models", return_value=models):
            runs = self.client.get("/api/v1/market-opportunities/runs?page=1&limit=5")
            candidates = self.client.get("/api/v1/market-opportunities/candidates?status=selected&page=1&limit=5")

        self.assertEqual(runs.status_code, 200)
        self.assertEqual(runs.json()["total"], 1)
        self.assertEqual(runs.json()["items"][0]["source_report_id"], 1)
        self.assertEqual(runs.json()["items"][0]["health_status"], "has_candidates")

        self.assertEqual(candidates.status_code, 200)
        payload = candidates.json()
        self.assertEqual(payload["total"], 1)
        candidate = payload["items"][0]
        self.assertEqual(candidate["symbol"], "002173.SZ")
        self.assertEqual([item["model"] for item in candidate["model_states"]], models)
        self.assertEqual(candidate["model_states"][0]["status"], "completed")
        self.assertEqual(candidate["model_states"][0]["no_plan_reason"], "awaiting_entry_confirmation")
        self.assertEqual(candidate["model_states"][0]["plan_status"], "non_executable")
        self.assertIs(candidate["model_states"][0]["plan_executable"], False)
        self.assertEqual(candidate["model_states"][0]["confirmation_progress"], {"current": 1, "required": 2})
        self.assertEqual(
            candidate["model_states"][0]["paper_reconciliation"]["classification"],
            "awaiting_entry_confirmation",
        )
        self.assertEqual(candidate["model_states"][1]["status"], "missing")
        self.assertIn("awaiting_entry_confirmation", candidate["no_plan_reason"])
        self.assertIn("job_not_created", candidate["no_plan_reason"])

    def test_candidate_model_filter_keeps_missing_model_jobs_visible(self) -> None:
        self._seed_run()
        now = datetime(2026, 8, 31, 15, 25, 0)
        with DatabaseManager.get_instance().session_scope() as session:
            run = session.query(MarketOpportunityRun).one()
            theme = session.query(MarketOpportunityTheme).one()
            second = MarketOpportunityCandidate(
                run_id=run.id,
                theme_id=theme.id,
                candidate_key="cn:sh:600001",
                market="cn",
                exchange="SH",
                stock_code="600001",
                symbol="600001.SH",
                stock_name="测试股份",
                primary_theme_key=theme.theme_key,
                related_themes_json='["concept:创新药"]',
                status="selected",
                rank=2,
                score=75.0,
                score_components_json='{"relative_strength": 17}',
                rejection_reasons_json="[]",
                data_quality_json='{"status": "ok"}',
                quote_snapshot_json='{"source": "daily"}',
                as_of=date(2026, 8, 31),
                valid_until=date(2026, 9, 1),
                snapshot_hash="hash-2",
                created_at=now,
                updated_at=now,
            )
            session.add(second)
            session.flush()
            session.add(MarketOpportunityAnalysisJob(
                job_key="job:claude",
                run_id=run.id,
                candidate_id=second.id,
                model="anthropic/claude-sonnet-5",
                status="completed",
                analysis_history_id=124,
                decision_signal_id=457,
                evidence_json='{"paper_reconciliation": {"classification": "watch_not_executable", "executable": false}}',
                created_at=now,
                updated_at=now,
            ))
            run.candidate_count = 2
            run.selected_candidate_count = 2

        models = ["deepseek/deepseek-v4-pro", "anthropic/claude-sonnet-5"]
        with patch("api.v1.endpoints.market_opportunities.list_analysis_models", return_value=models):
            response = self.client.get(
                "/api/v1/market-opportunities/candidates"
                "?status=selected&model=anthropic/claude-sonnet-5&page=1&limit=5"
            )
            paged = self.client.get(
                "/api/v1/market-opportunities/candidates"
                "?status=selected&model=anthropic/claude-sonnet-5&page=1&limit=1"
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["total"], 2)
        self.assertEqual(len(payload["items"]), 2)
        self.assertEqual(payload["configured_models"], ["anthropic/claude-sonnet-5"])
        by_symbol = {item["symbol"]: item for item in payload["items"]}
        self.assertEqual(
            [state["model"] for state in by_symbol["002173.SZ"]["model_states"]],
            ["anthropic/claude-sonnet-5"],
        )
        self.assertEqual(by_symbol["002173.SZ"]["model_states"][0]["status"], "missing")
        self.assertEqual(by_symbol["002173.SZ"]["model_states"][0]["no_plan_reason"], "job_not_created")
        self.assertEqual(by_symbol["002173.SZ"]["analysis_jobs"], [])
        self.assertEqual(by_symbol["600001.SH"]["model_states"][0]["status"], "completed")
        self.assertEqual(by_symbol["600001.SH"]["analysis_jobs"][0]["model"], "anthropic/claude-sonnet-5")
        self.assertEqual(paged.status_code, 200)
        self.assertEqual(paged.json()["total"], 2)
        self.assertEqual(len(paged.json()["items"]), 1)

    def test_model_states_expose_intent_research_and_advice_and_run_counts_llm_jobs(self) -> None:
        self._seed_run()
        now = datetime(2026, 8, 31, 15, 30, 0)
        model = "deepseek/deepseek-v4-pro"
        with DatabaseManager.get_instance().session_scope() as session:
            run = session.query(MarketOpportunityRun).one()
            theme = session.query(MarketOpportunityTheme).one()
            run_id = run.id

            def add(code: str, rank: int, *, job_key: str, status: str, evidence: dict, **job_fields) -> None:
                candidate = MarketOpportunityCandidate(
                    run_id=run.id,
                    theme_id=theme.id,
                    candidate_key=f"cn:sh:{code}",
                    market="cn",
                    exchange="SH",
                    stock_code=code,
                    symbol=f"{code}.SH",
                    stock_name=f"股票{code}",
                    primary_theme_key=theme.theme_key,
                    status="selected",
                    rank=rank,
                    score=70.0,
                    as_of=date(2026, 8, 31),
                    valid_until=date(2026, 9, 1),
                    created_at=now,
                    updated_at=now,
                )
                session.add(candidate)
                session.flush()
                session.add(MarketOpportunityAnalysisJob(
                    job_key=job_key,
                    run_id=run.id,
                    candidate_id=candidate.id,
                    model=model,
                    status=status,
                    evidence_json=json.dumps(evidence),
                    created_at=now,
                    updated_at=now,
                    **job_fields,
                ))

            add("600803", 2, job_key="market-opportunity-research:600803", status="completed", evidence={
                "reused_same_day_report": False,
                "research_kind": "initial",
                "research_bundle": {
                    "bundle_id": "bundle-1",
                    "outcome": {"classification": "bullish", "action": "buy", "action_label": "买入"},
                },
                "research_review": {"status": "scheduled", "due_date": "2026-09-01", "expires_on": "2026-09-01"},
                "paper_reconciliation": {"classification": "execution_not_authorized", "no_plan_reason": "research_only"},
            })
            add("600002", 3, job_key="job:plan", status="completed", paper_plan_id=789, evidence={
                "reused_same_day_report": True,
                "paper_reconciliation": {"plan_id": 789, "item": {
                    "id": 789,
                    "status": "non_executable",
                    "executable": False,
                    "action": "watch",
                    "strategy_thesis_raw_action": "buy",
                    "strategy_thesis_transition": {
                        "raw_action": "buy",
                        "reason": "awaiting_entry_confirmation",
                        "confirmation_count": 1,
                        "evidence": {"effective_confirmation_required": 2},
                    },
                }},
            })
            add("600003", 4, job_key="job:budget", status="failed", failure_reason="research_budget_exhausted", evidence={})
            add("600004", 5, job_key="job:timeout", status="failed_retryable",
                failure_reason="sync_analysis_exception:TimeoutError:slow", evidence={})
            add("600005", 6, job_key="job:submitted", status="submitted", evidence={})
            add("600006", 7, job_key="job:pending", status="pending", evidence={})

        with patch("api.v1.endpoints.market_opportunities.list_analysis_models", return_value=[model]):
            candidates = self.client.get(f"/api/v1/market-opportunities/candidates?run_id={run_id}&page=1&limit=20")
            detail = self.client.get(f"/api/v1/market-opportunities/runs/{run_id}")

        self.assertEqual(candidates.status_code, 200)
        states = {item["symbol"]: item["model_states"][0] for item in candidates.json()["items"]}
        research = states["600803.SH"]
        self.assertEqual(research["execution_intent"], "research_only")
        self.assertEqual(research["research"]["kind"], "initial")
        self.assertEqual(research["research"]["outcome"], "bullish")
        self.assertEqual(research["research"]["bundle_id"], "bundle-1")
        self.assertEqual(research["research"]["review"]["due_date"], "2026-09-01")
        self.assertIsNone(research["research"]["review_of"])
        self.assertEqual((research["action"], research["action_label"]), ("buy", "买入"))
        paper = states["600002.SH"]
        self.assertEqual(paper["execution_intent"], "paper_candidate")
        self.assertIsNone(paper["research"])
        # 模型原始建议优先于被确认流程改写过的计划动作。
        self.assertEqual((paper["action"], paper["action_label"]), ("buy", "买入"))
        self.assertEqual(paper["confirmation_progress"]["required"], 2)
        no_plan = states["002173.SZ"]
        self.assertEqual(no_plan["execution_intent"], "paper_candidate")
        self.assertIsNone(no_plan["action"])
        self.assertIsNone(no_plan["action_label"])

        self.assertEqual(detail.status_code, 200)
        payload = detail.json()
        # 计入：已完成且非复用（002173、600803）、同步异常、已提交；不计：复用当日报告、预算不足、未提交。
        self.assertEqual(payload["llm_submitted_job_count"], 4)
        detail_states = {item["symbol"]: item["model_states"][0] for item in payload["candidates"]}
        self.assertEqual(detail_states["600803.SH"]["research"]["outcome"], "bullish")
        self.assertEqual(detail_states["600803.SH"]["action_label"], "买入")

    def test_run_list_and_detail_distinguish_source_unavailable_from_zero_candidate_health(self) -> None:
        now = datetime(2026, 8, 31, 15, 20, 0)
        with DatabaseManager.get_instance().session_scope() as session:
            report = AnalysisHistory(
                query_id="market-review-source-failed",
                code="MARKET",
                name="A股大盘",
                report_type="market_review",
                model_used="market-review",
                created_at=now,
            )
            session.add(report)
            session.flush()
            run = MarketOpportunityRun(
                run_key="cn:2026-08-31:source-failed:policy",
                market="cn",
                trade_date=date(2026, 8, 31),
                source_report_id=report.id,
                market_regime="normal",
                mode="observe",
                status="ready",
                policy_version="opportunity-policy-v1",
                config_snapshot_json="{}",
                provenance_json="{}",
                theme_count=1,
                selected_theme_count=0,
                candidate_count=0,
                selected_candidate_count=0,
                llm_budget=16,
                created_at=now,
                updated_at=now,
            )
            session.add(run)
            session.flush()
            session.add(MarketOpportunityTheme(
                run_id=run.id,
                theme_key="concept:算力",
                theme_type="concept",
                theme_name="算力",
                status="insufficient_data",
                score=None,
                score_components_json="{}",
                rejection_reasons_json='["constituent_snapshot_failed", "constituents_unavailable"]',
                data_quality_json='{"snapshot_status": "failed", "errors": ["provider_error:ConnectionError"]}',
                constituent_snapshot_json="{}",
                constituent_count=0,
                eligible_constituent_count=0,
                created_at=now,
                updated_at=now,
            ))
            run_id = int(run.id)

        listed = self.client.get("/api/v1/market-opportunities/runs?page=1&limit=5")
        detail = self.client.get(f"/api/v1/market-opportunities/runs/{run_id}")

        self.assertEqual(listed.status_code, 200)
        self.assertEqual(listed.json()["items"][0]["health_status"], "source_unavailable")
        self.assertEqual(listed.json()["items"][0]["health_reason"], "theme_constituents_unavailable")
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["health_status"], "source_unavailable")
        self.assertEqual(detail.json()["health_reason"], "theme_constituents_unavailable")

    def test_candidates_keep_total_above_first_page(self) -> None:
        self._seed_run()
        now = datetime(2026, 8, 31, 15, 30, 0)
        with DatabaseManager.get_instance().session_scope() as session:
            run = session.query(MarketOpportunityRun).one()
            theme = session.query(MarketOpportunityTheme).one()
            for index in range(500):
                code = f"{index:06d}"
                session.add(MarketOpportunityCandidate(
                    run_id=run.id,
                    theme_id=theme.id,
                    candidate_key=f"cn:sz:{code}",
                    market="cn",
                    exchange="SZ",
                    stock_code=code,
                    symbol=f"{code}.SZ",
                    stock_name=f"候选{index}",
                    primary_theme_key=theme.theme_key,
                    related_themes_json='["concept:创新药"]',
                    status="selected",
                    rank=index + 2,
                    score=50.0,
                    score_components_json="{}",
                    rejection_reasons_json="[]",
                    data_quality_json='{"status": "ok"}',
                    quote_snapshot_json='{"source": "daily"}',
                    as_of=date(2026, 8, 31),
                    valid_until=date(2026, 9, 1),
                    snapshot_hash=f"hash-{index}",
                    created_at=now,
                    updated_at=now,
                ))

        response = self.client.get("/api/v1/market-opportunities/candidates?status=selected&page=2&limit=50")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["total"], 501)
        self.assertEqual(payload["page"], 2)
        self.assertEqual(payload["limit"], 50)
        self.assertEqual(len(payload["items"]), 50)

    def test_candidate_api_uses_rank_order_and_preserves_market_and_status_filters(self) -> None:
        self._seed_run()
        with DatabaseManager.get_instance().session_scope() as session:
            selected = session.query(MarketOpportunityCandidate).one()
            run_id = selected.run_id
            values = {column.name: getattr(selected, column.name)
                      for column in MarketOpportunityCandidate.__table__.columns if column.name != "id"}
            for index, (status, rank, market) in enumerate([
                ("observing", 2, "cn"), ("rejected", None, "cn"), ("selected", 1, "hk"),
            ], start=1):
                session.add(MarketOpportunityCandidate(**{
                    **values, "candidate_key": f"api-order-{index}", "symbol": f"test-{index}",
                    "status": status, "rank": rank, "market": market,
                    "created_at": selected.created_at + timedelta(seconds=index),
                }))
        base = f"/api/v1/market-opportunities/candidates?run_id={run_id}&market=cn&limit=1"
        pages = [self.client.get(f"{base}&page={page}") for page in (1, 2, 3)]
        self.assertTrue(all(response.status_code == 200 for response in pages))
        self.assertEqual([response.json()["items"][0]["status"] for response in pages],
                         ["selected", "observing", "rejected"])
        self.assertTrue(all(response.json()["total"] == 3 for response in pages))
        filtered = self.client.get(f"{base}&status=observing").json()
        self.assertEqual(filtered["total"], 1)
        self.assertEqual(filtered["items"][0]["status"], "observing")

    def test_discover_is_rejected_when_mode_is_off_without_service_call(self) -> None:
        self._override_config(market_opportunity_mode="off")
        with patch("api.v1.endpoints.market_opportunities.MarketOpportunityService") as service_class:
            response = self.client.post("/api/v1/market-opportunities/discover", json={"market": "cn"})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "market_opportunity_disabled")
        service_class.assert_not_called()

    def test_discover_observe_uses_service_without_llm_job_runner(self) -> None:
        self._override_config(market_opportunity_mode="observe")
        with patch("api.v1.endpoints.market_opportunities.MarketOpportunityService") as service_class:
            service_class.return_value.discover_from_latest_market_review.return_value = {
                "status": "ready",
                "run_id": 9,
                "job_count": 0,
            }
            response = self.client.post("/api/v1/market-opportunities/discover", json={"market": "cn"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ready")
        service_class.return_value.discover_from_latest_market_review.assert_called_once_with(market="cn")
        service_class.return_value.process_pending_jobs.assert_not_called()
        service_class.return_value.analyze_run.assert_not_called()

    def test_analyze_is_rejected_in_observe_without_service_call(self) -> None:
        self._override_config(market_opportunity_mode="observe")
        with patch("api.v1.endpoints.market_opportunities.MarketOpportunityService") as service_class:
            response = self.client.post("/api/v1/market-opportunities/runs/1/analyze", json={"limit": 20})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "market_opportunity_not_paper")
        service_class.assert_not_called()

    def test_analyze_paper_is_bounded_by_llm_budget(self) -> None:
        self._override_config(
            market_opportunity_mode="paper",
            market_opportunity_observe_gate_days=0,
            market_opportunity_max_llm_calls_per_run=3,
        )
        with patch("api.v1.endpoints.market_opportunities.list_analysis_models", return_value=["deepseek/deepseek-v4-pro"]):
            with patch("api.v1.endpoints.market_opportunities.MarketOpportunityService") as service_class:
                service_class.return_value.analyze_run.return_value = {"status": "ok", "processed": 3}
                response = self.client.post(
                    "/api/v1/market-opportunities/runs/7/analyze",
                    json={"limit": 20, "owner": "api-test"},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")
        service_class.return_value.analyze_run.assert_called_once_with(7, owner="api-test", limit=3)

    def test_mode_put_rejects_paper_until_observe_gate_complete(self) -> None:
        self._override_config(market_opportunity_mode="observe", market_opportunity_observe_gate_days=3)

        response = self.client.put("/api/v1/market-opportunities/mode", json={"mode": "paper", "expected_version": 0})

        self.assertEqual(response.status_code, 409)
        payload = response.json()
        self.assertEqual(payload["error"], "observe_gate_incomplete")
        self.assertFalse(payload["observe_gate"]["completed"])

    def test_real_snapshot_counts_despite_candidate_failure_without_rewriting_run(self) -> None:
        from src.repositories.board_observation_repository import BoardObservationRepository
        self._override_config(market_opportunity_mode="observe", market_opportunity_observe_gate_days=3)
        self._seed_run()
        db = DatabaseManager.get_instance()
        day = date(2026, 9, 15)
        snapshot = BoardObservationRepository(db).upsert_snapshot({
            "snapshot_key": "cn:2026-09-15:csrc_tencent:csrc_industry", "market": "cn", "trade_date": day,
            "source": "csrc_tencent", "taxonomy": "csrc_industry", "status": "healthy",
            "coverage": {"status": "healthy", "theme_count": 1}, "snapshot_hash": "frozen-original",
            "fetched_at": datetime(2026, 9, 15, 7, 12),
            "themes": [{"theme_key": "industry:test", "theme_name": "test", "theme_code": "C39",
                        "theme_source": "csrc_tencent", "theme_taxonomy": "csrc_industry", "as_of": str(day)}],
        })
        frozen = json.dumps({"observation_health": {"healthy": False, "reason": "candidate_data_incomplete",
                            "source_snapshot_id": snapshot["id"], "source": "csrc_tencent"},
                            "analysis_block_reason": "market_regime_risk_off"})
        with db.session_scope() as session:
            run = session.get(MarketOpportunityRun, 1)
            run.trade_date = day
            run.market_regime = "risk_off"
            run.provenance_json = frozen
            candidate = session.query(MarketOpportunityCandidate).first()
            candidate.status = "rejected"
            candidate.rejection_reasons_json = '["daily_data_unavailable"]'
        gate = self.client.get("/api/v1/market-opportunities/status").json()["observe_gate"]
        self.assertEqual(gate["completed_trading_days"], 1)
        self.assertEqual(gate["dates"], ["2026-09-15"])
        self.assertFalse(gate["completed"])
        detail = self.client.get("/api/v1/market-opportunities/runs/1").json()
        listed = self.client.get("/api/v1/market-opportunities/runs").json()["items"][0]
        for payload in (detail, listed):
            self.assertTrue(payload["provenance"]["observation_health"]["healthy"])
            self.assertFalse(payload["provenance"]["recorded_observation_health"]["healthy"])
            self.assertEqual(payload["health_status"], "risk_blocked")
        quality = detail["provenance"]["observation_health"]["discovery_quality"]
        self.assertEqual(quality["candidate_data_incomplete_count"], 1)
        self.assertEqual(quality["scope"], "persisted_candidates")
        self.assertEqual(detail["candidates"][0]["status"], "rejected")
        blocked = self.client.put("/api/v1/market-opportunities/mode", json={"mode": "paper", "expected_version": 0})
        self.assertEqual(blocked.status_code, 409)
        self.assertEqual(blocked.json()["observe_gate"]["completed_trading_days"], 1)
        with db.session_scope() as session:
            self.assertEqual(session.get(MarketOpportunityRun, 1).provenance_json, frozen)

    def test_mode_put_requires_expected_version(self) -> None:
        response = self.client.put("/api/v1/market-opportunities/mode", json={"mode": "observe"})

        self.assertEqual(response.status_code, 422)

    def test_evaluation_endpoint_is_read_only_and_parses_windows(self) -> None:
        self._seed_run()
        response = self.client.get("/api/v1/market-opportunities/evaluation?run_id=1&windows=1,3")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["windows"], [1, 3])
        self.assertEqual(payload["candidate_count"], 1)
        self.assertIn("summary", payload)
        self.assertIn("model_execution_summary", payload)

    def test_evaluation_endpoint_rejects_invalid_windows(self) -> None:
        response = self.client.get("/api/v1/market-opportunities/evaluation?windows=abc")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "invalid_windows")


if __name__ == "__main__":
    unittest.main()


def test_historical_replay_health_and_observation_reason_are_distinct():
    from api.v1.endpoints.market_opportunities import _run_health, _candidate_no_plan_reason
    run={'status':'ready','selected_candidate_count':8,'provenance':{'frozen_membership_replay':True,
         'observation_health':{'healthy':False,'reason':'historical_collection_not_supported'}}}
    assert _run_health(run)['health_status'] == 'historical_replay'
    reason=_candidate_no_plan_reason({'status':'observing','rejection_reasons':['analysis_candidate_budget']},[],[])
    assert reason.startswith('candidate_observing:')
