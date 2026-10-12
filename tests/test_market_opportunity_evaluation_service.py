# -*- coding: utf-8 -*-
"""Prospective evaluation tests for market opportunity snapshots."""

from __future__ import annotations

import os
import json
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

from src.config import Config
from src.services.market_opportunity_evaluation_service import MarketOpportunityEvaluationService
from src.storage import (
    DatabaseManager,
    MarketOpportunityAnalysisJob,
    MarketOpportunityCandidate,
    MarketOpportunityRun,
    MarketOpportunityTheme,
    PaperPosition,
    PaperTradeEvent,
    StockDaily,
)


class MarketOpportunityEvaluationServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {
            key: os.environ.get(key)
            for key in (
                "ENV_FILE",
                "DATABASE_PATH",
                "DATABASE_URL",
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
                f"DATABASE_PATH={root / 'opportunity-eval.db'}",
            )) + "\n",
            encoding="utf-8",
        )
        os.environ["ENV_FILE"] = str(env_path)
        os.environ["DATABASE_PATH"] = str(root / "opportunity-eval.db")
        os.environ["DATABASE_URL"] = f"sqlite:///{root / 'opportunity-eval.db'}"
        os.environ["STOCK_LIST"] = "600519"
        os.environ["GEMINI_API_KEY"] = "test"
        Config.reset_instance()
        DatabaseManager.reset_instance()
        self.db = DatabaseManager.get_instance()

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        for key, value in self._old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.temp_dir.cleanup()

    def _seed_opportunity(self) -> int:
        now = datetime(2026, 8, 31, 15, 20, 0)
        with self.db.session_scope() as session:
            run = MarketOpportunityRun(
                run_key="cn:2026-08-31:market-review:policy",
                market="cn",
                trade_date=date(2026, 8, 31),
                source_report_id=1,
                market_regime="normal",
                mode="paper",
                status="completed",
                policy_version="opportunity-policy-v1",
                config_snapshot_json="{}",
                provenance_json="{}",
                theme_count=1,
                selected_theme_count=1,
                candidate_count=2,
                selected_candidate_count=2,
                llm_budget=4,
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
                score=80,
                score_components_json="{}",
                rejection_reasons_json="[]",
                data_quality_json="{}",
                constituent_snapshot_json="{}",
                constituent_count=2,
                eligible_constituent_count=2,
                created_at=now,
                updated_at=now,
            )
            session.add(theme)
            session.flush()
            first = MarketOpportunityCandidate(
                run_id=run.id,
                theme_id=theme.id,
                candidate_key="cn:sz:002173",
                market="cn",
                exchange="SZ",
                stock_code="002173",
                symbol="002173.SZ",
                stock_name="创新医疗",
                primary_theme_key="concept:创新药",
                related_themes_json='["concept:创新药"]',
                status="selected",
                rank=1,
                score=88,
                score_components_json="{}",
                rejection_reasons_json="[]",
                data_quality_json="{}",
                quote_snapshot_json='{"close": 10.0}',
                as_of=date(2026, 8, 31),
                valid_until=date(2026, 9, 1),
                snapshot_hash="hash-1",
                created_at=now,
                updated_at=now,
            )
            second = MarketOpportunityCandidate(
                run_id=run.id,
                theme_id=theme.id,
                candidate_key="cn:sh:600001",
                market="cn",
                exchange="SH",
                stock_code="600001",
                symbol="600001.SH",
                stock_name="测试股份",
                primary_theme_key="concept:创新药",
                related_themes_json='["concept:创新药"]',
                status="selected",
                rank=2,
                score=66,
                score_components_json="{}",
                rejection_reasons_json="[]",
                data_quality_json="{}",
                quote_snapshot_json="{}",
                as_of=date(2026, 8, 31),
                valid_until=date(2026, 9, 1),
                snapshot_hash="hash-2",
                created_at=now,
                updated_at=now,
            )
            session.add_all([first, second])
            session.flush()
            session.add_all([
                StockDaily(code="002173.SZ", date=date(2026, 8, 31), open=9.8, high=10.2, low=9.7, close=9.0),
                StockDaily(code="002173.SZ", date=date(2026, 9, 1), open=10.1, high=10.8, low=9.9, close=10.5),
                StockDaily(code="002173.SZ", date=date(2026, 9, 2), open=10.5, high=11.0, low=10.2, close=10.8),
                StockDaily(code="002173.SZ", date=date(2026, 9, 3), open=10.8, high=11.2, low=10.4, close=11.0),
                StockDaily(code="600001.SH", date=date(2026, 8, 31), open=20.0, high=20.5, low=19.8, close=20.0),
                StockDaily(code="600001.SH", date=date(2026, 9, 1), open=19.8, high=20.0, low=19.0, close=19.5),
                StockDaily(code="000300.SH", date=date(2026, 8, 31), open=4000.0, high=4010.0, low=3990.0, close=4000.0),
                StockDaily(code="000300.SH", date=date(2026, 9, 1), open=4000.0, high=4020.0, low=3980.0, close=4040.0),
                StockDaily(code="000300.SH", date=date(2026, 9, 2), open=4040.0, high=4050.0, low=4010.0, close=4080.0),
                StockDaily(code="000300.SH", date=date(2026, 9, 3), open=4080.0, high=4100.0, low=4040.0, close=4120.0),
            ])
            session.add(MarketOpportunityAnalysisJob(
                job_key="job:deepseek:002173",
                run_id=run.id,
                candidate_id=first.id,
                model="deepseek/deepseek-v4-pro",
                status="completed",
                analysis_history_id=101,
                decision_signal_id=201,
                paper_plan_id=301,
                evidence_json="{}",
                created_at=now,
                updated_at=now,
            ))
            position = PaperPosition(
                plan_id=301,
                source_report_id=101,
                decision_signal_id=201,
                source_model="deepseek/deepseek-v4-pro",
                market="cn",
                stock_code="002173",
                stock_name="创新医疗",
                status="closed",
                opened_on=date(2026, 9, 1),
                opened_price=10.0,
                normalized_notional=100000,
                quantity=100,
                initial_quantity=100,
                max_holding_sessions=10,
                closed_on=date(2026, 9, 2),
                closed_price=10.5,
                realized_quantity=100,
                realized_pnl=43,
                realized_fees=7,
                simulated_pnl=50,
            )
            session.add(position)
            session.flush()
            session.add(PaperTradeEvent(
                event_key="event:002173:close",
                plan_id=301,
                position_id=position.id,
                source_report_id=101,
                decision_signal_id=201,
                source_model="deepseek/deepseek-v4-pro",
                market="cn",
                stock_code="002173",
                stock_name="创新医疗",
                event_type="close",
                event_date=date(2026, 9, 2),
                price=10.5,
                quantity=100,
                executed_quantity=100,
                gross_amount=1050,
                commission=5,
                tax=1,
                slippage_amount=1,
                fees=7,
                net_amount=1043,
                gross_realized_pnl=50,
                net_realized_pnl=43,
                reason="test_close",
                details_json="{}",
            ))
            return int(run.id)

    def test_evaluate_uses_only_future_bars_and_marks_insufficient_windows(self) -> None:
        run_id = self._seed_opportunity()

        payload = MarketOpportunityEvaluationService(db_manager=self.db).evaluate(run_id=run_id, windows=(1, 3, 5))

        self.assertEqual(payload["windows"], [1, 3, 5])
        self.assertEqual(payload["candidate_count"], 2)
        first = next(item for item in payload["candidate_metrics"] if item["stock_code"] == "002173")
        self.assertEqual(first["entry_price"], 10.0)
        self.assertEqual(first["return_basis"], "signal_close_to_future_close_non_executable")
        self.assertEqual(first["stock_daily_match_code"], "002173.SZ")
        self.assertEqual(first["windows"]["1"]["return_basis"], "signal_close_to_future_close_non_executable")
        self.assertEqual(first["windows"]["1"]["return_pct"], 5.0)
        self.assertEqual(first["windows"]["1"]["benchmark_return_pct"], 1.0)
        self.assertEqual(first["windows"]["1"]["excess_return_pct"], 4.0)
        self.assertEqual(first["windows"]["3"]["return_pct"], 10.0)
        self.assertEqual(first["windows"]["5"]["status"], "insufficient_data")
        self.assertEqual(first["executable_entry"]["status"], "daily_open_proxy_unconfirmed")
        self.assertEqual(first["executable_entry"]["entry_date"], "2026-09-01")
        self.assertEqual(first["executable_entry"]["earliest_sell_date"], "2026-09-02")
        self.assertFalse(first["executable_entry"]["tradability"]["fill_confirmed"])
        self.assertEqual(first["executable_entry"]["tradability"]["limit_unfillable_status"], "cannot_confirm_without_limit_price_or_minute_data")
        self.assertEqual(first["executable_windows"]["1"]["return_basis"], "next_session_open_daily_proxy_unconfirmed")
        self.assertEqual(first["executable_windows"]["1"]["status"], "t_plus_one_blocked")
        self.assertEqual(first["paper_execution"]["positions"], 1)
        self.assertEqual(first["paper_execution"]["trade_events"], 1)
        self.assertEqual(first["paper_execution"]["total_fees"], 7.0)
        self.assertEqual(first["paper_execution"]["total_commission"], 5.0)
        self.assertEqual(first["paper_execution"]["total_tax"], 1.0)
        self.assertEqual(first["paper_execution"]["total_slippage_amount"], 1.0)
        self.assertEqual(first["paper_execution"]["gross_realized_pnl"], 50.0)
        self.assertEqual(first["paper_execution"]["net_realized_pnl"], 43.0)

        second = next(item for item in payload["candidate_metrics"] if item["stock_code"] == "600001")
        self.assertEqual(second["entry_price"], 20.0)
        self.assertEqual(second["windows"]["1"]["return_pct"], -2.5)
        self.assertEqual(second["windows"]["3"]["status"], "insufficient_data")

        self.assertEqual(payload["summary"]["windows"]["1"]["evaluated"], 2)
        self.assertEqual(payload["summary"]["windows"]["1"]["hit_rate"], 0.5)
        model_bucket = payload["model_execution_summary"]["deepseek/deepseek-v4-pro"]
        self.assertEqual(model_bucket["report_rate"], 1.0)
        self.assertEqual(model_bucket["plan_rate"], 1.0)
        self.assertEqual(model_bucket["realized_pnl"], 43.0)

    def test_research_jobs_are_counted_apart_from_paper_conversion(self) -> None:
        run_id = self._seed_opportunity()
        now = datetime(2026, 8, 31, 15, 30, 0)
        with self.db.session_scope() as session:
            first_id, second_id = [
                int(row.id)
                for row in session.query(MarketOpportunityCandidate)
                .filter_by(run_id=run_id)
                .order_by(MarketOpportunityCandidate.rank.asc())
                .all()
            ]
            session.add_all([
                MarketOpportunityAnalysisJob(
                    job_key="market-opportunity-research:initial:eval-a",
                    run_id=run_id,
                    candidate_id=second_id,
                    model="deepseek/deepseek-v4-pro",
                    status="completed",
                    analysis_history_id=102,
                    evidence_json="{}",
                    created_at=now,
                    updated_at=now,
                ),
                MarketOpportunityAnalysisJob(
                    job_key="market-opportunity-research:initial:eval-b",
                    run_id=run_id,
                    candidate_id=first_id,
                    model="research-only-model",
                    status="failed",
                    failure_reason="research_budget_exhausted",
                    evidence_json="{}",
                    created_at=now,
                    updated_at=now,
                ),
            ])

        payload = MarketOpportunityEvaluationService(db_manager=self.db).evaluate(run_id=run_id, windows=(1,))

        # 研究任务不改写既有模拟执行口径，只追加独立计数。
        paper_model = payload["model_execution_summary"]["deepseek/deepseek-v4-pro"]
        self.assertEqual((paper_model["jobs"], paper_model["reports"], paper_model["plans"]), (1, 1, 1))
        self.assertEqual((paper_model["report_rate"], paper_model["plan_rate"]), (1.0, 1.0))
        self.assertEqual((paper_model["research_jobs"], paper_model["research_reports"]), (1, 1))
        research_model = payload["model_execution_summary"]["research-only-model"]
        self.assertEqual(
            (research_model["jobs"], research_model["research_jobs"], research_model["research_reports"]),
            (0, 1, 0),
        )
        self.assertEqual((research_model["report_rate"], research_model["plan_rate"]), (0.0, 0.0))
        by_code = {item["stock_code"]: item["paper_execution"] for item in payload["candidate_metrics"]}
        self.assertEqual((by_code["002173"]["jobs"], by_code["002173"]["reports"], by_code["002173"]["plans"]), (1, 1, 1))
        self.assertEqual((by_code["002173"]["research_jobs"], by_code["002173"]["research_reports"]), (1, 0))
        self.assertEqual(by_code["002173"]["positions"], 1)
        self.assertEqual((by_code["600001"]["jobs"], by_code["600001"]["reports"]), (0, 0))
        self.assertEqual((by_code["600001"]["research_jobs"], by_code["600001"]["research_reports"]), (1, 1))

    def test_evaluate_accepts_matching_symbol_variants_and_fails_on_conflicts(self) -> None:
        run_id = self._seed_opportunity()
        with self.db.session_scope() as session:
            session.add(StockDaily(code="SZ002173", date=date(2026, 9, 1), open=10.1, high=10.8, low=9.9, close=10.5))

        payload = MarketOpportunityEvaluationService(db_manager=self.db).evaluate(run_id=run_id, windows=(1,))

        first = next(item for item in payload["candidate_metrics"] if item["stock_code"] == "002173")
        self.assertEqual(first["windows"]["1"]["status"], "ok")
        self.assertEqual(first["windows"]["1"]["return_pct"], 5.0)

        with self.db.session_scope() as session:
            row = session.query(StockDaily).filter_by(code="SZ002173", date=date(2026, 9, 1)).one()
            row.close = 10.6

        conflicted = MarketOpportunityEvaluationService(db_manager=self.db).evaluate(run_id=run_id, windows=(1,))

        first = next(item for item in conflicted["candidate_metrics"] if item["stock_code"] == "002173")
        self.assertEqual(first["windows"]["1"]["status"], "insufficient_data")
        self.assertEqual(first["windows"]["1"]["reason"], "identity_ambiguous")

    def test_evaluate_defaults_to_extended_windows_and_includes_frozen_legacy_group(self) -> None:
        run_id = self._seed_opportunity()
        now = datetime(2026, 8, 31, 15, 20, 0)
        with self.db.session_scope() as session:
            run = session.get(MarketOpportunityRun, run_id)
            theme = session.query(MarketOpportunityTheme).filter_by(run_id=run_id).one()
            legacy = MarketOpportunityCandidate(
                run_id=run_id,
                theme_id=theme.id,
                candidate_key="cn:sh:600002",
                market="cn",
                exchange="SH",
                stock_code="600002",
                symbol="600002.SH",
                stock_name="旧策略样本",
                primary_theme_key="concept:创新药",
                related_themes_json='["concept:创新药"]',
                status="rejected",
                rank=99,
                score=69,
                score_components_json=json.dumps({
                    "strategy_comparison": {
                        "legacy_selected": True,
                        "legacy_score": 69,
                        "legacy_reasons": [],
                        "current_selected": False,
                        "policy_version": "candidate-policy-v3",
                    }
                }),
                rejection_reasons_json='["below_current_policy"]',
                data_quality_json="{}",
                quote_snapshot_json='{"close": 30.0}',
                as_of=date(2026, 8, 31),
                valid_until=date(2026, 9, 1),
                snapshot_hash="hash-legacy",
                created_at=now,
                updated_at=now,
            )
            run.candidate_count = 3
            session.add(legacy)
            session.add(StockDaily(code="600002.SH", date=date(2026, 9, 1), open=30.0, high=31.0, low=29.5, close=30.6))

        payload = MarketOpportunityEvaluationService(db_manager=self.db).evaluate(run_id=run_id)

        self.assertEqual(payload["windows"], [1, 3, 5, 10, 20, 40])
        legacy_metric = next(item for item in payload["candidate_metrics"] if item["stock_code"] == "600002")
        self.assertEqual(legacy_metric["candidate_status"], "rejected")
        self.assertTrue(legacy_metric["comparison_groups"]["legacy_selected"])
        self.assertFalse(legacy_metric["comparison_groups"]["current_selected"])
        self.assertEqual(legacy_metric["windows"]["1"]["return_pct"], 2.0)


    def test_missing_first_session_does_not_shift_entry_or_window(self):
        run_id = self._seed_opportunity()
        with self.db.session_scope() as session:
            session.query(StockDaily).filter(StockDaily.code == "002173.SZ", StockDaily.date == date(2026, 9, 1)).delete()
        result = MarketOpportunityEvaluationService(self.db).evaluate(run_id=run_id, windows=(1, 3))
        metric = next(item for item in result["candidate_metrics"] if item["stock_code"] == "002173")
        self.assertEqual(metric["executable_entry"]["status"], "insufficient_future_bars")
        self.assertIsNone(metric["executable_entry"]["price"])
        self.assertEqual(metric["windows"]["1"]["status"], "insufficient_data")
        self.assertIn("2026-09-01", metric["windows"]["3"]["missing_dates"])

    def test_execution_proxy_benchmark_uses_same_open_and_calendar_dates(self):
        run_id = self._seed_opportunity()
        with self.db.session_scope() as session:
            benchmark = session.query(StockDaily).filter(StockDaily.code == "000300.SH", StockDaily.date == date(2026, 9, 1)).one()
            benchmark.open = 100.0
        metric = next(item for item in MarketOpportunityEvaluationService(self.db).evaluate(run_id=run_id, windows=(3,))["candidate_metrics"] if item["stock_code"] == "002173")
        with self.db.session_scope() as session:
            close = session.query(StockDaily).filter(StockDaily.code == "000300.SH", StockDaily.date == date(2026, 9, 3)).one().close
        self.assertAlmostEqual(metric["executable_windows"]["3"]["benchmark_return_pct"], (close / 100.0 - 1) * 100, places=4)

    def test_frozen_comparison_deduplicates_same_day_reruns(self):
        run_id = self._seed_opportunity()
        with self.db.session_scope() as session:
            original = session.query(MarketOpportunityCandidate).filter_by(run_id=run_id, stock_code="002173").one()
            rerun = MarketOpportunityRun(run_key="rerun-v3", market="cn", trade_date=date(2026, 8, 31), source_report_id=1,
                                        mode="observe", status="ready", policy_version="opportunity-policy-v3")
            session.add(rerun)
            session.flush()
            candidate = MarketOpportunityCandidate(run_id=rerun.id, candidate_key=original.candidate_key,
                market=original.market, exchange=original.exchange, stock_code=original.stock_code, symbol=original.symbol,
                primary_theme_key=original.primary_theme_key, status="observing", as_of=original.as_of,
                quote_snapshot_json=original.quote_snapshot_json, snapshot_hash="rerun")
            session.add(candidate)
            session.flush()
            new_id = candidate.id
        payload = MarketOpportunityEvaluationService(self.db).evaluate(market="cn", windows=(1,))
        duplicates = [row for row in payload["candidate_metrics"] if row["stock_code"] == "002173"]
        self.assertEqual(len(duplicates), 1)
        self.assertEqual(duplicates[0]["candidate_id"], new_id)
        self.assertEqual(payload["strategy_comparison_summary"]["groups"]["current"]["candidate_count"], 2)


if __name__ == "__main__":
    unittest.main()
