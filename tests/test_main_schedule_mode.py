# -*- coding: utf-8 -*-
"""Regression tests for scheduled mode stock selection behavior."""

import json
import logging
import os
import socket
import tempfile
import threading
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tests.litellm_stub import ensure_litellm_stub

ensure_litellm_stub()

_ENV_BEFORE_MAIN_IMPORT = dict(os.environ)
import main
from src.config import Config

# Real per-session lease helpers, captured before setUp patches them.
_REAL_EVENING_CLAIM = main._claim_market_opportunity_evening_session
_REAL_EVENING_FINISH = main._finish_market_opportunity_evening_session

_MAIN_IMPORT_ENV_ADDITIONS = frozenset(set(os.environ) - set(_ENV_BEFORE_MAIN_IMPORT))
_MAIN_IMPORT_ENV_OVERRIDES = {
    key: value
    for key, value in _ENV_BEFORE_MAIN_IMPORT.items()
    if os.environ.get(key) != value
}


def _api_app_stub_modules():
    """sys.modules entries so ``start_api_server`` can ``from api.app import app``
    without importing the real (heavy) app tree in these isolated unit tests.

    ``start_api_server`` imports the ASGI app object in the calling thread so the
    import stays out of the uvicorn startup probe window; these control-flow tests
    stub it the same way they already stub uvicorn.
    """
    import types

    api_pkg = types.ModuleType("api")
    api_app_mod = types.ModuleType("api.app")
    api_app_mod.app = SimpleNamespace()
    api_pkg.app = api_app_mod
    return {"api": api_pkg, "api.app": api_app_mod}


class _DummyConfig(SimpleNamespace):
    def validate(self):
        return []


class MainScheduleModeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.env_path = Path(self.temp_dir.name) / ".env"
        self.env_path.write_text("STOCK_LIST=600519\n", encoding="utf-8")
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)
        self.env_patch = patch.dict(os.environ, {"ENV_FILE": str(self.env_path)}, clear=False)
        self.env_patch.start()
        Config.reset_instance()
        # Evening-run tests default to an uncontended per-session lease; the
        # cross-instance tests patch these back to a real temp-DB lease.
        claim_patch = patch(
            "main._claim_market_opportunity_evening_session",
            side_effect=lambda session: {"claimed": True, "lease_key": f"evening:{session}"},
        )
        finish_patch = patch("main._finish_market_opportunity_evening_session")
        self.evening_claim = claim_patch.start()
        self.evening_finish = finish_patch.start()
        self.addCleanup(claim_patch.stop)
        self.addCleanup(finish_patch.stop)
        root_logger = logging.getLogger()
        self._original_root_handlers = list(root_logger.handlers)
        self._original_root_level = root_logger.level

    def tearDown(self) -> None:
        root_logger = logging.getLogger()
        current_handlers = list(root_logger.handlers)
        for handler in current_handlers:
            if handler not in self._original_root_handlers:
                root_logger.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass
        root_logger.setLevel(self._original_root_level)
        os.chdir(self.original_cwd)
        Config.reset_instance()
        self.env_patch.stop()
        for key in _MAIN_IMPORT_ENV_ADDITIONS:
            os.environ.pop(key, None)
        for key, value in _MAIN_IMPORT_ENV_OVERRIDES.items():
            os.environ[key] = value
        self.temp_dir.cleanup()

    def _make_args(self, **overrides):
        defaults = {
            "debug": False,
            "stocks": None,
            "webui": False,
            "webui_only": False,
            "serve": False,
            "serve_only": False,
            "host": None,
            "port": None,
            "backtest": False,
            "market_review": False,
            "schedule": False,
            "no_run_immediately": False,
            "no_notify": False,
            "check_notify": False,
            "no_market_review": False,
            "dry_run": False,
            "workers": 1,
            "force_run": False,
            "single_notify": False,
            "no_context_snapshot": False,
        }
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def _make_config(self, **overrides):
        defaults = {
            "log_dir": self.temp_dir.name,
            "webui_enabled": False,
            "webui_host": "127.0.0.1",
            "webui_port": 8000,
            "dingtalk_stream_enabled": False,
            "feishu_stream_enabled": False,
            "schedule_enabled": False,
            "schedule_time": "18:00",
            "schedule_multi_model_enabled": False,
            "paper_trading_enabled": False,
            "schedule_run_immediately": True,
            "run_immediately": True,
            "agent_event_monitor_enabled": False,
            "agent_event_alert_rules_json": "",
            "agent_event_monitor_interval_minutes": 5,
            "daily_market_context_enabled": True,
        }
        defaults.update(overrides)
        return _DummyConfig(**defaults)

    def test_scheduled_multi_model_runs_every_configured_model_without_fallback(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_multi_model_enabled=True,
            stock_list=["002714.SZ", "512480.SH"],
        )
        config.refresh_stock_list = MagicMock()
        deepseek_config = SimpleNamespace()
        claude_config = SimpleNamespace()

        with patch(
            "src.services.analysis_model_service.list_analysis_models",
            return_value=["deepseek/deepseek-v4-pro", "anthropic/claude-sonnet-5"],
        ), patch(
            "src.services.analysis_model_service.build_model_scoped_config",
            side_effect=[deepseek_config, claude_config],
        ) as build_scoped, patch("main.run_full_analysis", return_value=True) as run_full:
            result = main.run_scheduled_analysis(config, args)

        self.assertTrue(result)
        config.refresh_stock_list.assert_called_once_with()
        self.assertEqual(build_scoped.call_count, 2)
        self.assertEqual(
            deepseek_config.report_filename_suffix,
            "deepseek/deepseek-v4-pro",
        )
        self.assertEqual(
            claude_config.report_filename_suffix,
            "anthropic/claude-sonnet-5",
        )
        self.assertEqual(run_full.call_count, 2)
        for call in run_full.call_args_list:
            self.assertEqual(call.args[2], ["002714.SZ", "512480.SH"])
            self.assertTrue(call.kwargs["raise_errors"])

    def test_scheduled_single_model_mode_keeps_legacy_execution(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(schedule_multi_model_enabled=False)

        with patch("main.run_full_analysis", return_value=True) as run_full:
            result = main.run_scheduled_analysis(config, args, ["002714.SZ"])

        self.assertTrue(result)
        run_full.assert_called_once_with(
            config,
            args,
            ["002714.SZ"],
            raise_errors=True,
        )

    def test_paper_monitor_runs_once_after_complete_multi_model_batch(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_multi_model_enabled=True,
            paper_trading_enabled=True,
            stock_list=["002714.SZ"],
        )
        config.refresh_stock_list = MagicMock()

        with patch(
            "src.services.analysis_model_service.list_analysis_models",
            return_value=["model-a", "model-b"],
        ), patch(
            "src.services.analysis_model_service.build_model_scoped_config",
            side_effect=[SimpleNamespace(), SimpleNamespace()],
        ), patch("main.run_full_analysis", return_value=True) as run_full, patch(
            "main._run_paper_trading_monitor_if_enabled"
        ) as monitor:
            result = main.run_scheduled_analysis(config, args)

        self.assertTrue(result)
        self.assertEqual(run_full.call_count, 2)
        monitor.assert_called_once_with(config)

    def test_market_opportunity_runs_after_success_before_paper_monitor(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            market_opportunity_mode="paper",
            paper_trading_enabled=True,
        )
        calls = []

        def opportunity(_config):
            calls.append("opportunity")

        def monitor(_config):
            calls.append("paper")

        with patch("main.run_full_analysis", return_value=True), patch(
            "main._run_market_opportunity_pool_if_enabled",
            side_effect=opportunity,
        ) as opportunity_mock, patch(
            "main._run_paper_trading_monitor_if_enabled",
            side_effect=monitor,
        ) as monitor_mock:
            result = main.run_scheduled_analysis(config, args, ["002714.SZ"])

        self.assertTrue(result)
        opportunity_mock.assert_called_once_with(config)
        monitor_mock.assert_called_once_with(config)
        self.assertEqual(calls, ["opportunity", "paper"])

    def test_auxiliary_ledgers_run_before_partial_multi_model_failure_is_raised(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_multi_model_enabled=True,
            paper_trading_enabled=True,
            market_opportunity_mode="paper",
            stock_list=["002714.SZ"],
        )
        config.refresh_stock_list = MagicMock()
        calls = []

        with patch(
            "src.services.analysis_model_service.list_analysis_models",
            return_value=["model-a", "model-b"],
        ), patch(
            "src.services.analysis_model_service.build_model_scoped_config",
            side_effect=[SimpleNamespace(), SimpleNamespace()],
        ), patch(
            "main.run_full_analysis",
            side_effect=[True, RuntimeError("provider failed")],
        ), patch(
            "main._run_market_opportunity_pool_if_enabled",
            side_effect=lambda _config: calls.append("opportunity"),
        ), patch(
            "main._run_paper_trading_monitor_if_enabled",
            side_effect=lambda _config: calls.append("paper"),
        ):
            with self.assertRaisesRegex(RuntimeError, "model-b"):
                main.run_scheduled_analysis(config, args)

        self.assertEqual(calls, ["opportunity", "paper"])

    def test_schedule_mode_reaches_scheduled_analysis_when_only_opportunity_observe_enabled(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(market_opportunity_mode="observe")

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            shared_lease_enabled=None,
        ):
            task()

        with patch("main.parse_arguments", return_value=args), patch(
            "main.get_config", return_value=config
        ), patch("main._reload_runtime_config", return_value=config), patch(
            "main._build_schedule_time_provider", return_value=lambda: "18:00"
        ), patch("main.prepare_webui_frontend_assets", return_value=True), patch(
            "src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule
        ), patch(
            "main.run_scheduled_analysis"
        ) as scheduled, patch(
            "main.run_full_analysis"
        ) as legacy:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        scheduled.assert_called_once_with(config, args, None)
        legacy.assert_not_called()

    def test_market_opportunity_region_only_runs_cn_for_mixed_market_review_region(self) -> None:
        config = self._make_config(market_opportunity_mode="observe", market_review_region="cn,us")
        service = MagicMock()
        service.run_after_market_review.return_value = {
            "status": "ok",
            "discovery": {"run_id": 1, "selected_candidate_count": 0, "job_count": 0},
            "jobs": {"processed": 0, "failed": 0},
        }

        with patch("src.services.market_opportunity_service.MarketOpportunityService", return_value=service):
            main._run_market_opportunity_pool_if_enabled(config)

        service.run_after_market_review.assert_called_once()
        self.assertEqual(service.run_after_market_review.call_args.kwargs["market"], "cn")

    def test_market_opportunity_log_separates_research_block_and_outcomes(self) -> None:
        config = self._make_config(market_opportunity_mode="paper", market_review_region="cn")
        service = MagicMock()
        outcomes = {"bullish": 1, "non_bullish": 2, "analysis_failed": 1, "budget_exhausted": 0}
        service.run_after_market_review.return_value = {
            "status": "ok",
            "discovery": {
                "run_id": 7, "selected_candidate_count": 4, "job_count": 4, "reason": None,
                "research": {"block_reason": "research_budget_exhausted"},
            },
            "jobs": {"processed": 3, "failed": 1},
            "research_outcomes": outcomes,
        }

        with patch("main._market_opportunity_pool_enabled", return_value=True), patch(
            "src.services.market_opportunity_service.MarketOpportunityService", return_value=service
        ), self.assertLogs("main", level="INFO") as captured:
            main._run_market_opportunity_pool_if_enabled(config)

        line = next(item for item in captured.output if "大盘风口机会池完成" in item)
        self.assertIn("research_block=research_budget_exhausted", line)
        self.assertIn(f"research_outcomes={outcomes}", line)

    def test_market_opportunity_region_skips_non_cn_market_review_region(self) -> None:
        config = self._make_config(market_opportunity_mode="paper", market_review_region="jp,kr")

        with patch("src.services.market_opportunity_service.MarketOpportunityService") as service_cls:
            main._run_market_opportunity_pool_if_enabled(config)

        service_cls.assert_not_called()

    def test_market_opportunity_off_does_not_build_service(self) -> None:
        config = self._make_config(market_opportunity_mode="off", market_review_region="cn")

        with patch("src.services.market_opportunity_service.MarketOpportunityService") as service_cls:
            main._run_market_opportunity_pool_if_enabled(config)

        service_cls.assert_not_called()

    def test_auxiliary_ledgers_run_when_single_model_returns_failure(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(paper_trading_enabled=True)

        with patch("main.run_full_analysis", return_value=False), patch(
            "main._run_market_opportunity_pool_if_enabled"
        ) as opportunity, patch(
            "main._run_paper_trading_monitor_if_enabled"
        ) as monitor:
            with self.assertRaisesRegex(RuntimeError, "analysis_unsuccessful"):
                main.run_scheduled_analysis(config, args, ["002714.SZ"])

        opportunity.assert_called_once_with(config)
        monitor.assert_called_once_with(config)

    def test_single_model_and_both_auxiliary_errors_are_aggregated(self) -> None:
        config = self._make_config(paper_trading_enabled=True)
        with patch("main.run_full_analysis", side_effect=RuntimeError("provider_down")), patch(
            "main._run_market_opportunity_pool_if_enabled", side_effect=RuntimeError("discovery_down")
        ) as opportunity, patch(
            "main._run_paper_trading_monitor_if_enabled", side_effect=RuntimeError("ledger_down")
        ) as monitor:
            with self.assertRaises(RuntimeError) as caught:
                main.run_scheduled_analysis(config, self._make_args(schedule=True))
        for reason in ("provider_down", "discovery_down", "ledger_down"):
            self.assertIn(reason, str(caught.exception))
        opportunity.assert_called_once_with(config)
        monitor.assert_called_once_with(config)

    def test_multi_model_setup_failure_still_runs_auxiliaries(self) -> None:
        config = self._make_config(schedule_multi_model_enabled=True, stock_list=[])
        config.refresh_stock_list = MagicMock()
        with patch("src.services.analysis_model_service.list_analysis_models", return_value=[]), patch(
            "main._run_market_opportunity_pool_if_enabled", side_effect=RuntimeError("pool_down")
        ), patch("main._run_paper_trading_monitor_if_enabled") as monitor:
            with self.assertRaisesRegex(RuntimeError, "analysis_setup"):
                main.run_scheduled_analysis(config, self._make_args(schedule=True))
        monitor.assert_called_once_with(config)

    def test_auxiliary_service_exceptions_are_not_swallowed(self) -> None:
        config = self._make_config(paper_trading_enabled=True, market_opportunity_mode="paper")
        with patch("main._build_paper_trading_service") as build:
            build.return_value.run_monitor.side_effect = RuntimeError("ledger_down")
            with self.assertRaisesRegex(RuntimeError, "ledger_down"):
                main._run_paper_trading_monitor_if_enabled(config)
        with patch("main._market_opportunity_pool_enabled", return_value=True), patch(
            "src.services.market_opportunity_service.MarketOpportunityService"
        ) as service:
            service.return_value.run_after_market_review.side_effect = RuntimeError("pool_down")
            with self.assertRaisesRegex(RuntimeError, "pool_down"):
                main._run_market_opportunity_pool_if_enabled(config)

    def test_evening_run_time_moves_pool_out_of_scheduled_batch(self) -> None:
        config = self._make_config(
            paper_trading_enabled=True,
            market_opportunity_mode="paper",
            market_opportunity_run_time="20:00",
        )

        with patch("main.run_full_analysis", return_value=True), patch(
            "main._run_market_opportunity_pool_if_enabled"
        ) as opportunity, patch("main._run_paper_trading_monitor_if_enabled") as monitor:
            result = main.run_scheduled_analysis(config, self._make_args(schedule=True), ["002714.SZ"])

        self.assertTrue(result)
        opportunity.assert_not_called()
        monitor.assert_called_once_with(config)

    @staticmethod
    def _china_clock(value):
        class FakeDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return value.replace(tzinfo=tz) if tz is not None else value

        return FakeDateTime

    def test_evening_pool_waits_for_run_time_and_runs_once_per_session(self) -> None:
        config = self._make_config(market_opportunity_mode="paper", market_opportunity_run_time="20:00")

        with patch("main._market_opportunity_evening_last_session", None), \
             patch("main._market_opportunity_pool_enabled", return_value=True), \
             patch("src.core.trading_calendar.is_market_open", return_value=True), \
             patch("src.services.market_opportunity_service.MarketOpportunityService") as service_cls, \
             patch("main._run_market_opportunity_pool_if_enabled") as pool:
            service_cls.return_value.market_review_readiness.return_value = {
                "ready": True,
                "session": "2026-10-09",
                "report_id": 7,
            }
            with patch("main.datetime", self._china_clock(datetime(2026, 10, 9, 19, 55))):
                early = main._run_market_opportunity_pool_evening_if_due(config)
            with patch("main.datetime", self._china_clock(datetime(2026, 10, 9, 20, 0))):
                first = main._run_market_opportunity_pool_evening_if_due(config)
                repeat = main._run_market_opportunity_pool_evening_if_due(config)
            with patch("main.datetime", self._china_clock(datetime(2026, 10, 12, 20, 5))):
                next_session = main._run_market_opportunity_pool_evening_if_due(config)

        self.assertEqual(early, {"status": "skipped", "reason": "before_run_time"})
        self.assertIsNone(first)
        self.assertEqual(repeat, {"status": "skipped", "reason": "already_ran_today"})
        self.assertIsNone(next_session)
        self.assertEqual(pool.call_count, 2)
        pool.assert_called_with(config)
        self.assertEqual(service_cls.return_value.market_review_readiness.call_args.kwargs["market"], "cn")

    def test_evening_pool_waits_for_current_market_review_without_consuming_session(self) -> None:
        config = self._make_config(market_opportunity_mode="paper", market_opportunity_run_time="20:00")

        with patch("main._market_opportunity_evening_last_session", None), \
             patch("main._market_opportunity_pool_enabled", return_value=True), \
             patch("src.core.trading_calendar.is_market_open", return_value=True), \
             patch("src.services.market_opportunity_service.MarketOpportunityService") as service_cls, \
             patch("main._run_market_opportunity_pool_if_enabled") as pool, \
             patch("main.datetime", self._china_clock(datetime(2026, 10, 9, 20, 30))):
            service_cls.return_value.market_review_readiness.side_effect = [
                {"ready": False, "reason": "market_review_not_ready", "session": "2026-10-09"},
                {"ready": True, "session": "2026-10-09", "report_id": 8},
            ]
            waiting = main._run_market_opportunity_pool_evening_if_due(config)
            ran = main._run_market_opportunity_pool_evening_if_due(config)

        self.assertEqual(waiting, {"status": "skipped", "reason": "market_review_not_ready"})
        self.assertIsNone(ran)
        pool.assert_called_once_with(config)

    def test_evening_pool_skips_while_analysis_batch_holds_shared_lock(self) -> None:
        from src.services.runtime_scheduler import run_with_global_analysis_lock

        config = self._make_config(market_opportunity_mode="paper", market_opportunity_run_time="20:00")
        observed = {}

        def batch(_config, _args, _stock_codes):
            observed["during_batch"] = main._run_market_opportunity_pool_evening_if_due(config)
            return True

        with patch("main._market_opportunity_evening_last_session", None), \
             patch("main._market_opportunity_pool_enabled", return_value=True), \
             patch("src.core.trading_calendar.is_market_open", return_value=True), \
             patch("src.services.market_opportunity_service.MarketOpportunityService") as service_cls, \
             patch("main._run_market_opportunity_pool_if_enabled") as pool, \
             patch("main.datetime", self._china_clock(datetime(2026, 10, 9, 20, 30))):
            service_cls.return_value.market_review_readiness.return_value = {"ready": True, "session": "2026-10-09"}
            self.assertTrue(run_with_global_analysis_lock(batch, config, None, None))
            after_batch = main._run_market_opportunity_pool_evening_if_due(config)

        self.assertEqual(observed["during_batch"], {"status": "skipped", "reason": "analysis_busy"})
        self.assertIsNone(after_batch)
        pool.assert_called_once_with(config)

    def test_evening_pool_gates_skip_before_building_service(self) -> None:
        calendar_down = RuntimeError("exchange-calendars unavailable")
        cases = (
            ("evening_run_disabled", {"market_opportunity_run_time": ""}, True, True),
            ("market_closed", {}, False, True),
            ("trading_calendar_unavailable", {}, calendar_down, True),
            ("pool_disabled", {}, True, False),
            ("market_review_region_not_cn", {"market_review_region": "us"}, True, True),
        )
        for reason, overrides, calendar, pool_enabled in cases:
            with self.subTest(reason=reason):
                config = self._make_config(**{
                    "market_opportunity_mode": "paper",
                    "market_opportunity_run_time": "20:00",
                    **overrides,
                })
                calendar_patch = (
                    {"side_effect": calendar} if isinstance(calendar, Exception) else {"return_value": calendar}
                )
                with patch("main._market_opportunity_evening_last_session", None), \
                     patch("main._market_opportunity_pool_enabled", return_value=pool_enabled), \
                     patch("src.core.trading_calendar.is_market_open", **calendar_patch) as market_open, \
                     patch("src.services.market_opportunity_service.MarketOpportunityService") as service_cls, \
                     patch("main._run_market_opportunity_pool_if_enabled") as pool, \
                     patch("main.datetime", self._china_clock(datetime(2026, 10, 9, 20, 30))):
                    result = main._run_market_opportunity_pool_evening_if_due(config)
                    # A gate skip never consumes the session's single attempt.
                    self.assertIsNone(main._market_opportunity_evening_last_session)

                self.assertEqual(result, {"status": "skipped", "reason": reason})
                if reason != "evening_run_disabled":
                    market_open.assert_called_once_with("cn", date(2026, 10, 9), strict=True)
                service_cls.assert_not_called()
                pool.assert_not_called()

    def test_evening_pool_failure_surfaces_without_same_session_retry(self) -> None:
        config = self._make_config(market_opportunity_mode="paper", market_opportunity_run_time="20:00")

        with patch("main._market_opportunity_evening_last_session", None), \
             patch("main._market_opportunity_pool_enabled", return_value=True), \
             patch("src.core.trading_calendar.is_market_open", return_value=True), \
             patch("src.services.market_opportunity_service.MarketOpportunityService") as service_cls, \
             patch("main._run_market_opportunity_pool_if_enabled", side_effect=RuntimeError("pool_down")) as pool, \
             patch("main.datetime", self._china_clock(datetime(2026, 10, 9, 20, 30))):
            service_cls.return_value.market_review_readiness.return_value = {"ready": True, "session": "2026-10-09"}
            with self.assertRaisesRegex(RuntimeError, "pool_down"):
                main._run_market_opportunity_pool_evening_if_due(config)
            again = main._run_market_opportunity_pool_evening_if_due(config)

        self.assertEqual(again, {"status": "skipped", "reason": "already_ran_today"})
        pool.assert_called_once_with(config)

    def _run_evening_as_owner(self, config, owner_id, clock, pool_side_effect=None):
        """Run one evening tick as a separate scheduler instance against the real lease."""
        with patch.dict(os.environ, {"DSA_SCHEDULER_OWNER_ID": owner_id}, clear=False), \
             patch("main._claim_market_opportunity_evening_session", _REAL_EVENING_CLAIM), \
             patch("main._finish_market_opportunity_evening_session", _REAL_EVENING_FINISH), \
             patch("main._market_opportunity_evening_last_session", None), \
             patch("main._market_opportunity_pool_enabled", return_value=True), \
             patch("src.core.trading_calendar.is_market_open", return_value=True), \
             patch("src.services.market_opportunity_service.MarketOpportunityService") as service_cls, \
             patch("main._run_market_opportunity_pool_if_enabled", side_effect=pool_side_effect) as pool, \
             patch("main.datetime", self._china_clock(clock)):
            service_cls.return_value.market_review_readiness.return_value = {"ready": True, "session": "2026-10-09"}
            try:
                result = main._run_market_opportunity_pool_evening_if_due(config)
            except Exception as exc:  # surfaced like the scheduler would see it
                result = exc
            return result, pool.call_count

    def _with_temp_lease_db(self):
        from src.storage import DatabaseManager

        db_patch = patch.dict(os.environ, {"DATABASE_PATH": str(Path(self.temp_dir.name) / "lease.db")}, clear=False)
        db_patch.start()
        DatabaseManager.reset_instance()
        Config.reset_instance()

        def cleanup():
            DatabaseManager.reset_instance()
            db_patch.stop()

        self.addCleanup(cleanup)

    def test_evening_pool_runs_once_per_session_across_scheduler_instances(self) -> None:
        # Server runtime scheduler + analyzer CLI scheduler: different processes,
        # different 5-minute buckets (20:00 vs 20:05), one shared DB.
        self._with_temp_lease_db()
        config = self._make_config(market_opportunity_mode="paper", market_opportunity_run_time="20:00")

        first, first_calls = self._run_evening_as_owner(config, "analyzer:1", datetime(2026, 10, 9, 20, 0))
        second, second_calls = self._run_evening_as_owner(config, "server:1", datetime(2026, 10, 9, 20, 5))
        next_session, next_calls = self._run_evening_as_owner(config, "server:1", datetime(2026, 10, 12, 20, 5))

        self.assertIsNone(first)
        self.assertEqual(first_calls, 1)
        self.assertEqual(second, {"status": "skipped", "reason": "session_claimed_by_other_instance"})
        self.assertEqual(second_calls, 0)
        self.assertIsNone(next_session)
        self.assertEqual(next_calls, 1)

        from src.services.scheduler_heartbeat import scheduler_task_states

        lease = scheduler_task_states()["tasks"]["market_opportunity_evening_session"]["lease"]
        self.assertEqual(lease["status"], "succeeded")

    def test_evening_pool_running_or_failed_session_is_not_rerun_by_other_instance(self) -> None:
        self._with_temp_lease_db()
        config = self._make_config(market_opportunity_mode="paper", market_opportunity_run_time="20:00")
        observed = {}

        def pool_while_running(_config):
            # The other instance ticks while this run is still in progress.
            observed["during"] = self._run_evening_as_owner(config, "server:1", datetime(2026, 10, 9, 20, 5))
            raise RuntimeError("pool_down")

        failed, _ = self._run_evening_as_owner(
            config, "analyzer:1", datetime(2026, 10, 9, 20, 0), pool_side_effect=pool_while_running
        )
        after_failure, after_calls = self._run_evening_as_owner(config, "server:1", datetime(2026, 10, 9, 20, 30))

        self.assertIsInstance(failed, RuntimeError)
        self.assertEqual(observed["during"], ({"status": "skipped", "reason": "session_claimed_by_other_instance"}, 0))
        self.assertEqual(after_failure, {"status": "skipped", "reason": "session_claimed_by_other_instance"})
        self.assertEqual(after_calls, 0)

    def test_evening_pool_unavailable_session_claim_skips_without_consuming_session(self) -> None:
        config = self._make_config(market_opportunity_mode="paper", market_opportunity_run_time="20:00")
        self.evening_claim.side_effect = [RuntimeError("db_down"), {"claimed": True, "lease_key": "k"}]

        with patch("main._market_opportunity_evening_last_session", None), \
             patch("main._market_opportunity_pool_enabled", return_value=True), \
             patch("src.core.trading_calendar.is_market_open", return_value=True), \
             patch("src.services.market_opportunity_service.MarketOpportunityService") as service_cls, \
             patch("main._run_market_opportunity_pool_if_enabled") as pool, \
             patch("main.datetime", self._china_clock(datetime(2026, 10, 9, 20, 30))):
            service_cls.return_value.market_review_readiness.return_value = {"ready": True, "session": "2026-10-09"}
            unavailable = main._run_market_opportunity_pool_evening_if_due(config)
            self.assertIsNone(main._market_opportunity_evening_last_session)
            ran = main._run_market_opportunity_pool_evening_if_due(config)

        self.assertEqual(unavailable, {"status": "skipped", "reason": "session_claim_unavailable"})
        self.assertIsNone(ran)
        pool.assert_called_once_with(config)
        self.evening_finish.assert_called_once_with("k")

    def test_schedule_mode_batch_holds_shared_lock_and_registers_evening_runner(self) -> None:
        from src.services.runtime_scheduler import global_analysis_running

        args = self._make_args(schedule=True)
        config = self._make_config(market_opportunity_mode="paper", market_opportunity_run_time="20:00")
        scheduled_call = {}
        lock_held_during_batch = []

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            shared_lease_enabled=None,
        ):
            scheduled_call["background_tasks"] = background_tasks or []
            scheduled_call["result"] = task()

        def scheduled(_config, _args, _stock_codes):
            lock_held_during_batch.append(global_analysis_running())
            return True

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._reload_runtime_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main.setup_logging"), \
             patch("main._market_opportunity_pool_enabled", return_value=True), \
             patch("main.run_scheduled_analysis", side_effect=scheduled) as scheduled_mock, \
             patch(
                 "main._run_market_opportunity_pool_evening_if_due",
                 return_value={"status": "skipped", "reason": "before_run_time"},
             ) as evening_runner, \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertIs(scheduled_call["result"], True)
        self.assertEqual(lock_held_during_batch, [True])
        self.assertFalse(global_analysis_running())
        scheduled_mock.assert_called_once_with(config, args, None)
        evening_tasks = [
            task for task in scheduled_call["background_tasks"]
            if task["name"] == "market_opportunity_evening_run"
        ]
        self.assertEqual(len(evening_tasks), 1)
        self.assertEqual(evening_tasks[0]["task"](), {"status": "skipped", "reason": "before_run_time"})
        evening_runner.assert_called_once_with(config)

    def test_daily_market_context_target_date_routes_jp_kr_calendars(self) -> None:
        current_time = datetime(2026, 5, 7, 0, 30, tzinfo=timezone.utc)
        calls = []

        def resolve_effective_date(market, *, current_time=None):
            calls.append((market, current_time))
            return date(2026, 5, 7)

        with patch(
            "src.core.trading_calendar.get_effective_trading_date",
            side_effect=resolve_effective_date,
        ):
            self.assertEqual(
                main._resolve_daily_market_context_target_date("jp", current_time),
                date(2026, 5, 7),
            )
            self.assertEqual(
                main._resolve_daily_market_context_target_date("kr", current_time),
                date(2026, 5, 7),
            )
            self.assertEqual(
                main._resolve_daily_market_context_target_date("jp,kr", current_time),
                date(2026, 5, 7),
            )

        self.assertEqual(
            calls,
            [
                ("jp", current_time),
                ("kr", current_time),
                ("jp", current_time),
            ],
        )

    def test_compute_trading_day_filter_supports_comma_list_regions(self) -> None:
        args = self._make_args()
        config = self._make_config(
            trading_day_check_enabled=True,
            market_review_enabled=True,
            market_review_region="jp,kr",
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )

        stock_codes = ["cn-stock", "jp-stock", "kr-stock", "us-stock"]

        with patch(
            "src.core.trading_calendar.get_market_for_stock",
            side_effect=lambda code: {"cn-stock": "cn", "jp-stock": "jp", "kr-stock": "kr", "us-stock": "us"}.get(code),
        ), patch("src.core.trading_calendar.get_open_markets_today", return_value={"jp", "kr"}):
            filtered_codes, effective_region, should_skip_all = main._compute_trading_day_filter(
                config,
                args,
                stock_codes,
            )

        self.assertEqual(filtered_codes, ["jp-stock", "kr-stock"])
        self.assertEqual(effective_region, "jp,kr")
        self.assertFalse(should_skip_all)

    def test_cn_exchange_symbols_follow_real_calendar_and_keep_identity(self) -> None:
        config = self._make_config(trading_day_check_enabled=True,
                                   market_review_enabled=True, market_review_region="cn")
        codes = ["513980.SH", "512480.SS", "513120.SH", "002714.SZ",
                 "SZ159915", "SH.601606", "920748.BJ", "600879"]
        from src.core.trading_calendar import is_market_open
        for session, expected_open in ((date(2026, 9, 12), False),
                                       (date(2026, 10, 1), False),
                                       (date(2026, 9, 14), True)):
            with self.subTest(session=session):
                self.assertEqual(is_market_open("cn", session), expected_open)
                with patch("src.core.trading_calendar.get_open_markets_today",
                           return_value={"cn"} if expected_open else set()):
                    result = main._compute_trading_day_filter(config, self._make_args(), codes)
                self.assertEqual(result, (codes, "cn", False) if expected_open else ([], "", True))
        with patch("src.core.trading_calendar.get_open_markets_today", return_value={"us"}):
            self.assertEqual(main._compute_trading_day_filter(
                config, self._make_args(), codes + ["AAPL"]), (["AAPL"], "", False))
        with patch("src.core.trading_calendar.get_open_markets_today", return_value=set()):
            self.assertEqual(main._compute_trading_day_filter(
                config, self._make_args(force_run=True), codes), (codes, None, False))
            config.trading_day_check_enabled = False
            self.assertEqual(main._compute_trading_day_filter(
                config, self._make_args(), codes), (codes, None, False))

    def test_closed_cn_batch_does_not_start_pipeline_or_notify(self) -> None:
        config = self._make_config(trading_day_check_enabled=True,
                                   market_review_enabled=True, market_review_region="cn")
        with patch("src.core.trading_calendar.get_open_markets_today", return_value=set()), \
             patch("main._refresh_stock_index_cache_for_analysis"), \
             patch("src.core.pipeline.StockAnalysisPipeline") as pipeline, \
             patch("src.core.market_review.run_market_review") as review, \
             patch("src.notification.NotificationService") as notifier:
            self.assertTrue(main.run_full_analysis(
                config, self._make_args(), ["513980.SH", "002714.SZ", "SZ159915"],
                raise_errors=True))
        pipeline.assert_not_called()
        review.assert_not_called()
        notifier.assert_not_called()

    def test_calendar_unavailable_blocks_pipeline_but_force_remains_explicit(self) -> None:
        config = self._make_config(trading_day_check_enabled=True,
                                   market_review_enabled=True, market_review_region="cn")
        with patch("src.core.trading_calendar._XCALS_AVAILABLE", False), \
             patch("main._refresh_stock_index_cache_for_analysis"), \
             patch("src.core.pipeline.StockAnalysisPipeline") as pipeline, \
             patch("src.core.market_review.run_market_review") as review:
            with self.assertRaisesRegex(RuntimeError, "calendar_unavailable"):
                main.run_full_analysis(config, self._make_args(), ["513980.SH"], raise_errors=True)
            self.assertEqual(main._compute_trading_day_filter(
                config, self._make_args(force_run=True), ["513980.SH"]), (["513980.SH"], None, False))
        pipeline.assert_not_called()
        review.assert_not_called()

    def test_unknown_market_is_not_automatic_permission(self) -> None:
        config = self._make_config(trading_day_check_enabled=True, market_review_enabled=False)
        with self.assertRaisesRegex(ValueError, "无法确定股票所属市场"):
            main._compute_trading_day_filter(config, self._make_args(), ["unrecognized.symbol"])

    def test_partial_analysis_is_reported_after_successful_results_are_preserved(self) -> None:
        config = self._make_config(trading_day_check_enabled=False, market_review_enabled=False)
        result = SimpleNamespace(code="002714", name="fixture", success=True,
                                 sentiment_score=50, operation_advice="观望", trend_prediction="震荡",
                                 get_emoji=lambda: "")
        with patch("main._refresh_stock_index_cache_for_analysis"), \
             patch("src.core.pipeline.StockAnalysisPipeline") as pipeline:
            pipeline.return_value.run.return_value = [result]
            with self.assertRaisesRegex(RuntimeError, "analysis_partial_failure: succeeded=1 expected=2"):
                main.run_full_analysis(config, self._make_args(), ["002714", "513980"], raise_errors=True)
            self.assertEqual(pipeline.return_value.run.return_value, [result])

    def test_public_webui_bind_warns_when_auth_is_disabled(self) -> None:
        with patch("src.auth.is_auth_enabled", return_value=False), \
             patch("main.logger.warning") as warning_log:
            main._warn_if_public_webui_without_auth("0.0.0.0")

        warning_log.assert_called_once()
        self.assertIn("WEBUI_HOST=%s", warning_log.call_args.args[0])
        self.assertEqual(warning_log.call_args.args[1], "0.0.0.0")

    def test_loopback_webui_bind_does_not_warn_when_auth_is_disabled(self) -> None:
        with patch("src.auth.is_auth_enabled", return_value=False), \
             patch("main.logger.warning") as warning_log:
            main._warn_if_public_webui_without_auth("127.0.0.1")

        warning_log.assert_not_called()

    def test_web_service_bind_uses_config_when_cli_omits_host_and_port(self) -> None:
        args = self._make_args(host=None, port=None)
        config = self._make_config(webui_host="127.0.0.1", webui_port=18000)

        host, port = main._resolve_web_service_bind(args, config)

        self.assertEqual(host, "127.0.0.1")
        self.assertEqual(port, 18000)

    def test_web_service_bind_keeps_explicit_cli_host_and_port(self) -> None:
        args = self._make_args(host="0.0.0.0", port=8000)
        config = self._make_config(webui_host="127.0.0.1", webui_port=18000)

        host, port = main._resolve_web_service_bind(args, config)

        self.assertEqual(host, "0.0.0.0")
        self.assertEqual(port, 8000)

    def test_serve_only_uses_config_bind_when_cli_omits_host_and_port(self) -> None:
        args = self._make_args(serve_only=True)
        config = self._make_config(webui_enabled=False, webui_host="127.0.0.1", webui_port=18000)
        observed_bind = []

        def fake_start_api_server(host, port, config):
            observed_bind.append((host, port))

        with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=False), \
             patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.prepare_webui_frontend_assets", return_value=True), \
             patch("main.start_api_server", side_effect=fake_start_api_server), \
             patch("main.start_bot_stream_clients"), \
             patch("main.time.sleep", side_effect=KeyboardInterrupt):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(observed_bind, [("127.0.0.1", 18000)])

    def test_serve_only_keeps_explicit_cli_bind_over_config(self) -> None:
        args = self._make_args(serve_only=True, host="0.0.0.0", port=8000)
        config = self._make_config(webui_enabled=False, webui_host="127.0.0.1", webui_port=18000)
        observed_bind = []

        def fake_start_api_server(host, port, config):
            observed_bind.append((host, port))

        with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=False), \
             patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.prepare_webui_frontend_assets", return_value=True), \
             patch("main.start_api_server", side_effect=fake_start_api_server), \
             patch("main.start_bot_stream_clients"), \
             patch("main.time.sleep", side_effect=KeyboardInterrupt):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(observed_bind, [("0.0.0.0", 8000)])

    def test_start_api_server_fails_before_thread_when_port_is_busy(self) -> None:
        config = self._make_config(log_level="INFO")

        class BusySocket:
            def bind(self, address):
                raise OSError("address already in use")

            def close(self):
                pass

        with patch("socket.socket", return_value=BusySocket()) as socket_factory, \
             patch("threading.Thread") as thread_cls:
            with self.assertRaises(RuntimeError) as caught:
                main.start_api_server("127.0.0.1", 8000, config)

        socket_factory.assert_called_once_with(socket.AF_INET, socket.SOCK_STREAM)
        self.assertIn("127.0.0.1:8000", str(caught.exception))
        thread_cls.assert_not_called()

    def test_start_api_server_fails_when_uvicorn_background_startup_fails(self) -> None:
        config = self._make_config(log_level="INFO")

        class _FakeUvicornServer:
            def __init__(self, config):
                self.config = config
                self.started = False

            def run(self) -> None:
                raise RuntimeError("lifespan bootstrap failed")

        class _FakeUvicornConfig:
            def __init__(self, *args, **kwargs):
                pass

        class _FakeUvicornModule:
            Config = _FakeUvicornConfig

            Server = _FakeUvicornServer

        class _UnusedSocket:
            def bind(self, address):
                pass

            def close(self):
                pass

        with patch("socket.socket", return_value=_UnusedSocket()), \
             patch.dict(
                 "sys.modules",
                 {"uvicorn": _FakeUvicornModule(), **_api_app_stub_modules()},
             ):

            with self.assertRaises(RuntimeError) as caught:
                main.start_api_server("127.0.0.1", 8000, config)

        self.assertIn("lifespan bootstrap failed", str(caught.exception))

    def test_start_api_server_compatible_with_uvicorn_install_signal_handlers_method(self) -> None:
        config = self._make_config(log_level="INFO")

        class _CompatServer:
            instance = None

            def __init__(self, config):
                type(self).instance = self
                self.config = config
                self.started = False
                self.install_signal_handlers = self._install_signal_handlers

            def _install_signal_handlers(self) -> None:
                return None

            def run(self) -> None:
                self.started = True

        class _CompatConfig:
            def __init__(self, *args, **kwargs):
                if "install_signal_handlers" in kwargs:
                    raise TypeError("install_signal_handlers is unsupported")

        class _UnusedSocket:
            def bind(self, address):
                pass

            def close(self):
                pass

        with patch("socket.socket", return_value=_UnusedSocket()), \
             patch.dict(
                 "sys.modules",
                 {
                     "uvicorn": SimpleNamespace(Config=_CompatConfig, Server=_CompatServer),
                     **_api_app_stub_modules(),
                 },
             ):
            main.start_api_server("127.0.0.1", 8000, config)

        self.assertIsNotNone(_CompatServer.instance)
        self.assertTrue(callable(_CompatServer.instance.install_signal_handlers))
        self.assertTrue(_CompatServer.instance.started)

    def test_schedule_mode_ignores_cli_stock_snapshot(self) -> None:
        args = self._make_args(schedule=True, stocks="600519,000001")
        config = self._make_config(schedule_enabled=False)
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            shared_lease_enabled=None,
        ):
            scheduled_call["schedule_time"] = schedule_time
            scheduled_call["run_immediately"] = run_immediately
            scheduled_call["background_tasks"] = background_tasks or []
            scheduled_call["shared_lease_enabled"] = shared_lease_enabled
            scheduled_call["resolved_schedule_time"] = (
                schedule_time_provider() if schedule_time_provider is not None else None
            )
            task()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._reload_runtime_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("main.logger.warning") as warning_log, \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(scheduled_call["schedule_time"], "18:00")
        self.assertEqual(scheduled_call["run_immediately"], True)
        self.assertEqual(scheduled_call["shared_lease_enabled"], True)
        self.assertEqual(scheduled_call["resolved_schedule_time"], "18:00")
        self.assertEqual(
            [
                {
                    "name": task["name"],
                    "interval_seconds": task["interval_seconds"],
                    "run_immediately": task["run_immediately"],
                }
                for task in scheduled_call["background_tasks"]
            ],
            [
                {
                    "name": "paper_trading_opening_confirmation_0930",
                    "interval_seconds": 30,
                    "run_immediately": False,
                },
                {
                    "name": "paper_trading_intraday_monitor",
                    "interval_seconds": 5 * 60,
                    "run_immediately": False,
                },
                {
                    "name": "market_opportunity_evening_run",
                    "interval_seconds": 5 * 60,
                    "run_immediately": False,
                },
            ],
        )
        opening_task = scheduled_call["background_tasks"][0]
        self.assertEqual(opening_task["active_time_start"], "09:30:00")
        self.assertEqual(opening_task["active_time_end"], "09:35:00")
        self.assertEqual(opening_task["active_timezone"], "Asia/Shanghai")
        evening_task = scheduled_call["background_tasks"][-1]
        self.assertEqual(evening_task["active_time_start"], "15:00:00")
        self.assertEqual(evening_task["active_time_end"], "23:59:59")
        self.assertEqual(evening_task["active_timezone"], "Asia/Shanghai")
        self.assertEqual(evening_task["lease_task_name"], "market_opportunity_evening_run")
        run_full_analysis.assert_called_once_with(config, args, None, raise_errors=True)
        warning_log.assert_any_call(
            "定时模式下检测到 --stocks 参数；计划执行将忽略启动时股票快照，并在每次运行前重新读取最新的 STOCK_LIST。"
        )

    def test_standalone_run_resolves_stocks_before_run_full_analysis(self) -> None:
        args = self._make_args(stocks="005930")
        config = self._make_config(run_immediately=True)

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        run_full_analysis.assert_called_once()
        _, _, stock_codes = run_full_analysis.call_args.args
        self.assertEqual(stock_codes, ["005930.KS"])

    def test_schedule_mode_reload_uses_latest_runtime_config(self) -> None:
        args = self._make_args(schedule=True)
        startup_config = self._make_config(schedule_enabled=True, schedule_time="18:00")
        runtime_config = self._make_config(schedule_enabled=True, schedule_time="09:30")
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            shared_lease_enabled=None,
        ):
            scheduled_call["schedule_time"] = schedule_time
            scheduled_call["shared_lease_enabled"] = shared_lease_enabled
            scheduled_call["resolved_schedule_time"] = (
                schedule_time_provider() if schedule_time_provider is not None else None
            )
            task()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=startup_config), \
             patch("main._reload_runtime_config", return_value=runtime_config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "09:30"), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            scheduled_call,
            {"schedule_time": "18:00", "shared_lease_enabled": True, "resolved_schedule_time": "09:30"},
        )
        run_full_analysis.assert_called_once_with(runtime_config, args, None, raise_errors=True)

    def test_schedule_mode_registers_event_monitor_background_task(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=False,
            agent_event_monitor_enabled=True,
            agent_event_monitor_interval_minutes=7,
        )
        worker = MagicMock()
        worker.run_once.return_value = {"triggered": 2}
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            shared_lease_enabled=None,
        ):
            scheduled_call["schedule_time"] = schedule_time
            scheduled_call["run_immediately"] = run_immediately
            scheduled_call["background_tasks"] = background_tasks or []
            scheduled_call["shared_lease_enabled"] = shared_lease_enabled
            scheduled_call["resolved_schedule_time"] = (
                schedule_time_provider() if schedule_time_provider is not None else None
            )

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._reload_runtime_config", return_value=config) as reload_config, \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("src.services.alert_worker.AlertWorker", return_value=worker) as worker_cls, \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        worker_cls.assert_called_once()
        self.assertIs(worker_cls.call_args.kwargs["config_provider"], reload_config)
        run_full_analysis.assert_not_called()
        self.assertEqual(scheduled_call["schedule_time"], "18:00")
        self.assertEqual(scheduled_call["run_immediately"], True)
        self.assertEqual(scheduled_call["shared_lease_enabled"], True)
        self.assertEqual(scheduled_call["resolved_schedule_time"], "18:00")
        background_tasks = scheduled_call["background_tasks"]
        event_tasks = [task for task in background_tasks if task["name"] == "agent_event_monitor"]
        paper_tasks = [task for task in background_tasks if task["name"] == "paper_trading_intraday_monitor"]
        self.assertEqual(len(event_tasks), 1)
        self.assertEqual(len(paper_tasks), 1)
        background_task = event_tasks[0]
        self.assertEqual(background_task["name"], "agent_event_monitor")
        self.assertEqual(background_task["interval_seconds"], 7 * 60)
        self.assertEqual(background_task["run_immediately"], True)

        with patch("main.logger.info") as info_log:
            background_task["task"]()

        worker.run_once.assert_called_once_with()
        info_log.assert_any_call("[EventMonitor] 本轮触发 %d 条提醒", 2)

    def test_schedule_mode_registers_event_monitor_worker_without_legacy_rules(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=False,
            agent_event_monitor_enabled=True,
            agent_event_alert_rules_json="",
        )
        worker = MagicMock()
        worker.run_once.return_value = {"triggered": 0}
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            shared_lease_enabled=None,
        ):
            scheduled_call["background_tasks"] = background_tasks or []
            scheduled_call["shared_lease_enabled"] = shared_lease_enabled

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._reload_runtime_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("src.services.alert_worker.AlertWorker", return_value=worker) as worker_cls, \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        worker_cls.assert_called_once()
        run_full_analysis.assert_not_called()
        self.assertEqual(scheduled_call["shared_lease_enabled"], True)
        background_tasks = scheduled_call["background_tasks"]
        self.assertEqual(
            [task["name"] for task in background_tasks],
            [
                "agent_event_monitor",
                "paper_trading_opening_confirmation_0930",
                "paper_trading_intraday_monitor",
                "market_opportunity_evening_run",
            ],
        )

    def test_check_notify_returns_before_other_modes(self) -> None:
        args = self._make_args(check_notify=True, serve=True, schedule=True, market_review=True)
        config = self._make_config(webui_enabled=False)
        diagnostic_result = SimpleNamespace(ok=True)

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch("main.start_api_server") as start_api_server, \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch(
                 "src.services.notification_diagnostics.run_notification_diagnostics",
                 return_value=diagnostic_result,
             ) as run_diagnostics, \
             patch(
                 "src.services.notification_diagnostics.format_notification_diagnostics",
                 return_value="通知配置诊断",
             ), \
             patch("builtins.print") as print_output:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        run_diagnostics.assert_called_once_with(config)
        print_output.assert_called_once_with("通知配置诊断")
        start_api_server.assert_not_called()
        run_full_analysis.assert_not_called()

    def test_serve_mode_exits_when_api_server_start_fails(self) -> None:
        args = self._make_args(serve_only=True, host="127.0.0.1", port=8000)
        config = self._make_config(webui_enabled=False)

        with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=False), \
             patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.prepare_webui_frontend_assets", return_value=True), \
             patch("main.start_api_server", side_effect=RuntimeError("port busy")), \
             patch("main.start_bot_stream_clients") as start_bots, \
             patch("main.logger.error") as error_log:
            exit_code = main.main()

        self.assertEqual(exit_code, 1)
        start_bots.assert_not_called()
        error_log.assert_called_once()

    def test_webui_only_maps_to_serve_only_and_exits_when_api_server_start_fails(self) -> None:
        args = self._make_args(webui_only=True, host="127.0.0.1", port=8000)
        config = self._make_config(webui_enabled=False)

        with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=False), \
             patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.prepare_webui_frontend_assets", return_value=True), \
             patch("main.start_api_server", side_effect=RuntimeError("port busy")), \
             patch("main.start_bot_stream_clients") as start_bots, \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("main.logger.error") as error_log:
            exit_code = main.main()

        self.assertEqual(exit_code, 1)
        start_bots.assert_not_called()
        run_full_analysis.assert_not_called()
        error_log.assert_called_once()

    def test_serve_mode_continues_single_analysis_when_api_server_start_fails(self) -> None:
        args = self._make_args(serve=True, host="127.0.0.1", port=8000)
        config = self._make_config(webui_enabled=False, run_immediately=True)

        with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=False), \
             patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.prepare_webui_frontend_assets", return_value=True), \
             patch("main.start_api_server", side_effect=RuntimeError("port busy")), \
             patch("main.start_bot_stream_clients") as start_bots, \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("main._run_analysis_with_runtime_scheduler_lock") as run_with_lock, \
             patch("main.logger.error") as error_log:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        start_bots.assert_not_called()
        run_with_lock.assert_called_once_with(config, args, None)
        run_full_analysis.assert_not_called()
        error_log.assert_called_once()

    def test_serve_schedule_mode_continues_scheduler_when_api_server_start_fails(self) -> None:
        args = self._make_args(serve=True, schedule=True, host="127.0.0.1", port=8000)
        config = self._make_config(webui_enabled=False, schedule_enabled=False)
        scheduled_call = {}

        def fake_run_with_schedule(
            task,
            schedule_time,
            run_immediately,
            background_tasks=None,
            schedule_time_provider=None,
            shared_lease_enabled=None,
        ):
            scheduled_call["schedule_time"] = schedule_time
            scheduled_call["run_immediately"] = run_immediately
            scheduled_call["background_tasks"] = background_tasks or []
            scheduled_call["shared_lease_enabled"] = shared_lease_enabled
            task()

        with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=False), \
             patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._reload_runtime_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main.prepare_webui_frontend_assets", return_value=True), \
             patch("main.start_api_server", side_effect=RuntimeError("port busy")), \
             patch("main.start_bot_stream_clients") as start_bots, \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("src.scheduler.run_with_schedule", side_effect=fake_run_with_schedule), \
             patch("main.logger.error") as error_log:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        start_bots.assert_not_called()
        run_full_analysis.assert_called_once_with(config, args, None, raise_errors=True)
        self.assertEqual(scheduled_call["schedule_time"], "18:00")
        self.assertEqual(scheduled_call["run_immediately"], True)
        self.assertEqual(scheduled_call["shared_lease_enabled"], True)
        self.assertEqual(
            [
                {
                    "name": task["name"],
                    "interval_seconds": task["interval_seconds"],
                    "run_immediately": task["run_immediately"],
                }
                for task in scheduled_call["background_tasks"]
            ],
            [
                {
                    "name": "paper_trading_opening_confirmation_0930",
                    "interval_seconds": 30,
                    "run_immediately": False,
                },
                {
                    "name": "paper_trading_intraday_monitor",
                    "interval_seconds": 5 * 60,
                    "run_immediately": False,
                },
                {
                    "name": "market_opportunity_evening_run",
                    "interval_seconds": 5 * 60,
                    "run_immediately": False,
                },
            ],
        )
        error_log.assert_called_once()

    def test_paper_trading_intraday_monitor_is_dormant_until_enabled(self) -> None:
        config = self._make_config(
            paper_trading_enabled=True,
            paper_trading_intraday_enabled=False,
            paper_trading_intraday_interval_minutes=5,
        )

        with patch("main._build_paper_trading_service") as build_service:
            main._run_paper_trading_intraday_monitor_if_enabled(config)

        build_service.assert_not_called()

    def test_monitor_failure_reaches_scheduler_and_shared_lease_status(self) -> None:
        from src.scheduler import Scheduler

        config = self._make_config(paper_trading_enabled=True, paper_trading_intraday_enabled=True,
                                   paper_trading_intraday_interval_minutes=5)

        class OpeningClock:
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 9, 7, 9, 31).replace(tzinfo=tz)

        for helper, method in (
            (main._run_paper_trading_intraday_monitor_if_enabled, "run_intraday_monitor"),
            (main._run_paper_trading_opening_confirmation_if_enabled, "run_opening_confirmation_check"),
        ):
            with self.subTest(helper=helper.__name__):
                scheduler = Scheduler(register_signals=False, shared_lease_enabled=False)
                scheduler.add_background_task(lambda: helper(config), interval_seconds=300, name="monitor-regression")
                entry = scheduler._background_tasks[0]
                service = MagicMock()
                getattr(service, method).side_effect = RuntimeError("monitor_database_unavailable")
                done = threading.Event()
                with patch("main._build_paper_trading_service", return_value=service), patch(
                    "main._paper_trading_intraday_last_attempt_monotonic", None
                ), patch("main._paper_trading_opening_confirmation_last_attempt_monotonic", None), patch(
                    "main.datetime", OpeningClock
                ), patch("src.core.trading_calendar.is_market_open", return_value=True), patch.object(
                    scheduler, "_claim_shared_lease", return_value={"claimed": True, "lease_key": "isolated"}
                ), patch.object(scheduler, "_finish_shared_lease", side_effect=lambda *a, **k: done.set()) as finish:
                    self.assertTrue(scheduler._start_background_task(entry))
                    self.assertTrue(done.wait(5))
                    worker = entry.get("thread")
                    if worker is not None:
                        worker.join(5)
                    self.assertIsNone(entry["last_success_at"])
                    self.assertEqual(entry["last_error"], "monitor_database_unavailable")
                    self.assertEqual(finish.call_args.kwargs["status"], "failed")

                    # The immediate retry is throttled, not a recovery.  It
                    # must preserve the prior error in both task and lease UI.
                    done.clear()
                    self.assertTrue(scheduler._start_background_task(entry))
                    self.assertTrue(done.wait(5))
                    worker = entry.get("thread")
                    if worker is not None:
                        worker.join(5)
                    self.assertEqual(entry["last_error"], "monitor_database_unavailable")
                    self.assertIsNone(entry["last_success_at"])
                    self.assertEqual(finish.call_args.kwargs["status"], "skipped")
                    self.assertEqual(finish.call_args.kwargs["error_message"], "monitor_database_unavailable")
                    self.assertEqual(entry["last_skip_reason"], "monitor_throttled")
                    getattr(service, method).assert_called_once()

    def test_opening_confirmation_helper_does_not_build_service_outside_window(self) -> None:
        config = self._make_config(
            paper_trading_enabled=True,
            paper_trading_intraday_enabled=True,
        )

        class FakeDateTime:
            @classmethod
            def now(cls, tz=None):
                value = datetime(2026, 8, 27, 10, 15, 0)
                return value.replace(tzinfo=tz) if tz is not None else value

        with patch("main.datetime", FakeDateTime), \
             patch("main._build_paper_trading_service") as build_service:
            main._run_paper_trading_opening_confirmation_if_enabled(config)

        build_service.assert_not_called()

    def test_opening_confirmation_helper_runs_inside_window(self) -> None:
        config = self._make_config(
            paper_trading_enabled=True,
            paper_trading_intraday_enabled=True,
        )
        service = MagicMock()
        service.run_opening_confirmation_check.return_value = {
            "observations": 1,
            "confirmed": 1,
            "reanalysis_requested": 0,
            "failed_closed": 0,
        }

        class FakeDateTime:
            @classmethod
            def now(cls, tz=None):
                value = datetime(2026, 8, 27, 9, 31, 0)
                return value.replace(tzinfo=tz) if tz is not None else value

        previous_attempt = main._paper_trading_opening_confirmation_last_attempt_monotonic
        main._paper_trading_opening_confirmation_last_attempt_monotonic = None
        try:
            with patch("main.datetime", FakeDateTime), \
                 patch("main._build_paper_trading_service", return_value=service):
                main._run_paper_trading_opening_confirmation_if_enabled(config)
        finally:
            main._paper_trading_opening_confirmation_last_attempt_monotonic = previous_attempt

        service.run_opening_confirmation_check.assert_called_once_with()

    def test_opening_confirmation_helper_skips_non_trading_day(self) -> None:
        config = self._make_config(
            paper_trading_enabled=True,
            paper_trading_intraday_enabled=True,
        )

        class FakeDateTime:
            @classmethod
            def now(cls, tz=None):
                value = datetime(2026, 8, 29, 9, 31, 0)
                return value.replace(tzinfo=tz) if tz is not None else value

        with patch("main.datetime", FakeDateTime), \
             patch("src.core.trading_calendar.is_market_open", return_value=False), \
             patch("main._build_paper_trading_service") as build_service:
            main._run_paper_trading_opening_confirmation_if_enabled(config)

        build_service.assert_not_called()

    def test_paper_trading_intraday_monitor_throttles_by_latest_interval(self) -> None:
        config = self._make_config(
            paper_trading_enabled=True,
            paper_trading_intraday_enabled=True,
            paper_trading_intraday_interval_minutes=5,
        )
        service = MagicMock()
        service.run_intraday_monitor.return_value = {
            "observations": 0,
            "positions_opened": 0,
            "positions_closed": 0,
        }
        previous_attempt = main._paper_trading_intraday_last_attempt_monotonic
        main._paper_trading_intraday_last_attempt_monotonic = None
        try:
            with patch("main._build_paper_trading_service", return_value=service), \
                 patch("main.time.monotonic", side_effect=[100.0, 160.0, 401.0]):
                first = main._run_paper_trading_intraday_monitor_if_enabled(config)
                throttled = main._run_paper_trading_intraday_monitor_if_enabled(config)
                second = main._run_paper_trading_intraday_monitor_if_enabled(config)
        finally:
            main._paper_trading_intraday_last_attempt_monotonic = previous_attempt

        self.assertEqual(first["observations"], 0)
        self.assertEqual(throttled, {"status": "skipped", "reason": "monitor_throttled"})
        self.assertEqual(second["observations"], 0)
        self.assertEqual(service.run_intraday_monitor.call_count, 2)
        for call in service.run_intraday_monitor.call_args_list:
            self.assertEqual(call.kwargs["interval_seconds"], 5 * 60)

    def test_paper_trading_intraday_monitor_uses_reduced_interval_without_restart(self) -> None:
        config = self._make_config(
            paper_trading_enabled=True,
            paper_trading_intraday_enabled=True,
            paper_trading_intraday_interval_minutes=10,
        )
        service = MagicMock()
        service.run_intraday_monitor.return_value = {
            "observations": 0,
            "positions_opened": 0,
            "positions_closed": 0,
        }
        previous_attempt = main._paper_trading_intraday_last_attempt_monotonic
        main._paper_trading_intraday_last_attempt_monotonic = None
        try:
            with patch("main._build_paper_trading_service", return_value=service), \
                 patch("main.time.monotonic", side_effect=[100.0, 160.0]):
                main._run_paper_trading_intraday_monitor_if_enabled(config)
                config.paper_trading_intraday_interval_minutes = 1
                main._run_paper_trading_intraday_monitor_if_enabled(config)
        finally:
            main._paper_trading_intraday_last_attempt_monotonic = previous_attempt

        self.assertEqual(service.run_intraday_monitor.call_count, 2)
        self.assertEqual(
            [call.kwargs["interval_seconds"] for call in service.run_intraday_monitor.call_args_list],
            [10 * 60, 60],
        )

    def test_serve_with_enabled_schedule_uses_api_runtime_scheduler(self) -> None:
        from src.services.runtime_scheduler import (
            CLI_SCHEDULER_OWNER_ENV,
            RUNTIME_SCHEDULER_ARGS_ENV,
            RUNTIME_SCHEDULER_RUN_IMMEDIATELY_ENV,
        )

        args = self._make_args(
            serve=True,
            schedule=False,
            host="127.0.0.1",
            port=8000,
            no_notify=True,
            no_market_review=True,
            dry_run=True,
            force_run=True,
            single_notify=True,
            no_context_snapshot=True,
            workers=4,
        )
        config = self._make_config(webui_enabled=False, schedule_enabled=True)
        marker_seen_by_server = []
        run_immediately_seen_by_server = []
        runtime_args_seen_by_server = []

        def fake_start_api_server(host, port, config):
            marker_seen_by_server.append(os.getenv(CLI_SCHEDULER_OWNER_ENV))
            run_immediately_seen_by_server.append(os.getenv(RUNTIME_SCHEDULER_RUN_IMMEDIATELY_ENV))
            runtime_args_seen_by_server.append(json.loads(os.getenv(RUNTIME_SCHEDULER_ARGS_ENV, "{}")))

        with patch.dict(
            os.environ,
            {"GITHUB_ACTIONS": "false", CLI_SCHEDULER_OWNER_ENV: "true"},
            clear=False,
        ), \
             patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.prepare_webui_frontend_assets", return_value=True), \
             patch("main.start_api_server", side_effect=fake_start_api_server), \
             patch("main.start_bot_stream_clients") as start_bots, \
             patch("main.time.sleep", side_effect=KeyboardInterrupt), \
             patch("src.scheduler.run_with_schedule") as run_with_schedule:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(marker_seen_by_server, [None])
        self.assertEqual(run_immediately_seen_by_server, ["true"])
        self.assertEqual(runtime_args_seen_by_server, [{
            "no_notify": True,
            "no_market_review": True,
            "dry_run": True,
            "force_run": True,
            "single_notify": True,
            "no_context_snapshot": True,
            "workers": 4,
        }])
        start_bots.assert_called_once_with(config)
        run_with_schedule.assert_not_called()

    def test_serve_mode_uses_shared_analysis_lock_for_immediate_run_full_analysis(self) -> None:
        args = self._make_args(serve=True, schedule=False, host="127.0.0.1", port=8000)
        config = self._make_config(webui_enabled=False, run_immediately=True)

        with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=False), \
             patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.prepare_webui_frontend_assets", return_value=True), \
             patch("main.start_api_server"), \
             patch("main.start_bot_stream_clients") as start_bots, \
             patch("main.time.sleep", side_effect=KeyboardInterrupt), \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("main._run_analysis_with_runtime_scheduler_lock") as run_with_lock:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(run_with_lock.call_count, 1)
        run_with_lock.assert_called_once_with(config, args, None)
        run_full_analysis.assert_not_called()
        start_bots.assert_called_once_with(config)

    def test_serve_schedule_flag_enables_api_runtime_scheduler(self) -> None:
        from src.services.runtime_scheduler import (
            CLI_SCHEDULER_OWNER_ENV,
            RUNTIME_SCHEDULER_ARGS_ENV,
            RUNTIME_SCHEDULER_FORCE_ENABLED_ENV,
            RUNTIME_SCHEDULER_RUN_IMMEDIATELY_ENV,
        )

        args = self._make_args(
            serve=True,
            schedule=True,
            host="127.0.0.1",
            port=8000,
            no_notify=True,
            no_market_review=True,
            dry_run=True,
            force_run=True,
            single_notify=True,
            no_context_snapshot=True,
            workers=4,
        )
        config = self._make_config(webui_enabled=False, schedule_enabled=False)
        marker_seen_by_server = []
        force_enabled_seen_by_server = []
        run_immediately_seen_by_server = []
        runtime_args_seen_by_server = []

        def fake_start_api_server(host, port, config):
            marker_seen_by_server.append(os.getenv(CLI_SCHEDULER_OWNER_ENV))
            force_enabled_seen_by_server.append(os.getenv(RUNTIME_SCHEDULER_FORCE_ENABLED_ENV))
            run_immediately_seen_by_server.append(os.getenv(RUNTIME_SCHEDULER_RUN_IMMEDIATELY_ENV))
            runtime_args_seen_by_server.append(json.loads(os.getenv(RUNTIME_SCHEDULER_ARGS_ENV, "{}")))

        with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=False), \
             patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.prepare_webui_frontend_assets", return_value=True), \
             patch("main.start_api_server", side_effect=fake_start_api_server), \
             patch("main.start_bot_stream_clients"), \
             patch("main.time.sleep", side_effect=KeyboardInterrupt), \
             patch("src.scheduler.run_with_schedule") as run_with_schedule:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(marker_seen_by_server, [None])
        self.assertEqual(force_enabled_seen_by_server, ["true"])
        self.assertEqual(run_immediately_seen_by_server, ["true"])
        self.assertEqual(runtime_args_seen_by_server, [{
            "no_notify": True,
            "no_market_review": True,
            "dry_run": True,
            "force_run": True,
            "single_notify": True,
            "no_context_snapshot": True,
            "workers": 4,
        }])
        self.assertFalse(config.schedule_enabled)
        run_with_schedule.assert_not_called()

    def test_serve_schedule_flag_passes_no_run_immediately_to_runtime_scheduler(self) -> None:
        from src.services.runtime_scheduler import RUNTIME_SCHEDULER_RUN_IMMEDIATELY_ENV

        args = self._make_args(
            serve=True,
            schedule=True,
            no_run_immediately=True,
            host="127.0.0.1",
            port=8000,
        )
        config = self._make_config(webui_enabled=False, schedule_enabled=False)
        run_immediately_seen_by_server = []

        def fake_start_api_server(host, port, config):
            run_immediately_seen_by_server.append(os.getenv(RUNTIME_SCHEDULER_RUN_IMMEDIATELY_ENV))

        with patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=False), \
             patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.prepare_webui_frontend_assets", return_value=True), \
             patch("main.start_api_server", side_effect=fake_start_api_server), \
             patch("main.start_bot_stream_clients"), \
             patch("main.time.sleep", side_effect=KeyboardInterrupt), \
             patch("src.scheduler.run_with_schedule") as run_with_schedule:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(run_immediately_seen_by_server, ["false"])
        run_with_schedule.assert_not_called()

    def test_serve_only_suppresses_startup_scheduler_without_disabling_runtime_owner(self) -> None:
        from src.services.runtime_scheduler import (
            CLI_SCHEDULER_OWNER_ENV,
            RUNTIME_SCHEDULER_RUN_IMMEDIATELY_ENV,
            RUNTIME_SCHEDULER_SUPPRESS_START_ENV,
        )

        args = self._make_args(serve_only=True, host="127.0.0.1", port=8000)
        config = self._make_config(webui_enabled=False, schedule_enabled=True)
        marker_seen_by_server = []
        suppress_seen_by_server = []
        run_immediately_seen_by_server = []

        def fake_start_api_server(host, port, config):
            marker_seen_by_server.append(os.getenv(CLI_SCHEDULER_OWNER_ENV))
            suppress_seen_by_server.append(os.getenv(RUNTIME_SCHEDULER_SUPPRESS_START_ENV))
            run_immediately_seen_by_server.append(os.getenv(RUNTIME_SCHEDULER_RUN_IMMEDIATELY_ENV))

        with patch.dict(
            os.environ,
            {"GITHUB_ACTIONS": "false", CLI_SCHEDULER_OWNER_ENV: "true"},
            clear=False,
        ), \
             patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.prepare_webui_frontend_assets", return_value=True), \
             patch("main.start_api_server", side_effect=fake_start_api_server), \
             patch("main.start_bot_stream_clients") as start_bots, \
             patch("main.time.sleep", side_effect=KeyboardInterrupt), \
             patch("src.scheduler.run_with_schedule") as run_with_schedule:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(marker_seen_by_server, [None])
        self.assertEqual(suppress_seen_by_server, ["true"])
        self.assertEqual(run_immediately_seen_by_server, [None])
        start_bots.assert_called_once_with(config)
        run_with_schedule.assert_not_called()

    def test_reload_runtime_config_preserves_process_env_overrides(self) -> None:
        self.env_path.write_text(
            "OPENAI_API_KEY=stale-file\nSCHEDULE_TIME=09:30\n",
            encoding="utf-8",
        )
        runtime_config = self._make_config(schedule_enabled=True, schedule_time="09:30")

        with patch.dict(
            os.environ,
            {
                "ENV_FILE": str(self.env_path),
                "OPENAI_API_KEY": "runtime-secret",
                "SCHEDULE_TIME": "18:00",
            },
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {"OPENAI_API_KEY": "runtime-secret"},
        ), patch.object(
            main,
            "_RUNTIME_ENV_FILE_KEYS",
            {"SCHEDULE_TIME"},
        ), patch(
            "main.get_config",
            return_value=runtime_config,
        ) as get_config_mock:
            reloaded_config = main._reload_runtime_config()
            self.assertEqual(os.environ["OPENAI_API_KEY"], "runtime-secret")
            self.assertEqual(os.environ["SCHEDULE_TIME"], "09:30")

        self.assertIs(reloaded_config, runtime_config)
        get_config_mock.assert_called_once_with()

    def test_reload_env_file_values_preserves_managed_env_vars_when_read_fails(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV_FILE": str(self.env_path),
                "OPENAI_API_KEY": "runtime-secret",
                "SCHEDULE_TIME": "09:30",
            },
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {},
        ), patch.object(
            main,
            "_RUNTIME_ENV_FILE_KEYS",
            {"OPENAI_API_KEY", "SCHEDULE_TIME"},
        ), patch(
            "main.dotenv_values",
            side_effect=OSError("boom"),
        ):
            main._reload_env_file_values_preserving_overrides()

            self.assertEqual(os.environ["OPENAI_API_KEY"], "runtime-secret")
            self.assertEqual(os.environ["SCHEDULE_TIME"], "09:30")
            self.assertEqual(
                main._RUNTIME_ENV_FILE_KEYS,
                {"OPENAI_API_KEY", "SCHEDULE_TIME"},
            )

    def test_reload_runtime_config_refreshes_env_before_resetting_singleton(self) -> None:
        runtime_config = self._make_config(schedule_enabled=True, schedule_time="09:30")
        call_order = []

        def fake_reload_env() -> None:
            call_order.append("reload_env")

        def fake_reset_instance() -> None:
            call_order.append("reset_instance")

        def fake_get_config():
            call_order.append("get_config")
            return runtime_config

        with patch(
            "main._reload_env_file_values_preserving_overrides",
            side_effect=fake_reload_env,
        ), patch(
            "main.Config.reset_instance",
            side_effect=fake_reset_instance,
        ), patch(
            "main.get_config",
            side_effect=fake_get_config,
        ):
            reloaded_config = main._reload_runtime_config()

        self.assertIs(reloaded_config, runtime_config)
        self.assertEqual(call_order, ["reload_env", "reset_instance", "get_config"])

    def test_schedule_time_provider_propagates_config_read_failures(self) -> None:
        with patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {},
        ), patch(
            "src.core.config_manager.ConfigManager.read_config_map",
            side_effect=RuntimeError("boom"),
        ):
            provider = main._build_schedule_time_provider("18:00")

            with self.assertRaisesRegex(RuntimeError, "boom"):
                provider()

    def test_schedule_time_provider_respects_process_env_precedence(self) -> None:
        with patch.dict(
            os.environ,
            {"SCHEDULE_TIME": "18:00"},
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {"SCHEDULE_TIME": "18:00"},
        ), patch(
            "src.core.config_manager.ConfigManager.read_config_map",
            side_effect=AssertionError("should not read .env when process env override exists"),
        ):
            provider = main._build_schedule_time_provider("09:30")

            self.assertEqual(provider(), "18:00")

    def test_schedule_time_provider_falls_back_to_system_default_on_clear(self) -> None:
        """When SCHEDULE_TIME is cleared/removed from config, provider returns '18:00'."""
        with patch.dict(
            os.environ,
            {"SCHEDULE_TIME": "09:30"},
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {},
        ), patch(
            "src.core.config_manager.ConfigManager.read_config_map",
            return_value={},
        ):
            provider = main._build_schedule_time_provider("09:30")
            self.assertEqual(provider(), "18:00")

    def test_schedule_time_provider_falls_back_to_system_default_on_empty(self) -> None:
        """When SCHEDULE_TIME is empty string in config, provider returns '18:00'."""
        with patch.dict(
            os.environ,
            {"SCHEDULE_TIME": "09:30"},
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {},
        ), patch(
            "src.core.config_manager.ConfigManager.read_config_map",
            return_value={"SCHEDULE_TIME": "  "},
        ):
            provider = main._build_schedule_time_provider("09:30")
            self.assertEqual(provider(), "18:00")

    def test_single_run_keeps_cli_stock_override(self) -> None:
        args = self._make_args(stocks="600519,000001")
        config = self._make_config(run_immediately=True)

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        run_full_analysis.assert_called_once_with(config, args, ["600519", "000001"])

    def test_run_full_analysis_skips_market_review_when_shared_lock_is_held(self) -> None:
        from src.core.market_review_lock import (
            release_market_review_lock,
            try_acquire_market_review_lock,
        )

        args = self._make_args()
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            daily_market_context_enabled=True,
            no_market_review=False,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline.run.return_value = []
        events = []
        pipeline_kwargs = {}

        def refresh_index(config_arg):
            events.append("refresh")

        def build_pipeline(*args, **kwargs):
            events.append("pipeline")
            pipeline_kwargs.update(kwargs)
            return pipeline

        lock_token = try_acquire_market_review_lock(config)
        self.assertIsNotNone(lock_token)
        try:
            with patch.object(main, "_refresh_stock_index_cache_for_analysis", side_effect=refresh_index) as refresh, \
                 patch("src.core.pipeline.StockAnalysisPipeline", side_effect=build_pipeline), \
                 patch("src.core.market_review.run_market_review") as run_market_review:
                main.run_full_analysis(config, args, [])
        finally:
            release_market_review_lock(lock_token)

            refresh.assert_called_once_with(config)
        self.assertEqual(events[:2], ["refresh", "pipeline"])
        self.assertTrue(pipeline_kwargs["daily_market_context_allow_generate"])
        pipeline.run.assert_called_once()
        run_market_review.assert_not_called()

    def test_run_full_analysis_disables_generation_when_no_market_review_flag_set(self) -> None:
        args = self._make_args(no_market_review=True)
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            daily_market_context_enabled=True,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline.run.return_value = []
        pipeline_kwargs = {}

        def build_pipeline(*args, **kwargs):
            pipeline_kwargs.update(kwargs)
            return pipeline

        with patch.object(main, "_refresh_stock_index_cache_for_analysis") as refresh, \
             patch("main._compute_trading_day_filter", return_value=([], "cn", False)), \
             patch("src.core.pipeline.StockAnalysisPipeline", side_effect=build_pipeline), \
             patch("main._prime_daily_market_context") as prime_context, \
             patch("src.core.market_review.run_market_review") as run_market_review:
            main.run_full_analysis(config, args, [])

        self.assertFalse(pipeline_kwargs["daily_market_context_allow_generate"])
        self.assertEqual(pipeline_kwargs["daily_market_context_enabled"], False)
        prime_context.assert_not_called()
        run_market_review.assert_not_called()
        refresh.assert_called_once_with(config)

    def test_run_full_analysis_defaults_daily_context_on_without_disabling_market_review(self) -> None:
        args = self._make_args()
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline.run.return_value = []
        pipeline_kwargs = {}

        def build_pipeline(*args, **kwargs):
            pipeline_kwargs.update(kwargs)
            return pipeline

        with patch.object(main, "_refresh_stock_index_cache_for_analysis") as refresh, \
             patch("main._compute_trading_day_filter", return_value=([], "cn", False)), \
             patch("src.core.pipeline.StockAnalysisPipeline", side_effect=build_pipeline), \
             patch("main._prime_daily_market_context", side_effect=[("", ""), ("缓存摘要", "完整复盘")]) as prime_context, \
             patch("main._run_market_review_with_shared_lock", return_value=SimpleNamespace(report="大盘复盘")) as run_with_lock:
            main.run_full_analysis(config, args, [])

        self.assertTrue(pipeline_kwargs["daily_market_context_enabled"])
        self.assertTrue(pipeline_kwargs["daily_market_context_allow_generate"])
        self.assertEqual(prime_context.call_count, 2)
        run_with_lock.assert_called_once()
        refresh.assert_called_once_with(config)

    def test_run_full_analysis_primes_daily_market_context_before_stock_analysis(self) -> None:
        args = self._make_args()
        target_date = date(2026, 3, 26)
        reference_times = []
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            daily_market_context_enabled=True,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline.run.return_value = []
        pipeline_kwargs = {}

        def build_pipeline(*args, **kwargs):
            pipeline_kwargs.update(kwargs)
            return pipeline

        def resolve_target_date(region, current_time):
            self.assertEqual(region, "cn")
            reference_times.append(current_time)
            return target_date

        with patch.object(main, "_refresh_stock_index_cache_for_analysis") as refresh, \
             patch("main._compute_trading_day_filter", return_value=([], "cn", False)), \
             patch("main._resolve_daily_market_context_target_date", side_effect=resolve_target_date), \
             patch("src.core.pipeline.StockAnalysisPipeline", side_effect=build_pipeline), \
             patch("main._prime_daily_market_context", return_value=("大盘退潮，高风险，建议观望，仓位上限30%。", "完整复盘正文")) as prime_context, \
             patch("main._run_market_review_with_shared_lock") as run_with_lock, \
             patch("src.core.market_review.run_market_review") as run_market_review:
            main.run_full_analysis(config, args, [])

        self.assertTrue(pipeline_kwargs["daily_market_context_allow_generate"])
        prime_context.assert_has_calls(
            [
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=False,
                ),
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=True,
                    require_current_query_match=True,
                ),
            ]
        )
        self.assertEqual(len(reference_times), 1)
        self.assertIs(pipeline.run.call_args.kwargs["current_time"], reference_times[0])
        self.assertEqual(pipeline.run.call_args.kwargs["current_time"].tzinfo, timezone.utc)
        run_with_lock.assert_called_once()
        self.assertFalse(run_with_lock.call_args.kwargs["merge_notification"])
        self.assertTrue(run_with_lock.call_args.kwargs["send_notification"])
        run_market_review.assert_not_called()
        refresh.assert_called_once_with(config)
        pipeline.run.assert_called_once()

    def test_resolve_daily_market_context_target_date_passes_jp_kr_to_trading_calendar(self) -> None:
        current_time = datetime(2026, 3, 26, 12, 0, tzinfo=timezone.utc)
        target_date = date(2026, 3, 25)

        with patch("src.core.trading_calendar.get_effective_trading_date", return_value=target_date) as get_date:
            self.assertEqual(
                main._resolve_daily_market_context_target_date("jp", current_time),
                target_date,
            )
            self.assertEqual(
                main._resolve_daily_market_context_target_date("kr", current_time),
                target_date,
            )

        self.assertEqual(
            get_date.call_args_list,
            [
                unittest.mock.call("jp", current_time=current_time),
                unittest.mock.call("kr", current_time=current_time),
            ],
        )

    def test_run_full_analysis_does_not_reuse_single_context_for_multi_market_review(self) -> None:
        args = self._make_args()
        target_date = date(2026, 3, 26)
        config = self._make_config(
            trading_day_check_enabled=True,
            market_review_region="both",
            market_review_enabled=True,
            daily_market_context_enabled=True,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline.run.return_value = []
        pipeline_kwargs = {}

        def build_pipeline(*args, **kwargs):
            pipeline_kwargs.update(kwargs)
            return pipeline

        with patch.object(main, "_refresh_stock_index_cache_for_analysis") as refresh, \
             patch("main._compute_trading_day_filter", return_value=([], "cn,us", False)), \
             patch("main._resolve_daily_market_context_target_date", return_value=target_date), \
             patch("src.core.pipeline.StockAnalysisPipeline", side_effect=build_pipeline), \
             patch("main._prime_daily_market_context", return_value=("A股缓存摘要", "")) as prime_context, \
             patch("main._run_market_review_with_shared_lock", return_value="多市场复盘") as run_with_lock, \
             patch("src.core.market_review.run_market_review") as run_market_review:
            main.run_full_analysis(config, args, [])

        self.assertTrue(pipeline_kwargs["daily_market_context_allow_generate"])
        prime_context.assert_has_calls(
            [
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn,us",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=False,
                ),
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn,us",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=True,
                    require_current_query_match=True,
                ),
            ]
        )
        run_with_lock.assert_called_once()
        self.assertEqual(run_with_lock.call_args.kwargs["override_region"], "cn,us")
        run_market_review.assert_not_called()
        refresh.assert_called_once_with(config)
        pipeline.run.assert_called_once()

    def test_prime_daily_market_context_readonly_mode_still_reuses_cached_context(self) -> None:
        target_date = date(2026, 3, 26)
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            market_review_region="cn",
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline._daily_market_context_service = None
        pipeline.db = MagicMock()
        pipeline.query_id = "prime-query"
        context = SimpleNamespace(source="analysis_history", summary="历史复盘摘要")
        service = MagicMock()
        service.get_context.return_value = context

        with patch(
            "src.services.daily_market_context.DailyMarketContextService",
            return_value=service,
        ) as service_cls:
            summary, full_report = main._prime_daily_market_context(
                config,
                pipeline=pipeline,
                region="cn",
                no_market_review=False,
                allow_generate=False,
                target_date=target_date,
                return_full_report=True,
            )

        self.assertEqual(summary, "历史复盘摘要")
        self.assertEqual(full_report, "")
        service_cls.assert_called_once_with(db_manager=pipeline.db)
        call_kwargs = service.get_context.call_args.kwargs
        self.assertEqual(call_kwargs["region"], "cn")
        self.assertFalse(call_kwargs["force_refresh"])
        self.assertFalse(call_kwargs["allow_generate"])
        self.assertFalse(call_kwargs["persist_market_review_history"])
        self.assertEqual(call_kwargs["target_date"], target_date)
        self.assertEqual(call_kwargs["current_query_id"], "prime-query")

    def test_prime_daily_market_context_query_fallback_reuses_runtime_context(self) -> None:
        target_date = date(2026, 3, 26)
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            market_review_region="cn",
            single_stock_notify=False,
            merge_email_notification=True,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline._daily_market_context_service = None
        pipeline.db = MagicMock()
        pipeline.query_id = "prime-query"
        context = SimpleNamespace(
            source="market_review_runtime",
            summary="本轮运行时复盘摘要",
            full_report="本轮运行时完整复盘",
            query_id="prime-query",
        )
        service = MagicMock()
        service.get_context.return_value = context

        with patch(
            "src.services.daily_market_context.DailyMarketContextService",
            return_value=service,
        ):
            summary, full_report = main._prime_daily_market_context(
                config,
                pipeline=pipeline,
                region="cn",
                no_market_review=False,
                allow_generate=False,
                target_date=target_date,
                return_full_report=True,
                require_current_query_match=True,
            )

        self.assertEqual(summary, "本轮运行时复盘摘要")
        self.assertEqual(full_report, "本轮运行时完整复盘")
        self.assertTrue(service.get_context.call_args.kwargs["require_query_id_match"])

    def test_run_full_analysis_generates_full_market_review_once_after_stock_analysis(self) -> None:
        args = self._make_args()
        target_date = date(2026, 3, 26)
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            daily_market_context_enabled=True,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        events = []
        pipeline.run.side_effect = lambda **kwargs: events.append("stock-run") or []
        pipeline_kwargs = {}

        def build_pipeline(*args, **kwargs):
            events.append("pipeline")
            pipeline_kwargs.update(kwargs)
            return pipeline

        def run_with_lock(*args, **kwargs):
            events.append("market-review")
            return SimpleNamespace(report="完整复盘")

        with patch.object(main, "_refresh_stock_index_cache_for_analysis") as refresh, \
             patch("main._compute_trading_day_filter", return_value=([], "cn", False)), \
             patch("main._resolve_daily_market_context_target_date", return_value=target_date), \
             patch("src.core.pipeline.StockAnalysisPipeline", side_effect=build_pipeline), \
             patch("main._prime_daily_market_context", return_value=("", "")) as prime_context, \
             patch("main._run_market_review_with_shared_lock", side_effect=run_with_lock) as run_with_lock_mock, \
             patch("src.core.market_review.run_market_review") as run_market_review:
            main.run_full_analysis(config, args, [])

        self.assertTrue(pipeline_kwargs["daily_market_context_allow_generate"])
        self.assertEqual(events, ["pipeline", "stock-run", "market-review"])
        query_scoped_read = unittest.mock.call(
            config,
            pipeline=pipeline,
            region="cn",
            no_market_review=False,
            allow_generate=False,
            target_date=target_date,
            return_full_report=True,
            require_current_query_match=True,
        )
        self.assertEqual(
            prime_context.call_args_list,
            [
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=False,
                ),
                query_scoped_read,
                query_scoped_read,
            ],
        )
        run_with_lock_mock.assert_called_once()
        run_market_review.assert_not_called()
        refresh.assert_called_once_with(config)
        pipeline.run.assert_called_once()

    def test_run_full_analysis_reuses_runtime_market_context_after_stock_analysis(self) -> None:
        args = self._make_args()
        target_date = date(2026, 3, 26)
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            daily_market_context_enabled=True,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline.run.return_value = []
        pipeline.notifier = MagicMock(
            is_available=MagicMock(return_value=True),
            send=MagicMock(return_value=True),
        )
        pipeline_kwargs = {}

        def build_pipeline(*args, **kwargs):
            pipeline_kwargs.update(kwargs)
            return pipeline

        runtime_context = ("本轮运行时复盘摘要", "## 本轮运行时完整复盘")
        with patch.object(main, "_refresh_stock_index_cache_for_analysis") as refresh, \
             patch("main._compute_trading_day_filter", return_value=([], "cn", False)), \
             patch("main._resolve_daily_market_context_target_date", return_value=target_date), \
             patch("src.core.pipeline.StockAnalysisPipeline", side_effect=build_pipeline), \
             patch(
                 "main._prime_daily_market_context",
                 side_effect=[("", ""), ("", ""), runtime_context],
             ) as prime_context, \
             patch("main._run_market_review_with_shared_lock") as run_with_lock, \
             patch("src.core.market_review.run_market_review") as run_market_review:
            main.run_full_analysis(config, args, [])

        self.assertTrue(pipeline_kwargs["daily_market_context_allow_generate"])
        run_with_lock.assert_not_called()
        run_market_review.assert_not_called()
        pipeline.notifier.send.assert_called_once()
        self.assertIn("## 本轮运行时完整复盘", pipeline.notifier.send.call_args.args[0])
        self.assertEqual(pipeline.notifier.send.call_args.kwargs["route_type"], "report")
        query_scoped_read = unittest.mock.call(
            config,
            pipeline=pipeline,
            region="cn",
            no_market_review=False,
            allow_generate=False,
            target_date=target_date,
            return_full_report=True,
            require_current_query_match=True,
        )
        self.assertEqual(
            prime_context.call_args_list,
            [
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=False,
                ),
                query_scoped_read,
                query_scoped_read,
            ],
        )
        refresh.assert_called_once_with(config)
        pipeline.run.assert_called_once()

    def test_run_full_analysis_saves_reused_runtime_market_context_without_notify(self) -> None:
        args = self._make_args(no_notify=True)
        target_date = date(2026, 3, 26)
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            daily_market_context_enabled=True,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline.run.return_value = []
        pipeline.notifier = MagicMock()
        pipeline.notifier.save_report_to_file.return_value = "/tmp/market_review.md"

        def build_pipeline(*args, **kwargs):
            return pipeline

        runtime_context = ("本轮运行时复盘摘要", "## 本轮运行时完整复盘")
        with (
            patch.object(main, "_refresh_stock_index_cache_for_analysis") as refresh,
            patch("main._compute_trading_day_filter", return_value=([], "cn", False)),
            patch("main._resolve_daily_market_context_target_date", return_value=target_date),
            patch("src.core.pipeline.StockAnalysisPipeline", side_effect=build_pipeline),
            patch(
                "main._prime_daily_market_context",
                side_effect=[("", ""), ("", ""), runtime_context],
            ) as prime_context,
            patch("main._run_market_review_with_shared_lock") as run_with_lock,
            patch("src.core.market_review.run_market_review") as run_market_review,
        ):
            main.run_full_analysis(config, args, [])

        run_with_lock.assert_not_called()
        run_market_review.assert_not_called()
        pipeline.notifier.send.assert_not_called()
        pipeline.notifier.save_report_to_file.assert_called_once()
        saved_content, saved_filename = pipeline.notifier.save_report_to_file.call_args.args
        self.assertTrue(saved_content.startswith("# 🎯 大盘复盘\n\n"))
        self.assertIn("## 本轮运行时完整复盘", saved_content)
        self.assertTrue(saved_filename.startswith("market_review_"))
        self.assertTrue(saved_filename.endswith(".md"))
        self.assertEqual(prime_context.call_count, 3)
        refresh.assert_called_once_with(config)
        pipeline.run.assert_called_once()

    def test_run_full_analysis_still_runs_market_review_for_merge_disabled_with_reused_context(self) -> None:
        args = self._make_args()
        target_date = date(2026, 3, 26)
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            daily_market_context_enabled=True,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline.run.return_value = []
        pipeline_kwargs = {}

        def build_pipeline(*args, **kwargs):
            pipeline_kwargs.update(kwargs)
            return pipeline

        with patch.object(main, "_refresh_stock_index_cache_for_analysis") as refresh, \
             patch("main._compute_trading_day_filter", return_value=([], "cn", False)), \
             patch("main._resolve_daily_market_context_target_date", return_value=target_date), \
             patch("src.core.pipeline.StockAnalysisPipeline", side_effect=build_pipeline), \
             patch(
                "main._prime_daily_market_context",
                return_value=("大盘退潮，高风险，建议观望。", "## 完整大盘复盘\n市场结构偏弱，建议保守。"),
             ) as prime_context, \
             patch("main._run_market_review_with_shared_lock") as run_with_lock, \
             patch("src.core.market_review.run_market_review") as run_market_review:
            main.run_full_analysis(config, args, [])

        self.assertTrue(pipeline_kwargs["daily_market_context_allow_generate"])
        prime_context.assert_has_calls(
            [
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=False,
                ),
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=True,
                    require_current_query_match=True,
                ),
            ]
        )
        run_with_lock.assert_called_once()
        self.assertFalse(run_with_lock.call_args.kwargs["merge_notification"])
        run_market_review.assert_not_called()
        refresh.assert_called_once_with(config)
        pipeline.run.assert_called_once()

    def test_run_full_analysis_waits_for_analysis_delay_before_market_review(self) -> None:
        args = self._make_args()
        target_date = date(2026, 3, 26)
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            daily_market_context_enabled=True,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=2,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        events = []
        pipeline.run.side_effect = lambda **kwargs: events.append("stock-run") or []
        pipeline_kwargs = {}

        def build_pipeline(*args, **kwargs):
            events.append("pipeline")
            pipeline_kwargs.update(kwargs)
            return pipeline

        def run_with_lock(*args, **kwargs):
            events.append("market-review")
            return SimpleNamespace(report="完整复盘")

        with patch.object(main, "_refresh_stock_index_cache_for_analysis") as refresh, \
             patch("main._compute_trading_day_filter", return_value=([], "cn", False)), \
             patch("main._resolve_daily_market_context_target_date", return_value=target_date), \
             patch("src.core.pipeline.StockAnalysisPipeline", side_effect=build_pipeline), \
             patch("main._prime_daily_market_context", return_value=("", "")) as prime_context, \
             patch("main._run_market_review_with_shared_lock", side_effect=run_with_lock) as run_with_lock_mock, \
             patch("time.sleep") as sleep, \
             patch("src.core.market_review.run_market_review") as run_market_review:
            main.run_full_analysis(config, args, [])

        self.assertTrue(pipeline_kwargs["daily_market_context_allow_generate"])
        self.assertEqual(events, ["pipeline", "stock-run", "market-review"])
        self.assertEqual(sleep.call_count, 1)
        sleep.assert_called_once_with(2)
        self.assertEqual(
            run_with_lock_mock.call_args.kwargs["send_notification"],
            True,
        )
        run_market_review.assert_not_called()
        run_with_lock_mock.assert_called_once()
        refresh.assert_called_once_with(config)
        prime_context.assert_has_calls(
            [
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=False,
                ),
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=True,
                    require_current_query_match=True,
                ),
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=True,
                    require_current_query_match=True,
                ),
            ]
        )

    def test_run_full_analysis_reuses_cached_market_context_as_full_report(self) -> None:
        args = self._make_args()
        target_date = date(2026, 3, 26)
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            daily_market_context_enabled=True,
            single_stock_notify=False,
            merge_email_notification=True,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
            report_type="simple",
        )
        pipeline = MagicMock()
        pipeline.run.return_value = []
        pipeline.notifier = MagicMock(
            is_available=MagicMock(return_value=True),
            generate_aggregate_report=MagicMock(return_value=""),
            send=MagicMock(return_value=True),
        )
        pipeline_kwargs = {}

        def build_pipeline(*args, **kwargs):
            pipeline_kwargs.update(kwargs)
            return pipeline

        with patch.object(main, "_refresh_stock_index_cache_for_analysis") as refresh, \
             patch("main._compute_trading_day_filter", return_value=([], "cn", False)), \
             patch("main._resolve_daily_market_context_target_date", return_value=target_date), \
             patch("src.core.pipeline.StockAnalysisPipeline", side_effect=build_pipeline), \
             patch(
                "main._prime_daily_market_context",
                return_value=(
                    "大盘退潮，高风险，建议观望。",
                    "## 完整大盘复盘\n市场结构偏弱，建议保守。",
                ),
             ) as prime_context, \
             patch("main._run_market_review_with_shared_lock") as run_with_lock, \
             patch("src.core.market_review.run_market_review") as run_market_review:
            main.run_full_analysis(config, args, [])

        self.assertTrue(pipeline_kwargs["daily_market_context_allow_generate"])
        prime_context.assert_has_calls(
            [
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=False,
                ),
                unittest.mock.call(
                    config,
                    pipeline=pipeline,
                    region="cn",
                    no_market_review=False,
                    allow_generate=False,
                    target_date=target_date,
                    return_full_report=True,
                    require_current_query_match=True,
                ),
            ]
        )
        run_with_lock.assert_not_called()
        run_market_review.assert_not_called()
        refresh.assert_called_once_with(config)
        pipeline.run.assert_called_once_with(
            stock_codes=[],
            dry_run=False,
            send_notification=True,
            merge_notification=True,
            current_time=unittest.mock.ANY,
        )
        notifier_message = pipeline.notifier.send.call_args.args[0]
        self.assertIn("## 完整大盘复盘", notifier_message)
        self.assertNotIn("大盘退潮，高风险，建议观望。", notifier_message)

    def test_run_market_review_with_shared_lock_forwards_request_config(self) -> None:
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            daily_market_context_enabled=True,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        run_review = MagicMock(return_value="复盘结果")

        with patch("src.core.market_review_lock.try_acquire_market_review_lock", return_value=object()) as acquire_lock, \
             patch("src.core.market_review_lock.release_market_review_lock") as release_lock:
            result = main._run_market_review_with_shared_lock(
                config,
                run_review,
                send_notification=False,
            )

        self.assertEqual(result, "复盘结果")
        acquire_lock.assert_called_once_with(config)
        run_review.assert_called_once_with(config=config, send_notification=False)
        release_lock.assert_called_once_with(unittest.mock.ANY)

    def test_prime_daily_market_context_uses_ephemeral_service_for_multi_market_region(self) -> None:
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline._daily_market_context_service = MagicMock()
        pipeline._daily_market_context_service.get_context.return_value = SimpleNamespace(
            source="analysis_history",
            summary="旧A股复盘摘要",
        )
        pipeline.db = MagicMock()
        context = SimpleNamespace(source="analysis_history", summary="多市场复盘摘要", full_report="完整复盘正文")
        regional_service = MagicMock()
        regional_service.get_context.return_value = context

        with patch("src.services.daily_market_context.DailyMarketContextService", return_value=regional_service) as service_cls:
            summary, full_report = main._prime_daily_market_context(
                config,
                pipeline=pipeline,
                region="cn,us",
                no_market_review=False,
                allow_generate=False,
                target_date=date(2026, 3, 26),
                return_full_report=True,
            )

        self.assertEqual(summary, "多市场复盘摘要")
        self.assertEqual(full_report, "完整复盘正文")
        service_cls.assert_called_once_with(db_manager=pipeline.db)
        regional_service.get_context.assert_called_once()
        self.assertIsNot(
            regional_service,
            pipeline._daily_market_context_service,
            "多市场预热必须使用独立服务避免共享缓存污染",
        )
        pipeline._daily_market_context_service.get_context.assert_not_called()

        get_context_kwargs = regional_service.get_context.call_args.kwargs
        self.assertEqual(get_context_kwargs["region"], "cn,us")
        self.assertFalse(get_context_kwargs["force_refresh"])
        self.assertFalse(get_context_kwargs["allow_generate"])
        self.assertFalse(get_context_kwargs["persist_market_review_history"])

    def test_config_enabled_schedule_marks_market_review_source_as_schedule(self) -> None:
        args = self._make_args(schedule=False)
        target_date = date(2026, 3, 26)
        config = self._make_config(
            schedule_enabled=True,
            trading_day_check_enabled=False,
            market_review_enabled=True,
            no_market_review=False,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline.run.return_value = []

        with patch.object(main, "_refresh_stock_index_cache_for_analysis"), \
             patch.object(main, "_compute_trading_day_filter", return_value=(["600519"], "cn", False)), \
             patch("main._resolve_daily_market_context_target_date", return_value=target_date), \
             patch("src.core.pipeline.StockAnalysisPipeline", return_value=pipeline), \
             patch("main._prime_daily_market_context", return_value=("", "")), \
             patch("main._run_market_review_with_shared_lock", return_value="market report") as run_with_lock, \
             patch("src.core.market_review.run_market_review") as run_market_review:
            main.run_full_analysis(config, args, ["600519"])

        pipeline.run.assert_called_once()
        run_with_lock.assert_called_once()
        call_args = run_with_lock.call_args
        self.assertIs(call_args.args[1], run_market_review)
        self.assertEqual(call_args.kwargs["trigger_source"], "schedule")

    def test_market_review_mode_uses_shared_runtime_assembly(self) -> None:
        args = self._make_args(market_review=True)
        config = self._make_config(
            trading_day_check_enabled=True,
            market_review_region="both",
            market_review_enabled=False,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        runtime_notifier = MagicMock()
        runtime_analyzer = MagicMock()
        runtime_search_service = MagicMock()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch("main._run_market_review_with_shared_lock") as run_with_lock, \
             patch(
                 "src.core.market_review_runtime.build_market_review_runtime",
                 return_value=(
                    runtime_notifier,
                    runtime_analyzer,
                    runtime_search_service,
                 ),
             ) as runtime_builder, \
             patch("src.core.market_review.run_market_review") as run_market_review, \
             patch("src.core.trading_calendar.get_open_markets_today", return_value={"cn", "us"}), \
             patch("src.core.trading_calendar.compute_effective_region", return_value="cn,us"):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        runtime_builder.assert_called_once_with(config)
        run_with_lock.assert_called_once()
        call_args = run_with_lock.call_args
        self.assertEqual(call_args.args[0], config)
        self.assertIs(call_args.args[1], run_market_review)
        self.assertIs(call_args.kwargs["notifier"], runtime_notifier)
        self.assertIs(call_args.kwargs["analyzer"], runtime_analyzer)
        self.assertIs(call_args.kwargs["search_service"], runtime_search_service)
        self.assertTrue(call_args.kwargs["send_notification"])
        self.assertNotIn("merge_notification", call_args.kwargs)
        self.assertEqual(call_args.kwargs["override_region"], "cn,us")
        self.assertEqual(call_args.kwargs["trigger_source"], "cli")

    def test_market_review_mode_respects_comma_list_market_review_region(self) -> None:
        args = self._make_args(market_review=True)
        config = self._make_config(
            trading_day_check_enabled=True,
            market_review_region="jp,kr",
            market_review_enabled=False,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        runtime_notifier = MagicMock()
        runtime_analyzer = MagicMock()
        runtime_search_service = MagicMock()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch("main._run_market_review_with_shared_lock") as run_with_lock, \
             patch(
                 "src.core.market_review_runtime.build_market_review_runtime",
                 return_value=(runtime_notifier, runtime_analyzer, runtime_search_service),
             ) as runtime_builder, \
             patch("src.core.market_review.run_market_review"), \
             patch("src.core.trading_calendar.get_open_markets_today", return_value={"jp", "kr"}):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        runtime_builder.assert_called_once_with(config)
        call_args = run_with_lock.call_args
        self.assertIs(call_args.args[0], config)
        self.assertEqual(call_args.kwargs["override_region"], "jp,kr")
        self.assertEqual(call_args.kwargs["trigger_source"], "cli")

    def test_bootstrap_logging_persists_when_config_load_fails(self) -> None:
        """Config load failure must be logged to stderr and return exit code 1.

        Bootstrap logging is stderr-only so healthy runs never write to a
        hard-coded directory.  The error is still captured by process runners
        (e.g. GitHub Actions) that collect stderr output.
        """
        import io

        args = self._make_args()

        capture_stream = io.StringIO()
        capture_handler = logging.StreamHandler(capture_stream)
        capture_handler.setLevel(logging.DEBUG)
        capture_handler.setFormatter(logging.Formatter("%(message)s"))

        root_logger = logging.getLogger()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", side_effect=RuntimeError("config boom")):
            root_logger.addHandler(capture_handler)
            try:
                exit_code = main.main()
            finally:
                root_logger.removeHandler(capture_handler)
                capture_handler.close()

        self.assertEqual(exit_code, 1)
        output = capture_stream.getvalue()
        self.assertIn("加载配置失败", output)
        self.assertIn("config boom", output)

    def test_bootstrap_logging_failure_does_not_block_startup(self) -> None:
        """Bootstrap log dir unwritable must not prevent startup (P1 regression)."""
        args = self._make_args()
        config = self._make_config()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._setup_bootstrap_logging", side_effect=OSError("read-only fs")), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_mock:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        run_mock.assert_called_once()

    def test_runtime_file_logging_permission_error_falls_back_to_console(self) -> None:
        """Configured file logging failures should not prevent Docker startup."""
        import io

        args = self._make_args()
        config = self._make_config(log_dir="/app/logs")
        capture_stream = io.StringIO()
        capture_handler = logging.StreamHandler(capture_stream)
        capture_handler.setLevel(logging.DEBUG)
        capture_handler.setFormatter(logging.Formatter("%(message)s"))

        root_logger = logging.getLogger()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch(
                 "main.setup_logging",
                 side_effect=PermissionError("/app/logs/stock_analysis_20260511.log"),
             ), \
             patch("main.run_full_analysis") as run_mock:
            root_logger.addHandler(capture_handler)
            try:
                exit_code = main.main()
            finally:
                root_logger.removeHandler(capture_handler)
                capture_handler.close()

        self.assertEqual(exit_code, 0)
        run_mock.assert_called_once()
        output = capture_stream.getvalue()
        self.assertIn("文件日志初始化失败，已降级为控制台日志输出", output)
        self.assertIn("/app/logs", output)
        self.assertIn("官方 Docker 镜像启动入口会自动修复默认挂载目录权限", output)

    def test_run_full_analysis_import_failure_propagates(self) -> None:
        """P1: import failures in run_full_analysis must propagate, not be swallowed."""
        args = self._make_args()
        config = self._make_config()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch.dict("sys.modules", {"src.core.pipeline": None}):
            exit_code = main.main()

        self.assertEqual(exit_code, 1)

    def test_lazy_pipeline_triggers_env_bootstrap(self) -> None:
        """P2: lazy StockAnalysisPipeline access must call _bootstrap_environment."""
        # Reset the lazy descriptor cache so __get__ fires again
        main._LazyPipelineDescriptor._resolved = None
        main._env_bootstrapped = False

        with patch("main._bootstrap_environment", wraps=main._bootstrap_environment) as mock_boot, \
             patch("src.core.pipeline.StockAnalysisPipeline", create=True, new_callable=lambda: type("FakePipeline", (), {})):
            try:
                _ = main.StockAnalysisPipeline
            except Exception:
                pass
            mock_boot.assert_called()

        # Cleanup: reset state
        main._LazyPipelineDescriptor._resolved = None
        main._env_bootstrapped = False


if __name__ == "__main__":
    unittest.main()
