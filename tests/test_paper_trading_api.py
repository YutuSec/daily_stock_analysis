# -*- coding: utf-8 -*-
"""API contract checks for the paper-only monitor surface."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from api.app import create_app
from api.deps import get_runtime_scheduler_service
from src.config import Config
from src.storage import AnalysisHistory, DailyMarketRegime, DatabaseManager, ReportPlanReconciliation


class PaperTradingApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {
            key: os.environ.get(key)
            for key in (
                "ENV_FILE",
                "DATABASE_PATH",
                "PAPER_TRADING_ENABLED",
                "PAPER_TRADING_INTRADAY_ENABLED",
                "PAPER_TRADING_FEISHU_NOTIFY",
                "FEISHU_WEBHOOK_URL",
                "FEISHU_APP_ID",
                "FEISHU_APP_SECRET",
                "FEISHU_CHAT_ID",
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
                "PAPER_TRADING_ENABLED=false",
                "PAPER_TRADING_INTRADAY_ENABLED=false",
                "PAPER_TRADING_FEISHU_NOTIFY=false",
                f"DATABASE_PATH={root / 'paper-api.db'}",
            )) + "\n",
            encoding="utf-8",
        )
        os.environ["ENV_FILE"] = str(env_path)
        os.environ["DATABASE_PATH"] = str(root / "paper-api.db")
        os.environ["PAPER_TRADING_ENABLED"] = "false"
        os.environ["PAPER_TRADING_INTRADAY_ENABLED"] = "false"
        os.environ["PAPER_TRADING_FEISHU_NOTIFY"] = "false"
        os.environ.pop("FEISHU_WEBHOOK_URL", None)
        os.environ.pop("FEISHU_APP_ID", None)
        os.environ.pop("FEISHU_APP_SECRET", None)
        os.environ.pop("FEISHU_CHAT_ID", None)
        Config.reset_instance()
        DatabaseManager.reset_instance()
        self.client = TestClient(create_app(static_dir=root / "missing-static"))

    def _cn_session_date(self) -> date:
        return datetime.now(timezone.utc).astimezone(ZoneInfo("Asia/Shanghai")).date()

    def _seed_market_regime(
        self,
        regime: str,
        *,
        session_date: date | None = None,
        freshness_expires_at: datetime | None = None,
    ) -> None:
        session_date = session_date or self._cn_session_date()
        multiplier = {"normal": 1.0, "cautious": 0.5, "risk_off": 0.0}.get(regime, 0.0)
        entry_allowed = regime in {"normal", "cautious"}
        expires_at = (
            freshness_expires_at
            if freshness_expires_at is not None
            else datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=1)
        )
        with DatabaseManager.get_instance().session_scope() as session:
            session.add(DailyMarketRegime(
                regime_key=f"cn:{session_date.isoformat()}:{regime}:api-test",
                region="cn",
                session_date=session_date,
                regime=regime,
                allocation_multiplier=multiplier,
                entry_allowed=entry_allowed,
                freshness_expires_at=expires_at,
                policy_version="market-regime-v1",
                provenance_json='{"source":"api-test"}',
            ))

    def _status_with_shared_scheduler(self, shared_scheduler: dict, *, local_enabled: bool = False, local_running: bool = False) -> dict:
        class _FakeScheduler:
            @staticmethod
            def status() -> dict:
                return {
                    "enabled": local_enabled,
                    "running": local_running,
                    "shared_scheduler": shared_scheduler,
                    "schedule_times": ["15:10"],
                    "next_run_at": "2026-01-01T15:10:00",
                }

        self.client.app.dependency_overrides[get_runtime_scheduler_service] = lambda: _FakeScheduler()
        try:
            response = self.client.get("/api/v1/paper-trading/status")
        finally:
            self.client.app.dependency_overrides.pop(get_runtime_scheduler_service, None)
        self.assertEqual(response.status_code, 200)
        return response.json()

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

    def test_lists_model_filtered_plans_and_performance(self) -> None:
        plan = {
            "id": 1,
            "source_report_id": 9,
            "source_model": "model-a",
            "market": "cn",
            "stock_code": "600519",
            "report_date": "2026-08-20",
            "action": "buy",
            "executable": True,
            "status": "active",
        }
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_plans",
            return_value={"items": [plan], "total": 1},
        ) as list_plans, patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.performance",
            return_value={"model": "model-a", "closed_positions": 1},
        ):
            response = self.client.get("/api/v1/paper-trading/plans?model=model-a")
            metrics = self.client.get("/api/v1/paper-trading/performance?model=model-a")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["items"][0]["source_model"], "model-a")
        list_plans.assert_called_once_with(model="model-a", status=None, offset=0, limit=50)
        self.assertEqual(metrics.status_code, 200)
        self.assertEqual(metrics.json()["metrics"]["closed_positions"], 1)

    def test_lists_current_operations_with_pagination(self) -> None:
        operation = {
            "id": 1,
            "source_report_id": 9,
            "source_model": "model-a",
            "market": "cn",
            "stock_code": "600519",
            "report_date": "2026-08-20",
            "action": "buy",
            "executable": True,
            "status": "triggered",
            "stage": "position_monitoring",
            "is_operational": True,
        }
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_operations",
            return_value={"items": [operation], "total": 501},
        ) as list_operations:
            response = self.client.get(
                "/api/v1/paper-trading/operations?model=model-a&page=2&limit=50"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["total"], 501)
        self.assertEqual(response.json()["items"][0]["stage"], "position_monitoring")
        list_operations.assert_called_once_with(model="model-a", offset=50, limit=50)

    def test_lists_resolution_and_risk_revision_audit_ledgers(self) -> None:
        resolution = {
            "id": 1,
            "resolution_key": "res-1",
            "selected_intent": "model_reduce",
            "candidate_intents": [{"kind": "model_reduce"}],
            "suppressed_intents": [{"kind": "hold"}],
            "chosen_intent": {"intent": "model_reduce"},
            "requested_quantity": 100,
            "executed_quantity": 100,
            "unfilled_quantity": 0,
        }
        revision = {
            "id": 2,
            "revision_key": "risk-1",
            "position_id": 9,
            "result": "applied",
            "old_stop_loss": 95,
            "new_stop_loss": 96,
            "applied_stop_loss": 96,
        }
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_resolutions",
            return_value={"items": [resolution], "total": 1},
        ) as list_resolutions, patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_risk_revisions",
            return_value={"items": [revision], "total": 1},
        ) as list_risk_revisions:
            resolution_response = self.client.get(
                "/api/v1/paper-trading/resolutions?model=model-a&page=2&limit=10"
            )
            revision_response = self.client.get(
                "/api/v1/paper-trading/risk-revisions?model=model-a&page=3&limit=20"
            )

        self.assertEqual(resolution_response.status_code, 200)
        self.assertEqual(resolution_response.json()["items"][0]["selected_intent"], "model_reduce")
        list_resolutions.assert_called_once_with(model="model-a", offset=10, limit=10)
        self.assertEqual(revision_response.status_code, 200)
        self.assertEqual(revision_response.json()["items"][0]["result"], "applied")
        list_risk_revisions.assert_called_once_with(model="model-a", offset=40, limit=20)

    def test_lists_reanalysis_risk_snapshots_events_and_manual_controls(self) -> None:
        request = {"id": 1, "request_key": "paper-reanalysis:1", "status": "submitted"}
        snapshot = {
            "id": 2,
            "capital_account_id": 7,
            "source_model": "model-a",
            "active_state": "reduce_only",
            "active_reasons": ["daily_loss_limit_breached"],
            "permissions": {"open": False, "reduce": True, "close": True},
        }
        event = {"id": 3, "event_key": "risk-event:k", "event_type": "manual_hold", "new_state": "manual_hold"}
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_reanalysis_requests",
            return_value={"items": [request], "total": 1},
        ) as list_reanalysis, patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_account_risk_snapshots",
            return_value={"items": [snapshot], "total": 1},
        ) as list_snapshots, patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_account_risk_events",
            return_value={"items": [event], "total": 1},
        ) as list_events, patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.set_account_manual_hold",
            return_value={"created": True, "item": event},
        ) as manual_hold, patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.resume_account_risk",
            return_value={"created": True, "item": {**event, "event_type": "manual_resume", "new_state": "normal"}},
        ) as manual_resume:
            reanalysis_response = self.client.get("/api/v1/paper-trading/reanalysis-requests?model=model-a&status=submitted")
            snapshot_response = self.client.get("/api/v1/paper-trading/risk-snapshots?model=model-a&account_id=7")
            event_response = self.client.get("/api/v1/paper-trading/risk-events?model=model-a&account_id=7")
            hold_response = self.client.post(
                "/api/v1/paper-trading/risk/manual-hold",
                json={"account_id": 7, "reason": "pause", "idempotency_key": "hold-1"},
            )
            resume_response = self.client.post(
                "/api/v1/paper-trading/risk/manual-resume",
                json={"account_id": 7, "reason": "resume", "idempotency_key": "resume-1"},
            )

        self.assertEqual(reanalysis_response.status_code, 200)
        self.assertEqual(snapshot_response.json()["items"][0]["permissions"]["open"], False)
        self.assertEqual(event_response.json()["items"][0]["event_type"], "manual_hold")
        self.assertEqual(hold_response.status_code, 200)
        self.assertEqual(resume_response.status_code, 200)
        list_reanalysis.assert_called_once_with(model="model-a", status="submitted", offset=0, limit=100)
        list_snapshots.assert_called_once_with(model="model-a", account_id=7, offset=0, limit=100)
        list_events.assert_called_once_with(model="model-a", account_id=7, offset=0, limit=100)
        manual_hold.assert_called_once_with(account_id=7, reason="pause", idempotency_key="hold-1")
        manual_resume.assert_called_once_with(account_id=7, reason="resume", idempotency_key="resume-1")

    def test_status_exposes_configured_monitor_state(self) -> None:
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService._feishu_route_configured",
            return_value=False,
        ):
            response = self.client.get("/api/v1/paper-trading/status")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["enabled"])
        self.assertFalse(payload["intraday_enabled"])
        self.assertEqual(payload["account_daily_loss_limit_pct"], 3.0)
        self.assertEqual(payload["account_unknown_cap_pct"], 50.0)
        self.assertEqual(payload["interval_minutes"], 5)
        self.assertTrue(payload["strategy_thesis_enabled"])
        self.assertEqual(payload["strategy_entry_confirmation_sessions"], 2)
        self.assertEqual(payload["strategy_flip_confirmation_sessions"], 2)
        self.assertTrue(payload["strategy_hard_risk_immediate"])
        self.assertEqual(payload["min_factor_domains"], 2)
        self.assertEqual(payload["initial_capital"], 100000.0)
        self.assertEqual(payload["risk_per_trade_pct"], 1.0)
        self.assertEqual(payload["max_position_pct"], 20.0)
        self.assertEqual(payload["max_total_exposure_pct"], 80.0)
        self.assertEqual(payload["max_open_positions"], 5)
        self.assertFalse(payload["feishu_notify_enabled"])
        self.assertFalse(payload["feishu_route_configured"])
        self.assertIn("local_scheduler_enabled", payload)
        self.assertIn("local_scheduler_running", payload)
        self.assertEqual(payload["local_scheduler_owner"], "api_process")
        self.assertIn("scheduler_enabled", payload)
        self.assertEqual(payload["market_context_risk"]["scope"]["market"], "cn")
        self.assertEqual(payload["market_context_risk"]["scope"]["source"], "daily_market_regimes")
        self.assertEqual(payload["market_context_risk"]["regime"], "unavailable")
        self.assertEqual(payload["market_context_risk"]["allocation_multiplier"], 0.0)
        self.assertFalse(payload["market_context_risk"]["entry_allowed"])

    def test_status_treats_fresh_idle_enabled_task_heartbeat_as_scheduler_running(self) -> None:
        shared = {
            "available": True,
            "fresh": False,
            "tasks": {
                "daily_analysis:15:10": {
                    "heartbeat": {
                        "fresh": True,
                        "status": "idle",
                        "owner_kind": "runtime_scheduler",
                        "owner_id": "owner-idle",
                        "observed_at": "2026-08-29T09:30:00",
                        "details": {
                            "enabled": True,
                            "running": False,
                            "schedule_time": "15:10",
                        },
                    }
                }
            },
        }

        payload = self._status_with_shared_scheduler(shared)

        self.assertTrue(payload["scheduler_enabled"])
        self.assertTrue(payload["scheduler_running"])
        self.assertEqual(payload["scheduler_owner_kind"], "runtime_scheduler")
        self.assertEqual(payload["scheduler_owner_id"], "owner-idle")
        self.assertEqual(payload["scheduler_last_seen_at"], "2026-08-29T09:30:00")
        self.assertFalse(
            payload["scheduler_task_states"]["daily_analysis:15:10"]["heartbeat"]["details"]["running"]
        )

    def test_status_exposes_intraday_last_run_summary_separate_from_observation_bucket(self) -> None:
        shared = {
            "available": True,
            "fresh": True,
            "tasks": {
                "paper_trading_intraday_monitor": {
                    "heartbeat": {
                        "fresh": True,
                        "status": "idle",
                        "owner_kind": "runtime_scheduler",
                        "owner_id": "owner-idle",
                        "observed_at": "2026-09-08T05:45:36",
                        "last_success_at": "2026-09-08T05:41:36",
                        "details": {
                            "enabled": True,
                            "running": False,
                            "last_skip_reason": "not_due",
                            "last_run_summary": {
                                "status": "succeeded",
                                "finished_at": "2026-09-08T05:41:36",
                                "target_identity_count": 0,
                                "quote_fetch_attempts": 0,
                                "quote_fetch_successes": 0,
                                "skipped_closed_market": 0,
                                "failed_closed": 0,
                                "observations": 0,
                            },
                        },
                    }
                }
            },
        }

        payload = self._status_with_shared_scheduler(shared)

        self.assertIn("scheduler_last_run_summaries", payload)
        summary = payload["scheduler_last_run_summaries"]["paper_trading_intraday_monitor"]
        self.assertEqual(summary["status"], "succeeded")
        self.assertEqual(summary["target_identity_count"], 0)
        self.assertEqual(payload["intraday_monitor_last_run_summary"], summary)
        self.assertIn("latest_observation_bucket", payload)

    def test_status_does_not_report_stale_stopped_or_disabled_heartbeats_running(self) -> None:
        for name, heartbeat in {
            "stale": {"fresh": False, "status": "idle", "details": {"enabled": True}},
            "stopped": {"fresh": True, "status": "stopped", "details": {"enabled": True}},
            "disabled": {"fresh": True, "status": "idle", "details": {"enabled": False}},
            "unknown": {"fresh": True, "status": "unknown", "details": {"enabled": True}},
            "empty": {"fresh": True, "status": "", "details": {"enabled": True}},
            "dead": {"fresh": True, "status": "dead", "details": {"enabled": True}},
            "error": {"fresh": True, "status": "error", "details": {"enabled": True}},
        }.items():
            with self.subTest(name=name):
                payload = self._status_with_shared_scheduler({
                    "available": True,
                    "fresh": False,
                    "tasks": {f"daily_analysis:{name}": {"heartbeat": heartbeat}},
                })

                self.assertFalse(payload["scheduler_running"])
                self.assertIsNone(payload["scheduler_owner_kind"])
                self.assertIsNone(payload["scheduler_owner_id"])

    def test_status_treats_active_task_lease_as_scheduler_running(self) -> None:
        payload = self._status_with_shared_scheduler({
            "available": True,
            "fresh": False,
            "tasks": {
                "paper_trading_intraday_monitor": {
                    "lease": {
                        "active": True,
                        "owner_kind": "runtime_scheduler",
                        "owner_id": "lease-owner",
                        "status": "running",
                        "started_at": "2026-08-29T09:31:00",
                    }
                }
            },
        })

        self.assertTrue(payload["scheduler_running"])
        self.assertTrue(payload["scheduler_enabled"])
        self.assertFalse(payload["scheduler_heartbeat_fresh"])
        self.assertEqual(payload["scheduler_owner_kind"], "runtime_scheduler")
        self.assertEqual(payload["scheduler_owner_id"], "lease-owner")
        self.assertEqual(payload["scheduler_last_seen_at"], "2026-08-29T09:31:00")

    def test_status_keeps_single_container_local_scheduler_running_compatibility(self) -> None:
        payload = self._status_with_shared_scheduler(
            {"available": False, "fresh": False, "tasks": {}},
            local_enabled=True,
            local_running=True,
        )

        self.assertTrue(payload["scheduler_enabled"])
        self.assertTrue(payload["scheduler_running"])

    def test_status_market_context_risk_exposes_normal_snapshot(self) -> None:
        self._seed_market_regime("normal")

        response = self.client.get("/api/v1/paper-trading/status")

        payload = response.json()["market_context_risk"]
        self.assertEqual(payload["regime"], "normal")
        self.assertTrue(payload["fresh"])
        self.assertTrue(payload["entry_allowed"])
        self.assertEqual(payload["allocation_multiplier"], 1.0)
        self.assertEqual(payload["source"], "api-test")
        self.assertEqual(payload["session_date"], self._cn_session_date().isoformat())

    def test_status_market_context_risk_exposes_cautious_snapshot(self) -> None:
        self._seed_market_regime("cautious")

        payload = self.client.get("/api/v1/paper-trading/status").json()["market_context_risk"]

        self.assertEqual(payload["regime"], "cautious")
        self.assertTrue(payload["fresh"])
        self.assertTrue(payload["entry_allowed"])
        self.assertEqual(payload["allocation_multiplier"], 0.5)

    def test_status_market_context_risk_exposes_risk_off_snapshot(self) -> None:
        self._seed_market_regime("risk_off")

        payload = self.client.get("/api/v1/paper-trading/status").json()["market_context_risk"]

        self.assertEqual(payload["regime"], "risk_off")
        self.assertTrue(payload["fresh"])
        self.assertFalse(payload["entry_allowed"])
        self.assertEqual(payload["allocation_multiplier"], 0.0)
        self.assertIn("market_context_risk_off", payload["reasons"])

    def test_status_market_context_risk_exposes_stale_snapshot(self) -> None:
        self._seed_market_regime(
            "normal",
            freshness_expires_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )

        payload = self.client.get("/api/v1/paper-trading/status").json()["market_context_risk"]

        self.assertEqual(payload["regime"], "normal")
        self.assertFalse(payload["fresh"])
        self.assertFalse(payload["entry_allowed"])
        self.assertEqual(payload["allocation_multiplier"], 0.0)
        self.assertIn("market_context_stale", payload["reasons"])

    def test_lists_report_plan_reconciliation_status(self) -> None:
        with DatabaseManager.get_instance().session_scope() as session:
            report = AnalysisHistory(
                query_id="api-reconcile",
                code="600519",
                name="贵州茅台",
                report_type="full",
                model_used="model-a",
                operation_advice="买入",
                raw_result="{}",
                created_at=datetime(2026, 8, 10, 15, 0),
            )
            session.add(report)
            session.flush()
            session.add(ReportPlanReconciliation(
                reconciliation_key=f"analysis:{report.id}:paper-hybrid-v3",
                source_report_id=report.id,
                plan_version="paper-hybrid-v3",
                source_model="model-a",
                status="retry_wait",
                classification="transient_error",
                attempt_count=2,
                max_attempts=3,
                next_retry_at=datetime(2026, 8, 10, 16, 0),
                last_attempt_at=datetime(2026, 8, 10, 15, 30),
                last_error="temporary",
                decision_signal_id=11,
                paper_plan_id=22,
                details_json='{"trigger_source":"scheduled_reconciler"}',
            ))

        response = self.client.get(
            "/api/v1/paper-trading/report-plan-reconciliations?model=model-a&status=retry_wait&page=1&limit=10"
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["total"], 1)
        item = payload["items"][0]
        self.assertEqual(item["source_model"], "model-a")
        self.assertEqual(item["status"], "retry_wait")
        self.assertEqual(item["classification"], "transient_error")
        self.assertEqual(item["attempt_count"], 2)
        self.assertEqual(item["max_attempts"], 3)
        self.assertEqual(item["next_retry_at"], "2026-08-10T16:00:00")
        self.assertEqual(item["last_error"], "temporary")
        self.assertEqual(item["decision_signal_id"], 11)
        self.assertEqual(item["paper_plan_id"], 22)
        self.assertEqual(item["report"]["code"], "600519")
        self.assertEqual(item["details"]["trigger_source"], "scheduled_reconciler")

    def test_lists_capital_accounts_and_dispatches(self) -> None:
        account = {
            "id": 1,
            "account_key": "model-a:cn:CNY",
            "source_model": "model-a",
            "market": "cn",
            "currency": "CNY",
            "cash": 80000,
        }
        dispatch = {
            "id": 2,
            "event_id": 10,
            "channel": "feishu",
            "status": "failed",
            "attempt_count": 1,
        }
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_accounts",
            return_value={"items": [account], "total": 1},
        ) as list_accounts, patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_dispatches",
            return_value={"items": [dispatch], "total": 1},
        ) as list_dispatches:
            accounts_response = self.client.get(
                "/api/v1/paper-trading/accounts?model=model-a&market=cn&page=2&limit=10"
            )
            dispatches_response = self.client.get(
                "/api/v1/paper-trading/dispatches?model=model-a&status=failed&limit=20"
            )

        self.assertEqual(accounts_response.status_code, 200)
        self.assertEqual(accounts_response.json()["items"][0]["account_key"], "model-a:cn:CNY")
        list_accounts.assert_called_once_with(model="model-a", market="cn", offset=10, limit=10)
        self.assertEqual(dispatches_response.status_code, 200)
        self.assertEqual(dispatches_response.json()["items"][0]["status"], "failed")
        list_dispatches.assert_called_once_with(model="model-a", status="failed", offset=0, limit=20)

    def test_lists_aggregate_holdings(self) -> None:
        holding = {
            "source_model": "model-a",
            "market": "cn",
            "stock_code": "002714",
            "stock_name": "牧原股份",
            "currency": "CNY",
            "total_quantity": 200,
            "available_quantity": 0,
            "frozen_quantity": 200,
            "weighted_average_price": 50,
            "total_cost": 10000,
            "lot_count": 2,
            "unrealized_pnl": None,
            "unrealized_return_pct": None,
            "latest_price": None,
            "observed_at": None,
            "symbol_concentration_pct": 20.0,
        }
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_holdings",
            return_value={"items": [holding], "total": 1},
        ) as list_holdings:
            response = self.client.get("/api/v1/paper-trading/holdings?model=model-a&page=2&limit=10")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["items"][0]["stock_code"], "002714")
        self.assertEqual(payload["items"][0]["frozen_quantity"], 200)
        self.assertEqual(payload["items"][0]["symbol_concentration_pct"], 20.0)
        list_holdings.assert_called_once_with(model="model-a", offset=10, limit=10)

    def test_initializes_account_and_adjusts_funds(self) -> None:
        account = {
            "id": 7,
            "account_key": "model-a:cn:CNY",
            "source_model": "model-a",
            "market": "cn",
            "currency": "CNY",
            "cash": 100000,
        }
        adjustment = {
            "id": 3,
            "account_id": 7,
            "adjustment_type": "deposit",
            "amount": 5000,
            "after_cash": 105000,
        }
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.initialize_account",
            return_value={"created": True, "item": account},
        ) as initialize_account, patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.adjust_account",
            return_value={"created": True, "item": adjustment, "account": {**account, "cash": 105000}},
        ) as adjust_account, patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_adjustments",
            return_value={"items": [adjustment], "total": 1},
        ) as list_adjustments:
            init_response = self.client.post(
                "/api/v1/paper-trading/accounts",
                json={"source_model": "model-a", "market": "cn", "initial_capital": 100000},
            )
            adjust_response = self.client.post(
                "/api/v1/paper-trading/accounts/7/adjustments",
                json={
                    "adjustment_type": "deposit",
                    "amount": 5000,
                    "idempotency_key": "api-key-1",
                    "reason": "top up",
                },
            )
            history_response = self.client.get(
                "/api/v1/paper-trading/adjustments?model=model-a&account_id=7&adjustment_type=deposit&limit=20"
            )

        self.assertEqual(init_response.status_code, 200)
        self.assertTrue(init_response.json()["created"])
        initialize_account.assert_called_once_with(source_model="model-a", market="cn", initial_capital=100000.0)
        self.assertEqual(adjust_response.status_code, 200)
        self.assertEqual(adjust_response.json()["account"]["cash"], 105000)
        adjust_account.assert_called_once_with(
            7,
            adjustment_type="deposit",
            amount=5000.0,
            idempotency_key="api-key-1",
            reason="top up",
        )
        self.assertEqual(history_response.status_code, 200)
        list_adjustments.assert_called_once_with(
            model="model-a",
            account_id=7,
            adjustment_type="deposit",
            offset=0,
            limit=20,
        )

    def test_adjustment_errors_are_stable_4xx(self) -> None:
        from src.services.paper_trading_service import PaperTradingConflictError, PaperTradingNotFoundError

        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.adjust_account",
            side_effect=PaperTradingNotFoundError("missing"),
        ):
            missing = self.client.post(
                "/api/v1/paper-trading/accounts/999/adjustments",
                json={"adjustment_type": "deposit", "amount": 1, "idempotency_key": "missing", "reason": "missing account"},
            )
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.adjust_account",
            side_effect=PaperTradingConflictError("conflict"),
        ):
            conflict = self.client.post(
                "/api/v1/paper-trading/accounts/7/adjustments",
                json={"adjustment_type": "deposit", "amount": 1, "idempotency_key": "conflict", "reason": "same key"},
            )
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.adjust_account",
            side_effect=ValueError("bad amount"),
        ):
            validation = self.client.post(
                "/api/v1/paper-trading/accounts/7/adjustments",
                json={"adjustment_type": "deposit", "amount": 1, "idempotency_key": "bad", "reason": "bad request"},
            )
        missing_reason = self.client.post(
            "/api/v1/paper-trading/accounts/7/adjustments",
            json={"adjustment_type": "deposit", "amount": 1, "idempotency_key": "no-reason"},
        )

        def _payload(response):
            body = response.json()
            if isinstance(body, dict) and "detail" in body:
                return body["detail"]
            return body

        missing_payload = _payload(missing)
        conflict_payload = _payload(conflict)
        validation_payload = _payload(validation)
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing_payload["error"], "not_found")
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(conflict_payload["error"], "idempotency_conflict")
        self.assertEqual(validation.status_code, 400)
        self.assertEqual(validation_payload["error"], "validation_error")
        self.assertEqual(missing_reason.status_code, 422)

    def test_manual_run_validation_error_is_safe_400(self) -> None:
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.run_monitor",
            side_effect=ValueError("no complete daily bar"),
        ):
            response = self.client.post("/api/v1/paper-trading/run", json={})

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertEqual((payload.get("detail") or payload)["error"], "validation_error")

    def test_sync_from_portfolio_contract(self) -> None:
        link = {
            "id": 1,
            "direction": "portfolio_to_paper",
            "source_model": "model-a",
            "market": "cn",
            "stock_code": "600519",
            "portfolio_position_id": 7,
            "paper_position_id": 9,
            "quantity": 100,
            "price": 10,
            "trade_date": "2026-08-24",
            "origin": "portfolio_import",
            "status": "synced",
        }
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.sync_portfolio_position_to_paper",
            return_value={"created": True, "items": [link]},
        ) as sync:
            response = self.client.post(
                "/api/v1/paper-trading/sync/from-portfolio",
                json={
                    "portfolio_position_id": 7,
                    "source_models": ["model-a"],
                    "trade_date": "2026-08-24",
                    "idempotency_key": "import-1",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["created"])
        self.assertEqual(response.json()["items"][0]["paper_position_id"], 9)
        sync.assert_called_once()
        _, kwargs = sync.call_args
        self.assertEqual(kwargs["portfolio_position_id"], 7)
        self.assertEqual(kwargs["source_models"], ["model-a"])
        self.assertEqual(kwargs["idempotency_key"], "import-1")

    def test_sync_to_portfolio_conflict_is_409(self) -> None:
        from src.services.paper_trading_service import PaperTradingConflictError

        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.sync_paper_position_to_portfolio",
            side_effect=PaperTradingConflictError("duplicate"),
        ):
            response = self.client.post(
                "/api/v1/paper-trading/sync/to-portfolio",
                json={
                    "paper_position_id": 9,
                    "portfolio_account_id": 2,
                    "side": "buy",
                    "quantity": 100,
                    "price": 10,
                    "trade_date": "2026-08-24",
                    "fee": 0,
                    "idempotency_key": "paper-1",
                },
            )

        self.assertEqual(response.status_code, 409)
        body = response.json()
        payload = body.get("detail") if isinstance(body, dict) and "detail" in body else body
        self.assertEqual(payload["error"], "sync_conflict")

    def test_create_plan_rejects_research_report_even_with_payload_purpose(self) -> None:
        from src.storage import AnalysisReportExecutionIntent, PaperExecutionPlan

        with DatabaseManager.get_instance().session_scope() as session:
            report = AnalysisHistory(
                query_id="api-research",
                code="600519",
                name="贵州茅台",
                report_type="full",
                model_used="model-a",
                operation_advice="买入",
                raw_result="{}",
                created_at=datetime(2026, 8, 10, 15, 0),
            )
            session.add(report)
            session.flush()
            session.add(AnalysisReportExecutionIntent(
                analysis_history_id=report.id,
                execution_intent="research_only",
                execution_scope="primary",
                execution_policy_version="report-execution-intent-v1",
                intent_source="api_test",
            ))
            report_id = report.id

        response = self.client.post(
            "/api/v1/paper-trading/plans",
            # Request fields cannot upgrade the purpose frozen at first save.
            json={"source_report_id": report_id, "execution_intent": "paper_candidate", "execution_scope": "primary"},
        )

        self.assertEqual(response.status_code, 409)
        body = response.json()
        self.assertEqual(body["error"], "execution_not_authorized")
        self.assertEqual(body["detail"], {"reason": "research_only", "report_id": report_id})
        with DatabaseManager.get_instance().get_session() as session:
            self.assertEqual(session.query(PaperExecutionPlan).count(), 0)

    def test_lists_rule_evaluation_evidence(self) -> None:
        evaluation = {
            "id": 3,
            "plan_id": 1,
            "evaluation_date": "2026-08-20",
            "result": "not_triggered",
            "reasons": ["volume_ratio_below_threshold"],
            "evidence": {"rules": [{"passed": False}]},
            "source_model": "model-a",
            "stock_code": "600519",
            "stock_name": "贵州茅台",
        }
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_evaluations",
            return_value={"items": [evaluation], "total": 1},
        ) as list_evaluations:
            response = self.client.get(
                "/api/v1/paper-trading/evaluations?model=model-a&limit=10"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["items"][0]["evidence"]["rules"][0]["passed"], False)
        list_evaluations.assert_called_once_with(model="model-a", offset=0, limit=10)

    def test_lists_intraday_observation_evidence(self) -> None:
        observation = {
            "id": 4,
            "plan_id": 1,
            "position_id": None,
            "source_report_id": 9,
            "decision_signal_id": 7,
            "source_model": "model-a",
            "market": "cn",
            "stock_code": "600519",
            "stock_name": "贵州茅台",
            "mode": "intraday",
            "observed_at": "2026-08-21T10:00:00",
            "bucket_started_at": "2026-08-21T10:00:00",
            "quote_price": 101.2,
            "quote_source": "unit_quote",
            "quote_provider_timestamp": None,
            "quote_fetched_at": "2026-08-21T10:00:01+08:00",
            "result": "not_triggered",
            "action": "none",
            "composite_score": 0.3,
            "required_passed": False,
            "factors_passed": 1,
            "factors_available": 3,
            "evidence": {"factor_domains": [{"domain": "volume", "available": False}]},
        }
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_observations",
            return_value={"items": [observation], "total": 501},
        ) as list_observations:
            response = self.client.get(
                "/api/v1/paper-trading/observations?model=model-a&page=2&limit=50"
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["total"], 501)
        self.assertEqual(payload["items"][0]["quote_fetched_at"], "2026-08-21T10:00:01+08:00")
        self.assertFalse(payload["items"][0]["required_passed"])
        list_observations.assert_called_once_with(model="model-a", offset=50, limit=50)

    def test_lists_auction_observations_and_current_risk_locks(self) -> None:
        auction_observation = {
            "id": 5,
            "plan_id": 1,
            "source_model": "model-a",
            "market": "cn",
            "stock_code": "600519",
            "mode": "intraday",
            "result": "reanalysis_blocked",
            "action": "none",
            "evidence": {
                "mode": "preopen_auction",
                "auction": {"available": True, "phase": "opening_auction"},
            },
        }
        lock = {
            "capital_account_id": 7,
            "snapshot_id": 11,
            "source_model": "model-a",
            "market": "cn",
            "active_state": "reduce_only",
            "open_enabled": False,
            "add_enabled": False,
            "exits_enabled": True,
            "active_reasons": ["unknown_concentration_cap_exceeded"],
        }
        with patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_auction_observations",
            return_value={"items": [auction_observation], "total": 1},
        ) as list_auction, patch(
            "api.v1.endpoints.paper_trading.PaperTradingService.list_current_account_risk_locks",
            return_value={"items": [lock], "total": 1},
        ) as list_locks:
            auction_response = self.client.get(
                "/api/v1/paper-trading/auction-observations?model=model-a&page=2&limit=50"
            )
            locks_response = self.client.get(
                "/api/v1/paper-trading/account-risk/locks?model=model-a&account_id=7&page=1&limit=20"
            )

        self.assertEqual(auction_response.status_code, 200)
        self.assertEqual(auction_response.json()["items"][0]["evidence"]["mode"], "preopen_auction")
        self.assertEqual(locks_response.status_code, 200)
        self.assertFalse(locks_response.json()["items"][0]["open_enabled"])
        self.assertTrue(locks_response.json()["items"][0]["exits_enabled"])
        list_auction.assert_called_once_with(model="model-a", offset=50, limit=50)
        list_locks.assert_called_once_with(model="model-a", account_id=7, offset=0, limit=20)


if __name__ == "__main__":
    unittest.main()
