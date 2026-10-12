# -*- coding: utf-8 -*-
"""Tests for Scheduler background task support."""

import json
import os
import tempfile
from datetime import datetime, timedelta
import sys
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


class _FakeJob:
    def __init__(self, schedule_module):
        self._schedule_module = schedule_module
        self.next_run = datetime(2026, 1, 1, 18, 0, 0)
        self.at_time = None

    @property
    def day(self):
        return self

    def at(self, value):
        self.at_time = value
        hour, minute = [int(part) for part in value.split(":")]
        self.next_run = datetime(2026, 1, 1, hour, minute, 0)
        return self

    def do(self, fn):
        self.job_func = fn
        self._schedule_module.jobs.append(self)
        return self


class _FakeScheduleModule:
    def __init__(self):
        self.jobs = []

    def every(self):
        return _FakeJob(self)

    def get_jobs(self):
        return list(self.jobs)

    def run_pending(self):
        return None

    def cancel_job(self, job):
        self.jobs.remove(job)


class SchedulerBackgroundTaskTestCase(unittest.TestCase):
    def test_daily_failure_is_persisted_and_other_schedule_can_continue(self):
        from src.config import Config
        from src.storage import DatabaseManager
        from src.services.scheduler_heartbeat import scheduler_task_states
        from src.scheduler import Scheduler

        with tempfile.TemporaryDirectory() as tmpdir, patch.dict(os.environ, {
            "DATABASE_PATH": os.path.join(tmpdir, "failure.db"),
            "DATABASE_URL": "", "POSTGRES_DEFAULT_URL": "",
        }):
            Config.reset_instance()
            DatabaseManager.reset_instance()
            try:
                scheduler = Scheduler(schedule_times=["15:10", "18:00"],
                                      register_signals=False, shared_lease_enabled=True)
                entered, release = threading.Event(), threading.Event()

                def fail():
                    entered.set()
                    release.wait(5)
                    raise RuntimeError("provider_failure_fixture")

                scheduler._task_callback = fail
                self.assertTrue(scheduler._start_daily_task("15:10"))
                self.assertTrue(entered.wait(5))
                worker = scheduler._daily_schedule_state("15:10")["thread"]
                release.set()
                worker.join(5)
                self.assertFalse(worker.is_alive())
                scheduler._record_shared_heartbeat("alive")
                states = scheduler_task_states(db_manager=DatabaseManager.get_instance())["tasks"]
                failed = states["daily_analysis:15:10"]
                self.assertEqual(failed["lease"]["status"], "failed")
                self.assertIsNone(failed["heartbeat"]["details"]["last_success_at"])
                self.assertIn("provider_failure_fixture", failed["heartbeat"]["details"]["last_error"])
                self.assertFalse(scheduler._daily_task_running)
                scheduler._task_callback = lambda: True
                self.assertTrue(scheduler._start_daily_task("18:00"))
                worker = scheduler._daily_schedule_state("18:00")["thread"]
                if worker is not None:
                    worker.join(5)
                    self.assertFalse(worker.is_alive())
                states = scheduler_task_states(db_manager=DatabaseManager.get_instance())["tasks"]
                self.assertEqual(states["daily_analysis:18:00"]["lease"]["status"], "succeeded")
            finally:
                DatabaseManager.reset_instance()
                Config.reset_instance()

    def test_daily_false_return_is_failure_not_success(self):
        from src.scheduler import Scheduler
        scheduler = Scheduler(register_signals=False, shared_lease_enabled=False)
        scheduler._task_callback = lambda: False
        with self.assertRaisesRegex(RuntimeError, "返回失败状态"):
            scheduler._safe_run_task()

    def test_background_false_return_records_failed_lease(self):
        from src.scheduler import Scheduler
        scheduler = Scheduler(register_signals=False, shared_lease_enabled=False)
        scheduler.add_background_task(lambda: False, interval_seconds=60, name="false-fixture")
        entry = scheduler._background_tasks[0]
        with patch.object(scheduler, "_finish_shared_lease") as finish:
            self.assertTrue(scheduler._start_background_task(entry))
            worker = entry.get("thread")
            if worker is not None:
                worker.join(5)
                self.assertFalse(worker.is_alive())
        self.assertIsNone(entry["last_success_at"])
        self.assertEqual(entry["last_run_summary"]["status"], "failed")
        self.assertEqual(finish.call_args.kwargs["status"], "failed")

    def test_shared_heartbeat_publishes_registered_background_before_first_execution(self):
        old_database_path = os.environ.get("DATABASE_PATH")
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ["DATABASE_PATH"] = os.path.join(tmpdir, "scheduler-heartbeat.db")
            from src.config import Config
            from src.storage import DatabaseManager
            from src.services.scheduler_heartbeat import scheduler_task_states

            Config.reset_instance()
            DatabaseManager.reset_instance()
            try:
                fake_schedule = _FakeScheduleModule()
                with patch.dict(sys.modules, {"schedule": fake_schedule}):
                    from src.scheduler import Scheduler

                    scheduler = Scheduler(
                        schedule_time="18:00",
                        shared_lease_enabled=True,
                        owner_kind="unit-scheduler",
                    )
                    scheduler.add_background_task(
                        lambda: None,
                        interval_seconds=60,
                        run_immediately=False,
                        name="paper_trading_intraday_monitor",
                        lease_task_name="paper_trading_intraday_monitor",
                    )
                    scheduler._record_shared_heartbeat("alive")

                states = scheduler_task_states(db_manager=DatabaseManager.get_instance())
                self.assertIn("daily_analysis:18:00", states["tasks"])
                self.assertNotIn("daily_analysis", states["tasks"])
                self.assertIn("paper_trading_intraday_monitor", states["tasks"])
                heartbeat = states["tasks"]["paper_trading_intraday_monitor"]["heartbeat"]
                self.assertEqual(heartbeat["status"], "idle")
                self.assertTrue(heartbeat["fresh"])
                details = heartbeat["details"]
                self.assertTrue(details["enabled"])
                self.assertFalse(details["running"])
                self.assertEqual(details["interval_seconds"], 60)
                self.assertIsNotNone(details["next_due_at"])
            finally:
                DatabaseManager.reset_instance()
                Config.reset_instance()
                if old_database_path is None:
                    os.environ.pop("DATABASE_PATH", None)
                else:
                    os.environ["DATABASE_PATH"] = old_database_path

    def test_daily_heartbeat_and_lease_share_time_scoped_task_identity(self):
        old_database_path = os.environ.get("DATABASE_PATH")
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ["DATABASE_PATH"] = os.path.join(tmpdir, "scheduler-daily-identity.db")
            from src.config import Config
            from src.storage import DatabaseManager
            from src.services.scheduler_heartbeat import scheduler_task_states

            Config.reset_instance()
            DatabaseManager.reset_instance()
            try:
                fake_schedule = _FakeScheduleModule()
                with patch.dict(sys.modules, {"schedule": fake_schedule}):
                    from src.scheduler import Scheduler

                    scheduler = Scheduler(
                        schedule_time="18:00",
                        schedule_times=["15:10", "18:00"],
                        shared_lease_enabled=True,
                    )
                    calls = []
                    scheduler.set_daily_task(lambda: calls.append("daily"), run_immediately=False)
                    scheduler._record_shared_heartbeat("alive")

                    states = scheduler_task_states(db_manager=DatabaseManager.get_instance())
                    self.assertIn("daily_analysis:15:10", states["tasks"])
                    self.assertIn("daily_analysis:18:00", states["tasks"])
                    self.assertNotIn("daily_analysis", states["tasks"])
                    self.assertNotIn("lease", states["tasks"]["daily_analysis:15:10"])

                    fake_thread = MagicMock()
                    fake_thread.is_alive.return_value = False

                    def _make_thread(target=None, **kwargs):
                        fake_thread.start.side_effect = target
                        return fake_thread

                    with patch("src.scheduler.threading.Thread", side_effect=_make_thread):
                        self.assertTrue(scheduler._start_daily_task(schedule_time="15:10"))
                    scheduler._record_shared_heartbeat("alive")

                states = scheduler_task_states(db_manager=DatabaseManager.get_instance())
                scoped = states["tasks"]["daily_analysis:15:10"]
                self.assertEqual(calls, ["daily"])
                self.assertIn("heartbeat", scoped)
                self.assertIn("lease", scoped)
                self.assertEqual(scoped["lease"]["task_name"], "daily_analysis:15:10")
                self.assertEqual(scoped["heartbeat"]["details"]["task_name"], "daily_analysis:15:10")
                self.assertEqual(scoped["heartbeat"]["details"]["schedule_time"], "15:10")
            finally:
                DatabaseManager.reset_instance()
                Config.reset_instance()
                if old_database_path is None:
                    os.environ.pop("DATABASE_PATH", None)
                else:
                    os.environ["DATABASE_PATH"] = old_database_path

    def test_daily_heartbeat_keeps_per_schedule_next_due_and_runtime_state(self):
        old_database_path = os.environ.get("DATABASE_PATH")
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ["DATABASE_PATH"] = os.path.join(tmpdir, "scheduler-daily-per-time.db")
            from src.config import Config
            from src.storage import DatabaseManager
            from src.services.scheduler_heartbeat import scheduler_task_states

            Config.reset_instance()
            DatabaseManager.reset_instance()
            try:
                fake_schedule = _FakeScheduleModule()
                with patch.dict(sys.modules, {"schedule": fake_schedule}):
                    from src.scheduler import Scheduler

                    scheduler = Scheduler(
                        schedule_time="15:10",
                        schedule_times=["15:10", "18:00"],
                        shared_lease_enabled=True,
                    )
                    calls = []
                    scheduler.set_daily_task(lambda: calls.append("daily"), run_immediately=False)
                    scheduler._record_shared_heartbeat("alive")

                    before = scheduler_task_states(db_manager=DatabaseManager.get_instance())["tasks"]
                    self.assertEqual(before["daily_analysis:15:10"]["heartbeat"]["details"]["next_due_at"], "2026-01-01 15:10:00")
                    self.assertEqual(before["daily_analysis:18:00"]["heartbeat"]["details"]["next_due_at"], "2026-01-01 18:00:00")

                    fake_thread = MagicMock()
                    fake_thread.is_alive.return_value = False

                    def _make_thread(target=None, **kwargs):
                        fake_thread.start.side_effect = target
                        return fake_thread

                    with patch("src.scheduler.threading.Thread", side_effect=_make_thread):
                        self.assertTrue(scheduler._start_daily_task(schedule_time="15:10"))
                    scheduler._record_shared_heartbeat("alive")

                states = scheduler_task_states(db_manager=DatabaseManager.get_instance())["tasks"]
                first = states["daily_analysis:15:10"]["heartbeat"]["details"]
                second = states["daily_analysis:18:00"]["heartbeat"]["details"]
                self.assertEqual(calls, ["daily"])
                self.assertIsNotNone(first["last_wake_at"])
                self.assertIsNotNone(first["last_start_at"])
                self.assertIsNotNone(first["last_finish_at"])
                self.assertIsNotNone(first["last_success_at"])
                self.assertIsNone(second["last_wake_at"])
                self.assertIsNone(second["last_start_at"])
                self.assertIsNone(second["last_finish_at"])
                self.assertIsNone(second["last_success_at"])
                self.assertEqual(first["next_due_at"], "2026-01-01 15:10:00")
                self.assertEqual(second["next_due_at"], "2026-01-01 18:00:00")
                self.assertIn("lease", states["daily_analysis:15:10"])
                self.assertNotIn("lease", states["daily_analysis:18:00"])
            finally:
                DatabaseManager.reset_instance()
                Config.reset_instance()
                if old_database_path is None:
                    os.environ.pop("DATABASE_PATH", None)
                else:
                    os.environ["DATABASE_PATH"] = old_database_path

    def test_shared_lease_store_exception_fails_closed_for_background_dispatch(self):
        fake_schedule = _FakeScheduleModule()

        def claim_scheduler_task_bucket(**_kwargs):
            raise RuntimeError("lease-store-down")

        fake_heartbeat = SimpleNamespace(claim_scheduler_task_bucket=claim_scheduler_task_bucket)
        with patch.dict(sys.modules, {"schedule": fake_schedule, "src.services.scheduler_heartbeat": fake_heartbeat}):
            from src.scheduler import Scheduler

            calls = []
            scheduler = Scheduler(schedule_time="18:00", shared_lease_enabled=True)
            fake_thread = MagicMock()
            fake_thread.is_alive.return_value = False

            def _make_thread(target=None, **kwargs):
                fake_thread.start.side_effect = target
                return fake_thread

            with patch("src.scheduler.threading.Thread", side_effect=_make_thread):
                scheduler.add_background_task(
                    lambda: calls.append("ran"),
                    interval_seconds=30,
                    run_immediately=True,
                    name="paper_trading_intraday_monitor",
                )

        self.assertEqual(calls, [])
        self.assertEqual(scheduler._background_tasks[0]["last_skip_reason"], "lease_claim_denied")

    def test_shared_heartbeat_reports_stale_dead_owner_per_task(self):
        old_database_path = os.environ.get("DATABASE_PATH")
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ["DATABASE_PATH"] = os.path.join(tmpdir, "scheduler-stale.db")
            from src.config import Config
            from src.storage import DatabaseManager, RuntimeSchedulerHeartbeat, utc_naive_now
            from src.services.scheduler_heartbeat import scheduler_task_states

            Config.reset_instance()
            DatabaseManager.reset_instance()
            db = DatabaseManager.get_instance()
            try:
                with db.session_scope() as session:
                    session.add(RuntimeSchedulerHeartbeat(
                        heartbeat_key="scheduler:paper_trading_opening_confirmation_0930:dead-owner",
                        owner_id="dead-owner",
                        owner_kind="scheduler",
                        task_name="paper_trading_opening_confirmation_0930",
                        status="idle",
                        observed_at=utc_naive_now() - timedelta(minutes=10),
                        details_json=json.dumps({"enabled": True, "running": False}, ensure_ascii=False),
                    ))

                states = scheduler_task_states(max_age_seconds=30, db_manager=db)
                heartbeat = states["tasks"]["paper_trading_opening_confirmation_0930"]["heartbeat"]
                self.assertFalse(heartbeat["fresh"])
                self.assertEqual(heartbeat["owner_id"], "dead-owner")
                self.assertGreater(heartbeat["age_seconds"], 30)
            finally:
                DatabaseManager.reset_instance()
                Config.reset_instance()
                if old_database_path is None:
                    os.environ.pop("DATABASE_PATH", None)
                else:
                    os.environ["DATABASE_PATH"] = old_database_path

    def test_background_task_runs_when_interval_elapsed(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src.scheduler import Scheduler

            scheduler = Scheduler(schedule_time="18:00")
            calls = []
            fake_thread = MagicMock()
            fake_thread.is_alive.return_value = False

            def _make_thread(target=None, **kwargs):
                fake_thread.start.side_effect = target
                return fake_thread

            with patch("src.scheduler.threading.Thread", side_effect=_make_thread):
                scheduler.add_background_task(lambda: calls.append("ran"), interval_seconds=1, run_immediately=True, name="test")

        self.assertEqual(calls, ["ran"])

    def test_shared_lease_prevents_duplicate_background_bucket_owners(self):
        fake_schedule = _FakeScheduleModule()
        lease_rows = {}
        finished = []

        def claim_scheduler_task_bucket(*, task_name, bucket_started_at, owner_kind, **kwargs):
            key = f"{task_name}:{bucket_started_at.isoformat()}"
            if key in lease_rows:
                return {**lease_rows[key], "claimed": False}
            row = {
                "claimed": True,
                "lease_key": key,
                "owner_id": owner_kind,
                "owner_kind": owner_kind,
                "task_name": task_name,
                "bucket_started_at": bucket_started_at.isoformat(),
                "status": "running",
            }
            lease_rows[key] = dict(row)
            return row

        def finish_scheduler_task_lease(*, lease_key, status, **kwargs):
            lease_rows[lease_key]["status"] = status
            finished.append((lease_key, status))

        fake_heartbeat = SimpleNamespace(
            claim_scheduler_task_bucket=claim_scheduler_task_bucket,
            finish_scheduler_task_lease=finish_scheduler_task_lease,
        )
        with patch.dict(sys.modules, {"schedule": fake_schedule, "src.services.scheduler_heartbeat": fake_heartbeat}):
            from src.scheduler import Scheduler

            calls = []
            fake_thread = MagicMock()
            fake_thread.is_alive.return_value = False

            def _make_thread(target=None, **kwargs):
                fake_thread.start.side_effect = target
                return fake_thread

            with patch("src.scheduler.threading.Thread", side_effect=_make_thread):
                first = Scheduler(schedule_time="18:00", shared_lease_enabled=True, owner_kind="owner-a")
                second = Scheduler(schedule_time="18:00", shared_lease_enabled=True, owner_kind="owner-b")
                first.add_background_task(lambda: calls.append("owner-a"), interval_seconds=30, run_immediately=True, name="bucketed")
                second.add_background_task(lambda: calls.append("owner-b"), interval_seconds=30, run_immediately=True, name="bucketed")

        self.assertEqual(calls, ["owner-a"])
        self.assertEqual(len(finished), 1)

    def test_background_heartbeat_keeps_last_run_summary_across_not_due_skip(self):
        fake_schedule = _FakeScheduleModule()
        lease_rows = {}
        heartbeat_rows = []

        def claim_scheduler_task_bucket(*, task_name, bucket_started_at, owner_kind, **kwargs):
            key = f"{task_name}:{bucket_started_at.isoformat()}"
            row = {
                "claimed": True,
                "lease_key": key,
                "owner_id": owner_kind,
                "owner_kind": owner_kind,
                "task_name": task_name,
                "bucket_started_at": bucket_started_at.isoformat(),
                "status": "running",
            }
            lease_rows[key] = dict(row)
            return row

        def finish_scheduler_task_lease(*, lease_key, status, details=None, **kwargs):
            lease_rows[lease_key]["status"] = status
            lease_rows[lease_key]["details"] = details or {}

        def record_scheduler_heartbeat(**kwargs):
            heartbeat_rows.append(kwargs)

        fake_heartbeat = SimpleNamespace(
            claim_scheduler_task_bucket=claim_scheduler_task_bucket,
            finish_scheduler_task_lease=finish_scheduler_task_lease,
            record_scheduler_heartbeat=record_scheduler_heartbeat,
        )
        with patch.dict(sys.modules, {"schedule": fake_schedule, "src.services.scheduler_heartbeat": fake_heartbeat}):
            from src.scheduler import Scheduler

            fake_thread = MagicMock()
            fake_thread.is_alive.return_value = False

            def _make_thread(target=None, **kwargs):
                fake_thread.start.side_effect = target
                return fake_thread

            with patch("src.scheduler.threading.Thread", side_effect=_make_thread):
                scheduler = Scheduler(schedule_time="18:00", shared_lease_enabled=True, owner_kind="owner-a")
                scheduler.add_background_task(
                    lambda: {
                        "mode": "intraday",
                        "target_identity_count": 0,
                        "quote_fetch_attempts": 0,
                        "quote_fetch_successes": 0,
                        "skipped_closed_market": 0,
                        "failed_closed": 0,
                        "observations": 0,
                    },
                    interval_seconds=60,
                    run_immediately=True,
                    name="paper_trading_intraday_monitor",
                    lease_task_name="paper_trading_intraday_monitor",
                )
                scheduler._run_background_tasks()
                scheduler._record_shared_heartbeat("alive")

        summaries = [
            row["details"].get("last_run_summary")
            for row in heartbeat_rows
            if row.get("task_name") == "paper_trading_intraday_monitor"
        ]
        self.assertTrue(summaries)
        self.assertEqual(summaries[-1]["status"], "succeeded")
        self.assertEqual(summaries[-1]["target_identity_count"], 0)
        self.assertEqual(summaries[-1]["quote_fetch_successes"], 0)
        self.assertEqual(scheduler._background_tasks[0]["last_skip_reason"], "not_due")
        self.assertEqual(next(iter(lease_rows.values()))["details"]["last_run_summary"]["status"], "succeeded")

    def test_background_task_waits_for_interval(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src.scheduler import Scheduler

            scheduler = Scheduler(schedule_time="18:00")
            calls = []
            scheduler.add_background_task(lambda: calls.append("ran"), interval_seconds=60, run_immediately=False, name="test")

            with patch("src.scheduler.time.time", return_value=scheduler._background_tasks[0]["last_run"] + 10):
                scheduler._run_background_tasks()

        self.assertEqual(calls, [])

    def test_window_background_task_catches_up_inside_official_window_only(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src.scheduler import Scheduler
            from datetime import datetime as real_datetime

            class FakeDateTime:
                current = real_datetime(2026, 8, 26, 9, 30, 20)

                @classmethod
                def now(cls, tz=None):
                    if tz is not None:
                        return cls.current.replace(tzinfo=tz)
                    return cls.current

                @staticmethod
                def strptime(value, fmt):
                    return real_datetime.strptime(value, fmt)

            scheduler = Scheduler(schedule_time="18:00")
            calls = []
            fake_thread = MagicMock()
            fake_thread.is_alive.return_value = False

            def _make_thread(target=None, **kwargs):
                fake_thread.start.side_effect = target
                return fake_thread

            with patch("src.scheduler.datetime", FakeDateTime), patch("src.scheduler.time.time", return_value=1000.0):
                scheduler.add_background_task(
                    lambda: calls.append(FakeDateTime.current.strftime("%H:%M:%S")),
                    interval_seconds=30,
                    run_immediately=False,
                    name="paper_trading_opening_confirmation_0930",
                    active_time_start="09:30:00",
                    active_time_end="09:35:00",
                    active_timezone="Asia/Shanghai",
                )
                with patch("src.scheduler.threading.Thread", side_effect=_make_thread):
                    scheduler._run_background_tasks()
                    scheduler._run_background_tasks()
                FakeDateTime.current = real_datetime(2026, 8, 26, 9, 35, 1)
                with patch("src.scheduler.threading.Thread", side_effect=_make_thread):
                    scheduler._run_background_tasks()

        self.assertEqual(calls, ["09:30:20"])

    def test_legacy_0926_window_is_not_active_for_opening_confirmation(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src.scheduler import Scheduler
            from datetime import datetime as real_datetime

            class FakeDateTime:
                current = real_datetime(2026, 8, 26, 9, 26, 20)

                @classmethod
                def now(cls, tz=None):
                    if tz is not None:
                        return cls.current.replace(tzinfo=tz)
                    return cls.current

                @staticmethod
                def strptime(value, fmt):
                    return real_datetime.strptime(value, fmt)

            scheduler = Scheduler(schedule_time="18:00")
            calls = []
            scheduler.add_background_task(
                lambda: calls.append("called"),
                interval_seconds=30,
                run_immediately=False,
                name="paper_trading_opening_confirmation_0930",
                active_time_start="09:30:00",
                active_time_end="09:35:00",
                active_timezone="Asia/Shanghai",
            )
            with patch("src.scheduler.datetime", FakeDateTime), patch("src.scheduler.time.time", return_value=1000.0):
                scheduler._run_background_tasks()

        self.assertEqual(calls, [])

    def test_long_daily_task_does_not_block_background_tasks(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src.scheduler import Scheduler

            scheduler = Scheduler(schedule_time="18:00")
            daily_started = threading.Event()
            release_daily = threading.Event()
            background_ran = threading.Event()

            def daily_task():
                daily_started.set()
                release_daily.wait(timeout=2)

            scheduler.set_daily_task(daily_task, run_immediately=True)
            self.assertTrue(daily_started.wait(timeout=1))

            scheduler.add_background_task(
                background_ran.set,
                interval_seconds=60,
                run_immediately=True,
                name="paper_trading_intraday_monitor",
            )
            self.assertTrue(background_ran.wait(timeout=1))

            release_daily.set()
            worker = scheduler._daily_task_thread
            if worker is not None:
                worker.join(timeout=1)

    def test_overlapping_daily_trigger_is_skipped(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src.scheduler import Scheduler

            scheduler = Scheduler(schedule_time="18:00")
            daily_started = threading.Event()
            release_daily = threading.Event()
            calls = []

            def daily_task():
                calls.append("ran")
                daily_started.set()
                release_daily.wait(timeout=2)

            scheduler.set_daily_task(daily_task, run_immediately=True)
            self.assertTrue(daily_started.wait(timeout=1))
            self.assertFalse(scheduler._start_daily_task())
            self.assertEqual(calls, ["ran"])

            release_daily.set()
            worker = scheduler._daily_task_thread
            if worker is not None:
                worker.join(timeout=1)

    def test_run_with_schedule_registers_background_tasks_before_immediate_daily_task(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src import scheduler as scheduler_module

            order = []

            class FakeScheduler:
                def __init__(self, schedule_time="18:00", schedule_time_provider=None):
                    order.append(("init", schedule_time))
                    order.append(("provider", callable(schedule_time_provider)))

                def add_background_task(self, **kwargs):
                    order.append(("background", kwargs))

                def set_daily_task(self, task, run_immediately=True):
                    order.append(("daily", run_immediately))

                def run(self):
                    order.append(("run", None))

            with patch.object(scheduler_module, "Scheduler", FakeScheduler):
                scheduler_module.run_with_schedule(
                    task=lambda: None,
                    run_immediately=True,
                    background_tasks=[{
                        "task": lambda: None,
                        "interval_seconds": 60,
                        "run_immediately": True,
                        "name": "event_monitor",
                        "active_time_start": "09:30:00",
                        "active_time_end": "09:35:00",
                        "active_timezone": "Asia/Shanghai",
                        "run_once_per_window": True,
                    }],
                )

        self.assertEqual(order[0:2], [("init", "18:00"), ("provider", False)])
        self.assertEqual(order[2][0], "background")
        self.assertEqual(
            {
                key: order[2][1][key]
                for key in (
                    "name", "active_time_start", "active_time_end",
                    "active_timezone", "run_once_per_window",
                )
            },
            {
                "name": "event_monitor",
                "active_time_start": "09:30:00",
                "active_time_end": "09:35:00",
                "active_timezone": "Asia/Shanghai",
                "run_once_per_window": True,
            },
        )
        self.assertEqual(order[3], ("daily", True))

    def test_scheduler_reloads_daily_job_when_schedule_time_changes(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src.scheduler import Scheduler

            scheduler = Scheduler(
                schedule_time="18:00",
                schedule_time_provider=lambda: "09:30",
            )
            scheduler.set_daily_task(lambda: None, run_immediately=False)

            self.assertEqual(len(fake_schedule.jobs), 1)
            self.assertEqual(fake_schedule.jobs[0].at_time, "18:00")

            scheduler._refresh_daily_schedule_if_needed()

        self.assertEqual(len(fake_schedule.jobs), 1)
        self.assertEqual(fake_schedule.jobs[0].at_time, "09:30")
        self.assertEqual(scheduler.schedule_time, "09:30")

    def test_scheduler_registers_multiple_daily_jobs(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src.scheduler import Scheduler

            scheduler = Scheduler(
                schedule_time="18:00",
                schedule_times=["15:10", "09:20", "15:10"],
            )
            scheduler.set_daily_task(lambda: None, run_immediately=False)

        self.assertEqual([job.at_time for job in fake_schedule.jobs], ["09:20", "15:10"])
        self.assertEqual(scheduler.schedule_times, ["09:20", "15:10"])

    def test_scheduler_stop_cancels_registered_daily_jobs(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src.scheduler import Scheduler

            scheduler = Scheduler(
                schedule_time="18:00",
                schedule_times=["09:20", "15:10"],
            )
            scheduler.set_daily_task(lambda: None, run_immediately=False)
            self.assertEqual(len(fake_schedule.jobs), 2)

            scheduler.stop()

        self.assertEqual(fake_schedule.jobs, [])
        self.assertEqual(scheduler._daily_jobs, [])
        self.assertIsNone(scheduler._daily_job)

    def test_scheduler_keeps_existing_daily_job_when_schedule_time_invalid(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src.scheduler import Scheduler

            scheduler = Scheduler(
                schedule_time="18:00",
                schedule_time_provider=lambda: "25:99",
            )
            scheduler.set_daily_task(lambda: None, run_immediately=False)

            scheduler._refresh_daily_schedule_if_needed()

        self.assertEqual(len(fake_schedule.jobs), 1)
        self.assertEqual(fake_schedule.jobs[0].at_time, "18:00")
        self.assertEqual(scheduler.schedule_time, "18:00")

    def test_scheduler_keeps_current_daily_job_when_schedule_time_provider_fails(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src.scheduler import Scheduler

            provider_calls = {"count": 0}

            def provider():
                provider_calls["count"] += 1
                if provider_calls["count"] == 1:
                    return "09:30"
                raise RuntimeError("boom")

            scheduler = Scheduler(
                schedule_time="18:00",
                schedule_time_provider=provider,
            )
            scheduler.set_daily_task(lambda: None, run_immediately=False)

            scheduler._refresh_daily_schedule_if_needed()
            scheduler._refresh_daily_schedule_if_needed()

        self.assertEqual(len(fake_schedule.jobs), 1)
        self.assertEqual(fake_schedule.jobs[0].at_time, "09:30")
        self.assertEqual(scheduler.schedule_time, "09:30")

    def test_scheduler_rejects_invalid_initial_schedule_time(self):
        fake_schedule = _FakeScheduleModule()
        with patch.dict(sys.modules, {"schedule": fake_schedule}):
            from src.scheduler import Scheduler

            scheduler = Scheduler(schedule_time="25:99")
            calls = []

            with self.assertRaisesRegex(ValueError, "25:99"):
                scheduler.set_daily_task(lambda: calls.append("ran"), run_immediately=True)

        self.assertEqual(calls, [])
        self.assertEqual(fake_schedule.jobs, [])


if __name__ == "__main__":
    unittest.main()
