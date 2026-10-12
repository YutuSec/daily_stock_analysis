# -*- coding: utf-8 -*-
"""
===================================
定时调度模块
===================================

职责：
1. 支持每日定时执行股票分析
2. 支持定时执行大盘复盘
3. 优雅处理信号，确保可靠退出

依赖：
- schedule: 轻量级定时任务库
"""

import logging
import re
import signal
import threading
import time
from datetime import datetime, time as datetime_time, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, Union
from zoneinfo import ZoneInfo
from src.utils.sanitize import sanitize_decision_signal_text

logger = logging.getLogger(__name__)


def normalize_schedule_times(
    schedule_times: Optional[Union[Sequence[str], str]],
    *,
    fallback_time: str = "18:00",
) -> List[str]:
    """Return sorted unique HH:MM schedule times with SCHEDULE_TIME fallback."""
    if isinstance(schedule_times, str):
        raw_items = [item.strip() for item in schedule_times.split(",")]
    elif schedule_times is None:
        raw_items = []
    else:
        raw_items = [str(item).strip() for item in schedule_times]

    valid = {
        item
        for item in raw_items
        if item and re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", item)
    }
    if not valid:
        fallback = (fallback_time or "18:00").strip() or "18:00"
        valid.add(fallback if re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", fallback) else "18:00")
    return sorted(valid)


class GracefulShutdown:
    """
    优雅退出处理器

    捕获 SIGTERM/SIGINT 信号，确保任务完成后再退出
    """

    def __init__(self, register_signals: bool = True):
        self.shutdown_requested = False
        self._lock = threading.Lock()
        if not register_signals:
            return

        # 注册信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """信号处理函数"""
        with self._lock:
            if not self.shutdown_requested:
                logger.info(f"收到退出信号 ({signum})，等待当前任务完成...")
                self.shutdown_requested = True

    @property
    def should_shutdown(self) -> bool:
        """检查是否应该退出"""
        with self._lock:
            return self.shutdown_requested


class Scheduler:
    """
    定时任务调度器

    基于 schedule 库实现，支持：
    - 每日定时执行
    - 启动时立即执行
    - 优雅退出
    """

    def __init__(
        self,
        schedule_time: str = "18:00",
        schedule_time_provider: Optional[Callable[[], str]] = None,
        schedule_times: Optional[Sequence[str]] = None,
        schedule_times_provider: Optional[Callable[[], Union[Sequence[str], str]]] = None,
        register_signals: bool = True,
        shared_lease_enabled: bool = False,
        owner_kind: str = "scheduler",
    ):
        """
        初始化调度器

        Args:
            schedule_time: 每日执行时间，格式 "HH:MM"
        """
        try:
            import schedule
            self.schedule = schedule
        except ImportError:
            logger.error("schedule 库未安装，请执行: pip install schedule")
            raise ImportError("请安装 schedule 库: pip install schedule")

        self.schedule_time = schedule_time
        self.schedule_times = (
            normalize_schedule_times(schedule_times, fallback_time=schedule_time)
            if schedule_times is not None
            else [(schedule_time or "").strip()]
        )
        self._schedule_time_provider = schedule_time_provider
        self._schedule_times_provider = schedule_times_provider
        self.shutdown_handler = GracefulShutdown(register_signals=register_signals)
        self._task_callback: Optional[Callable] = None
        self._daily_job: Optional[Any] = None
        self._daily_jobs: List[Any] = []
        self._daily_jobs_by_time: Dict[str, Any] = {}
        self._daily_task_lock = threading.Lock()
        self._daily_task_thread: Optional[threading.Thread] = None
        self._daily_task_running = False
        self._daily_schedule_states: Dict[str, Dict[str, Any]] = {}
        self._daily_last_wake_at: Optional[datetime] = None
        self._daily_last_start_at: Optional[datetime] = None
        self._daily_last_finish_at: Optional[datetime] = None
        self._daily_last_success_at: Optional[datetime] = None
        self._daily_last_error_at: Optional[datetime] = None
        self._daily_last_error: Optional[str] = None
        self._daily_last_skip_at: Optional[datetime] = None
        self._daily_last_skip_reason: Optional[str] = None
        self._background_tasks: List[Dict[str, Any]] = []
        self._running = False
        self._shared_lease_enabled = bool(shared_lease_enabled)
        self._owner_kind = str(owner_kind or "scheduler")
        self._daily_task_name = "daily_analysis"
        self._daily_task_lease_seconds = 6 * 60 * 60

    def set_daily_task(self, task: Callable, run_immediately: bool = True):
        """
        设置每日定时任务

        Args:
            task: 要执行的任务函数（无参数）
            run_immediately: 是否在设置后立即执行一次
        """
        self._task_callback = task
        if not self._configure_daily_tasks(self.schedule_times):
            raise ValueError(f"无效的定时执行时间: {self.schedule_time!r}")

        if run_immediately:
            logger.info("立即执行一次任务...")
            self._start_daily_task()

    @staticmethod
    def _is_valid_schedule_time(schedule_time: str) -> bool:
        """Validate time string in HH:MM 24-hour format."""
        candidate = (schedule_time or "").strip()
        if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", candidate):
            return False
        return True

    def _cancel_daily_job(self) -> None:
        """Remove the currently registered daily job if one exists."""
        if self._daily_job is None and not self._daily_jobs:
            return

        for job in list(self._daily_jobs or [self._daily_job]):
            if job is None:
                continue
            if hasattr(self.schedule, "cancel_job"):
                self.schedule.cancel_job(job)
            else:  # pragma: no cover - compatibility fallback
                jobs = getattr(self.schedule, "jobs", None)
                if isinstance(jobs, list) and job in jobs:
                    jobs.remove(job)

        self._daily_job = None
        self._daily_jobs = []
        self._daily_jobs_by_time = {}

    def _configure_daily_task(self, schedule_time: str) -> bool:
        """(Re)register the daily job at the requested time."""
        candidate = (schedule_time or "").strip()
        if not self._is_valid_schedule_time(candidate):
            logger.warning(
                "检测到无效的定时执行时间 %r，继续沿用当前时间 %s",
                schedule_time,
                self.schedule_time,
            )
            return False

        previous_time = self.schedule_time
        self._cancel_daily_job()
        self._daily_job = self.schedule.every().day.at(candidate).do(self._start_daily_task)
        self.schedule_time = candidate

        if previous_time == candidate:
            logger.info("已设置每日定时任务，执行时间: %s", self.schedule_time)
        else:
            logger.info(
                "检测到 SCHEDULE_TIME 变更，已将每日定时任务从 %s 更新为 %s",
                previous_time,
                self.schedule_time,
            )
        return True

    def _refresh_daily_schedule_if_needed(self) -> None:
        """Reload daily schedule time from the latest runtime config if needed."""
        if self._task_callback is None or self._schedule_time_provider is None:
            return

        try:
            latest_schedule_time = (self._schedule_time_provider() or "").strip()
        except Exception as exc:  # pragma: no cover - defensive branch
            logger.warning("读取最新 SCHEDULE_TIME 失败，继续沿用 %s: %s", self.schedule_time, exc)
            return

        if not latest_schedule_time or latest_schedule_time == self.schedule_time:
            return

        if self._configure_daily_task(latest_schedule_time):
            logger.info("更新后的下次执行时间: %s", self._get_next_run_time())

    def _configure_daily_tasks(self, schedule_times: Union[Sequence[str], str]) -> bool:
        """(Re)register daily jobs at the requested times."""
        raw_items = (
            [item.strip() for item in schedule_times.split(",")]
            if isinstance(schedule_times, str)
            else [str(item).strip() for item in schedule_times]
        )
        invalid_items = [item for item in raw_items if item and not self._is_valid_schedule_time(item)]
        if invalid_items:
            logger.warning(
                "Invalid schedule time values %r; keeping current times %s",
                invalid_items,
                ",".join(self.schedule_times),
            )
            return False

        candidates = normalize_schedule_times(raw_items, fallback_time=self.schedule_time)
        previous_times = list(self.schedule_times)
        self._cancel_daily_job()
        jobs_by_time: Dict[str, Any] = {}
        for candidate in candidates:
            jobs_by_time[candidate] = self.schedule.every().day.at(candidate).do(
                lambda candidate=candidate: self._start_daily_task(schedule_time=candidate)
            )
            self._daily_schedule_state(candidate)
        self._daily_jobs_by_time = jobs_by_time
        self._daily_jobs = list(jobs_by_time.values())
        self._daily_job = self._daily_jobs[0] if self._daily_jobs else None
        self.schedule_times = candidates
        self.schedule_time = candidates[0] if candidates else "18:00"

        if previous_times == candidates:
            logger.info("Daily scheduled jobs configured at: %s", ",".join(self.schedule_times))
        else:
            logger.info(
                "Schedule times changed from %s to %s",
                ",".join(previous_times),
                ",".join(self.schedule_times),
            )
        return True

    def _refresh_daily_schedule_if_needed(self) -> None:
        """Reload daily schedule times from the latest runtime config if needed."""
        if self._task_callback is None:
            return

        try:
            if self._schedule_times_provider is not None:
                latest_schedule_times = self._schedule_times_provider()
            elif self._schedule_time_provider is not None:
                latest_schedule_times = [(self._schedule_time_provider() or "").strip()]
            else:
                return
        except Exception as exc:  # pragma: no cover - defensive branch
            logger.warning(
                "Failed to read latest schedule times; keeping %s: %s",
                ",".join(self.schedule_times),
                exc,
            )
            return

        latest = normalize_schedule_times(latest_schedule_times, fallback_time=self.schedule_time)
        if latest == self.schedule_times:
            return

        if self._configure_daily_tasks(latest):
            logger.info("Schedule refreshed; next run: %s", self._get_next_run_time())

    def refresh_daily_schedule_if_needed(self) -> None:
        """Public wrapper for runtime scheduler reconciliation."""
        self._refresh_daily_schedule_if_needed()

    def _safe_run_task(self):
        """安全执行任务（带异常捕获）"""
        if self._task_callback is None:
            return

        try:
            logger.info("=" * 50)
            logger.info(f"定时任务开始执行 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 50)

            result = self._task_callback()
            if result is False:
                raise RuntimeError("定时任务返回失败状态")

            logger.info(f"定时任务执行完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        except Exception as e:
            logger.exception(f"定时任务执行失败: {e}")
            raise

    def _daily_task_identity(self, schedule_time: Optional[str] = None) -> str:
        candidate = str(schedule_time or (self.schedule_times[0] if self.schedule_times else self.schedule_time) or "").strip()
        return f"{self._daily_task_name}:{candidate}" if candidate else self._daily_task_name

    def _daily_schedule_state(self, schedule_time: Optional[str] = None) -> Dict[str, Any]:
        candidate = str(schedule_time or (self.schedule_times[0] if self.schedule_times else self.schedule_time) or "").strip()
        if not candidate:
            candidate = self.schedule_time or "18:00"
        state = self._daily_schedule_states.setdefault(candidate, {
            "running": False,
            "thread": None,
            "last_wake_at": None,
            "last_start_at": None,
            "last_finish_at": None,
            "last_success_at": None,
            "last_error_at": None,
            "last_error": None,
            "last_skip_at": None,
            "last_skip_reason": None,
        })
        return state

    def _daily_next_due_at(self, schedule_time: str) -> str:
        job = self._daily_jobs_by_time.get(schedule_time)
        next_run = getattr(job, "next_run", None) if job is not None else None
        if next_run is not None:
            return next_run.strftime('%Y-%m-%d %H:%M:%S')
        return self._get_next_run_time()

    def _start_daily_task(self, schedule_time: Optional[str] = None) -> bool:
        """Start the daily analysis in a single-flight worker thread.

        The scheduler loop also launches latency-sensitive background jobs such
        as the intraday paper-trading monitor.  Daily analysis can spend hours
        waiting on data providers or LLMs, so executing it inside
        ``schedule.run_pending()`` would starve every background job.  Keep the
        scheduler loop responsive and skip overlapping daily runs instead.
        """
        schedule_key = str(schedule_time or (self.schedule_times[0] if self.schedule_times else self.schedule_time) or "").strip()
        state = self._daily_schedule_state(schedule_key)
        with self._daily_task_lock:
            wake_at = self._heartbeat_now()
            state["last_wake_at"] = wake_at
            self._daily_last_wake_at = wake_at
            worker = self._daily_task_thread
            if self._daily_task_running or (worker is not None and worker.is_alive()):
                skip_at = self._heartbeat_now()
                state["last_skip_at"] = skip_at
                state["last_skip_reason"] = "already_running"
                self._daily_last_skip_at = skip_at
                self._daily_last_skip_reason = "already_running"
                logger.warning("上一轮定时分析仍在运行，跳过本次重复触发")
                return False

            def _runner() -> None:
                lease_key = claim.get("lease_key") if isinstance(claim, dict) else None
                error_message = None
                try:
                    self._safe_run_task()
                except Exception as exc:
                    error_message = sanitize_decision_signal_text(str(exc))
                    error_at = self._heartbeat_now()
                    state["last_error_at"] = error_at
                    state["last_error"] = error_message[:2000]
                    self._daily_last_error_at = error_at
                    self._daily_last_error = error_message[:2000]
                    # The worker records failure below; do not terminate the
                    # scheduler loop or emit an unhandled thread exception.
                finally:
                    finish_at = self._heartbeat_now()
                    state["last_finish_at"] = finish_at
                    self._daily_last_finish_at = finish_at
                    if error_message is None:
                        state["last_success_at"] = finish_at
                        state["last_error"] = None
                        self._daily_last_success_at = finish_at
                        self._daily_last_error = None
                    self._finish_shared_lease(
                        lease_key,
                        status="failed" if error_message else "succeeded",
                        error_message=error_message,
                    )
                    with self._daily_task_lock:
                        state["running"] = False
                        state["thread"] = None
                        self._daily_task_running = False
                        self._daily_task_thread = None

            claim = self._claim_shared_lease(
                task_name=self._daily_task_identity(schedule_key),
                interval_seconds=24 * 60 * 60,
                lease_seconds=self._daily_task_lease_seconds,
                details={"kind": "daily", "schedule_time": schedule_key, "schedule_times": list(self.schedule_times)},
            )
            if not claim.get("claimed", True):
                skip_at = self._heartbeat_now()
                state["last_skip_at"] = skip_at
                state["last_skip_reason"] = "lease_claim_denied"
                self._daily_last_skip_at = skip_at
                self._daily_last_skip_reason = "lease_claim_denied"
                logger.info(
                    "定时任务 %s 已被其他调度器实例领取，跳过本实例执行: %s",
                    self._daily_task_name,
                    claim.get("owner_id"),
                )
                return False
            state["running"] = True
            self._daily_task_running = True
            start_at = self._heartbeat_now()
            state["last_start_at"] = start_at
            state["last_skip_reason"] = None
            self._daily_last_start_at = start_at
            self._daily_last_skip_reason = None
            worker = threading.Thread(
                target=_runner,
                daemon=True,
                name=f"scheduler-daily-analysis-{schedule_key or 'default'}",
            )
            state["thread"] = worker
            self._daily_task_thread = worker

        try:
            worker.start()
        except Exception:
            with self._daily_task_lock:
                state["running"] = False
                state["thread"] = None
                if self._daily_task_thread is worker:
                    self._daily_task_running = False
                    self._daily_task_thread = None
            raise
        return True

    def add_background_task(
        self,
        task: Callable,
        interval_seconds: int,
        run_immediately: bool = False,
        name: Optional[str] = None,
        active_time_start: Optional[str] = None,
        active_time_end: Optional[str] = None,
        active_timezone: str = "Asia/Shanghai",
        run_once_per_window: bool = False,
        lease_enabled: Optional[bool] = None,
        lease_task_name: Optional[str] = None,
        lease_seconds: Optional[int] = None,
    ) -> None:
        """Register a periodic background task executed inside the scheduler loop.

        Note: The scheduler loop polls every 30 seconds, so *interval_seconds*
        below 30 will be clamped to 30 to avoid promising unreachable precision.
        """
        clamped_interval = max(30, int(interval_seconds))
        if int(interval_seconds) < 30:
            logger.warning(
                "后台任务 %s 请求间隔 %ds，但调度循环每 30s 轮询一次，已自动调整为 30s",
                name or getattr(task, "__name__", "background_task"),
                interval_seconds,
            )
        entry = {
            "task": task,
            "interval_seconds": clamped_interval,
            "last_run": 0.0,
            "name": name or getattr(task, "__name__", "background_task"),
            "enabled": True,
            "thread": None,
            "running": False,
            "lease_enabled": self._shared_lease_enabled if lease_enabled is None else bool(lease_enabled),
            "lease_task_name": lease_task_name or name or getattr(task, "__name__", "background_task"),
            "lease_seconds": max(60, int(lease_seconds or max(120, clamped_interval * 3))),
            "last_wake_at": None,
            "last_start_at": None,
            "last_finish_at": None,
            "last_success_at": None,
            "last_error_at": None,
            "last_error": None,
            "last_skip_at": None,
            "last_skip_reason": None,
            "last_run_summary": None,
        }
        if active_time_start or active_time_end:
            entry["active_time_start"] = active_time_start
            entry["active_time_end"] = active_time_end
            entry["active_timezone"] = active_timezone
            entry["run_once_per_window"] = bool(run_once_per_window)
            entry["last_window_run_date"] = None
        if not run_immediately:
            entry["last_run"] = time.time()
        self._background_tasks.append(entry)
        logger.info(
            "已注册后台任务: %s（间隔 %s 秒，立即执行=%s）",
            entry["name"],
            entry["interval_seconds"],
            run_immediately,
        )
        if run_immediately:
            self._start_background_task(entry)

    @staticmethod
    def _parse_background_clock(value: Optional[str]) -> Optional[datetime_time]:
        candidate = (value or "").strip()
        if not candidate:
            return None
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(candidate, fmt).time()
            except ValueError:
                continue
        return None

    def _background_window_due(self, entry: Dict[str, Any], *, now_monotonic: float) -> bool:
        """Return whether a window-scoped background task should run now."""

        start_raw = entry.get("active_time_start")
        end_raw = entry.get("active_time_end")
        if not start_raw and not end_raw:
            return now_monotonic - entry["last_run"] >= entry["interval_seconds"]

        start = self._parse_background_clock(start_raw)
        end = self._parse_background_clock(end_raw)
        if start is None or end is None:
            logger.warning("后台任务 %s 配置了无效窗口，跳过", entry.get("name"))
            return False

        try:
            tz = ZoneInfo(str(entry.get("active_timezone") or "Asia/Shanghai"))
        except Exception:
            tz = ZoneInfo("Asia/Shanghai")
        local_now = datetime.now(tz)
        local_clock = local_now.time().replace(tzinfo=None)
        if not (start <= local_clock <= end):
            return False

        window_date = local_now.date().isoformat()
        if entry.get("run_once_per_window") and entry.get("last_window_run_date") == window_date:
            return False
        if entry.get("last_window_run_date") != window_date:
            return True
        return now_monotonic - entry["last_run"] >= entry["interval_seconds"]

    @staticmethod
    def _heartbeat_now() -> datetime:
        return datetime.now(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _heartbeat_iso(value: Any) -> Optional[str]:
        if isinstance(value, datetime):
            return value.isoformat()
        return None

    def _background_next_due_at(self, entry: Dict[str, Any], *, now_epoch: Optional[float] = None) -> Optional[str]:
        start_raw = entry.get("active_time_start")
        end_raw = entry.get("active_time_end")
        now_epoch = time.time() if now_epoch is None else now_epoch
        if not start_raw and not end_raw:
            last_run = float(entry.get("last_run") or 0.0)
            due_epoch = now_epoch if last_run <= 0 else max(now_epoch, last_run + int(entry.get("interval_seconds") or 60))
            return datetime.fromtimestamp(due_epoch, timezone.utc).replace(tzinfo=None).isoformat()

        start = self._parse_background_clock(start_raw)
        end = self._parse_background_clock(end_raw)
        if start is None or end is None:
            return None
        try:
            tz = ZoneInfo(str(entry.get("active_timezone") or "Asia/Shanghai"))
        except Exception:
            tz = ZoneInfo("Asia/Shanghai")
        local_now = datetime.now(tz)
        local_clock = local_now.time().replace(tzinfo=None)
        due_date = local_now.date()
        candidate = datetime.combine(due_date, start, tzinfo=tz)
        if start <= local_clock <= end:
            candidate = local_now
        elif local_clock > end:
            due_date = due_date + timedelta(days=1)
            candidate = datetime.combine(due_date, start, tzinfo=tz)
        return candidate.astimezone(timezone.utc).replace(tzinfo=None).isoformat()

    def _background_heartbeat_details(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": entry.get("name"),
            "task_name": str(entry.get("lease_task_name") or entry.get("name") or "background_task"),
            "enabled": bool(entry.get("enabled", True)),
            "running": bool(entry.get("running")),
            "interval_seconds": int(entry.get("interval_seconds") or 0),
            "active_time_start": entry.get("active_time_start"),
            "active_time_end": entry.get("active_time_end"),
            "active_timezone": entry.get("active_timezone"),
            "run_once_per_window": bool(entry.get("run_once_per_window")),
            "last_wake_at": self._heartbeat_iso(entry.get("last_wake_at")),
            "last_start_at": self._heartbeat_iso(entry.get("last_start_at")),
            "last_finish_at": self._heartbeat_iso(entry.get("last_finish_at")),
            "last_success_at": self._heartbeat_iso(entry.get("last_success_at")),
            "last_error_at": self._heartbeat_iso(entry.get("last_error_at")),
            "last_error": entry.get("last_error"),
            "last_skip_at": self._heartbeat_iso(entry.get("last_skip_at")),
            "last_skip_reason": entry.get("last_skip_reason"),
            "last_run_summary": entry.get("last_run_summary"),
            "next_due_at": self._background_next_due_at(entry),
        }

    @staticmethod
    def _background_run_summary(
        result: Any,
        *,
        status: str,
        started_at: Optional[datetime],
        finished_at: datetime,
        duration_seconds: Optional[float],
        error_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = dict(result) if isinstance(result, dict) else {}
        events = payload.get("events")
        summary = {
            "status": status,
            "started_at": started_at.isoformat() if isinstance(started_at, datetime) else None,
            "finished_at": finished_at.isoformat(),
            "duration_seconds": duration_seconds,
            "mode": payload.get("mode"),
            "observed_at": payload.get("observed_at"),
            "bucket_started_at": payload.get("bucket_started_at"),
            "target_identity_count": payload.get("target_identity_count"),
            "plans_evaluated": payload.get("plans_evaluated"),
            "positions_evaluated": payload.get("positions_evaluated"),
            "quote_fetch_attempts": payload.get("quote_fetch_attempts"),
            "quote_fetch_successes": payload.get("quote_fetch_successes"),
            "quote_fetch_failures": payload.get("quote_fetch_failures"),
            "quote_stale_count": payload.get("quote_stale_count"),
            "observations": payload.get("observations"),
            "positions_opened": payload.get("positions_opened"),
            "positions_closed": payload.get("positions_closed"),
            "positions_reduced": payload.get("positions_reduced"),
            "valuation_marks_recorded": payload.get("valuation_marks_recorded"),
            "skipped_closed_market": payload.get("skipped_closed_market"),
            "failed_closed": payload.get("failed_closed"),
            "event_count": len(events) if isinstance(events, list) else 0,
        }
        if error_message:
            summary["error_message"] = sanitize_decision_signal_text(error_message)[:2000]
        return {key: value for key, value in summary.items() if value is not None}

    def _start_background_task(self, entry: Dict[str, Any]) -> bool:
        """Start one background task in a dedicated daemon thread."""
        worker = entry.get("thread")
        if worker is not None and worker.is_alive():
            entry["last_skip_at"] = self._heartbeat_now()
            entry["last_skip_reason"] = "already_running"
            return False
        claim = self._claim_shared_lease(
            task_name=str(entry.get("lease_task_name") or entry.get("name") or "background_task"),
            interval_seconds=int(entry.get("interval_seconds") or 60),
            lease_seconds=int(entry.get("lease_seconds") or 300),
            details={
                "kind": "background",
                "name": entry.get("name"),
                "interval_seconds": entry.get("interval_seconds"),
                "active_time_start": entry.get("active_time_start"),
                "active_time_end": entry.get("active_time_end"),
                "active_timezone": entry.get("active_timezone"),
            },
            enabled=bool(entry.get("lease_enabled")),
        )
        if not claim.get("claimed", True):
            entry["last_run"] = time.time()
            entry["last_skip_at"] = self._heartbeat_now()
            entry["last_skip_reason"] = "lease_claim_denied"
            logger.info(
                "后台任务 %s 已被其他调度器实例领取，跳过本实例执行: %s",
                entry.get("name"),
                claim.get("owner_id"),
            )
            return False

        def _runner() -> None:
            lease_key = claim.get("lease_key") if isinstance(claim, dict) else None
            error_message = None
            skipped = False
            result = None
            started_at = entry.get("last_start_at")
            started_perf = time.perf_counter()
            try:
                logger.info("后台任务开始执行: %s", entry["name"])
                result = entry["task"]()
                if result is False:
                    raise RuntimeError("后台任务返回失败状态")
                skipped = isinstance(result, dict) and result.get("status") == "skipped"
                if skipped:
                    entry["last_skip_at"] = self._heartbeat_now()
                    entry["last_skip_reason"] = str(result.get("reason") or "callback_skipped")
            except Exception as exc:
                error_message = sanitize_decision_signal_text(exc)
                entry["last_error_at"] = self._heartbeat_now()
                entry["last_error"] = error_message[:2000]
                logger.error("后台任务执行失败 [%s]: %s", entry["name"], error_message)
            finally:
                finish_at = self._heartbeat_now()
                duration_seconds = max(0.0, time.perf_counter() - started_perf)
                entry["last_finish_at"] = finish_at
                if error_message is None and not skipped:
                    entry["last_success_at"] = finish_at
                    entry["last_error"] = None
                    entry["last_run_summary"] = self._background_run_summary(
                        result,
                        status="succeeded",
                        started_at=started_at,
                        finished_at=finish_at,
                        duration_seconds=duration_seconds,
                    )
                elif error_message is not None:
                    entry["last_run_summary"] = self._background_run_summary(
                        {},
                        status="failed",
                        started_at=started_at,
                        finished_at=finish_at,
                        duration_seconds=duration_seconds,
                        error_message=error_message,
                    )
                self._finish_shared_lease(
                    lease_key,
                    status="failed" if error_message else ("skipped" if skipped else "succeeded"),
                    error_message=error_message or (entry.get("last_error") if skipped else None),
                    details={"last_run_summary": entry.get("last_run_summary")} if entry.get("last_run_summary") else None,
                )
                entry["running"] = False
                entry["thread"] = None
                self._record_shared_heartbeat("alive")

        entry["last_run"] = time.time()
        entry["running"] = True
        entry["last_start_at"] = self._heartbeat_now()
        entry["last_skip_reason"] = None
        worker = threading.Thread(
            target=_runner,
            daemon=True,
            name=f"scheduler-bg-{entry['name']}",
        )
        entry["thread"] = worker
        worker.start()
        return True

    def _run_background_tasks(self) -> None:
        """Execute any background tasks whose interval has elapsed."""
        if not self._background_tasks:
            return

        now = time.time()
        for entry in self._background_tasks:
            entry["last_wake_at"] = self._heartbeat_now()
            worker = entry.get("thread")
            if worker is not None and worker.is_alive():
                entry["last_skip_at"] = self._heartbeat_now()
                entry["last_skip_reason"] = "already_running"
                continue
            if entry.get("running"):
                entry["running"] = False
                entry["thread"] = None
            if not self._background_window_due(entry, now_monotonic=now):
                entry["last_skip_at"] = self._heartbeat_now()
                entry["last_skip_reason"] = "not_due"
                continue
            if self._start_background_task(entry):
                if entry.get("active_time_start") or entry.get("active_time_end"):
                    try:
                        tz = ZoneInfo(str(entry.get("active_timezone") or "Asia/Shanghai"))
                    except Exception:
                        tz = ZoneInfo("Asia/Shanghai")
                    entry["last_window_run_date"] = datetime.now(tz).date().isoformat()

    def run(self):
        """
        运行调度器主循环

        阻塞运行，直到收到退出信号
        """
        self._running = True
        logger.info("调度器开始运行...")
        logger.info(f"下次执行时间: {self._get_next_run_time()}")

        while self._running and not self.shutdown_handler.should_shutdown:
            self._refresh_daily_schedule_if_needed()
            self.schedule.run_pending()
            self._run_background_tasks()
            self._record_shared_heartbeat("alive")
            time.sleep(30)  # 每30秒检查一次

            # 每小时打印一次心跳
            if datetime.now().minute == 0 and datetime.now().second < 30:
                logger.info(f"调度器运行中... 下次执行: {self._get_next_run_time()}")

        self._record_shared_heartbeat("stopped")
        logger.info("调度器已停止")

    def _record_shared_heartbeat(self, status: str) -> None:
        """Best-effort shared heartbeat for web/analyse split deployments."""

        try:
            from src.services.scheduler_heartbeat import record_scheduler_heartbeat

            jobs = []
            for job in self.schedule.get_jobs():
                jobs.append({
                    "next_run": getattr(job, "next_run", None).isoformat()
                    if getattr(job, "next_run", None) is not None else None,
                })
            background = [self._background_heartbeat_details(entry) for entry in self._background_tasks]
            for schedule_time in self.schedule_times:
                task_name = self._daily_task_identity(schedule_time)
                state = self._daily_schedule_state(schedule_time)
                next_due_at = self._daily_next_due_at(schedule_time)
                record_scheduler_heartbeat(
                    owner_kind=self._owner_kind,
                    task_name=task_name,
                    status=(
                        "stopped" if status == "stopped"
                        else "running" if state.get("running")
                        else "idle"
                    ),
                    schedule_times=[schedule_time],
                    next_run_at=next_due_at,
                    jobs=[
                        {
                            "schedule_time": schedule_time,
                            "next_run": (
                                getattr(self._daily_jobs_by_time.get(schedule_time), "next_run", None).isoformat()
                                if getattr(self._daily_jobs_by_time.get(schedule_time), "next_run", None) is not None
                                else None
                            ),
                        }
                    ],
                    last_success_at=state.get("last_success_at"),
                    last_error_at=state.get("last_error_at"),
                    error_message=state.get("last_error"),
                    details={
                        "task_name": task_name,
                        "enabled": self._task_callback is not None,
                        "running": bool(state.get("running")),
                        "schedule_time": schedule_time,
                        "schedule_times": list(self.schedule_times),
                        "last_wake_at": self._heartbeat_iso(state.get("last_wake_at")),
                        "last_start_at": self._heartbeat_iso(state.get("last_start_at")),
                        "last_finish_at": self._heartbeat_iso(state.get("last_finish_at")),
                        "last_success_at": self._heartbeat_iso(state.get("last_success_at")),
                        "last_error_at": self._heartbeat_iso(state.get("last_error_at")),
                        "last_error": state.get("last_error"),
                        "last_skip_at": self._heartbeat_iso(state.get("last_skip_at")),
                        "last_skip_reason": state.get("last_skip_reason"),
                        "next_due_at": next_due_at,
                        "background_tasks": background,
                    },
                )
            for entry in self._background_tasks:
                details = self._background_heartbeat_details(entry)
                task_status = (
                    "stopped" if status == "stopped"
                    else "running" if details["running"]
                    else "idle"
                )
                record_scheduler_heartbeat(
                    owner_kind=self._owner_kind,
                    task_name=details["task_name"],
                    status=task_status,
                    schedule_times=[],
                    next_run_at=details.get("next_due_at"),
                    jobs=[],
                    last_success_at=entry.get("last_success_at"),
                    last_error_at=entry.get("last_error_at"),
                    error_message=entry.get("last_error"),
                    details=details,
                )
        except Exception as exc:  # pragma: no cover - status reporting must not stop schedules.
            logger.debug("Shared scheduler heartbeat skipped: %s", exc, exc_info=True)

    @staticmethod
    def _bucket_started_at(interval_seconds: int) -> datetime:
        interval = max(30, int(interval_seconds or 60))
        timestamp = int(datetime.now(timezone.utc).timestamp())
        return datetime.fromtimestamp(timestamp - (timestamp % interval), timezone.utc).replace(tzinfo=None)

    def _claim_shared_lease(
        self,
        *,
        task_name: str,
        interval_seconds: int,
        lease_seconds: int,
        details: Optional[Dict[str, Any]] = None,
        enabled: Optional[bool] = None,
    ) -> Dict[str, Any]:
        if not (self._shared_lease_enabled if enabled is None else bool(enabled)):
            return {"claimed": True, "lease_disabled": True}
        try:
            from src.services.scheduler_heartbeat import claim_scheduler_task_bucket

            return claim_scheduler_task_bucket(
                task_name=task_name,
                bucket_started_at=self._bucket_started_at(interval_seconds),
                lease_seconds=lease_seconds,
                owner_kind=self._owner_kind,
                details=details or {},
            )
        except Exception as exc:  # pragma: no cover - scheduler should keep local mode alive if status DB is unavailable.
            logger.warning("Shared scheduler lease unavailable for %s: %s", task_name, exc)
            return {"claimed": False, "lease_unavailable": True, "error": str(exc)}

    def _finish_shared_lease(
        self,
        lease_key: Optional[str],
        *,
        status: str,
        error_message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not lease_key:
            return
        try:
            from src.services.scheduler_heartbeat import finish_scheduler_task_lease

            finish_scheduler_task_lease(
                lease_key=lease_key,
                status=status,
                error_message=error_message,
                details=details,
            )
        except Exception as exc:  # pragma: no cover - status reporting must not stop schedules.
            logger.debug("Shared scheduler lease finish skipped for %s: %s", lease_key, exc, exc_info=True)

    def _get_next_run_time(self) -> str:
        """获取下次执行时间"""
        jobs = self.schedule.get_jobs()
        if jobs:
            next_run = min(job.next_run for job in jobs)
            return next_run.strftime('%Y-%m-%d %H:%M:%S')
        return "未设置"

    def stop(self):
        """停止调度器"""
        self._running = False
        self._cancel_daily_job()


def run_with_schedule(
    task: Callable,
    schedule_time: str = "18:00",
    run_immediately: bool = True,
    background_tasks: Optional[List[Dict[str, Any]]] = None,
    schedule_time_provider: Optional[Callable[[], str]] = None,
    schedule_times: Optional[Sequence[str]] = None,
    schedule_times_provider: Optional[Callable[[], Union[Sequence[str], str]]] = None,
    shared_lease_enabled: bool = False,
):
    """
    便捷函数：使用定时调度运行任务

    Args:
        task: 要执行的任务函数
        schedule_time: 每日执行时间
        run_immediately: 是否立即执行一次
        background_tasks: 可选的后台任务定义列表。每项为一个字典，
            需包含 `task` 与 `interval_seconds`，可选包含 `name`
            和 `run_immediately`。`interval_seconds` 单位为秒。
        schedule_time_provider: 可选的时间提供器；调度器每轮检查前会读取，
            当返回值变化时自动重建 daily job。
    """
    scheduler_kwargs: Dict[str, Any] = {
        "schedule_time": schedule_time,
        "schedule_time_provider": schedule_time_provider,
    }
    if shared_lease_enabled:
        scheduler_kwargs["shared_lease_enabled"] = True
    if schedule_times is not None:
        scheduler_kwargs["schedule_times"] = schedule_times
    if schedule_times_provider is not None:
        scheduler_kwargs["schedule_times_provider"] = schedule_times_provider
    scheduler = Scheduler(**scheduler_kwargs)
    for entry in background_tasks or []:
        scheduler.add_background_task(
            task=entry["task"],
            interval_seconds=entry["interval_seconds"],
            run_immediately=entry.get("run_immediately", False),
            name=entry.get("name"),
            active_time_start=entry.get("active_time_start"),
            active_time_end=entry.get("active_time_end"),
            active_timezone=entry.get("active_timezone", "Asia/Shanghai"),
            run_once_per_window=entry.get("run_once_per_window", False),
            lease_enabled=entry.get("lease_enabled"),
            lease_task_name=entry.get("lease_task_name"),
            lease_seconds=entry.get("lease_seconds"),
        )
    scheduler.set_daily_task(task, run_immediately=run_immediately)
    scheduler.run()


if __name__ == "__main__":
    # 测试定时调度
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    )

    def test_task():
        print(f"任务执行中... {datetime.now()}")
        time.sleep(2)
        print("任务完成!")

    print("启动测试调度器（按 Ctrl+C 退出）")
    run_with_schedule(test_task, schedule_time="23:59", run_immediately=True)
