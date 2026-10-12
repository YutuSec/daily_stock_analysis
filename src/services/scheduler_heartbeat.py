# -*- coding: utf-8 -*-
"""Shared scheduler heartbeat helpers for cross-process status reporting."""

from __future__ import annotations

import json
import logging
import os
import socket
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

from sqlalchemy import desc, select
from sqlalchemy.exc import IntegrityError

from src.storage import DatabaseManager, RuntimeSchedulerHeartbeat, RuntimeSchedulerLease, utc_naive_now

logger = logging.getLogger(__name__)

SCHEDULER_HEARTBEAT_TTL_SECONDS = 120


def _owner_id(owner_kind: str) -> str:
    configured = os.getenv("DSA_SCHEDULER_OWNER_ID", "").strip()
    if configured:
        return configured[:128]
    host = socket.gethostname() or "unknown-host"
    return f"{host}:{os.getpid()}:{owner_kind}"[:128]


def scheduler_owner_id(owner_kind: str = "scheduler") -> str:
    """Public stable owner id for scheduler lease/status evidence."""

    return _owner_id(owner_kind)


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return "{}"


def record_scheduler_heartbeat(
    *,
    owner_kind: str,
    task_name: str,
    status: str = "alive",
    schedule_times: Optional[Sequence[str]] = None,
    next_run_at: Optional[str] = None,
    jobs: Optional[Iterable[Mapping[str, Any]]] = None,
    last_success_at: Optional[datetime] = None,
    last_error_at: Optional[datetime] = None,
    error_message: Optional[str] = None,
    details: Optional[Mapping[str, Any]] = None,
    db_manager: Optional[DatabaseManager] = None,
) -> None:
    """Persist one best-effort heartbeat row update."""

    db = db_manager or DatabaseManager.get_instance()
    now = utc_naive_now()
    kind = str(owner_kind or "scheduler")[:32]
    name = str(task_name or "scheduler")[:96]
    owner = _owner_id(kind)
    heartbeat_key = f"{kind}:{name}:{owner}"[:160]
    payload = {
        "schedule_times": list(schedule_times or []),
        "next_run_at": next_run_at,
        "jobs": list(jobs or []),
        **dict(details or {}),
    }
    with db.session_scope() as session:
        row = session.execute(
            select(RuntimeSchedulerHeartbeat)
            .where(RuntimeSchedulerHeartbeat.heartbeat_key == heartbeat_key)
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            row = RuntimeSchedulerHeartbeat(
                heartbeat_key=heartbeat_key,
                owner_id=owner,
                owner_kind=kind,
                task_name=name,
                status=str(status or "alive")[:32],
                pid=os.getpid(),
                observed_at=now,
                last_success_at=last_success_at,
                last_error_at=last_error_at,
                error_message=(str(error_message)[:2000] if error_message else None),
                details_json=_json_dumps(payload),
            )
            session.add(row)
            try:
                session.flush()
                return
            except IntegrityError:
                session.rollback()
                with db.session_scope() as retry_session:
                    retry_row = retry_session.execute(
                        select(RuntimeSchedulerHeartbeat)
                        .where(RuntimeSchedulerHeartbeat.heartbeat_key == heartbeat_key)
                        .limit(1)
                    ).scalar_one_or_none()
                    if retry_row is None:
                        raise
                    _apply_heartbeat(
                        retry_row,
                        status=status,
                        observed_at=now,
                        last_success_at=last_success_at,
                        last_error_at=last_error_at,
                        error_message=error_message,
                        details=payload,
                    )
                return
        _apply_heartbeat(
            row,
            status=status,
            observed_at=now,
            last_success_at=last_success_at,
            last_error_at=last_error_at,
            error_message=error_message,
            details=payload,
        )


def claim_scheduler_task_bucket(
    *,
    task_name: str,
    bucket_started_at: datetime,
    lease_seconds: int = 900,
    owner_kind: str = "scheduler",
    details: Optional[Mapping[str, Any]] = None,
    db_manager: Optional[DatabaseManager] = None,
) -> Dict[str, Any]:
    """Atomically claim a scheduler task/bucket lease across containers.

    The unique ``lease_key`` is scoped to task+bucket, not owner, so duplicate
    scheduler owners converge on one authoritative row.  Expired leases can be
    reclaimed; active leases cannot.
    """

    db = db_manager or DatabaseManager.get_instance()
    now = utc_naive_now()
    bucket = _utc_naive(bucket_started_at) or now
    task = str(task_name or "scheduled_task")[:96]
    owner = _owner_id(owner_kind)
    lease_key = f"{task}:{bucket.isoformat()}"[:220]
    lease_until = now + timedelta(seconds=max(30, int(lease_seconds or 0)))
    payload = _json_dumps(dict(details or {}))

    def _serialize(row: RuntimeSchedulerLease, claimed: bool) -> Dict[str, Any]:
        return {
            "claimed": bool(claimed),
            "lease_key": row.lease_key,
            "owner_id": row.owner_id,
            "owner_kind": row.owner_kind,
            "task_name": row.task_name,
            "bucket_started_at": row.bucket_started_at.isoformat() if row.bucket_started_at else None,
            "lease_until": row.lease_until.isoformat() if row.lease_until else None,
            "status": row.status,
            "last_error": row.last_error,
        }

    session = db.get_session()
    try:
        row = session.execute(
            select(RuntimeSchedulerLease)
            .where(RuntimeSchedulerLease.lease_key == lease_key)
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            row = RuntimeSchedulerLease(
                lease_key=lease_key,
                owner_id=owner,
                owner_kind=str(owner_kind or "scheduler")[:32],
                task_name=task,
                bucket_started_at=bucket,
                lease_until=lease_until,
                status="running",
                started_at=now,
                last_heartbeat_at=now,
                details_json=payload,
            )
            session.add(row)
            try:
                session.commit()
                return _serialize(row, True)
            except IntegrityError:
                session.rollback()
                row = session.execute(
                    select(RuntimeSchedulerLease)
                    .where(RuntimeSchedulerLease.lease_key == lease_key)
                    .limit(1)
                ).scalar_one()
        terminal_claimed = row.status in {"succeeded", "finished", "skipped"}
        if terminal_claimed:
            result = _serialize(row, False)
            session.rollback()
            return result
        active = bool(row.lease_until and row.lease_until > now and row.status in {"running", "claimed"})
        if active and row.owner_id != owner:
            result = _serialize(row, False)
            session.rollback()
            return result
        if (not active) or row.owner_id == owner:
            row.owner_id = owner
            row.owner_kind = str(owner_kind or "scheduler")[:32]
            row.task_name = task
            row.bucket_started_at = bucket
            row.lease_until = lease_until
            row.status = "running"
            row.started_at = row.started_at or now
            row.finished_at = None
            row.last_heartbeat_at = now
            row.last_error = None
            row.details_json = payload
            row.updated_at = now
            session.commit()
            return _serialize(row, True)
        result = _serialize(row, False)
        session.rollback()
        return result
    finally:
        session.close()


def heartbeat_scheduler_task_lease(
    *,
    lease_key: str,
    extend_seconds: int = 900,
    status: str = "running",
    db_manager: Optional[DatabaseManager] = None,
) -> None:
    db = db_manager or DatabaseManager.get_instance()
    now = utc_naive_now()
    with db.session_scope() as session:
        row = session.execute(
            select(RuntimeSchedulerLease)
            .where(RuntimeSchedulerLease.lease_key == str(lease_key or "")[:220])
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            return
        row.status = str(status or "running")[:32]
        row.last_heartbeat_at = now
        row.lease_until = now + timedelta(seconds=max(30, int(extend_seconds or 0)))
        row.updated_at = now


def finish_scheduler_task_lease(
    *,
    lease_key: str,
    status: str,
    error_message: Optional[str] = None,
    details: Optional[Mapping[str, Any]] = None,
    db_manager: Optional[DatabaseManager] = None,
) -> None:
    db = db_manager or DatabaseManager.get_instance()
    now = utc_naive_now()
    with db.session_scope() as session:
        row = session.execute(
            select(RuntimeSchedulerLease)
            .where(RuntimeSchedulerLease.lease_key == str(lease_key or "")[:220])
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            return
        row.status = str(status or "finished")[:32]
        row.finished_at = now
        row.last_heartbeat_at = now
        row.last_error = str(error_message)[:2000] if error_message else None
        if details is not None:
            row.details_json = _json_dumps(dict(details or {}))
        row.updated_at = now


def scheduler_task_states(
    *,
    max_age_seconds: int = SCHEDULER_HEARTBEAT_TTL_SECONDS,
    db_manager: Optional[DatabaseManager] = None,
) -> Dict[str, Any]:
    """Return authoritative per-task heartbeat+lease states from shared DB."""

    db = db_manager or DatabaseManager.get_instance()
    now = utc_naive_now()
    ttl = max(30, int(max_age_seconds))
    states: Dict[str, Any] = {}
    with db.get_session() as session:
        heartbeats = list(session.execute(
            select(RuntimeSchedulerHeartbeat)
            .order_by(desc(RuntimeSchedulerHeartbeat.observed_at), desc(RuntimeSchedulerHeartbeat.id))
            .limit(100)
        ).scalars().all())
        for row in heartbeats:
            task = str(row.task_name or "scheduler")
            if task in states and states[task].get("heartbeat"):
                continue
            observed_at = _utc_naive(row.observed_at)
            age = max(0.0, (now - observed_at).total_seconds()) if observed_at else None
            details = _json_object(row.details_json)
            states.setdefault(task, {})["heartbeat"] = {
                "fresh": bool(age is not None and age <= ttl),
                "status": row.status,
                "owner_id": row.owner_id,
                "owner_kind": row.owner_kind,
                "observed_at": observed_at.isoformat() if observed_at else None,
                "age_seconds": age,
                "last_success_at": row.last_success_at.isoformat() if row.last_success_at else None,
                "last_error_at": row.last_error_at.isoformat() if row.last_error_at else None,
                "error_message": row.error_message,
                "details": details,
            }
        leases = list(session.execute(
            select(RuntimeSchedulerLease)
            .order_by(desc(RuntimeSchedulerLease.bucket_started_at), desc(RuntimeSchedulerLease.updated_at), desc(RuntimeSchedulerLease.id))
            .limit(200)
        ).scalars().all())
        for row in leases:
            task = str(row.task_name or "scheduled_task")
            if states.setdefault(task, {}).get("lease"):
                continue
            active = bool(row.lease_until and row.lease_until > now and row.status in {"running", "claimed"})
            states[task]["lease"] = {
                "lease_key": row.lease_key,
                "task_name": row.task_name,
                "owner_id": row.owner_id,
                "owner_kind": row.owner_kind,
                "bucket_started_at": row.bucket_started_at.isoformat() if row.bucket_started_at else None,
                "lease_until": row.lease_until.isoformat() if row.lease_until else None,
                "active": active,
                "status": row.status,
                "started_at": row.started_at.isoformat() if row.started_at else None,
                "finished_at": row.finished_at.isoformat() if row.finished_at else None,
                "heartbeat_at": row.last_heartbeat_at.isoformat() if row.last_heartbeat_at else None,
                "last_error": row.last_error,
                "details": _json_object(row.details_json),
            }
    return {"available": True, "fresh": any(bool(item.get("heartbeat", {}).get("fresh")) for item in states.values()), "tasks": states}


def _apply_heartbeat(
    row: RuntimeSchedulerHeartbeat,
    *,
    status: str,
    observed_at: datetime,
    last_success_at: Optional[datetime],
    last_error_at: Optional[datetime],
    error_message: Optional[str],
    details: Mapping[str, Any],
) -> None:
    row.status = str(status or "alive")[:32]
    row.pid = os.getpid()
    row.observed_at = observed_at
    if last_success_at is not None:
        row.last_success_at = _utc_naive(last_success_at)
    if last_error_at is not None:
        row.last_error_at = _utc_naive(last_error_at)
    row.error_message = str(error_message)[:2000] if error_message else None
    row.details_json = _json_dumps(details)
    row.updated_at = utc_naive_now()


def latest_scheduler_heartbeat(
    *,
    max_age_seconds: int = SCHEDULER_HEARTBEAT_TTL_SECONDS,
    db_manager: Optional[DatabaseManager] = None,
) -> Dict[str, Any]:
    """Return the freshest shared scheduler heartbeat with stale classification."""

    db = db_manager or DatabaseManager.get_instance()
    with db.get_session() as session:
        row = session.execute(
            select(RuntimeSchedulerHeartbeat)
            .order_by(desc(RuntimeSchedulerHeartbeat.observed_at), desc(RuntimeSchedulerHeartbeat.id))
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            return {"available": False, "fresh": False, "status": "missing"}
        observed_at = _utc_naive(row.observed_at)
        now = utc_naive_now()
        age_seconds = max(0.0, (now - observed_at).total_seconds()) if observed_at else None
        fresh = age_seconds is not None and age_seconds <= max(30, int(max_age_seconds))
        details = _json_object(row.details_json)
        return {
            "available": True,
            "fresh": bool(fresh),
            "status": str(row.status or "unknown"),
            "heartbeat_key": row.heartbeat_key,
            "owner_id": row.owner_id,
            "owner_kind": row.owner_kind,
            "task_name": row.task_name,
            "pid": row.pid,
            "observed_at": observed_at.isoformat() if observed_at else None,
            "age_seconds": age_seconds,
            "last_success_at": row.last_success_at.isoformat() if row.last_success_at else None,
            "last_error_at": row.last_error_at.isoformat() if row.last_error_at else None,
            "error_message": row.error_message,
            "details": details,
            "schedule_times": details.get("schedule_times") if isinstance(details.get("schedule_times"), list) else [],
            "next_run_at": details.get("next_run_at"),
            "jobs": details.get("jobs") if isinstance(details.get("jobs"), list) else [],
            "task_states": scheduler_task_states(
                max_age_seconds=max_age_seconds,
                db_manager=db,
            ).get("tasks", {}),
        }


def _json_object(raw: Any) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(str(raw))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _utc_naive(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is not None and value.utcoffset() is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.replace(tzinfo=None)
