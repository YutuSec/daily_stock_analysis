# -*- coding: utf-8 -*-
"""Repository for durable daily board observation snapshots."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.exc import IntegrityError

from src.storage import BoardObservation, DatabaseManager, utc_naive_now


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True, default=str)


def _json_loads(value: Any, default: Any) -> Any:
    if not value:
        return default
    try:
        parsed = json.loads(value)
    except Exception:
        return default
    return parsed if parsed is not None else default


class BoardObservationRepository:
    """Persistence facade for source-attributed full board snapshots."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        self.db = db_manager or DatabaseManager.get_instance()

    def upsert_snapshot(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        key = str(payload["snapshot_key"])
        now = utc_naive_now()
        incoming_status = str(payload.get("status") or "healthy").strip().lower()
        try:
            with self.db.session_scope() as session:
                row = session.query(BoardObservation).filter_by(snapshot_key=key).one_or_none()
                if row is None:
                    row = BoardObservation(snapshot_key=key)
                    session.add(row)
                self._apply_payload(row, payload, incoming_status=incoming_status, now=now)
                session.flush()
                return self._to_dict(row)
        except IntegrityError:
            with self.db.session_scope() as session:
                row = session.query(BoardObservation).filter_by(snapshot_key=key).one()
                self._apply_payload(row, payload, incoming_status=incoming_status, now=now)
                session.flush()
                return self._to_dict(row)

    @staticmethod
    def _apply_payload(row: BoardObservation, payload: Dict[str, Any], *, incoming_status: str, now: datetime) -> None:
        existing_status = str(getattr(row, "status", "") or "").strip().lower()
        if existing_status == "healthy":
            if incoming_status != "healthy":
                return
            if row.snapshot_hash:
                return
        row.market = str(payload.get("market", "cn")).strip().lower() or "cn"
        row.trade_date = payload["trade_date"]
        row.source = str(payload.get("source") or "").strip()
        row.taxonomy = str(payload.get("taxonomy") or "").strip()
        row.status = incoming_status
        row.reason = payload.get("reason")
        row.coverage_json = _json_dumps(payload.get("coverage", {}))
        row.themes_json = json.dumps(payload.get("themes", []), ensure_ascii=False, sort_keys=True, default=str)
        row.snapshot_hash = str(payload.get("snapshot_hash") or "")
        row.fetched_at = payload.get("fetched_at") or now
        row.updated_at = now

    def get_snapshot(
        self,
        *,
        market: str,
        trade_date: date,
        source: Optional[str] = None,
        taxonomy: Optional[str] = None,
        healthy_only: bool = False,
    ) -> Optional[Dict[str, Any]]:
        query_market = str(market or "cn").strip().lower() or "cn"
        with self.db.session_scope() as session:
            query = session.query(BoardObservation).filter(
                BoardObservation.market == query_market,
                BoardObservation.trade_date == trade_date,
            )
            if source:
                query = query.filter(BoardObservation.source == source)
            if taxonomy:
                query = query.filter(BoardObservation.taxonomy == taxonomy)
            if healthy_only:
                query = query.filter(BoardObservation.status == "healthy")
            row = query.order_by(BoardObservation.fetched_at.desc(), BoardObservation.id.desc()).first()
            return self._to_dict(row) if row is not None else None

    def list_snapshots(
        self,
        *,
        market: str,
        end_date: date,
        source: Optional[str] = None,
        taxonomy: Optional[str] = None,
        limit: Optional[int] = 40,
        healthy_only: bool = True,
    ) -> List[Dict[str, Any]]:
        query_market = str(market or "cn").strip().lower() or "cn"
        with self.db.session_scope() as session:
            query = session.query(BoardObservation).filter(
                BoardObservation.market == query_market,
                BoardObservation.trade_date <= end_date,
            )
            if source:
                query = query.filter(BoardObservation.source == source)
            if taxonomy:
                query = query.filter(BoardObservation.taxonomy == taxonomy)
            if healthy_only:
                query = query.filter(BoardObservation.status == "healthy")
            query = query.order_by(BoardObservation.trade_date.desc(), BoardObservation.fetched_at.desc())
            if limit is not None:
                query = query.limit(max(1, min(int(limit or 40), 500)))
            rows = query.all()
            return [self._to_dict(row) for row in rows]

    @staticmethod
    def _to_dict(row: BoardObservation) -> Dict[str, Any]:
        return {
            "id": int(row.id),
            "snapshot_key": row.snapshot_key,
            "market": row.market,
            "trade_date": row.trade_date,
            "source": row.source,
            "taxonomy": row.taxonomy,
            "status": row.status,
            "reason": row.reason,
            "coverage": _json_loads(row.coverage_json, {}),
            "themes": _json_loads(row.themes_json, []),
            "snapshot_hash": row.snapshot_hash,
            "fetched_at": row.fetched_at,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
