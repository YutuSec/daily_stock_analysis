# -*- coding: utf-8 -*-
"""Daily board observation service for the market opportunity pool."""

from __future__ import annotations

import hashlib
import json
import logging
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from data_provider.base import DataFetcherManager
from src.config import get_config
from src.core.trading_calendar import get_market_now, is_market_open
from src.repositories.board_observation_repository import BoardObservationRepository
from src.storage import DatabaseManager, utc_naive_now

logger = logging.getLogger(__name__)

DEFAULT_SOURCE = "csrc_tencent"
DEFAULT_TAXONOMY = "csrc_industry"
DEFAULT_THEME_TYPE = "industry"
BOARD_DISCOVERY_POLICY_VERSION = "board-discovery-v2"
OBSERVATION_HEALTH_POLICY_VERSION = "board-observation-health-v2"


class BoardObservationService:
    """Collect and query complete same-source board observations."""

    def __init__(
        self,
        config: Optional[Any] = None,
        db_manager: Optional[DatabaseManager] = None,
        fetcher_manager: Optional[DataFetcherManager] = None,
    ) -> None:
        self.config = config or get_config()
        self.db = db_manager or DatabaseManager.get_instance()
        self.fetcher_manager = fetcher_manager or DataFetcherManager()
        self.repo = BoardObservationRepository(self.db)
        self.board_source = str(
            getattr(self.config, "market_opportunity_board_source", None)
            or DEFAULT_SOURCE
        ).strip() or DEFAULT_SOURCE

    def collect(self, market: str, trade_date: date) -> Dict[str, Any]:
        normalized_market = self._normalize_market(market)
        session_date = self._coerce_date(trade_date)
        if normalized_market != "cn":
            return self._collect_failure(normalized_market, session_date, "market_not_supported")
        existing = self.repo.get_snapshot(
            market=normalized_market,
            trade_date=session_date,
            source=self.board_source,
            taxonomy=DEFAULT_TAXONOMY,
            healthy_only=True,
        )
        if existing is not None:
            return self._snapshot_to_collect_result(existing)
        try:
            if not is_market_open(normalized_market, session_date, strict=True):
                return self._collect_failure(normalized_market, session_date, "non_trading_date")
        except Exception as exc:
            return self._collect_failure(normalized_market, session_date, f"calendar_unavailable:{type(exc).__name__}")
        close_guard = self._collection_close_guard(normalized_market, session_date)
        if close_guard is not None:
            return self._collect_failure(normalized_market, session_date, close_guard)

        try:
            payload = self.fetcher_manager.get_full_board_rankings(
                market=normalized_market,
                board_type=DEFAULT_THEME_TYPE,
                source=self.board_source,
            )
        except Exception as exc:
            return self._persist_collect_result(
                market=normalized_market,
                trade_date=session_date,
                source=self.board_source,
                taxonomy=DEFAULT_TAXONOMY,
                status="failed",
                reason=f"full_board_rankings_error:{type(exc).__name__}",
                coverage={"status": "failed", "reason": str(exc)[:240]},
                themes=[],
                snapshot_hash="",
                fetched_at=None,
            )
        payload_validation_error = self._validate_payload_header(payload, session_date=session_date)
        if payload_validation_error is not None:
            return self._persist_collect_result(
                market=normalized_market,
                trade_date=session_date,
                source=self.board_source,
                taxonomy=DEFAULT_TAXONOMY,
                status="failed",
                reason=payload_validation_error,
                coverage={**dict(payload.get("coverage") or {}), "requested_trade_date": session_date.isoformat()},
                themes=[],
                snapshot_hash=str(payload.get("snapshot_hash") or ""),
                fetched_at=payload.get("fetched_at"),
            )
        if payload.get("status") != "healthy" or not payload.get("themes"):
            reason = payload.get("reason") or (payload.get("coverage") or {}).get("reason") or "empty_full_board_snapshot"
            return self._persist_collect_result(
                market=normalized_market,
                trade_date=session_date,
                source=self.board_source,
                taxonomy=DEFAULT_TAXONOMY,
                status="failed",
                reason=reason,
                coverage=payload.get("coverage", {}),
                themes=[],
                snapshot_hash=str(payload.get("snapshot_hash") or ""),
                fetched_at=payload.get("fetched_at"),
            )
        themes, theme_error = self._normalize_and_validate_themes(payload.get("themes", []), session_date=session_date)
        if theme_error is not None:
            return self._persist_collect_result(
                market=normalized_market,
                trade_date=session_date,
                source=self.board_source,
                taxonomy=DEFAULT_TAXONOMY,
                status="failed",
                reason=theme_error,
                coverage={**dict(payload.get("coverage") or {}), "requested_trade_date": session_date.isoformat()},
                themes=[],
                snapshot_hash=str(payload.get("snapshot_hash") or ""),
                fetched_at=payload.get("fetched_at"),
            )
        snapshot_hash = str(payload.get("snapshot_hash") or "") or self._hash_themes(themes)
        return self._persist_collect_result(
            market=normalized_market,
            trade_date=session_date,
            source=self.board_source,
            taxonomy=DEFAULT_TAXONOMY,
            status="healthy",
            reason=None,
            coverage=payload.get("coverage", {}),
            themes=themes,
            snapshot_hash=snapshot_hash,
            fetched_at=payload.get("fetched_at"),
        )

    def snapshot_health(self, snapshot: Optional[Dict[str, Any]], *, market: str) -> Dict[str, Any]:
        """Recheck persisted evidence without fetching, relabelling or rewriting it."""
        row = snapshot or {}
        reason = None
        if not row or row.get("status") != "healthy":
            reason = row.get("reason") or "board_observation_unavailable"
        elif (market != "cn" or row.get("market") != market
              or row.get("source") != self.board_source or row.get("taxonomy") != DEFAULT_TAXONOMY):
            reason = "snapshot_source_mismatch"
        elif not row.get("snapshot_hash"):
            reason = "snapshot_hash_missing"
        elif (row.get("coverage") or {}).get("status") != "healthy":
            reason = "snapshot_coverage_unverified"
        else:
            try:
                day = self._coerce_date(row["trade_date"])
                now = get_market_now(market)
                if day > now.date():
                    reason = "future_trade_date"
                elif not is_market_open(market, day, strict=True):
                    reason = "non_trading_date"
                elif day == now.date() and now.hour < 15:
                    reason = "market_not_closed"
                else:
                    _, reason = self._normalize_and_validate_themes(row.get("themes"), session_date=day)
                    if reason is None and (row.get("coverage") or {}).get("theme_count") != len(row["themes"]):
                        reason = "snapshot_coverage_incomplete"
                    fetched = row.get("fetched_at")
                    if not isinstance(fetched, datetime):
                        fetched = datetime.fromisoformat(str(fetched))
                    if fetched.tzinfo is None:
                        fetched = fetched.replace(tzinfo=timezone.utc)
                    local_fetched = fetched.astimezone(ZoneInfo("Asia/Shanghai"))
                    if reason is None and (local_fetched.date() != day or local_fetched.hour < 15):
                        reason = "snapshot_not_collected_after_close"
            except (ValueError, TypeError, KeyError):
                reason = "snapshot_identity_invalid"
            except Exception:
                reason = "calendar_unavailable"
        return {
            "healthy": reason is None, "reason": reason,
            "source_snapshot_id": row.get("id"), "source": row.get("source"),
            "market": row.get("market"), "trade_date": str(row.get("trade_date") or ""),
            "counting_basis": "healthy_board_observation_days",
            "policy_version": OBSERVATION_HEALTH_POLICY_VERSION,
        }

    def observation_health_by_date(self, market: str, as_of: Optional[date] = None) -> Dict[str, Dict[str, Any]]:
        rows = self.repo.list_snapshots(
            market=market, end_date=as_of or date.max, source=self.board_source,
            taxonomy=DEFAULT_TAXONOMY, limit=None, healthy_only=False,
        )
        result: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            day = str(row["trade_date"])
            health = self.snapshot_health(row, market=market)
            if day not in result or (health["healthy"] and not result[day]["healthy"]):
                result[day] = health
        return result

    def discovery_themes(self, market: str, as_of: date, limit: int = 15) -> List[Dict[str, Any]]:
        normalized_market = self._normalize_market(market)
        session_date = self._coerce_date(as_of)
        snapshot = self.repo.get_snapshot(
            market=normalized_market,
            trade_date=session_date,
            source=self.board_source,
            taxonomy=DEFAULT_TAXONOMY,
            healthy_only=True,
        )
        if snapshot is None:
            return []

        rows: List[Dict[str, Any]] = []
        for theme in snapshot.get("themes", []):
            normalized = self._normalize_theme(theme)
            if not normalized:
                continue
            evidence = self.persistence_for_theme(
                normalized_market,
                session_date,
                normalized["theme_key"],
                normalized.get("theme_source") or self.board_source,
            )
            score_components = self.discovery_score_components(normalized, evidence)
            score = self._discovery_score(normalized, evidence)
            row = {
                **normalized,
                "rank": normalized.get("rank"),
                "change_pct": normalized.get("change_pct"),
                "observation_evidence": evidence,
                "observation_score": score,
                "observation_score_components": score_components,
            }
            rows.append(row)
        rows.sort(key=lambda item: (-float(item.get("observation_score") or 0.0), int(item.get("rank") or 9999)))
        return rows[: max(1, min(int(limit or 15), 30))]

    def persistence_for_theme(
        self,
        market: str,
        as_of: date,
        theme_key: str,
        provider: str,
    ) -> Dict[str, Any]:
        normalized_market = self._normalize_market(market)
        session_date = self._coerce_date(as_of)
        source = str(provider or self.board_source).strip() or self.board_source
        if source != self.board_source:
            return {
                "days": 0,
                "source": source,
                "taxonomy": DEFAULT_TAXONOMY,
                "theme_key": str(theme_key or ""),
                "window_dates": [],
                "positive_dates": [],
                "required_window_size": 0,
                "available_window_size": 0,
                "return_5d": None,
                "return_20d": None,
                "return_20d_evidence": {"status": "source_mismatch", "required": 20, "available": 0, "missing_dates": []},
                "missing_dates": [],
                "history_state": "source_mismatch",
            }
        window_dates = self._trading_window(normalized_market, session_date, size=5)
        available: List[date] = []
        positive: List[date] = []
        missing: List[date] = []
        returns: List[float] = []

        snapshots = {
            item["trade_date"]: item
            for item in self.repo.list_snapshots(
                market=normalized_market,
                end_date=session_date,
                source=source,
                taxonomy=DEFAULT_TAXONOMY,
                limit=60,
                healthy_only=True,
            )
        }
        current_theme = self._find_theme(
            snapshots.get(session_date, {}).get("themes", []),
            theme_key,
        )
        expected_theme_code = current_theme.get("theme_code") if current_theme else None

        for session_day in window_dates:
            snapshot = snapshots.get(session_day)
            if snapshot is None:
                missing.append(session_day)
                continue
            theme = self._find_theme(
                snapshot.get("themes", []),
                theme_key,
                expected_theme_code=expected_theme_code,
            )
            if theme is None:
                missing.append(session_day)
                continue
            available.append(session_day)
            change_pct = self._safe_float(theme.get("change_pct"))
            if change_pct is not None:
                returns.append(change_pct)
            if self._is_positive_theme(theme):
                positive.append(session_day)

        complete_window = len(available) == len(window_dates)
        complete_returns = len(returns) == len(window_dates)
        if not complete_window:
            history_state = "cold_start"
        elif len(positive) >= 3:
            history_state = "confirmed"
        else:
            history_state = "established_negative"

        medium_window = self._theme_window_evidence(
            market=normalized_market, as_of=session_date, theme_key=theme_key,
            source=source, expected_theme_code=expected_theme_code, window_size=20,
        )
        return {
            "days": len(positive),
            "source": source,
            "taxonomy": DEFAULT_TAXONOMY,
            "theme_key": theme_key,
            "window_dates": [item.isoformat() for item in window_dates],
            "positive_dates": [item.isoformat() for item in positive],
            "required_window_size": len(window_dates),
            "available_window_size": len(available),
            "return_5d": self._compound_return(returns) if complete_window and complete_returns else None,
            "return_20d": medium_window["return_pct"],
            "return_20d_evidence": medium_window,
            "missing_dates": [item.isoformat() for item in missing],
            "history_state": history_state,
        }

    def _persist_collect_result(
        self,
        *,
        market: str,
        trade_date: date,
        source: str,
        taxonomy: str,
        status: str,
        reason: Optional[str],
        coverage: Dict[str, Any],
        themes: List[Dict[str, Any]],
        snapshot_hash: str,
        fetched_at: Any,
    ) -> Dict[str, Any]:
        row = self.repo.upsert_snapshot(
            {
                "snapshot_key": f"{market}:{trade_date.isoformat()}:{source}:{taxonomy}",
                "market": market,
                "trade_date": trade_date,
                "source": source,
                "taxonomy": taxonomy,
                "status": status,
                "reason": reason,
                "coverage": coverage,
                "themes": themes,
                "snapshot_hash": snapshot_hash,
                "fetched_at": self._coerce_datetime(fetched_at) or utc_naive_now(),
            }
        )
        public_status = "ok" if row["status"] == "healthy" else row["status"]
        return {
            "status": public_status,
            "snapshot_id": row["id"],
            "trade_date": row["trade_date"],
            "source": row["source"],
            "coverage": row["coverage"],
            "reason": row["reason"],
            "themes": row["themes"],
        }

    @staticmethod
    def _snapshot_to_collect_result(snapshot: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "status": "ok",
            "snapshot_id": snapshot.get("id"),
            "trade_date": snapshot.get("trade_date"),
            "source": snapshot.get("source"),
            "coverage": snapshot.get("coverage") or {},
            "reason": snapshot.get("reason"),
            "themes": snapshot.get("themes") or [],
        }

    def _collect_failure(self, market: str, trade_date: date, reason: str) -> Dict[str, Any]:
        return {
            "status": "failed",
            "snapshot_id": None,
            "trade_date": trade_date,
            "source": self.board_source,
            "coverage": {"status": "failed", "reason": reason},
            "reason": reason,
            "themes": [],
        }

    @staticmethod
    def _collection_close_guard(market: str, session_date: date) -> Optional[str]:
        market_now = get_market_now(market)
        today = market_now.date()
        if session_date > today:
            return "future_trade_date"
        if session_date < today:
            return "historical_collection_not_supported"
        market_close = datetime.strptime("15:00:00", "%H:%M:%S").time()
        if market_now.time() < market_close:
            return "market_not_closed"
        return None

    @staticmethod
    def _normalize_market(market: str) -> str:
        return str(market or "cn").strip().lower() or "cn"

    @staticmethod
    def _coerce_date(value: Any) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return datetime.fromisoformat(str(value)).date()

    @staticmethod
    def _coerce_datetime(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value.replace(tzinfo=None) if value.tzinfo is not None else value
        if value is None:
            return None
        try:
            parsed = datetime.fromisoformat(str(value))
            return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
        except Exception:
            return None

    @classmethod
    def _normalize_theme(cls, item: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None
        name = str(item.get("theme_name") or item.get("name") or "").strip()
        if not name:
            return None
        source = str(item.get("theme_source") or item.get("source") or DEFAULT_SOURCE).strip() or DEFAULT_SOURCE
        taxonomy = str(item.get("theme_taxonomy") or DEFAULT_TAXONOMY).strip() or DEFAULT_TAXONOMY
        theme_type = str(item.get("theme_type") or item.get("type") or DEFAULT_THEME_TYPE).strip() or DEFAULT_THEME_TYPE
        code = str(item.get("theme_code") or item.get("code") or "").strip()
        key = str(item.get("theme_key") or "").strip()
        if not key:
            key = f"{theme_type}:{name}"
        return {
            "theme_key": key,
            "theme_type": theme_type,
            "theme_name": name,
            "theme_code": code or None,
            "theme_source": source,
            "theme_taxonomy": taxonomy,
            "rank": cls._safe_int(item.get("rank")),
            "change_pct": cls._safe_float(item.get("change_pct")),
            "member_count": cls._safe_int(item.get("member_count")),
            "aggregation": item.get("aggregation"),
            "as_of": item.get("as_of"),
        }

    def _validate_payload_header(self, payload: Any, *, session_date: date) -> Optional[str]:
        if not isinstance(payload, dict):
            return "full_board_rankings_payload_invalid"
        if str(payload.get("source") or "").strip() != self.board_source:
            return "snapshot_source_mismatch"
        if str(payload.get("taxonomy") or "").strip() != DEFAULT_TAXONOMY:
            return "snapshot_taxonomy_mismatch"
        payload_as_of = payload.get("as_of")
        if payload_as_of is None:
            return "snapshot_trade_date_missing"
        if self._coerce_date(payload_as_of) != session_date:
            return "snapshot_trade_date_mismatch"
        return None

    def _normalize_and_validate_themes(self, themes: Any, *, session_date: date) -> tuple[List[Dict[str, Any]], Optional[str]]:
        if not isinstance(themes, list) or not themes:
            return [], "snapshot_themes_empty"
        normalized_rows: List[Dict[str, Any]] = []
        for item in themes:
            normalized = self._normalize_theme(item)
            if normalized is None:
                return [], "theme_identity_invalid"
            if normalized.get("theme_source") != self.board_source:
                return [], "theme_source_mismatch"
            if normalized.get("theme_taxonomy") != DEFAULT_TAXONOMY:
                return [], "theme_taxonomy_mismatch"
            if not normalized.get("theme_code"):
                return [], "theme_code_missing"
            theme_as_of = normalized.get("as_of")
            if theme_as_of is None:
                return [], "theme_as_of_missing"
            if self._coerce_date(theme_as_of) != session_date:
                return [], "theme_as_of_mismatch"
            normalized["as_of"] = session_date.isoformat()
            normalized_rows.append(normalized)
        return normalized_rows, None

    @staticmethod
    def _find_theme(
        themes: Iterable[Any],
        theme_key: str,
        *,
        expected_theme_code: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        target = str(theme_key or "").strip()
        expected_code = str(expected_theme_code or "").strip()
        for item in themes or []:
            normalized = BoardObservationService._normalize_theme(item)
            if normalized and normalized["theme_key"] == target:
                if expected_code and normalized.get("theme_code") != expected_code:
                    continue
                return normalized
        return None

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            result = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(result):
            return None
        return result

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _is_positive_theme(theme: Dict[str, Any]) -> bool:
        change_pct = BoardObservationService._safe_float(theme.get("change_pct"))
        rank = BoardObservationService._safe_int(theme.get("rank"))
        return change_pct is not None and change_pct > 0 and (rank is None or rank <= 15)

    @staticmethod
    def _compound_return(values: Iterable[float]) -> Optional[float]:
        result = 1.0
        count = 0
        for value in values:
            result *= 1.0 + float(value) / 100.0
            count += 1
        if count == 0 or not math.isfinite(result):
            return None
        return round((result - 1.0) * 100.0, 4)

    def _theme_window_evidence(
        self,
        *,
        market: str,
        as_of: date,
        theme_key: str,
        source: str,
        expected_theme_code: Optional[str],
        window_size: int,
    ) -> Dict[str, Any]:
        dates = self._trading_window(market, as_of, size=window_size)
        snapshots = {
            item["trade_date"]: item
            for item in self.repo.list_snapshots(
                market=market,
                end_date=as_of,
                source=source,
                taxonomy=DEFAULT_TAXONOMY,
                limit=80,
                healthy_only=True,
            )
        }
        values: List[float] = []
        missing: List[str] = []
        for session_day in dates:
            theme = self._find_theme(
                snapshots.get(session_day, {}).get("themes", []),
                theme_key,
                expected_theme_code=expected_theme_code,
            )
            if theme is None:
                missing.append(session_day.isoformat())
                continue
            change_pct = self._safe_float(theme.get("change_pct"))
            if change_pct is None:
                missing.append(session_day.isoformat())
                continue
            values.append(change_pct)
        result = self._compound_return(values) if not missing else None
        return {
            "return_pct": result,
            "status": "missing_history" if result is None else (
                "complete_positive" if result > 0 else "complete_negative" if result < 0 else "complete_flat"
            ),
            "required": window_size, "available": len(values),
            "window_dates": [day.isoformat() for day in dates], "missing_dates": missing,
        }

    @staticmethod
    def _trading_window(market: str, as_of: date, *, size: int) -> List[date]:
        result: List[date] = []
        cursor = as_of
        attempts = 0
        while len(result) < size and attempts < 90:
            if is_market_open(market, cursor, strict=True):
                result.append(cursor)
            cursor -= timedelta(days=1)
            attempts += 1
        if len(result) < size:
            raise RuntimeError("trading_calendar_window_incomplete")
        return list(reversed(result))

    @staticmethod
    def _hash_themes(themes: List[Dict[str, Any]]) -> str:
        return hashlib.sha256(
            json.dumps(themes, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def discovery_score_components(theme: Dict[str, Any], evidence: Dict[str, Any]) -> Dict[str, Any]:
        """Bound daily influence while retaining signed, complete 5/20-day evidence.

        Returns are percentage points. Missing windows stay null and contribute
        no score; an observed negative return contributes a penalty. These are
        initial ranking weights, not empirically optimized trading parameters.
        """
        rank = BoardObservationService._safe_int(theme.get("rank")) or 999
        change_pct = BoardObservationService._safe_float(theme.get("change_pct")) or 0.0
        rank_score = max(0.0, 30.0 - min(max(rank - 1, 0), 30))
        daily_score = max(0.0, min(20.0, change_pct * 4.0))
        persistence_score = min(20.0, float(evidence.get("days") or 0) * 5.0)
        return_5d = BoardObservationService._safe_float(evidence.get("return_5d"))
        return_20d = BoardObservationService._safe_float(evidence.get("return_20d"))
        return {
            "policy_version": BOARD_DISCOVERY_POLICY_VERSION,
            "ranking": rank_score,
            "daily_strength": daily_score,
            "persistence": persistence_score,
            "trend_5d": max(-10.0, min(10.0, return_5d)) if return_5d is not None else None,
            "trend_20d": max(-20.0, min(20.0, return_20d)) if return_20d is not None else None,
            "return_5d": return_5d,
            "return_20d": return_20d,
            "missing_windows": [n for n, value in ((5, return_5d), (20, return_20d)) if value is None],
        }

    @classmethod
    def _discovery_score(cls, theme: Dict[str, Any], evidence: Dict[str, Any]) -> float:
        components = cls.discovery_score_components(theme, evidence)
        return round(sum(components[key] or 0.0 for key in (
            "ranking", "daily_strength", "persistence", "trend_5d", "trend_20d",
        )), 4)
