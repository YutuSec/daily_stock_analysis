# -*- coding: utf-8 -*-
"""Backfill daily bars for market-opportunity prospective evaluation."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import func, or_, select

from data_provider.base import DataFetcherManager
from src.core.trading_calendar import get_market_now, is_market_open
from src.services.paper_symbol_identity import stock_daily_variants
from src.storage import DatabaseManager, MarketOpportunityCandidate, StockDaily

logger = logging.getLogger(__name__)

CSI300_FETCH_CODE = "sh000300"
CSI300_SAVE_CODE = "000300.SH"
MAX_FORWARD_SESSIONS = 40
CANDIDATE_LOOKBACK_DAYS = 120


@dataclass(frozen=True)
class _RefreshTarget:
    symbol: str
    save_code: str
    variants: Tuple[str, ...]
    as_of: date
    target_end: date
    candidate_ids: Tuple[int, ...]


class MarketOpportunityHistoryService:
    """Persist future daily bars needed by opportunity evaluation.

    The service is intentionally small and side-effect limited: it only fetches
    market data into ``StockDaily`` and never creates reports, simulated trades,
    or opportunity jobs.
    """

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        fetcher_manager: Optional[DataFetcherManager] = None,
    ) -> None:
        self.db = db_manager or DatabaseManager.get_instance()
        self.fetcher_manager = fetcher_manager or DataFetcherManager()

    def refresh(self, market: str, trade_date: date) -> Dict[str, Any]:
        normalized_market = str(market or "cn").strip().lower() or "cn"
        session_date = self._coerce_date(trade_date)
        guard = self._refresh_guard(normalized_market, session_date)
        if guard is not None:
            return self._summary(
                status="skipped",
                market=normalized_market,
                trade_date=session_date,
                reason=guard,
            )

        targets = self._candidate_targets(normalized_market, session_date)
        summary = self._summary(
            status="ok",
            market=normalized_market,
            trade_date=session_date,
            reason=None,
            candidates_scanned=sum(len(target.candidate_ids) for target in targets),
            symbols_considered=len(targets),
        )
        if not targets:
            summary["reason"] = "no_tracked_candidates"
            return summary

        for target in targets:
            self._refresh_symbol(target, session_date, summary)
        if normalized_market == "cn":
            earliest_as_of = min((target.as_of for target in targets), default=session_date)
            self._refresh_index(earliest_as_of, session_date, summary)

        if summary["failed_symbols"]:
            summary["status"] = "partial" if summary["symbols_refreshed"] or summary["symbols_skipped_current"] else "failed"
        return summary

    def _refresh_symbol(self, target: _RefreshTarget, trade_date: date, summary: Dict[str, Any]) -> None:
        latest = self._latest_daily_date(target.variants)
        has_entry_date = self._has_daily_date(target.variants, target.as_of)
        if latest is not None and latest >= target.target_end and has_entry_date:
            summary["symbols_skipped_current"] += 1
            summary["skipped_symbols"].append(
                {
                    "symbol": target.symbol,
                    "save_code": target.save_code,
                    "latest_date": latest.isoformat(),
                    "target_end": target.target_end.isoformat(),
                    "reason": "stock_daily_current",
                }
            )
            return
        start_date = target.as_of if (not has_entry_date or latest is None or latest < target.as_of) else latest + timedelta(days=1)
        if start_date > target.target_end:
            summary["symbols_skipped_current"] += 1
            summary["skipped_symbols"].append(
                {
                    "symbol": target.symbol,
                    "save_code": target.save_code,
                    "latest_date": latest.isoformat() if latest else None,
                    "target_end": target.target_end.isoformat(),
                    "reason": "no_missing_trading_window",
                }
            )
            return
        try:
            days = max(1, (target.target_end - start_date).days + 10)
            df, source = self.fetcher_manager.get_daily_data(
                target.symbol,
                start_date=start_date.isoformat(),
                end_date=target.target_end.isoformat(),
                days=days,
            )
            saved = self.db.save_daily_data(df, code=target.save_code, data_source=source)
            summary["symbols_refreshed"] += 1
            summary["saved_rows"] += int(saved or 0)
            summary["refreshed_symbols"].append(
                {
                    "symbol": target.symbol,
                    "save_code": target.save_code,
                    "start_date": start_date.isoformat(),
                    "end_date": target.target_end.isoformat(),
                    "source": source,
                    "saved_rows": int(saved or 0),
                    "candidate_ids": list(target.candidate_ids),
                }
            )
        except Exception as exc:
            logger.warning("market opportunity daily refresh failed for %s: %s", target.symbol, exc)
            summary["failed_symbols"].append(
                {
                    "symbol": target.symbol,
                    "save_code": target.save_code,
                    "start_date": start_date.isoformat(),
                    "end_date": target.target_end.isoformat(),
                    "reason": f"{type(exc).__name__}: {str(exc)[:240]}",
                }
            )

    def _refresh_index(self, earliest_as_of: date, trade_date: date, summary: Dict[str, Any]) -> None:
        variants = (CSI300_SAVE_CODE, "SH000300", "SH.000300")
        latest = self._latest_daily_date(variants)
        has_entry_date = self._has_daily_date(variants, earliest_as_of)
        if latest is not None and latest >= trade_date and has_entry_date:
            summary["index"] = {
                "status": "skipped",
                "code": CSI300_SAVE_CODE,
                "latest_date": latest.isoformat(),
                "target_end": trade_date.isoformat(),
                "reason": "stock_daily_current",
            }
            return
        start_date = earliest_as_of if (not has_entry_date or latest is None or latest < earliest_as_of) else latest + timedelta(days=1)
        if start_date > trade_date:
            summary["index"] = {
                "status": "skipped",
                "code": CSI300_SAVE_CODE,
                "latest_date": latest.isoformat() if latest else None,
                "target_end": trade_date.isoformat(),
                "reason": "no_missing_trading_window",
            }
            return
        try:
            fetcher = getattr(self.fetcher_manager, "get_index_daily_data")
            df, source = fetcher(
                CSI300_FETCH_CODE,
                start_date=start_date.isoformat(),
                end_date=trade_date.isoformat(),
                days=max(1, (trade_date - start_date).days + 10),
            )
            saved = self.db.save_daily_data(df, code=CSI300_SAVE_CODE, data_source=source)
            summary["saved_rows"] += int(saved or 0)
            summary["index"] = {
                "status": "refreshed",
                "code": CSI300_SAVE_CODE,
                "fetch_code": CSI300_FETCH_CODE,
                "start_date": start_date.isoformat(),
                "end_date": trade_date.isoformat(),
                "source": source,
                "saved_rows": int(saved or 0),
            }
        except Exception as exc:
            logger.warning("market opportunity index refresh failed: %s", exc)
            summary["index"] = {
                "status": "failed",
                "code": CSI300_SAVE_CODE,
                "fetch_code": CSI300_FETCH_CODE,
                "start_date": start_date.isoformat(),
                "end_date": trade_date.isoformat(),
                "reason": f"{type(exc).__name__}: {str(exc)[:240]}",
            }

    def _candidate_targets(self, market: str, trade_date: date) -> List[_RefreshTarget]:
        lower_bound = trade_date - timedelta(days=CANDIDATE_LOOKBACK_DAYS)
        grouped: Dict[Tuple[str, date], Dict[str, Any]] = {}
        with self.db.session_scope() as session:
            rows = session.execute(
                select(MarketOpportunityCandidate)
                .where(
                    MarketOpportunityCandidate.market == market,
                    MarketOpportunityCandidate.as_of >= lower_bound,
                    MarketOpportunityCandidate.as_of <= trade_date,
                )
                .order_by(MarketOpportunityCandidate.as_of.asc(), MarketOpportunityCandidate.id.asc())
            ).scalars().all()
            for candidate in rows:
                if not self._is_tracked_candidate(candidate):
                    continue
                as_of = self._coerce_date(candidate.as_of)
                target_end = self._target_end_date(market, as_of, trade_date)
                if target_end is None:
                    continue
                symbol = str(candidate.symbol or candidate.stock_code or "").strip().upper()
                variants = tuple(stock_daily_variants(market, symbol))
                if not variants:
                    variants = tuple(stock_daily_variants(market, str(candidate.stock_code or "").strip().upper()))
                if not variants:
                    continue
                save_code = variants[0]
                key = symbol or save_code
                bucket = grouped.setdefault(
                    key,
                    {
                        "symbol": symbol or save_code,
                        "save_code": save_code,
                        "variants": variants,
                        "as_of": as_of,
                        "target_end": target_end,
                        "candidate_ids": [],
                    },
                )
                if target_end > bucket["target_end"]:
                    bucket["target_end"] = target_end
                if as_of < bucket["as_of"]:
                    bucket["as_of"] = as_of
                bucket["candidate_ids"].append(int(candidate.id))
        return [
            _RefreshTarget(
                symbol=item["symbol"],
                save_code=item["save_code"],
                variants=tuple(item["variants"]),
                as_of=item["as_of"],
                target_end=item["target_end"],
                candidate_ids=tuple(item["candidate_ids"]),
            )
            for item in grouped.values()
        ]

    @staticmethod
    def _is_tracked_candidate(candidate: MarketOpportunityCandidate) -> bool:
        if str(candidate.status or "").strip().lower() in {"selected", "observing"}:
            return True
        try:
            score_components = json.loads(candidate.score_components_json or "{}")
        except Exception:
            score_components = {}
        comparison = score_components.get("strategy_comparison") if isinstance(score_components, dict) else None
        return bool(isinstance(comparison, dict) and comparison.get("legacy_selected"))

    @staticmethod
    def _target_end_date(market: str, as_of: date, trade_date: date) -> Optional[date]:
        if trade_date < as_of:
            return None
        dates = MarketOpportunityHistoryService._forward_sessions(market, as_of, MAX_FORWARD_SESSIONS)
        if not dates:
            return as_of if trade_date == as_of else None
        due_dates = [item for item in dates if item <= trade_date]
        if not due_dates:
            return as_of
        return due_dates[-1]

    @staticmethod
    def _forward_sessions(market: str, as_of: date, count: int) -> List[date]:
        result: List[date] = []
        cursor = as_of
        try:
            for _ in range(count * 4 + 30):
                cursor += timedelta(days=1)
                if is_market_open(market, cursor, strict=True):
                    result.append(cursor)
                    if len(result) >= count:
                        break
        except Exception:
            return []
        return result

    def _latest_daily_date(self, variants: Sequence[str]) -> Optional[date]:
        if not variants:
            return None
        with self.db.session_scope() as session:
            return session.execute(
                select(func.max(StockDaily.date)).where(StockDaily.code.in_(list(variants)))
            ).scalar_one_or_none()

    def _has_daily_date(self, variants: Sequence[str], target_date: date) -> bool:
        if not variants:
            return False
        with self.db.session_scope() as session:
            return session.execute(
                select(StockDaily.id)
                .where(StockDaily.code.in_(list(variants)), StockDaily.date == target_date)
                .limit(1)
            ).scalar_one_or_none() is not None

    @staticmethod
    def _refresh_guard(market: str, trade_date: date) -> Optional[str]:
        if market != "cn":
            return "market_not_supported"
        try:
            if not is_market_open(market, trade_date, strict=True):
                return "non_trading_date"
        except Exception as exc:
            return f"calendar_unavailable:{type(exc).__name__}"
        market_now = get_market_now(market)
        today = market_now.date()
        if trade_date > today:
            return "future_trade_date"
        if trade_date < today:
            return "stale_trade_date"
        market_close = datetime.strptime("15:00:00", "%H:%M:%S").time()
        if market_now.time() < market_close:
            return "market_not_closed"
        return None

    @staticmethod
    def _summary(
        *,
        status: str,
        market: str,
        trade_date: date,
        reason: Optional[str],
        candidates_scanned: int = 0,
        symbols_considered: int = 0,
    ) -> Dict[str, Any]:
        return {
            "status": status,
            "market": market,
            "trade_date": trade_date.isoformat(),
            "reason": reason,
            "lookback_days": CANDIDATE_LOOKBACK_DAYS,
            "forward_sessions": MAX_FORWARD_SESSIONS,
            "candidates_scanned": candidates_scanned,
            "symbols_considered": symbols_considered,
            "symbols_refreshed": 0,
            "symbols_skipped_current": 0,
            "saved_rows": 0,
            "refreshed_symbols": [],
            "skipped_symbols": [],
            "failed_symbols": [],
            "index": {"status": "not_attempted", "code": CSI300_SAVE_CODE},
        }

    @staticmethod
    def _coerce_date(value: Any) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        return datetime.fromisoformat(str(value)).date()
