# -*- coding: utf-8 -*-
"""Prospective evaluation for persisted market opportunity snapshots."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import desc, select

from src.services.paper_symbol_identity import stock_daily_variants
from src.storage import (
    DatabaseManager,
    MarketOpportunityAnalysisJob,
    MarketOpportunityCandidate,
    MarketOpportunityRun,
    PaperPosition,
    PaperTradeEvent,
    StockDaily,
    is_market_opportunity_research_job_key,
)

EVALUATION_WINDOWS = (1, 3, 5, 10, 20, 40)


def _safe_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


class MarketOpportunityEvaluationService:
    """Compute on-demand no-lookahead opportunity evaluation metrics."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        self.db = db_manager or DatabaseManager.get_instance()

    def evaluate(
        self,
        *,
        run_id: Optional[int] = None,
        market: Optional[str] = None,
        windows: Iterable[int] = EVALUATION_WINDOWS,
    ) -> Dict[str, Any]:
        requested_windows = tuple(sorted({int(item) for item in windows if int(item) > 0}))
        if not requested_windows:
            requested_windows = EVALUATION_WINDOWS
        max_window = max(requested_windows)
        with self.db.session_scope() as session:
            query = (
                select(MarketOpportunityCandidate, MarketOpportunityRun)
                .join(MarketOpportunityRun, MarketOpportunityRun.id == MarketOpportunityCandidate.run_id)
                .order_by(
                    MarketOpportunityRun.trade_date.asc(),
                    MarketOpportunityCandidate.rank.asc().nullslast(),
                    desc(MarketOpportunityCandidate.score),
                    MarketOpportunityCandidate.id.asc(),
                )
            )
            if run_id is not None:
                query = query.where(MarketOpportunityCandidate.run_id == int(run_id))
            if market:
                query = query.where(MarketOpportunityCandidate.market == str(market).strip().lower())
            rows = session.execute(query).all()
            paper_by_candidate = self._paper_execution_by_candidate(
                session,
                candidate_ids=[int(candidate.id) for candidate, _run in rows],
            )
            candidates: List[Dict[str, Any]] = []
            for candidate, run in rows:
                score_components = self._json_dict(candidate.score_components_json)
                comparison = score_components.get("strategy_comparison") if isinstance(score_components.get("strategy_comparison"), dict) else {}
                current_selected = bool(comparison.get("current_selected", candidate.status == "selected"))
                legacy_selected = bool(comparison.get("legacy_selected"))
                if candidate.status not in {"selected", "observing"} and not legacy_selected:
                    continue
                identity = self._stock_daily_identity(candidate)
                bars_payload = self._future_bars(session, identity["variants"], candidate.as_of, max_window, market=candidate.market)
                entry_payload = self._entry_price(session, candidate, identity["variants"])
                executable_entry = self._next_session_entry_payload(bars_payload.get("bars", []), candidate.market, candidate.as_of)
                benchmark = self._benchmark_payload(session, candidate.market, candidate.as_of, max_window)
                perf = self._candidate_performance(
                    entry_price=entry_payload.get("price"),
                    bars=bars_payload.get("bars", []),
                    identity=identity,
                    bar_status=bars_payload.get("status", "ok"),
                    benchmark=benchmark,
                    windows=requested_windows,
                    return_basis="signal_close_to_future_close_non_executable",
                    expected_dates=self._session_dates(candidate.market, candidate.as_of, max_window),
                )
                executable_perf = self._candidate_performance(
                    entry_price=executable_entry.get("price"),
                    bars=bars_payload.get("bars", []),
                    identity=identity,
                    bar_status=bars_payload.get("status", "ok"),
                    benchmark=benchmark,
                    windows=requested_windows,
                    entry_date=executable_entry.get("entry_date"),
                    t_plus_one=str(candidate.market or "").lower() == "cn",
                    return_basis="next_session_open_daily_proxy_unconfirmed",
                    expected_dates=self._session_dates(candidate.market, candidate.as_of, max_window),
                )
                candidates.append(
                    {
                        "candidate_id": int(candidate.id),
                        "run_id": int(candidate.run_id),
                        "trade_date": run.trade_date.isoformat() if run.trade_date else None,
                        "market": candidate.market,
                        "stock_code": candidate.stock_code,
                        "symbol": candidate.symbol,
                        "stock_name": candidate.stock_name,
                        "primary_theme_key": candidate.primary_theme_key,
                        "rank": candidate.rank,
                        "score": candidate.score,
                        "candidate_status": candidate.status,
                        "strategy_comparison": comparison,
                        "comparison_groups": {
                            "current": current_selected or candidate.status in {"selected", "observing"},
                            "current_selected": candidate.status == "selected" or current_selected,
                            "legacy_selected": legacy_selected,
                        },
                        "as_of": candidate.as_of.isoformat() if candidate.as_of else None,
                        "status": perf["status"],
                        "return_basis": "signal_close_to_future_close_non_executable",
                        "entry_price": entry_payload.get("price"),
                        "entry_status": entry_payload.get("status"),
                        "executable_entry": executable_entry,
                        "stock_daily_variants": identity["variants"],
                        "stock_daily_match_code": bars_payload.get("match_code") or entry_payload.get("match_code"),
                        "windows": perf["windows"],
                        "executable_windows": executable_perf["windows"],
                        "paper_execution": paper_by_candidate.get(int(candidate.id), self._empty_paper_execution()),
                    }
                )
            jobs = self._job_execution_summary(session, run_id=run_id, market=market)
        # Multiple reruns of the same signal day must not multiply evidence.
        unique = {}
        for item in candidates:
            key = (item["market"], item["as_of"], item["symbol"])
            previous = unique.get(key)
            if previous is None or (item["run_id"], item["candidate_id"]) > (previous["run_id"], previous["candidate_id"]):
                unique[key] = item
        candidates = list(unique.values())
        groups = {name: self._summarize_candidates(
            [item for item in candidates if item["comparison_groups"].get(name)], requested_windows)
            for name in ("current", "current_selected", "legacy_selected")}
        return {
            "strategy_comparison_summary": {"basis": "same_observed_constituent_sample", "groups": groups,
                                            "deduplication": "market_as_of_symbol_latest_run"},
            "windows": list(requested_windows),
            "candidate_count": len(candidates),
            "candidate_metrics": candidates,
            "summary": self._summarize_candidates(candidates, requested_windows),
            "theme_summary": self._summarize_by(candidates, "primary_theme_key", requested_windows),
            "model_execution_summary": jobs,
        }

    @staticmethod
    def _session_dates(market: str, as_of: Optional[date], count: int) -> List[date]:
        if as_of is None:
            return []
        from src.core.trading_calendar import is_market_open
        result = []
        day = as_of
        try:
            for _ in range(count * 4 + 30):
                day += timedelta(days=1)
                if is_market_open(market, day, strict=True):
                    result.append(day)
                    if len(result) == count:
                        break
        except Exception:
            return []
        return result

    @staticmethod
    def _stock_daily_identity(candidate: MarketOpportunityCandidate) -> Dict[str, Any]:
        variants = stock_daily_variants(str(candidate.market or ""), str(candidate.symbol or candidate.stock_code or ""))
        if not variants:
            variants = stock_daily_variants(str(candidate.market or ""), str(candidate.stock_code or ""))
        return {
            "status": "ok" if variants else "invalid_identity",
            "variants": list(dict.fromkeys(variants)),
        }

    def _future_bars(self, session: Any, variants: List[str], as_of: Optional[date], max_window: int, *, market: str = "cn") -> Dict[str, Any]:
        if not variants:
            return {"status": "invalid_identity", "bars": []}
        if as_of is None:
            return {"status": "missing_as_of", "bars": []}
        from src.core.trading_calendar import get_effective_trading_date, is_market_open
        try:
            completed_date = get_effective_trading_date(market)
            if not is_market_open(market, completed_date, strict=True):
                return {"status": "calendar_unavailable", "bars": []}
        except Exception:
            return {"status": "calendar_unavailable", "bars": []}
        rows = list(
            session.execute(
                select(StockDaily)
                .where(
                    StockDaily.code.in_(variants),
                    StockDaily.date > as_of,
                    StockDaily.date <= completed_date,
                )
                .order_by(StockDaily.date.asc())
                .limit(max_window * max(1, len(variants)))
            ).scalars()
        )
        return self._reconcile_daily_rows(rows, variants=variants, max_window=max_window)

    def _entry_price(self, session: Any, candidate: MarketOpportunityCandidate, variants: List[str]) -> Dict[str, Any]:
        snapshot = {}
        try:
            import json

            snapshot = json.loads(candidate.quote_snapshot_json or "{}")
        except Exception:
            snapshot = {}
        quoted = _safe_float(snapshot.get("close") if isinstance(snapshot, dict) else None)
        if quoted is not None and quoted > 0:
            return {"status": "snapshot_close", "price": quoted, "match_code": None}
        if candidate.as_of is None:
            return {"status": "missing_as_of", "price": None, "match_code": None}
        if not variants:
            return {"status": "invalid_identity", "price": None, "match_code": None}
        rows = list(
            session.execute(
                select(StockDaily)
                .where(StockDaily.code.in_(variants), StockDaily.date == candidate.as_of)
                .order_by(StockDaily.id.asc())
            ).scalars()
        )
        reconciled = self._reconcile_daily_rows(rows, variants=variants, max_window=1)
        if reconciled.get("status") != "ok" or not reconciled.get("bars"):
            return {"status": reconciled.get("status", "entry_price_missing"), "price": None, "match_code": reconciled.get("match_code")}
        row = reconciled["bars"][0].close
        parsed = _safe_float(row)
        return {
            "status": "stock_daily_close" if parsed is not None and parsed > 0 else "entry_price_missing",
            "price": parsed if parsed is not None and parsed > 0 else None,
            "match_code": reconciled.get("match_code"),
        }

    @staticmethod
    def _next_session_entry_payload(bars: List[StockDaily], market: str, as_of: Optional[date] = None) -> Dict[str, Any]:
        sessions = MarketOpportunityEvaluationService._session_dates(market, as_of, 2)
        if as_of is not None and (not sessions or not bars or bars[0].date != sessions[0]):
            bars = []
        if not bars:
            return {
                "status": "insufficient_future_bars",
                "price": None,
                "entry_date": None,
                "earliest_sell_date": None,
                "tradability": {
                    "fill_confirmed": False,
                    "confirmation_source": "daily_bar_only",
                    "limit_price_confirmed": False,
                    "minute_data_confirmed": False,
                    "reason": "missing_next_session_bar",
                },
            }
        entry_bar = bars[0]
        entry_open = _safe_float(entry_bar.open)
        entry_date = entry_bar.date
        earliest_sell_date = None
        if str(market or "").strip().lower() == "cn":
            earliest_sell_date = sessions[1] if len(sessions) > 1 else None
        else:
            earliest_sell_date = entry_date
        volume = _safe_float(getattr(entry_bar, "volume", None))
        amount = _safe_float(getattr(entry_bar, "amount", None))
        suspended = (volume is not None and volume <= 0) or (amount is not None and amount <= 0)
        status = "daily_open_proxy_unconfirmed" if entry_open is not None and entry_open > 0 and not suspended else "untradable_daily_bar"
        return {
            "status": status,
            "price": entry_open if entry_open is not None and entry_open > 0 and not suspended else None,
            "entry_date": entry_date.isoformat() if entry_date else None,
            "earliest_sell_date": earliest_sell_date.isoformat() if earliest_sell_date else None,
            "t_plus_one_applies": str(market or "").strip().lower() == "cn",
            "tradability": {
                "fill_confirmed": False,
                "confirmation_source": "daily_bar_only",
                "limit_price_confirmed": False,
                "minute_data_confirmed": False,
                "suspension_checked_from_daily_volume": volume is not None or amount is not None,
                "limit_unfillable_status": "cannot_confirm_without_limit_price_or_minute_data",
                "reason": "daily_open_is_proxy_not_real_fill" if status == "daily_open_proxy_unconfirmed" else status,
            },
            "cost_model": {
                "status": "paper_results_reported_separately",
                "slippage_confirmed": False,
                "fees_confirmed": False,
            },
        }

    def _benchmark_payload(self, session: Any, market: str, as_of: Optional[date], max_window: int) -> Dict[str, Any]:
        if str(market or "").strip().lower() != "cn":
            return {"status": "unsupported_market", "entry_price": None, "bars": []}
        if as_of is None:
            return {"status": "missing_as_of", "entry_price": None, "bars": []}
        variants = ["000300.SH", "SH000300", "SH.000300"]
        entry_rows = list(
            session.execute(
                select(StockDaily)
                .where(StockDaily.code.in_(variants), StockDaily.date == as_of)
                .order_by(StockDaily.id.asc())
            ).scalars()
        )
        entry = self._reconcile_daily_rows(entry_rows, variants=variants, max_window=1)
        if entry.get("status") != "ok" or not entry.get("bars"):
            return {"status": "benchmark_entry_missing", "entry_price": None, "bars": []}
        entry_price = _safe_float(entry["bars"][0].close)
        if entry_price is None or entry_price <= 0:
            return {"status": "benchmark_entry_missing", "entry_price": None, "bars": []}
        future_rows = list(
            session.execute(
                select(StockDaily)
                .where(StockDaily.code.in_(variants), StockDaily.date > as_of)
                .order_by(StockDaily.date.asc())
                .limit(max_window * len(variants))
            ).scalars()
        )
        future = self._reconcile_daily_rows(future_rows, variants=variants, max_window=max_window)
        return {
            "status": future.get("status", "ok") if future.get("bars") else "benchmark_future_missing",
            "entry_price": entry_price,
            "bars": future.get("bars", []),
            "match_code": future.get("match_code") or entry.get("match_code"),
        }

    @staticmethod
    def _reconcile_daily_rows(rows: List[StockDaily], *, variants: List[str], max_window: int) -> Dict[str, Any]:
        if not rows:
            return {"status": "insufficient_data", "bars": [], "match_code": None}
        matched_codes = {str(row.code) for row in rows}
        match_code = next(iter(matched_codes))
        by_date: Dict[date, List[StockDaily]] = defaultdict(list)
        for row in rows:
            if row.date is not None:
                by_date[row.date].append(row)
        reconciled: List[StockDaily] = []
        for bar_date in sorted(by_date)[:max_window]:
            daily_rows = by_date[bar_date]
            conflict = MarketOpportunityEvaluationService._daily_rows_conflict(daily_rows)
            if conflict:
                return {
                    "status": "identity_ambiguous",
                    "bars": [],
                    "match_code": None,
                    "matched_codes": sorted({str(row.code) for row in daily_rows}),
                    "conflict_date": bar_date.isoformat(),
                }
            daily_rows.sort(key=lambda item: variants.index(item.code) if item.code in variants else len(variants))
            reconciled.append(daily_rows[0])
        return {"status": "ok", "bars": reconciled, "match_code": match_code}

    @staticmethod
    def _daily_rows_conflict(rows: List[StockDaily]) -> bool:
        if len({str(row.code) for row in rows}) <= 1:
            return False
        fingerprints = set()
        for row in rows:
            fingerprints.add(tuple(
                None if _safe_float(getattr(row, field)) is None else round(float(getattr(row, field)), 6)
                for field in ("open", "high", "low", "close", "volume", "amount")
            ))
        return len(fingerprints) > 1

    @staticmethod
    def _candidate_performance(
        *,
        entry_price: Optional[float],
        bars: List[StockDaily],
        identity: Dict[str, Any],
        bar_status: str,
        benchmark: Dict[str, Any],
        windows: Tuple[int, ...],
        entry_date: Optional[Any] = None,
        return_basis: str = "signal_close_to_future_close_non_executable",
        expected_dates: Optional[List[date]] = None,
        t_plus_one: bool = False,
    ) -> Dict[str, Any]:
        if identity.get("status") != "ok":
            return {
                "status": "insufficient_data",
                "windows": {str(window): {"status": "insufficient_data", "reason": identity.get("status"), "return_basis": return_basis} for window in windows},
            }
        if bar_status not in {"ok", "insufficient_data"}:
            return {
                "status": "insufficient_data",
                "windows": {str(window): {"status": "insufficient_data", "reason": bar_status, "return_basis": return_basis} for window in windows},
            }
        if entry_price is None or entry_price <= 0:
            return {"status": "insufficient_data", "windows": {str(window): {"status": "missing_entry_price", "return_basis": return_basis} for window in windows}}
        usable_bars = list(bars)
        if entry_date is not None:
            parsed_entry = entry_date
            if isinstance(parsed_entry, str):
                try:
                    parsed_entry = date.fromisoformat(parsed_entry)
                except ValueError:
                    parsed_entry = None
            if parsed_entry is not None:
                usable_bars = [row for row in usable_bars if row.date is not None and row.date >= parsed_entry]
        output: Dict[str, Any] = {}
        for window in windows:
            if expected_dates is not None:
                wanted = expected_dates[:window]
                by_date = {row.date: row for row in usable_bars}
                missing = [day.isoformat() for day in wanted if day not in by_date]
                if len(wanted) < window or missing:
                    output[str(window)] = {
                        "status": "insufficient_data", "reason": "missing_trading_sessions",
                        "missing_dates": missing, "required_bars": window,
                        "available_bars": sum(day in by_date for day in wanted), "return_basis": return_basis,
                    }
                    continue
                usable_bars = [by_date[day] for day in expected_dates if day in by_date]
            if entry_date is not None and t_plus_one and window == 1:
                output[str(window)] = {"status": "t_plus_one_blocked", "return_pct": None, "return_basis": return_basis}
                continue
            if len(usable_bars) < window:
                output[str(window)] = {
                    "status": "insufficient_data",
                    "available_bars": len(usable_bars),
                    "required_bars": window,
                    "return_basis": return_basis,
                }
                continue
            subset = usable_bars[:window]
            close = _safe_float(subset[-1].close)
            highs = [_safe_float(row.high) for row in subset]
            lows = [_safe_float(row.low) for row in subset]
            if close is None or close <= 0:
                output[str(window)] = {"status": "insufficient_data", "reason": "future_close_missing"}
                continue
            max_high = max([item for item in highs if item is not None] or [close])
            min_low = min([item for item in lows if item is not None] or [close])
            return_pct = round((close / entry_price - 1.0) * 100.0, 4)
            benchmark_by_date = {row.date: row for row in benchmark.get("bars", [])}
            aligned_benchmark = dict(benchmark)
            aligned_benchmark["bars"] = [benchmark_by_date[row.date] for row in subset if row.date in benchmark_by_date]
            if entry_date is not None:
                first_benchmark = benchmark_by_date.get(subset[0].date)
                aligned_benchmark["entry_price"] = _safe_float(first_benchmark.open) if first_benchmark is not None else None
            benchmark_metric = MarketOpportunityEvaluationService._benchmark_window_metric(aligned_benchmark, window)
            output[str(window)] = {
                "status": "ok",
                "end_date": subset[-1].date.isoformat() if subset[-1].date else None,
                "return_basis": return_basis,
                "return_pct": return_pct,
                "mfe_pct": round((max_high / entry_price - 1.0) * 100.0, 4),
                "mae_pct": round((min_low / entry_price - 1.0) * 100.0, 4),
                "benchmark_status": benchmark_metric.get("status"),
                "benchmark_return_pct": benchmark_metric.get("return_pct"),
                "excess_return_pct": (
                    round(return_pct - float(benchmark_metric["return_pct"]), 4)
                    if benchmark_metric.get("return_pct") is not None
                    else None
                ),
            }
        status = "ok" if any(item.get("status") == "ok" for item in output.values()) else "insufficient_data"
        return {"status": status, "windows": output}

    def _paper_execution_by_candidate(self, session: Any, *, candidate_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        if not candidate_ids:
            return {}
        rows = session.execute(
            select(MarketOpportunityAnalysisJob)
            .where(MarketOpportunityAnalysisJob.candidate_id.in_(candidate_ids))
        ).scalars().all()
        output: Dict[int, Dict[str, Any]] = defaultdict(self._empty_paper_execution)
        report_ids_by_candidate: Dict[int, List[int]] = defaultdict(list)
        plan_ids_by_candidate: Dict[int, List[int]] = defaultdict(list)
        for job in rows:
            bucket = output[int(job.candidate_id)]
            # 研究任务不进入主账户链路：单独计数，不计入模拟执行的任务/报告/计划口径。
            if is_market_opportunity_research_job_key(job.job_key):
                bucket["research_jobs"] += 1
                if job.analysis_history_id:
                    bucket["research_reports"] += 1
                continue
            bucket["jobs"] += 1
            if job.analysis_history_id:
                bucket["reports"] += 1
                report_ids_by_candidate[int(job.candidate_id)].append(int(job.analysis_history_id))
            if job.paper_plan_id:
                bucket["plans"] += 1
                plan_ids_by_candidate[int(job.candidate_id)].append(int(job.paper_plan_id))
        all_report_ids = sorted({item for values in report_ids_by_candidate.values() for item in values})
        all_plan_ids = sorted({item for values in plan_ids_by_candidate.values() for item in values})
        positions_by_candidate: Dict[int, List[PaperPosition]] = defaultdict(list)
        if all_report_ids or all_plan_ids:
            position_query = select(PaperPosition)
            clauses = []
            if all_report_ids:
                clauses.append(PaperPosition.source_report_id.in_(all_report_ids))
            if all_plan_ids:
                clauses.append(PaperPosition.plan_id.in_(all_plan_ids))
            if len(clauses) == 1:
                position_query = position_query.where(clauses[0])
            else:
                from sqlalchemy import or_
                position_query = position_query.where(or_(*clauses))
            for position in session.execute(position_query).scalars().all():
                for candidate_id in set(report_ids_by_candidate) | set(plan_ids_by_candidate):
                    report_ids = report_ids_by_candidate.get(candidate_id, [])
                    if position.source_report_id in report_ids or position.plan_id in plan_ids_by_candidate.get(candidate_id, []):
                        positions_by_candidate[candidate_id].append(position)
        position_ids = sorted({int(position.id) for values in positions_by_candidate.values() for position in values if position.id is not None})
        events_by_position: Dict[int, List[PaperTradeEvent]] = defaultdict(list)
        if position_ids:
            for event in session.execute(
                select(PaperTradeEvent).where(PaperTradeEvent.position_id.in_(position_ids))
            ).scalars().all():
                if event.position_id is not None:
                    events_by_position[int(event.position_id)].append(event)
        for candidate_id, positions in positions_by_candidate.items():
            bucket = output[candidate_id]
            seen_positions = {int(position.id) for position in positions if position.id is not None}
            bucket["positions"] = len(seen_positions)
            for position in positions:
                if position.status == "open":
                    bucket["open_positions"] += 1
                if position.status == "closed" or float(position.realized_quantity or 0) > 0:
                    bucket["closed_positions"] += 1
                events = events_by_position.get(int(position.id), [])
                event_gross = 0.0
                event_net = 0.0
                event_has_realized_pnl = False
                for event in events:
                    bucket["trade_events"] += 1
                    gross_value = _safe_float(getattr(event, "gross_realized_pnl", None))
                    net_value = _safe_float(getattr(event, "net_realized_pnl", None))
                    if gross_value is not None:
                        event_gross += gross_value
                        event_has_realized_pnl = True
                    if net_value is not None:
                        event_net += net_value
                        event_has_realized_pnl = True
                    bucket["total_fees"] += float(getattr(event, "fees", 0.0) or 0.0)
                    bucket["total_commission"] += float(getattr(event, "commission", 0.0) or 0.0)
                    bucket["total_tax"] += float(getattr(event, "tax", 0.0) or getattr(event, "sell_tax", 0.0) or 0.0)
                    bucket["total_slippage_amount"] += float(getattr(event, "slippage_amount", 0.0) or 0.0)
                if event_has_realized_pnl:
                    bucket["gross_realized_pnl"] += event_gross
                    bucket["net_realized_pnl"] += event_net
                else:
                    # The paper position ledger stores realized_pnl after exit
                    # fees.  Do not charge those fees a second time here.
                    net = float(position.realized_pnl if position.realized_pnl is not None else position.simulated_pnl or 0.0)
                    bucket["net_realized_pnl"] += net
                    bucket["gross_realized_pnl"] += net + float(position.realized_fees or 0.0)
            self._round_paper_execution(bucket)
        return dict(output)

    @staticmethod
    def _empty_paper_execution() -> Dict[str, Any]:
        return {
            "jobs": 0,
            "reports": 0,
            "plans": 0,
            "positions": 0,
            "open_positions": 0,
            "closed_positions": 0,
            "trade_events": 0,
            "gross_realized_pnl": 0.0,
            "net_realized_pnl": 0.0,
            "total_fees": 0.0,
            "total_commission": 0.0,
            "total_tax": 0.0,
            "total_slippage_amount": 0.0,
            "research_jobs": 0,
            "research_reports": 0,
        }

    @staticmethod
    def _round_paper_execution(bucket: Dict[str, Any]) -> None:
        for key in ("gross_realized_pnl", "net_realized_pnl", "total_fees", "total_commission", "total_tax", "total_slippage_amount"):
            bucket[key] = round(float(bucket.get(key) or 0.0), 6)

    @staticmethod
    def _json_dict(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        if not value:
            return {}
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _benchmark_window_metric(benchmark: Dict[str, Any], window: int) -> Dict[str, Any]:
        entry_price = _safe_float(benchmark.get("entry_price"))
        bars = benchmark.get("bars") or []
        if entry_price is None or entry_price <= 0:
            return {"status": benchmark.get("status") or "benchmark_unavailable", "return_pct": None}
        if len(bars) < window:
            return {"status": "benchmark_insufficient_data", "return_pct": None}
        close = _safe_float(bars[window - 1].close)
        if close is None or close <= 0:
            return {"status": "benchmark_future_close_missing", "return_pct": None}
        return {"status": "ok", "return_pct": round((close / entry_price - 1.0) * 100.0, 4)}

    def _job_execution_summary(self, session: Any, *, run_id: Optional[int], market: Optional[str]) -> Dict[str, Any]:
        query = select(MarketOpportunityAnalysisJob, MarketOpportunityCandidate).join(
            MarketOpportunityCandidate,
            MarketOpportunityCandidate.id == MarketOpportunityAnalysisJob.candidate_id,
        )
        if run_id is not None:
            query = query.where(MarketOpportunityAnalysisJob.run_id == int(run_id))
        if market:
            query = query.where(MarketOpportunityCandidate.market == str(market).strip().lower())
        by_model: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "jobs": 0,
            "reports": 0,
            "signals": 0,
            "plans": 0,
            "positions": 0,
            "closed_positions": 0,
            "realized_pnl": 0.0,
            "research_jobs": 0,
            "research_reports": 0,
        })
        report_ids = []
        for job, _candidate in session.execute(query).all():
            model = str(job.model or "unknown")
            bucket = by_model[model]
            # 研究任务单独计数，报告率/计划率仍只按主账户候选任务计算。
            if is_market_opportunity_research_job_key(job.job_key):
                bucket["research_jobs"] += 1
                if job.analysis_history_id:
                    bucket["research_reports"] += 1
                continue
            bucket["jobs"] += 1
            if job.analysis_history_id:
                bucket["reports"] += 1
                report_ids.append(int(job.analysis_history_id))
            if job.decision_signal_id:
                bucket["signals"] += 1
            if job.paper_plan_id:
                bucket["plans"] += 1
        if report_ids:
            positions = session.execute(
                select(PaperPosition)
                .where(PaperPosition.source_report_id.in_(report_ids))
            ).scalars().all()
            for position in positions:
                bucket = by_model[str(position.source_model or "unknown")]
                bucket["positions"] += 1
                if position.status == "closed" or float(position.realized_quantity or 0) > 0:
                    bucket["closed_positions"] += 1
                    bucket["realized_pnl"] += float(position.realized_pnl or position.simulated_pnl or 0.0)
        return {model: self._with_rates(values) for model, values in sorted(by_model.items())}

    @staticmethod
    def _with_rates(values: Dict[str, Any]) -> Dict[str, Any]:
        jobs = max(1, int(values.get("jobs") or 0))
        reports = int(values.get("reports") or 0)
        plans = int(values.get("plans") or 0)
        return {
            **values,
            "report_rate": round(reports / jobs, 4),
            "plan_rate": round(plans / jobs, 4),
            "realized_pnl": round(float(values.get("realized_pnl") or 0.0), 4),
        }

    @staticmethod
    def _summarize_candidates(candidates: List[Dict[str, Any]], windows: Tuple[int, ...]) -> Dict[str, Any]:
        return MarketOpportunityEvaluationService._summarize_by(candidates, "all", windows).get("all", {})

    @staticmethod
    def _summarize_by(candidates: List[Dict[str, Any]], key: str, windows: Tuple[int, ...]) -> Dict[str, Any]:
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in candidates:
            group_key = "all" if key == "all" else str(item.get(key) or "unknown")
            groups[group_key].append(item)
        output: Dict[str, Any] = {}
        for group_key, items in groups.items():
            metrics = {"candidate_count": len(items), "windows": {}}
            for window in windows:
                returns = [
                    item["windows"].get(str(window), {}).get("return_pct")
                    for item in items
                    if item.get("windows", {}).get(str(window), {}).get("status") == "ok"
                ]
                returns = [float(value) for value in returns if value is not None]
                metrics["windows"][str(window)] = {
                    "evaluated": len(returns),
                    "insufficient": len(items) - len(returns),
                    "avg_return_pct": round(sum(returns) / len(returns), 4) if returns else None,
                    "hit_rate": round(sum(1 for value in returns if value > 0) / len(returns), 4) if returns else None,
                    "top_n_hit_rate": MarketOpportunityEvaluationService._top_n_hit_rate(items, window, top_n=3),
                }
            output[group_key] = metrics
        return output

    @staticmethod
    def _top_n_hit_rate(items: List[Dict[str, Any]], window: int, *, top_n: int) -> Optional[float]:
        ranked = sorted(
            items,
            key=lambda item: (
                int(item.get("rank") or 1_000_000),
                -float(item.get("score") or 0.0),
                int(item.get("candidate_id") or 0),
            ),
        )[:top_n]
        returns = [
            item.get("windows", {}).get(str(window), {}).get("return_pct")
            for item in ranked
            if item.get("windows", {}).get(str(window), {}).get("status") == "ok"
        ]
        returns = [float(value) for value in returns if value is not None]
        if not returns:
            return None
        return round(sum(1 for value in returns if value > 0) / len(returns), 4)
