# -*- coding: utf-8 -*-
"""Tests for market opportunity daily-bar history refresh."""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from src.config import Config
from src.services.market_opportunity_history_service import MarketOpportunityHistoryService
from src.storage import (
    AnalysisHistory,
    DatabaseManager,
    MarketOpportunityCandidate,
    MarketOpportunityRun,
    StockDaily,
)


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    old_database_url = os.environ.get("DATABASE_URL")
    db_path = tmp_path / "opportunity-history.db"
    os.environ["DATABASE_PATH"] = str(db_path)
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager.get_instance()
    try:
        yield db
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        if old_database_path is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = old_database_path
        if old_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = old_database_url


class _FakeFetcherManager:
    def __init__(self, *, failing_symbols=None):
        self.failing_symbols = set(failing_symbols or [])
        self.daily_calls = []
        self.index_calls = []

    def get_daily_data(self, stock_code, start_date=None, end_date=None, days=30):
        self.daily_calls.append(
            {"stock_code": stock_code, "start_date": start_date, "end_date": end_date, "days": days}
        )
        if stock_code in self.failing_symbols:
            raise RuntimeError(f"{stock_code} unavailable")
        return _daily_frame(start_date, end_date), "fake_daily"

    def get_index_daily_data(self, stock_code, start_date=None, end_date=None, days=30):
        self.index_calls.append(
            {"stock_code": stock_code, "start_date": start_date, "end_date": end_date, "days": days}
        )
        return _daily_frame(start_date, end_date, base=4000), "fake_index"


def _patch_calendar(monkeypatch, *, now: datetime):
    monkeypatch.setattr(
        "src.services.market_opportunity_history_service.get_market_now",
        lambda market: now,
    )
    monkeypatch.setattr(
        "src.services.market_opportunity_history_service.is_market_open",
        lambda market, check_date, strict=False: check_date.weekday() < 5,
    )


def _daily_frame(start_date, end_date, *, base=10):
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    rows = []
    cursor = start
    index = 0
    while cursor <= end:
        if cursor.weekday() < 5:
            price = base + index
            rows.append(
                {
                    "date": cursor,
                    "open": price,
                    "high": price + 1,
                    "low": price - 1,
                    "close": price + 0.5,
                    "volume": 1000 + index,
                    "amount": 100000 + index,
                    "pct_chg": 1.0,
                    "ma5": price,
                    "ma10": price,
                    "ma20": price,
                    "volume_ratio": 1.0,
                }
            )
            index += 1
        cursor = cursor.fromordinal(cursor.toordinal() + 1)
    return pd.DataFrame(rows)


def _seed_run_and_candidate(
    db,
    *,
    symbol="600519.SH",
    stock_code="600519",
    as_of=date(2026, 9, 8),
    status="selected",
    legacy_selected=False,
):
    with db.session_scope() as session:
        report = AnalysisHistory(
            query_id=f"report-{symbol}",
            code="MARKET",
            name="Market",
            report_type="market",
            raw_result="{}",
            created_at=datetime.combine(as_of, datetime.min.time()),
        )
        session.add(report)
        session.flush()
        run = MarketOpportunityRun(
            run_key=f"run-{symbol}-{as_of.isoformat()}",
            market="cn",
            trade_date=as_of,
            source_report_id=report.id,
            status="ready",
            policy_version="opportunity-policy-v3",
        )
        session.add(run)
        session.flush()
        candidate = MarketOpportunityCandidate(
            run_id=run.id,
            candidate_key=f"candidate-{symbol}-{status}-{legacy_selected}",
            market="cn",
            exchange="SH",
            stock_code=stock_code,
            symbol=symbol,
            stock_name=symbol,
            primary_theme_key="industry:test",
            status=status,
            rank=1,
            score=80,
            score_components_json=json.dumps(
                {"strategy_comparison": {"legacy_selected": legacy_selected}},
                ensure_ascii=False,
            ),
            rejection_reasons_json="[]",
            data_quality_json="{}",
            quote_snapshot_json="{}",
            as_of=as_of,
            valid_until=as_of,
            snapshot_hash=f"hash-{symbol}",
        )
        session.add(candidate)
        session.flush()
        return int(candidate.id)


def _count_daily(db, code):
    with db.session_scope() as session:
        return session.query(StockDaily).filter_by(code=code).count()


def test_refresh_backfills_selected_legacy_and_index_daily_bars(isolated_db, monkeypatch):
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    selected_id = _seed_run_and_candidate(isolated_db, symbol="600519.SH", stock_code="600519")
    legacy_id = _seed_run_and_candidate(
        isolated_db,
        symbol="000001.SZ",
        stock_code="000001",
        status="rejected",
        legacy_selected=True,
    )
    manager = _FakeFetcherManager()
    service = MarketOpportunityHistoryService(db_manager=isolated_db, fetcher_manager=manager)

    summary = service.refresh("cn", date(2026, 9, 14))

    assert summary["status"] == "ok"
    assert summary["candidates_scanned"] == 2
    assert summary["symbols_refreshed"] == 2
    assert {call["stock_code"] for call in manager.daily_calls} == {"600519.SH", "000001.SZ"}
    assert manager.daily_calls[0]["start_date"] == "2026-09-08"
    assert all(call["end_date"] == "2026-09-14" for call in manager.daily_calls)
    assert manager.index_calls == [
        {"stock_code": "sh000300", "start_date": "2026-09-08", "end_date": "2026-09-14", "days": 16}
    ]
    assert _count_daily(isolated_db, "600519.SH") > 0
    assert _count_daily(isolated_db, "000001.SZ") > 0
    assert _count_daily(isolated_db, "000300.SH") > 0
    refreshed_ids = {item for row in summary["refreshed_symbols"] for item in row["candidate_ids"]}
    assert refreshed_ids == {selected_id, legacy_id}


def test_refresh_no_tracked_candidates_does_not_fetch(isolated_db, monkeypatch):
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    _seed_run_and_candidate(isolated_db, status="rejected", legacy_selected=False)
    manager = _FakeFetcherManager()
    service = MarketOpportunityHistoryService(db_manager=isolated_db, fetcher_manager=manager)

    summary = service.refresh("cn", date(2026, 9, 14))

    assert summary["status"] == "ok"
    assert summary["reason"] == "no_tracked_candidates"
    assert summary["symbols_considered"] == 0
    assert manager.daily_calls == []
    assert manager.index_calls == []


def test_refresh_symbol_failure_does_not_block_other_symbols_or_index(isolated_db, monkeypatch):
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    _seed_run_and_candidate(isolated_db, symbol="600519.SH", stock_code="600519")
    _seed_run_and_candidate(isolated_db, symbol="000001.SZ", stock_code="000001")
    manager = _FakeFetcherManager(failing_symbols={"600519.SH"})
    service = MarketOpportunityHistoryService(db_manager=isolated_db, fetcher_manager=manager)

    summary = service.refresh("cn", date(2026, 9, 14))

    assert summary["status"] == "partial"
    assert summary["symbols_refreshed"] == 1
    assert summary["failed_symbols"][0]["symbol"] == "600519.SH"
    assert _count_daily(isolated_db, "000001.SZ") > 0
    assert _count_daily(isolated_db, "000300.SH") > 0


def test_refresh_skips_symbol_when_stock_daily_is_current(isolated_db, monkeypatch):
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    _seed_run_and_candidate(isolated_db, symbol="600519.SH", stock_code="600519")
    isolated_db.save_daily_data(_daily_frame("2026-09-08", "2026-09-14"), code="600519.SH", data_source="seed")
    isolated_db.save_daily_data(_daily_frame("2026-09-08", "2026-09-14"), code="000300.SH", data_source="seed")
    manager = _FakeFetcherManager()
    service = MarketOpportunityHistoryService(db_manager=isolated_db, fetcher_manager=manager)

    summary = service.refresh("cn", date(2026, 9, 14))

    assert summary["status"] == "ok"
    assert summary["symbols_refreshed"] == 0
    assert summary["symbols_skipped_current"] == 1
    assert summary["skipped_symbols"][0]["reason"] == "stock_daily_current"
    assert summary["index"]["status"] == "skipped"
    assert manager.daily_calls == []
    assert manager.index_calls == []


def test_refresh_saves_current_as_of_entry_day_and_deduplicates_symbol(isolated_db, monkeypatch):
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    first_id = _seed_run_and_candidate(
        isolated_db,
        symbol="600519.SH",
        stock_code="600519",
        as_of=date(2026, 9, 14),
    )
    second_id = _seed_run_and_candidate(
        isolated_db,
        symbol="600519.SH",
        stock_code="600519",
        as_of=date(2026, 9, 8),
        legacy_selected=True,
    )
    manager = _FakeFetcherManager()
    service = MarketOpportunityHistoryService(db_manager=isolated_db, fetcher_manager=manager)

    summary = service.refresh("cn", date(2026, 9, 14))

    assert summary["status"] == "ok"
    assert summary["symbols_considered"] == 1
    assert summary["symbols_refreshed"] == 1
    assert manager.daily_calls == [
        {"stock_code": "600519.SH", "start_date": "2026-09-08", "end_date": "2026-09-14", "days": 16}
    ]
    assert manager.index_calls == [
        {"stock_code": "sh000300", "start_date": "2026-09-08", "end_date": "2026-09-14", "days": 16}
    ]
    assert set(summary["refreshed_symbols"][0]["candidate_ids"]) == {first_id, second_id}
    assert _count_daily(isolated_db, "600519.SH") > 0
    assert _count_daily(isolated_db, "000300.SH") > 0


@pytest.mark.parametrize(
    ("now", "trade_date", "reason"),
    [
        (datetime(2026, 9, 14, 11, 0, tzinfo=ZoneInfo("Asia/Shanghai")), date(2026, 9, 14), "market_not_closed"),
        (datetime(2026, 9, 15, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")), date(2026, 9, 14), "stale_trade_date"),
        (datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")), date(2026, 9, 15), "future_trade_date"),
    ],
)
def test_refresh_rejects_intraday_stale_and_future_without_fetch(isolated_db, monkeypatch, now, trade_date, reason):
    _patch_calendar(monkeypatch, now=now)
    _seed_run_and_candidate(isolated_db)
    manager = _FakeFetcherManager()
    service = MarketOpportunityHistoryService(db_manager=isolated_db, fetcher_manager=manager)

    summary = service.refresh("cn", trade_date)

    assert summary["status"] == "skipped"
    assert summary["reason"] == reason
    assert manager.daily_calls == []
    assert manager.index_calls == []
