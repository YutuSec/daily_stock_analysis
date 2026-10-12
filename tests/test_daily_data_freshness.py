# -*- coding: utf-8 -*-
"""Regression tests for DATA-1 daily-bar freshness contracts."""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from data_provider.base import DataFetchError, DataFetcherManager
from src.core.pipeline import StockAnalysisPipeline


@pytest.fixture(autouse=True)
def _reset_daily_source_health():
    DataFetcherManager.reset_daily_source_health()
    yield
    DataFetcherManager.reset_daily_source_health()


class _DailyFetcher:
    def __init__(self, name: str, rows):
        self.name = name
        self.priority = 0
        self.rows = rows
        self.calls = []

    def get_daily_data(self, stock_code, start_date=None, end_date=None, days=30):
        self.calls.append(
            {
                "stock_code": stock_code,
                "start_date": start_date,
                "end_date": end_date,
                "days": days,
            }
        )
        if isinstance(self.rows, Exception):
            raise self.rows
        return pd.DataFrame(self.rows)


def _daily_row(day: str, close: float = 10.0) -> dict:
    return {
        "date": day,
        "open": close - 0.1,
        "high": close + 0.2,
        "low": close - 0.2,
        "close": close,
        "volume": 1000,
        "amount": close * 1000,
    }


def test_manager_falls_back_when_primary_daily_latest_is_before_completed_bar() -> None:
    primary = _DailyFetcher("PrimaryDaily", [_daily_row("2026-09-08")])
    backup = _DailyFetcher("BackupDaily", [_daily_row("2026-09-09", close=11.0)])
    manager = DataFetcherManager(fetchers=[primary, backup])

    df, source = manager.get_daily_data("600519", days=5, end_date="2026-09-09")

    assert source == "BackupDaily"
    assert df.iloc[-1]["close"] == 11.0
    assert primary.calls and backup.calls


def test_manager_rejects_all_stale_daily_sources_instead_of_returning_nonempty_old_data() -> None:
    primary = _DailyFetcher("PrimaryDaily", [_daily_row("2026-09-08")])
    backup = _DailyFetcher("BackupDaily", [_daily_row("2026-09-07")])
    manager = DataFetcherManager(fetchers=[primary, backup])

    with pytest.raises(DataFetchError) as excinfo:
        manager.get_daily_data("600519", days=5, end_date="2026-09-09")

    message = str(excinfo.value)
    assert "StaleDailyData" in message
    assert "expected=2026-09-09" in message


def test_manager_rejects_unknown_daily_dates_as_current_evidence() -> None:
    primary = _DailyFetcher(
        "PrimaryDaily",
        [
            {
                "open": 9.9,
                "high": 10.2,
                "low": 9.8,
                "close": 10.0,
                "volume": 1000,
                "amount": 10000,
            }
        ],
    )
    manager = DataFetcherManager(fetchers=[primary])

    with pytest.raises(DataFetchError) as excinfo:
        manager.get_daily_data("600519", days=5, end_date="2026-09-09")

    assert "date unknown" in str(excinfo.value)


def test_manager_filters_future_rows_for_historical_asof() -> None:
    primary = _DailyFetcher(
        "PrimaryDaily",
        [_daily_row("2026-09-09", close=11.0), _daily_row("2026-09-10", close=99.0)],
    )
    manager = DataFetcherManager(fetchers=[primary])

    df, source = manager.get_daily_data("600519", days=5, end_date="2026-09-09")

    assert source == "PrimaryDaily"
    assert list(pd.to_datetime(df["date"]).dt.date) == [date(2026, 9, 9)]
    assert df.iloc[-1]["close"] == 11.0


def test_manager_uses_effective_completed_trading_date_when_end_date_is_omitted() -> None:
    primary = _DailyFetcher("PrimaryDaily", [_daily_row("2026-09-08")])
    manager = DataFetcherManager(fetchers=[primary])
    current_time = datetime(2026, 9, 9, 10, 0)

    with patch("src.core.trading_calendar.get_effective_trading_date", return_value=date(2026, 9, 8)):
        df, source = manager.get_daily_data("600519", days=5, current_time=current_time)

    assert source == "PrimaryDaily"
    assert primary.calls[0]["end_date"] == "2026-09-08"
    assert pd.to_datetime(df.iloc[-1]["date"]).date() == date(2026, 9, 8)


def test_manager_explicit_weekend_end_date_accepts_previous_completed_session() -> None:
    primary = _DailyFetcher("PrimaryDaily", [_daily_row("2026-09-11")])
    manager = DataFetcherManager(fetchers=[primary])

    with patch("src.core.trading_calendar.get_effective_trading_date", return_value=date(2026, 9, 11)):
        df, source = manager.get_daily_data("600519", days=5, end_date="2026-09-12")

    assert source == "PrimaryDaily"
    assert primary.calls[0]["end_date"] == "2026-09-12"
    assert pd.to_datetime(df.iloc[-1]["date"]).date() == date(2026, 9, 11)


def test_manager_future_explicit_end_date_is_capped_to_current_completed_session() -> None:
    primary = _DailyFetcher("PrimaryDaily", [_daily_row("2026-09-11")])
    manager = DataFetcherManager(fetchers=[primary])

    def _effective(_market, current_time=None):
        if current_time is not None and current_time.date() == date(2099, 1, 1):
            return date(2099, 1, 1)
        return date(2026, 9, 11)

    with patch("src.core.trading_calendar.get_effective_trading_date", side_effect=_effective):
        df, source = manager.get_daily_data("600519", days=5, end_date="2099-01-01")

    assert source == "PrimaryDaily"
    assert pd.to_datetime(df.iloc[-1]["date"]).date() == date(2026, 9, 11)


def test_pipeline_does_not_reuse_stale_cached_context_when_target_daily_bar_is_missing() -> None:
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    pipeline.db = MagicMock()
    pipeline.db.get_analysis_context.return_value = {
        "code": "600519",
        "date": "2026-09-08",
        "today": {"date": "2026-09-08", "close": 10.0},
        "yesterday": {"date": "2026-09-07", "close": 9.8},
    }
    pipeline.fetcher_manager = MagicMock()
    pipeline.fetcher_manager.get_daily_data.side_effect = DataFetchError(
        "stale daily data latest=2026-09-08 expected=2026-09-09"
    )

    context = pipeline._get_analysis_context_with_market_fallback(
        "600519",
        paper_trading_style_context={"settings": {"analysis_window_sessions": 60}},
        target_date=date(2026, 9, 9),
    )

    assert context is None
    pipeline.fetcher_manager.get_daily_data.assert_called_once_with(
        "600519",
        days=60,
        end_date="2026-09-09",
    )


def test_pipeline_fetch_and_save_requests_effective_daily_bar_date() -> None:
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    pipeline.config = SimpleNamespace(
        paper_trading_style="position",
        paper_trading_style_overrides={},
    )
    pipeline.fetcher_manager = MagicMock()
    pipeline.fetcher_manager.get_stock_name.return_value = "贵州茅台"
    pipeline.fetcher_manager.get_daily_data.return_value = (
        pd.DataFrame([_daily_row("2026-09-08")]),
        "unit",
    )
    pipeline.db = MagicMock()
    pipeline.db.has_today_data.return_value = False
    pipeline.db.save_daily_data.return_value = 1
    pipeline._resolve_resume_target_date = lambda code, current_time=None: date(2026, 9, 8)

    ok, error = pipeline.fetch_and_save_stock_data(
        "600519",
        current_time=datetime(2026, 9, 9, 10, 0),
    )

    assert ok is True
    assert error is None
    pipeline.fetcher_manager.get_daily_data.assert_called_once_with(
        "600519",
        days=180,
        end_date="2026-09-08",
        current_time=datetime(2026, 9, 9, 10, 0),
    )
