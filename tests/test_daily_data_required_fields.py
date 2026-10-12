# -*- coding: utf-8 -*-
"""Tests for caller-requested daily data field quality routing."""

from __future__ import annotations

import pandas as pd
import pytest

from data_provider.base import DataFetchError, DataFetcherManager


class _DailyFrameFetcher:
    def __init__(self, name: str, priority: int, frame: pd.DataFrame):
        self.name = name
        self.priority = priority
        self.frame = frame
        self.calls = 0

    def get_daily_data(self, **kwargs):
        self.calls += 1
        return self.frame.copy()


def _frame(*, amount_values=None, include_amount: bool = True) -> pd.DataFrame:
    rows = {
        "date": pd.to_datetime(["2026-09-11", "2026-09-14"]),
        "open": [10.0, 10.2],
        "high": [10.5, 10.7],
        "low": [9.9, 10.0],
        "close": [10.2, 10.6],
        "volume": [1000.0, 1200.0],
    }
    if include_amount:
        rows["amount"] = amount_values if amount_values is not None else [10000.0, 12000.0]
    return pd.DataFrame(rows)


def setup_function() -> None:
    DataFetcherManager.reset_daily_source_health()


def test_required_amount_falls_back_to_second_complete_provider() -> None:
    first = _DailyFrameFetcher("FirstMissingAmount", 0, _frame(include_amount=False))
    second = _DailyFrameFetcher("SecondCompleteAmount", 1, _frame(amount_values=[10000.0, 12000.0]))
    manager = DataFetcherManager(fetchers=[first, second])

    df, source = manager.get_daily_data(
        "000001",
        start_date="2026-09-01",
        end_date="2026-09-14",
        required_fields=["amount"],
        required_tail_rows=2,
    )

    assert source == "SecondCompleteAmount"
    assert first.calls == 1
    assert second.calls == 1
    assert df["amount"].tolist() == [10000.0, 12000.0]


def test_required_amount_fails_when_all_providers_lack_quality_field() -> None:
    first = _DailyFrameFetcher("FirstMissingAmount", 0, _frame(include_amount=False))
    second = _DailyFrameFetcher("SecondNonFiniteAmount", 1, _frame(amount_values=[10000.0, float("nan")]))
    manager = DataFetcherManager(fetchers=[first, second])

    with pytest.raises(DataFetchError, match="RequiredFieldQuality"):
        manager.get_daily_data(
            "000001",
            start_date="2026-09-01",
            end_date="2026-09-14",
            required_fields=["amount"],
            required_tail_rows=2,
        )

    assert first.calls == 1
    assert second.calls == 1


def test_default_daily_data_accepts_ohlc_only_provider() -> None:
    first = _DailyFrameFetcher("FirstOhlcOnly", 0, _frame(include_amount=False))
    second = _DailyFrameFetcher("SecondCompleteAmount", 1, _frame(amount_values=[10000.0, 12000.0]))
    manager = DataFetcherManager(fetchers=[first, second])

    df, source = manager.get_daily_data(
        "000001",
        start_date="2026-09-01",
        end_date="2026-09-14",
    )

    assert source == "FirstOhlcOnly"
    assert first.calls == 1
    assert second.calls == 0
    assert "amount" not in df.columns
