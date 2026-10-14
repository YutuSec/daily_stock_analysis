# -*- coding: utf-8 -*-
"""自选股分级分析（定时批次降频）测试。"""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.services import watchlist_tiering_service as tiering
from src.services.watchlist_tiering_service import select_scheduled_codes

# 2026-10-14 是周三；10-12 周一、10-09 周五、10-08 周四都是 A 股交易日。
TODAY = date(2026, 10, 14)


def _config(**overrides):
    values = {
        "watchlist_tiering_enabled": True,
        "watchlist_refresh_trading_days": 3,
        "watchlist_anomaly_pct_chg": 4.0,
        "watchlist_anomaly_volume_ratio": 2.0,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _FakeDb:
    """按 code 返回最近一次分析时间；未登记的 code 视为从未分析。"""

    def __init__(self, last_analyzed=None, error=None):
        self.last_analyzed = dict(last_analyzed or {})
        self.error = error
        self.queries = []

    def get_analysis_history(self, code=None, days=30, limit=50, **_):
        self.queries.append(code)
        if self.error:
            raise self.error
        created = self.last_analyzed.get(code)
        return [SimpleNamespace(created_at=created)] if created else []


class _FakeFetcher:
    def __init__(self, quotes=None, error=None):
        self.quotes = dict(quotes or {})
        self.error = error
        self.requested = []

    def prefetch_realtime_quotes(self, codes):
        return len(codes)

    def get_realtime_quote(self, code, *, log_final_failure=True):
        self.requested.append(code)
        if self.error:
            raise self.error
        return self.quotes.get(code)


def _quote(change_pct=None, volume_ratio=None):
    return SimpleNamespace(change_pct=change_pct, volume_ratio=volume_ratio)


def _paper_repo(positions=(), plans=()):
    repo = MagicMock()
    repo.list_open_positions_all.return_value = [SimpleNamespace(stock_code=c) for c in positions]
    repo.list_active_plans.return_value = [SimpleNamespace(stock_code=c) for c in plans]
    return repo


def _portfolio_repo(symbols=()):
    repo = MagicMock()
    repo.list_cached_position_identities.return_value = [("cn", s) for s in symbols]
    return repo


def _recent(day=date(2026, 10, 13)):
    return datetime(day.year, day.month, day.day, 18, 0)


def _run(codes, *, db=None, fetcher=None, paper=None, portfolio=None, config=None):
    with patch(
        "src.repositories.paper_trading_repo.PaperTradingRepository",
        return_value=paper or _paper_repo(),
    ):
        return select_scheduled_codes(
            codes,
            config=config or _config(),
            db=db or _FakeDb(),
            fetcher_manager=fetcher if fetcher is not None else _FakeFetcher(),
            portfolio_repo=portfolio or _portfolio_repo(),
            today=TODAY,
        )


def test_disabled_keeps_full_list_without_queries():
    db = _FakeDb()
    fetcher = _FakeFetcher()

    result = _run(["600519", "159915"], db=db, fetcher=fetcher, config=_config(watchlist_tiering_enabled=False))

    assert result.to_analyze == ["600519", "159915"]
    assert result.skipped == []
    assert db.queries == [] and fetcher.requested == []


@pytest.mark.parametrize("source", ["portfolio", "paper_position", "paper_plan"])
def test_held_or_planned_is_always_analyzed(source):
    # 昨天刚分析过、今天无异动，也必须分析。
    db = _FakeDb({"600879": _recent()})
    fetcher = _FakeFetcher({"600879": _quote(0.1, 0.8)})
    kwargs = {
        "portfolio": _portfolio_repo(["SH600879"]) if source == "portfolio" else None,
        "paper": _paper_repo(
            positions=["600879"] if source == "paper_position" else (),
            plans=["600879"] if source == "paper_plan" else (),
        ),
    }

    result = _run(["600879.SH"], db=db, fetcher=fetcher, **kwargs)

    assert result.to_analyze == ["600879.SH"]
    assert result.reasons["600879.SH"] == "held_or_planned"
    # 必分析的股票不需要取行情。
    assert fetcher.requested == []


def test_prefixed_watchlist_code_matches_plain_paper_plan():
    result = _run(["SZ159915"], db=_FakeDb({"SZ159915": _recent()}), paper=_paper_repo(plans=["159915"]))

    assert result.reasons["SZ159915"] == "held_or_planned"


def test_never_analyzed_is_analyzed():
    result = _run(["600519"])

    assert result.reasons["600519"] == "never_analyzed"
    assert result.to_analyze == ["600519"]


@pytest.mark.parametrize(("last_day", "expected"), [
    # 10-09(五) 之后 10-12、10-13、10-14 共 3 个交易日：到期。
    (date(2026, 10, 9), "refresh_due"),
    # 10-12(一) 之后只有 10-13、10-14 两个交易日：未到期且无异动，跳过。
    (date(2026, 10, 12), "skipped"),
])
def test_refresh_interval_counts_trading_days(last_day, expected):
    db = _FakeDb({"600519": _recent(last_day)})
    fetcher = _FakeFetcher({"600519": _quote(0.5, 1.0)})

    result = _run(["600519"], db=db, fetcher=fetcher)

    assert result.reasons["600519"] == expected


def test_history_lookup_tries_raw_and_normalized_codes():
    # 历史记录以纯代码保存，自选里写的是带后缀格式。
    db = _FakeDb({"600519": _recent(date(2026, 10, 13))})
    fetcher = _FakeFetcher({"600519.SH": _quote(0.2, 1.0)})

    result = _run(["600519.SH"], db=db, fetcher=fetcher)

    assert set(db.queries) == {"600519.SH", "600519"}
    assert result.reasons["600519.SH"] == "skipped"


@pytest.mark.parametrize(("quote", "expected"), [
    (_quote(4.0, 1.0), "anomaly_pct_chg"),
    (_quote(-5.2, 1.0), "anomaly_pct_chg"),
    (_quote(1.0, 2.0), "anomaly_volume_ratio"),
    (_quote(3.9, 1.9), "skipped"),
    (_quote(None, None), "quote_unavailable"),
    (None, "quote_unavailable"),
])
def test_anomaly_rules(quote, expected):
    db = _FakeDb({"600519": _recent()})
    fetcher = _FakeFetcher({"600519": quote} if quote is not None else {})

    result = _run(["600519"], db=db, fetcher=fetcher)

    assert result.reasons["600519"] == expected
    assert (result.skipped == ["600519"]) is (expected == "skipped")


def test_etf_follows_same_rules_as_stock():
    db = _FakeDb({"159915": _recent(), "600519": _recent()})
    fetcher = _FakeFetcher({"159915": _quote(0.3, 1.1), "600519": _quote(0.3, 1.1)})

    result = _run(["159915", "600519"], db=db, fetcher=fetcher)

    assert result.skipped == ["159915", "600519"]


def test_quote_errors_fail_open():
    db = _FakeDb({"600519": _recent()})

    result = _run(["600519"], db=db, fetcher=_FakeFetcher(error=RuntimeError("source down")))

    assert result.to_analyze == ["600519"]
    assert result.reasons["600519"] == "quote_unavailable"


def test_history_errors_fail_open():
    result = _run(["600519"], db=_FakeDb(error=RuntimeError("db down")))

    assert result.reasons["600519"] == "history_check_error"
    assert result.to_analyze == ["600519"]


def test_position_lookup_errors_do_not_drop_stocks():
    portfolio = MagicMock()
    portfolio.list_cached_position_identities.side_effect = RuntimeError("boom")
    paper = MagicMock()
    paper.list_open_positions_all.side_effect = RuntimeError("boom")

    result = _run(["600519"], portfolio=portfolio, paper=paper)

    # 持仓查询失败不影响其余规则；从未分析过仍照常分析。
    assert result.to_analyze == ["600519"]


def test_preserves_watchlist_order_and_summary():
    db = _FakeDb({"000001": _recent(), "600519": _recent()})
    fetcher = _FakeFetcher({"000001": _quote(0.1, 1.0), "600519": _quote(6.0, 1.0)})

    result = _run(["300750", "000001", "600519"], db=db, fetcher=fetcher)

    assert result.to_analyze == ["300750", "600519"]
    assert result.skipped == ["000001"]
    assert "分析 2 只" in result.summary and "跳过 1 只" in result.summary


def test_trading_days_since_unknown_market_returns_none():
    assert tiering._trading_days_since(date(2026, 10, 9), TODAY, "zz") is None
