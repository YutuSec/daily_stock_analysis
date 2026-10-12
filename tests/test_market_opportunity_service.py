# -*- coding: utf-8 -*-
"""Focused tests for market-review driven opportunity orchestration."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from data_provider.base import DataFetcherManager
from data_provider.akshare_board_provider import (
    BOARD_CONSTITUENT_SCHEMA_VERSION,
    BoardConstituent,
    BoardConstituentSnapshot,
    BoardDailyBar,
    BoardHistorySnapshot,
)
from src.config import Config
from src.repositories.market_opportunity_repository import MarketOpportunityRepository
from src.services.market_opportunity_service import MarketOpportunityService
from src.services.market_opportunity_followup_service import build_selection_context
from src.services.runtime_settings_service import RuntimeSettingsService
from src.storage import DatabaseManager, MarketOpportunityAnalysisJob, MarketOpportunityRun
from tests.report_intent_helpers import add_report_execution_intent


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    old_database_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_PATH"] = str(tmp_path / "market-opportunity.db")
    os.environ["DATABASE_URL"] = f"sqlite:///{tmp_path / 'market-opportunity.db'}"
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
    def __init__(self, constituents):
        self.constituents = constituents
        self.daily_calls = []

    def get_board_constituents(self, board_name, board_type="concept"):
        return list(self.constituents)

    def get_daily_data(self, code, start_date=None, end_date=None, days=120):
        self.daily_calls.append(code)
        rows = []
        dates = pd.bdate_range(end="2026-06-30", periods=max(int(days or 120), 90))
        for idx, day in enumerate(dates):
            close = 10 + idx * 0.12
            rows.append(
                {
                    "date": day.date(),
                    "open": close - 0.05,
                    "high": close + 0.10,
                    "low": close - 0.10,
                    "close": close,
                    "volume": 1_000_000,
                    "amount": 220_000_000,
                }
            )
        return pd.DataFrame(rows), "fake_daily"


class _OfflineDailyBackend:
    name = "offline-daily"
    priority = 0

    def get_daily_data(self, **_kwargs):
        return pd.DataFrame(
            [
                {"date": date(2026, 6, 30), "open": 10, "high": 11, "low": 9, "close": 10, "volume": 1, "amount": 1}
            ]
        )


class _FakeBoardProvider:
    def __init__(self, constituents, *, history_status="ok"):
        self.constituents = constituents
        self.history_status = history_status

    def get_board_constituents(self, *, market="cn", board_type, board_name, board_code=None, as_of=None):
        rows = [
            BoardConstituent(
                market="cn",
                exchange=str(item.get("exchange") or ""),
                code=str(item.get("code") or ""),
                symbol=str(item.get("symbol") or ""),
                name=str(item.get("name") or ""),
                raw=dict(item),
            )
            for item in self.constituents
        ]
        return BoardConstituentSnapshot(
            market=market,
            board_type=board_type,
            board_name=board_name,
            board_code=board_code,
            as_of=as_of or date(2026, 6, 30),
            provider="fake",
            fetched_at=datetime(2026, 6, 30, 16, 0),
            status="ok" if rows else "failed",
            schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
            constituents=rows,
            errors=[] if rows else ["empty_snapshot"],
            source_columns=["code", "exchange", "symbol", "name"],
            snapshot_hash="fake-hash",
        )

    def get_board_history(self, *, market="cn", board_type, board_name, as_of=None, lookback_days=10):
        bars = [
            BoardDailyBar(date=date(2026, 6, 25) + timedelta(days=idx), close=100 + idx, change_pct=1.0, amount=1_000_000_000)
            for idx in range(6)
        ]
        return BoardHistorySnapshot(
            market=market,
            board_type=board_type,
            board_name=board_name,
            as_of=as_of or date(2026, 6, 30),
            provider="fake",
            fetched_at=datetime(2026, 6, 30, 16, 0),
            status=self.history_status,
            schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
            bars=bars if self.history_status == "ok" else [],
            errors=[] if self.history_status == "ok" else ["history_unavailable"],
        )


class _FakeRepo:
    def __init__(self, jobs, candidate, *, submitted_jobs=None):
        self.jobs = list(jobs)
        self.candidate = candidate
        self.submitted_jobs = list(submitted_jobs or [])
        self.completed = []
        self.linked = []
        self.submitted = []

    def claim_pending_jobs(self, *, owner, limit, run_id=None, lease_seconds=900, effective_session=None):
        return self.jobs[:limit]

    def mark_job_submitted(self, job_id, *, owner, query_id):
        self.submitted.append((job_id, owner, query_id))
        return True

    def list_submitted_jobs(self, *, run_id=None, limit=200):
        return self.submitted_jobs[:limit]

    def get_candidate(self, candidate_id):
        return self.candidate if int(candidate_id) == int(self.candidate.id) else None

    def complete_job(self, job_id, **kwargs):
        self.completed.append((job_id, kwargs))

    def link_job_artifacts(self, job_id, **kwargs):
        self.linked.append((job_id, kwargs))
        return True


class _PersistenceOnlyRepo:
    def __init__(self, rows):
        self.rows = list(rows)

    def theme_persistence_window(self, **_kwargs):
        return list(self.rows)


class _FakeBoardObservationService:
    def __init__(self, evidence):
        self.evidence = dict(evidence)

    def persistence_for_theme(self, *_args, **_kwargs):
        return dict(self.evidence)


def _service_config(**overrides):
    defaults = {
        "market_opportunity_mode": "paper",
        "market_opportunity_max_themes": 3,
        "market_opportunity_max_candidates_per_theme": 3,
        "market_opportunity_max_total_candidates": 4,
        "market_opportunity_max_llm_calls_per_run": 4,
        "market_opportunity_min_20d_avg_amount_cny": 100_000_000,
        "market_opportunity_blacklist": [],
        "market_opportunity_observe_gate_days": 0,
        "paper_trading_enabled": True,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_extract_themes_reads_market_review_sector_and_concept_top_lists():
    service = MarketOpportunityService(config=_service_config())

    themes = service._extract_themes(
        {
            "sectors": {"top": [{"name": "半导体", "change_pct": 2.5, "source": "eastmoney_industry", "code": "BK1036"}]},
            "concepts": {"top": [{"name": "创新药", "change_pct": 3.1}]},
        }
    )

    assert [item["theme_key"] for item in themes] == ["industry:半导体", "concept:创新药"]
    assert themes[0]["theme_source"] == "eastmoney_industry"
    assert themes[0]["theme_code"] == "BK1036"


def test_theme_breadth_prefers_constituent_snapshot_without_daily_fanout():
    service = MarketOpportunityService(config=_service_config())
    service._daily_bars = MagicMock(side_effect=AssertionError("raw snapshot must avoid daily fanout"))
    snapshot = BoardConstituentSnapshot(
        market="cn",
        board_type="industry",
        board_name="半导体",
        board_code=None,
        as_of=date(2026, 8, 31),
        provider="fake",
        fetched_at=datetime(2026, 8, 31, 15, 20),
        status="ok",
        schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
        constituents=[
            BoardConstituent(
                market="cn",
                exchange="sh",
                code=f"600{idx:03d}",
                symbol=f"600{idx:03d}.SH",
                name=f"测试{idx}",
                raw={"涨跌幅": 1.0 if idx < 6 else -1.0, "成交额": 100_000_000, "量比": 1.3 if idx < 4 else 0.9},
            )
            for idx in range(10)
        ],
        errors=[],
        source_columns=["涨跌幅", "成交额", "量比"],
        snapshot_hash="raw-breadth",
    )

    metrics = service._theme_breadth_metrics(snapshot)

    assert metrics["ok"] is True
    assert metrics["valid_count"] == 10
    assert metrics["advancing_ratio"] == 0.6
    assert metrics["amount_coverage_ratio"] == 1.0
    assert metrics["volume_expansion_ratio"] == 0.4
    assert metrics["fallback_daily_fetches"] == 0
    service._daily_bars.assert_not_called()


def test_theme_breadth_caps_missing_snapshot_daily_fallback_at_thirty():
    service = MarketOpportunityService(config=_service_config())
    service._daily_bars = MagicMock(return_value=None)
    snapshot = BoardConstituentSnapshot(
        market="cn",
        board_type="concept",
        board_name="机器人",
        board_code=None,
        as_of=date(2026, 8, 31),
        provider="fake",
        fetched_at=datetime(2026, 8, 31, 15, 20),
        status="ok",
        schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
        constituents=[
            BoardConstituent(
                market="cn",
                exchange="sz",
                code=f"30{idx:04d}",
                symbol=f"30{idx:04d}.SZ",
                name=f"测试{idx}",
                raw={},
            )
            for idx in range(80)
        ],
        errors=[],
        source_columns=[],
        snapshot_hash="fallback-cap",
    )

    metrics = service._theme_breadth_metrics(snapshot)

    assert metrics["ok"] is False
    assert metrics["reason"] == "insufficient_constituent_daily_bars"
    assert metrics["fallback_daily_fetches"] == 30
    assert service._daily_bars.call_count == 30


def test_candidate_scoring_uses_fetcher_manager_and_rejects_etf_without_llm():
    constituents = [{"code": "512480", "exchange": "sh", "symbol": "512480.SH", "name": "半导体ETF"}]
    constituents.extend([
        {"code": f"600{i:03d}", "exchange": "sh", "symbol": f"600{i:03d}.SH", "name": f"股票{i}"}
        for i in range(1, 31)
    ])
    service = MarketOpportunityService(
        config=_service_config(),
        fetcher_manager=_FakeFetcherManager(constituents),
        board_provider=_FakeBoardProvider(constituents),
    )
    policy = service._policy()

    selected_themes, theme_rows, candidate_rows = service._score_themes_and_candidates(
        themes=[
            {
                "theme_key": "industry:半导体",
                "theme_type": "industry",
                "theme_name": "半导体",
                "theme_source": "eastmoney_industry",
                "theme_code": "BK1036",
                "rank": 1,
                "change_pct": 3.0,
            }
        ],
        policy=policy,
        market="cn",
        trade_date=date(2026, 6, 30),
        regime="normal",
    )
    capped = service._rank_and_cap_candidates(candidate_rows, policy=policy, regime="normal")

    assert selected_themes
    assert theme_rows[0]["constituent_count"] == 31
    assert theme_rows[0]["score_components"]["persistence"] == 10.0
    assert theme_rows[0]["score_components"]["breadth"] == 25.0
    assert theme_rows[0]["score"] == 95.0
    assert sum(1 for item in capped if item["status"] == "selected") == 3
    assert any(
        item["stock_code"] == "512480" and "etf_not_supported" in item["rejection_reasons"]
        for item in candidate_rows
    )
    selected = [item for item in capped if item["status"] == "selected"]
    assert selected[0]["quote_snapshot"]["source"] == "fake_daily"
    assert selected[0]["quote_snapshot"]["benchmark_source"] == "fake_daily"


def test_daily_bars_consumes_real_fetcher_manager_tuple_contract_and_preserves_source():
    manager = DataFetcherManager(fetchers=[_OfflineDailyBackend()])
    service = MarketOpportunityService(config=_service_config(), fetcher_manager=manager)

    bars = service._daily_bars("600001.SH", date(2026, 6, 30))

    assert bars is not None
    assert list(bars["close"]) == [10]
    assert bars.attrs["source_name"] == "offline-daily"


def test_benchmark_bars_request_explicit_csi300_index_identity():
    manager = _FakeFetcherManager([])
    service = MarketOpportunityService(config=_service_config(), fetcher_manager=manager)

    bars = service._benchmark_bars(date(2026, 6, 30), days=80)

    assert bars is not None
    assert manager.daily_calls[0] == "000300.SH"


def test_candidate_presort_pool_does_not_starve_high_score_later_theme():
    service = MarketOpportunityService(config=_service_config(market_opportunity_max_total_candidates=8))
    policy = service._policy()
    candidates = []
    for idx in range(30):
        candidates.append(
            {
                "candidate_key": f"cn:sh:600{idx:03d}",
                "status": "selected",
                "score": 80 - idx * 0.1,
                "primary_theme_key": "industry:A",
                "rejection_reasons": [],
            }
        )
    for idx in range(3):
        candidates.append(
            {
                "candidate_key": f"cn:sz:300{idx:03d}",
                "status": "selected",
                "score": 99 - idx,
                "primary_theme_key": "concept:B",
                "rejection_reasons": [],
            }
        )

    presorted = service._candidate_presort_pool(candidates, policy=policy, regime="normal")
    ranked = service._rank_and_cap_candidates(presorted, policy=policy, regime="normal")

    selected = [item for item in ranked if item["status"] == "selected"]
    assert [item["primary_theme_key"] for item in selected[:3]] == ["concept:B", "concept:B", "concept:B"]


def test_candidate_presort_keeps_other_themes_available_after_per_theme_cap():
    service = MarketOpportunityService(
        config=_service_config(
            market_opportunity_max_total_candidates=8,
            market_opportunity_max_candidates_per_theme=3,
        )
    )
    policy = service._policy()
    candidates = [
        {
            "candidate_key": f"A{idx}",
            "status": "selected",
            "score": 100 - idx,
            "primary_theme_key": "A",
            "rejection_reasons": [],
        }
        for idx in range(24)
    ] + [
        {
            "candidate_key": f"B{idx}",
            "status": "selected",
            "score": 75 - idx,
            "primary_theme_key": "B",
            "rejection_reasons": [],
        }
        for idx in range(5)
    ]

    presorted = service._candidate_presort_pool(candidates, policy=policy, regime="normal")
    ranked = service._rank_and_cap_candidates(presorted, policy=policy, regime="normal")
    selected = [item for item in ranked if item["status"] == "selected"]

    assert len(selected) == 6
    assert {item["primary_theme_key"] for item in selected} == {"A", "B"}
    assert sum(item["primary_theme_key"] == "A" for item in selected) == 3
    assert sum(item["primary_theme_key"] == "B" for item in selected) == 3


def test_stale_board_constituent_snapshot_is_insufficient_and_never_scores_candidates():
    constituents = [
        {
            "code": f"600{idx:03d}",
            "exchange": "sh",
            "symbol": f"600{idx:03d}.SH",
            "name": f"测试{idx}",
        }
        for idx in range(10)
    ]
    board_provider = _FakeBoardProvider(constituents)
    stale_snapshot = board_provider.get_board_constituents(
        market="cn",
        board_type="industry",
        board_name="半导体",
        as_of=date(2026, 6, 29),
    )
    board_provider.get_board_constituents = MagicMock(return_value=stale_snapshot)
    service = MarketOpportunityService(
        config=_service_config(),
        fetcher_manager=_FakeFetcherManager(constituents),
        board_provider=board_provider,
    )
    service._score_theme = MagicMock(side_effect=AssertionError("stale board evidence must not be scored"))
    service._score_candidate = MagicMock(side_effect=AssertionError("stale board membership must not create candidates"))

    selected, themes, candidates = service._score_themes_and_candidates(
        themes=[{
            "theme_key": "industry:半导体",
            "theme_type": "industry",
            "theme_name": "半导体",
            "theme_source": "eastmoney_industry",
            "theme_code": "BK1036",
            "rank": 1,
            "change_pct": 3.0,
        }],
        policy=service._policy(),
        market="cn",
        trade_date=date(2026, 6, 30),
        regime="normal",
    )

    assert selected == []
    assert candidates == []
    assert themes[0]["status"] == "insufficient_data"
    assert "constituent_snapshot_as_of_mismatch" in themes[0]["rejection_reasons"]
    assert themes[0]["data_quality"]["snapshot_as_of"] == "2026-06-29"
    assert themes[0]["data_quality"]["expected_as_of"] == "2026-06-30"
    assert themes[0]["data_quality"]["snapshot_date_matches"] is False


def test_candidate_requires_stock_and_benchmark_bars_exactly_as_of_trade_date():
    service = MarketOpportunityService(config=_service_config(market_opportunity_min_20d_avg_amount_cny=0))
    stale_rows = []
    exact_rows = []
    for idx, day in enumerate(pd.bdate_range(end="2026-06-29", periods=180)):
        close = 10 + idx * 0.1
        stale_rows.append(
            {"date": day.date(), "open": close - 0.1, "high": close + 0.2, "low": close - 0.2, "close": close, "volume": 1_000_000, "amount": 200_000_000}
        )
    for idx, day in enumerate(pd.bdate_range(end="2026-06-30", periods=180)):
        close = 10 + idx * 0.1
        exact_rows.append(
            {"date": day.date(), "open": close - 0.1, "high": close + 0.2, "low": close - 0.2, "close": close, "volume": 1_000_000, "amount": 200_000_000}
        )
    stock_bars = pd.DataFrame(stale_rows)
    benchmark_bars = pd.DataFrame(exact_rows)
    service._daily_bars = MagicMock(return_value=stock_bars)
    service._benchmark_bars = MagicMock(return_value=benchmark_bars)

    candidate = service._score_candidate(
        constituent={"code": "600001", "exchange": "sh", "symbol": "600001.SH", "name": "测试股份"},
        theme={"theme_key": "industry:半导体", "theme_name": "半导体", "score": 100},
        trade_date=date(2026, 6, 30),
        policy=service._policy(),
        threshold=70,
    )

    assert candidate["status"] == "rejected"
    assert "daily_bar_not_as_of_trade_date" in candidate["rejection_reasons"]
    assert "benchmark_bar_not_as_of_trade_date" not in candidate["rejection_reasons"]


def test_candidate_rejects_stale_benchmark_even_when_stock_bar_is_current():
    service = MarketOpportunityService(config=_service_config(market_opportunity_min_20d_avg_amount_cny=0))
    current_rows = []
    stale_rows = []
    for idx, day in enumerate(pd.bdate_range(end="2026-06-30", periods=180)):
        close = 10 + idx * 0.1
        current_rows.append(
            {"date": day.date(), "open": close - 0.1, "high": close + 0.2, "low": close - 0.2, "close": close, "volume": 1_000_000, "amount": 200_000_000}
        )
    for idx, day in enumerate(pd.bdate_range(end="2026-06-29", periods=180)):
        close = 10 + idx * 0.1
        stale_rows.append(
            {"date": day.date(), "open": close - 0.1, "high": close + 0.2, "low": close - 0.2, "close": close, "volume": 1_000_000, "amount": 200_000_000}
        )
    service._daily_bars = MagicMock(return_value=pd.DataFrame(current_rows))
    service._benchmark_bars = MagicMock(return_value=pd.DataFrame(stale_rows))

    candidate = service._score_candidate(
        constituent={"code": "600001", "exchange": "sh", "symbol": "600001.SH", "name": "测试股份"},
        theme={"theme_key": "industry:半导体", "theme_name": "半导体", "score": 100},
        trade_date=date(2026, 6, 30),
        policy=service._policy(),
        threshold=70,
    )

    assert "daily_bar_not_as_of_trade_date" not in candidate["rejection_reasons"]
    assert "benchmark_bar_not_as_of_trade_date" in candidate["rejection_reasons"]


def test_position_style_candidate_scoring_consumes_medium_term_ma_without_hard_rejecting():
    service = MarketOpportunityService(config=_service_config(market_opportunity_min_20d_avg_amount_cny=0, paper_trading_style="position"))
    dates = pd.bdate_range(end="2026-06-30", periods=180)
    rows = []
    for idx, day in enumerate(dates):
        close = 20.0 if idx < 165 else 10.0 + (idx - 165) * 0.15
        rows.append(
            {"date": day.date(), "open": close - 0.1, "high": close + 0.2, "low": close - 0.2, "close": close, "volume": 1_000_000, "amount": 200_000_000}
        )
    bars = pd.DataFrame(rows)
    service._daily_bars = MagicMock(return_value=bars)
    service._benchmark_bars = MagicMock(return_value=bars)

    candidate = service._score_candidate(
        constituent={"code": "600001", "exchange": "sh", "symbol": "600001.SH", "name": "测试股份"},
        theme={"theme_key": "industry:半导体", "theme_name": "半导体", "score": 100},
        trade_date=date(2026, 6, 30),
        policy=service._policy(),
        threshold=70,
    )

    assert candidate["score_components"]["trading_style"] == "position"
    assert candidate["score_components"]["trading_style_core_ma_windows"] == [10, 20, 60]
    assert candidate["score_components"]["style_medium_term_alignment"] is False
    assert candidate["score_components"]["trend_quality"] == 4.0
    assert candidate["score_components"]["style_price_structure_window"] == 60
    assert candidate["score_components"]["price_structure"] == 7.0
    assert "daily_bar_not_as_of_trade_date" not in candidate["rejection_reasons"]


def test_position_style_trend_score_is_not_driven_by_short_ma_stack_alone():
    payload = MarketOpportunityService._candidate_style_score_payload(
        close=12.0,
        ma5=11.5,
        ma10=11.0,
        ma20=10.5,
        ma60=20.0,
        style_context={
            "trading_style": "position",
            "settings": {"core_ma_windows": [10, 20, 60]},
        },
    )

    assert payload["style_trend_basis"] == "style_core_ma_position_10_20_60"
    assert payload["medium_term_alignment"] is False
    assert payload["trend_score"] < 10.0


def test_position_style_history_sufficient_requires_full_analysis_window_not_ma_only():
    service = MarketOpportunityService(config=_service_config(paper_trading_style="position"))
    dates = pd.bdate_range(end="2026-06-30", periods=61)
    bars = pd.DataFrame({
        "date": dates,
        "close": [10 * 1.001 ** idx for idx in range(61)],
        "open": [10.0] * 61,
        "high": [10.2] * 61,
        "low": [9.8] * 61,
        "volume": [1_000_000] * 61,
        "amount": [200_000_000] * 61,
    })

    metrics = service._candidate_metrics(
        bars,
        benchmark_bars=bars.assign(close=10.0),
        style_context=service._candidate_style_context(market="cn", stock_code="600001.SH"),
    )

    components = metrics["components"]
    assert components["style_required_history_sessions"] == 180
    assert components["style_bars_observed"] == 61
    assert components["style_benchmark_bars_observed"] == 61
    assert components["style_history_sufficient"] is False


def test_continuation_setup_allows_small_neutral_benchmark_lag():
    service = MarketOpportunityService(config=_service_config(paper_trading_style="position"))
    dates = pd.bdate_range(end="2026-06-30", periods=180)
    bars = pd.DataFrame({
        "date": dates,
        "close": [10 * 1.001 ** idx for idx in range(180)],
        "open": [10 * 1.001 ** idx for idx in range(180)],
        "high": [10.2 * 1.001 ** idx for idx in range(180)],
        "low": [9.8 * 1.001 ** idx for idx in range(180)],
        "volume": [1_000_000] * 180,
        "amount": [200_000_000] * 180,
    })
    benchmark = bars.assign(close=[10 * 1.0013 ** idx for idx in range(180)])

    metrics = service._candidate_metrics(
        bars,
        benchmark_bars=benchmark,
        style_context=service._candidate_style_context(market="cn", stock_code="600001.SH"),
    )

    assert metrics["components"]["style_return_pct"] < metrics["components"]["style_benchmark_return_pct"]
    assert metrics["components"]["relative_strength"] < 10
    assert metrics["components"]["setup_type"] == "trend_continuation"


@pytest.mark.parametrize("style,history,windows,volatility", [
    ("short_swing", 80, [5, 10, 20], 20),
    ("position", 180, [10, 20, 60], 60),
    ("trend", 500, [20, 60, 120, 250], 120),
])
def test_candidate_consumes_real_configured_style_windows(style, history, windows, volatility):
    service = MarketOpportunityService(config=_service_config(paper_trading_style=style))
    closes = [10 * 1.002 ** idx for idx in range(history)]
    bars = pd.DataFrame({"date": pd.bdate_range(end="2026-06-30", periods=history),
                         "close": closes, "volume": 1_000_000, "amount": 200_000_000})
    benchmark = bars.assign(close=10.0)
    service._daily_bars = MagicMock(return_value=bars)
    service._benchmark_bars = MagicMock(return_value=benchmark)
    candidate = service._score_candidate(
        constituent={"code": "600001", "exchange": "sh", "symbol": "600001.SH", "name": "测试"},
        theme={"theme_key": "industry:test", "theme_name": "test", "score": 100},
        trade_date=date(2026, 6, 30), policy=service._policy(), threshold=0,
    )
    components = candidate["score_components"]
    assert components["style_history_sufficient"] is True
    assert set(components["style_core_moving_averages"]) == {f"ma{window}" for window in windows}
    assert components["style_return_window"] == max(windows)
    assert components["style_price_structure_window"] == max(windows)
    assert components["style_volatility_window"] == volatility
    assert components["relative_strength"] == pytest.approx(10 + (1.002 ** 20 - 1) * 100)
    assert components["trend_quality"] == 20
    service._daily_bars.assert_called_once_with("600001.SH", date(2026, 6, 30), days=history)


def test_pool_style_defaults_to_paper_trading_style_when_unset():
    # 未配置机会池专用风格时沿用 PAPER_TRADING_STYLE，保持既有行为。
    service = MarketOpportunityService(
        config=_service_config(paper_trading_style="position", market_opportunity_style="")
    )
    context = service._candidate_style_context(market="cn", stock_code="600001.SH")
    assert context["trading_style"] == "position"
    assert context["source"] == "paper_trading_style_config"


def test_pool_style_override_switches_only_pool_base_style():
    # 机会池切短线：基础风格与窗口按 short_swing，但不改主账户风格来源。
    service = MarketOpportunityService(
        config=_service_config(paper_trading_style="position", market_opportunity_style="short_swing")
    )
    context = service._candidate_style_context(market="cn", stock_code="600001.SH")
    assert context["trading_style"] == "short_swing"
    assert context["source"] == "base_style_override"
    assert context["settings"]["analysis_window_sessions"] == 80


def test_pool_style_respects_per_symbol_override_over_pool_base():
    # 精确 market:symbol 覆盖优先于机会池基础风格。
    service = MarketOpportunityService(
        config=_service_config(
            paper_trading_style="position",
            market_opportunity_style="short_swing",
            paper_trading_style_overrides={"cn:600001": "trend"},
        )
    )
    context = service._candidate_style_context(market="cn", stock_code="600001.SH")
    assert context["trading_style"] == "trend"
    assert context["source"] == "paper_trading_style_override_config"
    assert context["matched_override_key"] == "cn:600001"


def test_pool_style_ignores_invalid_value_and_falls_back():
    # 非法机会池风格值被忽略，回落到 PAPER_TRADING_STYLE。
    service = MarketOpportunityService(
        config=_service_config(paper_trading_style="position", market_opportunity_style="bogus")
    )
    context = service._candidate_style_context(market="cn", stock_code="600001.SH")
    assert context["trading_style"] == "position"
    assert context["source"] == "paper_trading_style_config"


def test_candidate_analysis_style_uses_style_frozen_at_selection():
    # LLM 分析沿用候选入选时冻结的风格；复核候选读取原始入选风格；缺失时回落到当前机会池风格。
    service = MarketOpportunityService(
        config=_service_config(paper_trading_style="position", market_opportunity_style="trend")
    )
    frozen = SimpleNamespace(score_components_json=json.dumps({"trading_style": "short_swing"}))
    followup = SimpleNamespace(score_components={
        "origin_score": 80,
        "origin_components": {"trading_style": "short_swing"},
        "current_session_rescoring": "delegated_to_full_llm_reanalysis",
    })
    unresolved = SimpleNamespace(score_components_json=json.dumps({"trading_style": "unavailable"}))
    inherit = MarketOpportunityService(
        config=_service_config(paper_trading_style="position", market_opportunity_style="")
    )

    assert service._candidate_analysis_style(frozen) == "short_swing"
    assert service._candidate_analysis_style(followup) == "short_swing"
    assert service._candidate_analysis_style(unresolved) == "trend"
    assert inherit._candidate_analysis_style(unresolved) is None


def test_reuse_compares_report_style_with_candidate_frozen_style(isolated_db):
    # 入选后才改机会池风格：复用判断与实际分析一致，按候选冻结的风格比对。
    candidate = SimpleNamespace(candidate_key="cn:sh:600001", symbol="600001.SH", stock_code="600001",
                                snapshot_hash="frozen-hash", quote_snapshot_json='{"close": 10}',
                                score_components_json=json.dumps({"trading_style": "short_swing"}))
    selection = build_selection_context(candidate=candidate, job=object())
    service = MarketOpportunityService(
        config=_service_config(paper_trading_style="position", market_opportunity_style="position"),
        db_manager=isolated_db,
    )

    def reusable(style):
        context = {"analysis_phase": "postmarket", "as_of": "2026-06-30",
                   "paper_trading_style": style, "selection_context": selection}
        return service._same_day_report_is_reusable(
            SimpleNamespace(context_snapshot=json.dumps(context), raw_result='{"success": true}'),
            candidate=candidate, trade_date=date(2026, 6, 30),
        )

    assert reusable("short_swing")
    assert not reusable("position")


def test_candidate_below_analysis_threshold_remains_observing_not_rejected():
    service = MarketOpportunityService(config=_service_config(market_opportunity_min_20d_avg_amount_cny=0))
    dates = pd.bdate_range(end="2026-06-30", periods=180)
    bars = pd.DataFrame({
        "date": dates,
        "open": [10.0] * 180,
        "high": [10.3] * 180,
        "low": [9.8] * 180,
        "close": [10.0 + idx * 0.02 for idx in range(180)],
        "volume": [1_000_000] * 180,
        "amount": [200_000_000] * 180,
    })
    service._daily_bars = MagicMock(return_value=bars)
    service._benchmark_bars = MagicMock(return_value=bars)

    candidate = service._score_candidate(
        constituent={"code": "600001", "exchange": "sh", "symbol": "600001.SH", "name": "测试股份"},
        theme={"theme_key": "industry:半导体", "theme_name": "半导体", "score": 100},
        trade_date=date(2026, 6, 30),
        policy=service._policy(),
        threshold=999,
    )

    assert candidate["status"] == "observing"
    assert "below_analysis_threshold" in candidate["rejection_reasons"]
    assert candidate["score_components"]["strategy_comparison"]["current_selected"] is False


def test_trend_style_rejects_short_history_and_does_not_ignore_ma250():
    service = MarketOpportunityService(config=_service_config(paper_trading_style="trend"))
    bars = _FakeFetcherManager([]).get_daily_data("600001.SH")[0]
    service._daily_bars = MagicMock(return_value=bars)
    service._benchmark_bars = MagicMock(return_value=bars)
    candidate = service._score_candidate(
        constituent={"code": "600001", "exchange": "sh", "symbol": "600001.SH", "name": "测试"},
        theme={"theme_key": "industry:test", "theme_name": "test", "score": 100},
        trade_date=date(2026, 6, 30), policy=service._policy(), threshold=0,
    )
    assert candidate["status"] != "selected"
    assert "insufficient_daily_bars" in candidate["rejection_reasons"]
    payload = service._candidate_style_score_payload(
        close=12, ma5=11.5, ma10=11, ma20=10.5, ma60=10,
        style_context=service._candidate_style_context(market="cn", stock_code="600001.SH"),
        moving_averages={120: 15, 250: 20},
    )
    assert payload["trend_score"] == 4


def test_reuse_matches_frozen_payload_not_transient_ids_or_hash_alone(isolated_db):
    candidate = SimpleNamespace(candidate_key="cn:sh:600001", symbol="600001.SH", stock_code="600001",
                                snapshot_hash="legacy-hash", quote_snapshot_json='{"close": 10}')
    selection = build_selection_context(candidate=candidate, job=object())
    selection.update(candidate_id=900, run_id=901, job_id=902)
    context = {"analysis_phase": "postmarket", "as_of": "2026-06-30",
               "paper_trading_style": "position", "selection_context": selection}
    service = MarketOpportunityService(config=_service_config(paper_trading_style="position"), db_manager=isolated_db)
    def reusable():
        return service._same_day_report_is_reusable(
            SimpleNamespace(context_snapshot=json.dumps(context), raw_result='{"success": true}'),
            candidate=candidate, trade_date=date(2026, 6, 30),
        )
    assert reusable()
    selection["quote_snapshot"] = {"close": 11}
    assert not reusable()
    selection["quote_snapshot"] = {"close": 10}
    selection.pop("policy_version")
    assert not reusable()
    selection.update(build_selection_context(candidate=candidate, job=object()))
    candidate.snapshot_hash = ""
    selection["snapshot_hash"] = ""
    assert not reusable()


def test_theme_without_persistence_keeps_cold_start_evidence_and_legacy_rejection():
    service = MarketOpportunityService(
        config=_service_config(),
        fetcher_manager=_FakeFetcherManager(
            [
                {"code": f"600{i:03d}", "exchange": "sh", "symbol": f"600{i:03d}.SH", "name": f"股票{i}"}
                for i in range(1, 31)
            ]
        ),
        board_provider=_FakeBoardProvider(
            [
                {"code": f"600{i:03d}", "exchange": "sh", "symbol": f"600{i:03d}.SH", "name": f"股票{i}"}
                for i in range(1, 31)
            ],
            history_status="failed",
        ),
    )
    policy = service._policy()

    selected_themes, theme_rows, candidate_rows = service._score_themes_and_candidates(
        themes=[{
            "theme_key": "industry:半导体",
            "theme_type": "industry",
            "theme_name": "半导体",
            "theme_source": "eastmoney_industry",
            "theme_code": "BK1036",
            "rank": 1,
            "change_pct": 3.0,
        }],
        policy=policy,
        market="cn",
        trade_date=date(2026, 6, 30),
        regime="normal",
    )

    assert selected_themes
    assert candidate_rows
    assert theme_rows[0]["score_components"]["evidence_stage"] == "cold_start"
    assert "theme_persistence_unavailable" in theme_rows[0]["score_components"]["legacy_reasons"]


@pytest.mark.parametrize(
    "theme",
    [
        {
            "theme_key": "industry:医药制造业",
            "theme_type": "industry",
            "theme_name": "医药制造业",
            "rank": 1,
            "change_pct": 2.0,
        },
        {
            "theme_key": "industry:医药制造业",
            "theme_type": "industry",
            "theme_name": "医药制造业",
            "theme_source": "eastmoney_industry",
            "theme_code": "csrc_C27",
            "rank": 1,
            "change_pct": 2.0,
        },
        {
            "theme_key": "industry:医药制造业",
            "theme_type": "industry",
            "theme_name": "医药制造业",
            "theme_source": "csrc_tencent",
            "theme_code": "BK医药",
            "rank": 1,
            "change_pct": 2.0,
        },
        {
            "theme_key": "industry:医药制造业",
            "theme_type": "industry",
            "theme_name": "医药制造业",
            "theme_source": "csrc",
            "theme_code": "csrc_C27",
            "rank": 1,
            "change_pct": 2.0,
        },
        {
            "theme_key": "industry:医药制造业",
            "theme_type": "industry",
            "theme_name": "医药制造业",
            "theme_source": "eastmoney_industry",
            "theme_code": "BK医药",
            "rank": 1,
            "change_pct": 2.0,
        },
    ],
)
def test_industry_theme_without_coherent_source_code_is_rejected_before_member_lookup(theme):
    board_provider = MagicMock()
    board_provider.get_board_constituents.side_effect = AssertionError("unsafe name-only industry must not request members")
    service = MarketOpportunityService(
        config=_service_config(),
        board_provider=board_provider,
        fetcher_manager=_FakeFetcherManager([]),
    )

    selected, themes, candidates = service._score_themes_and_candidates(
        themes=[theme],
        policy=service._policy(),
        market="cn",
        trade_date=date(2026, 6, 30),
        regime="normal",
    )

    assert selected == []
    assert candidates == []
    assert themes[0]["status"] == "insufficient_data"
    assert themes[0]["rejection_reasons"] == ["source_identity_unavailable"]
    assert themes[0]["data_quality"]["snapshot_status"] == "not_requested"
    board_provider.get_board_constituents.assert_not_called()


def test_outer_manager_name_only_industry_ranking_is_rejected_after_report_serialization():
    class EmptyRankingFetcher:
        name = "akshare"

        def get_sector_rankings(self, n=5):
            return None

    class NameOnlyRankingFetcher:
        name = "efinance"

        def get_sector_rankings(self, n=5):
            return ([{"name": "医药制造业", "change_pct": 2.0}], [])

    manager = DataFetcherManager.__new__(DataFetcherManager)
    manager._fetchers = [EmptyRankingFetcher(), NameOnlyRankingFetcher()]
    top_sectors, bottom_sectors = manager.get_sector_rankings(5)

    from src.market_analyzer import MarketAnalyzer, MarketOverview

    analyzer = MarketAnalyzer.__new__(MarketAnalyzer)
    analyzer.region = "cn"
    analyzer.config = SimpleNamespace(report_language="zh")
    analyzer.profile = SimpleNamespace(has_market_stats=False)
    analyzer._get_output_language = MagicMock(return_value="zh")
    analyzer._split_report_sections = MagicMock(return_value=[])
    analyzer._extract_report_title = MagicMock(return_value="A股复盘")
    analyzer._get_review_title = MagicMock(return_value="A股复盘")
    analyzer._supports_market_light = MagicMock(return_value=False)
    analyzer._get_market_scope_name = MagicMock(return_value="A股")
    analyzer._normalize_news_item = MagicMock(side_effect=lambda item: item)
    payload = analyzer.build_market_review_payload(
        MarketOverview(date="2026-06-30", top_sectors=top_sectors, bottom_sectors=bottom_sectors),
        [],
        "A股复盘",
    )

    board_provider = MagicMock()
    board_provider.get_board_constituents.side_effect = AssertionError("name-only fallback ranking must not request members")
    service = MarketOpportunityService(
        config=_service_config(),
        board_provider=board_provider,
        fetcher_manager=_FakeFetcherManager([]),
    )
    themes = service._extract_themes(payload)
    selected, theme_rows, candidates = service._score_themes_and_candidates(
        themes=themes,
        policy=service._policy(),
        market="cn",
        trade_date=date(2026, 6, 30),
        regime="normal",
    )

    assert top_sectors == [{"name": "医药制造业", "change_pct": 2.0}]
    assert themes[0]["theme_key"] == "industry:医药制造业"
    assert themes[0]["theme_source"] is None
    assert selected == []
    assert candidates == []
    assert theme_rows[0]["status"] == "insufficient_data"
    assert theme_rows[0]["rejection_reasons"] == ["source_identity_unavailable"]
    board_provider.get_board_constituents.assert_not_called()


@pytest.mark.parametrize(
    ("source", "code"),
    [
        ("eastmoney_industry", "BK1036"),
        ("csrc_tencent", "csrc_C27"),
        ("sina_industry", "hangye_ZC27"),
        ("akshare_sina", "hangye_ZC27"),
    ],
)
def test_industry_theme_with_explicit_coherent_identity_can_request_matching_members(source, code):
    constituents = [
        {"code": f"600{i:03d}", "exchange": "sh", "symbol": f"600{i:03d}.SH", "name": f"股票{i}", "涨跌幅": 1.0, "成交额": 100_000_000, "量比": 1.3}
        for i in range(1, 31)
    ]
    board_provider = _FakeBoardProvider(constituents)
    board_provider.get_board_constituents = MagicMock(wraps=board_provider.get_board_constituents)
    service = MarketOpportunityService(
        config=_service_config(),
        fetcher_manager=_FakeFetcherManager(constituents),
        board_provider=board_provider,
    )

    service._score_themes_and_candidates(
        themes=[{
            "theme_key": "industry:医药制造业",
            "theme_type": "industry",
            "theme_name": "医药制造业",
            "theme_source": source,
            "theme_code": code,
            "rank": 1,
            "change_pct": 2.0,
        }],
        policy=service._policy(),
        market="cn",
        trade_date=date(2026, 6, 30),
        regime="normal",
    )

    board_provider.get_board_constituents.assert_called_once()
    assert board_provider.get_board_constituents.call_args.kwargs["board_code"] == code


def test_llm_payload_persistence_claim_cannot_bypass_observation_gate():
    constituents = [
        {"code": f"600{i:03d}", "exchange": "sh", "symbol": f"600{i:03d}.SH", "name": f"股票{i}", "涨跌幅": 1.0, "成交额": 100_000_000, "量比": 1.3}
        for i in range(1, 31)
    ]
    service = MarketOpportunityService(
        config=_service_config(),
        fetcher_manager=_FakeFetcherManager(constituents),
        board_provider=_FakeBoardProvider(constituents, history_status="failed"),
        repository=_PersistenceOnlyRepo([]),
    )
    policy = service._policy()

    selected_themes, theme_rows, candidate_rows = service._score_themes_and_candidates(
        themes=[
            {
                "theme_key": "concept:创新药",
                "theme_type": "concept",
                "theme_name": "创新药",
                "rank": 1,
                "change_pct": 3.0,
                "persistence_days": 5,
                "persistence": {"days": 5},
            }
        ],
        policy=policy,
        market="cn",
        trade_date=date(2026, 6, 30),
        regime="normal",
    )

    assert selected_themes
    assert candidate_rows
    assert theme_rows[0]["score_components"]["persistence_source"] == "persisted_theme_observations"
    assert theme_rows[0]["score_components"]["persistence"] == 0
    assert "theme_persistence_unavailable" in theme_rows[0]["score_components"]["legacy_reasons"]


def test_sina_fallback_theme_with_insufficient_observation_window_remains_observation_only():
    constituents = [
        {"code": f"600{i:03d}", "exchange": "sh", "symbol": f"600{i:03d}.SH", "name": f"股票{i}", "涨跌幅": 1.0, "成交额": 100_000_000, "量比": 1.3}
        for i in range(1, 31)
    ]
    service = MarketOpportunityService(
        config=_service_config(),
        fetcher_manager=_FakeFetcherManager(constituents),
        board_provider=_FakeBoardProvider(constituents, history_status="failed"),
        repository=_PersistenceOnlyRepo(
            [
                {"trade_date": date(2026, 6, 25), "present": True, "provider_matches": True, "change_pct": 1.0},
                {"trade_date": date(2026, 6, 26), "present": True, "provider_matches": True, "change_pct": 1.0},
                {"trade_date": date(2026, 6, 29), "present": True, "provider_matches": True, "change_pct": 1.0},
            ]
        ),
    )
    policy = service._policy()

    selected_themes, theme_rows, candidate_rows = service._score_themes_and_candidates(
        themes=[{"theme_key": "concept:创新药", "theme_type": "concept", "theme_name": "创新药", "rank": 1, "change_pct": 3.0}],
        policy=policy,
        market="cn",
        trade_date=date(2026, 6, 30),
        regime="normal",
    )

    assert selected_themes
    assert candidate_rows
    assert theme_rows[0]["score_components"]["evidence_stage"] == "cold_start"
    assert "theme_persistence_unavailable" in theme_rows[0]["score_components"]["legacy_reasons"]
    assert theme_rows[0]["score_components"]["persistence_source"] == "persisted_theme_observations"
    assert theme_rows[0]["score_components"]["available_window_size"] == 4


@pytest.mark.parametrize("source", ["akshare_sina", "csrc_tencent"])
def test_non_eastmoney_membership_never_uses_same_name_eastmoney_history(source):
    from dataclasses import replace
    constituents = [
        {"code": f"600{i:03d}", "exchange": "sh", "symbol": f"600{i:03d}.SH",
         "name": f"股票{i}", "涨跌幅": 1.0, "成交额": 100_000_000}
        for i in range(1, 11)
    ]
    provider = _FakeBoardProvider(constituents)
    snapshot = replace(provider.get_board_constituents(board_type="industry", board_name="医药制造业"), provider=source)
    provider.get_board_history = MagicMock(side_effect=AssertionError("must not cross taxonomy"))
    service = MarketOpportunityService(
        config=_service_config(), board_provider=provider,
        fetcher_manager=_FakeFetcherManager(constituents), repository=_PersistenceOnlyRepo([]),
        board_observation_service=_FakeBoardObservationService(
            {
                "days": None,
                "source": "csrc_tencent",
                "window_dates": [],
                "positive_dates": [],
                "required_window_size": 5,
                "available_window_size": 1,
                "missing_dates": [],
                "return_5d": None,
                "return_20d": None,
            }
        ),
    )
    _, components, reasons = service._score_theme(
        {"theme_key": "industry:医药制造业", "rank": 1, "change_pct": 2}, snapshot,
    )
    provider.get_board_history.assert_not_called()
    assert components["persistence_source"] == ("csrc_tencent" if source == "csrc_tencent" else "persisted_theme_observations")
    assert components["evidence_stage"] == "cold_start"
    assert "theme_persistence_unavailable" not in reasons
    assert "theme_persistence_unavailable" in components["legacy_reasons"]


def test_fixed_observation_window_allows_candidates_only_after_three_of_five_days():
    constituents = [
        {"code": f"600{i:03d}", "exchange": "sh", "symbol": f"600{i:03d}.SH", "name": f"股票{i}", "涨跌幅": 1.0, "成交额": 100_000_000, "量比": 1.3}
        for i in range(1, 31)
    ]
    service = MarketOpportunityService(
        config=_service_config(),
        fetcher_manager=_FakeFetcherManager(constituents),
        board_provider=_FakeBoardProvider(constituents, history_status="failed"),
        repository=_PersistenceOnlyRepo(
            [
                {"trade_date": date(2026, 6, 24), "present": True, "provider_matches": True, "change_pct": 1.0},
                {"trade_date": date(2026, 6, 25), "present": False, "provider_matches": False, "change_pct": None},
                {"trade_date": date(2026, 6, 26), "present": True, "provider_matches": True, "change_pct": -1.0},
                {"trade_date": date(2026, 6, 29), "present": True, "provider_matches": True, "change_pct": 1.0},
            ]
        ),
    )
    policy = service._policy()

    selected_themes, theme_rows, candidate_rows = service._score_themes_and_candidates(
        themes=[{"theme_key": "concept:创新药", "theme_type": "concept", "theme_name": "创新药", "rank": 1, "change_pct": 3.0}],
        policy=policy,
        market="cn",
        trade_date=date(2026, 6, 30),
        regime="normal",
    )

    assert selected_themes
    assert theme_rows[0]["score_components"]["persistence_days"] == 3
    assert theme_rows[0]["score_components"]["persistence_window_dates"] == [
        "2026-06-24",
        "2026-06-25",
        "2026-06-26",
        "2026-06-29",
        "2026-06-30",
    ]
    assert theme_rows[0]["score_components"]["persistence_positive_dates"] == [
        "2026-06-24",
        "2026-06-29",
        "2026-06-30",
    ]
    assert candidate_rows


def test_candidate_without_benchmark_relative_strength_fails_closed():
    service = MarketOpportunityService(
        config=_service_config(),
        fetcher_manager=_FakeFetcherManager([]),
    )
    service._benchmark_bars = MagicMock(return_value=None)
    candidate = service._score_candidate(
        constituent={"code": "600001", "exchange": "sh", "symbol": "600001.SH", "name": "测试股份"},
        theme={"theme_key": "industry:半导体", "theme_name": "半导体", "score": 90},
        trade_date=date(2026, 6, 30),
        policy=service._policy(),
        threshold=70,
    )

    assert candidate["status"] == "rejected"
    assert "relative_strength_benchmark_unavailable" in candidate["rejection_reasons"]


def test_blacklist_matches_theme_names_and_exact_stock_identity_only():
    service = MarketOpportunityService(
        config=_service_config(market_opportunity_blacklist=["高风险主题", "600001.SH", "600002"]),
        fetcher_manager=_FakeFetcherManager([]),
    )
    policy = service._policy()

    assert service._theme_is_blacklisted(
        {"theme_key": "concept:高风险主题", "theme_name": "高风险主题"},
        policy,
    )

    exact = service._score_candidate(
        constituent={"code": "600001", "exchange": "sh", "symbol": "600001.SH", "name": "测试股份"},
        theme={"theme_key": "industry:半导体", "theme_name": "半导体", "score": 90},
        trade_date=date(2026, 6, 30),
        policy=policy,
        threshold=70,
    )
    bare = service._score_candidate(
        constituent={"code": "600002", "exchange": "sh", "symbol": "600002.SH", "name": "测试股份"},
        theme={"theme_key": "industry:半导体", "theme_name": "半导体", "score": 90},
        trade_date=date(2026, 6, 30),
        policy=policy,
        threshold=70,
    )

    assert "blacklisted" in exact["rejection_reasons"]
    assert "blacklisted" not in bare["rejection_reasons"]


def test_build_jobs_uses_every_configured_model_with_strict_budget():
    service = MarketOpportunityService(config=_service_config(market_opportunity_max_llm_calls_per_run=4))
    policy = service._policy()
    candidates = [
        {"candidate_key": "cn:sh:600001", "status": "selected"},
        {"candidate_key": "cn:sh:600002", "status": "selected"},
        {"candidate_key": "cn:sh:600003", "status": "selected"},
    ]

    jobs = service._build_jobs(run_id=7, candidates=candidates, models=["model-a", "model-b"], policy=policy)

    assert len(jobs) == 4
    assert {job["model"] for job in jobs} == {"model-a", "model-b"}
    assert {job["candidate_key"] for job in jobs} == {"cn:sh:600001", "cn:sh:600002"}


def test_process_pending_jobs_reconciles_completed_report_through_paper_service():
    candidate = SimpleNamespace(
        id=11,
        run_id=3,
        stock_code="600001",
        exchange="sh",
        symbol="600001.SH",
        as_of=date(2026, 6, 30),
        candidate_key="cn:sh:600001",
        primary_theme_key="industry:半导体",
        score=91.5,
    )
    job = SimpleNamespace(
        id=21,
        run_id=3,
        candidate_id=11,
        query_id=None,
        job_key="job-key",
        model="deepseek/deepseek-v4-pro",
    )
    analysis_service = SimpleNamespace(analyze_stock=MagicMock(return_value={"ok": True}))
    repo = _FakeRepo([job], candidate)
    service = MarketOpportunityService(
        config=_service_config(),
        repository=repo,
        analysis_service_factory=lambda: analysis_service,
    )
    service.runtime_settings_service.get_market_opportunity_mode = MagicMock(
        return_value=SimpleNamespace(value="paper", source="test", version=0)
    )
    service._same_day_stock_report_id = MagicMock(return_value=None)
    service._analysis_history_id_for_query = MagicMock(return_value=101)
    service._attach_opportunity_provenance = MagicMock()
    service._reconcile_report_to_paper_plan = MagicMock(return_value={"status": "completed", "signal_id": 201, "plan_id": 301})
    service._refresh_run_status = MagicMock()

    stats = service.process_pending_jobs(owner="owner", limit=1)

    assert stats == {"status": "ok", "processed": 1, "failed": 0, "uncertain_submitted": 0}
    analysis_service.analyze_stock.assert_called_once()
    _, kwargs = analysis_service.analyze_stock.call_args
    assert kwargs["query_source"] == "market_opportunity"
    assert kwargs["selection_context"]["candidate_key"] == "cn:sh:600001"
    assert "buy mandate" in kwargs["selection_context"]["instruction"]
    assert kwargs["model"] == "deepseek/deepseek-v4-pro"
    assert kwargs["send_notification"] is False
    assert repo.submitted == [(21, "owner", "market_opp_21")]
    service._reconcile_report_to_paper_plan.assert_called_once_with(
        101,
        trigger_source="market_opportunity_report_completed",
    )
    completion = repo.completed[-1][1]
    assert completion["analysis_history_id"] == 101
    assert completion["evidence"]["paper_reconciliation"]["signal_id"] == 201
    assert completion["evidence"]["paper_reconciliation"]["plan_id"] == 301


def test_paper_job_analysis_uses_candidate_frozen_style():
    # 进主账户前的分析按候选入选时冻结的风格，计划随报告冻结该风格的止损与持有期。
    candidate = SimpleNamespace(
        id=11,
        run_id=3,
        stock_code="600001",
        exchange="sh",
        symbol="600001.SH",
        as_of=date(2026, 6, 30),
        candidate_key="cn:sh:600001",
        primary_theme_key="industry:半导体",
        score=91.5,
        score_components_json=json.dumps({"trading_style": "short_swing"}),
    )
    job = SimpleNamespace(id=21, run_id=3, candidate_id=11, query_id=None, job_key="job-key", model="model-a")
    analysis_service = SimpleNamespace(analyze_stock=MagicMock(return_value={"ok": True}))
    service = MarketOpportunityService(
        config=_service_config(paper_trading_style="position"),
        repository=_FakeRepo([job], candidate),
        analysis_service_factory=lambda: analysis_service,
    )
    service.runtime_settings_service.get_market_opportunity_mode = MagicMock(
        return_value=SimpleNamespace(value="paper", source="test", version=0)
    )
    service._same_day_stock_report_id = MagicMock(return_value=None)
    service._analysis_history_id_for_query = MagicMock(return_value=101)
    service._attach_opportunity_provenance = MagicMock()
    service._reconcile_report_to_paper_plan = MagicMock(return_value={"status": "completed", "signal_id": 201, "plan_id": 301})
    service._refresh_run_status = MagicMock()

    stats = service.process_pending_jobs(owner="owner", limit=1)

    assert stats["processed"] == 1
    kwargs = analysis_service.analyze_stock.call_args.kwargs
    assert kwargs["trading_style_override"] == "short_swing"
    assert kwargs["execution_intent"]["execution_intent"] == "paper_candidate"


def test_submitted_job_recovery_uses_query_id_before_reinvoking_llm():
    candidate = SimpleNamespace(
        id=11,
        run_id=3,
        stock_code="600001",
        exchange="sh",
        symbol="600001.SH",
        as_of=date(2026, 6, 30),
        candidate_key="cn:sh:600001",
        primary_theme_key="industry:半导体",
        score=91.5,
    )
    submitted_job = SimpleNamespace(
        id=22,
        run_id=3,
        candidate_id=11,
        query_id="market_opp_22",
        job_key="job-key",
        model="deepseek/deepseek-v4-pro",
    )
    analysis_service = SimpleNamespace(analyze_stock=MagicMock())
    repo = _FakeRepo([], candidate, submitted_jobs=[submitted_job])
    service = MarketOpportunityService(
        config=_service_config(),
        repository=repo,
        analysis_service_factory=lambda: analysis_service,
    )
    service.runtime_settings_service.get_market_opportunity_mode = MagicMock(
        return_value=SimpleNamespace(value="paper", source="test", version=0)
    )
    service._analysis_history_id_for_query = MagicMock(return_value=102)
    service._attach_opportunity_provenance = MagicMock()
    service._reconcile_report_to_paper_plan = MagicMock(return_value={"status": "completed", "signal_id": 202, "plan_id": 302})
    service._refresh_run_status = MagicMock()

    stats = service.process_pending_jobs(
        owner="owner",
        limit=1,
        run_id=3,
        effective_session=date(2026, 6, 30),
    )

    assert stats["processed"] == 1
    assert stats["uncertain_submitted"] == 0
    analysis_service.analyze_stock.assert_not_called()
    assert repo.completed[-1][1]["analysis_history_id"] == 102


def test_process_pending_jobs_marks_sync_failure_retryable_not_submitted():
    candidate = SimpleNamespace(
        id=11,
        run_id=3,
        stock_code="600001",
        exchange="sh",
        symbol="600001.SH",
        as_of=date(2026, 6, 30),
        candidate_key="cn:sh:600001",
        primary_theme_key="industry:半导体",
        score=91.5,
    )
    job = SimpleNamespace(
        id=21,
        run_id=3,
        candidate_id=11,
        query_id=None,
        job_key="job-key",
        model="deepseek/deepseek-v4-pro",
    )
    analysis_service = SimpleNamespace(analyze_stock=MagicMock(return_value=None))
    repo = _FakeRepo([job], candidate)
    service = MarketOpportunityService(
        config=_service_config(),
        repository=repo,
        analysis_service_factory=lambda: analysis_service,
    )
    service.runtime_settings_service.get_market_opportunity_mode = MagicMock(
        return_value=SimpleNamespace(value="paper", source="test", version=0)
    )
    service._same_day_stock_report_id = MagicMock(return_value=None)
    service._analysis_history_id_for_query = MagicMock(return_value=None)
    service._refresh_run_status = MagicMock()

    stats = service.process_pending_jobs(owner="owner", limit=1, effective_session=date(2026, 6, 30))

    assert stats == {"status": "ok", "processed": 0, "failed": 1, "uncertain_submitted": 0}
    assert repo.completed[-1][1]["status"] == "failed_retryable"
    assert repo.completed[-1][1]["failure_reason"] == "analysis_report_missing_after_sync_call"


def test_after_market_review_recovers_stale_submitted_jobs_across_runs(isolated_db):
    repo = MarketOpportunityRepository(isolated_db)
    stale_date = date(2026, 8, 31)
    current_date = date(2026, 9, 1)
    run = repo.create_or_get_run({
        "run_key": "stale-submitted-run",
        "market": "cn",
        "trade_date": stale_date,
        "source_report_id": 1,
        "market_regime": "normal",
        "mode": "paper",
        "status": "analyzing",
        "policy_version": "opportunity-policy-v2",
        "config_snapshot": {},
        "provenance": {},
        "llm_budget": 1,
    })
    repo.replace_run_children(
        run_id=int(run.id),
        themes=[],
        candidates=[{
            "candidate_key": "cn:sh:600001",
            "market": "cn",
            "exchange": "sh",
            "stock_code": "600001",
            "symbol": "600001.SH",
            "status": "selected",
            "rank": 1,
            "as_of": stale_date,
            "valid_until": stale_date,
        }],
        jobs=[{
            "candidate_key": "cn:sh:600001",
            "job_key": "stale-submitted-job",
            "model": "model-a",
            "status": "pending",
        }],
    )
    job_id = int(repo.get_run_detail(int(run.id))["analysis_jobs"][0]["id"])
    with isolated_db.session_scope() as session:
        job = session.get(MarketOpportunityAnalysisJob, job_id)
        job.status = "submitted"
        job.query_id = "stale-query"

    service = MarketOpportunityService(
        config=_service_config(),
        db_manager=isolated_db,
        repository=repo,
    )
    service.discover_from_latest_market_review = MagicMock(
        return_value={"status": "completed", "trade_date": current_date.isoformat(), "job_count": 0}
    )
    service._materialize_due_followup_runs = MagicMock(return_value={"status": "none_due", "runs": [], "expired": 0})
    service._analysis_history_id_for_query = MagicMock(return_value=None)

    with patch("src.core.trading_calendar.get_effective_trading_date", return_value=current_date), patch(
        "src.services.daily_market_context.market_context_freshness_expires_at",
        return_value=datetime(2026, 9, 2, 7, 0),
    ):
        result = service.run_after_market_review(market="cn", current_time=datetime(2026, 9, 1, 15, 20))

    assert result["submitted_recovery"]["failed"] == 1
    assert result["status"] == "ok"
    updated = repo.get_job(job_id)
    assert updated["status"] == "failed"
    assert updated["failure_reason"] == "submitted_job_session_expired"


def test_recover_expired_submitted_job_links_report_without_current_paper_plan(isolated_db):
    from src.storage import AnalysisHistory

    repo = MarketOpportunityRepository(isolated_db)
    stale_date = date(2026, 8, 31)
    current_date = date(2026, 9, 1)
    with isolated_db.session_scope() as session:
        session.add(
            AnalysisHistory(
                query_id="expired-but-finished",
                code="600001.SH",
                name="测试股份",
                report_type="detailed",
                model_used="model-a",
                raw_result='{"success": true}',
                context_snapshot="{}",
                created_at=datetime(2026, 8, 31, 15, 20),
            )
        )
    run = repo.create_or_get_run({
        "run_key": "expired-submitted-with-report",
        "market": "cn",
        "trade_date": stale_date,
        "source_report_id": 1,
        "market_regime": "normal",
        "mode": "paper",
        "status": "analyzing",
        "policy_version": "opportunity-policy-v2",
        "config_snapshot": {},
        "provenance": {},
        "llm_budget": 1,
    })
    repo.replace_run_children(
        run_id=int(run.id),
        themes=[],
        candidates=[{
            "candidate_key": "cn:sh:600001",
            "market": "cn",
            "exchange": "sh",
            "stock_code": "600001",
            "symbol": "600001.SH",
            "status": "selected",
            "rank": 1,
            "as_of": stale_date,
            "valid_until": stale_date,
        }],
        jobs=[{
            "candidate_key": "cn:sh:600001",
            "job_key": "expired-submitted-with-report-job",
            "model": "model-a",
            "status": "pending",
        }],
    )
    job_id = int(repo.get_run_detail(int(run.id))["analysis_jobs"][0]["id"])
    with isolated_db.session_scope() as session:
        job = session.get(MarketOpportunityAnalysisJob, job_id)
        job.status = "submitted"
        job.query_id = "expired-but-finished"

    service = MarketOpportunityService(
        config=_service_config(),
        db_manager=isolated_db,
        repository=repo,
    )
    service._reconcile_report_to_paper_plan = MagicMock()

    result = service._recover_submitted_jobs(
        run_id=int(run.id),
        limit=10,
        effective_session=current_date,
    )

    assert result == {"processed": 1, "failed": 0, "uncertain": 0}
    service._reconcile_report_to_paper_plan.assert_not_called()
    updated = repo.get_job(job_id)
    assert updated["status"] == "completed"
    assert updated["analysis_history_id"] is not None
    assert updated["failure_reason"] == "submitted_job_session_expired_after_report"
    assert updated["evidence"]["paper_reconciliation_skipped"] == "expired_session"


def test_submitted_recovery_deadline_is_bounded_without_reinvoking_llm():
    candidate = SimpleNamespace(
        id=11,
        run_id=3,
        stock_code="600001",
        exchange="sh",
        symbol="600001.SH",
        as_of=date(2026, 6, 30),
        candidate_key="cn:sh:600001",
        primary_theme_key="industry:半导体",
        score=91.5,
    )
    submitted_job = SimpleNamespace(
        id=22,
        run_id=3,
        candidate_id=11,
        query_id="market_opp_22",
        job_key="job-key",
        model="deepseek/deepseek-v4-pro",
        updated_at=datetime(2026, 6, 30, 9, 0),
    )
    analysis_service = SimpleNamespace(analyze_stock=MagicMock())
    repo = _FakeRepo([], candidate, submitted_jobs=[submitted_job])
    service = MarketOpportunityService(
        config=_service_config(),
        repository=repo,
        analysis_service_factory=lambda: analysis_service,
    )
    service.runtime_settings_service.get_market_opportunity_mode = MagicMock(
        return_value=SimpleNamespace(value="paper", source="test", version=0)
    )
    service._analysis_history_id_for_query = MagicMock(return_value=None)

    with patch(
        "src.services.market_opportunity_service.utc_naive_now",
        return_value=datetime(2026, 6, 30, 9, 16),
    ):
        stats = service.process_pending_jobs(
            owner="owner",
            limit=1,
            run_id=3,
            effective_session=date(2026, 6, 30),
        )

    assert stats == {"status": "ok", "processed": 0, "failed": 1, "uncertain_submitted": 0}
    analysis_service.analyze_stock.assert_not_called()
    assert repo.completed[-1][1]["status"] == "failed"
    assert repo.completed[-1][1]["failure_reason"] == "submitted_job_recovery_deadline_expired"


def test_after_market_review_skips_submitted_recovery_when_calendar_untrusted(isolated_db):
    service = MarketOpportunityService(config=_service_config(), db_manager=isolated_db)
    service.discover_from_latest_market_review = MagicMock(
        return_value={"status": "completed", "trade_date": "2026-09-01", "job_count": 0}
    )
    service._materialize_due_followup_runs = MagicMock(return_value={"status": "none_due", "runs": [], "expired": 0})
    service._recover_submitted_jobs = MagicMock()

    with patch("src.core.trading_calendar.get_effective_trading_date", return_value=date(2026, 9, 1)), patch(
        "src.services.daily_market_context.market_context_freshness_expires_at",
        return_value=None,
    ):
        result = service.run_after_market_review(market="cn", current_time=datetime(2026, 9, 1, 15, 20))

    assert result["submitted_recovery"]["skipped_reason"] == "current_session_unavailable"
    service._recover_submitted_jobs.assert_not_called()


def test_after_market_review_recovers_submitted_before_discovery_failure(isolated_db):
    service = MarketOpportunityService(config=_service_config(), db_manager=isolated_db)
    service._recover_submitted_jobs = MagicMock(return_value={"processed": 0, "failed": 1, "uncertain": 0})
    service.discover_from_latest_market_review = MagicMock(side_effect=RuntimeError("source unavailable"))

    with patch("src.core.trading_calendar.get_effective_trading_date", return_value=date(2026, 9, 1)), patch(
        "src.services.daily_market_context.market_context_freshness_expires_at",
        return_value=datetime(2026, 9, 2, 7, 0),
    ), pytest.raises(RuntimeError):
        service.run_after_market_review(market="cn", current_time=datetime(2026, 9, 1, 15, 20))

    service._recover_submitted_jobs.assert_called_once_with(
        run_id=None,
        limit=200,
        effective_session=date(2026, 9, 1),
    )


def test_next_session_followup_is_durable_idempotent_and_fully_reanalyzed(isolated_db):
    from src.storage import AnalysisHistory

    origin_date = date(2026, 9, 7)
    due_date = date(2026, 9, 8)
    with isolated_db.session_scope() as session:
        market_report = AnalysisHistory(
            query_id="market-0908",
            code="MARKET",
            name="A股大盘",
            report_type="market_review",
            model_used="model-a",
            raw_result='{"success": true}',
            context_snapshot='{"market_review_region":"cn"}',
            created_at=datetime(2026, 9, 8, 15, 10),
        )
        stock_report = AnalysisHistory(
            query_id="stock-0908",
            code="600001.SH",
            name="测试股份",
            report_type="detailed",
            model_used="model-a",
            raw_result='{"success": true}',
            context_snapshot='{}',
            created_at=datetime(2026, 9, 8, 15, 20),
        )
        session.add_all([market_report, stock_report])
        session.flush()
        market_report_id = int(market_report.id)
        stock_report_id = int(stock_report.id)

    repo = MarketOpportunityRepository(isolated_db)
    origin_run = repo.create_or_get_run({
        "run_key": "origin-followup-test",
        "market": "cn",
        "trade_date": origin_date,
        "source_report_id": market_report_id,
        "market_regime": "normal",
        "mode": "paper",
        "status": "completed",
        "policy_version": "opportunity-policy-v2",
        "config_snapshot": {},
        "provenance": {"source": "unit"},
        "llm_budget": 1,
    })
    repo.replace_run_children(
        run_id=int(origin_run.id),
        themes=[],
        candidates=[{
            "candidate_key": "cn:sh:600001",
            "market": "cn",
            "exchange": "sh",
            "stock_code": "600001",
            "symbol": "600001.SH",
            "stock_name": "测试股份",
            "primary_theme_key": "industry:test",
            "related_themes": [{"theme_key": "industry:test"}],
            "status": "selected",
            "rank": 1,
            "score": 85,
            "score_components": {"trend": 20},
            "data_quality": {"source": "unit"},
            "quote_snapshot": {"close": 10},
            "as_of": origin_date,
            "valid_until": due_date,
            "snapshot_hash": "origin-snapshot",
        }],
        jobs=[{
            "candidate_key": "cn:sh:600001",
            "job_key": "origin-job",
            "model": "model-a",
            "status": "pending",
            "max_attempts": 2,
        }],
    )
    detail = repo.get_run_detail(int(origin_run.id))
    origin_job = detail["analysis_jobs"][0]
    repo.complete_job(
        int(origin_job["id"]),
        status="completed",
        analysis_history_id=stock_report_id,
        evidence={
            "source": "market_opportunity",
            "followup": {
                "policy_version": "opportunity-followup-v1",
                "status": "scheduled",
                "due_date": due_date.isoformat(),
                "expires_on": due_date.isoformat(),
                "origin_job_id": int(origin_job["id"]),
                "origin_report_id": stock_report_id,
                "origin_candidate_id": int(detail["candidates"][0]["id"]),
                "origin_run_id": int(origin_run.id),
                "model": "model-a",
                "confirmation_current": 1,
                "confirmation_required": 2,
                "reason": "awaiting_entry_confirmation",
            },
        },
    )

    analysis_service = SimpleNamespace(analyze_stock=MagicMock(return_value={"ok": True}))
    service = MarketOpportunityService(
        config=_service_config(),
        db_manager=isolated_db,
        repository=repo,
        analysis_service_factory=lambda: analysis_service,
    )
    report = service._get_market_review(market_report_id)
    service._latest_market_review = MagicMock(return_value=report)
    service._extract_market_payload = MagicMock(return_value={"date": due_date.isoformat()})
    service._postmarket_report_gate = MagicMock(return_value={"ok": True})
    service._latest_regime = MagicMock(return_value=None)

    first = service._materialize_due_followup_runs(market="cn", current_time=datetime(2026, 9, 8, 15, 20))
    second = service._materialize_due_followup_runs(market="cn", current_time=datetime(2026, 9, 8, 15, 25))

    assert len(first["runs"]) == 1
    assert len(second["runs"]) == 1
    assert first["runs"][0]["run_id"] == second["runs"][0]["run_id"]
    followup_run_id = first["runs"][0]["run_id"]
    followup_detail = repo.get_run_detail(followup_run_id)
    assert len(followup_detail["analysis_jobs"]) == 1
    assert followup_detail["candidates"][0]["valid_until"] == due_date.isoformat()
    assert followup_detail["candidates"][0]["data_quality"]["followup_reanalysis"] is True

    service._same_day_stock_report_id = MagicMock(return_value=None)
    service._analysis_history_id_for_query = MagicMock(return_value=stock_report_id)
    service._attach_opportunity_provenance = MagicMock()
    service._reconcile_report_to_paper_plan = MagicMock(return_value={
        "status": "completed",
        "classification": "stock_report_plan_reconciled",
        "executable": True,
        "plan_status": "active",
        "item": {
            "executable": True,
            "status": "active",
            "strategy_thesis_transition": {
                "reason": "accepted_entry_thesis",
                "confirmation_count": 2,
                "evidence": {"effective_confirmation_required": 2},
            },
        },
    })

    processed = service.process_pending_jobs(
        owner="followup-test",
        limit=1,
        run_id=followup_run_id,
        effective_session=due_date,
    )

    assert processed["processed"] == 1
    kwargs = analysis_service.analyze_stock.call_args.kwargs
    assert kwargs["force_refresh"] is True
    assert kwargs["analysis_phase"] == "postmarket"
    assert kwargs["selection_context"]["data_quality"]["followup_reanalysis"] is True
    completed_detail = repo.get_run_detail(followup_run_id)
    completed_evidence = completed_detail["analysis_jobs"][0]["evidence"]
    assert "followup" not in completed_evidence
    origin_after = repo.get_job(int(origin_job["id"]))
    assert origin_after["evidence"]["followup"]["status"] == "completed"
    assert origin_after["evidence"]["followup"]["result"] == "confirmed"


def test_missed_followup_session_expires_without_late_replay(isolated_db):
    from src.storage import AnalysisHistory

    repo = MarketOpportunityRepository(isolated_db)
    origin_date = date(2026, 9, 7)
    due_date = date(2026, 9, 8)
    observed_date = date(2026, 9, 9)
    with isolated_db.session_scope() as session:
        market_report = AnalysisHistory(
            query_id="market-followup-expiry",
            code="MARKET",
            name="A股大盘",
            report_type="market_review",
            model_used="market-review",
            context_snapshot=json.dumps({"market_review_payload": {"date": observed_date.isoformat()}}),
            created_at=datetime(2026, 9, 9, 15, 15),
        )
        stock_report = AnalysisHistory(
            query_id="origin-followup-expiry",
            code="600001.SH",
            name="测试股份",
            report_type="detailed",
            model_used="model-a",
            raw_result='{"success": true}',
            created_at=datetime(2026, 9, 7, 15, 20),
        )
        session.add_all([market_report, stock_report])
        session.flush()
        market_report_id = int(market_report.id)
        stock_report_id = int(stock_report.id)

    origin_run = repo.create_or_get_run({
        "run_key": "origin-expired-followup",
        "market": "cn",
        "trade_date": origin_date,
        "source_report_id": market_report_id,
        "market_regime": "normal",
        "mode": "paper",
        "status": "analyzing",
        "policy_version": "opportunity-policy-v2",
        "config_snapshot": {},
        "provenance": {},
        "llm_budget": 1,
    })
    repo.replace_run_children(
        run_id=int(origin_run.id),
        themes=[],
        candidates=[{
            "candidate_key": "cn:sh:600001",
            "market": "cn",
            "exchange": "sh",
            "stock_code": "600001",
            "symbol": "600001.SH",
            "stock_name": "测试股份",
            "status": "selected",
            "rank": 1,
            "score": 80,
            "as_of": origin_date,
            "valid_until": due_date,
            "snapshot_hash": "expired-origin",
        }],
        jobs=[{
            "candidate_key": "cn:sh:600001",
            "job_key": "origin-expired-job",
            "model": "model-a",
            "status": "pending",
        }],
    )
    detail = repo.get_run_detail(int(origin_run.id))
    origin_job = detail["analysis_jobs"][0]
    origin_candidate_id = int(detail["candidates"][0]["id"])
    repo.complete_job(
        int(origin_job["id"]),
        status="completed",
        analysis_history_id=stock_report_id,
        evidence={
            "followup": {
                "policy_version": "opportunity-followup-v1",
                "status": "scheduled",
                "due_date": due_date.isoformat(),
                "expires_on": due_date.isoformat(),
                "origin_job_id": int(origin_job["id"]),
                "origin_report_id": stock_report_id,
                "origin_candidate_id": origin_candidate_id,
                "origin_run_id": int(origin_run.id),
                "model": "model-a",
                "confirmation_current": 1,
                "confirmation_required": 2,
                "reason": "awaiting_entry_confirmation",
            }
        },
    )
    service = MarketOpportunityService(
        config=_service_config(),
        db_manager=isolated_db,
        repository=repo,
    )
    service._latest_market_review = MagicMock(
        return_value=SimpleNamespace(id=market_report_id, created_at=datetime(2026, 9, 9, 15, 15))
    )
    service._extract_market_payload = MagicMock(return_value={"date": due_date.isoformat()})
    service._postmarket_report_gate = MagicMock(return_value={"ok": True})
    service._latest_regime = MagicMock(return_value=None)

    spawned = service._materialize_due_followup_runs(
        market="cn",
        current_time=datetime(2026, 9, 8, 15, 20),
    )
    spawned_run_id = int(spawned["runs"][0]["run_id"])
    spawned_detail = repo.get_run_detail(spawned_run_id)
    child_job_id = int(spawned_detail["analysis_jobs"][0]["id"])
    with isolated_db.session_scope() as session:
        child = session.get(MarketOpportunityAnalysisJob, child_job_id)
        child.status = "submitted"
        child.query_id = "unresolved-followup"

    service._extract_market_payload.return_value = {"date": observed_date.isoformat()}

    result = service._materialize_due_followup_runs(
        market="cn",
        current_time=datetime(2026, 9, 9, 15, 20),
    )

    assert result["runs"] == []
    assert result["expired"] == 1
    origin_followup = repo.get_job(int(origin_job["id"]))["evidence"]["followup"]
    assert origin_followup["status"] == "expired"
    assert origin_followup["result"] == "frozen"
    assert origin_followup["reason"] == "followup_not_completed_by_expiry"
    assert repo.get_job(child_job_id)["status"] == "failed"
    assert repo.get_job(child_job_id)["failure_reason"] == "followup_not_completed_by_expiry"
    assert repo.get_run_detail(spawned_run_id)["status"] == "blocked"


def test_repeated_discover_reuses_existing_children_without_replacing_sqlite(isolated_db):
    from src.services.market_opportunity_service import OPPORTUNITY_POLICY_VERSION
    repo = MarketOpportunityRepository(isolated_db)
    trade_date = date(2026, 8, 31)
    run = repo.create_or_get_run(
        {
            "run_key": f"market-opportunity:77:paper:{OPPORTUNITY_POLICY_VERSION}",
            "market": "cn",
            "trade_date": trade_date,
            "source_report_id": 77,
            "market_regime_id": 1,
            "market_regime": "normal",
            "mode": "paper",
            "status": "analyzing",
            "policy_version": OPPORTUNITY_POLICY_VERSION,
            "config_snapshot": {},
            "provenance": {"source": "unit"},
            "llm_budget": 1,
            "started_at": datetime(2026, 8, 31, 15, 30),
        }
    )
    repo.replace_run_children(
        run_id=int(run.id),
        themes=[
            {
                "theme_key": "industry:半导体",
                "theme_type": "industry",
                "theme_name": "半导体",
                "status": "selected",
                "score": 88,
                "as_of": trade_date,
            }
        ],
        candidates=[
            {
                "candidate_key": "cn:sh:600001",
                "market": "cn",
                "exchange": "sh",
                "stock_code": "600001",
                "symbol": "600001.SH",
                "stock_name": "测试股份",
                "primary_theme_key": "industry:半导体",
                "status": "selected",
                "rank": 1,
                "score": 91,
                "as_of": trade_date,
                "valid_until": trade_date + timedelta(days=1),
            }
        ],
        jobs=[
            {
                "candidate_key": "cn:sh:600001",
                "job_key": "job-existing",
                "model": "model-a",
                "status": "pending",
            }
        ],
    )
    detail = repo.get_run_detail(int(run.id))
    job_id = int(detail["analysis_jobs"][0]["id"])
    with isolated_db.session_scope() as session:
        job = session.get(MarketOpportunityAnalysisJob, job_id)
        job.status = "submitted"
        job.query_id = "market_opp_existing"

    service = MarketOpportunityService(config=_service_config(), db_manager=isolated_db, repository=repo)
    service._get_market_review = MagicMock(return_value=SimpleNamespace(id=77, report_type="market_review", created_at=datetime(2026, 8, 31, 15, 30)))
    service._extract_market_payload = MagicMock(return_value={"date": "2026-08-31", "sectors": {"top": [{"name": "半导体"}]}})
    service._postmarket_report_gate = MagicMock(return_value={"ok": True})
    service._latest_regime = MagicMock(return_value=SimpleNamespace(id=1, regime="normal"))
    repo.replace_run_children = MagicMock(side_effect=AssertionError("must not replace existing opportunity children"))

    result = service.discover_from_report(report_id=77, market="cn")
    after = repo.get_run_detail(int(run.id))

    assert result["reused_run"] is True
    assert result["job_count"] == 1
    assert int(after["analysis_jobs"][0]["id"]) == job_id
    assert after["analysis_jobs"][0]["status"] == "submitted"


def test_blocked_source_run_is_refreshable_but_healthy_zero_candidate_run_is_terminal(isolated_db):
    repo = MarketOpportunityRepository(isolated_db)
    service = MarketOpportunityService(
        config=_service_config(market_opportunity_mode="observe"),
        db_manager=isolated_db,
        repository=repo,
    )
    report = SimpleNamespace(
        id=79,
        report_type="market_review",
        created_at=datetime(2026, 8, 31, 15, 30),
    )
    service._get_market_review = MagicMock(return_value=report)
    service._extract_market_payload = MagicMock(
        return_value={"date": "2026-08-31", "concepts": {"top": [{"name": "创新药"}]}}
    )
    service._postmarket_report_gate = MagicMock(return_value={"ok": True})
    service._latest_regime = MagicMock(return_value=SimpleNamespace(id=1, regime="normal"))
    service._score_themes_and_candidates = MagicMock(
        return_value=(
            [],
            [{
                "theme_key": "concept:创新药",
                "theme_type": "concept",
                "theme_name": "创新药",
                "status": "insufficient_data",
                "rejection_reasons": ["constituents_unavailable"],
            }],
            [],
        )
    )

    first = service.discover_from_report(report_id=79, market="cn")

    assert first["status"] == "blocked"
    assert first["reason"] == "theme_constituents_unavailable"

    service._score_themes_and_candidates.return_value = (
        [],
        [{
            "theme_key": "concept:创新药",
            "theme_type": "concept",
            "theme_name": "创新药",
            "status": "rejected",
            "rejection_reasons": ["score_below_threshold"],
        }],
        [],
    )
    second = service.discover_from_report(report_id=79, market="cn")
    third = service.discover_from_report(report_id=79, market="cn")

    assert second["status"] == "ready"
    assert second["selected_candidate_count"] == 0
    assert third["reused_run"] is True
    assert third["status"] == "ready"
    assert service._score_themes_and_candidates.call_count == 2


def _persist_healthy_board_snapshot(db, day):
    from src.repositories.board_observation_repository import BoardObservationRepository
    return BoardObservationRepository(db).upsert_snapshot({
        "snapshot_key": f"cn:{day}:csrc_tencent:csrc_industry", "market": "cn", "trade_date": day,
        "source": "csrc_tencent", "taxonomy": "csrc_industry", "status": "healthy",
        "snapshot_hash": f"frozen-{day}", "fetched_at": datetime.combine(day, datetime.min.time()).replace(hour=7, minute=10),
        "coverage": {"status": "healthy", "theme_count": 1},
        "themes": [{"theme_key": "industry:test", "theme_name": "test", "theme_code": "C39",
                    "theme_source": "csrc_tencent", "theme_taxonomy": "csrc_industry", "as_of": str(day)}],
    })


def test_paper_mode_discovery_requires_completed_observe_gate(isolated_db):
    repo = MarketOpportunityRepository(isolated_db)
    service = MarketOpportunityService(
        config=_service_config(market_opportunity_observe_gate_days=3),
        db_manager=isolated_db,
        repository=repo,
    )
    report = SimpleNamespace(id=88, report_type="market_review", created_at=datetime(2026, 8, 31, 15, 30))
    service._get_market_review = MagicMock(return_value=report)
    service._extract_market_payload = MagicMock(return_value={"date": "2026-08-31", "sectors": {"top": [{"name": "半导体"}]}})
    service._postmarket_report_gate = MagicMock(return_value={"ok": True})
    service._latest_regime = MagicMock(return_value=SimpleNamespace(id=1, regime="normal"))
    service._score_themes_and_candidates = MagicMock(return_value=([], [], []))
    result = service.discover_from_report(report_id=88, market="cn")
    assert result["job_count"] == 0
    detail = repo.get_run_detail(result["run_id"])
    assert detail["analysis_jobs"] == []
    assert detail["provenance"]["analysis_block_reason"] == "observe_gate_incomplete"

    for idx, day in enumerate((date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28)), start=1):
        _persist_healthy_board_snapshot(isolated_db, day)
        with isolated_db.session_scope() as session:
            session.add(MarketOpportunityRun(
                run_key=f"observe:{idx}",
                market="cn",
                trade_date=day,
                source_report_id=idx,
                market_regime="normal",
                mode="observe",
                status="completed",
                policy_version="opportunity-policy-v1",
                selected_candidate_count=0,
                provenance_json='{"observation_health":{"healthy":true,"source_snapshot_id":1}}',
            ))
    with isolated_db.session_scope() as session:
        session.add(MarketOpportunityRun(
            run_key="observe:failed",
            market="cn",
            trade_date=date(2026, 8, 29),
            source_report_id=99,
            market_regime="normal",
            mode="observe",
            status="blocked",
            policy_version="opportunity-policy-v1",
            selected_candidate_count=3,
        ))

    status = service._observe_gate_status(market="cn", as_of=date(2026, 8, 31), policy=service._policy())
    assert status["completed"] is True
    assert status["completed_trading_days"] == 3


def test_analyze_run_creates_jobs_only_when_run_matches_current_session(isolated_db):
    repo = MagicMock()
    repo.get_run_detail.return_value = {
        "id": 91,
        "market": "cn",
        "trade_date": "2026-08-31",
        "candidates": [{"candidate_key": "cn:sh:600001", "status": "selected"}],
    }
    repo.create_jobs_for_run.return_value = 1
    service = MarketOpportunityService(
        config=_service_config(),
        db_manager=isolated_db,
        repository=repo,
    )
    service._observe_gate_status = MagicMock(return_value={"completed": True})
    service.process_pending_jobs = MagicMock(return_value={"status": "ok", "processed": 0})

    with patch(
        "src.services.market_opportunity_service.list_analysis_models",
        return_value=["model-a"],
    ), patch("src.core.trading_calendar.get_effective_trading_date", return_value=date(2026, 8, 31)), patch(
        "src.services.daily_market_context.market_context_freshness_expires_at",
        return_value=datetime(2026, 9, 1, 7, 0),
    ):
        result = service.analyze_run(91, owner="api-test", limit=5)

    assert result["created_jobs"] == 1
    repo.create_jobs_for_run.assert_called_once()
    assert repo.create_jobs_for_run.call_args.kwargs["effective_session"] == date(2026, 8, 31)
    assert service.process_pending_jobs.call_args.kwargs["effective_session"] == date(2026, 8, 31)


def test_analyze_run_blocks_stale_run_instead_of_validating_against_own_trade_date(isolated_db):
    repo = MagicMock()
    repo.get_run_detail.return_value = {
        "id": 91,
        "market": "cn",
        "trade_date": "2026-08-31",
        "candidates": [{"candidate_key": "cn:sh:600001", "status": "selected"}],
    }
    service = MarketOpportunityService(
        config=_service_config(),
        db_manager=isolated_db,
        repository=repo,
    )
    service._recover_submitted_jobs = MagicMock(return_value={"processed": 0, "failed": 1, "uncertain": 0})
    service._observe_gate_status = MagicMock(return_value={"completed": True})
    service.process_pending_jobs = MagicMock()

    with patch("src.core.trading_calendar.get_effective_trading_date", return_value=date(2026, 9, 1)), patch(
        "src.services.daily_market_context.market_context_freshness_expires_at",
        return_value=datetime(2026, 9, 2, 7, 0),
    ):
        result = service.analyze_run(91, owner="api-test", limit=5)

    assert result["status"] == "blocked"
    assert result["reason"] == "stale_run_not_current_session"
    assert result["created_jobs"] == 0
    repo.create_jobs_for_run.assert_not_called()
    service.process_pending_jobs.assert_not_called()
    service._recover_submitted_jobs.assert_called_once_with(
        run_id=91,
        limit=5,
        effective_session=date(2026, 9, 1),
    )


def test_analyze_run_blocks_when_current_session_cannot_be_proven(isolated_db):
    repo = MagicMock()
    repo.get_run_detail.return_value = {
        "id": 91,
        "market": "cn",
        "trade_date": "2026-08-31",
        "candidates": [{"candidate_key": "cn:sh:600001", "status": "selected"}],
    }
    service = MarketOpportunityService(
        config=_service_config(),
        db_manager=isolated_db,
        repository=repo,
    )
    service._observe_gate_status = MagicMock(return_value={"completed": True})
    service.process_pending_jobs = MagicMock()

    with patch("src.core.trading_calendar.get_effective_trading_date", return_value=date(2026, 8, 31)), patch(
        "src.services.daily_market_context.market_context_freshness_expires_at",
        return_value=None,
    ):
        result = service.analyze_run(91, owner="api-test", limit=5)

    assert result["status"] == "blocked"
    assert result["reason"] == "current_session_unavailable"
    assert result["created_jobs"] == 0
    repo.create_jobs_for_run.assert_not_called()
    service.process_pending_jobs.assert_not_called()


def test_market_review_readiness_waits_for_current_session_review_without_writing(isolated_db):
    """The deferred evening run must not discover from the previous session's review."""

    from src.storage import AnalysisHistory

    service = MarketOpportunityService(config=_service_config(), db_manager=isolated_db)

    def add_review(day, *, region="cn"):
        with isolated_db.session_scope() as session:
            row = AnalysisHistory(
                query_id=f"market-{day.isoformat()}-{region}",
                code="MARKET",
                name="大盘复盘",
                report_type="market_review",
                model_used="market-review",
                raw_result='{"success": true}',
                context_snapshot=json.dumps({
                    "market_review_region": region,
                    "market_review_payload": {"date": day.isoformat()},
                }),
                created_at=datetime(day.year, day.month, day.day, 15, 40),
            )
            session.add(row)
            session.flush()
            return int(row.id)

    def readiness(expires_at=datetime(2026, 10, 12, 7, 0)):
        with patch("src.core.trading_calendar.get_effective_trading_date", return_value=date(2026, 10, 9)), patch(
            "src.services.daily_market_context.market_context_freshness_expires_at",
            return_value=expires_at,
        ):
            return service.market_review_readiness(market="cn", current_time=datetime(2026, 10, 9, 12, 0))

    assert readiness() == {"ready": False, "reason": "market_review_not_found", "session": "2026-10-09"}
    previous_id = add_review(date(2026, 10, 8))
    stale = {
        "ready": False,
        "reason": "market_review_not_ready",
        "session": "2026-10-09",
        "report_id": previous_id,
        "report_date": "2026-10-08",
    }
    assert readiness() == stale
    add_review(date(2026, 10, 9), region="us")
    assert readiness() == stale
    current_id = add_review(date(2026, 10, 9))
    assert readiness(expires_at=None) == {"ready": False, "reason": "current_session_unavailable"}
    assert readiness() == {
        "ready": True,
        "reason": None,
        "session": "2026-10-09",
        "report_id": current_id,
        "report_date": "2026-10-09",
    }
    # Unlike a blocked discovery, the readiness probe never persists a run.
    with isolated_db.session_scope() as session:
        assert session.query(MarketOpportunityRun).count() == 0


def test_refresh_run_status_keeps_submitted_job_analyzing(isolated_db):
    repo = MarketOpportunityRepository(isolated_db)
    run = repo.create_or_get_run({
        "run_key": "submitted-remains-active",
        "market": "cn",
        "trade_date": date(2026, 8, 31),
        "source_report_id": 1,
        "market_regime": "normal",
        "mode": "paper",
        "status": "analyzing",
        "policy_version": "opportunity-policy-v2",
        "config_snapshot": {},
        "provenance": {},
        "llm_budget": 1,
    })
    repo.replace_run_children(
        run_id=int(run.id),
        themes=[],
        candidates=[{
            "candidate_key": "cn:sh:600001",
            "market": "cn",
            "exchange": "sh",
            "stock_code": "600001",
            "symbol": "600001.SH",
            "status": "selected",
            "rank": 1,
            "as_of": date(2026, 8, 31),
            "valid_until": date(2026, 8, 31),
        }],
        jobs=[{
            "candidate_key": "cn:sh:600001",
            "job_key": "submitted-active-job",
            "model": "model-a",
            "status": "pending",
        }],
    )
    job_id = int(repo.get_run_detail(int(run.id))["analysis_jobs"][0]["id"])
    with isolated_db.session_scope() as session:
        job = session.get(MarketOpportunityAnalysisJob, job_id)
        job.status = "submitted"
        job.query_id = "submitted-query"

    service = MarketOpportunityService(
        config=_service_config(),
        db_manager=isolated_db,
        repository=repo,
    )
    service._refresh_run_status(int(run.id))

    assert repo.get_run_detail(int(run.id))["status"] == "analyzing"


def test_postmarket_gate_blocks_intraday_market_report():
    service = MarketOpportunityService(config=_service_config())
    report = SimpleNamespace(id=1, created_at=datetime(2026, 8, 31, 10, 0), context_snapshot="{}", raw_result="{}")

    with patch("src.core.trading_calendar.get_market_now", return_value=datetime(2026, 8, 31, 10, 1)), patch(
        "src.core.trading_calendar.get_effective_trading_date", return_value=date(2026, 8, 31)
    ), patch("src.core.trading_calendar.is_market_open", return_value=True):
        gate = service._postmarket_report_gate(
            report=report,
            payload={"date": "2026-08-31"},
            market="cn",
            current_time=datetime(2026, 8, 31, 2, 1),
        )

    assert gate["ok"] is False
    assert gate["reason"] == "market_report_not_postmarket_complete"


def test_latest_market_review_accepts_comma_separated_region(isolated_db):
    from src.storage import AnalysisHistory

    with isolated_db.session_scope() as session:
        session.add(
            AnalysisHistory(
                query_id="market-cn-us",
                code="MARKET",
                name="A股大盘",
                report_type="market_review",
                model_used="market-review",
                context_snapshot='{"market_review_region": "cn,us"}',
                created_at=datetime(2026, 8, 31, 15, 10),
            )
        )

    service = MarketOpportunityService(config=_service_config(), db_manager=isolated_db)
    report = service._latest_market_review(market="cn")

    assert report is not None
    assert report.query_id == "market-cn-us"


def test_same_day_report_reuse_requires_complete_postmarket_asof_and_style(isolated_db):
    from src.storage import AnalysisHistory

    trade_date = date(2026, 6, 30)
    candidate = SimpleNamespace(
        stock_code="600001",
        exchange="sh",
        symbol="600001.SH",
        candidate_key="cn:sh:600001",
        snapshot_hash="frozen-evidence",
    )
    intraday_context = {
        "market_phase_summary": {
            "phase": "intraday",
            "effective_daily_bar_date": "2026-06-30",
        },
        "paper_trading_style_context": {"trading_style": "position"},
    }
    postmarket_context = {
        "market_phase_summary": {
            "phase": "postmarket",
            "effective_daily_bar_date": "2026-06-30",
        },
        "paper_trading_style_context": {"trading_style": "position"},
        "selection_context": build_selection_context(candidate=candidate, job=object()),
    }
    with isolated_db.session_scope() as session:
        reports = [
            AnalysisHistory(
                query_id="intraday",
                code="600001.SH",
                name="测试股份",
                report_type="detailed",
                model_used="model-a",
                raw_result='{"success": true}',
                context_snapshot=json.dumps(intraday_context, ensure_ascii=False),
                created_at=datetime(2026, 6, 30, 14, 30),
            ),
            AnalysisHistory(
                query_id="postmarket",
                code="600001.SH",
                name="测试股份",
                report_type="detailed",
                model_used="model-a",
                raw_result='{"success": true}',
                context_snapshot=json.dumps(postmarket_context, ensure_ascii=False),
                created_at=datetime(2026, 6, 30, 15, 30),
            ),
        ]
        session.add_all(reports)
        session.flush()
        for report in reports:
            add_report_execution_intent(session, report.id)

    service = MarketOpportunityService(config=_service_config(paper_trading_style="position"), db_manager=isolated_db)

    assert service._same_day_stock_report_id(candidate=candidate, model="model-a", trade_date=trade_date) is not None


@pytest.mark.parametrize(
    ("execution_intent", "execution_scope"),
    [("research_only", "primary"), ("paper_candidate", "experiment:exp1:b")],
)
def test_same_day_report_reuse_is_isolated_by_frozen_execution_purpose(
    isolated_db,
    execution_intent,
    execution_scope,
):
    from src.storage import AnalysisHistory

    trade_date = date(2026, 6, 30)
    candidate = SimpleNamespace(
        stock_code="600001",
        exchange="sh",
        symbol="600001.SH",
        candidate_key="cn:sh:600001",
        snapshot_hash="frozen-evidence",
    )
    context = {
        "market_phase_summary": {
            "phase": "postmarket",
            "effective_daily_bar_date": trade_date.isoformat(),
        },
        "paper_trading_style_context": {"trading_style": "position"},
        "selection_context": build_selection_context(candidate=candidate, job=object()),
    }
    with isolated_db.session_scope() as session:
        report = AnalysisHistory(
            query_id="other-purpose-postmarket",
            code="600001.SH",
            name="测试股份",
            report_type="detailed",
            model_used="model-a",
            raw_result='{"success": true}',
            context_snapshot=json.dumps(context, ensure_ascii=False),
            created_at=datetime(2026, 6, 30, 15, 30),
        )
        session.add(report)
        session.flush()
        add_report_execution_intent(
            session,
            report.id,
            execution_intent=execution_intent,
            execution_scope=execution_scope,
        )
        report_id = int(report.id)

    service = MarketOpportunityService(config=_service_config(paper_trading_style="position"), db_manager=isolated_db)

    # A report with another purpose must not become a primary paper candidate.
    assert service._same_day_stock_report_id(candidate=candidate, model="model-a", trade_date=trade_date) is None
    # The same evidence stays reusable inside its own frozen purpose.
    assert service._same_day_stock_report_id(
        candidate=candidate,
        model="model-a",
        trade_date=trade_date,
        execution_intent=execution_intent,
        execution_scope=execution_scope,
    ) == report_id


def test_same_day_report_reuse_rejects_intraday_only_report(isolated_db):
    from src.storage import AnalysisHistory

    trade_date = date(2026, 6, 30)
    candidate = SimpleNamespace(stock_code="600001", exchange="sh", symbol="600001.SH")
    context = {
        "market_phase_summary": {
            "phase": "intraday",
            "effective_daily_bar_date": "2026-06-30",
        },
        "paper_trading_style_context": {"trading_style": "position"},
    }
    with isolated_db.session_scope() as session:
        report = AnalysisHistory(
            query_id="intraday-only",
            code="600001.SH",
            name="测试股份",
            report_type="detailed",
            model_used="model-a",
            raw_result='{"success": true}',
            context_snapshot=json.dumps(context, ensure_ascii=False),
            created_at=datetime(2026, 6, 30, 14, 30),
        )
        session.add(report)
        session.flush()
        add_report_execution_intent(session, report.id)

    service = MarketOpportunityService(config=_service_config(paper_trading_style="position"), db_manager=isolated_db)

    assert service._same_day_stock_report_id(candidate=candidate, model="model-a", trade_date=trade_date) is None


def test_same_day_report_reuse_rejects_manual_postmarket_report_without_opportunity_evidence(isolated_db):
    from src.storage import AnalysisHistory

    trade_date = date(2026, 6, 30)
    candidate = SimpleNamespace(
        stock_code="600001",
        exchange="sh",
        symbol="600001.SH",
        candidate_key="cn:sh:600001",
    )
    context = {
        "market_phase_summary": {
            "phase": "postmarket",
            "effective_daily_bar_date": trade_date.isoformat(),
        },
        "paper_trading_style_context": {"trading_style": "position"},
    }
    with isolated_db.session_scope() as session:
        report = AnalysisHistory(
            query_id="manual-postmarket",
            code="600001.SH",
            name="测试股份",
            report_type="detailed",
            model_used="model-a",
            raw_result='{"success": true}',
            context_snapshot=json.dumps(context, ensure_ascii=False),
            created_at=datetime(2026, 6, 30, 15, 30),
        )
        session.add(report)
        session.flush()
        add_report_execution_intent(session, report.id)

    service = MarketOpportunityService(
        config=_service_config(paper_trading_style="position"),
        db_manager=isolated_db,
    )

    assert service._same_day_stock_report_id(
        candidate=candidate,
        model="model-a",
        trade_date=trade_date,
    ) is None


def test_same_day_report_reuse_rejects_mismatched_selection_snapshot(isolated_db):
    from src.storage import AnalysisHistory

    trade_date = date(2026, 6, 30)
    candidate = SimpleNamespace(
        stock_code="600001",
        exchange="sh",
        symbol="600001.SH",
        candidate_key="cn:sh:600001",
        snapshot_hash="new-theme-snapshot",
    )
    context = {
        "market_phase_summary": {
            "phase": "postmarket",
            "effective_daily_bar_date": trade_date.isoformat(),
        },
        "paper_trading_style_context": {"trading_style": "position"},
        "selection_context": {
            "source": "market_opportunity",
            "candidate_key": "cn:sh:600001",
            "snapshot_hash": "old-theme-snapshot",
        },
    }
    with isolated_db.session_scope() as session:
        report = AnalysisHistory(
            query_id="old-theme-postmarket",
            code="600001.SH",
            name="测试股份",
            report_type="detailed",
            model_used="model-a",
            raw_result='{"success": true}',
            context_snapshot=json.dumps(context, ensure_ascii=False),
            created_at=datetime(2026, 6, 30, 15, 30),
        )
        session.add(report)
        session.flush()
        add_report_execution_intent(session, report.id)

    service = MarketOpportunityService(
        config=_service_config(paper_trading_style="position"),
        db_manager=isolated_db,
    )

    assert service._same_day_stock_report_id(
        candidate=candidate,
        model="model-a",
        trade_date=trade_date,
    ) is None


def test_policy_reads_database_runtime_mode_override(isolated_db):
    config = _service_config(market_opportunity_mode="off")
    RuntimeSettingsService(config=config, db_manager=isolated_db).set_market_opportunity_mode(
        "observe",
        expected_version=0,
        actor="unit-test",
    )

    service = MarketOpportunityService(config=config, db_manager=isolated_db)
    policy = service._policy()

    assert policy.mode == "observe"
    assert policy.mode_source == "database"
    assert policy.mode_version == 1


def test_theme_selection_ranks_before_user_and_cautious_caps_at_repository_boundary(isolated_db):
    repo = MarketOpportunityRepository(isolated_db)
    service = MarketOpportunityService(
        config=_service_config(
            market_opportunity_mode="observe",
            market_opportunity_max_themes=1,
            market_opportunity_discovery_max_themes=1,
            market_opportunity_max_total_candidates=8,
        ),
        db_manager=isolated_db,
        repository=repo,
        board_provider=_FakeBoardProvider([{"code": "600001", "exchange": "sh", "symbol": "600001.SH", "name": "强势股份"}]),
    )
    report = SimpleNamespace(id=201, report_type="market_review", created_at=datetime(2026, 8, 31, 15, 30))
    service._get_market_review = MagicMock(return_value=report)
    service._extract_market_payload = MagicMock(
        return_value={
            "date": "2026-08-31",
            "sectors": {
                "top": [
                    {"name": "低分先到", "rank": 1, "change_pct": 1.0, "source": "eastmoney_industry", "code": "BK1001"},
                    {"name": "高分后到", "rank": 2, "change_pct": 4.0, "source": "eastmoney_industry", "code": "BK1002"},
                    {"name": "中分第三", "rank": 3, "change_pct": 3.0, "source": "eastmoney_industry", "code": "BK1003"},
                ]
            },
        }
    )
    service._postmarket_report_gate = MagicMock(return_value={"ok": True})
    service._latest_regime = MagicMock(return_value=SimpleNamespace(id=1, regime="cautious"))
    scores = {"industry:低分先到": 80.0, "industry:高分后到": 96.0, "industry:中分第三": 88.0}
    service._score_theme = MagicMock(
        side_effect=lambda theme, _snapshot: (
            scores[theme["theme_key"]],
            {"mocked_score": scores[theme["theme_key"]]},
            [],
        )
    )
    service._score_candidate = MagicMock(
        side_effect=lambda constituent, theme, trade_date, policy, threshold: {
            "candidate_key": f"cn:sh:{theme['theme_key']}",
            "market": "cn",
            "exchange": "sh",
            "stock_code": "600001",
            "symbol": "600001.SH",
            "stock_name": "强势股份",
            "primary_theme_key": theme["theme_key"],
            "related_themes": [],
            "status": "selected",
            "rank": None,
            "score": 90,
            "score_components": {},
            "rejection_reasons": [],
            "data_quality": {},
            "quote_snapshot": {},
            "as_of": trade_date,
            "valid_until": trade_date,
            "snapshot_hash": theme["theme_key"],
        }
    )

    result = service.discover_from_report(report_id=201, market="cn")
    detail = repo.get_run_detail(int(result["run_id"]))

    assert result["status"] == "ready"
    assert result["selected_theme_count"] == 1
    assert [theme["theme_key"] for theme in detail["themes"] if theme["status"] == "selected"] == ["industry:高分后到"]
    assert {
        theme["theme_key"]: theme["rejection_reasons"]
        for theme in detail["themes"]
        if theme["status"] == "rejected"
    } == {
        "industry:低分先到": ["over_total_theme_limit"],
        "industry:中分第三": ["over_total_theme_limit"],
    }
    assert [candidate["primary_theme_key"] for candidate in detail["candidates"]] == ["industry:高分后到"]


def test_zero_llm_budget_stays_zero_and_blocks_paper_discovery_with_selected_candidates(isolated_db):
    repo = MarketOpportunityRepository(isolated_db)
    service = MarketOpportunityService(
        config=_service_config(market_opportunity_max_llm_calls_per_run=0),
        db_manager=isolated_db,
        repository=repo,
    )
    assert service._policy().max_llm_calls_per_run == 0
    report = SimpleNamespace(id=202, report_type="market_review", created_at=datetime(2026, 8, 31, 15, 30))
    service._get_market_review = MagicMock(return_value=report)
    service._extract_market_payload = MagicMock(return_value={"date": "2026-08-31", "sectors": {"top": [{"name": "半导体"}]}})
    service._postmarket_report_gate = MagicMock(return_value={"ok": True})
    service._observe_gate_status = MagicMock(return_value={"completed": True})
    service._latest_regime = MagicMock(return_value=SimpleNamespace(id=1, regime="normal"))
    service._score_themes_and_candidates = MagicMock(
        return_value=(
            [{"theme_key": "industry:半导体", "status": "selected"}],
            [{"theme_key": "industry:半导体", "theme_type": "industry", "theme_name": "半导体", "status": "selected", "score": 90}],
            [{
                "candidate_key": "cn:sh:600001",
                "market": "cn",
                "exchange": "sh",
                "stock_code": "600001",
                "symbol": "600001.SH",
                "stock_name": "测试股份",
                "primary_theme_key": "industry:半导体",
                "status": "selected",
                "rank": 1,
                "score": 91,
                "as_of": date(2026, 8, 31),
                "valid_until": date(2026, 8, 31),
            }],
        )
    )

    with patch("src.services.market_opportunity_service.list_analysis_models", return_value=["deepseek/deepseek-v4-pro"]):
        result = service.discover_from_report(report_id=202, market="cn")

    detail = repo.get_run_detail(int(result["run_id"]))
    assert result["status"] == "blocked"
    assert result["reason"] == "llm_budget_zero"
    assert result["job_count"] == 0
    assert detail["status"] == "blocked"
    assert detail["error_message"] == "llm_budget_zero"
    assert detail["llm_budget"] == 0
    assert len(detail["analysis_jobs"]) == 0


def test_budget_smaller_than_model_group_blocks_analyze_run_before_job_creation(isolated_db):
    repo = MagicMock()
    repo.get_run_detail.return_value = {
        "id": 203,
        "market": "cn",
        "trade_date": "2026-08-31",
        "candidates": [{"candidate_key": "cn:sh:600001", "status": "selected"}],
    }
    service = MarketOpportunityService(
        config=_service_config(market_opportunity_max_llm_calls_per_run=1),
        db_manager=isolated_db,
        repository=repo,
    )
    service._observe_gate_status = MagicMock(return_value={"completed": True})
    service.process_pending_jobs = MagicMock()

    with patch("src.services.market_opportunity_service.list_analysis_models", return_value=["model-a", "model-b"]), patch(
        "src.core.trading_calendar.get_effective_trading_date",
        return_value=date(2026, 8, 31),
    ), patch(
        "src.services.daily_market_context.market_context_freshness_expires_at",
        return_value=datetime(2026, 9, 1, 7, 0),
    ):
        result = service.analyze_run(203, owner="unit-test", limit=5)

    assert result == {
        "status": "blocked",
        "reason": "llm_budget_insufficient_for_configured_models",
        "processed": 0,
        "created_jobs": 0,
    }
    repo.create_jobs_for_run.assert_not_called()
    service.process_pending_jobs.assert_not_called()
    repo.update_run.assert_called_with(
        203,
        status="blocked",
        error_message="llm_budget_insufficient_for_configured_models",
        completed_at=repo.update_run.call_args.kwargs["completed_at"],
    )


def test_automatic_weekend_run_recovers_submitted_jobs_but_skips_new_discovery_and_followups(isolated_db):
    service = MarketOpportunityService(config=_service_config(), db_manager=isolated_db)
    service._recover_submitted_jobs = MagicMock(return_value={"processed": 0, "failed": 1, "uncertain": 0})
    service.discover_from_latest_market_review = MagicMock(side_effect=AssertionError("weekend auto run must not discover"))
    service._materialize_due_followup_runs = MagicMock(side_effect=AssertionError("weekend auto run must not materialize followups"))
    service.process_pending_jobs = MagicMock(side_effect=AssertionError("weekend auto run must not submit pending jobs"))

    from src.core.trading_calendar import MarketPhase

    context = SimpleNamespace(
        phase=MarketPhase.NON_TRADING,
        is_trading_day=False,
        session_date=date(2026, 9, 13),
        warnings=[],
        to_dict=lambda: {
            "market": "cn",
            "phase": "non_trading",
            "session_date": "2026-09-13",
            "effective_daily_bar_date": "2026-09-11",
            "is_trading_day": False,
            "warnings": [],
        },
    )
    with patch("src.core.trading_calendar.get_effective_trading_date", return_value=date(2026, 9, 11)), patch(
        "src.services.daily_market_context.market_context_freshness_expires_at",
        return_value=datetime(2026, 9, 14, 7, 0),
    ), patch(
        "src.core.trading_calendar.build_market_phase_context",
        return_value=context,
    ):
        result = service.run_after_market_review(market="cn", current_time=datetime(2026, 9, 13, 12, 0))

    assert result["status"] == "ok"
    assert result["submitted_recovery"] == {"processed": 0, "failed": 1, "uncertain": 0}
    assert result["discovery"]["status"] == "skipped"
    assert result["discovery"]["reason"] == "market_not_open_for_new_opportunity_work"
    assert result["followups"]["status"] == "skipped"
    assert result["jobs"] == {"processed": 0, "failed": 0, "uncertain_submitted": 0}
    service._recover_submitted_jobs.assert_called_once_with(
        run_id=None,
        limit=200,
        effective_session=date(2026, 9, 11),
    )


def test_automatic_new_work_gate_uses_real_calendar_for_weekend_weekday_and_errors():
    sunday = MarketOpportunityService._automatic_new_work_gate(
        market="cn",
        current_time=datetime(2026, 9, 13, 12, 0),
    )
    weekday = MarketOpportunityService._automatic_new_work_gate(
        market="cn",
        current_time=datetime(2026, 9, 11, 7, 20, tzinfo=timezone.utc),
    )

    assert sunday["allowed"] is False
    assert sunday["reason"] == "market_not_open_for_new_opportunity_work"
    assert sunday["phase"] == "non_trading"
    assert weekday["allowed"] is True
    assert weekday["reason"] is None
    assert weekday["session_date"] == "2026-09-11"

    with patch("src.core.trading_calendar.is_market_open", side_effect=RuntimeError("calendar_unavailable: cn 2026-09-11")):
        unavailable = MarketOpportunityService._automatic_new_work_gate(
            market="cn",
            current_time=datetime(2026, 9, 11, 7, 20, tzinfo=timezone.utc),
        )

    assert unavailable["allowed"] is False
    assert unavailable["reason"] == "calendar_untrusted"
    assert unavailable["warnings"] == ["calendar_error"]


def test_weekend_run_after_market_review_real_calendar_recovers_submitted_job_without_new_work(isolated_db):
    from src.storage import AnalysisHistory

    repo = MarketOpportunityRepository(isolated_db)
    friday = date(2026, 9, 11)
    with isolated_db.session_scope() as session:
        report = AnalysisHistory(
            query_id="weekend-recovered-query",
            code="600001.SH",
            name="周末恢复股份",
            report_type="detailed",
            model_used="model-a",
            raw_result='{"success": true}',
            context_snapshot=json.dumps(
                {
                    "market_phase_summary": {
                        "phase": "postmarket",
                        "effective_daily_bar_date": friday.isoformat(),
                    }
                },
                ensure_ascii=False,
            ),
            created_at=datetime(2026, 9, 11, 15, 30),
        )
        session.add(report)

    run = repo.create_or_get_run({
        "run_key": "weekend-submitted-run",
        "market": "cn",
        "trade_date": friday,
        "source_report_id": 1,
        "market_regime": "normal",
        "mode": "paper",
        "status": "analyzing",
        "policy_version": "opportunity-policy-v2",
        "config_snapshot": {},
        "provenance": {},
        "llm_budget": 1,
    })
    repo.replace_run_children(
        run_id=int(run.id),
        themes=[],
        candidates=[{
            "candidate_key": "cn:sh:600001",
            "market": "cn",
            "exchange": "sh",
            "stock_code": "600001",
            "symbol": "600001.SH",
            "stock_name": "周末恢复股份",
            "primary_theme_key": "industry:恢复",
            "status": "selected",
            "rank": 1,
            "score": 91,
            "as_of": friday,
            "valid_until": friday,
        }],
        jobs=[{
            "candidate_key": "cn:sh:600001",
            "job_key": "weekend-submitted-job",
            "model": "model-a",
            "status": "pending",
        }],
    )
    job_id = int(repo.get_run_detail(int(run.id))["analysis_jobs"][0]["id"])
    with isolated_db.session_scope() as session:
        job = session.get(MarketOpportunityAnalysisJob, job_id)
        job.status = "submitted"
        job.query_id = "weekend-recovered-query"

    service = MarketOpportunityService(
        config=_service_config(paper_trading_enabled=False),
        db_manager=isolated_db,
        repository=repo,
    )
    service.discover_from_latest_market_review = MagicMock(side_effect=AssertionError("Sunday auto run must not discover"))
    service._materialize_due_followup_runs = MagicMock(side_effect=AssertionError("Sunday auto run must not materialize followups"))
    service.process_pending_jobs = MagicMock(side_effect=AssertionError("Sunday auto run must not submit pending jobs"))

    result = service.run_after_market_review(
        market="cn",
        current_time=datetime(2026, 9, 13, 12, 0),
    )

    recovered = repo.get_job(job_id)
    assert result["status"] == "ok"
    assert result["submitted_recovery"]["processed"] == 1
    assert result["discovery"]["status"] == "skipped"
    assert recovered["status"] == "completed"
    assert recovered["analysis_history_id"] is not None
    assert recovered["paper_plan_id"] is None
    assert repo.list_jobs_page(run_id=int(run.id))["total"] == 1


def test_healthy_observation_days_count_empty_dedupe_and_exclude_failed_future_weekend(isolated_db):
    service = MarketOpportunityService(config=_service_config(market_opportunity_observe_gate_days=3), db_manager=isolated_db)
    records = [
        ('empty', date(2026, 8, 26), True, 0),
        ('duplicate', date(2026, 8, 26), True, 5),
        ('failure', date(2026, 8, 27), False, 5),
        ('healthy', date(2026, 8, 28), True, 0),
        ('weekend', date(2026, 8, 29), True, 3),
        ('future', date(2099, 8, 31), True, 3),
    ]
    for _, day, healthy, _ in records:
        if healthy:
            _persist_healthy_board_snapshot(isolated_db, day)
    with isolated_db.session_scope() as session:
        for key, day, healthy, count in records:
            session.add(MarketOpportunityRun(
                run_key=key, market='cn', trade_date=day, source_report_id=1, mode='observe', status='ready',
                policy_version='opportunity-policy-v3', selected_candidate_count=count,
                provenance_json=json.dumps({'observation_health': {'healthy': healthy, 'reason': None if healthy else 'source_failed'}}),
            ))
    gate = service._observe_gate_status(market='cn', as_of=None, policy=service._policy())
    assert gate['completed_trading_days'] == 2
    assert gate['completed'] is False
    assert set(gate['dates']) == {'2026-08-26', '2026-08-28'}
    assert {row['date'] for row in gate['excluded_dates']} == {'2026-08-27', '2026-08-29', '2099-08-31'}


def test_observation_health_distinguishes_legitimate_empty_from_data_failure():
    observation = {'status': 'ok', 'snapshot_id': 1, 'source': 'csrc_tencent'}
    themes = [{'status': 'rejected', 'rejection_reasons': ['score_below_threshold']}]
    assert MarketOpportunityService._observation_health(observation, themes, [])['healthy'] is True
    candidates = [{'status': 'rejected', 'rejection_reasons': ['insufficient_daily_bars']}]
    verdict = MarketOpportunityService._observation_health(observation, themes, candidates)
    assert verdict['healthy'] is True
    assert verdict['discovery_quality']['candidate_data_incomplete_count'] == 1
    assert MarketOpportunityService._observation_health({'status': 'failed'}, themes, [])['healthy'] is False


@pytest.mark.parametrize(('regime', 'run_key'), [
    ('unavailable', 'market-opportunity:1:paper:opportunity-policy-v4'),
    # 红灯只放开研究；主账户次日确认在红灯下仍冻结。
    ('risk_off', 'opportunity-followup:abc'),
])
def test_manual_analysis_preserves_risk_regime_block(isolated_db, regime, run_key):
    repo = MagicMock()
    repo.get_run_detail.return_value = {
        'id': 1, 'market': 'cn', 'trade_date': '2026-08-31', 'market_regime': regime, 'run_key': run_key,
    }
    service = MarketOpportunityService(config=_service_config(), db_manager=isolated_db, repository=repo)
    service._current_effective_session = MagicMock(return_value=date(2026, 8, 31))
    service._observe_gate_status = MagicMock(return_value={'completed': True})
    result = service.analyze_run(1)
    assert result['reason'] == f'market_regime_{regime}'
    repo.create_jobs_for_run.assert_not_called()


@pytest.mark.parametrize(('remaining', 'expected_jobs', 'reason'), [
    (3, 2, None),
    (1, 0, 'research_budget_exhausted'),
])
def test_manual_risk_off_analysis_creates_only_budgeted_research_jobs(isolated_db, remaining, expected_jobs, reason):
    repo = MagicMock()
    repo.get_run_detail.return_value = {
        'id': 1, 'market': 'cn', 'trade_date': '2026-08-31', 'market_regime': 'risk_off',
        'run_key': 'market-opportunity:1:paper:opportunity-policy-v4',
        'candidates': [
            {'id': 11, 'candidate_key': 'cn:sh:600001', 'status': 'selected'},
            {'id': 12, 'candidate_key': 'cn:sh:600002', 'status': 'selected'},
            {'id': 13, 'candidate_key': 'cn:sh:600003', 'status': 'selected'},
        ],
        # 已有任务的候选不再重复建任务。
        'analysis_jobs': [{'candidate_id': 13, 'status': 'completed'}],
    }
    repo.research_budget_usage.return_value = {
        'limit': 4, 'used': 4 - remaining, 'charged': 3 - remaining, 'reserved': 1, 'remaining': remaining,
    }
    repo.create_jobs_for_run.return_value = expected_jobs
    service = MarketOpportunityService(config=_service_config(), db_manager=isolated_db, repository=repo)
    service._current_effective_session = MagicMock(return_value=date(2026, 8, 31))
    service._observe_gate_status = MagicMock(return_value={'completed': True})
    service.process_pending_jobs = MagicMock(return_value={'status': 'ok', 'processed': 0})

    with patch('src.services.market_opportunity_service.list_analysis_models', return_value=['model-a', 'model-b']):
        result = service.analyze_run(1)

    repo.research_budget_usage.assert_called_once_with(market='cn', session_date=date(2026, 8, 31), limit=4)
    if reason:
        assert result['status'] == 'blocked'
        assert result['reason'] == reason
        repo.create_jobs_for_run.assert_not_called()
        service.process_pending_jobs.assert_not_called()
        return
    jobs = repo.create_jobs_for_run.call_args.kwargs['jobs']
    # 剩余 3 次、两个模型：只接纳一个新候选的整组模型。
    assert [(job['candidate_key'], job['model']) for job in jobs] == [
        ('cn:sh:600001', 'model-a'), ('cn:sh:600001', 'model-b'),
    ]
    assert all(job['job_key'].startswith('market-opportunity-research:initial:') for job in jobs)
    assert all(job['evidence']['execution_intent'] == 'research_only' for job in jobs)
    assert result['created_jobs'] == 2


def test_observation_pool_and_analysis_budgets_are_independent():
    service = MarketOpportunityService(config=_service_config(market_opportunity_max_total_candidates=8,
        market_opportunity_max_observation_candidates=30, market_opportunity_max_themes=3,
        market_opportunity_max_llm_calls_per_run=16))
    rows = [{'candidate_key': f'cn:sh:{600000+i}', 'status': 'selected', 'score': 100-i/10,
             'primary_theme_key': f'industry:{i//3}', 'rejection_reasons': [],
             'score_components': {'strategy_comparison': {'legacy_selected': False}}} for i in range(50)]
    capped = service._rank_and_cap_candidates(rows, policy=service._policy(), regime='cautious')
    selected = [row for row in capped if row['status'] == 'selected']
    assert len(selected) == 8
    assert len([row for row in capped if row['status'] in {'observing', 'selected'}]) == 30
    assert len({row['primary_theme_key'] for row in selected}) == 3
    jobs = service._build_jobs(run_id=1, candidates=selected, models=['model-a', 'model-b'], policy=service._policy())
    assert len(jobs) == 16


@pytest.mark.parametrize('up_limit,expect_reject', [(12.0, False), (11.0, True), (None, True)])
def test_flat_bar_uses_actual_security_limit_not_universal_ten_percent(up_limit, expect_reject):
    manager = _FakeFetcherManager([])
    manager.get_limit_prices = MagicMock(return_value=None if up_limit is None else
        {'trade_date': '2026-06-30', 'up_limit': up_limit, 'down_limit': 8.0})
    service = MarketOpportunityService(config=_service_config(), fetcher_manager=manager)
    bars = manager.get_daily_data('300001.SZ', days=180)[0]
    bars.loc[:, 'close'] = [8.0 + i/100 for i in range(180)]
    bars.loc[len(bars)-1, ['open', 'high', 'low', 'close']] = 11.0
    service._daily_bars = MagicMock(return_value=bars)
    service._benchmark_bars = MagicMock(return_value=bars)
    candidate = service._score_candidate(
        constituent={'code': '300001', 'exchange': 'sz', 'symbol': '300001.SZ', 'name': '测试'},
        theme={'theme_key': 'industry:test', 'theme_name': 'test', 'score': 100},
        trade_date=date(2026, 6, 30), policy=service._policy(), threshold=0)
    manager.get_limit_prices.assert_called_once()
    errors = set(candidate['rejection_reasons']) & {'one_price_limit_untradable', 'limit_prices_unavailable'}
    assert bool(errors) == expect_reject


def test_current_short_history_is_ineligible_without_poisoning_healthy_day():
    service=MarketOpportunityService(config=_service_config(),fetcher_manager=_FakeFetcherManager([]))
    full=service.fetcher_manager.get_daily_data('600001.SH',days=180)[0]
    service._daily_bars=MagicMock(return_value=full.tail(10))
    service._benchmark_bars=MagicMock(return_value=full)
    candidate=service._score_candidate(constituent={'code':'600001','exchange':'sh','symbol':'600001.SH','name':'新股'},
        theme={'theme_key':'industry:test','theme_name':'test','score':90},trade_date=date(2026,6,30),policy=service._policy(),threshold=70)
    assert candidate['status'] == 'rejected'
    assert candidate['score_components']['history_status'] == 'short_history'
    health=service._observation_health({'status':'ok','snapshot_id':1},[{'status':'selected'}],[candidate])
    assert health['healthy'] is True
    service._daily_bars=MagicMock(return_value=None)
    failed=service._score_candidate(constituent={'code':'600001','exchange':'sh','symbol':'600001.SH','name':'新股'},
        theme={'theme_key':'industry:test','theme_name':'test','score':90},trade_date=date(2026,6,30),policy=service._policy(),threshold=70)
    verdict = service._observation_health({'status':'ok','snapshot_id':1},[{'status':'selected'}],[failed])
    assert verdict['healthy'] is True
    assert verdict['discovery_quality']['candidate_data_incomplete_count'] == 1
    assert failed['status'] == 'rejected'


def test_twenty_day_strength_survives_final_theme_and_stock_selection():
    from dataclasses import replace
    constituents = [{'code': f'600{i:03d}', 'exchange': 'sh', 'symbol': f'600{i:03d}.SH',
                     'name': f'股票{i}', '涨跌幅': 1.0, '成交额': 220_000_000, '量比': 1.3}
                    for i in range(1, 6)]
    class Provider(_FakeBoardProvider):
        def get_board_constituents(self, **kwargs):
            return replace(super().get_board_constituents(**kwargs), provider='csrc_tencent')
    class Observations:
        def persistence_for_theme(self, *, theme_key, **kwargs):
            return {'days': 5, 'required_window_size': 5, 'available_window_size': 5,
                    'source': 'csrc_tencent', 'return_5d': 5.0,
                    'return_20d': 20.0 if theme_key.endswith('中线延续') else -20.0}
    service = MarketOpportunityService(config=_service_config(),
        board_provider=Provider(constituents), fetcher_manager=_FakeFetcherManager(constituents),
        repository=_PersistenceOnlyRepo([]), board_observation_service=Observations())
    themes = [{'theme_key': f'industry:{name}', 'theme_type': 'industry', 'theme_name': name,
               'theme_code': code, 'theme_source': 'csrc_tencent', 'theme_taxonomy': 'csrc_industry',
               'rank': rank, 'change_pct': 2.0}
              for rank, (name, code) in enumerate([('短期反弹', 'csrc_C27'), ('中线延续', 'csrc_C39')], 1)]
    selected, rows, candidates = service._score_themes_and_candidates(
        themes=themes, policy=service._policy(), market='cn', trade_date=date(2026, 6, 30), regime='normal')
    assert selected[0]['theme_name'] == '中线延续'
    assert selected[0]['score'] > selected[1]['score']
    assert candidates and all(c['primary_theme_key'] == 'industry:中线延续' for c in candidates)
    assert rows[0]['score_components']['medium_term_adjustment'] == -10.0
    assert rows[1]['score_components']['medium_term_adjustment'] == 10.0
    assert service._config_snapshot(service._policy())['board_discovery_policy_version'] == 'board-discovery-v2'
    assert service._run_key(report_id=77, policy=service._policy()) != 'market-opportunity:77:paper:opportunity-policy-v3'


def test_board_observation_credit_survives_missing_report_and_partial_candidates(isolated_db):
    service = MarketOpportunityService(config=_service_config(market_opportunity_observe_gate_days=3), db_manager=isolated_db)
    day = date(2026, 9, 15)
    snapshot = _persist_healthy_board_snapshot(isolated_db, day)
    # Collection may complete before report generation fails: no run is required.
    gate = service._observe_gate_status(market="cn", as_of=day, policy=service._policy())
    assert gate["completed_trading_days"] == 1
    assert gate["dates"] == [str(day)]
    assert gate["completed"] is False
    assert gate["total_run_days"] == 0
    old_health = {"healthy": False, "reason": "candidate_data_incomplete",
                  "source_snapshot_id": snapshot["id"], "source": "csrc_tencent"}
    run = {"market": "cn", "trade_date": str(day), "market_regime": "risk_off",
           "provenance": {"observation_health": old_health, "analysis_block_reason": "market_regime_risk_off"}}
    candidate = {"status": "rejected", "rejection_reasons": ["daily_data_unavailable"]}
    projection = service.project_observation_health(run, themes=[{"status": "selected"}], candidates=[candidate])
    assert projection["provenance"]["observation_health"]["healthy"] is True
    assert projection["provenance"]["recorded_observation_health"] == old_health
    assert projection["provenance"]["observation_health"]["discovery_quality"]["candidate_data_incomplete_count"] == 1
    assert run["provenance"]["observation_health"]["healthy"] is False
    assert candidate["status"] == "rejected"
    assert projection["provenance"]["analysis_block_reason"] == "market_regime_risk_off"
    forged = {**run, "provenance": {"observation_health": {**old_health, "source_snapshot_id": 999}}}
    assert service.project_observation_health(forged)["provenance"]["observation_health"]["healthy"] is False
    replay = {**run, "provenance": {**run["provenance"], "frozen_membership_replay": True}}
    assert service.project_observation_health(replay) == replay


def test_recorded_healthy_run_without_real_snapshot_cannot_grant_credit(isolated_db):
    service = MarketOpportunityService(config=_service_config(market_opportunity_observe_gate_days=3), db_manager=isolated_db)
    with isolated_db.session_scope() as session:
        session.add(MarketOpportunityRun(run_key="legacy-claimed-healthy", market="cn", trade_date=date(2026, 9, 14),
            source_report_id=1, mode="observe", status="ready", policy_version="opportunity-policy-v4",
            provenance_json=json.dumps({"observation_health": {"healthy": True, "source_snapshot_id": 999}})))
    assert service._observe_gate_status(market="cn", as_of=None, policy=service._policy())["completed_trading_days"] == 0


class _SimulatedCrash(BaseException):
    """Process death after the report is saved but before the job is completed."""


class _ResearchReportWriter:
    """Persist reports like the pipeline does and record every model call."""

    def __init__(self, db, *, operation_advice="买入", sentiment_score=80, failures=0, crash_after_save=(),
                 advice_by_code=None, fail_codes=()):
        self.db = db
        self.operation_advice = operation_advice
        self.sentiment_score = sentiment_score
        self.failures = failures
        self.crash_after_save = set(crash_after_save)
        self.advice_by_code = dict(advice_by_code or {})
        self.fail_codes = set(fail_codes)
        self.calls = []

    def analyze_stock(self, code, **kwargs):
        from src.storage import AnalysisHistory

        self.calls.append({"code": code, **kwargs})
        if len(self.calls) <= self.failures or code in self.fail_codes:
            raise RuntimeError("provider_timeout")
        selection = kwargs["selection_context"]
        as_of = date.fromisoformat(selection["as_of"])
        intent = kwargs.get("execution_intent") or {}
        context = {
            "market_phase_summary": {"phase": "postmarket", "effective_daily_bar_date": as_of.isoformat()},
            # 与流水线一致：报告冻结本次分析实际使用的基础风格。
            "paper_trading_style_context": {"trading_style": kwargs.get("trading_style_override") or "position"},
            "selection_context": selection,
        }
        with self.db.session_scope() as session:
            row = AnalysisHistory(
                query_id=kwargs["query_id"],
                code=code,
                name="研究股份",
                report_type="detailed",
                model_used=kwargs["model"],
                raw_result=json.dumps({
                    "success": True,
                    "operation_advice": self.advice_by_code.get(code, self.operation_advice),
                    "sentiment_score": self.sentiment_score,
                }, ensure_ascii=False),
                context_snapshot=json.dumps(context, ensure_ascii=False),
                created_at=datetime(as_of.year, as_of.month, as_of.day, 15, 40),
            )
            session.add(row)
            session.flush()
            add_report_execution_intent(
                session,
                row.id,
                execution_intent=intent.get("execution_intent", "paper_candidate"),
                intent_source=intent.get("intent_source", "unit_test_fixture"),
            )
        if len(self.calls) in self.crash_after_save:
            raise _SimulatedCrash()
        return {"ok": True}


class _HealthyBoardObservations:
    def collect(self, *, market, trade_date):
        return {"status": "ok"}

    def discovery_themes(self, *, market, as_of, limit):
        return [{"theme_key": "industry:半导体", "theme_type": "industry", "theme_name": "半导体"}]


def _research_candidate_row(day, code="600001", *, score=90):
    return {
        "candidate_key": f"cn:sh:{code}",
        "market": "cn",
        "exchange": "sh",
        "stock_code": code,
        "symbol": f"{code}.SH",
        "stock_name": f"研究{code}",
        "primary_theme_key": "industry:半导体",
        "status": "selected",
        "rank": 1,
        "score": score,
        "as_of": day,
        "valid_until": day,
        "snapshot_hash": f"{code}-{day.isoformat()}",
    }


def _research_service(db, repo, writer, **overrides):
    service = MarketOpportunityService(
        config=_service_config(paper_trading_style="position", **overrides),
        db_manager=db,
        repository=repo,
        analysis_service_factory=lambda: writer,
        board_observation_service=_HealthyBoardObservations(),
    )
    service._automatic_new_work_gate = MagicMock(return_value={"allowed": True})
    service._observe_gate_status = MagicMock(return_value={"completed": True})
    service._postmarket_report_gate = MagicMock(return_value={"ok": True})
    # 研究结果进入主账户对账即违反契约。
    service._reconcile_report_to_paper_plan = MagicMock(side_effect=AssertionError("research must not reconcile"))
    return service


def _enter_research_session(service, db, day, *, regime="risk_off", candidates=()):
    """Point the service at one post-market session with a fresh market review."""

    from src.storage import AnalysisHistory

    with db.session_scope() as session:
        row = AnalysisHistory(
            query_id=f"market-{day.isoformat()}",
            code="MARKET",
            name="A股大盘",
            report_type="market_review",
            model_used="market-review",
            raw_result='{"success": true}',
            context_snapshot='{"market_review_region":"cn"}',
            created_at=datetime(day.year, day.month, day.day, 15, 10),
        )
        session.add(row)
        session.flush()
        report = SimpleNamespace(id=int(row.id), report_type="market_review", created_at=row.created_at)
    service._current_effective_session = MagicMock(return_value=day)
    service._latest_market_review = MagicMock(return_value=report)
    service._get_market_review = MagicMock(return_value=report)
    service._extract_market_payload = MagicMock(return_value={"date": day.isoformat()})
    service._latest_regime = MagicMock(return_value=SimpleNamespace(id=1, regime=regime))
    rows = [dict(item) for item in candidates]
    service._score_themes_and_candidates = MagicMock(return_value=(
        [{"theme_key": "industry:半导体", "status": "selected"}],
        [{"theme_key": "industry:半导体", "theme_type": "industry", "theme_name": "半导体", "status": "selected", "score": 90}],
        rows,
    ))
    return report


_HISTORY_REFRESH = "src.services.market_opportunity_history_service.MarketOpportunityHistoryService.refresh"
_ANALYSIS_MODELS = "src.services.market_opportunity_service.list_analysis_models"


def _primary_account_row_counts(db):
    from src.storage import (
        DecisionSignalRecord,
        PaperExecutionPlan,
        PaperStrategyThesis,
        PaperStrategyTransition,
        ReportPlanReconciliation,
    )

    with db.session_scope() as session:
        return {
            model.__tablename__: session.query(model).count()
            for model in (
                DecisionSignalRecord,
                PaperExecutionPlan,
                PaperStrategyThesis,
                PaperStrategyTransition,
                ReportPlanReconciliation,
            )
        }


def _only_job(repo, run_id):
    jobs = repo.get_run_detail(int(run_id))["analysis_jobs"]
    assert len(jobs) == 1
    return jobs[0]


def _outcomes(**counts):
    return {
        key: counts.get(key, 0)
        for key in (
            "bullish", "non_bullish", "analysis_failed", "budget_exhausted", "data_unavailable", "expired", "in_progress",
        )
    }


def test_risk_off_research_spans_two_sessions_without_primary_account_artifacts(isolated_db):
    day1, day2, day3 = date(2026, 9, 7), date(2026, 9, 8), date(2026, 9, 9)
    repo = MarketOpportunityRepository(isolated_db)
    writer = _ResearchReportWriter(isolated_db)
    service = _research_service(isolated_db, repo, writer)

    with patch(_HISTORY_REFRESH, return_value={"status": "skipped"}), patch(_ANALYSIS_MODELS, return_value=["model-a"]):
        _enter_research_session(service, isolated_db, day1, candidates=[_research_candidate_row(day1)])
        first = service.run_after_market_review(market="cn")
        rerun = service.run_after_market_review(market="cn")

        assert first["discovery"]["research"]["block_reason"] is None
        assert first["discovery"]["research"]["job_count"] == 1
        assert first["jobs"]["processed"] == 1
        # 同日重复运行复用已完成的研究，不再调用模型也不再计费。
        assert rerun["discovery"]["reused_run"] is True
        assert len(writer.calls) == 1
        call = writer.calls[0]
        assert "portfolio_context" not in call
        assert call["execution_intent"]["execution_intent"] == "research_only"
        assert call["execution_intent"]["intent_source"] == "market_opportunity_research"
        assert call["selection_context"]["as_of"] == day1.isoformat()
        origin = _only_job(repo, first["discovery"]["run_id"])
        assert origin["status"] == "completed"
        assert origin["job_key"].startswith("market-opportunity-research:initial:")
        assert origin["execution_intent"] == "research_only"
        assert origin["decision_signal_id"] is None and origin["paper_plan_id"] is None
        assert origin["evidence"]["paper_reconciliation"]["reason"] == "research_only"
        assert "followup" not in origin["evidence"]
        bundle = origin["evidence"]["research_bundle"]
        assert bundle["outcome"]["classification"] == "bullish"
        assert bundle["as_of"] == day1.isoformat()
        assert bundle["earliest_executable_session"] == day2.isoformat()
        assert bundle["input_hash"] and bundle["output_hash"] and bundle["bundle_id"]
        review = origin["evidence"]["research_review"]
        assert (review["status"], review["due_date"]) == ("scheduled", day2.isoformat())
        assert "confirmation_current" not in review
        assert repo.research_budget_usage(market="cn", session_date=day1, limit=4)["charged"] == 1
        assert repo.research_budget_usage(market="cn", session_date=day2, limit=4)["reserved"] == 1
        # 主账户同日复用查询看不到研究报告。
        origin_candidate = repo.get_candidate(int(origin["candidate_id"]))
        assert service._same_day_stock_report_id(candidate=origin_candidate, model="model-a", trade_date=day1) is None

        _enter_research_session(service, isolated_db, day2, candidates=[_research_candidate_row(day2)])
        second = service.run_after_market_review(market="cn")

    assert second["research_reviews"]["jobs"]["processed"] == 1
    assert second["jobs"]["processed"] == 1
    # 复核用当天新数据重新分析；再次发现的同一股票复用这份当日研究报告。
    assert len(writer.calls) == 2
    assert writer.calls[1]["selection_context"]["as_of"] == day2.isoformat()
    assert writer.calls[1]["execution_intent"]["execution_intent"] == "research_only"
    review_job = _only_job(repo, second["research_reviews"]["runs"][0]["run_id"])
    assert review_job["status"] == "completed"
    assert review_job["job_key"].startswith("market-opportunity-research:review:")
    assert review_job["analysis_history_id"] != origin["analysis_history_id"]
    assert "research_review" not in review_job["evidence"]
    origin_after = repo.get_job(int(origin["id"]))["evidence"]["research_review"]
    assert origin_after["status"] == "completed"
    assert origin_after["result"] == "bullish"
    assert origin_after["completed_report_id"] == review_job["analysis_history_id"]
    assert origin_after["bundle_id"] == review_job["evidence"]["research_bundle"]["bundle_id"]
    rediscovered = _only_job(repo, second["discovery"]["run_id"])
    assert rediscovered["evidence"]["reused_same_day_report"] is True
    assert rediscovered["analysis_history_id"] == review_job["analysis_history_id"]
    assert rediscovered["evidence"]["research_review"]["due_date"] == day3.isoformat()
    assert repo.research_budget_usage(market="cn", session_date=day2, limit=4) == {
        "limit": 4, "used": 1, "charged": 1, "reserved": 0, "remaining": 3,
    }
    assert repo.research_budget_usage(market="cn", session_date=day3, limit=4)["reserved"] == 1
    assert set(_primary_account_row_counts(isolated_db).values()) == {0}
    service._reconcile_report_to_paper_plan.assert_not_called()


def test_research_and_review_analyses_use_frozen_pool_style(isolated_db):
    # 机会池短线：初次研究与次日复核都按入选时冻结的 short_swing 分析，同日再发现复用这份短线报告。
    day1, day2 = date(2026, 9, 7), date(2026, 9, 8)
    repo = MarketOpportunityRepository(isolated_db)
    writer = _ResearchReportWriter(isolated_db)
    service = _research_service(isolated_db, repo, writer, market_opportunity_style="short_swing")

    def short_swing_row(day):
        return {**_research_candidate_row(day), "score_components": {"trading_style": "short_swing"}}

    with patch(_HISTORY_REFRESH, return_value={"status": "skipped"}), patch(_ANALYSIS_MODELS, return_value=["model-a"]):
        _enter_research_session(service, isolated_db, day1, candidates=[short_swing_row(day1)])
        service.run_after_market_review(market="cn")
        _enter_research_session(service, isolated_db, day2, candidates=[short_swing_row(day2)])
        second = service.run_after_market_review(market="cn")

    assert [call["trading_style_override"] for call in writer.calls] == ["short_swing", "short_swing"]
    assert second["research_reviews"]["jobs"]["processed"] == 1
    rediscovered = _only_job(repo, second["discovery"]["run_id"])
    assert rediscovered["evidence"]["reused_same_day_report"] is True


def test_observe_mode_never_calls_research_model_and_missed_review_expires(isolated_db):
    day1, day2, day3 = date(2026, 9, 7), date(2026, 9, 8), date(2026, 9, 9)
    repo = MarketOpportunityRepository(isolated_db)
    writer = _ResearchReportWriter(isolated_db)
    paper = _research_service(isolated_db, repo, writer)
    observe = _research_service(isolated_db, repo, writer, market_opportunity_mode="observe")

    with patch(_HISTORY_REFRESH, return_value={"status": "skipped"}), patch(_ANALYSIS_MODELS, return_value=["model-a"]):
        _enter_research_session(paper, isolated_db, day1, candidates=[_research_candidate_row(day1)])
        first = paper.run_after_market_review(market="cn")
        assert len(writer.calls) == 1

        _enter_research_session(observe, isolated_db, day2, candidates=[_research_candidate_row(day2, "600002")])
        observed = observe.run_after_market_review(market="cn")
        # observe 只采集：既不研究新候选，也不物化到期复核。
        assert observed["discovery"]["status"] == "ready"
        assert observed["discovery"]["job_count"] == 0
        assert "research" not in observed["discovery"]
        assert observed["research_reviews"]["status"] == "disabled"
        assert len(writer.calls) == 1

        _enter_research_session(paper, isolated_db, day3)
        late = paper.run_after_market_review(market="cn")

    # 错过复核日后不补跑，也不补记复核结果。
    assert late["research_reviews"]["expired"] == 1
    assert late["research_reviews"]["runs"] == []
    assert len(writer.calls) == 1
    review = _only_job(repo, first["discovery"]["run_id"])["evidence"]["research_review"]
    assert (review["status"], review["result"]) == ("expired", "expired")
    assert review["reason"] == "research_review_not_completed_by_expiry"


@pytest.mark.parametrize(("overrides", "models", "reason"), [
    ({"market_opportunity_research_daily_llm_budget": 0}, ["model-a"], "research_budget_zero"),
    # 每日研究预算与既有单次上限取更严格者。
    ({"market_opportunity_max_llm_calls_per_run": 0}, ["model-a"], "research_budget_zero"),
    ({"market_opportunity_research_daily_llm_budget": 1}, ["model-a", "model-b"],
     "research_budget_insufficient_for_configured_models"),
])
def test_risk_off_discovery_reports_research_budget_block_without_model_calls(isolated_db, overrides, models, reason):
    day = date(2026, 9, 7)
    repo = MarketOpportunityRepository(isolated_db)
    writer = _ResearchReportWriter(isolated_db)
    service = _research_service(isolated_db, repo, writer, **overrides)

    with patch(_HISTORY_REFRESH, return_value={"status": "skipped"}), patch(_ANALYSIS_MODELS, return_value=models):
        _enter_research_session(service, isolated_db, day, candidates=[_research_candidate_row(day)])
        result = service.run_after_market_review(market="cn")

    assert result["discovery"]["research"]["block_reason"] == reason
    assert result["discovery"]["job_count"] == 0
    detail = repo.get_run_detail(int(result["discovery"]["run_id"]))
    assert detail["analysis_jobs"] == []
    assert detail["provenance"]["research"]["block_reason"] == reason
    assert detail["provenance"]["analysis_block_reason"] == "market_regime_risk_off"
    assert writer.calls == []


def test_research_budget_charges_retries_and_keeps_reserved_reviews_first(isolated_db):
    day = date(2026, 9, 8)
    repo = MarketOpportunityRepository(isolated_db)
    writer = _ResearchReportWriter(isolated_db, failures=1)
    service = _research_service(isolated_db, repo, writer, market_opportunity_research_daily_llm_budget=3)
    # 前一会话已为复核预留 1 次；新候选只能用剩余 2 次。
    assert repo.reserve_research_budget_slot(market="cn", session_date=day, limit=3, reservation_key="research-review:x")

    with patch(_HISTORY_REFRESH, return_value={"status": "skipped"}), patch(_ANALYSIS_MODELS, return_value=["model-a"]):
        _enter_research_session(service, isolated_db, day, candidates=[
            _research_candidate_row(day, "600001", score=95),
            _research_candidate_row(day, "600002", score=90),
            _research_candidate_row(day, "600003", score=85),
        ])
        discovery = service.discover_from_latest_market_review(market="cn")
        assert discovery["research"]["max_candidates"] == 2
        run_id = int(discovery["run_id"])
        assert len(repo.get_run_detail(run_id)["analysis_jobs"]) == 2

        first = service.process_pending_jobs(owner="retry", limit=10, run_id=run_id, effective_session=day)
        retry = service.process_pending_jobs(owner="retry", limit=10, run_id=run_id, effective_session=day)

    # 第一次调用失败也已计费；重试需要新名额，而剩余名额属于已预留的复核。
    assert (first["processed"], first["failed"]) == (1, 1)
    assert (retry["processed"], retry["failed"]) == (0, 1)
    assert len(writer.calls) == 2
    assert repo.research_budget_usage(market="cn", session_date=day, limit=3) == {
        "limit": 3, "used": 3, "charged": 2, "reserved": 1, "remaining": 0,
    }
    statuses = sorted(
        (job["status"], job["failure_reason"], job["attempt_count"])
        for job in repo.get_run_detail(run_id)["analysis_jobs"]
    )
    assert statuses == [("completed", None, 1), ("failed", "research_budget_exhausted", 2)]


def test_scheduler_separates_research_failure_watch_success_budget_and_no_candidates(isolated_db):
    day1, day2 = date(2026, 9, 7), date(2026, 9, 8)
    repo = MarketOpportunityRepository(isolated_db)
    # 600002 由模型给出观望，情绪分落在买入区间也不得被改写为买入；600003 调用失败。
    writer = _ResearchReportWriter(isolated_db, advice_by_code={"600002.SH": "观望"}, fail_codes={"600003.SH"})
    service = _research_service(isolated_db, repo, writer, market_opportunity_research_daily_llm_budget=3)

    with patch(_HISTORY_REFRESH, return_value={"status": "skipped"}), patch(_ANALYSIS_MODELS, return_value=["model-a"]):
        _enter_research_session(service, isolated_db, day1, candidates=[
            _research_candidate_row(day1, "600001", score=95),
            _research_candidate_row(day1, "600002", score=90),
            _research_candidate_row(day1, "600003", score=85),
        ])
        first = service.run_after_market_review(market="cn")
        # 同会话重跑：失败任务的重试需要新名额，而当日 3 次已全部计费。
        rerun = service.run_after_market_review(market="cn")
        _enter_research_session(service, isolated_db, day2)
        quiet = service.run_after_market_review(market="cn")
        detail = service.get_run_detail(int(first["discovery"]["run_id"]))

    assert first["research_outcomes"] == _outcomes(bullish=1, non_bullish=1, analysis_failed=1)
    assert first["discovery"]["research"]["block_reason"] is None
    assert rerun["discovery"]["reused_run"] is True
    assert rerun["discovery"]["research"]["block_reason"] is None
    assert rerun["research_outcomes"] == _outcomes(bullish=1, non_bullish=1, budget_exhausted=1)
    # 无候选与失败、观望分开呈现；次日到期复核单独计数。
    assert quiet["discovery"]["selected_candidate_count"] == 0
    assert quiet["discovery"]["reason"] is None
    assert quiet["discovery"]["research"]["block_reason"] is None
    assert quiet["research_reviews"]["jobs"]["processed"] == 1
    assert quiet["research_outcomes"] == _outcomes(bullish=1)
    assert [call["code"] for call in writer.calls] == ["600001.SH", "600002.SH", "600003.SH", "600001.SH"]
    # 运行详情与调度汇总使用同一结果口径；观望保持观望，不安排复核。
    states = {
        item["stock_code"]: item["model_states"][0]["research"]["outcome"]
        for item in detail["candidates"]
    }
    assert states == {"600001": "bullish", "600002": "non_bullish", "600003": "budget_exhausted"}
    watch = next(item for item in detail["candidates"] if item["stock_code"] == "600002")["analysis_jobs"][0]
    assert watch["evidence"]["research_bundle"]["outcome"]["action"] == "watch"
    assert "research_review" not in watch["evidence"]
    assert set(_primary_account_row_counts(isolated_db).values()) == {0}


def test_restart_recovers_saved_research_same_session_but_never_backfills_late_review(isolated_db):
    day1, day2, day3 = date(2026, 9, 7), date(2026, 9, 8), date(2026, 9, 9)
    repo = MarketOpportunityRepository(isolated_db)
    # 两次调用都在报告落库后、任务收尾前进程退出。
    writer = _ResearchReportWriter(isolated_db, crash_after_save={1, 2})
    service = _research_service(isolated_db, repo, writer)

    with patch(_HISTORY_REFRESH, return_value={"status": "skipped"}), patch(_ANALYSIS_MODELS, return_value=["model-a"]):
        _enter_research_session(service, isolated_db, day1, candidates=[_research_candidate_row(day1)])
        with pytest.raises(_SimulatedCrash):
            service.run_after_market_review(market="cn")
        restarted = service.run_after_market_review(market="cn")

        # 同会话重启只按 query_id 回收报告，不重复调用模型或计费，仍安排次日复核。
        assert restarted["submitted_recovery"]["processed"] == 1
        assert len(writer.calls) == 1
        origin = _only_job(repo, restarted["discovery"]["run_id"])
        assert origin["status"] == "completed"
        assert origin["evidence"]["recovered_submitted"] is True
        assert origin["evidence"]["research_review"]["status"] == "scheduled"
        assert repo.research_budget_usage(market="cn", session_date=day1, limit=4)["charged"] == 1

        _enter_research_session(service, isolated_db, day2)
        with pytest.raises(_SimulatedCrash):
            service.run_after_market_review(market="cn")
        review = repo.list_jobs_page(status="submitted", page=1, limit=10)["items"]
        assert len(review) == 1 and review[0]["job_key"].startswith("market-opportunity-research:review:")

        _enter_research_session(service, isolated_db, day3)
        late = service.run_after_market_review(market="cn")

    # 复核报告在错过的会话之后才被回收：只留档，不能补成复核结果。
    assert late["submitted_recovery"]["processed"] == 1
    assert late["research_reviews"]["runs"] == []
    assert len(writer.calls) == 2
    late_review = repo.get_job(int(review[0]["id"]))
    assert late_review["status"] == "completed"
    assert late_review["failure_reason"] == "submitted_job_session_expired_after_report"
    assert late_review["evidence"]["recovered_after_expiry"] is True
    closed = repo.get_job(int(origin["id"]))["evidence"]["research_review"]
    assert (closed["status"], closed["result"]) == ("expired", "expired")
    assert closed["reason"] == "submitted_job_session_expired_after_report"
    assert set(_primary_account_row_counts(isolated_db).values()) == {0}
    service._reconcile_report_to_paper_plan.assert_not_called()


def test_initial_research_recovered_on_later_session_is_archived_without_review(isolated_db):
    day1, day2 = date(2026, 9, 7), date(2026, 9, 8)
    repo = MarketOpportunityRepository(isolated_db)
    writer = _ResearchReportWriter(isolated_db, crash_after_save={1})
    service = _research_service(isolated_db, repo, writer)

    with patch(_HISTORY_REFRESH, return_value={"status": "skipped"}), patch(_ANALYSIS_MODELS, return_value=["model-a"]):
        _enter_research_session(service, isolated_db, day1, candidates=[_research_candidate_row(day1)])
        with pytest.raises(_SimulatedCrash):
            service.run_after_market_review(market="cn")
        _enter_research_session(service, isolated_db, day2)
        late = service.run_after_market_review(market="cn")

    assert late["submitted_recovery"]["processed"] == 1
    assert len(writer.calls) == 1
    completed = repo.list_jobs_page(status="completed", page=1, limit=10)["items"]
    assert len(completed) == 1
    job = completed[0]
    assert job["failure_reason"] == "submitted_job_session_expired_after_report"
    assert job["evidence"]["recovered_after_expiry"] is True
    assert job["evidence"]["research_bundle"]["as_of"] == day1.isoformat()
    # 跨日迟到的首次研究只留档：不安排复核，也不占用下一会话的研究预算。
    assert "research_review" not in job["evidence"]
    assert repo.research_budget_usage(market="cn", session_date=day2, limit=4)["reserved"] == 0
    assert late["research_reviews"]["runs"] == []
    with patch(_ANALYSIS_MODELS, return_value=["model-a"]):
        states = service.get_run_detail(int(job["run_id"]))["candidates"][0]["model_states"]
    assert states[0]["research"]["outcome"] == "expired"


def test_unavailable_regime_still_blocks_due_research_review(isolated_db):
    day1, day2 = date(2026, 9, 7), date(2026, 9, 8)
    repo = MarketOpportunityRepository(isolated_db)
    writer = _ResearchReportWriter(isolated_db)
    service = _research_service(isolated_db, repo, writer)

    with patch(_HISTORY_REFRESH, return_value={"status": "skipped"}), patch(_ANALYSIS_MODELS, return_value=["model-a"]):
        _enter_research_session(service, isolated_db, day1, candidates=[_research_candidate_row(day1)])
        first = service.run_after_market_review(market="cn")
        _enter_research_session(service, isolated_db, day2, regime="unavailable",
                                candidates=[_research_candidate_row(day2, "600002")])
        second = service.run_after_market_review(market="cn")

    # 所需数据不可用时，红灯研究与复核都不调用模型。
    assert len(writer.calls) == 1
    assert "research" not in second["discovery"]
    assert second["discovery"]["job_count"] == 0
    assert second["research_outcomes"] == _outcomes(data_unavailable=1)
    review_job = _only_job(repo, second["research_reviews"]["runs"][0]["run_id"])
    assert (review_job["status"], review_job["failure_reason"]) == ("failed", "market_regime_unavailable")
    origin = repo.get_job(int(_only_job(repo, first["discovery"]["run_id"])["id"]))["evidence"]["research_review"]
    assert (origin["status"], origin["result"]) == ("failed", "not_reviewed")


@pytest.mark.parametrize(("regime", "research"), [("normal", False), ("cautious", False), ("risk_off", True)])
def test_only_red_light_discovery_turns_new_work_into_research(isolated_db, regime, research):
    day = date(2026, 9, 7)
    repo = MarketOpportunityRepository(isolated_db)
    service = _research_service(isolated_db, repo, _ResearchReportWriter(isolated_db))

    with patch(_ANALYSIS_MODELS, return_value=["model-a"]):
        _enter_research_session(service, isolated_db, day, regime=regime, candidates=[_research_candidate_row(day)])
        result = service.discover_from_latest_market_review(market="cn")

    job = _only_job(repo, result["run_id"])
    # 绿黄灯任务保持原主账户用途；红灯新增任务只做研究。
    assert job["job_key"].startswith("market-opportunity-research:initial:") is research
    assert job["execution_intent"] == ("research_only" if research else "paper_candidate")
    assert ("research" in result) is research


def test_research_reuse_is_same_session_research_only_and_ignores_selection_drift(isolated_db):
    from src.storage import AnalysisHistory

    day1, day2 = date(2026, 9, 7), date(2026, 9, 8)
    service = MarketOpportunityService(config=_service_config(paper_trading_style="position"), db_manager=isolated_db)
    candidate = SimpleNamespace(
        stock_code="600001", exchange="sh", symbol="600001.SH", candidate_key="cn:sh:600001",
        snapshot_hash="rediscovered-day2", as_of=day2,
    )
    carried = SimpleNamespace(**{**vars(candidate), "snapshot_hash": "review-carried-day1"})

    def add_report(query_id, day, *, intent, selection_candidate):
        context = {
            "market_phase_summary": {"phase": "postmarket", "effective_daily_bar_date": day.isoformat()},
            "paper_trading_style_context": {"trading_style": "position"},
            "selection_context": build_selection_context(candidate=selection_candidate, job=object()),
        }
        with isolated_db.session_scope() as session:
            row = AnalysisHistory(
                query_id=query_id, code="600001.SH", name="研究股份", report_type="detailed", model_used="model-a",
                raw_result='{"success": true}', context_snapshot=json.dumps(context, ensure_ascii=False),
                created_at=datetime(day.year, day.month, day.day, 15, 30),
            )
            session.add(row)
            session.flush()
            add_report_execution_intent(session, row.id, execution_intent=intent)
            return int(row.id)

    def lookup(execution_intent, day=day2):
        return service._same_day_stock_report_id(
            candidate=candidate, model="model-a", trade_date=day, execution_intent=execution_intent,
        )

    add_report("paper-day2", day2, intent="paper_candidate", selection_candidate=candidate)
    add_report("research-day1", day1, intent="research_only", selection_candidate=SimpleNamespace(
        **{**vars(candidate), "as_of": day1},
    ))
    # 同日主账户报告与前一会话研究报告都不能充当当日研究。
    assert lookup("research_only") is None

    review_report = add_report("review-day2", day2, intent="research_only", selection_candidate=carried)
    # 当日研究只做一次：选股证据不同的复核报告也可供再次发现的同一股票复用。
    assert lookup("research_only") == review_report
    # 主账户候选仍要求完全相同的选股证据，且永不复用研究报告。
    paper_report = lookup("paper_candidate")
    assert paper_report is not None and paper_report != review_report
