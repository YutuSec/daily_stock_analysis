# -*- coding: utf-8 -*-
"""Base trading-style override contract of StockAnalysisPipeline."""

from unittest.mock import MagicMock, patch

from src.core.pipeline import StockAnalysisPipeline


def _pipeline(*, style_overrides=None, **kwargs):
    config = MagicMock()
    config.max_workers = 1
    config.agent_mode = False
    config.agent_skills = []
    config.bocha_api_keys = []
    config.tavily_api_keys = []
    config.brave_api_keys = []
    config.serpapi_keys = []
    config.searxng_base_urls = []
    config.searxng_public_instances_enabled = False
    config.news_max_age_days = 7
    config.enable_realtime_quote = True
    config.enable_chip_distribution = True
    config.realtime_source_priority = []
    config.save_context_snapshot = False
    config.paper_trading_style = "position"
    config.paper_trading_style_overrides = dict(style_overrides or {})
    with patch("src.core.pipeline.get_db"), \
         patch("src.core.pipeline.DataFetcherManager"), \
         patch("src.core.pipeline.GeminiAnalyzer"), \
         patch("src.core.pipeline.NotificationService"), \
         patch("src.core.pipeline.SearchService"):
        return StockAnalysisPipeline(config=config, **kwargs)


def test_without_override_keeps_paper_trading_style():
    context = _pipeline()._paper_trading_style_context("600519")
    assert context["trading_style"] == "position"
    assert context["source"] == "paper_trading_style_config"


def test_explicit_override_sets_base_style_for_analysis():
    # 机会池候选按入选时冻结的风格分析，分析窗口随风格切换。
    context = _pipeline(trading_style_override="short_swing")._paper_trading_style_context("600519")
    assert context["trading_style"] == "short_swing"
    assert context["source"] == "base_style_override"
    assert context["settings"]["analysis_window_sessions"] == 80


def test_same_session_reanalysis_falls_back_to_source_plan_style():
    # 模拟盘同会话重分析沿用源计划冻结的风格；显式覆盖优先。
    portfolio_context = {"paper_trading": {"same_session_reanalysis": True, "trading_style": "short_swing"}}
    inherited = _pipeline(portfolio_context=portfolio_context)
    explicit = _pipeline(portfolio_context=portfolio_context, trading_style_override="trend")
    assert inherited._paper_trading_style_context("600519")["trading_style"] == "short_swing"
    assert explicit._paper_trading_style_context("600519")["trading_style"] == "trend"


def test_per_symbol_override_and_invalid_override_keep_existing_precedence():
    # 精确 market:symbol 覆盖仍然优先；非法覆盖值被忽略。
    pinned = _pipeline(style_overrides={"cn:600519": "trend"}, trading_style_override="short_swing")
    invalid = _pipeline(trading_style_override="bogus")
    assert pinned._paper_trading_style_context("600519")["trading_style"] == "trend"
    assert invalid._paper_trading_style_context("600519")["trading_style"] == "position"
    assert invalid._paper_trading_style_context("600519")["source"] == "paper_trading_style_config"
