# -*- coding: utf-8 -*-
"""Prompt-level contracts for style-aware paper-trading analysis."""

from __future__ import annotations

from src.analysis_context_pack_prompt import format_analysis_context_pack_prompt_section
from src.analyzer import GeminiAnalyzer


def _analyzer(*, legacy: bool = False) -> GeminiAnalyzer:
    analyzer = GeminiAnalyzer.__new__(GeminiAnalyzer)
    analyzer._skill_instructions_override = ""
    analyzer._default_skill_policy_override = ""
    analyzer._use_legacy_default_prompt_override = legacy
    analyzer._resolved_prompt_state = None
    return analyzer


def _minimal_context(*, holding_state: str = "empty", style: str = "medium_position") -> dict:
    return {
        "code": "002714",
        "stock_name": "牧原股份",
        "date": "2026-09-08",
        "news_window_days": 3,
        "today": {
            "close": 40.0,
            "pct_chg": 0.2,
            "volume": 1000000,
            "amount": 40000000,
            "ma5": 39.8,
            "ma10": 39.1,
            "ma20": 38.5,
        },
        "ma_status": "均线缠绕",
        "portfolio_context": {
            "source": "paper_trading",
            "holding_state": holding_state,
            "quantity": 0 if holding_state == "empty" else 500,
            "avg_cost": None if holding_state == "empty" else 39.5,
            "cash": 80000,
            "total_exposure_pct": 20,
        },
        "paper_trading_style_context": {
            "trading_style": style,
            "policy_version": "test",
            "settings": {
                "analysis_window_sessions": 60,
                "core_ma_windows": [10, 20, 60],
                "volatility_windows": [14],
                "max_holding_sessions": 30,
                "plan_refresh_cadence_sessions": 1,
                "compatible_horizons": ["medium"],
                "default_holding_sessions": 15,
            },
        },
        "paper_trading_style_indicators": {
            "bars_observed": 60,
            "moving_averages": {"ma10": 39.1, "ma20": 38.5, "ma60": 37.2},
            "volatility": {"atr14": 1.2},
        },
    }


def test_system_prompt_allows_empty_buy_and_forbids_empty_sell_words() -> None:
    prompt = _analyzer()._get_analysis_system_prompt("zh", "002714")

    assert '"empty": {"action": "buy/watch/avoid/alert"' in prompt
    assert "空仓可以买入" in prompt
    assert "不得使用持有/减仓/卖出" in prompt
    assert "不得写“空仓不能买入”" in prompt
    assert '"holding": {\n            "action": "add/hold/reduce/sell/watch/avoid/alert"' in prompt


def test_legacy_prompt_no_longer_uses_ma5_stack_as_universal_buy_gate() -> None:
    prompt = _analyzer(legacy=True)._get_analysis_system_prompt("zh", "002714")

    assert "MA5/MA10/MA20 多头排列是短周期趋势参考" in prompt
    assert "所有交易风格的买入硬门槛" in prompt
    assert "中线/持仓需结合 MA10/MA20/MA60" in prompt
    assert "趋势风格需结合 MA20/MA60/MA120" in prompt
    assert "多头排列必须条件" not in prompt
    assert "严禁追高！直接判定" not in prompt
    assert "- ✅ 多头排列：MA5 > MA10 > MA20" not in prompt


def test_style_context_overrides_legacy_default_prompt_conflicts() -> None:
    prompt = _analyzer(legacy=True)._get_analysis_system_prompt(
        "zh",
        "002714",
        paper_trading_style_context={
            "trading_style": "medium_position",
            "settings": {
                "analysis_window_sessions": 60,
                "core_ma_windows": [10, 20, 60],
            },
        },
    )

    assert "纸面交易风格覆盖默认短周期基线" in prompt
    assert "默认技能基线" not in prompt
    assert "专注于趋势交易" not in prompt
    assert "多头排列必须条件" not in prompt
    assert "只做多头排列的股票" not in prompt
    assert "严禁追高！直接判定" not in prompt
    assert "不得把 `MA5 > MA10 > MA20`" in prompt
    assert "`confidence_level` 必须只填写单独标签" in prompt


def test_generated_prompt_separates_holding_thesis_from_entry_timing() -> None:
    prompt = _analyzer()._format_prompt(
        _minimal_context(holding_state="empty", style="medium_position"),
        "牧原股份",
        news_context=None,
        report_language="zh",
    )

    assert "当前标的持仓状态 | empty" in prompt
    assert "证据充分时可以 `buy`" in prompt
    assert "不得写“空仓不能买入”" in prompt
    assert "持仓 thesis" in prompt
    assert "MA5 乖离、MA5/MA10 短均线更多用于是否等待更优买点" in prompt
    assert "中线风格的核心判断应结合 MA10/MA20/MA60" in prompt
    assert "不代表没有事件、没有利空或基本面已经更新" in prompt


def test_context_pack_prompt_exposes_style_and_evidence_limit_rules() -> None:
    section = format_analysis_context_pack_prompt_section(
        {
            "subject": {"code": "002714", "stock_name": "牧原股份", "market": "cn"},
            "blocks": {
                "quote": {"status": "available"},
                "news": {"status": "missing"},
                "fundamentals": {"status": "stale"},
            },
            "data_quality": {"overall_score": 70, "level": "usable", "limitations": []},
            "metadata": {
                "paper_trading_style_context": {
                    "trading_style": "medium_position",
                    "settings": {
                        "analysis_window_sessions": 60,
                        "core_ma_windows": [10, 20, 60],
                        "volatility_windows": [14],
                        "max_holding_sessions": 30,
                        "plan_refresh_cadence_sessions": 1,
                    },
                },
                "paper_trading_style_indicators": {"bars_observed": 60},
            },
        },
        report_language="zh",
    )

    assert "纸面交易风格契约" in section
    assert "MA5/短均线排列主要是时机证据" in section
    assert "不得把 MA5>MA10>MA20 当作所有风格的买入硬门槛" in section
    assert "当前窗口没有可引用新闻证据" in section
    assert "陈旧基本面只能作历史背景" in section
