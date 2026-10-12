# -*- coding: utf-8 -*-
"""Agent realtime quote tool contract tests."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.agent.tools.data_tools import _handle_get_realtime_quote, get_realtime_quote_tool
from src.agent.tools.execution import serialize_tool_result
from src.agent.tools.registry import ToolRegistry


def test_realtime_quote_tool_preserves_stale_and_fallback_metadata_for_agent_context() -> None:
    quote = SimpleNamespace(
        code="600519",
        name="贵州茅台",
        price=1680.0,
        change_pct=1.2,
        change_amount=19.9,
        volume=123456,
        amount=987654321,
        volume_ratio=1.1,
        turnover_rate=0.8,
        amplitude=2.0,
        open_price=1660.0,
        high=1690.0,
        low=1650.0,
        pre_close=1660.1,
        pe_ratio=20.1,
        pb_ratio=6.5,
        total_mv=1.6e12,
        circ_mv=1.6e12,
        change_60d=3.4,
        source=SimpleNamespace(value="tencent"),
        fetched_at="2026-09-08T10:30:00+08:00",
        provider_timestamp="2026-09-08T09:20:00+08:00",
        stale_seconds=4200,
        is_stale=True,
        fallback_from="efinance",
    )
    manager = SimpleNamespace(get_realtime_quote=MagicMock(return_value=quote))

    with patch("src.agent.tools.data_tools._get_fetcher_manager", return_value=manager):
        payload = _handle_get_realtime_quote("600519")

    assert payload["fetched_at"] == "2026-09-08T10:30:00+08:00"
    assert payload["provider_timestamp"] == "2026-09-08T09:20:00+08:00"
    assert payload["stale_seconds"] == 4200
    assert payload["is_stale"] is True
    assert payload["fallback_from"] == "efinance"
    assert payload["quote_freshness_status"] == "stale"
    assert "quote_stale" in payload["data_limitations"]
    assert "quote_fallback" in payload["data_limitations"]

    agent_input = json.loads(serialize_tool_result(payload))
    assert agent_input["is_stale"] is True
    assert agent_input["fallback_from"] == "efinance"
    assert "quote_stale" in agent_input["data_limitations"]


def test_realtime_quote_tool_marks_unknown_timestamp_as_not_proven_live() -> None:
    quote = SimpleNamespace(
        code="600519",
        name="贵州茅台",
        price=1680.0,
        change_pct=1.2,
        change_amount=None,
        volume=None,
        amount=None,
        volume_ratio=None,
        turnover_rate=None,
        amplitude=None,
        open_price=None,
        high=None,
        low=None,
        pre_close=None,
        pe_ratio=None,
        pb_ratio=None,
        total_mv=None,
        circ_mv=None,
        change_60d=None,
        source=SimpleNamespace(value="akshare"),
        fetched_at="2026-09-08T10:30:00+08:00",
        provider_timestamp=None,
        stale_seconds=None,
        is_stale=None,
    )
    manager = SimpleNamespace(get_realtime_quote=MagicMock(return_value=quote))

    with patch("src.agent.tools.data_tools._get_fetcher_manager", return_value=manager):
        payload = _handle_get_realtime_quote("600519")

    assert payload["quote_freshness_status"] == "unknown"
    assert "is_stale" not in payload
    assert "quote_freshness_unknown" in payload["data_limitations"]
    assert "do not use it as current execution/trade price evidence" in payload["quote_freshness_warning"]


def test_realtime_quote_tool_does_not_trust_default_false_without_provider_timestamp() -> None:
    quote = SimpleNamespace(
        code="600519",
        name="贵州茅台",
        price=1680.0,
        change_pct=1.2,
        change_amount=None,
        volume=None,
        amount=None,
        volume_ratio=None,
        turnover_rate=None,
        amplitude=None,
        open_price=None,
        high=None,
        low=None,
        pre_close=None,
        pe_ratio=None,
        pb_ratio=None,
        total_mv=None,
        circ_mv=None,
        change_60d=None,
        source=SimpleNamespace(value="akshare"),
        fetched_at="2026-09-08T10:30:00+08:00",
        provider_timestamp=None,
        stale_seconds=None,
        is_stale=False,
    )
    manager = SimpleNamespace(get_realtime_quote=MagicMock(return_value=quote))

    with patch("src.agent.tools.data_tools._get_fetcher_manager", return_value=manager):
        payload = _handle_get_realtime_quote("600519")

    assert payload["quote_freshness_status"] == "unknown"
    assert "is_stale" not in payload
    assert "quote_freshness_unknown" in payload["data_limitations"]
    assert "do not use it as current execution/trade price evidence" in payload["quote_freshness_warning"]


def test_realtime_quote_tool_accepts_false_stale_only_with_complete_provider_freshness() -> None:
    quote = SimpleNamespace(
        code="600519",
        name="贵州茅台",
        price=1680.0,
        change_pct=1.2,
        change_amount=None,
        volume=None,
        amount=None,
        volume_ratio=None,
        turnover_rate=None,
        amplitude=None,
        open_price=None,
        high=None,
        low=None,
        pre_close=None,
        pe_ratio=None,
        pb_ratio=None,
        total_mv=None,
        circ_mv=None,
        change_60d=None,
        source=SimpleNamespace(value="akshare"),
        fetched_at="2026-09-08T10:30:00+08:00",
        provider_timestamp="2026-09-08T10:29:30+08:00",
        stale_seconds=30,
        is_stale=False,
    )
    manager = SimpleNamespace(get_realtime_quote=MagicMock(return_value=quote))

    with patch("src.agent.tools.data_tools._get_fetcher_manager", return_value=manager):
        payload = _handle_get_realtime_quote("600519")

    assert payload["quote_freshness_status"] == "fresh"
    assert payload["is_stale"] is False
    assert "quote_freshness_warning" not in payload


def test_realtime_quote_tool_registry_serializes_unknown_freshness_warning_to_agent_context() -> None:
    quote = SimpleNamespace(
        code="600519",
        name="贵州茅台",
        price=1680.0,
        change_pct=1.2,
        change_amount=None,
        volume=None,
        amount=None,
        volume_ratio=None,
        turnover_rate=None,
        amplitude=None,
        open_price=None,
        high=None,
        low=None,
        pre_close=None,
        pe_ratio=None,
        pb_ratio=None,
        total_mv=None,
        circ_mv=None,
        change_60d=None,
        source=SimpleNamespace(value="akshare"),
        fetched_at="2026-09-08T10:30:00+08:00",
        provider_timestamp=None,
        stale_seconds=None,
        is_stale=False,
        fallback_from="primary_realtime_provider",
    )
    manager = SimpleNamespace(get_realtime_quote=MagicMock(return_value=quote))
    registry = ToolRegistry()
    registry.register(get_realtime_quote_tool)

    descriptor = registry.to_openai_tools()[0]["function"]["description"]
    assert "stale or unknown freshness must not be used" in descriptor
    assert "fallback_from" in descriptor

    with patch("src.agent.tools.data_tools._get_fetcher_manager", return_value=manager):
        payload = registry.execute("get_realtime_quote", stock_code="600519")

    agent_context = serialize_tool_result(payload)
    assert "quote_freshness_status" in agent_context
    assert "unknown" in agent_context
    assert "fallback_from" in agent_context
    assert "do not use it as current execution/trade price evidence" in agent_context
