# -*- coding: utf-8 -*-
"""Contract tests for provider-neutral AKShare board constituent snapshots."""

from __future__ import annotations

from datetime import date

import pandas as pd

from data_provider.akshare_board_provider import AkshareBoardConstituentProvider


def test_normalizes_exact_exchange_identity_and_snapshot_provenance(monkeypatch):
    provider = AkshareBoardConstituentProvider(cache_ttl_seconds=60, min_interval_seconds=0)
    frame = pd.DataFrame(
        [
            {"代码": "600519", "名称": "贵州茅台"},
            {"代码": "000001", "名称": "平安银行"},
        ]
    )
    calls = []
    monkeypatch.setattr(provider, "_fetch", lambda board_type, board_name: calls.append((board_type, board_name)) or frame)

    snapshot = provider.get_board_constituents(
        market="cn",
        board_type="industry",
        board_name="白酒",
        as_of=date(2026, 8, 31),
    )
    cached = provider.get_board_constituents(
        market="cn",
        board_type="industry",
        board_name="白酒",
        as_of=date(2026, 8, 31),
    )

    assert calls == [("industry", "白酒")]
    assert cached is snapshot
    assert snapshot.status == "ok"
    assert snapshot.provider == "akshare"
    assert snapshot.as_of == date(2026, 8, 31)
    assert snapshot.schema_version == "board-constituents-v1"
    assert [(item.exchange, item.code, item.symbol) for item in snapshot.constituents] == [
        ("sh", "600519", "600519.SH"),
        ("sz", "000001", "000001.SZ"),
    ]
    assert snapshot.snapshot_hash


def test_constituent_cache_is_scoped_by_as_of_date(monkeypatch):
    provider = AkshareBoardConstituentProvider(cache_ttl_seconds=60, min_interval_seconds=0)
    calls = []

    def fetch(*_):
        calls.append(1)
        return pd.DataFrame([{"代码": "600519", "名称": "贵州茅台"}])

    monkeypatch.setattr(provider, "_fetch", fetch)

    first = provider.get_board_constituents(
        market="cn",
        board_type="industry",
        board_name="白酒",
        as_of=date(2026, 8, 31),
    )
    second = provider.get_board_constituents(
        market="cn",
        board_type="industry",
        board_name="白酒",
        as_of=date(2026, 9, 1),
    )

    assert len(calls) == 2
    assert first.as_of == date(2026, 8, 31)
    assert second.as_of == date(2026, 9, 1)


def test_akshare_constituents_use_verified_board_code_when_available(monkeypatch):
    provider = AkshareBoardConstituentProvider(cache_ttl_seconds=0, min_interval_seconds=0)
    calls = []
    monkeypatch.setattr(
        provider,
        "_fetch",
        lambda board_type, board_symbol: calls.append((board_type, board_symbol)) or pd.DataFrame(
            [{"代码": "600519", "名称": "贵州茅台"}]
        ),
    )

    snapshot = provider.get_board_constituents(
        market="cn",
        board_type="concept",
        board_name="创新药",
        board_code="BK1234",
        as_of=date(2026, 8, 31),
    )

    assert calls == [("concept", "BK1234")]
    assert snapshot.status == "ok"
    assert snapshot.board_name == "创新药"
    assert snapshot.board_code == "BK1234"
    assert [item.symbol for item in snapshot.constituents] == ["600519.SH"]


def test_akshare_constituents_ignore_non_eastmoney_board_code(monkeypatch):
    provider = AkshareBoardConstituentProvider(cache_ttl_seconds=0, min_interval_seconds=0)
    calls = []
    monkeypatch.setattr(
        provider,
        "_fetch",
        lambda board_type, board_symbol: calls.append((board_type, board_symbol)) or pd.DataFrame(
            [{"代码": "600519", "名称": "贵州茅台"}]
        ),
    )

    snapshot = provider.get_board_constituents(
        market="cn",
        board_type="concept",
        board_name="创新药",
        board_code="gn_cxy",
        as_of=date(2026, 8, 31),
    )

    assert calls == [("concept", "创新药")]
    assert snapshot.status == "ok"
    assert snapshot.board_code == "gn_cxy"


def test_schema_drift_and_duplicate_conflict_do_not_create_ambiguous_constituents(monkeypatch):
    provider = AkshareBoardConstituentProvider(cache_ttl_seconds=0, min_interval_seconds=0)
    monkeypatch.setattr(
        provider,
        "_fetch",
        lambda *_: pd.DataFrame(
            [
                {"未知代码": "600519", "名称": "字段漂移"},
                {"代码": "600519", "名称": "贵州茅台"},
                {"代码": "600519", "名称": "冲突重复"},
                {"代码": "430001", "名称": "北交所未支持"},
            ]
        ),
    )

    snapshot = provider.get_board_constituents(
        market="cn",
        board_type="concept",
        board_name="消费",
        as_of=date(2026, 8, 31),
    )

    assert snapshot.status == "partial"
    assert [item.symbol for item in snapshot.constituents] == ["600519.SH"]
    assert any(reason.startswith("ambiguous_symbol") for reason in snapshot.errors)
    assert "duplicate_symbol:sh600519" in snapshot.errors


def test_unsupported_market_or_type_fails_closed_without_provider_call(monkeypatch):
    provider = AkshareBoardConstituentProvider(cache_ttl_seconds=0, min_interval_seconds=0)
    monkeypatch.setattr(provider, "_fetch", lambda *_: (_ for _ in ()).throw(AssertionError("must not call provider")))

    unsupported_market = provider.get_board_constituents(
        market="hk",
        board_type="concept",
        board_name="创新药",
    )
    unsupported_type = provider.get_board_constituents(
        market="cn",
        board_type="markdown_guess",
        board_name="创新药",
    )

    assert unsupported_market.status == "failed"
    assert unsupported_market.errors == ["market_not_supported"]
    assert unsupported_type.status == "failed"
    assert unsupported_type.errors == ["board_type_not_supported"]


def test_transient_provider_failure_retries_with_a_strict_bound(monkeypatch):
    provider = AkshareBoardConstituentProvider(
        cache_ttl_seconds=0,
        min_interval_seconds=0,
        max_attempts=2,
        retry_backoff_seconds=0,
    )
    calls = []

    def flaky_fetch(*_):
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError("transient")
        return pd.DataFrame([{"代码": "600519", "名称": "贵州茅台"}])

    monkeypatch.setattr(provider, "_fetch", flaky_fetch)
    snapshot = provider.get_board_constituents(
        market="cn",
        board_type="industry",
        board_name="白酒",
    )

    assert len(calls) == 2
    assert snapshot.status == "ok"
    assert snapshot.constituents[0].symbol == "600519.SH"


def test_board_history_excludes_future_rows_and_reports_conflicts(monkeypatch):
    provider = AkshareBoardConstituentProvider(cache_ttl_seconds=0, min_interval_seconds=0)
    monkeypatch.setattr(
        provider,
        "_fetch_history",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {"日期": "2026-08-24", "收盘": 100.0, "涨跌幅": 1.0, "成交额": 10_000_000},
                {"日期": "2026-08-25", "收盘": 101.0, "涨跌幅": 1.0, "成交额": 11_000_000},
                {"日期": "2026-08-26", "收盘": 102.0, "涨跌幅": 1.0, "成交额": 12_000_000},
                {"日期": "2026-08-27", "收盘": 103.0, "涨跌幅": 1.0, "成交额": 13_000_000},
                {"日期": "2026-08-28", "收盘": 104.0, "涨跌幅": 1.0, "成交额": 14_000_000},
                {"日期": "2026-08-31", "收盘": 105.0, "涨跌幅": 1.0, "成交额": 15_000_000},
                {"日期": "2026-09-01", "收盘": 999.0, "涨跌幅": 99.0, "成交额": 1},
            ]
        ),
    )

    snapshot = provider.get_board_history(
        market="cn",
        board_type="concept",
        board_name="创新药",
        as_of=date(2026, 8, 31),
        lookback_days=10,
    )

    assert snapshot.status == "partial"
    assert [bar.date for bar in snapshot.bars][-1] == date(2026, 8, 31)
    assert all(bar.date <= date(2026, 8, 31) for bar in snapshot.bars)
    assert "future_history_row:2026-09-01" in snapshot.errors
