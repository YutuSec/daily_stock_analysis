# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from data_provider.efinance_fetcher import EfinanceFetcher
from data_provider.tushare_fetcher import TushareFetcher


def test_efinance_auction_snapshot_rejects_ordinary_quote_fields():
    fetcher = EfinanceFetcher(sleep_min=0, sleep_max=0)

    with patch("data_provider.efinance_fetcher.requests.get") as request_get:
        snapshot = fetcher.get_auction_snapshot("600519", market="cn")

    assert snapshot is None
    request_get.assert_not_called()
    diagnostics = fetcher.get_auction_capability_diagnostics("600519", market="cn")
    assert diagnostics["supported"] is False
    assert diagnostics["forbidden_fields"] == ["f43", "f47", "f49"]


def test_efinance_auction_snapshot_rejects_non_cn_or_ambiguous_identity():
    fetcher = EfinanceFetcher(sleep_min=0, sleep_max=0)

    with patch("data_provider.efinance_fetcher.requests.get") as request_get:
        assert fetcher.get_auction_snapshot("AAPL", market="us") is None
        assert fetcher.get_auction_snapshot("AAPL", market="cn") is None

    request_get.assert_not_called()


def test_efinance_auction_snapshot_does_not_invent_provider_timestamp():
    fetcher = EfinanceFetcher(sleep_min=0, sleep_max=0)

    with patch("data_provider.efinance_fetcher.requests.get") as request_get:
        snapshot = fetcher.get_auction_snapshot("600519", market="cn")

    assert snapshot is None
    request_get.assert_not_called()


def test_tushare_stk_auction_snapshot_uses_dedicated_fields_without_provider_timestamp():
    fetcher = TushareFetcher()
    fetcher._api = SimpleNamespace()

    def fake_call(method_name, **kwargs):
        assert method_name == "stk_auction"
        assert kwargs["ts_code"] == "600519.SH"
        assert kwargs["trade_date"] == "20260826"
        return pd.DataFrame([{
            "ts_code": "600519.SH",
            "trade_date": "20260826",
            "vol": 120000,
            "price": 168.8,
            "amount": 20256000.0,
            "pre_close": 166.0,
            "turnover_rate": 0.2,
            "volume_ratio": 0.31,
        }])

    with patch.object(fetcher, "_get_china_now", return_value=datetime(2026, 8, 26, 9, 27)), patch.object(
        fetcher, "_call_api_with_rate_limit", side_effect=fake_call
    ):
        snapshot = fetcher.get_auction_snapshot("600519", market="cn")

    assert snapshot is not None
    assert snapshot.code == "600519.SH"
    assert snapshot.virtual_price == 168.8
    assert snapshot.matched_volume == 120000.0
    assert snapshot.provider_timestamp is None
    assert snapshot.has_attributable_data()
    assert snapshot.provenance["api_name"] == "stk_auction"
    assert snapshot.provenance["ordinary_realtime_quote_alias"] is False


def test_tushare_stk_auction_is_unavailable_outside_official_publish_window():
    fetcher = TushareFetcher()
    fetcher._api = SimpleNamespace()

    with patch.object(fetcher, "_get_china_now", return_value=datetime(2026, 8, 26, 9, 25, 59)), patch.object(
        fetcher, "_call_api_with_rate_limit"
    ) as api_call:
        assert fetcher.get_auction_snapshot("600519", market="cn") is None

    api_call.assert_not_called()
