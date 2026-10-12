# -*- coding: utf-8 -*-
"""Deterministic tests for the bounded CSRC+Tencent derived provider."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest
import requests

from data_provider.csrc_board_provider import CsrcBoardConstituentProvider


SESSION = date(2026, 9, 14)


def _classification(industry: str = "C27医药制造业", *, count: int = 5):
    rows = []
    for idx in range(count):
        code = f"600{idx + 1:03d}"
        rows.append(
            {
                "updateDate": "2026-09-14",
                "code": f"sh.{code}",
                "code_name": f"股票{idx + 1}",
                "industry": industry,
                "industryClassification": "证监会行业分类",
            }
        )
    return rows


def _payload(symbol: str, *, change: float = 1.0, provider_time: str = "20260914140000", code: str | None = None):
    fields = ["0"] * 50
    fields[1] = f"名称{symbol[-3:]}"
    fields[2] = code or symbol[2:]
    fields[3] = "10.00"
    fields[4] = "9.90"
    fields[5] = "9.95"
    fields[6] = "1000"
    fields[30] = provider_time
    fields[31] = "0.10"
    fields[32] = str(change)
    fields[33] = "10.10"
    fields[34] = "9.80"
    fields[35] = "10.00/1000/1234567.89"
    fields[37] = "123.45"
    fields[38] = "0.50"
    fields[44] = "20"
    fields[49] = "1.23"
    return f'v_{symbol}="{"~".join(fields)}";'


def _patch_tencent_session(monkeypatch, fake_get):
    class FakeSession:
        def get(self, url, headers=None, timeout=None):
            return fake_get(url, headers=headers, timeout=timeout)

        def close(self):
            pass

    monkeypatch.setattr("data_provider.csrc_board_provider.requests.Session", FakeSession)


@pytest.fixture(autouse=True)
def isolated_provider(monkeypatch):
    CsrcBoardConstituentProvider._reset_shared_cache_for_tests()
    monkeypatch.setattr(
        "data_provider.csrc_board_provider.get_effective_trading_date",
        lambda market: SESSION,
    )
    monkeypatch.setattr(
        "data_provider.csrc_board_provider.is_market_open",
        lambda market, check_date, strict=False: check_date == SESSION,
    )
    monkeypatch.setattr(
        "data_provider.csrc_board_provider.datetime",
        _FixedDateTime,
    )
    yield
    CsrcBoardConstituentProvider._reset_shared_cache_for_tests()


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 9, 14, 14, 1, 0)
        if tz is not None:
            return value.replace(tzinfo=tz)
        return value


def _wire(monkeypatch, rows, changes=None, *, omit_symbols=frozenset(), duplicate_symbol=None, foreign_symbol=None):
    changes = changes or {}

    def fake_classification():
        return rows

    def fake_timeout(func, *args, **kwargs):
        call_name = kwargs.pop("call_name")
        timeout = kwargs.pop("timeout")
        if call_name == "csrc_classification":
            assert timeout == 60.0
            return func()
        assert call_name == "csrc_tencent_quotes"
        assert 0 < timeout <= 120.0
        return func(*args, **kwargs)

    def fake_get(url, headers=None, timeout=None):
        assert timeout == (3.0, 8.0)
        requested = url.split("=", 1)[1].split(",")
        parts = []
        for symbol in requested:
            if symbol in omit_symbols:
                continue
            parts.append(_payload(symbol, change=changes.get(symbol, 1.0)))
        if duplicate_symbol:
            parts.append(_payload(duplicate_symbol, change=2.0))
        if foreign_symbol:
            parts.append(_payload(foreign_symbol, change=2.0))
        return SimpleNamespace(status_code=200, text="".join(parts), encoding=None)

    monkeypatch.setattr("data_provider.sina_board_provider._fetch_csrc_industries", fake_classification)
    monkeypatch.setattr("data_provider.akshare_fetcher._akshare_call_with_timeout", fake_timeout)
    _patch_tencent_session(monkeypatch, fake_get)


def test_rankings_and_constituents_use_exact_csrc_identity_and_equal_weight_math(monkeypatch):
    rows = _classification(count=5)
    changes = {f"sh600{idx + 1:03d}": float(idx + 1) for idx in range(5)}
    _wire(monkeypatch, rows, changes)

    provider = CsrcBoardConstituentProvider()
    top, bottom = provider.get_sector_rankings(n=5)
    snapshot = provider.get_board_constituents(
        board_type="industry",
        board_name="医药制造业",
        board_code="csrc_C27",
    )

    assert top == [
        {
            "name": "医药制造业",
            "change_pct": 3.0,
            "code": "csrc_C27",
            "source": "csrc_tencent",
            "as_of": "2026-09-14",
            "aggregation": "equal_weight_member_return",
        }
    ]
    assert bottom == top
    assert snapshot.status == "ok"
    assert snapshot.provider == "csrc_tencent"
    assert snapshot.board_code == "csrc_C27"
    assert [(item.exchange, item.code, item.symbol, item.name) for item in snapshot.constituents][:2] == [
        ("sh", "600001", "600001.SH", "股票1"),
        ("sh", "600002", "600002.SH", "股票2"),
    ]
    assert snapshot.constituents[0].raw["涨跌幅"] == 1.0
    assert snapshot.constituents[0].raw["成交额"] == 1234567.89
    assert snapshot.constituents[0].raw["量比"] == 1.23
    assert snapshot.constituents[0].raw["price"] == 10.0
    assert snapshot.constituents[0].raw["provider_timestamp"].startswith("2026-09-14T14:00:00")
    assert snapshot.constituents[0].raw["industry_provenance"]["industry_code"] == "C27"


def test_missing_quote_blocks_entire_industry_from_ranking(monkeypatch):
    _wire(monkeypatch, _classification(count=5), omit_symbols={"sh600003"})

    with pytest.raises(RuntimeError, match="no_complete_csrc_industries"):
        CsrcBoardConstituentProvider().get_sector_rankings()


@pytest.mark.parametrize(
    "provider_time,price,preclose,change",
    [
        ("20260911150000", "10.00", "9.90", "1.0"),
        ("20260914135000", "10.00", "9.90", "1.0"),
        ("20260914140000", "0", "9.90", "1.0"),
        ("20260914140000", "10.00", "0", "1.0"),
        ("20260914140000", "10.00", "9.90", "nan"),
    ],
)
def test_stale_or_malformed_member_quote_blocks_industry(monkeypatch, provider_time, price, preclose, change):
    rows = _classification(count=5)

    def fake_get(url, headers=None, timeout=None):
        parts = []
        for symbol in url.split("=", 1)[1].split(","):
            payload = _payload(symbol, provider_time=provider_time)
            if symbol == "sh600003":
                fields = payload.split('"')[1].split("~")
                fields[3] = price
                fields[4] = preclose
                fields[32] = change
                payload = f'v_{symbol}="{"~".join(fields)}";'
            parts.append(payload)
        return SimpleNamespace(status_code=200, text="".join(parts), encoding=None)

    _wire(monkeypatch, rows)
    _patch_tencent_session(monkeypatch, fake_get)

    with pytest.raises(RuntimeError, match="no_complete_csrc_industries"):
        CsrcBoardConstituentProvider().get_sector_rankings()


def test_foreign_duplicate_and_code_mismatch_quotes_fail_closed(monkeypatch):
    _wire(monkeypatch, _classification(count=5), foreign_symbol="sz000001")
    with pytest.raises(ValueError, match="tencent_unexpected_symbol"):
        CsrcBoardConstituentProvider().get_sector_rankings()

    CsrcBoardConstituentProvider._reset_shared_cache_for_tests()
    _wire(monkeypatch, _classification(count=5), duplicate_symbol="sh600001")
    with pytest.raises(ValueError, match="tencent_duplicate_symbol"):
        CsrcBoardConstituentProvider().get_sector_rankings()

    CsrcBoardConstituentProvider._reset_shared_cache_for_tests()

    def invalid_then_valid_duplicate_get(url, headers=None, timeout=None):
        requested = url.split("=", 1)[1].split(",")
        parts = [_payload("sh600001", change=1.0, provider_time="20260911150000")]
        parts.extend(_payload(symbol, change=1.0) for symbol in requested)
        return SimpleNamespace(status_code=200, text="".join(parts), encoding=None)

    _wire(monkeypatch, _classification(count=5))
    _patch_tencent_session(monkeypatch, invalid_then_valid_duplicate_get)
    with pytest.raises(ValueError, match="tencent_duplicate_symbol"):
        CsrcBoardConstituentProvider().get_sector_rankings()

    CsrcBoardConstituentProvider._reset_shared_cache_for_tests()

    def mismatch_get(url, headers=None, timeout=None):
        return SimpleNamespace(
            status_code=200,
            text="".join(_payload(symbol, code="000001" if symbol == "sh600001" else None) for symbol in url.split("=", 1)[1].split(",")),
            encoding=None,
        )

    _wire(monkeypatch, _classification(count=5))
    _patch_tencent_session(monkeypatch, mismatch_get)
    with pytest.raises(ValueError, match="tencent_code_mismatch"):
        CsrcBoardConstituentProvider().get_sector_rankings()


def test_conflicting_classification_code_name_and_historical_requests(monkeypatch):
    rows = _classification(count=5) + [
        {
            "updateDate": "2026-09-14",
            "code": "sh.600900",
            "code_name": "冲突",
            "industry": "C27另一个名称",
            "industryClassification": "证监会行业分类",
        }
    ]
    _wire(monkeypatch, rows)
    with pytest.raises(ValueError, match="csrc_code_name_conflict"):
        CsrcBoardConstituentProvider().get_sector_rankings()

    CsrcBoardConstituentProvider._reset_shared_cache_for_tests()
    _wire(monkeypatch, _classification(count=5))
    snapshot = CsrcBoardConstituentProvider().get_board_constituents(
        board_type="industry",
        board_name="医药制造业",
        board_code="csrc_C28",
    )
    assert snapshot.status == "failed"
    assert snapshot.errors == ["board_code_name_mismatch"]

    historical = CsrcBoardConstituentProvider().get_board_constituents(
        board_type="industry",
        board_name="医药制造业",
        as_of=SESSION - timedelta(days=1),
    )
    assert historical.status == "failed"
    assert "historical_membership_unavailable" in historical.errors


def test_shared_cache_date_rollover_singleflight_cooldown_and_budgets(monkeypatch):
    calls = {"classification": 0, "http": 0}

    def fake_classification():
        calls["classification"] += 1
        return _classification(count=5)

    def fake_timeout(func, *args, **kwargs):
        call_name = kwargs.pop("call_name", None)
        kwargs.pop("timeout", None)
        if call_name == "csrc_classification":
            return func()
        return func(*args, **kwargs)

    def fake_get(url, headers=None, timeout=None):
        calls["http"] += 1
        return SimpleNamespace(
            status_code=200,
            text="".join(_payload(symbol) for symbol in url.split("=", 1)[1].split(",")),
            encoding=None,
        )

    monkeypatch.setattr("data_provider.sina_board_provider._fetch_csrc_industries", fake_classification)
    monkeypatch.setattr("data_provider.akshare_fetcher._akshare_call_with_timeout", fake_timeout)
    _patch_tencent_session(monkeypatch, fake_get)

    CsrcBoardConstituentProvider().get_sector_rankings()
    CsrcBoardConstituentProvider().get_sector_rankings()
    assert calls == {"classification": 1, "http": 1}

    monkeypatch.setattr("data_provider.csrc_board_provider.get_effective_trading_date", lambda market: date(2026, 9, 15))
    monkeypatch.setattr(
        "data_provider.csrc_board_provider.is_market_open",
        lambda market, check_date, strict=False: False,
    )
    with pytest.raises(RuntimeError, match="cn_trading_calendar_unavailable"):
        CsrcBoardConstituentProvider().get_sector_rankings()

    CsrcBoardConstituentProvider._reset_shared_cache_for_tests()
    monkeypatch.setattr("data_provider.csrc_board_provider.get_effective_trading_date", lambda market: SESSION)
    monkeypatch.setattr(
        "data_provider.csrc_board_provider.is_market_open",
        lambda market, check_date, strict=False: check_date == SESSION,
    )
    monkeypatch.setattr("data_provider.sina_board_provider._fetch_csrc_industries", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    provider = CsrcBoardConstituentProvider()
    with pytest.raises(RuntimeError, match="boom"):
        provider.get_sector_rankings()
    with pytest.raises(RuntimeError, match="RuntimeError:boom"):
        provider.get_sector_rankings()

    CsrcBoardConstituentProvider._reset_shared_cache_for_tests()
    monkeypatch.setattr("data_provider.sina_board_provider._fetch_csrc_industries", fake_classification)
    monotonic = iter([0.0, 0.0, 0.0, 121.0, 121.0])
    monkeypatch.setattr("data_provider.csrc_board_provider.time.monotonic", lambda: next(monotonic))
    with pytest.raises(TimeoutError, match="csrc_tencent_build_timeout"):
        CsrcBoardConstituentProvider().get_sector_rankings()


def test_quote_stage_timeout_uses_remaining_budget_and_does_not_cache(monkeypatch):
    calls = {"classification": 0, "quote_timeout": 0, "http": 0}
    quote_should_timeout = True

    def fake_classification():
        calls["classification"] += 1
        return _classification(count=5)

    def fake_timeout(func, *args, **kwargs):
        call_name = kwargs.pop("call_name")
        timeout = kwargs.pop("timeout")
        if call_name == "csrc_classification":
            assert timeout == 60.0
            return func()
        assert call_name == "csrc_tencent_quotes"
        assert 0 < timeout <= 120.0
        calls["quote_timeout"] += 1
        if quote_should_timeout:
            raise TimeoutError("quote_stage_timeout")
        return func(*args, **kwargs)

    def fake_get(url, headers=None, timeout=None):
        calls["http"] += 1
        return SimpleNamespace(
            status_code=200,
            text="".join(_payload(symbol) for symbol in url.split("=", 1)[1].split(",")),
            encoding=None,
        )

    monkeypatch.setattr("data_provider.sina_board_provider._fetch_csrc_industries", fake_classification)
    monkeypatch.setattr("data_provider.akshare_fetcher._akshare_call_with_timeout", fake_timeout)
    _patch_tencent_session(monkeypatch, fake_get)

    provider = CsrcBoardConstituentProvider(failure_cooldown_seconds=0)
    with pytest.raises(TimeoutError, match="quote_stage_timeout"):
        provider.get_sector_rankings()
    assert CsrcBoardConstituentProvider._state.snapshot is None
    assert calls == {"classification": 1, "quote_timeout": 1, "http": 0}

    quote_should_timeout = False
    top, _ = provider.get_sector_rankings()
    assert top[0]["code"] == "csrc_C27"
    assert calls == {"classification": 2, "quote_timeout": 2, "http": 1}


@pytest.mark.parametrize("failed_call,count", [(1, 5), (2, 105)])
def test_tencent_session_retries_first_or_middle_batch_timeout_once(monkeypatch, failed_call, count):
    calls = {"classification": 0, "http": 0, "sessions": 0, "sleep": 0}

    def fake_classification():
        calls["classification"] += 1
        return _classification(count=count)

    def fake_timeout(func, *args, **kwargs):
        call_name = kwargs.pop("call_name")
        kwargs.pop("timeout")
        if call_name == "csrc_classification":
            return func()
        assert call_name == "csrc_tencent_quotes"
        return func(*args, **kwargs)

    def fake_get(url, headers=None, timeout=None):
        calls["http"] += 1
        if calls["http"] == failed_call:
            raise requests.exceptions.Timeout("one bounded retry")
        return SimpleNamespace(
            status_code=200,
            text="".join(_payload(symbol) for symbol in url.split("=", 1)[1].split(",")),
            encoding=None,
        )

    class FakeSession:
        def __init__(self):
            calls["sessions"] += 1

        def get(self, url, headers=None, timeout=None):
            return fake_get(url, headers=headers, timeout=timeout)

        def close(self):
            pass

    monkeypatch.setattr("data_provider.sina_board_provider._fetch_csrc_industries", fake_classification)
    monkeypatch.setattr("data_provider.akshare_fetcher._akshare_call_with_timeout", fake_timeout)
    monkeypatch.setattr("data_provider.csrc_board_provider.requests.Session", FakeSession)
    monkeypatch.setattr("data_provider.csrc_board_provider.time.sleep", lambda seconds: calls.__setitem__("sleep", calls["sleep"] + 1))

    top, _ = CsrcBoardConstituentProvider().get_sector_rankings()

    assert top[0]["code"] == "csrc_C27"
    assert calls["classification"] == 1
    assert calls["sessions"] == 1
    assert calls["http"] == (2 if failed_call == 1 else 3)
    assert calls["sleep"] >= 1


def test_tencent_session_two_timeouts_fail_and_do_not_cache_snapshot(monkeypatch):
    calls = {"classification": 0, "http": 0}
    should_fail = True

    def fake_classification():
        calls["classification"] += 1
        return _classification(count=5)

    def fake_timeout(func, *args, **kwargs):
        call_name = kwargs.pop("call_name")
        kwargs.pop("timeout")
        if call_name == "csrc_classification":
            return func()
        return func(*args, **kwargs)

    def fake_get(url, headers=None, timeout=None):
        calls["http"] += 1
        if should_fail:
            raise requests.exceptions.ConnectionError("still down")
        return SimpleNamespace(
            status_code=200,
            text="".join(_payload(symbol) for symbol in url.split("=", 1)[1].split(",")),
            encoding=None,
        )

    monkeypatch.setattr("data_provider.sina_board_provider._fetch_csrc_industries", fake_classification)
    monkeypatch.setattr("data_provider.akshare_fetcher._akshare_call_with_timeout", fake_timeout)
    _patch_tencent_session(monkeypatch, fake_get)
    monkeypatch.setattr("data_provider.csrc_board_provider.time.sleep", lambda seconds: None)

    provider = CsrcBoardConstituentProvider(failure_cooldown_seconds=0)
    with pytest.raises(requests.exceptions.ConnectionError, match="still down"):
        provider.get_sector_rankings()
    assert calls == {"classification": 1, "http": 2}
    assert CsrcBoardConstituentProvider._state.snapshot is None

    should_fail = False
    top, _ = provider.get_sector_rankings()
    assert top[0]["code"] == "csrc_C27"
    assert calls == {"classification": 2, "http": 3}


def test_real_baostock_field_direction_and_row_fail_closed(monkeypatch):
    rows = _classification(count=5)
    rows[2]["updateDate"] = ""
    _wire(monkeypatch, rows)

    with pytest.raises(ValueError, match="classification_update_date_missing"):
        CsrcBoardConstituentProvider().get_sector_rankings()


def test_nonempty_shsz_row_with_wrong_taxonomy_fails_closed(monkeypatch):
    rows = _classification(count=5)
    rows[2]["industryClassification"] = "申万行业分类"
    _wire(monkeypatch, rows)

    with pytest.raises(ValueError, match="classification_taxonomy_mismatch"):
        CsrcBoardConstituentProvider().get_sector_rankings()


def test_empty_industry_and_non_shsz_scope_rows_are_excluded(monkeypatch):
    rows = _classification(count=5) + [
        {
            "updateDate": "2026-09-14",
            "code": "sh.600900",
            "code_name": "空行业",
            "industry": "",
            "industryClassification": "申万行业分类",
        },
        {
            "updateDate": "2026-09-14",
            "code": "bj.920001",
            "code_name": "北交所",
            "industry": "C27医药制造业",
            "industryClassification": "申万行业分类",
        },
    ]
    _wire(monkeypatch, rows)

    top, _ = CsrcBoardConstituentProvider().get_sector_rankings()
    assert top[0]["code"] == "csrc_C27"


def test_effective_session_fallback_rejects_fail_open_natural_or_invalid_dates(monkeypatch):
    class PreOpenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 9, 14, 8, 50, 0)
            if tz is not None:
                return value.replace(tzinfo=tz)
            return value

    monkeypatch.setattr("data_provider.csrc_board_provider.datetime", PreOpenDateTime)
    monkeypatch.setattr("data_provider.csrc_board_provider.is_market_open", lambda market, check_date, strict=False: False)
    monkeypatch.setattr("data_provider.csrc_board_provider.get_effective_trading_date", lambda market: SESSION)
    with pytest.raises(RuntimeError, match="cn_trading_calendar_unavailable"):
        CsrcBoardConstituentProvider._effective_session_date()

    monkeypatch.setattr(
        "data_provider.csrc_board_provider.is_market_open",
        lambda market, check_date, strict=False: True,
    )
    monkeypatch.setattr("data_provider.csrc_board_provider.get_effective_trading_date", lambda market: date(2026, 9, 15))
    with pytest.raises(RuntimeError, match="cn_trading_calendar_unavailable"):
        CsrcBoardConstituentProvider._effective_session_date()

    monkeypatch.setattr("data_provider.csrc_board_provider.get_effective_trading_date", lambda market: SESSION)
    with pytest.raises(RuntimeError, match="cn_trading_calendar_unavailable"):
        CsrcBoardConstituentProvider._effective_session_date()


def test_effective_session_fallback_accepts_verified_past_completed_session(monkeypatch):
    class PreOpenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 9, 14, 8, 50, 0)
            if tz is not None:
                return value.replace(tzinfo=tz)
            return value

    previous = date(2026, 9, 11)
    monkeypatch.setattr("data_provider.csrc_board_provider.datetime", PreOpenDateTime)
    monkeypatch.setattr(
        "data_provider.csrc_board_provider.is_market_open",
        lambda market, check_date, strict=False: check_date == previous,
    )
    monkeypatch.setattr("data_provider.csrc_board_provider.get_effective_trading_date", lambda market: previous)

    assert CsrcBoardConstituentProvider._effective_session_date() == previous


def test_after_close_accepts_same_day_close_quotes(monkeypatch):
    class AfterCloseDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 9, 14, 20, 50, 0)
            if tz is not None:
                return value.replace(tzinfo=tz)
            return value

    monkeypatch.setattr("data_provider.csrc_board_provider.datetime", AfterCloseDateTime)
    rows = _classification(count=5)

    def fake_classification():
        return rows

    def fake_timeout(func, *args, **kwargs):
        call_name = kwargs.pop("call_name", None)
        kwargs.pop("timeout", None)
        if call_name == "csrc_classification":
            return func()
        return func(*args, **kwargs)

    def fake_get(url, headers=None, timeout=None):
        return SimpleNamespace(
            status_code=200,
            text="".join(
                _payload(symbol, change=1.0, provider_time="20260914161400")
                for symbol in url.split("=", 1)[1].split(",")
            ),
            encoding=None,
        )

    monkeypatch.setattr("data_provider.sina_board_provider._fetch_csrc_industries", fake_classification)
    monkeypatch.setattr("data_provider.akshare_fetcher._akshare_call_with_timeout", fake_timeout)
    _patch_tencent_session(monkeypatch, fake_get)

    top, _ = CsrcBoardConstituentProvider().get_sector_rankings()
    assert top[0]["code"] == "csrc_C27"
