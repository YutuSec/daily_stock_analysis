# -*- coding: utf-8 -*-
"""Fallback provider tests for market opportunity board constituents."""

from __future__ import annotations

import json
from datetime import date, datetime

import pandas as pd
import pytest

from data_provider.akshare_board_provider import (
    BOARD_CONSTITUENT_SCHEMA_VERSION,
    BoardConstituent,
    BoardConstituentSnapshot,
)
from data_provider.board_provider import CompositeBoardConstituentProvider
from data_provider.sina_board_provider import SinaConceptBoardConstituentProvider, normalize_board_display_name


def _snapshot(*, status="failed", board_type="concept", provider="primary", errors=None, constituents=None):
    return BoardConstituentSnapshot(
        market="cn",
        board_type=board_type,
        board_name="创新药",
        board_code=None,
        as_of=date(2026, 8, 31),
        provider=provider,
        fetched_at=datetime(2026, 8, 31, 16, 0),
        status=status,
        schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
        constituents=list(constituents or []),
        errors=list(errors or []),
        source_columns=[],
        snapshot_hash="hash" if constituents else "",
    )


def test_sina_exact_concept_directory_match_only(monkeypatch):
    provider = SinaConceptBoardConstituentProvider(cache_ttl_seconds=0, min_interval_seconds=0)
    monkeypatch.setattr(
        provider,
        "_fetch_spot_directory",
        lambda: pd.DataFrame(
            [
                {"板块": "创新药", "label": "gn_cxy"},
                {"板块": "创新药板块", "label": "gn_cxy_extra"},
            ]
        ),
    )
    detail_calls = []
    monkeypatch.setattr(
        provider,
        "_fetch_detail",
        lambda sector: detail_calls.append(sector) or pd.DataFrame(
            [{"代码": "600001", "名称": "测试股份", "changepercent": 2.5, "amount": 123_000_000}]
        ),
    )

    snapshot = provider.get_board_constituents(
        market="cn",
        board_type="concept",
        board_name="创新药",
        as_of=None,
    )
    near_miss = provider.get_board_constituents(
        market="cn",
        board_type="concept",
        board_name="创新药板块",
        as_of=None,
    )

    assert normalize_board_display_name(" 创 新 药 ") == "创新药"
    assert snapshot.status == "ok"
    assert snapshot.provider == "akshare_sina"
    assert snapshot.board_code == "gn_cxy"
    assert [item.symbol for item in snapshot.constituents] == ["600001.SH"]
    assert snapshot.constituents[0].raw["涨跌幅"] == 2.5
    assert snapshot.constituents[0].raw["成交额"] == 123_000_000
    assert near_miss.status == "ok"
    assert near_miss.board_code == "gn_cxy_extra"
    assert detail_calls == ["gn_cxy", "gn_cxy_extra"]


def test_sina_does_not_strip_board_suffix_for_unlisted_alias(monkeypatch):
    provider = SinaConceptBoardConstituentProvider(cache_ttl_seconds=0, min_interval_seconds=0)
    monkeypatch.setattr(provider, "_fetch_spot_directory", lambda: pd.DataFrame([{"板块": "创新药", "label": "gn_cxy"}]))
    monkeypatch.setattr(provider, "_fetch_detail", lambda *_: (_ for _ in ()).throw(AssertionError("must not fetch detail")))

    snapshot = provider.get_board_constituents(
        market="cn",
        board_type="concept",
        board_name="创新药板块",
        as_of=None,
    )

    assert snapshot.status == "failed"
    assert snapshot.errors == ["exact_board_name_not_found"]


def test_sina_ambiguous_exact_directory_name_fails_closed(monkeypatch):
    provider = SinaConceptBoardConstituentProvider(cache_ttl_seconds=0, min_interval_seconds=0)
    monkeypatch.setattr(
        provider,
        "_fetch_spot_directory",
        lambda: pd.DataFrame(
            [
                {"板块": "创新药", "label": "gn_a"},
                {"板块": "创新药", "label": "gn_b"},
                {"板块": "创新药", "label": "gn_a"},
            ]
        ),
    )
    monkeypatch.setattr(provider, "_fetch_detail", lambda *_: (_ for _ in ()).throw(AssertionError("ambiguous name must not fetch detail")))

    snapshot = provider.get_board_constituents(
        market="cn",
        board_type="concept",
        board_name="创新药",
        as_of=None,
    )

    assert snapshot.status == "failed"
    assert snapshot.errors == ["exact_board_name_not_found"]


def test_sina_industry_fails_closed_without_broad_remap(monkeypatch):
    provider = SinaConceptBoardConstituentProvider(cache_ttl_seconds=0, min_interval_seconds=0)
    monkeypatch.setattr(provider, "_fetch_spot_directory", lambda: (_ for _ in ()).throw(AssertionError("industry must not call sina")))

    snapshot = provider.get_board_constituents(
        market="cn",
        board_type="industry",
        board_name="半导体",
        as_of=None,
    )

    assert snapshot.status == "failed"
    assert "sina_taxonomy_mismatch" in snapshot.errors


def test_sina_detail_uses_bounded_https_and_reads_past_full_page(monkeypatch):
    provider = SinaConceptBoardConstituentProvider(cache_ttl_seconds=0, min_interval_seconds=0)
    calls = []
    def get_text(url, params):
        calls.append((url, params))
        page = int(params["page"])
        return json.dumps([{"symbol": f"sh{600000+i}"} for i in range(80)] if page == 1 else [{"symbol": "sz000001"}])
    monkeypatch.setattr(provider, "_get_text", get_text)
    frame = provider._fetch_detail("gn_cxy")
    assert len(frame) == 81
    assert len(calls) == 2
    assert calls[0][0].startswith("https://vip.stock.finance.sina.com.cn/")
    assert calls[1][1]["node"] == "gn_cxy"
    assert calls[1][1]["page"] == "2"


def test_sina_detail_rejects_repeated_page_instead_of_silent_truncation(monkeypatch):
    provider = SinaConceptBoardConstituentProvider(cache_ttl_seconds=0, min_interval_seconds=0)
    monkeypatch.setattr(provider, "_get_text", lambda *_: json.dumps([{"symbol": f"sh{600000+i}"} for i in range(80)]))
    with pytest.raises(ValueError, match="duplicate_sina_page_symbol"):
        provider._fetch_detail("gn_cxy")


def test_sina_normalizes_same_source_breadth_aliases():
    constituents, errors = SinaConceptBoardConstituentProvider._normalize_rows(
        pd.DataFrame(
            [
                {
                    "code": "600001",
                    "name": "测试股份",
                    "changepercent": 1.23,
                    "amount": 4567890,
                    "volume": 123400,
                }
            ]
        )
    )

    assert errors == []
    assert len(constituents) == 1
    assert constituents[0].raw["涨跌幅"] == 1.23
    assert constituents[0].raw["成交额"] == 4567890
    assert constituents[0].raw["成交量"] == 123400


def test_composite_uses_exact_sina_for_both_types_after_primary_failure():
    fallback_constituent = BoardConstituent(
        market="cn",
        exchange="sh",
        code="600001",
        symbol="600001.SH",
        name="测试股份",
    )

    class Primary:
        def get_board_constituents(self, **kwargs):
            return _snapshot(status="failed", board_type=kwargs["board_type"], provider="akshare", errors=["provider_error:RemoteDisconnected"])

    class Fallback:
        def get_board_constituents(self, **kwargs):
            return _snapshot(status="ok", board_type=kwargs["board_type"], provider="akshare_sina", constituents=[fallback_constituent])

    provider = CompositeBoardConstituentProvider(primary=Primary(), concept_fallback=Fallback(), industry_fallback=Fallback())

    concept = provider.get_board_constituents(board_type="concept", board_name="创新药")
    industry = provider.get_board_constituents(board_type="industry", board_name="半导体")

    assert concept.status == "ok"
    assert concept.provider == "akshare_sina"
    assert "primary_provider_error:RemoteDisconnected" in concept.errors
    assert industry.status == "ok"
    assert industry.provider == "akshare_sina"


def test_composite_all_source_concept_failure_is_source_attributed():
    class Primary:
        def get_board_constituents(self, **kwargs):
            return _snapshot(status="failed", board_type=kwargs["board_type"], provider="akshare", errors=["provider_error:RemoteDisconnected"])

    class Fallback:
        def get_board_constituents(self, **kwargs):
            return _snapshot(status="failed", board_type=kwargs["board_type"], provider="akshare_sina", errors=["exact_board_name_not_found"])

    provider = CompositeBoardConstituentProvider(primary=Primary(), concept_fallback=Fallback())

    snapshot = provider.get_board_constituents(board_type="concept", board_name="不存在")

    assert snapshot.status == "failed"
    assert snapshot.provider == "composite"
    assert snapshot.errors == ["provider_error:RemoteDisconnected", "fallback_exact_board_name_not_found"]


def test_sina_industry_code_must_match_exact_directory(monkeypatch):
    provider = SinaConceptBoardConstituentProvider(board_type="industry", min_interval_seconds=0)
    monkeypatch.setattr(provider, "_fetch_spot_directory", lambda: pd.DataFrame([{"板块": "医药制造业", "label": "hangye_C27"}]))
    monkeypatch.setattr(provider, "_fetch_detail", lambda _: pd.DataFrame([{"symbol": "sh600001", "code": "600001", "name": "测试"}]))
    from zoneinfo import ZoneInfo
    monkeypatch.setattr(provider, "_csrc_classifications", lambda: [{
        "code": "sh.600001", "industry": "医药制造业", "industryClassification": "证监会行业分类",
        "updateDate": datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat(),
    }])
    good = provider.get_board_constituents(board_type="industry", board_name="医药制造业", board_code="hangye_C27")
    assert good.status == "ok"
    assert good.board_code == "hangye_C27"
    for code in ("BK0001", "hangye_C28"):
        bad = provider.get_board_constituents(board_type="industry", board_name="医药制造业", board_code=code)
        assert bad.status == "failed"
        assert bad.errors == ["board_code_name_mismatch"]


def test_sina_never_backdates_live_membership(monkeypatch):
    provider = SinaConceptBoardConstituentProvider(board_type="industry")
    monkeypatch.setattr(provider, "_fetch_spot_directory", lambda: pytest.fail("must not fetch historical membership"))
    result = provider.get_board_constituents(board_type="industry", board_name="医药制造业", as_of=date(2000, 1, 1))
    assert result.status == "failed"
    assert result.as_of != date(2000, 1, 1)
    assert "historical_membership_unavailable" in result.errors


def test_composite_source_code_routes_without_cross_taxonomy_fallback():
    calls = []
    class Source:
        def __init__(self, name):
            self.name = name
        def get_board_constituents(self, **kwargs):
            calls.append(self.name)
            return _snapshot(provider=self.name)
    provider = CompositeBoardConstituentProvider(primary=Source("em"), industry_fallback=Source("sina"))
    provider.get_board_constituents(board_type="industry", board_name="行业", board_code="hangye_C27")
    assert calls == ["sina"]
    calls.clear()
    provider.get_board_constituents(board_type="industry", board_name="行业", board_code="BK001")
    assert calls == ["em"]


def test_sina_http_timeout_and_chinese_decode(monkeypatch):
    from types import SimpleNamespace
    import data_provider.sina_board_provider as module
    calls = []
    def get(url, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(content="医药制造业".encode("gb18030"), raise_for_status=lambda: None)
    monkeypatch.setattr(module.requests, "get", get)
    provider = SinaConceptBoardConstituentProvider(min_interval_seconds=0)
    assert provider._get_text("https://example.invalid", {"param": "industry"}) == "医药制造业"
    assert calls[0]["timeout"] == (3.0, 8.0)


@pytest.mark.parametrize("payload", ["null", "{}", "<html>blocked</html>", '[{"code":"600001"}]'])
def test_sina_invalid_page_is_not_accepted(monkeypatch, payload):
    provider = SinaConceptBoardConstituentProvider(min_interval_seconds=0)
    monkeypatch.setattr(provider, "_get_text", lambda *_: payload)
    with pytest.raises(ValueError):
        provider._fetch_detail("gn_cxy")


def test_sina_max_page_limit_is_failure(monkeypatch):
    provider = SinaConceptBoardConstituentProvider(min_interval_seconds=0)
    calls = []
    def fetch(_, params):
        calls.append(params["page"])
        offset = int(params["page"]) * 80
        return json.dumps([{"symbol": f"sh{600000+offset+i}"} for i in range(80)])
    monkeypatch.setattr(provider, "_get_text", fetch)
    with pytest.raises(ValueError, match="sina_pagination_limit_exceeded"):
        provider._fetch_detail("gn_cxy")
    assert len(calls) == 20


def test_sina_does_not_accept_conflicting_exchange():
    rows, errors = SinaConceptBoardConstituentProvider._normalize_rows(pd.DataFrame([{"symbol": "sz600001", "code": "600001"}]))
    assert rows == []
    assert errors == ["symbol_exchange_mismatch:sz600001"]


def test_sina_beijing_constituent_remains_explicitly_out_of_scope():
    rows, errors = SinaConceptBoardConstituentProvider._normalize_rows(pd.DataFrame([{"symbol": "bj920009", "code": "920009"}]))
    assert rows == []
    assert errors == ["unsupported_exchange:bj:920009"]


def test_csrc_identity_routes_directly_without_aliasing_sina_or_eastmoney():
    from types import SimpleNamespace
    def forbidden(**kwargs):
        pytest.fail("CSRC identities must not be remapped to legacy providers")
    calls = []
    sentinel = _snapshot(status="ok", board_type="industry", provider="csrc_tencent")
    def get(**kwargs):
        calls.append(kwargs)
        return sentinel
    provider = CompositeBoardConstituentProvider(
        primary=SimpleNamespace(get_board_constituents=forbidden),
        industry_fallback=SimpleNamespace(get_board_constituents=forbidden),
        csrc_provider=SimpleNamespace(get_board_constituents=get),
    )
    result = provider.get_board_constituents(board_type="industry", board_name="医药制造业", board_code="csrc_C27")
    assert result is sentinel
    assert calls[0]["board_code"] == "csrc_C27"


@pytest.mark.parametrize("classification, reason", [
    ({"industry": "家具制造业"}, "taxonomy_member_industry_mismatch"),
    ({"industry": ""}, "classification_industry_unavailable:603661.SH"),
    ({"industryClassification": "申万"}, "classification_standard_mismatch"),
    ({"updateDate": "2000-01-01"}, "classification_date_stale_or_future"),
    ({"updateDate": "2099-01-01"}, "classification_date_stale_or_future"),
    ({"updateDate": ""}, "classification_date_unavailable"),
    ({"code": "sz.603661"}, "classification_identity_unavailable:603661.SH"),
])
def test_industry_semantic_guard_rejects_wrong_missing_or_stale_classification(monkeypatch, classification, reason):
    provider = SinaConceptBoardConstituentProvider(board_type="industry", min_interval_seconds=0)
    row = {"code": "sh.603661", "industry": "石油加工、炼焦和核燃料加工业",
           "industryClassification": "证监会行业分类", "updateDate": "2026-09-14", **classification}
    monkeypatch.setattr(provider, "_csrc_classifications", lambda: [row])
    members, _ = provider._normalize_rows(pd.DataFrame([{"symbol": "sh603661", "code": "603661", "name": "恒林股份"}]))
    errors = provider._validate_industry_membership("石油加工、炼焦和核燃料加工业", members, date(2026, 9, 14))
    assert reason in errors


def test_industry_unavailable_validator_fails_before_scoring(monkeypatch):
    provider = SinaConceptBoardConstituentProvider(board_type="industry", min_interval_seconds=0)
    monkeypatch.setattr(provider, "_fetch_spot_directory", lambda: pd.DataFrame([{"板块": "医药制造业", "label": "hangye_ZC27"}]))
    monkeypatch.setattr(provider, "_fetch_detail", lambda _: pd.DataFrame([{"symbol": "sh600062", "code": "600062", "name": "华润双鹤"}]))
    monkeypatch.setattr(provider, "_csrc_classifications", lambda: (_ for _ in ()).throw(TimeoutError()))
    snapshot = provider.get_board_constituents(board_type="industry", board_name="医药制造业")
    assert snapshot.status == "failed"
    assert snapshot.constituents == []
    assert "taxonomy_semantic_unverified" in snapshot.errors


def test_industry_validator_requires_every_member_and_records_provenance(monkeypatch):
    provider = SinaConceptBoardConstituentProvider(board_type="industry", min_interval_seconds=0)
    rows = [{"code": "sh.600062", "industry": "医药制造业", "industryClassification": "证监会行业分类", "updateDate": "2026-09-14"}]
    monkeypatch.setattr(provider, "_csrc_classifications", lambda: rows)
    members, _ = provider._normalize_rows(pd.DataFrame([{"symbol": "sh600062", "code": "600062", "name": "华润双鹤"}]))
    assert provider._validate_industry_membership("医药制造业", members, date(2026, 9, 14)) == []
    assert members[0].raw["industry_validation"]["provider"] == "baostock"
    rows.append(dict(rows[0]))
    assert "classification_identity_unavailable:600062.SH" in provider._validate_industry_membership("医药制造业", members, date(2026, 9, 14))


def test_classification_timeout_is_bounded_and_failure_has_cooldown(monkeypatch):
    import data_provider.akshare_fetcher as module
    provider = SinaConceptBoardConstituentProvider(board_type="industry", min_interval_seconds=0)
    calls = []
    def fetch(*args, **kwargs):
        calls.append(kwargs)
        raise TimeoutError()
    monkeypatch.setattr(module, "_akshare_call_with_timeout", fetch)
    with pytest.raises(TimeoutError):
        provider._csrc_classifications()
    with pytest.raises(RuntimeError, match="classification_retry_cooldown"):
        provider._csrc_classifications()
    assert calls == [{"timeout": 60.0, "call_name": "baostock_industry"}]


@pytest.mark.parametrize("industry, accepted", [("C27医药制造业", True), ("C21家具制造业", False)])
def test_baostock_native_csrc_prefix_is_metadata_not_fuzzy_name(monkeypatch, industry, accepted):
    provider = SinaConceptBoardConstituentProvider(board_type="industry", min_interval_seconds=0)
    monkeypatch.setattr(provider, "_csrc_classifications", lambda: [{
        "code": "sh.600062", "industry": industry,
        "industryClassification": "证监会行业分类", "updateDate": "2026-09-14",
    }])
    members, _ = provider._normalize_rows(pd.DataFrame([{"symbol": "sh600062", "name": "华润双鹤"}]))
    errors = provider._validate_industry_membership("医药制造业", members, date(2026, 9, 14))
    assert (errors == []) is accepted
    if accepted:
        assert members[0].raw["industry_validation"]["industry_code"] == "C27"
    else:
        assert "taxonomy_member_industry_mismatch" in errors


@pytest.mark.parametrize("fault", [None, "missing_field", "short_row", "early_cursor_end", "full_page", "query_error"])
def test_baostock_single_page_reader_checks_complete_cursor_and_schema(monkeypatch, fault):
    import socket
    import baostock as bs
    import baostock.common.contants as constants
    from types import SimpleNamespace
    from data_provider.sina_board_provider import _fetch_csrc_industries
    fields = ["updateDate", "code", "code_name", "industry", "industryClassification"]
    values = [["2026-09-14", "sh.600062", "华润双鹤", "C27医药制造业", "证监会行业分类"]]
    if fault == "missing_field":
        fields = fields[:-1]
    if fault == "short_row":
        values = [values[0][:-1]]
    result = SimpleNamespace(fields=fields, data=values, error_code="1" if fault == "query_error" else "0")
    if fault == "full_page":
        result.data = [None] * 10000
    remaining = iter(values)
    pending = []
    def next_row():
        if fault == "early_cursor_end":
            return False
        value = next(remaining, None)
        if value is None:
            return False
        pending.append(value)
        return True
    result.next = next_row
    result.get_row_data = lambda: pending.pop(0)
    calls = []
    monkeypatch.setattr(socket, "setdefaulttimeout", lambda *_: None)
    monkeypatch.setattr(constants, "BAOSTOCK_PER_PAGE_COUNT", 2000)
    monkeypatch.setattr(bs, "login", lambda: SimpleNamespace(error_code="0"))
    monkeypatch.setattr(bs, "logout", lambda: calls.append("logout"))
    def query(**kwargs):
        calls.append(kwargs)
        assert constants.BAOSTOCK_PER_PAGE_COUNT == 10000
        return result
    monkeypatch.setattr(bs, "query_stock_industry", query)
    if fault is None:
        rows = _fetch_csrc_industries()
        assert rows[0]["industry"] == "C27医药制造业"
    else:
        with pytest.raises((ValueError, RuntimeError)):
            _fetch_csrc_industries()
    assert calls[-1] == "logout"
    assert date.fromisoformat(calls[0]["date"])
