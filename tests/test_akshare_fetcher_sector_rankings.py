from types import SimpleNamespace

import pandas as pd
import pytest

import data_provider.akshare_fetcher as akshare_fetcher


def _fetcher(monkeypatch):
    monkeypatch.setattr(akshare_fetcher, "get_config", lambda: SimpleNamespace(enable_eastmoney_patch=False))
    return akshare_fetcher.AkshareFetcher(sleep_min=0, sleep_max=0)


@pytest.mark.parametrize("primary_result", ["timeout", "missing_schema", "nonfinite"])
def test_sector_rankings_fail_over_to_coherent_csrc_identity(monkeypatch, primary_result):
    import data_provider.csrc_board_provider as csrc
    fetcher = _fetcher(monkeypatch)
    calls = []
    def primary(*args, **kwargs):
        calls.append(("em", kwargs))
        if primary_result == "timeout":
            raise TimeoutError("Eastmoney unavailable")
        if primary_result == "missing_schema":
            return pd.DataFrame([{"unexpected": "schema"}])
        return pd.DataFrame([{"板块名称": "无效", "涨跌幅": float("inf")}])
    expected = ([{"name": "医药制造业", "change_pct": 2.0, "code": "csrc_C27", "source": "csrc_tencent",
                  "aggregation": "equal_weight_member_return", "as_of": "2026-09-14"}], [])
    def fallback(n):
        calls.append(("csrc", n))
        return expected
    monkeypatch.setattr(akshare_fetcher, "_akshare_call_with_timeout", primary)
    monkeypatch.setattr(csrc, "CsrcBoardConstituentProvider", lambda: SimpleNamespace(get_sector_rankings=fallback))
    monkeypatch.setattr(akshare_fetcher.requests, "get", lambda *a, **k: pytest.fail("must not use old Sina directory"))
    assert fetcher.get_sector_rankings(5) == expected
    assert calls == [("em", {"timeout": akshare_fetcher._AKSHARE_SECTOR_RANKING_CALL_TIMEOUT,
                             "call_name": "ak.stock_board_industry_name_em"}), ("csrc", 5)]


def test_sector_rankings_keep_eastmoney_code_when_primary_succeeds(monkeypatch):
    import data_provider.csrc_board_provider as csrc
    fetcher = _fetcher(monkeypatch)
    monkeypatch.setattr(akshare_fetcher, "_akshare_call_with_timeout", lambda *a, **k: pd.DataFrame([
        {"板块名称": "半导体", "板块代码": "BK1036", "涨跌幅": 2.5},
        {"板块名称": "煤炭", "板块代码": "BK0437", "涨跌幅": -1.2},
    ]))
    monkeypatch.setattr(csrc, "CsrcBoardConstituentProvider", lambda: pytest.fail("fallback after primary success"))
    top, bottom = fetcher.get_sector_rankings(1)
    assert top == [{"name": "半导体", "change_pct": 2.5, "code": "BK1036", "source": "eastmoney_industry"}]
    assert bottom == [{"name": "煤炭", "change_pct": -1.2, "code": "BK0437", "source": "eastmoney_industry"}]


def test_sector_rankings_all_failed_does_not_restore_unverified_sina(monkeypatch):
    import data_provider.csrc_board_provider as csrc
    fetcher = _fetcher(monkeypatch)
    monkeypatch.setattr(akshare_fetcher, "_akshare_call_with_timeout", lambda *a, **k: pd.DataFrame())
    def unavailable(*args, **kwargs):
        raise TimeoutError("classification unavailable")
    monkeypatch.setattr(csrc, "CsrcBoardConstituentProvider", lambda: SimpleNamespace(get_sector_rankings=unavailable))
    monkeypatch.setattr(akshare_fetcher.requests, "get", lambda *a, **k: pytest.fail("no unverified fallback"))
    assert fetcher.get_sector_rankings() is None


def test_rank_top_n_filters_empty_names_and_non_finite_changes():
    frame = pd.DataFrame(
        [
            {"板块": "", "涨跌幅": 9.9},
            {"板块": float("nan"), "涨跌幅": 9.8},
            {"板块": "有效", "涨跌幅": 1.2},
            {"板块": "无限", "涨跌幅": float("inf")},
            {"板块": "无数字", "涨跌幅": "--"},
        ]
    )

    top, bottom = akshare_fetcher.AkshareFetcher._get_rank_top_n(frame, "涨跌幅", "板块", 3)

    assert top == [{"name": "有效", "change_pct": 1.2}]
    assert bottom == [{"name": "有效", "change_pct": 1.2}]


def test_concept_rank_preserves_eastmoney_identity():
    frame = pd.DataFrame([{"板块名称": "创新药", "板块代码": "BK1234", "涨跌幅": 2}])
    top, _ = akshare_fetcher.AkshareFetcher._get_rank_top_n(frame, "涨跌幅", "板块名称", 1, board_type="concept")
    assert top == [{"name": "创新药", "code": "BK1234", "source": "eastmoney_concept", "change_pct": 2.0}]
