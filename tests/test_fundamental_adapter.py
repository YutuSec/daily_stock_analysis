# -*- coding: utf-8 -*-
"""
Tests for fundamental adapter helpers.
"""

import os
import sys
import time
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data_provider.fundamental_adapter import (
    AkshareFundamentalAdapter,
    _build_dividend_payload,
    _extract_financial_indicator_row,
    _extract_latest_row,
    _parse_dividend_plan_to_per_share,
    _recent_report_periods,
)


class TestFundamentalAdapter(unittest.TestCase):
    def test_past_report_period_cannot_override_future_announcement(self) -> None:
        frame = pd.DataFrame([{'股票代码':'002714','报告期':'2026-06-30',
                               '公告日期':'2026-08-31','净利润':100}])
        row = _extract_latest_row(frame,'002714',as_of=date(2026,7,1),
                                  require_date=True,max_age_days=540)
        self.assertIsNone(row)

    def test_parse_dividend_plan_to_per_share_supports_cn_patterns(self) -> None:
        self.assertAlmostEqual(_parse_dividend_plan_to_per_share("10派3元(含税)"), 0.3, places=6)
        self.assertAlmostEqual(_parse_dividend_plan_to_per_share("每10股派发2.5元"), 0.25, places=6)
        self.assertAlmostEqual(_parse_dividend_plan_to_per_share("每股派0.8元"), 0.8, places=6)
        self.assertIsNone(_parse_dividend_plan_to_per_share("仅送股，不现金分红"))

    def test_extract_latest_row_returns_none_when_code_mismatch(self) -> None:
        df = pd.DataFrame(
            {
                "股票代码": ["600000", "000001"],
                "值": [1, 2],
            }
        )
        row = _extract_latest_row(df, "600519")
        self.assertIsNone(row)

    def test_extract_latest_row_fallback_when_no_code_column(self) -> None:
        df = pd.DataFrame({"值": [1, 2]})
        row = _extract_latest_row(df, "600519")
        self.assertIsNotNone(row)
        self.assertEqual(row["值"], 1)

    def test_extract_latest_row_date_aware_uses_newest_matching_current_row(self) -> None:
        as_of = date(2026, 9, 8)
        df = pd.DataFrame(
            {
                "股票代码": ["600519", "000001", "600519", "600519"],
                "报告期": ["2020-03-31", "2026-06-30", "2026-03-31", "2026-06-30"],
                "预告": ["stale-first-row", "wrong-symbol", "older-current", "newest-current"],
            }
        )

        row = _extract_latest_row(
            df,
            "600519",
            as_of=as_of,
            max_age_days=540,
            require_date=True,
        )

        self.assertIsNotNone(row)
        self.assertEqual(row["预告"], "newest-current")

    def test_extract_latest_row_date_aware_rejects_future_stale_and_unknown_dates(self) -> None:
        as_of = date(2026, 9, 8)
        for report_date in ("2026-12-31", "2020-03-31", ""):
            with self.subTest(report_date=report_date):
                df = pd.DataFrame(
                    {
                        "股票代码": ["600519"],
                        "报告期": [report_date],
                        "预告": ["should-not-surface"],
                    }
                )
                row = _extract_latest_row(
                    df,
                    "600519",
                    as_of=as_of,
                    max_age_days=540,
                    require_date=True,
                )
                self.assertIsNone(row)

    def test_recent_report_periods_are_completed_and_bounded(self) -> None:
        periods = _recent_report_periods(date(2026, 9, 8), max_periods=2)
        self.assertEqual(periods, ["20260630", "20260331"])
        self.assertNotIn("20260930", periods)

    def test_financial_indicator_horizontal_abstract_selects_latest_current_date_column(self) -> None:
        df = pd.DataFrame(
            {
                "指标": [
                    "营业总收入",
                    "归母净利润",
                    "经营活动产生的现金流量净额",
                    "净资产收益率",
                    "营业收入同比增长率",
                    "净利润同比增长率",
                    "销售毛利率",
                ],
                "2026-12-31": [9999, 9999, 9999, 99, 99, 99, 99],
                "2026-06-30": [1000, 300, 500, 18.2, 12.0, 9.5, 55.0],
                "2020-03-31": [100, 30, 50, 8.2, -12.0, -9.5, 35.0],
            }
        )

        row = _extract_financial_indicator_row(df, "600519", as_of=date(2026, 9, 8))

        self.assertIsNotNone(row)
        self.assertEqual(row["报告期"], "2026-06-30")
        self.assertEqual(row["营业总收入"], 1000)
        self.assertEqual(row["归母净利润"], 300)
        self.assertEqual(row["营业收入同比"], 12.0)
        self.assertEqual(row["净利润同比"], 9.5)

    def test_financial_indicator_frozen_as_of_rejects_future_horizontal_date_column(self) -> None:
        df = pd.DataFrame(
            {
                "指标": ["营业总收入", "归母净利润"],
                "2026-06-30": [1000, 300],
                "2026-03-31": [800, 250],
            }
        )

        row = _extract_financial_indicator_row(df, "600519", as_of=date(2026, 4, 15))

        self.assertIsNotNone(row)
        self.assertEqual(row["报告期"], "2026-03-31")
        self.assertEqual(row["营业总收入"], 800)

    def test_financial_indicator_row_shape_rejects_future_stale_and_unknown_dates(self) -> None:
        as_of = date(2026, 9, 8)
        for report_date in ("2026-12-31", "2020-03-31", ""):
            with self.subTest(report_date=report_date):
                df = pd.DataFrame(
                    {
                        "股票代码": ["600519"],
                        "报告期": [report_date],
                        "营业总收入": [1000.0],
                        "归母净利润": [300.0],
                    }
                )
                row = _extract_financial_indicator_row(df, "600519", as_of=as_of)
                self.assertIsNone(row)

    def test_dragon_tiger_no_match_with_code_column_is_ok(self) -> None:
        adapter = AkshareFundamentalAdapter()
        df = pd.DataFrame(
            {
                "股票代码": ["600000"],
                "日期": ["2026-01-01"],
            }
        )
        with patch.object(adapter, "_call_df_candidates", return_value=(df, "stock_lhb_stock_statistic_em", [])):
            result = adapter.get_dragon_tiger_flag("600519")
        self.assertEqual(result["status"], "ok")
        self.assertFalse(result["is_on_list"])
        self.assertEqual(result["recent_count"], 0)

    def test_dragon_tiger_match_is_ok(self) -> None:
        adapter = AkshareFundamentalAdapter()
        today = pd.Timestamp.now().strftime("%Y-%m-%d")
        df = pd.DataFrame(
            {
                "股票代码": ["600519"],
                "日期": [today],
            }
        )
        with patch.object(adapter, "_call_df_candidates", return_value=(df, "stock_lhb_stock_statistic_em", [])):
            result = adapter.get_dragon_tiger_flag("600519")
        self.assertEqual(result["status"], "ok")
        self.assertTrue(result["is_on_list"])
        self.assertGreaterEqual(result["recent_count"], 1)

    def test_fundamental_bundle_includes_financial_report_and_dividend_payload(self) -> None:
        adapter = AkshareFundamentalAdapter()
        now = datetime.now()
        within_ttm = (now - timedelta(days=30)).strftime("%Y-%m-%d")
        future_day = (now + timedelta(days=10)).strftime("%Y-%m-%d")
        old_day = (now - timedelta(days=500)).strftime("%Y-%m-%d")
        fin_df = pd.DataFrame(
            {
                "股票代码": ["600519"],
                "报告期": [within_ttm],
                "营业总收入": [1000.0],
                "归母净利润": [300.0],
                "经营活动产生的现金流量净额": [500.0],
                "净资产收益率": [18.2],
                "营业收入同比": [12.0],
                "净利润同比": [9.5],
            }
        )
        forecast_df = pd.DataFrame({"股票代码": ["600519"], "报告期": [within_ttm], "预告": ["预增"]})
        quick_df = pd.DataFrame({"股票代码": ["600519"], "报告期": [within_ttm], "快报": ["快报摘要"]})
        dividend_df = pd.DataFrame(
            {
                "股票代码": ["600519", "600519", "600519", "600519"],
                "除息日": [within_ttm, within_ttm, future_day, old_day],
                "分配方案": ["10派3元(含税)", "10派3元(含税)", "10派5元", "10派1元"],
            }
        )

        def _fake_call(candidates, **_kwargs):
            func_name, _call_kwargs = candidates[0]
            if func_name == "stock_financial_abstract":
                return fin_df, func_name, []
            if func_name == "stock_yjyg_em":
                return forecast_df, func_name, []
            if func_name == "stock_yjkb_em":
                return quick_df, func_name, []
            if func_name == "stock_fhps_detail_em":
                return dividend_df, func_name, []
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_fundamental_bundle("600519")

        financial_report = result["earnings"].get("financial_report", {})
        self.assertEqual(financial_report.get("report_date"), within_ttm)
        self.assertEqual(financial_report.get("revenue"), 1000.0)
        self.assertEqual(financial_report.get("net_profit_parent"), 300.0)
        self.assertEqual(financial_report.get("operating_cash_flow"), 500.0)
        self.assertEqual(financial_report.get("roe"), 18.2)

        dividend_payload = result["earnings"].get("dividend", {})
        events = dividend_payload.get("events", [])
        self.assertEqual(len(events), 2)  # duplicate + future day filtered
        self.assertEqual(dividend_payload.get("ttm_event_count"), 1)
        self.assertAlmostEqual(dividend_payload.get("ttm_cash_dividend_per_share"), 0.3, places=6)

    def test_fundamental_bundle_publishes_partial_progress_and_honors_deadline(self) -> None:
        adapter = AkshareFundamentalAdapter()
        fin_df = pd.DataFrame(
            {
                "指标": ["营业总收入", "归母净利润"],
                "2026-06-30": [1000.0, 300.0],
            }
        )
        calls = []
        snapshots = []

        def _fake_call(candidates, **_kwargs):
            func_name, kwargs = candidates[0]
            calls.append((func_name, dict(kwargs)))
            if func_name == "stock_financial_abstract":
                time.sleep(0.02)
                return fin_df, func_name, []
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_fundamental_bundle(
                "600519",
                as_of=date(2026, 9, 8),
                deadline=time.monotonic() + 0.01,
                on_progress=snapshots.append,
            )

        self.assertEqual(calls, [("stock_financial_abstract", {"symbol": "600519"})])
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["earnings"]["financial_report"]["report_date"], "2026-06-30")
        self.assertTrue(any("deadline_exceeded" in err for err in result["errors"]))
        self.assertTrue(snapshots)
        self.assertEqual(snapshots[0]["earnings"]["financial_report"]["report_date"], "2026-06-30")

    def test_fundamental_bundle_uses_current_horizontal_financial_abstract(self) -> None:
        adapter = AkshareFundamentalAdapter()
        fin_df = pd.DataFrame(
            {
                "指标": ["营业总收入", "归母净利润", "经营活动产生的现金流量净额", "净资产收益率"],
                "2026-12-31": [9999.0, 9999.0, 9999.0, 99.0],
                "2026-06-30": [1000.0, 300.0, 500.0, 18.2],
                "2020-03-31": [100.0, 30.0, 50.0, 8.2],
            }
        )

        def _fake_call(candidates, **_kwargs):
            func_name, _kwargs = candidates[0]
            if func_name == "stock_financial_abstract":
                return fin_df, func_name, []
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_fundamental_bundle("600519", as_of=date(2026, 9, 8))

        self.assertEqual(result["growth"].get("roe"), 18.2)
        financial_report = result["earnings"].get("financial_report", {})
        self.assertEqual(financial_report.get("report_date"), "2026-06-30")
        self.assertEqual(financial_report.get("revenue"), 1000.0)
        self.assertEqual(financial_report.get("net_profit_parent"), 300.0)
        self.assertEqual(financial_report.get("operating_cash_flow"), 500.0)

    def test_fundamental_bundle_does_not_surface_stale_financial_report_as_current(self) -> None:
        adapter = AkshareFundamentalAdapter()
        fin_df = pd.DataFrame(
            {
                "股票代码": ["600519"],
                "报告期": ["2020-03-31"],
                "营业总收入": [1000.0],
                "归母净利润": [300.0],
                "净资产收益率": [18.2],
            }
        )

        def _fake_call(candidates, **_kwargs):
            func_name, _kwargs = candidates[0]
            if func_name == "stock_financial_abstract":
                return fin_df, func_name, []
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_fundamental_bundle("600519", as_of=date(2026, 9, 8))

        self.assertEqual(result["growth"], {})
        self.assertNotIn("financial_report", result["earnings"])
        self.assertEqual(result["status"], "not_supported")

    def test_fundamental_bundle_does_not_surface_stale_forecast_as_current(self) -> None:
        adapter = AkshareFundamentalAdapter()
        stale_forecast = pd.DataFrame(
            {
                "股票代码": ["600519"],
                "报告期": ["2020-03-31"],
                "预告": ["2020Q1 stale forecast"],
            }
        )

        def _fake_call(candidates, **_kwargs):
            func_name, _kwargs = candidates[0]
            if func_name in {"stock_yjyg_em", "stock_yjbb_em", "stock_yjkb_em"}:
                return stale_forecast, func_name, []
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_fundamental_bundle("600519")

        self.assertNotIn("forecast_summary", result["earnings"])
        self.assertNotIn("quick_report_summary", result["earnings"])
        self.assertEqual(result["status"], "not_supported")
        self.assertFalse(any("no_current_row" in err for err in result["errors"]))

    def test_fundamental_bundle_uses_bounded_period_fallback_after_primary_error(self) -> None:
        adapter = AkshareFundamentalAdapter()
        as_of = date(2026, 9, 8)
        calls = []

        def _fake_call(candidates, **_kwargs):
            func_name, kwargs = candidates[0]
            calls.append((func_name, dict(kwargs)))
            if func_name == "stock_yjyg_em" and kwargs["date"] == "20260630":
                return None, None, [f"{func_name}:TypeError"]
            if func_name == "stock_yjyg_em" and kwargs["date"] == "20260331":
                return (
                    pd.DataFrame(
                        {
                            "股票代码": ["600519"],
                            "公告日期": ["2026-04-20"],
                            "业绩变动": ["预增"],
                        }
                    ),
                    func_name,
                    [],
                )
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_fundamental_bundle("600519", as_of=as_of)

        yj_calls = [kwargs for func, kwargs in calls if func == "stock_yjyg_em"]
        self.assertTrue(yj_calls)
        self.assertTrue(all("symbol" not in kwargs for kwargs in yj_calls))
        self.assertTrue(all("date" in kwargs for kwargs in yj_calls))
        self.assertEqual(yj_calls, [{"date": "20260630"}, {"date": "20260331"}])
        self.assertEqual(result["earnings"].get("forecast_summary"), "预增")
        self.assertEqual(result["earnings"]["forecast"]["announcement_date"], "2026-04-20")
        self.assertEqual(result["earnings"]["forecast"]["as_of"], "2026-09-08")
        self.assertEqual(result["earnings"]["forecast"]["source"], "stock_yjyg_em")
        self.assertIn("stock_yjyg_em:TypeError", result["errors"])
        self.assertIn("earnings_forecast:stock_yjyg_em", result["source_chain"])

    def test_yjbb_actual_report_is_not_forecast_and_ignores_date_only_rows(self) -> None:
        adapter = AkshareFundamentalAdapter()
        calls = []

        def _fake_call(candidates, **_kwargs):
            func_name, kwargs = candidates[0]
            calls.append((func_name, dict(kwargs)))
            if func_name == "stock_yjbb_em" and kwargs["date"] == "20260630":
                return (
                    pd.DataFrame(
                        {
                            "股票代码": ["600519"],
                            "报告期": ["2026-06-30"],
                            "公告日期": ["2026-08-30"],
                        }
                    ),
                    func_name,
                    [],
                )
            if func_name == "stock_yjbb_em" and kwargs["date"] == "20260331":
                return (
                    pd.DataFrame(
                        {
                            "股票代码": ["600519"],
                            "报告期": ["2026-03-31"],
                            "公告日期": ["2026-04-25"],
                            "净利润-净利润": [300.0],
                            "营业收入-营业收入": [1000.0],
                        }
                    ),
                    func_name,
                    [],
                )
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_fundamental_bundle("600519", as_of=date(2026, 9, 8))

        yjbb_calls = [kwargs for func, kwargs in calls if func == "stock_yjbb_em"]
        self.assertEqual(yjbb_calls, [{"date": "20260630"}, {"date": "20260331"}])
        self.assertNotIn("forecast_summary", result["earnings"])
        self.assertEqual(
            result["earnings"].get("report_summary"),
            "报告期2026-03-31；营业收入1000.0；净利润300.0",
        )
        self.assertEqual(result["earnings"]["actual_report"]["report_date"], "2026-03-31")
        self.assertEqual(result["earnings"]["actual_report"]["announcement_date"], "2026-04-25")
        self.assertEqual(result["earnings"]["actual_report"]["as_of"], "2026-09-08")
        self.assertEqual(result["earnings"]["actual_report"]["source"], "stock_yjbb_em")

    def test_yjbb_actual_report_does_not_treat_announcement_date_as_report_period(self) -> None:
        adapter = AkshareFundamentalAdapter()
        yjbb_df = pd.DataFrame(
            {
                "股票代码": ["600519"],
                "报告期": ["2026-06-30"],
                "最新公告日期": ["2026-08-15"],
                "净利润-净利润": [300.0],
                "营业收入-营业收入": [1000.0],
            }
        )

        def _fake_call(candidates, **_kwargs):
            func_name, _kwargs = candidates[0]
            if func_name == "stock_yjbb_em":
                return yjbb_df, func_name, []
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_fundamental_bundle("600519", as_of=date(2026, 9, 8))

        self.assertEqual(result["earnings"]["actual_report"]["report_date"], "2026-06-30")
        self.assertEqual(result["earnings"]["actual_report"]["announcement_date"], "2026-08-15")

    def test_period_scoped_actual_report_uses_query_period_when_row_has_announcement_and_metrics(self) -> None:
        adapter = AkshareFundamentalAdapter()
        yjbb_df = pd.DataFrame(
            {
                "股票代码": ["600519"],
                "最新公告日期": ["2026-08-15"],
                "净利润-净利润": [300.0],
                "营业收入-营业收入": [1000.0],
            }
        )

        def _fake_call(candidates, **_kwargs):
            func_name, kwargs = candidates[0]
            if func_name == "stock_yjbb_em" and kwargs == {"date": "20260630"}:
                return yjbb_df, func_name, []
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_fundamental_bundle("600519", as_of=date(2026, 9, 8))

        actual_report = result["earnings"]["actual_report"]
        self.assertEqual(actual_report["report_date"], "2026-06-30")
        self.assertEqual(actual_report["announcement_date"], "2026-08-15")

    def test_recent_query_period_does_not_validate_undated_row(self) -> None:
        adapter = AkshareFundamentalAdapter()

        def _fake_call(candidates, **_kwargs):
            func_name, _kwargs = candidates[0]
            if func_name == "stock_yjyg_em":
                return pd.DataFrame({"股票代码": ["600519"], "预告": ["undated old text"]}), func_name, []
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_fundamental_bundle("600519", as_of=date(2026, 9, 8))

        self.assertNotIn("forecast_summary", result["earnings"])
        self.assertEqual(result["status"], "not_supported")

    def test_institution_and_top10_use_explicit_current_period_parameters(self) -> None:
        adapter = AkshareFundamentalAdapter()
        calls = []
        inst_df = pd.DataFrame(
            {
                "股票代码": ["600519"],
                "持股比例增幅": [1.25],
            }
        )
        top10_df = pd.DataFrame(
            {
                "股东名称": ["holder-a", "holder-b"],
                "持股变化": [100.0, -25.0],
            }
        )

        def _fake_call(candidates, **_kwargs):
            func_name, kwargs = candidates[0]
            for candidate_func, candidate_kwargs in candidates:
                calls.append((candidate_func, dict(candidate_kwargs)))
            if func_name == "stock_institute_hold" and kwargs == {"symbol": "20262"}:
                return inst_df, func_name, []
            if func_name == "stock_gdfx_top_10_em" and kwargs == {"symbol": "sh600519", "date": "20260630"}:
                return top10_df, func_name, []
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_fundamental_bundle("600519", as_of=date(2026, 9, 8))

        self.assertIn(("stock_financial_analysis_indicator", {"symbol": "600519", "start_year": "2024"}), calls)
        self.assertNotIn(("stock_institute_hold", {}), calls)
        self.assertNotIn(("stock_institute_recommend", {}), calls)
        self.assertNotIn(("stock_gdfx_top_10_em", {"symbol": "600519"}), calls)
        self.assertNotIn(("stock_gdfx_top_10_em", {}), calls)
        self.assertEqual(result["institution"]["institution_holding_change"], 1.25)
        self.assertEqual(result["institution"]["top10_holder_change"], 75.0)
        self.assertEqual(result["institution"]["top10_holder_report_date"], "2026-06-30")

    def test_capital_flow_uses_installed_signatures_and_numeric_stock_flow_only(self) -> None:
        adapter = AkshareFundamentalAdapter()
        calls = []
        stock_df = pd.DataFrame(
            {
                "日期": ["2026-09-08"],
                "主力净流入-净额": ["1.5亿"],
                "5日主力净流入-净额": ["-200万"],
                "10日主力净流入-净额": [0],
            }
        )
        sector_df = pd.DataFrame(
            {
                "名称": ["白酒", "银行"],
                "今日主力净流入-净额": ["2亿", "-1亿"],
            }
        )

        def _fake_call(candidates, **_kwargs):
            func_name, kwargs = candidates[0]
            calls.append((func_name, dict(kwargs)))
            if func_name == "stock_individual_fund_flow":
                return stock_df, func_name, []
            if func_name == "stock_sector_fund_flow_rank":
                return sector_df, func_name, []
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_capital_flow("600519", top_n=1, as_of=date(2026, 9, 8))

        self.assertEqual(calls[0], ("stock_individual_fund_flow", {"stock": "600519", "market": "sh"}))
        self.assertIn(("stock_sector_fund_flow_rank", {"indicator": "今日", "sector_type": "行业资金流"}), calls)
        self.assertNotIn(("stock_individual_fund_flow", {"symbol": "600519"}), calls)
        self.assertNotIn(("stock_individual_fund_flow", {}), calls)
        self.assertEqual(result["stock_flow"], {
            "main_net_inflow": 150000000.0,
            "inflow_5d": -2000000.0,
            "inflow_10d": 0.0,
        })
        self.assertEqual(result["stock_flow_meta"], {
            "date": "2026-09-08",
            "as_of": "2026-09-08",
            "is_current": True,
            "source": "stock_individual_fund_flow",
        })
        self.assertEqual(result["sector_rankings"]["top"][0]["name"], "白酒")
        self.assertEqual(result["sector_rankings"]["bottom"][0]["net_inflow"], -100000000.0)

    def test_capital_flow_prefers_net_amount_columns_over_ratio_columns(self) -> None:
        adapter = AkshareFundamentalAdapter()
        stock_df = pd.DataFrame(
            {
                "日期": ["2026-09-08"],
                "主力净流入-净占比": [1.23],
                "主力净流入-净额": ["1.5亿"],
                "5日主力净流入-净占比": [2.34],
                "5日主力净流入-净额": ["-200万"],
                "10日主力净流入-净占比": [3.45],
                "10日主力净流入-净额": ["300万"],
            }
        )

        def _fake_call(candidates, **_kwargs):
            func_name, _kwargs = candidates[0]
            if func_name == "stock_individual_fund_flow":
                return stock_df, func_name, []
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_capital_flow("600519", as_of=date(2026, 9, 8))

        self.assertEqual(result["stock_flow"]["main_net_inflow"], 150000000.0)
        self.assertEqual(result["stock_flow"]["inflow_5d"], -2000000.0)
        self.assertEqual(result["stock_flow"]["inflow_10d"], 3000000.0)

    def test_capital_sector_flow_prefers_net_amount_columns_over_ratio_columns(self) -> None:
        adapter = AkshareFundamentalAdapter()
        sector_df = pd.DataFrame(
            {
                "名称": ["白酒", "银行"],
                "今日主力净流入-净占比": [1.23, -4.56],
                "今日主力净流入-净额": ["2亿", "-1亿"],
            }
        )

        def _fake_call(candidates, **_kwargs):
            func_name, _kwargs = candidates[0]
            if func_name == "stock_sector_fund_flow_rank":
                return sector_df, func_name, []
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            result = adapter.get_capital_flow("600519", top_n=1, as_of=date(2026, 9, 8))

        self.assertEqual(result["sector_rankings"]["top"][0]["name"], "白酒")
        self.assertEqual(result["sector_rankings"]["top"][0]["net_inflow"], 200000000.0)
        self.assertEqual(result["sector_rankings"]["bottom"][0]["name"], "银行")
        self.assertEqual(result["sector_rankings"]["bottom"][0]["net_inflow"], -100000000.0)

    def test_capital_flow_does_not_surface_stale_stock_flow_as_current(self) -> None:
        adapter = AkshareFundamentalAdapter()
        stock_df = pd.DataFrame({"日期": ["2020-03-31"], "主力净流入-净额": ["1亿"]})

        with patch.object(adapter, "_call_df_candidates", return_value=(stock_df, "stock_individual_fund_flow", [])):
            result = adapter.get_capital_flow("600519", as_of=date(2026, 9, 8))

        self.assertEqual(result["stock_flow"], {})
        self.assertEqual(result["stock_flow_meta"]["date"], "2020-03-31")
        self.assertFalse(result["stock_flow_meta"]["is_current"])
        self.assertEqual(result["status"], "not_supported")

    def test_capital_flow_previous_trading_day_does_not_count_as_current_as_of(self) -> None:
        adapter = AkshareFundamentalAdapter()
        stock_df = pd.DataFrame({"日期": ["2026-09-07"], "主力净流入-净额": ["1亿"]})

        with patch.object(adapter, "_call_df_candidates", return_value=(stock_df, "stock_individual_fund_flow", [])):
            result = adapter.get_capital_flow("600519", as_of=date(2026, 9, 8))

        self.assertEqual(result["stock_flow"], {})
        self.assertEqual(result["stock_flow_meta"]["date"], "2026-09-07")
        self.assertFalse(result["stock_flow_meta"]["is_current"])
        self.assertEqual(result["status"], "not_supported")

    def test_dragon_tiger_unavailable_preserves_unknown_not_false(self) -> None:
        adapter = AkshareFundamentalAdapter()
        with patch.object(adapter, "_call_df_candidates", return_value=(None, None, ["stock_lhb_stock_statistic_em:Timeout"])):
            result = adapter.get_dragon_tiger_flag("600519", as_of=date(2026, 9, 8))

        self.assertEqual(result["status"], "not_supported")
        self.assertIsNone(result["is_on_list"])
        self.assertIsNone(result["recent_count"])

    def test_dragon_tiger_prefers_dated_detail_and_limits_month_statistic_fallback(self) -> None:
        adapter = AkshareFundamentalAdapter()
        captured = []

        def _fake_call(candidates, **_kwargs):
            captured.append([(func, dict(kwargs)) for func, kwargs in candidates])
            return None, None, []

        with patch.object(adapter, "_call_df_candidates", side_effect=_fake_call):
            adapter.get_dragon_tiger_flag("600519", lookback_days=20, as_of=date(2026, 9, 8))
            adapter.get_dragon_tiger_flag("600519", lookback_days=30, as_of=date(2026, 9, 8))

        self.assertEqual(captured[0], [
            ("stock_lhb_detail_em", {"start_date": "20260819", "end_date": "20260908"}),
        ])
        self.assertEqual(captured[1][-1], ("stock_lhb_stock_statistic_em", {"symbol": "近一月"}))

    def test_dragon_tiger_matched_row_without_date_or_count_is_unknown(self) -> None:
        adapter = AkshareFundamentalAdapter()
        df = pd.DataFrame({"股票代码": ["600519"], "名称": ["贵州茅台"]})

        with patch.object(adapter, "_call_df_candidates", return_value=(df, "stock_lhb_detail_em", [])):
            result = adapter.get_dragon_tiger_flag("600519", lookback_days=20, as_of=date(2026, 9, 8))

        self.assertEqual(result["status"], "partial")
        self.assertIsNone(result["is_on_list"])
        self.assertIsNone(result["recent_count"])
        self.assertTrue(any("matched_row_without_parseable_date_or_count" in err for err in result["errors"]))

    def test_dragon_tiger_stale_match_is_not_recent_on_list(self) -> None:
        adapter = AkshareFundamentalAdapter()
        df = pd.DataFrame({"股票代码": ["600519"], "上榜日期": ["2020-03-31"]})

        with patch.object(adapter, "_call_df_candidates", return_value=(df, "stock_lhb_stock_statistic_em", [])):
            result = adapter.get_dragon_tiger_flag("600519", lookback_days=20, as_of=date(2026, 9, 8))

        self.assertEqual(result["status"], "ok")
        self.assertFalse(result["is_on_list"])
        self.assertEqual(result["recent_count"], 0)
        self.assertEqual(result["latest_date"], "2020-03-31")

    def test_build_dividend_payload_returns_empty_when_code_not_matched(self) -> None:
        now = datetime.now().strftime("%Y-%m-%d")
        df = pd.DataFrame(
            {
                "股票代码": ["000001"],
                "除息日": [now],
                "分配方案": ["10派3元(含税)"],
            }
        )

        payload = _build_dividend_payload(df, stock_code="600519")
        self.assertEqual(payload, {})

    def test_build_dividend_payload_skips_after_tax_plan(self) -> None:
        now = datetime.now().strftime("%Y-%m-%d")
        df = pd.DataFrame(
            {
                "股票代码": ["600519"],
                "除息日": [now],
                "分配方案": ["10派3元(税后)"],
            }
        )

        payload = _build_dividend_payload(df, stock_code="600519")
        self.assertEqual(payload, {})

    def test_build_dividend_payload_ttm_window_boundary(self) -> None:
        now = datetime.now()
        day_365 = (now - timedelta(days=365)).strftime("%Y-%m-%d")
        day_366 = (now - timedelta(days=366)).strftime("%Y-%m-%d")
        df = pd.DataFrame(
            {
                "股票代码": ["600519", "600519"],
                "除息日": [day_365, day_366],
                "分配方案": ["10派3元(含税)", "10派5元(含税)"],
            }
        )

        payload = _build_dividend_payload(df, stock_code="600519")
        self.assertEqual(payload.get("ttm_event_count"), 1)
        self.assertAlmostEqual(payload.get("ttm_cash_dividend_per_share"), 0.3, places=6)


if __name__ == "__main__":
    unittest.main()
