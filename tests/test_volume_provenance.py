"""Regression coverage for September 7 report 268 and mixed-source cache."""
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from data_provider.akshare_fetcher import AkshareFetcher
from data_provider.efinance_fetcher import EfinanceFetcher
from data_provider.volume_units import cached_volume_shares, lots_to_shares
from src.core.pipeline import StockAnalysisPipeline
from src.stock_analyzer import StockTrendAnalyzer, TrendAnalysisResult, VolumeStatus
from src.storage import DatabaseManager, StockDaily


def pipeline():
    result = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    result.config = SimpleNamespace(report_language='zh', enable_realtime_technical_indicators=True)
    result.search_service = None
    return result


@pytest.mark.parametrize('fetcher', [EfinanceFetcher, AkshareFetcher])
def test_mainland_daily_normalizes_lots_to_shares(fetcher):
    raw = pd.DataFrame([{'日期': '2026-09-04', '收盘':43.97, '成交量':767895, '成交额':3351156402.46}])
    normalized = fetcher.__new__(fetcher)._normalize_data(raw, '002714')
    assert normalized.iloc[0]['volume'] == 76789500
    assert normalized.attrs['volume_unit'] == 'shares'
    assert raw.iloc[0]['成交量'] == 767895


def test_sina_and_foreign_volume_not_scaled():
    raw = pd.DataFrame([{'收盘':43.97, '成交量':76789500}])
    raw.attrs['volume_unit'] = 'shares'
    fetcher = AkshareFetcher.__new__(AkshareFetcher)
    assert fetcher._normalize_data(raw, '002714').iloc[0]['volume'] == 76789500
    raw.attrs.clear()
    assert fetcher._normalize_data(raw, 'hk00700').iloc[0]['volume'] == 76789500


def test_legacy_reads_are_non_destructive_and_new_writes_not_double_converted():
    row = StockDaily(code='002714', date=date(2026,9,4),volume=767895,data_source='EfinanceFetcher')
    assert row.to_dict()['volume'] == row.to_dict()['volume'] == 76789500
    assert row.volume == 767895
    assert cached_volume_shares(76789500,'EfinanceFetcher|vol=shares') == 76789500
    assert cached_volume_shares(767895,'AkshareFetcher') is None
    assert cached_volume_shares(767895,None) is None
    assert cached_volume_shares(767895,'unknown_source') is None
    assert StockDaily(volume=123).to_dict()['volume_unit'] == 'unknown'
    assert cached_volume_shares(76789500,'TencentFetcher') == 76789500
    assert lots_to_shares(float('nan')) is None


def test_provider_storage_context_roundtrip_real_report_ratio(tmp_path):
    DatabaseManager.reset_instance()
    db = DatabaseManager(db_url=f'sqlite:///{tmp_path}/roundtrip.db')
    try:
        raw = pd.DataFrame([
            {'日期':'2026-09-04','收盘':43.97,'成交量':767895},
            {'日期':'2026-09-07','收盘':43.91,'成交量':450249},
        ])
        frame = EfinanceFetcher.__new__(EfinanceFetcher)._normalize_data(raw,'002714')
        db.save_daily_data(frame,'002714','EfinanceFetcher')
        context = db.get_analysis_context('002714')
        assert context['volume_change_ratio'] == 0.59
        assert context['today']['volume'] == 45024900
        assert context['today']['data_source'].endswith('|vol=shares')
        db.save_daily_data(frame,'002714','EfinanceFetcher')
        assert db.get_analysis_context('002714')['volume_change_ratio'] == 0.59
    finally:
        DatabaseManager.reset_instance()


def test_postmarket_and_weekend_do_not_relabel_or_override_cached_bar():
    context={'code':'002714','date':'2026-09-07','today':{'date':'2026-09-07','close':43.91},
             'yesterday':{'date':'2026-09-04','close':43.97}}
    quote=SimpleNamespace(price=99, volume=1, source='tencent')
    trend=TrendAnalysisResult(code='002714',ma5=43)
    for phase in ('postmarket','non_trading'):
        out=pipeline()._enhance_context(context,quote,None,trend,'牧原股份',
            fundamental_context={},market_phase_context={'phase':phase})
        assert out['today']==context['today']
        assert out['date']=='2026-09-07'


def test_intraday_compares_previous_latest_bar_not_second_last():
    context={'code':'002714','date':'2026-09-04',
             'today':{'date':'2026-09-04','close':43.97,'volume':76789500},
             'yesterday':{'date':'2026-09-03','close':42.08,'volume':26787100}}
    quote=SimpleNamespace(price=43.91,volume=45024900,source='tencent')
    trend=TrendAnalysisResult(code='002714',ma5=43)
    with patch('src.core.pipeline.get_market_now',return_value=datetime(2026,9,7,10)):
        out=pipeline()._enhance_context(context,quote,None,trend,'牧原股份',
            fundamental_context={},market_phase_context={'phase':'intraday'})
    assert out['yesterday']['date']=='2026-09-04'
    assert out['volume_change_ratio']==0.59
    assert out['daily_comparison_status']=='ok'


def test_missing_previous_session_suppresses_false_day_comparison():
    context={'code':'518880.SH','today':{'date':'2026-09-07'},
             'yesterday':{'date':'2026-09-03'},'volume_change_ratio':0.01,'price_change_ratio':-1}
    pipeline()._validate_daily_comparison(context)
    assert context['daily_comparison_status']=='unavailable'
    assert 'volume_change_ratio' not in context
    assert 'price_change_ratio' not in context


def test_exchange_suffix_does_not_use_us_labor_day_calendar_for_chinese_etf():
    context={'code':'518880.SH','today':{'date':'2026-09-07'},
             'yesterday':{'date':'2026-09-04'},'volume_change_ratio':1.09}
    pipeline()._validate_daily_comparison(context)
    assert context['daily_comparison_status']=='ok'
    assert context['volume_change_ratio']==1.09


def test_unknown_cached_volume_does_not_become_shrinking_volume_signal():
    frame=pd.DataFrame({'close':[10]*6,'volume':[None]*5+[100]})
    trend=TrendAnalysisResult(code='002714')
    StockTrendAnalyzer()._analyze_volume(frame,trend)
    assert trend.volume_status==VolumeStatus.NORMAL
    assert '不可用' in trend.volume_trend


def test_stale_daily_history_is_not_valid_style_evidence():
    frame=pd.DataFrame({'date':['2026-09-03']*180,'close':[10.0]*180})
    style={'trading_style':'position','settings':{'analysis_window_sessions':180,'core_ma_windows':[10,20,60]}}
    result=pipeline()._style_indicator_snapshot(frame,style,expected_bar_date='2026-09-07')
    assert not result['history_sufficient']


def test_latest_invalid_close_or_duplicate_dates_cannot_prove_style_history():
    style={'trading_style':'position','settings':{'analysis_window_sessions':2,'core_ma_windows':[2]}}
    frame=pd.DataFrame({'date':['2026-09-03','2026-09-04','2026-09-07'],
                        'close':[10.0,11.0,float('nan')]})
    result=pipeline()._style_indicator_snapshot(frame,style,expected_bar_date='2026-09-07')
    assert result['bars_observed']==2
    assert not result['history_sufficient']
    frame=pd.DataFrame({'date':['2026-09-07']*3,'close':[10.0]*3})
    assert not pipeline()._style_indicator_snapshot(frame,style)['history_sufficient']


def test_stale_intraday_quote_cannot_replace_daily_evidence():
    context={'code':'002714','date':'2026-09-07',
             'today':{'date':'2026-09-07','close':43.91},
             'yesterday':{'date':'2026-09-04','close':43.97}}
    quote=SimpleNamespace(price=99,volume=1,source='tencent',is_stale=True)
    out=pipeline()._enhance_context(context,quote,None,TrendAnalysisResult(code='002714',ma5=43),
        '牧原股份',fundamental_context={},market_phase_context={'phase':'intraday'})
    assert out['today']==context['today']


def test_provider_timestamp_dates_remain_sortable_after_intraday_append():
    frame=pd.DataFrame({'date':pd.to_datetime(['2026-09-04','2026-09-07']),
                        'close':[43.97,43.91],'volume':[76789500,45024900]})
    quote=SimpleNamespace(price=44.0,volume=None)
    with patch('src.core.pipeline.get_market_now',return_value=datetime(2026,9,8,11)):
        result=pipeline()._augment_historical_with_realtime(frame,quote,'002714',
            market_phase_context={'phase':'intraday'})
    assert result.sort_values('date').iloc[-1]['date']==date(2026,9,8)
    assert pd.isna(result.iloc[-1]['volume'])
    assert len(frame)==2 and str(frame['date'].dtype).startswith('datetime64')
