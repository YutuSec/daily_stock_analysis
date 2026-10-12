# -*- coding: utf-8 -*-
"""Tests for complete daily board observation snapshots."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from src.config import Config
from src.repositories.board_observation_repository import BoardObservationRepository
from src.services.board_observation_service import BoardObservationService
from src.storage import BoardObservation, DatabaseManager


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    old_database_url = os.environ.get("DATABASE_URL")
    db_path = tmp_path / "board-observation.db"
    os.environ["DATABASE_PATH"] = str(db_path)
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager.get_instance()
    try:
        yield db
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        if old_database_path is None:
            os.environ.pop("DATABASE_PATH", None)
        else:
            os.environ["DATABASE_PATH"] = old_database_path
        if old_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = old_database_url


class _FakeFetcherManager:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def get_full_board_rankings(self, *, market="cn", board_type="industry", source="csrc_tencent"):
        self.calls.append({"market": market, "board_type": board_type, "source": source})
        if self.payloads:
            payload = self.payloads.pop(0)
            if isinstance(payload, BaseException):
                raise payload
            return payload
        raise AssertionError("unexpected fetch")


def _payload(trade_date: date, themes=None):
    return {
        "status": "healthy",
        "market": "cn",
        "source": "csrc_tencent",
        "taxonomy": "csrc_industry",
        "board_type": "industry",
        "as_of": trade_date,
        "fetched_at": datetime.combine(trade_date, datetime.min.time()).replace(hour=15, minute=5),
        "coverage": {"status": "healthy", "theme_count": len(themes) if themes is not None else 1},
        "themes": themes or [
            {
                "theme_key": "industry:半导体",
                "theme_type": "industry",
                "theme_name": "半导体",
                "theme_code": "csrc_C39",
                "theme_source": "csrc_tencent",
                "theme_taxonomy": "csrc_industry",
                "rank": 1,
                "change_pct": 2.0,
                "member_count": 8,
                "as_of": trade_date.isoformat(),
            },
        ],
        "snapshot_hash": f"hash-{trade_date.isoformat()}",
    }


def _patch_calendar(monkeypatch, *, now: datetime):
    monkeypatch.setattr(
        "src.services.board_observation_service.get_market_now",
        lambda market: now,
    )
    monkeypatch.setattr(
        "src.services.board_observation_service.is_market_open",
        lambda market, check_date, strict=False: check_date.weekday() < 5,
    )


@pytest.mark.parametrize('invalid,expected', [
    ({}, None),
    ({'status': 'failed'}, 'board_observation_unavailable'),
    ({'source': 'other'}, 'snapshot_source_mismatch'),
    ({'taxonomy': 'concept'}, 'snapshot_source_mismatch'),
    ({'snapshot_hash': ''}, 'snapshot_hash_missing'),
    ({'coverage': {}}, 'snapshot_coverage_unverified'),
    ({'coverage': {'status': 'healthy', 'theme_count': 2}}, 'snapshot_coverage_incomplete'),
    ({'fetched_at': datetime(2026, 9, 15, 6, 59)}, 'snapshot_not_collected_after_close'),
    ({'fetched_at': datetime(2026, 9, 16, 7, 10)}, 'snapshot_not_collected_after_close'),
    ({'themes': []}, 'snapshot_themes_empty'),
    ({'trade_date': date(2026, 9, 17)}, 'future_trade_date'),
    ({'trade_date': date(2026, 9, 13)}, 'non_trading_date'),
])
def test_snapshot_credit_requires_authentic_identity_and_postclose_date(isolated_db, monkeypatch, invalid, expected):
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 16, 10, tzinfo=ZoneInfo('Asia/Shanghai')))
    payload = _payload(date(2026, 9, 15))
    row = {**payload, 'id': 1, 'trade_date': date(2026, 9, 15), 'fetched_at': datetime(2026, 9, 15, 7, 10)}
    row.update(invalid)
    service = BoardObservationService(db_manager=isolated_db, fetcher_manager=_FakeFetcherManager([]))
    health = service.snapshot_health(row, market='cn')
    assert health['reason'] == expected
    assert health['healthy'] is (expected is None)


def test_snapshot_credit_rejects_preclose_and_untrusted_calendar(isolated_db, monkeypatch):
    day = date(2026, 9, 15)
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 15, 14, 59, tzinfo=ZoneInfo('Asia/Shanghai')))
    row = {**_payload(day), 'id': 1, 'trade_date': day, 'fetched_at': datetime(2026, 9, 15, 7, 10)}
    service = BoardObservationService(db_manager=isolated_db, fetcher_manager=_FakeFetcherManager([]))
    assert service.snapshot_health(row, market='cn')['reason'] == 'market_not_closed'
    def fail_calendar(*args, **kwargs):
        raise RuntimeError('calendar unavailable')
    monkeypatch.setattr('src.services.board_observation_service.is_market_open', fail_calendar)
    assert service.snapshot_health(row, market='cn')['reason'] == 'calendar_unavailable'


def test_collect_persists_complete_snapshot_after_close_and_returns_ok(isolated_db, monkeypatch):
    trade_date = date(2026, 9, 14)
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    manager = _FakeFetcherManager([_payload(trade_date)])
    service = BoardObservationService(
        config=SimpleNamespace(market_opportunity_board_source="csrc_tencent"),
        db_manager=isolated_db,
        fetcher_manager=manager,
    )

    result = service.collect("cn", trade_date)

    assert result["status"] == "ok"
    assert result["source"] == "csrc_tencent"
    assert result["themes"][0]["theme_key"] == "industry:半导体"
    assert result["themes"][0]["theme_code"] == "csrc_C39"
    assert manager.calls == [{"market": "cn", "board_type": "industry", "source": "csrc_tencent"}]
    with isolated_db.session_scope() as session:
        row = session.query(BoardObservation).one()
        assert row.status == "healthy"
        assert row.trade_date == trade_date
        assert row.source == "csrc_tencent"
        assert row.taxonomy == "csrc_industry"


def test_collect_reuses_existing_snapshot_before_close_without_refetch(isolated_db, monkeypatch):
    trade_date = date(2026, 9, 14)
    repo = BoardObservationRepository(isolated_db)
    repo.upsert_snapshot(
        {
            "snapshot_key": "cn:2026-09-14:csrc_tencent:csrc_industry",
            "market": "cn",
            "trade_date": trade_date,
            "source": "csrc_tencent",
            "taxonomy": "csrc_industry",
            "status": "healthy",
            "coverage": {"status": "healthy"},
            "themes": _payload(trade_date)["themes"],
            "snapshot_hash": "frozen",
            "fetched_at": datetime(2026, 9, 14, 15, 5),
        }
    )
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 11, 0, tzinfo=ZoneInfo("Asia/Shanghai")))
    manager = _FakeFetcherManager([])
    service = BoardObservationService(db_manager=isolated_db, fetcher_manager=manager)

    result = service.collect("cn", trade_date)

    assert result["status"] == "ok"
    assert result["snapshot_id"] is not None
    assert manager.calls == []


@pytest.mark.parametrize(
    "trade_date,now,reason",
    [
        (date(2026, 9, 14), datetime(2026, 9, 14, 11, 0, tzinfo=ZoneInfo("Asia/Shanghai")), "market_not_closed"),
        (date(2026, 9, 15), datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")), "future_trade_date"),
        (date(2026, 9, 11), datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")), "historical_collection_not_supported"),
    ],
)
def test_collect_rejects_unclosed_future_and_historical_fetches(isolated_db, monkeypatch, trade_date, now, reason):
    _patch_calendar(monkeypatch, now=now)
    manager = _FakeFetcherManager([_payload(trade_date)])
    service = BoardObservationService(db_manager=isolated_db, fetcher_manager=manager)

    result = service.collect("cn", trade_date)

    assert result["status"] == "failed"
    assert result["reason"] == reason
    assert manager.calls == []


def test_collect_rejects_snapshot_date_mismatch_without_themes(isolated_db, monkeypatch):
    trade_date = date(2026, 9, 14)
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    manager = _FakeFetcherManager([_payload(date(2026, 9, 11))])
    service = BoardObservationService(db_manager=isolated_db, fetcher_manager=manager)

    result = service.collect("cn", trade_date)

    assert result["status"] == "failed"
    assert result["reason"] == "snapshot_trade_date_mismatch"
    with isolated_db.session_scope() as session:
        row = session.query(BoardObservation).one()
        assert row.status == "failed"
        assert row.themes_json == "[]"


@pytest.mark.parametrize(
    "mutator,reason",
    [
        (lambda payload: payload.update(source="eastmoney"), "snapshot_source_mismatch"),
        (lambda payload: payload.update(taxonomy="eastmoney_industry"), "snapshot_taxonomy_mismatch"),
        (lambda payload: payload.pop("as_of"), "snapshot_trade_date_missing"),
        (lambda payload: payload["themes"][0].update(theme_source="eastmoney"), "theme_source_mismatch"),
        (lambda payload: payload["themes"][0].update(theme_taxonomy="eastmoney_industry"), "theme_taxonomy_mismatch"),
        (lambda payload: payload["themes"][0].pop("theme_code"), "theme_code_missing"),
        (lambda payload: payload["themes"][0].pop("as_of"), "theme_as_of_missing"),
        (lambda payload: payload["themes"][0].update(as_of="2026-09-11"), "theme_as_of_mismatch"),
    ],
)
def test_collect_rejects_source_taxonomy_and_row_contract_mismatches(isolated_db, monkeypatch, mutator, reason):
    trade_date = date(2026, 9, 14)
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    payload = _payload(trade_date)
    mutator(payload)
    service = BoardObservationService(db_manager=isolated_db, fetcher_manager=_FakeFetcherManager([payload]))

    result = service.collect("cn", trade_date)

    assert result["status"] == "failed"
    assert result["reason"] == reason
    with isolated_db.session_scope() as session:
        row = session.query(BoardObservation).one()
        assert row.status == "failed"
        assert row.source == "csrc_tencent"
        assert row.taxonomy == "csrc_industry"
        assert row.themes_json == "[]"


def test_collect_persists_fetch_exception_as_failed_snapshot(isolated_db, monkeypatch):
    trade_date = date(2026, 9, 14)
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    service = BoardObservationService(
        db_manager=isolated_db,
        fetcher_manager=_FakeFetcherManager([RuntimeError("provider down")]),
    )

    result = service.collect("cn", trade_date)

    assert result["status"] == "failed"
    assert result["reason"] == "full_board_rankings_error:RuntimeError"
    with isolated_db.session_scope() as session:
        row = session.query(BoardObservation).one()
        assert row.status == "failed"
        assert row.reason == "full_board_rankings_error:RuntimeError"


def test_healthy_snapshot_is_frozen_against_later_failed_or_changed_upserts(isolated_db):
    repo = BoardObservationRepository(isolated_db)
    trade_date = date(2026, 9, 14)
    key = "cn:2026-09-14:csrc_tencent:csrc_industry"
    original = repo.upsert_snapshot(
        {
            "snapshot_key": key,
            "market": "cn",
            "trade_date": trade_date,
            "source": "csrc_tencent",
            "taxonomy": "csrc_industry",
            "status": "healthy",
            "coverage": {"status": "healthy"},
            "themes": _payload(trade_date)["themes"],
            "snapshot_hash": "original",
            "fetched_at": datetime(2026, 9, 14, 15, 5),
        }
    )

    failed = repo.upsert_snapshot(
        {
            "snapshot_key": key,
            "market": "cn",
            "trade_date": trade_date,
            "source": "csrc_tencent",
            "taxonomy": "csrc_industry",
            "status": "failed",
            "reason": "late_failure",
            "coverage": {"status": "failed"},
            "themes": [],
            "snapshot_hash": "failed",
            "fetched_at": datetime(2026, 9, 14, 15, 6),
        }
    )
    changed = repo.upsert_snapshot(
        {
            "snapshot_key": key,
            "market": "cn",
            "trade_date": trade_date,
            "source": "csrc_tencent",
            "taxonomy": "csrc_industry",
            "status": "healthy",
            "coverage": {"status": "healthy", "changed": True},
            "themes": [],
            "snapshot_hash": "changed",
            "fetched_at": datetime(2026, 9, 14, 15, 7),
        }
    )

    assert original["snapshot_hash"] == "original"
    assert failed["status"] == "healthy"
    assert failed["snapshot_hash"] == "original"
    assert changed["snapshot_hash"] == "original"


def test_configured_board_source_is_used_consistently_without_default_fallback(isolated_db, monkeypatch):
    trade_date = date(2026, 9, 14)
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    manager = _FakeFetcherManager([
        {
            **_payload(trade_date),
            "source": "manual_provider",
            "themes": [
                {
                    **_payload(trade_date)["themes"][0],
                    "theme_source": "manual_provider",
                }
            ],
        }
    ])
    service = BoardObservationService(
        config=SimpleNamespace(market_opportunity_board_source="manual_provider"),
        db_manager=isolated_db,
        fetcher_manager=manager,
    )

    result = service.collect("cn", trade_date)
    themes = service.discovery_themes("cn", trade_date, limit=15)

    assert result["status"] == "ok"
    assert manager.calls == [{"market": "cn", "board_type": "industry", "source": "manual_provider"}]
    assert themes[0]["theme_source"] == "manual_provider"


def test_discovery_and_persistence_report_source_windows_and_missing_dates(isolated_db, monkeypatch):
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    repo = BoardObservationRepository(isolated_db)
    for idx, session_day in enumerate(
        [date(2026, 9, 8), date(2026, 9, 9), date(2026, 9, 10), date(2026, 9, 14)]
    ):
        repo.upsert_snapshot(
            {
                "snapshot_key": f"cn:{session_day.isoformat()}:csrc_tencent:csrc_industry",
                "market": "cn",
                "trade_date": session_day,
                "source": "csrc_tencent",
                "taxonomy": "csrc_industry",
                "status": "healthy",
                "coverage": {"status": "healthy"},
                "themes": _payload(
                    session_day,
                    themes=[
                        {
                            "theme_key": "industry:半导体",
                            "theme_type": "industry",
                            "theme_name": "半导体",
                            "theme_code": "csrc_C39",
                            "theme_source": "csrc_tencent",
                            "theme_taxonomy": "csrc_industry",
                            "rank": 1,
                            "change_pct": 1.0 + idx,
                            "member_count": 8,
                        }
                    ],
                )["themes"],
                "snapshot_hash": f"hash-{session_day}",
                "fetched_at": datetime.combine(session_day, datetime.min.time()).replace(hour=15, minute=5),
            }
        )
    service = BoardObservationService(db_manager=isolated_db, fetcher_manager=_FakeFetcherManager([]))

    evidence = service.persistence_for_theme("cn", date(2026, 9, 14), "industry:半导体", "csrc_tencent")
    themes = service.discovery_themes("cn", date(2026, 9, 14), limit=15)

    assert evidence["source"] == "csrc_tencent"
    assert evidence["taxonomy"] == "csrc_industry"
    assert evidence["required_window_size"] == 5
    assert evidence["available_window_size"] == 4
    assert evidence["missing_dates"] == ["2026-09-11"]
    assert evidence["days"] == 4
    assert evidence["return_5d"] is None
    assert evidence["history_state"] == "cold_start"
    assert themes[0]["theme_key"] == "industry:半导体"
    assert themes[0]["theme_code"] == "csrc_C39"
    assert themes[0]["theme_source"] == "csrc_tencent"


def test_persistence_rejects_cross_source_provider_without_csrc_history(isolated_db, monkeypatch):
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    service = BoardObservationService(db_manager=isolated_db, fetcher_manager=_FakeFetcherManager([]))

    evidence = service.persistence_for_theme("cn", date(2026, 9, 14), "concept:机器人", "eastmoney_concept")

    assert evidence["history_state"] == "source_mismatch"
    assert evidence["window_dates"] == []
    assert evidence["return_5d"] is None


def test_persistence_zero_and_down_days_do_not_count_positive_and_returns_have_no_lookahead(isolated_db, monkeypatch):
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    repo = BoardObservationRepository(isolated_db)
    sessions = [
        date(2026, 8, 18), date(2026, 8, 19), date(2026, 8, 20), date(2026, 8, 21), date(2026, 8, 24),
        date(2026, 8, 25), date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28), date(2026, 8, 31),
        date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3), date(2026, 9, 4), date(2026, 9, 7),
        date(2026, 9, 8), date(2026, 9, 9), date(2026, 9, 10), date(2026, 9, 11), date(2026, 9, 14),
        date(2026, 9, 15),
    ]
    changes = [-1.0] * 15 + [0.0, -2.0, 1.0, -1.0, 3.0, 99.0]
    for session_day, change_pct in zip(sessions, changes):
        repo.upsert_snapshot(
            {
                "snapshot_key": f"cn:{session_day.isoformat()}:csrc_tencent:csrc_industry",
                "market": "cn",
                "trade_date": session_day,
                "source": "csrc_tencent",
                "taxonomy": "csrc_industry",
                "status": "healthy",
                "coverage": {"status": "healthy"},
                "themes": _payload(
                    session_day,
                    themes=[
                        {
                            "theme_key": "industry:半导体",
                            "theme_type": "industry",
                            "theme_name": "半导体",
                            "theme_code": "csrc_C39",
                            "theme_source": "csrc_tencent",
                            "theme_taxonomy": "csrc_industry",
                            "rank": 1,
                            "change_pct": change_pct,
                            "member_count": 8,
                            "as_of": session_day.isoformat(),
                        }
                    ],
                )["themes"],
                "snapshot_hash": f"hash-{session_day}",
                "fetched_at": datetime.combine(session_day, datetime.min.time()).replace(hour=15, minute=5),
            }
        )
    service = BoardObservationService(db_manager=isolated_db, fetcher_manager=_FakeFetcherManager([]))

    evidence = service.persistence_for_theme("cn", date(2026, 9, 14), "industry:半导体", "csrc_tencent")

    assert evidence["window_dates"] == ["2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14"]
    assert evidence["positive_dates"] == ["2026-09-10", "2026-09-14"]
    assert evidence["days"] == 2
    assert evidence["history_state"] == "established_negative"
    assert evidence["return_5d"] == pytest.approx(0.9299)
    assert evidence["return_20d"] == pytest.approx(-13.1944)
    assert evidence["return_20d_evidence"]["status"] == "complete_negative"
    assert evidence["return_20d_evidence"]["available"] == 20


def test_persistence_does_not_mix_same_key_when_theme_code_changes(isolated_db, monkeypatch):
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo("Asia/Shanghai")))
    repo = BoardObservationRepository(isolated_db)
    sessions = [
        date(2026, 9, 8),
        date(2026, 9, 9),
        date(2026, 9, 10),
        date(2026, 9, 11),
        date(2026, 9, 14),
    ]
    for session_day in sessions:
        theme_code = "csrc_OLD" if session_day == date(2026, 9, 9) else "csrc_C39"
        repo.upsert_snapshot(
            {
                "snapshot_key": f"cn:{session_day.isoformat()}:csrc_tencent:csrc_industry",
                "market": "cn",
                "trade_date": session_day,
                "source": "csrc_tencent",
                "taxonomy": "csrc_industry",
                "status": "healthy",
                "coverage": {"status": "healthy"},
                "themes": _payload(
                    session_day,
                    themes=[
                        {
                            "theme_key": "industry:半导体",
                            "theme_type": "industry",
                            "theme_name": "半导体",
                            "theme_code": theme_code,
                            "theme_source": "csrc_tencent",
                            "theme_taxonomy": "csrc_industry",
                            "rank": 1,
                            "change_pct": 1.0,
                            "member_count": 8,
                            "as_of": session_day.isoformat(),
                        }
                    ],
                )["themes"],
                "snapshot_hash": f"hash-{session_day}",
                "fetched_at": datetime.combine(session_day, datetime.min.time()).replace(hour=15, minute=5),
            }
        )
    service = BoardObservationService(db_manager=isolated_db, fetcher_manager=_FakeFetcherManager([]))

    evidence = service.persistence_for_theme("cn", date(2026, 9, 14), "industry:半导体", "csrc_tencent")

    assert evidence["required_window_size"] == 5
    assert evidence["available_window_size"] == 4
    assert evidence["missing_dates"] == ["2026-09-09"]
    assert evidence["return_5d"] is None
    assert evidence["history_state"] == "cold_start"


def test_twenty_day_evidence_changes_real_discovery_membership_without_future_data(isolated_db, monkeypatch):
    as_of = date(2026, 9, 14)
    _patch_calendar(monkeypatch, now=datetime(2026, 9, 14, 15, 5, tzinfo=ZoneInfo('Asia/Shanghai')))
    days = []
    day = as_of
    while len(days) < 20:
        if day.weekday() < 5:
            days.append(day)
        day -= timedelta(days=1)
    days.reverse()
    repo = BoardObservationRepository(isolated_db)
    for index, day in enumerate(days + [as_of + timedelta(days=1)]):
        themes = []
        for rank, (name, code, direction) in enumerate([
            ('短期反弹', 'csrc_C27', -1), ('中线延续', 'csrc_C39', 1),
        ], 1):
            daily_return = direction * 1.5 if index < 15 else 1.0
            if index == 20:
                daily_return = -direction * 50.0
            themes.append({**_payload(day)['themes'][0], 'theme_key': f'industry:{name}',
                           'theme_name': name, 'theme_code': code, 'rank': rank,
                           'change_pct': daily_return})
        repo.upsert_snapshot({'snapshot_key': f'20d:{day}', 'market': 'cn', 'trade_date': day,
                              'source': 'csrc_tencent', 'taxonomy': 'csrc_industry',
                              'status': 'healthy', 'coverage': {'status': 'healthy'},
                              'themes': themes, 'snapshot_hash': f'hash:{day}'})
    service = BoardObservationService(db_manager=isolated_db, fetcher_manager=_FakeFetcherManager([]))
    rows = service.discovery_themes('cn', as_of, limit=1)
    assert [row['theme_name'] for row in rows] == ['中线延续']
    assert rows[0]['rank'] == 2  # Original daily rank is retained for audit.
    evidence = rows[0]['observation_evidence']
    assert evidence['return_20d_evidence']['window_dates'] == [d.isoformat() for d in days]
    assert evidence['return_20d_evidence']['status'] == 'complete_positive'
    assert rows[0]['observation_score_components']['trend_20d'] > 0
    with isolated_db.session_scope() as session:
        session.query(BoardObservation).filter(BoardObservation.trade_date == days[0]).delete()
    cold = service.persistence_for_theme('cn', as_of, 'industry:中线延续', 'csrc_tencent')
    assert cold['return_20d'] is None
    assert cold['return_20d_evidence']['status'] == 'missing_history'
    assert cold['return_20d_evidence']['missing_dates'] == [days[0].isoformat()]
    assert service.discovery_score_components(rows[0], cold)['trend_20d'] is None


@pytest.mark.parametrize('invalid', [None, float('nan'), float('inf'), -float('inf')])
def test_missing_twenty_day_evidence_is_not_a_negative_or_infinite_signal(invalid):
    theme = {'rank': 2, 'change_pct': 1.0}
    base = {'days': 3, 'return_5d': 2.0}
    missing = BoardObservationService.discovery_score_components(theme, {**base, 'return_20d': invalid})
    negative = BoardObservationService.discovery_score_components(theme, {**base, 'return_20d': -8.0})
    assert missing['trend_20d'] is None and 20 in missing['missing_windows']
    assert negative['trend_20d'] == -8 and 20 not in negative['missing_windows']
    assert BoardObservationService._discovery_score(theme, {**base, 'return_20d': invalid}) > BoardObservationService._discovery_score(theme, {**base, 'return_20d': -8.0})
