"""Concurrency and partial-result contracts across the real manager wrappers."""
from contextvars import ContextVar
from datetime import date, datetime
from threading import BoundedSemaphore, Event, Lock
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch
import time

from data_provider.base import DataFetcherManager


def config():
    return SimpleNamespace(
        enable_fundamental_pipeline=True, fundamental_cache_ttl_seconds=0,
        fundamental_stage_timeout_seconds=0.15, fundamental_fetch_timeout_seconds=0.1,
        fundamental_retry_max=1,
    )


def block():
    return {"status": "ok", "data": {"observed": True}, "source_chain": [], "errors": []}


def test_slow_valuation_does_not_starve_independent_blocks():
    manager = DataFetcherManager(fetchers=[])
    release = Event()
    observed = []

    def endpoint(code, budget_seconds):
        observed.append((code, budget_seconds))
        return block()

    try:
        with patch("src.config.get_config", return_value=config()), \
                patch.object(manager, "get_realtime_quote", side_effect=lambda _: release.wait(1)), \
                patch.object(manager._fundamental_adapter, "get_fundamental_bundle", return_value={
                    "status": "partial", "earnings": {"report_summary": "current report"},
                }), \
                patch.object(manager, "get_capital_flow_context", side_effect=endpoint), \
                patch.object(manager, "get_dragon_tiger_context", side_effect=endpoint), \
                patch.object(manager, "get_board_context", side_effect=endpoint):
            started = time.monotonic()
            result = manager.get_fundamental_context("600519")
        assert time.monotonic() - started < 0.5
        assert len(observed) == 3
        assert all(budget > 0 for _, budget in observed)
        assert result["earnings"]["data"]["report_summary"] == "current report"
        assert result["coverage"]["valuation"] != "ok"
        assert result["coverage"]["boards"] == "ok"
    finally:
        release.set()


def test_completed_bundle_data_survives_later_timeout_and_late_mutation():
    manager = DataFetcherManager(fetchers=[])
    release, finished = Event(), Event()
    callbacks = []

    def bundle(code, *, as_of, deadline, on_progress):
        payload = {"status": "partial", "earnings": {"report_summary": "verified"}, "errors": []}
        callbacks.append(on_progress)
        on_progress(payload)
        release.wait(1)
        payload["earnings"]["report_summary"] = "late mutation"
        on_progress(payload)
        finished.set()
        return payload

    try:
        with patch("src.config.get_config", return_value=config()), \
                patch.object(manager, "get_realtime_quote", return_value=None), \
                patch.object(manager._fundamental_adapter, "get_fundamental_bundle", side_effect=bundle), \
                patch.object(manager, "get_capital_flow_context", return_value=block()), \
                patch.object(manager, "get_dragon_tiger_context", return_value=block()), \
                patch.object(manager, "get_board_context", return_value=block()):
            result = manager.get_fundamental_context("600519")
        assert callbacks
        assert result["earnings"]["data"]["report_summary"] == "verified"
        assert any("timeout" in error for error in result["earnings"]["errors"])
        release.set()
        assert finished.wait(1)
        assert result["earnings"]["data"]["report_summary"] == "verified"
    finally:
        release.set()


def test_zero_budget_never_starts_provider_requests():
    manager = DataFetcherManager(fetchers=[])
    with patch("src.config.get_config", return_value=config()), \
            patch.object(manager, "get_realtime_quote") as quote, \
            patch.object(manager._fundamental_adapter, "get_fundamental_bundle") as bundle, \
            patch.object(manager, "get_capital_flow_context") as capital:
        result = manager.get_fundamental_context("600519", budget_seconds=0)
    quote.assert_not_called()
    bundle.assert_not_called()
    capital.assert_not_called()
    assert result["coverage"]["earnings"] == "failed"


def test_full_worker_pool_does_not_launch_unbounded_replacements():
    manager = DataFetcherManager(fetchers=[])
    manager._fundamental_timeout_slots = BoundedSemaphore(0)
    with patch("src.config.get_config", return_value=config()), \
            patch.object(manager, "get_realtime_quote") as quote, \
            patch.object(manager._fundamental_adapter, "get_fundamental_bundle") as bundle:
        result = manager.get_fundamental_context("600519")
    quote.assert_not_called()
    bundle.assert_not_called()
    assert result["coverage"]["earnings"] == "failed"
    assert any("worker pool exhausted" in e for e in result["errors"])


def test_timeout_worker_preserves_frozen_request_context():
    marker = ContextVar("fundamental_test_asof", default=None)
    token = marker.set("2026-09-07")
    try:
        manager = DataFetcherManager(fetchers=[])
        value, error, _ = manager._run_with_timeout(marker.get, 0.2, "context_test")
        assert error is None
        assert value == "2026-09-07"
    finally:
        marker.reset(token)


def test_unavailable_dragon_tiger_remains_unknown_not_false():
    manager = DataFetcherManager(fetchers=[])
    with patch("src.config.get_config", return_value=config()), \
            patch.object(manager._fundamental_adapter, "get_dragon_tiger_flag", return_value={
                "status": "failed", "is_on_list": None, "recent_count": None,
                "errors": ["connection_failed"],
            }):
        result = manager.get_dragon_tiger_context("600519")
    assert result["status"] == "failed"
    assert result["data"]["is_on_list"] is None
    assert result["data"]["recent_count"] is None


def test_flow_metadata_or_transport_errors_do_not_count_as_observed_money():
    manager = DataFetcherManager(fetchers=[])
    with patch("src.config.get_config", return_value=config()), \
            patch.object(manager._fundamental_adapter, "get_capital_flow", return_value={
                "status": "not_supported", "stock_flow": {"source": "unavailable", "date": "2026-09-08"},
                "errors": ["ConnectionError"],
            }):
        result = manager.get_capital_flow_context("600519")
    assert result["status"] == "failed"


def test_failed_empty_bundle_is_not_reported_as_unsupported():
    manager = DataFetcherManager(fetchers=[])
    with patch("src.config.get_config", return_value=config()), \
            patch.object(manager, "get_realtime_quote", return_value=None), \
            patch.object(manager._fundamental_adapter, "get_fundamental_bundle", return_value={
                "status": "not_supported", "growth": {}, "earnings": {},
                "errors": ["stock_yjbb_em:ConnectionError"],
            }), \
            patch.object(manager, "get_capital_flow_context", return_value=block()), \
            patch.object(manager, "get_dragon_tiger_context", return_value=block()), \
            patch.object(manager, "get_board_context", return_value=block()):
        result = manager.get_fundamental_context("600519")
    assert result["coverage"]["earnings"] == "failed"


def test_frozen_asof_reaches_parallel_adapter_consumers():
    from src.services.history_loader import set_frozen_target_date, reset_frozen_target_date
    manager = DataFetcherManager(fetchers=[])
    asof = date(2026, 9, 7)
    token = set_frozen_target_date(asof)
    try:
        with patch("src.config.get_config", return_value=config()), \
                patch.object(manager, "get_realtime_quote", return_value=None), \
                patch.object(manager._fundamental_adapter, "get_fundamental_bundle", return_value={}) as bundle, \
                patch.object(manager._fundamental_adapter, "get_capital_flow", return_value={}) as flow, \
                patch.object(manager._fundamental_adapter, "get_dragon_tiger_flag", return_value={}) as dragon, \
                patch.object(manager, "get_board_context", return_value=block()):
            manager.get_fundamental_context("600519")
        assert bundle.call_args.kwargs["as_of"] == asof
        assert flow.call_args.kwargs["as_of"] == asof
        assert dragon.call_args.kwargs["as_of"] == asof
    finally:
        reset_frozen_target_date(token)


def test_concurrent_timeout_workers_are_bounded_and_recover():
    """Real timeout threads stay capped during failure, then return every slot."""
    manager = DataFetcherManager(fetchers=[])
    cap = manager._fundamental_timeout_worker_limit
    release, saturated = Event(), Event()
    lock = Lock()
    counters = {"active": 0, "peak": 0, "started": 0}

    def stalled_provider():
        with lock:
            counters["active"] += 1
            counters["started"] += 1
            counters["peak"] = max(counters["peak"], counters["active"])
            if counters["active"] == cap:
                saturated.set()
        try:
            release.wait(3)
            return "recovered"
        finally:
            with lock:
                counters["active"] -= 1

    try:
        with ThreadPoolExecutor(max_workers=cap) as callers:
            futures = [callers.submit(manager._run_with_timeout, stalled_provider, .3, "outage") for _ in range(cap)]
            assert saturated.wait(2)
            results = [f.result(timeout=2) for f in futures]
        assert all(result[0] is None and "timeout" in result[1] for result in results)
        # Repeated calls while upstream remains stuck cannot launch replacements.
        for _ in range(cap * 3):
            result = manager._run_with_timeout(stalled_provider, .01, "saturated")
            assert "pool exhausted" in result[1]
        assert counters["started"] == counters["peak"] == cap
    finally:
        release.set()
    # Acquire all permits: this waits for each actual worker's finally release.
    acquired = 0
    try:
        for _ in range(cap):
            assert manager._fundamental_timeout_slots.acquire(timeout=2)
            acquired += 1
        assert counters["active"] == 0
    finally:
        for _ in range(acquired):
            manager._fundamental_timeout_slots.release()
    assert manager._run_with_timeout(lambda: "next-stock-ok", .2, "next-stock")[:2] == ("next-stock-ok", None)


def test_live_cache_refreshes_on_session_close_and_local_date_change():
    manager = DataFetcherManager(fetchers=[])
    cfg = config()
    cfg.fundamental_cache_ttl_seconds = 120
    quote = SimpleNamespace(pe_ratio=None, pb_ratio=None, total_mv=100, circ_mv=100)
    # Use the real exchange calendar and cache; only external provider IO/time are controlled.
    from src.core.trading_calendar import get_market_now
    with patch("src.config.get_config", return_value=cfg), \
            patch("src.core.trading_calendar.get_market_now") as clock, \
            patch.object(manager, "get_realtime_quote", return_value=quote) as fetch, \
            patch.object(manager._fundamental_adapter, "get_fundamental_bundle", return_value={"status": "not_supported"}):
        for timestamp, expected_calls in [
            ("2026-09-08T14:59:30", 1),
            ("2026-09-08T14:59:50", 1),
            ("2026-09-08T15:00:01", 2),
            ("2026-09-08T23:59:59", 2),
            ("2026-09-09T00:00:01", 3),
        ]:
            clock.return_value = get_market_now("cn", datetime.fromisoformat(timestamp))
            manager.get_fundamental_context("159915")
            assert fetch.call_count == expected_calls, timestamp


def test_frozen_cache_identity_does_not_follow_wall_clock():
    from src.services.history_loader import set_frozen_target_date, reset_frozen_target_date
    manager = DataFetcherManager(fetchers=[])
    token = set_frozen_target_date(date(2026, 9, 7))
    try:
        with patch("src.core.trading_calendar.get_market_now") as clock:
            first = manager._get_fundamental_cache_key("600519", 1)
            clock.return_value = datetime(2026, 9, 9)
            assert manager._get_fundamental_cache_key("600519", 1) == first
            clock.assert_not_called()
    finally:
        reset_frozen_target_date(token)
