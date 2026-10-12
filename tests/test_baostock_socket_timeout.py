# -*- coding: utf-8 -*-
"""Baostock sockets must time out instead of hanging the caller forever."""

import socket
import threading
import time
from types import SimpleNamespace

import pytest

pytest.importorskip("baostock")

import baostock.common.context as bs_context  # noqa: E402
import baostock.common.contants as bs_constants  # noqa: E402

from data_provider import baostock_fetcher  # noqa: E402
from data_provider.base import DataFetchError  # noqa: E402


@pytest.fixture
def silent_server():
    """A local TCP server that accepts connections and never answers."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(4)
    accepted = []
    stop = threading.Event()

    def accept_loop():
        server.settimeout(0.1)
        while not stop.is_set():
            try:
                conn, _ = server.accept()
                accepted.append(conn)
            except OSError:
                continue

    thread = threading.Thread(target=accept_loop, daemon=True)
    thread.start()
    yield server.getsockname()
    stop.set()
    thread.join(timeout=2)
    for conn in accepted:
        conn.close()
    server.close()


def test_silent_baostock_server_becomes_data_fetch_error(monkeypatch, silent_server):
    host, port = silent_server
    monkeypatch.setattr(bs_constants, "BAOSTOCK_SERVER_IP", host)
    monkeypatch.setattr(bs_constants, "BAOSTOCK_SERVER_PORT", port)
    monkeypatch.setattr(baostock_fetcher, "_BAOSTOCK_SOCKET_TIMEOUT", 0.5)
    default_before = socket.getdefaulttimeout()

    fetcher = baostock_fetcher.BaostockFetcher()
    started = time.monotonic()
    with pytest.raises(DataFetchError, match="Baostock 登录失败"):
        fetcher._fetch_raw_data("300347", "2026-10-01", "2026-10-09")

    assert time.monotonic() - started < 10
    assert socket.getdefaulttimeout() == default_before


def test_login_sets_timeout_on_baostock_connection_and_restores_default(monkeypatch):
    seen = {}

    class FakeSocket:
        def settimeout(self, value):
            seen["conn_timeout"] = value

    def fake_login():
        seen["default_during_login"] = socket.getdefaulttimeout()
        monkeypatch.setattr(bs_context, "default_socket", FakeSocket(), raising=False)
        return SimpleNamespace(error_code="0")

    default_before = socket.getdefaulttimeout()
    result = baostock_fetcher._baostock_login_with_timeout(SimpleNamespace(login=fake_login), 7.0)

    assert result.error_code == "0"
    assert seen == {"default_during_login": 7.0, "conn_timeout": 7.0}
    assert socket.getdefaulttimeout() == default_before


def test_login_restores_default_timeout_when_login_raises():
    def broken_login():
        raise RuntimeError("boom")

    default_before = socket.getdefaulttimeout()
    with pytest.raises(RuntimeError):
        baostock_fetcher._baostock_login_with_timeout(SimpleNamespace(login=broken_login), 3.0)
    assert socket.getdefaulttimeout() == default_before
