# -*- coding: utf-8 -*-
"""Tests for shared audited runtime settings."""

from __future__ import annotations

import os
from dataclasses import replace

import pytest

from src.config import Config
from src.services.runtime_settings_service import (
    RuntimeSettingConflictError,
    RuntimeSettingValidationError,
    RuntimeSettingsService,
)
from src.storage import DatabaseManager, RuntimeSettingAudit


@pytest.fixture()
def isolated_db(tmp_path):
    old_database_path = os.environ.get("DATABASE_PATH")
    os.environ["DATABASE_PATH"] = str(tmp_path / "runtime-settings.db")
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


def test_market_opportunity_mode_uses_environment_until_database_override(isolated_db):
    config = replace(Config.get_instance(), market_opportunity_mode="observe")
    service = RuntimeSettingsService(config=config, db_manager=isolated_db)

    effective = service.get_market_opportunity_mode()
    assert effective.value == "observe"
    assert effective.source == "environment"
    assert effective.version == 0

    updated = service.set_market_opportunity_mode(
        "paper",
        expected_version=0,
        actor="test",
        reason="unit test",
    )
    assert updated.value == "paper"
    assert updated.source == "database"
    assert updated.version == 1

    reloaded = RuntimeSettingsService(config=config, db_manager=isolated_db).get_market_opportunity_mode()
    assert reloaded.value == "paper"
    assert reloaded.source == "database"
    assert reloaded.version == 1


def test_market_opportunity_mode_conflict_and_clear_restore_fallback(isolated_db):
    config = replace(Config.get_instance(), market_opportunity_mode="observe")
    service = RuntimeSettingsService(config=config, db_manager=isolated_db)
    first = service.set_market_opportunity_mode("off", expected_version=0, actor="test")

    with pytest.raises(RuntimeSettingConflictError) as conflict:
        service.set_market_opportunity_mode("paper", expected_version=999, actor="test")
    assert conflict.value.current_version == first.version

    fallback = service.clear_market_opportunity_mode(
        expected_version=first.version,
        actor="test",
        reason="restore env",
    )
    assert fallback.value == "observe"
    assert fallback.source == "environment"
    assert fallback.version == 2

    with isolated_db.session_scope() as session:
        audits = [
            (row.action, row.old_version, row.new_version)
            for row in session.query(RuntimeSettingAudit).order_by(RuntimeSettingAudit.id).all()
        ]
    assert audits == [("update", None, 1), ("clear", 1, 2)]
    assert service.get_market_opportunity_mode().version == 2


def test_market_opportunity_mode_round_trip_set_clear_set_uses_monotonic_revision(isolated_db):
    config = replace(Config.get_instance(), market_opportunity_mode="observe")
    service = RuntimeSettingsService(config=config, db_manager=isolated_db)

    first = service.set_market_opportunity_mode("off", expected_version=0, actor="test")
    fallback = service.clear_market_opportunity_mode(expected_version=first.version, actor="test")
    second = service.set_market_opportunity_mode("observe", expected_version=fallback.version, actor="test")

    assert fallback.source == "environment"
    assert fallback.version == 2
    assert second.version == 3
    with isolated_db.session_scope() as session:
        audits = [
            (row.action, row.old_version, row.new_version)
            for row in session.query(RuntimeSettingAudit).order_by(RuntimeSettingAudit.id).all()
        ]
    assert audits == [("update", None, 1), ("clear", 1, 2), ("update", 2, 3)]


def test_market_opportunity_mode_stale_pre_clear_client_cannot_recreate(isolated_db):
    config = replace(Config.get_instance(), market_opportunity_mode="observe")
    service = RuntimeSettingsService(config=config, db_manager=isolated_db)

    first = service.set_market_opportunity_mode("off", expected_version=0, actor="first")
    service.clear_market_opportunity_mode(expected_version=first.version, actor="second")

    with pytest.raises(RuntimeSettingConflictError) as conflict:
        service.set_market_opportunity_mode("paper", expected_version=first.version, actor="stale")

    assert conflict.value.current_version == 2


def test_market_opportunity_mode_concurrent_create_conflicts_after_first_writer(isolated_db):
    config = replace(Config.get_instance(), market_opportunity_mode="observe")
    first = RuntimeSettingsService(config=config, db_manager=isolated_db)
    second = RuntimeSettingsService(config=config, db_manager=isolated_db)

    assert first.get_market_opportunity_mode().version == 0
    assert second.get_market_opportunity_mode().version == 0
    created = first.set_market_opportunity_mode("observe", expected_version=0, actor="first")

    with pytest.raises(RuntimeSettingConflictError) as conflict:
        second.set_market_opportunity_mode("paper", expected_version=0, actor="second")

    assert created.version == 1
    assert conflict.value.current_version == 1


def test_market_opportunity_mode_rejects_invalid_values(isolated_db):
    service = RuntimeSettingsService(
        config=replace(Config.get_instance(), market_opportunity_mode="observe"),
        db_manager=isolated_db,
    )
    with pytest.raises(RuntimeSettingValidationError):
        service.set_market_opportunity_mode("true", expected_version=0, actor="test")


def test_market_opportunity_mode_fails_closed_when_database_read_fails(isolated_db):
    config = replace(Config.get_instance(), market_opportunity_mode="paper")
    service = RuntimeSettingsService(config=config, db_manager=isolated_db)
    service._get_setting_with_revision = lambda key: (_ for _ in ()).throw(RuntimeError("db unavailable"))

    effective = service.get_market_opportunity_mode()

    assert effective.value == "off"
    assert effective.source == "degraded"
    assert effective.degraded is True
    assert effective.version is None
    assert effective.provenance["reason"] == "runtime_setting_read_failed"


def test_market_opportunity_mode_reads_row_and_revision_in_one_method_call(isolated_db):
    config = replace(Config.get_instance(), market_opportunity_mode="observe")
    service = RuntimeSettingsService(config=config, db_manager=isolated_db)
    service.set_market_opportunity_mode("paper", expected_version=0, actor="test")
    calls = []
    original = service._get_setting_with_revision

    def tracked(key):
        calls.append(key)
        return original(key)

    service._get_setting_with_revision = tracked

    effective = service.get_market_opportunity_mode()

    assert calls == ["MARKET_OPPORTUNITY_MODE"]
    assert effective.source == "database"
    assert effective.value == "paper"
    assert effective.version == 1


def test_market_opportunity_mode_requires_expected_version(isolated_db):
    service = RuntimeSettingsService(
        config=replace(Config.get_instance(), market_opportunity_mode="observe"),
        db_manager=isolated_db,
    )

    with pytest.raises(RuntimeSettingConflictError) as conflict:
        service.set_market_opportunity_mode("off", actor="test")

    assert conflict.value.current_version == 0


def test_market_opportunity_mode_stale_two_reader_update_conflicts(isolated_db):
    config = replace(Config.get_instance(), market_opportunity_mode="observe")
    first = RuntimeSettingsService(config=config, db_manager=isolated_db)
    second = RuntimeSettingsService(config=config, db_manager=isolated_db)

    created = first.set_market_opportunity_mode("observe", expected_version=0, actor="first")
    assert second.get_market_opportunity_mode().version == created.version
    updated = first.set_market_opportunity_mode("off", expected_version=created.version, actor="first")

    with pytest.raises(RuntimeSettingConflictError) as conflict:
        second.set_market_opportunity_mode("paper", expected_version=created.version, actor="second")

    assert updated.version == 2
    assert conflict.value.current_version == 2
