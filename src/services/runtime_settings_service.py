# -*- coding: utf-8 -*-
"""Shared, audited runtime settings stored in the application database."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from sqlalchemy import desc
from sqlalchemy.exc import IntegrityError

from src.config import Config, MARKET_OPPORTUNITY_MODES
from src.storage import (
    DatabaseManager,
    RuntimeSetting,
    RuntimeSettingAudit,
    utc_naive_now,
)


logger = logging.getLogger(__name__)
MARKET_OPPORTUNITY_MODE_KEY = "MARKET_OPPORTUNITY_MODE"


class RuntimeSettingConflictError(Exception):
    """Raised when a runtime setting update uses a stale version."""

    def __init__(self, current_version: Optional[int]):
        super().__init__("Runtime setting version conflict")
        self.current_version = current_version


class RuntimeSettingValidationError(ValueError):
    """Raised when a runtime setting value is outside its supported domain."""


@dataclass(frozen=True)
class EffectiveRuntimeSetting:
    key: str
    value: str
    source: str
    version: Optional[int]
    updated_at: Optional[str]
    updated_by: Optional[str]
    provenance: Dict[str, Any]
    degraded: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "source": self.source,
            "version": self.version,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
            "provenance": dict(self.provenance),
            "degraded": self.degraded,
        }


def normalize_market_opportunity_mode(value: Optional[str]) -> str:
    """Normalize and strictly validate the market opportunity runtime mode."""

    mode = str(value or "off").strip().lower()
    if mode not in MARKET_OPPORTUNITY_MODES:
        raise RuntimeSettingValidationError(
            "MARKET_OPPORTUNITY_MODE must be one of: off, observe, paper"
        )
    return mode


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True, default=str)


def _json_loads(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


class RuntimeSettingsService:
    """Resolve and mutate shared runtime settings with audit and version checks."""

    def __init__(
        self,
        *,
        config: Optional[Config] = None,
        db_manager: Optional[DatabaseManager] = None,
    ) -> None:
        self.config = config
        self.db = db_manager or DatabaseManager.get_instance()

    def get_market_opportunity_mode(self) -> EffectiveRuntimeSetting:
        try:
            row, fallback_version = self._get_setting_with_revision(MARKET_OPPORTUNITY_MODE_KEY)
        except Exception as exc:
            logger.warning("读取运行时设置失败，已 fail-closed 到 MARKET_OPPORTUNITY_MODE=off: %s", exc, exc_info=True)
            return EffectiveRuntimeSetting(
                key=MARKET_OPPORTUNITY_MODE_KEY,
                value="off",
                source="degraded",
                version=None,
                updated_at=None,
                updated_by=None,
                provenance={
                    "degraded": True,
                    "reason": "runtime_setting_read_failed",
                    "error_type": type(exc).__name__,
                },
                degraded=True,
            )
        if row is not None:
            try:
                mode = normalize_market_opportunity_mode(row.value)
            except RuntimeSettingValidationError as exc:
                logger.warning("运行时设置值无效，已 fail-closed 到 MARKET_OPPORTUNITY_MODE=off: %s", exc)
                return EffectiveRuntimeSetting(
                    key=MARKET_OPPORTUNITY_MODE_KEY,
                    value="off",
                    source="degraded",
                    version=int(row.version or 0),
                    updated_at=row.updated_at.isoformat() if row.updated_at else None,
                    updated_by=row.updated_by,
                    provenance={
                        "degraded": True,
                        "reason": "runtime_setting_invalid_value",
                        "stored_value": row.value,
                    },
                    degraded=True,
                )
            return EffectiveRuntimeSetting(
                key=MARKET_OPPORTUNITY_MODE_KEY,
                value=mode,
                source="database",
                version=int(row.version or 0),
                updated_at=row.updated_at.isoformat() if row.updated_at else None,
                updated_by=row.updated_by,
                provenance=_json_loads(row.provenance_json),
            )
        env_value = getattr(self.config, "market_opportunity_mode", "off") if self.config is not None else "off"
        try:
            mode = normalize_market_opportunity_mode(env_value)
        except RuntimeSettingValidationError:
            mode = "off"
        return EffectiveRuntimeSetting(
            key=MARKET_OPPORTUNITY_MODE_KEY,
            value=mode,
            source="environment",
            version=fallback_version,
            updated_at=None,
            updated_by=None,
            provenance={"fallback": "config.market_opportunity_mode"},
        )

    def set_market_opportunity_mode(
        self,
        mode: str,
        *,
        expected_version: Optional[int] = None,
        actor: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> EffectiveRuntimeSetting:
        normalized = normalize_market_opportunity_mode(mode)
        return self._set_setting(
            MARKET_OPPORTUNITY_MODE_KEY,
            normalized,
            expected_version=expected_version,
            actor=actor,
            reason=reason,
            provenance={"setting": MARKET_OPPORTUNITY_MODE_KEY, "validation": "strict_enum"},
        )

    def clear_market_opportunity_mode(
        self,
        *,
        expected_version: Optional[int] = None,
        actor: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> EffectiveRuntimeSetting:
        self._clear_setting(
            MARKET_OPPORTUNITY_MODE_KEY,
            expected_version=expected_version,
            actor=actor,
            reason=reason,
        )
        return self.get_market_opportunity_mode()

    def _get_setting(self, key: str) -> Optional[RuntimeSetting]:
        row, _revision = self._get_setting_with_revision(key)
        return row

    def _get_setting_with_revision(self, key: str) -> tuple[Optional[RuntimeSetting], int]:
        with self.db.session_scope() as session:
            row = session.get(RuntimeSetting, key)
            revision_row = (
                session.query(RuntimeSettingAudit.new_version)
                .filter(RuntimeSettingAudit.key == key, RuntimeSettingAudit.new_version.isnot(None))
                .order_by(desc(RuntimeSettingAudit.new_version))
                .first()
            )
            revision = int(revision_row[0]) if revision_row and revision_row[0] is not None else 0
            if row is not None:
                revision = int(row.version or revision)
                session.expunge(row)
            return row, revision

    def _latest_audit_version(self, key: str) -> int:
        with self.db.session_scope() as session:
            row = (
                session.query(RuntimeSettingAudit.new_version)
                .filter(RuntimeSettingAudit.key == key, RuntimeSettingAudit.new_version.isnot(None))
                .order_by(desc(RuntimeSettingAudit.new_version))
                .first()
            )
            return int(row[0]) if row and row[0] is not None else 0

    def _set_setting(
        self,
        key: str,
        value: str,
        *,
        expected_version: Optional[int],
        actor: Optional[str],
        reason: Optional[str],
        provenance: Dict[str, Any],
    ) -> EffectiveRuntimeSetting:
        now = utc_naive_now()
        if expected_version is None:
            raise RuntimeSettingConflictError(current_version=self.get_market_opportunity_mode().version)
        with self.db.session_scope() as session:
            row = session.get(RuntimeSetting, key)
            if row is None:
                current_version = self._latest_audit_version(key)
                if int(expected_version) != current_version:
                    raise RuntimeSettingConflictError(current_version=current_version)
                new_version = current_version + 1
                row = RuntimeSetting(
                    key=key,
                    value=value,
                    version=new_version,
                    updated_by=actor,
                    provenance_json=_json_dumps(provenance),
                    created_at=now,
                    updated_at=now,
                )
                try:
                    session.add(row)
                    session.flush()
                except IntegrityError as exc:
                    raise RuntimeSettingConflictError(current_version=self._latest_audit_version(key)) from exc
                old_value = None
                old_version = None if current_version == 0 else current_version
            else:
                current_version = int(row.version or 0)
                if int(expected_version) != current_version:
                    raise RuntimeSettingConflictError(current_version=current_version)
                old_value = row.value
                old_version = current_version
                new_version = current_version + 1
                rowcount = (
                    session.query(RuntimeSetting)
                    .filter(RuntimeSetting.key == key, RuntimeSetting.version == current_version)
                    .update(
                        {
                            RuntimeSetting.value: value,
                            RuntimeSetting.version: new_version,
                            RuntimeSetting.updated_by: actor,
                            RuntimeSetting.provenance_json: _json_dumps(provenance),
                            RuntimeSetting.updated_at: now,
                        },
                        synchronize_session=False,
                    )
                )
                if rowcount != 1:
                    fresh = session.get(RuntimeSetting, key)
                    raise RuntimeSettingConflictError(
                        current_version=int(fresh.version or 0) if fresh is not None else None
                    )
            session.add(
                RuntimeSettingAudit(
                    key=key,
                    old_value=old_value,
                    new_value=value,
                    old_version=old_version,
                    new_version=new_version,
                    action="update",
                    actor=actor,
                    reason=reason,
                    provenance_json=_json_dumps(provenance),
                    created_at=now,
                )
            )
            session.flush()
            payload = EffectiveRuntimeSetting(
                key=key,
                value=value,
                source="database",
                version=new_version,
                updated_at=now.isoformat(),
                updated_by=actor,
                provenance=dict(provenance),
            )
        return payload

    def _clear_setting(
        self,
        key: str,
        *,
        expected_version: Optional[int],
        actor: Optional[str],
        reason: Optional[str],
    ) -> None:
        now = utc_naive_now()
        if expected_version is None:
            raise RuntimeSettingConflictError(current_version=self.get_market_opportunity_mode().version)
        with self.db.session_scope() as session:
            row = session.get(RuntimeSetting, key)
            if row is None:
                current_version = self._latest_audit_version(key)
                if int(expected_version) != current_version:
                    raise RuntimeSettingConflictError(current_version=current_version)
                return
            current_version = int(row.version or 0)
            if int(expected_version) != current_version:
                raise RuntimeSettingConflictError(current_version=current_version)
            old_value = row.value
            new_version = current_version + 1
            rowcount = (
                session.query(RuntimeSetting)
                .filter(RuntimeSetting.key == key, RuntimeSetting.version == current_version)
                .delete(synchronize_session=False)
            )
            if rowcount != 1:
                fresh = session.get(RuntimeSetting, key)
                raise RuntimeSettingConflictError(
                    current_version=int(fresh.version or 0) if fresh is not None else None
                )
            session.add(
                RuntimeSettingAudit(
                    key=key,
                    old_value=old_value,
                    new_value=None,
                    old_version=current_version,
                    new_version=new_version,
                    action="clear",
                    actor=actor,
                    reason=reason,
                    provenance_json=_json_dumps({"setting": key, "fallback": "environment"}),
                    created_at=now,
                )
            )
