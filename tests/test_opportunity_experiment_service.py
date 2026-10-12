# -*- coding: utf-8 -*-
"""R4: trusted evidence import and isolation for the opportunity A/B/C experiment.

These tests assert that one shared research bundle becomes an executable
arm-local report only through ``import_research_bundle``, that the copy is bound
to ``experiment:<id>:<arm>`` and idempotent, that a tampered or wrong-version
bundle fails closed, and that the registry proves the three arm databases are
distinct and never the main database — without ever exposing credentials.
"""

import hashlib
import json
import os
from datetime import date, datetime

import pytest

from src.config import Config
from src.storage import (
    DatabaseManager,
    evaluate_report_execution_intent,
)
from src.services.market_opportunity_service import RESEARCH_BUNDLE_VERSION
from src.services.opportunity_experiment_service import (
    OpportunityExperimentError,
    OpportunityExperimentRegistry,
    OpportunityExperimentService,
    experiment_scope,
    market_input_batch_id,
)

AS_OF = date(2026, 9, 24)


@pytest.fixture()
def isolated_db(tmp_path):
    old = os.environ.get("DATABASE_PATH")
    old_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_PATH"] = str(tmp_path / "opp-exp.db")
    os.environ.pop("DATABASE_URL", None)
    Config.reset_instance()
    DatabaseManager.reset_instance()
    db = DatabaseManager.get_instance()
    try:
        yield db
    finally:
        DatabaseManager.reset_instance()
        Config.reset_instance()
        for key, value in (("DATABASE_PATH", old), ("DATABASE_URL", old_url)):
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _stable_hash(value):
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _report_payload():
    """A bullish, executable analysis payload with no account/position state."""

    return {
        "code": "600519",
        "name": "贵州茅台",
        "sentiment_score": 68,
        "trend_prediction": "看多",
        "operation_advice": "买入",
        "decision_type": "buy",
        "confidence_level": "高",
        "report_language": "zh",
        "action": "buy",
        "action_label": "买入",
        "raw_score": 68,
        "adjusted_score": 68,
        "holding_state": "empty",
        "position_actions": {
            "empty": {"action": "buy", "reason": "空仓建仓"},
            "holding": {"action": "hold", "reason": "持有"},
        },
        "dashboard": {
            "battle_plan": {
                "sniper_points": {
                    "ideal_buy": 100.0,
                    "secondary_buy": 96.0,
                    "stop_loss": 95.0,
                    "take_profit": 130.0,
                }
            },
            "phase_decision": {
                "entry_mode": "conditional",
                "action_window": "T+1 开盘",
                "immediate_action": "等待回踩",
                "next_check_time": "次日收盘",
                "watch_conditions": ["放量突破"],
            },
        },
        "analysis_summary": "示例研究正文",
        "success": True,
        "model_used": "model-a",
    }


def _bundle(*, payload=None, report_id=4242, model="model-a", candidate_key="cn:600519:1"):
    payload = payload if payload is not None else _report_payload()
    input_hash = _stable_hash({"as_of": AS_OF.isoformat(), "code": "600519"})
    output_hash = _stable_hash(payload)
    bundle_id = _stable_hash({
        "report_id": int(report_id),
        "candidate_key": candidate_key,
        "model": model,
        "as_of": AS_OF.isoformat(),
        "research_kind": "initial",
        "input_hash": input_hash,
        "output_hash": output_hash,
        "bundle_version": RESEARCH_BUNDLE_VERSION,
    })
    return {
        "bundle_version": RESEARCH_BUNDLE_VERSION,
        "bundle_id": bundle_id,
        "experiment_id": None,
        "report_id": int(report_id),
        "research_kind": "initial",
        "as_of": AS_OF.isoformat(),
        "candidate": {"candidate_key": candidate_key, "stock_code": "600519"},
        "model": model,
        "input_hash": input_hash,
        "output_hash": output_hash,
        "report_payload": payload,
        "report_type": "full",
        "outcome": {"classification": "bullish", "action": "buy", "action_label": "买入"},
        "conditions": {},
    }


def _registry_mapping(**overrides):
    base = {
        "experiment_id": "exp1",
        "arm_db_env": {"a": "EXP_A_DB_URL", "b": "EXP_B_DB_URL", "c": "EXP_C_DB_URL"},
        "initial_capital": 100000.0,
        "code_version": "batch2-r4",
        "main_db_url": "postgresql://dsa:secret@prod-host:5432/main",
    }
    base.update(overrides)
    return base


# ---- import round-trip --------------------------------------------------


def test_import_creates_arm_scoped_report_intent_and_signal(isolated_db):
    service = OpportunityExperimentService(db_manager=isolated_db)
    result = service.import_research_bundle(_bundle(), experiment_id="exp1", arm="b")

    assert result["created"] is True
    assert result["execution_scope"] == "experiment:exp1:b"
    assert result["report_id"] > 0
    assert result["decision_signal_id"] is not None

    intent = isolated_db.get_report_execution_intent(result["report_id"])
    assert intent is not None
    assert intent.execution_scope == "experiment:exp1:b"
    assert intent.execution_intent == "paper_candidate"
    assert intent.intent_source == "opportunity_experiment_import"

    verdict = evaluate_report_execution_intent(intent, execution_scope="experiment:exp1:b")
    assert verdict["authorized"] is True
    # 主账户拒绝该实验记录：primary scope 授权失败。
    assert evaluate_report_execution_intent(intent, execution_scope="primary")["authorized"] is False


def test_reimport_is_idempotent(isolated_db):
    service = OpportunityExperimentService(db_manager=isolated_db)
    bundle = _bundle()
    first = service.import_research_bundle(bundle, experiment_id="exp1", arm="b")
    second = service.import_research_bundle(bundle, experiment_id="exp1", arm="b")

    assert second["created"] is False
    assert second["report_id"] == first["report_id"]


def test_same_bundle_into_three_arms_stays_isolated(isolated_db):
    service = OpportunityExperimentService(db_manager=isolated_db)
    bundle = _bundle()
    reports = {
        arm: service.import_research_bundle(bundle, experiment_id="exp1", arm=arm)
        for arm in ("a", "b", "c")
    }

    report_ids = {res["report_id"] for res in reports.values()}
    assert len(report_ids) == 3  # 同一证据在三组各自独立落库，互不复用。
    for arm, res in reports.items():
        intent = isolated_db.get_report_execution_intent(res["report_id"])
        assert intent.execution_scope == f"experiment:exp1:{arm}"
        # 每组报告只对本组 scope 授权，其它组无法消费。
        for other in ("a", "b", "c"):
            expected = other == arm
            authorized = evaluate_report_execution_intent(
                intent, execution_scope=f"experiment:exp1:{other}"
            )["authorized"]
            assert authorized is expected


# ---- fail-closed integrity ---------------------------------------------


def test_wrong_bundle_version_is_rejected(isolated_db):
    service = OpportunityExperimentService(db_manager=isolated_db)
    bundle = _bundle()
    bundle["bundle_version"] = "opportunity-research-bundle-v0"
    with pytest.raises(OpportunityExperimentError):
        service.import_research_bundle(bundle, experiment_id="exp1", arm="b")


def test_tampered_bundle_id_is_rejected(isolated_db):
    service = OpportunityExperimentService(db_manager=isolated_db)
    bundle = _bundle()
    bundle["model"] = "model-b"  # bundle_id no longer matches contents
    with pytest.raises(OpportunityExperimentError):
        service.import_research_bundle(bundle, experiment_id="exp1", arm="b")


def test_tampered_payload_breaks_output_hash(isolated_db):
    service = OpportunityExperimentService(db_manager=isolated_db)
    bundle = _bundle()
    # Swap the payload for one whose hash no longer equals output_hash.
    bundle["report_payload"] = {**_report_payload(), "action": "sell"}
    with pytest.raises(OpportunityExperimentError):
        service.import_research_bundle(bundle, experiment_id="exp1", arm="b")


def test_failed_payload_is_not_executable(isolated_db):
    service = OpportunityExperimentService(db_manager=isolated_db)
    payload = {**_report_payload(), "success": False}
    with pytest.raises(OpportunityExperimentError):
        service.import_research_bundle(_bundle(payload=payload), experiment_id="exp1", arm="b")


def test_missing_payload_is_rejected(isolated_db):
    service = OpportunityExperimentService(db_manager=isolated_db)
    bundle = _bundle()
    bundle["report_payload"] = None
    with pytest.raises(OpportunityExperimentError):
        service.import_research_bundle(bundle, experiment_id="exp1", arm="b")


# ---- registry isolation -------------------------------------------------


def test_registry_resolves_distinct_non_main_databases():
    registry = OpportunityExperimentRegistry.from_mapping(_registry_mapping())
    env = {
        "EXP_A_DB_URL": "postgresql://dsa:pa@host:5432/exp_a",
        "EXP_B_DB_URL": "postgresql://dsa:pb@host:5432/exp_b",
        "EXP_C_DB_URL": "postgresql://dsa:pc@host:5432/exp_c",
    }
    resolved = registry.resolve_isolated_databases(
        main_db_url="postgresql://dsa:secret@prod-host:5432/main", env=env
    )
    assert set(resolved) == {"a", "b", "c"}
    for safe in resolved.values():
        assert "***" in safe  # credentials hidden in audit output


def test_registry_rejects_arm_pointing_at_main_database():
    registry = OpportunityExperimentRegistry.from_mapping(_registry_mapping())
    main = "postgresql://dsa:secret@prod-host:5432/main"
    env = {
        "EXP_A_DB_URL": "postgresql://other:pw@prod-host:5432/main",  # same target as main
        "EXP_B_DB_URL": "postgresql://dsa:pb@host:5432/exp_b",
        "EXP_C_DB_URL": "postgresql://dsa:pc@host:5432/exp_c",
    }
    with pytest.raises(OpportunityExperimentError):
        registry.resolve_isolated_databases(main_db_url=main, env=env)


def test_registry_rejects_two_arms_sharing_a_database():
    registry = OpportunityExperimentRegistry.from_mapping(_registry_mapping())
    env = {
        "EXP_A_DB_URL": "postgresql://dsa:pa@host:5432/exp_shared",
        "EXP_B_DB_URL": "postgresql://dsa:pb@host:5432/exp_shared",
        "EXP_C_DB_URL": "postgresql://dsa:pc@host:5432/exp_c",
    }
    with pytest.raises(OpportunityExperimentError):
        registry.resolve_isolated_databases(
            main_db_url="postgresql://dsa:secret@prod-host:5432/main", env=env
        )


def test_registry_requires_distinct_env_var_names():
    with pytest.raises(OpportunityExperimentError):
        OpportunityExperimentRegistry.from_mapping(
            _registry_mapping(arm_db_env={"a": "SAME", "b": "SAME", "c": "EXP_C_DB_URL"})
        )


def test_registry_rejects_missing_arm_env_name():
    with pytest.raises(OpportunityExperimentError):
        OpportunityExperimentRegistry.from_mapping(
            _registry_mapping(arm_db_env={"a": "EXP_A_DB_URL", "b": "EXP_B_DB_URL"})
        )


# ---- shared helpers -----------------------------------------------------


def test_experiment_scope_is_normalized():
    assert experiment_scope("exp1", "B") == "experiment:exp1:b"
    with pytest.raises(OpportunityExperimentError):
        experiment_scope("exp1", "d")


def test_market_input_batch_id_is_stable_and_arm_independent():
    first = market_input_batch_id("exp1", AS_OF)
    second = market_input_batch_id("exp1", datetime(2026, 9, 24, 12, 0))
    assert first == second  # same session, no arm in the key
    assert first != market_input_batch_id("exp1", date(2026, 9, 25))
