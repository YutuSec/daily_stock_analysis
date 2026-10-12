# -*- coding: utf-8 -*-
"""R6/R7: read-only execution-scope routing on the paper-trading list surface.

These tests prove the API can surface an isolated experiment arm's ledger on the
existing page without disturbing the main-DB singleton: ``primary`` reads the
main account, an ``experiment:<id>:<arm>`` scope is resolved only through the
*trusted* registry file (never a client-supplied DB address) to a read-only
connection on that arm's isolated database, and unknown / unconfigured scopes
fail closed instead of silently reading the main account.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from api.app import create_app
from src.config import Config
from src.storage import DatabaseManager, PaperCapitalAccount


class PaperTradingScopeApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._old_env = {
            key: os.environ.get(key)
            for key in (
                "ENV_FILE",
                "DATABASE_PATH",
                "DATABASE_URL",
                "EXP_A_DB_URL",
                "EXP_B_DB_URL",
                "EXP_C_DB_URL",
                "OPPORTUNITY_EXPERIMENT_REGISTRY_PATH",
                "PAPER_TRADING_ENABLED",
            )
        }
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.main_db = root / "paper-main.db"
        self.arm_b_db = root / "paper-exp-b.db"

        # 三组隔离数据库路径；registry 只存 env 变量名，不存地址。
        self.env = {
            "EXP_A_DB_URL": f"sqlite:///{root / 'paper-exp-a.db'}",
            "EXP_B_DB_URL": f"sqlite:///{self.arm_b_db}",
            "EXP_C_DB_URL": f"sqlite:///{root / 'paper-exp-c.db'}",
        }
        registry_path = root / "experiments.json"
        registry_path.write_text(json.dumps({
            "experiment_id": "exp1",
            "arm_db_env": {"a": "EXP_A_DB_URL", "b": "EXP_B_DB_URL", "c": "EXP_C_DB_URL"},
            "initial_capital": 100000.0,
            "code_version": "batch3-r6",
            "main_db_url": f"sqlite:///{self.main_db}",
        }, ensure_ascii=False), encoding="utf-8")

        env_path = root / ".env"
        env_path.write_text(
            "\n".join((
                "GEMINI_API_KEY=test",
                "ADMIN_AUTH_ENABLED=false",
                "PAPER_TRADING_ENABLED=false",
                f"DATABASE_PATH={self.main_db}",
                f"OPPORTUNITY_EXPERIMENT_REGISTRY_PATH={registry_path}",
            )) + "\n",
            encoding="utf-8",
        )
        os.environ["ENV_FILE"] = str(env_path)
        os.environ["DATABASE_PATH"] = str(self.main_db)
        os.environ.pop("DATABASE_URL", None)
        os.environ["OPPORTUNITY_EXPERIMENT_REGISTRY_PATH"] = str(registry_path)
        os.environ["PAPER_TRADING_ENABLED"] = "false"
        for key, value in self.env.items():
            os.environ[key] = value

        Config.reset_instance()
        DatabaseManager.reset_instance()
        # Seed a marker account in arm B's isolated DB so reads that hit it are provable.
        self._seed_account(self.env["EXP_B_DB_URL"], model="arm-b-model")
        # Rebuild the process singleton bound to the MAIN db and seed a distinct marker.
        Config.reset_instance()
        DatabaseManager.reset_instance()
        os.environ["DATABASE_PATH"] = str(self.main_db)
        os.environ.pop("DATABASE_URL", None)
        Config.reset_instance()
        DatabaseManager.reset_instance()
        main_db = DatabaseManager.get_instance()
        with main_db.session_scope() as session:
            session.add(PaperCapitalAccount(
                account_key="primary-model:cn:CNY", source_model="primary-model",
                market="cn", currency="CNY", initial_capital=100000.0, cash=100000.0,
            ))
        self.client = TestClient(create_app(static_dir=root / "missing-static"))

    def _seed_account(self, db_url: str, *, model: str) -> None:
        # Build a real (writable) manager on the arm URL just to seed it.
        os.environ["DATABASE_URL"] = db_url
        os.environ.pop("DATABASE_PATH", None)
        Config.reset_instance()
        DatabaseManager.reset_instance()
        arm_db = DatabaseManager.get_instance()
        with arm_db.session_scope() as session:
            session.add(PaperCapitalAccount(
                account_key=f"{model}:cn:CNY", source_model=model, market="cn",
                currency="CNY", initial_capital=100000.0, cash=100000.0,
            ))
        DatabaseManager.reset_instance()
        os.environ.pop("DATABASE_URL", None)

    def tearDown(self) -> None:
        self.client.app.dependency_overrides.clear()
        DatabaseManager.reset_instance()
        Config.reset_instance()
        for key, value in self._old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.temp_dir.cleanup()

    def test_scopes_endpoint_lists_primary_and_experiment_arms(self) -> None:
        response = self.client.get("/api/v1/paper-trading/scopes")
        self.assertEqual(response.status_code, 200)
        items = response.json()["items"]
        scope_ids = [item["scope_id"] for item in items]
        self.assertIn("primary", scope_ids)
        self.assertIn("experiment:exp1:a", scope_ids)
        self.assertIn("experiment:exp1:b", scope_ids)
        self.assertIn("experiment:exp1:c", scope_ids)
        # 范围元数据绝不暴露数据库地址或凭据。
        for item in items:
            self.assertNotIn("sqlite", json.dumps(item))
            self.assertNotIn("://", json.dumps(item))

    def test_primary_scope_reads_main_account(self) -> None:
        response = self.client.get("/api/v1/paper-trading/accounts")
        self.assertEqual(response.status_code, 200)
        models = [item.get("source_model") for item in response.json()["items"]]
        self.assertIn("primary-model", models)
        self.assertNotIn("arm-b-model", models)

    def test_experiment_scope_reads_isolated_arm_ledger(self) -> None:
        response = self.client.get(
            "/api/v1/paper-trading/accounts?scope_id=experiment:exp1:b"
        )
        self.assertEqual(response.status_code, 200)
        models = [item.get("source_model") for item in response.json()["items"]]
        # 只读到 B 组隔离库里的账户，读不到主账户。
        self.assertIn("arm-b-model", models)
        self.assertNotIn("primary-model", models)

    def test_unknown_experiment_scope_fails_closed(self) -> None:
        response = self.client.get(
            "/api/v1/paper-trading/accounts?scope_id=experiment:nope:a"
        )
        self.assertEqual(response.status_code, 404)
        self.assertNotIn("primary-model", json.dumps(response.json()))

    def test_malformed_scope_is_rejected(self) -> None:
        response = self.client.get(
            "/api/v1/paper-trading/accounts?scope_id=not-a-scope"
        )
        self.assertEqual(response.status_code, 404)

    def test_experiment_scope_without_registry_fails_closed(self) -> None:
        # 清掉 registry 配置后（.env 与环境变量都不含），实验范围不可用，绝不回落到主账户。
        root = Path(self.temp_dir.name)
        env_path = root / ".env"
        env_path.write_text(
            "\n".join((
                "GEMINI_API_KEY=test",
                "ADMIN_AUTH_ENABLED=false",
                "PAPER_TRADING_ENABLED=false",
                f"DATABASE_PATH={self.main_db}",
            )) + "\n",
            encoding="utf-8",
        )
        os.environ.pop("OPPORTUNITY_EXPERIMENT_REGISTRY_PATH", None)
        os.environ["DATABASE_PATH"] = str(self.main_db)
        os.environ.pop("DATABASE_URL", None)
        Config.reset_instance()
        DatabaseManager.reset_instance()
        client = TestClient(create_app(static_dir=root / "missing-static"))
        response = client.get("/api/v1/paper-trading/accounts?scope_id=experiment:exp1:b")
        self.assertEqual(response.status_code, 404)
        self.assertNotIn("primary-model", json.dumps(response.json()))
        self.assertNotIn("arm-b-model", json.dumps(response.json()))


if __name__ == "__main__":
    unittest.main()
