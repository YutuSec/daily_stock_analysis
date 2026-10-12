# -*- coding: utf-8 -*-
import unittest
import sys
import os
import sqlite3
import tempfile
import threading
from datetime import date
from unittest.mock import patch

import pandas as pd
from sqlalchemy import and_, create_engine as sqlalchemy_create_engine, select
from sqlalchemy.sql import func

# Ensure src module can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.config import Config
from src.storage import (
    REPORT_EXECUTION_INTENT_BASELINE_VERSION,
    AnalysisHistory,
    AnalysisReportExecutionIntent,
    Base,
    CURRENT_SCHEMA_VERSION,
    DatabaseManager,
    DatabaseSchemaMigration,
    PaperAccountRiskEvent,
    PaperAccountRiskSnapshot,
    StockDaily,
    evaluate_report_execution_intent,
)

class TestStorage(unittest.TestCase):

    @staticmethod
    def _list_sqlite_indexes(db_path: str, table_name: str) -> dict[str, list[str]]:
        with sqlite3.connect(db_path) as conn:
            indexes = {}
            for row in conn.execute(f"PRAGMA index_list({table_name})").fetchall():
                index_name = row[1]
                indexes[index_name] = [
                    index_info[2]
                    for index_info in conn.execute(
                        f"PRAGMA index_xinfo({index_name})"
                    ).fetchall()
                    if index_info[2] is not None and int(index_info[5]) == 1
                ]
            return indexes

    @staticmethod
    def _list_sqlite_unique_indexes(db_path: str, table_name: str) -> dict[str, list[str]]:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(f"PRAGMA index_list({table_name})").fetchall()
            unique_indexes = {}
            for row in rows:
                if int(row[2]) != 1:
                    continue
                index_name = row[1]
                index_columns = []
                for index_info in conn.execute(f"PRAGMA index_xinfo({index_name})").fetchall():
                    column_name = index_info[2]
                    if column_name is not None:
                        index_columns.append(column_name)
                unique_indexes[index_name] = index_columns
            return unique_indexes

    @staticmethod
    def _list_sqlite_foreign_keys(db_path: str, table_name: str) -> list[tuple[str, str, str]]:
        with sqlite3.connect(db_path) as conn:
            return [
                (row[2], row[3], row[4])
                for row in conn.execute(f"PRAGMA foreign_key_list({table_name})").fetchall()
            ]

    def test_legacy_intelligence_items_url_unique_index_rebuilds_without_collision(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "legacy_intel.sqlite")

        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """CREATE TABLE intelligence_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    source_type TEXT NOT NULL DEFAULT 'rss',
                    url TEXT NOT NULL,
                    scope_type TEXT NOT NULL DEFAULT 'market',
                    scope_value TEXT,
                    market TEXT NOT NULL DEFAULT 'cn',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    last_status TEXT,
                    last_error TEXT,
                    last_fetched_at DATETIME,
                    created_at DATETIME,
                    updated_at DATETIME
                )"""
                )
                conn.execute(
                    """CREATE TABLE intelligence_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER,
                    source_name TEXT,
                    source_type TEXT NOT NULL DEFAULT 'rss',
                    title TEXT NOT NULL,
                    summary TEXT,
                    url TEXT NOT NULL,
                    source TEXT,
                    published_at DATETIME,
                    fetched_at DATETIME,
                    scope_type TEXT NOT NULL DEFAULT 'market',
                    scope_value TEXT,
                    market TEXT NOT NULL DEFAULT 'cn',
                    raw_payload TEXT
                )"""
                )
                conn.execute("CREATE UNIQUE INDEX uix_intelligence_item_url_legacy ON intelligence_items(url)")
                conn.execute("CREATE INDEX ix_intel_item_scope_time ON intelligence_items(scope_type, scope_value, market, published_at)")
                conn.execute("CREATE INDEX ix_intel_item_fetch_time ON intelligence_items(fetched_at)")
                conn.execute("INSERT INTO intelligence_sources (name, url) VALUES ('legacy', 'https://legacy.example.com/rss.xml')")
                source_id = conn.execute("SELECT id FROM intelligence_sources WHERE name='legacy'").fetchone()[0]
                conn.executemany(
                    """INSERT INTO intelligence_items (
                    source_id,
                    source_name,
                    source_type,
                    title,
                    summary,
                    url,
                    source,
                    published_at,
                    fetched_at,
                    scope_type,
                    scope_value,
                    market,
                    raw_payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        (source_id, 'legacy-source', 'rss', 'A', 'legacy-a', 'https://legacy.example.com/a', 'legacy',
                         '2026-01-01 00:00:00', '2026-01-01 00:00:00', 'market', None, 'cn', None),
                        (source_id, 'legacy-source', 'rss', 'B', 'legacy-b', 'https://legacy.example.com/b', 'legacy',
                         '2026-01-02 00:00:00', '2026-01-02 00:00:00', 'market', None, 'cn', None),
                    ],
                )

            unique_indexes_before = self._list_sqlite_unique_indexes(db_path, "intelligence_items")
            self.assertIn("uix_intelligence_item_url_legacy", unique_indexes_before)
            self.assertEqual(unique_indexes_before["uix_intelligence_item_url_legacy"], ["url"])

            DatabaseManager.reset_instance()
            Config.reset_instance()
            DatabaseManager(db_url=f"sqlite:///{db_path}")

            unique_indexes_after = self._list_sqlite_unique_indexes(db_path, "intelligence_items")
            self.assertNotIn("uix_intelligence_item_url_legacy", unique_indexes_after)
            self.assertIn("uix_intel_item_scope", unique_indexes_after)
            self.assertEqual(
                unique_indexes_after["uix_intel_item_scope"],
                ["source_id", "url", "scope_type", "scope_value", "market"],
            )
            with sqlite3.connect(db_path) as conn:
                table_count = conn.execute("SELECT COUNT(*) FROM intelligence_items").fetchone()[0]
                temp_tables = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'intelligence_items_recreate_tmp_%'"
                ).fetchall()

            self.assertEqual(table_count, 2)
            self.assertEqual(temp_tables, [])
        finally:
            DatabaseManager.reset_instance()
            Config.reset_instance()
            temp_dir.cleanup()

    def test_database_initialization_records_schema_version(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")

        with db.get_session() as session:
            row = session.get(DatabaseSchemaMigration, CURRENT_SCHEMA_VERSION)

        self.assertIsNotNone(row)
        self.assertEqual(row.version, CURRENT_SCHEMA_VERSION)
        self.assertIn("metadata.create_all", row.description)

        DatabaseManager.reset_instance()

    def test_schema_migration_record_is_idempotent(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")

        db._ensure_schema_migration_record()
        db._ensure_schema_migration_record()

        with db.get_session() as session:
            count = session.execute(
                select(func.count())
                .select_from(DatabaseSchemaMigration)
                .where(DatabaseSchemaMigration.version == CURRENT_SCHEMA_VERSION)
            ).scalar_one()

        self.assertEqual(count, 1)

        DatabaseManager.reset_instance()

    def test_paper_account_risk_tables_are_created_with_unique_keys_and_fk_idempotently(self):
        DatabaseManager.reset_instance()
        Config.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "paper_risk_schema.db")

        try:
            DatabaseManager(db_url=f"sqlite:///{db_path}")
            DatabaseManager.reset_instance()
            DatabaseManager(db_url=f"sqlite:///{db_path}")

            with sqlite3.connect(db_path) as conn:
                tables = {
                    row[0] for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
                self.assertIn(PaperAccountRiskSnapshot.__tablename__, tables)
                self.assertIn(PaperAccountRiskEvent.__tablename__, tables)
                snapshot_unique = self._list_sqlite_unique_indexes(
                    db_path,
                    PaperAccountRiskSnapshot.__tablename__,
                )
                event_unique = self._list_sqlite_unique_indexes(
                    db_path,
                    PaperAccountRiskEvent.__tablename__,
                )
                self.assertTrue(any(cols == ["snapshot_key"] for cols in snapshot_unique.values()))
                self.assertTrue(any(cols == ["event_key"] for cols in event_unique.values()))
                event_fks = self._list_sqlite_foreign_keys(db_path, PaperAccountRiskEvent.__tablename__)
                self.assertIn(("paper_capital_accounts", "capital_account_id", "id"), event_fks)
                self.assertIn(("paper_account_risk_snapshots", "snapshot_id", "id"), event_fks)
                self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
                self.assertEqual(conn.execute("PRAGMA foreign_key_check").fetchall(), [])
        finally:
            DatabaseManager.reset_instance()
            Config.reset_instance()
            temp_dir.cleanup()

    def test_legacy_analysis_history_backfills_model_used(self):
        DatabaseManager.reset_instance()
        Config.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "legacy_analysis_history.db")

        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "CREATE TABLE analysis_history ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, raw_result TEXT)"
                )
                conn.executemany(
                    "INSERT INTO analysis_history (raw_result) VALUES (?)",
                    [
                        ('{"model_used":"provider/model-a"}',),
                        ('{"model_used":""}',),
                        ('not-json',),
                    ],
                )

            DatabaseManager(db_url=f"sqlite:///{db_path}")

            with sqlite3.connect(db_path) as conn:
                columns = {
                    row[1] for row in conn.execute("PRAGMA table_info(analysis_history)")
                }
                values = conn.execute(
                    "SELECT model_used FROM analysis_history ORDER BY id"
                ).fetchall()

            self.assertIn("model_used", columns)
            self.assertEqual(values, [("provider/model-a",), (None,), (None,)])
            self.assertEqual(
                self._list_sqlite_indexes(db_path, "analysis_history").get(
                    "ix_analysis_history_model_used"
                ),
                ["model_used"],
            )
        finally:
            DatabaseManager.reset_instance()
            Config.reset_instance()
            temp_dir.cleanup()

    def test_legacy_backtest_tables_gain_dual_metric_columns_idempotently(self):
        DatabaseManager.reset_instance()
        Config.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "legacy_backtest_metrics.db")

        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE analysis_history (id INTEGER PRIMARY KEY, raw_result TEXT)")
                conn.execute("CREATE TABLE backtest_results (id INTEGER PRIMARY KEY)")
                conn.execute(
                    "CREATE TABLE backtest_summaries ("
                    "id INTEGER PRIMARY KEY, total_evaluations INTEGER)"
                )
                conn.execute(
                    "INSERT INTO backtest_summaries (id, total_evaluations) VALUES (1, 7)"
                )

            db = DatabaseManager(db_url=f"sqlite:///{db_path}")
            db._ensure_backtest_metric_columns()

            with sqlite3.connect(db_path) as conn:
                result_columns = {
                    row[1] for row in conn.execute("PRAGMA table_info(backtest_results)")
                }
                summary_columns = {
                    row[1] for row in conn.execute("PRAGMA table_info(backtest_summaries)")
                }
                holding_unavailable_count = conn.execute(
                    "SELECT holding_unavailable_count FROM backtest_summaries WHERE id = 1"
                ).fetchone()[0]

            self.assertIn("holding_action_evaluable", result_columns)
            self.assertIn("holding_decision_correct", result_columns)
            self.assertIn("holding_evaluable_count", summary_columns)
            self.assertIn("holding_decision_accuracy_pct", summary_columns)
            self.assertEqual(holding_unavailable_count, 7)
        finally:
            DatabaseManager.reset_instance()
            Config.reset_instance()
            temp_dir.cleanup()

    def test_legacy_paper_sync_links_rebuild_removes_refreshable_position_fk(self):
        DatabaseManager.reset_instance()
        Config.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "legacy_paper_sync_fk.db")

        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute("CREATE TABLE portfolio_positions (id INTEGER PRIMARY KEY)")
                conn.execute("INSERT INTO portfolio_positions (id) VALUES (99)")
                conn.execute(
                    """CREATE TABLE paper_sync_links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    idempotency_key VARCHAR(160) NOT NULL,
                    payload_hash VARCHAR(64) NOT NULL,
                    direction VARCHAR(32) NOT NULL,
                    status VARCHAR(32) NOT NULL DEFAULT 'synced',
                    source_model VARCHAR(255) NOT NULL,
                    market VARCHAR(8) NOT NULL,
                    stock_code VARCHAR(16) NOT NULL,
                    portfolio_account_id INTEGER REFERENCES portfolio_accounts(id),
                    portfolio_position_id INTEGER REFERENCES portfolio_positions(id),
                    portfolio_trade_id INTEGER REFERENCES portfolio_trades(id),
                    paper_plan_id INTEGER REFERENCES paper_execution_plans(id),
                    paper_position_id INTEGER REFERENCES paper_positions(id),
                    paper_event_id INTEGER REFERENCES paper_trade_events(id),
                    quantity FLOAT NOT NULL,
                    price FLOAT NOT NULL,
                    fee FLOAT NOT NULL DEFAULT 0,
                    trade_date DATE NOT NULL,
                    origin VARCHAR(32) NOT NULL,
                    details_json TEXT NOT NULL DEFAULT '{}',
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )"""
                )
                conn.execute(
                    "CREATE UNIQUE INDEX ix_paper_sync_links_idempotency_key "
                    "ON paper_sync_links(idempotency_key)"
                )
                conn.execute(
                    "CREATE INDEX ix_paper_sync_identity_direction "
                    "ON paper_sync_links(source_model, market, stock_code, direction)"
                )
                conn.execute(
                    "CREATE INDEX ix_paper_sync_portfolio_position "
                    "ON paper_sync_links(portfolio_position_id, direction)"
                )
                conn.execute(
                    """INSERT INTO paper_sync_links (
                    id,
                    idempotency_key,
                    payload_hash,
                    direction,
                    status,
                    source_model,
                    market,
                    stock_code,
                    portfolio_position_id,
                    quantity,
                    price,
                    fee,
                    trade_date,
                    origin,
                    details_json,
                    created_at,
                    updated_at
                ) VALUES (
                    1,
                    'import-1',
                    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                    'portfolio_to_paper',
                    'synced',
                    'model-a',
                    'cn',
                    '600519',
                    99,
                    100,
                    101,
                    0,
                    '2026-08-24',
                    'portfolio_import',
                    '{"note":"keep"}',
                    '2026-08-24 09:30:00',
                    '2026-08-24 09:30:00'
                )"""
                )

            self.assertIn(
                ("portfolio_positions", "portfolio_position_id", "id"),
                self._list_sqlite_foreign_keys(db_path, "paper_sync_links"),
            )

            DatabaseManager(db_url=f"sqlite:///{db_path}")
            DatabaseManager.reset_instance()
            DatabaseManager(db_url=f"sqlite:///{db_path}")

            foreign_keys = self._list_sqlite_foreign_keys(db_path, "paper_sync_links")
            indexes = self._list_sqlite_indexes(db_path, "paper_sync_links")
            unique_indexes = self._list_sqlite_unique_indexes(db_path, "paper_sync_links")
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT id, idempotency_key, portfolio_position_id, details_json "
                    "FROM paper_sync_links"
                ).fetchone()
                temp_tables = conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name LIKE 'paper_sync_links_recreate_tmp_%'"
                ).fetchall()

            self.assertNotIn(
                ("portfolio_positions", "portfolio_position_id", "id"),
                foreign_keys,
            )
            self.assertIn(("portfolio_accounts", "portfolio_account_id", "id"), foreign_keys)
            self.assertIn(("paper_positions", "paper_position_id", "id"), foreign_keys)
            self.assertEqual(row, (1, "import-1", 99, '{"note":"keep"}'))
            self.assertTrue(
                any(columns == ["idempotency_key"] for columns in unique_indexes.values())
            )
            self.assertEqual(
                indexes.get("ix_paper_sync_identity_direction"),
                ["source_model", "market", "stock_code", "direction"],
            )
            self.assertEqual(
                indexes.get("ix_paper_sync_portfolio_position"),
                ["portfolio_position_id", "direction"],
            )
            self.assertEqual(temp_tables, [])
        finally:
            DatabaseManager.reset_instance()
            Config.reset_instance()
            temp_dir.cleanup()

    def test_fresh_decision_signal_schema_has_profile_indexes(self):
        DatabaseManager.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "fresh_decision_profile.db")

        try:
            DatabaseManager(db_url=f"sqlite:///{db_path}")

            indexes = self._list_sqlite_indexes(db_path, "decision_signals")
            self.assertEqual(
                indexes.get("ix_decision_signals_decision_profile"),
                ["decision_profile"],
            )
            self.assertEqual(
                indexes.get("ix_decision_signal_market_stock_profile_created"),
                ["market", "stock_code", "decision_profile", "created_at"],
            )
            self.assertEqual(
                indexes.get(
                    "ix_decision_signal_report_type_market_stock_profile_action_horizon_phase"
                ),
                [
                    "source_report_id", "source_type", "market", "stock_code",
                    "decision_profile", "action", "horizon", "market_phase",
                ],
            )
            self.assertEqual(
                indexes.get(
                    "ix_decision_signal_trace_type_market_stock_profile_action_horizon_phase"
                ),
                [
                    "trace_id", "source_type", "market", "stock_code",
                    "decision_profile", "action", "horizon", "market_phase",
                ],
            )
        finally:
            DatabaseManager.reset_instance()
            Config.reset_instance()
            temp_dir.cleanup()

    def test_decision_signal_profile_migration_adds_column_indexes_and_closed_stats(self):
        DatabaseManager.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "legacy_decision_profile.db")
        deeply_nested_json = "[" * 10_000 + "]" * 10_000

        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """CREATE TABLE decision_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT,
                    market TEXT,
                    source_type TEXT,
                    source_report_id INTEGER,
                    trace_id TEXT,
                    action TEXT,
                    horizon TEXT,
                    market_phase TEXT,
                    created_at DATETIME,
                    metadata_json TEXT
                )"""
                )
                conn.execute(
                    "CREATE INDEX ix_decision_signal_report_type_market_stock_action_horizon_phase "
                    "ON decision_signals "
                    "(source_report_id, source_type, market, stock_code, action, horizon, market_phase)"
                )
                conn.execute(
                    "CREATE INDEX ix_decision_signal_trace_type_market_stock_action_horizon_phase "
                    "ON decision_signals "
                    "(trace_id, source_type, market, stock_code, action, horizon, market_phase)"
                )
                conn.executemany(
                    """INSERT INTO decision_signals (
                    stock_code,
                    market,
                    source_type,
                    source_report_id,
                    trace_id,
                    action,
                    horizon,
                    market_phase,
                    created_at,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        ("600519", "cn", "analysis", 1, "trace-1", "buy", "3d", "intraday", "2026-01-01", '{"decision_profile":"balanced"}'),
                        ("600519", "cn", "analysis", 2, "trace-2", "buy", "3d", "intraday", "2026-01-01", None),
                        ("600519", "cn", "analysis", 3, "trace-3", "buy", "3d", "intraday", "2026-01-01", ""),
                        ("600519", "cn", "analysis", 4, "trace-4", "buy", "3d", "intraday", "2026-01-01", "   "),
                        ("600519", "cn", "analysis", 5, "trace-5", "buy", "3d", "intraday", "2026-01-01", "{not-json"),
                        ("600519", "cn", "analysis", 6, "trace-6", "buy", "3d", "intraday", "2026-01-01", sqlite3.Binary(b"\xff")),
                        ("600519", "cn", "analysis", 7, "trace-7", "buy", "3d", "intraday", "2026-01-01", "null"),
                        ("600519", "cn", "analysis", 8, "trace-8", "buy", "3d", "intraday", "2026-01-01", "[]"),
                        ("600519", "cn", "analysis", 9, "trace-9", "buy", "3d", "intraday", "2026-01-01", '"balanced"'),
                        ("600519", "cn", "analysis", 10, "trace-10", "buy", "3d", "intraday", "2026-01-01", "1"),
                        ("600519", "cn", "analysis", 11, "trace-11", "buy", "3d", "intraday", "2026-01-01", "{}"),
                        ("600519", "cn", "analysis", 12, "trace-12", "buy", "3d", "intraday", "2026-01-01", '{"decision_profile":null}'),
                        ("600519", "cn", "analysis", 13, "trace-13", "buy", "3d", "intraday", "2026-01-01", '{"decision_profile":""}'),
                        ("600519", "cn", "analysis", 14, "trace-14", "buy", "3d", "intraday", "2026-01-01", '{"decision_profile":"   "}'),
                        ("600519", "cn", "analysis", 15, "trace-15", "buy", "3d", "intraday", "2026-01-01", '{"decision_profile":"reckless"}'),
                        ("600519", "cn", "analysis", 16, "trace-16", "buy", "3d", "intraday", "2026-01-01", deeply_nested_json),
                    ],
                )

            with self.assertLogs("src.storage", level="INFO") as logs:
                DatabaseManager(db_url=f"sqlite:///{db_path}")

            with sqlite3.connect(db_path) as conn:
                columns = {row[1] for row in conn.execute("PRAGMA table_info(decision_signals)").fetchall()}
                rows = conn.execute(
                    "SELECT id, decision_profile FROM decision_signals ORDER BY id"
                ).fetchall()

            self.assertIn("decision_profile", columns)
            self.assertEqual(rows[0], (1, "balanced"))
            self.assertTrue(all(profile is None for _, profile in rows[1:]))

            indexes = self._list_sqlite_indexes(db_path, "decision_signals")
            expected_indexes = {
                "ix_decision_signals_decision_profile": ["decision_profile"],
                "ix_decision_signal_market_stock_profile_created": [
                    "market", "stock_code", "decision_profile", "created_at",
                ],
                "ix_decision_signal_report_type_market_stock_profile_action_horizon_phase": [
                    "source_report_id", "source_type", "market", "stock_code",
                    "decision_profile", "action", "horizon", "market_phase",
                ],
                "ix_decision_signal_trace_type_market_stock_profile_action_horizon_phase": [
                    "trace_id", "source_type", "market", "stock_code",
                    "decision_profile", "action", "horizon", "market_phase",
                ],
            }
            for index_name, index_columns in expected_indexes.items():
                self.assertEqual(indexes.get(index_name), index_columns)
            self.assertEqual(
                indexes.get("ix_decision_signal_report_type_market_stock_action_horizon_phase"),
                [
                    "source_report_id", "source_type", "market", "stock_code",
                    "action", "horizon", "market_phase",
                ],
            )
            self.assertEqual(
                indexes.get("ix_decision_signal_trace_type_market_stock_action_horizon_phase"),
                [
                    "trace_id", "source_type", "market", "stock_code",
                    "action", "horizon", "market_phase",
                ],
            )

            log_text = "\n".join(logs.output)
            self.assertIn("candidate_count=16", log_text)
            self.assertIn("backfilled_count=1", log_text)
            self.assertIn("guard_skipped_count=0", log_text)
            self.assertIn("missing_metadata_count=1", log_text)
            self.assertIn("missing_profile_count=4", log_text)
            # CPython's JSON decoder may either reject the pathological deep
            # nesting with RecursionError or decode it as a non-object. Both
            # paths are safely skipped by the migration.
            self.assertTrue(
                ("invalid_json_count=5" in log_text and "non_object_count=4" in log_text)
                or ("invalid_json_count=4" in log_text and "non_object_count=5" in log_text)
            )
            self.assertIn("invalid_profile_count=1", log_text)
            self.assertIn("skipped_existing_profile_count=0", log_text)

            DatabaseManager.reset_instance()
            with self.assertLogs("src.storage", level="INFO") as second_logs:
                DatabaseManager(db_url=f"sqlite:///{db_path}")
            second_log_text = "\n".join(second_logs.output)
            self.assertIn("candidate_count=15", second_log_text)
            self.assertIn("backfilled_count=0", second_log_text)
            self.assertIn("guard_skipped_count=0", second_log_text)
            self.assertIn("missing_metadata_count=1", second_log_text)
            self.assertIn("missing_profile_count=4", second_log_text)
            self.assertTrue(
                (
                    "invalid_json_count=5" in second_log_text
                    and "non_object_count=4" in second_log_text
                )
                or (
                    "invalid_json_count=4" in second_log_text
                    and "non_object_count=5" in second_log_text
                )
            )
            self.assertIn("invalid_profile_count=1", second_log_text)
            self.assertIn("skipped_existing_profile_count=1", second_log_text)
        finally:
            DatabaseManager.reset_instance()
            Config.reset_instance()
            temp_dir.cleanup()

    def test_decision_signal_profile_migration_runs_when_column_already_exists(self):
        DatabaseManager.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "existing_decision_profile.db")

        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """CREATE TABLE decision_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_code TEXT,
                    market TEXT,
                    source_type TEXT,
                    source_report_id INTEGER,
                    trace_id TEXT,
                    action TEXT,
                    horizon TEXT,
                    market_phase TEXT,
                    created_at DATETIME,
                    metadata_json TEXT,
                    decision_profile VARCHAR(16)
                )"""
                )
                conn.executemany(
                    "INSERT INTO decision_signals (metadata_json, decision_profile) VALUES (?, ?)",
                    [
                        ('{"decision_profile":"aggressive"}', None),
                        (None, None),
                        ('{"decision_profile":"balanced"}', "conservative"),
                        ('{"decision_profile":"balanced"}', ""),
                    ],
                )

            with self.assertLogs("src.storage", level="INFO") as logs:
                DatabaseManager(db_url=f"sqlite:///{db_path}")

            with sqlite3.connect(db_path) as conn:
                profiles = conn.execute(
                    "SELECT decision_profile FROM decision_signals ORDER BY id"
                ).fetchall()

            self.assertEqual(
                profiles,
                [("aggressive",), (None,), ("conservative",), ("",)],
            )
            log_text = "\n".join(logs.output)
            self.assertIn("candidate_count=2", log_text)
            self.assertIn("backfilled_count=1", log_text)
            self.assertIn("missing_metadata_count=1", log_text)
            self.assertIn("skipped_existing_profile_count=2", log_text)
        finally:
            DatabaseManager.reset_instance()
            Config.reset_instance()
            temp_dir.cleanup()

    def test_decision_signal_profile_migration_fails_when_column_inspection_fails(self):
        class BrokenInspector:
            def has_table(self, _table_name: str) -> bool:
                return True

            def get_columns(self, _table_name: str):
                raise RuntimeError("inspection failed")

        DatabaseManager.reset_instance()
        try:
            with patch("src.storage.inspect", return_value=BrokenInspector()):
                with self.assertLogs("src.storage", level="ERROR") as logs:
                    with self.assertRaises(RuntimeError):
                        DatabaseManager(db_url="sqlite:///:memory:")

            self.assertIn(
                "profile migration cannot continue safely",
                "\n".join(logs.output),
            )
        finally:
            DatabaseManager.reset_instance()
            Config.reset_instance()

    def test_schema_migration_record_handles_concurrent_initialization(self):
        DatabaseManager.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "schema_migration_race.db")
        db = DatabaseManager(db_url=f"sqlite:///{db_path}")
        worker_count = 8
        barrier = threading.Barrier(worker_count)
        errors = []
        state_lock = threading.Lock()

        with db.get_session() as session:
            session.query(DatabaseSchemaMigration).delete()
            session.commit()

        def ensure_record() -> None:
            try:
                barrier.wait(timeout=5)
                db._ensure_schema_migration_record()
            except Exception as exc:
                with state_lock:
                    errors.append(exc)

        threads = [threading.Thread(target=ensure_record) for _ in range(worker_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        with db.get_session() as session:
            rows = session.execute(select(DatabaseSchemaMigration)).scalars().all()

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].version, CURRENT_SCHEMA_VERSION)

        DatabaseManager.reset_instance()
        temp_dir.cleanup()
    
    def test_parse_sniper_value(self):
        """测试解析狙击点位数值"""
        
        # 1. 正常数值
        self.assertEqual(DatabaseManager._parse_sniper_value(100), 100.0)
        self.assertEqual(DatabaseManager._parse_sniper_value(100.5), 100.5)
        self.assertEqual(DatabaseManager._parse_sniper_value("100"), 100.0)
        self.assertEqual(DatabaseManager._parse_sniper_value("100.5"), 100.5)
        
        # 2. 包含中文描述和"元"
        self.assertEqual(DatabaseManager._parse_sniper_value("建议在 100 元附近买入"), 100.0)
        self.assertEqual(DatabaseManager._parse_sniper_value("价格：100.5元"), 100.5)
        
        # 3. 包含干扰数字（修复的Bug场景）
        # 之前 "MA5" 会被错误提取为 5.0，现在应该提取 "元" 前面的 100
        text_bug = "无法给出。需等待MA5数据恢复，在股价回踩MA5且乖离率<2%时考虑100元"
        self.assertEqual(DatabaseManager._parse_sniper_value(text_bug), 100.0)
        
        # 4. 更多干扰场景
        text_complex = "MA10为20.5，建议在30元买入"
        self.assertEqual(DatabaseManager._parse_sniper_value(text_complex), 30.0)
        
        text_multiple = "支撑位10元，阻力位20元" # 应该提取最后一个"元"前面的数字，即20，或者更复杂的逻辑？
        # 当前逻辑是找最后一个冒号，然后找之后的第一个"元"，提取中间的数字。
        # 测试没有冒号的情况
        self.assertEqual(DatabaseManager._parse_sniper_value("30元"), 30.0)
        
        # 测试多个数字在"元"之前
        self.assertEqual(DatabaseManager._parse_sniper_value("MA5 10 20元"), 20.0)
        
        # 5. Fallback: no "元" character — extracts last non-MA number
        self.assertEqual(DatabaseManager._parse_sniper_value("102.10-103.00（MA5附近）"), 103.0)
        self.assertEqual(DatabaseManager._parse_sniper_value("97.62-98.50（MA10附近）"), 98.5)
        self.assertEqual(DatabaseManager._parse_sniper_value("93.40下方（MA20支撑）"), 93.4)
        self.assertEqual(DatabaseManager._parse_sniper_value("108.00-110.00（前期高点阻力）"), 110.0)

        # 6. 无效输入
        self.assertIsNone(DatabaseManager._parse_sniper_value(None))
        self.assertIsNone(DatabaseManager._parse_sniper_value(""))
        self.assertIsNone(DatabaseManager._parse_sniper_value("没有数字"))
        self.assertIsNone(DatabaseManager._parse_sniper_value("MA5但没有元"))

        # 7. 回归：括号内技术指标数字不应被提取
        self.assertNotEqual(DatabaseManager._parse_sniper_value("1.52-1.53 (回踩MA5/10附近)"), 10.0)
        self.assertNotEqual(DatabaseManager._parse_sniper_value("1.55-1.56(MA5/M20支撑)"), 20.0)
        self.assertNotEqual(DatabaseManager._parse_sniper_value("1.49-1.50(MA60附近企稳)"), 60.0)
        # 验证正确值在区间内
        self.assertIn(DatabaseManager._parse_sniper_value("1.52-1.53 (回踩MA5/10附近)"), [1.52, 1.53])
        self.assertIn(DatabaseManager._parse_sniper_value("1.55-1.56(MA5/M20支撑)"), [1.55, 1.56])
        self.assertIn(DatabaseManager._parse_sniper_value("1.49-1.50(MA60附近企稳)"), [1.49, 1.50])

    def test_get_chat_sessions_prefix_is_scoped_by_colon_boundary(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")

        db.save_conversation_message("telegram_12345:chat", "user", "first user")
        db.save_conversation_message("telegram_123456:chat", "user", "second user")

        sessions = db.get_chat_sessions(session_prefix="telegram_12345")

        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0]["session_id"], "telegram_12345:chat")

        DatabaseManager.reset_instance()

    def test_get_chat_sessions_can_include_legacy_exact_session_id(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")

        db.save_conversation_message("feishu_u1", "user", "legacy chat")
        db.save_conversation_message("feishu_u1:ask_600519", "user", "ask session")

        sessions = db.get_chat_sessions(
            session_prefix="feishu_u1:",
            extra_session_ids=["feishu_u1"],
        )

        self.assertEqual({item["session_id"] for item in sessions}, {"feishu_u1", "feishu_u1:ask_600519"})

        DatabaseManager.reset_instance()

    def test_conversation_summary_upsert_and_delete_with_session(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")

        db.save_conversation_message("summary-session", "user", "hello")
        db.upsert_conversation_summary(
            "summary-session",
            "first summary",
            covered_message_id=1,
            source_message_count=1,
            estimated_tokens=10,
        )
        db.upsert_conversation_summary(
            "summary-session",
            "updated summary",
            covered_message_id=2,
            source_message_count=2,
            estimated_tokens=12,
        )

        summary = db.get_conversation_summary("summary-session")
        self.assertIsNotNone(summary)
        self.assertEqual(summary["summary"], "updated summary")
        self.assertEqual(summary["covered_message_id"], 2)
        self.assertEqual(summary["source_message_count"], 2)

        deleted = db.delete_conversation_session("summary-session")

        self.assertEqual(deleted, 1)
        self.assertIsNone(db.get_conversation_summary("summary-session"))

        DatabaseManager.reset_instance()

    def test_conversation_message_save_returns_id(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")

        message_id = db.save_conversation_message("message-id-session", "user", "hello")

        self.assertIsInstance(message_id, int)
        self.assertGreater(message_id, 0)

        DatabaseManager.reset_instance()

    def test_provider_turn_round_trip_preserves_protocol_fields_and_flags(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")
        user_id = db.save_conversation_message("trace-session", "user", "question")
        assistant_id = db.save_conversation_message("trace-session", "assistant", "final")
        trace_messages = [
            {
                "role": "assistant",
                "content": "checking",
                "reasoning_content": "reasoning",
                "tool_calls": [
                    {
                        "id": "call_1",
                        "name": "echo",
                        "arguments": {"message": "hello"},
                        "provider_specific_fields": {"thought_signature": "sig"},
                    }
                ],
            },
            {"role": "tool", "tool_call_id": "call_1", "content": "{\"ok\": true}"},
        ]

        turn_id = db.save_agent_provider_turn(
            session_id="trace-session",
            run_id="run-1",
            provider="deepseek",
            model="deepseek/deepseek-chat",
            anchor_user_message_id=user_id,
            anchor_assistant_message_id=assistant_id,
            messages=trace_messages,
            contains_reasoning=True,
            contains_tool_calls=True,
            contains_thinking_blocks=False,
            must_roundtrip=True,
            estimated_tokens=42,
        )
        rows = db.get_agent_provider_turns("trace-session")

        self.assertIsInstance(turn_id, int)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["messages"], trace_messages)
        self.assertTrue(rows[0]["contains_reasoning"])
        self.assertTrue(rows[0]["contains_tool_calls"])
        self.assertTrue(rows[0]["must_roundtrip"])
        self.assertEqual(rows[0]["estimated_tokens"], 42)

        DatabaseManager.reset_instance()

    def test_provider_turns_do_not_appear_in_visible_or_web_messages_and_delete_with_session(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")
        user_id = db.save_conversation_message("trace-hidden", "user", "visible question")
        assistant_id = db.save_conversation_message("trace-hidden", "assistant", "visible answer")
        db.save_agent_provider_turn(
            session_id="trace-hidden",
            run_id="run-hidden",
            provider="deepseek",
            model="deepseek/deepseek-chat",
            anchor_user_message_id=user_id,
            anchor_assistant_message_id=assistant_id,
            messages=[{"role": "assistant", "reasoning_content": "SECRET_REASONING", "tool_calls": []}],
            contains_reasoning=True,
            contains_tool_calls=True,
            contains_thinking_blocks=False,
            must_roundtrip=True,
            estimated_tokens=5,
        )

        self.assertEqual(
            [(m["role"], m["content"]) for m in db.get_visible_conversation_messages("trace-hidden")],
            [("user", "visible question"), ("assistant", "visible answer")],
        )
        self.assertEqual(
            [(m["role"], m["content"]) for m in db.get_conversation_history("trace-hidden")],
            [("user", "visible question"), ("assistant", "visible answer")],
        )
        self.assertEqual(
            [(m["role"], m["content"]) for m in db.get_conversation_messages("trace-hidden")],
            [("user", "visible question"), ("assistant", "visible answer")],
        )

        deleted = db.delete_conversation_session("trace-hidden")

        self.assertEqual(deleted, 2)
        self.assertEqual(db.get_agent_provider_turns("trace-hidden"), [])

        DatabaseManager.reset_instance()

    def test_provider_turn_retention_is_bucketed_by_session_provider_model(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")
        for idx in range(5):
            user_id = db.save_conversation_message("retention", "user", f"q{idx}")
            assistant_id = db.save_conversation_message("retention", "assistant", f"a{idx}")
            db.save_agent_provider_turn(
                session_id="retention",
                run_id=f"run-{idx}",
                provider="deepseek",
                model="deepseek/deepseek-chat",
                anchor_user_message_id=user_id,
                anchor_assistant_message_id=assistant_id,
                messages=[{"role": "assistant", "reasoning_content": f"r{idx}", "tool_calls": [{"id": f"c{idx}", "name": "echo", "arguments": {}}]}],
                contains_reasoning=True,
                contains_tool_calls=True,
                contains_thinking_blocks=False,
                must_roundtrip=True,
                estimated_tokens=idx + 1,
            )
        user_id = db.save_conversation_message("retention", "user", "other")
        assistant_id = db.save_conversation_message("retention", "assistant", "other")
        db.save_agent_provider_turn(
            session_id="retention",
            run_id="run-other",
            provider="anthropic",
            model="anthropic/claude-test",
            anchor_user_message_id=user_id,
            anchor_assistant_message_id=assistant_id,
            messages=[{"role": "assistant", "provider_blocks": [{"type": "thinking"}], "tool_calls": [{"id": "c-other", "name": "echo", "arguments": {}}]}],
            contains_reasoning=False,
            contains_tool_calls=True,
            contains_thinking_blocks=True,
            must_roundtrip=True,
            estimated_tokens=1,
        )

        deepseek_rows = db.get_agent_provider_turns(
            "retention",
            provider="deepseek",
            model="deepseek/deepseek-chat",
        )
        anthropic_rows = db.get_agent_provider_turns(
            "retention",
            provider="anthropic",
            model="anthropic/claude-test",
        )

        self.assertEqual(len(deepseek_rows), 3)
        self.assertEqual([row["run_id"] for row in deepseek_rows], ["run-2", "run-3", "run-4"])
        self.assertEqual(len(anthropic_rows), 1)

        DatabaseManager.reset_instance()

    def test_get_visible_conversation_messages_returns_ordered_visible_content(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")

        db.save_conversation_message("visible-session", "system", "hidden")
        db.save_conversation_message("visible-session", "user", "question")
        db.save_conversation_message("visible-session", "assistant", "answer")

        messages = db.get_visible_conversation_messages("visible-session")

        self.assertEqual(
            [(item["role"], item["content"]) for item in messages],
            [("user", "question"), ("assistant", "answer")],
        )
        self.assertIsInstance(messages[0]["id"], int)

        DatabaseManager.reset_instance()

    def test_get_visible_conversation_messages_limit_returns_ordered_tail(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")

        for idx in range(25):
            db.save_conversation_message("visible-limit", "user", f"msg-{idx}")

        messages = db.get_visible_conversation_messages("visible-limit", limit=20)

        self.assertEqual(len(messages), 20)
        self.assertEqual(messages[0]["content"], "msg-5")
        self.assertEqual(messages[-1]["content"], "msg-24")

        DatabaseManager.reset_instance()

    def test_file_sqlite_enables_wal_and_busy_timeout(self):
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "sqlite_pragmas.db")
        original_env = {
            "DATABASE_PATH": os.environ.get("DATABASE_PATH"),
            "SQLITE_BUSY_TIMEOUT_MS": os.environ.get("SQLITE_BUSY_TIMEOUT_MS"),
            "SQLITE_WAL_ENABLED": os.environ.get("SQLITE_WAL_ENABLED"),
        }

        try:
            os.environ["DATABASE_PATH"] = db_path
            os.environ["SQLITE_BUSY_TIMEOUT_MS"] = "1234"
            os.environ["SQLITE_WAL_ENABLED"] = "true"
            Config.reset_instance()
            DatabaseManager.reset_instance()

            db = DatabaseManager.get_instance()
            with db.get_session() as session:
                journal_mode = session.connection().exec_driver_sql("PRAGMA journal_mode").scalar()
                busy_timeout = session.connection().exec_driver_sql("PRAGMA busy_timeout").scalar()

            self.assertEqual(str(journal_mode).lower(), "wal")
            self.assertEqual(int(busy_timeout), 1234)
        finally:
            DatabaseManager.reset_instance()
            Config.reset_instance()
            for key, value in original_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            temp_dir.cleanup()

    def test_get_instance_waits_for_cold_start_initialization(self):
        DatabaseManager.reset_instance()
        Config.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "sqlite_cold_start.db")
        original_database_path = os.environ.get("DATABASE_PATH")
        create_all_entered = threading.Event()
        competitor_entered = threading.Event()
        release_create_all = threading.Event()
        competitor_done = threading.Event()
        state_lock = threading.Lock()
        init_errors = []
        competitor_errors = []
        instances = []
        query_values = []
        original_create_all = Base.metadata.create_all

        def delayed_create_all(bind, *args, **kwargs):
            create_all_entered.set()
            if not release_create_all.wait(timeout=5):
                raise TimeoutError("Timed out waiting to release create_all")
            return original_create_all(bind, *args, **kwargs)

        def initialize_manager() -> None:
            try:
                db = DatabaseManager.get_instance()
                with state_lock:
                    instances.append(db)
            except Exception as exc:
                with state_lock:
                    init_errors.append(exc)

        def use_manager() -> None:
            try:
                competitor_entered.set()
                db = DatabaseManager.get_instance()
                session = db.get_session()
                try:
                    value = session.connection().exec_driver_sql("SELECT 1").scalar()
                finally:
                    session.close()
                with state_lock:
                    instances.append(db)
                    query_values.append(value)
            except Exception as exc:
                with state_lock:
                    competitor_errors.append(exc)
            finally:
                competitor_done.set()

        try:
            os.environ["DATABASE_PATH"] = db_path
            Config.reset_instance()
            with patch.object(Base.metadata, "create_all", side_effect=delayed_create_all):
                init_thread = threading.Thread(target=initialize_manager)
                competitor_thread = threading.Thread(target=use_manager)

                init_thread.start()
                self.assertTrue(create_all_entered.wait(timeout=5))

                competitor_thread.start()
                self.assertTrue(competitor_entered.wait(timeout=5))
                self.assertFalse(
                    competitor_done.wait(timeout=0.2),
                    "DatabaseManager.get_instance() returned before initialization completed",
                )

                release_create_all.set()
                init_thread.join(timeout=5)
                competitor_thread.join(timeout=5)

                self.assertFalse(init_thread.is_alive())
                self.assertFalse(competitor_thread.is_alive())

            self.assertEqual(init_errors, [])
            self.assertEqual(competitor_errors, [])
            self.assertEqual(query_values, [1])
            self.assertEqual(len({id(instance) for instance in instances}), 1)
        finally:
            release_create_all.set()
            DatabaseManager.reset_instance()
            Config.reset_instance()
            if original_database_path is None:
                os.environ.pop("DATABASE_PATH", None)
            else:
                os.environ["DATABASE_PATH"] = original_database_path
            temp_dir.cleanup()

    def test_direct_construction_serializes_before_get_instance(self):
        DatabaseManager.reset_instance()
        Config.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        direct_db_path = os.path.join(temp_dir.name, "direct.db")
        env_db_path = os.path.join(temp_dir.name, "env.db")
        direct_db_url = f"sqlite:///{direct_db_path}"
        original_database_path = os.environ.get("DATABASE_PATH")
        direct_init_entered = threading.Event()
        competitor_entered = threading.Event()
        allow_direct_init = threading.Event()
        competitor_done = threading.Event()
        state_lock = threading.Lock()
        errors = []
        instances = []
        query_values = []
        original_init = DatabaseManager.__init__

        def delayed_direct_init(self, db_url=None):
            if db_url == direct_db_url:
                direct_init_entered.set()
                if not competitor_entered.wait(timeout=5):
                    raise TimeoutError("Timed out waiting for competitor")
                if not allow_direct_init.wait(timeout=5):
                    raise TimeoutError("Timed out waiting to initialize direct instance")
            return original_init(self, db_url=db_url)

        def construct_directly() -> None:
            try:
                db = DatabaseManager(db_url=direct_db_url)
                with state_lock:
                    instances.append(db)
            except Exception as exc:
                with state_lock:
                    errors.append(exc)

        def use_get_instance() -> None:
            try:
                competitor_entered.set()
                db = DatabaseManager.get_instance()
                session = db.get_session()
                try:
                    value = session.connection().exec_driver_sql("SELECT 1").scalar()
                finally:
                    session.close()
                with state_lock:
                    instances.append(db)
                    query_values.append(value)
            except Exception as exc:
                with state_lock:
                    errors.append(exc)
            finally:
                competitor_done.set()

        try:
            os.environ["DATABASE_PATH"] = env_db_path
            Config.reset_instance()
            with patch.object(DatabaseManager, "__init__", new=delayed_direct_init):
                direct_thread = threading.Thread(target=construct_directly)
                competitor_thread = threading.Thread(target=use_get_instance)

                direct_thread.start()
                self.assertTrue(direct_init_entered.wait(timeout=5))

                competitor_thread.start()
                self.assertTrue(competitor_entered.wait(timeout=5))
                self.assertFalse(
                    competitor_done.wait(timeout=0.2),
                    "get_instance() should not initialize over an in-flight direct construction",
                )

                allow_direct_init.set()
                direct_thread.join(timeout=5)
                competitor_thread.join(timeout=5)

                self.assertFalse(direct_thread.is_alive())
                self.assertFalse(competitor_thread.is_alive())

            self.assertEqual(errors, [])
            self.assertEqual(query_values, [1])
            self.assertEqual(len({id(instance) for instance in instances}), 1)
            self.assertEqual(DatabaseManager._instance._db_url, direct_db_url)
        finally:
            allow_direct_init.set()
            DatabaseManager.reset_instance()
            Config.reset_instance()
            if original_database_path is None:
                os.environ.pop("DATABASE_PATH", None)
            else:
                os.environ["DATABASE_PATH"] = original_database_path
            temp_dir.cleanup()

    def test_init_cleanup_preserves_original_initialization_error(self):
        DatabaseManager.reset_instance()
        original_error = RuntimeError("create all failed")
        cleanup_error = RuntimeError("dispose failed")

        def create_engine_with_failing_dispose(*args, **kwargs):
            engine = sqlalchemy_create_engine(*args, **kwargs)

            def failing_dispose() -> None:
                raise cleanup_error

            engine.dispose = failing_dispose
            return engine

        try:
            with patch("src.storage.create_engine", side_effect=create_engine_with_failing_dispose):
                with patch.object(Base.metadata, "create_all", side_effect=original_error):
                    with self.assertRaisesRegex(RuntimeError, "create all failed") as ctx:
                        DatabaseManager.get_instance()

            self.assertIs(ctx.exception, original_error)
            self.assertIsNone(DatabaseManager._instance)
        finally:
            DatabaseManager.reset_instance()

    def test_sqlite_write_transactions_begin_immediate(self):
        DatabaseManager.reset_instance()
        db = DatabaseManager(db_url="sqlite:///:memory:")
        session = db.get_session()
        connection = session.connection()

        try:
            with patch.object(db, "get_session", return_value=session):
                with patch.object(connection, "exec_driver_sql", wraps=connection.exec_driver_sql) as mock_exec:
                    result = db._run_write_transaction("unit-test", lambda current_session: 7)

            self.assertEqual(result, 7)
            self.assertTrue(
                any(call.args == ("BEGIN IMMEDIATE",) for call in mock_exec.call_args_list)
            )
        finally:
            DatabaseManager.reset_instance()

    def test_save_daily_data_sqlite_concurrent_same_code_date_counts_only_new_rows(self):
        DatabaseManager.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "sqlite_daily_concurrency.db")
        db = DatabaseManager(db_url=f"sqlite:///{db_path}")

        results = []
        results_lock = threading.Lock()
        start_barrier = threading.Barrier(2)

        def worker() -> None:
            start_barrier.wait()
            count = db.save_daily_data(
                pd.DataFrame(
                    [
                        {
                            'date': date(2026, 4, 1),
                            'open': 10,
                            'high': 11,
                            'low': 9,
                            'close': 10.5,
                            'volume': 100,
                            'amount': 1050,
                            'pct_chg': 1.2,
                            'ma5': 10.1,
                            'ma10': 10.2,
                            'ma20': 10.3,
                            'volume_ratio': 1.0,
                        }
                    ]
                ),
                code='600519',
                data_source='test',
            )
            with results_lock:
                results.append(count)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        try:
            self.assertCountEqual(results, [1, 0])

            with db.get_session() as session:
                total = session.execute(
                    select(func.count()).select_from(StockDaily).where(
                        and_(
                            StockDaily.code == '600519',
                            StockDaily.date == date(2026, 4, 1),
                        )
                    )
                ).scalar()

            self.assertEqual(total, 1)
        finally:
            temp_dir.cleanup()
            DatabaseManager.reset_instance()

    @staticmethod
    def _intent_result(code: str = "600519"):
        from src.analyzer import AnalysisResult

        return AnalysisResult(
            code=code,
            name="贵州茅台",
            sentiment_score=70,
            trend_prediction="看多",
            operation_advice="买入",
        )

    def test_report_execution_intent_is_frozen_with_first_report_save(self) -> None:
        DatabaseManager.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        try:
            db = DatabaseManager(db_url=f"sqlite:///{os.path.join(temp_dir.name, 'intent.db')}")
            report_id = db.save_analysis_history(
                result=self._intent_result(),
                query_id="intent-save",
                report_type="detailed",
                news_content=None,
                execution_intent={
                    "execution_intent": "research_only",
                    "execution_scope": "experiment:exp1:a",
                    "decision_as_of": "2026-09-25",
                    "intent_source": "unit_test",
                },
            )
            self.assertGreater(report_id, 0)
            intent = db.get_report_execution_intent(report_id)
            self.assertEqual(
                (
                    intent.execution_intent,
                    intent.execution_scope,
                    intent.decision_as_of,
                    intent.execution_policy_version,
                    intent.intent_source,
                ),
                ("research_only", "experiment:exp1:a", date(2026, 9, 25), "report-execution-intent-v1", "unit_test"),
            )

            with self.assertRaisesRegex(ValueError, "immutable"):
                with db.session_scope() as session:
                    session.get(AnalysisReportExecutionIntent, intent.id).execution_intent = "paper_candidate"
            self.assertEqual(db.get_report_execution_intent(report_id).execution_intent, "research_only")

            self.assertEqual(db.delete_analysis_history_records([report_id]), 1)
            self.assertIsNone(db.get_report_execution_intent(report_id))
            with db.get_session() as session:
                self.assertEqual(session.query(AnalysisReportExecutionIntent).count(), 0)
        finally:
            DatabaseManager.reset_instance()
            temp_dir.cleanup()

    def test_invalid_report_execution_intent_fails_the_whole_first_save(self) -> None:
        DatabaseManager.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        try:
            db = DatabaseManager(db_url=f"sqlite:///{os.path.join(temp_dir.name, 'intent-invalid.db')}")
            for invalid in (
                "paper_candidate",
                {"execution_intent": "live_trade", "intent_source": "unit_test"},
                {"execution_intent": "paper_candidate", "execution_scope": "account:main", "intent_source": "unit_test"},
                {"execution_intent": "paper_candidate", "execution_scope": "experiment:Exp1:a", "intent_source": "unit_test"},
                {"execution_intent": "paper_candidate", "execution_scope": "experiment:exp1:d", "intent_source": "unit_test"},
                {"execution_intent": "paper_candidate", "intent_source": ""},
            ):
                with self.subTest(invalid=invalid):
                    self.assertEqual(
                        db.save_analysis_history(
                            result=self._intent_result(),
                            query_id="intent-invalid",
                            report_type="detailed",
                            news_content=None,
                            execution_intent=invalid,
                        ),
                        0,
                    )
            with db.get_session() as session:
                self.assertEqual(session.query(AnalysisHistory).count(), 0)
                self.assertEqual(session.query(AnalysisReportExecutionIntent).count(), 0)

            undeclared_id = db.save_analysis_history(
                result=self._intent_result(),
                query_id="intent-undeclared",
                report_type="detailed",
                news_content=None,
            )
            self.assertGreater(undeclared_id, 0)
            self.assertEqual(
                evaluate_report_execution_intent(
                    db.get_report_execution_intent(undeclared_id), execution_scope="primary"
                )["reason"],
                "execution_intent_missing",
            )
        finally:
            DatabaseManager.reset_instance()
            temp_dir.cleanup()

    def test_report_execution_intent_legacy_baseline_runs_once_for_stock_reports(self) -> None:
        DatabaseManager.reset_instance()
        temp_dir = tempfile.TemporaryDirectory()
        db_path = os.path.join(temp_dir.name, "legacy-intent.db")
        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "CREATE TABLE analysis_history ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, code VARCHAR(10), "
                    "report_type VARCHAR(16), raw_result TEXT)"
                )
                conn.executemany(
                    "INSERT INTO analysis_history (id, code, report_type, raw_result) VALUES (?, ?, ?, ?)",
                    [
                        (1, "600519", "detailed", "{}"),
                        (2, "MARKET", "market_review", "{}"),
                        (3, "000001", "market_review", "{}"),
                        (4, "AAPL", None, "{}"),
                    ],
                )

            db = DatabaseManager(db_url=f"sqlite:///{db_path}")
            with sqlite3.connect(db_path) as conn:
                baseline = conn.execute(
                    "SELECT analysis_history_id, execution_intent, execution_scope, intent_source "
                    "FROM analysis_report_execution_intents ORDER BY analysis_history_id"
                ).fetchall()
                marker = conn.execute(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version = ?",
                    (REPORT_EXECUTION_INTENT_BASELINE_VERSION,),
                ).fetchone()[0]
                # A report written after the upgrade without a declared purpose.
                conn.execute(
                    "INSERT INTO analysis_history (id, code, report_type, raw_result) "
                    "VALUES (5, '000858', 'detailed', '{}')"
                )
            self.assertEqual(
                baseline,
                [
                    (1, "paper_candidate", "primary", "legacy_baseline"),
                    (4, "paper_candidate", "primary", "legacy_baseline"),
                ],
            )
            self.assertEqual(marker, 1)
            self.assertTrue(evaluate_report_execution_intent(db.get_report_execution_intent(1), execution_scope="primary")["authorized"])

            DatabaseManager.reset_instance()
            db = DatabaseManager(db_url=f"sqlite:///{db_path}")
            self.assertIsNone(db.get_report_execution_intent(5))
            with sqlite3.connect(db_path) as conn:
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM analysis_report_execution_intents").fetchone()[0],
                    2,
                )
        finally:
            DatabaseManager.reset_instance()
            temp_dir.cleanup()

if __name__ == '__main__':
    unittest.main()
