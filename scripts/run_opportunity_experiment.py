# -*- coding: utf-8 -*-
"""R4: run one opportunity A/B/C experiment arm in an isolated process.

Each arm binds to its own database and reuses the single paper-trading engine.
Isolation is enforced *before* importing ``src``: this process reads the arm's
``DATABASE_URL`` from the env var named in the registry, sets it as the active
``DATABASE_URL`` so the ``DatabaseManager`` singleton binds to the arm DB, and
refuses to start if that URL is missing, shared with another arm, or equal to
the main database.

The arm worker never emits normal trading notifications, never syncs paper
positions into a real portfolio, and never enqueues work onto the shared task
queue — it registers only its own monitoring loop against its own DB.  The main
scheduler does not scan experiment databases, and an arm failure must not fall
back to the main database.

Usage:
    python scripts/run_opportunity_experiment.py --experiment-id exp1 --arm b \\
        --registry path/to/registry.json --mode eod --evaluation-date 2026-09-30
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_registry(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _bind_arm_database_before_import(registry: dict, arm: str) -> str:
    """Resolve the arm's DB URL and make it the active DATABASE_URL.

    Runs before any ``src`` import so the DatabaseManager singleton and Config
    bind to the arm database, never the main one.  Returns the arm URL.
    """

    arm = arm.strip().lower()
    if arm not in ("a", "b", "c"):
        raise SystemExit(f"unknown arm: {arm!r}")
    arm_env = (registry.get("arm_db_env") or {})
    var_name = str(arm_env.get(arm) or "").strip()
    if not var_name:
        raise SystemExit(f"registry has no db env var name for arm {arm!r}")
    arm_url = str(os.environ.get(var_name) or "").strip()
    if not arm_url:
        raise SystemExit(f"arm {arm!r} db env var {var_name!r} is not set")
    # 绑定到本组数据库；显式清空 DATABASE_PATH 以免 SQLite 回退到主库路径。
    os.environ["DATABASE_URL"] = arm_url
    os.environ.pop("DATABASE_PATH", None)
    return arm_url


def _verify_isolation(registry: dict) -> None:
    """Prove the three arm DBs are distinct and none equals the main database.

    Imported lazily so this runs only after DATABASE_URL was set to the arm URL.
    We compare the *other* arms' env-configured URLs and the pre-bind main URL;
    credentials are never printed.
    """

    from src.services.opportunity_experiment_service import (
        OpportunityExperimentRegistry,
        _render_safe_url,
    )

    main_url = str(registry.get("main_db_url") or "").strip()
    if not main_url:
        raise SystemExit("registry must carry main_db_url for isolation checks")
    parsed = OpportunityExperimentRegistry.from_mapping(registry)
    resolved = parsed.resolve_isolated_databases(main_db_url=main_url)
    print(f"[isolation] main={_render_safe_url(main_url)}")
    for arm_key, safe in resolved.items():
        print(f"[isolation] arm {arm_key}={safe}")


def _build_service(registry: dict, experiment_id: str, arm: str):
    from src.config import get_config
    from src.repositories.paper_trading_repo import PaperTradingRepository
    from src.services.opportunity_experiment_service import experiment_scope
    from src.services.paper_trading_service import PaperTradingService

    config = get_config()
    # 实验工作进程强制关闭普通交易通知，禁用主账户任务消费与持仓同步入口。
    try:
        config.paper_trading_feishu_notify = False
    except Exception:
        pass
    scope = experiment_scope(experiment_id, arm)
    service = PaperTradingService.from_config(
        config,
        repo=PaperTradingRepository(),
        execution_scope=scope,
    )
    service.feishu_notify = False
    return service


def _frozen_fetcher_manager():
    """Placeholder for the per-session frozen-quote fetcher.

    Real runs pass a fetcher_manager built from the frozen market-input batch so
    the arm never hits the network mid-session.  Kept as a hook; a None value
    lets intraday fall back to its own manager only when explicitly allowed.
    """

    return None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Run one isolated opportunity experiment arm")
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--arm", required=True, choices=["a", "b", "c"])
    parser.add_argument("--registry", required=True, help="path to the experiment registry JSON")
    parser.add_argument("--mode", choices=["eod", "intraday", "verify"], default="verify")
    parser.add_argument("--evaluation-date", help="ISO date for --mode eod")
    args = parser.parse_args(argv)

    registry = _load_registry(args.registry)
    _bind_arm_database_before_import(registry, args.arm)
    _verify_isolation(registry)
    if args.mode == "verify":
        print("[ok] isolation verified; no ledger mutation in verify mode")
        return 0

    service = _build_service(registry, args.experiment_id, args.arm)
    if args.mode == "eod":
        from datetime import date

        if not args.evaluation_date:
            raise SystemExit("--mode eod requires --evaluation-date")
        eval_date = date.fromisoformat(args.evaluation_date)
        # 盘后模式的完整 bar 由冻结的市场输入批次提供；此处留出真实接线点。
        summary = service.run_eod(eval_date, bars_by_stock={})
        print(json.dumps({"mode": "eod", "summary": summary}, ensure_ascii=False, default=str))
        return 0

    summary = service.run_intraday_monitor(fetcher_manager=_frozen_fetcher_manager())
    print(json.dumps({"mode": "intraday", "summary": summary}, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
