# -*- coding: utf-8 -*-
"""Durable ledger tests for market-opportunity discovery and analysis jobs."""

from __future__ import annotations

from datetime import date

import pytest

from src.repositories.market_opportunity_repository import MarketOpportunityRepository
from src.storage import AnalysisHistory, DatabaseManager


@pytest.fixture()
def opportunity_repo(tmp_path):
    DatabaseManager.reset_instance()
    db = DatabaseManager(db_url=f"sqlite:///{tmp_path / 'opportunities.db'}")
    yield MarketOpportunityRepository(db), db
    DatabaseManager.reset_instance()


def _source_report(db: DatabaseManager) -> int:
    with db.session_scope() as session:
        row = AnalysisHistory(
            code="MARKET",
            name="A股大盘复盘",
            report_type="market_review",
            query_id="market-20260831",
            model_used="market-review",
        )
        session.add(row)
        session.flush()
        return int(row.id)


def _source_report_for(db: DatabaseManager, query_id: str) -> int:
    with db.session_scope() as session:
        row = AnalysisHistory(
            code="MARKET",
            name="A股大盘复盘",
            report_type="market_review",
            query_id=query_id,
            model_used="market-review",
        )
        session.add(row)
        session.flush()
        return int(row.id)


def _create_run_for(repo: MarketOpportunityRepository, db: DatabaseManager, trade_date: date, suffix: str, status: str = "ready"):
    report_id = _source_report_for(db, f"market-{suffix}")
    run = repo.create_or_get_run(
        {
            "run_key": f"cn:{trade_date.isoformat()}:{report_id}:candidate-policy-v1",
            "market": "cn",
            "trade_date": trade_date,
            "source_report_id": report_id,
            "market_regime": "normal",
            "mode": "observe",
            "status": status,
            "llm_budget": 0,
        }
    )
    repo.update_run(run.id, status=status)
    return run


def _create_run(repo: MarketOpportunityRepository, db: DatabaseManager):
    report_id = _source_report(db)
    return repo.create_or_get_run(
        {
            "run_key": f"cn:2026-08-31:{report_id}:candidate-policy-v1",
            "market": "cn",
            "trade_date": date(2026, 8, 31),
            "source_report_id": report_id,
            "market_regime": "normal",
            "mode": "paper",
            "status": "analyzing",
            "llm_budget": 4,
        }
    )


def test_exact_candidate_identity_and_job_lease_are_durable(opportunity_repo):
    repo, db = opportunity_repo
    run = _create_run(repo, db)
    repo.replace_run_children(
        run_id=run.id,
        themes=[
            {
                "theme_key": "concept:黄金",
                "theme_type": "concept",
                "theme_name": "黄金",
                "status": "selected",
                "score": 82.0,
            }
        ],
        candidates=[
            {
                "candidate_key": "cn:sh:600519",
                "market": "cn",
                "exchange": "sh",
                "stock_code": "600519",
                "symbol": "600519.SH",
                "stock_name": "贵州茅台",
                "primary_theme_key": "concept:黄金",
                "related_themes": ["concept:黄金"],
                "status": "selected",
                "rank": 1,
                "score": 80.0,
                "as_of": date(2026, 8, 31),
                "valid_until": date(2026, 9, 1),
            }
        ],
        jobs=[
            {
                "candidate_key": "cn:sh:600519",
                "job_key": "run:cn:sh:600519:model-a",
                "model": "model-a",
            }
        ],
    )

    first = repo.claim_pending_jobs(owner="worker-a", limit=10, lease_seconds=600, effective_session=date(2026, 8, 31))
    second = repo.claim_pending_jobs(owner="worker-b", limit=10, lease_seconds=600, effective_session=date(2026, 8, 31))

    assert len(first) == 1
    assert first[0].attempt_count == 1
    assert second == []
    assert repo.mark_job_submitted(first[0].id, owner="worker-a", query_id="opp-query-1") is True
    assert repo.claim_pending_jobs(owner="worker-b", limit=10, lease_seconds=600, effective_session=date(2026, 8, 31)) == []
    submitted = repo.list_submitted_jobs(run_id=run.id)
    assert len(submitted) == 1
    assert submitted[0].query_id == "opp-query-1"
    page = repo.list_candidates_page(run_id=run.id, page=1, limit=10)
    assert page["total"] == 1
    assert page["items"][0]["symbol"] == "600519.SH"
    assert page["items"][0]["exchange"] == "sh"


def test_only_same_session_candidates_create_or_claim_jobs(opportunity_repo):
    repo, db = opportunity_repo
    run = _create_run(repo, db)
    repo.update_run(run.id, trade_date=date(2026, 9, 7))
    repo.replace_run_children(
        run_id=run.id,
        themes=[],
        candidates=[
            {
                "candidate_key": "cn:sh:600519",
                "market": "cn",
                "exchange": "sh",
                "stock_code": "600519",
                "symbol": "600519.SH",
                "stock_name": "贵州茅台",
                "status": "selected",
                "rank": 1,
                "score": 80.0,
                "as_of": date(2026, 8, 31),
                "valid_until": date(2026, 9, 1),
            },
            {
                "candidate_key": "cn:sh:600000",
                "market": "cn",
                "exchange": "sh",
                "stock_code": "600000",
                "symbol": "600000.SH",
                "stock_name": "浦发银行",
                "status": "selected",
                "rank": 2,
                "score": 79.0,
                "as_of": date(2026, 9, 7),
                "valid_until": date(2026, 9, 8),
            },
        ],
        jobs=[],
    )

    created = repo.create_jobs_for_run(
        run_id=run.id,
        effective_session=date(2026, 9, 7),
        jobs=[
            {"candidate_key": "cn:sh:600519", "job_key": "expired-job", "model": "model-a"},
            {"candidate_key": "cn:sh:600000", "job_key": "fresh-job", "model": "model-a"},
        ],
    )
    claimed = repo.claim_pending_jobs(owner="worker-a", limit=10, effective_session=date(2026, 9, 7))

    assert created == 1
    assert len(claimed) == 1
    assert claimed[0].job_key == "fresh-job"


def test_existing_expired_pending_jobs_are_not_claimed(opportunity_repo):
    repo, db = opportunity_repo
    run = _create_run(repo, db)
    repo.replace_run_children(
        run_id=run.id,
        themes=[],
        candidates=[
            {
                "candidate_key": "cn:sh:600519",
                "market": "cn",
                "exchange": "sh",
                "stock_code": "600519",
                "symbol": "600519.SH",
                "stock_name": "贵州茅台",
                "status": "selected",
                "rank": 1,
                "score": 80.0,
                "as_of": date(2026, 8, 31),
                "valid_until": date(2026, 9, 1),
            }
        ],
        jobs=[{"candidate_key": "cn:sh:600519", "job_key": "expired-pending", "model": "model-a"}],
    )

    assert repo.claim_pending_jobs(owner="worker-a", limit=10, effective_session=date(2026, 9, 7)) == []
    detail = repo.get_run_detail(run.id)
    assert detail["analysis_jobs"][0]["status"] == "pending"
    assert detail["analysis_jobs"][0]["attempt_count"] == 0


def test_completed_job_persists_artifact_links_from_reconciliation_atomically(opportunity_repo):
    repo, db = opportunity_repo
    run = _create_run(repo, db)
    repo.replace_run_children(
        run_id=run.id,
        themes=[],
        candidates=[
            {
                "candidate_key": "cn:sh:600519",
                "market": "cn",
                "exchange": "sh",
                "stock_code": "600519",
                "symbol": "600519.SH",
                "stock_name": "贵州茅台",
                "status": "selected",
                "rank": 1,
                "score": 80.0,
                "as_of": date(2026, 8, 31),
                "valid_until": date(2026, 9, 1),
            }
        ],
        jobs=[{"candidate_key": "cn:sh:600519", "job_key": "artifact-job", "model": "model-a"}],
    )
    job = repo.claim_pending_jobs(owner="worker-a", limit=1, effective_session=date(2026, 8, 31))[0]

    repo.complete_job(
        job.id,
        status="completed",
        query_id="opp-query-artifact",
        analysis_history_id=101,
        evidence={
            "paper_reconciliation": {
                "signal_id": 202,
                "plan_id": 303,
                "classification": "accepted",
            }
        },
    )

    stored = repo.get_job(job.id)
    assert stored["status"] == "completed"
    assert stored["analysis_history_id"] == 101
    assert stored["decision_signal_id"] == 202
    assert stored["paper_plan_id"] == 303
    assert stored["evidence"]["paper_reconciliation"]["plan_id"] == 303

    assert repo.link_job_artifacts(job.id, status="completed", analysis_history_id=101, evidence=stored["evidence"]) is True
    assert repo.get_job(job.id)["paper_plan_id"] == 303


def test_server_pagination_reports_total_beyond_500(opportunity_repo):
    repo, db = opportunity_repo
    run = _create_run(repo, db)
    candidates = []
    for index in range(501):
        code = f"{600000 + index:06d}"
        candidates.append(
            {
                "candidate_key": f"cn:sh:{code}",
                "market": "cn",
                "exchange": "sh",
                "stock_code": code,
                "symbol": f"{code}.SH",
                "stock_name": code,
                "status": "rejected",
                "rank": index + 1,
                "score": 0.0,
                "rejection_reasons": ["fixture_rejection"],
                "as_of": date(2026, 8, 31),
            }
        )
    repo.replace_run_children(run_id=run.id, themes=[], candidates=candidates, jobs=[])

    first = repo.list_candidates_page(run_id=run.id, page=1, limit=200)
    third = repo.list_candidates_page(run_id=run.id, page=3, limit=200)

    assert first["total"] == 501
    assert len(first["items"]) == 200
    assert third["page"] == 3
    assert len(third["items"]) == 101


def test_replace_run_children_refuses_to_delete_submitted_jobs(opportunity_repo):
    repo, db = opportunity_repo
    run = _create_run(repo, db)
    repo.replace_run_children(
        run_id=run.id,
        themes=[],
        candidates=[
            {
                "candidate_key": "cn:sh:600519",
                "market": "cn",
                "exchange": "sh",
                "stock_code": "600519",
                "symbol": "600519.SH",
                "stock_name": "贵州茅台",
                "status": "selected",
                "rank": 1,
                "score": 80.0,
                "as_of": date(2026, 8, 31),
                "valid_until": date(2026, 9, 1),
            }
        ],
        jobs=[{"candidate_key": "cn:sh:600519", "job_key": "job-submitted", "model": "model-a"}],
    )
    job = repo.claim_pending_jobs(owner="worker-a", limit=1, effective_session=date(2026, 8, 31))[0]
    assert repo.mark_job_submitted(job.id, owner="worker-a", query_id="market_opp_guard") is True

    with pytest.raises(RuntimeError, match="nonreplaceable"):
        repo.replace_run_children(run_id=run.id, themes=[], candidates=[], jobs=[])

    detail = repo.get_run_detail(run.id)
    assert len(detail["analysis_jobs"]) == 1
    assert detail["analysis_jobs"][0]["status"] == "submitted"


def test_theme_persistence_window_keeps_missing_dates_and_exact_provider(opportunity_repo):
    repo, db = opportunity_repo
    theme_key = "concept:创新药"
    dates = [
        date(2026, 8, 25),
        date(2026, 8, 26),
        date(2026, 8, 27),
        date(2026, 8, 28),
    ]
    for idx, trade_date in enumerate(dates):
        run = _create_run_for(repo, db, trade_date, str(idx))
        themes = []
        if idx != 1:
            themes.append(
                {
                    "theme_key": theme_key,
                    "theme_type": "concept",
                    "theme_name": "创新药",
                    "status": "selected",
                    "change_pct": 1.0 if idx in {0, 3} else -1.0,
                    "score": 70.0,
                    "constituent_snapshot": {"provider": "akshare_sina", "board_name": "创新药"},
                }
            )
        repo.replace_run_children(run_id=run.id, themes=themes, candidates=[], jobs=[])

    window = repo.theme_persistence_window(
        market="cn",
        as_of=date(2026, 8, 31),
        theme_key=theme_key,
        provider="akshare_sina",
        window_size=5,
    )

    assert [item["trade_date"] for item in window] == dates
    assert [item["present"] for item in window] == [True, False, True, True]
    assert [item["change_pct"] for item in window] == [1.0, None, -1.0, 1.0]


@pytest.mark.parametrize("run_key_prefix", ["opportunity-followup:", "opportunity-research-review:"])
def test_theme_persistence_window_ignores_later_single_stock_reanalysis_runs(opportunity_repo, run_key_prefix):
    repo, db = opportunity_repo
    theme_key = "concept:创新药"
    trade_date = date(2026, 8, 28)
    discovery = _create_run_for(repo, db, trade_date, "discovery", status="completed")
    repo.replace_run_children(run_id=discovery.id, themes=[{
        "theme_key": theme_key,
        "theme_type": "concept",
        "theme_name": "创新药",
        "status": "selected",
        "change_pct": 1.0,
        "score": 70.0,
        "constituent_snapshot": {"provider": "akshare_sina", "board_name": "创新药"},
    }], candidates=[], jobs=[])
    # 复核/确认运行只复制单只股票、不带题材；它们晚创建也不能把当天题材记为缺席。
    reanalysis = repo.create_or_get_run({
        "run_key": f"{run_key_prefix}abc",
        "market": "cn",
        "trade_date": trade_date,
        "source_report_id": _source_report_for(db, "market-reanalysis"),
        "market_regime": "risk_off",
        "mode": "paper",
        "status": "completed",
        "llm_budget": 1,
    })
    repo.update_run(reanalysis.id, status="completed")

    window = repo.theme_persistence_window(
        market="cn",
        as_of=date(2026, 8, 31),
        theme_key=theme_key,
        provider="akshare_sina",
        window_size=5,
    )

    assert [(item["trade_date"], item["present"]) for item in window] == [(trade_date, True)]


def test_frozen_membership_replay_requires_exact_report_and_date(opportunity_repo):
    repo, db = opportunity_repo
    run = _create_run(repo, db)
    repo.replace_run_children(run_id=run.id, themes=[{
        'theme_key': 'industry:test', 'theme_type': 'industry', 'theme_name': 'test', 'theme_code': 'csrc_A01',
        'status': 'rejected', 'constituent_snapshot': {'as_of': '2026-08-31', 'status': 'ok',
            'provider': 'csrc_tencent', 'board_code': 'csrc_A01', 'constituents': [{'symbol': '600001.SH'}]},
    }], candidates=[], jobs=[])
    frozen = repo.frozen_constituents_for_report(report_id=run.source_report_id, market='cn', trade_date=date(2026, 8, 31))
    assert len(frozen) == 1 and frozen[0]['run_id'] == run.id
    assert repo.frozen_constituents_for_report(report_id=run.source_report_id, market='cn', trade_date=date(2026, 9, 1)) == []
    assert repo.frozen_constituents_for_report(report_id=run.source_report_id+1, market='cn', trade_date=date(2026, 8, 31)) == []


def test_candidate_page_ranks_active_pool_ahead_of_later_inserted_rejections(opportunity_repo):
    repo, db = opportunity_repo
    run = _create_run(repo, db)
    candidates=[]
    for index, (status, rank, score) in enumerate([('selected', 1, 95), ('observing', 2, 80), ('rejected', None, 99)]):
        candidates.append({'candidate_key': f'cn:sh:{600100+index}', 'market':'cn', 'exchange':'sh',
            'stock_code': str(600100+index), 'symbol':f'{600100+index}.SH', 'status':status,
            'rank':rank, 'score':score, 'as_of':date(2026,8,31), 'snapshot_hash':str(index)})
    repo.replace_run_children(run_id=run.id,themes=[],candidates=candidates,jobs=[])
    first = repo.list_candidates_page(run_id=run.id,limit=2)
    assert first['total'] == 3
    assert [row['status'] for row in first['items']] == ['selected','observing']


def test_research_budget_charges_each_attempt_once_and_never_exceeds_limit(opportunity_repo):
    repo, _db = opportunity_repo
    session = date(2026, 8, 31)

    first = repo.acquire_research_budget_slot(market="CN", session_date=session, limit=2, job_id=1, attempt_no=1)
    restarted = repo.acquire_research_budget_slot(market="cn", session_date=session, limit=2, job_id=1, attempt_no=1)
    retry = repo.acquire_research_budget_slot(market="cn", session_date=session, limit=2, job_id=1, attempt_no=2)
    exhausted = repo.acquire_research_budget_slot(market="cn", session_date=session, limit=2, job_id=2, attempt_no=1)

    assert first["slot_no"] == 1 and first["state"] == "charged"
    assert restarted["id"] == first["id"]
    assert retry["slot_no"] == 2
    assert exhausted is None
    assert repo.research_budget_usage(market="cn", session_date=session, limit=2)["remaining"] == 0
    # 其他会话、其他市场各自独立计数。
    assert repo.acquire_research_budget_slot(
        market="cn", session_date=date(2026, 9, 1), limit=2, job_id=2, attempt_no=1,
    )["slot_no"] == 1
    # 下调上限立即生效；0 停止全部研究调用。
    assert repo.research_budget_usage(market="cn", session_date=session, limit=1)["remaining"] == 0
    assert repo.acquire_research_budget_slot(market="cn", session_date=session, limit=0, job_id=3, attempt_no=1) is None


def test_research_review_reservation_is_idempotent_and_consumed_first(opportunity_repo):
    repo, _db = opportunity_repo
    due = date(2026, 9, 1)

    reserved = repo.reserve_research_budget_slot(market="cn", session_date=due, limit=2, reservation_key="review:a")
    again = repo.reserve_research_budget_slot(market="cn", session_date=due, limit=2, reservation_key="review:a")
    assert again["id"] == reserved["id"] and reserved["state"] == "reserved"
    assert repo.research_budget_usage(market="cn", session_date=due, limit=2) == {
        "limit": 2, "used": 1, "charged": 0, "reserved": 1, "remaining": 1,
    }

    new_candidate = repo.acquire_research_budget_slot(market="cn", session_date=due, limit=2, job_id=10, attempt_no=1)
    assert new_candidate["slot_no"] != reserved["slot_no"]
    # 复核名额被预留后，新候选不能挤占。
    assert repo.acquire_research_budget_slot(market="cn", session_date=due, limit=2, job_id=11, attempt_no=1) is None

    review = repo.acquire_research_budget_slot(
        market="cn", session_date=due, limit=2, job_id=12, attempt_no=1,
        job_kind="review", reservation_key="review:a",
    )
    assert review["id"] == reserved["id"]
    assert review["state"] == "charged" and review["job_id"] == 12
    usage = repo.research_budget_usage(market="cn", session_date=due, limit=2)
    assert usage["charged"] == 2 and usage["reserved"] == 0
    # 已消费的预留不能被第二个任务重复使用；重试需要新的名额。
    assert repo.acquire_research_budget_slot(
        market="cn", session_date=due, limit=2, job_id=13, attempt_no=1,
        job_kind="review", reservation_key="review:a",
    ) is None
    assert repo.reserve_research_budget_slot(market="cn", session_date=due, limit=2, reservation_key="review:b") is None
