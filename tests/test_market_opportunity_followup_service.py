from datetime import date
from types import SimpleNamespace

from src.services.market_opportunity_followup_service import (
    RESEARCH_REVIEW_POLICY_VERSION,
    build_followup_obligation,
    build_research_review_obligation,
    build_selection_context,
    followup_state_for_session,
)


def _candidate():
    return SimpleNamespace(
        id=7,
        run_id=3,
        candidate_key="cn:sz:002714",
        stock_code="002714",
        symbol="002714.SZ",
        primary_theme_key="industry:agriculture",
        related_themes=["industry:agriculture"],
        rank=1,
        score=82.5,
        score_components={"trend": 20},
        data_quality={"daily_bar_source": "tushare"},
        quote_snapshot={"close": 40.0},
        as_of=date(2026, 9, 7),
        valid_until=date(2026, 9, 8),
        snapshot_hash="abc",
    )


def test_followup_only_uses_authoritative_awaiting_confirmation_transition():
    reconciliation = {
        "item": {
            "executable": False,
            "strategy_thesis_transition": {
                "reason": "awaiting_entry_confirmation",
                "confirmation_count": 1,
                "evidence": {"effective_confirmation_required": 2},
            },
        }
    }
    obligation = build_followup_obligation(
        reconciliation=reconciliation,
        candidate=_candidate(),
        model="model-a",
        due_date=date(2026, 9, 8),
        origin_job_id=11,
        origin_report_id=21,
    )
    assert obligation is not None
    assert obligation["confirmation_current"] == 1
    assert obligation["confirmation_required"] == 2
    assert followup_state_for_session(obligation, effective_session=date(2026, 9, 7)) == "scheduled"
    assert followup_state_for_session(obligation, effective_session=date(2026, 9, 8)) == "due"
    assert followup_state_for_session(obligation, effective_session=date(2026, 9, 9)) == "expired"


def test_followup_does_not_schedule_for_display_buy_or_completed_confirmation():
    assert build_followup_obligation(
        reconciliation={"item": {"action": "buy"}},
        candidate=_candidate(),
        model="model-a",
        due_date=date(2026, 9, 8),
        origin_job_id=11,
        origin_report_id=21,
    ) is None
    assert build_followup_obligation(
        reconciliation={
            "item": {
                "strategy_thesis_transition": {
                    "reason": "awaiting_entry_confirmation",
                    "confirmation_count": 2,
                    "evidence": {"effective_confirmation_required": 2},
                }
            }
        },
        candidate=_candidate(),
        model="model-a",
        due_date=date(2026, 9, 8),
        origin_job_id=11,
        origin_report_id=21,
    ) is None


def test_selection_context_is_bounded_and_explicitly_non_authoritative():
    context = build_selection_context(
        candidate=_candidate(),
        job=SimpleNamespace(id=11, model="model-a"),
    )
    assert context["source"] == "market_opportunity"
    assert context["as_of"] == "2026-09-07"
    assert context["data_quality"]["daily_bar_source"] == "tushare"
    assert "do not treat as a buy mandate" in context["instruction"]


def test_research_review_is_one_day_and_only_for_bullish_research():
    kwargs = dict(
        candidate=_candidate(),
        model="model-a",
        due_date=date(2026, 9, 8),
        origin_job_id=11,
        origin_report_id=21,
        reservation_key="research-review:abc",
    )
    assert build_research_review_obligation(outcome="non_bullish", **kwargs) is None
    assert build_research_review_obligation(outcome="failed", **kwargs) is None

    obligation = build_research_review_obligation(outcome="bullish", **kwargs)
    assert obligation["policy_version"] == RESEARCH_REVIEW_POLICY_VERSION
    assert obligation["reservation_key"] == "research-review:abc"
    # 研究复核不是主账户确认，不携带确认计数。
    assert "confirmation_current" not in obligation
    assert followup_state_for_session(obligation, effective_session=date(2026, 9, 7)) == "scheduled"
    assert followup_state_for_session(obligation, effective_session=date(2026, 9, 8)) == "due"
    assert followup_state_for_session(obligation, effective_session=date(2026, 9, 9)) == "expired"
