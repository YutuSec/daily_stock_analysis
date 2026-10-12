# -*- coding: utf-8 -*-
"""Pure contracts for bounded next-session opportunity confirmation."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, Mapping, Optional


FOLLOWUP_POLICY_VERSION = "opportunity-followup-v1"
SELECTION_CONTEXT_VERSION = "opportunity-selection-context-v1"
RESEARCH_REVIEW_POLICY_VERSION = "opportunity-research-review-v1"


def _coerce_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def plan_transition(reconciliation: Mapping[str, Any]) -> Dict[str, Any]:
    """Return the authoritative strategy transition embedded in a plan."""

    item = reconciliation.get("item")
    if not isinstance(item, Mapping):
        return {}
    transition = item.get("strategy_thesis_transition")
    return dict(transition) if isinstance(transition, Mapping) else {}


def build_followup_obligation(
    *,
    reconciliation: Mapping[str, Any],
    candidate: Any,
    model: str,
    due_date: date,
    origin_job_id: int,
    origin_report_id: int,
) -> Optional[Dict[str, Any]]:
    """Create a follow-up only for the existing two-session confirmation state.

    This function never infers a bullish state from display text.  The paper
    strategy transition is the single execution authority.
    """

    transition = plan_transition(reconciliation)
    if transition.get("reason") != "awaiting_entry_confirmation":
        return None
    evidence = transition.get("evidence")
    evidence = dict(evidence) if isinstance(evidence, Mapping) else {}
    confirmation_count = int(transition.get("confirmation_count") or 0)
    required = int(
        evidence.get("effective_confirmation_required")
        or evidence.get("entry_confirmation_required")
        or 2
    )
    if confirmation_count <= 0 or confirmation_count >= required:
        return None
    return {
        "policy_version": FOLLOWUP_POLICY_VERSION,
        "status": "scheduled",
        "due_date": due_date.isoformat(),
        "expires_on": due_date.isoformat(),
        "origin_job_id": int(origin_job_id),
        "origin_report_id": int(origin_report_id),
        "origin_candidate_id": int(getattr(candidate, "id", 0) or 0),
        "origin_run_id": int(getattr(candidate, "run_id", 0) or 0),
        "model": str(model),
        "confirmation_current": confirmation_count,
        "confirmation_required": required,
        "reason": "awaiting_entry_confirmation",
    }


def build_research_review_obligation(
    *,
    outcome: str,
    candidate: Any,
    model: str,
    due_date: date,
    origin_job_id: int,
    origin_report_id: int,
    reservation_key: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Schedule one next-session review for a bullish research-only result.

    The review re-analyses fresh data in research-only mode.  It is never a
    main-account confirmation, and a review result never schedules another
    review, so one research chain spans at most two adjacent sessions.
    """

    if outcome != "bullish":
        return None
    return {
        "policy_version": RESEARCH_REVIEW_POLICY_VERSION,
        "status": "scheduled",
        "due_date": due_date.isoformat(),
        "expires_on": due_date.isoformat(),
        "origin_job_id": int(origin_job_id),
        "origin_report_id": int(origin_report_id),
        "origin_candidate_id": int(getattr(candidate, "id", 0) or 0),
        "origin_run_id": int(getattr(candidate, "run_id", 0) or 0),
        "model": str(model),
        "reservation_key": reservation_key,
        "reason": "bullish_research_outcome",
    }


def followup_state_for_session(
    obligation: Mapping[str, Any],
    *,
    effective_session: date,
) -> str:
    """Classify a stored obligation without allowing late analysis."""

    due_date = _coerce_date(obligation.get("due_date"))
    expires_on = _coerce_date(obligation.get("expires_on")) or due_date
    if due_date is None:
        return "invalid"
    if expires_on is not None and effective_session > expires_on:
        return "expired"
    if effective_session < due_date:
        return "scheduled"
    return "due"


def build_selection_context(*, candidate: Any, job: Any) -> Dict[str, Any]:
    """Freeze the bounded, trusted evidence supplied to the LLM."""

    def load_mapping(value: Any) -> Dict[str, Any]:
        if isinstance(value, Mapping):
            return dict(value)
        if isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value)
                return dict(parsed) if isinstance(parsed, Mapping) else {}
            except (TypeError, ValueError):
                return {}
        return {}

    score_components = load_mapping(
        getattr(candidate, "score_components", None)
        or getattr(candidate, "score_components_json", None)
    )
    data_quality = load_mapping(
        getattr(candidate, "data_quality", None)
        or getattr(candidate, "data_quality_json", None)
    )
    quote_snapshot = load_mapping(
        getattr(candidate, "quote_snapshot", None)
        or getattr(candidate, "quote_snapshot_json", None)
    )
    related_themes = getattr(candidate, "related_themes", None)
    if related_themes is None:
        raw_related = getattr(candidate, "related_themes_json", None)
        if isinstance(raw_related, str) and raw_related.strip():
            try:
                related_themes = json.loads(raw_related)
            except (TypeError, ValueError):
                related_themes = []
    if not isinstance(related_themes, list):
        related_themes = []
    return {
        "source": "market_opportunity",
        "policy_version": SELECTION_CONTEXT_VERSION,
        "instruction": "selection evidence only; independently validate and do not treat as a buy mandate",
        "candidate_id": int(getattr(candidate, "id", 0) or 0),
        "run_id": int(getattr(candidate, "run_id", 0) or 0),
        "candidate_key": getattr(candidate, "candidate_key", None),
        "stock_code": getattr(candidate, "stock_code", None),
        "symbol": getattr(candidate, "symbol", None),
        "primary_theme": getattr(candidate, "primary_theme_key", None),
        "related_themes": related_themes,
        "rank": getattr(candidate, "rank", None),
        "score": getattr(candidate, "score", None),
        "score_components": score_components,
        "data_quality": data_quality,
        "quote_snapshot": quote_snapshot,
        "as_of": (
            getattr(candidate, "as_of").isoformat()
            if getattr(candidate, "as_of", None) is not None
            else None
        ),
        "valid_until": (
            getattr(candidate, "valid_until").isoformat()
            if getattr(candidate, "valid_until", None) is not None
            else None
        ),
        "snapshot_hash": getattr(candidate, "snapshot_hash", None),
        "job_id": int(getattr(job, "id", 0) or 0),
        "model": getattr(job, "model", None),
    }
