# -*- coding: utf-8 -*-
"""Record the trusted report purpose that production writes with each report."""

from __future__ import annotations

from src.storage import (
    REPORT_EXECUTION_INTENT_PAPER_CANDIDATE,
    REPORT_EXECUTION_SCOPE_PRIMARY,
    AnalysisReportExecutionIntent,
    normalize_report_execution_intent,
)


def add_report_execution_intent(
    session,
    report_id: int,
    *,
    execution_intent: str = REPORT_EXECUTION_INTENT_PAPER_CANDIDATE,
    execution_scope: str = REPORT_EXECUTION_SCOPE_PRIMARY,
    intent_source: str = "unit_test_fixture",
) -> None:
    """Fixtures that insert reports directly must declare purpose like the pipeline does."""

    session.add(AnalysisReportExecutionIntent(
        analysis_history_id=int(report_id),
        **normalize_report_execution_intent({
            "execution_intent": execution_intent,
            "execution_scope": execution_scope,
            "intent_source": intent_source,
        }),
    ))
