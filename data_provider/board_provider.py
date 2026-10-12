# -*- coding: utf-8 -*-
"""Composite board constituent provider with bounded source fallbacks."""

from __future__ import annotations

from datetime import date
from typing import Any, List, Optional

from data_provider.akshare_board_provider import (
    AkshareBoardConstituentProvider,
    BoardConstituentSnapshot,
)
from data_provider.sina_board_provider import SinaConceptBoardConstituentProvider


class CompositeBoardConstituentProvider:
    """Route known source identities; name-only legacy themes may fall back exactly."""

    provider_name = "composite"

    def __init__(
        self,
        *,
        primary: Optional[Any] = None,
        concept_fallback: Optional[Any] = None,
        industry_fallback: Optional[Any] = None,
        csrc_provider: Optional[Any] = None,
    ) -> None:
        self.primary = primary or AkshareBoardConstituentProvider()
        self.concept_fallback = concept_fallback or SinaConceptBoardConstituentProvider()
        self.industry_fallback = industry_fallback or SinaConceptBoardConstituentProvider(board_type="industry")
        self.csrc_provider = csrc_provider

    def get_board_constituents(
        self,
        *,
        market: str = "cn",
        board_type: str,
        board_name: str,
        board_code: Optional[str] = None,
        as_of: Optional[date] = None,
    ) -> BoardConstituentSnapshot:
        kwargs = dict(
            market=market,
            board_type=board_type,
            board_name=board_name,
            board_code=board_code,
            as_of=as_of,
        )
        kind = str(board_type or "").strip().lower()
        code = str(board_code or "").strip()
        if code.startswith("csrc_"):
            if self.csrc_provider is None:
                from data_provider.csrc_board_provider import CsrcBoardConstituentProvider
                self.csrc_provider = CsrcBoardConstituentProvider()
            return self.csrc_provider.get_board_constituents(**kwargs)
        fallback = self.industry_fallback if kind == "industry" else self.concept_fallback
        # A Sina industry is not an Eastmoney industry with a similar name.
        if code.startswith(("hangye_", "gn_")):
            return fallback.get_board_constituents(**kwargs)
        primary_snapshot = self.primary.get_board_constituents(**kwargs)
        if primary_snapshot.status in {"ok", "partial"} and primary_snapshot.constituents:
            return primary_snapshot
        if kind not in {"concept", "industry"} or code:
            return primary_snapshot
        fallback_snapshot = fallback.get_board_constituents(**kwargs)
        if fallback_snapshot.status in {"ok", "partial"} and fallback_snapshot.constituents:
            merged_errors: List[str] = list(fallback_snapshot.errors or [])
            merged_errors.extend(f"primary_{item}" for item in list(primary_snapshot.errors or []))
            return BoardConstituentSnapshot(
                market=fallback_snapshot.market,
                board_type=fallback_snapshot.board_type,
                board_name=fallback_snapshot.board_name,
                board_code=fallback_snapshot.board_code,
                as_of=fallback_snapshot.as_of,
                provider=fallback_snapshot.provider,
                fetched_at=fallback_snapshot.fetched_at,
                status=fallback_snapshot.status,
                schema_version=fallback_snapshot.schema_version,
                constituents=fallback_snapshot.constituents,
                errors=merged_errors,
                source_columns=fallback_snapshot.source_columns,
                snapshot_hash=fallback_snapshot.snapshot_hash,
            )
        merged_errors = list(primary_snapshot.errors or [])
        merged_errors.extend(f"fallback_{item}" for item in list(fallback_snapshot.errors or []))
        return BoardConstituentSnapshot(
            market=primary_snapshot.market,
            board_type=primary_snapshot.board_type,
            board_name=primary_snapshot.board_name,
            board_code=primary_snapshot.board_code,
            as_of=primary_snapshot.as_of,
            provider="composite",
            fetched_at=primary_snapshot.fetched_at,
            status="failed",
            schema_version=primary_snapshot.schema_version,
            constituents=[],
            errors=merged_errors or ["all_board_providers_failed"],
            source_columns=primary_snapshot.source_columns,
            snapshot_hash="",
        )

    def get_board_history(self, **kwargs: Any) -> Any:
        get_history = getattr(self.primary, "get_board_history", None)
        if not callable(get_history):
            return None
        return get_history(**kwargs)
