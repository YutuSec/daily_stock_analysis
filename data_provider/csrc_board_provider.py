# -*- coding: utf-8 -*-
"""Derived CSRC industry boards backed by BaoStock classification and Tencent quotes.

This provider intentionally does not alias EastMoney/Sina taxonomies.  It builds
one bounded, same-session in-memory snapshot from BaoStock's CSRC classifications
and Tencent member quotes, then exposes exact CSRC industry rankings and
constituents through the existing board snapshot dataclasses.
"""

from __future__ import annotations

import logging
import hashlib
import json
import math
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import requests

from data_provider.akshare_board_provider import (
    BOARD_CONSTITUENT_SCHEMA_VERSION,
    SUPPORTED_BOARD_TYPES,
    AkshareBoardConstituentProvider,
    BoardConstituent,
    BoardConstituentSnapshot,
)
from data_provider.base import normalize_stock_code
from src.core.trading_calendar import get_effective_trading_date, is_market_open

logger = logging.getLogger(__name__)


_CSRC_CODE_RE = re.compile(r"^([A-Z]\d{2})(.+)$")
_TENCENT_RECORD_RE = re.compile(r'v_([a-z]{2}\d{6})="([^"]*)";')
_MAX_CLASSIFIED_ROWS = 10000
_MAX_BUILD_SECONDS = 120.0
_CLASSIFICATION_TIMEOUT_SECONDS = 60.0
_QUOTE_BATCH_SIZE = 100
_QUOTE_BATCH_PACING_SECONDS = 0.2
_QUOTE_CONNECT_TIMEOUT_SECONDS = 3.0
_QUOTE_READ_TIMEOUT_SECONDS = 8.0
_QUOTE_RETRY_BACKOFF_SECONDS = 0.5
_INTRADAY_QUOTE_MAX_AGE_SECONDS = 300.0
_CN_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class _Member:
    exchange: str
    code: str
    symbol: str
    name: str
    csrc_code: str
    csrc_name: str
    update_date: date
    quote: Dict[str, Any]


@dataclass(frozen=True)
class _Industry:
    code: str
    name: str
    as_of: date
    change_pct: float
    members: Tuple[_Member, ...]


@dataclass(frozen=True)
class _Snapshot:
    as_of: date
    fetched_at: datetime
    industries: Dict[str, _Industry]
    name_to_code: Dict[str, str]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class _SharedState:
    snapshot: Optional[_Snapshot] = None
    expires_at: float = 0.0
    failure_retry_at: float = 0.0
    failure_error: Optional[str] = None


class CsrcBoardConstituentProvider:
    """Exact CSRC industry provider with equal-weight Tencent member returns."""

    provider_name = "csrc_tencent"

    _state = _SharedState()
    _lock = threading.Lock()
    _build_lock = threading.Lock()

    def __init__(
        self,
        *,
        cache_ttl_seconds: int = 900,
        failure_cooldown_seconds: int = 30,
        min_ranking_members: int = 5,
    ) -> None:
        self.cache_ttl_seconds = max(0.0, float(cache_ttl_seconds))
        self.failure_cooldown_seconds = max(0.0, float(failure_cooldown_seconds))
        self.min_ranking_members = max(1, int(min_ranking_members))

    def get_sector_rankings(self, n: int = 5) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        snapshot = self._get_snapshot()
        limit = max(0, int(n))
        rows = [
            self._ranking_row(industry)
            for industry in snapshot.industries.values()
            if len(industry.members) >= self.min_ranking_members
        ]
        rows.sort(key=lambda row: (row["change_pct"], row["code"]))
        bottom = rows[:limit]
        top = list(reversed(rows[-limit:])) if limit else []
        return top, bottom

    def get_full_board_rankings(
        self,
        *,
        market: str = "cn",
        board_type: str = "industry",
    ) -> Dict[str, Any]:
        normalized_market = str(market or "cn").strip().lower() or "cn"
        normalized_type = str(board_type or "industry").strip().lower() or "industry"
        if normalized_market != "cn":
            return self._full_rankings_failure(normalized_market, normalized_type, "market_not_supported")
        if normalized_type != "industry":
            return self._full_rankings_failure(normalized_market, normalized_type, "csrc_taxonomy_mismatch")

        snapshot = self._get_snapshot()
        rows = [
            self._full_ranking_row(industry)
            for industry in snapshot.industries.values()
            if len(industry.members) >= self.min_ranking_members
        ]
        rows.sort(key=lambda row: (-float(row["change_pct"]), str(row["theme_code"])))
        for idx, row in enumerate(rows, start=1):
            row["rank"] = idx
        coverage = {
            "status": "healthy" if rows else "empty",
            "source": self.provider_name,
            "taxonomy": "csrc_industry",
            "as_of": snapshot.as_of.isoformat(),
            "theme_count": len(rows),
            "raw_theme_count": len(snapshot.industries),
            "min_ranking_members": self.min_ranking_members,
            **dict(snapshot.metadata or {}),
        }
        snapshot_hash = hashlib.sha256(
            json.dumps(rows, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        return {
            "status": "healthy" if rows else "failed",
            "market": "cn",
            "source": self.provider_name,
            "taxonomy": "csrc_industry",
            "board_type": "industry",
            "as_of": snapshot.as_of,
            "fetched_at": snapshot.fetched_at,
            "themes": rows,
            "coverage": coverage,
            "snapshot_hash": snapshot_hash,
            "reason": None if rows else "no_rankable_csrc_industries",
        }

    def get_board_constituents(
        self,
        *,
        market: str = "cn",
        board_type: str,
        board_name: str,
        board_code: Optional[str] = None,
        as_of: Optional[date] = None,
    ) -> BoardConstituentSnapshot:
        normalized_market = str(market or "cn").strip().lower() or "cn"
        normalized_type = str(board_type or "").strip().lower()
        normalized_name = str(board_name or "").strip()
        normalized_code = str(board_code or "").strip() or None
        fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)
        effective_date = self._effective_session_date()
        errors: List[str] = []
        if normalized_market != "cn":
            errors.append("market_not_supported")
        if normalized_type not in SUPPORTED_BOARD_TYPES:
            errors.append("board_type_not_supported")
        if normalized_type != "industry":
            errors.append("csrc_taxonomy_mismatch")
        if not normalized_name:
            errors.append("board_name_required")
        if as_of is not None and as_of != effective_date:
            errors.append("historical_membership_unavailable")
        if errors:
            return self._fail(
                market=normalized_market,
                board_type=normalized_type,
                board_name=normalized_name,
                board_code=normalized_code,
                as_of=effective_date,
                fetched_at=fetched_at,
                errors=errors,
            )

        try:
            snapshot = self._get_snapshot()
        except Exception as exc:
            return self._fail(
                market=normalized_market,
                board_type=normalized_type,
                board_name=normalized_name,
                board_code=normalized_code,
                as_of=effective_date,
                fetched_at=fetched_at,
                errors=[f"provider_error:{type(exc).__name__}"],
            )

        code = normalized_code
        if code:
            code = code.removeprefix("csrc_")
            industry = snapshot.industries.get(code)
            if industry is None or industry.name != normalized_name:
                return self._fail(
                    market=normalized_market,
                    board_type=normalized_type,
                    board_name=normalized_name,
                    board_code=normalized_code,
                    as_of=snapshot.as_of,
                    fetched_at=snapshot.fetched_at,
                    errors=["board_code_name_mismatch"],
                )
        else:
            code = snapshot.name_to_code.get(normalized_name)
            industry = snapshot.industries.get(code or "")
            if industry is None:
                return self._fail(
                    market=normalized_market,
                    board_type=normalized_type,
                    board_name=normalized_name,
                    board_code=normalized_code,
                    as_of=snapshot.as_of,
                    fetched_at=snapshot.fetched_at,
                    errors=["exact_board_name_not_found"],
                )

        constituents = [self._member_to_constituent(member) for member in industry.members]
        board_code_out = f"csrc_{industry.code}"
        return BoardConstituentSnapshot(
            market="cn",
            board_type="industry",
            board_name=industry.name,
            board_code=board_code_out,
            as_of=snapshot.as_of,
            provider=self.provider_name,
            fetched_at=snapshot.fetched_at,
            status="ok",
            schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
            constituents=constituents,
            errors=[],
            source_columns=[
                "baostock.updateDate",
                "baostock.code",
                "baostock.code_name",
                "baostock.industry",
                "baostock.industryClassification",
                "tencent.field2",
                "tencent.field3",
                "tencent.field4",
                "tencent.field30",
                "tencent.field32",
                "tencent.field35/37",
                "tencent.field49",
            ],
            snapshot_hash=AkshareBoardConstituentProvider._hash_snapshot(
                "cn",
                "industry",
                industry.name,
                board_code_out,
                snapshot.as_of,
                constituents,
            ),
        )

    @classmethod
    def _reset_shared_cache_for_tests(cls) -> None:
        with cls._lock:
            cls._state = _SharedState()

    def _get_snapshot(self) -> _Snapshot:
        now = time.monotonic()
        effective_date = self._effective_session_date()
        with self._lock:
            state = self._state
            if (
                state.snapshot
                and state.expires_at > now
                and state.snapshot.as_of == effective_date
                and self._snapshot_quotes_fresh_for_now(state.snapshot)
            ):
                return state.snapshot
            if state.failure_retry_at > now:
                raise RuntimeError(state.failure_error or "provider_in_failure_cooldown")
        with self._build_lock:
            now = time.monotonic()
            effective_date = self._effective_session_date()
            with self._lock:
                state = self._state
                if (
                    state.snapshot
                    and state.expires_at > now
                    and state.snapshot.as_of == effective_date
                    and self._snapshot_quotes_fresh_for_now(state.snapshot)
                ):
                    return state.snapshot
                if state.failure_retry_at > now:
                    raise RuntimeError(state.failure_error or "provider_in_failure_cooldown")
            try:
                snapshot = self._build_snapshot(effective_date=effective_date)
            except Exception as exc:
                with self._lock:
                    self._state.failure_retry_at = time.monotonic() + self.failure_cooldown_seconds
                    self._state.failure_error = f"{type(exc).__name__}:{exc}"
                raise
            with self._lock:
                self._state.snapshot = snapshot
                self._state.expires_at = time.monotonic() + self.cache_ttl_seconds
                self._state.failure_retry_at = 0.0
                self._state.failure_error = None
            return snapshot

    def _build_snapshot(self, *, effective_date: date) -> _Snapshot:
        started = time.monotonic()
        rows = self._fetch_classification_rows()
        groups = self._parse_classification(rows=rows, effective_date=effective_date)
        self._check_budget(started)
        quote_symbols = sorted({member.symbol for members in groups.values() for member in members})
        quotes = self._fetch_tencent_quotes_with_budget(
            quote_symbols,
            effective_date=effective_date,
            started=started,
        )

        industries: Dict[str, _Industry] = {}
        name_to_code: Dict[str, str] = {}
        blocked = 0
        for code, members in groups.items():
            enriched: List[_Member] = []
            missing_or_invalid = False
            for member in members:
                quote = quotes.get(member.symbol)
                if not quote:
                    missing_or_invalid = True
                    break
                enriched.append(
                    _Member(
                        exchange=member.exchange,
                        code=member.code,
                        symbol=member.symbol,
                        name=member.name,
                        csrc_code=member.csrc_code,
                        csrc_name=member.csrc_name,
                        update_date=member.update_date,
                        quote=quote,
                    )
                )
            if missing_or_invalid:
                blocked += 1
                continue
            change_pct = sum(float(member.quote["change_pct"]) for member in enriched) / len(enriched)
            name = enriched[0].csrc_name
            industries[code] = _Industry(
                code=code,
                name=name,
                as_of=effective_date,
                change_pct=change_pct,
                members=tuple(enriched),
            )
            name_to_code[name] = code
            self._check_budget(started)

        if not industries:
            raise RuntimeError("no_complete_csrc_industries")
        return _Snapshot(
            as_of=effective_date,
            fetched_at=datetime.now(timezone.utc).replace(tzinfo=None),
            industries=industries,
            name_to_code=name_to_code,
            metadata={
                "provider": self.provider_name,
                "classification_rows": len(rows),
                "complete_industries": len(industries),
                "blocked_industries": blocked,
                "aggregation": "equal_weight_member_return",
                "quote_source": "tencent",
                "classification_source": "baostock_csrc",
            },
        )

    @staticmethod
    def _fetch_classification_rows() -> List[Dict[str, str]]:
        from data_provider.akshare_fetcher import _akshare_call_with_timeout
        from data_provider.sina_board_provider import _fetch_csrc_industries

        rows = _akshare_call_with_timeout(
            _fetch_csrc_industries,
            timeout=_CLASSIFICATION_TIMEOUT_SECONDS,
            call_name="csrc_classification",
        )
        if not isinstance(rows, list) or not rows:
            raise ValueError("classification_response_incomplete")
        if len(rows) > _MAX_CLASSIFIED_ROWS:
            raise ValueError("classification_row_limit_exceeded")
        return rows

    @classmethod
    def _fetch_tencent_quotes_with_budget(
        cls, symbols: Sequence[str], *, effective_date: date, started: float
    ) -> Dict[str, Dict[str, Any]]:
        from data_provider.akshare_fetcher import _akshare_call_with_timeout

        cls._check_budget(started)
        remaining = max(0.001, _MAX_BUILD_SECONDS - (time.monotonic() - started))
        return _akshare_call_with_timeout(
            cls._fetch_tencent_quotes,
            list(symbols),
            timeout=remaining,
            call_name="csrc_tencent_quotes",
            effective_date=effective_date,
            started=started,
        )

    @classmethod
    def _parse_classification(
        cls, *, rows: Sequence[Dict[str, Any]], effective_date: date
    ) -> Dict[str, List[_Member]]:
        names_by_code: Dict[str, str] = {}
        identities: Dict[Tuple[str, str], Tuple[str, str]] = {}
        groups: Dict[str, List[_Member]] = {}
        for row in rows:
            raw_industry = str(row.get("industry") or "").strip()
            if not raw_industry:
                continue
            code_raw = str(row.get("code") or "").strip()
            exchange, code = cls._parse_baostock_code(code_raw)
            if exchange is None or code is None:
                if not code_raw.lower().startswith(("sh.", "sz.")):
                    continue
                raise ValueError("classification_symbol_invalid")
            taxonomy = str(row.get("industryClassification") or "").strip()
            if taxonomy != "证监会行业分类":
                raise ValueError("classification_taxonomy_mismatch")
            match = _CSRC_CODE_RE.match(raw_industry)
            if not match:
                raise ValueError("csrc_industry_code_name_malformed")
            csrc_code, csrc_name = match.group(1), match.group(2).strip()
            if not csrc_name:
                raise ValueError("csrc_industry_name_missing")
            update_date = cls._parse_date(row.get("updateDate"))
            if update_date is None:
                raise ValueError("classification_update_date_missing")
            if update_date > effective_date:
                raise ValueError("classification_update_date_in_future")
            if effective_date - update_date > timedelta(days=31):
                raise ValueError("classification_update_date_stale")
            name = str(row.get("code_name") or "").strip()
            if not name:
                raise ValueError("classification_stock_name_missing")
            previous_name = names_by_code.get(csrc_code)
            if previous_name is not None and previous_name != csrc_name:
                raise ValueError("csrc_code_name_conflict")
            names_by_code[csrc_code] = csrc_name
            identity = (exchange, code)
            previous_classification = identities.get(identity)
            current_classification = (csrc_code, csrc_name)
            if previous_classification is not None:
                if previous_classification != current_classification:
                    raise ValueError("duplicate_conflicting_classification")
                raise ValueError("duplicate_classification_identity")
            identities[identity] = current_classification
            groups.setdefault(csrc_code, []).append(
                _Member(
                    exchange=exchange,
                    code=code,
                    symbol=f"{exchange}{code}",
                    name=name,
                    csrc_code=csrc_code,
                    csrc_name=csrc_name,
                    update_date=update_date,
                    quote={},
                )
            )
        if not groups:
            raise ValueError("empty_csrc_classification")
        return groups

    @classmethod
    def _fetch_tencent_quotes(
        cls, symbols: Sequence[str], *, effective_date: date, started: float
    ) -> Dict[str, Dict[str, Any]]:
        if not symbols:
            return {}
        quotes: Dict[str, Dict[str, Any]] = {}
        session = requests.Session()
        try:
            for index in range(0, len(symbols), _QUOTE_BATCH_SIZE):
                cls._check_budget(started)
                batch = list(symbols[index : index + _QUOTE_BATCH_SIZE])
                if index > 0 and _QUOTE_BATCH_PACING_SECONDS > 0:
                    time.sleep(_QUOTE_BATCH_PACING_SECONDS)
                response_text = cls._request_tencent_batch_with_retry(batch, session=session, started=started)
                parsed = cls._parse_tencent_response(
                    response_text,
                    expected_symbols=batch,
                    effective_date=effective_date,
                )
                unexpected = set(parsed) - set(batch)
                if unexpected:
                    raise ValueError("tencent_unexpected_symbol")
                for symbol, quote in parsed.items():
                    if symbol in quotes:
                        raise ValueError("tencent_duplicate_symbol")
                    quotes[symbol] = quote
        finally:
            session.close()
        return quotes

    @staticmethod
    def _request_tencent_batch(symbols: Sequence[str], *, session: Optional[requests.Session] = None) -> str:
        from data_provider.akshare_fetcher import TENCENT_REALTIME_ENDPOINT

        url = f"https://{TENCENT_REALTIME_ENDPOINT}={','.join(symbols)}"
        headers = {
            "Referer": "http://finance.qq.com",
            "User-Agent": "Mozilla/5.0",
        }
        requester = session or requests
        response = requester.get(
            url,
            headers=headers,
            timeout=(_QUOTE_CONNECT_TIMEOUT_SECONDS, _QUOTE_READ_TIMEOUT_SECONDS),
        )
        response.encoding = "gbk"
        if response.status_code != 200:
            raise RuntimeError(f"tencent_http_status:{response.status_code}")
        return response.text

    @classmethod
    def _request_tencent_batch_with_retry(
        cls, symbols: Sequence[str], *, session: requests.Session, started: float
    ) -> str:
        last_error: Optional[BaseException] = None
        for attempt in range(2):
            cls._check_budget(started)
            try:
                return cls._request_tencent_batch(symbols, session=session)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                last_error = exc
                if attempt >= 1:
                    break
                time.sleep(_QUOTE_RETRY_BACKOFF_SECONDS)
        if last_error is not None:
            raise last_error
        raise RuntimeError("tencent_retry_exhausted")

    @classmethod
    def _parse_tencent_response(
        cls, text: str, *, expected_symbols: Sequence[str], effective_date: date
    ) -> Dict[str, Dict[str, Any]]:
        expected = set(expected_symbols)
        parsed: Dict[str, Dict[str, Any]] = {}
        seen_payloads = set()
        records = _TENCENT_RECORD_RE.findall(text or "")
        if not records:
            raise ValueError("tencent_empty_response")
        for payload_symbol, payload in records:
            if payload_symbol in seen_payloads:
                raise ValueError("tencent_duplicate_symbol")
            seen_payloads.add(payload_symbol)
            if payload_symbol not in expected:
                raise ValueError("tencent_unexpected_symbol")
            fields = payload.split("~")
            quote = cls._parse_tencent_fields(payload_symbol, fields, effective_date=effective_date)
            if quote is not None:
                parsed[payload_symbol] = quote
        return parsed

    @staticmethod
    def _parse_tencent_fields(
        requested_symbol: str, fields: Sequence[str], *, effective_date: date
    ) -> Optional[Dict[str, Any]]:
        from data_provider.akshare_fetcher import _normalize_tencent_volume, _parse_tencent_amount
        from data_provider.realtime_types import safe_float

        if len(fields) < 45:
            return None
        code = normalize_stock_code(fields[2] if len(fields) > 2 else "")
        if requested_symbol[2:] != code:
            raise ValueError("tencent_code_mismatch")
        price = safe_float(fields[3])
        pre_close = safe_float(fields[4])
        change_pct = safe_float(fields[32]) if len(fields) > 32 else None
        if price is None or pre_close is None or change_pct is None:
            return None
        if price <= 0 or pre_close <= 0:
            return None
        if not all(math.isfinite(float(value)) for value in (price, pre_close, change_pct)):
            return None
        provider_timestamp = CsrcBoardConstituentProvider._parse_provider_timestamp(
            fields[30] if len(fields) > 30 else ""
        )
        if provider_timestamp is None:
            return None
        now_cn = datetime.now(_CN_TZ)
        if provider_timestamp > now_cn:
            return None
        if provider_timestamp.date() != effective_date:
            return None
        if not CsrcBoardConstituentProvider._provider_timestamp_fresh_for_now(provider_timestamp, now_cn):
            return None
        amount = _parse_tencent_amount(list(fields))
        volume = _normalize_tencent_volume(list(fields))
        volume_ratio = safe_float(fields[49]) if len(fields) > 49 and fields[49] else None
        return {
            "price": float(price),
            "pre_close": float(pre_close),
            "change_pct": float(change_pct),
            "amount": amount,
            "volume": volume,
            "volume_ratio": volume_ratio,
            "provider_timestamp": provider_timestamp.isoformat(),
        }

    @staticmethod
    def _ranking_row(industry: _Industry) -> Dict[str, Any]:
        return {
            "name": industry.name,
            "change_pct": industry.change_pct,
            "code": f"csrc_{industry.code}",
            "source": CsrcBoardConstituentProvider.provider_name,
            "as_of": industry.as_of.isoformat(),
            "aggregation": "equal_weight_member_return",
        }

    @staticmethod
    def _full_ranking_row(industry: _Industry) -> Dict[str, Any]:
        theme_code = f"csrc_{industry.code}"
        return {
            "theme_key": f"industry:{industry.name}",
            "theme_type": "industry",
            "theme_name": industry.name,
            "theme_code": theme_code,
            "theme_source": CsrcBoardConstituentProvider.provider_name,
            "theme_taxonomy": "csrc_industry",
            "rank": None,
            "change_pct": float(industry.change_pct),
            "as_of": industry.as_of.isoformat(),
            "member_count": len(industry.members),
            "aggregation": "equal_weight_member_return",
            "source": CsrcBoardConstituentProvider.provider_name,
        }

    @staticmethod
    def _full_rankings_failure(market: str, board_type: str, reason: str) -> Dict[str, Any]:
        return {
            "status": "failed",
            "market": market,
            "source": CsrcBoardConstituentProvider.provider_name,
            "taxonomy": "csrc_industry",
            "board_type": board_type,
            "themes": [],
            "coverage": {"status": "failed", "reason": reason},
            "snapshot_hash": "",
            "reason": reason,
        }

    @staticmethod
    def _member_to_constituent(member: _Member) -> BoardConstituent:
        return BoardConstituent(
            market="cn",
            exchange=member.exchange,
            code=member.code,
            symbol=f"{member.code}.{member.exchange.upper()}",
            name=member.name,
            raw={
                "涨跌幅": member.quote["change_pct"],
                "成交额": member.quote.get("amount"),
                "量比": member.quote.get("volume_ratio"),
                "price": member.quote["price"],
                "provider_timestamp": member.quote["provider_timestamp"],
                "industry_provenance": {
                    "source": "baostock_csrc",
                    "industry": f"{member.csrc_code}{member.csrc_name}",
                    "industryClassification": "证监会行业分类",
                    "industry_code": member.csrc_code,
                    "industry_name": member.csrc_name,
                    "classification_update_date": member.update_date.isoformat(),
                    "quote_source": "tencent",
                    "aggregation": "equal_weight_member_return",
                },
            },
        )

    @staticmethod
    def _fail(
        *,
        market: str,
        board_type: str,
        board_name: str,
        board_code: Optional[str],
        as_of: date,
        fetched_at: datetime,
        errors: List[str],
    ) -> BoardConstituentSnapshot:
        return BoardConstituentSnapshot(
            market=market,
            board_type=board_type,
            board_name=board_name,
            board_code=board_code,
            as_of=as_of,
            provider=CsrcBoardConstituentProvider.provider_name,
            fetched_at=fetched_at,
            status="failed",
            schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
            constituents=[],
            errors=errors,
            source_columns=[],
            snapshot_hash="",
        )

    @staticmethod
    def _parse_baostock_code(value: str) -> Tuple[Optional[str], Optional[str]]:
        text = str(value or "").strip().lower()
        if "." not in text:
            return None, None
        exchange, code = text.split(".", 1)
        if exchange not in {"sh", "sz"}:
            return None, None
        code = normalize_stock_code(code)
        if not code.isdigit() or len(code) != 6:
            return None, None
        inferred = AkshareBoardConstituentProvider._exchange_for_cn_code(code)
        if inferred != exchange:
            return None, None
        return exchange, code

    @staticmethod
    def _parse_date(value: Any) -> Optional[date]:
        try:
            return datetime.strptime(str(value or "").strip(), "%Y-%m-%d").date()
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_provider_timestamp(value: Any) -> Optional[datetime]:
        try:
            return datetime.strptime(str(value or "").strip(), "%Y%m%d%H%M%S").replace(tzinfo=_CN_TZ)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _effective_session_date() -> date:
        today = datetime.now(_CN_TZ).date()
        now_time = datetime.now(_CN_TZ).time()
        try:
            if is_market_open("cn", today, strict=True) and now_time >= datetime.strptime("09:30:00", "%H:%M:%S").time():
                return today
        except Exception as exc:
            raise RuntimeError("cn_trading_calendar_unavailable") from exc
        effective = get_effective_trading_date("cn")
        CsrcBoardConstituentProvider._validate_effective_session_date(effective, today=today, now_time=now_time)
        return effective

    @staticmethod
    def _validate_effective_session_date(effective: date, *, today: date, now_time: Any) -> None:
        if effective > today:
            raise RuntimeError("cn_trading_calendar_unavailable")
        try:
            if not is_market_open("cn", effective, strict=True):
                raise RuntimeError("cn_trading_calendar_unavailable")
        except Exception as exc:
            raise RuntimeError("cn_trading_calendar_unavailable") from exc
        if effective == today and now_time < datetime.strptime("15:00:00", "%H:%M:%S").time():
            raise RuntimeError("cn_trading_calendar_unavailable")

    @staticmethod
    def _check_budget(started: float) -> None:
        if time.monotonic() - started > _MAX_BUILD_SECONDS:
            raise TimeoutError("csrc_tencent_build_timeout")

    @staticmethod
    def _provider_timestamp_fresh_for_now(provider_timestamp: datetime, now_cn: datetime) -> bool:
        quote_time = provider_timestamp.time()
        now_time = now_cn.time()
        if provider_timestamp.date() != now_cn.date():
            return True
        morning_open = datetime.strptime("09:30:00", "%H:%M:%S").time()
        morning_close = datetime.strptime("11:30:00", "%H:%M:%S").time()
        afternoon_open = datetime.strptime("13:00:00", "%H:%M:%S").time()
        continuous_close = datetime.strptime("14:57:00", "%H:%M:%S").time()
        close_quote_time = datetime.strptime("15:00:00", "%H:%M:%S").time()
        if morning_open <= now_time < morning_close or afternoon_open <= now_time < continuous_close:
            return (now_cn - provider_timestamp).total_seconds() <= _INTRADAY_QUOTE_MAX_AGE_SECONDS
        if morning_close <= now_time < afternoon_open:
            return quote_time >= datetime.strptime("11:25:00", "%H:%M:%S").time()
        if now_time >= continuous_close:
            return quote_time >= close_quote_time
        return True

    @classmethod
    def _snapshot_quotes_fresh_for_now(cls, snapshot: _Snapshot) -> bool:
        now_cn = datetime.now(_CN_TZ)
        if snapshot.as_of != cls._effective_session_date():
            return False
        for industry in snapshot.industries.values():
            for member in industry.members:
                provider_timestamp = cls._parse_provider_timestamp_from_iso(member.quote.get("provider_timestamp"))
                if provider_timestamp is None or not cls._provider_timestamp_fresh_for_now(provider_timestamp, now_cn):
                    return False
        return True

    @staticmethod
    def _parse_provider_timestamp_from_iso(value: Any) -> Optional[datetime]:
        try:
            parsed = datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=_CN_TZ)
        return parsed.astimezone(_CN_TZ)


__all__ = ["CsrcBoardConstituentProvider"]
