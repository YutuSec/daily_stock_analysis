# -*- coding: utf-8 -*-
"""Provider-neutral A-share board constituent snapshots backed by AKShare."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from data_provider.base import normalize_stock_code

logger = logging.getLogger(__name__)

BOARD_CONSTITUENT_SCHEMA_VERSION = "board-constituents-v1"
SUPPORTED_BOARD_TYPES = {"concept", "industry"}


@dataclass(frozen=True)
class BoardConstituent:
    """One exact A-share constituent identity from a board snapshot."""

    market: str
    exchange: str
    code: str
    symbol: str
    name: str
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "market": self.market,
            "exchange": self.exchange,
            "code": self.code,
            "symbol": self.symbol,
            "name": self.name,
            "raw": dict(self.raw),
        }


@dataclass(frozen=True)
class BoardConstituentSnapshot:
    """As-of, source-attributed board constituent snapshot."""

    market: str
    board_type: str
    board_name: str
    board_code: Optional[str]
    as_of: date
    provider: str
    fetched_at: datetime
    status: str
    schema_version: str
    constituents: List[BoardConstituent] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    source_columns: List[str] = field(default_factory=list)
    snapshot_hash: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "market": self.market,
            "board_type": self.board_type,
            "board_name": self.board_name,
            "board_code": self.board_code,
            "as_of": self.as_of.isoformat(),
            "provider": self.provider,
            "fetched_at": self.fetched_at.isoformat(),
            "status": self.status,
            "schema_version": self.schema_version,
            "constituents": [item.to_dict() for item in self.constituents],
            "errors": list(self.errors),
            "source_columns": list(self.source_columns),
            "snapshot_hash": self.snapshot_hash,
        }


@dataclass(frozen=True)
class BoardDailyBar:
    date: date
    close: float
    change_pct: Optional[float]
    amount: Optional[float]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date.isoformat(),
            "close": self.close,
            "change_pct": self.change_pct,
            "amount": self.amount,
        }


@dataclass(frozen=True)
class BoardHistorySnapshot:
    market: str
    board_type: str
    board_name: str
    as_of: date
    provider: str
    fetched_at: datetime
    status: str
    schema_version: str
    bars: List[BoardDailyBar] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "market": self.market,
            "board_type": self.board_type,
            "board_name": self.board_name,
            "as_of": self.as_of.isoformat(),
            "provider": self.provider,
            "fetched_at": self.fetched_at.isoformat(),
            "status": self.status,
            "schema_version": self.schema_version,
            "bars": [bar.to_dict() for bar in self.bars],
            "errors": list(self.errors),
        }


class AkshareBoardConstituentProvider:
    """Read EastMoney board constituents through AKShare without raw HTTP duplication."""

    def __init__(
        self,
        *,
        cache_ttl_seconds: int = 900,
        min_interval_seconds: float = 0.3,
        max_attempts: int = 2,
        retry_backoff_seconds: float = 0.5,
    ) -> None:
        self.cache_ttl_seconds = max(0, int(cache_ttl_seconds))
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self.max_attempts = max(1, min(int(max_attempts), 3))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self._cache: Dict[Tuple[str, str, str, str, str], Tuple[float, BoardConstituentSnapshot]] = {}
        self._history_cache: Dict[Tuple[str, str, str, str], Tuple[float, BoardHistorySnapshot]] = {}
        self._last_call_at = 0.0

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
        snapshot_date = as_of or date.today()
        fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)

        if normalized_market != "cn":
            return self._fail(
                market=normalized_market,
                board_type=normalized_type,
                board_name=normalized_name,
                board_code=normalized_code,
                as_of=snapshot_date,
                fetched_at=fetched_at,
                errors=["market_not_supported"],
            )
        if normalized_type not in SUPPORTED_BOARD_TYPES:
            return self._fail(
                market=normalized_market,
                board_type=normalized_type,
                board_name=normalized_name,
                board_code=normalized_code,
                as_of=snapshot_date,
                fetched_at=fetched_at,
                errors=["board_type_not_supported"],
            )
        if not normalized_name:
            return self._fail(
                market=normalized_market,
                board_type=normalized_type,
                board_name=normalized_name,
                board_code=normalized_code,
                as_of=snapshot_date,
                fetched_at=fetched_at,
                errors=["board_name_required"],
            )

        cache_key = (
            normalized_market,
            normalized_type,
            normalized_name,
            normalized_code or "",
            snapshot_date.isoformat(),
        )
        now = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached and cached[0] > now:
            return cached[1]

        last_error: Optional[Exception] = None
        df: Optional[pd.DataFrame] = None
        fetch_symbol = (
            normalized_code
            if normalized_code and normalized_code.upper().startswith("BK")
            else normalized_name
        )
        for attempt in range(1, self.max_attempts + 1):
            try:
                df = self._fetch(normalized_type, fetch_symbol)
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                if attempt < self.max_attempts and self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * attempt)
        if last_error is not None:
            exc = last_error
            logger.warning("AKShare board constituent fetch failed: %s", exc)
            snapshot = self._fail(
                market=normalized_market,
                board_type=normalized_type,
                board_name=normalized_name,
                board_code=normalized_code,
                as_of=snapshot_date,
                fetched_at=fetched_at,
                errors=[f"provider_error:{type(exc).__name__}"],
            )
            self._store_cache(cache_key, snapshot)
            return snapshot

        source_columns = [str(column) for column in getattr(df, "columns", [])]
        if df is None or df.empty:
            snapshot = self._fail(
                market=normalized_market,
                board_type=normalized_type,
                board_name=normalized_name,
                board_code=normalized_code,
                as_of=snapshot_date,
                fetched_at=fetched_at,
                errors=["empty_snapshot"],
                source_columns=source_columns,
            )
            self._store_cache(cache_key, snapshot)
            return snapshot

        constituents, errors = self._normalize_rows(df)
        status = "ok" if constituents and not errors else ("partial" if constituents else "failed")
        snapshot_hash = self._hash_snapshot(
            normalized_market,
            normalized_type,
            normalized_name,
            normalized_code,
            snapshot_date,
            constituents,
        )
        snapshot = BoardConstituentSnapshot(
            market=normalized_market,
            board_type=normalized_type,
            board_name=normalized_name,
            board_code=normalized_code,
            as_of=snapshot_date,
            provider="akshare",
            fetched_at=fetched_at,
            status=status,
            schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
            constituents=constituents,
            errors=errors,
            source_columns=source_columns,
            snapshot_hash=snapshot_hash,
        )
        self._store_cache(cache_key, snapshot)
        return snapshot

    def get_board_history(
        self,
        *,
        market: str = "cn",
        board_type: str,
        board_name: str,
        as_of: Optional[date] = None,
        lookback_days: int = 10,
    ) -> BoardHistorySnapshot:
        """Return complete daily board bars ending at ``as_of``; never includes future rows."""
        normalized_market = str(market or "cn").strip().lower() or "cn"
        normalized_type = str(board_type or "").strip().lower()
        normalized_name = str(board_name or "").strip()
        snapshot_date = as_of or date.today()
        fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)
        errors: List[str] = []
        if normalized_market != "cn":
            errors.append("market_not_supported")
        if normalized_type not in SUPPORTED_BOARD_TYPES:
            errors.append("board_type_not_supported")
        if not normalized_name:
            errors.append("board_name_required")
        if errors:
            return BoardHistorySnapshot(
                market=normalized_market,
                board_type=normalized_type,
                board_name=normalized_name,
                as_of=snapshot_date,
                provider="akshare",
                fetched_at=fetched_at,
                status="failed",
                schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
                errors=errors,
            )

        safe_lookback = max(5, min(int(lookback_days or 10), 120))
        cache_key = (normalized_market, normalized_type, normalized_name, snapshot_date.isoformat())
        now = time.monotonic()
        cached = self._history_cache.get(cache_key)
        if cached and cached[0] > now:
            return cached[1]

        frame: Optional[pd.DataFrame] = None
        last_error: Optional[Exception] = None
        start_date = snapshot_date - timedelta(days=max(20, safe_lookback * 3))
        for attempt in range(1, self.max_attempts + 1):
            try:
                frame = self._fetch_history(
                    normalized_type,
                    normalized_name,
                    start_date=start_date,
                    end_date=snapshot_date,
                )
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                if attempt < self.max_attempts and self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * attempt)
        if last_error is not None:
            snapshot = BoardHistorySnapshot(
                market=normalized_market,
                board_type=normalized_type,
                board_name=normalized_name,
                as_of=snapshot_date,
                provider="akshare",
                fetched_at=fetched_at,
                status="failed",
                schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
                errors=[f"provider_error:{type(last_error).__name__}"],
            )
            self._store_history_cache(cache_key, snapshot)
            return snapshot

        bars, normalization_errors = self._normalize_history_rows(frame, as_of=snapshot_date)
        bars = bars[-safe_lookback:]
        status = "ok" if len(bars) >= 6 and not normalization_errors else (
            "partial" if bars else "failed"
        )
        snapshot = BoardHistorySnapshot(
            market=normalized_market,
            board_type=normalized_type,
            board_name=normalized_name,
            as_of=snapshot_date,
            provider="akshare",
            fetched_at=fetched_at,
            status=status,
            schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
            bars=bars,
            errors=normalization_errors,
        )
        self._store_history_cache(cache_key, snapshot)
        return snapshot

    def _fetch(self, board_type: str, board_name: str) -> pd.DataFrame:
        import akshare as ak

        elapsed = time.monotonic() - self._last_call_at
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)
        self._last_call_at = time.monotonic()
        if board_type == "industry":
            fetch = ak.stock_board_industry_cons_em
        else:
            fetch = ak.stock_board_concept_cons_em
        from data_provider.akshare_fetcher import _akshare_call_with_timeout
        return _akshare_call_with_timeout(fetch, symbol=board_name, timeout=8.0, call_name="board_constituents")

    def _fetch_history(
        self,
        board_type: str,
        board_name: str,
        *,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        import akshare as ak

        elapsed = time.monotonic() - self._last_call_at
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)
        self._last_call_at = time.monotonic()
        common = {
            "symbol": board_name,
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
            "adjust": "",
        }
        if board_type == "industry":
            return ak.stock_board_industry_hist_em(period="日k", **common)
        return ak.stock_board_concept_hist_em(period="daily", **common)

    def _store_cache(
        self,
        cache_key: Tuple[str, str, str, str, str],
        snapshot: BoardConstituentSnapshot,
    ) -> None:
        if self.cache_ttl_seconds > 0:
            self._cache[cache_key] = (time.monotonic() + self.cache_ttl_seconds, snapshot)

    def _store_history_cache(
        self,
        cache_key: Tuple[str, str, str, str],
        snapshot: BoardHistorySnapshot,
    ) -> None:
        if self.cache_ttl_seconds > 0:
            self._history_cache[cache_key] = (time.monotonic() + self.cache_ttl_seconds, snapshot)

    @classmethod
    def _normalize_history_rows(
        cls,
        frame: Optional[pd.DataFrame],
        *,
        as_of: date,
    ) -> Tuple[List[BoardDailyBar], List[str]]:
        if frame is None or frame.empty:
            return [], ["empty_history"]
        errors: List[str] = []
        by_date: Dict[date, BoardDailyBar] = {}
        for _, row in frame.iterrows():
            raw_date = cls._first_present(row, ("日期", "date"))
            raw_close = cls._first_present(row, ("收盘", "close"))
            try:
                bar_date = pd.to_datetime(raw_date).date()
                close = float(raw_close)
            except (TypeError, ValueError):
                errors.append("invalid_history_row")
                continue
            if bar_date > as_of:
                errors.append(f"future_history_row:{bar_date.isoformat()}")
                continue
            if not math.isfinite(close) or close <= 0:
                errors.append(f"invalid_history_close:{bar_date.isoformat()}")
                continue
            change_pct = cls._optional_finite_float(cls._first_present(row, ("涨跌幅", "pct_chg")))
            amount = cls._optional_finite_float(cls._first_present(row, ("成交额", "amount")))
            current = BoardDailyBar(bar_date, close, change_pct, amount)
            previous = by_date.get(bar_date)
            if previous is not None and previous != current:
                errors.append(f"conflicting_history_row:{bar_date.isoformat()}")
                by_date.pop(bar_date, None)
                continue
            by_date[bar_date] = current
        return [by_date[key] for key in sorted(by_date)], errors

    @staticmethod
    def _optional_finite_float(value: Any) -> Optional[float]:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if math.isfinite(parsed) else None

    @classmethod
    def _normalize_rows(cls, df: pd.DataFrame) -> Tuple[List[BoardConstituent], List[str]]:
        errors: List[str] = []
        constituents: List[BoardConstituent] = []
        seen = set()
        for _, row in df.iterrows():
            raw = {str(k): cls._jsonable(v) for k, v in row.to_dict().items()}
            code_raw = cls._first_present(row, ("代码", "股票代码", "code", "证券代码"))
            name_raw = cls._first_present(row, ("名称", "股票名称", "name", "证券简称"))
            code = normalize_stock_code(str(code_raw or "").strip())
            name = str(name_raw or "").strip()
            exchange = cls._exchange_for_cn_code(code)
            if not code or not code.isdigit() or len(code) != 6 or exchange is None:
                errors.append(f"ambiguous_symbol:{code_raw}")
                continue
            identity = ("cn", exchange, code)
            if identity in seen:
                errors.append(f"duplicate_symbol:{exchange}{code}")
                continue
            seen.add(identity)
            constituents.append(
                BoardConstituent(
                    market="cn",
                    exchange=exchange,
                    code=code,
                    symbol=f"{code}.{exchange.upper()}",
                    name=name,
                    raw=raw,
                )
            )
        return constituents, errors

    @staticmethod
    def _first_present(row: Any, names: Tuple[str, ...]) -> Any:
        for name in names:
            if name in row and row[name] is not None:
                value = row[name]
                try:
                    if pd.isna(value):
                        continue
                except Exception:
                    pass
                text = str(value).strip()
                if text:
                    return value
        return None

    @staticmethod
    def _exchange_for_cn_code(code: str) -> Optional[str]:
        if not code.isdigit() or len(code) != 6:
            return None
        if code.startswith(("60", "68", "51", "52", "56", "58", "90", "50", "11")):
            return "sh"
        if code.startswith(("00", "30", "15", "16", "18", "12", "20")):
            return "sz"
        return None

    @staticmethod
    def _jsonable(value: Any) -> Any:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if isinstance(value, (str, int, bool)):
            return value
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        return str(value)

    @staticmethod
    def _hash_snapshot(
        market: str,
        board_type: str,
        board_name: str,
        board_code: Optional[str],
        as_of: date,
        constituents: List[BoardConstituent],
    ) -> str:
        payload = {
            "market": market,
            "board_type": board_type,
            "board_name": board_name,
            "board_code": board_code,
            "as_of": as_of.isoformat(),
            "constituents": [
                {"exchange": item.exchange, "code": item.code, "name": item.name}
                for item in constituents
            ],
        }
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

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
        source_columns: Optional[List[str]] = None,
    ) -> BoardConstituentSnapshot:
        return BoardConstituentSnapshot(
            market=market,
            board_type=board_type,
            board_name=board_name,
            board_code=board_code,
            as_of=as_of,
            provider="akshare",
            fetched_at=fetched_at,
            status="failed",
            schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
            constituents=[],
            errors=list(errors),
            source_columns=list(source_columns or []),
            snapshot_hash="",
        )
