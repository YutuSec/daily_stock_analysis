# -*- coding: utf-8 -*-
"""Exact Sina board snapshots using bounded HTTPS requests (no taxonomy remap)."""

from __future__ import annotations

import logging
import json
import math
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from data_provider.akshare_board_provider import (
    BOARD_CONSTITUENT_SCHEMA_VERSION,
    SUPPORTED_BOARD_TYPES,
    AkshareBoardConstituentProvider,
    BoardConstituent,
    BoardConstituentSnapshot,
)
from data_provider.base import normalize_stock_code

logger = logging.getLogger(__name__)


def _fetch_csrc_industries() -> List[Dict[str, str]]:
    """Read BaoStock's independent CSRC classification in an isolated process.

    Use the native cursor: older BaoStock get_data() uses removed pandas.append.
    The caller bounds the complete process, including login/query/logout.
    """
    import baostock as bs
    import baostock.common.contants as bs_constants
    import socket

    # Runs only in the isolated worker. A single bounded page avoids the SDK's
    # anonymous-session pagination failures; a full page is rejected below so
    # a growing universe cannot be silently truncated.
    bs_constants.BAOSTOCK_PER_PAGE_COUNT = 10000
    # The public full-universe query takes ~36s server-side in live probes.
    # Bound individual socket I/O and the complete worker separately; a 5s
    # socket / 20s worker deadline rejects valid complete responses.
    socket.setdefaulttimeout(45.0)
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError("classification_login_failed")
    try:
        result = bs.query_stock_industry(date=datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat())
        if result.error_code != "0":
            raise RuntimeError("classification_query_failed")
        expected_count = len(result.data)
        fields = list(result.fields)
        required = {"updateDate", "code", "code_name", "industry", "industryClassification"}
        if not required.issubset(fields) or len(set(fields)) != len(fields):
            raise ValueError("classification_schema_mismatch")
        if expected_count >= bs_constants.BAOSTOCK_PER_PAGE_COUNT:
            raise ValueError("classification_row_limit_exceeded")
        rows = []
        while result.next():
            values = result.get_row_data()
            if len(values) != len(fields):
                raise ValueError("classification_schema_mismatch")
            rows.append(dict(zip(fields, values)))
            if len(rows) > 10000:
                raise ValueError("classification_row_limit_exceeded")
        # With a short single page next() performs no network pagination.
        # Still verify the complete decoded page was consumed by the cursor.
        if result.error_code != "0" or not rows or len(rows) != expected_count:
            raise ValueError("classification_response_incomplete")
        return rows
    finally:
        bs.logout()


def normalize_board_display_name(value: Any) -> str:
    """Normalize provider display names for exact equality only."""

    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", "", text)
    return text


class SinaConceptBoardConstituentProvider:
    """Read one Sina taxonomy; the historical class name remains compatible."""

    # Stable ledger source identifier; transport is now native Sina HTTPS.
    # Retain it so existing same-source observations remain comparable.
    provider_name = "akshare_sina"

    def __init__(
        self,
        *,
        cache_ttl_seconds: int = 900,
        min_interval_seconds: float = 0.3,
        max_attempts: int = 2,
        retry_backoff_seconds: float = 0.5,
        board_type: str = "concept",
    ) -> None:
        self.cache_ttl_seconds = max(0, int(cache_ttl_seconds))
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self.max_attempts = max(1, min(int(max_attempts), 3))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        if board_type not in SUPPORTED_BOARD_TYPES:
            raise ValueError("unsupported Sina board type")
        self.board_type = board_type
        self._cache: Dict[Tuple[str, str, str, str, str], Tuple[float, BoardConstituentSnapshot]] = {}
        self._directory_cache: Optional[Tuple[float, Dict[str, Dict[str, Any]]]] = None
        self._classification_cache: Optional[Tuple[float, List[Dict[str, str]]]] = None
        self._classification_retry_at = 0.0
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
        # Live membership is not a historical membership API. Never backdate it.
        snapshot_date = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)

        errors: List[str] = []
        if normalized_market != "cn":
            errors.append("market_not_supported")
        if normalized_type not in SUPPORTED_BOARD_TYPES:
            errors.append("board_type_not_supported")
        if normalized_type != self.board_type:
            errors.append("sina_taxonomy_mismatch")
        if as_of is not None and as_of != snapshot_date:
            errors.append("historical_membership_unavailable")
        if not normalized_name:
            errors.append("board_name_required")
        if errors:
            return self._fail(
                market=normalized_market,
                board_type=normalized_type,
                board_name=normalized_name,
                board_code=normalized_code,
                as_of=snapshot_date,
                fetched_at=fetched_at,
                errors=errors,
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

        sector: Optional[Dict[str, Any]] = None
        frame: Optional[pd.DataFrame] = None
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                sector = self._lookup_exact_sector(normalized_name)
                if sector is None:
                    snapshot = self._fail(
                        market=normalized_market,
                        board_type=normalized_type,
                        board_name=normalized_name,
                        board_code=normalized_code,
                        as_of=snapshot_date,
                        fetched_at=fetched_at,
                        errors=["exact_board_name_not_found"],
                    )
                    self._store_cache(cache_key, snapshot)
                    return snapshot
                if normalized_code and normalized_code != sector["sector"]:
                    return self._fail(
                        market=normalized_market, board_type=normalized_type,
                        board_name=normalized_name, board_code=normalized_code,
                        as_of=snapshot_date, fetched_at=fetched_at,
                        errors=["board_code_name_mismatch"],
                    )
                frame = self._fetch_detail(str(sector["sector"]))
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                if attempt < self.max_attempts and self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * attempt)
        if last_error is not None:
            logger.warning("Sina %s board constituent fetch failed: %s", self.board_type, last_error)
            snapshot = self._fail(
                market=normalized_market,
                board_type=normalized_type,
                board_name=normalized_name,
                board_code=normalized_code,
                as_of=snapshot_date,
                fetched_at=fetched_at,
                errors=[f"provider_error:{type(last_error).__name__}"],
            )
            self._store_cache(cache_key, snapshot)
            return snapshot

        source_columns = [str(column) for column in getattr(frame, "columns", [])]
        constituents, row_errors = self._normalize_rows(frame)
        if normalized_type == "industry" and constituents:
            validation_errors = self._validate_industry_membership(normalized_name, constituents, snapshot_date)
            if validation_errors:
                snapshot = self._fail(
                    market=normalized_market, board_type=normalized_type,
                    board_name=normalized_name, board_code=str(sector["sector"]),
                    as_of=snapshot_date, fetched_at=fetched_at,
                    errors=validation_errors + row_errors, source_columns=source_columns,
                )
                self._store_cache(cache_key, snapshot)
                return snapshot
        if not constituents:
            snapshot = self._fail(
                market=normalized_market,
                board_type=normalized_type,
                board_name=normalized_name,
                board_code=normalized_code,
                as_of=snapshot_date,
                fetched_at=fetched_at,
                errors=row_errors or ["empty_snapshot"],
                source_columns=source_columns,
            )
            self._store_cache(cache_key, snapshot)
            return snapshot

        snapshot = BoardConstituentSnapshot(
            market=normalized_market,
            board_type=normalized_type,
            board_name=normalized_name,
            board_code=str(sector.get("sector")) if sector else normalized_code,
            as_of=snapshot_date,
            provider=self.provider_name,
            fetched_at=fetched_at,
            status="ok" if not row_errors else "partial",
            schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
            constituents=constituents,
            errors=row_errors,
            source_columns=source_columns,
            snapshot_hash=AkshareBoardConstituentProvider._hash_snapshot(
                normalized_market,
                normalized_type,
                normalized_name,
                str(sector.get("sector")) if sector else normalized_code,
                snapshot_date,
                constituents,
            ),
        )
        self._store_cache(cache_key, snapshot)
        return snapshot

    def _validate_industry_membership(
        self, board_name: str, constituents: List[BoardConstituent], as_of: date,
    ) -> List[str]:
        """Do not trust a source's matching label when the member industry differs."""
        try:
            rows = self._csrc_classifications()
        except Exception as exc:
            logger.warning("Independent industry classification unavailable: %s", type(exc).__name__)
            return ["taxonomy_semantic_unverified", f"classification_provider_error:{type(exc).__name__}"]
        by_code: Dict[str, List[Dict[str, str]]] = {}
        for row in rows:
            by_code.setdefault(str(row.get("code") or "").strip().lower(), []).append(row)
        for item in constituents:
            matches = by_code.get(f"{item.exchange}.{item.code}", [])
            if len(matches) != 1:
                return ["taxonomy_semantic_unverified", f"classification_identity_unavailable:{item.symbol}"]
            row = matches[0]
            try:
                updated = date.fromisoformat(str(row.get("updateDate") or ""))
            except ValueError:
                return ["taxonomy_semantic_unverified", "classification_date_unavailable"]
            if not as_of - timedelta(days=31) <= updated <= as_of:
                return ["taxonomy_semantic_unverified", "classification_date_stale_or_future"]
            if row.get("industryClassification") != "证监会行业分类":
                return ["taxonomy_semantic_unverified", "classification_standard_mismatch"]
            # BaoStock returns a CSRC code followed by the exact industry name,
            # e.g. C27医药制造业. The code is metadata, not part of the label.
            classification = str(row.get("industry") or "").strip()
            if not classification:
                return ["taxonomy_semantic_unverified", f"classification_industry_unavailable:{item.symbol}"]
            match = re.fullmatch(r"([A-Z]\d{2})(.+)", classification)
            industry_name = match.group(2) if match else classification
            if normalize_board_display_name(industry_name) != normalize_board_display_name(board_name):
                return ["taxonomy_member_industry_mismatch", f"classification_member:{item.symbol}"]
            item.raw["industry_validation"] = {
                "provider": "baostock", "standard": row["industryClassification"],
                "industry": industry_name, "industry_code": match.group(1) if match else None,
                "updated_at": updated.isoformat(),
            }
        return []

    def _csrc_classifications(self) -> List[Dict[str, str]]:
        now = time.monotonic()
        if self._classification_cache and self._classification_cache[0] > now:
            return self._classification_cache[1]
        if now < self._classification_retry_at:
            raise RuntimeError("classification_retry_cooldown")
        from data_provider.akshare_fetcher import _akshare_call_with_timeout
        try:
            rows = _akshare_call_with_timeout(_fetch_csrc_industries, timeout=60.0, call_name="baostock_industry")
        except Exception:
            self._classification_retry_at = time.monotonic() + 30.0
            raise
        self._classification_cache = (time.monotonic() + self.cache_ttl_seconds, rows)
        return rows

    def _lookup_exact_sector(self, board_name: str) -> Optional[Dict[str, Any]]:
        target = normalize_board_display_name(board_name)
        if not target:
            return None
        directory = self._sector_directory()
        return directory.get(target)

    def _sector_directory(self) -> Dict[str, Dict[str, Any]]:
        now = time.monotonic()
        if self._directory_cache and self._directory_cache[0] > now:
            return self._directory_cache[1]
        frame = self._fetch_spot_directory()
        directory: Dict[str, Dict[str, Any]] = {}
        ambiguous_names = set()
        for _, row in frame.iterrows():
            row_dict = {str(key): self._jsonable(value) for key, value in row.to_dict().items()}
            name = self._first_present(row, ("板块", "板块名称", "name", "label", "名称"))
            sector = self._first_present(row, ("label", "sector", "板块代码", "代码"))
            normalized = normalize_board_display_name(name)
            if not normalized or not sector or normalized in ambiguous_names:
                continue
            existing = directory.get(normalized)
            candidate = {"name": str(name).strip(), "sector": str(sector).strip(), "raw": row_dict}
            prefix = "hangye_" if self.board_type == "industry" else "gn_"
            if not candidate["sector"].startswith(prefix):
                continue
            if existing is not None and existing.get("sector") != candidate["sector"]:
                directory.pop(normalized, None)
                ambiguous_names.add(normalized)
                continue
            directory[normalized] = candidate
        if self.cache_ttl_seconds > 0:
            self._directory_cache = (now + self.cache_ttl_seconds, directory)
        return directory

    def _fetch_spot_directory(self) -> pd.DataFrame:
        text = self._get_text(
            "https://money.finance.sina.com.cn/q/view/newFLJK.php",
            {"param": "industry" if self.board_type == "industry" else "class"},
        )
        start = text.find("{")
        if start < 0:
            raise ValueError("invalid_sina_directory")
        payload = json.loads(text[start:].strip().rstrip(";"))
        if not isinstance(payload, dict) or not payload:
            raise ValueError("empty_sina_directory")
        rows = []
        for value in payload.values():
            parts = str(value).split(",")
            if len(parts) < 13:
                raise ValueError("invalid_sina_directory_row")
            rows.append({"label": parts[0], "板块": parts[1]})
        return pd.DataFrame(rows)

    def _fetch_detail(self, sector: str) -> pd.DataFrame:
        # Sina's count endpoint can undercount. Read to an empty/short page,
        # reject repeated pages and a missing terminator rather than truncate.
        rows: List[Dict[str, Any]] = []
        seen = set()
        for page in range(1, 21):
            text = self._get_text(
                "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData",
                {"page": str(page), "num": "80", "sort": "symbol", "asc": "1",
                 "node": sector, "symbol": "", "_s_r_a": "page"},
            )
            data = json.loads(text)
            if not isinstance(data, list) or len(data) > 80:
                raise ValueError("invalid_sina_constituent_page")
            for row in data:
                if not isinstance(row, dict) or not row.get("symbol"):
                    raise ValueError("invalid_sina_constituent_row")
                symbol = str(row["symbol"])
                if symbol in seen:
                    raise ValueError("duplicate_sina_page_symbol")
                seen.add(symbol)
                rows.append(row)
            if len(data) < 80:
                return pd.DataFrame(rows)
        raise ValueError("sina_pagination_limit_exceeded")

    def _get_text(self, url: str, params: Dict[str, str]) -> str:
        self._throttle()
        response = requests.get(url, params=params, timeout=(3.0, 8.0))
        response.raise_for_status()
        # These public Sina endpoints return GBK/GB2312 Chinese labels.
        return response.content.decode("gb18030")

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_call_at
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)
        self._last_call_at = time.monotonic()

    @classmethod
    def _normalize_rows(cls, frame: Optional[pd.DataFrame]) -> Tuple[List[BoardConstituent], List[str]]:
        if frame is None or frame.empty:
            return [], ["empty_snapshot"]
        errors: List[str] = []
        constituents: List[BoardConstituent] = []
        seen = set()
        for _, row in frame.iterrows():
            raw = {str(key): cls._jsonable(value) for key, value in row.to_dict().items()}
            if "changepercent" in raw and "涨跌幅" not in raw:
                raw["涨跌幅"] = raw.get("changepercent")
            if "amount" in raw and "成交额" not in raw:
                raw["成交额"] = raw.get("amount")
            if "volume" in raw and "成交量" not in raw:
                raw["成交量"] = raw.get("volume")
            code_raw = cls._first_present(row, ("代码", "股票代码", "symbol", "code"))
            name_raw = cls._first_present(row, ("名称", "股票名称", "name", "证券简称"))
            code = normalize_stock_code(str(code_raw or "").strip())
            name = str(name_raw or "").strip()
            if re.fullmatch(r"bj\d{6}", str(raw.get("symbol") or "").lower()):
                errors.append(f"unsupported_exchange:bj:{code}")
                continue
            exchange = AkshareBoardConstituentProvider._exchange_for_cn_code(code)
            if not code or not code.isdigit() or len(code) != 6 or exchange is None:
                errors.append(f"ambiguous_symbol:{code_raw}")
                continue
            explicit = re.match(r"^(sh|sz|bj)(\d{6})$", str(raw.get("symbol") or "").lower())
            if explicit and (explicit.group(1) != exchange or explicit.group(2) != code):
                errors.append(f"symbol_exchange_mismatch:{code_raw}")
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

    def _store_cache(self, cache_key: Tuple[str, str, str, str, str], snapshot: BoardConstituentSnapshot) -> None:
        if self.cache_ttl_seconds > 0:
            ttl = self.cache_ttl_seconds if snapshot.status in {"ok", "partial"} else min(30, self.cache_ttl_seconds)
            self._cache[cache_key] = (time.monotonic() + ttl, snapshot)

    def _fail(
        self,
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
            provider=self.provider_name,
            fetched_at=fetched_at,
            status="failed",
            schema_version=BOARD_CONSTITUENT_SCHEMA_VERSION,
            constituents=[],
            errors=list(errors),
            source_columns=list(source_columns or []),
            snapshot_hash="",
        )
