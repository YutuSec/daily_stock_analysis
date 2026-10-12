# -*- coding: utf-8 -*-
"""
AkShare fundamental adapter (fail-open).

This adapter intentionally uses capability probing against multiple AkShare
endpoint candidates. It should never raise to caller; partial data is allowed.
"""

from __future__ import annotations

import logging
import copy
import re
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

_DIVIDEND_KEYWORD_MAP: Dict[str, List[str]] = {
    "per_share": [
        "每股派息",
        "每股现金红利",
        "每股分红",
        "每股派现",
        "派现(元/股)",
        "派息(元/股)",
        "税前派息(元/股)",
        "现金分红(税前)",
    ],
    "plan_text": [
        "分配方案",
        "分红方案",
        "实施方案",
        "派息方案",
        "方案",
        "预案",
        "方案说明",
    ],
    "ex_dividend_date": ["除权除息日", "除息日", "除权日", "除权除息", "除息日期"],
    "record_date": ["股权登记日", "登记日"],
    "announce_date": ["公告日期", "公告日", "实施公告日", "预案公告日"],
    "report_date": ["报告期", "报告日期", "截止日期", "统计截止日期"],
}

_REPORT_DATE_KEYWORDS = ["报告期", "报告日期", "截止日期", "统计截止日期", "report_date"]
_ANNOUNCE_DATE_KEYWORDS = ["公告日期", "公告日", "披露日期", "更新日期", "announce_date", "公告时间"]
_FLOW_DATE_KEYWORDS = ["交易日期", "日期", "date"]
_CURRENT_EARNINGS_MAX_AGE_DAYS = 540
_EARNINGS_PERIOD_PROBE_COUNT = 2
_FINANCIAL_REPORT_MAX_AGE_DAYS = 540
_INSTITUTION_PERIOD_PROBE_COUNT = 2


def _safe_float(value: Any) -> Optional[float]:
    """Best-effort float conversion."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    s = str(value).strip().replace(",", "").replace("%", "")
    if not s:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _safe_amount_float(value: Any) -> Optional[float]:
    """Best-effort numeric conversion, accepting common Chinese amount units."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return _safe_float(value)
    text = str(value).strip().replace(",", "")
    if not text or text in ("-", "nan", "None"):
        return None
    multiplier = 1.0
    if "万亿" in text:
        multiplier = 1_000_000_000_000.0
    elif "亿" in text:
        multiplier = 100_000_000.0
    elif "万" in text:
        multiplier = 10_000.0
    number = re.sub(r"[^0-9+\-.]", "", text)
    parsed = _safe_float(number)
    return parsed * multiplier if parsed is not None else None


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    try:
        parsed = pd.to_datetime(value)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    try:
        return parsed.to_pydatetime()
    except Exception:
        return None


def _normalize_code(raw: Any) -> str:
    s = _safe_str(raw).upper()
    if "." in s:
        s = s.split(".", 1)[0]
    s = re.sub(r"^(SH|SZ|BJ)", "", s)
    return s


def _stock_market_prefix(stock_code: str) -> str:
    code = _normalize_code(stock_code)
    if code.startswith(("6", "9")):
        return "sh"
    if code.startswith(("0", "2", "3")):
        return "sz"
    if code.startswith(("4", "8")):
        return "bj"
    return "sh"


def _stock_symbol_with_market(stock_code: str) -> str:
    code = _normalize_code(stock_code)
    return f"{_stock_market_prefix(code)}{code}"


def _pick_by_keywords(row: pd.Series, keywords: List[str]) -> Optional[Any]:
    """
    Return first non-empty row value whose column name contains any keyword.
    """
    for col in row.index:
        col_s = str(col)
        if any(k in col_s for k in keywords):
            val = row.get(col)
            if val is not None and str(val).strip() not in ("", "-", "nan", "None"):
                return val
    return None


def _pick_amount_by_keywords(row: pd.Series, required_keywords: List[str]) -> Optional[Any]:
    """Pick a numeric amount-like column, excluding ratio/percentage columns."""
    exact_preferred: List[Tuple[int, Any]] = []
    fuzzy_candidates: List[Tuple[int, Any]] = []
    for pos, col in enumerate(row.index):
        col_s = str(col)
        if any(bad in col_s for bad in ("占比", "比例", "比率", "占总", "净占比", "%")):
            continue
        if not any(keyword in col_s for keyword in required_keywords):
            continue
        val = row.get(col)
        if val is None or str(val).strip() in ("", "-", "nan", "None"):
            continue
        if "净额" in col_s or "净流入" in col_s:
            exact_preferred.append((pos, val))
        else:
            fuzzy_candidates.append((pos, val))
    if exact_preferred:
        exact_preferred.sort(key=lambda item: item[0])
        return exact_preferred[0][1]
    if fuzzy_candidates:
        fuzzy_candidates.sort(key=lambda item: item[0])
        return fuzzy_candidates[0][1]
    return None


def _pick_amount_column(columns: List[Any], required_keywords: List[str]) -> Optional[Any]:
    exact_preferred: List[Tuple[int, Any]] = []
    fuzzy_candidates: List[Tuple[int, Any]] = []
    for pos, col in enumerate(columns):
        col_s = str(col)
        if any(bad in col_s for bad in ("占比", "比例", "比率", "占总", "净占比", "%")):
            continue
        if not any(keyword in col_s for keyword in required_keywords):
            continue
        if "净额" in col_s or "净流入" in col_s:
            exact_preferred.append((pos, col))
        else:
            fuzzy_candidates.append((pos, col))
    if exact_preferred:
        exact_preferred.sort(key=lambda item: item[0])
        return exact_preferred[0][1]
    if fuzzy_candidates:
        fuzzy_candidates.sort(key=lambda item: item[0])
        return fuzzy_candidates[0][1]
    return None


def _parse_dividend_plan_to_per_share(plan_text: str) -> Optional[float]:
    """Parse per-share cash dividend from Chinese plan text."""
    text = _safe_str(plan_text)
    if not text:
        return None

    for pattern in (
        r"(?:每)?\s*10\s*股?\s*派(?:发)?\s*([0-9]+(?:\.[0-9]+)?)\s*元",
        r"10\s*派\s*([0-9]+(?:\.[0-9]+)?)\s*元",
    ):
        match = re.search(pattern, text)
        if match:
            parsed = _safe_float(match.group(1))
            if parsed is not None and parsed > 0:
                return parsed / 10.0

    match_per_share = re.search(r"每\s*股\s*派(?:发)?\s*([0-9]+(?:\.[0-9]+)?)\s*元", text)
    if match_per_share:
        parsed = _safe_float(match_per_share.group(1))
        if parsed is not None and parsed > 0:
            return parsed
    return None


def _extract_cash_dividend_per_share(row: pd.Series) -> Optional[float]:
    """Extract pre-tax cash dividend per share from a row."""
    plan_text = _safe_str(_pick_by_keywords(row, _DIVIDEND_KEYWORD_MAP["plan_text"]))
    # Keep pre-tax semantics; skip explicit after-tax plans unless pre-tax marker exists.
    if "税后" in plan_text and "税前" not in plan_text and "含税" not in plan_text:
        return None

    direct = _safe_float(_pick_by_keywords(row, _DIVIDEND_KEYWORD_MAP["per_share"]))
    if direct is not None and direct > 0:
        return direct
    return _parse_dividend_plan_to_per_share(plan_text)


def _filter_rows_by_code(df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    code_cols = [c for c in df.columns if any(k in str(c) for k in ("代码", "股票代码", "证券代码", "symbol", "ts_code"))]
    if not code_cols:
        return df

    target = _normalize_code(stock_code)
    for col in code_cols:
        try:
            series = df[col].astype(str).map(_normalize_code)
            filtered = df[series == target]
            if not filtered.empty:
                return filtered
        except Exception:
            continue
    return pd.DataFrame()


def _normalize_report_date(value: Any) -> Optional[str]:
    parsed = _safe_datetime(value)
    return parsed.date().isoformat() if parsed else None


def _coerce_date(value: Any) -> Optional[date]:
    parsed = _safe_datetime(value)
    return parsed.date() if parsed else None


def _pick_explicit_row_date(row: pd.Series) -> Optional[date]:
    for keywords in (_REPORT_DATE_KEYWORDS, _ANNOUNCE_DATE_KEYWORDS):
        picked = _pick_by_keywords(row, keywords)
        parsed = _coerce_date(picked)
        if parsed is not None:
            return parsed
    return None


def _pick_row_date(row: pd.Series, fallback_date: Optional[date] = None) -> Optional[date]:
    explicit_date = _pick_explicit_row_date(row)
    if explicit_date is not None:
        return explicit_date
    return fallback_date


def _pick_flow_row_date(row: pd.Series) -> Optional[date]:
    return _coerce_date(_pick_by_keywords(row, _FLOW_DATE_KEYWORDS))


def _extract_current_flow_row(
    df: pd.DataFrame,
    stock_code: str,
    *,
    as_of: date,
) -> Optional[pd.Series]:
    work_df = _filter_rows_by_code(df, stock_code)
    if work_df.empty:
        return None
    valid_rows: List[Tuple[date, int, pd.Series]] = []
    for pos, (_, row) in enumerate(work_df.iterrows()):
        flow_date = _pick_flow_row_date(row)
        if flow_date is None:
            continue
        if flow_date != as_of:
            continue
        valid_rows.append((flow_date, -pos, row))
    if not valid_rows:
        return None
    valid_rows.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return valid_rows[0][2]


def _row_date_metadata(row: pd.Series) -> Dict[str, Optional[str]]:
    report_date = _coerce_date(_pick_by_keywords(row, _REPORT_DATE_KEYWORDS))
    announcement_date = _coerce_date(_pick_by_keywords(row, _ANNOUNCE_DATE_KEYWORDS))
    return {
        "report_date": report_date.isoformat() if report_date else None,
        "announcement_date": announcement_date.isoformat() if announcement_date else None,
    }


def _row_has_named_value(row: pd.Series, keyword_groups: List[List[str]]) -> bool:
    for keywords in keyword_groups:
        if _safe_str(_pick_by_keywords(row, keywords)):
            return True
    return False


def _is_row_current(
    row: pd.Series,
    *,
    as_of: date,
    max_age_days: Optional[int],
    require_date: bool,
    fallback_date: Optional[date] = None,
) -> bool:
    announcement_date = _coerce_date(_pick_by_keywords(row, _ANNOUNCE_DATE_KEYWORDS))
    if announcement_date is not None and announcement_date > as_of:
        return False
    explicit_row_date = _pick_explicit_row_date(row)
    if require_date and explicit_row_date is None:
        return False
    row_date = _pick_row_date(row, fallback_date=fallback_date)
    if row_date is None:
        return not require_date
    if row_date > as_of:
        return False
    if max_age_days is not None and (as_of - row_date).days > max_age_days:
        return False
    return True


def _recent_report_periods(
    as_of: Optional[date] = None,
    max_periods: int = _EARNINGS_PERIOD_PROBE_COUNT,
) -> List[str]:
    """Return recent completed AKShare report periods in YYYYMMDD form."""
    anchor = as_of or datetime.now().date()
    quarter_ends = ((12, 31), (9, 30), (6, 30), (3, 31))
    periods: List[date] = []
    for year in range(anchor.year, anchor.year - 5, -1):
        for month, day in quarter_ends:
            period = date(year, month, day)
            if period <= anchor:
                periods.append(period)
            if len(periods) >= max(1, max_periods):
                return [item.strftime("%Y%m%d") for item in periods]
    return [item.strftime("%Y%m%d") for item in periods]


def _report_period_to_institute_symbol(period: str) -> Optional[str]:
    parsed = _coerce_date(period)
    if parsed is None:
        return None
    quarter = {3: "1", 6: "2", 9: "3", 12: "4"}.get(parsed.month)
    if quarter is None:
        return None
    return f"{parsed.year}{quarter}"


def _deadline_exceeded(deadline: Optional[float]) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def _publish_bundle_progress(
    callback: Optional[Callable[[Dict[str, Any]], None]],
    result: Dict[str, Any],
) -> None:
    if callback is None:
        return
    try:
        callback(copy.deepcopy(result))
    except Exception as exc:
        logger.debug("[fundamental_adapter] progress callback skipped: %s", exc)


def _build_evidence_metadata(
    row: pd.Series,
    *,
    as_of: date,
    source: Optional[str],
) -> Dict[str, Optional[str]]:
    return {
        **_row_date_metadata(row),
        "as_of": as_of.isoformat(),
        "source": source,
    }


def _build_actual_report_summary(row: pd.Series) -> str:
    parts: List[str] = []
    metadata = _row_date_metadata(row)
    if metadata.get("report_date"):
        parts.append(f"报告期{metadata['report_date']}")
    elif metadata.get("announcement_date"):
        parts.append(f"公告日{metadata['announcement_date']}")
    has_measured_value = False
    for label, keywords in (
        ("营业收入", ["营业收入-营业收入", "营业总收入", "营业收入", "营收"]),
        ("净利润", ["净利润-净利润", "归母净利润", "净利润"]),
        ("营收同比", ["营业收入-同比增长", "营业收入同比", "营收同比"]),
        ("净利同比", ["净利润-同比增长", "净利润同比", "归母净利润同比"]),
        ("每股收益", ["每股收益", "EPS"]),
        ("净资产收益率", ["净资产收益率", "ROE"]),
    ):
        value = _pick_by_keywords(row, keywords)
        text = _safe_str(value)
        if text:
            has_measured_value = True
            parts.append(f"{label}{text}")
    if not has_measured_value:
        return ""
    return "；".join(parts)[:200]


def _date_columns(df: pd.DataFrame) -> List[Tuple[date, Any]]:
    date_cols: List[Tuple[date, Any]] = []
    for col in df.columns:
        parsed = _coerce_date(col)
        if parsed is not None:
            date_cols.append((parsed, col))
    return date_cols


def _select_current_date_column(
    df: pd.DataFrame,
    *,
    as_of: date,
    max_age_days: int,
) -> Optional[Tuple[date, Any]]:
    valid: List[Tuple[date, Any]] = []
    for parsed, col in _date_columns(df):
        if parsed > as_of:
            continue
        if (as_of - parsed).days > max_age_days:
            continue
        valid.append((parsed, col))
    if not valid:
        return None
    valid.sort(key=lambda item: item[0], reverse=True)
    return valid[0]


def _horizontal_label_text(row: pd.Series, date_cols: set) -> str:
    parts: List[str] = []
    for col in row.index:
        if col in date_cols:
            continue
        text = _safe_str(row.get(col))
        if text and text.lower() not in ("nan", "none", "null"):
            parts.append(text)
    return " ".join(parts)


def _pick_horizontal_metric(
    df: pd.DataFrame,
    value_col: Any,
    keywords: List[str],
    exclude_keywords: Optional[List[str]] = None,
) -> Optional[Any]:
    date_cols = {col for _, col in _date_columns(df)}
    for _, row in df.iterrows():
        label = _horizontal_label_text(row, date_cols)
        if (
            label
            and any(keyword in label for keyword in keywords)
            and not any(keyword in label for keyword in (exclude_keywords or []))
        ):
            value = row.get(value_col)
            if _safe_str(value) not in ("", "-", "nan", "None"):
                return value
    return None


def _extract_financial_indicator_row(
    df: pd.DataFrame,
    stock_code: str,
    *,
    as_of: date,
    max_age_days: int = _FINANCIAL_REPORT_MAX_AGE_DAYS,
) -> Optional[pd.Series]:
    """
    Normalize AkShare financial indicator shapes into one dated metric row.

    Some endpoints return one row per report period. ``stock_financial_abstract``
    commonly returns a horizontal table where report periods are columns and
    metric names are row values; select the latest current date column there.
    """
    selected_date_col = _select_current_date_column(
        df,
        as_of=as_of,
        max_age_days=max_age_days,
    )
    if selected_date_col is not None:
        report_date, value_col = selected_date_col
        payload = {
            "报告期": report_date.isoformat(),
            "营业收入同比": _pick_horizontal_metric(df, value_col, ["营业收入同比", "营收同比", "收入同比", "同比增长"]),
            "净利润同比": _pick_horizontal_metric(df, value_col, ["净利润同比", "净利同比", "归母净利润同比"]),
            "净资产收益率": _pick_horizontal_metric(df, value_col, ["净资产收益率", "ROE", "净资产收益"]),
            "毛利率": _pick_horizontal_metric(df, value_col, ["毛利率"]),
            "营业总收入": _pick_horizontal_metric(
                df,
                value_col,
                ["营业总收入", "营业收入", "营收"],
                exclude_keywords=["同比", "增长", "环比", "率"],
            ),
            "归母净利润": _pick_horizontal_metric(
                df,
                value_col,
                ["归母净利润", "母公司股东净利润", "净利润"],
                exclude_keywords=["同比", "增长", "环比", "率"],
            ),
            "经营活动产生的现金流量净额": _pick_horizontal_metric(
                df,
                value_col,
                ["经营活动产生的现金流量净额", "经营现金流", "经营活动现金流"],
            ),
        }
        if any(value is not None and _safe_str(value) for key, value in payload.items() if key != "报告期"):
            return pd.Series(payload)

    return _extract_latest_row(
        df,
        stock_code,
        as_of=as_of,
        max_age_days=max_age_days,
        require_date=True,
    )


def _build_dividend_payload(
    dividend_df: pd.DataFrame,
    stock_code: str,
    max_events: int = 5,
) -> Dict[str, Any]:
    work_df = _filter_rows_by_code(dividend_df, stock_code)
    if work_df.empty:
        return {}

    now_date = datetime.now().date()
    ttm_start_date = now_date - timedelta(days=365)
    dedupe_keys = set()
    events: List[Dict[str, Any]] = []

    for _, row in work_df.iterrows():
        if not isinstance(row, pd.Series):
            continue
        ex_dt = _safe_datetime(_pick_by_keywords(row, _DIVIDEND_KEYWORD_MAP["ex_dividend_date"]))
        record_dt = _safe_datetime(_pick_by_keywords(row, _DIVIDEND_KEYWORD_MAP["record_date"]))
        announce_dt = _safe_datetime(_pick_by_keywords(row, _DIVIDEND_KEYWORD_MAP["announce_date"]))
        event_dt = ex_dt or record_dt or announce_dt
        if event_dt is None:
            continue
        event_date = event_dt.date()
        if event_date > now_date:
            continue

        per_share = _extract_cash_dividend_per_share(row)
        if per_share is None or per_share <= 0:
            continue

        dedupe_key = (event_date.isoformat(), round(per_share, 6))
        if dedupe_key in dedupe_keys:
            continue
        dedupe_keys.add(dedupe_key)

        events.append(
            {
                "event_date": event_date.isoformat(),
                "ex_dividend_date": ex_dt.date().isoformat() if ex_dt else None,
                "record_date": record_dt.date().isoformat() if record_dt else None,
                "announcement_date": announce_dt.date().isoformat() if announce_dt else None,
                "cash_dividend_per_share": round(per_share, 6),
                "is_pre_tax": True,
            }
        )

    if not events:
        return {}

    events.sort(key=lambda item: item.get("event_date") or "", reverse=True)
    ttm_events: List[Dict[str, Any]] = []
    for item in events:
        event_dt = _safe_datetime(item.get("event_date"))
        if event_dt is None:
            continue
        event_date = event_dt.date()
        if ttm_start_date <= event_date <= now_date:
            ttm_events.append(item)

    return {
        "events": events[:max(1, max_events)],
        "ttm_event_count": len(ttm_events),
        "ttm_cash_dividend_per_share": (
            round(sum(float(item.get("cash_dividend_per_share") or 0.0) for item in ttm_events), 6)
            if ttm_events else None
        ),
        "coverage": "cash_dividend_pre_tax",
        "as_of": now_date.isoformat(),
    }


def _extract_latest_row(
    df: pd.DataFrame,
    stock_code: str,
    *,
    as_of: Optional[date] = None,
    max_age_days: Optional[int] = None,
    require_date: bool = False,
    fallback_date: Optional[date] = None,
) -> Optional[pd.Series]:
    """
    Select the most relevant row for the given stock.

    By default this preserves the legacy first-match behavior used by generic
    probes. When an ``as_of`` date is provided, rows are filtered to dated,
    non-future and non-stale evidence before choosing the newest row.
    """
    if df is None or df.empty:
        return None

    code_cols = [c for c in df.columns if any(k in str(c) for k in ("代码", "股票代码", "证券代码", "ts_code", "symbol"))]
    target = _normalize_code(stock_code)
    candidates = df
    if code_cols:
        candidates = pd.DataFrame()
        for col in code_cols:
            try:
                series = df[col].astype(str).map(_normalize_code)
                matched = df[series == target]
                if not matched.empty:
                    candidates = matched
                    break
            except Exception:
                continue
        if candidates.empty:
            return None

    if as_of is None:
        return candidates.iloc[0]

    valid_rows: List[Tuple[date, int, pd.Series]] = []
    for pos, (_, row) in enumerate(candidates.iterrows()):
        if not isinstance(row, pd.Series):
            continue
        explicit_row_date = _pick_explicit_row_date(row)
        if require_date and explicit_row_date is None:
            continue
        row_date = _pick_row_date(row, fallback_date=fallback_date)
        if row_date is None:
            row_date = date.min
        if not _is_row_current(
            row,
            as_of=as_of,
            max_age_days=max_age_days,
            require_date=require_date,
            fallback_date=fallback_date,
        ):
            continue
        valid_rows.append((row_date, -pos, row))

    if not valid_rows:
        return None
    valid_rows.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return valid_rows[0][2]


class AkshareFundamentalAdapter:
    """AkShare adapter for fundamentals, capital flow and dragon-tiger signals."""

    def _call_df_candidates(
        self,
        candidates: List[Tuple[str, Dict[str, Any]]],
        *,
        deadline: Optional[float] = None,
    ) -> Tuple[Optional[pd.DataFrame], Optional[str], List[str]]:
        errors: List[str] = []
        try:
            import akshare as ak
        except Exception as exc:
            return None, None, [f"import_akshare:{type(exc).__name__}"]

        for func_name, kwargs in candidates:
            if _deadline_exceeded(deadline):
                errors.append(f"{func_name}:deadline_exceeded")
                break
            fn = getattr(ak, func_name, None)
            if fn is None:
                continue
            try:
                df = fn(**kwargs)
                if isinstance(df, pd.Series):
                    df = df.to_frame().T
                if isinstance(df, pd.DataFrame) and not df.empty:
                    return df, func_name, errors
            except Exception as exc:
                errors.append(f"{func_name}:{type(exc).__name__}")
                continue
        return None, None, errors

    def _call_current_row_candidates(
        self,
        candidates: List[Tuple[str, Dict[str, Any], Optional[date]]],
        stock_code: str,
        *,
        as_of: Optional[date] = None,
        max_age_days: int = _CURRENT_EARNINGS_MAX_AGE_DAYS,
        record_no_current: bool = True,
        required_value_keywords: Optional[List[List[str]]] = None,
        deadline: Optional[float] = None,
        allow_fallback_date_as_evidence: bool = False,
    ) -> Tuple[Optional[pd.Series], Optional[str], List[str]]:
        """Call bounded endpoint candidates until a current row for stock_code is found."""
        errors: List[str] = []
        query_as_of = as_of or datetime.now().date()
        for func_name, kwargs, fallback_date in candidates:
            if _deadline_exceeded(deadline):
                errors.append(f"{func_name}:deadline_exceeded")
                break
            df, source, call_errors = self._call_df_candidates([(func_name, kwargs)], deadline=deadline)
            errors.extend(call_errors)
            if df is None or source is None:
                continue
            row = _extract_latest_row(
                df,
                stock_code,
                as_of=query_as_of,
                max_age_days=max_age_days,
                require_date=not allow_fallback_date_as_evidence,
                fallback_date=fallback_date,
            )
            if row is not None and (
                required_value_keywords is None
                or _row_has_named_value(row, required_value_keywords)
            ):
                if fallback_date is not None and _coerce_date(_pick_by_keywords(row, _REPORT_DATE_KEYWORDS)) is None:
                    row = row.copy()
                    row["报告期"] = fallback_date.isoformat()
                return row, source, errors
            if record_no_current:
                suffix = f":{kwargs}" if kwargs else ""
                errors.append(f"{func_name}{suffix}:no_current_row")
        return None, None, errors

    def get_fundamental_bundle(
        self,
        stock_code: str,
        as_of: Optional[date] = None,
        *,
        deadline: Optional[float] = None,
        on_progress: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> Dict[str, Any]:
        """
        Return normalized fundamental blocks from AkShare with partial tolerance.
        """
        result: Dict[str, Any] = {
            "status": "not_supported",
            "growth": {},
            "earnings": {},
            "institution": {},
            "source_chain": [],
            "errors": [],
        }

        as_of_date = as_of or datetime.now().date()

        def _mark_status() -> None:
            has_current_content = bool(result["growth"] or result["earnings"] or result["institution"])
            result["status"] = "partial" if has_current_content else "not_supported"

        def _publish() -> None:
            _mark_status()
            _publish_bundle_progress(on_progress, result)

        def _stop_if_deadline(block: str) -> bool:
            if not _deadline_exceeded(deadline):
                return False
            result["errors"].append(f"{block}:deadline_exceeded")
            _publish()
            return True

        # Financial indicators
        if _stop_if_deadline("financial_indicators"):
            return result
        fin_df, fin_source, fin_errors = self._call_df_candidates([
            ("stock_financial_abstract", {"symbol": stock_code}),
            ("stock_financial_analysis_indicator", {"symbol": stock_code, "start_year": str(max(1900, as_of_date.year - 2))}),
        ], deadline=deadline)
        result["errors"].extend(fin_errors)
        if fin_df is not None:
            row = _extract_financial_indicator_row(fin_df, stock_code, as_of=as_of_date)
            if row is not None:
                revenue_yoy = _safe_float(_pick_by_keywords(row, ["营业收入同比", "营收同比", "收入同比", "同比增长"]))
                profit_yoy = _safe_float(_pick_by_keywords(row, ["净利润同比", "净利同比", "归母净利润同比"]))
                roe = _safe_float(_pick_by_keywords(row, ["净资产收益率", "ROE", "净资产收益"]))
                gross_margin = _safe_float(_pick_by_keywords(row, ["毛利率"]))
                report_date = _normalize_report_date(_pick_by_keywords(row, _DIVIDEND_KEYWORD_MAP["report_date"]))
                revenue = _safe_float(_pick_by_keywords(row, ["营业总收入", "营业收入", "营收"]))
                net_profit_parent = _safe_float(_pick_by_keywords(row, ["归母净利润", "母公司股东净利润", "净利润"]))
                operating_cash_flow = _safe_float(
                    _pick_by_keywords(row, ["经营活动产生的现金流量净额", "经营现金流", "经营活动现金流"])
                )
                result["growth"] = {
                    "revenue_yoy": revenue_yoy,
                    "net_profit_yoy": profit_yoy,
                    "roe": roe,
                    "gross_margin": gross_margin,
                }
                financial_report_payload = {
                    "report_date": report_date,
                    "revenue": revenue,
                    "net_profit_parent": net_profit_parent,
                    "operating_cash_flow": operating_cash_flow,
                    "roe": roe,
                }
                if any(v is not None for v in financial_report_payload.values()):
                    result["earnings"]["financial_report"] = financial_report_payload
                result["source_chain"].append(f"growth:{fin_source}")
        _publish()

        report_periods = _recent_report_periods(as_of_date, max_periods=_EARNINGS_PERIOD_PROBE_COUNT)

        # Actual earnings report. stock_yjbb_em is a measured report endpoint,
        # not forecast guidance; expose it separately so dates/metrics are
        # visible to LLM consumers and a date-only row cannot masquerade as a
        # forecast summary.
        if _stop_if_deadline("earnings_report"):
            return result
        report_candidates = [
            ("stock_yjbb_em", {"date": period}, _coerce_date(period))
            for period in report_periods
        ]
        report_row, report_source, report_errors = self._call_current_row_candidates(
            report_candidates,
            stock_code,
            as_of=as_of_date,
            max_age_days=_CURRENT_EARNINGS_MAX_AGE_DAYS,
            record_no_current=False,
            required_value_keywords=[
                ["营业收入-营业收入", "营业总收入", "营业收入", "营收"],
                ["净利润-净利润", "归母净利润", "净利润"],
                ["营业收入-同比增长", "营业收入同比", "营收同比"],
                ["净利润-同比增长", "净利润同比", "归母净利润同比"],
                ["每股收益", "EPS"],
                ["净资产收益率", "ROE"],
            ],
            deadline=deadline,
            allow_fallback_date_as_evidence=True,
        )
        result["errors"].extend(report_errors)
        if report_row is not None:
            report_summary = _build_actual_report_summary(report_row)
            if report_summary:
                result["earnings"]["report_summary"] = report_summary
                result["earnings"]["actual_report"] = {
                    "summary": report_summary,
                    **_build_evidence_metadata(report_row, as_of=as_of_date, source=report_source),
                }
                result["source_chain"].append(f"earnings_report:{report_source}")
        _publish()

        # Earnings quick report
        if _stop_if_deadline("earnings_quick"):
            return result
        quick_candidates = [
            ("stock_yjkb_em", {"date": period}, _coerce_date(period))
            for period in report_periods
        ]
        quick_row, quick_source, quick_errors = self._call_current_row_candidates(
            quick_candidates,
            stock_code,
            as_of=as_of_date,
            max_age_days=_CURRENT_EARNINGS_MAX_AGE_DAYS,
            record_no_current=False,
            required_value_keywords=[
                ["快报", "摘要", "说明", "内容", "类型"],
                ["营业收入-营业收入", "营业总收入", "营业收入", "营收"],
                ["净利润-净利润", "归母净利润", "净利润"],
                ["每股收益", "EPS"],
            ],
            deadline=deadline,
        )
        result["errors"].extend(quick_errors)
        if quick_row is not None:
            quick_summary = _safe_str(
                _pick_by_keywords(quick_row, ["快报", "摘要", "说明", "内容", "类型"])
            )[:200]
            if not quick_summary:
                quick_summary = _build_actual_report_summary(quick_row)
            if quick_summary:
                result["earnings"]["quick_report_summary"] = quick_summary
                result["earnings"]["quick_report"] = {
                    "summary": quick_summary,
                    **_build_evidence_metadata(quick_row, as_of=as_of_date, source=quick_source),
                }
                result["source_chain"].append(f"earnings_quick:{quick_source}")
        _publish()

        # Dividend details (cash dividend, pre-tax)
        if _stop_if_deadline("dividend"):
            return result
        dividend_df, dividend_source, dividend_errors = self._call_df_candidates([
            ("stock_fhps_detail_em", {"symbol": stock_code}),
            ("stock_history_dividend_detail", {"symbol": stock_code, "indicator": "分红", "date": ""}),
            ("stock_dividend_cninfo", {"symbol": stock_code}),
        ], deadline=deadline)
        result["errors"].extend(dividend_errors)
        if dividend_df is not None:
            dividend_payload = _build_dividend_payload(dividend_df, stock_code, max_events=5)
            if dividend_payload:
                result["earnings"]["dividend"] = dividend_payload
                result["source_chain"].append(f"dividend:{dividend_source}")
        _publish()

        # Earnings forecast. AKShare's Eastmoney endpoints are period-scoped
        # (date=YYYYMMDD); do not pass the stock code as ``symbol`` because
        # supported versions reject that signature and mask into stale empties.
        if _stop_if_deadline("earnings_forecast"):
            return result
        forecast_candidates = [
            ("stock_yjyg_em", {"date": period}, _coerce_date(period))
            for period in report_periods
        ]
        forecast_row, forecast_source, forecast_errors = self._call_current_row_candidates(
            forecast_candidates,
            stock_code,
            as_of=as_of_date,
            max_age_days=_CURRENT_EARNINGS_MAX_AGE_DAYS,
            record_no_current=False,
            required_value_keywords=[["预告类型", "预告", "业绩变动", "业绩变动原因", "内容", "摘要", "类型"]],
            deadline=deadline,
        )
        result["errors"].extend(forecast_errors)
        if forecast_row is not None:
            forecast_summary = _safe_str(
                _pick_by_keywords(forecast_row, ["预告类型", "预告", "业绩变动", "业绩变动原因", "内容", "摘要", "类型"])
            )[:200]
            if forecast_summary:
                result["earnings"]["forecast_summary"] = forecast_summary
                result["earnings"]["forecast"] = {
                    "summary": forecast_summary,
                    **_build_evidence_metadata(forecast_row, as_of=as_of_date, source=forecast_source),
                }
                result["source_chain"].append(f"earnings_forecast:{forecast_source}")
        _publish()

        # Institution / top shareholders
        if _stop_if_deadline("institution"):
            return result
        institution_candidates: List[Tuple[str, Dict[str, Any], Optional[date]]] = []
        for period in report_periods[:_INSTITUTION_PERIOD_PROBE_COUNT]:
            inst_symbol = _report_period_to_institute_symbol(period)
            if inst_symbol:
                institution_candidates.append(("stock_institute_hold", {"symbol": inst_symbol}, _coerce_date(period)))
                institution_candidates.append(("stock_institute_hold_detail", {"stock": stock_code, "quarter": inst_symbol}, _coerce_date(period)))
        inst_row, inst_source, inst_errors = self._call_current_row_candidates(
            institution_candidates,
            stock_code,
            as_of=as_of_date,
            max_age_days=_FINANCIAL_REPORT_MAX_AGE_DAYS,
            record_no_current=False,
            required_value_keywords=[["增减", "变化", "变动", "持股变化", "持股比例增幅"]],
            deadline=deadline,
            allow_fallback_date_as_evidence=True,
        )
        result["errors"].extend(inst_errors)
        if inst_row is not None:
            inst_change = _safe_float(_pick_by_keywords(inst_row, ["持股比例增幅", "增减", "变化", "变动", "持股变化"]))
            if inst_change is not None:
                result["institution"]["institution_holding_change"] = inst_change
                result["institution"]["institution_holding_change_as_of"] = as_of_date.isoformat()
                result["institution"]["institution_holding_change_source"] = inst_source
                result["source_chain"].append(f"institution:{inst_source}")
        _publish()

        if _stop_if_deadline("top10"):
            return result
        top10_df: Optional[pd.DataFrame] = None
        top10_source: Optional[str] = None
        top10_period: Optional[str] = None
        top10_errors: List[str] = []
        for period in report_periods[:_INSTITUTION_PERIOD_PROBE_COUNT]:
            if _deadline_exceeded(deadline):
                top10_errors.append("stock_gdfx_top_10_em:deadline_exceeded")
                break
            df, source, errors = self._call_df_candidates(
                [("stock_gdfx_top_10_em", {"symbol": _stock_symbol_with_market(stock_code), "date": period})],
                deadline=deadline,
            )
            top10_errors.extend(errors)
            if df is not None and source is not None:
                top10_df = df
                top10_source = source
                top10_period = period
                break
        result["errors"].extend(top10_errors)
        if top10_df is not None:
            if not _filter_rows_by_code(top10_df, stock_code).empty:
                top10_work = _filter_rows_by_code(top10_df, stock_code)
            elif any(c for c in top10_df.columns if any(k in str(c) for k in ("代码", "股票代码", "证券代码", "symbol", "ts_code"))):
                top10_work = pd.DataFrame()
            else:
                top10_work = top10_df
            if not top10_work.empty:
                change_values: List[float] = []
                for _, row in top10_work.iterrows():
                    value = _safe_float(_pick_by_keywords(row, ["持股变化", "增减", "变化", "变动"]))
                    if value is not None:
                        change_values.append(value)
                if change_values:
                    result["institution"]["top10_holder_change"] = round(sum(change_values), 6)
                    report_period = top10_period or report_periods[0]
                    result["institution"]["top10_holder_report_date"] = report_period[:4] + "-" + report_period[4:6] + "-" + report_period[6:]
                    result["institution"]["top10_holder_change_source"] = top10_source
                    result["source_chain"].append(f"top10:{top10_source}")
        _publish()

        _mark_status()
        return result

    def get_capital_flow(
        self,
        stock_code: str,
        top_n: int = 5,
        *,
        as_of: Optional[date] = None,
    ) -> Dict[str, Any]:
        """
        Return stock + sector capital flow.
        """
        result: Dict[str, Any] = {
            "status": "not_supported",
            "stock_flow": {},
            "stock_flow_meta": {},
            "sector_rankings": {"top": [], "bottom": []},
            "source_chain": [],
            "errors": [],
        }
        as_of_date = as_of or datetime.now().date()

        stock_df, stock_source, stock_errors = self._call_df_candidates([
            ("stock_individual_fund_flow", {"stock": _normalize_code(stock_code), "market": _stock_market_prefix(stock_code)}),
        ])
        result["errors"].extend(stock_errors)
        if stock_df is not None:
            row = _extract_current_flow_row(
                stock_df,
                stock_code,
                as_of=as_of_date,
            )
            if row is not None:
                row_date = _pick_flow_row_date(row)
                net_inflow = _safe_amount_float(_pick_amount_by_keywords(row, ["主力净流入", "净流入", "净额"]))
                inflow_5d = _safe_amount_float(_pick_amount_by_keywords(row, ["5日", "五日"]))
                inflow_10d = _safe_amount_float(_pick_amount_by_keywords(row, ["10日", "十日"]))
                numeric_flow = {
                    "main_net_inflow": net_inflow,
                    "inflow_5d": inflow_5d,
                    "inflow_10d": inflow_10d,
                }
                if row_date is not None and any(value is not None for value in numeric_flow.values()):
                    result["stock_flow"] = numeric_flow
                    result["stock_flow_meta"] = {
                        "date": row_date.isoformat() if row_date else None,
                        "as_of": as_of_date.isoformat(),
                        "is_current": True,
                        "source": stock_source,
                    }
                    result["source_chain"].append(f"capital_stock:{stock_source}")
            else:
                latest_row = _extract_latest_row(stock_df, stock_code)
                if latest_row is not None:
                    latest_date = _pick_flow_row_date(latest_row)
                    result["stock_flow_meta"] = {
                        "date": latest_date.isoformat() if latest_date else None,
                        "as_of": as_of_date.isoformat(),
                        "is_current": False,
                        "stale_or_future": latest_date is not None,
                        "source": stock_source,
                    }

        sector_df, sector_source, sector_errors = self._call_df_candidates([
            ("stock_sector_fund_flow_rank", {"indicator": "今日", "sector_type": "行业资金流"}),
        ])
        result["errors"].extend(sector_errors)
        if sector_df is not None:
            name_col = next((c for c in sector_df.columns if any(k in str(c) for k in ("板块", "行业", "名称", "name"))), None)
            flow_col = _pick_amount_column(list(sector_df.columns), ["净流入", "主力", "flow", "净额"])
            if name_col and flow_col:
                work_df = sector_df[[name_col, flow_col]].copy()
                work_df[flow_col] = work_df[flow_col].map(_safe_amount_float)
                work_df = work_df.dropna(subset=[flow_col])
                top_df = work_df.nlargest(top_n, flow_col)
                bottom_df = work_df.nsmallest(top_n, flow_col)
                result["sector_rankings"] = {
                    "top": [{"name": _safe_str(r[name_col]), "net_inflow": float(r[flow_col])} for _, r in top_df.iterrows()],
                    "bottom": [{"name": _safe_str(r[name_col]), "net_inflow": float(r[flow_col])} for _, r in bottom_df.iterrows()],
                }
                result["source_chain"].append(f"capital_sector:{sector_source}")

        has_content = bool(result["stock_flow"] or result["sector_rankings"]["top"] or result["sector_rankings"]["bottom"])
        result["status"] = "partial" if has_content else "not_supported"
        return result

    def get_dragon_tiger_flag(
        self,
        stock_code: str,
        lookback_days: int = 20,
        *,
        as_of: Optional[date] = None,
    ) -> Dict[str, Any]:
        """
        Return dragon-tiger signal in lookback window.
        """
        result: Dict[str, Any] = {
            "status": "not_supported",
            "is_on_list": None,
            "recent_count": None,
            "latest_date": None,
            "source_chain": [],
            "errors": [],
        }

        as_of_dt = datetime.combine(as_of or datetime.now().date(), datetime.min.time())
        start_dt = as_of_dt - timedelta(days=max(1, lookback_days))
        start_s = start_dt.strftime("%Y%m%d")
        end_s = as_of_dt.strftime("%Y%m%d")
        lhb_candidates: List[Tuple[str, Dict[str, Any]]] = [
            ("stock_lhb_detail_em", {"start_date": start_s, "end_date": end_s}),
        ]
        # The statistic endpoint is period-bucketed ("近一月"), so use it only
        # for a month-like lookback. It must not masquerade as an exact 20-day
        # result when dated detail endpoints are unavailable.
        if 28 <= lookback_days <= 31:
            lhb_candidates.append(("stock_lhb_stock_statistic_em", {"symbol": "近一月"}))
        df, source, errors = self._call_df_candidates(lhb_candidates)
        result["errors"].extend(errors)
        if df is None:
            return result

        # Try code filter
        code_cols = [c for c in df.columns if any(k in str(c) for k in ("代码", "股票代码", "证券代码"))]
        target = _normalize_code(stock_code)
        matched = pd.DataFrame()
        for col in code_cols:
            try:
                series = df[col].astype(str).map(_normalize_code)
                cur = df[series == target]
                if not cur.empty:
                    matched = cur
                    break
            except Exception:
                continue
        if matched.empty:
            result["source_chain"].append(f"dragon_tiger:{source}")
            result["status"] = "ok" if code_cols else "partial"
            if code_cols:
                result["is_on_list"] = False
                result["recent_count"] = 0
            return result

        date_col = next((c for c in matched.columns if any(k in str(c) for k in ("日期", "上榜", "交易日", "time"))), None)
        parsed_dates: List[datetime] = []
        if date_col is not None:
            for val in matched[date_col].astype(str).tolist():
                try:
                    parsed_dates.append(pd.to_datetime(val).to_pydatetime())
                except Exception:
                    continue
        if not parsed_dates:
            count_values: List[int] = []
            for _, row in matched.iterrows():
                count = _safe_float(_pick_by_keywords(row, ["上榜次数", "次数"]))
                if count is not None and count > 0:
                    count_values.append(int(count))
            if count_values:
                result["is_on_list"] = True
                result["recent_count"] = sum(count_values)
                result["status"] = "ok"
                result["source_chain"].append(f"dragon_tiger:{source}")
                return result
            result["errors"].append(f"{source}:matched_row_without_parseable_date_or_count")
            result["source_chain"].append(f"dragon_tiger:{source}")
            result["status"] = "partial"
            return result
        recent_dates = [d for d in parsed_dates if start_dt <= d <= as_of_dt]

        result["is_on_list"] = bool(recent_dates)
        result["recent_count"] = len(recent_dates)
        result["latest_date"] = max(recent_dates).date().isoformat() if recent_dates else (
            max(parsed_dates).date().isoformat() if parsed_dates else None
        )
        result["status"] = "ok"
        result["source_chain"].append(f"dragon_tiger:{source}")
        return result
