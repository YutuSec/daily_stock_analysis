# -*- coding: utf-8 -*-
"""Strict market-aware symbol identities for the paper-trading ledger."""

from __future__ import annotations

from typing import List, Optional, Tuple


_SUFFIX_MARKET = {
    "SH": "cn", "SS": "cn", "SZ": "cn", "BJ": "cn",
    "HK": "hk", "US": "us", "T": "jp", "KS": "kr", "KQ": "kr",
    "TW": "tw", "TWO": "tw",
}


def _cn_exchange(code: str) -> str:
    if code.startswith(("4", "8", "92")):
        return "BJ"
    if code.startswith(("5", "6", "9")):
        return "SH"
    return "SZ"


def canonical_paper_identity(market: str, stock_code: str) -> Optional[Tuple[str, str]]:
    """Return a strict identity, rejecting suffix/market or exchange conflicts."""

    market_norm = str(market or "").strip().lower()
    raw = str(stock_code or "").strip().upper()
    if market_norm not in {"cn", "hk", "us", "jp", "kr", "tw"} or not raw:
        return None

    suffix = None
    base = raw
    if "." in raw:
        base, suffix = raw.rsplit(".", 1)
        suffix_market = _SUFFIX_MARKET.get(suffix)
        if suffix_market != market_norm:
            return None

    if market_norm == "cn":
        explicit_exchange = suffix
        if base.startswith(("SH", "SS", "SZ", "BJ")):
            prefix = "SH" if base[:2] == "SS" else base[:2]
            if explicit_exchange and ("SH" if explicit_exchange == "SS" else explicit_exchange) != prefix:
                return None
            explicit_exchange = prefix
            base = base[2:]
        if not (base.isdigit() and len(base) == 6):
            return None
        expected = _cn_exchange(base)
        normalized_explicit = "SH" if explicit_exchange == "SS" else explicit_exchange
        if normalized_explicit and normalized_explicit != expected:
            return None
        return market_norm, base

    if market_norm == "hk":
        if base.startswith("HK"):
            base = base[2:]
        if not (base.isdigit() and 1 <= len(base) <= 5):
            return None
        return market_norm, f"HK{base.zfill(5)}"

    if market_norm == "us":
        if suffix not in (None, "US") or not (base.isalpha() and 1 <= len(base) <= 10):
            return None
        return market_norm, base

    required_suffixes = {
        "jp": {"T"}, "kr": {"KS", "KQ"}, "tw": {"TW", "TWO"},
    }[market_norm]
    if suffix not in required_suffixes or not base.isdigit():
        return None
    return market_norm, f"{base}.{suffix}"


def stock_daily_variants(market: str, stock_code: str) -> List[str]:
    """Return only market-consistent StockDaily representations.

    Offshore numeric bare codes are deliberately excluded because the table
    has no market column and a bare value cannot prove market identity.
    """

    identity = canonical_paper_identity(market, stock_code)
    if identity is None:
        return []
    market_norm, code = identity
    if market_norm == "cn":
        exchange = _cn_exchange(code)
        variants = [f"{code}.{exchange}", f"{exchange}{code}", f"{exchange}.{code}"]
        if exchange == "SH":
            variants.extend((f"{code}.SS", f"SS{code}", f"SS.{code}"))
        return list(dict.fromkeys(variants))
    if market_norm == "hk":
        digits = code[2:]
        short = digits.lstrip("0") or "0"
        return list(dict.fromkeys((f"{digits}.HK", f"{short}.HK", code)))
    if market_norm == "us":
        return [f"{code}.US", code]
    return [code]
