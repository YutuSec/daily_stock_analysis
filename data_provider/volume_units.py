"""Volume provenance at the provider/cache boundary (canonical unit: shares).

Do not infer units from price/turnover: adjusted prices and instruments differ.
The suffix versions new cache writes without rewriting historical records.
"""
import math
from typing import Any, Optional

SHARES_SOURCE_SUFFIX = "|vol=shares"
LEGACY_SHARES_SOURCES = frozenset({
    'TencentFetcher', 'TushareFetcher', 'BaostockFetcher', 'YfinanceFetcher',
    'LongbridgeFetcher', 'TickFlowFetcher', 'FinnhubFetcher', 'AlphaVantageFetcher',
})


def cached_volume_unit_known(source: Optional[str]) -> bool:
    source = str(source or '')
    return source.endswith(SHARES_SOURCE_SUFFIX) or source in {'EfinanceFetcher', 'PytdxFetcher'} or source in LEGACY_SHARES_SOURCES


def lots_to_shares(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (ValueError, TypeError):
        return None
    return number * 100 if math.isfinite(number) and number >= 0 else None


def cached_volume_shares(value: Any, source: Optional[str]) -> Optional[float]:
    """Read legacy cache non-destructively; ambiguous multi-provider units abstain."""
    source = str(source or "")
    if source.endswith(SHARES_SOURCE_SUFFIX):
        return value
    if source in {"EfinanceFetcher", "PytdxFetcher"}:  # Mainland daily lots.
        return lots_to_shares(value)
    if source in LEGACY_SHARES_SOURCES:
        return value
    return None  # NULL/custom/old Akshare rows have no provable unit.
