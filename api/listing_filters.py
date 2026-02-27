"""
Shared filtering and sorting helpers for listings/search endpoints.
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any, Optional

_TEXT_SENTINELS = {"", "N/A", "NA", "NULL", "NONE", "-", "--"}
_NUMERIC_PATTERN = re.compile(r"^[+-]?\d+(?:\.\d+)?$")

SORT_COLUMN_MAP = {
    "last_seen_date": "last_seen_date",
    "first_seen_date": "first_seen_date",
    "gross_revenue_num": "gross_revenue_num",
    "ebitda_num": "ebitda_num",
    "cash_flow_num": "cash_flow_num",
    "price_num": "price_num",
}

# Legacy aliases maintained for backward compatibility with older clients.
LEGACY_SORT_ALIASES = {
    "gross_revenue": "gross_revenue_num",
    "ebitda": "ebitda_num",
    "cash_flow": "cash_flow_num",
    "price": "price_num",
}


def normalize_text_filter(value: Optional[str]) -> Optional[str]:
    """Trim text filter values; empty strings are treated as omitted."""
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def parse_financial_value(value: Any) -> Optional[float]:
    """
    Parse text-like financial values to float.

    Handles values like "$1,200,000", "(12345)", and returns None for
    N/A/empty/malformed inputs.
    """
    if value is None:
        return None

    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None
    if text.upper() in _TEXT_SENTINELS:
        return None

    # Convert accounting negatives "(123.45)" -> "-123.45"
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"

    cleaned = text.replace("$", "").replace(",", "").replace(" ", "")
    if not _NUMERIC_PATTERN.match(cleaned):
        return None

    try:
        return float(cleaned)
    except ValueError:
        return None


def with_financial_numeric_fields(row: dict[str, Any]) -> dict[str, Any]:
    """
    Attach `{field}_numeric` values expected by the frontend.

    Uses numeric DB columns first, then falls back to parsing text columns.
    """
    mapped = {
        "gross_revenue": "gross_revenue_num",
        "cash_flow": "cash_flow_num",
        "ebitda": "ebitda_num",
        "price": "price_num",
    }

    output = dict(row)
    for text_col, numeric_col in mapped.items():
        numeric_value = parse_financial_value(output.get(numeric_col))
        if numeric_value is None:
            numeric_value = parse_financial_value(output.get(text_col))
        output[f"{text_col}_numeric"] = numeric_value
    return output


def validate_min_max(min_value: Optional[float], max_value: Optional[float], label: str) -> None:
    """Validate numeric min/max range."""
    if min_value is not None and max_value is not None and min_value > max_value:
        raise ValueError(f"Invalid range for {label}: min_{label} cannot be greater than max_{label}.")


def build_listing_filter_conditions(
    *,
    source: Optional[str] = None,
    industry: Optional[str] = None,
    state: Optional[str] = None,
    country: Optional[str] = None,
    city: Optional[str] = None,
    min_cash_flow: Optional[float] = None,
    max_cash_flow: Optional[float] = None,
    min_ebitda: Optional[float] = None,
    max_ebitda: Optional[float] = None,
    min_revenue: Optional[float] = None,
    max_revenue: Optional[float] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    table_alias: Optional[str] = None,
) -> tuple[list[str], list[Any]]:
    """Build SQL conditions and bind params for listing/search filters."""
    prefix = f"{table_alias}." if table_alias else ""
    conditions: list[str] = []
    params: list[Any] = []

    text_filters = {
        "source": normalize_text_filter(source),
        "industry": normalize_text_filter(industry),
        "state": normalize_text_filter(state),
        "country": normalize_text_filter(country),
        "city": normalize_text_filter(city),
    }
    for column, value in text_filters.items():
        if value is not None:
            conditions.append(f"{prefix}{column} = %s")
            params.append(value)

    numeric_filters = (
        ("cash_flow_num", min_cash_flow, max_cash_flow),
        ("ebitda_num", min_ebitda, max_ebitda),
        ("gross_revenue_num", min_revenue, max_revenue),
        ("price_num", min_price, max_price),
    )
    for column, min_value, max_value in numeric_filters:
        if min_value is not None:
            conditions.append(f"{prefix}{column} >= %s")
            params.append(min_value)
        if max_value is not None:
            conditions.append(f"{prefix}{column} <= %s")
            params.append(max_value)

    return conditions, params


def resolve_sort(sort_by: str, sort_order: str) -> tuple[str, str]:
    """Resolve and validate sort configuration."""
    normalized_sort = LEGACY_SORT_ALIASES.get(sort_by, sort_by)
    column = SORT_COLUMN_MAP.get(normalized_sort)
    if not column:
        allowed = ", ".join(SORT_COLUMN_MAP.keys())
        raise ValueError(f"Invalid sort_by '{sort_by}'. Allowed values: {allowed}.")

    normalized_order = sort_order.lower()
    if normalized_order not in {"asc", "desc"}:
        raise ValueError("Invalid sort_order. Allowed values: asc, desc.")

    return column, normalized_order.upper()
