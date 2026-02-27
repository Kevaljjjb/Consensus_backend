from decimal import Decimal

import pytest

from api.listing_filters import (
    build_listing_filter_conditions,
    parse_financial_value,
    resolve_sort,
    validate_min_max,
    with_financial_numeric_fields,
)


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("$1,200,000", 1_200_000.0),
        ("  450000  ", 450_000.0),
        ("(12,500.50)", -12_500.5),
        (Decimal("99.25"), 99.25),
    ],
)
def test_parse_financial_value_valid(raw_value, expected):
    assert parse_financial_value(raw_value) == expected


@pytest.mark.parametrize("raw_value", ["N/A", "na", "", None, "abc", "$12.3.4", "$4k"])
def test_parse_financial_value_invalid(raw_value):
    assert parse_financial_value(raw_value) is None


def test_validate_min_max_rejects_invalid_ranges():
    with pytest.raises(ValueError, match="min_cash_flow cannot be greater than max_cash_flow"):
        validate_min_max(500_000, 200_000, "cash_flow")


def test_build_listing_filter_conditions_combines_filters():
    conditions, params = build_listing_filter_conditions(
        source="BizBen",
        industry="Manufacturing",
        state="CA",
        country="US",
        min_cash_flow=100_000,
        max_cash_flow=800_000,
        min_revenue=500_000,
        max_price=2_000_000,
    )

    assert conditions == [
        "source = %s",
        "industry = %s",
        "state = %s",
        "country = %s",
        "cash_flow_num >= %s",
        "cash_flow_num <= %s",
        "gross_revenue_num >= %s",
        "price_num <= %s",
    ]
    assert params == [
        "BizBen",
        "Manufacturing",
        "CA",
        "US",
        100_000,
        800_000,
        500_000,
        2_000_000,
    ]


def test_resolve_sort_whitelist_and_legacy_alias():
    assert resolve_sort("gross_revenue_num", "desc") == ("gross_revenue_num", "DESC")
    assert resolve_sort("cash_flow", "asc") == ("cash_flow_num", "ASC")
    with pytest.raises(ValueError, match="Invalid sort_by"):
        resolve_sort("drop table", "asc")


def test_with_financial_numeric_fields_prefers_numeric_columns():
    row = {
        "price": "$150,000",
        "gross_revenue": "$1,000,000",
        "cash_flow": "N/A",
        "ebitda": "$250,000",
        "price_num": Decimal("145000"),
        "gross_revenue_num": None,
        "cash_flow_num": None,
        "ebitda_num": Decimal("260000"),
    }

    output = with_financial_numeric_fields(row)
    assert output["price_numeric"] == 145_000.0
    assert output["gross_revenue_numeric"] == 1_000_000.0
    assert output["cash_flow_numeric"] is None
    assert output["ebitda_numeric"] == 260_000.0
