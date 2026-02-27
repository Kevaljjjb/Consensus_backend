"""
Consensus — Listings & Stats API routes.

GET /api/listings         — paginated list with filters
GET /api/listings/{id}    — single listing
GET /api/stats            — dashboard statistics
"""

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from api.listing_filters import (
    build_listing_filter_conditions,
    detect_numeric_columns,
    numeric_select_columns_sql,
    resolve_sort,
    validate_min_max,
    with_financial_numeric_fields,
)
from db.connection import get_db

router = APIRouter(tags=["listings"])


_BASE_SELECT_COLUMNS = """
id, url, source, title, city, state, country, industry, description,
listed_by_firm, listed_by_name, phone, email,
price, gross_revenue, cash_flow, inventory, ebitda,
financial_data, source_link, extra_information, deal_date,
first_seen_date, last_seen_date, scraping_date
"""


def _row_to_dict(row, columns) -> dict:
    """Convert a DB row tuple to a dict using column names."""
    return with_financial_numeric_fields(dict(zip(columns, row)))


def _validate_ranges_or_422(
    *,
    min_cash_flow: Optional[float],
    max_cash_flow: Optional[float],
    min_ebitda: Optional[float],
    max_ebitda: Optional[float],
    min_revenue: Optional[float],
    max_revenue: Optional[float],
    min_price: Optional[float],
    max_price: Optional[float],
) -> None:
    try:
        validate_min_max(min_cash_flow, max_cash_flow, "cash_flow")
        validate_min_max(min_ebitda, max_ebitda, "ebitda")
        validate_min_max(min_revenue, max_revenue, "revenue")
        validate_min_max(min_price, max_price, "price")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _where_clause(conditions: list[str]) -> str:
    if not conditions:
        return ""
    return "WHERE " + " AND ".join(conditions)


def _distinct_filter_values(cur, column: str) -> list[str]:
    cur.execute(
        f"""
        SELECT DISTINCT {column}
        FROM raw_listings
        WHERE {column} IS NOT NULL
          AND BTRIM({column}) <> ''
          AND UPPER(BTRIM({column})) <> 'N/A'
        ORDER BY LOWER({column}), {column}
        """
    )
    return [row[0] for row in cur.fetchall()]


class ListingsResponse(BaseModel):
    total: int
    page: int
    per_page: int
    total_pages: int
    data: list[dict[str, Any]]


class ListingFilterOptionsResponse(BaseModel):
    source: list[str] = Field(default_factory=list)
    industry: list[str] = Field(default_factory=list)
    state: list[str] = Field(default_factory=list)
    country: list[str] = Field(default_factory=list)


@router.get("/listings", response_model=ListingsResponse)
def list_listings(
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=100),
    source: Optional[str] = Query(None, description="Exact source filter (e.g., BizBen)"),
    industry: Optional[str] = Query(None, description="Exact industry filter"),
    city: Optional[str] = Query(None, description="Exact city filter (legacy-compatible)"),
    state: Optional[str] = Query(None, description="Exact state filter"),
    country: Optional[str] = Query(None, description="Exact country filter"),
    min_cash_flow: Optional[float] = Query(None),
    max_cash_flow: Optional[float] = Query(None),
    min_ebitda: Optional[float] = Query(None),
    max_ebitda: Optional[float] = Query(None),
    min_revenue: Optional[float] = Query(None),
    max_revenue: Optional[float] = Query(None),
    min_price: Optional[float] = Query(None),
    max_price: Optional[float] = Query(None),
    # Legacy aliases maintained for older clients.
    revenue_min: Optional[float] = Query(None, include_in_schema=False),
    revenue_max: Optional[float] = Query(None, include_in_schema=False),
    ebitda_min: Optional[float] = Query(None, include_in_schema=False),
    ebitda_max: Optional[float] = Query(None, include_in_schema=False),
    sort_by: str = Query(
        "last_seen_date",
        description=(
            "Allowed: last_seen_date, first_seen_date, gross_revenue_num, "
            "ebitda_num, cash_flow_num, price_num"
        ),
    ),
    sort_order: str = Query("desc", description="Allowed: asc, desc"),
):
    """Paginated listing of deals with optional source/location/financial filters."""
    effective_min_revenue = min_revenue if min_revenue is not None else revenue_min
    effective_max_revenue = max_revenue if max_revenue is not None else revenue_max
    effective_min_ebitda = min_ebitda if min_ebitda is not None else ebitda_min
    effective_max_ebitda = max_ebitda if max_ebitda is not None else ebitda_max

    _validate_ranges_or_422(
        min_cash_flow=min_cash_flow,
        max_cash_flow=max_cash_flow,
        min_ebitda=effective_min_ebitda,
        max_ebitda=effective_max_ebitda,
        min_revenue=effective_min_revenue,
        max_revenue=effective_max_revenue,
        min_price=min_price,
        max_price=max_price,
    )

    try:
        sort_column, sql_sort_order = resolve_sort(sort_by=sort_by, sort_order=sort_order)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    with get_db() as conn:
        cur = conn.cursor()
        numeric_columns_available = detect_numeric_columns(cur)

        conditions, params = build_listing_filter_conditions(
            source=source,
            industry=industry,
            city=city,
            state=state,
            country=country,
            min_cash_flow=min_cash_flow,
            max_cash_flow=max_cash_flow,
            min_ebitda=effective_min_ebitda,
            max_ebitda=effective_max_ebitda,
            min_revenue=effective_min_revenue,
            max_revenue=effective_max_revenue,
            min_price=min_price,
            max_price=max_price,
            numeric_columns_available=numeric_columns_available,
        )
        where_sql = _where_clause(conditions)
        select_columns = (
            f"{_BASE_SELECT_COLUMNS}, "
            f"{numeric_select_columns_sql(numeric_columns_available=numeric_columns_available)}"
        )

        # Count total
        cur.execute(f"SELECT COUNT(*) FROM raw_listings {where_sql}", params)
        total = cur.fetchone()[0]

        # Fetch page
        offset = (page - 1) * per_page
        sql = f"""
            SELECT {select_columns}
            FROM raw_listings
            {where_sql}
            ORDER BY {sort_column} {sql_sort_order} NULLS LAST, id DESC
            LIMIT %s OFFSET %s
        """
        cur.execute(sql, params + [per_page, offset])
        columns = [desc[0] for desc in cur.description]
        rows = [_row_to_dict(r, columns) for r in cur.fetchall()]

        cur.close()

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
        "data": rows,
    }


@router.get("/listings/filter-options", response_model=ListingFilterOptionsResponse)
def get_listing_filter_options():
    """Return distinct, non-empty filter option values for the listings UI."""
    with get_db() as conn:
        cur = conn.cursor()
        response = {
            "source": _distinct_filter_values(cur, "source"),
            "industry": _distinct_filter_values(cur, "industry"),
            "state": _distinct_filter_values(cur, "state"),
            "country": _distinct_filter_values(cur, "country"),
        }
        cur.close()
    return response


@router.get("/listings/{listing_id}")
def get_listing(listing_id: int):
    """Get a single listing by ID."""
    with get_db() as conn:
        cur = conn.cursor()
        numeric_columns_available = detect_numeric_columns(cur)
        select_columns = (
            f"{_BASE_SELECT_COLUMNS}, "
            f"{numeric_select_columns_sql(numeric_columns_available=numeric_columns_available)}"
        )
        cur.execute(
            f"SELECT {select_columns} FROM raw_listings WHERE id = %s",
            (listing_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Listing not found")

        columns = [desc[0] for desc in cur.description]
        result = _row_to_dict(row, columns)
        cur.close()

    return result


@router.get("/stats")
def get_stats():
    """Dashboard statistics."""
    with get_db() as conn:
        cur = conn.cursor()

        # Total listings
        cur.execute("SELECT COUNT(*) FROM raw_listings")
        total = cur.fetchone()[0]

        # By source
        cur.execute("SELECT source, COUNT(*) FROM raw_listings GROUP BY source ORDER BY COUNT(*) DESC")
        by_source = {row[0]: row[1] for row in cur.fetchall()}

        # Recently added (last 7 days)
        cur.execute("SELECT COUNT(*) FROM raw_listings WHERE first_seen_date > NOW() - INTERVAL '7 days'")
        new_this_week = cur.fetchone()[0]

        # Distinct industries
        cur.execute("SELECT COUNT(DISTINCT industry) FROM raw_listings WHERE industry != 'N/A'")
        distinct_industries = cur.fetchone()[0]

        # Top industries
        cur.execute("""
            SELECT industry, COUNT(*) as cnt
            FROM raw_listings
            WHERE industry != 'N/A'
            GROUP BY industry
            ORDER BY cnt DESC
            LIMIT 5
        """)
        top_industries = [{"industry": r[0], "count": r[1]} for r in cur.fetchall()]

        cur.close()

    return {
        "total_listings": total,
        "by_source": by_source,
        "new_this_week": new_this_week,
        "distinct_industries": distinct_industries,
        "top_industries": top_industries,
    }
