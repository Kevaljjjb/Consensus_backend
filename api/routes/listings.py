"""
Consensus — Listings & Stats API routes.

GET /api/listings         — paginated list with filters
GET /api/listings/{id}    — single listing
GET /api/stats            — dashboard statistics
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from db.connection import get_db

router = APIRouter(tags=["listings"])


def _parse_money_db(val: str) -> Optional[float]:
    """Try to parse a money string from DB to float for sorting/comparison."""
    if not val or val == "N/A":
        return None
    import re
    cleaned = re.sub(r"[^\d.]", "", str(val))
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _row_to_dict(row, columns) -> dict:
    """Convert a DB row tuple to a dict using column names."""
    d = dict(zip(columns, row))
    # Parse numeric fields for the frontend
    for field in ("gross_revenue", "cash_flow", "ebitda", "price"):
        if field in d:
            d[f"{field}_numeric"] = _parse_money_db(d.get(field, ""))
    return d


@router.get("/listings")
def list_listings(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    source: Optional[str] = None,
    industry: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    revenue_min: Optional[float] = None,
    revenue_max: Optional[float] = None,
    ebitda_min: Optional[float] = None,
    ebitda_max: Optional[float] = None,
    sort_by: str = Query("last_seen_date", pattern="^(last_seen_date|title|price|gross_revenue|ebitda|cash_flow|first_seen_date)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
):
    """Paginated listing of deals with optional filters."""
    with get_db() as conn:
        cur = conn.cursor()

        conditions = []
        params = []

        if source:
            conditions.append("source = %s")
            params.append(source)
        if industry:
            conditions.append("industry ILIKE %s")
            params.append(f"%{industry}%")
        if city:
            conditions.append("city ILIKE %s")
            params.append(f"%{city}%")
        if state:
            conditions.append("state ILIKE %s")
            params.append(f"%{state}%")

        where = ""
        if conditions:
            where = "WHERE " + " AND ".join(conditions)

        # Count total
        cur.execute(f"SELECT COUNT(*) FROM raw_listings {where}", params)
        total = cur.fetchone()[0]

        # Fetch page
        offset = (page - 1) * per_page
        sql = f"""
            SELECT id, url, source, title, city, state, country, industry, description,
                   listed_by_firm, listed_by_name, phone, email,
                   price, gross_revenue, cash_flow, inventory, ebitda,
                   financial_data, source_link, extra_information, deal_date,
                   first_seen_date, last_seen_date, scraping_date
            FROM raw_listings
            {where}
            ORDER BY {sort_by} {sort_order}
            LIMIT %s OFFSET %s
        """
        cur.execute(sql, params + [per_page, offset])
        columns = [desc[0] for desc in cur.description]
        rows = [_row_to_dict(r, columns) for r in cur.fetchall()]

        # Apply in-memory revenue/ebitda range filters if specified
        if revenue_min is not None:
            rows = [r for r in rows if (r.get("gross_revenue_numeric") or 0) >= revenue_min]
        if revenue_max is not None:
            rows = [r for r in rows if (r.get("gross_revenue_numeric") or 0) <= revenue_max]
        if ebitda_min is not None:
            rows = [r for r in rows if (r.get("ebitda_numeric") or 0) >= ebitda_min]
        if ebitda_max is not None:
            rows = [r for r in rows if (r.get("ebitda_numeric") or 0) <= ebitda_max]

        cur.close()

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
        "data": rows,
    }


@router.get("/listings/{listing_id}")
def get_listing(listing_id: int):
    """Get a single listing by ID."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, url, source, title, city, state, country, industry, description,
                   listed_by_firm, listed_by_name, phone, email,
                   price, gross_revenue, cash_flow, inventory, ebitda,
                   financial_data, source_link, extra_information, deal_date,
                   first_seen_date, last_seen_date, scraping_date
            FROM raw_listings WHERE id = %s
            """,
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
