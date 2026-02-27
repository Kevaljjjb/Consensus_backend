"""
Consensus — Semantic Search API route.

GET /api/search?q=...   — embed the query and find nearest listings via pgvector
"""

import os
import time
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query as FastAPIQuery
from pydantic import BaseModel

from api.listing_filters import (
    build_listing_filter_conditions,
    detect_numeric_columns,
    numeric_select_columns_sql,
    validate_min_max,
    with_financial_numeric_fields,
)
from db.connection import get_db
from embeddings import get_embedding, rerank_documents

router = APIRouter(tags=["search"])

_VECTOR_FETCH_MULTIPLIER = max(1, int(os.environ.get("SEARCH_VECTOR_FETCH_MULTIPLIER", "2")))
_VECTOR_FETCH_MIN = max(10, int(os.environ.get("SEARCH_VECTOR_FETCH_MIN", "30")))
_VECTOR_FETCH_MAX = max(20, int(os.environ.get("SEARCH_VECTOR_FETCH_MAX", "120")))
_RERANK_DEFAULT_TOP_K = max(1, int(os.environ.get("SEARCH_RERANK_TOP_K", "20")))
_RERANK_HARD_MAX = max(1, int(os.environ.get("SEARCH_RERANK_HARD_MAX", "40")))
_RERANK_MAX_CHARS = max(200, int(os.environ.get("SEARCH_RERANK_MAX_CHARS", "1200")))


_BASE_SELECT_COLUMNS = """
id, url, source, title, city, state, country, industry, description,
listed_by_firm, listed_by_name, phone, email,
price, gross_revenue, cash_flow, inventory, ebitda,
financial_data, source_link, extra_information, deal_date,
first_seen_date, last_seen_date, scraping_date
"""


class SearchResponse(BaseModel):
    query: str
    method: str
    total: int
    latency_ms: float
    data: list[dict[str, Any]]


def _rows_to_dicts(cur) -> list[dict]:
    columns = [desc[0] for desc in cur.description]
    return [with_financial_numeric_fields(dict(zip(columns, row))) for row in cur.fetchall()]


def _text_search(
    cur,
    q: str,
    limit: int,
    filter_conditions: list[str],
    filter_params: list[Any],
    select_columns_sql: str,
) -> list[dict]:
    pattern = f"%{q}%"
    conditions = [
        """(
            title ILIKE %s
            OR description ILIKE %s
            OR industry ILIKE %s
            OR city ILIKE %s
            OR state ILIKE %s
        )"""
    ]
    conditions.extend(filter_conditions)
    where_sql = "WHERE " + " AND ".join(conditions)

    cur.execute(
        f"""
        SELECT {select_columns_sql}
        FROM raw_listings
        {where_sql}
        LIMIT %s
        """,
        [pattern, pattern, pattern, pattern, pattern, *filter_params, limit],
    )
    return _rows_to_dicts(cur)


def _semantic_candidates(
    cur,
    vec_str: str,
    fetch_limit: int,
    filter_conditions: list[str],
    filter_params: list[Any],
    select_columns_sql: str,
) -> list[dict]:
    where_conditions = ["description_embedding IS NOT NULL", *filter_conditions]
    where_sql = "WHERE " + " AND ".join(where_conditions)

    cur.execute(
        f"""
        SELECT {select_columns_sql},
               (description_embedding <=> %s::vector) AS distance
        FROM raw_listings
        {where_sql}
        ORDER BY distance
        LIMIT %s
        """,
        [vec_str, *filter_params, fetch_limit],
    )
    rows = _rows_to_dicts(cur)
    for row in rows:
        distance = row.get("distance", 1.0) or 1.0
        row["similarity_score"] = round(1.0 - distance, 4)
    return rows


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


@router.get("/search", response_model=SearchResponse)
def semantic_search(
    q: str = FastAPIQuery(..., min_length=1, description="Search query text"),
    limit: int = FastAPIQuery(20, ge=1, le=100),
    source: Optional[str] = FastAPIQuery(None),
    industry: Optional[str] = FastAPIQuery(None),
    state: Optional[str] = FastAPIQuery(None),
    country: Optional[str] = FastAPIQuery(None),
    min_cash_flow: Optional[float] = FastAPIQuery(None),
    max_cash_flow: Optional[float] = FastAPIQuery(None),
    min_ebitda: Optional[float] = FastAPIQuery(None),
    max_ebitda: Optional[float] = FastAPIQuery(None),
    min_revenue: Optional[float] = FastAPIQuery(None),
    max_revenue: Optional[float] = FastAPIQuery(None),
    min_price: Optional[float] = FastAPIQuery(None),
    max_price: Optional[float] = FastAPIQuery(None),
    # Legacy aliases maintained for older clients.
    revenue_min: Optional[float] = FastAPIQuery(None, include_in_schema=False),
    revenue_max: Optional[float] = FastAPIQuery(None, include_in_schema=False),
    ebitda_min: Optional[float] = FastAPIQuery(None, include_in_schema=False),
    ebitda_max: Optional[float] = FastAPIQuery(None, include_in_schema=False),
    threshold: float = FastAPIQuery(0.6, ge=0.0, le=1.0, description="Max cosine distance (lower = more similar)"),
    rerank: bool = FastAPIQuery(True, description="Apply reranking model"),
    rerank_top_k: int = FastAPIQuery(_RERANK_DEFAULT_TOP_K, ge=1, le=100, description="How many candidates to rerank"),
):
    """
    Semantic search across listings.

    For short queries (< 3 words), falls back to SQL ILIKE text search.
    For longer queries, embeds the query and uses pgvector cosine distance,
    then reranks the top results using Qwen3-Reranker-8B.
    """
    started = time.perf_counter()
    words = q.strip().split()
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

    with get_db() as conn:
        with conn.cursor() as cur:
            numeric_columns_available = detect_numeric_columns(cur)
            select_columns_sql = (
                f"{_BASE_SELECT_COLUMNS}, "
                f"{numeric_select_columns_sql(numeric_columns_available=numeric_columns_available)}"
            )
            filter_conditions, filter_params = build_listing_filter_conditions(
                source=source,
                industry=industry,
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

            if len(words) < 3:
                results = _text_search(
                    cur,
                    q,
                    limit,
                    filter_conditions,
                    filter_params,
                    select_columns_sql,
                )
                return {
                    "query": q,
                    "method": "text",
                    "total": len(results),
                    "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                    "data": results,
                }

            try:
                query_embedding = get_embedding(q)
            except Exception:
                # Fallback to text search on embedding failure.
                results = _text_search(
                    cur,
                    q,
                    limit,
                    filter_conditions,
                    filter_params,
                    select_columns_sql,
                )
                return {
                    "query": q,
                    "method": "text (embedding fallback)",
                    "total": len(results),
                    "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                    "data": results,
                }

            if not query_embedding:
                return {
                    "query": q,
                    "method": "semantic",
                    "total": 0,
                    "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                    "data": [],
                }

            vec_str = "[" + ",".join(str(v) for v in query_embedding) + "]"
            fetch_limit = min(
                _VECTOR_FETCH_MAX,
                max(limit * _VECTOR_FETCH_MULTIPLIER, _VECTOR_FETCH_MIN, limit),
            )
            results = _semantic_candidates(
                cur,
                vec_str,
                fetch_limit,
                filter_conditions,
                filter_params,
                select_columns_sql,
            )

    # Reranking step
    if results:
        results = [r for r in results if (r.get("distance", 1.0) or 1.0) <= threshold]

    if rerank and results:
        rerank_count = min(len(results), min(_RERANK_HARD_MAX, max(limit, rerank_top_k)))
        rerank_slice = results[:rerank_count]
        descriptions = [(d.get("description") or "N/A")[:_RERANK_MAX_CHARS] for d in rerank_slice]
        try:
            scores = rerank_documents(q, descriptions) or []
            if scores and len(scores) == len(rerank_slice):
                for res, score in zip(rerank_slice, scores):
                    res["rerank_score"] = score
                    res["similarity_score"] = round((res["similarity_score"] * 0.4) + (score * 0.6), 4)
                rerank_slice.sort(key=lambda x: x["rerank_score"], reverse=True)
                results = rerank_slice + results[rerank_count:]
            else:
                print(
                    f"⚠️ Rerank skipped: returned scores length ({len(scores)}) "
                    f"!= documents length ({len(rerank_slice)})."
                )
        except Exception as exc:
            print(f"⚠️ Reranker failed: {exc}")

    # Take top `limit`
    results = results[:limit]

    return {
        "query": q,
        "method": "semantic + rerank" if rerank else "semantic",
        "total": len(results),
        "latency_ms": round((time.perf_counter() - started) * 1000, 2),
        "data": results,
    }
