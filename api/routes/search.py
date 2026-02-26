"""
Consensus — Semantic Search API route.

GET /api/search?q=...   — embed the query and find nearest listings via pgvector
"""

import os
import time

from fastapi import APIRouter, Query as FastAPIQuery

from db.connection import get_db
from embeddings import get_embedding, rerank_documents

router = APIRouter(tags=["search"])

_VECTOR_FETCH_MULTIPLIER = max(1, int(os.environ.get("SEARCH_VECTOR_FETCH_MULTIPLIER", "2")))
_VECTOR_FETCH_MIN = max(10, int(os.environ.get("SEARCH_VECTOR_FETCH_MIN", "30")))
_VECTOR_FETCH_MAX = max(20, int(os.environ.get("SEARCH_VECTOR_FETCH_MAX", "120")))
_RERANK_DEFAULT_TOP_K = max(1, int(os.environ.get("SEARCH_RERANK_TOP_K", "20")))
_RERANK_HARD_MAX = max(1, int(os.environ.get("SEARCH_RERANK_HARD_MAX", "40")))
_RERANK_MAX_CHARS = max(200, int(os.environ.get("SEARCH_RERANK_MAX_CHARS", "1200")))


_SELECT_COLUMNS = """
id, url, source, title, city, state, country, industry, description,
listed_by_firm, listed_by_name, phone, email,
price, gross_revenue, cash_flow, inventory, ebitda,
financial_data, source_link, extra_information, deal_date,
first_seen_date, last_seen_date, scraping_date
"""


def _rows_to_dicts(cur) -> list[dict]:
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _text_search(cur, q: str, limit: int) -> list[dict]:
    pattern = f"%{q}%"
    cur.execute(
        f"""
        SELECT {_SELECT_COLUMNS}
        FROM raw_listings
        WHERE title ILIKE %s
           OR description ILIKE %s
           OR industry ILIKE %s
           OR city ILIKE %s
           OR state ILIKE %s
        LIMIT %s
        """,
        (pattern, pattern, pattern, pattern, pattern, limit),
    )
    return _rows_to_dicts(cur)


def _semantic_candidates(cur, vec_str: str, fetch_limit: int) -> list[dict]:
    cur.execute(
        f"""
        SELECT {_SELECT_COLUMNS},
               (description_embedding <=> %s::vector) AS distance
        FROM raw_listings
        WHERE description_embedding IS NOT NULL
        ORDER BY description_embedding <=> %s::vector
        LIMIT %s
        """,
        (vec_str, vec_str, fetch_limit),
    )
    rows = _rows_to_dicts(cur)
    for row in rows:
        distance = row.get("distance", 1.0) or 1.0
        row["similarity_score"] = round(1.0 - distance, 4)
    return rows


@router.get("/search")
def semantic_search(
    q: str = FastAPIQuery(..., min_length=1, description="Search query text"),
    limit: int = FastAPIQuery(20, ge=1, le=100),
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

    with get_db() as conn:
        with conn.cursor() as cur:
            if len(words) < 3:
                results = _text_search(cur, q, limit)
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
                results = _text_search(cur, q, limit)
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
            results = _semantic_candidates(cur, vec_str, fetch_limit)

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
