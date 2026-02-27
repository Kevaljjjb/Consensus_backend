"""
Consensus — Dashboard overview API route.

GET /api/dashboard/overview — aggregated, low-latency dashboard payload.
"""

from __future__ import annotations

import copy
import os
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from db.connection import get_db

router = APIRouter(tags=["dashboard"])

_DEFAULT_COUNTRY_SCOPE = ("US", "CA")
_CACHE_TTL_SECONDS = max(1, int(os.environ.get("DASHBOARD_OVERVIEW_CACHE_TTL_SECONDS", "300")))
_CACHE_MAX_ENTRIES = max(8, int(os.environ.get("DASHBOARD_OVERVIEW_CACHE_MAX_ENTRIES", "128")))

_cache_lock = threading.Lock()
_cache: dict[tuple[int, int, tuple[str, ...]], tuple[float, dict[str, Any]]] = {}


class SnapshotResponse(BaseModel):
    total_listings: int
    new_this_week: int
    qualified_count: int
    pass_rate: float
    active_sources: int
    distinct_industries: int


class CriteriaFunnelStage(BaseModel):
    stage: str
    count: int


class SourceYieldRow(BaseModel):
    source: str
    total: int
    qualified: int
    qualified_rate: float


class PriorityQueueItem(BaseModel):
    id: int
    title: str
    source: str
    state: str
    country: str
    gross_revenue: str
    ebitda: str
    cash_flow: str
    first_seen_date: Optional[str]
    fit_score: int
    reasons: list[str] = Field(default_factory=list)


class SLAResponse(BaseModel):
    response_48h_rate: Optional[float]
    offer_5d_rate: Optional[float]
    close_60d_rate: Optional[float]
    in_pipeline: Optional[int]


class DataQualityResponse(BaseModel):
    parseable_revenue_pct: float
    parseable_ebitda_pct: float
    parseable_cash_flow_pct: float
    parseable_location_pct: float


class DashboardOverviewResponse(BaseModel):
    generated_at: str
    snapshot: SnapshotResponse
    criteria_funnel: list[CriteriaFunnelStage]
    source_yield: list[SourceYieldRow]
    priority_queue: list[PriorityQueueItem]
    sla: SLAResponse
    data_quality: DataQualityResponse


def reset_dashboard_overview_cache() -> None:
    """Test helper: clear in-process dashboard cache."""
    with _cache_lock:
        _cache.clear()


def _parse_country_scope(raw_value: str) -> list[str]:
    seen: set[str] = set()
    countries: list[str] = []
    for chunk in raw_value.split(","):
        code = chunk.strip().upper()
        if not code:
            continue
        if code in seen:
            continue
        seen.add(code)
        countries.append(code)

    if not countries:
        return list(_DEFAULT_COUNTRY_SCOPE)
    return countries


def _to_float(value: Any, *, digits: int = 4, default: Optional[float] = 0.0) -> Optional[float]:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return round(float(value), digits)
    if isinstance(value, (int, float)):
        return round(float(value), digits)
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, *, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, (int, float, Decimal)):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _to_iso_datetime(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def _cache_get(key: tuple[int, int, tuple[str, ...]]) -> Optional[dict[str, Any]]:
    now = time.monotonic()
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None

        expires_at, payload = entry
        if expires_at <= now:
            _cache.pop(key, None)
            return None

        return copy.deepcopy(payload)


def _cache_set(key: tuple[int, int, tuple[str, ...]], payload: dict[str, Any]) -> None:
    now = time.monotonic()
    with _cache_lock:
        _cache[key] = (now + _CACHE_TTL_SECONDS, copy.deepcopy(payload))

        if len(_cache) <= _CACHE_MAX_ENTRIES:
            return

        expired_keys = [k for k, (expires_at, _) in _cache.items() if expires_at <= now]
        for cache_key in expired_keys:
            _cache.pop(cache_key, None)

        if len(_cache) <= _CACHE_MAX_ENTRIES:
            return

        oldest_key = min(_cache.items(), key=lambda item: item[1][0])[0]
        _cache.pop(oldest_key, None)


def _fetch_snapshot_funnel_and_quality(cur, *, lookback_days: int, country_scope: list[str]) -> dict[str, Any]:
    cur.execute(
        """
        WITH scoped AS (
            SELECT
                source,
                country,
                state,
                industry,
                first_seen_date,
                gross_revenue_num,
                ebitda_num,
                cash_flow_num
            FROM raw_listings
            WHERE COALESCE(last_seen_date, first_seen_date) >= NOW() - (%s * INTERVAL '1 day')
        ),
        flagged AS (
            SELECT
                *,
                UPPER(BTRIM(COALESCE(country, ''))) = ANY(%s::text[]) AS is_local,
                cash_flow_num >= 2000000 AS cash_flow_fit,
                (
                    gross_revenue_num IS NOT NULL
                    AND ebitda_num IS NOT NULL
                    AND gross_revenue_num > 0
                    AND (ebitda_num / gross_revenue_num) >= 0.10
                ) AS margin_fit,
                first_seen_date >= NOW() - INTERVAL '7 days' AS is_new_week
            FROM scoped
        )
        SELECT
            COUNT(*) AS total_listings,
            COUNT(*) FILTER (WHERE is_new_week) AS new_this_week,
            COUNT(*) FILTER (WHERE is_local AND cash_flow_fit AND margin_fit) AS qualified_count,
            COALESCE(
                (COUNT(*) FILTER (WHERE is_local AND cash_flow_fit AND margin_fit))::NUMERIC
                / NULLIF(COUNT(*), 0),
                0
            ) AS pass_rate,
            COUNT(DISTINCT source) FILTER (
                WHERE source IS NOT NULL
                  AND BTRIM(source) <> ''
                  AND UPPER(BTRIM(source)) <> 'N/A'
            ) AS active_sources,
            COUNT(DISTINCT industry) FILTER (
                WHERE industry IS NOT NULL
                  AND BTRIM(industry) <> ''
                  AND UPPER(BTRIM(industry)) <> 'N/A'
            ) AS distinct_industries,
            COUNT(*) FILTER (WHERE is_local) AS funnel_local,
            COUNT(*) FILTER (WHERE cash_flow_fit) AS funnel_cash_flow,
            COUNT(*) FILTER (WHERE margin_fit) AS funnel_margin,
            COALESCE(
                (COUNT(*) FILTER (WHERE gross_revenue_num IS NOT NULL))::NUMERIC
                / NULLIF(COUNT(*), 0),
                0
            ) AS parseable_revenue_pct,
            COALESCE(
                (COUNT(*) FILTER (WHERE ebitda_num IS NOT NULL))::NUMERIC
                / NULLIF(COUNT(*), 0),
                0
            ) AS parseable_ebitda_pct,
            COALESCE(
                (COUNT(*) FILTER (WHERE cash_flow_num IS NOT NULL))::NUMERIC
                / NULLIF(COUNT(*), 0),
                0
            ) AS parseable_cash_flow_pct,
            COALESCE(
                (
                    COUNT(*) FILTER (
                        WHERE state IS NOT NULL
                          AND country IS NOT NULL
                          AND BTRIM(state) <> ''
                          AND BTRIM(country) <> ''
                          AND UPPER(BTRIM(state)) <> 'N/A'
                          AND UPPER(BTRIM(country)) <> 'N/A'
                    )
                )::NUMERIC
                / NULLIF(COUNT(*), 0),
                0
            ) AS parseable_location_pct
        FROM flagged
        """,
        (lookback_days, country_scope),
    )
    row = cur.fetchone() or (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    total_listings = _to_int(row[0])
    qualified_count = _to_int(row[2])

    return {
        "snapshot": {
            "total_listings": total_listings,
            "new_this_week": _to_int(row[1]),
            "qualified_count": qualified_count,
            "pass_rate": _to_float(row[3], default=0.0) or 0.0,
            "active_sources": _to_int(row[4]),
            "distinct_industries": _to_int(row[5]),
        },
        "criteria_funnel": [
            {"stage": "All Listings", "count": total_listings},
            {"stage": "Local (US/CA)", "count": _to_int(row[6])},
            {"stage": "Cash Flow Fit", "count": _to_int(row[7])},
            {"stage": "Margin Fit", "count": _to_int(row[8])},
            {"stage": "Shortlist", "count": qualified_count},
        ],
        "data_quality": {
            "parseable_revenue_pct": _to_float(row[9], default=0.0) or 0.0,
            "parseable_ebitda_pct": _to_float(row[10], default=0.0) or 0.0,
            "parseable_cash_flow_pct": _to_float(row[11], default=0.0) or 0.0,
            "parseable_location_pct": _to_float(row[12], default=0.0) or 0.0,
        },
    }


def _fetch_source_yield(cur, *, lookback_days: int, country_scope: list[str]) -> list[dict[str, Any]]:
    cur.execute(
        """
        WITH scoped AS (
            SELECT source, country, gross_revenue_num, ebitda_num, cash_flow_num
            FROM raw_listings
            WHERE COALESCE(last_seen_date, first_seen_date) >= NOW() - (%s * INTERVAL '1 day')
        ),
        flagged AS (
            SELECT
                CASE
                    WHEN source IS NULL OR BTRIM(source) = '' OR UPPER(BTRIM(source)) = 'N/A' THEN 'Unknown'
                    ELSE source
                END AS source,
                UPPER(BTRIM(COALESCE(country, ''))) = ANY(%s::text[]) AS is_local,
                cash_flow_num >= 2000000 AS cash_flow_fit,
                (
                    gross_revenue_num IS NOT NULL
                    AND ebitda_num IS NOT NULL
                    AND gross_revenue_num > 0
                    AND (ebitda_num / gross_revenue_num) >= 0.10
                ) AS margin_fit
            FROM scoped
        )
        SELECT
            source,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE is_local AND cash_flow_fit AND margin_fit) AS qualified,
            COALESCE(
                (COUNT(*) FILTER (WHERE is_local AND cash_flow_fit AND margin_fit))::NUMERIC
                / NULLIF(COUNT(*), 0),
                0
            ) AS qualified_rate
        FROM flagged
        GROUP BY 1
        ORDER BY qualified_rate DESC, qualified DESC, total DESC, source ASC
        """,
        (lookback_days, country_scope),
    )
    rows = cur.fetchall()
    return [
        {
            "source": row[0],
            "total": _to_int(row[1]),
            "qualified": _to_int(row[2]),
            "qualified_rate": _to_float(row[3], default=0.0) or 0.0,
        }
        for row in rows
    ]


def _fetch_priority_queue(
    cur,
    *,
    lookback_days: int,
    country_scope: list[str],
    priority_limit: int,
) -> list[dict[str, Any]]:
    cur.execute(
        """
        WITH scoped AS (
            SELECT
                id,
                title,
                source,
                state,
                country,
                gross_revenue,
                ebitda,
                cash_flow,
                first_seen_date,
                gross_revenue_num,
                ebitda_num,
                cash_flow_num
            FROM raw_listings
            WHERE COALESCE(last_seen_date, first_seen_date) >= NOW() - (%s * INTERVAL '1 day')
        ),
        flagged AS (
            SELECT
                id,
                COALESCE(NULLIF(BTRIM(title), ''), 'N/A') AS title,
                CASE
                    WHEN source IS NULL OR BTRIM(source) = '' OR UPPER(BTRIM(source)) = 'N/A' THEN 'Unknown'
                    ELSE source
                END AS source,
                COALESCE(NULLIF(BTRIM(state), ''), 'N/A') AS state,
                COALESCE(NULLIF(BTRIM(country), ''), 'N/A') AS country,
                COALESCE(NULLIF(BTRIM(gross_revenue), ''), 'N/A') AS gross_revenue,
                COALESCE(NULLIF(BTRIM(ebitda), ''), 'N/A') AS ebitda,
                COALESCE(NULLIF(BTRIM(cash_flow), ''), 'N/A') AS cash_flow,
                first_seen_date,
                UPPER(BTRIM(COALESCE(country, ''))) = ANY(%s::text[]) AS is_local,
                cash_flow_num,
                gross_revenue_num,
                (
                    gross_revenue_num IS NOT NULL
                    AND ebitda_num IS NOT NULL
                    AND gross_revenue_num > 0
                    AND (ebitda_num / gross_revenue_num) >= 0.10
                ) AS margin_fit
            FROM scoped
        ),
        scored AS (
            SELECT
                id,
                title,
                source,
                state,
                country,
                gross_revenue,
                ebitda,
                cash_flow,
                first_seen_date,
                CASE WHEN is_local THEN 20 ELSE 0 END AS local_score,
                CASE
                    WHEN cash_flow_num >= 2000000 THEN 35
                    WHEN cash_flow_num >= 1000000 THEN 20
                    ELSE 0
                END AS cash_flow_score,
                CASE WHEN margin_fit THEN 25 ELSE 0 END AS margin_score,
                CASE WHEN gross_revenue_num >= 10000000 THEN 10 ELSE 0 END AS revenue_score,
                CASE WHEN first_seen_date >= NOW() - INTERVAL '7 days' THEN 10 ELSE 0 END AS freshness_score,
                ARRAY_REMOVE(
                    ARRAY[
                        CASE WHEN is_local THEN 'Local' END,
                        CASE
                            WHEN cash_flow_num >= 2000000 THEN 'Cash Flow Fit'
                            WHEN cash_flow_num >= 1000000 THEN 'Cash Flow Near Fit'
                            ELSE NULL
                        END,
                        CASE WHEN margin_fit THEN 'Margin Fit' END,
                        CASE WHEN gross_revenue_num >= 10000000 THEN 'Revenue >= $10M' END,
                        CASE WHEN first_seen_date >= NOW() - INTERVAL '7 days' THEN 'New This Week' END
                    ],
                    NULL
                ) AS reasons
            FROM flagged
        )
        SELECT
            id,
            title,
            source,
            state,
            country,
            gross_revenue,
            ebitda,
            cash_flow,
            first_seen_date,
            LEAST(100, local_score + cash_flow_score + margin_score + revenue_score + freshness_score) AS fit_score,
            reasons
        FROM scored
        ORDER BY fit_score DESC, first_seen_date DESC NULLS LAST, id DESC
        LIMIT %s
        """,
        (lookback_days, country_scope, priority_limit),
    )
    rows = cur.fetchall()
    output: list[dict[str, Any]] = []
    for row in rows:
        reasons = row[10] if isinstance(row[10], list) else []
        output.append(
            {
                "id": _to_int(row[0]),
                "title": row[1],
                "source": row[2],
                "state": row[3],
                "country": row[4],
                "gross_revenue": row[5],
                "ebitda": row[6],
                "cash_flow": row[7],
                "first_seen_date": _to_iso_datetime(row[8]),
                "fit_score": _to_int(row[9]),
                "reasons": reasons,
            }
        )
    return output


def _default_sla_payload() -> dict[str, Any]:
    return {
        "response_48h_rate": None,
        "offer_5d_rate": None,
        "close_60d_rate": None,
        "in_pipeline": None,
    }


def _pipeline_table_supported(cur) -> bool:
    cur.execute("SELECT to_regclass('public.pipeline')")
    table_ref = cur.fetchone()
    if not table_ref or table_ref[0] is None:
        return False

    required_columns = ["created_at", "responded_at", "offered_at", "closed_at", "status"]
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'pipeline'
          AND column_name = ANY(%s)
        """,
        (required_columns,),
    )
    found_columns = {row[0] for row in cur.fetchall()}
    return set(required_columns).issubset(found_columns)


def _fetch_sla(cur, *, lookback_days: int) -> dict[str, Any]:
    if not _pipeline_table_supported(cur):
        return _default_sla_payload()

    cur.execute(
        """
        SELECT
            (
                (COUNT(*) FILTER (
                    WHERE created_at IS NOT NULL
                      AND responded_at IS NOT NULL
                      AND responded_at <= created_at + INTERVAL '48 hours'
                ))::NUMERIC
                / NULLIF(
                    COUNT(*) FILTER (
                        WHERE created_at IS NOT NULL
                          AND responded_at IS NOT NULL
                    ),
                    0
                )
            ) AS response_48h_rate,
            (
                (COUNT(*) FILTER (
                    WHERE created_at IS NOT NULL
                      AND offered_at IS NOT NULL
                      AND offered_at <= created_at + INTERVAL '5 days'
                ))::NUMERIC
                / NULLIF(
                    COUNT(*) FILTER (
                        WHERE created_at IS NOT NULL
                          AND offered_at IS NOT NULL
                    ),
                    0
                )
            ) AS offer_5d_rate,
            (
                (COUNT(*) FILTER (
                    WHERE created_at IS NOT NULL
                      AND closed_at IS NOT NULL
                      AND closed_at <= created_at + INTERVAL '60 days'
                ))::NUMERIC
                / NULLIF(
                    COUNT(*) FILTER (
                        WHERE created_at IS NOT NULL
                          AND closed_at IS NOT NULL
                    ),
                    0
                )
            ) AS close_60d_rate,
            COUNT(*) FILTER (
                WHERE COALESCE(LOWER(BTRIM(status)), '') NOT IN ('closed', 'lost', 'dead')
            ) AS in_pipeline
        FROM pipeline
        WHERE created_at >= NOW() - (%s * INTERVAL '1 day')
        """,
        (lookback_days,),
    )
    row = cur.fetchone() or (None, None, None, None)
    return {
        "response_48h_rate": _to_float(row[0], default=None),
        "offer_5d_rate": _to_float(row[1], default=None),
        "close_60d_rate": _to_float(row[2], default=None),
        "in_pipeline": _to_int(row[3]) if row[3] is not None else None,
    }


@router.get("/dashboard/overview", response_model=DashboardOverviewResponse)
def dashboard_overview(
    lookback_days: int = Query(90, ge=1),
    priority_limit: int = Query(12, ge=1, le=50),
    country_scope: str = Query("US,CA"),
):
    """
    Aggregated dashboard payload for frontend overview cards and queues.

    Cached in-process for low-latency reads.
    """
    normalized_country_scope = _parse_country_scope(country_scope)
    cache_key = (lookback_days, priority_limit, tuple(normalized_country_scope))

    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with get_db() as conn:
        cur = conn.cursor()

        core = _fetch_snapshot_funnel_and_quality(
            cur,
            lookback_days=lookback_days,
            country_scope=normalized_country_scope,
        )
        source_yield = _fetch_source_yield(
            cur,
            lookback_days=lookback_days,
            country_scope=normalized_country_scope,
        )
        priority_queue = _fetch_priority_queue(
            cur,
            lookback_days=lookback_days,
            country_scope=normalized_country_scope,
            priority_limit=priority_limit,
        )
        sla_payload = _fetch_sla(cur, lookback_days=lookback_days)

        cur.close()

    payload = {
        "generated_at": _iso_utc_now(),
        "snapshot": core["snapshot"],
        "criteria_funnel": core["criteria_funnel"],
        "source_yield": source_yield,
        "priority_queue": priority_queue,
        "sla": sla_payload,
        "data_quality": core["data_quality"],
    }
    _cache_set(cache_key, payload)
    return payload
