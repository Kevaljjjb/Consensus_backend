"""
Consensus - per-listing AI evaluation routes.

GET  /api/listings/{listing_id}/evaluation
POST /api/listings/{listing_id}/evaluation/refresh
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from psycopg2.extras import Json

from api.deal_scoring import evaluate_deal
from db.connection import get_db

router = APIRouter(tags=["evaluations"])

_LISTING_SELECT_COLUMNS = """
id, url, source, title, city, state, country, industry, description,
listed_by_firm, listed_by_name, phone, email,
price, gross_revenue, cash_flow, inventory, ebitda,
financial_data, source_link, extra_information, deal_date,
first_seen_date, last_seen_date, scraping_date
"""

_EVALUATION_SELECT_COLUMNS = """
fit_score,
score_breakdown,
pros,
cons,
summary,
model_used,
evaluated_at
"""


class ScoreBreakdownResponse(BaseModel):
    cash_flow: int = Field(..., ge=0, le=25)
    profitability: int = Field(..., ge=0, le=25)
    maturity: int = Field(..., ge=0, le=20)
    locality: int = Field(..., ge=0, le=15)
    stability: int = Field(..., ge=0, le=15)


class EvaluationResponse(BaseModel):
    fit_score: int = Field(..., ge=0, le=100)
    score_breakdown: ScoreBreakdownResponse
    pros: list[str] = Field(default_factory=list)
    cons: list[str] = Field(default_factory=list)
    summary: str
    model_used: str
    evaluated_at: datetime
    cached: bool


def _row_to_dict(row: tuple[Any, ...], columns: list[str]) -> dict[str, Any]:
    return dict(zip(columns, row))


def _coerce_json_field(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _evaluation_row_to_response(row: tuple[Any, ...], columns: list[str], *, cached: bool) -> dict[str, Any]:
    payload = _row_to_dict(row, columns)
    payload["score_breakdown"] = _coerce_json_field(payload["score_breakdown"])
    payload["pros"] = _coerce_json_field(payload["pros"])
    payload["cons"] = _coerce_json_field(payload["cons"])
    payload["cached"] = cached
    return payload


def _get_cached_evaluation(cur, listing_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT {_EVALUATION_SELECT_COLUMNS}
        FROM deal_evaluations
        WHERE listing_id = %s
        """,
        (listing_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    columns = [desc[0] for desc in cur.description]
    return _evaluation_row_to_response(row, columns, cached=True)


def _get_listing(cur, listing_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT {_LISTING_SELECT_COLUMNS}
        FROM raw_listings
        WHERE id = %s
        """,
        (listing_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    columns = [desc[0] for desc in cur.description]
    return _row_to_dict(row, columns)


def _persist_evaluation(conn, cur, listing_id: int, evaluation: dict[str, Any], *, replace_existing: bool) -> dict[str, Any]:
    try:
        if replace_existing:
            cur.execute("DELETE FROM deal_evaluations WHERE listing_id = %s", (listing_id,))

        cur.execute(
            f"""
            INSERT INTO deal_evaluations (
                listing_id,
                fit_score,
                score_breakdown,
                pros,
                cons,
                summary,
                model_used
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (listing_id) DO UPDATE SET
                fit_score = EXCLUDED.fit_score,
                score_breakdown = EXCLUDED.score_breakdown,
                pros = EXCLUDED.pros,
                cons = EXCLUDED.cons,
                summary = EXCLUDED.summary,
                model_used = EXCLUDED.model_used,
                evaluated_at = NOW()
            RETURNING {_EVALUATION_SELECT_COLUMNS}
            """,
            (
                listing_id,
                evaluation["fit_score"],
                Json(evaluation["score_breakdown"]),
                Json(evaluation["pros"]),
                Json(evaluation["cons"]),
                evaluation["summary"],
                evaluation["model_used"],
            ),
        )
        row = cur.fetchone()
        columns = [desc[0] for desc in cur.description]
        conn.commit()
        return _evaluation_row_to_response(row, columns, cached=False)
    except Exception:
        conn.rollback()
        raise


@router.get("/listings/{listing_id}/evaluation", response_model=EvaluationResponse)
def get_listing_evaluation(listing_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        try:
            cached_evaluation = _get_cached_evaluation(cur, listing_id)
            if cached_evaluation is not None:
                return cached_evaluation

            listing = _get_listing(cur, listing_id)
            if listing is None:
                raise HTTPException(status_code=404, detail="Listing not found")

            evaluation = evaluate_deal(listing)
            return _persist_evaluation(conn, cur, listing_id, evaluation, replace_existing=False)
        finally:
            cur.close()


@router.post("/listings/{listing_id}/evaluation/refresh", response_model=EvaluationResponse)
def refresh_listing_evaluation(listing_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        try:
            listing = _get_listing(cur, listing_id)
            if listing is None:
                raise HTTPException(status_code=404, detail="Listing not found")

            evaluation = evaluate_deal(listing)
            return _persist_evaluation(conn, cur, listing_id, evaluation, replace_existing=True)
        finally:
            cur.close()
