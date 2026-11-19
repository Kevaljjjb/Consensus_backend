"""
Consensus — Upload API routes.

POST /api/upload/single  — insert one listing with duplicate detection
POST /api/upload/csv     — bulk upload from CSV with per-row dupe check
"""

import csv
import hashlib
import io
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel, Field

from db.connection import get_db
from db.operations import _normalise, _COLUMN_MAP
from embeddings import get_embedding

router = APIRouter(tags=["upload"])
logger = logging.getLogger(__name__)

# Upload limits — each CSV row can trigger a paid embedding API call, so cap
# both the payload size and the number of rows processed per request.
MAX_CSV_BYTES = 5 * 1024 * 1024  # 5 MB
MAX_CSV_ROWS = 1000
_EMBED_MAX_CHARS = 8000


# ── Helpers ──────────────────────────────────────────────────────────────────

def _hash_url(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _check_url_duplicate(cur, url: str) -> bool:
    """Level 1: exact URL match."""
    cur.execute("SELECT id FROM raw_listings WHERE url = %s", (url,))
    return cur.fetchone() is not None


def _embed_description(description: str) -> Optional[str]:
    """
    Embed a listing description once and return it as a pgvector literal.

    Returns None for empty descriptions or if the embedding API fails, so an
    upload never fails just because embeddings are unavailable.
    """
    if not description or description == "N/A":
        return None

    try:
        vec = get_embedding(description[:_EMBED_MAX_CHARS])
    except Exception as exc:
        logger.warning("Embedding failed; storing listing without embedding: %s", exc)
        return None

    return "[" + ",".join(str(v) for v in vec) + "]"


def _check_semantic_duplicate(cur, vec_str: Optional[str], threshold: float = 0.15) -> List[dict]:
    """
    Level 2: semantic similarity check.
    Returns listings within the cosine distance threshold of ``vec_str``.
    """
    if not vec_str:
        return []

    cur.execute(
        """
        SELECT id, title, url, source, city, state,
               (description_embedding <=> %s::vector) AS distance
        FROM raw_listings
        WHERE description_embedding IS NOT NULL
          AND (description_embedding <=> %s::vector) < %s
        ORDER BY distance
        LIMIT 5
        """,
        (vec_str, vec_str, threshold),
    )
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _insert_listing(cur, data: dict, embedding=None) -> int:
    """Insert a single listing row and return its ID."""
    url = data.get("url", "")
    listing_hash = _hash_url(url)

    cur.execute(
        """
        INSERT INTO raw_listings (
            url, listing_hash, source,
            title, city, state, country, industry, description,
            listed_by_firm, listed_by_name, phone, email,
            price, gross_revenue, cash_flow, inventory, ebitda,
            financial_data, source_link, extra_information, deal_date,
            scraping_date, first_seen_date, last_seen_date, description_embedding
        ) VALUES (
            %(url)s, %(listing_hash)s, %(source)s,
            %(title)s, %(city)s, %(state)s, %(country)s, %(industry)s, %(description)s,
            %(listed_by_firm)s, %(listed_by_name)s, %(phone)s, %(email)s,
            %(price)s, %(gross_revenue)s, %(cash_flow)s, %(inventory)s, %(ebitda)s,
            %(financial_data)s, %(source_link)s, %(extra_information)s, %(deal_date)s,
            %(scraping_date)s, NOW(), NOW(), %(embedding)s
        )
        ON CONFLICT (url) DO UPDATE SET
            last_seen_date = NOW(),
            title = EXCLUDED.title,
            price = EXCLUDED.price,
            gross_revenue = EXCLUDED.gross_revenue,
            cash_flow = EXCLUDED.cash_flow,
            ebitda = EXCLUDED.ebitda,
            description = EXCLUDED.description,
            description_embedding = EXCLUDED.description_embedding
        RETURNING id
        """,
        {
            "url": url,
            "listing_hash": listing_hash,
            "source": data.get("source", "Manual"),
            "title": data.get("title", "N/A"),
            "city": data.get("city", "N/A"),
            "state": data.get("state", "N/A"),
            "country": data.get("country", "US"),
            "industry": data.get("industry", "N/A"),
            "description": data.get("description", "N/A"),
            "listed_by_firm": data.get("listed_by_firm", "N/A"),
            "listed_by_name": data.get("listed_by_name", "N/A"),
            "phone": data.get("phone", "N/A"),
            "email": data.get("email", "N/A"),
            "price": data.get("price", "N/A"),
            "gross_revenue": data.get("gross_revenue", "N/A"),
            "cash_flow": data.get("cash_flow", "N/A"),
            "inventory": data.get("inventory", "N/A"),
            "ebitda": data.get("ebitda", "N/A"),
            "financial_data": data.get("financial_data", "N/A"),
            "source_link": data.get("source_link", data.get("url", "N/A")),
            "extra_information": data.get("extra_information", "N/A"),
            "deal_date": data.get("deal_date", "N/A"),
            "scraping_date": datetime.now().strftime("%Y-%m-%d"),
            "embedding": embedding,
        },
    )
    return cur.fetchone()[0]


# ── Request models ───────────────────────────────────────────────────────────

class SingleDealRequest(BaseModel):
    title: str
    url: str = ""
    source: str = "Manual"
    industry: str = ""
    city: str = ""
    state: str = ""
    country: str = "US"
    description: str = ""
    listed_by_firm: str = ""
    listed_by_name: str = ""
    phone: str = ""
    email: str = ""
    price: str = ""
    gross_revenue: str = ""
    cash_flow: str = ""
    ebitda: str = ""
    inventory: str = ""
    deal_date: str = ""
    financial_data: str = ""
    source_link: str = ""
    extra_information: str = ""


# ── Routes ───────────────────────────────────────────────────────────────────

@router.post("/upload/single")
def upload_single(deal: SingleDealRequest):
    """
    Upload a single deal with duplication detection.

    Returns:
      - inserted: True if the deal was inserted
      - duplicate_url: True if an exact URL match was found
      - similar_listings: list of semantically similar listings (potential duplicates)
    """
    with get_db() as conn:
        cur = conn.cursor()

        data = deal.model_dump()

        # Generate URL if missing
        if not data["url"]:
            data["url"] = f"manual://{_hash_url(data['title'] + data.get('description', ''))}"

        # Level 1: exact URL duplicate check
        is_url_dup = _check_url_duplicate(cur, data["url"])

        # Embed once — reused for the semantic check and for storage.
        embedding_str = _embed_description(data.get("description", ""))

        # Level 2: semantic duplicate check
        similar = _check_semantic_duplicate(cur, embedding_str)

        if is_url_dup:
            cur.close()
            return {
                "inserted": False,
                "duplicate_url": True,
                "similar_listings": similar,
                "message": "A listing with this exact URL already exists.",
            }

        new_id = _insert_listing(cur, data, embedding=embedding_str)
        conn.commit()
        cur.close()

    return {
        "inserted": True,
        "id": new_id,
        "duplicate_url": False,
        "similar_listings": similar,
        "message": f"Deal '{data['title']}' created successfully."
        + (f" Warning: {len(similar)} similar listing(s) found." if similar else ""),
    }


@router.post("/upload/csv")
def upload_csv(file: UploadFile = File(...)):
    """
    Bulk upload deals from a CSV file.

    Expected CSV columns match the scraper output schema (Title, URL, Source, etc.).
    Returns summary: inserted count, skipped (duplicates), and similar listings.

    Each row runs inside its own SAVEPOINT, so one bad row is rolled back on
    its own without discarding the rows inserted before it.

    Declared as a sync route so FastAPI runs it in a worker thread — the
    per-row embedding HTTP calls and DB work would otherwise block the event loop.
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted.")

    content = file.file.read(MAX_CSV_BYTES + 1)
    if len(content) > MAX_CSV_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"CSV file is too large (max {MAX_CSV_BYTES // (1024 * 1024)} MB).",
        )

    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    with get_db() as conn:
        cur = conn.cursor()

        inserted = 0
        skipped = 0
        errors = []
        all_similar: List[dict] = []
        truncated = False

        for row_num, csv_row in enumerate(reader, start=2):  # row 1 = header
            if row_num - 1 > MAX_CSV_ROWS:
                truncated = True
                break

            # Map CSV columns to DB columns
            data = {}
            for csv_key, db_col in _COLUMN_MAP.items():
                val = csv_row.get(csv_key, "")
                data[db_col] = _normalise(val)
            data["listing_hash"] = _hash_url(data.get("url", ""))

            url = data.get("url", "")
            if not url or url == "N/A":
                errors.append({"row": row_num, "error": "Missing URL"})
                continue

            cur.execute("SAVEPOINT csv_row")
            try:
                # Level 1: URL dupe check
                if _check_url_duplicate(cur, url):
                    skipped += 1
                    cur.execute("RELEASE SAVEPOINT csv_row")
                    continue

                # Embed once — reused for the semantic check and for storage.
                embedding_str = _embed_description(data.get("description", ""))

                # Level 2: semantic dupe check (lighter — just flag, don't block)
                similar = _check_semantic_duplicate(cur, embedding_str, threshold=0.15)
                if similar:
                    all_similar.append({
                        "row": row_num,
                        "title": data.get("title", "N/A"),
                        "similar_to": similar,
                    })

                _insert_listing(cur, data, embedding=embedding_str)
                cur.execute("RELEASE SAVEPOINT csv_row")
                inserted += 1
            except Exception as exc:
                # Undo only this row; earlier rows stay in the transaction.
                cur.execute("ROLLBACK TO SAVEPOINT csv_row")
                errors.append({"row": row_num, "error": str(exc)})

        conn.commit()
        cur.close()

    message = f"Processed CSV: {inserted} inserted, {skipped} skipped (duplicates), {len(errors)} errors."
    if truncated:
        message += f" Only the first {MAX_CSV_ROWS} rows were processed."

    return {
        "inserted": inserted,
        "skipped_duplicates": skipped,
        "errors": errors[:20],  # Limit error output
        "potential_duplicates": all_similar[:20],
        "truncated": truncated,
        "message": message,
    }
