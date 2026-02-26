"""
Tucker's Farm — Database operations for raw_listings.

Provides upsert logic with Level 1 deduplication (URL-based re-scrape guard).
Null / zero / blank values are normalised to 'N/A' before storage.
"""

import hashlib
from typing import Any, Dict, List


# ── Value normalisation ──────────────────────────────────────────────────────

_FALSY = {"", "0", "$0", "0.0", "0.00", "$0.00", "none", "n/a", "null"}


def _normalise(value: Any) -> str:
    """Convert blank / zero / None values to 'N/A'."""
    if value is None:
        return "N/A"
    s = str(value).strip()
    if s.lower() in _FALSY:
        return "N/A"
    return s


def _hash_url(url: str) -> str:
    """Deterministic SHA-256 hash of the listing URL."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


# ── Column mapping ───────────────────────────────────────────────────────────
# Maps scraper dict keys → raw_listings columns.

_COLUMN_MAP = {
    "Title":              "title",
    "City":               "city",
    "State":              "state",
    "Country":            "country",
    "URL":                "url",
    "Industry":           "industry",
    "Source":             "source",
    "Description":        "description",
    "Listed By (Firm)":   "listed_by_firm",
    "Listed By (Name)":   "listed_by_name",
    "Phone":              "phone",
    "Email":              "email",
    "Price":              "price",
    "Gross Revenue":      "gross_revenue",
    "Cash Flow":          "cash_flow",
    "Inventory":          "inventory",
    "EBITDA":             "ebitda",
    "Scraping Date":      "scraping_date",
    "Financial Data":     "financial_data",
    "source Link":        "source_link",
    "Extra Information":  "extra_information",
    "Deal Date":          "deal_date",
}


def _row_to_db(row: Dict[str, str]) -> Dict[str, str]:
    """Convert a scraper output dict to a DB-ready dict with normalised values."""
    db_row: Dict[str, str] = {}
    for scraper_key, db_col in _COLUMN_MAP.items():
        db_row[db_col] = _normalise(row.get(scraper_key))
    # Computed fields
    db_row["listing_hash"] = _hash_url(db_row["url"])
    return db_row


# ── Upsert SQL ───────────────────────────────────────────────────────────────

_UPSERT_SQL = """
INSERT INTO raw_listings (
    url, listing_hash, source,
    title, city, state, country, industry, description,
    listed_by_firm, listed_by_name, phone, email,
    price, gross_revenue, cash_flow, inventory, ebitda,
    financial_data, source_link, extra_information, deal_date,
    scraping_date, first_seen_date, last_seen_date
) VALUES (
    %(url)s, %(listing_hash)s, %(source)s,
    %(title)s, %(city)s, %(state)s, %(country)s, %(industry)s, %(description)s,
    %(listed_by_firm)s, %(listed_by_name)s, %(phone)s, %(email)s,
    %(price)s, %(gross_revenue)s, %(cash_flow)s, %(inventory)s, %(ebitda)s,
    %(financial_data)s, %(source_link)s, %(extra_information)s, %(deal_date)s,
    %(scraping_date)s, NOW(), NOW()
)
ON CONFLICT (url) DO UPDATE SET
    last_seen_date      = NOW(),
    title               = EXCLUDED.title,
    price               = EXCLUDED.price,
    gross_revenue       = EXCLUDED.gross_revenue,
    cash_flow           = EXCLUDED.cash_flow,
    ebitda              = EXCLUDED.ebitda,
    inventory           = EXCLUDED.inventory,
    financial_data      = EXCLUDED.financial_data,
    extra_information   = EXCLUDED.extra_information,
    description         = EXCLUDED.description,
    scraping_date       = EXCLUDED.scraping_date
;
"""


def upsert_listing(cursor, row: Dict[str, str]) -> None:
    """
    Insert a single listing or update it if the URL already exists.

    Level 1 dedup: URL is the unique key.
    - New URL  → INSERT with first_seen_date = NOW()
    - Same URL → UPDATE mutable fields + last_seen_date = NOW()
    """
    db_row = _row_to_db(row)
    cursor.execute(_UPSERT_SQL, db_row)


def bulk_upsert_listings(cursor, rows: List[Dict[str, str]]) -> int:
    """
    Upsert a batch of listings.  Returns the count of rows processed.
    """
    count = 0
    for row in rows:
        upsert_listing(cursor, row)
        count += 1
    return count


def get_existing_urls(cursor, source: str) -> set:
    """Return the set of URLs already stored for a given source."""
    cursor.execute(
        "SELECT url FROM raw_listings WHERE source = %s",
        (source,),
    )
    return {r[0] for r in cursor.fetchall()}
