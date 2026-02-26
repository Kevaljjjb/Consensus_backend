"""
BizBen scraper – uses the site's XHR search API directly.

Endpoint : POST https://j2wbssljg5.execute-api.us-east-1.amazonaws.com/Prod/search-direct
Pagination: nextPageToken returned in each response
"""

import argparse
import csv
import html
import os
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

# Load .env so DATABASE_URL is available
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Add parent dir to path so we can import the db module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db.connection import get_connection
from db.operations import bulk_upsert_listings

# ── Output schema (matches BizBuySell scraper) ──────────────────────────────

OUTPUT_COLUMNS = [
    "Title",
    "City",
    "State",
    "Country",
    "URL",
    "Industry",
    "Source",
    "Description",
    "Listed By (Firm)",
    "Listed By (Name)",
    "Phone",
    "Email",
    "Price",
    "Gross Revenue",
    "Cash Flow",
    "Inventory",
    "EBITDA",
    "Scraping Date",
    "Financial Data",
    "source Link",
    "Extra Information",
    "Deal Date",
]

BIZBEN_API_URL = (
    "https://j2wbssljg5.execute-api.us-east-1.amazonaws.com/Prod/search-direct"
)

BIZBEN_BASE = "https://www.bizben.com"

# Headers that mimic a normal browser XHR request
DEFAULT_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "cache-control": "no-cache",
    "content-type": "application/json",
    "dnt": "1",
    "origin": BIZBEN_BASE,
    "pragma": "no-cache",
    "referer": f"{BIZBEN_BASE}/",
    "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}


# ── Financial filter helpers ─────────────────────────────────────────────────

def _parse_money(value: str) -> float:
    """Parse a money string like '$2,500,000' or '2500000' to a float. Returns 0.0 on failure."""
    if not value or value == "N/A":
        return 0.0
    cleaned = re.sub(r"[^\d.]", "", str(value))
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


def passes_financial_filter(row: Dict[str, str], mode: str = "non-software") -> bool:
    """
    Apply the priority-based financial filter from project_master.md.

    Non-Software Deals (priority order — first available metric wins):
      1. CF or EBITDA >= $2M  (always checked first)
      2. Revenue >= $5M       (only if CF AND EBITDA are missing)
      3. List Price >= $6M    (only if CF, EBITDA, AND Revenue are missing)

    Software Deals:
      Revenue >= $2M regardless of CF or EBITDA
    """
    cf = _parse_money(row.get("Cash Flow", ""))
    ebitda = _parse_money(row.get("EBITDA", ""))
    revenue = _parse_money(row.get("Gross Revenue", ""))
    price = _parse_money(row.get("Price", ""))

    if mode == "software":
        return revenue >= 2_000_000

    # Non-software: priority-based
    has_cf = cf > 0
    has_ebitda = ebitda > 0
    has_revenue = revenue > 0

    # Priority 1: CF or EBITDA available → check >= $2M
    if has_cf or has_ebitda:
        return cf >= 2_000_000 or ebitda >= 2_000_000

    # Priority 2: Revenue available (but CF and EBITDA both missing)
    if has_revenue:
        return revenue >= 5_000_000

    # Priority 3: Only price available
    if price > 0:
        return price >= 6_000_000

    # No financial data at all → exclude
    return False


# ── Helpers ──────────────────────────────────────────────────────────────────

def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


_FALSY = {"", "0", "$0", "0.0", "0.00", "$0.00", "none", "n/a", "null"}


def _safe_str(value: Any) -> str:
    """Convert a value to a cleaned string; returns 'N/A' for blanks / zeros."""
    if value is None:
        return "N/A"
    if isinstance(value, (int, float)):
        s = str(value)
    else:
        s = str(value).strip()
    if s.lower() in _FALSY:
        return "N/A"
    return s


def _build_industry(item: dict) -> str:
    """Combine businessCategory and businessTypes into 'Category (Type1, Type2)'."""
    cat = item.get("businessCategory", "")
    types = item.get("businessTypes", [])

    # businessCategory can be a string or a list
    if isinstance(cat, list):
        cat = ", ".join(c for c in cat if c)
    cat = _safe_str(cat)

    type_str = ", ".join(t for t in types if t) if types else ""

    if cat and type_str:
        return f"{cat} ({type_str})"
    return cat or type_str


def _build_name(item: dict) -> str:
    """Build full name from firstName + lastName, fallback to contactName."""
    first = _safe_str(item.get("firstName", ""))
    last = _safe_str(item.get("lastName", ""))
    full = f"{first} {last}".strip()
    if full:
        return full
    return _safe_str(item.get("contactName", ""))


def _build_firm(item: dict) -> str:
    """Extract firm name — not available in the API, left empty."""
    return ""


def _build_city(item: dict) -> str:
    """Build city from county field + ' County'."""
    county = _safe_str(item.get("county", ""))
    if county:
        return f"{county}"
    return ""


def _build_url(item: dict) -> str:
    """Build the full BizBen listing URL from urlPath."""
    path = item.get("urlPath", "")
    if not path:
        return ""
    return f"{BIZBEN_BASE}/business-for-sale/{path}"


def _get_revenue(item: dict) -> str:
    """Get revenue, preferring the integer version."""
    rev = item.get("revenueInt") or item.get("revenue")
    return _safe_str(rev)


def _get_ebitda(item: dict) -> str:
    """Get EBITDA / adjusted net."""
    val = item.get("adjustedNet", "")
    return _safe_str(val)


def _build_extra_info(item: dict) -> str:
    """Assemble extra information useful for PE fund evaluation."""
    parts: List[str] = []

    employees = item.get("employees")
    if employees:
        parts.append(f"Employees: {employees}")

    year = item.get("establishedYear")
    if year:
        parts.append(f"Established: {year}")

    sqft = item.get("sizeInSquareFeet")
    if sqft:
        parts.append(f"Size: {sqft} sqft")

    reason = _safe_str(item.get("saleReason", ""))
    if reason:
        parts.append(f"Sale Reason: {reason}")

    attrs = item.get("businessAttributes", [])
    if attrs:
        parts.append(f"Attributes: {', '.join(attrs)}")

    down = _safe_str(item.get("down", ""))
    if down:
        parts.append(f"Down Payment: {down}")

    status = _safe_str(item.get("businessStatus", ""))
    if status:
        parts.append(f"Status: {status}")

    ffe = _safe_str(item.get("ffe", ""))
    if ffe:
        parts.append(f"FF&E: {ffe}")

    return " | ".join(parts)


def _format_deal_date(item: dict) -> str:
    """Convert createdAt epoch-ms to YYYY-MM-DD."""
    ts = item.get("createdAt")
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(int(ts) / 1000).strftime("%Y-%m-%d")
    except (ValueError, TypeError, OSError):
        return ""


# ── Mapping a single API result → CSV row ────────────────────────────────────

def map_listing(item: dict) -> Dict[str, str]:
    """Map one API result object to the output CSV schema."""
    price = _safe_str(item.get("askingPrice", ""))
    revenue = _get_revenue(item)
    cash_flow = _safe_str(item.get("cashFlow", ""))
    ebitda = _get_ebitda(item)
    url = _build_url(item)

    financial_parts: List[str] = []
    if revenue:
        financial_parts.append(f"Revenue: {revenue}")
    if cash_flow:
        financial_parts.append(f"Cash Flow: {cash_flow}")
    if ebitda:
        financial_parts.append(f"EBITDA: {ebitda}")

    return {
        "Title": _safe_str(item.get("title", "")),
        "City": _build_city(item),
        "State": _safe_str(item.get("state", "")),
        "Country": "US",
        "URL": url,
        "Industry": _build_industry(item),
        "Source": "BizBen",
        "Description": _strip_html(_safe_str(item.get("description", ""))),
        "Listed By (Firm)": _build_firm(item),
        "Listed By (Name)": _build_name(item),
        "Phone": _safe_str(item.get("phoneNumber", "")),
        "Email": _safe_str(item.get("email", "")),
        "Price": price,
        "Gross Revenue": revenue,
        "Cash Flow": cash_flow,
        "Inventory": "",
        "EBITDA": ebitda,
        "Scraping Date": datetime.now().strftime("%Y-%m-%d"),
        "Financial Data": "; ".join(financial_parts),
        "source Link": url,
        "Extra Information": _build_extra_info(item),
        "Deal Date": _format_deal_date(item),
    }


# ── API interaction ──────────────────────────────────────────────────────────

def fetch_page(
    session: requests.Session,
    cash_flow_min: int,
    next_page_token: Optional[dict] = None,
    retries: int = 3,
) -> dict:
    """Call the BizBen search-direct API and return the JSON response."""
    payload: Dict[str, Any] = {"cashFlowMin": cash_flow_min}
    if next_page_token:
        payload["nextPageToken"] = next_page_token

    for attempt in range(1, retries + 1):
        try:
            resp = session.post(
                BIZBEN_API_URL,
                json=payload,
                headers=DEFAULT_HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            print(f"  API error (attempt {attempt}/{retries}): {exc}")
            if attempt < retries:
                time.sleep(attempt * 3)
    return {}


# ── CSV output ───────────────────────────────────────────────────────────────

def write_rows(output_csv: str, rows: List[Dict[str, str]]) -> None:
    with open(output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in OUTPUT_COLUMNS})


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape BizBen listings via their search API."
    )
    parser.add_argument(
        "--output-csv",
        default="data_collection/bizben_scraped.csv",
        help="Output CSV path (default: data_collection/bizben_scraped.csv)",
    )
    parser.add_argument(
        "--csv-only",
        action="store_true",
        help="Write to CSV only (skip database). Useful for debugging.",
    )
    parser.add_argument(
        "--cash-flow-min",
        type=int,
        default=100000,
        help="Minimum cash-flow filter sent to the API (default: 100000)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max listings to collect (0 = all).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Seconds to wait between API pages (default: 2).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Max number of API pages to fetch (0 = unlimited).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retries per API call on failure (default: 3).",
    )
    parser.add_argument(
        "--mode",
        choices=["non-software", "software", "all"],
        default="all",
        help="Financial filter mode: 'non-software', 'software', or 'all' (no filter). Default: all.",
    )
    args = parser.parse_args()

    session = requests.Session()
    all_rows: List[Dict[str, str]] = []
    next_token: Optional[dict] = None
    page_num = 0

    print(f"Starting BizBen scrape  (cashFlowMin={args.cash_flow_min})")

    while True:
        page_num += 1

        if args.max_pages and page_num > args.max_pages:
            print(f"Reached max pages ({args.max_pages}). Stopping.")
            break

        print(f"\n── Page {page_num} ──")
        data = fetch_page(
            session,
            cash_flow_min=args.cash_flow_min,
            next_page_token=next_token,
            retries=args.retries,
        )

        results = data.get("results", [])
        if not results:
            print("No more results.")
            break

        for item in results:
            row = map_listing(item)

            # Apply financial filter if a mode is set
            if args.mode != "all" and not passes_financial_filter(row, args.mode):
                continue

            all_rows.append(row)
            print(f"  [{len(all_rows)}] {row['Title'][:80]}")

            if args.limit and len(all_rows) >= args.limit:
                break

        if args.limit and len(all_rows) >= args.limit:
            print(f"Reached limit ({args.limit}). Stopping.")
            break

        # Check for next page
        next_token = data.get("nextPageToken")
        if not next_token:
            print("No nextPageToken — all pages fetched.")
            break

        print(f"  Next page token received. Waiting {args.delay}s …")
        time.sleep(args.delay)

    # ── Write output ──────────────────────────────────────────────────────
    if args.csv_only:
        write_rows(args.output_csv, all_rows)
        print(f"\nDone. Saved {len(all_rows)} listings → {args.output_csv}")
    else:
        # Write to Supabase PostgreSQL
        print(f"\nWriting {len(all_rows)} listings to database …")
        conn = get_connection()
        try:
            cur = conn.cursor()
            count = bulk_upsert_listings(cur, all_rows)
            conn.commit()
            print(f"✅ Upserted {count} listings into raw_listings.")
        except Exception as exc:
            conn.rollback()
            print(f"❌ Database error: {exc}")
            # Fallback: save to CSV so data isn't lost
            write_rows(args.output_csv, all_rows)
            print(f"   Fallback: saved to {args.output_csv}")
        finally:
            conn.close()


if __name__ == "__main__":
    main()
