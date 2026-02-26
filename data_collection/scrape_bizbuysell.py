import argparse
import csv
import os
import random
import re
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

from camoufox.sync_api import Camoufox
from playwright.sync_api import Page
from dotenv import load_dotenv

# Load .env so DATABASE_URL is available
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Add parent dir to path so we can import the db module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db.connection import get_connection
from db.operations import upsert_listing


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


BLOCK_PATTERNS = [
    "access denied",
    "forbidden",
    "temporarily unavailable",
    "blocked",
    "attention required",
    "verify you are human",
    "captcha",
    "enable javascript and cookies",
]


_FALSY = {"", "0", "$0", "0.0", "0.00", "$0.00", "none", "n/a", "null"}


def clean_text(value: str) -> str:
    """Clean whitespace; returns 'N/A' for blank / zero values."""
    if not value:
        return "N/A"
    s = re.sub(r"\s+", " ", value).strip()
    if s.lower() in _FALSY:
        return "N/A"
    return s


def load_urls_from_csv(csv_path: str) -> List[str]:
    urls: List[str] = []
    seen = set()

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            for cell in row:
                cell = (cell or "").strip()
                if cell.startswith("http") and "bizbuysell.com" in cell:
                    if cell not in seen:
                        seen.add(cell)
                        urls.append(cell)
                    break

    return urls


def looks_blocked(page: Page) -> bool:
    """Check if the current page looks like a bot-detection block page."""
    try:
        title = (page.title() or "").lower()
        if "access denied" in title or "attention required" in title:
            return True
    except Exception:
        pass
        
    try:
        source = (page.content() or "").lower()[:12000]
        # Only consider it blocked if we don't see typical listing markers
        if any(pattern in source for pattern in BLOCK_PATTERNS):
            # If we see 'asking price' or other cues, it might have loaded despite patterns
            if "asking price" in source or "business description" in source:
                return False
            return True
    except Exception:
        pass
    return False


def human_delay(min_s: float = 1.5, max_s: float = 3.5) -> None:
    """Sleep for a random human-like duration."""
    time.sleep(random.uniform(min_s, max_s))


def open_with_stealth(
    page: Page,
    url: str,
    retries: int = 3,
    min_delay: float = 2.0,
    max_delay: float = 4.5,
) -> None:
    """Navigate to *url* with retry logic for bot-detection blocks."""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            print(f"  Attempt {attempt}/{retries}: Navigating to {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            # Additional wait for network to settle - Akamai sometimes takes a second
            try:
                page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
                
            human_delay(min_delay, max_delay)

            if not looks_blocked(page):
                return

            print(f"  Blocked or incomplete page detected (attempt {attempt}/{retries})")
        except Exception as exc:
            last_error = exc
            print(f"  Navigation error (attempt {attempt}/{retries}): {exc}")

        # Back-off before retry
        time.sleep(min(15, attempt * 5))

    if last_error:
        raise RuntimeError(f"Failed to open {url}: {last_error}")
    raise RuntimeError(f"Failed to bypass block page for {url}")


def login(page: Page, email: str, password: str) -> bool:
    if not email or not password:
        print("Skipping login: BIZBUYSELL_EMAIL / BIZBUYSELL_PASSWORD not set.")
        return False

    try:
        open_with_stealth(page, "https://www.bizbuysell.com/users/login.aspx", retries=2)

        email_sel = "#ctl00_ctl00_Content_ContentPlaceHolder1_LoginControl_txtUserName"
        password_sel = "#ctl00_ctl00_Content_ContentPlaceHolder1_LoginControl_txtPassword"
        submit_sel = "#ctl00_ctl00_Content_ContentPlaceHolder1_LoginControl_BtnLogin"

        page.wait_for_selector(email_sel, timeout=20000)
        page.fill(email_sel, email)
        page.fill(password_sel, password)
        page.click(submit_sel)
        human_delay(3, 5)
        return True
    except Exception as exc:
        print(f"Login failed: {exc}")
        return False


def first_text(page: Page, selectors: List[str]) -> str:
    """Return the first non-empty text content found across *selectors*."""
    for sel in selectors:
        try:
            # Use XPath if selector starts with // or xpath=
            is_xpath = sel.startswith("//") or sel.startswith("xpath=")
            if is_xpath:
                el = page.query_selector(sel if sel.startswith("xpath=") else f"xpath={sel}")
            else:
                el = page.query_selector(sel)
                
            if el:
                text = clean_text(el.text_content() or "")
                if text:
                    return text
        except Exception:
            pass
    return ""


def first_attr(page: Page, selectors: List[str], attr: str) -> str:
    """Return the first non-empty attribute value found across *selectors*."""
    for sel in selectors:
        try:
            is_xpath = sel.startswith("//") or sel.startswith("xpath=")
            if is_xpath:
                el = page.query_selector(sel if sel.startswith("xpath=") else f"xpath={sel}")
            else:
                el = page.query_selector(sel)
                
            if el:
                value = clean_text(el.get_attribute(attr) or "")
                if value:
                    return value
        except Exception:
            pass
    return ""


def parse_location(location: str) -> Dict[str, str]:
    city = ""
    state = ""
    country = "US"

    location = clean_text(location)
    if not location:
        return {"city": city, "state": state, "country": country}

    parts = [clean_text(part) for part in location.split(",") if clean_text(part)]
    if len(parts) >= 2:
        city = parts[0]
        state = parts[1]
        if len(parts) >= 3:
            country = parts[2]
    elif len(parts) == 1:
        city = parts[0]

    return {"city": city, "state": state, "country": country}


def parse_listing(page: Page, url: str, retries: int = 3) -> Dict[str, str]:
    open_with_stealth(page, url, retries=retries)

    # Wait for main content marker
    try:
        page.wait_for_selector("h1", timeout=10000)
    except Exception:
        pass

    title = first_text(page, ["h1"])
    location = first_text(
        page,
        [
            "h2.gray",
            ".listing-location",
            ".business-location",
        ],
    )
    location_parts = parse_location(location)

    # Updated XPaths from User
    description = first_text(page, ['//div[contains(@class,"businessDescription")]'])
    
    # Financials
    price = first_text(page, ['//span[contains(text(), "Asking Price:")]/following-sibling::span'])
    gross_revenue = first_text(page, ['//span[contains(text(), "Gross Revenue:")]/following-sibling::span'])
    cash_flow = first_text(page, ['//span[contains(text(), "Cash Flow")]/following-sibling::span'])
    inventory = first_text(page, ['//span[contains(text(), "Inventory")]/following-sibling::span'])
    ebitda = first_text(page, ["//span[contains(text(), 'EBITDA:')]/following-sibling::span"])

    # Broker Info
    # User provided: //*[contains(text()," Listed By")]
    # This might return the parent div or label. We need the name and firm.
    listed_by_full = first_text(page, ['//*[contains(text()," Listed By")]'])
    
    # We can also try specific selectors based on the provided HTML
    listed_by_name = first_text(page, ["#ContactBrokerNameHyperLink", ".broker-name"])
    listed_by_firm = first_text(page, [".cmp-name"])
    
    if not listed_by_name and listed_by_full:
        listed_by_name = listed_by_full.replace("Business Listed By:", "").strip()

    # Phone - User provided: //span[contains(text(), "Phone Number")]/../following-sibling::span/a
    phone = first_attr(
        page,
        ['//span[contains(text(), "Phone Number")]/../following-sibling::span/a'],
        "href"
    )
    if phone:
        phone = phone.replace("tel:", "").strip()

    email = first_attr(
        page,
        ['a[href^="mailto:"]'],
        "href",
    )
    if email:
        email = email.replace("mailto:", "").strip()

    financial_parts = []
    if gross_revenue:
        financial_parts.append(f"Gross Revenue: {gross_revenue}")
    if cash_flow:
        financial_parts.append(f"Cash Flow: {cash_flow}")
    if inventory:
        financial_parts.append(f"Inventory: {inventory}")
    if ebitda:
        financial_parts.append(f"EBITDA: {ebitda}")

    scrape_date = datetime.now().strftime("%Y-%m-%d")
    current_url = page.url

    return {
        "Title": title or "N/A",
        "City": location_parts["city"] or "N/A",
        "State": location_parts["state"] or "N/A",
        "Country": location_parts["country"] or "US",
        "URL": current_url,
        "Industry": "N/A",  # Removed as info is gone
        "Source": "BizBuySell",
        "Description": description or "N/A",
        "Listed By (Firm)": listed_by_firm or "N/A",
        "Listed By (Name)": listed_by_name or "N/A",
        "Phone": phone or "N/A",
        "Email": email or "N/A",
        "Price": price or "N/A",
        "Gross Revenue": gross_revenue or "N/A",
        "Cash Flow": cash_flow or "N/A",
        "Inventory": inventory or "N/A",
        "EBITDA": ebitda or "N/A",
        "Scraping Date": scrape_date,
        "Financial Data": "; ".join(financial_parts) or "N/A",
        "source Link": current_url,
        "Extra Information": "N/A",
        "Deal Date": "N/A",
    }


def write_rows(output_csv: str, rows: List[Dict[str, str]]) -> None:
    with open(output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in OUTPUT_COLUMNS})


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape BizBuySell listings from a URL CSV.")
    parser.add_argument(
        "--input-csv",
        default="data_collection/bizbuysell.csv",
        help="CSV with BizBuySell URLs (default: data_collection/bizbuysell.csv)",
    )
    parser.add_argument(
        "--output-csv",
        default="data_collection/bizbuysell_scraped.csv",
        help="Output CSV path (default: data_collection/bizbuysell_scraped.csv)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only scrape first N URLs (0 = all).",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run in headless mode.",
    )
    parser.add_argument(
        "--skip-login",
        action="store_true",
        help="Skip login even if credentials are set.",
    )
    parser.add_argument(
        "--csv-only",
        action="store_true",
        help="Write to CSV only (skip database). Useful for debugging.",
    )
    parser.add_argument(
        "--proxy",
        default="",
        help='Optional proxy. Format: "host:port" or "user:pass@host:port".',
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Retries per listing when blocked (default: 3).",
    )
    args = parser.parse_args()

    urls = load_urls_from_csv(args.input_csv)
    if args.limit > 0:
        urls = urls[: args.limit]

    if not urls:
        print(f"No BizBuySell URLs found in {args.input_csv}")
        return

    print(f"Loaded {len(urls)} URLs from {args.input_csv}")

    # --- Camoufox kwargs ---
    cfox_kwargs: Dict = {
        "headless": args.headless,
        "humanize": True,             # human-like cursor movements
        "os": "windows",              # spoof as Windows
    }
    if args.proxy:
        proxy_parts = args.proxy.split("@")
        if len(proxy_parts) == 2:
            user_pass, server = proxy_parts
            user, pw = user_pass.split(":", 1)
            cfox_kwargs["proxy"] = {
                "server": f"http://{server}",
                "username": user,
                "password": pw,
            }
        else:
            cfox_kwargs["proxy"] = {"server": f"http://{args.proxy}"}

    # --- Launch Camoufox browser ---
    print("Launching Camoufox browser…")
    try:
        with Camoufox(**cfox_kwargs) as browser:
            print("Browser launched successfully.")
            page = browser.new_page()
            
            # Establishing cookies by visiting home page first
            print("Visiting homepage to establish session…")
            try:
                page.goto("https://www.bizbuysell.com/", wait_until="domcontentloaded", timeout=30000)
                human_delay(2, 4)
            except Exception as e:
                print(f"Warning: Could not load homepage: {e}")

            if not args.skip_login:
                login(
                    page,
                    email=os.getenv("BIZBUYSELL_EMAIL", ""),
                    password=os.getenv("BIZBUYSELL_PASSWORD", ""),
                )

            scraped_rows: List[Dict[str, str]] = []
            conn = None
            if not args.csv_only:
                conn = get_connection()

            for idx, url in enumerate(urls, start=1):
                if "Business-Auction" in url:
                    print(f"[{idx}/{len(urls)}] Skipping auction URL: {url}")
                    continue

                try:
                    print(f"[{idx}/{len(urls)}] Processing: {url}")
                    row = parse_listing(page, url, retries=max(1, args.retries))
                    scraped_rows.append(row)
                    print(f"[{idx}/{len(urls)}] Scraped: {row.get('Title', '')}")

                    # Write to DB immediately per listing (not batch)
                    if conn and not args.csv_only:
                        try:
                            cur = conn.cursor()
                            upsert_listing(cur, row)
                            conn.commit()
                        except Exception as db_exc:
                            conn.rollback()
                            print(f"  ⚠️ DB upsert failed: {db_exc}")
                except Exception as exc:
                    print(f"[{idx}/{len(urls)}] Failed: {url} | {exc}")
                    continue

                # Polite delay between listings
                human_delay(2, 5)

            if conn:
                conn.close()

            if args.csv_only:
                write_rows(args.output_csv, scraped_rows)
                print(f"Saved {len(scraped_rows)} rows to {args.output_csv}")
            else:
                print(f"✅ Upserted {len(scraped_rows)} listings into raw_listings.")
    except Exception as exc:
        print(f"FATAL: Camoufox browser error: {exc}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
