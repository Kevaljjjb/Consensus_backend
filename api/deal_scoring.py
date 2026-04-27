"""
AI-powered Tucker Property Group deal scoring.

Builds a per-listing prompt for DeepInfra-hosted Qwen, validates the JSON
response, and falls back to a deterministic heuristic score if the model output
is unavailable or unusable.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from openai import OpenAI

_DEFAULT_MODEL = "Qwen/Qwen3.5-27B"
_DEFAULT_BASE_URL = "https://api.deepinfra.com/v1/openai"
_TIMEOUT_SECONDS = float(os.environ.get("DEAL_EVALUATION_TIMEOUT_SECONDS", "45"))

_SCORE_LIMITS = {
    "cash_flow": 25,
    "profitability": 25,
    "maturity": 20,
    "locality": 15,
    "stability": 15,
}

_TEXT_NULLS = {"", "n/a", "na", "null", "none", "-", "--"}
_LOCALITY_TARGETS = {"us", "usa", "united states", "united states of america", "ca", "canada"}
_SOFTWARE_OR_FRANCHISE_KEYWORDS = (
    "software",
    "saas",
    "internet",
    "digital",
    "franchise",
    "franchisor",
    "online",
    "e-commerce",
    "ecommerce",
)
_SERVICE_ALIGNMENT_KEYWORDS = (
    "service",
    "hvac",
    "plumbing",
    "electrical",
    "locksmith",
    "security",
    "medspa",
    "spa",
    "aesthetic",
    "restaurant",
    "consumer",
    "brand",
    "franchise",
    "repair",
    "maintenance",
    "cleaning",
    "landscaping",
    "facility",
    "auto",
)
_STABILITY_KEYWORDS = (
    "recurring",
    "subscription",
    "contract",
    "retention",
    "repeat",
    "loyal",
    "essential",
    "mission critical",
    "mission-critical",
    "niche",
    "defensible",
    "brand",
    "trusted",
)

_SYSTEM_PROMPT = """
You are a deal evaluation engine for a permanent-hold acquisition firm modeled after Berkshire
Hathaway. The founding team comes from Apollo's private equity group and Viking's hedge fund team,
but the firm operates as a long-term holding company, not Wall Street.

===========================================================================
FIRM PHILOSOPHY
===========================================================================

The firm values STABILITY OVER GROWTH. It optimizes for DURABILITY and LONGEVITY, not speed.
Key indicators of stability:
  - Mission criticality (high retention, low churn)
  - Consistent and diversified customer base
  - Consistent and diversified supplier base
  - Niche market focus with defensible positioning
  - Strong brand recognition and trust
  - Predictable, recurring revenue streams

Process speed:
  - Respond within 48 HOURS
  - Make an offer within 5 DAYS
  - Close a partnership within 2 MONTHS

===========================================================================
PORTFOLIO CONTEXT (for fit pattern recognition)
===========================================================================

  - Annie Aesthetic, VIO Medspa — aesthetics/medspa
  - Wonder Franchises — franchise platform
  - Badlands Security — mission-critical services
  - Christmas Decor — seasonal services
  - Pizza Factory, Rodizio Grill — consumer/restaurant brands
  - Webster Locksmiths — local essential services
  - Soccer 5 — experiential/sports brand
  - Plus additional stealth/confidential holdings

Portfolio patterns: local services, franchise models, mission-critical businesses, consumer brands
with loyal customer bases, medspa/aesthetics, security, seasonal services.

===========================================================================
INVESTMENT CRITERIA - SCORING RUBRIC
===========================================================================

Score each deal on these 5 dimensions. The total MUST equal the sum of individual scores (0-100).

1. CASH FLOW FIT (max 25 points)
   Target: $2M-$10M of normalized operating cash flows (~EBIT), with at least a 3-year track
   record. Bigger also works.
   Scoring guide:
     - $2M-$10M EBIT with 3+ year history -> 20-25 pts
     - $1M-$2M EBIT (near threshold) -> 12-18 pts
     - >$10M EBIT (bigger, still works) -> 15-22 pts
     - <$1M or unknown -> 0-10 pts

2. PROFITABILITY (max 25 points)
   Target: More than 10% normalized operating cash flow (~EBIT) margins.
   Exceptions: subscale software assets and franchisor assets can have lower margins.
   Scoring guide:
     - >15% margins -> 20-25 pts
     - 10-15% margins -> 15-20 pts
     - 5-10% margins (with software/franchisor exception) -> 8-15 pts
     - <5% margins -> 0-7 pts

3. MATURITY / AGE (max 20 points)
   Target: More than 6 years old. Exceptions for digital, internet, or vertical market software
   assets with best-in-class retention.
   Scoring guide:
     - 10+ years -> 16-20 pts
     - 6-10 years -> 12-16 pts
     - 3-6 years (with software/digital exception) -> 6-12 pts
     - <3 years -> 0-5 pts

4. LOCALITY (max 15 points)
   Target: US or Canada. Exceptions for franchisors, digital, internet, or software assets.
   Also exceptions for exceptional revenue quality regardless of geography.
   Scoring guide:
     - US or Canada -> 13-15 pts
     - Outside US/CA but digital/software/franchisor -> 7-12 pts
     - Outside US/CA, non-digital -> 0-5 pts

5. STABILITY & STRATEGIC FIT (max 15 points)
   Evaluate based on ALL available deal information for indicators of:
     - Mission criticality / essential service nature
     - Customer retention and low churn signals
     - Diversified customer and supplier base
     - Niche market position / defensibility
     - Brand strength and recognition
     - Alignment with existing portfolio themes
     - Low organic growth (<30% topline - high growth is a NEGATIVE unless inorganic/high-ROI)
     - Revenue quality and predictability
   Scoring guide:
     - Strong stability signals + portfolio alignment -> 12-15 pts
     - Moderate stability indicators -> 7-11 pts
     - Weak or unknown stability -> 0-6 pts

===========================================================================
OUTPUT FORMAT - RESPOND WITH VALID JSON ONLY
===========================================================================

You MUST respond with a single valid JSON object. No markdown, no explanation, no preamble.

{
  "fit_score": <integer 0-100, MUST equal sum of breakdown values>,
  "score_breakdown": {
    "cash_flow": <integer 0-25>,
    "profitability": <integer 0-25>,
    "maturity": <integer 0-20>,
    "locality": <integer 0-15>,
    "stability": <integer 0-15>
  },
  "pros": [
    "<specific, data-driven positive point referencing actual deal data>",
    "<another pro - aim for 3-5 points>"
  ],
  "cons": [
    "<specific concern referencing actual deal data or data gaps>",
    "<another con - aim for 3-5 points>"
  ],
  "summary": "<1-2 sentence strategic assessment as a potential acquirer>"
}

WRITING STYLE RULES:
  - Write like a concise investment memo — professional, direct, data-driven.
  - Do NOT repeatedly reference the firm name. Never say "Tucker", "Tucker's", or
    "Tucker Property Group" in pros, cons, or summary. Write as if the reader already
    knows the mandate. Use phrasing like "the mandate", "the investment criteria",
    "the target range", or simply state the analysis directly.
  - Avoid templated or generic phrasing. Each point should feel like original analysis.

RULES FOR PROS AND CONS:
  - Each point MUST reference specific data from the deal (numbers, industry, location, etc.)
  - Do NOT use generic statements like "good business" - be specific
  - If data is missing (e.g., no EBITDA reported), flag it as a con: "EBITDA not reported — margins unverifiable"
  - Pros should explain WHY each attribute fits the investment criteria
  - Cons should explain the RISK or gap from an acquisition perspective
  - Include 3-5 pros and 3-5 cons minimum

RULES FOR SUMMARY:
  - Write from the perspective of a potential acquirer, not an outside observer
  - Reference the most important pro and most important con
  - Keep it actionable: "worth pursuing" vs "likely a pass" vs "needs more diligence on X"
  - Do NOT start with "From [firm name]'s perspective" — just state the assessment directly
""".strip()

_CLIENT: OpenAI | None = None


def _get_client() -> OpenAI:
    """Return a shared OpenAI client pointed at DeepInfra's OpenAI-compatible API."""
    global _CLIENT
    if _CLIENT is None:
        api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("EMBEDDING_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set.")
        _CLIENT = OpenAI(
            api_key=api_key,
            base_url=os.environ.get("DEAL_EVALUATION_BASE_URL", _DEFAULT_BASE_URL),
            timeout=_TIMEOUT_SECONDS,
        )
    return _CLIENT


def _get_model() -> str:
    return os.environ.get("DEAL_EVALUATION_MODEL", _DEFAULT_MODEL)


def _stringify_value(value: Any, *, default: str = "N/A") -> str:
    if value is None:
        return default
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or default


def _build_user_prompt(listing: Mapping[str, Any]) -> str:
    return (
        "Evaluate this business acquisition opportunity:\n\n"
        "=== DEAL INFORMATION ===\n"
        f"Title: {_stringify_value(listing.get('title'))}\n"
        f"Industry: {_stringify_value(listing.get('industry'))}\n"
        f"Location: {_stringify_value(listing.get('city'))}, "
        f"{_stringify_value(listing.get('state'))}, "
        f"{_stringify_value(listing.get('country'))}\n"
        f"Source: {_stringify_value(listing.get('source'))}\n\n"
        "=== FINANCIALS ===\n"
        f"Asking Price: {_stringify_value(listing.get('price'))}\n"
        f"Gross Revenue: {_stringify_value(listing.get('gross_revenue'))}\n"
        f"EBITDA: {_stringify_value(listing.get('ebitda'))}\n"
        f"Cash Flow: {_stringify_value(listing.get('cash_flow'))}\n"
        f"Inventory: {_stringify_value(listing.get('inventory'))}\n\n"
        "=== LISTING DESCRIPTION ===\n"
        f"{_stringify_value(listing.get('description'))}\n\n"
        "=== FINANCIAL DATA (RAW) ===\n"
        f"{_stringify_value(listing.get('financial_data'))}\n\n"
        "=== ADDITIONAL INFORMATION ===\n"
        f"{_stringify_value(listing.get('extra_information'))}\n\n"
        "=== DEAL METADATA ===\n"
        f"Deal Date: {_stringify_value(listing.get('deal_date'))}\n"
        f"First Seen: {_stringify_value(listing.get('first_seen_date'))}\n"
        f"Source Link: {_stringify_value(listing.get('source_link'))}\n\n"
        "Score this deal against the investment criteria. "
        "Respond with valid JSON only."
    )


def _message_content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            if isinstance(item, Mapping):
                text = item.get("text")
                if text:
                    parts.append(str(text))
                    continue
            text = getattr(item, "text", None)
            if text:
                parts.append(str(text))
                continue
            parts.append(str(item))
        return "".join(parts)
    return str(content or "")


def _extract_json_payload(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        raise ValueError("LLM returned empty content")

    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("LLM response did not contain a JSON object") from None
        payload = json.loads(stripped[start : end + 1])

    if not isinstance(payload, dict):
        raise ValueError("LLM payload must be a JSON object")
    return payload


def _coerce_int(value: Any, *, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    try:
        number = int(value)
    except (TypeError, ValueError):
        try:
            number = int(float(value))
        except (TypeError, ValueError):
            raise ValueError(f"{field} must be an integer") from None
    if number < minimum or number > maximum:
        raise ValueError(f"{field} must be between {minimum} and {maximum}")
    return number


def _coerce_string_list(value: Any, *, field: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be an array")
    items = [str(item).strip() for item in value if str(item).strip()]
    if len(items) < 3:
        raise ValueError(f"{field} must contain at least 3 non-empty items")
    return items[:5]


def _validate_model_payload(payload: dict[str, Any]) -> dict[str, Any]:
    raw_breakdown = payload.get("score_breakdown")
    if not isinstance(raw_breakdown, Mapping):
        raise ValueError("score_breakdown must be an object")

    breakdown: dict[str, int] = {}
    total = 0
    for score_name, max_score in _SCORE_LIMITS.items():
        score_value = _coerce_int(
            raw_breakdown.get(score_name),
            field=f"score_breakdown.{score_name}",
            minimum=0,
            maximum=max_score,
        )
        breakdown[score_name] = score_value
        total += score_value

    # LLMs often get the sum wrong — auto-correct by using the computed total
    fit_score = min(100, total)

    summary = str(payload.get("summary") or "").strip()
    if not summary:
        raise ValueError("summary must be present")

    return {
        "fit_score": fit_score,
        "score_breakdown": breakdown,
        "pros": _coerce_string_list(payload.get("pros"), field="pros"),
        "cons": _coerce_string_list(payload.get("cons"), field="cons"),
        "summary": summary,
        "model_used": _get_model(),
    }


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))

    text = str(value).strip()
    if not text or text.lower() in _TEXT_NULLS:
        return None
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    text = re.sub(r"[,$ ]", "", text)
    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", text) is None:
        return None

    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _format_currency(value: Decimal | None) -> str:
    if value is None:
        return "N/A"
    quantized = value.quantize(Decimal("1"))
    return f"${quantized:,.0f}"


def _format_ratio(value: Decimal | None) -> str:
    if value is None:
        return "N/A"
    rounded = value.quantize(Decimal("0.1"))
    return f"{rounded}x"


def _format_percent(value: Decimal | None) -> str:
    if value is None:
        return "N/A"
    rounded = value.quantize(Decimal("0.1"))
    return f"{rounded}%"


def _combined_listing_text(listing: Mapping[str, Any]) -> str:
    return " ".join(
        _stringify_value(listing.get(key), default="")
        for key in ("title", "industry", "description", "financial_data", "extra_information")
    ).lower()


def _has_any_keyword(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _location_text(listing: Mapping[str, Any]) -> str:
    parts = [
        _stringify_value(listing.get("city"), default=""),
        _stringify_value(listing.get("state"), default=""),
        _stringify_value(listing.get("country"), default=""),
    ]
    cleaned = [part for part in parts if part and part.lower() not in _TEXT_NULLS]
    return ", ".join(cleaned) or "location not disclosed"


def _country_normalized(listing: Mapping[str, Any]) -> str:
    country = _stringify_value(listing.get("country"), default="").lower()
    state = _stringify_value(listing.get("state"), default="").lower()
    if country and country not in _TEXT_NULLS:
        return country
    if state and state not in _TEXT_NULLS:
        return "us"
    return ""


def _extract_business_age_years(listing: Mapping[str, Any]) -> int | None:
    combined = _combined_listing_text(listing)

    years_match = re.search(r"\b(\d{1,2})\+?\s+years?\b", combined)
    if years_match:
        years = int(years_match.group(1))
        if years > 0:
            return years

    founded_match = re.search(r"\b(?:since|established|founded)\s+(19\d{2}|20\d{2})\b", combined)
    if not founded_match:
        return None

    founded_year = int(founded_match.group(1))
    current_year = datetime.now(timezone.utc).year
    age = current_year - founded_year
    return age if age > 0 else None


def _operating_cash_flow(listing: Mapping[str, Any]) -> Decimal | None:
    return _to_decimal(listing.get("cash_flow_num")) or _to_decimal(listing.get("ebitda_num")) or _to_decimal(
        listing.get("cash_flow")
    ) or _to_decimal(listing.get("ebitda"))


def _gross_revenue(listing: Mapping[str, Any]) -> Decimal | None:
    return _to_decimal(listing.get("gross_revenue_num")) or _to_decimal(listing.get("gross_revenue"))


def _asking_price(listing: Mapping[str, Any]) -> Decimal | None:
    return _to_decimal(listing.get("price_num")) or _to_decimal(listing.get("price"))


def _has_exception_profile(listing: Mapping[str, Any]) -> bool:
    return _has_any_keyword(_combined_listing_text(listing), _SOFTWARE_OR_FRANCHISE_KEYWORDS)


def _score_cash_flow(listing: Mapping[str, Any]) -> int:
    cash_flow = _operating_cash_flow(listing)
    if cash_flow is None or cash_flow <= 0:
        return 5
    if Decimal("2000000") <= cash_flow <= Decimal("10000000"):
        return 23
    if Decimal("1000000") <= cash_flow < Decimal("2000000"):
        return 16
    if cash_flow > Decimal("10000000"):
        return 19
    return 8


def _score_profitability(listing: Mapping[str, Any]) -> int:
    revenue = _gross_revenue(listing)
    cash_flow = _operating_cash_flow(listing)
    if revenue is None or revenue <= 0 or cash_flow is None:
        return 6

    margin = (cash_flow / revenue) * Decimal("100")
    if margin > Decimal("15"):
        return 23
    if margin >= Decimal("10"):
        return 18
    if margin >= Decimal("5"):
        return 12 if _has_exception_profile(listing) else 7
    return 3


def _score_maturity(listing: Mapping[str, Any]) -> int:
    age_years = _extract_business_age_years(listing)
    if age_years is None:
        return 5
    if age_years >= 10:
        return 18
    if age_years >= 6:
        return 14
    if age_years >= 3:
        return 10 if _has_exception_profile(listing) else 7
    return 3


def _score_locality(listing: Mapping[str, Any]) -> int:
    country = _country_normalized(listing)
    if country in _LOCALITY_TARGETS:
        return 15
    if _has_exception_profile(listing):
        return 9
    if country:
        return 3
    return 5


def _score_stability(listing: Mapping[str, Any]) -> int:
    text = _combined_listing_text(listing)
    score = 4
    if _has_any_keyword(text, _SERVICE_ALIGNMENT_KEYWORDS):
        score += 4
    if _has_any_keyword(text, _STABILITY_KEYWORDS):
        score += 3
    if len(_stringify_value(listing.get("description"), default="")) >= 120:
        score += 1
    if _has_exception_profile(listing):
        score += 1
    return min(score, _SCORE_LIMITS["stability"])


def _dedupe_points(points: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for point in points:
        stripped = point.strip()
        if not stripped or stripped in seen:
            continue
        seen.add(stripped)
        ordered.append(stripped)
    return ordered


def _build_heuristic_pros(listing: Mapping[str, Any], breakdown: dict[str, int]) -> list[str]:
    text = _combined_listing_text(listing)
    location = _location_text(listing)
    industry = _stringify_value(listing.get("industry"))
    cash_flow = _operating_cash_flow(listing)
    revenue = _gross_revenue(listing)
    price = _asking_price(listing)
    margin = ((cash_flow / revenue) * Decimal("100")) if cash_flow and revenue and revenue > 0 else None

    points: list[str] = []
    if cash_flow is not None and cash_flow >= Decimal("2000000"):
        points.append(
            f"Reported cash flow/EBITDA of {_format_currency(cash_flow)} falls within the preferred "
            "$2M–$10M earnings band for the mandate."
        )
    elif cash_flow is not None and cash_flow >= Decimal("1000000"):
        points.append(
            f"Reported cash flow/EBITDA of {_format_currency(cash_flow)} is approaching the lower end of "
            "the target earnings range."
        )
    elif cash_flow is not None and cash_flow > 0:
        points.append(
            f"Reported cash flow/EBITDA of {_format_currency(cash_flow)} provides a concrete earnings base "
            "for preliminary underwriting."
        )

    if margin is not None and margin >= Decimal("10"):
        points.append(
            f"Operating margin of approximately {_format_percent(margin)} ({_format_currency(cash_flow)} on "
            f"{_format_currency(revenue)} revenue) clears the >10% profitability threshold."
        )

    if breakdown["locality"] >= 13:
        points.append(f"Located in {location}, within the preferred US/Canada geography.")

    if _has_any_keyword(text, _SERVICE_ALIGNMENT_KEYWORDS):
        points.append(
            f"The '{industry}' sector aligns well with existing portfolio themes around local services, "
            "franchises, and consumer brands."
        )

    if price is not None and cash_flow is not None and cash_flow > 0:
        points.append(
            f"Asking price of {_format_currency(price)} implies roughly {_format_ratio(price / cash_flow)} "
            "on reported cash flow/EBITDA — a useful valuation anchor."
        )

    if revenue is not None:
        points.append(f"Gross revenue of {_format_currency(revenue)} provides a clear top-line benchmark for diligence.")

    source = _stringify_value(listing.get("source"))
    points.append(f"Sourced via {source}, providing an actionable path for follow-up.")
    return _dedupe_points(points)[:5]


def _build_heuristic_cons(listing: Mapping[str, Any], breakdown: dict[str, int]) -> list[str]:
    text = _combined_listing_text(listing)
    location = _location_text(listing)
    cash_flow = _operating_cash_flow(listing)
    revenue = _gross_revenue(listing)
    margin = ((cash_flow / revenue) * Decimal("100")) if cash_flow and revenue and revenue > 0 else None
    age_years = _extract_business_age_years(listing)

    points: list[str] = []
    if cash_flow is None:
        points.append("Cash flow and EBITDA are not clearly reported — normalized earnings are unverifiable.")
    elif cash_flow < Decimal("1000000"):
        points.append(
            f"Reported cash flow/EBITDA of {_format_currency(cash_flow)} falls below the target "
            "$2M–$10M earnings range."
        )

    if revenue is None or revenue <= 0:
        points.append("Gross revenue is not disclosed, making margin quality and revenue durability hard to assess.")
    elif margin is not None and margin < Decimal("10"):
        points.append(
            f"Operating margin of approximately {_format_percent(margin)} falls short of the >10% "
            "profitability threshold."
        )

    if age_years is None:
        points.append("Operating history is not disclosed — the 6+ year maturity requirement is unresolved.")
    elif age_years < 6 and not _has_exception_profile(listing):
        points.append(
            f"Stated operating history of {age_years} years is below the preferred 6+ year maturity threshold."
        )

    if breakdown["locality"] <= 5 and _country_normalized(listing):
        points.append(
            f"Location in {location} is outside the core US/Canada geography without a qualifying "
            "software or franchisor exception."
        )

    if not _has_any_keyword(text, _STABILITY_KEYWORDS):
        points.append(
            "No mention of retention, recurring revenue, contracts, or supplier diversity — "
            "stability remains unproven."
        )

    description = _stringify_value(listing.get("description"), default="")
    if len(description) < 100:
        points.append("Listing description is too thin to meaningfully assess defensibility or churn risk.")

    return _dedupe_points(points)[:5]


def _pad_points(points: list[str], fallbacks: list[str], *, minimum: int) -> list[str]:
    combined = _dedupe_points(points)
    for fallback in fallbacks:
        if len(combined) >= minimum:
            break
        if fallback not in combined:
            combined.append(fallback)
    if len(combined) < minimum:
        raise ValueError("Not enough unique fallback points to satisfy minimum length")
    return combined[:5]


def _heuristic_evaluation(listing: Mapping[str, Any]) -> dict[str, Any]:
    breakdown = {
        "cash_flow": _score_cash_flow(listing),
        "profitability": _score_profitability(listing),
        "maturity": _score_maturity(listing),
        "locality": _score_locality(listing),
        "stability": _score_stability(listing),
    }
    fit_score = sum(breakdown.values())

    location = _location_text(listing)
    industry = _stringify_value(listing.get("industry"))
    pros = _pad_points(
        _build_heuristic_pros(listing, breakdown),
        [
            f"Located in {location}, providing a defined market for diligence.",
            f"Industry '{industry}' is clearly identified, supporting a first-pass portfolio fit screen.",
            f"Asking price of {_stringify_value(listing.get('price'))} offers a concrete valuation starting point.",
        ],
        minimum=3,
    )
    cons = _pad_points(
        _build_heuristic_cons(listing, breakdown),
        [
            "Customer concentration, retention, and supplier diversity are not disclosed.",
            "Normalized operating adjustments are not provided — deeper quality-of-earnings work is needed.",
            "Insufficient detail to verify long-term stability with confidence.",
        ],
        minimum=3,
    )

    cash_flow = _operating_cash_flow(listing)
    revenue = _gross_revenue(listing)

    # Build a natural summary that reads like an investment memo
    if fit_score >= 80:
        stance = "This is a strong fit worth pursuing."
    elif fit_score >= 60:
        stance = "Worth a closer look, but focused diligence is needed."
    else:
        stance = "Likely a pass unless further diligence materially changes the picture."

    # Create a data-aware summary rather than a templated one
    highlights: list[str] = []
    if cash_flow is not None and cash_flow >= Decimal("2000000"):
        highlights.append(f"cash flow of {_format_currency(cash_flow)} within the target band")
    elif cash_flow is not None and cash_flow > 0:
        highlights.append(f"cash flow of {_format_currency(cash_flow)}")
    if revenue is not None and revenue > 0:
        highlights.append(f"{_format_currency(revenue)} in revenue")
    if location and location != "location not disclosed":
        highlights.append(f"based in {location}")

    if highlights:
        context = ", ".join(highlights)
        summary = f"This {industry} opportunity ({context}) scores {fit_score}/100. {stance}"
    else:
        summary = f"This {industry} opportunity scores {fit_score}/100. {stance}"

    return {
        "fit_score": fit_score,
        "score_breakdown": breakdown,
        "pros": pros,
        "cons": cons,
        "summary": summary,
        "model_used": "heuristic-fallback",
    }


def evaluate_deal(listing: Mapping[str, Any]) -> dict[str, Any]:
    """
    Evaluate a single listing against Tucker Property Group's criteria.

    Returns a validated dict suitable for persistence in deal_evaluations.
    """
    try:
        response = _get_client().chat.completions.create(
            model=_get_model(),
            temperature=0.1,
            max_tokens=1200,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": _build_user_prompt(listing)},
            ],
        )
        content = _message_content_to_text(response.choices[0].message.content)
        payload = _extract_json_payload(content)
        return _validate_model_payload(payload)
    except Exception:
        return _heuristic_evaluation(listing)
