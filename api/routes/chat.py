"""
Consensus — Chat API route.

POST /api/chat  — RAG chatbot that answers questions using scraped listing data.

Flow:
  1. Embed the user's question via Qwen3-Embedding-8B
  2. Retrieve the top-K most relevant listings from pgvector
  3. Build a context prompt with those listings
  4. Send conversation history + context to OpenAI GPT
  5. Return the AI reply with source references
"""

import os
import threading
import time
import uuid
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from openai import OpenAI
from pydantic import BaseModel, Field

from db.connection import get_db
from embeddings import get_embedding

router = APIRouter(tags=["chat"])

# ── Configuration ────────────────────────────────────────────────────────────

_CHAT_MODEL = os.environ.get("CHAT_MODEL", "gpt-5-mini")
_CONTEXT_TOP_K = int(os.environ.get("CHAT_CONTEXT_TOP_K", "10"))
_MAX_HISTORY_TURNS = int(os.environ.get("CHAT_MAX_HISTORY_TURNS", "20"))
_SESSION_TTL_SECONDS = int(os.environ.get("CHAT_SESSION_TTL_SECONDS", "3600"))
_MAX_DESC_CHARS = int(os.environ.get("CHAT_MAX_DESC_CHARS", "500"))

# ── In-memory session store ──────────────────────────────────────────────────

_sessions_lock = threading.Lock()
# { session_id: { "messages": [...], "last_active": float } }
_sessions: dict[str, dict[str, Any]] = {}


def _prune_expired_sessions() -> None:
    """Remove sessions that have been inactive for longer than the TTL."""
    now = time.time()
    expired = [
        sid for sid, data in _sessions.items()
        if now - data["last_active"] > _SESSION_TTL_SECONDS
    ]
    for sid in expired:
        del _sessions[sid]


def _get_or_create_session(session_id: Optional[str]) -> tuple[str, list[dict]]:
    """Return (session_id, messages) — creates a new session if needed."""
    with _sessions_lock:
        _prune_expired_sessions()

        if session_id and session_id in _sessions:
            session = _sessions[session_id]
            session["last_active"] = time.time()
            return session_id, session["messages"]

        new_id = session_id or str(uuid.uuid4())
        _sessions[new_id] = {"messages": [], "last_active": time.time()}
        return new_id, _sessions[new_id]["messages"]


def reset_sessions() -> None:
    """Test helper: clear all sessions."""
    with _sessions_lock:
        _sessions.clear()


# ── OpenAI client ────────────────────────────────────────────────────────────

_openai_client: OpenAI | None = None


def _get_openai_client() -> OpenAI:
    global _openai_client
    if _openai_client is None:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError(
                "OPENAI_API_KEY is not set. "
                "Add it to your .env file to enable the chat feature."
            )
        _openai_client = OpenAI(api_key=api_key)
    return _openai_client


# ── Retrieve relevant listings from DB ───────────────────────────────────────

_SELECT_COLUMNS = """
id, url, source, title, city, state, country, industry, description,
price, gross_revenue, cash_flow, ebitda
"""


def _retrieve_listings(question: str, top_k: int = _CONTEXT_TOP_K) -> list[dict]:
    """Embed the question and retrieve the most relevant listings via pgvector."""
    try:
        embedding = get_embedding(question)
    except Exception:
        return []

    if not embedding:
        return []

    vec_str = "[" + ",".join(str(v) for v in embedding) + "]"

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {_SELECT_COLUMNS},
                       (description_embedding <=> %s::vector) AS distance
                FROM raw_listings
                WHERE description_embedding IS NOT NULL
                ORDER BY distance
                LIMIT %s
                """,
                [vec_str, top_k],
            )
            columns = [desc[0] for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    return rows


def _format_listing_for_context(listing: dict) -> str:
    """Format a single listing into a concise text block for the AI prompt."""
    desc = (listing.get("description") or "N/A")[:_MAX_DESC_CHARS]
    parts = [
        f"Title: {listing.get('title', 'N/A')}",
        f"Industry: {listing.get('industry', 'N/A')}",
        f"Location: {listing.get('city', 'N/A')}, {listing.get('state', 'N/A')}, {listing.get('country', 'US')}",
        f"Price: {listing.get('price', 'N/A')}",
        f"Revenue: {listing.get('gross_revenue', 'N/A')}",
        f"Cash Flow: {listing.get('cash_flow', 'N/A')}",
        f"EBITDA: {listing.get('ebitda', 'N/A')}",
        f"Source: {listing.get('source', 'N/A')}",
        f"URL: {listing.get('url', 'N/A')}",
        f"Description: {desc}",
    ]
    return "\n".join(parts)


def _build_system_prompt(listings: list[dict]) -> str:
    """Build the system prompt including retrieved listing context."""
    if not listings:
        return (
            "You are a helpful business acquisition assistant for the Consensus platform. "
            "You help users find and analyze businesses for sale. "
            "Currently there are no listings matching the query in the database. "
            "Let the user know and suggest they try rephrasing their question."
        )

    listing_blocks = []
    for i, listing in enumerate(listings, 1):
        listing_blocks.append(f"--- Listing {i} ---\n{_format_listing_for_context(listing)}")

    context = "\n\n".join(listing_blocks)

    return (
        "You are a helpful business acquisition assistant for the Consensus platform. "
        "You help users find and analyze businesses for sale.\n\n"
        "Below are the most relevant business listings from our database. "
        "Use ONLY these listings to answer the user's question. "
        "If the answer is not in the listings, say so honestly.\n"
        "When referencing a listing, mention its title and key financial details.\n"
        "Be concise but thorough.\n\n"
        f"=== LISTINGS DATA ===\n\n{context}\n\n=== END LISTINGS DATA ==="
    )


# ── Request / Response models ────────────────────────────────────────────────

class ChatRequest(BaseModel):
    session_id: Optional[str] = None
    message: str = Field(..., min_length=1, description="The user's question")


class ChatSource(BaseModel):
    id: int
    title: str
    url: str


class ChatResponse(BaseModel):
    session_id: str
    reply: str
    sources: list[ChatSource]


# ── Route ────────────────────────────────────────────────────────────────────

@router.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest):
    """
    RAG chatbot endpoint.

    Retrieves relevant listings via semantic search, then uses OpenAI GPT
    to generate an answer grounded in the listing data.
    """
    message = request.message.strip()
    if not message:
        raise HTTPException(status_code=422, detail="Message cannot be empty.")

    # 1. Get or create session
    session_id, history = _get_or_create_session(request.session_id)

    # 2. Retrieve relevant listings
    listings = _retrieve_listings(message)

    # 3. Build the messages payload for OpenAI
    system_prompt = _build_system_prompt(listings)
    openai_messages = [{"role": "system", "content": system_prompt}]

    # Add conversation history (keep last N turns)
    recent_history = history[-_MAX_HISTORY_TURNS:]
    openai_messages.extend(recent_history)

    # Add the new user message
    openai_messages.append({"role": "user", "content": message})

    # 4. Call OpenAI
    try:
        client = _get_openai_client()
        completion = client.chat.completions.create(
            model=_CHAT_MODEL,
            messages=openai_messages,
            temperature=0.3,
            max_tokens=1024,
        )
        reply = completion.choices[0].message.content or "I couldn't generate a response."
    except RuntimeError:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to get response from AI model: {exc}",
        )

    # 5. Store the conversation turn
    history.append({"role": "user", "content": message})
    history.append({"role": "assistant", "content": reply})

    # Trim history if too long
    if len(history) > _MAX_HISTORY_TURNS * 2:
        del history[: len(history) - _MAX_HISTORY_TURNS * 2]

    # 6. Build source references
    sources = []
    for listing in listings:
        listing_id = listing.get("id")
        title = listing.get("title", "N/A")
        url = listing.get("url", "")
        if listing_id is not None:
            sources.append(ChatSource(id=listing_id, title=title, url=url))

    return ChatResponse(
        session_id=session_id,
        reply=reply,
        sources=sources,
    )
