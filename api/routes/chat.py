"""
Consensus — AI Chat API route with chat history persistence.

Endpoints:
POST   /api/chat                      — send conversation history and receive AI reply (JSON)
POST   /api/chat/stream               — stream AI reply and persist conversation/messages
GET    /api/chat/conversations        — list a user's chat conversations
GET    /api/chat/conversations/{id}   — fetch a conversation with all messages
POST   /api/chat/conversations        — create a new empty conversation
DELETE /api/chat/conversations/{id}   — delete a conversation
"""

import json
import os
from typing import Any, Optional
from uuid import UUID

import openai
from db.connection import get_db
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

router = APIRouter(tags=["chat"])

_OPENAI_BASE_URL = "https://api.openai.com/v1"


def _resolve_chat_api_key() -> str:
    direct_key = os.environ.get("CHAT_OPENAI_API_KEY") or os.environ.get("API_KEY", "")
    if direct_key:
        return direct_key

    configured_base_url = (
        os.environ.get("OPENAI_BASE_URL") or _OPENAI_BASE_URL
    ).rstrip("/")
    if configured_base_url == _OPENAI_BASE_URL.rstrip("/"):
        return os.environ.get("OPENAI_API_KEY", "")

    return ""


_API_KEY = _resolve_chat_api_key()
_MODEL = "gpt-5.4"
_TEXT_CONFIG = {
    "format": {"type": "text"},
    "verbosity": "medium",
}
_REASONING_CONFIG = {
    "effort": "medium",
    "summary": "auto",
}
_TOOLS = [
    {
        "type": "web_search",
        "user_location": {"type": "approximate"},
        "search_context_size": "medium",
    }
]
_INCLUDE = [
    "reasoning.encrypted_content",
    "web_search_call.action.sources",
]

_async_client: openai.AsyncOpenAI | None = None
_sync_client: openai.OpenAI | None = None

SYSTEM_PROMPT = (
    "You are Consensus AI, an intelligent assistant for the Consensus platform. "
    # "Use web search when the user asks for current events, external market context, "
    # "benchmarks, or facts that are not contained in the conversation itself. "
    # "When you use external context, make it clear what came from web search versus "
    # "what came from the conversation."
)


class ChatMessage(BaseModel):
    role: str  # "user" | "assistant" | "system"
    content: str


class ChatRequest(BaseModel):
    messages: list[ChatMessage]
    model: Optional[str] = None
    user_id: UUID
    conversation_id: Optional[UUID] = None
    title: Optional[str] = None
    edited_message_index: Optional[int] = Field(default=None, ge=0)


class ChatResponse(BaseModel):
    reply: str
    model: str
    conversation_id: UUID
    sources: list[dict[str, Any]] = Field(default_factory=list)
    annotations: list[dict[str, Any]] = Field(default_factory=list)


class ConversationSummary(BaseModel):
    id: UUID
    user_id: UUID
    title: Optional[str] = None
    created_at: str
    updated_at: str
    message_count: int = 0
    preview: str = ""


class StoredMessage(BaseModel):
    id: UUID
    conversation_id: UUID
    user_id: UUID
    role: str
    content: str
    message_order: int
    edited_at: Optional[str] = None
    created_at: str


class ConversationDetail(BaseModel):
    id: UUID
    user_id: UUID
    title: Optional[str] = None
    created_at: str
    updated_at: str
    messages: list[StoredMessage]


def _normalize_title(
    messages: list[ChatMessage], explicit_title: Optional[str] = None
) -> str:
    if explicit_title and explicit_title.strip():
        return explicit_title.strip()[:120]

    for msg in messages:
        if msg.role == "user" and msg.content.strip():
            candidate = " ".join(msg.content.strip().split())
            return candidate[:120]

    return "New chat"


def _build_api_messages(req: ChatRequest) -> list[dict[str, str]]:
    """Prepend system prompt and convert request messages to API format."""
    api_messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in req.messages:
        api_messages.append({"role": msg.role, "content": msg.content})
    return api_messages


def _extract_output_text(response: Any) -> str:
    output_text = ""
    for item in response.output:
        if hasattr(item, "content"):
            for block in item.content:
                if hasattr(block, "text"):
                    output_text += block.text
    return output_text.strip()


def _to_jsonable(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}
    if hasattr(value, "__dict__"):
        return {
            key: _to_jsonable(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    return value


def _dedupe_payloads(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()

    for item in items:
        key = json.dumps(item, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return deduped


def _extract_annotations(response: Any) -> list[dict[str, Any]]:
    annotations: list[dict[str, Any]] = []

    for item in getattr(response, "output", []) or []:
        if getattr(item, "type", None) != "message":
            continue
        for block in getattr(item, "content", []) or []:
            for annotation in getattr(block, "annotations", []) or []:
                payload = _to_jsonable(annotation)
                if isinstance(payload, dict):
                    annotations.append(payload)

    return _dedupe_payloads(annotations)


def _extract_web_search_sources(response: Any) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []

    for item in getattr(response, "output", []) or []:
        if getattr(item, "type", None) != "web_search_call":
            continue
        action = getattr(item, "action", None)
        if getattr(action, "type", None) != "search":
            continue
        for source in getattr(action, "sources", []) or []:
            payload = _to_jsonable(source)
            if isinstance(payload, dict):
                sources.append(payload)

    return _dedupe_payloads(sources)


def _get_sync_client() -> openai.OpenAI:
    global _sync_client

    if _sync_client is None:
        if not _API_KEY:
            raise RuntimeError("CHAT_OPENAI_API_KEY or API_KEY not configured")
        _sync_client = openai.OpenAI(
            api_key=_API_KEY,
            base_url=_OPENAI_BASE_URL,
        )

    return _sync_client


def _get_async_client() -> openai.AsyncOpenAI:
    global _async_client

    if _async_client is None:
        if not _API_KEY:
            raise RuntimeError("CHAT_OPENAI_API_KEY or API_KEY not configured")
        _async_client = openai.AsyncOpenAI(
            api_key=_API_KEY,
            base_url=_OPENAI_BASE_URL,
        )

    return _async_client


def _row_to_conversation_summary(row: tuple[Any, ...]) -> ConversationSummary:
    return ConversationSummary(
        id=row[0],
        user_id=row[1],
        title=row[2],
        created_at=row[3].isoformat(),
        updated_at=row[4].isoformat(),
        message_count=row[5] or 0,
        preview=row[6] or "",
    )


def _row_to_stored_message(row: tuple[Any, ...]) -> StoredMessage:
    return StoredMessage(
        id=row[0],
        conversation_id=row[1],
        user_id=row[2],
        role=row[3],
        content=row[4],
        message_order=row[5],
        edited_at=row[6].isoformat() if row[6] else None,
        created_at=row[7].isoformat(),
    )


def _get_conversation_owner(conversation_id: UUID) -> Optional[UUID]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id
            FROM chat_conversations
            WHERE id = %s
            """,
            (str(conversation_id),),
        )
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None


def _ensure_conversation_access(conversation_id: UUID, user_id: UUID) -> None:
    owner_id = _get_conversation_owner(conversation_id)
    if owner_id is None:
        raise HTTPException(status_code=404, detail="Conversation not found")
    if str(owner_id) != str(user_id):
        raise HTTPException(
            status_code=403, detail="You do not have access to this conversation"
        )


def _upsert_conversation(
    user_id: UUID,
    messages: list[ChatMessage],
    conversation_id: Optional[UUID] = None,
    title: Optional[str] = None,
) -> UUID:
    normalized_title = _normalize_title(messages, title)

    with get_db() as conn:
        cur = conn.cursor()

        if conversation_id is None:
            cur.execute(
                """
                INSERT INTO chat_conversations (user_id, title)
                VALUES (%s, %s)
                RETURNING id
                """,
                (str(user_id), normalized_title),
            )
            created_row = cur.fetchone()
            if not created_row:
                raise HTTPException(
                    status_code=500, detail="Failed to create conversation"
                )
            created_id = created_row[0]
            conn.commit()
            cur.close()
            return created_id

        cur.execute(
            """
            SELECT id, user_id
            FROM chat_conversations
            WHERE id = %s
            """,
            (str(conversation_id),),
        )
        existing = cur.fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="Conversation not found")
        if str(existing[1]) != str(user_id):
            raise HTTPException(
                status_code=403, detail="You do not have access to this conversation"
            )

        cur.execute(
            """
            UPDATE chat_conversations
            SET title = COALESCE(NULLIF(%s, ''), title),
                updated_at = NOW()
            WHERE id = %s
            """,
            (normalized_title, str(conversation_id)),
        )
        conn.commit()
        cur.close()
        return conversation_id


def _replace_conversation_messages(
    conversation_id: UUID,
    user_id: UUID,
    messages: list[ChatMessage],
    assistant_reply: str,
    edited_message_index: Optional[int] = None,
) -> None:
    stored_rows: list[tuple[str, str, str, int, str, Optional[str]]] = []

    for idx, msg in enumerate(messages):
        edited_at = None
        if (
            edited_message_index is not None
            and idx == edited_message_index
            and msg.role == "user"
        ):
            edited_at = "NOW_MARKER"
        stored_rows.append(
            (
                str(conversation_id),
                str(user_id),
                msg.role,
                idx,
                msg.content,
                edited_at,
            )
        )

    stored_rows.append(
        (
            str(conversation_id),
            str(user_id),
            "assistant",
            len(messages),
            assistant_reply,
            None,
        )
    )

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM chat_messages
            WHERE conversation_id = %s
            """,
            (str(conversation_id),),
        )

        for (
            conversation_id_value,
            user_id_value,
            role,
            message_order,
            content,
            edited_at,
        ) in stored_rows:
            if edited_at == "NOW_MARKER":
                cur.execute(
                    """
                    INSERT INTO chat_messages (
                        conversation_id,
                        user_id,
                        role,
                        content,
                        message_order,
                        edited_at
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        conversation_id_value,
                        user_id_value,
                        role,
                        content,
                        message_order,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO chat_messages (
                        conversation_id,
                        user_id,
                        role,
                        content,
                        message_order,
                        edited_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        conversation_id_value,
                        user_id_value,
                        role,
                        content,
                        message_order,
                        edited_at,
                    ),
                )

        cur.execute(
            """
            UPDATE chat_conversations
            SET updated_at = NOW(),
                title = COALESCE(title, %s)
            WHERE id = %s
            """,
            (_normalize_title(messages), str(conversation_id)),
        )
        conn.commit()
        cur.close()


def _fetch_conversation_detail(
    conversation_id: UUID, user_id: UUID
) -> ConversationDetail:
    _ensure_conversation_access(conversation_id, user_id)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, user_id, title, created_at, updated_at
            FROM chat_conversations
            WHERE id = %s
            """,
            (str(conversation_id),),
        )
        conversation = cur.fetchone()
        if not conversation:
            cur.close()
            raise HTTPException(status_code=404, detail="Conversation not found")

        cur.execute(
            """
            SELECT
                id,
                conversation_id,
                user_id,
                role,
                content,
                message_order,
                edited_at,
                created_at
            FROM chat_messages
            WHERE conversation_id = %s
            ORDER BY message_order ASC, created_at ASC
            """,
            (str(conversation_id),),
        )
        message_rows = cur.fetchall()
        cur.close()

    return ConversationDetail(
        id=conversation[0],
        user_id=conversation[1],
        title=conversation[2],
        created_at=conversation[3].isoformat(),
        updated_at=conversation[4].isoformat(),
        messages=[_row_to_stored_message(row) for row in message_rows],
    )


@router.get("/chat/conversations", response_model=list[ConversationSummary])
def list_conversations(user_id: UUID):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                c.id,
                c.user_id,
                c.title,
                c.created_at,
                c.updated_at,
                COUNT(m.id) AS message_count,
                COALESCE(
                    (
                        SELECT LEFT(m2.content, 140)
                        FROM chat_messages m2
                        WHERE m2.conversation_id = c.id
                        ORDER BY m2.message_order DESC
                        LIMIT 1
                    ),
                    ''
                ) AS preview
            FROM chat_conversations c
            LEFT JOIN chat_messages m
                ON m.conversation_id = c.id
            WHERE c.user_id = %s
            GROUP BY c.id, c.user_id, c.title, c.created_at, c.updated_at
            ORDER BY c.updated_at DESC, c.created_at DESC
            """,
            (str(user_id),),
        )
        rows = cur.fetchall()
        cur.close()

    return [_row_to_conversation_summary(row) for row in rows]


@router.get("/chat/conversations/{conversation_id}", response_model=ConversationDetail)
def get_conversation(conversation_id: UUID, user_id: UUID):
    return _fetch_conversation_detail(conversation_id, user_id)


class CreateConversationRequest(BaseModel):
    user_id: UUID
    title: Optional[str] = None


@router.post("/chat/conversations", response_model=ConversationDetail)
def create_conversation(req: CreateConversationRequest):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO chat_conversations (user_id, title)
            VALUES (%s, %s)
            RETURNING id
            """,
            (str(req.user_id), (req.title or "New chat").strip()[:120]),
        )
        conversation_row = cur.fetchone()
        if not conversation_row:
            raise HTTPException(status_code=500, detail="Failed to create conversation")
        conversation_id = conversation_row[0]
        conn.commit()
        cur.close()

    return _fetch_conversation_detail(conversation_id, req.user_id)


@router.delete("/chat/conversations/{conversation_id}")
def delete_conversation(conversation_id: UUID, user_id: UUID):
    _ensure_conversation_access(conversation_id, user_id)

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM chat_conversations
            WHERE id = %s AND user_id = %s
            """,
            (str(conversation_id), str(user_id)),
        )
        deleted = cur.rowcount
        conn.commit()
        cur.close()

    if deleted == 0:
        raise HTTPException(status_code=404, detail="Conversation not found")

    return {"ok": True, "conversation_id": str(conversation_id)}


# ── Non-streaming endpoint (kept for backwards compat) ──────────────────────
@router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest, authorization: Optional[str] = Header(default=None)):
    del authorization  # reserved for future auth validation

    if not _API_KEY:
        raise HTTPException(
            status_code=500,
            detail="CHAT_OPENAI_API_KEY or API_KEY not configured",
        )
    if not req.messages:
        raise HTTPException(status_code=422, detail="messages array must not be empty")

    client = _get_sync_client()
    api_messages = _build_api_messages(req)

    try:
        response = client.responses.create(
            model=req.model or _MODEL,
            input=api_messages,
            text=_TEXT_CONFIG,
            reasoning=_REASONING_CONFIG,
            tools=_TOOLS,
            include=_INCLUDE,
            store=True,
        )
    except openai.APIStatusError as exc:
        print(f"⚠️ Chat API error: {exc.status_code} — {exc.message}")
        raise HTTPException(
            status_code=502,
            detail=f"Upstream AI service returned {exc.status_code}",
        ) from exc
    except openai.OpenAIError as exc:
        print(f"⚠️ Chat API error: {exc}")
        raise HTTPException(
            status_code=502, detail="Failed to reach AI service"
        ) from exc

    output_text = _extract_output_text(response)
    sources = _extract_web_search_sources(response)
    annotations = _extract_annotations(response)

    if not output_text:
        raise HTTPException(status_code=502, detail="No response from AI model")

    conversation_id = _upsert_conversation(
        user_id=req.user_id,
        messages=req.messages,
        conversation_id=req.conversation_id,
        title=req.title,
    )
    _replace_conversation_messages(
        conversation_id=conversation_id,
        user_id=req.user_id,
        messages=req.messages,
        assistant_reply=output_text,
        edited_message_index=req.edited_message_index,
    )

    return ChatResponse(
        reply=output_text,
        model=response.model,
        conversation_id=conversation_id,
        sources=sources,
        annotations=annotations,
    )


# ── Streaming endpoint (SSE) ────────────────────────────────────────────────
@router.post("/chat/stream")
async def chat_stream(
    req: ChatRequest, authorization: Optional[str] = Header(default=None)
):
    del authorization  # reserved for future auth validation

    if not _API_KEY:
        raise HTTPException(
            status_code=500,
            detail="CHAT_OPENAI_API_KEY or API_KEY not configured",
        )
    if not req.messages:
        raise HTTPException(status_code=422, detail="messages array must not be empty")

    conversation_id = _upsert_conversation(
        user_id=req.user_id,
        messages=req.messages,
        conversation_id=req.conversation_id,
        title=req.title,
    )
    client = _get_async_client()
    api_messages = _build_api_messages(req)

    async def event_generator():
        collected_chunks: list[str] = []
        collected_sources: list[dict[str, Any]] = []
        collected_annotations: list[dict[str, Any]] = []

        try:
            stream = await client.responses.create(
                model=req.model or _MODEL,
                input=api_messages,
                text=_TEXT_CONFIG,
                reasoning=_REASONING_CONFIG,
                tools=_TOOLS,
                store=True,
                include=_INCLUDE,
                stream=True,
            )
            async for event in stream:
                if event.type == "response.output_text.delta":
                    collected_chunks.append(event.delta)
                    payload = json.dumps(
                        {"delta": event.delta, "conversation_id": str(conversation_id)}
                    )
                    yield f"data: {payload}\n\n"
                elif event.type == "response.output_item.done":
                    item = getattr(event, "item", None)
                    if getattr(item, "type", None) == "web_search_call":
                        response_like = type("ResponseLike", (), {"output": [item]})()
                        collected_sources = _dedupe_payloads(
                            collected_sources
                            + _extract_web_search_sources(response_like)
                        )
                elif event.type == "response.output_text.annotation.added":
                    annotation = _to_jsonable(getattr(event, "annotation", None))
                    if isinstance(annotation, dict):
                        collected_annotations = _dedupe_payloads(
                            collected_annotations + [annotation]
                        )
                elif event.type == "response.completed":
                    full_text = "".join(collected_chunks).strip()
                    if full_text:
                        _replace_conversation_messages(
                            conversation_id=conversation_id,
                            user_id=req.user_id,
                            messages=req.messages,
                            assistant_reply=full_text,
                            edited_message_index=req.edited_message_index,
                        )
                    sources = (
                        _extract_web_search_sources(event.response) or collected_sources
                    )
                    annotations = (
                        _extract_annotations(event.response) or collected_annotations
                    )
                    done_payload = json.dumps(
                        {
                            "model": event.response.model,
                            "conversation_id": str(conversation_id),
                            "sources": sources,
                            "annotations": annotations,
                        }
                    )
                    yield f"data: {done_payload}\n\n"
            yield "data: [DONE]\n\n"
        except openai.APIStatusError as exc:
            err = json.dumps(
                {
                    "error": f"Upstream AI service returned {exc.status_code}",
                    "conversation_id": str(conversation_id),
                }
            )
            yield f"data: {err}\n\n"
            yield "data: [DONE]\n\n"
        except openai.OpenAIError as exc:
            err = json.dumps(
                {"error": str(exc), "conversation_id": str(conversation_id)}
            )
            yield f"data: {err}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
