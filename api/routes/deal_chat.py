"""
Consensus — Deal-scoped AI Chat (streaming).

POST /api/deal-chat/stream  — accepts deal context + conversation history,
                              streams tokens from gpt-5.4-mini via SSE.
"""

import json
import os
from typing import Optional

import openai
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter(tags=["deal-chat"])

_API_KEY = os.environ.get("API_KEY", "")
_MODEL = "gpt-5.4-mini"

async_client = openai.AsyncOpenAI(
    api_key=_API_KEY or None,
    base_url="https://api.openai.com/v1",
)


def _build_system_prompt(deal: dict) -> str:
    """Build a system prompt that injects the full deal context."""
    return (
        "You are a concise, knowledgeable M&A analyst assistant embedded on a deal page. "
        "The user is evaluating the business acquisition opportunity described below. "
        "Answer questions about this deal using ONLY the provided deal data. "
        "Be specific, reference real numbers from the deal, and keep answers focused. "
        "Use markdown formatting (bold, lists, headers) when helpful. "
        "If the deal data does not contain the answer, say so honestly.\n\n"
        "=== DEAL CONTEXT ===\n"
        f"Title: {deal.get('title', 'N/A')}\n"
        f"Industry: {deal.get('industry', 'N/A')}\n"
        f"Location: {deal.get('city', 'N/A')}, {deal.get('state', 'N/A')}, {deal.get('country', 'N/A')}\n"
        f"Source: {deal.get('source', 'N/A')}\n\n"
        "--- Financials ---\n"
        f"Asking Price: {deal.get('price', 'N/A')}\n"
        f"Gross Revenue: {deal.get('gross_revenue', 'N/A')}\n"
        f"EBITDA: {deal.get('ebitda', 'N/A')}\n"
        f"Cash Flow: {deal.get('cash_flow', 'N/A')}\n"
        f"Inventory: {deal.get('inventory', 'N/A')}\n\n"
        "--- Description ---\n"
        f"{deal.get('description', 'N/A')}\n\n"
        "--- Additional Information ---\n"
        f"{deal.get('extra_information', 'N/A')}\n"
    )


class ChatMessage(BaseModel):
    role: str  # "user" | "assistant"
    content: str


class DealChatRequest(BaseModel):
    deal: dict
    messages: list[ChatMessage]


@router.post("/deal-chat/stream")
async def deal_chat_stream(req: DealChatRequest):
    if not _API_KEY:
        raise HTTPException(status_code=500, detail="API_KEY not configured")
    if not req.messages:
        raise HTTPException(status_code=422, detail="messages array must not be empty")

    system_prompt = _build_system_prompt(req.deal)
    api_messages = [{"role": "system", "content": system_prompt}]
    for msg in req.messages:
        api_messages.append({"role": msg.role, "content": msg.content})

    async def event_generator():
        try:
            stream = await async_client.responses.create(
                model=_MODEL,
                input=api_messages,
                text={
                    "format": {"type": "text"},
                    "verbosity": "low",
                },
                reasoning={
                    "effort": "low",
                    "summary": "auto",
                },
                store=True,
                stream=True,
            )
            async for event in stream:
                if event.type == "response.output_text.delta":
                    payload = json.dumps({"delta": event.delta})
                    yield f"data: {payload}\n\n"
                elif event.type == "response.completed":
                    done_payload = json.dumps({"model": event.response.model})
                    yield f"data: {done_payload}\n\n"
            yield "data: [DONE]\n\n"
        except openai.APIStatusError as exc:
            err = json.dumps({"error": f"Upstream AI service returned {exc.status_code}"})
            yield f"data: {err}\n\n"
            yield "data: [DONE]\n\n"
        except openai.OpenAIError as exc:
            err = json.dumps({"error": str(exc)})
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
