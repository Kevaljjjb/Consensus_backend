import json
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from api.routes import chat as chat_route
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(chat_route.router, prefix="/api")
    return app


class FakeChatCursor:
    def __init__(self):
        self.executions: list[tuple[str, tuple | list | None]] = []
        self._fetchone_queue: list[tuple | None] = []
        self._fetchall_queue: list[list[tuple]] = []
        self.rowcount = 0

    def queue_fetchone(self, *rows):
        self._fetchone_queue.extend(rows)

    def queue_fetchall(self, *rows_sets):
        self._fetchall_queue.extend(rows_sets)

    def execute(self, query, params=None):
        normalized = " ".join(query.split())
        bound_params = tuple(params) if params is not None else None
        self.executions.append((normalized, bound_params))

        if normalized.startswith("DELETE FROM chat_conversations"):
            self.rowcount = 1
        else:
            self.rowcount = 0

    def fetchone(self):
        if self._fetchone_queue:
            return self._fetchone_queue.pop(0)
        return None

    def fetchall(self):
        if self._fetchall_queue:
            return self._fetchall_queue.pop(0)
        return []

    def close(self):
        return None


class FakeChatConnection:
    def __init__(self, cursor: FakeChatCursor):
        self._cursor = cursor
        self.commit_count = 0
        self.rollback_count = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1


def _patch_db(monkeypatch, cursor: FakeChatCursor):
    connection = FakeChatConnection(cursor)

    @contextmanager
    def fake_get_db():
        yield connection

    monkeypatch.setattr(chat_route, "get_db", fake_get_db)
    return connection


class FakeAsyncStream:
    def __init__(self, events):
        self._events = list(events)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._events:
            raise StopAsyncIteration
        return self._events.pop(0)


class FakeAsyncResponsesAPI:
    def __init__(self, events):
        self._events = events
        self.calls: list[dict] = []

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        return FakeAsyncStream(self._events)


class FakeAsyncClient:
    def __init__(self, events):
        self.responses = FakeAsyncResponsesAPI(events)


class FakeSyncResponsesAPI:
    def __init__(self, response):
        self._response = response
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return self._response


class FakeSyncClient:
    def __init__(self, response):
        self.responses = FakeSyncResponsesAPI(response)


def test_list_conversations_returns_summaries(monkeypatch):
    cursor = FakeChatCursor()
    _patch_db(monkeypatch, cursor)
    client = TestClient(_build_app())

    user_id = uuid4()
    conversation_id = uuid4()
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    cursor.queue_fetchall(
        [
            (
                conversation_id,
                user_id,
                "Deal review",
                now,
                now,
                4,
                "Latest assistant reply",
            )
        ]
    )

    response = client.get("/api/chat/conversations", params={"user_id": str(user_id)})

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": str(conversation_id),
            "user_id": str(user_id),
            "title": "Deal review",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "message_count": 4,
            "preview": "Latest assistant reply",
        }
    ]

    query, params = cursor.executions[0]
    assert "FROM chat_conversations c" in query
    assert "LEFT JOIN chat_messages m" in query
    assert params == (str(user_id),)


def test_get_conversation_returns_messages(monkeypatch):
    cursor = FakeChatCursor()
    _patch_db(monkeypatch, cursor)
    client = TestClient(_build_app())

    user_id = uuid4()
    conversation_id = uuid4()
    message_id = uuid4()
    now = datetime(2026, 1, 2, 15, 30, tzinfo=timezone.utc)

    cursor.queue_fetchone((user_id,))
    cursor.queue_fetchone((conversation_id, user_id, "HVAC chat", now, now))
    cursor.queue_fetchall(
        [
            (
                message_id,
                conversation_id,
                user_id,
                "user",
                "Tell me about this deal",
                0,
                None,
                now,
            ),
            (
                uuid4(),
                conversation_id,
                user_id,
                "assistant",
                "Here is the analysis",
                1,
                None,
                now,
            ),
        ]
    )

    response = client.get(
        f"/api/chat/conversations/{conversation_id}",
        params={"user_id": str(user_id)},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(conversation_id)
    assert body["user_id"] == str(user_id)
    assert body["title"] == "HVAC chat"
    assert len(body["messages"]) == 2
    assert body["messages"][0]["role"] == "user"
    assert body["messages"][0]["content"] == "Tell me about this deal"
    assert body["messages"][1]["role"] == "assistant"

    assert "SELECT user_id FROM chat_conversations" in cursor.executions[0][0]
    assert (
        "SELECT id, user_id, title, created_at, updated_at FROM chat_conversations"
        in cursor.executions[1][0]
    )
    assert "FROM chat_messages" in cursor.executions[2][0]


def test_get_conversation_forbidden_for_other_user(monkeypatch):
    cursor = FakeChatCursor()
    _patch_db(monkeypatch, cursor)
    client = TestClient(_build_app())

    conversation_id = uuid4()
    owner_user_id = uuid4()
    requesting_user_id = uuid4()

    cursor.queue_fetchone((owner_user_id,))

    response = client.get(
        f"/api/chat/conversations/{conversation_id}",
        params={"user_id": str(requesting_user_id)},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "You do not have access to this conversation"


def test_create_conversation_returns_empty_detail(monkeypatch):
    cursor = FakeChatCursor()
    connection = _patch_db(monkeypatch, cursor)
    client = TestClient(_build_app())

    user_id = uuid4()
    conversation_id = uuid4()
    now = datetime(2026, 1, 3, 9, 0, tzinfo=timezone.utc)

    cursor.queue_fetchone((conversation_id,))
    cursor.queue_fetchone((user_id,))
    cursor.queue_fetchone((conversation_id, user_id, "New chat", now, now))
    cursor.queue_fetchall([[]][0])

    response = client.post(
        "/api/chat/conversations",
        json={"user_id": str(user_id), "title": "New chat"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(conversation_id)
    assert body["messages"] == []

    insert_query, insert_params = cursor.executions[0]
    assert "INSERT INTO chat_conversations (user_id, title)" in insert_query
    assert insert_params == (str(user_id), "New chat")
    assert connection.commit_count == 1


def test_delete_conversation_removes_owned_conversation(monkeypatch):
    cursor = FakeChatCursor()
    connection = _patch_db(monkeypatch, cursor)
    client = TestClient(_build_app())

    conversation_id = uuid4()
    user_id = uuid4()

    cursor.queue_fetchone((user_id,))

    response = client.delete(
        f"/api/chat/conversations/{conversation_id}",
        params={"user_id": str(user_id)},
    )

    assert response.status_code == 200
    assert response.json() == {"ok": True, "conversation_id": str(conversation_id)}

    assert "SELECT user_id FROM chat_conversations" in cursor.executions[0][0]
    delete_query, delete_params = cursor.executions[1]
    assert "DELETE FROM chat_conversations" in delete_query
    assert delete_params == (str(conversation_id), str(user_id))
    assert connection.commit_count == 1


def test_chat_uses_gpt54_web_search_config(monkeypatch):
    user_id = uuid4()
    conversation_id = uuid4()
    source = {"type": "url", "url": "https://example.com/benchmarks"}
    annotation = {
        "type": "url_citation",
        "title": "Benchmarks",
        "url": "https://example.com/benchmarks",
        "start_index": 0,
        "end_index": 10,
    }
    fake_response = SimpleNamespace(
        model="gpt-5.4",
        output=[
            SimpleNamespace(
                type="message",
                content=[
                    SimpleNamespace(
                        type="output_text",
                        text="Use current market benchmarks alongside your diligence.",
                        annotations=[annotation],
                    )
                ],
            ),
            SimpleNamespace(
                type="web_search_call",
                action=SimpleNamespace(type="search", sources=[source]),
            ),
        ],
    )
    fake_client = FakeSyncClient(fake_response)
    captured: dict[str, object] = {}

    def fake_upsert_conversation(**kwargs):
        captured["upsert"] = kwargs
        return conversation_id

    def fake_replace_conversation_messages(**kwargs):
        captured["replace"] = kwargs

    monkeypatch.setattr(chat_route, "_API_KEY", "test-key")
    monkeypatch.setattr(chat_route, "_sync_client", None)
    monkeypatch.setattr(chat_route, "_get_sync_client", lambda: fake_client)
    monkeypatch.setattr(chat_route, "_upsert_conversation", fake_upsert_conversation)
    monkeypatch.setattr(
        chat_route,
        "_replace_conversation_messages",
        fake_replace_conversation_messages,
    )

    client = TestClient(_build_app())
    response = client.post(
        "/api/chat",
        json={
            "user_id": str(user_id),
            "messages": [{"role": "user", "content": "How should I benchmark this?"}],
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "reply": "Use current market benchmarks alongside your diligence.",
        "model": "gpt-5.4",
        "conversation_id": str(conversation_id),
        "sources": [source],
        "annotations": [annotation],
    }

    request_kwargs = fake_client.responses.calls[0]
    assert request_kwargs["model"] == "gpt-5.4"
    assert request_kwargs["text"] == {
        "format": {"type": "text"},
        "verbosity": "medium",
    }
    assert request_kwargs["reasoning"] == {
        "effort": "medium",
        "summary": "auto",
    }
    assert request_kwargs["tools"] == [
        {
            "type": "web_search",
            "user_location": {"type": "approximate"},
            "search_context_size": "medium",
        }
    ]
    assert request_kwargs["include"] == [
        "reasoning.encrypted_content",
        "web_search_call.action.sources",
    ]
    assert request_kwargs["store"] is True
    assert request_kwargs["input"][0]["role"] == "system"
    assert "web search" in request_kwargs["input"][0]["content"].lower()
    assert captured["replace"]["assistant_reply"] == (
        "Use current market benchmarks alongside your diligence."
    )


def test_chat_stream_includes_sources_and_annotations(monkeypatch):
    user_id = uuid4()
    conversation_id = uuid4()
    source = {"type": "url", "url": "https://example.com/benchmarks"}
    annotation = {
        "type": "url_citation",
        "title": "Benchmarks",
        "url": "https://example.com/benchmarks",
        "start_index": 0,
        "end_index": 10,
    }
    events = [
        SimpleNamespace(type="response.output_text.delta", delta="Market "),
        SimpleNamespace(type="response.output_text.delta", delta="context"),
        SimpleNamespace(
            type="response.output_item.done",
            item=SimpleNamespace(
                type="web_search_call",
                action=SimpleNamespace(type="search", sources=[source]),
            ),
        ),
        SimpleNamespace(
            type="response.completed",
            response=SimpleNamespace(
                model="gpt-5.4",
                output=[
                    SimpleNamespace(
                        type="message",
                        content=[
                            SimpleNamespace(
                                type="output_text",
                                text="Market context",
                                annotations=[annotation],
                            )
                        ],
                    ),
                    SimpleNamespace(
                        type="web_search_call",
                        action=SimpleNamespace(type="search", sources=[source]),
                    ),
                ],
            ),
        ),
    ]
    fake_client = FakeAsyncClient(events)
    captured: dict[str, object] = {}

    def fake_upsert_conversation(**kwargs):
        captured["upsert"] = kwargs
        return conversation_id

    def fake_replace_conversation_messages(**kwargs):
        captured["replace"] = kwargs

    monkeypatch.setattr(chat_route, "_API_KEY", "test-key")
    monkeypatch.setattr(chat_route, "_async_client", None)
    monkeypatch.setattr(chat_route, "_get_async_client", lambda: fake_client)
    monkeypatch.setattr(chat_route, "_upsert_conversation", fake_upsert_conversation)
    monkeypatch.setattr(
        chat_route,
        "_replace_conversation_messages",
        fake_replace_conversation_messages,
    )

    client = TestClient(_build_app())
    response = client.post(
        "/api/chat/stream",
        json={
            "user_id": str(user_id),
            "messages": [{"role": "user", "content": "How should I benchmark this?"}],
        },
    )

    assert response.status_code == 200

    payloads = [
        json.loads(line[6:])
        for line in response.text.splitlines()
        if line.startswith("data: ") and line[6:] != "[DONE]"
    ]

    assert payloads[0] == {
        "delta": "Market ",
        "conversation_id": str(conversation_id),
    }
    assert payloads[1] == {
        "delta": "context",
        "conversation_id": str(conversation_id),
    }
    assert payloads[-1] == {
        "model": "gpt-5.4",
        "conversation_id": str(conversation_id),
        "sources": [source],
        "annotations": [annotation],
    }

    request_kwargs = fake_client.responses.calls[0]
    assert request_kwargs["model"] == "gpt-5.4"
    assert request_kwargs["text"] == {
        "format": {"type": "text"},
        "verbosity": "medium",
    }
    assert request_kwargs["reasoning"] == {
        "effort": "medium",
        "summary": "auto",
    }
    assert request_kwargs["tools"] == [
        {
            "type": "web_search",
            "user_location": {"type": "approximate"},
            "search_context_size": "medium",
        }
    ]
    assert request_kwargs["include"] == [
        "reasoning.encrypted_content",
        "web_search_call.action.sources",
    ]
    assert request_kwargs["store"] is True
    assert request_kwargs["stream"] is True
    assert captured["replace"]["assistant_reply"] == "Market context"
