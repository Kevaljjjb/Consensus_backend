"""Tests for POST /api/chat — RAG chatbot endpoint."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes import chat as chat_route
from api.routes.chat import reset_sessions


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(chat_route.router, prefix="/api")
    return app


# ── Fake DB layer ────────────────────────────────────────────────────────────

class FakeChatCursor:
    """Minimal cursor that returns sample listings for vector search."""

    def __init__(self, rows=None):
        self._rows = rows or [
            (
                1, "https://example.com/1", "BizBen", "HVAC Company – Los Angeles",
                "Los Angeles", "CA", "US", "HVAC", "Full-service HVAC company",
                "$500,000", "$1,200,000", "$300,000", "$250,000", 0.1,
            ),
            (
                2, "https://example.com/2", "BizBuySell", "Plumbing Business – San Diego",
                "San Diego", "CA", "US", "Plumbing", "Residential plumbing services",
                "$350,000", "$900,000", "$200,000", "$180,000", 0.2,
            ),
        ]
        self.description = [
            ("id",), ("url",), ("source",), ("title",),
            ("city",), ("state",), ("country",), ("industry",),
            ("description",),
            ("price",), ("gross_revenue",), ("cash_flow",), ("ebitda",),
            ("distance",),
        ]

    def execute(self, query, params=None):
        pass

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeChatConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _patch_db(monkeypatch, cursor=None):
    cursor = cursor or FakeChatCursor()
    connection = FakeChatConnection(cursor)

    @contextmanager
    def fake_get_db():
        yield connection

    monkeypatch.setattr(chat_route, "get_db", fake_get_db)


def _patch_embedding(monkeypatch):
    """Mock get_embedding to return a dummy vector."""
    monkeypatch.setattr(chat_route, "get_embedding", lambda text: [0.1] * 1024)


def _patch_openai(monkeypatch):
    """Mock the OpenAI client to return a canned response."""
    mock_client = MagicMock()
    mock_choice = MagicMock()
    mock_choice.message.content = "Based on the listings, there are HVAC businesses in California."
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client.chat.completions.create.return_value = mock_completion

    monkeypatch.setattr(chat_route, "_get_openai_client", lambda: mock_client)
    return mock_client


# ── Tests ────────────────────────────────────────────────────────────────────

def test_chat_returns_reply_and_sources(monkeypatch):
    """A valid chat request should return a reply and source listings."""
    _patch_db(monkeypatch)
    _patch_embedding(monkeypatch)
    _patch_openai(monkeypatch)
    reset_sessions()

    client = TestClient(_build_app())
    response = client.post("/api/chat", json={"message": "What HVAC businesses are in California?"})

    assert response.status_code == 200
    body = response.json()
    assert "session_id" in body
    assert "reply" in body
    assert body["reply"] != ""
    assert "sources" in body
    assert len(body["sources"]) == 2
    assert body["sources"][0]["title"] == "HVAC Company – Los Angeles"
    assert body["sources"][1]["id"] == 2


def test_chat_creates_new_session(monkeypatch):
    """When no session_id is provided, a new one should be created."""
    _patch_db(monkeypatch)
    _patch_embedding(monkeypatch)
    _patch_openai(monkeypatch)
    reset_sessions()

    client = TestClient(_build_app())
    response = client.post("/api/chat", json={"message": "Show me businesses"})

    assert response.status_code == 200
    body = response.json()
    session_id = body["session_id"]
    assert session_id is not None
    assert len(session_id) == 36  # UUID format


def test_chat_maintains_session_history(monkeypatch):
    """Same session_id across two calls should maintain conversation history."""
    _patch_db(monkeypatch)
    _patch_embedding(monkeypatch)
    mock_client = _patch_openai(monkeypatch)
    reset_sessions()

    client = TestClient(_build_app())

    # First message
    r1 = client.post("/api/chat", json={"message": "Show me HVAC businesses"})
    assert r1.status_code == 200
    session_id = r1.json()["session_id"]

    # Second message with same session
    r2 = client.post("/api/chat", json={
        "session_id": session_id,
        "message": "Tell me more about the first one",
    })
    assert r2.status_code == 200
    assert r2.json()["session_id"] == session_id

    # The second OpenAI call should include history (system + user1 + assistant1 + user2)
    second_call_messages = mock_client.chat.completions.create.call_args_list[1][1]["messages"]
    # Should have: system prompt + user msg 1 + assistant msg 1 + user msg 2
    roles = [m["role"] for m in second_call_messages]
    assert roles[0] == "system"
    assert "user" in roles
    assert "assistant" in roles


def test_chat_empty_message_returns_422(monkeypatch):
    """An empty message should be rejected with 422."""
    reset_sessions()
    client = TestClient(_build_app())
    response = client.post("/api/chat", json={"message": ""})
    assert response.status_code == 422
