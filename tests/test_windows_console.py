"""
Regression tests: the API must not crash when stdout can't encode emoji.

Windows consoles default to cp1252. The app used to print() emoji (e.g.
"⚠️ Reranker failed"), which raised UnicodeEncodeError inside error
handlers — turning a recoverable reranker failure into a 500, and making
startup crash when the database was unreachable.
"""

import io
import sys
from contextlib import contextmanager

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes import search as search_route


def _cp1252_stdout(monkeypatch):
    stream = io.TextIOWrapper(io.BytesIO(), encoding="cp1252", errors="strict")
    monkeypatch.setattr(sys, "stdout", stream)
    return stream


def test_search_falls_back_when_reranker_fails_on_cp1252_console(monkeypatch):
    _cp1252_stdout(monkeypatch)

    class _Cursor:
        def execute(self, *a, **k):
            pass

        def fetchone(self):
            return (4,)  # numeric filter columns exist

        def fetchall(self):
            return []

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    @contextmanager
    def fake_get_db():
        yield type("Conn", (), {"cursor": lambda self: _Cursor()})()

    def failing_rerank(*_args, **_kwargs):
        raise RuntimeError("reranker down")

    candidates = [
        {"id": 1, "title": "HVAC Co", "description": "hvac services", "distance": 0.10},
        {"id": 2, "title": "Plumbing Co", "description": "plumbing", "distance": 0.20},
    ]

    monkeypatch.setattr(search_route, "get_db", fake_get_db)
    monkeypatch.setattr(search_route, "get_embedding", lambda q: [0.1, 0.2])
    monkeypatch.setattr(search_route, "_semantic_candidates", lambda *a, **k: [dict(c) for c in candidates])
    monkeypatch.setattr(search_route, "rerank_documents", failing_rerank)
    monkeypatch.setattr(search_route, "get_numeric_columns_available", lambda cur: True, raising=False)

    app = FastAPI()
    app.include_router(search_route.router, prefix="/api")
    res = TestClient(app).get("/api/search", params={"q": "hvac service business in texas"})

    assert res.status_code == 200
    assert [d["id"] for d in res.json()["data"]] == [1, 2]


def test_app_startup_survives_unreachable_db_on_cp1252_console(monkeypatch):
    _cp1252_stdout(monkeypatch)
    monkeypatch.setenv("OPENAI_API_KEY", "test")  # deal_chat builds a client at import

    import db.connection as connection
    import api.main as main

    def failing_init_pool(**_kwargs):
        raise RuntimeError("could not connect")

    monkeypatch.setattr(connection, "init_pool", failing_init_pool)

    with TestClient(main.app) as client:  # runs the lifespan startup + shutdown
        assert client.get("/api/health").status_code == 200
