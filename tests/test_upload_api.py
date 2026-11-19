from contextlib import contextmanager

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes import upload as upload_route


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(upload_route.router, prefix="/api")
    return app


class FakeUploadCursor:
    """
    Minimal cursor that simulates savepoint semantics: rows inserted after a
    SAVEPOINT are discarded by ROLLBACK TO SAVEPOINT, and everything pending
    is discarded by a full connection rollback.
    """

    def __init__(self, conn, existing_urls=(), failing_urls=(), similar=None):
        self.conn = conn
        self.existing_urls = set(existing_urls)
        self.failing_urls = set(failing_urls)
        self.similar = similar or []
        self.executions: list[tuple[str, object]] = []
        self.description = []
        self._fetchone = None
        self._fetchall = []
        self._savepoint_mark = None

    def execute(self, query, params=None):
        self.executions.append((query, params))
        q = " ".join(query.split())

        if q == "SAVEPOINT csv_row":
            self._savepoint_mark = len(self.conn.pending)
            return
        if q == "RELEASE SAVEPOINT csv_row":
            self._savepoint_mark = None
            return
        if q == "ROLLBACK TO SAVEPOINT csv_row":
            del self.conn.pending[self._savepoint_mark:]
            return

        if q.startswith("SELECT id FROM raw_listings WHERE url"):
            url = params[0]
            known = url in self.existing_urls or url in self.conn.committed or url in self.conn.pending
            self._fetchone = (1,) if known else None
            return

        if "description_embedding <=>" in q:
            self.description = [(c,) for c in ["id", "title", "url", "source", "city", "state", "distance"]]
            self._fetchall = list(self.similar)
            return

        if q.startswith("INSERT INTO raw_listings"):
            if params["url"] in self.failing_urls:
                raise RuntimeError("simulated insert failure")
            self.conn.pending.append(params["url"])
            self._fetchone = (len(self.conn.committed) + len(self.conn.pending),)
            return

        raise AssertionError(f"Unexpected query: {q}")

    def fetchone(self):
        return self._fetchone

    def fetchall(self):
        return self._fetchall

    def close(self):
        pass


class FakeConn:
    def __init__(self, **cursor_kwargs):
        self.pending: list[str] = []
        self.committed: list[str] = []
        self.rollbacks = 0
        self.cursor_obj = FakeUploadCursor(self, **cursor_kwargs)

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed.extend(self.pending)
        self.pending.clear()

    def rollback(self):
        self.rollbacks += 1
        self.pending.clear()


def _patch(monkeypatch, conn, embed_calls=None):
    @contextmanager
    def fake_get_db():
        yield conn

    def fake_get_embedding(text):
        if embed_calls is not None:
            embed_calls.append(text)
        return [0.1, 0.2, 0.3]

    monkeypatch.setattr(upload_route, "get_db", fake_get_db)
    monkeypatch.setattr(upload_route, "get_embedding", fake_get_embedding)


def _csv(rows):
    header = "Title,URL,Description\n"
    return (header + "".join(f"{t},{u},{d}\n" for t, u, d in rows)).encode("utf-8")


def _post_csv(client, content, filename="deals.csv"):
    return client.post("/api/upload/csv", files={"file": (filename, content, "text/csv")})


def test_csv_failed_row_does_not_discard_earlier_rows(monkeypatch):
    conn = FakeConn(failing_urls={"https://x.com/2"})
    _patch(monkeypatch, conn)
    client = TestClient(_build_app())

    res = _post_csv(client, _csv([
        ("A", "https://x.com/1", "first"),
        ("B", "https://x.com/2", "second"),
        ("C", "https://x.com/3", "third"),
    ]))

    assert res.status_code == 200
    body = res.json()
    assert body["inserted"] == 2
    assert [e["row"] for e in body["errors"]] == [3]
    # The reported count matches what was actually committed.
    assert conn.committed == ["https://x.com/1", "https://x.com/3"]
    assert conn.rollbacks == 0


def test_csv_skips_existing_and_missing_urls(monkeypatch):
    conn = FakeConn(existing_urls={"https://x.com/old"})
    _patch(monkeypatch, conn)
    client = TestClient(_build_app())

    res = _post_csv(client, _csv([
        ("Old", "https://x.com/old", "dup"),
        ("NoUrl", "", "missing"),
        ("New", "https://x.com/new", "fresh"),
    ]))

    body = res.json()
    assert body["inserted"] == 1
    assert body["skipped_duplicates"] == 1
    assert body["errors"] == [{"row": 3, "error": "Missing URL"}]
    assert conn.committed == ["https://x.com/new"]


def test_csv_embeds_each_description_once(monkeypatch):
    conn = FakeConn()
    calls: list[str] = []
    _patch(monkeypatch, conn, embed_calls=calls)
    client = TestClient(_build_app())

    _post_csv(client, _csv([("A", "https://x.com/1", "desc one"), ("B", "https://x.com/2", "desc two")]))

    assert calls == ["desc one", "desc two"]


def test_csv_rejects_oversized_file(monkeypatch):
    conn = FakeConn()
    _patch(monkeypatch, conn)
    monkeypatch.setattr(upload_route, "MAX_CSV_BYTES", 50)
    client = TestClient(_build_app())

    res = _post_csv(client, _csv([("A", "https://x.com/1", "x" * 100)]))

    assert res.status_code == 413
    assert conn.committed == []


def test_csv_caps_row_count(monkeypatch):
    conn = FakeConn()
    _patch(monkeypatch, conn)
    monkeypatch.setattr(upload_route, "MAX_CSV_ROWS", 2)
    client = TestClient(_build_app())

    res = _post_csv(client, _csv([(f"T{i}", f"https://x.com/{i}", "d") for i in range(5)]))

    body = res.json()
    assert body["inserted"] == 2
    assert body["truncated"] is True
    assert "first 2 rows" in body["message"]


def test_csv_rejects_non_csv_filename(monkeypatch):
    conn = FakeConn()
    _patch(monkeypatch, conn)
    client = TestClient(_build_app())

    res = _post_csv(client, b"a,b\n", filename="deals.txt")

    assert res.status_code == 400


def test_single_upload_embeds_once_and_returns_similar(monkeypatch):
    similar_row = (7, "Similar Co", "https://x.com/7", "BizBen", "LA", "CA", 0.05)
    conn = FakeConn(similar=[similar_row])
    calls: list[str] = []
    _patch(monkeypatch, conn, embed_calls=calls)
    client = TestClient(_build_app())

    res = client.post("/api/upload/single", json={
        "title": "Bakery",
        "url": "https://x.com/bakery",
        "description": "A busy bakery",
    })

    assert res.status_code == 200
    body = res.json()
    assert body["inserted"] is True
    assert body["similar_listings"][0]["id"] == 7
    assert calls == ["A busy bakery"]
    assert conn.committed == ["https://x.com/bakery"]


def test_single_upload_rejects_duplicate_url(monkeypatch):
    conn = FakeConn(existing_urls={"https://x.com/bakery"})
    _patch(monkeypatch, conn)
    client = TestClient(_build_app())

    res = client.post("/api/upload/single", json={"title": "Bakery", "url": "https://x.com/bakery"})

    body = res.json()
    assert body["inserted"] is False
    assert body["duplicate_url"] is True
    assert conn.committed == []
