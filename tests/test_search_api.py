from contextlib import contextmanager
from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes import search as search_route


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(search_route.router, prefix="/api")
    return app


class FakeSearchCursor:
    def __init__(self):
        self.executions: list[tuple[str, list]] = []
        self.description = []
        self._fetchall = []

    def execute(self, query, params=None):
        bound_params = list(params or [])
        self.executions.append((query, bound_params))
        self.description = [(name,) for name in [
            "id", "url", "source", "title", "city", "state", "country", "industry", "description",
            "listed_by_firm", "listed_by_name", "phone", "email",
            "price", "gross_revenue", "cash_flow", "inventory", "ebitda",
            "financial_data", "source_link", "extra_information", "deal_date",
            "first_seen_date", "last_seen_date", "scraping_date",
            "price_num", "gross_revenue_num", "cash_flow_num", "ebitda_num",
        ]]
        self._fetchall = [(
            2, "https://example.com/2", "BizBen", "Auto Shop", "Reno", "NV", "US", "Automotive", "Auto service",
            "Firm", "Broker", "222", "b@example.com",
            "$400,000", "$1,100,000", "$220,000", "N/A", "$190,000",
            "N/A", "source", "N/A", "N/A",
            "2026-01-01T00:00:00Z", "2026-02-25T00:00:00Z", "2026-02-25",
            Decimal("400000"), Decimal("1100000"), Decimal("220000"), Decimal("190000"),
        )]

    def fetchall(self):
        return self._fetchall

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def close(self):
        return None


class FakeSearchConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _patch_db(monkeypatch, cursor):
    connection = FakeSearchConnection(cursor)

    @contextmanager
    def fake_get_db():
        yield connection

    monkeypatch.setattr(search_route, "get_db", fake_get_db)


def test_search_text_path_applies_filters_in_sql(monkeypatch):
    cursor = FakeSearchCursor()
    _patch_db(monkeypatch, cursor)
    client = TestClient(_build_app())

    response = client.get(
        "/api/search",
        params={
            "q": "hvac",  # short query => text path
            "limit": 20,
            "source": "BizBen",
            "industry": "Automotive",
            "state": "NV",
            "country": "US",
            "min_price": 100000,
            "max_price": 500000,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["method"] == "text"
    assert body["total"] == 1
    assert body["data"][0]["price_numeric"] == 400000.0

    sql, params = cursor.executions[0]
    assert "source = %s" in sql
    assert "industry = %s" in sql
    assert "price_num >= %s" in sql
    assert "price_num <= %s" in sql
    assert params[-1] == 20


def test_search_invalid_range_returns_422():
    client = TestClient(_build_app())
    response = client.get("/api/search", params={"q": "hvac", "min_price": 900000, "max_price": 100000})
    assert response.status_code == 422
    assert "min_price cannot be greater than max_price" in response.json()["detail"]
