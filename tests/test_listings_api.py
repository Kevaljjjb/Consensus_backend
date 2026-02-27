from contextlib import contextmanager
from decimal import Decimal

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes import listings as listings_route


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(listings_route.router, prefix="/api")
    return app


class FakeListingsCursor:
    def __init__(self):
        self.executions: list[tuple[str, list]] = []
        self.description = []
        self._fetchone = None
        self._fetchall = []

    def execute(self, query, params=None):
        bound_params = list(params or [])
        self.executions.append((query, bound_params))

        if "COUNT(*)" in query:
            self._fetchone = (21,)
            return

        if "SELECT DISTINCT source" in query:
            self._fetchall = [("BizBen",), ("BizBuySell",)]
            return

        if "SELECT DISTINCT industry" in query:
            self._fetchall = [("Auto",), ("Manufacturing",)]
            return

        if "SELECT DISTINCT state" in query:
            self._fetchall = [("CA",), ("NV",)]
            return

        if "SELECT DISTINCT country" in query:
            self._fetchall = [("US",), ("CA",)]
            return

        self.description = [(name,) for name in [
            "id", "url", "source", "title", "city", "state", "country", "industry", "description",
            "listed_by_firm", "listed_by_name", "phone", "email",
            "price", "gross_revenue", "cash_flow", "inventory", "ebitda",
            "financial_data", "source_link", "extra_information", "deal_date",
            "first_seen_date", "last_seen_date", "scraping_date",
            "price_num", "gross_revenue_num", "cash_flow_num", "ebitda_num",
        ]]
        self._fetchall = [(
            1, "https://example.com/1", "BizBen", "HVAC Business", "Los Angeles", "CA", "US", "Services", "Nice deal",
            "Firm A", "Broker A", "111", "a@example.com",
            "$900,000", "$1,500,000", "$300,000", "N/A", "$250,000",
            "N/A", "source", "N/A", "N/A",
            "2026-02-01T00:00:00Z", "2026-02-20T00:00:00Z", "2026-02-20",
            Decimal("900000"), Decimal("1500000"), Decimal("300000"), Decimal("250000"),
        )]

    def fetchone(self):
        return self._fetchone

    def fetchall(self):
        return self._fetchall

    def close(self):
        return None


class FakeListingsConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _patch_db(monkeypatch, cursor):
    connection = FakeListingsConnection(cursor)

    @contextmanager
    def fake_get_db():
        yield connection

    monkeypatch.setattr(listings_route, "get_db", fake_get_db)


def test_listings_filter_combination_and_pagination(monkeypatch):
    cursor = FakeListingsCursor()
    _patch_db(monkeypatch, cursor)
    client = TestClient(_build_app())

    response = client.get(
        "/api/listings",
        params={
            "page": 2,
            "per_page": 10,
            "source": "BizBen",
            "industry": "Services",
            "state": "CA",
            "country": "US",
            "min_cash_flow": 100000,
            "max_cash_flow": 500000,
            "min_revenue": 1000000,
            "sort_by": "cash_flow_num",
            "sort_order": "asc",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 21
    assert body["page"] == 2
    assert body["per_page"] == 10
    assert body["total_pages"] == 3
    assert body["data"][0]["cash_flow_numeric"] == 300000.0

    count_query, count_params = cursor.executions[0]
    assert "source = %s" in count_query
    assert "industry = %s" in count_query
    assert "cash_flow_num >= %s" in count_query
    assert "cash_flow_num <= %s" in count_query
    assert "gross_revenue_num >= %s" in count_query
    assert count_params[:4] == ["BizBen", "Services", "CA", "US"]

    page_query, page_params = cursor.executions[1]
    assert "ORDER BY cash_flow_num ASC NULLS LAST, id DESC" in page_query
    assert page_params[-2:] == [10, 10]


def test_listings_invalid_range_returns_422():
    client = TestClient(_build_app())
    response = client.get("/api/listings", params={"min_cash_flow": 500000, "max_cash_flow": 100000})
    assert response.status_code == 422
    assert "min_cash_flow cannot be greater than max_cash_flow" in response.json()["detail"]


def test_listings_invalid_sort_returns_422():
    client = TestClient(_build_app())
    response = client.get("/api/listings", params={"sort_by": "title"})
    assert response.status_code == 422
    assert "Invalid sort_by" in response.json()["detail"]


def test_filter_options_endpoint(monkeypatch):
    cursor = FakeListingsCursor()
    _patch_db(monkeypatch, cursor)
    client = TestClient(_build_app())

    response = client.get("/api/listings/filter-options")
    assert response.status_code == 200
    assert response.json() == {
        "source": ["BizBen", "BizBuySell"],
        "industry": ["Auto", "Manufacturing"],
        "state": ["CA", "NV"],
        "country": ["US", "CA"],
    }
