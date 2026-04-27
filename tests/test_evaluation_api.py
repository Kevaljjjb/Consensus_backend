from contextlib import contextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes import evaluation as evaluation_route


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(evaluation_route.router, prefix="/api")
    return app


_LISTING_COLUMNS = [
    "id",
    "url",
    "source",
    "title",
    "city",
    "state",
    "country",
    "industry",
    "description",
    "listed_by_firm",
    "listed_by_name",
    "phone",
    "email",
    "price",
    "gross_revenue",
    "cash_flow",
    "inventory",
    "ebitda",
    "price_num",
    "gross_revenue_num",
    "cash_flow_num",
    "ebitda_num",
    "financial_data",
    "source_link",
    "extra_information",
    "deal_date",
    "first_seen_date",
    "last_seen_date",
    "scraping_date",
]

_EVALUATION_COLUMNS = [
    "fit_score",
    "score_breakdown",
    "pros",
    "cons",
    "summary",
    "model_used",
    "evaluated_at",
]

_LISTING_ROW = (
    1,
    "https://example.com/1",
    "BizBen",
    "HVAC Services Business",
    "Dallas",
    "TX",
    "US",
    "Home Services",
    "Established HVAC service business with recurring maintenance contracts.",
    "Firm A",
    "Broker A",
    "111",
    "broker@example.com",
    "$6,000,000",
    "$8,000,000",
    "$2,500,000",
    "$150,000",
    "$2,200,000",
    6000000,
    8000000,
    2500000,
    2200000,
    "Revenue stable for the last 3 years.",
    "https://example.com/source/1",
    "Founded in 2008. Mission-critical service work.",
    "2026-03-01",
    datetime(2026, 3, 2, tzinfo=timezone.utc),
    datetime(2026, 3, 20, tzinfo=timezone.utc),
    "2026-03-20",
)

_FIRST_EVALUATION = {
    "fit_score": 73,
    "score_breakdown": {
        "cash_flow": 18,
        "profitability": 17,
        "maturity": 12,
        "locality": 15,
        "stability": 11,
    },
    "pros": [
        "Reported cash flow of $2,500,000 is in range.",
        "Dallas, TX keeps the deal in Tucker's target geography.",
        "Recurring HVAC maintenance contracts support stability.",
    ],
    "cons": [
        "Customer concentration is not disclosed.",
        "Supplier diversification is not disclosed.",
        "Revenue growth quality is not described.",
    ],
    "summary": "Worth deeper diligence because the deal clears the cash flow and locality screens.",
    "model_used": "Qwen/Qwen3.5-27B",
}

_REFRESHED_EVALUATION = {
    "fit_score": 81,
    "score_breakdown": {
        "cash_flow": 21,
        "profitability": 19,
        "maturity": 14,
        "locality": 15,
        "stability": 12,
    },
    "pros": [
        "Reported cash flow of $2,500,000 supports platform-level earnings.",
        "Dallas, TX fits Tucker's target geography.",
        "The HVAC category aligns with Tucker's local service focus.",
    ],
    "cons": [
        "Customer concentration is still not disclosed.",
        "Supplier concentration is still not disclosed.",
        "The listing still lacks detailed churn metrics.",
    ],
    "summary": "This looks actionable for Tucker if concentration diligence comes back clean.",
    "model_used": "Qwen/Qwen3.5-27B",
}


def _unwrap_json(value):
    return getattr(value, "adapted", value)


class FakeEvaluationCursor:
    def __init__(self, *, has_listing=True, evaluation=None):
        self.executions: list[tuple[str, list]] = []
        self.description = []
        self._fetchone = None
        self._listing_row = _LISTING_ROW if has_listing else None
        self.evaluation = evaluation
        self.persisted_at = datetime(2026, 3, 29, 10, 0, tzinfo=timezone.utc)

    def execute(self, query, params=None):
        normalized = " ".join(query.split())
        bound_params = list(params or [])
        self.executions.append((normalized, bound_params))

        if normalized.startswith("SELECT") and "FROM deal_evaluations" in normalized:
            self.description = [(name,) for name in _EVALUATION_COLUMNS]
            self._fetchone = self._evaluation_tuple(self.evaluation) if self.evaluation else None
            return

        if normalized.startswith("SELECT") and "FROM raw_listings" in normalized:
            self.description = [(name,) for name in _LISTING_COLUMNS]
            self._fetchone = self._listing_row
            return

        if normalized.startswith("DELETE FROM deal_evaluations"):
            self.evaluation = None
            self._fetchone = None
            return

        if normalized.startswith("INSERT INTO deal_evaluations"):
            self.description = [(name,) for name in _EVALUATION_COLUMNS]
            self.evaluation = {
                "fit_score": bound_params[1],
                "score_breakdown": _unwrap_json(bound_params[2]),
                "pros": _unwrap_json(bound_params[3]),
                "cons": _unwrap_json(bound_params[4]),
                "summary": bound_params[5],
                "model_used": bound_params[6],
            }
            self._fetchone = self._evaluation_tuple(self.evaluation)
            return

        raise AssertionError(f"Unexpected query: {normalized}")

    def _evaluation_tuple(self, evaluation):
        return (
            evaluation["fit_score"],
            evaluation["score_breakdown"],
            evaluation["pros"],
            evaluation["cons"],
            evaluation["summary"],
            evaluation["model_used"],
            self.persisted_at,
        )

    def fetchone(self):
        return self._fetchone

    def close(self):
        return None


class FakeEvaluationConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _patch_db(monkeypatch, cursor):
    connection = FakeEvaluationConnection(cursor)

    @contextmanager
    def fake_get_db():
        yield connection

    monkeypatch.setattr(evaluation_route, "get_db", fake_get_db)
    return connection


def test_get_evaluation_cache_miss_then_cache_hit(monkeypatch):
    cursor = FakeEvaluationCursor()
    connection = _patch_db(monkeypatch, cursor)
    calls = {"count": 0}

    def fake_evaluate_deal(listing):
        calls["count"] += 1
        assert listing["title"] == "HVAC Services Business"
        return _FIRST_EVALUATION

    monkeypatch.setattr(evaluation_route, "evaluate_deal", fake_evaluate_deal)
    client = TestClient(_build_app())

    first = client.get("/api/listings/1/evaluation")
    assert first.status_code == 200
    assert first.json()["cached"] is False
    assert first.json()["fit_score"] == 73
    assert first.json()["model_used"] == "Qwen/Qwen3.5-27B"
    assert connection.commits == 1

    second = client.get("/api/listings/1/evaluation")
    assert second.status_code == 200
    assert second.json()["cached"] is True
    assert second.json()["fit_score"] == 73
    assert calls["count"] == 1


def test_get_evaluation_returns_404_when_listing_missing(monkeypatch):
    cursor = FakeEvaluationCursor(has_listing=False)
    _patch_db(monkeypatch, cursor)
    monkeypatch.setattr(evaluation_route, "evaluate_deal", lambda listing: _FIRST_EVALUATION)
    client = TestClient(_build_app())

    response = client.get("/api/listings/999/evaluation")
    assert response.status_code == 404
    assert response.json()["detail"] == "Listing not found"


def test_refresh_recomputes_and_replaces_existing_evaluation(monkeypatch):
    cursor = FakeEvaluationCursor(evaluation=_FIRST_EVALUATION)
    connection = _patch_db(monkeypatch, cursor)
    monkeypatch.setattr(evaluation_route, "evaluate_deal", lambda listing: _REFRESHED_EVALUATION)
    client = TestClient(_build_app())

    response = client.post("/api/listings/1/evaluation/refresh")
    assert response.status_code == 200
    body = response.json()
    assert body["cached"] is False
    assert body["fit_score"] == 81
    assert body["summary"] == _REFRESHED_EVALUATION["summary"]
    assert connection.commits == 1
    assert cursor.evaluation["fit_score"] == 81
    assert any(sql.startswith("DELETE FROM deal_evaluations") for sql, _ in cursor.executions)
