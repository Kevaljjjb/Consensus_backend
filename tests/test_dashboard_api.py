from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes import dashboard as dashboard_route


def _build_app() -> FastAPI:
    app = FastAPI()
    app.include_router(dashboard_route.router, prefix="/api")
    return app


class FakeDashboardCursor:
    def __init__(
        self,
        *,
        snapshot_row,
        source_rows,
        priority_rows,
        pipeline_exists: bool = False,
        pipeline_columns_ok: bool = True,
        sla_row=None,
    ):
        self.snapshot_row = snapshot_row
        self.source_rows = source_rows
        self.priority_rows = priority_rows
        self.pipeline_exists = pipeline_exists
        self.pipeline_columns_ok = pipeline_columns_ok
        self.sla_row = sla_row

        self.executions: list[tuple[str, list]] = []
        self._fetchone = None
        self._fetchall = []

    def execute(self, query, params=None):
        bound_params = list(params or [])
        self.executions.append((query, bound_params))

        if "COUNT(*) AS total_listings" in query:
            self._fetchone = self.snapshot_row
            self._fetchall = []
            return

        if "GROUP BY 1" in query and "qualified_rate" in query:
            self._fetchone = None
            self._fetchall = self.source_rows
            return

        if "FROM scored" in query and "LEAST(100" in query:
            self._fetchone = None
            self._fetchall = self.priority_rows
            return

        if "SELECT to_regclass('public.pipeline')" in query:
            self._fetchone = ("public.pipeline",) if self.pipeline_exists else (None,)
            self._fetchall = []
            return

        if "information_schema.columns" in query and "table_name = 'pipeline'" in query:
            if self.pipeline_columns_ok:
                self._fetchall = [
                    ("created_at",),
                    ("responded_at",),
                    ("offered_at",),
                    ("closed_at",),
                    ("status",),
                ]
            else:
                self._fetchall = [("created_at",)]
            self._fetchone = None
            return

        if "FROM pipeline" in query and "response_48h_rate" in query:
            self._fetchone = self.sla_row
            self._fetchall = []
            return

        raise AssertionError(f"Unexpected query: {query}")

    def fetchone(self):
        return self._fetchone

    def fetchall(self):
        return self._fetchall

    def close(self):
        return None


class FakeDashboardConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _patch_db(monkeypatch, cursor):
    connection = FakeDashboardConnection(cursor)

    @contextmanager
    def fake_get_db():
        yield connection

    monkeypatch.setattr(dashboard_route, "get_db", fake_get_db)


@pytest.fixture(autouse=True)
def reset_dashboard_cache():
    dashboard_route.reset_dashboard_overview_cache()
    yield
    dashboard_route.reset_dashboard_overview_cache()


def _default_snapshot_row():
    return (
        100,
        9,
        18,
        Decimal("0.18"),
        4,
        12,
        72,
        40,
        31,
        Decimal("0.91"),
        Decimal("0.73"),
        Decimal("0.66"),
        Decimal("0.95"),
    )


def _default_source_rows():
    return [
        ("BizBen", 50, 15, Decimal("0.3")),
        ("BizBuySell", 40, 3, Decimal("0.075")),
    ]


def _default_priority_rows():
    return [
        (
            101,
            "Great Deal",
            "BizBen",
            "CA",
            "US",
            "$12,000,000",
            "$1,800,000",
            "$2,400,000",
            datetime(2026, 2, 25, 12, 0, tzinfo=timezone.utc),
            100,
            ["Local", "Cash Flow Fit", "Margin Fit", "Revenue >= $10M", "New This Week"],
        ),
        (
            102,
            "Solid Deal",
            "BizBuySell",
            "NV",
            "US",
            "$5,000,000",
            "$500,000",
            "$1,200,000",
            datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc),
            65,
            ["Local", "Cash Flow Near Fit", "Margin Fit"],
        ),
    ]


def test_dashboard_overview_response_shape_and_sla_nulls(monkeypatch):
    cursor = FakeDashboardCursor(
        snapshot_row=_default_snapshot_row(),
        source_rows=_default_source_rows(),
        priority_rows=_default_priority_rows(),
        pipeline_exists=False,
    )
    _patch_db(monkeypatch, cursor)

    client = TestClient(_build_app())
    response = client.get("/api/dashboard/overview")

    assert response.status_code == 200
    body = response.json()

    assert set(body.keys()) == {
        "generated_at",
        "snapshot",
        "criteria_funnel",
        "source_yield",
        "priority_queue",
        "sla",
        "data_quality",
    }
    assert body["generated_at"].endswith("Z")

    assert set(body["snapshot"].keys()) == {
        "total_listings",
        "new_this_week",
        "qualified_count",
        "pass_rate",
        "active_sources",
        "distinct_industries",
    }
    assert [stage["stage"] for stage in body["criteria_funnel"]] == [
        "All Listings",
        "Local (US/CA)",
        "Cash Flow Fit",
        "Margin Fit",
        "Shortlist",
    ]
    assert body["sla"] == {
        "response_48h_rate": None,
        "offer_5d_rate": None,
        "close_60d_rate": None,
        "in_pipeline": None,
    }


def test_dashboard_overview_criteria_counts(monkeypatch):
    cursor = FakeDashboardCursor(
        snapshot_row=_default_snapshot_row(),
        source_rows=_default_source_rows(),
        priority_rows=_default_priority_rows(),
        pipeline_exists=False,
    )
    _patch_db(monkeypatch, cursor)

    client = TestClient(_build_app())
    response = client.get("/api/dashboard/overview", params={"lookback_days": 120, "country_scope": "US, CA"})

    assert response.status_code == 200
    body = response.json()

    assert body["snapshot"]["total_listings"] == 100
    assert body["snapshot"]["qualified_count"] == 18
    assert body["snapshot"]["pass_rate"] == 0.18

    assert body["criteria_funnel"][0]["count"] == 100
    assert body["criteria_funnel"][1]["count"] == 72
    assert body["criteria_funnel"][2]["count"] == 40
    assert body["criteria_funnel"][3]["count"] == 31
    assert body["criteria_funnel"][4]["count"] == 18


def test_dashboard_priority_queue_score_ordering(monkeypatch):
    cursor = FakeDashboardCursor(
        snapshot_row=_default_snapshot_row(),
        source_rows=_default_source_rows(),
        priority_rows=_default_priority_rows(),
        pipeline_exists=False,
    )
    _patch_db(monkeypatch, cursor)

    client = TestClient(_build_app())
    response = client.get("/api/dashboard/overview", params={"priority_limit": 2})

    assert response.status_code == 200
    queue = response.json()["priority_queue"]

    assert queue[0]["fit_score"] >= queue[1]["fit_score"]
    assert queue[0]["reasons"][:3] == ["Local", "Cash Flow Fit", "Margin Fit"]

    priority_query = next(q for q, _ in cursor.executions if "FROM scored" in q and "LEAST(100" in q)
    assert "ORDER BY fit_score DESC" in priority_query


def test_dashboard_overview_null_handling_for_empty_data(monkeypatch):
    cursor = FakeDashboardCursor(
        snapshot_row=(0, 0, 0, Decimal("0"), 0, 0, 0, 0, 0, Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0")),
        source_rows=[],
        priority_rows=[],
        pipeline_exists=False,
    )
    _patch_db(monkeypatch, cursor)

    client = TestClient(_build_app())
    response = client.get("/api/dashboard/overview", params={"lookback_days": 30, "country_scope": ""})

    assert response.status_code == 200
    body = response.json()

    assert body["snapshot"]["pass_rate"] == 0.0
    assert body["source_yield"] == []
    assert body["priority_queue"] == []
    assert body["data_quality"] == {
        "parseable_revenue_pct": 0.0,
        "parseable_ebitda_pct": 0.0,
        "parseable_cash_flow_pct": 0.0,
        "parseable_location_pct": 0.0,
    }
