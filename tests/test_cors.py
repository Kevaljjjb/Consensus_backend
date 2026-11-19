from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient

from api.cors import get_cors_settings


def _client(env):
    app = FastAPI()
    app.add_middleware(CORSMiddleware, **get_cors_settings(env))

    @app.get("/api/health")
    def health():
        return {"status": "ok"}

    return TestClient(app)


def _preflight(client, origin):
    return client.options(
        "/api/health",
        headers={"Origin": origin, "Access-Control-Request-Method": "GET"},
    )


def test_default_allows_any_origin_without_credentials():
    settings = get_cors_settings({})
    assert settings["allow_origins"] == ["*"]
    assert settings["allow_credentials"] is False

    res = _client({}).get("/api/health", headers={"Origin": "https://anything.example"})
    assert res.headers["access-control-allow-origin"] == "*"
    assert "access-control-allow-credentials" not in res.headers


def test_allowlist_is_parsed_and_trimmed():
    settings = get_cors_settings({"CORS_ORIGINS": " https://app.example.com/ , http://localhost:3000,, "})
    assert settings["allow_origins"] == ["https://app.example.com", "http://localhost:3000"]
    assert settings["allow_credentials"] is True


def test_allowlist_accepts_listed_origin_and_rejects_others():
    client = _client({"CORS_ORIGINS": "https://app.example.com"})

    ok = _preflight(client, "https://app.example.com")
    assert ok.status_code == 200
    assert ok.headers["access-control-allow-origin"] == "https://app.example.com"
    assert ok.headers["access-control-allow-credentials"] == "true"

    bad = _preflight(client, "https://evil.example")
    assert bad.status_code == 400
    assert "access-control-allow-origin" not in bad.headers


def test_origin_regex_allows_preview_deployments():
    client = _client({
        "CORS_ORIGINS": "https://consensus.vercel.app",
        "CORS_ORIGIN_REGEX": r"https://consensus-[a-z0-9-]+\.vercel\.app",
    })

    res = _preflight(client, "https://consensus-git-feature-abc.vercel.app")
    assert res.status_code == 200
    assert _preflight(client, "https://evil.vercel.app").status_code == 400


def test_explicit_wildcard_never_enables_credentials():
    settings = get_cors_settings({"CORS_ORIGINS": "*"})
    assert settings["allow_origins"] == ["*"]
    assert settings["allow_credentials"] is False
