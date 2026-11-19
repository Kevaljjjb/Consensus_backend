import time
from contextlib import contextmanager
from types import SimpleNamespace
from uuid import UUID, uuid4

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient

from api import auth
from api.routes import chat as chat_route
from api.routes import upload as upload_route

SECRET = "test-jwt-secret-that-is-long-enough-for-hs256"


@pytest.fixture(autouse=True)
def clean_auth_env(monkeypatch):
    for name in ("AUTH_MODE", "SUPABASE_JWT_SECRET", "SUPABASE_URL"):
        monkeypatch.delenv(name, raising=False)
    auth._jwks_client.cache_clear()
    yield
    # A test may have monkeypatched _jwks_client with a plain function.
    getattr(auth._jwks_client, "cache_clear", lambda: None)()


def _claims(user_id: UUID, **overrides):
    now = int(time.time())
    claims = {
        "sub": str(user_id),
        "aud": "authenticated",
        "role": "authenticated",
        "email": "user@example.com",
        "iat": now,
        "exp": now + 3600,
    }
    claims.update(overrides)
    return claims


def _hs256_token(user_id: UUID, secret: str = SECRET, **overrides) -> str:
    return jwt.encode(_claims(user_id, **overrides), secret, algorithm="HS256")


# ── verify_token ─────────────────────────────────────────────────────────────

def test_verify_hs256_token(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", SECRET)
    user_id = uuid4()

    user = auth.verify_token(_hs256_token(user_id))

    assert user.id == user_id
    assert user.email == "user@example.com"


@pytest.mark.parametrize(
    "make_token",
    [
        lambda uid: _hs256_token(uid, secret="a-different-secret-of-sufficient-length!!"),
        lambda uid: _hs256_token(uid, exp=int(time.time()) - 3600),
        lambda uid: _hs256_token(uid, aud="someone-else"),
        lambda uid: _hs256_token(uid, sub="not-a-uuid"),
        lambda uid: jwt.encode(_claims(uid), key=None, algorithm="none"),
    ],
    ids=["wrong-secret", "expired", "wrong-audience", "non-uuid-subject", "alg-none"],
)
def test_verify_rejects_bad_tokens(monkeypatch, make_token):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", SECRET)

    with pytest.raises(jwt.InvalidTokenError):
        auth.verify_token(make_token(uuid4()))


def test_verify_hs256_without_secret_is_config_error():
    with pytest.raises(auth.AuthConfigError):
        auth.verify_token(_hs256_token(uuid4()))


def test_verify_es256_token_via_jwks(monkeypatch):
    private_key = ec.generate_private_key(ec.SECP256R1())
    user_id = uuid4()
    token = jwt.encode(_claims(user_id), private_key, algorithm="ES256", headers={"kid": "key-1"})
    requested_urls = []

    class FakeJWKSClient:
        def get_signing_key_from_jwt(self, _token):
            return SimpleNamespace(key=private_key.public_key())

    def fake_jwks_client(url):
        requested_urls.append(url)
        return FakeJWKSClient()

    monkeypatch.setenv("SUPABASE_URL", "https://proj.supabase.co/")
    monkeypatch.setattr(auth, "_jwks_client", fake_jwks_client)

    user = auth.verify_token(token)

    assert user.id == user_id
    assert requested_urls == ["https://proj.supabase.co/auth/v1/.well-known/jwks.json"]


# ── get_optional_user ────────────────────────────────────────────────────────

def test_off_mode_without_token_returns_none():
    assert auth.get_optional_user(None) is None


def test_off_mode_ignores_invalid_token(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", SECRET)
    assert auth.get_optional_user("Bearer garbage") is None


def test_off_mode_still_uses_valid_token(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", SECRET)
    user_id = uuid4()
    user = auth.get_optional_user(f"Bearer {_hs256_token(user_id)}")
    assert user is not None and user.id == user_id


@pytest.mark.parametrize("header", [None, "", "Basic abc", "Bearer "])
def test_on_mode_requires_bearer_token(monkeypatch, header):
    monkeypatch.setenv("AUTH_MODE", "on")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", SECRET)

    with pytest.raises(HTTPException) as exc:
        auth.get_optional_user(header)
    assert exc.value.status_code == 401


def test_on_mode_expired_token_says_session_expired(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "on")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", SECRET)
    token = _hs256_token(uuid4(), exp=int(time.time()) - 3600)

    with pytest.raises(HTTPException) as exc:
        auth.get_optional_user(f"Bearer {token}")
    assert exc.value.status_code == 401
    assert "expired" in exc.value.detail.lower()


def test_on_mode_without_verification_config_is_503(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "on")

    with pytest.raises(HTTPException) as exc:
        auth.get_optional_user(f"Bearer {_hs256_token(uuid4())}")
    assert exc.value.status_code == 503


# ── resolve_user_id ──────────────────────────────────────────────────────────

def test_resolve_prefers_token_user():
    user = auth.AuthUser(id=uuid4())
    assert auth.resolve_user_id(None, user) == user.id
    assert auth.resolve_user_id(user.id, user) == user.id


def test_resolve_rejects_mismatched_user_id():
    with pytest.raises(HTTPException) as exc:
        auth.resolve_user_id(uuid4(), auth.AuthUser(id=uuid4()))
    assert exc.value.status_code == 403


def test_resolve_falls_back_to_claimed_id_in_off_mode():
    claimed = uuid4()
    assert auth.resolve_user_id(claimed, None) == claimed


def test_resolve_requires_some_user_id_in_off_mode():
    with pytest.raises(HTTPException) as exc:
        auth.resolve_user_id(None, None)
    assert exc.value.status_code == 422


# ── Routes ───────────────────────────────────────────────────────────────────

class _RecordingCursor:
    def __init__(self):
        self.executions = []

    def execute(self, query, params=None):
        self.executions.append((" ".join(query.split()), params))

    def fetchall(self):
        return []

    def close(self):
        pass


def _chat_client(monkeypatch):
    cursor = _RecordingCursor()

    @contextmanager
    def fake_get_db():
        yield SimpleNamespace(cursor=lambda: cursor)

    monkeypatch.setattr(chat_route, "get_db", fake_get_db)
    app = FastAPI()
    app.include_router(chat_route.router, prefix="/api")
    return TestClient(app), cursor


def test_list_conversations_off_mode_keeps_legacy_user_id(monkeypatch):
    client, cursor = _chat_client(monkeypatch)
    claimed = uuid4()

    res = client.get("/api/chat/conversations", params={"user_id": str(claimed)})

    assert res.status_code == 200
    assert cursor.executions[0][1] == (str(claimed),)


def test_list_conversations_on_mode_uses_token_user(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "on")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", SECRET)
    client, cursor = _chat_client(monkeypatch)
    user_id = uuid4()

    res = client.get(
        "/api/chat/conversations",
        headers={"Authorization": f"Bearer {_hs256_token(user_id)}"},
    )

    assert res.status_code == 200
    assert cursor.executions[0][1] == (str(user_id),)


def test_on_mode_blocks_reading_another_users_conversations(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "on")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", SECRET)
    client, cursor = _chat_client(monkeypatch)

    res = client.get(
        "/api/chat/conversations",
        params={"user_id": str(uuid4())},
        headers={"Authorization": f"Bearer {_hs256_token(uuid4())}"},
    )

    assert res.status_code == 403
    assert cursor.executions == []


def test_on_mode_chat_without_token_is_401_before_any_work(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "on")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", SECRET)
    client, cursor = _chat_client(monkeypatch)

    res = client.post(
        "/api/chat",
        json={"messages": [{"role": "user", "content": "hi"}], "user_id": str(uuid4())},
    )

    assert res.status_code == 401
    assert res.headers["www-authenticate"] == "Bearer"
    assert cursor.executions == []


def _upload_client():
    # Mirrors how api/main.py mounts protected routers.
    app = FastAPI()
    app.include_router(upload_route.router, prefix="/api", dependencies=[Depends(auth.require_user)])
    return TestClient(app)


def test_main_app_protects_the_right_routes(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test")  # deal_chat builds a client at import
    import api.main as main

    def is_protected(path):
        route = next(r for r in main.app.routes if getattr(r, "path", None) == path)
        return any(dep.call is auth.require_user for dep in route.dependant.dependencies)

    assert is_protected("/api/upload/csv")
    assert is_protected("/api/upload/single")
    assert is_protected("/api/listings/{listing_id}/evaluation")
    assert is_protected("/api/listings/{listing_id}/evaluation/refresh")
    assert is_protected("/api/deal-chat/stream")
    assert not is_protected("/api/listings")
    assert not is_protected("/api/health")


def test_protected_router_is_open_in_off_mode():
    res = _upload_client().post("/api/upload/csv", files={"file": ("x.txt", b"a", "text/plain")})
    # Reaches the route itself (which rejects the non-CSV file), not auth.
    assert res.status_code == 400


def test_protected_router_requires_token_in_on_mode(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "on")
    monkeypatch.setenv("SUPABASE_JWT_SECRET", SECRET)
    client = _upload_client()

    assert client.post("/api/upload/csv", files={"file": ("x.csv", b"a", "text/csv")}).status_code == 401

    res = client.post(
        "/api/upload/csv",
        files={"file": ("x.txt", b"a", "text/plain")},
        headers={"Authorization": f"Bearer {_hs256_token(uuid4())}"},
    )
    assert res.status_code == 400  # authenticated → reaches the route
