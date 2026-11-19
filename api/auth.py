"""
Consensus — Supabase JWT authentication.

The frontend signs users in with Supabase and sends the session's access
token as ``Authorization: Bearer <jwt>``. This module verifies that token
and exposes the user to routes as a FastAPI dependency.

Configuration (environment variables)
-------------------------------------
AUTH_MODE
    ``off`` (default) — tokens are optional. A valid token is still used to
        identify the user, but requests without one keep working exactly as
        before (chat falls back to the ``user_id`` sent by the client). This
        lets the code be deployed before the frontend sends tokens.
    ``on`` — a valid token is required on protected routes (401 otherwise),
        and chat only ever uses the user id from the token.

SUPABASE_JWT_SECRET
    The project's legacy JWT secret (Supabase dashboard → Project Settings →
    API → JWT Settings). Used to verify HS256-signed tokens.

SUPABASE_URL
    e.g. ``https://<project-ref>.supabase.co``. Used to fetch the project's
    public signing keys (JWKS) for projects that use asymmetric JWT signing
    keys (ES256 / RS256).

Set whichever matches the project; setting both is fine — the token's own
``alg`` header decides which one is used.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional
from uuid import UUID

import jwt
from fastapi import Depends, Header, HTTPException

logger = logging.getLogger(__name__)

_AUDIENCE = "authenticated"
_ASYMMETRIC_ALGORITHMS = ["ES256", "RS256"]


@dataclass(frozen=True)
class AuthUser:
    id: UUID
    email: Optional[str] = None
    role: Optional[str] = None


class AuthConfigError(RuntimeError):
    """AUTH_MODE=on but no way to verify tokens is configured."""


def auth_required() -> bool:
    return os.environ.get("AUTH_MODE", "off").strip().lower() in {"on", "true", "1", "required"}


@lru_cache(maxsize=4)
def _jwks_client(jwks_url: str) -> jwt.PyJWKClient:
    # PyJWKClient caches keys and refetches when it sees an unknown key id.
    return jwt.PyJWKClient(jwks_url, cache_keys=True, lifespan=3600, timeout=5)


def _signing_key_for(token: str, header: dict) -> tuple[object, list[str]]:
    alg = header.get("alg")
    secret = os.environ.get("SUPABASE_JWT_SECRET", "").strip()
    supabase_url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")

    if alg == "HS256":
        if not secret:
            raise AuthConfigError("Token is HS256-signed but SUPABASE_JWT_SECRET is not set.")
        return secret, ["HS256"]

    if alg in _ASYMMETRIC_ALGORITHMS:
        if not supabase_url:
            raise AuthConfigError(f"Token is {alg}-signed but SUPABASE_URL is not set.")
        client = _jwks_client(f"{supabase_url}/auth/v1/.well-known/jwks.json")
        return client.get_signing_key_from_jwt(token).key, [alg]

    raise jwt.InvalidTokenError(f"Unsupported token algorithm: {alg!r}")


def verify_token(token: str) -> AuthUser:
    """
    Verify a Supabase access token and return the user it belongs to.

    Raises ``jwt.InvalidTokenError`` for bad/expired/forged tokens and
    ``AuthConfigError`` when the server can't verify tokens at all.
    """
    header = jwt.get_unverified_header(token)
    key, algorithms = _signing_key_for(token, header)

    claims = jwt.decode(
        token,
        key=key,
        algorithms=algorithms,
        audience=_AUDIENCE,
        options={"require": ["exp", "sub"]},
        leeway=30,
    )

    try:
        user_id = UUID(str(claims["sub"]))
    except ValueError as exc:
        raise jwt.InvalidTokenError("Token subject is not a user id") from exc

    return AuthUser(id=user_id, email=claims.get("email"), role=claims.get("role"))


def _bearer_token(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    scheme, _, token = authorization.strip().partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status_code=401,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def get_optional_user(authorization: Optional[str] = Header(default=None)) -> Optional[AuthUser]:
    """
    Dependency: the verified user, or None.

    - AUTH_MODE=on: missing or invalid token → 401.
    - AUTH_MODE=off: missing or invalid token → None (request proceeds as
      before). A valid token still identifies the user.
    """
    token = _bearer_token(authorization)
    required = auth_required()

    if token is None:
        if required:
            raise _unauthorized("Not authenticated. Please sign in again.")
        return None

    try:
        return verify_token(token)
    except AuthConfigError as exc:
        logger.error("Auth misconfigured: %s", exc)
        if required:
            raise HTTPException(status_code=503, detail="Authentication is not configured on the server.") from exc
        return None
    except jwt.PyJWKClientError as exc:
        logger.error("Could not fetch Supabase signing keys: %s", exc)
        if required:
            raise HTTPException(status_code=503, detail="Could not verify sign-in right now. Please try again.") from exc
        return None
    except jwt.InvalidTokenError as exc:
        if required:
            message = "Session expired. Please sign in again." if isinstance(exc, jwt.ExpiredSignatureError) else "Invalid session. Please sign in again."
            raise _unauthorized(message) from exc
        logger.info("Ignoring invalid token while AUTH_MODE=off: %s", exc)
        return None


def require_user(user: Optional[AuthUser] = Depends(get_optional_user)) -> Optional[AuthUser]:
    """
    Dependency for routes that must be signed in once AUTH_MODE=on.

    With AUTH_MODE=off this is a no-op (returns the user if a valid token was
    sent, else None), so existing clients keep working.
    """
    return user


# Router-level dependency list for routes that need sign-in once AUTH_MODE=on.
# Used in api/main.py: app.include_router(router, dependencies=SIGNED_IN).
# Chat resolves the user per request instead (see resolve_user_id).
SIGNED_IN = [Depends(require_user)]


def resolve_user_id(claimed_user_id: Optional[UUID], user: Optional[AuthUser]) -> UUID:
    """
    Decide which user a chat request acts as.

    - With a verified token, the token's user id wins. A different
      ``user_id`` in the request is rejected (403) instead of silently used.
    - Without a token (only possible when AUTH_MODE=off), fall back to the
      client-supplied ``user_id`` for backwards compatibility.
    """
    if user is not None:
        if claimed_user_id is not None and claimed_user_id != user.id:
            raise HTTPException(status_code=403, detail="user_id does not match the signed-in user.")
        return user.id

    if auth_required():  # defensive: get_optional_user already enforces this
        raise _unauthorized("Not authenticated. Please sign in again.")

    if claimed_user_id is None:
        raise HTTPException(status_code=422, detail="user_id is required.")
    return claimed_user_id


def log_auth_configuration() -> None:
    """Log the auth setup at startup so misconfiguration is obvious in logs."""
    # uvicorn's logger, so the message shows in normal server output.
    startup_logger = logging.getLogger("uvicorn.error")
    has_secret = bool(os.environ.get("SUPABASE_JWT_SECRET", "").strip())
    has_url = bool(os.environ.get("SUPABASE_URL", "").strip())

    if auth_required():
        if not (has_secret or has_url):
            startup_logger.error(
                "AUTH_MODE=on but neither SUPABASE_JWT_SECRET nor SUPABASE_URL is set - "
                "every protected request will fail with 503."
            )
        else:
            startup_logger.info("Auth: required (AUTH_MODE=on).")
    else:
        startup_logger.info("Auth: optional (AUTH_MODE=off). Set AUTH_MODE=on to require sign-in.")
