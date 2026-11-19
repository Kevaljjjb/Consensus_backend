"""
Consensus — CORS configuration.

Origins are read from the environment so production can be locked down
without code changes:

  CORS_ORIGINS       Comma-separated list of allowed origins,
                     e.g. "https://consensus.vercel.app,http://localhost:3000"
  CORS_ORIGIN_REGEX  Optional regex for extra origins, e.g. Vercel previews:
                     "https://consensus-.*\\.vercel\\.app"

If neither is set, any origin is allowed but *without* credentials. A
wildcard origin combined with allow_credentials=True makes Starlette echo
back whatever Origin the browser sends, which lets any website make
credentialed requests to the API — so credentials are only enabled when an
explicit allowlist is configured.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any


def get_cors_settings(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Return keyword arguments for ``CORSMiddleware``."""
    env = os.environ if env is None else env

    origins = [
        origin.strip().rstrip("/")
        for origin in env.get("CORS_ORIGINS", "").split(",")
        if origin.strip()
    ]
    origin_regex = env.get("CORS_ORIGIN_REGEX", "").strip() or None

    if "*" in origins or (not origins and not origin_regex):
        return {
            "allow_origins": ["*"],
            "allow_credentials": False,
            "allow_methods": ["*"],
            "allow_headers": ["*"],
        }

    return {
        "allow_origins": origins,
        "allow_origin_regex": origin_regex,
        "allow_credentials": True,
        "allow_methods": ["*"],
        "allow_headers": ["*"],
    }
