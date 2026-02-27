"""
Consensus — FastAPI application entry point.

Provides REST API endpoints for:
  - Listing browsing & filtering
  - Semantic search (pgvector + Qwen3 embeddings)
  - Deal upload with duplication detection
  - Dashboard statistics
"""

import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Load .env before anything else
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from api.routes.listings import router as listings_router
from api.routes.dashboard import router as dashboard_router
from api.routes.search import router as search_router
from api.routes.upload import router as upload_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    from db.connection import init_pool, close_pool, get_db
    try:
        init_pool(minconn=2, maxconn=10)
        # Quick verification that the pool works
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
        print("✅ Database connection pool initialised.")
    except Exception as exc:
        print(f"⚠️  Database pool init failed: {exc}")
    yield
    close_pool()
    print("🛑 Database connection pool closed.")


app = FastAPI(
    title="Consensus API",
    description="AI-powered business acquisition intelligence platform",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow all origins for the public API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount route modules
app.include_router(listings_router, prefix="/api")
app.include_router(dashboard_router, prefix="/api")
app.include_router(search_router, prefix="/api")
app.include_router(upload_router, prefix="/api")


@app.get("/api/health")
def health():
    return {"status": "ok", "app": "Consensus"}
