"""
Tucker's Farm — Database connection helper.

Uses a ThreadedConnectionPool to keep persistent connections to Supabase,
eliminating the ~800ms TCP + SSL + auth handshake overhead per request.
"""

import os
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool

# ── Connection pool (module-level singleton) ─────────────────────────────────

_pool: pool.ThreadedConnectionPool | None = None


def init_pool(minconn: int = 2, maxconn: int = 10) -> None:
    """Initialise the connection pool.  Call once at app startup."""
    global _pool
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. "
            "Add it to your .env or export it before running."
        )
    _pool = pool.ThreadedConnectionPool(minconn, maxconn, database_url)


def close_pool() -> None:
    """Close every connection in the pool.  Call at app shutdown."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


def get_connection():
    """Return a connection from the pool (or create a fresh one as fallback)."""
    if _pool is not None:
        return _pool.getconn()
    # Fallback for scripts that don't call init_pool (scrapers, CLI tools)
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set.")
    return psycopg2.connect(database_url)


def put_connection(conn) -> None:
    """Return a connection back to the pool."""
    if _pool is not None:
        _pool.putconn(conn)
    else:
        conn.close()


@contextmanager
def get_db():
    """Context manager that auto-returns the connection to the pool."""
    conn = get_connection()
    try:
        yield conn
    finally:
        put_connection(conn)


# ── Schema helper (unchanged) ────────────────────────────────────────────────

def run_schema(schema_path: str = None) -> None:
    """Execute the schema.sql migration file against the database."""
    if schema_path is None:
        schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")

    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()

    with get_db() as conn:
        try:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            print("✅ Schema applied successfully.")
        except Exception as exc:
            conn.rollback()
            print(f"❌ Schema error: {exc}")
            raise


if __name__ == "__main__":
    # Quick way to apply schema: python -m db.connection
    from dotenv import load_dotenv
    load_dotenv()
    run_schema()
