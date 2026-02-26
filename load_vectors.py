"""
Consensus — Backfill embeddings for raw_listings.

Queries rows where description_embedding IS NULL, generates embeddings
via Qwen3-Embedding-8B, and writes them back to the database.

Usage:
    cd backend
    python load_vectors.py              # process all rows
    python load_vectors.py --limit 50   # process at most 50 rows
    python load_vectors.py --batch 10   # batch size 10 (default 25)
"""

import argparse
import os
import sys
import time

from dotenv import load_dotenv

# Load .env
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from db.connection import get_connection
from embeddings import get_embeddings_batch


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill vector embeddings for raw_listings.")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to process (0 = all).")
    parser.add_argument("--batch", type=int, default=25, help="Batch size for API calls (default: 25).")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between batches in seconds.")
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    # Fetch rows needing embeddings
    query = "SELECT id, description FROM raw_listings WHERE description_embedding IS NULL"
    if args.limit > 0:
        query += f" LIMIT {args.limit}"
    cur.execute(query)
    rows = cur.fetchall()

    if not rows:
        print("✅ All rows already have embeddings.")
        conn.close()
        return

    total = len(rows)
    print(f"Found {total} rows needing embeddings (batch size {args.batch}).\n")

    processed = 0
    for i in range(0, total, args.batch):
        batch = rows[i : i + args.batch]
        ids = [r[0] for r in batch]
        texts = [r[1] or "" for r in batch]

        # Truncate very long descriptions to ~8000 chars to avoid token limits
        texts = [t[:8000] for t in texts]

        try:
            embeddings = get_embeddings_batch(texts)
        except Exception as exc:
            print(f"  ❌ API error on batch {i // args.batch + 1}: {exc}")
            print("  Waiting 10s before retrying...")
            time.sleep(10)
            try:
                embeddings = get_embeddings_batch(texts)
            except Exception as exc2:
                print(f"  ❌ Retry failed: {exc2}. Skipping batch.")
                continue

        for row_id, vec in zip(ids, embeddings):
            # pgvector expects a string like '[0.1, 0.2, ...]'
            vec_str = "[" + ",".join(str(v) for v in vec) + "]"
            cur.execute(
                "UPDATE raw_listings SET description_embedding = %s WHERE id = %s",
                (vec_str, row_id),
            )

        conn.commit()
        processed += len(batch)
        print(f"  ✅ Batch {i // args.batch + 1}: {processed}/{total} rows updated.")

        if i + args.batch < total:
            time.sleep(args.delay)

    conn.close()
    print(f"\n🎉 Done! Updated {processed} rows with embeddings.")


if __name__ == "__main__":
    main()
