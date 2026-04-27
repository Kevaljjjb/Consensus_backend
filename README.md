<![CDATA[# Consensus — Backend

> FastAPI server powering the AI-driven business acquisition intelligence platform.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Framework | FastAPI |
| Language | Python 3.10+ |
| Database | PostgreSQL (Supabase) |
| Vector Search | pgvector |
| DB Driver | psycopg2 |
| Embeddings | Qwen3-Embedding-8B via DeepInfra |
| Reranking | Qwen3-Reranker-8B via DeepInfra |
| LLM | DeepSeek V3.2 (OpenAI-compatible) |
| Scraping | Camoufox (stealth browser) |
| Testing | pytest |

---

## Getting Started

### Prerequisites

- **Python** ≥ 3.10
- **PostgreSQL** database with pgvector extension (Supabase recommended)
- API keys for DeepInfra (embeddings + LLM)

### 1. Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment

Create a `.env` file in this directory:

```env
# Database
DATABASE_URL=postgresql://postgres:<password>@db.<project-ref>.supabase.co:5432/postgres

# LLM (OpenAI-compatible)
OPENAI_API_KEY=<your-deepinfra-key>
OPENAI_BASE_URL=https://api.deepinfra.com/v1/openai
OPENAI_MODEL=deepseek-ai/DeepSeek-V3.2

# Embeddings
EMBEDDING_API_KEY=<your-deepinfra-key>
EMBEDDING_BASE_URL=https://api.deepinfra.com/v1/openai
EMBEDDING_MODEL=Qwen/Qwen3-Embedding-8B
EMBEDDING_DIMENSIONS=1024

# OpenAI (optional — used for some features)
API_KEY=<your-openai-key>
```

### 4. Apply database migrations

Run the migration scripts in order against your PostgreSQL database:

```bash
psql $DATABASE_URL -f db/schema.sql
psql $DATABASE_URL -f db/migrations/20260227_add_listing_filter_columns.sql
psql $DATABASE_URL -f db/migrations/20260227_dashboard_overview_optimizations.sql
psql $DATABASE_URL -f db/migrations/20260329_add_deal_evaluations.sql
```

### 5. Start the server

```bash
python3 -m uvicorn api.main:app --reload
```

The API will be available at **http://localhost:8000**. Docs at **http://localhost:8000/docs**.

---

## Project Structure

```
backend/
├── api/                        # FastAPI application
│   ├── main.py                 # App entry point, CORS, router mounting
│   ├── deal_scoring.py         # AI deal evaluation & heuristic scoring
│   ├── listing_filters.py      # Reusable listing filter/sort builders
│   ├── auth/                   # Auth utilities
│   └── routes/
│       ├── listings.py         # GET /api/listings, filter-options, single listing
│       ├── dashboard.py        # GET /api/dashboard/overview, /api/stats
│       ├── search.py           # GET /api/search (semantic + text)
│       ├── upload.py           # POST /api/upload/single, /api/upload/csv
│       ├── chat.py             # POST /api/chat, /api/chat/stream
│       ├── deal_chat.py        # POST /api/deal-chat/stream
│       └── evaluation.py       # GET/POST /api/listings/{id}/evaluation
│
├── db/                         # Database layer
│   ├── schema.sql              # Full database schema
│   ├── connection.py           # Connection pool management
│   ├── operations.py           # Shared DB operations
│   ├── migrate_vectors.sql     # Vector migration script
│   └── migrations/
│       ├── 20260227_add_listing_filter_columns.sql
│       ├── 20260227_dashboard_overview_optimizations.sql
│       └── 20260329_add_deal_evaluations.sql
│
├── data_collection/            # Web scrapers
│   ├── scrape_bizben.py        # BizBen listing scraper
│   └── scrape_bizbuysell.py    # BizBuySell listing scraper
│
├── embeddings.py               # Embedding generation utilities
├── load_vectors.py             # Bulk vector loading script
│
├── tests/                      # Test suite
│   ├── conftest.py             # Shared fixtures
│   ├── test_listings_api.py
│   ├── test_dashboard_api.py
│   ├── test_search_api.py
│   ├── test_listing_filters.py
│   ├── test_chat_api.py
│   ├── test_deal_scoring.py
│   └── test_evaluation_api.py
│
├── requirements.txt
└── .env
```

---

## API Reference

### Health

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | Service health check |

### Listings

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/listings` | Paginated deal feed with filters & sorting |
| `GET` | `/api/listings/filter-options` | Distinct values for source, industry, state, country |
| `GET` | `/api/listings/{listing_id}` | Single listing with financials & evaluation score |

**Query Parameters for `/api/listings`:**

| Param | Type | Description |
|---|---|---|
| `page` | int | Page number (default: `1`) |
| `per_page` | int | Results per page (default: `10`, max: `100`) |
| `source` | string | Filter by source |
| `industry` | string | Filter by industry |
| `state` | string | Filter by state |
| `country` | string | Filter by country |
| `city` | string | Filter by city (legacy) |
| `min_cash_flow` / `max_cash_flow` | number | Cash flow range |
| `min_ebitda` / `max_ebitda` | number | EBITDA range |
| `min_revenue` / `max_revenue` | number | Revenue range |
| `min_price` / `max_price` | number | Asking price range |
| `sort_by` | string | Sort column (e.g., `last_seen_date`, `gross_revenue_num`) |
| `sort_order` | string | `asc` or `desc` |

### Dashboard

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/stats` | Legacy summary statistics |
| `GET` | `/api/dashboard/overview` | Full dashboard payload (KPIs, funnel, source yield, priority queue, SLA, data quality) |

**Query Parameters for `/api/dashboard/overview`:**

| Param | Default | Description |
|---|---|---|
| `lookback_days` | `90` | Analysis window |
| `priority_limit` | `12` | Max priority queue items (max: `50`) |
| `country_scope` | `US,CA` | Comma-separated country codes |

### Search

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/search` | Semantic + text search with all listing filters |

Short queries use SQL text matching; longer queries use pgvector embeddings with optional reranking.

### Upload

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/upload/single` | Upload a single listing (with duplicate detection) |
| `POST` | `/api/upload/csv` | Bulk CSV import (with per-row duplicate checks) |

### AI Evaluation

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/listings/{id}/evaluation` | Get cached evaluation or generate a new one |
| `POST` | `/api/listings/{id}/evaluation/refresh` | Force re-evaluate and overwrite cache |

Evaluations return a structured payload: `fit_score` (0–100), category breakdown (cash flow, profitability, maturity, locality, stability), pros, cons, summary, and model identifier.

### AI Chat

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/chat` | Non-streaming general chat |
| `POST` | `/api/chat/stream` | Streaming general chat (SSE) |
| `POST` | `/api/deal-chat/stream` | Streaming deal-scoped chat (SSE) — injects listing context |

---

## API Examples

```bash
# Filtered, sorted listings
curl "http://localhost:8000/api/listings?page=1&per_page=10&source=BizBen&industry=Manufacturing&state=CA&min_revenue=500000&sort_by=gross_revenue_num&sort_order=desc"

# Semantic search with filters
curl "http://localhost:8000/api/search?q=hvac%20business&limit=20&source=BizBuySell&min_cash_flow=200000"

# Dashboard overview
curl "http://localhost:8000/api/dashboard/overview?lookback_days=90&priority_limit=12&country_scope=US,CA"

# Filter options
curl "http://localhost:8000/api/listings/filter-options"
```

---

## Database

### Primary Tables

| Table | Description |
|---|---|
| `raw_listings` | Canonical store for all ingested listings |
| `deal_evaluations` | Cached AI evaluation results per listing |
| `business_entities` | Future-facing deduplicated business records |

### Key Features

- **Numeric normalization** — `parse_financial_numeric()` trigger auto-converts text financials (e.g., `"$1.2M"`) into numeric columns (`price_num`, `gross_revenue_num`, `cash_flow_num`, `ebitda_num`)
- **URL-based deduplication** — `url` is unique; repeated ingestion updates `last_seen_date`
- **Semantic duplicate detection** — pgvector distance flags near-duplicates during upload
- **pgvector embeddings** — stored for description-based semantic search

### Migrations

Apply in order:

1. `db/schema.sql` — base schema
2. `db/migrations/20260227_add_listing_filter_columns.sql` — numeric filter columns
3. `db/migrations/20260227_dashboard_overview_optimizations.sql` — dashboard read-path optimizations
4. `db/migrations/20260329_add_deal_evaluations.sql` — deal evaluations table

---

## Data Collection

Built-in scrapers for business listing marketplaces:

| Source | Script | Status |
|---|---|---|
| BizBen | `data_collection/scrape_bizben.py` | Active |
| BizBuySell | `data_collection/scrape_bizbuysell.py` | Active |

Scrapers use **Camoufox** (stealth Playwright) for anti-bot bypass and write directly into `raw_listings`.

---

## Testing

```bash
pytest
```

Tests cover listings, dashboard, search, chat, deal scoring, and evaluation APIs.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | ✅ | PostgreSQL connection string |
| `OPENAI_API_KEY` | ✅ | LLM API key (DeepInfra) |
| `OPENAI_BASE_URL` | ✅ | LLM endpoint URL |
| `OPENAI_MODEL` | ✅ | Chat/evaluation model name |
| `EMBEDDING_API_KEY` | ✅ | Embedding API key |
| `EMBEDDING_BASE_URL` | ✅ | Embedding endpoint URL |
| `EMBEDDING_MODEL` | ✅ | Embedding model name |
| `EMBEDDING_DIMENSIONS` | ✅ | Embedding vector dimensions |
| `API_KEY` | ➖ | OpenAI API key (optional, for select features) |
]]>
