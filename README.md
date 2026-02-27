# Tucker's Farm Backend

## Deal Feed Filtering

`GET /api/listings` supports pagination, exact text filters, numeric range filters, and safe sorting.

### Query params
- `page` (default `1`)
- `per_page` (default `10`, max `100`)
- `source`, `industry`, `state`, `country` (`city` kept for legacy compatibility)
- `min_cash_flow`, `max_cash_flow`
- `min_ebitda`, `max_ebitda`
- `min_revenue`, `max_revenue`
- `min_price`, `max_price`
- `sort_by`: `last_seen_date`, `first_seen_date`, `gross_revenue_num`, `ebitda_num`, `cash_flow_num`, `price_num`
- `sort_order`: `asc`, `desc`

`GET /api/search` accepts the same filter params (plus `q`, `limit`, `threshold`, rerank options).

`GET /api/listings/filter-options` returns distinct sorted values for:
- `source`
- `industry`
- `state`
- `country`

### Examples

```bash
curl "http://localhost:8000/api/listings?page=1&per_page=10&source=BizBen&industry=Manufacturing&state=CA&country=US&min_revenue=500000&max_revenue=5000000&sort_by=gross_revenue_num&sort_order=desc"
```

```bash
curl "http://localhost:8000/api/search?q=hvac%20business&limit=20&source=BizBuySell&min_cash_flow=200000&max_price=3000000"
```

```bash
curl "http://localhost:8000/api/listings/filter-options"
```

## Dashboard Overview API

`GET /api/dashboard/overview` returns a single aggregated payload for KPI cards, funnel, source yield, ranked priority queue, SLA, and data quality.

### Query params
- `lookback_days` (default `90`)
- `priority_limit` (default `12`, max `50`)
- `country_scope` (default `US,CA`)

### Example

```bash
curl "http://localhost:8000/api/dashboard/overview?lookback_days=90&priority_limit=12&country_scope=US,CA"
```

Notes:
- SLA values are returned as `null` until a compatible `pipeline` table exists.
- Response is cached server-side for 5 minutes per query-param combination.

## Migration

Apply the migration script before using numeric range filters/sorts:

`db/migrations/20260227_add_listing_filter_columns.sql`

For dashboard read-path optimization and numeric backfill safety, also apply:

`db/migrations/20260227_dashboard_overview_optimizations.sql`
