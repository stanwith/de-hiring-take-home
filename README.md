# Wikipedia ETL Pipeline

A data integration pipeline that crawls the [Wikipedia Toronto page](https://en.wikipedia.org/wiki/Toronto), follows internal links to depth 2, transforms and validates the data, then loads it into a SQLite database with separate staging and production tables.

## Setup

**Requirements**: Python 3.13+, [uv](https://docs.astral.sh/uv/getting-started/installation/)

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install dependencies
git clone <repo-url>
cd de-hiring-take-home
uv sync
```

## Running the Pipeline

**Default run** (Toronto page, depth 2, 1 req/s, 50 depth-1 links, 10 depth-2 links per page):

```bash
uv run wikipedia-etl
```

**Custom run** with all available options:

```bash
uv run wikipedia-etl \
  --max-links-depth1 50 \    # links to follow from root page (default: 50)
  --max-links-depth2 10 \    # links to follow per depth-1 page (default: 10)
  --rate-limit 1.0 \         # requests/second, respect Wikipedia limits (default: 1.0)
  --concurrent 3 \           # max concurrent requests (default: 3)
  --db-path data/wikipedia.db \
  --log-level INFO
```

**Run tests:**

```bash
uv run pytest tests/ -v --cov=src/wikipedia_etl
```

**Query results:**

```bash
# Pages by depth level
sqlite3 data/wikipedia.db "SELECT depth_level, COUNT(*) FROM production_pages GROUP BY depth_level;"

# Production view with link counts
sqlite3 data/wikipedia.db "SELECT page_title, depth_level, word_count, outbound_link_count FROM production_page_link_summary LIMIT 10;"

# Pipeline run history
sqlite3 data/wikipedia.db "SELECT run_id, status, pages_valid, pages_invalid FROM pipeline_runs;"
```

## Data Schema

All data is stored in `data/wikipedia.db` (SQLite). Each pipeline run is identified by a unique `scrape_run_id` (UUID).

### `staging_pages`
Raw extracted data — all columns nullable, with per-row validation tracking.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER | Auto-increment primary key |
| `scrape_run_id` | TEXT | UUID identifying the pipeline run |
| `page_title` | TEXT | Wikipedia article title |
| `url` | TEXT | Canonical Wikipedia URL |
| `depth_level` | INTEGER | Crawl depth (0=root, 1=linked from root, 2=linked from depth-1) |
| `scrape_timestamp` | TEXT | UTC timestamp when page was fetched (ISO 8601) |
| `word_count` | INTEGER | Word count from plain-text article extract |
| `last_modified` | TEXT | Article's last revision timestamp (ISO 8601) |
| `links_json` | TEXT | JSON array of linked article titles |
| `http_status` | INTEGER | HTTP response code |
| `validation_status` | TEXT | `pending` → `valid` or `invalid` after transformation |
| `validation_errors` | TEXT | JSON array of validation error messages (null if valid) |

### `staging_links`
Raw link graph between pages.

| Column | Type | Description |
|---|---|---|
| `scrape_run_id` | TEXT | Pipeline run identifier |
| `source_url` | TEXT | URL of the page containing the link |
| `source_title` | TEXT | Title of the source page |
| `target_title` | TEXT | Title of the linked Wikipedia article |
| `link_order` | INTEGER | Position of the link on the source page |

### `production_pages`
Clean, validated pages. Has CHECK constraints enforcing data quality.

| Column | Type | Constraints |
|---|---|---|
| `scrape_run_id` | TEXT | NOT NULL |
| `page_title` | TEXT | NOT NULL |
| `url` | TEXT | NOT NULL |
| `depth_level` | INTEGER | NOT NULL, CHECK IN (0, 1, 2) |
| `scrape_timestamp` | TEXT | NOT NULL |
| `word_count` | INTEGER | CHECK >= 0 (nullable) |
| `last_modified` | TEXT | nullable |
| `promoted_at` | TEXT | Timestamp when row was promoted from staging |

### `production_links`
Clean link graph — only links whose source page is in `production_pages`.

| Column | Type | Description |
|---|---|---|
| `scrape_run_id` | TEXT | Pipeline run identifier |
| `source_url` | TEXT | Source page URL |
| `target_title` | TEXT | Target article title |
| `link_order` | INTEGER | Link position on source page |

### `production_page_link_summary` (VIEW)
Joins `production_pages` with an aggregate of `production_links`. Useful for analytics.

| Column | Description |
|---|---|
| `page_title` | Article title |
| `url` | Canonical URL |
| `depth_level` | Crawl depth (0/1/2) |
| `word_count` | Word count |
| `last_modified` | Last edit timestamp |
| `scrape_timestamp` | When crawled |
| `outbound_link_count` | Number of Wikipedia links found on the page |

### `pipeline_runs`
Audit log of every pipeline execution.

| Column | Description |
|---|---|
| `run_id` | UUID |
| `start_time` | Run start (ISO 8601 UTC) |
| `end_time` | Run end |
| `status` | `running` / `completed` / `failed` |
| `pages_extracted` | Total pages staged |
| `pages_valid` | Pages promoted to production |
| `pages_invalid` | Pages that failed validation |

## Transformations Applied

The transformation phase validates each staged page and classifies it as `valid` or `invalid`:

| Check | Pass condition | Failure action |
|---|---|---|
| Required fields | `page_title`, `url`, `depth_level`, `scrape_timestamp`, `scrape_run_id` must be non-null | Mark invalid |
| URL format | Must match `https://en.wikipedia.org/wiki/<article>` | Mark invalid |
| Depth range | Must be 0, 1, or 2 | Mark invalid |
| Word count | Must be ≥ 0 if present | Mark invalid |
| `scrape_timestamp` | Must be parseable as ISO 8601 datetime | Mark invalid |
| `last_modified` | Parsed with `pd.to_datetime(..., errors='coerce')` | Set to null (tolerated) |

For links: orphaned links (source page is invalid) and duplicate `(source_url, target_title)` pairs are dropped before promotion.

## Assumptions and Design Decisions

**Wikipedia MediaWiki API over HTML scraping**: One API call (`action=query&prop=info|extracts|revisions|links|pageprops`) returns the canonical URL, plain-text content, last-modified timestamp, outbound links, and disambiguation flag per page. This is cleaner and more reliable than HTML parsing.

**Link scope**: Only Wikipedia article links (`plnamespace=0`) are followed. External links, file links, and category links are excluded since they don't represent Wikipedia article relationships.

**Disambiguation pages**: Pages flagged as disambiguation by the MediaWiki API are fetched and stored, but their links are not followed — following disambiguation links would fan out to unrelated topic areas.

**Configurable crawl limits**: The Toronto article contains ~1,500+ internal links. Following all of them to depth 2 would be tens of thousands of pages. The defaults (50 at depth 1, 10 per depth-2 page) give a ~550-page crawl in ~9 minutes at 1 req/s — enough to demonstrate the pipeline at a reasonable scale.

**Staging → Production separation**: All extracted data lands in staging first, regardless of quality. The transformer then validates and annotates each row. Only rows that pass all validation checks are promoted to the production tables. This makes the pipeline re-runnable without data loss: failed rows stay in staging with error messages for debugging.

**SQLite**: Chosen for zero-setup portability. WAL mode is enabled for better write concurrency. `ON CONFLICT DO NOTHING` makes the promotion step idempotent.

**Rate limiting**: Wikipedia's API usage guidelines recommend a maximum of 1 req/s per User-Agent for automated crawlers. The `RateLimiter` (token-bucket via `asyncio.Lock`) enforces this. The `--rate-limit` flag allows increasing this for research use.

## Performance Characteristics

| Rate (req/s) | Concurrent | ~Pages | ~Time | Throughput |
|---|---|---|---|---|
| 1.0 (default, safe) | 3 | ~551 | ~9 min | ~60 pages/min |
| 2.0 | 5 | ~551 | ~4.5 min | ~120 pages/min |
| 5.0 (aggressive) | 10 | ~551 | ~1.8 min | ~300 pages/min |

**Bottleneck**: The rate limiter, not the network. Wikipedia's API responds in ~100–300ms; the 1s token gap dominates.

**Optimization path**: The MediaWiki API supports batch title queries (`titles=A|B|C...`, up to 50 titles per request). Implementing this would yield ~50x throughput at the same rate limit — fetching 50 pages per request instead of 1. Not implemented here to keep the code straightforward, but the architecture supports adding it to `_fetch_page_api`.
