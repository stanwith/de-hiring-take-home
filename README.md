# Hometask - ETL Pipeline

## Implementation Summary

This solution implements a Wikipedia ETL pipeline that crawls from the Toronto page, follows links to depth 2, transforms and validates the data, and loads it into a DuckDB database with staging and production layers.

### Approach

1. **Extract (crawl)**  
   The pipeline starts from the Toronto Wikipedia URL and performs an async breadth-first search (BFS) to a configurable depth (default 2). At each level it fetches pages concurrently (up to a semaphore limit), respects a per-request delay to avoid rate limiting, and uses exponential backoff on 429/5xx. URLs are normalized and deduplicated; a `visited` set prevents circular reference traversal. Only in-article links under `/wiki/` are followed; namespaces like `Wikipedia:`, `Help:`, `File:`, `Category:` are skipped.

2. **Transform**  
   Each fetched HTML page is parsed once with BeautifulSoup (lxml). We extract title, summary (first substantial paragraph), full text content (with scripts/refs/nav removed), and outbound links. Text is cleaned (citation markers removed, whitespace collapsed). Results are validated with Pydantic models; invalid pages or links are skipped and reported as errors. Transform runs during the crawl so we do not store raw HTML.

3. **Load**  
   Validated pages and links are bulk-inserted into DuckDB staging tables. Production views are defined over staging: `production.pages` filters for non-null title and content length > 50; `production.page_links` only includes links whose source and target both exist in `production.pages`.

4. **Reporting**  
   The pipeline returns a `PipelineReport` with counts (pages crawled, links loaded), duration, throughput (pages/min), and a list of errors (e.g. failed fetches, validation failures, load failures). The CLI prints this summary and exits with code 1 if any errors occurred.

### Setup Instructions

**Prerequisites:** Python 3.13+, [uv](https://docs.astral.sh/uv/)

```bash
# Clone and enter the repository
cd de-hiring-take-home

# Install dependencies (creates .venv and installs packages)
uv sync

# Optional: install dev dependencies for tests
uv sync --extra dev
```

### How to Run the Pipeline

```bash
# Run with default settings (Toronto, depth 2, 25 links/page)
uv run python -m etl_pipeline

# Or install first and run as module
uv sync && uv run python -m etl_pipeline

# Options
uv run python -m etl_pipeline --help
uv run python -m etl_pipeline --depth 1 --max-links-per-page 10 --db output.duckdb -v
```

### Data Schema

**staging.pages** — Raw crawled data after validation

| Column       | Type       | Description                          |
|-------------|------------|--------------------------------------|
| url         | VARCHAR    | Canonical Wikipedia URL (PK)        |
| title       | VARCHAR    | Page title                          |
| summary     | TEXT       | First paragraph / intro text        |
| content     | TEXT       | Full article plaintext              |
| word_count  | INTEGER    | Word count                          |
| depth       | INTEGER    | Crawl depth (0, 1, or 2)            |
| parent_url  | VARCHAR    | URL of page that linked here        |
| last_modified | TIMESTAMP | From HTTP Last-Modified header (see [Date format](#date-and-time-format)) |
| crawled_at  | TIMESTAMP  | When the page was crawled (see [Date format](#date-and-time-format))     |

**staging.page_links** — Link graph

| Column     | Type    | Description                |
|------------|---------|----------------------------|
| source_url | VARCHAR | Page containing the link   |
| target_url | VARCHAR | Linked page                |
| link_text  | VARCHAR | Anchor text                |

**production.pages** — Filtered view: `title IS NOT NULL AND content != '' AND length(content) > 50`

**production.page_links** — Only links where both source and target exist in production.pages

#### Date and time format

- **last_modified:** Parsed from the HTTP `Last-Modified` header (RFC 5322 format). Stored in DuckDB as `TIMESTAMP` in UTC. For export or APIs, use ISO 8601 (e.g. `YYYY-MM-DDTHH:MM:SS+00:00`).
- **crawled_at:** Set at crawl time in UTC. Stored as `TIMESTAMP`; same ISO 8601 recommendation for interoperability.

### Transformations

- **Text cleaning:** Strip citation markers `[1]`, `[citation needed]`; collapse whitespace
- **URL normalization:** Canonical Wikipedia URLs, fragment removal
- **Validation:** Pydantic models ensure non-null title, valid depth, etc.
- **Derived fields:** `word_count` computed from content

### Assumptions and Design Decisions

1. **DuckDB** for storage: single-file, no server, fast batch inserts, SQL views for staging/production
2. **Async BFS crawl** with httpx: concurrent fetches (semaphore), level-by-level to respect depth
3. **Link filtering:** Only follow `/wiki/` article links; skip Wikipedia:, Help:, File:, Category:, etc.
4. **max_links_per_page=25** to keep the crawl practical (~650 pages max at depth 2)
5. **Respectful crawling:** 0.5s delay between requests, custom User-Agent, exponential backoff on 429/5xx
6. **Circular references:** `visited` set prevents re-fetching same URL
7. **English Wikipedia only:** URL normalization and followable checks assume `en.wikipedia.org`; other locales would need config or code changes
8. **HTML parser:** lxml is used for parsing; it is faster and stricter than the default html.parser
9. **Single run:** The pipeline is designed for a single run (e.g. cron or one-off job); the staging tables are dropped and recreated each run
10. **Errors are non-fatal:** Failed URLs are logged and added to `PipelineReport.errors`; the run continues and still loads successful pages

### Performance Metrics Considered

#### Pages/min - Shows crawl efficiency

- **What it measures**: How fast is the pipeline discovering and processing articles
- **Use case**: "Can it crawl the entire Wikipedia category in time?"

#### Links/min - Shows data extraction throughput

- **What it measures**: Total data volume processed
- **Use case**: "How much relationship data are we extracting?"

### Docker (production / scalability)

For deployment or consistent environments, the pipeline can be run in Docker:

```bash
# Build
docker build -t etl-pipeline .

# Run with defaults (writes pipeline.duckdb in the container)
docker run --rm etl-pipeline

# Run with options and persist the database on the host
docker run --rm -v "$(pwd)/data:/data" etl-pipeline --db /data/pipeline.duckdb --depth 1 --max-links-per-page 10

# Verbose logging
docker run --rm etl-pipeline -v
```

The image uses Python 3.13 and installs dependencies with `uv`; the default command runs the pipeline once. Mount a volume to persist the DuckDB file (e.g. `-v /host/path:/data`) and pass `--db /data/pipeline.duckdb`.

### Performance

- **Throughput (pages/min) [End-to-End ETL Pipeline Run]:**
  - **Depth 1, 10 links/page:** ~183 pages/min (e.g. 11 pages in 3.6s).
  - **Depth 2, 25 links/page:** ~329 pages/min (e.g. 504 pages in 92s).
- **Bottlenecks:** Network latency and rate limiting; the 0.5s delay is the main limiter. Additionally, if scaled, memory could be a bottleneck (Point 4 discusses this in detail.)
- **Optimization opportunities:** Reduce `request_delay` for faster runs (at the cost of being less respectful and based on the limits from the provider); increase `concurrency` (CPU bound);

#### Additional bottlenecks and optimizations

| Area | Bottleneck / observation | Possible optimization |
|------|---------------------------|------------------------|
| **Extract** | Request delay is per-task inside the semaphore; bursts of N requests then delay. | Use a global rate limiter (e.g. token bucket) to cap requests/sec across all workers for smoother rate limiting. |
| **Extract** | HTTP/1.1 only; one request per connection at a time. | Enable HTTP/2 on `httpx.AsyncClient(http2=True)` so multiple streams share one connection to en.wikipedia.org and reduce connection overhead. |
| **Extract** | HTML parsing (BeautifulSoup + lxml) runs in the async task and is CPU-bound, so it can block the event loop and delay other fetches under high concurrency. | Run `parse_page(...)` in a thread pool via `asyncio.run_in_executor()` so the event loop stays responsive and I/O and CPU overlap better. |
| **Extract** | Full response body (`resp.text`) is loaded into memory per page. | For very large pages or huge crawls, consider streaming or a cap on response size; for typical Wikipedia articles this is fine. |
| **Transform** | `div#mw-content-text` is located multiple times (title, summary, links, content). | Resolve the content node once and pass it into the extractors to avoid repeated selector work. |
| **Load** | All pages and links are held in memory until the load phase; at depth 2 with 25 links/page this can be hundreds of thousands of link rows. | For scale, stream results into the loader (e.g. a queue plus a writer coroutine/thread that inserts in chunks) to bound memory. |
| **Load** | Inserts use chunked parameterized VALUES. | For very large link tables, DuckDB’s `INSERT ... FROM SELECT` over a PyArrow table or `COPY` from a temp file can be faster than many VALUES rounds. |
| **End-to-end** | No per-stage timings in `PipelineReport`. | Add `crawl_duration_seconds` and `load_duration_seconds` to the report (and optionally log them) to see where time is spent. |
