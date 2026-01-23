# Documentation

## Setup

Requires Python 3.13+ and `uv`.

```bash
uv sync
uv run python main.py
```

## Output

All output goes to `output/`:
- `data.db`: SQLite database
- `metadata.json`: run stats + errors
- `errors.log`: warnings/errors

## Schema

**staging** (raw data)
```sql
url TEXT PRIMARY KEY, title TEXT, content TEXT, depth INTEGER
```

**production** (cleaned)
```sql
url TEXT PRIMARY KEY, title TEXT, content TEXT, depth INTEGER, fetched_at TEXT
```

## Config

In `main.py`: `START_URL`, `MAX_DEPTH` (2), `NUM_WORKERS` (10)

## Assumptions

- **English Wikipedia only**: Only crawls `en.wikipedia.org`, no other language editions or external sites.
- **Article content only**: Extracts text from `<p>` tags within `#mw-content-text`. Ignores infoboxes, tables, and sidebars.
- **Links are valid Wikipedia articles**: Assumes `/wiki/` paths are valid articles; no verification of redirect pages.
- **Citation format is `[N]`**: Transformation assumes citations follow the `[1]`, `[2]` pattern. Other formats (e.g., `[a]`, `[citation needed]`) are not stripped.
- **Skip Wikipedia meta namespaces**: Links to `File:`, `Category:`, `Template:`, `Help:`, `Portal:`, `Special:`, `Talk:`, `User:`, and `Wikipedia:` pages are ignored. These are administrative/meta pages, not articles.
- **Ignore URL fragments and query strings**: URLs with `#` or `?` are skipped to avoid duplicate visits to the same article (e.g., `Toronto#History` vs `Toronto`).

## Design Decisions

### Level-by-level BFS with parallel workers
Instead of a single worker or fully async crawling, the pipeline uses level-by-level BFS with a thread pool:
- **Thread-safe without locks**: The `visited` set is only read during parallel fetching. Writes happen sequentially in the main thread after all workers finish a level. This avoids race conditions without needing `threading.Lock`.
- **Correct depth tracking**: Processing all URLs at depth N before depth N+1 ensures pages are assigned the correct depth value.
- **Predictable memory usage**: We know exactly how many URLs are queued for each level.
- **Trade-off**: A fully async approach could be faster but would require explicit locking for the visited set or a different data structure.

### SQLite for storage
Chose SQLite over CSV/JSON for:
- ACID guarantees
- Easy querying for verification
- No external server required

## Performance
~1,200 links/minute (depth 2, 10 workers).
