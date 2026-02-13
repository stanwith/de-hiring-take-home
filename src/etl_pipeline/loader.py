"""DuckDB staging and production loading."""

import logging
from pathlib import Path

import duckdb

from .models import PageData, PageLink

logger = logging.getLogger(__name__)

BULK_CHUNK_SIZE = 1000


def _bulk_insert(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    columns: list[str],
    rows: list[tuple],
) -> None:
    """Insert rows via multi-VALUES statements, chunked to avoid parameter limits."""
    if not rows:
        return
    ncols = len(columns)
    placeholder = f"({', '.join(['?'] * ncols)})"
    cols = ", ".join(columns)
    for i in range(0, len(rows), BULK_CHUNK_SIZE):
        chunk = rows[i : i + BULK_CHUNK_SIZE]
        values = ", ".join([placeholder] * len(chunk))
        params = [val for row in chunk for val in row]
        conn.execute(
            f"INSERT INTO {table} ({cols}) SELECT * FROM (VALUES {values})", params
        )


def load_to_db(
    db_path: str,
    pages: list[PageData],
    links: list[PageLink],
) -> None:
    """
    Load validated pages and links into DuckDB.
    Creates staging tables and production views.
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)

    # Create schemas
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute("CREATE SCHEMA IF NOT EXISTS production")

    # Drop and recreate staging tables (idempotent)
    conn.execute("DROP TABLE IF EXISTS staging.pages")
    conn.execute("DROP TABLE IF EXISTS staging.page_links")

    conn.execute("""
        CREATE TABLE staging.pages (
            url VARCHAR PRIMARY KEY,
            title VARCHAR,
            summary TEXT,
            content TEXT,
            word_count INTEGER,
            depth INTEGER,
            parent_url VARCHAR,
            last_modified TIMESTAMP,
            crawled_at TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE staging.page_links (
            source_url VARCHAR,
            target_url VARCHAR,
            link_text VARCHAR
        )
    """)

    # Bulk insert pages
    if pages:
        page_rows = [
            (
                p.url, p.title, p.summary, p.content, p.word_count,
                p.depth, p.parent_url, p.last_modified, p.crawled_at,
            )
            for p in pages
        ]
        _bulk_insert(
            conn,
            "staging.pages",
            ["url", "title", "summary", "content", "word_count",
             "depth", "parent_url", "last_modified", "crawled_at"],
            page_rows,
        )
        logger.info("Loaded %d pages into staging.pages", len(pages))

    # Bulk insert links
    if links:
        link_rows = [
            (link.source_url, link.target_url, link.link_text) for link in links
        ]
        _bulk_insert(
            conn,
            "staging.page_links",
            ["source_url", "target_url", "link_text"],
            link_rows,
        )
        logger.info("Loaded %d links into staging.page_links", len(links))

    # Create production views
    conn.execute(
        "CREATE OR REPLACE VIEW production.pages AS SELECT * FROM staging.pages WHERE title IS NOT NULL AND content != '' AND length(content) > 50"
    )
    conn.execute("""
        CREATE OR REPLACE VIEW production.page_links AS
        SELECT pl.source_url, pl.target_url, pl.link_text
        FROM staging.page_links pl
        INNER JOIN production.pages p1 ON pl.source_url = p1.url
        INNER JOIN production.pages p2 ON pl.target_url = p2.url
    """)

    conn.close()
    logger.info("Created production views")
