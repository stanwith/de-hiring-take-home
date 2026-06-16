from __future__ import annotations

import logging
import os
import sqlite3
from typing import Optional

import pandas as pd

from .models import PipelineRun, StagingLink, StagingPage

logger = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id           TEXT NOT NULL UNIQUE,
    start_time       TEXT NOT NULL,
    end_time         TEXT,
    status           TEXT DEFAULT 'running'
                     CHECK(status IN ('running', 'completed', 'failed')),
    pages_extracted  INTEGER DEFAULT 0,
    pages_valid      INTEGER DEFAULT 0,
    pages_invalid    INTEGER DEFAULT 0,
    error_message    TEXT
);

CREATE TABLE IF NOT EXISTS staging_pages (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    scrape_run_id       TEXT NOT NULL,
    page_title          TEXT,
    url                 TEXT,
    depth_level         INTEGER,
    scrape_timestamp    TEXT,
    word_count          INTEGER,
    last_modified       TEXT,
    links_json          TEXT,
    http_status         INTEGER,
    validation_status   TEXT DEFAULT 'pending'
                        CHECK(validation_status IN ('pending', 'valid', 'invalid')),
    validation_errors   TEXT,
    created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scrape_run_id, url)
);

CREATE TABLE IF NOT EXISTS staging_links (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    scrape_run_id  TEXT NOT NULL,
    source_url     TEXT NOT NULL,
    source_title   TEXT,
    target_title   TEXT NOT NULL,
    link_order     INTEGER NOT NULL DEFAULT 0,
    created_at     TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scrape_run_id, source_url, target_title)
);

CREATE TABLE IF NOT EXISTS production_pages (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    scrape_run_id    TEXT NOT NULL,
    page_title       TEXT NOT NULL,
    url              TEXT NOT NULL,
    depth_level      INTEGER NOT NULL CHECK(depth_level IN (0, 1, 2)),
    scrape_timestamp TEXT NOT NULL,
    word_count       INTEGER CHECK(word_count IS NULL OR word_count >= 0),
    last_modified    TEXT,
    promoted_at      TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scrape_run_id, url)
);

CREATE TABLE IF NOT EXISTS production_links (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    scrape_run_id  TEXT NOT NULL,
    source_url     TEXT NOT NULL,
    target_title   TEXT NOT NULL,
    link_order     INTEGER NOT NULL DEFAULT 0,
    UNIQUE(scrape_run_id, source_url, target_title)
);

CREATE VIEW IF NOT EXISTS production_page_link_summary AS
SELECT
    pp.page_title,
    pp.url,
    pp.depth_level,
    pp.word_count,
    pp.last_modified,
    pp.scrape_timestamp,
    pp.scrape_run_id,
    COUNT(pl.target_title) AS outbound_link_count,
    pp.promoted_at
FROM production_pages pp
LEFT JOIN production_links pl
    ON pp.url = pl.source_url
   AND pp.scrape_run_id = pl.scrape_run_id
GROUP BY pp.scrape_run_id, pp.url;

CREATE INDEX IF NOT EXISTS idx_staging_pages_run_status
    ON staging_pages(scrape_run_id, validation_status);

CREATE INDEX IF NOT EXISTS idx_staging_links_source
    ON staging_links(scrape_run_id, source_url);

CREATE INDEX IF NOT EXISTS idx_production_pages_depth
    ON production_pages(scrape_run_id, depth_level);

CREATE INDEX IF NOT EXISTS idx_production_links_source
    ON production_links(scrape_run_id, source_url);
"""


class DatabaseLoader:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def __enter__(self) -> "DatabaseLoader":
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        return self

    def __exit__(self, *args) -> None:
        if self.conn:
            self.conn.commit()
            self.conn.close()
            self.conn = None

    def _conn(self) -> sqlite3.Connection:
        if self.conn is None:
            raise RuntimeError("DatabaseLoader must be used as a context manager")
        return self.conn

    def initialize_schema(self) -> None:
        conn = self._conn()
        for statement in _DDL.strip().split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(stmt)
        conn.commit()
        logger.debug("Schema initialized")

    def insert_staging_pages(self, pages: list[StagingPage]) -> int:
        if not pages:
            return 0
        conn = self._conn()
        rows = [
            (
                p.scrape_run_id,
                p.page_title,
                p.url,
                p.depth_level,
                p.scrape_timestamp,
                p.word_count,
                p.last_modified,
                p.links_json,
                p.http_status,
                p.validation_status,
                p.validation_errors,
            )
            for p in pages
        ]
        conn.executemany(
            """
            INSERT OR IGNORE INTO staging_pages
            (scrape_run_id, page_title, url, depth_level, scrape_timestamp,
             word_count, last_modified, links_json, http_status,
             validation_status, validation_errors)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        inserted = conn.execute(
            "SELECT changes()"
        ).fetchone()[0]
        logger.debug("Inserted %d staging pages", len(pages))
        return len(pages)

    def insert_staging_links(self, links: list[StagingLink]) -> int:
        if not links:
            return 0
        conn = self._conn()
        rows = [
            (lnk.scrape_run_id, lnk.source_url, lnk.source_title, lnk.target_title, lnk.link_order)
            for lnk in links
        ]
        conn.executemany(
            """
            INSERT OR IGNORE INTO staging_links
            (scrape_run_id, source_url, source_title, target_title, link_order)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        logger.debug("Inserted staging links for %d links", len(links))
        return len(links)

    def update_validation_status(
        self,
        valid_ids: list[int],
        invalid_ids: list[int],
        errors_by_id: dict[int, str],
    ) -> None:
        conn = self._conn()
        if valid_ids:
            conn.executemany(
                "UPDATE staging_pages SET validation_status='valid', validation_errors=NULL WHERE id=?",
                [(i,) for i in valid_ids],
            )
        if invalid_ids:
            conn.executemany(
                "UPDATE staging_pages SET validation_status='invalid', validation_errors=? WHERE id=?",
                [(errors_by_id.get(i), i) for i in invalid_ids],
            )
        conn.commit()
        logger.debug(
            "Updated validation: %d valid, %d invalid", len(valid_ids), len(invalid_ids)
        )

    def promote_to_production(self, run_id: str) -> tuple[int, int]:
        conn = self._conn()
        conn.execute(
            """
            INSERT INTO production_pages
                (scrape_run_id, page_title, url, depth_level,
                 scrape_timestamp, word_count, last_modified)
            SELECT
                scrape_run_id, page_title, url, depth_level,
                scrape_timestamp, word_count, last_modified
            FROM staging_pages
            WHERE validation_status = 'valid'
              AND scrape_run_id = ?
            ON CONFLICT(scrape_run_id, url) DO NOTHING
            """,
            (run_id,),
        )
        pages_promoted = conn.execute("SELECT changes()").fetchone()[0]

        conn.execute(
            """
            INSERT INTO production_links
                (scrape_run_id, source_url, target_title, link_order)
            SELECT
                sl.scrape_run_id, sl.source_url, sl.target_title, sl.link_order
            FROM staging_links sl
            INNER JOIN production_pages pp
                ON sl.source_url = pp.url
               AND sl.scrape_run_id = pp.scrape_run_id
            WHERE sl.scrape_run_id = ?
            ON CONFLICT(scrape_run_id, source_url, target_title) DO NOTHING
            """,
            (run_id,),
        )
        links_promoted = conn.execute("SELECT changes()").fetchone()[0]
        conn.commit()
        logger.debug(
            "Promoted %d pages and %d links to production", pages_promoted, links_promoted
        )
        return pages_promoted, links_promoted

    def upsert_pipeline_run(self, run: PipelineRun) -> None:
        conn = self._conn()
        conn.execute(
            """
            INSERT OR REPLACE INTO pipeline_runs
            (run_id, start_time, end_time, status,
             pages_extracted, pages_valid, pages_invalid, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            run.to_db_tuple(),
        )
        conn.commit()

    def get_staging_pages_df(self, run_id: str) -> pd.DataFrame:
        return pd.read_sql_query(
            "SELECT * FROM staging_pages WHERE scrape_run_id = ?",
            self._conn(),
            params=(run_id,),
        )

    def get_staging_links_df(self, run_id: str) -> pd.DataFrame:
        return pd.read_sql_query(
            "SELECT * FROM staging_links WHERE scrape_run_id = ?",
            self._conn(),
            params=(run_id,),
        )

    def get_summary(self, run_id: str) -> dict:
        conn = self._conn()
        staging_count = conn.execute(
            "SELECT COUNT(*) FROM staging_pages WHERE scrape_run_id=?", (run_id,)
        ).fetchone()[0]
        valid_count = conn.execute(
            "SELECT COUNT(*) FROM staging_pages WHERE scrape_run_id=? AND validation_status='valid'",
            (run_id,),
        ).fetchone()[0]
        invalid_count = conn.execute(
            "SELECT COUNT(*) FROM staging_pages WHERE scrape_run_id=? AND validation_status='invalid'",
            (run_id,),
        ).fetchone()[0]
        prod_pages = conn.execute(
            "SELECT COUNT(*) FROM production_pages WHERE scrape_run_id=?", (run_id,)
        ).fetchone()[0]
        prod_links = conn.execute(
            "SELECT COUNT(*) FROM production_links WHERE scrape_run_id=?", (run_id,)
        ).fetchone()[0]
        return {
            "staging_pages": staging_count,
            "valid_pages": valid_count,
            "invalid_pages": invalid_count,
            "production_pages": prod_pages,
            "production_links": prod_links,
        }
