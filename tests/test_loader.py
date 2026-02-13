"""Tests for the loader module."""

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from etl_pipeline.loader import load_to_db
from etl_pipeline.models import PageData, PageLink


def test_load_to_db_creates_staging_tables():
    """Loader creates staging.pages and staging.page_links."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.duckdb"
        pages = [
            PageData(
                url="https://en.wikipedia.org/wiki/Test",
                title="Test",
                summary="Summary",
                content="Content here with enough words.",
                word_count=5,
                depth=0,
                parent_url=None,
                last_modified=None,
                crawled_at=datetime.now(timezone.utc),
            )
        ]
        links = [
            PageLink(
                source_url="https://en.wikipedia.org/wiki/Test",
                target_url="https://en.wikipedia.org/wiki/Other",
                link_text="Other",
            )
        ]
        load_to_db(str(db_path), pages, links)

        conn = duckdb.connect(str(db_path))
        rows = conn.execute("SELECT url, title FROM staging.pages").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "https://en.wikipedia.org/wiki/Test"
        assert rows[0][1] == "Test"

        link_rows = conn.execute("SELECT source_url, target_url FROM staging.page_links").fetchall()
        assert len(link_rows) == 1
        assert link_rows[0][1] == "https://en.wikipedia.org/wiki/Other"
        conn.close()


def test_load_to_db_creates_production_views():
    """Loader creates production views with quality filters."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.duckdb"
        pages = [
            PageData(
                url="https://en.wikipedia.org/wiki/Good",
                title="Good",
                summary="Summary",
                content="This is a long enough content to pass the filter of fifty characters.",
                word_count=12,
                depth=0,
                parent_url=None,
                last_modified=None,
                crawled_at=datetime.now(timezone.utc),
            ),
        ]
        load_to_db(str(db_path), pages, [])

        conn = duckdb.connect(str(db_path))
        prod = conn.execute("SELECT url, title FROM production.pages").fetchall()
        assert len(prod) == 1
        assert prod[0][1] == "Good"
        conn.close()


def test_load_to_db_filters_short_content_from_production():
    """Production view excludes pages with very short content."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.duckdb"
        pages = [
            PageData(
                url="https://en.wikipedia.org/wiki/Short",
                title="Short",
                summary="",
                content="Too short",
                word_count=2,
                depth=0,
                parent_url=None,
                last_modified=None,
                crawled_at=datetime.now(timezone.utc),
            ),
        ]
        load_to_db(str(db_path), pages, [])

        conn = duckdb.connect(str(db_path))
        staging_count = conn.execute("SELECT COUNT(*) FROM staging.pages").fetchone()[0]
        prod_count = conn.execute("SELECT COUNT(*) FROM production.pages").fetchone()[0]
        assert staging_count == 1
        assert prod_count == 0
        conn.close()


def test_load_to_db_empty_tables():
    """Loader handles empty pages and links."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.duckdb"
        load_to_db(str(db_path), [], [])

        conn = duckdb.connect(str(db_path))
        assert conn.execute("SELECT COUNT(*) FROM staging.pages").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM staging.page_links").fetchone()[0] == 0
        conn.close()
