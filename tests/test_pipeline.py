from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from pydantic import ValidationError

from wikipedia_etl.config import PipelineConfig
from wikipedia_etl.extractor import RateLimiter, WikipediaExtractor
from wikipedia_etl.loader import DatabaseLoader
from wikipedia_etl.models import (
    PipelineRun,
    RawPageData,
    StagingLink,
    StagingPage,
    ValidationStatus,
)
from wikipedia_etl.transformer import transform_links, transform_pages

# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def now_utc() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def valid_raw(now_utc) -> RawPageData:
    return RawPageData(
        scrape_run_id="run-001",
        page_title="Toronto",
        url="https://en.wikipedia.org/wiki/Toronto",
        depth_level=0,
        scrape_timestamp=now_utc,
        word_count=5000,
        last_modified=now_utc,
        links_found=["Ontario", "Canada"],
        http_status=200,
        is_disambiguation=False,
    )


@pytest.fixture
def db_loader(tmp_path) -> DatabaseLoader:
    loader = DatabaseLoader(str(tmp_path / "test.db"))
    with loader:
        loader.initialize_schema()
        yield loader


@pytest.fixture
def default_config() -> PipelineConfig:
    return PipelineConfig(
        max_links_depth1=5,
        max_links_depth2=2,
        rate_limit_rps=100.0,
        n_concurrent=2,
        db_path="data/test.db",
    )


# ── TestModels ───────────────────────────────────────────────────────────────


class TestModels:
    def test_raw_page_data_valid(self, valid_raw):
        assert valid_raw.page_title == "Toronto"
        assert valid_raw.depth_level == 0
        assert valid_raw.word_count == 5000

    def test_raw_page_data_invalid_depth(self, now_utc):
        with pytest.raises(ValidationError, match="depth_level"):
            RawPageData(
                scrape_run_id="r",
                page_title="X",
                url="https://en.wikipedia.org/wiki/X",
                depth_level=3,
                scrape_timestamp=now_utc,
            )

    def test_raw_page_data_invalid_url(self, now_utc):
        with pytest.raises(ValidationError, match="url"):
            RawPageData(
                scrape_run_id="r",
                page_title="X",
                url="https://example.com/wiki/X",
                depth_level=0,
                scrape_timestamp=now_utc,
            )

    def test_raw_page_data_negative_word_count(self, now_utc):
        with pytest.raises(ValidationError, match="word_count"):
            RawPageData(
                scrape_run_id="r",
                page_title="X",
                url="https://en.wikipedia.org/wiki/X",
                depth_level=0,
                scrape_timestamp=now_utc,
                word_count=-1,
            )

    def test_staging_page_from_raw(self, valid_raw):
        sp = StagingPage.from_raw(valid_raw)
        assert sp.page_title == "Toronto"
        assert sp.scrape_run_id == "run-001"
        links = json.loads(sp.links_json)
        assert links == ["Ontario", "Canada"]
        assert sp.scrape_timestamp is not None

    def test_production_page_null_word_count_allowed(self, now_utc):
        from wikipedia_etl.models import ProductionPage

        pp = ProductionPage(
            scrape_run_id="r",
            page_title="X",
            url="https://en.wikipedia.org/wiki/X",
            depth_level=1,
            scrape_timestamp=now_utc,
            word_count=None,
        )
        assert pp.word_count is None

    def test_validation_status_enum_values(self):
        assert ValidationStatus.PENDING == "pending"
        assert ValidationStatus.VALID == "valid"
        assert ValidationStatus.INVALID == "invalid"


# ── TestTransformer ──────────────────────────────────────────────────────────


def _make_pages_df(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "id": 1,
        "scrape_run_id": "run-001",
        "page_title": "Toronto",
        "url": "https://en.wikipedia.org/wiki/Toronto",
        "depth_level": 0,
        "scrape_timestamp": "2024-06-01T12:00:00+00:00",
        "word_count": 1000,
        "last_modified": "2024-05-01T00:00:00+00:00",
    }
    records = [{**defaults, **r} for r in rows]
    return pd.DataFrame(records)


class TestTransformer:
    def test_transform_null_title_marked_invalid(self):
        df = _make_pages_df([{"id": 1, "page_title": None}])
        valid, invalid = transform_pages(df)
        assert len(valid) == 0
        assert len(invalid) == 1
        errs = json.loads(invalid.iloc[0]["validation_errors"])
        assert any("page_title" in e for e in errs)

    def test_transform_invalid_url_format(self):
        df = _make_pages_df([{"id": 1, "url": "https://example.com/page"}])
        valid, invalid = transform_pages(df)
        assert len(invalid) == 1
        errs = json.loads(invalid.iloc[0]["validation_errors"])
        assert any("url" in e for e in errs)

    def test_transform_invalid_depth(self):
        df = _make_pages_df([{"id": 1, "depth_level": 5}])
        valid, invalid = transform_pages(df)
        assert len(invalid) == 1

    def test_transform_invalid_timestamp_unparseable(self):
        df = _make_pages_df([{"id": 1, "scrape_timestamp": "NOT-A-DATE"}])
        valid, invalid = transform_pages(df)
        assert len(invalid) == 1
        errs = json.loads(invalid.iloc[0]["validation_errors"])
        assert any("scrape_timestamp" in e for e in errs)

    def test_transform_bad_last_modified_tolerated(self):
        df = _make_pages_df([{"id": 1, "last_modified": "BADDATE"}])
        valid, invalid = transform_pages(df)
        assert len(valid) == 1
        assert len(invalid) == 0

    def test_transform_negative_word_count_invalid(self):
        df = _make_pages_df([{"id": 1, "word_count": -5}])
        valid, invalid = transform_pages(df)
        assert len(invalid) == 1

    def test_transform_valid_row_passes(self):
        df = _make_pages_df([{"id": 1}])
        valid, invalid = transform_pages(df)
        assert len(valid) == 1
        assert len(invalid) == 0

    def test_transform_links_drops_orphans(self):
        df = pd.DataFrame([
            {"scrape_run_id": "r", "source_url": "https://en.wikipedia.org/wiki/Toronto", "source_title": "Toronto", "target_title": "Ontario", "link_order": 0},
            {"scrape_run_id": "r", "source_url": "https://en.wikipedia.org/wiki/NotValid", "source_title": "NotValid", "target_title": "Canada", "link_order": 0},
        ])
        valid_urls = {"https://en.wikipedia.org/wiki/Toronto"}
        result = transform_links(df, valid_urls)
        assert len(result) == 1
        assert result.iloc[0]["target_title"] == "Ontario"

    def test_transform_links_deduplicates(self):
        df = pd.DataFrame([
            {"scrape_run_id": "r", "source_url": "https://en.wikipedia.org/wiki/Toronto", "source_title": "Toronto", "target_title": "Ontario", "link_order": 0},
            {"scrape_run_id": "r", "source_url": "https://en.wikipedia.org/wiki/Toronto", "source_title": "Toronto", "target_title": "Ontario", "link_order": 1},
        ])
        valid_urls = {"https://en.wikipedia.org/wiki/Toronto"}
        result = transform_links(df, valid_urls)
        assert len(result) == 1

    def test_transform_empty_df(self):
        df = pd.DataFrame()
        valid, invalid = transform_pages(df)
        assert valid.empty
        assert invalid.empty


# ── TestLoader ───────────────────────────────────────────────────────────────


class TestLoader:
    def test_initialize_schema_creates_tables(self, tmp_path):
        loader = DatabaseLoader(str(tmp_path / "test.db"))
        with loader:
            loader.initialize_schema()
            conn = loader._conn()
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
                ).fetchall()
            }
        expected = {
            "pipeline_runs",
            "staging_pages",
            "staging_links",
            "production_pages",
            "production_links",
            "production_page_link_summary",
        }
        assert expected.issubset(tables)

    def test_insert_staging_pages_single(self, db_loader, valid_raw, now_utc):
        sp = StagingPage.from_raw(valid_raw)
        count = db_loader.insert_staging_pages([sp])
        assert count == 1
        row = db_loader._conn().execute("SELECT COUNT(*) FROM staging_pages").fetchone()[0]
        assert row == 1

    def test_insert_staging_pages_duplicate_ignored(self, db_loader, valid_raw):
        sp = StagingPage.from_raw(valid_raw)
        db_loader.insert_staging_pages([sp])
        db_loader.insert_staging_pages([sp])
        row = db_loader._conn().execute("SELECT COUNT(*) FROM staging_pages").fetchone()[0]
        assert row == 1

    def test_update_validation_status(self, db_loader, valid_raw, now_utc):
        sp = StagingPage.from_raw(valid_raw)
        db_loader.insert_staging_pages([sp])
        row_id = db_loader._conn().execute("SELECT id FROM staging_pages").fetchone()[0]
        db_loader.update_validation_status(
            valid_ids=[row_id], invalid_ids=[], errors_by_id={}
        )
        status = db_loader._conn().execute(
            "SELECT validation_status FROM staging_pages WHERE id=?", (row_id,)
        ).fetchone()[0]
        assert status == "valid"

    def test_promote_to_production_valid_only(self, db_loader, now_utc):
        pages = [
            StagingPage(
                scrape_run_id="run-1",
                page_title="Toronto",
                url="https://en.wikipedia.org/wiki/Toronto",
                depth_level=0,
                scrape_timestamp=now_utc.isoformat(),
                word_count=100,
                validation_status=ValidationStatus.VALID,
            ),
            StagingPage(
                scrape_run_id="run-1",
                page_title="Bad Page",
                url="https://en.wikipedia.org/wiki/Bad_Page",
                depth_level=1,
                scrape_timestamp=now_utc.isoformat(),
                validation_status=ValidationStatus.INVALID,
                validation_errors=json.dumps(["missing data"]),
            ),
        ]
        db_loader.insert_staging_pages(pages)
        pages_count, _ = db_loader.promote_to_production("run-1")
        assert pages_count == 1
        prod = db_loader._conn().execute(
            "SELECT page_title FROM production_pages"
        ).fetchall()
        assert len(prod) == 1
        assert prod[0][0] == "Toronto"

    def test_promote_idempotent(self, db_loader, now_utc):
        sp = StagingPage(
            scrape_run_id="run-1",
            page_title="Toronto",
            url="https://en.wikipedia.org/wiki/Toronto",
            depth_level=0,
            scrape_timestamp=now_utc.isoformat(),
            word_count=100,
            validation_status=ValidationStatus.VALID,
        )
        db_loader.insert_staging_pages([sp])
        db_loader.promote_to_production("run-1")
        db_loader.promote_to_production("run-1")
        count = db_loader._conn().execute(
            "SELECT COUNT(*) FROM production_pages"
        ).fetchone()[0]
        assert count == 1

    def test_promote_links_follow_valid_pages(self, db_loader, now_utc):
        pages = [
            StagingPage(
                scrape_run_id="run-1",
                page_title="Toronto",
                url="https://en.wikipedia.org/wiki/Toronto",
                depth_level=0,
                scrape_timestamp=now_utc.isoformat(),
                validation_status=ValidationStatus.VALID,
            ),
            StagingPage(
                scrape_run_id="run-1",
                page_title="InvalidPage",
                url="https://en.wikipedia.org/wiki/InvalidPage",
                depth_level=1,
                scrape_timestamp=now_utc.isoformat(),
                validation_status=ValidationStatus.INVALID,
                validation_errors=json.dumps(["err"]),
            ),
        ]
        db_loader.insert_staging_pages(pages)
        links = [
            StagingLink(
                scrape_run_id="run-1",
                source_url="https://en.wikipedia.org/wiki/Toronto",
                target_title="Ontario",
                link_order=0,
            ),
            StagingLink(
                scrape_run_id="run-1",
                source_url="https://en.wikipedia.org/wiki/InvalidPage",
                target_title="Canada",
                link_order=0,
            ),
        ]
        db_loader.insert_staging_links(links)
        db_loader.promote_to_production("run-1")
        prod_links = db_loader._conn().execute(
            "SELECT target_title FROM production_links"
        ).fetchall()
        assert len(prod_links) == 1
        assert prod_links[0][0] == "Ontario"


# ── TestExtractor ────────────────────────────────────────────────────────────


def _make_api_response(
    title: str = "Toronto",
    pageid: int = 12345,
    extract: str = "Toronto is a city.",
    last_modified: str = "2024-01-01T00:00:00Z",
    links: Optional[list[str]] = None,
    is_disambiguation: bool = False,
    canonical_url: str = "https://en.wikipedia.org/wiki/Toronto",
) -> dict:
    page = {
        "pageid": pageid,
        "title": title,
        "canonicalurl": canonical_url,
        "extract": extract,
        "revisions": [{"timestamp": last_modified}],
        "links": [{"title": lnk} for lnk in (links or [])],
    }
    if is_disambiguation:
        page["pageprops"] = {"disambiguation": ""}
    return {"query": {"pages": [page]}}


class TestExtractor:
    def test_parse_api_response_valid(self, default_config):
        extractor = WikipediaExtractor(default_config, "run-1")
        api_json = _make_api_response(links=["Ontario", "Canada"])
        result = extractor._parse_api_response(api_json, depth=0, title_queried="Toronto")
        assert result is not None
        assert result.page_title == "Toronto"
        assert result.depth_level == 0
        assert "Ontario" in result.links_found
        assert result.is_disambiguation is False
        assert result.word_count == 4

    def test_parse_api_response_missing_page(self, default_config):
        extractor = WikipediaExtractor(default_config, "run-1")
        api_json = {"query": {"pages": [{"missing": True, "pageid": -1, "title": "NoPage"}]}}
        result = extractor._parse_api_response(api_json, depth=1, title_queried="NoPage")
        assert result is None

    def test_parse_api_response_no_pages(self, default_config):
        extractor = WikipediaExtractor(default_config, "run-1")
        result = extractor._parse_api_response({"query": {"pages": []}}, 0, "X")
        assert result is None

    def test_parse_api_response_disambiguation(self, default_config):
        extractor = WikipediaExtractor(default_config, "run-1")
        api_json = _make_api_response(
            title="Toronto (disambiguation)",
            is_disambiguation=True,
            links=["Toronto, Ontario"],
        )
        result = extractor._parse_api_response(api_json, depth=1, title_queried="Toronto (disambiguation)")
        assert result is not None
        assert result.is_disambiguation is True
        assert len(result.links_found) == 1

    @pytest.mark.asyncio
    async def test_rate_limiter_spacing(self):
        import time
        limiter = RateLimiter(rate=10.0)
        start = time.monotonic()
        for _ in range(3):
            await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed >= 0.18

    @pytest.mark.asyncio
    async def test_crawl_deduplicates_pages(self, default_config):
        toronto_response = _make_api_response(links=["Ontario", "Ontario"])
        ontario_response = _make_api_response(
            title="Ontario",
            pageid=2,
            canonical_url="https://en.wikipedia.org/wiki/Ontario",
            links=[],
        )

        call_count = {"n": 0}

        async def mock_fetch_api(title, pllimit):
            call_count["n"] += 1
            if title == "Toronto":
                return toronto_response
            return ontario_response

        async with WikipediaExtractor(default_config, "run-1") as extractor:
            extractor._fetch_page_api = mock_fetch_api
            results = await extractor.crawl()

        titles = [r.page_title for r in results]
        assert titles.count("Ontario") <= 1

    @pytest.mark.asyncio
    async def test_crawl_respects_max_depth(self, default_config):
        default_config.max_depth = 1
        depth2_called = {"called": False}

        async def mock_fetch_api(title, pllimit):
            return _make_api_response(
                title=title,
                pageid=hash(title) % 100000,
                canonical_url=f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
                links=["PageA", "PageB"],
            )

        fetched_depths: list[int] = []

        async def mock_fetch_page(title, depth, pllimit):
            fetched_depths.append(depth)
            api_json = await mock_fetch_api(title, pllimit)
            return extractor._parse_api_response(api_json, depth, title)

        async with WikipediaExtractor(default_config, "run-1") as extractor:
            extractor._fetch_page = mock_fetch_page
            results = await extractor.crawl()

        assert max(fetched_depths) <= 1

    @pytest.mark.asyncio
    async def test_crawl_handles_none_results(self, default_config):
        default_config.max_depth = 1
        default_config.max_links_depth1 = 2

        call_count = {"n": 0}

        async def mock_fetch_api(title, pllimit):
            call_count["n"] += 1
            if title == "Toronto":
                return _make_api_response(links=["PageGood", "PageBad"])
            if title == "PageGood":
                return _make_api_response(
                    title="PageGood",
                    pageid=2,
                    canonical_url="https://en.wikipedia.org/wiki/PageGood",
                    links=[],
                )
            return {"query": {"pages": [{"missing": True, "pageid": -1, "title": title}]}}

        async with WikipediaExtractor(default_config, "run-1") as extractor:
            extractor._fetch_page_api = mock_fetch_api
            results = await extractor.crawl()

        titles = [r.page_title for r in results]
        assert "Toronto" in titles
        assert "PageGood" in titles
        assert "PageBad" not in titles
