"""Tests for the extractor module."""

import pytest
import respx
from httpx import Response

from etl_pipeline.config import PipelineConfig
from etl_pipeline.extractor import crawl
from etl_pipeline.utils import is_followable, normalize_url


def test_normalize_url():
    """URL normalization produces canonical Wikipedia URLs."""
    assert normalize_url("/wiki/Toronto") == "https://en.wikipedia.org/wiki/Toronto"
    assert normalize_url("https://en.wikipedia.org/wiki/Toronto") == "https://en.wikipedia.org/wiki/Toronto"
    assert normalize_url("https://en.wikipedia.org/wiki/Toronto#Section") == "https://en.wikipedia.org/wiki/Toronto"
    # Normalize does not filter namespaces; is_followable does
    assert normalize_url("/wiki/File:Test.png") == "https://en.wikipedia.org/wiki/File:Test.png"


def test_is_followable():
    """Followable check skips special Wikipedia namespaces."""
    assert is_followable("https://en.wikipedia.org/wiki/Toronto") is True
    assert is_followable("https://en.wikipedia.org/wiki/Ontario") is True
    assert is_followable("https://en.wikipedia.org/wiki/Wikipedia:About") is False
    assert is_followable("https://en.wikipedia.org/wiki/File:Logo.png") is False
    assert is_followable("https://en.wikipedia.org/wiki/Category:Cities") is False
    assert is_followable("https://example.com/wiki/Page") is False


@respx.mock
@pytest.mark.asyncio
async def test_crawl_fetches_single_page():
    """Crawl fetches a single page when depth is 0."""
    html = """
    <html>
    <body>
        <h1 id="firstHeading">Toronto</h1>
        <div id="mw-content-text">
            <p>Toronto is a city in Canada with a very long description to pass the summary length check.</p>
            <a href="/wiki/Ontario">Ontario</a>
        </div>
    </body>
    </html>
    """
    respx.get("https://en.wikipedia.org/wiki/Toronto").mock(
        return_value=Response(200, text=html)
    )

    config = PipelineConfig(
        start_url="https://en.wikipedia.org/wiki/Toronto",
        max_depth=0,
        max_links_per_page=0,
        concurrency=10,
        request_delay=0,
    )
    pages, links, _ = await crawl(config)

    assert len(pages) == 1
    assert pages[0].url == "https://en.wikipedia.org/wiki/Toronto"
    assert pages[0].title == "Toronto"
    assert pages[0].depth == 0
    assert pages[0].word_count > 0


@respx.mock
@pytest.mark.asyncio
async def test_crawl_follows_links_to_depth_1():
    """Crawl follows links when depth is 1."""
    page_a = """
    <html><body>
        <h1 id="firstHeading">A</h1>
        <div id="mw-content-text">
            <p>Content A is long enough to pass the content quality checks easily.</p>
            <a href="/wiki/B">B</a>
        </div>
    </body></html>
    """
    page_b = """
    <html><body>
        <h1 id="firstHeading">B</h1>
        <div id="mw-content-text">
            <p>Content B is also long enough to pass the content quality checks easily.</p>
        </div>
    </body></html>
    """
    respx.get("https://en.wikipedia.org/wiki/A").mock(return_value=Response(200, text=page_a))
    respx.get("https://en.wikipedia.org/wiki/B").mock(return_value=Response(200, text=page_b))

    config = PipelineConfig(
        start_url="https://en.wikipedia.org/wiki/A",
        max_depth=1,
        max_links_per_page=5,
        concurrency=10,
        request_delay=0,
    )
    pages, links, _ = await crawl(config)

    assert len(pages) == 2
    urls = {p.url for p in pages}
    assert "https://en.wikipedia.org/wiki/A" in urls
    assert "https://en.wikipedia.org/wiki/B" in urls


@respx.mock
@pytest.mark.asyncio
async def test_crawl_handles_circular_references():
    """Crawl does not infinite loop on circular links."""
    page_a = """
    <html><body>
        <h1 id="firstHeading">A</h1>
        <div id="mw-content-text">
            <p>Content that is long enough to pass quality filters for the pipeline.</p>
            <a href="/wiki/A">Self</a>
        </div>
    </body></html>
    """
    respx.get("https://en.wikipedia.org/wiki/A").mock(return_value=Response(200, text=page_a))

    config = PipelineConfig(
        start_url="https://en.wikipedia.org/wiki/A",
        max_depth=2,
        max_links_per_page=5,
        concurrency=10,
        request_delay=0,
    )
    pages, links, _ = await crawl(config)

    assert len(pages) == 1
    assert pages[0].url == "https://en.wikipedia.org/wiki/A"
