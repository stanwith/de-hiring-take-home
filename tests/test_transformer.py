"""Tests for the transformer module."""

from datetime import datetime, timezone

import pytest

from etl_pipeline.transformer import parse_page


def test_parse_page_extracts_title_summary_content():
    """parse_page extracts title, summary, and content from Wikipedia HTML."""
    html = """
    <html>
    <body>
        <h1 id="firstHeading">Toronto</h1>
        <div id="mw-content-text">
            <p>Toronto is the most populous city in Canada [1]. It has many neighbourhoods.</p>
            <p>Second paragraph with more content.</p>
        </div>
    </body>
    </html>
    """
    page, links, followable = parse_page(
        html=html,
        url="https://en.wikipedia.org/wiki/Toronto",
        depth=0,
        parent_url=None,
        last_modified=None,
        crawled_at=datetime.now(timezone.utc),
    )
    assert page is not None
    assert page.title == "Toronto"
    assert "Canada" in page.summary
    assert "[1]" not in page.summary
    assert page.word_count > 0
    assert page.depth == 0
    assert page.url == "https://en.wikipedia.org/wiki/Toronto"


def test_parse_page_cleans_citation_markers():
    """Citation markers like [1] are stripped from text."""
    html = """
    <html>
    <body>
        <h1 id="firstHeading">Test</h1>
        <div id="mw-content-text">
            <p>Some text [1] more [2] and [citation needed] here.</p>
        </div>
    </body>
    </html>
    """
    page, _, _ = parse_page(
        html=html,
        url="https://en.wikipedia.org/wiki/Test",
        depth=0,
        parent_url=None,
        last_modified=None,
        crawled_at=datetime.now(timezone.utc),
    )
    assert page is not None
    assert "[1]" not in page.content
    assert "[2]" not in page.content
    assert "[citation needed]" not in page.content.lower()


def test_parse_page_extracts_links():
    """parse_page extracts PageLinks from article body."""
    html = """
    <html>
    <body>
        <h1 id="firstHeading">Toronto</h1>
        <div id="mw-content-text">
            <p>See <a href="/wiki/Ontario">Ontario</a> and <a href="/wiki/Canada">Canada</a>.</p>
        </div>
    </body>
    </html>
    """
    page, links, followable = parse_page(
        html=html,
        url="https://en.wikipedia.org/wiki/Toronto",
        depth=0,
        parent_url=None,
        last_modified=None,
        crawled_at=datetime.now(timezone.utc),
    )
    assert page is not None
    assert len(links) >= 2
    urls = {l.target_url for l in links}
    assert "https://en.wikipedia.org/wiki/Ontario" in urls
    assert "https://en.wikipedia.org/wiki/Canada" in urls
    # followable_urls should match link targets
    assert "https://en.wikipedia.org/wiki/Ontario" in followable
    assert "https://en.wikipedia.org/wiki/Canada" in followable


def test_parse_page_multiple_pages():
    """parse_page works correctly for multiple pages independently."""
    html = """
    <html>
    <body>
        <h1 id="firstHeading">Page</h1>
        <div id="mw-content-text"><p>Content here.</p></div>
    </body>
    </html>
    """
    page_a, _, _ = parse_page(
        html=html.replace("Page", "A").replace("Content", "A content"),
        url="https://en.wikipedia.org/wiki/A",
        depth=0,
        parent_url=None,
        last_modified=None,
        crawled_at=datetime.now(timezone.utc),
    )
    page_b, _, _ = parse_page(
        html=html.replace("Page", "B").replace("Content", "B content"),
        url="https://en.wikipedia.org/wiki/B",
        depth=1,
        parent_url="https://en.wikipedia.org/wiki/A",
        last_modified=None,
        crawled_at=datetime.now(timezone.utc),
    )
    assert page_a is not None
    assert page_b is not None
    assert page_a.url.endswith("/A")
    assert page_b.url.endswith("/B")


def test_parse_page_links_extracted_before_decompose():
    """Links inside infobox/navbox are captured (extracted before decompose)."""
    html = """
    <html>
    <body>
        <h1 id="firstHeading">Test</h1>
        <div id="mw-content-text">
            <p>Main content paragraph.</p>
            <div class="infobox">
                <a href="/wiki/InfoboxLink">Infobox Link</a>
            </div>
            <a href="/wiki/BodyLink">Body Link</a>
        </div>
    </body>
    </html>
    """
    page, links, followable = parse_page(
        html=html,
        url="https://en.wikipedia.org/wiki/Test",
        depth=0,
        parent_url=None,
        last_modified=None,
        crawled_at=datetime.now(timezone.utc),
    )
    urls = {l.target_url for l in links}
    # Both links should be captured since extraction happens before decompose
    assert "https://en.wikipedia.org/wiki/InfoboxLink" in urls
    assert "https://en.wikipedia.org/wiki/BodyLink" in urls
