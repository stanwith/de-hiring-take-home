"""Data transformation: parse HTML, clean text, validate with Pydantic."""

import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .models import PageData, PageLink
from .utils import WIKI_BASE, is_followable, normalize_url

logger = logging.getLogger(__name__)


def _clean_text(text: str) -> str:
    """Strip citation markers [1], collapse whitespace, decode HTML entities."""
    if not text:
        return ""
    # Remove citation markers like [1], [2], [12], [citation needed]
    text = re.sub(r"\[\d+\]", "", text)
    text = re.sub(r"\[citation needed\]", "", text, flags=re.IGNORECASE)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_title(soup: BeautifulSoup) -> str:
    """Extract page title from h1#firstHeading."""
    h1 = soup.select_one("h1#firstHeading")
    if h1:
        return _clean_text(h1.get_text())
    return ""


def _extract_summary(soup: BeautifulSoup) -> str:
    """Extract first paragraph from article body."""
    content = soup.select_one("div#mw-content-text")
    if not content:
        return ""
    for p in content.select("p"):
        text = _clean_text(p.get_text())
        if len(text) > 50:
            return text
    return ""


def _extract_links_with_text(
    soup: BeautifulSoup, source_url: str
) -> list[tuple[str, str]]:
    """Extract (target_url, link_text) from article body."""
    content = soup.select_one("div#mw-content-text")
    if not content:
        return []
    seen: set[str] = set()
    result: list[tuple[str, str]] = []
    for a in content.select("a[href^='/wiki/']"):
        href = a.get("href")
        if not href:
            continue
        full = urljoin(WIKI_BASE, href)
        normalized = normalize_url(full)
        if not is_followable(normalized):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        link_text = _clean_text(a.get_text())
        result.append((normalized, link_text))
    return result


def _extract_content(soup: BeautifulSoup) -> str:
    """Extract full article plaintext. WARNING: mutates soup via decompose()."""
    content = soup.select_one("div#mw-content-text")
    if not content:
        return ""
    # Remove script, style, refs, nav
    for tag in content.select("script, style, .reference, .navbox, .infobox"):
        tag.decompose()
    text = content.get_text(separator=" ", strip=True)
    return _clean_text(text)


def parse_page(
    html: str,
    url: str,
    depth: int,
    parent_url: Optional[str],
    last_modified: Optional[datetime],
    crawled_at: datetime,
) -> tuple[PageData | None, list[PageLink], list[str]]:
    """
    Parse HTML once: extract title, summary, links, then content.
    Links are extracted BEFORE _extract_content decomposes soup elements.
    Returns (page_data, page_links, followable_urls_for_bfs).
    """
    soup = BeautifulSoup(html, "lxml")
    title = _extract_title(soup)
    summary = _extract_summary(soup)

    # Extract links BEFORE _extract_content mutates the soup
    link_data = _extract_links_with_text(soup, url)
    followable_urls = [target for target, _ in link_data]

    # Now safe to decompose for content extraction
    content = _extract_content(soup)
    word_count = len(content.split()) if content else 0

    try:
        page = PageData(
            url=url,
            title=title or url.split("/wiki/", 1)[-1].replace("_", " "),
            summary=summary,
            content=content,
            word_count=word_count,
            depth=depth,
            parent_url=parent_url,
            last_modified=last_modified,
            crawled_at=crawled_at,
        )
    except Exception as e:
        logger.warning("Validation failed for %s: %s", url, e)
        return None, [], []

    links: list[PageLink] = []
    for target_url, link_text in link_data:
        try:
            links.append(
                PageLink(source_url=url, target_url=target_url, link_text=link_text)
            )
        except Exception as e:
            logger.debug("Skipped link %s -> %s: %s", url, target_url, e)

    return page, links, followable_urls
