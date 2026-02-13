"""Async BFS crawler for Wikipedia with rate limiting and retries."""

import asyncio
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import httpx

from .config import PipelineConfig
from .models import PageData, PageLink
from .transformer import parse_page
from .utils import normalize_url

logger = logging.getLogger(__name__)


async def _fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    config: PipelineConfig,
) -> tuple[httpx.Response | None, Optional[datetime]]:
    """Fetch URL with exponential backoff retry. Returns (response, last_modified)."""
    last_modified: Optional[datetime] = None
    last_error: Optional[Exception] = None

    for attempt in range(config.max_retries):
        try:
            resp = await client.get(
                url,
                timeout=config.timeout_seconds,
                follow_redirects=True,
            )
            if resp.status_code in (429, 500, 502, 503):
                if attempt < config.max_retries - 1:
                    delay = 2**attempt
                    logger.warning(
                        "Got %s for %s, retrying in %ss (attempt %d)",
                        resp.status_code,
                        url,
                        delay,
                        attempt + 1,
                    )
                    await asyncio.sleep(delay)
                    continue
                resp.raise_for_status()
            resp.raise_for_status()

            lm = resp.headers.get("last-modified")
            if lm:
                try:
                    last_modified = parsedate_to_datetime(lm)
                except (ValueError, TypeError):
                    pass

            return resp, last_modified

        except httpx.HTTPError as e:
            last_error = e
            if attempt < config.max_retries - 1:
                delay = 2**attempt
                logger.warning(
                    "HTTP error %s for %s, retrying in %ss",
                    e,
                    url,
                    delay,
                )
                await asyncio.sleep(delay)

    if last_error:
        raise last_error
    return None, None


async def crawl(
    config: PipelineConfig,
) -> tuple[list[PageData], list[PageLink], list[str]]:
    """
    BFS crawl starting from config.start_url, up to config.max_depth.
    Pages are parsed during crawl (single HTML parse per page).
    Returns (pages, links, errors) ready for loading and reporting.
    """
    all_pages: list[PageData] = []
    all_links: list[PageLink] = []
    all_errors: list[str] = []
    visited: set[str] = set()
    current_level: list[tuple[str, int, str | None]] = [
        (config.start_url, 0, None)
    ]
    sem = asyncio.Semaphore(config.concurrency)

    headers = {"User-Agent": config.user_agent}

    async with httpx.AsyncClient(headers=headers) as client:
        while current_level:
            next_level_set: set[str] = set()
            next_level: list[tuple[str, int, str | None]] = []

            async def fetch_one(
                url: str, depth: int, parent: str | None
            ) -> tuple[PageData | None, list[PageLink], list[str], list[str]]:
                """Fetch one URL, parse it, return (page, links, followable_urls, errors)."""
                normalized = normalize_url(url)
                if not normalized or normalized in visited:
                    return None, [], [], []
                visited.add(normalized)

                async with sem:
                    await asyncio.sleep(config.request_delay)
                    try:
                        resp, last_mod = await _fetch_with_retry(
                            client, normalized, config
                        )
                        if resp is None:
                            return None, [], [], [f"Failed to fetch {normalized}: no response after retries"]
                        page, links, followable = parse_page(
                            html=resp.text,
                            url=normalized,
                            depth=depth,
                            parent_url=parent,
                            last_modified=last_mod,
                            crawled_at=datetime.now(timezone.utc),
                        )
                        if page is None:
                            return None, [], [], [f"Validation failed for {normalized}"]
                        return page, links, followable, []
                    except Exception as e:
                        err = f"Failed to fetch {normalized}: {e}"
                        logger.error("%s", err)
                        return None, [], [], [err]

            tasks = [fetch_one(u, d, p) for u, d, p in current_level]
            outs = await asyncio.gather(*tasks)

            for (page, links, followable, errs), (_, d, _) in zip(outs, current_level):
                all_errors.extend(errs)
                if page is not None:
                    all_pages.append(page)
                    all_links.extend(links)
                    if d < config.max_depth:
                        for link_url in followable[: config.max_links_per_page]:
                            if link_url not in visited and link_url not in next_level_set:
                                next_level_set.add(link_url)
                                next_level.append((link_url, d + 1, page.url))

            current_level = next_level

    return all_pages, all_links, all_errors
