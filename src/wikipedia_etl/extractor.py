from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from .config import PipelineConfig
from .models import RawPageData

logger = logging.getLogger(__name__)

_WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"


class RateLimiter:
    """Token-bucket rate limiter for async code."""

    def __init__(self, rate: float) -> None:
        self._rate = rate
        self._lock = asyncio.Lock()
        self._last_call: float = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            gap = 1.0 / self._rate
            wait = self._last_call + gap - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()


class WikipediaExtractor:
    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        self.config = config
        self.run_id = run_id
        self._client: Optional[httpx.AsyncClient] = None
        self._rate_limiter = RateLimiter(config.rate_limit_rps)
        self._semaphore = asyncio.Semaphore(config.n_concurrent)

    async def __aenter__(self) -> "WikipediaExtractor":
        self._client = httpx.AsyncClient(
            http2=True,
            timeout=30.0,
            headers={"User-Agent": self.config.user_agent},
        )
        return self

    async def __aexit__(self, *args) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _build_retry(self):
        cfg = self.config
        return retry(
            stop=stop_after_attempt(cfg.retry_max_attempts),
            wait=wait_exponential(multiplier=1, min=cfg.retry_min_wait, max=cfg.retry_max_wait),
            retry=retry_if_exception_type((httpx.NetworkError, httpx.TimeoutException)),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

    async def _fetch_page_api(self, title: str, pllimit: int) -> dict:
        assert self._client is not None

        async def _do_request() -> dict:
            await self._rate_limiter.acquire()
            params = {
                "action": "query",
                "prop": "info|extracts|revisions|links|pageprops",
                "titles": title,
                "inprop": "url",
                "explaintext": "1",
                "exsectionformat": "plain",
                "rvprop": "timestamp",
                "rvlimit": "1",
                "pllimit": str(pllimit),
                "plnamespace": "0",
                "redirects": "1",
                "format": "json",
                "formatversion": "2",
            }
            response = await self._client.get(_WIKIPEDIA_API, params=params)
            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", "5"))
                logger.warning("HTTP 429 — sleeping %.1fs (Retry-After)", retry_after)
                await asyncio.sleep(retry_after)
                raise httpx.NetworkError("429 Too Many Requests")
            response.raise_for_status()
            return response.json()

        decorated = self._build_retry()(_do_request)
        return await decorated()

    def _parse_api_response(
        self, api_json: dict, depth: int, title_queried: str
    ) -> Optional[RawPageData]:
        query = api_json.get("query", {})
        pages = query.get("pages", [])
        if not pages:
            logger.debug("No pages in API response for %r", title_queried)
            return None

        page = pages[0]
        if page.get("missing", False) or page.get("pageid", -1) == -1:
            logger.debug("Page missing in API: %r", title_queried)
            return None

        page_title = page.get("title", title_queried)
        canonical_url = page.get("canonicalurl") or f"https://en.wikipedia.org/wiki/{page_title.replace(' ', '_')}"

        is_disambiguation = "disambiguation" in page.get("pageprops", {})

        extract: str = page.get("extract") or ""
        word_count = len(extract.split()) if extract.strip() else 0

        last_modified: Optional[datetime] = None
        revisions = page.get("revisions", [])
        if revisions:
            try:
                last_modified = datetime.fromisoformat(
                    revisions[0]["timestamp"].replace("Z", "+00:00")
                )
            except (KeyError, ValueError):
                pass

        raw_links = page.get("links", [])
        links_found = [lnk["title"] for lnk in raw_links if "title" in lnk]

        return RawPageData(
            scrape_run_id=self.run_id,
            page_title=page_title,
            url=canonical_url,
            depth_level=depth,
            scrape_timestamp=datetime.now(timezone.utc),
            word_count=word_count,
            last_modified=last_modified,
            links_found=links_found,
            http_status=200,
            is_disambiguation=is_disambiguation,
        )

    async def _fetch_page(
        self, title: str, depth: int, pllimit: int
    ) -> Optional[RawPageData]:
        async with self._semaphore:
            try:
                api_json = await self._fetch_page_api(title, pllimit)
                return self._parse_api_response(api_json, depth, title)
            except Exception as exc:
                logger.warning("Failed to fetch %r at depth %d: %s", title, depth, exc)
                return None

    async def crawl(self) -> list[RawPageData]:
        """BFS crawl from start_url to max_depth."""
        cfg = self.config
        start_title = cfg.start_title

        visited: set[str] = {start_title}
        results: list[RawPageData] = []

        pllimit_d0 = cfg.max_links_depth1
        pending: set[asyncio.Task] = {
            asyncio.create_task(
                self._fetch_page(start_title, 0, pllimit_d0),
                name=f"page::{start_title}::0",
            )
        }

        logger.info("Starting BFS crawl from %r (max_depth=%d)", start_title, cfg.max_depth)
        fetched = 0

        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                result = task.result()
                if result is None:
                    continue

                results.append(result)
                fetched += 1
                if fetched % 10 == 0:
                    logger.info("Fetched %d pages so far...", fetched)

                if result.depth_level >= cfg.max_depth:
                    continue
                if result.is_disambiguation:
                    logger.debug("Skipping link expansion for disambiguation page %r", result.page_title)
                    continue

                next_depth = result.depth_level + 1
                pllimit = cfg.max_links_for_depth(result.depth_level)
                link_cap = pllimit

                newly_queued = 0
                for link_title in result.links_found[:link_cap]:
                    normalized = link_title
                    if normalized in visited:
                        continue
                    visited.add(normalized)
                    pending.add(
                        asyncio.create_task(
                            self._fetch_page(normalized, next_depth, cfg.max_links_for_depth(next_depth)),
                            name=f"page::{normalized}::{next_depth}",
                        )
                    )
                    newly_queued += 1

                logger.debug(
                    "Queued %d new tasks from %r (depth %d → %d)",
                    newly_queued,
                    result.page_title,
                    result.depth_level,
                    next_depth,
                )

        logger.info("Crawl complete: %d pages fetched total", len(results))
        return results
