"""Orchestrator tying Extract+Transform and Load together."""

import asyncio
import logging
import time
from dataclasses import dataclass

from .config import PipelineConfig
from .extractor import crawl
from .loader import load_to_db

logger = logging.getLogger(__name__)


@dataclass
class PipelineReport:
    """Summary report from a pipeline run."""

    pages_crawled: int
    pages_loaded: int
    links_extracted: int
    links_loaded: int
    duration_seconds: float
    pages_per_minute: float
    links_per_minute: float
    errors: list[str]


def run_pipeline(config: PipelineConfig) -> PipelineReport:
    """Run the full ETL pipeline: Extract+Transform -> Load."""
    errors: list[str] = []
    start = time.perf_counter()

    logger.info(
        "Starting ETL pipeline from %s (depth=%d)", config.start_url, config.max_depth
    )

    # Extract + Transform (pages are parsed during crawl)
    t0 = time.perf_counter()
    pages, links, crawl_errors = asyncio.run(crawl(config))  # ✅ Unpack 3 values
    errors.extend(crawl_errors)  # Add crawl errors to the report
    crawl_duration = time.perf_counter() - t0
    logger.info(
        "Extract+Transform: %d pages, %d links in %.2fs",
        len(pages),
        len(links),
        crawl_duration,
    )

    # Load
    t1 = time.perf_counter()
    load_to_db(config.db_path, pages, links)
    load_duration = time.perf_counter() - t1
    logger.info("Load: completed in %.2fs", load_duration)

    total_duration = time.perf_counter() - start

    # Calculate both throughput metrics
    pages_per_min = (len(pages) / total_duration * 60) if total_duration > 0 else 0.0
    links_per_min = (len(links) / total_duration * 60) if total_duration > 0 else 0.0

    logger.info(
        "Pipeline complete. Duration: %.2fs, Throughput: %.1f pages/min, %.1f links/min",
        total_duration,
        pages_per_min,
        links_per_min,
    )

    return PipelineReport(
        pages_crawled=len(pages),
        pages_loaded=len(pages),
        links_extracted=len(links),
        links_loaded=len(links),
        duration_seconds=total_duration,
        pages_per_minute=pages_per_min,
        links_per_minute=links_per_min,
        errors=errors,  # This now includes crawl_errors
    )
