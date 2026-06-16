from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from rich.logging import RichHandler
from rich.table import Table
from rich.console import Console

from .config import parse_args, PipelineConfig
from .extractor import WikipediaExtractor
from .loader import DatabaseLoader
from .models import PipelineRun, StagingLink, StagingPage
from .transformer import transform_links, transform_pages

logger = logging.getLogger(__name__)
console = Console()


def setup_logging(cfg: PipelineConfig) -> None:
    logging.basicConfig(
        level=cfg.log_level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, markup=True)],
    )


async def run_pipeline(cfg: PipelineConfig) -> None:
    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)
    logger.info("Pipeline run [bold]%s[/bold] starting", run_id)

    # ── 1. EXTRACT ────────────────────────────────────────────────────────────
    logger.info("Phase 1: Extracting data from Wikipedia...")
    async with WikipediaExtractor(cfg, run_id) as extractor:
        raw_pages = await extractor.crawl()
    logger.info("Extracted %d pages", len(raw_pages))

    # ── 2. STAGE ──────────────────────────────────────────────────────────────
    logger.info("Phase 2: Loading to staging...")
    staging_pages = [StagingPage.from_raw(p) for p in raw_pages]
    staging_links: list[StagingLink] = []
    for page in raw_pages:
        for order, target_title in enumerate(page.links_found):
            staging_links.append(
                StagingLink(
                    scrape_run_id=run_id,
                    source_url=page.url,
                    source_title=page.page_title,
                    target_title=target_title,
                    link_order=order,
                )
            )

    with DatabaseLoader(cfg.db_path) as loader:
        loader.initialize_schema()
        pipeline_run = PipelineRun(
            run_id=run_id,
            start_time=start_time,
            pages_extracted=len(staging_pages),
        )
        loader.upsert_pipeline_run(pipeline_run)

        n_pages = loader.insert_staging_pages(staging_pages)
        n_links = loader.insert_staging_links(staging_links)
        logger.info("Staged %d pages and %d links", n_pages, n_links)

        # ── 3. TRANSFORM ──────────────────────────────────────────────────────
        logger.info("Phase 3: Transforming and validating...")
        pages_df = loader.get_staging_pages_df(run_id)
        links_df = loader.get_staging_links_df(run_id)

        valid_pages_df, invalid_pages_df = transform_pages(pages_df)
        valid_urls = set(valid_pages_df["url"].dropna())
        clean_links_df = transform_links(links_df, valid_urls)

        valid_ids = valid_pages_df["id"].tolist()
        invalid_ids = invalid_pages_df["id"].tolist()
        errors_by_id = {
            int(row["id"]): row["validation_errors"]
            for _, row in invalid_pages_df.iterrows()
            if row.get("validation_errors")
        }
        loader.update_validation_status(valid_ids, invalid_ids, errors_by_id)
        logger.info(
            "Validation: %d valid, %d invalid", len(valid_ids), len(invalid_ids)
        )

        # ── 4. PROMOTE ────────────────────────────────────────────────────────
        logger.info("Phase 4: Promoting to production...")
        pages_promoted, links_promoted = loader.promote_to_production(run_id)
        logger.info(
            "Promoted %d pages and %d links to production",
            pages_promoted,
            links_promoted,
        )

        # ── 5. FINALIZE ───────────────────────────────────────────────────────
        end_time = datetime.now(timezone.utc)
        elapsed = (end_time - start_time).total_seconds()
        pipeline_run = PipelineRun(
            run_id=run_id,
            start_time=start_time,
            end_time=end_time,
            status="completed",
            pages_extracted=len(staging_pages),
            pages_valid=pages_promoted,
            pages_invalid=len(invalid_ids),
        )
        loader.upsert_pipeline_run(pipeline_run)

        summary = loader.get_summary(run_id)

    _print_summary(run_id, summary, elapsed)


def _print_summary(run_id: str, summary: dict, elapsed: float) -> None:
    table = Table(title=f"Pipeline Run Summary — {run_id[:8]}...", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green", justify="right")
    table.add_row("Elapsed", f"{elapsed:.1f}s")
    table.add_row("Staging Pages", str(summary["staging_pages"]))
    table.add_row("Valid Pages", str(summary["valid_pages"]))
    table.add_row("Invalid Pages", str(summary["invalid_pages"]))
    table.add_row("Production Pages", str(summary["production_pages"]))
    table.add_row("Production Links", str(summary["production_links"]))
    if elapsed > 0:
        pages_per_min = round(summary["production_pages"] / elapsed * 60)
        table.add_row("Throughput", f"~{pages_per_min} pages/min")
    console.print(table)


def main() -> None:
    cfg = parse_args()
    setup_logging(cfg)
    asyncio.run(run_pipeline(cfg))


if __name__ == "__main__":
    main()
