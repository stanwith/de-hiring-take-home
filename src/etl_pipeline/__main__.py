"""CLI entry point for the ETL pipeline."""

import argparse
import logging
import sys

from .config import PipelineConfig
from .pipeline import run_pipeline


def main() -> None:
    """Run the pipeline from the command line."""
    parser = argparse.ArgumentParser(
        description="Wikipedia ETL pipeline - extract, transform, load data from Toronto page"
    )
    parser.add_argument(
        "--url",
        default="https://en.wikipedia.org/wiki/Toronto",
        help="Start URL for crawl (default: Toronto)",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=2,
        help="Max crawl depth (default: 2)",
    )
    parser.add_argument(
        "--max-links-per-page",
        type=int,
        default=25,
        help="Max links to follow per page (default: 25)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Max concurrent HTTP requests (default: 10)",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.5,
        help="Min delay between requests in seconds (default: 0.5)",
    )
    parser.add_argument(
        "--db",
        default="pipeline.duckdb",
        help="DuckDB database path (default: pipeline.duckdb)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = PipelineConfig(
        start_url=args.url,
        max_depth=args.depth,
        max_links_per_page=args.max_links_per_page,
        concurrency=args.concurrency,
        request_delay=args.request_delay,
        db_path=args.db,
    )

    report = run_pipeline(config)

    print("\nPipeline complete:")
    print(f"  Pages crawled:     {report.pages_crawled}")
    print(f"  Pages loaded:      {report.pages_loaded}")
    print(f"  Links extracted:   {report.links_extracted}")
    print(f"  Links loaded:      {report.links_loaded}")
    print(f"  Duration:          {report.duration_seconds:.2f}s")
    print(f"  Throughput:        {report.pages_per_minute:.1f} pages/min")
    print(f"                     {report.links_per_minute:.1f} links/min")

    if report.errors:
        print(f"  Errors:            {len(report.errors)}")
        for e in report.errors:
            print(f"    - {e}")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
