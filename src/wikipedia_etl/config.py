from __future__ import annotations

import argparse
from dataclasses import dataclass, field


@dataclass
class PipelineConfig:
    start_url: str = "https://en.wikipedia.org/wiki/Toronto"
    max_depth: int = 2
    max_links_depth1: int = 50
    max_links_depth2: int = 10
    rate_limit_rps: float = 1.0
    n_concurrent: int = 3
    db_path: str = "data/wikipedia.db"
    user_agent: str = (
        "WikipediaETL/1.0 (educational project; github.com/de-hiring-take-home)"
    )
    retry_max_attempts: int = 3
    retry_min_wait: float = 1.0
    retry_max_wait: float = 10.0
    log_level: str = "INFO"

    def max_links_for_depth(self, depth: int) -> int:
        if depth == 0:
            return self.max_links_depth1
        return self.max_links_depth2

    @property
    def start_title(self) -> str:
        return self.start_url.split("/wiki/", 1)[1]


def parse_args() -> PipelineConfig:
    parser = argparse.ArgumentParser(
        description="Wikipedia ETL pipeline — crawl Toronto page to depth 2"
    )
    parser.add_argument(
        "--start-url",
        default="https://en.wikipedia.org/wiki/Toronto",
        help="Starting Wikipedia page URL",
    )
    parser.add_argument(
        "--max-links-depth1",
        type=int,
        default=50,
        help="Max links to follow from depth-0 page (default: 50)",
    )
    parser.add_argument(
        "--max-links-depth2",
        type=int,
        default=10,
        help="Max links to follow per depth-1 page (default: 10)",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=1.0,
        dest="rate_limit_rps",
        help="Max requests per second (default: 1.0)",
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        default=3,
        dest="n_concurrent",
        help="Max concurrent requests (default: 3)",
    )
    parser.add_argument(
        "--db-path",
        default="data/wikipedia.db",
        help="Path to SQLite database (default: data/wikipedia.db)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()
    return PipelineConfig(
        start_url=args.start_url,
        max_links_depth1=args.max_links_depth1,
        max_links_depth2=args.max_links_depth2,
        rate_limit_rps=args.rate_limit_rps,
        n_concurrent=args.n_concurrent,
        db_path=args.db_path,
        log_level=args.log_level,
    )
