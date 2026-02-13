"""Pipeline configuration with defaults, overridable via CLI."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration for the ETL pipeline."""

    start_url: str = "https://en.wikipedia.org/wiki/Toronto"
    max_depth: int = 2
    max_links_per_page: int = 25
    concurrency: int = 10
    request_delay: float = 0.5
    db_path: str = "pipeline.duckdb"
    max_retries: int = 3
    timeout_seconds: float = 30.0
    user_agent: str = field(
        default_factory=lambda: "ETLPipeline/1.0 (Wikipedia crawl; educational)"
    )
