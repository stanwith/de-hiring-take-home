"""Pydantic models for validated pipeline data."""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class PageData(BaseModel):
    """Validated page data after extraction and transformation."""

    url: str = Field(..., description="Canonical Wikipedia URL")
    title: str = Field(..., min_length=1, description="Page title")
    summary: str = Field(default="", description="First paragraph / intro text")
    content: str = Field(default="", description="Full article plaintext")
    word_count: int = Field(default=0, ge=0, description="Word count of content")
    depth: int = Field(..., ge=0, le=2, description="Crawl depth 0, 1, or 2")
    parent_url: Optional[str] = Field(
        default=None, description="Which page linked here"
    )
    last_modified: Optional[datetime] = Field(
        default=None,
        description="From HTTP Last-Modified header (RFC 5322); stored in UTC",
    )
    crawled_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When the page was crawled (UTC); export as ISO 8601 for APIs",
    )


class PageLink(BaseModel):
    """Link between two pages."""

    source_url: str = Field(..., description="URL of the page containing the link")
    target_url: str = Field(..., description="URL of the linked page")
    link_text: str = Field(default="", description="Anchor text of the link")
