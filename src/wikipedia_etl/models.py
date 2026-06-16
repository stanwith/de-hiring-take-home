from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import StrEnum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class ValidationStatus(StrEnum):
    PENDING = "pending"
    VALID = "valid"
    INVALID = "invalid"


class RawPageData(BaseModel):
    scrape_run_id: str
    page_title: str
    url: str
    depth_level: int
    scrape_timestamp: datetime
    word_count: Optional[int] = None
    last_modified: Optional[datetime] = None
    links_found: list[str] = Field(default_factory=list)
    http_status: Optional[int] = None
    is_disambiguation: bool = False

    @field_validator("depth_level")
    @classmethod
    def depth_in_range(cls, v: int) -> int:
        if v not in (0, 1, 2):
            raise ValueError(f"depth_level must be 0, 1, or 2; got {v}")
        return v

    @field_validator("url")
    @classmethod
    def wikipedia_url(cls, v: str) -> str:
        if not v.startswith("https://en.wikipedia.org/wiki/"):
            raise ValueError(f"url must be a Wikipedia article URL; got {v!r}")
        return v

    @field_validator("word_count")
    @classmethod
    def non_negative_words(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 0:
            raise ValueError(f"word_count must be >= 0; got {v}")
        return v


class StagingPage(BaseModel):
    scrape_run_id: str
    page_title: Optional[str] = None
    url: Optional[str] = None
    depth_level: Optional[int] = None
    scrape_timestamp: Optional[str] = None
    word_count: Optional[int] = None
    last_modified: Optional[str] = None
    links_json: Optional[str] = None
    http_status: Optional[int] = None
    validation_status: ValidationStatus = ValidationStatus.PENDING
    validation_errors: Optional[str] = None

    @classmethod
    def from_raw(cls, raw: RawPageData) -> "StagingPage":
        return cls(
            scrape_run_id=raw.scrape_run_id,
            page_title=raw.page_title,
            url=raw.url,
            depth_level=raw.depth_level,
            scrape_timestamp=raw.scrape_timestamp.isoformat(),
            word_count=raw.word_count,
            last_modified=raw.last_modified.isoformat() if raw.last_modified else None,
            links_json=json.dumps(raw.links_found),
            http_status=raw.http_status,
        )


class ProductionPage(BaseModel):
    scrape_run_id: str
    page_title: str
    url: str
    depth_level: int
    scrape_timestamp: datetime
    word_count: Optional[int] = None
    last_modified: Optional[datetime] = None

    @field_validator("depth_level")
    @classmethod
    def depth_in_range(cls, v: int) -> int:
        if v not in (0, 1, 2):
            raise ValueError(f"depth_level must be 0, 1, or 2; got {v}")
        return v

    @field_validator("url")
    @classmethod
    def wikipedia_url(cls, v: str) -> str:
        if not v.startswith("https://en.wikipedia.org/wiki/"):
            raise ValueError(f"url must be a Wikipedia article URL; got {v!r}")
        return v

    @field_validator("word_count")
    @classmethod
    def non_negative_words(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 0:
            raise ValueError(f"word_count must be >= 0; got {v}")
        return v


class StagingLink(BaseModel):
    scrape_run_id: str
    source_url: str
    source_title: Optional[str] = None
    target_title: str
    link_order: int = 0


class ProductionLink(BaseModel):
    scrape_run_id: str
    source_url: str
    target_title: str
    link_order: int = 0


class PipelineRun(BaseModel):
    run_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"
    pages_extracted: int = 0
    pages_valid: int = 0
    pages_invalid: int = 0
    error_message: Optional[str] = None

    def to_db_tuple(self) -> tuple:
        return (
            self.run_id,
            self.start_time.isoformat(),
            self.end_time.isoformat() if self.end_time else None,
            self.status,
            self.pages_extracted,
            self.pages_valid,
            self.pages_invalid,
            self.error_message,
        )
