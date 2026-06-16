from __future__ import annotations

import json
import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

_WIKIPEDIA_URL_RE = re.compile(r"^https://en\.wikipedia\.org/wiki/.+")
_VALID_DEPTHS = {0, 1, 2}
_CRITICAL_COLS = ["page_title", "url", "depth_level", "scrape_timestamp", "scrape_run_id"]


def transform_pages(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate staging page rows; return (valid_df, invalid_df)."""
    if df.empty:
        return df.copy(), df.copy()

    errors: dict[int, list[str]] = {idx: [] for idx in df.index}

    for col in _CRITICAL_COLS:
        if col not in df.columns:
            for idx in df.index:
                errors[idx].append(f"missing column: {col}")
            continue
        null_mask = df[col].isna()
        for idx in df.index[null_mask]:
            errors[idx].append(f"{col} is null")

    if "url" in df.columns:
        invalid_url = df["url"].notna() & ~df["url"].str.match(_WIKIPEDIA_URL_RE, na=False)
        for idx in df.index[invalid_url]:
            errors[idx].append(f"invalid url: {df.at[idx, 'url']!r}")

    if "depth_level" in df.columns:
        invalid_depth = df["depth_level"].notna() & ~df["depth_level"].isin(_VALID_DEPTHS)
        for idx in df.index[invalid_depth]:
            errors[idx].append(f"depth_level {df.at[idx, 'depth_level']} not in {{0,1,2}}")

    if "word_count" in df.columns:
        invalid_wc = df["word_count"].notna() & (df["word_count"] < 0)
        for idx in df.index[invalid_wc]:
            errors[idx].append(f"word_count {df.at[idx, 'word_count']} < 0")

    if "scrape_timestamp" in df.columns:
        parsed_ts = pd.to_datetime(df["scrape_timestamp"], utc=True, errors="coerce")
        bad_ts = df["scrape_timestamp"].notna() & parsed_ts.isna()
        for idx in df.index[bad_ts]:
            errors[idx].append(f"scrape_timestamp unparseable: {df.at[idx, 'scrape_timestamp']!r}")
        null_ts = df["scrape_timestamp"].isna()
        for idx in df.index[null_ts]:
            if "scrape_timestamp is null" not in errors[idx]:
                errors[idx].append("scrape_timestamp is null")

    if "last_modified" in df.columns:
        pd.to_datetime(df["last_modified"], utc=True, errors="coerce")

    df = df.copy()
    df["validation_errors"] = [
        json.dumps(errs) if errs else None for errs in [errors[i] for i in df.index]
    ]
    df["validation_status"] = [
        "valid" if not errors[i] else "invalid" for i in df.index
    ]

    valid_df = df[df["validation_status"] == "valid"].copy()
    invalid_df = df[df["validation_status"] == "invalid"].copy()

    logger.info(
        "Transformation complete: %d valid, %d invalid pages",
        len(valid_df),
        len(invalid_df),
    )
    return valid_df, invalid_df


def transform_links(df: pd.DataFrame, valid_urls: set[str]) -> pd.DataFrame:
    """Filter and deduplicate staging links; keep only links from valid source pages."""
    if df.empty:
        return df.copy()

    original_count = len(df)
    df = df[df["source_url"].isin(valid_urls)].copy()
    df = df[df["target_title"].notna() & (df["target_title"].str.strip() != "")]
    df = df.drop_duplicates(subset=["scrape_run_id", "source_url", "target_title"])
    logger.info(
        "Link transformation: %d → %d rows (dropped %d)",
        original_count,
        len(df),
        original_count - len(df),
    )
    return df
