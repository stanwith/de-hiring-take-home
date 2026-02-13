"""URL utility functions for Wikipedia link handling."""

from urllib.parse import urljoin, urlparse

WIKI_BASE = "https://en.wikipedia.org"

SKIP_PREFIXES = (
    "Wikipedia:",
    "Help:",
    "File:",
    "Template:",
    "Category:",
    "Special:",
    "Talk:",
    "Portal:",
    "User:",
    "Draft:",
    "Media:",
)


def normalize_url(url: str) -> str:
    """Convert to canonical Wikipedia URL, removing fragments."""
    parsed = urlparse(url)
    if not parsed.netloc:
        full = urljoin(WIKI_BASE + "/", url)
        parsed = urlparse(full)
    path = parsed.path.rstrip("/") or "/"
    if not path.startswith("/wiki/"):
        return ""
    clean = f"{parsed.scheme or 'https'}://{parsed.netloc or 'en.wikipedia.org'}{path}"
    return clean.split("#")[0]


def is_followable(url: str) -> bool:
    """Check if URL is a Wikipedia article we should follow."""
    if not url or not url.startswith("https://en.wikipedia.org/wiki/"):
        return False
    parts = url.split("/wiki/", 1)
    if len(parts) != 2:
        return False
    slug = parts[1]
    if ":" in slug:
        prefix = slug.split(":")[0] + ":"
        if prefix in SKIP_PREFIXES:
            return False
    return True
