import re, logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def transform(fetched_pages):
    """Take fetched pages, validate and clean them. Return clean and error records."""
    clean = []
    errors = []

    for r in fetched_pages:
        # check we have the fields we need
        if not r.get("url") or not r.get("title") or not r.get("content"):
            errors.append({"url": r.get("url"), "reason": "missing fields"})
            continue

        # check title and content arent empty
        if r["title"].strip() == "" or r["content"].strip() == "":
            errors.append({"url": r.get("url"), "reason": "empty title or content"})
            continue

        # clean the text
        content = r["content"]
        content = re.sub(r'\[\d+\]', '', content)  # remove [1] [2] etc
        content = " ".join(content.split())  # normalize whitespace

        title = " ".join(r["title"].split())

        clean.append({
            "url": r["url"],
            "title": title,
            "content": content,
            "depth": r["depth"],
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        })

    log.info(f"Transform done: {len(clean)} clean, {len(errors)} errors")
    return clean, errors
