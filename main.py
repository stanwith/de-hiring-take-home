import json, logging, os, time
from datetime import datetime, timezone

from extractor import extract
from transformer import transform
from loader import load

log = logging.getLogger()
log.setLevel(logging.INFO)

# console handler - all logs
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
log.addHandler(console)

# file handler - errors only
os.makedirs("output", exist_ok=True)
error_file = logging.FileHandler("output/errors.log", mode="w")
error_file.setLevel(logging.WARNING)
error_file.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
log.addHandler(error_file)

START_URL = "https://en.wikipedia.org/wiki/Toronto"
MAX_DEPTH = 2
NUM_WORKERS = 10


def run_pipeline():
    start_time = time.time()
    os.makedirs("output", exist_ok=True)

    # ETL
    log.info("EXTRACT")
    fetched_pages, fetch_errors = extract(START_URL, MAX_DEPTH, NUM_WORKERS)

    log.info("TRANSFORM")
    clean_data, transform_errors = transform(fetched_pages)

    log.info("LOAD")
    load("output/data.db", fetched_pages, clean_data)

    # combine all errors for reporting
    error_data = fetch_errors + transform_errors

    # metadata
    elapsed = time.time() - start_time
    links_per_minute = len(fetched_pages) / (elapsed / 60)
    metadata = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "stats": {
            "fetched": len(fetched_pages),
            "fetch_errors": len(fetch_errors),
            "clean": len(clean_data),
            "transform_errors": len(transform_errors),
            "seconds": round(elapsed, 2),
            "links_per_minute": round(links_per_minute, 2)
        },
        "errors": error_data
    }
    with open("output/metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    log.info(f"DONE - {len(clean_data)} pages in production")
    return metadata


if __name__ == "__main__":
    run_pipeline()
