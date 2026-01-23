import sqlite3, logging

log = logging.getLogger(__name__)


def load(db_path, fetched_pages, clean_data):
    """Load fetched pages to staging table, clean data to production table."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # staging - raw fetched pages (pre-transform)
    c.execute("DROP TABLE IF EXISTS staging")
    c.execute("CREATE TABLE staging (url TEXT PRIMARY KEY, title TEXT, content TEXT, depth INTEGER)")
    for r in fetched_pages:
        c.execute("INSERT INTO staging VALUES (?, ?, ?, ?)", (r["url"], r["title"], r["content"], r["depth"]))
    log.info(f"Loaded {len(fetched_pages)} rows to staging")

    # production - clean data
    c.execute("DROP TABLE IF EXISTS production")
    c.execute("CREATE TABLE production (url TEXT PRIMARY KEY, title TEXT, content TEXT, depth INTEGER, fetched_at TEXT)")
    for r in clean_data:
        c.execute("INSERT INTO production VALUES (?, ?, ?, ?, ?)", (r["url"], r["title"], r["content"], r["depth"], r["fetched_at"]))
    log.info(f"Loaded {len(clean_data)} rows to production")

    conn.commit()
    conn.close()
