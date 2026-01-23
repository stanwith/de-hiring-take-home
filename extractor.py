import time, logging, requests
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

WIKI_BASE = "https://en.wikipedia.org"

def fetch_page(url, session, retries=3):
    """Fetch a page with retries."""
    for _ in range(retries):
        try:
            resp = session.get(url, timeout=15)

            if resp.status_code == 429:
                log.warning(f"Rate limited on {url}, waiting 5s")
                time.sleep(5)
                continue
            if resp.status_code >= 500:
                log.warning(f"Server error {resp.status_code} on {url}, retrying")
                time.sleep(5)
                continue

            resp.raise_for_status()
            return resp

        except requests.RequestException as e:
            log.warning(f"Request failed: {e}")
            time.sleep(2)

    return None


def get_wiki_links(soup, visited):
    """Extract wikipedia article links, excluding already visited."""
    content = soup.find("div", {"id": "mw-content-text"})
    if content is None:
        return []

    skip = ("File:", "Category:", "Template:", "Help:", "Portal:",
            "Special:", "Talk:", "User:", "Wikipedia:", "Main_Page")

    links = []
    for a in content.find_all("a", href=True):
        href = a["href"]

        if not href.startswith("/wiki/"):
            continue
        if any(s in href for s in skip):
            continue
        if "?" in href or "#" in href:
            continue

        full_url = urljoin(WIKI_BASE, href)
        if full_url not in visited and full_url not in links:
            links.append(full_url)
            if len(links) >= 100:
                break

    return links


def parse_page(url, html, depth, visited):
    """Parse a wiki page and extract data + links."""
    soup = BeautifulSoup(html, "html.parser")

    title_el = soup.find("h1", {"id": "firstHeading"})
    title = title_el.get_text(strip=True) if title_el else ""

    content = soup.find("div", {"id": "mw-content-text"})
    paragraphs = content.find_all("p") if content else []
    text = "\n".join(p.get_text() for p in paragraphs)

    links = get_wiki_links(soup, visited)

    return {
        "url": url,
        "title": title,
        "content": text,
        "links": links,
        "depth": depth,
    }


def crawl_one(url, depth, session, visited):
    """Crawl a single page. Returns (result, error)."""
    log.info(f"Fetching {url} (depth {depth})")

    resp = fetch_page(url, session)
    if resp is None:
        return None, {"url": url, "error": "fetch failed", "depth": depth}

    try:
        data = parse_page(url, resp.text, depth, visited)
        return data, None
    except Exception as e:
        log.error(f"Parse error for {url}: {e}")
        return None, {"url": url, "error": str(e), "depth": depth}


def extract(start_url, max_depth, num_workers):
    """ Crawl wikipedia using level-by-level BFS. Returns: (results, errors) """
    session = requests.Session()
    session.headers["User-Agent"] = "WikiCrawler/1.0"

    visited = {start_url}
    results = []
    errors = []

    # Level-by-level BFS
    current_level = [start_url]

    for depth in range(max_depth + 1):
        if not current_level:
            break

        log.info(f"Depth {depth}: crawling {len(current_level)} pages")
        next_level = []

        # Crawl all pages at this depth in parallel
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {
                executor.submit(crawl_one, url, depth, session, visited): url
                for url in current_level
            }

            for future in as_completed(futures):
                result, error = future.result()

                if error:
                    errors.append(error)
                if result:
                    results.append(result)

                    # Collect links for next level (if not at max depth)
                    if depth < max_depth:
                        for link in result["links"]:
                            if link not in visited:
                                visited.add(link)
                                next_level.append(link)

        current_level = next_level

    log.info(f"Done. Crawled {len(results)} pages, {len(errors)} errors.")
    return results, errors
