"""BFS web crawler with depth/rate limits, robots.txt, and URL canonicalization."""

from __future__ import annotations

import time
import urllib.robotparser
from collections import deque
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup

from clustereye.utils.logging import get_logger

log = get_logger(__name__)

# Tracking params to strip for URL canonicalization
_TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "ref", "source", "fbclid", "gclid", "msclkid",
}


def _canonicalize_url(url: str) -> str:
    """Canonicalize a URL to prevent crawl bloat.

    - Strip fragments (#section)
    - Remove trailing slashes from path
    - Remove tracking params
    - Normalize scheme/host to lowercase
    """
    parsed = urlparse(url)

    # Normalize scheme and host to lowercase
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()

    # Strip trailing slash from path (but keep "/" for root)
    path = parsed.path.rstrip("/") or "/"

    # Remove tracking params
    if parsed.query:
        from urllib.parse import parse_qs, urlencode
        params = parse_qs(parsed.query, keep_blank_values=True)
        filtered = {k: v for k, v in params.items() if k.lower() not in _TRACKING_PARAMS}
        query = urlencode(filtered, doseq=True)
    else:
        query = ""

    # Strip fragment
    return urlunparse((scheme, netloc, path, parsed.params, query, ""))


def _is_same_domain(url: str, base_url: str) -> bool:
    """Check if url belongs to the same domain as base_url."""
    return urlparse(url).netloc.lower() == urlparse(base_url).netloc.lower()


def _get_robots_parser(base_url: str) -> urllib.robotparser.RobotFileParser | None:
    """Fetch and parse robots.txt for a domain."""
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    rp = urllib.robotparser.RobotFileParser()
    try:
        resp = httpx.get(robots_url, timeout=10, follow_redirects=True)
        if resp.status_code == 200:
            rp.parse(resp.text.splitlines())
            return rp
    except Exception:
        pass

    return None


def crawl(
    start_url: str,
    *,
    max_depth: int = 1,
    rate_limit: float = 2.0,
    workdir: Path,
) -> list[Path]:
    """BFS crawl starting from start_url, saving pages to workdir.

    Args:
        start_url: The starting URL to crawl.
        max_depth: Maximum crawl depth (0 = start_url only, 1 = start + linked pages).
        rate_limit: Maximum requests per second.
        workdir: Directory to save downloaded pages.

    Returns:
        List of paths to downloaded HTML files.
    """
    workdir.mkdir(parents=True, exist_ok=True)

    # Fetch robots.txt
    robots = _get_robots_parser(start_url)

    # BFS queue: (url, depth)
    queue: deque[tuple[str, int]] = deque()
    canonical_start = _canonicalize_url(start_url)
    queue.append((canonical_start, 0))

    visited: set[str] = set()
    downloaded: list[Path] = []
    min_interval = 1.0 / rate_limit if rate_limit > 0 else 0
    last_request_time = 0.0

    while queue:
        url, depth = queue.popleft()

        canonical = _canonicalize_url(url)
        if canonical in visited:
            continue
        visited.add(canonical)

        # Check robots.txt
        if robots and not robots.can_fetch("*", url):
            log.debug("robots_blocked", url=url)
            continue

        # Rate limiting
        now = time.monotonic()
        elapsed = now - last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        # Download
        log.debug("crawl_fetch", url=url, depth=depth)
        try:
            resp = httpx.get(url, follow_redirects=True, timeout=30)
            resp.raise_for_status()
            last_request_time = time.monotonic()
        except Exception as e:
            log.warning("crawl_fetch_failed", url=url, error=str(e))
            continue

        content_type = resp.headers.get("content-type", "")
        if "text/html" not in content_type and "application/xhtml" not in content_type:
            continue

        # Save to file
        safe_name = url.split("//")[-1].replace("/", "_").replace("?", "_")[:200]
        if not safe_name.endswith(".html"):
            safe_name += ".html"
        dest = workdir / safe_name
        dest.write_text(resp.text, encoding="utf-8")
        downloaded.append(dest)
        log.info("crawl_page", page=len(downloaded), queue=len(queue), url=url[:80])

        # Extract links for next depth level
        if depth < max_depth:
            soup = BeautifulSoup(resp.text, "html.parser")
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                absolute = urljoin(url, href)
                canonical_link = _canonicalize_url(absolute)

                if canonical_link not in visited and _is_same_domain(canonical_link, start_url):
                    queue.append((canonical_link, depth + 1))

    log.info("crawl_complete", start_url=start_url, pages=len(downloaded))
    return downloaded
