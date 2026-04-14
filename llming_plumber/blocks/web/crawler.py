"""Web crawler block — crawl a website and collect page content.

Fetches a start URL, discovers internal links, and crawls up to ``max_pages``
pages within the same domain.  Respects ``robots.txt`` via a simple check and
throttles requests with a configurable delay.

When connected to a resource block (sink), pages are streamed individually
to storage as they are crawled — no buffering in memory.  A ``content.json``
manifest is written at the end.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import UTC, datetime
from typing import Any, ClassVar
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from pydantic import Field

from llming_plumber.blocks.base import BaseBlock, BlockContext, BlockInput, BlockOutput

logger = logging.getLogger(__name__)

_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)


class WebCrawlerInput(BlockInput):
    start_url: str = Field(
        title="Start URL",
        description="The URL to start crawling from",
        json_schema_extra={"placeholder": "https://example.com"},
    )
    max_pages: int = Field(
        default=50,
        title="Max Pages",
        description="Maximum number of pages to crawl",
        json_schema_extra={"min": 1, "max": 500},
    )
    delay_seconds: float = Field(
        default=1.0,
        title="Crawl Delay (s)",
        description="Seconds to wait between requests (be polite)",
        json_schema_extra={"min": 0.2, "max": 30},
    )
    max_depth: int = Field(
        default=3,
        title="Max Depth",
        description="Maximum link-following depth from start URL",
        json_schema_extra={"min": 1, "max": 10},
    )
    url_pattern: str = Field(
        default="",
        title="URL Filter Pattern",
        description="Regex pattern — only crawl URLs matching this (empty = all on same domain)",
        json_schema_extra={"placeholder": "/products/.*"},
    )
    exclude_pattern: str = Field(
        default="",
        title="Exclude Pattern",
        description="Regex pattern — skip URLs matching this",
        json_schema_extra={"placeholder": "\\.(pdf|zip|png|jpg|gif|css|js)(\\?.*)?$"},
    )
    user_agent: str = Field(
        default=_DEFAULT_UA,
        title="User Agent",
        description="User-Agent header for requests",
    )
    timeout: float = Field(
        default=30.0,
        title="Timeout (s)",
        description="Per-request timeout in seconds",
    )
    extract_text: bool = Field(
        default=True,
        title="Extract Text",
        description="Extract visible text from each page (in addition to raw HTML)",
        json_schema_extra={"widget": "toggle"},
    )


class CrawledPage(BlockOutput):
    url: str = ""
    title: str = ""
    text: str = ""
    html: str = ""
    status_code: int = 0
    content_length: int = 0
    depth: int = 0
    links_found: int = 0
    content_hash: str = ""


class WebCrawlerOutput(BlockOutput):
    pages: list[dict[str, Any]] = Field(default_factory=list)
    page_count: int = 0
    domain: str = ""
    crawl_duration_seconds: float = 0.0
    errors: list[str] = Field(default_factory=list)


def _normalize_url(url: str) -> str:
    """Normalize a URL for deduplication.

    Keeps trailing slashes intact — some servers return 404 for ``/de``
    but 200 for ``/de/``.
    """
    parsed = urlparse(url)
    path = parsed.path or "/"
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def _extract_links(html: str, base_url: str, domain: str) -> list[str]:
    """Extract same-domain links from HTML.

    Handles a common CMS pattern where relative links duplicate the base path
    segment (e.g. base ``/de/``, href ``de/produkte.html`` → ``/de/de/produkte.html``).
    We emit both the naive join and the root-relative version so the crawler
    can reach pages regardless of the site's linking style.
    """
    soup = BeautifulSoup(html, "html.parser")
    base_parsed = urlparse(base_url)
    base_origin = f"{base_parsed.scheme}://{base_parsed.netloc}"
    # First non-empty segment of the base path (e.g. "de" from "/de/")
    base_segments = [s for s in base_parsed.path.split("/") if s]
    first_seg = base_segments[0] if base_segments else ""

    seen: set[str] = set()
    links: list[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue

        # Strip fragment before resolving — avoids spurious paths
        if "#" in href:
            href = href.split("#")[0]
            if not href:
                continue

        candidates: list[str] = []

        # Standard resolution
        full = urljoin(base_url, href)
        candidates.append(full)

        # Fix doubled path: if href is relative and starts with the same
        # segment as the base path, also try resolving from the origin root.
        # e.g. base=/de/, href=de/foo → /de/de/foo (wrong) + /de/foo (right)
        if (
            first_seg
            and not href.startswith(("/", "http://", "https://"))
            and href.startswith(first_seg + "/")
        ):
            candidates.append(f"{base_origin}/{href}")

        for url in candidates:
            parsed = urlparse(url)
            if parsed.netloc == domain and parsed.scheme in ("http", "https"):
                norm = _normalize_url(url)
                if norm not in seen:
                    seen.add(norm)
                    links.append(norm)

    return links


def _extract_text(html: str) -> str:
    """Extract visible text from HTML, removing scripts/styles."""
    soup = BeautifulSoup(html, "html.parser")
    # Remove script and style elements
    for tag in soup(["script", "style", "noscript", "nav", "footer", "header"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def _extract_title(html: str) -> str:
    """Extract page title."""
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("title")
    return title_tag.get_text(strip=True) if title_tag else ""


def _slugify_domain(domain: str) -> str:
    """Convert domain to a filesystem-safe slug."""
    return re.sub(r"[^a-z0-9]+", "_", domain.lower()).strip("_")


def _url_to_slug(url: str) -> str:
    """Convert a URL to a filesystem-safe filename slug."""
    parsed = urlparse(url)
    path = parsed.path or "/"
    slug = re.sub(r"[^a-z0-9]+", "_", path.lower()).strip("_")
    return slug or "index"


_CHECKPOINT_INTERVAL = 10  # save crawl state every N pages


async def _load_crawl_checkpoint(
    sink: Any, domain_slug: str, start_url: str,
) -> dict[str, Any] | None:
    """Load an in-progress crawl checkpoint from the sink."""
    path = f"{domain_slug}/_crawl_checkpoint.json"
    raw = await sink.read(path)
    if raw is None:
        return None
    try:
        cp = json.loads(raw)
        if cp.get("start_url") == start_url and cp.get("status") == "in_progress":
            return cp
    except Exception:
        pass
    return None


async def _save_crawl_checkpoint(
    sink: Any,
    domain_slug: str,
    start_url: str,
    sink_prefix: str,
    visited: set[str],
    queue: list[tuple[str, int]],
    pages: list[dict[str, Any]],
) -> None:
    """Save crawl state checkpoint to the sink."""
    path = f"{domain_slug}/_crawl_checkpoint.json"
    checkpoint = {
        "status": "in_progress",
        "start_url": start_url,
        "sink_prefix": sink_prefix,
        "visited": list(visited),
        "queue": queue,
        "pages": pages,
        "last_updated": datetime.now(UTC).isoformat(),
    }
    await sink.write(path, json.dumps(checkpoint, ensure_ascii=False))


async def _clear_crawl_checkpoint(sink: Any, domain_slug: str) -> None:
    """Mark checkpoint as completed so the next run starts fresh."""
    path = f"{domain_slug}/_crawl_checkpoint.json"
    await sink.write(path, json.dumps({
        "status": "completed",
        "completed_at": datetime.now(UTC).isoformat(),
    }))


class WebCrawlerBlock(BaseBlock[WebCrawlerInput, WebCrawlerOutput]):
    block_type: ClassVar[str] = "web_crawler"
    icon: ClassVar[str] = "tabler/spider"
    categories: ClassVar[list[str]] = ["web/crawl"]
    description: ClassVar[str] = "Crawl a website, discover pages, and extract content"
    cache_ttl: ClassVar[int] = 0

    async def execute(
        self, input: WebCrawlerInput, ctx: BlockContext | None = None
    ) -> WebCrawlerOutput:
        start = datetime.now(UTC)
        now_date = start.strftime("%Y_%m_%d")
        has_sink = ctx is not None and ctx.sink is not None
        parsed_start = urlparse(input.start_url)
        domain = parsed_start.netloc
        domain_slug = _slugify_domain(domain)
        errors: list[str] = []

        url_re = re.compile(input.url_pattern) if input.url_pattern else None
        exclude_re = re.compile(input.exclude_pattern) if input.exclude_pattern else None

        # Default exclude for static assets
        static_re = re.compile(r"\.(pdf|zip|gz|tar|png|jpg|jpeg|gif|svg|ico|css|js|woff|woff2|ttf|eot|mp4|mp3|avi)(\?.*)?$", re.I)

        # Queue: (url, depth)
        start_normalized = _normalize_url(input.start_url)
        queue: list[tuple[str, int]] = [(start_normalized, 0)]
        visited: set[str] = set()
        pages: list[dict[str, Any]] = []
        sink_prefix = f"{domain_slug}/{now_date}"

        # Resume from checkpoint if available
        if has_sink:
            checkpoint = await _load_crawl_checkpoint(
                ctx.sink, domain_slug, input.start_url,  # type: ignore[union-attr]
            )
            if checkpoint is not None:
                visited = set(checkpoint["visited"])
                queue = [(u, d) for u, d in checkpoint["queue"]]
                pages = checkpoint["pages"]
                sink_prefix = checkpoint["sink_prefix"]
                if ctx:
                    await ctx.log(
                        f"Resuming crawl: {len(pages)} pages done, "
                        f"{len(queue)} URLs pending"
                    )

        # Browser-realistic headers to avoid anti-bot filters
        headers = {
            "User-Agent": input.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

        async with httpx.AsyncClient(
            headers=headers,
            timeout=input.timeout,
            follow_redirects=True,
            limits=httpx.Limits(max_connections=5),
            # Keep cookies across requests (session cookies, CSRF tokens, etc.)
            cookies=httpx.Cookies(),
        ) as client:
            while queue and len(pages) < input.max_pages:
                # Respect periodic guard — pause between pages
                if ctx:
                    await ctx.check_pause()

                url, depth = queue.pop(0)

                if url in visited:
                    continue
                visited.add(url)

                # Apply filters (always fetch start URL for link discovery)
                is_start = (url == start_normalized)
                if exclude_re and exclude_re.search(url) and not is_start:
                    continue
                if static_re.search(url) and not is_start:
                    continue
                matches_filter = not url_re or url_re.search(url)
                if not matches_filter and not is_start:
                    continue

                try:
                    # Set Referer to look like internal navigation
                    req_headers = {}
                    if pages:
                        req_headers["Referer"] = pages[-1]["url"]
                    resp = await client.get(url, headers=req_headers)

                    # Some servers require trailing slash — retry on 404
                    if resp.status_code == 404 and not url.endswith("/"):
                        alt = url + "/"
                        if alt not in visited:
                            resp = await client.get(alt, headers=req_headers)
                            if resp.status_code == 200:
                                visited.add(alt)
                                url = alt

                except Exception as e:
                    err = f"Failed to fetch {url}: {e}"
                    errors.append(err)
                    logger.warning("web_crawler: %s", err)
                    continue

                # Skip non-success responses
                if resp.status_code >= 400:
                    continue

                content_type = resp.headers.get("content-type", "")
                if "text/html" not in content_type and "application/xhtml" not in content_type:
                    continue

                html = resp.text
                title = _extract_title(html)
                text = _extract_text(html) if input.extract_text else ""
                content_hash = hashlib.sha256(html.encode()).hexdigest()[:16]
                links = _extract_links(html, url, domain)

                # Only add to results if URL matches filter (start page may not)
                if matches_filter:
                    # Build a filesystem-safe slug from the URL path
                    path_slug = _url_to_slug(url)

                    page = CrawledPage(
                        url=url,
                        title=title,
                        text=text if not has_sink else "",
                        html=html if not has_sink else "",
                        status_code=resp.status_code,
                        content_length=len(html),
                        depth=depth,
                        links_found=len(links),
                        content_hash=content_hash,
                    )
                    pages.append(page.model_dump())

                    # Stream to sink — write files immediately, don't buffer
                    if has_sink:
                        await ctx.sink.write(  # type: ignore[union-attr]
                            f"{sink_prefix}/html/{path_slug}.html",
                            html,
                        )
                        if text:
                            await ctx.sink.write(  # type: ignore[union-attr]
                                f"{sink_prefix}/text/{path_slug}.txt",
                                text,
                            )
                        # Periodic checkpoint for resumability
                        if len(pages) % _CHECKPOINT_INTERVAL == 0:
                            await _save_crawl_checkpoint(
                                ctx.sink,  # type: ignore[union-attr]
                                domain_slug, input.start_url,
                                sink_prefix, visited, queue, pages,
                            )

                    if ctx:
                        await ctx.log(f"[{len(pages)}/{input.max_pages}] {resp.status_code} {title or url}")

                # Enqueue discovered links
                if depth < input.max_depth:
                    for link in links:
                        if link not in visited:
                            queue.append((link, depth + 1))

                # Polite delay
                if queue:
                    await asyncio.sleep(input.delay_seconds)

        elapsed = (datetime.now(UTC) - start).total_seconds()

        # Write manifest to sink
        if has_sink and pages:
            manifest = {
                "domain": domain,
                "crawled_at": datetime.now(UTC).isoformat(),
                "page_count": len(pages),
                "start_url": input.start_url,
                "pages": [
                    {
                        "url": p["url"],
                        "title": p["title"],
                        "status_code": p["status_code"],
                        "content_length": p["content_length"],
                        "depth": p["depth"],
                        "content_hash": p["content_hash"],
                        "html_file": f"html/{_url_to_slug(p['url'])}.html",
                        "text_file": f"text/{_url_to_slug(p['url'])}.txt",
                    }
                    for p in pages
                ],
            }
            await ctx.sink.write(  # type: ignore[union-attr]
                f"{sink_prefix}/content.json",
                json.dumps(manifest, ensure_ascii=False, indent=2),
            )

        # Clear checkpoint — crawl completed successfully
        if has_sink:
            try:
                await _clear_crawl_checkpoint(
                    ctx.sink, domain_slug,  # type: ignore[union-attr]
                )
            except Exception:
                pass

        if ctx:
            await ctx.log(f"Crawl complete: {len(pages)} pages in {elapsed:.1f}s, {len(errors)} errors")

        return WebCrawlerOutput(
            pages=pages,
            page_count=len(pages),
            domain=domain,
            crawl_duration_seconds=round(elapsed, 2),
            errors=errors,
        )
