"""
HTML fetching (server + rendered), link extraction, text cleaning, schema extraction.
Ported from Universal_GEO_Auditor notebook with resilience improvements.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections import Counter
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

GOOGLEBOT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/W.X.Y.Z Mobile Safari/537.36 "
        "(compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    )
}

PLAYWRIGHT_TIMEOUT_MS = int(os.environ.get("PLAYWRIGHT_TIMEOUT_MS", "30000"))


# ---------------------------------------------------------------------------
# Text cleaning / chunking
# ---------------------------------------------------------------------------

def clean_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "aside"]):
        tag.extract()
    return soup.get_text(separator=" ", strip=True)


def simulate_rag_chunking(
    text: str, chunk_size: int = 1200, overlap: int = 200
) -> list[str]:
    chunks: list[str] = []
    start = 0
    while start < len(text):
        chunks.append(text[start : start + chunk_size])
        start += chunk_size - overlap
    return chunks


# ---------------------------------------------------------------------------
# Link extraction & scoring
# ---------------------------------------------------------------------------

_SKIP_EXTENSIONS = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js", ".xml", ".pdf", ".zip", ".ico")
_SKIP_PATHS = ("/wp-content/", "/wp-admin/", "/feed/", "/tag/", "/author/", "/cart", "/checkout", "/account", "/login", "/signup")


def _resolve_internal(href: str, base_url: str, base_domain: str) -> str | None:
    """Resolve an href to an absolute internal URL, or None if external/invalid."""
    href = (href or "").strip()
    if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
        return None
    abs_url = urljoin(base_url, href).split("#")[0].rstrip("/")
    if urlparse(abs_url).netloc != base_domain:
        return None
    path = urlparse(abs_url).path.lower()
    if path in ("", "/"):
        return None
    if any(path.endswith(ext) for ext in _SKIP_EXTENSIONS):
        return None
    if any(skip in path for skip in _SKIP_PATHS):
        return None
    return abs_url


def extract_internal_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    base_domain = urlparse(base_url).netloc
    links: set[str] = set()
    for a in soup.select("a[href]"):
        resolved = _resolve_internal(a.get("href", ""), base_url, base_domain)
        if resolved:
            links.add(resolved)
    return sorted(links)


def extract_nav_links(html: str, base_url: str) -> list[str]:
    """Extract internal links from <nav>, <header>, and common nav patterns."""
    soup = BeautifulSoup(html, "html.parser")
    base_domain = urlparse(base_url).netloc
    nav_links: list[str] = []
    seen: set[str] = set()

    # Look in <nav>, <header>, and elements with nav-like roles/classes
    nav_selectors = [
        "nav a[href]",
        "header a[href]",
        "[role='navigation'] a[href]",
        ".nav a[href]",
        ".navbar a[href]",
        ".menu a[href]",
        ".main-menu a[href]",
        ".primary-menu a[href]",
        ".site-nav a[href]",
    ]

    for selector in nav_selectors:
        for a in soup.select(selector):
            resolved = _resolve_internal(a.get("href", ""), base_url, base_domain)
            if resolved and resolved not in seen:
                seen.add(resolved)
                nav_links.append(resolved)

    return nav_links


_DEFAULT_PATH_BOOST: dict[str, int] = {
    "/services/": 4,
    "/service/": 4,
    "/about": 2,
    "/contact": 1,
    "/blog": 1,
    "/geo": 3,
    "/seo": 2,
    "/ppc": 2,
    "/content": 2,
    "/local": 2,
}


def score_candidate_urls(
    links: list[str],
    path_rules: dict[str, int] | None = None,
    nav_links: list[str] | None = None,
    top_n: int = 30,
) -> list[str]:
    """Score and rank internal links. Navigation links get top priority."""
    boost_rules = path_rules if path_rules is not None else _DEFAULT_PATH_BOOST
    nav_set = set(nav_links or [])

    scored: list[tuple[float, str]] = []
    for u in links:
        path = urlparse(u).path.lower()
        score = 0.0

        # Navigation links get a big bonus — these are the site's key pages
        if u in nav_set:
            score += 10

        # Path keyword boost
        if boost_rules:
            for pattern, weight in boost_rules.items():
                if pattern.lower() in path:
                    score += weight

        # Depth-based score — shallower pages are more important
        depth = len([s for s in path.split("/") if s])
        score += max(0, 5 - depth)

        scored.append((score, u))

    scored.sort(reverse=True, key=lambda x: x[0])
    return [u for _, u in scored[:top_n]]


# ---------------------------------------------------------------------------
# JSON-LD extraction & parsing
# ---------------------------------------------------------------------------

def extract_jsonld_blocks(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    return [
        s.get_text(strip=True)
        for s in soup.select('script[type="application/ld+json"]')
        if s.get_text(strip=True)
    ]


def parse_jsonld_blocks(raw_blocks: list[str]) -> tuple[list[dict], list[dict]]:
    parsed: list[dict] = []
    errors: list[dict] = []
    for i, raw in enumerate(raw_blocks):
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                parsed.append(obj)
            elif isinstance(obj, list):
                parsed.extend(o for o in obj if isinstance(o, dict))
            else:
                errors.append(
                    {"block": i, "error": f"Unexpected root type: {type(obj).__name__}", "preview": raw[:280]}
                )
        except Exception as e:
            errors.append({"block": i, "error": str(e), "preview": raw[:280]})
    return parsed, errors


def collect_types(parsed_objects: list[dict]) -> Counter:
    types: list[str] = []

    def walk(node):
        if isinstance(node, dict):
            t = node.get("@type")
            if isinstance(t, str):
                types.append(t)
            elif isinstance(t, list):
                types.extend(x for x in t if isinstance(x, str))
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for x in node:
                walk(x)

    for obj in parsed_objects:
        walk(obj)
    return Counter(types)


def indexability_signals(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    robots = soup.find("meta", attrs={"name": re.compile(r"^robots$", re.I)})
    canonical = soup.find("link", rel=lambda x: x and "canonical" in str(x).lower())
    return {
        "robots_meta": robots.get("content") if robots else None,
        "canonical": canonical.get("href") if canonical else None,
    }


# ---------------------------------------------------------------------------
# HTML fetching
# ---------------------------------------------------------------------------

async def fetch_rendered_html(url: str) -> str:
    """Fetch page with headless Chromium (Playwright). Returns empty string on failure."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("Playwright not installed — skipping rendered fetch for %s", url)
        return ""

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.set_extra_http_headers(GOOGLEBOT_HEADERS)
            try:
                await page.goto(url, timeout=PLAYWRIGHT_TIMEOUT_MS)
                await page.wait_for_load_state("networkidle", timeout=PLAYWRIGHT_TIMEOUT_MS)
                html = await page.content()
                return html
            finally:
                await browser.close()
    except Exception as e:
        logger.error("Rendered fetch failed for %s: %s", url, e)
        return ""


async def fetch_server_html(url: str) -> str:
    """Fetch raw HTML via HTTP GET (what crawlers see before JS)."""
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            r = await client.get(url, headers=GOOGLEBOT_HEADERS)
            return r.text
    except Exception as e:
        logger.error("Server fetch failed for %s: %s", url, e)
        return ""


# ---------------------------------------------------------------------------
# Audit helpers
# ---------------------------------------------------------------------------

def audit_html(html: str) -> dict:
    """Run schema audit on an HTML string. Returns audit dict."""
    blocks = extract_jsonld_blocks(html)
    parsed, errors = parse_jsonld_blocks(blocks)
    types = collect_types(parsed)
    signals = indexability_signals(html)
    return {
        "blocks_count": len(blocks),
        "errors_count": len(errors),
        "types": dict(types.most_common(50)),
        "signals": signals,
        "errors_preview": errors[:3],
    }


async def fetch_page_data(
    url: str,
    path_rules: dict[str, int] | None = None,
    candidate_service_urls: list[str] | None = None,
) -> dict:
    """Fetch a page, extract text, schema, links, and audit results.

    Returns a dict with keys: url, text, parsed_schema, schema_errors,
    candidate_service_urls, server_audit, rendered_audit, rendered_html.
    """
    server_html = await fetch_server_html(url)
    rendered_html = await fetch_rendered_html(url)

    # Use rendered HTML if available, else fall back to server
    primary_html = rendered_html or server_html
    text = clean_text(primary_html)

    raw_blocks = extract_jsonld_blocks(primary_html)
    parsed, errors = parse_jsonld_blocks(raw_blocks)

    if candidate_service_urls is None:
        nav = extract_nav_links(primary_html, url)
        internal_links = extract_internal_links(primary_html, url)
        candidate_service_urls = score_candidate_urls(internal_links, path_rules=path_rules, nav_links=nav)

    server_audit = audit_html(server_html) if server_html else None
    rendered_audit = audit_html(rendered_html) if rendered_html else None

    return {
        "url": url,
        "text": text,
        "parsed_schema": parsed,
        "schema_errors": errors,
        "candidate_service_urls": candidate_service_urls,
        "server_audit": server_audit,
        "rendered_audit": rendered_audit,
        "rendered_html_available": bool(rendered_html),
    }
