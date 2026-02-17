"""
Wikidata wbsearchentities client with retry logic.
"""

from __future__ import annotations

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_HEADERS = {
    "User-Agent": "GEOAuditTool/1.0 (https://github.com/geo-audit-tool; contact@example.com)",
}
MAX_RETRIES = 3


async def search_entities(
    query: str, language: str = "en", limit: int = 5
) -> list[dict]:
    """Search Wikidata for entities matching *query*.

    Returns list of ``{"qid": "Q...", "label": "...", "description": "..."}``.
    """
    params = {
        "action": "wbsearchentities",
        "search": query,
        "language": language,
        "format": "json",
        "limit": limit,
    }

    for attempt in range(MAX_RETRIES):
        try:
            async with httpx.AsyncClient(timeout=10, headers=WIKIDATA_HEADERS) as client:
                r = await client.get(WIKIDATA_API, params=params)
                r.raise_for_status()
                data = r.json()

            results = []
            for item in data.get("search", []):
                results.append(
                    {
                        "qid": item.get("id", ""),
                        "label": item.get("label", ""),
                        "description": item.get("description", ""),
                    }
                )
            return results

        except Exception as e:
            wait = 2**attempt
            logger.warning(
                "Wikidata search attempt %d failed for %r: %s (retrying in %ds)",
                attempt + 1, query, e, wait,
            )
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(wait)

    logger.error("Wikidata search exhausted retries for %r", query)
    return []


async def search_multiple(
    concepts: list[str], language: str = "en", limit: int = 5
) -> list[dict]:
    """Search Wikidata for multiple concepts in parallel.

    Returns list of ``{"concept": "...", "candidates": [...]}``.
    """
    tasks = [search_entities(c, language=language, limit=limit) for c in concepts]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    out = []
    for concept, result in zip(concepts, results):
        if isinstance(result, Exception):
            logger.error("Wikidata search failed for %r: %s", concept, result)
            out.append({"concept": concept, "candidates": []})
        else:
            out.append({"concept": concept, "candidates": result})
    return out
