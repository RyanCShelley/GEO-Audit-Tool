"""
GEO Auditor service — orchestrates crawl, LLM analysis, schema fix, and flatten.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re

import google.generativeai as genai

from . import crawler, flatten, prompts, schema_fix
from ..wiki import wikidata

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


def _configure_genai() -> genai.GenerativeModel:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set")
    genai.configure(api_key=api_key)
    model_name = os.environ.get("GEMINI_MODEL", "gemini-2.5-pro")
    return genai.GenerativeModel(model_name)


async def _call_gemini(model: genai.GenerativeModel, prompt: str) -> str:
    """Call Gemini with exponential-backoff retry."""
    for attempt in range(MAX_RETRIES):
        try:
            response = model.generate_content(prompt)
            return response.text
        except Exception as e:
            wait = 2**attempt
            logger.warning("Gemini attempt %d failed: %s (retrying in %ds)", attempt + 1, e, wait)
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(wait)
            else:
                raise


def _parse_report_sections(text: str) -> dict:
    """Parse the LLM response into structured sections.

    Handles varied heading formats from the LLM, including:
    - ``### 1) Page Intent``  (markdown heading)
    - ``1) Page Intent:``     (plain numbered)
    - ``**Page Intent:**``    (bold)
    """
    sections = {
        "page_intent": "",
        "visibility_diagnosis": "",
        "fix_plan": "",
        "raw_response": text,
    }

    def _heading(label: str) -> str:
        """Build a flexible regex for a section heading."""
        return rf"(?:#{{1,4}}\s*)?(?:\d+\)\s*)?(?:\*\*)?{label}(?:\*\*)?[:\s]*"

    def _between(label_start: str, label_end: str) -> str | None:
        """Extract text between two section headings."""
        pat = _heading(label_start) + r"(.+?)" + r"(?=" + _heading(label_end) + r")"
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        return m.group(1).strip() if m else None

    def _after(label: str) -> str | None:
        """Extract text after the last known heading (to end or next fenced block)."""
        pat = _heading(label) + r"(.+?)(?=```|\Z)"
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        return m.group(1).strip() if m else None

    sections["page_intent"] = _between("Page Intent", "Visibility Diagnosis") or ""
    sections["visibility_diagnosis"] = _between("Visibility Diagnosis", "Fix Plan") or ""
    sections["fix_plan"] = _between("Fix Plan", "JSON-LD") or _after("Fix Plan") or ""

    return sections


async def audit_single_url(
    url: str,
    path_rules: dict[str, int] | None = None,
    candidate_service_urls: list[str] | None = None,
    approved_qids: list[dict] | None = None,
) -> dict:
    """Run a full audit on a single URL. Returns a structured result dict."""
    model = _configure_genai()

    # 1. Fetch page data
    page_data = await crawler.fetch_page_data(
        url,
        path_rules=path_rules,
        candidate_service_urls=candidate_service_urls,
    )

    # 2. Build audit summary for prompt
    audit_summary = {}
    if page_data["server_audit"]:
        audit_summary["server"] = page_data["server_audit"]
    if page_data["rendered_audit"]:
        audit_summary["rendered"] = page_data["rendered_audit"]

    if not audit_summary:
        return {
            "url": url,
            "error": "Failed to fetch HTML from both server and renderer",
            "stage": "crawl",
        }

    # 3. Build prompt
    chunks = crawler.simulate_rag_chunking(page_data["text"])
    prompt = prompts.build_audit_prompt(
        url=url,
        audit_summary=audit_summary,
        candidate_service_urls=page_data["candidate_service_urls"],
        schema_parse_errors=page_data["schema_errors"],
        sample_chunks=chunks[:3],
        approved_qids=approved_qids,
    )

    # 4. Call Gemini
    try:
        llm_response = await _call_gemini(model, prompt)
    except Exception as e:
        return {
            "url": url,
            "error": f"Gemini API error: {e}",
            "stage": "analyze",
        }

    # 5. Parse response
    sections = _parse_report_sections(llm_response)

    # 6. Extract JSON-LD from response
    jsonld = schema_fix.parse_jsonld_from_llm_response(llm_response)
    corrections: list[dict] = []
    flattened = ""
    if jsonld:
        jsonld, corrections = schema_fix.run_pipeline(jsonld)
        flattened = flatten.flatten_graph(jsonld)

    # 7. Extract suggested concepts (first pass only)
    suggested_concepts: list[str] = []
    suggested_qids: list[dict] = []
    if not approved_qids:
        suggested_concepts = schema_fix.parse_suggested_concepts(llm_response)
        if suggested_concepts:
            suggested_qids = await wikidata.search_multiple(suggested_concepts)

    return {
        "url": url,
        "page_intent": sections["page_intent"],
        "visibility_diagnosis": sections["visibility_diagnosis"],
        "fix_plan": sections["fix_plan"],
        "json_ld": jsonld,
        "json_ld_corrections": corrections,
        "flattened_schema": flattened,
        "best_practices": flatten.BEST_PRACTICES_TEXT,
        "suggested_concepts": suggested_concepts,
        "suggested_qids": suggested_qids,
        "used_qids": approved_qids or [],
        "rendered_html_available": page_data["rendered_html_available"],
        "raw_response": sections["raw_response"],
    }


async def regenerate_with_qids(
    url: str,
    approved_qids: list[dict],
    path_rules: dict[str, int] | None = None,
    candidate_service_urls: list[str] | None = None,
) -> dict:
    """Re-run audit for a URL with approved QIDs to produce final JSON-LD."""
    return await audit_single_url(
        url,
        path_rules=path_rules,
        candidate_service_urls=candidate_service_urls,
        approved_qids=approved_qids,
    )
