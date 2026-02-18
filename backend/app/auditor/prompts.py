"""
Gemini prompt templates for GEO audit.
Updated with JSON-LD structure rules, approved QID injection,
and structured output for suggested_concepts.
"""

from __future__ import annotations

import json


def build_audit_prompt(
    url: str,
    audit_summary: dict,
    candidate_service_urls: list[str],
    schema_parse_errors: list[dict],
    sample_chunks: list[str],
    approved_qids: list[dict] | None = None,
) -> str:
    """Build the full Gemini prompt for a single URL audit.

    *approved_qids* is a list of ``{"name": "...", "qid": "Q..."}`` dicts.
    When provided, the model is told to use ONLY these QIDs.
    When None, the model is told to suggest concepts (no sameAs yet).
    """

    # ---- QID section ----
    if approved_qids:
        qid_lines = "\n".join(
            f'  - {q["name"]}: https://www.wikidata.org/wiki/{q["qid"]}'
            for q in approved_qids
        )
        qid_section = f"""
--- APPROVED WIKIDATA QIDs (USE ONLY THESE) ---
{qid_lines}

Rules:
1) Every output JSON-LD MUST include an "about" array linking to these Wikidata entities via sameAs.
2) Pattern: {{ "@type":"Thing", "name":"<Concept>", "sameAs":"https://www.wikidata.org/wiki/<QID>" }}
3) Do NOT output OWL/SKOS/RDF code.
4) Do NOT invent QIDs — use ONLY the approved list above.
5) If none of the approved QIDs are relevant to this page, include Thing with "name" only (omit sameAs).
"""
    else:
        qid_section = """
--- GEO ENTITY BRIDGE (WIKIDATA) ---
Hard requirements:
1) In your output, include a "suggested_concepts" section: a JSON array of concept names
   (plain strings like ["FinTech", "Foreign Exchange"]) that are relevant to this page.
   These will be looked up on Wikidata by the system — do NOT include QIDs yourself.
2) Do NOT include "about" arrays with sameAs in the JSON-LD for this pass.
   The about/sameAs will be added after the user approves the QIDs.
3) Do NOT output OWL/SKOS/RDF code.
4) Do NOT invent QIDs.
"""

    return f"""You are a "Generative Engine Optimization (GEO) Architect" and "Technical SEO auditor".

Your job:
(A) Determine page intent and recommend schema improvements.
(B) Diagnose whether the schema is VISIBLE/USABLE to crawlers.
(C) If schema is not visible/usable, recommend TECHNICAL fixes (not just new JSON-LD).
(D) Ensure Service.url points to the correct dedicated service page (not the homepage).
(E) Bridge entities to Wikidata for GEO visibility.

--- URL ---
{url}

--- CRAWL VISIBILITY AUDIT (IMPORTANT) ---
- "server" = raw HTML from requests (closer to initial crawler fetch)
- "rendered" = post-JS DOM from headless browser
Audit summary:
{json.dumps(audit_summary, indent=2)}

Interpretation rules:
1) If server.blocks_count == 0 but rendered.blocks_count > 0:
   -> Schema likely injected by JS. Recommend SSR / output JSON-LD in initial HTML.
2) If errors_count > 0 on server or rendered:
   -> JSON-LD invalid JSON. Recommend fixing syntax, one valid JSON per script, no trailing commas.
3) If robots_meta includes "noindex" or canonical points away:
   -> Recommend fixing indexability/canonicalization first.

--- CANDIDATE SERVICE PAGE URLS (INTERNAL LINKS FOUND) ---
{json.dumps(candidate_service_urls[:25], indent=2)}

IMPORTANT RULE (Service URLs):
- When outputting a Service node, set Service.url to the MOST RELEVANT dedicated service page.
- Prefer URLs like /services/... that match the service name.
- If the current page IS the service page, set Service.url to this page URL.
- If NO dedicated service page exists, OMIT Service.url (do NOT point everything to the homepage).
{qid_section}
--- JSON-LD STRUCTURE RULES (MANDATORY — MUST MATCH schema.org) ---
Your JSON-LD output MUST follow this structure AND only use properties valid on each type per https://schema.org/docs/schemas.html:

1) "logo" MUST be an ImageObject: {{ "@type": "ImageObject", "url": "<logo_url>" }} — NEVER a bare string.
2) Include a WebSite node: @id = "<site_url>/#website", url, name, publisher -> #organization.
3) WebPage: isPartOf -> #website (NOT #organization); mainEntity -> #service.
   Valid properties: name, url, description, isPartOf, mainEntity, about, breadcrumb, datePublished, dateModified.
4) Put "about" ONLY on WebPage (the GEO entity bridge). NEVER on Service or Organization nodes.
5) Organization / LocalBusiness / ProfessionalService:
   Valid properties: name, url, logo, description, address, telephone, email, sameAs, areaServed, geo, priceRange, openingHours, parentOrganization.
   NOTE: "provider" is NOT valid on ProfessionalService or Organization. Use "parentOrganization" instead.
6) Service (use this type for service offerings, NOT ProfessionalService):
   Valid properties: name, url, description, provider, serviceType, areaServed, offers, category.
   "provider" IS valid here — link to #organization.
7) Use @id references (e.g. {{ "@id": "<url>/#organization" }}) to link nodes — do not duplicate data.
8) IMPORTANT: Only use properties that exist on schema.org for the given @type.
   Check the type hierarchy: ProfessionalService → LocalBusiness → Organization → Thing.
   Service → Intangible → Thing. These are DIFFERENT branches — do not mix their properties.

--- SCHEMA PARSE ERRORS (first 3) ---
{json.dumps(schema_parse_errors[:3], indent=2)}

--- PAGE CONTENT (first 3 chunks) ---
{json.dumps(sample_chunks, indent=2)}

TASKS — respond with these exact sections:

1) Page Intent:
   State one of: Home / Service / Blog / Product / About / Contact

2) Visibility Diagnosis:
   3-6 bullets on schema visibility issues.

3) Fix Plan:
   Prioritized steps (technical + schema + validation).

4) JSON-LD:
   Output corrected JSON-LD in a ```json fenced code block.
   Follow all JSON-LD STRUCTURE RULES above.

{"" if approved_qids else '''5) Suggested Concepts:
   Output a JSON array of concept name strings relevant to this page, e.g.:
   ```json
   ["FinTech", "Foreign Exchange", "Payment Processing"]
   ```
   These will be looked up on Wikidata for the user to approve.'''}
"""
