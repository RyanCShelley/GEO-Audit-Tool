"""
JSON-LD post-processing pipeline.

Each transform takes a graph (list of nodes) and returns
(modified_graph, list_of_correction_dicts).
"""

from __future__ import annotations

import copy
import json
import re
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_nodes_by_type(graph: list[dict], *type_names: str) -> list[dict]:
    """Return nodes whose @type matches any of *type_names*."""
    out = []
    for node in graph:
        t = node.get("@type", "")
        types = [t] if isinstance(t, str) else t
        if any(tn in types for tn in type_names):
            out.append(node)
    return out


def _extract_graph(data: dict | list) -> list[dict]:
    """Normalise JSON-LD into a flat list of nodes."""
    if isinstance(data, list):
        return list(data)
    if isinstance(data, dict):
        if "@graph" in data:
            return list(data["@graph"])
        return [data]
    return []


def _wrap_graph(nodes: list[dict], original: dict | list) -> dict | list:
    """Re-wrap nodes into the original container shape."""
    if isinstance(original, dict) and "@graph" in original:
        out = copy.deepcopy(original)
        out["@graph"] = nodes
        return out
    if isinstance(original, list):
        return nodes
    if len(nodes) == 1:
        return nodes[0]
    return {"@graph": nodes}


# ---------------------------------------------------------------------------
# Transform 1: normalize logo
# ---------------------------------------------------------------------------

def normalize_logo(graph: list[dict]) -> tuple[list[dict], list[dict]]:
    corrections = []
    for node in graph:
        logo = node.get("logo")
        if isinstance(logo, str):
            node["logo"] = {"@type": "ImageObject", "url": logo}
            corrections.append({
                "transform": "normalize_logo",
                "node_id": node.get("@id", "?"),
                "detail": f"Converted bare logo string to ImageObject: {logo}",
            })
    return graph, corrections


# ---------------------------------------------------------------------------
# Transform 2: ensure WebSite node
# ---------------------------------------------------------------------------

def ensure_website_node(graph: list[dict]) -> tuple[list[dict], list[dict]]:
    corrections = []
    websites = _find_nodes_by_type(graph, "WebSite")
    if websites:
        return graph, corrections

    # Try to infer from an Organization or WebPage
    orgs = _find_nodes_by_type(graph, "Organization", "ProfessionalService")
    pages = _find_nodes_by_type(graph, "WebPage", "ServicePage", "AboutPage", "ContactPage", "CollectionPage")

    base_url = None
    org_id = None
    for org in orgs:
        url = org.get("url") or org.get("@id", "")
        if url:
            parsed = urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            org_id = org.get("@id")
            break

    if not base_url:
        for page in pages:
            url = page.get("url") or page.get("@id", "")
            if url:
                parsed = urlparse(url)
                base_url = f"{parsed.scheme}://{parsed.netloc}"
                break

    if not base_url:
        return graph, corrections

    website_id = f"{base_url}/#website"
    website = {
        "@type": "WebSite",
        "@id": website_id,
        "url": base_url,
        "name": base_url.split("//")[-1],
    }
    if org_id:
        website["publisher"] = {"@id": org_id}

    graph.append(website)
    corrections.append({
        "transform": "ensure_website_node",
        "detail": f"Added missing WebSite node: {website_id}",
    })

    # Fix WebPage.isPartOf to point to WebSite
    for page in pages:
        is_part_of = page.get("isPartOf")
        if not is_part_of or (isinstance(is_part_of, dict) and is_part_of.get("@id") != website_id):
            page["isPartOf"] = {"@id": website_id}
            corrections.append({
                "transform": "ensure_website_node",
                "node_id": page.get("@id", "?"),
                "detail": f"Set WebPage.isPartOf to {website_id}",
            })

    return graph, corrections


# ---------------------------------------------------------------------------
# Transform 3: fix about placement
# ---------------------------------------------------------------------------

def fix_about_placement(graph: list[dict]) -> tuple[list[dict], list[dict]]:
    """Remove 'about' from Service/ProfessionalService nodes.
    If WebPage exists and lacks 'about', move the removed items there."""
    corrections = []
    service_types = ("Service", "ProfessionalService", "FinancialService")
    services = _find_nodes_by_type(graph, *service_types)
    pages = _find_nodes_by_type(graph, "WebPage", "ServicePage", "AboutPage", "ContactPage", "CollectionPage")

    moved_about: list = []
    for svc in services:
        about = svc.pop("about", None)
        if about is not None:
            corrections.append({
                "transform": "fix_about_placement",
                "node_id": svc.get("@id", "?"),
                "detail": "Removed 'about' from Service node",
            })
            if isinstance(about, list):
                moved_about.extend(about)
            else:
                moved_about.append(about)

    if moved_about and pages:
        page = pages[0]
        existing = page.get("about", [])
        if not isinstance(existing, list):
            existing = [existing] if existing else []
        existing.extend(moved_about)
        page["about"] = existing
        corrections.append({
            "transform": "fix_about_placement",
            "node_id": page.get("@id", "?"),
            "detail": f"Moved {len(moved_about)} about entries to WebPage",
        })

    return graph, corrections


# ---------------------------------------------------------------------------
# Transform 4: set mainEntity
# ---------------------------------------------------------------------------

def set_main_entity(graph: list[dict]) -> tuple[list[dict], list[dict]]:
    """If a WebPage and a Service exist, ensure WebPage.mainEntity -> Service."""
    corrections = []
    pages = _find_nodes_by_type(graph, "WebPage", "ServicePage")
    services = _find_nodes_by_type(graph, "Service", "ProfessionalService", "FinancialService")

    if not pages or not services:
        return graph, corrections

    service = services[0]
    service_id = service.get("@id")
    if not service_id:
        return graph, corrections

    page = pages[0]
    existing = page.get("mainEntity")
    if existing and isinstance(existing, dict) and existing.get("@id") == service_id:
        return graph, corrections

    page["mainEntity"] = {"@id": service_id}
    corrections.append({
        "transform": "set_main_entity",
        "node_id": page.get("@id", "?"),
        "detail": f"Set WebPage.mainEntity to {service_id}",
    })
    return graph, corrections


# ---------------------------------------------------------------------------
# Transform 5: validate properties against schema.org
# ---------------------------------------------------------------------------

# Valid properties per schema.org type (common types we generate).
# Includes inherited properties from parent types.
# Meta properties (@context, @type, @id, @graph) are always allowed.
_VALID_PROPERTIES: dict[str, set[str]] = {
    "Organization": {
        "name", "url", "logo", "description", "sameAs", "address", "telephone",
        "email", "foundingDate", "founder", "numberOfEmployees", "areaServed",
        "image", "identifier", "alternateName", "contactPoint", "member",
        "department", "subOrganization", "parentOrganization", "brand",
        "award", "knowsAbout", "slogan", "legalName", "taxID",
    },
    "LocalBusiness": {
        "name", "url", "logo", "description", "sameAs", "address", "telephone",
        "email", "foundingDate", "areaServed", "image", "identifier",
        "alternateName", "contactPoint", "parentOrganization", "brand",
        "openingHours", "openingHoursSpecification", "priceRange", "geo",
        "review", "aggregateRating", "hasOfferCatalog", "paymentAccepted",
        "currenciesAccepted", "menu", "servesCuisine",
    },
    "ProfessionalService": {
        "name", "url", "logo", "description", "sameAs", "address", "telephone",
        "email", "foundingDate", "areaServed", "image", "identifier",
        "alternateName", "contactPoint", "parentOrganization", "brand",
        "openingHours", "openingHoursSpecification", "priceRange", "geo",
        "review", "aggregateRating", "hasOfferCatalog", "serviceType",
        "knowsAbout", "slogan",
    },
    "Service": {
        "name", "url", "description", "sameAs", "image", "identifier",
        "provider", "serviceType", "areaServed", "offers", "hasOfferCatalog",
        "category", "serviceOutput", "serviceArea", "availableChannel",
        "termsOfService", "aggregateRating", "review", "brand",
        "alternateName",
    },
    "WebSite": {
        "name", "url", "description", "publisher", "potentialAction",
        "sameAs", "alternateName", "inLanguage",
    },
    "WebPage": {
        "name", "url", "description", "isPartOf", "mainEntity", "about",
        "breadcrumb", "primaryImageOfPage", "datePublished", "dateModified",
        "author", "inLanguage", "potentialAction", "sameAs", "significantLink",
        "speakable", "relatedLink", "lastReviewed", "reviewedBy",
    },
    "ImageObject": {
        "url", "contentUrl", "width", "height", "caption", "description",
        "name", "encodingFormat", "thumbnail",
    },
    "Thing": {
        "name", "url", "description", "sameAs", "identifier", "image",
        "alternateName",
    },
}

# Properties that can be moved to a valid alternative
_PROPERTY_FIXES: dict[str, dict[str, str | None]] = {
    # provider is not valid on Organization-based types → remove it
    "Organization": {"provider": None},
    "LocalBusiness": {"provider": None},
    "ProfessionalService": {"provider": None},
    # about is not valid on Service → remove (already handled by fix_about_placement)
    "Service": {"about": None},
}

_META_KEYS = {"@context", "@type", "@id", "@graph", "@vocab", "@reverse", "@language"}


def validate_properties(graph: list[dict]) -> tuple[list[dict], list[dict]]:
    """Remove properties not valid for their schema.org type."""
    corrections = []
    for node in graph:
        node_type = node.get("@type", "")
        types = [node_type] if isinstance(node_type, str) else list(node_type)

        # Collect all valid properties for this node's types
        valid = set(_META_KEYS)
        matched_type = False
        for t in types:
            if t in _VALID_PROPERTIES:
                valid |= _VALID_PROPERTIES[t]
                matched_type = True

        if not matched_type:
            # Unknown type — skip validation
            continue

        # Check for fixable properties first
        for t in types:
            if t in _PROPERTY_FIXES:
                for bad_prop, replacement in _PROPERTY_FIXES[t].items():
                    if bad_prop in node:
                        val = node.pop(bad_prop)
                        detail = f"Removed invalid '{bad_prop}' from {t}"
                        if replacement and replacement not in node:
                            node[replacement] = val
                            detail = f"Replaced invalid '{bad_prop}' with '{replacement}' on {t}"
                        corrections.append({
                            "transform": "validate_properties",
                            "node_id": node.get("@id", "?"),
                            "detail": detail,
                        })

        # Remove remaining invalid properties
        invalid_keys = [k for k in node if k not in valid]
        for k in invalid_keys:
            node.pop(k)
            corrections.append({
                "transform": "validate_properties",
                "node_id": node.get("@id", "?"),
                "detail": f"Removed invalid property '{k}' from {'/'.join(types)} (not in schema.org spec)",
            })

    return graph, corrections


# ---------------------------------------------------------------------------
# Transform 6: validate @id references
# ---------------------------------------------------------------------------

def validate_id_refs(graph: list[dict]) -> tuple[list[dict], list[dict]]:
    """Check that all @id references resolve to a node in the graph."""
    corrections = []
    defined_ids = {node.get("@id") for node in graph if node.get("@id")}

    def check_refs(obj, parent_path=""):
        if isinstance(obj, dict):
            # A reference-only node: {"@id": "..."}
            if list(obj.keys()) == ["@id"] and obj["@id"] not in defined_ids:
                corrections.append({
                    "transform": "validate_id_refs",
                    "detail": f"Dangling @id reference: {obj['@id']} (at {parent_path})",
                })
            for k, v in obj.items():
                check_refs(v, f"{parent_path}.{k}")
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                check_refs(v, f"{parent_path}[{i}]")

    for node in graph:
        check_refs(node, node.get("@id", "root"))

    return graph, corrections


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

TRANSFORMS = [
    normalize_logo,
    ensure_website_node,
    fix_about_placement,
    set_main_entity,
    validate_properties,
    validate_id_refs,
]


def run_pipeline(jsonld: dict | list) -> tuple[dict | list, list[dict]]:
    """Run all transforms on a JSON-LD object. Returns (fixed_jsonld, all_corrections)."""
    graph = _extract_graph(copy.deepcopy(jsonld))
    all_corrections: list[dict] = []

    for transform in TRANSFORMS:
        graph, corrections = transform(graph)
        all_corrections.extend(corrections)

    return _wrap_graph(graph, jsonld), all_corrections


def parse_jsonld_from_llm_response(text: str) -> dict | list | None:
    """Extract a JSON-LD object from a fenced ```json code block in LLM output."""
    pattern = r"```json\s*\n(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL)
    for match in matches:
        try:
            obj = json.loads(match)
            # Heuristic: if it has @context or @graph or @type, it's JSON-LD
            if isinstance(obj, dict) and any(k in obj for k in ("@context", "@graph", "@type")):
                return obj
            if isinstance(obj, list) and any(
                isinstance(item, dict) and "@type" in item for item in obj
            ):
                return obj
        except json.JSONDecodeError:
            continue
    return None


def parse_suggested_concepts(text: str) -> list[str]:
    """Extract the suggested_concepts JSON array from LLM output."""
    # Flexible heading: ### 5) Suggested Concepts  or  Suggested Concepts:  etc.
    pattern = r"(?:#{1,4}\s*)?(?:\d+\)\s*)?(?:\*\*)?Suggested[_ ]Concepts(?:\*\*)?[:\s]*```json\s*\n(\[.*?\])\s*```"
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Fallback: find any JSON array that looks like a list of strings
    arrays = re.findall(r"```json\s*\n(\[.*?\])\s*```", text, re.DOTALL)
    for arr_str in arrays:
        try:
            arr = json.loads(arr_str)
            if isinstance(arr, list) and all(isinstance(x, str) for x in arr):
                return arr
        except json.JSONDecodeError:
            continue

    return []
