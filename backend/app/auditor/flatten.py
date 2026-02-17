"""
Flatten a JSON-LD @graph into natural-language prose for vector search / embeddings.
Target: ~300-500 tokens, no JSON syntax.
"""

from __future__ import annotations


def _get(node: dict, key: str, default: str = "") -> str:
    val = node.get(key, default)
    if isinstance(val, dict):
        return val.get("name") or val.get("url") or val.get("@id") or str(val)
    if isinstance(val, list):
        parts = []
        for item in val:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(item.get("name") or item.get("@id") or str(item))
        return ", ".join(parts)
    return str(val) if val else default


def _node_type(node: dict) -> str:
    t = node.get("@type", "")
    return t if isinstance(t, str) else ", ".join(t) if isinstance(t, list) else ""


def _extract_graph(data: dict | list) -> list[dict]:
    if isinstance(data, list):
        return list(data)
    if isinstance(data, dict):
        if "@graph" in data:
            return list(data["@graph"])
        return [data]
    return []


def _about_labels(node: dict) -> list[str]:
    """Extract concept names from an about array."""
    about = node.get("about", [])
    if not isinstance(about, list):
        about = [about]
    labels = []
    for item in about:
        if isinstance(item, dict):
            name = item.get("name", "")
            if name:
                labels.append(name)
        elif isinstance(item, str):
            labels.append(item)
    return labels


def flatten_graph(jsonld: dict | list) -> str:
    """Convert a JSON-LD graph into natural-language prose."""
    graph = _extract_graph(jsonld)
    if not graph:
        return ""

    # Categorize nodes
    orgs = [n for n in graph if _node_type(n) in (
        "Organization", "ProfessionalService", "Corporation", "LocalBusiness"
    )]
    websites = [n for n in graph if _node_type(n) == "WebSite"]
    pages = [n for n in graph if _node_type(n) in (
        "WebPage", "ServicePage", "AboutPage", "ContactPage", "CollectionPage",
        "ItemPage", "FAQPage",
    )]
    services = [n for n in graph if _node_type(n) in (
        "Service", "FinancialService",
    )]
    blog_posts = [n for n in graph if _node_type(n) in (
        "BlogPosting", "Article", "NewsArticle",
    )]

    sentences: list[str] = []

    # Organization
    for org in orgs:
        name = _get(org, "name", "the organization")
        url = _get(org, "url")
        desc = _get(org, "description")
        org_type = _node_type(org)
        s = f"{name} is a {org_type}"
        if url:
            s += f" located at {url}"
        if desc:
            s += f". {desc}"
        area = _get(org, "areaServed")
        if area:
            s += f". They serve {area}"
        sentences.append(s.rstrip(".") + ".")

        # About concepts on org
        labels = _about_labels(org)
        if labels:
            sentences.append(f"{name} relates to {', '.join(labels)}.")

    # Services
    for svc in services:
        svc_name = _get(svc, "name", "a service")
        desc = _get(svc, "description")
        url = _get(svc, "url")
        s = f"They provide {svc_name}"
        if desc:
            s += f": {desc}"
        if url:
            s += f" The service is available at {url}."
        sentences.append(s.rstrip(".") + ".")

        catalog = svc.get("hasOfferCatalog")
        if isinstance(catalog, dict):
            offers = catalog.get("itemListElement", [])
            offer_names = []
            for offer in offers:
                if isinstance(offer, dict):
                    offer_names.append(_get(offer, "name"))
            if offer_names:
                sentences.append(f"Service offerings include {', '.join(offer_names)}.")

    # WebPage
    for page in pages:
        page_name = _get(page, "name")
        page_url = _get(page, "url")
        if page_name and page_url:
            sentences.append(f"This page ({page_url}) covers {page_name}.")
        elif page_url:
            sentences.append(f"This page is at {page_url}.")

        labels = _about_labels(page)
        if labels:
            sentences.append(f"Key topics: {', '.join(labels)}.")

    # Blog posts
    for post in blog_posts:
        title = _get(post, "headline") or _get(post, "name")
        author = _get(post, "author")
        if title:
            s = f"Article: {title}"
            if author:
                s += f" by {author}"
            sentences.append(s + ".")

    if not sentences:
        return ""

    return " ".join(sentences)


BEST_PRACTICES_TEXT = """Implementation Best Practices:

1. JSON-LD Markup (for crawlers): Place the full JSON-LD in a <script type="application/ld+json"> \
tag inside <head>. This is what Google, Bing, and other search engines read for rich results \
and knowledge graph entries.

2. Flattened Text (for vector search): Use the natural-language version of your structured data \
for embeddings and semantic retrieval (RAG pipelines, vector databases). This version contains \
the same information but in a format that embedding models understand well — no JSON syntax, \
just descriptive sentences.

3. Keep Both in Sync: When you update your JSON-LD schema, regenerate the flattened text. \
They should always represent the same underlying data.

4. Avoid Duplicate Content: The flattened text can supplement your meta description or be \
placed in a semantically relevant location on the page, but do not create visible duplicate \
content blocks that would confuse users or trigger duplicate content issues."""
