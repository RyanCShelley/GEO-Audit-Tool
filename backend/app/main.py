"""
FastAPI application — GEO Audit Tool API.
"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import db, jobs
from .auditor import schema_fix, flatten
from .wiki import wikidata

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="GEO Audit Tool", version="1.0.0")

# ---------------------------------------------------------------------------
# Health check (before auth middleware so it's never blocked)
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}

# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup():
    await db.init_db()

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------
cors_origins = os.environ.get("CORS_ORIGINS", "http://localhost:5173").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in cors_origins],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Rate limiting (simple in-memory)
# ---------------------------------------------------------------------------
_rate_limit: dict[str, list[float]] = defaultdict(list)
RATE_LIMIT_MAX = 5  # per minute
MAX_URLS = int(os.environ.get("MAX_URLS_PER_AUDIT", "10"))


def _check_rate_limit(client_ip: str) -> bool:
    now = time.time()
    window = [t for t in _rate_limit[client_ip] if now - t < 60]
    _rate_limit[client_ip] = window
    if len(window) >= RATE_LIMIT_MAX:
        return False
    _rate_limit[client_ip].append(now)
    return True


# ---------------------------------------------------------------------------
# Auth middleware (optional)
# ---------------------------------------------------------------------------
API_SECRET = os.environ.get("API_SECRET_KEY", "")


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    if API_SECRET and request.url.path.startswith("/api/"):
        token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        if token != API_SECRET:
            return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    return await call_next(request)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class AuditRequest(BaseModel):
    urls: list[str] | None = None
    seed_url: str | None = None
    path_rules: dict[str, int] | None = None
    project_id: str | None = None


class ReportRequest(BaseModel):
    job_id: str
    url: str
    approved_qids: list[dict]  # [{"name": "...", "qid": "Q..."}]
    project_id: str | None = None


class ValidateRequest(BaseModel):
    jsonld: dict | list


class ProjectCreateRequest(BaseModel):
    name: str
    description: str = ""


class ProjectUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None


class AddUrlsRequest(BaseModel):
    urls: list[str]


class SetQidsRequest(BaseModel):
    url: str
    qids: list[dict]  # [{"name": "...", "qid": "Q..."}]


# ---------------------------------------------------------------------------
# Project Endpoints
# ---------------------------------------------------------------------------

@app.post("/api/projects")
async def create_project(req: ProjectCreateRequest):
    project = await db.create_project(req.name, req.description)
    return project


@app.get("/api/projects")
async def list_projects():
    return await db.list_projects()


@app.get("/api/projects/{project_id}")
async def get_project(project_id: str):
    project = await db.get_project(project_id)
    if not project:
        raise HTTPException(404, "Project not found.")
    return project


@app.put("/api/projects/{project_id}")
async def update_project(project_id: str, req: ProjectUpdateRequest):
    project = await db.update_project(project_id, name=req.name, description=req.description)
    if not project:
        raise HTTPException(404, "Project not found.")
    return project


@app.delete("/api/projects/{project_id}")
async def delete_project(project_id: str):
    deleted = await db.delete_project(project_id)
    if not deleted:
        raise HTTPException(404, "Project not found.")
    return {"ok": True}


@app.post("/api/projects/{project_id}/urls")
async def add_project_urls(project_id: str, req: AddUrlsRequest):
    # Verify project exists
    project = await db.get_project(project_id)
    if not project:
        raise HTTPException(404, "Project not found.")
    added = await db.add_project_urls(project_id, req.urls)
    return {"added": added}


@app.delete("/api/projects/{project_id}/urls/{url_id}")
async def remove_project_url(project_id: str, url_id: str):
    removed = await db.remove_project_url(project_id, url_id)
    if not removed:
        raise HTTPException(404, "URL not found.")
    return {"ok": True}


@app.get("/api/projects/{project_id}/urls/{url_id}/history")
async def get_url_history(project_id: str, url_id: str):
    history = await db.get_url_history(project_id, url_id)
    if not history:
        raise HTTPException(404, "URL not found.")
    return history


@app.get("/api/projects/{project_id}/qids")
async def get_project_qids(project_id: str, url: str):
    return await db.get_approved_qids(project_id, url)


@app.put("/api/projects/{project_id}/qids")
async def set_project_qids(project_id: str, req: SetQidsRequest):
    await db.set_approved_qids(project_id, req.url, req.qids)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Audit Endpoints
# ---------------------------------------------------------------------------

@app.post("/api/audit")
async def start_audit(req: AuditRequest, background_tasks: BackgroundTasks, request: Request):
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(429, "Rate limit exceeded. Try again in a minute.")

    urls = req.urls or []

    if req.seed_url and not urls:
        # Crawl seed URL to discover candidate URLs
        from .auditor.crawler import fetch_server_html, extract_internal_links, score_candidate_urls

        html = await fetch_server_html(req.seed_url)
        if not html:
            raise HTTPException(400, f"Could not fetch seed URL: {req.seed_url}")
        internal = extract_internal_links(html, req.seed_url)
        candidates = score_candidate_urls(internal, path_rules=req.path_rules)
        # Return candidates for user to select (don't start audit yet)
        return {"mode": "seed_crawl", "seed_url": req.seed_url, "candidate_urls": candidates}

    if not urls:
        raise HTTPException(400, "Provide 'urls' or 'seed_url'.")

    if len(urls) > MAX_URLS:
        raise HTTPException(400, f"Maximum {MAX_URLS} URLs per audit.")

    # If project_id provided, auto-add URLs to the project
    if req.project_id:
        project = await db.get_project(req.project_id)
        if not project:
            raise HTTPException(404, "Project not found.")
        await db.add_project_urls(req.project_id, urls)

    job = jobs.create_job(urls, path_rules=req.path_rules, project_id=req.project_id)
    background_tasks.add_task(jobs.run_audit_job, job)

    return {"job_id": job.id, "status": job.status.value, "total_urls": len(urls)}


@app.get("/api/audit/{job_id}")
async def get_audit(job_id: str):
    result = await jobs.get_job_or_db(job_id)
    if not result:
        raise HTTPException(404, "Job not found.")
    return result


@app.post("/api/audit/report")
async def regenerate_report(req: ReportRequest):
    from .auditor.service import regenerate_with_qids

    # Look up original job to get path_rules and candidate URLs
    job = jobs.get_job(req.job_id)
    path_rules = job.path_rules if job else None
    candidate_urls = job.candidate_service_urls if job else None

    result = await regenerate_with_qids(
        url=req.url,
        approved_qids=req.approved_qids,
        path_rules=path_rules,
        candidate_service_urls=candidate_urls,
    )

    # If project_id provided, persist approved QIDs and save result
    if req.project_id:
        await db.set_approved_qids(req.project_id, req.url, req.approved_qids)
        if "error" not in result and req.job_id:
            await db.save_audit_result(req.job_id, req.url, result, is_error=False)

    return result


@app.post("/api/schema/validate")
async def validate_schema(req: ValidateRequest):
    fixed, corrections = schema_fix.run_pipeline(req.jsonld)
    flattened = flatten.flatten_graph(fixed)
    return {
        "json_ld": fixed,
        "corrections": corrections,
        "flattened_schema": flattened,
    }


@app.get("/api/wikidata/search")
async def wikidata_search(q: str, limit: int = 5):
    results = await wikidata.search_entities(q, limit=limit)
    return {"query": q, "results": results}


# ---------------------------------------------------------------------------
# Serve React frontend in production
# ---------------------------------------------------------------------------
FRONTEND_BUILD = Path(__file__).resolve().parent.parent.parent / "frontend" / "dist"

if FRONTEND_BUILD.is_dir():
    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=FRONTEND_BUILD / "assets"), name="assets")

    # Catch-all: serve index.html for SPA routing
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        from fastapi.responses import FileResponse
        index = FRONTEND_BUILD / "index.html"
        if index.exists():
            return FileResponse(index)
        raise HTTPException(404)
