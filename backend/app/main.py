"""
FastAPI application — GEO Audit Tool API.
"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from pathlib import Path

import asyncpg
from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import db, jobs
from .auth import (
    create_access_token,
    create_invite_token,
    decode_invite_token,
    get_current_user,
    hash_password,
    require_admin,
    verify_password,
)
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
# Startup / Shutdown
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup():
    await db.init_db()


@app.on_event("shutdown")
async def on_shutdown():
    await db.close_db()

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
# Pydantic models
# ---------------------------------------------------------------------------

class RegisterRequest(BaseModel):
    email: str
    password: str
    name: str
    team_name: str | None = None
    invite_token: str | None = None


class LoginRequest(BaseModel):
    email: str
    password: str


class InviteRequest(BaseModel):
    email: str


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
# Auth Endpoints
# ---------------------------------------------------------------------------

@app.post("/api/auth/register")
async def register(req: RegisterRequest):
    # Check if user already exists
    existing = await db.get_user_by_email(req.email)
    if existing:
        raise HTTPException(400, "Email already registered")

    hashed = hash_password(req.password)

    has_users = await db.has_any_users()

    if req.invite_token:
        # Invited user — join existing team
        invite = decode_invite_token(req.invite_token)
        if not invite:
            raise HTTPException(400, "Invalid or expired invite token")
        if invite["invite_email"].lower() != req.email.lower():
            raise HTTPException(400, "Email does not match invitation")
        team_id = invite["team_id"]
        team = await db.get_team(team_id)
        if not team:
            raise HTTPException(400, "Team no longer exists")
        try:
            user = await db.create_user(team_id, req.email, req.name, hashed, role="member")
        except asyncpg.UniqueViolationError:
            raise HTTPException(400, "Email already registered")
    elif not has_users:
        # First user ever — create a new team as admin
        team_name = req.team_name or f"{req.name}'s Team"
        team = await db.create_team(team_name)
        try:
            user = await db.create_user(team["id"], req.email, req.name, hashed, role="admin")
        except asyncpg.UniqueViolationError:
            raise HTTPException(400, "Email already registered")
    else:
        raise HTTPException(403, "Registration requires an invite link. Contact your team admin.")

    token = create_access_token(user["id"], user["team_id"], user["role"])
    return {
        "token": token,
        "user": {
            "id": user["id"],
            "email": user["email"],
            "name": user["name"],
            "role": user["role"],
            "team_id": user["team_id"],
        },
    }


@app.post("/api/auth/login")
async def login(req: LoginRequest):
    user = await db.get_user_by_email(req.email)
    if not user or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(401, "Invalid email or password")

    team = await db.get_team(user["team_id"])
    token = create_access_token(user["id"], user["team_id"], user["role"])
    return {
        "token": token,
        "user": {
            "id": user["id"],
            "email": user["email"],
            "name": user["name"],
            "role": user["role"],
            "team_id": user["team_id"],
            "team_name": team["name"] if team else None,
        },
    }


@app.get("/api/auth/me")
async def get_me(user: dict = Depends(get_current_user)):
    team = await db.get_team(user["team_id"])
    return {
        "id": user["user_id"],
        "email": user["email"],
        "name": user["name"],
        "role": user["role"],
        "team_id": user["team_id"],
        "team_name": team["name"] if team else None,
    }


# ---------------------------------------------------------------------------
# Team Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/team/members")
async def list_team_members(user: dict = Depends(require_admin)):
    members = await db.get_team_members(user["team_id"])
    return members


@app.post("/api/team/invite")
async def invite_member(req: InviteRequest, user: dict = Depends(require_admin)):
    # Check if email is already on the team
    existing = await db.get_user_by_email(req.email)
    if existing and existing["team_id"] == user["team_id"]:
        raise HTTPException(400, "User is already a team member")

    token = create_invite_token(req.email, user["team_id"])
    return {"invite_token": token, "email": req.email}


@app.delete("/api/team/members/{member_id}")
async def remove_member(member_id: str, user: dict = Depends(require_admin)):
    if member_id == user["user_id"]:
        raise HTTPException(400, "Cannot remove yourself")

    # Verify member belongs to same team
    member = await db.get_user_by_id(member_id)
    if not member or member["team_id"] != user["team_id"]:
        raise HTTPException(404, "Member not found")

    await db.delete_user(member_id)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Project Endpoints
# ---------------------------------------------------------------------------

@app.post("/api/projects")
async def create_project(req: ProjectCreateRequest, user: dict = Depends(get_current_user)):
    project = await db.create_project(req.name, req.description, team_id=user["team_id"])
    return project


@app.get("/api/projects")
async def list_projects(user: dict = Depends(get_current_user)):
    return await db.list_projects(team_id=user["team_id"])


@app.get("/api/projects/{project_id}")
async def get_project(project_id: str, user: dict = Depends(get_current_user)):
    project = await db.get_project(project_id, team_id=user["team_id"])
    if not project:
        raise HTTPException(404, "Project not found.")
    return project


@app.put("/api/projects/{project_id}")
async def update_project(project_id: str, req: ProjectUpdateRequest, user: dict = Depends(get_current_user)):
    project = await db.update_project(project_id, team_id=user["team_id"], name=req.name, description=req.description)
    if not project:
        raise HTTPException(404, "Project not found.")
    return project


@app.delete("/api/projects/{project_id}")
async def delete_project(project_id: str, user: dict = Depends(get_current_user)):
    deleted = await db.delete_project(project_id, team_id=user["team_id"])
    if not deleted:
        raise HTTPException(404, "Project not found.")
    return {"ok": True}


@app.post("/api/projects/{project_id}/urls")
async def add_project_urls(project_id: str, req: AddUrlsRequest, user: dict = Depends(get_current_user)):
    # Verify project belongs to team
    project = await db.get_project(project_id, team_id=user["team_id"])
    if not project:
        raise HTTPException(404, "Project not found.")
    added = await db.add_project_urls(project_id, req.urls)
    return {"added": added}


@app.delete("/api/projects/{project_id}/urls/{url_id}")
async def remove_project_url(project_id: str, url_id: str, user: dict = Depends(get_current_user)):
    # Verify project belongs to team
    project = await db.get_project(project_id, team_id=user["team_id"])
    if not project:
        raise HTTPException(404, "Project not found.")
    removed = await db.remove_project_url(project_id, url_id)
    if not removed:
        raise HTTPException(404, "URL not found.")
    return {"ok": True}


@app.get("/api/projects/{project_id}/urls/{url_id}/history")
async def get_url_history(project_id: str, url_id: str, user: dict = Depends(get_current_user)):
    # Verify project belongs to team
    project = await db.get_project(project_id, team_id=user["team_id"])
    if not project:
        raise HTTPException(404, "Project not found.")
    history = await db.get_url_history(project_id, url_id)
    if not history:
        raise HTTPException(404, "URL not found.")
    return history


@app.get("/api/projects/{project_id}/qids")
async def get_project_qids(project_id: str, url: str, user: dict = Depends(get_current_user)):
    project = await db.get_project(project_id, team_id=user["team_id"])
    if not project:
        raise HTTPException(404, "Project not found.")
    return await db.get_approved_qids(project_id, url)


@app.put("/api/projects/{project_id}/qids")
async def set_project_qids(project_id: str, req: SetQidsRequest, user: dict = Depends(get_current_user)):
    project = await db.get_project(project_id, team_id=user["team_id"])
    if not project:
        raise HTTPException(404, "Project not found.")
    await db.set_approved_qids(project_id, req.url, req.qids)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Audit Endpoints
# ---------------------------------------------------------------------------

@app.post("/api/audit")
async def start_audit(req: AuditRequest, background_tasks: BackgroundTasks, request: Request, user: dict = Depends(get_current_user)):
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(429, "Rate limit exceeded. Try again in a minute.")

    urls = req.urls or []

    if req.seed_url and not urls:
        # Crawl seed URL to discover candidate URLs
        from .auditor.crawler import fetch_server_html, extract_internal_links, extract_nav_links, score_candidate_urls

        html = await fetch_server_html(req.seed_url)
        if not html:
            raise HTTPException(400, f"Could not fetch seed URL: {req.seed_url}")
        nav = extract_nav_links(html, req.seed_url)
        internal = extract_internal_links(html, req.seed_url)
        candidates = score_candidate_urls(internal, path_rules=req.path_rules, nav_links=nav)
        # Return candidates for user to select (don't start audit yet)
        return {"mode": "seed_crawl", "seed_url": req.seed_url, "candidate_urls": candidates}

    if not urls:
        raise HTTPException(400, "Provide 'urls' or 'seed_url'.")

    if len(urls) > MAX_URLS:
        raise HTTPException(400, f"Maximum {MAX_URLS} URLs per audit.")

    # If project_id provided, verify it belongs to user's team and auto-add URLs
    if req.project_id:
        project = await db.get_project(req.project_id, team_id=user["team_id"])
        if not project:
            raise HTTPException(404, "Project not found.")
        await db.add_project_urls(req.project_id, urls)

    job = jobs.create_job(
        urls,
        path_rules=req.path_rules,
        project_id=req.project_id,
        user_id=user["user_id"],
        user_name=user["name"],
    )
    background_tasks.add_task(jobs.run_audit_job, job)

    return {"job_id": job.id, "status": job.status.value, "total_urls": len(urls)}


@app.get("/api/audit/{job_id}")
async def get_audit(job_id: str, user: dict = Depends(get_current_user)):
    result = await jobs.get_job_or_db(job_id)
    if not result:
        raise HTTPException(404, "Job not found.")
    return result


@app.post("/api/audit/report")
async def regenerate_report(req: ReportRequest, user: dict = Depends(get_current_user)):
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
        # Verify project belongs to team
        project = await db.get_project(req.project_id, team_id=user["team_id"])
        if project:
            await db.set_approved_qids(req.project_id, req.url, req.approved_qids)
            if "error" not in result and req.job_id:
                await db.save_audit_result(req.job_id, req.url, result, is_error=False)

    return result


@app.post("/api/schema/validate")
async def validate_schema(req: ValidateRequest, user: dict = Depends(get_current_user)):
    fixed, corrections = schema_fix.run_pipeline(req.jsonld)
    flattened = flatten.flatten_graph(fixed)
    return {
        "json_ld": fixed,
        "corrections": corrections,
        "flattened_schema": flattened,
    }


@app.get("/api/wikidata/search")
async def wikidata_search(q: str, limit: int = 5, user: dict = Depends(get_current_user)):
    results = await wikidata.search_entities(q, limit=limit)
    return {"query": q, "results": results}


# ---------------------------------------------------------------------------
# Serve React frontend in production
# ---------------------------------------------------------------------------
FRONTEND_BUILD = Path(__file__).resolve().parent.parent.parent / "frontend" / "dist"

if FRONTEND_BUILD.is_dir():
    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=FRONTEND_BUILD / "assets"), name="assets")

    # Catch-all: serve static files from build dir, then fall back to index.html for SPA routing
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        from fastapi.responses import FileResponse
        # Check if a real file exists in the build directory (e.g. sma-logo.png, favicon.ico)
        requested = FRONTEND_BUILD / full_path
        if full_path and requested.is_file():
            return FileResponse(requested)
        index = FRONTEND_BUILD / "index.html"
        if index.exists():
            return FileResponse(index)
        raise HTTPException(404)
