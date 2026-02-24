"""
PostgreSQL persistence layer for the GEO Audit Tool.

Uses asyncpg for async access with a connection pool.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _id() -> str:
    return uuid.uuid4().hex[:12]


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Set up JSONB codec so values are automatically decoded/encoded."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def init_db() -> None:
    """Create the connection pool and all tables if they don't exist."""
    global _pool
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is required")
    _pool = await asyncpg.create_pool(
        database_url, min_size=2, max_size=10, init=_init_connection
    )

    async with _pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                email TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'member',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS project_urls (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                url TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(project_id, url)
            );

            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                project_id TEXT REFERENCES projects(id) ON DELETE SET NULL,
                user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                progress_current INTEGER NOT NULL DEFAULT 0,
                progress_total INTEGER NOT NULL DEFAULT 0,
                current_url TEXT NOT NULL DEFAULT '',
                path_rules JSONB,
                candidate_service_urls JSONB,
                created_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ
            );

            CREATE TABLE IF NOT EXISTS audit_results (
                id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                url TEXT NOT NULL,
                is_error BOOLEAN NOT NULL DEFAULT FALSE,
                data JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS approved_qids (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                url TEXT NOT NULL,
                name TEXT NOT NULL,
                qid TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(project_id, url, qid)
            );
        """)
        # Indexes
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
            CREATE INDEX IF NOT EXISTS idx_users_team_id ON users(team_id);
            CREATE INDEX IF NOT EXISTS idx_projects_team_id ON projects(team_id);
            CREATE INDEX IF NOT EXISTS idx_project_urls_project_id ON project_urls(project_id);
            CREATE INDEX IF NOT EXISTS idx_jobs_project_id ON jobs(project_id);
            CREATE INDEX IF NOT EXISTS idx_jobs_user_id ON jobs(user_id);
            CREATE INDEX IF NOT EXISTS idx_audit_results_job_id ON audit_results(job_id);
            CREATE INDEX IF NOT EXISTS idx_approved_qids_project_id ON approved_qids(project_id);
        """)
    logger.info("PostgreSQL database initialised")


async def close_db() -> None:
    """Close the connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("PostgreSQL connection pool closed")


def _get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database not initialised. Call init_db() first.")
    return _pool


# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------

async def create_team(name: str) -> dict:
    pool = _get_pool()
    tid = _id()
    now = _now()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO teams (id, name, created_at) VALUES ($1, $2, $3)",
            tid, name, datetime.fromisoformat(now),
        )
    return {"id": tid, "name": name, "created_at": now}


async def has_any_users() -> bool:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT EXISTS(SELECT 1 FROM users) AS has_users")
    return row["has_users"]


async def get_team(team_id: str) -> dict | None:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM teams WHERE id = $1", team_id)
    if not row:
        return None
    return {"id": row["id"], "name": row["name"], "created_at": row["created_at"].isoformat()}


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

async def create_user(team_id: str, email: str, name: str, password_hash: str, role: str = "member") -> dict:
    pool = _get_pool()
    uid = _id()
    now = _now()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, team_id, email, name, password_hash, role, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            uid, team_id, email, name, password_hash, role, datetime.fromisoformat(now),
        )
    return {"id": uid, "team_id": team_id, "email": email, "name": name, "role": role, "created_at": now}


async def get_user_by_email(email: str) -> dict | None:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE email = $1", email)
    if not row:
        return None
    return {
        "id": row["id"],
        "team_id": row["team_id"],
        "email": row["email"],
        "name": row["name"],
        "password_hash": row["password_hash"],
        "role": row["role"],
        "created_at": row["created_at"].isoformat(),
    }


async def get_user_by_id(user_id: str) -> dict | None:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
    if not row:
        return None
    return {
        "id": row["id"],
        "team_id": row["team_id"],
        "email": row["email"],
        "name": row["name"],
        "password_hash": row["password_hash"],
        "role": row["role"],
        "created_at": row["created_at"].isoformat(),
    }


async def get_team_members(team_id: str) -> list[dict]:
    pool = _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, email, name, role, created_at FROM users WHERE team_id = $1 ORDER BY created_at",
            team_id,
        )
    return [
        {"id": r["id"], "email": r["email"], "name": r["name"], "role": r["role"], "created_at": r["created_at"].isoformat()}
        for r in rows
    ]


async def delete_user(user_id: str) -> bool:
    pool = _get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM users WHERE id = $1", user_id)
    return result == "DELETE 1"


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------

async def create_project(name: str, description: str = "", team_id: str = "") -> dict:
    pool = _get_pool()
    pid = _id()
    now = _now()
    ts = datetime.fromisoformat(now)
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO projects (id, team_id, name, description, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6)",
            pid, team_id, name, description, ts, ts,
        )
    return {"id": pid, "name": name, "description": description, "created_at": now, "updated_at": now}


async def list_projects(team_id: str = "") -> list[dict]:
    pool = _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.*,
                   (SELECT COUNT(*) FROM project_urls WHERE project_id = p.id) AS url_count,
                   (SELECT MAX(j.created_at) FROM jobs j WHERE j.project_id = p.id) AS last_audit
            FROM projects p
            WHERE p.team_id = $1
            ORDER BY p.updated_at DESC
            """,
            team_id,
        )
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "description": r["description"],
            "created_at": r["created_at"].isoformat(),
            "updated_at": r["updated_at"].isoformat(),
            "url_count": r["url_count"],
            "last_audit": r["last_audit"].isoformat() if r["last_audit"] else None,
        }
        for r in rows
    ]


async def get_project(project_id: str, team_id: str = "") -> dict | None:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM projects WHERE id = $1 AND team_id = $2", project_id, team_id
        )
        if not row:
            return None

        # URLs
        url_rows = await conn.fetch(
            "SELECT * FROM project_urls WHERE project_id = $1 ORDER BY created_at", project_id
        )
        urls = [{"id": r["id"], "url": r["url"], "created_at": r["created_at"].isoformat()} for r in url_rows]

        # Recent jobs with user info
        job_rows = await conn.fetch(
            """
            SELECT j.id, j.status, j.progress_total, j.created_at, j.completed_at, j.user_id,
                   u.name AS user_name
            FROM jobs j
            LEFT JOIN users u ON j.user_id = u.id
            WHERE j.project_id = $1
            ORDER BY j.created_at DESC LIMIT 20
            """,
            project_id,
        )
        recent_jobs = [
            {
                "id": r["id"],
                "status": r["status"],
                "url_count": r["progress_total"],
                "created_at": r["created_at"].isoformat(),
                "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
                "user_name": r["user_name"],
            }
            for r in job_rows
        ]

    return {
        "id": row["id"],
        "name": row["name"],
        "description": row["description"],
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
        "urls": urls,
        "recent_jobs": recent_jobs,
    }


async def update_project(project_id: str, team_id: str = "", name: str | None = None, description: str | None = None) -> dict | None:
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM projects WHERE id = $1 AND team_id = $2", project_id, team_id
        )
        if not row:
            return None
        new_name = name if name is not None else row["name"]
        new_desc = description if description is not None else row["description"]
        now = datetime.fromisoformat(_now())
        await conn.execute(
            "UPDATE projects SET name = $1, description = $2, updated_at = $3 WHERE id = $4",
            new_name, new_desc, now, project_id,
        )
    return {
        "id": project_id,
        "name": new_name,
        "description": new_desc,
        "created_at": row["created_at"].isoformat(),
        "updated_at": now.isoformat(),
    }


async def delete_project(project_id: str, team_id: str = "") -> bool:
    pool = _get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM projects WHERE id = $1 AND team_id = $2", project_id, team_id
        )
    return result == "DELETE 1"


# ---------------------------------------------------------------------------
# Project URLs
# ---------------------------------------------------------------------------

async def add_project_urls(project_id: str, urls: list[str]) -> list[dict]:
    pool = _get_pool()
    now = _now()
    ts = datetime.fromisoformat(now)
    added: list[dict] = []
    async with pool.acquire() as conn:
        for url in urls:
            uid = _id()
            try:
                await conn.execute(
                    "INSERT INTO project_urls (id, project_id, url, created_at) VALUES ($1, $2, $3, $4)",
                    uid, project_id, url, ts,
                )
                added.append({"id": uid, "url": url, "created_at": now})
            except asyncpg.UniqueViolationError:
                pass
    return added


async def remove_project_url(project_id: str, url_id: str) -> bool:
    pool = _get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM project_urls WHERE id = $1 AND project_id = $2", url_id, project_id
        )
    return result == "DELETE 1"


async def get_project_urls(project_id: str) -> list[dict]:
    pool = _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM project_urls WHERE project_id = $1 ORDER BY created_at", project_id
        )
    return [{"id": r["id"], "url": r["url"], "created_at": r["created_at"].isoformat()} for r in rows]


# ---------------------------------------------------------------------------
# Jobs (persistence)
# ---------------------------------------------------------------------------

async def save_job(
    job_id: str,
    status: str,
    progress_current: int,
    progress_total: int,
    current_url: str,
    created_at: str,
    completed_at: str | None,
    path_rules: dict | None = None,
    candidate_service_urls: list[str] | None = None,
    project_id: str | None = None,
    user_id: str | None = None,
) -> None:
    pool = _get_pool()
    created_ts = datetime.fromisoformat(created_at)
    completed_ts = datetime.fromisoformat(completed_at) if completed_at else None
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO jobs (id, project_id, user_id, status, progress_current, progress_total,
                              current_url, path_rules, candidate_service_urls, created_at, completed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT(id) DO UPDATE SET
                status=EXCLUDED.status,
                progress_current=EXCLUDED.progress_current,
                progress_total=EXCLUDED.progress_total,
                current_url=EXCLUDED.current_url,
                completed_at=EXCLUDED.completed_at
            """,
            job_id,
            project_id,
            user_id,
            status,
            progress_current,
            progress_total,
            current_url,
            path_rules,
            candidate_service_urls,
            created_ts,
            completed_ts,
        )


async def save_audit_result(job_id: str, url: str, data: dict, is_error: bool = False) -> None:
    pool = _get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO audit_results (id, job_id, url, is_error, data, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
            _id(), job_id, url, is_error, data, datetime.fromisoformat(_now()),
        )


async def get_job_from_db(job_id: str) -> dict | None:
    """Reconstruct a full job response from Postgres."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
        if not row:
            return None

        # User name
        user_name = None
        if row["user_id"]:
            user_row = await conn.fetchrow("SELECT name FROM users WHERE id = $1", row["user_id"])
            if user_row:
                user_name = user_row["name"]

        # Results
        result_rows = await conn.fetch(
            "SELECT * FROM audit_results WHERE job_id = $1 AND is_error = FALSE ORDER BY created_at",
            job_id,
        )
        results = [r["data"] for r in result_rows]

        # Errors
        error_rows = await conn.fetch(
            "SELECT * FROM audit_results WHERE job_id = $1 AND is_error = TRUE ORDER BY created_at",
            job_id,
        )
        errors = [r["data"] for r in error_rows]

    return {
        "job_id": row["id"],
        "project_id": row["project_id"],
        "status": row["status"],
        "progress": {
            "current": row["progress_current"],
            "total": row["progress_total"],
            "current_url": row["current_url"],
        },
        "results": results,
        "errors": errors,
        "created_at": row["created_at"].isoformat(),
        "completed_at": row["completed_at"].isoformat() if row["completed_at"] else None,
        "user_name": user_name,
    }


async def get_jobs_for_project(project_id: str) -> list[dict]:
    pool = _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT j.*,
                   (SELECT COUNT(*) FROM audit_results WHERE job_id = j.id AND is_error = FALSE) AS result_count,
                   (SELECT COUNT(*) FROM audit_results WHERE job_id = j.id AND is_error = TRUE) AS error_count
            FROM jobs j
            WHERE j.project_id = $1
            ORDER BY j.created_at DESC
            """,
            project_id,
        )
    return [
        {
            "id": r["id"],
            "status": r["status"],
            "progress_total": r["progress_total"],
            "result_count": r["result_count"],
            "error_count": r["error_count"],
            "created_at": r["created_at"].isoformat(),
            "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# URL history
# ---------------------------------------------------------------------------

async def get_url_history(project_id: str, url_id: str) -> dict | None:
    """All audit results for a single URL within a project."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        url_row = await conn.fetchrow(
            "SELECT url FROM project_urls WHERE id = $1 AND project_id = $2", url_id, project_id
        )
        if not url_row:
            return None
        url = url_row["url"]

        rows = await conn.fetch(
            """
            SELECT ar.data, ar.created_at, j.id AS job_id, j.created_at AS job_created_at
            FROM audit_results ar
            JOIN jobs j ON ar.job_id = j.id
            WHERE j.project_id = $1 AND ar.url = $2 AND ar.is_error = FALSE
            ORDER BY ar.created_at DESC
            """,
            project_id, url,
        )
        entries = [
            {
                "job_id": r["job_id"],
                "job_created_at": r["job_created_at"].isoformat(),
                "created_at": r["created_at"].isoformat(),
                "data": r["data"],
            }
            for r in rows
        ]
    return {"url_id": url_id, "url": url, "entries": entries}


# ---------------------------------------------------------------------------
# Approved QIDs
# ---------------------------------------------------------------------------

async def get_approved_qids(project_id: str, url: str) -> list[dict]:
    pool = _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT name, qid FROM approved_qids WHERE project_id = $1 AND url = $2",
            project_id, url,
        )
    return [{"name": r["name"], "qid": r["qid"]} for r in rows]


async def set_approved_qids(project_id: str, url: str, qids: list[dict]) -> list[dict]:
    """Replace all approved QIDs for a project+URL."""
    pool = _get_pool()
    now = datetime.fromisoformat(_now())
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM approved_qids WHERE project_id = $1 AND url = $2",
            project_id, url,
        )
        for q in qids:
            try:
                await conn.execute(
                    "INSERT INTO approved_qids (id, project_id, url, name, qid, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
                    _id(), project_id, url, q["name"], q["qid"], now,
                )
            except asyncpg.UniqueViolationError:
                pass
    return qids


async def get_all_approved_qids_for_project(project_id: str) -> dict[str, list[dict]]:
    """Get all approved QIDs grouped by URL for a project."""
    pool = _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT url, name, qid FROM approved_qids WHERE project_id = $1",
            project_id,
        )
    result: dict[str, list[dict]] = {}
    for r in rows:
        result.setdefault(r["url"], []).append({"name": r["name"], "qid": r["qid"]})
    return result
