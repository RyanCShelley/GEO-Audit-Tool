"""
SQLite persistence layer for the GEO Audit Tool.

Uses aiosqlite for async access. The database file is auto-created on
first startup at ``backend/data/geo_audit.db``.
"""

from __future__ import annotations

import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator

import aiosqlite

logger = logging.getLogger(__name__)

DB_DIR = Path(__file__).resolve().parent.parent / "data"
DB_PATH = DB_DIR / "geo_audit.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_urls (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(project_id, url)
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    project_id TEXT REFERENCES projects(id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    progress_current INTEGER NOT NULL DEFAULT 0,
    progress_total INTEGER NOT NULL DEFAULT 0,
    current_url TEXT NOT NULL DEFAULT '',
    path_rules TEXT,
    candidate_service_urls TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS audit_results (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    is_error INTEGER NOT NULL DEFAULT 0,
    data TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS approved_qids (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    name TEXT NOT NULL,
    qid TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(project_id, url, qid)
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _id() -> str:
    return uuid.uuid4().hex[:12]


@asynccontextmanager
async def _db() -> AsyncIterator[aiosqlite.Connection]:
    async with aiosqlite.connect(str(DB_PATH)) as conn:
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = aiosqlite.Row
        yield conn


async def init_db() -> None:
    """Create the data directory and all tables if they don't exist."""
    DB_DIR.mkdir(parents=True, exist_ok=True)
    async with _db() as conn:
        await conn.executescript(_SCHEMA)
        await conn.commit()
    logger.info("SQLite database initialised at %s", DB_PATH)


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------

async def create_project(name: str, description: str = "") -> dict:
    pid = _id()
    now = _now()
    async with _db() as conn:
        await conn.execute(
            "INSERT INTO projects (id, name, description, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (pid, name, description, now, now),
        )
        await conn.commit()
    return {"id": pid, "name": name, "description": description, "created_at": now, "updated_at": now}


async def list_projects() -> list[dict]:
    async with _db() as conn:
        cursor = await conn.execute(
            """
            SELECT p.*,
                   (SELECT COUNT(*) FROM project_urls WHERE project_id = p.id) AS url_count,
                   (SELECT MAX(j.created_at) FROM jobs j WHERE j.project_id = p.id) AS last_audit
            FROM projects p
            ORDER BY p.updated_at DESC
            """
        )
        rows = await cursor.fetchall()
        return [
            {
                "id": r["id"],
                "name": r["name"],
                "description": r["description"],
                "created_at": r["created_at"],
                "updated_at": r["updated_at"],
                "url_count": r["url_count"],
                "last_audit": r["last_audit"],
            }
            for r in rows
        ]


async def get_project(project_id: str) -> dict | None:
    async with _db() as conn:
        cursor = await conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
        row = await cursor.fetchone()
        if not row:
            return None

        # URLs
        cursor = await conn.execute(
            "SELECT * FROM project_urls WHERE project_id = ? ORDER BY created_at", (project_id,)
        )
        urls = [{"id": r["id"], "url": r["url"], "created_at": r["created_at"]} for r in await cursor.fetchall()]

        # Recent jobs
        cursor = await conn.execute(
            """
            SELECT id, status, progress_total, created_at, completed_at
            FROM jobs WHERE project_id = ? ORDER BY created_at DESC LIMIT 20
            """,
            (project_id,),
        )
        recent_jobs = [
            {
                "id": r["id"],
                "status": r["status"],
                "url_count": r["progress_total"],
                "created_at": r["created_at"],
                "completed_at": r["completed_at"],
            }
            for r in await cursor.fetchall()
        ]

        return {
            "id": row["id"],
            "name": row["name"],
            "description": row["description"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "urls": urls,
            "recent_jobs": recent_jobs,
        }


async def update_project(project_id: str, name: str | None = None, description: str | None = None) -> dict | None:
    async with _db() as conn:
        cursor = await conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
        row = await cursor.fetchone()
        if not row:
            return None
        new_name = name if name is not None else row["name"]
        new_desc = description if description is not None else row["description"]
        now = _now()
        await conn.execute(
            "UPDATE projects SET name = ?, description = ?, updated_at = ? WHERE id = ?",
            (new_name, new_desc, now, project_id),
        )
        await conn.commit()
        return {"id": project_id, "name": new_name, "description": new_desc, "created_at": row["created_at"], "updated_at": now}


async def delete_project(project_id: str) -> bool:
    async with _db() as conn:
        cursor = await conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))
        await conn.commit()
        return cursor.rowcount > 0


# ---------------------------------------------------------------------------
# Project URLs
# ---------------------------------------------------------------------------

async def add_project_urls(project_id: str, urls: list[str]) -> list[dict]:
    now = _now()
    added: list[dict] = []
    async with _db() as conn:
        for url in urls:
            uid = _id()
            try:
                await conn.execute(
                    "INSERT INTO project_urls (id, project_id, url, created_at) VALUES (?, ?, ?, ?)",
                    (uid, project_id, url, now),
                )
                added.append({"id": uid, "url": url, "created_at": now})
            except aiosqlite.IntegrityError:
                # duplicate — skip
                pass
        await conn.commit()
    return added


async def remove_project_url(project_id: str, url_id: str) -> bool:
    async with _db() as conn:
        cursor = await conn.execute(
            "DELETE FROM project_urls WHERE id = ? AND project_id = ?", (url_id, project_id)
        )
        await conn.commit()
        return cursor.rowcount > 0


async def get_project_urls(project_id: str) -> list[dict]:
    async with _db() as conn:
        cursor = await conn.execute(
            "SELECT * FROM project_urls WHERE project_id = ? ORDER BY created_at", (project_id,)
        )
        return [{"id": r["id"], "url": r["url"], "created_at": r["created_at"]} for r in await cursor.fetchall()]


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
) -> None:
    async with _db() as conn:
        await conn.execute(
            """
            INSERT INTO jobs (id, project_id, status, progress_current, progress_total,
                              current_url, path_rules, candidate_service_urls, created_at, completed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                status=excluded.status,
                progress_current=excluded.progress_current,
                progress_total=excluded.progress_total,
                current_url=excluded.current_url,
                completed_at=excluded.completed_at
            """,
            (
                job_id,
                project_id,
                status,
                progress_current,
                progress_total,
                current_url,
                json.dumps(path_rules) if path_rules else None,
                json.dumps(candidate_service_urls) if candidate_service_urls else None,
                created_at,
                completed_at,
            ),
        )
        await conn.commit()


async def save_audit_result(job_id: str, url: str, data: dict, is_error: bool = False) -> None:
    async with _db() as conn:
        await conn.execute(
            "INSERT INTO audit_results (id, job_id, url, is_error, data, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (_id(), job_id, url, int(is_error), json.dumps(data), _now()),
        )
        await conn.commit()


async def get_job_from_db(job_id: str) -> dict | None:
    """Reconstruct a full job response from SQLite."""
    async with _db() as conn:
        cursor = await conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        row = await cursor.fetchone()
        if not row:
            return None

        # Results
        cursor = await conn.execute(
            "SELECT * FROM audit_results WHERE job_id = ? AND is_error = 0 ORDER BY created_at",
            (job_id,),
        )
        results = [json.loads(r["data"]) for r in await cursor.fetchall()]

        # Errors
        cursor = await conn.execute(
            "SELECT * FROM audit_results WHERE job_id = ? AND is_error = 1 ORDER BY created_at",
            (job_id,),
        )
        errors = [json.loads(r["data"]) for r in await cursor.fetchall()]

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
            "created_at": row["created_at"],
            "completed_at": row["completed_at"],
        }


async def get_jobs_for_project(project_id: str) -> list[dict]:
    async with _db() as conn:
        cursor = await conn.execute(
            """
            SELECT j.*,
                   (SELECT COUNT(*) FROM audit_results WHERE job_id = j.id AND is_error = 0) AS result_count,
                   (SELECT COUNT(*) FROM audit_results WHERE job_id = j.id AND is_error = 1) AS error_count
            FROM jobs j
            WHERE j.project_id = ?
            ORDER BY j.created_at DESC
            """,
            (project_id,),
        )
        return [
            {
                "id": r["id"],
                "status": r["status"],
                "progress_total": r["progress_total"],
                "result_count": r["result_count"],
                "error_count": r["error_count"],
                "created_at": r["created_at"],
                "completed_at": r["completed_at"],
            }
            for r in await cursor.fetchall()
        ]


# ---------------------------------------------------------------------------
# URL history
# ---------------------------------------------------------------------------

async def get_url_history(project_id: str, url_id: str) -> dict | None:
    """All audit results for a single URL within a project."""
    async with _db() as conn:
        # Look up the URL string
        cursor = await conn.execute(
            "SELECT url FROM project_urls WHERE id = ? AND project_id = ?", (url_id, project_id)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        url = row["url"]

        # Find all results for this URL in jobs belonging to this project
        cursor = await conn.execute(
            """
            SELECT ar.data, ar.created_at, j.id AS job_id, j.created_at AS job_created_at
            FROM audit_results ar
            JOIN jobs j ON ar.job_id = j.id
            WHERE j.project_id = ? AND ar.url = ? AND ar.is_error = 0
            ORDER BY ar.created_at DESC
            """,
            (project_id, url),
        )
        entries = []
        for r in await cursor.fetchall():
            data = json.loads(r["data"])
            entries.append({
                "job_id": r["job_id"],
                "job_created_at": r["job_created_at"],
                "created_at": r["created_at"],
                "data": data,
            })
        return {"url_id": url_id, "url": url, "entries": entries}


# ---------------------------------------------------------------------------
# Approved QIDs
# ---------------------------------------------------------------------------

async def get_approved_qids(project_id: str, url: str) -> list[dict]:
    async with _db() as conn:
        cursor = await conn.execute(
            "SELECT name, qid FROM approved_qids WHERE project_id = ? AND url = ?",
            (project_id, url),
        )
        return [{"name": r["name"], "qid": r["qid"]} for r in await cursor.fetchall()]


async def set_approved_qids(project_id: str, url: str, qids: list[dict]) -> list[dict]:
    """Replace all approved QIDs for a project+URL."""
    now = _now()
    async with _db() as conn:
        await conn.execute(
            "DELETE FROM approved_qids WHERE project_id = ? AND url = ?",
            (project_id, url),
        )
        for q in qids:
            try:
                await conn.execute(
                    "INSERT INTO approved_qids (id, project_id, url, name, qid, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (_id(), project_id, url, q["name"], q["qid"], now),
                )
            except aiosqlite.IntegrityError:
                pass
        await conn.commit()
    return qids


async def get_all_approved_qids_for_project(project_id: str) -> dict[str, list[dict]]:
    """Get all approved QIDs grouped by URL for a project."""
    async with _db() as conn:
        cursor = await conn.execute(
            "SELECT url, name, qid FROM approved_qids WHERE project_id = ?",
            (project_id,),
        )
        result: dict[str, list[dict]] = {}
        for r in await cursor.fetchall():
            result.setdefault(r["url"], []).append({"name": r["name"], "qid": r["qid"]})
        return result
