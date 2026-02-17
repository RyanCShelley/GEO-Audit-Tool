"""
Hybrid async job store — in-memory for running jobs, SQLite for persistence.

Running / pending jobs live in ``_jobs`` so the polling endpoint is fast.
On completion the full job (results + errors) is flushed to SQLite so it
survives restarts.  ``get_job`` falls back to the database when the id
isn't found in memory.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Job:
    id: str
    urls: list[str]
    status: JobStatus = JobStatus.PENDING
    progress_current: int = 0
    progress_total: int = 0
    current_url: str = ""
    results: list[dict] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)
    created_at: str = ""
    completed_at: str | None = None
    path_rules: dict[str, int] | None = None
    candidate_service_urls: list[str] | None = None
    project_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.id,
            "project_id": self.project_id,
            "status": self.status.value,
            "progress": {
                "current": self.progress_current,
                "total": self.progress_total,
                "current_url": self.current_url,
            },
            "results": self.results,
            "errors": self.errors,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
        }


# In-memory store — keyed by job_id
_jobs: dict[str, Job] = {}


def create_job(
    urls: list[str],
    path_rules: dict[str, int] | None = None,
    candidate_service_urls: list[str] | None = None,
    project_id: str | None = None,
) -> Job:
    job_id = uuid.uuid4().hex[:12]
    job = Job(
        id=job_id,
        urls=urls,
        progress_total=len(urls),
        created_at=datetime.now(timezone.utc).isoformat(),
        path_rules=path_rules,
        candidate_service_urls=candidate_service_urls,
        project_id=project_id,
    )
    _jobs[job_id] = job
    return job


def get_job(job_id: str) -> Job | None:
    return _jobs.get(job_id)


async def get_job_or_db(job_id: str) -> dict | None:
    """Return job dict — from memory if running, otherwise from SQLite."""
    job = _jobs.get(job_id)
    if job:
        return job.to_dict()
    # Fall back to database
    from . import db
    return await db.get_job_from_db(job_id)


async def _flush_to_db(job: Job) -> None:
    """Persist a completed/failed job and its results to SQLite."""
    from . import db

    await db.save_job(
        job_id=job.id,
        status=job.status.value,
        progress_current=job.progress_current,
        progress_total=job.progress_total,
        current_url=job.current_url,
        created_at=job.created_at,
        completed_at=job.completed_at,
        path_rules=job.path_rules,
        candidate_service_urls=job.candidate_service_urls,
        project_id=job.project_id,
    )

    for result in job.results:
        await db.save_audit_result(job.id, result.get("url", ""), result, is_error=False)

    for error in job.errors:
        await db.save_audit_result(job.id, error.get("url", ""), error, is_error=True)

    logger.info("Job %s flushed to SQLite (%d results, %d errors)", job.id, len(job.results), len(job.errors))


async def run_audit_job(job: Job) -> None:
    """Run the audit for each URL in the job. Updates job in-place."""
    from .auditor.service import audit_single_url
    from . import db

    job.status = JobStatus.RUNNING

    # If this job belongs to a project, auto-load approved QIDs
    project_qids: dict[str, list[dict]] = {}
    if job.project_id:
        project_qids = await db.get_all_approved_qids_for_project(job.project_id)

    for i, url in enumerate(job.urls):
        job.progress_current = i + 1
        job.current_url = url

        # Check if there are pre-approved QIDs for this URL
        approved_qids = project_qids.get(url)

        try:
            result = await audit_single_url(
                url,
                path_rules=job.path_rules,
                candidate_service_urls=job.candidate_service_urls,
                approved_qids=approved_qids,
            )
            if "error" in result:
                job.errors.append({
                    "url": url,
                    "stage": result.get("stage", "unknown"),
                    "message": result["error"],
                })
            else:
                job.results.append(result)
        except Exception as e:
            logger.exception("Unexpected error auditing %s", url)
            job.errors.append({
                "url": url,
                "stage": "unknown",
                "message": str(e),
            })

    job.current_url = ""
    job.completed_at = datetime.now(timezone.utc).isoformat()
    job.status = JobStatus.COMPLETED if job.results else JobStatus.FAILED

    # Flush to SQLite
    try:
        await _flush_to_db(job)
    except Exception:
        logger.exception("Failed to flush job %s to SQLite", job.id)
