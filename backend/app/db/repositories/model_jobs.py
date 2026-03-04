from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ...schemas.modeling import ModelJob as ModelJobSchema
from ..models import ModelJob


def create_model_job(
    db: Session,
    *,
    status: str,
    request: dict,
    dataset_id: int,
) -> ModelJob:
    job = ModelJob(status=status, request=request, dataset_id=dataset_id)
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def get_model_job(db: Session, job_id: int) -> ModelJob | None:
    return db.get(ModelJob, job_id)


def list_model_jobs(
    db: Session,
    *,
    dataset_id: int | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[ModelJob]:
    stmt = select(ModelJob)
    if dataset_id is not None:
        stmt = stmt.where(ModelJob.dataset_id == dataset_id)
    if status is not None:
        stmt = stmt.where(ModelJob.status == status)
    stmt = stmt.order_by(ModelJob.id.desc()).offset(offset).limit(limit)
    return list(db.scalars(stmt))


def count_model_jobs(
    db: Session,
    *,
    dataset_id: int | None = None,
    status: str | None = None,
) -> int:
    stmt = select(func.count()).select_from(ModelJob)
    if dataset_id is not None:
        stmt = stmt.where(ModelJob.dataset_id == dataset_id)
    if status is not None:
        stmt = stmt.where(ModelJob.status == status)
    return int(db.scalar(stmt) or 0)


def set_model_job_status(
    db: Session,
    *,
    job_id: int,
    status: str,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    progress: float | None = None,
    error: str | None = None,
    model_id: int | None = None,
) -> ModelJob | None:
    job = db.get(ModelJob, job_id)
    if job is None:
        return None
    job.status = status
    if started_at is not None:
        job.started_at = started_at
    if finished_at is not None:
        job.finished_at = finished_at
    if progress is not None:
        job.progress = progress
    if error is not None:
        job.error = error
    if model_id is not None:
        job.model_id = model_id
    db.commit()
    db.refresh(job)
    return job


def append_model_job_log(db: Session, *, job_id: int, message: str) -> ModelJob | None:
    job = db.get(ModelJob, job_id)
    if job is None:
        return None
    logs = list(job.logs or [])
    logs.append(message)
    job.logs = logs
    db.commit()
    db.refresh(job)
    return job


def delete_model_job(db: Session, job_id: int) -> bool:
    job = db.get(ModelJob, job_id)
    if job is None:
        return False
    db.delete(job)
    db.commit()
    return True


def model_job_to_schema(job: ModelJob) -> ModelJobSchema:
    return ModelJobSchema.model_validate(
        {
            "id": job.id,
            "status": job.status,
            "request": job.request,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "progress": job.progress,
            "logs": job.logs,
            "error": job.error,
            "model_id": job.model_id,
        }
    )
