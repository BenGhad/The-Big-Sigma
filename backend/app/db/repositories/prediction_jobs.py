from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ...schemas.prediction import PredictionJob as PredictionJobSchema
from ..models import PredictionJob


def create_prediction_job(
    db: Session,
    *,
    status: str,
    request: dict,
    dataset_id: int,
) -> PredictionJob:
    job = PredictionJob(status=status, request=request, dataset_id=dataset_id)
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def get_prediction_job(db: Session, job_id: int) -> PredictionJob | None:
    return db.get(PredictionJob, job_id)


def list_prediction_jobs(
    db: Session,
    *,
    dataset_id: int | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[PredictionJob]:
    stmt = select(PredictionJob)
    if dataset_id is not None:
        stmt = stmt.where(PredictionJob.dataset_id == dataset_id)
    if status is not None:
        stmt = stmt.where(PredictionJob.status == status)
    stmt = stmt.order_by(PredictionJob.id.desc()).offset(offset).limit(limit)
    return list(db.scalars(stmt))


def count_prediction_jobs(
    db: Session,
    *,
    dataset_id: int | None = None,
    status: str | None = None,
) -> int:
    stmt = select(func.count()).select_from(PredictionJob)
    if dataset_id is not None:
        stmt = stmt.where(PredictionJob.dataset_id == dataset_id)
    if status is not None:
        stmt = stmt.where(PredictionJob.status == status)
    return int(db.scalar(stmt) or 0)


def set_prediction_job_status(
    db: Session,
    *,
    job_id: int,
    status: str,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    error: str | None = None,
    results: list[dict] | None = None,
) -> PredictionJob | None:
    job = db.get(PredictionJob, job_id)
    if job is None:
        return None
    job.status = status
    if started_at is not None:
        job.started_at = started_at
    if finished_at is not None:
        job.finished_at = finished_at
    if error is not None:
        job.error = error
    if results is not None:
        job.results = results
    db.commit()
    db.refresh(job)
    return job


def delete_prediction_job(db: Session, job_id: int) -> bool:
    job = db.get(PredictionJob, job_id)
    if job is None:
        return False
    db.delete(job)
    db.commit()
    return True


def prediction_job_to_schema(job: PredictionJob) -> PredictionJobSchema:
    return PredictionJobSchema.model_validate(
        {
            "id": job.id,
            "status": job.status,
            "request": job.request,
            "created_at": job.created_at,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "error": job.error,
            "results": job.results,
        }
    )
