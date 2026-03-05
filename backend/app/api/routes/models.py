from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.orm import Session

from ..errors import ApiException
from ...db.repositories import (
    cancel_model_job,
    count_model_artifacts,
    create_model_job,
    delete_model_artifact,
    get_model_artifact,
    get_model_job,
    list_model_artifacts,
    model_artifact_to_schema,
    model_job_to_schema,
)
from ...db.session import get_db
from ...schemas import (
    JobStatus,
    ModelArtifact,
    ModelArtifactListResponse,
    ModelJob,
    ModelJobLogsResponse,
    ModelType,
    TrainModelRequest,
)

router = APIRouter(prefix="/v1", tags=["models"])


@router.get(path="/models", response_model=ModelArtifactListResponse, status_code=status.HTTP_200_OK)
def list_models_endpoint(
    dataset_id: int | None = Query(default=None, ge=1),
    model_type: ModelType | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> ModelArtifactListResponse:
    _ = model_type  # not supported in repository yet
    models = list_model_artifacts(db, dataset_id=dataset_id, limit=limit, offset=offset)
    total = count_model_artifacts(db, dataset_id=dataset_id)
    return ModelArtifactListResponse(
        items=[model_artifact_to_schema(model) for model in models],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/models/{model_id}", response_model=ModelArtifact, status_code=status.HTTP_200_OK)
def get_model_endpoint(
    model_id: int,
    db: Session = Depends(get_db),
) -> ModelArtifact:
    model = get_model_artifact(db, model_id=model_id)
    if model is None:
        raise ApiException(
            status_code=404,
            code="MODEL_NOT_FOUND",
            message=f"Model {model_id} was not found",
        )
    return model_artifact_to_schema(model)


@router.delete("/models/{model_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_model_endpoint(
    model_id: int,
    db: Session = Depends(get_db),
) -> Response:
    deleted = delete_model_artifact(db, model_id=model_id)
    if not deleted:
        raise ApiException(
            status_code=404,
            code="MODEL_NOT_FOUND",
            message=f"Model {model_id} was not found",
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/model-jobs", response_model=ModelJob, status_code=status.HTTP_202_ACCEPTED)
def create_model_job_endpoint(
    request: TrainModelRequest,
    db: Session = Depends(get_db),
) -> ModelJob:
    job = create_model_job(
        db,
        status=JobStatus.QUEUED.value,
        request=request.model_dump(mode="python"),
        dataset_id=request.dataset_id,
    )
    return model_job_to_schema(job)


@router.get("/model-jobs/{job_id}", response_model=ModelJob, status_code=status.HTTP_200_OK)
def get_model_job_endpoint(
    job_id: int,
    db: Session = Depends(get_db),
) -> ModelJob:
    job = get_model_job(db, job_id=job_id)
    if job is None:
        raise ApiException(
            status_code=404,
            code="MODEL_JOB_NOT_FOUND",
            message=f"Model job {job_id} was not found",
        )
    return model_job_to_schema(job)


@router.get(
    "/model-jobs/{job_id}/logs",
    response_model=ModelJobLogsResponse,
    status_code=status.HTTP_200_OK,
)
def get_model_job_logs_endpoint(
    job_id: int,
    tail: int = Query(default=200, ge=1, le=1000),
    since_index: int | None = Query(default=None, ge=0),
    db: Session = Depends(get_db),
) -> ModelJobLogsResponse:
    job = get_model_job(db, job_id=job_id)
    if job is None:
        raise ApiException(
            status_code=404,
            code="MODEL_JOB_NOT_FOUND",
            message=f"Model job {job_id} was not found",
        )

    all_logs = list(job.logs or [])
    total_logs = len(all_logs)

    if since_index is None:
        start = max(total_logs - tail, 0)
    else:
        start = min(since_index, total_logs)

    end = min(start + tail, total_logs)
    return ModelJobLogsResponse(
        job_id=job_id,
        logs=all_logs[start:end],
        next_index=end,
    )

@router.post("/model-jobs/{job_id}/cancel", response_model=ModelJob, status_code=status.HTTP_200_OK)
def cancel_model_job_endpoint(
    job_id: int,
    db: Session = Depends(get_db),
) -> ModelJob:
    job = get_model_job(db, job_id=job_id)

    if job is None:
        raise ApiException(
            status_code=404,
            code="MODEL_JOB_NOT_FOUND",
            message=f"Model job {job_id} was not found",
        )
    canceled_job = cancel_model_job(db, job_id=job_id)
    if canceled_job is None:
        raise ApiException(
            status_code=409,
            code="MODEL_JOB_NOT_CANCELED",
            message=f"Model job {job_id} with status {job.status} could not be cancelled",
        )
    return model_job_to_schema(canceled_job)
