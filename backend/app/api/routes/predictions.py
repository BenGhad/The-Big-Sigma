from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from ..errors import ApiException
from ...db.repositories import (
    cancel_prediction_job,
    create_prediction_job,
    get_prediction_job as get_prediction_job_repo,
    list_prediction_jobs as list_prediction_jobs_repo,
    prediction_job_to_schema,
)
from ...db.session import get_db
from ...schemas import JobStatus, PredictionJob
from ...schemas.prediction import PredictRequest, PredictResponse, PredictionResult
from ...services.predictions import (
    DatasetNotFoundError,
    ModelDatasetMismatchError,
    ModelNotFoundError,
    PredictMissingXColsError,
    PredictSyncLimitError,
    run_sync_prediction,
)

router = APIRouter(prefix="/v1", tags=["predictions"])


@router.get("/prediction-jobs", response_model=list[PredictionJob], status_code=status.HTTP_200_OK)
def list_prediction_jobs_endpoint(
    dataset_id: int | None = Query(default=None, ge=1),
    job_status: JobStatus | None = Query(default=None, alias="status"),
    limit: int = Query(default=50, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> list[PredictionJob]:
    jobs = list_prediction_jobs_repo(
        db,
        dataset_id=dataset_id,
        status=job_status.value if job_status else None,
        limit=limit,
        offset=offset,
    )
    return [prediction_job_to_schema(job) for job in jobs]

#===============|
# SYNC predicts |
#===============|

@router.post("/predict", response_model=PredictResponse, status_code=status.HTTP_200_OK)
def predict_sync_endpoint(
    request: PredictRequest,
    db: Session = Depends(get_db),
) -> PredictResponse:
    try:
        return run_sync_prediction(db=db, request=request)
    except DatasetNotFoundError as exc:
        raise ApiException(
            status_code=404,
            code="DATASET_NOT_FOUND",
            message=f"Dataset {exc.dataset_id} was not found",
        )
    except ModelNotFoundError as exc:
        raise ApiException(
            status_code=404,
            code="MODEL_NOT_FOUND",
            message=f"Model {exc.model_id} was not found",
        )
    except PredictSyncLimitError as exc:
        raise ApiException(
            status_code=409,
            code="PREDICT_SYNC_LIMIT",
            message="Request exceeds synchronous prediction limits; use /v1/prediction-jobs",
            details={
                "max_rows": exc.max_rows,
                "max_models": exc.max_models,
                "requested_rows": exc.requested_rows,
                "requested_models": exc.requested_models,
            },
        )
    except ModelDatasetMismatchError as exc:
        raise ApiException(
            status_code=422,
            code="MODEL_DATASET_MISMATCH",
            message=f"Model {exc.model_id} does not belong to dataset {exc.dataset_id}",
        )
    except PredictMissingXColsError as exc:
        raise ApiException(
            status_code=422,
            code="PREDICT_MISSING_X_COLS",
            message=f"Dataset {exc.dataset_id} is missing required x_cols for model {exc.model_id}",
            details={"missing_x_cols": exc.missing_x_cols},
        )
    except NotImplementedError:
        raise ApiException(
            status_code=501,
            code="PREDICTION_NOT_IMPLEMENTED",
            message="Prediction service is not implemented yet",
        )


#================|
# ASYNC predicts |
#================|

@router.post("/prediction-jobs", response_model=PredictionJob, status_code=status.HTTP_202_ACCEPTED)
def create_prediction_job_endpoint(
    request: PredictRequest,
    db: Session = Depends(get_db),
) -> PredictionJob:
    job = create_prediction_job(
        db,
        status=JobStatus.QUEUED.value,
        request=request.model_dump(mode="python"),
        dataset_id=request.dataset_id,
    )
    return prediction_job_to_schema(job)


@router.get("/prediction-jobs/{job_id}", response_model=PredictionJob, status_code=status.HTTP_200_OK)
def get_prediction_job_endpoint(
    job_id: int,
    db: Session = Depends(get_db),
) -> PredictionJob:
    job = get_prediction_job_repo(db, job_id=job_id)
    if job is None:
        raise ApiException(
            status_code=404,
            code="PREDICTION_JOB_NOT_FOUND",
            message=f"Prediction job {job_id} was not found",
        )
    return prediction_job_to_schema(job)


@router.get(
    "/prediction-jobs/{job_id}/results",
    response_model=list[PredictionResult],
    status_code=status.HTTP_200_OK,
)
def get_prediction_job_results_endpoint(
    job_id: int,
    db: Session = Depends(get_db),
) -> list[PredictionResult]:
    job = get_prediction_job_repo(db, job_id=job_id)
    if job is None:
        raise ApiException(
            status_code=404,
            code="PREDICTION_JOB_NOT_FOUND",
            message=f"Prediction job {job_id} was not found",
        )

    job_schema = prediction_job_to_schema(job)
    if job_schema.status != JobStatus.COMPLETED:
        raise ApiException(
            status_code=409,
            code="PREDICTION_JOB_NOT_COMPLETED",
            message=f"Prediction job {job_id} is not completed",
            details={"status": job_schema.status.value},
        )

    return list(job_schema.results or [])


@router.post(
    "/prediction-jobs/{job_id}/cancel",
    response_model=PredictionJob,
    status_code=status.HTTP_200_OK,
)
def cancel_prediction_job_endpoint(
    job_id: int,
    db: Session = Depends(get_db),
) -> PredictionJob:
    job = get_prediction_job_repo(db, job_id=job_id)
    if job is None:
        raise ApiException(
            status_code=404,
            code="PREDICTION_JOB_NOT_FOUND",
            message=f"Prediction job {job_id} was not found",
        )

    canceled_job = cancel_prediction_job(db, job_id=job_id)
    if canceled_job is None:
        raise ApiException(
            status_code=409,
            code="PREDICTION_JOB_NOT_CANCELED",
            message=f"Prediction job {job_id} with status {job.status} could not be cancelled",
        )
    return prediction_job_to_schema(canceled_job)
