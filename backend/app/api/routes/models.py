from pathlib import Path
from sqlalchemy.orm import Session


from fastapi import APIRouter, Depends, Query

from backend.app.db.repositories import (
    list_model_artifacts,
    count_model_artifacts,
    model_artifact_to_schema,
    create_model_job,
)
from backend.app.db.session import get_db
from backend.app.schemas import (
    ModelArtifactListResponse,
    ModelType,
    TrainModelRequest,
    ModelJob,
    JobStatus,
)

router = APIRouter(prefix="/v1", tags=["models"])

DATASET_STORAGE_DIR = Path(__file__).resolve().parents[3] / "data" / "datasets"
DATASET_STORAGE_DIR.mkdir(parents=True, exist_ok=True)

#================|
# Model ARTIFACTS|
#================|
@router.get("/models", response_model=ModelArtifactListResponse)
def list_models_endpoint(
        dataset_id : int | None = Query(default=None, ge=1),
        model_type: ModelType | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
        db: Session = Depends(get_db)
) -> ModelArtifactListResponse:
    models = list_model_artifacts(db, dataset_id=dataset_id, limit=limit, offset=offset)
    total = count_model_artifacts(db)
    return ModelArtifactListResponse(
        items=[model_artifact_to_schema(model) for model in models],
        total=total,
        limit=limit,
        offset=offset,
    )

#============|
# Model JOBS |
#============|

@router.post("/model-jobs", response_model=ModelJob)
def create_model_job_endpoint(
        request: TrainModelRequest,
        db : Session = Depends(get_db)
) -> ModelJob:
    return create_model_job(
        db,
        status=JobStatus.QUEUED.value,
        request=request.model_dump(mode="python"),
        dataset_id=request.dataset_id,
    )