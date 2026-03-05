from __future__ import annotations

from sqlalchemy.orm import Session

from ..schemas import PredictionJob
from ..schemas.prediction import PredictRequest, PredictResponse


class PredictionServiceError(Exception):
    """Base class for domain-level prediction service errors."""


class DatasetNotFoundError(PredictionServiceError):
    def __init__(self, *, dataset_id: int) -> None:
        self.dataset_id = dataset_id
        super().__init__(f"Dataset {dataset_id} was not found")


class PredictSyncLimitError(PredictionServiceError):
    def __init__(
        self,
        *,
        max_rows: int,
        max_models: int,
        requested_rows: int,
        requested_models: int,
    ) -> None:
        self.max_rows = max_rows
        self.max_models = max_models
        self.requested_rows = requested_rows
        self.requested_models = requested_models
        super().__init__("Request exceeds synchronous prediction limits")


class ModelNotFoundError(PredictionServiceError):
    def __init__(self, *, model_id: int) -> None:
        self.model_id = model_id
        super().__init__(f"Model {model_id} was not found")


class ModelDatasetMismatchError(PredictionServiceError):
    def __init__(self, *, model_id: int, dataset_id: int) -> None:
        self.model_id = model_id
        self.dataset_id = dataset_id
        super().__init__(f"Model {model_id} does not belong to dataset {dataset_id}")


class PredictMissingXColsError(PredictionServiceError):
    def __init__(self, *, model_id: int, dataset_id: int, missing_x_cols: list[str]) -> None:
        self.model_id = model_id
        self.dataset_id = dataset_id
        self.missing_x_cols = missing_x_cols
        super().__init__(
            f"Dataset {dataset_id} is missing required x_cols for model {model_id}"
        )

#==============|
#SYNC predicts |
#==============|


def run_sync_prediction(
    *,
    db: Session,
    request: PredictRequest,
) -> PredictResponse:
    """
    Run synchronous predictions and return API response data.

    Raises:
        DatasetNotFoundError
        PredictSyncLimitError
        ModelNotFoundError
        ModelDatasetMismatchError
        PredictMissingXColsError
    """
    _ = db, request
    raise NotImplementedError("run_sync_prediction is not implemented yet")


#================|
# ASYNC predicts |
#================|

def run_async_prediction(
        *,
        db: Session,
        request: PredictRequest,
) -> PredictionJob:
    """
    Run async predictions and return API response data.

    Raises:
        DatasetNotFoundError
        ModelNotFoundError
        ModelDatasetMismatchError
        PredictMissingXColsError
    """
    raise NotImplementedError("run_async_prediction is not implemented yet")