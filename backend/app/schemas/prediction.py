from __future__ import annotations

from typing import Any

from pydantic import Field, model_validator

from .common import ID, JobStatus, NonNegativeInt, Probability, SchemaModel, Timestamp
from .modeling import MetricSet
from .query import QuerySpecPatch


class PredictRequest(SchemaModel):
    model_ids: list[ID] = Field(min_length=1)
    dataset_id: ID
    query: QuerySpecPatch | None = None

    @model_validator(mode="after")
    def _validate_unique_model_ids(self) -> "PredictRequest":
        if len(set(self.model_ids)) != len(self.model_ids):
            raise ValueError("model_ids must be unique")
        return self


class PredictionRow(SchemaModel):
    row_index: NonNegativeInt
    prediction: dict[str, Any]
    class_scores: dict[str, float] | None = None
    confidence: Probability | None = None
    y_true: dict[str, Any] | None = None


class PredictionResult(SchemaModel):
    model_id: ID
    predictions: list[PredictionRow] = Field(default_factory=list)
    metrics: MetricSet | None = None


class PredictResponse(SchemaModel):
    results: list[PredictionResult] = Field(default_factory=list)


class PredictionJob(SchemaModel):
    id: ID
    status: JobStatus
    request: PredictRequest
    created_at: Timestamp
    started_at: Timestamp | None = None
    finished_at: Timestamp | None = None
    error: str | None = None
    results: list[PredictionResult] | None = None
