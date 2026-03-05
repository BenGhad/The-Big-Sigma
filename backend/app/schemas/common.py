from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Annotated

ID = Annotated[int, Field(ge=1)]
PositiveInt = Annotated[int, Field(ge=1)]
NonNegativeInt = Annotated[int, Field(ge=0)]
PageLimit = Annotated[int, Field(ge=1, le=1000)]
Probability = Annotated[float, Field(ge=0.0, le=1.0)]
Timestamp = datetime


class SchemaModel(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

# Todo generalize status FSM
"""
 Queued -> Running ->   Completed*
        -> Canceled*    Failed*
                        Canceled*
"""
class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class ErrorBody(SchemaModel):
    code: str
    message: str
    details: Any | None = None


class ApiError(SchemaModel):
    error: ErrorBody
