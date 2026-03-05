from enum import Enum

from pydantic import Field

from .common import ID, NonNegativeInt, PageLimit, SchemaModel, Timestamp


class ColumnDType(str, Enum):
    INT = "int"
    FLOAT = "float"
    STRING = "string"
    BOOL = "bool"
    DATETIME = "datetime"
    UNKNOWN = "unknown"


class ColumnInfo(SchemaModel):
    name: str = Field(min_length=1)
    dtype: ColumnDType
    nullable: bool
    unique_count: NonNegativeInt | None = None
    null_count: NonNegativeInt | None = None


class NumericSummary(SchemaModel):
    min: float
    max: float
    mean: float
    std: float


class ColumnStats(ColumnInfo):
    summary: NumericSummary | None = None


class Dataset(SchemaModel):
    id: ID
    name: str = Field(min_length=1)
    filename: str = Field(min_length=1)
    row_count: NonNegativeInt
    column_count: NonNegativeInt
    columns: list[ColumnInfo] = Field(default_factory=list)
    created_at: Timestamp


class DatasetStats(SchemaModel):
    dataset_id: ID
    row_count: NonNegativeInt
    column_count: NonNegativeInt
    columns: list[ColumnStats] = Field(default_factory=list)


class DatasetListResponse(SchemaModel):
    items: list[Dataset] = Field(default_factory=list)
    total: NonNegativeInt
    limit: PageLimit = 50
    offset: NonNegativeInt = 0
