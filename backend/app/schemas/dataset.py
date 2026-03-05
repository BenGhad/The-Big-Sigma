from enum import Enum

from pydantic import Field, model_validator

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
    y_columns: list[str] = Field(default_factory=list)
    is_time_series: bool = False
    created_at: Timestamp

    @model_validator(mode="after")
    def _validate_y_columns(self) -> "Dataset":
        cleaned = [name.strip() for name in self.y_columns]
        if any(not name for name in cleaned):
            raise ValueError("y_columns entries must be non-empty")
        if len(set(cleaned)) != len(cleaned):
            raise ValueError("y_columns entries must be unique")
        self.y_columns = cleaned
        return self


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


class DatasetSettings(SchemaModel):
    dataset_id: ID
    y_columns: list[str] = Field(default_factory=list)
    is_time_series: bool = False

    @model_validator(mode="after")
    def _validate_y_columns(self) -> "DatasetSettings":
        cleaned = [name.strip() for name in self.y_columns]
        if any(not name for name in cleaned):
            raise ValueError("y_columns entries must be non-empty")
        if len(set(cleaned)) != len(cleaned):
            raise ValueError("y_columns entries must be unique")
        self.y_columns = cleaned
        return self


class DatasetSettingsUpdate(SchemaModel):
    y_columns: list[str] | None = None
    is_time_series: bool | None = None

    @model_validator(mode="after")
    def _validate_update(self) -> "DatasetSettingsUpdate":
        if self.y_columns is None and self.is_time_series is None:
            raise ValueError("at least one settings field must be provided")
        if self.y_columns is not None:
            cleaned = [name.strip() for name in self.y_columns]
            if any(not name for name in cleaned):
                raise ValueError("y_columns entries must be non-empty")
            if len(set(cleaned)) != len(cleaned):
                raise ValueError("y_columns entries must be unique")
            self.y_columns = cleaned
        return self
