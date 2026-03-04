from __future__ import annotations

from enum import Enum
from typing import Annotated, Literal

from pydantic import Field, model_validator

from .common import ID, JobStatus, NonNegativeInt, Probability, SchemaModel, Timestamp
from .query import QuerySpecPatch


class ModelType(str, Enum):
    LINEAR_REGRESSION = "linear_regression"
    RIDGE_REGRESSION = "ridge_regression"
    LASSO_REGRESSION = "lasso_regression"
    LOGISTIC_REGRESSION = "logistic_regression"
    SOFTMAX_REGRESSION = "softmax_regression"


class ClosedFormSolver(str, Enum):
    NORMAL_EQUATION = "normal_equation"
    QR = "qr"
    SVD = "svd"


class BatchMode(str, Enum):
    FULL_BATCH = "full_batch"
    SGD = "sgd"
    MINI_BATCH = "mini_batch"


class Optimizer(str, Enum):
    PLAIN = "plain"
    MOMENTUM = "momentum"
    NESTEROV = "nesterov"
    ADAGRAD = "adagrad"
    RMSPROP = "rmsprop"
    ADAM = "adam"


class ClosedFormTrainType(SchemaModel):
    kind: Literal["closed_form"]
    solver: ClosedFormSolver | None = None


class FirstOrderGradientDescentTrainType(SchemaModel):
    kind: Literal["first_order_gd"]
    batch_mode: BatchMode
    batch_size: int | None = Field(default=None, ge=1)
    optimizer: Optimizer

    @model_validator(mode="after")
    def _validate_batch_mode(self) -> "FirstOrderGradientDescentTrainType":
        if self.batch_mode == BatchMode.MINI_BATCH:
            if self.batch_size is None or self.batch_size < 2:
                raise ValueError("mini_batch requires batch_size >= 2")
        elif self.batch_mode == BatchMode.SGD:
            if self.batch_size is not None and self.batch_size != 1:
                raise ValueError("sgd uses batch_size=1 when provided")
        elif self.batch_mode == BatchMode.FULL_BATCH and self.batch_size is not None:
            raise ValueError("full_batch must omit batch_size")
        return self


TrainType = Annotated[
    ClosedFormTrainType | FirstOrderGradientDescentTrainType,
    Field(discriminator="kind"),
]


class SplitSpec(SchemaModel):
    validation_holdout: float | None = Field(default=None, ge=0.0, lt=1.0)
    test_holdout: float | None = Field(default=None, ge=0.0, lt=1.0)
    random_seed: int | None = None
    shuffle: bool | None = None
    is_time_series: bool | None = None

    @model_validator(mode="after")
    def _validate_holdout_sum(self) -> "SplitSpec":
        total = (self.validation_holdout or 0.0) + (self.test_holdout or 0.0)
        if total >= 1.0:
            raise ValueError("validation_holdout + test_holdout must be < 1")
        return self


class FillNullsMethod(str, Enum):
    MEAN = "mean"
    MEDIAN = "median"
    MODE = "mode"
    ZERO = "zero"


class PreprocessSpec(SchemaModel):
    drop_null_rows: bool | None = None
    standardize: bool | None = None
    normalize: bool | None = None
    one_hot_encode: bool | None = None
    fill_nulls: FillNullsMethod | None = None

    @model_validator(mode="after")
    def _validate_scaling_exclusivity(self) -> "PreprocessSpec":
        if self.standardize and self.normalize:
            raise ValueError("standardize and normalize cannot both be true")
        return self


class HyperParamSpec(SchemaModel):
    learning_rate: float | None = Field(default=None, gt=0)
    epochs: int | None = Field(default=None, gt=0)
    batch_size: int | None = Field(default=None, gt=0)
    lambda_reg: float | None = Field(default=None, gt=0)


class TuneSearch(str, Enum):
    GRID = "grid"
    RANDOM = "random"


class TuneSpec(SchemaModel):
    enabled: bool
    max_trials: int | None = Field(default=None, gt=0)
    search: TuneSearch | None = None


class TrainModelRequest(SchemaModel):
    dataset_id: ID
    name: str | None = None
    model_type: ModelType
    train_type: TrainType
    x_cols: list[str] = Field(min_length=1)
    y_cols: list[str] = Field(min_length=1)
    query: QuerySpecPatch | None = None
    split: SplitSpec | None = None
    preprocessing: PreprocessSpec | None = None
    hyperparams: HyperParamSpec
    tuning: TuneSpec | None = None

    @model_validator(mode="after")
    def _validate_columns(self) -> "TrainModelRequest":
        x_clean = [col.strip() for col in self.x_cols]
        y_clean = [col.strip() for col in self.y_cols]

        if any(not col for col in x_clean):
            raise ValueError("x_cols must contain non-empty column names")
        if any(not col for col in y_clean):
            raise ValueError("y_cols must contain non-empty column names")
        if len(set(x_clean)) != len(x_clean):
            raise ValueError("x_cols must be unique")
        if len(set(y_clean)) != len(y_clean):
            raise ValueError("y_cols must be unique")
        if set(x_clean) & set(y_clean):
            raise ValueError("x_cols and y_cols must be disjoint")

        self.x_cols = x_clean
        self.y_cols = y_clean
        return self

    @model_validator(mode="after")
    def _validate_model_and_train_type(self) -> "TrainModelRequest":
        if len(self.y_cols) != 1:
            raise ValueError("exactly one y_col is required")

        linear_or_ridge = {
            ModelType.LINEAR_REGRESSION,
            ModelType.RIDGE_REGRESSION,
        }
        gd_required = {
            ModelType.LASSO_REGRESSION,
            ModelType.LOGISTIC_REGRESSION,
            ModelType.SOFTMAX_REGRESSION,
        }

        if isinstance(self.train_type, ClosedFormTrainType):
            if self.model_type not in linear_or_ridge:
                raise ValueError("closed_form is only allowed for linear/ridge regression")
            if (
                self.hyperparams.learning_rate is not None
                or self.hyperparams.epochs is not None
                or self.hyperparams.batch_size is not None
            ):
                raise ValueError(
                    "closed_form requires learning_rate, epochs, and batch_size to be omitted"
                )
        else:
            if self.model_type in gd_required and not isinstance(
                self.train_type, FirstOrderGradientDescentTrainType
            ):
                raise ValueError(
                    "lasso/logistic/softmax regression require first_order_gd training"
                )
            if self.hyperparams.learning_rate is None or self.hyperparams.epochs is None:
                raise ValueError(
                    "first_order_gd requires hyperparams.learning_rate and hyperparams.epochs"
                )

        if self.model_type in {
            ModelType.RIDGE_REGRESSION,
            ModelType.LASSO_REGRESSION,
        }:
            if self.hyperparams.lambda_reg is None:
                raise ValueError("lambda_reg is required for ridge/lasso regression")
        elif self.hyperparams.lambda_reg is not None:
            raise ValueError("lambda_reg is only valid for ridge/lasso regression")

        return self


class MetricSet(SchemaModel):
    accuracy: float | None = None
    precision: float | None = None
    recall: float | None = None
    f1: float | None = None
    log_loss: float | None = None
    mse: float | None = None
    rmse: float | None = None
    mae: float | None = None
    r2: float | None = None


class ModelMetrics(SchemaModel):
    train: MetricSet | None = None
    validation: MetricSet | None = None
    test: MetricSet | None = None


class TuningSummary(SchemaModel):
    enabled: bool
    searched_fields: list[str] | None = None
    best_hyperparams: HyperParamSpec | None = None


class Coefficient(SchemaModel):
    feature: str = Field(min_length=1)
    value: float


class ModelArtifact(SchemaModel):
    id: ID
    name: str = Field(min_length=1)
    dataset_id: ID
    model_type: ModelType
    train_type: TrainType
    x_cols: list[str] = Field(min_length=1)
    y_cols: list[str] = Field(min_length=1)
    split: SplitSpec | None = None
    preprocessing: PreprocessSpec | None = None
    hyperparams: HyperParamSpec
    tuning: TuningSummary | None = None
    metrics: ModelMetrics
    coefficients: list[Coefficient] | None = None
    created_at: Timestamp


class ModelArtifactListResponse(SchemaModel):
    items: list[ModelArtifact] = Field(default_factory=list)
    total: NonNegativeInt
    limit: int = Field(default=50, ge=1, le=1000)
    offset: NonNegativeInt = 0


class ModelJob(SchemaModel):
    id: ID
    status: JobStatus
    request: TrainModelRequest
    created_at: Timestamp
    started_at: Timestamp | None = None
    finished_at: Timestamp | None = None
    progress: Probability | None = None
    logs: list[str] | None = None
    error: str | None = None
    model_id: ID | None = None


class ModelJobLogsResponse(SchemaModel):
    job_id: ID
    logs: list[str] = Field(default_factory=list)
    next_index: NonNegativeInt
