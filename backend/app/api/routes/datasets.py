from __future__ import annotations

import csv
import io
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, Depends, File, Form, Query, Response, UploadFile
from sqlalchemy.orm import Session

from ..errors import ApiException
from ...db.repositories import (
    count_datasets,
    count_saved_views,
    create_dataset,
    create_saved_view,
    dataset_to_schema,
    delete_dataset,
    delete_saved_view,
    get_dataset,
    get_saved_view,
    list_datasets,
    list_saved_views,
    saved_view_to_schema,
    update_saved_view,
)
from ...db.session import get_db
from ...schemas.dataset import (
    ColumnDType,
    ColumnStats,
    Dataset as DatasetSchema,
    DatasetListResponse,
    DatasetSettings,
    DatasetSettingsUpdate,
    DatasetStats,
    NumericSummary,
)
from ...schemas.query import (
    FilterClause,
    FilterOp,
    QueryResponse,
    QuerySpec,
    QuerySpecPatch,
    SavedView,
    SavedViewCreate,
    SavedViewListResponse,
    SavedViewUpdate,
    SortClause,
    SortDirection,
)

router = APIRouter(prefix="/v1", tags=["datasets"])

DATASET_STORAGE_DIR = Path(__file__).resolve().parents[3] / "data" / "datasets"
DATASET_STORAGE_DIR.mkdir(parents=True, exist_ok=True)


def _dataset_csv_path(dataset_id: int) -> Path:
    return DATASET_STORAGE_DIR / f"{dataset_id}.csv"


def _dataset_settings_path(dataset_id: int) -> Path:
    return DATASET_STORAGE_DIR / f"{dataset_id}.meta.json"


def _clean_column_list(columns: list[str]) -> list[str]:
    cleaned = [name.strip() for name in columns]
    if any(not name for name in cleaned):
        raise ValueError("y_columns entries must be non-empty")
    if len(set(cleaned)) != len(cleaned):
        raise ValueError("y_columns entries must be unique")
    return cleaned


def _validate_y_columns_against_dataset(columns_meta: list[dict[str, Any]], y_columns: list[str]) -> None:
    if not y_columns:
        return
    allowed_columns = {
        col["name"]
        for col in columns_meta
        if isinstance(col, dict) and isinstance(col.get("name"), str) and col.get("name")
    }
    unknown = [name for name in y_columns if name not in allowed_columns]
    if unknown:
        raise ValueError(f"Unknown y_columns: {unknown}")


def _parse_form_y_columns(raw: str | None) -> list[str]:
    if raw is None:
        return []
    if raw.strip() == "":
        return []
    candidates = [part.strip() for part in raw.split(",")]
    return _clean_column_list([name for name in candidates if name])


def _coerce_bool_like(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "t", "yes", "y"}:
            return True
        if lowered in {"0", "false", "f", "no", "n"}:
            return False
    return bool(value)


def _read_dataset_settings(dataset_id: int, columns_meta: list[dict[str, Any]]) -> dict[str, Any]:
    path = _dataset_settings_path(dataset_id)
    defaults = {"y_columns": [], "is_time_series": False}
    if not path.exists():
        return defaults

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return defaults

    y_columns_raw = payload.get("y_columns", [])
    if isinstance(y_columns_raw, list):
        y_columns = [
            value.strip()
            for value in y_columns_raw
            if isinstance(value, str) and value.strip()
        ]
    else:
        y_columns = []

    y_columns = list(dict.fromkeys(y_columns))
    try:
        _validate_y_columns_against_dataset(columns_meta, y_columns)
    except ValueError:
        y_columns = []

    is_time_series = _coerce_bool_like(payload.get("is_time_series", False))
    return {"y_columns": y_columns, "is_time_series": is_time_series}


def _write_dataset_settings(dataset_id: int, settings: dict[str, Any]) -> None:
    path = _dataset_settings_path(dataset_id)
    path.write_text(json.dumps(settings, indent=2), encoding="utf-8")


def _dataset_with_settings_schema(dataset: Any) -> DatasetSchema:
    base = dataset_to_schema(dataset).model_dump(mode="python")
    settings = _read_dataset_settings(dataset.id, list(dataset.columns_json or []))
    base.update(settings)
    return DatasetSchema.model_validate(base)


def _looks_like_int(value: str) -> bool:
    try:
        int(value)
        return True
    except ValueError:
        return False


def _looks_like_float(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False


def _looks_like_datetime(value: str) -> bool:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def _infer_column_dtype(values: list[str]) -> ColumnDType:
    if not values:
        return ColumnDType.UNKNOWN

    lowered = [value.lower() for value in values]
    bool_tokens = {"true", "false", "t", "f", "yes", "no", "y", "n"}
    if all(token in bool_tokens for token in lowered):
        return ColumnDType.BOOL
    if all(_looks_like_int(value) for value in values):
        return ColumnDType.INT
    if all(_looks_like_float(value) for value in values):
        return ColumnDType.FLOAT
    if all(_looks_like_datetime(value) for value in values):
        return ColumnDType.DATETIME
    return ColumnDType.STRING


def _build_column_metadata(
    fieldnames: list[str],
    rows: list[dict[str, str | None]],
) -> list[dict[str, Any]]:
    columns: list[dict[str, Any]] = []
    row_count = len(rows)
    for field in fieldnames:
        values = []
        for row in rows:
            raw = row.get(field)
            if raw is None:
                continue
            cleaned = raw.strip()
            if cleaned:
                values.append(cleaned)
        null_count = row_count - len(values)
        columns.append(
            {
                "name": field,
                "dtype": _infer_column_dtype(values).value,
                "nullable": null_count > 0,
                "unique_count": len(set(values)),
                "null_count": null_count,
            }
        )
    return columns


def _coerce_dtype(value: Any) -> ColumnDType:
    if isinstance(value, ColumnDType):
        return value
    try:
        return ColumnDType(str(value))
    except ValueError:
        return ColumnDType.UNKNOWN


def _cast_bool(value: str) -> bool | None:
    lowered = value.lower()
    if lowered in {"true", "t", "yes", "y", "1"}:
        return True
    if lowered in {"false", "f", "no", "n", "0"}:
        return False
    return None


def _cast_cell(value: str | None, dtype: ColumnDType) -> Any:
    if value is None:
        return None
    cleaned = value.strip()
    if cleaned == "":
        return None

    if dtype == ColumnDType.INT:
        try:
            return int(cleaned)
        except ValueError:
            return cleaned
    if dtype == ColumnDType.FLOAT:
        try:
            return float(cleaned)
        except ValueError:
            return cleaned
    if dtype == ColumnDType.BOOL:
        maybe_bool = _cast_bool(cleaned)
        return maybe_bool if maybe_bool is not None else cleaned
    return cleaned


def _coerce_filter_value(value: Any, dtype: ColumnDType) -> Any:
    if value is None:
        return None
    if isinstance(value, list):
        return [_coerce_filter_value(item, dtype) for item in value]

    if dtype == ColumnDType.INT:
        try:
            return int(value)
        except (TypeError, ValueError):
            return value
    if dtype == ColumnDType.FLOAT:
        try:
            return float(value)
        except (TypeError, ValueError):
            return value
    if dtype == ColumnDType.BOOL:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            maybe_bool = _cast_bool(value.strip())
            if maybe_bool is not None:
                return maybe_bool
    if dtype == ColumnDType.DATETIME and isinstance(value, datetime):
        return value.isoformat()
    return value


def _matches_filter(
    row: dict[str, Any],
    clause: FilterClause,
    dtype_by_column: dict[str, ColumnDType],
) -> bool:
    value = row.get(clause.column)
    op = clause.op

    if op == FilterOp.IS_NULL:
        return value is None
    if op == FilterOp.NOT_NULL:
        return value is not None
    if value is None:
        return False

    dtype = dtype_by_column.get(clause.column, ColumnDType.STRING)
    filter_value = _coerce_filter_value(clause.value, dtype)

    if op == FilterOp.EQ:
        return value == filter_value
    if op == FilterOp.NEQ:
        return value != filter_value

    if op in {FilterOp.LT, FilterOp.LTE, FilterOp.GT, FilterOp.GTE}:
        try:
            if op == FilterOp.LT:
                return value < filter_value
            if op == FilterOp.LTE:
                return value <= filter_value
            if op == FilterOp.GT:
                return value > filter_value
            return value >= filter_value
        except TypeError:
            return False

    if op == FilterOp.CONTAINS:
        return str(filter_value).lower() in str(value).lower()
    if op == FilterOp.STARTS_WITH:
        return str(value).lower().startswith(str(filter_value).lower())
    if op == FilterOp.ENDS_WITH:
        return str(value).lower().endswith(str(filter_value).lower())

    if op == FilterOp.IN:
        return isinstance(filter_value, list) and value in filter_value
    if op == FilterOp.NOT_IN:
        return isinstance(filter_value, list) and value not in filter_value
    if op == FilterOp.BETWEEN:
        if not isinstance(filter_value, list) or len(filter_value) != 2:
            return False
        low, high = filter_value
        try:
            return low <= value <= high
        except TypeError:
            return False

    return False


def _matches_filter_group(
    row: dict[str, Any],
    filter_group: list[FilterClause],
    dtype_by_column: dict[str, ColumnDType],
) -> bool:
    return all(_matches_filter(row, clause, dtype_by_column) for clause in filter_group)


def _matches_filter_groups(
    row: dict[str, Any],
    filter_groups: list[list[FilterClause]] | None,
    dtype_by_column: dict[str, ColumnDType],
) -> bool:
    if not filter_groups:
        return True
    return any(_matches_filter_group(row, group, dtype_by_column) for group in filter_groups)


def _sort_key(value: Any) -> tuple[int, Any]:
    if value is None:
        return (1, "")
    if isinstance(value, bool):
        return (0, int(value))
    if isinstance(value, (int, float, str)):
        return (0, value)
    return (0, str(value))


def _apply_sort(rows: list[dict[str, Any]], clauses: list[SortClause] | None) -> None:
    if not clauses:
        return
    for clause in reversed(clauses):
        reverse = clause.direction == SortDirection.DESC
        rows.sort(key=lambda row: _sort_key(row.get(clause.column)), reverse=reverse)


def _load_dataset_rows(
    dataset_id: int,
    columns_meta: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    csv_path = _dataset_csv_path(dataset_id)
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    dtype_by_column = {
        col["name"]: _coerce_dtype(col.get("dtype"))
        for col in columns_meta
        if isinstance(col, dict) and col.get("name")
    }

    rows: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            parsed: dict[str, Any] = {}
            for column, dtype in dtype_by_column.items():
                parsed[column] = _cast_cell(raw.get(column), dtype)
            rows.append(parsed)
    return rows


def _validate_select_columns(
    columns_meta: list[dict[str, Any]],
    selected: list[str] | None,
) -> str | None:
    if not selected:
        return None
    allowed = {col["name"] for col in columns_meta if isinstance(col, dict) and col.get("name")}
    unknown = [name for name in selected if name not in allowed]
    if unknown:
        return f"Unknown columns in select: {unknown}"
    return None


def _validate_query_y_columns(
    columns_meta: list[dict[str, Any]],
    y_columns: list[str] | None,
) -> str | None:
    if not y_columns:
        return None
    try:
        _validate_y_columns_against_dataset(columns_meta, y_columns)
    except ValueError as exc:
        return str(exc)
    return None


def _validate_query_clauses_against_select(query: QuerySpec) -> str | None:
    if not query.select:
        return None
    selected = set(query.select)

    if query.sort:
        invalid_sort = [clause.column for clause in query.sort if clause.column not in selected]
        if invalid_sort:
            return f"Sort columns must be selected columns when select is set: {invalid_sort}"

    if query.filters:
        invalid_filter = [
            clause.column
            for filter_group in query.filters
            for clause in filter_group
            if clause.column not in selected
        ]
        if invalid_filter:
            return f"Filter columns must be selected columns when select is set: {invalid_filter}"

    return None


def _validate_query_for_dataset(columns_meta: list[dict[str, Any]], query: QuerySpec) -> str | None:
    select_error = _validate_select_columns(columns_meta, query.select)
    if select_error is not None:
        return select_error

    y_cols_error = _validate_query_y_columns(columns_meta, query.y_columns)
    if y_cols_error is not None:
        return y_cols_error

    clause_error = _validate_query_clauses_against_select(query)
    if clause_error is not None:
        return clause_error

    return None


def _run_query(dataset: Any, query: QuerySpec) -> QueryResponse:
    columns_meta = list(dataset.columns_json or [])
    query_error = _validate_query_for_dataset(columns_meta, query)
    if query_error is not None:
        raise ValueError(query_error)

    rows = _load_dataset_rows(dataset.id, columns_meta)
    dtype_by_column = {
        col["name"]: _coerce_dtype(col.get("dtype"))
        for col in columns_meta
        if isinstance(col, dict) and col.get("name")
    }

    if query.filters:
        rows = [
            row
            for row in rows
            if _matches_filter_groups(row, query.filters, dtype_by_column)
        ]

    total_rows = len(rows)
    _apply_sort(rows, query.sort)

    applied_limit = query.limit if query.limit is not None else 50
    applied_offset = query.offset if query.offset is not None else 0

    paged_rows = rows[applied_offset : applied_offset + applied_limit]
    if query.select:
        paged_rows = [{column: row.get(column) for column in query.select} for row in paged_rows]

    returned_rows = len(paged_rows)
    next_offset = applied_offset + returned_rows
    if next_offset >= total_rows:
        next_offset = None

    applied_query = query.model_copy(update={"limit": applied_limit, "offset": applied_offset})

    return QueryResponse(
        rows=paged_rows,
        total_rows=total_rows,
        returned_rows=returned_rows,
        next_offset=next_offset,
        applied_query=applied_query,
    )


def _merge_saved_view_query(base: QuerySpec, override: QuerySpecPatch | None) -> QuerySpec:
    if override is None:
        return base
    merged = base.model_dump(mode="python")
    merged.update(override.model_dump(mode="python", exclude_unset=True))
    return QuerySpec.model_validate(merged)


@router.post("/datasets", response_model=DatasetSchema, status_code=201)
async def create_dataset_endpoint(
    file: UploadFile = File(...),
    name: str | None = Form(default=None),
    y_columns: str | None = Form(default=None),
    is_time_series: bool = Form(default=False),
    db: Session = Depends(get_db),
) -> DatasetSchema:
    if not file.filename:
        raise ApiException(
            status_code=422,
            code="INVALID_DATASET_FILE",
            message="CSV file must include a filename",
        )

    raw_bytes = await file.read()
    if not raw_bytes:
        raise ApiException(
            status_code=422,
            code="EMPTY_DATASET_FILE",
            message="CSV file is empty",
        )

    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise ApiException(
            status_code=422,
            code="INVALID_DATASET_ENCODING",
            message="CSV must be UTF-8 encoded, but thats like most of them. VSC support incoming",
        )

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ApiException(
            status_code=422,
            code="INVALID_DATASET_CSV",
            message="CSV must include a header row",
        )

    raw_fieldnames = list(reader.fieldnames)
    fieldnames = [name.strip() if name else "" for name in raw_fieldnames]
    if any(not name for name in fieldnames):
        raise ApiException(
            status_code=422,
            code="INVALID_COLUMN_NAME",
            message="Column names cannot be empty",
        )
    if len(set(fieldnames)) != len(fieldnames):
        raise ApiException(
            status_code=422,
            code="DUPLICATE_COLUMN_NAME",
            message="Column names must be unique",
        )

    rows = list(reader)
    columns = _build_column_metadata(fieldnames, rows)
    dataset_name = (name or Path(file.filename).stem).strip() or "dataset"

    try:
        parsed_y_columns = _parse_form_y_columns(y_columns)
        _validate_y_columns_against_dataset(columns, parsed_y_columns)
    except ValueError as exc:
        raise ApiException(
            status_code=422,
            code="INVALID_DATASET_SETTINGS",
            message=str(exc),
        )

    dataset = create_dataset(
        db,
        name=dataset_name,
        filename=file.filename,
        row_count=len(rows),
        column_count=len(fieldnames),
        columns=columns,
    )

    csv_path = _dataset_csv_path(dataset.id)
    try:
        csv_path.write_bytes(raw_bytes)
    except OSError as exc:
        delete_dataset(db, dataset.id)
        raise ApiException(
            status_code=500,
            code="DATASET_FILE_WRITE_FAILED",
            message="Failed to persist dataset file",
            details={"reason": str(exc)},
        )
    try:
        _write_dataset_settings(
            dataset.id,
            {
                "y_columns": parsed_y_columns,
                "is_time_series": is_time_series,
            },
        )
    except OSError as exc:
        if csv_path.exists():
            try:
                csv_path.unlink()
            except OSError:
                pass
        delete_dataset(db, dataset.id)
        raise ApiException(
            status_code=500,
            code="DATASET_SETTINGS_WRITE_FAILED",
            message="Failed to persist dataset settings",
            details={"reason": str(exc)},
        )

    try:
        default_view_query = QuerySpec(y_columns=parsed_y_columns)
        create_saved_view(
            db,
            dataset_id=dataset.id,
            name="Default View",
            query=default_view_query.model_dump(mode="python", exclude_none=True),
        )
    except Exception as exc:
        if csv_path.exists():
            try:
                csv_path.unlink()
            except OSError:
                pass
        settings_path = _dataset_settings_path(dataset.id)
        if settings_path.exists():
            try:
                settings_path.unlink()
            except OSError:
                pass
        delete_dataset(db, dataset.id)
        raise ApiException(
            status_code=500,
            code="DEFAULT_VIEW_CREATE_FAILED",
            message="Dataset was created but default view creation failed",
            details={"reason": str(exc)},
        )

    return _dataset_with_settings_schema(dataset)


@router.get("/datasets", response_model=DatasetListResponse)
def list_datasets_endpoint(
    limit: int = Query(default=50, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> DatasetListResponse:
    datasets = list_datasets(db, limit=limit, offset=offset)
    total = count_datasets(db)
    return DatasetListResponse(
        items=[_dataset_with_settings_schema(dataset) for dataset in datasets],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/datasets/{dataset_id}", response_model=DatasetSchema)
def get_dataset_endpoint(
    dataset_id: int,
    db: Session = Depends(get_db),
) -> DatasetSchema:
    dataset = get_dataset(db, dataset_id)
    if dataset is None:
        raise ApiException(
            status_code=404,
            code="DATASET_NOT_FOUND",
            message=f"Dataset {dataset_id} was not found",
        )
    return _dataset_with_settings_schema(dataset)


@router.get("/datasets/{dataset_id}/settings", response_model=DatasetSettings)
def get_dataset_settings_endpoint(
    dataset_id: int,
    db: Session = Depends(get_db),
) -> DatasetSettings:
    dataset = get_dataset(db, dataset_id)
    if dataset is None:
        raise ApiException(
            status_code=404,
            code="DATASET_NOT_FOUND",
            message=f"Dataset {dataset_id} was not found",
        )

    settings = _read_dataset_settings(dataset.id, list(dataset.columns_json or []))
    return DatasetSettings(
        dataset_id=dataset.id,
        y_columns=settings["y_columns"],
        is_time_series=settings["is_time_series"],
    )


@router.put("/datasets/{dataset_id}/settings", response_model=DatasetSettings)
def update_dataset_settings_endpoint(
    dataset_id: int,
    payload: DatasetSettingsUpdate,
    db: Session = Depends(get_db),
) -> DatasetSettings:
    dataset = get_dataset(db, dataset_id)
    if dataset is None:
        raise ApiException(
            status_code=404,
            code="DATASET_NOT_FOUND",
            message=f"Dataset {dataset_id} was not found",
        )

    columns_meta = list(dataset.columns_json or [])
    current = _read_dataset_settings(dataset.id, columns_meta)
    next_y_columns = list(current["y_columns"])
    next_is_time_series = bool(current["is_time_series"])

    if payload.y_columns is not None:
        try:
            next_y_columns = _clean_column_list(payload.y_columns)
            _validate_y_columns_against_dataset(columns_meta, next_y_columns)
        except ValueError as exc:
            raise ApiException(
                status_code=422,
                code="INVALID_DATASET_SETTINGS",
                message=str(exc),
            )
    if payload.is_time_series is not None:
        next_is_time_series = payload.is_time_series

    try:
        _write_dataset_settings(
            dataset.id,
            {
                "y_columns": next_y_columns,
                "is_time_series": next_is_time_series,
            },
        )
    except OSError as exc:
        raise ApiException(
            status_code=500,
            code="DATASET_SETTINGS_WRITE_FAILED",
            message="Failed to persist dataset settings",
            details={"reason": str(exc)},
        )

    return DatasetSettings(
        dataset_id=dataset.id,
        y_columns=next_y_columns,
        is_time_series=next_is_time_series,
    )


@router.delete("/datasets/{dataset_id}", status_code=204)
def delete_dataset_endpoint(
    dataset_id: int,
    db: Session = Depends(get_db),
) -> Response:
    deleted = delete_dataset(db, dataset_id)
    if not deleted:
        raise ApiException(
            status_code=404,
            code="DATASET_NOT_FOUND",
            message=f"Dataset {dataset_id} was not found",
        )

    csv_path = _dataset_csv_path(dataset_id)
    if csv_path.exists():
        try:
            csv_path.unlink()
        except OSError as exc:
            raise ApiException(
                status_code=500,
                code="DATASET_FILE_DELETE_FAILED",
                message=f"Dataset {dataset_id} was deleted, but its CSV file could not be removed",
                details={"reason": str(exc)},
            )

    settings_path = _dataset_settings_path(dataset_id)
    if settings_path.exists():
        try:
            settings_path.unlink()
        except OSError as exc:
            raise ApiException(
                status_code=500,
                code="DATASET_SETTINGS_DELETE_FAILED",
                message=f"Dataset {dataset_id} was deleted, but its settings file could not be removed",
                details={"reason": str(exc)},
            )
    return Response(status_code=204)


@router.get("/datasets/{dataset_id}/stats", response_model=DatasetStats)
def get_dataset_stats_endpoint(
    dataset_id: int,
    db: Session = Depends(get_db),
) -> DatasetStats:
    dataset = get_dataset(db, dataset_id)
    if dataset is None:
        raise ApiException(
            status_code=404,
            code="DATASET_NOT_FOUND",
            message=f"Dataset {dataset_id} was not found",
        )

    try:
        rows = _load_dataset_rows(dataset.id, list(dataset.columns_json or []))
    except FileNotFoundError:
        raise ApiException(
            status_code=500,
            code="DATASET_FILE_MISSING",
            message=f"Dataset file for {dataset_id} is missing on disk",
        )

    stats_columns: list[ColumnStats] = []
    for col_meta in list(dataset.columns_json or []):
        column_name = col_meta.get("name")
        if not column_name:
            continue

        dtype = _coerce_dtype(col_meta.get("dtype"))
        values = [row.get(column_name) for row in rows]
        non_null_values = [value for value in values if value is not None]
        null_count = len(values) - len(non_null_values)

        summary: NumericSummary | None = None
        if dtype in {ColumnDType.INT, ColumnDType.FLOAT} and non_null_values:
            numeric_values = [float(value) for value in non_null_values if isinstance(value, (int, float))]
            if numeric_values:
                mean = sum(numeric_values) / len(numeric_values)
                variance = sum((value - mean) ** 2 for value in numeric_values) / len(numeric_values)
                summary = NumericSummary(
                    min=min(numeric_values),
                    max=max(numeric_values),
                    mean=mean,
                    std=math.sqrt(variance),
                )

        stats_columns.append(
            ColumnStats(
                name=column_name,
                dtype=dtype,
                nullable=bool(col_meta.get("nullable", null_count > 0)),
                unique_count=len(set(non_null_values)),
                null_count=null_count,
                summary=summary,
            )
        )

    return DatasetStats(
        dataset_id=dataset.id,
        row_count=len(rows),
        column_count=len(stats_columns),
        columns=stats_columns,
    )


@router.post("/datasets/{dataset_id}/query", response_model=QueryResponse)
def query_dataset_endpoint(
    dataset_id: int,
    query: QuerySpec,
    db: Session = Depends(get_db),
) -> QueryResponse:
    dataset = get_dataset(db, dataset_id)
    if dataset is None:
        raise ApiException(
            status_code=404,
            code="DATASET_NOT_FOUND",
            message=f"Dataset {dataset_id} was not found",
        )

    try:
        return _run_query(dataset, query)
    except FileNotFoundError:
        raise ApiException(
            status_code=500,
            code="DATASET_FILE_MISSING",
            message=f"Dataset file for {dataset_id} is missing on disk",
        )
    except ValueError as exc:
        raise ApiException(
            status_code=422,
            code="INVALID_QUERY",
            message=str(exc),
        )


@router.post("/datasets/{dataset_id}/views", response_model=SavedView, status_code=201)
def create_saved_view_endpoint(
    dataset_id: int,
    payload: SavedViewCreate,
    db: Session = Depends(get_db),
) -> SavedView:
    dataset = get_dataset(db, dataset_id)
    if dataset is None:
        raise ApiException(
            status_code=404,
            code="DATASET_NOT_FOUND",
            message=f"Dataset {dataset_id} was not found",
        )

    query_error = _validate_query_for_dataset(list(dataset.columns_json or []), payload.query)
    if query_error is not None:
        raise ApiException(
            status_code=422,
            code="INVALID_QUERY",
            message=query_error,
        )

    saved_view = create_saved_view(
        db,
        dataset_id=dataset_id,
        name=payload.name.strip(),
        query=payload.query.model_dump(mode="python", exclude_none=True),
    )
    return saved_view_to_schema(saved_view)


@router.get("/datasets/{dataset_id}/views", response_model=SavedViewListResponse)
def list_saved_views_endpoint(
    dataset_id: int,
    limit: int = Query(default=50, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> SavedViewListResponse:
    dataset = get_dataset(db, dataset_id)
    if dataset is None:
        raise ApiException(
            status_code=404,
            code="DATASET_NOT_FOUND",
            message=f"Dataset {dataset_id} was not found",
        )

    saved_views = list_saved_views(db, dataset_id=dataset_id, limit=limit, offset=offset)
    total = count_saved_views(db, dataset_id=dataset_id)
    return SavedViewListResponse(
        items=[saved_view_to_schema(view) for view in saved_views],
        total=total,
    )


@router.get("/views/{view_id}", response_model=SavedView)
def get_saved_view_endpoint(
    view_id: int,
    db: Session = Depends(get_db),
) -> SavedView:
    saved_view = get_saved_view(db, view_id)
    if saved_view is None:
        raise ApiException(
            status_code=404,
            code="VIEW_NOT_FOUND",
            message=f"Saved view {view_id} was not found",
        )
    return saved_view_to_schema(saved_view)


@router.put("/views/{view_id}", response_model=SavedView)
def update_saved_view_endpoint(
    view_id: int,
    payload: SavedViewUpdate,
    db: Session = Depends(get_db),
) -> SavedView:
    current = get_saved_view(db, view_id)
    if current is None:
        raise ApiException(
            status_code=404,
            code="VIEW_NOT_FOUND",
            message=f"Saved view {view_id} was not found",
        )

    if payload.query is not None:
        dataset = get_dataset(db, current.dataset_id)
        if dataset is None:
            raise ApiException(
                status_code=404,
                code="DATASET_NOT_FOUND",
                message=f"Dataset {current.dataset_id} for saved view {view_id} was not found",
            )
        query_error = _validate_query_for_dataset(list(dataset.columns_json or []), payload.query)
        if query_error is not None:
            raise ApiException(
                status_code=422,
                code="INVALID_QUERY",
                message=query_error,
            )

    updated = update_saved_view(
        db,
        saved_view_id=view_id,
        name=payload.name.strip() if payload.name is not None else None,
        query=payload.query.model_dump(mode="python", exclude_none=True) if payload.query is not None else None,
    )
    if updated is None:
        raise ApiException(
            status_code=404,
            code="VIEW_NOT_FOUND",
            message=f"Saved view {view_id} was not found",
        )
    return saved_view_to_schema(updated)


@router.delete("/views/{view_id}", status_code=204)
def delete_saved_view_endpoint(
    view_id: int,
    db: Session = Depends(get_db),
) -> Response:
    deleted = delete_saved_view(db, view_id)
    if not deleted:
        raise ApiException(
            status_code=404,
            code="VIEW_NOT_FOUND",
            message=f"Saved view {view_id} was not found",
        )
    return Response(status_code=204)


@router.post("/views/{view_id}/query", response_model=QueryResponse)
def run_saved_view_endpoint(
    view_id: int,
    override: QuerySpecPatch | None = Body(default=None),
    db: Session = Depends(get_db),
) -> QueryResponse:
    saved_view = get_saved_view(db, view_id)
    if saved_view is None:
        raise ApiException(
            status_code=404,
            code="VIEW_NOT_FOUND",
            message=f"Saved view {view_id} was not found",
        )

    dataset = get_dataset(db, saved_view.dataset_id)
    if dataset is None:
        raise ApiException(
            status_code=404,
            code="DATASET_NOT_FOUND",
            message=f"Dataset {saved_view.dataset_id} for saved view {view_id} was not found",
        )

    try:
        stored_query = QuerySpec.model_validate(saved_view.query)
        applied_query = _merge_saved_view_query(stored_query, override)
        return _run_query(dataset, applied_query)
    except FileNotFoundError:
        raise ApiException(
            status_code=500,
            code="DATASET_FILE_MISSING",
            message=f"Dataset file for {saved_view.dataset_id} is missing on disk",
        )
    except ValueError as exc:
        raise ApiException(
            status_code=422,
            code="INVALID_QUERY",
            message=str(exc),
        )
