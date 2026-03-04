from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import Field, model_validator

from .common import ID, NonNegativeInt, PageLimit, SchemaModel, Timestamp


class FilterOp(str, Enum):
    EQ = "eq"
    NEQ = "neq"
    LT = "lt"
    LTE = "lte"
    GT = "gt"
    GTE = "gte"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    NOT_NULL = "not_null"
    BETWEEN = "between"


class SortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


class HighlightOp(str, Enum):
    EQ = "eq"
    LT = "lt"
    GT = "gt"
    BETWEEN = "between"
    CONTAINS = "contains"


class FilterClause(SchemaModel):
    column: str = Field(min_length=1)
    op: FilterOp
    value: Any | None = None

    @model_validator(mode="after")
    def _validate_value_requirements(self) -> "FilterClause":
        null_ops = {FilterOp.IS_NULL, FilterOp.NOT_NULL}
        list_ops = {FilterOp.IN, FilterOp.NOT_IN, FilterOp.BETWEEN}

        if self.op in null_ops and self.value is not None:
            raise ValueError("value must be omitted for is_null/not_null filters")

        if self.op not in null_ops and self.value is None:
            raise ValueError(f"value is required for '{self.op.value}' filters")

        if self.op in list_ops:
            if not isinstance(self.value, list):
                raise ValueError(f"value must be a list for '{self.op.value}' filters")
            if self.op in {FilterOp.IN, FilterOp.NOT_IN} and len(self.value) == 0:
                raise ValueError(f"value list cannot be empty for '{self.op.value}' filters")
            if self.op == FilterOp.BETWEEN and len(self.value) != 2:
                raise ValueError("between filters require exactly two values")
        return self


class SortClause(SchemaModel):
    column: str = Field(min_length=1)
    direction: SortDirection = SortDirection.ASC


class HighlightRule(SchemaModel):
    column: str = Field(min_length=1)
    op: HighlightOp
    value: Any
    label: str | None = None

    @model_validator(mode="after")
    def _validate_highlight_value(self) -> "HighlightRule":
        if self.op == HighlightOp.BETWEEN:
            if not isinstance(self.value, list) or len(self.value) != 2:
                raise ValueError("between highlights require value as a two-item list")
        return self


class QuerySpec(SchemaModel):
    select: list[str] | None = None
    filters: list[FilterClause] | None = None
    sort: list[SortClause] | None = None
    limit: PageLimit = 50
    offset: NonNegativeInt = 0
    highlights: list[HighlightRule] | None = None

    @model_validator(mode="after")
    def _validate_unique_select(self) -> "QuerySpec":
        if self.select is not None:
            cleaned = [col.strip() for col in self.select]
            if any(not col for col in cleaned):
                raise ValueError("select entries must be non-empty column names")
            if len(set(cleaned)) != len(cleaned):
                raise ValueError("select entries must be unique")
            self.select = cleaned
        return self


class QuerySpecPatch(SchemaModel):
    select: list[str] | None = None
    filters: list[FilterClause] | None = None
    sort: list[SortClause] | None = None
    limit: PageLimit | None = None
    offset: NonNegativeInt | None = None
    highlights: list[HighlightRule] | None = None

    @model_validator(mode="after")
    def _validate_unique_select(self) -> "QuerySpecPatch":
        if self.select is not None:
            cleaned = [col.strip() for col in self.select]
            if any(not col for col in cleaned):
                raise ValueError("select entries must be non-empty column names")
            if len(set(cleaned)) != len(cleaned):
                raise ValueError("select entries must be unique")
            self.select = cleaned
        return self


class QueryResponse(SchemaModel):
    rows: list[dict[str, Any]] = Field(default_factory=list)
    total_rows: NonNegativeInt
    returned_rows: NonNegativeInt
    next_offset: NonNegativeInt | None = None
    applied_query: QuerySpec


class SavedViewCreate(SchemaModel):
    name: str = Field(min_length=1)
    query: QuerySpec


class SavedViewUpdate(SchemaModel):
    name: str | None = None
    query: QuerySpec | None = None

    @model_validator(mode="after")
    def _validate_not_empty(self) -> "SavedViewUpdate":
        if self.name is None and self.query is None:
            raise ValueError("at least one field must be provided for update")
        return self


class SavedView(SchemaModel):
    id: ID
    dataset_id: ID
    name: str = Field(min_length=1)
    query: QuerySpec
    created_at: Timestamp


class SavedViewListResponse(SchemaModel):
    items: list[SavedView] = Field(default_factory=list)
    total: NonNegativeInt
