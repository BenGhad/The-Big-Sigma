from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ...schemas.dataset import Dataset as DatasetSchema
from ..models import Dataset


def create_dataset(
    db: Session,
    *,
    name: str,
    filename: str,
    row_count: int,
    column_count: int,
    columns: list[dict],
) -> Dataset:
    dataset = Dataset(
        name=name,
        filename=filename,
        row_count=row_count,
        column_count=column_count,
        columns_json=columns,
    )
    db.add(dataset)
    db.commit()
    db.refresh(dataset)
    return dataset


def get_dataset(db: Session, dataset_id: int) -> Dataset | None:
    return db.get(Dataset, dataset_id)


def list_datasets(db: Session, *, limit: int = 50, offset: int = 0) -> list[Dataset]:
    stmt = (
        select(Dataset)
        .order_by(Dataset.id.desc())
        .offset(offset)
        .limit(limit)
    )
    return list(db.scalars(stmt))


def count_datasets(db: Session) -> int:
    stmt = select(func.count()).select_from(Dataset)
    return int(db.scalar(stmt) or 0)


def delete_dataset(db: Session, dataset_id: int) -> bool:
    dataset = db.get(Dataset, dataset_id)
    if dataset is None:
        return False
    db.delete(dataset)
    db.commit()
    return True


def dataset_to_schema(dataset: Dataset) -> DatasetSchema:
    return DatasetSchema.model_validate(
        {
            "id": dataset.id,
            "name": dataset.name,
            "filename": dataset.filename,
            "row_count": dataset.row_count,
            "column_count": dataset.column_count,
            "columns": dataset.columns_json,
            "created_at": dataset.created_at,
        }
    )
