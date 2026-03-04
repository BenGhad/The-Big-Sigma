from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ...schemas.query import QuerySpec, SavedView as SavedViewSchema
from ..models import SavedView


def create_saved_view(
    db: Session,
    *,
    dataset_id: int,
    name: str,
    query: dict,
) -> SavedView:
    saved_view = SavedView(dataset_id=dataset_id, name=name, query=query)
    db.add(saved_view)
    db.commit()
    db.refresh(saved_view)
    return saved_view


def get_saved_view(db: Session, saved_view_id: int) -> SavedView | None:
    return db.get(SavedView, saved_view_id)


def list_saved_views(
    db: Session,
    *,
    dataset_id: int | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[SavedView]:
    stmt = select(SavedView)
    if dataset_id is not None:
        stmt = stmt.where(SavedView.dataset_id == dataset_id)
    stmt = stmt.order_by(SavedView.id.desc()).offset(offset).limit(limit)
    return list(db.scalars(stmt))


def count_saved_views(db: Session, *, dataset_id: int | None = None) -> int:
    stmt = select(func.count()).select_from(SavedView)
    if dataset_id is not None:
        stmt = stmt.where(SavedView.dataset_id == dataset_id)
    return int(db.scalar(stmt) or 0)


def update_saved_view(
    db: Session,
    *,
    saved_view_id: int,
    name: str | None = None,
    query: dict | None = None,
) -> SavedView | None:
    saved_view = db.get(SavedView, saved_view_id)
    if saved_view is None:
        return None
    if name is not None:
        saved_view.name = name
    if query is not None:
        saved_view.query = query
    db.commit()
    db.refresh(saved_view)
    return saved_view


def delete_saved_view(db: Session, saved_view_id: int) -> bool:
    saved_view = db.get(SavedView, saved_view_id)
    if saved_view is None:
        return False
    db.delete(saved_view)
    db.commit()
    return True


def saved_view_to_schema(saved_view: SavedView) -> SavedViewSchema:
    return SavedViewSchema.model_validate(
        {
            "id": saved_view.id,
            "dataset_id": saved_view.dataset_id,
            "name": saved_view.name,
            "query": QuerySpec.model_validate(saved_view.query),
            "created_at": saved_view.created_at,
        }
    )
