from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, Float, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

JsonDict = dict[str, Any]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Dataset(Base):
    __tablename__ = "datasets"
    __table_args__ = (
        CheckConstraint("row_count >= 0", name="ck_datasets_row_count_nonnegative"),
        CheckConstraint("column_count >= 0", name="ck_datasets_column_count_nonnegative"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    column_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Dataset.columns payload
    columns_json: Mapped[list[JsonDict]] = mapped_column("columns", JSON, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )

    saved_views: Mapped[list[SavedView]] = relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    model_artifacts: Mapped[list[ModelArtifact]] = relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    model_jobs: Mapped[list[ModelJob]] = relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    prediction_jobs: Mapped[list[PredictionJob]] = relationship(
        back_populates="dataset",
        cascade="all, delete-orphan",
    )


class SavedView(Base):
    __tablename__ = "saved_views"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_id: Mapped[int] = mapped_column(
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    query: Mapped[JsonDict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )

    dataset: Mapped[Dataset] = relationship(back_populates="saved_views")


class ModelArtifact(Base):
    __tablename__ = "model_artifacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    dataset_id: Mapped[int] = mapped_column(
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    model_type: Mapped[str] = mapped_column(String(64), nullable=False)
    train_type: Mapped[JsonDict] = mapped_column(JSON, nullable=False)
    x_cols: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    y_cols: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    split: Mapped[JsonDict | None] = mapped_column(JSON, nullable=True, default=None)
    preprocessing: Mapped[JsonDict | None] = mapped_column(JSON, nullable=True, default=None)
    hyperparams: Mapped[JsonDict] = mapped_column(JSON, nullable=False)
    tuning: Mapped[JsonDict | None] = mapped_column(JSON, nullable=True, default=None)
    metrics: Mapped[JsonDict] = mapped_column(JSON, nullable=False)
    coefficients: Mapped[list[JsonDict] | None] = mapped_column(JSON, nullable=True, default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )

    dataset: Mapped[Dataset] = relationship(back_populates="model_artifacts")
    model_jobs: Mapped[list[ModelJob]] = relationship(
        back_populates="model",
        foreign_keys="ModelJob.model_id",
    )


class ModelJob(Base):
    __tablename__ = "model_jobs"
    __table_args__ = (
        CheckConstraint(
            "progress IS NULL OR (progress >= 0.0 AND progress <= 1.0)",
            name="ck_model_jobs_progress_01",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    request: Mapped[JsonDict] = mapped_column(JSON, nullable=False)
    dataset_id: Mapped[int] = mapped_column(
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
    progress: Mapped[float | None] = mapped_column(Float, nullable=True, default=None)
    logs: Mapped[list[str] | None] = mapped_column(JSON, nullable=True, default=None)
    error: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)
    model_id: Mapped[int | None] = mapped_column(
        ForeignKey("model_artifacts.id", ondelete="SET NULL"),
        nullable=True,
        default=None,
        index=True,
    )

    dataset: Mapped[Dataset] = relationship(back_populates="model_jobs")
    model: Mapped[ModelArtifact | None] = relationship(
        back_populates="model_jobs",
        foreign_keys=[model_id],
    )


class PredictionJob(Base):
    __tablename__ = "prediction_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    request: Mapped[JsonDict] = mapped_column(JSON, nullable=False)
    dataset_id: Mapped[int] = mapped_column(
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)
    error: Mapped[str | None] = mapped_column(Text, nullable=True, default=None)
    results: Mapped[list[JsonDict] | None] = mapped_column(JSON, nullable=True, default=None)

    dataset: Mapped[Dataset] = relationship(back_populates="prediction_jobs")


__all__ = [
    "Base",
    "Dataset",
    "SavedView",
    "ModelArtifact",
    "ModelJob",
    "PredictionJob",
]
