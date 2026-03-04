from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ...schemas.modeling import ModelArtifact as ModelArtifactSchema
from ..models import ModelArtifact


def _normalize_tuning_for_schema(tuning: dict | None) -> dict | None:
    if tuning is None:
        return None

    # Backward compatibility: older rows may store request TuneSpec
    # (enabled/max_trials/search) while response schema expects TuningSummary.
    if "enabled" in tuning and (
        "searched_fields" not in tuning and "best_hyperparams" not in tuning
    ):
        return {"enabled": bool(tuning.get("enabled"))}

    return tuning


def create_model_artifact(
    db: Session,
    *,
    name: str,
    dataset_id: int,
    model_type: str,
    train_type: dict,
    x_cols: list[str],
    y_cols: list[str],
    split: dict | None,
    preprocessing: dict | None,
    hyperparams: dict,
    tuning: dict | None,
    metrics: dict,
    coefficients: list[dict] | None,
) -> ModelArtifact:
    artifact = ModelArtifact(
        name=name,
        dataset_id=dataset_id,
        model_type=model_type,
        train_type=train_type,
        x_cols=x_cols,
        y_cols=y_cols,
        split=split,
        preprocessing=preprocessing,
        hyperparams=hyperparams,
        tuning=tuning,
        metrics=metrics,
        coefficients=coefficients,
    )
    db.add(artifact)
    db.commit()
    db.refresh(artifact)
    return artifact


def get_model_artifact(db: Session, model_id: int) -> ModelArtifact | None:
    return db.get(ModelArtifact, model_id)


def list_model_artifacts(
    db: Session,
    *,
    dataset_id: int | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[ModelArtifact]:
    stmt = select(ModelArtifact)
    if dataset_id is not None:
        stmt = stmt.where(ModelArtifact.dataset_id == dataset_id)
    stmt = stmt.order_by(ModelArtifact.id.desc()).offset(offset).limit(limit)
    return list(db.scalars(stmt))


def count_model_artifacts(db: Session, *, dataset_id: int | None = None) -> int:
    stmt = select(func.count()).select_from(ModelArtifact)
    if dataset_id is not None:
        stmt = stmt.where(ModelArtifact.dataset_id == dataset_id)
    return int(db.scalar(stmt) or 0)


def delete_model_artifact(db: Session, model_id: int) -> bool:
    artifact = db.get(ModelArtifact, model_id)
    if artifact is None:
        return False
    db.delete(artifact)
    db.commit()
    return True


def model_artifact_to_schema(artifact: ModelArtifact) -> ModelArtifactSchema:
    return ModelArtifactSchema.model_validate(
        {
            "id": artifact.id,
            "name": artifact.name,
            "dataset_id": artifact.dataset_id,
            "model_type": artifact.model_type,
            "train_type": artifact.train_type,
            "x_cols": artifact.x_cols,
            "y_cols": artifact.y_cols,
            "split": artifact.split,
            "preprocessing": artifact.preprocessing,
            "hyperparams": artifact.hyperparams,
            "tuning": _normalize_tuning_for_schema(artifact.tuning),
            "metrics": artifact.metrics,
            "coefficients": artifact.coefficients,
            "created_at": artifact.created_at,
        }
    )
