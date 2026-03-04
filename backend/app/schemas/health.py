from __future__ import annotations

from .common import SchemaModel, Timestamp


class HealthResponse(SchemaModel):
    ok: bool
    time: Timestamp
