from __future__ import annotations

from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.errors import ApiException, api_exception_handler
from .api.routes.datasets import router as datasets_router
from .api.routes.models import router as models_router
from .api.routes.predictions import router as predictions_router
from .schemas.health import HealthResponse

app = FastAPI(title="The Big Sigma API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_exception_handler(ApiException, api_exception_handler)
app.include_router(datasets_router)
app.include_router(models_router)
app.include_router(predictions_router)


@app.get("/v1/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(ok=True, time=datetime.now(timezone.utc))
