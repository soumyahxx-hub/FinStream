"""Read-only pipeline processor and reconciliation status."""

from fastapi import APIRouter

from api.schemas.models import PipelineOperations
from api.services.operational import get_operational_repository

router = APIRouter(prefix="/operations", tags=["operations"])


@router.get("/pipeline", response_model=PipelineOperations)
def pipeline_status() -> PipelineOperations:
    result = get_operational_repository().pipeline_operations()
    return PipelineOperations.model_validate(result)
