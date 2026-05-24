from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, HTTPException

from app.collector import execute_collect_all_job, execute_collect_request
from app.config import settings
from app.models import (
    CollectAllRequest,
    CollectAllResponse,
    CollectRequest,
    CollectResponse,
    HealthResponse,
    ParentJobRecord,
    RunStatus,
)
from app.runs import run_store
from app.sources import load_sources_from_path

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(status="ok")


@router.post("/collect", response_model=CollectResponse)
async def collect(request: CollectRequest) -> CollectResponse:
    run = await execute_collect_request(request, trigger_type="api")
    return CollectResponse(run_id=run.run_id, target_id=run.target_id, status=run.status)


@router.post("/collect_all", response_model=CollectAllResponse)
async def collect_all(
    request: CollectAllRequest,
    background_tasks: BackgroundTasks,
) -> CollectAllResponse:
    source_path = request.source_path or settings.default_sources_path
    try:
        sources = load_sources_from_path(source_path)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    enabled_sources = [
        source for source in sources if request.include_disabled or source.get("enabled", True)
    ]

    parent_job = run_store.create_parent_job(
        ParentJobRecord(
            status=RunStatus.queued,
            targets_queued=len(enabled_sources),
            metadata=request.metadata,
        )
    )

    background_tasks.add_task(
        execute_collect_all_job,
        parent_job_id=parent_job.job_id,
        source_path=source_path,
        include_disabled=request.include_disabled,
        override_jobs=request.jobs,
        metadata=request.metadata,
    )

    return CollectAllResponse(
        job_id=parent_job.job_id,
        status=parent_job.status,
        targets_queued=len(enabled_sources),
    )


@router.get("/runs/{run_id}")
async def get_run(run_id: UUID):
    run = run_store.get_run(run_id)
    if run is not None:
        return run

    parent_job = run_store.get_parent_job(run_id)
    if parent_job is not None:
        return parent_job

    raise HTTPException(status_code=404, detail="Run or job not found")
