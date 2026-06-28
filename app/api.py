from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, HTTPException
from psycopg import Error as PsycopgError

from app.collector import execute_collect_all_job, execute_collect_request
from app.config import settings
from app.models import (
    CollectAllRequest,
    CollectAllResponse,
    CollectRequest,
    CollectResponse,
    CreatePartitionsRequest,
    CreatePartitionsResponse,
    DropPartitionsRequest,
    DropPartitionsResponse,
    HealthResponse,
    ParentJobRecord,
    RunStatus,
)
from app.repository import repository
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


@router.post("/repository/partitions", response_model=CreatePartitionsResponse)
async def create_repository_partitions(
    request: CreatePartitionsRequest,
) -> CreatePartitionsResponse:
    try:
        partitions = await repository.create_weekly_partitions(
            from_date=request.from_date,
            weeks_ahead=request.weeks_ahead,
            weeks_back=request.weeks_back,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return CreatePartitionsResponse(status="ok", partitions=partitions)


@router.post("/repository/partitions/purge", response_model=DropPartitionsResponse)
async def purge_repository_partitions(
    request: DropPartitionsRequest,
) -> DropPartitionsResponse:
    try:
        dropped_partitions, partitions = await repository.drop_partitions_older_than(
            retain_weeks=request.retain_weeks,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return DropPartitionsResponse(
        status="ok",
        dropped_partitions=dropped_partitions,
        partitions=partitions,
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
