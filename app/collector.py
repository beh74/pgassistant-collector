from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import UUID

from app.config import settings
from app.models import (
    CollectRequest,
    JobResult,
    JobType,
    ParentJobRecord,
    RunRecord,
    RunStatus,
)
from app.pgassistant_client import PgAssistantClient, PgAssistantClientError, parse_conn_str
from app.repository import repository
from app.runs import run_store
from app.sources import load_sources_from_path


def summarize_payload(job_type: JobType, payload: dict) -> dict:
    if not isinstance(payload, dict):
        return {"payload_type": type(payload).__name__}

    if job_type == JobType.rank_top_10_queries:
        ranked_queries = payload.get("ranked_queries", [])
        if isinstance(ranked_queries, list):
            return {
                "ranked_queries_count": len(ranked_queries),
                "top_priority_score": ranked_queries[0].get("priority_score") if ranked_queries else None,
                "top_priority_level": ranked_queries[0].get("priority_level") if ranked_queries else None,
            }

    if job_type == JobType.global_advisor_top_10:
        items = payload.get("ranked_queries") or payload.get("recommendations") or []
        if isinstance(items, list):
            high_count = sum(1 for item in items if isinstance(item, dict) and item.get("priority") == "HIGH")
            lock_count = sum(1 for item in items if isinstance(item, dict) and item.get("requires_lock") is True)
            manual_count = sum(1 for item in items if isinstance(item, dict) and item.get("manual_review_required") is True)

            return {
                "recommendations_count": len(items),
                "high_priority_count": high_count,
                "requires_lock_count": lock_count,
                "manual_review_required_count": manual_count,
                "top_rank": items[0].get("rank") if items and isinstance(items[0], dict) else None,
                "top_priority": items[0].get("priority") if items and isinstance(items[0], dict) else None,
                "top_title": items[0].get("title") if items and isinstance(items[0], dict) else None,
            }

    for key in ("ranked_queries", "recommendations", "queries", "data", "results", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return {f"{key}_count": len(value)}

    return {
        "payload_type": "dict",
        "keys": list(payload.keys()),
    }


async def execute_collect_request(
    request: CollectRequest,
    *,
    trigger_type: str,
    parent_job_id: UUID | None = None,
) -> RunRecord:
    run = RunRecord(
        parent_job_id=parent_job_id,
        target_id=request.target_id,
        trigger_type=trigger_type,  # type: ignore[arg-type]
        status=RunStatus.running,
        started_at=datetime.now(timezone.utc),
        environment=request.environment,
        group=request.group,
        metadata=request.metadata,
        jobs_requested=request.jobs,
    )
    run_store.create_run(run)
    await repository.save_run_started(run)

    try:
        if request.db_config is not None:
            db_config = request.db_config
        elif request.conn_str is not None:
            db_config = parse_conn_str(request.conn_str)
        else:
            raise PgAssistantClientError("Either conn_str or db_config must be provided")
    except PgAssistantClientError as exc:
        run.status = RunStatus.failed
        run.finished_at = datetime.now(timezone.utc)
        run.error_message = str(exc)
        run_store.update_run(run)
        await repository.save_run_finished(run)
        return run

    client = PgAssistantClient(timeout_seconds=settings.request_timeout_seconds)
    has_failure = False

    for job_type in request.jobs:
        try:
            result = await client.collect_job(
                pgassistant_api_url=request.pgassistant_api_url,
                db_config=db_config,
                job_type=job_type,
            )
            await repository.save_job_payload(
                run=run,
                job_type=job_type.value,
                payload=result.payload,
            )
            run.job_results.append(
                JobResult(
                    job_type=job_type,
                    status=RunStatus.completed,
                    response_time_ms=result.response_time_ms,
                    payload_summary=summarize_payload(job_type, result.payload),
                )
            )
        except PgAssistantClientError as exc:
            has_failure = True
            run.job_results.append(
                JobResult(
                    job_type=job_type,
                    status=RunStatus.failed,
                    error_message=str(exc),
                )
            )

    run.finished_at = datetime.now(timezone.utc)
    if has_failure:
        successful = [r for r in run.job_results if r.status == RunStatus.completed]
        run.status = RunStatus.partial if successful else RunStatus.failed
    else:
        run.status = RunStatus.completed

    run_store.update_run(run)
    await repository.save_run_finished(run)
    return run


def source_to_collect_request(source: dict, override_jobs: list[JobType] | None = None) -> CollectRequest:
    return CollectRequest(
        target_id=source["id"],
        conn_str=source.get("conn_str"),
        pgassistant_api_url=source["pgassistant_api_url"],
        jobs=override_jobs or source["jobs"],
        environment=source.get("environment"),
        group=source.get("group"),
        metadata=source.get("metadata", {}),
    )


async def execute_collect_all_job(
    *,
    parent_job_id: UUID,
    source_path: str,
    include_disabled: bool,
    override_jobs: list[JobType] | None,
    metadata: dict,
) -> None:
    parent = run_store.get_parent_job(parent_job_id)
    if parent is None:
        return

    parent.status = RunStatus.running
    run_store.update_parent_job(parent)

    try:
        sources = load_sources_from_path(source_path)
        enabled_sources = [
            source for source in sources if include_disabled or source.get("enabled", True)
        ]
        parent.targets_queued = len(enabled_sources)
        run_store.update_parent_job(parent)

        semaphore = asyncio.Semaphore(settings.max_concurrent_collects)

        async def run_one(source: dict) -> None:
            async with semaphore:
                request = source_to_collect_request(source, override_jobs=override_jobs)
                request.metadata = {
                    **request.metadata,
                    **metadata,
                    "collect_all_job_id": str(parent_job_id),
                }
                run = await execute_collect_request(
                    request,
                    trigger_type="collect_all",
                    parent_job_id=parent_job_id,
                )

                parent_current = run_store.get_parent_job(parent_job_id)
                if parent_current is not None:
                    parent_current.run_ids.append(run.run_id)
                    if run.status == RunStatus.completed:
                        parent_current.targets_completed += 1
                    elif run.status == RunStatus.partial:
                        parent_current.targets_partial += 1
                    else:
                        parent_current.targets_failed += 1
                    run_store.update_parent_job(parent_current)

        await asyncio.gather(*(run_one(source) for source in enabled_sources))

        parent = run_store.get_parent_job(parent_job_id)
        if parent is not None:
            parent.finished_at = datetime.now(timezone.utc)
            if parent.targets_failed == 0 and parent.targets_partial == 0:
                parent.status = RunStatus.completed
            elif parent.targets_completed > 0 or parent.targets_partial > 0:
                parent.status = RunStatus.partial
            else:
                parent.status = RunStatus.failed
            run_store.update_parent_job(parent)

    except Exception as exc:
        parent = run_store.get_parent_job(parent_job_id)
        if parent is not None:
            parent.status = RunStatus.failed
            parent.finished_at = datetime.now(timezone.utc)
            parent.error_message = str(exc)
            run_store.update_parent_job(parent)
