from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class JobType(str, Enum):
    rank_top_10_queries = "rank_top_10_queries"
    global_advisor_top_10 = "global_advisor_top_10"


class RunStatus(str, Enum):
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"
    partial = "partial"


class DbConfig(BaseModel):
    # Preferred format: keep the full PostgreSQL URI intact so libpq options
    # such as sslmode, connect_timeout, application_name and options
    # (for example search_path) are preserved.
    db_uri: str | None = Field(default=None, exclude=True)

    # Legacy decomposed connection fields. Kept for backward compatibility.
    db_host: str | None = None
    db_port: int = 5432
    db_name: str | None = None
    db_user: str | None = None
    db_password: str | None = Field(default=None, exclude=True)


class CollectRequest(BaseModel):
    target_id: str = Field(..., examples=["northwind-demo"])
    conn_str: str | None = Field(default=None, exclude=True)
    db_config: DbConfig | None = Field(default=None, exclude=True)
    pgassistant_api_url: str = Field(..., examples=["http://localhost:8080"])
    jobs: list[JobType] = Field(
        default_factory=lambda: [
            JobType.rank_top_10_queries,
            JobType.global_advisor_top_10,
        ]
    )
    environment: str | None = None
    group: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CollectAllRequest(BaseModel):
    source_path: str | None = None
    include_disabled: bool = False
    jobs: list[JobType] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class JobResult(BaseModel):
    job_type: JobType
    status: RunStatus
    response_time_ms: int | None = None
    error_message: str | None = None
    payload_summary: dict[str, Any] = Field(default_factory=dict)


class RunRecord(BaseModel):
    run_id: UUID = Field(default_factory=uuid4)
    parent_job_id: UUID | None = None
    target_id: str
    trigger_type: Literal["api", "collect_all"]
    status: RunStatus = RunStatus.queued
    started_at: datetime | None = None
    finished_at: datetime | None = None
    environment: str | None = None
    group: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    jobs_requested: list[JobType] = Field(default_factory=list)
    job_results: list[JobResult] = Field(default_factory=list)
    error_message: str | None = None


class ParentJobRecord(BaseModel):
    job_id: UUID = Field(default_factory=uuid4)
    status: RunStatus = RunStatus.queued
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    run_ids: list[UUID] = Field(default_factory=list)
    targets_queued: int = 0
    targets_completed: int = 0
    targets_failed: int = 0
    targets_partial: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None


class CollectResponse(BaseModel):
    run_id: UUID
    target_id: str
    status: RunStatus


class CollectAllResponse(BaseModel):
    job_id: UUID
    status: RunStatus
    targets_queued: int


class HealthResponse(BaseModel):
    status: str
    service: str = "pgassistant-collector"
    time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
