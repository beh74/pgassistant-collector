from __future__ import annotations

from typing import Dict
from uuid import UUID

from app.models import ParentJobRecord, RunRecord


class RunStore:
    def __init__(self) -> None:
        self._runs: Dict[UUID, RunRecord] = {}
        self._jobs: Dict[UUID, ParentJobRecord] = {}

    def create_run(self, run: RunRecord) -> RunRecord:
        self._runs[run.run_id] = run
        return run

    def get_run(self, run_id: UUID) -> RunRecord | None:
        return self._runs.get(run_id)

    def update_run(self, run: RunRecord) -> None:
        self._runs[run.run_id] = run

    def create_parent_job(self, job: ParentJobRecord) -> ParentJobRecord:
        self._jobs[job.job_id] = job
        return job

    def get_parent_job(self, job_id: UUID) -> ParentJobRecord | None:
        return self._jobs.get(job_id)

    def update_parent_job(self, job: ParentJobRecord) -> None:
        self._jobs[job.job_id] = job


run_store = RunStore()
