from __future__ import annotations

import hashlib
import re
from datetime import date
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from app.config import settings
from app.models import PartitionSummary, RunRecord


class Repository:
    """Persist collection runs, query rankings and normalized Executive Plans."""

    def __init__(self) -> None:
        self.dsn = settings.repository_dsn

    @property
    def enabled(self) -> bool:
        return bool(self.dsn)

    async def _connect(self) -> psycopg.AsyncConnection:
        if not self.dsn:
            raise RuntimeError("Repository DSN is not configured")
        return await psycopg.AsyncConnection.connect(self.dsn)

    async def save_run_started(self, run: RunRecord) -> None:
        if not self.enabled:
            return

        async with await self._connect() as conn:
            await conn.execute(
                """
                INSERT INTO pga_collection_run (
                    run_id,
                    parent_job_id,
                    target_id,
                    target_name,
                    trigger_type,
                    status,
                    environment,
                    target_group,
                    db_host,
                    db_port,
                    db_name,
                    db_user,
                    metadata,
                    jobs_requested,
                    started_at,
                    finished_at,
                    error_message
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    target_name = EXCLUDED.target_name,
                    started_at = EXCLUDED.started_at,
                    db_host = EXCLUDED.db_host,
                    db_port = EXCLUDED.db_port,
                    db_name = EXCLUDED.db_name,
                    db_user = EXCLUDED.db_user,
                    metadata = EXCLUDED.metadata,
                    jobs_requested = EXCLUDED.jobs_requested
                """,
                (
                    run.run_id,
                    run.parent_job_id,
                    run.target_id,
                    run.target_name,
                    run.trigger_type,
                    run.status.value,
                    run.environment,
                    run.group,
                    run.db_host,
                    run.db_port,
                    run.db_name,
                    run.db_user,
                    Jsonb(run.metadata),
                    [job.value for job in run.jobs_requested],
                    run.started_at,
                    run.finished_at,
                    run.error_message,
                ),
            )

    async def save_run_finished(self, run: RunRecord) -> None:
        if not self.enabled:
            return

        async with await self._connect() as conn:
            await conn.execute(
                """
                UPDATE pga_collection_run
                SET status = %s,
                    finished_at = %s,
                    error_message = %s,
                    metadata = %s
                WHERE run_id = %s
                """,
                (
                    run.status.value,
                    run.finished_at,
                    run.error_message,
                    Jsonb(run.metadata),
                    run.run_id,
                ),
            )

            await conn.execute(
                "DELETE FROM pga_collection_job_result WHERE run_id = %s",
                (run.run_id,),
            )

            for result in run.job_results:
                await conn.execute(
                    """
                    INSERT INTO pga_collection_job_result (
                        run_id,
                        job_type,
                        status,
                        response_time_ms,
                        error_message,
                        payload_summary
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run.run_id,
                        result.job_type.value,
                        result.status.value,
                        result.response_time_ms,
                        result.error_message,
                        Jsonb(result.payload_summary),
                    ),
                )

    async def save_job_payload(self, run: RunRecord, job_type: str, payload: dict) -> None:
        if not self.enabled:
            return

        async with await self._connect() as conn:
            await conn.execute(
                """
                INSERT INTO pga_collection_payload (
                    run_id,
                    target_id,
                    job_type,
                    raw_payload
                )
                VALUES (%s, %s, %s, %s)
                """,
                (run.run_id, run.target_id, job_type, Jsonb(payload)),
            )

            if job_type == "rank_top_10_queries":
                await self._save_ranked_queries(conn, run, payload)
            elif job_type == "executive_plan":
                await self._save_executive_plan(conn, run, payload)

    async def create_weekly_partitions(
        self,
        *,
        from_date: date,
        weeks_ahead: int,
        weeks_back: int,
    ) -> list[PartitionSummary]:
        if not self.enabled:
            raise RuntimeError("Repository DSN is not configured")

        async with await self._connect() as conn:
            await conn.execute(
                "SELECT pga_create_weekly_partitions(%s, %s, %s)",
                (from_date, weeks_ahead, weeks_back),
            )
            return await self._partition_summaries(conn)

    async def drop_partitions_older_than(
        self,
        *,
        retain_weeks: int,
    ) -> tuple[int, list[PartitionSummary]]:
        if not self.enabled:
            raise RuntimeError("Repository DSN is not configured")

        async with await self._connect() as conn:
            cursor = await conn.execute(
                "SELECT pga_drop_partitions_older_than(%s)",
                (retain_weeks,),
            )
            dropped_partitions = await cursor.fetchone()
            return (
                int(dropped_partitions[0]) if dropped_partitions else 0,
                await self._partition_summaries(conn),
            )

    async def _partition_summaries(
        self,
        conn: psycopg.AsyncConnection,
    ) -> list[PartitionSummary]:
        cursor = await conn.execute(
            """
            SELECT
                parent_table::text,
                count(*)::integer AS partitions,
                min(range_start) AS first_partition,
                max(range_end) AS last_partition
            FROM pga_partition_registry
            GROUP BY parent_table
            ORDER BY parent_table
            """
        )
        rows = await cursor.fetchall()
        return [
            PartitionSummary(
                parent_table=row[0],
                partitions=row[1],
                first_partition=row[2],
                last_partition=row[3],
            )
            for row in rows
        ]

    async def _save_ranked_queries(
        self,
        conn: psycopg.AsyncConnection,
        run: RunRecord,
        payload: dict[str, Any],
    ) -> None:
        items = _extract_list(
            payload,
            preferred_keys=("ranked_queries", "queries", "data", "results"),
        )

        for index, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                continue

            await conn.execute(
                """
                INSERT INTO pga_ranked_query_snapshot (
                    run_id, target_id, rank_position, queryid,
                    priority_score, priority_level, reason,
                    calls, rows, rows_per_call,
                    total_exec_time_ms, mean_exec_time_ms, min_exec_time_ms,
                    max_exec_time_ms, stddev_exec_time_ms,
                    share_calls, share_total_time, share_io,
                    cache_hit_ratio, cache_miss_share,
                    shared_blks_hit, shared_blks_read, shared_blks_written,
                    total_blks_read, total_blks_written,
                    temp_blks_read, temp_blks_written,
                    local_blks_hit, local_blks_read, local_blks_written,
                    wal_bytes, wal_records, wal_fpi,
                    query, signals, raw_payload
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                """,
                (
                    run.run_id,
                    run.target_id,
                    _as_int(item.get("rank_position") or item.get("rank")) or index,
                    _as_text(item.get("queryid") or item.get("query_id")),
                    _as_number(item.get("priority_score") or item.get("score")),
                    _as_text(item.get("priority_level")),
                    _as_text(item.get("reason")),
                    _as_int(item.get("calls")),
                    _as_int(item.get("rows")),
                    _as_number(item.get("rows_per_call")),
                    _as_number(item.get("total_exec_time_ms") or item.get("total_exec_time") or item.get("total_time_ms")),
                    _as_number(item.get("mean_exec_time_ms") or item.get("mean_exec_time") or item.get("mean_time_ms")),
                    _as_number(item.get("min_exec_time_ms") or item.get("min_exec_time")),
                    _as_number(item.get("max_exec_time_ms") or item.get("max_exec_time")),
                    _as_number(item.get("stddev_exec_time_ms") or item.get("stddev_exec_time")),
                    _as_number(item.get("share_calls")),
                    _as_number(item.get("share_total_time")),
                    _as_number(item.get("share_io")),
                    _as_number(item.get("cache_hit_ratio")),
                    _as_number(item.get("cache_miss_share")),
                    _as_int(item.get("shared_blks_hit")),
                    _as_int(item.get("shared_blks_read")),
                    _as_int(item.get("shared_blks_written")),
                    _as_int(item.get("total_blks_read")),
                    _as_int(item.get("total_blks_written")),
                    _as_int(item.get("temp_blks_read")),
                    _as_int(item.get("temp_blks_written")),
                    _as_int(item.get("local_blks_hit")),
                    _as_int(item.get("local_blks_read")),
                    _as_int(item.get("local_blks_written")),
                    _as_number(item.get("wal_bytes")),
                    _as_int(item.get("wal_records")),
                    _as_int(item.get("wal_fpi")),
                    item.get("query"),
                    Jsonb(item.get("signals", [])),
                    Jsonb(item),
                ),
            )

    async def _save_executive_plan(
        self,
        conn: psycopg.AsyncConnection,
        run: RunRecord,
        payload: dict[str, Any],
    ) -> None:
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        errors = payload.get("errors") if isinstance(payload.get("errors"), list) else []
        teams = summary.get("teams") if isinstance(summary.get("teams"), dict) else {}
        phases = [item for item in payload.get("phases", []) if isinstance(item, dict)]
        tasks = [item for item in payload.get("tasks", []) if isinstance(item, dict)]

        await conn.execute(
            """
            INSERT INTO pga_executive_plan_snapshot (
                run_id, target_id, database_name, status,
                recommendations_collected,
                recommendations_after_deduplication,
                task_count, phase_count,
                dev_task_count, ops_task_count, dev_ops_task_count,
                advisor_errors, summary
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run.run_id,
                run.target_id,
                _as_text(payload.get("database")),
                "partial" if errors else _as_text(payload.get("status")) or "ok",
                _as_int(summary.get("recommendations_collected")) or 0,
                _as_int(summary.get("recommendations_after_deduplication")) or 0,
                _as_int(summary.get("tasks")) or len(tasks),
                _as_int(summary.get("phases")) or len(phases),
                _as_int(teams.get("DEV")) or 0,
                _as_int(teams.get("OPS")) or 0,
                _as_int(teams.get("DEV_OPS")) or 0,
                Jsonb(errors),
                Jsonb(summary),
            ),
        )

        phase_by_number = {
            _as_int(phase.get("number")): phase
            for phase in phases
            if _as_int(phase.get("number")) is not None
        }
        for task in tasks:
            phase_number = _as_int(task.get("phase"))
            if phase_number is not None and phase_number not in phase_by_number:
                phase_by_number[phase_number] = {
                    "number": phase_number,
                    "name": task.get("phase_name") or "Unclassified",
                    "rationale": task.get("phase_rationale") or "",
                    "tasks": [],
                }

        for phase_order, phase_number in enumerate(sorted(phase_by_number), start=1):
            phase = phase_by_number[phase_number]
            phase_tasks = phase.get("tasks") if isinstance(phase.get("tasks"), list) else []
            await conn.execute(
                """
                INSERT INTO pga_executive_plan_phase_snapshot (
                    run_id, phase_number, phase_order, name, rationale,
                    team, teams, task_count, sql_count,
                    requires_restart, requires_maintenance_window, raw_payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run.run_id,
                    phase_number,
                    phase_order,
                    _as_text(phase.get("name")) or "Unclassified",
                    _as_text(phase.get("rationale")),
                    _as_text(phase.get("team")),
                    Jsonb(phase.get("teams", [])),
                    len(phase_tasks) if phase_tasks else sum(1 for task in tasks if _as_int(task.get("phase")) == phase_number),
                    _as_int(phase.get("sql_count")) or 0,
                    bool(phase.get("requires_restart")),
                    bool(phase.get("requires_maintenance_window")),
                    Jsonb(_without(phase, "tasks")),
                ),
            )

        for task_order, task in enumerate(tasks, start=1):
            task_id = _as_text(task.get("id")) or _hash(run.target_id, "task", str(task_order))[:16]
            phase_number = _as_int(task.get("phase"))
            if phase_number is None:
                continue
            recommendations = [
                item for item in task.get("recommendations", []) if isinstance(item, dict)
            ]

            await conn.execute(
                """
                INSERT INTO pga_executive_plan_task_snapshot (
                    run_id, task_id, target_id, phase_number, task_order,
                    title, team, priority, score, workstream, scope_name,
                    recommendation_count, sql_count, query_ids, sources,
                    requires_restart, requires_maintenance_window, raw_payload
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    run.run_id,
                    task_id,
                    run.target_id,
                    phase_number,
                    task_order,
                    _as_text(task.get("title")) or "Untitled task",
                    _as_text(task.get("team")),
                    _as_text(task.get("priority")),
                    _as_number(task.get("score")),
                    _as_text(task.get("workstream")),
                    _as_text(task.get("scope_name")),
                    _as_int(task.get("recommendation_count")) or len(recommendations),
                    _as_int(task.get("sql_count")) or 0,
                    Jsonb(task.get("query_ids", [])),
                    Jsonb(task.get("sources", [])),
                    bool(task.get("requires_restart")),
                    bool(task.get("requires_maintenance_window")),
                    Jsonb(_without(task, "recommendations", "recommendation_groups")),
                ),
            )

            for recommendation_order, recommendation in enumerate(recommendations, start=1):
                finding_fingerprint, action_fingerprint = recommendation_fingerprints(
                    run.target_id,
                    recommendation,
                )
                sources = recommendation.get("sources")
                if not isinstance(sources, list):
                    sources = [recommendation.get("source")] if recommendation.get("source") else []

                await conn.execute(
                    """
                    INSERT INTO pga_executive_plan_recommendation_snapshot (
                        run_id, task_id, target_id, recommendation_order,
                        finding_fingerprint, action_fingerprint,
                        source, sources, advisor_id, planning_rule,
                        category_id, action_type, team, priority,
                        impact, confidence, effort,
                        scope, scope_name, urgency, risk_level,
                        database_name, schema_name, table_name, object_name,
                        title, description, recommendation_sql,
                        query_ids, evidence,
                        requires_lock, requires_restart,
                        requires_maintenance_window, raw_payload
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        run.run_id,
                        task_id,
                        run.target_id,
                        recommendation_order,
                        finding_fingerprint,
                        action_fingerprint,
                        _as_text(recommendation.get("source")),
                        Jsonb(sources),
                        _as_text(recommendation.get("advisor_id")),
                        _as_text(recommendation.get("planning_rule")),
                        _as_text(recommendation.get("category_id")),
                        _as_text(recommendation.get("action_type")),
                        _as_text(recommendation.get("team")),
                        _as_text(recommendation.get("priority")),
                        _as_int(recommendation.get("impact")),
                        _as_int(recommendation.get("confidence")),
                        _as_int(recommendation.get("effort")),
                        _as_text(recommendation.get("scope")),
                        _as_text(recommendation.get("scope_name")),
                        _as_text(recommendation.get("urgency")),
                        _as_text(recommendation.get("risk_level")),
                        _as_text(recommendation.get("database")),
                        _as_text(recommendation.get("schema_name")),
                        _as_text(recommendation.get("table_name")),
                        _as_text(recommendation.get("object_name")),
                        _as_text(recommendation.get("title")) or "Untitled recommendation",
                        _as_text(recommendation.get("description")),
                        _as_text(recommendation.get("sql")),
                        Jsonb(recommendation.get("query_ids", [])),
                        Jsonb(recommendation.get("evidence", [])),
                        bool(recommendation.get("requires_lock")),
                        bool(recommendation.get("requires_restart")),
                        bool(recommendation.get("requires_maintenance_window")),
                        Jsonb(recommendation),
                    ),
                )


def recommendation_fingerprints(
    target_id: str,
    recommendation: dict[str, Any],
) -> tuple[str, str]:
    """Build stable finding and exact-action identities for history comparisons."""
    sql = _normalize_sql(recommendation.get("sql"))
    action_type = _as_text(recommendation.get("action_type")) or ""
    semantic_target = ""
    if action_type == "CREATE_INDEX" and sql:
        match = re.search(r"\bon\s+([^\s(]+)\s*\(([^)]*)\)", sql, flags=re.IGNORECASE)
        if match:
            relation = match.group(1).replace('"', "")
            columns = re.sub(r'[\s"]+', "", match.group(2))
            semantic_target = f"{relation}|{columns}"

    finding = _hash(
        target_id,
        _as_text(recommendation.get("advisor_id")),
        _as_text(recommendation.get("category_id")),
        action_type,
        _as_text(recommendation.get("schema_name")),
        _as_text(recommendation.get("table_name")),
        _as_text(recommendation.get("object_name")),
        semantic_target,
    )
    return finding, _hash(finding, sql)


def _extract_list(payload: dict[str, Any], preferred_keys: tuple[str, ...]) -> list[Any]:
    for key in preferred_keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    return []


def _without(value: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key not in keys}


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_sql(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def _hash(*parts: str | None) -> str:
    normalized = "|".join(part or "" for part in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


repository = Repository()
