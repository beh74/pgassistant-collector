from __future__ import annotations

import hashlib
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from app.config import settings
from app.models import RunRecord


class Repository:
    """
    PostgreSQL persistence for collected runs and raw pgAssistant payloads.

    If PGA_COLLECTOR_REPOSITORY_DSN is not set, the repository is disabled and
    the application still works with the in-memory RunStore.
    """

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
                    trigger_type,
                    status,
                    environment,
                    target_group,
                    metadata,
                    jobs_requested,
                    started_at,
                    finished_at,
                    error_message
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    started_at = EXCLUDED.started_at,
                    metadata = EXCLUDED.metadata,
                    jobs_requested = EXCLUDED.jobs_requested
                """,
                (
                    run.run_id,
                    run.parent_job_id,
                    run.target_id,
                    run.trigger_type,
                    run.status.value,
                    run.environment,
                    run.group,
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
                INSERT INTO pga_collection_raw_payload (
                    run_id,
                    target_id,
                    job_type,
                    raw_payload
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    run.run_id,
                    run.target_id,
                    job_type,
                    Jsonb(payload),
                ),
            )

            if job_type == "rank_top_10_queries":
                await self._save_ranked_queries(conn, run, payload)
            elif job_type == "global_advisor_top_10":
                await self._save_global_advisor(conn, run, payload)

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
                    run_id,
                    target_id,

                    rank_position,
                    queryid,

                    priority_score,
                    priority_level,
                    reason,

                    calls,
                    rows,
                    rows_per_call,

                    total_exec_time_ms,
                    mean_exec_time_ms,
                    min_exec_time_ms,
                    max_exec_time_ms,
                    stddev_exec_time_ms,

                    share_calls,
                    share_total_time,
                    share_io,

                    cache_hit_ratio,
                    cache_miss_share,

                    shared_blks_hit,
                    shared_blks_read,
                    shared_blks_written,

                    total_blks_read,
                    total_blks_written,

                    temp_blks_read,
                    temp_blks_written,

                    local_blks_hit,
                    local_blks_read,
                    local_blks_written,

                    wal_bytes,
                    wal_records,
                    wal_fpi,

                    query,
                    signals,
                    raw_payload
                )
                VALUES (
                    %s, %s,
                    %s, %s,
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

                    item.get("rank_position") or item.get("rank") or index,
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

    async def _save_global_advisor(
        self,
        conn: psycopg.AsyncConnection,
        run: RunRecord,
        payload: dict[str, Any],
    ) -> None:
        items = _extract_list(
            payload,
            preferred_keys=("ranked_queries", "recommendations", "data", "results"),
        )

        for index, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                continue

            recommendation_id = _as_text(item.get("recommendation_id"))
            advisor_group = _as_text(item.get("advisor_group"))
            category_id = _as_text(item.get("category_id"))
            outcome_id = _as_text(item.get("outcome_id"))
            source = _as_text(item.get("source"))

            object_type = _as_text(item.get("object_type"))
            object_name = _as_text(item.get("object_name"))
            schema_name = _as_text(item.get("schema_name"))
            table_name = _as_text(item.get("table_name"))
            column_name = _as_text(item.get("column_name"))
            index_name = _as_text(item.get("index_name"))

            title = _as_text(item.get("title"))
            label = _as_text(item.get("label"))
            recommendation_note = _as_text(item.get("recommendation_note"))

            fingerprint = _fingerprint(
                run.target_id,
                recommendation_id,
                advisor_group,
                category_id,
                object_type,
                schema_name,
                table_name,
                column_name,
                index_name,
                object_name,
                recommendation_note,
            )

            await conn.execute(
                """
                INSERT INTO pga_global_advisor_snapshot (
                    run_id,
                    target_id,

                    rank_position,

                    recommendation_id,
                    title,
                    label,
                    description,
                    recommendation_note,
                    why_it_matters,
                    expected_benefit,
                    fix_strategy,
                    improvement_sql,

                    advisor_group,
                    category_id,
                    outcome_id,
                    source,

                    priority,
                    risk_level,
                    action_type,
                    action_safety,

                    confidence,
                    impact,
                    effort,
                    estimated_rows,

                    can_auto_apply,
                    can_generate_sql,
                    manual_review_required,
                    requires_lock,
                    requires_maintenance_window,

                    object_type,
                    object_id,
                    object_name,

                    schema_name,
                    schema_id,
                    table_name,
                    table_id,
                    column_name,
                    index_name,

                    query_id,

                    tags,

                    finding_fingerprint,
                    raw_payload
                )
                VALUES (
                    %s, %s,

                    %s,

                    %s, %s, %s, %s, %s, %s, %s, %s, %s,

                    %s, %s, %s, %s,

                    %s, %s, %s, %s,

                    %s, %s, %s, %s,

                    %s, %s, %s, %s, %s,

                    %s, %s, %s,

                    %s, %s, %s, %s, %s, %s,

                    %s,

                    %s,

                    %s, %s
                )
                """,
                (
                    run.run_id,
                    run.target_id,

                    _as_int(item.get("rank")) or _as_int(item.get("rank_position")) or index,

                    recommendation_id,
                    title,
                    label,
                    _as_text(item.get("description")),
                    recommendation_note,
                    _as_text(item.get("why_it_matters")),
                    _as_text(item.get("expected_benefit")),
                    _as_text(item.get("fix_strategy")),
                    _as_text(item.get("improvement_sql")),

                    advisor_group,
                    category_id,
                    outcome_id,
                    source,

                    _as_text(item.get("priority")),
                    _as_text(item.get("risk_level")),
                    _as_text(item.get("action_type")),
                    _as_text(item.get("action_safety")),

                    _as_number(item.get("confidence")),
                    _as_number(item.get("impact")),
                    _as_number(item.get("effort")),
                    _as_int(item.get("estimated_rows")),

                    _as_bool(item.get("can_auto_apply")),
                    _as_bool(item.get("can_generate_sql")),
                    _as_bool(item.get("manual_review_required")),
                    _as_bool(item.get("requires_lock")),
                    _as_bool(item.get("requires_maintenance_window")),

                    object_type,
                    _as_int(item.get("object_id")),
                    object_name,

                    schema_name,
                    _as_int(item.get("schema_id")),
                    table_name,
                    _as_int(item.get("table_id")),
                    column_name,
                    index_name,

                    _as_text(item.get("query_id")),

                    Jsonb(item.get("tags", [])),

                    fingerprint,
                    Jsonb(item),
                ),
            )


def _extract_list(payload: dict[str, Any], preferred_keys: tuple[str, ...]) -> list[Any]:
    for key in preferred_keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value

    if isinstance(payload, list):
        return payload

    return []


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)

def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "yes", "y", "1"}:
            return True
        if normalized in {"false", "f", "no", "n", "0"}:
            return False

    return None

    return None

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


def _fingerprint(*parts: str | None) -> str:
    normalized = "|".join(part or "" for part in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


repository = Repository()
