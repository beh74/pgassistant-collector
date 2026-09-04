import asyncio
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from app.collector import execute_collect_request, source_to_collect_request, summarize_payload
from app.models import CollectRequest, JobType, RunRecord, RunStatus
from app.pgassistant_client import PgAssistantCallResult, endpoint_for_job
from app.repository import Repository, recommendation_fingerprints


def sample_plan(*, errors=None):
    recommendation = {
        "source": "global_advisor",
        "sources": ["global_advisor"],
        "advisor_id": "missing_primary_key",
        "planning_rule": "schema_design",
        "category_id": "DESIGN",
        "action_type": "ALTER_TABLE",
        "team": "DEV",
        "priority": "HIGH",
        "impact": 90,
        "confidence": 90,
        "effort": 50,
        "scope": "table",
        "scope_name": "public.orders",
        "database": "application",
        "schema_name": "public",
        "table_name": "orders",
        "object_name": "public.orders",
        "title": "Add a primary key",
        "description": "The table has no primary key.",
        "sql": "ALTER TABLE public.orders ADD PRIMARY KEY (id);",
        "query_ids": [],
    }
    task = {
        "id": "task-1",
        "phase": 10,
        "phase_name": "Protect data integrity",
        "title": "Fix table design",
        "team": "DEV",
        "priority": "HIGH",
        "score": 88,
        "workstream": "SCHEMA",
        "scope_name": "public.orders",
        "recommendations": [recommendation],
        "recommendation_count": 1,
        "sql_count": 1,
        "query_ids": [],
        "sources": ["global_advisor"],
    }
    phase = {
        "number": 10,
        "name": "Protect data integrity",
        "rationale": "Start with correctness.",
        "tasks": [task],
        "sql_count": 1,
        "teams": ["DEV"],
        "team": "DEV",
    }
    return {
        "status": "ok",
        "database": "application",
        "phases": [phase],
        "tasks": [task],
        "errors": errors or [],
        "summary": {
            "recommendations_collected": 1,
            "recommendations_after_deduplication": 1,
            "tasks": 1,
            "phases": 1,
            "teams": {"DEV": 1, "OPS": 0, "DEV_OPS": 0},
        },
    }


class FakeConnection:
    def __init__(self):
        self.executions = []

    async def execute(self, statement, parameters=None):
        self.executions.append((statement, parameters))


class ExecutivePlanCollectionTests(unittest.TestCase):
    def test_executive_plan_job_uses_new_api(self):
        self.assertEqual(endpoint_for_job(JobType.executive_plan), "/api/v1/executive_plan")

    def test_source_defaults_preserve_target_name(self):
        request = source_to_collect_request({
            "id": "orders-production",
            "name": "Orders Production",
            "conn_str": "postgresql://postgres:secret@db/orders",
            "pgassistant_api_url": "http://pgassistant",
            "jobs": [JobType.executive_plan],
        })

        self.assertEqual(request.target_name, "Orders Production")

    def test_summary_exposes_plan_counts_and_errors(self):
        summary = summarize_payload(
            JobType.executive_plan,
            sample_plan(errors=[{"source": "autovacuum", "error": "failed"}]),
        )

        self.assertEqual(summary["recommendations_collected"], 1)
        self.assertEqual(summary["tasks"], 1)
        self.assertEqual(summary["advisor_errors"], 1)

    def test_plan_persistence_writes_snapshot_phase_task_and_recommendation(self):
        repository = Repository()
        connection = FakeConnection()
        run = RunRecord(
            target_id="orders-production",
            target_name="Orders Production",
            trigger_type="api",
        )

        asyncio.run(repository._save_executive_plan(connection, run, sample_plan()))

        statements = "\n".join(statement for statement, _ in connection.executions)
        self.assertIn("pga_executive_plan_snapshot", statements)
        self.assertIn("pga_executive_plan_phase_snapshot", statements)
        self.assertIn("pga_executive_plan_task_snapshot", statements)
        self.assertIn("pga_executive_plan_recommendation_snapshot", statements)
        self.assertEqual(len(connection.executions), 4)

    def test_advisor_errors_make_job_and_run_partial(self):
        request = CollectRequest(
            target_id="orders-production",
            conn_str="postgresql://postgres:secret@db/orders",
            pgassistant_api_url="http://pgassistant",
            jobs=[JobType.executive_plan],
        )
        result = PgAssistantCallResult(
            status_code=200,
            response_time_ms=100,
            payload=sample_plan(errors=[{"source": "autovacuum", "error": "failed"}]),
        )

        with (
            patch("app.collector.repository.save_run_started", new=AsyncMock()),
            patch("app.collector.repository.save_job_payload", new=AsyncMock()),
            patch("app.collector.repository.save_run_finished", new=AsyncMock()),
            patch("app.collector.PgAssistantClient.collect_job", new=AsyncMock(return_value=result)),
        ):
            run = asyncio.run(execute_collect_request(request, trigger_type="api"))

        self.assertEqual(run.status, RunStatus.partial)
        self.assertEqual(run.job_results[0].status, RunStatus.partial)


class RecommendationFingerprintTests(unittest.TestCase):
    def test_query_ids_do_not_change_finding_identity(self):
        recommendation = sample_plan()["tasks"][0]["recommendations"][0]
        first = recommendation_fingerprints("orders-production", recommendation)
        second = recommendation_fingerprints(
            "orders-production",
            {**recommendation, "query_ids": ["42"]},
        )

        self.assertEqual(first, second)

    def test_sql_change_preserves_finding_but_changes_action(self):
        recommendation = sample_plan()["tasks"][0]["recommendations"][0]
        first_finding, first_action = recommendation_fingerprints(
            "orders-production",
            recommendation,
        )
        second_finding, second_action = recommendation_fingerprints(
            "orders-production",
            {**recommendation, "sql": "ALTER TABLE public.orders ADD PRIMARY KEY (order_id);"},
        )

        self.assertEqual(first_finding, second_finding)
        self.assertNotEqual(first_action, second_action)


@unittest.skipUnless(
    os.getenv("PGA_COLLECTOR_TEST_REPOSITORY_DSN"),
    "PGA_COLLECTOR_TEST_REPOSITORY_DSN is not configured",
)
class ExecutivePlanRepositoryIntegrationTests(unittest.TestCase):
    def test_complete_plan_is_persisted_in_fresh_schema(self):
        asyncio.run(self._run_test())

    async def _run_test(self):
        repository = Repository()
        repository.dsn = os.environ["PGA_COLLECTOR_TEST_REPOSITORY_DSN"]
        run = RunRecord(
            target_id="orders-production",
            target_name="Orders Production",
            trigger_type="api",
            status=RunStatus.running,
            started_at=datetime.now(timezone.utc),
            db_host="db.example.com",
            db_port=5432,
            db_name="application",
            db_user="collector",
            jobs_requested=[JobType.executive_plan],
        )

        await repository.save_run_started(run)
        try:
            await repository.save_job_payload(run, JobType.executive_plan.value, sample_plan())

            async with await repository._connect() as connection:
                counts = []
                for table in (
                    "pga_executive_plan_snapshot",
                    "pga_executive_plan_phase_snapshot",
                    "pga_executive_plan_task_snapshot",
                    "pga_executive_plan_recommendation_snapshot",
                ):
                    cursor = await connection.execute(
                        f"SELECT count(*) FROM {table} WHERE run_id = %s",
                        (run.run_id,),
                    )
                    row = await cursor.fetchone()
                    counts.append(row[0])

            self.assertEqual(counts, [1, 1, 1, 1])
        finally:
            async with await repository._connect() as connection:
                await connection.execute(
                    "DELETE FROM pga_collection_run WHERE run_id = %s",
                    (run.run_id,),
                )


if __name__ == "__main__":
    unittest.main()
