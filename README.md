# pgAssistant Collector - FastAPI MVP

This is a first project skeleton for a pgAssistant collector.

It exposes four endpoints:

- `POST /collect`: collect diagnostics for a single PostgreSQL database supplied in the payload.
- `POST /collect_all`: trigger an asynchronous collection for all enabled databases declared in YAML sources.
- `GET /runs/{run_id}`: inspect either a single run or a parent collect_all job.
- `GET /health`: healthcheck.

## Architecture

![Architecture](medias/architecture.png)

## Design goals

- Keep credentials ownership outside the collector when using `POST /collect`.
- Support YAML-based sources for continuous or batch collection.
- Never persist `conn_str` or `db_password`.
- Prepare a repository PostgreSQL schema for Grafana dashboards.

## Run locally

```bash
export NORTHWIND_DB_PASSWORD=demo
uvicorn app.main:app --reload --host 0.0.0.0 --port 8081
```

## Run with Docker Compose

```bash
docker compose up --build
```

## YAML source example

```yaml
defaults:
  pgassistant_api_url: http://localhost:8080
  jobs:
    - rank_top_10_queries
    - executive_plan

sources:
  - id: northwind-demo
    enabled: true
    environment: demo
    group: demo
    conn_str: postgresql://postgres:${NORTHWIND_DB_PASSWORD}@host.docker.internal:5420/northwind
    metadata:
      app: northwind
      owner: demo-team
```

## POST /collect example

```bash
curl -X POST http://localhost:8081/collect \
  -H "Content-Type: application/json" \
  -d '{
    "target_id": "northwind-demo",
    "environment": "demo",
    "pgassistant_api_url": "http://localhost:8080",
    "conn_str": "postgresql://postgres:demo@host.docker.internal:5420/northwind",
    "jobs": [
      "rank_top_10_queries",
      "executive_plan"
    ],
    "metadata": {
      "source": "manual"
    }
  }'
```

## POST /collect_all example

```bash
curl -X POST http://localhost:8081/collect_all \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "config/sources.yaml",
    "include_disabled": false,
    "metadata": {
      "triggered_by": "manual"
    }
  }'
```

The response returns a `job_id`. Use it with:

```bash
curl http://localhost:8081/runs/<job_id>
```

## pgAssistant API compatibility

The client currently calls pgAssistant using `GET` with a JSON body, because the current pgAssistant API is:

```bash
curl -X GET http://localhost:8080/api/v1/rank_top_10_queries \
  -H "Content-Type: application/json" \
  -d '{ "db_config": { ... } }'
```

The Executive Plan is collected from:

```text
GET /api/v1/executive_plan
```

It consolidates Global, Index, Parameter and Autovacuum advisor results. The
default pgAssistant request timeout is 300 seconds because building the complete
plan is substantially more expensive than collecting the query ranking alone.

## Next steps

- Add API authentication.
- Add host allowlist / denylist for `POST /collect`.


## Repository PostgreSQL

The collector stores collected runs and pgAssistant payloads in a PostgreSQL repository when `PGA_COLLECTOR_REPOSITORY_DSN` is configured.

The provided `docker-compose.yml` starts a dedicated repository database:

```text
postgresql://pga_collector:pga_collector@collector-repository:5432/pga_collector
```

The schema is initialized from:

```text
sql/schema.sql
```

The repository uses a normalized Executive Plan model:

- `pga_collection_payload` stores every complete pgAssistant API response as `jsonb`.
- `pga_ranked_query_snapshot` extracts dashboard-friendly fields for ranked queries.
- `pga_executive_plan_snapshot` stores plan-level summaries and advisor errors.
- `pga_executive_plan_phase_snapshot` stores ordered implementation phases.
- `pga_executive_plan_task_snapshot` stores deployable work packages.
- `pga_executive_plan_recommendation_snapshot` stores normalized recommendations,
  stable finding fingerprints and exact-action fingerprints.
- `pga_collection_run` and `pga_collection_job_result` store execution metadata.

Each run also stores the non-secret database identity (`db_host`, `db_port`,
`db_name`, and `db_user`) used to help select the correct `target_id`. Connection
URIs and passwords are never persisted.

This schema intentionally replaces the former Global Advisor repository design.
There is no in-place migration from the former schema; initialize a new repository
database or recreate the collector repository volume when upgrading.

The high-volume repository tables are partitioned weekly:

- `pga_collection_payload` by `collected_at`.
- `pga_ranked_query_snapshot` by `collected_at`.

The schema creates partitions for the previous week, the current week, and the next 8 weeks by default. To prepare more partitions later:

```sql
SELECT pga_create_weekly_partitions(CURRENT_DATE, 12, 1);
```

To purge partitioned data older than a retention window, drop old weekly partitions:

```sql
SELECT pga_drop_partitions_older_than(8);
```

The argument is the number of weeks to retain. The purge also removes matching old `pga_collection_run` rows after their partitioned child rows have been dropped.

The same operations are exposed by the collector API:

```bash
curl -X POST http://localhost:8081/repository/partitions \
  -H "Content-Type: application/json" \
  -d '{
    "from_date": "2026-06-28",
    "weeks_ahead": 1,
    "weeks_back": 1
  }'
```

```bash
curl -X POST http://localhost:8081/repository/partitions/purge \
  -H "Content-Type: application/json" \
  -d '{
    "retain_weeks": 1
  }'
```

Connection strings and database passwords are never stored in the repository.
