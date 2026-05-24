# pgAssistant Collector - FastAPI MVP

This is a first project skeleton for a pgAssistant collector.

It exposes four endpoints:

- `POST /collect`: collect diagnostics for a single PostgreSQL database supplied in the payload.
- `POST /collect_all`: trigger an asynchronous collection for all enabled databases declared in YAML sources.
- `GET /runs/{run_id}`: inspect either a single run or a parent collect_all job.
- `GET /health`: healthcheck.

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
    - global_advisor_top_10

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
      "global_advisor_top_10"
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

The repository uses a hybrid model:

- `pga_collection_raw_payload` stores the full pgAssistant API response as `jsonb`.
- `pga_ranked_query_snapshot` extracts dashboard-friendly fields for ranked queries.
- `pga_global_advisor_snapshot` extracts dashboard-friendly fields for advisor findings.
- `pga_collection_run` and `pga_collection_job_result` store execution metadata.

Connection strings and database passwords are never stored in the repository.
