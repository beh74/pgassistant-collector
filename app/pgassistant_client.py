from __future__ import annotations

import json
from dataclasses import dataclass
from time import perf_counter
from urllib.parse import urlparse

import httpx

from app.models import DbConfig, JobType
from app.security import mask_secret


class PgAssistantClientError(Exception):
    pass


@dataclass
class PgAssistantCallResult:
    status_code: int
    response_time_ms: int
    payload: dict


def parse_conn_str(conn_str: str) -> DbConfig:
    """
    Validate a PostgreSQL connection URI and keep it intact.

    The collector must not decompose the URI into host/user/password fields,
    otherwise libpq URI parameters such as options, sslmode, connect_timeout,
    application_name or target_session_attrs are lost before reaching
    pgAssistant.
    """
    conn_str = conn_str.strip()
    parsed = urlparse(conn_str)

    if parsed.scheme not in {"postgresql", "postgres"}:
        raise PgAssistantClientError("Only postgresql:// connection strings are supported")
    if not parsed.hostname:
        raise PgAssistantClientError("Missing hostname in connection string")
    if not parsed.path or parsed.path == "/":
        raise PgAssistantClientError("Missing database name in connection string")
    if parsed.username is None:
        raise PgAssistantClientError("Missing username in connection string")
    if parsed.password is None:
        raise PgAssistantClientError("Missing password in connection string")

    return DbConfig(db_uri=conn_str)


def endpoint_for_job(job_type: JobType) -> str:
    match job_type:
        case JobType.rank_top_10_queries:
            return "/api/v1/rank_top_10_queries"
        case JobType.global_advisor_top_10:
            return "/api/v1/global_advisor"
        case _:
            raise PgAssistantClientError(f"Unsupported job type: {job_type}")


class PgAssistantClient:
    def __init__(self, timeout_seconds: float = 60.0):
        self.timeout_seconds = timeout_seconds

    async def collect_job(
        self,
        *,
        pgassistant_api_url: str,
        db_config: DbConfig,
        job_type: JobType,
    ) -> PgAssistantCallResult:
        endpoint = endpoint_for_job(job_type)
        url = pgassistant_api_url.rstrip("/") + endpoint

        if db_config.db_uri:
            payload_db_config = {"db_uri": db_config.db_uri}
        else:
            missing_fields = [
                field_name
                for field_name in ("db_host", "db_name", "db_user", "db_password")
                if not getattr(db_config, field_name)
            ]
            if missing_fields:
                raise PgAssistantClientError(
                    "Missing database connection fields: " + ", ".join(missing_fields)
                )

            payload_db_config = {
                "db_host": db_config.db_host,
                "db_port": db_config.db_port,
                "db_name": db_config.db_name,
                "db_user": db_config.db_user,
                "db_password": db_config.db_password,
            }

        payload = {"db_config": payload_db_config}
        started = perf_counter()

        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                # pgAssistant currently accepts GET with a raw JSON body.
                # This intentionally mirrors:
                # curl -X GET ... -H "Content-Type: application/json" --data-raw '{...}'
                response = await client.request(
                    "GET",
                    url,
                    content=json.dumps(payload),
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                )

        except httpx.HTTPError as exc:
            raise PgAssistantClientError(
                f"pgAssistant call failed for {job_type}. "
                f"url={url}. "
                f"error={mask_secret(str(exc))}"
            ) from exc

        elapsed_ms = int((perf_counter() - started) * 1000)

        response_text_preview = response.text[:1000]

        try:
            response_payload = response.json()
        except ValueError as exc:
            raise PgAssistantClientError(
                f"pgAssistant returned non-JSON response for {job_type}. "
                f"url={url}. "
                f"status_code={response.status_code}. "
                f"body={mask_secret(response_text_preview)}"
            ) from exc

        if response.status_code >= 400:
            raise PgAssistantClientError(
                f"pgAssistant returned HTTP {response.status_code} for {job_type}. "
                f"url={url}. "
                f"body={mask_secret(response_text_preview)}"
            )

        return PgAssistantCallResult(
            status_code=response.status_code,
            response_time_ms=elapsed_ms,
            payload=response_payload,
        )