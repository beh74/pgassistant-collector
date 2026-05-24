from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

from app.models import JobType

ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z0-9_]+)\}")


class SourceConfigError(Exception):
    pass


def expand_env_vars(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        env_name = match.group(1)
        env_value = os.getenv(env_name)
        if env_value is None:
            raise SourceConfigError(f"Missing environment variable: {env_name}")
        return env_value
    return ENV_VAR_PATTERN.sub(replace, value)


def load_yaml_file(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}
    if not isinstance(data, dict):
        raise SourceConfigError(f"YAML file must contain an object: {path}")
    return data


def load_sources_from_path(source_path: str) -> list[dict[str, Any]]:
    path = Path(source_path)
    if not path.exists():
        raise SourceConfigError(f"Source path does not exist: {source_path}")

    yaml_documents: list[dict[str, Any]] = []
    if path.is_file():
        yaml_documents.append(load_yaml_file(path))
    else:
        yaml_files = sorted(path.glob("*.yaml")) + sorted(path.glob("*.yml"))
        for yaml_file in yaml_files:
            yaml_documents.append(load_yaml_file(yaml_file))

    sources: list[dict[str, Any]] = []
    for document in yaml_documents:
        defaults = document.get("defaults", {})
        raw_sources = document.get("sources", [])
        if not isinstance(raw_sources, list):
            raise SourceConfigError("'sources' must be a list")

        for raw_source in raw_sources:
            source = {**defaults, **raw_source}
            conn_str = source.get("conn_str")
            if conn_str:
                source["conn_str"] = expand_env_vars(conn_str)

            jobs = source.get("jobs") or defaults.get("jobs") or [
                JobType.rank_top_10_queries.value,
                JobType.global_advisor_top_10.value,
            ]
            source["jobs"] = [JobType(job) for job in jobs]

            if "id" not in source:
                raise SourceConfigError("Every source must define an 'id'")
            if "pgassistant_api_url" not in source:
                raise SourceConfigError(f"Source {source['id']} must define pgassistant_api_url")
            if "conn_str" not in source:
                raise SourceConfigError(f"Source {source['id']} must define conn_str")

            sources.append(source)

    return sources
