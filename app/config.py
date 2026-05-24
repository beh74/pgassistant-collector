from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    service_name: str = "pgassistant-collector"
    default_sources_path: str = "config/sources.yaml"
    request_timeout_seconds: float = 60.0
    max_concurrent_collects: int = 4
    api_token: str | None = None
    repository_dsn: str | None = None

    class Config:
        env_prefix = "PGA_COLLECTOR_"


settings = Settings()
