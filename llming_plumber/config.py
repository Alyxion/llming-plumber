from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Plumber application settings, loaded from environment variables and .env file."""

    # MongoDB
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "plumber"

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_cluster: bool = False

    # Deployment mode
    mode: Literal["all", "ui", "worker"] = "all"

    # Lemming (worker) settings
    lemming_concurrency: int = 4
    run_timeout: int = 3600
    health_check_interval: int = 30

    # API
    api_prefix: str = "/api"
    host: str = "0.0.0.0"
    port: int = 8000

    # API response cache
    api_cache: Literal["on", "off", "aggressive"] = "on"
    api_cache_max_ttl: int = 1800

    # Credentials encryption
    secret_key: str = ""

    model_config = {
        "env_prefix": "PLUMBER_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


settings = Settings()
