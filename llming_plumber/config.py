from __future__ import annotations

from typing import Literal

from pydantic import BaseModel
from pydantic_settings import BaseSettings


class LlmTierConfig(BaseModel):
    """Provider + model for a single LLM tier."""

    provider: str
    model: str


class LlmDefaults(BaseModel):
    """Global LLM defaults for three task complexity tiers."""

    complex: LlmTierConfig = LlmTierConfig(
        provider="anthropic", model="claude-opus-4-6"
    )
    medium: LlmTierConfig = LlmTierConfig(
        provider="anthropic", model="claude-sonnet-4-6"
    )
    fast: LlmTierConfig = LlmTierConfig(
        provider="anthropic", model="claude-haiku-4-5-20251001"
    )


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
    run_timeout: int = 7200
    health_check_interval: int = 30
    scheduler_poll_seconds: int = 2
    max_runs_per_pipeline: int = 50

    # API
    api_prefix: str = "/api"
    host: str = "0.0.0.0"
    port: int = 8100

    # API response cache
    api_cache: Literal["on", "off", "aggressive"] = "on"
    api_cache_max_ttl: int = 1800

    # Credentials encryption
    secret_key: str = ""

    # Global LLM defaults — override via env PLUMBER_LLM_COMPLEX_PROVIDER, etc.
    llm: LlmDefaults = LlmDefaults()

    model_config = {
        "env_prefix": "PLUMBER_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


settings = Settings()
