from __future__ import annotations

import pytest


class TestSettings:
    """Tests for config.Settings loading from environment variables."""

    def test_defaults(self) -> None:
        """Settings should have sensible defaults when no env vars are set."""
        from llming_plumber.config import Settings

        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.mongo_uri == "mongodb://localhost:27017"
        assert s.mongo_db == "plumber"
        assert s.redis_url == "redis://localhost:6379/0"
        assert s.mode == "all"
        assert s.lemming_concurrency == 4
        assert s.run_timeout == 3600
        assert s.health_check_interval == 30
        assert s.api_prefix == "/api"
        assert s.host == "0.0.0.0"
        assert s.port == 8000
        assert s.api_cache == "on"
        assert s.api_cache_max_ttl == 1800
        assert s.secret_key == ""

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Settings should be overridden by PLUMBER_* environment variables."""
        from llming_plumber.config import Settings

        monkeypatch.setenv("PLUMBER_MONGO_URI", "mongodb://custom:27017")
        monkeypatch.setenv("PLUMBER_MONGO_DB", "test_db")
        monkeypatch.setenv("PLUMBER_REDIS_URL", "redis://custom:6379/1")
        monkeypatch.setenv("PLUMBER_MODE", "worker")
        monkeypatch.setenv("PLUMBER_LEMMING_CONCURRENCY", "8")
        monkeypatch.setenv("PLUMBER_RUN_TIMEOUT", "7200")
        monkeypatch.setenv("PLUMBER_HEALTH_CHECK_INTERVAL", "60")
        monkeypatch.setenv("PLUMBER_API_PREFIX", "/v2/api")
        monkeypatch.setenv("PLUMBER_HOST", "127.0.0.1")
        monkeypatch.setenv("PLUMBER_PORT", "9000")
        monkeypatch.setenv("PLUMBER_API_CACHE", "aggressive")
        monkeypatch.setenv("PLUMBER_API_CACHE_MAX_TTL", "3600")
        monkeypatch.setenv("PLUMBER_SECRET_KEY", "supersecret")

        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.mongo_uri == "mongodb://custom:27017"
        assert s.mongo_db == "test_db"
        assert s.redis_url == "redis://custom:6379/1"
        assert s.mode == "worker"
        assert s.lemming_concurrency == 8
        assert s.run_timeout == 7200
        assert s.health_check_interval == 60
        assert s.api_prefix == "/v2/api"
        assert s.host == "127.0.0.1"
        assert s.port == 9000
        assert s.api_cache == "aggressive"
        assert s.api_cache_max_ttl == 3600
        assert s.secret_key == "supersecret"

    def test_invalid_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An invalid mode value should raise a validation error."""
        from pydantic import ValidationError

        from llming_plumber.config import Settings

        monkeypatch.setenv("PLUMBER_MODE", "invalid")
        with pytest.raises(ValidationError):
            Settings(_env_file=None)  # type: ignore[call-arg]

    def test_module_level_settings_instance(self) -> None:
        """A module-level `settings` instance should be available."""
        from llming_plumber.config import settings

        assert settings is not None
        assert hasattr(settings, "mongo_uri")
