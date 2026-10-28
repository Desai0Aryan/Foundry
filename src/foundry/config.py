from __future__ import annotations

from functools import lru_cache

from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_DEV_ADMIN_TOKEN = "dev-admin-token-change-me"  # noqa: S105
_DEV_PEPPER = "dev-pepper-change-me"


class Settings(BaseSettings):
    """All configuration comes from FOUNDRY_* environment variables (12-factor)."""

    model_config = SettingsConfigDict(env_prefix="FOUNDRY_", env_file=".env", extra="ignore")

    environment: str = "development"
    database_url: str = "postgresql://foundry:foundry@localhost:5432/foundry"
    db_pool_min: int = 2
    db_pool_max: int = 20
    auto_migrate: bool = True

    admin_token: SecretStr = SecretStr(_DEV_ADMIN_TOKEN)
    api_key_pepper: SecretStr = SecretStr(_DEV_PEPPER)

    max_payload_bytes: int = 256 * 1024

    # --- broker ---------------------------------------------------------------------
    broker_url: str = "amqp://guest:guest@localhost:5672/"
    broker_prefix: str = "foundry"  # names every exchange/queue; tests use a unique one

    # --- outbox relay ---------------------------------------------------------------
    outbox_batch_size: int = 200
    outbox_poll_seconds: float = 1.0
    outbox_retention_seconds: int = 3600

    # --- workers --------------------------------------------------------------------
    worker_concurrency: int = 4
    lease_seconds: float = 30.0  # a crashed worker's jobs are recovered after this long
    heartbeat_seconds: float = 10.0
    shutdown_grace_seconds: float = 20.0
    sim_speed: float = 1.0  # >1 makes the simulated handlers finish faster

    # --- retries --------------------------------------------------------------------
    backoff_base_seconds: float = 2.0
    backoff_cap_seconds: float = 300.0

    # --- reaper ---------------------------------------------------------------------
    reaper_interval_seconds: float = 2.0
    reaper_batch_size: int = 200
    redrive_after_seconds: float = 900.0
    log_level: str = "INFO"
    log_json: bool = True

    @model_validator(mode="after")
    def _heartbeat_must_fit_in_lease(self) -> Settings:
        if self.heartbeat_seconds * 2 >= self.lease_seconds:
            raise ValueError("heartbeat_seconds must be less than half of lease_seconds")
        return self

    @model_validator(mode="after")
    def _refuse_dev_secrets_in_production(self) -> Settings:
        if self.environment == "production" and (
            self.admin_token.get_secret_value() == _DEV_ADMIN_TOKEN
            or self.api_key_pepper.get_secret_value() == _DEV_PEPPER
        ):
            raise ValueError("refusing to start in production with default dev secrets")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
