"""
Environment-driven configuration.

Everything that changes between dev / staging / prod lives here, and
nowhere else. No module should read os.environ directly outside this file.
"""
from functools import lru_cache

from pydantic import Field, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- App ---
    app_name: str = "agent-stateful-scaffold"
    environment: str = Field(default="development")  # development | staging | production
    log_level: str = Field(default="INFO")

    # --- LLM providers ---
    anthropic_api_key: SecretStr | None = None
    openai_api_key: SecretStr | None = None
    default_model: str = Field(default="claude-sonnet-4-6")

    # --- Persistence ---
    database_url: PostgresDsn = Field(
        default="postgresql://postgres:postgres@localhost:5432/agent_scaffold"
    )
    checkpointer_backend: str = Field(default="postgres")  # postgres | redis (future)

    # --- HITL ---
    hitl_approval_timeout_seconds: int = Field(default=60 * 60 * 24 * 7)  # 7 days
    hitl_require_approver_id: bool = Field(default=True)

    # --- API ---
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_key: SecretStr | None = Field(
        default=None,
        description="If set, all /threads endpoints require X-API-Key header matching this.",
    )

    @property
    def is_production(self) -> bool:
        return self.environment == "production"


@lru_cache
def get_settings() -> Settings:
    """Cached settings singleton. Use this everywhere instead of Settings() directly,
    so config is parsed once and every module sees the same values."""
    return Settings()
