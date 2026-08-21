from datetime import datetime
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables or .env."""

    app_name: str = "Douyin Live Event Service"
    app_version: str = "0.1.0"
    environment: str = "development"
    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65535)
    log_level: str = "INFO"
    collector_mode: str = "mock"
    mock_room_id: str = "mock-room-001"
    mock_interval_seconds: float = Field(default=2.0, gt=0)
    douyin_ws_url: str | None = None
    douyin_room_id: str = ""
    douyin_room_title: str | None = None
    douyin_ttwid: str | None = None
    douyin_ws_expires_at: datetime | None = None
    douyin_signature_refresh_margin_seconds: float = Field(default=15.0, ge=0)
    douyin_user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
    event_bus_queue_size: int = Field(default=100, gt=0)
    reconnect_initial_seconds: float = Field(default=3.0, ge=0)
    reconnect_max_seconds: float = Field(default=60.0, gt=0)
    reconnect_jitter_ratio: float = Field(default=0.2, ge=0, le=1)
    reconnect_reset_after_seconds: float = Field(default=10.0, ge=0)
    heartbeat_interval_seconds: float = Field(default=15.0, gt=0)
    websocket_max_size: int | None = Field(default=4 * 1024 * 1024, gt=0)
    protocol_debug: bool = False
    # Kept for .env compatibility with the first skeleton. New deployments
    # should use DOUYIN_RECONNECT_INITIAL_SECONDS.
    reconnect_interval_seconds: float | None = Field(default=None, ge=0)

    @property
    def effective_reconnect_initial_seconds(self) -> float:
        if self.reconnect_interval_seconds is not None:
            return self.reconnect_interval_seconds
        return self.reconnect_initial_seconds

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="DOUYIN_",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
