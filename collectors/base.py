from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .signature import redact_sensitive_text


@dataclass
class CollectorStatus:
    state: str = "stopped"
    connected_at: datetime | None = None
    last_message_at: datetime | None = None
    last_event_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    last_error: str | None = None
    connection_attempts: int = 0
    reconnect_count: int = 0
    heartbeat_count: int = 0
    event_count: int = 0
    decode_error_count: int = 0
    parse_error_count: int = 0
    unsupported_message_count: int = 0
    needs_refresh: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "connected_at": self.connected_at.isoformat() if self.connected_at else None,
            "last_message_at": self.last_message_at.isoformat() if self.last_message_at else None,
            "last_event_at": self.last_event_at.isoformat() if self.last_event_at else None,
            "last_heartbeat_at": (
                self.last_heartbeat_at.isoformat() if self.last_heartbeat_at else None
            ),
            "last_error": self.last_error,
            "connection_attempts": self.connection_attempts,
            "reconnect_count": self.reconnect_count,
            "heartbeat_count": self.heartbeat_count,
            "event_count": self.event_count,
            "decode_error_count": self.decode_error_count,
            "parse_error_count": self.parse_error_count,
            "unsupported_message_count": self.unsupported_message_count,
            "needs_refresh": self.needs_refresh,
            "metadata": self.metadata,
        }


class Collector(ABC):
    """Raw event source contract. It knows nothing about the Event model."""

    def __init__(self) -> None:
        self.status = CollectorStatus()

    @abstractmethod
    async def connect(self) -> None:
        """Open a platform connection."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close a platform connection."""

    @abstractmethod
    def iter_raw_events(self) -> AsyncIterator[dict[str, Any]]:
        """Yield raw platform payloads."""

    async def reconnect(self) -> None:
        """Reconnect once for callers that need an explicit lifecycle hook."""
        self.status.reconnect_count += 1
        await self.disconnect()
        await self.connect()

    async def heartbeat(self) -> None:
        """Record a heartbeat; concrete transports may send a protocol frame."""
        self.status.heartbeat_count += 1
        self.status.last_heartbeat_at = datetime.now(timezone.utc)
        self.status.last_error = None

    async def wait_for_connection_refresh(self) -> bool:
        """Wait for a provider update, when the collector supports one."""
        return False

    def mark_connect_attempt(self) -> None:
        self.status.connection_attempts += 1

    def mark_message(self) -> None:
        self.status.last_message_at = datetime.now(timezone.utc)

    def mark_event(self) -> None:
        """Backward-compatible alias used by simple collectors for raw messages."""
        self.mark_message()

    def mark_normalized_event(self) -> None:
        self.status.last_event_at = datetime.now(timezone.utc)
        self.status.event_count += 1

    def mark_decode_error(self, error: str | None = None) -> None:
        self.status.decode_error_count += 1
        if error:
            self.status.last_error = redact_sensitive_text(error)

    def mark_parse_error(self, error: str | None = None) -> None:
        self.status.parse_error_count += 1
        if error:
            self.status.last_error = redact_sensitive_text(error)

    def mark_unsupported_message(self) -> None:
        self.status.unsupported_message_count += 1
