"""Connection information providers for signed Douyin WebSocket sessions.

The core collector intentionally does not know how a short-lived Douyin URL is
created.  This module provides the small boundary needed by the collector and
ships a local provider for a manually captured URL.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Mapping, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


class ConnectionRefreshRequired(RuntimeError):
    """Raised when a provider cannot return a usable connection anymore."""


def redact_sensitive_text(value: str) -> str:
    """Remove credentials and signed query values from error/log text."""

    if not value:
        return value

    redacted = re.sub(
        r"(?i)(signature|x-bogus|xbogus|msToken|ttwid|cookie)=([^&\s]+)",
        r"\1=<redacted>",
        value,
    )
    def redact_url(match: re.Match[str]) -> str:
        url = match.group(0).rstrip(".,;:)]")
        parts = urlsplit(url)
        if parts.scheme not in {"ws", "wss"} or not parts.query:
            return match.group(0)
        # Do not expose any WebSocket query value. The full query is a signed
        # session credential, even when a parameter is not on our known list.
        safe_query = [(key, "<redacted>") for key, _ in parse_qsl(parts.query, keep_blank_values=True)]
        safe_url = urlunsplit(
            (parts.scheme, parts.netloc, parts.path, urlencode(safe_query), parts.fragment)
        )
        return safe_url + match.group(0)[len(url) :]

    return re.sub(r"wss?://[^\s]+", redact_url, redacted)


@dataclass(frozen=True)
class ConnectionInfo:
    """One usable transport session description."""

    websocket_url: str
    room_id: str
    room_title: str | None = None
    ttwid: str | None = None
    user_agent: str = "Mozilla/5.0"
    headers: Mapping[str, str] = field(default_factory=dict)
    expires_at: datetime | None = None

    def is_expired(self, *, safety_margin_seconds: float = 15.0) -> bool:
        if self.expires_at is None:
            return False
        now = datetime.now(timezone.utc)
        expires_at = self.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        return now + timedelta(seconds=safety_margin_seconds) >= expires_at

    def safe_metadata(self) -> dict[str, object]:
        """Return connection metadata that is safe for health responses."""

        return {
            "room_id": self.room_id,
            "room_title": self.room_title,
            "has_ttwid": bool(self.ttwid),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }


class SignatureProvider(Protocol):
    """Provider boundary for short-lived, signed WebSocket connections."""

    async def get_connection(self) -> ConnectionInfo:
        """Return the current connection information."""

    async def invalidate(self, reason: str) -> None:
        """Mark the current connection unusable."""

    async def wait_for_update(self) -> bool:
        """Wait until a new connection is supplied, if the provider supports it."""


class StaticSignedUrlProvider:
    """Provider for a manually captured signed URL.

    ``replace`` is intentionally a small in-process hook for future refresh
    code and tests.  In the local .env workflow, restarting the process loads a
    newly captured URL and has the same effect.
    """

    def __init__(
        self,
        *,
        websocket_url: str,
        room_id: str,
        room_title: str | None = None,
        ttwid: str | None = None,
        user_agent: str = "Mozilla/5.0",
        headers: Mapping[str, str] | None = None,
        expires_at: datetime | None = None,
        safety_margin_seconds: float = 15.0,
    ) -> None:
        self._connection = ConnectionInfo(
            websocket_url=websocket_url,
            room_id=room_id,
            room_title=room_title,
            ttwid=ttwid,
            user_agent=user_agent,
            headers=dict(headers or {}),
            expires_at=expires_at,
        )
        self.safety_margin_seconds = safety_margin_seconds
        self._invalidated = False
        self._last_reason: str | None = None
        self._updated = asyncio.Event()

    @property
    def needs_refresh(self) -> bool:
        return self._invalidated or self._connection.is_expired(
            safety_margin_seconds=self.safety_margin_seconds
        )

    @property
    def last_reason(self) -> str | None:
        return self._last_reason

    async def get_connection(self) -> ConnectionInfo:
        if self._invalidated:
            raise ConnectionRefreshRequired(self._last_reason or "signed connection was invalidated")
        if self._connection.is_expired(safety_margin_seconds=self.safety_margin_seconds):
            self._invalidated = True
            self._last_reason = "signed connection is expired or near expiry"
            raise ConnectionRefreshRequired(self._last_reason)
        return self._connection

    async def invalidate(self, reason: str) -> None:
        self._invalidated = True
        self._last_reason = redact_sensitive_text(reason)

    async def wait_for_update(self) -> bool:
        await self._updated.wait()
        self._updated.clear()
        return True

    def replace(
        self,
        *,
        websocket_url: str,
        room_id: str | None = None,
        room_title: str | None = None,
        ttwid: str | None = None,
        expires_at: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Replace the short-lived connection and wake a waiting collector."""

        previous = self._connection
        self._connection = ConnectionInfo(
            websocket_url=websocket_url,
            room_id=room_id or previous.room_id,
            room_title=room_title if room_title is not None else previous.room_title,
            ttwid=ttwid if ttwid is not None else previous.ttwid,
            user_agent=previous.user_agent,
            headers=dict(headers if headers is not None else previous.headers),
            expires_at=expires_at,
        )
        self._invalidated = False
        self._last_reason = None
        self._updated.set()
