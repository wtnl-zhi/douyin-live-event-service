from abc import abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime, timezone
import logging
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

from events.models import Event, EventType, RoomInfo, UserInfo

from .base import Collector
from .douyin_protocol import decode_push_frame, encode_ack, encode_heartbeat, parse_chat_message
from .signature import (
    ConnectionRefreshRequired,
    ConnectionInfo,
    SignatureProvider,
    StaticSignedUrlProvider,
    redact_sensitive_text,
)


class DouyinCollector(Collector):
    """Douyin collector boundary shared by mock and real transports."""

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError("Douyin transport is not implemented")

    @abstractmethod
    async def disconnect(self) -> None:
        raise NotImplementedError("Douyin transport is not implemented")

    @abstractmethod
    def iter_raw_events(self) -> AsyncIterator[dict[str, Any]]:
        raise NotImplementedError("Douyin transport is not implemented")


class DouyinWebSocketCollector(DouyinCollector):
    """Real WebSocket transport for the first supported Douyin event type.

    The signed connection is obtained from a provider. Keeping signature
    creation outside the transport and business layers lets it be replaced
    without changing the Event pipeline.
    """

    def __init__(
        self,
        *,
        provider: SignatureProvider | None = None,
        websocket_url: str | None = None,
        room_id: str = "",
        room_title: str | None = None,
        ttwid: str | None = None,
        user_agent: str = "Mozilla/5.0",
        websocket_max_size: int | None = 4 * 1024 * 1024,
        signature_refresh_margin_seconds: float = 15.0,
        protocol_debug: bool = False,
    ) -> None:
        super().__init__()
        if provider is None:
            if not websocket_url:
                raise ValueError("websocket_url or provider is required")
            provider = StaticSignedUrlProvider(
                websocket_url=websocket_url,
                room_id=room_id,
                room_title=room_title,
                ttwid=ttwid,
                user_agent=user_agent,
                safety_margin_seconds=signature_refresh_margin_seconds,
            )
        self.provider = provider
        self.room_id = room_id
        self.room_title = room_title
        self.websocket_max_size = websocket_max_size
        self.protocol_debug = protocol_debug
        self._websocket: Any = None
        self._connection_info: ConnectionInfo | None = None
        self.status.metadata = {
            "mode": "websocket",
            "room_id": room_id,
            "provider": type(provider).__name__,
        }

    async def connect(self) -> None:
        self.status.state = "connecting"
        self.status.needs_refresh = False
        try:
            connection = await self.provider.get_connection()
            headers = dict(connection.headers)
            headers.setdefault("User-Agent", connection.user_agent)
            if connection.ttwid and "Cookie" not in headers:
                headers["Cookie"] = f"ttwid={connection.ttwid}"

            self._websocket = await websockets.connect(
                connection.websocket_url,
                additional_headers=headers,
                origin="https://live.douyin.com",
                max_size=self.websocket_max_size,
                proxy=None,
            )
            self._connection_info = connection
            self.room_id = connection.room_id
            self.room_title = connection.room_title
            self.status.state = "connected"
            self.status.connected_at = datetime.now(timezone.utc)
            self.status.last_error = None
            self.status.needs_refresh = False
            self.status.metadata = {
                "mode": "websocket",
                "room_id": connection.room_id,
                "room_title": connection.room_title,
                "provider": type(self.provider).__name__,
                "connection": connection.safe_metadata(),
            }
        except ConnectionRefreshRequired:
            self.status.state = "needs_refresh"
            self.status.needs_refresh = True
            raise
        except InvalidStatus as exc:
            if self._status_code(exc) in {401, 403, 404}:
                await self.provider.invalidate(f"WebSocket handshake rejected: {exc}")
                self.status.state = "needs_refresh"
                self.status.needs_refresh = True
            else:
                self.status.state = "error"
            raise
        except Exception:
            self.status.state = "error"
            raise

    async def disconnect(self) -> None:
        websocket = self._websocket
        self._websocket = None
        self._connection_info = None
        if websocket is not None:
            await websocket.close()
        self.status.state = "stopped"

    async def wait_for_connection_refresh(self) -> bool:
        return await self.provider.wait_for_update()

    async def iter_raw_events(self) -> AsyncIterator[dict[str, Any]]:
        if self._websocket is None:
            raise RuntimeError("Douyin WebSocket is not connected")

        try:
            while True:
                frame = await self._websocket.recv()
                if isinstance(frame, str):
                    continue

                try:
                    push_frame, response = decode_push_frame(bytes(frame))
                except Exception as exc:
                    self.mark_decode_error(str(exc))
                    if self.protocol_debug:
                        logging.getLogger(__name__).warning(
                            "ignored malformed Douyin frame: %s", redact_sensitive_text(str(exc))
                        )
                    continue

                if response.need_ack:
                    await self._websocket.send(encode_ack(push_frame.log_id, response.internal_ext))

                for message in response.messages:
                    self.mark_message()
                    if message.method.lower() != "webcastchatmessage":
                        self.mark_unsupported_message()
                        continue
                    try:
                        raw = parse_chat_message(message, room_title=self.room_title)
                    except Exception as exc:
                        self.mark_decode_error(str(exc))
                        if self.protocol_debug:
                            logging.getLogger(__name__).warning(
                                "ignored malformed Douyin comment: %s",
                                redact_sensitive_text(str(exc)),
                            )
                        continue
                    if raw is not None:
                        yield raw
        except ConnectionClosed:
            self.status.state = "disconnected"
            if getattr(self._connection_info, "is_expired", lambda **_: False)():
                await self.provider.invalidate("WebSocket closed after signed connection expiry")
                self.status.needs_refresh = True
            raise

    async def heartbeat(self) -> None:
        await super().heartbeat()
        if self._websocket is not None:
            await self._websocket.send(encode_heartbeat())

    @staticmethod
    def _status_code(error: InvalidStatus) -> int | None:
        response = getattr(error, "response", None)
        return getattr(response, "status_code", None) or getattr(error, "status_code", None)


class DouyinParser:
    """Translate raw Douyin-shaped payloads into the platform-independent Event."""

    def parse(self, raw: dict[str, Any]) -> Event | None:
        if not isinstance(raw, dict):
            return None
        method = str(raw.get("method", "")).lower()
        event_name = str(raw.get("event", "")).lower()
        if method not in {"webcastchatmessage", "comment"} and event_name != EventType.COMMENT:
            return None

        room_payload = raw.get("room") if isinstance(raw.get("room"), dict) else {}
        room_id = str(raw.get("room_id") or room_payload.get("id") or "unknown-room")
        room_title = raw.get("room_title") or room_payload.get("title")
        user_payload = raw.get("user") if isinstance(raw.get("user"), dict) else {}
        user_id = str(user_payload.get("id") or raw.get("user_id") or "unknown-user")
        nickname = user_payload.get("nickname") or raw.get("nickname")
        content = raw.get("content") or raw.get("text") or ""

        timestamp = self._parse_timestamp(raw.get("timestamp"))
        return Event(
            version="1.0",
            platform="douyin",
            event=EventType.COMMENT,
            room=RoomInfo(id=room_id, title=room_title),
            timestamp=timestamp,
            user=UserInfo(id=user_id, nickname=nickname),
            data={"content": str(content)},
            raw=self._safe_raw(raw),
        )

    @staticmethod
    def _safe_raw(raw: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "method",
            "event",
            "msg_id",
            "room_id",
            "room_title",
            "room",
            "user",
            "user_id",
            "nickname",
            "content",
            "text",
            "timestamp",
            "sequence",
        }
        safe = {key: value for key, value in raw.items() if key in allowed}
        if isinstance(safe.get("user"), dict):
            safe["user"] = {
                key: value for key, value in safe["user"].items() if key in {"id", "nickname"}
            }
        if isinstance(safe.get("room"), dict):
            safe["room"] = {
                key: value for key, value in safe["room"].items() if key in {"id", "title"}
            }
        return safe

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        if value is None:
            return datetime.now(timezone.utc)
        if isinstance(value, (int, float)):
            # Accept seconds and millisecond Unix timestamps.
            seconds = value / 1000 if value > 10_000_000_000 else value
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        if isinstance(value, str):
            try:
                normalized = value.replace("Z", "+00:00")
                parsed = datetime.fromisoformat(normalized)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                return datetime.now(timezone.utc)
        return datetime.now(timezone.utc)
