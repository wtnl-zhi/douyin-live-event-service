from collectors.douyin import DouyinParser, DouyinWebSocketCollector
from collectors.signature import StaticSignedUrlProvider
from collectors.mock import MockDouyinCollector
from config.settings import Settings
from events.bus import EventBus

from .event_service import EventService


def build_event_service(settings: Settings) -> EventService:
    mode = settings.collector_mode.lower()
    if mode == "mock":
        collector = MockDouyinCollector(
            room_id=settings.mock_room_id,
            interval_seconds=settings.mock_interval_seconds,
        )
    elif mode in {"douyin", "websocket"}:
        if not settings.douyin_ws_url:
            raise ValueError(
                "DOUYIN_WS_URL is required when DOUYIN_COLLECTOR_MODE=douyin"
            )
        if not settings.douyin_room_id:
            raise ValueError(
                "DOUYIN_ROOM_ID is required when DOUYIN_COLLECTOR_MODE=douyin"
            )
        provider = StaticSignedUrlProvider(
            websocket_url=settings.douyin_ws_url,
            room_id=settings.douyin_room_id,
            room_title=settings.douyin_room_title,
            ttwid=settings.douyin_ttwid,
            user_agent=settings.douyin_user_agent,
            expires_at=settings.douyin_ws_expires_at,
            safety_margin_seconds=settings.douyin_signature_refresh_margin_seconds,
        )
        collector = DouyinWebSocketCollector(
            provider=provider,
            websocket_max_size=settings.websocket_max_size,
            protocol_debug=settings.protocol_debug,
        )
    else:
        raise ValueError(
            f"Unsupported collector mode: {settings.collector_mode!r}. "
            "Use mock or douyin."
        )
    return EventService(
        collector=collector,
        parser=DouyinParser(),
        event_bus=EventBus(queue_size=settings.event_bus_queue_size),
        reconnect_initial_seconds=settings.effective_reconnect_initial_seconds,
        reconnect_max_seconds=settings.reconnect_max_seconds,
        reconnect_jitter_ratio=settings.reconnect_jitter_ratio,
        reconnect_reset_after_seconds=settings.reconnect_reset_after_seconds,
        heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
    )
