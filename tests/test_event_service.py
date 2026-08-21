import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

from collectors.base import Collector
from collectors.douyin import DouyinParser
from events.bus import EventBus
from services.event_service import EventService


class FlakyCollector(Collector):
    def __init__(self, failures: int = 2) -> None:
        super().__init__()
        self.failures_left = failures
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.heartbeat_calls = 0
        self.connected = False

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.failures_left:
            self.failures_left -= 1
            raise ConnectionError("temporary transport failure")
        self.connected = True
        self.status.state = "connected"
        self.status.connected_at = datetime.now(timezone.utc)

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.connected = False
        self.status.state = "stopped"

    async def heartbeat(self) -> None:
        self.heartbeat_calls += 1
        await super().heartbeat()

    async def iter_raw_events(self) -> AsyncIterator[dict[str, Any]]:
        if not self.connected:
            raise RuntimeError("not connected")
        yield {
            "method": "WebcastChatMessage",
            "room_id": "room-1",
            "user": {"id": "user-1", "nickname": "测试用户"},
            "content": "hello",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        while self.connected:
            await asyncio.sleep(0.01)


async def test_event_service_reconnects_and_stops_cleanly() -> None:
    collector = FlakyCollector()
    service = EventService(
        collector=collector,
        parser=DouyinParser(),
        event_bus=EventBus(),
        reconnect_initial_seconds=0.001,
        reconnect_max_seconds=0.005,
        reconnect_jitter_ratio=0,
        reconnect_reset_after_seconds=999,
        heartbeat_interval_seconds=0.005,
    )

    await service.start()
    async with service.event_bus.subscribe() as queue:
        event = await asyncio.wait_for(queue.get(), timeout=0.5)
        assert event.data["content"] == "hello"
        await asyncio.sleep(0.02)

    assert collector.connect_calls >= 3
    assert collector.status.connection_attempts == collector.connect_calls
    assert collector.status.event_count == 1
    assert collector.heartbeat_calls >= 1

    await service.stop()
    assert service.is_running is False
    assert collector.connected is False
    assert collector.disconnect_calls >= 1


async def test_event_service_start_is_idempotent() -> None:
    collector = FlakyCollector(failures=0)
    service = EventService(
        collector=collector,
        parser=DouyinParser(),
        event_bus=EventBus(),
        reconnect_initial_seconds=0,
        reconnect_max_seconds=0.01,
        reconnect_jitter_ratio=0,
        heartbeat_interval_seconds=0.01,
    )

    await service.start()
    first_task = service._task
    await service.start()
    assert service._task is first_task
    await service.stop()
