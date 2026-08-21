import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

from .douyin import DouyinCollector


class MockDouyinCollector(DouyinCollector):
    """Deterministic local source used to exercise the complete first-phase flow."""

    def __init__(self, room_id: str = "mock-room-001", interval_seconds: float = 2.0) -> None:
        super().__init__()
        self.room_id = room_id
        self.interval_seconds = interval_seconds
        self._running = False
        self._sequence = 0
        self.status.metadata = {"mode": "mock", "room_id": room_id}

    async def connect(self) -> None:
        self._running = True
        self.status.state = "connected"
        self.status.connected_at = datetime.now(timezone.utc)
        self.status.last_error = None

    async def disconnect(self) -> None:
        self._running = False
        self.status.state = "stopped"

    async def iter_raw_events(self) -> AsyncIterator[dict[str, Any]]:
        while self._running:
            await asyncio.sleep(self.interval_seconds)
            if not self._running:
                break
            self._sequence += 1
            self.mark_event()
            yield {
                "method": "WebcastChatMessage",
                "room_id": self.room_id,
                "room_title": "Mock 抖音直播间",
                "user": {
                    "id": f"mock-user-{self._sequence:04d}",
                    "nickname": f"测试用户{self._sequence}",
                },
                "content": f"这是第 {self._sequence} 条 mock comment",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "sequence": self._sequence,
            }
