import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from .models import Event


class EventBus:
    """Small in-process fan-out bus for the first phase."""

    def __init__(self, queue_size: int = 100) -> None:
        self.queue_size = queue_size
        self._subscribers: set[asyncio.Queue[Event]] = set()
        self._lock = asyncio.Lock()
        self.published_count = 0
        self.dropped_count = 0

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    async def publish(self, event: Event) -> None:
        async with self._lock:
            subscribers = tuple(self._subscribers)

        for queue in subscribers:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # A slow WebSocket must not stop collection for every other client.
                # The oldest queued event is dropped before the newest one is kept.
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                self.dropped_count += 1
                try:
                    queue.put_nowait(event)
                except asyncio.QueueFull:
                    self.dropped_count += 1
        self.published_count += 1

    @asynccontextmanager
    async def subscribe(self) -> AsyncIterator[asyncio.Queue[Event]]:
        queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=self.queue_size)
        async with self._lock:
            self._subscribers.add(queue)
        try:
            yield queue
        finally:
            async with self._lock:
                self._subscribers.discard(queue)
