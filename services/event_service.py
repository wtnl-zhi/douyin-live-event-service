import asyncio
import logging
import random
from contextlib import suppress
from datetime import datetime, timezone

from collectors.base import Collector
from collectors.douyin import DouyinParser
from collectors.signature import ConnectionRefreshRequired, redact_sensitive_text
from events.bus import EventBus

logger = logging.getLogger(__name__)


class EventService:
    """Coordinate collection, parsing, publication and transport lifecycle."""

    def __init__(
        self,
        collector: Collector,
        parser: DouyinParser,
        event_bus: EventBus,
        reconnect_initial_seconds: float = 3.0,
        reconnect_max_seconds: float = 60.0,
        reconnect_jitter_ratio: float = 0.2,
        reconnect_reset_after_seconds: float = 10.0,
        heartbeat_interval_seconds: float = 15.0,
        # Compatibility with the first skeleton's constructor.
        reconnect_interval_seconds: float | None = None,
    ) -> None:
        self.collector = collector
        self.parser = parser
        self.event_bus = event_bus
        if reconnect_interval_seconds is not None:
            reconnect_initial_seconds = reconnect_interval_seconds
        self.reconnect_initial_seconds = max(0.0, reconnect_initial_seconds)
        self.reconnect_max_seconds = max(self.reconnect_initial_seconds, reconnect_max_seconds)
        self.reconnect_jitter_ratio = max(0.0, min(1.0, reconnect_jitter_ratio))
        self.reconnect_reset_after_seconds = max(0.0, reconnect_reset_after_seconds)
        self.heartbeat_interval_seconds = max(0.01, heartbeat_interval_seconds)
        self._task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="collector-event-loop")
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name="collector-heartbeat-loop"
        )

    async def stop(self) -> None:
        self._running = False
        tasks = (self._task, self._heartbeat_task)
        for task in tasks:
            if task:
                task.cancel()
        for task in tasks:
            if task:
                with suppress(asyncio.CancelledError):
                    await task
        self._task = None
        self._heartbeat_task = None
        with suppress(Exception):
            await self.collector.disconnect()

    async def _run(self) -> None:
        backoff = self.reconnect_initial_seconds
        while self._running:
            connected_at: datetime | None = None
            try:
                self.collector.status.state = "connecting"
                self.collector.status.needs_refresh = False
                self.collector.mark_connect_attempt()
                await self.collector.connect()
                connected_at = datetime.now(timezone.utc)

                async for raw in self.collector.iter_raw_events():
                    if not self._running:
                        break
                    try:
                        event = self.parser.parse(raw)
                    except Exception as exc:
                        self.collector.mark_parse_error(str(exc))
                        logger.warning(
                            "ignored unparseable collector payload: %s",
                            redact_sensitive_text(str(exc)),
                        )
                        continue
                    if event is None:
                        self.collector.mark_unsupported_message()
                        continue
                    await self.event_bus.publish(event)
                    self.collector.mark_normalized_event()

                if self._running:
                    raise RuntimeError("collector event stream ended")
            except asyncio.CancelledError:
                raise
            except ConnectionRefreshRequired as exc:
                self.collector.status.state = "needs_refresh"
                self.collector.status.needs_refresh = True
                self.collector.status.last_error = redact_sensitive_text(str(exc))
                logger.warning(
                    "collector needs a fresh signed connection: %s",
                    self.collector.status.last_error,
                )
                if self._running:
                    await self.collector.wait_for_connection_refresh()
                    backoff = self.reconnect_initial_seconds
            except Exception as exc:
                self.collector.status.state = "error"
                self.collector.status.last_error = redact_sensitive_text(str(exc))
                logger.warning(
                    "collector loop failed; reconnecting: %s", self.collector.status.last_error
                )
                if not self._running:
                    break

                with suppress(Exception):
                    await self.collector.disconnect()

                if connected_at is not None:
                    elapsed = (datetime.now(timezone.utc) - connected_at).total_seconds()
                    if elapsed >= self.reconnect_reset_after_seconds:
                        backoff = self.reconnect_initial_seconds

                self.collector.status.reconnect_count += 1
                delay = self._backoff_delay(backoff)
                self.collector.status.state = "reconnecting"
                await asyncio.sleep(delay)
                backoff = min(
                    self.reconnect_max_seconds,
                    max(self.reconnect_initial_seconds, backoff * 2 or self.reconnect_max_seconds),
                )

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.heartbeat_interval_seconds)
            if not self._running:
                break
            if self.collector.status.state != "connected":
                continue
            try:
                await self.collector.heartbeat()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.collector.status.state = "error"
                self.collector.status.last_error = redact_sensitive_text(str(exc))
                logger.warning("collector heartbeat failed: %s", self.collector.status.last_error)
                with suppress(Exception):
                    await self.collector.disconnect()

    def _backoff_delay(self, base: float) -> float:
        if base <= 0:
            return 0.0
        jitter = random.uniform(0.0, base * self.reconnect_jitter_ratio)
        return min(self.reconnect_max_seconds, base + jitter)
