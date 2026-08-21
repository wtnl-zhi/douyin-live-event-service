import asyncio

from events.bus import EventBus
from events.models import Event, EventType, RoomInfo, UserInfo


def make_event(content: str) -> Event:
    return Event(
        event=EventType.COMMENT,
        room=RoomInfo(id="room"),
        user=UserInfo(id="user"),
        data={"content": content},
    )


async def test_event_bus_fans_out_to_all_subscribers() -> None:
    bus = EventBus(queue_size=2)
    async with bus.subscribe() as first, bus.subscribe() as second:
        event = make_event("hello")
        await bus.publish(event)
        assert await asyncio.wait_for(first.get(), timeout=0.1) == event
        assert await asyncio.wait_for(second.get(), timeout=0.1) == event


async def test_event_bus_drops_oldest_when_subscriber_is_slow() -> None:
    bus = EventBus(queue_size=2)
    async with bus.subscribe() as queue:
        await bus.publish(make_event("one"))
        await bus.publish(make_event("two"))
        await bus.publish(make_event("three"))
        assert (await queue.get()).data["content"] == "two"
        assert (await queue.get()).data["content"] == "three"
        assert bus.dropped_count == 1
