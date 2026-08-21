import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from events.bus import EventBus
from events.models import Event

router = APIRouter()


@router.websocket("/ws/events")
async def events_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    event_bus: EventBus = websocket.app.state.event_service.event_bus
    try:
        async with event_bus.subscribe() as queue:
            while True:
                event: Event = await queue.get()
                await websocket.send_text(event.model_dump_json())
    except (WebSocketDisconnect, RuntimeError, ConnectionError):
        pass
    except asyncio.CancelledError:
        raise
