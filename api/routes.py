from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse

from config.settings import Settings
from services.event_service import EventService

router = APIRouter()
STATIC_DIR = Path(__file__).resolve().parent.parent / "web" / "static"


@router.get("/", include_in_schema=False)
async def test_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@router.get("/health")
async def health(request: Request) -> dict:
    service: EventService = request.app.state.event_service
    settings: Settings = request.app.state.settings
    return {
        "status": "ok",
        "service": settings.app_name,
        "version": settings.app_version,
        "environment": settings.environment,
        "collector": service.collector.status.as_dict(),
        "event_bus": {
            "subscriber_count": service.event_bus.subscriber_count,
            "published_count": service.event_bus.published_count,
            "dropped_count": service.event_bus.dropped_count,
        },
    }


@router.get("/health/ready")
async def readiness(request: Request) -> dict:
    service: EventService = request.app.state.event_service
    is_ready = service.is_running and service.collector.status.state == "connected"
    state = service.collector.status.state
    return {
        "status": "ready" if is_ready else "starting",
        "ready": is_ready,
        "collector_state": state,
        "reason": None if is_ready else service.collector.status.last_error,
    }
