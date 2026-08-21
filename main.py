from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI

from api.routes import router as http_router
from api.websocket import router as websocket_router
from config.settings import Settings, get_settings
from services.dependencies import build_event_service
from services.event_service import EventService


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    logging.basicConfig(level=settings.log_level)
    event_service = build_event_service(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.event_service = event_service
        app.state.settings = settings
        await event_service.start()
        try:
            yield
        finally:
            await event_service.stop()

    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description="Protocol-independent Douyin live event collection foundation.",
        lifespan=lifespan,
    )
    app.include_router(http_router)
    app.include_router(websocket_router)
    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run("main:app", host=settings.host, port=settings.port, reload=False)
