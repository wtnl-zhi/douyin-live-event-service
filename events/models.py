from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class EventType(StrEnum):
    COMMENT = "comment"


class RoomInfo(BaseModel):
    id: str
    title: str | None = None


class UserInfo(BaseModel):
    id: str
    nickname: str | None = None


class Event(BaseModel):
    """The stable event contract used by all downstream features."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    version: str = "1.0"
    platform: str = "douyin"
    event: EventType
    room: RoomInfo
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    user: UserInfo
    data: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] | None = None

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": "9e0f3c1d-8f12-4e1c-9e17-000000000001",
                "version": "1.0",
                "platform": "douyin",
                "event": "comment",
                "room": {"id": "mock-room-001", "title": "Mock 直播间"},
                "timestamp": "2026-01-01T00:00:00Z",
                "user": {"id": "user-001", "nickname": "测试用户"},
                "data": {"content": "你好，直播间！"},
                "raw": {"method": "WebcastChatMessage", "content": "你好，直播间！"},
            }
        }
    }
