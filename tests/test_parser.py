from datetime import timezone

from collectors.douyin import DouyinParser
from events.models import EventType


def test_parser_normalizes_comment_payload() -> None:
    raw = {
        "method": "WebcastChatMessage",
        "room_id": "room-123",
        "room_title": "测试直播间",
        "user": {"id": "user-123", "nickname": "小明"},
        "content": "你好",
        "timestamp": 1_700_000_000_000,
    }

    event = DouyinParser().parse(raw)

    assert event is not None
    assert event.event == EventType.COMMENT
    assert event.room.id == "room-123"
    assert event.user.nickname == "小明"
    assert event.data == {"content": "你好"}
    assert event.raw == raw
    assert event.timestamp.tzinfo == timezone.utc


def test_parser_ignores_unsupported_event() -> None:
    assert DouyinParser().parse({"method": "WebcastGiftMessage"}) is None
