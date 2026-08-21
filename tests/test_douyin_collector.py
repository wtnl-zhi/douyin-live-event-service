from datetime import datetime, timedelta, timezone

from collectors import DouyinWebSocketCollector
from collectors.signature import StaticSignedUrlProvider
from collectors.douyin_protocol import encode_heartbeat


class FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[bytes] = []
        self.closed = False

    async def send(self, data: bytes) -> None:
        self.sent.append(data)

    async def close(self) -> None:
        self.closed = True


async def test_collector_uses_provider_without_exposing_signed_data(monkeypatch) -> None:
    provider = StaticSignedUrlProvider(
        websocket_url="wss://example.test/live?signature=do-not-expose",
        room_id="room-1",
        room_title="测试直播间",
        ttwid="do-not-expose-cookie",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )
    websocket = FakeWebSocket()
    captured: dict[str, object] = {}

    async def fake_connect(url: str, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return websocket

    monkeypatch.setattr("collectors.douyin.websockets.connect", fake_connect)

    collector = DouyinWebSocketCollector(provider=provider)
    await collector.connect()
    await collector.heartbeat()

    assert captured["url"] == "wss://example.test/live?signature=do-not-expose"
    assert websocket.sent == [encode_heartbeat()]
    assert collector.status.metadata["room_id"] == "room-1"
    assert "do-not-expose" not in str(collector.status.as_dict())

    await collector.disconnect()
    assert websocket.closed is True
    assert collector.status.state == "stopped"
