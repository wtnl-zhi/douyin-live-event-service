import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from collectors.signature import (
    ConnectionRefreshRequired,
    StaticSignedUrlProvider,
    redact_sensitive_text,
)


async def test_static_provider_hides_credentials_and_waits_for_replacement() -> None:
    provider = StaticSignedUrlProvider(
        websocket_url="wss://example.test/live?signature=secret-value",
        room_id="room-1",
        ttwid="cookie-value",
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    )

    connection = await provider.get_connection()
    metadata = connection.safe_metadata()
    assert "secret-value" not in str(metadata)
    assert "cookie-value" not in str(metadata)
    assert metadata["has_ttwid"] is True

    await provider.invalidate("signature=secret-value")
    with pytest.raises(ConnectionRefreshRequired):
        await provider.get_connection()

    waiter = asyncio.create_task(provider.wait_for_update())
    await asyncio.sleep(0)
    provider.replace(websocket_url="wss://example.test/live?signature=new-value")
    assert await asyncio.wait_for(waiter, timeout=0.1) is True
    assert (await provider.get_connection()).websocket_url.endswith("new-value")


async def test_static_provider_rejects_near_expiry_connection() -> None:
    provider = StaticSignedUrlProvider(
        websocket_url="wss://example.test/live",
        room_id="room-1",
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=1),
        safety_margin_seconds=5,
    )

    with pytest.raises(ConnectionRefreshRequired, match="expired"):
        await provider.get_connection()


def test_redaction_removes_websocket_query_values() -> None:
    error = redact_sensitive_text(
        "failed wss://example.test/live?signature=secret&device_id=private-value"
    )
    assert "secret" not in error
    assert "private-value" not in error
    assert "wss://example.test/live" in error
