import asyncio

from fastapi.testclient import TestClient

from config.settings import Settings
from main import create_app


def test_health_and_websocket_receive_mock_comment() -> None:
    app = create_app(Settings(mock_interval_seconds=0.01))
    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"
        assert health.json()["collector"]["event_count"] >= 0
        assert "dropped_count" in health.json()["event_bus"]

        readiness = client.get("/health/ready")
        assert readiness.status_code == 200
        assert readiness.json()["collector_state"] in {"connected", "connecting", "starting"}

        with client.websocket_connect("/ws/events") as websocket:
            message = websocket.receive_json()
            assert message["event"] == "comment"
            assert message["data"]["content"].startswith("这是第 ")


def test_test_page_is_available() -> None:
    app = create_app(Settings(mock_interval_seconds=0.01))
    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert "抖音直播事件流" in response.text
