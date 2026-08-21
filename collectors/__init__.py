"""External platform collectors and protocol adapters."""

from .base import Collector, CollectorStatus
from .douyin import DouyinCollector, DouyinParser, DouyinWebSocketCollector
from .mock import MockDouyinCollector
from .signature import (
    ConnectionInfo,
    ConnectionRefreshRequired,
    SignatureProvider,
    StaticSignedUrlProvider,
)

__all__ = [
    "Collector",
    "CollectorStatus",
    "DouyinCollector",
    "DouyinParser",
    "DouyinWebSocketCollector",
    "MockDouyinCollector",
    "ConnectionInfo",
    "ConnectionRefreshRequired",
    "SignatureProvider",
    "StaticSignedUrlProvider",
]
