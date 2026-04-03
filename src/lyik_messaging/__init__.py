"""Public API for :mod:`lyik_messaging`."""

from __future__ import annotations

import asyncio

from .broker import MessageBroker
from .exceptions import (
    BrokerNotInitializedError,
    ConfigurationError,
    ConnectionLostError,
    EncryptionError,
    FeatureNotSupportedError,
    LyikMessagingError,
    MessageBrokerError,
    PublishFailedError,
)
from .models import DataPacket, Message, MessageInfo, Middleware, Payload, ResponsePacket
from .utils import get_broker

__all__ = [
    "MessageBroker",
    "DataPacket",
    "ResponsePacket",
    "Payload",
    "Message",
    "Middleware",
    "MessageInfo",
    "LyikMessagingError",
    "MessageBrokerError",
    "ConfigurationError",
    "ConnectionLostError",
    "PublishFailedError",
    "FeatureNotSupportedError",
    "BrokerNotInitializedError",
    "EncryptionError",
    "get_broker",
    "connect",
]


async def connect(
    uri: str,
    *,
    aes_key: str | None = None,
    auto_start: bool = False,
    **kwargs: object,
) -> MessageBroker:
    """Create a ready-to-use `MessageBroker` instance.

    By default this helper connects the broker and returns it. If `auto_start`
    is enabled, the broker run loop is started in a background task.
    """

    broker = MessageBroker(
        uri,
        aes_key=aes_key,
        **kwargs,
    )

    await broker.connect()

    if auto_start:
        asyncio.create_task(broker.start())
        return broker

    return broker