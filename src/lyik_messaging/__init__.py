"""
Public API for `lyik_messaging`.

This is the entry point for users of the library, and it re-exports all the necessary 
components for working with the messaging system. It also provides a convenient `connect`
function to create and connect a `MessageBroker` instance in one step, with optional
auto-start of the broker's run loop.

This module is designed to be the main interface for users, abstracting away the internal
details and providing a clean and simple API for interacting with the messaging system.
"""

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
    """
    Create a ready-to-use `MessageBroker` instance.

    By default this helper connects the broker and returns it. If `auto_start`
    is enabled, the broker run loop is started in a background task.

    Args:
        `uri`: The URI of the message broker to connect to.
        `aes_key`: Optional AES key for encrypting messages.
        `auto_start`: Whether to automatically start the broker's run loop after connecting.
        `**kwargs`: Additional keyword arguments to pass to the `MessageBroker` constructor.

    Returns:
        An instance of `MessageBroker` that is connected and optionally started.
    
    Example:
        ```
        from lyik_messaging import connect
        broker = await connect("mqtt://localhost:1883", auto_start=True)
        ```
    
    Raises:
        LyikMessagingError: If there is an error during connection or initialization of the broker.
    """

    # Create the MessageBroker instance with the provided parameters
    broker = MessageBroker(
        uri,
        aes_key=aes_key,
        **kwargs,
    )

    # Connect the broker to the specified URI and handle any connection errors
    await broker.connect()

    # If auto_start is enabled, start the broker's run loop in a background task
    if auto_start:
        asyncio.create_task(broker.start())
        return broker

    # Return the connected broker instance without starting the run loop
    return broker