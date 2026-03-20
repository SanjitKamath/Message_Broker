"""
message_broker

A lightweight, async-friendly message broker abstraction supporting
multiple backends (e.g., Redis, Kafka).

This package provides:

- `connect` → Create and configure a broker instance
- `MessageBroker` → Core broker interface
- `Message` → High-level message abstraction
- `Payload` → Message payload type
- `DataPacket` / `ResponsePacket` → Transport-level structures
- `Middleware` → Interception hooks for message processing
- `BrokerCapability` → Feature flags supported by a broker

Example:
    from message_broker import connect, Message

    broker = connect(...)
    await broker.publish(Message(...))
"""

from __future__ import annotations

from typing import Final

from .message_broker import (
    BrokerCapability,
    DataPacket,
    Message,
    MessageBroker,
    Middleware,
    Payload,
    ResponsePacket,
    connect,
)

# Public API of the package
__all__: Final[list[str]] = [
    "MessageBroker",
    "DataPacket",
    "ResponsePacket",
    "Payload",
    "Message",
    "Middleware",
    "BrokerCapability",
    "connect",
]