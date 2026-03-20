"""Convenience shim for local workspace imports.

When running demo scripts from the repository root, Python may resolve this
outer directory as the ``message_broker`` package. Re-exporting from the real
inner package keeps imports stable:

    from message_broker import connect, Message
"""

from __future__ import annotations

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

__all__ = [
    "MessageBroker",
    "DataPacket",
    "ResponsePacket",
    "Payload",
    "Message",
    "Middleware",
    "BrokerCapability",
    "connect",
]
