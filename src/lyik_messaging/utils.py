"""Utility helpers for :mod:`lyik_messaging`."""

from __future__ import annotations

import contextvars
from typing import TYPE_CHECKING, cast

from faststream.message import StreamMessage

from .exceptions import BrokerNotInitializedError
from .models import MessageInfo

if TYPE_CHECKING:
    from collections.abc import Iterable

    from faststream.rabbit import RabbitBroker
    from faststream.redis import RedisBroker

    from .broker import MessageBroker

_current_broker: contextvars.ContextVar[object | None] = contextvars.ContextVar(
    "lyik_messaging_current_broker",
    default=None,
)


def get_broker() -> "MessageBroker":
    """Return the broker bound to the current async context.

    Raises:
        BrokerNotInitializedError: If no broker has been connected in this context.
    """

    broker = _current_broker.get()
    if broker is None:
        raise BrokerNotInitializedError(
            "No active MessageBroker is available in this context. Call connect() first."
        )
    return cast("MessageBroker", broker)


def set_current_broker(broker: object | None) -> None:
    """Set the broker for the current async context."""

    _current_broker.set(broker)


def resolve_broker_scheme(uri: str) -> str:
    """Return the normalized transport scheme for a broker URI."""

    from urllib.parse import urlparse
    
    from .exceptions import ConfigurationError

    scheme = urlparse(uri).scheme.strip().lower()
    if not scheme:
        raise ConfigurationError("Broker URI must include a scheme such as amqp:// or redis://.")
    if "+" in scheme:
        scheme = scheme.split("+", 1)[0]
    return scheme


def build_message_info(message: StreamMessage[object]) -> MessageInfo:
    """Convert a FastStream message into the public `MessageInfo` model."""

    headers = {str(key): value for key, value in message.headers.items()}
    raw: dict[str, object] = {
        "headers": headers,
        "path": dict(message.path),
        "message_id": message.message_id,
        "content_type": message.content_type,
        "source_type": message.source_type.value,
    }

    sender = _extract_sender(headers)
    reply_to = message.reply_to or headers.get("reply_to") or headers.get("x-reply-to")
    correlation_id = message.correlation_id or headers.get("correlation_id")

    return MessageInfo(
        correlation_id=correlation_id if isinstance(correlation_id, str) else None,
        reply_to=reply_to if isinstance(reply_to, str) else None,
        sender=sender,
        raw=raw,
    )


def _extract_sender(headers: dict[str, object]) -> str | None:
    for header_name in ("sender", "x-sender", "app_id", "x-app-id", "producer"):
        value = headers.get(header_name)
        if isinstance(value, str) and value.strip():
            return value
    return None