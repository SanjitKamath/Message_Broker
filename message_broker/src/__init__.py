"""Public API for the message_broker package.

Exports are resolved lazily so adapter/core-only imports remain independent from
legacy modules and optional transport dependencies.

The `connect` helper provides a single-line API for broker initialization.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .broker import MessageBroker
    from .schema import DataPacket, Payload, ResponsePacket
    from .core.interfaces import Broker, BrokerCapability

__all__ = ["MessageBroker", "DataPacket", "ResponsePacket", "Payload", "connect", "BrokerCapability"]


async def connect(connection_uri: str, **kwargs: Any) -> "Broker":
    """Initialize a broker from a URI with automatic adapter selection.

    This is a convenience helper that creates a `BrokerContext` from the URI
    and uses `BrokerRegistry.create()` to instantiate the appropriate adapter.
    Commonly used with async context managers for clean connection lifecycle.

    Args:
        connection_uri: Broker URI such as `redis://localhost:6379`,
                        `kafka://localhost:9092`, or `amqp://user:pass@localhost:5672//`.
        **kwargs: Additional options to merge with context defaults. For example,
              `timeout=10000`, `max_retries=5`, `config={...}`,
              `default_dlq_topic="events_dlq"`, or
              `dlq_topics={"orders": "orders_dlq"}`.

    Returns:
        An unconnected Broker instance. Call `await broker.connect()` or use
        an async context manager (`async with broker:`) to establish the connection.

    Raises:
        message_broker.core.exceptions.ConfigurationError: If the URI is invalid
            or no adapter is registered for the scheme.

    Example:
        >>> import asyncio
        >>> import message_broker
        >>> async def main():
        ...     broker = await message_broker.connect("redis://localhost:6379")
        ...     async with broker:
        ...         publisher = broker.get_publisher()
        ...         # publish messages...
        >>> asyncio.run(main())
    """

    from .core.context import BrokerContext
    from .core.registry import BrokerRegistry

    # Ensure available adapters self-register. Importing the adapters package
    # triggers module-level registration code in individual adapter modules.
    try:
        from . import adapters as _adapters  # noqa: F401
    except Exception:
        # If adapters fail to import, let registry.create raise a clear error
        # later. We do not want to crash here during static analysis.
        _adapters = None

    context = BrokerContext(connection_uri, **kwargs)
    BrokerRegistry.discover_plugins()
    return BrokerRegistry.create(context)


def __getattr__(name: str) -> Any:
    """Resolve exported symbols on first access to reduce import coupling."""

    if name == "MessageBroker":
        from .broker import MessageBroker

        return MessageBroker
    if name in {"DataPacket", "Payload", "ResponsePacket"}:
        from .schema import DataPacket, Payload, ResponsePacket

        mapping = {
            "DataPacket": DataPacket,
            "Payload": Payload,
            "ResponsePacket": ResponsePacket,
        }
        return mapping[name]
    if name == "BrokerCapability":
        from .core.interfaces import BrokerCapability

        return BrokerCapability

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
