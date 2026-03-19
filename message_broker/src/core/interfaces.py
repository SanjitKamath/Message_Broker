"""Abstract contracts for the broker-agnostic messaging layer.

The goal of these interfaces is to make transport backends replaceable without
changing application-level code. Adapters can vary in implementation details,
but they must comply with these contracts.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import Flag, auto
from types import TracebackType
from typing import Any, TypeAlias
from uuid import uuid4

JsonValue: TypeAlias = dict[str, Any] | list[Any] | str | int | float | bool | None

MessageHandler = Callable[["Message"], Awaitable[None]]


class BrokerCapability(Flag):
    """Enum of features that a broker adapter may or may not support.

    Use the capabilities property on a Broker instance to check if a feature
    is available before attempting to use it. This prevents silent failures and
    enables graceful degradation when advanced features are not supported.

    Examples:
        if BrokerCapability.DELAYED_DELIVERY in broker.capabilities:
            await publisher.publish(topic, msg, deliver_at=time.time() + 3600)
    """

    DELAYED_DELIVERY = auto()
    """Broker supports scheduled message delivery via deliver_at parameter."""

    BATCH_PUBLISH = auto()
    """Broker supports publishing multiple messages in a single atomic operation."""

    RPC_REPLIES = auto()
    """Broker supports request-reply pattern with correlation-based matching."""


@dataclass(slots=True)
class Message:
    """Represents a transport-neutral message envelope.

    A single envelope keeps application payloads independent from broker
    internals while still carrying operational metadata. Correlation IDs are
    always injected so logs and distributed traces can be connected reliably.

    Attributes:
        payload: User-defined data to send through the broker.
        headers: Broker-level string headers for interoperability.
        metadata: Internal metadata for routing, tracing, and diagnostics.
        correlation_id: Stable identifier used across producer/consumer hops.
    """

    payload: JsonValue
    headers: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, JsonValue] = field(default_factory=dict)
    correlation_id: str = field(default_factory=lambda: uuid4().hex)

    def __post_init__(self) -> None:
        """Normalize header and metadata containers.

        Defensive copies protect callers from accidental shared-state mutation,
        and guaranteed correlation propagation ensures observability does not
        depend on each adapter implementing it correctly.
        """

        if not self.correlation_id.strip():
            self.correlation_id = uuid4().hex

        self.headers = dict(self.headers)
        self.metadata = dict(self.metadata)

        incoming_header_id = self.headers.get("correlation_id")
        if incoming_header_id and incoming_header_id.strip():
            self.correlation_id = incoming_header_id
        else:
            self.headers["correlation_id"] = self.correlation_id

        self.metadata.setdefault("correlation_id", self.correlation_id)


class Serializer(ABC):
    """Abstract contract for message serialization.

    Implementers can provide custom serialization strategies (MsgPack, Protobuf, etc.)
    by inheriting from this class. All adapters use the serializer from BrokerContext.
    """

    @abstractmethod
    def serialize(self, data: Mapping[str, JsonValue]) -> str | bytes:
        """Serialize a message dictionary into a byte string or text string.

        Args:
            data: Dictionary representation of Message (typically asdict(Message)).
                  Keys include: payload, headers, metadata, correlation_id.

        Returns:
            Serialized message as str or bytes. Adapters will handle encoding as needed.

        Raises:
            Exceptions specific to the serialization format (e.g., json.JSONDecodeError).
        """

    @abstractmethod
    def deserialize(self, payload: str | bytes) -> dict[str, JsonValue]:
        """Deserialize a payload back into a message dictionary.

        Args:
            payload: Serialized message (str or bytes).

        Returns:
            Dictionary with keys: payload, headers, metadata, correlation_id.

        Raises:
            Exceptions specific to the serialization format if payload is malformed.
        """


class Middleware(ABC):
    """Abstract contract for message lifecycle hooks.

    Middleware instances are called during publish and consume operations to enable
    logging, tracing, validation, or other cross-cutting concerns without modifying
    adapter code.
    """

    @abstractmethod
    async def before_publish(self, topic: str, message: Message) -> Message:
        """Hook called before a message is serialized and sent.

        Args:
            topic: Destination topic for the message.
            message: The Message being published (may be modified).

        Returns:
            The Message to publish (may be modified by this middleware).

        Raises:
            Any exception raised here will stop the publish and propagate to the caller.
        """

    @abstractmethod
    async def after_consume(self, topic: str, message: Message) -> Message:
        """Hook called after a message is deserialized but before the handler.

        Args:
            topic: Source topic of the message.
            message: The Message that was consumed (may be modified).

        Returns:
            The Message to pass to the user's handler (may be modified).

        Raises:
            Any exception raised here will be logged but not crash the consume loop.
        """


class Publisher(ABC):
    """Defines the publishing contract for all adapters.

    Example:
        message = Message(payload={"event": "created"})
        await publisher.publish("events", message)
    """

    @abstractmethod
    async def publish(
        self,
        topic: str,
        message: Message,
        /,
        *,
        timeout_ms: int | None = None,
        deliver_at: float | None = None,
    ) -> None:
        """Publish a message to the given topic.

        Args:
            topic: Logical destination used by the underlying broker.
            message: Transport-neutral envelope to publish.
            timeout_ms: Optional request timeout in milliseconds.
            deliver_at: Optional POSIX timestamp (seconds since epoch) for scheduled delivery.
                         If provided, the message will be delivered at or after that time.
                         Not all adapters support this (e.g., Kafka raises FeatureNotSupportedError).

        Raises:
            message_broker.core.exceptions.PublishFailedError: If send fails.
            message_broker.core.exceptions.ConnectionLostError: If transport is
                unavailable during publish.
            message_broker.core.exceptions.FeatureNotSupportedError: If the adapter
                does not support scheduled delivery.

        Example:
            await publisher.publish("jobs", Message(payload={"id": 1}))
        """


@dataclass(slots=True)
class ScheduledEnvelope:
    """Transport-neutral envelope used for scheduled/delayed delivery.

    This object wraps a `Message` with the necessary scheduling and routing
    information in a transport-agnostic way so adapters can persist and route
    scheduled messages without mutating the original message metadata.
    """

    message: Message
    target: str
    deliver_at: float


class EnforcingPublisher(Publisher):
    """Publisher wrapper that enforces broker capabilities centrally.

    This wrapper checks requested features (e.g., `deliver_at`) against the
    owning broker's `capabilities` before delegating to the adapter-specific
    publisher implementation. It preserves the existing `publish` API so
    client code need not change.
    """

    def __init__(self, broker: "Broker", delegate: Publisher) -> None:
        self._broker = broker
        self._delegate = delegate

    async def publish(self, *args, **kwargs) -> None:
        """Flexible publish wrapper that accepts positional or keyword args.

        The core `Publisher.publish` has positional-only parameters for
        `topic` and `message`. To remain friendly to existing caller styles
        (some examples pass keywords), this wrapper extracts the values from
        either `args` or `kwargs` and delegates to the underlying publisher.
        """
        from .exceptions import FeatureNotSupportedError  # local import to avoid cycles

        # Extract topic and message from positional or keyword args
        if len(args) >= 2:
            topic = args[0]
            message = args[1]
        else:
            topic = kwargs.get("topic")
            message = kwargs.get("message")

        timeout_ms = kwargs.get("timeout_ms")
        deliver_at = kwargs.get("deliver_at")

        # Validate presence
        if topic is None or message is None:
            raise TypeError("publish() missing required 'topic' and/or 'message' arguments")

        # Enforce delayed delivery support centrally
        if deliver_at is not None:
            if BrokerCapability.DELAYED_DELIVERY not in self._broker.capabilities:
                raise FeatureNotSupportedError(
                    "Requested scheduled delivery (deliver_at) is not supported by this broker.",
                    broker=getattr(self._broker, "__name__", None) or None,
                    operation="publish",
                )

            # Wrap into ScheduledEnvelope and delegate (deliver_at captured in envelope)
            # Apply before_publish middleware from delegate's context if available
            ctx = getattr(self._delegate, "_context", None)
            if ctx is not None and getattr(ctx, "middlewares", None):
                for middleware in ctx.middlewares:
                    message = await middleware.before_publish(topic, message)

            envelope = ScheduledEnvelope(message=message, target=topic, deliver_at=deliver_at)
            await self._delegate.publish(topic, envelope, timeout_ms=timeout_ms, deliver_at=None)
            return

        # No special features requested — delegate directly
        # Apply before_publish middleware from delegate's context if available
        ctx = getattr(self._delegate, "_context", None)
        if ctx is not None and getattr(ctx, "middlewares", None):
            for middleware in ctx.middlewares:
                message = await middleware.before_publish(topic, message)

        await self._delegate.publish(topic, message, timeout_ms=timeout_ms, deliver_at=None)


class Subscriber(ABC):
    """Defines the subscription contract for all adapters.

    Example:
        async def handler(message: Message) -> None:
            print(message.payload)

        await subscriber.subscribe("events", handler)
    """

    @abstractmethod
    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        /,
        *,
        auto_ack: bool = True,
    ) -> None:
        """Start consuming messages for a topic.

        Args:
            topic: Logical source to consume from.
            handler: Callback invoked for each received message.
            auto_ack: Whether the adapter should acknowledge automatically.

        Raises:
            message_broker.core.exceptions.ConnectionLostError: If the
                subscription channel cannot be established.

        Example:
            await subscriber.subscribe("jobs", handler)
        """


class Broker(ABC):
    """Coordinates connection lifecycle and access to pub/sub primitives."""

    @property
    @abstractmethod
    def capabilities(self) -> frozenset[BrokerCapability]:
        """Return the set of features supported by this broker.

        Returns:
            Immutable set of BrokerCapability flags that this adapter supports.
            Empty set if no advanced features are available.

        Examples:
            broker = await connect("redis://localhost")
            if BrokerCapability.DELAYED_DELIVERY in broker.capabilities:
                print("Scheduled delivery is available")
        """

    @abstractmethod
    async def connect(self) -> None:
        """Establish all required broker connections and channels."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully terminate connections and release transport resources."""

    @abstractmethod
    def get_publisher(self) -> Publisher:
        """Return a publisher bound to this broker session."""

    @abstractmethod
    def get_subscriber(self) -> Subscriber:
        """Return a subscriber bound to this broker session."""

    async def __aenter__(self) -> "Broker":
        """Allow broker usage through asynchronous context managers.

        Returns:
            The connected broker instance.
        """

        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Ensure graceful shutdown at the end of a context-manager scope."""

        await self.disconnect()
