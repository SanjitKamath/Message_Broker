"""Abstract contracts for the broker-agnostic messaging layer.

The goal of these interfaces is to make transport backends replaceable without
changing application-level code. Adapters can vary in implementation details,
but they must comply with these contracts.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, TypeAlias
from uuid import uuid4

# Type alias for JSON-like values used in metadata and serialized envelopes.
JsonValue: TypeAlias = dict[str, Any] | list[Any] | str | int | float | bool | None

# Type alias for broker header mappings.
MessageHeaders: TypeAlias = dict[str, str]

# Type alias for the message handler callback used in Subscriber.subscribe.
MessageHandler = Callable[["Message"], Awaitable[None]]


@dataclass(slots=True)
class Message:
    """
    Represents a transport-neutral message envelope.

    This will be used for all messages sent through the broker, whether published directly 
    by the user or received from the broker.

    A single envelope keeps application payloads independent from broker
    internals while still carrying operational metadata. Correlation IDs are
    always injected so logs and distributed traces can be connected reliably.

    `Attributes`:
        - `payload`: Any
            Application-level content of the message.
        - `headers`: MessageHeaders = field(default_factory=dict)
            Transport-friendly string metadata for routing and broker features.
        - `metadata`: dict[str, JsonValue] = field(default_factory=dict)
            Internal-use mapping for adapters and middleware (not sent to broker).
        - `correlation_id`: str = field(default_factory=lambda: uuid4().hex)
            Unique identifier for tracing and correlation, always injected if not provided.


    `Header guidelines`:
        The `headers` mapping is transport-friendly string metadata. Keep values
        short and ASCII-safe for broad broker compatibility.

        Common header keys (recommended, not required):
        - `correlation_id`: Correlates request/response and tracing hops.
        - `content_type`: Payload media type, usually `application/json`.
        - `reply_to`: Queue/topic where responses should be sent.
        - `x-request-id`: Request identifier propagated from ingress.
        - `x-trace-id`: Distributed tracing identifier.
        - `x-tenant-id`: Tenant identifier in multi-tenant systems.
    
    `Metadata guidelines`:
        The `metadata` mapping is for internal use by adapters and middleware. It can
        contain any JSON-serializable data that helps with routing, retries, diagnostics, 
        or observability.

    `Example usage`:
    ```
        Message(
            payload={"event": "user.created", "user_id": 42},
            headers={
                "content_type": "application/json",
                "x-request-id": "req_8f7c2",
            },
            metadata={"source_topic": "users"},
        )
    ```

    """

    payload: Any
    headers: MessageHeaders = field(default_factory=dict)
    metadata: dict[str, JsonValue] = field(default_factory=dict)
    correlation_id: str = field(default_factory=lambda: uuid4().hex)

    def __post_init__(self) -> None:
        """
        Ensures the message has a valid correlation ID and that headers/metadata are 
        properly initialized.

        Defensive copies protect callers from accidental shared-state mutation,
        and guaranteed correlation propagation ensures observability does not
        depend on each adapter implementing it correctly.
        """

        # Ensure correlation_id is always set and non-empty (generate if missing or blank)
        if not self.correlation_id.strip():
            self.correlation_id = uuid4().hex

        # Defensive copy of headers and metadata to prevent shared mutable state issues.
        self.headers = dict(self.headers)
        self.metadata = dict(self.metadata)

        # Ensure correlation_id is present in headers for broker interoperability.
        incoming_header_id = self.headers.get("correlation_id")

        # If the incoming header has a non-empty correlation_id, use it to maintain 
        # trace continuity.
        if incoming_header_id and incoming_header_id.strip():
            self.correlation_id = incoming_header_id
        else:
            self.headers["correlation_id"] = self.correlation_id

        # Set correlation_id in metadata for internal use by adapters that prefer it there.
        self.metadata.setdefault("correlation_id", self.correlation_id)


class Serializer(ABC):
    """
    Abstract contract for message serialization.

    Helps decouple message formatting from transport concerns. This means that adapters 
    can focus on broker-specific logic while relying on a consistent serialization interface.

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

    async def before_publish_response(self, topic: str, message: Message) -> Message:
        """Hook called before publishing response messages.

        Default behavior delegates to `before_publish` for full backward
        compatibility. Override this method when response packets need
        different middleware behavior than request packets.
        """

        return await self.before_publish(topic, message)

    async def after_consume_response(self, topic: str, message: Message) -> Message:
        """Hook called after consuming response messages.

        Default behavior delegates to `after_consume` for full backward
        compatibility. Override this method when response packets need
        different middleware behavior than request packets.
        """

        return await self.after_consume(topic, message)


def resolve_packet_kind(message: Message) -> str:
    """Classify message as request or response for middleware routing.

    Resolution order:
    1. `message.metadata["packet_type"]` when set to `request` or `response`.
    2. Payload-shape inference for response envelopes.
    3. Fallback to `request`.
    """

    packet_type = message.metadata.get("packet_type")
    if isinstance(packet_type, str):
        normalized = packet_type.strip().lower()
        if normalized in {"request", "response"}:
            return normalized

    payload = message.payload
    if isinstance(payload, Mapping):
        if {
            "correlation_id",
            "in_response_to",
            "status",
        }.issubset(payload.keys()):
            return "response"

    return "request"


class Publisher(ABC):
    """
    Defines the publishing contract for all adapters.

    This means that any adapter must implement the `publish` method with the specified signature and behavior.
    Only then can it be used interchangeably in application code that relies on the Publisher interface.

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
        """
        Abstract method to publish a message to the given topic. This ensures that all adapters provide a consistent 
        interface for sending messages, regardless of the underlying transport.

        Args:
            topic: Logical destination used by the underlying broker.
            message: Transport-neutral envelope to publish.
            timeout_ms: Optional request timeout in milliseconds.
            deliver_at: Optional POSIX timestamp (seconds since epoch) for scheduled delivery.
                         If provided, the message will be delivered at or after that time.

        Raises:
            message_broker.core.exceptions.PublishFailedError: If send fails.
            message_broker.core.exceptions.ConnectionLostError: If transport is
                unavailable during publish.

        Example:
        ```
            await publisher.publish("jobs", Message(payload={"id": 1}))
        ```
        """


@dataclass(slots=True)
class ScheduledEnvelope:
    """
    Transport-neutral envelope used for scheduled/delayed delivery.

    This object wraps a `Message` with the necessary scheduling and routing
    information in a transport-agnostic way so adapters can persist and route
    scheduled messages without mutating the original message metadata.

    It is passed on to the broker when `deliver_at` is specified in the publish call, allowing adapters
    to handle delayed delivery according to their capabilities while keeping the core publish contract clean.
    """

    message: Message
    target: str
    deliver_at: float


class EnforcingPublisher(Publisher):
    """
    Publisher wrapper that applies middleware and handles scheduling envelopes.

    Delayed delivery is part of the core publish contract. This wrapper only
    normalizes call styles, applies `before_publish` middleware, and wraps
    delayed messages into ScheduledEnvelope for adapter consumption.
    """

    def __init__(self, delegate: Publisher) -> None:
        self._delegate = delegate

    async def _apply_before_publish_middlewares(self, topic: str, message: Message) -> Message:
        """Apply request or response publish hooks based on packet kind."""

        ctx = getattr(self._delegate, "_context", None)
        if ctx is None or not getattr(ctx, "middlewares", None):
            return message

        packet_kind = resolve_packet_kind(message)
        for middleware in ctx.middlewares:
            if packet_kind == "response":
                message = await middleware.before_publish_response(topic, message)
            else:
                message = await middleware.before_publish(topic, message)
        return message

    async def publish(self, *args, **kwargs) -> None:
        """
        Flexible publish wrapper that accepts positional or keyword args.

        This allows callers to use either style without breaking existing code. The core
        publish method has positional-only parameters for `topic` and `message`, but this wrapper
        can extract them from either `args` or `kwargs`. It also handles the `deliver_at` logic by 
        wrapping messages in a `ScheduledEnvelope` when needed, and applies any registered middleware 
        before delegating to the underlying publisher.

        The core `Publisher.publish` has positional-only parameters for
        `topic` and `message`. To remain friendly to existing caller styles
        (some examples pass keywords), this wrapper extracts the values from
        either `args` or `kwargs` and delegates to the underlying publisher.
        """
        # Extract topic and message from positional or keyword args
        if len(args) >= 2:
            topic = args[0]
            message = args[1]
        else:
            topic = kwargs.get("topic")
            message = kwargs.get("message")

        timeout_ms = kwargs.get("timeout_ms")
        deliver_at = kwargs.get("deliver_at")

        # Validate presence of required parameters regardless of how they were passed
        if topic is None or message is None:
            raise TypeError("publish() missing required 'topic' and/or 'message' arguments")

        # Wrap delayed messages into a transport-neutral scheduling envelope
        if deliver_at is not None:
            message = await self._apply_before_publish_middlewares(topic, message)

            # The ScheduledEnvelope allows adapters to handle delayed delivery without mutating the original message metadata.
            envelope = ScheduledEnvelope(message=message, target=topic, deliver_at=deliver_at)
            await self._delegate.publish(topic, envelope, timeout_ms=timeout_ms, deliver_at=None)
            return

        # No special features requested — delegate directly
        # Apply middleware from delegate's context if available.
        message = await self._apply_before_publish_middlewares(topic, message)

        await self._delegate.publish(topic, message, timeout_ms=timeout_ms, deliver_at=None)


class Subscriber(ABC):
    """Defines the subscription contract for all adapters.

    Example:
    ```
        async def handler(message: Message) -> None:
            print(message.payload)

        await subscriber.subscribe("events", handler)
    ```
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
        """
        Start consuming messages for a topic.

        Args:
            topic: Logical source to consume from.
            handler: Callback invoked for each received message.
            auto_ack: Whether the adapter should acknowledge automatically.

        Raises:
            message_broker.core.exceptions.ConnectionLostError: If the
                subscription channel cannot be established.

        Example:
        ```     
            await subscriber.subscribe("jobs", handler)
        ```
        """


class Broker(ABC):
    """Coordinates connection lifecycle and access to pub/sub primitives."""

    @abstractmethod
    async def connect(self) -> None:
        """
        Establish all required broker connections and channels. 
        This is separate from the constructor to allow for async initialization and better error handling.
        If the connection fails, an exception should be raised and the broker should not be considered usable.
        Users can rely on the fact that if `connect` completes successfully, the broker is ready for publishing and subscribing.
        For example:
        ```
            broker = MyBroker(...)
            await broker.connect()
            publisher = broker.get_publisher()
            await publisher.publish("events", Message(payload={"event": "created"}))
        ```
        """

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Gracefully terminate connections and release transport resources.
        This should ensure that all pending messages are flushed and that the broker is left in a clean state.
        If the broker is used as an async context manager, this will be called automatically at the end of the
        block to ensure proper cleanup.
        For example:
        ```
            async with MyBroker(...) as broker:
                publisher = broker.get_publisher()
                await publisher.publish("events", Message(payload={"event": "created"}))
        ```
        """

    @abstractmethod
    def get_publisher(self) -> Publisher:
        """
        Return a publisher bound to this broker session.
        This allows users to obtain a publisher instance that is properly configured with the broker's context, 
        including any middleware or serialization settings.
        For example:
            ```
            publisher = broker.get_publisher()
            await publisher.publish("events", Message(payload={"event": "created"}))
            ```
        This method abstracts away the details of how the publisher is created and allows for flexibility in the 
        underlying implementation.
        """

    @abstractmethod
    def get_subscriber(self) -> Subscriber:
        """
        Return a subscriber bound to this broker session.
        This allows users to obtain a subscriber instance that is properly configured with the broker's context, 
        including any middleware or serialization settings.
        For example:
        ```
            subscriber = broker.get_subscriber()
            await subscriber.subscribe("events", handler)
        ```
        This method abstracts away the details of how the subscriber is created and allows for flexibility in the 
        underlying implementation.
        """

    async def __aenter__(self) -> "Broker":
        """
        Allow broker usage through asynchronous context managers.

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
