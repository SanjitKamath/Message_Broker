"""High-level broker wrapper for :mod:`lyik_messaging`."""

from __future__ import annotations

import asyncio
import uuid
import warnings
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from inspect import isawaitable
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel

from ..decorators import OnMessageHandler, build_on_message_decorator
from ..encryption import normalize_aes_key
from ..exceptions import ConfigurationError, MessageBrokerError, PublishFailedError
from ..models import Message, Middleware, Payload, ResponsePacket
from .consumer import ConsumerService
from .core import BrokerCore
from .internal_types import MaybeAwaitable, ReplyHandler, StrictMessageHandler
from .publisher import PublisherService
from .retry import RetryPolicy

if TYPE_CHECKING:
    from .internal_types import FastStreamBroker


class MessageBroker:
    """FastStream-backed broker wrapper with strict message handlers."""

    def __init__(
        self,
        uri: str,
        queue_name: str = "default_queue",
        *,
        aes_key: str | None = None,
        **context_options: object,
    ) -> None:
        self._uri = uri.strip()
        if not self._uri:
            raise ConfigurationError(
                "Broker URI is empty. Provide a URI like amqp://... or redis://..."
            )

        self.queue_name = queue_name
        self.reply_queue = f"reply_queue_{uuid.uuid4().hex}"

        self._context_options = dict(context_options)
        if "observers" in self._context_options:
            raise ConfigurationError(
                "The 'observers' option is no longer supported. "
                "Remove it from MessageBroker(...) configuration."
            )

        self._processing_timeout_ms = _coerce_optional_positive_int(
            self._context_options.get("processing_timeout_ms")
        )

        self._retry_policy = RetryPolicy(
            max_retries=_coerce_non_negative_int(self._context_options.get("handler_max_retries"), 0),
            base_delay_ms=_coerce_positive_int(self._context_options.get("retry_base_delay_ms"), 100),
            max_delay_ms=_coerce_optional_positive_int(self._context_options.get("retry_max_delay_ms")),
            jitter=bool(self._context_options.get("retry_jitter", False)),
        )
        self._response_store_ttl_seconds = _coerce_optional_positive_float(
            self._context_options.get("response_store_ttl_seconds")
        )

        middleware_option = self._context_options.get("middlewares", [])
        if not isinstance(middleware_option, list):
            raise ConfigurationError(
                "Invalid 'middlewares' option: expected list[Middleware], "
                f"got {type(middleware_option).__name__}. Example: middlewares=[MyMiddleware()]."
            )
        self._middlewares: list[Middleware] = []
        for middleware in middleware_option:
            if not isinstance(middleware, Middleware):
                raise ConfigurationError(
                    "Invalid middleware entry: expected Middleware instance, "
                    f"got {type(middleware).__name__}."
                )
            self._middlewares.append(middleware)

        self._aes_key = normalize_aes_key(aes_key) if aes_key is not None else None

        self._broker: FastStreamBroker | None = None
        self._scheme: str | None = None
        self._run_task: asyncio.Task[None] | None = None

        self._strict_registrations: list[tuple[str, Callable[..., object]]] = []
        self._reply_handler: ReplyHandler | None = None
        self._reply_wrapper: Callable[..., Awaitable[None]] | None = None
        self._registered_queues: set[str] = set()
        self._used_queues: set[str] = set()
        self._request_reply_queues: set[str] = set()
        self._pending_responses: dict[str, asyncio.Future[ResponsePacket]] = {}

        # Keep orchestration thin: lifecycle/publish/consume concerns live in services.
        self._core = BrokerCore(self)
        self._publisher = PublisherService(
            self,
            response_store_ttl_seconds=self._response_store_ttl_seconds,
        )
        self._consumer = ConsumerService(self)

    async def connect(self) -> None:
        """Create and connect the underlying FastStream broker."""

        await self._core.connect()

    async def start(self) -> None:
        """Start subscriptions and keep processing until cancelled."""

        await self._core.start()

    async def run(self) -> None:
        """Connect and start broker lifecycle in one call."""

        await self._core.run()

    async def disconnect(self) -> None:
        """Stop and release the underlying FastStream broker."""

        await self._core.disconnect()

    async def __aenter__(self) -> "MessageBroker":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.disconnect()

    def on_message(self, queue: str) -> Callable[[OnMessageHandler], OnMessageHandler]:
        """Register a strict handler using `@broker.on_message("queue")`."""

        if not isinstance(queue, str):
            raise ConfigurationError(
                "Invalid on_message usage. Use @broker.on_message(\"queue\") with a string queue name."
            )

        normalized_queue = queue.strip()
        if not normalized_queue:
            raise ConfigurationError("Queue name must not be empty for @broker.on_message(\"queue\").")
        return build_on_message_decorator(self, normalized_queue)

    def on_reply(
        self,
        handler: ReplyHandler | None = None,
    ) -> Callable[[ReplyHandler], Callable[..., Awaitable[None]]] | Callable[..., Awaitable[None]]:
        """Register a reply handler for `ResponsePacket` payloads."""

        if handler is not None:
            return self._register_reply_handler(handler)

        def decorator(fn: ReplyHandler) -> Callable[..., Awaitable[None]]:
            return self._register_reply_handler(fn)

        return decorator

    async def send_message(
        self,
        content: Payload,
        sender: str,
        reply: bool = False,
        deliver_at: datetime | None = None,
    ) -> str:
        """Fire-and-forget publish API with optional delayed delivery."""
        self._used_queues.add(self.queue_name)
        if self.queue_name not in self._registered_queues:
            warnings.warn(
                f"Sending to queue '{self.queue_name}' with no local handler registered. "
                "If this is intentional, ignore this warning.",
                RuntimeWarning,
                stacklevel=2,
            )

        return await self._publisher.send_message(
            content=content,
            sender=sender,
            reply=reply,
            deliver_at=deliver_at,
        )

    async def send_and_wait(
        self,
        content: Payload,
        sender: str,
        *,
        timeout: float,
        deliver_at: datetime | None = None,
    ) -> ResponsePacket:
        """Request/response API that returns a correlated `ResponsePacket`."""
        if timeout is None:
            raise ConfigurationError("send_and_wait requires a timeout to prevent indefinite blocking.")
        if timeout <= 0:
            raise ConfigurationError("send_and_wait timeout must be greater than 0 seconds.")

        self._used_queues.add(self.queue_name)
        self._request_reply_queues.add(self.queue_name)
        self._validate_request_reply_queues()

        return await self._publisher.send_and_wait(
            content=content,
            sender=sender,
            timeout=timeout,
            deliver_at=deliver_at,
        )

    async def send_and_store(
        self,
        content: Payload,
        sender: str,
        *,
        timeout: float,
        deliver_at: datetime | None = None,
    ) -> str:
        """Request/response API that stores the response and returns a response id."""
        if timeout is None:
            raise ConfigurationError("send_and_store requires a timeout to prevent indefinite blocking.")
        if timeout <= 0:
            raise ConfigurationError("send_and_store timeout must be greater than 0 seconds.")

        self._used_queues.add(self.queue_name)
        self._request_reply_queues.add(self.queue_name)
        self._validate_request_reply_queues()

        return await self._publisher.send_and_store(
            content=content,
            sender=sender,
            timeout=timeout,
            deliver_at=deliver_at,
        )

    def get_response(self, response_id: str) -> ResponsePacket | None:
        """Retrieve a response previously stored with `send_and_store`."""

        return self._publisher.get_response(response_id)

    async def publish(
        self,
        topic: str,
        payload: object,
        *,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        is_response: bool = False,
    ) -> str:
        """Low-level publish API for advanced/internal use cases."""

        return await self._publisher.publish(
            topic=topic,
            payload=payload,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
            is_response=is_response,
        )

    def _register_message_handler(
        self,
        *,
        queue: str,
        payload_model: type[BaseModel],
        handler: StrictMessageHandler,
    ) -> None:
        self._consumer.register_message_handler(
            queue=queue,
            payload_model=payload_model,
            handler=handler,
        )

    def _register_reply_handler(
        self,
        handler: ReplyHandler,
    ) -> Callable[..., Awaitable[None]]:
        return self._consumer.register_reply_handler(handler)

    def _ensure_reply_subscription(self) -> None:
        """Ensure reply queue subscription exists for internal request/reply flows."""
        if self._reply_wrapper is None:
            self._register_reply_handler(_noop_reply_handler)

    def _create_pending_response_waiter(self, correlation_id: str) -> asyncio.Future[ResponsePacket]:
        """Create a pending waiter keyed by request correlation id."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[ResponsePacket] = loop.create_future()
        self._pending_responses[correlation_id] = future
        return future

    def _pop_pending_response_waiter(self, correlation_id: str) -> asyncio.Future[ResponsePacket] | None:
        """Remove and return pending waiter if present."""
        return self._pending_responses.pop(correlation_id, None)

    def _resolve_pending_response(self, response: ResponsePacket) -> bool:
        """Resolve pending waiter for a matched response correlation id."""
        future = self._pending_responses.get(response.correlation_id)
        if future is None:
            return False
        if future.done():
            raise MessageBrokerError(
                "Conflicting responses received for the same correlation_id. "
                "Ensure only one consumer handles each request."
            )
        future.set_result(response)
        return True

    async def _send_response(self, topic: str, response: ResponsePacket) -> None:
        """Broker-level response publishing to avoid service coupling."""

        await self._publisher.publish_response(topic, response)

    def _coerce_response_packet(
        self,
        payload: object,
        *,
        correlation_id: str | None,
        in_response_to: str | None,
    ) -> ResponsePacket:
        return self._publisher._coerce_response_packet(
            payload,
            correlation_id=correlation_id,
            in_response_to=in_response_to,
        )

    async def _resolve_with_timeout(
        self,
        maybe_result: MaybeAwaitable[Payload | None],
    ) -> Payload | None:
        if self._processing_timeout_ms is None:
            return await _resolve_result(maybe_result)

        return await asyncio.wait_for(
            _resolve_result(maybe_result),
            timeout=self._processing_timeout_ms / 1000.0,
        )

    async def _apply_publish_middlewares(
        self,
        topic: str,
        message: Message,
        *,
        is_response: bool,
    ) -> Message:
        current = message
        for middleware in self._middlewares:
            attempt = 0
            while True:
                try:
                    if is_response:
                        current = await middleware.before_publish_response(topic, current)
                    else:
                        current = await middleware.before_publish(topic, current)
                    break
                except Exception as exc:
                    if attempt >= self._retry_policy.max_retries:
                        raise PublishFailedError(
                            f"Middleware '{type(middleware).__name__}' failed during publish on topic '{topic}'. "
                            "Check middleware implementation and retry policy settings."
                        ) from exc
                    delay = self._retry_policy.compute_delay_seconds(attempt)
                    attempt += 1
                    await asyncio.sleep(delay)
        return current

    async def _apply_consume_middlewares(
        self,
        topic: str,
        message: Message,
        *,
        is_response: bool,
    ) -> Message:
        current = message
        for middleware in self._middlewares:
            attempt = 0
            while True:
                try:
                    if is_response:
                        current = await middleware.after_consume_response(topic, current)
                    else:
                        current = await middleware.after_consume(topic, current)
                    break
                except Exception as exc:
                    if attempt >= self._retry_policy.max_retries:
                        raise MessageBrokerError(
                            f"Middleware '{type(middleware).__name__}' failed during consume on topic '{topic}'. "
                            "Check middleware implementation and retry policy settings."
                        ) from exc
                    delay = self._retry_policy.compute_delay_seconds(attempt)
                    attempt += 1
                    await asyncio.sleep(delay)
        return current

    def _validate_request_reply_queues(self) -> None:
        """Fail fast when request/reply queues have no strict consumer handlers."""
        missing = sorted(queue for queue in self._request_reply_queues if queue not in self._registered_queues)
        if missing:
            missing_list = ", ".join(f"'{queue}'" for queue in missing)
            raise ConfigurationError(
                "Request/reply queue validation failed. Register a handler via "
                f"@broker.on_message(\"queue\") for: {missing_list}."
            )

    async def _sleep_until(self, deliver_at: datetime | None) -> None:
        if deliver_at is None:
            return

        target = deliver_at
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)

        delay = (target - datetime.now(timezone.utc)).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)


async def _resolve_result(maybe_result: MaybeAwaitable[Payload | None]) -> Payload | None:
    if isawaitable(maybe_result):
        return await cast("Awaitable[Payload | None]", maybe_result)
    return cast("Payload | None", maybe_result)


def _coerce_positive_int(value: object, default: int) -> int:
    if not isinstance(value, int) or value <= 0:
        return default
    return value


def _coerce_non_negative_int(value: object, default: int) -> int:
    if not isinstance(value, int) or value < 0:
        return default
    return value


def _coerce_optional_positive_int(value: object) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or value <= 0:
        return None
    return value


def _coerce_optional_positive_float(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, (int, float)) or float(value) <= 0.0:
        return None
    return float(value)


async def _noop_reply_handler(_response: ResponsePacket) -> None:
    return None
