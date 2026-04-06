"""Publish, request/response, and response storage helpers."""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
import warnings
from datetime import datetime, timezone
from inspect import isawaitable
from typing import TYPE_CHECKING, TypeAlias, cast

from faststream.redis import RedisBroker

from cryptography.exceptions import InvalidTag
from faststream.message import StreamMessage
from faststream.rabbit import RabbitBroker
from pydantic import ValidationError

from ..exceptions import (
    ConfigurationError,
    EncryptionError,
    FeatureNotSupportedError,
    PublishFailedError,
)
from ..models import DataPacket, Message, Payload, ResponsePacket
from .delay_strategies import DelayStrategy, RabbitMQDelayStrategy, RedisDelayStrategy

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from collections.abc import Mapping

    from . import MessageBroker

StoredResponse: TypeAlias = tuple[ResponsePacket, float]


class PublisherService:
    """
    Encapsulates transport publish/request operations.
    
    This service is used internally by `MessageBroker` to implement high-level publish and request/response patterns,
    including `send_message`, `send_and_wait`, and `send_and_store`. It also provides a low-level `publish` method 
    for advanced use cases. The service manages an optional in-memory response store for `send_and_store` operations, 
    with TTL-based cleanup to prevent unbounded memory growth.
    """

    def __init__(self, broker_owner: "MessageBroker", *, response_store_ttl_seconds: float | None) -> None:
        self._owner = broker_owner
        self._rabbit_delay_strategy = RabbitMQDelayStrategy()
        self._redis_delay_strategy = RedisDelayStrategy()
        # In-memory response cache for send_and_store/get_response.
        # Thread lock keeps reads/writes safe across threads and async tasks.
        self._response_store_ttl_seconds = response_store_ttl_seconds
        self._response_store: dict[str, StoredResponse] = {}
        self._response_store_lock = threading.Lock()

    async def send_message(
        self,
        content: Payload,
        sender: str,
        reply: bool = False,
        deliver_at: datetime | None = None,
    ) -> str:
        """
        Publish a request envelope and return its correlation id.

        Arguments:
        - `content`: The payload of the message to be sent. This can be any JSON-serializable object that represents the data you want to transmit to consumers.
        - `sender`: A string identifier for the sender of the message, which will be included in the message headers for consumers to identify the source of the message.
        - `reply`: A boolean flag indicating whether the sender expects a reply to this message.
        - `deliver_at`: An optional datetime indicating when the message should be delivered. If provided, the message will be scheduled for future delivery.

        Example:
        ```
        correlation_id = await broker.publisher.send_message(
            content={"task": "process_data", "data_id": 123},
            sender="data_processor_service",
            reply=True,
            deliver_at=datetime.utcnow() + timedelta(minutes=5),
        )
        print(f"Message sent with correlation_id: {correlation_id}")
        ```
        
        If `reply` is True, the message has to include a `reply_to` field with the broker's reply queue, 
        allowing consumers to send responses back to the sender.

        The `deliver_at` parameter allows scheduling the message for future delivery. If provided, the method will
        wait until the specified time before publishing the message. This is a best-effort scheduling mechanism
        and may not guarantee exact delivery timing due to broker and network factors.

        Returns the correlation id of the published message, which can be used for tracking and correlation in 
        request/response patterns.

        Raises:
            `PublishFailedError`: If the message fails to publish, with details for troubleshooting connectivity and 
                                configuration issues.
            `ConfigurationError`: If the broker is not connected or if there are issues with the provided parameters, 
                                such as invalid `deliver_at` values.
        """

        owner = self._owner
        owner._core.require_broker()
        
        # Create a DataPacket with the provided content and sender information, including reply_to if reply is requested.
        packet = DataPacket(
            sender=sender,
            content=content,
            reply_to=owner.reply_queue if reply else None,
            deliver_at=deliver_at,
        )

        # Apply publish middlewares to the message before sending, allowing for transformations, logging, or other processing.
        prepared = await owner._apply_publish_middlewares(
            owner.queue_name,
            Message(
                payload=packet.model_dump(mode="json"),
                headers={"sender": sender},
                metadata={"packet_type": "request", "submitted_at": time.time()},
                correlation_id=packet.correlation_id,
            ),
            is_response=False,
        )

        delay_ms = self._compute_delay_ms(deliver_at)
        await self._publish_with_optional_delay(
            topic=owner.queue_name,
            payload=prepared.payload,
            delay_ms=delay_ms,
            correlation_id=packet.correlation_id,
            reply_to=packet.reply_to,
            headers=prepared.headers,
        )

        # Return the correlation id of the published message for tracking and correlation purposes.
        return packet.correlation_id

    async def send_and_wait(
        self,
        content: Payload,
        sender: str,
        *,
        timeout: float,
        deliver_at: datetime | None = None,
    ) -> ResponsePacket:
        """
        Send a request envelope and wait for one correlated response.
        
        This method implements a request/response pattern where the sender publishes a message and waits for a response
        that has a matching correlation id. The response is expected to be published by a consumer that processes the 
        request and sends a reply to the sender's reply queue.

        Arguments:
        - `content`: The payload of the request message to be sent. This can be any JSON-serializable object that represents the data you want to transmit to consumers.
        - `sender`: A string identifier for the sender of the message, which will be included in the message headers for consumers to identify the source of the message.
        - `timeout`: The maximum amount of time (in seconds) to wait for a response before giving up. This is required to prevent indefinite blocking if a response is not received. If the timeout is reached without receiving a response, a `TimeoutError` is raised.
        - `deliver_at`: An optional datetime indicating when the request should be delivered. If provided, the request will be scheduled for future delivery.

        Example:
        ```
        try:
            response = await broker.publisher.send_and_wait(
                content={"task": "process_data", "data_id": 123},
                sender="data_processor_service",
                timeout=30.0,
                deliver_at=datetime.utcnow() + timedelta(minutes=5),
            )
            print(f"Received response: {response}")
        except TimeoutError:
            print("No response received within the timeout period.")
        ```

        The `timeout` parameter is required to prevent indefinite blocking if a response is not received. If the timeout
        is reached without receiving a response, a `TimeoutError` is raised.

        The `deliver_at` parameter allows scheduling the request for future delivery, similar to `send_message`.

        Returns the `ResponsePacket` received in reply to the request.

        Raises:
            `ConfigurationError`: If the timeout is not provided or is invalid.
            `TimeoutError`: If the timeout is reached without receiving a response.
            `EncryptionError`: If decryption of the response fails, likely due to mismatched AES keys between producer and consumer.
            `PublishFailedError`: If there is an error during publishing or requesting, with details for troubleshooting connectivity and configuration issues.
        """
        if timeout is None:
            raise ConfigurationError("send_and_wait requires a timeout to prevent indefinite blocking.")
        if timeout <= 0:
            raise ConfigurationError("send_and_wait timeout must be greater than 0 seconds.")

        owner = self._owner
        owner._core.require_broker()
        if owner._scheme in {"redis", "rediss"}:
            warnings.warn(
                "Redis transport does not guarantee strict request-response semantics like RabbitMQ.",
                RuntimeWarning,
                stacklevel=2,
            )
            # For Redis, we implement request-response using an explicit reply queue and correlation matching, since Redis does not
            # support native request-response patterns like RabbitMQ. This is a best-effort implementation and may not guarantee
            # strict correlation in high-throughput scenarios, but it provides a way to achieve similar functionality with Redis.
            return await self._send_and_wait_via_reply_queue(
                content=content,
                sender=sender,
                timeout=timeout,
                deliver_at=deliver_at,
            )
        # For RabbitMQ, we can leverage the broker's native request-response support, which handles correlation and 
        # reply queues more robustly.

        # Create a DataPacket with the provided content and sender information, including reply_to for RabbitMQ's 
        # request-response pattern.
        packet = DataPacket(
            sender=sender,
            content=content,
            reply_to=owner.reply_queue,
        )

        # Apply publish middlewares to the message before sending, allowing for transformations, logging, or other processing.
        prepared = await owner._apply_publish_middlewares(
            owner.queue_name,
            Message(
                payload=packet.model_dump(mode="json"),
                headers={"sender": sender},
                metadata={"packet_type": "request", "submitted_at": time.time()},
                correlation_id=packet.correlation_id,
            ),
            is_response=False,
        )

        delay_ms = self._compute_delay_ms(deliver_at)
        await self._publish_with_optional_delay(
            topic=owner.queue_name,
            payload=prepared.payload,
            delay_ms=delay_ms,
            correlation_id=packet.correlation_id,
            reply_to=packet.reply_to,
            headers=prepared.headers,
        )

        # Use the low-level _request_raw method to send the request and wait for a response, which handles the actual
        # transport-specific request/response logic. This will raise a TimeoutError if the timeout is reached without 
        # receiving a response.
        try:
            response_message = await self._request_raw(
                topic=owner.queue_name,
                payload=prepared.payload,
                correlation_id=packet.correlation_id,
                timeout=timeout,
                headers=prepared.headers,
            )
        except TimeoutError as exc:
            raise TimeoutError(
                f"Timed out waiting for response correlation_id={packet.correlation_id}. "
                "Increase timeout or ensure a consumer is running for this queue."
            ) from exc

        try:
            decoded_response = await response_message.decode()
        except (EncryptionError, InvalidTag) as exc:
            raise EncryptionError(
                "Decryption failed while reading request-response reply. "
                "Ensure both producer and consumer use the same AES key."
            ) from exc

        response = self._coerce_response_packet(
            decoded_response,
            correlation_id=packet.correlation_id,
            in_response_to=packet.id,
        )
        if response.correlation_id != packet.correlation_id:
            raise PublishFailedError(
                "Response correlation mismatch detected. "
                "Ensure request/response handlers preserve correlation_id."
            )

        reply_handler = owner._reply_handler
        if reply_handler is not None:
            callback_result = reply_handler(response)
            if isawaitable(callback_result):
                await cast("Awaitable[None]", callback_result)

        return response

    async def _send_and_wait_via_reply_queue(
        self,
        *,
        content: Payload,
        sender: str,
        timeout: float,
        deliver_at: datetime | None,
    ) -> ResponsePacket:
        """Fallback request/reply for Redis using explicit reply queue correlation matching."""
        owner = self._owner
        owner._ensure_reply_subscription()

        packet = DataPacket(
            sender=sender,
            content=content,
            reply_to=owner.reply_queue,
            deliver_at=deliver_at,
        )

        prepared = await owner._apply_publish_middlewares(
            owner.queue_name,
            Message(
                payload=packet.model_dump(mode="json"),
                headers={"sender": sender},
                metadata={"packet_type": "request", "submitted_at": time.time()},
                correlation_id=packet.correlation_id,
            ),
            is_response=False,
        )

        waiter = owner._create_pending_response_waiter(packet.correlation_id)
        try:
            delay_ms = self._compute_delay_ms(deliver_at)
            await self._publish_with_optional_delay(
                topic=owner.queue_name,
                payload=prepared.payload,
                delay_ms=delay_ms,
                correlation_id=packet.correlation_id,
                reply_to=packet.reply_to,
                headers=prepared.headers,
            )
            try:
                return await asyncio.wait_for(waiter, timeout=timeout)
            except TimeoutError as exc:
                raise TimeoutError(
                    "Timed out waiting for a correlated Redis reply. "
                    "Responses with mismatched correlation_id were ignored."
                ) from exc
        finally:
            owner._pop_pending_response_waiter(packet.correlation_id)

    async def send_and_store(
        self,
        content: Payload,
        sender: str,
        *,
        timeout: float,
        deliver_at: datetime | None = None,
    ) -> str:
        """Send a request/response message and store the response for later retrieval."""

        response = await self.send_and_wait(
            content=content,
            sender=sender,
            timeout=timeout,
            deliver_at=deliver_at,
        )
        # response_id intentionally differs from correlation_id so callers can treat
        # storage retrieval as an independent lookup concern.
        response_id = uuid.uuid4().hex
        with self._response_store_lock:
            self._cleanup_expired_locked()
            self._response_store[response_id] = (response, time.monotonic())
        return response_id

    def get_response(self, response_id: str) -> ResponsePacket | None:
        """Return a stored response by id, or `None` if missing."""

        with self._response_store_lock:
            self._cleanup_expired_locked()
            entry = self._response_store.get(response_id)
            if entry is None:
                return None
            response, _stored_at = entry
            return response

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
        """Low-level publish API for advanced/internal usage."""

        owner = self._owner
        owner._core.require_broker()

        corr_id = correlation_id or uuid.uuid4().hex
        prepared = await owner._apply_publish_middlewares(
            topic,
            Message(
                payload=payload,
                headers=headers or {},
                metadata={
                    "packet_type": "response" if is_response else "request",
                    "submitted_at": time.time(),
                },
                correlation_id=corr_id,
            ),
            is_response=is_response,
        )
        await self._publish_raw(
            topic=topic,
            payload=prepared.payload,
            correlation_id=corr_id,
            reply_to=reply_to,
            headers=prepared.headers,
        )
        return corr_id

    async def publish_response(self, topic: str, response: ResponsePacket) -> None:
        """Publish a normalized `ResponsePacket` to a reply queue."""

        owner = self._owner
        prepared = await owner._apply_publish_middlewares(
            topic,
            Message(
                payload=response.model_dump(mode="json"),
                headers={},
                metadata={"packet_type": "response", "submitted_at": time.time()},
                correlation_id=response.correlation_id,
            ),
            is_response=True,
        )
        await self._publish_raw(
            topic=topic,
            payload=prepared.payload,
            correlation_id=response.correlation_id,
            reply_to=None,
            headers=prepared.headers,
        )

    async def _publish_raw(
        self,
        *,
        topic: str,
        payload: object,
        correlation_id: str,
        reply_to: str | None,
        headers: "Mapping[str, object]",
    ) -> None:
        broker = self._owner._core.require_broker()
        try:
            if isinstance(broker, RabbitBroker):
                await broker.publish(
                    payload,
                    queue=topic,
                    correlation_id=correlation_id,
                    reply_to=reply_to,
                    headers=headers,
                )
                return

            await broker.publish(
                payload,
                list=topic,
                correlation_id=correlation_id,
                reply_to=reply_to or "",
                headers=headers,
            )
        except Exception as exc:
            raise PublishFailedError(
                f"Failed to publish message to '{topic}'. Verify broker connectivity and queue/topic name."
            ) from exc

    async def _publish_with_optional_delay(
        self,
        *,
        topic: str,
        payload: object,
        delay_ms: int,
        correlation_id: str,
        reply_to: str | None,
        headers: "Mapping[str, object]",
    ) -> None:
        owner = self._owner
        if delay_ms <= 0:
            await self._publish_raw(
                topic=topic,
                payload=payload,
                correlation_id=correlation_id,
                reply_to=reply_to,
                headers=headers,
            )
            return

        scheme = owner._scheme
        if scheme is None:
            raise ConfigurationError("Broker is not connected. Call connect() first.")
        strategy = self._get_delay_strategy(scheme)
        broker = owner._core.require_broker()
        await strategy.publish_with_delay(
            broker=broker,
            topic=topic,
            payload=payload,
            delay_ms=delay_ms,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
        )

    def _get_delay_strategy(self, scheme: str) -> DelayStrategy:
        if scheme in {"amqp", "amqps"}:
            return self._rabbit_delay_strategy
        if scheme in {"redis", "rediss"}:
            return self._redis_delay_strategy
        raise FeatureNotSupportedError("Delayed delivery not supported")

    def _compute_delay_ms(self, deliver_at: datetime | None) -> int:
        if deliver_at is None:
            return 0
        target = deliver_at if deliver_at.tzinfo is not None else deliver_at.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return max(0, int((target - now).total_seconds() * 1000))

    async def _request_raw(
        self,
        *,
        topic: str,
        payload: object,
        correlation_id: str,
        timeout: float,
        headers: dict[str, str],
    ) -> StreamMessage[object]:
        """
        The low-level request method that directly interacts with the broker's request/response capabilities. 
        This is used by `send_and_wait` to implement the request/response pattern. It sends a request message and 
        waits for a response with a matching correlation id, handling transport-specific logic for RabbitMQ and Redis.

        Raises:
            `TimeoutError`: If the timeout is reached without receiving a response.
            `PublishFailedError`: If there is an error during the request operation, with details for 
                                troubleshooting connectivity and configuration issues.
        """
        broker = self._owner._core.require_broker()
        try:
            if isinstance(broker, RabbitBroker):
                return cast(
                    "StreamMessage[object]",
                    await broker.request(
                        payload,
                        queue=topic,
                        correlation_id=correlation_id,
                        timeout=timeout,
                        headers=headers,
                    ),
                )
            elif isinstance(broker, RedisBroker):
                return cast(
                    "StreamMessage[object]",
                    await broker.request(
                        payload,
                        list=topic,
                        correlation_id=correlation_id,
                        timeout=timeout,
                        headers=headers,
                    ),
                )
        except TimeoutError:
            raise TimeoutError(
                f"Timed out waiting for response correlation_id={correlation_id}. "     
                "Increase timeout or ensure a consumer is running for this queue."
            )
        except Exception as exc:
            raise PublishFailedError(
                f"Failed to perform request on '{topic}'. Verify broker request/reply support for this transport."
            ) from exc

    def _coerce_response_packet(
        self,
        payload: object,
        *,
        correlation_id: str | None,
        in_response_to: str | None,
    ) -> ResponsePacket:
        """
        Try to coerce a raw response payload into a `ResponsePacket`. If the payload is a dict, we attempt to parse it as a
        `ResponsePacket` using Pydantic's validation. If parsing fails, we fall back to treating the entire payload as the 
        content of a new `ResponsePacket`, using the provided correlation and in_response_to values for metadata. If the 
        payload is not a dict, we directly create a `ResponsePacket` with the payload as content and the provided metadata.
        This allows for flexible response formats while still providing a normalized `ResponsePacket` structure for downstream 
        processing.
        """
        if isinstance(payload, dict):
            try:
                return ResponsePacket.model_validate(payload)
            except ValidationError:
                return ResponsePacket(
                    correlation_id=str(payload.get("correlation_id") or correlation_id or uuid.uuid4().hex),
                    in_response_to=str(payload.get("in_response_to") or in_response_to or "unknown"),
                    status=str(payload.get("status") or "processed"),
                    content=payload.get("content", payload),
                )

        return ResponsePacket(
            correlation_id=correlation_id or uuid.uuid4().hex,
            in_response_to=in_response_to or "unknown",
            status="processed",
            content=cast("Payload", payload),
        )

    def _cleanup_expired_locked(self) -> None:
        """
        Remove expired entries from the in-memory response store based on the configured TTL. This method should be called
        while holding the `_response_store_lock` to ensure thread safety. It checks the current time
        against the stored timestamp of each response and removes any entries that have exceeded the TTL, preventing unbounded
        memory growth from stale responses.
        """
        ttl = self._response_store_ttl_seconds
        if ttl is None:
            return

        now = time.monotonic()
        expired_ids = [response_id for response_id, (_response, stored_at) in self._response_store.items() if now - stored_at > ttl]
        for response_id in expired_ids:
            self._response_store.pop(response_id, None)
