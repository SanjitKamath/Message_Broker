"""Publish, request/response, and response storage helpers."""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
import warnings
from datetime import datetime
from inspect import isawaitable
from typing import TYPE_CHECKING, TypeAlias, cast

from cryptography.exceptions import InvalidTag
from faststream.message import StreamMessage
from faststream.rabbit import RabbitBroker
from pydantic import ValidationError

from ..exceptions import ConfigurationError, EncryptionError, PublishFailedError
from ..models import DataPacket, Message, Payload, ResponsePacket

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from . import MessageBroker

StoredResponse: TypeAlias = tuple[ResponsePacket, float]


class PublisherService:
    """Encapsulates transport publish/request operations."""

    def __init__(self, broker_owner: "MessageBroker", *, response_store_ttl_seconds: float | None) -> None:
        self._owner = broker_owner
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
        """Publish a request envelope and return its correlation id."""

        owner = self._owner
        owner._core.require_broker()

        packet = DataPacket(
            sender=sender,
            content=content,
            reply_to=owner.reply_queue if reply else None,
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
        await owner._sleep_until(deliver_at)

        await self._publish_raw(
            topic=owner.queue_name,
            payload=prepared.payload,
            correlation_id=packet.correlation_id,
            reply_to=packet.reply_to,
            headers=prepared.headers,
        )
        return packet.correlation_id

    async def send_and_wait(
        self,
        content: Payload,
        sender: str,
        *,
        timeout: float,
        deliver_at: datetime | None = None,
    ) -> ResponsePacket:
        """Send a request envelope and wait for one correlated response."""
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
            return await self._send_and_wait_via_reply_queue(
                content=content,
                sender=sender,
                timeout=timeout,
                deliver_at=deliver_at,
            )

        packet = DataPacket(
            sender=sender,
            content=content,
            reply_to=owner.reply_queue,
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
        await owner._sleep_until(deliver_at)

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
            await owner._sleep_until(deliver_at)
            await self._publish_raw(
                topic=owner.queue_name,
                payload=prepared.payload,
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
        headers: dict[str, str],
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

    async def _request_raw(
        self,
        *,
        topic: str,
        payload: object,
        correlation_id: str,
        timeout: float,
        headers: dict[str, str],
    ) -> StreamMessage[object]:
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
            raise
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
        ttl = self._response_store_ttl_seconds
        if ttl is None:
            return

        now = time.monotonic()
        expired_ids = [response_id for response_id, (_response, stored_at) in self._response_store.items() if now - stored_at > ttl]
        for response_id in expired_ids:
            self._response_store.pop(response_id, None)
