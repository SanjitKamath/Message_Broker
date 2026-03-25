"""RabbitMQ adapter built on aio-pika primitives.

This adapter mirrors the Redis adapter contract while using RabbitMQ queues for
transport. It keeps the same publish/subscribe interface and middleware flow so
applications can switch transports by URI only.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from dataclasses import asdict
from inspect import isawaitable
from typing import Any
import time

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractIncomingMessage,
    AbstractQueue,
)
from aiormq.exceptions import AMQPConnectionError, AMQPError

from ..core.context import BrokerContext
from ..core.exceptions import ConfigurationError, ConnectionLostError, PublishFailedError
from ..core.interfaces import (
    Broker,
    EnforcingPublisher,
    Message,
    MessageHandler,
    Publisher,
    ScheduledEnvelope,
    Subscriber,
)
from ..core.registry import BrokerRegistry
from ..core.resilience import with_retries


class RabbitMQPublisher(Publisher):
    """Publishes serialized messages to RabbitMQ queues via default exchange."""

    def __init__(self, channel: AbstractChannel, context: BrokerContext) -> None:
        self._channel = channel
        self._context = context
        self._declare_lock = asyncio.Lock()
        self._declared_queues: set[str] = set()
        self._declared_delay_queues: set[str] = set()
        self._delay_prefix = str(
            self._context.options.get("rabbitmq_delay_queue_prefix", "broker.delay")
        )

    @with_retries(max_retries=3, base_delay=1.0)
    async def publish(
        self,
        topic: str,
        message: Message,
        /,
        *,
        timeout_ms: int | None = None,
        deliver_at: float | None = None,
    ) -> None:
        """Serialize and publish a message to a durable queue.

        Delayed delivery is implemented with durable delay queues:
        messages are published to an intermediate queue with per-message TTL
        and dead-lettered back to the target queue when due.
        """

        resolved_topic = topic
        effective_deliver_at = deliver_at

        if isinstance(message, ScheduledEnvelope):
            scheduled = message
            resolved_topic = scheduled.target
            effective_deliver_at = float(scheduled.deliver_at)
            payload = self._context.serializer.serialize(asdict(scheduled.message))
            headers = dict(scheduled.message.headers)
            correlation_id = scheduled.message.correlation_id
        else:
            payload = self._context.serializer.serialize(asdict(message))
            headers = dict(message.headers)
            correlation_id = message.correlation_id

        body = payload.encode("utf-8") if isinstance(payload, str) else payload

        resolved_timeout_ms = timeout_ms or int(self._context.options.get("timeout", 5000))
        timeout_s = resolved_timeout_ms / 1000.0

        try:
            if effective_deliver_at is None:
                await self._publish_immediately(
                    topic=resolved_topic,
                    body=body,
                    correlation_id=correlation_id,
                    headers=headers,
                    timeout_s=timeout_s,
                )
                return

            delay_ms = max(0, int((float(effective_deliver_at) - time.time()) * 1000))
            if delay_ms == 0:
                await self._publish_immediately(
                    topic=resolved_topic,
                    body=body,
                    correlation_id=correlation_id,
                    headers=headers,
                    timeout_s=timeout_s,
                )
                return

            # RabbitMQ expiration field is a string and bounded to a 32-bit unsigned int.
            bounded_delay_ms = min(delay_ms, 2_147_483_647)
            await self._publish_delayed(
                topic=resolved_topic,
                body=body,
                correlation_id=correlation_id,
                headers=headers,
                delay_ms=bounded_delay_ms,
                timeout_s=timeout_s,
            )
        except (AMQPConnectionError, AMQPError) as exc:
            raise ConnectionLostError(
                "RabbitMQ publish failed due to connection interruption.",
                broker="rabbitmq",
                operation="publish",
                details={"topic": resolved_topic, "timeout_ms": resolved_timeout_ms},
            ) from exc
        except Exception as exc:
            raise PublishFailedError(
                "RabbitMQ publish failed unexpectedly.",
                broker="rabbitmq",
                operation="publish",
                details={"topic": resolved_topic, "timeout_ms": resolved_timeout_ms},
            ) from exc

    async def _publish_immediately(
        self,
        *,
        topic: str,
        body: bytes,
        correlation_id: str,
        headers: dict[str, str],
        timeout_s: float,
    ) -> None:
        await self._ensure_target_queue(topic)
        amqp_message = aio_pika.Message(
            body=body,
            correlation_id=correlation_id,
            headers=headers,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await asyncio.wait_for(
            self._channel.default_exchange.publish(
                amqp_message,
                routing_key=topic,
            ),
            timeout=timeout_s,
        )

    async def _publish_delayed(
        self,
        *,
        topic: str,
        body: bytes,
        correlation_id: str,
        headers: dict[str, str],
        delay_ms: int,
        timeout_s: float,
    ) -> None:
        delay_queue_name = await self._ensure_delay_queue(topic)
        amqp_message = aio_pika.Message(
            body=body,
            correlation_id=correlation_id,
            headers=headers,
            content_type="application/json",
            expiration=timedelta(milliseconds=delay_ms),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await asyncio.wait_for(
            self._channel.default_exchange.publish(
                amqp_message,
                routing_key=delay_queue_name,
            ),
            timeout=timeout_s,
        )

    async def _ensure_target_queue(self, topic: str) -> None:
        if topic in self._declared_queues:
            return
        async with self._declare_lock:
            if topic in self._declared_queues:
                return
            await self._channel.declare_queue(topic, durable=True)
            self._declared_queues.add(topic)

    async def _ensure_delay_queue(self, topic: str) -> str:
        delay_queue_name = f"{self._delay_prefix}.{topic}"
        if delay_queue_name in self._declared_delay_queues:
            return delay_queue_name

        async with self._declare_lock:
            if delay_queue_name in self._declared_delay_queues:
                return delay_queue_name

            await self._channel.declare_queue(topic, durable=True)
            await self._channel.declare_queue(
                delay_queue_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": topic,
                },
            )
            self._declared_queues.add(topic)
            self._declared_delay_queues.add(delay_queue_name)
            return delay_queue_name


class RabbitMQSubscriber(Subscriber):
    """Consumes messages from RabbitMQ queues with middleware-aware callbacks."""

    def __init__(self, channel: AbstractChannel, context: BrokerContext) -> None:
        self._channel = channel
        self._context = context
        self._consumers: list[tuple[AbstractQueue, str]] = []

    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        /,
        *,
        auto_ack: bool = True,
    ) -> None:
        """Subscribe a callback to a durable queue."""

        try:
            queue = await self._channel.declare_queue(topic, durable=True)

            async def _on_message(incoming: AbstractIncomingMessage) -> None:
                message = self._deserialize(incoming.body)
                message.metadata.setdefault("source_topic", topic)

                for middleware in self._context.middlewares:
                    try:
                        message = await middleware.after_consume(topic, message)
                    except Exception:
                        continue

                if auto_ack:
                    await handler(message)
                    return

                try:
                    await handler(message)
                except Exception:
                    await incoming.nack(requeue=True)
                    return

                await incoming.ack()

            consumer_tag = await queue.consume(_on_message, no_ack=auto_ack)
            self._consumers.append((queue, consumer_tag))
        except (AMQPConnectionError, AMQPError) as exc:
            raise ConnectionLostError(
                "Failed to start RabbitMQ subscriber.",
                broker="rabbitmq",
                operation="subscribe",
                details={"topic": topic},
            ) from exc

    async def prepare_shutdown(self, drain_timeout_ms: int | None = None) -> None:
        """Cancel active consumers before broker shutdown."""

        timeout_s = (drain_timeout_ms or 0) / 1000.0
        tasks = [queue.cancel(tag) for queue, tag in self._consumers]
        if not tasks:
            return

        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=timeout_s or None)
        except asyncio.TimeoutError:
            return
        finally:
            self._consumers.clear()

    def _deserialize(self, payload: bytes) -> Message:
        """Deserialize queue payload into Message dataclass."""

        loaded = self._context.serializer.deserialize(payload)
        headers = loaded.get("headers")
        metadata = loaded.get("metadata")

        correlation_id = loaded.get("correlation_id")
        correlation = str(correlation_id) if correlation_id is not None else ""

        return Message(
            payload=loaded.get("payload"),
            headers=dict(headers) if isinstance(headers, dict) else {},
            metadata=dict(metadata) if isinstance(metadata, dict) else {},
            correlation_id=correlation,
        )


class RabbitMQBroker(Broker):
    """Concrete broker implementation backed by aio-pika."""

    def __init__(self, context: BrokerContext) -> None:
        self.context = context
        self._connection: AbstractConnection | None = None
        self._channel: AbstractChannel | None = None
        self._subscribers: list[RabbitMQSubscriber] = []
        observers = self.context.options.get("observers", [])
        self._observers = list(observers) if isinstance(observers, list) else []

    async def connect(self) -> None:
        """Establish RabbitMQ connection/channel and configure QoS."""

        timeout_ms = int(self.context.options.get("timeout", 5000))
        timeout_s = timeout_ms / 1000.0

        try:
            self._connection = await aio_pika.connect_robust(self.context.connection_uri, timeout=timeout_s)
            self._channel = await self._connection.channel()
            prefetch = self.context.options.get("concurrency", 10)
            if isinstance(prefetch, int) and prefetch > 0:
                await self._channel.set_qos(prefetch_count=prefetch)
            await self._notify_runtime_event(
                event="connected",
                details={"uri": self.context.connection_uri, "prefetch": prefetch},
            )
        except (AMQPConnectionError, AMQPError) as exc:
            raise ConnectionLostError(
                "Failed to connect to RabbitMQ broker.",
                broker="rabbitmq",
                operation="connect",
                details={"uri": self.context.connection_uri},
            ) from exc

    async def disconnect(self) -> None:
        """Cancel subscribers and close RabbitMQ resources cleanly."""

        drain_timeout_ms = self.context.options.get("shutdown_drain_timeout_ms", 1500)
        if isinstance(drain_timeout_ms, int) and drain_timeout_ms > 0:
            for subscriber in list(self._subscribers):
                try:
                    await subscriber.prepare_shutdown(drain_timeout_ms=drain_timeout_ms)
                except Exception:
                    pass

        if self._channel is not None:
            try:
                await self._channel.close()
            except Exception:
                pass
            finally:
                self._channel = None

        if self._connection is not None:
            try:
                await self._connection.close()
            except Exception as exc:
                raise ConnectionLostError(
                    "Failed while closing RabbitMQ broker connection.",
                    broker="rabbitmq",
                    operation="disconnect",
                ) from exc
            finally:
                self._connection = None

        self._subscribers.clear()

    def get_publisher(self) -> Publisher:
        """Return a RabbitMQPublisher bound to the active channel."""

        channel = self._require_channel()
        delegate = RabbitMQPublisher(channel=channel, context=self.context)
        return EnforcingPublisher(delegate=delegate)

    def get_subscriber(self) -> Subscriber:
        """Return a RabbitMQSubscriber bound to the active channel."""

        channel = self._require_channel()
        subscriber = RabbitMQSubscriber(channel=channel, context=self.context)
        self._subscribers.append(subscriber)
        return subscriber

    async def _notify_runtime_event(self, *, event: str, details: dict[str, Any]) -> None:
        """Emit adapter runtime events to optional observers when available."""

        for observer in self._observers:
            callback = getattr(observer, "on_runtime_event", None)
            if callback is None:
                continue
            result = callback(event=event, details=details)
            if isawaitable(result):
                await result

    def _require_channel(self) -> AbstractChannel:
        if self._channel is None:
            raise ConnectionLostError(
                "RabbitMQ broker is not connected.",
                broker="rabbitmq",
                operation="channel_access",
            )
        return self._channel


def rabbitmq_factory(context: BrokerContext) -> Broker:
    """Create a RabbitMQBroker for BrokerRegistry integration."""

    return RabbitMQBroker(context=context)


try:
    BrokerRegistry.register("rabbitmq", rabbitmq_factory)
except ConfigurationError as exc:
    if "already registered" not in str(exc):
        raise
