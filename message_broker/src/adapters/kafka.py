"""Kafka adapter built on aiokafka.

This adapter implements persistent, append-only event streaming. It strictly
enforces Kafka's limitations (like the lack of native delayed messaging) to 
prevent developers from accidentally building unsupported architectures.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict
from typing import Any

try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from aiokafka.errors import KafkaError, KafkaConnectionError
except ImportError:
    raise ImportError(
        "The 'aiokafka' library is required to use the Kafka adapter. "
        "Install it with: pip install aiokafka"
    )

from ..core.context import BrokerContext
from ..core.exceptions import (
    ConfigurationError,
    ConnectionLostError,
    PublishFailedError,
    FeatureNotSupportedError,
)
from ..core.interfaces import (
    Broker,
    BrokerCapability,
    Message,
    MessageHandler,
    Publisher,
    Subscriber,
    EnforcingPublisher,
)
from ..core.registry import BrokerRegistry
from ..core.resilience import with_retries


class KafkaPublisher(Publisher):
    """Publishes messages to Kafka topics using an AIOKafkaProducer."""

    def __init__(self, producer: AIOKafkaProducer, context: BrokerContext) -> None:
        """Initialize the publisher.

        Args:
            producer: A connected AIOKafkaProducer instance.
            context: Normalized broker context.
        """
        self._producer = producer
        self._context = context

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
        """Serialize and append a message to a Kafka topic.

        Args:
            topic: The target Kafka topic.
            message: Broker-neutral message envelope.
            timeout_ms: Optional timeout override.
            deliver_at: Scheduled delivery timestamp (not supported by Kafka).

        Raises:
            FeatureNotSupportedError: If 'deliver_at' scheduling is attempted.
            ConnectionLostError: If Kafka connection drops.
            PublishFailedError: If sending fails.
        """
        
        # Middleware is applied centrally by EnforcingPublisher. Adapter no
        # longer runs `before_publish` to avoid double-invocation and to
        # prevent transport-specific envelopes from leaking into middleware.

        resolved_timeout = timeout_ms or int(self._context.options["timeout"])
        payload = self._context.serializer.serialize(asdict(message)).encode("utf-8")

        try:
            # send_and_wait ensures the message is acknowledged by the broker
            await self._producer.send_and_wait(topic, value=payload)
        except KafkaConnectionError as exc:
            raise ConnectionLostError(
                "Kafka publish failed due to connection interruption.",
                broker="kafka",
                operation="publish",
                details={"topic": topic, "timeout_ms": resolved_timeout},
            ) from exc
        except KafkaError as exc:
            raise PublishFailedError(
                "Kafka publish failed unexpectedly.",
                broker="kafka",
                operation="publish",
                details={"topic": topic, "timeout_ms": resolved_timeout},
            ) from exc


class KafkaSubscriber(Subscriber):
    """Consumes messages from Kafka topics using AIOKafkaConsumer."""

    def __init__(
        self, 
        bootstrap_servers: str, 
        group_id: str, 
        task_registry: set[asyncio.Task[None]],
        consumer_registry: set[AIOKafkaConsumer],
        context: BrokerContext,
    ) -> None:
        """Initialize the subscriber factory.

        Args:
            bootstrap_servers: Kafka connection string (host:port).
            group_id: Consumer group identifier.
            task_registry: Shared set to track background tasks.
            consumer_registry: Shared set to track active consumers for shutdown.
            context: BrokerContext with serializer and middlewares.
        """
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._task_registry = task_registry
        self._consumer_registry = consumer_registry
        self._context = context
        self._queues: list[asyncio.Queue[Message]] = []

    async def subscribe(
        self,
        topic: str,
        handler: MessageHandler,
        /,
        *,
        auto_ack: bool = True,
    ) -> None:
        """Start consumer and worker tasks with backpressure.

        This method spawns:
        1. One _consume task that reads from Kafka and enqueues messages
        2. N _worker tasks that dequeue, apply middleware, and call the handler

        The internal asyncio.Queue with bounded maxsize provides backpressure:
        when workers can't keep up, queue.put() blocks, which pauses Kafka consumption.

        Args:
            topic: The Kafka topic to subscribe to.
            handler: Async callback for received messages.
            auto_ack: Maps to Kafka's enable_auto_commit feature.
        """
        
        try:
            # Extract backpressure settings from context options
            concurrency = self._context.options.get("concurrency", 10)
            max_queue_size = self._context.options.get("max_queue_size", 100)
            
            # Ensure we have valid integers
            if not isinstance(concurrency, int) or concurrency < 1:
                concurrency = 10
            if not isinstance(max_queue_size, int) or max_queue_size < 1:
                max_queue_size = 100
            
            # Create queue with bounded size for backpressure
            queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=max_queue_size)
            self._queues.append(queue)
            
            # Kafka requires a unique consumer instance per subscription/group
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                enable_auto_commit=auto_ack,
            )
            await consumer.start()
            self._consumer_registry.add(consumer)

            # Start the consumer task (reads from Kafka and enqueues)
            consume_task = asyncio.create_task(
                self._consume(consumer, topic, queue),
                name=f"kafka-consume-{topic}"
            )
            self._task_registry.add(consume_task)
            consume_task.add_done_callback(self._task_registry.discard)
            
            # Spawn N worker tasks (dequeue, process, call handler)
            for i in range(concurrency):
                worker_task = asyncio.create_task(
                    self._worker(topic, queue, handler),
                    name=f"kafka-worker-{topic}-{i}"
                )
                self._task_registry.add(worker_task)
                worker_task.add_done_callback(self._task_registry.discard)
            
        except KafkaError as exc:
            raise ConnectionLostError(
                "Failed to connect Kafka consumer.",
                broker="kafka",
                operation="subscribe",
                details={"topic": topic},
            ) from exc

    async def _consume(
        self,
        consumer: AIOKafkaConsumer,
        topic: str,
        queue: asyncio.Queue[Message]
    ) -> None:
        """Read messages from Kafka and enqueue them for workers.

        This task only handles Kafka I/O. It does not apply middleware or call
        the user's handler. By separating concerns, workers can apply backpressure
        to the Kafka consumer when they can't keep up.

        Args:
            consumer: Connected AIOKafkaConsumer instance.
            topic: Source Kafka topic (used for logging).
            queue: Internal queue to put messages into.
        """
        try:
            async for msg in consumer:
                try:
                    # Kafka messages are bytes, decode to string
                    encoded = msg.value.decode("utf-8") if msg.value else ""
                    
                    # Deserialize using context's serializer
                    loaded: dict[str, Any] = self._context.serializer.deserialize(encoded)
                    
                    # Reconstruct Message
                    headers = loaded.get("headers", {})
                    metadata = loaded.get("metadata", {})
                    correlation_id = loaded.get("correlation_id", "")
                    
                    message = Message(
                        payload=loaded.get("payload"),
                        headers=dict(headers) if isinstance(headers, dict) else {},
                        metadata=dict(metadata) if isinstance(metadata, dict) else {},
                        correlation_id=str(correlation_id) if correlation_id else "",
                    )
                    message.metadata.setdefault("source_topic", topic)
                    
                    # Enqueue for workers (this awaits if queue is full - backpressure!)
                    await queue.put(message)
                except Exception:
                    # Errors in deserialization should not kill the consumer loop
                    logging.exception("Unhandled error in Kafka consumer loop for topic: %s", topic)
                    continue
        except asyncio.CancelledError:
            pass
        except KafkaError as exc:
            raise ConnectionLostError(
                "Kafka subscription loop lost connectivity.",
                broker="kafka",
                operation="subscribe",
                details={"topic": topic},
            ) from exc

    async def _worker(
        self,
        topic: str,
        queue: asyncio.Queue[Message],
        handler: MessageHandler
    ) -> None:
        """Worker task that processes messages from the queue.

        Each worker:
        1. Waits for a message from the queue
        2. Applies after_consume middleware hooks
        3. Calls the user's handler
        4. Marks the message as done (for queue.join())

        Workers run concurrently, allowing the consumer task to pull from Kafka while
        workers are still processing previous messages. The bounded queue size
        ensures we don't buffer unlimited messages in memory.

        Args:
            topic: Source topic (used for logging).
            queue: Internal queue to get messages from.
            handler: User's async handler callback.
        """

        while True:
            try:
                message = await queue.get()
                try:
                    # Apply after_consume middleware hooks
                    for middleware in self._context.middlewares:
                        try:
                            message = await middleware.after_consume(topic, message)
                        except Exception:
                            logging.exception(
                                "Error in after_consume middleware for topic: %s", topic
                            )
                            # Continue to next middleware instead of skipping handler
                    
                    # Call the user's handler
                    await handler(message)
                except Exception:
                    # Handler exceptions should not kill the worker
                    logging.exception(
                        "Unhandled error in Kafka worker for topic: %s", topic
                    )
                finally:
                    # Always mark task as done so queue.join() works
                    queue.task_done()
            except asyncio.CancelledError:
                # Gracefully exit when cancelled (e.g., during broker shutdown)
                raise

    async def prepare_shutdown(self, drain_timeout_ms: int | None = None) -> None:
        """Best-effort queue drain before broker task cancellation."""

        timeout_s = (drain_timeout_ms or 0) / 1000.0
        if timeout_s <= 0:
            return

        for queue in self._queues:
            try:
                await asyncio.wait_for(queue.join(), timeout=timeout_s)
            except asyncio.TimeoutError:
                continue


class KafkaBroker(Broker):
    """Concrete broker implementation backed by aiokafka."""

    def __init__(self, context: BrokerContext) -> None:
        """Store context and setup registries for tasks/consumers."""
        self.context = context
        self._bootstrap_servers = f"{self.context.host}:{self.context.port}"
        
        # We need a group_id for consumers. Default to 'message_broker_default' 
        # if the user didn't explicitly pass one in the URI query params or kwargs.
        self._group_id = self.context.options.get("group_id", "message_broker_default")
        
        self._producer: AIOKafkaProducer | None = None
        self._subscriber_tasks: set[asyncio.Task[None]] = set()
        self._active_consumers: set[AIOKafkaConsumer] = set()
        self._subscribers: list[KafkaSubscriber] = []

    @property
    def capabilities(self) -> frozenset[BrokerCapability]:
        """Return the set of features supported by Kafka broker.

        Kafka does not support scheduled/delayed message delivery natively.

        Returns:
            Empty frozenset, as Kafka has no advanced capabilities.
        """
        return frozenset()

    async def connect(self) -> None:
        """Initialize and start the global Kafka producer."""
        try:
            self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
            await self._producer.start()
        except KafkaError as exc:
            raise ConnectionLostError(
                "Failed to connect to Kafka broker.",
                broker="kafka",
                operation="connect",
                details={"bootstrap_servers": self._bootstrap_servers},
            ) from exc

    async def disconnect(self) -> None:
        """Cleanly stop the producer, cancel tasks, and stop all active consumers."""

        drain_timeout_ms = self.context.options.get("shutdown_drain_timeout_ms", 0)
        if isinstance(drain_timeout_ms, int) and drain_timeout_ms > 0:
            for subscriber in list(self._subscribers):
                try:
                    await subscriber.prepare_shutdown(drain_timeout_ms=drain_timeout_ms)
                except Exception:
                    pass
        
        # 1. Cancel background subscriber loops
        tasks = list(self._subscriber_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._subscriber_tasks.clear()
        self._subscribers.clear()

        # 2. Stop all Kafka consumers safely so they can commit final offsets
        for consumer in self._active_consumers:
            try:
                await consumer.stop()
            except KafkaError:
                pass
        self._active_consumers.clear()

        # 3. Stop the global producer
        if self._producer is not None:
            try:
                await self._producer.stop()
            except KafkaError as exc:
                raise ConnectionLostError(
                    "Failed while closing Kafka producer connection.",
                    broker="kafka",
                    operation="disconnect",
                ) from exc
            finally:
                self._producer = None

    def get_publisher(self) -> Publisher:
        """Return a KafkaPublisher bound to the active producer."""
        if self._producer is None:
            raise ConnectionLostError(
                "Kafka broker is not connected.", broker="kafka", operation="client_access"
            )
        delegate = KafkaPublisher(producer=self._producer, context=self.context)
        return EnforcingPublisher(broker=self, delegate=delegate)

    def get_subscriber(self) -> Subscriber:
        """Return a KafkaSubscriber ready to spawn consumer groups."""
        subscriber = KafkaSubscriber(
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            task_registry=self._subscriber_tasks,
            consumer_registry=self._active_consumers,
            context=self.context
        )
        self._subscribers.append(subscriber)
        return subscriber


def kafka_factory(context: BrokerContext) -> Broker:
    """Create a KafkaBroker for BrokerRegistry integration."""
    return KafkaBroker(context=context)


try:
    BrokerRegistry.register("kafka", kafka_factory)
except ConfigurationError as exc:
    if "already registered" not in str(exc):
        raise