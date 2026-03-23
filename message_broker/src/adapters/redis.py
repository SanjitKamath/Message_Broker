"""Redis adapter built on redis.asyncio queue primitives.

This adapter uses Redis lists as simple FIFO queues. It intentionally avoids
higher-level frameworks so broker behavior stays explicit and transport-neutral.
It also supports message scheduling using Redis Sorted Sets (ZSET).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import asdict
from inspect import isawaitable
from typing import Any
from uuid import uuid4

from redis import asyncio as redis
from redis.exceptions import RedisError

from ..app_logging import get_logger
from ..core.context import BrokerContext
from ..core.exceptions import ConfigurationError
from ..core.exceptions import ConnectionLostError, PublishFailedError
from ..core.interfaces import (
    Broker,
    Message,
    MessageHandler,
    Publisher,
    Subscriber,
    ScheduledEnvelope,
    EnforcingPublisher,
)
from ..core.registry import BrokerRegistry
from ..core.resilience import with_retries


class RedisPublisher(Publisher):
    """Publishes messages to Redis lists using LPUSH and ZADD for scheduling."""

    def __init__(self, client: redis.Redis, context: BrokerContext) -> None:
        """Initialize publisher state.

        Args:
            client: Connected redis.asyncio client.
            context: Normalized broker context for defaults.
        """

        self._client = client
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
        """Serialize and enqueue a message into a Redis topic list.

        If `deliver_at` is provided, schedules the message in a global ZSET
        instead of pushing it immediately. The scheduled message will be moved
        to the target topic list when its delivery time arrives.

        Args:
            topic: Redis list key used as the queue topic.
            message: Broker-neutral message envelope.
            timeout_ms: Optional timeout override for queue operation.
            deliver_at: Optional POSIX timestamp for scheduled delivery.

        Raises:
            ConnectionLostError: If Redis connection fails unexpectedly.
            PublishFailedError: If serialization or enqueue fails.
        """

        # Middleware is applied centrally by EnforcingPublisher. Adapter no
        # longer runs `before_publish` to avoid double-invocation and to
        # prevent transport-specific envelopes from leaking into middleware.
        
        resolved_timeout = timeout_ms or int(self._context.options["timeout"])
        resolved_topic = topic
        effective_deliver_at = deliver_at
        
        # Prepare payloads. For scheduled messages we persist routing and payload
        # in dedicated Redis structures and place only a token in the ZSET.
        if isinstance(message, ScheduledEnvelope):
            scheduled = message
            resolved_topic = scheduled.target
            effective_deliver_at = float(scheduled.deliver_at)
            payload = self._context.serializer.serialize(asdict(scheduled.message))
        elif hasattr(message, "message") and hasattr(message, "target"):
            # Defensive: support objects with message/target attributes
            resolved_topic = str(getattr(message, "target"))
            effective_deliver_at = float(getattr(message, "deliver_at"))
            payload = self._context.serializer.serialize(asdict(getattr(message, "message")))
        else:
            payload = self._context.serializer.serialize(asdict(message))

        try:
            if effective_deliver_at is not None:
                # Persist delayed messages transactionally so scheduler can atomically
                # move due entries without deserializing transport payloads in Lua.
                score = float(effective_deliver_at)
                token = uuid4().hex
                async with self._client.pipeline(transaction=True) as pipeline:
                    pipeline.hset(RedisBroker._SCHEDULED_PAYLOADS_KEY, token, payload)
                    pipeline.hset(RedisBroker._SCHEDULED_TARGETS_KEY, token, resolved_topic)
                    pipeline.zadd(RedisBroker._SCHEDULED_ZSET_KEY, {token: score})
                    await pipeline.execute()
            else:
                # Otherwise, standard immediate delivery
                await self._client.lpush(resolved_topic, payload)
        except RedisError as exc:
            raise ConnectionLostError(
                "Redis publish failed due to connection interruption.",
                broker="redis",
                operation="publish",
                details={"topic": resolved_topic, "timeout_ms": resolved_timeout},
            ) from exc
        except Exception as exc:
            raise PublishFailedError(
                "Redis publish failed unexpectedly.",
                broker="redis",
                operation="publish",
                details={"topic": resolved_topic, "timeout_ms": resolved_timeout},
            ) from exc


class RedisSubscriber(Subscriber):
    """Consumes messages from Redis lists using BRPOP in background tasks."""

    def __init__(
        self, 
        client: redis.Redis, 
        task_registry: set[asyncio.Task[None]],
        context: BrokerContext,
    ) -> None:
        """Initialize subscriber with task tracking and context for serialization.

        Args:
            client: Connected redis.asyncio client.
            task_registry: Shared task set for broker-managed cancellation.
            context: BrokerContext with serializer and middlewares.
        """

        self._client = client
        self._task_registry = task_registry
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
        """Start background consumer and worker tasks with backpressure.

        Redis list operations remove items atomically at pop time, so the
        auto_ack flag is accepted for interface compatibility but does not alter
        Redis behavior.

        This method spawns:
        1. One _consume task that pulls from Redis and enqueues messages
        2. N _worker tasks that dequeue, apply middleware, and call the handler

        The internal asyncio.Queue with bounded maxsize provides backpressure:
        when workers can't keep up, queue.put() blocks, which pauses Redis pulls.

        Args:
            topic: Redis list key used as the queue topic.
            handler: Async callback for each decoded message.
            auto_ack: Compatibility flag; ignored for Redis lists.

        Raises:
            ConnectionLostError: If task creation fails due to runtime issues.
        """

        _ = auto_ack
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
            
            # Start the consumer task (pulls from Redis and enqueues)
            consume_task = asyncio.create_task(
                self._consume(topic, queue),
                name=f"redis-consume-{topic}"
            )
            self._task_registry.add(consume_task)
            consume_task.add_done_callback(self._task_registry.discard)
            
            # Spawn N worker tasks (dequeue, process, call handler)
            for i in range(concurrency):
                worker_task = asyncio.create_task(
                    self._worker(topic, queue, handler),
                    name=f"redis-worker-{topic}-{i}"
                )
                self._task_registry.add(worker_task)
                worker_task.add_done_callback(self._task_registry.discard)
        except Exception as exc:
            raise ConnectionLostError(
                "Failed to start Redis subscriber tasks.",
                broker="redis",
                operation="subscribe",
                details={"topic": topic},
            ) from exc

    async def _consume(
        self,
        topic: str,
        queue: asyncio.Queue[Message],
    ) -> None:
        """Pull messages from Redis and enqueue them for workers.

        This task only handles Redis I/O. It does not apply middleware or call
        the user's handler. By separating concerns, workers can apply backpressure
        to the Redis puller when they can't keep up.

        Args:
            topic: Redis list key to pull from.
            queue: Internal queue to put messages into.
        """

        while True:
            try:
                item = await self._client.brpop(topic, timeout=1)
                if item is None:
                    continue

                _, raw_body = item
                serialized_payload: str | bytes
                if isinstance(raw_body, (bytes, str)):
                    serialized_payload = raw_body
                else:
                    serialized_payload = str(raw_body)

                # Deserialize using context's serializer
                loaded = self._context.serializer.deserialize(serialized_payload)
                
                # Reconstruct Message
                message = Message(
                    payload=loaded.get("payload"),
                    headers=dict(loaded.get("headers", {})) if isinstance(loaded.get("headers"), dict) else {},
                    metadata=dict(loaded.get("metadata", {})) if isinstance(loaded.get("metadata"), dict) else {},
                    correlation_id=str(loaded.get("correlation_id", "")),
                )
                message.metadata.setdefault("source_topic", topic)
                
                # Enqueue for workers (this awaits if queue is full - backpressure!)
                await queue.put(message)
            except asyncio.CancelledError:
                raise
            except RedisError as exc:
                raise ConnectionLostError(
                    "Redis subscription loop lost connectivity.",
                    broker="redis",
                    operation="subscribe",
                    details={"topic": topic},
                ) from exc
            except Exception:
                # Errors in deserialization should not kill the consumer loop
                logging.exception("Unhandled error in Redis consumer loop for topic: %s", topic)
                continue

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

        Workers run concurrently, allowing the consumer task to pull while
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
                    # Handler exceptions should not kill the worker thread
                    logging.exception(
                        "Unhandled error in Redis worker for topic: %s", topic
                    )
                finally:
                    # Always mark task as done so queue.join() works
                    queue.task_done()
            except asyncio.CancelledError:
                # Gracefully exit when cancelled (e.g., during broker shutdown)
                raise

    async def prepare_shutdown(self, drain_timeout_ms: int | None = None) -> None:
        """Request workers to finish queued messages before forced cancellation."""

        timeout_s = (drain_timeout_ms or 0) / 1000.0
        if timeout_s <= 0:
            return

        for queue in self._queues:
            try:
                await asyncio.wait_for(queue.join(), timeout=timeout_s)
            except asyncio.TimeoutError:
                continue

    @staticmethod
    def _deserialize(payload: str) -> Message:
        """Deserialize JSON payload into Message dataclass."""

        loaded: dict[str, Any] = json.loads(payload)
        headers = loaded.get("headers")
        metadata = loaded.get("metadata")

        normalized_headers = dict(headers) if isinstance(headers, dict) else {}
        normalized_metadata = dict(metadata) if isinstance(metadata, dict) else {}

        correlation_id = loaded.get("correlation_id")
        correlation = str(correlation_id) if correlation_id is not None else ""

        return Message(
            payload=loaded.get("payload"),
            headers=normalized_headers,
            metadata=normalized_metadata,
            correlation_id=correlation,
        )


class RedisBroker(Broker):
    """Concrete broker implementation backed by redis.asyncio."""

    _SCHEDULED_ZSET_KEY = "broker:scheduled_messages"
    _SCHEDULED_PAYLOADS_KEY = "broker:scheduled:payloads"
    _SCHEDULED_TARGETS_KEY = "broker:scheduled:targets"
    _SCHEDULER_LOCK_KEY = "broker:scheduler:lock"
    _PROCESSED_KEY_PREFIX = "broker:processed:"

    def __init__(self, context: BrokerContext) -> None:
        """Store context and defer I/O until connect is called.

        Args:
            context: Parsed Redis connection context.
        """

        self.context = context
        self._log = get_logger(self.__class__.__name__)
        self._client: redis.Redis | None = None
        self._subscriber_tasks: set[asyncio.Task[None]] = set()
        self._scheduler_task: asyncio.Task[None] | None = None
        self._subscribers: list[RedisSubscriber] = []
        self._instance_id = uuid4().hex
        self._scheduler_lock_held = False
        self._scheduler_lock_ttl_ms = int(self.context.options.get("scheduler_lock_ttl_ms", 5000))
        self._scheduler_batch_size = int(self.context.options.get("scheduler_batch_size", 100))
        self._idempotency_ttl_sec = int(self.context.options.get("idempotency_ttl_sec", 86400))
        observers = self.context.options.get("observers", [])
        self._observers = list(observers) if isinstance(observers, list) else []

        # Compares lock owner and extends lock TTL atomically.
        self._renew_lock_lua = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('PEXPIRE', KEYS[1], ARGV[2])
        end
        return 0
        """

        # Deletes lock only when this process owns it.
        self._release_lock_lua = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
        """

        # Atomically moves due scheduled messages from ZSET to target lists.
        self._move_due_messages_lua = """
        local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
        local moved = 0
        for _, member in ipairs(due) do
            local payload = redis.call('HGET', KEYS[2], member)
            local target = redis.call('HGET', KEYS[3], member)

            if payload and target then
                redis.call('LPUSH', target, payload)
                redis.call('ZREM', KEYS[1], member)
                redis.call('HDEL', KEYS[2], member)
                redis.call('HDEL', KEYS[3], member)
                moved = moved + 1
            else
                local ok, decoded = pcall(cjson.decode, member)
                if ok and type(decoded) == 'table' then
                    if decoded['__scheduled__'] and decoded['target'] and decoded['payload'] then
                        redis.call('LPUSH', decoded['target'], cjson.encode(decoded['payload']))
                        redis.call('ZREM', KEYS[1], member)
                        moved = moved + 1
                    elseif decoded['metadata'] and decoded['metadata']['_target_topic'] then
                        redis.call('LPUSH', decoded['metadata']['_target_topic'], member)
                        redis.call('ZREM', KEYS[1], member)
                        moved = moved + 1
                    end
                end
            end
        end
        return moved
        """

    async def connect(self) -> None:
        """Initialize Redis connection pool, validate connectivity, and start scheduler."""

        try:
            self._client = redis.from_url(self.context.connection_uri, decode_responses=False)
            await self._client.ping()
            
            # Start the background scheduler loop for ZSET delayed messages
            self._scheduler_task = asyncio.create_task(
                self._run_scheduler_loop(), name="redis-scheduler"
            )
        except RedisError as exc:
            raise ConnectionLostError(
                "Failed to connect to Redis broker.",
                broker="redis",
                operation="connect",
                details={"uri": self.context.connection_uri},
            ) from exc

    async def disconnect(self) -> None:
        """Cancel subscriber loops, the scheduler task, and close the Redis client pool cleanly."""

        drain_timeout_ms = self.context.options.get("shutdown_drain_timeout_ms", 0)
        if isinstance(drain_timeout_ms, int) and drain_timeout_ms > 0:
            for subscriber in list(self._subscribers):
                try:
                    await subscriber.prepare_shutdown(drain_timeout_ms=drain_timeout_ms)
                except Exception:
                    pass

        # 1. Cancel the scheduler
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
            finally:
                self._scheduler_task = None

        # 2. Cancel subscribers
        tasks = list(self._subscriber_tasks)
        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._subscriber_tasks.clear()
        self._subscribers.clear()

        # 3. Close client connection
        if self._client is not None:
            try:
                await self._client.aclose()
            except RedisError as exc:
                raise ConnectionLostError(
                    "Failed while closing Redis broker connection.",
                    broker="redis",
                    operation="disconnect",
                ) from exc
            finally:
                self._client = None

    async def _acquire_scheduler_lock(self) -> bool:
        """Try to become the active scheduler owner using a distributed Redis lock."""

        client = self._require_client()
        acquired = await client.set(
            self._SCHEDULER_LOCK_KEY,
            self._instance_id,
            nx=True,
            px=self._scheduler_lock_ttl_ms,
        )
        return bool(acquired)

    async def _renew_scheduler_lock(self) -> bool:
        """Extend scheduler lock TTL only when this instance still owns the lock."""

        client = self._require_client()
        renewed = await client.eval(
            self._renew_lock_lua,
            1,
            self._SCHEDULER_LOCK_KEY,
            self._instance_id,
            str(self._scheduler_lock_ttl_ms),
        )
        return bool(int(renewed) if renewed is not None else 0)

    async def _release_scheduler_lock(self) -> None:
        """Release scheduler lock safely, only when owned by this instance."""

        try:
            client = self._require_client()
            await client.eval(
                self._release_lock_lua,
                1,
                self._SCHEDULER_LOCK_KEY,
                self._instance_id,
            )
        except Exception:
            # Shutdown should remain best-effort.
            return

    async def _move_due_scheduled_messages(self) -> int:
        """Atomically move due delayed messages from ZSET into destination lists."""

        client = self._require_client()
        moved = await client.eval(
            self._move_due_messages_lua,
            3,
            self._SCHEDULED_ZSET_KEY,
            self._SCHEDULED_PAYLOADS_KEY,
            self._SCHEDULED_TARGETS_KEY,
            str(time.time()),
            str(self._scheduler_batch_size),
        )
        return int(moved) if moved is not None else 0

    async def try_acquire_idempotency(self, message_id: str) -> bool:
        """Reserve processing slot for a message ID using SET NX with TTL."""

        client = self._require_client()
        key = f"{self._PROCESSED_KEY_PREFIX}{message_id}"
        acquired = await client.set(key, "processing", nx=True, ex=self._idempotency_ttl_sec)
        return bool(acquired)

    async def mark_idempotency_completed(self, message_id: str) -> None:
        """Mark a message ID as completed while preserving expiration semantics."""

        client = self._require_client()
        key = f"{self._PROCESSED_KEY_PREFIX}{message_id}"
        await client.set(key, "completed", ex=self._idempotency_ttl_sec)

    async def clear_idempotency_processing(self, message_id: str) -> None:
        """Clear idempotency marker when processing fails and should be retriable."""

        client = self._require_client()
        key = f"{self._PROCESSED_KEY_PREFIX}{message_id}"
        await client.delete(key)

    async def _notify_runtime_event(self, *, event: str, details: dict[str, Any]) -> None:
        """Emit adapter runtime events to optional observers when available."""

        for observer in self._observers:
            callback = getattr(observer, "on_runtime_event", None)
            if callback is None:
                continue
            result = callback(event=event, details=details)
            if isawaitable(result):
                await result

    async def _run_scheduler_loop(self) -> None:
        """Continuously move due delayed messages while this instance owns the lock."""

        lock_wait_s = 0.5
        idle_wait_s = 0.25
        busy_wait_s = 0.1
        renew_interval_s = max(self._scheduler_lock_ttl_ms / 1000.0 / 3.0, 0.5)
        last_renewal = 0.0

        while self._client is None:
            await asyncio.sleep(0.1)

        try:
            while True:
                try:
                    if not self._scheduler_lock_held:
                        acquired = await self._acquire_scheduler_lock()
                        if not acquired:
                            await asyncio.sleep(lock_wait_s)
                            continue

                        self._scheduler_lock_held = True
                        last_renewal = time.monotonic()
                        self._log.info("Scheduler lock acquired", instance_id=self._instance_id)
                        await self._notify_runtime_event(
                            event="scheduler_lock_acquired",
                            details={"instance_id": self._instance_id},
                        )

                    now_monotonic = time.monotonic()
                    if now_monotonic - last_renewal >= renew_interval_s:
                        renewed = await self._renew_scheduler_lock()
                        if not renewed:
                            self._scheduler_lock_held = False
                            self._log.warning("Scheduler lock lost", instance_id=self._instance_id)
                            await self._notify_runtime_event(
                                event="scheduler_lock_lost",
                                details={"instance_id": self._instance_id},
                            )
                            await asyncio.sleep(lock_wait_s)
                            continue
                        last_renewal = now_monotonic

                    moved = await self._move_due_scheduled_messages()
                    if moved > 0:
                        self._log.info("Scheduled batch moved", moved_count=moved)
                        await self._notify_runtime_event(
                            event="scheduler_batch_moved",
                            details={"moved_count": moved, "instance_id": self._instance_id},
                        )
                        await asyncio.sleep(busy_wait_s)
                    else:
                        await asyncio.sleep(idle_wait_s)
                except asyncio.CancelledError:
                    raise
                except RedisError:
                    if self._scheduler_lock_held:
                        self._scheduler_lock_held = False
                        await self._notify_runtime_event(
                            event="scheduler_lock_lost",
                            details={"instance_id": self._instance_id, "reason": "redis_error"},
                        )
                    await asyncio.sleep(1.0)
                except Exception:
                    await asyncio.sleep(1.0)
        finally:
            if self._scheduler_lock_held:
                await self._release_scheduler_lock()
                self._scheduler_lock_held = False
                await self._notify_runtime_event(
                    event="scheduler_lock_released",
                    details={"instance_id": self._instance_id},
                )

    def get_publisher(self) -> Publisher:
        """Return a RedisPublisher bound to the active connection."""

        client = self._require_client()
        delegate = RedisPublisher(client=client, context=self.context)
        return EnforcingPublisher(delegate=delegate)

    def get_subscriber(self) -> Subscriber:
        """Return a RedisSubscriber bound to the active connection."""

        client = self._require_client()
        subscriber = RedisSubscriber(
            client=client,
            task_registry=self._subscriber_tasks,
            context=self.context,
        )
        self._subscribers.append(subscriber)
        return subscriber

    def _require_client(self) -> redis.Redis:
        """Ensure the broker is connected before creating primitives."""

        if self._client is None:
            raise ConnectionLostError(
                "Redis broker is not connected.",
                broker="redis",
                operation="client_access",
            )
        return self._client


def redis_factory(context: BrokerContext) -> Broker:
    """Create a RedisBroker for BrokerRegistry integration."""

    return RedisBroker(context=context)


try:
    BrokerRegistry.register("redis", redis_factory)
except ConfigurationError as exc:
    if "already registered" not in str(exc):
        raise