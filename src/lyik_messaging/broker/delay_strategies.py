"""Transport-specific delayed delivery strategies."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Mapping
from typing import Protocol, cast

from ..exceptions import ConfigurationError

REDIS_DELAYED_ZSET_KEY = "lyik:delayed"
REDIS_DELAY_POLL_INTERVAL_SECONDS = 0.2


class DelayStrategy(Protocol):
    """
    This class defines the interface for delayed delivery strategies. 
    Each transport that supports delayed delivery should implement this interface, 
    allowing the broker to use it for scheduling messages with a delay.

    publish_with_delay: Schedules a message to be published after a specified delay 
    in milliseconds.
    """
    async def publish_with_delay(
        self,
        broker: object,
        topic: str,
        payload: object,
        delay_ms: int,
        headers: Mapping[str, object],
        correlation_id: str,
        reply_to: str | None,
    ) -> None: ...


class _RedisDelayedClient(Protocol):
    """
    This is a protocol that defines the expected interface for a Redis client that supports
    the necessary sorted set operations for implementing delayed delivery.

    zadd: Adds one or more members to a sorted set, or updates the score of an existing member.
    zrangebyscore: Returns all the members in a sorted set with scores within the given range.
    zrem: Removes one or more members from a sorted set.
    """
    async def zadd(self, name: str, mapping: dict[object, float | int]) -> int: ...

    async def zrangebyscore(
        self,
        name: str,
        min: float | int | str,
        max: float | int | str,
        start: int | None = None,
        num: int | None = None,
    ) -> list[object]: ...

    async def zrem(self, name: str, *values: object) -> int: ...


class _BrokerPublisher(Protocol):
    """
    This protocol defines the expected interface for a broker publisher that can be used by
    delay strategies to publish messages.
    
    publish: Publishes a message to the broker with the given payload, queue/topic, 
    correlation ID, reply-to address, and headers.
    """
    async def publish(self, *args: object, **kwargs: object) -> object: ...


class RabbitMQDelayStrategy:
    """
    This delay strategy uses RabbitMQ's delayed-message-exchange plugin to schedule 
    messages with a delay.

    publish_with_delay: Schedules a message to be published after a specified delay in
    milliseconds by adding the "x-delay" header to the message. 
    This requires that the RabbitMQ server has the delayed-message-exchange plugin 
    installed and properly configured.
    """
    async def publish_with_delay(
        self,
        broker: object,
        topic: str,
        payload: object,
        delay_ms: int,
        headers: Mapping[str, object],
        correlation_id: str,
        reply_to: str | None,
    ) -> None:
        delayed_headers = dict(headers)
        # Requires RabbitMQ delayed-message-exchange plugin and delayed exchange binding.
        delayed_headers["x-delay"] = delay_ms
        publisher = cast("_BrokerPublisher", broker)
        await publisher.publish(
            payload,
            queue=topic,
            correlation_id=correlation_id,
            reply_to=reply_to,
            headers=delayed_headers,
        )


class RedisDelayStrategy:
    """
    This delay strategy uses Redis sorted sets to implement delayed delivery.
    publish_with_delay: Schedules a message to be published after a specified delay in
    milliseconds by adding it to a Redis sorted set with the execution timestamp as the score.
    A separate polling mechanism (not implemented here) would be responsible for checking the
    sorted set and publishing messages when their scheduled time has arrived.
    """
    async def publish_with_delay(
        self,
        broker: object,
        topic: str,
        payload: object,
        delay_ms: int,
        headers: Mapping[str, object],
        correlation_id: str,
        reply_to: str | None,
    ) -> None:
        redis = resolve_redis_delayed_client(broker)
        if redis is None:
            raise ConfigurationError(
                "Redis delayed delivery requires an active Redis connection."
            )

        execute_at_ms = _utc_now_ms() + max(0, delay_ms)
        envelope = {
            "delayed_id": uuid.uuid4().hex,
            "topic": topic,
            "payload": payload,
            "headers": dict(headers),
            "correlation_id": correlation_id,
            "reply_to": reply_to,
        }
        serialized = json.dumps(envelope, separators=(",", ":"))
        await redis.zadd(REDIS_DELAYED_ZSET_KEY, {serialized: execute_at_ms})


def resolve_redis_delayed_client(broker: object) -> _RedisDelayedClient | None:
    """
    Attempts to resolve a Redis client from the broker instance by checking common attributes
    where a Redis client might be stored. This is necessary because different broker 
    implementations may store their Redis client in different ways. 
    The function checks for attributes like "_connection", "connection", "_redis", "redis",
    "_client", and "client" and verifies if any of them support the required sorted set 
    operations for delayed delivery.
    """
    candidates = (
        getattr(broker, "_connection", None),
        getattr(broker, "connection", None),
        getattr(broker, "_redis", None),
        getattr(broker, "redis", None),
        getattr(broker, "_client", None),
        getattr(broker, "client", None),
    )
    for candidate in candidates:
        if _is_redis_zset_client(candidate):
            return cast("_RedisDelayedClient", candidate)
        nested = getattr(candidate, "_connection", None) if candidate is not None else None
        if _is_redis_zset_client(nested):
            return cast("_RedisDelayedClient", nested)
    return None


def _is_redis_zset_client(client: object) -> bool:
    """
    Returns True if the given client object supports the necessary Redis sorted set operations
    (zadd, zrangebyscore, zrem) for implementing delayed delivery. This is a heuristic 
    check to determine if the client can be used for the RedisDelayStrategy.
    """
    return (
        client is not None
        and hasattr(client, "zadd")
        and hasattr(client, "zrangebyscore")
        and hasattr(client, "zrem")
    )


def _utc_now_ms() -> int:
    #  Returns the current UTC time in milliseconds
    return int(time.time() * 1000)
