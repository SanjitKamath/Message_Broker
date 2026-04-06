"""Connection lifecycle primitives for `lyik_messaging.broker`."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, cast

from faststream.rabbit import RabbitBroker
from faststream.redis import RedisBroker

from ..encryption import build_aesgcm_middleware_factory
from ..exceptions import ConfigurationError, ConnectionLostError
from ..utils import resolve_broker_scheme, set_current_broker
from .delay_strategies import (
    REDIS_DELAY_POLL_INTERVAL_SECONDS,
    REDIS_DELAYED_ZSET_KEY,
    resolve_redis_delayed_client,
)

if TYPE_CHECKING:
    from . import MessageBroker
    from .internal_types import FastStreamBroker

logger = logging.getLogger(__name__)
_DELAYED_PROCESSED_TTL_SECONDS = 600.0


class BrokerCore:
    """
    Owns broker connectivity and runtime lifecycle.
    
    This class encapsulates the core logic for managing the connection lifecycle of the underlying message broker,
    including connecting, starting the run loop, and disconnecting. It also handles the creation of the appropriate 
    broker instance based on the URI scheme and manages middleware configuration.

    The BrokerCore is designed to be used internally by the MessageBroker class, providing a clean separation of concerns
    between the high-level broker interface and the low-level connection management logic. It ensures that the
    MessageBroker can focus on providing the public API for message handling and interaction, while the BrokerCore
    takes care of the complexities of connecting to the broker, managing the run loop, and handling
    disconnections gracefully.

    Attributes:
        `_owner`: The MessageBroker instance that owns this BrokerCore, allowing it to access and modify the broker state and 
                    configuration as needed.
        `_broker`: The currently connected broker instance, or None if not connected.
        `_scheme`: The URI scheme of the connected broker, used for determining the broker type and middleware configuration.
        `_context_options`: A dictionary of context options for configuring the broker connection and middleware behavior.
        `_run_task`: The asyncio Task that is running the broker's run loop, used for managing the lifecycle and handling 
                        cancellations gracefully.
    
    Methods:
        `connect()`: Connects to the broker based on the provided URI and configuration, setting up the necessary middlewares and validating the configuration.
        `start()`: Starts the broker's run loop, allowing it to process messages and handle interactions. This method will run indefinitely until cancelled.
        `run()`: A convenience method that combines `connect()` and `start()` for ease of use in common scenarios where you want to connect and start the broker in one step.
        `disconnect()`: Disconnects from the broker gracefully, ensuring that any necessary cleanup is performed and the broker state is reset appropriately.
        `require_broker()`: A helper method to retrieve the currently connected broker instance, raising a ConfigurationError if the broker is not connected. This is used internally to ensure that broker operations are only performed when a connection is established.
        `_build_middlewares()`: A helper method to build the list of middlewares based on the current configuration, such as adding encryption middleware if an AES key is provided.
        `_create_broker()`: A helper method to create the appropriate broker instance based on the URI scheme, ensuring that the correct broker type is instantiated with the configured
                            middlewares.
           
    """

    def __init__(self, broker_owner: "MessageBroker") -> None:
        self._owner = broker_owner
        self._redis_delayed_worker_task: asyncio.Task[None] | None = None
        self._processed_delayed_ids: dict[str, float] = {}

    async def connect(self) -> None:
        """
        Connect to the broker specified by the owner's URI and configuration.
        This method resolves the broker type based on the URI scheme, builds the necessary middlewares, 
        and establishes the connection to the broker. It also handles any connection errors gracefully, 
        ensuring that the broker state is reset if the connection fails.

        Raises:
            `ConfigurationError`: If the broker URI scheme is unsupported or if the configuration is invalid.
            `ConnectionLostError`: If there is an error during the connection process to the broker.

        Example:
            ```
            broker_core = BrokerCore(message_broker_instance)
            await broker_core.connect()
            ```
        """
        owner = self._owner

        # If the broker is already connected, set it as the current broker and return early to avoid redundant connections.
        if owner._broker is not None:
            set_current_broker(owner)
            return
        
        # Validate the request/reply queue configuration before attempting to connect to ensure that the broker is properly 
        # set up for handling messages.
        owner._validate_request_reply_queues()

        # Resolve the broker scheme from the URI and build the necessary middlewares based on the configuration, 
        # such as encryption middleware if an AES key is provided.
        scheme = resolve_broker_scheme(owner._uri)

        # Build the list of middlewares based on the current configuration, ensuring that any necessary middlewares 
        # are included for the broker connection.
        middlewares = self._build_middlewares()

        # Create the appropriate broker instance based on the resolved scheme and configured middlewares, 
        # ensuring that the correct broker type is instantiated.
        broker = self._create_broker(scheme, middlewares)

        # Attach the broker's message handler registrations to the consumer to ensure that incoming messages are 
        # properly routed to the registered handlers.
        owner._consumer.attach_registrations(broker)

        # Attempt to connect to the broker and handle any connection errors gracefully, 
        # ensuring that the broker state is reset if the connection fails.
        try:
            await broker.connect()
        except Exception as exc:
            owner._broker = None
            raise ConnectionLostError("Failed to connect the underlying broker.") from exc

        # If connection is successful, set the broker and scheme on the owner and set it as the current broker for the context.
        owner._scheme = scheme

        # Set the connected broker on the owner and update the current broker context to ensure that subsequent operations 
        # use the correct broker instance.
        owner._broker = broker

        # Set the current broker context to the owner to ensure that any operations that rely on the current broker context 
        # will use this newly connected broker.
        set_current_broker(owner)

    async def start(self) -> None:
        """
        Start the broker's run loop to begin processing messages and handling interactions. 
        This method will run indefinitely until cancelled, allowing the broker to continuously operate and handle 
        incoming messages.

        Raises:
            `ConnectionLostError`: If there is an error during the startup process of the broker.
            
        Example:
            ```
            await broker_core.start()
            ```
        """
        # Retrieve the owner and the currently connected broker instance, ensuring that the broker is connected 
        # before attempting to start.
        owner = self._owner

        # Ensure that the broker is connected before starting the run loop, raising a ConfigurationError if not
        # connected to provide clear feedback to the user.
        broker = self.require_broker()

        # Start the broker's run loop and handle any errors gracefully, ensuring that the broker is properly 
        # stopped and the state is reset if there are issues during startup.
        await broker.start()

        # Store the current asyncio Task that is running the broker's run loop to allow for proper management 
        # of the lifecycle and handling of cancellations gracefully.
        owner._run_task = asyncio.current_task()
        self._start_redis_delayed_worker_if_needed()

        # Run indefinitely until cancelled, allowing the broker to continuously operate and handle incoming messages.
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise
        finally:
            await self.disconnect()

    async def run(self) -> None:
        """
        A convenience method that combines `connect()` and `start()` for ease of use in common scenarios where you want 
        to connect and start the broker in one step.
        This method will first establish the connection to the broker and then start the run loop, allowing for a
        streamlined workflow for getting the broker up and running.

        Raises:
            `ConfigurationError`: If the broker URI scheme is unsupported or if the configuration is invalid.
            `ConnectionLostError`: If there is an error during the connection or startup process of the broker.
        Example:
            ```
            await broker_core.run()
            ``` 
        """
        await self.connect()
        await self.start()

    async def disconnect(self) -> None:
        """
        This method handles the disconnection process from the broker gracefully, ensuring that any necessary cleanup 
        is performed and the broker state is reset appropriately. It attempts to stop the broker's run loop and disconnect 
        from the broker, handling any errors that may occur during disconnection and ensuring that the broker state is 
        reset to reflect that it is no longer connected.

        Raises:
            `ConnectionLostError`: If there is an error during the disconnection process from the broker.
        
        Example:
            ```
            await broker_core.disconnect()
            ```
        """
        owner = self._owner
        broker = owner._broker
        if broker is None:
            set_current_broker(None)
            return
        await self._stop_redis_delayed_worker()
        # Attempt to stop the broker and handle any errors gracefully, ensuring that the broker state is reset even 
        # if there are issues during disconnection.
        try:
            await broker.stop()
        except Exception as exc:
            raise ConnectionLostError("Failed to stop the underlying broker.") from exc
        finally:
            owner._broker = None
            owner._scheme = None
            set_current_broker(None)

    def require_broker(self) -> "FastStreamBroker":
        """
        A helper method to retrieve the currently connected broker instance, raising a ConfigurationError 
        if the broker is not connected. This is used internally to ensure that broker operations are only 
        performed when a connection is established, providing a clear error message if the broker is not 
        connected to guide the user towards proper usage.

        Returns:
            The currently connected broker instance.
        
        Raises:
            `ConfigurationError`: If the broker is not connected, with a message guiding the user to connect first.
        """

        owner = self._owner
        broker = owner._broker
        if broker is None:
            raise ConfigurationError(
                "Broker is not connected. Call connect() first "
                "(or use run(), async with MessageBroker(...), or connect(..., auto_start=True))."
            )
        return broker

    def _build_middlewares(self) -> Sequence[Callable[[object | None], object]]:
        """
        A helper method to build the list of middlewares based on the current configuration, 
        such as adding encryption middleware if an AES key is provided.
        This method checks the owner's configuration for an AES key and builds the appropriate middleware factory
        if encryption is configured, allowing for dynamic middleware configuration based on the broker settings.

        Returns:
            A sequence of middleware factory callables to be applied to the broker connection.
        """
        aes_key = self._owner._aes_key
        if aes_key is None:
            return ()
        return (build_aesgcm_middleware_factory(aes_key),)

    def _create_broker(
        self,
        scheme: str,
        middlewares: Sequence[Callable[[object | None], object]],
    ) -> "FastStreamBroker":
        """
        A helper method to create the appropriate broker instance based on the URI scheme, ensuring that the 
        correct broker type is instantiated with the configured middlewares.

        Args:
            `scheme`: The URI scheme of the broker (e.g., "amqp", "redis") used to determine the broker type.
            `middlewares`: A sequence of middleware factory callables to be applied to the broker connection.
        
        Returns:
            An instance of the appropriate broker type based on the URI scheme, configured with the provided middlewares.

        Raises:
            `ConfigurationError`: If the URI scheme is not supported.
        """
        owner = self._owner
        # Create the appropriate broker instance based on the resolved scheme and configured middlewares, 
        # ensuring that the correct broker type is instantiated. If the scheme is not supported, raise a ConfigurationError
        if scheme in {"amqp", "amqps"}:
            return cast(
                "FastStreamBroker",
                RabbitBroker(owner._uri, middlewares=middlewares),
            )
        
        # If the scheme is not recognized as a supported broker type, raise a ConfigurationError with a clear message 
        # to guide the user towards using a supported scheme.
        if scheme in {"redis", "rediss"}:
            return cast(
                "FastStreamBroker",
                RedisBroker(owner._uri, middlewares=middlewares),
            )
        
        # If the scheme is not recognized as a supported broker type, raise a ConfigurationError with a clear message 
        # to guide the user towards using a supported scheme.
        raise ConfigurationError(
            f"Unsupported broker scheme '{scheme}'. Use amqp:// or redis://."
        )

    def _start_redis_delayed_worker_if_needed(self) -> None:
        owner = self._owner
        if owner._scheme not in {"redis", "rediss"}:
            return
        if self._redis_delayed_worker_task is not None and not self._redis_delayed_worker_task.done():
            return
        broker = self.require_broker()
        if resolve_redis_delayed_client(broker) is None:
            raise ConfigurationError(
                "Redis delayed delivery requires an active Redis connection."
            )
        self._redis_delayed_worker_task = asyncio.create_task(
            self._run_redis_delayed_worker(),
            name="lyik-redis-delayed-worker",
        )

    async def _stop_redis_delayed_worker(self) -> None:
        task = self._redis_delayed_worker_task
        if task is None:
            return
        self._redis_delayed_worker_task = None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return

    async def _run_redis_delayed_worker(self) -> None:
        owner = self._owner
        while True:
            try:
                broker = owner._broker
                if broker is None:
                    return
                redis = resolve_redis_delayed_client(broker)
                if redis is None:
                    raise ConfigurationError(
                        "Redis delayed delivery requires an active Redis connection."
                    )

                self._cleanup_processed_delayed_ids()

                now_ms = int(time.time() * 1000)
                entries = await redis.zrangebyscore(
                    REDIS_DELAYED_ZSET_KEY,
                    "-inf",
                    now_ms,
                    start=0,
                    num=100,
                )
                if not entries:
                    await asyncio.sleep(REDIS_DELAY_POLL_INTERVAL_SECONDS)
                    continue

                for member in entries:
                    try:
                        envelope = json.loads(_decode_redis_member(member))
                    except (json.JSONDecodeError, TypeError):
                        logger.warning("Discarding malformed delayed Redis entry.")
                        await redis.zrem(REDIS_DELAYED_ZSET_KEY, member)
                        continue

                    normalized = self._normalize_delayed_envelope(envelope)
                    if normalized is None:
                        logger.warning("Discarding invalid delayed Redis envelope.")
                        await redis.zrem(REDIS_DELAYED_ZSET_KEY, member)
                        continue

                    (
                        delayed_id,
                        topic,
                        payload,
                        headers,
                        correlation_id,
                        reply_to,
                    ) = normalized

                    if delayed_id is not None and delayed_id in self._processed_delayed_ids:
                        logger.debug("Skipping duplicate delayed message id=%s", delayed_id)
                        await redis.zrem(REDIS_DELAYED_ZSET_KEY, member)
                        continue

                    published = await self._publish_delayed_with_retry(
                        topic=topic,
                        payload=payload,
                        headers=headers,
                        correlation_id=correlation_id,
                        reply_to=reply_to,
                    )
                    if not published:
                        logger.debug(
                            "Delayed publish failed after retries for correlation_id=%s",
                            correlation_id,
                        )
                        continue

                    await redis.zrem(REDIS_DELAYED_ZSET_KEY, member)
                    if delayed_id is not None:
                        self._processed_delayed_ids[delayed_id] = time.monotonic()
                    logger.debug(
                        "Processed delayed message topic=%s correlation_id=%s",
                        topic,
                        correlation_id,
                    )

                await asyncio.sleep(0)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Redis delayed worker loop error.")
                await asyncio.sleep(1.0)

    async def _publish_delayed_with_retry(
        self,
        *,
        topic: str,
        payload: object,
        headers: dict[str, str],
        correlation_id: str,
        reply_to: str | None,
    ) -> bool:
        owner = self._owner
        retry_policy = owner._retry_policy
        max_attempts = retry_policy.max_retries + 1
        for attempt in range(max_attempts):
            try:
                await owner._publisher.publish(
                    topic=topic,
                    payload=payload,
                    headers=headers,
                    correlation_id=correlation_id,
                    reply_to=reply_to,
                )
                return True
            except Exception:
                if attempt >= retry_policy.max_retries:
                    logger.exception(
                        "Delayed publish permanently failed topic=%s correlation_id=%s",
                        topic,
                        correlation_id,
                    )
                    return False
                delay_seconds = retry_policy.compute_delay_seconds(attempt)
                logger.debug(
                    "Delayed publish retry topic=%s correlation_id=%s attempt=%s delay=%.3f",
                    topic,
                    correlation_id,
                    attempt + 1,
                    delay_seconds,
                )
                await asyncio.sleep(delay_seconds)
        return False

    def _normalize_delayed_envelope(
        self,
        envelope: object,
    ) -> tuple[str | None, str, object, dict[str, str], str, str | None] | None:
        if not isinstance(envelope, dict):
            return None

        topic = envelope.get("topic")
        if not isinstance(topic, str) or not topic.strip():
            return None

        correlation_id_raw = envelope.get("correlation_id")
        correlation_id = (
            correlation_id_raw
            if isinstance(correlation_id_raw, str) and correlation_id_raw
            else uuid.uuid4().hex
        )

        reply_to_raw = envelope.get("reply_to")
        reply_to = reply_to_raw if isinstance(reply_to_raw, str) and reply_to_raw else None

        headers = _coerce_headers(envelope.get("headers"))
        payload = envelope.get("payload")
        if not _is_json_serializable(payload):
            return None

        delayed_id_raw = envelope.get("delayed_id")
        delayed_id = delayed_id_raw if isinstance(delayed_id_raw, str) and delayed_id_raw else None
        return delayed_id, topic, payload, headers, correlation_id, reply_to

    def _cleanup_processed_delayed_ids(self) -> None:
        if not self._processed_delayed_ids:
            return
        now = time.monotonic()
        expired = [
            delayed_id
            for delayed_id, processed_at in self._processed_delayed_ids.items()
            if now - processed_at > _DELAYED_PROCESSED_TTL_SECONDS
        ]
        for delayed_id in expired:
            self._processed_delayed_ids.pop(delayed_id, None)


def _decode_redis_member(member: object) -> str:
    if isinstance(member, bytes):
        return member.decode("utf-8")
    if isinstance(member, str):
        return member
    return str(member)


def _coerce_headers(raw_headers: object) -> dict[str, str]:
    if not isinstance(raw_headers, dict):
        return {}
    result: dict[str, str] = {}
    for key, value in raw_headers.items():
        if not isinstance(key, str):
            continue
        if isinstance(value, str):
            result[key] = value
            continue
        if value is None:
            continue
        result[key] = str(value)
    return result


def _is_json_serializable(value: object) -> bool:
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return False
    return True
