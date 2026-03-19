"""
This code is the entry point for the MessageBroker class, which provides a high-level abstraction for building message-driven applications on top of various broker 
adapters (like Redis or Kafka). The MessageBroker class manages the connection to the broker, allows users to register message handlers and reply handlers, and handles 
the publishing and consuming of messages in a transport-agnostic way. It also includes features like retry logic, processing timeouts, and dead-letter queue handling to 
make it robust for real-world applications. The code is designed to be flexible and extensible, allowing users to focus on their business logic while the MessageBroker 
takes care of the underlying mechanics of message handling.
"""
import asyncio
import time
import uuid
from collections.abc import Awaitable
from datetime import datetime
from inspect import isawaitable
from typing import Callable, TypeAlias, TypeVar, cast

from .app_logging import get_logger
from .core.context import BrokerContext
from .core.interfaces import Message
from .core.registry import BrokerRegistry
from .schema import DataPacket, Payload, ResponsePacket


HandlerResult = TypeVar("HandlerResult") # Creates a generic placeholder type for whatever a user's function might return.
MaybeAwaitable: TypeAlias = HandlerResult | Awaitable[HandlerResult] # This is what allows developers to write both def handler() and async def handler() seamlessly.
OnMessageHandler: TypeAlias = Callable[[DataPacket], MaybeAwaitable[Payload | None]] # Defines the signature of a user's message handler.
OnReplyHandler: TypeAlias = Callable[[ResponsePacket], MaybeAwaitable[None]] # It takes a ResponsePacket and returns nothing
OnMessageDecorator: TypeAlias = Callable[[OnMessageHandler], Callable[[Message], Awaitable[None]]] # This takes a user's handler and wrap it in a function that the underlying adapter understands
OnReplyDecorator: TypeAlias = Callable[[OnReplyHandler], Callable[[Message], Awaitable[None]]]


class MessageBroker:
    """High-level request/response wrapper built on top of broker adapters.

    This class keeps business handlers transport-neutral while exposing a small
    ergonomic API for publish/subscribe and RPC-style request/response.

    Example:
        broker = MessageBroker("redis://localhost:6379")

        @broker.on_message
        async def handle(data: DataPacket) -> dict[str, bool]:
            return {"ok": True}

        await broker.connect()
        await broker.start()
    """

    def __init__(
        self,
        connection_uri: str,
        queue_name: str = "default_queue",
        **context_options: object, # A "catch-all" for any extra keyword arguments (like max_retries=5 or middlewares=[...])
    ) -> None:
        self.log = get_logger(self.__class__.__name__) # Initializes a structured logger specifically named "MessageBroker"
        self.context = BrokerContext(connection_uri, **context_options) # Takes the raw URI and extra options and feeds them into
        self.broker = BrokerRegistry.create(self.context) # Uses the context to figure out which adapter to instantiate (like RedisBroker or KafkaBroker) based on the URI scheme and options

        self.queue_name = queue_name # Saves the target queue name
        self.reply_queue = f"reply_queue_{uuid.uuid4().hex}"

        self._publisher = None
        self._subscriber = None
        self._message_handler: OnMessageHandler | None = None
        self._reply_handler: OnReplyHandler | None = None
        self._message_wrapper: Callable[[Message], Awaitable[None]] | None = None
        self._reply_wrapper: Callable[[Message], Awaitable[None]] | None = None

        self._run_task: asyncio.Task[None] | None = None # Will hold the main infinite loop that keeps the broker alive.
        self._active_handler_tasks: set[asyncio.Task[object]] = set() # Handles graceful shitdowns
        self._pending_replies: dict[str, asyncio.Future[ResponsePacket]] = {} # A dictionary that maps a correlation_id to an asyncio.Future

        observers = self.context.options.get("observers", []) # Extracts any observers from the context
        self._observers = list(observers) if isinstance(observers, list) else []

    async def connect(self) -> None:
        """Connect the underlying adapter and prepare pub/sub primitives."""

        await self.broker.connect() # Establishes a TCP connection with the underlying adapter's server (like Redis or Kafka)
        self._publisher = self.broker.get_publisher()
        self._subscriber = self.broker.get_subscriber()

    async def disconnect(self) -> None:
        """Gracefully stop tasks, resolve waiters, and release broker resources."""

        current = asyncio.current_task()
        if self._run_task is not None and self._run_task is not current:
            self._run_task.cancel()
            try:
                await self._run_task
            except asyncio.CancelledError:
                pass
            finally:
                self._run_task = None

        for task in list(self._active_handler_tasks):
            task.cancel()

        if self._active_handler_tasks:
            await asyncio.gather(*self._active_handler_tasks, return_exceptions=True)
        self._active_handler_tasks.clear()

        for correlation_id, waiter in list(self._pending_replies.items()):
            if not waiter.done():
                waiter.cancel()
            self._pending_replies.pop(correlation_id, None)

        await self.broker.disconnect() # Tells the underlying adapter to clear its connection pool and shutdown any inernal workers

    async def send_message(
        self,
        content: Payload,
        sender: str,
        reply: bool = False,
        deliver_at: datetime | None = None,
    ) -> str:
        """Publish an application payload and optionally request a reply.

        Args:
            content: JSON-like payload to publish.
            sender: Logical sender/service name.
            reply: When True, includes an auto-generated reply queue.
            deliver_at: Optional UTC datetime for delayed delivery.

        Returns:
            Correlation ID for tracking the published request.

        Example:
            correlation_id = await broker.send_message(
                content={"task": "sync"},
                sender="worker-api",
                reply=True,
            )
        """

        reply_to = self.reply_queue if reply else None
        return await self._build_and_publish_packet(
            content=content,
            sender=sender,
            reply_to=reply_to, # If the user set reply=True, it grabs the unique reply_queue_a1b2... string we generated in the __init__
            deliver_at=deliver_at, # If the user provided a datetime for delayed delivery, it will be passed down to the adapter to handle scheduling. If not, the message will be published immediately.
        )

    async def send_and_wait(
        self,
        content: Payload,
        sender: str,
        *,
        timeout: float | None = None,
        deliver_at: datetime | None = None,
    ) -> ResponsePacket:
        """Send a request and wait for the correlated response.

        Args:
            content: JSON-like payload to publish.
            sender: Logical sender/service name.
            timeout: Max wait time in seconds for the response.
            deliver_at: Optional UTC datetime for delayed delivery.

        Returns:
            A response packet with matching correlation ID.

        Example:
            response = await broker.send_and_wait(
                content={"task": "healthcheck"},
                sender="cli",
                timeout=3.0,
            )
        """

        if self._subscriber is None:
            raise RuntimeError("Call connect() before send_and_wait().")
        
        # This block ensures the broker is actively listening to its temporary reply_queue
        if self._reply_wrapper is None:
            if self._reply_handler is None:
                async def _noop_reply(_resp: ResponsePacket) -> None:
                    self.log.debug("No reply handler registered; using RPC waiter only")

                self._reply_handler = _noop_reply
            self._reply_wrapper = self._make_reply_wrapper(self._reply_handler)
            await self._subscriber.subscribe(self.reply_queue, self._reply_wrapper)

        correlation_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[ResponsePacket] = loop.create_future()
        self._pending_replies[correlation_id] = waiter

        try:
            # Sends the message out to Redis/Kafka.
            await self._build_and_publish_packet(
                content=content,
                sender=sender,
                reply_to=self.reply_queue, 
                deliver_at=deliver_at,
                correlation_id=correlation_id,
            )
            return await asyncio.wait_for(waiter, timeout=timeout) # Waits for the future to be set with a response that has the same correlation_id. If the timeout is reached before that happens, it raises a TimeoutError.
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"Timed out waiting for response correlation_id={correlation_id}"
            )
        finally:
            self._pending_replies.pop(correlation_id, None) # This guarantees we delete the waiter box from memory

    # The following methods are responsible for taking user-defined handlers and wrapping them in a way that the underlying adapter can call when messages arrive. This is where we bridge the gap between the transport-specific Message objects and the user's business logic that operates on DataPacket and ResponsePacket.
    def _make_message_wrapper(
        self,
        user_handler: OnMessageHandler,
    ) -> Callable[[Message], Awaitable[None]]:
        """Build and cache adapter-facing message wrapper once per registration."""

        async def wrapper(msg: Message) -> None: # This is the actual function that Redis/Kafka will call when a message arrives.
            data: DataPacket | None = None
            try:
                data = DataPacket(**msg.payload)
                self.log.info(
                    "Message received",
                    packet_id=data.id,
                    correlation_id=data.correlation_id,
                )

                max_retries = self._resolve_handler_max_retries(msg)
                timeout_ms = self._resolve_processing_timeout_ms(msg)
                source_topic = str(msg.metadata.get("source_topic", self.queue_name))

                attempt = 0
                while True:
                    started = time.perf_counter()
                    status = "processed"
                    """
                    Try block for handler execution: We attempt to execute the user's handler function with the parsed DataPacket. 
                    If the handler executes successfully within the allowed processing time, we proceed to notify observers and publish 
                    a response if needed. However, if the handler raises an exception or exceeds the processing timeout, we catch that 
                    and enter the retry logic. This structure allows us to handle both expected and unexpected errors gracefully while 
                    providing hooks for observability and response handling.
                    """
                    try:
                        handler_result = user_handler(data)
                        if timeout_ms is not None:
                            handler_result = await asyncio.wait_for(
                                self._resolve_result(handler_result),
                                timeout=timeout_ms / 1000.0,
                            )
                        else:
                            handler_result = await self._resolve_result(handler_result) # Checks if the user wrote def or async def and safely awaits it either way

                        elapsed_ms = (time.perf_counter() - started) * 1000.0
                        await self._notify_handler(
                            topic=source_topic,
                            elapsed_ms=elapsed_ms,
                            message=msg,
                            status=status,
                        )

                        if data.reply_to:
                            response = ResponsePacket(
                                correlation_id=data.correlation_id,
                                in_response_to=data.id,
                                status="processed",
                                content=handler_result,
                            )
                            await self._publisher.publish(
                                data.reply_to,
                                Message(payload=response.model_dump()),
                            )
                            self.log.info(
                                "Response published",
                                correlation_id=data.correlation_id,
                            )
                        return
                    
                    # Error handling and retry logic: If the handler raises an exception or if it exceeds the specified processing timeout, we catch that 
                    # and decide whether to retry based on the max_retries configuration. If we exhaust all retries, we publish a failure response (if reply_to is set) 
                    # and/or send the original message to a dead-letter queue for later inspection.
                    except asyncio.TimeoutError as exc:
                        status = "timeout"
                        elapsed_ms = (time.perf_counter() - started) * 1000.0
                        await self._notify_handler(
                            topic=source_topic,
                            elapsed_ms=elapsed_ms,
                            message=msg,
                            status=status,
                        )
                        if attempt < max_retries:
                            attempt += 1
                            await self._notify_retry(
                                operation="handler",
                                attempt=attempt,
                                max_retries=max_retries,
                                error=exc,
                            )
                            continue
                        await self._publish_failure_response(data, str(exc))
                        await self._publish_to_dlq(
                            source_topic=source_topic,
                            original_message=msg,
                            error=exc,
                            retries=attempt,
                        )
                        return
                    
                    # Error handling for exceptions raised by the handler itself (not just timeouts)
                    except Exception as exc:
                        status = "failed"
                        elapsed_ms = (time.perf_counter() - started) * 1000.0
                        await self._notify_handler(
                            topic=source_topic,
                            elapsed_ms=elapsed_ms,
                            message=msg,
                            status=status,
                        )
                        if attempt < max_retries:
                            attempt += 1
                            await self._notify_retry(
                                operation="handler",
                                attempt=attempt,
                                max_retries=max_retries,
                                error=exc,
                            )
                            continue
                        await self._publish_failure_response(data, str(exc))
                        await self._publish_to_dlq(
                            source_topic=source_topic,
                            original_message=msg,
                            error=exc,
                            retries=attempt,
                        )
                        return
            
            # Error handling for issues that occur during message parsing or before the handler is even called. Since we might not have a valid DataPacket 
            # (if parsing fails), we can't rely on correlation_id or reply_to, so we just log the error and attempt to publish to the DLQ if possible.
            except Exception as exc:
                pkt_id = getattr(data, "id", None)
                corr = getattr(data, "correlation_id", None)
                self.log.exception(
                    "Message processing failed",
                    packet_id=pkt_id,
                    correlation_id=corr,
                )
                await self._publish_to_dlq(
                    source_topic=str(msg.metadata.get("source_topic", self.queue_name)),
                    original_message=msg,
                    error=exc,
                    retries=self._resolve_handler_max_retries(msg),
                )

        return wrapper

    """
    This function allows users to register their message handler functions in a flexible way. They can either use it as a decorator (e.g., @broker.on_message) or
    call it directly with their handler function. The method takes care of wrapping the user's handler in a function that the underlying adapter can call when messages 
    arrive, and it also caches this wrapper for efficiency. This design abstracts away the transport-specific details and lets users focus on their business logic.
    """
    def on_message(
        self,
        handler: OnMessageHandler | None = None,
    ) -> OnMessageDecorator | Callable[[Message], Awaitable[None]]:
        """Register a message handler.

        Purpose:
            Attach a request handler for inbound packets.

        Args:
            handler: Optional handler function. If omitted, returns a decorator.

        Returns:
            Either a decorator or an adapter-facing async wrapper.

        Example:
            @broker.on_message
            async def handle(data: DataPacket) -> dict[str, bool]:
                return {"ok": True}
        """

        if handler is None:
            def decorator(fn: OnMessageHandler) -> Callable[[Message], Awaitable[None]]:
                self._message_handler = fn
                self._message_wrapper = self._make_message_wrapper(fn)
                return self._message_wrapper

            return cast(OnMessageDecorator, decorator)

        self._message_handler = handler
        self._message_wrapper = self._make_message_wrapper(handler)
        return self._message_wrapper

    """
    This function is similar to on_message but for reply handlers. It allows users to register a function that will be called when a response is 
    received on the reply queue.
    """
    def _make_reply_wrapper(
        self,
        user_handler: OnReplyHandler,
    ) -> Callable[[Message], Awaitable[None]]:
        """Build and cache adapter-facing reply wrapper once per registration."""

        async def wrapper(msg: Message) -> None:
            data = ResponsePacket(**msg.payload)

            self.log.info("Reply received", correlation_id=data.correlation_id)

            pending = self._pending_replies.get(data.correlation_id)
            if pending is not None and not pending.done():
                pending.set_result(data)

            result = user_handler(data)
            if isawaitable(result):
                await result

        return wrapper

    async def _build_and_publish_packet(
        self,
        *,
        content: Payload,
        sender: str,
        reply_to: str | None,
        deliver_at: datetime | None,
        correlation_id: str | None = None,
    ) -> str:
        """Build the transport message envelope and publish it.

        Args:
            content: JSON-like payload to publish.
            sender: Logical sender/service name.
            reply_to: Optional reply queue name.
            deliver_at: Optional UTC datetime for delayed delivery.
            correlation_id: Optional explicit correlation ID.

        Returns:
            Correlation ID used for the published packet.
        """
        packet = DataPacket(
            sender=sender,
            content=content,
            reply_to=reply_to,
            deliver_at=deliver_at,
            correlation_id=correlation_id or uuid.uuid4().hex,
        )

        self.log.info(
            "Sending packet",
            id=packet.id,
            correlation_id=packet.correlation_id,
        )

        message = Message(payload=packet.model_dump())
        message.metadata.setdefault("submitted_at", time.time())

        started = time.perf_counter()
        if deliver_at:
            ts = float(deliver_at.timestamp())
            await self._publisher.publish(
                self.queue_name,
                message,
                deliver_at=ts,
            )
        else:
            await self._publisher.publish(self.queue_name, message)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        await self._notify_publish(topic=self.queue_name, elapsed_ms=elapsed_ms, message=message)

        return packet.correlation_id

    def on_reply(
        self,
        handler: OnReplyHandler | None = None,
    ) -> OnReplyDecorator | Callable[[Message], Awaitable[None]]:
        """Register a reply handler for inbound response packets.

        Purpose:
            Attach a callback for responses published to this broker's reply queue.

        Args:
            handler: Optional handler function. If omitted, returns a decorator.

        Returns:
            Either a decorator or an adapter-facing async wrapper.

        Example:
            @broker.on_reply
            async def handle_reply(reply: ResponsePacket) -> None:
                print(reply.status)
        """

        if handler is None:
            def decorator(fn: OnReplyHandler) -> Callable[[Message], Awaitable[None]]:
                self._reply_handler = fn
                self._reply_wrapper = self._make_reply_wrapper(fn)
                return self._reply_wrapper

            return cast(OnReplyDecorator, decorator)

        self._reply_handler = handler
        self._reply_wrapper = self._make_reply_wrapper(handler)
        return self._reply_wrapper

    async def start(self) -> None:
        """Start consuming message and reply topics until cancellation."""

        if self._subscriber is None:
            raise RuntimeError("Call connect() before start().")

        if self._message_wrapper is None:
            async def _noop_message(_msg: DataPacket) -> None:
                self.log.debug("No message handler registered; dropping message")

            self._message_handler = _noop_message
            self._message_wrapper = self._make_message_wrapper(_noop_message)

        if self._reply_wrapper is None:
            async def _noop_reply(_resp: ResponsePacket) -> None:
                self.log.debug("No reply handler registered; ignoring reply")

            self._reply_handler = _noop_reply
            self._reply_wrapper = self._make_reply_wrapper(_noop_reply)

        await self._subscriber.subscribe(self.queue_name, self._message_wrapper)
        await self._subscriber.subscribe(self.reply_queue, self._reply_wrapper)

        self.log.info("Broker started")

        try:
            self._run_task = asyncio.current_task()
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            self.log.info("Broker shutdown requested")
            raise
        finally:
            await self.disconnect()

    """
    This function checks if the result returned by the user's handler is an awaitable (i.e., if the user defined their handler with async def). 
    If it is awaitable, we create a task for it and add it to the set of active handler tasks for tracking. We then await the task to get the 
    final result. If it's not awaitable, we simply return it as is. This allows users to write both synchronous and asynchronous handlers without 
    worrying about the underlying mechanics of awaiting.
    """
    async def _resolve_result(self, maybe_result: MaybeAwaitable[Payload | None]) -> Payload | None:
        if isawaitable(maybe_result):
            task = asyncio.create_task(cast(Awaitable[Payload | None], maybe_result))
            self._active_handler_tasks.add(task)
            task.add_done_callback(self._active_handler_tasks.discard)
            return await task
        return cast(Payload | None, maybe_result)

    """
    This function checks if there's a max_retries configuration specified for the message handler, either in the message metadata or 
    in the broker's context options. It returns the maximum number of retries allowed for that handler, defaulting to 0 if not configured 
    or if the value is invalid. This allows for flexible retry strategies on a per-message basis while also providing a global default.
    """
    def _resolve_handler_max_retries(self, message: Message) -> int:
        configured = message.metadata.get("handler_max_retries")
        if configured is None:
            configured = self.context.options.get("handler_max_retries", 0)
        if not isinstance(configured, int) or configured < 0:
            return 0
        return configured

    """
    This function checks if there's a processing timeout specified for the message handler, either in the message metadata or 
    in the broker's context options. If a valid timeout is found (a positive integer), it returns that value in milliseconds. 
    If not, it returns None, indicating that there is no processing timeout and the handler can run indefinitely until it finishes 
    or raises an exception.
    """
    def _resolve_processing_timeout_ms(self, message: Message) -> int | None:
        configured = message.metadata.get("processing_timeout_ms")
        if configured is None:
            configured = self.context.options.get("processing_timeout_ms")
        if configured is None:
            return None
        if not isinstance(configured, int) or configured <= 0:
            return None
        return configured

    """
    This function is responsible for determining the appropriate dead-letter queue (DLQ) topic based on the source topic of 
    the message and the broker's configuration. It first checks if there is a specific DLQ topic configured for the source 
    topic in the "dlq_topics" dictionary. If not, it falls back to a default DLQ topic specified by "default_dlq_topic". 
    If neither is properly configured, it returns None, indicating that there is no DLQ to publish to.
    """
    def _resolve_dlq_topic(self, source_topic: str) -> str | None:
        dlq_topics = self.context.options.get("dlq_topics", {})
        if isinstance(dlq_topics, dict):
            topic = dlq_topics.get(source_topic)
            if isinstance(topic, str) and topic.strip():
                return topic
        fallback = self.context.options.get("default_dlq_topic")
        if isinstance(fallback, str) and fallback.strip():
            return fallback
        return None

    """
    This function is responsible for publishing a standardized failure response back to the reply_to queue (if it exists) 
    when a handler fails after exhausting all retries. It constructs a ResponsePacket with status "failed" and includes 
    the error message in the content. This allows the original requester to receive structured information about the failure 
    instead of just timing out.
    """
    async def _publish_failure_response(self, packet: DataPacket | None, error: str) -> None:
        if packet is None or not packet.reply_to:
            return
        failed = ResponsePacket(
            correlation_id=packet.correlation_id,
            in_response_to=packet.id,
            status="failed",
            content={"error": error},
        )
        await self._publisher.publish(
            packet.reply_to,
            Message(payload=failed.model_dump()),
        )

    """
    This function is called when we've exhausted all retry attempts for a given message handler and are about to give up on processing 
    that message. It attempts to publish the original message along with error details to a dead-letter queue (DLQ) for later inspection. 
    The DLQ topic is determined based on the source topic of the message and the broker's configuration. This allows developers to analyze 
    failed messages and understand why they couldn't be processed successfully.
    """
    async def _publish_to_dlq(
        self,
        *,
        source_topic: str,
        original_message: Message,
        error: Exception,
        retries: int,
    ) -> None:
        dlq_topic = self._resolve_dlq_topic(source_topic)
        if dlq_topic is None or self._publisher is None:
            return

        failed_payload = {
            "original_payload": original_message.payload,
            "original_headers": dict(original_message.headers),
            "original_metadata": dict(original_message.metadata),
            "failure": {
                "error": str(error),
                "retries": retries,
                "failed_at": time.time(),
                "source_topic": source_topic,
            },
        }

        dlq_message = Message(
            payload=failed_payload,
            headers={**original_message.headers, "x-dlq": "true"},
            metadata={
                **original_message.metadata,
                "dlq": True,
                "source_topic": source_topic,
                "failed_at": time.time(),
            },
            correlation_id=original_message.correlation_id,
        )
        await self._publisher.publish(dlq_topic, dlq_message)

    """
    This method is called after a message is published to notify any observers about the publish event, including the topic, 
    time taken to publish, and the original message.
    """
    async def _notify_publish(self, *, topic: str, elapsed_ms: float, message: Message) -> None:
        for observer in self._observers:
            callback = getattr(observer, "on_publish", None)
            if callback is None:
                continue
            result = callback(topic=topic, elapsed_ms=elapsed_ms, message=message)
            if isawaitable(result):
                await result

    """
    This method is called after a handler finishes processing (whether successfully or with an error) to notify 
    any observers about the outcome, including the topic, processing time, original message, and status.
    """
    async def _notify_handler(
        self,
        *,
        topic: str,
        elapsed_ms: float,
        message: Message,
        status: str,
    ) -> None:
        for observer in self._observers:
            callback = getattr(observer, "on_handler_execution", None)
            if callback is None:
                continue
            result = callback(
                topic=topic,
                elapsed_ms=elapsed_ms,
                message=message,
                status=status,
            )
            if isawaitable(result):
                await result

    """
    This method is called when a handler raises an exception or exceeds its processing timeout, 
    and we are about to retry the operation. It notifies observers about the retry attempt, including 
    which operation is being retried (like "handler"), the current attempt number, the maximum retries 
    allowed, and the error that caused the retry. This allows for centralized logging, alerting, or 
    even dynamic adjustment of retry strategies based on observed failures.
    """
    async def _notify_retry(
        self,
        *,
        operation: str,
        attempt: int,
        max_retries: int,
        error: Exception,
    ) -> None:
        for observer in self._observers:
            callback = getattr(observer, "on_retry_attempt", None)
            if callback is None:
                continue
            result = callback(
                operation=operation,
                attempt=attempt,
                max_retries=max_retries,
                error=error,
            )
            if isawaitable(result):
                await result
