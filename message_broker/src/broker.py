"""
This code is the entry point for the MessageBroker class, which provides a high-level abstraction for building message-driven applications on top of various broker 
adapters. The MessageBroker class manages the connection to the broker, allows users to register message handlers and reply handlers, and handles 
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

"""
The following definitions define exactly what kind of functions users can write and how they behave. They allow the system to accept 
both synchronous and asynchronous handlers, enforce consistent input and output structures, and internally wrap user-defined 
functions into a format that the underlying messaging system can execute safely and reliably.
"""
# Creates a generic placeholder type for whatever a user's function might return.
HandlerResult = TypeVar("HandlerResult") 

# Allows developers to write both def handler() and async def handler() seamlessly.
MaybeAwaitable: TypeAlias = HandlerResult | Awaitable[HandlerResult] 

# Defines the signature of a user's message handler.
OnMessageHandler: TypeAlias = Callable[[DataPacket], MaybeAwaitable[Payload | None]] 

# Takes a ResponsePacket and returns nothing
OnReplyHandler: TypeAlias = Callable[[ResponsePacket], MaybeAwaitable[None]] 

# This takes a user's handler and wrap it in a function that the underlying adapter understands
OnMessageDecorator: TypeAlias = Callable[[OnMessageHandler], Callable[[Message], Awaitable[None]]]

# Similar to OnMessageDecorator but for reply handlers, allowing users to register a function that will be called when a response is 
# received on the reply queue.
OnReplyDecorator: TypeAlias = Callable[[OnReplyHandler], Callable[[Message], Awaitable[None]]]


class MessageBroker:
    """
    `MessageBroker` is a high-level request/response wrapper built on top of broker adapters.

    This class keeps business handlers transport-neutral while exposing a small
    ergonomic API for publish/subscribe and RPC-style request/response.

    `Arguments`:
        - `connection_uri`: Broker URI used to resolve the adapter implementation.
            Examples: `redis://localhost:6379/0`,
            `amqp://guest:guest@localhost:5672//`.
        - `queue_name`: Default queue/topic used by `start()`, `send_message()`,
            and `send_and_wait()` unless another route is provided.
        - `context_options`: Extra options merged into `BrokerContext`.
            Common options include:
            - `timeout` (int, ms): publish/subscribe operation timeout.
            - `max_retries` (int): adapter retry count for transient failures.
            - `processing_timeout_ms` (int): max handler runtime.
            - `handler_max_retries` (int): retries for user handler errors.
            - `default_dlq_topic` (str): fallback dead-letter queue.
            - `dlq_topics` (dict[str, str]): per-topic DLQ overrides.
            - `middlewares` (list[Middleware]): lifecycle hooks.
            - `observers` (list[object]): optional observability callbacks.

    `Example`:
    ```
        broker = MessageBroker("redis://localhost:6379")

        @broker.on_message
        async def handle(data: DataPacket) -> dict[str, bool]:
            return {"ok": True}

        await broker.connect()
        await broker.start()
    ```
    """

    def __init__(
        self,
        connection_uri: str, # URI of the broker to connect to
        queue_name: str = "default_queue", # Queue or topic name to subscribe to for incoming messages, default: default_queue
        **context_options: object, # A "catch-all" for any extra keyword arguments (like max_retries=5 or middlewares=[...])
    ) -> None: # None return type since this is just the constructor
        """
        Create a broker client with adapter auto-selection.

        `MessageBroker` is a high-level request/response wrapper built on top of broker adapters.

        This class keeps business handlers transport-neutral while exposing a small
        ergonomic API for publish/subscribe and RPC-style request/response.

        `Arguments`:
            - `connection_uri`: Broker URI used to resolve the adapter implementation.
                Examples: `redis://localhost:6379/0`,
                `amqp://guest:guest@localhost:5672//`.
            - `queue_name`: Default queue/topic used by `start()`, `send_message()`,
                and `send_and_wait()` unless another route is provided.
            - `context_options`: Extra options merged into `BrokerContext`.
                Common options include:
                - `timeout` (int, ms): publish/subscribe operation timeout.
                - `max_retries` (int): adapter retry count for transient failures.
                - `processing_timeout_ms` (int): max handler runtime.
                - `handler_max_retries` (int): retries for user handler errors.
                - `default_dlq_topic` (str): fallback dead-letter queue.
                - `dlq_topics` (dict[str, str]): per-topic DLQ overrides.
                - `middlewares` (list[Middleware]): lifecycle hooks.
                - `observers` (list[object]): optional observability callbacks.

        `Example`:
        ```
            broker = MessageBroker("redis://localhost:6379")

            @broker.on_message
            async def handle(data: DataPacket) -> dict[str, bool]:
                return {"ok": True}

            await broker.connect()
            await broker.start()
        ```
        """
        self.log = get_logger(self.__class__.__name__) # Initializes a structured logger specifically named "MessageBroker"
        self.context = BrokerContext(connection_uri, **context_options) # Takes the raw URI and extra options and feeds them into
        self.broker = BrokerRegistry.create(self.context) # Uses the context to figure out which adapter to instantiate based on the URI

        self.queue_name = queue_name # Saves the target queue name, default: default_queue
        self.reply_queue = f"reply_queue_{uuid.uuid4().hex}" # Generates a unique queue reply

        self._publisher = None # Initializes a publisher reference that is set when a broker is connected
        self._subscriber = None # Initializes a subscriber reference that is set when a broker is connected
        self._message_handler: OnMessageHandler | None = None # Initializes a message handler to hold a user defined function for messages
        self._reply_handler: OnReplyHandler | None = None # Initializes a reply handler to hold a user defined function for replies
        self._message_wrapper: Callable[[Message], Awaitable[None]] | None = None # Wrapper for message handler to translate to the broker
        self._reply_wrapper: Callable[[Message], Awaitable[None]] | None = None # Wrapper for reply handler to translate to the broker

        self._run_task: asyncio.Task[None] | None = None # Will hold the main infinite loop that keeps the broker alive. Allows us to cancel tasks
        self._active_handler_tasks: set[asyncio.Task[object]] = set() # Handles graceful shutdowns, keeps track of active handler tasks
        self._pending_replies: dict[str, asyncio.Future[ResponsePacket]] = {} # A dictionary that maps a correlation_id to an asyncio.Future

        observers = self.context.options.get("observers", []) # Extracts any user defined observers, defaults to an empty list
        # Observers are user-defined functions that can be called at various points in the message handling lifecycle 
        # For example, users might want to register an observer that gets called every time a message is processed, 
        # or every time a retry happens.

        self._observers = list(observers) if isinstance(observers, list) else [] # Ensures that the observers are stored as a list

    async def connect(self) -> None:
        """
        Connect the underlying adapter and prepare pub/sub primitives.
        Purpose:
            Establish connection to the broker and set up publisher and subscriber interfaces. 
        
        How to use:
        ```
            broker = MessageBroker("redis://localhost:6379")
            await broker.connect()
        ```
        """

        await self.broker.connect() # Establishes a TCP connection with the underlying adapter's server
        self._publisher = self.broker.get_publisher()
        self._subscriber = self.broker.get_subscriber()

    async def disconnect(self) -> None:
        """
        Gracefully stop tasks, resolve waiters, and release broker resources.
        Purpose:
            Cleanly shut down the broker by canceling active tasks, resolving pending replies, and disconnecting from the broker.

        How to use:
        ```
            await broker.disconnect()    
        ```
        """

        # Find all running tasks that are a part of the broker's operation
        current = asyncio.current_task() 

        # Iterate through each task and cancel them
        if self._run_task is not None and self._run_task is not current: 
            self._run_task.cancel()
            try:
                # Wait for the task to finish its cancellation process. 
                await self._run_task 
            except asyncio.CancelledError:
                # We expect a CancelledError to be raised when we cancel the task, so we catch it and simply
                pass
            finally:
                # After cancellation, we set the reference to None to clean up.
                self._run_task = None

        # Iterate through any active handler tasks and cancel them as well
        for task in list(self._active_handler_tasks):
            task.cancel()

        # Wait for all active handler tasks to finish their cancellation process. 
        # We use return_exceptions=True to ensure that we wait for all tasks to complete even if some of them raise exceptions during cancellation.
        if self._active_handler_tasks:
            await asyncio.gather(*self._active_handler_tasks, return_exceptions=True)

        # Clear any remaining references to active handler tasks to free up memory and ensure a clean shutdown state.
        self._active_handler_tasks.clear()

        # Resolve any pending reply waiters by setting an exception on them to unblock any coroutines that are waiting for a response. 
        # This ensures that if the broker is shutting down while there are still pending send_and_wait calls, 
        # those calls will be unblocked and can handle the shutdown gracefully instead of hanging indefinitely.
        for correlation_id, waiter in list(self._pending_replies.items()):
            if not waiter.done():
                waiter.cancel()
            self._pending_replies.pop(correlation_id, None)

        # Tells the underlying adapter to clear its connection pool and shutdown any inernal workers
        await self.broker.disconnect() 

    async def send_message(
        self,
        content: Payload,
        sender: str,
        reply: bool = False,
        deliver_at: datetime | None = None,
    ) -> str:
        """
        Publish an application payload and optionally request a reply.

        Args:
            content: Business payload serialized into `DataPacket.content`.
                Typical values include dict/list structures, for example:
                `{"event": "order.created", "order_id": 42}`.
            sender: Producer identifier stored in `DataPacket.sender`.
                Examples: `orders-service`, `api-gateway`, `worker-1`.
            reply: When True, automatically sets `DataPacket.reply_to` to this
                broker instance's ephemeral reply queue.
            deliver_at: Optional timezone-aware UTC datetime for delayed
                delivery. When `None`, message is sent immediately.

        Returns:
            Correlation ID for tracking the published request.

        Example:
        ```
            correlation_id = await broker.send_message(
                content={"task": "sync"},
                sender="worker-api",
                reply=True,
            )
        ```
        """
        # If the user sets reply=True, we want to include a reply_to field in the published message that points to the unique reply 
        # queue we generated in the constructor.
        reply_to = self.reply_queue if reply else None

        # Wait for the message to be published through the underlying adapter and return the correlation_id that was used for that message
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
        """
        Send a request and wait for the correlated response.

        Args:
            content: JSON-like payload to publish.
            sender: Logical sender/service name.
            timeout: Max wait time in seconds for the response.
            deliver_at: Optional UTC datetime for delayed delivery.

        Returns:
            A response packet with matching correlation ID.

        Example:
        ```
            response = await broker.send_and_wait(
                content={"task": "healthcheck"},
                sender="cli",
                timeout=3.0,
            )
        ```
        """

        # Sanity check to ensure we have a subscriber to listen for the response. Since send_and_wait relies on the reply queue and the 
        # subscriber to receive the response, we need to make sure those are set up before we can proceed. If the subscriber is not 
        # initialized, it means the broker is not properly connected or configured, and we should raise an error to inform the user.
        if self._subscriber is None:
            raise RuntimeError("Call connect() before send_and_wait().")
        
        # This block ensures the broker is actively listening to its temporary reply_queue so that when we send a message with reply_to 
        # set to that queue, we can receive the response. If the user hasn't registered a reply handler, we set up a no-op handler that 
        # just logs the received reply without doing anything with it. This allows send_and_wait to function properly even if the user 
        # doesn't care about handling replies outside of the RPC waiter.
        if self._reply_wrapper is None:
            if self._reply_handler is None:
                async def _noop_reply(_resp: ResponsePacket) -> None:
                    self.log.debug("No reply handler registered; using RPC waiter only")

                self._reply_handler = _noop_reply
            self._reply_wrapper = self._make_reply_wrapper(self._reply_handler)
            await self._subscriber.subscribe(self.reply_queue, self._reply_wrapper)

        # Generate a unique correlation id for the current request, will be included in the published message, to correlate the response
        correlation_id = uuid.uuid4().hex

        # Gets the current event loop so we can create a Future that will be awaited until a response with the matching correlation_id is received. 
        # A Future in asyncio is an object that represents a result that may not be available yet.
        loop = asyncio.get_running_loop() 

        # Creates a Future object that will be stored in the _pending_replies dictionary with the correlation_id as the key.,
        waiter: asyncio.Future[ResponsePacket] = loop.create_future()
        # We store this Future in the _pending_replies dictionary so that when a response arrives on the reply queue, the reply wrapper can look up
        # the corresponding Future using the correlation_id and set its result, which will unblock the send_and_wait method and return the response 
        # to the caller.
        self._pending_replies[correlation_id] = waiter

        try:
            # Sends the message out through the configured broker adapter.
            await self._build_and_publish_packet(
                content=content,
                sender=sender,
                reply_to=self.reply_queue, 
                deliver_at=deliver_at,
                correlation_id=correlation_id,
            )
            return await asyncio.wait_for(waiter, timeout=timeout) # Waits for the future to be set with a response that has the same correlation_id. 
        
        # If the timeout is reached before a response is received, it raises a TimeoutError.
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"Timed out waiting for response correlation_id={correlation_id}"
            )
        finally:
            self._pending_replies.pop(correlation_id, None) # This guarantees we delete the waiter box from memory
            # This ensures that if the broker is shutting down while there are still pending send_and_wait calls, those calls will be unblocked and can handle
            # the shutdown gracefully instead of hanging indefinitely.

    # The following methods are responsible for taking user-defined handlers and wrapping them in a way that the underlying adapter can call when messages arrive. 
    # This is where we bridge the gap between the transport-specific Message objects and the user's business logic that operates on DataPacket and ResponsePacket.
    def _make_message_wrapper(
        self,
        user_handler: OnMessageHandler,
    ) -> Callable[[Message], Awaitable[None]]:
        """
        Build and cache adapter-facing message wrapper once per registration.
        This wrapper is responsible for:
        1. Parsing the incoming Message into a DataPacket.
        2. Logging the receipt of the message with its packet ID and correlation ID.
        3. Handling idempotency by reserving the message ID before processing and marking it as completed or aborted afterward.
        4. Executing the user's handler function with retry logic and processing timeouts.
        5. Notifying observers about handler execution and retry attempts.
        6. Publishing a response back to the reply_to queue if specified in the DataPacket

        For example:
        ```
            @broker.on_message
            async def handle(data: DataPacket) -> dict[str, bool]:
                return {"ok": True}
        ```  
        In this example, the user's handler function takes a DataPacket as input and returns a dictionary. 
        The _make_message_wrapper function will take care of all the mechanics of parsing the incoming message, 
        executing the handler with retries and timeouts, and publishing a response if needed, allowing the user to 
        focus solely on their business logic.
        """

        # This is the actual function the adapter calls when a message arrives.
        async def wrapper(msg: Message) -> None: 
            data: DataPacket | None = None
            try:
                data = DataPacket.model_validate(msg.payload)
                self.log.info(
                    "Message received",
                    packet_id=data.id,
                    correlation_id=data.correlation_id,
                )
                
                # Idempotency handling: Before we execute the user's handler, we check if the message ID has already been reserved for processing. 
                # This is to prevent duplicate processing of the same message, which can happen in cases of retries or redeliveries. 
                # If the reservation fails (meaning another worker is already processing this message), we log that it's a duplicate and skip processing it again.
                idempotency_reserved = await self._begin_idempotent_processing(
                    message_id=data.id,
                    topic=str(msg.metadata.get("source_topic", self.queue_name)),
                    message=msg,
                )

                # If we fail to reserve the message for idempotent processing, it means another worker is already processing this message,
                #  so we should not process it again.
                if not idempotency_reserved:
                    return

                # We set a flag to track whether processing succeeded or not. This is important for the idempotency logic in the finally block, where we need to 
                # know whether to mark the message as completed or to abort it so that it can be retried later.
                processing_succeeded = False 

                max_retries = self._resolve_handler_max_retries(msg) 
                timeout_ms = self._resolve_processing_timeout_ms(msg)
                source_topic = str(msg.metadata.get("source_topic", self.queue_name))

                try:
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
                            # We call the user's handler function with the parsed DataPacket. 
                            handler_call = user_handler(data)
                            if timeout_ms is not None:
                                # If a processing timeout is configured, we use asyncio.wait_for to enforce that the handler completes within the specified time limit.
                                handler_result = await asyncio.wait_for(
                                    self._resolve_result(handler_call),
                                    timeout=timeout_ms / 1000.0,
                                )
                            else:
                                handler_result = await self._resolve_result(handler_call) # Checks if the user wrote def or async def and safely awaits it either way

                            elapsed_ms = (time.perf_counter() - started) * 1000.0
                            # After the handler finishes (either successfully or with an error), we notify any observers about the execution outcome, including the topic, 
                            # time taken, original message, and status. This allows for centralized logging, metrics collection, or any other side effects that observers 
                            # might want to perform based on handler executions.
                            await self._notify_handler(
                                topic=source_topic,
                                elapsed_ms=elapsed_ms,
                                message=msg,
                                status=status,
                            )

                            # If the DataPacket includes a reply_to field, it means the sender is expecting a response. In that case, we construct a ResponsePacket with the 
                            # handler's result and publish it back to the specified reply queue.
                            if data.reply_to:
                                # Create a response packet with the same correlation_id and the handler's result as the content.
                                response = ResponsePacket(
                                    correlation_id=data.correlation_id,
                                    in_response_to=data.id,
                                    status="processed",
                                    content=handler_result,
                                )
                                # Wait for the publish operation to complete before marking processing as succeeded. 
                                # If the publish fails, we want to make sure we enter the retry logic instead of
                                await self._publisher.publish(
                                    data.reply_to,
                                    Message(payload=response.model_dump(mode="json")),
                                )
                                self.log.info(
                                    "Response published",
                                    correlation_id=data.correlation_id,
                                )
                            processing_succeeded = True
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
                            # If we have retries left, we notify observers about the retry attempt and loop back to try executing the handler again. 
                            # If we've exhausted all retries, we publish a failure response and/or send the message to the DLQ.
                            if attempt < max_retries:
                                attempt += 1
                                await self._notify_retry(
                                    operation="handler",
                                    attempt=attempt,
                                    max_retries=max_retries,
                                    error=exc,
                                )
                                continue

                            # Await for the failure response to be published before exiting to ensure that we don't lose the response in case of a publish failure.
                            await self._publish_failure_response(data, str(exc))

                            # Publish to DLQ if configured: 
                            # If the handler fails after exhausting all retries, we attempt to publish the original message along with error details to a 
                            # dead-letter queue (DLQ) for later inspection.
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
                            # If we have retries left, we notify observers about the retry attempt and loop back to try executing the handler again. 
                            # If we've exhausted all retries, we publish a failure response and/or send the message to the DLQ.
                            if attempt < max_retries:
                                attempt += 1
                                await self._notify_retry(
                                    operation="handler",
                                    attempt=attempt,
                                    max_retries=max_retries,
                                    error=exc,
                                )
                                continue

                            # Await for the failure response to be published before exiting to ensure that we don't lose the response in case of a publish failure.
                            await self._publish_failure_response(data, str(exc))

                            # Publish to DLQ if configured: If the handler fails after exhausting all retries, we attempt to publish the original message along with 
                            # error details to a dead-letter queue (DLQ) for later inspection.
                            await self._publish_to_dlq(
                                source_topic=source_topic,
                                original_message=msg,
                                error=exc,
                                retries=attempt,
                            )
                            return
                finally:
                    """
                    In the finally block, we check if processing succeeded or not. If it succeeded, we mark the message as completed
                    for idempotency purposes, which might involve recording the message ID in a store to prevent future duplicate processing. 
                    If it did not succeed, we abort the idempotent processing, which might involve releasing any locks or reservations we 
                    made for that message ID so that it can be retried later. This ensures that we maintain the integrity of our idempotency 
                    guarantees regardless of whether the handler succeeded, failed, or raised an unexpected exception.
                    """
                    if processing_succeeded:
                        await self._complete_idempotent_processing(data.id)
                    else:
                        await self._abort_idempotent_processing(data.id)
            
            # Error handling for issues that occur during message parsing or before the handler is even called. Since we might not have a valid DataPacket 
            # (if parsing fails), we can't rely on correlation_id or reply_to, so we just log the error and attempt to publish to the DLQ if possible.
            except Exception as exc:
                pkt_id = getattr(data, "id", None) # Get the packet ID if we were able to parse it, otherwise None
                corr = getattr(data, "correlation_id", None) # Get the correlation ID if we were able to parse it, otherwise None
                self.log.exception(
                    "Message processing failed",
                    packet_id=pkt_id,
                    correlation_id=corr,
                )
                await self._publish_to_dlq(
                    # If we failed to parse the message, we won't have a valid source topic in the metadata, so we fall back to using the broker's 
                    # queue name forlogging and DLQ purposes.
                    source_topic=str(msg.metadata.get("source_topic", self.queue_name)), 
                    original_message=msg, # Since we failed to parse the message, we pass the original Message object to the DLQ so that we can inspect it later 
                    error=exc, # We pass the exception object to the DLQ so that we can see the error details when we inspect the failed message later
                    retries=self._resolve_handler_max_retries(msg), # Shows the max no. of retries configured for the failes message
                )

        # Return the wrapper function that the adapter will call when a message arrives.
        return wrapper

    """
    This function allows users to register their message handler functions in a flexible way. They can either use it as a decorator (e.g., @broker.on_message) or
    call it directly with their handler function (e.g., broker.on_message(handle_message)). The method takes care of wrapping the user's handler in a function that 
    the underlying adapter can call when messages arrive, and it also caches this wrapper for efficiency. This design abstracts away the transport-specific details 
    and lets users focus on their business logic.
    """
    def on_message(
        self,
        handler: OnMessageHandler | None = None,
    ) -> OnMessageDecorator | Callable[[Message], Awaitable[None]]:
        """
        Register a message handler.

        Purpose:
            Attach a request handler for inbound packets.

        Args:
            handler: Optional handler function. If omitted, returns a decorator.

        Returns:
            Either a decorator or an adapter-facing async wrapper.

        Example:
        ```
            @broker.on_message
            async def handle(data: DataPacket) -> dict[str, bool]:
                return {"ok": True}
        ```
        """
        # If no handler is provided, we return a decorator function that the user can use to decorate their handler. 
        # This allows for a more natural and Pythonic way of registering handlers.
        if handler is None:
            # Define the decorator function that takes the user's handler function as an argument, sets it as the message handler, 
            # creates the wrapper, and returns the wrapper to be used by the adapter.
            def decorator(fn: OnMessageHandler) -> Callable[[Message], Awaitable[None]]:
                self._message_handler = fn # Set the user's handler function as the message handler for the broker
                self._message_wrapper = self._make_message_wrapper(fn) # Create adapter-facing wrapper for the user's handler function and cache it
                return self._message_wrapper # Return the wrapper function that the adapter will call when a message arrives

            # Return the decorator function so that users can use it with the @ syntax to register their message handlers in a clean and intuitive way.
            return cast(OnMessageDecorator, decorator)

        self._message_handler = handler # If the user provided a handler function directly, we set it as the message handler for the broker
        self._message_wrapper = self._make_message_wrapper(handler) # Create adapter-facing wrapper for the user's handler function and cache it
        return self._message_wrapper # Return the wrapper function that the adapter will call when a message arrives.

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
            # We parse the incoming Message into a ResponsePacket, which is the format that the user's reply handler expects.
            data = ResponsePacket.model_validate(msg.payload)

            self.log.info("Reply received", correlation_id=data.correlation_id)

            # Define the pending Future for this correlation_id if it exists. 
            # This allows the send_and_wait method to be unblocked and receive the response when it arrives.
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
        # Creates a DataPacket instance using the provided content, sender, reply_to, deliver_at, and correlation_id. 
        # If correlation_id is not provided, it generates a new unique ID using uuid4.
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
        # We then wrap this DataPacket in a Message object, which is the format that the underlying adapter expects for publishing. 
        # We also add a "submitted_at" timestamp to the metadata for observability purposes.
        message = Message(payload=packet.model_dump(mode="json"))
        message.metadata.setdefault("submitted_at", time.time())

        # Counter for measuring how long the publish operation takes, which can be useful for logging and monitoring. 
        # We start the timer right before we call the publish method on the adapter,
        started = time.perf_counter()

        # Handle delayed delivery if deliver_at is provided. 
        # We convert the datetime to a timestamp and pass it to the adapter's publish method, which should handle the scheduling.
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

        # Returns the correlation_id of the published packet, which can be used by the caller to track the message or correlate it with responses.
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
        ```
            @broker.on_reply
            async def handle_reply(reply: ResponsePacket) -> None:
                print(reply.status)
        ```
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
        """
        Start consuming message and reply topics until cancellation.
        
        This method subscribes to the main message topic and the reply topic (if not already subscribed) using the underlying adapter.

        How to use:
        ```
            1. Start the broker in the background
                broker_task = asyncio.create_task(broker.start())

            2. Run other application logic here while the broker is running...

            3. When ready to shut down, cancel the broker task
                ```
                broker_task.cancel()
                try:
                    await broker_task
                except asyncio.CancelledError:
                    pass
                ```
        ```
        """

        if self._subscriber is None:
            raise RuntimeError("Call connect() before start().")

        if self._message_wrapper is None:
            async def _noop_message(_msg: DataPacket) -> None:
                self.log.debug("No message handler registered; dropping message")

            # If the user hasn't registered a message handler, we set up a no-op handler that just logs the received message without doing anything with it.
            # This allows the broker to start and consume messages without errors, even if the user hasn't defined any business logic for handling messages yet. 
            # It also provides a clear log message to indicate that messages are being received but not processed.
            self._message_handler = _noop_message 
            self._message_wrapper = self._make_message_wrapper(_noop_message)

        if self._reply_wrapper is None:
            async def _noop_reply(_resp: ResponsePacket) -> None:
                self.log.debug("No reply handler registered; ignoring reply")

            self._reply_handler = _noop_reply
            self._reply_wrapper = self._make_reply_wrapper(_noop_reply)

        # We subscribe to the main message topic using the adapter, passing in the wrapper function that will be called when messages arrive.
        await self._subscriber.subscribe(self.queue_name, self._message_wrapper)
        await self._subscriber.subscribe(self.reply_queue, self._reply_wrapper)

        self.log.info("Broker started")

        try:
            # Tries to run indefinitely until a cancellation is requested. 
            # We store the current task in self._run_task so that we can check for cancellation in other parts of the code if needed.
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
    worrying about the underlying mechanics of awaiting. This function is needed in order to support both sync and async handlers seamlessly, 
    giving users the flexibility to choose the style that best fits their use case while ensuring that we can handle the results correctly in either case.
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
    Note: DLQs are special topics where messages that fail processing after all retries are exhausted can be sent for later analysis.
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
            Message(payload=failed.model_dump(mode="json")),
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
    Indempotency handling: These functions work together to ensure that if the same message is received multiple times (due to retries, redeliveries, etc.),
    we only process it once. The _begin_idempotent_processing function tries to reserve the message ID for processing, and if it fails, 
    it means another worker is already processing that message, so we skip it. The _complete_idempotent_processing function is called when processing 
    succeeds to mark the message as completed, and the _abort_idempotent_processing function is called when processing fails to clear the reservation so 
    that it can be retried later.
    """
    async def _begin_idempotent_processing(
        self,
        *,
        message_id: str,
        topic: str,
        message: Message,
    ) -> bool:
        """Reserve a message ID before executing a handler to avoid duplicates."""

        reserve = getattr(self.broker, "try_acquire_idempotency", None)
        if reserve is None:
            return True

        try:
            acquired = await reserve(message_id)
        except Exception as exc:
            self.log.warning(
                "Idempotency reservation failed; processing message",
                packet_id=message_id,
                error=str(exc),
            )
            return True

        if acquired:
            return True

        self.log.info("Duplicate message skipped", packet_id=message_id, topic=topic)
        await self._notify_handler(
            topic=topic,
            elapsed_ms=0.0,
            message=message,
            status="idempotency_skipped",
        )
        await self._notify_runtime_event(
            event="idempotency_skip",
            details={"message_id": message_id, "topic": topic},
        )
        return False

    # Once we've successfully processed a message, we call this function to mark the message ID as completed. This is important for the idempotency logic,
    # as it tells the broker that this message has been handled and should not be processed again
    async def _complete_idempotent_processing(self, message_id: str) -> None:
        """Mark a message ID as completed once handler processing succeeds."""

        complete = getattr(self.broker, "mark_idempotency_completed", None)
        if complete is None:
            return
        try:
            await complete(message_id)
        except Exception:
            self.log.exception("Failed to mark idempotency as completed", packet_id=message_id)
    
    # If processing fails and we want to allow the message to be retried later, we call this function to clear the processing marker for that message ID.
    # This way, the next time the same message is received, it will be treated as a new message and processed again. This is crucial for ensuring that 
    # transient errors don't cause messages to be permanently skipped.
    async def _abort_idempotent_processing(self, message_id: str) -> None:
        """Clear processing marker when execution fails and should remain retriable."""

        abort = getattr(self.broker, "clear_idempotency_processing", None)
        if abort is None:
            return
        try:
            await abort(message_id)
        except Exception:
            self.log.exception("Failed to clear idempotency marker", packet_id=message_id)

    # This method is designed to emit optional runtime events that observers can listen to if they have implemented a custom hook for it.
    # It iterates through all registered observers and checks if they have an "on_runtime_event" method. If they do, it calls that method with the event name and details.
    async def _notify_runtime_event(self, *, event: str, details: dict[str, object]) -> None:
        """Emit optional runtime events for observers that support custom hooks."""

        for observer in self._observers:
            callback = getattr(observer, "on_runtime_event", None)
            if callback is None:
                continue
            result = callback(event=event, details=details)
            if isawaitable(result):
                await result

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
