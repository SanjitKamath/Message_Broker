"""High-level broker wrapper for `lyik_messaging`."""

from __future__ import annotations

import asyncio
from re import M
import uuid
import warnings
from collections.abc import Awaitable, Callable
from datetime import datetime
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
    """
    FastStream-backed broker wrapper with strict message handlers.
    
    This class provides a high-level interface for connecting to a message broker,
    registering message handlers, and publishing messages. It abstracts away the underlying
    FastStream implementation details and offers features like middleware support, 
    retry policies, and request/response patterns with correlation IDs. The broker can be 
    used as an async context manager to ensure proper connection management or
    manually connected and started as needed.

    It supports both asynchronous message handlers and synchronous ones that return a '
    result directly. The broker will handle awaiting asynchronous handlers and applying middlewares
    around the publish and consume flows. The `connect` function provides a convenient way to
    create and connect a `MessageBroker` instance in one step, with optional auto-start of
    the broker's run loop.

    Args:
        `uri`: The URI of the message broker to connect to.
        `aes_key`: Optional AES key for encrypting messages.
        `auto_start`: Whether to automatically start the broker's run loop after connecting.
        `**kwargs`: Additional keyword arguments to pass to the `MessageBroker` constructor.
    
    Returns:
        An instance of `MessageBroker` that is connected and optionally started.

    Example:
        ```
        import asyncio
        from lyik_messaging import MessageBroker, Payload

        async def main():
            async with MessageBroker("amqp://localhost:5672") as broker:
                @broker.on_message("my_queue")
                async def handle_message(payload: Payload):
                    print(f"Received message: {payload}")

                await broker.send_message({"hello": "world"}, sender="test")

        asyncio.run(main())
        ```
    
    Note:
        - The `connect` function is a convenient helper for creating and connecting a `MessageBroker
            instance. It handles connection and optional auto-start of the run loop, but users can
            also choose to manage the lifecycle manually by instantiating `MessageBroker` directly
            and calling `connect` and `start` as needed.

        - Middleware support allows users to define custom logic that runs before publishing messages
            and after consuming messages, with configurable retry policies for handling middleware errors.
    """

    def __init__(
        self,
        uri: str,
        queue_name: str = "default_queue",
        *,
        aes_key: str | None = None,
        **context_options: object,
    ) -> None:
        # Strip whitespace and validate URI early to fail fast on misconfiguration
        self._uri = uri.strip()

        # URI validation is deferred to the underlying FastStream broker, but we can catch common issues here
        if not self._uri:
            raise ConfigurationError(
                "Broker URI is empty. Provide a URI like amqp://... or redis://..."
            )

        # Queue name is used for strict handler registration and message correlation.
        self.queue_name = queue_name

        # Generate a unique reply queue name for internal request/reply flows to avoid collisions.
        self.reply_queue = f"reply_queue_{uuid.uuid4().hex}"

        # Store context options for middleware and retry policy configuration, with validation.
        self._context_options = dict(context_options)

        # Coerce and validate middleware and retry policy settings from context options.
        # Ensure that invalid values are handled gracefully with defaults and clear error messages for misconfiguration.
        self._processing_timeout_ms = _coerce_optional_positive_int(
            self._context_options.get("processing_timeout_ms")
        )

        # Configure retry policy for middleware operations with sensible defaults and validation.
        # Ensure that invalid values for retry settings are handled gracefully and do not cause issues during runtime.
        self._retry_policy = RetryPolicy(
            max_retries=_coerce_non_negative_int(self._context_options.get("handler_max_retries"), 0),
            base_delay_ms=_coerce_positive_int(self._context_options.get("retry_base_delay_ms"), 100),
            max_delay_ms=_coerce_optional_positive_int(self._context_options.get("retry_max_delay_ms")),
            jitter=bool(self._context_options.get("retry_jitter", False)),
        )

        # Validate and initialize middlewares from context options, ensuring they are proper Middleware instances.
        middleware_option = self._context_options.get("middlewares", [])
        if not isinstance(middleware_option, list):
            raise ConfigurationError(
                "Invalid 'middlewares' option: expected list[Middleware], "
                f"got {type(middleware_option).__name__}. Example: middlewares=[MyMiddleware()]."
            )
        
        # Validate each middleware entry and store them in a list for later use in publish/consume flows.
        self._middlewares: list[Middleware] = []

        # Validate that each middleware is an instance of the Middleware base class and provide
        # clear error messages for misconfiguration.
        for middleware in middleware_option:
            if not isinstance(middleware, Middleware):
                raise ConfigurationError(
                    "Invalid middleware entry: expected Middleware instance, "
                    f"got {type(middleware).__name__}."
                )
            self._middlewares.append(middleware)

        # Normalize AES key if provided, ensuring it meets expected format and length requirements for encryption.
        self._aes_key = normalize_aes_key(aes_key) if aes_key is not None else None

        # Internal state for broker lifecycle, handler registrations, and pending responses.
        self._broker: FastStreamBroker | None = None

        # Scheme is extracted from URI for internal use in routing and topic management, but validation is deferred to FastStream.
        self._scheme: str | None = None

        # Task for the broker's run loop, used for managing lifecycle when auto_start is enabled or when run() is called directly.  
        self._run_task: asyncio.Task[None] | None = None

        # Internal registries for strict message handlers, reply handlers, and tracking of registered and used 
        # queues for validation and error handling.
        self._strict_registrations: list[tuple[str, Callable[..., object]]] = []

        # Reply handler and wrapper are used for internal request/reply flows to process responses and correlate them 
        # with pending requests.
        self._reply_handler: ReplyHandler | None = None

        # The reply wrapper is a middleware-like mechanism to ensure that reply handlers are invoked for messages
        # that are responses, even if they don't match a strict queue handler. This allows the broker to handle 
        # request/reply patterns without requiring users to set up explicit handlers for the internal reply queue.
        self._reply_wrapper: Callable[..., Awaitable[None]] | None = None

        # Track registered queues for strict handlers and used queues for sending messages to provide warnings and validation.
        self._registered_queues: set[str] = set()

        # Track queues that are involved in request/reply patterns to validate that they have strict handlers registered, 
        # since request/reply relies on receiving messages and correlating responses.
        self._used_queues: set[str] = set()

        # Track queues that are used for request/reply patterns to ensure they have strict handlers registered, 
        # since request/reply relies on receiving messages and correlating responses.
        self._request_reply_queues: set[str] = set()

        # Pending responses are stored in a dictionary keyed by correlation ID, allowing the broker to resolve 
        # waiting requests when responses are received.
        self._pending_responses: dict[str, asyncio.Future[ResponsePacket]] = {}

        # Keep orchestration thin: lifecycle/publish/consume concerns live in services.
        self._core = BrokerCore(self)

        # Publisher and consumer services handle the details of message publishing and consumption,
        # allowing the main broker class to focus on orchestration and providing a clean API.
        self._publisher = PublisherService(self)

        # Consumer service manages message handlers, including strict handlers for specific queues and the 
        # reply handler for request/reply patterns.
        self._consumer = ConsumerService(self)

    async def connect(self) -> None:
        """
        Create and connect the underlying FastStream broker.
        
        This method initializes the FastStream broker instance based on the provided URI and context options,
        and establishes a connection to the message broker. It also handles any necessary setup for encryption
        if an AES key is provided. This method must be called before starting the broker's run
        loop or publishing messages. If the connection fails, a `ConnectionLostError` will be raised with details 
        about the failure.

        Raises:
            `ConnectionLostError`: If the broker fails to connect to the specified URI, with details about the error.
            `EncryptionError`: If there is an issue with the provided AES key for encryption, such as invalid format or length.
            `ConfigurationError`: If there are issues with the provided context options that prevent successful connection.

        Example:
            ```
            import asyncio
            from lyik_messaging import MessageBroker    
            async def main():
                broker = MessageBroker("amqp://localhost:5672", aes_key="mysecretkey123")
                await broker.connect()
            asyncio.run(main())
            ```
        """
        # Delegate connection logic to the BrokerCore, which will handle the details of initializing and connecting 
        # the FastStream broker.
        await self._core.connect()

    async def start(self) -> None:
        """
        Start subscriptions and keep processing until cancelled.
        
        This method starts the broker's run loop, which begins processing incoming messages and invoking registered
        handlers. It should be called after `connect` to begin consuming messages. The run loop will continue running 
        until the broker is disconnected or the task is cancelled. If there are issues during startup or while running, 
        a `MessageBrokerError` will be raised with details about the failure.

        Example:
            ```
            import asyncio
            from lyik_messaging import MessageBroker    
            async def main():
                broker = MessageBroker("amqp://localhost:5672")
                await broker.connect()
                await broker.start()
            asyncio.run(main())
            ```
        """
        # Delegate the run loop logic to the BrokerCore, which will manage subscriptions and message processing.
        # This keeps the main broker class focused on orchestration and allows the core to handle the details of the run loop.
        await self._core.start()

    async def run(self) -> None:
        """
        Connect and start broker lifecycle in one call.
        
        This method is a convenience function that combines the connection and startup of the broker into a single call.
        It first calls `connect` to establish a connection to the message broker, and then starts the run loop to begin 
        processing messages. 

        If there are any issues during connection or startup, a `MessageBrokerError` will be raised with details about the failure.

        Example:
            ```
            import asyncio
            from lyik_messaging import MessageBroker    
            async def main():
                broker = MessageBroker("amqp://localhost:5672")
                await broker.run()
            asyncio.run(main())
            ```
        """
        # Delegate the combined connect and start logic to the BrokerCore, which will handle the details of both operations.
        await self._core.run()

    async def disconnect(self) -> None:
        """
        Stop and release the underlying FastStream broker.
        
        This method gracefully shuts down the broker's run loop and releases any resources associated with the connection to 
        the message broker. It should be called when the broker is no longer needed to ensure proper cleanup. If there are 
        issues during disconnection, a `MessageBrokerError` will be raised with details about the failure.

        Example:
            ```
            import asyncio
            from lyik_messaging import MessageBroker    
            async def main():
                broker = MessageBroker("amqp://localhost:5672") 
                await broker.connect()
                # ... use the broker ...
                await broker.disconnect()
            asyncio.run(main())
            ```
        """
        # Delegate disconnection logic to the BrokerCore, which will handle the details of stopping the run loop and 
        # releasing resources.
        await self._core.disconnect()

    async def __aenter__(self) -> "MessageBroker":
        """
        This allows the broker to be used as an async context manager, automatically connecting on entry and disconnecting on exit.
        Example:
            ```
            import asyncio
            from lyik_messaging import MessageBroker    
            async def main():
                async with MessageBroker("amqp://localhost:5672") as broker:
                    # ... use the broker ...
            asyncio.run(main())
            ```
        """
        # Connect the broker when entering the context and return the instance for use within the block. 
        # The broker will be automatically disconnected when exiting the block.
        # Return self to allow usage of the broker instance within the context block.
        await self.connect()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        """
        Ensure the broker is properly disconnected when exiting an async context, even if exceptions occur.
        This method is called when exiting an async context block, and it ensures that the broker is properly disconnected to
        clean up resources. If an exception occurred within the context block, it will still attempt to disconnect 
        the broker, and any exceptions raised during disconnection will be logged but not re-raised to avoid masking 
        the original exception.
        """
        # Attempt to disconnect the broker, and log any exceptions that occur during disconnection without re-raising them.
        try:
            await self.disconnect()
        except Exception as e:
            return MessageBrokerError(f"An error occurred while disconnecting the broker during context exit. Error" \
            " details: {e}")
        
    def on_message(self, queue: str) -> Callable[[OnMessageHandler], OnMessageHandler]:
        """
        Register a strict handler using `@broker.on_message("queue")`.
        
        This method is a decorator factory that allows users to register message handlers for specific queues using a simple
        decorator syntax. The `queue` parameter specifies the name of the queue that the handler will be associated with. 
        The decorated function must be an async function that accepts a `Payload` and returns either a `Payload` or `None`. 
        The broker will handle awaiting the handler and applying any middlewares around the handler execution. 
        If the handler is not an async function or does not have the correct signature, a `ConfigurationError` will be
        raised with details about the expected handler format.

        Args:
            `queue`: The name of the queue that the handler will be associated with. This is used for routing messages to the correct handler.
        
        Returns:
            A decorator that can be applied to an async function to register it as a message handler for the specified queue.

        Raises:
            - `ConfigurationError`: If the decorated handler does not meet the expected signature requirements, 
                                  such as not being an async function or having incorrect parameters.
            
        
        Example:
            ```
            from lyik_messaging import MessageBroker, Payload
            broker = MessageBroker("amqp://localhost:5672")
            
            @broker.on_message("my_queue")
            async def handle_message(payload: Payload):
                print(f"Received message: {payload}")
            ```
        """
        # Validate that the queue name is a non-empty string to ensure proper handler registration and avoid misconfiguration.
        if not isinstance(queue, str):
            raise ConfigurationError(
                "Invalid on_message usage. Use @broker.on_message(\"queue\") with a string queue name."
            )
        # Strip whitespace from the queue name and validate that it is not empty to prevent registration of handlers
        # with invalid queue names.
        normalized_queue = queue.strip()

        # Fail fast if the normalized queue name is empty, as this would lead to issues with handler registration and 
        # message routing.
        if not normalized_queue:
            raise ConfigurationError("Queue name must not be empty for @broker.on_message(\"queue\").")
        
        # Delegate the actual registration logic to a helper function that builds the decorator, passing the normalized queue name.
        return build_on_message_decorator(self, normalized_queue)

    def on_reply(
        self,
        handler: ReplyHandler | None = None,
    ) -> Callable[[ReplyHandler], Callable[..., Awaitable[None]]] | Callable[..., Awaitable[None]]:
        """
        Register a reply handler for `ResponsePacket` payloads.
        
        This method allows users to register a handler that will be invoked for messages that are responses in 
        request/reply patterns. The handler must be an async function that accepts a `ResponsePacket` and returns 
        `None`. The broker will handle invoking this handler for messages that are identified as responses, even 
        if they don't match a strict queue handler. This allows the broker to support request/reply patterns without
        requiring users to set up explicit handlers for the internal reply queue.
        """
        # If a handler is provided directly, register it immediately. 
        # Otherwise, return a decorator that can be used to register the handler.
        if handler is not None:
            return self._register_reply_handler(handler)
        # If no handler is provided, return a decorator that can be used to register the handler when the function is defined.
        def decorator(fn: ReplyHandler) -> Callable[..., Awaitable[None]]:
            return self._register_reply_handler(fn)
        # Return the decorator function for use when the handler is defined.
        return decorator

    async def send_message(
        self,
        content: Payload,
        sender: str,
        reply: bool = False,
        deliver_at: datetime | None = None,
    ) -> str:
        """
        Fire-and-forget publish API with optional delayed delivery.
        
        This method allows users to publish a message to the broker without waiting for a response. The `content` parameter
        is the payload of the message, and `sender` is a string identifier for the sender of the message. The `reply` parameter 
        indicates whether the message expects a reply, which can be used for tracking and validation purposes. 
        The `deliver_at` parameter allows for optional delayed delivery of the message, where the message will not be published 
        until the specified datetime is reached. If there are issues with the provided parameters, such as invalid content or 
        sender, a `ConfigurationError` will be raised with details about the expected format.

        Args:
            `content`: The payload of the message to be published, which can be any serializable object.
            `sender`: A string identifier for the sender of the message, used for tracking and validation purposes.
            `reply`: A boolean indicating whether the message expects a reply, which can be used for tracking and validation purposes.
            `deliver_at`: An optional datetime indicating when the message should be published. If provided, the message will not be published until the specified datetime is reached.
        
        Returns:
            A string identifier for the published message, which can be used for tracking and correlation purposes.
        
        Example:
            ```
            import asyncio
            from datetime import datetime, timedelta
            from lyik_messaging import MessageBroker, Payload    
            async def main():               
                broker = MessageBroker("amqp://localhost:5672") 
                await broker.connect()

                # Publish a message to be delivered immediately
                message_id = await broker.send_message({"hello": "world"}, sender="test")
                print(f"Published message with ID: {message_id}")

                # Publish a message to be delivered in 10 seconds
                future_time = datetime.now() + timedelta(seconds=10)
                delayed_message_id = await broker.send_message({"delayed": "message"}, sender="test", deliver_at=future_time)
                print(f"Published delayed message with ID: {delayed_message_id}")
            asyncio.run(main())
            ```
        """
        # Track the queue associated with this message for validation and warning purposes. 
        # If the message is being sent to a queue that has not had a strict handler registered,
        # a warning will be issued to alert the user of potential misconfiguration.
        self._used_queues.add(self.queue_name)

        # If the message is marked as expecting a reply, track the queue for request/reply validation to ensure 
        # that it has a strict handler registered.
        if self.queue_name not in self._registered_queues:
            # Warn if sending to a queue with no registered handler, as this may indicate a misconfiguration or oversight.
            warnings.warn(
                f"Sending to queue '{self.queue_name}' with no local handler registered. "
                "If this is intentional, ignore this warning.",
                RuntimeWarning,
                stacklevel=2,
            )
        # Delegate the actual sending logic to the PublisherService, which will handle applying middlewares, 
        # managing correlation IDs, and publishing the message.
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
        """
        Request/response API that returns a correlated `ResponsePacket`.
        
        This method allows users to publish a message and wait for a correlated response. The `content` parameter is 
        the payload of the message, and `sender` is a string identifier for the sender of the message. The `timeout` 
        parameter specifies how long to wait for a response before giving up, and the `deliver_at` parameter allows 
        for optional delayed delivery of the message. The method will return a `ResponsePacket` that is correlated 
        with the original request, or raise a `MessageBrokerError` if the timeout is reached without receiving a 
        response or if there are issues with the provided parameters.

        Args:
            `content`: The payload of the message to be published, which can be any serializable object.
            `sender`: A string identifier for the sender of the message, used for tracking and validation purposes.
            `timeout`: The maximum time to wait for a response before giving up.
            `deliver_at`: An optional datetime to specify when the message should be delivered.

        Returns:
            A `ResponsePacket` that is correlated with the original request, containing the response payload and metadata.
        
        Example:
            ```
            import asyncio
            from datetime import datetime, timedelta
            from lyik_messaging import MessageBroker, Payload
            async def main():
                broker = MessageBroker("amqp://localhost:5672")
                await broker.connect()
                try:
                    response = await broker.send_and_wait(
                        {"request": "data"},
                        sender="test",
                        timeout=5.0,
                        deliver_at=datetime.now() + timedelta(seconds=2)
                    )
                    print(f"Received response: {response.payload}")
                except MessageBrokerError as e:
                    print(f"Failed to receive response: {e}")
            asyncio.run(main())
            ```
        """
        # Validate that a timeout is provided and is greater than 0 to prevent indefinite blocking, 
        # as waiting for a response without a timeout can lead to issues if the response is never received.
        if timeout is None:
            raise ConfigurationError("send_and_wait requires a timeout to prevent indefinite blocking.")
        # Validate that the timeout is greater than 0 to ensure that the method behaves as expected and does 
        # not allow for invalid timeout values.
        if timeout <= 0:
            raise ConfigurationError("send_and_wait timeout must be greater than 0 seconds.")

        # Track the queue associated with this message for validation and warning purposes. 
        # If the message is being sent to a queue that has not had a strict handler registered, 
        # a warning will be issued to alert the user of potential misconfiguration.
        self._used_queues.add(self.queue_name)

        # If this message is part of a request/reply pattern, track the queue for validation to ensure that 
        # it has a strict handler registered.
        self._request_reply_queues.add(self.queue_name)

        # Validate that the queue has a registered handler, as request/reply relies on receiving messages 
        # and correlating responses.
        self._validate_request_reply_queues()

        # Delegate the actual send and wait logic to the PublisherService, which will handle applying middlewares, managing
        # correlation IDs, publishing the message, and waiting for the correlated response.
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
        """
        Request/response API that stores the response and returns a response id.
        
        This method allows users to publish a message and have the correlated response stored by the broker, 
        returning a response ID that can be used to retrieve the response later. The `content` parameter is 
        the payload of the message, and `sender` is a string identifier for the sender of the message. 
        The `timeout` parameter specifies how long to wait for a response before giving up, and the `deliver_at` 
        parameter allows for optional delayed delivery of the message. The method will return a response ID that 
        can be used with `get_response` to retrieve the correlated response, or raise a `MessageBrokerError` if 
        the timeout is reached without receiving a response or if there are issues with the provided parameters.

        Args:
            `content`: The payload of the message to be published, which can be any serializable object.
            `sender`: A string identifier for the sender of the message, used for tracking and validation purposes.
            `timeout`: The maximum time to wait for a response before giving up.
            `deliver_at`: An optional datetime to specify when the message should be delivered.
        
        Returns:
            A string response ID that can be used with `get_response` to retrieve the correlated response later.

        Example:
            ```
            import asyncio
            from datetime import datetime, timedelta
            from lyik_messaging import MessageBroker, Payload
            async def main():
                broker = MessageBroker("amqp://localhost:5672")
                await broker.connect()
                try:
                    response_id = await broker.send_and_store(
                        {"request": "data"},
                        sender="test",
                        timeout=5.0,
                        deliver_at=datetime.now() + timedelta(seconds=2)
                    )
                    print(f"Message sent with response ID: {response_id}")
                    # ... do other work ...
                    response = await broker.get_response(response_id)
                    if response:
                        print(f"Retrieved response: {response.payload}")
                    else:
                        print("Response not available yet.")
                except MessageBrokerError as e:
                    print(f"Failed to send message and store response: {e}")
            asyncio.run(main())
            ```
        """
        # Validate that a timeout is provided and is greater than 0 to prevent indefinite blocking, 
        # as waiting for a response without a timeout can lead to issues if the response is never received.
        if timeout is None:
            raise ConfigurationError("send_and_store requires a timeout to prevent indefinite blocking.")
        
        # Validate that the timeout is greater than 0 to ensure that the method behaves as expected and does 
        # not allow for invalid timeout values.
        if timeout <= 0:
            raise ConfigurationError("send_and_store timeout must be greater than 0 seconds.")
        
        # Track the queue associated with this message for validation and warning purposes.
        self._used_queues.add(self.queue_name)
        # If this message is part of a request/reply pattern, track the queue for validation to ensure that
        # the reply queue is properly subscribed to receive responses.
        self._request_reply_queues.add(self.queue_name)
        # Validate that the request/reply queues have registered handlers, as request/reply relies on 
        # receiving messages and correlating responses.
        self._validate_request_reply_queues()

        # Delegate the actual send and store logic to the PublisherService, which will handle applying middlewares, managing
        # correlation IDs, publishing the message, waiting for the correlated response, and storing the response
        # with an associated response ID for later retrieval.
        return await self._publisher.send_and_store(
            content=content,
            sender=sender,
            timeout=timeout,
            deliver_at=deliver_at,
        )

    async def get_response(self, response_id: str) -> ResponsePacket | None:
        """
        Retrieve a response previously stored with `send_and_store`.
        
        This method allows users to retrieve a response that was previously stored by the broker 
        using the `send_and_store` method. The `response_id` parameter is the string identifier that was returned 
        by `send_and_store` when the message was published.

        Args:
            `response_id`: The string response ID that was returned by `send_and_store` when the message was published.

        Returns:
            A `ResponsePacket` that is associated with the provided response ID, or `None` if the response is not available 
            or has expired. The `ResponsePacket` contains the response payload and metadata such as correlation ID 
            and timestamps.

        Example:
            ```
            import asyncio
            from datetime import datetime, timedelta
            from lyik_messaging import MessageBroker, Payload
            async def main():
                broker = MessageBroker("amqp://localhost:5672")
                await broker.connect()
                try:
                    response_id = await broker.send_and_store(
                        {"request": "data"},
                        sender="test",
                        timeout=5.0,
                        deliver_at=datetime.now() + timedelta(seconds=2)
                    )
                    print(f"Message sent with response ID: {response_id}")
                    # ... do other work ...
                    response = await broker.get_response(response_id)
                    if response:
                        print(f"Retrieved response: {response.payload}")
                    else:
                        print("Response not available yet.")
                except MessageBrokerError as e:
                    print(f"Failed to send message and store response: {e}")
            asyncio.run(main())
            ```
        """
        # Delegate the retrieval logic to the PublisherService, which manages the storage of responses and can return the
        # associated ResponsePacket for the given response ID, or None if it is not available.
        return await self._publisher.get_response(response_id)

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
        """
        Low-level publish API for advanced/internal use cases.
        
        This method provides a low-level API for publishing messages with advanced options such as custom headers, 
        correlation IDs, and reply-to addresses. It is intended for internal use and advanced use cases where the 
        higher-level `send_message` and `send_and_wait` methods do not provide sufficient control. The `topic` 
        parameter specifies the topic to publish to, and the `payload` is the message content. The optional `headers` 
        parameter allows for custom metadata to be included with the message, while `correlation_id` and `reply_to` 
        can be used for request/reply patterns. The `is_response` flag indicates whether the message being published
        is a response in a request/reply flow, which can affect how middlewares are applied and how the message is 
        processed by the broker. If there are issues with the provided parameters, a `ConfigurationError` will be 
        raised with details about the expected format.

        Args:
            `topic`: The topic to publish the message to, which can be used for routing and subscription purposes.
            `payload`: The content of the message to be published, which can be any serializable object.
            `headers`: Optional custom headers to include with the message, provided as a dictionary of string key-value pairs.
            `correlation_id`: An optional string identifier used for correlating messages in request/reply patterns.
            `reply_to`: An optional string specifying the topic or queue to which replies should be sent in request/reply patterns.
            `is_response`: A boolean flag indicating whether the message being published is a response in a request/reply flow, which can affect
            how middlewares are applied and how the message is processed by the broker.

        Returns:
            A string identifier for the published message, which can be used for tracking and correlation purposes.

        Example:
            ```
            import asyncio
            from lyik_messaging import MessageBroker, Payload
            async def main():
                broker = MessageBroker("amqp://localhost:5672")
                await broker.connect()
                message_id = await broker.publish(
                    topic="my.topic",
                    payload={"key": "value"},
                    headers={"header1": "value1"},
                    correlation_id="12345",
                    reply_to="reply.topic",
                    is_response=False
                )
                print(f"Published message with ID: {message_id}")
            asyncio.run(main())
            ```
        """
        # Delegate the actual publish logic to the PublisherService, which will handle applying middlewares, 
        # managing correlation IDs, and publishing the message.
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
        """
        Register a message handler for the specified queue.
        This internal method is used to register a message handler for a specific queue. It is called by the decorator
        factory created by the `on_message` method. The `queue` parameter specifies the name of the queue that the 
        handler will be associated with, `payload_model` is the Pydantic model that defines the expected structure of 
        the message payload, and `handler` is the async function that will be invoked when a message is received on the 
        specified queue. The method delegates the actual registration logic to the ConsumerService, which manages the 
        mapping of queues to handlers and ensures that messages are processed correctly when received. If there are 
        issues with the provided parameters, such as invalid queue names or handler formats, a `ConfigurationError` 
        will be raised with details about the expected format.

        Args:
            `queue`: The name of the queue to which the handler will be associated.
            `payload_model`: The Pydantic model that defines the expected structure of the message payload.
            `handler`: The async function that will be invoked when a message is received on the specified queue.

        """
        # Delegate the registration of the message handler to the ConsumerService, which will manage the mapping of 
        # queues to handlers and ensure that messages are processed correctly when received on the specified queue.
        self._consumer.register_message_handler(
            queue=queue,
            payload_model=payload_model,
            handler=handler,
        )

    def _register_reply_handler(
        self,
        handler: ReplyHandler,
    ) -> Callable[..., Awaitable[None]]:
        """
        This internal method registers a reply handler for processing `ResponsePacket` messages in request/reply patterns.
        The `handler` parameter is an async function that accepts a `ResponsePacket` and returns `None`. This method is used
        to set up the handler that will be invoked for messages that are identified as responses, allowing the broker to
        support request/reply patterns without requiring users to set up explicit handlers for the internal reply queue
        """
        return self._consumer.register_reply_handler(handler)

    def _ensure_reply_subscription(self) -> None:
        """
        Ensure reply queue subscription exists for internal request/reply flows.
        
        This method checks if a reply handler has been registered, and if not, it registers a default no-op handler to 
        ensure that there is always a subscription for the internal reply queue used in request/reply patterns. This 
        allows the broker to properly route response messages even if the user has not explicitly registered a reply 
        handler, preventing issues with unhandled messages in request/reply flows.
        """
        if self._reply_wrapper is None:
            self._register_reply_handler(_noop_reply_handler)

    def _create_pending_response_waiter(self, correlation_id: str) -> asyncio.Future[ResponsePacket]:
        """
        Create a pending waiter keyed by request correlation id.
        
        This method creates an `asyncio.Future` that is stored in the `_pending_responses` dictionary, keyed by the provided
        correlation ID. This future will be used to wait for a response that matches the correlation ID in request/reply 
        patterns. When a response is received, the broker will look up the future using the correlation ID and set its result 
        to the received response, allowing the original sender to await the response asynchronously. If there are issues with 
        the provided correlation ID, such as it being empty or not a string, a `ConfigurationError` will be raised with 
        details about the expected format.
        """
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
        # Delegate the coercion of the response packet to the PublisherService, which will create a ResponsePacket instance
        # with the appropriate metadata based on the provided payload, correlation ID, and in_response_to values. 
        # This allows the broker to maintain a consistent format for response messages and ensures that all necessary 
        # metadata is included.
        return self._publisher._coerce_response_packet(
            payload,
            correlation_id=correlation_id,
            in_response_to=in_response_to,
        )

    async def _resolve_with_timeout(
        self,
        maybe_result: MaybeAwaitable[Payload | None],
    ) -> Payload | None:
        
        # If no processing timeout is configured, simply resolve the result without applying a timeout.
        if self._processing_timeout_ms is None:
            return await _resolve_result(maybe_result)

        # If a processing timeout is configured, use asyncio.wait_for to apply the timeout to the resolution of the result.
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
        """
        This method applies the registered middlewares in sequence before publishing a message. 
        It iterates through each middleware and calls the appropriate method based on whether the message is a 
        response or not. If any middleware raises an exception, it will retry according to the configured retry 
        policy, and if the maximum number of retries is exceeded, it will raise a `PublishFailedError` with 
        details about which middleware failed and on which topic. This ensures that transient issues in middlewares 
        can be retried while also providing clear error reporting when failures occur.

        Args:
            `topic`: The topic to which the message is being published, used for logging and error reporting purposes.
            `message`: The message object that is being processed by the middlewares before publishing.
            `is_response`: A boolean flag indicating whether the message being published is a response in a request/reply 
                flow, which determines which middleware method to call (before_publish_response vs before_publish).
        """

        current = message
        """
        Iterate through each registered middleware and apply it to the message before publishing. 
        For each middleware, if an exception occurs, it will retry according to the configured retry policy. 
        If the maximum number of retries is exceeded, a PublishFailedError will be raised with details about the failure. 
        This allows for robust handling of middleware failures while providing clear error reporting when issues arise.
        """
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
        """
        This method applies the registered middlewares in sequence after consuming a message. 
        It iterates through each middleware and calls the appropriate method based on whether the message is a
        response or not. If any middleware raises an exception, it will retry according to the configured retry
        policy, and if the maximum number of retries is exceeded, it will raise a `MessageBrokerError` with
        details about which middleware failed and on which topic. This ensures that transient issues in middlewares
        can be retried while also providing clear error reporting when failures occur during message consumption.

        Args:
            `topic`: The topic from which the message was consumed, used for logging and error reporting purposes.
            `message`: The message object that is being processed by the middlewares after consumption.
            `is_response`: A boolean flag indicating whether the message being consumed is a response in a request/reply
                flow, which determines which middleware method to call (after_consume_response vs after_consume).
        """

        current = message
        """
        Iterate through each registered middleware and apply it to the message after consuming. 
        For each middleware, if an exception occurs, it will retry according to the configured retry policy. 
        If the maximum number of retries is exceeded, a MessageBrokerError will be raised with details about the failure. 
        This allows for robust handling of middleware failures while providing clear error reporting
        when issues arise during message consumption.
        """
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

async def _resolve_result(maybe_result: MaybeAwaitable[Payload | None]) -> Payload | None:
    """Resolve a value that may be awaitable, returning the final payload."""
    if isawaitable(maybe_result):
        return await cast("Awaitable[Payload | None]", maybe_result)
    return cast("Payload | None", maybe_result)


def _coerce_positive_int(value: object, default: int) -> int:
    """Coerce a value to a positive integer, returning a default if invalid."""
    if not isinstance(value, int) or value <= 0:
        return default
    return value


def _coerce_non_negative_int(value: object, default: int) -> int:
    """Coerce a value to a non-negative integer, returning a default if invalid."""
    if not isinstance(value, int) or value < 0:
        return default
    return value


def _coerce_optional_positive_int(value: object) -> int | None:
    """Coerce a value to an optional positive integer, returning None if invalid."""
    if value is None:
        return None
    if not isinstance(value, int) or value <= 0:
        return None
    return value


async def _noop_reply_handler(_response: ResponsePacket) -> None:
    """A no-op reply handler to ensure reply subscription exists for request/reply patterns."""
    return None
