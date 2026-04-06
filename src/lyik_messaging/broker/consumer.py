"""Consumer registration and strict handler execution pipeline."""

from __future__ import annotations

import asyncio
from inspect import isawaitable
from typing import TYPE_CHECKING, cast

from cryptography.exceptions import InvalidTag
from faststream.message import StreamMessage
from faststream.rabbit import RabbitBroker
from faststream.redis import RedisBroker
from pydantic import BaseModel

from ..exceptions import ConfigurationError, EncryptionError, MessageBrokerError
from ..models import DataPacket, Message, MessageInfo, ResponsePacket

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from . import MessageBroker
    from .internal_types import FastStreamBroker, ReplyHandler, StrictMessageHandler

class ConsumerService:
    """
    Encapsulates strict consumer registration and execution.
    
    This service manages the registration of strict message handlers for specific queues, ensuring that only one handler is 
    registered per queue. It also provides a mechanism for registering a reply handler for processing responses on the
    broker's reply queue. The service handles the attachment of registered handlers to the broker when it is initialized 
    and provides wrapper functions that implement the strict execution pipeline, including payload decoding, middleware 
    application, and retry logic for user handlers.
    """

    def __init__(self, broker_owner: "MessageBroker") -> None:
        self._owner = broker_owner

    def register_message_handler(
        self,
        *,
        queue: str,
        payload_model: type[BaseModel],
        handler: "StrictMessageHandler",
    ) -> None:
        """
        Register a strict `(payload, info)` handler for a queue.
        
        Each queue can only have one strict handler, and attempts to register multiple handlers for the same queue will raise a
        `ConfigurationError`. The handler will be wrapped in a function that implements the strict execution pipeline, which 
        includes:
        1. Decoding the incoming message and validating it against the declared `payload_model`.
        2. Applying any registered consume middlewares to the message before it reaches the user handler.
        3. Executing the user handler with the decoded payload and message info, with retry logic based on the configured `RetryPolicy`.
        4. Sending a response back to the `reply_to` address if it is provided in the incoming message, with the result of the handler execution
              or error information if an exception occurs.
    
        The wrapper function is automatically subscribed to the broker when it is initialized, and any subsequent 
        registrations will also be subscribed immediately if the broker is already running.
        """
        owner = self._owner
        if queue in owner._registered_queues:
            raise ConfigurationError(
                f"Handler already registered for queue '{queue}'. Only one handler is allowed."
            )

        wrapper = self._build_strict_wrapper(
            queue=queue,
            payload_model=payload_model,
            user_handler=handler,
        )
        owner._strict_registrations.append((queue, wrapper))
        owner._registered_queues.add(queue)

        broker = owner._broker
        if broker is not None:
            self._subscribe_raw(broker, queue, wrapper)

    def register_reply_handler(
        self,
        handler: "ReplyHandler",
    ) -> "Callable[..., Awaitable[None]]":
        """
        Register a reply callback on the broker reply queue.
        
        This method registers a handler function that will be called for any messages received on the broker's reply queue. 
        The handler is wrapped in a function that applies consume middlewares and normalizes the incoming message into a 
        `ResponsePacket` before invoking the user handler.

        The wrapper function is automatically subscribed to the broker's reply queue when it is initialized, and if the broker is
        already running, it will be subscribed immediately. The method returns the wrapper function for potential use in testing 
        or other contexts.

        """

        owner = self._owner
        wrapper = self._build_reply_wrapper(handler)
        owner._reply_handler = handler
        owner._reply_wrapper = wrapper

        broker = owner._broker
        if broker is not None:
            self._subscribe_raw(broker, owner.reply_queue, wrapper)

        return wrapper

    def attach_registrations(self, broker: "FastStreamBroker") -> None:
        """Attach all known subscriptions to a newly created broker."""

        owner = self._owner
        for queue, handler in owner._strict_registrations:
            self._subscribe_raw(broker, queue, handler)

        if owner._reply_wrapper is not None:
            self._subscribe_raw(broker, owner.reply_queue, owner._reply_wrapper)

    def _subscribe_raw(self, broker: "FastStreamBroker", queue: str, handler: "Callable[..., object]") -> None:
        if isinstance(broker, RabbitBroker):
            broker.subscriber(queue)(handler)
            return
        if isinstance(broker, RedisBroker):
            broker.subscriber(list=queue)(handler)

    def _build_strict_wrapper(
        self,
        *,
        queue: str,
        payload_model: type[BaseModel],
        user_handler: "StrictMessageHandler",
    ) -> "Callable[[object], Awaitable[object | None]]":
        owner = self._owner

        async def wrapper(message: object) -> object | None:
            # Decode and validate the transport envelope exactly once.
            # Decoding/parsing failures are intentionally not retried.
            try:
                if isinstance(message, StreamMessage):
                    decoded_payload = await message.decode()
                    packet = DataPacket.model_validate(decoded_payload)
                    headers = message.headers
                else:
                    packet = DataPacket.model_validate(message)
                    headers = {}
            except (EncryptionError, InvalidTag) as exc:
                raise EncryptionError(
                    "Decryption failed: invalid authentication tag. "
                    "Ensure both producer and consumer use the same AES key."
                ) from exc

            # Convert the user payload into the declared pydantic model.
            payload = payload_model.model_validate(packet.content)
            info = MessageInfo(
                correlation_id=packet.correlation_id,
                sender=packet.sender,
                reply_to=packet.reply_to,
                raw=packet.model_dump(mode="json"),
            )

            try:
                await owner._apply_consume_middlewares(
                    queue,
                    Message(
                        payload=packet.model_dump(mode="json"),
                        headers=_normalize_headers(headers),
                        metadata={"source_topic": queue, "packet_type": "request"},
                        correlation_id=packet.correlation_id,
                    ),
                    is_response=False,
                )
            except (EncryptionError, InvalidTag):
                if packet.reply_to is not None:
                    await owner._send_response(
                        packet.reply_to,
                        ResponsePacket(
                            correlation_id=packet.correlation_id,
                            in_response_to=packet.id,
                            status="failed",
                            content={
                                "error": "Decryption failed",
                                "reason": "Invalid authentication tag or corrupted encrypted payload.",
                                "action": "Check AES key and message integrity on producer/consumer.",
                            },
                        ),
                    )
                return None
            except Exception as exc:
                if packet.reply_to is not None:
                    await owner._send_response(
                        packet.reply_to,
                        ResponsePacket(
                            correlation_id=packet.correlation_id,
                            in_response_to=packet.id,
                            status="failed",
                            content={
                                "error": "Middleware execution failed",
                                "reason": str(exc),
                                "action": "Check middleware logic and retry configuration.",
                            },
                        ),
                    )
                    return None
                raise MessageBrokerError(
                    "Consume middleware failed and no reply_to queue was available for failure reporting. "
                    "Check middleware logic and retry configuration."
                ) from exc

            attempt = 0
            while True:
                try:
                    # Retry only user handler execution with exponential backoff.
                    maybe_result = user_handler(payload, info)
                    result = await owner._resolve_with_timeout(maybe_result)

                    # Keep response envelope behavior compatible with existing DataPacket flows.
                    if packet.reply_to is not None:
                        await owner._send_response(
                            packet.reply_to,
                            ResponsePacket(
                                correlation_id=packet.correlation_id,
                                in_response_to=packet.id,
                                status="processed",
                                content=result,
                            ),
                        )

                    return result
                except Exception as exc:
                    if attempt < owner._retry_policy.max_retries:
                        retry_index = attempt
                        attempt += 1
                        await asyncio.sleep(owner._retry_policy.compute_delay_seconds(retry_index))
                        continue

                    # Final failure still produces a correlated failed response when reply_to exists.
                    if packet.reply_to is not None:
                        await owner._send_response(
                            packet.reply_to,
                            ResponsePacket(
                                correlation_id=packet.correlation_id,
                                in_response_to=packet.id,
                                status="failed",
                                content={"error": str(exc)},
                            ),
                        )
                    raise

        wrapper.__annotations__ = {"message": object}
        return wrapper

    def _build_reply_wrapper(
        self,
        user_handler: "ReplyHandler",
    ) -> "Callable[..., Awaitable[None]]":
        owner = self._owner

        async def wrapper(message: object) -> None:
            if isinstance(message, StreamMessage):
                payload = await message.decode()
                correlation_id = message.correlation_id
                message_id = message.message_id
                headers = message.headers
            else:
                payload = message
                correlation_id = None
                message_id = None
                headers = {}

            response = owner._coerce_response_packet(
                payload,
                correlation_id=correlation_id,
                in_response_to=message_id,
            )

            try:
                await owner._apply_consume_middlewares(
                    owner.reply_queue,
                    Message(
                        payload=response.model_dump(mode="json"),
                        headers=_normalize_headers(headers),
                        metadata={"source_topic": owner.reply_queue, "packet_type": "response"},
                        correlation_id=response.correlation_id,
                    ),
                    is_response=True,
                )
            except (EncryptionError, InvalidTag):
                failed_response = ResponsePacket(
                    correlation_id=correlation_id or "unknown",
                    in_response_to=message_id or "unknown",
                    status="failed",
                    content={
                        "error": "Decryption failed",
                        "reason": "Invalid authentication tag or corrupted encrypted payload.",
                        "action": "Check AES key and message integrity on producer/consumer.",
                    },
                )
                callback_result = user_handler(failed_response)
                if isawaitable(callback_result):
                    await cast("Awaitable[None]", callback_result)
                return
            except Exception as exc:
                raise MessageBrokerError(
                    "Reply middleware failed. Check middleware logic and retry configuration."
                ) from exc

            owner._resolve_pending_response(response)

            callback_result = user_handler(response)
            if isawaitable(callback_result):
                await cast("Awaitable[None]", callback_result)

        wrapper.__annotations__ = {"message": object}
        return wrapper


def _normalize_headers(headers: dict[str, object]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in headers.items():
        normalized[str(key)] = value if isinstance(value, str) else str(value)
    return normalized
