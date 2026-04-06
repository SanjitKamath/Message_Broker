"""Pydantic models and contracts for `lyik_messaging`."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TypeAlias
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


JsonValue: TypeAlias = dict[str, Any] | list[Any] | str | int | float | bool | None
Payload: TypeAlias = JsonValue


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DataPacket(BaseModel):
    """
    Request envelope used by packet handlers.
    
    DataPacket is intentionally transport-agnostic and does not include any fields
    that are specific to a particular broker or protocol. It serves as a generic
    container for message content and essential metadata, allowing the core library to
    operate independently of the underlying transport implementation.

    Arguments:
    - `id`: Unique identifier for the message, automatically generated if not provided.
    - `sender`: Identifier of the message sender (e.g., service name, client ID).
    - `content`: The actual message payload, which can be any JSON-serializable data
        structure.
    - `correlation_id`: Unique identifier for correlating requests and responses in
        asynchronous communication patterns. Automatically generated if not provided.
    - `reply_to`: Optional field specifying the topic or queue where responses should
        be sent, enabling request/response interactions.
    - `deliver_at`: Optional timestamp indicating when the message should be delivered,
        allowing for scheduled or delayed messaging scenarios.
    """

    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: str(uuid4()))
    sender: str = Field(..., min_length=1)
    content: Payload
    correlation_id: str = Field(default_factory=lambda: uuid4().hex)
    reply_to: str | None = None
    deliver_at: datetime | None = None


class ResponsePacket(BaseModel):
    """
    Response envelope used by request/response flows.
    
    Pydantic model representing the structure of response messages in request/response 
    communication patterns. Like `DataPacket`, `ResponsePacket` is designed to be 
    transport-agnostic, containing only fields that are relevant to the content and 
    metadata of a response message, without any broker-specific attributes.

    Arguments:
    - `correlation_id`: Unique identifier that matches the `correlation_id` of the
        corresponding request, enabling the requester to correlate responses with their
        original requests.
    - `in_response_to`: Optional field indicating the ID of the request message that
        this response is addressing, providing additional context for the requester.
    - `status`: String indicating the outcome of the request (e.g., "success", "error").
    - `content`: The actual response payload, which can be any JSON-serializable data
        structure.
    - `processed_at`: Timestamp indicating when the response was generated, automatically
        set to the current UTC time if not provided.
    """

    model_config = ConfigDict(extra="forbid")

    correlation_id: str
    in_response_to: str
    status: str
    content: Payload = None
    processed_at: datetime = Field(default_factory=_utc_now)


class MessageInfo(BaseModel):
    """
    Transport metadata exposed to strict two-argument handlers.
    
    This model encapsulates transport-specific metadata that is made available to 
    user-defined message handlers that opt to receive it. The fields in `MessageInfo` 
    are designed to be generic enough to apply across different broker implementations while still
    providing useful context about the message and its transport details. This allows
    handlers to access relevant information about the message and its delivery without being
    tightly coupled to any specific messaging protocol or broker API.

    Arguments:
    - `correlation_id`: Unique identifier for correlating messages in asynchronous
        communication patterns.
    - `reply_to`: Optional field specifying the topic or queue where responses should
        be sent, enabling request/response interactions.
    - `sender`: Identifier of the message sender (e.g., service name, client ID).
    - `raw`: Optional field containing the raw message data as received from the broker,
        providing access to the underlying transport-specific information.

    """

    model_config = ConfigDict(extra="forbid")

    correlation_id: str | None = Field(default=None)
    reply_to: str | None = Field(default=None)
    sender: str | None = Field(default=None)
    raw: dict[str, object] | None = Field(default=None)


@dataclass(slots=True)
class Message:
    """
    Transport-neutral message used by middleware hooks.
    
    This class represents a message in a transport-agnostic format that is used within
    middleware hooks for both publishing and consuming messages. It serves as a common
    representation of a message that can be manipulated by middleware components without
    needing to be aware of the specific broker or protocol being used. The fields in
    `Message` are designed to capture the essential content and metadata of a message while
    remaining flexible enough to accommodate various messaging patterns and use cases.

    Arguments:
    - `payload`: The actual message content, which can be any JSON-serializable data structure.
    - `headers`: A dictionary of message headers.
    - `metadata`: A dictionary of message metadata.
    - `correlation_id`: Unique identifier for correlating messages in asynchronous
        communication patterns. Automatically generated if not provided.
    """

    payload: object
    headers: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, JsonValue] = field(default_factory=dict)
    correlation_id: str = field(default_factory=lambda: uuid4().hex)

    def __post_init__(self) -> None:
        self.headers = dict(self.headers)
        self.metadata = dict(self.metadata)
        if not self.correlation_id.strip():
            self.correlation_id = uuid4().hex
        self.headers.setdefault("correlation_id", self.correlation_id)


class Middleware(ABC):
    """
    Middleware contract for publish and consume hooks.
    
    This abstract base class defines the contract for middleware components that can be used 
    to hook into the message processing pipeline for both publishing and consuming messages. 
    Middleware components that inherit from this class must implement the `before_publish` and
    `after_consume` methods, which allow them to manipulate messages before they are published
    and after they are consumed, respectively. The `before_publish_response` and
    `after_consume_response` methods provide default implementations that simply call the
    corresponding `before_publish` and `after_consume` methods, but they can be overridden by
    middleware components that need to differentiate between request and response messages or
    require additional processing for responses.

    The design of this middleware contract allows for flexible and powerful message processing
    capabilities while maintaining a clear separation of concerns between the core messaging logic
    and the custom behavior implemented by middleware components.

    Arguments:
    - `before_publish`: Method that runs before a message is published, allowing middleware to
        modify the message or perform actions such as logging, validation, or enrichment.
    - `after_consume`: Method that runs after a message is consumed but before handler execution
        allowing middleware to modify the message, perform actions such as logging, or enforce
        policies before the message is processed by the handler.
    - `before_publish_response`: Optional method that runs before a response message is published,
        allowing middleware to differentiate between request and response messages or perform
        additional processing for responses. By default, it calls `before_publish`.
    - `after_consume_response`: Optional method that runs after a response message is consumed but
        before handler execution, allowing middleware to differentiate between request and response
        messages or perform additional processing for responses. By default, it calls `after_consume`.
    """

    @abstractmethod
    async def before_publish(self, topic: str, message: Message) -> Message:
        """Run before a message is published."""

    @abstractmethod
    async def after_consume(self, topic: str, message: Message) -> Message:
        """Run after a message is consumed but before handler execution."""

    async def before_publish_response(self, topic: str, message: Message) -> Message:
        return await self.before_publish(topic, message)

    async def after_consume_response(self, topic: str, message: Message) -> Message:
        return await self.after_consume(topic, message)