"""
This module defines the core data models used for message exchange in the MessageBroker system. It includes the DataPacket and ResponsePacket classes, 
which serve as the standard envelopes for messages sent through the broker. The DataPacket is used for incoming messages that handlers will process, 
while the ResponsePacket is used for outgoing responses that handlers can publish back to a reply queue. Both classes are designed to be flexible and 
accommodate a wide range of payloads while also providing important metadata for tracking and correlation.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, TypeAlias
import uuid

from pydantic import BaseModel, ConfigDict, Field


# JSON-like payload used throughout the package.
#
# The alias intentionally allows Any for dict/list elements to avoid
# over-constraining dynamic payloads while still communicating the expected
# shape to type checkers.
Payload: TypeAlias = dict[str, Any] | list[Any] | str | int | float | bool | None


def _utc_now() -> datetime:
    """Return the current timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


class DataPacket(BaseModel):
    """
    Request envelope used by handlers and transport adapters.

    This model is the canonical request shape passed to user-defined message
    handlers. It captures business payload plus correlation/routing metadata.

    Arguments:
        - `id` Unique packet identifier, usually a UUID string. Auto-generated if not provided.
        - `sender` Logical producer identifier (e.g., service/app name). Required.
        - `content` Business payload consumed by handlers. Accepts dict/list primitives or scalar JSON-like values. Required.
        - `correlation_id` Correlation token linking request and response across hops. Auto-generated if not provided.
        - `reply_to` Queue/topic where the receiver should publish a ResponsePacket. Optional; leave null for fire-and-forget messages.
        - `deliver_at` Optional timezone-aware UTC datetime for delayed delivery. When omitted, the message is delivered immediately.

    Example:
        ```
        DataPacket(
            sender="billing-api",
            content={"event": "invoice.created", "invoice_id": "inv_123"},
            reply_to="reply_queue_a1b2c3",
        )
        ```
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": "5d8ecf9f-4719-4be2-a7bb-e2e7cb53a077",
                    "sender": "orders-service",
                    "content": {"event": "order.created", "order_id": 101},
                    "correlation_id": "d31f84f093074f3d8e83f0e8dd8ebd5f",
                    "reply_to": "orders-replies",
                }
            ]
        }
    )

    id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique packet identifier, usually a UUID string.",
        examples=["5d8ecf9f-4719-4be2-a7bb-e2e7cb53a077"],
    )
    sender: str = Field(
        ...,
        min_length=3,
        description=(
            "Logical producer identifier (service/app name). "
            "Examples: 'orders-service', 'billing-api', 'worker-1'."
        ),
        examples=["orders-service"],
    )
    content: Payload = Field(
        ...,
        description=(
            "Business payload consumed by handlers. Accepts dict/list primitives "
            "or scalar JSON-like values."
        ),
        examples=[{"event": "order.created", "order_id": 101}],
    )
    correlation_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description=(
            "Correlation token linking request and response across hops. "
            "Populate this when continuing an existing trace."
        ),
        examples=["d31f84f093074f3d8e83f0e8dd8ebd5f"],
    )
    reply_to: str | None = Field(
        None,
        description=(
            "Queue/topic where the receiver should publish a ResponsePacket. "
            "Leave null for fire-and-forget messages."
        ),
        examples=["orders-replies"],
    )
    deliver_at: datetime | None = Field(
        None,
        description=(
            "Optional timezone-aware UTC datetime for delayed delivery. "
            "When omitted, the message is delivered immediately."
        ),
        examples=["2026-03-30T12:30:00Z"],
    )


class ResponsePacket(BaseModel):
    """
    Response envelope published to reply queues.

    Responses are correlated to the originating request using `correlation_id`.

    Arguments:
        - `correlation_id` Correlation ID copied from the originating DataPacket. Required for traceability.
        - `in_response_to` Identifier of the original DataPacket (`DataPacket.id`). Required for direct correlation.
        - `status` Outcome label from the handler. Common values are 'processed' and 'failed', but custom status strings are allowed. Required.
        - `content` Response payload body returned by the handler. Accepts dict/list primitives or scalar JSON-like values. Optional; can be null if no response body is needed.
        - `processed_at` Timezone-aware UTC timestamp when this response was created. Auto-generated at instantiation.

    Example:
    ```
        ResponsePacket(
            correlation_id="d31f84f093074f3d8e83f0e8dd8ebd5f",
            in_response_to="5d8ecf9f-4719-4be2-a7bb-e2e7cb53a077",
            status="processed",
            content={"ok": True},
        )
    ```
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "correlation_id": "d31f84f093074f3d8e83f0e8dd8ebd5f",
                    "in_response_to": "5d8ecf9f-4719-4be2-a7bb-e2e7cb53a077",
                    "status": "processed",
                    "content": {"ok": True},
                    "processed_at": "2026-03-30T12:30:04Z",
                }
            ]
        }
    )

    correlation_id: str = Field(
        ...,
        description="Correlation ID copied from the originating DataPacket.",
        examples=["d31f84f093074f3d8e83f0e8dd8ebd5f"],
    )
    in_response_to: str = Field(
        ...,
        description="Identifier of the original DataPacket (`DataPacket.id`).",
        examples=["5d8ecf9f-4719-4be2-a7bb-e2e7cb53a077"],
    )
    status: str = Field(
        ...,
        description=(
            "Outcome label from the handler. Common values are 'processed' and "
            "'failed', but custom status strings are allowed."
        ),
        examples=["processed"],
    )
    content: Payload = Field(
        default=None,
        description="Response payload body returned by the handler.",
        examples=[{"ok": True}],
    )
    processed_at: datetime = Field(
        default_factory=_utc_now,
        description="Timezone-aware UTC timestamp when this response was created.",
        examples=["2026-03-30T12:30:04Z"],
    )