from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, TypeAlias
import uuid

from pydantic import BaseModel, Field


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
    """Request envelope sent through a queue.

    Fields:
    - `id`: Unique message identifier.
    - `sender`: Name of the producing service or application.
    - `content`: JSON-like payload for business data.
    - `correlation_id`: Tracks request/response pairs.
    - `reply_to`: Queue name where a response should be published.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique ID for the packet")
    sender: str = Field(..., min_length=3, description="Producer name or service identifier")
    content: Payload = Field(..., description="JSON-like message payload")
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Correlation ID for responses")
    reply_to: str | None = Field(None, description="Reply queue name where producer listens for responses")
    deliver_at: datetime | None = Field(None, description="UTC datetime for scheduled delivery")


class ResponsePacket(BaseModel):
    """Response envelope published back to a caller queue.

    Fields:
    - `correlation_id`: Correlation ID from the original request.
    - `in_response_to`: Original request packet id.
    - `status`: Processing status, for example `processed` or `failed`.
    - `content`: JSON-like response payload.
    - `processed_at`: UTC timestamp when response was created.
    """

    correlation_id: str = Field(..., description="Correlation id copied from original message")
    in_response_to: str = Field(..., description="Original DataPacket.id")
    status: str = Field(..., description="Processing status or short response")
    content: Payload = Field(default=None, description="JSON-like response payload")
    processed_at: datetime = Field(default_factory=_utc_now, description="UTC time when response was created")