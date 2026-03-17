from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional, TypeAlias
import uuid

from pydantic import BaseModel, Field


# Payload is a JSON-like payload used throughout the package. Using `Any`
# here avoids recursive TypeAlias handling issues when Pydantic generates
# schemas. Consumers should prefer passing JSON-serializable structures.
Payload: TypeAlias = Any


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
    reply_to: Optional[str] = Field(None, description="Reply queue name where producer listens for responses")
    deliver_at: Optional[datetime] = Field(None, description="UTC datetime for scheduled delivery")


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
    content: Optional[Payload] = Field(default=None, description="JSON-like response payload")
    processed_at: datetime = Field(default_factory=_utc_now, description="UTC time when response was created")