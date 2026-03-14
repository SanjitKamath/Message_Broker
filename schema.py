from __future__ import annotations

from typing import Optional
from datetime import datetime
import uuid

from pydantic import BaseModel, Field


class DataPacket(BaseModel):
    id: int = Field(..., description="Unique ID for the packet")
    sender: str = Field(..., min_length=3)
    content: str = Field(..., max_length=50)
    priority: int = Field(default=1, ge=1, le=10)
    # Correlation id to allow the consumer to send responses back to the originating producer
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Correlation ID for responses")
    # Optional reply queue name where the producer is listening for responses
    reply_to: Optional[str] = Field(None, description="Reply queue name where producer listens for responses")
    # Optional UTC datetime specifying when the consumer should process this message
    deliver_at: Optional[datetime] = Field(None, description="UTC datetime when consumer should process this message")


class ResponsePacket(BaseModel):
    correlation_id: str = Field(..., description="Correlation id copied from original message")
    in_response_to: int = Field(..., description="Original DataPacket.id")
    status: str = Field(..., description="Processing status or short response")
    processed_at: datetime = Field(default_factory=lambda: datetime.utcnow(), description="UTC time when response was created")