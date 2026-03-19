"""Pluggable serialization strategies for message payloads.

This module provides the default JSON serializer and serves as a template
for users implementing custom serializers (MsgPack, Protobuf, etc.).
"""

from __future__ import annotations

import json
from collections.abc import Mapping

from .interfaces import JsonValue, Serializer


class JsonSerializer(Serializer):
    """Default JSON serializer for message envelopes.

    Uses the standard library `json` module with compact formatting.
    All Message fields (payload, headers, metadata, correlation_id) are
    serialized as JSON and encoded as UTF-8 strings.
    """

    def serialize(self, data: Mapping[str, JsonValue]) -> str:
        """Convert a dictionary to a JSON string.

        Args:
            data: Dictionary to serialize (typically asdict(Message)).

        Returns:
            Compact JSON string (no whitespace).
        """

        return json.dumps(data, separators=(",", ":"), ensure_ascii=False)

    def deserialize(self, payload: str | bytes) -> dict[str, JsonValue]:
        """Parse a JSON string into a dictionary.

        Args:
            payload: JSON string to deserialize.

        Returns:
            Dictionary with keys: payload, headers, metadata, correlation_id.

        Raises:
            json.JSONDecodeError: If payload is not valid JSON.
        """

        text_payload = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        return json.loads(text_payload)
