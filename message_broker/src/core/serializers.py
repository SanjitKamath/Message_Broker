"""
Pluggable serialization strategies for message payloads.

This module provides the default JSON serializer and serves as a template
for users implementing custom serializers (MsgPack, Protobuf, etc.).

Serializers can be used to convert message envelopes to and from string or byte formats suitable
for transmission over the network or storage. The default JsonSerializer uses the standard library's
json module to serialize message data into compact JSON strings. Custom serializers can be implemented by
subclassing the Serializer protocol and implementing the serialize and deserialize methods according to the desired format.

This helps users who need more efficient serialization formats or have specific requirements for message encoding to 
easily integrate their own serializers into the broker system. By defining a clear interface for serializers, 
the package allows for flexible and extensible message handling while maintaining a consistent API for users.

For example:
If the incoming data is a dictionary like 
        {"payload": {"key": "value"}, "headers": {}, "metadata": {}, "correlation_id": "12345"}
the JsonSerializer would convert this to a JSON string like 
        '{"payload":{"key":"value"},"headers":{},"metadata":{},"correlation_id":"12345"}'
When receiving this JSON string, the deserialize method would parse it back into the original dictionary format 
for processing by the broker.
"""

from __future__ import annotations

import json
from datetime import date, datetime
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

        def _json_default(value: object) -> str:
            if isinstance(value, (datetime, date)):
                return value.isoformat()
            raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

        return json.dumps(data, separators=(",", ":"), ensure_ascii=False, default=_json_default)

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
