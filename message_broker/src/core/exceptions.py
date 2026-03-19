"""Unified exception hierarchy for broker operations.

The package intentionally wraps backend-specific exceptions so calling code can
depend on stable error types regardless of transport implementation.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class MessageBrokerError(Exception):
    """Base type for all broker-related failures.

    Attributes:
        broker: Optional adapter name involved in the failure.
        operation: Optional operation label, for example "connect".
        details: Optional structured metadata for diagnostics.
    """

    def __init__(
        self,
        message: str,
        *,
        broker: str | None = None,
        operation: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        self.broker = broker
        self.operation = operation
        self.details = dict(details) if details is not None else {}
        super().__init__(message)


class ConnectionLostError(MessageBrokerError):
    """Raised when broker connectivity is interrupted or unavailable."""


class PublishFailedError(MessageBrokerError):
    """Raised when a message cannot be delivered by the active adapter."""


class ConfigurationError(MessageBrokerError):
    """Raised when broker configuration or URI input is invalid."""

class FeatureNotSupportedError(MessageBrokerError):
    """Raised when a specific broker adapter does not support an advanced feature."""
