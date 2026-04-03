"""Custom exceptions for :mod:`lyik_messaging`."""

from __future__ import annotations


class LyikMessagingError(Exception):
    """Base exception for all package-level failures."""


class MessageBrokerError(LyikMessagingError):
    """Base error type for broker-related failures."""


class ConfigurationError(LyikMessagingError, ValueError):
    """Raised when user configuration, signatures, or broker selection is invalid."""


class BrokerNotInitializedError(LyikMessagingError):
    """Raised when broker state is requested before `connect()` completes."""


class EncryptionError(LyikMessagingError):
    """Raised when AES-GCM key handling or payload encryption fails."""


class ConnectionLostError(MessageBrokerError):
    """Raised when broker connectivity is interrupted or unavailable."""


class PublishFailedError(MessageBrokerError):
    """Raised when a publish operation fails."""


class FeatureNotSupportedError(MessageBrokerError):
    """Raised when a broker backend does not support a requested feature."""