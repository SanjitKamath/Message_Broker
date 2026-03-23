"""Core primitives for broker-agnostic messaging.

This package defines the stable contracts and shared infrastructure used by
all broker adapters. Keeping these contracts centralized prevents adapter
implementations from leaking transport-specific behavior into user code.
"""

from .context import BrokerContext
from .exceptions import (
    ConfigurationError,
    ConnectionLostError,
    MessageBrokerError,
    PublishFailedError,
)
from .interfaces import Broker, Message, Middleware, Publisher, Serializer, Subscriber
from .observability import BrokerObserver, MetricsMiddleware, OpenTelemetryObserver
from .registry import BrokerRegistry
from .resilience import with_retries
from .serializers import JsonSerializer

__all__ = [
    "Broker",
    "BrokerContext",
    "ConfigurationError",
    "ConnectionLostError",
    "JsonSerializer",
    "Message",
    "MessageBrokerError",
    "Middleware",
    "PublishFailedError",
    "Publisher",
    "Serializer",
    "Subscriber",
    "BrokerRegistry",
    "BrokerObserver",
    "with_retries",
    "MetricsMiddleware",
    "OpenTelemetryObserver",
]
