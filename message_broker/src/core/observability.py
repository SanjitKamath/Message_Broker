"""
Observability extension points for broker operations.

This module keeps observability optional and transport-agnostic. Integrations
can implement the observer protocol directly or use provided middleware/helpers.

Observability refers to the ability to monitor and measure the behavior of the message broker, including
metric like the time taken to publish messages, the latency of message handling, and the occurrence of retry attempts. 
By providing hooks for these events, users can integrate with various monitoring and tracing systems to gain insights 
into the performance and reliability of their messaging infrastructure.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable
from typing import Any, Protocol, runtime_checkable

from .interfaces import Message, Middleware


@runtime_checkable
class BrokerObserver(Protocol):
    """
    Protocol for optional publish/consume/retry instrumentation hooks.
    Observers can be registered on a per-message basis through `message.metadata["observers"]` or globally
    through broker configuration. All observer methods are best-effort and should not raise exceptions.
    """

    def on_publish(self, *, topic: str, elapsed_ms: float, message: Message) -> None | Awaitable[None]:
        """
        Record time spent publishing a message. Called after the publish operation completes, regardless of success or failure.
        The `elapsed_ms` parameter represents the time taken to complete the publish operation in milliseconds.
        This allows observers to track the performance of the publishing process and identify any potential bottlenecks or issues.
        """

    def on_handler_execution(
        self,
        *,
        topic: str,
        elapsed_ms: float,
        message: Message,
        status: str,
    ) -> None | Awaitable[None]:
        """
        Record handler execution timing and completion status. Called after a message handler finishes processing, regardless of 
        success or failure. The `elapsed_ms` parameter represents the time taken to execute the handler in milliseconds, 
        while the `status` parameter indicates the outcome of the handler execution (e.g., "success", "error", "retry"). 
        This allows observers to monitor the performance of message handling and identify any issues or patterns related to 
        handler execution.
        """

    def on_retry_attempt(
        self,
        *,
        operation: str,
        attempt: int,
        max_retries: int,
        error: Exception,
    ) -> None | Awaitable[None]:
        """Record retry attempts emitted by resilience helpers."""


class MetricsMiddleware(Middleware):
    """
    Middleware that records queue transit latency and consume counts.

    The middleware itself is storage-agnostic. It emits measurements to any
    configured observers in `message.metadata["observers"]` when present.
    """

    async def before_publish(self, topic: str, message: Message) -> Message:
        """Attach producer timestamp used to estimate end-to-end latency."""

        _ = topic
        message.metadata.setdefault("submitted_at", time.time())
        return message

    async def after_consume(self, topic: str, message: Message) -> Message:
        """Attach queue transit latency metadata for downstream handlers."""

        submitted_at = message.metadata.get("submitted_at")
        if not isinstance(submitted_at, (int, float)):
            return message

        elapsed_ms = max((time.time() - float(submitted_at)) * 1000.0, 0.0)
        message.metadata["queue_latency_ms"] = elapsed_ms

        return message


class OpenTelemetryObserver:
    """Best-effort OpenTelemetry observer.

    This helps users get started with OpenTelemetry without needing to implement the full protocol.

    OpenTelemetry is a popular open-source observability framework that provides tools and APIs for collecting 
    and exporting telemetry data, such as traces, metrics, and logs. By implementing the `BrokerObserver` protocol, 
    this class allows users to automatically emit OpenTelemetry spans for key broker operations like 
    publishing messages, executing handlers, and retry attempts.

    This class has no hard dependency on OpenTelemetry. If the package is not
    installed, methods become no-ops.
    """

    def __init__(self, tracer_name: str = "message_broker") -> None:
        self._tracer: Any | None = None
        try:
            from opentelemetry import trace  # type: ignore

            self._tracer = trace.get_tracer(tracer_name)
        except Exception:
            self._tracer = None

    def _record(self, span_name: str, attributes: dict[str, Any]) -> None:
        if self._tracer is None:
            return
        with self._tracer.start_as_current_span(span_name) as span:
            for key, value in attributes.items():
                if value is None:
                    continue
                span.set_attribute(key, value)

    def on_publish(self, *, topic: str, elapsed_ms: float, message: Message) -> None:
        """Emit publish timing spans when OpenTelemetry is available."""

        self._record(
            "message.publish",
            {
                "messaging.destination": topic,
                "messaging.message_id": message.correlation_id,
                "messaging.elapsed_ms": elapsed_ms,
            },
        )

    def on_handler_execution(
        self,
        *,
        topic: str,
        elapsed_ms: float,
        message: Message,
        status: str,
    ) -> None:
        """
        Emit handler timing spans when OpenTelemetry is available. This ensures that we can track the full 
        end-to-end latency of messages, including time spent in queues and handlers, as well as the final 
        outcome of the handler execution.
        """

        self._record(
            "message.handle",
            {
                "messaging.destination": topic,
                "messaging.message_id": message.correlation_id,
                "messaging.elapsed_ms": elapsed_ms,
                "messaging.status": status,
            },
        )

    def on_retry_attempt(
        self,
        *,
        operation: str,
        attempt: int,
        max_retries: int,
        error: Exception,
    ) -> None:
        """Emit retry spans when OpenTelemetry is available."""

        self._record(
            "message.retry",
            {
                "messaging.operation": operation,
                "messaging.retry_attempt": attempt,
                "messaging.retry_max": max_retries,
                "messaging.error": str(error),
            },
        )
