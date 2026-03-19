"""
Consumer example demonstrating middleware integration, serialization, and message handling.

This example shows:
  1. Consuming messages with custom middleware (logging, metrics, etc.)
  2. Middleware hooks executing on message arrival (after_consume)
  3. Graceful handling of deserialized payloads
  4. Matching the same middleware setup used by the producer
"""

import asyncio
import json
import logging
import time
from typing import Any

from message_broker.src import connect
from message_broker.src.core.interfaces import Message, Middleware

# Configure logging to see middleware in action
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


class MetricsMiddleware(Middleware):
    """Tracks message latency and throughput for observability."""

    def __init__(self) -> None:
        """Initialize metrics tracker."""
        self.message_count = 0
        self.total_latency = 0.0

    async def before_publish(self, topic: str, message: Message) -> Message:
        """Not used in consumer, but required by Middleware ABC.

        Args:
            topic: The target topic.
            message: The message being published.

        Returns:
            The message unmodified.
        """
        return message

    async def after_consume(self, topic: str, message: Message) -> Message:
        """Calculate latency and log metrics.

        Args:
            topic: The topic where the message came from.
            message: The consumed message.

        Returns:
            The message unmodified.
        """
        # If metadata contains a submission timestamp, calculate latency
        submission_time = message.metadata.get("submission_time", time.time())
        latency_ms = (time.time() - submission_time) * 1000

        self.message_count += 1
        self.total_latency += latency_ms

        avg_latency = self.total_latency / self.message_count if self.message_count > 0 else 0
        logger.info(
            f"Metrics [topic={topic}]: "
            f"count={self.message_count} | "
            f"last_latency={latency_ms:.2f}ms | "
            f"avg_latency={avg_latency:.2f}ms"
        )
        return message


class LoggingMiddleware(Middleware):
    """Logs every consumed message for debugging and auditing."""

    async def before_publish(self, topic: str, message: Message) -> Message:
        """Not used in consumer.

        Args:
            topic: The target topic.
            message: The message being published.

        Returns:
            The message unmodified.
        """
        return message

    async def after_consume(self, topic: str, message: Message) -> Message:
        """Log the message details after consumption.

        Args:
            topic: The topic where the message came from.
            message: The consumed message.

        Returns:
            The message unmodified.
        """
        payload_str = json.dumps(message.payload) if message.payload else ""
        logger.info(
            f"Consumed from '{topic}' | payload_size={len(payload_str)} bytes | "
            f"correlation_id={message.correlation_id}"
        )
        return message


async def handle_user_events(message: Message) -> None:
    """Process a user event message (login, logout, etc.).

    Args:
        message: The deserialized message from the broker.
    """
    if not isinstance(message.payload, dict):
        logger.warning(f"Unexpected payload type: {type(message.payload)}")
        return

    user_id = message.payload.get("user_id", "unknown")
    action = message.payload.get("action", "unknown")
    logger.info(f"👤 User Event: {user_id} performed '{action}'")


async def handle_delayed_events(message: Message) -> None:
    """Process a delayed/scheduled event that was delivered.

    Args:
        message: The deserialized message from the broker.
    """
    if not isinstance(message.payload, dict):
        logger.warning(f"Unexpected payload type: {type(message.payload)}")
        return

    user_id = message.payload.get("user_id", "unknown")
    action = message.payload.get("action", "unknown")
    scheduled_for = message.metadata.get("scheduled_for", "unknown")
    logger.info(f"⏰ Delayed Event: {user_id} / '{action}' (was scheduled for {scheduled_for})")


async def handle_batch_events(message: Message) -> None:
    """Process a batch event.

    Args:
        message: The deserialized message from the broker.
    """
    if not isinstance(message.payload, dict):
        logger.warning(f"Unexpected payload type: {type(message.payload)}")
        return

    batch_id = message.metadata.get("batch_id", "unknown")
    index = message.payload.get("index", "?")
    logger.info(f"📦 Batch Event: batch_id={batch_id}, item_index={index}")


async def main() -> None:
    """Initialize broker with middleware and consume from multiple topics."""

    # Create a broker with the same middleware setup as the producer
    # This ensures consistent message tracking across publish and consume
    metrics = MetricsMiddleware()
    async with await connect(
        "redis://localhost:6379/0",
        middlewares=[LoggingMiddleware(), metrics],
        timeout=5000,
        # Configure worker pool and queue size for backpressure
        concurrency=3,
        max_queue_size=20,
    ) as broker:
        subscriber = broker.get_subscriber()

        logger.info(f"Broker capabilities: {broker.capabilities}")

        # Subscribe to multiple topics with different handlers
        logger.info("Starting consumer with middleware enabled...")
        logger.info("Subscribed to: user_events, delayed_events, batch_events")

        # In the current architecture, subscribe() starts internal background
        # consumer/worker tasks and returns quickly. So we subscribe once and
        # then block this coroutine until interrupted.
        await subscriber.subscribe("user_events", handle_user_events)
        await subscriber.subscribe("delayed_events", handle_delayed_events)
        await subscriber.subscribe("batch_events", handle_batch_events)

        logger.info("Consumer running. Press Ctrl+C to stop.")
        stop_event = asyncio.Event()
        try:
            await stop_event.wait()
        except asyncio.CancelledError:
            logger.info("Shutting down consumer...")
            raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Consumer stopped.")
