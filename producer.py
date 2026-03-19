"""
Producer example demonstrating middleware, serialization swapping, and delivery scheduling.

This example shows:
  1. Custom middleware that logs message usage and validates payloads
  2. Creating brokers with custom serializers (if desired)
  3. Publishing messages with explicit delivery_at scheduling (Redis only)
  4. Publishing without scheduling for immediate consumption
"""

import asyncio
import json
import logging
import time
from typing import Any

from message_broker.src import connect
from message_broker.src.core.interfaces import Message, Middleware, BrokerCapability

# Configure logging to see middleware in action
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


class LoggingMiddleware(Middleware):
    """Logs every message before publishing to track traffic and debugging."""

    async def before_publish(self, topic: str, message: Message) -> Message:
        """Log the message topic and approximate payload size.

        Args:
            topic: The target topic name.
            message: The message being published.

        Returns:
            The message unmodified (middleware passed through).
        """
        payload_str = json.dumps(message.payload) if message.payload else ""
        logger.info(
            f"Publishing to topic '{topic}' | payload_size={len(payload_str)} bytes | "
            f"headers={message.headers} | correlation_id={message.correlation_id}"
        )
        return message

    async def after_consume(self, topic: str, message: Message) -> Message:
        """Not used in publisher, but required by Middleware ABC.

        Args:
            topic: The topic where the message came from.
            message: The consumed message.

        Returns:
            The message unmodified.
        """
        return message


class ValidationMiddleware(Middleware):
    """Validates that payloads contain required fields before publishing.

    This example enforces that 'user_id' and 'action' fields are present.
    """

    async def before_publish(self, topic: str, message: Message) -> Message:
        """Validate required fields in the payload.

        Args:
            topic: The target topic.
            message: The message being published.

        Returns:
            The message unmodified if validation passes.

        Raises:
            ValueError: If required fields are missing.
        """
        if not isinstance(message.payload, dict):
            raise ValueError(f"Payload must be a dict, got {type(message.payload)}")

        required = {"user_id", "action"}
        missing = required - set(message.payload.keys())
        if missing:
            raise ValueError(f"Payload missing required fields: {missing}")

        logger.info(f"Validation passed for {topic}: user_id={message.payload.get('user_id')}")
        return message

    async def after_consume(self, topic: str, message: Message) -> Message:
        """Not used in publisher.

        Args:
            topic: The topic where the message came from.
            message: The consumed message.

        Returns:
            The message unmodified.
        """
        return message


async def main() -> None:
    """Demonstrate publishing with middleware, scheduling, and namespaced options."""

    # Create a broker with custom middlewares (they will be called in order)
    # The middlewares are instantiated once and applied to all messages
    async with await connect(
        "redis://localhost:6379/0",
        middlewares=[LoggingMiddleware(), ValidationMiddleware()],
        timeout=5000,
        # Backpressure / worker pool tuning
        concurrency=4,
        max_queue_size=50,
        # Optionally pass broker-specific options to avoid key collisions:
        # redis={"key_prefix": "my_app_"}
        # kafka={"group_id": "my-producer-group"}
    ) as broker:
        publisher = broker.get_publisher()
        logger.info(f"Broker capabilities: {broker.capabilities}")

        # --- Example 1: Immediate publish (standard queue behavior) ---
        logger.info("=== Publishing immediate message (no scheduling) ===")
        await publisher.publish(
            topic="user_events",
            message=Message(
                payload={
                    "user_id": "user_123",
                    "action": "login",
                    "timestamp": time.time(),
                },
                headers={"x-version": "1.0", "x-client": "mobile"},
                metadata={"source": "producer.py"},
            ),
        )

        # --- Example 2: Scheduled publish (Redis only!) ---
        logger.info("=== Publishing scheduled message (5 seconds from now) ===")
        deliver_in_seconds = 25
        deliver_at = time.time() + deliver_in_seconds

        # Check capability before scheduling (EnforcingPublisher also enforces)
        if BrokerCapability.DELAYED_DELIVERY in broker.capabilities:
            try:
                await publisher.publish(
                    topic="delayed_events",
                    message=Message(
                        payload={
                            "user_id": "user_456",
                            "action": "email_reminder",
                            "timestamp": time.time(),
                        },
                        headers={"x-priority": "high"},
                        metadata={"scheduled_for": deliver_at},
                    ),
                    deliver_at=deliver_at,  # Exactly 5 seconds in the future
                )
                logger.info(
                    f"Message scheduled for delivery at {deliver_at} (in {deliver_in_seconds}s)"
                )
            except Exception as exc:
                logger.warning(f"Scheduling failed: {exc}")
        else:
            logger.warning("Broker does not support delayed delivery; skipping scheduled publish")

        # --- Example 3: Publish with validation error (caught by middleware) ---
        logger.info("=== Attempting publish with missing required field (will fail validation) ===")
        try:
            await publisher.publish(
                topic="user_events",
                message=Message(
                    payload={
                        "user_id": "user_789",
                        # 'action' field is missing - validation will catch this
                    },
                    headers={"x-version": "1.0"},
                ),
            )
        except ValueError as exc:
            logger.error(f"Validation error (expected): {exc}")

        # --- Example 4: Batch publish with middleware in action ---
        logger.info("=== Batch publishing (middleware logging applied to each) ===")
        for i in range(3):
            await publisher.publish(
                topic="batch_events",
                message=Message(
                    payload={
                        "user_id": f"user_batch_{i}",
                        "action": f"action_{i}",
                        "index": i,
                    },
                    headers={"x-batch-index": str(i)},
                    metadata={"batch_id": "batch_001"},
                ),
            )
            await asyncio.sleep(0.1)  # Small delay between publishes

        logger.info("All publishing complete. Consumer will receive messages when running.")


if __name__ == "__main__":
    asyncio.run(main())
