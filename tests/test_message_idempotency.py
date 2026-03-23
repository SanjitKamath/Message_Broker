from __future__ import annotations

import unittest
from unittest.mock import AsyncMock

from message_broker.src.broker import MessageBroker
from message_broker.src.core.interfaces import Message


class MessageBrokerIdempotencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_duplicate_message_is_skipped(self) -> None:
        broker = MessageBroker("redis://localhost:6379")
        broker.broker = AsyncMock()
        broker.broker.try_acquire_idempotency = AsyncMock(return_value=False)

        handler = AsyncMock(return_value={"ok": True})
        wrapper = broker._make_message_wrapper(handler)

        message = Message(
            payload={
                "id": "packet-1",
                "sender": "svc-a",
                "content": {"x": 1},
                "correlation_id": "corr-1",
                "reply_to": None,
                "deliver_at": None,
            },
            metadata={"source_topic": "jobs"},
        )

        await wrapper(message)

        handler.assert_not_awaited()
        broker.broker.try_acquire_idempotency.assert_awaited_once_with("packet-1")


if __name__ == "__main__":
    unittest.main()
