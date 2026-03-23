from __future__ import annotations

import unittest
from unittest.mock import AsyncMock

from message_broker.src.core.context import BrokerContext
from message_broker.src.adapters.redis import RedisBroker


class RedisBrokerSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_scheduler_lock_acquire_uses_nx_px(self) -> None:
        context = BrokerContext("redis://localhost:6379")
        broker = RedisBroker(context)
        broker._client = AsyncMock()
        broker._client.set = AsyncMock(return_value=True)

        acquired = await broker._acquire_scheduler_lock()

        self.assertTrue(acquired)
        broker._client.set.assert_awaited_once_with(
            "broker:scheduler:lock",
            broker._instance_id,
            nx=True,
            px=int(context.options["scheduler_lock_ttl_ms"]),
        )

    async def test_scheduler_lock_renew_and_release_only_owner(self) -> None:
        context = BrokerContext("redis://localhost:6379")
        broker = RedisBroker(context)
        broker._client = AsyncMock()
        broker._client.eval = AsyncMock(side_effect=[1, 1])

        renewed = await broker._renew_scheduler_lock()
        await broker._release_scheduler_lock()

        self.assertTrue(renewed)
        self.assertEqual(broker._client.eval.await_count, 2)

    async def test_atomic_move_uses_batch_size_option(self) -> None:
        context = BrokerContext("redis://localhost:6379?scheduler_batch_size=250")
        broker = RedisBroker(context)
        broker._client = AsyncMock()
        broker._client.eval = AsyncMock(return_value=42)

        moved = await broker._move_due_scheduled_messages()

        self.assertEqual(moved, 42)
        args = broker._client.eval.await_args.args
        self.assertEqual(args[1], 3)
        self.assertEqual(args[2], "broker:scheduled_messages")
        self.assertEqual(args[3], "broker:scheduled:payloads")
        self.assertEqual(args[4], "broker:scheduled:targets")
        self.assertEqual(args[6], "250")

    async def test_idempotency_state_transitions(self) -> None:
        context = BrokerContext("redis://localhost:6379?idempotency_ttl_sec=60")
        broker = RedisBroker(context)
        broker._client = AsyncMock()
        broker._client.set = AsyncMock(side_effect=[True, True])
        broker._client.delete = AsyncMock(return_value=1)

        acquired = await broker.try_acquire_idempotency("msg-1")
        await broker.mark_idempotency_completed("msg-1")
        await broker.clear_idempotency_processing("msg-1")

        self.assertTrue(acquired)
        broker._client.set.assert_any_await(
            "broker:processed:msg-1",
            "processing",
            nx=True,
            ex=60,
        )
        broker._client.set.assert_any_await(
            "broker:processed:msg-1",
            "completed",
            ex=60,
        )
        broker._client.delete.assert_awaited_once_with("broker:processed:msg-1")


if __name__ == "__main__":
    unittest.main()
