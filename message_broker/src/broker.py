import asyncio
import json
import time
import uuid
from datetime import datetime
from typing import Awaitable, Callable, Optional, TypeVar, Union

import redis.asyncio as aioredis
from faststream import FastStream
from faststream.redis import RedisBroker
from .schema import DataPacket, Payload, ResponsePacket
from .app_logging import get_logger


HandlerResult = TypeVar("HandlerResult")
MaybeAwaitable = Union[HandlerResult, Awaitable[HandlerResult]]

class MessageBroker:
    """General-purpose request/response broker wrapper over Redis + FastStream.

    Parameters:
    - `redis_url`: Redis connection URL, for example `redis://127.0.0.1:6379`.
    - `queue_name`: Queue used to consume incoming requests.
    """

    def __init__(self, redis_url: str, queue_name: str = "default_queue"):
        self.log = get_logger(self.__class__.__name__)
        self.broker = RedisBroker(redis_url)
        self.app = FastStream(self.broker)
        self.queue_name = queue_name
        self.reply_queue = f"reply_queue_{uuid.uuid4().hex}"
        # raw redis client used for scheduling operations (ZSET)
        # use decode_responses so members are returned as str
        self._redis = aioredis.from_url(redis_url, encoding="utf-8", decode_responses=True)
        self._scheduler_task: Optional[asyncio.Task] = None

    async def send_message(
        self,
        content: Payload,
        sender: str,
        reply: bool = False,
        deliver_at: Optional["datetime"] = None,
    ) -> str:
        """Send a request packet to the configured queue.

        Parameters:
        - `content`: JSON-like payload to process.
        - `sender`: Producer/service name.
        - `reply`: If true, attach this broker instance's reply queue.

        Returns:
        - Correlation ID for tracking response packets.
        """
        reply_to = self.reply_queue if reply else None
        packet = DataPacket(
            sender=sender,
            content=content,
            reply_to=reply_to,
            deliver_at=deliver_at,
        )
        self.log.info("Sending packet", id=packet.id, correlation_id=packet.correlation_id)
        # If a deliver_at timestamp is provided, schedule into a ZSET instead
        if packet.deliver_at:
            try:
                # use UTC timestamp as score
                # ensure we have a float timestamp
                ts = float(packet.deliver_at.timestamp())
            except Exception:
                ts = time.time()
            zset_key = f"{self.queue_name}_scheduled"
            member = packet.model_dump_json()
            # ZADD score member
            await self._redis.zadd(zset_key, {member: ts})
            self.log.info("Packet scheduled", correlation_id=packet.correlation_id, deliver_at=ts)
        else:
            await self.broker.publish(packet, list=self.queue_name)
        return packet.correlation_id

    def on_message(
        self,
        handler: Callable[[DataPacket], MaybeAwaitable[Optional[Payload]]],
    ) -> Callable[[DataPacket], Awaitable[None]]:
        """Register request handler for `queue_name`.

        The handler may be sync or async. If a request includes `reply_to`,
        the handler result is returned as `ResponsePacket.content`.
        """
        @self.broker.subscriber(list=self.queue_name)
        async def wrapper(data: DataPacket) -> None:
            self.log.info("Message Received", packet_id=data.id, correlation_id=data.correlation_id)
            try:
                result = handler(data)
                if asyncio.iscoroutine(result):
                    result = await result
                if data.reply_to:
                    response = ResponsePacket(
                        correlation_id=data.correlation_id,
                        in_response_to=data.id,
                        status="processed",
                        content=result,
                    )
                    await self.broker.publish(response, list=data.reply_to)
                    self.log.info("Response published", correlation_id=data.correlation_id, status="processed")
            except Exception as exc:
                self.log.exception("Message processing failed", packet_id=data.id, correlation_id=data.correlation_id)
                if data.reply_to:
                    failed_response = ResponsePacket(
                        correlation_id=data.correlation_id,
                        in_response_to=data.id,
                        status="failed",
                        content={"error": str(exc)},
                    )
                    await self.broker.publish(failed_response, list=data.reply_to)
                    self.log.info("Response published", correlation_id=data.correlation_id, status="failed")
        return wrapper

    async def _run_scheduler_loop(self) -> None:
        """Background loop that moves due messages from ZSET -> Redis List.

        Uses a Lua script to atomically fetch and remove ready items (with score <= now).
        The script returns an array of alternating member, score values.
        """
        zset_key = f"{self.queue_name}_scheduled"
        # Lua script: ZRANGEBYSCORE WITHSCORES LIMIT 0 count; ZREM those members; return array
        lua = r"""
local res = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2], 'WITHSCORES')
if #res == 0 then
  return {}
end
local members = {}
for i=1,#res,2 do
  table.insert(members, res[i])
end
redis.call('ZREM', KEYS[1], unpack(members))
return res
"""

        # how many items to pop per iteration
        batch = 100
        while True:
            try:
                now_ts = str(time.time())
                raw = await self._redis.eval(lua, 1, zset_key, now_ts, str(batch))
                if not raw:
                    await asyncio.sleep(1)
                    continue
                # raw is [member1, score1, member2, score2, ...]
                it = iter(raw)
                for member, score in zip(it, it):
                    try:
                        # deserialize into DataPacket
                        packet = DataPacket.model_validate_json(member)
                        # publish to faststream list
                        await self.broker.publish(packet, list=self.queue_name)
                        self.log.info('Scheduled packet delivered', correlation_id=packet.correlation_id)
                    except Exception:
                        self.log.exception('Failed to deliver scheduled packet')
                # small sleep to avoid hammering
                await asyncio.sleep(0)
            except Exception:
                self.log.exception('Scheduler loop crashed; continuing')
                await asyncio.sleep(1)

    def on_reply(
        self,
        handler: Callable[[ResponsePacket], MaybeAwaitable[None]],
    ) -> Callable[[ResponsePacket], Awaitable[None]]:
        """Register reply handler for this broker instance's reply queue.

        The handler may be sync or async.
        """
        @self.broker.subscriber(list=self.reply_queue)
        async def wrapper(data: ResponsePacket) -> None:
            self.log.info("Reply received", correlation_id=data.correlation_id)
            result = handler(data)
            if asyncio.iscoroutine(result):
                await result
        return wrapper

    async def start(self) -> None:
        """Start the FastStream application and block until shutdown."""
        self.log.info("Starting Message Broker... Waiting for messages.")
        # start scheduler background task
        if self._scheduler_task is None:
            self._scheduler_task = asyncio.create_task(self._run_scheduler_loop())
        await self.app.run()
