import asyncio
from datetime import datetime, timedelta
import uuid

from faststream import FastStream
from faststream.redis import RedisBroker
from schema import DataPacket, ResponsePacket
from app_logging import get_logger

log = get_logger("Producer")

# Create a dedicated reply queue for this producer instance
reply_queue = f"reply_queue_{uuid.uuid4().hex}"

broker = RedisBroker("redis://127.0.0.1:6379")
app = FastStream(broker)


@broker.subscriber(list=reply_queue)
async def handle_response(data: ResponsePacket):
    log.info("Response received from consumer",
             correlation_id=data.correlation_id,
             in_response_to=data.in_response_to,
             status=data.status)


async def run_demo():
    # Run the FastStream app in background so the reply subscriber is active
    runner = asyncio.create_task(app.run())
    # Give the subscriber a moment to register
    await asyncio.sleep(0.5)

    # Send a message that should be delivered 5 seconds from now
    deliver_at = datetime.utcnow() + timedelta(seconds=5)
    packet = DataPacket(id=1, sender="Sanjit", content="Hello with delay!", priority=5,
                        reply_to=reply_queue, deliver_at=deliver_at)
    log.info("Sending packet with delayed delivery", id=packet.id, correlation_id=packet.correlation_id, deliver_at=str(deliver_at))
    await broker.publish(packet, list="demo_queue")

    # Wait for a short period to receive the response (consumer will wait until deliver_at)
    await asyncio.sleep(8)

    # Cleanup: stop the FastStream runner
    runner.cancel()


if __name__ == "__main__":
    asyncio.run(run_demo())