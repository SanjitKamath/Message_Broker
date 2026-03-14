import asyncio
from datetime import datetime
from faststream import FastStream
from faststream.redis import RedisBroker
from schema import DataPacket, ResponsePacket
from app_logging import get_logger

log = get_logger("Consumer")
broker = RedisBroker("redis://127.0.0.1:6379")
app = FastStream(broker)

# We are using a Redis List to act as a durable queue
@broker.subscriber(list="demo_queue")
async def process_packet(data: DataPacket):
    log.info("Message Received",
             packet_id=data.id,
             sender=data.sender,
             priority=data.priority,
             correlation_id=data.correlation_id)

    # If a deliver_at is provided and it's in the future, delay processing until then
    if data.deliver_at:
        try:
            now = datetime.utcnow()
            deliver_at = data.deliver_at
            # compute seconds to wait
            wait_seconds = (deliver_at - now).total_seconds()
            if wait_seconds > 0:
                log.info("Delaying delivery until specified time", delay=wait_seconds, packet_id=data.id)
                await asyncio.sleep(wait_seconds)
        except Exception:
            # If parsing fails, just continue to process immediately
            log.warn("Failed to interpret deliver_at; processing immediately", packet_id=data.id)

    # Simulate processing
    await asyncio.sleep(0.5)
    log.info("Processing successful", status="DONE")

    # Send a response back to the producer if a reply queue was provided
    if data.reply_to:
        response = ResponsePacket(
            correlation_id=data.correlation_id,
            in_response_to=data.id,
            status="processed",
        )
        try:
            await broker.publish(response, list=data.reply_to)
            log.info("Response published to producer reply queue", reply_to=data.reply_to, correlation_id=data.correlation_id)
        except Exception as e:
            log.error("Failed to publish response", error=str(e), correlation_id=data.correlation_id)


if __name__ == "__main__":
    log.info("Starting Consumer... Waiting for messages.")
    asyncio.run(app.run())