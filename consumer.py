import asyncio
from faststream import FastStream
from faststream.redis import RedisBroker
from schema import DataPacket
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
             priority=data.priority)
    
    await asyncio.sleep(0.5)
    log.msg("Processing successful", status="DONE")

if __name__ == "__main__":
    log.info("Starting Consumer... Waiting for messages.")
    asyncio.run(app.run())