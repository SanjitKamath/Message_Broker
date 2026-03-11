import asyncio
from faststream.redis import RedisBroker
from schema import DataPacket
from app_logging import get_logger

log = get_logger("Producer")

async def run_demo():
    async with RedisBroker("redis://127.0.0.1:6379") as broker:
        valid_packet = DataPacket(id=1, sender="Sanjit", content="Hello Redis List!", priority=5)
        log.info("Sending VALID packet", id=valid_packet.id)
        
        # Pushing to the exact same list
        await broker.publish(valid_packet, list="demo_queue")

        await asyncio.sleep(2)

        log.warn("Attempting to send INVALID data...")
        try:
            invalid_packet = DataPacket(id=2, sender="SJ", content="Fail", priority=99)
            # Make sure this is also pointing to the list so it doesn't throw an unexpected error
            await broker.publish(invalid_packet, list="demo_queue")
        except Exception as e:
            log.error("Pydantic blocked the send!", error=str(e))

if __name__ == "__main__":
    asyncio.run(run_demo())