import asyncio

from message_broker import DataPacket, MessageBroker


async def main() -> None:
    broker = MessageBroker("redis://127.0.0.1:6379", queue_name="default_queue")

    @broker.on_message
    async def handle_message(data: DataPacket):
        print(f"Received message: {data.content}")
        await asyncio.sleep(1)
        return {"status": "ok", "result": "pdf_generated.pdf"}

    await broker.start()


def run() -> None:
    asyncio.run(main())
