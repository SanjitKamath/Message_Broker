import asyncio
from message_broker import MessageBroker, DataPacket

async def main():
    broker = MessageBroker("redis://127.0.0.1:6379", queue_name="default_queue")

    @broker.on_message
    async def handle_message(data: DataPacket):
        print(f"Received message: {data.content}")
        # Simulate processing
        await asyncio.sleep(1)
        return {"status": "ok", "result": "pdf_generated.pdf"}

    await broker.start()

def run() -> None:
    """Synchronous entry point used by console script `mb-receiver`.

    Calls the async `main()` using `asyncio.run` so the package can expose
    a normal, non-async entry point for users unfamiliar with asyncio.
    """
    asyncio.run(main())

if __name__ == "__main__":
    run()
