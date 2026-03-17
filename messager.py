import asyncio
from message_broker import MessageBroker, ResponsePacket
from datetime import datetime, timedelta, timezone

async def main():
    broker = MessageBroker("redis://127.0.0.1:6379")

    @broker.on_reply
    async def handle_reply(data: ResponsePacket):
        print(f"Received reply: {data.content}")

    # Start the broker to listen for replies
    runner = asyncio.create_task(broker.start())
    await asyncio.sleep(1)  # Give it a moment to start

    # Send a message and expect a reply
    await broker.send_message(
        content={"task": "generate_pdf", "data": "some_report_data"},
        sender="messager_app",
        reply=True,
        deliver_at=datetime.now(timezone.utc)+timedelta(seconds=25)
    )

    await asyncio.sleep(30)  # Wait for reply
    runner.cancel()

def run() -> None:
    """Synchronous entry point used by console script `mb-messager`.

    Calls the async `main()` using `asyncio.run` so the package can expose
    a normal, non-async entry point for users unfamiliar with asyncio.
    """
    asyncio.run(main())

if __name__ == "__main__":
    run()

# Before you run this, if you are using windows and have docker installed, you can run a Redis instance with:
# docker run -p 6379:6379 redis
# Else if you have WSL, create the venv in WSL and install message_broker there, then ensure redis is running in WSL with:
# redis-cli ping