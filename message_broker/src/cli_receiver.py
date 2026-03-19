"""CLI example that receives requests and returns handler responses.

Run:
    python -m message_broker.cli_receiver

Behavior:
    Starts a broker consumer on the default queue, processes incoming packets,
    and returns a small success payload for each message.
"""

import asyncio

from message_broker import DataPacket, MessageBroker


async def main() -> None:
    """Run the example receiver flow."""

    broker = MessageBroker("redis://127.0.0.1:6379", queue_name="default_queue")

    @broker.on_message
    async def handle_message(data: DataPacket) -> dict[str, str]:
        print(f"Received message: {data.content}")
        await asyncio.sleep(1)
        return {"status": "ok", "result": "pdf_generated.pdf"}

    await broker.start()


def run() -> None:
    """Entrypoint for the mb-receiver console script."""

    asyncio.run(main())
