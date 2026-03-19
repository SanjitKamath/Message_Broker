"""CLI example that sends a request and waits for a reply.

Run:
    python -m message_broker.cli_messager

Behavior:
    Starts a broker instance, subscribes to reply messages, sends one request,
    waits briefly for a response, then exits.
"""

import asyncio

from message_broker import MessageBroker, ResponsePacket


async def main() -> None:
    """Run the example message sender flow."""

    broker = MessageBroker("redis://127.0.0.1:6379")

    @broker.on_reply
    async def handle_reply(data: ResponsePacket) -> None:
        print(f"Received reply: {data.status} {data.content}")

    runner = asyncio.create_task(broker.start())
    await asyncio.sleep(1)

    await broker.send_message(
        content={"task": "generate_pdf", "data": "some_report_data"},
        sender="messager_app",
        reply=True,
    )

    await asyncio.sleep(5)
    runner.cancel()


def run() -> None:
    """Entrypoint for the mb-messager console script."""

    asyncio.run(main())
