import asyncio

from message_broker import MessageBroker, ResponsePacket


async def main() -> None:
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
    asyncio.run(main())
