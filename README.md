# lyik_messaging

## 🤔 Why This Library?

Raw FastStream usage can leak transport details into application code and make handler contracts inconsistent across teams.
`lyik_messaging` adds a strict, typed API over RabbitMQ/Redis with one broker facade, predictable request/response behavior, and clear failure modes.
It keeps transport internals inside the library so app code stays clean, stable, and portable.

## 🚀 Features

- Single `MessageBroker` facade for publish/subscribe and request/response
- Strict handler contract: `(payload: BaseModel, info: MessageInfo)` only
- Built-in correlation-aware `ResponsePacket` request/reply workflow
- Transparent AES-GCM encryption support
- Configurable retry policy for handlers and middleware

## 📦 Installation

```bash
pip install lyik_messaging
```

For local development:

```bash
pip install -e .
```

## ⚡ Quick Start

```python
import asyncio
from pydantic import BaseModel

from lyik_messaging import MessageBroker, MessageInfo


class Ping(BaseModel):
    text: str


broker = MessageBroker(
    "amqp://guest:guest@localhost:5672/",
    queue_name="demo.queue",
)


@broker.on_message("demo.queue")
async def handle_ping(payload: Ping, info: MessageInfo) -> dict[str, str]:
    return {"echo": payload.text, "correlation_id": info.correlation_id or ""}


@broker.on_reply
async def handle_reply(response) -> None:
    print("reply:", response.status, response.content)


async def main() -> None:
    await broker.connect()

    correlation_id = await broker.send_message(
        {"text": "fire-and-forget"},
        sender="demo-client",
    )
    print("sent correlation_id:", correlation_id)

    response = await broker.send_and_wait(
        {"text": "request"},
        sender="demo-client",
        timeout=5.0,  # required
    )
    print("sync response:", response.content)

    await broker.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
```

## 📚 Complete API Reference

### `MessageBroker(...)`

```python
MessageBroker(
    uri: str,
    queue_name: str = "default_queue",
    *,
    aes_key: str | None = None,
    processing_timeout_ms: int | None = None,
    handler_max_retries: int = 0,
    retry_base_delay_ms: int = 100,
    retry_max_delay_ms: int | None = None,
    retry_jitter: bool = False,
    middlewares: list[Middleware] = [],
    response_store_ttl_seconds: float | None = None,
)
```

Purpose:

- Create a broker facade with strict handler validation and transport abstraction.

Parameters:

- `uri`: Broker URI. Supported schemes: `amqp://`, `amqps://`, `redis://`, `rediss://`.
- `queue_name`: Default queue/topic used by send APIs.
- `aes_key`: Optional AES key; enables transparent AES-GCM middleware.
- `processing_timeout_ms`: Optional per-handler execution timeout.
- `handler_max_retries`: Max retry count for handler and middleware failures.
- `retry_base_delay_ms`: Base retry delay for exponential backoff.
- `retry_max_delay_ms`: Optional retry delay cap.
- `retry_jitter`: Add jitter when `True`.
- `middlewares`: `list[Middleware]` for publish/consume hooks.
- `response_store_ttl_seconds`: Optional TTL for responses saved by `send_and_store`.

Return value:

- `MessageBroker`.

Example:

```python
broker = MessageBroker("redis://localhost:6379", queue_name="orders")
```

### `send_message(...)`

Purpose:

- Fire-and-forget publish on the default queue.

When to use this:

- Event notifications and async workflows where caller does not need immediate response.

When NOT to use this:

- When the caller must receive a result before continuing.

Parameters:

- `content`: JSON-serializable payload.
- `sender`: Sender identifier.
- `reply`: If `True`, attaches `reply_to`.
- `deliver_at`: Optional UTC datetime for delayed publish.

Return value:

- `str` correlation ID.

Example:

```python
correlation_id = await broker.send_message(
    content={"event": "user.created"},
    sender="user-service",
)
```

### `send_and_wait(...)`

Purpose:

- Send request and wait for one correlated `ResponsePacket`.

When to use this:

- RPC-like flows where immediate response is required.

When NOT to use this:

- High-throughput async pipelines where blocking on responses reduces throughput.

Parameters:

- `content`: JSON-serializable payload.
- `sender`: Sender identifier.
- `timeout`: Required, must be `> 0`.
- `deliver_at`: Optional UTC datetime for delayed publish.

Return value:

- `ResponsePacket`.

Example:

```python
response = await broker.send_and_wait(
    content={"op": "healthcheck"},
    sender="api-gateway",
    timeout=3.0,
)
print(response.status, response.content)
```

### `send_and_store(...)`

Purpose:

- Send request, wait for response, then store it for retrieval.

When to use this:

- You need request/reply semantics but retrieval is decoupled from execution path.

When NOT to use this:

- You need the response value immediately in the same code path.

Parameters:

- `content`: JSON-serializable payload.
- `sender`: Sender identifier.
- `timeout`: Required, must be `> 0`.
- `deliver_at`: Optional UTC datetime for delayed publish.

Return value:

- `str` response ID for later lookup.

Example:

```python
response_id = await broker.send_and_store(
    content={"job_id": "abc-123"},
    sender="scheduler",
    timeout=10.0,
)
```

### `get_response(response_id)`

Purpose:

- Retrieve response previously stored by `send_and_store`.

When to use this:

- Poll-style workflows that fetch results by ID.

Parameters:

- `response_id`: ID returned by `send_and_store`.

Return value:

- `ResponsePacket | None`.

Example:

```python
stored = broker.get_response(response_id)
if stored:
    print(stored.content)
```

### `publish(...)` (Advanced/Internal)

Purpose:

- Low-level publish primitive for internal or specialized integration logic.

When to use this:

- You need explicit control over topic, headers, correlation, or response routing.

When NOT to use this:

- Standard application-level publish or request/reply flows.

Parameters:

- `topic`: Target queue/topic.
- `payload`: Raw payload.
- `headers`: Optional string headers.
- `correlation_id`: Optional correlation ID override.
- `reply_to`: Optional reply destination.
- `is_response`: Marks message as response in middleware flow.

Return value:

- `str` correlation ID used for publish.

Example:

```python
corr_id = await broker.publish(
    topic="internal.queue",
    payload={"raw": True},
    headers={"x-source": "custom"},
)
```

### `on_message("queue")`

Purpose:

- Register one strict consumer handler for one queue.

When to use this:

- Typed consumer endpoints that must enforce payload and metadata contracts.

Parameters:

- `queue`: Non-empty queue/topic name.

Return value:

- Decorator registering the handler.

Required handler signature:

```python
async def handler(payload: BaseModel, info: MessageInfo) -> object:
    ...
```

Example:

```python
class OrderCreated(BaseModel):
    order_id: str


@broker.on_message("orders.created")
async def handle_order(payload: OrderCreated, info: MessageInfo) -> dict[str, str]:
    return {"order_id": payload.order_id, "corr": info.correlation_id or ""}
```

### `on_reply`

Purpose:

- Register callback for correlated `ResponsePacket` replies.

When to use this:

- Centralized reply processing, observability, or custom response side-effects.

Parameters:

- `handler`: Optional function; can also be used as decorator.

Return value:

- Wrapped async callback registration.

Example:

```python
@broker.on_reply
async def handle_reply(response) -> None:
    if response.status != "processed":
        print("request failed:", response.content)
```

### Top-level `connect(...)`

Purpose:

- Convenience helper that constructs and connects `MessageBroker`.

Parameters:

- `uri`, `aes_key`, and all `MessageBroker` keyword options via `**kwargs`.
- `auto_start`: If `True`, starts broker loop in a background task.

Return value:

- Connected `MessageBroker`.

Example:

```python
from lyik_messaging import connect

broker = await connect("redis://localhost:6379", queue_name="demo")
```

## 🧭 Choosing the Right API

| API | Best for | Avoid when |
| --- | --- | --- |
| `send_message` | Fire-and-forget events | You need immediate response data |
| `send_and_wait` | Synchronous request/response | Very high-throughput async-only flows |
| `send_and_store` | Deferred response retrieval by ID | You need response inline immediately |
| `publish` | Advanced/internal transport control | Normal application messaging |

## 🧠 Core Concepts

- `MessageBroker`: public facade for lifecycle, publish, consume, and request/reply.
- `DataPacket`: request envelope with sender, payload, correlation, and optional `reply_to`.
- `ResponsePacket`: normalized response envelope.
- `MessageInfo`: metadata passed to strict handlers (`correlation_id`, `reply_to`, `sender`, `raw`).
- `Middleware`: extensibility hooks for publish/consume processing.

## 🔄 Message Flow

```mermaid
flowchart LR
    A[Producer] --> B[send_message or send_and_wait]
    B --> C[Broker Transport]
    C --> D[@broker.on_message("queue")]
    D --> E[ResponsePacket]
    E --> C
    C --> F[@broker.on_reply]
```

## 🔐 Encryption Flow

AES-GCM is transparent when `aes_key` is configured.

- Producer encrypts before publish.
- Consumer decrypts before validation and handler execution.
- Both sides must use the same key.
- Key mismatch or tampering causes decryption failure (`EncryptionError`).

## 🔁 Retry Behavior

Backoff formula:

```text
delay = retry_base_delay_ms * (2 ** retry_index)
```

Applies to:

- handler execution
- publish middleware
- consume middleware

Does not apply to:

- decrypt/decode failures
- payload parsing/validation failures

## ⚠️ Redis vs RabbitMQ

RabbitMQ:

- Native request/response behavior
- Stronger request/reply guarantees

Redis:

- Fallback reply-queue + correlation matching
- Works well for many cases, but semantics are weaker than RabbitMQ
- Strict ordering and RPC-like guarantees are not equivalent

## 🧩 Design Principles

- Strict typing: handlers are validated early and enforced consistently.
- Fail-fast validation: invalid configuration/signatures fail at registration or startup.
- Minimal API surface: a few clear entry points cover common workflows.
- Transport abstraction: FastStream internals stay inside the library boundary.

## 🧱 Project Structure

```text
src/
  lyik_messaging/
    __init__.py        # Public exports and connect helper
    decorators.py      # Strict handler signature validation
    encryption.py      # AES-GCM middleware integration
    exceptions.py      # Public exception hierarchy
    models.py          # DataPacket, ResponsePacket, MessageInfo, Middleware
    utils.py           # Broker scheme resolution/helpers
    broker/
      __init__.py      # MessageBroker facade
      core.py          # Broker creation and lifecycle
      consumer.py      # Subscription and handler execution
      publisher.py     # Publish/request/store logic
      retry.py         # Retry policy
      internal_types.py
```

## 🔌 Adding a New Broker (Maintainers)

Checklist:

1. Update broker creation in `broker/core.py` (`BrokerCore._create_broker`).
2. Map URI scheme to the new broker implementation.
3. Ensure publish path works (`send_message`, `publish`).
4. Ensure request/response works (`send_and_wait`, `send_and_store`).
5. Validate correlation handling end-to-end.
6. Test middleware compatibility for publish and consume paths.

Warning:

If FastStream types appear in public API, abstraction is broken.

## 🛠️ Troubleshooting

### Invalid handler signature

Symptom:

- `ConfigurationError` during `@broker.on_message("queue")` registration.

Cause:

- Handler is not exactly `(payload: BaseModel, info: MessageInfo)`.

Fix:

- Use strict two-argument signature with explicit type annotations.

### Request timed out

Symptom:

- `TimeoutError` from `send_and_wait` or `send_and_store`.

Cause:

- Missing/slow consumer, wrong queue, or timeout too low.

Fix:

- Ensure consumer is running on the same queue and increase `timeout` when needed.

### Encryption mismatch

Symptom:

- `EncryptionError` or invalid authentication tag failures.

Cause:

- Different `aes_key` values across producer/consumer or payload tampering.

Fix:

- Use identical key material on both ends and verify key distribution.

### Missing consumers for request/reply

Symptom:

- Request/reply queue validation errors or warnings about absent handlers.

Cause:

- Queue used for request/reply has no strict consumer registered.

Fix:

- Register `@broker.on_message("queue")` handler and run the consumer process.
