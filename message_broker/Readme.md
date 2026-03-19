# 🚀 message_broker — Enterprise Async Messaging Framework

> A transport-agnostic, asyncio-native messaging framework designed for **high-performance, scalable, and resilient distributed systems**.

---

## 📖 Overview

`message_broker` is a **production-grade messaging abstraction layer** that lets you build asynchronous, event-driven systems **without coupling your application to a specific broker** like Redis or Kafka.

Instead of writing broker-specific code, you write against a **unified interface**, and the framework handles:

* connection management
* serialization
* retries & resilience
* backpressure
* observability
* broker capability differences

---

## 🎯 What It Solves

* Tight coupling to Redis/Kafka APIs
* Inconsistent retry/error handling
* Lack of backpressure → memory crashes
* Hard-to-maintain messaging logic

✔️ Provides:

* clean abstraction layer
* pluggable adapters
* safe async processing
* enterprise-level features out of the box

---

## ✨ Core Features

* 🔌 **Pluggable Broker Adapters** (Redis, Kafka, extensible)
* ⚡ **Async-first architecture** (`asyncio` native)
* 🧠 **Backpressure-aware consumers**
* 🔁 **Retry with exponential backoff**
* 📡 **Observability hooks (OpenTelemetry ready)**
* 🧩 **Middleware system**
* 🧱 **Strict typing & schema validation**
* 🧬 **Capability-based feature enforcement**
* 🔄 **Built-in RPC (request-response pattern)**

---

## 📦 Installation

```bash
git clone https://github.com/SanjitKamath/Message_Broker.git
cd message_broker

python -m venv .venv
source .venv/bin/activate   # Windows: .\.venv\Scripts\activate

pip install -e .

# Optional (OpenTelemetry)
pip install -e .[otel]
```

---

## ⚡ Quick Example

```python
import asyncio
from message_broker import MessageBroker

async def main():
    broker = MessageBroker("redis://127.0.0.1:6379")

    @broker.on_message
    async def handle(data):
        print(data.content)
        return {"status": "ok"}

    await broker.start()

asyncio.run(main())
```

---

# 🧩 Architecture Overview

```
Application Layer
      ↓
MessageBroker (High-Level API)
      ↓
Core Layer (Interfaces, Context, Registry)
      ↓
Adapters (Redis, Kafka, Custom)
      ↓
Actual Broker (Redis/Kafka/etc.)
```

---

## 📂 Folder Structure

```text
message_broker/
├── src/
│   ├── core/
│   │   ├── context.py          # Connection parsing + config merging
│   │   ├── interfaces.py       # Contracts (Publisher, Subscriber, Broker)
│   │   ├── registry.py         # Adapter registration system
│   │   ├── resilience.py       # Retry logic
│   │   ├── serializers.py      # Serialization layer
│   │   └── observability.py    # Middleware + tracing
│   │
│   └── adapters/
│       ├── redis.py            # Redis implementation
│       └── kafka.py            # Kafka implementation
│
├── broker.py                   # High-level API (MessageBroker)
├── schema.py                   # DataPacket / ResponsePacket
├── cli_messager.py             # Example sender
├── cli_receiver.py             # Example receiver
├── pyproject.toml
└── README.md
```

---

# ⚙️ How It Works (Core Flow)

### 1. Context Creation

```python
context = BrokerContext("redis://localhost:6379")
```

* Parses URI
* Merges config
* Sets defaults

---

### 2. Broker Resolution

```python
broker = BrokerRegistry.create(context)
```

* Finds correct adapter
* Instantiates broker dynamically

---

### 3. Publishing

```python
await publisher.publish("topic", message)
```

Flow:

```
Message → Middleware → Serializer → Adapter → Broker
```

---

### 4. Consuming

```
Broker → Adapter → Queue → Workers → Middleware → Handler
```

* Uses bounded queue → prevents overload
* Workers process messages concurrently

---

### 5. RPC (Request-Response)

```
send_and_wait()
   ↓
Temporary reply queue
   ↓
Correlation ID tracking
   ↓
Response matched automatically
```

---

# 📊 API Reference

## 🔹 High-Level API (Recommended)

| Function             | Description            | Example                           |
| -------------------- | ---------------------- | --------------------------------- |
| `MessageBroker(uri)` | Create broker instance | `MessageBroker("redis://...")`    |
| `connect()`          | Connect to broker      | `await broker.connect()`          |
| `start()`            | Start consuming        | `await broker.start()`            |
| `send_message()`     | Send message           | `await broker.send_message(...)`  |
| `send_and_wait()`    | RPC request-response   | `await broker.send_and_wait(...)` |
| `on_message()`       | Register handler       | `@broker.on_message`              |
| `on_reply()`         | Register reply handler | `@broker.on_reply`                |
| `disconnect()`       | Graceful shutdown      | `await broker.disconnect()`       |

---

## 🔹 Core API (Advanced)

| Component        | Purpose                         |
| ---------------- | ------------------------------- |
| `BrokerContext`  | Parses config + manages options |
| `BrokerRegistry` | Resolves adapters dynamically   |
| `Message`        | Transport-neutral message       |
| `Publisher`      | Sends messages                  |
| `Subscriber`     | Consumes messages               |
| `Serializer`     | Handles encoding/decoding       |
| `Middleware`     | Inject cross-cutting logic      |

---

# 🏗️ Advanced Usage

## 🔁 Retry Configuration

```python
BrokerContext(
    "redis://...",
    max_retries=5
)
```

---

## ⚡ Backpressure Control

```python
BrokerContext(
    "redis://...",
    concurrency=20,
    max_queue_size=500
)
```

---

## 🔍 Middleware Example

```python
from message_broker.src.core.observability import MetricsMiddleware

BrokerContext(
    "redis://...",
    middlewares=[MetricsMiddleware()]
)
```

---

## 🧠 Capability Check

```python
if BrokerCapability.DELAYED_DELIVERY in broker.capabilities:
    ...
```

---

# 🔌 Extending the Framework

## ➕ Add a New Broker

### Step 1: Create adapter

```text
src/adapters/mybroker.py
```

---

### Step 2: Implement interfaces

* `Publisher`
* `Subscriber`
* `Broker`

---

### Step 3: Register adapter

```python
BrokerRegistry.register("mybroker", lambda ctx: MyBroker(ctx))
```

---

### Step 4: Use it

```python
MessageBroker("mybroker://localhost")
```

---

## ➖ Remove a Broker

* Delete adapter file
* Remove import
* (Optional) unregister from registry

---

## 🔄 Replace a Broker

No code changes needed:

```python
# Switch from Redis → Kafka
MessageBroker("kafka://localhost:9092")
```

---

# 🧪 CLI Examples

```bash
python -m message_broker.cli_receiver
python -m message_broker.cli_messager
```

---

# ⚙️ Configuration System

Supports:

* URI query params
* kwargs
* environment overrides

```python
BrokerContext(
    "redis://localhost:6379?timeout=3000",
    concurrency=10,
    max_retries=3
)
```