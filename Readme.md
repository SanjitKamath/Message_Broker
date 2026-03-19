# Message Broker

Production-grade, modular async message broker with pluggable adapters for Redis, Kafka, RabbitMQ, and in-memory transports.

## Overview

`message_broker` is a unified, adapter-based messaging package that decouples application code from transport specifics. 

**Key features:**
- **Unified async interfaces** - Single `Publisher`, `Subscriber`, and `Broker` contracts for all transports
- **Pluggable adapters** - Redis, Kafka, RabbitMQ, and in-memory adapters available; add new ones without changing core  
- **Registry pattern** - Dynamic adapter loading from URI schemes (e.g., `redis://`, `kafka://`, `amqp://`)
- **Resilience built-in** - Automatic retry with exponential backoff, connection pooling, graceful shutdown
- **Type-safe** - 100% type hints (mypy strict), Google-style docstrings throughout
- **Async-first** - Full asyncio native from ground up; no sync wrappers

## Architecture

The package is organized into three layers:

### Core (`src/core/`)
Broker-agnostic primitives and contracts:
- **`interfaces.py`** - Abstract `Broker`, `Publisher`, `Subscriber`, and `Message` contracts
- **`context.py`** - `BrokerContext` parses URIs, merges config, and validates options
- **`registry.py`** - `BrokerRegistry` maps adapter names to factories (e.g., "redis" → RedisBroker)
- **`exceptions.py`** - Unified exception hierarchy (wraps transport-specific errors as `ConnectionLostError`, `PublishFailedError`)
- **`resilience.py`** - `@with_retries` decorator for exponential backoff on transient failures

### Adapters (`src/adapters/`)
Transport implementations (each fully decoupled):
- **`redis.py`** (built-in) - Uses `redis.asyncio` for list-based FIFO queues
- **`kafka.py`** (built-in) - Uses `aiokafka` for topic-based pub/sub
- **`rabbitmq.py`** (planned) - Uses `aio_pika` for AMQP broker patterns
- **`memory.py`** (built-in) - In-memory adapter for testing, no external deps

### Examples
- **`producer.py`** - Publishes a message via `BrokerContext` + `BrokerRegistry`
- **`consumer.py`** - Subscribes and processes messages with async handler

### Demo Modes

The top-level demo scripts now support two modes:

- **Simple mode (default)** - Minimal producer/consumer flow for quick demos
- **Advanced mode (`--advanced`)** - Full middleware, validation, metrics, and multi-topic showcase

Run simple demo:

```powershell
# Terminal 1
python consumer.py

# Terminal 2
python producer.py
```

Run advanced demo:

```powershell
# Terminal 1
python consumer.py --advanced

# Terminal 2
python producer.py --advanced
```

## Quick Start

### Installation

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e message_broker
```

Or with specific adapters:
```powershell
# Redis only (base + redis)
pip install -e message_broker

# Kafka support (includes aiokafka)
pip install -e "message_broker[kafka]"
```

### Basic Usage

```python
from message_broker import Message, connect

async with await connect("redis://localhost:6379") as broker:
    publisher = broker.get_publisher()
    subscriber = broker.get_subscriber()
    
    # Publish
    await publisher.publish("my_topic", Message(
        payload={"data": "example"},
        headers={"sender": "app1"}
    ))
    
    # Subscribe
    async def handle(msg: Message):
        print(f"Received: {msg.payload}")
    
    await subscriber.subscribe("my_topic", handle)
```

## Getting Started

```python
from message_broker import MessageBroker

broker = MessageBroker("redis://localhost:6379")

@broker.on_message
async def handler(data):
    return {"ok": True}

await broker.connect()
await broker.start()
```

```python
await broker.send_message(content={"task": "ping"}, sender="app", reply=True)
```

## Tier 2 Enterprise Features

The message broker includes three production-grade extensibility features designed for enterprise deployments:

### 1. Pluggable Serialization

Swap serialization strategies without changing adapter code. Default uses JSON, but you can provide custom serializers (MessagePack, Protobuf, etc.):

```python
from message_broker import Serializer

class ProtobufSerializer(Serializer):
    """Custom serializer using Protocol Buffers."""
    
    async def serialize(self, payload: dict) -> str:
        """Serialize dict to Protobuf bytes (as string)."""
        # Your protobuf serialization logic
        return protobuf.dumps(payload).decode("utf-8")
    
    async def deserialize(self, data: str) -> dict:
        """Deserialize Protobuf bytes back to dict."""
        return protobuf.loads(data.encode("utf-8"))

# Use the custom serializer
async with await connect(
    "redis://localhost",
    serializer=ProtobufSerializer()
) as broker:
    # All publish/subscribe operations now use ProtobufSerializer
    pass
```

### 2. Middleware/Interceptor Layer

Inject business logic (logging, tracing, validation, metrics) into publish and consume lifecycles without modifying adapters:

```python
from message_broker import Middleware

class LoggingMiddleware(Middleware):
    """Logs all published and consumed messages."""
    
    async def before_publish(self, topic: str, message: Message) -> Message:
        """Called before message is serialized and sent."""
        print(f"Publishing to {topic}: {message.payload}")
        return message  # Must return message (can be modified)
    
    async def after_consume(self, topic: str, message: Message) -> Message:
        """Called after message is deserialized, before handler."""
        print(f"Consumed from {topic}: {message.payload}")
        return message

class ValidationMiddleware(Middleware):
    """Validates payloads before publishing."""
    
    async def before_publish(self, topic: str, message: Message) -> Message:
        # Validate and optionally transform the message
        if not isinstance(message.payload, dict):
            raise ValueError("Payload must be a dict")
        return message
    
    async def after_consume(self, topic: str, message: Message) -> Message:
        return message

# Middleware are applied in order
async with await connect(
    "redis://localhost",
    middlewares=[ValidationMiddleware(), LoggingMiddleware()]
) as broker:
    # Middleware run for every publish and consume call
    pass
```

### 3. Namespaced Context Options

Prevent configuration key collisions when using multiple brokers or broker-specific settings:

```python
async with await connect(
    "redis://localhost",
    timeout=5000,  # Global option (applies to all brokers)
    max_retries=3,
    # Broker-specific options nested under scheme
    redis={
        "key_prefix": "my_app_",
        "connection_pool_kwargs": {"max_connections": 10}
    },
    kafka={
        "group_id": "consumer-group-1",
        "auto_offset_reset": "earliest"
    }
) as broker:
    # The context separates global options from broker-specific ones
    pass
```

### Combining All Three Features

```python
from message_broker import Message, Middleware, Serializer, connect

# Custom serializer
class CompactJsonSerializer(Serializer):
    async def serialize(self, data: dict) -> str:
        return json.dumps(data, separators=(",", ":"))
    
    async def deserialize(self, data: str) -> dict:
        return json.loads(data)

# Middleware stack: validation → metrics → logging
class MetricsMiddleware(Middleware):
    def __init__(self):
        self.count = 0
    
    async def before_publish(self, topic: str, message: Message) -> Message:
        self.count += 1
        print(f"Published {self.count} messages")
        return message
    
    async def after_consume(self, topic: str, message: Message) -> Message:
        return message

class ValidationMiddleware(Middleware):
    async def before_publish(self, topic: str, message: Message) -> Message:
        required_fields = {"id", "timestamp"}
        if not required_fields.issubset(message.payload.keys()):
            raise ValueError(f"Missing fields: {required_fields - set(message.payload.keys())}")
        return message
    
    async def after_consume(self, topic: str, message: Message) -> Message:
        return message

# Compose everything
async with await connect(
    "redis://localhost:6379",
    serializer=CompactJsonSerializer(),
    middlewares=[
        ValidationMiddleware(),
        MetricsMiddleware(),
    ],
    timeout=5000,
    redis={"key_prefix": "myapp_"}
) as broker:
    publisher = broker.get_publisher()
    
    # All three features now work together
    await publisher.publish("events", Message(
        payload={"id": "123", "timestamp": time.time()}
    ))
```

## Tier 3 Advanced Features

### 1. Broker Capabilities Registry

Introspect broker features before using them to prevent silent failures. Some backends natively support certain features (like scheduled delivery) while others don't.

```python
from message_broker import BrokerCapability, connect

async with await connect("redis://localhost:6379") as broker:
    # Check what this broker can do
    if BrokerCapability.DELAYED_DELIVERY in broker.capabilities:
        # Safe to use deliver_at parameter
        await publisher.publish(topic, msg, deliver_at=time.time() + 3600)
    else:
        # Fall back to application-layer scheduling
        logger.warning("Broker does not support delayed delivery")
```

**Supported Capabilities:**
- `BrokerCapability.DELAYED_DELIVERY` - Scheduled message delivery with `deliver_at` parameter (Redis ✅, Kafka ❌)
- `BrokerCapability.BATCH_PUBLISH` - Atomic multi-message send (planned)
- `BrokerCapability.RPC_REPLIES` - Request-reply patterns (planned)

### 2. Backpressure & Worker Pools

Prevent Out-of-Memory crashes under heavy load by enforcing backpressure limits. The subscriber spawns:
- **1 consumer task** - Pulls messages from the broker
- **N worker tasks** - Process messages concurrently
- **1 bounded queue** - Acts as a buffer with natural backpressure

When the queue fills, the consumer automatically pauses pulling from Redis/Kafka. This prevents unbounded memory growth and allows graceful degradation under load.

```python
# Configure backpressure limits
async with await connect(
    "redis://localhost:6379",
    concurrency=10,        # Number of concurrent worker tasks
    max_queue_size=100,    # Max messages held in memory
) as broker:
    subscriber = broker.get_subscriber()
    
    async def slow_handler(msg: Message) -> None:
        # Handler can be slow - backpressure ensures we don't OOM
        await database.insert(msg.payload)
    
    await subscriber.subscribe("incoming", slow_handler)
```

**How Backpressure Works:**

```
Timeline with max_queue_size=10, concurrency=3:

T=0s:   Consumer pulls 10 messages (queue full)
        Queue: [msg1, msg2, ... msg10]
        
T=0s:   3 worker tasks start processing
        Active: [worker1(msg1), worker2(msg2), worker3(msg3)]
        
T=0s:   Consumer blocks on queue.put() → natural backpressure
        
T≈0.5s: 1 worker finishes, calls queue.task_done()
        Queue has 1 slot → consumer unblocks
        
T≈0.5s: Consumer pulls next message, queue fills again
        Queue: [msg4, msg5, ... msg13]
```

**Benefits:**
- ✅ Zero unbounded growth - queue never exceeds `max_queue_size`
- ✅ Natural throttling - consumer pauses automatically
- ✅ Parallel processing - N workers handle messages concurrently
- ✅ No busy-waiting - all tasks are async-awaited

### 3. Configuration Tuning for Load

Default settings balance safety and throughput:

```python
# For I/O-bound workloads (slow network calls):
async with await connect(
    "redis://localhost:6379",
    concurrency=50,        # More concurrent workers
    max_queue_size=500,    # Larger buffer
) as broker:
    pass

# For CPU-bound workloads (heavy computation):
async with await connect(
    "redis://localhost:6379",
    concurrency=4,         # Match CPU core count
    max_queue_size=50,     # Smaller buffer (prevent memory build-up)
) as broker:
    pass
```

## Usage Examples

The `producer.py` and `consumer.py` files in the root directory demonstrate:
- Publishing with middleware and validation
- Consuming with metrics middleware
- Scheduled delivery (Redis only)
- Multi-topic consumption with different handlers

The `tier3_example.py` demonstrates Tier 3 features:
- Introspecting broker capabilities
- Configuring backpressure limits
- Observing worker pool behavior
- Testing OOM prevention

### Running the Examples

Ensure a Redis server is running:
```powershell
# If you have Docker
docker run -p 6379:6379 redis
```

Then run:
```powershell
# In one terminal (consumer starts listening)
python consumer.py

# In another (producer publishes messages)
python producer.py

# Tier 3 example (shows capabilities and backpressure)
python tier3_example.py
```

## Module Map

```
message_broker/
├── src/
│   ├── core/
│   │   ├── __init__.py
│   │   ├── interfaces.py      # Abstract Publisher, Subscriber, Broker, Message
│   │   ├── context.py         # BrokerContext: URI parsing + option merging
│   │   ├── registry.py        # BrokerRegistry: adapter factory mapping
│   │   ├── exceptions.py      # MessageBrokerError hierarchy
│   │   └── resilience.py      # @with_retries decorator + backoff
│   ├── adapters/
│   │   ├── __init__.py
│   │   ├── redis.py           # RedisPublisher/Subscriber/Broker (LPUSH/BRPOP)
│   │   ├── kafka.py           # KafkaPublisher/Subscriber/Broker (aiokafka)
│   │   ├── rabbitmq.py        # RabbitMQPublisher/Subscriber/Broker (aio_pika)
│   │   └── memory.py          # In-memory adapter (asyncio.Queue)
│   ├── broker.py              # Legacy MessageBroker (FastStream wrapper)
│   ├── schema.py              # Pydantic models (legacy)
│   ├── app_logging.py         # Structlog setup
│   └── __init__.py
├── pyproject.toml
└── Readme.md
```

## Connection URIs

Adapter selection is automatic from URI scheme:

| Scheme | Adapter | Example |
|--------|---------|---------|
| `redis://` | Redis | `redis://localhost:6379` |
| `rediss://` | Redis (TLS) | `rediss://localhost:6380` |
| `kafka://` | Kafka | `kafka://localhost:9092` |
| `amqp://` | RabbitMQ | `amqp://user:pass@localhost:5672//` |
| `amqps://` | RabbitMQ (TLS) | `amqps://user:pass@localhost:5671//` |
| `memory://` | In-Memory | `memory://` |

## Design Principles

1. **No transport leakage** - Adapters wrap backend exceptions in unified types
2. **Async native** - All public APIs are async; no hidden blocking calls
3. **Configuration centralized** - `BrokerContext` handles URI parsing, merging defaults, and validation
4. **Modularity** - Deleting any adapter doesn't break the rest of the package
5. **Testability** - In-memory adapter enables deterministic, speed-of-light tests

## Contributing

To add a new adapter:

1. Create `src/adapters/mybroker.py` implementing abstract classes
2. At module bottom, register with `BrokerRegistry.register("mybroker", factory)`
3. Import in `src/adapters/__init__.py` to trigger auto-registration
4. Add URI scheme mapping in `src/core/context.py` if needed

## License

LYIK