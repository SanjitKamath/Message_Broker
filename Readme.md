# Message Broker

Small async request/response message broker built on FastStream + Redis.

This repository contains a lightweight package `message_broker` that provides
a simple request/reply API plus two example scripts that demonstrate producing
and consuming messages.

Quick overview
- `messager.py`: example producer. Creates a `MessageBroker` instance, sends a
  message (optionally requesting a reply) and listens on a generated reply
  queue for responses.
- `receiver.py`: example consumer/worker. Registers a handler with
  `MessageBroker.on_message` and processes incoming `DataPacket`s. If the
  incoming packet includes `reply_to`, the handler's return value is sent
  back as a `ResponsePacket`.

Folder structure

```
Message_Broker/
├── messager.py                # Example producer (sends messages, listens for replies)
├── receiver.py                # Example consumer (processes messages)
├── requirements.txt           # Runtime deps (faststream, pydantic, redis, ...)
├── message_broker/            # Local package and build metadata
│   ├── pyproject.toml         # package metadata / console scripts
│   ├── Readme.md              # package-level README (API + scheduling docs)
│   └── src/                   # package source (importable as message_broker)
│       ├── __init__.py
│       ├── broker.py          # MessageBroker implementation (FastStream wrapper)
│       ├── schema.py          # Pydantic models: DataPacket, ResponsePacket
│       ├── app_logging.py     # small structlog setup
│       └── cli_*.py           # helper CLI entrypoints for installed scripts
```

Running locally
- Ensure a Redis server is reachable (default examples use `redis://127.0.0.1:6379`).
- Create and activate a virtualenv and install deps:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

- Run a consumer in one shell:

```powershell
python receiver.py
```

- Run the producer in another shell to send a message and wait for the reply:

```powershell
python messager.py
```

Notes
- The package supports scheduled (delayed) delivery: use the `deliver_at`
  field on `DataPacket` to schedule messages; the broker stores scheduled
  packets in a Redis Sorted Set and a background scheduler republishes them
  to the main list when they are due.