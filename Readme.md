# FastStream & Redis Message Queue Demo

A clean, modular, and zero-boilerplate asynchronous messaging application. This project demonstrates how to build a robust producer-consumer architecture using **FastStream**, **Redis**, and **Pydantic** for strict, type-safe data validation.

## 📂 Folder Structure

```text
.
├── app_logging.py   # Configures structured, color-coded terminal logging
├── consumer.py      # The background worker that listens to the Redis queue
├── producer.py      # The script that generates and sends data to the broker
├── requirements.txt # Project dependencies
└── schema.py        # The Pydantic data contract for automatic validation

```

---

## 🚀 Setup Instructions

### Prerequisites

* **Python 3.8+** installed.
* **Redis Broker** running locally on port `6379`.

### Windows Setup

1. **Install Redis:** You can run Redis natively via [Memurai](https://www.memurai.com/get-memurai) or use the official Linux version via WSL2 (`sudo apt install redis-server`).
2. **Open PowerShell** in your project directory.
3. **Create and activate a virtual environment:**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1

```

4. **Install dependencies:**
```powershell
pip install -r requirements.txt

```



### Mac Setup

1. **Install Redis:** The easiest way is via Homebrew.
```bash
brew install redis
brew services start redis

```


2. **Open your Terminal** in your project directory.
3. **Create and activate a virtual environment:**
```bash
python3 -m venv .venv
source .venv/bin/activate

```


4. **Install dependencies:**
```bash
pip install -r requirements.txt

```



---

## ⚙️ How It Works

This application utilizes a **Redis List** to act as a durable message queue.

Here is the exact lifecycle of a message within this system:

1. **The Contract (`schema.py`):** Before any data moves, the `DataPacket` Pydantic model defines the strict rules for the payload (e.g., specific string lengths, priority boundaries).
2. **The Producer (`producer.py`):** The producer creates a `DataPacket` object.
* *Fail-Fast:* If the data is invalid, Pydantic throws an error locally *before* making a network call, preventing bad data from ever entering the system.
* *Publish:* If valid, FastStream serializes the object to JSON and pushes it onto the `demo_queue` Redis List.


3. **The Message Broker (Redis):** Redis holds the message safely in memory. Because we are using a List (instead of a Pub/Sub channel), the message will wait patiently even if the consumer is currently offline.
4. **The Consumer (`consumer.py`):** When online, the FastStream consumer automatically pops the oldest message off the Redis List.
* *Zero-Boilerplate Parsing:* FastStream automatically parses the incoming JSON string back into a `DataPacket` Python object.
* *Processing & Acknowledgment:* The `process_packet` function executes its business logic. Once it finishes without crashing, FastStream automatically sends an acknowledgment to Redis, safely removing the message from the queue forever.


