"""Top-level package shim for local development.

The package intentionally uses lazy exports so importing submodules such as
``message_broker.src.adapters.redis`` does not force loading optional legacy
dependencies unrelated to the selected adapter path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
	from .src.broker import MessageBroker
	from .src.schema import DataPacket, Payload, ResponsePacket
	from .src.core.interfaces import Message, Middleware, Broker

__all__ = [
	"MessageBroker",
	"DataPacket",
	"ResponsePacket",
	"Payload",
	"Message",
	"Middleware",
	"connect",
]


async def connect(connection_uri: str, **kwargs: Any) -> "Broker":
	"""
	Initialize a broker from a URI with automatic adapter selection.
	- The URI scheme determines the adapter (e.g., "redis://..." uses the Redis adapter).
	- Optional kwargs are passed to the adapter's connection method.
	- Some of the additional kwargs may be as follows:
		- `middlewares`: List of middleware instances to apply to the broker.
		- `logger`: Optional logger instance for adapter logging.
		- `timeout`: Optional default timeout for broker operations (in milliseconds).
		- `max_retries`: Optional retry count for transient connection failures.
		- `observers`: Optional list of observability callbacks for message lifecycle events.
	"""

	from .src import connect as _connect

	return await _connect(connection_uri, **kwargs)


def __getattr__(name: str) -> Any:
	"""Resolve public symbols lazily to keep optional imports decoupled."""

	if name == "MessageBroker":
		from .src.broker import MessageBroker

		return MessageBroker
	if name in {"DataPacket", "Payload", "ResponsePacket"}:
		from .src.schema import DataPacket, Payload, ResponsePacket

		mapping = {
			"DataPacket": DataPacket,
			"Payload": Payload,
			"ResponsePacket": ResponsePacket,
		}
		return mapping[name]
	if name in {"Message", "Middleware"}:
		from .src.core.interfaces import Message, Middleware

		mapping = {
			"Message": Message,
			"Middleware": Middleware,
		}
		return mapping[name]

	raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
