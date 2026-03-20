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
	from .src.core.interfaces import Message, Middleware, BrokerCapability, Broker

__all__ = [
	"MessageBroker",
	"DataPacket",
	"ResponsePacket",
	"Payload",
	"Message",
	"Middleware",
	"BrokerCapability",
	"connect",
]


async def connect(connection_uri: str, **kwargs: Any) -> "Broker":
	"""Initialize a broker from a URI with automatic adapter selection."""

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
	if name in {"Message", "Middleware", "BrokerCapability"}:
		from .src.core.interfaces import Message, Middleware, BrokerCapability

		mapping = {
			"Message": Message,
			"Middleware": Middleware,
			"BrokerCapability": BrokerCapability,
		}
		return mapping[name]

	raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
