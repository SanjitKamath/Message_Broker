"""Transport adapter implementations.

Importing this package triggers adapter self-registration for available
transports while keeping each adapter module independent.
"""

from .redis import RedisBroker

try:
	from .rabbit import RabbitMQBroker
except ModuleNotFoundError:
	RabbitMQBroker = None  # type: ignore[assignment]

__all__ = ["RedisBroker", "RabbitMQBroker"]
