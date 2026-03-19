"""Transport adapter implementations.

Importing this package triggers adapter self-registration for available
transports while keeping each adapter module independent.
"""

from .redis import RedisBroker

__all__ = ["RedisBroker"]
