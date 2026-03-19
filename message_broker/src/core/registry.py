"""Adapter registry for dynamic broker resolution.

This module implements the Registry pattern so the core package can instantiate
broker adapters without hardcoding transport-specific imports.
"""

from __future__ import annotations

from collections.abc import Callable
from importlib import metadata
from typing import ClassVar

from .context import BrokerContext
from .exceptions import ConfigurationError
from .interfaces import Broker

BrokerFactory = Callable[[BrokerContext], Broker]


class BrokerRegistry:
    """Stores and resolves broker adapter factory functions.

    Adapters self-register by name, allowing runtime selection from
    configuration without adding import-time coupling in the core layer.
    """

    _factories: ClassVar[dict[str, BrokerFactory]] = {}
    _plugins_loaded: ClassVar[bool] = False

    @classmethod
    def register(cls, name: str, factory: BrokerFactory) -> None:
        """Register a broker factory under a normalized name.

        Args:
            name: Canonical broker identifier (for example "redis").
            factory: Callable that creates a broker from BrokerContext.

        Raises:
            ConfigurationError: If name is empty or factory is already present.
        """

        normalized_name = name.strip().lower()
        if not normalized_name:
            raise ConfigurationError("Broker name must not be empty.")

        if normalized_name in cls._factories:
            raise ConfigurationError(
                f"Broker '{normalized_name}' is already registered."
            )

        cls._factories[normalized_name] = factory

    @classmethod
    def create(cls, context: BrokerContext) -> Broker:
        """Create a broker instance for the provided context.

        Args:
            context: Normalized broker context containing broker_name.

        Returns:
            A broker instance produced by the registered adapter factory.

        Raises:
            ConfigurationError: If no adapter is registered for the broker.
        """

        cls.discover_plugins()

        broker_name = context.broker_name.strip().lower()
        factory = cls._factories.get(broker_name)
        if factory is None:
            available = ", ".join(sorted(cls._factories)) or "none"
            raise ConfigurationError(
                f"No broker adapter registered for '{broker_name}'. "
                f"Available adapters: {available}."
            )

        return factory(context)

    @classmethod
    def discover_plugins(cls, *, force: bool = False) -> None:
        """Discover adapter factories from setuptools entry points.

        Plugins may expose either a BrokerFactory directly or a callable that
        returns a BrokerFactory.
        """

        if cls._plugins_loaded and not force:
            return

        group_name = "message_broker.adapters"

        try:
            entry_points = metadata.entry_points()
            if hasattr(entry_points, "select"):
                candidates = entry_points.select(group=group_name)
            else:
                candidates = entry_points.get(group_name, [])
        except Exception:
            cls._plugins_loaded = True
            return

        for entry_point in candidates:
            normalized_name = entry_point.name.strip().lower()
            if not normalized_name or normalized_name in cls._factories:
                continue
            try:
                loaded = entry_point.load()
                if not callable(loaded):
                    continue

                def _factory(context: BrokerContext, _loaded=loaded) -> Broker:
                    candidate = _loaded
                    broker = candidate(context)
                    if isinstance(broker, Broker):
                        return broker
                    if callable(broker):
                        maybe_broker = broker(context)
                        if isinstance(maybe_broker, Broker):
                            return maybe_broker
                    raise ConfigurationError(
                        f"Plugin '{entry_point.name}' did not return a Broker instance."
                    )

                cls.register(normalized_name, _factory)
            except ConfigurationError:
                # Preserve existing registration semantics and ignore duplicates.
                continue
            except Exception:
                # Optional plugin loading must never break normal startup paths.
                continue

        cls._plugins_loaded = True

    @classmethod
    def clear(cls) -> None:
        """Remove all registered adapters.

        This is primarily useful for isolated tests that need deterministic
        registry state across test cases.
        """

        cls._factories.clear()
        cls._plugins_loaded = False
