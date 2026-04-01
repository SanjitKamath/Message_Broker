"""
Adapter registry for dynamic broker resolution.

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

"""
This class serves as a central registry for broker adapter factory functions. Adapters can self-register by name, 
allowing the core package to create broker instances dynamically at runtime based on configuration. The registry 
also includes a plugin discovery mechanism that looks for adapter factories exposed via setuptools entry points, 
enabling a flexible and extensible architecture for supporting multiple broker implementations without coupling 
the core to specific transports.
"""
class BrokerRegistry:
    """
    Stores and resolves broker adapter factory functions.

    Adapters self-register by name, allowing runtime selection from
    configuration without adding import-time coupling in the core layer.
    """

    # _factories maps normalized broker names to their corresponding factory functions 
    # _plugins_loaded tracks whether the plugin discovery process has been completed to avoid redundant loading.
    _factories: ClassVar[dict[str, BrokerFactory]] = {}
    _plugins_loaded: ClassVar[bool] = False

    """
    Register a broker factory under a normalized name.
    """
    @classmethod
    def register(cls, name: str, factory: BrokerFactory) -> None:
        """Register a broker factory under a normalized name.

        Args:
            name: Canonical broker identifier (for example "redis").
            factory: Callable that creates a broker from BrokerContext.

        Raises:
            ConfigurationError: If name is empty or factory is already present.
        
        For example:
            If you have a Redis broker implementation, you might register it like this:
            def redis_factory(context: BrokerContext) -> Broker:
                return RedisBroker(context)
            BrokerRegistry.register("redis", redis_factory)
            
            In this example, the `redis_factory` function takes a `BrokerContext` and returns 
            an instance of `RedisBroker`, which implements the `Broker` interface.
        """

        normalized_name = name.strip().lower()
        if not normalized_name:
            raise ConfigurationError("Broker name must not be empty.")

        if normalized_name in cls._factories:
            raise ConfigurationError(
                f"Broker '{normalized_name}' is already registered."
            )

        cls._factories[normalized_name] = factory

    """
    Create a broker instance for the provided context. This method first ensures that plugins have been discovered, then 
    looks up the appropriate factory based on the broker name in the context. If a factory is found, it is called with 
    the context to create and return a broker instance. If no factory is registered for the broker name, a ConfigurationError 
    is raised with a message that includes the list of available adapters for easier debugging.
    """
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

        cls.discover_plugins() # Ensure plugins are loaded before attempting to create a broker.

        broker_name = context.broker_name.strip().lower()
        factory = cls._factories.get(broker_name)
        if factory is None:
            available = ", ".join(sorted(cls._factories)) or "none"
            raise ConfigurationError(
                f"No broker adapter registered for '{broker_name}'. "
                f"Available adapters: {available}."
            )

        return factory(context)

    """
    This function discovers adapter factories from setuptools entry points. It looks for entry points in the 
    "message_broker.adapters" group and attempts to load them. Each entry point is expected to be a callable 
    that returns a BrokerFactory or a Broker instance directly. The method normalizes the entry point names and registers 
    valid factories in the registry. It also includes error handling to ensure that optional plugin loading does not break
    normal startup paths, and it avoids duplicate registrations.

    For example, if a plugin package defines an entry point like this in its setup.py:
    entry_points={
        "message_broker.adapters": [
            "rabbitmq = my_plugin.rabbitmq:factory_function",
        ],
    }
    The `discover_plugins` method will load the `factory_function` from `my_plugin.rabbitmq`, which should return a
    BrokerFactory or a Broker instance when called. This allows users to extend the broker functionality by simply installing
    additional packages that expose the appropriate entry points, without needing to modify the core codebase.
    """
    @classmethod
    def discover_plugins(cls, *, force: bool = False) -> None:
        """
        Discover adapter factories from setuptools entry points.

        Plugins may expose either a BrokerFactory directly or a callable that
        returns a BrokerFactory. For example, an entry point could point to a function that creates and returns a 
        BrokerFactory, or it could point directly to a BrokerFactory function. The method normalizes the entry point 
        names and registers valid factories in the registry. It also includes error handling to ensure that optional 
        plugin loading does not break normal startup paths, and it avoids duplicate registrations.
        """
        # If plugins have already been loaded and force is not True, skip discovery to avoid redundant loading.
        if cls._plugins_loaded and not force:
            return

        # Group name for entry points that plugins should use to register their broker factories.
        group_name = "message_broker.adapters"

        # Attempt to retrieve entry points for the specified group. If this fails (e.g., due to an older 
        # version of importlib.metadata), mark plugins as loaded and return without raising an exception.
        try:
            entry_points = metadata.entry_points()
            if hasattr(entry_points, "select"):
                candidates = entry_points.select(group=group_name)
            else:
                candidates = entry_points.get(group_name, [])
        except Exception:
            cls._plugins_loaded = True
            return
        
        # Iterate over the discovered entry points and attempt to load them. For each entry point, normalize its name 
        # and check if it is valid for registration.
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
        
        # Mark plugins as loaded to prevent redundant discovery in future calls.
        cls._plugins_loaded = True

    """
    Clear all registered adapters. This is primarily useful for isolated tests that need deterministic registry 
    state across test cases. By calling this method, you can ensure that the registry is reset to an empty state
    before each test, allowing for consistent and predictable behavior when testing broker registration and 
    creation logic.
    """
    @classmethod
    def clear(cls) -> None:
        """Remove all registered adapters.

        This is primarily useful for isolated tests that need deterministic
        registry state across test cases.
        """

        cls._factories.clear()
        cls._plugins_loaded = False
