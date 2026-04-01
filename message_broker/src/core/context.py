"""
Connection context for broker adapters.

This module centralizes URI parsing and option normalization so every adapter
inherits consistent behavior for defaults, security inference, and validation.

Helps users by providing a single source of truth for connection settings, preventing
copy-pasted parsing logic across adapters, and keeping adapter code focused on transport 
operations instead of configuration plumbing.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import parse_qs, unquote, urlparse

from .exceptions import ConfigurationError
from .interfaces import JsonValue

if TYPE_CHECKING:
    from .interfaces import Middleware, Serializer


class BrokerContext:
    """Stores normalized connection settings for a broker session.

    The context prevents each adapter from re-implementing URI parsing logic.
    A single source of truth improves consistency and keeps adapter code focused
    on transport operations instead of configuration plumbing.

    Args:
        connection_uri: Broker URI, for example redis://localhost:6379.
        **kwargs: User overrides merged with query params and package defaults.

    Attributes:
        connection_uri: Raw URI provided by the caller.
        scheme: Lowercased URI scheme, for example amqps.
        broker_name: Normalized broker key such as rabbitmq or redis.
        host: Parsed hostname.
        port: Effective port after URI/default resolution.
        secure: True when transport security is required.
        username: Optional decoded URI username.
        password: Optional decoded URI password.
        virtual_host: URI path with leading slash removed when present.
        options: Immutable merged option mapping.
        timeout: Connection and operation timeout in milliseconds.

    Example:
    ```
        context = BrokerContext("redis://localhost:6379/0", timeout=3000)
        assert context.broker_name == "redis"
    ```
    """

    DEFAULT_OPTIONS: ClassVar[dict[str, JsonValue]] = {
        "timeout": 5000,
        "max_retries": 3,
        "concurrency": 10,
        "max_queue_size": 100,
        "scheduler_lock_ttl_ms": 5000,
        "scheduler_batch_size": 100,
        "idempotency_ttl_sec": 86400,
        "processing_timeout_ms": None,
        "handler_max_retries": 0,
        "shutdown_drain_timeout_ms": 1500,
        "default_dlq_topic": None,
        "dlq_topics": {},
        "observers": [],
    }

    DEFAULT_PORTS: ClassVar[dict[str, int]] = {
        "redis": 6379,
        "rediss": 6380,
        "rabbitmq": 5672,
        "amqp": 5672,
        "amqps": 5671,
        "memory": 0,
    }

    connection_uri: str
    scheme: str
    broker_name: str
    host: str
    port: int
    secure: bool
    username: str | None
    password: str | None
    virtual_host: str | None
    options: Mapping[str, JsonValue]
    serializer: "Serializer"
    middlewares: list["Middleware"]

    def __init__(
        self,
        connection_uri: str,
        serializer: "Serializer | None" = None,
        middlewares: "list[Middleware] | None" = None,
        **kwargs: Any,
    ) -> None:
        if not connection_uri.strip():
            raise ConfigurationError("Connection URI must not be empty.")

        parsed = urlparse(connection_uri)
        if not parsed.scheme:
            raise ConfigurationError(
                "Connection URI must include a scheme (for example redis://)."
            )

        # Normalize scheme and broker name, infer security requirement, parse host/port/auth/vhost, 
        # and merge options from defaults, URI query, env vars, and kwargs.
        scheme = parsed.scheme.lower()
        broker_name = self._normalize_broker_name(scheme)
        secure = self._is_secure_scheme(scheme)
        host = parsed.hostname or "localhost"
        port = parsed.port if parsed.port is not None else self._resolve_port(scheme, broker_name)
        username = unquote(parsed.username) if parsed.username is not None else None
        password = unquote(parsed.password) if parsed.password is not None else None
        virtual_host = parsed.path.lstrip("/") or None

        # Validate critical options early for deterministic failures. This prevents the broker from 
        # starting with invalid configuration, which could lead to more complex errors later on when operations are attempted.
        query_options = self._parse_query_options(parsed.query)
        config_options = self._extract_config_options(kwargs)
        env_options = self._load_env_overrides(broker_name)
        
        # Separate broker-namespaced kwargs from flat global options
        namespaced_keys = {"redis", "rabbitmq", "memory"}
        broker_namespaced_kwargs = {k: v for k, v in kwargs.items() if k in namespaced_keys}
        flat_kwargs = {k: v for k, v in kwargs.items() if k not in namespaced_keys}
        
        merged_options = {
            **self.DEFAULT_OPTIONS,
            **config_options,
            **query_options,
            **env_options,
            **flat_kwargs,
            **broker_namespaced_kwargs,  # Preserve nested dicts for broker-specific config
        }
        self._validate_options(merged_options)

        self.connection_uri = connection_uri
        self.scheme = scheme
        self.broker_name = broker_name
        self.host = host
        self.port = port
        self.secure = secure
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.options = MappingProxyType(dict(merged_options))
        
        # Initialize serializer (default to JsonSerializer if not provided)
        if serializer is None:
            from .serializers import JsonSerializer
            self.serializer = JsonSerializer()
        else:
            self.serializer = serializer
        
        # Initialize middlewares (empty list if not provided)
        self.middlewares = middlewares if middlewares is not None else []

    """
    This class method is responsible for normalizing the broker name based on the URI scheme. It maps common scheme variants to a 
    canonical broker identifier, which allows the rest of the code to refer to brokers in a consistent way regardless of the specific 
    URI format used by the caller.
    """
    @staticmethod
    def _normalize_broker_name(scheme: str) -> str:
        """Map URI scheme variants to a canonical broker identifier."""

        normalized = scheme.split("+", maxsplit=1)[0]
        if normalized in {"amqp", "amqps"}:
            return "rabbitmq"
        if normalized in {"redis", "rediss"}:
            return "redis"
        return normalized

    """
    This static method infers whether transport security is required based on the URI scheme. It checks for common indicators of secure schemes, such as
    schemes that end with "s" (like amqps), or contain "+ssl" or "+tls". This allows the context to automatically determine if the connection should be secure without
    requiring the caller to explicitly specify it.
    """
    @staticmethod
    def _is_secure_scheme(scheme: str) -> bool:
        """Infer transport security requirement from URI scheme."""

        lowered = scheme.lower()
        return (
            lowered.endswith("s")
            or "+ssl" in lowered
            or "+tls" in lowered
            or lowered in {"https", "wss"}
        )

    """
    This class method resolves the effective port number for the connection. 
    It first checks if the URI includes an explicit port. If not, it looks up a default port
    based on the scheme or broker name.
    """
    @classmethod
    def _resolve_port(cls, scheme: str, broker_name: str) -> int:
        """Return a deterministic default port when URI port is missing."""

        if scheme in cls.DEFAULT_PORTS:
            return cls.DEFAULT_PORTS[scheme]
        if broker_name in cls.DEFAULT_PORTS:
            return cls.DEFAULT_PORTS[broker_name]
        return 0

    """
    This method parses the query string from the URI and converts it into a dictionary of options. It uses the parse_qs 
    function to handle URL-encoded query parameters and then applies a coercion function to convert string values into 
    appropriate Python types (like int, bool, etc.). This allows users to specify configuration options directly in the 
    URI query string in a flexible way.
    """
    @classmethod
    def _parse_query_options(cls, query: str) -> dict[str, JsonValue]:
        """Decode URI query values into scalar Python objects.

        Query values override package defaults because they are part of the URI,
        while explicit keyword arguments override both for call-site control.
        """

        parsed = parse_qs(query, keep_blank_values=False)
        options: dict[str, JsonValue] = {}
        for key, values in parsed.items():
            if not values:
                continue
            options[key] = cls._coerce_scalar(values[-1])
        return options

    """
    This static method takes a raw string value and attempts to coerce it into a more specific Python type. 
    It handles common cases like "true"/"false" for booleans, and tries to convert numeric strings into int or float. 
    If it cannot coerce the value into a more specific type, it returns the original string. This is useful for
    interpreting configuration options that are provided as strings (for example, from environment variables or 
    query parameters) into their intended types.
    """
    @staticmethod
    def _coerce_scalar(raw_value: str) -> JsonValue:
        """
        Convert common scalar string values to typed Python values.

        For example:
            - "true" -> True
            - "false" -> False  
            - "42" -> 42
            - "3.14" -> 3.14
            - "hello" -> "hello"      
        """

        value = raw_value.strip()
        lowered = value.lower()

        if lowered in {"true", "false"}:
            return lowered == "true"

        try:
            return int(value)
        except ValueError:
            pass

        try:
            return float(value)
        except ValueError:
            return value

    """
    This static method validates critical option values to ensure they meet expected criteria (like being positive integers). 
    It raises a ConfigurationError if any of the options are invalid. This early validation helps catch misconfigurations 
    before the broker starts processing messages, leading to more deterministic failures and easier debugging.
    """
    @staticmethod
    def _validate_options(options: Mapping[str, JsonValue]) -> None:
        """
        Validate critical option values early for deterministic failures.
        For example:
            - timeout must be a positive integer
            - max_retries must be a non-negative integer
            - processing_timeout_ms must be a positive integer
            - handler_max_retries must be a non-negative integer
            - scheduler_lock_ttl_ms must be a positive integer
            - scheduler_batch_size must be a positive integer
            - idempotency_ttl_sec must be a positive integer

        Note: Namespaced options (redis, rabbitmq, etc.) are not validated here
        because they may contain adapter-specific arbitrary config.
        """

        timeout = options.get("timeout")
        if isinstance(timeout, int) and timeout <= 0:
            raise ConfigurationError("Option 'timeout' must be a positive integer.")

        max_retries = options.get("max_retries")
        if isinstance(max_retries, int) and max_retries < 0:
            raise ConfigurationError(
                "Option 'max_retries' must be an integer greater than or equal to zero."
            )

        processing_timeout_ms = options.get("processing_timeout_ms")
        if processing_timeout_ms is not None:
            if not isinstance(processing_timeout_ms, int) or processing_timeout_ms <= 0:
                raise ConfigurationError(
                    "Option 'processing_timeout_ms' must be a positive integer when provided."
                )

        handler_max_retries = options.get("handler_max_retries")
        if isinstance(handler_max_retries, int) and handler_max_retries < 0:
            raise ConfigurationError(
                "Option 'handler_max_retries' must be greater than or equal to zero."
            )

        scheduler_lock_ttl_ms = options.get("scheduler_lock_ttl_ms")
        if not isinstance(scheduler_lock_ttl_ms, int) or scheduler_lock_ttl_ms <= 0:
            raise ConfigurationError(
                "Option 'scheduler_lock_ttl_ms' must be a positive integer."
            )

        scheduler_batch_size = options.get("scheduler_batch_size")
        if not isinstance(scheduler_batch_size, int) or scheduler_batch_size <= 0:
            raise ConfigurationError(
                "Option 'scheduler_batch_size' must be a positive integer."
            )

        idempotency_ttl_sec = options.get("idempotency_ttl_sec")
        if not isinstance(idempotency_ttl_sec, int) or idempotency_ttl_sec <= 0:
            raise ConfigurationError(
                "Option 'idempotency_ttl_sec' must be a positive integer."
            )

    """
    This method is responsible for extracting a configuration object from the keyword arguments passed to the BrokerContext 
    constructor. It looks for a "config" key in the kwargs, and if it finds one, it attempts to normalize it into a plain 
    dictionary. This allows users to pass in configuration using various formats (like Pydantic models or other mapping-like 
    objects) while ensuring that the BrokerContext can work with it in a consistent way.
    """
    @classmethod
    def _extract_config_options(cls, kwargs: dict[str, Any]) -> dict[str, JsonValue]:
        """Pop optional config object and normalize it to a plain dict."""

        config = kwargs.pop("config", None)
        if config is None:
            return {}

        if isinstance(config, Mapping):
            return dict(config)

        if hasattr(config, "model_dump"):
            dumped = config.model_dump()
            if isinstance(dumped, Mapping):
                return dict(dumped)

        if hasattr(config, "dict"):
            dumped = config.dict()
            if isinstance(dumped, Mapping):
                return dict(dumped)

        raise ConfigurationError("Option 'config' must be a mapping-like object.")

    """ 
    This class method loads optional overrides from environment variables. It supports both global and broker-specific forms, 
    allowing users to specify configuration options in the environment that can override defaults and URI query parameters. 
    The method checks for environment variables in the format of "MB_{OPTION}" for global overrides, and "MB_{BROKER}_{OPTION}" 
    for broker-specific overrides, where {OPTION} is the uppercase name of the option and {BROKER} is the uppercase name of the 
    broker. It then coerces the raw string values from the environment into appropriate Python types using the _coerce_scalar 
    method
    """
    @classmethod
    def _load_env_overrides(cls, broker_name: str) -> dict[str, JsonValue]:
        """
        Load optional overrides from environment variables.

        Supports both global and broker-specific forms:
        - MB_TIMEOUT=5000
        - MB_REDIS_TIMEOUT=5000
        """

        overrides: dict[str, JsonValue] = {}
        for option in cls.DEFAULT_OPTIONS:
            upper_key = option.upper()
            global_name = f"MB_{upper_key}"
            broker_name_key = f"MB_{broker_name.upper()}_{upper_key}"

            raw = os.getenv(broker_name_key)
            if raw is None:
                raw = os.getenv(global_name)
            if raw is None:
                continue

            # Keep simple scalar coercion behavior aligned with query parsing.
            overrides[option] = cls._coerce_scalar(raw)

        return overrides
