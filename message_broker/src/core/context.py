"""Connection context for broker adapters.

This module centralizes URI parsing and option normalization so every adapter
inherits consistent behavior for defaults, security inference, and validation.
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

    Example:
        context = BrokerContext("redis://localhost:6379/0", timeout=3000)
        assert context.broker_name == "redis"
    """

    DEFAULT_OPTIONS: ClassVar[dict[str, JsonValue]] = {
        "timeout": 5000,
        "max_retries": 3,
        "concurrency": 10,
        "max_queue_size": 100,
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
        "kafka": 9092,
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

        scheme = parsed.scheme.lower()
        broker_name = self._normalize_broker_name(scheme)
        secure = self._is_secure_scheme(scheme)
        host = parsed.hostname or "localhost"
        port = parsed.port if parsed.port is not None else self._resolve_port(scheme, broker_name)
        username = unquote(parsed.username) if parsed.username is not None else None
        password = unquote(parsed.password) if parsed.password is not None else None
        virtual_host = parsed.path.lstrip("/") or None

        query_options = self._parse_query_options(parsed.query)
        config_options = self._extract_config_options(kwargs)
        env_options = self._load_env_overrides(broker_name)
        
        # Separate broker-namespaced kwargs from flat global options
        namespaced_keys = {"redis", "kafka", "rabbitmq", "memory"}
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

    @staticmethod
    def _normalize_broker_name(scheme: str) -> str:
        """Map URI scheme variants to a canonical broker identifier."""

        normalized = scheme.split("+", maxsplit=1)[0]
        if normalized in {"amqp", "amqps"}:
            return "rabbitmq"
        if normalized in {"redis", "rediss"}:
            return "redis"
        return normalized

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

    @classmethod
    def _resolve_port(cls, scheme: str, broker_name: str) -> int:
        """Return a deterministic default port when URI port is missing."""

        if scheme in cls.DEFAULT_PORTS:
            return cls.DEFAULT_PORTS[scheme]
        if broker_name in cls.DEFAULT_PORTS:
            return cls.DEFAULT_PORTS[broker_name]
        return 0

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

    @staticmethod
    def _coerce_scalar(raw_value: str) -> JsonValue:
        """Convert common scalar string values to typed Python values."""

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

    @staticmethod
    def _validate_options(options: Mapping[str, JsonValue]) -> None:
        """Validate critical option values early for deterministic failures.
        
        Note: Namespaced options (redis, kafka, etc.) are not validated here
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

    @classmethod
    def _load_env_overrides(cls, broker_name: str) -> dict[str, JsonValue]:
        """Load optional overrides from environment variables.

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
