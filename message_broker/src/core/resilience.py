"""Resilience helpers for broker operations.

Centralized retry behavior avoids copy-pasted error handling across adapters and
ensures failures surface through package-defined exceptions.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import ParamSpec, TypeVar
from inspect import isawaitable

from .exceptions import ConnectionLostError, PublishFailedError

P = ParamSpec("P")
R = TypeVar("R")


def with_retries(
    *,
    max_retries: int,
    base_delay: float,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """Retry a broker operation using exponential backoff.

    The decorator catches transient connection failures and retries execution
    with delays of base_delay, base_delay * 2, base_delay * 4, and so on.
    Once retries are exhausted, it raises a PublishFailedError so callers do
    not need to handle transport-specific failure semantics.

    Args:
        max_retries: Number of retry attempts after the initial failure.
        base_delay: Initial delay in seconds before the first retry.

    Returns:
        A decorator that applies retry behavior to the wrapped callable.

    Raises:
        ValueError: If retry configuration is invalid.
    """

    if max_retries < 0:
        raise ValueError("max_retries must be greater than or equal to zero.")
    if base_delay <= 0:
        raise ValueError("base_delay must be greater than zero.")

    def decorator(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        """Decorate a callable with retry/backoff behavior."""

        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            """Execute wrapped operation with retry handling."""

            attempts = 0
            while True:
                try:
                    result = await func(*args, **kwargs)
                    return result
                except ConnectionLostError as exc:
                    if attempts >= max_retries:
                        raise PublishFailedError(
                            "Publish operation failed after retry exhaustion.",
                            operation=func.__name__,
                            details={
                                "max_retries": max_retries,
                                "base_delay": base_delay,
                            },
                        ) from exc

                    # Optional observability callback: args[0] is typically `self`.
                    owner = args[0] if args else None
                    context = getattr(owner, "_context", None)
                    options = getattr(context, "options", {}) if context is not None else {}
                    observers = options.get("observers", []) if isinstance(options, dict) or hasattr(options, "get") else []
                    for observer in list(observers or []):
                        callback = getattr(observer, "on_retry_attempt", None)
                        if callback is None:
                            continue
                        result = callback(
                            operation=func.__name__,
                            attempt=attempts + 1,
                            max_retries=max_retries,
                            error=exc,
                        )
                        if isawaitable(result):
                            await result

                    delay = base_delay * (2**attempts)
                    await asyncio.sleep(delay)
                    attempts += 1

        return wrapper

    return decorator
