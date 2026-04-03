"""Connection lifecycle primitives for :mod:`lyik_messaging.broker`."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, cast

from faststream.rabbit import RabbitBroker
from faststream.redis import RedisBroker

from ..encryption import build_aesgcm_middleware_factory
from ..exceptions import ConfigurationError, ConnectionLostError
from ..utils import resolve_broker_scheme, set_current_broker

if TYPE_CHECKING:
    from . import MessageBroker
    from .internal_types import FastStreamBroker


class BrokerCore:
    """Owns broker connectivity and runtime lifecycle."""

    def __init__(self, broker_owner: "MessageBroker") -> None:
        self._owner = broker_owner

    async def connect(self) -> None:
        owner = self._owner
        if owner._broker is not None:
            set_current_broker(owner)
            return
        owner._validate_request_reply_queues()

        scheme = resolve_broker_scheme(owner._uri)
        middlewares = self._build_middlewares()
        broker = self._create_broker(scheme, middlewares)
        owner._consumer.attach_registrations(broker)

        try:
            await broker.connect()
        except Exception as exc:
            owner._broker = None
            raise ConnectionLostError("Failed to connect the underlying broker.") from exc

        owner._scheme = scheme
        owner._broker = broker
        set_current_broker(owner)

    async def start(self) -> None:
        owner = self._owner
        broker = self.require_broker()

        await broker.start()
        owner._run_task = asyncio.current_task()

        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise
        finally:
            await self.disconnect()

    async def run(self) -> None:
        await self.connect()
        await self.start()

    async def disconnect(self) -> None:
        owner = self._owner
        broker = owner._broker
        if broker is None:
            set_current_broker(None)
            return

        try:
            await broker.stop()
        except Exception as exc:
            raise ConnectionLostError("Failed to stop the underlying broker.") from exc
        finally:
            owner._broker = None
            owner._scheme = None
            set_current_broker(None)

    def require_broker(self) -> "FastStreamBroker":
        owner = self._owner
        broker = owner._broker
        if broker is None:
            raise ConfigurationError(
                "Broker is not connected. Call connect() first "
                "(or use run(), async with MessageBroker(...), or connect(..., auto_start=True))."
            )
        return broker

    def _build_middlewares(self) -> Sequence[Callable[[object | None], object]]:
        aes_key = self._owner._aes_key
        if aes_key is None:
            return ()
        return (build_aesgcm_middleware_factory(aes_key),)

    def _create_broker(
        self,
        scheme: str,
        middlewares: Sequence[Callable[[object | None], object]],
    ) -> "FastStreamBroker":
        owner = self._owner
        if scheme in {"amqp", "amqps"}:
            return cast(
                "FastStreamBroker",
                RabbitBroker(owner._uri, middlewares=middlewares),
            )
        if scheme in {"redis", "rediss"}:
            return cast(
                "FastStreamBroker",
                RedisBroker(owner._uri, middlewares=middlewares),
            )
        raise ConfigurationError(
            f"Unsupported broker scheme '{scheme}'. Use amqp:// or redis://."
        )
