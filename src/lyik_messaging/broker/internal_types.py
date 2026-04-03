"""Internal broker typing contracts used across broker services."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, TypeAlias, TypeVar

from pydantic import BaseModel

from ..models import MessageInfo, Payload, ResponsePacket

if TYPE_CHECKING:
    from faststream.rabbit import RabbitBroker
    from faststream.redis import RedisBroker

    FastStreamBroker = RabbitBroker | RedisBroker


HandlerResult = TypeVar("HandlerResult")
MaybeAwaitable: TypeAlias = HandlerResult | Awaitable[HandlerResult]
StrictMessageHandler: TypeAlias = Callable[[BaseModel, MessageInfo], MaybeAwaitable[Payload | None]]
ReplyHandler: TypeAlias = Callable[[ResponsePacket], MaybeAwaitable[None]]
