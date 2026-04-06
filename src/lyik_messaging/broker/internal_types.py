"""Internal broker typing contracts used across broker services."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, TypeAlias, TypeVar

from pydantic import BaseModel

from ..models import MessageInfo, Payload, ResponsePacket

# Only import FastStreamBroker for type checking to avoid circular imports and unnecessary dependencies at runtime.
# This allows us to reference FastStreamBroker in type annotations without importing the actual broker classes at runtime, 
# which may not be needed in all contexts and can help reduce import overhead and potential circular import issues.
if TYPE_CHECKING:
    from faststream.rabbit import RabbitBroker
    from faststream.redis import RedisBroker
    # Define FastStreamBroker as a union of supported broker types for type hinting purposes.
    # This allows us to use FastStreamBroker as a type hint for any of the supported broker implementations without
    # importing them directly at runtime.
    FastStreamBroker = RabbitBroker | RedisBroker

# Define type variables and type aliases for message handler results and handler function signatures.
HandlerResult = TypeVar("HandlerResult")

# Define a type alias for a message handler that can return either a direct result or an awaitable result, 
# allowing for both synchronous and asynchronous handlers.
MaybeAwaitable: TypeAlias = HandlerResult | Awaitable[HandlerResult]

# Define type aliases for different handler function signatures used in the broker, such as strict message handlers 
# and reply handlers.
StrictMessageHandler: TypeAlias = Callable[[BaseModel, MessageInfo], MaybeAwaitable[Payload | None]]

# Define a type alias for a reply handler that processes response packets, allowing for 
# both synchronous and asynchronous handlers.
ReplyHandler: TypeAlias = Callable[[ResponsePacket], MaybeAwaitable[None]]
