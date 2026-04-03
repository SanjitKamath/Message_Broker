"""Message handler decorators for :mod:`lyik_messaging`."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Annotated, get_args, get_origin, get_type_hints

from pydantic import BaseModel

from .exceptions import ConfigurationError
from .models import MessageInfo

if TYPE_CHECKING:
    from .broker import MessageBroker

HandlerResult = object | None
OnMessageHandler = Callable[..., HandlerResult | Awaitable[HandlerResult]]


def build_on_message_decorator(
    broker: "MessageBroker",
    queue: str,
) -> Callable[[OnMessageHandler], OnMessageHandler]:
    """Create the strict `@broker.on_message(queue)` decorator."""

    def decorator(user_handler: OnMessageHandler) -> OnMessageHandler:
        payload_model = _validate_handler_signature(user_handler)
        broker._register_message_handler(
            queue=queue,
            payload_model=payload_model,
            handler=user_handler,
        )
        return user_handler

    return decorator

def _validate_handler_signature(user_handler: OnMessageHandler) -> type[BaseModel]:
    signature = inspect.signature(user_handler)
    parameters = tuple(signature.parameters.values())
    if len(parameters) != 2:
        raise ConfigurationError(
            f"{user_handler.__name__} has invalid signature. "
            "Expected exactly: (payload: BaseModel, info: MessageInfo)."
        )

    first = parameters[0]
    second = parameters[1]
    for parameter in parameters:
        if parameter.kind in {inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD}:
            raise ConfigurationError(
                f"{user_handler.__name__} may not use *args or **kwargs. "
                "Use explicit parameters: payload, info."
            )
        if parameter.annotation is inspect._empty:
            raise ConfigurationError(
                f"{user_handler.__name__} must type-annotate every parameter. "
                "Example: async def handler(payload: MyModel, info: MessageInfo)."
            )

    try:
        hints = get_type_hints(user_handler, include_extras=True)
    except Exception as exc:
        raise ConfigurationError(
            f"Unable to resolve type hints for {user_handler.__name__}. "
            "Ensure referenced annotations are importable in module scope."
        ) from exc

    payload_annotation = _unwrap_annotation(hints.get(first.name, first.annotation))

    if not isinstance(payload_annotation, type) or not issubclass(payload_annotation, BaseModel):
        raise ConfigurationError(
            f"{user_handler.__name__} first parameter must be a subclass of pydantic.BaseModel, "
            f"got {payload_annotation!r}."
        )

    info_annotation = _unwrap_annotation(hints.get(second.name, second.annotation))
    if info_annotation is not MessageInfo:
        raise ConfigurationError(
            f"{user_handler.__name__} second parameter must be MessageInfo, got {info_annotation!r}."
        )

    return payload_annotation


def _unwrap_annotation(annotation: object) -> object:
    origin = get_origin(annotation)
    if origin is None:
        return annotation

    if origin is Annotated:
        return get_args(annotation)[0]

    return annotation
