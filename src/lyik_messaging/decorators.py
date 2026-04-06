"""Message handler decorators for `lyik_messaging`."""

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
    """
    Create the strict `@broker.on_message(queue)` decorator.
    
    This decorator validates the decorated handler's signature to ensure it matches the expected format:
        ```python
        async def handler(payload: BaseModel, info: MessageInfo):
            ...
        ``` 
    """
    # The returned decorator registers the handler with the broker and returns the original handler unmodified.
    def decorator(user_handler: OnMessageHandler) -> OnMessageHandler:
        # Validate the handler's signature and extract the payload model.
        payload_model = _validate_handler_signature(user_handler)

        # Register the handler with the broker for the specified queue and payload model.
        broker._register_message_handler(
            queue=queue, # This is the queue name that the handler will listen to.
            payload_model=payload_model, # The Pydantic model that the handler expects as its payload.
            handler=user_handler, # The actual handler function provided by the user.
        )

        # Return the original handler unmodified, allowing it to be used as normal in the user's code.
        return user_handler

    return decorator

def _validate_handler_signature(user_handler: OnMessageHandler) -> type[BaseModel]:
    """
    Validate that the user-provided handler has the correct signature and extract the payload model type.
     - The handler must have exactly two parameters.
     - The first parameter must be a subclass of `pydantic.BaseModel` (the payload).
     - The second parameter must be `MessageInfo` (the message metadata).
     - The handler must not use *args or **kwargs.
     - All parameters must be type-annotated.
     - The function must be async.
    """
    # Check if the handler is an async function.
    if not inspect.iscoroutinefunction(user_handler):
        raise ConfigurationError(
            f"{user_handler.__name__} must be an async function."
        )
    # signature stores the parameters and their annotations of the user handler function as an ordered dictionary. We convert it to a tuple of parameters for easier access.
    signature = inspect.signature(user_handler) 
    parameters = tuple(signature.parameters.values())
    # We expect exactly two parameters: the payload and the message info. If there are not exactly two, we raise a configuration error.
    if len(parameters) != 2:
        raise ConfigurationError(
            f"{user_handler.__name__} has invalid signature. "
            "Expected exactly: (payload: BaseModel, info: MessageInfo)."
        )

    first = parameters[0]
    second = parameters[1]

    # We check that the handler does not use *args or **kwargs, as this would make it impossible to validate the parameters. We also check that all parameters are type-annotated, as this is required for validation and documentation purposes.
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
    # We attempt to resolve the type hints for the handler function, including any annotations. If this fails (e.g., due to missing imports), we raise a configuration error with a helpful message.
    try:
        hints = get_type_hints(user_handler, include_extras=True)
    except Exception as exc:
        raise ConfigurationError(
            f"Unable to resolve type hints for {user_handler.__name__}. "
            "Ensure referenced annotations are importable in module scope."
        ) from exc

    # We unwrap the first parameter's annotation to get the actual payload model type. 
    # This handles cases where the annotation might be wrapped in `Annotated` or other generic types. We then check that the payload annotation is a subclass of `pydantic.BaseModel`, as required for validation and parsing of incoming messages.
    payload_annotation = _unwrap_annotation(hints.get(first.name, first.annotation))

    # If the payload annotation is not a type or is not a subclass of `BaseModel`, we raise a configuration error indicating that the first parameter must be a Pydantic model.
    if not isinstance(payload_annotation, type) or not issubclass(payload_annotation, BaseModel):
        raise ConfigurationError(
            f"{user_handler.__name__} first parameter must be a subclass of pydantic.BaseModel, "
            f"got {payload_annotation!r}."
        )

    # We unwrap the second parameter's annotation to get the actual type. 
    # We then check that it is `MessageInfo`, which is required for the handler to receive the message metadata.
    info_annotation = _unwrap_annotation(hints.get(second.name, second.annotation))
    if info_annotation is not MessageInfo:
        raise ConfigurationError(
            f"{user_handler.__name__} second parameter must be MessageInfo, got {info_annotation!r}."
        )

    # If all checks pass, we return the payload annotation, which is the Pydantic model type that the handler expects for the message payload. 
    # This will be used by the broker to validate and parse incoming messages before passing them to the handler.
    return payload_annotation


def _unwrap_annotation(annotation: object) -> object:
    """
    Unwrap an annotation to get the underlying type, handling cases like `Annotated` and other generic wrappers.
    - If the annotation is not a generic type, return it as is.
    - If the annotation is `Annotated[T, ...]`, return `T`.
    - For other generic types, return the origin (e.g., for `List[int]`, return `List`).
    """
    # We use `get_origin` to check if the annotation is a generic type. If it is not (i.e., it is a regular type), we return it as is.
    origin = get_origin(annotation)
    if origin is None:
        return annotation

    # If the annotation is `Annotated[T, ...]`, we use `get_args` to extract the underlying type `T` and return it. 
    # This allows us to support handlers that use `Annotated` for additional metadata without affecting the core type validation.
    if origin is Annotated:
        return get_args(annotation)[0]

    return annotation
