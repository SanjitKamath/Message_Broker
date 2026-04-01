"""
Application logging configuration and utilities.
This module sets up structlog for structured logging and provides a helper function to retrieve configured loggers.
"""
import structlog

# Configure structlog once at module import time. Calling `configure`
# repeatedly (for example, on every `get_logger` call) can lead to
# duplicated processors and unexpected logger output. Keep configuration
# idempotent by setting it at import.
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="ISO"),
        structlog.dev.ConsoleRenderer(colors=True),
    ]
)


def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    """Return a configured structlog logger for `name`.

    This function is intentionally lightweight — configuration happens at
    import time to avoid side-effects when called multiple times.
    """
    return structlog.get_logger(name)