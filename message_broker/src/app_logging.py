import structlog

def get_logger(name: str) -> structlog.typing.FilteringBoundLogger:
    """Create a configured structured logger.

    Parameters:
    - `name`: Logger name shown in output.

    Returns:
    - Configured structlog logger instance.
    """
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.dev.ConsoleRenderer(colors=True),
        ]
    )
    return structlog.get_logger(name)