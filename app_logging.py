import structlog

def get_logger(name: str):
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.dev.ConsoleRenderer(colors=True),
        ]
    )
    return structlog.get_logger(name)