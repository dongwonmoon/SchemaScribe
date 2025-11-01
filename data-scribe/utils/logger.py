import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a logger with a specified name.

    The logger is configured to output messages to the console (stdout).
    The log format includes the timestamp, log level, and the message.

    Args:
        name: The name for the logger, typically __name__.

    Returns:
        A configured logger instance.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
