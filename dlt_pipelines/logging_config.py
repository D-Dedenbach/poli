import logging
import sys
from typing import Optional

logger = logging.getLogger("dlt_pipelines")



def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    format_str: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
) -> logging.Logger:
    """
    Configure logging for dlt_pipelines.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to log file
        format_str: Log message format

    Returns:
        Configured logger instance
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Create formatter
    formatter = logging.Formatter(format_str)

    # Configure root logger
    logger.setLevel(log_level)

    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Prevent duplicate logs from dlt
    logging.getLogger("dlt").setLevel(log_level)