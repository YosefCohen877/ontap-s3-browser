"""
utils/logging.py — Structured JSON logging using structlog.
Configures once at startup; all modules import `get_logger`.
"""
from __future__ import annotations

import logging
import sys
from typing import Any

import structlog


def _add_logger_name(logger: Any, method: str, event_dict: dict) -> dict:
    """
    Safe replacement for structlog.stdlib.add_logger_name.
    PrintLogger has no .name attribute, so we read it from the bound
    context key '_logger_name' that get_logger() injects.
    """
    event_dict.setdefault("logger", event_dict.pop("_logger_name", "app"))
    return event_dict


def configure_logging(log_level: str = "INFO") -> None:
    """Call once from app startup."""
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Route stdlib logging through structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            _add_logger_name,                         # safe for PrintLogger
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = __name__) -> structlog.BoundLogger:
    """Return a bound logger that carries the module name through the pipeline."""
    return structlog.get_logger().bind(_logger_name=name)
