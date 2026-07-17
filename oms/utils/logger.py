"""Structured logging setup using structlog + standard logging."""

from __future__ import annotations

import logging
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

import structlog

# Set once per process when logging is initialised (each OMS start).
_session_datetime: Optional[str] = None


def session_datetime_str(timezone: str = "Asia/Kolkata") -> str:
    """Timestamp string for log filenames, e.g. ``20260520_120729``."""
    try:
        tz = ZoneInfo(timezone)
    except Exception:
        tz = ZoneInfo("UTC")
    return datetime.now(tz).strftime("%Y%m%d_%H%M%S")


def resolve_log_filename(template: str, dt: Optional[str] = None) -> str:
    """
    Expand ``{datetime}`` / ``{date}`` in a log filename template.

    Examples:
      ``oms_{datetime}.log`` → ``oms_20260520_120729.log``
      ``xts_{date}.log``     → ``xts_20260520.log``
    """
    stamp = dt or session_datetime_str()
    return (
        template.replace("{datetime}", stamp).replace("{date}", stamp[:8])
    )


def setup_logging(
    level: str = "INFO",
    log_dir: str = "./logs",
    log_file: str = "oms_{datetime}.log",
    rotation_size_mb: int = 50,
    backup_count: int = 7,
    timezone: str = "Asia/Kolkata",
) -> Tuple[Path, str]:
    """
    Configure structlog with console + per-session file output.

    Each OMS start creates a new log file when ``log_file`` contains
    ``{datetime}`` (default).

    Returns
    -------
    (log_path, session_datetime) — absolute path and stamp used in the name.
    """
    global _session_datetime
    _session_datetime = session_datetime_str(timezone)
    resolved_name = resolve_log_filename(log_file, _session_datetime)

    log_level = getattr(logging, level.upper(), logging.INFO)

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path = Path(log_dir) / resolved_name

    # ---------- stdlib handlers ----------
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # One file per OMS session (no rotation within a single run).
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)

    fmt = "%(asctime)s | %(levelname)-8s | %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt=date_fmt)
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    root.addHandler(console_handler)
    root.addHandler(file_handler)

    # Silence noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # ---------- structlog ----------
    shared_processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.contextvars.merge_contextvars,
    ]

    structlog.configure(
        processors=shared_processors
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Attach a ProcessorFormatter to the root handler so structlog
    # records get rendered with key=value pairs.
    proc_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(colors=False),
    )
    for handler in root.handlers:
        handler.setFormatter(proc_formatter)

    return log_path, _session_datetime


def get_logger(name: Optional[str] = None) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger.  Use like: log = get_logger(__name__)"""
    return structlog.get_logger(name)


# Cache so we only add the file handler once per strategy per process
_strategy_loggers: dict = {}


def get_strategy_logger(strategy_id: str, log_dir: str = "./logs") -> logging.Logger:
    """
    Return a stdlib Logger that writes exclusively to
    ``{log_dir}/strategy_{strategy_id}.log``.

    The logger is cached so repeated calls with the same strategy_id
    always return the same instance without duplicating handlers.
    """
    if strategy_id in _strategy_loggers:
        return _strategy_loggers[strategy_id]

    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(f"strategy.{strategy_id}")
    logger.setLevel(logging.DEBUG)
    # Prevent records from bubbling up to the root logger (which writes oms.log)
    logger.propagate = False

    log_path = Path(log_dir) / f"strategy_{strategy_id}.log"
    handler = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes=20 * 1024 * 1024,   # 20 MB per strategy file
        backupCount=5,
        encoding="utf-8",
    )
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    _strategy_loggers[strategy_id] = logger
    return logger


_xts_logger: Optional[logging.Logger] = None


def get_xts_logger(
    log_dir: str = "./logs",
    log_file: str = "xts_{datetime}.log",
    session_datetime: Optional[str] = None,
) -> Tuple[logging.Logger, Path]:
    """
    Dedicated logger for all XTS REST and Socket.IO traffic.

    Uses the same ``{datetime}`` stamp as :func:`setup_logging` when
    ``session_datetime`` is omitted.

    Returns
    -------
    (logger, log_path)
    """
    global _xts_logger
    if _xts_logger is not None:
        # Already initialised this process
        stamp = session_datetime or _session_datetime or session_datetime_str()
        path = Path(log_dir) / resolve_log_filename(log_file, stamp)
        return _xts_logger, path

    stamp = session_datetime or _session_datetime or session_datetime_str()
    resolved_name = resolve_log_filename(log_file, stamp)

    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("xts")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    log_path = Path(log_dir) / resolved_name
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    _xts_logger = logger
    return logger, log_path
