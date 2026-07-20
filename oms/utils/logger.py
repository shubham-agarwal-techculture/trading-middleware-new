"""Structured logging setup using structlog + standard logging."""

from __future__ import annotations

import logging
import logging.handlers
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional, Tuple
from zoneinfo import ZoneInfo

import structlog

# Set once per process when logging is initialised (each OMS start).
_session_datetime: Optional[str] = None
_log_timezone: str = "Asia/Kolkata"


def _zoneinfo(timezone: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone)
    except Exception:
        return ZoneInfo("UTC")


def session_datetime_str(timezone: str = "Asia/Kolkata") -> str:
    """Timestamp string for log filenames, e.g. ``20260520_120729``."""
    return datetime.now(_zoneinfo(timezone)).strftime("%Y%m%d_%H%M%S")


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


def _make_timestamper(timezone: str) -> Callable[..., dict]:
    """structlog processor that stamps events in the configured timezone (IST by default)."""

    tz = _zoneinfo(timezone)

    def timestamper(
        logger: Any, method_name: str, event_dict: dict
    ) -> dict:
        event_dict["timestamp"] = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        return event_dict

    return timestamper


def _formatter_converter(timezone: str) -> Callable[[Optional[float]], time.struct_time]:
    """``logging.Formatter.converter`` that renders asctime in *timezone*."""

    tz = _zoneinfo(timezone)

    def converter(secs: Optional[float] = None) -> time.struct_time:
        if secs is None:
            secs = time.time()
        return datetime.fromtimestamp(secs, tz).timetuple()

    return converter


def _stdlib_formatter(timezone: str) -> logging.Formatter:
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    formatter.converter = _formatter_converter(timezone)  # type: ignore[assignment]
    return formatter


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
    ``{datetime}`` (default). Log line timestamps use *timezone*
    (default ``Asia/Kolkata`` / IST).

    Returns
    -------
    (log_path, session_datetime) — absolute path and stamp used in the name.
    """
    global _session_datetime, _log_timezone
    _log_timezone = timezone or "Asia/Kolkata"
    _session_datetime = session_datetime_str(_log_timezone)
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

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    root.addHandler(console_handler)
    root.addHandler(file_handler)

    # Silence noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    # ---------- structlog (IST timestamps) ----------
    timestamper = _make_timestamper(_log_timezone)
    shared_processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
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
    # records get rendered with key=value pairs. Foreign (stdlib) records
    # also get an IST timestamp via foreign_pre_chain.
    proc_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(colors=False),
        foreign_pre_chain=[
            timestamper,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
        ],
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
    handler.setFormatter(_stdlib_formatter(_log_timezone))
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
    ``session_datetime`` is omitted. Timestamps use the timezone from
    :func:`setup_logging` (default IST).

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
    handler.setFormatter(_stdlib_formatter(_log_timezone))
    logger.addHandler(handler)

    _xts_logger = logger
    return logger, log_path
