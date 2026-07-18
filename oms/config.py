"""OMS configuration — dataclasses and YAML loader.

Values in the YAML file may reference environment variables with
``${VAR}`` syntax; these are expanded at load time (after loading ``.env``)
so secrets never have to be written into ``config.yaml``.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from oms.utils.env import load_env


@dataclass
class OMSConfig:
    pull_address: str = "tcp://127.0.0.1:5555"
    pub_address: str = "tcp://127.0.0.1:5556"
    max_queue_size: int = 1000
    order_workers: int = 3
    retry_attempts: int = 2
    retry_delay_ms: int = 500
    order_sync_interval: int = 30
    active_order_sync_interval: float = 2.0
    # Max seconds to wait for an order to reach the exchange open-order list
    # before attempting a broker modify (avoids XTS 400 during PendingNew).
    modify_open_wait_secs: float = 2.0
    timezone: str = "Asia/Kolkata"  # Timestamps in responses and CSV logs


@dataclass
class BrokerConfig:
    type: str = "xts"
    url: str = "http://127.0.0.1:7000"
    app_key: str = ""
    secret_key: str = ""
    source: str = "WEBAPI"
    client_id: str = ""
    verify_ssl: bool = True
    socket_enabled: bool = True
    socket_reconnect: bool = True


@dataclass
class StorageConfig:
    data_dir: str = "./data"
    orders_log_file: str = "orders_log_{date}.csv"
    orders_state_file: str = "orders_state.json"
    trades_file: str = "trades_{date}.csv"
    positions_file: str = "positions.json"
    statistics_file: str = "statistics_{date}.json"


@dataclass
class LogConfig:
    level: str = "INFO"
    log_dir: str = "./logs"
    log_file: str = "oms_{datetime}.log"
    xts_log_file: str = "xts_{datetime}.log"
    rotation_size_mb: int = 50
    backup_count: int = 7


@dataclass
class AppConfig:
    oms: OMSConfig = field(default_factory=OMSConfig)
    broker: BrokerConfig = field(default_factory=BrokerConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    logging: LogConfig = field(default_factory=LogConfig)


_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _expand_env(value: Any) -> Any:
    """Recursively expand ``${VAR}`` references in strings using os.environ."""
    if isinstance(value, str):
        return _ENV_VAR_RE.sub(lambda m: os.environ.get(m.group(1), ""), value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def load_config(config_path: str = "config.yaml") -> AppConfig:
    """Load configuration from a YAML file. Falls back to defaults if not found.

    Environment variables referenced as ``${VAR}`` in the YAML are expanded
    after loading the local ``.env`` file.
    """
    load_env()

    path = Path(config_path)
    if not path.exists():
        return AppConfig()

    with open(path, encoding="utf-8") as f:
        data = _expand_env(yaml.safe_load(f) or {})

    cfg = AppConfig()
    if "oms" in data:
        cfg.oms = OMSConfig(**data["oms"])
    if "broker" in data:
        cfg.broker = BrokerConfig(**data["broker"])
    if "storage" in data:
        cfg.storage = StorageConfig(**data["storage"])
    if "logging" in data:
        cfg.logging = LogConfig(**data["logging"])
    return cfg
