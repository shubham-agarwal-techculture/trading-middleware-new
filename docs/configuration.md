# Configuration

## Secrets (`.env`)

Copy `.env.example` → `.env` (gitignored). Single-quote values that contain `$` or `#`.

| Variable | Used by |
|----------|---------|
| `XTS_APP_KEY` / `XTS_SECRET_KEY` / `XTS_CLIENT_ID` | OMS Interactive API (`config.yaml` `${…}` refs) |
| `XTS_MD_API_KEY` / `XTS_MD_API_SECRET` | Market-data LTP + master downloads |
| `XTS_MD_REST_URL` / `XTS_MD_SOURCE` | Market-data base URL / source |
| `EXCHANGE1_API_KEY` / `EXCHANGE1_API_SECRET` / `EXCHANGE1_URL` | Crypto OMS path (`crypto_broker` in `config.yaml`) |

## `config.yaml`

OMS / broker / storage / logging. `config.yaml` is gitignored; keep a local
copy in the repository root (or pass `--config`). Broker secrets use env
interpolation:

```yaml
broker:
  app_key: "${XTS_APP_KEY}"
  secret_key: "${XTS_SECRET_KEY}"
  client_id: "${XTS_CLIENT_ID}"
```

Loaded by `oms.config.load_config` after `python-dotenv`.

### OMS settings

| Setting | Purpose |
|---------|---------|
| `pull_address` / `pub_address` | ZMQ command input and response output |
| `max_queue_size` | Backpressure limit for queued broker work |
| `order_workers` | Number of concurrent queue consumers |
| `retry_attempts` / `retry_delay_ms` | Order-placement retry policy |
| `order_sync_interval` | Idle order-book reconciliation interval |
| `active_order_sync_interval` | Reconciliation interval with open orders |
| `timezone` | Response, log, and persistence timestamp timezone |

### Broker settings

| Setting | Purpose |
|---------|---------|
| `type` | Adapter selected by `create_broker` (`xts`, or `exchange1` for crypto-only) |
| `url` | Broker REST base URL |
| `source` / `client_id` | XTS account/session parameters |
| `verify_ssl` | TLS certificate verification for broker requests |
| `socket_enabled` | Enables low-latency Socket.IO order events (XTS) |
| `socket_reconnect` | Enables socket reconnection behavior |

### Crypto broker (optional)

Add a `crypto_broker` section to route inferred crypto symbols to eXchange1
while keeping India orders on XTS. The webhook/bridge payload does **not**
need an `assetClass` field — `BTCUSDT` / `ETH/USDT` style symbols are detected
automatically.

```yaml
crypto_broker:
  enabled: true
  type: exchange1
  url: "${EXCHANGE1_URL}"
  app_key: "${EXCHANGE1_API_KEY}"
  secret_key: "${EXCHANGE1_API_SECRET}"
  verify_ssl: true
```

| Variable | Purpose |
|----------|---------|
| `EXCHANGE1_API_KEY` / `EXCHANGE1_API_SECRET` | eXchange1 OpenAPI credentials |
| `EXCHANGE1_URL` | Default `https://openapi.exchange1.com` |

### Storage and logging

Storage paths are relative to the process working directory. Launching from a
different directory can therefore create a second set of runtime files.
Start from the repository root unless absolute paths are configured.

Log file names support session timestamp templates. Rotation size and backup
count control local log retention; they do not archive order/trade CSV files.

## Webhook `.env`

See `webhook/.env.example` for `SIGNAL_PORT`, `BRIDGE_HOST`, `BRIDGE_PORT`, `BRIDGE_API_BASE`.

`BRIDGE_HOST` and `BRIDGE_PORT` are used by the Node server when forwarding.
`BRIDGE_API_BASE` is sent to browser code and must be reachable from the
browser's machine.

## Precedence and validation

1. Environment is loaded from `.env`.
2. YAML values are parsed.
3. `${VAR}` placeholders are recursively expanded.
4. Parsed sections are converted to configuration dataclasses.

Missing or unsupported broker configuration should fail during construction
or login. Keep secret placeholders in YAML rather than copying secret values
into `config.yaml`.

## Launch

```bat
script.bat
```

Or manually:

```bash
python run_oms.py
python run_bridge.py --port 5002
cd webhook && npm start
```

See [operations.md](operations.md) for startup order, health checks, and
recovery procedures.
