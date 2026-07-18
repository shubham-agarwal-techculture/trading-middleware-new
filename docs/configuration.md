# Configuration

## Secrets (`.env`)

Copy `.env.example` → `.env` (gitignored). Single-quote values that contain `$` or `#`.

| Variable | Used by |
|----------|---------|
| `XTS_APP_KEY` / `XTS_SECRET_KEY` / `XTS_CLIENT_ID` | OMS Interactive API (`config.yaml` `${…}` refs) |
| `XTS_MD_API_KEY` / `XTS_MD_API_SECRET` | Market-data LTP + master downloads |
| `XTS_MD_REST_URL` / `XTS_MD_SOURCE` | Market-data base URL / source |

## `config.yaml`

OMS / broker / storage / logging. Broker secrets use env interpolation:

```yaml
broker:
  app_key: "${XTS_APP_KEY}"
  secret_key: "${XTS_SECRET_KEY}"
  client_id: "${XTS_CLIENT_ID}"
```

Loaded by `oms.config.load_config` after `python-dotenv`.

## Webhook `.env`

See `webhook/.env.example` for `SIGNAL_PORT`, `BRIDGE_HOST`, `BRIDGE_PORT`, `BRIDGE_API_BASE`.

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
