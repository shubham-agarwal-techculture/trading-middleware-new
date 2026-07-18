# Operations Guide

This guide covers startup, health checks, runtime files, recovery, and common
failure scenarios. Commands assume the repository root unless noted.

## Startup order

Start components in this order:

1. OMS: `python run_oms.py`
2. Bridge: `python run_bridge.py --port 5002`
3. Webhook/dashboard: `cd webhook && npm start`
4. Optional public ingress: `ngrok http 5001`

`script.bat` opens these processes in Windows Terminal. It also downloads
master data concurrently with bridge startup, so a cold start can race if
master CSVs are missing or still being written. Prefer downloading masters
before starting the bridge on a fresh machine.

The OMS can start in degraded mode if broker login fails, but order placement
will not work until the broker connection is available. The bridge expects
market-data access during startup because it fetches initial ATM contracts.

## Pre-flight checklist

- `.env` exists and contains valid interactive and market-data credentials.
- Local `config.yaml` exists (gitignored) and contains the intended broker
  URL, ZMQ addresses, and `${XTS_*}` secret placeholders.
- `webhook/.env` contains the externally reachable bridge API base when the
  dashboard is not running entirely on localhost.
- master contract CSVs are current;
- ports `5001`, `5002`, `5555`, and `5556` are free;
- the account has permission for the configured product and exchange segment;
- system time and timezone are correct.

## Health checks

### Process and port checks

Expected listeners:

```text
5001  Node webhook and dashboard
5002  Python bridge HTTP API
5555  OMS ZMQ PULL
5556  OMS ZMQ PUB
```

### Bridge API checks

```bash
curl http://localhost:5002/positions
curl http://localhost:5002/alerts
curl http://localhost:5002/history
```

A successful HTTP response confirms the bridge HTTP thread is running. It
does not by itself prove the broker session or OMS sockets are healthy.

### End-to-end signal check

Use a contract and quantity suitable for the configured account:

```bash
curl -X POST http://localhost:5002/signal \
  -H "Content-Type: application/json" \
  -d "{\"action\":\"BUY\",\"position\":\"long\",\"quantity\":75,\"ticker\":\"NIFTY260630C27000\"}"
```

Record the returned `signal_id`, then query:

```bash
curl "http://localhost:5002/status?signal_id=<signal-id>"
```

For production checks, use a controlled instrument, quantity, and order type.
Do not treat a live order as a harmless health probe.

Posting through Node on port `5001` tests public ingress, but that route
acknowledges receipt before asynchronous forwarding and does not return the
bridge `signal_id`. Check Node logs for the forwarding result.

## Logs and runtime state

### OMS

The OMS writes session logs under `logs/`:

- `oms_<timestamp>.log` for orchestration and order lifecycle;
- `xts_<timestamp>.log` for broker requests, responses, and socket activity.

The `data/` directory contains:

- `orders_state.json`: current order snapshot used on restart;
- `positions.json`: OMS position snapshot;
- `orders_log_<date>.csv`: append-only order events;
- `trades_<date>.csv`: append-only fills;
- `statistics_<date>.json`: daily aggregate statistics.

### Bridge

The bridge maintains root-level dashboard state:

- `positions.json`: bridge-visible open positions;
- `history.json`: recently closed positions;
- `alerts.json`: recent warnings and events.

### Webhook

`webhook/signals.json` stores the most recent normalized webhook signals,
capped at 1000 entries.

Do not edit runtime JSON while its owning process is writing it. Stop the
process first and keep a backup before manual recovery.

## Failure and recovery

### Broker login failure

Symptoms:

- OMS logs “continuing in degraded mode”;
- no Socket.IO feed starts;
- place/modify/cancel operations return broker errors.

Checks:

1. Verify `XTS_APP_KEY`, `XTS_SECRET_KEY`, and `XTS_CLIENT_ID`.
2. Verify the broker base URL and account mode.
3. Check SSL verification settings and local network access.
4. Restart the OMS after credentials or connectivity are corrected.

### Bridge startup fails while fetching ATM data

Check market-data credentials, REST URL, master data, and network access.
Ticker-explicit requests still require the bridge process to start, so the
initial ATM fetch is currently a startup dependency.

### Orders acknowledge but fills do not appear

1. Check whether the XTS Socket.IO connection is active.
2. Check OMS order-book polling logs.
3. Confirm the broker order appears in the broker terminal.
4. Compare broker order ID, cumulative fill quantity, and OMS state.
5. Avoid manually replaying a fill unless duplicate handling has been
   verified.

The polling synchronizer is designed to recover missed socket events. With
active orders it uses `active_order_sync_interval`; otherwise it uses
`order_sync_interval`.

### Dashboard loads but API data fails

The dashboard is served by Node on `:5001`, while browser API requests go
directly to the bridge. Check:

- `BRIDGE_API_BASE` in `webhook/.env`;
- `/runtime-config.js` in the browser;
- bridge CORS responses;
- whether the browser can reach the configured host and port.

`127.0.0.1` means the browser's machine, not necessarily the server hosting
Node.

### Stale contract or “contract not found”

Refresh master files:

```bash
python -m market_data.download_masters
```

Then restart the bridge so cached loaders and ATM data are rebuilt.

### Port already in use

Stop the old process or change the configured port. If changing bridge port,
update both the bridge launcher and webhook/browser configuration. If changing
ZMQ addresses, update OMS configuration and bridge client constants together.

## Safe restart procedure

1. Stop webhook ingress to prevent new signals.
2. Wait for active orders to reach a known state in the broker terminal.
3. Stop the bridge.
4. Stop the OMS and allow graceful persistence to complete.
5. Back up `data/` and bridge runtime JSON if investigating an incident.
6. Start OMS, bridge, then webhook.
7. Reconcile open broker orders and positions with restored local state.

Local JSON and CSV files are not a replacement for broker reconciliation.
After a crash or uncertain network period, treat the broker order book as the
external source of truth.

## Backup and retention

- Back up `.env` securely outside source control.
- Back up `data/` if local order and trade history is required.
- Rotate or archive `logs/` according to operational policy.
- `orders_log` and `trades` are append-only daily files; dashboard history and
  alerts are capped and should not be treated as permanent audit storage.

## Security notes

- Never commit `.env`, broker tokens, client IDs, or copied API responses that
  contain credentials.
- Bind public HTTP ports only when required and place authentication or an
  allowlist in front of internet-facing webhook endpoints.
- The bridge HTTP API currently allows CORS `*` and has no authentication.
- ZMQ sockets have no authentication, durable delivery, or replay.
- Ngrok URLs expose the webhook to the internet; rotate URLs/tokens and stop
  tunnels when not in use.
- `verify_ssl: false` disables broker TLS certificate verification. Enable it
  when the broker endpoint and certificate chain support verification.
- Restrict access to runtime files because they contain trading activity and
  account metadata.

## Known operational limits

- Node `/signal` acknowledges before asynchronous bridge forwarding finishes.
- Bridge positions can appear on acknowledgement, before fills; OMS positions
  are a separate fill-derived ledger.
- Bridge pending-order state and ATM cache are in-memory only.
- OMS placement retries cover place-order failures; cancel/modify/square-off
  do not share that retry loop.
- Degraded OMS mode after login failure does not auto-recover a session.
- There is no in-repo health/readiness endpoint or metrics exporter.
