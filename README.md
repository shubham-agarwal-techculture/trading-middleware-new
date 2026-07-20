# Trading Middleware

End-to-end async signal → order pipeline: **Node webhook + dashboard**, **Python signal bridge**, and a **ZeroMQ OMS** with broker adapters for **XTS (India equity/F&O)** and optional **eXchange1 (crypto spot)**.

## Docs

| Doc | Contents |
|-----|----------|
| [docs/architecture.md](docs/architecture.md) | Topology, patterns, process map |
| [docs/design-patterns.md](docs/design-patterns.md) | Pattern participants, trade-offs, state transitions, extension points |
| [docs/oms.md](docs/oms.md) | OMS package & lifecycle |
| [docs/bridge.md](docs/bridge.md) | Bridge HTTP API & resolution |
| [docs/webhook.md](docs/webhook.md) | Node webhook & dashboard |
| [docs/configuration.md](docs/configuration.md) | `.env`, `config.yaml`, XTS + Exchange1, launch |
| [docs/message-formats.md](docs/message-formats.md) | HTTP / ZMQ / JSON schemas |
| [docs/operations.md](docs/operations.md) | Startup, health checks, recovery, security |
| [docs/development.md](docs/development.md) | Testing, extension recipes, review checklist |
| [webhook/README.md](webhook/README.md) | Webhook quick start |

## Quick start

1. Copy `.env.example` → `.env` and fill XTS credentials (and Exchange1 keys if using crypto).
2. Ensure a local `config.yaml` exists (gitignored) with `${XTS_*}` broker
   placeholders. For crypto, add a `crypto_broker` section (see
   [configuration](docs/configuration.md)).
3. Install Python deps (`pip install -r requirements.txt`) and `cd webhook && npm install`.
4. Launch (Windows): `script.bat` — or manually:

```bash
python run_oms.py
python run_bridge.py --port 5002
cd webhook && npm start
```

- Dashboard: http://localhost:5001  
- Bridge API: http://localhost:5002  
- OMS ZMQ: `tcp://127.0.0.1:5555` (PUSH) / `5556` (SUB)

If ports `5001` / `5002` are busy, the webhook and bridge auto-select the next free port and log the chosen URL. The bridge writes `.bridge_http_port` so the dashboard stays in sync.

## Architecture (summary)

```mermaid
flowchart LR
  TV[TradingView] --> WH[webhook :5001]
  WH --> BR[run_bridge :5002]
  BR --> OMS[run_oms ZMQ]
  OMS --> XTS[XTS Interactive]
  OMS --> EX1[eXchange1 Spot]
  DASH[Dashboard UI] -->|API| BR
```

**Note:** The dashboard is served by Node on `:5001`, but its position/history/alert/order calls go **directly to the Python bridge** on `:5002`.

### Asset routing (no payload change)

The bridge classifies each signal from existing fields (`symbol` / `ticker` / `exchange_segment`):

| Detected as | Examples | Broker |
|-------------|----------|--------|
| India F&O / equity | `NIFTY260721C25150`, `RELIANCE`, `NSEFO` | XTS |
| Crypto | `BTCUSDT`, `ETH/USDT`, `SOL-USDT` | eXchange1 |

There is **no** required `assetClass` field. The dashboard **Crypto** tab posts the same signal shape as India orders; routing is server-side.

## Entry points

| Role | Command |
|------|---------|
| OMS | `python run_oms.py` |
| Bridge | `python run_bridge.py` |
| Webhook | `node webhook/server.js` |
| Master CSVs | `python -m market_data.download_masters` |
| ATM smoke | `python -m market_data.atm` |

## Dashboard

- **Open Positions / New Order / Alerts / History** — India (XTS) book and orders  
- **Crypto** — Exchange1 pairs, crypto positions, same long-only / square-off rules  

Alerts show compact cards with a **Details** expand for full order fields.

## Tests

```bash
python -m pytest -q
```

Covers bridge resolution/display, crypto vs India classification, and OMS place/cancel/modify/fill flows via a `FakeBroker`.

## Design principles

- Keep broker-specific requests and status parsing behind the broker adapter
  (XTS, Exchange1) and route via the broker factory / router.
- Treat `OrderManager` as the OMS facade and compose dependencies at entry
  points.
- Use bounded queues for broker work and tolerate duplicate socket/polling
  events.
- Keep HTTP, ZMQ, and runtime file formats backward compatible.
- Reconcile local state with the broker after uncertain failures.

For implementation details, see
[design patterns](docs/design-patterns.md) and the
[development guide](docs/development.md). For production startup and recovery,
see the [operations guide](docs/operations.md).

## Verification curl

India (XTS):

```bash
curl -X POST http://localhost:5001/signal \
  -H "Content-Type: application/json" \
  -d "{\"action\":\"BUY\",\"position\":\"long\",\"quantity\":75,\"ticker\":\"NIFTY260630C27000\"}"
```

Crypto (Exchange1 — same payload shape):

```bash
curl -X POST http://localhost:5001/signal \
  -H "Content-Type: application/json" \
  -d "{\"action\":\"BUY\",\"position\":\"long\",\"quantity\":0.01,\"symbol\":\"BTCUSDT\",\"orderType\":\"LIMIT\",\"limitPrice\":65000,\"productType\":\"SPOT\"}"
```
