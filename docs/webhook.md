# Webhook & Dashboard

Entry point: `node webhook/server.js` (shim: `index.js`).

## Layout

```
webhook/
  server.js                 Process entry
  RESTSignalReceiver.js     Express /signal route
  normalizeSignal.js        Shared validation / normalization
  bridgeClient.js           fetch-based forwarder to Python bridge
  SignalSource.js           Abstract base
  public/
    index.html              Dashboard shell
    styles.css
    app.js
  .env.example
```

## Env vars

| Variable | Default | Meaning |
|----------|---------|---------|
| `SIGNAL_PORT` | `5001` | Webhook + dashboard listen port |
| `BRIDGE_HOST` | `127.0.0.1` | Python bridge host |
| `BRIDGE_PORT` / `OMS_BRIDGE_PORT` | `5002` | Python bridge port |
| `BRIDGE_API_BASE` | `http://host:port` | Browser-side API base (served as `/runtime-config.js`) |

## Signal path

1. `POST /signal` → `normalizeSignal`
2. Emit `signal` event → `bridgeClient.forward` → Python `POST :5002/signal`
3. Persist to `signals.json` (capped at 1000 entries)

## Dashboard

Served at `http://localhost:5001/`. Position/history/alert/order calls hit the
Python bridge directly (not proxied through Node).
