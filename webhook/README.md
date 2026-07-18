# Webhook service

Node.js Express app that:

1. Receives TradingView (and other) HTTP trade signals
2. Normalizes them (`normalizeSignal.js`)
3. Forwards them to the Python bridge (`bridgeClient.js`)
4. Serves the trading dashboard under `public/`

## Quick start

```bash
cd webhook
cp .env.example .env   # optional
npm install
npm start              # node server.js
```

Dashboard: http://localhost:5001  
Bridge target defaults to http://127.0.0.1:5002

## Test signal

```bash
curl -X POST http://localhost:5001/signal \
  -H "Content-Type: application/json" \
  -d "{\"action\":\"BUY\",\"position\":\"long\",\"quantity\":75,\"ticker\":\"NIFTY260630C27000\"}"
```

See [../docs/webhook.md](../docs/webhook.md) for full details.
