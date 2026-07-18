# OMS Package

Entry point: `python run_oms.py`.

## Layout

```
oms/
  broker/     AbstractBrokerAdapter, XTS adapter, Socket.IO, factory
  core/       OrderManager facade, transport, dispatcher, workers, sync, positions
  models/     Order, OrderResponse, enums, XTS_STATUS_MAP
  storage/    StorageBackend protocol + FileStore (async to_thread I/O)
  utils/      env, timeutil, runtime, logger, rate_limiter
  config.py   YAML + ${VAR} env interpolation
```

## Lifecycle

1. Load `config.yaml` (secrets from `.env` via `${XTS_APP_KEY}` etc.)
2. `create_broker(cfg.broker)` → login
3. Optional XTS Socket.IO attach for live fills
4. `OrderManager.start()` binds ZMQ, restores order state, runs workers

## Message types (ingress)

`PLACE_ORDER`, `CANCEL_ORDER`, `MODIFY_ORDER`, `SQUAREOFF`, `CANCEL_ALL`

## Message types (egress, topic = strategy_id)

`ORDER_ACK`, `ORDER_OPEN`, `ORDER_PARTIAL`, `ORDER_FILLED`, `ORDER_CANCELLED`,
`ORDER_REJECTED`, `ORDER_EXPIRED`, `ORDER_ERROR`, `CANCEL_ACK`, `MODIFY_ACK`,
`MODIFY_REJECTED`, `SQUAREOFF_ACK`

See [message-formats.md](message-formats.md) for payloads.
