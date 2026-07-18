# Message Formats

## HTTP → Bridge (`POST /signal`)

```json
{
  "action": "BUY",
  "position": "long",
  "quantity": 75,
  "ticker": "NIFTY260630C27000",
  "orderType": "LIMIT",
  "productType": "MIS",
  "limitPrice": 100.0
}
```

Required: `action`, `position`, `quantity`. Optional: `ticker`/`symbol`,
`exchange_segment` + `exchange_instrument_id`, order/product/limit fields.

## ZMQ → OMS (PUSH JSON)

```json
{
  "msg_type": "PLACE_ORDER",
  "strategy_id": "NIFTY_SIGNAL_BRIDGE",
  "signal_id": "abc123",
  "exchange_segment": "NSEFO",
  "exchange_instrument_id": 41723,
  "instrument_name": "NIFTY26JUN27000CE",
  "product_type": "MIS",
  "order_type": "LIMIT",
  "order_side": "BUY",
  "time_in_force": "DAY",
  "order_quantity": 75,
  "limit_price": 100.0,
  "tags": {}
}
```

## ZMQ ← OMS (PUB: `"{strategy_id} {json}"`)

```json
{
  "msg_type": "ORDER_FILLED",
  "strategy_id": "NIFTY_SIGNAL_BRIDGE",
  "oms_order_id": "…",
  "signal_id": "abc123",
  "status": "FILLED",
  "filled_quantity": 75,
  "avg_fill_price": 100.0,
  "broker_order_id": "BRK1"
}
```

## Bridge file schemas

- `positions.json` — keyed by `exchange_instrument_id` string
- `history.json` — array of closed positions (newest first, max 1000)
- `alerts.json` — array of alert objects (max 100)
- OMS `data/` — order log CSV, trades CSV, orders_state.json, positions.json
