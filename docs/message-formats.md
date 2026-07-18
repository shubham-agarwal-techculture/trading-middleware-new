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

Direct bridge requests require `action` and `position`; omitted quantity
defaults to the resolved contract lot size. The Node webhook additionally
requires a truthy `quantity`. Optional fields include:

- `ticker` or `symbol`;
- `exchange_segment` + `exchange_instrument_id`;
- `instrument_name` and `lot_size` for explicit IDs not found in masters;
- `orderType`/`order_type`, `productType`/`product_type`;
- `limitPrice`/`limit_price`, `stopPrice`/`stop_price`;
- `optionType` for ATM fallback.

The webhook normalizes action, quantity, order type, product type, and
timestamps before forwarding.

### Correlation

Every accepted order is associated with a `signal_id`. Preserve it across
logs and API calls:

```text
webhook record -> bridge pending order -> OMS request -> broker order
 signal_id          signal_id             signal_id      broker_order_id
```

`oms_order_id` identifies the internal OMS order. `broker_order_id` identifies
the external broker order. They are not interchangeable.

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

Commands use one JSON object per ZMQ message. Supported `msg_type` values are:

- `PLACE_ORDER`
- `CANCEL_ORDER`
- `MODIFY_ORDER`
- `SQUAREOFF`
- `CANCEL_ALL`

All commands should include `strategy_id` for response routing and
`signal_id` for request correlation.

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

The wire frame starts with the strategy topic, one space, then the JSON
payload. Subscribers must subscribe to the exact strategy topic they use for
commands.

Response types are:

- lifecycle: `ORDER_ACK`, `ORDER_OPEN`, `ORDER_PARTIAL`, `ORDER_FILLED`,
  `ORDER_CANCELLED`, `ORDER_REJECTED`, `ORDER_MODIFIED`, `ORDER_EXPIRED`,
  `ORDER_ERROR`;
- operation acknowledgements: `CANCEL_ACK`, `MODIFY_ACK`,
  `MODIFY_REJECTED`, `SQUAREOFF_ACK`.

Fill fields have cumulative and incremental meanings:

- `filled_quantity` and `avg_fill_price` are cumulative;
- `last_fill_quantity` and `last_fill_price` describe the latest execution;
- `pending_quantity` is the remaining unfilled quantity.

Consumers should use IDs and cumulative quantities to tolerate duplicated or
reordered broker observations.

## Bridge HTTP responses

`GET /status?signal_id=<id>` returns the bridge's current pending-order record,
or `{"status":"not_found"}`. A missing query parameter returns HTTP 400.

`GET /positions`, `/history`, and `/alerts` return their corresponding bridge
state. Position display values may be hydrated with live LTP and calculated
P&L when market data is available.

`POST /squareoff` accepts an `instrument_key` that identifies a bridge
position and creates a correlated reverse `MARKET` place-order request. It
does not use the OMS `SQUAREOFF` command type.

## Bridge file schemas

- `positions.json` — keyed by `exchange_instrument_id` string
- `history.json` — array of closed positions (newest first, max 1000)
- `alerts.json` — array of alert objects (max 100)
- OMS `data/` — order log CSV, trades CSV, orders_state.json, positions.json

Runtime file schemas are compatibility boundaries. Change them with migration
logic because existing files are loaded after process restarts.
