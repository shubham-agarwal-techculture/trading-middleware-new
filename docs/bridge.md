# Signal Bridge

Entry point: `python run_bridge.py --port 5002` (shim: `nifty_signal_bridge.py`).

## Package layout

```
bridge/
  resolution.py     TV ticker → master CSV contract (multi-exchange)
  positions.py      positions.json / history.json / alerts.json
  market_data.py    LTP hydrate for dashboard display
  signal_service.py handle_signal + OMS response tracking
  http_server.py    HTTP routes + JSON/CORS helper
  state.py          process globals (client, loop, atm_data, …)
```

Related packages:

- `clients/oms_client.py` — ZMQ client used by the bridge
- `market_data/` — XTS quotes, `ContractLoader`, ATM helpers, master downloader

## HTTP API (`:5002`)

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/signal` | Place / square-off via action + position |
| GET | `/status?signal_id=` | Pending order status |
| GET | `/positions` | Open positions (+ live LTP) |
| GET | `/alerts` | Recent alerts |
| GET | `/history` | Closed positions |
| POST | `/squareoff` | Manual square-off by `instrument_key` |

## Resolution order

1. Explicit `exchange_segment` + `exchange_instrument_id`
2. `ticker` / `symbol` via master CSVs (`NSEFO`, `BSEFO`, `MCXFO`, `NSECM`, `BSECM`)
3. Fallback: cached NIFTY ATM CE/PE from `market_data.get_atm_data()`
