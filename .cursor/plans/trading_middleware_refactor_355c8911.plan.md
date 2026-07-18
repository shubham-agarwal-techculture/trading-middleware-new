---
name: Trading Middleware Refactor
overview: "Refactor the entire trading middleware (Python OMS, the signal bridge and helper scripts, and the Node.js webhook/dashboard) into a clean, well-documented, pattern-driven codebase while preserving all external behavior and entry points. Work proceeds safety-net-first: add characterization tests, then refactor incrementally, verifying behavior after each phase."
todos:
  - id: safety-net
    content: "Phase 0a: Add characterization pytest suite for bridge pure logic (ticker parse, tv->xts, expiry, resolution, position display) and OMS OrderManager flows via a FakeBroker."
    status: completed
  - id: secrets
    content: "Phase 0b: Move broker + XTS market-data credentials to env/.env with .env.example; add ${VAR} interpolation in oms/config.py; scrub config.yaml and script literals."
    status: completed
  - id: dead-code
    content: "Phase 0c: Remove commented dead blocks, duplicate imports, unused maps/vars, and dead getLogs()."
    status: completed
  - id: shared-foundation
    content: "Phase 1: Consolidate now_iso/IST, Win32 loop bootstrap, canonical XTS_STATUS_MAP, and shared XTS credential config across files."
    status: completed
  - id: oms-split
    content: "Phase 2a: Split OrderManager into ZmqTransport, SignalDispatcher (Command), OrderWorker pool, BrokerEventProcessor, OrderBookSync; keep OrderManager as facade."
    status: completed
  - id: oms-broker
    content: "Phase 2b: Complete AbstractBrokerAdapter (cancel_all, squareoff, BrokerEventParser protocol), add broker factory from config.type, wire from oms_server.py."
    status: completed
  - id: oms-models-storage
    content: "Phase 2c: Enforce enums + from_dict on Order/OrderResponse; add StorageBackend protocol with async-safe file I/O; add Position dataclass with apply_fill."
    status: completed
  - id: bridge-package
    content: "Phase 3a: Decompose nifty_signal_bridge.py into bridge/ package (resolution, positions, market_data, signal_service, http_server); keep thin entrypoint."
    status: completed
  - id: scripts-package
    content: "Phase 3b: Move strategy_client/nifty_atm_ltp/get_masters_data into clients/ and market_data/ packages sharing one XTS client + ContractLoader; relocate root test script."
    status: completed
  - id: webhook
    content: "Phase 4: Refactor Node webhook (normalizeSignal, fetch-based bridgeClient, config-driven host/port, fix SignalSource JSDoc, split index.html, remove debug log)."
    status: completed
  - id: rename
    content: "Renaming: apply the file/module rename map (run_bridge.py, run_oms.py, clients/oms_client.py, market_data/*, download_masters.py, scripts/resolve_contracts_demo.py, webhook server.js + split index.html) and update all imports, entry points, script.bat, package.json, README/docs."
    status: completed
  - id: docs
    content: "Phase 5: Add docstrings/JSDoc throughout and create docs/ (architecture, oms, bridge, webhook, configuration, message-formats) + update README and add webhook/README."
    status: completed
  - id: verify
    content: Run pytest and end-to-end smoke test (OMS + bridge + webhook) after each phase to confirm identical behavior.
    status: completed
isProject: false
---

# Trading Middleware Refactor

## Guiding constraints
- **Behavior must not change.** Same CLI entry points (`python oms_server.py`, `python nifty_signal_bridge.py`, `cd webhook && npm start`), same HTTP endpoints, same ZMQ message contract, same JSON file schemas (`positions.json`, `history.json`, `alerts.json`, `data/*`).
- **Safety-net-first.** Add characterization tests before touching logic; run them after every phase.
- Do not commit unless asked. Refactor in place on the current branch.

## Target architecture
```mermaid
flowchart TD
  subgraph node [webhook/ Node.js]
    rec[RESTSignalReceiver] --> norm[normalizeSignal]
    norm --> fwd[bridgeClient.forward]
  end
  subgraph bridge [bridge/ Python package]
    http[http_server] --> svc[SignalService]
    svc --> res[resolution]
    svc --> md[market_data]
    svc --> pos[PositionStore]
    svc --> omsc[OMSClient]
  end
  subgraph oms [oms/ package]
    tr[ZmqTransport] --> disp[SignalDispatcher]
    disp --> wk[OrderWorker pool]
    wk --> brk[BrokerAdapter via factory]
    evp[BrokerEventProcessor] --> wk
    brk --> store[StorageBackend]
  end
  fwd -->|POST :5002| http
  omsc -->|ZMQ 5555/5556| tr
```

## Phase 0 — Safety net, secrets, dead-code hygiene
- Add characterization tests (pytest) that pin current behavior of pure/high-value logic BEFORE refactoring:
  - Bridge resolution: `parse_tv_option_ticker`, `tv_to_xts_description`, `is_monthly_expiry`, `resolve_contract_by_ticker` (fixture CSV), `get_position_display_values` (extend `[tests/test_resolver.py](tests/test_resolver.py)`, `[tests/test_position_display.py](tests/test_position_display.py)`).
  - OMS: introduce a `FakeBroker(AbstractBrokerAdapter)` and test `OrderManager` place/cancel/modify/squareoff + `inject_broker_event` fill flow without network/ZMQ where feasible.
- Move secrets to env: add `python-dotenv` loading; read `broker.app_key/secret_key/client_id` and the XTS market-data credentials in `[nifty_atm_ltp.py](nifty_atm_ltp.py)` / `[get_masters_data.py](get_masters_data.py)` from env. Add `.env.example` and scrub literals from `[config.yaml](config.yaml)` (use `${VAR}` interpolation in `[oms/config.py](oms/config.py)`).
- Remove dead code: commented `tv_to_xts`/Thursday-expiry blocks and duplicate imports in `[nifty_signal_bridge.py](nifty_signal_bridge.py)`; commented test matrix in `[tests/test_resolver.py](tests/test_resolver.py)`; unused `asdict`/`datetime` imports and duplicate `XTS_STATUS_MAP` in `[oms/models/order.py](oms/models/order.py)`; unused `XTS_MARKETDATA_URL`; dead `getLogs()` in webhook.

## Phase 1 — Shared foundation (kill duplication)
- Consolidate cross-file boilerplate into shared helpers:
  - IST/`now_iso` → everyone uses `[oms/utils/timeutil.py](oms/utils/timeutil.py)` (remove local `get_ist_now` copies in `[nifty_signal_bridge.py](nifty_signal_bridge.py)` and `[strategy_client.py](strategy_client.py)`).
  - Win32 selector-loop bootstrap → one `oms/utils/runtime.py` helper used by all four entry points.
  - Single canonical `XTS_STATUS_MAP` in `[oms/models/order.py](oms/models/order.py)`; `[oms/broker/xts_adapter.py](oms/broker/xts_adapter.py)` imports it.
  - Shared XTS credentials/config object consumed by market-data scripts.

## Phase 2 — OMS package refactor (design patterns)
- Split the `OrderManager` god-class in `[oms/core/order_manager.py](oms/core/order_manager.py)` into collaborators, keeping `OrderManager` as a thin **Facade**:
  - `ZmqTransport` (receive/publish), `SignalDispatcher` (**Command** map `msg_type`→handler), `OrderWorker` pool (producer/consumer), `BrokerEventProcessor` (fill/qty normalization shared with adapter), `OrderBookSync`.
- Complete the broker **Adapter** interface in `[oms/broker/base.py](oms/broker/base.py)`: add `cancel_all_orders`, `squareoff_position`, and a `BrokerEventParser` **Protocol**; inject the parser into `BrokerEventProcessor` instead of hardcoding `XTSBrokerAdapter.parse_order_event`.
- Add a broker **Factory** keyed on `config.broker.type` (wired from `[oms_server.py](oms_server.py)`).
- Typed domain models: enforce enums on `Order` fields, add `Order.from_dict`; type `OrderResponse.msg_type`. 
- Storage: `StorageBackend` **Protocol** (**Repository/Strategy**) with the current file impl in `[oms/storage/file_store.py](oms/storage/file_store.py)`; wrap blocking I/O in `asyncio.to_thread`. Add a `Position` dataclass with `apply_fill` in `[oms/core/position_tracker.py](oms/core/position_tracker.py)`.

## Phase 3 — Bridge + client scripts decomposition
- Break `[nifty_signal_bridge.py](nifty_signal_bridge.py)` (~1500 lines) into a `bridge/` package; keep `nifty_signal_bridge.py` as a thin entrypoint:
  - `bridge/resolution.py` (ticker parsing, tv→xts, multi-exchange resolver, cached master loader — **Strategy** per resolution mode).
  - `bridge/positions.py` (`PositionStore` **Repository** for positions/history/alerts JSON).
  - `bridge/market_data.py` (LTP fetch + position hydration).
  - `bridge/signal_service.py` (`handle_signal` orchestration; **Command** objects for BUY / SELL / FLAT / manual square-off).
  - `bridge/http_server.py` (route table + a JSON/CORS response helper to remove the repeated `send_response`/header boilerplate).
- Move helpers into packages sharing one XTS market-data client + `ContractLoader`: `market_data/` (from `[nifty_atm_ltp.py](nifty_atm_ltp.py)` + `[get_masters_data.py](get_masters_data.py)`) and `clients/` (from `[strategy_client.py](strategy_client.py)`), fixing the docstring/return-value drift in `OMSClient`.
- Relocate `[test_multi_exchange_resolution.py](test_multi_exchange_resolution.py)` into `tests/` or `scripts/`.

## Phase 4 — Node.js webhook + dashboard
- Extract `normalizeSignal(body)` (shared validation/normalization) and a promise/`fetch`-based `bridgeClient` (replace nested-callback `forwardSignal`) in `[webhook/index.js](webhook/index.js)` / `[webhook/RESTSignalReceiver.js](webhook/RESTSignalReceiver.js)`.
- Config-drive bridge host/port and dashboard `API_BASE`; add `.env.example`. Fix `SignalSource` JSDoc drift in `[webhook/SignalSource.js](webhook/SignalSource.js)`. Split `[webhook/public/index.html](webhook/public/index.html)` into HTML + `app.js` + `styles.css`. Remove `console.log(req.body)`.

## Phase 5 — Documentation
- Add docstrings (module/class/function) across all refactored Python + JSDoc for JS.
- Create `docs/`: `architecture.md`, `oms.md`, `bridge.md`, `webhook.md`, `configuration.md`, `message-formats.md` (with mermaid diagrams), and update `[README.md](README.md)` (fix the dashboard-routes-through-Node inaccuracy). Add `webhook/README.md`.

## Verification after each phase
- `pytest` green; manual smoke: start OMS + bridge + webhook, submit the README test signals, confirm identical responses, position/history/alert files, and ZMQ payloads.