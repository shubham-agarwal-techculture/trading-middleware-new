# Standalone Nifty Signal Bridge

This plan introduces a standalone script, `nifty_signal_bridge.py`, which is loosely coupled from the current `oms_bridge.py` bridge server. 

When a trade signal is received, the script automatically parses the `master_data/NSEFO.csv` file, resolves the `NIFTY 25000 CE` contract with the desired expiry (supporting both the nearest upcoming expiry or the chronologically furthest/latest expiry as of today), and routes the order to the OMS client using a dummy price.

## User Review Required

> [!NOTE]
> **Expiry Date Selection Modes:**
> We have implemented configurable selection modes for "latest expiry as of today". In options trading, "latest" can refer to:
> 1. `nearest` (Recommended): The closest active/upcoming weekly or monthly contract (e.g., June 9, 2026). This is the standard, liquid contract traders use.
> 2. `furthest` (Literal Latest): The chronologically furthest expiration date in the CSV (e.g., December 28, 2027).
> 
> The script will default to `nearest` but can be easily toggled to `furthest` via a constant or configuration.

> [!IMPORTANT]
> **Dummy Pricing:**
> As requested, there is no integration with market data (LTP/depth feeds) for this script. A constant dummy limit price (default `1.0`) is used for order placement, or we can use the limit price from the incoming signal if provided.

## Proposed Changes

### Nifty Signal Bridge Script

#### [NEW] [nifty_signal_bridge.py](file:///d:/projects-shubham/02.06.2026/mukul_scripts/nifty_signal_bridge.py)

We will create a standalone script that contains:
1. **CSV Parsing Logic**:
   - Reads `master_data/NSEFO.csv` on startup.
   - Indexes all contracts by `(Name, StrikePrice, OptionType)` in a dictionary to make lookups fast and $O(1)$.
   - Filters the contracts where `Name == "NIFTY"`, `StrikePrice == 25000`, `OptionType == "3"` (Call Option / CE), and the parsed `ContractExpiration` is $\ge$ the current date.
   - Resolves the contract based on the configured mode (`nearest` or `furthest`).
2. **HTTP Server**:
   - Listens on a configurable port (defaults to `5002` to act as a drop-in replacement for the Node webhook forwarder).
   - Exposes a `/signal` POST route.
3. **OMS Client Integration**:
   - Imports `OMSClient` from `strategy_client.py` to handle the ZeroMQ connection to the OMS.
   - On signal receipt, resolves the contract details from the indexed CSV data, extracts the `ExchangeInstrumentID`, `Description` (for name), and `LotSize` (for quantity).
   - Generates and sends a `PLACE_ORDER` (or `SQUAREOFF`) signal using a dummy limit price.
   - Waits for the ZeroMQ `ORDER_ACK` and returns the status and `oms_order_id` in the response.

## Verification Plan

### Automated Tests
We will verify the script by:
1. Running the standalone bridge script:
   ```bash
   python nifty_signal_bridge.py --port 5002
   ```
2. Triggering a POST signal using `curl`:
   ```bash
   curl -X POST http://localhost:5002/signal \
     -H "Content-Type: application/json" \
     -d '{"action": "BUY", "position": "long", "quantity": 65}'
   ```
3. Observing the console output to verify that:
   - The CSV is loaded and indexed correctly.
   - The contract resolved is `NIFTY2660925000CE` (June 9, 2026 expiry) under `nearest` mode, or `NIFTY27DEC25000CE` (Dec 28, 2027 expiry) under `furthest` mode.
   - The correct instrument ID, lot size, and dummy price are printed and sent to the OMS client.
