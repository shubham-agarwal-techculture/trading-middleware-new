# Development Guide

## Repository map

```text
run_oms.py                 OMS composition root and process lifecycle
run_bridge.py              Bridge composition root
oms/                       Order-management domain and infrastructure
bridge/                    HTTP signal orchestration and dashboard state
clients/                   ZMQ OMS client
market_data/               XTS market-data client and contract loaders
webhook/                   Node webhook receiver and browser dashboard
tests/                     Characterization and domain tests
docs/                      Architecture and operating documentation
```

Entry points should construct and connect components. Business behavior belongs
in packages, not in launcher scripts.

## Local setup

```bash
python -m venv .venv
pip install -r requirements.txt
cd webhook
npm install
```

Copy `.env.example` to `.env` and `webhook/.env.example` to `webhook/.env`,
then provide local values. Never use production credentials in committed test
fixtures.

## Test suite

Run all Python tests:

```bash
python -m pytest -q
```

The suite covers:

- TradingView ticker parsing and contract resolution;
- monthly-expiry behavior;
- position display calculations;
- `Position` fill accounting;
- OMS place, cancel, modify, partial-fill, full-fill, and broker-error flows
  using a `FakeBroker`.

Current gaps include live ZMQ lifecycle, bridge HTTP/threading, Node
normalization/forwarding, XTS REST/socket integration, order-book sync,
persistence restore/failure paths, and end-to-end smoke automation. There is
no Node test runner or CI workflow in-repo yet.

The tests are primarily characterization tests. When changing behavior,
determine whether the old behavior is part of an external contract before
updating assertions.

## Testing boundaries

### Pure logic

Prefer direct unit tests for ticker conversion, enum/domain transitions,
normalization, and position calculations.

### OMS workflows

Use the fake broker and in-memory test state. Assert both broker calls and
published OMS responses. Include duplicate, partial, terminal, timeout, and
error cases when changing lifecycle logic.

### Broker adapter

Keep parsing tests independent of a live account. Feed captured, scrubbed
broker payloads to event parsers and verify normalized output. Never commit
tokens, account numbers, or personal data.

### HTTP and webhook

Test normalization separately from forwarding. Integration tests should bind
ephemeral ports and use temporary runtime files where possible.

## Change recipes

### Add an OMS command

1. Define the request fields in `docs/message-formats.md`.
2. Add or reuse a response type in `oms.models.response`.
3. Implement the async handler in the OMS workflow.
4. Register it with `SignalDispatcher`.
5. Decide whether it uses the worker queue or can execute immediately.
6. Persist state before publishing a success that depends on that state.
7. Add happy-path, malformed-input, broker-error, and duplicate tests.
8. Document the command in `docs/oms.md`.

### Add a broker

1. Implement `AbstractBrokerAdapter`.
2. Implement event normalization compatible with `BrokerEventProcessor` /
   `OrderManager.inject_broker_event`.
3. Add the adapter to `create_broker`.
4. Extend configuration without exposing credentials.
5. Confirm retry, cancellation, and shutdown semantics.
6. Run common workflow tests against the adapter contract.

### Replace storage

1. Implement `StorageBackend`.
2. Change `run_oms.py` construction; startup currently hardcodes `FileStore`.
3. Preserve atomic snapshot semantics and async-safe I/O.
4. Add restore/failure tests before relying on the new backend.

### Add a bridge endpoint

1. Keep HTTP parsing and response headers in `bridge.http_server`.
2. Put business orchestration in a bridge service module.
3. Avoid blocking calls on the bridge asyncio loop.
4. Define request, response, and error examples.
5. Update CORS behavior only as narrowly as required.
6. Add endpoint tests and update `docs/bridge.md`.

### Add a contract-resolution mode

1. Normalize external ticker syntax at the boundary.
2. Keep master-file lookup in `bridge.resolution` or `market_data.contracts`.
3. Use explicit segment hints before broad searches.
4. Return a common contract shape.
5. Add fixture-based tests for expiry, strike, option type, and exchange.

## Concurrency rules

- Do not perform blocking file or network operations directly on the asyncio
  event loop.
- Use the bounded order queue for broker-bound OMS work.
- Bridge HTTP runs in a separate thread; schedule coroutine work on the main
  loop through the established thread-safe path.
- Treat socket and polling updates as potentially duplicated and reordered.
- Protect shared state transitions by keeping mutations in the owning event
  loop where possible.
- On shutdown, stop intake before cancelling workers and close sockets after
  pending persistence completes.

## Data and compatibility rules

External behavior includes:

- HTTP paths and JSON field names;
- ZMQ topic framing and message types;
- accepted camelCase and snake_case signal aliases;
- runtime JSON and CSV schemas;
- entry-point commands and default ports.

Refactors should preserve these contracts unless a migration is explicitly
planned. If a schema changes, document versioning, compatibility behavior,
and recovery for existing files.

## Logging guidelines

- Include `strategy_id`, `signal_id`, `oms_order_id`, and `broker_order_id`
  whenever available.
- Log state transitions, not every polling iteration.
- Keep secrets and authentication responses out of logs.
- Use structured OMS logging fields rather than interpolating large broker
  payloads into messages.
- Record enough context to correlate webhook, bridge, OMS, and broker events.

## Documentation checklist

For user-visible changes, update the relevant files:

- architecture or dependency changes: `architecture.md`,
  `design-patterns.md`;
- configuration changes: `configuration.md`, `.env.example`;
- request/response changes: `message-formats.md`;
- operational behavior: `operations.md`;
- component behavior: `oms.md`, `bridge.md`, or `webhook.md`;
- startup commands or new documents: root `README.md`.

## Review checklist

- Are broker and storage details kept behind their interfaces?
- Can malformed external input fail without corrupting state?
- Is retry behavior safe for an operation that may already have succeeded?
- Are duplicate broker events idempotent?
- Is state persisted before dependent success responses are emitted?
- Are new files and caches ignored appropriately?
- Do tests pass without live broker credentials?
- Do docs describe actual behavior rather than intended behavior?
