# Darkstore

An inventory-reservation and order service for a quick-commerce dark store, written in Go.

The problem it targets: a dark store holds a few dozen units of an item, and hundreds of people check out at the same moment. The service must never sell the same unit twice, must not let a retried request create a second order, and must release stock when a customer abandons checkout.

Stock can live in memory or in PostgreSQL, behind one `inventory.Inventory` interface; the same guarantees are tested against both. The only third-party dependency is the [pgx](https://github.com/jackc/pgx) PostgreSQL driver; everything else is the standard library (`net/http`, `log/slog`, `sync`, `context`).

## What it does

1. **Checkout** reserves stock for every item in the cart, or for none of them, and holds it for a limited time (default 2 minutes).
2. **Confirm** (payment succeeded) turns the hold into a sale and hands the order to a pool of workers.
3. **Cancel**, or letting the hold expire, returns the stock.
4. Workers move each order through `picking → packed → awaiting_rider → out_for_delivery → delivered`, assigning the nearest free rider.

## Guarantees, and how they are tested

| Guarantee | How | Test |
|---|---|---|
| Never oversell | One mutex guards stock and reservations; all cart lines are checked before any stock is taken | 1,000 goroutines race for 50 units: exactly 50 succeed, 950 are rejected, stock ends at 0. Repeated through the HTTP layer. |
| Never oversell, on PostgreSQL | `SELECT ... FOR UPDATE` on every SKU in the cart inside one transaction; a `CHECK (available >= 0)` constraint as a backstop | The same 1,000-vs-50 race against a real database: exactly 50 succeed, stock ends at 0 |
| Overlapping carts can't deadlock | Row locks are always taken in sorted SKU order | 16 goroutines reserving `[a,b]` and `[b,a]` carts 800 times finish with exact stock and no deadlock |
| A confirm racing a cancel settles once | The reservation row is locked and its status re-checked | 20 rounds of simultaneous confirm and cancel: exactly one wins and stock matches the winner |
| State survives a restart | Stock and reservations are rows; seeding uses `ON CONFLICT DO NOTHING` | A new store over the same database sees and settles the old reservation; re-seeding does not reset stock |
| A cart is all-or-nothing | Validate every line first, then take stock | A cart that fails on its second item holds nothing for its first |
| A retried checkout makes one order | `Idempotency-Key` header; the first response is stored and replayed | A retry returns the same order; 50 simultaneous duplicates produce one order |
| A key can't be abused | Request body is fingerprinted; keys are scoped per user | Same key with a different cart: `422`. Same key from two users: independent |
| Late payment is refused | `Confirm` checks the deadline itself, not just the background cleanup | Confirm after TTL returns `410 Gone` and stock is back |
| A rider is never double-booked | Rider pool behind a mutex | 200 goroutines compete for 100 riders: each rider assigned exactly once |
| No panic on shutdown | `Submit` and `Shutdown` coordinate through an `RWMutex` | 50 rounds of `Submit` racing `Shutdown` |
| A user can't flood checkout | Per-user token bucket; refused requests get `429` with `Retry-After` | 1,000 simultaneous attempts against a burst of 50: exactly 50 pass. A refused request takes no stock |
| A refused request doesn't poison its key | Rate limit is checked before the idempotency lookup, so a `429` is never stored | Retry with the same key after the cool-down succeeds |
| Memory stays bounded | Idempotency entries expire after a TTL; idle rate-limit buckets are swept; in-flight entries are never dropped | Expiry tests with a fake clock |
| No data races | | Whole suite passes under `go test -race`, including with `-count=5` and `-count=10` |

58 tests: 44 run anywhere, and 14 more run against PostgreSQL when `TEST_DATABASE_URL` is set. Results are logged in [docs/TESTING.md](docs/TESTING.md).

## Run it

Requires Go 1.22 or newer (method-and-path routing).

```bash
go run ./cmd/server
```

That keeps stock in memory. To use PostgreSQL instead, start it and point the server at it (the tables are created on startup, and the catalogue is loaded only the first time):

```bash
docker compose up -d
```

```bash
DATABASE_URL=postgres://darkstore:darkstore@localhost:5433/darkstore go run ./cmd/server
```

Configuration is by environment variable:

| Variable | Default | Meaning |
|---|---|---|
| `ADDR` | `:8080` | Listen address |
| `RESERVATION_TTL` | `2m` | How long checkout holds stock |
| `WORKERS` | `4` | Fulfilment goroutines |
| `QUEUE_SIZE` | `256` | Confirmed orders that can wait for a worker |
| `RATE_PER_SEC` | `2` | Sustained checkouts per user per second |
| `RATE_BURST` | `10` | Checkouts a user can make in a burst |
| `IDEMPOTENCY_TTL` | `24h` | How long a checkout response can be replayed |
| `DATABASE_URL` | unset | PostgreSQL connection string. Unset keeps stock in memory |

The store starts with `milk-1l` (50), `bread-whole` (20), `eggs-12` (30) and `banana-6` (40).

## API

| Method and path | Purpose |
|---|---|
| `GET /healthz` | Liveness check |
| `GET /stock` | Available quantity per SKU |
| `POST /checkout` | Reserve stock. Requires an `Idempotency-Key` header |
| `GET /orders/{id}` | Order status and fulfilment stage |
| `POST /orders/{id}/confirm` | Payment succeeded: sell the stock and start fulfilment |
| `POST /orders/{id}/cancel` | Return the held stock |

Status codes: `201` reserved, `409` out of stock or order already confirmed, `410` reservation expired, `422` invalid cart or key reused with a different body, `429` too many checkouts from this user (see `Retry-After`), `404` unknown order, `503` confirmed but the fulfilment queue is unavailable.

```bash
curl -X POST localhost:8080/checkout \
  -H "Idempotency-Key: order-1001" \
  -d '{"user_id":"u1","items":[{"sku":"milk-1l","qty":2}]}'
```

```json
{"order_id":"c6269baadbb04ccc","status":"pending","items":[{"sku":"milk-1l","qty":2}],"expires_at":"2026-10-23T13:41:38+05:30"}
```

Send the same request again and you get the same order back with an `Idempotent-Replayed: true` header, and stock is taken once.

```bash
curl -X POST localhost:8080/orders/c6269baadbb04ccc/confirm
curl localhost:8080/orders/c6269baadbb04ccc
```

## Tests

```bash
go test ./...
```

The PostgreSQL tests are skipped unless you give them a database. Each test creates and drops its own schema, so it is safe to point at the compose database:

```bash
TEST_DATABASE_URL=postgres://darkstore:darkstore@localhost:5433/darkstore go test ./...
```

The race detector needs CGO, which a plain Windows setup lacks, so `scripts/test-race.ps1` runs the suite in a `golang` container (Docker required):

```bash
powershell -ExecutionPolicy Bypass -File scripts/test-race.ps1
```

If `TEST_DATABASE_URL` is set in your shell, the script forwards it into the container (rewriting `localhost` to `host.docker.internal`) so the PostgreSQL tests run under the race detector too.

With the server running in another terminal, `scripts/smoke.ps1` exercises every endpoint end to end.

## Layout

```
cmd/server/            entrypoint: config, wiring, graceful shutdown
internal/inventory/    Inventory interface; in-memory stock, reservations, TTL expiry
internal/pgstore/      PostgreSQL implementation and schema
internal/api/          HTTP handlers, idempotency cache
internal/ratelimit/    per-user token bucket
internal/worker/       generic goroutine pool with backpressure and drain
internal/fulfilment/   order stages, nearest-rider assignment
scripts/               smoke test, race-detector runner
docker-compose.yml     local PostgreSQL
docs/TESTING.md        recorded test results
```

## Design notes

- **One mutex in memory, row locks in PostgreSQL.** In memory a single lock makes a multi-item reservation atomic and keeps the code easy to reason about; unrelated SKUs contend, which only matters at a scale one process would not reach. In PostgreSQL the lock is per row, so carts for different SKUs do not block each other, and carts that share a SKU queue on it.
- **Sorted lock order.** Postgres locks rows as it reads them. Two carts `[a,b]` and `[b,a]` would deadlock if each locked in its own order, so every transaction locks SKUs in alphabetical order.
- **The database is the last line of defense.** `CHECK (available >= 0)` means even a bug in the application cannot oversell; the transaction would fail instead.
- **Stock writes survive a client disconnect.** A reservation, confirm or cancel runs on a context detached from the HTTP request (with a 5 second timeout), so a closing socket cannot abandon it half way. A server error (5xx) is never stored against an idempotency key, so a database hiccup is not replayed to the client for 24 hours.
- **Idempotency stores every outcome, including failures.** A retry of a request that found no stock gets the same `409`, not a surprise success later.
- **Backpressure over unbounded queues.** The worker queue is bounded. When it is full, confirm waits for space or the request times out, rather than memory growing without limit.
- **Shutdown order.** Stop accepting HTTP requests first so nothing new is confirmed, then let workers finish accepted orders. If draining takes longer than 15 seconds, in-flight orders are cancelled through their `context` and riders are returned to the pool.
- **Timeouts use a real deadline, not a timer per reservation.** A single janitor goroutine sweeps expired holds every second, and `Confirm` double-checks the deadline so a late payment never succeeds in the gap.

## Known limitations

This service demonstrates the concurrency and consistency design. Be clear about what it is not:

- **Only stock and reservations are durable.** With `DATABASE_URL` set they survive a restart and several instances can share them. Idempotency keys, the fulfilment tracker and the worker queue are still in memory, so after a restart a retried checkout is treated as new and in-flight order tracking is lost.
- **An unknown commit can orphan a reservation.** If the connection drops after the database committed but before the response arrived, the client retries and gets a second reservation; the first one expires on its TTL. Storing the idempotency key in the same transaction would close this.
- **Confirm-then-enqueue is not atomic.** If payment is confirmed but the worker queue rejects the order, the API returns `503` and the order stays confirmed with nobody fulfilling it. The fix is an outbox: write the order event in the same transaction as the confirm and have workers poll it.
- **No authentication and no real payment step.** `user_id` is taken from the request body, so the rate limit keys on a value the client controls. Behind real auth it would key on the authenticated identity.
- **The rate limiter and idempotency cache are per-process.** With several instances each would need a shared store (Redis) to enforce limits globally.
- **No benchmark numbers yet.** Throughput and latency under load have not been measured.

## Roadmap

- [x] PostgreSQL reservation using `SELECT ... FOR UPDATE`
- [ ] A Redis Lua-script version, benchmarked against the PostgreSQL and in-memory ones
- [x] Per-user token-bucket rate limiting
- [x] Expire old idempotency keys
- [ ] Persist idempotency keys (in the same transaction as the reservation) and orders
- [ ] Outbox pattern so a confirmed order is never lost
- [ ] k6 load test with p95/p99 latency in this README
- [ ] Prometheus metrics
