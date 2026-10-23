# Test log

Results recorded as each step is finished. Environment: Windows 11, Go 1.27.0 (windows/amd64).

## Step 1 — Inventory reservation core (`internal/inventory`)

Date: 2026-10-23

### Run 1: full suite, verbose

```
go test -v ./internal/inventory
```

| Test | What it proves | Result |
|---|---|---|
| `TestReserveNeverOversells` | 1,000 goroutines race for 50 units; exactly 50 succeed, 950 get `ErrOutOfStock`, stock ends at 0 | PASS (0.01s) |
| `TestReserveIsAllOrNothing` | A multi-item cart that fails on one SKU holds no stock for the others | PASS |
| `TestReserveMergesDuplicateLines` | Two lines for the same SKU are summed before the stock check (2 + 2 > 3 is rejected) | PASS |
| `TestReserveRejectsBadInput/empty` | Empty cart is rejected | PASS |
| `TestReserveRejectsBadInput/zero_qty` | Quantity ≤ 0 is rejected | PASS |
| `TestReserveRejectsBadInput/unknown_sku` | Unknown SKU is rejected | PASS |
| `TestExpiredReservationReturnsStock` | After the TTL, the janitor releases the hold and confirm fails with `ErrExpired` | PASS |
| `TestConfirmAfterDeadlineExpiresImmediately` | A late payment is refused even if the janitor has not run yet | PASS |
| `TestReleaseReturnsStockOnce` | Cancelling twice does not return stock twice | PASS |
| `TestConfirmedStockStaysSold` | A confirmed order cannot be released back into stock | PASS |

```
PASS
ok      darkstore/internal/inventory    0.923s
```

### Run 2: concurrency test repeated 20 times

Checks that the no-oversell result is stable across goroutine schedules, not a one-off pass.

```
go test -count=20 -run TestReserveNeverOversells ./internal/inventory
```

```
ok      darkstore/internal/inventory    0.812s
```

20 of 20 runs passed.

### Not yet covered

- The race detector (`go test -race`) has not been run. On Windows it needs CGO and a C compiler, so it will run in Docker in step 4.

## Step 2 — HTTP API with idempotency (`internal/api`, `cmd/server`)

Date: 2026-10-23

### Run 1: unit tests through the HTTP handler

```
go test -v ./internal/api
```

| Test | What it proves | Result |
|---|---|---|
| `TestCheckoutRequiresIdempotencyKey` | Checkout without an `Idempotency-Key` header is 400 | PASS |
| `TestCheckoutValidatesBody` | Bad JSON, unknown fields, missing user are 400; unknown SKU and zero qty are 422 | PASS |
| `TestRetriedCheckoutCreatesOneOrder` | A retry (even with different whitespace and field order) returns the same order, sets `Idempotent-Replayed: true`, and takes stock once | PASS |
| `TestConcurrentDuplicateCheckoutsCreateOneOrder` | 50 copies of one request at the same moment produce one order | PASS |
| `TestKeyReusedWithDifferentBodyIsRejected` | Same key with a different cart is 422 | PASS |
| `TestSameKeyFromDifferentUsersIsIndependent` | Keys are scoped per user | PASS |
| `TestConcurrentCheckoutsOverHTTPNeverOversell` | 1,000 concurrent HTTP checkouts against 50 units: exactly 50 × 201 and 950 × 409 | PASS |
| `TestConfirmDispatchesOrder` | Confirm is 200, hands the order to the dispatcher, and a second confirm is 409 | PASS |
| `TestConfirmReportsUnavailableQueue` | Confirm returns 503 if the fulfilment queue rejects the order | PASS |
| `TestConfirmAfterExpiryIsGone` | Confirm after the TTL is 410 and the stock is returned | PASS |
| `TestCancelReturnsStock` | Cancel is 200 / released and returns the stock | PASS |
| `TestGetUnknownOrderIsNotFound` | Unknown order is 404 | PASS |

### Run 2: live server smoke test

```
go run ./cmd/server
powershell -ExecutionPolicy Bypass -File scripts/smoke.ps1
```

All 12 checks passed: health, checkout 201, retry returns the same order with the replay header, stock dropped by 2 not 4, key reuse 422, missing key 400, oversized cart 409, confirm 200, second confirm 409, cancel returns stock, unknown order 404.

### Known gap

- If payment is confirmed but the fulfilment queue is unavailable, the API returns 503 and the order stays confirmed with nobody working on it. The fix is an outbox (store the order event in the same transaction as the confirm, and have workers poll it).

## Step 3 — Worker pool, fulfilment and graceful shutdown (`internal/worker`, `internal/fulfilment`)

Date: 2026-10-23

### Run 1: unit tests

```
go test -v ./internal/worker ./internal/fulfilment
```

| Test | What it proves | Result |
|---|---|---|
| `TestProcessesEveryJob` | 100 jobs over 4 workers are each handled exactly once (sum 1..100 = 5050) | PASS |
| `TestShutdownDrainsQueuedJobs` | Shutdown waits for queued jobs instead of dropping them | PASS |
| `TestSubmitAfterShutdownIsRejected` | Submitting after shutdown returns `ErrClosed` | PASS |
| `TestSubmitBlocksWhenQueueIsFull` | A full queue pushes back on the caller until its context times out | PASS |
| `TestShutdownTimeoutThenCancelStopsWorkers` | If draining times out, cancelling the worker context stops in-flight jobs | PASS |
| `TestConcurrentSubmitAndShutdown` | 50 rounds of Submit racing Shutdown never panic with "send on closed channel" | PASS |
| `TestAssignNearestPicksClosestFreeRider` | Riders are handed out nearest first | PASS |
| `TestConcurrentAssignmentNeverDoubleBooks` | 200 goroutines competing for 100 riders: every rider assigned exactly once | PASS |
| `TestProcessDeliversAndFreesRider` | An order reaches `delivered` and its rider returns to the pool | PASS |
| `TestProcessWaitsForARider` | With no free rider the order waits in `awaiting_rider`, then completes when one frees up | PASS |
| `TestProcessStopsOnCancelAndReturnsRider` | Cancelling mid-delivery marks the order cancelled and still releases the rider | PASS |

API tests were updated: confirm now reports `fulfilment.stage = queued`, and a failed hand-off leaves no stale stage.

### Run 2: live order, end to end

Smoke script: all 12 checks passed. One order was then followed after confirm:

```
t=0s   queued
t~1s   picking
t~3s   out_for_delivery, rider-2
t~16s  delivered, rider-2
```

`rider-2` was chosen because the nearer `rider-1` was still out with the smoke test's order.

### Run 3: graceful shutdown (manual)

Start the server, confirm an order, and press Ctrl+C while it is `out_for_delivery`. Expected: the log shows `shutting down`, then `order finished ... stage=delivered`, then `stopped`, so the in-flight order completes before exit.

## Step 4 — Race detector (whole suite, in Docker)

Date: 2026-10-23

`go test -race` needs CGO and a C compiler, which this Windows machine lacks, so the suite runs in a `golang:1.27` Linux container via `scripts/test-race.ps1`.

```
powershell -ExecutionPolicy Bypass -File scripts/test-race.ps1
powershell -ExecutionPolicy Bypass -File scripts/test-race.ps1 -Count 10
```

| Run | Result |
|---|---|
| `go test -race ./...` | PASS in all 4 packages, no data races reported |
| `go test -race -count=10 ./...` | PASS in all 4 packages (every test, including the 1,000-vs-50 checkout tests, run 10 times) |

The first run took ~7 minutes (image download); later runs take under 30 seconds because modules and build output are cached in Docker volumes.
