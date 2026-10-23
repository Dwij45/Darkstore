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
