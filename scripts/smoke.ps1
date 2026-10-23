# Manual smoke test for a running server. Start the server first:
#   go run ./cmd/server
# then in a second terminal:
#   powershell -ExecutionPolicy Bypass -File scripts/smoke.ps1
param([string]$Base = "http://localhost:8080")

$ErrorActionPreference = "Stop"
$failures = 0

function Call($Method, $Path, $Body = $null, $Key = $null) {
    $headers = @{}
    if ($Key) { $headers["Idempotency-Key"] = $Key }
    try {
        $r = Invoke-WebRequest -UseBasicParsing -Method $Method -Uri "$Base$Path" `
            -Headers $headers -ContentType "application/json" -Body $Body
        return @{ Code = [int]$r.StatusCode; Body = $r.Content.Trim(); Replayed = ($r.Headers["Idempotent-Replayed"] -join "") }
    } catch {
        $resp = $_.Exception.Response
        if (-not $resp) { throw }
        return @{ Code = [int]$resp.StatusCode; Body = "$($_.ErrorDetails.Message)".Trim(); Replayed = "" }
    }
}

function Check($Name, $Cond, $Detail) {
    if ($Cond) { Write-Host "PASS  $Name" -ForegroundColor Green }
    else { Write-Host "FAIL  $Name -> $Detail" -ForegroundColor Red; $script:failures++ }
}

$key = "smoke-" + [guid]::NewGuid().ToString("N").Substring(0, 8)
$cart = '{"user_id":"smoke","items":[{"sku":"milk-1l","qty":2}]}'

$h = Call GET "/healthz"
Check "health is 200" ($h.Code -eq 200) $h.Body

$before = (Call GET "/stock").Body | ConvertFrom-Json
Write-Host "      milk-1l before: $($before.'milk-1l')"

$c1 = Call POST "/checkout" $cart $key
Check "checkout is 201" ($c1.Code -eq 201) "$($c1.Code) $($c1.Body)"
$order = $c1.Body | ConvertFrom-Json

$c2 = Call POST "/checkout" $cart $key
Check "retry with same key returns same order" ((($c2.Body | ConvertFrom-Json).order_id) -eq $order.order_id) $c2.Body
Check "retry is marked Idempotent-Replayed" ($c2.Replayed -eq "true") "header was '$($c2.Replayed)'"

$after = (Call GET "/stock").Body | ConvertFrom-Json
Check "stock dropped by 2, not 4" (($before.'milk-1l' - $after.'milk-1l') -eq 2) "before $($before.'milk-1l'), after $($after.'milk-1l')"

$reuse = Call POST "/checkout" '{"user_id":"smoke","items":[{"sku":"milk-1l","qty":5}]}' $key
Check "same key, different body is 422" ($reuse.Code -eq 422) "$($reuse.Code) $($reuse.Body)"

$nokey = Call POST "/checkout" $cart
Check "missing Idempotency-Key is 400" ($nokey.Code -eq 400) "$($nokey.Code)"

$big = Call POST "/checkout" '{"user_id":"smoke","items":[{"sku":"bread-whole","qty":9999}]}' ($key + "-big")
Check "too much bread is 409" ($big.Code -eq 409) "$($big.Code) $($big.Body)"

$conf = Call POST "/orders/$($order.order_id)/confirm"
Check "confirm is 200 / confirmed" (($conf.Code -eq 200) -and (($conf.Body | ConvertFrom-Json).status -eq "confirmed")) "$($conf.Code) $($conf.Body)"

$again = Call POST "/orders/$($order.order_id)/confirm"
Check "second confirm is 409" ($again.Code -eq 409) "$($again.Code)"

$c3 = Call POST "/checkout" $cart ($key + "-cancel")
$id3 = ($c3.Body | ConvertFrom-Json).order_id
$mid = ((Call GET "/stock").Body | ConvertFrom-Json).'milk-1l'
$can = Call POST "/orders/$id3/cancel"
$end = ((Call GET "/stock").Body | ConvertFrom-Json).'milk-1l'
Check "cancel returns stock" (($can.Code -eq 200) -and ($end -eq $mid + 2)) "$($can.Code), stock $mid -> $end"

$nf = Call GET "/orders/does-not-exist"
Check "unknown order is 404" ($nf.Code -eq 404) "$($nf.Code)"

Write-Host ""
if ($failures -eq 0) { Write-Host "All checks passed." -ForegroundColor Green; exit 0 }
Write-Host "$failures check(s) failed." -ForegroundColor Red; exit 1
