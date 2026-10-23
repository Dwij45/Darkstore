# Runs the test suite under Go's race detector inside a Linux container.
# On Windows, -race needs CGO and a C compiler; the golang image has both.
#   powershell -ExecutionPolicy Bypass -File scripts/test-race.ps1
#   powershell -ExecutionPolicy Bypass -File scripts/test-race.ps1 -Count 10
param(
    [int]$Count = 1,
    [string]$Image = "golang:1.27"
)

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Docker is not running. Start Docker Desktop, wait for it to say 'Engine running', then re-run." -ForegroundColor Red
    exit 2
}

$root = Split-Path -Parent $PSScriptRoot

# Named volumes cache modules and build output between runs, so only the first run is slow.
docker run --rm `
    -v "${root}:/src" `
    -v darkstore-gomod:/go/pkg/mod `
    -v darkstore-gocache:/root/.cache/go-build `
    -w /src `
    $Image `
    go test -race "-count=$Count" ./...

exit $LASTEXITCODE
