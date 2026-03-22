$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
$statePath = Join-Path (Join-Path $root "logs") "live_service_state.json"
$dbPath = Join-Path $root "bot_state.db"

$result = [ordered]@{
    state_file_exists = Test-Path $statePath
    wrapper_pid = $null
    runtime_pid = $null
    wrapper_alive = $false
    runtime_alive = $false
    emergency_stop = $null
    emergency_reason = ""
    service_pid_db = ""
    service_started_at = ""
    healthy = $false
}

if (Test-Path $statePath) {
    try {
        $state = Get-Content $statePath -Raw | ConvertFrom-Json
        $result.wrapper_pid = $state.wrapper_pid
        $result.runtime_pid = $state.runtime_pid
        if ($state.wrapper_pid) {
            $result.wrapper_alive = [bool](Get-Process -Id ([int]$state.wrapper_pid) -ErrorAction SilentlyContinue)
        }
        if ($state.runtime_pid) {
            $result.runtime_alive = [bool](Get-Process -Id ([int]$state.runtime_pid) -ErrorAction SilentlyContinue)
        }
    } catch {
    }
}

try {
    $payload = @"
from binance_bot.storage import StateStore
store = StateStore(r"$dbPath")
print(store.get_state("emergency_stop") or "")
print(store.get_state("emergency_reason") or "")
print(store.get_state("service_pid") or "")
print(store.get_state("service_started_at") or "")
"@ | & $python -
    $lines = ($payload | Out-String).Trim().Split([Environment]::NewLine)
    if ($lines.Count -ge 4) {
        $result.emergency_stop = $lines[0]
        $result.emergency_reason = $lines[1]
        $result.service_pid_db = $lines[2]
        $result.service_started_at = $lines[3]
        if ($result.service_pid_db) {
            $result.runtime_pid = $result.service_pid_db
            $result.runtime_alive = [bool](Get-Process -Id ([int]$result.service_pid_db) -ErrorAction SilentlyContinue)
        }
    }
} catch {
}

$result.healthy = ($result.runtime_alive -and ($result.emergency_stop -ne "1"))
$result | ConvertTo-Json -Depth 3
