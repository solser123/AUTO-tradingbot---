$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
$statePath = Join-Path (Join-Path $root "logs") "live_service_state.json"
$dbPath = Join-Path $root "bot_state.db"
$heartbeatMaxAgeSeconds = 180
$logFreshSeconds = 300

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
    service_heartbeat_at = ""
    heartbeat_age_seconds = $null
    log_path = ""
    log_age_seconds = $null
    healthy = $false
}

if (Test-Path $statePath) {
    try {
        $state = Get-Content $statePath -Raw | ConvertFrom-Json
        $result.wrapper_pid = $state.wrapper_pid
        $result.runtime_pid = $state.runtime_pid
        $result.log_path = $state.err
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
import json
from binance_bot.storage import StateStore
store = StateStore(r"$dbPath")
print(json.dumps({
    "emergency_stop": store.get_state("emergency_stop") or "",
    "emergency_reason": store.get_state("emergency_reason") or "",
    "service_pid": store.get_state("service_pid") or "",
    "service_started_at": store.get_state("service_started_at") or "",
    "service_heartbeat_at": store.get_state("service_heartbeat_at") or "",
}))
"@ | & $python -
    $raw = ($payload | Out-String).Trim()
    if ($raw) {
        $parsed = $raw | ConvertFrom-Json
        $result.emergency_stop = $parsed.emergency_stop
        $result.emergency_reason = $parsed.emergency_reason
        $result.service_pid_db = $parsed.service_pid
        $result.service_started_at = $parsed.service_started_at
        $result.service_heartbeat_at = $parsed.service_heartbeat_at
        if ($result.service_pid_db) {
            $result.runtime_pid = $result.service_pid_db
            $result.runtime_alive = [bool](Get-Process -Id ([int]$result.service_pid_db) -ErrorAction SilentlyContinue)
        }
    }
} catch {
}

if ($result.service_heartbeat_at) {
    try {
        $heartbeatAt = [datetimeoffset]::Parse($result.service_heartbeat_at)
        $result.heartbeat_age_seconds = [math]::Round(((Get-Date).ToUniversalTime() - $heartbeatAt.UtcDateTime).TotalSeconds, 0)
    } catch {
    }
}

if ($result.log_path -and (Test-Path $result.log_path)) {
    $logItem = Get-Item $result.log_path
    $result.log_age_seconds = [math]::Round(((Get-Date).ToUniversalTime() - $logItem.LastWriteTimeUtc).TotalSeconds, 0)
}

$heartbeatHealthy = ($null -ne $result.heartbeat_age_seconds -and $result.heartbeat_age_seconds -le $heartbeatMaxAgeSeconds)
$logHealthy = ($null -ne $result.log_age_seconds -and $result.log_age_seconds -le $logFreshSeconds)
$result.healthy = (($result.runtime_alive -or $result.wrapper_alive) -and ($result.emergency_stop -ne "1") -and ($heartbeatHealthy -or $logHealthy))
$result | ConvertTo-Json -Depth 3
