$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$mainPath = Join-Path $root "main.py"
$python = Join-Path $root ".venv\Scripts\python.exe"
$dbPath = Join-Path $root "bot_state.db"
$statePath = Join-Path (Join-Path $root "logs") "live_service_state.json"
$supervisorStatePath = Join-Path (Join-Path $root "logs") "live_supervisor_state.json"
$stopped = $false

if (Test-Path $supervisorStatePath) {
    try {
        $supervisorState = Get-Content $supervisorStatePath -Raw | ConvertFrom-Json
        if ($supervisorState.pid) {
            Stop-Process -Id ([int]$supervisorState.pid) -Force -ErrorAction SilentlyContinue
            Write-Output ("STOPPED_SUPERVISOR=" + $supervisorState.pid)
            $stopped = $true
        }
    } catch {
    }
    Remove-Item $supervisorStatePath -Force -ErrorAction SilentlyContinue
}

if (Test-Path $statePath) {
    try {
        $state = Get-Content $statePath -Raw | ConvertFrom-Json
        foreach ($pidValue in @($state.runtime_pid, $state.wrapper_pid, $state.pid)) {
            if ($pidValue) {
                Stop-Process -Id ([int]$pidValue) -Force -ErrorAction SilentlyContinue
                Write-Output ("STOPPED=" + $pidValue)
                $stopped = $true
            }
        }
    } catch {
    }
    Remove-Item $statePath -Force -ErrorAction SilentlyContinue
}

try {
    $runtimePid = @"
from binance_bot.storage import StateStore
store = StateStore(r"$dbPath")
print(store.get_state("service_pid") or "")
"@ | & $python -
    $runtimePid = ($runtimePid | Out-String).Trim()
    if ($runtimePid) {
        Stop-Process -Id ([int]$runtimePid) -Force -ErrorAction SilentlyContinue
        Write-Output ("STOPPED=" + $runtimePid)
        $stopped = $true
    }
} catch {
}

try {
    @"
from datetime import datetime, timezone
from binance_bot.storage import StateStore
store = StateStore(r"$dbPath")
store.set_state("service_pid", "")
store.set_state("service_started_at", "")
store.set_state("service_heartbeat_at", "")
store.set_state("service_stopped_at", datetime.now(timezone.utc).isoformat())
"@ | & $python - | Out-Null
} catch {
}

$processes = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq "python.exe" -and $_.CommandLine -like "*$mainPath*"
}

foreach ($process in $processes) {
    try {
        Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
        Write-Output ("STOPPED=" + $process.ProcessId)
        $stopped = $true
    } catch {
        Write-Output ("FAILED=" + $process.ProcessId)
    }
}

if (-not $stopped) {
    Write-Output "STOPPED=none"
}
