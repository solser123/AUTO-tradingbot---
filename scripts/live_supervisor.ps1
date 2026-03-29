$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$logsDir = Join-Path $root "logs"
$restartScript = Join-Path $PSScriptRoot "restart_live_if_needed.ps1"
$statePath = Join-Path $logsDir "live_supervisor_state.json"
$intervalSeconds = 60

if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir | Out-Null
}

$state = [ordered]@{
    pid = $PID
    started_at = (Get-Date).ToString("o")
    interval_seconds = $intervalSeconds
    script = $MyInvocation.MyCommand.Path
}
$state | ConvertTo-Json | Set-Content -Path $statePath -Encoding UTF8

while ($true) {
    Start-Sleep -Seconds $intervalSeconds
    try {
        & $restartScript | Out-Null
        $state = [ordered]@{
            pid = $PID
            started_at = $state.started_at
            last_check_at = (Get-Date).ToString("o")
            interval_seconds = $intervalSeconds
            script = $MyInvocation.MyCommand.Path
        }
        $state | ConvertTo-Json | Set-Content -Path $statePath -Encoding UTF8
    } catch {
    }
}
