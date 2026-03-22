$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$healthScript = Join-Path $PSScriptRoot "healthcheck_live.ps1"
$startScript = Join-Path $PSScriptRoot "start_live_service.ps1"

$health = & $healthScript | ConvertFrom-Json

if ($health.healthy) {
    Write-Output "STATUS=healthy"
    exit 0
}

if ($health.emergency_stop -eq "1") {
    Write-Output "STATUS=blocked_by_emergency"
    Write-Output ("REASON=" + $health.emergency_reason)
    exit 0
}

Write-Output "STATUS=restarting"
& $startScript
