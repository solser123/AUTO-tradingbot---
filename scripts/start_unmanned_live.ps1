$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$logsDir = Join-Path $root "logs"
$supervisorScript = Join-Path $PSScriptRoot "live_supervisor.ps1"
$restartScript = Join-Path $PSScriptRoot "restart_live_if_needed.ps1"
$statePath = Join-Path $logsDir "live_supervisor_state.json"

function Start-AsyncRestart {
    param(
        [string]$ScriptPath,
        [string]$WorkingDirectory
    )
    Start-Process `
        -FilePath "powershell.exe" `
        -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`"" `
        -WorkingDirectory $WorkingDirectory `
        -WindowStyle Hidden | Out-Null
}

if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir | Out-Null
}

if (Test-Path $statePath) {
    try {
        $existing = Get-Content $statePath -Raw | ConvertFrom-Json
        if ($existing.pid) {
            $running = Get-Process -Id ([int]$existing.pid) -ErrorAction SilentlyContinue
            if ($running) {
                Start-AsyncRestart -ScriptPath $restartScript -WorkingDirectory $root
                Write-Output "SUPERVISOR_PID=$($existing.pid)"
                Write-Output "STATUS=already_running"
                exit 0
            }
        }
    } catch {
    }
}

$proc = Start-Process `
    -FilePath "powershell.exe" `
    -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$supervisorScript`"" `
    -WorkingDirectory $root `
    -WindowStyle Hidden `
    -PassThru

Start-AsyncRestart -ScriptPath $restartScript -WorkingDirectory $root

Write-Output "SUPERVISOR_PID=$($proc.Id)"
Write-Output "STATUS=started"
