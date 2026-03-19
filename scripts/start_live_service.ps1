$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$python = Join-Path $root ".venv\Scripts\python.exe"
$logDir = Join-Path $root "logs"
$logPath = Join-Path $logDir "live_service_$timestamp.log"
$errPath = Join-Path $logDir "live_service_$timestamp.err.log"

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

$process = Start-Process `
    -FilePath $python `
    -ArgumentList "main.py" `
    -WorkingDirectory $root `
    -WindowStyle Hidden `
    -RedirectStandardOutput $logPath `
    -RedirectStandardError $errPath `
    -PassThru

Write-Output "PID=$($process.Id)"
Write-Output "LOG=$logPath"
Write-Output "ERR=$errPath"
