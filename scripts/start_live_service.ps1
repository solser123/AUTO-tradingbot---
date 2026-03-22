$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$python = Join-Path $root ".venv\Scripts\python.exe"
$mainPath = Join-Path $root "main.py"
$logDir = Join-Path $root "logs"
$logPath = Join-Path $logDir "live_service_$timestamp.log"
$errPath = Join-Path $logDir "live_service_$timestamp.err.log"
$statePath = Join-Path $logDir "live_service_state.json"

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

$existing = $null
if (Test-Path $statePath) {
    try {
        $existing = Get-Content $statePath -Raw | ConvertFrom-Json
    } catch {
        $existing = $null
    }
}

if ($existing -and $existing.pid) {
    $running = Get-Process -Id ([int]$existing.pid) -ErrorAction SilentlyContinue
    if ($running) {
        Write-Output "PID=$($existing.pid)"
        Write-Output "LOG=$($existing.log)"
        Write-Output "ERR=$($existing.err)"
        Write-Output "STATUS=already_running"
        exit 0
    }
}

$stale = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq "python.exe" -and $_.CommandLine -like "*$mainPath*"
}
foreach ($process in $stale) {
    try {
        Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
    } catch {
    }
}

$process = Start-Process `
    -FilePath $python `
    -ArgumentList "`"$mainPath`"" `
    -WorkingDirectory $root `
    -WindowStyle Hidden `
    -RedirectStandardOutput $logPath `
    -RedirectStandardError $errPath `
    -PassThru

$state = [ordered]@{
    pid = $process.Id
    started_at = (Get-Date).ToString("o")
    root = $root
    log = $logPath
    err = $errPath
}
$state | ConvertTo-Json | Set-Content -Path $statePath -Encoding UTF8

Write-Output "PID=$($process.Id)"
Write-Output "LOG=$logPath"
Write-Output "ERR=$errPath"
Write-Output "STATE=$statePath"
