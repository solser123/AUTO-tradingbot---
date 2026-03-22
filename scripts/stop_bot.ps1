$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$mainPath = Join-Path $root "main.py"
$statePath = Join-Path (Join-Path $root "logs") "live_service_state.json"
$stopped = $false

if (Test-Path $statePath) {
    try {
        $state = Get-Content $statePath -Raw | ConvertFrom-Json
        if ($state.pid) {
            Stop-Process -Id ([int]$state.pid) -Force -ErrorAction Stop
            Write-Output ("STOPPED=" + $state.pid)
            $stopped = $true
        }
    } catch {
    }
    Remove-Item $statePath -Force -ErrorAction SilentlyContinue
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
