$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$processes = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -in @("python.exe", "cmd.exe") -and $_.CommandLine -like "*main.py*"
}

foreach ($process in $processes) {
    try {
        Stop-Process -Id $process.ProcessId -Force -ErrorAction Stop
        Write-Output ("STOPPED=" + $process.ProcessId)
    } catch {
        Write-Output ("FAILED=" + $process.ProcessId)
    }
}

if (-not $processes) {
    Write-Output "STOPPED=none"
}
