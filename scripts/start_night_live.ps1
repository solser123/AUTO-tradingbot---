$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$now = Get-Date
$target = Get-Date -Hour 8 -Minute 0 -Second 0
if ($now -ge $target) {
    $target = $target.AddDays(1)
}
$durationMinutes = [int][Math]::Floor(($target - $now).TotalMinutes)
if ($durationMinutes -lt 1) {
    throw "Calculated duration is less than 1 minute."
}

$dbName = "bot_state_live_$timestamp.db"
$logPath = Join-Path $root "logs\live_night_$timestamp.log"
$errPath = Join-Path $root "logs\live_night_$timestamp.err.log"
$command = 'set BOT_DATABASE_PATH=' + $dbName + ' && "' + $python + '" main.py --duration-minutes ' + $durationMinutes + ' 1> "' + $logPath + '" 2> "' + $errPath + '"'

$process = Start-Process -FilePath "cmd.exe" -ArgumentList "/c", $command -WorkingDirectory $root -WindowStyle Hidden -PassThru

Write-Output ("PID=" + $process.Id)
Write-Output ("DURATION_MINUTES=" + $durationMinutes)
Write-Output ("DB=" + (Join-Path $root $dbName))
Write-Output ("LOG=" + $logPath)
Write-Output ("ERR=" + $errPath)
