param(
  [int]$Port = 8766,
  [ValidateSet("claude", "codex", "ollama")]
  [string]$Backend = "claude",
  [string]$OllamaModel = "llama3.2",
  [string]$TaskName = "ClaudeBridge"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$LogPath = Join-Path $Root "bridge_startup.log"
$Py = Join-Path $Root "venv\Scripts\python.exe"
$Bridge = Join-Path $Root "bridge_v2.py"

if (-not (Test-Path $Py)) {
  throw "venv Python not found: $Py . Run install_windows.ps1 first."
}
if (-not (Test-Path $Bridge)) {
  throw "bridge_v2.py not found: $Bridge"
}

$arg = "`"$Bridge`" --port $Port --backend $Backend"
if ($Backend -eq "ollama") {
  $arg += " --model $OllamaModel"
}

$action = New-ScheduledTaskAction -Execute $Py -Argument $arg -WorkingDirectory $Root
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 0)

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Description "Auto-start claude-bridge on Windows logon" -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName

Write-Host "Scheduled task created: $TaskName"
Write-Host "Python: $Py"
Write-Host "Args: $arg"
Write-Host "Task started. Check runtime log at: $LogPath"
