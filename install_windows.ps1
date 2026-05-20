param(
  [int]$Port = 8766,
  [ValidateSet("claude", "codex", "ollama")]
  [string]$Backend = "claude",
  [string]$OllamaModel = "llama3.2",
  [switch]$SkipCliInstall
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

Write-Host "==> claude-bridge one-click install (Windows)"

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
  throw "Python launcher 'py' not found. Install Python 3.10+ first."
}
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  throw "Node.js not found. Install Node.js LTS first."
}
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
  throw "npm not found. Reinstall Node.js."
}

Write-Host "==> Creating virtualenv (if needed)"
if (-not (Test-Path ".\venv\Scripts\python.exe")) {
  py -3 -m venv venv
}

Write-Host "==> Installing Python dependencies"
.\venv\Scripts\python -m pip install --upgrade pip
.\venv\Scripts\python -m pip install -r requirements.txt

Write-Host "==> Preparing local session folders"
New-Item -ItemType Directory -Force -Path "$HOME\.claude\projects" | Out-Null
New-Item -ItemType Directory -Force -Path "$HOME\.codex\sessions" | Out-Null

if (-not $SkipCliInstall) {
  if ($Backend -eq "claude") {
    if (-not (Get-Command claude -ErrorAction SilentlyContinue)) {
      Write-Host "==> Installing Claude CLI"
      npm install -g @anthropic-ai/claude-code
    } else {
      Write-Host "Claude CLI already installed"
    }
  } elseif ($Backend -eq "codex") {
    if (-not (Get-Command codex -ErrorAction SilentlyContinue)) {
      Write-Host "==> Installing Codex CLI"
      npm install -g @openai/codex
    } else {
      Write-Host "Codex CLI already installed"
    }
  }
}

$Cmd = ".\venv\Scripts\python bridge_v2.py --port $Port --backend $Backend"
if ($Backend -eq "ollama") {
  $Cmd += " --model $OllamaModel"
}

Write-Host ""
Write-Host "Install complete."
Write-Host "Start command:"
Write-Host "  $Cmd"
Write-Host ""
Write-Host "To start now, run:"
Invoke-Expression $Cmd
