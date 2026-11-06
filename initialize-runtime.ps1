param([switch]$Lan)
$ErrorActionPreference = 'Stop'
$taskRuntimeRoot = $PSScriptRoot
Set-Location -LiteralPath $taskRuntimeRoot
$env:MEME_QUANT_HOME = $taskRuntimeRoot
$env:MEME_QUANT_TARGETS = Join-Path $taskRuntimeRoot 'config\targets.local.json'
$env:MEME_QUANT_DB = Join-Path $taskRuntimeRoot 'data\meme-quant.sqlite3'
$env:TRADING_CONFIG_FILE = Join-Path $taskRuntimeRoot 'trading-config.json'
$env:SEED_BLOCKS = '0'
$env:MEME_QUANT_LISTEN_ADDR = '127.0.0.1:8787'
if (-not (Test-Path -LiteralPath $env:MEME_QUANT_TARGETS -PathType Leaf)) {
    throw 'Copy config\targets.example.json to config\targets.local.json and enter your own monitored addresses first.'
}
# Never enable/compile live trading or import a key from inherited environment.
Remove-Item Env:WALLET_KEY_FILE -ErrorAction SilentlyContinue
$taskRuntimeTokenPath = Join-Path $taskRuntimeRoot 'config\dashboard-control-token.local.txt'
if ($Lan) {
    if (-not (Test-Path -LiteralPath $taskRuntimeTokenPath)) {
        $taskRuntimeBytes = New-Object byte[] 32
        $taskRuntimeRng = [Security.Cryptography.RandomNumberGenerator]::Create()
        try { $taskRuntimeRng.GetBytes($taskRuntimeBytes) } finally { $taskRuntimeRng.Dispose() }
        # New file only; secret is never printed or sent as a command argument.
        $taskRuntimeStream = New-Object IO.FileStream($taskRuntimeTokenPath,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
        try {
            $taskRuntimeEncoded = [Text.Encoding]::UTF8.GetBytes([Convert]::ToBase64String($taskRuntimeBytes))
            $taskRuntimeStream.Write($taskRuntimeEncoded,0,$taskRuntimeEncoded.Length)
        } finally { $taskRuntimeStream.Dispose() }
    }
    $taskRuntimeSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    & icacls.exe $taskRuntimeTokenPath /inheritance:r /grant:r "*${taskRuntimeSid}:(F)" '*S-1-5-18:(F)' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Cannot restrict token-file permissions. LAN start cancelled.' }
    $env:DASHBOARD_CONTROL_TOKEN = [IO.File]::ReadAllText($taskRuntimeTokenPath).Trim()
    if ($env:DASHBOARD_CONTROL_TOKEN.Length -lt 24) { throw 'Invalid local control token.' }
    $env:MEME_QUANT_LISTEN_ADDR = '0.0.0.0:8787'
    Write-Host 'LAN mode: trusted network only. Control token is in config\dashboard-control-token.local.txt (not printed).'
} else { Remove-Item Env:DASHBOARD_CONTROL_TOKEN -ErrorAction SilentlyContinue }

if ($env:CARGO_TARGET_DIR) { $taskRuntimeBuildRoot = [IO.Path]::GetFullPath($env:CARGO_TARGET_DIR) }
else { $taskRuntimeBuildRoot = Join-Path $taskRuntimeRoot 'target' }
$taskBackendExe = Join-Path $taskRuntimeBuildRoot 'release\meme-quant.exe'
# Cargo is always invoked with its default feature set, never --features live-trading.
& cargo build --release --locked
if ($LASTEXITCODE -ne 0) { throw 'Default monitoring build failed.' }
& $taskBackendExe --check-config
if ($LASTEXITCODE -ne 0) { throw 'Local configuration is invalid. No backend started.' }
