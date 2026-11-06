param([switch]$Lan)
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'initialize-runtime.ps1') -Lan:$Lan
Write-Host 'Dashboard: http://127.0.0.1:8787/ ; Ctrl+C stops this foreground backend.'
& $taskBackendExe
if ($LASTEXITCODE -ne 0) { throw 'Backend exited with an error; review the local console.' }
