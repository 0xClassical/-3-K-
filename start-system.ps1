param([switch]$Lan)
$ErrorActionPreference = 'Stop'
$taskSystemRoot = $PSScriptRoot
# No takeover of an already running backend, including another local project.
$taskSystemPort = New-Object Net.Sockets.TcpClient
try {
    $taskSystemProbe = $taskSystemPort.BeginConnect('127.0.0.1',8787,$null,$null)
    $taskSystemBusy = $taskSystemProbe.AsyncWaitHandle.WaitOne(500) -and $taskSystemPort.Connected
} finally { $taskSystemPort.Dispose() }
if ($taskSystemBusy) { throw 'Port 8787 is occupied. Use the existing dashboard or stop that instance yourself; nothing was stopped.' }
. (Join-Path $taskSystemRoot 'initialize-runtime.ps1') -Lan:$Lan
$taskSystemData = Join-Path $taskSystemRoot 'data'
if (-not (Test-Path -LiteralPath $taskSystemData)) { New-Item -ItemType Directory -Path $taskSystemData | Out-Null }
$taskSystemLog = Join-Path $taskSystemData ('launcher-'+[DateTime]::Now.ToString('yyyyMMdd-HHmmss'))
$taskSystemBackend = Start-Process -FilePath $taskBackendExe -WorkingDirectory $taskSystemRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput ($taskSystemLog+'.stdout.log') -RedirectStandardError ($taskSystemLog+'.stderr.log')
$taskSystemReady = $false
for ($taskSystemAttempt=0; $taskSystemAttempt -lt 40; $taskSystemAttempt++) {
    if ($taskSystemBackend.HasExited) { throw 'Backend exited; review data\launcher-*.stderr.log.' }
    try { $taskSystemHealth = Invoke-RestMethod 'http://127.0.0.1:8787/api/health' -TimeoutSec 1; $taskSystemReady = $taskSystemHealth.liveBuild -eq $false } catch { $taskSystemReady = $false }
    if ($taskSystemReady) { break }
    Start-Sleep -Milliseconds 500
}
if (-not $taskSystemReady) { throw 'Backend startup not confirmed; no browser was opened.' }
$taskSystemEdgeCandidates = @(
    (Join-Path ${env:ProgramFiles(x86)} 'Microsoft\Edge\Application\msedge.exe'),
    (Join-Path $env:ProgramFiles 'Microsoft\Edge\Application\msedge.exe')
)
$taskSystemEdge = $taskSystemEdgeCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if (-not $taskSystemEdge) { Write-Host 'Backend is running. Install/load the extension in your Chromium browser manually.'; return }
$taskSystemProfile = Join-Path $taskSystemData 'edge-profile'
# Dedicated profile only; never copy a personal browser profile into this directory.
Start-Process -FilePath $taskSystemEdge -WindowStyle Hidden -ArgumentList @(
    ('--user-data-dir="'+$taskSystemProfile+'"'), '--disable-background-mode', '--disable-session-crashed-bubble',
    '--new-window', 'http://127.0.0.1:8787/'
) | Out-Null
Write-Host 'Backend + dedicated Edge started. First time: load browser-extension from edge://extensions/ and click Create/repair four windows.'
Write-Host 'Buying remains disabled; no wallet was loaded. Other browser profiles are not modified.'
