param([switch]$Compact, [switch]$UseSnapshot)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot '.competition_venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $pythonExe)) { throw 'Run competition\setup.ps1 first.' }
$configName = if ($Compact) { 'config_compact.json' } else { 'config.json' }
$configPath = Join-Path $PSScriptRoot $configName
$workFolder = if ($Compact) { 'work_compact_vi_en_zh' } else { 'work_vi_en_zh' }
$pidPath = Join-Path $PSScriptRoot "$workFolder\pipeline.pid"
New-Item -ItemType Directory -Path (Split-Path -Parent $pidPath) -Force | Out-Null
$launchLock = [IO.File]::Open((Join-Path $PSScriptRoot '.start_system.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
try {
foreach ($candidateWork in @('work','work_compact','work_vi_en_zh','work_compact_vi_en_zh')) {
    $supervisorPidPath = Join-Path $PSScriptRoot "$candidateWork\system_supervisor.pid"
    if (Test-Path -LiteralPath $supervisorPidPath) {
        $supervisorId = [int](Get-Content -LiteralPath $supervisorPidPath -Raw)
        $supervisor = Get-CimInstance Win32_Process -Filter "ProcessId=$supervisorId" -ErrorAction SilentlyContinue
        if ($supervisor -and $supervisor.CommandLine -match 'supervise_system\.ps1') {
            Write-Output "System supervisor already running: PID $supervisorId ($candidateWork)."
            return
        }
    }
    $otherPidPath = Join-Path $PSScriptRoot "$candidateWork\pipeline.pid"
    if (Test-Path -LiteralPath $otherPidPath) {
        $otherId = [int](Get-Content -LiteralPath $otherPidPath -Raw)
        $otherProcess = Get-CimInstance Win32_Process -Filter "ProcessId=$otherId" -ErrorAction SilentlyContinue
        if ($otherProcess -and $otherProcess.CommandLine -match 'competition.pipeline') {
            Write-Output "An existing system pipeline is waiting/running: PID $otherId ($candidateWork)."
            return
        }
    }
}
if (Test-Path -LiteralPath $pidPath) {
    $existingId = [int](Get-Content -LiteralPath $pidPath -Raw)
    $existing = Get-CimInstance Win32_Process -Filter "ProcessId=$existingId" -ErrorAction SilentlyContinue
    if ($existing -and $existing.CommandLine -match 'competition.pipeline') {
        Write-Output "Pipeline already waiting/running: PID $existingId"
        return
    }
}
@{ stop=$false } | ConvertTo-Json -Compress | Set-Content -LiteralPath (Join-Path $PSScriptRoot "$workFolder\pipeline_control.json") -Encoding utf8
$supervisorScript = Join-Path $PSScriptRoot 'supervise_system.ps1'
$arguments = @('-NoProfile','-ExecutionPolicy','Bypass','-File',"`"$supervisorScript`"",'-ConfigPath',"`"$configPath`"",'-WorkFolder',$workFolder)
if ($UseSnapshot) { $arguments += '-UseSnapshot' }
# WMI starts the supervisor independently of this terminal's process/job tree.
# It uses the current user's token, hides the console, and adds no scheduled task.
$powershellExe = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
$startup = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ ShowWindow=[uint16]0 }
$creation = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
    CommandLine = ('"' + $powershellExe + '" ' + ($arguments -join ' '))
    CurrentDirectory = $projectRoot
    ProcessStartupInformation = $startup
}
if ($creation.ReturnValue -ne 0) { throw "Independent supervisor launch failed, Windows return code $($creation.ReturnValue)." }
$creation.ProcessId | Set-Content -LiteralPath (Join-Path $PSScriptRoot "$workFolder\system_supervisor.pid")
Write-Output "Independent system supervisor started: PID $($creation.ProcessId). Config: $configName. Use finalized snapshot: $UseSnapshot"
Write-Output "Progress: $PSScriptRoot\$workFolder\pipeline_status.json"
} finally { $launchLock.Dispose() }
