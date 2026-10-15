param(
    [Parameter(Mandatory=$true)][string]$ConfigPath,
    [Parameter(Mandatory=$true)][string]$WorkFolder,
    [switch]$UseSnapshot,
    [ValidateRange(0,20)][int]$MaxRestarts = 5
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot '.competition_venv\Scripts\python.exe'
$work = Join-Path $PSScriptRoot $WorkFolder
$controlPath = Join-Path $work 'pipeline_control.json'
$statusPath = Join-Path $work 'pipeline_status.json'
$supervisorLock = [IO.File]::Open((Join-Path $PSScriptRoot '.system_supervisor.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
$PID | Set-Content -LiteralPath (Join-Path $work 'system_supervisor.pid')
Start-Transcript -Path (Join-Path $work 'system_supervisor.log') -Append | Out-Null
function Stop-Requested {
    if (Test-Path -LiteralPath $controlPath) {
        try { return ((Get-Content -LiteralPath $controlPath -Raw | ConvertFrom-Json).stop -eq $true) }
        catch { return $false }
    }
    return $false
}
try {
    for ($attempt=0; $attempt -le $MaxRestarts; $attempt++) {
        if (Stop-Requested) { break }
        $stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
        $stdout = Join-Path $work "pipeline_run_${stamp}_${attempt}_stdout.log"
        $stderr = Join-Path $work "pipeline_run_${stamp}_${attempt}_stderr.log"
        $arguments = @('-u','-m','competition.pipeline','run','--config',"`"$ConfigPath`"")
        $arguments += $(if ($UseSnapshot) { '--use-snapshot' } else { '--wait-for-crawl' })
        $launchedAt = [DateTimeOffset]::UtcNow
        $process = Start-Process -FilePath $pythonExe -ArgumentList $arguments -WorkingDirectory $projectRoot -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
        $null = $process.Handle
        $process.Id | Set-Content -LiteralPath (Join-Path $work 'pipeline.pid')
        @{ supervisor_pid=$PID; launcher_pid=$process.Id; attempt=$attempt; stdout=$stdout; stderr=$stderr } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $work 'pipeline_runtime.json') -Encoding utf8
        Write-Output "Started system PID $($process.Id), attempt $attempt. Logs: $stderr"
        $process.WaitForExit()
        $process.Refresh()
        $exitCode = $process.ExitCode
        Copy-Item -LiteralPath $stdout -Destination (Join-Path $work 'pipeline_stdout.log') -Force
        Copy-Item -LiteralPath $stderr -Destination (Join-Path $work 'pipeline_stderr.log') -Force
        $state = 'missing_status'
        if (Test-Path -LiteralPath $statusPath) {
            $lastStatus = Get-Content -LiteralPath $statusPath -Raw | ConvertFrom-Json
            if ([DateTimeOffset]::Parse($lastStatus.updated_at) -ge $launchedAt) { $state = $lastStatus.state }
        }
        Write-Output "System exited with code $exitCode, stage $state."
        if ((Stop-Requested) -or $state -in @('submission_ready','stopped')) { break }
        # A validation/configuration error needs repair; repeated execution cannot
        # fix it. Unexpected exits without an error status resume saved vectors.
        if ($state -eq 'error') { throw "System reported an error. Inspect $stderr and $statusPath." }
        if ($attempt -eq $MaxRestarts) { throw "System exited repeatedly; checkpoint preserved. Inspect $stderr." }
        Write-Output 'Unexpected exit; resume from checkpoint after 30 seconds.'
        for ($second=0; $second -lt 30; $second++) {
            Start-Sleep -Seconds 1
            if (Stop-Requested) { return }
        }
    }
} catch {
    ($_ | Out-String) | Add-Content -LiteralPath (Join-Path $work 'system_supervisor_error.log')
    throw
} finally {
    Stop-Transcript | Out-Null
    $supervisorLock.Dispose()
}
