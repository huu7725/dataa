$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$venvPython = Join-Path $projectRoot '.competition_venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $venvPython)) {
    & python -m venv --system-site-packages (Join-Path $projectRoot '.competition_venv')
    if ($LASTEXITCODE -ne 0) { throw 'Cannot create isolated Python environment.' }
}
& $venvPython -m pip install -r (Join-Path $PSScriptRoot 'requirements.txt') --disable-pip-version-check
if ($LASTEXITCODE -ne 0) { throw 'Dependency setup failed.' }
Write-Output 'Competition environment ready. Existing crawler environment is preserved.'
