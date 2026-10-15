$ErrorActionPreference = 'Stop'
foreach ($workFolder in @('work','work_compact','work_vi_en_zh','work_compact_vi_en_zh')) {
    $directory = Join-Path $PSScriptRoot $workFolder
    if (Test-Path -LiteralPath $directory) {
        @{ stop=$true } | ConvertTo-Json -Compress | Set-Content -LiteralPath (Join-Path $directory 'pipeline_control.json') -Encoding utf8
    }
}
Write-Output 'System stop requested. Completed index/vector/ranking checkpoints are preserved. The crawler continues independently.'
