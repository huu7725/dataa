$ErrorActionPreference = 'Stop'
$Host.UI.RawUI.WindowTitle = 'ViBioMIR - xu ly va training he thong'
$work = Join-Path $PSScriptRoot 'work_vi_en_zh'
$statusPath = Join-Path $work 'pipeline_status.json'
function Read-SharedText([string]$Path) {
    $stream = [IO.File]::Open($Path, [IO.FileMode]::Open, [IO.FileAccess]::Read, ([IO.FileShare]::ReadWrite -bor [IO.FileShare]::Delete))
    $reader = New-Object IO.StreamReader($stream, [Text.Encoding]::UTF8)
    try { $reader.ReadToEnd() } finally { $reader.Dispose() }
}
$labels = @{
    preparing_training_pairs='Chuan bi cap du lieu training tu corpus'
    loading_frozen_model_for_training='Nap BGE-M3 cong khai cho training'
    encoding_training_pairs='Ma hoa cap training bang GPU'
    training_retrieval_adapter='Huan luyen lop dieu chinh vector'
    preparing_verified_tokenizer='Chuan bi tokenizer da xac minh'
    importing_source_ids='Doi chieu ID nguon'
    chunking_and_lexical_index='Phan doan va lap chi muc tu khoa'
    loading_embedding_model='Nap mo hinh embedding'
    encoding_corpus='Tao embedding corpus'
    building_vectors='Tao embedding corpus'
    embedding_chunks='Tao embedding corpus'
    embedding_batch_reduced='Giam batch GPU va thu lai vector'
    dense_candidate_search='Truy hoi vector'
    reranking='Xep hang lai 1.200 truy van'
    submission_ready='Da xuat va kiem tra JSON + ZIP bai nop'
    waiting_for_memory='Dang cho du RAM/VRAM'
    stopped='Da dung, checkpoint duoc giu'
    error='Gap loi; xem pipeline_stderr.log'
}
while ($true) {
    try {
        $s=Read-SharedText $statusPath | ConvertFrom-Json
        $p=Get-CimInstance Win32_Process -Filter "ProcessId=$($s.pid)" -ErrorAction SilentlyContinue
        if ($p -and $p.CommandLine -notmatch 'competition.pipeline') { $p=$null }
        $label=if ($labels.ContainsKey($s.state)) { $labels[$s.state] } else { $s.state }
        Clear-Host
        Write-Host 'ViBioMIR - XU LY DU LIEU VA TRAINING' -ForegroundColor Cyan
        Write-Host "Tien trinh: $(if($p){'DANG CHAY'}else{'DA DUNG'}) | PID $($s.pid)"
        Write-Host "Trang thai: $label"
        if ($s.scanned_documents) { Write-Host ('Da doc de chon cap training: {0:N0} tai lieu' -f $s.scanned_documents) }
        if ($s.sampled_by_language) { Write-Host "Cap training: VI $($s.sampled_by_language.vi) | EN $($s.sampled_by_language.en) | ZH $($s.sampled_by_language.zh)" }
        if ($s.total_pairs) { Write-Host ('Training embedding: {0:N0} / {1:N0} cap' -f $s.completed_pairs,$s.total_pairs) }
        if ($s.epoch) { Write-Host "Training epoch: $($s.epoch) / $($s.epochs) | Loss: $($s.train_loss)" }
        if ($s.chunks) { Write-Host ('Da tao: {0:N0} chunk' -f $s.chunks) }
        if ($s.total) { Write-Host ('Tien do stage: {0:N0} / {1:N0}' -f $s.completed,$s.total) }
        if ($s.chunks_per_second) { Write-Host ('Toc do embedding: {0:N1} chunk/giay | Con khoang {1:N1} gio cho embedding' -f $s.chunks_per_second,$s.remaining_hours) }
        if ($s.total_queries) { Write-Host ('Xep hang: {0:N0} / {1:N0} cau' -f $s.completed_queries,$s.total_queries) }
        if ($s.available_ram_mb) { Write-Host "RAM trong: $($s.available_ram_mb) MB | VRAM trong: $($s.free_vram_mb) MB" }
        if ($s.error) { Write-Host $s.error -ForegroundColor Yellow }
        $runtimePath=Join-Path $work 'pipeline_runtime.json'
        if (Test-Path -LiteralPath $runtimePath) {
            $runtime=Read-SharedText $runtimePath | ConvertFrom-Json
            Write-Host "Luot chay: $($runtime.attempt) | Log loi: $($runtime.stderr)"
        }
        Write-Host "Cap nhat: $(([DateTimeOffset]::Parse($s.updated_at)).ToOffset([TimeSpan]::FromHours(7)).ToString('yyyy-MM-dd HH:mm:ss')) (UTC+7)"
        Write-Host 'Training dung nhan yeu tu corpus, khong phai dap an BTC.'
        Write-Host 'Chua co diem thi; muc tieu >0.5 chua duoc kiem chung.'
        Write-Host 'Dong cua so theo doi khong dung he thong.'
        Write-Host 'Dung he thong: D:\R2AI\stop_system.bat'
    } catch { Write-Host 'Dang cho trang thai he thong...' }
    Start-Sleep -Seconds 2
}
