"""Package source and provenance separately from the one-JSON prediction ZIP."""
from datetime import datetime, timezone
import importlib.metadata as metadata
from pathlib import Path
import zipfile

from .core import ROOT, file_hash, read_json, write_json
from .models import approved_model


def main():
    package = ROOT / 'competition'
    files = [p for p in package.iterdir() if p.is_file() and p.suffix in {'.py', '.json', '.txt', '.md', '.ps1'}
             and p.name != 'source_bundle_manifest.json']
    files.extend(sorted((package/'evidence').glob('*')))
    files.extend([ROOT/'start_system_after_crawl.bat', ROOT/'start_system_now.bat', ROOT/'watch_system.bat', ROOT/'stop_system.bat', ROOT/'crawl_host_settings.json',
                  ROOT/'query.parquet', ROOT/'links_corpus_vi_en_zh.parquet', ROOT/'revised_links_audit_20261012.json'])
    files.extend(package/'work_vi_en_zh'/'training'/name for name in
                 ['pairs.jsonl','pairs.manifest.json','adapter.json','training_report.json','embedding_checkpoint.json'])
    files.extend([package/'work_vi_en_zh'/'snapshot_transition.json',package/'work_vi_en_zh'/'run_plan.json'])
    files.extend(ROOT/'corpus_data_vi_en_zh'/name for name in
                 ['corpus_manifest.json','system_snapshot_20261013.json','corpus_audit.json','pubmed_api_provenance.jsonl','recovery_provenance.jsonl'])
    info = {'created_at': datetime.now(timezone.utc).isoformat(), 'artifact': 'system source; not a prediction submission',
            'files': {str(p.relative_to(ROOT)).replace('\\', '/'): file_hash(p) for p in files if p.is_file()},
            'models': [approved_model(k) for k in ['bge_m3', 'reranker_m3', 'e5_small_onnx']],
            'tested_runtime': {name: metadata.version(name) for name in ['torch', 'transformers', 'tokenizers', 'onnxruntime', 'pyarrow']},
            'main_model_fixture_passed': read_json(package/'evidence/main_models_runtime_check.json')['english_chinese_relevance_above_distractor'],
            'full_pipeline_main_model_smoke_passed': read_json(package/'demo_strong/smoke_report.json')['valid'],
            'competition_score': None, 'prediction_submission_after_crawl': 'competition/work_vi_en_zh/submission.zip'}
    write_json(package/'source_bundle_manifest.json', info)
    files.append(package/'source_bundle_manifest.json')
    output = ROOT/'vibiomir_system_source.zip'
    temporary = output.with_suffix('.zip.building')
    with zipfile.ZipFile(temporary, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=4) as archive:
        for path in files:
            if path.is_file():
                archive.write(path, arcname=str(path.relative_to(ROOT)).replace('\\', '/'))
    with zipfile.ZipFile(temporary) as archive:
        names = archive.namelist()
        if archive.testzip() is not None or len(names) != len(set(names)):
            raise ValueError('Source archive is corrupt or has duplicate filenames')
        if any(name.startswith('/') or '..' in Path(name).parts for name in names):
            raise ValueError('Invalid source archive path')
    temporary.replace(output)
    print({'source_zip': str(output), 'bytes': output.stat().st_size, 'files': len(files), 'competition_score': None})


if __name__ == '__main__':
    main()
