from __future__ import annotations

import argparse
from datetime import datetime, timezone
import itertools
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback

os.environ.setdefault("OMP_NUM_THREADS", "2")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "2")

from .core import ROOT, RULE_CUTOFF, RunLock, StopRequested, digest, file_hash, load_config, read_json, runtime_status, write_json


def process_matches(pid, name):
    import psutil
    try:
        process = psutil.Process(int(pid))
        return name in " ".join(process.cmdline())
    except (psutil.NoSuchProcess, psutil.AccessDenied, TypeError, ValueError):
        return False


def crawl_finished(config):
    data_dir = Path(config.get("crawl_data_dir", "corpus_data"))
    if not data_dir.is_absolute():
        data_dir = ROOT / data_dir
    path = data_dir / "crawl_status.json"
    if not path.exists():
        return False, "waiting_for_crawl_status"
    status = read_json(path)
    if process_matches(status.get("pid"), "crawl_local.py"):
        return False, "waiting_for_crawl"
    supervisor_pid = ROOT / ("crawl_vi_en_zh_supervisor.pid" if data_dir.name == "corpus_data_vi_en_zh" else "crawl_supervisor.pid")
    supervisor_script = 'supervise_crawl_vi_en_zh.ps1' if data_dir.name == 'corpus_data_vi_en_zh' else 'supervise_crawl.ps1'
    if supervisor_pid.exists() and process_matches(supervisor_pid.read_text().strip(), supervisor_script):
        return False, "waiting_for_crawl_supervisor"
    if status.get("state") not in {"pass_complete", "finished_with_missing"}:
        return False, "waiting_for_clean_crawl_completion"
    manifest_path = data_dir / "corpus_manifest.json"
    if not manifest_path.exists():
        return False, "waiting_for_final_corpus_inventory"
    inventory = read_json(manifest_path)
    if inventory["counts"]["saved"] != status.get("corpus_saved"):
        return False, "waiting_for_matching_inventory"
    if Path(inventory.get('source_links', '')).resolve() != Path(config['links']).resolve():
        return False, 'waiting_for_matching_source_links'
    return True, "ready"


def wait_for_crawl(config):
    while True:
        done, reason = crawl_finished(config)
        if done:
            return
        runtime_status(config["work_dir"], reason, note="Heavy retrieval work starts only after the crawler ends cleanly.")
        time.sleep(20)


def preflight(config, wait=False):
    import psutil
    import shutil
    while True:
        available = int(psutil.virtual_memory().available / 1048576)
        free_disk = shutil.disk_usage(ROOT).free / 1024 ** 3
        if free_disk < config["minimum_free_disk_gb"]:
            raise RuntimeError(f"Need at least {config['minimum_free_disk_gb']} GiB free disk; available {free_disk:.1f}")
        free_vram = None
        if config["device"].startswith("cuda"):
            output = subprocess.check_output(["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits"], text=True, timeout=10)
            free_vram = int(output.splitlines()[0].strip())
        enough = available >= config["minimum_available_ram_mb"] and (free_vram is None or free_vram >= config["minimum_free_vram_mb"])
        if enough:
            return
        details = {"available_ram_mb": available, "free_vram_mb": free_vram,
                   "required_ram_mb": config["minimum_available_ram_mb"], "note": "Free memory or select the compact CPU config."}
        runtime_status(config["work_dir"], "waiting_for_memory", **details)
        if not wait:
            raise RuntimeError(f"Insufficient free memory: {details}")
        time.sleep(20)


def source_report(config):
    from .models import approved_model
    work = Path(config["work_dir"])
    models = [approved_model(config["embedding_model"])]
    if config["reranker_model"]:
        models.append(approved_model(config["reranker_model"]))
    corpus = read_json(work / "corpus_index.json")
    report = {"created_at": datetime.now(timezone.utc).isoformat(), "system": "ViBioMIR hybrid retrieval",
              "model_cutoff_local": RULE_CUTOFF.isoformat(), "maximum_model_parameters": 15_000_000_000,
              "models": models, "hosted_llm_used": False, "chunk_generation_used": False,
              "data": {"dataset_url": "https://huggingface.co/datasets/AIGuruTinix/ViBioMIR",
                       "links_file": config["links"], "links_sha256": file_hash(config["links"]),
                       "query_file": config["queries"], "query_sha256": file_hash(config["queries"]),
                       "sources": "Original URLs stored in corpus.sqlite:sources and in the crawl JSONL records",
                       "documents_indexed": corpus["documents"], "chunks_indexed": corpus["chunks"],
                       "missing_source_ids": corpus["missing_source_ids"], "source_complete": corpus["source_complete"],
                       "external_training_data": [], "citation": "MedKB — Medical Knowledge Base, https://medkb.tinix.ai/"},
              "source_representations": {"crawl_host_settings": read_json(ROOT / "crawl_host_settings.json"),
                   "pubmed": {'api': 'https://www.ebi.ac.uk/europepmc/webservices/rest/search',
                               'lookup': 'MED records matched by exact PMID to original PubMed URL',
                               'content': 'Source title and abstract; no full text substituted or generated'},
                   "note": "Some original URLs use official mobile/API representations; original doc IDs/URLs are retained with identity checks."},
              "config": config, "score_status": "unmeasured_without_reference_labels_or_BTC_results",
              "final_score_formula": None, "target_final_score": ">0.5"}
    training_report = work / 'training' / 'training_report.json'
    if training_report.exists():
        report['team_training'] = read_json(training_report)
    report['data']['corpus_audit'] = str(Path(config['source_manifest']).parent / 'corpus_audit.json')
    write_json(work / "system_manifest.json", report)
    return report


def build(config):
    from .models import Encoder, approved_model, load_tokenizer
    from .corpus import build_corpus
    from .dense import build_vectors
    from .training import train
    train(config)
    runtime_status(config["work_dir"], "preparing_verified_tokenizer")
    tokenizer = load_tokenizer(config["embedding_model"])
    build_corpus(config, tokenizer, approved_model(config["embedding_model"]))
    del tokenizer
    runtime_status(config["work_dir"], "loading_embedding_model")
    encoder = Encoder(config["embedding_model"], config["device"], config["embedding_batch_size"], config["embedding_max_tokens"], config.get('embedding_adapter'), config.get('embedding_sort_by_length',False))
    try:
        build_vectors(config, encoder)
    finally:
        encoder.close()
    source_report(config)


def tune(config, references, output, tokenizer, validation_ids=None, final_doc_weight=None):
    from .corpus import Corpus
    from .evaluation import evaluate
    from .retrieval import select_predictions
    ranked = read_json(Path(config["work_dir"]) / "ranked_candidates.json")
    reference_ids = {r["id"] for r in references}
    if validation_ids is None:
        # Deterministic holdout fixed independently of scores; never claim a
        # leaderboard score from parameters selected and measured on the same labels.
        validation_ids = {qid for qid in reference_ids if int(digest(qid)[:8], 16) % 5 == 0}
    else:
        validation_ids = set(validation_ids)
    training_ids = reference_ids - validation_ids
    if not validation_ids or not training_ids or not validation_ids <= reference_ids:
        raise ValueError("Tuning requires nonempty disjoint tuning/validation query sets")
    corpus = Corpus(Path(config["work_dir"]) / "corpus.sqlite")
    runs = []
    for docs_k, chunks_k, neighbors in itertools.product([5, 10, 20, 30], [10, 20, 40, 60], [0, 1]):
        trial = dict(config, docs_top_k=docs_k, chunks_top_k=chunks_k, context_neighbors=neighbors)
        predictions = select_predictions([r for r in ranked if r["id"] in training_ids], trial, corpus)
        score = evaluate(predictions, [r for r in references if r["id"] in training_ids], tokenizer, final_doc_weight)
        # This selection objective is explicit; it is not presented as BTC FINAL SCORE.
        objective = score['final_score'] if final_doc_weight is not None else (score["macro"]["docs_f2"] + score["macro"]["chunks_f2"]) / 2
        runs.append({"config": trial, "selection_objective": objective, "tuning_macro": score["macro"]})
    best = max(runs, key=lambda r: r["selection_objective"])
    validation = evaluate(select_predictions([r for r in ranked if r["id"] in validation_ids], best["config"], corpus),
                          [r for r in references if r["id"] in validation_ids], tokenizer, final_doc_weight)
    corpus.close()
    report = {"tuning_ids": sorted(training_ids), "validation_ids": sorted(validation_ids), "best_config": best["config"],
              "selection_objective": 'user-confirmed linear BTC formula' if final_doc_weight is not None else 'proxy mean of document/chunk macro F2; not BTC FINAL SCORE',
              "validation": validation, "trials": [{k: v for k, v in r.items() if k != "config"} | {
                  "docs_top_k": r["config"]["docs_top_k"], "chunks_top_k": r["config"]["chunks_top_k"],
                  "context_neighbors": r["config"]["context_neighbors"]} for r in runs]}
    write_json(output, report)
    write_json(Path(output).with_name("config_tuned.json"), best["config"])
    return report


def main():
    parser = argparse.ArgumentParser(description="Competition-compliant local multilingual retrieval")
    parser.add_argument("command", choices=["run", "build", "predict", "validate", "evaluate", "tune", "status"])
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("config.json"))
    parser.add_argument("--wait-for-crawl", action="store_true")
    parser.add_argument("--use-snapshot", action="store_true", help="Explicitly process the last finalized immutable snapshot")
    parser.add_argument("--submission", type=Path)
    parser.add_argument("--qrels", type=Path)
    parser.add_argument("--validation-ids", type=Path)
    parser.add_argument("--evaluation-tokenizer", choices=["whitespace", "cjk_char"], default="whitespace")
    parser.add_argument("--final-doc-weight", type=float, help="Supply only after BTC confirms its final-score formula")
    args = parser.parse_args()
    config = load_config(args.config)
    work = Path(config["work_dir"])
    if args.command == "status":
        print(orjson_text(read_json(work / "pipeline_status.json")))
        return
    # All profiles share one job lock to avoid competing for this machine's RAM.
    lock_path = ROOT / 'competition' / '.system.lock' if args.command in {'run', 'build', 'predict'} else work / '.report.lock'
    with RunLock(lock_path):
        try:
            if args.command in {"run", "build", "predict"}:
                if args.wait_for_crawl:
                    wait_for_crawl(config)
                elif not args.use_snapshot and not crawl_finished(config)[0]:
                    raise RuntimeError("Crawler still active/incomplete. Use --wait-for-crawl or explicitly --use-snapshot.")
                preflight(config, wait=args.wait_for_crawl or args.use_snapshot)
            if args.command in {"run", "build"}:
                build(config)
            if args.command in {"run", "predict"}:
                from .retrieval import predict
                report = predict(config)
                source_report(config)
                runtime_status(work, "submission_ready", **report, score_status="not_yet_measured")
                print(orjson_text(report))
            elif args.command == "build":
                runtime_status(work, "index_ready")
            elif args.command == "validate":
                from .corpus import Corpus
                from .submission import queries, validate
                corpus = Corpus(work / "corpus.sqlite")
                result = validate(read_json(args.submission or work / "submission.json"), queries(config["queries"], config["expected_queries"]), corpus, config["id_serialization"])
                corpus.close()
                print(orjson_text(result))
            elif args.command == "evaluate":
                from .evaluation import evaluate
                from .corpus import Corpus
                from .submission import queries, validate
                if not args.qrels:
                    raise ValueError("Reference labels are required. No score is inferred without them.")
                predictions = read_json(args.submission or work / 'submission.json')
                reference_rows = read_json(args.qrels)
                query_rows = queries(config['queries'], config['expected_queries'])
                corpus = Corpus(work / 'corpus.sqlite')
                try:
                    validate(predictions, query_rows, corpus, config['id_serialization'])
                finally:
                    corpus.close()
                reference_ids = {r['id'] for r in reference_rows}
                if not reference_ids <= {r['id'] for r in query_rows}:
                    raise ValueError('Reference labels contain unknown query IDs')
                result = evaluate([r for r in predictions if r['id'] in reference_ids], reference_rows,
                                  args.evaluation_tokenizer, args.final_doc_weight)
                result.update(submission_query_count=len(predictions), reference_file=str(args.qrels), reference_sha256=file_hash(args.qrels))
                write_json(work / "evaluation.json", result)
                print(orjson_text({"macro": result["macro"], "final_score": result["final_score"]}))
            elif args.command == "tune":
                if not args.qrels:
                    raise ValueError("Tuning requires genuine reference labels")
                result = tune(config, read_json(args.qrels), work / "tuning_report.json", args.evaluation_tokenizer,
                              read_json(args.validation_ids) if args.validation_ids else None, args.final_doc_weight)
                print(orjson_text(result["validation"]["macro"]))
        except StopRequested:
            runtime_status(work, 'stopped', note='Restart the launcher to resume checkpoints.')
        except Exception as exc:
            runtime_status(work, "error", error=str(exc), traceback=traceback.format_exc())
            raise


def orjson_text(value):
    import orjson
    return orjson.dumps(value, option=orjson.OPT_INDENT_2).decode()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
