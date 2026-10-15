"""Small real open-model VI/EN/ZH smoke run; never a competition submission."""
from pathlib import Path
import argparse
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from .core import read_json, write_json
from .corpus import build_corpus
from .dense import build_vectors
from .models import Encoder, approved_model, load_tokenizer
from .retrieval import predict


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--strong', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).parent / ("demo_strong" if args.strong else "demo")
    root.mkdir(exist_ok=True)
    # Synthetic software test fixtures, isolated from every competition index.
    records = [
        {"doc_id": 101, "url": "https://example.test/lung-en", "clean_text": "Lung cancer is a disease of the lungs. Biomedical articles describe lung tumors and cancer diagnosis."},
        {"doc_id": 202, "url": "https://example.test/lung-zh", "clean_text": "肺癌是肺部的恶性肿瘤。医学文献介绍肺癌的诊断和相关临床研究。"},
        {"doc_id": 303, "url": "https://example.test/diabetes-vi", "clean_text": "Tài liệu này trình bày bệnh đái tháo đường và nghiên cứu về nồng độ đường trong máu."},
    ]
    pq.write_table(pa.Table.from_pylist([{"id": r["doc_id"], "url": r["url"]} for r in records]), root / "links.parquet")
    pq.write_table(pa.Table.from_pylist([{"id": 1, "query": "Ung thư phổi"}, {"id": 2, "query": "Chẩn đoán ung thư phổi"},
                                       {"id": 3, "query": "Bệnh đái tháo đường và đường huyết"}]), root / "queries.parquet")
    shard = root / "corpus_part_0000.jsonl"
    import orjson
    shard.write_bytes(b"".join(orjson.dumps(record) + b"\n" for record in records))
    write_json(root / "corpus_manifest.json", {"created_at": "2026-10-12T00:00:00+00:00", "complete": True,
                "total_unique_ids": 3, "counts": {"saved": 3}, "shards": [{"file": shard.name, "bytes": shard.stat().st_size}]})
    config = read_json(Path(__file__).with_name("config.json" if args.strong else "config_compact.json"))
    config.update(links=str(root / "links.parquet"), queries=str(root / "queries.parquet"),
                  source_manifest=str(root / "corpus_manifest.json"), work_dir=str(root / "work"),
                  expected_queries=3, demo=True, dense_top_k=3, lexical_top_k=3,
                  rerank_top_k=3, docs_top_k=2, chunks_top_k=2, context_neighbors=0)
    write_json(root / "config_demo.json", config)
    tokenizer = load_tokenizer(config["embedding_model"])
    build_corpus(config, tokenizer, approved_model(config["embedding_model"]))
    del tokenizer
    encoder = Encoder(config["embedding_model"], config["device"], batch_size=config["embedding_batch_size"])
    build_vectors(config, encoder)
    encoder.close()
    report = predict(config)
    predictions = read_json(root / "work" / "submission.json")
    first = {int(i) for i in predictions[0]["relevant_docs"]}
    if 202 not in first:
        raise AssertionError(f"Vietnamese-to-Chinese retrieval test failed: {first}")
    if args.strong and first != {101, 202}:
        raise AssertionError(f"Main-model English/Chinese retrieval test failed: {first}")
    for file in ["submission.json", "submission.zip"]:
        source = root / "work" / file
        source.replace(root / "work" / ("demo_" + file))
    report.update(competition_score=None, demonstration_only=True,
                  json_file=str(root / "work" / "demo_submission.json"), zip_file=str(root / "work" / "demo_submission.zip"),
                  predictions=predictions,
                  cross_language_query="Vietnamese query retrieves English and Chinese lung-cancer fixtures" if args.strong else "Vietnamese query retrieves the Chinese lung-cancer fixture",
                  limitation="Synthetic smoke run only; no competition score is established.")
    write_json(root / "smoke_report.json", report)
    write_json(root / "work" / "submission_validation.json", report)
    print(orjson.dumps(report, option=orjson.OPT_INDENT_2).decode())


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
