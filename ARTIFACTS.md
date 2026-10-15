# ViBioMIR data and competition artifacts

This repository contains the reproducible source, the revised links/query data, crawl inventories and provenance, the validated integer-ID submission, and scoring evidence.

## Current result

`results/submission.zip` is the corrected submission. It contains exactly one root-level `submission.json`, covers 1,200 queries, uses integer document IDs, and was locally verified against the original corpus text. `results/submission_diagnosis.json` explains the first 0-score submission: the scorer interpreted numeric strings as `pubmed:<id>` and reported `unknown_doc: 13353`.

## Data included

- `links_corpus_vi_en_zh.parquet`: revised 1,194,469-link source list.
- `query.parquet`: 1,200 competition queries.
- `corpus_data_vi_en_zh/corpus_manifest.json`, `system_snapshot_20261013.json`, and `corpus_audit.json`: frozen inventory and audit.
- `missing_urls.jsonl`, PubMed provenance and recovery provenance: crawl coverage and source receipts.
- `competition/`: source code, verified model registry, evaluation contract, evidence and launcher scripts.

## Intentionally omitted

Multi-gigabyte JSONL/SQLite/vector/model directories remain on the local machine and are listed in `omitted_heavy_artifacts.json`. GitHub's ordinary file and repository limits make uploading those artifacts unreliable; this repository includes their manifests and hashes where available.
