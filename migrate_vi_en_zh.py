"""Create a resumable corpus directory for the revised ViBioMIR URL list.

The old crawl directory is read-only input. Only successful records whose ID
and URL occur in links_corpus_vi_en_zh.parquet are copied; old failures are not
carried over because the revised list can contain new URLs with old IDs.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import time

import orjson
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
OLD_DATA = ROOT / "corpus_data"
NEW_DATA = ROOT / "corpus_data_vi_en_zh"
NEW_LINKS = ROOT / "links_corpus_vi_en_zh.parquet"


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def main():
    if not NEW_LINKS.exists():
        raise FileNotFoundError(NEW_LINKS)
    if NEW_DATA.exists() and any(NEW_DATA.iterdir()):
        raise FileExistsError(f"Destination already contains files: {NEW_DATA}")
    NEW_DATA.mkdir(parents=True, exist_ok=True)
    links = pq.read_table(NEW_LINKS, columns=["id", "url"]).to_pylist()
    source = {int(row["id"]): str(row["url"]) for row in links}
    if len(source) != len(links) or len(source) != 1_194_469:
        raise ValueError(f"Revised links file must contain 1,194,469 unique IDs; got {len(source)}")
    output = None
    output_count = 0
    next_part = 0
    copied = 0
    duplicates = 0
    mismatches = []
    seen = set()
    old_parts = sorted(OLD_DATA.glob("corpus_part_*.jsonl"))
    started = time.monotonic()
    try:
        for part in old_parts:
            with part.open("rb") as stream:
                for line_number, line in enumerate(stream, 1):
                    try:
                        record = orjson.loads(line)
                        doc_id = int(record["doc_id"])
                        url = str(record["url"])
                        text = record["clean_text"]
                    except (orjson.JSONDecodeError, KeyError, TypeError, ValueError):
                        continue
                    expected_url = source.get(doc_id)
                    if expected_url is None:
                        continue
                    if expected_url != url:
                        if len(mismatches) < 20:
                            mismatches.append({"doc_id": doc_id, "old_url": url, "new_url": expected_url})
                        continue
                    if doc_id in seen:
                        duplicates += 1
                        continue
                    if not isinstance(text, str) or not text.strip():
                        continue
                    seen.add(doc_id)
                    if output is None or output_count >= 50_000:
                        if output:
                            output.flush()
                            output.close()
                        output = (NEW_DATA / f"corpus_part_{next_part:04d}.jsonl").open("wb", buffering=1024 * 1024)
                        next_part += 1
                        output_count = 0
                    output.write(orjson.dumps({"doc_id": doc_id, "url": url, "clean_text": text}) + b"\n")
                    output_count += 1
                    copied += 1
                    if copied % 100_000 == 0:
                        print({"copied": copied, "source_part": part.name, "seconds": round(time.monotonic() - started, 1)}, flush=True)
    finally:
        if output:
            output.flush()
            output.close()
    control = {"concurrency": 64, "per_host": 1, "host_interval": 0.24, "stop": False,
               "hosts": orjson.loads((ROOT / "crawl_host_settings.json").read_bytes())}
    (NEW_DATA / "crawl_control.json").write_bytes(orjson.dumps(control, option=orjson.OPT_INDENT_2))
    (NEW_DATA / "source_links.json").write_bytes(orjson.dumps({"file": NEW_LINKS.name, "sha256": sha256(NEW_LINKS),
                                                                  "rows": len(links), "copied_saved": copied,
                                                                  "excluded_old_saved": orjson.loads((OLD_DATA / 'corpus_manifest.json').read_bytes())['counts']['saved'] - copied,
                                                                  "created_at": datetime.now(timezone.utc).isoformat(),
                                                                  "url_mismatches": mismatches,
                                                                  "duplicate_saved_lines_skipped": duplicates}, option=orjson.OPT_INDENT_2))
    print(json.dumps({"destination": str(NEW_DATA), "new_links": len(links), "saved_copied": copied,
                      "duplicate_saved_lines_skipped": duplicates, "url_mismatches": len(mismatches),
                      "seconds": round(time.monotonic() - started, 1)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
