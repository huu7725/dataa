"""Read-only validation of the last finalized corpus snapshot."""
from collections import Counter
import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys
import time
from urllib.parse import urlsplit

import orjson

from crawl_local import atomic_json

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "corpus_data_vi_en_zh"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, default=DATA)
    args = parser.parse_args()
    data = args.data_dir
    manifest = orjson.loads((data / "corpus_manifest.json").read_bytes())
    flags = bytearray(int(manifest.get('max_id', 0)) + 1)
    counts = Counter()
    host_counts = Counter()
    issues = []
    started = time.monotonic()
    for shard in manifest["shards"]:
        path = data / shard["file"]
        if path.stat().st_size != shard["bytes"]:
            raise RuntimeError(f"Snapshot shard size changed: {path.name}")
        with path.open("rb") as handle:
            for number, line in enumerate(handle, 1):
                counts["lines"] += 1
                try:
                    record = orjson.loads(line)
                    doc_id = record["doc_id"]
                    if type(doc_id) is not int or doc_id < 0:
                        raise ValueError("invalid_doc_id")
                    if doc_id >= len(flags):
                        flags.extend(b'\x00' * (doc_id + 1 - len(flags)))
                    if not isinstance(record.get("url"), str) or not record["url"].startswith(("http://", "https://")):
                        raise ValueError("invalid_url")
                    text = record.get("clean_text")
                    if not isinstance(text, str) or not text.strip():
                        raise ValueError("empty_text")
                    if flags[doc_id]:
                        counts["duplicate_ids"] += 1
                    else:
                        flags[doc_id] = 1
                        counts["unique_valid_ids"] += 1
                        host_counts[urlsplit(record["url"]).hostname or ""] += 1
                    counts["valid_lines"] += 1
                    counts["text_characters"] += len(text)
                    if len(text) < 100:
                        counts["short_text_under_100_chars"] += 1
                except (ValueError, TypeError, KeyError) as exc:
                    counts["invalid_lines"] += 1
                    if len(issues) < 100:
                        issues.append({"file": path.name, "line": number, "error": str(exc)})
                if counts["lines"] % 500000 == 0:
                    print(f"Validated {counts['lines']:,} lines in {time.monotonic() - started:.1f}s", flush=True)
    expected = manifest["counts"]["saved"]
    valid = not counts["invalid_lines"] and counts["unique_valid_ids"] == expected
    report = {"created_at": datetime.now(timezone.utc).isoformat(),
              "snapshot_created_at": manifest["created_at"], "schema_and_id_count_valid": valid,
              "expected_saved_ids": expected, "counts": dict(counts), "issues": issues,
              "hosts": dict(host_counts), "elapsed_seconds": round(time.monotonic() - started, 2),
              "note": "Checks JSONL syntax, required fields, duplicate IDs and snapshot counts; does not verify medical accuracy or extraction completeness."}
    atomic_json(data / "corpus_audit.json", report)
    print(orjson.dumps({"valid": valid, "counts": dict(counts), "elapsed_seconds": report["elapsed_seconds"]}).decode(), flush=True)
    if not valid:
        raise SystemExit(1)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
