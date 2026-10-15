"""Export a finalized crawl snapshot to bounded-memory, unique-ID Parquet shards."""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys
import time

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from crawl_local import atomic_json

ROOT = Path(__file__).resolve().parent
SCHEMA = pa.schema([("doc_id", pa.int64()), ("url", pa.string()), ("clean_text", pa.string())])


def export(data_dir, output_dir, shard_size=25000, batch_size=1000):
    source_manifest = orjson.loads((data_dir / "corpus_manifest.json").read_bytes())
    expected = source_manifest["counts"]["saved"]
    if output_dir.exists():
        raise FileExistsError(f"Output already exists; choose a new --output-dir: {output_dir}")
    output_dir.mkdir(parents=True)
    seen = bytearray(int(source_manifest.get('max_id', 0)) + 1)
    duplicates = count = part_count = 0
    batch = []
    shards = []
    writer = None
    temporary = None
    started = time.monotonic()
    first_sample = last_sample = None

    def flush():
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=SCHEMA))
            batch.clear()

    def close_shard():
        nonlocal writer
        if writer is None:
            return
        flush()
        writer.close()
        writer = None
        final = temporary.with_suffix("")
        temporary.replace(final)
        actual = pq.ParquetFile(final)
        if actual.metadata.num_rows != part_count or actual.schema_arrow != SCHEMA:
            raise RuntimeError(f"Parquet verification failed: {final.name}")
        shards.append({"file": final.name, "rows": part_count, "bytes": final.stat().st_size})

    try:
        for source_shard in source_manifest["shards"]:
            path = data_dir / source_shard["file"]
            if path.stat().st_size != source_shard["bytes"]:
                raise RuntimeError(f"Source snapshot changed: {path.name}")
            with path.open("rb") as source:
                for line in source:
                    record = orjson.loads(line)
                    doc_id = record["doc_id"]
                    if type(doc_id) is not int or doc_id < 0:
                        raise ValueError(f"Invalid document ID in {path.name}")
                    if doc_id >= len(seen):
                        seen.extend(b'\x00' * (doc_id + 1 - len(seen)))
                    if not isinstance(record.get("url"), str) or not isinstance(record.get("clean_text"), str) or not record["clean_text"].strip():
                        raise ValueError(f"Invalid document fields in {path.name}")
                    if seen[doc_id]:
                        duplicates += 1
                        continue
                    seen[doc_id] = 1
                    if writer is None:
                        temporary = output_dir / f"corpus_part_{len(shards):04d}.parquet.partial"
                        writer = pq.ParquetWriter(temporary, SCHEMA, compression="zstd", compression_level=3,
                                                 use_dictionary=False, write_statistics=False)
                        part_count = 0
                    output_record = {name: record[name] for name in SCHEMA.names}
                    if first_sample is None:
                        first_sample = output_record
                    last_sample = output_record
                    batch.append(output_record)
                    count += 1
                    part_count += 1
                    if len(batch) >= batch_size:
                        flush()
                    if part_count >= shard_size:
                        close_shard()
                    if count % 250000 == 0:
                        print(f"Exported {count:,}/{expected:,} unique documents ({time.monotonic() - started:.1f}s)", flush=True)
        close_shard()
        if count != expected:
            raise RuntimeError(f"Exported {count:,} IDs; source manifest expects {expected:,}")
        # Round-trip checks preserve both the original URL and exact article text.
        first_file = pq.ParquetFile(output_dir / shards[0]["file"])
        assert next(first_file.iter_batches(batch_size=1)).to_pylist()[0] == first_sample
        last_file = pq.ParquetFile(output_dir / shards[-1]["file"])
        final_rows = last_file.read_row_group(last_file.num_row_groups - 1).to_pylist()
        assert final_rows[-1] == last_sample
        inventory = {"created_at": datetime.now(timezone.utc).isoformat(),
                     "source_snapshot_created_at": source_manifest["created_at"],
                     "source_manifest": str(data_dir / "corpus_manifest.json"),
                     "total_documents": count, "duplicate_lines_removed": duplicates,
                     "total_source_ids": source_manifest["total_unique_ids"],
                     "missing_source_ids": source_manifest["total_unique_ids"] - count,
                     "source_complete": source_manifest["complete"],
                     "schema": {"doc_id": "int64", "url": "string", "clean_text": "string"},
                     "format": "parquet", "compression": "zstd", "shards": shards,
                     "total_bytes": sum(shard["bytes"] for shard in shards),
                     "elapsed_seconds": round(time.monotonic() - started, 1)}
        atomic_json(output_dir / "manifest.json", inventory)
        print(orjson.dumps({"output": str(output_dir), "documents": count, "duplicates_removed": duplicates,
                            "bytes": inventory["total_bytes"], "seconds": inventory["elapsed_seconds"]}).decode(), flush=True)
    finally:
        if writer is not None:
            writer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=ROOT / "corpus_data_vi_en_zh")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    source = orjson.loads((args.data_dir / "corpus_manifest.json").read_bytes())
    snapshot = datetime.fromisoformat(source["created_at"]).strftime("%Y%m%dT%H%M%SZ")
    output = args.output_dir or ROOT / "corpus_ready_vi_en_zh" / snapshot
    export(args.data_dir, output)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
