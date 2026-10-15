from __future__ import annotations

import gc
import os
from pathlib import Path
import time

import numpy as np

from .core import digest, read_json, runtime_status, write_json
from .corpus import Corpus


def build_vectors(config, encoder):
    corpus = Corpus(Path(config["work_dir"]) / "corpus.sqlite")
    try:
        return _build_vectors(config, encoder, corpus)
    finally:
        corpus.close()


def _build_vectors(config, encoder, corpus):
    work = Path(config["work_dir"])
    source = read_json(work / "corpus_index.json")
    identity = digest({"corpus": source["fingerprint"], "model": encoder.entry,
                       "maximum_tokens": config["embedding_max_tokens"], "quantization": "normalized-int8-v1"})
    state_path = work / "vector_state.json"
    state = read_json(state_path) if state_path.exists() else {"identity": identity, "parts": [], "rows": 0, "last_id": 0, "active": None}
    if state["identity"] != identity:
        raise ValueError("Embedding/corpus checkpoint mismatch; use a new work_dir")
    vector_dir = work / "vectors"
    vector_dir.mkdir(exist_ok=True)
    total = source["chunks"]
    started = time.monotonic()
    rows_at_start = state['rows']
    last_checkpoint = started
    # The last shard can be fully flushed before its completion metadata is
    # published. Resume must finalize that active shard even at rows == total.
    while state["rows"] < total or state["active"]:
        if not state["active"]:
            capacity = min(config["vector_shard_rows"], total - state["rows"])
            name = f"part_{len(state['parts']):05d}"
            active = {"name": name, "capacity": capacity, "written": 0,
                      "start_last_id": state["last_id"], "rows_before": state["rows"]}
            state["active"] = active
            codes = np.lib.format.open_memmap(vector_dir / (name + ".i8.npy"), mode="w+", dtype=np.int8, shape=(capacity, encoder.dimension))
            ids = np.lib.format.open_memmap(vector_dir / (name + ".ids.npy"), mode="w+", dtype=np.int64, shape=(capacity,))
            norms = np.lib.format.open_memmap(vector_dir / (name + ".norms.npy"), mode="w+", dtype=np.float32, shape=(capacity,))
            write_json(state_path, state)
        else:
            active = state["active"]
            name, capacity = active["name"], active["capacity"]
            codes = np.lib.format.open_memmap(vector_dir / (name + ".i8.npy"), mode="r+")
            ids = np.lib.format.open_memmap(vector_dir / (name + ".ids.npy"), mode="r+")
            norms = np.lib.format.open_memmap(vector_dir / (name + ".norms.npy"), mode="r+")
        written = active["written"]
        since_checkpoint = 0
        for chunk_ids, texts in corpus.embedding_batches(state["last_id"], config.get('embedding_read_batch_size',config["embedding_batch_size"])):
            count = min(len(texts), capacity - written)
            chunk_ids, texts = chunk_ids[:count], texts[:count]
            if not count:
                break
            vectors = encoder.encode(texts)
            quantized = np.clip(np.rint(vectors * 127), -127, 127).astype(np.int8)
            row_norms = np.linalg.norm(quantized.astype(np.float32), axis=1)
            if (row_norms == 0).any():
                raise ValueError("Zero quantized vector")
            codes[written:written + count] = quantized
            ids[written:written + count] = chunk_ids
            norms[written:written + count] = row_norms
            written += count
            state["rows"] = active["rows_before"] + written
            state["last_id"] = chunk_ids[-1]
            active["written"] = written
            since_checkpoint += count
            if since_checkpoint >= config.get('embedding_checkpoint_rows',256) or written == capacity or time.monotonic()-last_checkpoint>=15:
                for array in (codes, ids, norms):
                    array.flush()
                for suffix in (".i8.npy", ".ids.npy", ".norms.npy"):
                    with (vector_dir / (name + suffix)).open("r+b") as handle:
                        os.fsync(handle.fileno())
                write_json(state_path, state)
                rate = (state['rows']-rows_at_start)/max(time.monotonic()-started,.001)
                recovery = getattr(encoder, 'last_recovery', None)
                if recovery:
                    runtime_status(work, "embedding_batch_reduced", completed=state["rows"], total=total,
                                   model=encoder.entry["model_id"], batch_size=encoder.batch_size,
                                   recovery=recovery)
                    del encoder.last_recovery
                runtime_status(work, "embedding_chunks", completed=state["rows"], total=total,
                               model=encoder.entry["model_id"], batch_size=encoder.batch_size,
                               chunks_per_second=round(rate,2),
                               remaining_hours=round((total-state['rows'])/rate/3600,2) if rate else None)
                since_checkpoint = 0
                last_checkpoint = time.monotonic()
            if written == capacity:
                break
        if written != capacity:
            raise ValueError("Corpus ended before expected embedding count")
        del codes, ids, norms
        state["parts"].append({"name": name, "rows": written})
        state["active"] = None
        write_json(state_path, state)
        gc.collect()
    manifest = {"identity": identity, "dimension": encoder.dimension, "rows": total,
                "parts": state["parts"], "model": encoder.entry, "corpus_fingerprint": source["fingerprint"]}
    write_json(work / "vector_manifest.json", manifest)
    return manifest


def dense_search(work, queries, k, block_rows=4096, query_batch=64, device="cpu"):
    work = Path(work)
    manifest = read_json(work / "vector_manifest.json")
    queries = np.asarray(queries, dtype=np.float32)
    if queries.shape[1] != manifest["dimension"]:
        raise ValueError("Query vectors use the wrong model dimension")
    k = min(k, manifest["rows"])
    best_ids = np.full((len(queries), k), -1, dtype=np.int64)
    best_scores = np.full((len(queries), k), -np.inf, dtype=np.float32)
    use_gpu = device.startswith("cuda")
    if use_gpu:
        import torch
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA requested for search but unavailable")
    processed = 0
    for part in manifest["parts"]:
        name = part["name"]
        codes = np.load(work / "vectors" / (name + ".i8.npy"), mmap_mode="r")
        ids = np.load(work / "vectors" / (name + ".ids.npy"), mmap_mode="r")
        norms = np.load(work / "vectors" / (name + ".norms.npy"), mmap_mode="r")
        for start in range(0, part["rows"], block_rows):
            end = min(start + block_rows, part["rows"])
            matrix = np.asarray(codes[start:end], dtype=np.float32) / np.asarray(norms[start:end])[:, None]
            row_ids = np.asarray(ids[start:end])
            if use_gpu:
                matrix_gpu = torch.from_numpy(matrix).to(device)
            for qstart in range(0, len(queries), query_batch):
                qend = min(qstart + query_batch, len(queries))
                if use_gpu:
                    with torch.inference_mode():
                        scores = (torch.from_numpy(queries[qstart:qend]).to(device) @ matrix_gpu.T).cpu().numpy()
                else:
                    scores = queries[qstart:qend] @ matrix.T
                local_k = min(k, len(row_ids))
                positions = np.argpartition(scores, -local_k, axis=1)[:, -local_k:]
                local_scores = np.take_along_axis(scores, positions, axis=1)
                candidate_scores = np.concatenate([best_scores[qstart:qend], local_scores], axis=1)
                candidate_ids = np.concatenate([best_ids[qstart:qend], row_ids[positions]], axis=1)
                selected = np.argpartition(candidate_scores, -k, axis=1)[:, -k:]
                best_scores[qstart:qend] = np.take_along_axis(candidate_scores, selected, axis=1)
                best_ids[qstart:qend] = np.take_along_axis(candidate_ids, selected, axis=1)
            if use_gpu:
                del matrix_gpu
            processed += end - start
        runtime_status(work, "dense_candidate_search", completed=processed, total=manifest["rows"], query_variants=len(queries))
        del codes, ids, norms
    result = []
    for ids, scores in zip(best_ids, best_scores):
        result.append(sorted([(int(i), float(s)) for i, s in zip(ids, scores) if i >= 0], key=lambda item: (-item[1], item[0])))
    return result
