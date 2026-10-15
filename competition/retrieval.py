from __future__ import annotations

from collections import defaultdict
from pathlib import Path
import time

import numpy as np
import orjson

from .core import connect, digest, file_hash, query_variants, read_json, runtime_status, write_json
from .corpus import Corpus
from .dense import dense_search
from .models import Encoder, Reranker, approved_model, load_tokenizer
from .submission import package, queries


def candidate_identity(config):
    work = Path(config["work_dir"])
    return digest({"config": config, "query_sha256": file_hash(config["queries"]),
                   "vectors": read_json(work / "vector_manifest.json")["identity"]})


def build_candidates(config, encoder):
    work = Path(config["work_dir"])
    rows = queries(config["queries"], config["expected_queries"])
    tokenizer = load_tokenizer(config["embedding_model"])
    texts, owners = [], []
    for row in rows:
        variants = query_variants(row["query"], tokenizer)
        for position, text in enumerate(variants):
            texts.append(text)
            owners.append((row["id"], 1.0 if position == 0 else 0.8))
    vectors = encoder.encode(texts, query=True)
    encoder.close()
    dense = dense_search(work, vectors, config["dense_top_k"], config["search_block_rows"],
                         config["search_query_batch"], config["device"])
    corpus = Corpus(work / "corpus.sqlite")
    fused = {r["id"]: defaultdict(float) for r in rows}
    for (qid, weight), ranking in zip(owners, dense):
        for rank, (chunk_id, _) in enumerate(ranking, 1):
            fused[qid][chunk_id] += weight / (60 + rank)
    output = []
    for row in rows:
        for rank, chunk_id in enumerate(corpus.lexical(row["query"], config["lexical_top_k"]), 1):
            fused[row["id"]][chunk_id] += 0.8 / (60 + rank)
        ranking = sorted(fused[row["id"]].items(), key=lambda item: (-item[1], item[0]))[:config["rerank_top_k"]]
        output.append({"id": row["id"], "query": row["query"],
                       "candidates": [{"chunk_id": cid, "fusion_score": score} for cid, score in ranking]})
    corpus.close()
    write_json(work / "candidates.json", {"identity": candidate_identity(config), "queries": output})
    return output


def ranked_results(config, candidates, reranker=None):
    work = Path(config["work_dir"])
    corpus = Corpus(work / "corpus.sqlite")
    db = connect(work / "ranking.sqlite")
    db.execute("CREATE TABLE IF NOT EXISTS ranked (id INTEGER PRIMARY KEY, identity TEXT NOT NULL, result BLOB NOT NULL)")
    identity = digest({"candidates": candidates, "reranker": approved_model(config["reranker_model"]) if config["reranker_model"] else None,
                       "max_tokens": config["rerank_max_tokens"]})
    result = []
    for number, item in enumerate(candidates, 1):
        saved = db.execute("SELECT identity,result FROM ranked WHERE id=?", (item["id"],)).fetchone()
        if saved and saved[0] == identity:
            result.append(orjson.loads(saved[1]))
            continue
        chunks = [corpus.chunk(candidate["chunk_id"]) for candidate in item["candidates"]]
        scores = reranker.score(item["query"], [c["chunk_text"] for c in chunks]) if reranker else [c["fusion_score"] for c in item["candidates"]]
        for chunk, candidate, score in zip(chunks, item["candidates"], scores):
            chunk.update(score=float(score), fusion_score=candidate["fusion_score"])
            chunk.pop("chunk_text")
        chunks.sort(key=lambda c: (-c["score"], -c["fusion_score"], c["chunk_id"]))
        row = {"id": item["id"], "candidates": chunks}
        db.execute("INSERT OR REPLACE INTO ranked VALUES (?,?,?)", (item["id"], identity, orjson.dumps(row)))
        db.commit()
        result.append(row)
        runtime_status(work, "reranking", completed_queries=number, total_queries=len(candidates))
    db.close()
    corpus.close()
    write_json(work / "ranked_candidates.json", result)
    return result


def select_predictions(ranked, config, corpus):
    predictions = []
    serialize = str if config["id_serialization"] == "string" else int
    for row in ranked:
        grouped = defaultdict(list)
        for candidate in row["candidates"]:
            grouped[candidate["doc_id"]].append(candidate)
        document_ids = sorted(grouped, key=lambda doc_id: (-max(c["score"] for c in grouped[doc_id]), doc_id))[:config["docs_top_k"]]
        passages = []
        for doc_id in document_ids:
            regions = []
            for candidate in grouped[doc_id]:
                a, b, text = corpus.context(candidate, config["context_neighbors"], config["context_max_characters"])
                merged = False
                for region in regions:
                    overlap = max(0, min(b, region["end"]) - max(a, region["start"]))
                    if overlap and max(b, region["end"]) - min(a, region["start"]) <= config["context_max_characters"]:
                        region["start"], region["end"] = min(a, region["start"]), max(b, region["end"])
                        region["score"] = max(region["score"], candidate["score"])
                        merged = True
                        break
                    if overlap >= 0.7 * min(b - a, region["end"] - region["start"]):
                        merged = True
                        break
                if not merged and len(regions) < config["chunks_per_doc"]:
                    regions.append({"doc_id": doc_id, "start": a, "end": b, "score": candidate["score"]})
            passages.extend(regions)
        passages.sort(key=lambda c: (-c["score"], c["doc_id"], c["start"]))
        chunks = []
        seen = set()
        if config['chunks_top_k'] > 0:
            for passage in passages:
                text = corpus.document(passage['doc_id'])[passage['start']:passage['end']]
                key = (passage['doc_id'], text)
                if key in seen:
                    continue
                seen.add(key)
                chunks.append({'doc_id': serialize(passage['doc_id']), 'chunk_text': text, 'chunk_order': len(chunks)})
                if len(chunks) >= config['chunks_top_k']:
                    break
        predictions.append({"id": row["id"], "relevant_docs": [serialize(i) for i in document_ids], "relevant_chunks": chunks})
    return predictions


def predict(config):
    work = Path(config["work_dir"])
    cached = work / "candidates.json"
    if cached.exists() and read_json(cached).get("identity") == candidate_identity(config):
        candidates = read_json(cached)["queries"]
    else:
        encoder = Encoder(config["embedding_model"], config["device"], config["embedding_batch_size"], config["embedding_max_tokens"], config.get('embedding_adapter'))
        try:
            candidates = build_candidates(config, encoder)
        finally:
            encoder.close()
    reranker = Reranker(config["reranker_model"], config["device"], config["rerank_batch_size"], config["rerank_max_tokens"]) if config["reranker_model"] else None
    try:
        ranked = ranked_results(config, candidates, reranker)
    finally:
        if reranker:
            reranker.close()
    corpus = Corpus(work / "corpus.sqlite")
    try:
        predictions = select_predictions(ranked, config, corpus)
        return package(predictions, config, corpus)
    finally:
        corpus.close()
