from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import time
import zlib

import orjson
import pyarrow.parquet as pq

from .core import canonical_id, connect, digest, file_hash, lexical_terms, read_json, runtime_status, token_spans, write_json


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sources (doc_id INTEGER PRIMARY KEY, url TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS docs (doc_id INTEGER PRIMARY KEY REFERENCES sources(doc_id), body BLOB NOT NULL, chars INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS chunks (id INTEGER PRIMARY KEY, doc_id INTEGER NOT NULL REFERENCES docs(doc_id), ordinal INTEGER NOT NULL, start INTEGER NOT NULL, end INTEGER NOT NULL, UNIQUE(doc_id,ordinal));
CREATE TABLE IF NOT EXISTS imports (file TEXT PRIMARY KEY, offset INTEGER NOT NULL);
CREATE VIRTUAL TABLE IF NOT EXISTS lexical USING fts5(terms, content='', tokenize='unicode61');
CREATE VIRTUAL TABLE IF NOT EXISTS vocabulary USING fts5vocab(lexical, 'row');
"""


def build_corpus(config, tokenizer, model_entry):
    db = connect(Path(config["work_dir"]) / "corpus.sqlite")
    try:
        return _build_corpus(config, tokenizer, model_entry, db)
    finally:
        db.close()


def _build_corpus(config, tokenizer, model_entry, db):
    work = Path(config["work_dir"])
    work.mkdir(parents=True, exist_ok=True)
    manifest = read_json(config["source_manifest"])
    parent = Path(config["source_manifest"]).parent
    identity = {"manifest_sha256": file_hash(config["source_manifest"]),
                "links_sha256": file_hash(config["links"]), "tokenizer_model": model_entry,
                "chunk_tokens": config["chunk_tokens"], "chunk_overlap": config["chunk_overlap"]}
    fingerprint = digest(identity)
    db.executescript(SCHEMA)
    prior = db.execute("SELECT value FROM meta WHERE key='fingerprint'").fetchone()
    if prior and prior[0] != fingerprint:
        raise ValueError("Source/tokenizer changed. Use a new work_dir to keep indices consistent.")
    db.execute("INSERT OR IGNORE INTO meta VALUES ('fingerprint',?)", (fingerprint,))
    db.commit()
    if not db.execute("SELECT value FROM meta WHERE key='links_complete'").fetchone():
        links = pq.ParquetFile(config["links"])
        count = 0
        for batch in links.iter_batches(batch_size=4096, columns=["id", "url"]):
            rows = [(canonical_id(r["id"]), str(r["url"])) for r in batch.to_pylist()]
            db.executemany("INSERT OR IGNORE INTO sources VALUES (?,?)", rows)
            db.commit()
            count += len(rows)
            if count % 262144 == 0:
                runtime_status(work, "importing_source_ids", rows=count)
        db.execute("INSERT OR REPLACE INTO meta VALUES ('links_complete','true')")
        db.commit()
    next_chunk = db.execute("SELECT coalesce(max(id),0)+1 FROM chunks").fetchone()[0]
    processed = duplicates = 0
    started = time.monotonic()
    for shard in manifest["shards"]:
        path = parent / shard["file"]
        if path.stat().st_size != shard["bytes"]:
            raise ValueError(f"Snapshot shard changed during indexing: {path}")
        imported = db.execute("SELECT offset FROM imports WHERE file=?", (path.name,)).fetchone()
        offset = imported[0] if imported else 0
        if offset > path.stat().st_size:
            raise ValueError("Invalid source checkpoint")
        with path.open("rb") as source:
            source.seek(offset)
            pending = 0
            for line in source:
                record = orjson.loads(line)
                doc_id = canonical_id(record["doc_id"])
                text, url = record["clean_text"], record["url"]
                if not isinstance(text, str) or not text.strip():
                    raise ValueError(f"Empty document: {doc_id}")
                authoritative = db.execute("SELECT url FROM sources WHERE doc_id=?", (doc_id,)).fetchone()
                if not authoritative or authoritative[0] != url:
                    raise ValueError(f"Document ID/URL does not match links_corpus.parquet: {doc_id}")
                if db.execute("SELECT 1 FROM docs WHERE doc_id=?", (doc_id,)).fetchone():
                    duplicates += 1
                else:
                    db.execute("INSERT INTO docs VALUES (?,?,?)", (doc_id, zlib.compress(text.encode(), 3), len(text)))
                    spans = list(token_spans(text, tokenizer, config["chunk_tokens"], config["chunk_overlap"]))
                    if not spans:
                        raise ValueError(f"No usable text tokens in document: {doc_id}")
                    for ordinal, (a, b) in enumerate(spans):
                        db.execute("INSERT INTO chunks VALUES (?,?,?,?,?)", (next_chunk, doc_id, ordinal, a, b))
                        db.execute("INSERT INTO lexical(rowid,terms) VALUES (?,?)", (next_chunk, " ".join(lexical_terms(text[a:b]))))
                        next_chunk += 1
                processed += 1
                pending += 1
                if pending >= 256:
                    db.execute("INSERT OR REPLACE INTO imports VALUES (?,?)", (path.name, source.tell()))
                    db.commit()
                    pending = 0
                    runtime_status(work, "chunking_and_lexical_index", source_lines_this_run=processed,
                                   chunks=next_chunk - 1, elapsed_seconds=round(time.monotonic() - started, 1))
            db.execute("INSERT OR REPLACE INTO imports VALUES (?,?)", (path.name, source.tell()))
            db.commit()
    documents = db.execute("SELECT count(*) FROM docs").fetchone()[0]
    chunks = db.execute("SELECT count(*) FROM chunks").fetchone()[0]
    if documents != manifest["counts"]["saved"]:
        raise ValueError("Indexed document count differs from the finalized crawl snapshot")
    info = {"fingerprint": fingerprint, "source": identity, "documents": documents, "chunks": chunks,
            "source_complete": manifest["complete"], "missing_source_ids": manifest["total_unique_ids"] - documents,
            "duplicates_skipped_this_run": duplicates}
    write_json(work / "corpus_index.json", info)
    write_json(work / "source_snapshot.json", manifest)
    return info


class Corpus:
    def __init__(self, path):
        self.db = connect(path, readonly=True)
    @lru_cache(maxsize=16)
    def document(self, doc_id):
        row = self.db.execute("SELECT body FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        if row is None:
            raise ValueError(f"Document is not in the indexed original corpus: {doc_id}")
        return zlib.decompress(row[0]).decode("utf-8")
    def chunk(self, chunk_id):
        row = self.db.execute("SELECT doc_id,ordinal,start,end FROM chunks WHERE id=?", (int(chunk_id),)).fetchone()
        if row is None:
            raise ValueError(f"Unknown chunk: {chunk_id}")
        doc_id, ordinal, a, b = row
        return {"chunk_id": int(chunk_id), "doc_id": doc_id, "ordinal": ordinal,
                "start": a, "end": b, "chunk_text": self.document(doc_id)[a:b]}
    def embedding_batches(self, after_id, batch_size):
        cursor = self.db.execute("SELECT id,doc_id,start,end FROM chunks WHERE id>? ORDER BY id", (after_id,))
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                return
            yield [r[0] for r in rows], [self.document(r[1])[r[2]:r[3]] for r in rows]
    def context(self, candidate, neighbors, maximum_characters):
        rows = self.db.execute("SELECT start,end FROM chunks WHERE doc_id=? AND ordinal BETWEEN ? AND ? ORDER BY ordinal",
                               (candidate["doc_id"], max(0, candidate["ordinal"] - neighbors), candidate["ordinal"] + neighbors)).fetchall()
        a, b = min(r[0] for r in rows), max(r[1] for r in rows)
        if b - a > maximum_characters:
            a, b = candidate["start"], candidate["end"]
        return a, b, self.document(candidate["doc_id"])[a:b]
    def lexical(self, query, k):
        if k <= 0:
            return []
        terms = list(dict.fromkeys(lexical_terms(query)))
        ranked = []
        for term in terms:
            row = self.db.execute("SELECT doc FROM vocabulary WHERE term=?", (term,)).fetchone()
            if row:
                ranked.append((row[0], term))
        terms = [term for _, term in sorted(ranked)[:48]]
        if not terms:
            return []
        expression = " OR ".join('"' + term.replace('"', '""') + '"' for term in terms)
        return [row[0] for row in self.db.execute(
            "SELECT rowid FROM lexical WHERE lexical MATCH ? ORDER BY bm25(lexical),rowid LIMIT ?", (expression, k))]
    def close(self):
        self.document.cache_clear()
        self.db.close()
