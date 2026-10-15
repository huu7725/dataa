from __future__ import annotations

from pathlib import Path
import hashlib
import os
import tempfile
import zipfile

import orjson
import pyarrow.parquet as pq

from .core import canonical_id, file_hash, read_json, write_json


def queries(path, expected=1200):
    rows = pq.read_table(path, columns=["id", "query"]).to_pylist()
    if len(rows) != expected or len({r["id"] for r in rows}) != expected:
        raise ValueError(f"Expected {expected} distinct queries; got {len(rows)}")
    for row in rows:
        if type(row["id"]) is not int or not isinstance(row["query"], str) or not row["query"].strip():
            raise ValueError("Invalid query ID/text")
    return sorted(rows, key=lambda r: r["id"])


def validate(predictions, query_rows, corpus, serialization="string"):
    expected_ids = {r["id"] for r in query_rows}
    if not isinstance(predictions, list) or len(predictions) != len(query_rows):
        raise ValueError("Submission must contain one item per query")
    observed = set()
    total_docs = total_chunks = 0
    def checked_id(value):
        expected_type = str if serialization == "string" else int
        if type(value) is not expected_type:
            raise ValueError(f"Document IDs must use {serialization} serialization consistently")
        doc_id = canonical_id(value)
        if not corpus.db.execute("SELECT 1 FROM sources WHERE doc_id=?", (doc_id,)).fetchone():
            raise ValueError(f"ID not present in links_corpus.parquet: {doc_id}")
        return doc_id
    for result in predictions:
        if not isinstance(result, dict) or set(result) != {"id", "relevant_docs", "relevant_chunks"}:
            raise ValueError("Invalid submission item fields")
        qid = result["id"]
        if type(qid) is not int or qid not in expected_ids or qid in observed:
            raise ValueError("Query IDs must be unique integers matching the full query file")
        observed.add(qid)
        if not isinstance(result["relevant_docs"], list) or not isinstance(result["relevant_chunks"], list):
            raise ValueError("Both retrieval fields must be arrays, including when empty")
        doc_ids = [checked_id(value) for value in result["relevant_docs"]]
        if len(doc_ids) != len(set(doc_ids)):
            raise ValueError("Duplicate predicted document IDs")
        chunk_keys = set()
        for chunk in result["relevant_chunks"]:
            if not isinstance(chunk, dict) or not {"doc_id", "chunk_text"} <= set(chunk) or set(chunk) - {"doc_id", "chunk_text", "chunk_order"}:
                raise ValueError("Invalid chunk fields")
            doc_id = checked_id(chunk["doc_id"])
            text = chunk["chunk_text"]
            if not isinstance(text, str) or not text.strip() or text not in corpus.document(doc_id):
                raise ValueError("chunk_text must be an exact, nonempty substring of its original document")
            if "chunk_order" in chunk and (type(chunk["chunk_order"]) is not int or chunk["chunk_order"] < 0):
                raise ValueError("chunk_order must be a nonnegative integer")
            key = (doc_id, text)
            if key in chunk_keys:
                raise ValueError("Duplicate predicted chunk")
            chunk_keys.add(key)
        total_docs += len(doc_ids)
        total_chunks += len(result["relevant_chunks"])
    if observed != expected_ids:
        raise ValueError("Missing or extra query IDs")
    return {"valid": True, "queries": len(observed), "documents": total_docs,
            "chunks": total_chunks, "id_serialization": serialization,
            "original_chunk_text_verified": True}


def package(predictions, config, corpus, output=None):
    rows = queries(config["queries"], config["expected_queries"])
    report = validate(predictions, rows, corpus, config["id_serialization"])
    output = Path(output or Path(config["work_dir"]) / "submission.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=output.parent, prefix=output.name+'.', delete=False) as handle:
        temporary_json = Path(handle.name)
        handle.write(b'[\n')
        for position, row in enumerate(sorted(predictions, key=lambda r: r["id"])):
            if position:
                handle.write(b',\n')
            handle.write(orjson.dumps(row))
        handle.write(b'\n]\n')
        handle.flush()
        os.fsync(handle.fileno())
    temporary_json.replace(output)
    expected_hash = file_hash(output)
    zipped = output.with_suffix(".zip")
    temporary = zipped.with_suffix(".zip.tmp")
    with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        archive.write(output, arcname=output.name)
    with zipfile.ZipFile(temporary) as archive:
        if archive.namelist() != [output.name] or archive.testzip() is not None:
            raise ValueError("ZIP must contain exactly one JSON file at its root")
        contents_hash = hashlib.sha256()
        with archive.open(output.name) as contents:
            for block in iter(lambda: contents.read(1024*1024), b''):
                contents_hash.update(block)
        if contents_hash.hexdigest() != expected_hash:
            raise ValueError('ZIP payload differs from the validated JSON')
    temporary.replace(zipped)
    report.update(json_file=str(output), zip_file=str(zipped), json_sha256=file_hash(output), zip_sha256=file_hash(zipped))
    write_json(output.with_name("submission_validation.json"), report)
    return report
