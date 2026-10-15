from __future__ import annotations

from collections import defaultdict
import math

from .core import canonical_id, normalize


def tokens(text, tokenizer="whitespace"):
    text = normalize(text)
    if tokenizer == "whitespace":
        return text.split()
    if tokenizer == "cjk_char":
        import re
        return re.findall(r"[\u3400-\u9fff]|[^\W_]+", text)
    raise ValueError("Unknown evaluation tokenizer")


def lcs_length(a, b):
    # Exact bit-parallel LCS, not substring overlap or unordered token overlap.
    masks = defaultdict(int)
    for position, token in enumerate(b):
        masks[token] |= 1 << position
    state = 0
    for token in a:
        x = state | masks.get(token, 0)
        state = x & ~(x - ((state << 1) | 1))
    return state.bit_count()


def prf(correct, predicted, references):
    precision = correct / predicted if predicted else 0.0
    recall = correct / references if references else 0.0
    return precision, recall, (5 * precision * recall / (4 * precision + recall)) if precision + recall else 0.0


def evaluate(predictions, references, tokenizer="whitespace", final_doc_weight=None):
    expected = {r["id"] for r in references}
    if len(expected) != len(references) or any(type(r["id"]) is not int for r in references):
        raise ValueError("Reference query IDs must be unique integers")
    predicted = {r["id"]: r for r in predictions}
    if len(predicted) != len(predictions) or set(predicted) != expected:
        raise ValueError("Evaluation requires exactly the labeled query IDs; no omitted queries")
    rows = []
    for reference in references:
        prediction = predicted[reference["id"]]
        docs = {canonical_id(i) for i in prediction["relevant_docs"]}
        gold_docs = {canonical_id(i) for i in reference["relevant_docs"]}
        dp, dr, df = prf(len(docs & gold_docs), len(docs), len(gold_docs))
        gold_chunks = [(canonical_id(c["doc_id"]), tokens(c["chunk_text"], tokenizer)) for c in reference["relevant_chunks"]]
        if any(not text for _, text in gold_chunks):
            raise ValueError("Empty reference chunk has undefined overlap")
        found = set()
        correct = 0
        for chunk in prediction["relevant_chunks"]:
            doc_id, text = canonical_id(chunk["doc_id"]), tokens(chunk["chunk_text"], tokenizer)
            matches = [i for i, (gold_id, gold_text) in enumerate(gold_chunks)
                       if doc_id == gold_id and lcs_length(text, gold_text) * 5 >= len(gold_text) * 2]
            correct += bool(matches)
            found.update(matches)
        cp = correct / len(prediction["relevant_chunks"]) if prediction["relevant_chunks"] else 0.0
        cr = len(found) / len(gold_chunks) if gold_chunks else 0.0
        cf = 5 * cp * cr / (4 * cp + cr) if cp + cr else 0.0
        rows.append({"id": reference["id"], "docs_precision": dp, "docs_recall": dr, "docs_f2": df,
                     "chunks_precision": cp, "chunks_recall": cr, "chunks_f2": cf})
    if not rows:
        raise ValueError("No reference queries")
    metrics = {key: sum(row[key] for row in rows) / len(rows) for key in rows[0] if key != "id"}
    result = {"macro": metrics, "per_query": rows, "queries": len(rows), "overlap_threshold": 0.4,
              "tokenizer": tokenizer, "zero_denominator_convention": "zero",
              "final_score": None, "target_over_0_5_verified": None}
    if final_doc_weight is not None:
        if not 0 <= final_doc_weight <= 1:
            raise ValueError("Document score weight must be 0..1")
        result["final_score"] = final_doc_weight * metrics["docs_f2"] + (1 - final_doc_weight) * metrics["chunks_f2"]
        result["target_over_0_5_verified"] = result["final_score"] > 0.5
        result["final_score_formula"] = {"docs_f2_weight": final_doc_weight, "chunks_f2_weight": 1 - final_doc_weight,
                                         "source": "user-specified; must match BTC formula"}
    return result
