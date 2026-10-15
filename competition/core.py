from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import html
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import time
import unicodedata

import orjson

ROOT = Path(__file__).resolve().parent.parent
RULE_CUTOFF = datetime.fromisoformat("2026-08-01T00:00:00+07:00")
MAX_PARAMETERS = 15_000_000_000


def read_json(path):
    return orjson.loads(Path(path).read_bytes().removeprefix(b"\xef\xbb\xbf"))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=path.name + ".", delete=False) as f:
        f.write(orjson.dumps(value, option=orjson.OPT_INDENT_2))
        temporary = Path(f.name)
    try:
        for attempt in range(10):
            try:
                temporary.replace(path)
                return
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.05)
    finally:
        temporary.unlink(missing_ok=True)


def digest(value):
    return hashlib.sha256(orjson.dumps(value, option=orjson.OPT_SORT_KEYS)).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def canonical_id(value):
    if type(value) is int and value > 0:
        return value
    if isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value):
        return int(value)
    raise ValueError(f"Invalid original document ID: {value!r}")


def normalize(text):
    return " ".join(html.unescape(unicodedata.normalize("NFKC", text)).lower().split())


STOPWORDS = set("em toi bi la va co cua cho nhung voi khi thi ma mot cac duoc khong nay trong de o do nhu da tu sao the nao xin hoi can on anh chi bac si it is the a an of to for in on and or are with this that be as".split())


def lexical_terms(text):
    text = normalize(text).replace("đ", "d")
    text = "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")
    result = []
    for run in re.findall(r"[\u3400-\u9fff]+|[^\W_]+", text, re.UNICODE):
        if re.fullmatch(r"[\u3400-\u9fff]+", run):
            result.extend("cjk" + run[i:i + 2] for i in range(max(1, len(run) - 1)))
        elif run not in STOPWORDS and len(run) > 1:
            result.append(run)
    return result


def token_spans(text, tokenizer, window=240, overlap=48):
    if not 0 <= overlap < window:
        raise ValueError("Chunk overlap must be smaller than its token window")
    encoding = tokenizer.encode(text, add_special_tokens=False)
    offsets = [(a, b) for a, b in encoding.offsets if b > a]
    if not offsets:
        return
    step = window - overlap
    previous = None
    for i in range(0, len(offsets), step):
        start, end = offsets[i][0], offsets[min(i + window, len(offsets)) - 1][1]
        # Trim whitespace by moving offsets, never rewrite the chunk text.
        while start < end and text[start].isspace():
            start += 1
        while end > start and text[end - 1].isspace():
            end -= 1
        if end > start and (start, end) != previous:
            yield start, end
            previous = (start, end)
        if i + window >= len(offsets):
            break


def query_variants(text, tokenizer, max_tokens=240, maximum=3):
    variants = [text]
    offsets = tokenizer.encode(text, add_special_tokens=False).offsets
    if len(offsets) > max_tokens:
        for start in [(len(offsets) - max_tokens) // 2, len(offsets) - max_tokens]:
            part = text[offsets[start][0]:offsets[start + max_tokens - 1][1]]
            if part and part not in variants:
                variants.append(part)
    return variants[:maximum]


def connect(path, readonly=False):
    if readonly:
        db = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(path)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA cache_size=-8192")
    db.execute("PRAGMA temp_store=FILE")
    db.execute("PRAGMA foreign_keys=ON")
    return db


def load_config(path):
    config = read_json(path)
    for key in ["links", "queries", "source_manifest", "work_dir"] + (["embedding_adapter"] if config.get("embedding_adapter") else []):
        p = Path(config[key])
        config[key] = str(p if p.is_absolute() else ROOT / p)
    if config["expected_queries"] != 1200 and not config.get("demo", False):
        raise ValueError("Competition runs must contain exactly 1,200 queries")
    if config["id_serialization"] not in {"string", "integer"}:
        raise ValueError("id_serialization must be string or integer")
    if not 0 <= config["chunk_overlap"] < config["chunk_tokens"] or config["embedding_max_tokens"] < config["chunk_tokens"] + 8:
        raise ValueError("Chunk window/overlap must fit the embedding model input including special tokens")
    return config


class StopRequested(Exception):
    pass


def runtime_status(work_dir, state, **details):
    control = Path(work_dir) / 'pipeline_control.json'
    if state not in {'stopped', 'error'} and control.exists():
        try:
            requested = read_json(control).get('stop') is True
        except (OSError, orjson.JSONDecodeError):
            requested = False  # The control file may be between Windows writes.
        if requested:
            raise StopRequested('Graceful stop requested; completed checkpoints are preserved')
    write_json(Path(work_dir) / "pipeline_status.json", {"state": state,
               "pid": os.getpid(), "updated_at": datetime.now(timezone.utc).isoformat(), **details})


class RunLock:
    def __init__(self, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.handle = Path(path).open("a+b")
    def __enter__(self):
        self.handle.seek(0)
        if os.name == "nt":
            import msvcrt
            if not self.handle.read(1):
                self.handle.write(b"0")
                self.handle.flush()
            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return self
    def __exit__(self, *args):
        self.handle.close()
