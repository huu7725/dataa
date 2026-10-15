from __future__ import annotations

from datetime import datetime
import gc
from pathlib import Path
import re

import numpy as np

from .core import MAX_PARAMETERS, RULE_CUTOFF, ROOT, digest, file_hash, read_json, write_json

REGISTRY_PATH = Path(__file__).with_name("models.json")


def approved_model(key):
    registry = read_json(REGISTRY_PATH)
    if key not in registry:
        raise ValueError(f"Model is not in the verified public registry: {key}")
    model = registry[key]
    if not 0 < model["parameter_count"] <= MAX_PARAMETERS:
        raise ValueError("Model exceeds the 15B parameter limit")
    for field in ["published_at", "revision_at"]:
        if datetime.fromisoformat(model[field]) >= RULE_CUTOFF:
            raise ValueError("Model/revision must predate 2026-08-01 00:00 UTC+7")
    if not re.fullmatch(r"[a-f0-9]{40}", model["revision"]):
        raise ValueError("Model revisions must be immutable commit hashes")
    if not re.fullmatch(r"[a-f0-9]{64}", model["weights_sha256"]):
        raise ValueError("Verified weight checksum required")
    if model["backend"] not in {"onnx", "torch"} or not model["public_url"].startswith("https://huggingface.co/"):
        raise ValueError("Only verified local public models are supported")
    return model


def model_directory(key):
    entry = approved_model(key)
    return ROOT / "competition" / "models" / (key + "--" + entry["revision"][:12])


def fetch_model(key, tokenizer_only=False):
    entry = approved_model(key)
    directory = model_directory(key)
    patterns = ["config.json", "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json", "sentencepiece.bpe.model"]
    if not tokenizer_only:
        patterns.append(entry["weights_file"])
    if not all((directory / name).exists() for name in patterns):
        import os
        os.environ.setdefault("HF_HUB_DISABLE_XET", "1")
        from huggingface_hub import snapshot_download
        snapshot_download(entry["model_id"], revision=entry["revision"], local_dir=str(directory),
                          allow_patterns=patterns, max_workers=1)
    if not tokenizer_only:
        weights = directory / entry["weights_file"]
        fingerprint = {"bytes": weights.stat().st_size, "mtime_ns": weights.stat().st_mtime_ns,
                       "expected_sha256": entry["weights_sha256"]}
        receipt = directory / "verified_weights.json"
        if not receipt.exists() or read_json(receipt).get("fingerprint") != fingerprint:
            actual = file_hash(weights)
            if actual != entry["weights_sha256"]:
                raise ValueError(f"Weight checksum mismatch: {key}")
            write_json(receipt, {"fingerprint": fingerprint, "sha256": actual, "model": entry})
    return directory


def load_tokenizer(key):
    from tokenizers import Tokenizer
    directory = fetch_model(key, tokenizer_only=True)
    tokenizer = Tokenizer.from_file(str(directory / "tokenizer.json"))
    tokenizer.no_truncation()
    tokenizer.no_padding()
    return tokenizer


class Encoder:
    def __init__(self, key, device="cpu", batch_size=4, max_tokens=256, adapter_path=None, sort_by_length=False):
        self.key = key
        self.entry = approved_model(key)
        self.directory = fetch_model(key)
        self.device = device
        self.batch_size = batch_size
        self.max_tokens = max_tokens
        self.dimension = self.entry["dimension"]
        self.sort_by_length = sort_by_length
        self.adapter_weights = None
        if adapter_path:
            adapter = read_json(adapter_path)
            weights = np.asarray(adapter["weights"], dtype=np.float32)
            if (adapter.get("version") != "positive-diagonal-v1" or
                    adapter.get("base_weights_sha256") != self.entry["weights_sha256"] or
                    weights.shape != (self.dimension,) or not np.isfinite(weights).all() or
                    (weights < np.exp(-.25)-1e-6).any() or (weights > np.exp(.25)+1e-6).any() or
                    adapter.get("parameter_count") != self.dimension or
                    self.entry["parameter_count"] + self.dimension > MAX_PARAMETERS):
                raise ValueError("Invalid team-trained retrieval adapter")
            if adapter.get("selected") is True:
                self.adapter_weights = weights
                self.entry = dict(self.entry, team_trained_adapter={"sha256":file_hash(adapter_path),
                                  "parameters":self.dimension,"kind":"positive-diagonal-v1"})
        from tokenizers import Tokenizer
        self.tokenizer = Tokenizer.from_file(str(self.directory / "tokenizer.json"))
        config = read_json(self.directory / "tokenizer_config.json")
        pad = config.get("pad_token", "[PAD]")
        pad = pad.get("content") if isinstance(pad, dict) else pad
        pad_id = self.tokenizer.token_to_id(pad)
        if pad_id is None:
            raise ValueError("Tokenizer does not define a valid padding token")
        self.tokenizer.enable_padding(pad_id=pad_id, pad_token=pad)
        self.tokenizer.enable_truncation(max_length=max_tokens)
        if self.entry["backend"] == "onnx":
            import onnxruntime as ort
            options = ort.SessionOptions()
            options.intra_op_num_threads = 2
            options.inter_op_num_threads = 1
            options.enable_cpu_mem_arena = False
            options.enable_mem_pattern = False
            self.session = ort.InferenceSession(str(self.directory / self.entry["weights_file"]),
                                                sess_options=options, providers=["CPUExecutionProvider"])
        else:
            import torch
            from transformers import AutoModel
            self.torch = torch
            if device.startswith("cuda") and not torch.cuda.is_available():
                raise RuntimeError("CUDA unavailable; choose config_compact.json for the CPU profile")
            kwargs = {"local_files_only": True, "trust_remote_code": False, "use_safetensors": True,
                      "torch_dtype": torch.float16 if device.startswith("cuda") else torch.float32,
                      "low_cpu_mem_usage": True}
            if device.startswith("cuda"):
                kwargs["device_map"] = {"": device}
            self.model = AutoModel.from_pretrained(str(self.directory), **kwargs).eval()
            count = sum(p.numel() for p in self.model.parameters())
            if count > MAX_PARAMETERS or count != self.entry["parameter_count"]:
                raise ValueError(f"Loaded model parameter count does not match registry: {count}")

    def _inputs(self, texts):
        encoded = self.tokenizer.encode_batch(texts)
        return {"input_ids": np.asarray([e.ids for e in encoded], dtype=np.int64),
                "attention_mask": np.asarray([e.attention_mask for e in encoded], dtype=np.int64),
                "token_type_ids": np.asarray([e.type_ids for e in encoded], dtype=np.int64)}

    def encode(self, texts, query=False):
        if not texts:
            return np.empty((0, self.dimension), dtype=np.float32)
        prefix = self.entry["query_prefix" if query else "passage_prefix"]
        if self.sort_by_length and len(texts) > self.batch_size:
            # Keep each tokenization buffer bounded. The output is restored to
            # original ID order; padding changes, source/token content does not.
            outputs = []
            for start in range(0, len(texts), 256):
                part = texts[start:start+256]
                encoded = self.tokenizer.encode_batch([prefix+t for t in part])
                lengths = [sum(e.attention_mask) for e in encoded]
                order = sorted(range(len(part)), key=lambda i:lengths[i])
                result = np.empty((len(part),self.dimension),dtype=np.float32)
                position = 0
                while position < len(order):
                    chosen = order[position:position+self.batch_size]
                    width = max(lengths[i] for i in chosen)
                    inputs = {'input_ids':np.asarray([encoded[i].ids[:width] for i in chosen],dtype=np.int64),
                              'attention_mask':np.asarray([encoded[i].attention_mask[:width] for i in chosen],dtype=np.int64)}
                    if self.entry['backend']=='onnx':
                        inputs['token_type_ids']=np.asarray([encoded[i].type_ids[:width] for i in chosen],dtype=np.int64)
                    vector = self._encode_inputs(inputs)
                    if vector is None:
                        continue
                    result[chosen] = vector
                    position += len(chosen)
                outputs.append(result)
            return np.concatenate(outputs)
        output = []
        position = 0
        while position < len(texts):
            part = [prefix + text for text in texts[position:position + self.batch_size]]
            inputs = self._inputs(part)
            vector = self._encode_inputs(inputs)
            if vector is None:
                continue
            output.append(vector)
            position += len(part)
        return np.concatenate(output)

    def _encode_inputs(self, inputs):
        if self.entry['backend']=='onnx':
            names = {i.name for i in self.session.get_inputs()}
            hidden = self.session.run(None,{k:v for k,v in inputs.items() if k in names})[0]
            if hidden.ndim == 2:
                vector = hidden
            elif self.entry['pooling']=='cls':
                vector = hidden[:,0]
            else:
                mask = inputs['attention_mask'][...,None]
                vector = (hidden*mask).sum(1)/np.maximum(mask.sum(1),1)
        else:
            torch = self.torch
            tensors = {k:torch.from_numpy(v).to(self.device) for k,v in inputs.items() if k!='token_type_ids'}
            try:
                with torch.inference_mode():
                    hidden = self.model(**tensors).last_hidden_state
                    vector = hidden[:,0].float().cpu().numpy()
                del hidden
            except torch.cuda.OutOfMemoryError:
                del tensors
                torch.cuda.empty_cache()
                if self.batch_size==1:
                    raise
                self.batch_size=max(1,self.batch_size//2)
                return None
            del tensors
        vector=np.asarray(vector,dtype=np.float32)
        if self.adapter_weights is not None:
            vector=vector*self.adapter_weights
        norms=np.linalg.norm(vector,axis=1,keepdims=True)
        if vector.ndim != 2 or vector.shape[1] != self.dimension or not np.isfinite(vector).all() or (norms == 0).any():
            # A long-running laptop GPU can occasionally return a non-finite
            # attention result at a large batch even when memory is available.
            # Retry the exact same source texts at half batch; never sanitize or
            # write an invalid vector. The caller repeats the unchanged cursor.
            if self.entry['backend'] == 'torch' and self.batch_size > 1:
                previous = self.batch_size
                self.batch_size = max(1, self.batch_size // 2)
                self.torch.cuda.empty_cache()
                self.last_recovery = {'kind': 'invalid_dense_output', 'previous_batch': previous,
                                      'new_batch': self.batch_size}
                return None
            raise ValueError(f'Invalid dense model output at batch {self.batch_size}')
        return vector/norms

    def close(self):
        if hasattr(self, "model"):
            del self.model
            self.torch.cuda.empty_cache()
        if hasattr(self, "session"):
            del self.session
        gc.collect()


class Reranker:
    def __init__(self, key, device="cuda", batch_size=2, max_tokens=512):
        self.entry = approved_model(key)
        directory = fetch_model(key)
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        self.torch, self.device = torch, device
        self.batch_size, self.max_tokens = batch_size, max_tokens
        self.tokenizer = AutoTokenizer.from_pretrained(str(directory), local_files_only=True, trust_remote_code=False)
        self.model = AutoModelForSequenceClassification.from_pretrained(
            str(directory), local_files_only=True, trust_remote_code=False, use_safetensors=True,
            torch_dtype=torch.float16 if device.startswith("cuda") else torch.float32,
            low_cpu_mem_usage=True, device_map={"": device}).eval()
        count = sum(p.numel() for p in self.model.parameters())
        if count != self.entry["parameter_count"] or count > MAX_PARAMETERS:
            raise ValueError("Reranker parameter count differs from its verified registry")

    def score(self, query, passages):
        query_ids = self.tokenizer.encode(query, add_special_tokens=False)
        limit = min(192, self.max_tokens // 2)
        if len(query_ids) > limit:
            query = self.tokenizer.decode(query_ids[:limit // 2] + query_ids[-limit // 2:], skip_special_tokens=True)
        scores = []
        i = 0
        while i < len(passages):
            part = passages[i:i + self.batch_size]
            inputs = self.tokenizer([query] * len(part), part, padding=True, truncation="only_second",
                                    max_length=self.max_tokens, return_tensors="pt")
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            try:
                with self.torch.inference_mode():
                    scores.extend(self.model(**inputs).logits.flatten().float().cpu().tolist())
            except self.torch.cuda.OutOfMemoryError:
                del inputs
                self.torch.cuda.empty_cache()
                if self.batch_size == 1:
                    raise
                self.batch_size = max(1, self.batch_size // 2)
                continue
            i += len(part)
        return scores

    def close(self):
        del self.model
        self.torch.cuda.empty_cache()
        gc.collect()
