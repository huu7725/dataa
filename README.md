# ViBioMIR crawler

Repair on 2026-10-13: the apparent freeze at 1,039,536 documents was a blocked VOV source waiting 30 seconds between HTTP 403 responses. Its host failure threshold is now eight, and similarly blocked QDND is deferred sooner. Progress exposes `activity`, `cooling_hosts` and `pass_number`; the revised watcher explains when no connections are active because sources are cooling down. The recovery run uses one parser for low RAM and at most two additional passes, then publishes an honest inventory for the retrieval pipeline. Missing URLs remain retryable. Diagnostic/backups are in `repair_20261013` and `crawl_diagnosis_20261013.json`.

Competition retrieval code is in [`competition/README.md`](competition/README.md). `start_system_after_crawl.bat` starts the system job waiting for clean crawl completion, then builds the multilingual index and produces the validated JSON/ZIP for all 1,200 queries. Models are pinned public versions before the competition cutoff; chunks remain exact source spans. `stop_system.bat` requests a checkpointed system stop without stopping the crawler. No competition score is claimed without reference labels or BTC evaluation.

Project and data location: `D:\R2AI`. The launcher at the old Desktop path forwards to this location.

Double-click `crawl.bat` in this folder or the Desktop R2AI folder to start/resume and show progress. If the crawler is already running, it shows that run without starting another one. Data is always written to `D:\R2AI\corpus_data` and saved IDs are skipped. Close the progress window to leave the crawler running; double-click `stop_crawl.bat` to request a graceful stop.
The start script chooses 1–2 parser processes and 96–256 connections based on available RAM. A hidden supervisor restarts unexpected process failures up to five times. In repeat mode, a crashed parser pool is retried with fewer processes and connections. The crawler keeps all existing corpus files and resumes by saved ID.

Start in the background with PowerShell:

```powershell
& 'D:\R2AI\start_crawl.ps1'
```

Or run in a terminal:

```powershell
Set-Location D:\R2AI
python -u crawl_local.py --concurrency 64 --per-host 1 --host-interval 0.24 --parse-workers 2 --timeout 30 --repeat --retry-delay 30 --max-passes 10
```

Results: `corpus_data\corpus_part_*.jsonl`, preserving `doc_id`, `url`, `clean_text`.
Progress: `corpus_data\crawl_status.json` (refreshed every 10 seconds).
Failures: `corpus_data\crawl_failures.jsonl`. Startup resumes saved IDs automatically.
Permanently missing pages (400/404/410/451) are skipped on future runs; add `--retry-failed` to retry them too.
Temporary failures and deferred hosts are retried on the next run. A host with repeated block/connection errors is deferred for the current pass; see `deferred_hosts` in the progress file. A finished pass does not imply every URL was successfully collected.
Host positions are checkpointed in SQLite every 30 seconds. Subsequent passes continue beyond a failed prefix and wrap around once, so the same first failed URLs cannot starve accessible pages later in the host index.

Repeat mode ends after three consecutive passes save no new documents, or after `--max-passes` (default 10). Set `--max-passes 0` to remove the pass limit; the no-progress limit remains active. This produces `finished_with_missing`, not a claim that every page was collected. Run `crawl.bat` again to attempt the remaining URLs later. Missing pages remain retryable; these limits do not turn them into terminal failures.

At a graceful stop or final pass, `corpus_data\corpus_manifest.json` lists the saved shards and counts of saved, retryable, terminal and unvisited IDs. `corpus_data\missing_urls.jsonl` contains every missing ID and original URL for future recovery. The manifest is a snapshot at the last clean stop; a running crawl may already have added more records. Existing JSONL shards use the same `doc_id`, `url`, `clean_text` schema and can be consumed by the retrieval system.

`python audit_corpus.py` validates all shards listed in the last manifest and writes `corpus_data\corpus_audit.json`, checking JSONL syntax, required fields, unique ID counts and duplicates. It does not validate medical accuracy. Duplicate IDs, if reported, should be removed by ID during indexing.

`python prepare_corpus.py` exports that finalized snapshot into a new dated folder under `corpus_ready`, preserving the three document fields, keeping the first valid record per ID, and removing duplicate IDs. Output consists of Zstandard-compressed Parquet shards of up to 25,000 documents and `manifest.json`. Export uses batches of 1,000 records to keep memory bounded. It checks every shard's schema and row count and round-trips the first and last records. Original JSONL data is preserved. Subsequent clean stops can be exported as new snapshots; new downloads are not included in an older snapshot.

To read the prepared corpus without loading it all into RAM:

```python
import json
from pathlib import Path
import pyarrow.dataset as ds

folder = Path(r"D:\R2AI\corpus_ready\20261012T034551Z")
manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
corpus = ds.dataset([folder / shard["file"] for shard in manifest["shards"]], format="parquet")
for batch in corpus.to_batches(batch_size=1024):
    documents = batch.to_pylist()  # Feed these documents to your indexer.
```

Use `--limit 600` for a short run. Original crawler: `crawl_local.original.py`.
Integration checks: `python test_crawler.py`.

The crawler streams Parquet into a reusable index grouped by host, limits buffers, reuses HTTP connections, and extracts HTML in separate processes. It checkpoints JSONL before committing success IDs to SQLite. One writer per data directory is enforced. Close the terminal or press Ctrl+C to interrupt a foreground run, then run the same command to resume. For a background run, inspect the PID in `crawl_status.json` before stopping it.

Dependencies are listed in `requirements.txt`; the optional PDF parser is installed locally in `.vendor`.

Live controls: edit `corpus_data\crawl_control.json` with `concurrency` (1–1024), `per_host` (1–32), `host_interval` (0–60 seconds), or `stop`. Settings are applied every 10 seconds. Set `stop` to `true` to drain pending downloads, extraction and writes before exiting. Restart with the start script, which resets `stop` to `false`.

Each host can have separate settings under `hosts`: `per_host`, `host_interval`, `timeout`, `connect_timeout`, `retries`, and `host_failure_limit`. For example, a healthy source can use four connections and a 0.2-second gap while an offline source uses a five-second connection timeout, no immediate retries, and an eight-error threshold. The launcher loads `crawl_host_settings.json` for initial profiles and preserves profiles already present in live control. `stop_crawl.ps1` preserves nested host settings too. `saved_by_host`, `failed_by_host`, and `active_requests_by_host` in progress identify which sources are contributing output.

For `ask.39.net`, `prefer_mobile: true` selects the official `wapask.39.net` page declared in the desktop page's mobile metadata. Requests retain the same numeric question ID, and a successful response is accepted only when its final host/path and hidden question-ID field match. Saved records keep their original `doc_id` and desktop `url`. Verification pages or mismatched question IDs are never saved as documents. A mobile 404 remains retryable because it does not prove the original desktop page is missing. Shared `39.net` scheduling now rotates fairly after serving one subdomain, preventing early subdomains from consuming every family slot. The scheduler waits until the next allowed request time instead of rounding every gap up to a fixed polling interval.
Alternatively, run `& 'D:\R2AI\stop_crawl.ps1'` to request a graceful stop. `start_crawl.ps1` accepts `-ParseWorkers`, `-Concurrency`, `-PerHost`, `-HostInterval`, `-RetryDelay`, and `-MaxPasses` to override defaults.
Progress includes `recent_saved_per_second`, queue sizes and download rate to measure usable output throughput. Known verification pages and binary images are filtered before extraction, and repeatedly blocked hosts are deferred. Extraction settings for article content are unchanged.

Observed live output rates while tuning on this machine: 2 parser processes ≈61 saved documents/s, 4 ≈119, 6 ≈119, and 8 ≈116. The default is 4 processes to avoid additional RAM use for negligible measured gain. These windows crawled different URLs and are not a controlled benchmark; rate varies with pages and site availability. Measurements are saved in `tuning_metrics.json`.

The start script retries pending and transiently failed URLs automatically after each pass, waiting 60 seconds by default. The default global host gap is 0.24 seconds; the shared `39.net` gap is one quarter of that (0.06 seconds). Profiles for repeatedly blocked sources retain their one-second gaps and low error thresholds. The working `ask.39.net` profile uses 16 connections and a 0.06-second gap, with two parser processes on this machine. During the retry interval `state` is `waiting_retry`; `next_retry_at` shows when work resumes. Requests have a 30-second timeout because some reachable sources took longer than 12 seconds. Stop requests also work during the retry wait. `crawl_supervisor.log` records restarts; `crawl_stderr.log` records crawler errors.
Open `watch_crawl.ps1` in PowerShell for a visible progress window; closing that window leaves the background crawler running.

Repair on 2026-10-12: fixed Windows progress-file replacement failures (`WinError 5`) with retrying atomic writes and nonfatal progress publishing; changed the watcher to share delete access, display UTC+7 and identify an exited process; added request pacing across `39.net` subdomains, HTTPS requests for their legacy HTTP links, and an extraction fallback for the explicit article container on `baotayninh.vn`. Verification/challenge pages remain excluded. Backups of the pre-repair scripts, logs and SQLite state are in `repair_backup_20261012`.

Speed tuning on 2026-10-12: tested live connection/gap changes, added host profiles, official mobile question fetching with identity checks, and fixed starvation in the shared-domain scheduler. Repeatedly unavailable sources defer after eight errors with shorter connection waits; those URLs retain retryable status. Source profiles improve output without adding parser processes on this machine's limited free RAM. Backups from before speed tuning are in `speed_backup_20261012`.

Afternoon speed tuning on 2026-10-12: increased the live source and shared-domain limits together, measured 7.4–8.1 saved documents/s with eight connections and 10.92/s over 60 seconds with twelve. The single parser then reached one CPU core's capacity, so the crawler was stopped cleanly and resumed with two parsers and 16 connections for `ask.39.net`. Subsequent 10-second windows saved 15–15.8 documents/s with no new failures at that source. Settings are saved in the launcher and source profiles; measured snapshots/backups are in `speed_tuning_20261012_afternoon`. Earlier observations above refer to their respective tuning stages, not the current default.

The public [dataset repository](https://huggingface.co/datasets/AIGuruTinix/ViBioMIR/tree/main) was checked during repair: it provides URLs and queries, without a pre-extracted corpus download. The local link file is intentionally preserved to keep its ID mapping. Some source websites may remain blocked, offline or redirect to verification; the missing-URL report records that limitation.
