"""Bounded-memory, resumable ViBioMIR crawler. Run: python -u crawl_local.py"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import gzip
import html
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import unicodedata
from collections import Counter, deque
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
import orjson

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR / ".vendor"))
REPO_ID = "AIGuruTinix/ViBioMIR"
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}
TERMINAL_STATUS = {400, 404, 410, 451}
HOST_ERRORS = {"http_403", "http_429", "http_444", "http_521", "TooManyRedirects",
               "http_428", "http_503", "ClientConnectorDNSError", "ClientConnectorSSLError",
               "challenge_page", "javascript_challenge",
               "pubmed_api_invalid",
               "ConnectionTimeoutError", "SocketTimeoutError", "TimeoutError",
               "ClientConnectorError", "ClientConnectorCertificateError", "ServerDisconnectedError"}


def atomic_json(path, value):
    # Windows readers/antivirus can briefly deny replacement. A progress read
    # must never bring down the download pipeline or corrupt the previous JSON.
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=path.name + ".", suffix=".tmp", delete=False) as handle:
        handle.write(orjson.dumps(value, option=orjson.OPT_INDENT_2))
        temporary = Path(handle.name)
    try:
        for attempt in range(12):
            try:
                temporary.replace(path)
                return
            except PermissionError:
                if attempt == 11:
                    raise
                time.sleep(min(0.2, 0.01 * 2 ** attempt))
    finally:
        temporary.unlink(missing_ok=True)


def publish_status(data_dir, stats):
    try:
        atomic_json(data_dir / "crawl_status.json", stats)
        stats.pop("status_write_error", None)
    except OSError as exc:
        stats["status_write_error"] = str(exc)
        print(f"Progress file busy; crawl continues: {exc}", file=sys.stderr, flush=True)


def build_index(links_path, index_dir):
    """Spool URLs by host once, avoiding millions of in-memory Python objects."""
    source = links_path.stat()
    fingerprint = {"bytes": source.st_size, "mtime_ns": source.st_mtime_ns}
    manifest_path = index_dir / "manifest.json"
    if manifest_path.exists():
        manifest = orjson.loads(manifest_path.read_bytes())
        if manifest["source"] == fingerprint and all(
            (index_dir / h["file"]).exists() for h in manifest["hosts"]
        ):
            return manifest
    import pyarrow.parquet as pq

    index_dir.mkdir(parents=True, exist_ok=True)
    handles, hosts = {}, {}
    maximum, count = 0, 0
    parquet = pq.ParquetFile(links_path)
    print(f"Building host index for {parquet.metadata.num_rows:,} URLs...", flush=True)
    try:
        for batch in parquet.iter_batches(batch_size=16384, columns=["id", "url"]):
            buffers = {}
            ids, urls = batch.column(0).to_pylist(), batch.column(1).to_pylist()
            for doc_id, url in zip(ids, urls):
                doc_id = int(doc_id)
                if doc_id < 0:
                    raise ValueError(f"Negative document ID: {doc_id}")
                maximum = max(maximum, doc_id)
                url = str(url or "")
                host = url.split("//", 1)[-1].split("/", 1)[0].lower()
                if host not in handles:
                    filename = hashlib.sha256(host.encode()).hexdigest()[:20] + ".jsonl"
                    handles[host] = (index_dir / filename).open("wb", buffering=262144)
                    hosts[host] = {"host": host, "file": filename, "count": 0}
                buffers.setdefault(host, []).append(orjson.dumps([doc_id, url]) + b"\n")
                hosts[host]["count"] += 1
                count += 1
            for host, lines in buffers.items():
                handles[host].writelines(lines)
            if count % (16384 * 32) == 0:
                print(f"Index: {count:,}/{parquet.metadata.num_rows:,}", flush=True)
    finally:
        for handle in handles.values():
            handle.close()
    manifest = {"source": fingerprint, "total": count, "max_id": maximum,
                "hosts": list(hosts.values())}
    atomic_json(manifest_path, manifest)
    return manifest


def acquire_lock(data_dir):
    handle = (data_dir / ".crawl.lock").open("a+b")
    handle.seek(0)
    if os.name == "nt":
        import msvcrt
        if handle.read(1) == b"":
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            handle.close()
            raise RuntimeError(f"Another crawler is writing to {data_dir}")
    else:
        import fcntl
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return handle


def open_state(data_dir, max_id):
    database = sqlite3.connect(data_dir / "crawl_state.sqlite")
    database.execute("PRAGMA journal_mode=WAL")
    database.execute("PRAGMA synchronous=FULL")
    database.execute("CREATE TABLE IF NOT EXISTS docs (id INTEGER PRIMARY KEY, status INTEGER NOT NULL)")
    database.execute("CREATE TABLE IF NOT EXISTS imported (name TEXT PRIMARY KEY, offset INTEGER NOT NULL)")
    database.execute("CREATE TABLE IF NOT EXISTS host_cursors (host TEXT PRIMARY KEY, source TEXT NOT NULL, offset INTEGER NOT NULL)")
    malformed = 0
    # JSONL is authoritative; DB commits always follow flushing successful records.
    for path in sorted(data_dir.glob("corpus_part_*.jsonl")):
        previous = database.execute("SELECT offset FROM imported WHERE name=?", (path.name,)).fetchone()
        offset = previous[0] if previous and previous[0] <= path.stat().st_size else 0
        if offset == path.stat().st_size:
            continue
        print(f"Indexing saved documents: {path.name}", flush=True)
        with path.open("rb") as handle:
            handle.seek(offset)
            records = []
            for line in handle:
                try:
                    record = orjson.loads(line)
                    doc_id = int(record["doc_id"])
                    if not isinstance(record.get("clean_text"), str) or not record["clean_text"].strip():
                        raise ValueError("Empty document")
                    records.append((doc_id, 1))
                except (orjson.JSONDecodeError, KeyError, ValueError, TypeError):
                    malformed += 1
                if len(records) >= 5000:
                    database.executemany("INSERT OR REPLACE INTO docs VALUES (?,?)", records)
                    records.clear()
            database.executemany("INSERT OR REPLACE INTO docs VALUES (?,?)", records)
            database.execute("INSERT OR REPLACE INTO imported VALUES (?,?)", (path.name, handle.tell()))
            database.commit()
    if malformed:
        print(f"WARNING: {malformed} malformed existing lines were not marked complete.", flush=True)
    flags = bytearray(max_id + 1)
    saved, terminal, transient = 0, 0, 0
    for doc_id, status in database.execute("SELECT id,status FROM docs"):
        if 0 <= doc_id <= max_id:
            flags[doc_id] = status
        saved += status == 1
        terminal += status == 2
        transient += status == 3
    return database, flags, saved, terminal, transient


def parse_many(items):
    """Run CPU-heavy extraction in processes, outside the network event loop."""
    import trafilatura

    results = []
    for doc_id, url, body, encoding in items:
        try:
            if body.startswith(b"%PDF"):
                try:
                    from pypdf import PdfReader
                except ImportError:
                    results.append((doc_id, url, None, "pdf_parser_missing", 3))
                    continue
                import io
                text = "\n".join(page.extract_text() or "" for page in PdfReader(io.BytesIO(body)).pages)
            else:
                try:
                    document = body.decode(encoding) if encoding else body
                except (LookupError, UnicodeDecodeError):
                    document = body
                title = re.search(br"<title[^>]*>(.*?)</title>", body[:131072], re.I | re.S)
                if title and re.search(br"(?:just a moment|one moment,? please|access denied|attention required|captcha)", title[1], re.I):
                    results.append((doc_id, url, None, "challenge_page", 3))
                    continue
                text = trafilatura.extract(document, include_links=False, include_images=False,
                                            favor_recall=True)
                # This site embeds a second HTML document inside the article.
                # Trafilatura returns None although its explicit content container
                # contains the complete article. Restrict fallback to that container.
                if not text and re.match(r"https?://(?:www\.)?baotayninh\.vn/", url):
                    from lxml import html as lxml_html
                    decoded = document.decode("utf-8") if isinstance(document, bytes) else document
                    tree = lxml_html.fromstring(decoded)
                    for node in tree.xpath('//*[@id="content-detail"]'):
                        for unwanted in node.xpath('.//script | .//style | .//nav | .//aside | .//form'):
                            unwanted.drop_tree()
                        candidate = " ".join(node.text_content().split())
                        if len(candidate) >= 100 and len(node.xpath('.//p')) >= 2:
                            text = candidate
                            break
            clean = " ".join(html.unescape(unicodedata.normalize("NFKC", text or "")).split())
            results.append((doc_id, url, clean if len(clean) > 30 else None,
                            None if len(clean) > 30 else "empty_text", 3))
        except Exception as exc:
            results.append((doc_id, url, None, "parse_" + type(exc).__name__, 3))
    return results


@dataclass
class Host:
    name: str
    handle: object
    total: int = 0
    read: int = 0
    active: int = 0
    exhausted: bool = False
    cooldown: float = 0.0
    errors: int = 0
    suspend_reason: str | None = None
    start_offset: int = 0
    wrapped: bool = False
    next_request_at: float = 0.0


HOST_SETTING_LIMITS = {"per_host": (int, 1, 32), "host_interval": (float, 0, 60),
                       "timeout": (float, 1, 120), "connect_timeout": (float, 0.1, 30),
                       "retries": (int, 0, 5), "host_failure_limit": (int, 1, 1000)}


def host_setting(args, host, name, default=None):
    return args.host_settings.get(host.name, {}).get(name, getattr(args, name, default))


def mobile_question_request(url, enabled):
    match = re.fullmatch(r"https?://ask\.39\.net/question/(\d+)\.html(?:\?[^#]*)?", url) if enabled else None
    if match:
        return f"https://wapask.39.net/question/{match[1]}.html", match[1]
    return url, None


def pubmed_api_request(url):
    match = re.fullmatch(r"https?://pubmed\.ncbi\.nlm\.nih\.gov/(\d+)/?", url)
    if not match:
        return url, None
    # Europe PMC is the public structured representation of the same PMID.
    # Keep the original PubMed URL in the saved record and use the API only for
    # fetching the source title/abstract when NCBI blocks direct HTML access.
    pmid = match[1]
    return ("https://www.ebi.ac.uk/europepmc/webservices/rest/search?query="
            f"EXT_ID:{pmid}%20AND%20SRC:MED&format=json&resultType=core&pageSize=1"), pmid


def europe_pmc_html(body, pmid):
    try:
        payload = orjson.loads(body)
        records = payload.get("resultList", {}).get("result", [])
        if not records:
            return None
        record = records[0]
        if str(record.get("pmid", "")) != pmid or record.get("source") != "MED":
            return None
        title = html.escape(str(record.get("title") or ""))
        abstract_source = str(record.get("abstractText") or "")
        if "<" in abstract_source:
            from lxml import html as lxml_html
            fragment = lxml_html.fragment_fromstring(abstract_source, create_parent="div")
            for node in fragment.xpath('.//script | .//style'):
                node.drop_tree()
            for node in fragment.xpath('.//h1 | .//h2 | .//h3 | .//h4 | .//p | .//div | .//li | .//br'):
                node.tail = ' ' + (node.tail or '')
            abstract_source = fragment.text_content()
        abstract = html.escape(abstract_source)
        if len(re.sub(r"\s+", " ", abstract).strip()) < 30:
            return None
        return (f"<html><head><title>{title}</title></head><body><article>"
                f"<h1>{title}</h1><p>{abstract}</p></article></body></html>").encode("utf-8")
    except (orjson.JSONDecodeError, AttributeError, TypeError, ValueError):
        return None


def europe_pmc_results(body, items):
    """Match every original document to its exact MED PMID; never fill gaps."""
    try:
        payload = orjson.loads(body)
        records = payload["resultList"]["result"]
        if not isinstance(records, list):
            raise ValueError("Invalid Europe PMC result list")
        by_pmid = {}
        for record in records:
            pmid = str(record.get("pmid", ""))
            if record.get("source") == "MED":
                if pmid in by_pmid:
                    raise ValueError("Ambiguous PMID returned")
                by_pmid[pmid] = record
    except (orjson.JSONDecodeError, KeyError, AttributeError, TypeError, ValueError):
        return [(doc_id, url, None, "pubmed_api_invalid", 3) for doc_id, url in items]
    results = []
    for doc_id, url in items:
        _, pmid = pubmed_api_request(url)
        record = by_pmid.get(pmid)
        converted = europe_pmc_html(orjson.dumps({"resultList": {"result": [record]}}), pmid) if record else None
        results.append((doc_id, url, converted, "utf-8") if converted else
                       (doc_id, url, None, "pubmed_missing_abstract", 3))
    return results


async def fetch_pubmed_batch(session, items, host, args, stats):
    pmids = [pubmed_api_request(url)[1] for _, url in items]
    if not all(pmids):
        raise ValueError("Batch contains a non-PubMed URL")
    timeout_seconds = host_setting(args, host, "timeout")
    timeout = aiohttp.ClientTimeout(total=timeout_seconds, connect=min(8, timeout_seconds))
    params = {"query": "EXT_ID:(" + " OR ".join(pmids) + ") AND SRC:MED",
              "format": "json", "resultType": "core", "pageSize": str(len(set(pmids)))}
    error = "pubmed_api_unavailable"
    for attempt in range(host_setting(args, host, "retries") + 1):
        if attempt:
            if time.monotonic() < host.cooldown:
                break
            stats["retries"] += 1
            await asyncio.sleep(1)
        try:
            async with session.get("https://www.ebi.ac.uk/europepmc/webservices/rest/search", params=params, timeout=timeout) as response:
                if response.status == 200:
                    body = bytearray()
                    async for piece in response.content.iter_chunked(65536):
                        body.extend(piece)
                        if len(body) > args.max_page_mb * 1024 * 1024:
                            break
                    if len(body) > args.max_page_mb * 1024 * 1024:
                        error = "page_too_large"
                        break
                    stats["download_bytes"] += len(body)
                    results = europe_pmc_results(body, items)
                    if hasattr(args, 'data_dir'):
                        # Preserve the received API text for source verification.
                        checksum = hashlib.sha256(body).hexdigest()
                        cache = args.data_dir / 'pubmed_api_cache'
                        cache.mkdir(exist_ok=True)
                        cached = cache / (checksum + '.json.gz')
                        if not cached.exists():
                            with gzip.open(cached, 'wb') as evidence:
                                evidence.write(body)
                        receipt = {'time': datetime.now(timezone.utc).isoformat(),
                                   'api_url': str(response.url), 'source': 'Europe PMC MED/PubMed',
                                   'requested_documents': [{'doc_id': doc_id, 'url': url} for doc_id,url in items],
                                   'response_sha256': checksum, 'response_file': str(cached.relative_to(args.data_dir))}
                        receipt['response_json_valid'] = not all(r[2] is None and r[3] == 'pubmed_api_invalid' for r in results)
                        with (args.data_dir / 'pubmed_api_provenance.jsonl').open('ab') as provenance:
                            provenance.write(orjson.dumps(receipt) + b'\n')
                    return results
                error = f"http_{response.status}"
                if response.status == 429:
                    try:
                        pause = max(30, float(response.headers.get("Retry-After", "30")))
                    except ValueError:
                        pause = 60
                    host.cooldown = max(host.cooldown, time.monotonic() + pause)
                    break
                if response.status not in RETRY_STATUS:
                    break
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            error = type(exc).__name__
    return [(doc_id, url, None, error, 3) for doc_id, url in items]


def verified_mobile_question(body, question_id):
    # The official mobile page embeds the question ID in a hidden input.
    # Never associate a redirect/homepage/other question with the original ID.
    return bool(re.search(rb'<input\b[^>]*\bid=["\']tid(?:_gla)?["\'][^>]*\bvalue=["\']' +
                          question_id.encode("ascii") + rb'["\']', body, re.I))


async def fetch(session, item, host, args, stats):
    from crawl_recovery import supported
    if supported(host.name) and getattr(args, 'recovery_client', None) is not None:
        return await args.recovery_client.fetch(item, host, args, stats, host_setting)
    doc_id, url = item
    request_url = "https://" + url[7:] if url.startswith("http://") and host.name.endswith(".39.net") else url
    request_url, mobile_question = mobile_question_request(request_url, host_setting(args, host, "prefer_mobile", False))
    request_url, pubmed_id = pubmed_api_request(request_url)
    final_error, final_status = "network_error", 3
    timeout_seconds = host_setting(args, host, "timeout")
    connect_seconds = min(timeout_seconds, host_setting(args, host, "connect_timeout", min(10, timeout_seconds)))
    timeout = aiohttp.ClientTimeout(total=timeout_seconds, connect=connect_seconds, sock_read=timeout_seconds)
    for attempt in range(host_setting(args, host, "retries") + 1):
        if attempt:
            if host.suspend_reason or time.monotonic() < host.cooldown:
                break
            stats["retries"] += 1
            await asyncio.sleep(min(4.0, 0.5 * 2 ** (attempt - 1)))
        try:
            async with session.get(request_url, timeout=timeout) as response:
                status = response.status
                if status != 200:
                    final_error = f"http_{status}"
                    # A missing mobile representation does not prove the
                    # original desktop source is permanently missing.
                    final_status = 2 if status in TERMINAL_STATUS and not mobile_question else 3
                    if status == 429:
                        try:
                            retry_after = response.headers.get("Retry-After", "5")
                            pause = max(2.0, float(retry_after))
                        except ValueError:
                            from email.utils import parsedate_to_datetime
                            try:
                                pause = max(2.0, parsedate_to_datetime(retry_after).timestamp() - time.time())
                            except (ValueError, TypeError, OverflowError):
                                pause = 5.0
                        host.cooldown = max(host.cooldown, time.monotonic() + pause)
                        if pause > 60:
                            host.suspend_reason = "http_429_long_retry_after"
                        break
                    if status not in RETRY_STATUS:
                        break
                    continue
                # Observed redirect from 39.net articles to a verification page.
                # This response has no article text; do not send it to CPU workers.
                if response.url.host == "image.39.net" and response.url.path == "/verify.html":
                    return (doc_id, url, None, "challenge_page", 3)
                if response.content_type.startswith(("image/", "video/", "audio/")):
                    return (doc_id, url, None, "unsupported_media", 3)
                body = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    body.extend(chunk)
                    if len(body) > args.max_page_mb * 1024 * 1024:
                        return (doc_id, url, None, "page_too_large", 3)
                stats["download_bytes"] += len(body)
                if pubmed_id:
                    converted = europe_pmc_html(bytes(body), pubmed_id)
                    if converted is None:
                        return (doc_id, url, None, "pubmed_empty_or_mismatch", 3)
                    body = bytearray(converted)
                title = re.search(br"<title[^>]*>(.*?)</title>", body[:131072], re.I | re.S)
                if title and re.search(br"(?:just a moment|one moment,? please|access denied|attention required|captcha)", title[1], re.I):
                    return (doc_id, url, None, "challenge_page", 3)
                if len(body) < 2048 and b"document.cookie=" in body and b"window.location.reload" in body:
                    without_scripts = re.sub(br"<script\b[^>]*>.*?</script>", b"", body, flags=re.I | re.S)
                    if not re.sub(br"<[^>]+>", b"", without_scripts).strip():
                        return (doc_id, url, None, "javascript_challenge", 3)
                if mobile_question and (response.url.host not in {"wapask.39.net", "ask.39.net"} or
                                        response.url.path != f"/question/{mobile_question}.html" or
                                        not verified_mobile_question(body, mobile_question)):
                    return (doc_id, url, None, "mobile_question_mismatch", 3)
                return (doc_id, url, bytes(body), response.charset)
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            final_error = type(exc).__name__
    return (doc_id, url, None, final_error, final_status)


async def download_loop(manifest, index_dir, flags, args, html_queue, output_queue, stats, database):
    all_hosts = [Host(h["host"], (index_dir / h["file"]).open("rb", buffering=32768), h["count"])
                 for h in manifest["hosts"]]
    hosts = deque(all_hosts)
    source_key = hashlib.sha256(orjson.dumps(manifest["source"])).hexdigest()
    cursors = dict(database.execute("SELECT host,offset FROM host_cursors WHERE source=?", (source_key,)))
    for host in all_hosts:
        offset = cursors.get(host.name, 0)
        if 0 < offset < os.fstat(host.handle.fileno()).st_size:
            host.start_offset = offset
            host.handle.seek(offset)
    # Scheduling enforces the live limits. Bound redirects sharing one destination.
    connector = aiohttp.TCPConnector(limit=1024, limit_per_host=32,
                                     ttl_dns_cache=300, resolver=aiohttp.ThreadedResolver())
    timeout = aiohttp.ClientTimeout(total=args.timeout, connect=min(10, args.timeout), sock_read=args.timeout)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36",
               "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8"}
    pending = {}
    seen = bytearray(len(flags))
    scheduled = 0
    family_next_request = {}

    def checkpoint_hosts():
        database.executemany("INSERT OR REPLACE INTO host_cursors VALUES (?,?,?)",
                             [(h.name, source_key, h.handle.tell()) for h in all_hosts])
        database.commit()

    last_checkpoint = time.monotonic()

    def next_ready_delay():
        if len(pending) >= args.concurrency:
            return 0.2
        deadlines = []
        for host in hosts:
            if host.exhausted or host.active >= host_setting(args, host, "per_host"):
                continue
            family = "39.net" if host.name.endswith(".39.net") else host.name
            deadlines.append(max(host.cooldown, host.next_request_at, family_next_request.get(family, 0)))
        return min(0.2, max(0.005, min(deadlines) - time.monotonic())) if deadlines else 0.2

    async def collect(finished):
        for task in finished:
            host = pending.pop(task)
            host.active -= 1
            received = task.result()
            for result in received if isinstance(received, list) else [received]:
                stats["downloaded"] += 1
                if result[2] is not None:
                    host.errors = 0
                    await html_queue.put(result)
                else:
                    host.errors += 1
                    if host.errors >= 8 and result[3] in {"http_403", "http_429"}:
                        host.cooldown = max(host.cooldown, time.monotonic() + 30)
                    if host.suspend_reason or (host.errors >= host_setting(args, host, "host_failure_limit") and result[3] in HOST_ERRORS):
                        host.exhausted = True
                        stats["deferred_hosts"][host.name] = {
                            "reason": host.suspend_reason or result[3],
                            "remaining_index_rows": host.total - host.read,
                            "retry_after_seconds": round(max(0, host.cooldown - time.monotonic()))}
                    await output_queue.put(result)

    try:
        from crawl_recovery import RecoveryClient
        async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers,
                                         trust_env=True) as session, RecoveryClient() as recovery_client:
            args.recovery_client = recovery_client
            while hosts or pending:
                made_progress = False
                for _ in range(len(hosts)):
                    host = hosts.popleft()
                    if host.exhausted:
                        if host.active:
                            hosts.append(host)
                        continue
                    hosts.append(host)
                    if len(pending) >= args.concurrency or args.stop_requested or (args.limit and scheduled >= args.limit):
                        break
                    family = "39.net" if host.name.endswith(".39.net") else host.name
                    now = time.monotonic()
                    per_host = host_setting(args, host, "per_host")
                    host_interval = host_setting(args, host, "host_interval")
                    if host.active >= per_host or now < max(host.cooldown, host.next_request_at, family_next_request.get(family, 0)):
                        continue
                    host_scheduled = False
                    for _ in range(2048):
                        if host.wrapped and host.handle.tell() >= host.start_offset:
                            host.exhausted = True
                            break
                        line = host.handle.readline()
                        if not line:
                            if host.start_offset and not host.wrapped:
                                host.handle.seek(0)
                                host.wrapped = True
                                continue
                            host.exhausted = True
                            break
                        host.read += 1
                        doc_id, url = orjson.loads(line)
                        if seen[doc_id] or flags[doc_id] == 1 or (flags[doc_id] == 2 and not args.retry_failed):
                            continue
                        seen[doc_id] = 1
                        items = [(doc_id, url)]
                        if host.name == "pubmed.ncbi.nlm.nih.gov":
                            for _ in range(2048):
                                if len(items) >= 50 or (args.limit and scheduled + len(items) >= args.limit):
                                    break
                                if host.wrapped and host.handle.tell() >= host.start_offset:
                                    host.exhausted = True
                                    break
                                following = host.handle.readline()
                                if not following:
                                    # The next scheduler visit handles wrap-around.
                                    break
                                host.read += 1
                                next_id, next_url = orjson.loads(following)
                                if seen[next_id] or flags[next_id] == 1 or (flags[next_id] == 2 and not args.retry_failed):
                                    continue
                                seen[next_id] = 1
                                items.append((next_id, next_url))
                            task = asyncio.create_task(fetch_pubmed_batch(session, items, host, args, stats))
                        else:
                            task = asyncio.create_task(fetch(session, items[0], host, args, stats))
                        pending[task] = host
                        host.active += 1
                        host.next_request_at = now + host_interval
                        family_next_request[family] = now + (args.host_interval / 4 if family == "39.net" else host_interval)
                        scheduled += len(items)
                        stats["scheduled"] = scheduled
                        made_progress = True
                        host_scheduled = True
                        break
                    await asyncio.sleep(0)
                    # Preserve the deque position after serving a shared family.
                    # Otherwise each full scan restarts at the same hosts, letting
                    # early 39.net subdomains consume every slot and starve ask.
                    if host_scheduled and family == "39.net":
                        break
                stats["active_requests"] = len(pending)
                stats["active_requests_by_host"] = dict(Counter(h.name for h in pending.values()))
                now = time.monotonic()
                cooling_hosts = {h.name: round(max(0, h.cooldown - now), 1) for h in hosts
                                 if not h.exhausted and h.cooldown > now}
                stats["cooling_hosts"] = cooling_hosts
                if args.stop_requested:
                    stats["activity"] = "draining"
                elif not pending and cooling_hosts:
                    stats["activity"] = "waiting_host_cooldown"
                else:
                    stats["activity"] = "downloading"
                if time.monotonic() - last_checkpoint >= 30:
                    checkpoint_hosts()
                    last_checkpoint = time.monotonic()
                if args.stop_requested or (args.limit and scheduled >= args.limit):
                    if pending:
                        done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                        await collect(done)
                        continue
                    break
                if pending:
                    done = {t for t in pending if t.done()}
                    if not done and (len(pending) >= args.concurrency or not made_progress):
                        done, _ = await asyncio.wait(pending, timeout=next_ready_delay(), return_when=asyncio.FIRST_COMPLETED)
                    await collect(done)
                elif not made_progress and hosts:
                    await asyncio.sleep(next_ready_delay())
    finally:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        checkpoint_hosts()
        for host in all_hosts:
            host.handle.close()
        stats["active_requests"] = 0
        stats["active_requests_by_host"] = {}
        stats["cooling_hosts"] = {}
        stats["activity"] = "downloads_finished"


async def parser_loop(html_queue, output_queue, executor):
    loop = asyncio.get_running_loop()
    while True:
        first = await html_queue.get()
        if first is None:
            html_queue.task_done()
            return
        batch = [first]
        while len(batch) < 4:
            try:
                item = html_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is None:
                html_queue.task_done()
                await html_queue.put(None)
                break
            batch.append(item)
        results = await loop.run_in_executor(executor, parse_many, batch)
        for result in results:
            await output_queue.put(result)
        for _ in batch:
            html_queue.task_done()


async def writer_loop(data_dir, database, flags, output_queue, args, stats):
    indices = [int(p.stem.rsplit("_", 1)[1]) for p in data_dir.glob("corpus_part_*.jsonl")]
    part = max(indices, default=-1) + 1
    out = None
    count = 0
    tracking = (data_dir / "crawled_ids.txt").open("ab", buffering=262144)
    failures = (data_dir / "crawl_failures.jsonl").open("ab", buffering=262144)
    rows = []
    last_flush = time.monotonic()

    def flush():
        nonlocal last_flush
        if out:
            out.flush()
            os.fsync(out.fileno())
        tracking.flush()
        failures.flush()
        database.executemany("INSERT OR REPLACE INTO docs VALUES (?,?)", rows)
        if out:
            database.execute("INSERT OR REPLACE INTO imported VALUES (?,?)", (Path(out.name).name, out.tell()))
        database.commit()
        rows.clear()
        last_flush = time.monotonic()

    try:
        while True:
            try:
                item = await asyncio.wait_for(output_queue.get(), timeout=1)
            except asyncio.TimeoutError:
                flush()
                if shutil.disk_usage(data_dir).free < args.min_free_gb * 1024 ** 3:
                    raise RuntimeError("Free disk space below configured minimum")
                continue
            if item is None:
                output_queue.task_done()
                break
            doc_id, url, text, error, status = item
            if text:
                if out is None:
                    out = (data_dir / f"corpus_part_{part:04d}.jsonl").open("ab", buffering=1048576)
                record = {"doc_id": doc_id, "url": url, "clean_text": text}
                out.write(orjson.dumps(record) + b"\n")
                tracking.write(str(doc_id).encode() + b"\n")
                rows.append((doc_id, 1))
                flags[doc_id] = 1
                stats["saved"] += 1
                stats["saved_by_host"][url.split("//", 1)[-1].split("/", 1)[0].lower()] += 1
                count += 1
                if count >= args.shard_size:
                    flush()
                    out.close()
                    out = None
                    part += 1
                    count = 0
            else:
                failures.write(orjson.dumps({"doc_id": doc_id, "url": url, "error": error,
                                             "status": status, "time": datetime.now(timezone.utc).isoformat()}) + b"\n")
                rows.append((doc_id, status))
                flags[doc_id] = status
                stats["failed"] += 1
                stats["failed_by_host"][url.split("//", 1)[-1].split("/", 1)[0].lower()] += 1
                stats["errors"][error] += 1
            stats["processed"] += 1
            output_queue.task_done()
            if len(rows) >= 1000 or time.monotonic() - last_flush >= 1:
                flush()
                if shutil.disk_usage(data_dir).free < args.min_free_gb * 1024 ** 3:
                    raise RuntimeError("Free disk space below configured minimum")
    finally:
        flush()
        if out:
            out.close()
        tracking.close()
        failures.close()


def apply_control(args, stats):
    path = args.data_dir / "crawl_control.json"
    if not path.exists():
        return
    try:
        control = orjson.loads(path.read_bytes().removeprefix(b"\xef\xbb\xbf"))
        for name, minimum, maximum in [("concurrency", 1, 1024), ("per_host", 1, 32)]:
            if name in control:
                value = int(control[name])
                if not minimum <= value <= maximum:
                    raise ValueError(f"{name} must be {minimum}..{maximum}")
                setattr(args, name, value)
                stats["settings"][name] = value
        if "host_interval" in control:
            value = float(control["host_interval"])
            if not 0 <= value <= 60:
                raise ValueError("host_interval must be 0..60 seconds")
            args.host_interval = value
            stats["settings"]["host_interval"] = value
        if "hosts" in control:
            if not isinstance(control["hosts"], dict):
                raise ValueError("hosts must map host names to settings")
            profiles = {}
            for hostname, profile in control["hosts"].items():
                if not isinstance(profile, dict):
                    raise ValueError(f"Invalid settings for {hostname}")
                checked = {}
                for name, value in profile.items():
                    if name == "prefer_mobile":
                        if hostname.lower() != "ask.39.net" or type(value) is not bool:
                            raise ValueError("prefer_mobile is a boolean supported only for ask.39.net")
                        checked[name] = value
                        continue
                    if name not in HOST_SETTING_LIMITS:
                        raise ValueError(f"Unsupported host setting: {name}")
                    convert, minimum, maximum = HOST_SETTING_LIMITS[name]
                    value = convert(value)
                    if not minimum <= value <= maximum:
                        raise ValueError(f"{hostname}: {name} must be {minimum}..{maximum}")
                    checked[name] = value
                profiles[hostname.lower()] = checked
            args.host_settings = profiles
            stats["settings"]["hosts"] = profiles
        if control.get("stop") is True:
            args.stop_requested = True
            stats["state"] = "stopping"
        stats.pop("control_error", None)
    except (ValueError, TypeError, OSError) as exc:
        stats["control_error"] = str(exc)


async def reporter(data_dir, stats, stop, args, html_queue, output_queue):
    start = time.monotonic()
    previous, previous_time = 0, start
    previous_saved = previous_download_bytes = 0
    while True:
        if not stop.is_set():
            apply_control(args, stats)
        elapsed = max(0.001, time.monotonic() - start)
        interval = max(0.001, time.monotonic() - previous_time)
        stats["elapsed_seconds"] = round(elapsed, 1)
        stats["urls_per_second"] = round(stats["processed"] / elapsed, 2)
        stats["recent_urls_per_second"] = round((stats["processed"] - previous) / interval, 2)
        stats["recent_saved_per_second"] = round((stats["saved"] - previous_saved) / interval, 2)
        stats["recent_download_mb_per_second"] = round((stats["download_bytes"] - previous_download_bytes) / interval / 1048576, 2)
        stats["html_queue_size"] = html_queue.qsize()
        stats["output_queue_size"] = output_queue.qsize()
        stats["corpus_saved"] = stats["existing_saved"] + stats["saved"]
        stats["disk_free_gb"] = round(shutil.disk_usage(data_dir).free / 1024 ** 3, 2)
        stats["updated_at"] = datetime.now(timezone.utc).isoformat()
        publish_status(data_dir, stats)
        print(f"Processed {stats['processed']:,} | saved +{stats['saved']:,} | failed {stats['failed']:,} | "
              f"{stats['recent_urls_per_second']:.1f} URL/s | active {stats['active_requests']} | "
              f"{stats['recent_saved_per_second']:.1f} saved/s | HTML queue {html_queue.qsize()}", flush=True)
        previous, previous_time = stats["processed"], time.monotonic()
        previous_saved, previous_download_bytes = stats["saved"], stats["download_bytes"]
        if stop.is_set():
            return
        try:
            await asyncio.wait_for(stop.wait(), timeout=10)
        except asyncio.TimeoutError:
            pass


async def run(args, database, flags, manifest, saved, terminal, transient):
    data_dir = args.data_dir
    html_queue = asyncio.Queue(maxsize=args.parse_workers * 8)
    output_queue = asyncio.Queue(maxsize=2000)
    stop = asyncio.Event()
    stats = {"pid": os.getpid(), "state": "running", "total_urls": manifest["total"],
             "existing_saved": saved, "existing_terminal_failures": terminal,
             "pass_number": args.pass_number,
             "existing_retryable_failures": transient, "scheduled": 0, "downloaded": 0,
             "saved": 0, "failed": 0, "processed": 0, "retries": 0,
             "saved_by_host": Counter(), "failed_by_host": Counter(),
             "download_bytes": 0, "active_requests": 0, "errors": Counter(),
             "deferred_hosts": {},
             "settings": {"concurrency": args.concurrency, "per_host": args.per_host,
                          "parse_workers": args.parse_workers, "timeout": args.timeout,
                          "host_interval": args.host_interval}}
    executor = ProcessPoolExecutor(max_workers=args.parse_workers)
    writer = asyncio.create_task(writer_loop(data_dir, database, flags, output_queue, args, stats))
    parsers = [asyncio.create_task(parser_loop(html_queue, output_queue, executor)) for _ in range(args.parse_workers)]
    reporting = asyncio.create_task(reporter(data_dir, stats, stop, args, html_queue, output_queue))

    async def pipeline():
        await download_loop(manifest, args.index_dir, flags, args, html_queue, output_queue, stats, database)
        for _ in parsers:
            await html_queue.put(None)
        await asyncio.gather(*parsers)
        await output_queue.put(None)
        await writer

    work = asyncio.create_task(pipeline())
    try:
        # Surface worker errors immediately, even when a bounded queue is full.
        watched = {work, writer, reporting, *parsers}
        while not work.done():
            done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task.exception():
                    raise task.exception()
            watched.difference_update(done)
        await work
        stats["state"] = "stopped" if args.stop_requested else "sample_complete" if args.limit else (
            "pass_complete_with_deferred_hosts" if stats["deferred_hosts"] else "pass_complete")
    except BaseException as exc:
        stats["state"] = "interrupted" if isinstance(exc, asyncio.CancelledError) else "error"
        stats["last_error"] = str(exc)
        for task in [work, *parsers]:
            task.cancel()
        await asyncio.gather(work, *parsers, return_exceptions=True)
        if not writer.done():
            await output_queue.put(None)
            await writer
        raise
    finally:
        stop.set()
        await reporting
        executor.shutdown(wait=True, cancel_futures=True)
    return stats


def finalize_corpus(args, database, manifest, stats, reason):
    """Publish an honest inventory and retry list; leave every corpus shard intact."""
    flags = bytearray(manifest["max_id"] + 1)
    for doc_id, status in database.execute("SELECT id,status FROM docs"):
        if 0 <= doc_id < len(flags):
            flags[doc_id] = status
    counts = Counter()
    hosts = []
    shard_files = [{"file": p.name, "bytes": p.stat().st_size}
                   for p in sorted(args.data_dir.glob("corpus_part_*.jsonl"))]
    missing_path = args.data_dir / "missing_urls.jsonl"
    # Not a live progress file; the writer lock protects this final inventory.
    temporary = missing_path.with_suffix(".jsonl.building")
    seen = bytearray(len(flags))
    with temporary.open("wb", buffering=1048576) as output:
        for entry in manifest["hosts"]:
            host_counts = Counter()
            with (args.index_dir / entry["file"]).open("rb") as source:
                for line in source:
                    doc_id, url = orjson.loads(line)
                    if seen[doc_id]:
                        continue
                    seen[doc_id] = 1
                    label = {0: "unvisited", 1: "saved", 2: "terminal", 3: "retryable"}[flags[doc_id]]
                    host_counts[label] += 1
                    counts[label] += 1
                    if label != "saved":
                        output.write(orjson.dumps({"doc_id": doc_id, "url": url, "status": label}) + b"\n")
            hosts.append({"host": entry["host"], **host_counts})
    temporary.replace(missing_path)
    inventory = {"created_at": datetime.now(timezone.utc).isoformat(), "reason": reason,
                 "complete": counts["saved"] == sum(counts.values()),
                 "total_unique_ids": sum(counts.values()), "counts": dict(counts),
                 "max_id": manifest["max_id"], "source_links": str(args.links.resolve()),
                 "source_fingerprint": manifest["source"],
                 "schema": ["doc_id", "url", "clean_text"], "shards": shard_files,
                 "missing_urls": missing_path.name, "hosts": hosts}
    atomic_json(args.data_dir / "corpus_manifest.json", inventory)
    stats.update(corpus_saved=counts["saved"], remaining_urls=sum(counts.values()) - counts["saved"],
                 completion_reason=reason, corpus_manifest="corpus_manifest.json",
                 updated_at=datetime.now(timezone.utc).isoformat())
    if reason in {"no_progress", "max_passes"}:
        stats["state"] = "finished_with_missing"
    publish_status(args.data_dir, stats)
    print(f"Corpus inventory: {counts['saved']:,} saved; {stats['remaining_urls']:,} missing. Reason: {reason}.", flush=True)


def main():
    import psutil

    defaults = min(2, max(1, int(psutil.virtual_memory().available // (384 * 1024 ** 2))))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=BASE_DIR / "corpus_data_vi_en_zh")
    parser.add_argument("--links", type=Path, default=BASE_DIR / "links_corpus_vi_en_zh.parquet")
    parser.add_argument("--index-dir", type=Path, default=BASE_DIR / ".cache" / "crawl_index_vi_en_zh")
    parser.add_argument("--concurrency", type=int, default=256)
    parser.add_argument("--per-host", type=int, default=1)
    parser.add_argument("--host-interval", type=float, default=0.24, help="Minimum seconds between requests to one host; shared 39.net gap is one quarter")
    parser.add_argument("--parse-workers", type=int, default=defaults)
    parser.add_argument("--timeout", type=float, default=12)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--repeat", action="store_true", help="Retry pending/transient URLs after each pass")
    parser.add_argument("--retry-delay", type=float, default=120, help="Seconds between passes in repeat mode")
    parser.add_argument("--no-progress-passes", type=int, default=3, help="Finish with a missing-URL report after this many passes save nothing")
    parser.add_argument("--max-passes", type=int, default=10, help="Maximum repeat passes per run; 0 = no pass limit")
    parser.add_argument("--host-failure-limit", type=int, default=32,
                        help="Defer a host after consecutive connection/block errors; retry next run")
    parser.add_argument("--limit", type=int, default=0, help="0 = all pending URLs")
    parser.add_argument("--retry-failed", action="store_true", help="Also retry previously terminal failures")
    parser.add_argument("--shard-size", type=int, default=50000)
    parser.add_argument("--max-page-mb", type=int, default=4)
    parser.add_argument("--min-free-gb", type=float, default=2)
    args = parser.parse_args()
    args.stop_requested = False
    args.host_settings = {}
    if any(v < 1 for v in [args.concurrency, args.per_host, args.parse_workers, args.shard_size,
                          args.max_page_mb, args.host_failure_limit]):
        parser.error("Concurrency, workers, shard size and page size must be positive")
    if args.timeout <= 0 or args.retries < 0 or args.limit < 0 or args.min_free_gb < 0 or args.retry_delay < 1 or args.host_interval < 0 or args.no_progress_passes < 1 or args.max_passes < 0:
        parser.error("Invalid timeout, retries, limit or free-space threshold")
    if args.repeat and args.limit:
        parser.error("--repeat cannot be combined with --limit")
    args.data_dir = args.data_dir.resolve()
    args.data_dir.mkdir(parents=True, exist_ok=True)
    lock = acquire_lock(args.data_dir)
    database = None
    try:
        if shutil.disk_usage(args.data_dir).free < args.min_free_gb * 1024 ** 3:
            raise RuntimeError("Insufficient disk space")
        if not args.links.exists():
            from huggingface_hub import hf_hub_download
            if args.links.name != "links_corpus_vi_en_zh.parquet":
                raise FileNotFoundError(args.links)
            hf_hub_download(REPO_ID, filename=args.links.name, repo_type="dataset", local_dir=str(args.links.parent))
        manifest = build_index(args.links, args.index_dir)
        pass_number = 0
        no_progress = 0
        while True:
            pass_number += 1
            args.pass_number = pass_number
            database, flags, saved, terminal, transient = open_state(args.data_dir, manifest["max_id"])
            print(f"Pass {pass_number}: URLs: {manifest['total']:,}; already saved: {saved:,}; "
                  f"terminal failures: {terminal:,}; parallel requests: {args.concurrency}; "
                  f"parser processes: {args.parse_workers}", flush=True)
            parser_crashed = False
            try:
                stats = asyncio.run(run(args, database, flags, manifest, saved, terminal, transient))
            except BrokenProcessPool:
                if not args.repeat:
                    raise
                parser_crashed = True
                stats = orjson.loads((args.data_dir / "crawl_status.json").read_bytes())
                args.parse_workers = max(1, args.parse_workers // 2)
                args.concurrency = min(args.concurrency, 96)
                control_path = args.data_dir / "crawl_control.json"
                control = orjson.loads(control_path.read_bytes().removeprefix(b"\xef\xbb\xbf")) if control_path.exists() else {}
                control["concurrency"] = args.concurrency
                atomic_json(control_path, control)
                print(f"Parser process crashed. Resuming saved IDs after {args.retry_delay:g}s "
                      f"with {args.parse_workers} parser(s) and {args.concurrency} requests.", flush=True)
            retryable = database.execute("SELECT count(*) FROM docs WHERE status=3").fetchone()[0]
            no_progress = no_progress + 1 if not stats["saved"] and not parser_crashed else 0
            stats["pass_number"] = pass_number
            stats["no_progress_passes"] = no_progress
            reason = None
            if args.stop_requested:
                reason = "stopped"
            elif not args.repeat:
                reason = "sample" if args.limit else "single_pass"
            elif not parser_crashed and not stats["deferred_hosts"] and not retryable:
                reason = "all_resolved"
            elif no_progress >= args.no_progress_passes:
                reason = "no_progress"
            elif args.max_passes and pass_number >= args.max_passes:
                reason = "max_passes"
            if reason:
                if not args.limit:
                    finalize_corpus(args, database, manifest, stats, reason)
                break
            database.close()
            database = None
            stats["state"] = "waiting_retry"
            stats["activity"] = "waiting_between_passes"
            stats["pass_number"] = pass_number
            stats["recent_saved_per_second"] = 0
            stats["recent_urls_per_second"] = 0
            stats["next_retry_at"] = datetime.fromtimestamp(time.time() + args.retry_delay, timezone.utc).isoformat()
            print(f"Pending sources remain. Retrying in {args.retry_delay:g} seconds.", flush=True)
            deadline = time.monotonic() + args.retry_delay
            while time.monotonic() < deadline:
                apply_control(args, stats)
                if args.stop_requested:
                    stats["state"] = "stopped"
                stats["updated_at"] = datetime.now(timezone.utc).isoformat()
                publish_status(args.data_dir, stats)
                if args.stop_requested:
                    break
                time.sleep(min(1, max(0, deadline - time.monotonic())))
            if args.stop_requested:
                database, flags, saved, terminal, transient = open_state(args.data_dir, manifest["max_id"])
                finalize_corpus(args, database, manifest, stats, "stopped")
                break
    finally:
        if database:
            database.close()
        lock.close()


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    main()
