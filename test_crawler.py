"""Integration checks: extraction, retries, ID deduplication, shards and resume."""
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from unittest.mock import patch
import asyncio
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
REQUESTS = Counter()
BLOCKED = True
TEXT = "Thông tin y học: Bệnh nhân cần được bác sĩ khám và tư vấn để lựa chọn phương pháp điều trị phù hợp. "


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        REQUESTS[self.path] += 1
        if self.path == "/rate-limited":
            self.send_response(429)
            self.send_header("Retry-After", "2")
            self.end_headers()
            return
        if self.path == "/slow":
            time.sleep(0.2)
        if self.path == "/blocked" and BLOCKED:
            self.send_response(403)
            self.end_headers()
            return
        if self.path in {"/blocked-prefix/0", "/blocked-prefix/1"}:
            self.send_response(403)
            self.end_headers()
            return
        if self.path == "/404":
            self.send_response(404)
            self.end_headers()
            return
        if self.path == "/503" or (self.path == "/flaky" and REQUESTS[self.path] == 1):
            self.send_response(503)
            self.end_headers()
            return
        if self.path == "/javascript-only":
            body = b'<html><body><script>document.cookie="test=1";window.location.reload(true);</script></body></html>'
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
            return
        title = "Just a moment" if self.path == "/challenge" else "One moment, please..." if self.path == "/new-challenge" else "Thông tin y học"
        body = (f"<html><head><title>{title}</title></head><body><article><h1>{title}</h1>"
                f"<p>{TEXT * 12}</p><p>{TEXT * 5}</p></article></body></html>").encode()
        if self.path == "/article-with-javascript":
            body = (f'<html><body><article><h1>{title}</h1><p>{TEXT * 3}</p></article>'
                    '<script>document.cookie="test=1";window.location.reload(true);</script></body></html>').encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    global BLOCKED
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    with tempfile.TemporaryDirectory(prefix="crawler-check-", dir=ROOT) as directory:
        root = Path(directory)
        import crawl_local as crawler
        target = root / "atomic.json"
        target.write_text('{"old":true}')
        original_replace = Path.replace
        calls = []
        def busy_then_replace(path, destination):
            calls.append(path)
            if len(calls) < 3:
                raise PermissionError("Windows sharing violation")
            return original_replace(path, destination)
        with patch.object(Path, "replace", busy_then_replace):
            crawler.atomic_json(target, {"new": True})
        assert len(calls) == 3 and json.loads(target.read_text()) == {"new": True}
        assert not list(root.glob("atomic.json.*.tmp"))
        with patch.object(crawler, "atomic_json", side_effect=PermissionError("Busy progress reader")):
            progress = {}
            crawler.publish_status(root, progress)
        assert "status_write_error" in progress
        # Explicit article content survives malformed nested HTML; navigation
        # cannot become a saved document through the source-specific fallback.
        nested_html = ('<html><body><nav>Trang chu Tin tuc</nav><div id="content-detail">'
                       '<!DOCTYPE html><html><body><p>' + TEXT * 3 + '</p><p>' + TEXT * 2 +
                       '</p></body></html></div></body></html>').encode()
        with patch('trafilatura.extract', return_value=None):
            recovered_article = crawler.parse_many([(1, 'https://baotayninh.vn/article.html', nested_html, 'utf-8')])[0]
            wrong_source = crawler.parse_many([(1, 'https://example.com/article.html', nested_html, 'utf-8')])[0]
            navigation_only = crawler.parse_many([(1, 'https://baotayninh.vn/article.html', b'<nav>Menu only</nav>', 'utf-8')])[0]
        assert TEXT.strip() in recovered_article[2] and 'Trang chu' not in recovered_article[2]
        assert wrong_source[2] is None and navigation_only[2] is None
        mobile_url, question_id = crawler.mobile_question_request('https://ask.39.net/question/103687999.html', True)
        assert mobile_url == 'https://wapask.39.net/question/103687999.html' and question_id == '103687999'
        assert crawler.mobile_question_request('https://example.com/question/103687999.html', True)[1] is None
        identity_html = b'<input type="hidden" id="tid_gla" value="103687999">'
        assert crawler.verified_mobile_question(identity_html, question_id)
        assert not crawler.verified_mobile_question(identity_html, '103687998')
        assert not crawler.verified_mobile_question(b'<h1>103687999</h1><nav>Home</nav>', question_id)
        pubmed_request, pubmed_id = crawler.pubmed_api_request('https://pubmed.ncbi.nlm.nih.gov/33461452/')
        assert pubmed_id == '33461452' and 'europepmc' in pubmed_request and 'EXT_ID:33461452' in pubmed_request
        pubmed_body = json.dumps({'resultList': {'result': [{'source': 'MED', 'pmid': '33461452', 'title': 'A title',
                         'abstractText': 'A sufficiently long biomedical abstract about heart failure and biomarkers.'}]}}).encode()
        assert b'A title' in crawler.europe_pmc_html(pubmed_body, '33461452')
        assert crawler.europe_pmc_html(pubmed_body, '99999999') is None
        mapped = crawler.europe_pmc_results(pubmed_body, [(7466161, 'https://pubmed.ncbi.nlm.nih.gov/33461452/'),
                                                       (4466162, 'https://pubmed.ncbi.nlm.nih.gov/11111111/')])
        assert mapped[0][0] == 7466161 and mapped[0][2] and mapped[0][3] == 'utf-8'
        assert mapped[1][4] == 3 and mapped[1][3] == 'pubmed_missing_abstract'
        # Batched API responses may be returned out of order; original IDs above
        # five million must remain correctly associated with PubMed URLs.
        pubmed_index = root / 'pubmed-index'
        pubmed_index.mkdir()
        pubmed_items = [(7466000 + i, f'https://pubmed.ncbi.nlm.nih.gov/{10000000+i}/') for i in range(121)]
        (pubmed_index / 'pubmed.jsonl').write_text(''.join(json.dumps(item)+'\n' for item in pubmed_items))
        api_calls = []
        class ApiResponse:
            status = 200
            def __init__(self, identifiers):
                self.content = self
                self.body = json.dumps({'resultList': {'result': [
                    {'source': 'MED', 'pmid': pmid, 'title': f'Source title {pmid}', 'abstractText': TEXT * 3}
                    for pmid in reversed(identifiers)]}}).encode()
            async def iter_chunked(self, size):
                # The network may yield small partial reads before EOF.
                for i in range(0, len(self.body), 123):
                    yield self.body[i:i+123]
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                return False
        class ApiSession:
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                return False
            def get(self, url, params, **kwargs):
                assert url == 'https://www.ebi.ac.uk/europepmc/webservices/rest/search'
                identifiers = __import__('re').findall(r'\d+', params['query'])
                api_calls.append(identifiers)
                return ApiResponse(identifiers)
        async def check_api_batching():
            args = SimpleNamespace(concurrency=2, per_host=2, host_interval=0, timeout=2,
                                   retries=0, max_page_mb=4, limit=0, stop_requested=False, retry_failed=False,
                                   host_failure_limit=16, host_settings={})
            db = sqlite3.connect(':memory:')
            db.execute('CREATE TABLE host_cursors (host TEXT PRIMARY KEY, source TEXT, offset INTEGER)')
            stats = {'scheduled':0, 'downloaded':0, 'download_bytes':0, 'active_requests':0, 'retries':0, 'deferred_hosts':{}}
            html_results = asyncio.Queue()
            with patch.object(crawler.aiohttp, 'ClientSession', return_value=ApiSession()):
                await crawler.download_loop({'hosts':[{'host':'pubmed.ncbi.nlm.nih.gov','file':'pubmed.jsonl','count':121}],
                                             'source':{'bytes':100,'mtime_ns':0}}, pubmed_index, bytearray(7466121), args,
                                            html_results, asyncio.Queue(), stats, db)
            db.close()
            assert stats['scheduled'] == stats['downloaded'] == 121 and html_results.qsize() == 121
            for original_id, original_url in pubmed_items:
                result = html_results.get_nowait()
                assert (result[0],result[1]) == (original_id,original_url)
                pmid = original_url.rstrip('/').rsplit('/',1)[1]
                assert f'Source title {pmid}'.encode() in result[2]
        asyncio.run(check_api_batching())
        assert sorted(map(len,api_calls)) == [21,50,50]
        class AlternateMissing:
            status = 404
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                return False
        class MissingSession:
            def get(self, url, **kwargs):
                self.request_url = url
                return AlternateMissing()
        mobile_args = SimpleNamespace(timeout=1, retries=0, host_settings={'ask.39.net': {'prefer_mobile': True}})
        mobile_source = crawler.Host('ask.39.net', None)
        missing_session = MissingSession()
        missing_mobile = asyncio.run(crawler.fetch(missing_session, (1, 'https://ask.39.net/question/103687999.html'),
                                                   mobile_source, mobile_args, {}))
        assert missing_mobile[1] == 'https://ask.39.net/question/103687999.html' and missing_mobile[4] == 3
        assert missing_session.request_url == mobile_url
        # Shared domain pacing must serve later hosts before repeating a prefix.
        fairness_index = root / 'fairness-index'
        fairness_index.mkdir()
        fairness_hosts = []
        for position in range(5):
            host_name = f'host{position}.39.net'
            filename = f'{position}.jsonl'
            rows = [[position * 2 + i + 1, f'https://{host_name}/article/{i}'] for i in range(2)]
            (fairness_index / filename).write_text(''.join(json.dumps(row) + '\n' for row in rows))
            fairness_hosts.append({'host': host_name, 'file': filename, 'count': 2})
        fair_db = sqlite3.connect(':memory:')
        fair_db.execute('CREATE TABLE host_cursors (host TEXT PRIMARY KEY, source TEXT, offset INTEGER)')
        order = []
        async def immediate_fetch(session, item, host, args, stats):
            order.append(host.name)
            return item[0], item[1], b'<html>Article</html>', 'utf-8'
        async def check_fairness():
            args = SimpleNamespace(concurrency=64, per_host=1, host_interval=0.04, timeout=1,
                                   limit=0, stop_requested=False, retry_failed=False,
                                   host_failure_limit=32, host_settings={})
            stats = {'scheduled': 0, 'downloaded': 0, 'active_requests': 0, 'deferred_hosts': {}}
            with patch.object(crawler, 'fetch', immediate_fetch):
                await crawler.download_loop({'hosts': fairness_hosts, 'source': {'bytes': 10, 'mtime_ns': 0}},
                                            fairness_index, bytearray(11), args,
                                            asyncio.Queue(), asyncio.Queue(), stats, fair_db)
            assert stats['downloaded'] == 10
        asyncio.run(check_fairness())
        assert len(set(order[:5])) == 5, order
        fair_db.close()
        # A cooldown with zero active connections must have an explicit reason
        # and a stop must exit it promptly without scheduling another request.
        cooldown_index = root/'cooldown-index'
        cooldown_index.mkdir()
        hostname = f'127.0.0.1:{server.server_port}'
        (cooldown_index/'host.jsonl').write_text(''.join(json.dumps([i,f'http://{hostname}/rate-limited'])+'\n' for i in [1,2]))
        async def check_cooldown_status():
            args = SimpleNamespace(concurrency=1, per_host=1, host_interval=0, timeout=2, retries=0,
                                   max_page_mb=1, limit=0, stop_requested=False, retry_failed=False,
                                   host_failure_limit=8, host_settings={})
            db = sqlite3.connect(':memory:')
            db.execute('CREATE TABLE host_cursors(host TEXT PRIMARY KEY,source TEXT,offset INTEGER)')
            stats = {'scheduled':0,'downloaded':0,'active_requests':0,'deferred_hosts':{},'retries':0}
            work = asyncio.create_task(crawler.download_loop({'hosts':[{'host':hostname,'file':'host.jsonl','count':2}],
                'source':{'bytes':1,'mtime_ns':0}},cooldown_index,bytearray(3),args,asyncio.Queue(),asyncio.Queue(),stats,db))
            try:
                deadline = time.monotonic()+2
                while stats.get('activity') != 'waiting_host_cooldown' and time.monotonic()<deadline:
                    await asyncio.sleep(0.01)
                assert stats.get('activity') == 'waiting_host_cooldown', stats
                assert stats['active_requests'] == 0 and stats['cooling_hosts'][hostname]>0
                args.stop_requested=True
                await asyncio.wait_for(work,timeout=1)
                assert stats['scheduled'] == 1 and not stats['cooling_hosts']
                assert stats['activity'] == 'downloads_finished'
            finally:
                if not work.done():
                    work.cancel()
                    await asyncio.gather(work,return_exceptions=True)
                db.close()
        asyncio.run(check_cooldown_status())
        urls = [f"http://{'localhost' if i % 2 else '127.0.0.1'}:{server.server_port}/doc/{i}" for i in range(1, 25)]
        rows = [{"id": i, "url": u} for i, u in enumerate(urls, 1)]
        rows += [rows[0]]
        for doc_id, path in [(25, "404"), (26, "503"), (27, "flaky"), (28, "challenge")]:
            rows.append({"id": doc_id, "url": f"http://127.0.0.1:{server.server_port}/{path}"})
        pq.write_table(pa.Table.from_pylist(rows), root / "links.parquet")
        command = [sys.executable, "-u", str(ROOT / "crawl_local.py"), "--links", str(root / "links.parquet"),
                   "--data-dir", str(root / "data"), "--index-dir", str(root / "index"),
                   "--concurrency", "12", "--per-host", "6", "--parse-workers", "2", "--shard-size", "7"]
        first = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", timeout=60)
        assert first.returncode == 0, first.stderr + first.stdout
        status = json.loads((root / "data" / "crawl_status.json").read_text())
        assert status["saved"] == 25 and status["failed"] == 3 and status["processed"] == 28, status
        assert status['pass_number'] == 1
        assert REQUESTS["/doc/1"] == 1 and REQUESTS["/flaky"] == 2 and REQUESTS["/503"] == 2, REQUESTS
        files = sorted((root / "data").glob("corpus_part_*.jsonl"))
        assert len(files) == 4
        documents = [json.loads(line) for path in files for line in path.read_text(encoding="utf-8").splitlines()]
        assert len({d["doc_id"] for d in documents}) == 25
        assert all("Thông tin y học" in d["clean_text"] for d in documents)
        before = dict(REQUESTS)
        second = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", timeout=60)
        assert second.returncode == 0, second.stderr + second.stdout
        status = json.loads((root / "data" / "crawl_status.json").read_text())
        assert status["existing_saved"] == 25 and status["scheduled"] == 2 and status["saved"] == 0, status
        assert REQUESTS["/doc/1"] == before["/doc/1"] and REQUESTS["/404"] == before["/404"]
        # Recover a saved row absent from a stale database after a crash.
        db = sqlite3.connect(root / "data" / "crawl_state.sqlite")
        db.execute("DELETE FROM docs WHERE id=1")
        db.execute("DELETE FROM imported")
        db.commit()
        db.close()
        # Host-specific concurrency, timeout and retry controls must affect real
        # requests, while original document IDs/URLs remain unchanged.
        pq.write_table(pa.Table.from_pylist([
            {"id": 1, "url": f"http://127.0.0.1:{server.server_port}/slow"},
            {"id": 2, "url": f"http://127.0.0.1:{server.server_port}/503"},
        ]), root / "profiles.parquet")
        profile_data = root / "profile-data"
        profile_data.mkdir()
        profile_control = {"hosts": {f"127.0.0.1:{server.server_port}": {
            "per_host": 1, "host_interval": 0.05, "timeout": 2, "connect_timeout": 1, "retries": 0,
        }}}
        (profile_data / "crawl_control.json").write_text(json.dumps(profile_control))
        requests_before = REQUESTS["/503"]
        profiled = subprocess.run([sys.executable, "-u", str(ROOT / "crawl_local.py"),
                                   "--links", str(root / "profiles.parquet"), "--data-dir", str(profile_data),
                                   "--index-dir", str(root / "profiles-index"), "--timeout", "0.05",
                                   "--parse-workers", "1", "--per-host", "4", "--host-interval", "0"],
                                  capture_output=True, text=True, encoding="utf-8", timeout=30)
        assert profiled.returncode == 0, profiled.stderr + profiled.stdout
        profiled_status = json.loads((profile_data / "crawl_status.json").read_text())
        assert profiled_status["saved"] == 1 and profiled_status["failed"] == 1, profiled_status
        assert REQUESTS["/503"] - requests_before == 1, profiled_status
        assert profiled_status["saved_by_host"][f"127.0.0.1:{server.server_port}"] == 1
        assert profiled_status["settings"]["hosts"] == profile_control["hosts"]
        third = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", timeout=60)
        assert third.returncode == 0, third.stderr + third.stdout
        assert REQUESTS["/doc/1"] == before["/doc/1"]
        assert json.loads((root / "data" / "crawl_status.json").read_text())["existing_saved"] == 25
        # Defer inaccessible hosts without declaring their unvisited IDs complete.
        blocked_rows = [{"id": i, "url": f"http://127.0.0.1:{server.server_port}/blocked"}
                        for i in range(100, 110)]
        pq.write_table(pa.Table.from_pylist(blocked_rows), root / "blocked.parquet")
        blocked_command = [sys.executable, "-u", str(ROOT / "crawl_local.py"),
                           "--links", str(root / "blocked.parquet"), "--data-dir", str(root / "blocked-data"),
                           "--index-dir", str(root / "blocked-index"), "--concurrency", "1", "--per-host", "1",
                           "--parse-workers", "1", "--host-failure-limit", "2"]
        blocked = subprocess.run(blocked_command, capture_output=True, text=True, encoding="utf-8", timeout=60)
        assert blocked.returncode == 0, blocked.stderr + blocked.stdout
        status = json.loads((root / "blocked-data" / "crawl_status.json").read_text())
        assert status["state"] == "pass_complete_with_deferred_hosts" and status["processed"] == 2, status
        assert next(iter(status["deferred_hosts"].values()))["remaining_index_rows"] == 8
        BLOCKED = False
        recovered = subprocess.run(blocked_command, capture_output=True, text=True, encoding="utf-8", timeout=60)
        assert recovered.returncode == 0, recovered.stderr + recovered.stdout
        status = json.loads((root / "blocked-data" / "crawl_status.json").read_text())
        assert status["saved"] == 10 and not status["deferred_hosts"], status
        # A live stop request must exit cleanly without scheduling new work.
        control = root / "blocked-data" / "crawl_control.json"
        control.write_text(json.dumps({"concurrency": 3, "per_host": 2, "stop": True}), encoding="utf-8-sig")
        stopped = subprocess.run(blocked_command, capture_output=True, text=True, encoding="utf-8", timeout=60)
        assert stopped.returncode == 0, stopped.stderr + stopped.stdout
        status = json.loads((root / "blocked-data" / "crawl_status.json").read_text())
        assert status["state"] == "stopped" and status["scheduled"] == 0, status
        assert status["settings"]["concurrency"] == 3 and status["settings"]["per_host"] == 2
        # Script challenges are filtered; actual articles containing scripts survive.
        pq.write_table(pa.Table.from_pylist([
            {"id": 1, "url": f"http://127.0.0.1:{server.server_port}/javascript-only"},
            {"id": 2, "url": f"http://127.0.0.1:{server.server_port}/article-with-javascript"},
        ]), root / "javascript.parquet")
        javascript_command = [sys.executable, "-u", str(ROOT / "crawl_local.py"),
                              "--links", str(root / "javascript.parquet"), "--data-dir", str(root / "js-data"),
                              "--index-dir", str(root / "js-index"), "--parse-workers", "1"]
        javascript = subprocess.run(javascript_command, capture_output=True, text=True, encoding="utf-8", timeout=60)
        assert javascript.returncode == 0, javascript.stderr + javascript.stdout
        status = json.loads((root / "js-data" / "crawl_status.json").read_text())
        assert status["saved"] == 1 and status["errors"].get("javascript_challenge") == 1, status
        saved_js = [json.loads(line) for path in (root / "js-data").glob("corpus_part_*.jsonl")
                    for line in path.read_text(encoding="utf-8").splitlines()]
        assert len(saved_js) == 1 and saved_js[0]["doc_id"] == 2
        # Repeat mode keeps the process alive and accepts stop during retry waits.
        pq.write_table(pa.Table.from_pylist([
            {"id": 1, "url": f"http://127.0.0.1:{server.server_port}/503"}
        ]), root / "repeat.parquet")
        repeat_command = [sys.executable, "-u", str(ROOT / "crawl_local.py"),
                          "--links", str(root / "repeat.parquet"), "--data-dir", str(root / "repeat-data"),
                          "--index-dir", str(root / "repeat-index"), "--parse-workers", "1",
                          "--repeat", "--retry-delay", "5"]
        repeat_process = subprocess.Popen(repeat_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                          text=True, encoding="utf-8")
        try:
            retry_deadline = time.monotonic() + 30
            repeat_status = root / "repeat-data" / "crawl_status.json"
            while time.monotonic() < retry_deadline:
                if repeat_status.exists():
                    status = json.loads(repeat_status.read_text())
                    if status["state"] == "waiting_retry":
                        break
                if repeat_process.poll() is not None:
                    raise AssertionError(repeat_process.communicate())
                time.sleep(0.1)
            else:
                raise AssertionError("No retry wait observed")
            assert repeat_process.poll() is None and status.get("next_retry_at")
            (root / "repeat-data" / "crawl_control.json").write_text('{"stop":true}', encoding="utf-8")
            output, errors = repeat_process.communicate(timeout=15)
            assert repeat_process.returncode == 0, output + errors
            assert json.loads(repeat_status.read_text())["state"] == "stopped"
        finally:
            if repeat_process.poll() is None:
                repeat_process.terminate()
                repeat_process.communicate(timeout=10)
        # An abrupt parser exit must resume the uncommitted document in repeat mode.
        helper = root / "crash_parser_once.py"
        helper.write_text(
            "import os, sys\nfrom pathlib import Path\n"
            f"sys.path.insert(0, {str(ROOT)!r})\n"
            "import crawl_local as crawler\noriginal_parse = crawler.parse_many\n"
            "def crash_once(items):\n"
            "    marker = Path(__file__).with_suffix('.marker')\n"
            "    if not marker.exists():\n"
            "        marker.write_text('crashed')\n        os._exit(3)\n"
            "    return original_parse(items)\n"
            "if __name__ == '__main__':\n"
            "    crawler.parse_many = crash_once\n    crawler.main()\n", encoding="utf-8")
        pq.write_table(pa.Table.from_pylist([
            {"id": 1, "url": f"http://127.0.0.1:{server.server_port}/doc/crash-recovery"}
        ]), root / "crash.parquet")
        crash_command = [sys.executable, "-u", str(helper), "--links", str(root / "crash.parquet"),
                         "--data-dir", str(root / "crash-data"), "--index-dir", str(root / "crash-index"),
                         "--parse-workers", "2", "--repeat", "--retry-delay", "1"]
        crash = subprocess.run(crash_command, capture_output=True, text=True, encoding="utf-8", timeout=45)
        assert crash.returncode == 0, crash.stderr + crash.stdout
        assert helper.with_suffix('.marker').exists()
        crash_status = json.loads((root / "crash-data" / "crawl_status.json").read_text())
        assert crash_status["state"] == "pass_complete" and crash_status["saved"] == 1, crash_status
        assert crash_status["settings"]["parse_workers"] == 1 and crash_status["settings"]["concurrency"] == 96
        # A blocked prefix must not starve the accessible tail on every pass.
        pq.write_table(pa.Table.from_pylist([
            {"id": i + 1, "url": f"http://127.0.0.1:{server.server_port}/blocked-prefix/{i}"}
            for i in range(6)
        ]), root / "prefix.parquet")
        prefix_command = [sys.executable, "-u", str(ROOT / "crawl_local.py"),
                          "--links", str(root / "prefix.parquet"), "--data-dir", str(root / "prefix-data"),
                          "--index-dir", str(root / "prefix-index"), "--concurrency", "1", "--per-host", "1",
                          "--host-interval", "0", "--parse-workers", "1", "--host-failure-limit", "2",
                          "--repeat", "--retry-delay", "1", "--no-progress-passes", "2", "--max-passes", "5"]
        prefix = subprocess.run(prefix_command, capture_output=True, text=True, encoding="utf-8", timeout=60)
        assert prefix.returncode == 0, prefix.stderr + prefix.stdout
        prefix_status = json.loads((root / "prefix-data" / "crawl_status.json").read_text())
        assert prefix_status["state"] == "finished_with_missing" and prefix_status["corpus_saved"] == 4, prefix_status
        inventory = json.loads((root / "prefix-data" / "corpus_manifest.json").read_text())
        assert inventory["complete"] is False and inventory["counts"] == {"retryable": 2, "saved": 4}, inventory
        missing = [json.loads(line) for line in (root / "prefix-data" / "missing_urls.jsonl").read_text().splitlines()]
        assert {row["doc_id"] for row in missing} == {1, 2} and all(row["status"] == "retryable" for row in missing)
        assert REQUESTS["/blocked-prefix/2"] == 1, REQUESTS
        db = sqlite3.connect(root / "prefix-data" / "crawl_state.sqlite")
        assert db.execute("select count(*) from host_cursors").fetchone()[0] == 1
        assert db.execute("select count(*) from docs where status=2").fetchone()[0] == 0
        db.close()
    server.shutdown()
    print("PASS: extraction, retries, deduplication, resume, live controls, repeat/stop, parser-crash recovery, busy progress files, host cursor recovery, bounded retries, honest missing-URL inventory, per-host timeout/retry settings, mobile question identity, batched PubMed identity/streaming, fair shared-host scheduling, cooldown visibility and safe stop")


if __name__ == "__main__":
    main()
