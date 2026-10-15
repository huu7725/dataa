"""Public source recovery with ordinary HTTP, exact identity checks and receipts.

No JavaScript execution, CAPTCHA solving, proxy rotation or disabled TLS checks.
The transport supports the HTTP/TLS format used by the publishers' web browsers.
"""
from __future__ import annotations

import asyncio
import gzip
import hashlib
import html
from pathlib import Path
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit

import orjson
from lxml import html as lh

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / '.recovery_vendor'))

RECOVERY_HOSTS = {'nhathuoclongchau.com.vn', 'laodong.vn', 'vov.vn',
                  'www.qdnd.vn', 'pmc-ecm-healthblog.beta.pharmacity.io'}


def supported(host):
    return host in RECOVERY_HOSTS or (host.endswith('.39.net') and host != 'ask.39.net')


def published_url(original):
    u = urlsplit(original)
    if u.hostname == 'pmc-ecm-healthblog.beta.pharmacity.io':
        slug = u.path.strip('/')
        if not slug or '/' in slug:
            raise ValueError('Unrecognized Pharmacity article URL')
        return 'https://www.pharmacity.vn/' + slug + '.htm'
    if u.scheme == 'http' and u.hostname.endswith('.39.net'):
        return 'https://' + original[7:]
    return original


def url_identity(url):
    u = urlsplit(url)
    return (u.hostname, u.path.rstrip('/'))


def source_article(body, original, final_url):
    """Return HTML containing only source text; reject homepage/error redirects."""
    requested = published_url(original)
    if url_identity(requested) != url_identity(final_url):
        raise ValueError('source_redirect_mismatch')
    host = urlsplit(original).hostname
    tree = lh.fromstring(body, parser=lh.HTMLParser(encoding='utf-8') if host in RECOVERY_HOSTS else None)
    titles = tree.xpath('//title/text()')
    if titles and re.search(r'just a moment|one moment|access denied|attention required|captcha|403 Forbidden|404 Not Found', str(titles[0]), re.I):
        raise ValueError('challenge_page')
    canonical = tree.xpath('//link[@rel="canonical"]/@href')
    if canonical and url_identity(str(canonical[0])) != url_identity(requested):
        raise ValueError('source_canonical_mismatch')
    h1 = tree.xpath('//h1')
    if host in RECOVERY_HOSTS and (not h1 or not h1[0].text_content().strip()):
        raise ValueError('source_article_missing')
    if host == 'nhathuoclongchau.com.vn':
        nodes = tree.xpath('//script[@id="__NEXT_DATA__"]/text()')
        if not nodes:
            raise ValueError('source_article_missing')
        page = orjson.loads(str(nodes[0]))['props']['pageProps']
        data = page.get('detail', {}).get('data') or page.get('data')
        if not isinstance(data, dict) or data.get('slug', '').strip('/') != urlsplit(original).path.strip('/'):
            raise ValueError('source_article_id_mismatch')
        # These are the publisher's visible article sections, in display order.
        fields = ['shortDescription', 'description', 'symptom', 'aetiologies',
                  'risk', 'diagnoseAndTreat', 'diagnoseAndTreaty', 'livingAndPreventive']
        fragments = [str(data[k]) for k in fields if isinstance(data.get(k), str) and data[k].strip()]
        if not data.get('description') or len(''.join(fragments)) < 100:
            raise ValueError('source_article_missing')
        title = str(data.get('headline') or data.get('name') or h1[0].text_content())
        content = '<h1>' + html.escape(title) + '</h1>' + ''.join('<div>'+v+'</div>' for v in fragments)
        method = 'publisher_next_data_exact_slug'
    elif host == 'laodong.vn':
        nodes = tree.xpath('//article[contains(concat(" ",normalize-space(@class)," ")," article-detail ")]')
        if not nodes:
            raise ValueError('source_article_missing')
        article = nodes[0]
        parts = article.xpath('.//*[contains(concat(" ",normalize-space(@class)," ")," chappeau ") or contains(concat(" ",normalize-space(@class)," ")," art-body ")]')
        if not parts:
            raise ValueError('source_article_missing')
        content = '<h1>' + html.escape(h1[0].text_content()) + '</h1>'
        for part in parts:
            for node in part.xpath('.//script|.//style|.//nav|.//aside|.//form'):
                node.drop_tree()
            # Photo reports store their actual prose in figcaption elements.
            # Preserve that prose and block boundaries rather than drop captions.
            for node in part.xpath('.//p|.//div|.//li|.//br|.//figcaption|.//h2|.//h3|.//h4'):
                node.tail = ' ' + (node.tail or '')
            content += '<p>' + html.escape(part.text_content()) + '</p>'
        method = 'publisher_article_detail'
    elif host == 'pmc-ecm-healthblog.beta.pharmacity.io':
        nodes = h1[0].getparent().xpath('./div[contains(concat(" ",normalize-space(@class)," ")," mt-4 ") and contains(concat(" ",normalize-space(@class)," ")," mb-4 ")]')
        nodes = [n for n in nodes if len(n.text_content().strip()) > 100 and n.xpath('.//p')]
        if len(nodes) != 1:
            raise ValueError('source_article_missing')
        content = lh.tostring(h1[0], encoding='unicode') + lh.tostring(nodes[0], encoding='unicode')
        method = 'publisher_moved_exact_slug'
    else:
        if host.endswith('.39.net') and urlsplit(final_url).hostname == 'image.39.net':
            raise ValueError('challenge_page')
        return body, 'exact_original_url'
    fragment = lh.fromstring('<article>' + content + '</article>')
    for n in fragment.xpath('.//script|.//style|.//nav|.//aside|.//form'):
        n.drop_tree()
    if len(fragment.text_content().strip()) < 100:
        raise ValueError('source_article_missing')
    return lh.tostring(fragment, encoding='utf-8'), method


def preserve_response(data_dir, doc_id, original, fetched, body, method):
    checksum = hashlib.sha256(body).hexdigest()
    cache = data_dir / 'recovery_source_cache' / str(doc_id // 5000)
    cache.mkdir(parents=True, exist_ok=True)
    path = cache / f'{doc_id}_{checksum}.html.gz'
    if not path.exists():
        with gzip.open(path, 'wb', compresslevel=1) as f:
            f.write(body)
    receipt = {'time': datetime.now(timezone.utc).isoformat(), 'doc_id': doc_id,
               'original_url': original, 'fetched_url': fetched, 'method': method,
               'response_sha256': checksum, 'response_file': str(path.relative_to(data_dir)),
               'identity_verified': True}
    with (data_dir / 'recovery_provenance.jsonl').open('ab') as f:
        f.write(orjson.dumps(receipt) + b'\n')


class RecoveryClient:
    async def __aenter__(self):
        from curl_cffi.requests import AsyncSession
        self.session = AsyncSession(impersonate='chrome', max_clients=32)
        self.cookie_lock = asyncio.Lock()
        self.receipt_lock = asyncio.Lock()
        return self

    async def __aexit__(self, *exc):
        await self.session.close()

    async def read(self, url, args, timeout):
        async with self.session.stream('GET', url, timeout=timeout) as response:
            body = bytearray()
            async for chunk in response.aiter_content():
                body.extend(chunk)
                if len(body) > args.max_page_mb * 1024 * 1024:
                    raise ValueError('page_too_large')
            return response.status_code, str(response.url), bytes(body), response.headers

    async def fetch(self, item, host, args, stats, setting):
        from curl_cffi.requests.exceptions import RequestException
        doc_id, original = item
        request_url = published_url(original)
        error, failure_status = 'network_error', 3
        for attempt in range(setting(args, host, 'retries') + 1):
            if attempt:
                if time.monotonic() < host.cooldown or host.suspend_reason:
                    break
                stats['retries'] += 1
                await asyncio.sleep(1)
            try:
                timeout = setting(args, host, 'timeout')
                status, final_url, body, headers = await self.read(request_url, args, timeout)
                if host.name == 'laodong.vn' and status == 200 and len(body) < 2048:
                    match = re.search(rb'document\.cookie="D1N=([a-f0-9]{32})"', body)
                    if match:
                        # Only accept this exact public publisher cookie, never run JS.
                        async with self.cookie_lock:
                            self.session.cookies.set('D1N', match[1].decode(), domain='laodong.vn', path='/')
                        await asyncio.sleep(setting(args, host, 'host_interval'))
                        status, final_url, body, headers = await self.read(request_url, args, timeout)
                if status != 200:
                    error = f'http_{status}'
                    failure_status = 2 if status in {400, 404, 410, 451} else 3
                    if status == 429:
                        try:
                            delay = max(30, float(headers.get('Retry-After', '60')))
                        except ValueError:
                            delay = 60
                        host.cooldown = max(host.cooldown, time.monotonic() + delay)
                        break
                    if status not in {408, 425, 500, 502, 503, 504}:
                        break
                    continue
                stats['download_bytes'] += len(body)
                extracted, method = source_article(body, original, final_url)
                if hasattr(args, 'data_dir'):
                    async with self.receipt_lock:
                        await asyncio.to_thread(preserve_response, args.data_dir, doc_id, original, final_url, body, method)
                return (doc_id, original, extracted, 'utf-8' if host.name in RECOVERY_HOSTS else None)
            except ValueError as exc:
                error = str(exc) if str(exc) in {'page_too_large','source_redirect_mismatch','source_canonical_mismatch','source_article_missing','source_article_id_mismatch','challenge_page'} else 'source_invalid'
                break
            except (RequestException, asyncio.TimeoutError) as exc:
                error = 'TimeoutError' if 'timeout' in str(exc).lower() else 'ClientConnectorError'
        return (doc_id, original, None, error, failure_status)
