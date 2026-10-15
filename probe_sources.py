"""Small public URL diagnostics; never writes corpus records."""
import asyncio
from pathlib import Path
import re
import sys
import aiohttp
import orjson

ROOT = Path(__file__).resolve().parent


async def main():
    diagnosis = orjson.loads((ROOT / "crawl_diagnosis.json").read_bytes())
    semaphore = asyncio.Semaphore(4)
    results = []
    cache = ROOT / "tuning_samples" / "recovery_probe"
    cache.mkdir(exist_ok=True)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36"}
    async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=20, connect=8)) as session:
        async def probe(host, doc_id, url, variant):
            async with semaphore:
                result = {"host": host, "doc_id": doc_id, "variant": variant, "url": url}
                try:
                    async with session.get(url) as response:
                        body = await response.content.read(2 * 1024 * 1024)
                        result.update(status=response.status, final_url=str(response.url), bytes=len(body), content_type=response.content_type)
                        title = re.search(br"<title[^>]*>(.*?)</title>", body, re.I | re.S)
                        result["title"] = title[1].decode(response.charset or "utf-8", errors="replace") if title else ""
                        path = cache / f"{doc_id}_{variant}.html"
                        path.write_bytes(body)
                        result["file"] = str(path)
                except Exception as exc:
                    result["error"] = type(exc).__name__ + ": " + str(exc)
                results.append(result)
                print(orjson.dumps(result).decode(), flush=True)
        work = []
        for host in diagnosis["hosts"]:
            doc_id, url = host["samples"][0]
            work.append(probe(host["host"], doc_id, url, "original"))
            if url.startswith("http://"):
                work.append(probe(host["host"], doc_id, "https://" + url[7:], "https"))
        await asyncio.gather(*work)
    (ROOT / "probe_results.json").write_bytes(orjson.dumps(results, option=orjson.OPT_INDENT_2))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    asyncio.run(main())
