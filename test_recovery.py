"""Identity and source extraction checks for alternative public fetches."""
import asyncio
from collections import Counter
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import orjson

from crawl_local import Host, host_setting, parse_many
from crawl_recovery import RecoveryClient, source_article


class RecoveryTests(unittest.TestCase):
    def test_exact_slug_and_article_only(self):
        u = 'https://nhathuoclongchau.com.vn/bai-viet/source.html'
        text = 'Nội dung gốc về một bệnh và các triệu chứng. ' * 12
        d = {'props': {'pageProps': {'detail': {'data': {'slug':'bai-viet/source.html', 'name':'Tên bài', 'description':'<p>'+text+'</p>'}}}}}
        b = ('<html><head><link rel="canonical" href="'+u+'"></head><body><h1>Tên bài</h1><nav>Unrelated navigation</nav><script id="__NEXT_DATA__">'+orjson.dumps(d).decode()+'</script></body></html>').encode()
        out, _ = source_article(b, u, u)
        result = parse_many([(7466161,u,out,'utf-8')])[0]
        self.assertIn('Nội dung gốc', result[2])
        self.assertNotIn('Unrelated navigation', result[2])
        with self.assertRaisesRegex(ValueError, 'source_redirect_mismatch'):
            source_article(b,u,'https://nhathuoclongchau.com.vn/')
        d['props']['pageProps']['detail']['data']['slug']='bai-viet/different.html'
        mismatch=b.replace(b'bai-viet/source.html"', b'bai-viet/different.html"',1)
        with self.assertRaises(ValueError):
            source_article(mismatch,u,u)

    def test_moved_source_is_same_slug_and_no_recommendations(self):
        original='https://pmc-ecm-healthblog.beta.pharmacity.io/source/'
        u='https://www.pharmacity.vn/source.htm'
        b=('<html><head><link rel="canonical" href="'+u+'"></head><body><div><h1>Source</h1><div class="mt-4 mb-4"><p>'+('Exact original content. '*15)+'</p></div><section>Related articles must be excluded.</section></div></body></html>').encode()
        out,_=source_article(b,original,u)
        self.assertNotIn(b'Related articles',out)
        with self.assertRaises(ValueError):
            source_article(b,original,'https://www.pharmacity.vn/different.htm')

    def test_http_cookie_retry_and_429_cooldown(self):
        async def check():
            async with RecoveryClient() as client:
                calls=[]
                u='https://laodong.vn/suc-khoe/source-123.ldo'
                b=('<html><head><link rel="canonical" href="'+u+'"></head><body><article class="article-detail"><h1>Source</h1><p>'+('Source content. '*20)+'</p></article></body></html>').encode()
                async def read(url,args,timeout):
                    calls.append(url)
                    if len(calls)==1:return 200,u,b'<script>document.cookie="D1N=0123456789abcdef0123456789abcdef";window.location.reload(true);</script>',{}
                    return 200,u,b,{}
                client.read=read
                args=SimpleNamespace(host_settings={},retries=0,host_interval=0,timeout=3,max_page_mb=4)
                stats={'retries':0,'download_bytes':0}
                host=Host('laodong.vn',None)
                result=await client.fetch((123,u),host,args,stats,host_setting)
                self.assertEqual(len(calls),2)
                self.assertIsNotNone(result[2])
                async def limited(*args):return 429,u,b'',{'Retry-After':'90'}
                client.read=limited
                result=await client.fetch((123,u),host,args,stats,host_setting)
                self.assertEqual(result[3],'http_429')
                self.assertGreater(host.cooldown,0)
        asyncio.run(check())


if __name__=='__main__':
    unittest.main()
