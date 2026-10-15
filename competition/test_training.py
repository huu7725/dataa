"""Verify weak-label provenance, training behavior and snapshot adoption guards."""
from pathlib import Path
import tempfile
import sqlite3
import unittest
from unittest.mock import patch

import numpy as np

from .core import digest, file_hash, write_json
from .training import weak_pair, optimize_projection
from .models import Encoder, approved_model
from .adopt_snapshot import adopt


class TrainingTests(unittest.TestCase):
    def test_training_pair_contains_only_disjoint_source_spans(self):
        text='Viêm phổi có thể gây sốt và khó thở. '+'Bác sĩ cần kiểm tra triệu chứng và hướng dẫn điều trị phù hợp. '*12
        row={'doc_id':7466161,'url':'https://example.vn/source.html','clean_text':text}
        pair=weak_pair(row)
        self.assertIsNotNone(pair)
        self.assertEqual(pair['language'],'vi')
        a,b=pair['query_span'];c,d=pair['passage_span']
        self.assertEqual(pair['query'],text[a:b])
        self.assertEqual(pair['passage'],text[c:d])
        self.assertLessEqual(b,c)
        self.assertNotIn('query_id',pair)
        row['clean_text']='Hotline liên hệ hỗ trợ khách hàng. '+text*6
        self.assertIsNone(weak_pair(row))

    def test_projection_trains_and_reports_only_holdout_proxy(self):
        rng=np.random.default_rng(3)
        q=rng.normal(size=(90,12)).astype(np.float32)
        p=q.copy();p[:,6:]+=rng.normal(size=(90,6))*2
        q/=np.linalg.norm(q,axis=1,keepdims=True);p/=np.linalg.norm(p,axis=1,keepdims=True)
        rows=[{'language':['vi','en','zh'][i%3],'split':'validation' if i>=60 else 'train'} for i in range(90)]
        settings={'seed':7,'epochs':8,'learning_rate':.02}
        weights,report=optimize_projection(q,p,rows,settings)
        self.assertTrue(np.isfinite(weights).all())
        self.assertTrue(np.any(np.abs(weights-1)>.001))
        self.assertLessEqual(weights.max(),np.exp(.25)+1e-6)
        self.assertEqual(report['baseline']['pairs'],30)
        self.assertIn('not BTC',report['baseline']['task'])
        self.assertNotIn('final_score',report)
        self.assertEqual(len(report['history']),8)

    def test_adapter_rejects_unapproved_base_and_nonfinite_coefficients(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)/'adapter.json'
            d={'version':'positive-diagonal-v1','base_weights_sha256':'0'*64,'dimension':1024,
               'parameter_count':1024,'selected':True,'weights':[1]*1024}
            write_json(p,d)
            with patch('competition.models.fetch_model',return_value=Path(directory)):
                with self.assertRaisesRegex(ValueError,'Invalid team-trained'):
                    Encoder('bge_m3',adapter_path=p)

    def test_snapshot_adoption_checks_full_indexed_source_text(self):
        import orjson
        import zlib
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);work=root/'work';work.mkdir()
            links=root/'links.parquet';links.write_bytes(b'unchanged link identity')
            row={'doc_id':71,'url':'https://example.vn/71','clean_text':'Original medical source content.'}
            body=orjson.dumps(row)+b'\n';shard=root/'corpus_part_0000.jsonl';shard.write_bytes(body)
            prior=root/'prior.json';current=root/'current.json'
            manifest={'source_fingerprint':{'bytes':5},'shards':[{'file':shard.name,'bytes':len(body)}],'counts':{'saved':1}}
            write_json(prior,manifest);write_json(current,dict(manifest,counts={'saved':2},shards=manifest['shards']+[{'file':'corpus_part_0001.jsonl','bytes':1}]))
            config={'work_dir':str(work),'source_manifest':str(current),'links':str(links),
                    'embedding_model':'bge_m3','chunk_tokens':240,'chunk_overlap':48}
            identity={'manifest_sha256':file_hash(prior),'links_sha256':file_hash(links),
                      'tokenizer_model':approved_model('bge_m3'),'chunk_tokens':240,'chunk_overlap':48}
            db=sqlite3.connect(work/'corpus.sqlite')
            db.executescript('CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT); CREATE TABLE docs(doc_id INTEGER PRIMARY KEY,body BLOB); CREATE TABLE sources(doc_id INTEGER PRIMARY KEY,url TEXT); CREATE TABLE imports(file TEXT PRIMARY KEY,offset INTEGER);')
            db.execute('INSERT INTO meta VALUES (?,?)',('fingerprint',digest(identity)))
            db.execute('INSERT INTO docs VALUES (?,?)',(71,zlib.compress(row['clean_text'].encode())))
            db.execute('INSERT INTO sources VALUES (?,?)',(71,row['url']))
            db.execute('INSERT INTO imports VALUES (?,?)',(shard.name,len(body)))
            db.commit();db.close()
            shard.write_bytes(body.replace(b'Original',b'Alteredd'))
            with self.assertRaisesRegex(ValueError,'Indexed source changed'):
                adopt(config,prior)
            shard.write_bytes(body)
            report=adopt(config,prior)
            self.assertEqual(report['verified_indexed_documents'],1)


if __name__=='__main__':
    unittest.main(verbosity=2)
