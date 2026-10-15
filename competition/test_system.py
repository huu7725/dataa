"""Integration/contract tests. Synthetic fixture scores are not competition scores."""
import copy
from contextlib import ExitStack, closing
import json
from pathlib import Path
import random
import re
import tempfile
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .core import RULE_CUTOFF, canonical_id, load_config, normalize, read_json, token_spans, write_json
from .corpus import Corpus, build_corpus
from .dense import build_vectors, dense_search
from .evaluation import evaluate, lcs_length
from .models import approved_model, Encoder
from .retrieval import ranked_results, select_predictions
from .submission import package, queries, validate
from .pipeline import crawl_finished


class FixtureTokenizer:
    def encode(self, text, add_special_tokens=False):
        return SimpleNamespace(offsets=[m.span() for m in re.finditer(r'[\u3400-\u9fff]|\w+|[^\w\s]', text)])


class FixtureEncoder:
    entry = approved_model('e5_small_onnx')
    dimension = 4
    batch_size = 2
    def encode(self, texts, query=False):
        values = [[len(t) % 13 + 1, len(t.split()) % 11 + 1, sum(ord(c) for c in t) % 17 + 1, 1] for t in texts]
        array = np.asarray(values, dtype=np.float32)
        return array / np.linalg.norm(array, axis=1, keepdims=True)


class SystemTests(unittest.TestCase):
    def test_sorted_embedding_restores_original_ids_and_retries_smaller_batches(self):
        from tokenizers import Tokenizer, models, pre_tokenizers
        tokenizer = Tokenizer(models.WordLevel({'[UNK]':0,'[PAD]':1,'a':2,'b':3,'c':4},unk_token='[UNK]'))
        tokenizer.pre_tokenizer = pre_tokenizers.Whitespace()
        tokenizer.enable_padding(pad_id=1,pad_token='[PAD]')
        tokenizer.enable_truncation(max_length=8)
        e = Encoder.__new__(Encoder)
        e.tokenizer=tokenizer;e.batch_size=4;e.dimension=2;e.sort_by_length=False
        e.entry={'query_prefix':'','passage_prefix':'','backend':'torch'}
        calls=[]
        def cpu_fixture(inputs):
            ids=inputs['input_ids'];mask=inputs['attention_mask']
            calls.append((len(ids),ids.shape[1]))
            result=np.stack([(ids*mask).sum(1),mask.sum(1)],axis=1).astype(np.float32)
            return result/np.linalg.norm(result,axis=1,keepdims=True)
        e._encode_inputs=cpu_fixture
        texts=['b c a b c a','a','c c','b b a','a c b a c b','b']
        expected=e.encode(texts)
        calls.clear();e.sort_by_length=True
        actual=e.encode(texts)
        np.testing.assert_allclose(actual,expected)
        self.assertEqual(calls[0][1],3)
        def reducing_fixture(inputs):
            if len(inputs['input_ids'])>2:
                e.batch_size=2
                return None
            return cpu_fixture(inputs)
        e.batch_size=4;e._encode_inputs=reducing_fixture
        np.testing.assert_allclose(e.encode(texts),expected)

    def test_lcs_is_exact_subsequence_and_macro_is_per_query(self):
        self.assertEqual(lcs_length(['a', 'x', 'b'], ['a', 'b']), 2)
        randomizer = random.Random(5)
        for _ in range(80):
            a = [randomizer.choice('abc') for _ in range(12)]
            b = [randomizer.choice('abc') for _ in range(9)]
            table = [[0] * (len(b) + 1) for _ in range(len(a) + 1)]
            for i, x in enumerate(a, 1):
                for j, y in enumerate(b, 1):
                    table[i][j] = table[i-1][j-1]+1 if x == y else max(table[i-1][j], table[i][j-1])
            self.assertEqual(lcs_length(a, b), table[-1][-1])
        gold = [{'id': 1, 'relevant_docs': ['101'], 'relevant_chunks': [{'doc_id': '101', 'chunk_text': 'a b c d e'}]},
                {'id': 2, 'relevant_docs': ['101'], 'relevant_chunks': [{'doc_id': '101', 'chunk_text': 'a b c d e'}]}]
        pred = [{'id': 1, 'relevant_docs': ['101'], 'relevant_chunks': [{'doc_id': '101', 'chunk_text': 'a x b'}]},
                {'id': 2, 'relevant_docs': ['101', '202', '303'], 'relevant_chunks': [{'doc_id': '202', 'chunk_text': 'a b c d e'}]}]
        score = evaluate(pred, gold)
        self.assertAlmostEqual(score['macro']['docs_f2'], 6/7)
        self.assertAlmostEqual(score['macro']['chunks_f2'], 0.5)
        self.assertIsNone(score['final_score'])
        self.assertIsNone(score['target_over_0_5_verified'])
        exact = [{'id': 1, 'relevant_docs': [], 'relevant_chunks': [{'doc_id': '101', 'chunk_text': 'Ａ &amp; B c d e'}]}]
        reference = [{'id': 1, 'relevant_docs': [], 'relevant_chunks': [
            {'doc_id': '101', 'chunk_text': 'a & b'}, {'doc_id': '101', 'chunk_text': 'c d e'}]}]
        self.assertEqual(evaluate(exact, reference)['macro']['chunks_recall'], 1)
        self.assertEqual(normalize(' Ａ &amp;  B '), 'a & b')

    def test_model_cutoff_and_size_fail_closed(self):
        registry = read_json(Path(__file__).with_name('models.json'))
        for key in registry:
            self.assertLess(approved_model(key)['parameter_count'], 15_000_000_000)
        with self.assertRaises(ValueError):
            approved_model('GPT-4o')
        modified = copy.deepcopy(registry)
        modified['bge_m3']['revision_at'] = RULE_CUTOFF.isoformat()
        with patch('competition.models.read_json', return_value=modified):
            with self.assertRaises(ValueError):
                approved_model('bge_m3')
        modified['bge_m3']['revision_at'] = registry['bge_m3']['revision_at']
        modified['bge_m3']['parameter_count'] = 15_000_000_001
        with patch('competition.models.read_json', return_value=modified):
            with self.assertRaises(ValueError):
                approved_model('bge_m3')

    def test_revised_high_ids_audit_export_and_ready_state(self):
        from prepare_corpus import export
        from audit_corpus import main as audit
        with tempfile.TemporaryDirectory(prefix='revised-check-',dir=Path(__file__).parent) as directory:
            root = Path(directory)
            text = 'A source abstract whose original document ID is larger than five million.'
            shard = root/'corpus_part_0000.jsonl'
            shard.write_text(json.dumps({'doc_id':7466161,'url':'https://pubmed.ncbi.nlm.nih.gov/33461452/','clean_text':text})+'\n')
            links = root/'links_corpus_vi_en_zh.parquet'
            pq.write_table(pa.Table.from_pylist([{'id':7466161,'url':'https://pubmed.ncbi.nlm.nih.gov/33461452/'}]),links)
            inventory={'created_at':'2026-10-12T00:00:00+00:00','complete':True,'total_unique_ids':1,
                       'counts':{'saved':1},'max_id':7466161,'source_links':str(links),
                       'shards':[{'file':shard.name,'bytes':shard.stat().st_size}]}
            write_json(root/'corpus_manifest.json',inventory)
            write_json(root/'crawl_status.json',{'state':'pass_complete','pid':0,'corpus_saved':1})
            cfg={'crawl_data_dir':str(root),'links':str(links)}
            with patch('competition.pipeline.process_matches',return_value=False):
                self.assertEqual(crawl_finished(cfg),(True,'ready'))
                self.assertEqual(crawl_finished(dict(cfg,links=str(root/'wrong.parquet')))[1],'waiting_for_matching_source_links')
            with patch('sys.argv',['audit_corpus.py','--data-dir',str(root)]):
                audit()
            self.assertTrue(read_json(root/'corpus_audit.json')['schema_and_id_count_valid'])
            export(root,root/'ready',shard_size=1,batch_size=1)
            actual=pq.read_table(root/'ready/corpus_part_0000.parquet').to_pylist()
            self.assertEqual(actual[0]['doc_id'],7466161)
            self.assertEqual(actual[0]['clean_text'],text)
    def test_build_resume_dense_search_and_submission(self):
        with tempfile.TemporaryDirectory(prefix='system-check-', dir=Path(__file__).parent) as directory, ExitStack() as handles:
            root = Path(directory)
            texts = ['Alpha source text about lung cancer. ' * 12,
                     'Thông tin nguyên văn về tài liệu y sinh đa ngôn ngữ. ' * 7,
                     '肺癌相关医学资料与诊断信息。' * 10]
            ids = [101, 202, 303]
            links = [{'id': i, 'url': f'https://example.test/{i}'} for i in ids]
            pq.write_table(pa.Table.from_pylist(links), root/'links.parquet')
            questions = [{'id': i + 1, 'query': t[:60]} for i, t in enumerate(texts)]
            pq.write_table(pa.Table.from_pylist(questions), root/'queries.parquet')
            records = [{'doc_id': i, 'url': l['url'], 'clean_text': t} for i, l, t in zip(ids, links, texts)]
            shard = root/'corpus_part_0000.jsonl'
            shard.write_text(''.join(json.dumps(r, ensure_ascii=False)+'\n' for r in records + records[:1]), encoding='utf-8')
            write_json(root/'corpus_manifest.json', {'created_at': '2026-10-12T00:00:00+00:00', 'complete': False,
                       'total_unique_ids': 4, 'counts': {'saved': 3, 'unvisited': 1},
                       'shards': [{'file': shard.name, 'bytes': shard.stat().st_size}]})
            config = read_json(Path(__file__).with_name('config_compact.json'))
            config.update(work_dir=str(root/'work'), links=str(root/'links.parquet'), queries=str(root/'queries.parquet'),
                          source_manifest=str(root/'corpus_manifest.json'), expected_queries=3, demo=True,
                          chunk_tokens=18, chunk_overlap=4, embedding_batch_size=2, vector_shard_rows=7)
            first = build_corpus(config, FixtureTokenizer(), FixtureEncoder.entry)
            second = build_corpus(config, FixtureTokenizer(), FixtureEncoder.entry)
            self.assertEqual(first['documents'], 3)
            self.assertGreater(first['chunks'], 3)
            self.assertEqual(first['chunks'], second['chunks'])
            corpus = handles.enter_context(closing(Corpus(root/'work/corpus.sqlite')))
            self.assertTrue(corpus.lexical('lung cancer', 10))
            self.assertTrue(corpus.lexical('肺癌', 10))
            for chunk_id in range(1, first['chunks']+1):
                chunk = corpus.chunk(chunk_id)
                self.assertIn(chunk['chunk_text'], corpus.document(chunk['doc_id']))
            repeated_text = {}
            repeated_pair = None
            for chunk_id in range(1, first['chunks']+1):
                c = corpus.chunk(chunk_id)
                key = (c['doc_id'], c['chunk_text'])
                if key in repeated_text and c['start'] > repeated_text[key]['end']:
                    repeated_pair = [repeated_text[key], c]
                    break
                repeated_text.setdefault(key, c)
            self.assertIsNotNone(repeated_pair)
            for position, candidate in enumerate(repeated_pair):
                candidate['score'] = 10-position
            duplicate_ranked = [{'id': 1, 'candidates': repeated_pair}]
            selected = select_predictions(duplicate_ranked, dict(config, context_neighbors=0), corpus)
            self.assertEqual(len(selected[0]['relevant_chunks']), 1)
            encoder = FixtureEncoder()
            class CrashingEncoder(FixtureEncoder):
                def __init__(self):
                    self.calls = 0
                def encode(self, texts, query=False):
                    self.calls += 1
                    if self.calls == 5:
                        raise RuntimeError('Simulated interruption after one completed vector shard')
                    return super().encode(texts, query)
            with self.assertRaises(RuntimeError):
                build_vectors(config, CrashingEncoder())
            self.assertEqual(read_json(root/'work/vector_state.json')['rows'], 7)
            # Change execution batch/checkpoint sizes on resume. A read buffer
            # crossing a vector-shard boundary must not skip or reorder IDs.
            config.update(embedding_read_batch_size=12,embedding_checkpoint_rows=10)
            vectors = build_vectors(config, encoder)
            # Fault boundary: final bytes are durable but the final part has not
            # moved from active to completed metadata yet.
            boundary = read_json(root/'work/vector_state.json')
            final_part = boundary['parts'].pop()
            boundary['active'] = {'name': final_part['name'], 'capacity': final_part['rows'],
                                  'written': final_part['rows'], 'start_last_id': 0,
                                  'rows_before': boundary['rows']-final_part['rows']}
            write_json(root/'work/vector_state.json', boundary)
            repeated = build_vectors(config, encoder)
            self.assertEqual(vectors, repeated)
            query_vectors = encoder.encode([texts[0][:30], texts[2][:30]])
            ranking = dense_search(root/'work', query_vectors, 5, block_rows=3, query_batch=1)
            all_ids, all_vectors = [], []
            for part in vectors['parts']:
                folder = root/'work/vectors'
                codes = np.load(folder/(part['name']+'.i8.npy')).astype(np.float32)
                norms = np.load(folder/(part['name']+'.norms.npy'))
                all_vectors.extend(codes / norms[:, None])
                all_ids.extend(np.load(folder/(part['name']+'.ids.npy')).tolist())
            scores = query_vectors @ np.asarray(all_vectors).T
            for q, returned in enumerate(ranking):
                expected_scores = sorted(scores[q].tolist(), reverse=True)[:5]
                np.testing.assert_allclose([s for _, s in returned], expected_scores, atol=1e-6)
            candidates = [{'id': q['id'], 'query': q['query'], 'candidates': [
                {'chunk_id': cid, 'fusion_score': 1/(rank+1)} for rank, (cid, score) in enumerate(ranking[(q['id']-1)%2])]} for q in questions]
            ranked = ranked_results(config, candidates)
            self.assertEqual(ranked, ranked_results(config, candidates))
            predictions = select_predictions(ranked, config, corpus)
            result = package(predictions, config, corpus)
            self.assertTrue(result['valid'])
            with zipfile.ZipFile(result['zip_file']) as archive:
                self.assertEqual(archive.namelist(), ['submission.json'])
            write_json(root/'config_test.json', config)
            write_json(root/'public_labels.json', predictions[:2])
            scored = subprocess.run([sys.executable, '-m', 'competition.pipeline', 'evaluate', '--config', str(root/'config_test.json'),
                                     '--qrels', str(root/'public_labels.json')], capture_output=True, text=True, timeout=30)
            self.assertEqual(scored.returncode, 0, scored.stderr)
            public_score = read_json(root/'work/evaluation.json')
            self.assertEqual(public_score['queries'], 2)
            self.assertEqual(public_score['submission_query_count'], 3)
            self.assertIsNone(public_score['final_score'])
            altered = copy.deepcopy(predictions)
            altered[0]['relevant_chunks'][0]['chunk_text'] += ' generated answer'
            with self.assertRaises(ValueError):
                validate(altered, questions, corpus)
            with self.assertRaises(ValueError):
                validate(predictions[:-1], questions, corpus)
            original = copy.deepcopy(predictions)
            original[0]['id'] = True
            with self.assertRaises(ValueError):
                validate(original, questions, corpus)
            # Full competition contract: all 1,200 IDs, including explicit empties.
            all_questions = [{'id': i, 'query': 'Query'} for i in range(1, 1201)]
            empties = [{'id': i, 'relevant_docs': [], 'relevant_chunks': []} for i in range(1, 1201)]
            self.assertEqual(validate(empties, all_questions, corpus)['queries'], 1200)
            corpus.close()
            # Reject stale sources instead of silently mixing checkpoints.
            changed = dict(config, chunk_tokens=19)
            with self.assertRaises(ValueError):
                build_corpus(changed, FixtureTokenizer(), encoder.entry)


if __name__ == '__main__':
    unittest.main(verbosity=2)
