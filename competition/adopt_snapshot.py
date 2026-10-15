"""Adopt appended crawl shards only after verifying every indexed source byte.

This preserves an interrupted lexical index. Published vector/ranking indices
are deliberately ineligible; those need their own new work directory.
"""
import argparse
from pathlib import Path
import zlib

from .core import ROOT, RunLock, connect, digest, file_hash, load_config, read_json, write_json
from .models import approved_model


def adopt(config, prior_path):
    work = Path(config['work_dir'])
    if any((work/name).exists() for name in ['corpus_index.json','vector_state.json','vector_manifest.json','candidates.json','ranking.sqlite']):
        raise ValueError('Only an interrupted source import may adopt an appended snapshot')
    old = read_json(prior_path)
    new = read_json(config['source_manifest'])
    old_shards = {s['file']: s['bytes'] for s in old['shards']}
    new_shards = {s['file']: s['bytes'] for s in new['shards']}
    if not old_shards.items() <= new_shards.items() or old['source_fingerprint'] != new['source_fingerprint']:
        raise ValueError('Existing shard sizes or source links changed')
    common = {'links_sha256':file_hash(config['links']), 'tokenizer_model':approved_model(config['embedding_model']),
              'chunk_tokens':config['chunk_tokens'], 'chunk_overlap':config['chunk_overlap']}
    previous = digest(dict(common,manifest_sha256=file_hash(prior_path)))
    current = digest(dict(common,manifest_sha256=file_hash(config['source_manifest'])))
    db = connect(work/'corpus.sqlite')
    try:
        actual = db.execute("SELECT value FROM meta WHERE key='fingerprint'").fetchone()[0]
        if actual != previous:
            raise ValueError('Previous snapshot/tokenizer fingerprint does not match checkpoint')
        verified = set()
        hashes = {}
        parent = Path(config['source_manifest']).parent
        for filename, offset in db.execute('SELECT file,offset FROM imports').fetchall():
            if filename not in old_shards or not 0 <= offset <= old_shards[filename]:
                raise ValueError('Import is outside the previous snapshot')
            path = parent/filename
            if path.stat().st_size != old_shards[filename]:
                raise ValueError('Previously indexed shard changed size')
            with path.open('rb') as f:
                while f.tell() < offset:
                    row = __import__('orjson').loads(f.readline())
                    indexed = db.execute('SELECT docs.body,sources.url FROM docs JOIN sources USING(doc_id) WHERE doc_id=?',(row['doc_id'],)).fetchone()
                    if not indexed or indexed[1] != row['url'] or zlib.decompress(indexed[0]).decode() != row['clean_text']:
                        raise ValueError(f"Indexed source changed: {row['doc_id']}")
                    verified.add(row['doc_id'])
                if f.tell() != offset:
                    raise ValueError('Import offset is not a JSONL boundary')
            hashes[filename]=file_hash(path)
            print(f'Verified {filename}: {len(verified):,} original indexed documents',flush=True)
        if len(verified) != db.execute('SELECT count(*) FROM docs').fetchone()[0]:
            raise ValueError('Some indexed documents were not verified against source')
        report={'previous_fingerprint':previous,'current_fingerprint':current,
                'previous_manifest_sha256':file_hash(prior_path),'new_manifest_sha256':file_hash(config['source_manifest']),
                'previous_saved':old['counts']['saved'],'new_saved':new['counts']['saved'],
                'verified_indexed_documents':len(verified),'verified_shards_sha256':hashes,
                'validation':'Every imported original ID, URL and full source text matched the current frozen JSONL prefix; tokenizer and links unchanged.'}
        write_json(work/'snapshot_transition.json',report)
        db.execute("UPDATE meta SET value=? WHERE key='fingerprint'",(current,))
        db.commit()
        return report
    finally:
        db.close()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path,default=Path(__file__).with_name('config.json'))
    p.add_argument('--prior-manifest',type=Path,required=True)
    args=p.parse_args()
    with RunLock(ROOT/'competition/.system.lock'):
        print(adopt(load_config(args.config),args.prior_manifest))


if __name__=='__main__':
    main()
