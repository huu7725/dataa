"""Compare BGE-M3 pooling modes on held-out source-derived pairs before re-embedding."""
import os
os.environ.setdefault('OMP_NUM_THREADS','2')
os.environ.setdefault('OPENBLAS_NUM_THREADS','2')
from pathlib import Path
import time
import numpy as np
import orjson
from .core import ROOT,RunLock,load_config,read_json,write_json
from .models import Encoder

def score(q,p,rows):
    q=q/np.linalg.norm(q,axis=1,keepdims=True);p=p/np.linalg.norm(p,axis=1,keepdims=True)
    sim=q@p.T;rank=(sim>np.diag(sim)[:,None]).sum(1)+1
    result={}
    for lang in ['vi','en','zh']:
        ix=np.asarray([r['language']==lang for r in rows])
        result[lang]={'n':int(ix.sum()),'r1':float(np.mean(rank[ix]<=1)),'r5':float(np.mean(rank[ix]<=5))}
    return {'by_language':result,'macro_r1':float(np.mean([v['r1'] for v in result.values()])),'macro_r5':float(np.mean([v['r5'] for v in result.values()]))}

def main():
    config=load_config(Path(__file__).with_name('config.json'));work=Path(config['work_dir'])
    rows=[orjson.loads(line) for line in (work/'training/pairs.jsonl').open('rb')]
    rows=[r for r in rows if r['split']=='validation']
    with RunLock(ROOT/'competition/.system.lock'):
        import torch
        encoder=Encoder(config['embedding_model'],config['device'],16,config['embedding_max_tokens'],config.get('embedding_adapter'),True)
        try:
            query=[r['query'] for r in rows];passages=[r['passage'] for r in rows]
            results={}
            for pooling in ['cls','mean']:
                encoder.entry=dict(encoder.entry,pooling=pooling)
                torch.cuda.empty_cache();start=time.perf_counter()
                q=encoder.encode(query,query=True);p=encoder.encode(passages)
                torch.cuda.synchronize()
                results[pooling]={'metrics':score(q,p,rows),'seconds':round(time.perf_counter()-start,2)}
                print(pooling,results[pooling],flush=True)
            cls=results['cls']['metrics'];mean=results['mean']['metrics']
            results['recommendation']='mean' if (mean['macro_r1'],mean['macro_r5'])>(cls['macro_r1'],cls['macro_r5']) else 'cls'
            results['source']='Held-out inverse-cloze pairs only; not competition F2.'
            write_json(ROOT/'competition/evidence/pooling_benchmark_20261015.json',results)
            print('recommendation',results['recommendation'],flush=True)
        finally:encoder.close()

if __name__=='__main__':main()
