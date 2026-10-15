"""Compare sorted microbatches to original embedding on multilingual chunks."""
import os
os.environ.setdefault('OMP_NUM_THREADS','2')
os.environ.setdefault('OPENBLAS_NUM_THREADS','2')
from pathlib import Path
import time
import numpy as np
from .core import ROOT, RunLock, load_config, read_json, write_json
from .corpus import Corpus
from .models import Encoder

def main():
    c=load_config(Path(__file__).with_name('config.json'))
    work=Path(c['work_dir'])
    with RunLock(ROOT/'competition/.system.lock'):
        import torch
        torch.set_num_threads(2)
        state=read_json(work/'vector_state.json')
        corpus=Corpus(work/'corpus.sqlite')
        texts=[]
        try:
            for after in [0,state['last_id'],4_000_000]:
                _,part=next(corpus.embedding_batches(after,128))
                texts.extend(part)
        finally:corpus.close()
        e=Encoder(c['embedding_model'],c['device'],4,c['embedding_max_tokens'],c.get('embedding_adapter'))
        try:
            e.encode(texts[:4]);torch.cuda.synchronize()
            runs=[];reference=None
            for batch,sorted_input in [(4,False),(16,False),(16,True)]:
                e.batch_size=batch;e.sort_by_length=sorted_input
                durations=[]
                for i in range(2):
                    torch.cuda.synchronize();start=time.perf_counter();v=e.encode(texts);torch.cuda.synchronize()
                    durations.append(time.perf_counter()-start)
                if reference is None:reference=v.copy()
                cosine=(reference*v).sum(1)
                report={'batch':e.batch_size,'sorted':sorted_input,'chunks_per_second':len(texts)/float(np.median(durations)),
                        'minimum_cosine_vs_original':float(cosine.min()),'max_component_difference':float(np.abs(reference-v).max()),
                        'seconds':durations,'finite':bool(np.isfinite(v).all())}
                runs.append(report);print(report,flush=True)
            if min(r['minimum_cosine_vs_original'] for r in runs)<.9995:
                raise ValueError('Accelerated embeddings differ too much from original')
            report={'complete':True,'samples':len(texts),'sources':'128 original chunks each from VI start, current cursor and EN end',
                    'original_id_order_preserved':True,'results':runs,'use_sorted':runs[2]['chunks_per_second']>runs[1]['chunks_per_second']*1.02,
                    'speedup_vs_original':runs[2]['chunks_per_second']/runs[0]['chunks_per_second']}
            write_json(ROOT/'competition/evidence/embedding_sorted_20261013.json',report)
        finally:e.close()

if __name__=='__main__':main()
