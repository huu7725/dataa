"""Measure GPU batch throughput and numerical compatibility on original chunks."""
import os
os.environ.setdefault('OMP_NUM_THREADS','2')
os.environ.setdefault('OPENBLAS_NUM_THREADS','2')
from pathlib import Path
import time

import numpy as np

from .core import ROOT, RunLock, file_hash, load_config, read_json, write_json
from .corpus import Corpus
from .models import Encoder


def main():
    config=load_config(Path(__file__).with_name('config.json'))
    work=Path(config['work_dir'])
    with RunLock(ROOT/'competition/.system.lock'):
        import torch
        torch.set_num_threads(2)
        state=read_json(work/'vector_state.json')
        corpus=Corpus(work/'corpus.sqlite')
        try:
            _,texts=next(corpus.embedding_batches(state['last_id'],128))
        finally:corpus.close()
        encoder=Encoder(config['embedding_model'],config['device'],4,config['embedding_max_tokens'],config.get('embedding_adapter'))
        try:
            encoder.encode(texts[:4])
            torch.cuda.synchronize()
            results=[]
            reference=None
            for batch in [4,8,12,16,24,32]:
                encoder.batch_size=batch
                torch.cuda.empty_cache()
                torch.cuda.reset_peak_memory_stats()
                timings=[]
                for repeat in range(3):
                    torch.cuda.synchronize()
                    start=time.perf_counter()
                    vectors=encoder.encode(texts)
                    torch.cuda.synchronize()
                    timings.append(time.perf_counter()-start)
                seconds=float(np.median(timings))
                if reference is None:reference=vectors.copy()
                cosine=np.sum(reference*vectors,axis=1)
                free,total=torch.cuda.mem_get_info()
                result={'requested_batch':batch,'effective_batch':encoder.batch_size,
                        'chunks_per_second':len(texts)/seconds,'seconds_per_128':seconds,
                        'timings':timings,'peak_allocated_mb':torch.cuda.max_memory_allocated()/1048576,
                        'peak_reserved_mb':torch.cuda.max_memory_reserved()/1048576,
                        'free_vram_mb_after':free/1048576,
                        'minimum_cosine_vs_batch4':float(cosine.min()),
                        'maximum_component_difference':float(np.abs(vectors-reference).max())}
                results.append(result)
                print(result,flush=True)
                write_json(ROOT/'competition/evidence/embedding_speed_20261013.json',
                           {'sample':'Next 128 original corpus chunks at the saved embedding cursor',
                            'sample_after_id':state['last_id'],'chunks':len(texts),'model':encoder.entry,
                            'attention_implementation':encoder.model.config._attn_implementation,
                            'results':results,'complete':False})
            eligible=[r for r in results if r['requested_batch']==r['effective_batch'] and r['minimum_cosine_vs_batch4']>=.9995 and r['free_vram_mb_after']>=400]
            best=max(eligible,key=lambda r:r['chunks_per_second'])
            # Choose the smallest stable batch within 3% of the highest speed.
            best=min((r for r in eligible if r['chunks_per_second']>=best['chunks_per_second']*.97),key=lambda r:r['requested_batch'])
            report={'sample':'Next 128 original corpus chunks at the saved embedding cursor',
                    'sample_after_id':state['last_id'],'chunks':len(texts),'model':encoder.entry,
                    'attention_implementation':encoder.model.config._attn_implementation,
                    'results':results,'complete':True,'recommended_batch':best['requested_batch'],
                    'speedup_vs_batch4':best['chunks_per_second']/results[0]['chunks_per_second'],
                    'limitation':'Short local throughput sample; whole-corpus speed depends on chunk length, RAM paging and laptop GPU power.'}
            write_json(ROOT/'competition/evidence/embedding_speed_20261013.json',report)
            print('RECOMMENDATION',report['recommended_batch'],report['speedup_vs_batch4'],flush=True)
        finally:encoder.close()


if __name__=='__main__':
    main()
