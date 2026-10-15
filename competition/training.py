"""Train a small retrieval adapter on weak, source-derived inverse-cloze pairs.

The public BGE model stays frozen. The 1,024 learned positive diagonal weights
adapt its embeddings. No competition query, answer key, hosted LLM, or generated
chunk enters training. Validation measures this proxy task, not competition F2.
"""
from __future__ import annotations

from collections import Counter
import gc
import hashlib
import os
from pathlib import Path
import random
import re
from urllib.parse import urlsplit

import numpy as np
import orjson

from .core import MAX_PARAMETERS, digest, file_hash, read_json, runtime_status, write_json
from .models import Encoder, approved_model


def weak_pair(record):
    text=record['clean_text']
    host=urlsplit(record['url']).hostname or ''
    language='en' if host=='pubmed.ncbi.nlm.nih.gov' else 'zh' if len(re.findall(r'[\u3400-\u9fff]',text[:700]))>30 else 'vi'
    boundary=re.search(r'[。！？]|[.!?](?:\s|$)',text[:240])
    if not boundary:
        return None
    start=0
    end=boundary.end()
    while end>start and text[end-1].isspace():
        end-=1
    query=text[start:end].strip()
    passage=text[end:].lstrip()[:1200]
    if not 24 <= len(query) <= 240 or len(passage)<300 or sum(c.isalpha() for c in query)<len(query)*.55:
        return None
    if re.search(r'copyright|hotline|đăng nhập|bản quyền|tốt nghiệp|39健康|来源|点击|栏目关注|网友',query,re.I):
        return None
    if language!='zh' and len(query.split())<5:
        return None
    passage_start=text.find(passage,end)
    return {'doc_id':record['doc_id'],'url':record['url'],'language':language,
            'query':query,'passage':passage,'query_span':[start,end],
            'passage_span':[passage_start,passage_start+len(passage)],
            'group':hashlib.sha256(text.encode()).hexdigest(),
            'label_kind':'weak_same_document_context; not a BTC relevance label'}


def prepare_pairs(config, output):
    settings=config['training']
    limit=settings['examples_per_language']
    randomizer=random.Random(settings['seed'])
    manifest=read_json(config['source_manifest'])
    parent=Path(config['source_manifest']).parent
    buckets={k:[] for k in ['vi','en','zh']}
    counts=Counter()
    seen_queries=set()
    scanned=0
    for shard in manifest['shards']:
        path=parent/shard['file']
        if path.stat().st_size!=shard['bytes']:
            raise ValueError('Training source differs from frozen snapshot')
        with path.open('rb') as f:
            for line in f:
                scanned+=1
                row=orjson.loads(line)
                # Stable source sampling keeps preparation bounded in memory.
                if int(hashlib.sha256(str(row['doc_id']).encode()).hexdigest()[:8],16)%16:
                    continue
                pair=weak_pair(row)
                if not pair or pair['query'].lower() in seen_queries:
                    continue
                seen_queries.add(pair['query'].lower())
                language=pair['language']
                counts[language]+=1
                bucket=buckets[language]
                if len(bucket)<limit:
                    bucket.append(pair)
                else:
                    index=randomizer.randrange(counts[language])
                    if index<limit:
                        bucket[index]=pair
        runtime_status(config['work_dir'],'preparing_training_pairs',scanned_documents=scanned,
                       sampled_by_language={k:len(v) for k,v in buckets.items()})
    if min(map(len,buckets.values()))<40:
        raise ValueError('Need at least 40 distinct weak pairs per language')
    rows=[]
    for language,bucket in buckets.items():
        # Group split forbids duplicate document contents crossing the holdout.
        for row in bucket:
            row['split']='validation' if int(row['group'][:8],16)%5==0 else 'train'
            rows.append(row)
    groups={split:{r['group'] for r in rows if r['split']==split} for split in ['train','validation']}
    if groups['train']&groups['validation']:
        raise ValueError('Training/validation document leakage')
    output.parent.mkdir(parents=True,exist_ok=True)
    with output.open('wb') as f:
        for row in rows:
            f.write(orjson.dumps(row)+b'\n')
    info={'dataset_type':'inverse-cloze weak same-document pairs',
          'source_manifest':config['source_manifest'],'source_manifest_sha256':file_hash(config['source_manifest']),
          'source_links_sha256':file_hash(config['links']),'source_url':'https://huggingface.co/datasets/AIGuruTinix/ViBioMIR',
          'external_training_data':[],'competition_queries_used':False,'reference_labels_used':False,
          'generated_text_used':False,'scanned_documents':scanned,'pairs':len(rows),
          'counts':dict(Counter(r['language']+'/'+r['split'] for r in rows)),
          'sha256':file_hash(output),'seed':settings['seed'],
          'limitation':'Weak same-document contexts can be noisy; validation is monolingual and cannot establish cross-language F2 or leaderboard score.'}
    write_json(output.with_suffix('.manifest.json'),info)
    return rows,info


def proxy_metrics(query_vectors, passage_vectors, rows, weights):
    q=query_vectors*weights
    p=passage_vectors*weights
    q=q/np.maximum(np.linalg.norm(q,axis=1,keepdims=True),1e-12)
    p=p/np.maximum(np.linalg.norm(p,axis=1,keepdims=True),1e-12)
    scores=q@p.T
    diagonal=scores.diagonal()
    rank=(scores>diagonal[:,None]).sum(1)+1
    language={lang:{'recall_at_1':float(np.mean(rank[[r['language']==lang for r in rows]]<=1)),
                    'recall_at_5':float(np.mean(rank[[r['language']==lang for r in rows]]<=5))}
              for lang in ['vi','en','zh']}
    return {'task':'held-out inverse-cloze; not BTC evaluation', 'pairs':len(rows),'by_language':language,
            'macro_recall_at_1':float(np.mean([s['recall_at_1'] for s in language.values()])),
            'macro_recall_at_5':float(np.mean([s['recall_at_5'] for s in language.values()]))}


def optimize_projection(query_vectors, passage_vectors, rows, settings, progress=None):
    import torch
    torch.manual_seed(settings['seed'])
    torch.set_num_threads(2)
    train_ids=[i for i,r in enumerate(rows) if r['split']=='train']
    val_ids=[i for i,r in enumerate(rows) if r['split']=='validation']
    if not train_ids or any(not any(rows[i]['language']==lang for i in val_ids) for lang in ['vi','en','zh']):
        raise ValueError('Holdout needs all three languages')
    val_rows=[rows[i] for i in val_ids]
    baseline=proxy_metrics(query_vectors[val_ids],passage_vectors[val_ids],val_rows,np.ones(query_vectors.shape[1],dtype=np.float32))
    q=torch.from_numpy(query_vectors[train_ids].copy())
    p=torch.from_numpy(passage_vectors[train_ids].copy())
    log_weights=torch.nn.Parameter(torch.zeros(q.shape[1]))
    optimizer=torch.optim.Adam([log_weights],lr=settings['learning_rate'])
    generator=torch.Generator().manual_seed(settings['seed'])
    history=[]
    best=None
    for epoch in range(settings['epochs']):
        losses=[]
        order=torch.randperm(len(q),generator=generator)
        for start in range(0,len(order),64):
            ids=order[start:start+64]
            if len(ids)<2:
                continue
            weights=log_weights.exp()
            queries=torch.nn.functional.normalize(q[ids]*weights,dim=1)
            passages=torch.nn.functional.normalize(p[ids]*weights,dim=1)
            logits=queries@passages.T/.05
            labels=torch.arange(len(ids))
            loss=(torch.nn.functional.cross_entropy(logits,labels)+torch.nn.functional.cross_entropy(logits.T,labels))/2
            loss=loss+.03*log_weights.square().mean()
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            with torch.no_grad():log_weights.clamp_(-.25,.25)
            losses.append(loss.item())
        weights=log_weights.detach().exp().numpy().copy()
        metrics=proxy_metrics(query_vectors[val_ids],passage_vectors[val_ids],val_rows,weights)
        item={'epoch':epoch+1,'train_loss':float(np.mean(losses)),'validation':metrics}
        history.append(item)
        if progress:progress(item)
        if best is None or (metrics['macro_recall_at_1'],metrics['macro_recall_at_5'])>(best['validation']['macro_recall_at_1'],best['validation']['macro_recall_at_5']):
            best=dict(item,weights=weights)
    selected=(best['validation']['macro_recall_at_1']>baseline['macro_recall_at_1'] and
              best['validation']['macro_recall_at_5']>=baseline['macro_recall_at_5'] and
              all(best['validation']['by_language'][lang]['recall_at_1']>=baseline['by_language'][lang]['recall_at_1']-.02 for lang in ['vi','en','zh']))
    return best['weights'],{'baseline':baseline,'best_validation':best['validation'],'best_epoch':best['epoch'],
                            'selected_for_retrieval':bool(selected),'history':history,
                            'selection_rule':'Improve held-out macro R@1, preserve macro R@5 and limit each-language R@1 decrease to 0.02; otherwise retain original BGE embeddings.'}


def train(config):
    settings=config.get('training')
    if not settings:
        return None
    if settings['mode']!='inverse_cloze_diagonal':
        raise ValueError('Unsupported training method')
    work=Path(config['work_dir'])
    directory=work/'training'
    directory.mkdir(parents=True,exist_ok=True)
    base=approved_model(config['embedding_model'])
    identity=digest({'source':file_hash(config['source_manifest']),'base':base,'settings':settings,
                     'max_tokens':config['embedding_max_tokens'],'implementation':'inverse-cloze-diagonal-v1'})
    report_path=directory/'training_report.json'
    adapter_path=Path(config['embedding_adapter'])
    if report_path.exists() and read_json(report_path).get('identity')==identity:
        prior=read_json(report_path)
        if adapter_path.exists() and file_hash(adapter_path)==prior['adapter_sha256']:
            return prior
    pairs_path=directory/'pairs.jsonl'
    pairs_receipt=pairs_path.with_suffix('.manifest.json')
    if pairs_path.exists() and pairs_receipt.exists():
        info=read_json(pairs_receipt)
        if info['source_manifest_sha256']!=file_hash(config['source_manifest']) or info['sha256']!=file_hash(pairs_path):
            raise ValueError('Training pair checkpoint/source mismatch')
        rows=[orjson.loads(line) for line in pairs_path.open('rb')]
    else:
        rows,info=prepare_pairs(config,pairs_path)
    vector_path=directory/'base_vectors.npy'
    vector_state=directory/'embedding_checkpoint.json'
    if vector_state.exists():
        state=read_json(vector_state)
        if state['identity']!=identity or state['pairs_sha256']!=info['sha256']:
            raise ValueError('Training embedding checkpoint mismatch')
        vectors=np.lib.format.open_memmap(vector_path,mode='r+')
        done=state['completed_pairs']
    else:
        vectors=np.lib.format.open_memmap(vector_path,mode='w+',dtype=np.float32,shape=(len(rows),2,base['dimension']))
        state={'identity':identity,'pairs_sha256':info['sha256'],'completed_pairs':0}
        done=0
        write_json(vector_state,state)
    if done<len(rows):
        runtime_status(work,'loading_frozen_model_for_training',completed_pairs=done,total_pairs=len(rows))
        encoder=Encoder(config['embedding_model'],config['device'],config['embedding_batch_size'],config['embedding_max_tokens'])
        try:
            for start in range(done,len(rows),32):
                batch=rows[start:start+32]
                vectors[start:start+len(batch),0]=encoder.encode([r['query'] for r in batch],query=True)
                vectors[start:start+len(batch),1]=encoder.encode([r['passage'] for r in batch])
                vectors.flush()
                state['completed_pairs']=start+len(batch)
                write_json(vector_state,state)
                runtime_status(work,'encoding_training_pairs',completed_pairs=state['completed_pairs'],total_pairs=len(rows))
        finally:encoder.close()
    runtime_status(work,'training_retrieval_adapter',pairs=len(rows),trainable_parameters=base['dimension'])
    weights,metrics=optimize_projection(np.asarray(vectors[:,0]).copy(),np.asarray(vectors[:,1]).copy(),rows,settings,
                  lambda item:runtime_status(work,'training_retrieval_adapter',epoch=item['epoch'],epochs=settings['epochs'],train_loss=item['train_loss'],validation_proxy=item['validation']))
    adapter={'version':'positive-diagonal-v1','base_weights_sha256':base['weights_sha256'],'dimension':base['dimension'],
             'parameter_count':len(weights),'weights':weights.tolist(),'selected':metrics['selected_for_retrieval'],
             'training_identity':identity,'label_kind':'source-derived weak pairs; no BTC labels'}
    if base['parameter_count']+len(weights)>MAX_PARAMETERS:
        raise ValueError('Base model plus adapter exceeds model parameter limit')
    write_json(adapter_path,adapter)
    report={'identity':identity,'method':'Frozen public BGE-M3 plus a trained diagonal retrieval adapter',
            'base_model':base,'trainable_parameters':len(weights),'maximum_total_parameters':base['parameter_count']+len(weights),
            'data':info,'adapter_path':str(adapter_path),'adapter_sha256':file_hash(adapter_path),
            'settings':settings,**metrics,'competition_score':None,'closed_model_used':False,
            'limitation':'Held-out weak context retrieval is not document/chunk F2 on Vietnamese competition questions. It cannot verify the >0.5 target.'}
    write_json(report_path,report)
    del vectors
    gc.collect()
    return report
