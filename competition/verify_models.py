"""Check the main models on three isolated language fixtures, not leaderboard labels."""
from pathlib import Path
import sys

from .core import write_json
from .models import Encoder, Reranker, approved_model


def main():
    query = "Chẩn đoán ung thư phổi"
    passages = ["Lung cancer is a malignant tumor of the lungs. Biomedical literature discusses lung cancer diagnosis.",
                "肺癌是肺部的恶性肿瘤。医学文献介绍肺癌的诊断和临床研究。",
                "Tài liệu này nói về bệnh đái tháo đường và nồng độ đường trong máu."]
    encoder = Encoder('bge_m3', 'cuda', batch_size=1, max_tokens=256)
    try:
        query_vector = encoder.encode([query], query=True)
        document_vectors = encoder.encode(passages)
        cosine = (query_vector @ document_vectors.T)[0].tolist()
    finally:
        encoder.close()
    reranker = Reranker('reranker_m3', 'cuda', batch_size=1)
    try:
        scores = reranker.score(query, passages)
    finally:
        reranker.close()
    valid = min(scores[:2]) > scores[2]
    report = {"embedding": approved_model('bge_m3'), "reranker": approved_model('reranker_m3'),
              "languages": ['English', 'Chinese', 'Vietnamese distractor'], "cosine_scores": cosine,
              "reranker_logits": scores, "english_chinese_relevance_above_distractor": valid,
              "fixture_only": True, "competition_score": None}
    write_json(Path(__file__).parent / 'evidence' / 'main_models_runtime_check.json', report)
    print(report)
    if not valid:
        raise AssertionError('Cross-language reranking fixture failed')


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    main()
