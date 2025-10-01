#!/usr/bin/env python3
"""
POC Iterative 05 - Phase 1: Test Hybrid Search on POC04 Index
Quick validation: Does hybrid search fix Query 3 catastrophic failure?
"""

import sys
import os
import json
import time
from typing import List, Dict, Tuple

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.vector_search.client import VectorSearchClient


# Same ground truth queries from POC04 benchmark
GROUND_TRUTH_QUERIES = [
    {
        "query": "tbl_bot_rspn_sbmt_log มี column อะไรบ้าง",
        "language": "Thai",
        "category": "table_structure",
        "relevant_keywords": ["tbl_bot_rspn_sbmt_log", "column", "LOG_ID", "SUBMISSION_ID", "RESPONSE_STATUS"],
    },
    {
        "query": "tbl_bot_rspn_sbmt_log columns structure",
        "language": "English",
        "category": "table_structure",
        "relevant_keywords": ["tbl_bot_rspn_sbmt_log", "columns", "structure", "LOG_ID", "SUBMISSION_ID"],
    },
    {
        "query": "LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร",
        "language": "Mixed",
        "category": "column_definition",
        "relevant_keywords": ["LOG_ID", "SUBMISSION_ID", "RESPONSE_STATUS", "column"],
        "critical": True  # This is the one that failed in POC04
    },
    {
        "query": "ตาราง BOT response log structure",
        "language": "Mixed",
        "category": "table_structure",
        "relevant_keywords": ["BOT", "response", "log", "structure", "table"],
    },
    {
        "query": "DataX OPM RDT Submission to BOT",
        "language": "English",
        "category": "document",
        "relevant_keywords": ["DataX", "OPM", "RDT", "Submission", "BOT"],
    }
]


def calculate_recall_at_k(retrieved_results: List[Dict], ground_truth: Dict, k: int) -> float:
    if not retrieved_results:
        return 0.0
    top_k = retrieved_results[:k]
    relevant_count = sum(1 for result in top_k
                        if any(kw.lower() in result.get('content', '').lower()
                              for kw in ground_truth['relevant_keywords']))
    return relevant_count / len(ground_truth['relevant_keywords'])


def calculate_precision_at_k(retrieved_results: List[Dict], ground_truth: Dict, k: int) -> float:
    if not retrieved_results:
        return 0.0
    top_k = retrieved_results[:k]
    relevant_count = sum(1 for result in top_k
                        if any(kw.lower() in result.get('content', '').lower()
                              for kw in ground_truth['relevant_keywords']))
    return relevant_count / len(top_k)


def execute_vector_search(vs_client, endpoint_name: str, index_name: str,
                          query: str, query_type: str, num_results: int = 10) -> Tuple[List[Dict], float]:
    """
    Execute vector search with specified query type
    query_type: "ANN" (semantic only) or "HYBRID" (semantic + keyword)
    """
    start_time = time.time()

    try:
        # Get index
        index = vs_client.get_index(
            endpoint_name=endpoint_name,
            index_name=index_name
        )

        # Perform search with query_type parameter
        response = index.similarity_search(
            query_text=query,
            columns=["id", "content"],
            num_results=num_results,
            query_type=query_type  # "ANN" or "HYBRID"
        )

        latency_ms = (time.time() - start_time) * 1000

        results = []
        # Handle both dict and object response
        if isinstance(response, dict) and 'result' in response:
            data_array = response['result'].get('data_array', [])
        elif hasattr(response, 'data_array'):
            data_array = response.data_array
        else:
            data_array = []

        if data_array:
            for row in data_array:
                results.append({
                    'id': row[0] if len(row) > 0 else None,
                    'content': row[1] if len(row) > 1 else '',
                    'score': row[2] if len(row) > 2 else 0.0
                })

        return results, latency_ms

    except Exception as e:
        print(f"    ERROR: {e}")
        return [], 0.0


def main():
    print("=" * 80)
    print("POC05 PHASE 1: HYBRID SEARCH VALIDATION")
    print("=" * 80)
    print("\nGoal: Test if hybrid search fixes Query 3 catastrophic failure (0% recall)")

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    print("\n[1] Connecting...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()

    vs_client = VectorSearchClient(
        workspace_url=config['databricks']['host'],
        personal_access_token=config['databricks']['token'],
        disable_notice=True
    )

    # Test POC04 index with both ANN and HYBRID
    poc04_index = config['rag_config']['poc04_index']
    endpoint_name = config['rag_config']['vector_search_endpoint']

    print(f"\n[2] Configuration:")
    print(f"    Index: {poc04_index}")
    print(f"    Test queries: {len(GROUND_TRUTH_QUERIES)}")
    print(f"    Query types: ANN (semantic only) vs HYBRID (semantic + keyword)")

    # Run comparison
    results_comparison = []

    for idx, gt_query in enumerate(GROUND_TRUTH_QUERIES, 1):
        query_text = gt_query['query']
        is_critical = gt_query.get('critical', False)

        print(f"\n{'='*80}")
        if is_critical:
            print(f"[Query {idx}/5] CRITICAL TEST: {query_text}")
        else:
            print(f"[Query {idx}/5] {query_text}")
        print(f"Language: {gt_query['language']} | Category: {gt_query['category']}")

        # Test ANN (semantic only)
        print(f"\n  ANN (Semantic Only):")
        retrieved_ann, latency_ann = execute_vector_search(
            vs_client, endpoint_name, poc04_index, query_text, "ANN", num_results=10
        )
        recall_ann = calculate_recall_at_k(retrieved_ann, gt_query, 3)
        precision_ann = calculate_precision_at_k(retrieved_ann, gt_query, 3)
        print(f"    Latency: {latency_ann:.0f}ms | Results: {len(retrieved_ann)}")
        print(f"    Recall@3: {recall_ann:.3f} | Precision@3: {precision_ann:.3f}")
        if retrieved_ann:
            print(f"    Top score: {retrieved_ann[0]['score']:.6f}")

        # Test HYBRID (semantic + keyword)
        print(f"\n  HYBRID (Semantic + Keyword):")
        retrieved_hybrid, latency_hybrid = execute_vector_search(
            vs_client, endpoint_name, poc04_index, query_text, "HYBRID", num_results=10
        )
        recall_hybrid = calculate_recall_at_k(retrieved_hybrid, gt_query, 3)
        precision_hybrid = calculate_precision_at_k(retrieved_hybrid, gt_query, 3)
        print(f"    Latency: {latency_hybrid:.0f}ms | Results: {len(retrieved_hybrid)}")
        print(f"    Recall@3: {recall_hybrid:.3f} | Precision@3: {precision_hybrid:.3f}")
        if retrieved_hybrid:
            print(f"    Top score: {retrieved_hybrid[0]['score']:.6f}")

        # Show improvement
        recall_improvement = ((recall_hybrid - recall_ann) / (recall_ann + 0.001)) * 100
        precision_improvement = ((precision_hybrid - precision_ann) / (precision_ann + 0.001)) * 100

        print(f"\n  HYBRID IMPROVEMENT:")
        print(f"    Recall: {recall_improvement:+.1f}%")
        print(f"    Precision: {precision_improvement:+.1f}%")

        if is_critical:
            if recall_hybrid > 0:
                print(f"    STATUS: CRITICAL FAILURE FIXED! (was 0%, now {recall_hybrid:.1%})")
            else:
                print(f"    STATUS: Still failed (0% recall)")

        results_comparison.append({
            'query': query_text,
            'language': gt_query['language'],
            'critical': is_critical,
            'ann': {
                'recall': recall_ann,
                'precision': precision_ann,
                'latency_ms': latency_ann,
                'results_count': len(retrieved_ann),
                'top_score': retrieved_ann[0]['score'] if retrieved_ann else 0
            },
            'hybrid': {
                'recall': recall_hybrid,
                'precision': precision_hybrid,
                'latency_ms': latency_hybrid,
                'results_count': len(retrieved_hybrid),
                'top_score': retrieved_hybrid[0]['score'] if retrieved_hybrid else 0
            },
            'improvement': {
                'recall_pct': recall_improvement,
                'precision_pct': precision_improvement
            }
        })

    # Summary
    print("\n" + "=" * 80)
    print("PHASE 1 RESULTS: ANN vs HYBRID")
    print("=" * 80)

    import statistics
    avg_recall_ann = statistics.mean([r['ann']['recall'] for r in results_comparison])
    avg_recall_hybrid = statistics.mean([r['hybrid']['recall'] for r in results_comparison])
    avg_precision_ann = statistics.mean([r['ann']['precision'] for r in results_comparison])
    avg_precision_hybrid = statistics.mean([r['hybrid']['precision'] for r in results_comparison])

    print(f"\nAverage Metrics:")
    print(f"  Recall@3:")
    print(f"    ANN (semantic only): {avg_recall_ann:.3f}")
    print(f"    HYBRID (semantic + keyword): {avg_recall_hybrid:.3f}")
    print(f"    Improvement: {((avg_recall_hybrid - avg_recall_ann)/avg_recall_ann)*100:+.1f}%")

    print(f"\n  Precision@3:")
    print(f"    ANN: {avg_precision_ann:.3f}")
    print(f"    HYBRID: {avg_precision_hybrid:.3f}")
    print(f"    Improvement: {((avg_precision_hybrid - avg_precision_ann)/avg_precision_ann)*100:+.1f}%")

    # Critical query check
    critical_query = [r for r in results_comparison if r.get('critical')][0]
    print(f"\n" + "=" * 80)
    print("CRITICAL QUERY TEST (Query 3)")
    print("=" * 80)
    print(f"Query: {critical_query['query']}")
    print(f"POC04 (ANN): {critical_query['ann']['recall']:.1%} recall")
    print(f"POC05 (HYBRID): {critical_query['hybrid']['recall']:.1%} recall")

    if critical_query['hybrid']['recall'] > 0:
        print(f"\nSUCCESS: Hybrid search FIXED the catastrophic failure!")
        print(f"Query 3 went from 0% to {critical_query['hybrid']['recall']:.1%} recall")
    else:
        print(f"\nFAILURE: Hybrid search did not fix Query 3")

    # Decision
    print(f"\n" + "=" * 80)
    print("PHASE 1 DECISION")
    print("=" * 80)

    if avg_recall_hybrid > avg_recall_ann * 1.2:
        print(f"\nHYBRID SEARCH: SIGNIFICANT IMPROVEMENT (>20%)")
        print(f"Recommendation: Proceed to Phase 2 - Test embedding models")
    elif avg_recall_hybrid > avg_recall_ann:
        print(f"\nHYBRID SEARCH: MODERATE IMPROVEMENT")
        print(f"Recommendation: Proceed to Phase 2 - Test embedding models")
    else:
        print(f"\nHYBRID SEARCH: NO IMPROVEMENT")
        print(f"Recommendation: Investigate other strategies")

    # Save results
    output_file = os.path.join(os.path.dirname(__file__), 'phase1_ann_vs_hybrid.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'summary': {
                'ann_avg_recall': avg_recall_ann,
                'hybrid_avg_recall': avg_recall_hybrid,
                'ann_avg_precision': avg_precision_ann,
                'hybrid_avg_precision': avg_precision_hybrid,
                'recall_improvement_pct': ((avg_recall_hybrid - avg_recall_ann)/avg_recall_ann)*100,
                'precision_improvement_pct': ((avg_precision_hybrid - avg_precision_ann)/avg_precision_ann)*100
            },
            'query_results': results_comparison
        }, f, indent=2, ensure_ascii=False)

    print(f"\nResults saved: {output_file}")

    spark.stop()


if __name__ == "__main__":
    main()
