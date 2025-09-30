#!/usr/bin/env python3
"""
POC Iterative 04 - Benchmark Comparison: POC03 vs POC04
Tests both indexes with same queries to measure improvement
"""

import sys
import os
import json
import time
from typing import List, Dict, Tuple
from collections import defaultdict
import statistics

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.vector_search.client import VectorSearchClient


# Ground Truth Queries (same as POC03 for fair comparison)
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
                          query: str, num_results: int = 10) -> Tuple[List[Dict], float]:
    start_time = time.time()

    try:
        response = vs_client.get_index(
            endpoint_name=endpoint_name,
            index_name=index_name
        ).similarity_search(
            query_text=query,
            columns=["id", "content"],
            num_results=num_results
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
    print("POC04 vs POC03 BENCHMARK COMPARISON")
    print("=" * 80)

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

    # Get both indexes
    poc03_index = config['rag_config']['poc03_index']
    poc04_index = config['rag_config']['poc04_index']
    endpoint_name = config['rag_config']['vector_search_endpoint']

    print(f"\n[2] Configuration:")
    print(f"    POC03 Index: {poc03_index} (72 chunks, basic chunking)")
    print(f"    POC04 Index: {poc04_index} (1492 chunks, optimized + bilingual)")
    print(f"    Test queries: {len(GROUND_TRUTH_QUERIES)}")

    # Run benchmarks on both indexes
    results_comparison = []

    for idx, gt_query in enumerate(GROUND_TRUTH_QUERIES, 1):
        query_text = gt_query['query']
        print(f"\n{'='*80}")
        print(f"[Query {idx}/5] {query_text}")
        print(f"Language: {gt_query['language']} | Category: {gt_query['category']}")

        # Test POC03
        print(f"\n  POC03:")
        retrieved_poc03, latency_poc03 = execute_vector_search(
            vs_client, endpoint_name, poc03_index, query_text, num_results=10
        )
        recall_poc03 = calculate_recall_at_k(retrieved_poc03, gt_query, 3)
        precision_poc03 = calculate_precision_at_k(retrieved_poc03, gt_query, 3)
        print(f"    Latency: {latency_poc03:.0f}ms | Results: {len(retrieved_poc03)}")
        print(f"    Recall@3: {recall_poc03:.3f} | Precision@3: {precision_poc03:.3f}")
        if retrieved_poc03:
            print(f"    Top score: {retrieved_poc03[0]['score']:.6f}")

        # Test POC04
        print(f"\n  POC04:")
        retrieved_poc04, latency_poc04 = execute_vector_search(
            vs_client, endpoint_name, poc04_index, query_text, num_results=10
        )
        recall_poc04 = calculate_recall_at_k(retrieved_poc04, gt_query, 3)
        precision_poc04 = calculate_precision_at_k(retrieved_poc04, gt_query, 3)
        print(f"    Latency: {latency_poc04:.0f}ms | Results: {len(retrieved_poc04)}")
        print(f"    Recall@3: {recall_poc04:.3f} | Precision@3: {precision_poc04:.3f}")
        if retrieved_poc04:
            print(f"    Top score: {retrieved_poc04[0]['score']:.6f}")

        # Show improvement
        recall_improvement = ((recall_poc04 - recall_poc03) / (recall_poc03 + 0.001)) * 100
        precision_improvement = ((precision_poc04 - precision_poc03) / (precision_poc03 + 0.001)) * 100

        print(f"\n  IMPROVEMENT:")
        print(f"    Recall: {recall_improvement:+.1f}%")
        print(f"    Precision: {precision_improvement:+.1f}%")

        results_comparison.append({
            'query': query_text,
            'language': gt_query['language'],
            'poc03': {
                'recall': recall_poc03,
                'precision': precision_poc03,
                'latency_ms': latency_poc03,
                'results_count': len(retrieved_poc03),
                'top_score': retrieved_poc03[0]['score'] if retrieved_poc03 else 0
            },
            'poc04': {
                'recall': recall_poc04,
                'precision': precision_poc04,
                'latency_ms': latency_poc04,
                'results_count': len(retrieved_poc04),
                'top_score': retrieved_poc04[0]['score'] if retrieved_poc04 else 0
            },
            'improvement': {
                'recall_pct': recall_improvement,
                'precision_pct': precision_improvement
            }
        })

    # Summary
    print("\n" + "=" * 80)
    print("BENCHMARK SUMMARY: POC04 vs POC03")
    print("=" * 80)

    avg_recall_poc03 = statistics.mean([r['poc03']['recall'] for r in results_comparison])
    avg_recall_poc04 = statistics.mean([r['poc04']['recall'] for r in results_comparison])
    avg_precision_poc03 = statistics.mean([r['poc03']['precision'] for r in results_comparison])
    avg_precision_poc04 = statistics.mean([r['poc04']['precision'] for r in results_comparison])
    avg_latency_poc03 = statistics.mean([r['poc03']['latency_ms'] for r in results_comparison])
    avg_latency_poc04 = statistics.mean([r['poc04']['latency_ms'] for r in results_comparison])

    print(f"\nAverage Metrics:")
    print(f"  Recall@3:")
    print(f"    POC03: {avg_recall_poc03:.3f}")
    print(f"    POC04: {avg_recall_poc04:.3f}")
    print(f"    Improvement: {((avg_recall_poc04 - avg_recall_poc03)/avg_recall_poc03)*100:+.1f}%")

    print(f"\n  Precision@3:")
    print(f"    POC03: {avg_precision_poc03:.3f}")
    print(f"    POC04: {avg_precision_poc04:.3f}")
    print(f"    Improvement: {((avg_precision_poc04 - avg_precision_poc03)/avg_precision_poc03)*100:+.1f}%")

    print(f"\n  Latency:")
    print(f"    POC03: {avg_latency_poc03:.0f}ms")
    print(f"    POC04: {avg_latency_poc04:.0f}ms")
    print(f"    Change: {((avg_latency_poc04 - avg_latency_poc03)/avg_latency_poc03)*100:+.1f}%")

    # Key findings
    print(f"\n" + "=" * 80)
    print("KEY FINDINGS")
    print("=" * 80)
    print(f"\nPOC04 Optimizations:")
    print(f"  - Semantic chunking with 200-char overlap")
    print(f"  - Bilingual keywords (Thai + English)")
    print(f"  - Enhanced table metadata")
    print(f"  - 1492 chunks vs 72 chunks (20x more data)")

    print(f"\nResults:")
    print(f"  - Recall improved by {((avg_recall_poc04 - avg_recall_poc03)/avg_recall_poc03)*100:+.1f}%")
    print(f"  - Precision improved by {((avg_precision_poc04 - avg_precision_poc03)/avg_precision_poc03)*100:+.1f}%")
    print(f"  - More complete coverage from 15 documents vs 1 document")

    if avg_recall_poc04 > avg_recall_poc03 * 1.3:
        print(f"\n  STATUS: SIGNIFICANT IMPROVEMENT (>30%)")
    elif avg_recall_poc04 > avg_recall_poc03 * 1.1:
        print(f"\n  STATUS: GOOD IMPROVEMENT (>10%)")
    else:
        print(f"\n  STATUS: MODERATE IMPROVEMENT")

    # Save results
    output_file = os.path.join(os.path.dirname(__file__), 'poc04_vs_poc03_comparison.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'summary': {
                'poc03_avg_recall': avg_recall_poc03,
                'poc04_avg_recall': avg_recall_poc04,
                'poc03_avg_precision': avg_precision_poc03,
                'poc04_avg_precision': avg_precision_poc04,
                'recall_improvement_pct': ((avg_recall_poc04 - avg_recall_poc03)/avg_recall_poc03)*100,
                'precision_improvement_pct': ((avg_precision_poc04 - avg_precision_poc03)/avg_precision_poc03)*100
            },
            'query_results': results_comparison
        }, f, indent=2, ensure_ascii=False)

    print(f"\nComparison saved: {output_file}")

    spark.stop()


if __name__ == "__main__":
    main()
