#!/usr/bin/env python3
"""
Vector Search Benchmark for POC03 Index
Evaluates poc03_multimodal_rag_index performance
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


# Ground Truth Queries
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


def execute_vector_search(
    vs_client: VectorSearchClient,
    endpoint_name: str,
    index_name: str,
    query: str,
    num_results: int = 10
) -> Tuple[List[Dict], float]:

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
        # Handle both dict and object response types
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
    print("POC03 VECTOR SEARCH BENCHMARK")
    print("=" * 80)

    # Load config
    config_path = os.path.join(
        os.path.dirname(__file__), '..',
        'databricks_helper', 'databricks_config', 'config.json'
    )
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

    # Get index configuration
    namespace = config['rag_config']['full_namespace']
    index_name = config['rag_config'].get('poc03_index', f"{namespace}.poc03_multimodal_rag_index")
    endpoint_name = config['rag_config']['vector_search_endpoint']

    print(f"[2] Configuration:")
    print(f"    Index: {index_name}")
    print(f"    Endpoint: {endpoint_name}")

    # Run benchmark
    print("\n" + "=" * 80)
    print("EXECUTING QUERIES")
    print("=" * 80)

    all_latencies = []
    metrics_by_k = {3: defaultdict(list), 5: defaultdict(list), 10: defaultdict(list)}
    query_results = []

    for idx, gt_query in enumerate(GROUND_TRUTH_QUERIES, 1):
        query_text = gt_query['query']
        print(f"\n[Query {idx}/5] {query_text}")
        print(f"  Language: {gt_query['language']} | Category: {gt_query['category']}")

        retrieved, latency_ms = execute_vector_search(
            vs_client, endpoint_name, index_name, query_text, num_results=10
        )

        all_latencies.append(latency_ms)
        print(f"  Latency: {latency_ms:.2f}ms | Results: {len(retrieved)}")

        # Calculate metrics for different K values
        for k in [3, 5, 10]:
            recall = calculate_recall_at_k(retrieved, gt_query, k)
            precision = calculate_precision_at_k(retrieved, gt_query, k)

            metrics_by_k[k]['recall'].append(recall)
            metrics_by_k[k]['precision'].append(precision)

            print(f"  @{k}: Recall={recall:.3f} | Precision={precision:.3f}")

        # Show top result
        if retrieved:
            top = retrieved[0]
            preview = top['content'][:100] if top['content'] else ''
            print(f"  Top result:")
            print(f"    Score: {top['score']:.6f}")
            print(f"    Content: {preview}...")

        query_results.append({
            'query': query_text,
            'language': gt_query['language'],
            'num_results': len(retrieved),
            'latency_ms': latency_ms,
            'top_score': retrieved[0]['score'] if retrieved else 0.0,
            'recall_3': calculate_recall_at_k(retrieved, gt_query, 3),
            'precision_3': calculate_precision_at_k(retrieved, gt_query, 3)
        })

    # Summary
    print("\n" + "=" * 80)
    print("BENCHMARK RESULTS")
    print("=" * 80)

    # Latency stats
    if all_latencies:
        all_latencies.sort()
        print(f"\nLatency Statistics:")
        print(f"  Average: {statistics.mean(all_latencies):.2f}ms")
        print(f"  P50: {all_latencies[len(all_latencies)//2]:.2f}ms")
        print(f"  P95: {all_latencies[int(len(all_latencies)*0.95)]:.2f}ms")
        print(f"  Min: {min(all_latencies):.2f}ms")
        print(f"  Max: {max(all_latencies):.2f}ms")

    # Accuracy metrics
    print(f"\nAccuracy Metrics:")
    for k in [3, 5, 10]:
        avg_recall = statistics.mean(metrics_by_k[k]['recall'])
        avg_precision = statistics.mean(metrics_by_k[k]['precision'])
        print(f"\n  @{k}:")
        print(f"    Avg Recall:    {avg_recall:.3f}")
        print(f"    Avg Precision: {avg_precision:.3f}")

    # Per-query summary
    print(f"\nPer-Query Results:")
    for r in query_results:
        print(f"  {r['num_results']} results | score={r['top_score']:.4f} | "
              f"R@3={r['recall_3']:.2f} | P@3={r['precision_3']:.2f} | "
              f"{r['query'][:40]}...")

    # Save results
    output = {
        'index_name': index_name,
        'timestamp': time.strftime("%Y-%m-%dT%H:%M:%S"),
        'summary': {
            'total_queries': len(GROUND_TRUTH_QUERIES),
            'avg_latency_ms': statistics.mean(all_latencies) if all_latencies else 0,
            'avg_recall_3': statistics.mean(metrics_by_k[3]['recall']),
            'avg_precision_3': statistics.mean(metrics_by_k[3]['precision']),
        },
        'latency_stats': {
            'avg_ms': statistics.mean(all_latencies) if all_latencies else 0,
            'p50_ms': all_latencies[len(all_latencies)//2] if all_latencies else 0,
            'p95_ms': all_latencies[int(len(all_latencies)*0.95)] if all_latencies else 0,
            'min_ms': min(all_latencies) if all_latencies else 0,
            'max_ms': max(all_latencies) if all_latencies else 0,
        },
        'queries': query_results
    }

    output_file = os.path.join(os.path.dirname(__file__), 'poc03_benchmark_results.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n\nResults saved: {output_file}")

    spark.stop()


if __name__ == "__main__":
    main()
