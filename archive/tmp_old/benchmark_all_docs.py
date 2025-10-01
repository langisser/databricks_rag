#!/usr/bin/env python3
"""
Vector Search Benchmark for all_documents_index
Tests with correct column schema
"""

import sys
import os
import json
import time
from typing import List, Dict, Tuple, Any
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
    relevant_count = 0

    for result in top_k:
        content = result.get('content', '').lower()
        matches = sum(1 for keyword in ground_truth['relevant_keywords']
                     if keyword.lower() in content)
        if matches >= 1:
            relevant_count += 1

    max_relevant = len(ground_truth['relevant_keywords'])
    return relevant_count / max_relevant if max_relevant > 0 else 0.0


def calculate_precision_at_k(retrieved_results: List[Dict], ground_truth: Dict, k: int) -> float:
    if not retrieved_results:
        return 0.0

    top_k = retrieved_results[:k]
    relevant_count = 0

    for result in top_k:
        content = result.get('content', '').lower()
        matches = sum(1 for keyword in ground_truth['relevant_keywords']
                     if keyword.lower() in content)
        if matches >= 1:
            relevant_count += 1

    return relevant_count / len(top_k) if len(top_k) > 0 else 0.0


def execute_vector_search(
    vs_client: VectorSearchClient,
    index_name: str,
    query: str,
    num_results: int = 10
) -> Tuple[List[Dict], float, Dict]:
    start_time = time.time()

    try:
        response = vs_client.get_index(
            endpoint_name="rag-knowledge-base-endpoint",
            index_name=index_name
        ).similarity_search(
            query_text=query,
            columns=["chunk_id", "chunk_text", "source_file"],
            num_results=num_results
        )

        end_time = time.time()
        latency_ms = (end_time - start_time) * 1000

        results = []
        if hasattr(response, 'data_array') and response.data_array:
            for row in response.data_array:
                results.append({
                    'id': row[0] if len(row) > 0 else None,
                    'content': row[1] if len(row) > 1 else '',
                    'source_file': row[2] if len(row) > 2 else None,
                    'score': row[3] if len(row) > 3 else 0.0
                })

        return results, latency_ms, {'query': query, 'num_results': len(results)}

    except Exception as e:
        print(f"    ERROR: {e}")
        return [], 0.0, {'error': str(e)}


def main():
    print("=" * 80)
    print("VECTOR SEARCH BENCHMARK - all_documents_index")
    print("=" * 80)

    config_path = os.path.join(
        os.path.dirname(__file__), '..',
        'databricks_helper', 'databricks_config', 'config.json'
    )

    with open(config_path, 'r') as f:
        config = json.load(f)

    print("\n[STATUS] Connecting...")
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

    index_name = "sandbox.rdt_knowledge.all_documents_index"
    print(f"[STATUS] Testing index: {index_name}")

    print("\n" + "=" * 80)
    print("EXECUTING QUERIES")
    print("=" * 80)

    all_latencies = []
    results_summary = []

    for idx, gt_query in enumerate(GROUND_TRUTH_QUERIES, 1):
        query_text = gt_query['query']
        print(f"\n[Query {idx}/5] {query_text}")
        print(f"  Language: {gt_query['language']}")

        retrieved, latency_ms, metadata = execute_vector_search(
            vs_client, index_name, query_text, num_results=10
        )

        all_latencies.append(latency_ms)

        recall_3 = calculate_recall_at_k(retrieved, gt_query, 3)
        precision_3 = calculate_precision_at_k(retrieved, gt_query, 3)
        recall_5 = calculate_recall_at_k(retrieved, gt_query, 5)
        precision_5 = calculate_precision_at_k(retrieved, gt_query, 5)

        print(f"  Latency: {latency_ms:.2f}ms | Results: {len(retrieved)}")
        print(f"  @3: Recall={recall_3:.3f} | Precision={precision_3:.3f}")
        print(f"  @5: Recall={recall_5:.3f} | Precision={precision_5:.3f}")

        # Show top result with score
        if retrieved:
            top = retrieved[0]
            preview = top['content'][:100] if top['content'] else ''
            print(f"  Top result (score={top['score']:.6f}): {preview}...")

        results_summary.append({
            'query': query_text,
            'num_results': len(retrieved),
            'latency_ms': latency_ms,
            'recall_3': recall_3,
            'precision_3': precision_3,
            'top_score': retrieved[0]['score'] if retrieved else 0.0
        })

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    avg_latency = statistics.mean(all_latencies) if all_latencies else 0
    avg_recall = statistics.mean([r['recall_3'] for r in results_summary])
    avg_precision = statistics.mean([r['precision_3'] for r in results_summary])

    print(f"\nAvg Latency: {avg_latency:.2f}ms")
    print(f"Avg Recall@3: {avg_recall:.3f}")
    print(f"Avg Precision@3: {avg_precision:.3f}")

    print("\nPer-Query Scores:")
    for r in results_summary:
        print(f"  {r['num_results']} results | score={r['top_score']:.6f} | {r['query'][:50]}...")

    # Save results
    output = {
        'index': index_name,
        'summary': {
            'avg_latency_ms': avg_latency,
            'avg_recall_3': avg_recall,
            'avg_precision_3': avg_precision
        },
        'queries': results_summary
    }

    output_file = os.path.join(os.path.dirname(__file__), 'benchmark_all_docs_results.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\nResults saved: {output_file}")

    spark.stop()


if __name__ == "__main__":
    main()
