#!/usr/bin/env python3
"""
Vector Search Performance Benchmark and Accuracy Evaluation
Evaluates vector search quality using multiple metrics:
- Recall@K: % of relevant results retrieved
- Precision@K: % of retrieved results that are relevant
- NDCG@K: Ranking quality with relevance scores
- MRR: Mean Reciprocal Rank of first relevant result
- Latency: Response time metrics (P50, P95, P99)
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


# ============================================================================
# Ground Truth Dataset
# ============================================================================
GROUND_TRUTH_QUERIES = [
    {
        "query": "tbl_bot_rspn_sbmt_log มี column อะไรบ้าง",
        "language": "Thai",
        "category": "table_structure",
        "relevant_keywords": ["tbl_bot_rspn_sbmt_log", "column", "LOG_ID", "SUBMISSION_ID", "RESPONSE_STATUS"],
        "expected_content_type": ["text", "table"],
        "min_relevance_score": 0.005,
        "description": "Query about table columns in Thai"
    },
    {
        "query": "tbl_bot_rspn_sbmt_log columns structure",
        "language": "English",
        "category": "table_structure",
        "relevant_keywords": ["tbl_bot_rspn_sbmt_log", "columns", "structure", "LOG_ID", "SUBMISSION_ID"],
        "expected_content_type": ["text", "table"],
        "min_relevance_score": 0.005,
        "description": "Query about table structure in English"
    },
    {
        "query": "LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร",
        "language": "Mixed",
        "category": "column_definition",
        "relevant_keywords": ["LOG_ID", "SUBMISSION_ID", "RESPONSE_STATUS", "column"],
        "expected_content_type": ["text", "table"],
        "min_relevance_score": 0.004,
        "description": "Mixed Thai-English column query"
    },
    {
        "query": "ตาราง BOT response log structure",
        "language": "Mixed",
        "category": "table_structure",
        "relevant_keywords": ["BOT", "response", "log", "structure", "table"],
        "expected_content_type": ["text", "table"],
        "min_relevance_score": 0.004,
        "description": "Mixed Thai-English structure query"
    },
    {
        "query": "DataX OPM RDT Submission to BOT",
        "language": "English",
        "category": "document",
        "relevant_keywords": ["DataX", "OPM", "RDT", "Submission", "BOT"],
        "expected_content_type": ["text"],
        "min_relevance_score": 0.003,
        "description": "Document name query"
    }
]


# ============================================================================
# Evaluation Metrics Functions
# ============================================================================

def calculate_recall_at_k(retrieved_results: List[Dict], ground_truth: Dict, k: int) -> float:
    """
    Calculate Recall@K: % of relevant documents found in top K results
    """
    if not retrieved_results:
        return 0.0

    top_k = retrieved_results[:k]
    relevant_count = 0

    for result in top_k:
        content = result.get('content', '').lower()
        # Check if result contains relevant keywords
        matches = sum(1 for keyword in ground_truth['relevant_keywords']
                     if keyword.lower() in content)
        if matches >= 1:  # At least 1 relevant keyword found
            relevant_count += 1

    # Maximum possible relevant documents (assuming we know the total)
    max_relevant = len(ground_truth['relevant_keywords'])
    recall = relevant_count / max_relevant if max_relevant > 0 else 0.0

    return recall


def calculate_precision_at_k(retrieved_results: List[Dict], ground_truth: Dict, k: int) -> float:
    """
    Calculate Precision@K: % of retrieved documents that are relevant
    """
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

    precision = relevant_count / len(top_k) if len(top_k) > 0 else 0.0
    return precision


def calculate_ndcg_at_k(retrieved_results: List[Dict], ground_truth: Dict, k: int) -> float:
    """
    Calculate NDCG@K: Normalized Discounted Cumulative Gain
    Measures ranking quality with relevance scores
    """
    if not retrieved_results:
        return 0.0

    top_k = retrieved_results[:k]

    # Calculate DCG (Discounted Cumulative Gain)
    dcg = 0.0
    for i, result in enumerate(top_k):
        content = result.get('content', '').lower()
        # Relevance score based on keyword matches
        matches = sum(1 for keyword in ground_truth['relevant_keywords']
                     if keyword.lower() in content)
        relevance = matches / len(ground_truth['relevant_keywords'])

        # DCG formula: sum(rel_i / log2(i+2))
        dcg += relevance / (1 if i == 0 else (i + 1).bit_length())

    # Calculate IDCG (Ideal DCG) - perfect ranking
    ideal_relevances = [1.0] * min(k, len(ground_truth['relevant_keywords']))
    idcg = sum(rel / (1 if i == 0 else (i + 1).bit_length())
               for i, rel in enumerate(ideal_relevances))

    # NDCG = DCG / IDCG
    ndcg = dcg / idcg if idcg > 0 else 0.0
    return ndcg


def calculate_mrr(retrieved_results: List[Dict], ground_truth: Dict) -> float:
    """
    Calculate MRR: Mean Reciprocal Rank
    Position of first relevant result
    """
    if not retrieved_results:
        return 0.0

    for i, result in enumerate(retrieved_results):
        content = result.get('content', '').lower()
        matches = sum(1 for keyword in ground_truth['relevant_keywords']
                     if keyword.lower() in content)
        if matches >= 1:
            return 1.0 / (i + 1)

    return 0.0


# ============================================================================
# Vector Search Execution
# ============================================================================

def execute_vector_search(
    vs_client: VectorSearchClient,
    index_name: str,
    query: str,
    num_results: int = 10
) -> Tuple[List[Dict], float, Dict]:
    """
    Execute vector search and measure latency
    Returns: (results, latency_ms, metadata)
    """
    start_time = time.time()

    try:
        response = vs_client.get_index(
            endpoint_name="rag-knowledge-base-endpoint",
            index_name=index_name
        ).similarity_search(
            query_text=query,
            columns=["chunk_id", "combined_text", "chunk_type", "file_name"],
            num_results=num_results
        )

        end_time = time.time()
        latency_ms = (end_time - start_time) * 1000

        # Parse results
        results = []
        if hasattr(response, 'data_array') and response.data_array:
            for row in response.data_array:
                results.append({
                    'id': row[0] if len(row) > 0 else None,
                    'content': row[1] if len(row) > 1 else '',
                    'content_type': row[2] if len(row) > 2 else None,
                    'source_file': row[3] if len(row) > 3 else None,
                    'score': row[4] if len(row) > 4 else 0.0
                })

        metadata = {
            'query': query,
            'num_results': len(results),
            'latency_ms': latency_ms
        }

        return results, latency_ms, metadata

    except Exception as e:
        print(f"    ERROR: {e}")
        return [], 0.0, {'error': str(e)}


# ============================================================================
# Benchmark Execution
# ============================================================================

def run_benchmark(
    vs_client: VectorSearchClient,
    index_name: str,
    ground_truth_queries: List[Dict],
    k_values: List[int] = [3, 5, 10]
) -> Dict[str, Any]:
    """
    Run complete benchmark evaluation
    """
    print("\n" + "=" * 80)
    print("VECTOR SEARCH BENCHMARK EVALUATION")
    print("=" * 80)
    print(f"Index: {index_name}")
    print(f"Total queries: {len(ground_truth_queries)}")
    print(f"K values: {k_values}")

    results = {
        'index_name': index_name,
        'total_queries': len(ground_truth_queries),
        'k_values': k_values,
        'query_results': [],
        'aggregate_metrics': {},
        'latency_stats': {}
    }

    all_latencies = []

    # Metrics storage
    metrics_by_k = {k: defaultdict(list) for k in k_values}

    print("\n" + "-" * 80)
    print("EXECUTING QUERIES")
    print("-" * 80)

    for idx, gt_query in enumerate(ground_truth_queries, 1):
        query_text = gt_query['query']
        print(f"\n[Query {idx}/{len(ground_truth_queries)}] {query_text}")
        print(f"  Language: {gt_query['language']} | Category: {gt_query['category']}")

        # Execute search
        retrieved_results, latency_ms, metadata = execute_vector_search(
            vs_client, index_name, query_text, num_results=max(k_values)
        )

        all_latencies.append(latency_ms)
        print(f"  Latency: {latency_ms:.2f}ms | Results: {len(retrieved_results)}")

        # Calculate metrics for each K
        query_metrics = {
            'query': query_text,
            'language': gt_query['language'],
            'category': gt_query['category'],
            'latency_ms': latency_ms,
            'num_results': len(retrieved_results),
            'metrics_by_k': {}
        }

        for k in k_values:
            recall = calculate_recall_at_k(retrieved_results, gt_query, k)
            precision = calculate_precision_at_k(retrieved_results, gt_query, k)
            ndcg = calculate_ndcg_at_k(retrieved_results, gt_query, k)
            mrr = calculate_mrr(retrieved_results, gt_query)

            query_metrics['metrics_by_k'][k] = {
                'recall': recall,
                'precision': precision,
                'ndcg': ndcg,
                'mrr': mrr
            }

            # Store for aggregation
            metrics_by_k[k]['recall'].append(recall)
            metrics_by_k[k]['precision'].append(precision)
            metrics_by_k[k]['ndcg'].append(ndcg)
            metrics_by_k[k]['mrr'].append(mrr)

            print(f"  @{k}: Recall={recall:.3f} | Precision={precision:.3f} | NDCG={ndcg:.3f} | MRR={mrr:.3f}")

        results['query_results'].append(query_metrics)

    # Calculate aggregate metrics
    print("\n" + "-" * 80)
    print("AGGREGATE METRICS")
    print("-" * 80)

    for k in k_values:
        avg_recall = statistics.mean(metrics_by_k[k]['recall'])
        avg_precision = statistics.mean(metrics_by_k[k]['precision'])
        avg_ndcg = statistics.mean(metrics_by_k[k]['ndcg'])
        avg_mrr = statistics.mean(metrics_by_k[k]['mrr'])

        results['aggregate_metrics'][f'@{k}'] = {
            'avg_recall': avg_recall,
            'avg_precision': avg_precision,
            'avg_ndcg': avg_ndcg,
            'avg_mrr': avg_mrr
        }

        print(f"\n@{k} Metrics:")
        print(f"  Avg Recall:    {avg_recall:.3f}")
        print(f"  Avg Precision: {avg_precision:.3f}")
        print(f"  Avg NDCG:      {avg_ndcg:.3f}")
        print(f"  Avg MRR:       {avg_mrr:.3f}")

    # Latency statistics
    print("\n" + "-" * 80)
    print("LATENCY STATISTICS")
    print("-" * 80)

    all_latencies.sort()
    p50 = all_latencies[len(all_latencies) // 2]
    p95 = all_latencies[int(len(all_latencies) * 0.95)]
    p99 = all_latencies[int(len(all_latencies) * 0.99)]
    avg_latency = statistics.mean(all_latencies)

    results['latency_stats'] = {
        'avg_ms': avg_latency,
        'p50_ms': p50,
        'p95_ms': p95,
        'p99_ms': p99,
        'min_ms': min(all_latencies),
        'max_ms': max(all_latencies)
    }

    print(f"  Average:  {avg_latency:.2f}ms")
    print(f"  P50:      {p50:.2f}ms")
    print(f"  P95:      {p95:.2f}ms")
    print(f"  P99:      {p99:.2f}ms")
    print(f"  Min:      {min(all_latencies):.2f}ms")
    print(f"  Max:      {max(all_latencies):.2f}ms")

    return results


# ============================================================================
# Main Execution
# ============================================================================

def main():
    print("=" * 80)
    print("VECTOR SEARCH PERFORMANCE BENCHMARK")
    print("=" * 80)

    # Load configuration
    config_path = os.path.join(
        os.path.dirname(__file__), '..',
        'databricks_helper', 'databricks_config', 'config.json'
    )

    with open(config_path, 'r') as f:
        config = json.load(f)

    # Connect to Databricks
    print("\n[STATUS] Connecting to Databricks...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()
    print("         Connected")

    # Initialize Vector Search Client
    print("[STATUS] Initializing Vector Search Client...")
    vs_client = VectorSearchClient(
        workspace_url=config['databricks']['host'],
        personal_access_token=config['databricks']['token'],
        disable_notice=True
    )

    # Get index name
    index_name = config['rag_config'].get('bilingual_index') or \
                 config['rag_config'].get('production_index')

    print(f"         Index: {index_name}")

    # Run benchmark
    benchmark_results = run_benchmark(
        vs_client=vs_client,
        index_name=index_name,
        ground_truth_queries=GROUND_TRUTH_QUERIES,
        k_values=[3, 5, 10]
    )

    # Save results
    output_file = os.path.join(os.path.dirname(__file__), 'benchmark_results.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(benchmark_results, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 80)
    print("BENCHMARK COMPLETE")
    print("=" * 80)
    print(f"Results saved to: {output_file}")

    # Summary
    print("\n[SUMMARY]")
    print(f"  Total queries: {benchmark_results['total_queries']}")
    print(f"  Avg latency: {benchmark_results['latency_stats']['avg_ms']:.2f}ms")
    print(f"  P95 latency: {benchmark_results['latency_stats']['p95_ms']:.2f}ms")

    for k in benchmark_results['k_values']:
        metrics = benchmark_results['aggregate_metrics'][f'@{k}']
        print(f"\n  @{k} Performance:")
        print(f"    Recall:    {metrics['avg_recall']:.3f}")
        print(f"    Precision: {metrics['avg_precision']:.3f}")
        print(f"    NDCG:      {metrics['avg_ndcg']:.3f}")

    spark.stop()


if __name__ == "__main__":
    main()
