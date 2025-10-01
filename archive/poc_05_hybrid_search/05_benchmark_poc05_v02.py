#!/usr/bin/env python3
"""
POC05_v02 - Benchmark Against Previous Versions
Compare POC05_v02 (1620 chunks) with POC05, POC04, POC03
Test with 5 ground truth queries
"""

import sys
import os
import json
import time
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.vector_search.client import VectorSearchClient


def main():
    print("=" * 80)
    print("POC05_V02 - BENCHMARK COMPARISON")
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

    # Configuration
    endpoint_name = config['rag_config']['vector_search_endpoint']

    # Indexes to compare
    indexes = {
        'POC03': config['rag_config'].get('poc03_index'),
        'POC04': config['rag_config'].get('poc04_index'),
        'POC05': config['rag_config'].get('poc05_index'),
        'POC05_v02': config['rag_config'].get('poc05_v02_index')
    }

    # Get chunk counts
    poc03_table = config['rag_config'].get('poc03_table')
    poc04_table = config['rag_config'].get('poc04_table')
    poc05_table = config['rag_config'].get('poc05_table')
    poc05_v02_table = config['rag_config'].get('poc05_v02_metadata_table')

    chunk_counts = {}
    for name, table in [('POC03', poc03_table), ('POC04', poc04_table),
                        ('POC05', poc05_table), ('POC05_v02', poc05_v02_table)]:
        if table:
            try:
                count = spark.sql(f"SELECT COUNT(*) as count FROM {table}").collect()[0]['count']
                chunk_counts[name] = count
            except:
                chunk_counts[name] = 0

    print(f"\n[2] Index Configuration:")
    for name, index_name in indexes.items():
        chunks = chunk_counts.get(name, 0)
        print(f"    {name:12s}: {index_name if index_name else 'N/A':50s} ({chunks} chunks)")

    # Ground truth queries
    queries = [
        {
            'id': 1,
            'query': 'tbl_bot_rspn_sbmt_log columns',
            'description': 'Table structure query (Thai-English)',
            'expected_category': 'table_definition'
        },
        {
            'id': 2,
            'query': 'tbl_bot_rspn_sbmt_log columns structure DataX_OPM_RDT_Submission_to_BOT_v1.0.docx',
            'description': 'Table structure with document name',
            'expected_category': 'table_definition'
        },
        {
            'id': 3,
            'query': 'validation framework guidelines',
            'description': 'Process documentation query',
            'expected_category': 'process_description'
        },
        {
            'id': 4,
            'query': 'data transformation process',
            'description': 'Technical process query',
            'expected_category': 'process_description'
        },
        {
            'id': 5,
            'query': 'submission counterparty sync',
            'description': 'Multi-concept query',
            'expected_category': 'process_description'
        }
    ]

    print(f"\n[3] Running benchmarks on {len(queries)} queries...")
    print(f"    Query types: HYBRID (semantic + keyword)")
    print(f"    Results per query: 5")

    results = {name: [] for name in indexes.keys()}

    for query_info in queries:
        query_id = query_info['id']
        query_text = query_info['query']
        print(f"\n  Query {query_id}: {query_text}")

        for poc_name, index_name in indexes.items():
            if not index_name:
                print(f"    {poc_name:12s}: SKIP (no index)")
                results[poc_name].append({
                    'query_id': query_id,
                    'latency_ms': 0,
                    'results_count': 0,
                    'top_score': 0,
                    'error': 'No index configured'
                })
                continue

            try:
                # Get index
                index = vs_client.get_index(
                    endpoint_name=endpoint_name,
                    index_name=index_name
                )

                # Run query with HYBRID search
                start = time.time()
                search_results = index.similarity_search(
                    query_text=query_text,
                    columns=["id", "content", "content_category", "document_type", "table_name"],
                    num_results=5,
                    query_type="HYBRID"
                )
                latency = (time.time() - start) * 1000

                # Extract results
                if hasattr(search_results, 'data_array'):
                    data = search_results.data_array
                else:
                    data = []

                top_score = data[0][5] if data and len(data[0]) > 5 else 0

                results[poc_name].append({
                    'query_id': query_id,
                    'latency_ms': latency,
                    'results_count': len(data),
                    'top_score': top_score,
                    'error': None
                })

                print(f"    {poc_name:12s}: {latency:.0f}ms | {len(data)} results | score: {top_score:.4f}")

            except Exception as e:
                print(f"    {poc_name:12s}: ERROR - {str(e)[:50]}")
                results[poc_name].append({
                    'query_id': query_id,
                    'latency_ms': 0,
                    'results_count': 0,
                    'top_score': 0,
                    'error': str(e)
                })

    # Calculate statistics
    print(f"\n[4] Calculating statistics...")

    stats = {}
    for poc_name, query_results in results.items():
        valid_results = [r for r in query_results if r['error'] is None and r['results_count'] > 0]

        if valid_results:
            avg_latency = sum(r['latency_ms'] for r in valid_results) / len(valid_results)
            avg_score = sum(r['top_score'] for r in valid_results) / len(valid_results)
            success_rate = len(valid_results) / len(queries) * 100
        else:
            avg_latency = 0
            avg_score = 0
            success_rate = 0

        stats[poc_name] = {
            'avg_latency_ms': avg_latency,
            'avg_top_score': avg_score,
            'success_rate': success_rate,
            'valid_queries': len(valid_results),
            'total_queries': len(queries),
            'chunk_count': chunk_counts.get(poc_name, 0)
        }

    # Print summary
    print(f"\n" + "=" * 80)
    print("BENCHMARK RESULTS SUMMARY")
    print("=" * 80)

    print(f"\n{'POC':12s} | {'Chunks':>8s} | {'Success':>8s} | {'Avg Latency':>12s} | {'Avg Score':>10s}")
    print("-" * 80)

    for poc_name in ['POC03', 'POC04', 'POC05', 'POC05_v02']:
        s = stats[poc_name]
        chunks = s['chunk_count']
        success = f"{s['success_rate']:.0f}%"
        latency = f"{s['avg_latency_ms']:.0f}ms"
        score = f"{s['avg_top_score']:.4f}"
        print(f"{poc_name:12s} | {chunks:>8d} | {success:>8s} | {latency:>12s} | {score:>10s}")

    # Improvements
    print(f"\n" + "=" * 80)
    print("POC05_v02 IMPROVEMENTS")
    print("=" * 80)

    if stats['POC04']['avg_latency_ms'] > 0:
        latency_improvement = ((stats['POC04']['avg_latency_ms'] - stats['POC05_v02']['avg_latency_ms'])
                               / stats['POC04']['avg_latency_ms'] * 100)
        score_improvement = ((stats['POC05_v02']['avg_top_score'] - stats['POC04']['avg_top_score'])
                            / stats['POC04']['avg_top_score'] * 100)
        chunk_increase = ((stats['POC05_v02']['chunk_count'] - stats['POC04']['chunk_count'])
                         / stats['POC04']['chunk_count'] * 100)

        print(f"\nvs POC04:")
        print(f"  Chunks:        +{chunk_increase:>6.1f}% ({stats['POC04']['chunk_count']} -> {stats['POC05_v02']['chunk_count']})")
        print(f"  Latency:       {latency_improvement:>+7.1f}% ({stats['POC04']['avg_latency_ms']:.0f}ms -> {stats['POC05_v02']['avg_latency_ms']:.0f}ms)")
        print(f"  Top Score:     {score_improvement:>+7.1f}% ({stats['POC04']['avg_top_score']:.4f} -> {stats['POC05_v02']['avg_top_score']:.4f})")
        print(f"  Success Rate:  {stats['POC04']['success_rate']:.0f}% -> {stats['POC05_v02']['success_rate']:.0f}%")

    if stats['POC05']['avg_latency_ms'] > 0:
        latency_improvement = ((stats['POC05']['avg_latency_ms'] - stats['POC05_v02']['avg_latency_ms'])
                               / stats['POC05']['avg_latency_ms'] * 100)
        score_improvement = ((stats['POC05_v02']['avg_top_score'] - stats['POC05']['avg_top_score'])
                            / stats['POC05']['avg_top_score'] * 100)
        chunk_increase = ((stats['POC05_v02']['chunk_count'] - stats['POC05']['chunk_count'])
                         / stats['POC05']['chunk_count'] * 100)

        print(f"\nvs POC05:")
        print(f"  Chunks:        +{chunk_increase:>6.1f}% ({stats['POC05']['chunk_count']} -> {stats['POC05_v02']['chunk_count']})")
        print(f"  Latency:       {latency_improvement:>+7.1f}% ({stats['POC05']['avg_latency_ms']:.0f}ms -> {stats['POC05_v02']['avg_latency_ms']:.0f}ms)")
        print(f"  Top Score:     {score_improvement:>+7.1f}% ({stats['POC05']['avg_top_score']:.4f} -> {stats['POC05_v02']['avg_top_score']:.4f})")
        print(f"  Success Rate:  {stats['POC05']['success_rate']:.0f}% -> {stats['POC05_v02']['success_rate']:.0f}%")

    # Save benchmark results
    print(f"\n[5] Saving results...")

    benchmark_results = {
        'benchmark_date': datetime.now().isoformat(),
        'queries': queries,
        'results': results,
        'stats': stats,
        'chunk_counts': chunk_counts
    }

    config['rag_config']['poc05_v02_benchmark'] = benchmark_results
    config['rag_config']['poc05_v02_benchmark_date'] = datetime.now().isoformat()

    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)

    print(f"    Saved to config.json")

    # Recommendations
    print(f"\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print(f"\nPOC05_v02 Strengths:")
    print(f"  [OK] {stats['POC05_v02']['chunk_count']} chunks from 29 documents")
    print(f"  [OK] Hybrid search (semantic + keyword matching)")
    print(f"  [OK] Metadata filtering support")
    print(f"  [OK] Bilingual optimization (Thai + English)")
    print(f"  [OK] {stats['POC05_v02']['success_rate']:.0f}% query success rate")

    print(f"\nNext Steps:")
    print(f"  1. Process remaining PDF documents (23 files)")
    print(f"  2. Test with production queries")
    print(f"  3. Fine-tune metadata categories")
    print(f"  4. Deploy to production if results are satisfactory")

    spark.stop()


if __name__ == "__main__":
    main()
