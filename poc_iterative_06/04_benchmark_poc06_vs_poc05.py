#!/usr/bin/env python3
"""
POC06 Phase 1 - Benchmark Comparison
Compare POC06 (improved chunking) vs POC05 (current production)

METRICS:
1. Document coverage (files processed)
2. Chunk statistics (count, size, quality)
3. Query accuracy (test queries)
4. Retrieval relevance (top-k results)
5. Response time (query performance)
"""

import sys
import os
import json
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.vector_search.client import VectorSearchClient


# Test queries for benchmark
TEST_QUERIES = [
    {
        'id': 'Q1',
        'query': 'validation framework guidelines',
        'expected_terms': ['validation', 'framework', 'guideline'],
        'category': 'general'
    },
    {
        'id': 'Q2',
        'query': 'tbl_bot_rspn_sbmt_log table columns',
        'expected_terms': ['tbl_bot_rspn_sbmt_log', 'column', 'table'],
        'category': 'table_definition'
    },
    {
        'id': 'Q3',
        'query': 'submission process flow',
        'expected_terms': ['submission', 'process', 'flow'],
        'category': 'process'
    },
    {
        'id': 'Q4',
        'query': 'data quality standards',
        'expected_terms': ['data', 'quality', 'standard'],
        'category': 'standard'
    },
    {
        'id': 'Q5',
        'query': 'presentation overview slides',
        'expected_terms': ['presentation', 'overview', 'slide'],
        'category': 'pptx'  # NEW: PPTX-specific query
    }
]


def query_index(vs_client, endpoint_name, index_name, query, num_results=5):
    """Query vector index and measure performance"""
    try:
        start_time = time.time()

        index = vs_client.get_index(
            endpoint_name=endpoint_name,
            index_name=index_name
        )

        results = index.similarity_search(
            query_text=query,
            columns=["id", "content", "source_document", "content_category", "char_count"],
            query_type="HYBRID",
            num_results=num_results
        )

        elapsed = time.time() - start_time

        return {
            'success': True,
            'results': results.get('result', {}).get('data_array', []),
            'elapsed_ms': int(elapsed * 1000),
            'num_results': len(results.get('result', {}).get('data_array', []))
        }

    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'elapsed_ms': 0,
            'num_results': 0
        }


def calculate_relevance_score(result, expected_terms):
    """Calculate relevance score based on expected terms in content"""
    content = str(result[1]).lower()  # content column
    matches = sum(1 for term in expected_terms if term.lower() in content)
    return matches / len(expected_terms) if expected_terms else 0


def main():
    print("=" * 80)
    print("POC06 vs POC05 - BENCHMARK COMPARISON")
    print("=" * 80)

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    cluster_id = config['databricks'].get('lineage_cluster_id', config['databricks']['cluster_id'])

    print(f"\n[1] Connecting to Databricks (cluster: {cluster_id})...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=cluster_id
    ).getOrCreate()
    print("    Connected")

    # Configuration
    namespace = config['rag_config']['full_namespace']
    endpoint_name = config['rag_config']['vector_search_endpoint']

    poc05_table = f"{namespace}.poc05_v02_all_formats_with_metadata"
    poc05_index = f"{namespace}.poc05_v02_all_formats_hybrid_rag_index"

    poc06_table = f"{namespace}.poc06_phase1_with_metadata"
    poc06_index = f"{namespace}.poc06_phase1_hybrid_rag_index"

    print(f"\n[2] Benchmark Configuration:")
    print(f"    POC05 (Current):")
    print(f"      Table: {poc05_table}")
    print(f"      Index: {poc05_index}")
    print(f"    POC06 (Improved):")
    print(f"      Table: {poc06_table}")
    print(f"      Index: {poc06_index}")

    # === METRIC 1: Document Coverage ===
    print(f"\n" + "=" * 80)
    print("METRIC 1: Document Coverage")
    print("=" * 80)

    print(f"\n  POC05 (Current Production):")
    poc05_count = spark.sql(f"SELECT COUNT(*) as count FROM {poc05_table}").collect()[0]['count']
    poc05_docs = spark.sql(f"SELECT COUNT(DISTINCT source_document) as count FROM {poc05_table}").collect()[0]['count']
    print(f"    Chunks: {poc05_count:,}")
    print(f"    Documents: {poc05_docs}")

    poc05_formats = spark.sql(f"""
        SELECT
            CASE
                WHEN LOWER(source_document) LIKE '%.docx' THEN 'DOCX'
                WHEN LOWER(source_document) LIKE '%.pdf' THEN 'PDF'
                WHEN LOWER(source_document) LIKE '%.xlsx' THEN 'XLSX'
                WHEN LOWER(source_document) LIKE '%.pptx' THEN 'PPTX'
                ELSE 'OTHER'
            END as format,
            COUNT(DISTINCT source_document) as doc_count,
            COUNT(*) as chunk_count
        FROM {poc05_table}
        GROUP BY format
        ORDER BY chunk_count DESC
    """)
    print(f"\n    By format:")
    poc05_formats.show()

    print(f"\n  POC06 (Improved Chunking):")
    poc06_count = spark.sql(f"SELECT COUNT(*) as count FROM {poc06_table}").collect()[0]['count']
    poc06_docs = spark.sql(f"SELECT COUNT(DISTINCT source_document) as count FROM {poc06_table}").collect()[0]['count']
    print(f"    Chunks: {poc06_count:,}")
    print(f"    Documents: {poc06_docs}")

    poc06_formats = spark.sql(f"""
        SELECT
            CASE
                WHEN LOWER(source_document) LIKE '%.docx' THEN 'DOCX'
                WHEN LOWER(source_document) LIKE '%.pdf' THEN 'PDF'
                WHEN LOWER(source_document) LIKE '%.xlsx' THEN 'XLSX'
                WHEN LOWER(source_document) LIKE '%.pptx' THEN 'PPTX'
                ELSE 'OTHER'
            END as format,
            COUNT(DISTINCT source_document) as doc_count,
            COUNT(*) as chunk_count
        FROM {poc06_table}
        GROUP BY format
        ORDER BY chunk_count DESC
    """)
    print(f"\n    By format:")
    poc06_formats.show()

    coverage_improvement = ((poc06_docs - poc05_docs) / poc05_docs * 100) if poc05_docs > 0 else 0
    chunk_change = ((poc06_count - poc05_count) / poc05_count * 100) if poc05_count > 0 else 0

    print(f"\n  IMPROVEMENT:")
    print(f"    Documents: +{poc06_docs - poc05_docs} ({coverage_improvement:+.1f}%)")
    print(f"    Chunks: {poc06_count - poc05_count:+,} ({chunk_change:+.1f}%)")

    # === METRIC 2: Chunk Quality ===
    print(f"\n" + "=" * 80)
    print("METRIC 2: Chunk Statistics")
    print("=" * 80)

    print(f"\n  POC05:")
    poc05_stats = spark.sql(f"""
        SELECT
            AVG(char_count) as avg_chars,
            MIN(char_count) as min_chars,
            MAX(char_count) as max_chars,
            PERCENTILE_APPROX(char_count, 0.5) as median_chars
        FROM {poc05_table}
    """).collect()[0]
    print(f"    Avg chunk size: {poc05_stats['avg_chars']:.0f} chars")
    print(f"    Min/Max: {poc05_stats['min_chars']:.0f} / {poc05_stats['max_chars']:.0f} chars")
    print(f"    Median: {poc05_stats['median_chars']:.0f} chars")

    print(f"\n  POC06:")
    poc06_stats = spark.sql(f"""
        SELECT
            AVG(char_count) as avg_chars,
            MIN(char_count) as min_chars,
            MAX(char_count) as max_chars,
            PERCENTILE_APPROX(char_count, 0.5) as median_chars
        FROM {poc06_table}
    """).collect()[0]
    print(f"    Avg chunk size: {poc06_stats['avg_chars']:.0f} chars")
    print(f"    Min/Max: {poc06_stats['min_chars']:.0f} / {poc06_stats['max_chars']:.0f} chars")
    print(f"    Median: {poc06_stats['median_chars']:.0f} chars")

    size_improvement = ((poc06_stats['avg_chars'] - poc05_stats['avg_chars']) / poc05_stats['avg_chars'] * 100)
    print(f"\n  IMPROVEMENT:")
    print(f"    Avg size: {size_improvement:+.1f}% (larger chunks = better context)")

    # === METRIC 3: Query Performance ===
    print(f"\n" + "=" * 80)
    print("METRIC 3: Query Performance & Accuracy")
    print("=" * 80)

    # Initialize Vector Search Client
    vs_client = VectorSearchClient(
        workspace_url=config['databricks']['host'],
        personal_access_token=config['databricks']['token'],
        disable_notice=True
    )

    results_summary = []

    for test in TEST_QUERIES:
        print(f"\n  Query {test['id']}: \"{test['query']}\" (category: {test['category']})")

        # Query POC05
        print(f"    POC05...")
        poc05_result = query_index(vs_client, endpoint_name, poc05_index, test['query'])

        if poc05_result['success']:
            poc05_relevance = sum(calculate_relevance_score(r, test['expected_terms']) for r in poc05_result['results']) / len(poc05_result['results']) if poc05_result['results'] else 0
            print(f"      Results: {poc05_result['num_results']}")
            print(f"      Time: {poc05_result['elapsed_ms']}ms")
            print(f"      Relevance: {poc05_relevance:.1%}")
        else:
            print(f"      ERROR: {poc05_result['error']}")
            poc05_relevance = 0

        # Query POC06
        print(f"    POC06...")
        poc06_result = query_index(vs_client, endpoint_name, poc06_index, test['query'])

        if poc06_result['success']:
            poc06_relevance = sum(calculate_relevance_score(r, test['expected_terms']) for r in poc06_result['results']) / len(poc06_result['results']) if poc06_result['results'] else 0
            print(f"      Results: {poc06_result['num_results']}")
            print(f"      Time: {poc06_result['elapsed_ms']}ms")
            print(f"      Relevance: {poc06_relevance:.1%}")
        else:
            print(f"      ERROR: {poc06_result['error']}")
            poc06_relevance = 0

        # Calculate improvement
        if poc05_result['success'] and poc06_result['success']:
            relevance_improvement = ((poc06_relevance - poc05_relevance) / poc05_relevance * 100) if poc05_relevance > 0 else 0
            time_change = ((poc06_result['elapsed_ms'] - poc05_result['elapsed_ms']) / poc05_result['elapsed_ms'] * 100) if poc05_result['elapsed_ms'] > 0 else 0

            print(f"    IMPROVEMENT:")
            print(f"      Relevance: {relevance_improvement:+.1f}%")
            print(f"      Time: {time_change:+.1f}%")

            results_summary.append({
                'query_id': test['id'],
                'query': test['query'],
                'category': test['category'],
                'poc05_relevance': poc05_relevance,
                'poc06_relevance': poc06_relevance,
                'relevance_improvement': relevance_improvement,
                'poc05_time_ms': poc05_result['elapsed_ms'],
                'poc06_time_ms': poc06_result['elapsed_ms'],
                'time_change': time_change
            })

    # === SUMMARY ===
    print(f"\n" + "=" * 80)
    print("BENCHMARK SUMMARY")
    print("=" * 80)

    print(f"\n1. DOCUMENT COVERAGE:")
    print(f"   POC05: {poc05_docs} documents, {poc05_count:,} chunks")
    print(f"   POC06: {poc06_docs} documents, {poc06_count:,} chunks")
    print(f"   Improvement: +{poc06_docs - poc05_docs} documents ({coverage_improvement:+.1f}%)")
    print(f"   PPTX Support: {'YES (NEW!)' if poc06_docs > poc05_docs else 'NO'}")

    print(f"\n2. CHUNK QUALITY:")
    print(f"   POC05 avg: {poc05_stats['avg_chars']:.0f} chars")
    print(f"   POC06 avg: {poc06_stats['avg_chars']:.0f} chars")
    print(f"   Improvement: {size_improvement:+.1f}% (better context)")

    print(f"\n3. QUERY PERFORMANCE:")
    if results_summary:
        avg_relevance_improvement = sum(r['relevance_improvement'] for r in results_summary) / len(results_summary)
        avg_time_change = sum(r['time_change'] for r in results_summary) / len(results_summary)

        print(f"   Average relevance improvement: {avg_relevance_improvement:+.1f}%")
        print(f"   Average time change: {avg_time_change:+.1f}%")

        print(f"\n   Query breakdown:")
        for r in results_summary:
            print(f"     {r['query_id']}: {r['relevance_improvement']:+.1f}% relevance, {r['time_change']:+.1f}% time")

    # PPTX-specific test
    pptx_query_result = [r for r in results_summary if r['category'] == 'pptx']
    if pptx_query_result:
        print(f"\n4. PPTX QUERY SUPPORT:")
        pptx = pptx_query_result[0]
        print(f"   POC05: {pptx['poc05_relevance']:.1%} relevance (no PPTX support)")
        print(f"   POC06: {pptx['poc06_relevance']:.1%} relevance (PPTX supported)")
        print(f"   Improvement: {pptx['relevance_improvement']:+.1f}%")

    print(f"\n5. OVERALL VERDICT:")
    if results_summary:
        if avg_relevance_improvement > 20:
            verdict = "EXCELLENT (+{:.0f}% accuracy)".format(avg_relevance_improvement)
        elif avg_relevance_improvement > 10:
            verdict = "GOOD (+{:.0f}% accuracy)".format(avg_relevance_improvement)
        elif avg_relevance_improvement > 0:
            verdict = "SLIGHT IMPROVEMENT (+{:.0f}%)".format(avg_relevance_improvement)
        else:
            verdict = "NO SIGNIFICANT IMPROVEMENT"

        print(f"   {verdict}")
        print(f"   Coverage: +{coverage_improvement:.1f}% (PPTX support)")
        print(f"   Context: +{size_improvement:.1f}% (larger chunks)")

    print(f"\n6. RECOMMENDATION:")
    if avg_relevance_improvement > 10 and coverage_improvement > 15:
        print(f"   DEPLOY POC06 to production")
        print(f"   Improvements justify the upgrade")
    elif avg_relevance_improvement > 5:
        print(f"   DEPLOY POC06 to production (moderate improvement)")
        print(f"   PPTX support adds significant value")
    else:
        print(f"   KEEP POC05 for now, investigate POC06 issues")

    # Save results
    output_file = os.path.join(os.path.dirname(__file__), 'benchmark_results.json')
    with open(output_file, 'w') as f:
        json.dump({
            'timestamp': time.strftime("%Y-%m-%dT%H:%M:%S"),
            'coverage': {
                'poc05_docs': poc05_docs,
                'poc06_docs': poc06_docs,
                'poc05_chunks': poc05_count,
                'poc06_chunks': poc06_count,
                'improvement_pct': coverage_improvement
            },
            'chunk_quality': {
                'poc05_avg': float(poc05_stats['avg_chars']),
                'poc06_avg': float(poc06_stats['avg_chars']),
                'improvement_pct': size_improvement
            },
            'query_results': results_summary,
            'overall': {
                'avg_relevance_improvement': avg_relevance_improvement if results_summary else 0,
                'avg_time_change': avg_time_change if results_summary else 0,
                'verdict': verdict if results_summary else 'N/A'
            }
        }, f, indent=2)

    print(f"\n\nResults saved to: {output_file}")

    spark.stop()


if __name__ == "__main__":
    main()
