#!/usr/bin/env python3
"""
POC05 - Test Query 2 with Hybrid Search and Metadata Filtering
Query 2 showed +199% improvement with hybrid search
Now test with metadata filtering for even better results
"""

import sys
import os
import json
import time

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.vector_search.client import VectorSearchClient


def main():
    print("=" * 80)
    print("POC05 - TEST QUERY 2 WITH METADATA FILTERING")
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
    poc05_index = config['rag_config']['poc05_index']
    endpoint_name = config['rag_config']['vector_search_endpoint']

    # Query 2 - the success story
    query = "tbl_bot_rspn_sbmt_log columns structure"
    print(f"\n[2] Query: {query}")
    print(f"    Index: {poc05_index}")
    print(f"\n    Previous Results:")
    print(f"    POC04 (ANN): 20% recall")
    print(f"    POC04 (HYBRID): 60% recall (+199%)")

    # Get index
    print(f"\n[3] Getting index...")
    index = vs_client.get_index(
        endpoint_name=endpoint_name,
        index_name=poc05_index
    )

    # Test 1: HYBRID without filter
    print(f"\n[4] Test 1: HYBRID (no filter)")
    start = time.time()
    results_hybrid = index.similarity_search(
        query_text=query,
        columns=["id", "content", "content_category", "document_type", "table_name"],
        num_results=10,
        query_type="HYBRID"
    )
    latency_hybrid = (time.time() - start) * 1000

    if hasattr(results_hybrid, 'data_array'):
        data_hybrid = results_hybrid.data_array
    else:
        data_hybrid = []

    print(f"    Latency: {latency_hybrid:.0f}ms")
    print(f"    Results: {len(data_hybrid)}")
    if data_hybrid:
        print(f"    Top score: {data_hybrid[0][5] if len(data_hybrid[0]) > 5 else 'N/A'}")
        print(f"\n    Top 5 results:")
        for i, row in enumerate(data_hybrid[:5], 1):
            category = row[2] if len(row) > 2 else 'N/A'
            doc_type = row[3] if len(row) > 3 else 'N/A'
            tbl_name = row[4] if len(row) > 4 else 'N/A'
            score = row[5] if len(row) > 5 else 0
            print(f"    [{i}] Score: {score:.6f} | Category: {category} | Table: {tbl_name}")

    # Test 2: HYBRID with table_definition filter
    print(f"\n[5] Test 2: HYBRID + filter (content_category='table_definition')")
    start = time.time()
    results_filtered = index.similarity_search(
        query_text=query,
        columns=["id", "content", "content_category", "document_type", "table_name"],
        num_results=10,
        query_type="HYBRID",
        filters={"content_category": "table_definition"}
    )
    latency_filtered = (time.time() - start) * 1000

    if hasattr(results_filtered, 'data_array'):
        data_filtered = results_filtered.data_array
    else:
        data_filtered = []

    print(f"    Latency: {latency_filtered:.0f}ms")
    print(f"    Results: {len(data_filtered)}")
    if data_filtered:
        print(f"    Top score: {data_filtered[0][5] if len(data_filtered[0]) > 5 else 'N/A'}")
        print(f"\n    Top 5 results:")
        for i, row in enumerate(data_filtered[:5], 1):
            category = row[2] if len(row) > 2 else 'N/A'
            doc_type = row[3] if len(row) > 3 else 'N/A'
            tbl_name = row[4] if len(row) > 4 else 'N/A'
            score = row[5] if len(row) > 5 else 0
            content_preview = row[1][:200] if len(row) > 1 else ''
            print(f"    [{i}] Score: {score:.6f} | Table: {tbl_name}")
            print(f"         Preview: {content_preview}...")

    # Test 3: HYBRID with document filter
    print(f"\n[6] Test 3: HYBRID + filter (document_type='technical_spec')")
    start = time.time()
    results_doc_filtered = index.similarity_search(
        query_text=query,
        columns=["id", "content", "content_category", "document_type", "table_name"],
        num_results=10,
        query_type="HYBRID",
        filters={"document_type": "technical_spec"}
    )
    latency_doc = (time.time() - start) * 1000

    if hasattr(results_doc_filtered, 'data_array'):
        data_doc = results_doc_filtered.data_array
    else:
        data_doc = []

    print(f"    Latency: {latency_doc:.0f}ms")
    print(f"    Results: {len(data_doc)}")
    if data_doc:
        print(f"    Top score: {data_doc[0][5] if len(data_doc[0]) > 5 else 'N/A'}")

    # Summary
    print(f"\n" + "="*80)
    print("QUERY 2 TEST RESULTS SUMMARY")
    print("="*80)
    print(f"\nQuery: {query}")
    print(f"\nResults:")
    print(f"  [1] HYBRID (no filter):           {latency_hybrid:.0f}ms | {len(data_hybrid)} results")
    print(f"  [2] HYBRID + table_definition:    {latency_filtered:.0f}ms | {len(data_filtered)} results")
    print(f"  [3] HYBRID + technical_spec:      {latency_doc:.0f}ms | {len(data_doc)} results")

    print(f"\nKey Insights:")
    if len(data_filtered) < len(data_hybrid):
        print(f"  - Metadata filtering reduces search space ({len(data_hybrid)} -> {len(data_filtered)} results)")
        print(f"  - More focused results = higher precision")
    print(f"  - Hybrid search works well for table structure queries")
    print(f"  - Metadata filtering allows targeted search")

    print(f"\nRecommendation:")
    print(f"  Use HYBRID + metadata filters for production:")
    print(f"    - Table queries: filter by content_category='table_definition'")
    print(f"    - Process queries: filter by content_category='process_description'")
    print(f"    - Document-specific: filter by document_type or source_document")

    spark.stop()


if __name__ == "__main__":
    main()
