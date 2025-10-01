#!/usr/bin/env python3
"""
POC05 - Inspect Query 3 Results
Why does hybrid search get 0.93 score but 0% recall?
Let's see what it actually retrieved
"""

import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.vector_search.client import VectorSearchClient


def main():
    print("=" * 80)
    print("QUERY 3 INSPECTION: What did hybrid search actually find?")
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

    poc04_index = config['rag_config']['poc04_index']
    endpoint_name = config['rag_config']['vector_search_endpoint']

    # Query 3 - the failing one
    query_text = "LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร"
    relevant_keywords = ["LOG_ID", "SUBMISSION_ID", "RESPONSE_STATUS", "column"]

    print(f"\n[2] Query: {query_text}")
    print(f"    Expected keywords: {relevant_keywords}")

    # Get hybrid search results
    print(f"\n[3] Fetching HYBRID search results (top 10)...")
    index = vs_client.get_index(
        endpoint_name=endpoint_name,
        index_name=poc04_index
    )

    response = index.similarity_search(
        query_text=query_text,
        columns=["id", "content"],
        num_results=10,
        query_type="HYBRID"
    )

    # Parse results
    if hasattr(response, 'data_array'):
        data_array = response.data_array
    elif isinstance(response, dict) and 'result' in response:
        data_array = response['result'].get('data_array', [])
    else:
        data_array = []

    print(f"\n[4] Analyzing top 10 results...")
    print("="*80)

    for i, row in enumerate(data_array, 1):
        chunk_id = row[0] if len(row) > 0 else None
        content = row[1] if len(row) > 1 else ''
        score = row[2] if len(row) > 2 else 0.0

        # Check which keywords are present
        keywords_found = [kw for kw in relevant_keywords if kw.lower() in content.lower()]

        print(f"\n[Result {i}] Score: {score:.6f}")
        print(f"Keywords found: {len(keywords_found)}/{len(relevant_keywords)} - {keywords_found}")

        # Show first 500 chars of content
        content_preview = content[:500].replace('\n', ' ')
        print(f"Content preview: {content_preview}...")

        if i <= 3:
            # For top 3, show more detail
            print(f"\nFull content snippet:")
            print("-" * 80)
            print(content[:1000])
            print("-" * 80)

    # Analysis
    print(f"\n" + "="*80)
    print("ANALYSIS")
    print("="*80)

    print("""
The Query 3 problem:

Query: "LOG_ID SUBMISSION_ID RESPONSE_STATUS คือ column อะไร"
Meaning: "What are the LOG_ID SUBMISSION_ID RESPONSE_STATUS columns?"

Expected: Find chunks that DEFINE these columns (their purpose, data type, etc.)

What hybrid search found: Chunks that MENTION these columns (in any context)

The column names appear in many places:
- Table schema definitions (CORRECT - what we want)
- Code examples using these columns (INCORRECT)
- Process descriptions mentioning these columns (INCORRECT)
- Validation rules checking these columns (INCORRECT)

SOLUTION: Need to add content_type metadata to filter to ONLY table definitions
""")

    spark.stop()


if __name__ == "__main__":
    main()
