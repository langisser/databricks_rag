#!/usr/bin/env python3
"""Test query to debug why no results"""

import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.vector_search.client import VectorSearchClient

# Load config
config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
with open(config_path, 'r') as f:
    config = json.load(f)

# Vector Search Client
vs_client = VectorSearchClient(
    workspace_url=config['databricks']['host'],
    personal_access_token=config['databricks']['token'],
    disable_notice=True
)

endpoint_name = config['rag_config']['vector_search_endpoint']
index_name = "sandbox.rdt_knowledge.poc05_v02_all_formats_hybrid_rag_index"

print("=" * 80)
print("TEST QUERY - DEBUG")
print("=" * 80)
print(f"\nIndex: {index_name}")

# Get index
index = vs_client.get_index(
    endpoint_name=endpoint_name,
    index_name=index_name
)

# Check status
desc = index.describe()
print(f"\nStatus: {desc['status'].get('detailed_state')}")
print(f"Ready: {desc['status'].get('ready')}")
print(f"Indexed: {desc['status'].get('indexed_row_count')}")

# Try a simple query
print(f"\n[TEST 1] Simple ANN query: 'table'")
results = index.similarity_search(
    query_text="table",
    columns=["id", "content"],
    num_results=3,
    query_type="ANN"
)

if hasattr(results, 'data_array'):
    print(f"Results: {len(results.data_array)}")
    if results.data_array:
        print(f"\nFirst result:")
        print(f"  ID: {results.data_array[0][0]}")
        print(f"  Content preview: {results.data_array[0][1][:200]}")
        print(f"  Score: {results.data_array[0][2]}")
else:
    print(f"No data_array attribute")

# Try HYBRID
print(f"\n[TEST 2] HYBRID query: 'tbl_ingt_job columns'")
results2 = index.similarity_search(
    query_text="tbl_ingt_job columns",
    columns=["id", "content"],
    num_results=3,
    query_type="HYBRID"
)

if hasattr(results2, 'data_array'):
    print(f"Results: {len(results2.data_array)}")
    if results2.data_array:
        print(f"\nFirst result:")
        print(f"  ID: {results2.data_array[0][0]}")
        print(f"  Content preview: {results2.data_array[0][1][:200]}")
        print(f"  Score: {results2.data_array[0][2]}")
else:
    print(f"No data_array attribute")

# Try with filters
print(f"\n[TEST 3] HYBRID query with filter: content_category='table_definition'")
try:
    results3 = index.similarity_search(
        query_text="tbl_ingt_job columns",
        columns=["id", "content", "content_category"],
        num_results=3,
        query_type="HYBRID",
        filters={"content_category": "table_definition"}
    )

    if hasattr(results3, 'data_array'):
        print(f"Results: {len(results3.data_array)}")
        if results3.data_array:
            print(f"\nFirst result:")
            print(f"  ID: {results3.data_array[0][0]}")
            print(f"  Category: {results3.data_array[0][2]}")
            print(f"  Content preview: {results3.data_array[0][1][:200]}")
    else:
        print(f"No data_array attribute")
except Exception as e:
    print(f"ERROR: {e}")
