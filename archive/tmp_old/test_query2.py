#!/usr/bin/env python3
"""Test query to check result object structure"""

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
print("TEST QUERY - CHECK RESULT STRUCTURE")
print("=" * 80)

# Get index
index = vs_client.get_index(
    endpoint_name=endpoint_name,
    index_name=index_name
)

# Try a simple query
print(f"\nQuery: 'table'")
results = index.similarity_search(
    query_text="table",
    columns=["id", "content"],
    num_results=3,
    query_type="ANN"
)

print(f"\nResult type: {type(results)}")
print(f"Result attributes: {dir(results)}")

# Check for different possible attributes
if hasattr(results, 'data_array'):
    print(f"\nHas data_array: {len(results.data_array)} results")
elif hasattr(results, 'result'):
    print(f"\nHas result attribute")
    print(f"Result type: {type(results.result)}")
    if hasattr(results.result, 'data_array'):
        print(f"result.data_array: {len(results.result.data_array)} results")
elif hasattr(results, '__dict__'):
    print(f"\n__dict__: {results.__dict__.keys()}")

# Try to convert to dict
try:
    result_dict = results.dict() if hasattr(results, 'dict') else str(results)
    print(f"\nResult as string/dict: {str(result_dict)[:500]}")
except Exception as e:
    print(f"Cannot convert to dict: {e}")
