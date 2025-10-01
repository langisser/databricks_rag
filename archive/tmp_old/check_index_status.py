#!/usr/bin/env python3
"""Check vector index status"""

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
index_name = f"{config['rag_config']['full_namespace']}.poc05_v02_all_formats_hybrid_rag_index"

print("=" * 80)
print("CHECKING VECTOR INDEX STATUS")
print("=" * 80)
print(f"\nEndpoint: {endpoint_name}")
print(f"Index: {index_name}")

try:
    index = vs_client.get_index(
        endpoint_name=endpoint_name,
        index_name=index_name
    )

    desc = index.describe()

    print(f"\n[STATUS]")
    print(f"  State: {desc['status'].get('detailed_state', 'UNKNOWN')}")
    print(f"  Ready: {desc['status'].get('ready', False)}")
    print(f"  Indexed Rows: {desc['status'].get('indexed_row_count', 0)}")
    print(f"  Message: {desc['status'].get('message', 'N/A')}")

    indexed = desc['status'].get('indexed_row_count', 0)
    ready = desc['status'].get('ready', False)

    if ready and indexed > 0:
        print(f"\n[SUCCESS] Index is ready with {indexed} chunks indexed!")
    elif indexed > 0:
        print(f"\n[IN PROGRESS] Index is syncing... {indexed} chunks indexed so far")
    else:
        print(f"\n[WAITING] Index is being created...")

except Exception as e:
    print(f"\n[ERROR] {e}")
    print("\nIndex may not exist yet or still being provisioned.")
    print("This is normal if the index was just created.")
