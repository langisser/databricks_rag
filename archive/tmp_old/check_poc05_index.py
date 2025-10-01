#!/usr/bin/env python3
"""Check POC05 index columns and try basic search"""

import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.vector_search.client import VectorSearchClient


def main():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    print("Connecting...")
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

    poc05_index = config['rag_config']['poc05_index']
    endpoint_name = config['rag_config']['vector_search_endpoint']

    print(f"\nGetting index: {poc05_index}")
    index = vs_client.get_index(
        endpoint_name=endpoint_name,
        index_name=poc05_index
    )

    print("\nIndex description:")
    desc = index.describe()
    print(json.dumps(desc, indent=2, default=str))

    print("\n\nTrying basic search without metadata columns...")
    results = index.similarity_search(
        query_text="table columns structure",
        columns=["id", "content"],
        num_results=5,
        query_type="HYBRID"
    )

    if hasattr(results, 'data_array'):
        data = results.data_array
    else:
        data = []

    print(f"\nResults: {len(data)}")
    for i, row in enumerate(data[:3], 1):
        print(f"\n[{i}]")
        print(f"ID: {row[0] if len(row) > 0 else 'N/A'}")
        print(f"Content: {row[1][:200] if len(row) > 1 else 'N/A'}...")
        print(f"Score: {row[2] if len(row) > 2 else 'N/A'}")

    spark.stop()


if __name__ == "__main__":
    main()
