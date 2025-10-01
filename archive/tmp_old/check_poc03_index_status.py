#!/usr/bin/env python3
"""Check POC03 index sync status"""

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

    print("=" * 80)
    print("POC03 INDEX STATUS CHECK")
    print("=" * 80)

    # Connect to Databricks
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

    namespace = config['rag_config']['full_namespace']
    index_name = f"{namespace}.poc03_multimodal_rag_index"
    table_name = f"{namespace}.datax_multimodal_chunks"
    endpoint_name = config['rag_config']['vector_search_endpoint']

    print(f"\nIndex: {index_name}")
    print(f"Table: {table_name}")

    # Check source table
    print(f"\n[1] Source table row count:")
    row_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
    print(f"    {row_count} rows")

    # Check index status
    print(f"\n[2] Vector index status:")
    try:
        index = vs_client.get_index(
            endpoint_name=endpoint_name,
            index_name=index_name
        )
        print(f"    Index exists")
        print(f"    Index details: {index}")

        # Try to get index description
        if hasattr(index, 'describe'):
            desc = index.describe()
            print(f"\n[3] Index description:")
            print(f"    {desc}")
    except Exception as e:
        print(f"    ERROR: {e}")

    # Try a simple search
    print(f"\n[4] Test search:")
    try:
        response = vs_client.get_index(
            endpoint_name=endpoint_name,
            index_name=index_name
        ).similarity_search(
            query_text="table",
            columns=["id", "content"],
            num_results=3
        )

        if hasattr(response, 'data_array'):
            print(f"    Results returned: {len(response.data_array) if response.data_array else 0}")
            if response.data_array:
                print(f"    First result ID: {response.data_array[0][0]}")
        else:
            print(f"    Response: {response}")
    except Exception as e:
        print(f"    ERROR: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
