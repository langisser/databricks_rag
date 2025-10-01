#!/usr/bin/env python3
"""Debug response structure"""

import sys
import os
import json
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.vector_search.client import VectorSearchClient

def main():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    vs_client = VectorSearchClient(
        workspace_url=config['databricks']['host'],
        personal_access_token=config['databricks']['token'],
        disable_notice=True
    )

    index_name = "sandbox.rdt_knowledge.poc03_multimodal_rag_index"
    endpoint_name = "rag-knowledge-base-endpoint"

    # Test with specific query
    test_query = "tbl_bot_rspn_sbmt_log columns"

    print(f"Query: {test_query}")
    print("=" * 80)

    response = vs_client.get_index(
        endpoint_name=endpoint_name,
        index_name=index_name
    ).similarity_search(
        query_text=test_query,
        columns=["id", "content"],
        num_results=10
    )

    print(f"\nResponse type: {type(response)}")

    if isinstance(response, dict):
        print(f"Response keys: {response.keys()}")
        print(f"\nFull response:")
        print(json.dumps(response, indent=2, default=str))

        if 'result' in response and 'data_array' in response['result']:
            data_array = response['result']['data_array']
            print(f"\ndata_array length: {len(data_array)}")
            if data_array:
                print(f"First row: {data_array[0]}")
    else:
        if hasattr(response, 'data_array'):
            print(f"\ndata_array exists: {response.data_array is not None}")
            if response.data_array:
                print(f"data_array length: {len(response.data_array)}")
                print(f"\nFirst row:")
                print(response.data_array[0])

if __name__ == "__main__":
    main()
