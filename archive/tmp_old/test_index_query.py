#!/usr/bin/env python3
"""Test vector search without column specification"""

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

    print("Testing all_documents_index")
    print("=" * 80)

    # Try different column combinations
    test_columns = [
        ["id", "text"],
        ["document_id", "text"],
        ["chunk_id", "text"],
        ["chunk_id", "chunk_text"],
    ]

    for cols in test_columns:
        print(f"\nTrying columns: {cols}")
        try:
            response = vs_client.get_index(
                endpoint_name="rag-knowledge-base-endpoint",
                index_name="sandbox.rdt_knowledge.all_documents_index"
            ).similarity_search(
                query_text="tbl_bot_rspn_sbmt_log columns",
                columns=cols,
                num_results=3
            )
            print(f"  SUCCESS! Returned {len(response.data_array) if hasattr(response, 'data_array') else 0} results")
            if hasattr(response, 'data_array') and response.data_array and len(response.data_array) > 0:
                first_row = response.data_array[0]
                print(f"  First row has {len(first_row)} fields")
                for i, val in enumerate(first_row):
                    val_preview = str(val)[:80] if val else None
                    print(f"    [{i}]: {val_preview}")
                break
        except Exception as e:
            print(f"  FAILED: {str(e)[:100]}")

if __name__ == "__main__":
    main()
