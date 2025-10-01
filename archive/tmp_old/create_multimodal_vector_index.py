#!/usr/bin/env python3
"""
Create vector search index for multimodal DataX content
"""
import sys
import os
import json
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType, DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, PipelineType

def main():
    print("=" * 80)
    print("CREATE VECTOR SEARCH INDEX FOR MULTIMODAL CONTENT")
    print("=" * 80)

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Connect
    print("\n[STEP 1] Connecting to Databricks...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()

    w = WorkspaceClient(
        host=config['databricks']['host'],
        token=config['databricks']['token']
    )

    namespace = config['rag_config']['full_namespace']
    table_name = f"{namespace}.datax_multimodal_chunks"
    index_name = f"{namespace}.datax_multimodal_index"
    endpoint_name = config['rag_config']['vector_search_endpoint']

    print(f"  [SUCCESS] Connected")
    print(f"\n  Table: {table_name}")
    print(f"  Index: {index_name}")
    print(f"  Endpoint: {endpoint_name}")

    # Enable CDF
    print("\n[STEP 2] Enabling Change Data Feed...")
    try:
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print("  [SUCCESS] CDF enabled")
    except Exception as e:
        print(f"  [INFO] CDF may already be enabled: {e}")

    # Check if index exists
    print("\n[STEP 3] Checking existing index...")
    try:
        existing_index = w.vector_search_indexes.get_index(index_name)
        print(f"  [INFO] Index exists, deleting to recreate...")
        w.vector_search_indexes.delete_index(index_name)
        time.sleep(5)
        print("  [SUCCESS] Old index deleted")
    except Exception as e:
        print(f"  [INFO] No existing index found (this is ok)")

    # Create index
    print("\n[STEP 4] Creating vector search index...")
    try:
        from databricks.sdk.service.vectorsearch import VectorIndexType

        index = w.vector_search_indexes.create_index(
            name=index_name,
            endpoint_name=endpoint_name,
            primary_key='id',
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=table_name,
                pipeline_type=PipelineType.TRIGGERED,
                embedding_source_columns=[
                    EmbeddingSourceColumn(
                        name='content',
                        embedding_model_endpoint_name='databricks-gte-large-en'
                    )
                ]
            )
        )
        print(f"  [SUCCESS] Index creation started!")
        print(f"  Index: {index.name}")

    except Exception as e:
        print(f"  [ERROR] Failed to create index: {e}")
        import traceback
        traceback.print_exc()
        return

    # Wait for index to be ready
    print("\n[STEP 5] Waiting for index to sync...")
    max_wait = 180  # 3 minutes
    start_time = time.time()

    while (time.time() - start_time) < max_wait:
        try:
            status = w.vector_search_indexes.get_index(index_name)

            if hasattr(status, 'status'):
                state = status.status.message if hasattr(status.status, 'message') else 'Unknown'
                print(f"  Status: {state}")

                if hasattr(status.status, 'ready') and status.status.ready:
                    print(f"\n  [SUCCESS] Index is READY!")
                    break
            else:
                print(f"  Syncing...")

        except Exception as e:
            print(f"  Checking... {e}")

        time.sleep(10)

    # Verify
    print("\n[STEP 6] Verifying index...")
    try:
        final_status = w.vector_search_indexes.get_index(index_name)
        print(f"  Index name: {final_status.name}")
        print(f"  Primary key: {final_status.primary_key}")
        print(f"  Endpoint: {final_status.endpoint_name}")

        # Check row count
        table_df = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}")
        row_count = table_df.collect()[0]['count']
        print(f"  Source rows: {row_count}")

    except Exception as e:
        print(f"  [WARNING] Could not verify: {e}")

    # Summary
    print("\n" + "=" * 80)
    print("VECTOR SEARCH INDEX CREATED")
    print("=" * 80)
    print(f"\nIndex: {index_name}")
    print(f"Table: {table_name}")
    print(f"Endpoint: {endpoint_name}")
    print(f"\n[NEXT STEP] Test in AI Playground:")
    print(f"  1. Go to AI Playground in Databricks")
    print(f"  2. Select Retrieval tab")
    print(f"  3. Choose index: {index_name}")
    print(f"  4. Ask: 'What is the submission process to BOT?'")

if __name__ == "__main__":
    main()