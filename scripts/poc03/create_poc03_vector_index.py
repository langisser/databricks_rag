#!/usr/bin/env python3
"""
Create POC03 Vector Search Index for datax_multimodal_chunks
Creates index: poc03_multimodal_rag_index
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
    print("CREATE POC03 VECTOR SEARCH INDEX")
    print("=" * 80)

    # Load config
    config_path = os.path.join(
        os.path.dirname(__file__), '..',
        'databricks_helper', 'databricks_config', 'config.json'
    )
    with open(config_path, 'r') as f:
        config = json.load(f)

    print("\n[1] Connecting to Databricks...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()
    print("    Connected")

    # Initialize Vector Search Client
    print("\n[2] Initializing Vector Search Client...")
    vs_client = VectorSearchClient(
        workspace_url=config['databricks']['host'],
        personal_access_token=config['databricks']['token'],
        disable_notice=True
    )
    print("    Initialized")

    # Configuration
    namespace = config['rag_config']['full_namespace']
    source_table = f"{namespace}.datax_multimodal_chunks"
    index_name = f"{namespace}.poc03_multimodal_rag_index"
    endpoint_name = config['rag_config']['vector_search_endpoint']
    embedding_model = config['rag_config']['embedding_model']

    print(f"\n[3] Configuration:")
    print(f"    Source table: {source_table}")
    print(f"    Index name: {index_name}")
    print(f"    Endpoint: {endpoint_name}")
    print(f"    Embedding model: {embedding_model}")

    # Check if table exists and get schema
    print(f"\n[4] Validating source table...")
    try:
        df = spark.sql(f"SELECT * FROM {source_table} LIMIT 1")
        row_count = spark.sql(f"SELECT COUNT(*) as count FROM {source_table}").collect()[0]['count']
        print(f"    Table exists with {row_count} rows")

        # Show schema
        print(f"\n    Table schema:")
        schema = spark.sql(f"DESCRIBE {source_table}")
        schema.show(truncate=False)
    except Exception as e:
        print(f"    ERROR: Table not found or inaccessible: {e}")
        spark.stop()
        return

    # Create vector search index
    print(f"\n[5] Creating vector search index...")
    print(f"    This may take several minutes...")

    try:
        # Check if index already exists
        try:
            existing_index = vs_client.get_index(
                endpoint_name=endpoint_name,
                index_name=index_name
            )
            print(f"    WARNING: Index already exists!")
            print(f"    Deleting existing index...")
            vs_client.delete_index(
                endpoint_name=endpoint_name,
                index_name=index_name
            )
            print(f"    Deleted. Waiting 10 seconds...")
            time.sleep(10)
        except Exception:
            pass  # Index doesn't exist, which is fine

        # Create new index
        print(f"    Creating index...")
        index = vs_client.create_delta_sync_index(
            endpoint_name=endpoint_name,
            source_table_name=source_table,
            index_name=index_name,
            pipeline_type="TRIGGERED",
            primary_key="id",
            embedding_source_column="content",
            embedding_model_endpoint_name=embedding_model
        )

        print(f"    Index created successfully!")
        print(f"\n[6] Index details:")
        print(f"    Name: {index_name}")
        print(f"    Status: Created (syncing in background)")
        print(f"    Primary key: id")
        print(f"    Embedding source: content")
        print(f"    Model: {embedding_model}")

        # Wait for initial sync
        print(f"\n[7] Waiting for initial sync...")
        print(f"    Checking status every 30 seconds...")

        max_wait = 300  # 5 minutes
        waited = 0
        while waited < max_wait:
            try:
                status = vs_client.get_index(
                    endpoint_name=endpoint_name,
                    index_name=index_name
                )
                print(f"    Status check at {waited}s: Index exists")
                break
            except Exception as e:
                print(f"    Waiting... ({waited}s elapsed)")
                time.sleep(30)
                waited += 30

        # Update config
        print(f"\n[8] Updating configuration...")
        config['rag_config']['poc03_index'] = index_name
        config['rag_config']['poc03_table'] = source_table
        config['rag_config']['poc03_created_at'] = time.strftime("%Y-%m-%dT%H:%M:%S")

        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)

        print(f"    Configuration updated")

        # Summary
        print("\n" + "=" * 80)
        print("INDEX CREATION COMPLETE")
        print("=" * 80)
        print(f"\nVector Search Index: {index_name}")
        print(f"Source Table: {source_table}")
        print(f"Row Count: {row_count}")
        print(f"\nNext Steps:")
        print(f"1. Wait for index to finish syncing (check Databricks UI)")
        print(f"2. Test in AI Playground with index: {index_name}")
        print(f"3. Run benchmark: python tmp/benchmark_poc03_index.py")

    except Exception as e:
        print(f"\n    ERROR creating index: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()


if __name__ == "__main__":
    main()
