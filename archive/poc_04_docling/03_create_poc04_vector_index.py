#!/usr/bin/env python3
"""
POC Iterative 04 - Step 3: Create Optimized Vector Search Index
Creates: poc04_optimized_rag_index from poc04_optimized_chunks table
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
    print("CREATE POC04 OPTIMIZED VECTOR SEARCH INDEX")
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
    source_table = f"{namespace}.poc04_optimized_chunks"
    index_name = f"{namespace}.poc04_optimized_rag_index"
    endpoint_name = config['rag_config']['vector_search_endpoint']
    embedding_model = config['rag_config']['embedding_model']

    print(f"\n[3] Configuration:")
    print(f"    Source table: {source_table}")
    print(f"    Index name: {index_name}")
    print(f"    Endpoint: {endpoint_name}")
    print(f"    Embedding model: {embedding_model}")

    # Validate source table
    print(f"\n[4] Validating source table...")
    try:
        df = spark.sql(f"SELECT * FROM {source_table} LIMIT 1")
        row_count = spark.sql(f"SELECT COUNT(*) as count FROM {source_table}").collect()[0]['count']
        print(f"    Table exists with {row_count} rows")

        # Show schema
        print(f"\n    Table schema:")
        schema = spark.sql(f"DESCRIBE {source_table}")
        schema.show(truncate=False)

        # Show statistics
        print(f"\n    Content statistics:")
        stats = spark.sql(f"""
            SELECT content_type,
                   COUNT(*) as count,
                   AVG(char_count) as avg_chars,
                   AVG(original_char_count) as avg_original_chars
            FROM {source_table}
            GROUP BY content_type
        """)
        stats.show()

    except Exception as e:
        print(f"    ERROR: Table not found or inaccessible: {e}")
        spark.stop()
        return

    # Create vector search index
    print(f"\n[5] Creating vector search index...")
    print(f"    This may take 10-15 minutes for {row_count} chunks...")

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
            print(f"    Deleted. Waiting 15 seconds...")
            time.sleep(15)
        except Exception:
            pass  # Index doesn't exist, which is fine

        # Create new index
        print(f"    Creating index (be patient, focusing on quality over speed)...")
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
        print(f"    Embedding source: content (with bilingual metadata)")
        print(f"    Model: {embedding_model}")
        print(f"    Rows to index: {row_count}")

        # Wait for initial sync (extended timeout for large dataset)
        print(f"\n[7] Waiting for initial sync...")
        print(f"    Checking status every 30 seconds (may take 10-15 minutes)...")
        print(f"    FOCUS: Quality over speed - {row_count} chunks with bilingual embeddings")

        max_wait = 900  # 15 minutes
        waited = 0
        while waited < max_wait:
            try:
                status = vs_client.get_index(
                    endpoint_name=endpoint_name,
                    index_name=index_name
                )
                if hasattr(status, 'describe'):
                    desc = status.describe()
                    if 'status' in desc:
                        indexed_rows = desc['status'].get('indexed_row_count', 0)
                        ready = desc['status'].get('ready', False)
                        state = desc['status'].get('detailed_state', 'UNKNOWN')

                        print(f"    [{waited}s] State: {state} | Indexed: {indexed_rows}/{row_count} rows")

                        if ready and indexed_rows == row_count:
                            print(f"    SUCCESS: Index fully synced!")
                            break
            except Exception as e:
                print(f"    [{waited}s] Checking... (waiting for sync to complete)")

            time.sleep(30)
            waited += 30

        if waited >= max_wait:
            print(f"    NOTE: Index creation initiated but not fully synced yet")
            print(f"    Continue monitoring in Databricks UI")

        # Update config
        print(f"\n[8] Updating configuration...")
        config['rag_config']['poc04_index'] = index_name
        config['rag_config']['poc04_table'] = source_table
        config['rag_config']['poc04_created_at'] = time.strftime("%Y-%m-%dT%H:%M:%S")
        config['rag_config']['poc04_chunk_count'] = row_count

        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)

        print(f"    Configuration updated")

        # Summary
        print("\n" + "=" * 80)
        print("POC04 INDEX CREATION COMPLETE")
        print("=" * 80)
        print(f"\nVector Search Index: {index_name}")
        print(f"Source Table: {source_table}")
        print(f"Total Chunks: {row_count}")
        print(f"Embedding Model: {embedding_model}")
        print(f"\nOptimizations:")
        print(f"  - Semantic chunking with overlap (200 chars)")
        print(f"  - Bilingual metadata (Thai + English keywords)")
        print(f"  - Enhanced table column information")
        print(f"  - Optimized chunk sizes (text: 235 chars, tables: 1053 chars)")
        print(f"\nNext Steps:")
        print(f"1. Wait for index to finish syncing (check Databricks UI)")
        print(f"2. Test in AI Playground with index: {index_name}")
        print(f"3. Run benchmark: python poc_iterative_04/04_benchmark_poc04.py")
        print(f"4. Compare POC03 vs POC04 performance")

    except Exception as e:
        print(f"\n    ERROR creating index: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()


if __name__ == "__main__":
    main()
