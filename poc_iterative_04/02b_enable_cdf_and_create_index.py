#!/usr/bin/env python3
"""
Enable Change Data Feed and Create Vector Index for POC04
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
    print("ENABLE CDF & CREATE POC04 VECTOR INDEX")
    print("=" * 80)

    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    print("\n[1] Connecting...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()

    namespace = config['rag_config']['full_namespace']
    source_table = f"{namespace}.poc04_optimized_chunks"
    index_name = f"{namespace}.poc04_optimized_rag_index"

    # Enable Change Data Feed
    print(f"\n[2] Enabling Change Data Feed on {source_table}...")
    spark.sql(f"ALTER TABLE {source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print("    CDF enabled")

    # Verify
    row_count = spark.sql(f"SELECT COUNT(*) as count FROM {source_table}").collect()[0]['count']
    print(f"    Table ready: {row_count} rows")

    # Create index
    print(f"\n[3] Creating vector index...")
    print(f"    Index: {index_name}")
    print(f"    This will take 10-15 minutes for {row_count} chunks")

    vs_client = VectorSearchClient(
        workspace_url=config['databricks']['host'],
        personal_access_token=config['databricks']['token'],
        disable_notice=True
    )

    try:
        # Delete if exists
        try:
            vs_client.delete_index(
                endpoint_name=config['rag_config']['vector_search_endpoint'],
                index_name=index_name
            )
            print("    Deleted existing index, waiting...")
            time.sleep(15)
        except:
            pass

        # Create
        index = vs_client.create_delta_sync_index(
            endpoint_name=config['rag_config']['vector_search_endpoint'],
            source_table_name=source_table,
            index_name=index_name,
            pipeline_type="TRIGGERED",
            primary_key="id",
            embedding_source_column="content",
            embedding_model_endpoint_name=config['rag_config']['embedding_model']
        )

        print(f"    Index created!")

        # Wait for sync
        print(f"\n[4] Monitoring sync (checking every 30s, max 15 min)...")
        max_wait = 900
        waited = 0

        while waited < max_wait:
            try:
                status = vs_client.get_index(
                    endpoint_name=config['rag_config']['vector_search_endpoint'],
                    index_name=index_name
                )
                desc = status.describe()
                indexed = desc['status'].get('indexed_row_count', 0)
                state = desc['status'].get('detailed_state', 'UNKNOWN')
                ready = desc['status'].get('ready', False)

                print(f"    [{waited}s] {state} | Indexed: {indexed}/{row_count}")

                if ready and indexed == row_count:
                    print(f"    SUCCESS: Fully synced!")
                    break
            except:
                print(f"    [{waited}s] Syncing...")

            time.sleep(30)
            waited += 30

        # Update config
        config['rag_config']['poc04_index'] = index_name
        config['rag_config']['poc04_table'] = source_table
        config['rag_config']['poc04_created_at'] = time.strftime("%Y-%m-%dT%H:%M:%S")
        config['rag_config']['poc04_chunk_count'] = row_count

        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)

        print("\n" + "=" * 80)
        print("POC04 INDEX READY")
        print("=" * 80)
        print(f"Index: {index_name}")
        print(f"Chunks: {row_count}")
        print(f"Next: Run benchmark to compare with POC03")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    spark.stop()


if __name__ == "__main__":
    main()
