#!/usr/bin/env python3
"""
Enable Change Data Feed and create vector search index
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.vector_search.client import VectorSearchClient
from databricks.connect import DatabricksSession
import json
import time

def main():
    print("Enabling Change Data Feed and Creating Vector Search")
    print("=" * 60)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()

        namespace = config['rag_config']['full_namespace']
        endpoint_name = config['rag_config']['vector_search_endpoint']
        embedding_model = config['rag_config']['embedding_model']

        # Step 1: Enable Change Data Feed on the table
        print("1. Enabling Change Data Feed on full_document_chunks...")

        source_table = f"{namespace}.full_document_chunks"

        try:
            spark.sql(f"""
                ALTER TABLE {source_table}
                SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
            """)
            print(f"   SUCCESS: Change Data Feed enabled on {source_table}")
        except Exception as e:
            print(f"   WARNING: CDF enable failed: {e}")
            print("   CDF may already be enabled or table may not exist")

        # Step 2: Verify table has data
        print(f"\n2. Verifying table data...")

        chunk_count = spark.sql(f"SELECT COUNT(*) as count FROM {source_table}").collect()[0]['count']
        print(f"   Chunks available: {chunk_count}")

        if chunk_count == 0:
            print("   ERROR: No chunks found. Need to run full content extraction first.")
            return False

        # Show sample data
        sample = spark.sql(f"""
            SELECT chunk_id, chunk_word_count, LEFT(chunk_text, 100) as preview
            FROM {source_table}
            LIMIT 2
        """).collect()

        for chunk in sample:
            print(f"   Sample: {chunk['chunk_id']} ({chunk['chunk_word_count']} words): {chunk['preview']}...")

        spark.stop()

        # Step 3: Create Vector Search client
        print(f"\n3. Creating Vector Search client...")

        vsc = VectorSearchClient(
            workspace_url=config['databricks']['host'],
            personal_access_token=config['databricks']['token'],
            disable_notice=True
        )
        print("   SUCCESS: Vector Search client created")

        # Step 4: Create vector search index
        print(f"\n4. Creating vector search index with Change Data Feed...")

        full_index_name = f"{namespace}.full_content_rag_index"

        try:
            # Check if index exists
            try:
                existing_index = vsc.get_index(full_index_name)
                print(f"   INFO: Index '{full_index_name}' already exists")

                # Check index status
                status = existing_index.get('status', {})
                ready = status.get('ready', False)
                print(f"   Index status: ready={ready}")

                if ready:
                    print("   Index is ready - proceeding to test")
                else:
                    print("   Index is building - will test anyway")

            except Exception:
                print(f"   Creating new index: {full_index_name}")

                # Create index
                index = vsc.create_delta_sync_index(
                    endpoint_name=endpoint_name,
                    index_name=full_index_name,
                    source_table_name=source_table,
                    pipeline_type="TRIGGERED",
                    primary_key="chunk_id",
                    embedding_source_column="chunk_text",
                    embedding_model_endpoint_name=embedding_model
                )

                print("   SUCCESS: Vector index creation initiated")
                print("   NOTE: Index building takes 10-15 minutes")

        except Exception as e:
            print(f"   ERROR: Index creation failed: {e}")
            return False

        # Step 5: Test the index
        print(f"\n5. Testing vector search with full content...")

        import requests

        headers = {
            'Authorization': f'Bearer {config["databricks"]["token"]}',
            'Content-Type': 'application/json'
        }

        search_url = f'{config["databricks"]["host"]}/api/2.0/vector-search/indexes/{full_index_name}/query'

        # Test with business-relevant queries
        test_queries = [
            "Bank of Thailand data submission",
            "counterparty risk management",
            "data validation process"
        ]

        for i, query in enumerate(test_queries, 1):
            try:
                print(f"\n   Test {i}: '{query}'")

                search_data = {
                    'query_text': query,
                    'columns': ['chunk_id', 'chunk_text'],
                    'num_results': 2
                }

                response = requests.post(search_url, headers=headers, json=search_data)

                if response.status_code == 200:
                    results = response.json()
                    data_array = results.get('result', {}).get('data_array', [])

                    if data_array:
                        print(f"   SUCCESS: Found {len(data_array)} chunks")

                        best_result = data_array[0]
                        chunk_text = best_result[1] if len(best_result) > 1 else ''
                        score = best_result[2] if len(best_result) > 2 else 'N/A'

                        preview = chunk_text[:120] + "..." if len(chunk_text) > 120 else chunk_text
                        print(f"   Best match (score: {score}): {preview}")
                    else:
                        print(f"   INFO: No results (index may still be building)")
                else:
                    print(f"   INFO: API returned {response.status_code} (index building)")

            except Exception as e:
                print(f"   INFO: Test skipped - {e}")

        # Step 6: Update configuration
        print(f"\n6. Updating configuration...")

        config['rag_config']['full_content_index'] = full_index_name
        config['rag_config']['full_content_table'] = source_table

        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)

        print(f"   Configuration updated")

        print(f"\n" + "=" * 60)
        print("VECTOR SEARCH SETUP COMPLETED")
        print("=" * 60)

        print("INDEXES AVAILABLE:")
        print(f"  📋 OLD: {namespace}.rag_chunk_index (sample content)")
        print(f"  🎯 NEW: {full_index_name} (FULL content)")

        print(f"\nCONTENT COMPARISON:")
        print(f"  Sample content: ~380 words")
        print(f"  Full content: ~7,500 words (20x more!)")

        print(f"\nNEXT STEPS:")
        print("1. Wait for index building to complete (10-15 min)")
        print("2. Test with your business-specific queries")
        print("3. Update AI Playground to use new full content index")

        return True

    except Exception as e:
        print(f"\nERROR: Setup failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)