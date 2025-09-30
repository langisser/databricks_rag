#!/usr/bin/env python3
"""
Create Vector Search Index with Full Content - Fixed Version
- Remove metadata column that causes type incompatibility
- Use simplified table structure compatible with vector search
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.vector_search.client import VectorSearchClient
from databricks.connect import DatabricksSession
import json
import time
import requests

def main():
    print("=" * 60)
    print("CREATING VECTOR SEARCH WITH FULL CONTENT - FIXED")
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

        print("1. Configuration:")
        print(f"   Namespace: {namespace}")
        print(f"   Endpoint: {endpoint_name}")
        print(f"   Embedding Model: {embedding_model}")

        # Step 2: Create compatible table for vector search
        print(f"\n2. Creating vector search compatible table...")

        # New simplified table without metadata column
        vs_table_name = f"{namespace}.vs_document_chunks"

        spark.sql(f"DROP TABLE IF EXISTS {vs_table_name}")

        spark.sql(f"""
            CREATE TABLE {vs_table_name} (
                chunk_id STRING,
                document_id STRING,
                chunk_index INT,
                chunk_text STRING,
                chunk_word_count INT,
                chunk_char_count INT,
                chunk_strategy STRING,
                semantic_density DOUBLE,
                source_file STRING,
                extraction_method STRING,
                created_timestamp TIMESTAMP
            )
            USING DELTA
            TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)

        print(f"   Created compatible table: {vs_table_name}")

        # Step 3: Copy data from full table to compatible table
        print(f"\n3. Copying data to vector search compatible table...")

        spark.sql(f"""
            INSERT INTO {vs_table_name}
            SELECT
                chunk_id,
                document_id,
                chunk_index,
                chunk_text,
                chunk_word_count,
                chunk_char_count,
                chunk_strategy,
                semantic_density,
                metadata['source_file'] as source_file,
                metadata['extraction_method'] as extraction_method,
                created_timestamp
            FROM {namespace}.full_document_chunks
        """)

        chunk_count = spark.sql(f"SELECT COUNT(*) as count FROM {vs_table_name}").collect()[0]['count']
        print(f"   Copied {chunk_count} chunks to compatible table")

        # Show content statistics
        content_stats = spark.sql(f"""
            SELECT
                COUNT(DISTINCT document_id) as doc_count,
                COUNT(*) as chunk_count,
                SUM(chunk_word_count) as total_words,
                AVG(chunk_word_count) as avg_words,
                MIN(chunk_word_count) as min_words,
                MAX(chunk_word_count) as max_words
            FROM {vs_table_name}
        """).collect()[0]

        print(f"   Content stats: {content_stats['total_words']} total words across {content_stats['doc_count']} documents")
        print(f"   Chunk range: {content_stats['min_words']}-{content_stats['max_words']} words")

        spark.stop()

        # Step 4: Create Vector Search client
        print(f"\n4. Creating Vector Search client...")

        vsc = VectorSearchClient(
            workspace_url=config['databricks']['host'],
            personal_access_token=config['databricks']['token'],
            disable_notice=True
        )
        print("   SUCCESS: Vector Search client created")

        # Step 5: Create vector search index
        print(f"\n5. Creating vector search index with full content...")

        full_index_name = f"{namespace}.full_content_rag_index"

        try:
            # Delete existing index if it exists
            try:
                existing_index = vsc.get_index(full_index_name)
                print(f"   Deleting existing index...")
                vsc.delete_index(full_index_name)
                time.sleep(10)  # Wait for deletion
            except Exception:
                print(f"   No existing index to delete")

            # Create new index with compatible table
            print(f"   Creating index: {full_index_name}")
            print(f"   Source table: {vs_table_name}")

            index = vsc.create_delta_sync_index(
                endpoint_name=endpoint_name,
                index_name=full_index_name,
                source_table_name=vs_table_name,
                pipeline_type="TRIGGERED",
                primary_key="chunk_id",
                embedding_source_column="chunk_text",
                embedding_model_endpoint_name=embedding_model
            )

            print("   SUCCESS: Vector index creation initiated")
            print("   NOTE: Index building takes 15-20 minutes with full content")

        except Exception as e:
            print(f"   ERROR: Index creation failed: {e}")
            return False

        # Step 6: Test the index
        print(f"\n6. Testing vector search with full content...")

        # Wait for index to initialize
        print("   Waiting for index to initialize...")
        time.sleep(30)

        headers = {
            'Authorization': f'Bearer {config["databricks"]["token"]}',
            'Content-Type': 'application/json'
        }

        search_url = f'{config["databricks"]["host"]}/api/2.0/vector-search/indexes/{full_index_name}/query'

        # Test with relevant business queries
        test_queries = [
            "Bank of Thailand data submission requirements",
            "counterparty risk management procedures",
            "data validation rules and processes"
        ]

        successful_queries = 0

        for i, query in enumerate(test_queries, 1):
            try:
                print(f"\n   Test {i}: '{query}'")

                search_data = {
                    'query_text': query,
                    'columns': ['chunk_id', 'chunk_text', 'source_file'],
                    'num_results': 2
                }

                response = requests.post(search_url, headers=headers, json=search_data)

                if response.status_code == 200:
                    results = response.json()
                    data_array = results.get('result', {}).get('data_array', [])

                    if data_array:
                        print(f"   SUCCESS: Found {len(data_array)} relevant chunks")
                        best_result = data_array[0]
                        chunk_text = best_result[1] if len(best_result) > 1 else ''
                        source_file = best_result[2] if len(best_result) > 2 else 'unknown'

                        preview = chunk_text[:120] + "..." if len(chunk_text) > 120 else chunk_text
                        print(f"   Best match from {source_file}: {preview}")
                        successful_queries += 1
                    else:
                        print(f"   INFO: No results (index still building)")
                else:
                    print(f"   INFO: API returned {response.status_code} (index building)")

            except Exception as e:
                print(f"   INFO: Test skipped - {e}")

        # Step 7: Update configuration
        print(f"\n7. Updating configuration...")

        config['rag_config']['full_content_index'] = full_index_name
        config['rag_config']['full_content_table'] = vs_table_name

        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)

        print(f"   Configuration updated")

        # Step 8: Summary
        print(f"\n" + "=" * 60)
        print("VECTOR SEARCH WITH FULL CONTENT CREATED")
        print("=" * 60)

        print("INDEXES AVAILABLE:")
        print(f"  OLD: {namespace}.rag_chunk_index (sample content)")
        print(f"  NEW: {full_index_name} (FULL content)")

        print(f"\nCONTENT COMPARISON:")
        print(f"  Sample content: ~380 words")
        print(f"  Full content: {content_stats['total_words']} words ({content_stats['total_words']//380}x more!)")

        print(f"\nSOURCE:")
        print(f"  Table: {vs_table_name}")
        print(f"  Documents: {content_stats['doc_count']}")
        print(f"  Chunks: {content_stats['chunk_count']}")

        print(f"\nNEXT STEPS:")
        print("1. Wait 15-20 minutes for index building to complete")
        print("2. Test with your business-specific queries")
        print("3. Update AI Playground to use new full content index")

        if successful_queries > 0:
            print(f"\nEARLY SUCCESS: {successful_queries}/{len(test_queries)} queries already working!")
        else:
            print(f"\nINDEX BUILDING: Wait for completion, then test queries")

        return True

    except Exception as e:
        print(f"\nERROR: Vector search creation failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)