#!/usr/bin/env python3
"""
Create NEW Vector Search Index with FULL Document Content
- Use full_document_chunks table with real content
- Create new index for production RAG
- Test with actual business document queries
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
    print("CREATING VECTOR SEARCH WITH FULL CONTENT")
    print("=" * 60)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        rag_config = config['rag_config']
        namespace = rag_config['full_namespace']
        endpoint_name = rag_config['vector_search_endpoint']
        embedding_model = rag_config['embedding_model']

        print("1. Configuration:")
        print(f"   Namespace: {namespace}")
        print(f"   Endpoint: {endpoint_name}")
        print(f"   Embedding Model: {embedding_model}")

        # Step 2: Verify full content is available
        print(f"\n2. Verifying full content data...")

        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()

        # Check full documents
        doc_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.full_documents").collect()[0]['count']
        chunk_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.full_document_chunks").collect()[0]['count']

        print(f"   Full documents: {doc_count}")
        print(f"   Full content chunks: {chunk_count}")

        if chunk_count == 0:
            print("   ERROR: No full content chunks found. Run full content extraction first.")
            return False

        # Show content statistics
        content_stats = spark.sql(f"""
            SELECT
                AVG(chunk_word_count) as avg_words,
                MIN(chunk_word_count) as min_words,
                MAX(chunk_word_count) as max_words,
                SUM(chunk_word_count) as total_words
            FROM {namespace}.full_document_chunks
        """).collect()[0]

        print(f"   Content stats: {content_stats['total_words']:.0f} total words")
        print(f"   Chunk range: {content_stats['min_words']}-{content_stats['max_words']} words")
        print(f"   Average chunk: {content_stats['avg_words']:.0f} words")

        spark.stop()

        # Step 3: Create Vector Search client
        print(f"\n3. Creating Vector Search client...")

        vsc = VectorSearchClient(
            workspace_url=config['databricks']['host'],
            personal_access_token=config['databricks']['token'],
            disable_notice=True
        )
        print("   SUCCESS: Vector Search client created")

        # Step 4: Create new vector search index for full content
        print(f"\n4. Creating NEW vector search index for full content...")

        # New index name for full content
        full_index_name = f"{namespace}.full_content_rag_index"
        source_table = f"{namespace}.full_document_chunks"

        print(f"   New index: {full_index_name}")
        print(f"   Source table: {source_table}")

        try:
            # Check if index already exists
            try:
                existing_index = vsc.get_index(full_index_name)
                print(f"   INFO: Index '{full_index_name}' already exists")

                # Delete existing index to recreate with new data
                print(f"   Deleting existing index to recreate with full content...")
                vsc.delete_index(full_index_name)
                print(f"   Old index deleted")

                # Wait a moment for deletion to complete
                time.sleep(10)

            except Exception:
                print(f"   Index does not exist, will create new one")

            # Create new index with full content
            print(f"   Creating vector index with full document content...")

            index = vsc.create_delta_sync_index(
                endpoint_name=endpoint_name,
                index_name=full_index_name,
                source_table_name=source_table,
                pipeline_type="TRIGGERED",
                primary_key="chunk_id",
                embedding_source_column="chunk_text",
                embedding_model_endpoint_name=embedding_model
            )

            print("   SUCCESS: Full content vector index creation initiated")
            print("   NOTE: Index building with full content may take 15-20 minutes")

        except Exception as e:
            print(f"   ERROR: Index creation failed: {e}")
            return False

        # Step 5: Wait and test the new index
        print(f"\n5. Testing new vector search with full content...")

        # Wait a bit for index to start building
        print("   Waiting for index to initialize...")
        time.sleep(30)

        # Test queries relevant to your actual business documents
        test_queries = [
            "Bank of Thailand BOT data submission requirements",
            "counterparty risk management procedures",
            "data validation rules and processes",
            "cross validation IWT production environment",
            "data entities elements submission process"
        ]

        # Setup REST API for testing
        headers = {
            'Authorization': f'Bearer {config["databricks"]["token"]}',
            'Content-Type': 'application/json'
        }

        search_url = f'{config["databricks"]["host"]}/api/2.0/vector-search/indexes/{full_index_name}/query'

        successful_queries = 0

        for i, query in enumerate(test_queries, 1):
            try:
                print(f"\n   Test {i}: '{query}'")

                search_data = {
                    'query_text': query,
                    'columns': ['chunk_id', 'chunk_text'],
                    'num_results': 3
                }

                response = requests.post(search_url, headers=headers, json=search_data)

                if response.status_code == 200:
                    results = response.json()
                    data_array = results.get('result', {}).get('data_array', [])

                    if data_array:
                        print(f"   SUCCESS: Found {len(data_array)} relevant chunks")

                        # Show best result
                        best_result = data_array[0]
                        chunk_text = best_result[1] if len(best_result) > 1 else ''
                        score = best_result[2] if len(best_result) > 2 else 'N/A'

                        # Show relevant excerpt
                        preview = chunk_text[:150] + "..." if len(chunk_text) > 150 else chunk_text
                        print(f"   Best match (score: {score}): {preview}")

                        successful_queries += 1
                    else:
                        print(f"   WARNING: No results found (index may still be building)")
                else:
                    print(f"   INFO: Search API returned {response.status_code} (index building)")

            except Exception as e:
                print(f"   INFO: Query test skipped - index still building: {e}")

        # Step 6: Update configuration
        print(f"\n6. Updating configuration for full content index...")

        # Add new index to configuration
        config['rag_config']['full_content_index'] = full_index_name
        config['rag_config']['full_content_table'] = source_table

        # Save updated configuration
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)

        print(f"   Configuration updated with new index")

        # Step 7: Summary
        print(f"\n" + "=" * 60)
        print("VECTOR SEARCH WITH FULL CONTENT CREATED")
        print("=" * 60)

        print("STATUS:")
        print(f"  ✅ OLD INDEX: {namespace}.rag_chunk_index (sample content)")
        print(f"  ✅ NEW INDEX: {full_index_name} (FULL content)")
        print(f"  ✅ SOURCE: {source_table} ({chunk_count} chunks)")
        print(f"  ✅ CONTENT: {content_stats['total_words']:.0f} words total")

        print(f"\nCOMPARISON:")
        print(f"  OLD: ~380 words from sample content")
        print(f"  NEW: {content_stats['total_words']:.0f} words from FULL documents")
        print(f"  IMPROVEMENT: {content_stats['total_words']/380:.0f}x more content")

        print(f"\nNEXT STEPS:")
        print("1. Wait 15-20 minutes for index building to complete")
        print("2. Test vector search with your business queries")
        print("3. Update AI Playground to use new full content index")
        print("4. Deploy production RAG with complete documents")

        if successful_queries > 0:
            print(f"\n✅ EARLY SUCCESS: {successful_queries}/{len(test_queries)} queries already working!")
        else:
            print(f"\n⏳ INDEX BUILDING: Wait for completion, then test queries")

        print(f"\nFULL CONTENT INDEX: {full_index_name}")

        return True

    except Exception as e:
        print(f"\nERROR: Vector search creation failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)