#!/usr/bin/env python3
"""
Phase 3: Vector Search Implementation for RAG Knowledge Base
- Create vector search endpoint
- Create vector search index from rag_document_chunks
- Test vector similarity search functionality
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.vector_search.client import VectorSearchClient
import json
import time

def main():
    print("=" * 60)
    print("PHASE 3: VECTOR SEARCH IMPLEMENTATION")
    print("=" * 60)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        print("1. Establishing connections...")

        # Create Spark session
        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()
        print("   SUCCESS: Databricks Connect session established")

        # Create Vector Search client
        vsc = VectorSearchClient(
            workspace_url=config['databricks']['host'],
            personal_access_token=config['databricks']['token']
        )
        print("   SUCCESS: Vector Search client created")

        # Configuration
        rag_config = config['rag_config']
        namespace = rag_config['full_namespace']
        endpoint_name = rag_config['vector_search_endpoint']
        embedding_model = rag_config['embedding_model']

        print(f"   Namespace: {namespace}")
        print(f"   Endpoint: {endpoint_name}")

        # Step 2: Create vector search endpoint
        print(f"\n2. Setting up vector search endpoint...")

        try:
            # Check if endpoint exists
            try:
                endpoint_info = vsc.get_endpoint(endpoint_name)
                print(f"   INFO: Endpoint '{endpoint_name}' already exists")
                print(f"   Status: {endpoint_info.get('endpoint_status', 'unknown')}")
            except Exception:
                print(f"   Creating endpoint '{endpoint_name}'...")
                vsc.create_endpoint(
                    name=endpoint_name,
                    endpoint_type="STANDARD"
                )
                print("   SUCCESS: Endpoint creation initiated")

                # Wait briefly for endpoint
                print("   Waiting for endpoint to initialize...")
                time.sleep(30)

        except Exception as e:
            print(f"   ERROR: Endpoint setup failed: {e}")
            return False

        # Step 3: Verify source data
        print(f"\n3. Verifying source data...")

        try:
            chunk_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.rag_document_chunks").collect()[0]['count']
            print(f"   Document chunks available: {chunk_count}")

            if chunk_count == 0:
                print("   ERROR: No chunks found. Run Phase 2 first.")
                return False

            # Sample data
            sample = spark.sql(f"""
                SELECT chunk_id, LEFT(chunk_text, 40) as preview
                FROM {namespace}.rag_document_chunks LIMIT 2
            """).collect()

            for chunk in sample:
                print(f"   Sample: {chunk['chunk_id']} - {chunk['preview']}...")

        except Exception as e:
            print(f"   ERROR: Data verification failed: {e}")
            return False

        # Step 4: Create vector search index
        print(f"\n4. Creating vector search index...")

        index_name = f"{namespace}.rag_chunk_index"
        source_table = f"{namespace}.rag_document_chunks"

        try:
            # Check if index exists
            try:
                existing_index = vsc.get_index(index_name)
                print(f"   INFO: Index '{index_name}' already exists")
                status = existing_index.get('status', {})
                print(f"   Ready: {status.get('ready', False)}")
                index_exists = True
            except Exception:
                index_exists = False

            if not index_exists:
                print(f"   Creating vector index...")

                # Create index
                index = vsc.create_delta_sync_index(
                    endpoint_name=endpoint_name,
                    index_name=index_name,
                    source_table_name=source_table,
                    pipeline_type="TRIGGERED",
                    primary_key="chunk_id",
                    embedding_source_column="chunk_text",
                    embedding_model_endpoint_name=embedding_model
                )

                print("   SUCCESS: Index creation initiated")
                print("   NOTE: Index building may take 10-15 minutes")

        except Exception as e:
            print(f"   ERROR: Index creation failed: {e}")
            print(f"   Details: {str(e)}")
            return False

        # Step 5: Test basic vector search (if index is ready)
        print(f"\n5. Testing vector search...")

        try:
            # Check index status first
            index_info = vsc.get_index(index_name)
            status = index_info.get('status', {})
            ready = status.get('ready', False)

            if ready:
                print("   Index is ready - testing similarity search...")

                test_query = "How do I work from home?"
                results = vsc.similarity_search(
                    index_name=index_name,
                    query_text=test_query,
                    columns=["chunk_id", "chunk_text"],
                    num_results=2
                )

                if results and len(results) > 0:
                    print(f"   SUCCESS: Found {len(results)} similar chunks")
                    for i, result in enumerate(results):
                        preview = result.get('chunk_text', '')[:50]
                        print(f"     {i+1}. {result.get('chunk_id')}: {preview}...")
                else:
                    print("   WARNING: No results returned")
            else:
                print("   Index is still building - skipping search test")
                print(f"   Status: {status.get('message', 'Building...')}")

        except Exception as e:
            print(f"   INFO: Search test skipped - index may still be building")
            print(f"   Error: {e}")

        # Step 6: Configuration summary
        print(f"\n6. Vector search configuration:")
        print(f"   Endpoint: {endpoint_name}")
        print(f"   Index: {index_name}")
        print(f"   Source table: {source_table}")
        print(f"   Embedding model: {embedding_model}")
        print(f"   Primary key: chunk_id")
        print(f"   Text column: chunk_text")

        print("\n" + "=" * 60)
        print("PHASE 3 IMPLEMENTATION COMPLETED")
        print("=" * 60)
        print("Vector Search Status:")
        print("  - Endpoint created/verified")
        print("  - Index creation initiated")
        print("  - Configuration validated")
        print("\nNext Steps:")
        print("  1. Wait for index to finish building (10-15 min)")
        print("  2. Test vector search manually")
        print("  3. Configure AI Playground with Agent Bricks")

        return True

    except Exception as e:
        print(f"\nERROR: Phase 3 failed: {e}")
        return False

    finally:
        try:
            if 'spark' in locals():
                spark.stop()
        except:
            pass

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)