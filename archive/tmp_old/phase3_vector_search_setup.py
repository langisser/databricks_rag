#!/usr/bin/env python3
"""
Phase 3: Vector Search Implementation for RAG Knowledge Base
- Create vector search endpoint
- Create vector search index from rag_document_chunks
- Test vector similarity search functionality
- Validate vector search integration
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
        # Step 1: Create Databricks Connect session
        print("1. Establishing connections...")

        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

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

        # Extract configuration
        rag_config = config['rag_config']
        namespace = rag_config['full_namespace']
        endpoint_name = rag_config['vector_search_endpoint']
        embedding_model = rag_config['embedding_model']

        print(f"   Using namespace: {namespace}")
        print(f"   Endpoint name: {endpoint_name}")
        print(f"   Embedding model: {embedding_model}")

        # Step 2: Check and create vector search endpoint
        print(f"\n2. Setting up vector search endpoint '{endpoint_name}'...")

        try:
            # Check if endpoint exists
            existing_endpoints = vsc.list_endpoints()
            endpoint_exists = any(ep['name'] == endpoint_name for ep in existing_endpoints)

            if endpoint_exists:
                print(f"   INFO: Endpoint '{endpoint_name}' already exists")
                endpoint_status = vsc.get_endpoint(endpoint_name)
                print(f"   Status: {endpoint_status.get('endpoint_status', 'unknown')}")
            else:
                print(f"   Creating new endpoint '{endpoint_name}'...")
                vsc.create_endpoint(
                    name=endpoint_name,
                    endpoint_type="STANDARD"
                )
                print(f"   SUCCESS: Endpoint '{endpoint_name}' created")

                # Wait for endpoint to be ready
                print("   Waiting for endpoint to be online...")
                max_wait_time = 600  # 10 minutes
                wait_time = 0
                while wait_time < max_wait_time:
                    try:
                        endpoint_status = vsc.get_endpoint(endpoint_name)
                        status = endpoint_status.get('endpoint_status', 'unknown')
                        if status == 'ONLINE':
                            print("   SUCCESS: Endpoint is online")
                            break
                        else:
                            print(f"   Waiting... Status: {status}")
                            time.sleep(30)
                            wait_time += 30
                    except Exception as e:
                        print(f"   Checking status... ({wait_time}s)")
                        time.sleep(30)
                        wait_time += 30

                if wait_time >= max_wait_time:
                    print("   WARNING: Endpoint creation taking longer than expected")
                    print("   You may need to check endpoint status manually")

        except Exception as e:
            print(f"   ERROR: Endpoint setup failed: {e}")
            return False

        # Step 3: Verify data exists for index creation
        print(f"\n3. Verifying source data in {namespace}.rag_document_chunks...")

        try:
            # Check chunks table
            chunk_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.rag_document_chunks").collect()[0]['count']
            print(f"   Document chunks available: {chunk_count}")

            if chunk_count == 0:
                print("   ERROR: No document chunks found. Run Phase 2 first.")
                return False

            # Show sample data
            sample_chunks = spark.sql(f"""
                SELECT chunk_id, document_id, LEFT(chunk_text, 50) as preview
                FROM {namespace}.rag_document_chunks
                LIMIT 3
            """).collect()

            print("   Sample chunks:")
            for chunk in sample_chunks:
                print(f"     {chunk['chunk_id']}: {chunk['preview']}...")

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
                print(f"   Status: {existing_index.get('status', {}).get('ready', 'unknown')}")
                index_exists = True
            except Exception:
                index_exists = False

            if not index_exists:
                print(f"   Creating vector index '{index_name}'...")

                # Create delta sync index
                index = vsc.create_delta_sync_index(
                    endpoint_name=endpoint_name,
                    index_name=index_name,
                    source_table_name=source_table,
                    pipeline_type="TRIGGERED",
                    primary_key="chunk_id",
                    embedding_source_column="chunk_text",
                    embedding_model_endpoint_name=embedding_model
                )

                print("   SUCCESS: Vector index creation initiated")

                # Wait for index to be ready
                print("   Waiting for index to be ready...")
                max_wait_time = 900  # 15 minutes
                wait_time = 0
                while wait_time < max_wait_time:
                    try:
                        index_status = vsc.get_index(index_name)
                        status = index_status.get('status', {})
                        ready = status.get('ready', False)
                        message = status.get('message', 'Processing...')

                        if ready:
                            print("   SUCCESS: Index is ready")
                            break
                        else:
                            print(f"   Building index... {message} ({wait_time}s)")
                            time.sleep(60)
                            wait_time += 60
                    except Exception as e:
                        print(f"   Checking index status... ({wait_time}s)")
                        time.sleep(60)
                        wait_time += 60

                if wait_time >= max_wait_time:
                    print("   WARNING: Index creation taking longer than expected")
                    print("   Index may still be building. Check status manually.")

        except Exception as e:
            print(f"   ERROR: Index creation failed: {e}")
            print("   This may be due to permissions or endpoint not being ready")
            return False

        # Step 5: Test vector search functionality
        print(f"\n5. Testing vector search functionality...")

        try:
            # Test similarity search
            test_queries = [
                "How do I work remotely?",
                "Password reset instructions",
                "Databricks setup requirements"
            ]

            for query in test_queries:
                try:
                    print(f"\n   Testing query: '{query}'")
                    results = vsc.similarity_search(
                        index_name=index_name,
                        query_text=query,
                        columns=["chunk_id", "chunk_text", "document_id"],
                        num_results=2
                    )

                    if results and len(results) > 0:
                        print(f"   SUCCESS: Found {len(results)} similar chunks")
                        for i, result in enumerate(results[:2]):
                            chunk_preview = result.get('chunk_text', '')[:60]
                            print(f"     {i+1}. {result.get('chunk_id', 'N/A')}: {chunk_preview}...")
                    else:
                        print("   WARNING: No results returned")

                except Exception as e:
                    print(f"   ERROR testing query '{query}': {e}")

        except Exception as e:
            print(f"   ERROR: Vector search testing failed: {e}")
            print("   Index may still be building. Test manually later.")

        # Step 6: Validate vector search setup
        print(f"\n6. Validating vector search setup...")

        try:
            # Check endpoint status
            endpoint_info = vsc.get_endpoint(endpoint_name)
            print(f"   Endpoint status: {endpoint_info.get('endpoint_status', 'unknown')}")

            # Check index status
            index_info = vsc.get_index(index_name)
            index_status = index_info.get('status', {})
            print(f"   Index ready: {index_status.get('ready', False)}")
            print(f"   Index message: {index_status.get('message', 'N/A')}")

            # Show index configuration
            spec = index_info.get('spec', {})
            print(f"   Source table: {spec.get('source_table', 'N/A')}")
            print(f"   Embedding model: {spec.get('embedding_source_column', 'N/A')}")

        except Exception as e:
            print(f"   ERROR: Validation failed: {e}")

        print("\n" + "=" * 60)
        print("PHASE 3 COMPLETED")
        print("=" * 60)
        print("Vector Search Implementation Status:")
        print(f"  Endpoint: {endpoint_name}")
        print(f"  Index: {index_name}")
        print(f"  Source: {source_table}")
        print(f"  Model: {embedding_model}")
        print("\nNext: Configure AI Playground with Agent Bricks")

        return True

    except Exception as e:
        print(f"\nERROR: Phase 3 failed with exception: {e}")
        return False

    finally:
        try:
            if 'spark' in locals():
                spark.stop()
                print("\nDatabricks session closed")
        except:
            pass

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)