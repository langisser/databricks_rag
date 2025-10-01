#!/usr/bin/env python3
"""
Phase 3 Validation: Vector Search Testing and Verification
- Check vector search endpoint status
- Verify vector search index is ready
- Test similarity search functionality
- Validate integration readiness
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.vector_search.client import VectorSearchClient
import json
import time

def main():
    print("Phase 3 Validation: Vector Search Testing")
    print("=" * 50)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        rag_config = config['rag_config']
        namespace = rag_config['full_namespace']
        endpoint_name = rag_config['vector_search_endpoint']
        index_name = f"{namespace}.rag_chunk_index"

        print(f"Configuration:")
        print(f"  Endpoint: {endpoint_name}")
        print(f"  Index: {index_name}")

        # Step 1: Create Vector Search client
        print(f"\n1. Creating Vector Search client...")
        try:
            vsc = VectorSearchClient(
                workspace_url=config['databricks']['host'],
                personal_access_token=config['databricks']['token'],
                disable_notice=True
            )
            print("   SUCCESS: Vector Search client created")
        except Exception as e:
            print(f"   ERROR: Vector Search client failed: {e}")
            return False

        # Step 2: Check endpoint status
        print(f"\n2. Checking endpoint status...")
        try:
            endpoint_info = vsc.get_endpoint(endpoint_name)
            endpoint_status = endpoint_info.get('state', 'unknown')
            print(f"   Endpoint status: {endpoint_status}")

            if endpoint_status != 'ONLINE':
                print(f"   WARNING: Endpoint not yet online")
                print(f"   Status details: {endpoint_info}")
                return False
            else:
                print("   SUCCESS: Endpoint is online")
        except Exception as e:
            print(f"   ERROR: Failed to get endpoint status: {e}")
            return False

        # Step 3: Check index status
        print(f"\n3. Checking index status...")
        try:
            index_info = vsc.get_index(index_name)
            status = index_info.get('status', {})
            ready = status.get('ready', False)
            message = status.get('message', 'Unknown')

            print(f"   Index ready: {ready}")
            print(f"   Status message: {message}")

            if not ready:
                print(f"   WARNING: Index not yet ready")
                print(f"   Full status: {status}")
                return False
            else:
                print("   SUCCESS: Index is ready")
        except Exception as e:
            print(f"   ERROR: Failed to get index status: {e}")
            return False

        # Step 4: Test similarity search
        print(f"\n4. Testing similarity search...")

        test_queries = [
            "working from home policy",
            "password reset procedure",
            "Databricks setup requirements"
        ]

        all_tests_passed = True

        for i, query in enumerate(test_queries, 1):
            try:
                print(f"\n   Test {i}: '{query}'")
                results = vsc.similarity_search(
                    index_name=index_name,
                    query_text=query,
                    columns=["chunk_id", "chunk_text", "document_id"],
                    num_results=2
                )

                if results and len(results) > 0:
                    print(f"   SUCCESS: Found {len(results)} similar chunks")
                    for j, result in enumerate(results):
                        chunk_id = result.get('chunk_id', 'N/A')
                        chunk_preview = result.get('chunk_text', '')[:50]
                        print(f"     {j+1}. {chunk_id}: {chunk_preview}...")
                else:
                    print(f"   WARNING: No results for query '{query}'")
                    all_tests_passed = False

            except Exception as e:
                print(f"   ERROR: Search failed for '{query}': {e}")
                all_tests_passed = False

        # Step 5: Integration readiness check
        print(f"\n5. Integration readiness check...")

        try:
            # Get detailed index information
            index_info = vsc.get_index(index_name)
            spec = index_info.get('spec', {})

            print(f"   Source table: {spec.get('source_table', 'N/A')}")
            print(f"   Primary key: {spec.get('primary_key', 'N/A')}")
            print(f"   Embedding column: {spec.get('embedding_source_column', 'N/A')}")
            print(f"   Embedding model: {spec.get('embedding_model_endpoint_name', 'N/A')}")

            # Check if we can perform a basic search
            basic_search = vsc.similarity_search(
                index_name=index_name,
                query_text="test",
                columns=["chunk_id"],
                num_results=1
            )

            if basic_search is not None:
                print("   SUCCESS: Vector search is operational")
            else:
                print("   WARNING: Vector search returned None")
                all_tests_passed = False

        except Exception as e:
            print(f"   ERROR: Integration check failed: {e}")
            all_tests_passed = False

        # Summary
        print(f"\n" + "=" * 50)
        print("PHASE 3 VALIDATION SUMMARY")
        print("=" * 50)

        if all_tests_passed:
            print("✅ Vector Search Implementation: READY")
            print("✅ Endpoint Status: ONLINE")
            print("✅ Index Status: READY")
            print("✅ Similarity Search: WORKING")
            print("✅ Integration: READY")

            print(f"\nNext Steps:")
            print("1. Configure AI Playground with Agent Bricks")
            print("2. Test end-to-end RAG functionality")
            print("3. Create knowledge base queries")

            return True
        else:
            print("❌ Some validations failed")
            print("Please wait for components to be ready and retry")
            return False

    except Exception as e:
        print(f"ERROR: Phase 3 validation failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)