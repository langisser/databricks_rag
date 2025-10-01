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
import requests

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
            endpoint_status = endpoint_info.get('endpoint_status', {}).get('state', 'unknown')
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

        # Step 3: Check index status via list
        print(f"\n3. Checking index status...")
        try:
            indexes = vsc.list_indexes(endpoint_name)
            index_found = False
            for idx in indexes.get('vector_indexes', []):
                if idx.get('name') == index_name:
                    index_found = True
                    print(f"   Index found: {idx.get('name')}")
                    print(f"   Index type: {idx.get('index_type', 'N/A')}")
                    print(f"   Primary key: {idx.get('primary_key', 'N/A')}")
                    break

            if index_found:
                print("   SUCCESS: Index is available")
            else:
                print(f"   ERROR: Index {index_name} not found")
                return False
        except Exception as e:
            print(f"   ERROR: Failed to check index: {e}")
            return False

        # Step 4: Test similarity search via REST API
        print(f"\n4. Testing similarity search...")

        test_queries = [
            "working from home policy",
            "password reset procedure",
            "Databricks setup requirements"
        ]

        all_tests_passed = True

        # Setup REST API headers
        headers = {
            'Authorization': f'Bearer {config["databricks"]["token"]}',
            'Content-Type': 'application/json'
        }

        search_url = f'{config["databricks"]["host"]}/api/2.0/vector-search/indexes/{index_name}/query'

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

                    if data_array and len(data_array) > 0:
                        print(f"   SUCCESS: Found {len(data_array)} similar chunks")
                        for j, row in enumerate(data_array):
                            chunk_id = row[0] if len(row) > 0 else 'N/A'
                            chunk_text = row[1] if len(row) > 1 else ''
                            score = row[2] if len(row) > 2 else 'N/A'
                            chunk_preview = chunk_text[:50] if chunk_text else ''
                            print(f"     {j+1}. {chunk_id}: {chunk_preview}... (score: {score})")
                    else:
                        print(f"   WARNING: No results for query '{query}'")
                        all_tests_passed = False
                else:
                    print(f"   ERROR: API request failed with status {response.status_code}")
                    print(f"   Response: {response.text}")
                    all_tests_passed = False

            except Exception as e:
                print(f"   ERROR: Search failed for '{query}': {e}")
                all_tests_passed = False

        # Step 5: Integration readiness check
        print(f"\n5. Integration readiness check...")

        try:
            # Check endpoint status
            endpoint_info = vsc.get_endpoint(endpoint_name)
            endpoint_status = endpoint_info.get('endpoint_status', {}).get('state', 'unknown')
            print(f"   Endpoint status: {endpoint_status}")

            # Check index via list instead of get_index
            indexes = vsc.list_indexes(endpoint_name)
            index_found = False
            for idx in indexes.get('vector_indexes', []):
                if idx.get('name') == index_name:
                    index_found = True
                    print(f"   Index found: {idx.get('name')}")
                    print(f"   Primary key: {idx.get('primary_key', 'N/A')}")
                    print(f"   Index type: {idx.get('index_type', 'N/A')}")
                    break

            if not index_found:
                print(f"   ERROR: Index {index_name} not found")
                all_tests_passed = False

            # Test basic search functionality
            test_search_data = {
                'query_text': 'test',
                'columns': ['chunk_id'],
                'num_results': 1
            }

            test_response = requests.post(search_url, headers=headers, json=test_search_data)
            if test_response.status_code == 200:
                print("   SUCCESS: Vector search API is operational")
            else:
                print(f"   WARNING: Vector search API test failed: {test_response.status_code}")
                all_tests_passed = False

        except Exception as e:
            print(f"   ERROR: Integration check failed: {e}")
            all_tests_passed = False

        # Summary
        print(f"\n" + "=" * 50)
        print("PHASE 3 VALIDATION SUMMARY")
        print("=" * 50)

        if all_tests_passed:
            print("SUCCESS: Vector Search Implementation: READY")
            print("SUCCESS: Endpoint Status: ONLINE")
            print("SUCCESS: Index Status: READY")
            print("SUCCESS: Similarity Search: WORKING")
            print("SUCCESS: Integration: READY")

            print(f"\nNext Steps:")
            print("1. Configure AI Playground with Agent Bricks")
            print("2. Test end-to-end RAG functionality")
            print("3. Create knowledge base queries")

            return True
        else:
            print("ERROR: Some validations failed")
            print("Please wait for components to be ready and retry")
            return False

    except Exception as e:
        print(f"ERROR: Phase 3 validation failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)