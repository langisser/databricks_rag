#!/usr/bin/env python3
"""
Phase 4: AI Playground Integration with Agent Bricks
- Create Knowledge Assistant agent using vector search index
- Configure AI Playground for chatbot interface
- Test end-to-end RAG functionality
- Implement agent evaluation and monitoring
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json
import time
import requests

def main():
    print("=" * 60)
    print("PHASE 4: AI PLAYGROUND INTEGRATION")
    print("=" * 60)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        rag_config = config['rag_config']
        namespace = rag_config['full_namespace']
        endpoint_name = rag_config['vector_search_endpoint']

        # Use the new full content index if available, fallback to original
        if 'full_content_index' in rag_config:
            index_name = rag_config['full_content_index']
            print("   Using NEW full content index with complete documents")
        else:
            index_name = f"{namespace}.rag_chunk_index"
            print("   Using original sample content index")

        print("1. Configuration loaded:")
        print(f"   Workspace: {config['databricks']['host']}")
        print(f"   Vector Search Index: {index_name}")
        print(f"   Vector Search Endpoint: {endpoint_name}")

        # Step 2: Verify prerequisites
        print(f"\n2. Verifying prerequisites...")

        # Create Spark session for data verification
        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()

        # Verify data exists - check both tables
        try:
            # Check for new full content table first
            if 'full_content_table' in rag_config:
                table_name = rag_config['full_content_table']
                chunk_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
                print(f"   FULL content chunks available: {chunk_count}")
                if chunk_count > 0:
                    word_stats = spark.sql(f"SELECT SUM(chunk_word_count) as total_words FROM {table_name}").collect()[0]
                    print(f"   Total words in full content: {word_stats['total_words']}")
            else:
                # Fallback to original table
                chunk_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.rag_document_chunks").collect()[0]['count']
                print(f"   Sample content chunks available: {chunk_count}")

            if chunk_count == 0:
                print("   ERROR: No document chunks found. Run document processing first.")
                return False
        except Exception as e:
            print(f"   WARNING: Could not verify chunk count: {e}")
            chunk_count = 1  # Continue anyway

        # Verify vector search is operational
        headers = {
            'Authorization': f'Bearer {config["databricks"]["token"]}',
            'Content-Type': 'application/json'
        }

        search_url = f'{config["databricks"]["host"]}/api/2.0/vector-search/indexes/{index_name}/query'
        test_search = {
            'query_text': 'test query',
            'columns': ['chunk_id'],
            'num_results': 1
        }

        response = requests.post(search_url, headers=headers, json=test_search)
        if response.status_code == 200:
            print("   SUCCESS: Vector search is operational")
        else:
            print(f"   ERROR: Vector search not working: {response.status_code}")
            return False

        spark.stop()

        # Step 3: Create Agent Configuration
        print(f"\n3. Creating AI Agent configuration...")

        agent_config = {
            "name": "RAG Knowledge Assistant",
            "description": "A knowledge assistant that answers questions using company documents and policies",
            "knowledge_sources": [
                {
                    "type": "vector_search_index",
                    "index_name": index_name,
                    "columns": ["chunk_text", "document_id"]
                }
            ],
            "instructions": """
You are a helpful AI assistant that answers questions based on company documents and policies.
Always provide accurate information and cite your sources. If you don't know something, say so clearly.
Keep responses concise but informative.
            """.strip(),
            "model_config": {
                "foundation_model": "databricks-dbrx-instruct",
                "temperature": 0.1,
                "max_tokens": 500
            }
        }

        print(f"   Agent Name: {agent_config['name']}")
        print(f"   Knowledge Source: {index_name}")
        print(f"   Model: {agent_config['model_config']['foundation_model']}")

        # Step 4: Agent Bricks API Integration (Manual for now)
        print(f"\n4. Agent Bricks integration setup...")
        print("   NOTE: Agent Bricks requires manual setup through Databricks UI")
        print("   Auto-configuration via API is not yet available")

        print("\n   Manual Steps Required:")
        print("   1. Go to AI Playground in Databricks workspace")
        print("   2. Create new Agent with Knowledge Assistant")
        print("   3. Configure with vector search index:")
        print(f"      - Index: {index_name}")
        print("   4. Set instructions for the agent")
        print("   5. Test with sample questions")

        # Step 5: Create test queries for evaluation - updated for business documents
        print(f"\n5. Creating evaluation test cases...")

        test_queries = [
            {
                "query": "What are the Bank of Thailand data submission requirements?",
                "expected_keywords": ["Bank of Thailand", "BOT", "data submission", "requirements"],
                "category": "Regulatory Compliance"
            },
            {
                "query": "How does counterparty risk management work?",
                "expected_keywords": ["counterparty", "risk management", "procedures"],
                "category": "Risk Management"
            },
            {
                "query": "What are the data validation rules and processes?",
                "expected_keywords": ["data validation", "rules", "processes", "validation"],
                "category": "Data Quality"
            },
            {
                "query": "What is the cross validation process for IWT and PROD environments?",
                "expected_keywords": ["cross validation", "IWT", "PROD", "environment"],
                "category": "Environment Management"
            },
            {
                "query": "What data entities and elements need to be submitted?",
                "expected_keywords": ["data entities", "elements", "submission"],
                "category": "Data Management"
            }
        ]

        print(f"   Created {len(test_queries)} evaluation test cases")
        for i, test in enumerate(test_queries, 1):
            print(f"     {i}. {test['category']}: {test['query'][:50]}...")

        # Step 6: Export agent configuration
        print(f"\n6. Exporting configuration for manual setup...")

        config_export = {
            "agent_config": agent_config,
            "test_queries": test_queries,
            "vector_search": {
                "endpoint": endpoint_name,
                "index": index_name,
                "namespace": namespace
            },
            "setup_instructions": [
                "1. Navigate to AI Playground in Databricks workspace",
                "2. Click 'Create Agent' or 'New Assistant'",
                "3. Choose 'Knowledge Assistant' template",
                f"4. Connect vector search index: {index_name}",
                "5. Configure agent instructions and model settings",
                "6. Test with provided evaluation queries",
                "7. Deploy agent endpoint for production use"
            ]
        }

        config_file = os.path.join(os.path.dirname(__file__), 'agent_config.json')
        with open(config_file, 'w') as f:
            json.dump(config_export, f, indent=2)

        print(f"   Configuration exported to: {config_file}")

        # Step 7: Vector search validation for agent setup
        print(f"\n7. Validating vector search for agent integration...")

        for i, test in enumerate(test_queries[:3], 1):  # Test first 3 queries
            try:
                search_data = {
                    'query_text': test['query'],
                    'columns': ['chunk_id', 'chunk_text'],
                    'num_results': 2
                }

                response = requests.post(search_url, headers=headers, json=search_data)
                if response.status_code == 200:
                    results = response.json()
                    data_array = results.get('result', {}).get('data_array', [])

                    print(f"   Test {i}: '{test['query'][:40]}...'")
                    if data_array:
                        best_result = data_array[0]
                        chunk_text = best_result[1] if len(best_result) > 1 else ''
                        score = best_result[2] if len(best_result) > 2 else 'N/A'

                        # Check if expected keywords are found
                        found_keywords = [kw for kw in test['expected_keywords']
                                        if kw.lower() in chunk_text.lower()]

                        print(f"     Found {len(data_array)} results, score: {score}")
                        print(f"     Keywords matched: {found_keywords}")
                        print(f"     SUCCESS: Vector search working for agent queries")
                    else:
                        print(f"     WARNING: No results found")
                else:
                    print(f"     ERROR: Search failed with status {response.status_code}")

            except Exception as e:
                print(f"   Test {i} failed: {e}")

        print(f"\n" + "=" * 60)
        print("PHASE 4 PREPARATION COMPLETED")
        print("=" * 60)
        print("STATUS:")
        print("  Vector Search: READY")
        print("  Agent Configuration: CREATED")
        print("  Test Cases: PREPARED")
        print("  Manual Setup: REQUIRED")

        print(f"\nNEXT STEPS:")
        print("1. Open Databricks AI Playground")
        print("2. Create Knowledge Assistant with Agent Bricks")
        print(f"3. Connect vector search index: {index_name}")
        print("4. Test with evaluation queries")
        print("5. Deploy agent endpoint")

        print(f"\nCONFIGURATION FILE: {config_file}")
        print("Use this file for agent setup parameters")

        return True

    except Exception as e:
        print(f"\nERROR: Phase 4 failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)