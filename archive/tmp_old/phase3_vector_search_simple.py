#!/usr/bin/env python3
"""
Phase 3: Vector Search Implementation (Simplified)
- Create vector search endpoint
- Create vector search index
- Test basic functionality
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from databricks.vector_search.client import VectorSearchClient
import json
import time

def main():
    print("Phase 3: Vector Search Implementation")
    print("=" * 50)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        rag_config = config['rag_config']
        namespace = rag_config['full_namespace']
        endpoint_name = rag_config['vector_search_endpoint']
        embedding_model = rag_config['embedding_model']

        print(f"Configuration loaded:")
        print(f"  Namespace: {namespace}")
        print(f"  Endpoint: {endpoint_name}")
        print(f"  Embedding model: {embedding_model}")

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

        # Step 2: Create/verify vector search endpoint
        print(f"\n2. Setting up vector search endpoint...")
        try:
            # Check if endpoint exists
            try:
                endpoint_info = vsc.get_endpoint(endpoint_name)
                print(f"   INFO: Endpoint '{endpoint_name}' already exists")
                print(f"   Status: {endpoint_info.get('endpoint_status', 'unknown')}")
                endpoint_exists = True
            except Exception:
                endpoint_exists = False

            if not endpoint_exists:
                print(f"   Creating endpoint '{endpoint_name}'...")
                vsc.create_endpoint(
                    name=endpoint_name,
                    endpoint_type="STANDARD"
                )
                print("   SUCCESS: Endpoint creation initiated")
                print("   NOTE: Endpoint provisioning takes 5-10 minutes")

        except Exception as e:
            print(f"   ERROR: Endpoint operation failed: {e}")
            return False

        # Step 3: Verify data (simplified check)
        print(f"\n3. Verifying data setup...")
        try:
            # Create fresh session for data check
            spark = DatabricksSession.builder.remote(
                host=config['databricks']['host'],
                token=config['databricks']['token'],
                cluster_id=config['databricks']['cluster_id']
            ).getOrCreate()

            # Quick data check
            chunk_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.rag_document_chunks").collect()[0]['count']
            print(f"   Document chunks available: {chunk_count}")

            if chunk_count == 0:
                print("   ERROR: No chunks found. Run Phase 2 first.")
                spark.stop()
                return False

            spark.stop()
            print("   SUCCESS: Data verification completed")

        except Exception as e:
            print(f"   WARNING: Data verification failed: {e}")
            print("   Continuing with vector search setup...")

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
                print(f"   Message: {status.get('message', 'N/A')}")
                index_exists = True
            except Exception:
                index_exists = False

            if not index_exists:
                print(f"   Creating vector index...")
                print(f"   Source table: {source_table}")
                print(f"   Primary key: chunk_id")
                print(f"   Text column: chunk_text")

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

                print("   SUCCESS: Vector index creation initiated")
                print("   NOTE: Index building takes 10-15 minutes")

        except Exception as e:
            print(f"   ERROR: Index creation failed: {e}")
            print(f"   This may be due to:")
            print(f"   - Endpoint not yet ready")
            print(f"   - Permissions issues")
            print(f"   - Table access problems")
            return False

        # Step 5: Summary and next steps
        print(f"\n5. Vector Search Setup Summary:")
        print(f"   Endpoint: {endpoint_name}")
        print(f"   Index: {index_name}")
        print(f"   Source: {source_table}")
        print(f"   Embedding model: {embedding_model}")

        print(f"\nNext Steps:")
        print(f"1. Wait for endpoint to be ONLINE (check Databricks UI)")
        print(f"2. Wait for index to be ready (10-15 minutes)")
        print(f"3. Test vector search with sample queries")
        print(f"4. Configure AI Playground with Agent Bricks")

        print(f"\nPhase 3 Vector Search setup completed!")
        return True

    except Exception as e:
        print(f"ERROR: Phase 3 failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)