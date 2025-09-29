#!/usr/bin/env python3
"""
Phase 1: Environment Setup for RAG Knowledge Base
- Verify Databricks workspace configuration
- Check Unity Catalog status
- Create schema structure in sandbox.rdt_knowledge
- Validate permissions and access
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json

def main():
    print("=" * 60)
    print("PHASE 1: RAG ENVIRONMENT SETUP")
    print("=" * 60)

    try:
        # Step 1: Create Databricks Connect session
        print("1. Establishing Databricks Connect session...")

        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Create session
        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()

        print("   SUCCESS: Connected to Databricks cluster")
        print(f"   Host: {config['databricks']['host']}")
        print(f"   Cluster: {config['databricks']['cluster_id']}")

        # Extract RAG configuration
        rag_config = config['rag_config']
        catalog = rag_config['catalog']
        schema = rag_config['schema']
        full_namespace = rag_config['full_namespace']

        print(f"   Using namespace: {full_namespace}")

        # Step 2: Verify Unity Catalog and sandbox access
        print("\n2. Verifying Unity Catalog and sandbox access...")

        try:
            catalogs_df = spark.sql("SHOW CATALOGS")
            catalogs = [row['catalog'] for row in catalogs_df.collect()]
            print(f"   SUCCESS: Unity Catalog enabled")
            print(f"   Available catalogs: {len(catalogs)} found")

            # Check sandbox catalog access
            if catalog in catalogs:
                print(f"   SUCCESS: Access to '{catalog}' catalog confirmed")
            else:
                print(f"   ERROR: '{catalog}' catalog not accessible")
                return False

        except Exception as e:
            print(f"   ERROR: Unity Catalog check failed: {e}")
            return False

        # Step 3: Verify/Create schema structure
        print(f"\n3. Setting up schema structure in {full_namespace}...")

        try:
            # Check if schema exists
            schemas_df = spark.sql(f"SHOW SCHEMAS IN {catalog}")
            existing_schemas = [row['namespace'] for row in schemas_df.collect()]
            schema_full_name = f"{catalog}.{schema}"

            if schema_full_name in existing_schemas:
                print(f"   SUCCESS: Schema '{schema}' already exists")
            else:
                # Try to create schema
                try:
                    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_namespace}")
                    print(f"   SUCCESS: Created schema '{schema}'")
                except Exception as e:
                    print(f"   ERROR: Failed to create schema: {e}")
                    print(f"   NOTE: You may need to manually create schema '{schema}' in catalog '{catalog}'")
                    return False

        except Exception as e:
            print(f"   ERROR: Schema setup failed: {e}")
            return False

        # Step 4: Test permissions on the schema
        print(f"\n4. Testing permissions on {full_namespace}...")

        try:
            # Test table creation permission
            test_table = f"{full_namespace}.permission_test"
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {test_table} (
                    test_id STRING,
                    test_timestamp TIMESTAMP DEFAULT current_timestamp(),
                    test_data STRING
                ) USING DELTA
            """)
            print("   SUCCESS: Table creation permission verified")

            # Test insert permission
            spark.sql(f"""
                INSERT INTO {test_table}
                VALUES ('test_001', current_timestamp(), 'RAG setup test')
            """)
            print("   SUCCESS: Insert permission verified")

            # Test query permission
            result = spark.sql(f"SELECT COUNT(*) as count FROM {test_table}").collect()
            count = result[0]['count']
            print(f"   SUCCESS: Query permission verified (found {count} test records)")

            # Test update permission
            spark.sql(f"""
                UPDATE {test_table}
                SET test_data = 'RAG setup test - updated'
                WHERE test_id = 'test_001'
            """)
            print("   SUCCESS: Update permission verified")

            # Clean up test table
            spark.sql(f"DROP TABLE IF EXISTS {test_table}")
            print("   SUCCESS: Cleanup completed")

        except Exception as e:
            print(f"   ERROR: Permission test failed: {e}")
            print("   NOTE: You may need additional permissions on the schema")
            return False

        # Step 5: Check current user and workspace info
        print("\n5. Gathering workspace information...")

        try:
            result = spark.sql("""
                SELECT
                    current_timestamp() as current_time,
                    current_user() as current_user,
                    version() as spark_version
            """).collect()

            row = result[0]
            print("   SUCCESS: Workspace information gathered")
            print(f"   Current time: {row['current_time']}")
            print(f"   Current user: {row['current_user']}")
            print(f"   Spark version: {row['spark_version']}")

        except Exception as e:
            print(f"   WARNING: Workspace info check failed: {e}")

        # Step 6: Validate configuration for RAG implementation
        print("\n6. Validating RAG configuration...")

        try:
            # Test that we can use the namespace for RAG tables
            print(f"   Namespace: {full_namespace}")
            print(f"   Vector Search Endpoint: {rag_config['vector_search_endpoint']}")
            print(f"   Embedding Model: {rag_config['embedding_model']}")

            # Verify we can list tables in the schema
            tables_df = spark.sql(f"SHOW TABLES IN {full_namespace}")
            table_count = len(tables_df.collect())
            print(f"   Current tables in schema: {table_count}")

            print("   SUCCESS: RAG configuration validated")

        except Exception as e:
            print(f"   ERROR: RAG configuration validation failed: {e}")
            return False

        print("\n" + "=" * 60)
        print("PHASE 1 COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("✅ Databricks Connect session established")
        print("✅ Unity Catalog and sandbox access verified")
        print(f"✅ Schema '{schema}' ready in catalog '{catalog}'")
        print("✅ Full namespace configured: " + full_namespace)
        print("✅ Permissions validated for RAG operations")
        print("✅ Environment ready for Phase 2")
        print("\nNext: Run Phase 2 to create RAG data tables")

        return True

    except Exception as e:
        print(f"\nERROR: Phase 1 failed with exception: {e}")
        print("Please check your configuration and permissions")
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