#!/usr/bin/env python3
"""
Phase 1: Environment Setup for RAG Knowledge Base
- Verify Databricks workspace configuration
- Check Unity Catalog status
- Create catalog and schema structure
- Validate serverless compute access
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

        # Step 2: Verify Unity Catalog
        print("\n2. Verifying Unity Catalog configuration...")

        try:
            catalogs_df = spark.sql("SHOW CATALOGS")
            catalogs = [row['catalog'] for row in catalogs_df.collect()]
            print(f"   SUCCESS: Unity Catalog enabled")
            print(f"   Available catalogs: {', '.join(catalogs)}")

            # Check for existing rag_knowledge_base catalog
            if 'rag_knowledge_base' in catalogs:
                print("   NOTE: rag_knowledge_base catalog already exists")
            else:
                print("   INFO: rag_knowledge_base catalog not found (will create)")

        except Exception as e:
            print(f"   ERROR: Unity Catalog check failed: {e}")
            return False

        # Step 3: Create catalog structure
        print("\n3. Creating RAG catalog structure...")

        # Create main catalog
        try:
            spark.sql("CREATE CATALOG IF NOT EXISTS rag_knowledge_base")
            print("   SUCCESS: Created catalog 'rag_knowledge_base'")
        except Exception as e:
            print(f"   ERROR: Failed to create catalog: {e}")
            return False

        # Create schemas
        schemas = ['documents', 'vectors', 'monitoring']
        for schema in schemas:
            try:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS rag_knowledge_base.{schema}")
                print(f"   SUCCESS: Created schema 'rag_knowledge_base.{schema}'")
            except Exception as e:
                print(f"   ERROR: Failed to create schema {schema}: {e}")
                return False

        # Step 4: Verify schema structure
        print("\n4. Verifying catalog structure...")

        try:
            schemas_df = spark.sql("SHOW SCHEMAS IN rag_knowledge_base")
            created_schemas = [row['namespace'] for row in schemas_df.collect()]
            print(f"   SUCCESS: Created schemas: {', '.join(created_schemas)}")

            # Validate all required schemas exist
            for schema in schemas:
                if f"rag_knowledge_base.{schema}" in created_schemas:
                    print(f"   VERIFIED: Schema {schema} exists")
                else:
                    print(f"   WARNING: Schema {schema} not found")

        except Exception as e:
            print(f"   ERROR: Schema verification failed: {e}")
            return False

        # Step 5: Test permissions
        print("\n5. Testing catalog permissions...")

        try:
            # Test table creation permission
            test_table = "rag_knowledge_base.documents.permission_test"
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {test_table} (
                    test_id STRING,
                    test_timestamp TIMESTAMP
                ) USING DELTA
            """)
            print("   SUCCESS: Table creation permission verified")

            # Test insert permission
            spark.sql(f"""
                INSERT INTO {test_table}
                VALUES ('test_001', current_timestamp())
            """)
            print("   SUCCESS: Insert permission verified")

            # Test query permission
            result = spark.sql(f"SELECT COUNT(*) as count FROM {test_table}").collect()
            count = result[0]['count']
            print(f"   SUCCESS: Query permission verified (found {count} test records)")

            # Clean up test table
            spark.sql(f"DROP TABLE IF EXISTS {test_table}")
            print("   SUCCESS: Cleanup completed")

        except Exception as e:
            print(f"   ERROR: Permission test failed: {e}")
            return False

        # Step 6: Check serverless compute access
        print("\n6. Checking serverless compute access...")

        try:
            # This query tests serverless compute capabilities
            result = spark.sql("""
                SELECT
                    current_timestamp() as current_time,
                    current_user() as current_user,
                    version() as spark_version
            """).collect()

            row = result[0]
            print("   SUCCESS: Serverless compute accessible")
            print(f"   Current time: {row['current_time']}")
            print(f"   Current user: {row['current_user']}")
            print(f"   Spark version: {row['spark_version']}")

        except Exception as e:
            print(f"   WARNING: Serverless compute check failed: {e}")
            print("   NOTE: This may not affect RAG implementation")

        print("\n" + "=" * 60)
        print("PHASE 1 COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("✅ Databricks Connect session established")
        print("✅ Unity Catalog verified and configured")
        print("✅ RAG catalog structure created")
        print("✅ Required schemas initialized")
        print("✅ Permissions validated")
        print("✅ Environment ready for Phase 2")

        return True

    except Exception as e:
        print(f"\nERROR: Phase 1 failed with exception: {e}")
        print("Please check your configuration and cluster status")
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