#!/usr/bin/env python3
"""
Debug script to check schema status in sandbox catalog
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json

def main():
    print("Debugging schema access in sandbox catalog...")

    try:
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

        print("Connected to Databricks cluster")

        # Check schemas in sandbox
        print("\n1. Checking existing schemas in sandbox:")
        try:
            schemas_df = spark.sql("SHOW SCHEMAS IN sandbox")
            schemas = schemas_df.collect()
            print(f"Found {len(schemas)} schemas in sandbox:")
            for schema in schemas:
                print(f"   - {schema['namespace']}")

            # Check if rdt_knowledge exists
            existing_schemas = [row['namespace'] for row in schemas]
            if 'sandbox.rdt_knowledge' in existing_schemas:
                print("\n✅ rdt_knowledge schema exists")
            else:
                print("\n❌ rdt_knowledge schema does not exist")
                print("Attempting to create...")

                try:
                    spark.sql("CREATE SCHEMA IF NOT EXISTS sandbox.rdt_knowledge")
                    print("✅ Successfully created rdt_knowledge schema")
                except Exception as e:
                    print(f"❌ Failed to create schema: {e}")

        except Exception as e:
            print(f"Error checking schemas: {e}")

        # Test basic operations
        print("\n2. Testing basic operations on sandbox.rdt_knowledge:")
        try:
            # Test table creation
            spark.sql("""
                CREATE TABLE IF NOT EXISTS sandbox.rdt_knowledge.test_table (
                    id STRING,
                    name STRING,
                    created_at TIMESTAMP DEFAULT current_timestamp()
                ) USING DELTA
            """)
            print("✅ Table creation successful")

            # Test insert
            spark.sql("""
                INSERT INTO sandbox.rdt_knowledge.test_table
                VALUES ('test_1', 'Test RAG Setup', current_timestamp())
            """)
            print("✅ Insert operation successful")

            # Test query
            result = spark.sql("SELECT * FROM sandbox.rdt_knowledge.test_table").collect()
            print(f"✅ Query successful - found {len(result)} records")

            # Show table content
            if result:
                print("Table content:")
                for row in result:
                    print(f"   ID: {row['id']}, Name: {row['name']}, Created: {row['created_at']}")

            # Clean up
            spark.sql("DROP TABLE IF EXISTS sandbox.rdt_knowledge.test_table")
            print("✅ Cleanup successful")

        except Exception as e:
            print(f"❌ Operations test failed: {e}")

        print("\n✅ Schema debugging completed successfully")

    except Exception as e:
        print(f"❌ Debug script failed: {e}")

    finally:
        try:
            if 'spark' in locals():
                spark.stop()
        except:
            pass

if __name__ == "__main__":
    main()