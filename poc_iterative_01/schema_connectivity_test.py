#!/usr/bin/env python3
"""
Simple schema test for sandbox.rdt_knowledge
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json

def main():
    print("Testing sandbox.rdt_knowledge schema access...")

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

        print("Connected successfully")

        # Test 1: Check if we can create the schema
        print("\n1. Creating schema if not exists...")
        try:
            spark.sql("CREATE SCHEMA IF NOT EXISTS sandbox.rdt_knowledge")
            print("SUCCESS: Schema created or already exists")
        except Exception as e:
            print(f"ERROR creating schema: {e}")
            return False

        # Test 2: Simple table creation without DEFAULT values
        print("\n2. Testing table creation...")
        try:
            spark.sql("""
                CREATE TABLE IF NOT EXISTS sandbox.rdt_knowledge.test_simple (
                    id STRING,
                    name STRING
                ) USING DELTA
            """)
            print("SUCCESS: Table creation works")
        except Exception as e:
            print(f"ERROR creating table: {e}")
            return False

        # Test 3: Insert data
        print("\n3. Testing data insert...")
        try:
            spark.sql("""
                INSERT INTO sandbox.rdt_knowledge.test_simple
                VALUES ('test_1', 'Test Record')
            """)
            print("SUCCESS: Insert works")
        except Exception as e:
            print(f"ERROR inserting data: {e}")
            return False

        # Test 4: Query data
        print("\n4. Testing data query...")
        try:
            result = spark.sql("SELECT * FROM sandbox.rdt_knowledge.test_simple").collect()
            print(f"SUCCESS: Query works - found {len(result)} records")
            if result:
                print(f"Sample record: ID={result[0]['id']}, Name={result[0]['name']}")
        except Exception as e:
            print(f"ERROR querying data: {e}")
            return False

        # Test 5: List tables to confirm
        print("\n5. Listing tables in schema...")
        try:
            tables = spark.sql("SHOW TABLES IN sandbox.rdt_knowledge").collect()
            print(f"SUCCESS: Found {len(tables)} tables in schema")
            for table in tables:
                print(f"  - {table['tableName']}")
        except Exception as e:
            print(f"ERROR listing tables: {e}")

        # Cleanup
        print("\n6. Cleaning up test table...")
        try:
            spark.sql("DROP TABLE IF EXISTS sandbox.rdt_knowledge.test_simple")
            print("SUCCESS: Cleanup completed")
        except Exception as e:
            print(f"WARNING: Cleanup failed: {e}")

        print("\n=== SCHEMA TEST COMPLETED SUCCESSFULLY ===")
        print("sandbox.rdt_knowledge is ready for RAG implementation")

        return True

    except Exception as e:
        print(f"FATAL ERROR: {e}")
        return False

    finally:
        try:
            if 'spark' in locals():
                spark.stop()
                print("Session closed")
        except:
            pass

if __name__ == "__main__":
    success = main()
    if success:
        print("\nNext step: Execute Phase 1 and Phase 2 implementation")
    else:
        print("\nPlease check permissions and try again")
    sys.exit(0 if success else 1)