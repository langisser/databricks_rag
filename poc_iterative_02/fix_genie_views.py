#!/usr/bin/env python3
"""
Fix Genie views to work with production data
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json

def main():
    try:
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()

        namespace = config['rag_config']['full_namespace']

        print("FIXING GENIE VIEWS")
        print("=" * 30)

        # Fix document catalog view
        spark.sql(f"""
            CREATE OR REPLACE VIEW {namespace}.genie_document_catalog AS
            SELECT
                file_name as document_name,
                file_type as document_format,
                ROUND(file_size_mb, 2) as size_mb,
                word_count as total_words,
                processing_status as status,
                created_at as processed_date,
                CASE
                    WHEN file_name LIKE '%BOT%' OR file_name LIKE '%Counterparty%' THEN 'BOT Regulatory'
                    WHEN file_name LIKE '%DataX%' THEN 'DataX Framework'
                    WHEN file_name LIKE '%Validation%' THEN 'Data Validation'
                    WHEN file_name LIKE '%RDT%' THEN 'RDT Process'
                    WHEN file_name LIKE '%Schema%' THEN 'Data Integration'
                    ELSE 'General Business'
                END as business_category
            FROM {namespace}.production_documents
        """)

        # Fix content summary view
        spark.sql(f"""
            CREATE OR REPLACE VIEW {namespace}.genie_content_summary AS
            SELECT
                file_type as document_type,
                COUNT(*) as document_count,
                SUM(word_count) as total_words,
                AVG(word_count) as avg_words_per_doc,
                ROUND(SUM(file_size_mb), 2) as total_size_mb,
                MAX(created_at) as latest_update
            FROM {namespace}.production_documents
            GROUP BY file_type
        """)

        # Test views
        catalog_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.genie_document_catalog").collect()[0]['count']
        summary_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.genie_content_summary").collect()[0]['count']

        print(f"Document catalog: {catalog_count} documents")
        print(f"Content summary: {summary_count} document types")

        spark.stop()
        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    main()