#!/usr/bin/env python3
"""
Simple validation and data insertion for RAG Phase 2
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json
from datetime import datetime

def main():
    print("Phase 2 Validation and Data Insertion")
    print("=" * 50)

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
        namespace = config['rag_config']['full_namespace']
        print(f"Using namespace: {namespace}")

        # Clear existing test data
        print("\nClearing existing test data...")
        try:
            spark.sql(f"DELETE FROM {namespace}.raw_documents WHERE document_id LIKE 'rag_doc_%'")
            spark.sql(f"DELETE FROM {namespace}.rag_document_chunks WHERE chunk_id LIKE 'chunk_%'")
            print("Existing test data cleared")
        except Exception as e:
            print(f"Clear data warning: {e}")

        # Insert documents with proper escaping
        print("\nInserting sample documents...")

        documents = [
            ("rag_doc_001", "company_policy.txt", "Company Remote Work Policy: All employees are eligible for remote work up to 3 days per week.", "policy"),
            ("rag_doc_002", "support_faq.txt", "FAQ: How to reset password? Go to Settings and click Reset Password.", "faq"),
            ("rag_doc_003", "technical_guide.txt", "Databricks RAG Setup: Install databricks-vectorsearch and configure Unity Catalog.", "technical")
        ]

        for doc_id, filename, content, category in documents:
            try:
                current_time = datetime.now().isoformat()
                content_hash = str(abs(hash(content)))

                spark.sql(f"""
                    INSERT INTO {namespace}.raw_documents VALUES (
                        '{doc_id}',
                        '{filename}',
                        '{content}',
                        'txt',
                        {len(content)},
                        '{current_time}',
                        '/sample/{filename}',
                        '{content_hash}',
                        '{{"category": "{category}"}}',
                        'ready'
                    )
                """)
                print(f"SUCCESS: Inserted {doc_id}")
            except Exception as e:
                print(f"ERROR inserting {doc_id}: {e}")

        # Insert chunks
        print("\nInserting document chunks...")

        chunks = [
            ("chunk_001", "rag_doc_001", "Company Remote Work Policy: All employees are eligible for remote work up to 3 days per week.", 1),
            ("chunk_002", "rag_doc_002", "FAQ: How to reset password? Go to Settings and click Reset Password.", 1),
            ("chunk_003", "rag_doc_003", "Databricks RAG Setup: Install databricks-vectorsearch and configure Unity Catalog.", 1)
        ]

        for chunk_id, doc_id, chunk_text, chunk_index in chunks:
            try:
                current_time = datetime.now().isoformat()
                chunk_hash = str(abs(hash(chunk_text)))

                spark.sql(f"""
                    INSERT INTO {namespace}.rag_document_chunks VALUES (
                        '{chunk_id}',
                        '{doc_id}',
                        '{chunk_text}',
                        {chunk_index},
                        {len(chunk_text)},
                        0,
                        '{chunk_hash}',
                        '{current_time}',
                        '{{"chunk_type": "sample"}}'
                    )
                """)
                print(f"SUCCESS: Inserted {chunk_id}")
            except Exception as e:
                print(f"ERROR inserting {chunk_id}: {e}")

        # Validate results
        print("\nValidating data...")

        doc_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.raw_documents WHERE document_id LIKE 'rag_doc_%'").collect()[0]['count']
        chunk_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.rag_document_chunks WHERE chunk_id LIKE 'chunk_%'").collect()[0]['count']

        print(f"Documents inserted: {doc_count}")
        print(f"Chunks inserted: {chunk_count}")

        if doc_count > 0 and chunk_count > 0:
            print("\nSample data preview:")
            results = spark.sql(f"""
                SELECT
                    r.document_id,
                    r.filename,
                    COUNT(c.chunk_id) as chunks
                FROM {namespace}.raw_documents r
                LEFT JOIN {namespace}.rag_document_chunks c ON r.document_id = c.document_id
                WHERE r.document_id LIKE 'rag_doc_%'
                GROUP BY r.document_id, r.filename
                ORDER BY r.document_id
            """).collect()

            for row in results:
                print(f"  {row['document_id']}: {row['filename']} ({row['chunks']} chunks)")

        print("\nPhase 2 data preparation completed successfully!")
        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False

    finally:
        try:
            if 'spark' in locals():
                spark.stop()
        except:
            pass

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)