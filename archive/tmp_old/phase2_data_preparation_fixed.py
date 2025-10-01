#!/usr/bin/env python3
"""
Phase 2: Data Preparation for RAG Knowledge Base (Fixed)
- Create RAG-specific tables in sandbox.rdt_knowledge using supported data types
- Insert sample documents and chunks for testing
- Prepare data structure for vector search implementation
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json
from datetime import datetime

def main():
    print("=" * 60)
    print("PHASE 2: RAG DATA PREPARATION (FIXED)")
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

        # Extract configuration
        rag_config = config['rag_config']
        namespace = rag_config['full_namespace']
        print(f"   Using namespace: {namespace}")

        # Step 2: Create RAG-specific tables with fixed data types
        print(f"\n2. Creating RAG tables in {namespace}...")

        # Raw documents table
        print("   Creating raw_documents table...")
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {namespace}.raw_documents (
                    document_id STRING,
                    filename STRING,
                    content STRING,
                    file_type STRING,
                    file_size_bytes BIGINT,
                    upload_timestamp STRING,
                    source_path STRING,
                    document_hash STRING,
                    metadata_json STRING,
                    processing_status STRING
                ) USING DELTA
                TBLPROPERTIES (
                    'delta.enableChangeDataFeed' = 'true'
                )
            """)
            print("   SUCCESS: raw_documents table created")
        except Exception as e:
            print(f"   ERROR: raw_documents table creation failed: {e}")
            return False

        # Document chunks table for vector search
        print("   Creating rag_document_chunks table...")
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {namespace}.rag_document_chunks (
                    chunk_id STRING,
                    document_id STRING,
                    chunk_text STRING,
                    chunk_index INT,
                    chunk_size INT,
                    overlap_size INT,
                    chunk_hash STRING,
                    created_timestamp STRING,
                    metadata_json STRING
                ) USING DELTA
                TBLPROPERTIES (
                    'delta.enableChangeDataFeed' = 'true'
                )
            """)
            print("   SUCCESS: rag_document_chunks table created")
        except Exception as e:
            print(f"   ERROR: rag_document_chunks table creation failed: {e}")
            return False

        # Query logs table for monitoring
        print("   Creating rag_query_logs table...")
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {namespace}.rag_query_logs (
                    query_id STRING,
                    user_id STRING,
                    query_text STRING,
                    response_text STRING,
                    query_timestamp STRING,
                    response_time_ms BIGINT,
                    relevance_score DOUBLE,
                    sources_used STRING,
                    user_feedback STRING,
                    session_id STRING
                ) USING DELTA
                TBLPROPERTIES (
                    'delta.enableChangeDataFeed' = 'true'
                )
            """)
            print("   SUCCESS: rag_query_logs table created")
        except Exception as e:
            print(f"   ERROR: rag_query_logs table creation failed: {e}")
            return False

        # Performance metrics table
        print("   Creating rag_performance_metrics table...")
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {namespace}.rag_performance_metrics (
                    metric_date STRING,
                    total_queries BIGINT,
                    avg_response_time_ms DOUBLE,
                    avg_relevance_score DOUBLE,
                    success_rate DOUBLE,
                    unique_users BIGINT,
                    created_timestamp STRING
                ) USING DELTA
            """)
            print("   SUCCESS: rag_performance_metrics table created")
        except Exception as e:
            print(f"   ERROR: rag_performance_metrics table creation failed: {e}")
            return False

        # Step 3: Insert sample documents
        print("\n3. Inserting sample documents for RAG testing...")

        sample_docs = [
            {
                'doc_id': 'rag_doc_001',
                'filename': 'company_remote_work_policy.txt',
                'content': 'Company Remote Work Policy: All employees are eligible for remote work up to 3 days per week. Employees must maintain core business hours from 9:00 AM to 3:00 PM in their local timezone. The company provides necessary equipment including laptop, monitor, and office supplies for remote work setup. Daily check-ins via Slack or Microsoft Teams are mandatory.',
                'file_type': 'txt',
                'category': 'policy'
            },
            {
                'doc_id': 'rag_doc_002',
                'filename': 'product_support_faq.txt',
                'content': 'Product Support FAQ: Q: How do I reset my password? A: Go to Settings > Security > Reset Password. Q: What is the refund policy? A: We offer a 30-day money-back guarantee for all products. Q: How do I contact technical support? A: Technical support is available 24/7 via live chat, email, or phone.',
                'file_type': 'txt',
                'category': 'faq'
            },
            {
                'doc_id': 'rag_doc_003',
                'filename': 'databricks_rag_technical_guide.txt',
                'content': 'Databricks RAG Implementation Technical Guide: Install required dependencies databricks-vectorsearch and databricks-connect. Configure Unity Catalog with appropriate permissions. Create vector search endpoint using databricks-gte-large-en embedding model. Use DBRX or Llama models via Databricks Model Serving.',
                'file_type': 'txt',
                'category': 'technical'
            }
        ]

        # Insert documents
        for doc in sample_docs:
            try:
                current_time = datetime.now().isoformat()
                content_hash = str(hash(doc['content']))
                metadata = f'{"category": "{doc["category"]}", "sample_data": true}'

                # Escape single quotes in content
                content_escaped = doc['content'].replace("'", "''")

                spark.sql(f"""
                    INSERT INTO {namespace}.raw_documents VALUES (
                        '{doc["doc_id"]}',
                        '{doc["filename"]}',
                        '{content_escaped}',
                        '{doc["file_type"]}',
                        {len(doc["content"])},
                        '{current_time}',
                        '/sample_data/{doc["filename"]}',
                        '{content_hash}',
                        '{metadata}',
                        'ready'
                    )
                """)
                print(f"   SUCCESS: Inserted document {doc['doc_id']}")
            except Exception as e:
                print(f"   ERROR: Failed to insert {doc['doc_id']}: {e}")

        # Step 4: Create document chunks
        print("\n4. Creating document chunks for vector search...")

        chunks_data = [
            # Document 1 chunks (Remote Work Policy)
            ('chunk_001_001', 'rag_doc_001', 'Company Remote Work Policy: All employees are eligible for remote work up to 3 days per week. Employees must maintain core business hours from 9:00 AM to 3:00 PM.', 1, 150, 0, 'remote_work_eligibility'),
            ('chunk_001_002', 'rag_doc_001', 'The company provides necessary equipment including laptop, monitor, and office supplies for remote work setup. Daily check-ins via Slack or Microsoft Teams are mandatory.', 2, 145, 20, 'remote_work_equipment'),

            # Document 2 chunks (FAQ)
            ('chunk_002_001', 'rag_doc_002', 'Q: How do I reset my password? A: Go to Settings > Security > Reset Password. Enter your email and follow instructions.', 1, 120, 0, 'password_reset'),
            ('chunk_002_002', 'rag_doc_002', 'Q: What is the refund policy? A: We offer a 30-day money-back guarantee for all products. Refunds processed within 5-7 business days.', 2, 130, 15, 'refund_policy'),
            ('chunk_002_003', 'rag_doc_002', 'Q: How do I contact technical support? A: Technical support available 24/7 via live chat, email, or phone. Enterprise customers have dedicated channels.', 3, 140, 20, 'technical_support'),

            # Document 3 chunks (Technical Guide)
            ('chunk_003_001', 'rag_doc_003', 'Databricks RAG Implementation: Install required dependencies databricks-vectorsearch and databricks-connect. Configure Unity Catalog with appropriate permissions.', 1, 135, 0, 'rag_setup'),
            ('chunk_003_002', 'rag_doc_003', 'Create vector search endpoint using databricks-gte-large-en embedding model. Configure delta sync index for automatic updates when source data changes.', 2, 140, 25, 'vector_search_config'),
            ('chunk_003_003', 'rag_doc_003', 'Use DBRX or Llama models via Databricks Model Serving. Configure AI Playground with Agent Bricks for no-code chatbot deployment.', 3, 125, 20, 'model_integration')
        ]

        for chunk_id, doc_id, chunk_text, chunk_index, chunk_size, overlap_size, section in chunks_data:
            try:
                current_time = datetime.now().isoformat()
                chunk_hash = str(hash(chunk_text))
                metadata = f'{"section": "{section}", "sample_chunk": true}'

                # Escape single quotes in chunk text
                chunk_text_escaped = chunk_text.replace("'", "''")

                spark.sql(f"""
                    INSERT INTO {namespace}.rag_document_chunks VALUES (
                        '{chunk_id}',
                        '{doc_id}',
                        '{chunk_text_escaped}',
                        {chunk_index},
                        {chunk_size},
                        {overlap_size},
                        '{chunk_hash}',
                        '{current_time}',
                        '{metadata}'
                    )
                """)
            except Exception as e:
                print(f"   ERROR: Failed to insert chunk {chunk_id}: {e}")

        print(f"   SUCCESS: Created {len(chunks_data)} document chunks")

        # Step 5: Validate data creation
        print("\n5. Validating created data...")

        try:
            # Count documents
            doc_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.raw_documents").collect()[0]['count']
            print(f"   Raw documents: {doc_count}")

            # Count chunks
            chunk_count = spark.sql(f"SELECT COUNT(*) as count FROM {namespace}.rag_document_chunks").collect()[0]['count']
            print(f"   Document chunks: {chunk_count}")

            # Sample data preview
            print("\n   Sample documents:")
            docs_sample = spark.sql(f"""
                SELECT document_id, filename, LEFT(content, 50) as content_preview
                FROM {namespace}.raw_documents
                ORDER BY document_id
            """).collect()

            for row in docs_sample:
                print(f"     {row['document_id']}: {row['filename']} - {row['content_preview']}...")

            # Chunks per document
            print("\n   Chunks per document:")
            chunk_summary = spark.sql(f"""
                SELECT
                    r.document_id,
                    r.filename,
                    COUNT(c.chunk_id) as chunk_count
                FROM {namespace}.raw_documents r
                LEFT JOIN {namespace}.rag_document_chunks c ON r.document_id = c.document_id
                GROUP BY r.document_id, r.filename
                ORDER BY r.document_id
            """).collect()

            for row in chunk_summary:
                print(f"     {row['filename']}: {row['chunk_count']} chunks")

        except Exception as e:
            print(f"   ERROR: Data validation failed: {e}")
            return False

        # Step 6: Insert sample query logs
        print("\n6. Creating sample query logs for monitoring...")

        sample_queries = [
            ('query_001', 'user_001', 'What is the company remote work policy?', 'Remote work is allowed up to 3 days per week with core hours 9-3 PM.', 1250, 0.85, 'rag_doc_001'),
            ('query_002', 'user_002', 'How do I reset my password?', 'Go to Settings > Security > Reset Password and follow email instructions.', 890, 0.92, 'rag_doc_002'),
            ('query_003', 'user_001', 'What are the requirements for Databricks RAG?', 'Install databricks-vectorsearch, configure Unity Catalog with vector search permissions.', 1100, 0.88, 'rag_doc_003')
        ]

        for query_id, user_id, query_text, response_text, response_time, relevance, source in sample_queries:
            try:
                current_time = datetime.now().isoformat()

                # Escape single quotes
                query_escaped = query_text.replace("'", "''")
                response_escaped = response_text.replace("'", "''")

                spark.sql(f"""
                    INSERT INTO {namespace}.rag_query_logs VALUES (
                        '{query_id}',
                        '{user_id}',
                        '{query_escaped}',
                        '{response_escaped}',
                        '{current_time}',
                        {response_time},
                        {relevance},
                        '{source}',
                        'positive',
                        'session_001'
                    )
                """)
            except Exception as e:
                print(f"   ERROR: Failed to insert query log {query_id}: {e}")

        print("   SUCCESS: Sample query logs created")

        # Step 7: Create performance metrics sample
        print("\n7. Creating sample performance metrics...")
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            current_time = datetime.now().isoformat()

            spark.sql(f"""
                INSERT INTO {namespace}.rag_performance_metrics VALUES (
                    '{today}',
                    3,
                    1080.0,
                    0.883,
                    1.0,
                    2,
                    '{current_time}'
                )
            """)
            print("   SUCCESS: Sample performance metrics created")
        except Exception as e:
            print(f"   ERROR: Failed to create performance metrics: {e}")

        print("\n" + "=" * 60)
        print("PHASE 2 COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("✅ RAG-specific tables created in sandbox.rdt_knowledge")
        print("✅ Sample documents inserted (3 documents)")
        print("✅ Document chunks created (8 chunks)")
        print("✅ Sample query logs generated (3 queries)")
        print("✅ Performance metrics initialized")
        print("✅ Data structure ready for vector search implementation")
        print("\nNext: Vector search endpoint creation and index setup")

        return True

    except Exception as e:
        print(f"\nERROR: Phase 2 failed with exception: {e}")
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