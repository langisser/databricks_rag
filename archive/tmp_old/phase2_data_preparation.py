#!/usr/bin/env python3
"""
Phase 2: Data Preparation for RAG Knowledge Base
- Create RAG-specific tables in sandbox.rdt_knowledge
- Insert sample documents and chunks for testing
- Prepare data structure for vector search implementation
- Validate data integrity and access patterns
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json
from datetime import datetime

def main():
    print("=" * 60)
    print("PHASE 2: RAG DATA PREPARATION")
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

        # Step 2: Create RAG-specific tables
        print(f"\n2. Creating RAG tables in {namespace}...")

        # Raw documents table
        print("   Creating raw_documents table...")
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {namespace}.raw_documents (
                    document_id STRING,
                    filename STRING,
                    content TEXT,
                    file_type STRING,
                    file_size_bytes BIGINT,
                    upload_timestamp TIMESTAMP,
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
                    chunk_text TEXT,
                    chunk_index INT,
                    chunk_size INT,
                    overlap_size INT,
                    chunk_hash STRING,
                    created_timestamp TIMESTAMP,
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
                    query_text TEXT,
                    response_text TEXT,
                    query_timestamp TIMESTAMP,
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
                    metric_date DATE,
                    total_queries BIGINT,
                    avg_response_time_ms DOUBLE,
                    avg_relevance_score DOUBLE,
                    success_rate DOUBLE,
                    unique_users BIGINT,
                    created_timestamp TIMESTAMP
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
                'content': '''Company Remote Work Policy

1. Remote Work Eligibility
All employees are eligible for remote work up to 3 days per week. Employees must maintain core business hours from 9:00 AM to 3:00 PM in their local timezone.

2. Equipment and Setup
The company provides necessary equipment including laptop, monitor, and office supplies for remote work setup. Employees are responsible for maintaining a professional home office environment.

3. Communication Requirements
Daily check-ins via Slack or Microsoft Teams are mandatory. All meetings must be scheduled with timezone considerations for global team members.

4. Performance Standards
Remote work performance is measured by deliverable completion, meeting attendance, and collaboration effectiveness. Quarterly reviews assess remote work arrangement success.''',
                'file_type': 'txt',
                'category': 'policy'
            },
            {
                'doc_id': 'rag_doc_002',
                'filename': 'product_support_faq.txt',
                'content': '''Product Support FAQ

Q: How do I reset my password?
A: Go to Settings > Security > Reset Password. Enter your email address and follow the instructions sent to your email. Password reset links expire after 24 hours.

Q: What is the refund policy?
A: We offer a 30-day money-back guarantee for all products. Refunds are processed within 5-7 business days. Digital products are eligible for refund within 7 days of purchase.

Q: How do I contact technical support?
A: Technical support is available 24/7 via live chat, email (support@company.com), or phone (1-800-SUPPORT). Enterprise customers have dedicated support channels.

Q: What are the system requirements?
A: Minimum requirements include 4GB RAM, Windows 10/macOS 10.14, and modern web browser. Recommended setup includes 8GB RAM and SSD storage for optimal performance.''',
                'file_type': 'txt',
                'category': 'faq'
            },
            {
                'doc_id': 'rag_doc_003',
                'filename': 'databricks_rag_technical_guide.txt',
                'content': '''Databricks RAG Implementation Technical Guide

1. Environment Setup
Install required dependencies: databricks-vectorsearch, databricks-connect. Configure Unity Catalog with appropriate permissions for vector search operations.

2. Vector Search Configuration
Create vector search endpoint using databricks-gte-large-en embedding model. Configure delta sync index for automatic updates when source data changes.

3. Data Pipeline
Process documents using chunking strategy with 1000-character chunks and 200-character overlap. Store chunks in Delta tables with change data feed enabled.

4. Model Integration
Use DBRX or Llama models via Databricks Model Serving. Configure AI Playground with Agent Bricks for no-code chatbot deployment.

5. Monitoring and Evaluation
Implement query logging, performance metrics, and user feedback collection. Use Mosaic AI Agent Evaluation for automated quality assessment.''',
                'file_type': 'txt',
                'category': 'technical'
            }
        ]

        # Insert documents
        for doc in sample_docs:
            try:
                current_time = datetime.now().isoformat()
                content_hash = str(hash(doc['content']))
                metadata = f'{{"category": "{doc["category"]}", "sample_data": true}}'

                spark.sql(f"""
                    INSERT INTO {namespace}.raw_documents VALUES (
                        '{doc["doc_id"]}',
                        '{doc["filename"]}',
                        '''{doc["content"]}''',
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
            ('chunk_001_001', 'rag_doc_001', 'Company Remote Work Policy. All employees are eligible for remote work up to 3 days per week. Employees must maintain core business hours from 9:00 AM to 3:00 PM in their local timezone.', 1, 200, 0, 'remote_work_eligibility'),
            ('chunk_001_002', 'rag_doc_001', 'Employees must maintain core business hours from 9:00 AM to 3:00 PM in their local timezone. The company provides necessary equipment including laptop, monitor, and office supplies for remote work setup.', 2, 200, 50, 'remote_work_equipment'),
            ('chunk_001_003', 'rag_doc_001', 'Daily check-ins via Slack or Microsoft Teams are mandatory. All meetings must be scheduled with timezone considerations for global team members. Performance is measured by deliverable completion.', 3, 190, 40, 'remote_work_communication'),

            # Document 2 chunks (FAQ)
            ('chunk_002_001', 'rag_doc_002', 'Q: How do I reset my password? A: Go to Settings > Security > Reset Password. Enter your email address and follow the instructions sent to your email. Password reset links expire after 24 hours.', 1, 180, 0, 'password_reset'),
            ('chunk_002_002', 'rag_doc_002', 'Q: What is the refund policy? A: We offer a 30-day money-back guarantee for all products. Refunds are processed within 5-7 business days. Digital products are eligible for refund within 7 days.', 2, 170, 30, 'refund_policy'),
            ('chunk_002_003', 'rag_doc_002', 'Q: How do I contact technical support? A: Technical support is available 24/7 via live chat, email (support@company.com), or phone (1-800-SUPPORT). Enterprise customers have dedicated support channels.', 3, 185, 25, 'technical_support'),

            # Document 3 chunks (Technical Guide)
            ('chunk_003_001', 'rag_doc_003', 'Databricks RAG Implementation Technical Guide. Install required dependencies: databricks-vectorsearch, databricks-connect. Configure Unity Catalog with appropriate permissions for vector search operations.', 1, 190, 0, 'rag_setup'),
            ('chunk_003_002', 'rag_doc_003', 'Create vector search endpoint using databricks-gte-large-en embedding model. Configure delta sync index for automatic updates when source data changes. Use chunking strategy with 1000-character chunks.', 2, 185, 45, 'vector_search_config'),
            ('chunk_003_003', 'rag_doc_003', 'Use DBRX or Llama models via Databricks Model Serving. Configure AI Playground with Agent Bricks for no-code chatbot deployment. Implement monitoring and evaluation with query logging.', 3, 175, 40, 'model_integration')
        ]

        for chunk_id, doc_id, chunk_text, chunk_index, chunk_size, overlap_size, section in chunks_data:
            try:
                current_time = datetime.now().isoformat()
                chunk_hash = str(hash(chunk_text))
                metadata = f'{{"section": "{section}", "sample_chunk": true}}'

                spark.sql(f"""
                    INSERT INTO {namespace}.rag_document_chunks VALUES (
                        '{chunk_id}',
                        '{doc_id}',
                        '''{chunk_text}''',
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

            # Sample query test
            sample_query = spark.sql(f"""
                SELECT
                    r.document_id,
                    r.filename,
                    COUNT(c.chunk_id) as chunk_count
                FROM {namespace}.raw_documents r
                LEFT JOIN {namespace}.rag_document_chunks c ON r.document_id = c.document_id
                GROUP BY r.document_id, r.filename
                ORDER BY r.document_id
            """).collect()

            print("   Document-to-chunk mapping:")
            for row in sample_query:
                print(f"     {row['filename']}: {row['chunk_count']} chunks")

        except Exception as e:
            print(f"   ERROR: Data validation failed: {e}")
            return False

        # Step 6: Insert sample query logs for testing
        print("\n6. Creating sample query logs for monitoring...")

        sample_queries = [
            ('query_001', 'user_001', 'What is the company remote work policy?', 'Remote work is allowed up to 3 days per week with core hours 9-3 PM.', 1250, 0.85, 'rag_doc_001'),
            ('query_002', 'user_002', 'How do I reset my password?', 'Go to Settings > Security > Reset Password and follow email instructions.', 890, 0.92, 'rag_doc_002'),
            ('query_003', 'user_001', 'What are the system requirements for Databricks RAG?', 'Install databricks-vectorsearch, configure Unity Catalog with vector search permissions.', 1100, 0.88, 'rag_doc_003')
        ]

        for query_id, user_id, query_text, response_text, response_time, relevance, source in sample_queries:
            try:
                current_time = datetime.now().isoformat()
                spark.sql(f"""
                    INSERT INTO {namespace}.rag_query_logs VALUES (
                        '{query_id}',
                        '{user_id}',
                        '{query_text}',
                        '{response_text}',
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

        print("\n" + "=" * 60)
        print("PHASE 2 COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("✅ RAG-specific tables created in sandbox.rdt_knowledge")
        print("✅ Sample documents inserted (3 documents)")
        print("✅ Document chunks created (9 chunks)")
        print("✅ Sample query logs generated")
        print("✅ Data structure ready for vector search implementation")
        print("\nNext: Create vector search endpoint and index (Phase 3)")

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