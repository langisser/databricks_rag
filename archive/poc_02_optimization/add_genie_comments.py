#!/usr/bin/env python3
"""
ADD DATABRICKS GENIE COMMENTS TO PRODUCTION TABLES
- Add table and column comments for better Genie understanding
- Enhance metadata for natural language queries
- Optimize for business user interactions
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json
from datetime import datetime

def main():
    print("=" * 80)
    print("ADDING DATABRICKS GENIE COMMENTS TO PRODUCTION TABLES")
    print("=" * 80)
    print(f"Started: {datetime.now().strftime('%H:%M:%S')}")

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()

        namespace = config['rag_config']['full_namespace']

        print(f"\\nNamespace: {namespace}")

        # Step 1: Add comments to production_documents table
        print(f"\\n1. Adding comments to production_documents table...")

        documents_table = f"{namespace}.production_documents"

        # Add table comment
        spark.sql(f"""
            ALTER TABLE {documents_table}
            SET TBLPROPERTIES (
                'comment' = 'Production RDT knowledge base containing all regulatory and business documents for Bank of Thailand (BOT) data submission, validation frameworks, and operational procedures. Contains 24+ business documents including DOCX, XLSX, PDF, and PPTX files with comprehensive regulatory compliance content.'
            )
        """)

        # Add column comments
        column_comments = [
            ("doc_id", "Unique document identifier for each processed business document"),
            ("file_name", "Original file name from Unity Catalog volume containing regulatory documents, frameworks, and guidelines"),
            ("file_type", "Document format type (docx, xlsx, pdf, pptx, drawio) indicating content structure and processing method"),
            ("file_size_mb", "Document size in megabytes, useful for understanding content volume and processing complexity"),
            ("word_count", "Total number of words extracted from document, indicating content richness and detail level"),
            ("processing_status", "Document processing result (success, failed, estimated) showing extraction quality and reliability"),
            ("created_at", "Timestamp when document was processed and added to knowledge base for tracking and versioning")
        ]

        for column, comment in column_comments:
            try:
                spark.sql(f"""
                    ALTER TABLE {documents_table}
                    ALTER COLUMN {column}
                    COMMENT '{comment}'
                """)
                print(f"   Added comment to column: {column}")
            except Exception as e:
                print(f"   Warning: Could not add comment to {column}: {e}")

        # Step 2: Add comments to production_chunks table
        print(f"\\n2. Adding comments to production_chunks table...")

        chunks_table = f"{namespace}.production_chunks"

        # Add table comment
        spark.sql(f"""
            ALTER TABLE {chunks_table}
            SET TBLPROPERTIES (
                'comment' = 'Semantic chunks from RDT business documents optimized for RAG (Retrieval-Augmented Generation) and vector search. Contains intelligently segmented content from regulatory documents, operational frameworks, data validation guidelines, and BOT submission procedures. Used by AI Playground and Genie for natural language queries about business processes.'
            )
        """)

        # Add column comments for chunks table
        chunk_column_comments = [
            ("chunk_id", "Unique identifier for each semantic chunk of business content, used for vector search and retrieval"),
            ("doc_id", "Reference to parent document identifier, linking chunks back to source regulatory or operational documents"),
            ("chunk_text", "Semantic content segment containing business knowledge about BOT regulations, data validation, or operational procedures"),
            ("chunk_words", "Number of words in chunk, indicating content density and context size for optimal RAG performance"),
            ("file_name", "Source document name containing regulatory frameworks, BOT guidelines, or operational procedures"),
            ("file_type", "Original document format (docx, xlsx, pdf, pptx) indicating content structure and business document type"),
            ("created_at", "Processing timestamp for tracking when business content was indexed and made available for queries")
        ]

        for column, comment in chunk_column_comments:
            try:
                spark.sql(f"""
                    ALTER TABLE {chunks_table}
                    ALTER COLUMN {column}
                    COMMENT '{comment}'
                """)
                print(f"   Added comment to column: {column}")
            except Exception as e:
                print(f"   Warning: Could not add comment to {column}: {e}")

        # Step 3: Add business context metadata
        print(f"\\n3. Adding business context metadata...")

        # Add business tags and metadata
        business_metadata = {
            'business_domain': 'Regulatory Data Transmission (RDT)',
            'primary_regulator': 'Bank of Thailand (BOT)',
            'content_types': 'Data validation, submission frameworks, operational procedures, compliance guidelines',
            'languages': 'Thai and English',
            'use_cases': 'Regulatory compliance, data quality validation, operational guidance, BOT submission procedures',
            'genie_optimized': 'true',
            'rag_enabled': 'true',
            'last_updated': datetime.now().strftime('%Y-%m-%d')
        }

        # Add metadata to documents table
        for key, value in business_metadata.items():
            spark.sql(f"""
                ALTER TABLE {documents_table}
                SET TBLPROPERTIES ('{key}' = '{value}')
            """)

        # Add metadata to chunks table
        for key, value in business_metadata.items():
            spark.sql(f"""
                ALTER TABLE {chunks_table}
                SET TBLPROPERTIES ('{key}' = '{value}')
            """)

        print("   Business metadata added to both tables")

        # Step 4: Create Genie-optimized views
        print(f"\\n4. Creating Genie-optimized views...")

        # Create business-friendly view of documents
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
            FROM {documents_table}
            WHERE processing_status = 'success'
        """)

        # Add view comment
        spark.sql(f"""
            ALTER VIEW {namespace}.genie_document_catalog
            SET TBLPROPERTIES (
                'comment' = 'Business-friendly view of RDT knowledge base documents optimized for Databricks Genie natural language queries. Categorizes documents by business function (BOT Regulatory, DataX Framework, Data Validation, etc.) and provides human-readable metrics for document discovery and content analysis.'
            )
        """)

        # Create content summary view
        spark.sql(f"""
            CREATE OR REPLACE VIEW {namespace}.genie_content_summary AS
            SELECT
                file_type as document_type,
                COUNT(*) as document_count,
                SUM(word_count) as total_words,
                AVG(word_count) as avg_words_per_doc,
                ROUND(SUM(file_size_mb), 2) as total_size_mb,
                MAX(created_at) as latest_update
            FROM {documents_table}
            WHERE processing_status = 'success'
            GROUP BY file_type
        """)

        # Add summary view comment
        spark.sql(f"""
            ALTER VIEW {namespace}.genie_content_summary
            SET TBLPROPERTIES (
                'comment' = 'Summary statistics of RDT knowledge base content by document type, providing insights into content volume, word counts, and coverage for business users querying through Databricks Genie.'
            )
        """)

        print("   Created Genie-optimized views: genie_document_catalog, genie_content_summary")

        # Step 5: Verify comments and metadata
        print(f"\\n5. Verifying table comments and metadata...")

        # Check table comments
        for table_name in [documents_table, chunks_table]:
            try:
                describe_result = spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").collect()
                for row in describe_result:
                    if row['col_name'] == 'Comment':
                        print(f"   {table_name}: Comment added successfully")
                        break
            except Exception as e:
                print(f"   Warning: Could not verify {table_name}: {e}")

        spark.stop()

        # Step 6: Generate Genie usage guide
        print(f"\\n6. Generating Genie usage guide...")

        genie_guide = {
            "databricks_genie_setup": {
                "tables_optimized": [
                    f"{namespace}.production_documents",
                    f"{namespace}.production_chunks"
                ],
                "views_created": [
                    f"{namespace}.genie_document_catalog",
                    f"{namespace}.genie_content_summary"
                ],
                "business_metadata_added": True
            },
            "sample_genie_queries": [
                "How many BOT regulatory documents do we have?",
                "What is the total word count of DataX framework documents?",
                "Show me documents related to data validation",
                "Which document types have the most content?",
                "What are the latest processed documents?",
                "How many words are in counterparty documents?",
                "Show RDT process documentation summary",
                "What documents were processed today?"
            ],
            "thai_genie_queries": [
                "มีเอกสารระเบียบ BOT กี่ฉบับ?",
                "เอกสาร DataX มีจำนวนคำทั้งหมดเท่าไหร่?",
                "แสดงเอกสารที่เกี่ยวข้องกับการตรวจสอบข้อมูล",
                "ประเภทเอกสารไหนมีเนื้อหามากที่สุด?"
            ],
            "business_categories": {
                "BOT Regulatory": "Bank of Thailand regulations and compliance",
                "DataX Framework": "Data processing and operational frameworks",
                "Data Validation": "Quality assurance and validation procedures",
                "RDT Process": "Regulatory Data Transmission procedures",
                "Data Integration": "Schema synchronization and data flow",
                "General Business": "Other business documentation"
            },
            "genie_optimization_features": {
                "table_comments": "Comprehensive business context descriptions",
                "column_comments": "Detailed field descriptions for natural language understanding",
                "business_metadata": "Domain-specific tags and categorization",
                "friendly_views": "Business-user optimized data views",
                "multilingual": "Thai and English query support"
            }
        }

        # Save Genie guide
        genie_guide_path = os.path.join(os.path.dirname(__file__), 'genie_usage_guide.json')
        with open(genie_guide_path, 'w', encoding='utf-8') as f:
            json.dump(genie_guide, f, indent=2, ensure_ascii=False)

        print(f"   Genie usage guide saved: genie_usage_guide.json")

        # Final summary
        print(f"\\n" + "=" * 80)
        print("DATABRICKS GENIE OPTIMIZATION COMPLETED")
        print("=" * 80)
        print(f"Completed: {datetime.now().strftime('%H:%M:%S')}")

        print(f"\\nTABLES OPTIMIZED FOR GENIE:")
        print(f"  ✅ {documents_table} - Business documents with full metadata")
        print(f"  ✅ {chunks_table} - Semantic chunks for RAG queries")

        print(f"\\nVIEWS CREATED:")
        print(f"  ✅ {namespace}.genie_document_catalog - Business-friendly document view")
        print(f"  ✅ {namespace}.genie_content_summary - Content statistics view")

        print(f"\\nGENIE FEATURES ADDED:")
        print(f"  📝 Table comments with business context")
        print(f"  📋 Column comments for natural language understanding")
        print(f"  🏷️ Business metadata and categorization")
        print(f"  👁️ User-friendly views for business queries")
        print(f"  🌏 Thai and English language support")

        print(f"\\nSAMPLE GENIE QUERIES:")
        print(f"  • 'How many BOT regulatory documents do we have?'")
        print(f"  • 'What is the total word count of DataX documents?'")
        print(f"  • 'Show me documents related to data validation'")
        print(f"  • 'มีเอกสารระเบียบ BOT กี่ฉบับ?' (Thai)")

        print(f"\\nNEXT STEPS:")
        print("1. Test Databricks Genie with sample queries")
        print("2. Use business-friendly views for better results")
        print("3. Try both English and Thai natural language queries")
        print("4. Reference genie_usage_guide.json for more examples")

        print(f"\\nGENIE USAGE GUIDE: genie_usage_guide.json")

        return True

    except Exception as e:
        print(f"\\nERROR: Genie optimization failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)