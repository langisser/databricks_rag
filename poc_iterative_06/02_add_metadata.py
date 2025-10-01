#!/usr/bin/env python3
"""
POC06 Phase 1 - Add Metadata to Improved Chunks
Source: poc06_phase1_all_formats_chunks (docx, pdf, xlsx, pptx)
Adds: content_category, document_type, section_name, table_name

INCLUDES PPTX metadata:
- slide_number (preserved from processing)
- slide_title (preserved from processing)
"""

import sys
import os
import json

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F


def main():
    print("=" * 80)
    print("POC06 PHASE 1 - ADD METADATA (ALL FORMATS + PPTX)")
    print("=" * 80)

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    cluster_id = config['databricks'].get('lineage_cluster_id', config['databricks']['cluster_id'])

    print(f"\n[1] Connecting to Databricks (cluster: {cluster_id})...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=cluster_id
    ).getOrCreate()
    print("    Connected")

    # Get source table
    source_table = config['rag_config']['poc06_phase1_table']
    print(f"\n[2] Loading: {source_table}")

    df = spark.table(source_table)
    row_count = df.count()
    print(f"    Loaded {row_count} chunks")

    # Show format breakdown
    print(f"\n    Format breakdown:")
    df.groupBy('processing_method').count().orderBy(F.col('count').desc()).show(truncate=False)

    # Add metadata using SQL expressions
    print(f"\n[3] Adding metadata columns...")

    df_with_metadata = df.withColumn(
        'content_category',
        F.when(
            # PPTX slides
            F.col('content_type') == 'slide',
            'presentation_slide'
        ).when(
            # Tables with column definitions
            (F.col('content').contains('[TABLE]')) &
            ((F.lower(F.col('content')).contains('columns:')) |
             (F.lower(F.col('content')).contains('column name'))),
            'table_definition'
        ).when(
            # Table data
            F.col('content').contains('[TABLE]'),
            'table_data'
        ).when(
            # Code examples
            (F.col('content').contains('SELECT ')) |
            (F.col('content').contains('INSERT INTO')) |
            (F.col('content').contains('def ')) |
            (F.col('content').contains('function')),
            'code_example'
        ).when(
            # Images/diagrams
            F.col('content_type') == 'image',
            'image_diagram'
        ).when(
            # Headers
            (F.col('content').startswith('[SECTION]')) & (F.length(F.col('content')) < 200),
            'header'
        ).otherwise('process_description')
    ).withColumn(
        'document_type',
        F.when(
            # PPTX presentations
            F.lower(F.col('source_document')).endswith('.pptx'),
            'presentation'
        ).when(
            F.lower(F.col('source_document')).contains('validation'),
            'validation_guide'
        ).when(
            F.lower(F.col('source_document')).contains('framework'),
            'framework_guide'
        ).when(
            F.lower(F.col('source_document')).contains('process'),
            'process_guide'
        ).when(
            F.lower(F.col('source_document')).contains('submission'),
            'technical_spec'
        ).when(
            (F.lower(F.col('source_document')).contains('standard')) |
            (F.lower(F.col('source_document')).contains('guideline')),
            'standard_guide'
        ).when(
            F.lower(F.col('source_document')).contains('counterparty'),
            'technical_spec'
        ).otherwise('technical_spec')
    ).withColumn(
        'section_name',
        F.when(
            # PPTX: Use slide title
            (F.col('content_type') == 'slide') & (F.col('slide_title').isNotNull()),
            F.col('slide_title')
        ).when(
            # Extract from [SECTION] marker
            F.col('content').contains('[SECTION]'),
            F.regexp_extract(F.col('content'), r'\[SECTION\]\s*(.+?)(?:\n|KEYWORDS:)', 1)
        ).when(
            # Extract from [TABLE] marker
            F.col('content').contains('[TABLE]'),
            F.regexp_extract(F.col('content'), r'\[TABLE\]\s*(.+?)(?:\n|KEYWORDS:)', 1)
        ).when(
            # Extract from [SLIDE] marker
            F.col('content').contains('[SLIDE'),
            F.regexp_extract(F.col('content'), r'\[SLIDE \d+\]\s*(.+?)(?:\n|$)', 1)
        ).when(
            # Extract from markdown headers
            F.col('content').startswith('##'),
            F.regexp_extract(F.col('content'), r'^##?\s*(.+?)(?:\n|$)', 1)
        ).otherwise('Unknown')
    ).withColumn(
        'table_name',
        # Extract tbl_* pattern
        F.when(
            F.lower(F.col('content')).rlike(r'\btbl_[a-z_]+\b'),
            F.lower(F.regexp_extract(F.col('content'), r'\b(tbl_[a-z_]+)\b', 1))
        ).otherwise(None)
    )

    print("    Metadata columns added")

    # Show statistics
    print(f"\n[4] Metadata statistics:")

    print(f"\n    Content Category Distribution:")
    df_with_metadata.groupBy('content_category').count().orderBy(F.col('count').desc()).show()

    print(f"\n    Document Type Distribution:")
    df_with_metadata.groupBy('document_type').count().orderBy(F.col('count').desc()).show()

    # PPTX stats
    pptx_count = df_with_metadata.filter(F.col('content_category') == 'presentation_slide').count()
    if pptx_count > 0:
        print(f"\n    PPTX Slides: {pptx_count} [NEW!]")
        print(f"\n    PPTX Files:")
        df_with_metadata.filter(F.col('document_type') == 'presentation') \
            .groupBy('source_document').count() \
            .orderBy(F.col('count').desc()) \
            .limit(10).show(truncate=False)

    print(f"\n    Top 10 Table Names:")
    df_with_metadata.filter(F.col('table_name').isNotNull()) \
        .groupBy('table_name').count() \
        .orderBy(F.col('count').desc()) \
        .limit(10).show(truncate=False)

    # Save to new table
    output_table = f"{config['rag_config']['full_namespace']}.poc06_phase1_with_metadata"

    print(f"\n[5] Saving to: {output_table}")
    df_with_metadata.write.format("delta").mode("overwrite").saveAsTable(output_table)
    print(f"    Saved {row_count} chunks")

    # Verify
    print(f"\n[6] Verifying...")
    df_verify = spark.table(output_table)
    verify_count = df_verify.count()
    print(f"    Verified: {verify_count} rows")

    # Check categories
    print(f"\n[7] Category breakdown:")
    table_def_count = df_verify.filter(F.col('content_category') == 'table_definition').count()
    slide_count = df_verify.filter(F.col('content_category') == 'presentation_slide').count()
    image_count = df_verify.filter(F.col('content_category') == 'image_diagram').count()

    print(f"    Table definitions: {table_def_count}")
    print(f"    Presentation slides: {slide_count} [NEW!]")
    print(f"    Images/diagrams: {image_count}")

    if table_def_count > 0:
        print(f"\n    Sample table definitions:")
        df_verify.filter(F.col('content_category') == 'table_definition') \
            .select('table_name', 'section_name', F.substring('content', 1, 200).alias('preview')) \
            .limit(3).show(truncate=False)

    if slide_count > 0:
        print(f"\n    Sample PPTX slides:")
        df_verify.filter(F.col('content_category') == 'presentation_slide') \
            .select('source_document', 'slide_number', 'slide_title', F.substring('content', 1, 150).alias('preview')) \
            .limit(3).show(truncate=False)

    # Update config
    print(f"\n[8] Updating configuration...")
    config['rag_config']['poc06_phase1_metadata_table'] = output_table
    config['rag_config']['poc06_phase1_metadata_count'] = row_count
    config['rag_config']['poc06_phase1_metadata_created_at'] = __import__('time').strftime("%Y-%m-%dT%H:%M:%S")

    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    print("    Updated")

    # Summary
    print("\n" + "=" * 80)
    print("POC06 PHASE 1 - METADATA COMPLETE")
    print("=" * 80)
    print(f"\nTable: {output_table}")
    print(f"Total: {row_count} chunks")
    print(f"Formats: DOCX, PDF, XLSX, PPTX")
    print(f"\nMetadata Added:")
    print(f"  - content_category: presentation_slide, table_definition, table_data, code, process, image, header")
    print(f"  - document_type: presentation, technical_spec, framework_guide, process_guide, etc.")
    print(f"  - section_name: Extracted section/slide heading")
    print(f"  - table_name: Extracted tbl_* table names")
    print(f"\nBreakdown:")
    print(f"  - Table definitions: {table_def_count}")
    print(f"  - PPTX slides: {slide_count} [NEW!]")
    print(f"  - Images/diagrams: {image_count}")
    print(f"\nNext: Create hybrid vector index for all formats")

    spark.stop()


if __name__ == "__main__":
    main()
