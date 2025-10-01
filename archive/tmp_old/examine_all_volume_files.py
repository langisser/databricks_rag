#!/usr/bin/env python3
"""
Examine all files in Volume and categorize by file type
"""
import sys
import os
import json
from collections import defaultdict
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession

def get_file_extension(filename):
    """Get file extension in lowercase"""
    return os.path.splitext(filename)[1].lower()

def main():
    print("=" * 80)
    print("EXAMINE ALL FILES IN VOLUME")
    print("=" * 80)

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Connect
    print("\n[STEP 1] Connecting to Databricks...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()
    print("  Connected")

    # Volume path
    volume_path = "/Volumes/sandbox/rdt_knowledge/rdt_document"

    print(f"\n[STEP 2] Scanning Volume: {volume_path}")
    files_result = spark.sql(f"LIST '{volume_path}'")
    files_df = files_result.toPandas()

    print(f"  Total files: {len(files_df)}")

    # Categorize by file type
    print("\n[STEP 3] Categorizing by file type...")

    file_categories = defaultdict(list)
    total_size = 0

    for idx, row in files_df.iterrows():
        filename = row['name']
        filepath = row['path']
        filesize = row['size']
        total_size += filesize

        ext = get_file_extension(filename)

        file_categories[ext].append({
            'name': filename,
            'path': filepath,
            'size': filesize,
            'size_mb': round(filesize / 1024 / 1024, 2)
        })

    # Display summary
    print("\n[STEP 4] File Type Summary:")
    print("-" * 80)
    print(f"{'File Type':<15} {'Count':<10} {'Total Size':<20} {'Files'}")
    print("-" * 80)

    for ext in sorted(file_categories.keys()):
        files = file_categories[ext]
        count = len(files)
        type_size = sum(f['size'] for f in files)
        type_size_mb = type_size / 1024 / 1024

        print(f"{ext:<15} {count:<10} {type_size_mb:>10.2f} MB       {', '.join([f['name'][:30] for f in files[:2]])}")

    print("-" * 80)
    print(f"{'TOTAL':<15} {len(files_df):<10} {total_size / 1024 / 1024:>10.2f} MB")

    # Detailed breakdown
    print("\n[STEP 5] Detailed File List:")

    for ext in sorted(file_categories.keys()):
        files = file_categories[ext]
        print(f"\n{ext.upper()} Files ({len(files)}):")

        for f in files:
            print(f"  - {f['name']}")
            print(f"    Size: {f['size_mb']} MB")
            print(f"    Path: {f['path']}")

    # Recommended extraction strategy
    print("\n" + "=" * 80)
    print("RECOMMENDED EXTRACTION STRATEGY")
    print("=" * 80)

    strategy = {
        '.docx': {
            'library': 'python-docx',
            'extract': ['text', 'tables', 'images'],
            'features': 'Paragraphs, section headers, table structure, inline images'
        },
        '.xlsx': {
            'library': 'openpyxl, pandas',
            'extract': ['text', 'tables', 'formulas'],
            'features': 'Multiple sheets, cell values, formulas, charts metadata'
        },
        '.pdf': {
            'library': 'pdfplumber, PyPDF2',
            'extract': ['text', 'tables', 'images'],
            'features': 'Text extraction, table detection, image extraction, OCR if needed'
        },
        '.pptx': {
            'library': 'python-pptx',
            'extract': ['text', 'images', 'notes'],
            'features': 'Slides, text boxes, images, speaker notes'
        },
        '.drawio': {
            'library': 'xml.etree, custom parser',
            'extract': ['text', 'diagrams'],
            'features': 'Multiple tabs/pages, diagram structure, text labels'
        },
        '.txt': {
            'library': 'built-in',
            'extract': ['text'],
            'features': 'Plain text content'
        }
    }

    for ext in sorted(file_categories.keys()):
        if ext in strategy:
            strat = strategy[ext]
            print(f"\n{ext.upper()}:")
            print(f"  Library: {strat['library']}")
            print(f"  Extract: {', '.join(strat['extract'])}")
            print(f"  Features: {strat['features']}")
        else:
            print(f"\n{ext.upper()}:")
            print(f"  Status: Unsupported - needs custom handler")

    # Save analysis
    print("\n[STEP 6] Saving analysis...")
    analysis = {
        'total_files': len(files_df),
        'total_size_mb': round(total_size / 1024 / 1024, 2),
        'file_types': {ext: len(files) for ext, files in file_categories.items()},
        'files_by_type': {ext: [f['name'] for f in files] for ext, files in file_categories.items()},
        'extraction_strategy': strategy
    }

    output_file = os.path.join(os.path.dirname(__file__), 'volume_files_analysis.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, ensure_ascii=False, indent=2)

    print(f"  Saved to: {output_file}")

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()