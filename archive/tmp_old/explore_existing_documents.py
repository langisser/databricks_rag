#!/usr/bin/env python3
"""
Explore Existing Documents in Unity Catalog Volumes
- Check documents in /Volumes/sandbox/rdt_knowledge/rdt_document
- Analyze file types, sizes, and content structure
- Prepare for POC Iterative 2 real data processing
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession
import json

def main():
    print("Exploring Existing Documents in Unity Catalog Volumes")
    print("=" * 60)

    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)

        print("1. Establishing Databricks Connect session...")

        # Create Spark session
        spark = DatabricksSession.builder.remote(
            host=config['databricks']['host'],
            token=config['databricks']['token'],
            cluster_id=config['databricks']['cluster_id']
        ).getOrCreate()

        print("   SUCCESS: Connected to Databricks")

        # Step 2: Explore the volume directory
        print(f"\n2. Exploring /Volumes/sandbox/rdt_knowledge/rdt_document...")

        volume_path = "/Volumes/sandbox/rdt_knowledge/rdt_document"

        try:
            # List files in the volume
            files_df = spark.sql(f"""
                SELECT
                    path,
                    name,
                    size,
                    modification_time,
                    CASE
                        WHEN name LIKE '%.pdf' THEN 'PDF'
                        WHEN name LIKE '%.docx' THEN 'Word Document'
                        WHEN name LIKE '%.txt' THEN 'Text File'
                        WHEN name LIKE '%.md' THEN 'Markdown'
                        WHEN name LIKE '%.html' THEN 'HTML'
                        WHEN name LIKE '%.json' THEN 'JSON'
                        WHEN name LIKE '%.csv' THEN 'CSV'
                        ELSE 'Other'
                    END as file_type
                FROM dbfs.`{volume_path}`
                WHERE name NOT LIKE '.%'
                ORDER BY modification_time DESC
            """)

            files_list = files_df.collect()

            if files_list:
                print(f"   Found {len(files_list)} files:")
                print(f"   {'Name':<30} {'Type':<15} {'Size (KB)':<12} {'Modified'}")
                print(f"   {'-'*30} {'-'*15} {'-'*12} {'-'*20}")

                total_size = 0
                file_types = {}

                for file_info in files_list:
                    name = file_info['name']
                    file_type = file_info['file_type']
                    size_kb = round(file_info['size'] / 1024, 1) if file_info['size'] else 0
                    mod_time = str(file_info['modification_time'])[:19] if file_info['modification_time'] else 'Unknown'

                    print(f"   {name:<30} {file_type:<15} {size_kb:<12} {mod_time}")

                    total_size += size_kb
                    file_types[file_type] = file_types.get(file_type, 0) + 1

                print(f"\n   Summary:")
                print(f"   Total files: {len(files_list)}")
                print(f"   Total size: {round(total_size, 1)} KB")
                print(f"   File types breakdown:")
                for ftype, count in file_types.items():
                    print(f"     {ftype}: {count} files")

            else:
                print(f"   No files found in {volume_path}")

        except Exception as e:
            print(f"   ERROR: Could not access volume: {e}")
            print(f"   Trying alternative approach...")

            # Alternative: Use dbutils if available
            try:
                # Check if we can access the volume differently
                spark.sql(f"DESCRIBE VOLUME sandbox.rdt_knowledge.rdt_document").show()

                # Try listing with a different approach
                result = spark.sql(f"LIST '{volume_path}'").collect()
                print(f"   Alternative listing found {len(result)} items")

                for item in result[:10]:  # Show first 10
                    print(f"     {item}")

            except Exception as e2:
                print(f"   Alternative approach also failed: {e2}")

        # Step 3: Sample file content analysis
        print(f"\n3. Analyzing file content structure...")

        if 'files_list' in locals() and files_list:
            # Try to read a sample file for content analysis
            sample_files = [f for f in files_list if f['file_type'] in ['Text File', 'Markdown', 'JSON']]

            if sample_files:
                sample_file = sample_files[0]
                sample_path = f"{volume_path}/{sample_file['name']}"

                try:
                    print(f"   Analyzing sample file: {sample_file['name']}")

                    # Read first 1000 characters
                    content_df = spark.read.text(sample_path)
                    content_rows = content_df.take(5)  # Take first 5 lines

                    print(f"   File type: {sample_file['file_type']}")
                    print(f"   Size: {round(sample_file['size'] / 1024, 1)} KB")
                    print(f"   Sample content (first 5 lines):")

                    for i, row in enumerate(content_rows, 1):
                        line = row['value'][:100] + '...' if len(row['value']) > 100 else row['value']
                        print(f"     Line {i}: {line}")

                except Exception as e:
                    print(f"   Could not read sample file: {e}")
            else:
                print(f"   No readable text files found for content analysis")

        # Step 4: Generate processing recommendations
        print(f"\n4. Processing recommendations for POC Iterative 2...")

        if 'files_list' in locals() and files_list:
            recommendations = []

            # Check file types
            if file_types.get('PDF', 0) > 0:
                recommendations.append("- Implement PDF text extraction with PyPDF2 or pdfplumber")
            if file_types.get('Word Document', 0) > 0:
                recommendations.append("- Add DOCX processing with python-docx library")
            if file_types.get('Text File', 0) > 0:
                recommendations.append("- Direct text processing for TXT files")
            if file_types.get('Markdown', 0) > 0:
                recommendations.append("- Markdown parsing with proper formatting preservation")

            # Check total size
            if total_size > 10000:  # > 10MB
                recommendations.append("- Implement batch processing for large dataset")
                recommendations.append("- Add progress tracking and error handling")

            # Check file count
            if len(files_list) > 50:
                recommendations.append("- Use parallel processing for multiple files")
                recommendations.append("- Implement incremental processing")

            if recommendations:
                print(f"   Recommended enhancements:")
                for rec in recommendations:
                    print(f"   {rec}")

            # Generate configuration for POC Iterative 2
            poc2_config = {
                "volume_path": volume_path,
                "total_files": len(files_list),
                "total_size_kb": round(total_size, 1),
                "file_types": file_types,
                "processing_strategy": "batch" if len(files_list) > 10 else "individual",
                "recommended_chunk_size": 1000 if total_size > 5000 else 500,
                "parallel_processing": len(files_list) > 20
            }

            # Save configuration
            config_path = os.path.join(os.path.dirname(__file__), 'poc2_real_data_config.json')
            with open(config_path, 'w') as f:
                json.dump(poc2_config, f, indent=2)

            print(f"\n   Configuration saved to: {config_path}")

        else:
            print(f"   No files found - using default configuration")

        spark.stop()

        print(f"\n" + "=" * 60)
        print("EXPLORATION COMPLETED")
        print("=" * 60)
        print("STATUS: Ready for POC Iterative 2 real data processing")
        print(f"SOURCE: {volume_path}")

        if 'files_list' in locals() and files_list:
            print(f"FILES: {len(files_list)} documents available")
            print("NEXT: Implement advanced chunking and processing")
        else:
            print("FILES: No files found - check volume access")
            print("NEXT: Verify volume permissions and file placement")

        return True

    except Exception as e:
        print(f"\nERROR: Exploration failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)