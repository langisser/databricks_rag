#!/usr/bin/env python3
"""
Extract images from DataX DOCX - Simple extraction first
"""
import sys
import os
import json
from io import BytesIO
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession

def install_libraries():
    """Install required libraries"""
    print("Installing required libraries...")
    import subprocess
    libs = ["python-docx", "Pillow"]
    for lib in libs:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib, "--quiet"])
        except:
            pass
    print("Libraries installed\n")

def extract_images_from_docx(docx_content):
    """Extract all images from DOCX"""
    from docx import Document

    doc = Document(BytesIO(docx_content))
    images = []
    image_count = 0

    # Extract from inline shapes
    print("  Extracting inline shapes...")
    for shape in doc.inline_shapes:
        try:
            image_blob = shape._inline.graphic.graphicData.pic.blipFill.blip.embed
            image_part = doc.part.related_parts[image_blob]
            image_bytes = image_part.blob

            content_type = image_part.content_type
            ext = content_type.split('/')[-1]
            if ext == 'jpeg':
                ext = 'jpg'

            images.append({
                'id': image_count,
                'data': image_bytes,
                'format': ext,
                'size': len(image_bytes)
            })
            image_count += 1
            print(f"    Image {image_count}: {len(image_bytes):,} bytes ({ext})")

        except Exception as e:
            print(f"    Error: {e}")

    return images

def get_image_description(image_data, image_format):
    """Get basic image info using PIL"""
    try:
        from PIL import Image
        image = Image.open(BytesIO(image_data))

        return {
            'width': image.width,
            'height': image.height,
            'mode': image.mode,
            'aspect_ratio': round(image.width / image.height, 2)
        }
    except:
        return {}

def main():
    print("=" * 80)
    print("EXTRACT IMAGES FROM DataX DOCUMENT")
    print("=" * 80)

    install_libraries()

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

    # Load document
    doc_path = "/Volumes/sandbox/rdt_knowledge/rdt_document/DataX_OPM_RDT_Submission_to_BOT_v1.0.docx"
    print(f"\n[STEP 2] Loading document...")

    binary_df = spark.read.format("binaryFile").load(doc_path)
    doc_data = binary_df.collect()[0]
    doc_content = doc_data['content']

    print(f"  Loaded {len(doc_content):,} bytes")

    # Extract images
    print(f"\n[STEP 3] Extracting images...")
    images = extract_images_from_docx(doc_content)

    print(f"\n  Extracted {len(images)} images")

    # Analyze images
    print(f"\n[STEP 4] Analyzing images...")
    image_chunks = []

    for img in images:
        info = get_image_description(img['data'], img['format'])
        img_id = img['id']

        # Create descriptive content
        content_parts = [
            f"IMAGE {img_id + 1}",
            f"Format: {img['format'].upper()}",
            f"Size: {img['size']:,} bytes",
        ]

        if info:
            content_parts.append(f"Dimensions: {info['width']}x{info['height']}px")
            content_parts.append(f"Aspect ratio: {info['aspect_ratio']}")

            # Guess content type based on dimensions
            if info['aspect_ratio'] > 2:
                content_parts.append("Type: Process Flow Diagram / Horizontal Flowchart")
            elif info['width'] > 1000 and info['height'] > 500:
                content_parts.append("Type: Architecture Diagram / System Overview")
            elif info['width'] < 200 and info['height'] < 200:
                content_parts.append("Type: Icon / Small Graphic")
            else:
                content_parts.append("Type: Diagram / Flowchart")

        content = "\n".join(content_parts)

        print(f"\n  Image {img_id + 1}:")
        print(f"    Format: {img['format']}")
        print(f"    Size: {img['size']:,} bytes")
        if info:
            print(f"    Dimensions: {info['width']}x{info['height']}")

        image_chunks.append({
            'id': f'datax_img_{img_id}',
            'content': content,
            'content_type': 'image_visual',
            'image_format': img['format'],
            'image_size': img['size'],
            'image_width': info.get('width', 0),
            'image_height': info.get('height', 0),
            'char_count': len(content)
        })

    # Save results
    print("\n[STEP 5] Saving results...")
    results_file = os.path.join(os.path.dirname(__file__), 'image_extraction_results.json')
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(image_chunks, f, ensure_ascii=False, indent=2)

    print(f"  Saved to: {results_file}")

    # Summary
    print("\n" + "=" * 80)
    print("IMAGE EXTRACTION COMPLETE")
    print("=" * 80)
    print(f"\nTotal images: {len(images)}")
    print(f"Total size: {sum(img['size'] for img in images):,} bytes")
    print(f"Formats: {set(img['format'] for img in images)}")

    print("\n[RESULT] Image chunks ready to add to vector index")

if __name__ == "__main__":
    main()