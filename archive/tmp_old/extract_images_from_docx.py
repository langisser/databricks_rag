#!/usr/bin/env python3
"""
Extract images from DataX DOCX and process with OCR
Uses python-docx to extract images and EasyOCR for text extraction
"""
import sys
import os
import json
import tempfile
from io import BytesIO
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'helper_function'))

from databricks.connect import DatabricksSession

def install_libraries():
    """Install required libraries"""
    print("Installing required libraries...")
    import subprocess
    libs = ["python-docx", "Pillow", "easyocr", "opencv-python"]
    for lib in libs:
        try:
            print(f"  Installing {lib}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", lib, "--quiet"])
        except:
            pass
    print("Libraries installed\n")

def extract_images_from_docx(docx_content):
    """Extract all images from DOCX with their relationships"""
    from docx import Document
    from docx.oxml import parse_xml
    import zipfile

    # Load document
    doc = Document(BytesIO(docx_content))

    # Extract images from document.xml.rels
    images = []
    image_count = 0

    # Method 1: Extract from inline shapes
    print("  Method 1: Extracting inline shapes...")
    for shape in doc.inline_shapes:
        try:
            # Get image data
            image_blob = shape._inline.graphic.graphicData.pic.blipFill.blip.embed
            image_part = doc.part.related_parts[image_blob]
            image_bytes = image_part.blob

            # Determine format
            content_type = image_part.content_type
            ext = content_type.split('/')[-1]
            if ext == 'jpeg':
                ext = 'jpg'

            images.append({
                'id': f'image_{image_count}',
                'data': image_bytes,
                'format': ext,
                'size': len(image_bytes),
                'source': 'inline_shape'
            })
            image_count += 1
            print(f"    Found image {image_count}: {len(image_bytes):,} bytes ({ext})")

        except Exception as e:
            print(f"    Error extracting inline shape: {e}")

    # Method 2: Extract from document relationships (catches embedded images)
    print("\n  Method 2: Extracting from relationships...")
    try:
        for rel in doc.part.rels.values():
            if "image" in rel.target_ref:
                try:
                    image_bytes = rel.target_part.blob
                    content_type = rel.target_part.content_type
                    ext = content_type.split('/')[-1]
                    if ext == 'jpeg':
                        ext = 'jpg'

                    # Check if already extracted
                    already_exists = any(img['data'] == image_bytes for img in images)
                    if not already_exists:
                        images.append({
                            'id': f'image_{image_count}',
                            'data': image_bytes,
                            'format': ext,
                            'size': len(image_bytes),
                            'source': 'relationship'
                        })
                        image_count += 1
                        print(f"    Found image {image_count}: {len(image_bytes):,} bytes ({ext})")
                except Exception as e:
                    print(f"    Error extracting relationship image: {e}")
    except Exception as e:
        print(f"    Error processing relationships: {e}")

    return images

def extract_text_from_image_with_ocr(image_data, image_format):
    """Extract text from image using EasyOCR"""
    try:
        import easyocr
        from PIL import Image
        import numpy as np
        import warnings
        warnings.filterwarnings('ignore')

        # Initialize OCR reader (English and Thai)
        if not hasattr(extract_text_from_image_with_ocr, 'reader'):
            print("    Initializing EasyOCR (this may take a moment)...")
            extract_text_from_image_with_ocr.reader = easyocr.Reader(['en', 'th'], gpu=False, verbose=False)

        # Load image
        image = Image.open(BytesIO(image_data))

        # Convert to RGB if needed
        if image.mode != 'RGB':
            image = image.convert('RGB')

        # Convert to numpy array
        image_np = np.array(image)

        # Perform OCR
        results = extract_text_from_image_with_ocr.reader.readtext(image_np)

        # Extract text with confidence
        extracted_texts = []
        for (bbox, text, confidence) in results:
            if confidence > 0.3:  # Filter low confidence results
                extracted_texts.append({
                    'text': text,
                    'confidence': round(confidence, 2)
                })

        return extracted_texts

    except Exception as e:
        print(f"    OCR error: {e}")
        return []

def main():
    print("=" * 80)
    print("EXTRACT AND PROCESS IMAGES FROM DataX DOCUMENT")
    print("=" * 80)

    # Install libraries
    install_libraries()

    # Load config
    config_path = os.path.join(os.path.dirname(__file__), '..', 'databricks_helper', 'databricks_config', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Connect to Databricks
    print("\n[STEP 1] Connecting to Databricks...")
    spark = DatabricksSession.builder.remote(
        host=config['databricks']['host'],
        token=config['databricks']['token'],
        cluster_id=config['databricks']['cluster_id']
    ).getOrCreate()
    print("  [SUCCESS] Connected")

    # Document path
    doc_path = "/Volumes/sandbox/rdt_knowledge/rdt_document/DataX_OPM_RDT_Submission_to_BOT_v1.0.docx"

    print(f"\n[STEP 2] Loading document from Volume...")
    print(f"  Path: {doc_path}")

    # Read document as binary
    binary_df = spark.read.format("binaryFile").load(doc_path)
    doc_data = binary_df.collect()[0]
    doc_content = doc_data['content']

    print(f"  [SUCCESS] Loaded {len(doc_content):,} bytes")

    # Extract images
    print(f"\n[STEP 3] Extracting images...")
    images = extract_images_from_docx(doc_content)

    print(f"\n  [SUCCESS] Extracted {len(images)} images")

    # Process each image with OCR
    print(f"\n[STEP 4] Processing images with OCR...")

    image_chunks = []
    for idx, image in enumerate(images):
        print(f"\n  Image {idx + 1}:")
        print(f"    Size: {image['size']:,} bytes")
        print(f"    Format: {image['format']}")
        print(f"    Source: {image['source']}")

        # Perform OCR
        print(f"    Running OCR...")
        ocr_results = extract_text_from_image_with_ocr(image['data'], image['format'])

        if ocr_results:
            print(f"    [SUCCESS] Found {len(ocr_results)} text elements")

            # Build text content
            text_content = '\n'.join([f"- {r['text']} (confidence: {r['confidence']})"
                                     for r in ocr_results])

            # Show sample
            if ocr_results:
                print(f"    Sample: {ocr_results[0]['text'][:100]}...")

            image_chunks.append({
                'id': f'datax_img_{idx}',
                'content': f"IMAGE {idx + 1} - Extracted Text:\n{text_content}",
                'content_type': 'image_ocr',
                'image_format': image['format'],
                'image_size': image['size'],
                'ocr_elements': len(ocr_results),
                'char_count': len(text_content)
            })
        else:
            print(f"    [INFO] No text detected in image")
            image_chunks.append({
                'id': f'datax_img_{idx}',
                'content': f"IMAGE {idx + 1} - Diagram/Flowchart (no text detected)",
                'content_type': 'image_diagram',
                'image_format': image['format'],
                'image_size': image['size'],
                'ocr_elements': 0,
                'char_count': 50
            })

    # Summary
    print("\n" + "=" * 80)
    print("IMAGE EXTRACTION COMPLETE")
    print("=" * 80)
    print(f"\nTotal images extracted: {len(images)}")
    print(f"Images with OCR text: {sum(1 for c in image_chunks if c['ocr_elements'] > 0)}")
    print(f"Total OCR elements: {sum(c['ocr_elements'] for c in image_chunks)}")
    print(f"Total characters: {sum(c['char_count'] for c in image_chunks):,}")

    # Save results
    print("\n[STEP 5] Saving results...")
    results_file = os.path.join(os.path.dirname(__file__), 'image_extraction_results.json')
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(image_chunks, f, ensure_ascii=False, indent=2)

    print(f"  [SUCCESS] Results saved to: {results_file}")

    print("\n[NEXT] Run update script to add images to vector index")

if __name__ == "__main__":
    main()