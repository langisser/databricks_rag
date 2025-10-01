# POC Iterative 03 - Reprocessing Guide

## Overview
Complete pipeline for processing all documents in Volume and creating comprehensive vector search index.

---

## 📋 Processing Steps

### Step 1: Examine Volume Files
**Script**: `01_examine_volume_files.py`

**Purpose**: Analyze all files in Volume and categorize by type

**Run**:
```bash
python 01_examine_volume_files.py
```

**Output**:
- File type analysis
- File size summary
- Extraction strategy recommendations
- Saves: `volume_files_analysis.json`

**Expected Results**:
- 24 total files identified
- 5 file types: DOCX (17), XLSX (3), PDF (1), PPTX (2), DRAWIO (1)
- 107 MB total size

---

### Step 2: Process All Documents
**Script**: `02_process_all_documents.py`

**Purpose**: Extract text, tables, and images from all documents

**Run**:
```bash
python 02_process_all_documents.py
```

**What It Does**:
- Processes each file based on format
- DOCX: Extracts text, tables, images with section detection
- XLSX: Processes all sheets separately
- PDF: Extracts text and tables by page
- PPTX: Extracts slides, tables, and image metadata
- DRAWIO: Extracts diagram text and labels
- Creates Delta table with all chunks

**Output Table**: `sandbox.rdt_knowledge.all_documents_multimodal`

**Expected Results**:
- ~22 files processed successfully
- ~1,386 chunks extracted
- Content types: text, table, pdf_text, pdf_table, pptx_slide, excel_sheet, etc.

**Processing Time**: ~5-10 minutes

---

### Step 3: Create Vector Search Index
**Script**: `03_create_vector_index.py`

**Purpose**: Create searchable vector index from processed chunks

**Run**:
```bash
python 03_create_vector_index.py
```

**What It Does**:
- Verifies Delta table
- Enables Change Data Feed
- Creates vector search index with GTE-large-en embeddings
- Waits for index to sync

**Output Index**: `sandbox.rdt_knowledge.all_documents_index`

**Expected Results**:
- Index created and ONLINE
- 1,386 chunks indexed
- Ready for AI Playground queries

**Processing Time**: ~3-5 minutes

---

## 🔄 Complete Reprocessing

To reprocess everything from scratch:

```bash
# Step 1: Examine files (optional - for analysis)
python 01_examine_volume_files.py

# Step 2: Process all documents
python 02_process_all_documents.py

# Step 3: Create vector index
python 03_create_vector_index.py
```

**Total Time**: ~10-15 minutes

---

## 📊 Expected Output

### Delta Table
- **Name**: `sandbox.rdt_knowledge.all_documents_multimodal`
- **Rows**: 1,386
- **Schema**:
  - `id`: Unique chunk identifier
  - `content`: Extracted text/table/image content
  - `content_type`: Type (text, table, pptx_slide, etc.)
  - `section`: Section/location within document
  - `document_name`: Source document filename
  - `char_count`: Content length

### Vector Search Index
- **Name**: `sandbox.rdt_knowledge.all_documents_index`
- **Endpoint**: `rag-knowledge-base-endpoint`
- **Embedding Model**: `databricks-gte-large-en`
- **Status**: ONLINE
- **Chunks**: 1,386

---

## 🎯 Testing

### In AI Playground

1. Navigate to Databricks Workspace
2. Open AI Playground
3. Select Retrieval tab
4. Choose index: `sandbox.rdt_knowledge.all_documents_index`

### Sample Queries

**Cross-document search**:
```
What is the BOT submission process?
```

**Document discovery**:
```
Which document describes the schema sync process?
```

**Content-specific**:
```
Show me validation framework tables
```

**Format-specific**:
```
What is in the CardX RDT Platform presentation?
```

---

## 🔧 Troubleshooting

### Script Fails on File Read
**Error**: "Cluster id is required"
**Solution**: Check cluster status first
```bash
cd ../tmp
python check_cluster_status.py
```

### Corrupted Files
**Issue**: Some .v1.01 files may fail with "File is not a zip file"
**Solution**: Process continues with other files. Use .v1.0 versions instead.

### Index Creation Timeout
**Issue**: Index takes too long to sync
**Solution**: Wait 5-10 minutes for large datasets. Check Databricks UI for status.

### Missing Libraries
**Issue**: Import errors
**Solution**: Scripts auto-install required libraries. Ensure internet connection.

---

## 📦 Required Libraries

Libraries are auto-installed by scripts:
- `python-docx` - Word documents
- `openpyxl` - Excel files
- `pandas` - Data manipulation
- `pdfplumber` - PDF processing
- `PyPDF2` - PDF backup
- `python-pptx` - PowerPoint files
- `Pillow` - Image analysis

---

## 🎨 Customization

### Modify Chunk Size
Edit `02_process_all_documents.py`:
```python
# Line ~50 in process_docx function
if sum(len(t) for t in current_text) > 2000:  # Change 2000 to desired size
```

### Add New File Type
Edit `02_process_all_documents.py`:
1. Add processor function (e.g., `process_txt()`)
2. Add to main processing logic:
```python
elif ext == '.txt':
    chunks = process_txt(file_content, filename)
```

### Change Index Name
Edit `03_create_vector_index.py`:
```python
index_name = f"{namespace}.your_custom_index_name"
```

---

## 📝 File Descriptions

### 01_examine_volume_files.py
- Scans Volume for all files
- Analyzes file types and sizes
- Recommends extraction strategies
- Creates analysis report

### 02_process_all_documents.py
- Main multi-format processor
- Format-specific extraction functions
- Creates Delta table with chunks
- Handles errors gracefully

### 03_create_vector_index.py
- Creates vector search index
- Configures embeddings
- Monitors sync status
- Provides test queries

---

## 🚀 Quick Start

**First time setup**:
```bash
cd "c:/Users/KHACHORNPOPWONGPHAKA/OneDrive - Inteltion Co., Ltd/ClaudeCode02/Project/databricks_rag/poc_iterative_03"

# Process everything
python 02_process_all_documents.py
python 03_create_vector_index.py
```

**Reprocess after adding new files to Volume**:
```bash
python 02_process_all_documents.py  # mode="overwrite" will replace table
python 03_create_vector_index.py   # Recreates index with new data
```

---

## 📈 Performance

| Step | Time | Output |
|------|------|--------|
| Examine files | ~5 sec | Analysis report |
| Process documents | ~5-10 min | 1,386 chunks |
| Create index | ~3-5 min | Vector index |
| **Total** | **~10-15 min** | **Production-ready index** |

---

## ✅ Success Criteria

After running all scripts:

1. ✅ Delta table exists with ~1,386 rows
2. ✅ Vector index status: ONLINE
3. ✅ Content types include: text, table, pptx_slide, excel_sheet, pdf_text
4. ✅ Document names preserved in metadata
5. ✅ AI Playground queries return relevant results

---

## 📞 Support

For issues or questions:
1. Check script output for detailed error messages
2. Verify cluster is running: `python ../tmp/check_cluster_status.py`
3. Review Databricks logs for index sync status
4. Check comprehensive summary: `COMPREHENSIVE_MULTIFORMAT_SUMMARY.md`

---

**Last Updated**: 2025-09-30
**Status**: Production Ready
**Version**: POC Iterative 03