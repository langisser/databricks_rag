# Data Files for POC Iterative 2

This directory is designed to hold real data files for processing in POC Iterative 2.

## Supported File Formats

**Document Types:**
- `.pdf` - PDF documents
- `.docx` - Microsoft Word documents
- `.txt` - Plain text files
- `.md` - Markdown files
- `.html` - HTML files
- `.json` - JSON structured data

## File Organization

**Recommended Structure:**
```
data_files/
├── documents/          # Main document collection
│   ├── policies/       # Company policies
│   ├── manuals/        # User manuals and guides
│   ├── faqs/          # FAQ documents
│   └── technical/     # Technical documentation
├── raw/               # Raw, unprocessed files
└── processed/         # Processed and cleaned files
```

## Processing Pipeline

**Workflow:**
1. **Upload**: Place files in appropriate subdirectories
2. **Extraction**: Text extraction and cleaning
3. **Chunking**: Advanced semantic chunking
4. **Embedding**: Vector embedding generation
5. **Indexing**: Vector search index creation

## File Requirements

**Best Practices:**
- Use descriptive filenames
- Ensure files are readable and not corrupted
- Include metadata when possible
- Organize by content type or department

**File Size Considerations:**
- Individual files: Up to 50MB recommended
- Total dataset: Scalable to GB-level processing
- Chunking will optimize for vector search performance

## Usage Instructions

1. **Add Files**: Copy your real data files to appropriate subdirectories
2. **Run Processing**: Use POC Iterative 2 scripts to process files
3. **Validate Results**: Check extraction and chunking quality
4. **Index Creation**: Generate optimized vector search indexes

Ready to accept real data files for advanced RAG knowledge base processing.