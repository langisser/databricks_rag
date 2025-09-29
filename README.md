# Databricks RAG Knowledge Base

A comprehensive RAG (Retrieval-Augmented Generation) application built for Databricks with integrated helper functions for HTML export and command execution via Databricks Connect.

## Project Structure

```
databricks_rag/
├── planning/                      # Implementation plans and documentation
│   └── plan_version_01.md        # Comprehensive implementation plan v1.0
├── databricks_helper/             # Submodule for Databricks utilities
│   ├── helper_function/           # Core helper functions
│   │   ├── databricks_helper.py            # Main helper module
│   │   ├── databricks_html_log_export.py   # HTML log export functionality
│   │   └── demo/                           # Demo and example scripts
│   ├── databricks_config/         # Configuration management
│   └── databricks_cli/            # CLI tools
├── .gitmodules                   # Git submodule configuration
└── README.md                     # This file
```

## Features

### RAG Knowledge Base
- **Vector Search Integration**: Databricks Vector Search for document indexing
- **AI Playground Integration**: No-code chatbot creation via Databricks Playground
- **Document Processing**: Support for PDFs, text files, and unstructured data
- **Citation Support**: Automatic source citation in responses
- **Real-time Updates**: Automatic index updates when documents change

### Databricks Helper Integration
- **HTML Log Export**: Export Databricks job run logs in HTML format
- **Live Execution**: Real-time SQL query and notebook execution
- **Databricks Connect**: Direct cluster connection for immediate results
- **Output Capture**: Structured JSON results with stdout/stderr capture
- **Configuration Management**: Secure credential handling

## Quick Start

### 1. Clone with Submodules
```bash
git clone --recursive https://github.com/langisser/databricks_rag.git
cd databricks_rag
```

### 2. Initialize Submodule (if not cloned with --recursive)
```bash
git submodule update --init --recursive
```

### 3. Setup Databricks Helper
```bash
cd databricks_helper/databricks_config
python setup_config.py
# Edit config.json with your Databricks credentials
```

### 4. Follow Implementation Plan
See `planning/plan_version_01.md` for detailed step-by-step implementation guide.

## Implementation Timeline

- **Phase 1**: Environment Setup (Day 1)
- **Phase 2**: Data Preparation (Day 1-2)
- **Phase 3**: Vector Search Implementation (Day 2-3)
- **Phase 4**: RAG Chain Development (Day 3)
- **Phase 5**: AI Playground Integration (Day 4)
- **Phase 6**: Evaluation and Monitoring (Day 4-5)

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Serverless compute configured
- Personal access tokens for authentication
- Sample documents for knowledge base

## Usage Examples

### HTML Log Export
```python
from databricks_helper.helper_function.databricks_html_log_export import export_html_log
html_file = export_html_log("run_id_123")
```

### Live SQL Execution
```python
from databricks_helper.helper_function.databricks_helper import execute_sql_query
result = execute_sql_query("SELECT COUNT(*) FROM my_table")
print(result['result'])
```

## Documentation

- **Implementation Plan**: `planning/plan_version_01.md` - Complete implementation guide
- **Databricks Helper**: `databricks_helper/helper_function/README.md` - Helper function documentation
- **Configuration**: `databricks_helper/CLAUDE.md` - Setup and usage guidance

## Contributing

1. Update implementation plan versions when making architectural changes
2. Maintain submodule synchronization when updating databricks_helper
3. Follow security best practices for credential management
4. Document all new features in the appropriate plan version

## License

This project integrates multiple components with their respective licenses.