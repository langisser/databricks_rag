# Databricks Helper Integration Guide

This document explains how to use the databricks_helper submodule for HTML export and Databricks Connect functionality in your RAG project.

## Overview

The databricks_helper submodule provides two key capabilities:
1. **HTML Log Export**: Export Databricks job run logs in HTML format for documentation
2. **Databricks Connect**: Execute SQL queries and notebooks directly on Databricks clusters with real-time results

## Setup Instructions

### 1. Configure Databricks Helper

```bash
# Navigate to the submodule configuration directory
cd databricks_helper/databricks_config

# Run the setup script to create config.json from template
python setup_config.py

# Edit the generated config.json with your credentials
# (This file is git-ignored for security)
```

### 2. Required Configuration

Edit `databricks_helper/databricks_config/config.json`:

```json
{
  "databricks": {
    "host": "https://your-workspace.databricks.com/",
    "token": "your-personal-access-token",
    "cluster_id": "your-cluster-id",
    "warehouse_id": "your-warehouse-id"
  },
  "settings": {
    "log_level": "INFO",
    "timeout_seconds": 300,
    "max_output_size": "5MB"
  }
}
```

### 3. Get Your Credentials

#### Databricks Host
Your workspace URL (e.g., `https://adb-123456789.12.azuredatabricks.net/`)

#### Personal Access Token
1. Go to Databricks workspace → User Settings → Access Tokens
2. Click "Generate New Token"
3. Copy the token value

#### Cluster ID
1. Go to Compute → Clusters
2. Select your cluster
3. Copy the cluster ID from the URL or cluster details

#### Warehouse ID (Optional)
1. Go to SQL → Warehouses
2. Select your warehouse
3. Copy the warehouse ID from the URL

### 4. Install Dependencies

```bash
cd databricks_helper/helper_function
pip install -r requirements.txt
pip install databricks-connect==14.3.0
```

## Usage Examples

### HTML Log Export

Export Databricks job run logs to HTML format for documentation purposes:

```python
# Import the HTML export function
from databricks_helper.helper_function.databricks_html_log_export import export_html_log

# Export a specific job run to HTML
html_file = export_html_log("run_id_123456")
print(f"HTML log exported to: {html_file}")

# The HTML file will be saved in databricks_helper/tmp/ directory
# Perfect for documentation and sharing execution results
```

### Live SQL Execution with Databricks Connect

Execute SQL queries directly on your Databricks cluster and get real-time results:

```python
# Import the SQL execution function
from databricks_helper.helper_function.databricks_helper import execute_sql_query

# Execute a SQL query and get structured results
result = execute_sql_query("SELECT COUNT(*) as total_rows FROM my_table")

# Access different parts of the result
print("Query Results:", result['result'])      # Actual query output
print("Stdout Logs:", result['stdout'])        # Execution logs
print("Stderr Logs:", result['stderr'])        # Error messages
print("Execution Time:", result['timestamp'])  # When it was executed
```

### Advanced Usage - DatabricksExecutor

For more complex operations, use the DatabricksExecutor class:

```python
from databricks_helper.helper_function.databricks_helper import DatabricksExecutor

# Create an executor instance
executor = DatabricksExecutor("databricks_helper/databricks_config/config.json")

# Execute SQL with live output capture
sql_result = executor.execute_sql("""
    SELECT
        category,
        COUNT(*) as count,
        AVG(price) as avg_price
    FROM products
    GROUP BY category
    ORDER BY count DESC
""")

# Execute a notebook with parameters
notebook_result = executor.execute_notebook(
    "/Shared/data_processing_notebook",
    parameters={
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "output_table": "processed_data"
    }
)

# Get job run logs for analysis
logs = executor.get_job_run_logs(run_id=123456)
print("Job execution logs:", logs['stdout'])
```

## Integration with RAG Pipeline

### Use Case 1: Data Pipeline Monitoring

Monitor your RAG data pipeline execution and export results for analysis:

```python
# Execute document processing pipeline
pipeline_result = execute_sql_query("""
    INSERT INTO rag_knowledge_base.documents.document_chunks
    SELECT
        uuid() as chunk_id,
        document_id,
        chunk_text,
        chunk_index,
        metadata,
        current_timestamp()
    FROM processed_documents
""")

# Export the execution log for documentation
if pipeline_result['status'] == 'success':
    html_log = export_html_log(pipeline_result['run_id'])
    print(f"Pipeline execution documented: {html_log}")
```

### Use Case 2: Vector Search Index Monitoring

Monitor vector search index creation and performance:

```python
# Check vector search index status
index_status = execute_sql_query("""
    SELECT
        index_name,
        status,
        num_documents,
        last_updated
    FROM system.vector_search.indexes
    WHERE index_name = 'rag_knowledge_base.documents.chunk_index'
""")

print("Vector Search Index Status:", index_status['result'])
```

### Use Case 3: RAG Performance Analytics

Analyze RAG query performance and accuracy:

```python
# Get RAG performance metrics
performance_metrics = execute_sql_query("""
    SELECT
        DATE(query_timestamp) as date,
        COUNT(*) as total_queries,
        AVG(response_time_ms) as avg_response_time,
        AVG(relevance_score) as avg_relevance,
        COUNT(CASE WHEN user_feedback = 'positive' THEN 1 END) as positive_feedback
    FROM rag_query_logs
    WHERE query_timestamp >= current_date() - INTERVAL 7 DAYS
    GROUP BY DATE(query_timestamp)
    ORDER BY date DESC
""")

# Export performance report
performance_html = export_html_log(performance_metrics['run_id'])
print(f"Performance report exported: {performance_html}")
```

## File Structure After Integration

```
databricks_rag/
├── planning/
│   └── plan_version_01.md
├── databricks_helper/                    # Submodule
│   ├── helper_function/
│   │   ├── databricks_helper.py         # Main execution engine
│   │   ├── databricks_html_log_export.py   # HTML export functionality
│   │   ├── demo/
│   │   │   ├── demo_live_execution.py   # Live execution demo
│   │   │   └── demo_html_export.py      # HTML export demo
│   │   └── requirements.txt
│   ├── databricks_config/
│   │   ├── config.template.json         # Template (tracked)
│   │   ├── config.json                  # Your config (git-ignored)
│   │   └── setup_config.py              # Setup helper
│   ├── databricks_cli/                  # CLI tools
│   └── tmp/                             # Output directory
├── INTEGRATION.md                       # This file
└── README.md
```

## Security Considerations

1. **Configuration Security**: The `config.json` file containing your credentials is automatically git-ignored
2. **Token Management**: Store personal access tokens securely and rotate them regularly
3. **Cluster Access**: Ensure your clusters have appropriate access controls and security configurations
4. **Network Security**: Use secure connections and follow your organization's network policies

## Troubleshooting

### Common Issues

#### Connection Errors
```python
# Test your connection
from databricks_helper.helper_function.databricks_helper import DatabricksExecutor

try:
    executor = DatabricksExecutor("databricks_helper/databricks_config/config.json")
    result = executor.execute_sql("SELECT 1 as test")
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")
```

#### Version Compatibility
- Ensure `databricks-connect==14.3.0` matches your Databricks Runtime version
- Update the version if you're using a different runtime

#### Cluster Issues
- Verify your cluster is running or can auto-start
- Check cluster permissions and access policies
- Ensure the cluster ID in config.json is correct

### Getting Help

1. Check the databricks_helper documentation in the submodule
2. Review the demo scripts for working examples
3. Verify your Databricks workspace configuration
4. Test with simple queries before complex operations

## Best Practices

1. **Configuration Management**: Keep separate config files for different environments (dev/prod)
2. **Error Handling**: Always wrap database operations in try-catch blocks
3. **Resource Management**: Close connections and clean up resources after use
4. **Monitoring**: Use the HTML export feature to document important operations
5. **Testing**: Test connectivity and basic operations before deploying complex pipelines

This integration enables powerful capabilities for monitoring, debugging, and documenting your RAG knowledge base implementation on Databricks.