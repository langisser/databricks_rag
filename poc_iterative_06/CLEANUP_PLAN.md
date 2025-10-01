# POC06 - Environment Cleanup & Script Organization

## Current State Analysis

### POC Iterations Summary
- **POC01**: Initial end-to-end RAG setup (5 scripts)
- **POC02**: Translation optimization & Genie views (5 scripts)
- **POC03**: Multi-format document processing (3 scripts)
- **POC04**: Docling extraction & bilingual optimization (4 scripts)
- **POC05**: Hybrid search + metadata filtering + all formats (15 scripts)

### Issues Identified
1. **Duplicate Scripts**: Multiple versions of similar functionality across POCs
2. **Tmp Folder Clutter**: 80+ temporary scripts, many outdated
3. **No Clear Production Path**: Scripts scattered across POC folders
4. **Missing Documentation**: No clear guide on which scripts to use
5. **Inconsistent Naming**: Mix of numbered and descriptive names

## Proposed New Structure

```
databricks_rag/
├── production/                    # Production-ready scripts
│   ├── data_processing/
│   │   ├── process_documents.py       # Multi-format processing (from POC05)
│   │   ├── add_metadata.py            # Metadata enrichment
│   │   └── upload_to_volume.py        # Document upload utility
│   ├── vector_index/
│   │   ├── create_index.py            # Vector index creation
│   │   ├── check_status.py            # Index status monitoring
│   │   └── benchmark.py               # Performance benchmarking
│   └── README.md                      # Production usage guide
│
├── utilities/                     # Reusable utility scripts
│   ├── cluster_management.py          # Cluster status & selection
│   ├── volume_operations.py           # Volume file operations
│   └── config_validator.py            # Configuration validation
│
├── archive/                       # Historical POC scripts
│   ├── poc_01_initial/
│   ├── poc_02_optimization/
│   ├── poc_03_multiformat/
│   ├── poc_04_docling/
│   └── poc_05_hybrid_search/
│
├── experiments/                   # Active experimentation
│   └── poc_06_cleanup/                # Current POC
│
├── docs/                          # Documentation
│   ├── ARCHITECTURE.md                # System architecture
│   ├── PRODUCTION_GUIDE.md            # How to use production scripts
│   └── POC_HISTORY.md                 # Evolution & learnings
│
└── scripts/                       # Legacy location (to be migrated)
```

## Migration Plan

### Phase 1: Identify Production Scripts
**From POC05 (Current Best)**:
- ✅ `06_process_all_formats_v02.py` → `production/data_processing/process_documents.py`
- ✅ `07_add_metadata_all_formats.py` → `production/data_processing/add_metadata.py`
- ✅ `08_create_all_formats_index.py` → `production/vector_index/create_index.py`
- ✅ `09_benchmark_all_formats.py` → `production/vector_index/benchmark.py`

**From Scripts Folder**:
- ✅ `upload_documents_to_volume.py` → `production/data_processing/upload_to_volume.py`

**From Tmp Folder**:
- ✅ `check_cluster_status.py` → `utilities/cluster_management.py`
- ✅ `check_index_status.py` → `production/vector_index/check_status.py`
- ✅ `find_cluster.py` → merge into `utilities/cluster_management.py`

### Phase 2: Archive Old POCs
Move POC01-05 folders to `archive/` with README explaining:
- What each POC achieved
- Key learnings
- Why superseded
- Reference commits

### Phase 3: Clean Tmp Folder
**Keep (move to utilities)**:
- Cluster management scripts
- Volume operation scripts
- Useful test utilities

**Archive (move to archive/tmp)**:
- Old validation scripts
- Duplicate functionality
- Superseded implementations

**Delete**:
- Empty/incomplete scripts
- Failed experiments
- Benchmark result JSONs (keep in config.json)

### Phase 4: Documentation
Create comprehensive docs:
1. **ARCHITECTURE.md**: System design, data flow, components
2. **PRODUCTION_GUIDE.md**: How to run production pipeline
3. **POC_HISTORY.md**: Evolution from POC01 to POC05
4. **CLEANUP_HISTORY.md**: This cleanup process

## Success Criteria
- [ ] Clear production path: Single entry point for each operation
- [ ] All POC history preserved in archive/
- [ ] Tmp folder reduced to <10 active utilities
- [ ] Complete documentation for production usage
- [ ] Updated .gitignore for new structure
- [ ] All scripts tested and verified working

## Execution Steps
1. Create new folder structure
2. Copy (not move) production scripts to new locations
3. Test production scripts in new locations
4. Archive old POC folders
5. Clean tmp folder
6. Write documentation
7. Update .gitignore
8. Final verification & commit

## Timeline
- Analysis & Planning: Complete
- Folder Structure: 30 min
- Script Migration: 1 hour
- Testing: 30 min
- Documentation: 1 hour
- **Total**: ~3 hours
