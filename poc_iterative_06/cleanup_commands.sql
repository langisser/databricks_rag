-- ============================================================================
-- DATABASE CLEANUP COMMANDS - sandbox.rdt_knowledge
-- ============================================================================
-- KEEP ONLY: POC05_v02 All Formats (3 tables + 1 index)
-- REMOVE: Everything else (45 tables + old indexes)
--
-- IMPORTANT: Execute these commands manually and carefully!
-- Review each section before running.
-- ============================================================================

-- ============================================================================
-- TABLES TO KEEP (DO NOT DELETE)
-- ============================================================================
-- poc05_v02_all_formats_chunks
-- poc05_v02_all_formats_with_metadata
-- poc05_v02_all_formats_hybrid_rag_index

-- Vector Index to Keep:
-- sandbox.rdt_knowledge.poc05_v02_all_formats_hybrid_rag_index


-- ============================================================================
-- SECTION 1: DELETE POC05 PHASE 1 TABLES (Superseded by v02 All Formats)
-- ============================================================================

DROP TABLE IF EXISTS sandbox.rdt_knowledge.poc05_chunks_with_metadata;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.poc05_hybrid_rag_index;


-- ============================================================================
-- SECTION 2: DELETE POC05_v02 DOCX ONLY TABLES (Superseded by All Formats)
-- ============================================================================

DROP TABLE IF EXISTS sandbox.rdt_knowledge.poc05_v02_optimized_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.poc05_v02_chunks_with_metadata;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.poc05_v02_hybrid_rag_index;


-- ============================================================================
-- SECTION 3: DELETE POC03/POC04 ARCHIVE TABLES
-- ============================================================================

-- POC03
DROP TABLE IF EXISTS sandbox.rdt_knowledge.poc03_multimodal_rag_index;

-- POC04
DROP TABLE IF EXISTS sandbox.rdt_knowledge.poc04_optimized_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.poc04_optimized_rag_index;


-- ============================================================================
-- SECTION 4: DELETE EXPERIMENTAL DOCUMENT TABLES
-- ============================================================================

DROP TABLE IF EXISTS sandbox.rdt_knowledge.all_documents;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.all_documents_multimodal;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.raw_documents;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.documents_metadata;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.full_documents;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.real_documents_v2;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.production_documents;


-- ============================================================================
-- SECTION 5: DELETE EXPERIMENTAL CHUNK TABLES
-- ============================================================================

DROP TABLE IF EXISTS sandbox.rdt_knowledge.all_content_rag_index;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.all_document_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.all_vs_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.bilingual_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.datax_multimodal_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.document_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.full_content_rag_index;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.full_document_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.optimized_semantic_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.optimized_table_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.production_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.rag_document_chunks;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.real_document_chunks_v2;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.vs_document_chunks;


-- ============================================================================
-- SECTION 6: DELETE OLD INDEX TABLES
-- ============================================================================

DROP TABLE IF EXISTS sandbox.rdt_knowledge.all_content_rag_index_v1759206287;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.all_documents_index;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.bilingual_translation_index_v1759213209;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.datax_multimodal_index;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.production_rag_index_v1759207394;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.rag_chunk_index;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.semantic_focused_index_v1759209612;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.table_focused_index_v1759209612;


-- ============================================================================
-- SECTION 7: DELETE EMBEDDING TABLES
-- ============================================================================

DROP TABLE IF EXISTS sandbox.rdt_knowledge.document_embeddings;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.embeddings_vectors;


-- ============================================================================
-- SECTION 8: DELETE GENIE TABLES (Review first if Genie is still used)
-- ============================================================================

DROP TABLE IF EXISTS sandbox.rdt_knowledge.genie_content_summary;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.genie_document_catalog;


-- ============================================================================
-- SECTION 9: DELETE MONITORING TABLES (Review first if monitoring is active)
-- ============================================================================

DROP TABLE IF EXISTS sandbox.rdt_knowledge.chat_sessions;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.rag_performance_metrics;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.rag_query_logs;
DROP TABLE IF EXISTS sandbox.rdt_knowledge.system_performance;


-- ============================================================================
-- VERIFICATION: List remaining tables (should be only 3)
-- ============================================================================

SHOW TABLES IN sandbox.rdt_knowledge;

-- Expected result:
-- poc05_v02_all_formats_chunks
-- poc05_v02_all_formats_with_metadata
-- poc05_v02_all_formats_hybrid_rag_index


-- ============================================================================
-- SUMMARY
-- ============================================================================
-- Tables to Keep:  3 (POC05_v02 All Formats)
-- Tables to Delete: 45
-- Total Impact: 93% reduction (45 out of 48 tables)
--
-- Execution Time: ~5-10 minutes
-- Storage Freed: TBD (depends on table sizes)
-- ============================================================================
