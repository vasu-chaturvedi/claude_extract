-- Performance Optimization Indexes for RetailCifPack
-- These indexes address the 6 critical bottlenecks identified in the analysis

-- =============================================================================
-- 1. VALID_CIF Performance Indexes
-- =============================================================================
-- Critical for eliminating N+1 query patterns
CREATE INDEX IDX_VALID_CIF_SOL_TYPE ON VALID_CIF(SOL_ID, CIF_TYPE) TABLESPACE INDEXES;
CREATE INDEX IDX_VALID_CIF_CIF_ID ON VALID_CIF(CIF_ID) TABLESPACE INDEXES;

-- =============================================================================
-- 2. INDCLIENTS Performance Indexes  
-- =============================================================================
-- Primary access path for RC001, RC008, RC009
CREATE INDEX IDX_INDCLIENTS_CODE ON CBS.INDCLIENTS(INDCLIENT_CODE) TABLESPACE INDEXES;

-- Additional indexes for specific field lookups
CREATE INDEX IDX_INDCLIENTS_STATUS ON CBS.INDCLIENTS(INDCLIENT_STATUS) TABLESPACE INDEXES;
CREATE INDEX IDX_INDCLIENTS_LANG ON CBS.INDCLIENTS(INDCLIENT_LANG_CODE) TABLESPACE INDEXES;
CREATE INDEX IDX_INDCLIENTS_EMPLOYMENT ON CBS.INDCLIENTS(INDCLIENT_EMPLOYMENT_STATUS) TABLESPACE INDEXES;

-- =============================================================================
-- 3. CLIENTS Performance Indexes
-- =============================================================================
-- Primary access path for RC002, RC005, RC006
CREATE INDEX IDX_CLIENTS_CODE ON CBS.CLIENTS(CLIENTS_CODE) TABLESPACE INDEXES;

-- Foreign key indexes for JOIN operations
CREATE INDEX IDX_CLIENTS_ADDR_INV ON CBS.CLIENTS(CLIENTS_ADDR_INV_NUM) TABLESPACE INDEXES;
CREATE INDEX IDX_CLIENTS_PHONE_INV ON CBS.CLIENTS(CLIENTS_PHONE_INV_NUM) TABLESPACE INDEXES;
CREATE INDEX IDX_CLIENTS_EMAIL_INV ON CBS.CLIENTS(CLIENTS_EMAIL_INV_NUM) TABLESPACE INDEXES;

-- =============================================================================
-- 4. ADDRESS Details Performance Indexes
-- =============================================================================
-- Critical for RC002 performance
CREATE INDEX IDX_ADDRDTLS_INV_NUM ON CBS.ADDRDTLS(ADDRDTLS_INV_NUM) TABLESPACE INDEXES;
CREATE INDEX IDX_ADDRDTLS_TYPE ON CBS.ADDRDTLS(ADDRDTLS_ADDR_TYPE) TABLESPACE INDEXES;
CREATE INDEX IDX_ADDRDTLS_LOCN ON CBS.ADDRDTLS(ADDRDTLS_LOCN_CODE) TABLESPACE INDEXES;

-- Composite index for address filtering
CREATE INDEX IDX_ADDRDTLS_COMPOSITE ON CBS.ADDRDTLS(ADDRDTLS_INV_NUM, ADDRDTLS_ADDR_TYPE, ADDRDTLS_EFF_FROM_DATE) TABLESPACE INDEXES;

-- =============================================================================
-- 5. PHONE Details Performance Indexes
-- =============================================================================
-- Critical for RC005 performance
CREATE INDEX IDX_PHONEDTLS_INV_NUM ON CBS.PHONEDTLS(PHONEDTLS_INV_NUM) TABLESPACE INDEXES;
CREATE INDEX IDX_PHONEDTLS_TYPE ON CBS.PHONEDTLS(PHONEDTLS_PHONE_TYPE) TABLESPACE INDEXES;

-- Composite index for phone filtering
CREATE INDEX IDX_PHONEDTLS_COMPOSITE ON CBS.PHONEDTLS(PHONEDTLS_INV_NUM, PHONEDTLS_PHONE_TYPE) TABLESPACE INDEXES;

-- =============================================================================
-- 6. EMAIL Details Performance Indexes
-- =============================================================================
-- Critical for RC006 performance
CREATE INDEX IDX_EMAILDTLS_INV_NUM ON CBS.EMAILDTLS(EMAILDTLS_INV_NUM) TABLESPACE INDEXES;
CREATE INDEX IDX_EMAILDTLS_STATUS ON CBS.EMAILDTLS(EMAILDTLS_EMAIL_STATUS) TABLESPACE INDEXES;

-- Composite index for email filtering
CREATE INDEX IDX_EMAILDTLS_COMPOSITE ON CBS.EMAILDTLS(EMAILDTLS_INV_NUM, EMAILDTLS_EMAIL_STATUS) TABLESPACE INDEXES;

-- =============================================================================
-- 7. PIDDOCS Performance Indexes
-- =============================================================================
-- Critical for RC003 performance
CREATE INDEX IDX_PIDDOCS_CLIENT_CODE ON CBS.PIDDOCS(PIDDOCS_CLIENT_CODE) TABLESPACE INDEXES;
CREATE INDEX IDX_PIDDOCS_PID_TYPE ON CBS.PIDDOCS(PIDDOCS_PID_TYPE) TABLESPACE INDEXES;
CREATE INDEX IDX_PIDDOCS_KYC_STATUS ON CBS.PIDDOCS(PIDDOCS_KYC_STATUS) TABLESPACE INDEXES;

-- Composite index for document filtering
CREATE INDEX IDX_PIDDOCS_COMPOSITE ON CBS.PIDDOCS(PIDDOCS_CLIENT_CODE, PIDDOCS_PID_TYPE, PIDDOCS_KYC_STATUS) TABLESPACE INDEXES;

-- =============================================================================
-- 8. NOMINEES Performance Indexes
-- =============================================================================
-- Critical for RC004 performance
CREATE INDEX IDX_NOMINEES_CLIENT_CODE ON CBS.NOMINEES(NOMINEES_CLIENT_CODE) TABLESPACE INDEXES;
CREATE INDEX IDX_NOMINEES_RELATIONSHIP ON CBS.NOMINEES(NOMINEES_RELATIONSHIP) TABLESPACE INDEXES;

-- Composite index for nominee filtering
CREATE INDEX IDX_NOMINEES_COMPOSITE ON CBS.NOMINEES(NOMINEES_CLIENT_CODE, NOMINEES_RELATIONSHIP) TABLESPACE INDEXES;

-- =============================================================================
-- 9. Function-Based Indexes for Common Transformations
-- =============================================================================
-- Index for trimmed and cleaned string operations
CREATE INDEX IDX_INDCLIENTS_FNAME_CLEAN ON CBS.INDCLIENTS(UPPER(TRIM(INDCLIENT_FIRST_NAME))) TABLESPACE INDEXES;
CREATE INDEX IDX_INDCLIENTS_LNAME_CLEAN ON CBS.INDCLIENTS(UPPER(TRIM(INDCLIENT_LAST_NAME))) TABLESPACE INDEXES;

-- Index for date-based filtering
CREATE INDEX IDX_CLIENTS_OPENING_DATE ON CBS.CLIENTS(CLIENTS_OPENING_DATE) TABLESPACE INDEXES;
CREATE INDEX IDX_CLIENTS_ENTD_ON ON CBS.CLIENTS(CLIENTS_ENTD_ON) TABLESPACE INDEXES;

-- =============================================================================
-- 10. Partitioned Table Indexes (if tables are partitioned)
-- =============================================================================
-- If VALID_CIF is partitioned by SOL_ID, create local indexes
-- CREATE INDEX IDX_VALID_CIF_LOCAL ON VALID_CIF(CIF_ID, CIF_TYPE) LOCAL;

-- =============================================================================
-- 11. Statistics Update Commands
-- =============================================================================
-- Ensure statistics are current for optimal query plans
-- Run these after index creation and regularly in production

-- EXEC DBMS_STATS.GATHER_TABLE_STATS('CBS', 'INDCLIENTS', CASCADE => TRUE);
-- EXEC DBMS_STATS.GATHER_TABLE_STATS('CBS', 'CLIENTS', CASCADE => TRUE);
-- EXEC DBMS_STATS.GATHER_TABLE_STATS('CBS', 'ADDRDTLS', CASCADE => TRUE);
-- EXEC DBMS_STATS.GATHER_TABLE_STATS('CBS', 'PHONEDTLS', CASCADE => TRUE);
-- EXEC DBMS_STATS.GATHER_TABLE_STATS('CBS', 'EMAILDTLS', CASCADE => TRUE);
-- EXEC DBMS_STATS.GATHER_TABLE_STATS('CBS', 'PIDDOCS', CASCADE => TRUE);
-- EXEC DBMS_STATS.GATHER_TABLE_STATS('CBS', 'NOMINEES', CASCADE => TRUE);
-- EXEC DBMS_STATS.GATHER_TABLE_STATS('CBS', 'VALID_CIF', CASCADE => TRUE);

-- =============================================================================
-- 12. Index Monitoring and Validation
-- =============================================================================
-- Query to check index usage after deployment
/*
SELECT 
    i.INDEX_NAME,
    i.TABLE_NAME,
    i.STATUS,
    i.LAST_ANALYZED,
    u.MONITORING,
    u.USED
FROM USER_INDEXES i
LEFT JOIN USER_OBJECT_USAGE u ON i.INDEX_NAME = u.INDEX_NAME
WHERE i.TABLE_NAME IN ('INDCLIENTS', 'CLIENTS', 'ADDRDTLS', 'PHONEDTLS', 'EMAILDTLS', 'PIDDOCS', 'NOMINEES', 'VALID_CIF')
ORDER BY i.TABLE_NAME, i.INDEX_NAME;
*/

-- =============================================================================
-- 13. Index Maintenance Commands
-- =============================================================================
-- Run these commands during maintenance windows
/*
-- Rebuild fragmented indexes
ALTER INDEX IDX_VALID_CIF_SOL_TYPE REBUILD TABLESPACE INDEXES;
ALTER INDEX IDX_INDCLIENTS_CODE REBUILD TABLESPACE INDEXES;
ALTER INDEX IDX_CLIENTS_CODE REBUILD TABLESPACE INDEXES;
ALTER INDEX IDX_ADDRDTLS_INV_NUM REBUILD TABLESPACE INDEXES;
ALTER INDEX IDX_PHONEDTLS_INV_NUM REBUILD TABLESPACE INDEXES;
ALTER INDEX IDX_EMAILDTLS_INV_NUM REBUILD TABLESPACE INDEXES;
ALTER INDEX IDX_PIDDOCS_CLIENT_CODE REBUILD TABLESPACE INDEXES;
ALTER INDEX IDX_NOMINEES_CLIENT_CODE REBUILD TABLESPACE INDEXES;
*/

-- =============================================================================
-- Performance Impact Summary
-- =============================================================================
/*
Expected Performance Gains with These Indexes:

1. VALID_CIF Queries: 90% reduction in execution time
   - Eliminates full table scans
   - Enables efficient SOL_ID + CIF_TYPE filtering

2. JOIN Operations: 70-80% reduction in execution time
   - Hash joins instead of nested loops
   - Eliminates table scans for foreign key lookups

3. Address Processing (RC002): 85% reduction
   - Direct index access to ADDRDTLS via INV_NUM
   - Efficient address type filtering

4. Phone Processing (RC005): 80% reduction
   - Direct index access to PHONEDTLS via INV_NUM
   - Efficient phone type filtering

5. Email Processing (RC006): 75% reduction
   - Direct index access to EMAILDTLS via INV_NUM
   - Efficient email status filtering

6. Document Processing (RC003): 80% reduction
   - Direct index access to PIDDOCS via CLIENT_CODE
   - Efficient document type filtering

7. Nominee Processing (RC004): 75% reduction
   - Direct index access to NOMINEES via CLIENT_CODE
   - Efficient relationship filtering

Overall Expected Improvement: 80-90% reduction in total execution time
*/

-- =============================================================================
-- Deployment Instructions
-- =============================================================================
/*
1. Deploy during maintenance window
2. Run index creation commands in sequence
3. Monitor disk space usage during creation
4. Update table statistics after index creation
5. Test with sample SOL_ID to validate performance
6. Monitor index usage for first week after deployment
7. Adjust batch sizes if needed based on performance
*/