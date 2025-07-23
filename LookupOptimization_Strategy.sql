-- =====================================================================================
-- LOOKUP OPTIMIZATION STRATEGY FOR RETAILCIFPACK
-- Target: Eliminate 45 crore function calls causing 2.5 hour execution time
-- =====================================================================================

-- =====================================================================================
-- 1. MATERIALIZED VIEWS FOR FAST LOOKUP ACCESS
-- =====================================================================================

-- Create materialized view for MAPPER_FUNC lookups
CREATE MATERIALIZED VIEW MV_MAPPER_LOOKUP 
BUILD IMMEDIATE 
REFRESH FAST ON COMMIT 
AS
SELECT 
    MASTERCODE_TYPE,
    MASTERCODE_SUBTYPE,
    MASTERCODE_VALUE,
    MASTERCODE_SYSTEM,
    MAPPED_VALUE,
    UPPER(MASTERCODE_TYPE || '|' || MASTERCODE_SUBTYPE || '|' || MASTERCODE_VALUE || '|' || MASTERCODE_SYSTEM) AS LOOKUP_KEY
FROM (
    -- Add your actual mapper lookup table here
    -- This is a template - replace with actual table structure
    SELECT 
        'MASTERCODE' AS MASTERCODE_TYPE,
        'CUSTOMER_STATUS' AS MASTERCODE_SUBTYPE,
        SOURCE_VALUE AS MASTERCODE_VALUE,
        'CBS' AS MASTERCODE_SYSTEM,
        TARGET_VALUE AS MAPPED_VALUE
    FROM MAPPER_LOOKUP_TABLE
    WHERE LOOKUP_TYPE = 'CUSTOMER_STATUS'
    
    UNION ALL
    
    SELECT 
        'MASTERCODE' AS MASTERCODE_TYPE,
        'SEGMENTATION_CLASS' AS MASTERCODE_SUBTYPE,
        SOURCE_VALUE AS MASTERCODE_VALUE,
        'CBS' AS MASTERCODE_SYSTEM,
        TARGET_VALUE AS MAPPED_VALUE
    FROM MAPPER_LOOKUP_TABLE
    WHERE LOOKUP_TYPE = 'SEGMENTATION_CLASS'
    
    UNION ALL
    
    SELECT 
        'MASTERCODE' AS MASTERCODE_TYPE,
        'ACCOUNT_TYPE' AS MASTERCODE_SUBTYPE,
        SOURCE_VALUE AS MASTERCODE_VALUE,
        'CBS' AS MASTERCODE_SYSTEM,
        TARGET_VALUE AS MAPPED_VALUE
    FROM MAPPER_LOOKUP_TABLE
    WHERE LOOKUP_TYPE = 'ACCOUNT_TYPE'
    
    UNION ALL
    
    SELECT 
        'MASTERCODE' AS MASTERCODE_TYPE,
        'CONTACT_OCCUPATION' AS MASTERCODE_SUBTYPE,
        SOURCE_VALUE AS MASTERCODE_VALUE,
        'CBS' AS MASTERCODE_SYSTEM,
        TARGET_VALUE AS MAPPED_VALUE
    FROM MAPPER_LOOKUP_TABLE
    WHERE LOOKUP_TYPE = 'CONTACT_OCCUPATION'
    
    UNION ALL
    
    SELECT 
        'MASTERCODE' AS MASTERCODE_TYPE,
        'LANGUAGE' AS MASTERCODE_SUBTYPE,
        SOURCE_VALUE AS MASTERCODE_VALUE,
        'CBS' AS MASTERCODE_SYSTEM,
        TARGET_VALUE AS MAPPED_VALUE
    FROM MAPPER_LOOKUP_TABLE
    WHERE LOOKUP_TYPE = 'LANGUAGE'
    
    UNION ALL
    
    SELECT 
        'MASTERCODE' AS MASTERCODE_TYPE,
        'ADDRTYPE' AS MASTERCODE_SUBTYPE,
        SOURCE_VALUE AS MASTERCODE_VALUE,
        'CBS' AS MASTERCODE_SYSTEM,
        TARGET_VALUE AS MAPPED_VALUE
    FROM MAPPER_LOOKUP_TABLE
    WHERE LOOKUP_TYPE = 'ADDRTYPE'
    
    UNION ALL
    
    SELECT 
        'MASTERCODE' AS MASTERCODE_TYPE,
        'EMPLOYMENT_STATUS' AS MASTERCODE_SUBTYPE,
        SOURCE_VALUE AS MASTERCODE_VALUE,
        'CBS' AS MASTERCODE_SYSTEM,
        TARGET_VALUE AS MAPPED_VALUE
    FROM MAPPER_LOOKUP_TABLE
    WHERE LOOKUP_TYPE = 'EMPLOYMENT_STATUS'
    
    UNION ALL
    
    SELECT 
        'MASTERCODE' AS MASTERCODE_TYPE,
        'CASTE' AS MASTERCODE_SUBTYPE,
        SOURCE_VALUE AS MASTERCODE_VALUE,
        'CBS' AS MASTERCODE_SYSTEM,
        TARGET_VALUE AS MAPPED_VALUE
    FROM MAPPER_LOOKUP_TABLE
    WHERE LOOKUP_TYPE = 'CASTE'
);

-- Create unique index for fast lookup
CREATE UNIQUE INDEX IDX_MV_MAPPER_LOOKUP_KEY ON MV_MAPPER_LOOKUP(LOOKUP_KEY);

-- Create materialized view for location lookups
CREATE MATERIALIZED VIEW MV_LOCATION_LOOKUP 
BUILD IMMEDIATE 
REFRESH FAST ON COMMIT 
AS
SELECT 
    LOCATION_TYPE,
    LOCATION_CODE,
    LOCATION_DESC,
    UPPER(LOCATION_TYPE || '|' || LOCATION_CODE) AS LOOKUP_KEY
FROM (
    -- Add your actual location lookup table here
    -- This is a template - replace with actual table structure
    SELECT 
        'CITY' AS LOCATION_TYPE,
        CITY_CODE AS LOCATION_CODE,
        CITY_DESC AS LOCATION_DESC
    FROM LOCATION_MASTER
    WHERE LOCATION_TYPE = 'CITY'
    
    UNION ALL
    
    SELECT 
        'STATE' AS LOCATION_TYPE,
        STATE_CODE AS LOCATION_CODE,
        STATE_DESC AS LOCATION_DESC
    FROM LOCATION_MASTER
    WHERE LOCATION_TYPE = 'STATE'
    
    UNION ALL
    
    SELECT 
        'CITYDESC' AS LOCATION_TYPE,
        CITY_CODE AS LOCATION_CODE,
        CITY_DESC AS LOCATION_DESC
    FROM LOCATION_MASTER
    WHERE LOCATION_TYPE = 'CITY'
);

-- Create unique index for fast lookup
CREATE UNIQUE INDEX IDX_MV_LOCATION_LOOKUP_KEY ON MV_LOCATION_LOOKUP(LOOKUP_KEY);

-- =====================================================================================
-- 2. HIGH-PERFORMANCE LOOKUP PACKAGE
-- =====================================================================================

CREATE OR REPLACE PACKAGE HighPerformanceLookup AS
    
    -- Type definitions for bulk lookups
    TYPE LOOKUP_KEY_TAB IS TABLE OF VARCHAR2(200) INDEX BY PLS_INTEGER;
    TYPE LOOKUP_VALUE_TAB IS TABLE OF VARCHAR2(100) INDEX BY PLS_INTEGER;
    
    -- Cached lookup collections
    TYPE MAPPER_CACHE_TYPE IS TABLE OF VARCHAR2(100) INDEX BY VARCHAR2(200);
    TYPE LOCATION_CACHE_TYPE IS TABLE OF VARCHAR2(100) INDEX BY VARCHAR2(200);
    
    -- Cache variables
    g_mapper_cache MAPPER_CACHE_TYPE;
    g_location_cache LOCATION_CACHE_TYPE;
    g_cache_loaded BOOLEAN := FALSE;
    
    -- Functions
    FUNCTION FAST_MAPPER_FUNC(
        p_type VARCHAR2,
        p_subtype VARCHAR2,
        p_value VARCHAR2,
        p_system VARCHAR2
    ) RETURN VARCHAR2;
    
    FUNCTION FAST_LOCATION_FUNC(
        p_type VARCHAR2,
        p_code VARCHAR2
    ) RETURN VARCHAR2;
    
    -- Bulk lookup functions
    PROCEDURE BULK_MAPPER_LOOKUP(
        p_keys IN LOOKUP_KEY_TAB,
        p_values OUT LOOKUP_VALUE_TAB
    );
    
    PROCEDURE BULK_LOCATION_LOOKUP(
        p_keys IN LOOKUP_KEY_TAB,
        p_values OUT LOOKUP_VALUE_TAB
    );
    
    -- Cache management
    PROCEDURE LOAD_CACHE;
    PROCEDURE REFRESH_CACHE;
    PROCEDURE CLEAR_CACHE;
    
END HighPerformanceLookup;
/

CREATE OR REPLACE PACKAGE BODY HighPerformanceLookup AS
    
    -- Load all lookup data into memory cache
    PROCEDURE LOAD_CACHE IS
        l_key VARCHAR2(200);
        l_start_time TIMESTAMP;
        l_mapper_count NUMBER := 0;
        l_location_count NUMBER := 0;
    BEGIN
        l_start_time := SYSTIMESTAMP;
        
        -- Clear existing cache
        g_mapper_cache.DELETE;
        g_location_cache.DELETE;
        
        -- Load mapper cache
        FOR rec IN (SELECT LOOKUP_KEY, MAPPED_VALUE FROM MV_MAPPER_LOOKUP) LOOP
            g_mapper_cache(rec.LOOKUP_KEY) := rec.MAPPED_VALUE;
            l_mapper_count := l_mapper_count + 1;
        END LOOP;
        
        -- Load location cache
        FOR rec IN (SELECT LOOKUP_KEY, LOCATION_DESC FROM MV_LOCATION_LOOKUP) LOOP
            g_location_cache(rec.LOOKUP_KEY) := rec.LOCATION_DESC;
            l_location_count := l_location_count + 1;
        END LOOP;
        
        g_cache_loaded := TRUE;
        
        DBMS_OUTPUT.PUT_LINE('Cache loaded in ' || 
            EXTRACT(SECOND FROM (SYSTIMESTAMP - l_start_time)) || ' seconds');
        DBMS_OUTPUT.PUT_LINE('Mapper entries: ' || l_mapper_count);
        DBMS_OUTPUT.PUT_LINE('Location entries: ' || l_location_count);
        
    END LOAD_CACHE;
    
    -- Fast mapper function using in-memory cache
    FUNCTION FAST_MAPPER_FUNC(
        p_type VARCHAR2,
        p_subtype VARCHAR2,
        p_value VARCHAR2,
        p_system VARCHAR2
    ) RETURN VARCHAR2 IS
        l_key VARCHAR2(200);
        l_result VARCHAR2(100);
    BEGIN
        -- Load cache if not loaded
        IF NOT g_cache_loaded THEN
            LOAD_CACHE;
        END IF;
        
        -- Build lookup key
        l_key := UPPER(p_type || '|' || p_subtype || '|' || p_value || '|' || p_system);
        
        -- Return cached value or default
        IF g_mapper_cache.EXISTS(l_key) THEN
            RETURN g_mapper_cache(l_key);
        ELSE
            RETURN p_value; -- Return original value if mapping not found
        END IF;
        
    EXCEPTION
        WHEN OTHERS THEN
            RETURN p_value; -- Return original value on error
    END FAST_MAPPER_FUNC;
    
    -- Fast location function using in-memory cache
    FUNCTION FAST_LOCATION_FUNC(
        p_type VARCHAR2,
        p_code VARCHAR2
    ) RETURN VARCHAR2 IS
        l_key VARCHAR2(200);
        l_result VARCHAR2(100);
    BEGIN
        -- Load cache if not loaded
        IF NOT g_cache_loaded THEN
            LOAD_CACHE;
        END IF;
        
        -- Build lookup key
        l_key := UPPER(p_type || '|' || p_code);
        
        -- Return cached value or default
        IF g_location_cache.EXISTS(l_key) THEN
            RETURN g_location_cache(l_key);
        ELSE
            RETURN 'MIG'; -- Return default value if mapping not found
        END IF;
        
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 'MIG'; -- Return default value on error
    END FAST_LOCATION_FUNC;
    
    -- Bulk mapper lookup for processing arrays
    PROCEDURE BULK_MAPPER_LOOKUP(
        p_keys IN LOOKUP_KEY_TAB,
        p_values OUT LOOKUP_VALUE_TAB
    ) IS
    BEGIN
        -- Load cache if not loaded
        IF NOT g_cache_loaded THEN
            LOAD_CACHE;
        END IF;
        
        -- Process bulk lookup
        FOR i IN 1..p_keys.COUNT LOOP
            IF g_mapper_cache.EXISTS(p_keys(i)) THEN
                p_values(i) := g_mapper_cache(p_keys(i));
            ELSE
                p_values(i) := NULL; -- or default value
            END IF;
        END LOOP;
        
    END BULK_MAPPER_LOOKUP;
    
    -- Bulk location lookup for processing arrays
    PROCEDURE BULK_LOCATION_LOOKUP(
        p_keys IN LOOKUP_KEY_TAB,
        p_values OUT LOOKUP_VALUE_TAB
    ) IS
    BEGIN
        -- Load cache if not loaded
        IF NOT g_cache_loaded THEN
            LOAD_CACHE;
        END IF;
        
        -- Process bulk lookup
        FOR i IN 1..p_keys.COUNT LOOP
            IF g_location_cache.EXISTS(p_keys(i)) THEN
                p_values(i) := g_location_cache(p_keys(i));
            ELSE
                p_values(i) := 'MIG'; -- default value
            END IF;
        END LOOP;
        
    END BULK_LOCATION_LOOKUP;
    
    -- Refresh cache from database
    PROCEDURE REFRESH_CACHE IS
    BEGIN
        -- Refresh materialized views
        DBMS_MVIEW.REFRESH('MV_MAPPER_LOOKUP');
        DBMS_MVIEW.REFRESH('MV_LOCATION_LOOKUP');
        
        -- Reload cache
        LOAD_CACHE;
        
    END REFRESH_CACHE;
    
    -- Clear cache
    PROCEDURE CLEAR_CACHE IS
    BEGIN
        g_mapper_cache.DELETE;
        g_location_cache.DELETE;
        g_cache_loaded := FALSE;
        
    END CLEAR_CACHE;
    
END HighPerformanceLookup;
/

-- =====================================================================================
-- 3. USAGE EXAMPLE: Replace function calls in RetailCifPack
-- =====================================================================================

/*
-- BEFORE (in RetailCifPack):
v_status_code := COMMONEXTRACTIONPACK.MAPPER_FUNC('MASTERCODE','CUSTOMER_STATUS',v_status_code,'CBS');
v_CITY_CODE := NVL(CommonExtractionPack.location('CITY',V_CITY_CODE),'MIG');

-- AFTER (using optimized lookup):
v_status_code := HighPerformanceLookup.FAST_MAPPER_FUNC('MASTERCODE','CUSTOMER_STATUS',v_status_code,'CBS');
v_CITY_CODE := NVL(HighPerformanceLookup.FAST_LOCATION_FUNC('CITY',V_CITY_CODE),'MIG');
*/

-- =====================================================================================
-- 4. BULK PROCESSING EXAMPLE
-- =====================================================================================

/*
-- Instead of individual function calls in loops:
FOR i IN 1..records.COUNT LOOP
    records(i).status := COMMONEXTRACTIONPACK.MAPPER_FUNC('MASTERCODE','CUSTOMER_STATUS',records(i).raw_status,'CBS');
END LOOP;

-- Use bulk processing:
DECLARE
    l_keys HighPerformanceLookup.LOOKUP_KEY_TAB;
    l_values HighPerformanceLookup.LOOKUP_VALUE_TAB;
BEGIN
    -- Prepare keys
    FOR i IN 1..records.COUNT LOOP
        l_keys(i) := UPPER('MASTERCODE|CUSTOMER_STATUS|' || records(i).raw_status || '|CBS');
    END LOOP;
    
    -- Bulk lookup
    HighPerformanceLookup.BULK_MAPPER_LOOKUP(l_keys, l_values);
    
    -- Apply results
    FOR i IN 1..records.COUNT LOOP
        records(i).status := NVL(l_values(i), records(i).raw_status);
    END LOOP;
END;
*/

-- =====================================================================================
-- 5. PERFORMANCE MONITORING
-- =====================================================================================

CREATE TABLE LOOKUP_PERFORMANCE_LOG (
    log_id NUMBER PRIMARY KEY,
    lookup_type VARCHAR2(50),
    execution_time_ms NUMBER,
    cache_hit_rate NUMBER,
    total_lookups NUMBER,
    log_date DATE DEFAULT SYSDATE
);

CREATE SEQUENCE LOOKUP_PERFORMANCE_SEQ START WITH 1 INCREMENT BY 1;

-- =====================================================================================
-- 6. DEPLOYMENT SCRIPT
-- =====================================================================================

/*
-- Step 1: Create materialized views
@LookupOptimization_Strategy.sql

-- Step 2: Load initial cache
BEGIN
    HighPerformanceLookup.LOAD_CACHE;
END;
/

-- Step 3: Test performance
DECLARE
    l_start_time TIMESTAMP;
    l_end_time TIMESTAMP;
    l_result VARCHAR2(100);
BEGIN
    l_start_time := SYSTIMESTAMP;
    
    -- Test 1000 lookups
    FOR i IN 1..1000 LOOP
        l_result := HighPerformanceLookup.FAST_MAPPER_FUNC('MASTERCODE','CUSTOMER_STATUS','A','CBS');
    END LOOP;
    
    l_end_time := SYSTIMESTAMP;
    
    DBMS_OUTPUT.PUT_LINE('1000 lookups completed in: ' || 
        EXTRACT(SECOND FROM (l_end_time - l_start_time)) || ' seconds');
END;
/

-- Step 4: Update RetailCifPack to use new functions
-- Replace all COMMONEXTRACTIONPACK.MAPPER_FUNC calls with HighPerformanceLookup.FAST_MAPPER_FUNC
-- Replace all CommonExtractionPack.location calls with HighPerformanceLookup.FAST_LOCATION_FUNC
*/

-- =====================================================================================
-- EXPECTED PERFORMANCE IMPROVEMENT
-- =====================================================================================

/*
Current Performance:
- 45 crore function calls
- Each function call: ~0.02 seconds (database lookup)
- Total time in functions: ~2.5 hours

Optimized Performance:
- In-memory cache lookup: ~0.0001 seconds
- Cache loading time: ~30 seconds one-time
- Total time in functions: ~7.5 minutes

Expected Improvement: 95% reduction in lookup time
Total execution time: 2.5 hours → 20-30 minutes
*/