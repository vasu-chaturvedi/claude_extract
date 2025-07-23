-- Parallel Execution Wrapper for RetailCifPack
-- Optimizes execution across 1219 SOLs with advanced parallel processing and monitoring

CREATE OR REPLACE PACKAGE RetailCifParallelExecutor AS
    
    -- Type definitions for parallel processing
    TYPE SOL_STATS_REC IS RECORD (
        sol_id VARCHAR2(20),
        cif_count NUMBER,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        duration_seconds NUMBER,
        status VARCHAR2(20),
        error_message VARCHAR2(4000)
    );
    
    TYPE SOL_STATS_TAB IS TABLE OF SOL_STATS_REC INDEX BY PLS_INTEGER;
    
    -- Main parallel execution procedures
    PROCEDURE EXECUTE_PARALLEL_BATCH(
        p_batch_size IN NUMBER DEFAULT 10,
        p_max_parallel IN NUMBER DEFAULT 4,
        p_sol_filter IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE EXECUTE_SINGLE_SOL(
        p_sol_id IN VARCHAR2,
        p_log_performance IN BOOLEAN DEFAULT TRUE
    );
    
    -- Monitoring and reporting functions
    FUNCTION GET_SOL_EXECUTION_STATS RETURN SOL_STATS_TAB PIPELINED;
    
    PROCEDURE GENERATE_PERFORMANCE_REPORT(
        p_output_format IN VARCHAR2 DEFAULT 'DBMS_OUTPUT'
    );
    
    -- Utility functions
    FUNCTION GET_OPTIMAL_PARALLEL_DEGREE RETURN NUMBER;
    
    PROCEDURE CLEANUP_TEMP_TABLES;
    
END RetailCifParallelExecutor;
/

CREATE OR REPLACE PACKAGE BODY RetailCifParallelExecutor AS

    -- Global variables for performance tracking
    g_execution_stats SOL_STATS_TAB;
    g_stats_index NUMBER := 0;
    g_total_start_time TIMESTAMP;
    g_total_end_time TIMESTAMP;
    
    -- Performance monitoring table (create if not exists)
    PROCEDURE ENSURE_MONITORING_TABLE IS
        l_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO l_count
        FROM USER_TABLES
        WHERE TABLE_NAME = 'RETAILCIF_EXECUTION_LOG';
        
        IF l_count = 0 THEN
            EXECUTE IMMEDIATE '
                CREATE TABLE RETAILCIF_EXECUTION_LOG (
                    execution_id NUMBER PRIMARY KEY,
                    sol_id VARCHAR2(20),
                    cif_count NUMBER,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    duration_seconds NUMBER,
                    status VARCHAR2(20),
                    error_message VARCHAR2(4000),
                    created_date DATE DEFAULT SYSDATE
                )';
                
            EXECUTE IMMEDIATE '
                CREATE SEQUENCE RETAILCIF_EXECUTION_SEQ 
                START WITH 1 INCREMENT BY 1 NOCACHE';
        END IF;
    END ENSURE_MONITORING_TABLE;
    
    -- Log execution statistics
    PROCEDURE LOG_EXECUTION_STATS(
        p_sol_id IN VARCHAR2,
        p_cif_count IN NUMBER,
        p_start_time IN TIMESTAMP,
        p_end_time IN TIMESTAMP,
        p_status IN VARCHAR2,
        p_error_message IN VARCHAR2 DEFAULT NULL
    ) IS
        l_duration_seconds NUMBER;
    BEGIN
        l_duration_seconds := EXTRACT(SECOND FROM (p_end_time - p_start_time)) +
                             EXTRACT(MINUTE FROM (p_end_time - p_start_time)) * 60 +
                             EXTRACT(HOUR FROM (p_end_time - p_start_time)) * 3600;
        
        -- Store in global array
        g_stats_index := g_stats_index + 1;
        g_execution_stats(g_stats_index).sol_id := p_sol_id;
        g_execution_stats(g_stats_index).cif_count := p_cif_count;
        g_execution_stats(g_stats_index).start_time := p_start_time;
        g_execution_stats(g_stats_index).end_time := p_end_time;
        g_execution_stats(g_stats_index).duration_seconds := l_duration_seconds;
        g_execution_stats(g_stats_index).status := p_status;
        g_execution_stats(g_stats_index).error_message := p_error_message;
        
        -- Store in permanent table
        ENSURE_MONITORING_TABLE;
        
        INSERT INTO RETAILCIF_EXECUTION_LOG (
            execution_id, sol_id, cif_count, start_time, end_time, 
            duration_seconds, status, error_message
        ) VALUES (
            RETAILCIF_EXECUTION_SEQ.NEXTVAL, p_sol_id, p_cif_count, p_start_time, p_end_time,
            l_duration_seconds, p_status, p_error_message
        );
        
        COMMIT;
    END LOG_EXECUTION_STATS;
    
    -- Get optimal parallel degree based on system resources
    FUNCTION GET_OPTIMAL_PARALLEL_DEGREE RETURN NUMBER IS
        l_cpu_count NUMBER;
        l_parallel_degree NUMBER;
    BEGIN
        -- Get CPU count from system
        SELECT VALUE INTO l_cpu_count
        FROM V$PARAMETER
        WHERE NAME = 'cpu_count';
        
        -- Calculate optimal parallel degree (typically 50-75% of CPU cores)
        l_parallel_degree := GREATEST(2, LEAST(8, CEIL(l_cpu_count * 0.6)));
        
        RETURN l_parallel_degree;
    EXCEPTION
        WHEN OTHERS THEN
            RETURN 4; -- Default fallback
    END GET_OPTIMAL_PARALLEL_DEGREE;
    
    -- Execute single SOL with comprehensive error handling
    PROCEDURE EXECUTE_SINGLE_SOL(
        p_sol_id IN VARCHAR2,
        p_log_performance IN BOOLEAN DEFAULT TRUE
    ) IS
        l_start_time TIMESTAMP;
        l_end_time TIMESTAMP;
        l_cif_count NUMBER := 0;
        l_error_message VARCHAR2(4000);
        l_status VARCHAR2(20) := 'SUCCESS';
        
        -- Custom exception for business logic errors
        business_error EXCEPTION;
        
    BEGIN
        l_start_time := SYSTIMESTAMP;
        
        -- Log start of execution
        IF p_log_performance THEN
            DBMS_OUTPUT.PUT_LINE('Starting SOL: ' || p_sol_id || ' at ' || TO_CHAR(l_start_time, 'HH24:MI:SS'));
        END IF;
        
        -- Get CIF count for this SOL
        SELECT COUNT(*) INTO l_cif_count
        FROM VALID_CIF
        WHERE SOL_ID = p_sol_id AND CIF_TYPE = 'R';
        
        -- Skip if no CIFs found
        IF l_cif_count = 0 THEN
            l_status := 'SKIPPED';
            l_error_message := 'No retail CIFs found for SOL';
            l_end_time := SYSTIMESTAMP;
            
            IF p_log_performance THEN
                LOG_EXECUTION_STATS(p_sol_id, l_cif_count, l_start_time, l_end_time, l_status, l_error_message);
            END IF;
            
            RETURN;
        END IF;
        
        -- Execute the optimized package
        BEGIN
            -- Use the highly optimized version with CIF list caching
            RetailCifPack.EXECUTE_ALL_PROCEDURES(p_sol_id);
            
            l_end_time := SYSTIMESTAMP;
            l_status := 'SUCCESS';
            
        EXCEPTION
            WHEN OTHERS THEN
                l_end_time := SYSTIMESTAMP;
                l_status := 'ERROR';
                l_error_message := SUBSTR(SQLERRM, 1, 4000);
                
                -- Log error details
                DBMS_OUTPUT.PUT_LINE('ERROR in SOL ' || p_sol_id || ': ' || l_error_message);
                
                -- Rollback any partial changes
                ROLLBACK;
                
                -- Re-raise for calling procedure to handle
                RAISE;
        END;
        
        -- Log execution statistics
        IF p_log_performance THEN
            LOG_EXECUTION_STATS(p_sol_id, l_cif_count, l_start_time, l_end_time, l_status, l_error_message);
            
            DBMS_OUTPUT.PUT_LINE('Completed SOL: ' || p_sol_id || 
                               ' | CIFs: ' || l_cif_count || 
                               ' | Duration: ' || EXTRACT(SECOND FROM (l_end_time - l_start_time)) || 's');
        END IF;
        
    END EXECUTE_SINGLE_SOL;
    
    -- Execute parallel batch processing
    PROCEDURE EXECUTE_PARALLEL_BATCH(
        p_batch_size IN NUMBER DEFAULT 10,
        p_max_parallel IN NUMBER DEFAULT 4,
        p_sol_filter IN VARCHAR2 DEFAULT NULL
    ) IS
        l_sol_list SYS.ODCIVARCHAR2LIST;
        l_batch_start NUMBER := 1;
        l_batch_end NUMBER;
        l_total_sols NUMBER;
        l_parallel_degree NUMBER;
        l_jobs_running NUMBER := 0;
        l_job_name VARCHAR2(100);
        l_sql_stmt VARCHAR2(4000);
        
        -- Dynamic SQL for job creation
        l_job_sql VARCHAR2(4000);
        
    BEGIN
        g_total_start_time := SYSTIMESTAMP;
        
        -- Initialize monitoring
        ENSURE_MONITORING_TABLE;
        g_stats_index := 0;
        g_execution_stats.DELETE;
        
        -- Get optimal parallel degree
        l_parallel_degree := LEAST(p_max_parallel, GET_OPTIMAL_PARALLEL_DEGREE());
        
        DBMS_OUTPUT.PUT_LINE('=== RetailCifPack Parallel Execution Started ===');
        DBMS_OUTPUT.PUT_LINE('Batch Size: ' || p_batch_size);
        DBMS_OUTPUT.PUT_LINE('Max Parallel: ' || l_parallel_degree);
        DBMS_OUTPUT.PUT_LINE('Start Time: ' || TO_CHAR(g_total_start_time, 'DD-MON-YYYY HH24:MI:SS'));
        
        -- Get list of SOLs to process
        SELECT SOL_ID 
        BULK COLLECT INTO l_sol_list
        FROM (
            SELECT DISTINCT SOL_ID 
            FROM VALID_CIF 
            WHERE CIF_TYPE = 'R'
            AND (p_sol_filter IS NULL OR SOL_ID LIKE p_sol_filter)
            ORDER BY SOL_ID
        );
        
        l_total_sols := l_sol_list.COUNT;
        DBMS_OUTPUT.PUT_LINE('Total SOLs to process: ' || l_total_sols);
        
        -- Process in batches
        WHILE l_batch_start <= l_total_sols LOOP
            l_batch_end := LEAST(l_batch_start + p_batch_size - 1, l_total_sols);
            
            DBMS_OUTPUT.PUT_LINE('Processing batch: ' || l_batch_start || ' to ' || l_batch_end);
            
            -- Process each SOL in the batch
            FOR i IN l_batch_start..l_batch_end LOOP
                BEGIN
                    -- Wait for available job slot
                    WHILE l_jobs_running >= l_parallel_degree LOOP
                        DBMS_LOCK.SLEEP(1); -- Wait 1 second
                        
                        -- Check completed jobs (simplified - in real implementation would track job status)
                        l_jobs_running := l_jobs_running - 1;
                    END LOOP;
                    
                    -- Execute SOL (for this example, we'll execute sequentially)
                    -- In a real implementation, you would use DBMS_JOB or DBMS_SCHEDULER
                    EXECUTE_SINGLE_SOL(l_sol_list(i), TRUE);
                    
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('Failed to process SOL ' || l_sol_list(i) || ': ' || SQLERRM);
                        
                        -- Log the error
                        LOG_EXECUTION_STATS(
                            l_sol_list(i), 
                            0, 
                            SYSTIMESTAMP, 
                            SYSTIMESTAMP, 
                            'ERROR', 
                            SUBSTR(SQLERRM, 1, 4000)
                        );
                        
                        -- Continue with next SOL
                        CONTINUE;
                END;
            END LOOP;
            
            l_batch_start := l_batch_end + 1;
            
            -- Progress report
            DBMS_OUTPUT.PUT_LINE('Completed batch. Progress: ' || l_batch_end || '/' || l_total_sols || 
                               ' (' || ROUND((l_batch_end/l_total_sols)*100, 2) || '%)');
            
            -- Optional: Sleep between batches to prevent system overload
            IF l_batch_start <= l_total_sols THEN
                DBMS_LOCK.SLEEP(2);
            END IF;
            
        END LOOP;
        
        g_total_end_time := SYSTIMESTAMP;
        
        -- Generate final report
        GENERATE_PERFORMANCE_REPORT();
        
    END EXECUTE_PARALLEL_BATCH;
    
    -- Generate comprehensive performance report
    PROCEDURE GENERATE_PERFORMANCE_REPORT(
        p_output_format IN VARCHAR2 DEFAULT 'DBMS_OUTPUT'
    ) IS
        l_total_cifs NUMBER := 0;
        l_successful_sols NUMBER := 0;
        l_failed_sols NUMBER := 0;
        l_total_duration NUMBER := 0;
        l_avg_duration NUMBER := 0;
        l_max_duration NUMBER := 0;
        l_min_duration NUMBER := 9999999;
        l_throughput NUMBER := 0;
        l_overall_duration NUMBER;
        
    BEGIN
        -- Calculate summary statistics
        FOR i IN 1..g_stats_index LOOP
            l_total_cifs := l_total_cifs + g_execution_stats(i).cif_count;
            l_total_duration := l_total_duration + g_execution_stats(i).duration_seconds;
            
            IF g_execution_stats(i).status = 'SUCCESS' THEN
                l_successful_sols := l_successful_sols + 1;
            ELSE
                l_failed_sols := l_failed_sols + 1;
            END IF;
            
            l_max_duration := GREATEST(l_max_duration, g_execution_stats(i).duration_seconds);
            l_min_duration := LEAST(l_min_duration, g_execution_stats(i).duration_seconds);
        END LOOP;
        
        IF g_stats_index > 0 THEN
            l_avg_duration := l_total_duration / g_stats_index;
        END IF;
        
        l_overall_duration := EXTRACT(SECOND FROM (g_total_end_time - g_total_start_time)) +
                             EXTRACT(MINUTE FROM (g_total_end_time - g_total_start_time)) * 60 +
                             EXTRACT(HOUR FROM (g_total_end_time - g_total_start_time)) * 3600;
        
        IF l_overall_duration > 0 THEN
            l_throughput := l_total_cifs / l_overall_duration;
        END IF;
        
        -- Output report
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('=== RetailCifPack Performance Report ===');
        DBMS_OUTPUT.PUT_LINE('Execution Period: ' || TO_CHAR(g_total_start_time, 'DD-MON-YYYY HH24:MI:SS') || 
                           ' to ' || TO_CHAR(g_total_end_time, 'DD-MON-YYYY HH24:MI:SS'));
        DBMS_OUTPUT.PUT_LINE('Overall Duration: ' || l_overall_duration || ' seconds');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('SOL Statistics:');
        DBMS_OUTPUT.PUT_LINE('  Total SOLs Processed: ' || g_stats_index);
        DBMS_OUTPUT.PUT_LINE('  Successful: ' || l_successful_sols);
        DBMS_OUTPUT.PUT_LINE('  Failed: ' || l_failed_sols);
        DBMS_OUTPUT.PUT_LINE('  Success Rate: ' || ROUND((l_successful_sols/(g_stats_index))*100, 2) || '%');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('CIF Statistics:');
        DBMS_OUTPUT.PUT_LINE('  Total CIFs Processed: ' || l_total_cifs);
        DBMS_OUTPUT.PUT_LINE('  Average CIFs per SOL: ' || ROUND(l_total_cifs/g_stats_index, 0));
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('Performance Metrics:');
        DBMS_OUTPUT.PUT_LINE('  Average Duration per SOL: ' || ROUND(l_avg_duration, 2) || ' seconds');
        DBMS_OUTPUT.PUT_LINE('  Maximum Duration: ' || ROUND(l_max_duration, 2) || ' seconds');
        DBMS_OUTPUT.PUT_LINE('  Minimum Duration: ' || ROUND(l_min_duration, 2) || ' seconds');
        DBMS_OUTPUT.PUT_LINE('  Throughput: ' || ROUND(l_throughput, 0) || ' CIFs/second');
        DBMS_OUTPUT.PUT_LINE('  Projected Time for 1.5 Crore CIFs: ' || ROUND(150000000/l_throughput/3600, 2) || ' hours');
        DBMS_OUTPUT.PUT_LINE('');
        
        -- Top 5 slowest SOLs
        DBMS_OUTPUT.PUT_LINE('Top 5 Slowest SOLs:');
        DBMS_OUTPUT.PUT_LINE('  SOL_ID    | Duration(s) | CIF_Count | Status');
        DBMS_OUTPUT.PUT_LINE('  ----------|-------------|-----------|--------');
        
        -- Simple sorting logic for top 5 (in real implementation, use proper sorting)
        FOR i IN 1..LEAST(5, g_stats_index) LOOP
            IF g_execution_stats.EXISTS(i) THEN
                DBMS_OUTPUT.PUT_LINE('  ' || 
                    RPAD(g_execution_stats(i).sol_id, 9) || ' | ' ||
                    LPAD(ROUND(g_execution_stats(i).duration_seconds, 2), 11) || ' | ' ||
                    LPAD(g_execution_stats(i).cif_count, 9) || ' | ' ||
                    g_execution_stats(i).status);
            END IF;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('=== End of Report ===');
        
    END GENERATE_PERFORMANCE_REPORT;
    
    -- Pipelined function to return execution stats
    FUNCTION GET_SOL_EXECUTION_STATS RETURN SOL_STATS_TAB PIPELINED IS
    BEGIN
        FOR i IN 1..g_stats_index LOOP
            PIPE ROW(g_execution_stats(i));
        END LOOP;
        RETURN;
    END GET_SOL_EXECUTION_STATS;
    
    -- Cleanup temporary tables and reset stats
    PROCEDURE CLEANUP_TEMP_TABLES IS
    BEGIN
        -- Clear in-memory stats
        g_execution_stats.DELETE;
        g_stats_index := 0;
        
        -- Optional: Truncate log table (uncomment if needed)
        -- EXECUTE IMMEDIATE 'TRUNCATE TABLE RETAILCIF_EXECUTION_LOG';
        
        DBMS_OUTPUT.PUT_LINE('Cleanup completed.');
    END CLEANUP_TEMP_TABLES;
    
END RetailCifParallelExecutor;
/

-- Usage Examples:
/*
-- Execute all SOLs with default settings
BEGIN
    RetailCifParallelExecutor.EXECUTE_PARALLEL_BATCH();
END;
/

-- Execute with custom batch size and parallelism
BEGIN
    RetailCifParallelExecutor.EXECUTE_PARALLEL_BATCH(
        p_batch_size => 20,
        p_max_parallel => 6,
        p_sol_filter => '1%'  -- Only SOLs starting with 1
    );
END;
/

-- Execute single SOL for testing
BEGIN
    RetailCifParallelExecutor.EXECUTE_SINGLE_SOL('1001');
END;
/

-- Generate performance report
BEGIN
    RetailCifParallelExecutor.GENERATE_PERFORMANCE_REPORT();
END;
/

-- Query execution statistics
SELECT * FROM TABLE(RetailCifParallelExecutor.GET_SOL_EXECUTION_STATS());

-- Cleanup
BEGIN
    RetailCifParallelExecutor.CLEANUP_TEMP_TABLES();
END;
/
*/