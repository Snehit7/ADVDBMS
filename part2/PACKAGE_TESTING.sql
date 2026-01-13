SET SERVEROUTPUT ON SIZE UNLIMITED;

-- Basic ETL Execution
BEGIN
    PKG_CRIME_DW_ETL.run_complete_etl(p_full_load => TRUE);
END;
/

-- Verify data loaded
SELECT 'DIM_DATE' as TABLE_NAME, COUNT(*) as RECORD_COUNT FROM DIM_DATE
UNION ALL
SELECT 'DIM_LOCATION', COUNT(*) FROM DIM_LOCATION
UNION ALL
SELECT 'DIM_CRIME_TYPE', COUNT(*) FROM DIM_CRIME_TYPE
UNION ALL
SELECT 'DIM_POLICE_STATION', COUNT(*) FROM DIM_POLICE_STATION
UNION ALL
SELECT 'FACT_CRIME', COUNT(*) FROM FACT_CRIME
UNION ALL
SELECT 'ERROR_LOG', COUNT(*) FROM ERROR_LOG
UNION ALL
SELECT 'PROCESS_LOG', COUNT(*) FROM PROCESS_LOG;

-- Analytics - Crime by Location (Top 5)
BEGIN
    PKG_CRIME_DW_ETL.get_crime_stats_by_location(5);
END;
/

-- Analytics - Crime by Type (Top 5)
BEGIN
    PKG_CRIME_DW_ETL.get_crime_stats_by_type(5);
END;
/

-- Record Count Reconciliation
BEGIN
    PKG_CRIME_DW_ETL.reconcile_record_counts();
END;
/

-- Data Integrity Check
DECLARE
    v_is_valid BOOLEAN;
BEGIN
    v_is_valid := PKG_CRIME_DW_ETL.validate_fact_integrity();
    
    IF v_is_valid THEN
        DBMS_OUTPUT.PUT_LINE('✓ All integrity checks passed!');
    ELSE
        DBMS_OUTPUT.PUT_LINE('✗ Integrity issues found - check output above');
    END IF;
END;
/

-- SCD Type 2 - View Police Stations
SELECT DISTINCT PS_ID, PS_NAME 
FROM DIM_POLICE_STATION 
WHERE ROWNUM <= 3
ORDER BY PS_ID;

-- SCD Type 2 - View history for first police station
DECLARE
    v_ps_id NUMBER;
BEGIN
    SELECT MIN(PS_ID) INTO v_ps_id FROM DIM_POLICE_STATION;
    
    IF v_ps_id IS NOT NULL THEN
        PKG_CRIME_DW_ETL.get_ps_history(v_ps_id);
    ELSE
        DBMS_OUTPUT.PUT_LINE('No police stations found in dimension table');
    END IF;
END;
/

-- SCD Type 2 - Simulate Address Change
DECLARE
    v_ps_id NUMBER;
    v_old_key NUMBER;
    v_new_key NUMBER;
BEGIN
    -- Get first police station
    SELECT MIN(PS_ID) INTO v_ps_id FROM DIM_POLICE_STATION;
    
    IF v_ps_id IS NOT NULL THEN
        DBMS_OUTPUT.PUT_LINE('Testing SCD Type 2 for PS_ID: ' || v_ps_id);
        
        -- Get current key
        SELECT PS_KEY INTO v_old_key 
        FROM DIM_POLICE_STATION 
        WHERE PS_ID = v_ps_id AND IS_CURRENT = 'Y';
        
        DBMS_OUTPUT.PUT_LINE('Current PS_KEY: ' || v_old_key);
        
        -- Simulate address change
        v_new_key := PKG_CRIME_DW_ETL.get_police_station_key_scd2(
            p_ps_id => v_ps_id,
            p_ps_name => 'Test Station',
            p_ps_address => '123 New Address Street',
            p_ps_city => 'New City',
            p_ps_region => 'New Region',
            p_effective_date => SYSDATE
        );
        
        DBMS_OUTPUT.PUT_LINE('New PS_KEY after change: ' || v_new_key);
        
        -- Show updated history
        PKG_CRIME_DW_ETL.get_ps_history(v_ps_id);
    END IF;
END;
/

-- View all SCD Type 2 versions
SELECT 
    PS_ID,
    PS_NAME,
    PS_ADDRESS,
    PS_CITY,
    VERSION,
    IS_CURRENT,
    TO_CHAR(EFFECTIVE_DATE, 'YYYY-MM-DD') as EFF_DATE,
    TO_CHAR(EXPIRY_DATE, 'YYYY-MM-DD') as EXP_DATE
FROM DIM_POLICE_STATION
ORDER BY PS_ID, VERSION;

-- Process Log Summary
SELECT 
    PROCESS_ID,
    TO_CHAR(START_TIME, 'YYYY-MM-DD HH24:MI:SS') as START_TIME,
    TO_CHAR(END_TIME, 'YYYY-MM-DD HH24:MI:SS') as END_TIME,
    STATUS,
    GOOD_COUNT,
    BAD_COUNT,
    REMARKS
FROM PROCESS_LOG
ORDER BY PROCESS_ID DESC;

-- Error Log Summary
SELECT 
    ERROR_ID,
    SOURCE_SYSTEM,
    TO_CHAR(ERROR_TIME, 'YYYY-MM-DD HH24:MI:SS') as ERROR_TIME,
    SUBSTR(ERROR_MESSAGE, 1, 50) as ERROR_MESSAGE
FROM ERROR_LOG
WHERE ROWNUM <= 10
ORDER BY ERROR_ID DESC;

-- Sample Analytical Query - Crime trends
SELECT 
    d.YEAR,
    d.MONTH,
    l.LOCATION_NAME,
    c.CRIME_TYPE,
    SUM(f.CRIME_COUNT) as TOTAL_CRIMES
FROM FACT_CRIME f
JOIN DIM_DATE d ON f.DATE_KEY = d.DATE_KEY
JOIN DIM_LOCATION l ON f.LOCATION_KEY = l.LOCATION_KEY
JOIN DIM_CRIME_TYPE c ON f.CRIME_TYPE_KEY = c.CRIME_TYPE_KEY
WHERE ROWNUM <= 20
GROUP BY d.YEAR, d.MONTH, l.LOCATION_NAME, c.CRIME_TYPE
ORDER BY d.YEAR DESC, d.MONTH DESC, TOTAL_CRIMES DESC;

PROMPT Test Suite Complete!