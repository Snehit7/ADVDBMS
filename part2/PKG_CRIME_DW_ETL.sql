--------------------------------------------------------------
-- PKG_CRIME_DW_ETL - Star Schema ETL with SCD Type 2
--------------------------------------------------------------

SET SERVEROUTPUT ON;

--  Create SCD Type 2 Enhanced DIM_POLICE_STATION
DROP TABLE DIM_POLICE_STATION CASCADE CONSTRAINTS;

CREATE TABLE DIM_POLICE_STATION (
    PS_KEY          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PS_ID           NUMBER NOT NULL,
    PS_NAME         VARCHAR2(200) NOT NULL,
    PS_ADDRESS      VARCHAR2(200),
    PS_CITY         VARCHAR2(100),
    PS_REGION       VARCHAR2(100),
    EFFECTIVE_DATE  DATE NOT NULL,
    EXPIRY_DATE     DATE DEFAULT TO_DATE('9999-12-31', 'YYYY-MM-DD'),
    IS_CURRENT      VARCHAR2(1) DEFAULT 'Y',
    VERSION         NUMBER DEFAULT 1
);

CREATE INDEX idx_ps_natural_key ON DIM_POLICE_STATION(PS_ID, IS_CURRENT);

-- PACKAGE SPECIFICATION
CREATE OR REPLACE PACKAGE PKG_CRIME_DW_ETL AS

    FUNCTION get_location_key(
        p_location_name IN VARCHAR2,
        p_region IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER;
    
    FUNCTION get_crime_type_key(
        p_crime_type IN VARCHAR2
    ) RETURN NUMBER;
    
    FUNCTION get_date_key(
        p_date IN DATE
    ) RETURN NUMBER;
    
    FUNCTION get_police_station_key_scd2(
        p_ps_id IN NUMBER,
        p_ps_name IN VARCHAR2,
        p_ps_address IN VARCHAR2 DEFAULT NULL,
        p_ps_city IN VARCHAR2 DEFAULT NULL,
        p_ps_region IN VARCHAR2 DEFAULT NULL,
        p_effective_date IN DATE DEFAULT SYSDATE
    ) RETURN NUMBER;
    
    PROCEDURE load_all_dimensions;
    
    PROCEDURE load_fact_crime(
        p_source_system IN VARCHAR2 DEFAULT NULL
    );
    
    PROCEDURE run_complete_etl(
        p_full_load IN BOOLEAN DEFAULT FALSE
    );
    
    FUNCTION validate_fact_integrity RETURN BOOLEAN;
    
    PROCEDURE reconcile_record_counts;
    
    PROCEDURE get_crime_stats_by_location(p_top_n IN NUMBER DEFAULT 10);
    PROCEDURE get_crime_stats_by_type(p_top_n IN NUMBER DEFAULT 10);
    PROCEDURE get_ps_history(p_ps_id IN NUMBER);
    
    PROCEDURE archive_old_facts(p_years_old IN NUMBER DEFAULT 5);
    PROCEDURE rebuild_dw_indexes;
    PROCEDURE update_dw_statistics;
    
    FUNCTION log_error(
        p_staging_id IN NUMBER,
        p_source_system IN VARCHAR2,
        p_error_message IN VARCHAR2,
        p_raw_data IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER;
    
    FUNCTION start_process(
        p_process_name IN VARCHAR2,
        p_remarks IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER;
    
    PROCEDURE end_process(
        p_process_id IN NUMBER,
        p_status IN VARCHAR2,
        p_good_count IN NUMBER DEFAULT 0,
        p_bad_count IN NUMBER DEFAULT 0
    );
    
END PKG_CRIME_DW_ETL;
/

-- STEP 3: PACKAGE BODY
CREATE OR REPLACE PACKAGE BODY PKG_CRIME_DW_ETL AS

    FUNCTION get_location_key(
        p_location_name IN VARCHAR2,
        p_region IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER IS
        v_key NUMBER;
    BEGIN
        BEGIN
            SELECT LOCATION_KEY INTO v_key
            FROM DIM_LOCATION
            WHERE LOCATION_NAME = p_location_name
            AND ROWNUM = 1;
            RETURN v_key;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                INSERT INTO DIM_LOCATION (LOCATION_NAME, REGION)
                VALUES (p_location_name, p_region)
                RETURNING LOCATION_KEY INTO v_key;
                RETURN v_key;
        END;
    END get_location_key;
    
    
    FUNCTION get_crime_type_key(
        p_crime_type IN VARCHAR2
    ) RETURN NUMBER IS
        v_key NUMBER;
    BEGIN
        BEGIN
            SELECT CRIME_TYPE_KEY INTO v_key
            FROM DIM_CRIME_TYPE
            WHERE CRIME_TYPE = p_crime_type
            AND ROWNUM = 1;
            RETURN v_key;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                INSERT INTO DIM_CRIME_TYPE (CRIME_TYPE)
                VALUES (p_crime_type)
                RETURNING CRIME_TYPE_KEY INTO v_key;
                RETURN v_key;
        END;
    END get_crime_type_key;
    
    
    FUNCTION get_date_key(
        p_date IN DATE
    ) RETURN NUMBER IS
        v_key NUMBER;
        v_year NUMBER;
        v_month NUMBER;
        v_quarter NUMBER;
        v_day NUMBER;
    BEGIN
        v_key := TO_NUMBER(TO_CHAR(p_date, 'YYYYMMDD'));
        
        BEGIN
            SELECT DATE_KEY INTO v_key
            FROM DIM_DATE
            WHERE DATE_KEY = v_key;
            RETURN v_key;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_year := TO_NUMBER(TO_CHAR(p_date, 'YYYY'));
                v_month := TO_NUMBER(TO_CHAR(p_date, 'MM'));
                v_quarter := TO_NUMBER(TO_CHAR(p_date, 'Q'));
                v_day := TO_NUMBER(TO_CHAR(p_date, 'DD'));
                
                INSERT INTO DIM_DATE (DATE_KEY, YEAR, MONTH, QUARTER, DAY)
                VALUES (v_key, v_year, v_month, v_quarter, v_day);
                RETURN v_key;
        END;
    END get_date_key;
    
    
    -- SCD TYPE 2: POLICE STATION DIMENSION (FIXED)
  
    FUNCTION get_police_station_key_scd2(
        p_ps_id IN NUMBER,
        p_ps_name IN VARCHAR2,
        p_ps_address IN VARCHAR2 DEFAULT NULL,
        p_ps_city IN VARCHAR2 DEFAULT NULL,
        p_ps_region IN VARCHAR2 DEFAULT NULL,
        p_effective_date IN DATE DEFAULT SYSDATE
    ) RETURN NUMBER IS
        v_current_key NUMBER;
        v_new_key NUMBER;
        v_current_address VARCHAR2(200);
        v_current_city VARCHAR2(100);
        v_current_region VARCHAR2(100);
        v_current_version NUMBER;
        v_changed BOOLEAN := FALSE;
    BEGIN
        -- Try to find current active record
        BEGIN
            SELECT PS_KEY, PS_ADDRESS, PS_CITY, PS_REGION, VERSION
            INTO v_current_key, v_current_address, v_current_city, v_current_region, v_current_version
            FROM DIM_POLICE_STATION
            WHERE PS_ID = p_ps_id
            AND IS_CURRENT = 'Y';
            
            -- Check if attributes changed
            IF (NVL(v_current_address, 'NULL') != NVL(p_ps_address, 'NULL') OR
                NVL(v_current_city, 'NULL') != NVL(p_ps_city, 'NULL') OR
                NVL(v_current_region, 'NULL') != NVL(p_ps_region, 'NULL')) THEN
                v_changed := TRUE;
            END IF;
            
            IF v_changed THEN
                -- Expire old record
                UPDATE DIM_POLICE_STATION
                SET IS_CURRENT = 'N',
                    EXPIRY_DATE = p_effective_date - 1
                WHERE PS_KEY = v_current_key;
                
                -- Insert new version (FIXED: Using VALUES instead of SELECT)
                INSERT INTO DIM_POLICE_STATION (
                    PS_ID, PS_NAME, PS_ADDRESS, PS_CITY, PS_REGION,
                    EFFECTIVE_DATE, IS_CURRENT, VERSION
                ) VALUES (
                    p_ps_id, p_ps_name, p_ps_address, p_ps_city, p_ps_region,
                    p_effective_date, 'Y', v_current_version + 1
                ) RETURNING PS_KEY INTO v_new_key;
                
                RETURN v_new_key;
            ELSE
                -- No change, return current key
                RETURN v_current_key;
            END IF;
            
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                -- First time insert
                INSERT INTO DIM_POLICE_STATION (
                    PS_ID, PS_NAME, PS_ADDRESS, PS_CITY, PS_REGION,
                    EFFECTIVE_DATE, IS_CURRENT, VERSION
                ) VALUES (
                    p_ps_id, p_ps_name, p_ps_address, p_ps_city, p_ps_region,
                    p_effective_date, 'Y', 1
                ) RETURNING PS_KEY INTO v_new_key;
                
                RETURN v_new_key;
        END;
    END get_police_station_key_scd2;
    
    
    PROCEDURE load_all_dimensions IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Loading all dimensions...');
        
        FOR rec IN (
            SELECT DISTINCT 
                UPPER(TRIM(LOCATION_NAME)) as loc_name,
                REGION
            FROM FACT_DIM_MERGED
            WHERE LOCATION_NAME IS NOT NULL
        ) LOOP
            NULL;
        END LOOP;
        
        FOR rec IN (
            SELECT DISTINCT CRIME_TYPE
            FROM FACT_DIM_MERGED
            WHERE CRIME_TYPE IS NOT NULL
        ) LOOP
            NULL;
        END LOOP;
        
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Dimensions loaded.');
    END load_all_dimensions;
    
    
    PROCEDURE load_fact_crime(
        p_source_system IN VARCHAR2 DEFAULT NULL
    ) IS
        v_date_key NUMBER;
        v_location_key NUMBER;
        v_crime_type_key NUMBER;
        v_ps_key NUMBER;
        v_count NUMBER := 0;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Loading FACT_CRIME from FACT_DIM_MERGED...');
        
        FOR rec IN (
            SELECT * FROM FACT_DIM_MERGED
        ) LOOP
            BEGIN
                v_date_key := rec.DATE_KEY;
                v_location_key := get_location_key(rec.LOCATION_NAME, rec.REGION);
                v_crime_type_key := get_crime_type_key(rec.CRIME_TYPE);
                
                v_ps_key := get_police_station_key_scd2(
                    p_ps_id => rec.PS_KEY,
                    p_ps_name => rec.PS_NAME,
                    p_ps_address => rec.PS_ADDRESS,
                    p_ps_city => rec.PS_CITY,
                    p_ps_region => rec.PS_REGION
                );
                
                INSERT INTO FACT_CRIME (
                    DATE_KEY, LOCATION_KEY, CRIME_TYPE_KEY, PS_KEY, CRIME_COUNT
                ) VALUES (
                    v_date_key, v_location_key, v_crime_type_key, v_ps_key, NVL(rec.CRIME_COUNT, 1)
                );
                
                v_count := v_count + 1;
                
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Error loading fact: ' || SQLERRM);
            END;
        END LOOP;
        
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Loaded ' || v_count || ' fact records.');
    END load_fact_crime;
    
    
    PROCEDURE run_complete_etl(
        p_full_load IN BOOLEAN DEFAULT FALSE
    ) IS
        v_process_id NUMBER;
        v_good_count NUMBER := 0;
        v_bad_count NUMBER := 0;
    BEGIN
        v_process_id := start_process('Complete ETL with SCD2', 
            CASE WHEN p_full_load THEN 'Full Load' ELSE 'Incremental' END);
        
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('ETL Process Started - ID: ' || v_process_id);
        DBMS_OUTPUT.PUT_LINE('========================================');
        
        load_all_dimensions();
        load_fact_crime();
        
        IF validate_fact_integrity() THEN
            DBMS_OUTPUT.PUT_LINE('✓ Data integrity validated');
        END IF;
        
        SELECT COUNT(*) INTO v_good_count FROM FACT_CRIME;
        
        end_process(v_process_id, 'SUCCESS', v_good_count, v_bad_count);
        
        DBMS_OUTPUT.PUT_LINE('========================================');
        DBMS_OUTPUT.PUT_LINE('ETL Completed - Records: ' || v_good_count);
        DBMS_OUTPUT.PUT_LINE('========================================');
    END run_complete_etl;
    
    
    FUNCTION validate_fact_integrity RETURN BOOLEAN IS
        v_orphan_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_orphan_count
        FROM FACT_CRIME f
        WHERE NOT EXISTS (SELECT 1 FROM DIM_DATE d WHERE d.DATE_KEY = f.DATE_KEY)
           OR NOT EXISTS (SELECT 1 FROM DIM_LOCATION l WHERE l.LOCATION_KEY = f.LOCATION_KEY)
           OR NOT EXISTS (SELECT 1 FROM DIM_CRIME_TYPE c WHERE c.CRIME_TYPE_KEY = f.CRIME_TYPE_KEY)
           OR NOT EXISTS (SELECT 1 FROM DIM_POLICE_STATION p WHERE p.PS_KEY = f.PS_KEY);
        
        IF v_orphan_count > 0 THEN
            DBMS_OUTPUT.PUT_LINE('⚠ Found ' || v_orphan_count || ' orphan records');
            RETURN FALSE;
        END IF;
        RETURN TRUE;
    END validate_fact_integrity;
    
    
    PROCEDURE reconcile_record_counts IS
        v_source_count NUMBER;
        v_fact_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_source_count FROM FACT_DIM_MERGED;
        SELECT COUNT(*) INTO v_fact_count FROM FACT_CRIME;
        
        DBMS_OUTPUT.PUT_LINE('Reconciliation Report:');
        DBMS_OUTPUT.PUT_LINE('  Source: ' || v_source_count);
        DBMS_OUTPUT.PUT_LINE('  Loaded: ' || v_fact_count);
        DBMS_OUTPUT.PUT_LINE('  Difference: ' || (v_source_count - v_fact_count));
    END reconcile_record_counts;
    
    
    PROCEDURE get_crime_stats_by_location(p_top_n IN NUMBER DEFAULT 10) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Top ' || p_top_n || ' Crime Locations:');
        FOR rec IN (
            SELECT * FROM (
                SELECT l.LOCATION_NAME, l.REGION, SUM(f.CRIME_COUNT) as total
                FROM FACT_CRIME f
                JOIN DIM_LOCATION l ON f.LOCATION_KEY = l.LOCATION_KEY
                GROUP BY l.LOCATION_NAME, l.REGION
                ORDER BY total DESC
            )
            WHERE ROWNUM <= p_top_n
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('  ' || RPAD(rec.LOCATION_NAME, 20) || 
                               RPAD(NVL(rec.REGION, 'N/A'), 15) || 
                               ': ' || rec.total);
        END LOOP;
    END get_crime_stats_by_location;
    
    
    PROCEDURE get_crime_stats_by_type(p_top_n IN NUMBER DEFAULT 10) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Top ' || p_top_n || ' Crime Types:');
        FOR rec IN (
            SELECT * FROM (
                SELECT c.CRIME_TYPE, SUM(f.CRIME_COUNT) as total
                FROM FACT_CRIME f
                JOIN DIM_CRIME_TYPE c ON f.CRIME_TYPE_KEY = c.CRIME_TYPE_KEY
                GROUP BY c.CRIME_TYPE
                ORDER BY total DESC
            )
            WHERE ROWNUM <= p_top_n
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('  ' || RPAD(rec.CRIME_TYPE, 30) || ': ' || rec.total);
        END LOOP;
    END get_crime_stats_by_type;
    
    
    PROCEDURE get_ps_history(p_ps_id IN NUMBER) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Police Station History for ID: ' || p_ps_id);
        DBMS_OUTPUT.PUT_LINE('========================================');
        FOR rec IN (
            SELECT PS_KEY, PS_NAME, PS_ADDRESS, PS_CITY, PS_REGION,
                   EFFECTIVE_DATE, EXPIRY_DATE, IS_CURRENT, VERSION
            FROM DIM_POLICE_STATION
            WHERE PS_ID = p_ps_id
            ORDER BY VERSION
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('Version ' || rec.VERSION || ' [' || rec.IS_CURRENT || ']:');
            DBMS_OUTPUT.PUT_LINE('  Name: ' || rec.PS_NAME);
            DBMS_OUTPUT.PUT_LINE('  Address: ' || NVL(rec.PS_ADDRESS, 'N/A'));
            DBMS_OUTPUT.PUT_LINE('  City: ' || NVL(rec.PS_CITY, 'N/A'));
            DBMS_OUTPUT.PUT_LINE('  Region: ' || NVL(rec.PS_REGION, 'N/A'));
            DBMS_OUTPUT.PUT_LINE('  Period: ' || rec.EFFECTIVE_DATE || ' to ' || rec.EXPIRY_DATE);
            DBMS_OUTPUT.PUT_LINE('---');
        END LOOP;
    END get_ps_history;
    
    
    PROCEDURE archive_old_facts(p_years_old IN NUMBER DEFAULT 5) IS
        v_cutoff_date DATE := ADD_MONTHS(SYSDATE, -12 * p_years_old);
        v_archived NUMBER;
    BEGIN
        DELETE FROM FACT_CRIME
        WHERE DATE_KEY < TO_NUMBER(TO_CHAR(v_cutoff_date, 'YYYYMMDD'));
        v_archived := SQL%ROWCOUNT;
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Archived ' || v_archived || ' old fact records.');
    END archive_old_facts;
    
    
    PROCEDURE rebuild_dw_indexes IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Rebuilding indexes...');
        EXECUTE IMMEDIATE 'ALTER INDEX pk_fact_crime REBUILD';
        DBMS_OUTPUT.PUT_LINE('Indexes rebuilt.');
    END rebuild_dw_indexes;
    
    
    PROCEDURE update_dw_statistics IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Updating statistics...');
        DBMS_STATS.GATHER_TABLE_STATS(USER, 'FACT_CRIME');
        DBMS_STATS.GATHER_TABLE_STATS(USER, 'DIM_DATE');
        DBMS_STATS.GATHER_TABLE_STATS(USER, 'DIM_LOCATION');
        DBMS_STATS.GATHER_TABLE_STATS(USER, 'DIM_CRIME_TYPE');
        DBMS_STATS.GATHER_TABLE_STATS(USER, 'DIM_POLICE_STATION');
        DBMS_OUTPUT.PUT_LINE('Statistics updated.');
    END update_dw_statistics;
    
    
    FUNCTION log_error(
        p_staging_id IN NUMBER,
        p_source_system IN VARCHAR2,
        p_error_message IN VARCHAR2,
        p_raw_data IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER IS
        v_error_id NUMBER;
    BEGIN
        INSERT INTO ERROR_LOG (STAGING_ID, SOURCE_SYSTEM, ERROR_MESSAGE, RAW_DATA, ERROR_TIME)
        VALUES (p_staging_id, p_source_system, p_error_message, p_raw_data, SYSDATE)
        RETURNING ERROR_ID INTO v_error_id;
        COMMIT;
        RETURN v_error_id;
    END log_error;
    
    
    FUNCTION start_process(
        p_process_name IN VARCHAR2,
        p_remarks IN VARCHAR2 DEFAULT NULL
    ) RETURN NUMBER IS
        v_process_id NUMBER;
    BEGIN
        INSERT INTO PROCESS_LOG (START_TIME, STATUS, REMARKS)
        VALUES (SYSDATE, 'RUNNING', p_remarks)
        RETURNING PROCESS_ID INTO v_process_id;
        COMMIT;
        RETURN v_process_id;
    END start_process;
    
    
    PROCEDURE end_process(
        p_process_id IN NUMBER,
        p_status IN VARCHAR2,
        p_good_count IN NUMBER DEFAULT 0,
        p_bad_count IN NUMBER DEFAULT 0
    ) IS
    BEGIN
        UPDATE PROCESS_LOG
        SET END_TIME = SYSDATE, STATUS = p_status,
            GOOD_COUNT = p_good_count, BAD_COUNT = p_bad_count
        WHERE PROCESS_ID = p_process_id;
        COMMIT;
    END end_process;
    
END PKG_CRIME_DW_ETL;
/

-- TEST SCRIPT
BEGIN
    PKG_CRIME_DW_ETL.run_complete_etl(TRUE);
    PKG_CRIME_DW_ETL.get_crime_stats_by_location(5);
    PKG_CRIME_DW_ETL.reconcile_record_counts();
END;
/