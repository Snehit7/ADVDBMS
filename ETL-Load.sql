-- -- DATA INTEGRATION SCRIPT (Task 3.1 Extended) -- Load to Star Schema
-- SET SERVEROUTPUT ON;

-- 1. Drop DIM and FACT tables if already exist
BEGIN 
    EXECUTE IMMEDIATE 'DROP TABLE FACT_CRIME CASCADE CONSTRAINTS'; 
EXCEPTION 
    WHEN OTHERS THEN 
        IF SQLCODE != -942 THEN RAISE; END IF; 
END; 
/

BEGIN 
    EXECUTE IMMEDIATE 'DROP TABLE DIM_DATE CASCADE CONSTRAINTS'; 
EXCEPTION 
    WHEN OTHERS THEN 
        IF SQLCODE != -942 THEN RAISE; END IF; 
END; 
/

BEGIN 
    EXECUTE IMMEDIATE 'DROP TABLE DIM_LOCATION CASCADE CONSTRAINTS'; 
EXCEPTION 
    WHEN OTHERS THEN 
        IF SQLCODE != -942 THEN RAISE; END IF; 
END; 
/

BEGIN 
    EXECUTE IMMEDIATE 'DROP TABLE DIM_CRIME_TYPE CASCADE CONSTRAINTS'; 
EXCEPTION 
    WHEN OTHERS THEN 
        IF SQLCODE != -942 THEN RAISE; END IF; 
END; 
/

BEGIN 
    EXECUTE IMMEDIATE 'DROP TABLE DIM_POLICE_STATION CASCADE CONSTRAINTS'; 
EXCEPTION 
    WHEN OTHERS THEN 
        IF SQLCODE != -942 THEN RAISE; END IF; 
END; 
/

-- 2. Create DIM_DATE
CREATE TABLE DIM_DATE (
    DATE_KEY NUMBER PRIMARY KEY,
    FULL_DATE DATE,
    YEAR NUMBER,
    MONTH NUMBER,
    QUARTER NUMBER,
    DAY NUMBER
);
/

-- 3. Create DIM_LOCATION
CREATE TABLE DIM_LOCATION (
    LOCATION_KEY NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    LOCATION_NAME VARCHAR2(100) UNIQUE
);
/

-- 4. Create DIM_CRIME_TYPE
CREATE TABLE DIM_CRIME_TYPE (
    CRIME_TYPE_KEY NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    CRIME_TYPE VARCHAR2(200) UNIQUE
);
/

-- 5. Create DIM_POLICE_STATION
CREATE TABLE DIM_POLICE_STATION (
    PS_KEY NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PS_NAME VARCHAR2(200) UNIQUE
);
/

-- 6. Create FACT_CRIME
CREATE TABLE FACT_CRIME (
    FACT_KEY NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    DATE_KEY NUMBER,
    LOCATION_KEY NUMBER,
    CRIME_TYPE_KEY NUMBER,
    PS_KEY NUMBER,
    SOURCE_SYSTEM VARCHAR2(20),
    CRIME_STATUS VARCHAR2(30),
    CLOSED_DATE DATE,
    OFFICER_ID NUMBER,
    CRIME_COUNT NUMBER,
    FOREIGN KEY (DATE_KEY) REFERENCES DIM_DATE(DATE_KEY),
    FOREIGN KEY (LOCATION_KEY) REFERENCES DIM_LOCATION(LOCATION_KEY),
    FOREIGN KEY (CRIME_TYPE_KEY) REFERENCES DIM_CRIME_TYPE(CRIME_TYPE_KEY),
    FOREIGN KEY (PS_KEY) REFERENCES DIM_POLICE_STATION(PS_KEY)
);
/

-- 7. Loading Procedure: Clean, Transform, and Load Good Data to Star Schema
DECLARE
    -- Cursor for GOOD data only
    CURSOR c_good IS
        SELECT *
        FROM STG_CRIME m
        WHERE m.merged_id NOT IN (SELECT staging_id FROM ERROR_LOG);
    
    -- Cleaned values
    v_postcode VARCHAR2(100);
    v_crime_type VARCHAR2(200);
    v_location_name VARCHAR2(200);
    v_ps_name VARCHAR2(200);
    v_crime_date DATE;
    
    -- Date components
    v_date_key NUMBER;
    v_year NUMBER;
    v_month NUMBER;
    v_quarter NUMBER;
    v_day NUMBER;
    
    -- Surrogate keys
    v_location_key NUMBER;
    v_crime_type_key NUMBER;
    v_ps_key NUMBER;
    
    -- Fact values
    v_crime_count NUMBER;
    v_error_flag BOOLEAN := FALSE;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting Loading to Star Schema...');
    DBMS_OUTPUT.PUT_LINE('------------------------------------------------');
    
    FOR r IN c_good LOOP
        v_error_flag := FALSE;
        
        ----------------------------------------------------------
        -- 1. Handle Date (CRIME_DATE or LEDS_MONTH)
        ----------------------------------------------------------
        IF r.crime_date IS NOT NULL THEN
            v_crime_date := r.crime_date;
        ELSIF r.leds_month IS NOT NULL THEN
            -- Assume LEDS_MONTH format 'YYYY-MM', set to first of month
            BEGIN
                v_crime_date := TO_DATE(r.leds_month || '-01', 'YYYY-MM-DD');
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Invalid LEDS_MONTH for ID: ' || r.merged_id);
                    v_error_flag := TRUE;
            END;
        ELSE
            DBMS_OUTPUT.PUT_LINE('No date for ID: ' || r.merged_id);
            v_error_flag := TRUE;
        END IF;
        
        IF v_error_flag THEN CONTINUE; END IF;
        
        ----------------------------------------------------------
        -- 2. CLEAN POSTCODE / LOCATION (uppercase + trimmed)
        ----------------------------------------------------------
        IF r.source_system IN ('PRCS', 'PS_WALES') THEN
            v_postcode := UPPER(TRIM(r.postcode));
            v_location_name := v_postcode;
        ELSIF r.source_system = 'CRIME_LEEDS' THEN
            v_location_name := TRIM(r.leds_neighbourhood);
        ELSE
            v_location_name := 'Unknown';
        END IF;
        
        IF v_location_name IS NULL THEN
            v_location_name := 'Unknown';
        END IF;
        
        ----------------------------------------------------------
        -- 3. CLEAN CRIME TYPE (Initcap + trimmed)
        ----------------------------------------------------------
        IF r.crime_type IS NOT NULL THEN
            v_crime_type := INITCAP(TRIM(r.crime_type));
        ELSIF r.source_system = 'CRIME_LEEDS' THEN
            v_crime_type := 'All Crime';
        ELSE
            v_crime_type := 'Unknown';
        END IF;
        
        ----------------------------------------------------------
        -- 4. CLEAN POLICE STATION NAME (if exists)
        ----------------------------------------------------------
        IF r.station_id IS NOT NULL THEN
            v_ps_name := INITCAP(TRIM(TO_CHAR(r.station_id)));
        ELSIF r.source_system = 'CRIME_LEEDS' THEN
            v_ps_name := TRIM(r.leds_force);
        ELSE
            v_ps_name := 'Unknown';
        END IF;
        
        IF v_ps_name IS NULL THEN
            v_ps_name := 'Unknown';
        END IF;
        
        ----------------------------------------------------------
        -- 5. GENERATE DATE_KEY (YYYYMMDD) and Components
        ----------------------------------------------------------
        v_date_key := TO_NUMBER(TO_CHAR(v_crime_date, 'YYYYMMDD'));
        v_year := TO_NUMBER(TO_CHAR(v_crime_date, 'YYYY'));
        v_month := TO_NUMBER(TO_CHAR(v_crime_date, 'MM'));
        v_quarter := TO_NUMBER(TO_CHAR(v_crime_date, 'Q'));
        v_day := TO_NUMBER(TO_CHAR(v_crime_date, 'DD'));
        
        ----------------------------------------------------------
        -- 6. INSERT/GET SURROGATE KEYS
        ----------------------------------------------------------
        -- DIM_DATE (use MERGE to avoid duplicates)
        MERGE INTO DIM_DATE d
        USING (SELECT v_date_key AS date_key, v_crime_date AS full_date, v_year AS year, v_month AS month, v_quarter AS quarter, v_day AS day FROM dual) s
        ON (d.date_key = s.date_key)
        WHEN NOT MATCHED THEN
            INSERT (date_key, full_date, year, month, quarter, day)
            VALUES (s.date_key, s.full_date, s.year, s.month, s.quarter, s.day);
        
        -- DIM_LOCATION
        BEGIN
            SELECT LOCATION_KEY INTO v_location_key FROM DIM_LOCATION WHERE LOCATION_NAME = v_location_name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                INSERT INTO DIM_LOCATION (LOCATION_NAME) VALUES (v_location_name) RETURNING LOCATION_KEY INTO v_location_key;
        END;
        
        -- DIM_CRIME_TYPE
        BEGIN
            SELECT CRIME_TYPE_KEY INTO v_crime_type_key FROM DIM_CRIME_TYPE WHERE CRIME_TYPE = v_crime_type;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                INSERT INTO DIM_CRIME_TYPE (CRIME_TYPE) VALUES (v_crime_type) RETURNING CRIME_TYPE_KEY INTO v_crime_type_key;
        END;
        
        -- DIM_POLICE_STATION
        BEGIN
            SELECT PS_KEY INTO v_ps_key FROM DIM_POLICE_STATION WHERE PS_NAME = v_ps_name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                INSERT INTO DIM_POLICE_STATION (PS_NAME) VALUES (v_ps_name) RETURNING PS_KEY INTO v_ps_key;
        END;
        
        ----------------------------------------------------------
        -- 7. FACT TABLE INSERT
        ----------------------------------------------------------
        v_crime_count := CASE WHEN r.source_system = 'CRIME_LEEDS' AND r.leds_total_crime IS NOT NULL THEN r.leds_total_crime ELSE 1 END;
        
        INSERT INTO FACT_CRIME (
            DATE_KEY,
            LOCATION_KEY,
            CRIME_TYPE_KEY,
            PS_KEY,
            SOURCE_SYSTEM,
            CRIME_STATUS,
            CLOSED_DATE,
            OFFICER_ID,
            CRIME_COUNT
        ) VALUES (
            v_date_key,
            v_location_key,
            v_crime_type_key,
            v_ps_key,
            r.source_system,
            r.crime_status,
            r.closed_date,
            r.officer_id,
            v_crime_count
        );
        
        ----------------------------------------------------------
        -- OUTPUT FOR DEBUG
        ----------------------------------------------------------
        DBMS_OUTPUT.PUT_LINE('Loaded Record ID: ' || r.merged_id);
        DBMS_OUTPUT.PUT_LINE(' Clean Postcode/Location = ' || v_location_name);
        DBMS_OUTPUT.PUT_LINE(' Clean Crime Type = ' || v_crime_type);
        DBMS_OUTPUT.PUT_LINE(' Clean PS Name = ' || v_ps_name);
        DBMS_OUTPUT.PUT_LINE(' Derived Date Key = ' || v_date_key);
        DBMS_OUTPUT.PUT_LINE(' Crime Count = ' || v_crime_count);
        DBMS_OUTPUT.PUT_LINE('------------------------------------------------');
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Loading to Star Schema Completed Successfully.');
END;
/