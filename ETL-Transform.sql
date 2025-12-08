-- CLEANING + TRANSFORMATION LOGIC 
-- GOOD DATA ONLY
DECLARE
    -- Cursor for GOOD data only
    CURSOR c_good IS
        SELECT *
        FROM STG_CRIME m
        WHERE m.merged_id NOT IN (SELECT staging_id FROM ERROR_LOG);

    -- Cleaned values
    v_postcode        VARCHAR2(100);
    v_crime_type      VARCHAR2(200);
    v_location_name   VARCHAR2(200);
    v_ps_name         VARCHAR2(200);

    -- Date components
    v_date_key   NUMBER;
    v_year       NUMBER;
    v_month      NUMBER;
    v_quarter    NUMBER;
    v_day        NUMBER;

    -- Surrogate keys (for future dimension + fact loading)
    v_location_key     NUMBER;
    v_crime_type_key   NUMBER;
    v_ps_key           NUMBER;
    v_fact_key         NUMBER;
    v_crime_count      NUMBER;

BEGIN
    DBMS_OUTPUT.PUT_LINE('Starting Step 5: Cleaning & Transformation...');
    DBMS_OUTPUT.PUT_LINE('------------------------------------------------');

    FOR r IN c_good LOOP
        
        ----------------------------------------------------------
        -- 1. CLEAN POSTCODE (uppercase + trimmed)
        ----------------------------------------------------------
        v_postcode := UPPER(TRIM(r.postcode));
        ----------------------------------------------------------
        -- 2. CLEAN CRIME TYPE (Initcap + trimmed)
        ----------------------------------------------------------
        v_crime_type := INITCAP(TRIM(r.crime_type));
        ----------------------------------------------------------
        -- 3. GENERATE DATE_KEY (YYYYMMDD)
        ----------------------------------------------------------
        IF r.crime_date IS NOT NULL THEN
            v_date_key := TO_NUMBER(TO_CHAR(r.crime_date, 'YYYYMMDD'));
        ELSE
            v_date_key := NULL;
        END IF;

        ----------------------------------------------------------
        -- 4. DATE COMPONENTS (DIM_DATE VALUES)
        ----------------------------------------------------------
        IF r.crime_date IS NOT NULL THEN
            v_year    := TO_NUMBER(TO_CHAR(r.crime_date, 'YYYY'));
            v_month   := TO_NUMBER(TO_CHAR(r.crime_date, 'MM'));
            v_quarter := TO_NUMBER(TO_CHAR(r.crime_date, 'Q'));
            v_day     := TO_NUMBER(TO_CHAR(r.crime_date, 'DD'));
        END IF;

        ----------------------------------------------------------
        -- 5. SET LOCATION NAME (Postcode used as location granularity)
        ----------------------------------------------------------
        v_location_name := v_postcode;

        ----------------------------------------------------------
        -- 6. CLEAN POLICE STATION NAME (if exists)
        ----------------------------------------------------------
        IF r.station_id IS NOT NULL THEN
            v_ps_name := INITCAP(TRIM(r.station_id)); 
        END IF;

        ----------------------------------------------------------
        -- 7. FUTURE STEP: LOOKUP/INSERT SURROGATE KEYS
        -- (Not executed here – only placeholders)
        ----------------------------------------------------------
        v_location_key := NULL;
        v_crime_type_key := NULL;
        v_ps_key := NULL;

        ----------------------------------------------------------
        -- 8. FACT TABLE PREP
        ----------------------------------------------------------
        v_fact_key := NULL;  -- to be generated later from FACT_CRIME_SEQ
        v_crime_count := 1; 

        ----------------------------------------------------------
        -- OUTPUT FOR DEBUG
        ----------------------------------------------------------
        DBMS_OUTPUT.PUT_LINE('Record ID: ' || r.merged_id);
        DBMS_OUTPUT.PUT_LINE('  Clean Postcode      = ' || v_postcode);
        DBMS_OUTPUT.PUT_LINE('  Clean Crime Type    = ' || v_crime_type);
        DBMS_OUTPUT.PUT_LINE('  Location Name       = ' || v_location_name);
        DBMS_OUTPUT.PUT_LINE('  Derived Date Key    = ' || v_date_key);
        DBMS_OUTPUT.PUT_LINE('  Crime Count         = ' || v_crime_count);
        DBMS_OUTPUT.PUT_LINE('------------------------------------------------');

    END LOOP;

    DBMS_OUTPUT.PUT_LINE('Step 5 Completed Successfully.');
END;
/
