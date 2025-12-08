DECLARE
    v_good_count NUMBER := 0;
    v_bad_count  NUMBER := 0;

    CURSOR c_stg IS
        SELECT *
        FROM STG_CRIME; 

BEGIN
    FOR r IN c_stg LOOP

        -- Mandatory fields must NOT be NULL
        IF r.crime_date IS NULL 
           AND r.leds_month IS NULL THEN  
           
           INSERT INTO ERROR_LOG 
               (STAGING_ID, SOURCE_SYSTEM, ERROR_MESSAGE, RAW_DATA)
           VALUES 
               (r.merged_id, r.source_system,
                'Missing mandatory date field (CRIME_DATE or LEDS_MONTH)',
                'POSTCODE='||r.postcode||', TYPE='||r.crime_type);

           v_bad_count := v_bad_count + 1;
           CONTINUE;
        END IF;

        --  POSTCODE required IF source = PRCS or PS_WALES
        IF r.source_system IN ('PRCS','PS_WALES') 
           AND (r.postcode IS NULL OR TRIM(r.postcode) = '') THEN
        
           INSERT INTO ERROR_LOG 
               (STAGING_ID, SOURCE_SYSTEM, ERROR_MESSAGE, RAW_DATA)
           VALUES 
               (r.merged_id, r.source_system,
                'Postcode missing for PRCS/PS_WALES record',
                'POSTCODE='||r.postcode);

           v_bad_count := v_bad_count + 1;
           CONTINUE;
        END IF;

        --Crime date cannot be in future (only PRCS/PS_WALES)
        IF r.source_system IN ('PRCS','PS_WALES')
           AND r.crime_date > SYSDATE THEN

           INSERT INTO ERROR_LOG 
               (STAGING_ID, SOURCE_SYSTEM, ERROR_MESSAGE, RAW_DATA)
           VALUES 
               (r.merged_id, r.source_system,
                'Future crime date', 
                'CRIME_DATE='||r.crime_date);

           v_bad_count := v_bad_count + 1;
           CONTINUE;
        END IF;

        -- Closed date must be >= crime date
        IF r.closed_date IS NOT NULL 
           AND r.crime_date IS NOT NULL
           AND r.closed_date < r.crime_date THEN

           INSERT INTO ERROR_LOG 
               (STAGING_ID, SOURCE_SYSTEM, ERROR_MESSAGE, RAW_DATA)
           VALUES 
               (r.merged_id, r.source_system,
                'Closed date earlier than crime date',
                'CRIME_DATE='||r.crime_date||', CLOSED='||r.closed_date);

           v_bad_count := v_bad_count + 1;
           CONTINUE;
        END IF;

        -- Crime Type must exist (for PRCS / PS_WALES)
        IF r.source_system IN ('PRCS','PS_WALES')
           AND r.crime_type IS NULL THEN

           INSERT INTO ERROR_LOG 
               (STAGING_ID, SOURCE_SYSTEM, ERROR_MESSAGE, RAW_DATA)
           VALUES 
               (r.merged_id, r.source_system,
                'Crime type missing', 
                'CRIME_TYPE='||r.crime_type);

           v_bad_count := v_bad_count + 1;
           CONTINUE;
        END IF;

        -- IF ALL CHECKS PASSED THEN GOOD DATA
        v_good_count := v_good_count + 1;

    END LOOP;

    DBMS_OUTPUT.PUT_LINE('GOOD ROWS: ' || v_good_count);
    DBMS_OUTPUT.PUT_LINE('BAD ROWS : ' || v_bad_count);

END;
/
