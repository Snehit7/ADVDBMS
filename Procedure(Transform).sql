--CREATE SEQUENCE FOR ERROR_LOG
CREATE SEQUENCE ERROR_LOG_SEQ
START WITH 1
INCREMENT BY 1
NOCACHE;
/

-- CREATE SEQUENCE FOR PROCESS_LOG
CREATE SEQUENCE PROCESS_LOG_SEQ
START WITH 1
INCREMENT BY 1
NOCACHE;
/

----------------------------- 
-- PROCEDURE: VALIDATE_DATA
----------------------------
CREATE OR REPLACE PROCEDURE VALIDATE_DATA IS
    v_good_count NUMBER := 0;
    v_bad_count NUMBER := 0;
    v_error_msg VARCHAR2(4000);
BEGIN
    DBMS_OUTPUT.PUT_LINE('VALIDATION STARTED...');
    FOR r IN (SELECT * FROM STG_CRIME) LOOP
        v_error_msg := NULL;
        -- Validation rules
        IF r.crime_date IS NULL AND r.leds_month IS NULL THEN
            v_error_msg := 'Missing mandatory date field (CRIME_DATE or LEDS_MONTH)';
        ELSIF r.source_system = 'PRCS' AND (r.postcode IS NULL OR LENGTH(TRIM(r.postcode)) < 5) THEN
            v_error_msg := 'Postcode missing or too short';
        ELSIF r.source_system IN ('PRCS','PS_WALES') AND r.crime_date > SYSDATE THEN
            v_error_msg := 'Future crime date';
        ELSIF r.closed_date IS NOT NULL AND r.crime_date IS NOT NULL AND r.closed_date < r.crime_date THEN
            v_error_msg := 'Closed date earlier than crime date';
        ELSIF r.source_system IN ('PRCS','PS_WALES') AND r.crime_type IS NULL THEN
            v_error_msg := 'Crime type missing';
        ELSIF r.source_system NOT IN ('PRCS','PS_WALES','CRIME_LEEDS') THEN
            v_error_msg := 'Invalid SOURCE_SYSTEM';
        END IF;
        -- Good vs Bad
        IF v_error_msg IS NULL THEN
            v_good_count := v_good_count + 1;
        ELSE
            INSERT INTO ERROR_LOG (
                STAGING_ID,
                SOURCE_SYSTEM,
                ERROR_MESSAGE,
                ERROR_TIME,
                RAW_DATA
            ) VALUES (
                r.merged_id,
                r.source_system,
                v_error_msg,
                SYSDATE,
                'POSTCODE='||r.postcode||', TYPE='||r.crime_type
            );
            v_bad_count := v_bad_count + 1;
        END IF;
    END LOOP;
    -- Insert into PROCESS_LOG
    INSERT INTO PROCESS_LOG (
        START_TIME,
        GOOD_COUNT,
        BAD_COUNT,
        STATUS,
        REMARKS
    ) VALUES (
        SYSDATE,
        v_good_count,
        v_bad_count,
        'SUCCESS',
        'VALIDATE_DATA run completed'
    );
    DBMS_OUTPUT.PUT_LINE('VALIDATION COMPLETED.');
    DBMS_OUTPUT.PUT_LINE('GOOD RECORDS : ' || v_good_count);
    DBMS_OUTPUT.PUT_LINE('BAD RECORDS : ' || v_bad_count);
    COMMIT;
END VALIDATE_DATA;