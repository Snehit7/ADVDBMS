----------------------------
-- DATA INTEGRATION SCRIPT 
-----------------------------

SET SERVEROUTPUT ON;

--------------------------------------------------------------
-- Drop table if already exists
--------------------------------------------------------------
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE STG_CRIME CASCADE CONSTRAINTS';
EXCEPTION 
    WHEN OTHERS THEN 
        IF SQLCODE != -942 THEN RAISE;
        END IF;
END;
/


--------------------------------------------------------------
-- Create STG_CRIME unified table
--------------------------------------------------------------
CREATE TABLE STG_CRIME (
    MERGED_ID        NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    SOURCE_SYSTEM    VARCHAR2(20),
    CRIME_DATE       DATE,
    POSTCODE         VARCHAR2(50),
    CRIME_STATUS     VARCHAR2(30),
    CRIME_TYPE       VARCHAR2(200),
    CLOSED_DATE      DATE,
    OFFICER_ID       NUMBER,
    STATION_ID       NUMBER,
    -- From CRIME_LEEDS:
    LEDS_MONTH       VARCHAR2(50),
    LEDS_FORCE       VARCHAR2(50),
    LEDS_NEIGHBOURHOOD VARCHAR2(50),
    LEDS_TOTAL_CRIME NUMBER
);

--------------------------------------------------------------
--  Merge PRCS (PL_REPORTED_CRIME)
--------------------------------------------------------------

DECLARE
    CURSOR c_prcs IS
        SELECT 
            date_reported,
            crime_postcode,
            crime_status,
            fk1_crime_type_id,
            date_closed,
            fk2_station_id
        FROM pl_reported_crime;
BEGIN
    FOR r IN c_prcs LOOP
        INSERT INTO STG_CRIME
        (SOURCE_SYSTEM, CRIME_DATE, POSTCODE, CRIME_STATUS, CRIME_TYPE, 
         CLOSED_DATE, STATION_ID)
        VALUES 
        ('PRCS', r.date_reported, r.crime_postcode, r.crime_status,
         TO_CHAR(r.fk1_crime_type_id), r.date_closed, r.fk2_station_id);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('PRCS merged into STG_CRIME.');
END;
/


---------------------
-- Merge PS_WALES 
----------------------

DECLARE
    v_count NUMBER := 0;
BEGIN
    BEGIN
        SELECT COUNT(*) INTO v_count FROM crime_register;
    EXCEPTION 
        WHEN OTHERS THEN v_count := 0;
    END;

    IF v_count > 0 THEN
        INSERT INTO STG_CRIME
        (SOURCE_SYSTEM, CRIME_DATE, CRIME_TYPE, CRIME_STATUS, STATION_ID)
        SELECT
            'PS_WALES',
            reported_date,
            crime_type,
            crime_status,
            police_id
        FROM crime_register;

        DBMS_OUTPUT.PUT_LINE('PS_WALES merged: ' || v_count);
    ELSE
        DBMS_OUTPUT.PUT_LINE('PS_WALES has no data — skipping.');
    END IF;
END;
/


--------------------------------------------
-- Merge Crime_Leeds (aggregate dataset)
--------------------------------------------

DECLARE
    CURSOR c_leds IS
        SELECT 
            month,
            force,
            neighbourhood,
            all_crime
        FROM crime_leeds;
BEGIN
    FOR r IN c_leds LOOP    
        INSERT INTO STG_CRIME
        (SOURCE_SYSTEM, LEDS_MONTH, LEDS_FORCE, LEDS_NEIGHBOURHOOD, LEDS_TOTAL_CRIME)
        VALUES
        ('CRIME_LEEDS', r.month, r.force, r.neighbourhood, r.all_crime);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('CRIME_LEEDS merged into STG_CRIME.');
END;
/


--------------------------------------------------------------
--  Summary Row Count
--------------------------------------------------------------
DECLARE 
    v_total NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_total FROM STG_CRIME;
    DBMS_OUTPUT.PUT_LINE('-------------------------------------');
    DBMS_OUTPUT.PUT_LINE('TOTAL ROWS IN STG_CRIME = ' || v_total);
    DBMS_OUTPUT.PUT_LINE('-------------------------------------');
END;
/


COMMIT;

