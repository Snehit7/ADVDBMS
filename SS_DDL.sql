---------------------------
--ORACLE STAR SCHEMA 
---------------------------
--------------------------------------------------------------
-- SEQUENCES
--------------------------------------------------------------

CREATE SEQUENCE SEQ_DIM_DATE START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE SEQ_DIM_LOCATION START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE SEQ_DIM_CRIME_TYPE START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE SEQ_DIM_POLICE_STATION START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE SEQ_FACT_CRIME START WITH 1 INCREMENT BY 1;

--------------------------------------------------------------
-- TRIGGERS
--------------------------------------------------------------

-- DIM_DATE Trigger
CREATE OR REPLACE TRIGGER TRG_DIM_DATE_PK
BEFORE INSERT ON DIM_DATE
FOR EACH ROW
BEGIN
    IF :NEW.DATE_KEY IS NULL THEN
        SELECT SEQ_DIM_DATE.NEXTVAL INTO :NEW.DATE_KEY FROM dual;
    END IF;
END;
/

-- DIM_LOCATION Trigger
CREATE OR REPLACE TRIGGER TRG_DIM_LOCATION_PK
BEFORE INSERT ON DIM_LOCATION
FOR EACH ROW
BEGIN
    IF :NEW.LOCATION_KEY IS NULL THEN
        SELECT SEQ_DIM_LOCATION.NEXTVAL INTO :NEW.LOCATION_KEY FROM dual;
    END IF;
END;
/

-- DIM_CRIME_TYPE Trigger
CREATE OR REPLACE TRIGGER TRG_DIM_CRIME_TYPE_PK
BEFORE INSERT ON DIM_CRIME_TYPE
FOR EACH ROW
BEGIN
    IF :NEW.CRIME_TYPE_KEY IS NULL THEN
        SELECT SEQ_DIM_CRIME_TYPE.NEXTVAL INTO :NEW.CRIME_TYPE_KEY FROM dual;
    END IF;
END;
/

-- DIM_POLICE_STATION Trigger
CREATE OR REPLACE TRIGGER TRG_DIM_POLICE_STATION_PK
BEFORE INSERT ON DIM_POLICE_STATION
FOR EACH ROW
BEGIN
    IF :NEW.PS_KEY IS NULL THEN
        SELECT SEQ_DIM_POLICE_STATION.NEXTVAL INTO :NEW.PS_KEY FROM dual;
    END IF;
END;
/

-- FACT_CRIME Trigger
CREATE OR REPLACE TRIGGER TRG_FACT_CRIME_PK
BEFORE INSERT ON FACT_CRIME
FOR EACH ROW
BEGIN
    IF :NEW.FACT_CRIME_KEY IS NULL THEN
        SELECT SEQ_FACT_CRIME.NEXTVAL INTO :NEW.FACT_CRIME_KEY FROM dual;
    END IF;
END;
/

----------------
-- DROP TABLES 
----------------

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE FACT_CRIME CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE DIM_DATE CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE DIM_LOCATION CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE DIM_CRIME_TYPE CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE DIM_POLICE_STATION CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

--------------------------------------------------------------
-- DIMENSION TABLES
--------------------------------------------------------------

----------------------------
-- DIM_DATE
----------------------------
CREATE TABLE DIM_DATE (
    DATE_KEY       INTEGER       NOT NULL,
    DAY            INTEGER,
    MONTH          INTEGER,
    QUARTER        INTEGER,
    YEAR           INTEGER,
    CONSTRAINT pk_dim_date PRIMARY KEY (DATE_KEY)
);


----------------------------
-- DIM_LOCATION
----------------------------
CREATE TABLE DIM_LOCATION (
    LOCATION_KEY   INTEGER        NOT NULL,
    LOCATION_NAME  VARCHAR2(100)  NOT NULL,
    DISTRICT       VARCHAR2(100),
    CITY           VARCHAR2(100),
    REGION         VARCHAR2(100),
    CONSTRAINT pk_dim_location PRIMARY KEY (LOCATION_KEY)
);


----------------------------
-- DIM_CRIME_TYPE
----------------------------
CREATE TABLE DIM_CRIME_TYPE (
    CRIME_TYPE_KEY INTEGER        NOT NULL,
    CRIME_TYPE     VARCHAR2(100),
    CATEGORY       VARCHAR2(100),
    CONSTRAINT pk_dim_crime_type PRIMARY KEY (CRIME_TYPE_KEY)
);


----------------------------
-- DIM_POLICE_STATION
----------------------------
CREATE TABLE DIM_POLICE_STATION (
    PS_KEY     INTEGER        NOT NULL,
    PS_NAME    VARCHAR2(100)  NOT NULL,
    PS_ADDRESS VARCHAR2(200),
    PS_CITY    VARCHAR2(100),
    PS_REGION  VARCHAR2(100),
    CONSTRAINT pk_dim_police_station PRIMARY KEY (PS_KEY)
);


--------------------------------------------------------------
-- FACT TABLE
--------------------------------------------------------------

CREATE TABLE FACT_CRIME (
    FACT_CRIME_KEY  INTEGER       NOT NULL,
    DATE_KEY        INTEGER       NOT NULL,
    LOCATION_KEY    INTEGER       NOT NULL,
    CRIME_TYPE_KEY  INTEGER       NOT NULL,
    PS_KEY          INTEGER       NOT NULL,
    CRIME_COUNT     INTEGER,

    CONSTRAINT pk_fact_crime PRIMARY KEY (FACT_CRIME_KEY),

    -- Foreign keys
    CONSTRAINT fk_fact_date 
        FOREIGN KEY (DATE_KEY)
        REFERENCES DIM_DATE(DATE_KEY),

    CONSTRAINT fk_fact_location 
        FOREIGN KEY (LOCATION_KEY)
        REFERENCES DIM_LOCATION(LOCATION_KEY),

    CONSTRAINT fk_fact_crime_type 
        FOREIGN KEY (CRIME_TYPE_KEY)
        REFERENCES DIM_CRIME_TYPE(CRIME_TYPE_KEY),

    CONSTRAINT fk_fact_ps 
        FOREIGN KEY (PS_KEY)
        REFERENCES DIM_POLICE_STATION(PS_KEY)
);

--------------------------------------------------------------
-- END OF STAR SCHEMA
--------------------------------------------------------------
