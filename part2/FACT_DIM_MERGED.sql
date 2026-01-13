CREATE TABLE FACT_DIM_MERGED AS
SELECT 
    f.FACT_KEY,
    f.CRIME_COUNT,
    d.DATE_KEY,
    d.YEAR,
    d.MONTH,
    d.QUARTER,
    d.DAY,
    l.LOCATION_KEY,
    l.LOCATION_NAME,
    l.REGION,                   
    ct.CRIME_TYPE_KEY,
    ct.CRIME_TYPE,
    ps.PS_KEY,
    ps.PS_NAME,
    ps.PS_CITY,
    ps.PS_REGION,
    ps.PS_ADDRESS
    
FROM FACT_CRIME f
JOIN DIM_DATE          d  ON f.DATE_KEY        = d.DATE_KEY
JOIN DIM_LOCATION      l  ON f.LOCATION_KEY    = l.LOCATION_KEY
JOIN DIM_CRIME_TYPE    ct ON f.CRIME_TYPE_KEY  = ct.CRIME_TYPE_KEY
JOIN DIM_POLICE_STATION ps ON f.PS_KEY         = ps.PS_KEY;