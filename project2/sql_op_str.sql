-- create a table based on the structure of source table but make it empty
CREATE OR REPLACE TABLE FACT_STOCK_HISTORY_TEAM6_TEST AS
    SELECT *
    FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY
    WHERE 1=2;

-- check whether there are any data in the table
SELECT * FROM FACT_STOCK_HISTORY_TEAM6_TEST;

-- insert data that are not in our table, in the case, it could be all
INSERT INTO FACT_STOCK_HISTORY_TEAM6_TEST
    SELECT *
    FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY
    EXCEPT
    SELECT *
    FROM FACT_STOCK_HISTORY_TEAM6_TEST;

-- check where table are loaded successfully
SELECT * FROM FACT_STOCK_HISTORY_TEAM6_TEST;









