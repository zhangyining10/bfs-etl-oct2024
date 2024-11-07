MERGE INTO FACT_STOCK_HISTORY_TEAM6 AS target
USING
(
    SELECT SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE
    FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY SYMBOL, DATE ORDER BY DATE) AS row_num
        FROM US_STOCK_DAILY.DCCM.STOCK_HISTORY
    ) AS deduped_source
    WHERE row_num = 1
) AS source
ON target.SYMBOL = source.SYMBOL
AND target.TRADEDATE = source.DATE
WHEN MATCHED THEN
    UPDATE SET
        target.OPEN = source.OPEN,
        target.HIGH = source.HIGH,
        target.LOW = source.LOW,
        target.CLOSE = source.CLOSE,
        target.VOLUME = source.VOLUME,
        target.ADJCLOSE = source.ADJCLOSE
WHEN NOT MATCHED THEN
    INSERT (SYMBOL, TRADEDATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJCLOSE)
    VALUES (source.SYMBOL, source.DATE, source.OPEN, source.HIGH, source.LOW, source.CLOSE, source.VOLUME, source.ADJCLOSE);
SELECT * FROM DIM_COMPANY_PROFILE_TEAM6
SELECT * FROM US_STOCK_DAILY.DCCM.COMPANY_PROFILE limit 100
MERGE INTO DIM_COMPANY_PROFILE_TEAM6 AS target
USING US_STOCK_DAILY.DCCM.COMPANY_PROFILE AS source
    ON target.ID = source.ID
    WHEN MATCHED THEN
        UPDATE SET
            target.SYMBOL = source.SYMBOL,
            target.PRICE = source.PRICE,
            target.BETA = source.BETA,
            target.VOLAVG = source.VOLAVG,
            target.INDUSTRY = source.INDUSTRY,
            target.SECTOR = source.SECTOR,
            target.WEBSITE = source.WEBSITE,
            target.NAME = source.COMPANYNAME
    WHEN NOT MATCHED THEN
        INSERT (ID, SYMBOL, PRICE, BETA, VOLAVG, INDUSTRY, SECTOR,WEBSITE, NAME)
        VALUES (source.ID, source.SYMBOL, source.PRICE, source.BETA, source.VOLAVG, source.INDUSTRY, source.SECTOR, source.WEBSITE, source.COMPANYNAME);
MERGE INTO DIM_SYMBOLS_TEAM6 AS target
USING US_STOCK_DAILY.DCCM.SYMBOLS AS source
    ON target.SYMBOL = source.SYMBOL
    WHEN MATCHED THEN
        UPDATE SET
            target.NAME = source.NAME,
            target.EXCHANGE = source.EXCHANGE
    WHEN NOT MATCHED THEN
        INSERT (SYMBOL,NAME,EXCHANGE)
        VALUES (source.SYMBOL, source.NAME, source.EXCHANGE);