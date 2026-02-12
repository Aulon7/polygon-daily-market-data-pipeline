USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;
USE SCHEMA RAW;

COPY INTO SEC_PRICING.RAW.RAW_EOD_PRICES
(
    TRADE_DATE,
    SYMBOL,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME,
    _SRC_FILE,
    _INGEST_TS
)
FROM (
    SELECT
        TO_DATE('{{ ti.xcom_pull(task_ids='validate_metadata')["trading_date"] }}')      AS TRADE_DATE,
        $2::STRING                         AS SYMBOL,
        TRY_TO_DECIMAL($3, 18, 6)          AS OPEN,
        TRY_TO_DECIMAL($4, 18, 6)          AS HIGH,
        TRY_TO_DECIMAL($5, 18, 6)          AS LOW,
        TRY_TO_DECIMAL($6, 18, 6)          AS CLOSE,
        TRY_TO_NUMBER($7, 38, 0)           AS VOLUME,
        METADATA$FILENAME                  AS _SRC_FILE,
        CURRENT_TIMESTAMP()                AS _INGEST_TS
    FROM '@SEC_PRICING.RAW.EXT_BRONZE/{{ ti.xcom_pull(task_ids='validate_metadata')["s3_key"] }}'
)
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = ',',
    SKIP_HEADER = 1,
    NULL_IF = ('', 'NULL', 'NaN'),
    EMPTY_FIELD_AS_NULL = TRUE
)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;
