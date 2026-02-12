USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;

SELECT COUNT(*) > 0 AS data_loaded
FROM RAW.RAW_EOD_PRICES
WHERE TRADE_DATE = TO_DATE('{{ ti.xcom_pull(task_ids='validate_metadata')["trading_date"] }}');
