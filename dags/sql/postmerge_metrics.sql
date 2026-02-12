USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;

SELECT
  (SELECT COUNT(*)
        FROM CORE.EOD_PRICES
        WHERE TRADE_DATE = TO_DATE('{{ ti.xcom_pull(task_ids='validate_metadata')["trading_date"] }}')) AS core_rows,
  (SELECT COUNT(*)
     FROM DM_FACT.FACT_DAILY_PRICE
        WHERE TRADE_DATE = TO_DATE('{{ ti.xcom_pull(task_ids='validate_metadata')["trading_date"] }}')) AS fact_rows;
