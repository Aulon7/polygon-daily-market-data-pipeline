USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;

WITH td AS (
  SELECT TO_DATE('{{ ti.xcom_pull(task_ids=params.trading_ds_task_id, key="trading_date") }}') AS d
),
raw_cnt AS (
  SELECT COUNT(*) AS c
  FROM RAW.RAW_EOD_PRICES
  WHERE TRADE_DATE = (SELECT d FROM td)
),
today_keys AS (
  SELECT DISTINCT UPPER(TRIM(SYMBOL)) AS SYMBOL, TRADE_DATE
  FROM RAW.RAW_EOD_PRICES
  WHERE TRADE_DATE = (SELECT d FROM td)
),
key_cnt AS (
  SELECT COUNT(*) AS c FROM today_keys
),
core_existing AS (
  SELECT COUNT(*) AS c
  FROM today_keys k
  JOIN CORE.EOD_PRICES t
    ON UPPER(TRIM(t.SYMBOL)) = k.SYMBOL AND t.TRADE_DATE = k.TRADE_DATE
)
SELECT
  r.c         AS raw_cnt,
  e.c         AS core_existing_cnt,
  (k.c - e.c) AS core_inserts_est,
  e.c         AS core_updates_est
FROM raw_cnt r
CROSS JOIN key_cnt k
CROSS JOIN core_existing e;
