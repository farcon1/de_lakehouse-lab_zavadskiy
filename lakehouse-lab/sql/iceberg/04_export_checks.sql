SET spark.sql.shuffle.partitions=1;

INSERT OVERWRITE DIRECTORY '/tmp/lakehouse_exports/check_01_transactions_count'
USING csv
OPTIONS (header 'true')
SELECT
  count(*) AS transactions_cnt
FROM lake.ods.transactions;

INSERT OVERWRITE DIRECTORY '/tmp/lakehouse_exports/check_02_merchants_count'
USING csv
OPTIONS (header 'true')
SELECT
  count(*) AS merchants_cnt
FROM lake.ods.merchants;

INSERT OVERWRITE DIRECTORY '/tmp/lakehouse_exports/check_03_dm_dt_summary'
USING csv
OPTIONS (header 'true')
SELECT
  dt,
  sum(gross_amount) AS gross_amount_sum,
  sum(tx_cnt) AS tx_cnt_sum
FROM lake.dm.daily_revenue_by_merchant
GROUP BY dt
ORDER BY dt DESC;
