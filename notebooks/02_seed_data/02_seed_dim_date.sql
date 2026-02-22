-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Seed dim_date — Calendar Date Dimension
-- MAGIC Populates `dim_date` with dates from 2020-01-01 to 2035-12-31.
-- MAGIC Uses `sequence` + `explode` for pure SQL generation.

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

USE SCHEMA l2_2_spec_unified;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Generate and insert date rows

-- COMMAND ----------

MERGE INTO dim_date AS tgt
USING (
    SELECT
        CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT)         AS date_key,
        d                                                AS full_date,
        YEAR(d)                                          AS year,
        QUARTER(d)                                       AS quarter,
        MONTH(d)                                         AS month,
        DATE_FORMAT(d, 'MMMM')                           AS month_name,
        DAY(d)                                           AS day_of_month,
        DAYOFWEEK(d)                                     AS day_of_week,
        DATE_FORMAT(d, 'EEEE')                           AS day_name,
        WEEKOFYEAR(d)                                    AS week_of_year,
        CASE WHEN DAYOFWEEK(d) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        -- Fiscal year: assume July start (configurable)
        CASE WHEN MONTH(d) >= 7 THEN YEAR(d) + 1 ELSE YEAR(d) END AS fiscal_year,
        CASE WHEN MONTH(d) >= 7 THEN QUARTER(ADD_MONTHS(d, -6)) ELSE QUARTER(ADD_MONTHS(d, 6)) END AS fiscal_quarter
    FROM (
        SELECT EXPLODE(SEQUENCE(DATE'2020-01-01', DATE'2035-12-31', INTERVAL 1 DAY)) AS d
    )
) AS src
ON tgt.date_key = src.date_key
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify

-- COMMAND ----------

SELECT MIN(full_date) AS min_date, MAX(full_date) AS max_date, COUNT(*) AS total_rows
FROM dim_date;
