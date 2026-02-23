-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Populate Reference Dimensions & dim_date
-- MAGIC Populates static reference dimensions that are not derived from source systems:
-- MAGIC - `dim_uom` — Units of measure
-- MAGIC - `dim_limit_type` — Limit type hierarchy
-- MAGIC - `dim_regulatory_context` — Regulatory submission contexts
-- MAGIC - `dim_date` — Calendar date dimension

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

USE SCHEMA l2_2_unified_model;

-- COMMAND ----------

-- DBTITLE 1,Delete reference tables
-- Delete all rows from reference tables for fresh inserts
DELETE FROM dim_uom;
DELETE FROM dim_limit_type;
DELETE FROM dim_regulatory_context;
DELETE FROM dim_stability_condition;
DELETE FROM dim_timepoint;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_uom — Units of Measure

-- COMMAND ----------

-- DBTITLE 1,Cell 5
MERGE INTO dim_uom AS tgt
USING (
    SELECT
        col1 AS uom_code,
        col2 AS uom_name,
        col3 AS uom_category,
        col4 AS si_conversion_factor,
        col5 AS si_base_unit
    FROM VALUES
        ('mg',       'Milligrams',                  'MASS',          CAST(0.001 AS DECIMAL(18,10)),          'kg'),
        ('g',        'Grams',                       'MASS',          CAST(1.0 AS DECIMAL(18,10)),            'kg'),
        ('mcg',      'Micrograms',                  'MASS',          CAST(0.000001 AS DECIMAL(18,10)),       'kg'),
        ('%',        'Percent',                     'RATIO',         CAST(0.01 AS DECIMAL(18,10)),           CAST(NULL AS STRING)),
        ('% w/w',    'Percent weight/weight',       'RATIO',         CAST(0.01 AS DECIMAL(18,10)),           CAST(NULL AS STRING)),
        ('% w/v',    'Percent weight/volume',       'RATIO',         CAST(0.01 AS DECIMAL(18,10)),           CAST(NULL AS STRING)),
        ('% area',   'Percent area (HPLC)',         'RATIO',         CAST(0.01 AS DECIMAL(18,10)),           CAST(NULL AS STRING)),
        ('ppm',      'Parts per million',           'CONCENTRATION', CAST(0.000001 AS DECIMAL(18,10)),       CAST(NULL AS STRING)),
        ('ppb',      'Parts per billion',           'CONCENTRATION', CAST(0.000000001 AS DECIMAL(18,10)),    CAST(NULL AS STRING)),
        ('mg/mL',    'Milligrams per milliliter',   'CONCENTRATION', CAST(1.0 AS DECIMAL(18,10)),            'kg/m3'),
        ('mg/g',     'Milligrams per gram',         'CONCENTRATION', CAST(0.001 AS DECIMAL(18,10)),          CAST(NULL AS STRING)),
        ('mcg/mL',   'Micrograms per milliliter',   'CONCENTRATION', CAST(0.001 AS DECIMAL(18,10)),          'kg/m3'),
        ('IU',       'International Units',         'COUNT',         CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING)),
        ('IU/mg',    'International Units per mg',  'CONCENTRATION', CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING)),
        ('CFU/g',    'Colony forming units per g',  'COUNT',         CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING)),
        ('CFU/mL',   'Colony forming units per mL', 'COUNT',         CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING)),
        ('EU/mg',    'Endotoxin units per mg',      'COUNT',         CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING)),
        ('mL',       'Milliliters',                 'VOLUME',        CAST(0.000001 AS DECIMAL(18,10)),       'm3'),
        ('L',        'Liters',                      'VOLUME',        CAST(0.001 AS DECIMAL(18,10)),          'm3'),
        ('mm',       'Millimeters',                 'LENGTH',        CAST(0.001 AS DECIMAL(18,10)),          'm'),
        ('min',      'Minutes',                     'OTHER',         CAST(60.0 AS DECIMAL(18,10)),           's'),
        ('pH',       'pH units',                    'OTHER',         CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING)),
        ('N/A',      'Not applicable',              'OTHER',         CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING)),
        ('kp',       'Kilopond (hardness)',         'OTHER',         CAST(9.80665 AS DECIMAL(18,10)),        'N'),
        ('mg/tab',   'Milligrams per tablet',       'MASS',          CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING)),
        ('% (Q)',    'Percent dissolved (Q value)',  'RATIO',         CAST(0.01 AS DECIMAL(18,10)),           CAST(NULL AS STRING))
) src
ON tgt.uom_code = src.uom_code
WHEN MATCHED THEN UPDATE SET
  tgt.uom_name = src.uom_name,
  tgt.uom_category = src.uom_category,
  tgt.si_conversion_factor = src.si_conversion_factor,
  tgt.si_base_unit = src.si_base_unit,
  tgt.is_active = TRUE
WHEN NOT MATCHED THEN INSERT (uom_code, uom_name, uom_category, si_conversion_factor, si_base_unit, is_active)
VALUES (src.uom_code, src.uom_name, src.uom_category, src.si_conversion_factor, src.si_base_unit, TRUE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_limit_type — Limit Type Hierarchy

-- COMMAND ----------

-- DBTITLE 1,Cell 7
MERGE INTO dim_limit_type AS tgt
USING (
    VALUES
        ('AC',        'Acceptance Criteria',    'Regulatory acceptance limits for release and stability testing',   3, 'REGULATORY', TRUE, 1, CURRENT_TIMESTAMP()),
        ('NOR',       'Normal Operating Range', 'Tighter internal operating range derived from process capability', 1, 'OPERATIONAL', FALSE, 2, CURRENT_TIMESTAMP()),
        ('PAR',       'Proven Acceptable Range','Wider range demonstrated by development and validation data',      2, 'DESIGN_SPACE', FALSE, 3, CURRENT_TIMESTAMP()),
        ('ALERT',     'Alert Limit',            'Statistical alert limit — triggers investigation if breached',     NULL, 'PROCESS_CTRL', FALSE, 4, CURRENT_TIMESTAMP()),
        ('ACTION',    'Action Limit',           'Statistical action limit — triggers corrective action',            NULL, 'PROCESS_CTRL', FALSE, 5, CURRENT_TIMESTAMP()),
        ('IPC_LIMIT', 'In-Process Control',     'In-process control limit used during manufacturing',               NULL, 'PROCESS_CTRL', FALSE, 6, CURRENT_TIMESTAMP()),
        ('REPORT',    'Report Only',            'Informational limit — no pass/fail decision',                      NULL, 'INFORMATIONAL', FALSE, 7, CURRENT_TIMESTAMP())
) src
ON tgt.limit_type_code = src.col1
WHEN MATCHED THEN UPDATE SET
  tgt.limit_type_name = src.col2,
  tgt.limit_type_description = src.col3,
  tgt.hierarchy_level = src.col4,
  tgt.limit_category = src.col5,
  tgt.is_regulatory = src.col6,
  tgt.sort_order = src.col7,
  tgt.load_timestamp = src.col8
WHEN NOT MATCHED THEN INSERT (limit_type_code, limit_type_name, limit_type_description, hierarchy_level, limit_category, is_regulatory, sort_order, load_timestamp)
VALUES (src.col1, src.col2, src.col3, src.col4, src.col5, src.col6, src.col7, src.col8);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_regulatory_context — Regulatory Submission Contexts

-- COMMAND ----------

-- DBTITLE 1,Cell 9
MERGE INTO dim_regulatory_context AS tgt
USING (
    VALUES
        ('US-NDA-3.2.P.5.1',  'US',  'United States',   'FDA',  'NDA',  NULL, NULL, '3.2.P.5.1', 'Specifications — Drug Product',  CURRENT_TIMESTAMP()),
        ('US-NDA-3.2.S.4.1',  'US',  'United States',   'FDA',  'NDA',  NULL, NULL, '3.2.S.4.1', 'Specifications — Drug Substance', CURRENT_TIMESTAMP()),
        ('US-ANDA-3.2.P.5.1', 'US',  'United States',   'FDA',  'ANDA', NULL, NULL, '3.2.P.5.1', 'Specifications — Drug Product',  CURRENT_TIMESTAMP()),
        ('US-ANDA-3.2.S.4.1', 'US',  'United States',   'FDA',  'ANDA', NULL, NULL, '3.2.S.4.1', 'Specifications — Drug Substance', CURRENT_TIMESTAMP()),
        ('US-BLA-3.2.P.5.1',  'US',  'United States',   'FDA',  'BLA',  NULL, NULL, '3.2.P.5.1', 'Specifications — Drug Product',  CURRENT_TIMESTAMP()),
        ('US-IND-3.2.P.5.1',  'US',  'United States',   'FDA',  'IND',  NULL, NULL, '3.2.P.5.1', 'Specifications — Drug Product',  CURRENT_TIMESTAMP()),
        ('EU-MAA-3.2.P.5.1',  'EU',  'European Union',  'EMA',  'MAA',  NULL, NULL, '3.2.P.5.1', 'Specifications — Drug Product',  CURRENT_TIMESTAMP()),
        ('EU-MAA-3.2.S.4.1',  'EU',  'European Union',  'EMA',  'MAA',  NULL, NULL, '3.2.S.4.1', 'Specifications — Drug Substance', CURRENT_TIMESTAMP()),
        ('JP-JNDA-3.2.P.5.1', 'JP',  'Japan',           'PMDA', 'JNDA', NULL, NULL, '3.2.P.5.1', 'Specifications — Drug Product',  CURRENT_TIMESTAMP()),
        ('JP-JNDA-3.2.S.4.1', 'JP',  'Japan',           'PMDA', 'JNDA', NULL, NULL, '3.2.S.4.1', 'Specifications — Drug Substance', CURRENT_TIMESTAMP()),
        ('CN-NDA-3.2.P.5.1',  'CN',  'China',           'NMPA', 'NDA',  NULL, NULL, '3.2.P.5.1', 'Specifications — Drug Product',  CURRENT_TIMESTAMP()),
        ('CN-NDA-3.2.S.4.1',  'CN',  'China',           'NMPA', 'NDA',  NULL, NULL, '3.2.S.4.1', 'Specifications — Drug Substance', CURRENT_TIMESTAMP()),
        ('ROW-NDA-3.2.P.5.1', 'ROW', 'Rest of World',   NULL,   'NDA',  NULL, NULL, '3.2.P.5.1', 'Specifications — Drug Product',  CURRENT_TIMESTAMP()),
        ('ROW-NDA-3.2.S.4.1', 'ROW', 'Rest of World',   NULL,   'NDA',  NULL, NULL, '3.2.S.4.1', 'Specifications — Drug Substance', CURRENT_TIMESTAMP())
) src
ON tgt.regulatory_context_code = src.col1
WHEN MATCHED THEN UPDATE SET
  tgt.region_code = src.col2,
  tgt.region_name = src.col3,
  tgt.regulatory_body = src.col4,
  tgt.submission_type = src.col5,
  tgt.guideline_code = src.col6,
  tgt.guideline_name = src.col7,
  tgt.ctd_module = src.col8,
  tgt.ctd_section = src.col9,
  tgt.load_timestamp = src.col10
WHEN NOT MATCHED THEN INSERT (regulatory_context_code, region_code, region_name, regulatory_body, submission_type, guideline_code, guideline_name, ctd_module, ctd_section, load_timestamp)
VALUES (src.col1, src.col2, src.col3, src.col4, src.col5, src.col6, src.col7, src.col8, src.col9, src.col10);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_stability_condition — ICH Storage Conditions

-- COMMAND ----------

-- DBTITLE 1,Cell 11
MERGE INTO dim_stability_condition AS tgt
USING (
    VALUES
        (1, '25C60RH',  '25°C / 60% RH',  CAST(25.0 AS DECIMAL(5,1)), CAST(60.0 AS DECIMAL(5,1)), 'LONG_TERM',     TRUE),
        (2, '30C65RH',  '30°C / 65% RH',  CAST(30.0 AS DECIMAL(5,1)), CAST(65.0 AS DECIMAL(5,1)), 'INTERMEDIATE',  TRUE),
        (3, '40C75RH',  '40°C / 75% RH',  CAST(40.0 AS DECIMAL(5,1)), CAST(75.0 AS DECIMAL(5,1)), 'ACCELERATED',   TRUE),
        (4, '5C',       '5°C ± 3°C',      CAST(5.0 AS DECIMAL(5,1)),  NULL, 'REFRIGERATED',  TRUE),
        (5, 'REFRIG',   '2-8°C',          CAST(5.0 AS DECIMAL(5,1)),  NULL, 'REFRIGERATED',  TRUE),
        (6, 'FREEZER',  '-20°C ± 5°C',    CAST(-20.0 AS DECIMAL(5,1)),NULL, 'FROZEN',        TRUE)
) src
ON tgt.condition_code = src.col2
WHEN MATCHED THEN UPDATE SET
  tgt.condition_name = src.col3,
  tgt.temperature_celsius = src.col4,
  tgt.humidity_pct = src.col5,
  tgt.ich_condition_type = src.col6,
  tgt.is_active = src.col7
WHEN NOT MATCHED THEN INSERT (condition_key, condition_code, condition_name, temperature_celsius, humidity_pct, ich_condition_type, is_active)
VALUES (src.col1, src.col2, src.col3, src.col4, src.col5, src.col6, src.col7);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_timepoint — Stability Time Points

-- COMMAND ----------

-- DBTITLE 1,Cell 13
MERGE INTO dim_timepoint AS tgt
USING (
    VALUES
        (1, 'T0',   0,  'Initial',    1,  TRUE),
        (2, 'T1M',  1,  '1 Month',    2,  TRUE),
        (3, 'T3M',  3,  '3 Months',   3,  TRUE),
        (4, 'T6M',  6,  '6 Months',   4,  TRUE),
        (5, 'T9M',  9,  '9 Months',   5,  TRUE),
        (6, 'T12M', 12, '12 Months',  6,  TRUE),
        (7, 'T18M', 18, '18 Months',  7,  TRUE),
        (8, 'T24M', 24, '24 Months',  8,  TRUE),
        (9, 'T36M', 36, '36 Months',  9,  TRUE)
) src
ON tgt.timepoint_code = src.col2
WHEN MATCHED THEN UPDATE SET
  tgt.timepoint_months = src.col3,
  tgt.timepoint_name = src.col4,
  tgt.display_order = src.col5,
  tgt.is_active = src.col6
WHEN NOT MATCHED THEN INSERT (timepoint_key, timepoint_code, timepoint_months, timepoint_name, display_order, is_active)
VALUES (src.col1, src.col2, src.col3, src.col4, src.col5, src.col6);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_date — Calendar Date Dimension
-- MAGIC Populates dates from 2020-01-01 to 2035-12-31.

-- COMMAND ----------

-- DBTITLE 1,Cell 16
MERGE INTO dim_date AS tgt
USING (
    SELECT
        CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT)         AS date_key,
        d                                               AS full_date,
        YEAR(d)                                         AS year,
        QUARTER(d)                                      AS quarter,
        MONTH(d)                                        AS month,
        DATE_FORMAT(d, 'MMMM')                          AS month_name,
        DATE_FORMAT(d, 'MMM')                           AS month_name_short,
        WEEKOFYEAR(d)                                   AS week_of_year,
        DAYOFYEAR(d)                                    AS day_of_year,
        DAY(d)                                          AS day_of_month,
        DAYOFWEEK(d)                                    AS day_of_week,
        DATE_FORMAT(d, 'EEEE')                          AS day_name,
        SUBSTRING(DATE_FORMAT(d, 'EEE'), 1, 3)          AS day_name_short,
        CONCAT(YEAR(d), '-', LPAD(MONTH(d), 2, '0'))    AS year_month,
        CONCAT(YEAR(d), '-Q', QUARTER(d))               AS year_quarter,
        CASE WHEN MONTH(d) >= 7 THEN YEAR(d) + 1 ELSE YEAR(d) END AS fiscal_year,
        CASE WHEN MONTH(d) >= 7 THEN QUARTER(ADD_MONTHS(d, -6)) ELSE QUARTER(ADD_MONTHS(d, 6)) END AS fiscal_quarter,
        NULL                                            AS fiscal_month,
        CASE WHEN DAYOFWEEK(d) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN DAY(d) = 1 THEN TRUE ELSE FALSE END   AS is_month_start,
        CASE WHEN d = LAST_DAY(d) THEN TRUE ELSE FALSE END AS is_month_end,
        CASE WHEN DAYOFYEAR(d) = 1 THEN TRUE ELSE FALSE END AS is_year_start,
        CASE WHEN DAYOFYEAR(d) = 365 OR DAYOFYEAR(d) = 366 THEN TRUE ELSE FALSE END AS is_year_end,
        FALSE                                           AS is_quarter_start,
        FALSE                                           AS is_quarter_end,
        FALSE                                           AS is_holiday,
        NULL                                            AS holiday_name
    FROM (
        SELECT EXPLODE(SEQUENCE(DATE'2020-01-01', DATE'2035-12-31', INTERVAL 1 DAY)) AS d
    )
) AS src
ON tgt.date_key = src.date_key
WHEN NOT MATCHED THEN INSERT (
    date_key, full_date, year, quarter, month, month_name, month_name_short, week_of_year, day_of_year, day_of_month, day_of_week, day_name, day_name_short, year_month, year_quarter, fiscal_year, fiscal_quarter, fiscal_month, is_weekend, is_month_start, is_month_end, is_quarter_start, is_quarter_end, is_year_start, is_year_end, is_holiday, holiday_name
)
VALUES (
    src.date_key, src.full_date, src.year, src.quarter, src.month, src.month_name, src.month_name_short, src.week_of_year, src.day_of_year, src.day_of_month, src.day_of_week, src.day_name, src.day_name_short, src.year_month, src.year_quarter, src.fiscal_year, src.fiscal_quarter, src.fiscal_month, src.is_weekend, src.is_month_start, src.is_month_end, src.is_quarter_start, src.is_quarter_end, src.is_year_start, src.is_year_end, src.is_holiday, src.holiday_name
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify Reference Data

-- COMMAND ----------

SELECT 'dim_uom' AS table_name, COUNT(*) AS row_count FROM dim_uom
UNION ALL
SELECT 'dim_limit_type', COUNT(*) FROM dim_limit_type
UNION ALL
SELECT 'dim_regulatory_context', COUNT(*) FROM dim_regulatory_context
UNION ALL
SELECT 'dim_stability_condition', COUNT(*) FROM dim_stability_condition
UNION ALL
SELECT 'dim_timepoint', COUNT(*) FROM dim_timepoint
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date;
