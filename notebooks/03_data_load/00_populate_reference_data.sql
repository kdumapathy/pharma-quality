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
CREATE OR REPLACE TEMP VIEW tmp_dim_uom_seed AS
SELECT
  ROW_NUMBER() OVER (ORDER BY uom_code) AS uom_key,
  uom_code,
  uom_name,
  uom_category,
  si_conversion_factor,
  si_base_unit
FROM (
  SELECT *
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
      ('% (Q)',    'Percent dissolved (Q value)', 'RATIO',         CAST(0.01 AS DECIMAL(18,10)),           CAST(NULL AS STRING))
  AS seed(uom_code, uom_name, uom_category, si_conversion_factor, si_base_unit)
);

MERGE INTO dim_uom AS tgt
USING tmp_dim_uom_seed src
ON tgt.uom_code = src.uom_code
WHEN MATCHED THEN UPDATE SET
  tgt.uom_name = src.uom_name,
  tgt.uom_category = src.uom_category,
  tgt.si_conversion_factor = src.si_conversion_factor,
  tgt.si_base_unit = src.si_base_unit,
  tgt.is_active = TRUE
WHEN NOT MATCHED THEN INSERT (uom_key, uom_code, uom_name, uom_category, si_conversion_factor, si_base_unit, is_active)
VALUES (src.uom_key, src.uom_code, src.uom_name, src.uom_category, src.si_conversion_factor, src.si_base_unit, TRUE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_limit_type — Limit Type Hierarchy

-- COMMAND ----------

-- DBTITLE 1,Cell 7
CREATE OR REPLACE TEMP VIEW tmp_dim_limit_type_seed AS
SELECT
  ROW_NUMBER() OVER (ORDER BY limit_type_code) AS limit_type_key,
  limit_type_code,
  limit_type_name,
  limit_type_description,
  hierarchy_level,
  is_regulatory
FROM (
  SELECT *
  FROM VALUES
      ('AC',        'Acceptance Criteria',    'Regulatory acceptance limits for release and stability testing',   3, TRUE),
      ('NOR',       'Normal Operating Range', 'Tighter internal operating range derived from process capability', 1, FALSE),
      ('PAR',       'Proven Acceptable Range','Wider range demonstrated by development and validation data',      2, FALSE),
      ('ALERT',     'Alert Limit',            'Statistical alert limit — triggers investigation if breached',     NULL, FALSE),
      ('ACTION',    'Action Limit',           'Statistical action limit — triggers corrective action',            NULL, FALSE),
      ('IPC_LIMIT', 'In-Process Control',     'In-process control limit used during manufacturing',               NULL, FALSE),
      ('REPORT',    'Report Only',            'Informational limit — no pass/fail decision',                      NULL, FALSE)
  AS seed(limit_type_code, limit_type_name, limit_type_description, hierarchy_level, is_regulatory)
);

MERGE INTO dim_limit_type AS tgt
USING tmp_dim_limit_type_seed src
ON tgt.limit_type_code = src.limit_type_code
WHEN MATCHED THEN UPDATE SET
  tgt.limit_type_name = src.limit_type_name,
  tgt.limit_type_description = src.limit_type_description,
  tgt.hierarchy_rank = src.hierarchy_level,
  tgt.is_regulatory = src.is_regulatory,
  tgt.is_active = TRUE
WHEN NOT MATCHED THEN INSERT (limit_type_key, limit_type_code, limit_type_name, limit_type_description, hierarchy_rank, is_regulatory, is_active)
VALUES (src.limit_type_key, src.limit_type_code, src.limit_type_name, src.limit_type_description, src.hierarchy_level, src.is_regulatory, TRUE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_regulatory_context — Regulatory Submission Contexts

-- COMMAND ----------

-- DBTITLE 1,Cell 9
CREATE OR REPLACE TEMP VIEW tmp_dim_regulatory_context_seed AS
SELECT
  ROW_NUMBER() OVER (ORDER BY region_code, submission_type, ctd_module) AS regulatory_context_key,
  region_code,
  region_name,
  submission_type,
  ctd_module,
  ctd_section_title,
  regulatory_authority
FROM (
  SELECT *
  FROM VALUES
      ('US',  'United States',   'NDA',  '3.2.P.5.1', 'Specifications — Drug Product',  'FDA'),
      ('US',  'United States',   'NDA',  '3.2.S.4.1', 'Specifications — Drug Substance', 'FDA'),
      ('US',  'United States',   'ANDA', '3.2.P.5.1', 'Specifications — Drug Product',  'FDA'),
      ('US',  'United States',   'ANDA', '3.2.S.4.1', 'Specifications — Drug Substance', 'FDA'),
      ('US',  'United States',   'BLA',  '3.2.P.5.1', 'Specifications — Drug Product',  'FDA'),
      ('US',  'United States',   'IND',  '3.2.P.5.1', 'Specifications — Drug Product',  'FDA'),
      ('EU',  'European Union',  'MAA',  '3.2.P.5.1', 'Specifications — Drug Product',  'EMA'),
      ('EU',  'European Union',  'MAA',  '3.2.S.4.1', 'Specifications — Drug Substance', 'EMA'),
      ('JP',  'Japan',           'JNDA', '3.2.P.5.1', 'Specifications — Drug Product',  'PMDA'),
      ('JP',  'Japan',           'JNDA', '3.2.S.4.1', 'Specifications — Drug Substance', 'PMDA'),
      ('CN',  'China',           'NDA',  '3.2.P.5.1', 'Specifications — Drug Product',  'NMPA'),
      ('CN',  'China',           'NDA',  '3.2.S.4.1', 'Specifications — Drug Substance', 'NMPA'),
      ('ROW', 'Rest of World',   'NDA',  '3.2.P.5.1', 'Specifications — Drug Product',  NULL),
      ('ROW', 'Rest of World',   'NDA',  '3.2.S.4.1', 'Specifications — Drug Substance', NULL)
  AS seed(region_code, region_name, submission_type, ctd_module, ctd_section_title, regulatory_authority)
);

MERGE INTO dim_regulatory_context AS tgt
USING tmp_dim_regulatory_context_seed src
ON tgt.region_code = src.region_code AND tgt.submission_type = src.submission_type AND tgt.ctd_module = src.ctd_module
WHEN MATCHED THEN UPDATE SET
  tgt.region_name = src.region_name,
  tgt.ctd_section_title = src.ctd_section_title,
  tgt.regulatory_authority = src.regulatory_authority,
  tgt.is_active = TRUE
WHEN NOT MATCHED THEN INSERT (regulatory_context_key, region_code, region_name, submission_type, ctd_module, ctd_section_title, regulatory_authority, is_active)
VALUES (src.regulatory_context_key, src.region_code, src.region_name, src.submission_type, src.ctd_module, src.ctd_section_title, src.regulatory_authority, TRUE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_stability_condition — ICH Storage Conditions

-- COMMAND ----------

-- DBTITLE 1,Cell 11
CREATE OR REPLACE TEMP VIEW tmp_dim_stability_condition_seed AS
SELECT
  ROW_NUMBER() OVER (ORDER BY condition_code) AS condition_key,
  condition_code,
  condition_name,
  temperature_celsius,
  humidity_pct,
  ich_condition_type
FROM (
  SELECT *
  FROM VALUES
      ('25C60RH',  '25°C / 60% RH',  CAST(25.0 AS DECIMAL(5,1)), CAST(60.0 AS DECIMAL(5,1)), 'LONG_TERM'),
      ('30C65RH',  '30°C / 65% RH',  CAST(30.0 AS DECIMAL(5,1)), CAST(65.0 AS DECIMAL(5,1)), 'INTERMEDIATE'),
      ('40C75RH',  '40°C / 75% RH',  CAST(40.0 AS DECIMAL(5,1)), CAST(75.0 AS DECIMAL(5,1)), 'ACCELERATED'),
      ('5C',       '5°C ± 3°C',      CAST(5.0 AS DECIMAL(5,1)),  NULL, 'REFRIGERATED'),
      ('REFRIG',   '2-8°C',          CAST(5.0 AS DECIMAL(5,1)),  NULL, 'REFRIGERATED'),
      ('FREEZER',  '-20°C ± 5°C',    CAST(-20.0 AS DECIMAL(5,1)),NULL, 'FROZEN')
  AS seed(condition_code, condition_name, temperature_celsius, humidity_pct, ich_condition_type)
);

MERGE INTO dim_stability_condition AS tgt
USING tmp_dim_stability_condition_seed src
ON tgt.condition_code = src.condition_code
WHEN MATCHED THEN UPDATE SET
  tgt.condition_name = src.condition_name,
  tgt.temperature_celsius = src.temperature_celsius,
  tgt.humidity_pct = src.humidity_pct,
  tgt.ich_condition_type = src.ich_condition_type,
  tgt.is_active = TRUE
WHEN NOT MATCHED THEN INSERT (condition_key, condition_code, condition_name, temperature_celsius, humidity_pct, ich_condition_type, is_active)
VALUES (src.condition_key, src.condition_code, src.condition_name, src.temperature_celsius, src.humidity_pct, src.ich_condition_type, TRUE);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_timepoint — Stability Time Points

-- COMMAND ----------

-- DBTITLE 1,Cell 13
CREATE OR REPLACE TEMP VIEW tmp_dim_timepoint_seed AS
SELECT *
FROM VALUES
    (1, 'T0',   0,  'Initial',   1, TRUE),
    (2, 'T1M',  1,  '1 Month',   2, TRUE),
    (3, 'T3M',  3,  '3 Months',  3, TRUE),
    (4, 'T6M',  6,  '6 Months',  4, TRUE),
    (5, 'T9M',  9,  '9 Months',  5, TRUE),
    (6, 'T12M', 12, '12 Months', 6, TRUE),
    (7, 'T18M', 18, '18 Months', 7, TRUE),
    (8, 'T24M', 24, '24 Months', 8, TRUE),
    (9, 'T36M', 36, '36 Months', 9, TRUE)
AS seed(timepoint_key, timepoint_code, timepoint_months, timepoint_name, display_order, is_active);


MERGE INTO dim_timepoint AS tgt
USING tmp_dim_timepoint_seed src
ON tgt.timepoint_code = src.timepoint_code
WHEN MATCHED THEN UPDATE SET
  tgt.timepoint_months = src.timepoint_months,
  tgt.timepoint_name = src.timepoint_name,
  tgt.display_order = src.display_order,
  tgt.is_active = src.is_active
WHEN NOT MATCHED THEN INSERT (timepoint_key, timepoint_code, timepoint_months, timepoint_name, display_order, is_active)
VALUES (src.timepoint_key, src.timepoint_code, src.timepoint_months, src.timepoint_name, src.display_order, src.is_active);

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
        DAY(d)                                          AS day_of_month,
        DAYOFWEEK(d)                                    AS day_of_week,
        DATE_FORMAT(d, 'EEEE')                          AS day_name,
        WEEKOFYEAR(d)                                   AS week_of_year,
        CASE WHEN DAYOFWEEK(d) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN MONTH(d) >= 7 THEN YEAR(d) + 1 ELSE YEAR(d) END AS fiscal_year,
        CASE WHEN MONTH(d) >= 7 THEN QUARTER(ADD_MONTHS(d, -6)) ELSE QUARTER(ADD_MONTHS(d, 6)) END AS fiscal_quarter
    FROM (
        SELECT EXPLODE(SEQUENCE(DATE'2020-01-01', DATE'2035-12-31', INTERVAL 1 DAY)) AS d
    )
) AS src
ON tgt.date_key = src.date_key
WHEN NOT MATCHED THEN INSERT (
    date_key, full_date, year, quarter, month, month_name, day_of_month, day_of_week, day_name, week_of_year, is_weekend, fiscal_year, fiscal_quarter
)
VALUES (
    src.date_key, src.full_date, src.year, src.quarter, src.month, src.month_name, src.day_of_month, src.day_of_week, src.day_name, src.week_of_year, src.is_weekend, src.fiscal_year, src.fiscal_quarter
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
