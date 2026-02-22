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

USE SCHEMA l2_2_spec_unified;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_uom — Units of Measure

-- COMMAND ----------

MERGE INTO dim_uom AS tgt
USING (
    VALUES
        (1,  'mg',       'Milligrams',                  'MASS',          CAST(0.001 AS DECIMAL(18,10)),          'kg',   TRUE),
        (2,  'g',        'Grams',                       'MASS',          CAST(1.0 AS DECIMAL(18,10)),            'kg',   TRUE),
        (3,  'mcg',      'Micrograms',                  'MASS',          CAST(0.000001 AS DECIMAL(18,10)),       'kg',   TRUE),
        (4,  '%',        'Percent',                     'RATIO',         CAST(0.01 AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE),
        (5,  '% w/w',    'Percent weight/weight',       'RATIO',         CAST(0.01 AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE),
        (6,  '% w/v',    'Percent weight/volume',       'RATIO',         CAST(0.01 AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE),
        (7,  '% area',   'Percent area (HPLC)',         'RATIO',         CAST(0.01 AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE),
        (8,  'ppm',      'Parts per million',           'CONCENTRATION', CAST(0.000001 AS DECIMAL(18,10)),       CAST(NULL AS STRING),   TRUE),
        (9,  'ppb',      'Parts per billion',           'CONCENTRATION', CAST(0.000000001 AS DECIMAL(18,10)),    CAST(NULL AS STRING),   TRUE),
        (10, 'mg/mL',    'Milligrams per milliliter',   'CONCENTRATION', CAST(1.0 AS DECIMAL(18,10)),            'kg/m3',TRUE),
        (11, 'mg/g',     'Milligrams per gram',         'CONCENTRATION', CAST(0.001 AS DECIMAL(18,10)),          CAST(NULL AS STRING),   TRUE),
        (12, 'mcg/mL',   'Micrograms per milliliter',   'CONCENTRATION', CAST(0.001 AS DECIMAL(18,10)),          'kg/m3',TRUE),
        (13, 'IU',       'International Units',         'COUNT',         CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE),
        (14, 'IU/mg',    'International Units per mg',  'CONCENTRATION', CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE),
        (15, 'CFU/g',    'Colony forming units per g',  'COUNT',         CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE),
        (16, 'CFU/mL',   'Colony forming units per mL', 'COUNT',         CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE),
        (17, 'EU/mg',    'Endotoxin units per mg',      'COUNT',         CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE),
        (18, 'mL',       'Milliliters',                 'VOLUME',        CAST(0.000001 AS DECIMAL(18,10)),       'm3',   TRUE),
        (19, 'L',        'Liters',                      'VOLUME',        CAST(0.001 AS DECIMAL(18,10)),          'm3',   TRUE),
        (20, 'mm',       'Millimeters',                 'LENGTH',        CAST(0.001 AS DECIMAL(18,10)),          'm',    TRUE),
        (21, 'min',      'Minutes',                     'OTHER',         CAST(60.0 AS DECIMAL(18,10)),           's',    TRUE),
        (22, 'pH',       'pH units',                    'OTHER',         CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE),
        (23, 'N/A',      'Not applicable',              'OTHER',         CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE),
        (24, 'kp',       'Kilopond (hardness)',         'OTHER',         CAST(9.80665 AS DECIMAL(18,10)),        'N',    TRUE),
        (25, 'mg/tab',   'Milligrams per tablet',       'MASS',          CAST(NULL AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE),
        (26, '% (Q)',    'Percent dissolved (Q value)',  'RATIO',         CAST(0.01 AS DECIMAL(18,10)),           CAST(NULL AS STRING),   TRUE)
    ) AS src(uom_key, uom_code, uom_name, uom_category, si_conversion_factor, si_base_unit, is_active)
ON tgt.uom_key = src.uom_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_limit_type — Limit Type Hierarchy

-- COMMAND ----------

MERGE INTO dim_limit_type AS tgt
USING (
    VALUES
        (1, 'AC',        'Acceptance Criteria',    'Regulatory acceptance limits for release and stability testing',   CAST(2 AS INT), TRUE,  TRUE),
        (2, 'NOR',       'Normal Operating Range', 'Tighter internal operating range derived from process capability', CAST(1 AS INT), FALSE, TRUE),
        (3, 'PAR',       'Proven Acceptable Range','Wider range demonstrated by development and validation data',      CAST(3 AS INT), FALSE, TRUE),
        (4, 'ALERT',     'Alert Limit',            'Statistical alert limit — triggers investigation if breached',     CAST(NULL AS INT), FALSE, TRUE),
        (5, 'ACTION',    'Action Limit',           'Statistical action limit — triggers corrective action',            CAST(NULL AS INT), FALSE, TRUE),
        (6, 'IPC_LIMIT', 'In-Process Control',     'In-process control limit used during manufacturing',               CAST(NULL AS INT), FALSE, TRUE),
        (7, 'REPORT',    'Report Only',            'Informational limit — no pass/fail decision',                      CAST(NULL AS INT), FALSE, TRUE)
    ) AS src(limit_type_key, limit_type_code, limit_type_name, limit_type_description, hierarchy_rank, is_regulatory, is_active)
ON tgt.limit_type_key = src.limit_type_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_regulatory_context — Regulatory Submission Contexts

-- COMMAND ----------

MERGE INTO dim_regulatory_context AS tgt
USING (
    VALUES
        (1,  'US',  'United States',   'NDA',  '3.2.P.5.1', 'Specifications — Drug Product',          'FDA',  TRUE),
        (2,  'US',  'United States',   'NDA',  '3.2.S.4.1', 'Specifications — Drug Substance',        'FDA',  TRUE),
        (3,  'US',  'United States',   'ANDA', '3.2.P.5.1', 'Specifications — Drug Product',          'FDA',  TRUE),
        (4,  'US',  'United States',   'ANDA', '3.2.S.4.1', 'Specifications — Drug Substance',        'FDA',  TRUE),
        (5,  'US',  'United States',   'BLA',  '3.2.P.5.1', 'Specifications — Drug Product',          'FDA',  TRUE),
        (6,  'US',  'United States',   'IND',  '3.2.P.5.1', 'Specifications — Drug Product',          'FDA',  TRUE),
        (7,  'EU',  'European Union',  'MAA',  '3.2.P.5.1', 'Specifications — Drug Product',          'EMA',  TRUE),
        (8,  'EU',  'European Union',  'MAA',  '3.2.S.4.1', 'Specifications — Drug Substance',        'EMA',  TRUE),
        (9,  'JP',  'Japan',           'JNDA', '3.2.P.5.1', 'Specifications — Drug Product',          'PMDA', TRUE),
        (10, 'JP',  'Japan',           'JNDA', '3.2.S.4.1', 'Specifications — Drug Substance',        'PMDA', TRUE),
        (11, 'CN',  'China',           'NDA',  '3.2.P.5.1', 'Specifications — Drug Product',          'NMPA', TRUE),
        (12, 'CN',  'China',           'NDA',  '3.2.S.4.1', 'Specifications — Drug Substance',        'NMPA', TRUE),
        (13, 'ROW', 'Rest of World',   'NDA',  '3.2.P.5.1', 'Specifications — Drug Product',          CAST(NULL AS STRING),   TRUE),
        (14, 'ROW', 'Rest of World',   'NDA',  '3.2.S.4.1', 'Specifications — Drug Substance',        CAST(NULL AS STRING),   TRUE)
    ) AS src(regulatory_context_key, region_code, region_name, submission_type, ctd_module, ctd_section_title, regulatory_authority, is_active)
ON tgt.regulatory_context_key = src.regulatory_context_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_stability_condition — ICH Storage Conditions

-- COMMAND ----------

MERGE INTO dim_stability_condition AS tgt
USING (
    VALUES
        (1, '25C60RH',  '25°C / 60% RH',  CAST(25.0 AS DECIMAL(5,1)), CAST(60.0 AS DECIMAL(5,1)), 'LONG_TERM',     TRUE),
        (2, '30C65RH',  '30°C / 65% RH',  CAST(30.0 AS DECIMAL(5,1)), CAST(65.0 AS DECIMAL(5,1)), 'INTERMEDIATE',  TRUE),
        (3, '40C75RH',  '40°C / 75% RH',  CAST(40.0 AS DECIMAL(5,1)), CAST(75.0 AS DECIMAL(5,1)), 'ACCELERATED',   TRUE),
        (4, '5C',       '5°C ± 3°C',      CAST(5.0 AS DECIMAL(5,1)),  CAST(NULL AS DECIMAL(5,1)), 'REFRIGERATED',  TRUE),
        (5, 'REFRIG',   '2-8°C',          CAST(5.0 AS DECIMAL(5,1)),  CAST(NULL AS DECIMAL(5,1)), 'REFRIGERATED',  TRUE),
        (6, 'FREEZER',  '-20°C ± 5°C',    CAST(-20.0 AS DECIMAL(5,1)),CAST(NULL AS DECIMAL(5,1)), 'FROZEN',        TRUE)
    ) AS src(condition_key, condition_code, condition_name, temperature_celsius, humidity_pct, ich_condition_type, is_active)
ON tgt.condition_key = src.condition_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_timepoint — Stability Time Points

-- COMMAND ----------

MERGE INTO dim_timepoint AS tgt
USING (
    VALUES
        (1,  'T0',   0,  'Initial',    1,  TRUE),
        (2,  'T1M',  1,  '1 Month',    2,  TRUE),
        (3,  'T3M',  3,  '3 Months',   3,  TRUE),
        (4,  'T6M',  6,  '6 Months',   4,  TRUE),
        (5,  'T9M',  9,  '9 Months',   5,  TRUE),
        (6,  'T12M', 12, '12 Months',  6,  TRUE),
        (7,  'T18M', 18, '18 Months',  7,  TRUE),
        (8,  'T24M', 24, '24 Months',  8,  TRUE),
        (9,  'T36M', 36, '36 Months',  9,  TRUE)
    ) AS src(timepoint_key, timepoint_code, timepoint_months, timepoint_name, display_order, is_active)
ON tgt.timepoint_key = src.timepoint_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_date — Calendar Date Dimension
-- MAGIC Populates dates from 2020-01-01 to 2035-12-31.

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
