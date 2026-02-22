-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Seed Reference Dimensions
-- MAGIC Populates the L2.2 reference dimensions with static master data:
-- MAGIC - `dim_uom` — Units of measure
-- MAGIC - `dim_limit_type` — Limit type hierarchy
-- MAGIC - `dim_regulatory_context` — Regulatory submission contexts

-- COMMAND ----------

USE CATALOG pharma_quality;
USE SCHEMA l2_2_spec_unified;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## dim_uom — Units of Measure

-- COMMAND ----------

MERGE INTO dim_uom AS tgt
USING (
    VALUES
        (1,  'mg',       'Milligrams',                  'MASS',          0.001,          'kg',   TRUE),
        (2,  'g',        'Grams',                       'MASS',          1.0,            'kg',   TRUE),
        (3,  'mcg',      'Micrograms',                  'MASS',          0.000001,       'kg',   TRUE),
        (4,  '%',        'Percent',                     'RATIO',         0.01,           NULL,   TRUE),
        (5,  '% w/w',    'Percent weight/weight',       'RATIO',         0.01,           NULL,   TRUE),
        (6,  '% w/v',    'Percent weight/volume',       'RATIO',         0.01,           NULL,   TRUE),
        (7,  '% area',   'Percent area (HPLC)',         'RATIO',         0.01,           NULL,   TRUE),
        (8,  'ppm',      'Parts per million',           'CONCENTRATION', 0.000001,       NULL,   TRUE),
        (9,  'ppb',      'Parts per billion',           'CONCENTRATION', 0.000000001,    NULL,   TRUE),
        (10, 'mg/mL',    'Milligrams per milliliter',   'CONCENTRATION', 1.0,            'kg/m3',TRUE),
        (11, 'mg/g',     'Milligrams per gram',         'CONCENTRATION', 0.001,          NULL,   TRUE),
        (12, 'mcg/mL',   'Micrograms per milliliter',   'CONCENTRATION', 0.001,          'kg/m3',TRUE),
        (13, 'IU',       'International Units',         'COUNT',         NULL,           NULL,   TRUE),
        (14, 'IU/mg',    'International Units per mg',  'CONCENTRATION', NULL,           NULL,   TRUE),
        (15, 'CFU/g',    'Colony forming units per g',  'COUNT',         NULL,           NULL,   TRUE),
        (16, 'CFU/mL',   'Colony forming units per mL', 'COUNT',         NULL,           NULL,   TRUE),
        (17, 'EU/mg',    'Endotoxin units per mg',      'COUNT',         NULL,           NULL,   TRUE),
        (18, 'mL',       'Milliliters',                 'VOLUME',        0.000001,       'm3',   TRUE),
        (19, 'L',        'Liters',                      'VOLUME',        0.001,          'm3',   TRUE),
        (20, 'mm',       'Millimeters',                 'LENGTH',        0.001,          'm',    TRUE),
        (21, 'min',      'Minutes',                     'OTHER',         60.0,           's',    TRUE),
        (22, 'pH',       'pH units',                    'OTHER',         NULL,           NULL,   TRUE),
        (23, 'N/A',      'Not applicable',              'OTHER',         NULL,           NULL,   TRUE),
        (24, 'kp',       'Kilopond (hardness)',         'OTHER',         9.80665,        'N',    TRUE),
        (25, 'mg/tab',   'Milligrams per tablet',       'MASS',          NULL,           NULL,   TRUE)
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
        (1, 'AC',        'Acceptance Criteria',    'Regulatory acceptance limits for release and stability testing',   2, TRUE,  TRUE),
        (2, 'NOR',       'Normal Operating Range', 'Tighter internal operating range derived from process capability', 1, FALSE, TRUE),
        (3, 'PAR',       'Proven Acceptable Range','Wider range demonstrated by development and validation data',      3, FALSE, TRUE),
        (4, 'ALERT',     'Alert Limit',            'Statistical alert limit — triggers investigation if breached',     NULL, FALSE, TRUE),
        (5, 'ACTION',    'Action Limit',           'Statistical action limit — triggers corrective action',            NULL, FALSE, TRUE),
        (6, 'IPC_LIMIT', 'In-Process Control',     'In-process control limit used during manufacturing',               NULL, FALSE, TRUE),
        (7, 'REPORT',    'Report Only',            'Informational limit — no pass/fail decision',                      NULL, FALSE, TRUE)
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
        (13, 'ROW', 'Rest of World',   'NDA',  '3.2.P.5.1', 'Specifications — Drug Product',          NULL,   TRUE),
        (14, 'ROW', 'Rest of World',   'NDA',  '3.2.S.4.1', 'Specifications — Drug Substance',        NULL,   TRUE)
    ) AS src(regulatory_context_key, region_code, region_name, submission_type, ctd_module, ctd_section_title, regulatory_authority, is_active)
ON tgt.regulatory_context_key = src.regulatory_context_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify Seed Data

-- COMMAND ----------

SELECT 'dim_uom' AS table_name, COUNT(*) AS row_count FROM dim_uom
UNION ALL
SELECT 'dim_limit_type', COUNT(*) FROM dim_limit_type
UNION ALL
SELECT 'dim_regulatory_context', COUNT(*) FROM dim_regulatory_context;
