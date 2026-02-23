-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Validation Queries — Pharma Quality Unified Data Model
-- MAGIC End-to-end validation queries to verify data integrity across all layers.
-- MAGIC
-- MAGIC **Checks performed:**
-- MAGIC 1. Row counts per layer
-- MAGIC 2. Referential integrity (FK checks)
-- MAGIC 3. Limit hierarchy validation (PAR >= AC >= NOR)
-- MAGIC 4. Data quality flag summary
-- MAGIC 5. L3 output completeness

-- COMMAND ----------

USE CATALOG pharma_quality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Row Counts — All Layers

-- COMMAND ----------

-- L1 Raw
SELECT 'L1: raw_lims_specification' AS table_name, COUNT(*) AS row_count FROM l1_raw.raw_lims_specification
UNION ALL
SELECT 'L1: raw_lims_spec_item',     COUNT(*) FROM l1_raw.raw_lims_spec_item
UNION ALL
SELECT 'L1: raw_lims_spec_limit',    COUNT(*) FROM l1_raw.raw_lims_spec_limit
UNION ALL
-- L2.1 Source Conform
SELECT 'L2.1: src_lims_specification', COUNT(*) FROM l2_1_scl.src_lims_specification
UNION ALL
SELECT 'L2.1: src_lims_spec_item',    COUNT(*) FROM l2_1_scl.src_lims_spec_item
UNION ALL
SELECT 'L2.1: src_lims_spec_limit',   COUNT(*) FROM l2_1_scl.src_lims_spec_limit
UNION ALL
-- L2.2 Reference Dimensions
SELECT 'L2.2: dim_date',              COUNT(*) FROM l2_2_unified_model.dim_date
UNION ALL
SELECT 'L2.2: dim_uom',               COUNT(*) FROM l2_2_unified_model.dim_uom
UNION ALL
SELECT 'L2.2: dim_limit_type',        COUNT(*) FROM l2_2_unified_model.dim_limit_type
UNION ALL
SELECT 'L2.2: dim_regulatory_context', COUNT(*) FROM l2_2_unified_model.dim_regulatory_context
UNION ALL
-- L2.2 MDM Dimensions
SELECT 'L2.2: dim_product',           COUNT(*) FROM l2_2_unified_model.dim_product
UNION ALL
SELECT 'L2.2: dim_material',          COUNT(*) FROM l2_2_unified_model.dim_material
UNION ALL
SELECT 'L2.2: dim_test_method',       COUNT(*) FROM l2_2_unified_model.dim_test_method
UNION ALL
SELECT 'L2.2: dim_site',              COUNT(*) FROM l2_2_unified_model.dim_site
UNION ALL
SELECT 'L2.2: dim_market',            COUNT(*) FROM l2_2_unified_model.dim_market
UNION ALL
SELECT 'L2.2: dim_specification',     COUNT(*) FROM l2_2_unified_model.dim_specification
UNION ALL
SELECT 'L2.2: dim_specification_item', COUNT(*) FROM l2_2_unified_model.dim_specification_item
UNION ALL
-- L2.2 Fact + Denormalized
SELECT 'L2.2: fact_specification_limit', COUNT(*) FROM l2_2_unified_model.fact_specification_limit
UNION ALL
SELECT 'L2.2: dspec_specification',    COUNT(*) FROM l2_2_unified_model.dspec_specification
UNION ALL
-- L3 Final Products
SELECT 'L3: obt_specification_ctd',   COUNT(*) FROM l3_data_product.obt_specification_ctd
UNION ALL
SELECT 'L3: obt_acceptance_criteria', COUNT(*) FROM l3_data_product.obt_acceptance_criteria
ORDER BY table_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Reference Dimension Completeness

-- COMMAND ----------

SELECT 'dim_uom' AS dimension, COUNT(*) AS total,
       COUNT(DISTINCT uom_category) AS categories
FROM l2_2_unified_model.dim_uom;

-- COMMAND ----------

SELECT 'dim_limit_type' AS dimension, COUNT(*) AS total,
       COUNT(CASE WHEN is_regulatory = TRUE THEN 1 END) AS regulatory_types,
       COUNT(CASE WHEN is_regulatory = FALSE THEN 1 END) AS internal_types
FROM l2_2_unified_model.dim_limit_type;

-- COMMAND ----------

SELECT 'dim_regulatory_context' AS dimension, COUNT(*) AS total,
       COUNT(DISTINCT region_code) AS regions,
       COUNT(DISTINCT submission_type) AS submission_types
FROM l2_2_unified_model.dim_regulatory_context;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Referential Integrity — Star Schema FK Checks

-- COMMAND ----------

-- Fact → dim_specification (orphan check)
SELECT 'fact → dim_specification' AS fk_check,
       COUNT(*) AS orphan_rows
FROM l2_2_unified_model.fact_specification_limit f
LEFT JOIN l2_2_unified_model.dim_specification s ON f.spec_key = s.spec_key
WHERE s.spec_key IS NULL;

-- COMMAND ----------

-- Fact → dim_specification_item (orphan check)
SELECT 'fact → dim_specification_item' AS fk_check,
       COUNT(*) AS orphan_rows
FROM l2_2_unified_model.fact_specification_limit f
LEFT JOIN l2_2_unified_model.dim_specification_item i ON f.spec_item_key = i.spec_item_key
WHERE i.spec_item_key IS NULL;

-- COMMAND ----------

-- Fact → dim_limit_type (orphan check)
SELECT 'fact → dim_limit_type' AS fk_check,
       COUNT(*) AS orphan_rows
FROM l2_2_unified_model.fact_specification_limit f
LEFT JOIN l2_2_unified_model.dim_limit_type lt ON f.limit_type_key = lt.limit_type_key
WHERE lt.limit_type_key IS NULL;

-- COMMAND ----------

-- dim_specification_item → dim_specification (orphan check)
SELECT 'spec_item → dim_specification' AS fk_check,
       COUNT(*) AS orphan_rows
FROM l2_2_unified_model.dim_specification_item i
LEFT JOIN l2_2_unified_model.dim_specification s ON i.spec_key = s.spec_key
WHERE s.spec_key IS NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Limit Hierarchy Validation (PAR >= AC >= NOR)

-- COMMAND ----------

SELECT
    s.spec_number,
    i.test_name,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END) AS nor_lower,
    MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END) AS ac_lower,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) AS par_lower,
    MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) AS nor_upper,
    MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) AS ac_upper,
    MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) AS par_upper,
    CASE WHEN
        (MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) <=
         MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END) OR
         MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.lower_limit_value END) IS NULL)
        AND
        (MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.lower_limit_value END) <=
         MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END) OR
         MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.lower_limit_value END) IS NULL)
        AND
        (MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) >=
         MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) OR
         MAX(CASE WHEN lt.limit_type_code = 'PAR' THEN f.upper_limit_value END) IS NULL)
        AND
        (MAX(CASE WHEN lt.limit_type_code = 'AC'  THEN f.upper_limit_value END) >=
         MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) OR
         MAX(CASE WHEN lt.limit_type_code = 'NOR' THEN f.upper_limit_value END) IS NULL)
    THEN 'VALID' ELSE 'VIOLATION' END AS hierarchy_status
FROM l2_2_unified_model.fact_specification_limit f
JOIN l2_2_unified_model.dim_specification s ON f.spec_key = s.spec_key
JOIN l2_2_unified_model.dim_specification_item i ON f.spec_item_key = i.spec_item_key
JOIN l2_2_unified_model.dim_limit_type lt ON f.limit_type_key = lt.limit_type_key
WHERE f.is_current = TRUE
  AND f.stage_code = 'RELEASE'
  AND lt.limit_type_code IN ('AC', 'NOR', 'PAR')
GROUP BY s.spec_number, i.test_name
ORDER BY s.spec_number, i.test_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. L3 Output Summary

-- COMMAND ----------

-- CTD OBT breakdown by spec type and stage
SELECT spec_type_code, stage_code, COUNT(*) AS rows
FROM l3_data_product.obt_specification_ctd
GROUP BY spec_type_code, stage_code
ORDER BY spec_type_code, stage_code;

-- COMMAND ----------

-- Acceptance Criteria summary with width metrics
SELECT spec_number, test_name, ac_lower_limit, ac_upper_limit, ac_width,
       nor_lower_limit, nor_upper_limit, nor_width,
       par_lower_limit, par_upper_limit, par_width,
       nor_tightness_pct, par_vs_ac_factor, is_hierarchy_valid
FROM l3_data_product.obt_acceptance_criteria
ORDER BY spec_number, sequence_number;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. dspec Denormalized Table — Pivot Verification

-- COMMAND ----------

SELECT spec_number, spec_version, test_name,
       ac_limit_description, nor_limit_description, par_limit_description,
       alert_limit_description, action_limit_description,
       is_hierarchy_valid
FROM l2_2_unified_model.dspec_specification
WHERE is_current = TRUE
ORDER BY spec_number, sequence_number;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Validation Complete
-- MAGIC All checks above should show:
-- MAGIC - Non-zero row counts for populated tables
-- MAGIC - Zero orphan rows for all FK checks
-- MAGIC - `VALID` hierarchy status for all limit comparisons
-- MAGIC - Consistent row counts between L2.2 dspec and L3 OBTs
