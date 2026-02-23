-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Drop All Objects — Full Package Refresh
-- MAGIC Drops all tables in all four schemas in reverse dependency order so the
-- MAGIC deployment can run a clean, idempotent rebuild from scratch.
-- MAGIC
-- MAGIC **Execution order (reverse dependency):**
-- MAGIC 1. L3 `l3_data_product` — final OBTs (no dependents)
-- MAGIC 2. L2.2 `l2_2_unified_model` — facts, denormalized, dimensions
-- MAGIC 3. L2.1 `l2_1_scl` — source conform tables
-- MAGIC 4. L1 `l1_raw` — raw ingestion tables
-- MAGIC
-- MAGIC > **WARNING:** This notebook drops ALL tables. Run only when a full

-- COMMAND ----------

-- DBTITLE 1,Drop All Objects — Simplified
USE CATALOG pharma_quality;

-- L3 tables
DROP TABLE IF EXISTS l3_data_product.obt_stability_results;
DROP TABLE IF EXISTS l3_data_product.obt_acceptance_criteria;
DROP TABLE IF EXISTS l3_data_product.obt_specification_ctd;

-- L2.2 fact tables
DROP TABLE IF EXISTS l2_2_unified_model.fact_analytical_result;
DROP TABLE IF EXISTS l2_2_unified_model.fact_specification_limit;

-- L2.2 denormalized table
DROP TABLE IF EXISTS l2_2_unified_model.dspec_specification;

-- L2.2 conformed dimensions
DROP TABLE IF EXISTS l2_2_unified_model.dim_specification_item;
DROP TABLE IF EXISTS l2_2_unified_model.dim_specification;

-- L2.2 analytical dimensions
DROP TABLE IF EXISTS l2_2_unified_model.dim_batch;
DROP TABLE IF EXISTS l2_2_unified_model.dim_instrument;

-- L2.2 MDM dimensions
DROP TABLE IF EXISTS l2_2_unified_model.dim_product;
DROP TABLE IF EXISTS l2_2_unified_model.dim_material;
DROP TABLE IF EXISTS l2_2_unified_model.dim_test_method;
DROP TABLE IF EXISTS l2_2_unified_model.dim_site;
DROP TABLE IF EXISTS l2_2_unified_model.dim_market;

-- L2.2 reference dimensions
DROP TABLE IF EXISTS l2_2_unified_model.dim_date;
DROP TABLE IF EXISTS l2_2_unified_model.dim_uom;
DROP TABLE IF EXISTS l2_2_unified_model.dim_limit_type;
DROP TABLE IF EXISTS l2_2_unified_model.dim_regulatory_context;
DROP TABLE IF EXISTS l2_2_unified_model.dim_stability_condition;
DROP TABLE IF EXISTS l2_2_unified_model.dim_timepoint;

-- L2.1 source conform tables
DROP TABLE IF EXISTS l2_1_scl.src_vendor_analytical_results;
DROP TABLE IF EXISTS l2_1_scl.src_pdf_specification;
DROP TABLE IF EXISTS l2_1_scl.src_process_recipe;
DROP TABLE IF EXISTS l2_1_scl.src_lims_spec_limit;
DROP TABLE IF EXISTS l2_1_scl.src_lims_spec_item;
DROP TABLE IF EXISTS l2_1_scl.src_lims_specification;

-- L1 raw ingestion tables
DROP TABLE IF EXISTS l1_raw.raw_vendor_analytical_results;
DROP TABLE IF EXISTS l1_raw.raw_pdf_specification;
DROP TABLE IF EXISTS l1_raw.raw_process_recipe;
DROP TABLE IF EXISTS l1_raw.raw_lims_spec_limit;
DROP TABLE IF EXISTS l1_raw.raw_lims_spec_item;
DROP TABLE IF EXISTS l1_raw.raw_lims_specification;
